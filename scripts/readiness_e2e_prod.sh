#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ENV_FILE="${1:-$ROOT_DIR/.env.production}"
COMPOSE_FILE="$ROOT_DIR/docker-compose.prod.yml"
BASE_URL="${ACCORD_BASE_URL:-http://localhost:8000}"

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "[readiness] Missing compose file: $COMPOSE_FILE"
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "[readiness] Missing env file: $ENV_FILE"
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "[readiness] Docker CLI is required"
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "[readiness] curl is required"
  exit 1
fi

echo "[readiness] Using env file: $ENV_FILE"

compose() {
  docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

ensure_service_running() {
  local service="$1"
  local cid
  cid="$(compose ps -q "$service" 2>/dev/null || true)"
  if [[ -z "$cid" ]]; then
    echo "[readiness] Service not running: $service"
    compose ps
    exit 1
  fi
}

wait_for_healthy() {
  local service="$1"
  local max_tries="$2"

  ensure_service_running "$service"

  local i
  for i in $(seq 1 "$max_tries"); do
    local cid status
    cid="$(compose ps -q "$service")"
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$cid" 2>/dev/null || echo unknown)"

    if [[ "$status" == "healthy" || "$status" == "none" ]]; then
      echo "[readiness] $service status: $status"
      return 0
    fi

    if [[ "$status" == "unhealthy" || "$status" == "exited" || "$status" == "dead" ]]; then
      echo "[readiness] $service status: $status"
      compose logs "$service" --tail 120 || true
      exit 1
    fi

    sleep 2
  done

  echo "[readiness] $service did not become healthy in time"
  compose logs "$service" --tail 120 || true
  exit 1
}

echo "[readiness] Validating service healthchecks"
wait_for_healthy redis 45
wait_for_healthy backend 60
wait_for_healthy frontend 45

echo "[readiness] Validating backend DB path via health endpoint"
health_json="$(curl -fsS "$BASE_URL/api/v1/health")"
python3 - <<'PY' "$health_json"
import json
import sys

payload = json.loads(sys.argv[1])
status = str(payload.get("status", "")).lower()
if status not in {"ok", "degraded"}:
    raise SystemExit(f"Unexpected health status: {status}")

db = payload.get("database") or {}
if not db:
    raise SystemExit("Missing database payload in /api/v1/health")

print("[readiness] health status:", status)
print("[readiness] database backend:", db.get("configured_backend"))
PY

echo "[readiness] Validating Redis connectivity"
redis_ping="$(compose exec -T redis redis-cli ping | tr -d '\r')"
if [[ "$redis_ping" != "PONG" ]]; then
  echo "[readiness] Redis ping failed: $redis_ping"
  exit 1
fi

echo "[readiness] Minting SSE token"
token_json="$(curl -fsS "$BASE_URL/api/v1/ca/events/token?ca_id=201" -H "X-Role: admin" -H "X-Admin-Id: 1001")"
stream_token="$(python3 - <<'PY' "$token_json"
import json
import sys

payload = json.loads(sys.argv[1])
token = payload.get("token")
if not token:
    raise SystemExit("Missing token in /api/v1/ca/events/token response")
print(token)
PY
)"

echo "[readiness] Opening SSE stream"
stream_file="$(mktemp)"
cleanup() {
  if [[ -n "${stream_pid:-}" ]] && kill -0 "$stream_pid" >/dev/null 2>&1; then
    kill "$stream_pid" >/dev/null 2>&1 || true
  fi
  rm -f "$stream_file"
}
trap cleanup EXIT

curl -sN "$BASE_URL/api/v1/ca/events/stream?ca_id=201&token=$stream_token" >"$stream_file" &
stream_pid="$!"

sleep 2
if ! kill -0 "$stream_pid" >/dev/null 2>&1; then
  echo "[readiness] SSE stream process exited early"
  exit 1
fi

if ! grep -q "event: connected" "$stream_file"; then
  echo "[readiness] SSE did not emit connected handshake"
  cat "$stream_file"
  exit 1
fi

echo "[readiness] Triggering voice commit event"
curl -fsS "$BASE_URL/api/v2/mobile/connect-ca" \
  -H "Content-Type: application/json" \
  -H "X-Role: admin" \
  -H "X-Admin-Id: 1001" \
  -d '{"sme_id":101,"ca_id":201,"sme_name":"Readiness SME","ca_firm_name":"Readiness CA"}' >/dev/null

start_json="$(curl -fsS "$BASE_URL/api/v2/mobile/voice/session/start" \
  -H "Content-Type: application/json" \
  -H "X-Role: admin" \
  -H "X-Admin-Id: 1001" \
  -d '{"sme_id":101,"ca_id":201,"language":"en-IN"}')"

session_id="$(python3 - <<'PY' "$start_json"
import json
import sys

payload = json.loads(sys.argv[1])
session_id = payload.get("session_id")
if not session_id:
    raise SystemExit("Missing session_id from /voice/session/start")
print(session_id)
PY
)"

curl -fsS "$BASE_URL/api/v2/mobile/voice/session/chunk" \
  -H "Content-Type: application/json" \
  -H "X-Role: admin" \
  -H "X-Admin-Id: 1001" \
  -d "{\"session_id\":\"$session_id\",\"chunk_text\":\"record cash sale amount 1234\"}" >/dev/null

curl -fsS "$BASE_URL/api/v2/mobile/voice/session/commit" \
  -H "Content-Type: application/json" \
  -H "X-Role: admin" \
  -H "X-Admin-Id: 1001" \
  -d "{\"session_id\":\"$session_id\"}" >/dev/null

for i in {1..20}; do
  if grep -q "event: new_transaction" "$stream_file"; then
    break
  fi
  sleep 1
  if [[ "$i" -eq 20 ]]; then
    echo "[readiness] No new_transaction event observed on SSE stream"
    echo "[readiness] Stream output:"
    cat "$stream_file"
    exit 1
  fi
done

echo "[readiness] E2E checks passed: DB -> Redis -> SSE event stream"
