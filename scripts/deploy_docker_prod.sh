#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

ENV_FILE="${1:-$ROOT_DIR/.env.production}"
COMPOSE_FILE="$ROOT_DIR/docker-compose.prod.yml"

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "[deploy] Missing docker-compose.prod.yml"
  exit 1
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "[deploy] Missing env file: $ENV_FILE"
  echo "[deploy] Copy .env.production.example to .env.production and fill values first."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "[deploy] Docker daemon is not running"
  exit 1
fi

echo "[deploy] Using env file: $ENV_FILE"
echo "[deploy] Building and starting Accord production stack"

docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" up --build -d

echo "[deploy] Waiting for backend health"
for i in {1..60}; do
  if curl -fsS "http://localhost:8000/api/v1/health" >/dev/null 2>&1; then
    echo "[deploy] Backend healthy"
    break
  fi
  sleep 2
  if [[ "$i" -eq 60 ]]; then
    echo "[deploy] Backend did not become healthy in time"
    docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" logs backend --tail 120 || true
    exit 1
  fi
done

echo "[deploy] Checking realtime token endpoint"
curl -fsS "http://localhost:8000/api/v1/ca/events/token?ca_id=201" \
  -H "X-Role: admin" \
  -H "X-Admin-Id: 1001" >/dev/null

echo "[deploy] Stack is live"
echo "Frontend: http://localhost:3000"
echo "Backend:  http://localhost:8000/docs"
echo "Health:   http://localhost:8000/api/v1/health"

if [[ "${ACCORD_RUN_READINESS:-0}" == "1" ]]; then
  echo "[deploy] Running end-to-end readiness checks"
  "$ROOT_DIR/scripts/readiness_e2e_prod.sh" "$ENV_FILE"
fi
