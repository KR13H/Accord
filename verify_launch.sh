#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

FRONTEND_URL="${FRONTEND_URL:-http://localhost:3000}"
API_URL="${API_URL:-http://localhost:8000}"
PG_CONTAINER="${PG_CONTAINER:-postgres}"

ok() { printf "\033[32m[OK]\033[0m %s\n" "$1"; }
warn() { printf "\033[33m[WARN]\033[0m %s\n" "$1"; }
fail() { printf "\033[31m[FAIL]\033[0m %s\n" "$1"; exit 1; }

echo "[Verify] Accord launch verification started..."

RAM_DISK_PATH="/Volumes/AccordCache"
RAM_DISK_BUFFER_PATH="$RAM_DISK_PATH/receipt_buffer"
if [[ ! -d "$RAM_DISK_PATH" ]]; then
  warn "RAM disk not mounted at $RAM_DISK_PATH. Attempting remount..."
  if command -v hdiutil >/dev/null 2>&1 && command -v diskutil >/dev/null 2>&1; then
    RAM_SECTORS=$((2 * 1024 * 1024 * 1024 / 512))
    RAM_DEVICE="$(hdiutil attach -nomount "ram://$RAM_SECTORS" 2>/dev/null || true)"
    if [[ -n "$RAM_DEVICE" ]]; then
      diskutil erasevolume HFS+ AccordCache "$RAM_DEVICE" >/dev/null 2>&1 \
        && ok "RAM disk remounted at $RAM_DISK_PATH" \
        || fail "Failed to format RAM disk device $RAM_DEVICE"
    else
      fail "Failed to allocate RAM disk device"
    fi
  else
    fail "hdiutil/diskutil unavailable; cannot remount RAM disk"
  fi
else
  ok "RAM disk mounted at $RAM_DISK_PATH"
fi

mkdir -p "$RAM_DISK_BUFFER_PATH"
ok "RAM disk buffer path ready at $RAM_DISK_BUFFER_PATH"

curl -fsS "$FRONTEND_URL" >/dev/null && ok "Frontend reachable at $FRONTEND_URL" || fail "Frontend is down"

curl -fsS "$API_URL/api/v1/health" >/dev/null && ok "Backend health endpoint is up" || fail "Backend health failed"

FRIDAY_JSON="$(curl -fsS "$API_URL/api/v1/insights/friday-health" || true)"
if echo "$FRIDAY_JSON" | grep -q '"status":"ok"'; then
  ok "Friday health is OK"
else
  warn "Friday health is degraded: $FRIDAY_JSON"
fi

if docker info >/dev/null 2>&1; then
  if docker compose ps "$PG_CONTAINER" >/dev/null 2>&1; then
    docker compose exec -T "$PG_CONTAINER" psql -U accord -d accord -c "select 1" >/dev/null 2>&1 \
      && ok "Postgres query check passed" \
      || fail "Postgres container is up but query failed"
  else
    warn "Postgres service '$PG_CONTAINER' not found in compose"
  fi
else
  warn "Docker daemon not running; skipped Postgres container verification"
fi

if curl -fsS "http://localhost:11434/api/tags" >/dev/null 2>&1; then
  ok "Ollama is reachable"
else
  fail "Ollama endpoint is not reachable"
fi

OLLAMA_TAGS_JSON="$(curl -fsS "http://localhost:11434/api/tags")"
if echo "$OLLAMA_TAGS_JSON" | grep -Eq '"name":"mistral(:[^"]+)?"'; then
  ok "Mistral model present"
else
  fail "Mistral model missing in Ollama registry"
fi

if echo "$OLLAMA_TAGS_JSON" | grep -Eq '"name":"llava(:[^"]+)?"'; then
  ok "Llava model present"
else
  fail "Llava model missing in Ollama registry"
fi

FRIDAY_ASK="$(curl -s -X POST "$API_URL/api/v1/insights/ask-friday" -H "Content-Type: application/json" -d '{"question":"Quick launch verification risk summary","model":"llama3.2"}')"
if echo "$FRIDAY_ASK" | grep -q '"source":"OLLAMA_LOCAL"'; then
  ok "Friday ask endpoint handshake passed"
else
  warn "Friday ask endpoint returned unexpected payload: $FRIDAY_ASK"
fi

INVITE_RESP="$(curl -s -X POST "$API_URL/api/v1/ca/invite?email=test.ca%40accord.com" -H "X-Role: admin" -H "X-Admin-Id: 1001")"
INVITE_TOKEN="$(echo "$INVITE_RESP" | sed -n 's/.*"invite_token":"\([^"]*\)".*/\1/p')"
if [[ -n "$INVITE_TOKEN" ]]; then
  VERIFY_RESP="$(curl -s "$API_URL/api/v1/ca/verify-token/$INVITE_TOKEN")"
  if echo "$VERIFY_RESP" | grep -q '"status":"valid"'; then
    ok "CA invite lifecycle check passed"
  else
    warn "CA invite verification failed: $VERIFY_RESP"
  fi
else
  warn "CA invite creation skipped/unavailable: $INVITE_RESP"
fi

echo "[Verify] Completed."
