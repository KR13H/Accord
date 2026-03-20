#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-http://127.0.0.1:8000}"
ROLE_HEADER="X-Role: ca"
ADMIN_HEADER="X-Admin-Id: 1001"
PYTHON_BIN="${PYTHON_BIN:-$(command -v python3 || command -v python || true)}"

if [[ -z "$PYTHON_BIN" ]]; then
	echo "python3/python is required to run this demo script"
	exit 1
fi

now_ms() {
"$PYTHON_BIN" - <<'PY'
import time
print(int(time.time() * 1000))
PY
}

START_MS="$(now_ms)"

echo "[0/7] Resetting demo environment"
curl -s -X POST "$BASE_URL/api/v1/ca/demo/reset" -H "$ROLE_HEADER" -H "$ADMIN_HEADER" | cat

echo "\n[1/7] Seeding demo risk entry"
curl -s -X POST "$BASE_URL/api/v1/ca/alerts/demo-seed" -H "$ROLE_HEADER" -H "$ADMIN_HEADER" | cat

echo "\n[2/7] Evaluating alert rules"
curl -s -X POST "$BASE_URL/api/v1/ca/alerts/evaluate?limit=160" -H "$ROLE_HEADER" -H "$ADMIN_HEADER" | cat

echo "\n[3/7] Listing OPEN alerts"
OPEN_ALERTS_JSON="$(curl -s "$BASE_URL/api/v1/ca/alerts?status=OPEN&limit=20" -H "$ROLE_HEADER" -H "$ADMIN_HEADER")"
printf '%s' "$OPEN_ALERTS_JSON" | cat

ALERT_ID="$($PYTHON_BIN -c 'import json,sys
raw = sys.argv[1] if len(sys.argv) > 1 else "{}"
try:
	payload = json.loads(raw)
except Exception:
	print("")
	raise SystemExit(0)
alerts = payload.get("alerts") if isinstance(payload, dict) else []
if isinstance(alerts, list) and alerts:
	print(alerts[0].get("id", ""))
else:
	print("")' "$OPEN_ALERTS_JSON")"

echo "\n[4/7] Executing one-click remediation playbook"
if [[ -n "$ALERT_ID" ]]; then
  curl -s -X POST "$BASE_URL/api/v1/ca/playbooks/execute" \
	-H "Content-Type: application/json" \
	-H "$ROLE_HEADER" \
	-H "$ADMIN_HEADER" \
	-d "{\"alert_id\": $ALERT_ID, \"hold_hours\": 72, \"playbook_key\": \"ALERT_REMEDIATION_V1\"}" | cat
else
  echo "{\"status\":\"skipped\",\"detail\":\"No OPEN alert available for remediation\"}"
fi

echo "\n[5/7] Listing CLOSED alerts"
curl -s "$BASE_URL/api/v1/ca/alerts?status=CLOSED&limit=20" -H "$ROLE_HEADER" -H "$ADMIN_HEADER" | cat

echo "\n[6/7] Preview heatmap summary"
curl -s "$BASE_URL/api/v1/ca/heatmap?limit=20" -H "$ROLE_HEADER" -H "$ADMIN_HEADER" | cat

END_MS="$(now_ms)"
ELAPSED_MS=$((END_MS - START_MS))
echo "\nDemo completed in ${ELAPSED_MS} ms"
