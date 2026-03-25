#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${ROOT_DIR}/.env.production"
TLS_SCRIPT="${ROOT_DIR}/scripts/setup_nginx_tls.sh"
SEED_SCRIPT="${ROOT_DIR}/cloud-backend/seed_admin.py"

API_DOMAIN="${API_DOMAIN:-}"
LETSENCRYPT_EMAIL="${LETSENCRYPT_EMAIL:-}"
SUPERADMIN_EMAIL="${SUPERADMIN_EMAIL:-}"
SUPERADMIN_ID="${SUPERADMIN_ID:-1}"
BACKEND_UPSTREAM="${BACKEND_UPSTREAM:-http://127.0.0.1:8000}"

usage() {
  cat <<'EOF'
Usage:
  sudo ./run_production_runbook.sh \
    --api-domain api.yourdomain.com \
    --letsencrypt-email ops@yourdomain.com \
    --superadmin-email admin@yourdomain.com \
    [--superadmin-id 1] \
    [--backend-upstream http://127.0.0.1:8000]

Environment variables accepted as alternatives:
  API_DOMAIN, LETSENCRYPT_EMAIL, SUPERADMIN_EMAIL, SUPERADMIN_ID, BACKEND_UPSTREAM
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --api-domain)
      API_DOMAIN="$2"
      shift 2
      ;;
    --letsencrypt-email)
      LETSENCRYPT_EMAIL="$2"
      shift 2
      ;;
    --superadmin-email)
      SUPERADMIN_EMAIL="$2"
      shift 2
      ;;
    --superadmin-id)
      SUPERADMIN_ID="$2"
      shift 2
      ;;
    --backend-upstream)
      BACKEND_UPSTREAM="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[runbook] Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "[runbook] Missing required command: $1"
    exit 1
  }
}

require_non_empty() {
  local name="$1"
  local value="$2"
  if [[ -z "${value// }" ]]; then
    echo "[runbook] Missing required value: ${name}"
    exit 1
  fi
}

validate_not_placeholder() {
  local name="$1"
  local value="$2"
  if [[ "$value" =~ YOUR_ ]] || [[ "$value" =~ accord\.example ]] || [[ "$value" =~ replace-with ]] || [[ "$value" =~ change-this-in-production ]]; then
    echo "[runbook] ${name} still contains placeholder value: ${value}"
    exit 1
  fi
}

if [[ ! -f /etc/os-release ]]; then
  echo "[runbook] Cannot detect OS. Expected Ubuntu/Debian VPS."
  exit 1
fi

# shellcheck disable=SC1091
source /etc/os-release
if [[ "${ID:-}" != "ubuntu" && "${ID:-}" != "debian" && "${ID_LIKE:-}" != *"debian"* ]]; then
  echo "[runbook] Unsupported OS (${ID:-unknown}). This runbook supports Ubuntu/Debian only."
  exit 1
fi

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  if command -v sudo >/dev/null 2>&1; then
    echo "[runbook] Elevating with sudo..."
    exec sudo -E bash "$0" "$@"
  fi
  echo "[runbook] Run as root or install sudo."
  exit 1
fi

need_cmd bash
need_cmd python
need_cmd curl
need_cmd grep
need_cmd awk

if [[ ! -f "${TLS_SCRIPT}" ]]; then
  echo "[runbook] Missing TLS script: ${TLS_SCRIPT}"
  exit 1
fi

if [[ ! -f "${SEED_SCRIPT}" ]]; then
  echo "[runbook] Missing seed script: ${SEED_SCRIPT}"
  exit 1
fi

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "[runbook] Missing env file: ${ENV_FILE}"
  echo "[runbook] Copy .env.production.example to .env.production and fill all values first."
  exit 1
fi

# Load env and validate required keys.
set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

require_non_empty "API_DOMAIN" "${API_DOMAIN}"
require_non_empty "LETSENCRYPT_EMAIL" "${LETSENCRYPT_EMAIL}"
require_non_empty "SUPERADMIN_EMAIL" "${SUPERADMIN_EMAIL}"
require_non_empty "SUPERADMIN_ID" "${SUPERADMIN_ID}"

if ! [[ "${SUPERADMIN_ID}" =~ ^[0-9]+$ ]]; then
  echo "[runbook] SUPERADMIN_ID must be numeric"
  exit 1
fi

required_env_keys=(
  BACKEND_PUBLIC_URL
  FRONTEND_PUBLIC_URL
  ACCORD_DEPLOYMENT_MODE
  ACCORD_REDIS_URL
  ACCORD_BIOMETRIC_SECRET
  ACCORD_SSE_TOKEN_SECRET
  DATABASE_URL
  CORS_ALLOW_ORIGINS
  ACCORD_ENABLE_TRACEMALLOC
)

for key in "${required_env_keys[@]}"; do
  value="${!key:-}"
  require_non_empty "${key}" "${value}"
  validate_not_placeholder "${key}" "${value}"
done

if [[ "${ACCORD_ENABLE_TRACEMALLOC,,}" != "1" && "${ACCORD_ENABLE_TRACEMALLOC,,}" != "true" && "${ACCORD_ENABLE_TRACEMALLOC,,}" != "yes" ]]; then
  echo "[runbook] ACCORD_ENABLE_TRACEMALLOC must be enabled in .env.production for memory health checks."
  exit 1
fi

echo "[runbook] Guardrails passed: ${ID} VPS, root privileges, scripts and env validated."

echo "[runbook] Step 1/4: Configure nginx + TLS"
bash "${TLS_SCRIPT}" "${API_DOMAIN}" "${LETSENCRYPT_EMAIL}" "${BACKEND_UPSTREAM}"

echo "[runbook] Step 2/4: Seed SUPERADMIN"
python "${SEED_SCRIPT}" \
  --email "${SUPERADMIN_EMAIL}" \
  --admin-id "${SUPERADMIN_ID}" \
  --database-url "${DATABASE_URL}" \
  --display-name "Accord Superadmin" \
  --role "SUPERADMIN"

echo "[runbook] Step 3/4: Health check API"
curl -fsS "https://${API_DOMAIN}/api/v1/health" >/tmp/accord_health.json
cat /tmp/accord_health.json

echo "[runbook] Step 4/4: Health check memory profiler"
curl -fsS "https://${API_DOMAIN}/api/v1/system/memory-profile?limit=10&compare_with_previous=true" \
  -H "X-Role: admin" \
  -H "X-Admin-Id: ${SUPERADMIN_ID}" >/tmp/accord_memory_profile.json
cat /tmp/accord_memory_profile.json

echo "[runbook] ✅ Accord production runbook completed successfully"
echo "[runbook] API: https://${API_DOMAIN}/api/v1/health"
echo "[runbook] Memory: https://${API_DOMAIN}/api/v1/system/memory-profile"
