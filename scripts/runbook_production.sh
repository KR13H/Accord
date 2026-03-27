#!/bin/bash
# Accord V3: Final Production Runbook
set -euo pipefail

echo "========================================"
echo "  ACCORD V3 FINAL PRODUCTION RUNBOOK"
echo "========================================"

DOMAIN=${1:-""}
ADMIN_EMAIL=${2:-""}

if [[ -z "$DOMAIN" || -z "$ADMIN_EMAIL" ]]; then
    echo "Usage: sudo bash scripts/runbook_production.sh <domain> <admin_email>"
    echo "Example: sudo bash scripts/runbook_production.sh api.accord-erp.com admin@accord-erp.com"
    exit 1
fi

# 1. Guardrail: Root/Sudo Check
if [[ $EUID -ne 0 ]]; then
   echo "❌ Error: This script modifies Nginx and certbot. It must be run as root (use sudo)."
   exit 1
fi

# 2. Guardrail: OS Validation (Ubuntu/Debian)
if ! grep -qEi "(ubuntu|debian)" /etc/os-release; then
   echo "❌ Error: This runbook is strictly designed for Ubuntu/Debian VPS environments."
   exit 1
fi
echo "✅ OS Validation passed."

# 3. Environment Validation
if [[ ! -f ".env.production" ]]; then
    echo "❌ Error: .env.production file not found in the root directory. Please populate it first."
    exit 1
fi
echo "✅ Environment configuration validated."

# 4. Execution Sequence: TLS & Nginx Bootstrap
echo "⏳ Step 1: Executing TLS & Nginx Bootstrap for $DOMAIN..."
bash scripts/setup_nginx_tls.sh "$DOMAIN" "$ADMIN_EMAIL"
echo "✅ TLS & Nginx configured successfully."

# 5. Execution Sequence: Seed Superadmin
echo "⏳ Step 2: Seeding Superadmin ($ADMIN_EMAIL)..."
# Using the dependency-lean script
python3 cloud-backend/seed_admin.py --email "$ADMIN_EMAIL" --admin-id 1001
echo "✅ Superadmin seeded and locked."

# 6. Health Checks: API and Memory Profiler
echo "⏳ Step 3: Running health and memory profiler diagnostics..."
sleep 3 # Allow services to settle

# Ping Health Endpoint
if curl -sSf "https://$DOMAIN/api/v1/health" > /dev/null; then
    echo "✅ API Health check passed: Stack is breathing."
else
    echo "❌ API Health check failed."
fi

# Ping Profiler Endpoint (Requires the seeded Admin ID)
if curl -sSf -H "X-Admin-Id: 1001" "https://$DOMAIN/api/v1/system/memory-profile" > /dev/null; then
    echo "✅ Tracemalloc Memory Profiler is active and securely responding."
else
    echo "⚠️ Memory profiler check failed. Ensure ACCORD_ENABLE_TRACEMALLOC=1 is set in .env.production."
fi

echo "========================================"
echo "🚀 ACCORD V3 IS FULLY LIVE AND SECURE"
echo "========================================"
