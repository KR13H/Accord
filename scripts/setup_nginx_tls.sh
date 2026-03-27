#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE_PATH="$ROOT_DIR/deploy/nginx/accord-api.conf.template"
SITE_NAME="accord-api.conf"
SITE_AVAILABLE="/etc/nginx/sites-available/$SITE_NAME"
SITE_ENABLED="/etc/nginx/sites-enabled/$SITE_NAME"

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  echo "[nginx-tls] Please run as root (sudo)."
  exit 1
fi

if [[ ! -f "$TEMPLATE_PATH" ]]; then
  echo "[nginx-tls] Missing template: $TEMPLATE_PATH"
  exit 1
fi

API_DOMAIN="${1:-${API_DOMAIN:-}}"
LETSENCRYPT_EMAIL="${2:-${LETSENCRYPT_EMAIL:-}}"
BACKEND_UPSTREAM="${3:-${BACKEND_UPSTREAM:-http://127.0.0.1:8000}}"

if [[ -z "$API_DOMAIN" ]]; then
  echo "Usage: sudo ./scripts/setup_nginx_tls.sh <api-domain> <letsencrypt-email> [backend-upstream]"
  echo "Example: sudo ./scripts/setup_nginx_tls.sh api.accord-erp.com ops@accord-erp.com http://127.0.0.1:8000"
  exit 1
fi

if [[ -z "$LETSENCRYPT_EMAIL" ]]; then
  echo "[nginx-tls] LetsEncrypt email is required"
  exit 1
fi

if ! command -v apt-get >/dev/null 2>&1; then
  echo "[nginx-tls] This script currently supports Ubuntu/Debian (apt-get)."
  exit 1
fi

echo "[nginx-tls] Installing nginx and certbot"
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y nginx certbot python3-certbot-nginx

mkdir -p /etc/nginx/sites-available /etc/nginx/sites-enabled

if [[ -f "$SITE_AVAILABLE" ]]; then
  cp "$SITE_AVAILABLE" "$SITE_AVAILABLE.bak.$(date +%Y%m%d%H%M%S)"
fi

sed -e "s|__API_DOMAIN__|$API_DOMAIN|g" \
    -e "s|__BACKEND_UPSTREAM__|$BACKEND_UPSTREAM|g" \
    "$TEMPLATE_PATH" > "$SITE_AVAILABLE"

ln -sf "$SITE_AVAILABLE" "$SITE_ENABLED"
rm -f /etc/nginx/sites-enabled/default

nginx -t
systemctl enable nginx
systemctl restart nginx

echo "[nginx-tls] Requesting certificate for $API_DOMAIN"
certbot --nginx \
  -d "$API_DOMAIN" \
  --non-interactive \
  --agree-tos \
  --email "$LETSENCRYPT_EMAIL" \
  --redirect

nginx -t
systemctl reload nginx

echo "[nginx-tls] TLS setup complete"
echo "[nginx-tls] Validate: https://$API_DOMAIN/api/v1/health"
