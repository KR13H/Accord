#!/bin/bash

set -euo pipefail

ROOT_DIR="/Users/krish/Developer/Accord"
FRONTEND_DIR="$ROOT_DIR/friday-insights"

RENDER_URL_DEFAULT="https://accord-logic.onrender.com"

if ! command -v node >/dev/null 2>&1; then
  echo "Node.js is required for frontend build/deploy."
  exit 1
fi

if [ ! -f "$FRONTEND_DIR/package.json" ]; then
  echo "Frontend package.json not found in $FRONTEND_DIR"
  exit 1
fi

echo "Building frontend for deployment..."
cd "$FRONTEND_DIR"
npm run build

echo ""
echo "Cloud deployment assets are ready."
echo ""
echo "1) Backend on Render"
echo "   - Push branch with render.yaml"
echo "   - In Render, create service from repo root"
echo "   - Set DATABASE_URL to your managed Postgres or keep sqlite fallback"
echo "   - Expected backend URL: $RENDER_URL_DEFAULT"
echo ""
echo "2) Frontend on Vercel"
echo "   - Import project using friday-insights directory"
echo "   - vercel.json already proxies /api to $RENDER_URL_DEFAULT"
echo ""
echo "3) Verify deployment"
echo "   - GET $RENDER_URL_DEFAULT/api/v1/health"
echo "   - GET $RENDER_URL_DEFAULT/api/v1/system/deployment-info"
echo ""
echo "4) Custom domain"
echo "   - Attach domain in Vercel and/or Render settings"
echo ""
echo "Done."
