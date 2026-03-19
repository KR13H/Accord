#!/bin/bash

set -euo pipefail

ROOT_DIR="/Users/krish/Developer/Accord"
BACKEND_DIR="$ROOT_DIR/cloud-backend"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"

if ! command -v cloudflared >/dev/null 2>&1; then
  echo "cloudflared is not installed. Install with: brew install cloudflared"
  exit 1
fi

if [ ! -x "$PYTHON_BIN" ]; then
  echo "Python venv not found at $PYTHON_BIN"
  exit 1
fi

echo "Starting Accord backend (sovereign-local mode)..."
cd "$BACKEND_DIR"
ACCORD_DEPLOYMENT_MODE=sovereign-local \
BACKEND_PUBLIC_URL=http://localhost:8000 \
FRONTEND_PUBLIC_URL=http://localhost:5173 \
"$PYTHON_BIN" -m uvicorn main:app --host 0.0.0.0 --port 8000 &
BACKEND_PID=$!

cleanup() {
  if ps -p "$BACKEND_PID" >/dev/null 2>&1; then
    kill "$BACKEND_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

sleep 2

echo "Opening Cloudflare tunnel to local backend..."
cloudflared tunnel --url http://localhost:8000
