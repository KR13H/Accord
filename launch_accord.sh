#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

echo "[Accord] Starting launch sequence..."

if [[ ! -x "$ROOT_DIR/.venv/bin/python" ]]; then
  echo "[Accord] Python virtualenv not found at .venv. Creating..."
  python3 -m venv .venv
fi

PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
PIP_BIN="$ROOT_DIR/.venv/bin/pip"

echo "[Accord] Installing backend dependencies..."
"$PIP_BIN" install --upgrade pip
"$PIP_BIN" install -r cloud-backend/requirements.txt

echo "[Accord] Installing frontend dependencies..."
cd "$ROOT_DIR/friday-insights"
npm install
cd "$ROOT_DIR"

if ! command -v ollama >/dev/null 2>&1; then
  echo "[Accord] ERROR: Ollama is not installed. Install from https://ollama.com/download"
  exit 1
fi

echo "[Accord] Ensuring Ollama model is available (llama3.2)..."
if ! ollama list 2>/dev/null | awk '{print $1}' | grep -Eq '^llama3\.2(:|$)'; then
  echo "[Accord] llama3.2 not found locally. Pulling model..."
  ollama pull llama3.2 >/dev/null
else
  echo "[Accord] llama3.2 already present."
fi

if ! curl -fsS "http://localhost:11434/api/tags" >/dev/null 2>&1; then
  echo "[Accord] Starting Ollama service in background..."
  nohup ollama serve >/tmp/accord-ollama.log 2>&1 &
  sleep 2
fi

echo "[Accord] Launching Docker stack (Postgres + Backend + Frontend)..."
if docker info >/dev/null 2>&1; then
  if [[ "${FORCE_BUILD:-0}" == "1" ]]; then
    echo "[Accord] FORCE_BUILD=1 set. Rebuilding images..."
    docker compose up --build -d
  else
    docker compose up -d
  fi
  FRONTEND_URL="http://localhost:3000"
  BACKEND_URL="http://localhost:8000"
else
  echo "[Accord] Docker daemon unavailable. Falling back to local runtime..."
  (
    cd "$ROOT_DIR/cloud-backend"
    OLLAMA_HOST="http://localhost:11434" nohup "$PYTHON_BIN" -m uvicorn main:app --host 0.0.0.0 --port 8000 >"$ROOT_DIR/cloud-backend/backend.log" 2>&1 &
  )
  (
    cd "$ROOT_DIR/friday-insights"
    nohup npm run dev -- --host 0.0.0.0 --port 3000 >"$ROOT_DIR/friday-insights/frontend.log" 2>&1 &
  )
  FRONTEND_URL="http://localhost:3000"
  BACKEND_URL="http://localhost:8000"
fi

echo "[Accord] Waiting for backend health..."
for i in {1..40}; do
  if curl -fsS "http://localhost:8000/api/v1/health" >/dev/null 2>&1; then
    break
  fi
  sleep 2
  if [[ "$i" -eq 40 ]]; then
    echo "[Accord] ERROR: Backend did not become healthy in time."
    if docker info >/dev/null 2>&1; then
      docker compose logs backend --tail 120
    else
      tail -n 120 "$ROOT_DIR/cloud-backend/backend.log" || true
    fi
    exit 1
  fi
done

echo "[Accord] Checking Friday engine health..."
curl -fsS "http://localhost:8000/api/v1/insights/friday-health" || true

echo

echo "[Accord] LIVE"
echo "Frontend: ${FRONTEND_URL}"
echo "Backend Docs: ${BACKEND_URL}/docs"
echo "Backend Health: ${BACKEND_URL}/api/v1/health"
echo "Friday Health: ${BACKEND_URL}/api/v1/insights/friday-health"
