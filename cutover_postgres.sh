#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

export DATABASE_URL="${DATABASE_URL:-postgresql+psycopg://accord:accord@localhost:5432/accord}"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
PIP_BIN="$ROOT_DIR/.venv/bin/pip"

echo "[Cutover] Installing PostgreSQL driver + deps..."
"$PIP_BIN" install -r cloud-backend/requirements.txt

echo "[Cutover] Checking Docker daemon..."
if ! docker info >/dev/null 2>&1; then
  echo "[Cutover] ERROR: Docker daemon is not running. Start Docker Desktop and re-run."
  exit 1
fi

echo "[Cutover] Starting postgres service..."
docker compose up -d postgres

echo "[Cutover] Waiting for postgres readiness..."
for i in {1..40}; do
  if docker compose exec -T postgres pg_isready -U accord -d accord >/dev/null 2>&1; then
    break
  fi
  sleep 2
  if [[ "$i" -eq 40 ]]; then
    echo "[Cutover] ERROR: postgres did not become ready in time."
    docker compose logs postgres --tail 120
    exit 1
  fi
done

echo "[Cutover] Running migration from SQLite -> PostgreSQL..."
"$PYTHON_BIN" cloud-backend/migrate_to_postgres.py --truncate-target --postgres-url "$DATABASE_URL"

echo "[Cutover] Verifying migration integrity..."
"$PYTHON_BIN" cloud-backend/verify_postgres_migration.py --postgres-url "$DATABASE_URL"

echo "\n[Cutover] SUCCESS"
echo "PostgreSQL now contains migrated Accord data."
echo "Next step: complete application SQL dialect refactor to make Postgres the live runtime backend."
