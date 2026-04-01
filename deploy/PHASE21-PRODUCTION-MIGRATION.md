# Phase 21 Production Migration Runbook

## Scope
- TODO 111: PostgreSQL + Alembic baseline
- TODO 112: Redis caching/rate limiting
- TODO 113: Kubernetes manifests
- TODO 114: S3-compatible object storage
- TODO 115: Encrypted automated PostgreSQL backups

## 1) Local Setup
1. Configure env values in `cloud-backend/.env.example`.
2. Start local stack:
   - `docker compose up -d postgres redis backend frontend`
3. Ensure backend points to either:
   - PostgreSQL: `DATABASE_URL=postgresql://...`
   - SQLite fallback: `DATABASE_URL=sqlite:///...`

## 2) Alembic
From `cloud-backend`:
1. `alembic current`
2. `alembic upgrade head`

Notes:
- Runtime still preserves sqlite3 compatibility where existing ledger SQL paths rely on SQLite syntax.
- Alembic baseline currently provisions additive Phase 21 schema SQL files.

## 3) Redis Cache + Rate Limits
- Restock prediction endpoint now uses cache with safe fallback when Redis is unavailable.
- Heavy inventory endpoints apply IP-based limits using Redis sorted sets with local in-memory fallback.

## 4) Kubernetes Deploy Order
Apply in this order:
1. `deploy/k8s/namespace.yaml`
2. `deploy/k8s/configmap.yaml`
3. `deploy/k8s/secret.template.yaml` (copy + fill secrets)
4. `deploy/k8s/postgres.yaml`
5. `deploy/k8s/redis.yaml`
6. `deploy/k8s/backend.yaml`
7. `deploy/k8s/celery-worker.yaml`
8. `deploy/k8s/frontend.yaml`

Validation dry-run:
- `kubectl apply --dry-run=client -f deploy/k8s`

## 5) S3 Storage
- Set:
  - `ACCORD_STORAGE_BACKEND=s3`
  - `ACCORD_S3_BUCKET`
  - `ACCORD_S3_REGION`
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
- Demand letters and AI-vision scan images now upload through storage abstraction.

## 6) Backups (Every 6 Hours)
- Celery beat now includes task `workers.backup_tasks.run_postgres_backup` on 6-hour schedule.
- Backup flow:
  1. `pg_dump` custom format
  2. AES-GCM encryption
  3. Upload to storage backend key `db-backups/YYYY/MM/DD/...`

Dry-run command:
- `python cloud-backend/scripts/pg_backup.py --dry-run`

## 7) Restore Workflow (Operator)
1. Download encrypted backup object.
2. Decrypt with the same `ACCORD_BACKUP_ENCRYPTION_KEY`.
3. Restore:
   - `pg_restore --clean --if-exists --no-owner --no-privileges -d <target_db> <dump_file>`

## 8) Risk Notes
- Full ledger SQL dialect migration from SQLite to PostgreSQL remains incremental.
- Alembic baseline is additive and does not yet mirror every legacy runtime-created table in `main.py`.
