from __future__ import annotations

import os
import sqlite3
import tarfile
from datetime import datetime
from pathlib import Path

from services.postgres_backup_service import run_pg_backup
from utils.db_runtime import is_postgres_url
from workers.celery_app import celery

DB_URL = os.getenv("DATABASE_URL", "sqlite:////app/ledger.db")
BACKUP_DIR = Path(os.getenv("ACCORD_BACKUP_DIR", str(Path(__file__).resolve().parents[1] / "backups")))
RETENTION_DAYS = int(os.getenv("ACCORD_BACKUP_RETENTION_DAYS", "7"))


def _resolve_sqlite_path(database_url: str) -> Path:
    if not database_url.startswith("sqlite:///"):
        return Path(__file__).resolve().parents[1] / "ledger.db"

    path = Path(database_url.replace("sqlite:///", "", 1))
    if path.is_absolute():
        return path
    return Path(__file__).resolve().parents[1] / path


def _create_sqlite_snapshot(source_path: Path, snapshot_path: Path) -> None:
    with sqlite3.connect(source_path) as source_conn, sqlite3.connect(snapshot_path) as dest_conn:
        source_conn.backup(dest_conn)


def _prune_old_backups(target_dir: Path, keep_latest: int) -> int:
    archives = sorted(target_dir.glob("ledger-backup-*.tar.gz"), key=lambda file: file.stat().st_mtime, reverse=True)
    removed = 0
    for old_file in archives[keep_latest:]:
        old_file.unlink(missing_ok=True)
        removed += 1
    return removed


@celery.task(name="workers.backup_tasks.run_daily_sqlite_backup")
def run_daily_sqlite_backup() -> dict[str, str | int]:
    sqlite_path = _resolve_sqlite_path(DB_URL)
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    if not sqlite_path.exists():
        return {
            "status": "skipped",
            "detail": f"SQLite database not found at {sqlite_path}",
        }

    stamp = datetime.utcnow().date().isoformat()
    temp_snapshot = BACKUP_DIR / f"ledger-snapshot-{stamp}.db"
    archive_path = BACKUP_DIR / f"ledger-backup-{stamp}.tar.gz"

    if temp_snapshot.exists():
        temp_snapshot.unlink()

    _create_sqlite_snapshot(sqlite_path, temp_snapshot)

    try:
        with tarfile.open(archive_path, mode="w:gz") as tar:
            tar.add(temp_snapshot, arcname=f"ledger-{stamp}.db")
    finally:
        temp_snapshot.unlink(missing_ok=True)

    removed_count = _prune_old_backups(BACKUP_DIR, max(1, RETENTION_DAYS))

    return {
        "status": "ok",
        "archive": str(archive_path),
        "retention_days": RETENTION_DAYS,
        "removed_old_backups": removed_count,
    }


@celery.task(name="workers.backup_tasks.run_postgres_backup")
def run_postgres_backup() -> dict[str, str | int]:
    if not is_postgres_url(DB_URL):
        return {
            "status": "skipped",
            "detail": "DATABASE_URL is not PostgreSQL",
        }

    result = run_pg_backup(dry_run=False)
    return {
        "status": str(result.get("status", "error")),
        "detail": str(result.get("detail", "")),
        "key": str(result.get("key", "")),
        "uri": str(result.get("uri", "")),
    }
