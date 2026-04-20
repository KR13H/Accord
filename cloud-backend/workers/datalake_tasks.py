from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

from workers.celery_app import celery

DB_URL = os.getenv("DATABASE_URL", "sqlite:////app/ledger.db")
DATALAKE_DIR = Path(os.getenv("ACCORD_DATALAKE_DIR", str(Path(__file__).resolve().parents[1] / "datalake")))


def _resolve_sqlite_path(database_url: str) -> Path:
    if not database_url.startswith("sqlite:///"):
        return Path(__file__).resolve().parents[1] / "ledger.db"
    raw = database_url.replace("sqlite:///", "", 1)
    path = Path(raw)
    if path.is_absolute():
        return path
    return Path(__file__).resolve().parents[1] / raw


@celery.task(name="workers.datalake_tasks.export_to_parquet")
def export_to_parquet() -> dict[str, str | int]:
    sqlite_path = _resolve_sqlite_path(DB_URL)
    if not sqlite_path.exists():
        return {
            "status": "skipped",
            "detail": f"SQLite database not found at {sqlite_path}",
        }

    DATALAKE_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    with sqlite3.connect(sqlite_path) as conn:
        journal_df = pd.read_sql_query("SELECT * FROM journal_entries", conn)
        bookings_df = pd.read_sql_query("SELECT * FROM sales_bookings", conn)

    journal_path = DATALAKE_DIR / f"journal_entries_{stamp}.parquet"
    bookings_path = DATALAKE_DIR / f"sales_bookings_{stamp}.parquet"
    journal_df.to_parquet(journal_path, index=False)
    bookings_df.to_parquet(bookings_path, index=False)

    return {
        "status": "ok",
        "rows_journal_entries": int(len(journal_df.index)),
        "rows_sales_bookings": int(len(bookings_df.index)),
        "journal_entries_file": str(journal_path),
        "sales_bookings_file": str(bookings_path),
    }
