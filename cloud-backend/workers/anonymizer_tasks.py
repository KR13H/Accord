from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import datetime, timedelta
import os
from pathlib import Path
from typing import Any

from services.sme_credit_service import ensure_customer_schema
from services.universal_accounting import ensure_sme_schema
from workers.celery_app import celery


def _resolve_sqlite_db_path() -> str:
    raw = os.getenv("DATABASE_URL", os.getenv("ACCORD_DATABASE_URL", "sqlite:///ledger.db")).strip()
    if raw.startswith("sqlite:///"):
        path_value = raw.replace("sqlite:///", "", 1)
        return str(Path(path_value).expanduser())
    return str((Path(__file__).resolve().parents[1] / "ledger.db"))


def _get_conn() -> sqlite3.Connection:
    db_path = _resolve_sqlite_db_path()
    conn = sqlite3.connect(db_path, timeout=15.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def _mask_phone(phone_number: str) -> str:
    digits = "".join(ch for ch in phone_number if ch.isdigit())
    if len(digits) < 3:
        return "+91-XXXXX-XX000"
    return f"+91-XXXXX-XX{digits[-3:]}"


@celery.task(name="workers.anonymizer_tasks.scrub_inactive_pii")
def scrub_inactive_pii() -> dict[str, Any]:
    cutoff = (datetime.utcnow() - timedelta(days=365 * 3)).date().isoformat()

    with closing(_get_conn()) as conn:
        ensure_customer_schema(conn)
        ensure_sme_schema(conn)

        stale_rows = conn.execute(
            """
            SELECT c.id, c.phone
            FROM sme_customers c
            LEFT JOIN (
                SELECT business_id, MAX(date(created_at)) AS last_tx_date
                FROM sme_transactions
                GROUP BY business_id
            ) t ON t.business_id = c.business_id
            WHERE COALESCE(t.last_tx_date, '1970-01-01') < ?
            """,
            (cutoff,),
        ).fetchall()

        anonymized = 0
        for row in stale_rows:
            customer_id = int(row["id"])
            masked_phone = _mask_phone(str(row["phone"] or ""))
            conn.execute(
                "UPDATE sme_customers SET name = ?, phone = ? WHERE id = ?",
                ("Anonymized User", masked_phone, customer_id),
            )
            anonymized += 1

        conn.commit()

    return {
        "status": "ok",
        "cutoff_date": cutoff,
        "anonymized_count": anonymized,
    }
