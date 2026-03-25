from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from workers.celery_app import celery


DB_PATH = Path(__file__).resolve().parents[1] / "ledger.db"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def _money(value: str | Decimal) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.0001"))


@celery.task(name="workers.rent_tasks.generate_monthly_rent_invoices")
def generate_monthly_rent_invoices() -> dict[str, int]:
    today = date.today()
    generated = 0

    with closing(_conn()) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leases (
                lease_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                monthly_rent TEXT NOT NULL,
                next_billing_date TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'ACTIVE'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rent_invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lease_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                invoice_date TEXT NOT NULL,
                due_date TEXT NOT NULL,
                amount TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN'
            )
            """
        )
        conn.commit()

        rows = conn.execute(
            """
            SELECT lease_id, tenant_id, monthly_rent, next_billing_date
            FROM leases
            WHERE status = 'ACTIVE' AND next_billing_date = ?
            """,
            (today.isoformat(),),
        ).fetchall()

        for lease in rows:
            monthly_rent = _money(str(lease["monthly_rent"]))
            next_billing = date.fromisoformat(str(lease["next_billing_date"]))
            next_month = (next_billing + timedelta(days=32)).replace(day=next_billing.day)
            due_date = today + timedelta(days=10)

            conn.execute("BEGIN")
            conn.execute(
                """
                INSERT INTO rent_invoices(lease_id, tenant_id, invoice_date, due_date, amount, status)
                VALUES (?, ?, ?, ?, ?, 'OPEN')
                """,
                (
                    str(lease["lease_id"]),
                    str(lease["tenant_id"]),
                    today.isoformat(),
                    due_date.isoformat(),
                    f"{monthly_rent:.4f}",
                ),
            )
            conn.execute(
                "UPDATE leases SET next_billing_date = ? WHERE lease_id = ?",
                (next_month.isoformat(), str(lease["lease_id"])),
            )
            conn.commit()
            generated += 1

    return {"generated_invoices": generated}
