from __future__ import annotations

import sqlite3
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable


DEFAULT_BUSINESS_ID = "SME-001"
VALID_TRANSACTION_TYPES = {"INCOME", "EXPENSE"}
VALID_PAYMENT_METHODS = {"Cash", "UPI"}


def ensure_sme_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sme_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id TEXT NOT NULL DEFAULT 'SME-001',
            type TEXT NOT NULL CHECK (type IN ('INCOME', 'EXPENSE')),
            amount NUMERIC NOT NULL CHECK (amount > 0),
            category TEXT NOT NULL,
            payment_method TEXT NOT NULL CHECK (payment_method IN ('Cash', 'UPI')),
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_sme_transactions_business_date
        ON sme_transactions (business_id, created_at DESC);
        """
    )


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": int(row["id"]),
        "business_id": str(row["business_id"]),
        "type": str(row["type"]),
        "amount": float(row["amount"]),
        "category": str(row["category"]),
        "payment_method": str(row["payment_method"]),
        "created_at": str(row["created_at"]),
    }


def record_transaction(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    business_id: str | None,
    tx_type: str,
    amount: Decimal,
    category: str,
    payment_method: str,
) -> dict[str, Any]:
    clean_business_id = (business_id or DEFAULT_BUSINESS_ID).strip() or DEFAULT_BUSINESS_ID
    clean_type = tx_type.strip().upper()
    clean_category = category.strip() or "General"
    raw_payment_method = payment_method.strip()

    if clean_type not in VALID_TRANSACTION_TYPES:
        raise ValueError("type must be INCOME or EXPENSE")
    if raw_payment_method.upper() == "UPI":
        clean_payment_method = "UPI"
    elif raw_payment_method.lower() == "cash":
        clean_payment_method = "Cash"
    else:
        clean_payment_method = raw_payment_method

    if clean_payment_method not in VALID_PAYMENT_METHODS:
        raise ValueError("payment_method must be Cash or UPI")

    normalized_amount = Decimal(str(amount))
    if normalized_amount <= 0:
        raise ValueError("amount must be greater than 0")

    created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        ensure_sme_schema(conn)
        cursor = conn.execute(
            """
            INSERT INTO sme_transactions (business_id, type, amount, category, payment_method, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                clean_business_id,
                clean_type,
                f"{normalized_amount:.2f}",
                clean_category,
                clean_payment_method,
                created_at,
            ),
        )
        created = conn.execute(
            "SELECT id, business_id, type, amount, category, payment_method, created_at FROM sme_transactions WHERE id = ?",
            (int(cursor.lastrowid),),
        ).fetchone()
    payload = _row_to_dict(created)
    if payload is None:
        raise RuntimeError("failed to read inserted transaction")
    return payload


def get_daily_summary(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    business_id: str | None,
    target_date: date | None = None,
) -> dict[str, Any]:
    clean_business_id = (business_id or DEFAULT_BUSINESS_ID).strip() or DEFAULT_BUSINESS_ID
    # Transactions are stored in UTC; default summary date should match UTC day boundaries.
    day = (target_date or datetime.utcnow().date()).isoformat()

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        ensure_sme_schema(conn)
        rows = conn.execute(
            """
            SELECT type, amount FROM sme_transactions
            WHERE business_id = ? AND date(created_at) = ?
            """,
            (clean_business_id, day),
        ).fetchall()

    income_total = Decimal("0.00")
    expense_total = Decimal("0.00")
    for row in rows:
        amount = Decimal(str(row["amount"]))
        if str(row["type"]).upper() == "INCOME":
            income_total += amount
        else:
            expense_total += amount

    net_total = income_total - expense_total
    return {
        "business_id": clean_business_id,
        "date": day,
        "income_total": f"{income_total:.2f}",
        "expense_total": f"{expense_total:.2f}",
        "net_total": f"{net_total:.2f}",
        "transaction_count": len(rows),
    }


def get_transactions_between(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    business_id: str | None,
    start_date: date,
    end_date: date,
) -> list[dict[str, Any]]:
    clean_business_id = (business_id or DEFAULT_BUSINESS_ID).strip() or DEFAULT_BUSINESS_ID
    if start_date > end_date:
        raise ValueError("start_date must be on or before end_date")

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        ensure_sme_schema(conn)
        rows = conn.execute(
            """
            SELECT id, business_id, type, amount, category, payment_method, created_at
            FROM sme_transactions
            WHERE business_id = ?
              AND date(created_at) >= ?
              AND date(created_at) <= ?
            ORDER BY created_at ASC, id ASC
            """,
            (clean_business_id, start_date.isoformat(), end_date.isoformat()),
        ).fetchall()

    return [_row_to_dict(row) for row in rows if row is not None]
