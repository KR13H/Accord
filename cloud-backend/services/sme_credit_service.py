from __future__ import annotations

import sqlite3
from decimal import Decimal
from typing import Any, Callable


DEFAULT_BUSINESS_ID = "SME-001"


def ensure_customer_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sme_customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id TEXT NOT NULL DEFAULT 'SME-001',
            name TEXT NOT NULL,
            phone TEXT,
            outstanding_balance NUMERIC NOT NULL DEFAULT 0 CHECK (outstanding_balance >= 0)
        );

        CREATE INDEX IF NOT EXISTS idx_sme_customers_business
        ON sme_customers (business_id, name);
        """
    )


def _serialize_customer(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": int(row["id"]),
        "business_id": str(row["business_id"]),
        "name": str(row["name"]),
        "phone": str(row["phone"] or ""),
        "outstanding_balance": f"{Decimal(str(row['outstanding_balance'])):.2f}",
    }


def create_customer(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    business_id: str | None,
    name: str,
    phone: str | None,
) -> dict[str, Any]:
    clean_business_id = (business_id or DEFAULT_BUSINESS_ID).strip() or DEFAULT_BUSINESS_ID
    clean_name = name.strip()
    clean_phone = (phone or "").strip()
    if not clean_name:
        raise ValueError("name is required")

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        ensure_customer_schema(conn)
        cursor = conn.execute(
            """
            INSERT INTO sme_customers (business_id, name, phone, outstanding_balance)
            VALUES (?, ?, ?, 0)
            """,
            (clean_business_id, clean_name, clean_phone),
        )
        row = conn.execute(
            "SELECT id, business_id, name, phone, outstanding_balance FROM sme_customers WHERE id = ?",
            (int(cursor.lastrowid),),
        ).fetchone()

    payload = _serialize_customer(row)
    if payload is None:
        raise RuntimeError("failed to load customer after insert")
    return payload


def adjust_balance(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    customer_id: int,
    amount: Decimal,
    mode: str,
) -> dict[str, Any]:
    normalized_amount = Decimal(str(amount))
    if normalized_amount <= 0:
        raise ValueError("amount must be greater than 0")
    if mode not in {"charge", "settle"}:
        raise ValueError("mode must be charge or settle")

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        ensure_customer_schema(conn)
        row = conn.execute(
            "SELECT id, business_id, name, phone, outstanding_balance FROM sme_customers WHERE id = ?",
            (customer_id,),
        ).fetchone()
        if row is None:
            raise ValueError("customer not found")

        current_balance = Decimal(str(row["outstanding_balance"]))
        if mode == "charge":
            new_balance = current_balance + normalized_amount
        else:
            new_balance = current_balance - normalized_amount
            if new_balance < 0:
                raise ValueError("settlement amount exceeds outstanding balance")

        conn.execute(
            "UPDATE sme_customers SET outstanding_balance = ? WHERE id = ?",
            (f"{new_balance:.2f}", customer_id),
        )
        updated_row = conn.execute(
            "SELECT id, business_id, name, phone, outstanding_balance FROM sme_customers WHERE id = ?",
            (customer_id,),
        ).fetchone()

    payload = _serialize_customer(updated_row)
    if payload is None:
        raise RuntimeError("failed to load customer after balance update")
    return payload
