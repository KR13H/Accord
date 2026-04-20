from __future__ import annotations

import sqlite3
from decimal import Decimal
from typing import Any, Callable

DEFAULT_BUSINESS_ID = "SME-001"


def ensure_supplier_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sme_suppliers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id TEXT NOT NULL DEFAULT 'SME-001',
            name TEXT NOT NULL,
            phone TEXT,
            amount_owed NUMERIC NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_sme_suppliers_business_name
        ON sme_suppliers (business_id, name);
        """
    )


def _serialize_supplier(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": int(row["id"]),
        "business_id": str(row["business_id"]),
        "name": str(row["name"]),
        "phone": str(row["phone"]) if row["phone"] else None,
        "amount_owed": float(row["amount_owed"]),
    }


def add_supplier_bill(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    business_id: str | None,
    name: str,
    phone: str | None,
    amount: Decimal,
) -> dict[str, Any]:
    clean_business_id = (business_id or DEFAULT_BUSINESS_ID).strip() or DEFAULT_BUSINESS_ID
    clean_name = name.strip()
    clean_phone = (phone or "").strip() or None
    bill_amount = Decimal(str(amount))

    if not clean_name:
        raise ValueError("supplier name is required")
    if bill_amount <= 0:
        raise ValueError("amount must be greater than zero")

    with get_conn() as conn:
        ensure_supplier_schema(conn)
        existing = conn.execute(
            "SELECT id, business_id, name, phone, amount_owed FROM sme_suppliers WHERE business_id = ? AND lower(name) = lower(?)",
            (clean_business_id, clean_name),
        ).fetchone()

        if existing is None:
            cursor = conn.execute(
                """
                INSERT INTO sme_suppliers (business_id, name, phone, amount_owed)
                VALUES (?, ?, ?, ?)
                """,
                (clean_business_id, clean_name, clean_phone, f"{bill_amount:.4f}"),
            )
            row = conn.execute(
                "SELECT id, business_id, name, phone, amount_owed FROM sme_suppliers WHERE id = ?",
                (int(cursor.lastrowid),),
            ).fetchone()
            payload = _serialize_supplier(row)
            if payload is None:
                raise RuntimeError("failed to load supplier after create")
            return payload

        next_amount_owed = Decimal(str(existing["amount_owed"])) + bill_amount
        next_phone = clean_phone if clean_phone is not None else existing["phone"]
        conn.execute(
            "UPDATE sme_suppliers SET phone = ?, amount_owed = ? WHERE id = ?",
            (next_phone, f"{next_amount_owed:.4f}", int(existing["id"])),
        )
        updated = conn.execute(
            "SELECT id, business_id, name, phone, amount_owed FROM sme_suppliers WHERE id = ?",
            (int(existing["id"]),),
        ).fetchone()

    payload = _serialize_supplier(updated)
    if payload is None:
        raise RuntimeError("failed to load supplier after bill update")
    return payload


def record_supplier_payment(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    supplier_id: int,
    amount: Decimal,
) -> dict[str, Any]:
    payment_amount = Decimal(str(amount))
    if payment_amount <= 0:
        raise ValueError("amount must be greater than zero")

    with get_conn() as conn:
        ensure_supplier_schema(conn)
        row = conn.execute(
            "SELECT id, business_id, name, phone, amount_owed FROM sme_suppliers WHERE id = ?",
            (supplier_id,),
        ).fetchone()
        if row is None:
            raise ValueError("supplier not found")

        current_owed = Decimal(str(row["amount_owed"]))
        if payment_amount > current_owed:
            raise ValueError("payment amount cannot exceed current amount_owed")

        next_amount_owed = current_owed - payment_amount
        conn.execute(
            "UPDATE sme_suppliers SET amount_owed = ? WHERE id = ?",
            (f"{next_amount_owed:.4f}", supplier_id),
        )
        updated = conn.execute(
            "SELECT id, business_id, name, phone, amount_owed FROM sme_suppliers WHERE id = ?",
            (supplier_id,),
        ).fetchone()

    payload = _serialize_supplier(updated)
    if payload is None:
        raise RuntimeError("failed to load supplier after payment")
    return payload
