from __future__ import annotations

import sqlite3
from decimal import Decimal
from typing import Any, Callable


DEFAULT_BUSINESS_ID = "SME-001"


def ensure_inventory_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sme_inventory_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id TEXT NOT NULL DEFAULT 'SME-001',
            item_name TEXT NOT NULL,
            sku TEXT,
            current_stock NUMERIC NOT NULL DEFAULT 0,
            minimum_stock_level NUMERIC NOT NULL DEFAULT 0,
            unit_price NUMERIC NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_sme_inventory_business_sku
        ON sme_inventory_items (business_id, sku);
        """
    )


def _serialize_item(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": int(row["id"]),
        "business_id": str(row["business_id"]),
        "item_name": str(row["item_name"]),
        "sku": str(row["sku"] or ""),
        "current_stock": float(row["current_stock"]),
        "minimum_stock_level": float(row["minimum_stock_level"]),
        "unit_price": float(row["unit_price"]),
    }


def create_item(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    business_id: str | None,
    item_name: str,
    sku: str | None,
    current_stock: Decimal,
    minimum_stock_level: Decimal,
    unit_price: Decimal,
) -> dict[str, Any]:
    clean_business_id = (business_id or DEFAULT_BUSINESS_ID).strip() or DEFAULT_BUSINESS_ID
    clean_name = item_name.strip()
    clean_sku = (sku or "").strip()
    if not clean_name:
        raise ValueError("item_name is required")

    for label, value in {
        "current_stock": current_stock,
        "minimum_stock_level": minimum_stock_level,
        "unit_price": unit_price,
    }.items():
        if Decimal(str(value)) < 0:
            raise ValueError(f"{label} cannot be negative")

    with get_conn() as conn:
        ensure_inventory_schema(conn)
        cursor = conn.execute(
            """
            INSERT INTO sme_inventory_items (
                business_id, item_name, sku, current_stock, minimum_stock_level, unit_price
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                clean_business_id,
                clean_name,
                clean_sku,
                f"{Decimal(str(current_stock)):.4f}",
                f"{Decimal(str(minimum_stock_level)):.4f}",
                f"{Decimal(str(unit_price)):.4f}",
            ),
        )
        row = conn.execute(
            "SELECT id, business_id, item_name, sku, current_stock, minimum_stock_level, unit_price FROM sme_inventory_items WHERE id = ?",
            (int(cursor.lastrowid),),
        ).fetchone()

    payload = _serialize_item(row)
    if payload is None:
        raise RuntimeError("failed to load inventory item after create")
    return payload


def list_items(get_conn: Callable[[], sqlite3.Connection], *, business_id: str | None) -> list[dict[str, Any]]:
    clean_business_id = (business_id or DEFAULT_BUSINESS_ID).strip() or DEFAULT_BUSINESS_ID
    with get_conn() as conn:
        ensure_inventory_schema(conn)
        rows = conn.execute(
            """
            SELECT id, business_id, item_name, sku, current_stock, minimum_stock_level, unit_price
            FROM sme_inventory_items
            WHERE business_id = ?
            ORDER BY item_name ASC, id ASC
            """,
            (clean_business_id,),
        ).fetchall()
    return [_serialize_item(row) for row in rows if row is not None]


def get_item(get_conn: Callable[[], sqlite3.Connection], *, item_id: int) -> dict[str, Any] | None:
    with get_conn() as conn:
        ensure_inventory_schema(conn)
        row = conn.execute(
            "SELECT id, business_id, item_name, sku, current_stock, minimum_stock_level, unit_price FROM sme_inventory_items WHERE id = ?",
            (item_id,),
        ).fetchone()
    return _serialize_item(row)


def update_item(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    item_id: int,
    item_name: str | None,
    sku: str | None,
    current_stock: Decimal | None,
    minimum_stock_level: Decimal | None,
    unit_price: Decimal | None,
) -> dict[str, Any] | None:
    with get_conn() as conn:
        ensure_inventory_schema(conn)
        row = conn.execute(
            "SELECT id, item_name, sku, current_stock, minimum_stock_level, unit_price FROM sme_inventory_items WHERE id = ?",
            (item_id,),
        ).fetchone()
        if row is None:
            return None

        next_item_name = (item_name.strip() if item_name is not None else str(row["item_name"]))
        next_sku = (sku.strip() if sku is not None else str(row["sku"] or ""))
        next_current_stock = Decimal(str(current_stock)) if current_stock is not None else Decimal(str(row["current_stock"]))
        next_minimum_stock = (
            Decimal(str(minimum_stock_level)) if minimum_stock_level is not None else Decimal(str(row["minimum_stock_level"]))
        )
        next_unit_price = Decimal(str(unit_price)) if unit_price is not None else Decimal(str(row["unit_price"]))

        if not next_item_name:
            raise ValueError("item_name is required")
        if next_current_stock < 0 or next_minimum_stock < 0 or next_unit_price < 0:
            raise ValueError("stock levels and unit_price cannot be negative")

        conn.execute(
            """
            UPDATE sme_inventory_items
            SET item_name = ?, sku = ?, current_stock = ?, minimum_stock_level = ?, unit_price = ?
            WHERE id = ?
            """,
            (
                next_item_name,
                next_sku,
                f"{next_current_stock:.4f}",
                f"{next_minimum_stock:.4f}",
                f"{next_unit_price:.4f}",
                item_id,
            ),
        )

        updated = conn.execute(
            "SELECT id, business_id, item_name, sku, current_stock, minimum_stock_level, unit_price FROM sme_inventory_items WHERE id = ?",
            (item_id,),
        ).fetchone()

    return _serialize_item(updated)


def delete_item(get_conn: Callable[[], sqlite3.Connection], *, item_id: int) -> bool:
    with get_conn() as conn:
        ensure_inventory_schema(conn)
        cursor = conn.execute("DELETE FROM sme_inventory_items WHERE id = ?", (item_id,))
    return int(cursor.rowcount or 0) > 0


def get_low_stock_alerts(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    business_id: str | None,
) -> list[dict[str, Any]]:
    clean_business_id = (business_id or DEFAULT_BUSINESS_ID).strip() or DEFAULT_BUSINESS_ID
    with get_conn() as conn:
        ensure_inventory_schema(conn)
        rows = conn.execute(
            """
            SELECT id, business_id, item_name, sku, current_stock, minimum_stock_level, unit_price
            FROM sme_inventory_items
            WHERE business_id = ?
              AND CAST(current_stock AS REAL) <= CAST(minimum_stock_level AS REAL)
            ORDER BY current_stock ASC, item_name ASC
            """,
            (clean_business_id,),
        ).fetchall()
    return [_serialize_item(row) for row in rows if row is not None]
