from __future__ import annotations

import asyncio
import hashlib
import sqlite3
import time
from decimal import Decimal
from typing import Any, Callable

from services.translation_service import translate_item_name
from websockets.sme_sync import fire_and_forget_business_event


DEFAULT_BUSINESS_ID = "SME-001"


def _generate_system_serial() -> str:
    """Generate a unique ACC-ITM-{hash} serial."""
    timestamp = str(time.time_ns()).encode()
    hash_obj = hashlib.md5(timestamp)
    short_hash = hash_obj.hexdigest()[:6].upper()
    return f"ACC-ITM-{short_hash}"


def _translate_localized_name(item_name: str) -> str:
    clean_name = item_name.strip()
    if not clean_name:
        return ""

    try:
        return asyncio.run(translate_item_name(clean_name, target_language="hi"))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(translate_item_name(clean_name, target_language="hi"))
        finally:
            loop.close()


def ensure_inventory_schema(conn: sqlite3.Connection) -> None:
    # Check if table exists and what columns it has
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='sme_inventory_items'"
    )
    table_exists = cursor.fetchone() is not None
    
    if table_exists:
        # Check if new columns exist
        cursor = conn.execute("PRAGMA table_info(sme_inventory_items)")
        columns = {row[1] for row in cursor.fetchall()}
        
        if "system_serial" not in columns:
            # Migrate old schema to new schema
            conn.executescript(
                """
                -- Create new table with new schema
                CREATE TABLE sme_inventory_items_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    business_id TEXT NOT NULL DEFAULT 'SME-001',
                    item_name TEXT NOT NULL,
                    localized_name TEXT,
                    factory_serial TEXT,
                    system_serial TEXT NOT NULL UNIQUE,
                    is_system_generated BOOLEAN NOT NULL DEFAULT 0,
                    current_stock NUMERIC NOT NULL DEFAULT 0,
                    minimum_stock_level NUMERIC NOT NULL DEFAULT 0,
                    unit_price NUMERIC NOT NULL DEFAULT 0
                );

                -- Migrate data: old 'sku' becomes factory_serial, generate system_serial
                INSERT INTO sme_inventory_items_new (
                    id, business_id, item_name, localized_name, factory_serial, system_serial,
                    is_system_generated, current_stock, minimum_stock_level, unit_price
                )
                SELECT
                    id, business_id, item_name, item_name, sku, CASE WHEN sku IS NULL OR sku = '' THEN ('ACC-ITM-' || SUBSTR(HEX(RANDOMBLOB(3)), 1, 6)) ELSE sku END,
                    CASE WHEN sku IS NULL OR sku = '' THEN 1 ELSE 0 END,
                    current_stock, minimum_stock_level, unit_price
                FROM sme_inventory_items;

                -- Drop old table and rename new one
                DROP TABLE sme_inventory_items;
                ALTER TABLE sme_inventory_items_new RENAME TO sme_inventory_items;

                -- Recreate indices
                CREATE INDEX IF NOT EXISTS idx_sme_inventory_business_sku
                ON sme_inventory_items (business_id, system_serial);
                
                CREATE INDEX IF NOT EXISTS idx_sme_inventory_system_generated
                ON sme_inventory_items (business_id, is_system_generated);
                """
            )
    else:
        # Create new table with new schema
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS sme_inventory_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_id TEXT NOT NULL DEFAULT 'SME-001',
                item_name TEXT NOT NULL,
                localized_name TEXT,
                factory_serial TEXT,
                system_serial TEXT NOT NULL UNIQUE,
                is_system_generated BOOLEAN NOT NULL DEFAULT 0,
                current_stock NUMERIC NOT NULL DEFAULT 0,
                minimum_stock_level NUMERIC NOT NULL DEFAULT 0,
                unit_price NUMERIC NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_sme_inventory_business_sku
            ON sme_inventory_items (business_id, system_serial);

            CREATE INDEX IF NOT EXISTS idx_sme_inventory_system_generated
            ON sme_inventory_items (business_id, is_system_generated);
            """
        )

    cursor = conn.execute("PRAGMA table_info(sme_inventory_items)")
    columns = {row[1] for row in cursor.fetchall()}
    if "localized_name" not in columns:
        conn.execute("ALTER TABLE sme_inventory_items ADD COLUMN localized_name TEXT;")
        conn.execute("UPDATE sme_inventory_items SET localized_name = item_name WHERE localized_name IS NULL;")


def _serialize_item(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": int(row["id"]),
        "business_id": str(row["business_id"]),
        "item_name": str(row["item_name"]),
        "localized_name": str(row["localized_name"] or row["item_name"]),
        "factory_serial": str(row["factory_serial"]) if row["factory_serial"] else None,
        "system_serial": str(row["system_serial"]),
        "is_system_generated": bool(row["is_system_generated"]),
        "current_stock": float(row["current_stock"]),
        "minimum_stock_level": float(row["minimum_stock_level"]),
        "unit_price": float(row["unit_price"]),
    }


def create_item(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    business_id: str | None,
    item_name: str,
    factory_serial: str | None,
    current_stock: Decimal,
    minimum_stock_level: Decimal,
    unit_price: Decimal,
) -> dict[str, Any]:
    clean_business_id = (business_id or DEFAULT_BUSINESS_ID).strip() or DEFAULT_BUSINESS_ID
    clean_name = item_name.strip()
    clean_factory_serial = (factory_serial or "").strip() if factory_serial else None
    
    if not clean_name:
        raise ValueError("item_name is required")

    for label, value in {
        "current_stock": current_stock,
        "minimum_stock_level": minimum_stock_level,
        "unit_price": unit_price,
    }.items():
        if Decimal(str(value)) < 0:
            raise ValueError(f"{label} cannot be negative")

    # Determine system_serial and is_system_generated
    if clean_factory_serial:
        system_serial = clean_factory_serial
        is_system_generated = False
    else:
        system_serial = _generate_system_serial()
        is_system_generated = True

    localized_name = _translate_localized_name(clean_name)

    with get_conn() as conn:
        ensure_inventory_schema(conn)
        cursor = conn.execute(
            """
            INSERT INTO sme_inventory_items (
                business_id, item_name, localized_name, factory_serial, system_serial, is_system_generated,
                current_stock, minimum_stock_level, unit_price
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                clean_business_id,
                clean_name,
                localized_name,
                clean_factory_serial,
                system_serial,
                is_system_generated,
                f"{Decimal(str(current_stock)):.4f}",
                f"{Decimal(str(minimum_stock_level)):.4f}",
                f"{Decimal(str(unit_price)):.4f}",
            ),
        )
        row = conn.execute(
            "SELECT id, business_id, item_name, localized_name, factory_serial, system_serial, is_system_generated, current_stock, minimum_stock_level, unit_price FROM sme_inventory_items WHERE id = ?",
            (int(cursor.lastrowid),),
        ).fetchone()

    payload = _serialize_item(row)
    if payload is None:
        raise RuntimeError("failed to load inventory item after create")
    return payload


def list_items(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    business_id: str | None,
    factory_serial: str | None = None,
) -> list[dict[str, Any]]:
    clean_business_id = (business_id or DEFAULT_BUSINESS_ID).strip() or DEFAULT_BUSINESS_ID
    serial = (factory_serial or "").strip()
    with get_conn() as conn:
        ensure_inventory_schema(conn)
        if serial:
            rows = conn.execute(
                """
                SELECT id, business_id, item_name, localized_name, factory_serial, system_serial, is_system_generated, current_stock, minimum_stock_level, unit_price
                FROM sme_inventory_items
                WHERE business_id = ?
                  AND (factory_serial = ? OR system_serial = ?)
                ORDER BY item_name ASC, id ASC
                """,
                (clean_business_id, serial, serial),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, business_id, item_name, localized_name, factory_serial, system_serial, is_system_generated, current_stock, minimum_stock_level, unit_price
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
            "SELECT id, business_id, item_name, localized_name, factory_serial, system_serial, is_system_generated, current_stock, minimum_stock_level, unit_price FROM sme_inventory_items WHERE id = ?",
            (item_id,),
        ).fetchone()
    return _serialize_item(row)


def update_item(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    item_id: int,
    item_name: str | None,
    factory_serial: str | None,
    current_stock: Decimal | None,
    minimum_stock_level: Decimal | None,
    unit_price: Decimal | None,
) -> dict[str, Any] | None:
    with get_conn() as conn:
        ensure_inventory_schema(conn)
        row = conn.execute(
            "SELECT id, item_name, localized_name, factory_serial, system_serial, is_system_generated, current_stock, minimum_stock_level, unit_price FROM sme_inventory_items WHERE id = ?",
            (item_id,),
        ).fetchone()
        if row is None:
            return None

        next_item_name = (item_name.strip() if item_name is not None else str(row["item_name"]))
        next_localized_name = (
            _translate_localized_name(next_item_name)
            if item_name is not None
            else str(row["localized_name"] or row["item_name"])
        )
        next_factory_serial = (factory_serial.strip() if factory_serial is not None else (str(row["factory_serial"]) if row["factory_serial"] else None))
        
        # If factory_serial is being cleared, generate a new system_serial
        current_factory_serial = row["factory_serial"]
        if factory_serial is not None and factory_serial.strip() == "" and current_factory_serial:
            # Clearing factory_serial, need to generate new system_serial
            next_system_serial = _generate_system_serial()
            next_is_system_generated = True
        elif factory_serial is not None and factory_serial.strip() and not current_factory_serial:
            # Adding factory_serial where none existed, update system_serial to match
            next_system_serial = factory_serial.strip()
            next_is_system_generated = False
        else:
            # Keep existing system_serial and generation status
            next_system_serial = str(row["system_serial"])
            next_is_system_generated = bool(row["is_system_generated"])
        
        next_current_stock = Decimal(str(current_stock)) if current_stock is not None else Decimal(str(row["current_stock"]))
        next_minimum_stock = (
            Decimal(str(minimum_stock_level)) if minimum_stock_level is not None else Decimal(str(row["minimum_stock_level"]))
        )
        next_unit_price = Decimal(str(unit_price)) if unit_price is not None else Decimal(str(row["unit_price"]))

        if not next_item_name:
            raise ValueError("item_name is required")
        if next_current_stock < 0 or next_minimum_stock < 0 or next_unit_price < 0:
            raise ValueError("stock levels and unit_price cannot be negative")

        previous_stock = Decimal(str(row["current_stock"]))

        conn.execute(
            """
            UPDATE sme_inventory_items
            SET item_name = ?, localized_name = ?, factory_serial = ?, system_serial = ?, is_system_generated = ?, current_stock = ?, minimum_stock_level = ?, unit_price = ?
            WHERE id = ?
            """,
            (
                next_item_name,
                next_localized_name,
                next_factory_serial,
                next_system_serial,
                next_is_system_generated,
                f"{next_current_stock:.4f}",
                f"{next_minimum_stock:.4f}",
                f"{next_unit_price:.4f}",
                item_id,
            ),
        )

        updated = conn.execute(
            "SELECT id, business_id, item_name, localized_name, factory_serial, system_serial, is_system_generated, current_stock, minimum_stock_level, unit_price FROM sme_inventory_items WHERE id = ?",
            (item_id,),
        ).fetchone()

    payload = _serialize_item(updated)
    if payload is not None and next_current_stock != previous_stock:
        fire_and_forget_business_event(
            str(payload["business_id"]),
            {
                "event": "INVENTORY_UPDATED",
                "business_id": str(payload["business_id"]),
                "item_id": int(payload["id"]),
                "new_stock": float(payload["current_stock"]),
                "item_name": str(payload["item_name"]),
            },
        )

    return payload


def update_stock_count(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    item_id: int,
    counted_stock: Decimal,
) -> dict[str, Any] | None:
    next_stock = Decimal(str(counted_stock))
    if next_stock < 0:
        raise ValueError("counted_stock cannot be negative")

    with get_conn() as conn:
        ensure_inventory_schema(conn)
        row = conn.execute(
            "SELECT id, business_id, current_stock FROM sme_inventory_items WHERE id = ?",
            (item_id,),
        ).fetchone()
        if row is None:
            return None

        conn.execute(
            "UPDATE sme_inventory_items SET current_stock = ? WHERE id = ?",
            (f"{next_stock:.4f}", item_id),
        )

        updated = conn.execute(
            "SELECT id, business_id, item_name, localized_name, factory_serial, system_serial, is_system_generated, current_stock, minimum_stock_level, unit_price FROM sme_inventory_items WHERE id = ?",
            (item_id,),
        ).fetchone()

    payload = _serialize_item(updated)
    if payload is None:
        return None

    fire_and_forget_business_event(
        str(payload["business_id"]),
        {
            "event": "INVENTORY_UPDATED",
            "business_id": str(payload["business_id"]),
            "item_id": int(payload["id"]),
            "new_stock": float(payload["current_stock"]),
            "item_name": str(payload["item_name"]),
        },
    )
    return payload


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
                        SELECT id, business_id, item_name, localized_name, factory_serial, system_serial, is_system_generated, current_stock, minimum_stock_level, unit_price
            FROM sme_inventory_items
            WHERE business_id = ?
              AND CAST(current_stock AS REAL) <= CAST(minimum_stock_level AS REAL)
            ORDER BY current_stock ASC, item_name ASC
            """,
            (clean_business_id,),
        ).fetchall()
    return [_serialize_item(row) for row in rows if row is not None]
