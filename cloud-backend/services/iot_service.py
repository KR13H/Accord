from __future__ import annotations

import sqlite3
from decimal import Decimal
from typing import Any, Callable

from services.sme_inventory_service import DEFAULT_BUSINESS_ID, ensure_inventory_schema
from services.universal_accounting import record_transaction
from websockets.sme_sync import fire_and_forget_business_event


VALID_MACHINE_STATUSES = {"DISPENSED", "HEARTBEAT", "ERROR"}


def process_iot_pulse(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    machine_id: str,
    item_sku: str,
    status: str,
    business_id: str | None,
) -> dict[str, Any]:
    clean_business_id = (business_id or DEFAULT_BUSINESS_ID).strip() or DEFAULT_BUSINESS_ID
    clean_machine_id = machine_id.strip()
    clean_item_sku = item_sku.strip()
    clean_status = status.strip().upper()

    if not clean_machine_id:
        raise ValueError("machine_id is required")
    if not clean_item_sku:
        raise ValueError("item_sku is required")
    if clean_status not in VALID_MACHINE_STATUSES:
        raise ValueError("status must be DISPENSED, HEARTBEAT, or ERROR")

    if clean_status != "DISPENSED":
        fire_and_forget_business_event(
            clean_business_id,
            {
                "event": "IOT_MACHINE_STATUS",
                "business_id": clean_business_id,
                "machine_id": clean_machine_id,
                "status": clean_status,
                "item_sku": clean_item_sku,
            },
        )
        return {
            "status": "ok",
            "business_id": clean_business_id,
            "machine_id": clean_machine_id,
            "item_sku": clean_item_sku,
            "machine_status": clean_status,
            "processed": False,
        }

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        ensure_inventory_schema(conn)
        row = conn.execute(
            """
            SELECT id, business_id, item_name, system_serial, factory_serial, current_stock, unit_price
            FROM sme_inventory_items
            WHERE business_id = ?
              AND (system_serial = ? OR factory_serial = ?)
            ORDER BY id ASC
            LIMIT 1
            """,
            (clean_business_id, clean_item_sku, clean_item_sku),
        ).fetchone()

    if row is None:
        raise ValueError("inventory item not found for machine SKU")

    current_stock = Decimal(str(row["current_stock"]))
    if current_stock <= 0:
        raise ValueError("cannot dispense item with zero stock")

    next_stock = current_stock - Decimal("1")

    with get_conn() as conn:
        conn.execute(
            "UPDATE sme_inventory_items SET current_stock = ? WHERE id = ?",
            (f"{next_stock:.4f}", int(row["id"])),
        )

    sale_amount = Decimal(str(row["unit_price"]))
    transaction = record_transaction(
        get_conn,
        business_id=clean_business_id,
        tx_type="INCOME",
        amount=sale_amount,
        category=f"Unattended Sale - {str(row['item_name'])}",
        payment_method="UPI",
    )

    event_payload = {
        "event": "UNATTENDED_SALE",
        "business_id": clean_business_id,
        "machine_id": clean_machine_id,
        "item_id": int(row["id"]),
        "item_name": str(row["item_name"]),
        "item_sku": clean_item_sku,
        "new_stock": float(next_stock),
        "transaction_id": int(transaction["id"]),
        "amount": float(sale_amount),
    }
    fire_and_forget_business_event(clean_business_id, event_payload)

    return {
        "status": "ok",
        "processed": True,
        "business_id": clean_business_id,
        "machine_id": clean_machine_id,
        "item_sku": clean_item_sku,
        "item_id": int(row["id"]),
        "item_name": str(row["item_name"]),
        "new_stock": float(next_stock),
        "transaction": transaction,
    }
