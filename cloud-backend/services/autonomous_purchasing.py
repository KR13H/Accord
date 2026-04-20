from __future__ import annotations

import base64
import json
import os
import secrets
import sqlite3
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_UP
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable

import httpx

from services.sme_inventory_service import DEFAULT_BUSINESS_ID, ensure_inventory_schema
from services.sme_payable_service import ensure_supplier_schema
from services.whatsapp_receipt_service import send_purchase_order_approval

try:
    from workers.celery_app import celery
except Exception:  # noqa: BLE001
    celery = None


OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
AUTO_REORDER_MODEL = os.getenv("ACCORD_AUTOREORDER_MODEL", "llama3")
BACKEND_PUBLIC_URL = os.getenv("BACKEND_PUBLIC_URL", "http://127.0.0.1:8000").rstrip("/")
PURCHASE_ORDER_OWNER_PHONE = os.getenv("ACCORD_WHATSAPP_OWNER_PHONE", os.getenv("ACCORD_WHATSAPP_DEFAULT_TO", "919999999999"))
PURCHASE_ORDER_APPROVAL_TTL_SECONDS = int(os.getenv("ACCORD_PURCHASE_ORDER_APPROVAL_TTL_SECONDS", "21600"))


def _sqlite_db_path() -> Path:
    database_url = os.getenv("DATABASE_URL", f"sqlite:///{Path(__file__).resolve().parents[1] / 'ledger.db'}")
    if not database_url.startswith("sqlite:///"):
        return Path(__file__).resolve().parents[1] / "ledger.db"
    raw = database_url.replace("sqlite:///", "", 1)
    path = Path(raw)
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[1] / raw
    return path


def _task_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_sqlite_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def ensure_purchase_order_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sme_purchase_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id TEXT NOT NULL DEFAULT 'SME-001',
            supplier_name TEXT NOT NULL,
            supplier_phone TEXT,
            low_stock_items_json TEXT NOT NULL,
            supplier_history_json TEXT NOT NULL,
            draft_message TEXT NOT NULL,
            approval_token TEXT NOT NULL UNIQUE,
            approval_url TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING_APPROVAL' CHECK(status IN ('PENDING_APPROVAL', 'APPROVED', 'REJECTED', 'SENT', 'FAILED')),
            whatsapp_status TEXT NOT NULL DEFAULT 'PENDING',
            owner_phone TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            approved_at TEXT,
            sent_at TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_sme_purchase_orders_business_status
        ON sme_purchase_orders (business_id, status, created_at DESC);
        """
    )


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _normalize_text(value: str) -> str:
    return " ".join(value.strip().split())


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


def _fetch_low_stock_items(conn: sqlite3.Connection, business_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, item_name, current_stock, minimum_stock_level, unit_price, factory_serial, system_serial, is_system_generated
        FROM sme_inventory_items
        WHERE business_id = ?
          AND CAST(current_stock AS REAL) < CAST(minimum_stock_level AS REAL)
        ORDER BY CAST(current_stock AS REAL) ASC, item_name ASC
        """,
        (business_id,),
    ).fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        current_stock = Decimal(str(row["current_stock"]))
        minimum_stock = Decimal(str(row["minimum_stock_level"]))
        shortage = (minimum_stock - current_stock).quantize(Decimal("1"), rounding=ROUND_UP)
        items.append(
            {
                "item_id": int(row["id"]),
                "item_name": str(row["item_name"]),
                "current_stock": float(current_stock),
                "minimum_stock_level": float(minimum_stock),
                "shortage_units": int(max(shortage, Decimal("1"))),
                "unit_price": float(Decimal(str(row["unit_price"]))),
                "factory_serial": str(row["factory_serial"]) if row["factory_serial"] else None,
                "system_serial": str(row["system_serial"]),
                "is_system_generated": bool(row["is_system_generated"]),
            }
        )
    return items


def _fetch_supplier_history(conn: sqlite3.Connection, business_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, name, phone, amount_owed
        FROM sme_suppliers
        WHERE business_id = ?
        ORDER BY CAST(amount_owed AS REAL) DESC, name ASC
        LIMIT 10
        """,
        (business_id,),
    ).fetchall()

    return [
        {
            "supplier_id": int(row["id"]),
            "name": str(row["name"]),
            "phone": str(row["phone"]) if row["phone"] else None,
            "amount_owed": float(row["amount_owed"]),
        }
        for row in rows
    ]


def _fetch_recent_orders(conn: sqlite3.Connection, business_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT id, supplier_name, status, draft_message, created_at, updated_at
        FROM sme_purchase_orders
        WHERE business_id = ?
        ORDER BY id DESC
        LIMIT 8
        """,
        (business_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _select_supplier(
    conn: sqlite3.Connection,
    business_id: str,
) -> dict[str, Any]:
    suppliers = _fetch_supplier_history(conn, business_id)
    if suppliers:
        chosen = suppliers[0]
        return {
            "name": chosen["name"],
            "phone": chosen["phone"],
            "history": suppliers,
        }

    return {
        "name": os.getenv("ACCORD_DEFAULT_SUPPLIER_NAME", "Preferred Wholesaler").strip() or "Preferred Wholesaler",
        "phone": os.getenv("ACCORD_DEFAULT_SUPPLIER_PHONE", "").strip() or None,
        "history": suppliers,
    }


def _draft_message_with_ollama(
    *,
    supplier_name: str,
    low_stock_items: list[dict[str, Any]],
    supplier_history: list[dict[str, Any]],
) -> str:
    item_summary = ", ".join(
        f"{item['shortage_units']} units of {item['item_name']}" for item in low_stock_items[:5]
    )
    system_prompt = (
        "You are an Indian hardware store owner negotiating on WhatsApp. "
        "Draft a polite but firm Hinglish message to your supplier ordering [X] units of [Item]. "
        "Ask for a 5% bulk discount because you are a loyal customer. Keep it under 3 sentences."
    )
    user_prompt = _json_dump(
        {
            "supplier_name": supplier_name,
            "low_stock_items": low_stock_items,
            "supplier_history": supplier_history,
            "required_order_summary": item_summary,
        }
    )

    try:
        with httpx.Client(timeout=45.0) as client:
            response = client.post(
                f"{OLLAMA_HOST}/api/generate",
                json={
                    "model": AUTO_REORDER_MODEL,
                    "prompt": f"{system_prompt}\n\nContext: {user_prompt}",
                    "stream": False,
                },
            )
            response.raise_for_status()
            payload = response.json()
        draft = str(payload.get("response", "")).strip()
        if draft:
            return draft
    except Exception:  # noqa: BLE001
        pass

    item_lines = "; ".join(item_summary.split(", "))
    return (
        f"Namaste {supplier_name}, Accord ko {item_lines} urgently chahiye. "
        f"Please 5% bulk discount de dijiye as we are a loyal customer. "
        f"Confirmation bhej dijiye so we can place the order today."
    )


def _insert_purchase_order(
    conn: sqlite3.Connection,
    *,
    business_id: str,
    supplier_name: str,
    supplier_phone: str | None,
    low_stock_items: list[dict[str, Any]],
    supplier_history: list[dict[str, Any]],
    draft_message: str,
    approval_token: str,
) -> dict[str, Any]:
    ensure_purchase_order_schema(conn)
    approval_url = f"{BACKEND_PUBLIC_URL}/api/v1/sme/purchasing/approval/{approval_token}"
    now = _now_iso()
    cursor = conn.execute(
        """
        INSERT INTO sme_purchase_orders (
            business_id,
            supplier_name,
            supplier_phone,
            low_stock_items_json,
            supplier_history_json,
            draft_message,
            approval_token,
            approval_url,
            status,
            whatsapp_status,
            owner_phone,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING_APPROVAL', 'PENDING', ?, ?, ?)
        """,
        (
            business_id,
            supplier_name,
            supplier_phone,
            _json_dump(low_stock_items),
            _json_dump(supplier_history),
            draft_message,
            approval_token,
            approval_url,
            PURCHASE_ORDER_OWNER_PHONE or None,
            now,
            now,
        ),
    )
    order_id = int(cursor.lastrowid)
    row = conn.execute(
        "SELECT * FROM sme_purchase_orders WHERE id = ?",
        (order_id,),
    ).fetchone()
    if row is None:
        raise RuntimeError("failed to read purchase order after insert")
    return dict(row)


def _auto_reorder_critical_stock_impl(
    *,
    get_conn: Callable[[], sqlite3.Connection],
    business_id: str | None,
) -> dict[str, Any]:
    clean_business_id = (business_id or DEFAULT_BUSINESS_ID).strip() or DEFAULT_BUSINESS_ID

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        ensure_inventory_schema(conn)
        ensure_supplier_schema(conn)
        ensure_purchase_order_schema(conn)

        low_stock_items = _fetch_low_stock_items(conn, clean_business_id)
        if not low_stock_items:
            return {
                "status": "ok",
                "business_id": clean_business_id,
                "low_stock_items": [],
                "purchase_order": None,
                "message": "No critical low stock items found.",
            }

        supplier = _select_supplier(conn, clean_business_id)
        supplier_history = {
            "supplier_history": supplier.get("history", []),
            "recent_orders": _fetch_recent_orders(conn, clean_business_id),
        }
        draft_message = _draft_message_with_ollama(
            supplier_name=str(supplier["name"]),
            low_stock_items=low_stock_items,
            supplier_history=supplier_history["supplier_history"],
        )

        approval_token = secrets.token_urlsafe(24)
        purchase_order = _insert_purchase_order(
            conn,
            business_id=clean_business_id,
            supplier_name=str(supplier["name"]),
            supplier_phone=supplier.get("phone"),
            low_stock_items=low_stock_items,
            supplier_history=supplier_history["supplier_history"],
            draft_message=draft_message,
            approval_token=approval_token,
        )

        approval_result = send_purchase_order_approval(
            supplier_name=str(supplier["name"]),
            draft_message=draft_message,
            approval_url=str(purchase_order["approval_url"]),
            to_phone=PURCHASE_ORDER_OWNER_PHONE,
        )

        conn.execute(
            """
            UPDATE sme_purchase_orders
            SET whatsapp_status = ?,
                status = CASE WHEN ? = 'SENT' THEN 'PENDING_APPROVAL' ELSE status END,
                sent_at = CASE WHEN ? IN ('SENT', 'MOCKED') THEN ? ELSE sent_at END,
                updated_at = ?
            WHERE id = ?
            """,
            (
                str(approval_result.get("status", "PENDING")),
                str(approval_result.get("status", "")),
                str(approval_result.get("status", "")),
                _now_iso(),
                _now_iso(),
                int(purchase_order["id"]),
            ),
        )
        conn.commit()

        updated = conn.execute(
            "SELECT * FROM sme_purchase_orders WHERE id = ?",
            (int(purchase_order["id"]),),
        ).fetchone()

    return {
        "status": "ok",
        "business_id": clean_business_id,
        "low_stock_items": low_stock_items,
        "purchase_order": dict(updated) if updated is not None else purchase_order,
        "whatsapp": approval_result,
    }


def approve_purchase_order_by_token(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    approval_token: str,
) -> dict[str, Any]:
    token = approval_token.strip()
    if not token:
        raise ValueError("approval_token is required")

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        ensure_purchase_order_schema(conn)
        row = conn.execute(
            "SELECT * FROM sme_purchase_orders WHERE approval_token = ?",
            (token,),
        ).fetchone()
        if row is None:
            raise ValueError("purchase order not found")

        if str(row["status"]) == "APPROVED":
            return {"status": "ok", "purchase_order": dict(row), "already_approved": True}

        now = _now_iso()
        conn.execute(
            """
            UPDATE sme_purchase_orders
            SET status = 'APPROVED',
                approved_at = ?,
                updated_at = ?
            WHERE approval_token = ?
            """,
            (now, now, token),
        )
        updated = conn.execute(
            "SELECT * FROM sme_purchase_orders WHERE approval_token = ?",
            (token,),
        ).fetchone()
        conn.commit()

    return {"status": "ok", "purchase_order": dict(updated) if updated is not None else dict(row), "already_approved": False}


def run_auto_reorder_critical_stock(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    business_id: str | None,
) -> dict[str, Any]:
    return _auto_reorder_critical_stock_impl(get_conn=get_conn, business_id=business_id)


if celery is not None:

    @celery.task(name="services.autonomous_purchasing.auto_reorder_critical_stock")
    def auto_reorder_critical_stock(business_id: str = DEFAULT_BUSINESS_ID) -> dict[str, Any]:
        return _auto_reorder_critical_stock_impl(get_conn=_task_conn, business_id=business_id)

else:

    def auto_reorder_critical_stock(business_id: str = DEFAULT_BUSINESS_ID) -> dict[str, Any]:
        return _auto_reorder_critical_stock_impl(get_conn=_task_conn, business_id=business_id)