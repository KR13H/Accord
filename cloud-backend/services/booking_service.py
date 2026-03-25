from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


ALLOWED_STATUS = {"ACTIVE", "CANCELLED", "COMPLETED"}
MONEY_QUANT = Decimal("0.0001")


@dataclass(frozen=True)
class BookingRecord:
    booking_id: str
    project_id: str
    spv_id: str
    customer_name: str
    unit_code: str
    customer_id: str
    broker_id: str
    currency_code: str
    foreign_amount: str
    foreign_meta: str
    total_consideration: str
    booking_date: str
    status: str
    created_at: str
    updated_at: str


class BookingService:
    def __init__(self, *, get_conn: callable) -> None:
        self.get_conn = get_conn

    def ensure_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sales_bookings (
                booking_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                spv_id TEXT NOT NULL DEFAULT 'SPV-DEFAULT',
                customer_name TEXT,
                unit_code TEXT,
                customer_id TEXT,
                broker_id TEXT,
                currency_code TEXT NOT NULL DEFAULT 'INR',
                foreign_amount TEXT,
                foreign_meta TEXT,
                total_consideration TEXT NOT NULL DEFAULT '0.0000',
                booking_date TEXT,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        self._add_column_if_missing(conn, "sales_bookings", "spv_id", "TEXT NOT NULL DEFAULT 'SPV-DEFAULT'")
        self._add_column_if_missing(conn, "sales_bookings", "total_consideration", "TEXT NOT NULL DEFAULT '0.0000'")
        self._add_column_if_missing(conn, "sales_bookings", "booking_date", "TEXT")
        self._add_column_if_missing(conn, "sales_bookings", "customer_id", "TEXT")
        self._add_column_if_missing(conn, "sales_bookings", "broker_id", "TEXT")
        self._add_column_if_missing(conn, "sales_bookings", "currency_code", "TEXT NOT NULL DEFAULT 'INR'")
        self._add_column_if_missing(conn, "sales_bookings", "foreign_amount", "TEXT")
        self._add_column_if_missing(conn, "sales_bookings", "foreign_meta", "TEXT")
        conn.commit()

    def _add_column_if_missing(self, conn: sqlite3.Connection, table_name: str, column_name: str, ddl: str) -> None:
        cols = conn.execute(f"PRAGMA table_info({table_name});").fetchall()
        if any(str(row["name"]) == column_name for row in cols):
            return
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl};")

    def _parse_consideration(self, value: Any) -> str:
        parsed = Decimal(str(value)).quantize(MONEY_QUANT)
        if parsed < 0:
            raise ValueError("total_consideration must be non-negative")
        return f"{parsed:.4f}"

    def _normalize_status(self, status: str) -> str:
        normalized = status.strip().upper()
        if normalized not in ALLOWED_STATUS:
            raise ValueError("status must be ACTIVE, CANCELLED, or COMPLETED")
        return normalized

    def create_booking(self, booking_data: dict[str, Any]) -> dict[str, Any]:
        now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        booking_id = str(booking_data["booking_id"]).strip()
        project_id = str(booking_data["project_id"]).strip()
        spv_id = str(booking_data.get("spv_id") or "SPV-DEFAULT").strip()
        customer_name = str(booking_data["customer_name"]).strip()
        unit_code = str(booking_data["unit_code"]).strip()
        customer_id = str(booking_data.get("customer_id") or booking_id).strip()
        broker_id = str(booking_data.get("broker_id") or "").strip() or None
        currency_code = str(booking_data.get("currency_code") or "INR").strip().upper() or "INR"
        foreign_amount = booking_data.get("foreign_amount")
        foreign_amount_str = self._parse_consideration(foreign_amount) if foreign_amount is not None else None
        foreign_meta = booking_data.get("foreign_meta")
        foreign_meta_json = json.dumps(foreign_meta, separators=(",", ":")) if foreign_meta is not None else None
        total_consideration = self._parse_consideration(booking_data.get("total_consideration", "0"))
        booking_date = str(booking_data.get("booking_date") or datetime.utcnow().date().isoformat()).strip()
        status = self._normalize_status(str(booking_data.get("status") or "ACTIVE"))

        with self.get_conn() as conn:
            conn.row_factory = sqlite3.Row
            self.ensure_schema(conn)

            existing_unit = conn.execute(
                """
                SELECT booking_id
                FROM sales_bookings
                WHERE project_id = ? AND unit_code = ? AND status = 'ACTIVE'
                LIMIT 1
                """,
                (project_id, unit_code),
            ).fetchone()
            if existing_unit is not None:
                raise ValueError("unit_code is already booked for this project")

            conn.execute(
                """
                INSERT INTO sales_bookings(
                    booking_id, project_id, spv_id, customer_name, unit_code,
                    customer_id, broker_id, currency_code, foreign_amount, foreign_meta, total_consideration,
                    booking_date, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    booking_id,
                    project_id,
                    spv_id,
                    customer_name,
                    unit_code,
                    customer_id,
                    broker_id,
                    currency_code,
                    foreign_amount_str,
                    foreign_meta_json,
                    total_consideration,
                    booking_date,
                    status,
                    now_iso,
                    now_iso,
                ),
            )

            conn.execute(
                """
                INSERT INTO audit_edit_logs(table_name, record_id, user_id, action, old_value, new_value, created_at)
                VALUES('sales_bookings', 0, 0, 'booking_created', NULL, ?, ?)
                """,
                (
                    str(
                        {
                            "booking_id": booking_id,
                            "project_id": project_id,
                            "spv_id": spv_id,
                            "customer_id": customer_id,
                            "broker_id": broker_id,
                            "currency_code": currency_code,
                            "foreign_amount": foreign_amount_str,
                            "status": status,
                        }
                    ),
                    now_iso,
                ),
            )
            conn.commit()

            return self.get_booking(booking_id) or {}

    def get_booking(self, booking_id: str) -> dict[str, Any] | None:
        key = booking_id.strip()
        with self.get_conn() as conn:
            conn.row_factory = sqlite3.Row
            self.ensure_schema(conn)
            row = conn.execute(
                """
                SELECT booking_id, project_id, spv_id, customer_name, unit_code,
                      customer_id, broker_id, currency_code, foreign_amount, foreign_meta, total_consideration, booking_date, status, created_at, updated_at
                FROM sales_bookings
                WHERE booking_id = ?
                """,
                (key,),
            ).fetchone()
        if row is None:
            return None
        payload = dict(row)
        if payload.get("foreign_meta"):
            try:
                payload["foreign_meta"] = json.loads(str(payload["foreign_meta"]))
            except Exception:
                payload["foreign_meta"] = None
        return payload

    def list_bookings(self, *, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        safe_limit = max(1, min(limit, 500))
        with self.get_conn() as conn:
            conn.row_factory = sqlite3.Row
            self.ensure_schema(conn)
            if status:
                rows = conn.execute(
                    """
                    SELECT booking_id, project_id, spv_id, customer_name, unit_code,
                              customer_id, broker_id, currency_code, foreign_amount, foreign_meta, total_consideration, booking_date, status, created_at, updated_at
                    FROM sales_bookings
                    WHERE status = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (self._normalize_status(status), safe_limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT booking_id, project_id, spv_id, customer_name, unit_code,
                              customer_id, broker_id, currency_code, foreign_amount, foreign_meta, total_consideration, booking_date, status, created_at, updated_at
                    FROM sales_bookings
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (safe_limit,),
                ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            if payload.get("foreign_meta"):
                try:
                    payload["foreign_meta"] = json.loads(str(payload["foreign_meta"]))
                except Exception:
                    payload["foreign_meta"] = None
            items.append(payload)
        return items

    def update_booking(self, booking_id: str, patch_data: dict[str, Any]) -> dict[str, Any] | None:
        key = booking_id.strip()
        updates: dict[str, Any] = {}
        if "project_id" in patch_data and patch_data["project_id"] is not None:
            updates["project_id"] = str(patch_data["project_id"]).strip()
        if "spv_id" in patch_data and patch_data["spv_id"] is not None:
            updates["spv_id"] = str(patch_data["spv_id"]).strip()
        if "customer_name" in patch_data and patch_data["customer_name"] is not None:
            updates["customer_name"] = str(patch_data["customer_name"]).strip()
        if "unit_code" in patch_data and patch_data["unit_code"] is not None:
            updates["unit_code"] = str(patch_data["unit_code"]).strip()
        if "customer_id" in patch_data and patch_data["customer_id"] is not None:
            updates["customer_id"] = str(patch_data["customer_id"]).strip()
        if "broker_id" in patch_data and patch_data["broker_id"] is not None:
            updates["broker_id"] = str(patch_data["broker_id"]).strip()
        if "currency_code" in patch_data and patch_data["currency_code"] is not None:
            updates["currency_code"] = str(patch_data["currency_code"]).strip().upper()
        if "foreign_amount" in patch_data and patch_data["foreign_amount"] is not None:
            updates["foreign_amount"] = self._parse_consideration(patch_data["foreign_amount"])
        if "foreign_meta" in patch_data and patch_data["foreign_meta"] is not None:
            updates["foreign_meta"] = json.dumps(patch_data["foreign_meta"], separators=(",", ":"))
        if "total_consideration" in patch_data and patch_data["total_consideration"] is not None:
            updates["total_consideration"] = self._parse_consideration(patch_data["total_consideration"])
        if "booking_date" in patch_data and patch_data["booking_date"] is not None:
            updates["booking_date"] = str(patch_data["booking_date"]).strip()
        if "status" in patch_data and patch_data["status"] is not None:
            updates["status"] = self._normalize_status(str(patch_data["status"]))

        if not updates:
            raise ValueError("at least one field must be provided")

        updates["updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        values = list(updates.values()) + [key]

        with self.get_conn() as conn:
            conn.row_factory = sqlite3.Row
            self.ensure_schema(conn)
            existing = conn.execute("SELECT booking_id FROM sales_bookings WHERE booking_id = ?", (key,)).fetchone()
            if existing is None:
                return None
            conn.execute(f"UPDATE sales_bookings SET {set_clause} WHERE booking_id = ?", values)
            conn.commit()

        return self.get_booking(key)

    def cancel_booking(self, booking_id: str) -> dict[str, Any] | None:
        return self.update_booking(booking_id, {"status": "CANCELLED"})
