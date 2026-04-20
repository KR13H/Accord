from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Any, Callable

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field


class BrokerLeadIn(BaseModel):
    rera_registration_number: str = Field(min_length=5, max_length=64)
    customer_name: str = Field(min_length=2, max_length=180)
    project_id: str = Field(min_length=2, max_length=64)


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def ensure_broker_lead_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id TEXT NOT NULL UNIQUE,
            broker_id TEXT NOT NULL,
            rera_registration_number TEXT NOT NULL,
            customer_name TEXT NOT NULL,
            customer_key TEXT NOT NULL,
            project_id TEXT NOT NULL,
            linked_booking_id TEXT NULL,
            status TEXT NOT NULL DEFAULT 'REGISTERED' CHECK(status IN ('REGISTERED', 'CONVERTED')),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_broker_leads_lookup
        ON broker_leads(project_id, customer_key, status, created_at DESC)
        """
    )


def _customer_key(name: str) -> str:
    return " ".join(name.strip().lower().split())


def _next_lead_id(conn: sqlite3.Connection) -> tuple[str, str]:
    row = conn.execute("SELECT COUNT(1) AS c FROM broker_leads").fetchone()
    seq = int((row["c"] if row is not None else 0) or 0) + 1
    return f"BRL-{seq:06d}", f"BROKER-{seq:06d}"


def auto_link_broker_lead(
    conn: sqlite3.Connection,
    *,
    project_id: str,
    customer_name: str,
    booking_id: str,
) -> str | None:
    ensure_broker_lead_schema(conn)
    row = conn.execute(
        """
        SELECT lead_id, broker_id
        FROM broker_leads
        WHERE project_id = ?
          AND customer_key = ?
          AND status = 'REGISTERED'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (project_id.strip(), _customer_key(customer_name)),
    ).fetchone()
    if row is None:
        return None

    now = now_iso()
    conn.execute(
        """
        UPDATE broker_leads
        SET status = 'CONVERTED',
            linked_booking_id = ?,
            updated_at = ?
        WHERE lead_id = ?
        """,
        (booking_id.strip(), now, row["lead_id"]),
    )
    return str(row["broker_id"])


def create_broker_router(get_conn: Callable[[], sqlite3.Connection]) -> APIRouter:
    router = APIRouter(prefix="/api/v1/brokers", tags=["brokers", "lead-gen"])

    @router.post("/register")
    def post_broker_lead(payload: BrokerLeadIn) -> dict[str, Any]:
        clean_customer = payload.customer_name.strip()
        clean_project = payload.project_id.strip()
        clean_rera = payload.rera_registration_number.strip().upper()

        now = now_iso()
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            ensure_broker_lead_schema(conn)
            lead_id, broker_id = _next_lead_id(conn)
            conn.execute(
                """
                INSERT INTO broker_leads(
                    lead_id,
                    broker_id,
                    rera_registration_number,
                    customer_name,
                    customer_key,
                    project_id,
                    linked_booking_id,
                    status,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, NULL, 'REGISTERED', ?, ?)
                """,
                (
                    lead_id,
                    broker_id,
                    clean_rera,
                    clean_customer,
                    _customer_key(clean_customer),
                    clean_project,
                    now,
                    now,
                ),
            )
            conn.commit()

        return {
            "status": "ok",
            "lead_id": lead_id,
            "broker_id": broker_id,
            "project_id": clean_project,
            "customer_name": clean_customer,
            "state": "REGISTERED",
        }

    @router.get("/leads")
    def get_broker_leads(limit: int = 100) -> dict[str, Any]:
        safe_limit = max(1, min(limit, 500))
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            ensure_broker_lead_schema(conn)
            rows = conn.execute(
                """
                SELECT lead_id, broker_id, rera_registration_number, customer_name, project_id,
                       linked_booking_id, status, created_at, updated_at
                FROM broker_leads
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()

        return {"status": "ok", "count": len(rows), "items": [dict(row) for row in rows]}

    return router
