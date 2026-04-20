from __future__ import annotations

import sqlite3
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable

from fastapi import APIRouter, Header, HTTPException

APPROVAL_THRESHOLD = Decimal("5000000")
PENDING = "PENDING_APPROVAL"
APPROVED = "APPROVED"
REJECTED = "REJECTED"


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def ensure_approval_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS allocation_approvals (
            allocation_event_id INTEGER PRIMARY KEY,
            maker_admin_id INTEGER NOT NULL,
            checker_admin_id INTEGER NULL,
            status TEXT NOT NULL CHECK(status IN ('PENDING_APPROVAL', 'APPROVED', 'REJECTED')),
            decision_reason TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            decision_at TEXT NULL,
            FOREIGN KEY(allocation_event_id) REFERENCES rera_allocation_events(id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_allocation_approvals_status
        ON allocation_approvals(status, updated_at DESC)
        """
    )


def initialize_allocation_approval(
    conn: sqlite3.Connection,
    *,
    allocation_event_id: int,
    maker_admin_id: int,
    receipt_amount: Decimal,
) -> str:
    ensure_approval_schema(conn)
    created = now_iso()
    status = PENDING if Decimal(str(receipt_amount)) > APPROVAL_THRESHOLD else APPROVED

    conn.execute(
        """
        INSERT INTO allocation_approvals(
            allocation_event_id,
            maker_admin_id,
            checker_admin_id,
            status,
            decision_reason,
            created_at,
            updated_at,
            decision_at
        ) VALUES (?, ?, NULL, ?, NULL, ?, ?, ?)
        ON CONFLICT(allocation_event_id) DO UPDATE SET
            maker_admin_id = excluded.maker_admin_id,
            status = excluded.status,
            updated_at = excluded.updated_at,
            decision_at = excluded.decision_at,
            decision_reason = excluded.decision_reason,
            checker_admin_id = allocation_approvals.checker_admin_id
        """,
        (
            allocation_event_id,
            maker_admin_id,
            status,
            created,
            created,
            created if status == APPROVED else None,
        ),
    )
    return status


def get_allocation_approval_status(conn: sqlite3.Connection, allocation_event_id: int) -> str:
    ensure_approval_schema(conn)
    row = conn.execute(
        "SELECT status FROM allocation_approvals WHERE allocation_event_id = ?",
        (allocation_event_id,),
    ).fetchone()
    if row is None:
        return APPROVED
    return str(row["status"])


def _fetch_allocation_for_decision(conn: sqlite3.Connection, allocation_event_id: int) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT a.allocation_event_id,
               a.maker_admin_id,
               a.checker_admin_id,
               a.status,
               e.booking_id,
               e.payment_reference,
               e.receipt_amount
        FROM allocation_approvals a
        JOIN rera_allocation_events e ON e.id = a.allocation_event_id
        WHERE a.allocation_event_id = ?
        """,
        (allocation_event_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="allocation approval record not found")
    return row


def approve_allocation(
    conn: sqlite3.Connection,
    *,
    allocation_event_id: int,
    checker_admin_id: int,
) -> dict[str, Any]:
    ensure_approval_schema(conn)
    row = _fetch_allocation_for_decision(conn, allocation_event_id)

    if int(row["maker_admin_id"]) == checker_admin_id:
        raise HTTPException(status_code=403, detail="maker and checker must be different users")

    if str(row["status"]) == REJECTED:
        raise HTTPException(status_code=409, detail="rejected allocation cannot be approved")

    if str(row["status"]) == APPROVED and row["checker_admin_id"] is not None:
        return {
            "allocation_event_id": allocation_event_id,
            "status": APPROVED,
            "maker_admin_id": int(row["maker_admin_id"]),
            "checker_admin_id": int(row["checker_admin_id"]),
            "idempotent": True,
        }

    decided = now_iso()
    conn.execute(
        """
        UPDATE allocation_approvals
        SET status = ?,
            checker_admin_id = ?,
            decision_reason = NULL,
            updated_at = ?,
            decision_at = ?
        WHERE allocation_event_id = ?
        """,
        (APPROVED, checker_admin_id, decided, decided, allocation_event_id),
    )

    return {
        "allocation_event_id": allocation_event_id,
        "status": APPROVED,
        "maker_admin_id": int(row["maker_admin_id"]),
        "checker_admin_id": checker_admin_id,
        "idempotent": False,
    }


def reject_allocation(
    conn: sqlite3.Connection,
    *,
    allocation_event_id: int,
    checker_admin_id: int,
    reason: str,
) -> dict[str, Any]:
    ensure_approval_schema(conn)
    row = _fetch_allocation_for_decision(conn, allocation_event_id)

    if int(row["maker_admin_id"]) == checker_admin_id:
        raise HTTPException(status_code=403, detail="maker and checker must be different users")

    if str(row["status"]) == APPROVED:
        raise HTTPException(status_code=409, detail="approved allocation cannot be rejected")

    decided = now_iso()
    conn.execute(
        """
        UPDATE allocation_approvals
        SET status = ?,
            checker_admin_id = ?,
            decision_reason = ?,
            updated_at = ?,
            decision_at = ?
        WHERE allocation_event_id = ?
        """,
        (REJECTED, checker_admin_id, reason.strip() or "Rejected by checker", decided, decided, allocation_event_id),
    )

    return {
        "allocation_event_id": allocation_event_id,
        "status": REJECTED,
        "maker_admin_id": int(row["maker_admin_id"]),
        "checker_admin_id": checker_admin_id,
        "decision_reason": reason.strip() or "Rejected by checker",
    }


def create_approval_router(
    get_conn: Callable[[], sqlite3.Connection],
    require_role: Callable[[str | None, set[str]], str],
    require_admin_id: Callable[[str | None], int],
) -> APIRouter:
    router = APIRouter(prefix="/api/v1/approvals", tags=["approvals", "maker-checker"])

    @router.post("/{allocation_event_id}/approve")
    def post_approve_allocation(
        allocation_event_id: int,
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_role(x_role, {"admin", "ca"})
        checker_admin_id = require_admin_id(x_admin_id)

        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            decision = approve_allocation(
                conn,
                allocation_event_id=allocation_event_id,
                checker_admin_id=checker_admin_id,
            )
            conn.commit()

        return {"status": "ok", "decision": decision}

    @router.post("/{allocation_event_id}/reject")
    def post_reject_allocation(
        allocation_event_id: int,
        reason: str = "",
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_role(x_role, {"admin", "ca"})
        checker_admin_id = require_admin_id(x_admin_id)

        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            decision = reject_allocation(
                conn,
                allocation_event_id=allocation_event_id,
                checker_admin_id=checker_admin_id,
                reason=reason,
            )
            conn.commit()

        return {"status": "ok", "decision": decision}

    return router
