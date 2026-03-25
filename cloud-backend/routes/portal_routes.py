from __future__ import annotations

import base64
import hmac
import json
import os
import sqlite3
from datetime import datetime
from hashlib import sha256
from typing import Any, Callable

from fastapi import APIRouter, Depends, Header, HTTPException


def _b64url_decode(raw: str) -> bytes:
    padding = "=" * ((4 - len(raw) % 4) % 4)
    return base64.urlsafe_b64decode(raw + padding)


def _decode_customer_claims(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) != 3:
        raise HTTPException(status_code=401, detail="Invalid customer token format")

    header_raw, payload_raw, signature_raw = parts
    try:
        payload_bytes = _b64url_decode(payload_raw)
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=401, detail="Invalid customer token payload") from exc

    secret = os.getenv("ACCORD_PORTAL_JWT_SECRET", "").strip()
    if secret:
        signed = f"{header_raw}.{payload_raw}".encode("utf-8")
        expected = hmac.new(secret.encode("utf-8"), signed, sha256).digest()
        actual = _b64url_decode(signature_raw)
        if not hmac.compare_digest(expected, actual):
            raise HTTPException(status_code=401, detail="Invalid customer token signature")

    exp = payload.get("exp")
    if exp is not None:
        try:
            if int(exp) < int(datetime.utcnow().timestamp()):
                raise HTTPException(status_code=401, detail="Customer token expired")
        except HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=401, detail="Invalid customer token expiry") from exc

    return payload if isinstance(payload, dict) else {}


def create_portal_router(get_conn: Callable[[], sqlite3.Connection]) -> APIRouter:
    router = APIRouter(prefix="/api/v1/portal", tags=["portal"])

    def get_current_customer(authorization: str | None = Header(default=None, alias="Authorization")) -> dict[str, Any]:
        if authorization is None:
            raise HTTPException(status_code=401, detail="Missing Authorization header")
        parts = authorization.strip().split(" ", 1)
        if len(parts) != 2 or parts[0].lower() != "bearer":
            raise HTTPException(status_code=401, detail="Authorization must be Bearer token")

        claims = _decode_customer_claims(parts[1])
        customer_id = str(claims.get("customer_id") or claims.get("sub") or "").strip()
        if not customer_id:
            raise HTTPException(status_code=401, detail="customer_id missing in token")

        return {
            "customer_id": customer_id,
            "phone": str(claims.get("phone") or "").strip(),
            "email": str(claims.get("email") or "").strip(),
        }

    @router.get("/my-ledger")
    def get_my_ledger(customer: dict[str, Any] = Depends(get_current_customer)) -> dict[str, Any]:
        customer_id = customer["customer_id"]
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            cols = conn.execute("PRAGMA table_info(sales_bookings)").fetchall()
            has_customer_id = any(str(c["name"]) == "customer_id" for c in cols)

            if has_customer_id:
                bookings = conn.execute(
                    """
                    SELECT booking_id, project_id, unit_code, total_consideration, booking_date, status
                    FROM sales_bookings
                    WHERE customer_id = ?
                    ORDER BY created_at DESC
                    """,
                    (customer_id,),
                ).fetchall()
            else:
                bookings = conn.execute(
                    """
                    SELECT booking_id, project_id, unit_code, total_consideration, booking_date, status
                    FROM sales_bookings
                    WHERE booking_id = ?
                    ORDER BY created_at DESC
                    """,
                    (customer_id,),
                ).fetchall()

            booking_ids = [str(row["booking_id"]) for row in bookings]
            ledger_items: list[dict[str, Any]] = []
            for booking_id in booking_ids:
                rows = conn.execute(
                    """
                    SELECT id, date, reference, description, created_at
                    FROM journal_entries
                    WHERE reference LIKE ? OR description LIKE ?
                    ORDER BY id DESC
                    LIMIT 100
                    """,
                    (f"%{booking_id}%", f"%{booking_id}%"),
                ).fetchall()
                for row in rows:
                    ledger_items.append(
                        {
                            "entry_id": int(row["id"]),
                            "date": row["date"],
                            "reference": row["reference"],
                            "description": row["description"],
                            "created_at": row["created_at"],
                        }
                    )

        return {
            "status": "ok",
            "customer_id": customer_id,
            "bookings": [dict(row) for row in bookings],
            "journal_entries": ledger_items,
        }

    return router
