from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Callable

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field

from services.razorpay_service import get_razorpay_service


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


class CreateSubscriptionIn(BaseModel):
    business_id: str = Field(min_length=3, max_length=128)


def _normalize_subscription_status(status: str | None) -> str:
    token = str(status or "").strip().upper()
    if token in {"ACTIVE", "AUTHENTICATED", "PAUSED", "HALTED", "CANCELLED", "COMPLETED", "PENDING"}:
        return token
    return "PENDING"


def _verify_webhook_signature(payload: bytes, signature: str) -> None:
    try:
        get_razorpay_service().verify_webhook_signature(payload=payload, signature=signature)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=401, detail="Invalid Razorpay webhook signature") from exc


def _upsert_subscription(
    conn: sqlite3.Connection,
    *,
    business_id: str,
    razorpay_subscription_id: str,
    status: str,
    current_period_end: str | None,
) -> None:
    now = _now_iso()
    conn.execute(
        """
        INSERT INTO sme_subscriptions (
            business_id,
            razorpay_subscription_id,
            status,
            current_period_end,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(razorpay_subscription_id) DO UPDATE SET
            business_id = excluded.business_id,
            status = excluded.status,
            current_period_end = excluded.current_period_end,
            updated_at = excluded.updated_at
        """,
        (business_id, razorpay_subscription_id, status, current_period_end, now, now),
    )


def create_billing_router(get_conn: Callable[[], sqlite3.Connection]) -> APIRouter:
    router = APIRouter(prefix="/api/v1/billing", tags=["billing"])

    @router.post("/create-subscription")
    def post_create_subscription(payload: CreateSubscriptionIn) -> dict[str, Any]:
        clean_business_id = payload.business_id.strip()
        if not clean_business_id:
            raise HTTPException(status_code=422, detail="business_id is required")

        try:
            created = get_razorpay_service().create_subscription(business_id=clean_business_id)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=502, detail=f"Failed to create Razorpay subscription: {exc}") from exc

        subscription_id = str(created.get("id") or "").strip()
        if not subscription_id:
            raise HTTPException(status_code=502, detail="Razorpay returned empty subscription id")

        status = _normalize_subscription_status(str(created.get("status") or "pending"))
        period_end_raw = created.get("current_end")
        current_period_end = str(period_end_raw) if period_end_raw is not None else None

        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            _upsert_subscription(
                conn,
                business_id=clean_business_id,
                razorpay_subscription_id=subscription_id,
                status=status,
                current_period_end=current_period_end,
            )
            conn.commit()

        return {
            "status": "ok",
            "business_id": clean_business_id,
            "subscription_id": subscription_id,
            "subscription_status": status,
            "razorpay_payload": created,
        }

    @router.post("/webhook")
    async def post_billing_webhook(
        request: Request,
        x_razorpay_signature: str | None = Header(default=None, alias="X-Razorpay-Signature"),
    ) -> dict[str, Any]:
        if not x_razorpay_signature:
            raise HTTPException(status_code=401, detail="Missing Razorpay webhook signature")

        raw_body = await request.body()
        _verify_webhook_signature(raw_body, x_razorpay_signature)

        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=400, detail="Invalid webhook payload") from exc

        event = str(payload.get("event") or "").strip()
        entity = ((payload.get("payload") or {}).get("subscription") or {}).get("entity") or {}

        subscription_id = str(entity.get("id") or "").strip()
        if not subscription_id:
            raise HTTPException(status_code=422, detail="subscription id missing in webhook payload")

        notes = entity.get("notes") if isinstance(entity.get("notes"), dict) else {}
        business_id = str(notes.get("business_id") or "SME-001").strip() or "SME-001"
        incoming_status = _normalize_subscription_status(str(entity.get("status") or "PENDING"))
        status = "ACTIVE" if incoming_status == "ACTIVE" or event == "subscription.activated" else incoming_status

        current_end = entity.get("current_end")
        current_period_end = str(current_end) if current_end is not None else None

        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            _upsert_subscription(
                conn,
                business_id=business_id,
                razorpay_subscription_id=subscription_id,
                status=status,
                current_period_end=current_period_end,
            )
            conn.commit()

        return {
            "status": "ok",
            "event": event,
            "subscription_id": subscription_id,
            "subscription_status": status,
        }

    return router
