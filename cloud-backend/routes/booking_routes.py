from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from routes.broker_routes import auto_link_broker_lead, ensure_broker_lead_schema
from services.commission_service import CommissionService
from services.booking_service import BookingService
from services.tds_service import TdsService
from utils.currency_converter import CurrencyConverter


class BookingCreateIn(BaseModel):
    booking_id: str = Field(min_length=1, max_length=64)
    project_id: str = Field(min_length=1, max_length=64)
    spv_id: str = Field(min_length=1, max_length=64)
    customer_name: str = Field(min_length=1, max_length=180)
    unit_code: str = Field(min_length=1, max_length=64)
    customer_id: str | None = Field(default=None, min_length=1, max_length=64)
    broker_id: str | None = Field(default=None, min_length=1, max_length=64)
    currency_code: str = Field(default="INR", min_length=3, max_length=3)
    foreign_amount: Decimal | None = Field(default=None, ge=0)
    total_consideration: Decimal = Field(ge=0)
    booking_date: date
    status: str = Field(default="ACTIVE", min_length=3, max_length=16)


class BookingUpdateIn(BaseModel):
    project_id: str | None = Field(default=None, min_length=1, max_length=64)
    spv_id: str | None = Field(default=None, min_length=1, max_length=64)
    customer_name: str | None = Field(default=None, min_length=1, max_length=180)
    unit_code: str | None = Field(default=None, min_length=1, max_length=64)
    customer_id: str | None = Field(default=None, min_length=1, max_length=64)
    broker_id: str | None = Field(default=None, min_length=1, max_length=64)
    currency_code: str | None = Field(default=None, min_length=3, max_length=3)
    foreign_amount: Decimal | None = Field(default=None, ge=0)
    foreign_meta: dict[str, Any] | None = None
    total_consideration: Decimal | None = Field(default=None, ge=0)
    booking_date: date | None = None
    status: str | None = Field(default=None, min_length=3, max_length=16)


def create_booking_router(get_conn: callable, require_role: callable, require_admin_id: callable) -> APIRouter:
    router = APIRouter(prefix="/api/v1", tags=["bookings"])
    service = BookingService(get_conn=get_conn)
    tds_service = TdsService(get_conn=get_conn)
    commission_service = CommissionService(get_conn=get_conn)
    currency_converter = CurrencyConverter(get_conn=get_conn)

    def require_ops_or_admin(x_role: str | None, x_admin_id: str | None) -> None:
        require_role(x_role, {"ops", "admin", "ca"})
        require_admin_id(x_admin_id)

    @router.post("/bookings", status_code=201)
    def post_booking(
        payload: BookingCreateIn,
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_ops_or_admin(x_role, x_admin_id)
        try:
            safe_currency = payload.currency_code.strip().upper()
            inr_amount = payload.total_consideration
            foreign_amount = payload.foreign_amount
            foreign_meta: dict[str, Any] | None = None

            if safe_currency != "INR":
                if foreign_amount is None:
                    raise ValueError("foreign_amount is required when currency_code is not INR")
                conversion = currency_converter.convert_to_inr(
                    currency_code=safe_currency,
                    foreign_amount=foreign_amount,
                    as_of_date=payload.booking_date.isoformat(),
                )
                inr_amount = Decimal(str(conversion["inr_amount"]))
                foreign_meta = dict(conversion["foreign_meta"])
            elif foreign_amount is not None:
                foreign_meta = {
                    "currency_code": "INR",
                    "foreign_amount": f"{foreign_amount:.2f}",
                    "inr_rate": "1.0000",
                    "as_of_date": payload.booking_date.isoformat(),
                }

            booking = service.create_booking(
                {
                    "booking_id": payload.booking_id,
                    "project_id": payload.project_id,
                    "spv_id": payload.spv_id,
                    "customer_name": payload.customer_name,
                    "unit_code": payload.unit_code,
                    "customer_id": payload.customer_id,
                    "broker_id": payload.broker_id,
                    "currency_code": safe_currency,
                    "foreign_amount": foreign_amount,
                    "foreign_meta": foreign_meta,
                    "total_consideration": inr_amount,
                    "booking_date": payload.booking_date.isoformat(),
                    "status": payload.status,
                }
            )

            if not (payload.broker_id and payload.broker_id.strip()):
                with get_conn() as conn:
                    conn.row_factory = sqlite3.Row
                    ensure_broker_lead_schema(conn)
                    broker_id = auto_link_broker_lead(
                        conn,
                        project_id=payload.project_id,
                        customer_name=payload.customer_name,
                        booking_id=payload.booking_id,
                    )
                    if broker_id:
                        conn.execute(
                            "UPDATE sales_bookings SET broker_id = ? WHERE booking_id = ?",
                            (broker_id, payload.booking_id),
                        )
                        conn.commit()
                        booking = service.get_booking(payload.booking_id) or booking

            tds_record = tds_service.process_booking_for_tds(payload.booking_id)
            commission_record = None
            effective_broker_id = str(booking.get("broker_id") or "").strip()
            if effective_broker_id:
                commission_record = commission_service.create_commission_for_booking(
                    booking_id=payload.booking_id,
                    broker_id=effective_broker_id,
                )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {
            "status": "ok",
            "booking": booking,
            "tds": (
                {
                    "booking_id": tds_record.booking_id,
                    "tds_amount": f"{tds_record.tds_amount:.2f}",
                    "status": tds_record.status,
                }
                if tds_record is not None
                else None
            ),
            "commission": commission_record,
        }

    @router.get("/bookings")
    def list_bookings(
        status: str | None = None,
        limit: int = 100,
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_ops_or_admin(x_role, x_admin_id)
        bookings = service.list_bookings(status=status, limit=limit)
        return {"status": "ok", "count": len(bookings), "items": bookings}

    @router.get("/bookings/{booking_id}")
    def get_booking(
        booking_id: str,
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_ops_or_admin(x_role, x_admin_id)
        booking = service.get_booking(booking_id)
        if booking is None:
            raise HTTPException(status_code=404, detail="booking_id not found")
        return {"status": "ok", "booking": booking}

    @router.put("/bookings/{booking_id}")
    def put_booking(
        booking_id: str,
        payload: BookingUpdateIn,
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_ops_or_admin(x_role, x_admin_id)
        try:
            booking = service.update_booking(booking_id, payload.model_dump(exclude_unset=True))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if booking is None:
            raise HTTPException(status_code=404, detail="booking_id not found")
        return {"status": "ok", "booking": booking}

    @router.delete("/bookings/{booking_id}")
    def cancel_booking(
        booking_id: str,
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_ops_or_admin(x_role, x_admin_id)
        booking = service.cancel_booking(booking_id)
        if booking is None:
            raise HTTPException(status_code=404, detail="booking_id not found")
        return {"status": "ok", "booking": booking}

    return router
