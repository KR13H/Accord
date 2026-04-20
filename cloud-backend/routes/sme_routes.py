from __future__ import annotations

import calendar
import json
import sqlite3
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field

from services.sme_credit_service import adjust_balance, create_customer
from services.universal_accounting import (
    DEFAULT_BUSINESS_ID,
    get_daily_summary,
    get_transactions_between,
    record_transaction,
)
from utils.gstr1_generator import generate_gstr1_payload
from utils.tally_xml_bridge import generate_tally_xml
from utils.sme_auth import require_sme_owner


class SmeTransactionIn(BaseModel):
    business_id: str = Field(default=DEFAULT_BUSINESS_ID, min_length=1, max_length=64)
    type: str = Field(min_length=6, max_length=7)
    amount: Decimal = Field(gt=0)
    category: str = Field(default="General", min_length=1, max_length=80)
    payment_method: str = Field(default="Cash", min_length=3, max_length=4)


class SmeCustomerIn(BaseModel):
    business_id: str = Field(default=DEFAULT_BUSINESS_ID, min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=120)
    phone: str | None = Field(default=None, max_length=24)


class SmeAmountIn(BaseModel):
    amount: Decimal = Field(gt=0)


def create_sme_router(get_conn: Callable[[], sqlite3.Connection]) -> APIRouter:
    router = APIRouter(prefix="/api/v1/sme", tags=["sme", "universal-accounting"])

    @router.post("/transactions", status_code=201)
    def post_transaction(payload: SmeTransactionIn) -> dict[str, Any]:
        try:
            transaction = record_transaction(
                get_conn,
                business_id=payload.business_id,
                tx_type=payload.type,
                amount=payload.amount,
                category=payload.category,
                payment_method=payload.payment_method,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {"status": "ok", "transaction": transaction}

    @router.get("/summary")
    def get_summary(
        _: str = Depends(require_sme_owner),
        business_id: str = Query(default=DEFAULT_BUSINESS_ID),
        target_date: date | None = Query(default=None),
    ) -> dict[str, Any]:
        summary = get_daily_summary(
            get_conn,
            business_id=business_id,
            target_date=target_date,
        )
        return {"status": "ok", "summary": summary}

    @router.post("/customers", status_code=201)
    def post_customer(payload: SmeCustomerIn, _: str = Depends(require_sme_owner)) -> dict[str, Any]:
        try:
            customer = create_customer(
                get_conn,
                business_id=payload.business_id,
                name=payload.name,
                phone=payload.phone,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {"status": "ok", "customer": customer}

    @router.post("/customers/{customer_id}/charge")
    def post_customer_charge(
        customer_id: int,
        payload: SmeAmountIn,
        _: str = Depends(require_sme_owner),
    ) -> dict[str, Any]:
        try:
            customer = adjust_balance(
                get_conn,
                customer_id=customer_id,
                amount=payload.amount,
                mode="charge",
            )
        except ValueError as exc:
            detail = str(exc)
            status = 404 if "not found" in detail else 422
            raise HTTPException(status_code=status, detail=detail) from exc
        return {"status": "ok", "customer": customer}

    @router.post("/customers/{customer_id}/settle")
    def post_customer_settle(
        customer_id: int,
        payload: SmeAmountIn,
        _: str = Depends(require_sme_owner),
    ) -> dict[str, Any]:
        try:
            customer = adjust_balance(
                get_conn,
                customer_id=customer_id,
                amount=payload.amount,
                mode="settle",
            )
        except ValueError as exc:
            detail = str(exc)
            status = 404 if "not found" in detail else 422
            raise HTTPException(status_code=status, detail=detail) from exc
        return {"status": "ok", "customer": customer}

    @router.get("/export/tally")
    def export_tally(
        _: str = Depends(require_sme_owner),
        business_id: str = Query(default=DEFAULT_BUSINESS_ID),
        start_date: date | None = Query(default=None),
        end_date: date | None = Query(default=None),
    ) -> Response:
        safe_end_date = end_date or datetime.utcnow().date()
        safe_start_date = start_date or safe_end_date
        try:
            transactions = get_transactions_between(
                get_conn,
                business_id=business_id,
                start_date=safe_start_date,
                end_date=safe_end_date,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        xml_payload = generate_tally_xml(transactions)
        filename = f"tally_{business_id}_{safe_start_date.isoformat()}_{safe_end_date.isoformat()}.xml"
        return Response(
            content=xml_payload,
            media_type="application/xml",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @router.get("/export/gstr1")
    def export_gstr1(
        _: str = Depends(require_sme_owner),
        business_id: str = Query(default=DEFAULT_BUSINESS_ID),
        period: str | None = Query(default=None, description="YYYYMM filing period"),
        gstin: str = Query(default="27ABCDE1234F1Z5"),
    ) -> Response:
        filing_period = period or datetime.utcnow().strftime("%Y%m")
        start_date = datetime.strptime(f"{filing_period}01", "%Y%m%d").date()
        last_day = calendar.monthrange(start_date.year, start_date.month)[1]
        end_date = date(start_date.year, start_date.month, last_day)

        transactions = get_transactions_between(
            get_conn,
            business_id=business_id,
            start_date=start_date,
            end_date=end_date,
        )
        payload = generate_gstr1_payload(
            transactions,
            business_id=business_id,
            gstin=gstin,
            period_yyyymm=filing_period,
        )
        filename = f"gstr1_{business_id}_{filing_period}.json"
        return Response(
            content=json.dumps(payload, ensure_ascii=True, separators=(",", ":"), indent=2),
            media_type="application/json",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    return router
