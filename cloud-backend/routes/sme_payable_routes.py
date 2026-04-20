from __future__ import annotations

import sqlite3
from decimal import Decimal
from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from services.sme_payable_service import (
    DEFAULT_BUSINESS_ID,
    add_supplier_bill,
    record_supplier_payment,
)
from utils.sme_auth import require_sme_owner


class SupplierBillIn(BaseModel):
    business_id: str = Field(default=DEFAULT_BUSINESS_ID, min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=120)
    phone: str | None = Field(default=None, max_length=24)
    amount: Decimal = Field(gt=0)


class SupplierPaymentIn(BaseModel):
    supplier_id: int = Field(gt=0)
    amount: Decimal = Field(gt=0)


def create_sme_payable_router(get_conn: Callable[[], sqlite3.Connection]) -> APIRouter:
    router = APIRouter(prefix="/api/v1/sme/payables", tags=["sme", "payables"])

    @router.post("/bills", status_code=201)
    def post_supplier_bill(payload: SupplierBillIn, _: str = Depends(require_sme_owner)) -> dict[str, Any]:
        try:
            supplier = add_supplier_bill(
                get_conn,
                business_id=payload.business_id,
                name=payload.name,
                phone=payload.phone,
                amount=payload.amount,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {"status": "ok", "supplier": supplier}

    @router.post("/pay")
    def post_supplier_payment(payload: SupplierPaymentIn, _: str = Depends(require_sme_owner)) -> dict[str, Any]:
        try:
            supplier = record_supplier_payment(
                get_conn,
                supplier_id=payload.supplier_id,
                amount=payload.amount,
            )
        except ValueError as exc:
            detail = str(exc)
            status = 404 if "not found" in detail else 422
            raise HTTPException(status_code=status, detail=detail) from exc
        return {"status": "ok", "supplier": supplier}

    return router
