from __future__ import annotations

import sqlite3
from typing import Any, Callable

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from services.autonomous_purchasing import (
    approve_purchase_order_by_token,
    auto_reorder_critical_stock,
    run_auto_reorder_critical_stock,
)
from fastapi import Depends
from utils.sme_auth import require_sme_owner


class AutonomousReorderIn(BaseModel):
    business_id: str = Field(default="SME-001", min_length=1, max_length=64)


def create_autonomous_purchasing_router(get_conn: Callable[[], sqlite3.Connection]) -> APIRouter:
    router = APIRouter(prefix="/api/v1/sme/purchasing", tags=["sme", "autonomous-purchasing"])

    @router.post("/auto-reorder")
    def post_auto_reorder(payload: AutonomousReorderIn) -> dict[str, Any]:
        try:
            result = run_auto_reorder_critical_stock(get_conn, business_id=payload.business_id)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return result

    @router.get("/approval/{approval_token}")
    def get_purchase_order_approval(approval_token: str) -> dict[str, Any]:
        try:
            result = approve_purchase_order_by_token(get_conn, approval_token=approval_token)
        except ValueError as exc:
            status = 404 if "not found" in str(exc).lower() else 422
            raise HTTPException(status_code=status, detail=str(exc)) from exc
        return result

    @router.post("/critical-reorder")
    def post_critical_reorder(_: str = Depends(require_sme_owner)) -> dict[str, Any]:
        return auto_reorder_critical_stock()

    return router