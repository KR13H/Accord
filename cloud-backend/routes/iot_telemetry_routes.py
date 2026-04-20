from __future__ import annotations

import sqlite3
from typing import Any, Callable

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from services.iot_service import process_iot_pulse
from services.sme_inventory_service import DEFAULT_BUSINESS_ID


class IoTPulseIn(BaseModel):
    machine_id: str = Field(min_length=3, max_length=64)
    item_sku: str = Field(min_length=2, max_length=120)
    status: str = Field(min_length=4, max_length=32)
    business_id: str = Field(default=DEFAULT_BUSINESS_ID, min_length=1, max_length=64)


def create_iot_telemetry_router(get_conn: Callable[[], sqlite3.Connection]) -> APIRouter:
    router = APIRouter(prefix="/api/v1/sme/iot", tags=["sme", "iot"])

    @router.post("/pulse")
    def post_iot_pulse(payload: IoTPulseIn) -> dict[str, Any]:
        try:
            result = process_iot_pulse(
                get_conn,
                machine_id=payload.machine_id,
                item_sku=payload.item_sku,
                status=payload.status,
                business_id=payload.business_id,
            )
        except ValueError as exc:
            detail = str(exc)
            status_code = 404 if "not found" in detail else 422
            raise HTTPException(status_code=status_code, detail=detail) from exc
        return result

    return router
