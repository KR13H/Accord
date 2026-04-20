from __future__ import annotations

import sqlite3
from decimal import Decimal
from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from services.ai_vision_service import vision_scan_inventory
from services.predictive_restock import get_restock_predictions
from services.sme_inventory_service import (
    DEFAULT_BUSINESS_ID,
    create_item,
    delete_item,
    get_item,
    get_low_stock_alerts,
    list_items,
    update_item,
)
from utils.rate_limit import build_rate_limit_dependency


class InventoryItemIn(BaseModel):
    business_id: str = Field(default=DEFAULT_BUSINESS_ID, min_length=1, max_length=64)
    item_name: str = Field(min_length=1, max_length=120)
    factory_serial: str | None = Field(default=None, max_length=64)
    current_stock: Decimal = Field(default=Decimal("0"), ge=0)
    minimum_stock_level: Decimal = Field(default=Decimal("0"), ge=0)
    unit_price: Decimal = Field(default=Decimal("0"), ge=0)


class InventoryItemUpdateIn(BaseModel):
    item_name: str | None = Field(default=None, min_length=1, max_length=120)
    factory_serial: str | None = Field(default=None, max_length=64)
    current_stock: Decimal | None = Field(default=None, ge=0)
    minimum_stock_level: Decimal | None = Field(default=None, ge=0)
    unit_price: Decimal | None = Field(default=None, ge=0)


class VisionScanIn(BaseModel):
    business_id: str = Field(default=DEFAULT_BUSINESS_ID, min_length=1, max_length=64)
    image_base64: str = Field(min_length=20)


def create_sme_inventory_router(get_conn: Callable[[], sqlite3.Connection]) -> APIRouter:
    router = APIRouter(prefix="/api/v1/sme/inventory", tags=["sme", "inventory"])
    rate_limit_vision_scan = build_rate_limit_dependency("sme-vision-scan", limit=8, window_seconds=60)
    rate_limit_restock = build_rate_limit_dependency("sme-restock-predictions", limit=30, window_seconds=60)

    @router.post("/items", status_code=201)
    def post_item(payload: InventoryItemIn) -> dict[str, Any]:
        try:
            item = create_item(
                get_conn,
                business_id=payload.business_id,
                item_name=payload.item_name,
                factory_serial=payload.factory_serial,
                current_stock=payload.current_stock,
                minimum_stock_level=payload.minimum_stock_level,
                unit_price=payload.unit_price,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {"status": "ok", "item": item}

    @router.get("/items")
    def get_items(
        business_id: str = Query(default=DEFAULT_BUSINESS_ID),
        factory_serial: str | None = Query(default=None),
    ) -> dict[str, Any]:
        items = list_items(get_conn, business_id=business_id, factory_serial=factory_serial)
        return {"status": "ok", "count": len(items), "items": items}

    @router.get("/items/{item_id}")
    def get_single_item(item_id: int) -> dict[str, Any]:
        item = get_item(get_conn, item_id=item_id)
        if item is None:
            raise HTTPException(status_code=404, detail="inventory item not found")
        return {"status": "ok", "item": item}

    @router.put("/items/{item_id}")
    def put_item(item_id: int, payload: InventoryItemUpdateIn) -> dict[str, Any]:
        try:
            item = update_item(
                get_conn,
                item_id=item_id,
                item_name=payload.item_name,
                factory_serial=payload.factory_serial,
                current_stock=payload.current_stock,
                minimum_stock_level=payload.minimum_stock_level,
                unit_price=payload.unit_price,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if item is None:
            raise HTTPException(status_code=404, detail="inventory item not found")
        return {"status": "ok", "item": item}

    @router.delete("/items/{item_id}")
    def delete_single_item(item_id: int) -> dict[str, Any]:
        deleted = delete_item(get_conn, item_id=item_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="inventory item not found")
        return {"status": "ok", "deleted": True}

    @router.get("/alerts/low-stock")
    def get_inventory_low_stock_alerts(business_id: str = Query(default=DEFAULT_BUSINESS_ID)) -> dict[str, Any]:
        alerts = get_low_stock_alerts(get_conn, business_id=business_id)
        return {"status": "ok", "count": len(alerts), "items": alerts}

    @router.post("/vision-scan")
    def post_inventory_vision_scan(payload: VisionScanIn, _: bool = Depends(rate_limit_vision_scan)) -> dict[str, Any]:
        try:
            result = vision_scan_inventory(
                get_conn,
                business_id=payload.business_id,
                image_base64=payload.image_base64,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=502, detail=f"vision scan failed: {exc}") from exc
        return {"status": "ok", **result}

    @router.get("/restock-predictions")
    def get_inventory_restock_predictions(
        business_id: str = Query(default=DEFAULT_BUSINESS_ID),
        _: bool = Depends(rate_limit_restock),
    ) -> dict[str, Any]:
        result = get_restock_predictions(get_conn, business_id=business_id)
        return {"status": "ok", **result}

    return router
