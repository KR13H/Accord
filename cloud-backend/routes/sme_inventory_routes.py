from __future__ import annotations

import sqlite3
from decimal import Decimal
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from services.sme_inventory_service import (
    DEFAULT_BUSINESS_ID,
    create_item,
    delete_item,
    get_item,
    get_low_stock_alerts,
    list_items,
    update_item,
)


class InventoryItemIn(BaseModel):
    business_id: str = Field(default=DEFAULT_BUSINESS_ID, min_length=1, max_length=64)
    item_name: str = Field(min_length=1, max_length=120)
    sku: str | None = Field(default=None, max_length=64)
    current_stock: Decimal = Field(default=Decimal("0"), ge=0)
    minimum_stock_level: Decimal = Field(default=Decimal("0"), ge=0)
    unit_price: Decimal = Field(default=Decimal("0"), ge=0)


class InventoryItemUpdateIn(BaseModel):
    item_name: str | None = Field(default=None, min_length=1, max_length=120)
    sku: str | None = Field(default=None, max_length=64)
    current_stock: Decimal | None = Field(default=None, ge=0)
    minimum_stock_level: Decimal | None = Field(default=None, ge=0)
    unit_price: Decimal | None = Field(default=None, ge=0)


def create_sme_inventory_router(get_conn: Callable[[], sqlite3.Connection]) -> APIRouter:
    router = APIRouter(prefix="/api/v1/sme/inventory", tags=["sme", "inventory"])

    @router.post("/items", status_code=201)
    def post_item(payload: InventoryItemIn) -> dict[str, Any]:
        try:
            item = create_item(
                get_conn,
                business_id=payload.business_id,
                item_name=payload.item_name,
                sku=payload.sku,
                current_stock=payload.current_stock,
                minimum_stock_level=payload.minimum_stock_level,
                unit_price=payload.unit_price,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {"status": "ok", "item": item}

    @router.get("/items")
    def get_items(business_id: str = Query(default=DEFAULT_BUSINESS_ID)) -> dict[str, Any]:
        items = list_items(get_conn, business_id=business_id)
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
                sku=payload.sku,
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

    return router
