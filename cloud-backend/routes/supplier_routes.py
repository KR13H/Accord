from __future__ import annotations

import csv
import io
import sqlite3
import uuid
from decimal import Decimal
from typing import Any, Callable

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from services.sme_inventory_service import DEFAULT_BUSINESS_ID, ensure_inventory_schema


REQUIRED_COLUMNS = {"item_name", "current_stock", "minimum_stock_level", "unit_price"}
OPTIONAL_COLUMNS = {"factory_serial", "localized_name"}
MAX_UPLOAD_ROWS = 2000


def _to_decimal(value: str, field_name: str) -> Decimal:
    try:
        parsed = Decimal(str(value).strip())
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=422, detail=f"{field_name} must be numeric") from exc
    if parsed < 0:
        raise HTTPException(status_code=422, detail=f"{field_name} cannot be negative")
    return parsed


def _normalize_header(header: str) -> str:
    return header.strip().lower().replace(" ", "_")


def create_supplier_router(get_conn: Callable[[], sqlite3.Connection]) -> APIRouter:
    router = APIRouter(prefix="/api/v1/suppliers", tags=["sme", "suppliers"])

    @router.post("/bulk-upload")
    async def post_bulk_upload(
        csv_file: UploadFile = File(...),
        business_id: str = Form(default=DEFAULT_BUSINESS_ID),
    ) -> dict[str, Any]:
        if not csv_file.filename or not csv_file.filename.lower().endswith(".csv"):
            raise HTTPException(status_code=422, detail="Only CSV files are supported")

        raw_bytes = await csv_file.read()
        if not raw_bytes:
            raise HTTPException(status_code=422, detail="CSV file is empty")

        try:
            text = raw_bytes.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise HTTPException(status_code=422, detail="CSV must be UTF-8 encoded") from exc

        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            raise HTTPException(status_code=422, detail="CSV header row is required")

        headers = {_normalize_header(col) for col in reader.fieldnames if col}
        missing = REQUIRED_COLUMNS - headers
        if missing:
            raise HTTPException(status_code=422, detail=f"Missing required columns: {', '.join(sorted(missing))}")

        clean_business_id = (business_id or DEFAULT_BUSINESS_ID).strip() or DEFAULT_BUSINESS_ID
        records: list[tuple[Any, ...]] = []

        for index, row in enumerate(reader, start=1):
            if index > MAX_UPLOAD_ROWS:
                raise HTTPException(status_code=422, detail=f"CSV exceeds max rows ({MAX_UPLOAD_ROWS})")

            normalized = {_normalize_header(k): (v or "").strip() for k, v in row.items() if k}
            item_name = normalized.get("item_name", "")
            if not item_name:
                raise HTTPException(status_code=422, detail=f"Row {index}: item_name is required")

            current_stock = _to_decimal(normalized.get("current_stock", "0"), f"Row {index} current_stock")
            minimum_stock = _to_decimal(normalized.get("minimum_stock_level", "0"), f"Row {index} minimum_stock_level")
            unit_price = _to_decimal(normalized.get("unit_price", "0"), f"Row {index} unit_price")
            factory_serial = normalized.get("factory_serial") or None
            localized_name = normalized.get("localized_name") or item_name

            if factory_serial:
                system_serial = factory_serial
                is_system_generated = 0
            else:
                system_serial = f"ACC-CSV-{uuid.uuid4().hex[:8].upper()}"
                is_system_generated = 1

            records.append(
                (
                    clean_business_id,
                    item_name[:120],
                    localized_name[:120],
                    factory_serial[:64] if factory_serial else None,
                    system_serial,
                    is_system_generated,
                    f"{current_stock:.4f}",
                    f"{minimum_stock:.4f}",
                    f"{unit_price:.4f}",
                )
            )

        if not records:
            raise HTTPException(status_code=422, detail="CSV contains no data rows")

        with get_conn() as conn:
            ensure_inventory_schema(conn)
            conn.executemany(
                """
                INSERT INTO sme_inventory_items (
                    business_id, item_name, localized_name, factory_serial, system_serial,
                    is_system_generated, current_stock, minimum_stock_level, unit_price
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                records,
            )
            conn.commit()

        return {
            "status": "ok",
            "business_id": clean_business_id,
            "inserted_rows": len(records),
            "allowed_columns": sorted(REQUIRED_COLUMNS | OPTIONAL_COLUMNS),
        }

    return router
