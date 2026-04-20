from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable

import httpx

from services.sme_inventory_service import DEFAULT_BUSINESS_ID, ensure_inventory_schema
from services.storage_service import get_storage_service

OLLAMA_VISION_URL = "http://localhost:11434/api/generate"
VISION_MODEL = "llava"


def _extract_json_array(text: str) -> list[dict[str, Any]]:
    cleaned = text.strip()
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        match = re.search(r"\[.*\]", cleaned, flags=re.DOTALL)
        if not match:
            return []
        try:
            data = json.loads(match.group(0))
        except json.JSONDecodeError:
            return []

    if not isinstance(data, list):
        return []

    rows: list[dict[str, Any]] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        item_name = str(row.get("item_name", "")).strip()
        counted_stock = row.get("counted_stock")
        try:
            counted = int(float(str(counted_stock)))
        except Exception:  # noqa: BLE001
            continue
        if not item_name:
            continue
        rows.append({"item_name": item_name, "counted_stock": max(0, counted)})
    return rows


def _normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip().lower())


def vision_scan_inventory(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    business_id: str | None,
    image_base64: str,
    ollama_url: str = OLLAMA_VISION_URL,
    model: str = VISION_MODEL,
) -> dict[str, Any]:
    clean_business_id = (business_id or DEFAULT_BUSINESS_ID).strip() or DEFAULT_BUSINESS_ID
    clean_image = image_base64.strip()
    if not clean_image:
        raise ValueError("image_base64 is required")

    storage = get_storage_service()
    stored_image = storage.put_base64_image(
        key=f"vision-scans/{clean_business_id}/{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.jpg",
        image_base64=clean_image,
    )

    prompt = (
        "You are an inventory AI. Look at this image of a store shelf. "
        "Identify the products and count them. Return ONLY a strict JSON array "
        "of objects with 'item_name' and 'counted_stock'."
    )

    payload = {
        "model": model,
        "prompt": prompt,
        "images": [clean_image],
        "stream": False,
        "format": "json",
    }

    with httpx.Client(timeout=75.0) as client:
        response = client.post(ollama_url, json=payload)
        response.raise_for_status()
        data = response.json()

    detections = _extract_json_array(str(data.get("response", "")))

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        ensure_inventory_schema(conn)
        rows = conn.execute(
            """
            SELECT id, item_name, current_stock
            FROM sme_inventory_items
            WHERE business_id = ?
            ORDER BY item_name ASC
            """,
            (clean_business_id,),
        ).fetchall()

    inventory_rows = [
        {
            "id": int(row["id"]),
            "item_name": str(row["item_name"]),
            "current_stock": float(row["current_stock"]),
            "normalized": _normalize_name(str(row["item_name"])),
        }
        for row in rows
    ]

    suggestions: list[dict[str, Any]] = []
    for detection in detections:
        detected_name = str(detection["item_name"])
        detected_norm = _normalize_name(detected_name)
        counted_stock = int(detection["counted_stock"])

        match = None
        for item in inventory_rows:
            if item["normalized"] == detected_norm:
                match = item
                break
        if match is None:
            for item in inventory_rows:
                if detected_norm in item["normalized"] or item["normalized"] in detected_norm:
                    match = item
                    break

        if match is None:
            suggestions.append(
                {
                    "item_id": None,
                    "item_name": detected_name,
                    "current_stock": None,
                    "counted_stock": counted_stock,
                    "delta": None,
                    "action": "NO_MATCH_FOUND",
                }
            )
            continue

        current_stock = Decimal(str(match["current_stock"]))
        counted_decimal = Decimal(str(counted_stock))
        delta = counted_decimal - current_stock
        if delta == 0:
            action = "NO_CHANGE"
        elif delta > 0:
            action = "INCREASE_STOCK"
        else:
            action = "DECREASE_STOCK"

        suggestions.append(
            {
                "item_id": match["id"],
                "item_name": match["item_name"],
                "current_stock": float(current_stock),
                "counted_stock": counted_stock,
                "delta": float(delta),
                "action": action,
            }
        )

    return {
        "business_id": clean_business_id,
        "model": model,
        "scan_image_uri": stored_image.get("uri"),
        "scan_image_url": stored_image.get("url"),
        "scan_storage_backend": stored_image.get("backend"),
        "detections": detections,
        "suggestions": suggestions,
        "matched_count": sum(1 for s in suggestions if s.get("item_id") is not None),
    }
