from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_UP
from typing import Any, Callable

import httpx

from services.universal_accounting import DEFAULT_BUSINESS_ID, ensure_sme_schema
from utils.redis_runtime import cache_get_json, cache_set_json, namespaced_key

OLLAMA_PREDICT_URL = "http://localhost:11434/api/generate"
PREDICT_MODEL = "llama3"
CACHE_TTL_SECONDS = 300


def _build_velocity_table(rows: list[sqlite3.Row], days: int) -> list[dict[str, Any]]:
    series: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row["category"] or "General").strip() or "General"
        date_key = str(row["tx_day"])
        amount = Decimal(str(row["total_amount"]))

        bucket = series.setdefault(
            key,
            {
                "item_name": key,
                "total_sales": Decimal("0"),
                "daily": {},
            },
        )
        bucket["total_sales"] += amount
        bucket["daily"][date_key] = float(amount)

    table = []
    for item_name, bucket in series.items():
        daily_avg = (bucket["total_sales"] / Decimal(str(days))) if days > 0 else Decimal("0")
        table.append(
            {
                "item_name": item_name,
                "total_sales_30d": float(bucket["total_sales"]),
                "daily_average": float(daily_avg),
                "daily_points": bucket["daily"],
            }
        )

    table.sort(key=lambda row: row["total_sales_30d"], reverse=True)
    return table


def _fallback_predictions(velocity: list[dict[str, Any]]) -> list[dict[str, Any]]:
    predictions: list[dict[str, Any]] = []
    for row in velocity:
        daily_avg = Decimal(str(row["daily_average"]))
        weekly_need = (daily_avg * Decimal("7")).quantize(Decimal("1"), rounding=ROUND_UP)
        if weekly_need <= 0:
            continue
        predictions.append(
            {
                "item_name": row["item_name"],
                "predicted_order_qty": int(weekly_need),
                "justification": "Based on 30-day average velocity with a one-week coverage buffer.",
            }
        )
    return predictions


def get_restock_predictions(
    get_conn: Callable[[], sqlite3.Connection],
    *,
    business_id: str | None,
    ollama_url: str = OLLAMA_PREDICT_URL,
    model: str = PREDICT_MODEL,
) -> dict[str, Any]:
    clean_business_id = (business_id or DEFAULT_BUSINESS_ID).strip() or DEFAULT_BUSINESS_ID
    cache_key = namespaced_key("restock", clean_business_id, model)
    cached = cache_get_json(cache_key)
    if isinstance(cached, dict):
        cached["cache"] = "hit"
        return cached

    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=29)

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        ensure_sme_schema(conn)
        rows = conn.execute(
            """
            SELECT date(created_at) AS tx_day, category, SUM(CAST(amount AS REAL)) AS total_amount
            FROM sme_transactions
            WHERE business_id = ?
              AND type = 'INCOME'
              AND date(created_at) >= ?
              AND date(created_at) <= ?
            GROUP BY tx_day, category
            ORDER BY tx_day ASC
            """,
            (clean_business_id, start_date.isoformat(), end_date.isoformat()),
        ).fetchall()

    velocity = _build_velocity_table(rows, 30)
    if not velocity:
        payload = {
            "business_id": clean_business_id,
            "window": {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
            "predictions": [],
            "model": model,
        }
        payload["cache"] = "miss"
        cache_set_json(cache_key, payload, CACHE_TTL_SECONDS)
        return payload

    system_prompt = (
        "You are a supply chain expert. Analyze this 30-day sales velocity data. "
        "Return a JSON payload predicting how much of each item the shop owner needs "
        "to order for the next 7 days to avoid stocking out, including a 1-sentence "
        "justification for each."
    )
    user_prompt = json.dumps({"velocity": velocity}, ensure_ascii=True)

    predictions: list[dict[str, Any]]
    try:
        with httpx.Client(timeout=75.0) as client:
            response = client.post(
                ollama_url,
                json={
                    "model": model,
                    "system": system_prompt,
                    "prompt": user_prompt,
                    "stream": False,
                    "format": "json",
                },
            )
            response.raise_for_status()
            data = response.json()
        parsed = json.loads(str(data.get("response", "{}")))
        raw_predictions = parsed.get("predictions", parsed)
        predictions = []
        if isinstance(raw_predictions, list):
            for item in raw_predictions:
                if not isinstance(item, dict):
                    continue
                item_name = str(item.get("item_name", "")).strip()
                try:
                    qty = int(float(str(item.get("predicted_order_qty", 0))))
                except Exception:  # noqa: BLE001
                    qty = 0
                justification = str(item.get("justification", "")).strip()
                if item_name and qty > 0:
                    predictions.append(
                        {
                            "item_name": item_name,
                            "predicted_order_qty": qty,
                            "justification": justification
                            or "Predicted by local model from 30-day sales velocity.",
                        }
                    )
        if not predictions:
            predictions = _fallback_predictions(velocity)
    except Exception:  # noqa: BLE001
        predictions = _fallback_predictions(velocity)

    payload = {
        "business_id": clean_business_id,
        "window": {"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
        "predictions": predictions,
        "model": model,
        "sample_size_days": 30,
    }
    payload["cache"] = "miss"
    cache_set_json(cache_key, payload, CACHE_TTL_SECONDS)
    return payload
