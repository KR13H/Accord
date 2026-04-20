from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Callable

import httpx

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
OLLAMA_CHAT_URL = f"{OLLAMA_HOST}/api/chat"
PRICING_MODEL = os.getenv("ACCORD_PRICING_MODEL", "llama3.2")


@dataclass
class PricingSignal:
    project_id: str
    window_weeks: int
    units_sold: int
    sales_velocity: Decimal


class AiPricingEngine:
    def __init__(self, get_conn: Callable[[], sqlite3.Connection]) -> None:
        self.get_conn = get_conn

    def compute_sales_velocity(self, project_id: str, window_weeks: int = 4) -> PricingSignal:
        clean_project = project_id.strip()
        if not clean_project:
            raise ValueError("project_id is required")
        if window_weeks < 1 or window_weeks > 26:
            raise ValueError("window_weeks must be between 1 and 26")

        with self.get_conn() as conn:
            conn.row_factory = sqlite3.Row
            since_iso = (datetime.utcnow() - timedelta(weeks=window_weeks)).isoformat(timespec="seconds") + "Z"
            row = conn.execute(
                """
                SELECT COUNT(1) AS c
                FROM sales_bookings
                WHERE project_id = ?
                  AND UPPER(COALESCE(status, 'ACTIVE')) IN ('ACTIVE', 'BOOKED', 'SOLD')
                  AND COALESCE(booking_date, created_at, updated_at) >= ?
                """,
                (clean_project, since_iso),
            ).fetchone()

        units_sold = int((row["c"] if row is not None else 0) or 0)
        velocity = (Decimal(units_sold) / Decimal(window_weeks)).quantize(Decimal("0.01"))
        return PricingSignal(
            project_id=clean_project,
            window_weeks=window_weeks,
            units_sold=units_sold,
            sales_velocity=velocity,
        )

    async def recommend_price(self, project_id: str, window_weeks: int = 4) -> dict[str, Any]:
        signal = self.compute_sales_velocity(project_id=project_id, window_weeks=window_weeks)

        system_prompt = (
            "You are a Real Estate Revenue Manager for Indian residential projects. "
            "Return strict JSON only with keys: increase_pct, justification. "
            "increase_pct must be between 2 and 5 when sales velocity is high, else 0 to 2. "
            "justification must be exactly 2 concise sentences."
        )
        user_prompt = (
            f"Project: {signal.project_id}. "
            f"Window weeks: {signal.window_weeks}. "
            f"Units sold: {signal.units_sold}. "
            f"Sales velocity: {signal.sales_velocity} units/week. "
            "Recommend a base_price increase."
        )

        fallback = self._fallback_recommendation(signal)

        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                response = await client.post(
                    OLLAMA_CHAT_URL,
                    json={
                        "model": PRICING_MODEL,
                        "stream": False,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt},
                        ],
                        "options": {"temperature": 0.2},
                    },
                )
            response.raise_for_status()
            payload = response.json()
            content = str(payload.get("message", {}).get("content", "")).strip()
            parsed = self._parse_json(content)
            increase_pct = float(parsed.get("increase_pct", fallback["increase_pct"]))
            if increase_pct < 2.0 and signal.sales_velocity >= Decimal("3.00"):
                increase_pct = 2.0
            if increase_pct > 5.0:
                increase_pct = 5.0
            justification = str(parsed.get("justification", fallback["justification"])).strip()
            if not justification:
                justification = fallback["justification"]
            return {
                "project_id": signal.project_id,
                "units_sold": signal.units_sold,
                "window_weeks": signal.window_weeks,
                "sales_velocity": f"{signal.sales_velocity:.2f}",
                "increase_pct": round(increase_pct, 2),
                "justification": justification,
                "provider": "ollama",
                "model": PRICING_MODEL,
            }
        except Exception:
            return {
                **fallback,
                "project_id": signal.project_id,
                "units_sold": signal.units_sold,
                "window_weeks": signal.window_weeks,
                "sales_velocity": f"{signal.sales_velocity:.2f}",
                "provider": "heuristic-fallback",
                "model": None,
            }

    def _fallback_recommendation(self, signal: PricingSignal) -> dict[str, Any]:
        if signal.sales_velocity >= Decimal("5.00"):
            pct = 5.0
        elif signal.sales_velocity >= Decimal("4.00"):
            pct = 4.0
        elif signal.sales_velocity >= Decimal("3.00"):
            pct = 3.0
        elif signal.sales_velocity >= Decimal("2.00"):
            pct = 2.0
        else:
            pct = 0.5

        return {
            "increase_pct": pct,
            "justification": (
                "Recent unit absorption is strong relative to the selected time window. "
                "A measured increase protects margin while keeping pricing within expected market movement."
            ),
        }

    def _parse_json(self, model_output: str) -> dict[str, Any]:
        text = model_output.strip()
        if text.startswith("```"):
            text = text.strip("`")
            if text.lower().startswith("json"):
                text = text[4:].strip()
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try:
                parsed = json.loads(text[start : end + 1])
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass
        return {}
