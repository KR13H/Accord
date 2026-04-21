from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Callable

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from utils.feature_flags import is_feature_enabled, set_feature_flag
from utils.jwt_manager import verify_token


SUPER_ADMIN_ID = os.getenv("ACCORD_SUPER_ADMIN_ID", "krish@accord.local").strip().lower()
PROTECTED_FLAGS = {"ai_vision_service", "gstn_sandbox_api"}


class FeatureFlagToggleIn(BaseModel):
    enabled: bool = Field(...)


def _require_super_admin(authorization: str | None) -> dict[str, Any]:
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")
    parts = authorization.strip().split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Authorization must be Bearer token")

    try:
        claims = verify_token(parts[1], expected_type="access")
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc

    subject = str(claims.get("sub", "")).strip().lower()
    if subject != SUPER_ADMIN_ID:
        raise HTTPException(status_code=403, detail="Super-admin access required")
    return claims


def _daily_growth(conn: sqlite3.Connection, days: int = 30) -> list[dict[str, Any]]:
    end_day = datetime.utcnow().date()
    start_day = end_day - timedelta(days=max(1, days) - 1)

    tx_rows = conn.execute(
        """
        SELECT date(created_at) AS tx_day,
               SUM(CASE WHEN type = 'INCOME' THEN CAST(amount AS REAL) ELSE 0 END) AS gmv,
               SUM(CASE WHEN type = 'INCOME' THEN 1 ELSE 0 END) AS tx_count,
               COUNT(DISTINCT CASE WHEN type = 'INCOME' THEN business_id END) AS active_smes
        FROM sme_transactions
        WHERE date(created_at) BETWEEN ? AND ?
        GROUP BY date(created_at)
        """,
        (start_day.isoformat(), end_day.isoformat()),
    ).fetchall()

    by_day = {
        str(row["tx_day"]): {
            "gmv": float(row["gmv"] or 0),
            "transactions": int(row["tx_count"] or 0),
            "active_smes": int(row["active_smes"] or 0),
        }
        for row in tx_rows
        if row["tx_day"]
    }

    result: list[dict[str, Any]] = []
    cursor = start_day
    while cursor <= end_day:
        key = cursor.isoformat()
        day_data = by_day.get(key, {"gmv": 0.0, "transactions": 0, "active_smes": 0})
        result.append({"date": key, **day_data})
        cursor += timedelta(days=1)

    return result


def _row_num(row: sqlite3.Row | None, key: str) -> float:
    if row is None:
        return 0.0
    try:
        return float(row[key] or 0)
    except Exception:  # noqa: BLE001
        return 0.0


def create_superadmin_router(get_conn: Callable[[], sqlite3.Connection]) -> APIRouter:
    router = APIRouter(prefix="/api/v1/admin", tags=["admin", "super-admin"])

    @router.get("/platform-metrics")
    def get_platform_metrics(authorization: str | None = Header(default=None, alias="Authorization")) -> dict[str, Any]:
        claims = _require_super_admin(authorization)
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row

            totals = conn.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN type = 'INCOME' THEN CAST(amount AS REAL) ELSE 0 END), 0) AS total_gmv,
                    COALESCE(SUM(CASE WHEN type = 'INCOME' THEN 1 ELSE 0 END), 0) AS total_income_transactions
                FROM sme_transactions
                """
            ).fetchone()

            active_smes_row = conn.execute(
                """
                SELECT COUNT(DISTINCT business_id) AS active_smes
                FROM (
                    SELECT business_id FROM sme_transactions
                    UNION
                    SELECT business_id FROM sme_inventory_items
                    UNION
                    SELECT business_id FROM sme_customers
                    UNION
                    SELECT business_id FROM sme_subscriptions
                ) businesses
                """
            ).fetchone()

            active_subscriptions_row = conn.execute(
                """
                SELECT COUNT(*) AS active_subscriptions
                FROM sme_subscriptions
                WHERE upper(status) IN ('ACTIVE', 'AUTHENTICATED', 'CREATED')
                """
            ).fetchone()

            growth = _daily_growth(conn, days=30)

        return {
            "status": "ok",
            "super_admin": str(claims.get("sub", "")),
            "metrics": {
                "active_smes": int(_row_num(active_smes_row, "active_smes")),
                "total_gmv": _row_num(totals, "total_gmv"),
                "active_subscriptions": int(_row_num(active_subscriptions_row, "active_subscriptions")),
                "total_income_transactions": int(_row_num(totals, "total_income_transactions")),
            },
            "daily_growth": growth,
            "feature_flags": {
                "ai_vision_service": is_feature_enabled("ai_vision_service", default=True),
                "gstn_sandbox_api": is_feature_enabled("gstn_sandbox_api", default=True),
            },
        }

    @router.put("/feature-flags/{flag_name}")
    def put_feature_flag(
        flag_name: str,
        payload: FeatureFlagToggleIn,
        authorization: str | None = Header(default=None, alias="Authorization"),
    ) -> dict[str, Any]:
        _require_super_admin(authorization)
        normalized = flag_name.strip().lower()
        if normalized not in PROTECTED_FLAGS:
            raise HTTPException(status_code=404, detail="feature flag not managed by god-mode")

        enabled = set_feature_flag(normalized, payload.enabled)
        return {"status": "ok", "flag": normalized, "enabled": enabled}

    return router
