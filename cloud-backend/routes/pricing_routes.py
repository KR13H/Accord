from __future__ import annotations

from typing import Any, Callable

import sqlite3
from fastapi import APIRouter, Header, HTTPException

from services.ai_pricing_engine import AiPricingEngine


def create_pricing_router(
    get_conn: Callable[[], sqlite3.Connection],
    require_role: Callable[[str | None, set[str]], str],
    require_admin_id: Callable[[str | None], int],
) -> APIRouter:
    router = APIRouter(prefix="/api/v1/pricing", tags=["pricing", "ai"])
    engine = AiPricingEngine(get_conn=get_conn)

    @router.get("/recommendations")
    async def get_pricing_recommendations(
        project_id: str,
        window_weeks: int = 4,
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_role(x_role, {"admin", "ca", "ops"})
        require_admin_id(x_admin_id)

        try:
            recommendation = await engine.recommend_price(project_id=project_id, window_weeks=window_weeks)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        return {
            "status": "ok",
            "recommendation": recommendation,
        }

    return router
