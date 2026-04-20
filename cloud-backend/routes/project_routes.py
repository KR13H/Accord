from __future__ import annotations

from decimal import Decimal
from typing import Any, Callable

import sqlite3
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from services.demand_generator import generate_milestone_demands


class ConstructionStageUpdateIn(BaseModel):
    construction_stage: str = Field(min_length=2, max_length=80)
    milestone_percent: Decimal = Field(gt=0, le=100)


def create_project_router(
    get_conn: Callable[[], sqlite3.Connection],
    require_role: Callable[[str | None, set[str]], str],
    require_admin_id: Callable[[str | None], int],
) -> APIRouter:
    router = APIRouter(prefix="/api/v1/projects", tags=["projects", "demand-letters"])

    @router.post("/{project_id}/construction-stage")
    def post_project_construction_stage(
        project_id: str,
        payload: ConstructionStageUpdateIn,
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_role(x_role, {"admin", "ops", "ca"})
        require_admin_id(x_admin_id)

        clean_project = project_id.strip()
        if not clean_project:
            raise HTTPException(status_code=422, detail="project_id is required")

        task_fn = getattr(generate_milestone_demands, "delay", None)
        if callable(task_fn):
            task = task_fn(
                clean_project,
                payload.construction_stage.strip(),
                f"{payload.milestone_percent:.2f}",
            )
            return {
                "status": "accepted",
                "project_id": clean_project,
                "task_id": str(getattr(task, "id", "")),
                "message": "Milestone demand generation queued",
            }

        result = generate_milestone_demands(
            clean_project,
            payload.construction_stage.strip(),
            f"{payload.milestone_percent:.2f}",
        )
        return {
            "status": "ok",
            "project_id": clean_project,
            "task_mode": "sync-fallback",
            "result": result,
        }

    return router
