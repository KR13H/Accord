from __future__ import annotations

from fastapi import APIRouter, Header
from pydantic import BaseModel, Field


class RiskAssessmentRequest(BaseModel):
    customer_id: int = Field(..., gt=0)
    amount: float = Field(..., gt=0)


class RiskAssessmentResponse(BaseModel):
    risk_score: float
    band: str
    recommendation: str


def create_default_risk_router(get_conn, require_role, require_admin_id):
    router = APIRouter(prefix="/api/v1/risk", tags=["risk"])

    @router.post("/assess", response_model=RiskAssessmentResponse)
    def assess_risk(
        payload: RiskAssessmentRequest,
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ):
        require_role(x_role, {"admin", "ca", "auditor"})
        require_admin_id(x_admin_id)

        # Lightweight deterministic heuristic used as a safe fallback.
        utilization = min(payload.amount / 1_000_000.0, 1.0)
        risk_score = round(0.2 + (0.6 * utilization), 4)
        if risk_score < 0.4:
            band = "LOW"
            recommendation = "Auto-approve with standard terms"
        elif risk_score < 0.7:
            band = "MEDIUM"
            recommendation = "Require additional review"
        else:
            band = "HIGH"
            recommendation = "Escalate for manual underwriting"

        return RiskAssessmentResponse(
            risk_score=risk_score,
            band=band,
            recommendation=recommendation,
        )

    return router
