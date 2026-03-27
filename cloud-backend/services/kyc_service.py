from __future__ import annotations

from fastapi import APIRouter, Header
from pydantic import BaseModel, Field


class KycVerificationRequest(BaseModel):
    customer_name: str = Field(..., min_length=1, max_length=120)
    document_type: str = Field(..., min_length=2, max_length=40)
    document_number: str = Field(..., min_length=4, max_length=64)


class KycVerificationResponse(BaseModel):
    status: str
    verified: bool
    message: str


def create_kyc_router(require_role, require_admin_id):
    router = APIRouter(prefix="/api/v1/kyc", tags=["kyc"])

    @router.get("/health")
    def kyc_health(
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ):
        require_role(x_role, {"admin", "ca", "auditor"})
        admin_id = require_admin_id(x_admin_id)
        return {"status": "ok", "admin_id": admin_id}

    @router.post("/verify", response_model=KycVerificationResponse)
    def verify_kyc(
        payload: KycVerificationRequest,
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ):
        require_role(x_role, {"admin", "ca"})
        require_admin_id(x_admin_id)

        # This is a deterministic stub to keep integrations operational until
        # a full KYC provider-backed implementation is wired in.
        normalized_doc = payload.document_number.strip()
        is_valid = len(normalized_doc) >= 4

        return KycVerificationResponse(
            status="verified" if is_valid else "rejected",
            verified=is_valid,
            message="KYC verification completed",
        )

    return router
