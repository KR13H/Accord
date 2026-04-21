from __future__ import annotations

import calendar
import sqlite3
from datetime import date, datetime
from typing import Any, Callable

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from services.gstn_sandbox_api import GstnSandboxApi
from services.universal_accounting import DEFAULT_BUSINESS_ID, get_transactions_between
from utils.feature_flags import is_feature_enabled
from utils.gstr1_generator import generate_gstr1_payload


class GstnFilingIn(BaseModel):
    business_id: str = Field(default=DEFAULT_BUSINESS_ID, min_length=1, max_length=64)
    gstin: str = Field(min_length=15, max_length=15)
    period: str | None = Field(default=None, description="YYYYMM filing period")


def _resolve_period(period: str | None) -> tuple[str, date, date]:
    filing_period = period or datetime.utcnow().strftime("%Y%m")
    start_date = datetime.strptime(f"{filing_period}01", "%Y%m%d").date()
    last_day = calendar.monthrange(start_date.year, start_date.month)[1]
    end_date = date(start_date.year, start_date.month, last_day)
    return filing_period, start_date, end_date


def create_gstn_sandbox_router(get_conn: Callable[[], sqlite3.Connection], require_role: Callable[[str | None, set[str]], str], require_admin_id: Callable[[str | None], int]) -> APIRouter:
    router = APIRouter(prefix="/api/v1/sme/compliance", tags=["sme", "gstn-sandbox"])
    sandbox = GstnSandboxApi()

    @router.post("/file-gstr1")
    def post_file_gstr1(
        payload: GstnFilingIn,
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        if not is_feature_enabled("gstn_sandbox_api", default=True):
            raise HTTPException(status_code=503, detail="Service Unavailable: Maintenance Mode")
        require_role(x_role, {"admin", "ca"})
        require_admin_id(x_admin_id)

        filing_period, start_date, end_date = _resolve_period(payload.period)
        try:
            transactions = get_transactions_between(
                get_conn,
                business_id=payload.business_id,
                start_date=start_date,
                end_date=end_date,
            )
            gstn_payload = generate_gstr1_payload(
                transactions,
                business_id=payload.business_id,
                gstin=payload.gstin.strip().upper(),
                period_yyyymm=filing_period,
            )
            otp_session = sandbox.request_otp(gstin=payload.gstin.strip().upper())
            verified = sandbox.verify_otp(auth_token=otp_session["auth_token"], otp=sandbox.peek_otp(auth_token=otp_session["auth_token"]))
            result = sandbox.submit_gstr1(auth_token=otp_session["auth_token"], payload=gstn_payload)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=502, detail=f"GSTN sandbox filing failed: {exc}") from exc

        return {
            "status": "ok",
            "business_id": payload.business_id,
            "period": filing_period,
            "gateway": result["gateway"],
            "arn": result["arn"],
            "auth_token": result["auth_token"],
            "sek": result["sek"],
            "nonce": result["nonce"],
            "encrypted_payload": result["encrypted_payload"],
            "gstn_payload": gstn_payload,
            "otp_reference": otp_session["otp_reference"],
            "otp_verified": verified["verified"],
        }

    return router