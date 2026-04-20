from __future__ import annotations

import secrets
import sqlite3
import time
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field

from services.sms_service import send_otp_sms
from utils.jwt_manager import (
    mint_access_token,
    mint_refresh_token,
    refresh_cookie_name,
    verify_token,
)
from utils.redis_runtime import (
    blocklist_token_jti,
    delete_otp_code,
    get_otp_code,
    is_token_jti_blocklisted,
    set_otp_code,
)


OTP_TTL_SECONDS = 300


class RequestOtpIn(BaseModel):
    phone_number: str = Field(min_length=8, max_length=24)
    role: str = Field(default="owner", min_length=3, max_length=32)


class VerifyOtpIn(BaseModel):
    phone_number: str = Field(min_length=8, max_length=24)
    otp_code: str = Field(min_length=6, max_length=6)
    role: str = Field(default="owner", min_length=3, max_length=32)


def _normalize_phone(phone_number: str) -> str:
    return "".join(ch for ch in phone_number if ch.isdigit() or ch == "+")


def _normalize_role(role: str) -> str:
    clean = role.strip().lower()
    return clean or "owner"


def _issue_tokens(response: Response, *, subject: str, role: str, auth_method: str) -> dict[str, Any]:
    access_token, access_exp = mint_access_token(subject=subject, role=role, auth_method=auth_method)
    refresh_token, refresh_exp, _ = mint_refresh_token(subject=subject, role=role, auth_method=auth_method)
    now = int(time.time())

    response.set_cookie(
        key=refresh_cookie_name(),
        value=refresh_token,
        httponly=True,
        secure=True,
        samesite="strict",
        max_age=max(60, refresh_exp - now),
        path="/api/v1/auth",
    )

    return {
        "access_token": access_token,
        "access_token_expires_at": access_exp,
        "token_type": "bearer",
    }


def create_otp_auth_router(get_conn: Callable[[], sqlite3.Connection]) -> APIRouter:
    del get_conn
    router = APIRouter(prefix="/api/v1/auth", tags=["auth", "otp"])

    @router.post("/request-otp")
    def post_request_otp(payload: RequestOtpIn) -> dict[str, Any]:
        normalized_phone = _normalize_phone(payload.phone_number)
        if len(normalized_phone) < 8:
            raise HTTPException(status_code=422, detail="Invalid phone number")

        otp_code = f"{secrets.randbelow(1_000_000):06d}"
        set_otp_code(normalized_phone, otp_code, ttl_seconds=OTP_TTL_SECONDS)
        send_otp_sms(phone_number=normalized_phone, otp_code=otp_code)

        return {
            "status": "ok",
            "phone_number": normalized_phone,
            "otp_ttl_seconds": OTP_TTL_SECONDS,
            "delivery": "mock",
        }

    @router.post("/verify-otp")
    def post_verify_otp(payload: VerifyOtpIn, response: Response) -> dict[str, Any]:
        normalized_phone = _normalize_phone(payload.phone_number)
        cached_otp = get_otp_code(normalized_phone)
        if cached_otp is None:
            raise HTTPException(status_code=401, detail="OTP expired or not found")

        if payload.otp_code.strip() != cached_otp:
            raise HTTPException(status_code=401, detail="Invalid OTP code")

        delete_otp_code(normalized_phone)
        role = _normalize_role(payload.role)
        token_payload = _issue_tokens(
            response,
            subject=normalized_phone,
            role=role,
            auth_method="otp",
        )

        return {
            "status": "ok",
            "auth_method": "otp",
            "phone_number": normalized_phone,
            "role": role,
            **token_payload,
        }

    @router.post("/refresh")
    def post_refresh_token(request: Request, response: Response) -> dict[str, Any]:
        refresh_token = request.cookies.get(refresh_cookie_name())
        if not refresh_token:
            raise HTTPException(status_code=401, detail="Missing refresh token cookie")

        try:
            claims = verify_token(refresh_token, expected_type="refresh")
        except ValueError as exc:
            raise HTTPException(status_code=401, detail=str(exc)) from exc

        refresh_jti = str(claims.get("jti") or "")
        if not refresh_jti:
            raise HTTPException(status_code=401, detail="refresh token missing jti")

        if is_token_jti_blocklisted(refresh_jti):
            raise HTTPException(status_code=401, detail="refresh token revoked")

        now = int(time.time())
        refresh_exp = int(claims.get("exp", now))
        remaining_ttl = max(60, refresh_exp - now)
        blocklist_token_jti(refresh_jti, ttl_seconds=remaining_ttl)

        subject = str(claims.get("sub") or "")
        role = str(claims.get("role") or "owner")
        auth_method = str(claims.get("auth_method") or "otp")

        access_token, access_exp = mint_access_token(subject=subject, role=role, auth_method=auth_method)
        new_refresh_token, new_refresh_exp, _ = mint_refresh_token(subject=subject, role=role, auth_method=auth_method)

        response.set_cookie(
            key=refresh_cookie_name(),
            value=new_refresh_token,
            httponly=True,
            secure=True,
            samesite="strict",
            max_age=max(60, new_refresh_exp - now),
            path="/api/v1/auth",
        )

        return {
            "status": "ok",
            "token_type": "bearer",
            "access_token": access_token,
            "access_token_expires_at": access_exp,
            "refresh_ttl_seconds": max(60, new_refresh_exp - now),
        }

    return router
