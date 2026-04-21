from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
import re
from datetime import datetime, timedelta
from typing import Any, Callable

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, Field
from webauthn import (
    generate_authentication_options,
    generate_registration_options,
    verify_authentication_response,
    verify_registration_response,
)
from webauthn.helpers import base64url_to_bytes, bytes_to_base64url, options_to_json

from services.onboarding_service import inject_demo_data
from utils.jwt_manager import mint_access_token, mint_refresh_token, refresh_cookie_name
from utils.sme_auth import mint_sme_session_token


WEBAUTHN_RP_ID = os.getenv("ACCORD_WEBAUTHN_RP_ID", "localhost")
WEBAUTHN_RP_NAME = os.getenv("ACCORD_WEBAUTHN_RP_NAME", "Accord")
WEBAUTHN_ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv(
        "ACCORD_WEBAUTHN_ALLOWED_ORIGINS",
        "http://localhost:5173,http://127.0.0.1:5173,http://localhost:3000,http://127.0.0.1:3000",
    ).split(",")
    if origin.strip()
]
WEBAUTHN_SESSION_TTL_SECONDS = int(os.getenv("ACCORD_WEBAUTHN_SESSION_TTL_SECONDS", "43200"))


class RegistrationOptionsIn(BaseModel):
    username: str = Field(min_length=3, max_length=120)
    display_name: str | None = Field(default=None, max_length=120)
    role: str = Field(default="cashier", min_length=3, max_length=32)


class RegistrationVerificationIn(BaseModel):
    challenge_id: str = Field(min_length=8, max_length=128)
    credential: dict[str, Any]


class AuthenticationOptionsIn(BaseModel):
    username: str | None = Field(default=None, max_length=120)


class AuthenticationVerificationIn(BaseModel):
    challenge_id: str = Field(min_length=8, max_length=128)
    credential: dict[str, Any]


_challenge_lock = threading.Lock()
_pending_challenges: dict[str, dict[str, Any]] = {}


def ensure_webauthn_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS sme_passkey_credentials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            display_name TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'cashier',
            credential_id TEXT NOT NULL UNIQUE,
            credential_public_key TEXT NOT NULL,
            sign_count INTEGER NOT NULL DEFAULT 0,
            user_handle TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_sme_passkey_credentials_username
        ON sme_passkey_credentials (username, role);
        """
    )


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def _set_refresh_cookie(response: Response, refresh_token: str, refresh_exp: int) -> None:
    ttl = max(60, refresh_exp - int(datetime.utcnow().timestamp()))
    response.set_cookie(
        key=refresh_cookie_name(),
        value=refresh_token,
        httponly=True,
        secure=True,
        samesite="strict",
        max_age=ttl,
        path="/api/v1/auth",
    )


def _clean_origin_list() -> list[str]:
    return WEBAUTHN_ALLOWED_ORIGINS or ["http://localhost:5173"]


def _business_id_from_username(username: str) -> str:
    compact = re.sub(r"[^A-Za-z0-9]+", "-", username.strip()).strip("-").upper()
    if not compact:
        compact = "NEW-SME"
    return f"SME-{compact[:24]}"


def _store_challenge(*, operation: str, payload: dict[str, Any]) -> str:
    challenge_id = uuid.uuid4().hex
    expires_at = datetime.utcnow() + timedelta(minutes=5)
    with _challenge_lock:
        _pending_challenges[challenge_id] = {
            **payload,
            "operation": operation,
            "expires_at": expires_at,
        }
    return challenge_id


def _load_challenge(challenge_id: str, *, operation: str) -> dict[str, Any]:
    with _challenge_lock:
        record = _pending_challenges.get(challenge_id)
        if record is None:
            raise ValueError("challenge not found")
        if record.get("operation") != operation:
            raise ValueError("challenge operation mismatch")
        if datetime.utcnow() > record["expires_at"]:
            _pending_challenges.pop(challenge_id, None)
            raise ValueError("challenge expired")
        return dict(record)


def _consume_challenge(challenge_id: str) -> None:
    with _challenge_lock:
        _pending_challenges.pop(challenge_id, None)


def _credential_id_from_response(credential: dict[str, Any]) -> str:
    for key in ("id", "rawId"):
        value = credential.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    raise ValueError("credential id missing")


def _lookup_credential(conn: sqlite3.Connection, credential_id: str) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM sme_passkey_credentials WHERE credential_id = ?",
        (credential_id,),
    ).fetchone()
    if row is None:
        raise ValueError("credential not registered")
    return row


def create_sme_webauthn_router(get_conn: Callable[[], sqlite3.Connection]) -> APIRouter:
    router = APIRouter(prefix="/api/v1/auth", tags=["auth", "webauthn"])

    @router.post("/generate-registration-options")
    def post_generate_registration_options(payload: RegistrationOptionsIn) -> dict[str, Any]:
        clean_username = payload.username.strip()
        clean_display_name = (payload.display_name or payload.username).strip() or clean_username
        clean_role = payload.role.strip().lower() or "cashier"
        user_id = uuid.uuid4().bytes

        options = generate_registration_options(
            rp_id=WEBAUTHN_RP_ID,
            rp_name=WEBAUTHN_RP_NAME,
            user_name=clean_username,
            user_id=user_id,
            user_display_name=clean_display_name,
        )
        challenge_id = _store_challenge(
            operation="registration",
            payload={
                "username": clean_username,
                "display_name": clean_display_name,
                "role": clean_role,
                "user_id": bytes_to_base64url(user_id),
                "challenge": bytes_to_base64url(options.challenge),
            },
        )

        return {
            "status": "ok",
            "challenge_id": challenge_id,
            "options": json.loads(options_to_json(options)),
        }

    @router.post("/verify-registration")
    def post_verify_registration(payload: RegistrationVerificationIn, request: Request, response: Response) -> dict[str, Any]:
        challenge = _load_challenge(payload.challenge_id, operation="registration")
        origin = request.headers.get("origin") or ""
        allowed_origin = [origin] if origin else _clean_origin_list()

        try:
            verified = verify_registration_response(
                credential=payload.credential,
                expected_challenge=base64url_to_bytes(str(challenge["challenge"])),
                expected_rp_id=WEBAUTHN_RP_ID,
                expected_origin=allowed_origin,
                require_user_presence=True,
                require_user_verification=False,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=422, detail=f"registration verification failed: {exc}") from exc

        credential_id = bytes_to_base64url(verified.credential_id)
        credential_public_key = bytes_to_base64url(verified.credential_public_key)
        now = _now_iso()

        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            ensure_webauthn_schema(conn)
            conn.execute(
                """
                INSERT INTO sme_passkey_credentials (
                    username,
                    display_name,
                    role,
                    credential_id,
                    credential_public_key,
                    sign_count,
                    user_handle,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(credential_id) DO UPDATE SET
                    username = excluded.username,
                    display_name = excluded.display_name,
                    role = excluded.role,
                    credential_public_key = excluded.credential_public_key,
                    sign_count = excluded.sign_count,
                    user_handle = excluded.user_handle,
                    updated_at = excluded.updated_at
                """,
                (
                    str(challenge["username"]),
                    str(challenge["display_name"]),
                    str(challenge["role"]),
                    credential_id,
                    credential_public_key,
                    int(verified.sign_count),
                    str(challenge["user_id"]),
                    now,
                    now,
                ),
            )
            conn.commit()

        _consume_challenge(payload.challenge_id)
        business_id = _business_id_from_username(str(challenge["username"]))
        onboarding = inject_demo_data(get_conn, business_id)
        session_token = mint_sme_session_token(
            username=str(challenge["username"]),
            role=str(challenge["role"]),
            credential_id=credential_id,
            ttl_seconds=WEBAUTHN_SESSION_TTL_SECONDS,
        )
        access_token, access_exp = mint_access_token(
            subject=str(challenge["username"]),
            role=str(challenge["role"]),
            auth_method="passkey",
        )
        refresh_token, refresh_exp, _ = mint_refresh_token(
            subject=str(challenge["username"]),
            role=str(challenge["role"]),
            auth_method="passkey",
        )
        _set_refresh_cookie(response, refresh_token, refresh_exp)

        return {
            "status": "ok",
            "username": str(challenge["username"]),
            "display_name": str(challenge["display_name"]),
            "role": str(challenge["role"]),
            "credential_id": credential_id,
            "sign_count": int(verified.sign_count),
            "access_token": access_token,
            "access_token_expires_at": access_exp,
            "token_type": "bearer",
            "session_token": session_token,
            "business_id": business_id,
            "onboarding": onboarding,
            "user_verified": bool(verified.user_verified),
        }

    @router.post("/generate-authentication-options")
    def post_generate_authentication_options(payload: AuthenticationOptionsIn) -> dict[str, Any]:
        options = generate_authentication_options(rp_id=WEBAUTHN_RP_ID)
        challenge_id = _store_challenge(
            operation="authentication",
            payload={
                "username": payload.username.strip() if payload.username else None,
                "challenge": bytes_to_base64url(options.challenge),
            },
        )
        return {
            "status": "ok",
            "challenge_id": challenge_id,
            "options": json.loads(options_to_json(options)),
        }

    @router.post("/verify-authentication")
    def post_verify_authentication(payload: AuthenticationVerificationIn, request: Request, response: Response) -> dict[str, Any]:
        challenge = _load_challenge(payload.challenge_id, operation="authentication")
        origin = request.headers.get("origin") or ""
        allowed_origin = [origin] if origin else _clean_origin_list()
        credential_id = _credential_id_from_response(payload.credential)

        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            ensure_webauthn_schema(conn)
            credential_row = _lookup_credential(conn, credential_id)

        try:
            verified = verify_authentication_response(
                credential=payload.credential,
                expected_challenge=base64url_to_bytes(str(challenge["challenge"])),
                expected_rp_id=WEBAUTHN_RP_ID,
                expected_origin=allowed_origin,
                credential_public_key=base64url_to_bytes(str(credential_row["credential_public_key"])),
                credential_current_sign_count=int(credential_row["sign_count"]),
                require_user_verification=False,
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=422, detail=f"authentication verification failed: {exc}") from exc

        now = _now_iso()
        with get_conn() as conn:
            conn.execute(
                "UPDATE sme_passkey_credentials SET sign_count = ?, updated_at = ? WHERE credential_id = ?",
                (int(verified.new_sign_count), now, credential_id),
            )
            conn.commit()

        _consume_challenge(payload.challenge_id)
        session_token = mint_sme_session_token(
            username=str(credential_row["username"]),
            role=str(credential_row["role"]),
            credential_id=credential_id,
            ttl_seconds=WEBAUTHN_SESSION_TTL_SECONDS,
        )
        access_token, access_exp = mint_access_token(
            subject=str(credential_row["username"]),
            role=str(credential_row["role"]),
            auth_method="passkey",
        )
        refresh_token, refresh_exp, _ = mint_refresh_token(
            subject=str(credential_row["username"]),
            role=str(credential_row["role"]),
            auth_method="passkey",
        )
        _set_refresh_cookie(response, refresh_token, refresh_exp)

        return {
            "status": "ok",
            "username": str(credential_row["username"]),
            "display_name": str(credential_row["display_name"]),
            "role": str(credential_row["role"]),
            "credential_id": credential_id,
            "sign_count": int(verified.new_sign_count),
            "access_token": access_token,
            "access_token_expires_at": access_exp,
            "token_type": "bearer",
            "session_token": session_token,
            "user_verified": bool(verified.user_verified),
        }

    return router