from __future__ import annotations

import base64
import hmac
import os
import time
from hashlib import sha256

from fastapi import Header, HTTPException


SME_SESSION_SECRET = os.getenv("ACCORD_SME_SESSION_SECRET", "accord-local-sme-session-secret")
SME_SESSION_TTL_SECONDS = int(os.getenv("ACCORD_SME_SESSION_TTL_SECONDS", "43200"))


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode(value + padding)


def normalize_sme_role(role: str | None) -> str:
    if role is None:
        return "owner"
    cleaned = role.strip().lower()
    return cleaned or "owner"


def mint_sme_session_token(*, username: str, role: str, credential_id: str, ttl_seconds: int | None = None) -> str:
    expires_at = int(time.time()) + max(60, ttl_seconds or SME_SESSION_TTL_SECONDS)
    normalized_role = normalize_sme_role(role)
    safe_username = username.strip()
    safe_credential_id = credential_id.strip()
    payload = f"{safe_username}:{normalized_role}:{safe_credential_id}:{expires_at}"
    signature = hmac.new(SME_SESSION_SECRET.encode("utf-8"), payload.encode("utf-8"), sha256).digest()
    return f"{_b64url_encode(payload.encode('utf-8'))}.{_b64url_encode(signature)}"


def verify_sme_session_token(token: str | None) -> dict[str, str] | None:
    if token is None:
        return None

    parts = token.strip().split(".")
    if len(parts) != 2:
        return None

    try:
        payload_bytes = _b64url_decode(parts[0])
        incoming_sig = _b64url_decode(parts[1])
        payload = payload_bytes.decode("utf-8")
        username, role, credential_id, expires_raw = payload.split(":", 3)
        expires_at = int(expires_raw)
    except Exception:  # noqa: BLE001
        return None

    if expires_at < int(time.time()):
        return None

    expected_sig = hmac.new(SME_SESSION_SECRET.encode("utf-8"), payload_bytes, sha256).digest()
    if not hmac.compare_digest(expected_sig, incoming_sig):
        return None

    return {
        "username": username,
        "role": normalize_sme_role(role),
        "credential_id": credential_id,
        "expires_at": str(expires_at),
    }


def require_sme_session(x_sme_session_token: str | None = Header(default=None, alias="X-SME-Session-Token")) -> dict[str, str]:
    session = verify_sme_session_token(x_sme_session_token)
    if session is None:
        raise HTTPException(status_code=401, detail="Valid SME session token required")
    return session


def require_sme_owner(
    x_sme_role: str | None = Header(default=None, alias="X-SME-Role"),
    x_sme_session_token: str | None = Header(default=None, alias="X-SME-Session-Token"),
) -> str:
    role = normalize_sme_role(x_sme_role)
    if role == "owner":
        return role

    session = verify_sme_session_token(x_sme_session_token)
    if session is not None and session.get("role") == "owner":
        return "owner"

    raise HTTPException(status_code=403, detail="SME owner access required")
