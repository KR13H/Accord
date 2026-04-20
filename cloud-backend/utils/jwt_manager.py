from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
import uuid
from typing import Any


JWT_SECRET = os.getenv("ACCORD_JWT_SECRET", os.getenv("ACCORD_SME_SESSION_SECRET", "accord-local-jwt-secret"))
JWT_ACCESS_TTL_SECONDS = int(os.getenv("ACCORD_JWT_ACCESS_TTL_SECONDS", "900"))
JWT_REFRESH_TTL_SECONDS = int(os.getenv("ACCORD_JWT_REFRESH_TTL_SECONDS", str(7 * 24 * 3600)))
JWT_ISSUER = os.getenv("ACCORD_JWT_ISSUER", "accord")


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _sign(value: str) -> str:
    sig = hmac.new(JWT_SECRET.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).digest()
    return _b64url_encode(sig)


def _mint_token(claims: dict[str, Any]) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    encoded_header = _b64url_encode(json.dumps(header, separators=(",", ":"), ensure_ascii=True).encode("utf-8"))
    encoded_claims = _b64url_encode(json.dumps(claims, separators=(",", ":"), ensure_ascii=True).encode("utf-8"))
    signed_part = f"{encoded_header}.{encoded_claims}"
    return f"{signed_part}.{_sign(signed_part)}"


def mint_access_token(*, subject: str, role: str, auth_method: str) -> tuple[str, int]:
    now = int(time.time())
    expires_at = now + max(60, JWT_ACCESS_TTL_SECONDS)
    claims = {
        "iss": JWT_ISSUER,
        "sub": subject,
        "role": role,
        "auth_method": auth_method,
        "token_type": "access",
        "iat": now,
        "exp": expires_at,
        "jti": uuid.uuid4().hex,
    }
    return _mint_token(claims), expires_at


def mint_refresh_token(*, subject: str, role: str, auth_method: str) -> tuple[str, int, str]:
    now = int(time.time())
    expires_at = now + max(300, JWT_REFRESH_TTL_SECONDS)
    jti = uuid.uuid4().hex
    claims = {
        "iss": JWT_ISSUER,
        "sub": subject,
        "role": role,
        "auth_method": auth_method,
        "token_type": "refresh",
        "iat": now,
        "exp": expires_at,
        "jti": jti,
    }
    return _mint_token(claims), expires_at, jti


def verify_token(token: str, *, expected_type: str | None = None) -> dict[str, Any]:
    parts = token.strip().split(".")
    if len(parts) != 3:
        raise ValueError("invalid token format")

    signed_part = f"{parts[0]}.{parts[1]}"
    expected_sig = _sign(signed_part)
    if not hmac.compare_digest(expected_sig, parts[2]):
        raise ValueError("token signature mismatch")

    try:
        payload = json.loads(_b64url_decode(parts[1]).decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise ValueError("invalid token payload") from exc

    if not isinstance(payload, dict):
        raise ValueError("invalid token payload")

    now = int(time.time())
    token_type = str(payload.get("token_type", ""))
    exp = int(payload.get("exp", 0))

    if exp <= now:
        raise ValueError("token expired")
    if expected_type and token_type != expected_type:
        raise ValueError("token type mismatch")

    return payload


def refresh_cookie_name() -> str:
    return "accord_refresh_token"
