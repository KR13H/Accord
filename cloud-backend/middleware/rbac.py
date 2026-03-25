from __future__ import annotations

import base64
import json
from typing import Any, Iterable

from fastapi import Header, HTTPException, Request


ROLE_DEFAULT_PERMISSIONS: dict[str, set[str]] = {
    "super_admin": {"view_reports", "create_bookings", "execute_allocations", "manage_users", "manage_compliance"},
    "admin": {"view_reports", "create_bookings", "execute_allocations", "manage_users", "manage_compliance"},
    "spv_manager": {"view_reports", "create_bookings", "execute_allocations"},
    "manager": {"view_reports", "create_bookings", "execute_allocations"},
    "ops": {"view_reports", "create_bookings"},
    "ca": {"view_reports", "execute_allocations", "manage_compliance"},
    "data_entry_clerk": {"create_bookings"},
    "clerk": {"create_bookings"},
}

PATH_PERMISSION_RULES: list[tuple[str, str, set[str]]] = [
    ("POST", "/api/v1/bookings", {"create_bookings"}),
    ("PUT", "/api/v1/bookings", {"create_bookings"}),
    ("DELETE", "/api/v1/bookings", {"create_bookings"}),
    ("POST", "/api/v1/rera/allocations", {"execute_allocations"}),
    ("GET", "/api/v1/reports", {"view_reports"}),
    ("GET", "/api/v1/dashboard", {"view_reports"}),
]


def _b64url_decode(value: str) -> bytes:
    padding = "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _decode_jwt_claims(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    try:
        payload = json.loads(_b64url_decode(parts[1]).decode("utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _permissions_from_headers_or_claims(
    *,
    x_permissions: str | None,
    x_role: str | None,
    authorization: str | None,
) -> set[str]:
    if x_permissions and x_permissions.strip():
        return {part.strip() for part in x_permissions.split(",") if part.strip()}

    claims: dict[str, Any] = {}
    if authorization:
        parts = authorization.strip().split(" ", 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            claims = _decode_jwt_claims(parts[1])

    raw_permissions = claims.get("permissions")
    if isinstance(raw_permissions, list):
        return {str(item).strip() for item in raw_permissions if str(item).strip()}

    role = str(claims.get("role") or x_role or "").strip().lower()
    return set(ROLE_DEFAULT_PERMISSIONS.get(role, set()))


class RequirePermissions:
    def __init__(self, permissions: Iterable[str]) -> None:
        self.permissions = {str(item).strip() for item in permissions if str(item).strip()}

    def __call__(
        self,
        x_permissions: str | None = Header(default=None, alias="X-Permissions"),
        x_role: str | None = Header(default=None, alias="X-Role"),
        authorization: str | None = Header(default=None, alias="Authorization"),
    ) -> dict[str, Any]:
        granted = _permissions_from_headers_or_claims(
            x_permissions=x_permissions,
            x_role=x_role,
            authorization=authorization,
        )
        missing = sorted(self.permissions - granted)
        if missing:
            raise HTTPException(status_code=403, detail=f"Forbidden: missing permissions {', '.join(missing)}")
        return {
            "permissions": sorted(granted),
            "role": (x_role or "").strip().lower(),
        }


def enforce_rbac_policy(request: Request) -> None:
    method = request.method.upper()
    path = request.url.path

    matched_required: set[str] = set()
    for rule_method, prefix, required in PATH_PERMISSION_RULES:
        if method == rule_method and path.startswith(prefix):
            matched_required = required
            break

    if not matched_required:
        return

    granted = _permissions_from_headers_or_claims(
        x_permissions=request.headers.get("X-Permissions"),
        x_role=request.headers.get("X-Role"),
        authorization=request.headers.get("Authorization"),
    )
    missing = sorted(matched_required - granted)
    if missing:
        raise HTTPException(status_code=403, detail=f"Forbidden: missing permissions {', '.join(missing)}")
