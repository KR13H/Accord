from __future__ import annotations

import base64
import json
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Header, HTTPException, Request
from pydantic import BaseModel, Field

from services.email_service import send_admin_email

router = APIRouter(prefix="/api/v1/support", tags=["Support"])
ADMIN_SUPPORT_EMAIL = "krish.in02@gmail.com"


class ContactRequest(BaseModel):
    subject: str = Field(min_length=3, max_length=160)
    urgency: str = Field(min_length=3, max_length=16)
    message: str = Field(min_length=10, max_length=2000)


def _decode_bearer_payload(authorization: str | None) -> dict[str, Any]:
    if not authorization:
        return {}
    parts = authorization.strip().split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return {}

    token_segments = parts[1].split(".")
    if len(token_segments) < 2:
        return {}

    payload_segment = token_segments[1]
    padding = "=" * ((4 - len(payload_segment) % 4) % 4)

    try:
        raw_payload = base64.urlsafe_b64decode(payload_segment + padding)
        parsed = json.loads(raw_payload.decode("utf-8"))
        if isinstance(parsed, dict):
            return parsed
    except Exception:  # noqa: BLE001
        return {}
    return {}


def _resolve_actor(authorization: str | None, x_admin_id: str | None, x_role: str | None) -> dict[str, Any]:
    claims = _decode_bearer_payload(authorization)

    user_id_candidate = claims.get("user_id") or claims.get("sub") or claims.get("admin_id") or x_admin_id
    role = str(claims.get("role") or x_role or "unknown").strip().lower() or "unknown"
    email = str(claims.get("email") or claims.get("preferred_username") or "").strip()
    name = str(claims.get("name") or claims.get("full_name") or "").strip()

    user_id = 0
    try:
        user_id = int(str(user_id_candidate).strip())
    except Exception:  # noqa: BLE001
        user_id = 0

    return {
        "user_id": user_id,
        "role": role,
        "email": email,
        "name": name,
    }


@router.post("/contact")
async def contact_admin(
    req: ContactRequest,
    request: Request,
    background_tasks: BackgroundTasks,
    authorization: str | None = Header(default=None, alias="Authorization"),
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, str]:
    urgency = req.urgency.strip().capitalize()
    if urgency not in {"Low", "Medium", "High"}:
        raise HTTPException(status_code=422, detail="urgency must be one of Low, Medium, High")

    actor = _resolve_actor(authorization, x_admin_id, x_role)

    actor_context = "\n".join(
        [
            f"User ID: {actor['user_id']}",
            f"Role: {actor['role']}",
            f"Email: {actor['email'] or 'N/A'}",
            f"Name: {actor['name'] or 'N/A'}",
            f"Client IP: {request.client.host if request.client else 'unknown'}",
            f"Path: {request.url.path}",
        ]
    )

    subject = f"[ACCORD SUPPORT][{urgency.upper()}] {req.subject.strip()}"
    body = "\n\n".join(
        [
            "A new support request was submitted.",
            actor_context,
            "Message:",
            req.message.strip(),
        ]
    )

    background_tasks.add_task(send_admin_email, ADMIN_SUPPORT_EMAIL, subject, body)
    return {"status": "success", "message": "Admin notified."}
