from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import Any, Callable

import httpx
from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from services.ai_support_agent import AiSupportAgent
from services.ticket_service import create_automated_ticket


class ChatMessageIn(BaseModel):
    role: str = Field(min_length=1, max_length=16)
    content: str = Field(min_length=1, max_length=4000)


class SupportChatIn(BaseModel):
    message: str = Field(min_length=1, max_length=4000)
    history: list[ChatMessageIn] = Field(default_factory=list, max_length=50)


def create_chat_router(
    get_conn: Callable[[], sqlite3.Connection],
    require_role: Callable[..., str],
    require_admin_id: Callable[..., int],
) -> APIRouter:
    router = APIRouter(prefix="/api/v1/support", tags=["support", "chat"])
    agent = AiSupportAgent(model="llama3", timeout=15.0)

    @router.post("/chat")
    async def post_support_chat(
        payload: SupportChatIn,
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_role(x_role, {"admin", "ca", "ops"})
        user_id = require_admin_id(x_admin_id)

        # Keep only the last 6 turns to reduce local model context load.
        windowed_history = payload.history[-6:]
        history_for_model = [
            {"role": item.role.strip().lower(), "content": item.content.strip()}
            for item in windowed_history
            if item.content.strip()
        ]

        try:
            answer, escalation = await agent.respond(
                message=payload.message.strip(),
                history=history_for_model,
            )
        except httpx.TimeoutException:
            return {
                "status": "ok",
                "reply": "The local AI assistant timed out. Please try again in a moment.",
                "escalated": False,
                "ticket_id": None,
            }
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=502, detail=f"support agent failed: {exc}") from exc

        ticket_id: str | None = None
        final_reply = answer
        escalated = bool(escalation)

        if escalation:
            summary = str(escalation.get("summary") or payload.message.strip())
            priority = str(escalation.get("priority") or "medium")
            with closing(get_conn()) as conn:
                conn.row_factory = sqlite3.Row
                ticket_id = create_automated_ticket(conn, user_id=user_id, summary=summary, priority=priority)

            final_reply = (
                f"I have escalated this to our human support team. "
                f"Your reference number is **{ticket_id}**. We will reach out shortly."
            )

        return {
            "status": "ok",
            "reply": final_reply,
            "escalated": escalated,
            "ticket_id": ticket_id,
        }

    return router
