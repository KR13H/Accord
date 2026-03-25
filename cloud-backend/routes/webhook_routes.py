from __future__ import annotations

import os
import sqlite3
from typing import Any, Callable

import httpx
from fastapi import APIRouter
from pydantic import BaseModel, Field


def _infer_intent_local(message: str) -> str:
    text = message.strip().lower()
    if any(token in text for token in ["balance", "due", "pending", "kitna", "baki", "बाकी"]):
        return "balance_query"
    if any(token in text for token in ["receipt", "rasid", "रसीद", "download"]):
        return "download_receipt"
    return "unknown"


def _infer_intent_ollama(message: str) -> str:
    host = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
    model = os.getenv("ACCORD_WHATSAPP_INTENT_MODEL", "llama3:8b")
    prompt = (
        "Classify the message into one token only: balance_query or download_receipt or unknown.\n"
        f"Message: {message.strip()}"
    )
    try:
        with httpx.Client(timeout=15) as client:
            resp = client.post(
                f"{host}/api/generate",
                json={"model": model, "prompt": prompt, "stream": False},
            )
            resp.raise_for_status()
            raw = str(resp.json().get("response") or "unknown").strip().lower()
            if "balance" in raw:
                return "balance_query"
            if "receipt" in raw:
                return "download_receipt"
            return "unknown"
    except Exception:
        return _infer_intent_local(message)


class WhatsAppWebhookIn(BaseModel):
    # Twilio-style payload
    From: str | None = None
    Body: str | None = None
    # Meta-style payload
    messages: list[dict[str, Any]] | None = None


def create_webhook_router(get_conn: Callable[[], sqlite3.Connection]) -> APIRouter:
    router = APIRouter(prefix="/api/v1/webhooks", tags=["webhooks"])

    @router.post("/whatsapp")
    def post_whatsapp_webhook(payload: WhatsAppWebhookIn) -> dict[str, Any]:
        phone = str(payload.From or "").strip()
        message = str(payload.Body or "").strip()

        if payload.messages and (not phone or not message):
            msg = payload.messages[0]
            phone = phone or str(msg.get("from") or "").strip()
            text_obj = msg.get("text") if isinstance(msg.get("text"), dict) else {}
            message = message or str(text_obj.get("body") or "").strip()

        if not phone or not message:
            return {
                "status": "ignored",
                "reason": "phone/message not present",
            }

        intent = _infer_intent_ollama(message)

        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tenant_contacts (
                    tenant_id TEXT PRIMARY KEY,
                    phone TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.commit()

            row = conn.execute(
                "SELECT tenant_id FROM tenant_contacts WHERE phone = ?",
                (phone,),
            ).fetchone()
            tenant_id = str(row["tenant_id"]) if row is not None else ""

            if not tenant_id:
                return {
                    "status": "ok",
                    "intent": intent,
                    "reply": "We could not map your number to a tenant profile. Please contact support.",
                }

            pending = conn.execute(
                """
                SELECT COALESCE(SUM(CAST(amount AS REAL)), 0) AS due
                FROM rent_invoices
                WHERE tenant_id = ? AND status = 'OPEN'
                """,
                (tenant_id,),
            ).fetchone()
            due_value = float(pending["due"] if pending is not None else 0)

        if intent == "balance_query":
            reply = f"Tenant {tenant_id}, your pending rent balance is INR {due_value:,.2f}."
        elif intent == "download_receipt":
            reply = (
                f"Tenant {tenant_id}, receipt download is queued. "
                "Please check your registered email in a few minutes."
            )
        else:
            reply = "Please ask for your pending balance or request receipt download."

        return {
            "status": "ok",
            "intent": intent,
            "tenant_id": tenant_id,
            "reply": reply,
        }

    return router
