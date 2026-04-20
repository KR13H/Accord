from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import httpx

META_GRAPH_API_VERSION = os.getenv("META_GRAPH_API_VERSION", "v21.0")
META_PHONE_NUMBER_ID = os.getenv("META_PHONE_NUMBER_ID", "")
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "")
DEFAULT_OWNER_PHONE = os.getenv("ACCORD_WHATSAPP_OWNER_PHONE", os.getenv("ACCORD_WHATSAPP_DEFAULT_TO", "919999999999"))


def build_purchase_order_message(*, supplier_name: str, draft_message: str, approval_url: str) -> str:
    return (
        f"Accord auto-reorder draft for {supplier_name}.\n"
        f"{draft_message.strip()}\n"
        f"Approve now: {approval_url}"
    )


def send_purchase_order_approval(
    *,
    supplier_name: str,
    draft_message: str,
    approval_url: str,
    to_phone: str | None = None,
) -> dict[str, Any]:
    recipient = (to_phone or DEFAULT_OWNER_PHONE or "").strip()
    if not recipient:
        return {
            "status": "SKIPPED",
            "reason": "Recipient phone is not configured",
            "supplier_name": supplier_name,
            "approval_url": approval_url,
        }

    message_body = build_purchase_order_message(
        supplier_name=supplier_name,
        draft_message=draft_message,
        approval_url=approval_url,
    )

    if not META_PHONE_NUMBER_ID or not META_ACCESS_TOKEN:
        return {
            "status": "MOCKED",
            "supplier_name": supplier_name,
            "recipient": recipient,
            "approval_url": approval_url,
            "message": message_body,
        }

    endpoint = f"https://graph.facebook.com/{META_GRAPH_API_VERSION}/{META_PHONE_NUMBER_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": recipient,
        "type": "text",
        "text": {"preview_url": False, "body": message_body},
    }

    try:
        response = httpx.post(
            endpoint,
            json=payload,
            headers={"Authorization": f"Bearer {META_ACCESS_TOKEN}", "Content-Type": "application/json"},
            timeout=20.0,
        )
        response.raise_for_status()
        return {
            "status": "SENT",
            "supplier_name": supplier_name,
            "recipient": recipient,
            "approval_url": approval_url,
            "message": message_body,
            "response": response.json(),
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "FAILED",
            "supplier_name": supplier_name,
            "recipient": recipient,
            "approval_url": approval_url,
            "message": message_body,
            "error": str(exc),
        }