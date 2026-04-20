from __future__ import annotations

import os
from datetime import datetime
from typing import Any

import httpx

META_GRAPH_API_VERSION = os.getenv("META_GRAPH_API_VERSION", "v21.0")
META_PHONE_NUMBER_ID = os.getenv("META_PHONE_NUMBER_ID", "")
META_ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN", "")
META_TEMPLATE_NAME = os.getenv("META_TEMPLATE_NAME", "accord_demand_letter")
DEFAULT_RECIPIENT = os.getenv("ACCORD_WHATSAPP_DEFAULT_TO", "919999999999")


def build_meta_demand_payload(*, to_phone: str, media_url: str, template_name: str | None = None) -> dict[str, Any]:
    chosen_template = template_name or META_TEMPLATE_NAME
    return {
        "messaging_product": "whatsapp",
        "to": to_phone,
        "type": "template",
        "template": {
            "name": chosen_template,
            "language": {"code": "en"},
            "components": [
                {
                    "type": "header",
                    "parameters": [
                        {
                            "type": "document",
                            "document": {
                                "link": media_url,
                                "filename": "Demand_Letter.pdf",
                            },
                        }
                    ],
                },
                {
                    "type": "body",
                    "parameters": [
                        {"type": "text", "text": "Accord Milestone Demand"},
                        {"type": "text", "text": datetime.utcnow().date().isoformat()},
                    ],
                },
            ],
        },
    }


def send_demand_letter_notification(*, booking_id: str, media_url: str, to_phone: str | None = None) -> dict[str, Any]:
    recipient = (to_phone or DEFAULT_RECIPIENT or "").strip()
    if not recipient:
        return {
            "status": "SKIPPED",
            "booking_id": booking_id,
            "reason": "Recipient phone is not configured",
        }

    payload = build_meta_demand_payload(to_phone=recipient, media_url=media_url)

    if not META_PHONE_NUMBER_ID or not META_ACCESS_TOKEN:
        return {
            "status": "MOCKED",
            "booking_id": booking_id,
            "endpoint": None,
            "payload": payload,
        }

    endpoint = f"https://graph.facebook.com/{META_GRAPH_API_VERSION}/{META_PHONE_NUMBER_ID}/messages"

    try:
        response = httpx.post(
            endpoint,
            json=payload,
            headers={
                "Authorization": f"Bearer {META_ACCESS_TOKEN}",
                "Content-Type": "application/json",
            },
            timeout=20.0,
        )
        response.raise_for_status()
        data = response.json()
        return {
            "status": "SENT",
            "booking_id": booking_id,
            "endpoint": endpoint,
            "response": data,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "FAILED",
            "booking_id": booking_id,
            "endpoint": endpoint,
            "error": str(exc),
            "payload": payload,
        }
