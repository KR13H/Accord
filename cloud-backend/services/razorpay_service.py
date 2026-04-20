from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import razorpay  # type: ignore


@dataclass(slots=True)
class RazorpayConfig:
    key_id: str
    key_secret: str
    plan_id: str
    webhook_secret: str


class RazorpayService:
    def __init__(self) -> None:
        self.config = RazorpayConfig(
            key_id=os.getenv("RAZORPAY_KEY_ID", "").strip(),
            key_secret=os.getenv("RAZORPAY_KEY_SECRET", "").strip(),
            plan_id=os.getenv("RAZORPAY_PLAN_ID", "").strip(),
            webhook_secret=os.getenv("RAZORPAY_WEBHOOK_SECRET", "").strip(),
        )
        if not self.config.key_id or not self.config.key_secret:
            raise RuntimeError("Razorpay credentials are not configured")
        self.client = razorpay.Client(auth=(self.config.key_id, self.config.key_secret))

    def create_subscription(self, *, business_id: str, total_count: int = 120) -> dict[str, Any]:
        if not self.config.plan_id:
            raise RuntimeError("RAZORPAY_PLAN_ID is not configured")

        starts_at = int((datetime.utcnow() + timedelta(minutes=2)).timestamp())
        payload = {
            "plan_id": self.config.plan_id,
            "total_count": max(1, total_count),
            "quantity": 1,
            "customer_notify": 1,
            "start_at": starts_at,
            "notes": {
                "business_id": business_id,
                "tier": "premium-ai-cloud",
                "monthly_price_inr": "999",
            },
        }
        return self.client.subscription.create(data=payload)

    def verify_webhook_signature(self, *, payload: bytes, signature: str) -> None:
        self.client.utility.verify_webhook_signature(payload.decode("utf-8"), signature, self.config.webhook_secret)


_service_singleton: RazorpayService | None = None


def get_razorpay_service() -> RazorpayService:
    global _service_singleton
    if _service_singleton is None:
        _service_singleton = RazorpayService()
    return _service_singleton
