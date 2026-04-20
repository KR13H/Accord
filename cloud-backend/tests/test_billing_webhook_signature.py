from __future__ import annotations

import pytest
from fastapi import HTTPException

from routes.billing_routes import _verify_webhook_signature


class _ValidService:
    def verify_webhook_signature(self, *, payload: bytes, signature: str) -> None:
        assert payload == b"{}"
        assert signature == "good-signature"


class _InvalidService:
    def verify_webhook_signature(self, *, payload: bytes, signature: str) -> None:
        raise ValueError("signature mismatch")


def test_verify_webhook_signature_accepts_valid(monkeypatch):
    monkeypatch.setattr("routes.billing_routes.get_razorpay_service", lambda: _ValidService())
    _verify_webhook_signature(b"{}", "good-signature")


def test_verify_webhook_signature_rejects_invalid(monkeypatch):
    monkeypatch.setattr("routes.billing_routes.get_razorpay_service", lambda: _InvalidService())
    with pytest.raises(HTTPException) as exc:
        _verify_webhook_signature(b"{}", "bad-signature")
    assert exc.value.status_code == 401
