from __future__ import annotations

from utils import redis_runtime


def test_set_otp_uses_strict_ttl(monkeypatch):
    now = 1_700_000_000.0

    def fake_now() -> float:
        return now

    monkeypatch.setattr(redis_runtime, "_epoch_seconds", fake_now)

    phone = "+919876543210"
    redis_runtime.set_otp_code(phone, "123456", ttl_seconds=300)

    ttl = redis_runtime.cache_ttl_seconds(redis_runtime.otp_key(phone))
    assert ttl is not None
    assert 0 < ttl <= 300

    assert redis_runtime.get_otp_code(phone) == "123456"

    now = now + 301
    assert redis_runtime.get_otp_code(phone) is None
