from __future__ import annotations

import logging


_logger = logging.getLogger("accord.sms")


def send_otp_sms(*, phone_number: str, otp_code: str) -> None:
    """Local-dev mock sender for Twilio/MSG91 integrations."""
    masked = phone_number[-4:] if len(phone_number) >= 4 else phone_number
    _logger.info("OTP dispatched via mock SMS provider: phone_suffix=%s otp=%s", masked, otp_code)
