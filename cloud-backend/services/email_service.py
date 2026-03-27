from __future__ import annotations

import logging
import os
import smtplib
from email.message import EmailMessage

logger = logging.getLogger("accord.email")

SMTP_HOST = os.getenv("ACCORD_SMTP_HOST", "")
SMTP_PORT = int(os.getenv("ACCORD_SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("ACCORD_SMTP_USERNAME", "")
SMTP_PASSWORD = os.getenv("ACCORD_SMTP_PASSWORD", "")
SMTP_FROM_EMAIL = os.getenv("ACCORD_SMTP_FROM", SMTP_USERNAME or "no-reply@accord.local")
SMTP_USE_TLS = os.getenv("ACCORD_SMTP_USE_TLS", "true").strip().lower() in {"1", "true", "yes"}


def send_admin_email(admin_email: str, subject: str, message: str) -> dict[str, str]:
    """Send support mail to the platform admin without interrupting API flow."""
    if not SMTP_HOST:
        detail = "SMTP host is not configured (set ACCORD_SMTP_HOST)"
        logger.warning("support-email skipped: %s", detail)
        return {"status": "skipped", "detail": detail}

    mail = EmailMessage()
    mail["From"] = SMTP_FROM_EMAIL
    mail["To"] = admin_email
    mail["Subject"] = subject
    mail.set_content(message)

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as smtp:
            if SMTP_USE_TLS:
                smtp.starttls()
            if SMTP_USERNAME:
                smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
            smtp.send_message(mail)
        return {"status": "sent", "detail": "Email dispatched"}
    except Exception as exc:  # noqa: BLE001
        # Email failures should never crash business requests.
        logger.exception("support-email failed for admin recipient=%s", admin_email)
        return {"status": "failed", "detail": str(exc)}
