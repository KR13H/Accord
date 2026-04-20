from __future__ import annotations

import base64
import json
import os
import secrets
import sqlite3
import uuid
from datetime import datetime
from typing import Any, Callable

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


GSTN_PORTAL_NAME = os.getenv("ACCORD_GSTN_PORTAL_NAME", "GSTN Sandbox")
GSTN_RP_ID = os.getenv("ACCORD_GSTN_RP_ID", "localhost")


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode(value + padding)


class GstnSandboxApi:
    def __init__(self) -> None:
        self._sessions: dict[str, dict[str, Any]] = {}

    def request_otp(self, *, gstin: str) -> dict[str, Any]:
        clean_gstin = gstin.strip().upper()
        otp = f"{secrets.randbelow(1000000):06d}"
        auth_token = uuid.uuid4().hex
        sek = secrets.token_bytes(32)
        otp_reference = uuid.uuid4().hex[:12].upper()
        self._sessions[auth_token] = {
            "gstin": clean_gstin,
            "otp": otp,
            "sek": sek,
            "otp_reference": otp_reference,
            "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "verified": False,
        }
        return {
            "gstin": clean_gstin,
            "otp_reference": otp_reference,
            "auth_token": auth_token,
            "sek": _b64url_encode(sek),
            "masked_mobile": "XXXXXXXX90",
            "portal": GSTN_PORTAL_NAME,
            "rp_id": GSTN_RP_ID,
        }

    def peek_otp(self, *, auth_token: str) -> str:
        session = self._sessions.get(auth_token)
        if session is None:
            raise ValueError("auth_token not found")
        return str(session["otp"])

    def verify_otp(self, *, auth_token: str, otp: str) -> dict[str, Any]:
        session = self._sessions.get(auth_token)
        if session is None:
            raise ValueError("auth_token not found")
        if str(session["otp"]) != otp.strip():
            raise ValueError("invalid otp")
        session["verified"] = True
        session["verified_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        return {
            "auth_token": auth_token,
            "gstin": session["gstin"],
            "sek": _b64url_encode(session["sek"]),
            "verified": True,
        }

    def encrypt_payload(self, *, auth_token: str, payload: dict[str, Any]) -> dict[str, Any]:
        session = self._sessions.get(auth_token)
        if session is None:
            raise ValueError("auth_token not found")
        if not session.get("verified"):
            raise ValueError("otp not verified")

        sek = session["sek"]
        nonce = secrets.token_bytes(12)
        aesgcm = AESGCM(sek)
        plaintext = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        ciphertext = aesgcm.encrypt(nonce, plaintext, None)
        return {
            "auth_token": auth_token,
            "gstin": session["gstin"],
            "sek": _b64url_encode(sek),
            "nonce": _b64url_encode(nonce),
            "encrypted_payload": _b64url_encode(ciphertext),
            "payload_size": len(plaintext),
        }

    def submit_gstr1(self, *, auth_token: str, payload: dict[str, Any]) -> dict[str, Any]:
        encrypted = self.encrypt_payload(auth_token=auth_token, payload=payload)
        arn = f"ARN-{datetime.utcnow().strftime('%Y%m%d')}-{uuid.uuid4().hex[:12].upper()}"
        return {
            "status": "SUCCESS",
            "arn": arn,
            "gateway": GSTN_PORTAL_NAME,
            "auth_token": auth_token,
            "gstin": encrypted["gstin"],
            "sek": encrypted["sek"],
            "nonce": encrypted["nonce"],
            "encrypted_payload": encrypted["encrypted_payload"],
            "payload_size": encrypted["payload_size"],
        }