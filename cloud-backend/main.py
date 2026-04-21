from __future__ import annotations

import asyncio
import logging
import sqlite3
import base64
import csv
import io
import hmac
import smtplib
import os
import sys
import secrets
import shutil
import time
import uuid
import tracemalloc
from contextlib import closing
from datetime import date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any, List
import xml.etree.ElementTree as ET
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from email.message import EmailMessage

from fastapi import BackgroundTasks, Depends, FastAPI, File, Header, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse
from fastapi.routing import APIRoute
import httpx
from pydantic import BaseModel, Field
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from starlette.middleware.base import BaseHTTPMiddleware

try:
    from sqlalchemy import create_engine, text
except Exception:  # noqa: BLE001
    create_engine = None
    text = None

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from services.accounting_service import AccountingService
from services.ai_variance_analyzer import AiVarianceAnalyzer
from services.banking_service import BankingService
from services.compliance_service import ComplianceService
from services.commission_service import CommissionService
from services.currency_service import CurrencyService
from services.demo_service import reset_demo_environment
from services.govt_bridge_service import GovtBridgeService
from services.gst_service import GstService
from services.ingest_service import IngestService
from services.inventory_service import InventoryService
from services.kyc_service import create_kyc_router
from services.predictive_defaults import create_default_risk_router
from services.report_service import ReportService
from services.rera_allocation_service import AllocationInput, ReraAllocationService
from services.realtime_events import ca_event_bus
from services.statutory_service import StatutoryService
from services.telemetry_service import TelemetryService
from services.voice_service import VoiceService
from services.voucher_service import VoucherService
from middleware.audit_logger import register_audit_logger_middleware
from middleware.rbac import enforce_rbac_policy
from routes.booking_routes import create_booking_router
try:
    from routes.broker_routes import create_broker_router
except ModuleNotFoundError:  # pragma: no cover
    create_broker_router = None
from routes.chat_routes import create_chat_router
from routes.billing_routes import create_billing_router
from routes.invoice_routes import create_invoice_router
from routes.otp_auth_routes import create_otp_auth_router
from routes.portal_routes import create_portal_router
try:
    from routes.pricing_routes import create_pricing_router
except ModuleNotFoundError:  # pragma: no cover
    create_pricing_router = None

try:
    from routes.project_routes import create_project_router
except ModuleNotFoundError:  # pragma: no cover
    create_project_router = None
    from routes.autonomous_purchasing_routes import create_autonomous_purchasing_router
    from routes.gstn_sandbox_routes import create_gstn_sandbox_router
    from routes.sme_webauthn_routes import create_sme_webauthn_router
from routes.sme_inventory_routes import create_sme_inventory_router
from routes.iot_telemetry_routes import create_iot_telemetry_router
from routes.sme_payable_routes import create_sme_payable_router
from routes.supplier_routes import create_supplier_router
from routes.superadmin_routes import create_superadmin_router
from routes.sme_routes import create_sme_router
from routes.support_routes import router as support_router
from routes.tds_routes import create_phase7_router
from routes.vendor_routes import create_vendor_router
from routes.webhook_routes import create_webhook_router
from websockets.sme_sync import create_sme_sync_router
try:
    from services.approval_service import (
        create_approval_router,
        ensure_approval_schema,
        get_allocation_approval_status,
        initialize_allocation_approval,
    )
except ModuleNotFoundError:  # pragma: no cover
    create_approval_router = None

    def ensure_approval_schema(conn) -> None:  # noqa: ANN001
        return None

    def get_allocation_approval_status(conn, allocation_event_id: int) -> dict[str, Any]:  # noqa: ANN001
        return {
            "status": "unavailable",
            "reason": "approval_service module not installed",
            "allocation_event_id": allocation_event_id,
        }

    def initialize_allocation_approval(  # noqa: ANN001
        conn,
        allocation_event_id: int,
        maker_admin_id: int,
        receipt_amount: Decimal,
    ) -> dict[str, Any]:
        return {
            "status": "unavailable",
            "reason": "approval_service module not installed",
            "allocation_event_id": allocation_event_id,
            "maker_admin_id": maker_admin_id,
            "receipt_amount": str(receipt_amount),
        }
from routers.mobile_gateway import router as mobile_gateway_router
from utils.throttle import rate_limit_heavy_task
from utils.db_runtime import (
    get_database_url,
    is_postgres_url,
    resolve_sqlite_db_path,
    sqlalchemy_database_url,
)


DATABASE_URL = get_database_url()
DB_BACKEND = "postgresql" if is_postgres_url(DATABASE_URL) else "sqlite"
SQLITE_DB_PATH = resolve_sqlite_db_path(DATABASE_URL)

CHART_OF_ACCOUNTS = [
    ("Cash", "Asset", 0.0),
    ("Bank", "Asset", 0.0),
    ("Accounts Receivable", "Asset", 0.0),
    ("Inventory", "Asset", 0.0),
    ("GST Input", "Asset", 0.0),
    ("Accounts Payable", "Liability", 0.0),
    ("GST Output", "Liability", 0.0),
    ("Owner Equity", "Equity", 0.0),
    ("Sales Revenue", "Revenue", 0.0),
    ("Purchases", "Expense", 0.0),
    ("Operating Expenses", "Expense", 0.0),
]

MONEY_QUANT = Decimal("0.0001")
B2CL_INTER_STATE_THRESHOLD = Decimal("100000.0000")
DUAL_APPROVAL_THRESHOLD = Decimal("50000.0000")
GST_2026_ALLOWED_SLABS = {Decimal("5.0000"), Decimal("18.0000"), Decimal("40.0000")}
IMS_ALLOWED_STATUS = {"ACCEPTED", "REJECTED", "PENDING"}
BIOMETRIC_TOKEN_TTL_SECONDS = 300
BIOMETRIC_SECRET = os.getenv("ACCORD_BIOMETRIC_SECRET", "accord-local-biometric-secret")
SSE_TOKEN_TTL_SECONDS = int(os.getenv("ACCORD_SSE_TOKEN_TTL_SECONDS", "3600"))
SSE_TOKEN_SECRET = os.getenv("ACCORD_SSE_TOKEN_SECRET", BIOMETRIC_SECRET)
OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
OLLAMA_CHAT_URL = f"{OLLAMA_HOST}/api/chat"
OLLAMA_TAGS_URL = f"{OLLAMA_HOST}/api/tags"
OLLAMA_GENERATE_URL = f"{OLLAMA_HOST}/api/generate"
VISION_MODEL = os.getenv("ACCORD_VISION_MODEL", "llava")
RECON_MODEL = os.getenv("ACCORD_RECON_MODEL", "llama3:8b")
FORENSIC_MODEL = os.getenv("ACCORD_FORENSIC_MODEL", "mistral")
VARIANCE_MODEL = os.getenv("ACCORD_VARIANCE_MODEL", RECON_MODEL)
DEPLOYMENT_MODE = os.getenv("ACCORD_DEPLOYMENT_MODE", "sovereign-local").strip().lower() or "sovereign-local"
BACKEND_PUBLIC_URL = os.getenv("BACKEND_PUBLIC_URL", "").strip()
FRONTEND_PUBLIC_URL = os.getenv("FRONTEND_PUBLIC_URL", "").strip()
ACCORD_GSTIN = os.getenv("ACCORD_GSTIN", "29ABCDE1234F1Z5").strip().upper()
SMTP_HOST = os.getenv("ACCORD_SMTP_HOST", "")
SMTP_PORT = int(os.getenv("ACCORD_SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("ACCORD_SMTP_USERNAME", "")
SMTP_PASSWORD = os.getenv("ACCORD_SMTP_PASSWORD", "")
SMTP_FROM_EMAIL = os.getenv("ACCORD_SMTP_FROM", SMTP_USERNAME or "no-reply@accord.local")
SMTP_USE_TLS = os.getenv("ACCORD_SMTP_USE_TLS", "true").strip().lower() in {"1", "true", "yes"}

STORAGE_ROOT = Path(__file__).resolve().parents[1] / "storage"
RECEIPT_STORAGE_DIR = STORAGE_ROOT / "receipts"
TALLY_EXPORT_DIR = STORAGE_ROOT / "tally_exports"
OMNI_INGEST_DIR = STORAGE_ROOT / "omni_ingest"
PROCESSED_ARCHIVE_DIR = STORAGE_ROOT / "processed_archives"
NEXUS_GRAPH_DIR = STORAGE_ROOT / "nexus_graphs"
STUDIO_EXPORT_DIR = STORAGE_ROOT / "studio_exports"
MARKET_INTEL_DIR = STORAGE_ROOT / "market_intel"
MARKET_UPLOAD_DIR = MARKET_INTEL_DIR / "uploads"
MARKET_REPORT_DIR = MARKET_INTEL_DIR / "reports"
RAM_DISK_BUFFER = Path("/Volumes/AccordCache/receipt_buffer")
MAX_PARALLEL_WORKERS = 16  # M3 adaptive upper bound for mixed I/O + OCR workloads
ENABLE_TRACEMALLOC = os.getenv("ACCORD_ENABLE_TRACEMALLOC", "0").strip().lower() in {"1", "true", "yes"}
TRACEMALLOC_FRAMES = int(os.getenv("ACCORD_TRACEMALLOC_FRAMES", "25"))

CORS_DEFAULT_ORIGINS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]


def resolve_cors_allow_origins() -> list[str]:
    frontend_base = os.getenv("VITE_API_BASE_URL", "").strip()
    raw = os.getenv("CORS_ALLOW_ORIGINS", "").strip()
    if raw:
        requested = [origin.strip() for origin in raw.split(",") if origin.strip()]
    elif frontend_base:
        requested = [frontend_base]
    else:
        requested = CORS_DEFAULT_ORIGINS

    allow: list[str] = []
    for origin in requested:
        if origin == "*":
            continue
        allow.append(origin)

    return allow or CORS_DEFAULT_ORIGINS


SECURITY_CSP_POLICY = (
    "default-src 'self'; "
    "script-src 'self' https://checkout.razorpay.com https://api.razorpay.com; "
    "connect-src 'self' https://api.razorpay.com; "
    "img-src 'self' data: https://*.razorpay.com; "
    "style-src 'self' 'unsafe-inline'; "
    "frame-src 'self' https://api.razorpay.com https://checkout.razorpay.com; "
    "object-src 'none'; "
    "base-uri 'self'; "
    "form-action 'self';"
)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        response.headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains; preload"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Content-Security-Policy"] = SECURITY_CSP_POLICY
        return response

try:
    import cv2  # type: ignore
except Exception:  # noqa: BLE001
    cv2 = None

try:
    import pandas as pd  # type: ignore
except Exception:  # noqa: BLE001
    pd = None

try:
    import fitz  # type: ignore
except Exception:  # noqa: BLE001
    fitz = None

try:
    import networkx as nx  # type: ignore
except Exception:  # noqa: BLE001
    nx = None

try:
    import whisper as whisper_lib  # type: ignore
except Exception:  # noqa: BLE001
    whisper_lib = None

try:
    import pytesseract  # type: ignore
except Exception:  # noqa: BLE001
    pytesseract = None

try:
    from PIL import Image
except Exception:  # noqa: BLE001
    Image = None

try:
    import psutil  # type: ignore
except Exception:  # noqa: BLE001
    psutil = None

WHISPER_MODEL_CACHE: Any | None = None
INGEST_RETRY_BUCKETS: dict[str, dict[str, Any]] = {}
LATENCY_WARN_THRESHOLD_MS = 3000
api_logger = logging.getLogger("accord.api")
TRACEMALLOC_LAST_SNAPSHOT: tracemalloc.Snapshot | None = None

# Keep UQC values constrained to GSTN-style codes; alias map handles common user inputs.
ALLOWED_UQC = {
    "BAG",
    "BOX",
    "BTL",
    "BUN",
    "CAN",
    "CTN",
    "DOZ",
    "DRM",
    "GMS",
    "KGS",
    "KLR",
    "LTR",
    "MTR",
    "NOS",
    "OTH",
    "PAC",
    "PCS",
    "PRS",
    "QTL",
    "ROL",
    "SET",
    "SQF",
    "SQM",
    "TBS",
    "TGM",
    "THD",
    "TON",
    "TUB",
    "UGS",
    "UNT",
    "YDS",
}

UQC_ALIASES = {
    "UNIT": "UNT",
    "UNITS": "UNT",
    "PIECE": "PCS",
    "PIECES": "PCS",
    "NUMBER": "NOS",
    "NUMBERS": "NOS",
    "NO": "NOS",
    "KG": "KGS",
    "KILOGRAM": "KGS",
    "KILOGRAMS": "KGS",
    "LITER": "LTR",
    "LITERS": "LTR",
    "LITRE": "LTR",
    "LITRES": "LTR",
    "METER": "MTR",
    "METERS": "MTR",
    "METRE": "MTR",
    "METRES": "MTR",
    "OTHER": "OTH",
    "OTHERS": "OTH",
}


def money(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(MONEY_QUANT)


def money_str(value: Any) -> str:
    return f"{money(value):.4f}"


def normalize_currency_code(value: str | None) -> str:
    code = (value or "INR").strip().upper() or "INR"
    service = ensure_currency_service()
    if code in service.EXCHANGE_RATES:
        return code
    return "INR"


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * ((4 - len(value) % 4) % 4)
    return base64.urlsafe_b64decode(value + padding)


def mint_sse_token(*, ca_id: int) -> str:
    expires_at = int(time.time()) + max(60, SSE_TOKEN_TTL_SECONDS)
    payload = f"{ca_id}:{expires_at}"
    sig = hmac.new(SSE_TOKEN_SECRET.encode("utf-8"), payload.encode("utf-8"), sha256).digest()
    return f"{_b64url_encode(payload.encode('utf-8'))}.{_b64url_encode(sig)}"


def verify_sse_token(*, token: str, ca_id: int) -> bool:
    parts = token.split(".")
    if len(parts) != 2:
        return False
    try:
        payload_bytes = _b64url_decode(parts[0])
        incoming_sig = _b64url_decode(parts[1])
        payload = payload_bytes.decode("utf-8")
        payload_ca_id_raw, expires_raw = payload.split(":", 1)
        payload_ca_id = int(payload_ca_id_raw)
        expires_at = int(expires_raw)
    except Exception:  # noqa: BLE001
        return False

    if payload_ca_id != ca_id or expires_at < int(time.time()):
        return False

    expected_sig = hmac.new(SSE_TOKEN_SECRET.encode("utf-8"), payload_bytes, sha256).digest()
    return hmac.compare_digest(incoming_sig, expected_sig)


def infer_currency_code_from_text(text: str | None) -> str:
    lowered = str(text or "").lower()
    if any(token in lowered for token in ["usd", "dollar", "new york", "$"]):
        return "USD"
    if any(token in lowered for token in ["aed", "dirham", "dubai"]):
        return "AED"
    if any(token in lowered for token in ["gbp", "pound", "london"]):
        return "GBP"
    if any(token in lowered for token in ["eur", "euro"]):
        return "EUR"
    return "INR"


def resolve_exchange_rate(currency_code: str, incoming_rate: Any = None) -> Decimal:
    service = ensure_currency_service()
    if incoming_rate is not None and str(incoming_rate).strip() != "":
        parsed = parse_amount_from_text(str(incoming_rate))
        if parsed > 0:
            return money(parsed)
    return money(service.get_rate(currency_code))


def validate_hsn_code_format(hsn_code: str) -> str:
    code = hsn_code.strip()
    if (not code.isdigit()) or len(code) < 6:
        raise HTTPException(status_code=422, detail="HSN code must be numeric and at least 6 digits")
    return code


def normalize_supply_source(value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in {"DIRECT", "ECO"}:
        raise HTTPException(status_code=422, detail="supply_source must be DIRECT or ECO")
    return normalized


def validate_gstin(value: str) -> str:
    gstin = value.strip().upper()
    if not re.fullmatch(r"[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][0-9A-Z]Z[0-9A-Z]", gstin):
        raise HTTPException(status_code=422, detail="GSTIN must be a valid 15-character format")
    return gstin


def normalize_uqc(value: str) -> str:
    uqc = value.strip().upper()
    uqc = UQC_ALIASES.get(uqc, uqc)
    if not re.fullmatch(r"[A-Z]{2,3}", uqc):
        raise HTTPException(status_code=422, detail="UQC must be 2-3 uppercase letters")
    if uqc not in ALLOWED_UQC:
        raise HTTPException(status_code=422, detail=f"Unsupported UQC code: {uqc}")
    return uqc


def normalize_ims_status(value: str) -> str:
    status = value.strip().upper()
    if status not in IMS_ALLOWED_STATUS:
        raise HTTPException(status_code=422, detail="ims_status must be ACCEPTED, REJECTED, or PENDING")
    return status


def succeeding_month_cutoff(invoice_dt: date) -> date:
    if invoice_dt.month == 12:
        return date(invoice_dt.year + 1, 1, 11)
    return date(invoice_dt.year, invoice_dt.month + 1, 11)


def is_high_risk_delay(invoice_dt: date, filed_dt: date) -> bool:
    return filed_dt > succeeding_month_cutoff(invoice_dt)


def payment_advice_for_score(score: float) -> dict[str, Any]:
    if score < 40:
        return {
            "alert": "RED",
            "advice": "WITHHOLD_GST_PORTION",
            "message": "Vendor trust is critically low. Consider net-of-tax payment until filing behavior stabilizes.",
        }
    if score < 60:
        return {
            "alert": "YELLOW",
            "advice": "REVIEW_BEFORE_PAYMENT",
            "message": "Vendor trust is below safe threshold. Review IMS and filing history before releasing payment.",
        }
    return {
        "alert": "GREEN",
        "advice": "STANDARD_PAYMENT",
        "message": "Vendor trust is in safe zone.",
    }


def compute_reversal_risk(conn: sqlite3.Connection, as_of: date, min_credit_balance: Decimal | None = None) -> dict[str, Any]:
    current_fy_start_year = as_of.year if as_of.month >= 4 else as_of.year - 1
    previous_fy_start = date(current_fy_start_year - 1, 4, 1)
    previous_fy_end = date(current_fy_start_year, 3, 31)
    mandatory_reversal_cutoff = date(current_fy_start_year, 9, 20)
    interest_cutoff = date(current_fy_start_year, 9, 30)

    rows = conn.execute(
        """
        SELECT je.reference,
               je.date,
               je.counterparty_gstin,
               je.ims_status,
               tl.tax_amount
        FROM journal_entries je
        JOIN tax_ledger tl ON tl.entry_id = je.id
        WHERE tl.supply_type = 'B2B'
          AND je.date >= ?
          AND je.date <= ?
          AND je.ims_status != 'ACCEPTED'
        ORDER BY je.date ASC, je.reference ASC
        """,
        (previous_fy_start.isoformat(), previous_fy_end.isoformat()),
    ).fetchall()

    immediate_reversal_risk = Decimal("0")
    references: list[str] = []
    for row in rows:
        immediate_reversal_risk += money(row["tax_amount"])
        references.append(str(row["reference"]))

    min_balance = money(min_credit_balance) if min_credit_balance is not None else None
    liability_offset = money(min_balance - immediate_reversal_risk) if min_balance is not None else None
    safe_harbor_applicable = min_balance is not None and min_balance >= immediate_reversal_risk

    # Section 50(3) safe-harbor: if the credit ledger fully covered reversal amount, interest is nil.
    projected_annual_interest = Decimal("0") if safe_harbor_applicable else money(immediate_reversal_risk * Decimal("0.18"))
    if safe_harbor_applicable or as_of <= interest_cutoff:
        accrued_interest = Decimal("0")
    else:
        delay_days = (as_of - interest_cutoff).days
        accrued_interest = money((immediate_reversal_risk * Decimal("0.18") * Decimal(delay_days)) / Decimal("365"))

    return {
        "as_of_date": as_of.isoformat(),
        "previous_financial_year": {
            "start": previous_fy_start.isoformat(),
            "end": previous_fy_end.isoformat(),
        },
        "rule_37a": {
            "mandatory_reversal_cutoff": mandatory_reversal_cutoff.isoformat(),
            "interest_cutoff": interest_cutoff.isoformat(),
            "immediate_reversal_risk": money_str(immediate_reversal_risk),
            "projected_annual_interest_18pct": money_str(projected_annual_interest),
            "accrued_interest_if_past_cutoff": money_str(accrued_interest),
            "at_risk_invoice_count": len(references),
            "at_risk_references": references,
            "hard_stop": as_of >= mandatory_reversal_cutoff and len(references) > 0,
            "safe_harbor": {
                "min_credit_balance": money_str(min_balance) if min_balance is not None else None,
                "liability_offset": money_str(liability_offset) if liability_offset is not None else None,
                "interest_rate_applied_pct": "0.0000" if safe_harbor_applicable else "18.0000",
                "interest_outcome": "INR_0_SAFE_HARBOR" if safe_harbor_applicable else "INR_18PCT_STANDARD",
                "status": "SAFE_HARBOR_APPLICABLE" if safe_harbor_applicable else "STANDARD_INTEREST_APPLIES",
                "legal_basis": "Sec_50(3)_Full_Cover" if safe_harbor_applicable else "Sec_50(3)_Standard_Interest",
            },
        },
    }


def build_safe_harbor_certificate_pdf(
    *,
    batch_id: int,
    reference: str,
    as_of_date: str,
    min_credit_balance: Decimal,
    reversal_amount: Decimal,
    liability_offset: Decimal,
    safe_harbor_applicable: bool,
    legal_basis: str,
    batch_integrity_hash: str,
    generated_by: int,
) -> bytes:
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    y = height - 56

    def line(text: str, gap: int = 18, font: str = "Helvetica", size: int = 11) -> None:
        nonlocal y
        pdf.setFont(font, size)
        pdf.drawString(48, y, text)
        y -= gap

    pdf.setTitle(f"Accord Safe Harbor Certificate {reference}")
    line("ACCORD COMPLIANCE ENGINE", gap=22, font="Helvetica-Bold", size=14)
    line("Section 50(3) Safe Harbor Legal Defense Certificate", gap=18, font="Helvetica-Bold", size=12)
    line(f"Generated At (UTC): {datetime.utcnow().isoformat(timespec='seconds')}Z")
    line(f"Generated By Admin ID: {generated_by}")
    line(f"Reversal Batch ID: {batch_id}")
    line(f"Reversal Reference: {reference}")
    line(f"As-of Date: {as_of_date}")
    line("-" * 110, gap=14)
    line(f"Minimum Monthly Credit Balance (MMB): INR {money_str(min_credit_balance)}")
    line(f"Rule 37A Reversal Amount: INR {money_str(reversal_amount)}")
    line(f"Liability Offset: INR {money_str(liability_offset)}")
    line(f"Interest Outcome Under Section 50(3): {'0.0000%' if safe_harbor_applicable else '18.0000% p.a.'}")
    line(f"Legal Basis: {legal_basis}")
    line("-" * 110, gap=14)
    if safe_harbor_applicable:
        line(
            "Legal Argument: The minimum monthly Electronic Credit Ledger balance fully covered the reversal amount;"
        )
        line("therefore, interest payable is NIL as per Accord Section 50(3) safe-harbor interpretation.")
    else:
        line("Legal Argument: Minimum monthly credit balance did not fully cover the reversal amount.")
        line("Standard Section 50(3) interest framework applies.")
    line("-" * 110, gap=14)
    line("Accord SHA-256 Integrity Hash (Batch Fingerprint):", gap=16, font="Helvetica-Bold")
    line(batch_integrity_hash, gap=20, font="Courier", size=10)
    line(
        "This certificate is generated from immutable journal and audit records. "
        "Any tampering after export invalidates legal reliability.",
        gap=18,
        size=10,
    )
    line("Digital Signature: Accord Compliance Engine (System-Signed)", gap=16, font="Helvetica-Bold", size=10)

    pdf.showPage()
    pdf.save()
    content = buffer.getvalue()
    buffer.close()
    return content


def get_local_ecl_snapshot(conn: sqlite3.Connection) -> dict[str, str]:
    gst_input_row = conn.execute("SELECT balance FROM accounts WHERE name = 'GST Input' LIMIT 1").fetchone()
    gst_output_row = conn.execute("SELECT balance FROM accounts WHERE name = 'GST Output' LIMIT 1").fetchone()
    gst_input = money(gst_input_row["balance"] if gst_input_row else "0")
    gst_output = money(gst_output_row["balance"] if gst_output_row else "0")
    available = money(max(gst_input - gst_output, Decimal("0")))
    half = money(available / Decimal("2"))

    return {
        "igst": "0.0000",
        "cgst": money_str(half),
        "sgst": money_str(half),
        "cess": "0.0000",
        "total": money_str(available),
    }


def parse_structured_json(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        parsed = json.loads(cleaned)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:  # noqa: BLE001
        pass

    match = re.search(r"\{[\s\S]*\}", cleaned)
    if not match:
        return {}
    try:
        parsed = json.loads(match.group(0))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def parse_amount_from_text(value: str | None) -> Decimal:
    if value is None:
        return Decimal("0")
    cleaned = re.sub(r"[^0-9.\-]", "", str(value))
    if not cleaned:
        return Decimal("0")
    try:
        return money(cleaned)
    except Exception:  # noqa: BLE001
        return Decimal("0")


def parse_date_from_text(value: str | None) -> date:
    if not value:
        return date.today()
    raw = value.strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:  # noqa: BLE001
            continue
    return date.today()


def extract_text_with_tesseract(image_path: Path) -> str:
    if Image is None or pytesseract is None:
        return ""
    try:
        if cv2 is not None:
            frame = cv2.imread(str(image_path))
            if frame is not None:
                gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                binary = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 2)
                temp_path = image_path.with_suffix(".ocr.png")
                cv2.imwrite(str(temp_path), binary)
                try:
                    text = pytesseract.image_to_string(Image.open(temp_path))
                finally:
                    if temp_path.exists():
                        temp_path.unlink(missing_ok=True)
                return text.strip()
        return pytesseract.image_to_string(Image.open(image_path)).strip()
    except Exception:  # noqa: BLE001
        return ""


def preprocess_for_neural_ink(image_path: Path) -> tuple[Path, bool]:
    if cv2 is None:
        return image_path, False
    frame = cv2.imread(str(image_path))
    if frame is None:
        return image_path, False

    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    denoised = cv2.fastNlMeansDenoising(gray, None, h=16, templateWindowSize=7, searchWindowSize=21)
    boosted = cv2.convertScaleAbs(denoised, alpha=1.15, beta=5)
    binary = cv2.adaptiveThreshold(
        boosted,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY,
        31,
        2,
    )
    staged = image_path.with_suffix(".neural.png")
    ok = cv2.imwrite(str(staged), binary)
    if not ok:
        return image_path, False
    return staged, True


async def run_ollama_generate(*, model: str, prompt: str, image_b64: str | None = None) -> str:
    payload: dict[str, Any] = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.1},
    }
    if image_b64:
        payload["images"] = [image_b64]

    async with httpx.AsyncClient(timeout=75.0) as client:
        res = await client.post(OLLAMA_GENERATE_URL, json=payload)
        res.raise_for_status()
        data = res.json()
    return str(data.get("response", "")).strip()


def extract_market_text(file_path: Path) -> str:
    ext = file_path.suffix.lower()
    try:
        if ext == ".csv":
            with file_path.open("r", encoding="utf-8", newline="") as handle:
                rows = [dict(row) for row in csv.DictReader(handle)]
            return json.dumps(rows[:200], ensure_ascii=True)
        if ext == ".json":
            parsed = json.loads(file_path.read_text(encoding="utf-8"))
            return json.dumps(parsed, ensure_ascii=True)[:50000]
        if ext in {".txt", ".md", ".log"}:
            return file_path.read_text(encoding="utf-8", errors="ignore")[:50000]
    except Exception:  # noqa: BLE001
        pass
    return (
        f"file_name={file_path.name}; "
        f"file_size_bytes={file_path.stat().st_size}; "
        f"file_ext={file_path.suffix.lower()}; "
        "content_preview_unavailable"
    )


def normalize_market_trend_payload(parsed: dict[str, Any]) -> dict[str, Any]:
    def as_text(value: Any, default: str) -> str:
        if value is None:
            return default
        text_value = str(value).strip()
        return text_value or default

    def as_list(value: Any) -> list[str]:
        if isinstance(value, list):
            items = [str(item).strip() for item in value if str(item).strip()]
            return items[:8]
        if isinstance(value, str) and value.strip():
            chunks = [part.strip() for part in re.split(r"[\n;|]+", value) if part.strip()]
            return chunks[:8]
        return []

    normalized = {
        "trend_summary": as_text(parsed.get("trend_summary"), "Trend signal not available"),
        "demand_signals": as_text(parsed.get("demand_signals"), "Demand signal unavailable"),
        "risk_signals": as_text(parsed.get("risk_signals"), "Risk signal unavailable"),
        "cashflow_pressure": as_text(parsed.get("cashflow_pressure"), "Cashflow signal unavailable"),
        "pricing_momentum": as_text(parsed.get("pricing_momentum"), "Pricing signal unavailable"),
        "recommended_actions": as_text(parsed.get("recommended_actions"), "Review source payload manually"),
        "gst_filing_advice": as_list(parsed.get("gst_filing_advice")),
        "legal_basis": as_list(parsed.get("legal_basis")),
        "risk_level": as_text(parsed.get("risk_level"), "MEDIUM").upper(),
        "confidence_pct": as_text(parsed.get("confidence_pct"), "0"),
        "period_insight": as_text(parsed.get("period_insight"), "No period insight available"),
    }

    if normalized["risk_level"] not in {"LOW", "MEDIUM", "HIGH", "CRITICAL"}:
        normalized["risk_level"] = "MEDIUM"
    return normalized


async def analyze_market_trends_with_ollama(*, file_path: Path, source_kind: str) -> dict[str, Any]:
    content = extract_market_text(file_path)
    prompt = (
        "You are Accord Market Intelligence Analyst for India SME finance teams. "
        "Return ONLY strict JSON with keys: trend_summary, demand_signals, risk_signals, "
        "cashflow_pressure, pricing_momentum, recommended_actions, gst_filing_advice, "
        "legal_basis, risk_level, confidence_pct, period_insight. "
        "Rules: no markdown; no prose outside JSON; keep values concise; risk_level must be one of "
        "LOW, MEDIUM, HIGH, CRITICAL; gst_filing_advice and legal_basis must be arrays of short strings. "
        "For legal_basis, cite applicable GST references where relevant (for example Section 16(2)(aa), "
        "Rule 37A, Section 50(3), GSTR-1 due-date discipline) but do not claim legal finality. "
        "Focus on practical filing and cashflow actions. "
        f"Source kind: {source_kind}. Input data:\n{content[:40000]}"
    )
    try:
        raw = await run_ollama_generate(model=RECON_MODEL, prompt=prompt)
        parsed = parse_structured_json(raw)
        if parsed:
            normalized = normalize_market_trend_payload(parsed)
            normalized["raw_model_output"] = raw[:2000]
            return normalized
    except Exception:  # noqa: BLE001
        pass
    return {
        "trend_summary": "Model unavailable or parse failed",
        "demand_signals": "Pending",
        "risk_signals": "Pending",
        "cashflow_pressure": "Pending",
        "pricing_momentum": "Pending",
        "recommended_actions": "Review source payload manually",
        "gst_filing_advice": [
            "Reconcile GSTR-2B before filing to reduce ITC mismatch risk",
        ],
        "legal_basis": [
            "Section 16(2)(aa)",
            "Rule 37A",
            "Section 50(3)",
        ],
        "risk_level": "MEDIUM",
        "confidence_pct": "0",
        "period_insight": "Model unavailable; use manual CA review",
    }


async def extract_receipt_fields(image_path: Path, ocr_text: str) -> tuple[dict[str, Any], str]:
    image_b64 = base64.b64encode(image_path.read_bytes()).decode("utf-8")
    prompt = (
        "You are an accounting extractor. Return ONLY strict JSON with keys: "
        "date, vendor, gstin, total_amount, hsn, confidence, notes. "
        "If unknown, use empty string. total_amount must be numeric string with up to 2 decimals. "
        f"OCR fallback text:\n{ocr_text if ocr_text else '[none]'}"
    )

    try:
        llava_raw = await run_ollama_generate(model=VISION_MODEL, prompt=prompt, image_b64=image_b64)
        parsed = parse_structured_json(llava_raw)
        if parsed:
            return parsed, llava_raw
    except Exception:  # noqa: BLE001
        pass

    if not ocr_text:
        return {}, ""

    cleanup_prompt = (
        "Convert the following OCR text into strict JSON only with keys "
        "date, vendor, gstin, total_amount, hsn, confidence, notes.\n"
        f"Text:\n{ocr_text}"
    )
    try:
        llama_raw = await run_ollama_generate(model="llama3.2", prompt=cleanup_prompt)
        return parse_structured_json(llama_raw), llama_raw
    except Exception:  # noqa: BLE001
        return {}, ""


def _pick_value(payload: dict[str, Any], keys: list[str]) -> str:
    lowered = {str(k).strip().lower(): v for k, v in payload.items()}
    for key in keys:
        if key in lowered and lowered[key] not in (None, ""):
            return str(lowered[key]).strip()
    return ""


def _extract_excel_rows(file_path: Path) -> list[dict[str, Any]]:
    ext = file_path.suffix.lower()
    if ext == ".csv":
        with file_path.open("r", encoding="utf-8", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]

    if pd is None:
        raise HTTPException(status_code=503, detail="pandas/openpyxl not installed for Excel ingestion")

    dataframe = pd.read_excel(file_path)
    dataframe = dataframe.fillna("")
    return dataframe.to_dict(orient="records")


def _row_to_ledger_fields(row: dict[str, Any]) -> dict[str, Any]:
    date_value = _pick_value(row, ["date", "invoice_date", "voucher_date", "bill_date"])
    vendor = _pick_value(row, ["vendor", "supplier", "party", "name", "counterparty"])
    gstin = _pick_value(row, ["gstin", "vendor_gstin", "supplier_gstin", "counterparty_gstin"]).upper()
    hsn = _pick_value(row, ["hsn", "hsn_code", "item_hsn"])
    amount = _pick_value(row, ["total_amount", "amount", "total", "gross_amount", "net_amount", "invoice_amount"])
    narration = _pick_value(row, ["description", "narration", "remarks", "note"])

    return {
        "date": date_value,
        "vendor": vendor,
        "gstin": gstin,
        "hsn": hsn,
        "total_amount": amount,
        "confidence": "excel-structured",
        "notes": narration,
    }


async def _extract_from_text_blob(raw_text: str, source: str) -> tuple[dict[str, Any], str]:
    if not raw_text.strip():
        return {}, ""

    prompt = (
        "You are an accounting extractor. Return ONLY strict JSON with keys: "
        "date, vendor, gstin, total_amount, hsn, confidence, notes. "
        "Use empty string when unknown and keep total_amount numeric when possible. "
        f"Source type: {source}. Input text:\n{raw_text[:12000]}"
    )
    try:
        raw = await run_ollama_generate(model=RECON_MODEL, prompt=prompt)
        return parse_structured_json(raw), raw
    except Exception:  # noqa: BLE001
        return {}, ""


async def _extract_from_pdf(pdf_path: Path) -> tuple[dict[str, Any], str, str]:
    if fitz is None:
        raise HTTPException(status_code=503, detail="pymupdf is not installed for PDF ingestion")

    text_segments: list[str] = []
    llava_responses: list[str] = []
    best_extracted: dict[str, Any] = {}
    best_score = Decimal("0")

    doc = fitz.open(pdf_path)
    try:
        for page in doc:
            text_segments.append(page.get_text("text") or "")
    finally:
        doc.close()

    merged_text = "\n".join(text_segments).strip()
    if merged_text:
        extracted, raw = await _extract_from_text_blob(merged_text, "pdf")
        if extracted:
            return extracted, raw, merged_text

    doc = fitz.open(pdf_path)
    try:
        pages = min(3, len(doc))
        RAM_DISK_BUFFER.mkdir(parents=True, exist_ok=True)
        for idx in range(pages):
            page = doc[idx]
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
            rendered = RAM_DISK_BUFFER / f"pdf_page_{uuid.uuid4().hex}_{idx}.jpg"
            pix.save(str(rendered))
            try:
                ocr_text = extract_text_with_tesseract(rendered)
                extracted, model_response = await extract_receipt_fields(rendered, ocr_text)
                if model_response:
                    llava_responses.append(model_response)
                score = parse_amount_from_text(str(extracted.get("total_amount", "0")))
                if score > best_score:
                    best_extracted = extracted
                    best_score = score
            finally:
                rendered.unlink(missing_ok=True)
    finally:
        doc.close()

    return best_extracted, "\n\n".join(llava_responses), merged_text


async def _extract_from_video(video_path: Path) -> tuple[dict[str, Any], str, str]:
    if cv2 is None:
        raise HTTPException(status_code=503, detail="opencv-python is not installed for video ingestion")

    capture = cv2.VideoCapture(str(video_path))
    if not capture.isOpened():
        raise HTTPException(status_code=422, detail="Unable to open uploaded video")

    frame_count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    sample_total = 5
    picks: list[int] = []
    if frame_count > 0:
        for i in range(sample_total):
            picks.append(min(frame_count - 1, int((i / max(sample_total - 1, 1)) * (frame_count - 1))))
    else:
        picks = [0, 10, 20, 30, 40]

    best_extracted: dict[str, Any] = {}
    best_score = Decimal("0")
    all_ocr: list[str] = []
    all_responses: list[str] = []
    RAM_DISK_BUFFER.mkdir(parents=True, exist_ok=True)

    try:
        for idx, frame_no in enumerate(picks):
            capture.set(cv2.CAP_PROP_POS_FRAMES, frame_no)
            ok, frame = capture.read()
            if not ok or frame is None:
                continue

            frame_path = RAM_DISK_BUFFER / f"video_frame_{uuid.uuid4().hex}_{idx}.jpg"
            if not cv2.imwrite(str(frame_path), frame):
                continue

            try:
                ocr_text = extract_text_with_tesseract(frame_path)
                if ocr_text:
                    all_ocr.append(ocr_text)
                extracted, model_response = await extract_receipt_fields(frame_path, ocr_text)
                if model_response:
                    all_responses.append(model_response)
                score = parse_amount_from_text(str(extracted.get("total_amount", "0")))
                if score > best_score:
                    best_extracted = extracted
                    best_score = score
            finally:
                frame_path.unlink(missing_ok=True)
    finally:
        capture.release()

    return best_extracted, "\n\n".join(all_responses), "\n".join(all_ocr)


def _post_ledger_entry_from_extract(
    *,
    conn: sqlite3.Connection,
    extracted: dict[str, Any],
    fallback_text: str,
    description_prefix: str,
    actor_role: str,
    admin_id: int,
    source_file_path: Path,
    model_response: str,
    import_status: str,
) -> dict[str, Any]:
    imported_date = parse_date_from_text(str(extracted.get("date", "")))
    vendor_name = str(extracted.get("vendor", "")).strip() or "Omni Vendor"
    gstin = str(extracted.get("gstin", "")).strip().upper()
    hsn = str(extracted.get("hsn", "")).strip()
    amount = parse_amount_from_text(str(extracted.get("total_amount", "0")))
    if amount <= 0:
        amount = parse_amount_from_text(fallback_text)
    if amount <= 0:
        raise HTTPException(status_code=422, detail="Unable to detect a valid amount from ingested file")

    raw_currency = str(extracted.get("currency_code") or extracted.get("currency") or "").strip().upper()
    currency_code = normalize_currency_code(raw_currency or infer_currency_code_from_text(fallback_text))
    exchange_rate = resolve_exchange_rate(currency_code, extracted.get("exchange_rate"))
    amount_base = ensure_currency_service().convert_to_base(amount, currency_code, exchange_rate)

    check_period_lock(conn, imported_date)
    reference = next_journal_reference(conn, imported_date)
    purchases_id = get_account_id_by_name(conn, "Purchases")
    payable_id = get_account_id_by_name(conn, "Accounts Payable")
    created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    conn.execute(
        """
        INSERT INTO journal_entries(
            date,
            reference,
            description,
            company_state_code,
            counterparty_state_code,
            counterparty_gstin,
            eco_gstin,
            supply_source,
            ims_status,
            vendor_legal_name,
            vendor_gstr1_filed_at,
            status,
            reversal_of_id,
            is_filed,
            filed_at,
            filed_export_hash,
            approved_by_1,
            approved_by_2,
            currency_code,
            exchange_rate,
            created_at
        ) VALUES (?, ?, ?, NULL, NULL, ?, NULL, 'DIRECT', 'PENDING', ?, NULL, 'POSTED', NULL, 0, NULL, NULL, NULL, NULL, ?, ?, ?)
        """,
        (
            imported_date.isoformat(),
            reference,
            f"{description_prefix}: {vendor_name}",
            gstin or None,
            vendor_name,
            currency_code,
            money_str(exchange_rate),
            created_at,
        ),
    )
    entry_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])

    conn.execute(
        "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
        (entry_id, purchases_id, money_str(amount_base), money_str(Decimal("0"))),
    )
    update_account_balance(conn, purchases_id, amount_base, Decimal("0"))

    conn.execute(
        "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
        (entry_id, payable_id, money_str(Decimal("0")), money_str(amount_base)),
    )
    update_account_balance(conn, payable_id, Decimal("0"), amount_base)
    fingerprint = stamp_entry_fingerprint(conn, entry_id)

    conn.execute(
        """
        INSERT INTO receipt_imports(entry_id, file_path, ocr_text, extracted_json, model_response, status, created_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            entry_id,
            str(source_file_path),
            fallback_text or None,
            json.dumps(extracted) if extracted else None,
            model_response or None,
            import_status,
            admin_id,
            created_at,
        ),
    )

    _, filename, export_path, _, _ = generate_tally_export(conn, entry_id)
    log_audit(
        conn,
        table_name="journal_entries",
        record_id=entry_id,
        action="OMNI_READER_IMPORT",
        old_value=None,
        new_value={
            "reference": reference,
            "vendor": vendor_name,
            "gstin": gstin,
            "hsn": hsn,
            "amount": money_str(amount_base),
            "transaction_amount": money_str(amount),
            "currency_code": currency_code,
            "exchange_rate": money_str(exchange_rate),
            "entry_fingerprint": fingerprint,
            "source_file": str(source_file_path),
            "tally_export": str(export_path),
            "actor_role": actor_role,
            "import_status": import_status,
        },
        user_id=admin_id,
        high_priority=True,
    )

    return {
        "entry_id": entry_id,
        "reference": reference,
        "entry_fingerprint": fingerprint,
        "vendor": vendor_name,
        "gstin": gstin,
        "hsn": hsn,
        "total_amount": money_str(amount_base),
        "transaction_amount": money_str(amount),
        "currency_code": currency_code,
        "exchange_rate": money_str(exchange_rate),
        "date": imported_date.isoformat(),
        "confidence": extracted.get("confidence", ""),
        "notes": extracted.get("notes", ""),
        "tally_export_file": filename,
        "tally_export_path": str(export_path),
    }


def _flatten_json_records(payload: Any) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        records.append(payload)
        for value in payload.values():
            records.extend(_flatten_json_records(value))
    elif isinstance(payload, list):
        for item in payload:
            records.extend(_flatten_json_records(item))
    return records


def _normalize_2b_record(raw: dict[str, Any]) -> dict[str, Any]:
    gstin = _pick_value(
        raw,
        [
            "gstin",
            "ctin",
            "supplier_gstin",
            "vendor_gstin",
            "counterparty_gstin",
        ],
    ).upper()
    invoice_ref = _pick_value(raw, ["invoice_no", "invoice_number", "inum", "reference", "inv_no"])
    invoice_date = _pick_value(raw, ["invoice_date", "idt", "date", "doc_date"])
    taxable = _pick_value(raw, ["taxable_value", "txval", "assessable_value"])
    tax_amt = _pick_value(raw, ["tax_amount", "iamt", "camt", "samt", "csamt", "gst_amount"])
    total_amt = _pick_value(raw, ["total_amount", "total", "amount", "invoice_value", "val"])
    vendor_name = _pick_value(raw, ["vendor", "supplier", "trade_name", "legal_name", "name"])
    phone = _pick_value(raw, ["phone", "phone_number", "mobile", "contact", "whatsapp"])

    return {
        "gstin": gstin,
        "invoice_reference": invoice_ref,
        "invoice_date": invoice_date,
        "taxable_value": money_str(parse_amount_from_text(taxable)),
        "tax_amount": money_str(parse_amount_from_text(tax_amt)),
        "total_amount": money_str(parse_amount_from_text(total_amt)),
        "vendor_name": vendor_name,
        "phone_number": phone,
    }


def _extract_2b_records(file_path: Path) -> list[dict[str, Any]]:
    ext = file_path.suffix.lower()
    raw_records: list[dict[str, Any]] = []

    if ext in {".xlsx", ".xls", ".csv"}:
        raw_records = _extract_excel_rows(file_path)
    elif ext == ".json":
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        raw_records = _flatten_json_records(payload)
    else:
        raise HTTPException(status_code=422, detail="Only JSON/Excel/CSV files are supported for 2B reconciliation")

    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in raw_records:
        if not isinstance(raw, dict):
            continue
        item = _normalize_2b_record(raw)
        if not item["gstin"]:
            continue
        if item["invoice_reference"]:
            key = f"{item['gstin']}|{item['invoice_reference']}"
        else:
            key = f"{item['gstin']}|{item['tax_amount']}|{item['invoice_date']}"
        if key in seen:
            continue
        seen.add(key)
        normalized.append(item)
    return normalized


async def _draft_vendor_nudge_message(
    *,
    vendor_name: str,
    gstin: str,
    invoice_reference: str,
    invoice_amount: str,
    mismatch_reason: str = "",
) -> dict[str, str]:
    prompt = (
        "Write a concise WhatsApp nudge in Indian business tone. "
        "Objective: ask vendor to file missing invoice in GSTR-1 because ITC is blocked. "
        "Return strict JSON with keys: message, urgency, subject. "
        f"Vendor: {vendor_name}; GSTIN: {gstin}; Invoice: {invoice_reference}; Amount: {invoice_amount}; "
        f"Mismatch context: {mismatch_reason or 'Invoice not reflected in GSTR-2B'}"
    )

    try:
        raw = await run_ollama_generate(model=FORENSIC_MODEL, prompt=prompt)
        parsed = parse_structured_json(raw)
        if parsed.get("message"):
            return {
                "message": str(parsed.get("message", "")).strip(),
                "urgency": str(parsed.get("urgency", "HIGH")).strip() or "HIGH",
                "subject": str(parsed.get("subject", "GST Filing Action Required")).strip() or "GST Filing Action Required",
            }
    except Exception:  # noqa: BLE001
        pass

    message = (
        f"Hi {vendor_name}, Accord AI flagged invoice {invoice_reference} (GSTIN {gstin}) as not reflected in GSTR-2B. "
        "This is blocking our ITC claim. Please file/update GSTR-1 immediately to avoid payment withholding."
    )
    return {
        "message": message,
        "urgency": "HIGH",
        "subject": "GST Filing Action Required",
    }


async def _refresh_playbook_nudge_async(
    *,
    alert_id: int,
    entry_id: int,
    gstin: str,
    vendor_name: str,
    invoice_reference: str,
    invoice_amount: str,
    mismatch_reason: str,
    actor_role: str,
    user_id: int,
) -> None:
    try:
        nudge = await _draft_vendor_nudge_message(
            vendor_name=vendor_name,
            gstin=gstin,
            invoice_reference=invoice_reference,
            invoice_amount=invoice_amount,
            mismatch_reason=mismatch_reason,
        )
        with closing(get_conn()) as conn:
            conn.execute("BEGIN")
            conn.execute(
                """
                UPDATE ca_payment_holds
                SET nudge_subject = ?,
                    nudge_message = ?
                WHERE alert_id = ?
                """,
                (str(nudge["subject"]), str(nudge["message"]), alert_id),
            )
            log_audit(
                conn,
                table_name="ca_payment_holds",
                record_id=alert_id,
                action="CA_PLAYBOOK_NUDGE_DRAFTED",
                old_value=None,
                new_value={
                    "alert_id": alert_id,
                    "entry_id": entry_id,
                    "gstin": gstin,
                    "vendor_name": vendor_name,
                    "invoice_reference": invoice_reference,
                    "urgency": str(nudge.get("urgency") or "HIGH"),
                    "actor_role": actor_role,
                },
                user_id=user_id,
                high_priority=False,
            )
            conn.commit()
    except Exception as exc:  # noqa: BLE001
        api_logger.warning("Playbook nudge background refresh failed alert_id=%s detail=%s", alert_id, exc)


def _resolve_account_name(label: str, default_name: str) -> str:
    normalized = (label or "").strip().lower()
    aliases = {
        "cash": "Cash",
        "bank": "Bank",
        "sales": "Sales Revenue",
        "sale": "Sales Revenue",
        "revenue": "Sales Revenue",
        "purchase": "Purchases",
        "purchases": "Purchases",
        "expense": "Operating Expenses",
        "expenses": "Operating Expenses",
        "payable": "Accounts Payable",
        "receivable": "Accounts Receivable",
    }
    return aliases.get(normalized, default_name)


def _send_ca_invite_email(*, to_email: str, invite_link: str, expires_at: str) -> dict[str, str]:
    if not SMTP_HOST:
        return {"status": "skipped", "detail": "SMTP host not configured"}

    message = EmailMessage()
    message["From"] = SMTP_FROM_EMAIL
    message["To"] = to_email
    message["Subject"] = "Accord CA Invite: Join Sovereign Compliance Workspace"
    message.set_content(
        "You have been invited to Accord as a Chartered Accountant.\n\n"
        f"Accept invite: {invite_link}\n"
        f"Invite valid until: {expires_at}\n\n"
        "If this was not expected, you can ignore this email."
    )

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as smtp:
            if SMTP_USE_TLS:
                smtp.starttls()
            if SMTP_USERNAME:
                smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
            smtp.send_message(message)
        return {"status": "sent", "detail": "SMTP delivery accepted"}
    except Exception as exc:  # noqa: BLE001
        return {"status": "failed", "detail": str(exc)}


def _load_whisper_model() -> Any:
    global WHISPER_MODEL_CACHE
    if WHISPER_MODEL_CACHE is not None:
        return WHISPER_MODEL_CACHE
    if whisper_lib is None:
        raise HTTPException(status_code=503, detail="openai-whisper package is not installed")
    WHISPER_MODEL_CACHE = whisper_lib.load_model("base")
    return WHISPER_MODEL_CACHE


def _transcribe_audio_to_text(audio_path: Path) -> str:
    model = _load_whisper_model()
    result = model.transcribe(str(audio_path), language="en", fp16=False)
    text = str(result.get("text", "")).strip()
    if not text:
        raise HTTPException(status_code=422, detail="Unable to transcribe audio into text")
    return text


async def _extract_voice_voucher(text: str) -> dict[str, Any]:
    prompt = (
        "Convert this speech transcript into a journal voucher. Return strict JSON with keys: "
        "date, description, vendor, gstin, debit_account, credit_account, amount. "
        "Use only these account names when possible: Cash, Bank, Sales Revenue, Purchases, Operating Expenses, Accounts Receivable, Accounts Payable. "
        f"Transcript: {text}"
    )
    try:
        raw = await run_ollama_generate(model=RECON_MODEL, prompt=prompt)
        parsed = parse_structured_json(raw)
        if parsed:
            return parsed
    except Exception:  # noqa: BLE001
        pass

    return {
        "date": date.today().isoformat(),
        "description": f"Voice command: {text[:140]}",
        "vendor": "",
        "gstin": "",
        "debit_account": "Cash",
        "credit_account": "Sales Revenue",
        "amount": "0",
    }


def _build_nexus_graph(records: list[dict[str, Any]]) -> dict[str, Any]:
    if nx is None:
        return {
            "status": "degraded",
            "reason": "networkx not installed",
            "risk_clusters": [],
            "graph_file": None,
        }

    graph = nx.Graph()
    graph.add_node("ACCORD_BUYER", kind="anchor")
    bucket_to_vendors: dict[str, set[str]] = {}

    for row in records:
        gstin = str(row.get("counterparty_gstin") or row.get("gstin") or "").strip().upper()
        if not gstin:
            continue
        amount = parse_amount_from_text(str(row.get("tax_amount", "0")))
        bucket = f"AMT_{amount.quantize(Decimal('1.00'))}"

        graph.add_node(gstin, kind="vendor")
        graph.add_edge("ACCORD_BUYER", gstin, relation="purchase")
        graph.add_node(bucket, kind="amount_bucket")
        graph.add_edge(gstin, bucket, relation="same_tax_amount")
        bucket_to_vendors.setdefault(bucket, set()).add(gstin)

    risk_clusters: list[dict[str, Any]] = []
    for bucket, vendors in bucket_to_vendors.items():
        if len(vendors) >= 3:
            risk_clusters.append(
                {
                    "severity": "CRITICAL_FRAUD_RISK",
                    "bucket": bucket,
                    "vendors": sorted(vendors),
                    "reason": "3+ vendors share repeated tax amount bucket indicating potential circular-trading ring",
                }
            )

    NEXUS_GRAPH_DIR.mkdir(parents=True, exist_ok=True)
    graph_path = NEXUS_GRAPH_DIR / f"nexus_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex}.json"
    serialized = {
        "nodes": [{"id": str(node), **attrs} for node, attrs in graph.nodes(data=True)],
        "edges": [
            {"source": str(source), "target": str(target), **attrs}
            for source, target, attrs in graph.edges(data=True)
        ],
        "risk_clusters": risk_clusters,
    }
    graph_path.write_text(json.dumps(serialized, indent=2), encoding="utf-8")

    return {
        "status": "ok",
        "nodes": graph.number_of_nodes(),
        "edges": graph.number_of_edges(),
        "risk_clusters": risk_clusters,
        "graph_file": str(graph_path),
    }


def _extract_batch_candidate_worker(file_path_str: str, file_name: str, content_type: str) -> dict[str, Any]:
    file_path = Path(file_path_str)
    lower_name = file_name.lower()
    lower_type = (content_type or "").lower()

    is_excel = lower_name.endswith((".xlsx", ".xls", ".csv")) or "sheet" in lower_type or "excel" in lower_type
    is_pdf = lower_name.endswith(".pdf") or lower_type == "application/pdf"
    is_image = lower_name.endswith((".png", ".jpg", ".jpeg", ".webp")) or lower_type.startswith("image/")
    is_video = lower_name.endswith((".mp4", ".mov", ".avi", ".mkv", ".m4v")) or lower_type.startswith("video/")

    if not (is_excel or is_pdf or is_image or is_video):
        return {
            "status": "failed",
            "file": file_name,
            "reason": "Unsupported format",
        }

    raw_text = ""
    extracted: dict[str, Any] = {}
    pipeline = ""

    try:
        if is_excel:
            pipeline = "OMNI_BATCH_EXCEL"
            rows = _extract_excel_rows(file_path)
            candidate = next(
                (
                    _row_to_ledger_fields(row)
                    for row in rows
                    if parse_amount_from_text(str(_row_to_ledger_fields(row).get("total_amount", "0"))) > 0
                ),
                None,
            )
            if candidate is None:
                return {
                    "status": "failed",
                    "file": file_name,
                    "reason": "No valid amount row found",
                }
            extracted = candidate

        elif is_pdf:
            pipeline = "OMNI_BATCH_PDF"
            if fitz is None:
                return {
                    "status": "failed",
                    "file": file_name,
                    "reason": "pymupdf missing",
                }

            doc = fitz.open(file_path)
            try:
                text_chunks: list[str] = []
                for page in doc:
                    text_chunks.append(page.get_text("text") or "")
                raw_text = "\n".join(text_chunks)
            finally:
                doc.close()

            extracted = {
                "date": "",
                "vendor": "PDF Vendor",
                "gstin": "",
                "hsn": "",
                "total_amount": money_str(parse_amount_from_text(raw_text)),
                "confidence": "batch-pdf-heuristic",
                "notes": raw_text[:700],
            }

        elif is_image:
            pipeline = "OMNI_BATCH_IMAGE"
            raw_text = extract_text_with_tesseract(file_path)
            extracted = {
                "date": "",
                "vendor": "Image Vendor",
                "gstin": "",
                "hsn": "",
                "total_amount": money_str(parse_amount_from_text(raw_text)),
                "confidence": "batch-image-ocr",
                "notes": raw_text[:700],
            }

        elif is_video:
            pipeline = "OMNI_BATCH_VIDEO"
            if cv2 is None:
                return {
                    "status": "failed",
                    "file": file_name,
                    "reason": "opencv missing",
                }
            capture = cv2.VideoCapture(str(file_path))
            if not capture.isOpened():
                return {
                    "status": "failed",
                    "file": file_name,
                    "reason": "Cannot open video",
                }

            try:
                frame_count = int(capture.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
                frame_no = max(0, frame_count // 3)
                capture.set(cv2.CAP_PROP_POS_FRAMES, frame_no)
                ok, frame = capture.read()
                if not ok or frame is None:
                    return {
                        "status": "failed",
                        "file": file_name,
                        "reason": "No readable frame in video",
                    }

                temp_frame = file_path.with_suffix(".batch_frame.jpg")
                cv2.imwrite(str(temp_frame), frame)
                try:
                    raw_text = extract_text_with_tesseract(temp_frame)
                finally:
                    temp_frame.unlink(missing_ok=True)
            finally:
                capture.release()

            extracted = {
                "date": "",
                "vendor": "Video Vendor",
                "gstin": "",
                "hsn": "",
                "total_amount": money_str(parse_amount_from_text(raw_text)),
                "confidence": "batch-video-ocr",
                "notes": raw_text[:700],
            }

        amount = parse_amount_from_text(str(extracted.get("total_amount", "0")))
        if amount <= 0:
            return {
                "status": "failed",
                "file": file_name,
                "reason": "No valid amount detected",
            }

        return {
            "status": "ok",
            "file": file_name,
            "pipeline": pipeline,
            "raw_text": raw_text,
            "extracted": extracted,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "failed",
            "file": file_name,
            "reason": str(exc),
            "audit_id": uuid.uuid4().hex[:12],
        }


class AdaptiveIngest:
    """Adaptive ingest orchestrator tuned for M3 thermal and memory headroom.

    Worker count scales with CPU and available unified memory, and a circuit
    breaker pauses the queue if sustained saturation is detected.
    """

    def __init__(self, max_workers: int = 16, min_workers: int = 4):
        self.max_workers = max_workers
        self.min_workers = min_workers
        self.retry_bucket: list[dict[str, str]] = []
        self.last_cpu_load = 0.0
        self.last_memory_available_gb = 0.0
        self.circuit_breaker_open = False
        self.saturation_alert = ""

    def _sample_cpu_window(self) -> list[float]:
        if psutil is None:
            return [0.0]
        samples: list[float] = []
        for _ in range(5):
            try:
                samples.append(float(psutil.cpu_percent(interval=1.0)))
            except Exception:  # noqa: BLE001
                samples.append(50.0)
        return samples

    def get_optimal_workers(self) -> int:
        # Keep the host responsive under pressure while still saturating throughput when cool.
        if psutil is None:
            self.last_cpu_load = 0.0
            self.last_memory_available_gb = 0.0
            self.circuit_breaker_open = False
            return min(8, self.max_workers)

        load_samples = self._sample_cpu_window()
        avg_load = sum(load_samples) / max(len(load_samples), 1)
        self.last_cpu_load = avg_load

        try:
            vm = psutil.virtual_memory()
            available_gb = float(vm.available) / float(1024**3)
        except Exception:  # noqa: BLE001
            available_gb = 4.0
        self.last_memory_available_gb = available_gb

        sustained_hot = all(sample > 80 for sample in load_samples)
        self.circuit_breaker_open = sustained_hot
        if sustained_hot:
            self.saturation_alert = "Thermal pressure high (>80%), throttling worker pool"
            return self.min_workers

        if avg_load > 80 or available_gb < 2.5:
            return self.min_workers
        if avg_load < 30 and available_gb > 6.0:
            return self.max_workers
        return min(max(8, self.min_workers), self.max_workers)

    def _run_extraction(self, files: list[dict[str, str]], workers: int) -> list[dict[str, Any]]:
        extracted_results: list[dict[str, Any]] = []
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures: list[tuple[dict[str, str], Any]] = []
            for item in files:
                future = executor.submit(
                    _extract_batch_candidate_worker,
                    item["path"],
                    item["file_name"],
                    item["content_type"],
                )
                futures.append((item, future))

            for item, future in futures:
                result = future.result()
                if result.get("status") != "ok":
                    result.setdefault("audit_id", uuid.uuid4().hex[:12])
                extracted_results.append({"meta": item, "result": result})
        return extracted_results

    async def process_batch(self, files: list[dict[str, str]]) -> tuple[list[dict[str, Any]], int, int]:
        initial_workers = min(self.get_optimal_workers(), len(files))
        if self.circuit_breaker_open:
            # Circuit breaker backoff to avoid persistent thermal throttling loops.
            await asyncio.sleep(5)

        first_pass = self._run_extraction(files, workers=max(initial_workers, self.min_workers))

        self.retry_bucket = [
            bundle["meta"]
            for bundle in first_pass
            if bundle.get("result", {}).get("status") != "ok"
        ]

        if not self.retry_bucket:
            return first_pass, initial_workers, 0

        # A short backoff helps avoid cascading retries while CPU/memory pressure settles.
        await asyncio.sleep(5)
        retry_workers = min(max(self.min_workers, initial_workers // 2), len(self.retry_bucket))
        second_pass = self._run_extraction(self.retry_bucket, workers=max(retry_workers, 1))
        merged: dict[str, dict[str, Any]] = {bundle["meta"]["path"]: bundle for bundle in first_pass}
        for bundle in second_pass:
            merged[bundle["meta"]["path"]] = bundle

        return list(merged.values()), initial_workers, len(second_pass)

class NeuralInk:
    @staticmethod
    async def reconstruct(raw_ocr_text: str) -> tuple[dict[str, Any], str]:
        prompt = (
            "[INDIAN_ACCOUNTANT_MODE] Reconstruct messy OCR into strict JSON with keys: "
            "date, vendor, gstin, total_amount, hsn, cgst, sgst, igst, confidence, notes. "
            "Rules: gstin must be 15-char format if present, tax values numeric strings, "
            "hsn should be 6+ digits if present, output JSON only. "
            f"Raw OCR:\n{raw_ocr_text}"
        )
        raw = await run_ollama_generate(model=RECON_MODEL, prompt=prompt)
        return parse_structured_json(raw), raw


class JournalLineIn(BaseModel):
    account_id: int
    debit: Decimal = Field(default=Decimal("0"), ge=0)
    credit: Decimal = Field(default=Decimal("0"), ge=0)


class JournalEntryIn(BaseModel):
    date: date
    description: str = Field(default="", max_length=500)
    is_b2b: bool = False
    hsn_code: str | None = Field(default=None, min_length=6, max_length=8)
    company_state_code: str | None = Field(default=None, min_length=2, max_length=2)
    counterparty_state_code: str | None = Field(default=None, min_length=2, max_length=2)
    counterparty_gstin: str | None = Field(default=None, min_length=15, max_length=15)
    eco_gstin: str | None = Field(default=None, min_length=15, max_length=15)
    supply_source: str = Field(default="DIRECT", max_length=10)
    ims_status: str = Field(default="PENDING", max_length=20)
    vendor_legal_name: str | None = Field(default=None, max_length=200)
    vendor_gstr1_filed_at: date | None = None
    lines: list[JournalLineIn] = Field(min_length=2)


class ReversalIn(BaseModel):
    reason: str = Field(min_length=3, max_length=500)


class PeriodUnlockIn(BaseModel):
    admin_reason: str = Field(min_length=3, max_length=500)
    extension_hours: int = Field(default=24, ge=1, le=168)


class PeriodLockIn(BaseModel):
    reason: str = Field(default="", max_length=500)


class GstrExportIn(BaseModel):
    from_date: date | None = None
    to_date: date | None = None


class Reversal37AIn(BaseModel):
    as_of_date: date | None = None
    posting_date: date | None = None
    min_credit_balance: Decimal | None = Field(default=None, ge=0)


class ReversalArchiveIn(BaseModel):
    entry_ids: list[int] = Field(min_length=1)
    export_hash: str | None = Field(default=None, max_length=128)
    note: str | None = Field(default=None, max_length=500)


class ReversalApproveIn(BaseModel):
    entry_ids: list[int] = Field(min_length=1)
    export_hash: str | None = Field(default=None, max_length=128)
    note: str | None = Field(default=None, max_length=500)


class SafeHarborCertifyIn(BaseModel):
    as_of_date: date | None = None
    min_credit_balance: Decimal = Field(ge=0)
    note: str | None = Field(default=None, max_length=500)


class CAInviteAcceptIn(BaseModel):
    admin_id: int = Field(gt=0)


class AskFridayIn(BaseModel):
    question: str = Field(min_length=5, max_length=800)
    model: str = Field(default="llama3.2", min_length=2, max_length=60)
    as_of_date: date | None = None
    min_credit_balance: Decimal | None = Field(default=None, ge=0)


class GstnEclBridgeIn(BaseModel):
    gstin: str = Field(min_length=15, max_length=15)
    as_of_date: date | None = None
    period: str | None = Field(default=None, max_length=20)


class Gstr1GenerateIn(BaseModel):
    from_date: date | None = None
    to_date: date | None = None
    gstin: str | None = Field(default=None, min_length=15, max_length=15)
    period: str | None = Field(default=None, max_length=10)


class Gstr1PrepareIn(BaseModel):
    period: str = Field(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")
    filing_type: str = Field(default="GSTR-1", min_length=4, max_length=20)


class Gstr1ApproveIn(BaseModel):
    filing_id: int = Field(gt=0)


class InvestorModeIn(BaseModel):
    period: str = Field(default="2026-03", pattern=r"^\d{4}-(0[1-9]|1[0-2])$")
    run_concurrency: int = Field(default=50, ge=1, le=200)


class Gstr1FiledIn(BaseModel):
    period: str = Field(min_length=4, max_length=10)
    fingerprint: str = Field(min_length=32, max_length=128)
    filing_reference: str | None = Field(default=None, max_length=120)


class VoiceSyncIn(BaseModel):
    transcript: str = Field(min_length=5, max_length=1200)
    posting_date: date | None = None
    currency_code: str | None = Field(default=None, min_length=3, max_length=3)
    exchange_rate: Decimal | None = Field(default=None, gt=0)


class VendorNudgeIn(BaseModel):
    gstin: str = Field(min_length=15, max_length=15)
    vendor_name: str | None = Field(default=None, max_length=200)
    invoice_reference: str | None = Field(default=None, max_length=100)
    invoice_amount: Decimal | None = Field(default=None, ge=0)
    phone_number: str | None = Field(default=None, max_length=20)
    mismatch_reason: str | None = Field(default=None, max_length=500)


class CAAlertRuleUpsertIn(BaseModel):
    rule_key: str = Field(min_length=3, max_length=80)
    display_name: str = Field(min_length=3, max_length=160)
    enabled: bool = True
    min_trust_score: float = Field(default=60.0, ge=0, le=100)
    min_itc_risk: Decimal = Field(default=Decimal("0"), ge=0)
    target_risk_levels: list[str] = Field(default_factory=lambda: ["HIGH", "CRITICAL"])
    channels: list[str] = Field(default_factory=lambda: ["IN_APP"])


class CAManualAlertIn(BaseModel):
    gstin: str = Field(min_length=15, max_length=15)
    vendor_name: str = Field(min_length=2, max_length=200)
    risk_level: str = Field(default="HIGH", min_length=3, max_length=20)
    title: str = Field(min_length=4, max_length=180)
    message: str = Field(min_length=6, max_length=1200)


class CAPlaybookExecuteIn(BaseModel):
    alert_id: int = Field(gt=0)
    hold_hours: int = Field(default=72, ge=1, le=720)
    playbook_key: str = Field(default="ALERT_REMEDIATION_V1", min_length=4, max_length=80)


class MarketingSignupIn(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    email: str = Field(min_length=5, max_length=320)
    provider: str = Field(default="EMAIL", min_length=3, max_length=20)
    source: str = Field(default="public-signup", min_length=2, max_length=80)


class IngestBatchRetryIn(BaseModel):
    batch_id: str = Field(min_length=8, max_length=64)


class TallySyncFinalIn(BaseModel):
    entry_ids: list[int] = Field(min_length=1, max_length=2000)


class InventoryBatchUpsertIn(BaseModel):
    sku_code: str = Field(min_length=1, max_length=80)
    sku_name: str = Field(min_length=1, max_length=200)
    batch_code: str = Field(min_length=1, max_length=80)
    hsn_code: str = Field(min_length=6, max_length=12)
    gst_rate: Decimal = Field(ge=0)
    quantity: Decimal = Field(gt=0)
    unit_cost: Decimal = Field(ge=0)
    expiry_date: date | None = None


class StudioTemplateSaveIn(BaseModel):
    name: str = Field(min_length=2, max_length=120)
    template_type: str = Field(default="dashboard", min_length=3, max_length=40)
    layout: list[dict[str, Any]] = Field(min_length=1)
    blocks: list[dict[str, Any]] = Field(min_length=1)


class BankStatementRowIn(BaseModel):
    date: str = Field(min_length=4, max_length=40)
    amount: Decimal
    reference: str = Field(default="", max_length=300)
    narration: str = Field(default="", max_length=500)


class BankReconciliationIn(BaseModel):
    bank_rows: list[BankStatementRowIn] = Field(min_length=1, max_length=5000)
    from_date: date | None = None
    to_date: date | None = None
    fuzzy_threshold: float = Field(default=0.84, ge=0.5, le=0.995)
    amount_tolerance: Decimal = Field(default=Decimal("1.0000"), ge=Decimal("0"))
    enable_ai: bool = True


class ReraAllocationRequestIn(BaseModel):
    booking_id: str = Field(min_length=1, max_length=64)
    payment_reference: str = Field(min_length=1, max_length=80)
    event_type: str = Field(default="PAYMENT", min_length=7, max_length=7)
    receipt_amount: Decimal = Field(gt=0)
    override_rera_ratio: Decimal | None = Field(default=None, gt=0, lt=1)
    override_reason: str | None = Field(default=None, max_length=256)


class ReraBookingCreateIn(BaseModel):
    booking_id: str = Field(min_length=1, max_length=64)
    project_id: str = Field(min_length=1, max_length=64)
    customer_name: str | None = Field(default=None, max_length=160)
    unit_code: str | None = Field(default=None, max_length=64)
    status: str = Field(default="ACTIVE", min_length=3, max_length=16)


class ReraBookingUpdateIn(BaseModel):
    project_id: str | None = Field(default=None, min_length=1, max_length=64)
    customer_name: str | None = Field(default=None, max_length=160)
    unit_code: str | None = Field(default=None, max_length=64)
    status: str | None = Field(default=None, min_length=3, max_length=16)


class UserDeviceTokenUpsertIn(BaseModel):
    device_token: str = Field(min_length=20, max_length=4096)
    platform: str = Field(default="unknown", min_length=2, max_length=24)
    app_version: str | None = Field(default=None, max_length=80)
    user_id: int | None = Field(default=None, gt=0)


app = FastAPI(title="Friday Insights Ledger API", version="1.0.0")

telemetry_service = TelemetryService(api_logger=api_logger, latency_warn_threshold_ms=LATENCY_WARN_THRESHOLD_MS)
performance_monitor = telemetry_service.performance_monitor


class PerformanceRoute(APIRoute):
    """Applies the performance monitor decorator to every API route handler."""

    def get_route_handler(self):
        route_handler = super().get_route_handler()
        monitored_handler = performance_monitor(endpoint_name=self.path)(route_handler)

        async def custom_route_handler(request: Request):
            return await monitored_handler(request)

        return custom_route_handler


app.router.route_class = PerformanceRoute

accounting_service: AccountingService | None = None
ingest_service: IngestService | None = None
compliance_service: ComplianceService | None = None
inventory_service: InventoryService | None = None
banking_service: BankingService | None = None
report_service: ReportService | None = None
variance_analyzer_service: AiVarianceAnalyzer | None = None
statutory_service: StatutoryService | None = None
gst_service: GstService | None = None
currency_service: CurrencyService | None = None
voice_service: VoiceService | None = None
govt_bridge_service: GovtBridgeService | None = None
rera_allocation_service: ReraAllocationService | None = None
commission_service: CommissionService | None = None
voucher_service = VoucherService()

cors_allow_origins = resolve_cors_allow_origins()
cors_allow_origin_regex = os.getenv("CORS_ALLOW_ORIGIN_REGEX", "").strip() or None
cors_allow_credentials = "*" not in cors_allow_origins


def ensure_service_layer() -> tuple[AccountingService, IngestService, ComplianceService, InventoryService]:
    """Builds the service layer on first use after helper functions are available.

    Hardware Impact:
        One-time object construction with no recurring runtime overhead.
    Logic Invariants:
        Returns initialized service singletons for the current process.
    Legal Context:
        Centralized service wiring keeps audit-critical logic paths consistent.
    """
    global accounting_service, ingest_service, compliance_service, inventory_service

    if accounting_service is None:
        accounting_service = AccountingService(
            get_conn=get_conn,
            post_ledger_entry_from_extract=_post_ledger_entry_from_extract,
            generate_tally_export=generate_tally_export,
        )

    if ingest_service is None:
        ingest_service = IngestService(
            ram_disk_buffer=RAM_DISK_BUFFER,
            extract_text_with_tesseract=extract_text_with_tesseract,
            extract_receipt_fields=extract_receipt_fields,
            parse_amount_from_text=parse_amount_from_text,
            money_str=money_str,
            accounting_service=accounting_service,
        )

    if compliance_service is None:
        compliance_service = ComplianceService(
            extract_2b_records=_extract_2b_records,
            build_nexus_graph=_build_nexus_graph,
            draft_vendor_nudge_message=_draft_vendor_nudge_message,
            run_ollama_generate=run_ollama_generate,
            forensic_model=FORENSIC_MODEL,
            parse_amount_from_text=parse_amount_from_text,
            money_str=money_str,
        )

    if inventory_service is None:
        inventory_service = InventoryService(
            get_conn=get_conn,
            allowed_hsn_slabs=GST_2026_ALLOWED_SLABS,
        )

    return accounting_service, ingest_service, compliance_service, inventory_service


def ensure_banking_service() -> BankingService:
    global banking_service
    if banking_service is None:
        banking_service = BankingService(
            parse_amount_from_text=parse_amount_from_text,
            money_str=money_str,
            run_ollama_generate=run_ollama_generate,
            recon_model=RECON_MODEL,
        )
    return banking_service


def ensure_report_service() -> ReportService:
    global report_service
    if report_service is None:
        report_service = ReportService(
            money=money,
            money_str=money_str,
        )
    return report_service


def ensure_variance_analyzer_service() -> AiVarianceAnalyzer:
    global variance_analyzer_service
    if variance_analyzer_service is None:
        variance_analyzer_service = AiVarianceAnalyzer(
            get_conn=get_conn,
            run_ollama_generate=run_ollama_generate,
            model=VARIANCE_MODEL,
        )
    return variance_analyzer_service


def ensure_statutory_service() -> StatutoryService:
    global statutory_service
    if statutory_service is None:
        statutory_service = StatutoryService(
            parse_amount_from_text=parse_amount_from_text,
            money_str=money_str,
        )
    return statutory_service


def ensure_gst_service() -> GstService:
    global gst_service
    if gst_service is None:
        gst_service = GstService(
            parse_amount_from_text=parse_amount_from_text,
            money_str=money_str,
        )
    return gst_service


def ensure_currency_service() -> CurrencyService:
    global currency_service
    if currency_service is None:
        currency_service = CurrencyService()
    return currency_service


def ensure_voice_service() -> VoiceService:
    global voice_service
    if voice_service is None:
        voice_service = VoiceService(
            mistral_generate=run_ollama_generate,
            model=FORENSIC_MODEL,
        )
    return voice_service


def ensure_govt_bridge_service() -> GovtBridgeService:
    global govt_bridge_service
    if govt_bridge_service is None:
        govt_bridge_service = GovtBridgeService()
    return govt_bridge_service


def ensure_rera_allocation_service() -> ReraAllocationService:
    global rera_allocation_service
    if rera_allocation_service is None:
        rera_allocation_service = ReraAllocationService(
            get_conn=get_conn,
            high_value_alert_hook=dispatch_high_value_allocation_alert,
        )
    return rera_allocation_service


def ensure_user_device_token_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_device_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            device_token TEXT NOT NULL UNIQUE,
            platform TEXT NOT NULL,
            push_provider TEXT NOT NULL DEFAULT 'FCM',
            app_version TEXT,
            is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0, 1)),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS push_notification_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            device_token TEXT NOT NULL,
            platform TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING' CHECK(status IN ('PENDING', 'SENT', 'FAILED')),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_user_device_tokens_user ON user_device_tokens(user_id, is_active, updated_at DESC)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_push_outbox_status ON push_notification_outbox(status, created_at)"
    )
    conn.commit()


def dispatch_high_value_allocation_alert(payload: dict[str, Any]) -> None:
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    title = "High-Value Allocation Alert"
    body = (
        f"Booking {payload.get('booking_id')} posted allocation of INR {payload.get('receipt_amount')} "
        f"(ref {payload.get('payment_reference')})."
    )

    with closing(get_conn()) as conn:
        conn.row_factory = sqlite3.Row
        ensure_user_device_token_schema(conn)

        recipients = conn.execute(
            """
            SELECT user_id, device_token, platform
            FROM user_device_tokens
            WHERE is_active = 1
            ORDER BY updated_at DESC
            LIMIT 500
            """
        ).fetchall()

        if not recipients:
            return

        for row in recipients:
            outbox_payload = {
                "title": title,
                "body": body,
                "channel": "HIGH_VALUE_ALLOCATION",
                "allocation": payload,
            }
            conn.execute(
                """
                INSERT INTO push_notification_outbox(
                    event_type, user_id, device_token, platform, payload_json, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?)
                """,
                (
                    "HIGH_VALUE_ALLOCATION",
                    int(row["user_id"]),
                    str(row["device_token"]),
                    str(row["platform"]),
                    json.dumps(outbox_payload),
                    now_iso,
                    now_iso,
                ),
            )
        conn.commit()

    api_logger.info(
        "queued high-value allocation push alerts",
        extra={
            "booking_id": str(payload.get("booking_id") or ""),
            "payment_reference": str(payload.get("payment_reference") or ""),
            "receipt_amount": str(payload.get("receipt_amount") or "0"),
            "queued_recipients": len(recipients),
        },
    )


def ensure_commission_service() -> CommissionService:
    global commission_service
    if commission_service is None:
        commission_service = CommissionService(get_conn=get_conn)
    return commission_service

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_allow_origins,
    allow_origin_regex=None,
    allow_credentials=cors_allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(SecurityHeadersMiddleware)

app.include_router(mobile_gateway_router)


@app.middleware("http")
async def request_telemetry_middleware(request: Request, call_next):
    request_id = uuid.uuid4().hex[:12]
    started_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    start_ts = datetime.utcnow().timestamp()
    request.state.request_id = request_id

    response = await call_next(request)
    elapsed_ms = (datetime.utcnow().timestamp() - start_ts) * 1000
    response.headers["X-Request-Id"] = request_id
    telemetry_service.log_latency(
        method=request.method,
        path=request.url.path,
        elapsed_ms=elapsed_ms,
        started_at=started_at,
        request_id=request_id,
    )
    return response


@app.middleware("http")
async def rbac_middleware(request: Request, call_next):
    enforce_rbac_policy(request)
    return await call_next(request)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    envelope = telemetry_service.build_error_envelope(
        detail=str(exc.detail),
        status_code=exc.status_code,
        path=request.url.path,
        error_type="HTTP_EXCEPTION",
        request_id=getattr(request.state, "request_id", None),
    )
    return JSONResponse(
        status_code=exc.status_code,
        content=envelope,
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    envelope = telemetry_service.build_error_envelope(
        detail="Internal server error",
        status_code=500,
        path=request.url.path,
        error_type=type(exc).__name__,
        request_id=getattr(request.state, "request_id", None),
    )
    api_logger.exception(
        "Unhandled exception audit_id=%s method=%s path=%s",
        envelope["error"]["audit_id"],
        request.method,
        request.url.path,
    )
    return JSONResponse(
        status_code=500,
        content=envelope,
    )


def get_conn() -> sqlite3.Connection:
    # Cloud-ready bridge note:
    # Core ledger uses sqlite-compatible SQL currently. PostgreSQL is used through SQLAlchemy
    # for Stark Studio template storage until full ledger SQL dialect migration is completed.
    conn = sqlite3.connect(SQLITE_DB_PATH, timeout=15.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn


register_audit_logger_middleware(app, get_conn)


SQLA_ENGINE = None
if DB_BACKEND == "postgresql" and create_engine is not None:
    try:
        SQLA_ENGINE = create_engine(sqlalchemy_database_url(DATABASE_URL), pool_pre_ping=True, future=True)
    except Exception:  # noqa: BLE001
        SQLA_ENGINE = None


def ensure_studio_schema_sqlite(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS studio_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            template_type TEXT NOT NULL,
            layout_json TEXT NOT NULL,
            blocks_json TEXT NOT NULL,
            created_by INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS studio_template_exports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            template_id INTEGER NOT NULL,
            pdf_path TEXT NOT NULL,
            pdf_fingerprint TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(template_id) REFERENCES studio_templates(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_studio_templates_updated_at ON studio_templates(updated_at);
        CREATE INDEX IF NOT EXISTS idx_studio_template_exports_template_id ON studio_template_exports(template_id);
        """
    )


def ensure_studio_schema_postgres() -> None:
    if SQLA_ENGINE is None or text is None:
        return
    with SQLA_ENGINE.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS studio_templates (
                    id BIGSERIAL PRIMARY KEY,
                    name TEXT NOT NULL,
                    template_type TEXT NOT NULL,
                    layout_json TEXT NOT NULL,
                    blocks_json TEXT NOT NULL,
                    created_by INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS studio_template_exports (
                    id BIGSERIAL PRIMARY KEY,
                    template_id BIGINT NOT NULL REFERENCES studio_templates(id) ON DELETE CASCADE,
                    pdf_path TEXT NOT NULL,
                    pdf_fingerprint TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
        )


def build_studio_template_pdf(
    *,
    template_name: str,
    template_type: str,
    layout_json: str,
    blocks_json: str,
    created_by: int,
) -> bytes:
    buffer = io.BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    y = height - 56

    def line(text_value: str, gap: int = 16, font: str = "Helvetica", size: int = 10) -> None:
        nonlocal y
        if y < 72:
            pdf.showPage()
            y = height - 56
        pdf.setFont(font, size)
        pdf.drawString(36, y, text_value)
        y -= gap

    generated_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    line("ACCORD STARK-STUDIO TEMPLATE EXPORT", gap=20, font="Helvetica-Bold", size=13)
    line(f"Template: {template_name}", font="Helvetica-Bold")
    line(f"Type: {template_type}")
    line(f"Generated At (UTC): {generated_at}")
    line(f"Generated By: {created_by}")
    line("-" * 110, gap=14)
    line("Layout JSON", font="Helvetica-Bold")
    for chunk_start in range(0, len(layout_json), 110):
        line(layout_json[chunk_start : chunk_start + 110], gap=12, font="Courier", size=8)
    line("-" * 110, gap=14)
    line("Blocks JSON", font="Helvetica-Bold")
    for chunk_start in range(0, len(blocks_json), 110):
        line(blocks_json[chunk_start : chunk_start + 110], gap=12, font="Courier", size=8)

    pdf.showPage()
    pdf.save()
    payload = buffer.getvalue()
    buffer.close()
    return payload


def init_db() -> None:
    with closing(get_conn()) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                type TEXT NOT NULL CHECK(type IN ('Asset', 'Liability', 'Equity', 'Revenue', 'Expense')),
                balance TEXT NOT NULL DEFAULT '0.0000'
            );

            CREATE TABLE IF NOT EXISTS journal_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                reference TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL DEFAULT '',
                company_state_code TEXT NULL,
                counterparty_state_code TEXT NULL,
                counterparty_gstin TEXT NULL,
                eco_gstin TEXT NULL,
                supply_source TEXT NOT NULL DEFAULT 'DIRECT' CHECK(supply_source IN ('DIRECT', 'ECO')),
                ims_status TEXT NOT NULL DEFAULT 'PENDING' CHECK(ims_status IN ('ACCEPTED', 'REJECTED', 'PENDING')),
                vendor_legal_name TEXT NULL,
                vendor_gstr1_filed_at TEXT NULL,
                status TEXT NOT NULL DEFAULT 'POSTED' CHECK(status IN ('POSTED', 'REVERSED')),
                reversal_of_id INTEGER NULL,
                is_filed INTEGER NOT NULL DEFAULT 0 CHECK(is_filed IN (0, 1)),
                filed_at TEXT NULL,
                filed_export_hash TEXT NULL,
                approved_by_1 INTEGER NULL,
                approved_by_2 INTEGER NULL,
                voucher_type TEXT NOT NULL DEFAULT 'JOURNAL',
                currency_code TEXT NOT NULL DEFAULT 'INR',
                exchange_rate TEXT NOT NULL DEFAULT '1.0000',
                entry_fingerprint TEXT NULL,
                previous_entry_fingerprint TEXT NULL,
                cumulative_block_hash TEXT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS financial_periods (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period_name TEXT NOT NULL UNIQUE,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                is_locked INTEGER NOT NULL DEFAULT 0 CHECK(is_locked IN (0, 1)),
                unlocked_until TEXT NULL
            );

            CREATE TABLE IF NOT EXISTS document_sequences (
                sequence_key TEXT PRIMARY KEY,
                prefix TEXT NOT NULL,
                current_value INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS hsn_master (
                code TEXT PRIMARY KEY,
                description TEXT NOT NULL,
                gst_rate TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0, 1)),
                is_verified INTEGER NOT NULL DEFAULT 0 CHECK(is_verified IN (0, 1)),
                last_checked TEXT NULL,
                uqc TEXT NOT NULL DEFAULT 'NOS'
            );

            CREATE TABLE IF NOT EXISTS tax_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_id INTEGER NOT NULL,
                hsn_code TEXT,
                gst_rate_snapshot TEXT NOT NULL,
                taxable_value TEXT NOT NULL,
                tax_amount TEXT NOT NULL,
                supply_type TEXT NOT NULL CHECK(supply_type IN ('B2B', 'B2CS')),
                is_inter_state INTEGER NOT NULL DEFAULT 0 CHECK(is_inter_state IN (0, 1)),
                supply_source TEXT NOT NULL DEFAULT 'DIRECT' CHECK(supply_source IN ('DIRECT', 'ECO')),
                created_at TEXT NOT NULL,
                FOREIGN KEY(entry_id) REFERENCES journal_entries(id) ON DELETE RESTRICT,
                FOREIGN KEY(hsn_code) REFERENCES hsn_master(code) ON DELETE RESTRICT
            );

            CREATE TABLE IF NOT EXISTS export_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_type TEXT NOT NULL,
                period_id INTEGER NULL,
                period_from TEXT NOT NULL,
                period_to TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                payload_hash TEXT NOT NULL UNIQUE,
                payload_fingerprint TEXT NULL,
                approved_by_1 INTEGER NULL,
                approved_by_2 INTEGER NULL,
                last_verification_status TEXT NULL,
                last_verified_by INTEGER NULL,
                last_verified_at TEXT NULL,
                security_hold_until TEXT NULL,
                status TEXT NOT NULL CHECK(status IN ('GENERATED', 'FILED', 'FAILED')),
                arn_number TEXT NULL,
                FOREIGN KEY(period_id) REFERENCES financial_periods(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS vendor_trust_scores (
                gstin TEXT PRIMARY KEY,
                legal_name TEXT,
                filing_consistency_score REAL NOT NULL DEFAULT 100.0,
                avg_filing_delay_days INTEGER NOT NULL DEFAULT 0,
                last_gstr1_filed_at TEXT NULL,
                total_itc_at_risk TEXT NOT NULL DEFAULT '0.0000',
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS safe_harbor_attestations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                as_of_date TEXT NOT NULL,
                min_credit_balance TEXT NOT NULL,
                reversal_amount TEXT NOT NULL,
                liability_offset TEXT NOT NULL,
                status TEXT NOT NULL,
                legal_basis TEXT NOT NULL,
                certified_by INTEGER NOT NULL,
                certified_role TEXT NOT NULL,
                note TEXT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ca_invites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                token TEXT NOT NULL UNIQUE,
                expires_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING' CHECK(status IN ('PENDING', 'ACCEPTED', 'REVOKED', 'EXPIRED')),
                accepted_by INTEGER NULL,
                accepted_at TEXT NULL,
                created_by INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ca_alert_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_key TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0, 1)),
                min_trust_score REAL NOT NULL DEFAULT 60.0,
                min_itc_risk TEXT NOT NULL DEFAULT '0.0000',
                target_risk_levels TEXT NOT NULL,
                channels_json TEXT NOT NULL,
                created_by INTEGER NOT NULL,
                updated_by INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ca_alert_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_hash TEXT NOT NULL UNIQUE,
                rule_key TEXT NOT NULL,
                gstin TEXT NOT NULL,
                vendor_name TEXT NOT NULL,
                risk_level TEXT NOT NULL,
                severity TEXT NOT NULL,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                channels_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN' CHECK(status IN ('OPEN', 'ACKNOWLEDGED', 'CLOSED')),
                event_source TEXT NOT NULL,
                metadata_json TEXT NULL,
                acknowledged_by INTEGER NULL,
                acknowledged_at TEXT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS ca_payment_holds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                alert_id INTEGER NOT NULL UNIQUE,
                entry_id INTEGER NOT NULL,
                gstin TEXT NOT NULL,
                vendor_name TEXT NOT NULL,
                hold_reason TEXT NOT NULL,
                hold_status TEXT NOT NULL DEFAULT 'OPEN' CHECK(hold_status IN ('OPEN', 'RELEASED', 'EXPIRED')),
                hold_until TEXT NOT NULL,
                nudge_subject TEXT NOT NULL,
                nudge_message TEXT NOT NULL,
                created_by INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                released_by INTEGER NULL,
                released_at TEXT NULL,
                FOREIGN KEY(alert_id) REFERENCES ca_alert_events(id) ON DELETE CASCADE,
                FOREIGN KEY(entry_id) REFERENCES journal_entries(id) ON DELETE RESTRICT
            );

            CREATE TABLE IF NOT EXISTS receipt_imports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_id INTEGER NULL,
                file_path TEXT NOT NULL,
                ocr_text TEXT NULL,
                extracted_json TEXT NULL,
                model_response TEXT NULL,
                status TEXT NOT NULL DEFAULT 'PROCESSED',
                created_by INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(entry_id) REFERENCES journal_entries(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS marketing_signups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                provider TEXT NOT NULL,
                source TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS market_trend_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_file_path TEXT NOT NULL,
                source_kind TEXT NOT NULL,
                period_hint TEXT NULL,
                model_used TEXT NOT NULL,
                analysis_json TEXT NOT NULL,
                report_file_path TEXT NULL,
                created_by INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS inventory_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku_code TEXT NOT NULL,
                sku_name TEXT NOT NULL,
                batch_code TEXT NOT NULL,
                hsn_code TEXT NOT NULL,
                gst_rate TEXT NOT NULL,
                quantity TEXT NOT NULL,
                unit_cost TEXT NOT NULL,
                total_value TEXT NOT NULL,
                expiry_date TEXT NULL,
                status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE', 'EXPIRED', 'DEPLETED')),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                created_by INTEGER NOT NULL,
                UNIQUE(sku_code, batch_code)
            );

            CREATE TABLE IF NOT EXISTS journal_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_id INTEGER NOT NULL,
                account_id INTEGER NOT NULL,
                debit TEXT NOT NULL DEFAULT '0.0000',
                credit TEXT NOT NULL DEFAULT '0.0000',
                FOREIGN KEY(entry_id) REFERENCES journal_entries(id) ON DELETE RESTRICT,
                FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE RESTRICT
            );

            CREATE TABLE IF NOT EXISTS sme_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_id TEXT NOT NULL,
                razorpay_subscription_id TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'PENDING',
                current_period_end TEXT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS audit_edit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                table_name TEXT NOT NULL,
                record_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL DEFAULT 0,
                action TEXT NOT NULL,
                high_priority INTEGER NOT NULL DEFAULT 0 CHECK(high_priority IN (0, 1)),
                old_value TEXT,
                new_value TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL DEFAULT 0,
                action TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                payload_snapshot TEXT NOT NULL,
                timestamp TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_journal_lines_entry_id ON journal_lines(entry_id);
            CREATE INDEX IF NOT EXISTS idx_journal_lines_account_id ON journal_lines(account_id);
            CREATE INDEX IF NOT EXISTS idx_sme_subscriptions_business_status ON sme_subscriptions(business_id, status);
            CREATE INDEX IF NOT EXISTS idx_audit_edit_logs_table_record ON audit_edit_logs(table_name, record_id);
            CREATE INDEX IF NOT EXISTS idx_audit_logs_endpoint_timestamp ON audit_logs(endpoint, timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_audit_logs_user_timestamp ON audit_logs(user_id, timestamp DESC);
            CREATE INDEX IF NOT EXISTS idx_financial_periods_start_end ON financial_periods(start_date, end_date);
            CREATE INDEX IF NOT EXISTS idx_financial_periods_unlock_until ON financial_periods(unlocked_until);
            CREATE INDEX IF NOT EXISTS idx_tax_ledger_entry_id ON tax_ledger(entry_id);
            CREATE INDEX IF NOT EXISTS idx_tax_ledger_supply_type ON tax_ledger(supply_type);
            CREATE INDEX IF NOT EXISTS idx_export_history_report_period ON export_history(report_type, period_from, period_to);
            CREATE INDEX IF NOT EXISTS idx_vendor_trust_score ON vendor_trust_scores(filing_consistency_score);
            CREATE INDEX IF NOT EXISTS idx_ca_invites_email_status ON ca_invites(email, status);
            CREATE INDEX IF NOT EXISTS idx_ca_invites_expires_status ON ca_invites(expires_at, status);
            CREATE INDEX IF NOT EXISTS idx_ca_alert_rules_enabled ON ca_alert_rules(enabled, updated_at);
            CREATE INDEX IF NOT EXISTS idx_ca_alert_events_created ON ca_alert_events(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_ca_alert_events_status ON ca_alert_events(status, risk_level, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_ca_alert_events_gstin ON ca_alert_events(gstin, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_ca_payment_holds_status ON ca_payment_holds(hold_status, hold_until, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_ca_payment_holds_entry ON ca_payment_holds(entry_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_receipt_imports_entry ON receipt_imports(entry_id);
            CREATE INDEX IF NOT EXISTS idx_marketing_signups_updated_at ON marketing_signups(updated_at);
            CREATE INDEX IF NOT EXISTS idx_market_trend_reports_created_at ON market_trend_reports(created_at);
            CREATE INDEX IF NOT EXISTS idx_market_trend_reports_kind ON market_trend_reports(source_kind, created_at);
            CREATE INDEX IF NOT EXISTS idx_inventory_batches_status_expiry ON inventory_batches(status, expiry_date);
            """
        )

        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS gst_filings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                period TEXT NOT NULL,
                filing_type TEXT NOT NULL,
                status TEXT NOT NULL CHECK(status IN ('DRAFT', 'VALIDATION_FAILED', 'READY_FOR_REVIEW', 'APPROVED', 'FILED')),
                summary_data TEXT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                approved_by INTEGER NULL,
                approval_ts TEXT NULL
            );

            CREATE TABLE IF NOT EXISTS gst_validation_issues (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filing_id INTEGER NOT NULL,
                entry_id INTEGER NULL,
                severity TEXT NOT NULL CHECK(severity IN ('BLOCKER', 'WARNING')),
                issue_type TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(filing_id) REFERENCES gst_filings(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_gst_filings_period_type ON gst_filings(period, filing_type, status);
            CREATE INDEX IF NOT EXISTS idx_gst_issues_filing_severity ON gst_validation_issues(filing_id, severity);
            """
        )

        migrate_legacy_schema(conn)

        conn.executemany(
            """
            INSERT OR IGNORE INTO accounts(name, type, balance)
            VALUES (?, ?, ?)
            """,
            [(name, acc_type, money_str(balance)) for name, acc_type, balance in CHART_OF_ACCOUNTS],
        )

        seed_periods(conn)
        seed_hsn_master(conn)
        backfill_chain_of_trust(conn)
        ensure_studio_schema_sqlite(conn)
        ensure_rera_allocation_service().ensure_schema(conn)
        ensure_approval_schema(conn)
        ensure_commission_service().ensure_schema(conn)
        conn.commit()

    if DB_BACKEND == "postgresql":
        ensure_studio_schema_postgres()


def table_has_column(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name});").fetchall()
    return any(row["name"] == column_name for row in rows)


def migrate_legacy_schema(conn: sqlite3.Connection) -> None:
    # Convert REAL balances to TEXT to preserve deterministic decimal values.
    account_cols = conn.execute("PRAGMA table_info(accounts);").fetchall()
    balance_col = next((row for row in account_cols if row["name"] == "balance"), None)
    if balance_col and str(balance_col["type"]).upper() != "TEXT":
        conn.executescript(
            """
            ALTER TABLE accounts RENAME TO accounts_old;

            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                type TEXT NOT NULL CHECK(type IN ('Asset', 'Liability', 'Equity', 'Revenue', 'Expense')),
                balance TEXT NOT NULL DEFAULT '0.0000'
            );

            INSERT INTO accounts(id, name, type, balance)
            SELECT id, name, type, printf('%.4f', COALESCE(balance, 0))
            FROM accounts_old;

            DROP TABLE accounts_old;
            """
        )

    line_cols = conn.execute("PRAGMA table_info(journal_lines);").fetchall()
    debit_col = next((row for row in line_cols if row["name"] == "debit"), None)
    if debit_col and str(debit_col["type"]).upper() != "TEXT":
        conn.executescript(
            """
            ALTER TABLE journal_lines RENAME TO journal_lines_old;

            CREATE TABLE journal_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_id INTEGER NOT NULL,
                account_id INTEGER NOT NULL,
                debit TEXT NOT NULL DEFAULT '0.0000',
                credit TEXT NOT NULL DEFAULT '0.0000',
                FOREIGN KEY(entry_id) REFERENCES journal_entries(id) ON DELETE RESTRICT,
                FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE RESTRICT
            );

            INSERT INTO journal_lines(id, entry_id, account_id, debit, credit)
            SELECT id, entry_id, account_id, printf('%.4f', COALESCE(debit, 0)), printf('%.4f', COALESCE(credit, 0))
            FROM journal_lines_old;

            DROP TABLE journal_lines_old;
            """
        )

        conn.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_journal_lines_entry_id ON journal_lines(entry_id);
            CREATE INDEX IF NOT EXISTS idx_journal_lines_account_id ON journal_lines(account_id);
            """
        )

    if not table_has_column(conn, "journal_entries", "status"):
        conn.execute(
            "ALTER TABLE journal_entries ADD COLUMN status TEXT NOT NULL DEFAULT 'POSTED' CHECK(status IN ('POSTED', 'REVERSED'));"
        )

    if not table_has_column(conn, "journal_entries", "reversal_of_id"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN reversal_of_id INTEGER NULL;")

    if not table_has_column(conn, "journal_entries", "company_state_code"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN company_state_code TEXT NULL;")

    if not table_has_column(conn, "journal_entries", "counterparty_state_code"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN counterparty_state_code TEXT NULL;")

    if not table_has_column(conn, "journal_entries", "counterparty_gstin"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN counterparty_gstin TEXT NULL;")

    if not table_has_column(conn, "journal_entries", "eco_gstin"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN eco_gstin TEXT NULL;")

    if not table_has_column(conn, "journal_entries", "supply_source"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN supply_source TEXT NOT NULL DEFAULT 'DIRECT';")

    if not table_has_column(conn, "journal_entries", "ims_status"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN ims_status TEXT NOT NULL DEFAULT 'PENDING';")

    if not table_has_column(conn, "journal_entries", "vendor_legal_name"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN vendor_legal_name TEXT NULL;")

    if not table_has_column(conn, "journal_entries", "vendor_gstr1_filed_at"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN vendor_gstr1_filed_at TEXT NULL;")

    if not table_has_column(conn, "journal_entries", "is_filed"):
        conn.execute(
            "ALTER TABLE journal_entries ADD COLUMN is_filed INTEGER NOT NULL DEFAULT 0 CHECK(is_filed IN (0, 1));"
        )

    if not table_has_column(conn, "journal_entries", "filed_at"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN filed_at TEXT NULL;")

    if not table_has_column(conn, "journal_entries", "filed_export_hash"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN filed_export_hash TEXT NULL;")

    if not table_has_column(conn, "journal_entries", "approved_by_1"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN approved_by_1 INTEGER NULL;")

    if not table_has_column(conn, "journal_entries", "approved_by_2"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN approved_by_2 INTEGER NULL;")

    if not table_has_column(conn, "journal_entries", "entry_fingerprint"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN entry_fingerprint TEXT NULL;")

    if not table_has_column(conn, "journal_entries", "voucher_type"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN voucher_type TEXT NOT NULL DEFAULT 'JOURNAL';")

    if not table_has_column(conn, "journal_entries", "currency_code"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN currency_code TEXT NOT NULL DEFAULT 'INR';")

    if not table_has_column(conn, "journal_entries", "exchange_rate"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN exchange_rate TEXT NOT NULL DEFAULT '1.0000';")

    if not table_has_column(conn, "journal_entries", "previous_entry_fingerprint"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN previous_entry_fingerprint TEXT NULL;")

    if not table_has_column(conn, "journal_entries", "cumulative_block_hash"):
        conn.execute("ALTER TABLE journal_entries ADD COLUMN cumulative_block_hash TEXT NULL;")

    if not table_has_column(conn, "audit_edit_logs", "high_priority"):
        conn.execute(
            "ALTER TABLE audit_edit_logs ADD COLUMN high_priority INTEGER NOT NULL DEFAULT 0 CHECK(high_priority IN (0, 1));"
        )

    if not table_has_column(conn, "financial_periods", "unlocked_until"):
        conn.execute("ALTER TABLE financial_periods ADD COLUMN unlocked_until TEXT NULL;")

    if not table_has_column(conn, "hsn_master", "is_verified"):
        conn.execute("ALTER TABLE hsn_master ADD COLUMN is_verified INTEGER NOT NULL DEFAULT 0 CHECK(is_verified IN (0, 1));")

    if not table_has_column(conn, "hsn_master", "last_checked"):
        conn.execute("ALTER TABLE hsn_master ADD COLUMN last_checked TEXT NULL;")

    if not table_has_column(conn, "hsn_master", "uqc"):
        conn.execute("ALTER TABLE hsn_master ADD COLUMN uqc TEXT NOT NULL DEFAULT 'NOS';")

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS tax_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_id INTEGER NOT NULL,
            hsn_code TEXT,
            gst_rate_snapshot TEXT NOT NULL,
            taxable_value TEXT NOT NULL,
            tax_amount TEXT NOT NULL,
            supply_type TEXT NOT NULL CHECK(supply_type IN ('B2B', 'B2CS')),
            is_inter_state INTEGER NOT NULL DEFAULT 0 CHECK(is_inter_state IN (0, 1)),
            supply_source TEXT NOT NULL DEFAULT 'DIRECT' CHECK(supply_source IN ('DIRECT', 'ECO')),
            created_at TEXT NOT NULL,
            FOREIGN KEY(entry_id) REFERENCES journal_entries(id) ON DELETE RESTRICT,
            FOREIGN KEY(hsn_code) REFERENCES hsn_master(code) ON DELETE RESTRICT
        );

        CREATE INDEX IF NOT EXISTS idx_tax_ledger_entry_id ON tax_ledger(entry_id);
        CREATE INDEX IF NOT EXISTS idx_tax_ledger_supply_type ON tax_ledger(supply_type);

        CREATE TABLE IF NOT EXISTS export_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_type TEXT NOT NULL,
            period_id INTEGER NULL,
            period_from TEXT NOT NULL,
            period_to TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            payload_hash TEXT NOT NULL UNIQUE,
            payload_fingerprint TEXT NULL,
            approved_by_1 INTEGER NULL,
            approved_by_2 INTEGER NULL,
            last_verification_status TEXT NULL,
            last_verified_by INTEGER NULL,
            last_verified_at TEXT NULL,
            security_hold_until TEXT NULL,
            status TEXT NOT NULL CHECK(status IN ('GENERATED', 'FILED', 'FAILED')),
            arn_number TEXT NULL,
            FOREIGN KEY(period_id) REFERENCES financial_periods(id) ON DELETE SET NULL
        );

        CREATE INDEX IF NOT EXISTS idx_export_history_report_period ON export_history(report_type, period_from, period_to);

        CREATE TABLE IF NOT EXISTS vendor_trust_scores (
            gstin TEXT PRIMARY KEY,
            legal_name TEXT,
            filing_consistency_score REAL NOT NULL DEFAULT 100.0,
            avg_filing_delay_days INTEGER NOT NULL DEFAULT 0,
            last_gstr1_filed_at TEXT NULL,
            total_itc_at_risk TEXT NOT NULL DEFAULT '0.0000',
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS safe_harbor_attestations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            as_of_date TEXT NOT NULL,
            min_credit_balance TEXT NOT NULL,
            reversal_amount TEXT NOT NULL,
            liability_offset TEXT NOT NULL,
            status TEXT NOT NULL,
            legal_basis TEXT NOT NULL,
            certified_by INTEGER NOT NULL,
            certified_role TEXT NOT NULL,
            note TEXT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ca_invites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            token TEXT NOT NULL UNIQUE,
            expires_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING' CHECK(status IN ('PENDING', 'ACCEPTED', 'REVOKED', 'EXPIRED')),
            accepted_by INTEGER NULL,
            accepted_at TEXT NULL,
            created_by INTEGER NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ca_alert_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_key TEXT NOT NULL UNIQUE,
            display_name TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0, 1)),
            min_trust_score REAL NOT NULL DEFAULT 60.0,
            min_itc_risk TEXT NOT NULL DEFAULT '0.0000',
            target_risk_levels TEXT NOT NULL,
            channels_json TEXT NOT NULL,
            created_by INTEGER NOT NULL,
            updated_by INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ca_alert_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_hash TEXT NOT NULL UNIQUE,
            rule_key TEXT NOT NULL,
            gstin TEXT NOT NULL,
            vendor_name TEXT NOT NULL,
            risk_level TEXT NOT NULL,
            severity TEXT NOT NULL,
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            channels_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN' CHECK(status IN ('OPEN', 'ACKNOWLEDGED', 'CLOSED')),
            event_source TEXT NOT NULL,
            metadata_json TEXT NULL,
            acknowledged_by INTEGER NULL,
            acknowledged_at TEXT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS ca_payment_holds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id INTEGER NOT NULL UNIQUE,
            entry_id INTEGER NOT NULL,
            gstin TEXT NOT NULL,
            vendor_name TEXT NOT NULL,
            hold_reason TEXT NOT NULL,
            hold_status TEXT NOT NULL DEFAULT 'OPEN' CHECK(hold_status IN ('OPEN', 'RELEASED', 'EXPIRED')),
            hold_until TEXT NOT NULL,
            nudge_subject TEXT NOT NULL,
            nudge_message TEXT NOT NULL,
            created_by INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            released_by INTEGER NULL,
            released_at TEXT NULL,
            FOREIGN KEY(alert_id) REFERENCES ca_alert_events(id) ON DELETE CASCADE,
            FOREIGN KEY(entry_id) REFERENCES journal_entries(id) ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS receipt_imports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_id INTEGER NULL,
            file_path TEXT NOT NULL,
            ocr_text TEXT NULL,
            extracted_json TEXT NULL,
            model_response TEXT NULL,
            status TEXT NOT NULL DEFAULT 'PROCESSED',
            created_by INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(entry_id) REFERENCES journal_entries(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS marketing_signups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            provider TEXT NOT NULL,
            source TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS market_trend_reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_file_path TEXT NOT NULL,
            source_kind TEXT NOT NULL,
            period_hint TEXT NULL,
            model_used TEXT NOT NULL,
            analysis_json TEXT NOT NULL,
            report_file_path TEXT NULL,
            created_by INTEGER NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS inventory_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku_code TEXT NOT NULL,
            sku_name TEXT NOT NULL,
            batch_code TEXT NOT NULL,
            hsn_code TEXT NOT NULL,
            gst_rate TEXT NOT NULL,
            quantity TEXT NOT NULL,
            unit_cost TEXT NOT NULL,
            total_value TEXT NOT NULL,
            expiry_date TEXT NULL,
            status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK(status IN ('ACTIVE', 'EXPIRED', 'DEPLETED')),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            created_by INTEGER NOT NULL,
            UNIQUE(sku_code, batch_code)
        );

        CREATE TABLE IF NOT EXISTS sme_subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id TEXT NOT NULL,
            razorpay_subscription_id TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'PENDING',
            current_period_end TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_vendor_trust_score ON vendor_trust_scores(filing_consistency_score);
        CREATE INDEX IF NOT EXISTS idx_safe_harbor_attestations_as_of ON safe_harbor_attestations(as_of_date, created_at);
        CREATE INDEX IF NOT EXISTS idx_sme_subscriptions_business_status ON sme_subscriptions(business_id, status);
        CREATE INDEX IF NOT EXISTS idx_ca_invites_email_status ON ca_invites(email, status);
        CREATE INDEX IF NOT EXISTS idx_ca_invites_expires_status ON ca_invites(expires_at, status);
        CREATE INDEX IF NOT EXISTS idx_ca_alert_rules_enabled ON ca_alert_rules(enabled, updated_at);
        CREATE INDEX IF NOT EXISTS idx_ca_alert_events_created ON ca_alert_events(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_ca_alert_events_status ON ca_alert_events(status, risk_level, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_ca_alert_events_gstin ON ca_alert_events(gstin, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_ca_payment_holds_status ON ca_payment_holds(hold_status, hold_until, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_ca_payment_holds_entry ON ca_payment_holds(entry_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_receipt_imports_entry ON receipt_imports(entry_id);
        CREATE INDEX IF NOT EXISTS idx_marketing_signups_updated_at ON marketing_signups(updated_at);
        CREATE INDEX IF NOT EXISTS idx_market_trend_reports_created_at ON market_trend_reports(created_at);
        CREATE INDEX IF NOT EXISTS idx_market_trend_reports_kind ON market_trend_reports(source_kind, created_at);
        CREATE INDEX IF NOT EXISTS idx_inventory_batches_status_expiry ON inventory_batches(status, expiry_date);
        """
    )

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS gst_filings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            period TEXT NOT NULL,
            filing_type TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('DRAFT', 'VALIDATION_FAILED', 'READY_FOR_REVIEW', 'APPROVED', 'FILED')),
            summary_data TEXT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            approved_by INTEGER NULL,
            approval_ts TEXT NULL
        );

        CREATE TABLE IF NOT EXISTS gst_validation_issues (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filing_id INTEGER NOT NULL,
            entry_id INTEGER NULL,
            severity TEXT NOT NULL CHECK(severity IN ('BLOCKER', 'WARNING')),
            issue_type TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(filing_id) REFERENCES gst_filings(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_gst_filings_period_type ON gst_filings(period, filing_type, status);
        CREATE INDEX IF NOT EXISTS idx_gst_issues_filing_severity ON gst_validation_issues(filing_id, severity);
        """
    )

    if not table_has_column(conn, "marketing_signups", "updated_at"):
        conn.execute(
            "ALTER TABLE marketing_signups ADD COLUMN updated_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00Z';"
        )

    if not table_has_column(conn, "tax_ledger", "is_inter_state"):
        conn.execute(
            "ALTER TABLE tax_ledger ADD COLUMN is_inter_state INTEGER NOT NULL DEFAULT 0 CHECK(is_inter_state IN (0, 1));"
        )

    if not table_has_column(conn, "tax_ledger", "supply_source"):
        conn.execute(
            "ALTER TABLE tax_ledger ADD COLUMN supply_source TEXT NOT NULL DEFAULT 'DIRECT';"
        )

    if not table_has_column(conn, "export_history", "payload_fingerprint"):
        conn.execute("ALTER TABLE export_history ADD COLUMN payload_fingerprint TEXT NULL;")

    if not table_has_column(conn, "export_history", "approved_by_1"):
        conn.execute("ALTER TABLE export_history ADD COLUMN approved_by_1 INTEGER NULL;")

    if not table_has_column(conn, "export_history", "approved_by_2"):
        conn.execute("ALTER TABLE export_history ADD COLUMN approved_by_2 INTEGER NULL;")

    if not table_has_column(conn, "export_history", "last_verification_status"):
        conn.execute("ALTER TABLE export_history ADD COLUMN last_verification_status TEXT NULL;")

    if not table_has_column(conn, "export_history", "last_verified_by"):
        conn.execute("ALTER TABLE export_history ADD COLUMN last_verified_by INTEGER NULL;")

    if not table_has_column(conn, "export_history", "last_verified_at"):
        conn.execute("ALTER TABLE export_history ADD COLUMN last_verified_at TEXT NULL;")

    if not table_has_column(conn, "export_history", "security_hold_until"):
        conn.execute("ALTER TABLE export_history ADD COLUMN security_hold_until TEXT NULL;")

    try:
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_journal_entries_reference ON journal_entries(reference);")
    except sqlite3.IntegrityError as exc:
        raise RuntimeError("Duplicate journal reference values found; fix duplicates before enabling sequence guard") from exc


def seed_periods(conn: sqlite3.Connection) -> None:
    periods: list[tuple[str, str, str]] = []
    for year in (2025, 2026):
        fy_start = date(year, 4, 1)
        for offset in range(12):
            month = ((fy_start.month - 1 + offset) % 12) + 1
            period_year = fy_start.year + ((fy_start.month - 1 + offset) // 12)
            start_date = date(period_year, month, 1)
            if month == 12:
                end_date = date(period_year + 1, 1, 1)
            else:
                end_date = date(period_year, month + 1, 1)
            end_date = end_date.fromordinal(end_date.toordinal() - 1)
            periods.append((start_date.strftime("%B %Y"), start_date.isoformat(), end_date.isoformat()))

    conn.executemany(
        """
        INSERT OR IGNORE INTO financial_periods(period_name, start_date, end_date, is_locked)
        VALUES (?, ?, ?, 0)
        """,
        periods,
    )


def seed_hsn_master(conn: sqlite3.Connection) -> None:
    # SME-focused seed set; rates should be reviewed against latest official notifications before production use.
    rows = [
        ("010100", "Live horses, asses, mules and hinnies", "0.0000"),
        ("020100", "Meat of bovine animals, fresh or chilled", "5.0000"),
        ("040100", "Milk and cream, not concentrated", "5.0000"),
        ("100100", "Wheat and meslin", "0.0000"),
        ("100600", "Rice", "0.0000"),
        ("110100", "Wheat or meslin flour", "5.0000"),
        ("170100", "Cane or beet sugar", "5.0000"),
        ("190500", "Bread, pastry, cakes and biscuits", "18.0000"),
        ("210600", "Food preparations n.e.s.", "18.0000"),
        ("220100", "Water, including mineral waters", "12.0000"),
        ("220200", "Non-alcoholic beverages", "28.0000"),
        ("240200", "Cigars, cheroots, cigarillos and cigarettes", "28.0000"),
        ("271000", "Petroleum oils and oils from bituminous minerals", "18.0000"),
        ("300400", "Medicaments in measured doses", "12.0000"),
        ("330400", "Beauty or make-up preparations", "28.0000"),
        ("340100", "Soap; organic surface-active products", "18.0000"),
        ("392300", "Articles for packing of goods, of plastics", "18.0000"),
        ("401100", "New pneumatic tyres", "28.0000"),
        ("441200", "Plywood, veneered panels", "18.0000"),
        ("481900", "Cartons, boxes and cases, of paper", "12.0000"),
        ("520800", "Woven fabrics of cotton", "5.0000"),
        ("610900", "T-shirts, singlets and other vests", "12.0000"),
        ("620300", "Men's or boys' suits and ensembles", "12.0000"),
        ("630200", "Bed linen, table linen, toilet linen", "12.0000"),
        ("640300", "Footwear with outer soles of rubber/plastics", "18.0000"),
        ("680200", "Worked monumental/building stone", "18.0000"),
        ("690700", "Ceramic flags and paving", "18.0000"),
        ("700900", "Glass mirrors", "18.0000"),
        ("721400", "Bars and rods, of iron or non-alloy steel", "18.0000"),
        ("730800", "Structures and parts of structures, of iron/steel", "18.0000"),
        ("740800", "Copper wire", "18.0000"),
        ("760400", "Aluminium bars, rods and profiles", "18.0000"),
        ("820500", "Hand tools", "18.0000"),
        ("830200", "Base metal mountings and fittings", "18.0000"),
        ("841300", "Pumps for liquids", "18.0000"),
        ("841500", "Air conditioning machines", "28.0000"),
        ("842100", "Filtering or purifying machinery", "18.0000"),
        ("847100", "Automatic data processing machines", "18.0000"),
        ("848300", "Transmission shafts and gears", "18.0000"),
        ("850100", "Electric motors and generators", "18.0000"),
        ("850400", "Electrical transformers", "18.0000"),
        ("851700", "Telephone sets and smartphones", "18.0000"),
        ("852800", "Monitors and projectors", "18.0000"),
        ("853600", "Electrical apparatus for switching/protecting circuits", "18.0000"),
        ("870300", "Motor cars and motor vehicles", "28.0000"),
        ("871400", "Parts and accessories for motorcycles", "28.0000"),
        ("900400", "Spectacles and goggles", "12.0000"),
        ("940100", "Seats", "18.0000"),
        ("940300", "Other furniture", "18.0000"),
        ("950300", "Tricycles, scooters, dolls and toys", "12.0000"),
    ]
    checked_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    validated_rows: list[tuple[str, str, str, str, str]] = []
    for code, description, rate in rows:
        validated_rows.append((validate_hsn_code_format(code), description, rate, checked_at, normalize_uqc("NOS")))

    conn.executemany(
        """
        INSERT OR IGNORE INTO hsn_master(code, description, gst_rate, is_active, is_verified, last_checked, uqc)
        VALUES (?, ?, ?, 1, 1, ?, ?)
        """,
        validated_rows,
    )


def enforce_auto_relock(conn: sqlite3.Connection, now_dt: datetime) -> None:
    rows = conn.execute(
        """
        SELECT id, period_name, unlocked_until
        FROM financial_periods
        WHERE is_locked = 0
          AND unlocked_until IS NOT NULL
          AND unlocked_until <= ?
        """,
        (now_dt.isoformat(timespec="seconds") + "Z",),
    ).fetchall()

    for row in rows:
        conn.execute(
            """
            UPDATE financial_periods
            SET is_locked = 1,
                unlocked_until = NULL
            WHERE id = ?
            """,
            (row["id"],),
        )
        log_audit(
            conn,
            table_name="financial_periods",
            record_id=row["id"],
            action="AUTO_RELOCK",
            old_value={"is_locked": False, "unlocked_until": row["unlocked_until"]},
            new_value={"is_locked": True, "reason": "24h cooldown elapsed"},
            high_priority=True,
        )


def get_hsn_rate(conn: sqlite3.Connection, hsn_code: str) -> Decimal:
    hsn_code = validate_hsn_code_format(hsn_code)
    row = conn.execute(
        """
        SELECT gst_rate, is_active, is_verified, last_checked
        FROM hsn_master
        WHERE code = ?
        """,
        (hsn_code,),
    ).fetchone()
    if row is None or row["is_active"] != 1:
        raise HTTPException(status_code=400, detail=f"Invalid or inactive HSN code: {hsn_code}")
    if row["is_verified"] != 1:
        raise HTTPException(status_code=400, detail=f"Unverified HSN code: {hsn_code}")
    return money(row["gst_rate"])


def get_hsn_uqc(conn: sqlite3.Connection, hsn_code: str) -> str:
    row = conn.execute("SELECT uqc FROM hsn_master WHERE code = ?", (hsn_code,)).fetchone()
    if row is None:
        return "NOS"
    return normalize_uqc(str(row["uqc"]))


def get_period_meta(conn: sqlite3.Connection, entry_date: str) -> tuple[str | None, bool]:
    row = conn.execute(
        """
        SELECT period_name, is_locked
        FROM financial_periods
        WHERE start_date <= ? AND end_date >= ?
        LIMIT 1
        """,
        (entry_date, entry_date),
    ).fetchone()
    if row is None:
        return None, False
    return row["period_name"], bool(row["is_locked"])


def build_integrity_hash(reference: str, lines: list[sqlite3.Row]) -> str:
    payload = {
        "reference": reference,
        "lines": [
            {
                "account_id": int(line["account_id"]),
                "debit": money_str(line["debit"]),
                "credit": money_str(line["credit"]),
            }
            for line in lines
        ],
    }
    digest = sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
    return digest


def stamp_entry_fingerprint(conn: sqlite3.Connection, entry_id: int) -> str:
    entry = conn.execute("SELECT reference FROM journal_entries WHERE id = ?", (entry_id,)).fetchone()
    if entry is None:
        return ""

    lines = conn.execute(
        """
        SELECT account_id, debit, credit
        FROM journal_lines
        WHERE entry_id = ?
        ORDER BY id ASC
        """,
        (entry_id,),
    ).fetchall()
    if not lines:
        return ""

    normalized_lines = [
        {
            "account_name": str(line["name"]) if "name" in line.keys() else "",
            "debit": line["debit"],
            "credit": line["credit"],
        }
        for line in conn.execute(
            """
            SELECT jl.debit, jl.credit, a.name
            FROM journal_lines jl
            JOIN accounts a ON a.id = jl.account_id
            WHERE jl.entry_id = ?
            ORDER BY jl.id ASC
            """,
            (entry_id,),
        ).fetchall()
    ]
    voucher_type = voucher_service.classify_from_lines(normalized_lines)

    previous = conn.execute(
        """
        SELECT id, entry_fingerprint, cumulative_block_hash
        FROM journal_entries
        WHERE id < ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (entry_id,),
    ).fetchone()
    previous_fingerprint = str(previous["entry_fingerprint"] or "") if previous is not None else "GENESIS"
    previous_block_hash = str(previous["cumulative_block_hash"] or previous_fingerprint) if previous is not None else "GENESIS"

    digest = build_integrity_hash(str(entry["reference"]), lines)
    cumulative_block_hash = sha256(f"{previous_block_hash}:{digest}".encode("utf-8")).hexdigest()

    conn.execute(
        """
        UPDATE journal_entries
        SET entry_fingerprint = ?,
            previous_entry_fingerprint = ?,
            cumulative_block_hash = ?,
            voucher_type = ?
        WHERE id = ?
        """,
        (digest, previous_fingerprint, cumulative_block_hash, voucher_type, entry_id),
    )
    return digest


def backfill_chain_of_trust(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        """
        SELECT id
        FROM journal_entries
        ORDER BY id ASC
        """
    ).fetchall()
    for row in rows:
        stamp_entry_fingerprint(conn, int(row["id"]))


def get_latest_cumulative_block_hash(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        """
        SELECT cumulative_block_hash
        FROM journal_entries
        WHERE cumulative_block_hash IS NOT NULL AND cumulative_block_hash != ''
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return "GENESIS"
    return str(row["cumulative_block_hash"])


def verify_chain_of_trust(conn: sqlite3.Connection, limit: int = 5000) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT id, reference, entry_fingerprint, previous_entry_fingerprint, cumulative_block_hash, created_at
        FROM journal_entries
        WHERE status = 'POSTED'
        ORDER BY id ASC
        LIMIT ?
        """,
        (max(1, min(limit, 50000)),),
    ).fetchall()

    timeline: list[dict[str, Any]] = []
    mismatches: list[dict[str, Any]] = []
    prior_block_hash = "GENESIS"
    prior_fingerprint = "GENESIS"

    for row in rows:
        lines = conn.execute(
            """
            SELECT account_id, debit, credit
            FROM journal_lines
            WHERE entry_id = ?
            ORDER BY id ASC
            """,
            (int(row["id"]),),
        ).fetchall()
        expected_fingerprint = build_integrity_hash(str(row["reference"]), lines)
        expected_block_hash = sha256(f"{prior_block_hash}:{expected_fingerprint}".encode("utf-8")).hexdigest()
        stored_fingerprint = str(row["entry_fingerprint"] or "")
        stored_prev = str(row["previous_entry_fingerprint"] or "")
        stored_block = str(row["cumulative_block_hash"] or "")

        verified = (
            stored_fingerprint == expected_fingerprint
            and stored_block == expected_block_hash
            and (stored_prev == prior_fingerprint or (stored_prev == "GENESIS" and prior_fingerprint == "GENESIS"))
        )

        item = {
            "entry_id": int(row["id"]),
            "reference": str(row["reference"]),
            "stored_fingerprint": stored_fingerprint,
            "calculated_fingerprint": expected_fingerprint,
            "stored_previous_fingerprint": stored_prev,
            "expected_previous_fingerprint": prior_fingerprint,
            "stored_block_hash": stored_block,
            "calculated_block_hash": expected_block_hash,
            "verified": verified,
            "created_at": str(row["created_at"] or ""),
        }
        timeline.append(item)
        if not verified:
            mismatches.append(item)

        prior_fingerprint = expected_fingerprint
        prior_block_hash = expected_block_hash

    total = len(timeline)
    verified_count = total - len(mismatches)
    integrity_score = round((verified_count / total) * 100, 2) if total > 0 else 100.0
    return {
        "status": "ok" if len(mismatches) == 0 else "TAMPER_DETECTED",
        "hash_algorithm": "SHA-256",
        "total_checked": total,
        "verified_count": verified_count,
        "mismatch_count": len(mismatches),
        "integrity_score": integrity_score,
        "cumulative_block_hash": prior_block_hash,
        "timeline": timeline,
        "mismatches": mismatches,
    }


def generate_tally_export(conn: sqlite3.Connection, entry_id: int) -> tuple[bytes, str, Path, str, str]:
    entry = conn.execute(
        """
        SELECT id, date, reference, description, status
        FROM journal_entries
        WHERE id = ?
        """,
        (entry_id,),
    ).fetchone()
    if entry is None:
        raise HTTPException(status_code=404, detail="Journal entry not found")

    lines = conn.execute(
        """
        SELECT jl.debit, jl.credit, a.name
        FROM journal_lines jl
        JOIN accounts a ON a.id = jl.account_id
        WHERE jl.entry_id = ?
        ORDER BY jl.id ASC
        """,
        (entry_id,),
    ).fetchall()
    if not lines:
        raise HTTPException(status_code=422, detail="Entry has no journal lines")

    envelope = ET.Element("ENVELOPE")
    header = ET.SubElement(envelope, "HEADER")
    ET.SubElement(header, "TALLYREQUEST").text = "Import Data"

    body = ET.SubElement(envelope, "BODY")
    import_data = ET.SubElement(body, "IMPORTDATA")
    request_desc = ET.SubElement(import_data, "REQUESTDESC")
    ET.SubElement(request_desc, "REPORTNAME").text = "Vouchers"
    request_data = ET.SubElement(import_data, "REQUESTDATA")

    tally_message = ET.SubElement(request_data, "TALLYMESSAGE")
    voucher = ET.SubElement(tally_message, "VOUCHER", {"VCHTYPE": "Journal", "ACTION": "Create"})
    ET.SubElement(voucher, "DATE").text = str(entry["date"]).replace("-", "")
    ET.SubElement(voucher, "NARRATION").text = str(entry["description"] or "Vision Ledger Import")
    ET.SubElement(voucher, "VOUCHERNUMBER").text = str(entry["reference"])

    for line in lines:
        amount = money(line["debit"]) - money(line["credit"])
        ledger_entry = ET.SubElement(voucher, "ALLLEDGERENTRIES.LIST")
        ET.SubElement(ledger_entry, "LEDGERNAME").text = str(line["name"])
        ET.SubElement(ledger_entry, "ISDEEMEDPOSITIVE").text = "Yes" if amount < 0 else "No"
        ET.SubElement(ledger_entry, "AMOUNT").text = f"{amount:.4f}"

    xml_bytes = ET.tostring(envelope, encoding="utf-8", xml_declaration=True)
    safe_reference = re.sub(r"[^A-Za-z0-9._-]+", "-", str(entry["reference"]))
    filename = f"Accord_Tally_Export_{safe_reference}.xml"
    TALLY_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    export_path = TALLY_EXPORT_DIR / filename
    export_path.write_bytes(xml_bytes)
    return xml_bytes, filename, export_path, str(entry["reference"]), str(entry["status"])


def get_audit_header(reference_no: str, period_label: str | None, period_is_locked: bool, lines: list[sqlite3.Row]) -> dict[str, Any]:
    return {
        "system_id": "ACCORD-ERP-V1",
        "reference_no": reference_no,
        "period_label": period_label,
        "integrity_hash": build_integrity_hash(reference_no, lines),
        "compliance_status": "CERTIFIED" if period_is_locked else "DRAFT/OPEN",
    }


def slab_compliance_risk(gst_rate: Decimal) -> str | None:
    normalized = money(gst_rate)
    if normalized in GST_2026_ALLOWED_SLABS:
        return None
    return f"Legacy slab detected ({money_str(normalized)}). Review against 2026 GST 2.0 slab set."


def resolve_period_id(conn: sqlite3.Connection, report_from: date, report_to: date) -> int | None:
    row = conn.execute(
        """
        SELECT id
        FROM financial_periods
        WHERE start_date <= ?
          AND end_date >= ?
        ORDER BY id ASC
        LIMIT 1
        """,
        (report_from.isoformat(), report_to.isoformat()),
    ).fetchone()
    if row is None:
        return None
    return int(row["id"])


def create_export_history(
    conn: sqlite3.Connection,
    report_type: str,
    period_id: int | None,
    report_from: date,
    report_to: date,
    payload_hash: str,
    payload_fingerprint: str | None = None,
    approved_by_1: int | None = None,
    approved_by_2: int | None = None,
    status: str = "GENERATED",
    arn_number: str | None = None,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO export_history(
            report_type,
            period_id,
            period_from,
            period_to,
            generated_at,
            payload_hash,
            payload_fingerprint,
            approved_by_1,
            approved_by_2,
            status,
            arn_number
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            report_type,
            period_id,
            report_from.isoformat(),
            report_to.isoformat(),
            datetime.utcnow().isoformat(timespec="seconds") + "Z",
            payload_hash,
            payload_fingerprint,
            approved_by_1,
            approved_by_2,
            status,
            arn_number,
        ),
    )
    return int(cursor.lastrowid)


def recompute_vendor_trust(conn: sqlite3.Connection, gstin: str) -> None:
    rows = conn.execute(
        """
        SELECT je.date,
               je.vendor_legal_name,
               je.vendor_gstr1_filed_at,
               je.ims_status,
               tl.tax_amount
        FROM journal_entries je
        JOIN tax_ledger tl ON tl.entry_id = je.id
        WHERE tl.supply_type = 'B2B'
          AND je.counterparty_gstin = ?
        ORDER BY je.date ASC, je.id ASC
        """,
        (gstin,),
    ).fetchall()

    if not rows:
        return

    accepted = 0
    rejected = 0
    pending = 0
    total_delay_days = 0
    filed_count = 0
    high_risk_delay_count = 0
    itc_at_risk = Decimal("0")
    legal_name: str | None = None
    last_filed_at: str | None = None

    for row in rows:
        legal_name = legal_name or row["vendor_legal_name"]
        ims_status = normalize_ims_status(row["ims_status"])

        if ims_status == "ACCEPTED":
            accepted += 1
        elif ims_status == "REJECTED":
            rejected += 1
        else:
            pending += 1

        tax_amount = money(row["tax_amount"])
        if ims_status != "ACCEPTED":
            itc_at_risk += tax_amount

        if row["vendor_gstr1_filed_at"]:
            filed_dt = date.fromisoformat(str(row["vendor_gstr1_filed_at"]))
            invoice_dt = date.fromisoformat(str(row["date"]))
            delay_days = max((filed_dt - invoice_dt).days, 0)
            total_delay_days += delay_days
            filed_count += 1
            if is_high_risk_delay(invoice_dt, filed_dt):
                high_risk_delay_count += 1
            if last_filed_at is None or filed_dt.isoformat() > last_filed_at:
                last_filed_at = filed_dt.isoformat()

    avg_delay_days = int(total_delay_days / filed_count) if filed_count else 0

    # Penalize pending/rejected behavior and sustained filing delay.
    penalty = (pending * 4.0) + (rejected * 12.0) + (max(avg_delay_days - 2, 0) * 1.25) + (high_risk_delay_count * 10.0)
    score = max(0.0, min(100.0, 100.0 - penalty))

    conn.execute(
        """
        INSERT INTO vendor_trust_scores(
            gstin,
            legal_name,
            filing_consistency_score,
            avg_filing_delay_days,
            last_gstr1_filed_at,
            total_itc_at_risk,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(gstin) DO UPDATE SET
            legal_name = excluded.legal_name,
            filing_consistency_score = excluded.filing_consistency_score,
            avg_filing_delay_days = excluded.avg_filing_delay_days,
            last_gstr1_filed_at = excluded.last_gstr1_filed_at,
            total_itc_at_risk = excluded.total_itc_at_risk,
            updated_at = excluded.updated_at
        """,
        (
            gstin,
            legal_name,
            round(score, 2),
            avg_delay_days,
            last_filed_at,
            money_str(itc_at_risk),
            datetime.utcnow().isoformat(timespec="seconds") + "Z",
        ),
    )


def check_period_lock(conn: sqlite3.Connection, entry_date: date) -> None:
    row = conn.execute(
        """
        SELECT period_name
        FROM financial_periods
        WHERE start_date <= ? AND end_date >= ? AND is_locked = 1
        LIMIT 1
        """,
        (entry_date.isoformat(), entry_date.isoformat()),
    ).fetchone()
    if row is not None:
        raise HTTPException(status_code=403, detail=f"Period is locked: {row['period_name']}")


def get_fiscal_year_label(entry_date: date) -> str:
    # India FY boundary: Apr 1 to Mar 31.
    start_year = entry_date.year if entry_date.month >= 4 else entry_date.year - 1
    end_year = start_year + 1
    return f"{str(start_year)[-2:]}-{str(end_year)[-2:]}"


def next_journal_reference(conn: sqlite3.Connection, entry_date: date) -> str:
    fy = get_fiscal_year_label(entry_date)
    key = f"JOURNAL-{fy}"
    prefix = f"ACC/{fy}/"

    conn.execute(
        """
        INSERT OR IGNORE INTO document_sequences(sequence_key, prefix, current_value)
        VALUES (?, ?, 0)
        """,
        (key, prefix),
    )
    conn.execute(
        """
        UPDATE document_sequences
        SET current_value = current_value + 1
        WHERE sequence_key = ?
        """,
        (key,),
    )
    row = conn.execute(
        """
        SELECT prefix, current_value
        FROM document_sequences
        WHERE sequence_key = ?
        """,
        (key,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=500, detail="Failed to generate journal reference")
    return f"{row['prefix']}{int(row['current_value']):06d}"


def validate_line_items(lines: list[JournalLineIn]) -> tuple[Decimal, Decimal]:
    total_debits = Decimal("0")
    total_credits = Decimal("0")

    for line in lines:
        debit = money(line.debit)
        credit = money(line.credit)

        if debit == 0 and credit == 0:
            raise HTTPException(status_code=400, detail="Each line must have debit or credit amount.")

        if debit > 0 and credit > 0:
            raise HTTPException(status_code=400, detail="A single line cannot contain both debit and credit.")

        total_debits += debit
        total_credits += credit

    if total_debits != total_credits:
        raise HTTPException(
            status_code=400,
            detail=f"Unbalanced entry: total_debits={total_debits} total_credits={total_credits}",
        )

    return total_debits, total_credits


def account_exists(conn: sqlite3.Connection, account_id: int) -> bool:
    row = conn.execute("SELECT 1 FROM accounts WHERE id = ?", (account_id,)).fetchone()
    return row is not None


def get_account_id_by_name(conn: sqlite3.Connection, account_name: str) -> int:
    row = conn.execute("SELECT id FROM accounts WHERE name = ?", (account_name,)).fetchone()
    if row is None:
        raise HTTPException(status_code=400, detail=f"Required account missing: {account_name}")
    return int(row["id"])


def get_account_type(conn: sqlite3.Connection, account_id: int) -> str:
    row = conn.execute("SELECT type FROM accounts WHERE id = ?", (account_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=400, detail=f"Account {account_id} does not exist")
    return str(row["type"])


def compute_taxable_value(conn: sqlite3.Connection, lines: list[JournalLineIn]) -> Decimal:
    taxable = Decimal("0")
    for line in lines:
        account_type = get_account_type(conn, line.account_id)
        if account_type == "Revenue":
            taxable += money(line.credit) - money(line.debit)
    return money(max(taxable, Decimal("0")))


def update_account_balance(conn: sqlite3.Connection, account_id: int, debit: Decimal, credit: Decimal) -> None:
    row = conn.execute("SELECT balance FROM accounts WHERE id = ?", (account_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=400, detail=f"Account {account_id} does not exist")

    current_balance = money(row["balance"])
    new_balance = current_balance + debit - credit
    conn.execute("UPDATE accounts SET balance = ? WHERE id = ?", (money_str(new_balance), account_id))


def log_audit(
    conn: sqlite3.Connection,
    table_name: str,
    record_id: int,
    action: str,
    old_value: dict[str, Any] | None,
    new_value: dict[str, Any] | None,
    user_id: int = 0,
    high_priority: bool = False,
) -> None:
    conn.execute(
        """
        INSERT INTO audit_edit_logs(table_name, record_id, user_id, action, high_priority, old_value, new_value, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            table_name,
            record_id,
            user_id,
            action,
            1 if high_priority else 0,
            json.dumps(old_value) if old_value is not None else None,
            json.dumps(new_value) if new_value is not None else None,
            datetime.utcnow().isoformat(timespec="seconds") + "Z",
        ),
    )


def normalize_role(role: str | None) -> str:
    if role is None:
        return ""
    return role.strip().lower()


def require_role(role: str | None, allowed: set[str]) -> str:
    normalized = normalize_role(role)
    if normalized not in allowed:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return normalized


def require_admin_id(admin_id: str | None) -> int:
    if admin_id is None:
        raise HTTPException(status_code=401, detail="Missing X-Admin-Id header")
    try:
        parsed = int(admin_id.strip())
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=422, detail="X-Admin-Id must be a positive integer") from exc
    if parsed <= 0:
        raise HTTPException(status_code=422, detail="X-Admin-Id must be a positive integer")
    return parsed


app.include_router(create_booking_router(get_conn, require_role, require_admin_id))
if create_broker_router is not None:
    app.include_router(create_broker_router(get_conn))
app.include_router(create_invoice_router(get_conn, require_role, require_admin_id))
app.include_router(create_chat_router(get_conn, require_role, require_admin_id))
app.include_router(create_vendor_router(get_conn))
app.include_router(create_phase7_router(get_conn, require_role, require_admin_id))
app.include_router(create_portal_router(get_conn))
if create_project_router is not None:
    app.include_router(create_project_router(get_conn, require_role, require_admin_id))
app.include_router(create_webhook_router(get_conn))
app.include_router(create_billing_router(get_conn))
if create_approval_router is not None:
    app.include_router(create_approval_router(get_conn, require_role, require_admin_id))
if create_pricing_router is not None:
    app.include_router(create_pricing_router(get_conn, require_role, require_admin_id))
app.include_router(create_kyc_router(require_role, require_admin_id))
app.include_router(create_default_risk_router(get_conn, require_role, require_admin_id))
app.include_router(support_router)
app.include_router(create_sme_router(get_conn))
app.include_router(create_sme_inventory_router(get_conn))
app.include_router(create_sme_payable_router(get_conn))
app.include_router(create_supplier_router(get_conn))
app.include_router(create_autonomous_purchasing_router(get_conn))
app.include_router(create_iot_telemetry_router(get_conn))
app.include_router(create_sme_webauthn_router(get_conn))
app.include_router(create_otp_auth_router(get_conn))
app.include_router(create_gstn_sandbox_router(get_conn, require_role, require_admin_id))
app.include_router(create_superadmin_router(get_conn))
app.include_router(create_sme_sync_router())


def biometric_signature(admin_id: int, action: str, issued_at_unix: int) -> str:
    payload = f"{admin_id}:{action}:{issued_at_unix}".encode("utf-8")
    return hmac.new(BIOMETRIC_SECRET.encode("utf-8"), payload, "sha256").hexdigest()


def issue_biometric_token(admin_id: int, action: str) -> dict[str, Any]:
    issued_at = int(datetime.utcnow().timestamp())
    expires_at = issued_at + BIOMETRIC_TOKEN_TTL_SECONDS
    sig = biometric_signature(admin_id, action, issued_at)
    token = f"{admin_id}.{issued_at}.{sig}"
    return {
        "token": token,
        "issued_at": datetime.utcfromtimestamp(issued_at).isoformat(timespec="seconds") + "Z",
        "expires_at": datetime.utcfromtimestamp(expires_at).isoformat(timespec="seconds") + "Z",
        "ttl_seconds": BIOMETRIC_TOKEN_TTL_SECONDS,
        "action": action,
    }


def require_biometric_signoff(token: str | None, admin_id: int, action: str) -> None:
    if token is None:
        raise HTTPException(status_code=401, detail="Missing X-Biometric-Token header")

    parts = token.strip().split(".")
    if len(parts) != 3:
        raise HTTPException(status_code=401, detail="Invalid biometric token format")

    try:
        token_admin = int(parts[0])
        issued_at = int(parts[1])
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=401, detail="Invalid biometric token payload") from exc

    if token_admin != admin_id:
        raise HTTPException(status_code=403, detail="Biometric token does not match admin identity")

    now_unix = int(datetime.utcnow().timestamp())
    if issued_at > now_unix + 5 or now_unix - issued_at > BIOMETRIC_TOKEN_TTL_SECONDS:
        raise HTTPException(status_code=401, detail="Biometric token expired")

    expected = biometric_signature(admin_id, action, issued_at)
    if not hmac.compare_digest(expected, parts[2]):
        raise HTTPException(status_code=401, detail="Biometric token signature mismatch")


def validate_invite_email(email: str) -> str:
    normalized = email.strip().lower()
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", normalized):
        raise HTTPException(status_code=422, detail="A valid CA invite email is required")
    return normalized


def validate_marketing_email(email: str) -> str:
    normalized = email.strip().lower()
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", normalized):
        raise HTTPException(status_code=422, detail="A valid email is required")
    return normalized


def issue_ca_invite_token(email: str, issued_by: int) -> str:
    seed = f"{email}:{issued_by}:{datetime.utcnow().isoformat(timespec='microseconds')}:{secrets.token_urlsafe(32)}"
    return sha256(seed.encode("utf-8")).hexdigest()


@app.post("/api/v1/marketing/signup")
def post_marketing_signup(payload: MarketingSignupIn) -> dict[str, Any]:
    normalized_name = payload.name.strip()
    if len(normalized_name) < 2:
        raise HTTPException(status_code=422, detail="Name must be at least 2 characters")

    normalized_email = validate_marketing_email(payload.email)
    provider = payload.provider.strip().upper()
    if provider not in {"EMAIL", "GOOGLE", "APPLE"}:
        raise HTTPException(status_code=422, detail="provider must be one of EMAIL, GOOGLE, APPLE")

    source = payload.source.strip().lower() or "public-signup"
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        conn.execute(
            """
            INSERT INTO marketing_signups(name, email, provider, source, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(email) DO UPDATE SET
                name = excluded.name,
                provider = excluded.provider,
                source = excluded.source,
                updated_at = excluded.updated_at
            """,
            (normalized_name, normalized_email, provider, source, now_iso, now_iso),
        )

        record = conn.execute(
            """
            SELECT id, created_at, updated_at
            FROM marketing_signups
            WHERE email = ?
            LIMIT 1
            """,
            (normalized_email,),
        ).fetchone()

        record_id = int(record["id"]) if record else 0
        log_audit(
            conn,
            table_name="marketing_signups",
            record_id=record_id,
            action="MARKETING_SIGNUP_CAPTURED",
            old_value=None,
            new_value={
                "name": normalized_name,
                "email": normalized_email,
                "provider": provider,
                "source": source,
            },
            user_id=0,
            high_priority=False,
        )
        conn.commit()

    return {
        "status": "captured",
        "id": record_id,
        "name": normalized_name,
        "email": normalized_email,
        "provider": provider,
        "source": source,
        "captured_at": now_iso,
    }


@app.post("/api/v1/auth/biometric-token")
def post_issue_biometric_token(
    action: str,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    normalized_action = action.strip().upper()
    if not re.fullmatch(r"[A-Z0-9_]{3,60}", normalized_action):
        raise HTTPException(status_code=422, detail="Invalid biometric action")
    token_data = issue_biometric_token(admin_id, normalized_action)
    return {
        "status": "OK",
        **token_data,
    }


@app.post("/api/v1/ca/invite")
def invite_ca(
    email: str,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin"})
    admin_id = require_admin_id(x_admin_id)
    normalized_email = validate_invite_email(email)
    token = issue_ca_invite_token(normalized_email, admin_id)
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    expires_at = datetime.utcnow() + timedelta(days=7)
    expires_at_iso = expires_at.isoformat(timespec="seconds") + "Z"
    invite_link = ""

    with closing(get_conn()) as conn:
        try:
            existing = conn.execute(
                """
                SELECT id, token, expires_at
                FROM ca_invites
                WHERE email = ?
                  AND status = 'PENDING'
                  AND expires_at >= ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (normalized_email, now_iso),
            ).fetchone()

            if existing is not None:
                invite_id = int(existing["id"])
                invite_token = str(existing["token"])
                invite_expires_at = str(existing["expires_at"])
            else:
                conn.execute(
                    """
                    INSERT INTO ca_invites(email, token, expires_at, status, created_by, created_at)
                    VALUES (?, ?, ?, 'PENDING', ?, ?)
                    """,
                    (normalized_email, token, expires_at_iso, admin_id, now_iso),
                )
                invite_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
                invite_token = token
                invite_expires_at = expires_at_iso

            invite_link = f"http://localhost:3000/ca/accept/{invite_token}"

            log_audit(
                conn,
                table_name="ca_invites",
                record_id=invite_id,
                action="CA_INVITE_CREATED",
                old_value=None,
                new_value={
                    "email": normalized_email,
                    "invite_token": invite_token,
                    "expires_at": invite_expires_at,
                    "actor_role": role,
                },
                user_id=admin_id,
                high_priority=True,
            )
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Unable to create CA invite: {exc}") from exc

    email_delivery = _send_ca_invite_email(to_email=normalized_email, invite_link=invite_link, expires_at=invite_expires_at)
    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        log_audit(
            conn,
            table_name="ca_invites",
            record_id=invite_id,
            action="CA_INVITE_EMAIL_DISPATCH",
            old_value=None,
            new_value={
                "email": normalized_email,
                "invite_token": invite_token,
                "invite_link": invite_link,
                "email_status": email_delivery["status"],
                "email_detail": email_delivery["detail"],
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=False,
        )
        conn.commit()

    return {
        "status": "success",
        "email": normalized_email,
        "invite_token": invite_token,
        "expires_at": invite_expires_at,
        "invite_link": invite_link,
        "email_delivery": email_delivery,
    }


@app.get("/api/v1/ca/verify-token/{token}")
def verify_ca_token(token: str) -> dict[str, Any]:
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    with closing(get_conn()) as conn:
        invite = conn.execute(
            """
            SELECT id, email, status, expires_at
            FROM ca_invites
            WHERE token = ?
            LIMIT 1
            """,
            (token,),
        ).fetchone()

        if invite is None:
            raise HTTPException(status_code=404, detail="Invite token not found")

        status = str(invite["status"])
        expires_at = str(invite["expires_at"])
        if status != "PENDING":
            raise HTTPException(status_code=400, detail=f"Invite is {status.lower()}")
        if expires_at < now_iso:
            conn.execute(
                "UPDATE ca_invites SET status = 'EXPIRED' WHERE id = ?",
                (int(invite["id"]),),
            )
            conn.commit()
            raise HTTPException(status_code=400, detail="Invalid or expired invite token")

        return {
            "status": "valid",
            "email": str(invite["email"]),
            "expires_at": expires_at,
        }


@app.post("/api/v1/ca/accept/{token}")
def accept_ca_token(token: str, payload: CAInviteAcceptIn) -> dict[str, Any]:
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    with closing(get_conn()) as conn:
        invite = conn.execute(
            """
            SELECT id, email, status, expires_at
            FROM ca_invites
            WHERE token = ?
            LIMIT 1
            """,
            (token,),
        ).fetchone()
        if invite is None:
            raise HTTPException(status_code=404, detail="Invite token not found")
        if str(invite["status"]) != "PENDING":
            raise HTTPException(status_code=400, detail=f"Invite is {str(invite['status']).lower()}")
        if str(invite["expires_at"]) < now_iso:
            conn.execute("UPDATE ca_invites SET status = 'EXPIRED' WHERE id = ?", (int(invite["id"]),))
            conn.commit()
            raise HTTPException(status_code=400, detail="Invite token expired")

        try:
            conn.execute(
                """
                UPDATE ca_invites
                SET status = 'ACCEPTED', accepted_by = ?, accepted_at = ?
                WHERE id = ?
                """,
                (payload.admin_id, now_iso, int(invite["id"])),
            )
            log_audit(
                conn,
                table_name="ca_invites",
                record_id=int(invite["id"]),
                action="CA_INVITE_ACCEPTED",
                old_value={"status": "PENDING"},
                new_value={
                    "status": "ACCEPTED",
                    "email": str(invite["email"]),
                    "accepted_by": payload.admin_id,
                    "accepted_at": now_iso,
                },
                user_id=payload.admin_id,
                high_priority=True,
            )
            conn.commit()
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to accept CA invite: {exc}") from exc

    return {
        "status": "accepted",
        "email": str(invite["email"]),
        "accepted_by": payload.admin_id,
        "accepted_at": now_iso,
    }


@app.get("/api/v1/ca/audit-summary")
def get_ca_audit_summary(
    hours: int = 168,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"ca", "admin"})
    require_admin_id(x_admin_id)

    safe_hours = min(max(hours, 1), 24 * 31)
    cutoff = (datetime.utcnow() - timedelta(hours=safe_hours)).isoformat(timespec="seconds") + "Z"

    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT a.id AS audit_id,
                   a.record_id AS entry_id,
                   a.created_at AS reversal_created_at,
                   a.new_value,
                   je.reference,
                   je.date,
                   je.counterparty_gstin,
                   je.filed_export_hash
            FROM audit_edit_logs a
            JOIN journal_entries je ON je.id = a.record_id
            WHERE a.table_name = 'journal_entries'
              AND a.action = 'RULE_37A_REVERSAL'
              AND a.created_at >= ?
            ORDER BY a.created_at DESC, a.id DESC
            """,
            (cutoff,),
        ).fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        payload = json.loads(row["new_value"]) if row["new_value"] else {}
        amount = money_str(payload.get("risk_amount", "0"))
        fallback_material = f"{row['entry_id']}|{row['reference']}|{row['date']}|{row['counterparty_gstin'] or ''}|{amount}"
        fingerprint = row["filed_export_hash"] or sha256(fallback_material.encode("utf-8")).hexdigest()
        items.append(
            {
                "id": int(row["audit_id"]),
                "entry_id": int(row["entry_id"]),
                "date": str(row["reversal_created_at"]),
                "ref": str(row["reference"]),
                "gstin": str(row["counterparty_gstin"] or "-"),
                "amount": amount,
                "fingerprint": str(fingerprint),
                "read_only": True,
            }
        )

    return {
        "status": "ok",
        "window_hours": safe_hours,
        "count": len(items),
        "entries": items,
    }


@app.get("/api/v1/ca/network-integrity")
def get_ca_network_integrity(
    limit: int = 100,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"ca", "admin"})
    require_admin_id(x_admin_id)
    safe_limit = min(max(limit, 1), 500)

    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT COALESCE(counterparty_gstin, '-') AS gstin,
                   COALESCE(vendor_legal_name, 'Unknown Vendor') AS vendor_name,
                   COUNT(*) AS total_entries,
                   SUM(CASE WHEN entry_fingerprint IS NOT NULL AND entry_fingerprint != '' THEN 1 ELSE 0 END) AS verified_entries,
                   MAX(created_at) AS last_activity
            FROM journal_entries
            WHERE status = 'POSTED'
            GROUP BY COALESCE(counterparty_gstin, '-'), COALESCE(vendor_legal_name, 'Unknown Vendor')
            ORDER BY total_entries DESC, last_activity DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

        accepted_clients = conn.execute(
            """
            SELECT COUNT(*) AS accepted_count
            FROM ca_invites
            WHERE status = 'ACCEPTED'
            """
        ).fetchone()

    tenants: list[dict[str, Any]] = []
    total_entries = 0
    total_verified = 0

    for row in rows:
        entries = int(row["total_entries"] or 0)
        verified = int(row["verified_entries"] or 0)
        score = round((verified / entries) * 100, 2) if entries > 0 else 100.0
        total_entries += entries
        total_verified += verified
        tenants.append(
            {
                "gstin": str(row["gstin"]),
                "vendor_name": str(row["vendor_name"]),
                "total_entries": entries,
                "verified_entries": verified,
                "integrity_score": score,
                "last_activity": str(row["last_activity"] or ""),
                "read_only": True,
                "hash_algorithm": "SHA-256",
            }
        )

    aggregate_score = round((total_verified / total_entries) * 100, 2) if total_entries > 0 else 100.0

    accepted_count = int(accepted_clients["accepted_count"]) if accepted_clients is not None else 0

    return {
        "status": "ok",
        "hash_algorithm": "SHA-256",
        "aggregate_integrity_score": aggregate_score,
        "tenants": tenants,
        "tenant_count": len(tenants),
        "accepted_clients": accepted_count,
    }


@app.get("/api/v1/ca/heatmap")
def get_ca_heatmap(
    limit: int = 120,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"ca", "admin"})
    require_admin_id(x_admin_id)
    safe_limit = min(max(limit, 1), 500)

    with closing(get_conn()) as conn:
        gstins = conn.execute(
            """
            SELECT DISTINCT counterparty_gstin
            FROM journal_entries
            WHERE counterparty_gstin IS NOT NULL
              AND TRIM(counterparty_gstin) != ''
            """
        ).fetchall()

        conn.execute("BEGIN")
        for row in gstins:
            recompute_vendor_trust(conn, str(row["counterparty_gstin"]))
        conn.commit()

        rows = conn.execute(
            """
            SELECT v.gstin,
                   COALESCE(v.legal_name, COALESCE(j.vendor_legal_name, 'Unknown Vendor')) AS vendor_name,
                   v.filing_consistency_score,
                   v.avg_filing_delay_days,
                   v.last_gstr1_filed_at,
                   v.total_itc_at_risk,
                     COALESCE(ae.open_alert_count, 0) AS open_alert_count,
                   MAX(j.created_at) AS last_activity,
                   COUNT(j.id) AS total_entries,
                   SUM(
                       CASE
                           WHEN j.vendor_gstr1_filed_at IS NOT NULL
                            AND j.vendor_gstr1_filed_at > (
                               CASE
                                   WHEN CAST(strftime('%m', j.date) AS INTEGER) = 12
                                     THEN printf('%04d-01-11', CAST(strftime('%Y', j.date) AS INTEGER) + 1)
                                   ELSE printf(
                                       '%04d-%02d-11',
                                       CAST(strftime('%Y', j.date) AS INTEGER),
                                       CAST(strftime('%m', j.date) AS INTEGER) + 1
                                   )
                               END
                            )
                           THEN 1
                           ELSE 0
                       END
                   ) AS high_risk_delay_count
            FROM vendor_trust_scores v
            LEFT JOIN journal_entries j ON j.counterparty_gstin = v.gstin
            LEFT JOIN (
                SELECT gstin, COUNT(*) AS open_alert_count
                FROM ca_alert_events
                WHERE status = 'OPEN'
                GROUP BY gstin
            ) ae ON ae.gstin = v.gstin
            GROUP BY v.gstin, v.legal_name, v.filing_consistency_score, v.avg_filing_delay_days, v.last_gstr1_filed_at, v.total_itc_at_risk, ae.open_alert_count
            ORDER BY v.filing_consistency_score ASC, CAST(v.total_itc_at_risk AS REAL) DESC, last_activity DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

        latest_market = conn.execute(
            """
            SELECT source_kind, analysis_json, created_at
            FROM market_trend_reports
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()

    market_context: dict[str, Any] = {
        "risk_level": "MEDIUM",
        "source_kind": "NONE",
        "created_at": "",
        "trend_summary": "No market trend reports available",
    }
    if latest_market is not None:
        parsed_market: dict[str, Any] = {}
        try:
            parsed = json.loads(str(latest_market["analysis_json"] or "{}"))
            parsed_market = parsed if isinstance(parsed, dict) else {}
        except Exception:  # noqa: BLE001
            parsed_market = {}
        market_context = {
            "risk_level": str(parsed_market.get("risk_level") or "MEDIUM").upper(),
            "source_kind": str(latest_market["source_kind"] or "UNKNOWN"),
            "created_at": str(latest_market["created_at"] or ""),
            "trend_summary": str(parsed_market.get("trend_summary") or "Market trend summary unavailable"),
        }

    cells: list[dict[str, Any]] = []
    risk_buckets = {"LOW": 0, "MEDIUM": 0, "HIGH": 0, "CRITICAL": 0}
    for row in rows:
        trust_score = float(row["filing_consistency_score"] or 0)
        delay_days = int(row["avg_filing_delay_days"] or 0)
        high_risk_delay_count = int(row["high_risk_delay_count"] or 0)
        itc_risk = money(row["total_itc_at_risk"])
        payment_advice = payment_advice_for_score(trust_score)

        risk_level = "LOW"
        risk_reasons: list[str] = []
        if trust_score < 35 or high_risk_delay_count >= 3 or itc_risk >= Decimal("250000"):
            risk_level = "CRITICAL"
            risk_reasons.append("Persistent filing inconsistency with elevated ITC exposure")
        elif trust_score < 55 or delay_days > 5 or itc_risk >= Decimal("100000"):
            risk_level = "HIGH"
            risk_reasons.append("High filing delay trend and elevated ITC at risk")
        elif trust_score < 75 or delay_days > 2 or itc_risk >= Decimal("25000"):
            risk_level = "MEDIUM"
            risk_reasons.append("Moderate compliance drift detected")

        if market_context["risk_level"] in {"HIGH", "CRITICAL"} and risk_level in {"MEDIUM", "HIGH"}:
            risk_reasons.append("Market-Intel volatility elevated risk posture")
            if risk_level == "MEDIUM":
                risk_level = "HIGH"

        if not risk_reasons:
            risk_reasons.append("Compliance trend stable")

        risk_buckets[risk_level] += 1
        cells.append(
            {
                "gstin": str(row["gstin"]),
                "vendor_name": str(row["vendor_name"] or "Unknown Vendor"),
                "risk_level": risk_level,
                "risk_reasons": risk_reasons,
                "trust_score": round(trust_score, 2),
                "avg_filing_delay_days": delay_days,
                "high_risk_delay_count": high_risk_delay_count,
                "total_itc_at_risk": money_str(itc_risk),
                "last_gstr1_filed_at": str(row["last_gstr1_filed_at"] or ""),
                "last_activity": str(row["last_activity"] or ""),
                "total_entries": int(row["total_entries"] or 0),
                "open_alert_count": int(row["open_alert_count"] or 0),
                "payment_advice": payment_advice,
            }
        )

    aggregate_risk = "LOW"
    if risk_buckets["CRITICAL"] > 0:
        aggregate_risk = "CRITICAL"
    elif risk_buckets["HIGH"] > 0:
        aggregate_risk = "HIGH"
    elif risk_buckets["MEDIUM"] > 0:
        aggregate_risk = "MEDIUM"

    return {
        "status": "ok",
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "count": len(cells),
        "open_alerts_total": sum(int(cell["open_alert_count"]) for cell in cells),
        "aggregate_risk": aggregate_risk,
        "risk_buckets": risk_buckets,
        "market_context": market_context,
        "cells": cells,
    }


def _default_ca_alert_rules() -> list[dict[str, Any]]:
    return [
        {
            "rule_key": "GST_RISK_CRITICAL",
            "display_name": "Critical GST Risk Escalation",
            "enabled": True,
            "min_trust_score": 55.0,
            "min_itc_risk": "100000.0000",
            "target_risk_levels": ["HIGH", "CRITICAL"],
            "channels": ["IN_APP", "EMAIL"],
        }
    ]


def _ensure_default_ca_alert_rules(conn: sqlite3.Connection, actor_id: int) -> None:
    count_row = conn.execute("SELECT COUNT(*) AS cnt FROM ca_alert_rules").fetchone()
    if int(count_row["cnt"] or 0) > 0:
        return
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    for rule in _default_ca_alert_rules():
        conn.execute(
            """
            INSERT INTO ca_alert_rules(
                rule_key,
                display_name,
                enabled,
                min_trust_score,
                min_itc_risk,
                target_risk_levels,
                channels_json,
                created_by,
                updated_by,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rule["rule_key"],
                rule["display_name"],
                1 if rule["enabled"] else 0,
                float(rule["min_trust_score"]),
                str(rule["min_itc_risk"]),
                json.dumps(rule["target_risk_levels"]),
                json.dumps(rule["channels"]),
                actor_id,
                actor_id,
                now_iso,
                now_iso,
            ),
        )


def _parse_json_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    try:
        payload = json.loads(raw)
        if isinstance(payload, list):
            return [str(item).strip() for item in payload if str(item).strip()]
    except Exception:  # noqa: BLE001
        pass
    return []


@app.get("/api/v1/ca/alerts/rules")
def get_ca_alert_rules(
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"ca", "admin"})
    admin_id = require_admin_id(x_admin_id)
    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        _ensure_default_ca_alert_rules(conn, admin_id)
        rows = conn.execute(
            """
            SELECT id,
                   rule_key,
                   display_name,
                   enabled,
                   min_trust_score,
                   min_itc_risk,
                   target_risk_levels,
                   channels_json,
                   created_by,
                   updated_by,
                   created_at,
                   updated_at
            FROM ca_alert_rules
            ORDER BY updated_at DESC, id DESC
            """
        ).fetchall()
        conn.commit()

    items = [
        {
            "id": int(row["id"]),
            "rule_key": str(row["rule_key"]),
            "display_name": str(row["display_name"]),
            "enabled": bool(row["enabled"]),
            "min_trust_score": float(row["min_trust_score"]),
            "min_itc_risk": str(row["min_itc_risk"]),
            "target_risk_levels": _parse_json_list(row["target_risk_levels"]),
            "channels": _parse_json_list(row["channels_json"]),
            "created_by": int(row["created_by"]),
            "updated_by": int(row["updated_by"]),
            "created_at": str(row["created_at"]),
            "updated_at": str(row["updated_at"]),
        }
        for row in rows
    ]
    return {
        "status": "ok",
        "count": len(items),
        "rules": items,
    }


@app.post("/api/v1/ca/alerts/rules")
def post_ca_alert_rule_upsert(
    payload: CAAlertRuleUpsertIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"ca", "admin"})
    admin_id = require_admin_id(x_admin_id)
    rule_key = re.sub(r"[^A-Z0-9_]+", "_", payload.rule_key.upper()).strip("_")
    if len(rule_key) < 3:
        raise HTTPException(status_code=422, detail="Invalid rule_key")

    normalized_levels = [str(level).strip().upper() for level in payload.target_risk_levels if str(level).strip()]
    normalized_levels = [level for level in normalized_levels if level in {"LOW", "MEDIUM", "HIGH", "CRITICAL"}]
    if not normalized_levels:
        normalized_levels = ["HIGH", "CRITICAL"]

    normalized_channels = [str(channel).strip().upper() for channel in payload.channels if str(channel).strip()]
    normalized_channels = [channel for channel in normalized_channels if channel in {"IN_APP", "EMAIL", "WEBHOOK"}]
    if not normalized_channels:
        normalized_channels = ["IN_APP"]

    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        conn.execute(
            """
            INSERT INTO ca_alert_rules(
                rule_key,
                display_name,
                enabled,
                min_trust_score,
                min_itc_risk,
                target_risk_levels,
                channels_json,
                created_by,
                updated_by,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(rule_key) DO UPDATE SET
                display_name = excluded.display_name,
                enabled = excluded.enabled,
                min_trust_score = excluded.min_trust_score,
                min_itc_risk = excluded.min_itc_risk,
                target_risk_levels = excluded.target_risk_levels,
                channels_json = excluded.channels_json,
                updated_by = excluded.updated_by,
                updated_at = excluded.updated_at
            """,
            (
                rule_key,
                payload.display_name.strip(),
                1 if payload.enabled else 0,
                float(payload.min_trust_score),
                money_str(payload.min_itc_risk),
                json.dumps(normalized_levels),
                json.dumps(normalized_channels),
                admin_id,
                admin_id,
                now_iso,
                now_iso,
            ),
        )
        row = conn.execute("SELECT id FROM ca_alert_rules WHERE rule_key = ?", (rule_key,)).fetchone()
        rule_id = int(row["id"]) if row else 0
        log_audit(
            conn,
            table_name="ca_alert_rules",
            record_id=rule_id,
            action="CA_ALERT_RULE_UPSERT",
            old_value=None,
            new_value={
                "rule_key": rule_key,
                "display_name": payload.display_name.strip(),
                "enabled": payload.enabled,
                "min_trust_score": float(payload.min_trust_score),
                "min_itc_risk": money_str(payload.min_itc_risk),
                "target_risk_levels": normalized_levels,
                "channels": normalized_channels,
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    return {
        "status": "ok",
        "rule_id": rule_id,
        "rule_key": rule_key,
        "updated_at": now_iso,
    }


def _insert_alert_event(
    *,
    conn: sqlite3.Connection,
    event_hash: str,
    rule_key: str,
    gstin: str,
    vendor_name: str,
    risk_level: str,
    severity: str,
    title: str,
    message: str,
    channels: list[str],
    event_source: str,
    metadata: dict[str, Any] | None,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO ca_alert_events(
            event_hash,
            rule_key,
            gstin,
            vendor_name,
            risk_level,
            severity,
            title,
            message,
            channels_json,
            status,
            event_source,
            metadata_json,
            acknowledged_by,
            acknowledged_at,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?, NULL, NULL, ?)
        """,
        (
            event_hash,
            rule_key,
            gstin,
            vendor_name,
            risk_level,
            severity,
            title,
            message,
            json.dumps(channels),
            event_source,
            json.dumps(metadata) if metadata is not None else None,
            datetime.utcnow().isoformat(timespec="seconds") + "Z",
        ),
    )
    return int(cursor.lastrowid)


@app.post("/api/v1/ca/alerts/evaluate")
def post_ca_alerts_evaluate(
    limit: int = 160,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"ca", "admin"})
    admin_id = require_admin_id(x_admin_id)
    safe_limit = min(max(limit, 1), 500)
    heatmap = get_ca_heatmap(limit=safe_limit, x_role=x_role, x_admin_id=x_admin_id)
    cells = heatmap.get("cells") if isinstance(heatmap, dict) else []
    cells = cells if isinstance(cells, list) else []

    created_events: list[dict[str, Any]] = []
    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        _ensure_default_ca_alert_rules(conn, admin_id)
        rules = conn.execute(
            """
            SELECT rule_key, display_name, enabled, min_trust_score, min_itc_risk, target_risk_levels, channels_json
            FROM ca_alert_rules
            WHERE enabled = 1
            ORDER BY updated_at DESC, id DESC
            """
        ).fetchall()

        for cell in cells:
            for rule in rules:
                risk_level = str(cell.get("risk_level") or "MEDIUM").upper()
                if risk_level not in set(_parse_json_list(rule["target_risk_levels"])):
                    continue

                trust_score = float(cell.get("trust_score") or 0)
                itc_risk = money(cell.get("total_itc_at_risk") or "0")
                min_trust_score = float(rule["min_trust_score"] or 100)
                min_itc_risk = money(rule["min_itc_risk"] or "0")
                trust_breached = trust_score <= min_trust_score
                itc_breached = itc_risk >= min_itc_risk
                if not (trust_breached or itc_breached):
                    continue

                rule_key = str(rule["rule_key"])
                gstin = str(cell.get("gstin") or "")
                vendor_name = str(cell.get("vendor_name") or "Unknown Vendor")
                dedupe_window = datetime.utcnow().strftime("%Y-%m-%d")
                event_hash = sha256(f"{rule_key}:{gstin}:{risk_level}:{dedupe_window}".encode("utf-8")).hexdigest()
                exists = conn.execute(
                                        """
                                        SELECT id
                                        FROM ca_alert_events
                                        WHERE event_hash = ?
                                            AND status IN ('OPEN', 'ACKNOWLEDGED')
                                        LIMIT 1
                                        """,
                                        (event_hash,),
                ).fetchone()
                if exists is not None:
                    continue

                # Keep dedupe strict for active alerts, while allowing a fresh cycle after CLOSED events.
                collision_idx = 0
                while conn.execute(
                    "SELECT id FROM ca_alert_events WHERE event_hash = ? LIMIT 1",
                    (event_hash,),
                ).fetchone() is not None:
                    collision_idx += 1
                    event_hash = sha256(f"{rule_key}:{gstin}:{risk_level}:{dedupe_window}:{collision_idx}".encode("utf-8")).hexdigest()

                severity = "CRITICAL" if risk_level == "CRITICAL" else "HIGH"
                title = f"{rule['display_name']}: {vendor_name}"
                message = (
                    f"{vendor_name} ({gstin}) flagged at {risk_level}. "
                    f"Trust {trust_score:.2f} and ITC risk INR {money_str(itc_risk)} exceeded rule thresholds."
                )
                channels = _parse_json_list(rule["channels_json"]) or ["IN_APP"]
                alert_id = _insert_alert_event(
                    conn=conn,
                    event_hash=event_hash,
                    rule_key=rule_key,
                    gstin=gstin,
                    vendor_name=vendor_name,
                    risk_level=risk_level,
                    severity=severity,
                    title=title,
                    message=message,
                    channels=channels,
                    event_source="AUTO_RULE_EVAL",
                    metadata={
                        "trust_score": trust_score,
                        "itc_risk": money_str(itc_risk),
                        "rule_key": rule_key,
                        "actor_role": role,
                    },
                )
                created_events.append(
                    {
                        "id": alert_id,
                        "gstin": gstin,
                        "vendor_name": vendor_name,
                        "risk_level": risk_level,
                        "rule_key": rule_key,
                    }
                )
                log_audit(
                    conn,
                    table_name="ca_alert_events",
                    record_id=alert_id,
                    action="CA_ALERT_AUTO_CREATED",
                    old_value=None,
                    new_value={
                        "rule_key": rule_key,
                        "gstin": gstin,
                        "risk_level": risk_level,
                        "actor_role": role,
                    },
                    user_id=admin_id,
                    high_priority=True,
                )

        conn.commit()

    return {
        "status": "ok",
        "evaluated_clients": len(cells),
        "created_alerts": len(created_events),
        "alerts": created_events[:30],
    }


@app.post("/api/v1/ca/alerts/manual")
def post_ca_alert_manual(
    payload: CAManualAlertIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"ca", "admin"})
    admin_id = require_admin_id(x_admin_id)
    gstin = validate_gstin(payload.gstin)
    risk_level = str(payload.risk_level).strip().upper()
    if risk_level not in {"LOW", "MEDIUM", "HIGH", "CRITICAL"}:
        raise HTTPException(status_code=422, detail="risk_level must be LOW, MEDIUM, HIGH, or CRITICAL")
    dedupe_seed = f"MANUAL:{gstin}:{payload.title.strip().upper()}:{datetime.utcnow().strftime('%Y-%m-%d')}"
    event_hash = sha256(dedupe_seed.encode("utf-8")).hexdigest()

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        exists = conn.execute("SELECT id FROM ca_alert_events WHERE event_hash = ? LIMIT 1", (event_hash,)).fetchone()
        if exists is not None:
            conn.rollback()
            raise HTTPException(status_code=409, detail="Manual alert already exists for this client/title today")

        alert_id = _insert_alert_event(
            conn=conn,
            event_hash=event_hash,
            rule_key="MANUAL",
            gstin=gstin,
            vendor_name=payload.vendor_name.strip(),
            risk_level=risk_level,
            severity="CRITICAL" if risk_level == "CRITICAL" else "HIGH",
            title=payload.title.strip(),
            message=payload.message.strip(),
            channels=["IN_APP"],
            event_source="MANUAL",
            metadata={"actor_role": role},
        )
        log_audit(
            conn,
            table_name="ca_alert_events",
            record_id=alert_id,
            action="CA_ALERT_MANUAL_CREATED",
            old_value=None,
            new_value={
                "gstin": gstin,
                "risk_level": risk_level,
                "title": payload.title.strip(),
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    return {
        "status": "ok",
        "alert_id": alert_id,
        "event_hash": event_hash,
    }


@app.get("/api/v1/ca/alerts")
def get_ca_alerts(
    status: str = "OPEN",
    limit: int = 100,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"ca", "admin"})
    require_admin_id(x_admin_id)
    normalized_status = status.strip().upper()
    if normalized_status not in {"OPEN", "ACKNOWLEDGED", "CLOSED", "ALL"}:
        raise HTTPException(status_code=422, detail="status must be OPEN, ACKNOWLEDGED, CLOSED, or ALL")
    safe_limit = min(max(limit, 1), 500)

    with closing(get_conn()) as conn:
        if normalized_status == "ALL":
            rows = conn.execute(
                """
                SELECT id, rule_key, gstin, vendor_name, risk_level, severity, title, message, channels_json, status,
                       event_source, metadata_json, acknowledged_by, acknowledged_at, created_at
                FROM ca_alert_events
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, rule_key, gstin, vendor_name, risk_level, severity, title, message, channels_json, status,
                       event_source, metadata_json, acknowledged_by, acknowledged_at, created_at
                FROM ca_alert_events
                WHERE status = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (normalized_status, safe_limit),
            ).fetchall()

    alerts = []
    for row in rows:
        metadata = {}
        if row["metadata_json"]:
            try:
                parsed = json.loads(str(row["metadata_json"]))
                metadata = parsed if isinstance(parsed, dict) else {}
            except Exception:  # noqa: BLE001
                metadata = {}
        alerts.append(
            {
                "id": int(row["id"]),
                "rule_key": str(row["rule_key"]),
                "gstin": str(row["gstin"]),
                "vendor_name": str(row["vendor_name"]),
                "risk_level": str(row["risk_level"]),
                "severity": str(row["severity"]),
                "title": str(row["title"]),
                "message": str(row["message"]),
                "channels": _parse_json_list(row["channels_json"]),
                "status": str(row["status"]),
                "event_source": str(row["event_source"]),
                "metadata": metadata,
                "acknowledged_by": int(row["acknowledged_by"]) if row["acknowledged_by"] is not None else None,
                "acknowledged_at": str(row["acknowledged_at"] or ""),
                "created_at": str(row["created_at"]),
            }
        )

    return {
        "status": "ok",
        "count": len(alerts),
        "alerts": alerts,
    }


@app.get("/api/v1/ca/events/token")
def get_ca_event_stream_token(
    ca_id: int,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"ca", "admin"})
    admin_id = require_admin_id(x_admin_id)
    if ca_id <= 0:
        raise HTTPException(status_code=422, detail="ca_id must be positive")

    # CA users can only mint stream tokens for themselves.
    if role == "ca" and admin_id != ca_id:
        raise HTTPException(status_code=403, detail="CA can only subscribe to own event stream")

    token = mint_sse_token(ca_id=ca_id)
    return {
        "status": "ok",
        "ca_id": ca_id,
        "token": token,
        "expires_in_seconds": max(60, SSE_TOKEN_TTL_SECONDS),
    }


@app.get("/api/v1/ca/events/stream")
async def get_ca_event_stream(request: Request, ca_id: int, token: str) -> StreamingResponse:
    if ca_id <= 0:
        raise HTTPException(status_code=422, detail="ca_id must be positive")
    if not verify_sse_token(token=token, ca_id=ca_id):
        raise HTTPException(status_code=403, detail="Invalid or expired stream token")

    token, queue = await ca_event_bus.subscribe(ca_id)

    async def event_generator():
        try:
            # Initial handshake for immediate client feedback.
            yield "event: connected\ndata: {\"status\":\"ok\",\"ca_id\":%d}\n\n" % ca_id
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event_payload = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"event: new_transaction\\ndata: {json.dumps(event_payload)}\\n\\n"
                except asyncio.TimeoutError:
                    # Keep connection alive for proxies and browser EventSource.
                    yield "event: heartbeat\ndata: {}\n\n"
        finally:
            await ca_event_bus.unsubscribe(token)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/v1/ca/alerts/{alert_id}/ack")
def post_ca_alert_acknowledge(
    alert_id: int,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"ca", "admin"})
    admin_id = require_admin_id(x_admin_id)
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        row = conn.execute("SELECT id, status FROM ca_alert_events WHERE id = ? LIMIT 1", (alert_id,)).fetchone()
        if row is None:
            conn.rollback()
            raise HTTPException(status_code=404, detail="Alert not found")
        conn.execute(
            """
            UPDATE ca_alert_events
            SET status = 'ACKNOWLEDGED',
                acknowledged_by = ?,
                acknowledged_at = ?
            WHERE id = ?
            """,
            (admin_id, now_iso, alert_id),
        )
        log_audit(
            conn,
            table_name="ca_alert_events",
            record_id=alert_id,
            action="CA_ALERT_ACKNOWLEDGED",
            old_value={"status": str(row["status"])},
            new_value={"status": "ACKNOWLEDGED", "actor_role": role},
            user_id=admin_id,
            high_priority=False,
        )
        conn.commit()
    return {
        "status": "ok",
        "alert_id": alert_id,
        "acknowledged_at": now_iso,
    }


@app.post("/api/v1/ca/playbooks/execute", dependencies=[Depends(rate_limit_heavy_task(seconds=10))])
async def post_ca_playbooks_execute(
    payload: CAPlaybookExecuteIn,
    background_tasks: BackgroundTasks,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"ca", "admin"})
    admin_id = require_admin_id(x_admin_id)
    safe_hold_hours = min(max(payload.hold_hours, 1), 720)
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    hold_until_iso = (datetime.utcnow() + timedelta(hours=safe_hold_hours)).isoformat(timespec="seconds") + "Z"

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        alert = conn.execute(
            """
            SELECT id, rule_key, gstin, vendor_name, risk_level, status, title, message
            FROM ca_alert_events
            WHERE id = ?
            LIMIT 1
            """,
            (payload.alert_id,),
        ).fetchone()
        if alert is None:
            conn.rollback()
            raise HTTPException(status_code=404, detail="Alert not found")
        if str(alert["status"]).upper() == "CLOSED":
            conn.rollback()
            raise HTTPException(status_code=409, detail="Alert already resolved")

        entry_row = conn.execute(
            """
            SELECT je.id,
                   je.reference,
                   je.counterparty_gstin,
                   COALESCE(je.vendor_legal_name, ?) AS vendor_name,
                   COALESCE(
                       (
                           SELECT SUM(CAST(tl.tax_amount AS REAL))
                           FROM tax_ledger tl
                           WHERE tl.entry_id = je.id
                       ),
                       0
                   ) AS tax_amount
            FROM journal_entries je
            WHERE je.counterparty_gstin = ?
              AND je.status = 'POSTED'
            ORDER BY je.date DESC, je.id DESC
            LIMIT 1
            """,
            (str(alert["vendor_name"]), str(alert["gstin"])),
        ).fetchone()
        if entry_row is None:
            conn.rollback()
            raise HTTPException(status_code=404, detail="No posted ledger entry found for alert GSTIN")

        invoice_amount = money_str(entry_row["tax_amount"])
        draft_subject = f"GST Action Required: {str(entry_row['reference'])}"
        draft_message = (
            f"Hi {str(entry_row['vendor_name'])}, Accord has placed a temporary payment hold on invoice "
            f"{str(entry_row['reference'])} (GSTIN {str(alert['gstin'])}) due to compliance risk. "
            "Please file/update GSTR-1 to unlock payment."
        )

        conn.execute(
            """
            INSERT INTO ca_payment_holds(
                alert_id,
                entry_id,
                gstin,
                vendor_name,
                hold_reason,
                hold_status,
                hold_until,
                nudge_subject,
                nudge_message,
                created_by,
                created_at,
                released_by,
                released_at
            ) VALUES (?, ?, ?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, NULL, NULL)
            ON CONFLICT(alert_id) DO UPDATE SET
                entry_id = excluded.entry_id,
                gstin = excluded.gstin,
                vendor_name = excluded.vendor_name,
                hold_reason = excluded.hold_reason,
                hold_status = 'OPEN',
                hold_until = excluded.hold_until,
                nudge_subject = excluded.nudge_subject,
                nudge_message = excluded.nudge_message,
                created_by = excluded.created_by,
                created_at = excluded.created_at,
                released_by = NULL,
                released_at = NULL
            """,
            (
                int(alert["id"]),
                int(entry_row["id"]),
                str(alert["gstin"]),
                str(entry_row["vendor_name"]),
                f"Playbook {payload.playbook_key.strip().upper()} triggered for {alert['risk_level']} risk",
                hold_until_iso,
                draft_subject,
                draft_message,
                admin_id,
                now_iso,
            ),
        )
        hold_row = conn.execute(
            "SELECT id FROM ca_payment_holds WHERE alert_id = ? LIMIT 1",
            (int(alert["id"]),),
        ).fetchone()
        hold_id = int(hold_row["id"]) if hold_row is not None else 0

        conn.execute(
            """
            UPDATE ca_alert_events
            SET status = 'CLOSED',
                acknowledged_by = COALESCE(acknowledged_by, ?),
                acknowledged_at = COALESCE(acknowledged_at, ?)
            WHERE id = ?
            """,
            (admin_id, now_iso, int(alert["id"])),
        )

        log_audit(
            conn,
            table_name="ca_payment_holds",
            record_id=hold_id,
            action="CA_PLAYBOOK_HOLD_APPLIED",
            old_value=None,
            new_value={
                "alert_id": int(alert["id"]),
                "entry_id": int(entry_row["id"]),
                "gstin": str(alert["gstin"]),
                "vendor_name": str(entry_row["vendor_name"]),
                "hold_until": hold_until_iso,
                "playbook_key": payload.playbook_key.strip().upper(),
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=True,
        )
        log_audit(
            conn,
            table_name="ca_alert_events",
            record_id=int(alert["id"]),
            action="CA_ALERT_RESOLVED_PLAYBOOK",
            old_value={"status": str(alert["status"])},
            new_value={
                "status": "CLOSED",
                "playbook_key": payload.playbook_key.strip().upper(),
                "hold_id": hold_id,
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    response = {
        "status": "ok",
        "playbook_key": payload.playbook_key.strip().upper(),
        "alert_id": int(alert["id"]),
        "alert_status": "CLOSED",
        "payment_hold": {
            "hold_id": hold_id,
            "entry_id": int(entry_row["id"]),
            "reference": str(entry_row["reference"]),
            "gstin": str(alert["gstin"]),
            "vendor_name": str(entry_row["vendor_name"]),
            "hold_until": hold_until_iso,
        },
        "nudge": {
            "subject": draft_subject,
            "urgency": "HIGH",
            "message": draft_message,
            "draft_status": "QUEUED",
        },
    }

    background_tasks.add_task(
        _refresh_playbook_nudge_async,
        alert_id=int(alert["id"]),
        entry_id=int(entry_row["id"]),
        gstin=str(alert["gstin"]),
        vendor_name=str(entry_row["vendor_name"]),
        invoice_reference=str(entry_row["reference"]),
        invoice_amount=invoice_amount,
        mismatch_reason=str(alert["message"]),
        actor_role=role,
        user_id=admin_id,
    )

    return response


@app.post("/api/v1/ca/demo/reset")
def post_ca_demo_reset(
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"ca", "admin"})
    require_admin_id(x_admin_id)

    statutory = ensure_statutory_service()
    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        result = reset_demo_environment(conn, statutory_service=statutory)
        conn.commit()

    return result


@app.post("/api/v1/ca/alerts/demo-seed")
def post_ca_alerts_demo_seed(
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"ca", "admin"})
    require_admin_id(x_admin_id)
    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        conn.execute(
            """
            INSERT INTO journal_entries(
                date,
                reference,
                description,
                company_state_code,
                counterparty_state_code,
                counterparty_gstin,
                eco_gstin,
                supply_source,
                ims_status,
                vendor_legal_name,
                vendor_gstr1_filed_at,
                status,
                reversal_of_id,
                is_filed,
                filed_at,
                filed_export_hash,
                approved_by_1,
                approved_by_2,
                voucher_type,
                currency_code,
                exchange_rate,
                created_at
            ) VALUES (?, ?, ?, NULL, NULL, ?, NULL, 'DIRECT', 'PENDING', ?, ?, 'POSTED', NULL, 0, NULL, NULL, NULL, NULL, 'JOURNAL', 'INR', '1.0000', ?)
            """,
            (
                date.today().isoformat(),
                f"DEMO/ALERT/{uuid.uuid4().hex[:10].upper()}",
                "CA alert demo seed transaction",
                "29ABCDE1234F1Z5",
                "Demo Risk Vendor",
                (date.today() + timedelta(days=12)).isoformat(),
                datetime.utcnow().isoformat(timespec="seconds") + "Z",
            ),
        )
        entry_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
        conn.execute(
            """
            INSERT INTO tax_ledger(entry_id, hsn_code, gst_rate_snapshot, taxable_value, tax_amount, supply_type, is_inter_state, supply_source, created_at)
            VALUES (?, '847100', '18.0000', '1800000.0000', '324000.0000', 'B2B', 0, 'DIRECT', ?)
            """,
            (entry_id, datetime.utcnow().isoformat(timespec="seconds") + "Z"),
        )
        conn.commit()

    return {
        "status": "ok",
        "seeded_entry_id": entry_id,
    }


@app.post("/api/v1/ca/verify-integrity")
async def post_ca_verify_integrity(
    limit: int = 800,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"ca", "admin"})
    require_admin_id(x_admin_id)
    safe_limit = min(max(limit, 1), 5000)

    with closing(get_conn()) as conn:
        entries = conn.execute(
            """
            SELECT id, reference, entry_fingerprint, created_at
            FROM journal_entries
            WHERE status = 'POSTED'
            ORDER BY id DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

        timeline: list[dict[str, Any]] = []
        mismatches: list[dict[str, Any]] = []

        for row in entries:
            lines = conn.execute(
                """
                SELECT jl.debit, jl.credit, a.name AS account_name
                FROM journal_lines jl
                JOIN accounts a ON a.id = jl.account_id
                WHERE jl.entry_id = ?
                ORDER BY jl.id ASC
                """,
                (int(row["id"]),),
            ).fetchall()

            expected = build_integrity_hash(str(row["reference"]), lines)
            stored = str(row["entry_fingerprint"] or "")
            verified = bool(stored) and stored == expected

            item = {
                "entry_id": int(row["id"]),
                "reference": str(row["reference"]),
                "stored_fingerprint": stored,
                "calculated_fingerprint": expected,
                "verified": verified,
                "created_at": str(row["created_at"] or ""),
            }
            timeline.append(item)
            if not verified:
                mismatches.append(item)

    total = len(timeline)
    verified_count = total - len(mismatches)
    integrity_score = round((verified_count / total) * 100, 2) if total > 0 else 100.0

    forensic_summary = "Mistral forensic summary unavailable"
    try:
        forensic_summary = await run_ollama_generate(
            model=FORENSIC_MODEL,
            prompt=(
                "You are Accord forensic verifier. Summarize ledger integrity verification in 2 lines for CA audit. "
                f"Total entries checked: {total}. Verified: {verified_count}. Mismatches: {len(mismatches)}."
            ),
        )
    except Exception:  # noqa: BLE001
        pass

    return {
        "status": "ok",
        "hash_algorithm": "SHA-256",
        "total_checked": total,
        "verified_count": verified_count,
        "mismatch_count": len(mismatches),
        "integrity_score": integrity_score,
        "timeline": timeline,
        "mismatches": mismatches,
        "forensic_summary": str(forensic_summary).strip(),
    }


@app.on_event("startup")
def startup() -> None:
    init_db()
    if ENABLE_TRACEMALLOC and not tracemalloc.is_tracing():
        tracemalloc.start(max(TRACEMALLOC_FRAMES, 5))
        api_logger.info("tracemalloc enabled with %s frames", max(TRACEMALLOC_FRAMES, 5))


@app.get("/api/v1/health")
def get_system_health() -> dict[str, Any]:
    db_exists = SQLITE_DB_PATH.exists()
    with closing(get_conn()) as conn:
        chain = verify_chain_of_trust(conn, limit=2000)
    return {
        "status": "ok" if chain["status"] == "ok" else "degraded",
        "service": "accord-backend",
        "database": {
            "requested_url": DATABASE_URL,
            "backend_runtime": "sqlite3",
            "configured_backend": DB_BACKEND,
            "sqlite_path": str(SQLITE_DB_PATH),
            "sqlite_exists": db_exists,
            "postgres_migration_status": (
                "PENDING_SQL_DIALECT_MIGRATION" if DB_BACKEND == "postgresql" else "NOT_REQUESTED"
            ),
        },
        "chain_of_trust": {
            "status": chain["status"],
            "cumulative_block_hash": chain["cumulative_block_hash"],
            "total_checked": chain["total_checked"],
            "mismatch_count": chain["mismatch_count"],
        },
        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


@app.get("/api/v1/system/deployment-info")
def get_deployment_info() -> dict[str, Any]:
    return {
        "status": "ok",
        "deployment_mode": DEPLOYMENT_MODE,
        "backend_public_url": BACKEND_PUBLIC_URL,
        "frontend_public_url": FRONTEND_PUBLIC_URL,
        "database_backend": DB_BACKEND,
        "postgres_runtime_status": "PENDING_SQL_DIALECT_MIGRATION" if DB_BACKEND == "postgresql" else "NOT_REQUESTED",
        "cors_allow_origins": cors_allow_origins,
        "cors_allow_origin_regex": cors_allow_origin_regex,
        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


@app.get("/api/v1/system/memory-profile")
def get_memory_profile(
    limit: int = 20,
    compare_with_previous: bool = False,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin"})
    _ = require_admin_id(x_admin_id)

    if not ENABLE_TRACEMALLOC:
        raise HTTPException(
            status_code=503,
            detail="tracemalloc is disabled. Set ACCORD_ENABLE_TRACEMALLOC=1 and restart backend.",
        )
    if not tracemalloc.is_tracing():
        tracemalloc.start(max(TRACEMALLOC_FRAMES, 5))

    safe_limit = min(max(limit, 5), 100)
    global TRACEMALLOC_LAST_SNAPSHOT
    current = tracemalloc.take_snapshot()

    if compare_with_previous and TRACEMALLOC_LAST_SNAPSHOT is not None:
        stats = current.compare_to(TRACEMALLOC_LAST_SNAPSHOT, "lineno")
        top_items = []
        for idx, stat in enumerate(stats[:safe_limit], start=1):
            frame = stat.traceback[0] if stat.traceback else None
            top_items.append(
                {
                    "rank": idx,
                    "file": frame.filename if frame else "unknown",
                    "line": frame.lineno if frame else 0,
                    "size_diff_bytes": int(stat.size_diff),
                    "size_diff_kb": round(float(stat.size_diff) / 1024.0, 3),
                    "count_diff": int(stat.count_diff),
                }
            )
        mode = "diff"
    else:
        stats = current.statistics("lineno")
        top_items = []
        for idx, stat in enumerate(stats[:safe_limit], start=1):
            frame = stat.traceback[0] if stat.traceback else None
            top_items.append(
                {
                    "rank": idx,
                    "file": frame.filename if frame else "unknown",
                    "line": frame.lineno if frame else 0,
                    "size_bytes": int(stat.size),
                    "size_kb": round(float(stat.size) / 1024.0, 3),
                    "count": int(stat.count),
                }
            )
        mode = "absolute"

    current_mem, peak_mem = tracemalloc.get_traced_memory()
    TRACEMALLOC_LAST_SNAPSHOT = current

    return {
        "status": "ok",
        "mode": mode,
        "limit": safe_limit,
        "trace_frames": max(TRACEMALLOC_FRAMES, 5),
        "traced_memory_current_bytes": int(current_mem),
        "traced_memory_peak_bytes": int(peak_mem),
        "items": top_items,
        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


@app.post("/api/v1/studio/save-template")
def post_studio_save_template(
    payload: StudioTemplateSaveIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)

    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    normalized_name = payload.name.strip()
    template_type = payload.template_type.strip().lower() or "dashboard"
    layout_json = json.dumps(payload.layout, sort_keys=True)
    blocks_json = json.dumps(payload.blocks, sort_keys=True)

    pdf_bytes = build_studio_template_pdf(
        template_name=normalized_name,
        template_type=template_type,
        layout_json=layout_json,
        blocks_json=blocks_json,
        created_by=admin_id,
    )
    pdf_fingerprint = sha256(pdf_bytes).hexdigest()
    safe_template_name = re.sub(r"[^A-Za-z0-9._-]+", "-", normalized_name).strip("-") or "stark-template"
    STUDIO_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    pdf_name = f"StarkStudio_{safe_template_name}_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.pdf"
    pdf_path = STUDIO_EXPORT_DIR / pdf_name
    pdf_path.write_bytes(pdf_bytes)

    template_id = 0
    export_id = 0

    if DB_BACKEND == "postgresql" and SQLA_ENGINE is not None and text is not None:
        ensure_studio_schema_postgres()
        with SQLA_ENGINE.begin() as conn:
            created = conn.execute(
                text(
                    """
                    INSERT INTO studio_templates(name, template_type, layout_json, blocks_json, created_by, created_at, updated_at)
                    VALUES (:name, :template_type, :layout_json, :blocks_json, :created_by, :created_at, :updated_at)
                    RETURNING id
                    """
                ),
                {
                    "name": normalized_name,
                    "template_type": template_type,
                    "layout_json": layout_json,
                    "blocks_json": blocks_json,
                    "created_by": admin_id,
                    "created_at": now_iso,
                    "updated_at": now_iso,
                },
            ).fetchone()
            template_id = int(created[0]) if created is not None else 0

            exported = conn.execute(
                text(
                    """
                    INSERT INTO studio_template_exports(template_id, pdf_path, pdf_fingerprint, created_at)
                    VALUES (:template_id, :pdf_path, :pdf_fingerprint, :created_at)
                    RETURNING id
                    """
                ),
                {
                    "template_id": template_id,
                    "pdf_path": str(pdf_path),
                    "pdf_fingerprint": pdf_fingerprint,
                    "created_at": now_iso,
                },
            ).fetchone()
            export_id = int(exported[0]) if exported is not None else 0
    else:
        with closing(get_conn()) as conn:
            ensure_studio_schema_sqlite(conn)
            conn.execute("BEGIN")
            cursor = conn.execute(
                """
                INSERT INTO studio_templates(name, template_type, layout_json, blocks_json, created_by, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (normalized_name, template_type, layout_json, blocks_json, admin_id, now_iso, now_iso),
            )
            template_id = int(cursor.lastrowid)
            export_cursor = conn.execute(
                """
                INSERT INTO studio_template_exports(template_id, pdf_path, pdf_fingerprint, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (template_id, str(pdf_path), pdf_fingerprint, now_iso),
            )
            export_id = int(export_cursor.lastrowid)
            log_audit(
                conn,
                table_name="studio_templates",
                record_id=template_id,
                action="STARK_STUDIO_TEMPLATE_SAVED",
                old_value=None,
                new_value={
                    "name": normalized_name,
                    "template_type": template_type,
                    "pdf_path": str(pdf_path),
                    "pdf_fingerprint": pdf_fingerprint,
                },
                user_id=admin_id,
                high_priority=True,
            )
            conn.commit()

    return {
        "status": "saved",
        "template_id": template_id,
        "export_id": export_id,
        "name": normalized_name,
        "template_type": template_type,
        "pdf_path": str(pdf_path),
        "pdf_fingerprint": pdf_fingerprint,
        "saved_at": now_iso,
        "storage_backend": "postgresql" if (DB_BACKEND == "postgresql" and SQLA_ENGINE is not None) else "sqlite",
    }


@app.post("/api/v1/ledger/verify-integrity")
def post_ledger_verify_integrity(
    limit: int = 8000,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin", "ca"})
    require_admin_id(x_admin_id)
    with closing(get_conn()) as conn:
        return verify_chain_of_trust(conn, limit=limit)


@app.get("/api/v1/ledger/currencies")
def get_ledger_currencies() -> dict[str, Any]:
    service = ensure_currency_service()
    return {
        "status": "ok",
        **service.rates_payload(),
    }


@app.post("/api/v1/ledger/revalue")
def post_ledger_revalue(
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    service = ensure_currency_service()

    as_of = datetime.utcnow()
    impacted: list[dict[str, Any]] = []
    total_delta = Decimal("0")

    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT id, reference, currency_code, exchange_rate
            FROM journal_entries
            WHERE status = 'POSTED'
              AND COALESCE(currency_code, 'INR') != 'INR'
            ORDER BY id ASC
            """
        ).fetchall()

        for row in rows:
            currency_code = normalize_currency_code(str(row["currency_code"] or "INR"))
            booked_rate = parse_amount_from_text(str(row["exchange_rate"] or "1"))
            if booked_rate <= 0:
                booked_rate = Decimal("1")

            sums = conn.execute(
                """
                SELECT
                    COALESCE(SUM(CAST(debit AS REAL)), 0) AS debit_sum,
                    COALESCE(SUM(CAST(credit AS REAL)), 0) AS credit_sum
                FROM journal_lines
                WHERE entry_id = ?
                """,
                (int(row["id"]),),
            ).fetchone()

            book_amount_base = money(str((sums["debit_sum"] if sums is not None else 0) or 0))
            if book_amount_base <= 0:
                book_amount_base = money(str((sums["credit_sum"] if sums is not None else 0) or 0))
            if book_amount_base <= 0:
                continue

            foreign_amount = money(book_amount_base / booked_rate)
            current_rate = money(service.get_rate(currency_code, as_of=as_of))
            delta = service.calculate_unrealized_gain_loss(
                book_amount_base=book_amount_base,
                current_rate=current_rate,
                foreign_amount=foreign_amount,
            )
            if abs(delta) < Decimal("0.0001"):
                continue

            total_delta += delta
            impacted.append(
                {
                    "entry_id": int(row["id"]),
                    "reference": str(row["reference"]),
                    "currency_code": currency_code,
                    "booked_rate": money_str(booked_rate),
                    "current_rate": money_str(current_rate),
                    "book_amount_base": money_str(book_amount_base),
                    "foreign_amount": money_str(foreign_amount),
                    "delta_base": money_str(delta),
                }
            )

        reval_entry_id = None
        reval_reference = None
        if impacted and total_delta != 0:
            posting_date = as_of.date()
            reference = next_journal_reference(conn, posting_date)
            created_at = as_of.isoformat(timespec="seconds") + "Z"
            amount_abs = money(abs(total_delta))

            if total_delta > 0:
                debit_name = "Accounts Receivable"
                credit_name = "Sales Revenue"
                desc = "FX Revaluation Gain"
            else:
                debit_name = "Operating Expenses"
                credit_name = "Accounts Receivable"
                desc = "FX Revaluation Loss"

            debit_id = get_account_id_by_name(conn, debit_name)
            credit_id = get_account_id_by_name(conn, credit_name)

            conn.execute("BEGIN")
            conn.execute(
                """
                INSERT INTO journal_entries(
                    date,
                    reference,
                    description,
                    company_state_code,
                    counterparty_state_code,
                    counterparty_gstin,
                    eco_gstin,
                    supply_source,
                    ims_status,
                    vendor_legal_name,
                    vendor_gstr1_filed_at,
                    status,
                    reversal_of_id,
                    is_filed,
                    filed_at,
                    filed_export_hash,
                    approved_by_1,
                    approved_by_2,
                    voucher_type,
                    currency_code,
                    exchange_rate,
                    created_at
                ) VALUES (?, ?, ?, NULL, NULL, NULL, NULL, 'DIRECT', 'PENDING', ?, NULL, 'POSTED', NULL, 0, NULL, NULL, NULL, NULL, 'JOURNAL', 'INR', '1.0000', ?)
                """,
                (
                    posting_date.isoformat(),
                    reference,
                    f"{desc}: {len(impacted)} entries revalued",
                    "SOVEREIGN_FX_ENGINE",
                    created_at,
                ),
            )
            reval_entry_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
            reval_reference = reference

            conn.execute(
                "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
                (reval_entry_id, debit_id, money_str(amount_abs), "0.0000"),
            )
            update_account_balance(conn, debit_id, amount_abs, Decimal("0"))

            conn.execute(
                "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
                (reval_entry_id, credit_id, "0.0000", money_str(amount_abs)),
            )
            update_account_balance(conn, credit_id, Decimal("0"), amount_abs)
            reval_fingerprint = stamp_entry_fingerprint(conn, reval_entry_id)

            log_audit(
                conn,
                table_name="journal_entries",
                record_id=reval_entry_id,
                action="FX_REVALUATION_POSTED",
                old_value=None,
                new_value={
                    "reference": reval_reference,
                    "entries_impacted": len(impacted),
                    "total_delta_base": money_str(total_delta),
                    "entry_fingerprint": reval_fingerprint,
                    "actor_role": role,
                },
                user_id=admin_id,
                high_priority=True,
            )
            conn.commit()

    return {
        "status": "ok",
        "engine": "SOVEREIGN_FX_ENGINE",
        "base_currency": service.BASE_CURRENCY,
        "as_of": as_of.isoformat(timespec="seconds") + "Z",
        "entries_scanned": len(impacted),
        "revaluation_delta_base": money_str(total_delta),
        "revaluation_entry_id": reval_entry_id,
        "revaluation_reference": reval_reference,
        "impacted_entries": impacted[:200],
    }


RERA_BOOKING_ALLOWED_STATUS = {"ACTIVE", "CANCELLED", "CLOSED"}


def normalize_rera_booking_status(status: str) -> str:
    normalized = status.strip().upper()
    if normalized not in RERA_BOOKING_ALLOWED_STATUS:
        raise HTTPException(status_code=422, detail="status must be ACTIVE, CANCELLED, or CLOSED")
    return normalized


def serialize_rera_booking_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "booking_id": row["booking_id"],
        "project_id": row["project_id"],
        "customer_name": row["customer_name"],
        "unit_code": row["unit_code"],
        "status": row["status"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def compute_rera_idempotency_request_hash(
    *,
    booking_id: str,
    payment_reference: str,
    event_type: str,
    receipt_amount: Decimal,
    override_rera_ratio: Decimal | None,
    override_reason: str | None,
) -> str:
    receipt_amount_text = f"{Decimal(str(receipt_amount)).quantize(Decimal('0.01')):.2f}"
    ratio_text = ""
    if override_rera_ratio is not None:
        ratio_text = f"{Decimal(str(override_rera_ratio)).quantize(Decimal('0.0001')):.4f}"
    reason_text = (override_reason or "").strip()
    basis = "|".join(
        [
            booking_id,
            payment_reference,
            event_type,
            receipt_amount_text,
            ratio_text,
            reason_text,
        ]
    )
    return sha256(basis.encode("utf-8")).hexdigest()


def fetch_rera_allocation_event_row(conn: sqlite3.Connection, event_id: int) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT id, booking_id, payment_reference, event_type, receipt_amount,
               applied_rera_ratio, rera_amount, operations_amount,
               is_override, override_reason, actor_role, status, created_at
        FROM rera_allocation_events
        WHERE id = ?
        """,
        (event_id,),
    ).fetchone()


def build_rera_allocation_response(
    *,
    row: sqlite3.Row,
    actor_role: str,
    actor_admin_id: int,
    idempotency_key: str | None,
    idempotency_replayed: bool,
) -> dict[str, Any]:
    receipt_amount = Decimal(str(row["receipt_amount"]))
    if str(row["event_type"]).upper() == "REFUND":
        receipt_amount = receipt_amount * Decimal("-1")

    payload: dict[str, Any] = {
        "status": "ok",
        "event_id": int(row["id"]),
        "booking_id": row["booking_id"],
        "payment_reference": row["payment_reference"],
        "event_type": row["event_type"],
        "allocation": {
            "receipt_amount": f"{receipt_amount:.2f}",
            "applied_rera_ratio": row["applied_rera_ratio"],
            "rera_amount": row["rera_amount"],
            "operations_amount": row["operations_amount"],
            "is_override": bool(row["is_override"]),
        },
        "audit": {
            "override_reason": row["override_reason"],
            "actor_role": actor_role,
            "actor_admin_id": actor_admin_id,
            "created_at": row["created_at"],
        },
    }
    if idempotency_key:
        payload["idempotency"] = {
            "key": idempotency_key,
            "replayed": idempotency_replayed,
        }
    return payload


@app.post("/api/v1/rera/bookings")
def post_rera_booking(
    payload: ReraBookingCreateIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin", "ca"})
    require_admin_id(x_admin_id)

    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    booking_id = payload.booking_id.strip()
    project_id = payload.project_id.strip()
    status = normalize_rera_booking_status(payload.status)
    customer_name = (payload.customer_name or "").strip() or None
    unit_code = (payload.unit_code or "").strip() or None

    service = ensure_rera_allocation_service()
    with closing(get_conn()) as conn:
        service.ensure_schema(conn)
        try:
            conn.execute(
                """
                INSERT INTO sales_bookings(booking_id, project_id, customer_name, unit_code, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (booking_id, project_id, customer_name, unit_code, status, now_iso, now_iso),
            )
            conn.commit()
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail="booking_id already exists") from exc

        row = conn.execute(
            """
            SELECT booking_id, project_id, customer_name, unit_code, status, created_at, updated_at
            FROM sales_bookings
            WHERE booking_id = ?
            """,
            (booking_id,),
        ).fetchone()

    if row is None:
        raise HTTPException(status_code=500, detail="booking created but not found")

    return {
        "status": "ok",
        "booking": serialize_rera_booking_row(row),
    }


@app.get("/api/v1/rera/bookings")
def get_rera_bookings(
    status: str | None = None,
    limit: int = 100,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin", "ca"})
    require_admin_id(x_admin_id)

    if limit < 1 or limit > 500:
        raise HTTPException(status_code=422, detail="limit must be between 1 and 500")

    status_filter = None
    if status is not None and status.strip() != "":
        status_filter = normalize_rera_booking_status(status)

    service = ensure_rera_allocation_service()
    with closing(get_conn()) as conn:
        service.ensure_schema(conn)
        if status_filter is None:
            rows = conn.execute(
                """
                SELECT booking_id, project_id, customer_name, unit_code, status, created_at, updated_at
                FROM sales_bookings
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT booking_id, project_id, customer_name, unit_code, status, created_at, updated_at
                FROM sales_bookings
                WHERE status = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (status_filter, limit),
            ).fetchall()

    return {
        "status": "ok",
        "count": len(rows),
        "limit": limit,
        "status_filter": status_filter,
        "items": [serialize_rera_booking_row(row) for row in rows],
    }


@app.get("/api/v1/rera/bookings/{booking_id}")
def get_rera_booking_by_id(
    booking_id: str,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin", "ca"})
    require_admin_id(x_admin_id)

    booking_key = booking_id.strip()
    service = ensure_rera_allocation_service()
    with closing(get_conn()) as conn:
        service.ensure_schema(conn)
        row = conn.execute(
            """
            SELECT booking_id, project_id, customer_name, unit_code, status, created_at, updated_at
            FROM sales_bookings
            WHERE booking_id = ?
            """,
            (booking_key,),
        ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="booking_id not found")

    return {
        "status": "ok",
        "booking": serialize_rera_booking_row(row),
    }


@app.put("/api/v1/rera/bookings/{booking_id}")
def put_rera_booking(
    booking_id: str,
    payload: ReraBookingUpdateIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin", "ca"})
    require_admin_id(x_admin_id)

    updates: dict[str, Any] = {}
    if payload.project_id is not None:
        updates["project_id"] = payload.project_id.strip()
    if payload.customer_name is not None:
        updates["customer_name"] = payload.customer_name.strip() or None
    if payload.unit_code is not None:
        updates["unit_code"] = payload.unit_code.strip() or None
    if payload.status is not None:
        updates["status"] = normalize_rera_booking_status(payload.status)

    if not updates:
        raise HTTPException(status_code=422, detail="at least one field must be provided for update")

    updates["updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    booking_key = booking_id.strip()

    service = ensure_rera_allocation_service()
    with closing(get_conn()) as conn:
        service.ensure_schema(conn)
        current = conn.execute(
            "SELECT booking_id FROM sales_bookings WHERE booking_id = ?",
            (booking_key,),
        ).fetchone()
        if current is None:
            raise HTTPException(status_code=404, detail="booking_id not found")

        set_clause = ", ".join(f"{col} = ?" for col in updates.keys())
        values = list(updates.values()) + [booking_key]
        conn.execute(f"UPDATE sales_bookings SET {set_clause} WHERE booking_id = ?", values)
        conn.commit()

        row = conn.execute(
            """
            SELECT booking_id, project_id, customer_name, unit_code, status, created_at, updated_at
            FROM sales_bookings
            WHERE booking_id = ?
            """,
            (booking_key,),
        ).fetchone()

    if row is None:
        raise HTTPException(status_code=500, detail="booking updated but not found")

    return {
        "status": "ok",
        "booking": serialize_rera_booking_row(row),
    }


@app.delete("/api/v1/rera/bookings/{booking_id}")
def delete_rera_booking(
    booking_id: str,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin", "ca"})
    require_admin_id(x_admin_id)

    booking_key = booking_id.strip()
    service = ensure_rera_allocation_service()
    with closing(get_conn()) as conn:
        service.ensure_schema(conn)
        row = conn.execute(
            "SELECT booking_id FROM sales_bookings WHERE booking_id = ?",
            (booking_key,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="booking_id not found")

        try:
            conn.execute("DELETE FROM sales_bookings WHERE booking_id = ?", (booking_key,))
            conn.commit()
        except sqlite3.IntegrityError as exc:
            raise HTTPException(
                status_code=409,
                detail="booking has linked allocation events and cannot be deleted",
            ) from exc

    return {
        "status": "ok",
        "booking_id": booking_key,
        "deleted": True,
    }


@app.post("/api/v1/users/device-token")
def post_user_device_token(
    payload: UserDeviceTokenUpsertIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin", "ca", "ops"})
    admin_id = require_admin_id(x_admin_id)

    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    normalized_platform = payload.platform.strip().lower() or "unknown"
    effective_user_id = int(payload.user_id or admin_id)
    token_value = payload.device_token.strip()

    with closing(get_conn()) as conn:
        conn.row_factory = sqlite3.Row
        ensure_user_device_token_schema(conn)

        existing = conn.execute(
            "SELECT id FROM user_device_tokens WHERE device_token = ?",
            (token_value,),
        ).fetchone()

        if existing is None:
            cursor = conn.execute(
                """
                INSERT INTO user_device_tokens(
                    user_id, device_token, platform, push_provider, app_version,
                    is_active, created_at, updated_at, last_seen_at
                ) VALUES (?, ?, ?, 'FCM', ?, 1, ?, ?, ?)
                """,
                (
                    effective_user_id,
                    token_value,
                    normalized_platform,
                    (payload.app_version or "").strip() or None,
                    now_iso,
                    now_iso,
                    now_iso,
                ),
            )
            token_id = int(cursor.lastrowid)
            was_created = True
        else:
            token_id = int(existing["id"])
            conn.execute(
                """
                UPDATE user_device_tokens
                SET user_id = ?,
                    platform = ?,
                    push_provider = 'FCM',
                    app_version = ?,
                    is_active = 1,
                    updated_at = ?,
                    last_seen_at = ?
                WHERE id = ?
                """,
                (
                    effective_user_id,
                    normalized_platform,
                    (payload.app_version or "").strip() or None,
                    now_iso,
                    now_iso,
                    token_id,
                ),
            )
            was_created = False

        conn.commit()

    return {
        "status": "ok",
        "token_id": token_id,
        "user_id": effective_user_id,
        "platform": normalized_platform,
        "provider": "FCM",
        "created": was_created,
    }


@app.post("/api/v1/rera/allocations")
def post_rera_allocation(
    payload: ReraAllocationRequestIn,
    request: Request,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    x_idempotency_key: str | None = Header(default=None, alias="X-Idempotency-Key"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)

    service = ensure_rera_allocation_service()
    booking_id = payload.booking_id.strip()
    payment_reference = payload.payment_reference.strip()
    event_type = payload.event_type.strip().upper()

    if event_type not in {"PAYMENT", "REFUND"}:
        raise HTTPException(status_code=422, detail="event_type must be PAYMENT or REFUND")

    idempotency_key = (x_idempotency_key or request.headers.get("Idempotency-Key") or "").strip() or None
    if idempotency_key is not None and (len(idempotency_key) < 8 or len(idempotency_key) > 120):
        raise HTTPException(status_code=422, detail="idempotency key length must be between 8 and 120")

    request_hash = compute_rera_idempotency_request_hash(
        booking_id=booking_id,
        payment_reference=payment_reference,
        event_type=event_type,
        receipt_amount=payload.receipt_amount,
        override_rera_ratio=payload.override_rera_ratio,
        override_reason=payload.override_reason,
    )

    idempotency_reserved = False
    if idempotency_key:
        now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        with closing(get_conn()) as conn:
            service.ensure_schema(conn)
            existing = conn.execute(
                """
                SELECT request_hash, allocation_event_id
                FROM rera_allocation_idempotency
                WHERE idempotency_key = ?
                """,
                (idempotency_key,),
            ).fetchone()

            if existing is not None:
                if str(existing["request_hash"]) != request_hash:
                    raise HTTPException(
                        status_code=409,
                        detail="idempotency key already used with a different request payload",
                    )
                existing_event_id = existing["allocation_event_id"]
                if existing_event_id is None:
                    raise HTTPException(status_code=409, detail="idempotent request is currently being processed")
                event_row = fetch_rera_allocation_event_row(conn, int(existing_event_id))
                if event_row is None:
                    raise HTTPException(status_code=409, detail="idempotency record exists but event is missing")
                return build_rera_allocation_response(
                    row=event_row,
                    actor_role=role.upper(),
                    actor_admin_id=admin_id,
                    idempotency_key=idempotency_key,
                    idempotency_replayed=True,
                )

            conn.execute(
                """
                INSERT INTO rera_allocation_idempotency(
                    idempotency_key, request_hash, allocation_event_id, created_at, updated_at
                ) VALUES (?, ?, NULL, ?, ?)
                """,
                (idempotency_key, request_hash, now_iso, now_iso),
            )
            conn.commit()
            idempotency_reserved = True

    try:
        result = service.allocate(
            AllocationInput(
                booking_id=booking_id,
                payment_reference=payment_reference,
                event_type=event_type,
                receipt_amount=payload.receipt_amount,
                override_rera_ratio=payload.override_rera_ratio,
                override_reason=(payload.override_reason or "").strip() or None,
                actor_role=role.upper(),
            )
        )
    except ValueError as exc:
        if idempotency_key and idempotency_reserved:
            with closing(get_conn()) as conn:
                service.ensure_schema(conn)
                conn.execute(
                    """
                    DELETE FROM rera_allocation_idempotency
                    WHERE idempotency_key = ? AND request_hash = ? AND allocation_event_id IS NULL
                    """,
                    (idempotency_key, request_hash),
                )
                conn.commit()
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except sqlite3.IntegrityError as exc:
        if idempotency_key and idempotency_reserved:
            with closing(get_conn()) as conn:
                service.ensure_schema(conn)
                conn.execute(
                    """
                    DELETE FROM rera_allocation_idempotency
                    WHERE idempotency_key = ? AND request_hash = ? AND allocation_event_id IS NULL
                    """,
                    (idempotency_key, request_hash),
                )
                conn.commit()
        err = str(exc).lower()
        if "foreign key" in err:
            raise HTTPException(status_code=404, detail="booking_id not found in sales_bookings") from exc
        if "unique" in err:
            raise HTTPException(
                status_code=409,
                detail="allocation already exists for booking_id + payment_reference + event_type",
            ) from exc
        raise HTTPException(status_code=400, detail=f"allocation failed: {exc}") from exc

    with closing(get_conn()) as conn:
        service.ensure_schema(conn)
        row = fetch_rera_allocation_event_row(conn, int(result.event_id))
        approval_status = initialize_allocation_approval(
            conn,
            allocation_event_id=int(result.event_id),
            maker_admin_id=admin_id,
            receipt_amount=payload.receipt_amount,
        )
        if idempotency_key:
            now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            conn.execute(
                """
                UPDATE rera_allocation_idempotency
                SET allocation_event_id = ?, updated_at = ?
                WHERE idempotency_key = ? AND request_hash = ?
                """,
                (int(result.event_id), now_iso, idempotency_key, request_hash),
            )
            conn.commit()

    if row is None:
        raise HTTPException(status_code=500, detail="allocation created but event row not found")

    released_commissions = ensure_commission_service().release_commissions_for_booking(booking_id)

    response = build_rera_allocation_response(
        row=row,
        actor_role=role.upper(),
        actor_admin_id=admin_id,
        idempotency_key=idempotency_key,
        idempotency_replayed=False,
    )
    response["approval_status"] = approval_status
    response["broker_commissions_released"] = released_commissions
    return response


@app.get("/api/v1/rera/allocations")
def get_rera_allocations(
    booking_id: str | None = None,
    limit: int = 100,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin", "ca"})
    require_admin_id(x_admin_id)

    if limit < 1 or limit > 500:
        raise HTTPException(status_code=422, detail="limit must be between 1 and 500")

    service = ensure_rera_allocation_service()
    with closing(get_conn()) as conn:
        conn.row_factory = sqlite3.Row
        service.ensure_schema(conn)
        if booking_id is not None and booking_id.strip() != "":
            rows = conn.execute(
                """
                SELECT id, booking_id, payment_reference, event_type, receipt_amount,
                       applied_rera_ratio, rera_amount, operations_amount,
                       is_override, override_reason, actor_role, status, created_at
                FROM rera_allocation_events
                WHERE booking_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (booking_id.strip(), limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, booking_id, payment_reference, event_type, receipt_amount,
                       applied_rera_ratio, rera_amount, operations_amount,
                       is_override, override_reason, actor_role, status, created_at
                FROM rera_allocation_events
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        approval_status_map = {
            int(row["id"]): get_allocation_approval_status(conn, int(row["id"]))
            for row in rows
        }

    return {
        "status": "ok",
        "count": len(rows),
        "limit": limit,
        "booking_id_filter": booking_id.strip() if booking_id is not None else None,
        "items": [
            {
                "event_id": int(row["id"]),
                "booking_id": row["booking_id"],
                "payment_reference": row["payment_reference"],
                "event_type": row["event_type"],
                "receipt_amount": row["receipt_amount"],
                "applied_rera_ratio": row["applied_rera_ratio"],
                "rera_amount": row["rera_amount"],
                "operations_amount": row["operations_amount"],
                "is_override": bool(row["is_override"]),
                "override_reason": row["override_reason"],
                "actor_role": row["actor_role"],
                "status": row["status"],
                "approval_status": approval_status_map.get(int(row["id"]), "APPROVED"),
                "created_at": row["created_at"],
            }
            for row in rows
        ],
    }


@app.get("/api/v1/insights/friday-health")
async def get_friday_health(model: str = "llama3.2") -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(OLLAMA_TAGS_URL)
        response.raise_for_status()
        tags = response.json().get("models", [])
        available = [item.get("name") for item in tags if item.get("name")]
        model_ready = any(name == model or str(name).startswith(f"{model}:") for name in available)
        return {
            "status": "ok",
            "provider": "ollama",
            "endpoint": OLLAMA_HOST,
            "model_requested": model,
            "model_ready": model_ready,
            "available_models": available,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "degraded",
            "provider": "ollama",
            "endpoint": OLLAMA_HOST,
            "model_requested": model,
            "model_ready": False,
            "error": str(exc),
        }


@app.get("/api/v1/system/m3-telemetry")
async def get_m3_telemetry() -> dict[str, Any]:
    cpu_percent = 0.0
    ram_used_gb = 0.0
    ram_total_gb = 0.0
    cache_disk_free_mb = 0
    active_workers = 0
    thermal_pressure_pct = 0.0

    if psutil is not None:
        try:
            cpu_percent = float(psutil.cpu_percent(interval=None))
            vm = psutil.virtual_memory()
            ram_used_gb = round(vm.used / (1024**3), 2)
            ram_total_gb = round(vm.total / (1024**3), 2)
            active_workers = min(MAX_PARALLEL_WORKERS, len(psutil.Process().children(recursive=True)))
            memory_pressure = (float(vm.percent) if hasattr(vm, "percent") else 0.0)
            thermal_pressure_pct = round(min(100.0, (cpu_percent * 0.7) + (memory_pressure * 0.3)), 2)
        except Exception:  # noqa: BLE001
            pass

    target_workers = 8
    if thermal_pressure_pct < 30:
        target_workers = MAX_PARALLEL_WORKERS
    elif thermal_pressure_pct > 80:
        target_workers = 4

    try:
        if RAM_DISK_BUFFER.exists():
            usage = shutil.disk_usage(str(RAM_DISK_BUFFER.parent))
            cache_disk_free_mb = int(usage.free // (1024**2))
    except Exception:  # noqa: BLE001
        cache_disk_free_mb = 0

    return {
        "status": "ok",
        "cpu_percent": round(cpu_percent, 2),
        "ram_used_gb": ram_used_gb,
        "ram_total_gb": ram_total_gb,
        "thermal_pressure_pct": thermal_pressure_pct,
        "neural_engine_active": active_workers > 0,
        "active_workers": active_workers,
        "adaptive_target_workers": target_workers,
        "max_workers": MAX_PARALLEL_WORKERS,
        "cache_disk_free_mb": cache_disk_free_mb,
        "cache_disk_mounted": RAM_DISK_BUFFER.exists(),
        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


@app.get("/api/v1/system/saturation-pulse")
async def trigger_saturation_pulse() -> dict[str, Any]:
    """
    Demonstrates M3 maximum capacity via controlled CPU pulse.
    Spins 8-core CPU at 100% for 2 seconds to visually trigger Stark-Neon UI updates.
    Returns peak saturation telemetry snapshot for real-time dashboard display.
    
    Legal Context:
        Purely diagnostic—safe for demos, load testing, and hardware validation.
    """
    import time
    
    pulse_start = time.time()
    peak_cpu = 0.0
    peak_thermal = 0.0
    pulse_samples: list[dict[str, float]] = []
    
    # 2-second CPU burn loop: hammer all 8 cores simultaneously
    while time.time() - pulse_start < 2.0:
        # Compute-intensive workload (no I/O, pure CPU)
        _ = [x**2 for x in range(10000)]
        
        # Sample telemetry every 0.1 second
        if int((time.time() - pulse_start) * 10) > len(pulse_samples):
            if psutil is not None:
                try:
                    current_cpu = float(psutil.cpu_percent(interval=None))
                    vm = psutil.virtual_memory()
                    memory_pressure = float(vm.percent) if hasattr(vm, "percent") else 0.0
                    current_thermal = round(min(100.0, (current_cpu * 0.7) + (memory_pressure * 0.3)), 2)
                    peak_cpu = max(peak_cpu, current_cpu)
                    peak_thermal = max(peak_thermal, current_thermal)
                    pulse_samples.append({
                        "cpu_percent": round(current_cpu, 2),
                        "thermal_pressure_pct": current_thermal,
                        "timestamp": time.time() - pulse_start,
                    })
                except Exception:
                    pass
    
    pulse_duration = round(time.time() - pulse_start, 3)
    return {
        "status": "pulse_complete",
        "pulse_duration_seconds": pulse_duration,
        "peak_cpu_saturation": round(peak_cpu, 2),
        "peak_thermal_pressure": peak_thermal,
        "sample_count": len(pulse_samples),
        "samples": pulse_samples,
        "message": "M3 CPU floored for 2 seconds. Stark-Neon UI should now show saturation pulse.",
    }


@app.post("/api/v1/ledger/upload-photo")
async def upload_photo_to_ledger(
    file: UploadFile = File(...),
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)

    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing file name")
    content_type = (file.content_type or "").lower()
    if not (content_type.startswith("image/") or file.filename.lower().endswith((".png", ".jpg", ".jpeg", ".webp"))):
        raise HTTPException(status_code=422, detail="Only image uploads are supported")

    RECEIPT_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    ext = Path(file.filename).suffix.lower() or ".jpg"
    safe_name = f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex}{ext}"
    image_path = RECEIPT_STORAGE_DIR / safe_name

    raw = await file.read()
    if len(raw) == 0:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")
    image_path.write_bytes(raw)

    ocr_text = extract_text_with_tesseract(image_path)
    extracted, model_response = await extract_receipt_fields(image_path, ocr_text)

    imported_date = parse_date_from_text(str(extracted.get("date", "")))
    vendor_name = str(extracted.get("vendor", "")).strip() or "Receipt Vendor"
    gstin = str(extracted.get("gstin", "")).strip().upper()
    hsn = str(extracted.get("hsn", "")).strip()
    amount = parse_amount_from_text(str(extracted.get("total_amount", "0")))
    if amount <= 0:
        amount = parse_amount_from_text(ocr_text)
    if amount <= 0:
        raise HTTPException(status_code=422, detail="Unable to detect a valid receipt amount")

    with closing(get_conn()) as conn:
        try:
            check_period_lock(conn, imported_date)
            reference = next_journal_reference(conn, imported_date)
            purchases_id = get_account_id_by_name(conn, "Purchases")
            payable_id = get_account_id_by_name(conn, "Accounts Payable")

            created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            conn.execute(
                """
                INSERT INTO journal_entries(
                    date,
                    reference,
                    description,
                    company_state_code,
                    counterparty_state_code,
                    counterparty_gstin,
                    eco_gstin,
                    supply_source,
                    ims_status,
                    vendor_legal_name,
                    vendor_gstr1_filed_at,
                    status,
                    reversal_of_id,
                    is_filed,
                    filed_at,
                    filed_export_hash,
                    approved_by_1,
                    approved_by_2,
                    created_at
                ) VALUES (?, ?, ?, NULL, NULL, ?, NULL, 'DIRECT', 'PENDING', ?, NULL, 'POSTED', NULL, 0, NULL, NULL, NULL, NULL, ?)
                """,
                (
                    imported_date.isoformat(),
                    reference,
                    f"Vision receipt import: {vendor_name}",
                    gstin or None,
                    vendor_name,
                    created_at,
                ),
            )
            entry_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])

            conn.execute(
                "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
                (entry_id, purchases_id, money_str(amount), money_str(Decimal("0"))),
            )
            update_account_balance(conn, purchases_id, amount, Decimal("0"))

            conn.execute(
                "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
                (entry_id, payable_id, money_str(Decimal("0")), money_str(amount)),
            )
            update_account_balance(conn, payable_id, Decimal("0"), amount)
            fingerprint = stamp_entry_fingerprint(conn, entry_id)

            conn.execute(
                """
                INSERT INTO receipt_imports(entry_id, file_path, ocr_text, extracted_json, model_response, status, created_by, created_at)
                VALUES (?, ?, ?, ?, ?, 'PROCESSED', ?, ?)
                """,
                (
                    entry_id,
                    str(image_path),
                    ocr_text or None,
                    json.dumps(extracted) if extracted else None,
                    model_response or None,
                    admin_id,
                    created_at,
                ),
            )

            log_audit(
                conn,
                table_name="journal_entries",
                record_id=entry_id,
                action="VISION_RECEIPT_IMPORT",
                old_value=None,
                new_value={
                    "reference": reference,
                    "vendor": vendor_name,
                    "gstin": gstin,
                    "hsn": hsn,
                    "amount": money_str(amount),
                    "entry_fingerprint": fingerprint,
                    "receipt_file": str(image_path),
                    "actor_role": role,
                },
                user_id=admin_id,
                high_priority=True,
            )
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to process receipt: {exc}") from exc

    return {
        "status": "processed",
        "entry_id": entry_id,
        "reference": reference,
        "source": "VISION_LEDGER",
        "extracted": {
            "date": imported_date.isoformat(),
            "vendor": vendor_name,
            "gstin": gstin,
            "hsn": hsn,
            "total_amount": money_str(amount),
            "confidence": extracted.get("confidence", ""),
            "notes": extracted.get("notes", ""),
            "entry_fingerprint": fingerprint,
        },
    }


@app.post("/api/v1/ledger/mobile-sync")
async def mobile_sync_to_ledger(
    file: UploadFile = File(...),
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    """Accepts mobile photo capture and syncs it to the ledger via RAM-disk staging.

    Hardware Impact:
        Uses RAM-disk staging for low-latency image pipelines on Apple M3.
    Logic Invariants:
        Rejects non-image payloads and empty uploads before any ledger mutation.
    Legal Context:
        Maintains auditability by preserving extraction metadata and fingerprint continuity.
    """
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    _, ingest_layer, _, _ = ensure_service_layer()

    raw = await file.read()
    return await ingest_layer.mobile_sync(
        filename=file.filename or "",
        content_type=file.content_type or "",
        raw=raw,
        role=role,
        admin_id=admin_id,
    )


@app.post("/api/v1/ledger/neural-ink")
async def neural_ink_to_ledger(
    file: UploadFile = File(...),
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)

    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing file name")
    content_type = (file.content_type or "").lower()
    if not (content_type.startswith("image/") or file.filename.lower().endswith((".png", ".jpg", ".jpeg", ".webp"))):
        raise HTTPException(status_code=422, detail="Only image uploads are supported")

    RECEIPT_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    ext = Path(file.filename).suffix.lower() or ".jpg"
    safe_name = f"neural_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex}{ext}"
    image_path = RECEIPT_STORAGE_DIR / safe_name

    raw = await file.read()
    if len(raw) == 0:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")
    image_path.write_bytes(raw)

    preprocessed_path, preprocessed = preprocess_for_neural_ink(image_path)
    ocr_text = extract_text_with_tesseract(preprocessed_path)
    if not ocr_text:
        raise HTTPException(status_code=422, detail="Unable to extract OCR text from image")

    extracted, model_response = await NeuralInk.reconstruct(ocr_text)
    imported_date = parse_date_from_text(str(extracted.get("date", "")))
    vendor_name = str(extracted.get("vendor", "")).strip() or "Neural Ink Vendor"
    gstin = str(extracted.get("gstin", "")).strip().upper()
    hsn = str(extracted.get("hsn", "")).strip()
    amount = parse_amount_from_text(str(extracted.get("total_amount", "0")))
    if amount <= 0:
        amount = parse_amount_from_text(ocr_text)
    if amount <= 0:
        raise HTTPException(status_code=422, detail="Unable to detect a valid receipt amount")

    with closing(get_conn()) as conn:
        try:
            check_period_lock(conn, imported_date)
            reference = next_journal_reference(conn, imported_date)
            purchases_id = get_account_id_by_name(conn, "Purchases")
            payable_id = get_account_id_by_name(conn, "Accounts Payable")

            created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            cursor = conn.execute(
                """
                INSERT INTO journal_entries(
                    date,
                    reference,
                    description,
                    company_state_code,
                    counterparty_state_code,
                    counterparty_gstin,
                    eco_gstin,
                    supply_source,
                    ims_status,
                    vendor_legal_name,
                    vendor_gstr1_filed_at,
                    status,
                    reversal_of_id,
                    is_filed,
                    filed_at,
                    filed_export_hash,
                    approved_by_1,
                    approved_by_2,
                    created_at
                ) VALUES (?, ?, ?, NULL, NULL, ?, NULL, 'DIRECT', 'PENDING', ?, NULL, 'POSTED', NULL, 0, NULL, NULL, NULL, NULL, ?)
                """,
                (
                    imported_date.isoformat(),
                    reference,
                    f"Neural-Ink handwritten import: {vendor_name}",
                    gstin or None,
                    vendor_name,
                    created_at,
                ),
            )
            entry_id = int(cursor.lastrowid)

            conn.execute(
                "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
                (entry_id, purchases_id, money_str(amount), money_str(Decimal("0"))),
            )
            update_account_balance(conn, purchases_id, amount, Decimal("0"))

            conn.execute(
                "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
                (entry_id, payable_id, money_str(Decimal("0")), money_str(amount)),
            )
            update_account_balance(conn, payable_id, Decimal("0"), amount)
            fingerprint = stamp_entry_fingerprint(conn, entry_id)

            conn.execute(
                """
                INSERT INTO receipt_imports(entry_id, file_path, ocr_text, extracted_json, model_response, status, created_by, created_at)
                VALUES (?, ?, ?, ?, ?, 'NEURAL_INK', ?, ?)
                """,
                (
                    entry_id,
                    str(image_path),
                    ocr_text,
                    json.dumps(extracted) if extracted else None,
                    model_response,
                    admin_id,
                    created_at,
                ),
            )

            _, filename, export_path, _, _ = generate_tally_export(conn, entry_id)

            log_audit(
                conn,
                table_name="journal_entries",
                record_id=entry_id,
                action="NEURAL_INK_IMPORT",
                old_value=None,
                new_value={
                    "reference": reference,
                    "vendor": vendor_name,
                    "gstin": gstin,
                    "hsn": hsn,
                    "amount": money_str(amount),
                    "entry_fingerprint": fingerprint,
                    "receipt_file": str(image_path),
                    "tally_export": str(export_path),
                    "preprocessed": preprocessed,
                    "actor_role": role,
                },
                user_id=admin_id,
                high_priority=True,
            )
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Failed Neural-Ink import: {exc}") from exc
        finally:
            if preprocessed and preprocessed_path != image_path:
                preprocessed_path.unlink(missing_ok=True)

    return {
        "status": "processed",
        "pipeline": "Neural-Ink",
        "entry_id": entry_id,
        "reference": reference,
        "entry_fingerprint": fingerprint,
        "tally_export_file": filename,
        "tally_export_path": str(export_path),
        "ocr_preview": ocr_text[:600],
        "preprocessed_with_opencv": preprocessed,
        "extracted": {
            "date": imported_date.isoformat(),
            "vendor": vendor_name,
            "gstin": gstin,
            "hsn": hsn,
            "total_amount": money_str(amount),
            "cgst": extracted.get("cgst", ""),
            "sgst": extracted.get("sgst", ""),
            "igst": extracted.get("igst", ""),
            "confidence": extracted.get("confidence", ""),
            "notes": extracted.get("notes", ""),
        },
    }


@app.post("/api/v1/ledger/ingest")
async def omni_reader_ingest(
    file: UploadFile = File(...),
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)

    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing file name")

    content_type = (file.content_type or "").lower()
    name = file.filename.lower()

    is_excel = name.endswith((".xlsx", ".xls", ".csv")) or "sheet" in content_type or "excel" in content_type
    is_pdf = name.endswith(".pdf") or content_type == "application/pdf"
    is_image = name.endswith((".png", ".jpg", ".jpeg", ".webp")) or content_type.startswith("image/")
    is_video = name.endswith((".mp4", ".mov", ".avi", ".mkv", ".m4v")) or content_type.startswith("video/")

    if not (is_excel or is_pdf or is_image or is_video):
        raise HTTPException(status_code=422, detail="Supported formats: Excel/CSV, PDF, PNG/JPEG/WEBP, MP4/MOV/AVI/MKV")

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    OMNI_INGEST_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    ext = Path(file.filename).suffix.lower() or ".bin"
    ingest_name = f"omni_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex}{ext}"
    ingest_path = OMNI_INGEST_DIR / ingest_name
    ingest_path.write_bytes(raw)

    results: list[dict[str, Any]] = []
    pipeline = ""

    if is_excel:
        pipeline = "OMNI_EXCEL"
        rows = _extract_excel_rows(ingest_path)
        if not rows:
            raise HTTPException(status_code=422, detail="Excel/CSV contains no readable rows")

        for index, row in enumerate(rows, start=1):
            extracted = _row_to_ledger_fields(row)
            if parse_amount_from_text(str(extracted.get("total_amount", "0"))) <= 0:
                continue

            with closing(get_conn()) as conn:
                try:
                    entry = _post_ledger_entry_from_extract(
                        conn=conn,
                        extracted=extracted,
                        fallback_text=json.dumps(row),
                        description_prefix=f"Omni-Reader Excel row {index}",
                        actor_role=role,
                        admin_id=admin_id,
                        source_file_path=ingest_path,
                        model_response="",
                        import_status="OMNI_EXCEL",
                    )
                    conn.commit()
                    results.append(entry)
                except HTTPException:
                    conn.rollback()
                    raise
                except Exception as exc:  # noqa: BLE001
                    conn.rollback()
                    raise HTTPException(status_code=500, detail=f"Failed to post Excel row {index}: {exc}") from exc

        if not results:
            raise HTTPException(status_code=422, detail="No rows produced a valid amount for ledger posting")

    else:
        extracted: dict[str, Any] = {}
        model_response = ""
        fallback_text = ""

        if is_pdf:
            pipeline = "OMNI_PDF"
            extracted, model_response, fallback_text = await _extract_from_pdf(ingest_path)
        elif is_image:
            pipeline = "OMNI_IMAGE"
            fallback_text = extract_text_with_tesseract(ingest_path)
            extracted, model_response = await extract_receipt_fields(ingest_path, fallback_text)
        else:
            pipeline = "OMNI_VIDEO"
            extracted, model_response, fallback_text = await _extract_from_video(ingest_path)

        with closing(get_conn()) as conn:
            try:
                entry = _post_ledger_entry_from_extract(
                    conn=conn,
                    extracted=extracted,
                    fallback_text=fallback_text,
                    description_prefix=f"Omni-Reader {pipeline}",
                    actor_role=role,
                    admin_id=admin_id,
                    source_file_path=ingest_path,
                    model_response=model_response,
                    import_status=pipeline,
                )
                conn.commit()
                results.append(entry)
            except HTTPException:
                conn.rollback()
                raise
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                raise HTTPException(status_code=500, detail=f"Failed Omni-Reader ingestion: {exc}") from exc

    archived_path = PROCESSED_ARCHIVE_DIR / ingest_name
    shutil.copy2(ingest_path, archived_path)

    return {
        "status": "processed",
        "pipeline": pipeline,
        "input_file": str(ingest_path),
        "archive_file": str(archived_path),
        "entries_created": len(results),
        "results": results,
    }


def _post_batch_extract_results(
    extracted_results: list[dict[str, Any]],
    role: str,
    admin_id: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, str]]]:
    posted: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    retry_candidates: list[dict[str, str]] = []

    for bundle in extracted_results:
        item = bundle["meta"]
        result = bundle["result"]
        source_path = Path(item["path"])

        if result.get("status") != "ok":
            failed.append(
                {
                    "file": item["file_name"],
                    "reason": result.get("reason", "Unknown extraction failure"),
                }
            )
            retry_candidates.append(item)
            continue

        try:
            with closing(get_conn()) as conn:
                entry = _post_ledger_entry_from_extract(
                    conn=conn,
                    extracted=result.get("extracted") or {},
                    fallback_text=str(result.get("raw_text", "")),
                    description_prefix=f"Omni-Mixed {result.get('pipeline', 'BATCH')}",
                    actor_role=role,
                    admin_id=admin_id,
                    source_file_path=source_path,
                    model_response="",
                    import_status=str(result.get("pipeline", "OMNI_BATCH")),
                )
                conn.commit()
                posted.append(
                    {
                        "file": item["file_name"],
                        "pipeline": result.get("pipeline"),
                        "entry": entry,
                    }
                )
        except Exception as exc:  # noqa: BLE001
            failed.append({"file": item["file_name"], "reason": str(exc)})
            retry_candidates.append(item)

        archived = PROCESSED_ARCHIVE_DIR / source_path.name
        shutil.copy2(source_path, archived)

    return posted, failed, retry_candidates


@app.post("/api/v1/ledger/ingest-batch")
async def omni_reader_ingest_batch(
    files: list[UploadFile] = File(...),
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)

    if not files:
        raise HTTPException(status_code=400, detail="At least one file is required")
    if len(files) > 50:
        raise HTTPException(status_code=422, detail="Maximum 50 files per mixed batch")

    OMNI_INGEST_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    saved_files: list[dict[str, str]] = []
    for file in files:
        if not file.filename:
            continue
        raw = await file.read()
        if not raw:
            continue
        ext = Path(file.filename).suffix.lower() or ".bin"
        ingest_name = f"batch_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex}{ext}"
        ingest_path = OMNI_INGEST_DIR / ingest_name
        ingest_path.write_bytes(raw)
        saved_files.append(
            {
                "file_name": file.filename,
                "content_type": file.content_type or "",
                "path": str(ingest_path),
            }
        )

    if not saved_files:
        raise HTTPException(status_code=400, detail="No readable files found in batch")

    adaptive_ingest = AdaptiveIngest(max_workers=12, min_workers=4)
    extracted_results, chosen_workers, retried_count = await adaptive_ingest.process_batch(saved_files)
    posted, failed, retry_candidates = _post_batch_extract_results(extracted_results, role=role, admin_id=admin_id)

    batch_id = uuid.uuid4().hex[:16]
    INGEST_RETRY_BUCKETS[batch_id] = {
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "files": retry_candidates,
    }

    return {
        "status": "processed",
        "engine": "OMNI_GOD_MODE",
        "worker_model": f"ProcessPoolExecutor({chosen_workers})",
        "adaptive_cpu_load": round(adaptive_ingest.last_cpu_load, 2),
        "adaptive_memory_available_gb": round(adaptive_ingest.last_memory_available_gb, 2),
        "saturation_alert": adaptive_ingest.saturation_alert,
        "retry_attempted": retried_count,
        "retry_bucket_size": len(adaptive_ingest.retry_bucket),
        "retry_batch_id": batch_id,
        "submitted": len(saved_files),
        "posted_entries": len(posted),
        "failed_entries": len(failed),
        "results": posted,
        "failures": failed,
    }


@app.post("/api/v1/ledger/ingest-batch/retry-failed")
async def omni_reader_retry_failed(
    payload: IngestBatchRetryIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)

    bucket = INGEST_RETRY_BUCKETS.get(payload.batch_id)
    if bucket is None:
        raise HTTPException(status_code=404, detail="Retry batch not found or expired")

    files = list(bucket.get("files") or [])
    if not files:
        return {
            "status": "ok",
            "message": "No failed files left in retry bucket",
            "batch_id": payload.batch_id,
            "posted_entries": 0,
            "failed_entries": 0,
            "results": [],
            "failures": [],
        }

    adaptive_ingest = AdaptiveIngest(max_workers=12, min_workers=4)
    extracted_results, chosen_workers, retried_count = await adaptive_ingest.process_batch(files)
    posted, failed, retry_candidates = _post_batch_extract_results(extracted_results, role=role, admin_id=admin_id)

    INGEST_RETRY_BUCKETS[payload.batch_id]["files"] = retry_candidates

    return {
        "status": "processed",
        "batch_id": payload.batch_id,
        "engine": "OMNI_GOD_MODE_RETRY",
        "worker_model": f"ProcessPoolExecutor({chosen_workers})",
        "adaptive_cpu_load": round(adaptive_ingest.last_cpu_load, 2),
        "adaptive_memory_available_gb": round(adaptive_ingest.last_memory_available_gb, 2),
        "saturation_alert": adaptive_ingest.saturation_alert,
        "retry_attempted": retried_count,
        "retry_bucket_size": len(retry_candidates),
        "submitted": len(files),
        "posted_entries": len(posted),
        "failed_entries": len(failed),
        "results": posted,
        "failures": failed,
    }


@app.post("/api/v1/ledger/voice-sync")
async def post_voice_sync(
    payload: VoiceSyncIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    voice_layer = ensure_voice_service()

    parsed = await voice_layer.parse_command_to_ledger(payload.transcript)
    amount = parse_amount_from_text(str(parsed.get("amount") or "0"))
    if amount <= 0:
        amount = parse_amount_from_text(payload.transcript)
    if amount <= 0:
        raise HTTPException(status_code=422, detail="Voice transcript does not contain a valid amount")

    debit_name = _resolve_account_name(str(parsed.get("account_dr") or ""), "Cash")
    credit_name = _resolve_account_name(str(parsed.get("account_cr") or ""), "Sales Revenue")
    voucher_type = re.sub(r"[^A-Z_]", "", str(parsed.get("voucher_type") or "JOURNAL").upper()) or "JOURNAL"
    posted_date = payload.posting_date or parse_date_from_text(str(parsed.get("date") or ""))
    vendor_name = str(parsed.get("vendor") or "Voice Counterparty").strip() or "Voice Counterparty"
    gstin_raw = str(parsed.get("gstin") or "").strip().upper()
    gstin = validate_gstin(gstin_raw) if len(gstin_raw) == 15 else ""
    description = str(parsed.get("description") or "").strip() or f"Voice sync: {payload.transcript[:140]}"
    parsed_currency = str(parsed.get("currency_code") or "").strip().upper()
    payload_currency = str(payload.currency_code or "").strip().upper()
    currency_code = normalize_currency_code(payload_currency or parsed_currency or infer_currency_code_from_text(payload.transcript))
    exchange_rate = resolve_exchange_rate(currency_code, payload.exchange_rate if payload.exchange_rate is not None else parsed.get("exchange_rate"))
    amount_base = ensure_currency_service().convert_to_base(amount, currency_code, exchange_rate)

    with closing(get_conn()) as conn:
        try:
            check_period_lock(conn, posted_date)
            reference = next_journal_reference(conn, posted_date)
            debit_id = get_account_id_by_name(conn, debit_name)
            credit_id = get_account_id_by_name(conn, credit_name)
            created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

            conn.execute(
                """
                INSERT INTO journal_entries(
                    date,
                    reference,
                    description,
                    company_state_code,
                    counterparty_state_code,
                    counterparty_gstin,
                    eco_gstin,
                    supply_source,
                    ims_status,
                    vendor_legal_name,
                    vendor_gstr1_filed_at,
                    status,
                    reversal_of_id,
                    is_filed,
                    filed_at,
                    filed_export_hash,
                    approved_by_1,
                    approved_by_2,
                    voucher_type,
                    currency_code,
                    exchange_rate,
                    created_at
                ) VALUES (?, ?, ?, NULL, NULL, ?, NULL, 'DIRECT', 'PENDING', ?, NULL, 'POSTED', NULL, 0, NULL, NULL, NULL, NULL, ?, ?, ?, ?)
                """,
                (
                    posted_date.isoformat(),
                    reference,
                    description,
                    gstin or None,
                    vendor_name,
                    voucher_type,
                    currency_code,
                    money_str(exchange_rate),
                    created_at,
                ),
            )
            entry_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])

            conn.execute(
                "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
                (entry_id, debit_id, money_str(amount_base), "0.0000"),
            )
            update_account_balance(conn, debit_id, amount_base, Decimal("0"))

            conn.execute(
                "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
                (entry_id, credit_id, "0.0000", money_str(amount_base)),
            )
            update_account_balance(conn, credit_id, Decimal("0"), amount_base)
            fingerprint = stamp_entry_fingerprint(conn, entry_id)

            log_audit(
                conn,
                table_name="journal_entries",
                record_id=entry_id,
                action="VOICE_SYNC_IMPORT",
                old_value=None,
                new_value={
                    "reference": reference,
                    "transcript": payload.transcript,
                    "parsed": parsed,
                    "debit_account": debit_name,
                    "credit_account": credit_name,
                    "voucher_type": voucher_type,
                    "amount": money_str(amount_base),
                    "transaction_amount": money_str(amount),
                    "currency_code": currency_code,
                    "exchange_rate": money_str(exchange_rate),
                    "entry_fingerprint": fingerprint,
                    "actor_role": role,
                },
                user_id=admin_id,
                high_priority=True,
            )
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to post voice sync: {exc}") from exc

    return {
        "status": "processed",
        "pipeline": "NEURAL_TALK",
        "verified": True,
        "transcript": payload.transcript,
        "parsed_journal": {
            "date": posted_date.isoformat(),
            "vendor": vendor_name,
            "gstin": gstin,
            "debit_account": debit_name,
            "credit_account": credit_name,
            "amount": money_str(amount_base),
            "transaction_amount": money_str(amount),
            "currency_code": currency_code,
            "exchange_rate": money_str(exchange_rate),
            "voucher_type": voucher_type,
            "description": description,
        },
        "entry_id": entry_id,
        "reference": reference,
        "entry_fingerprint": fingerprint,
    }


@app.post("/api/v1/ledger/voice-cmd")
async def post_voice_command(
    file: UploadFile = File(...),
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)

    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing audio file name")

    content_type = (file.content_type or "").lower()
    if not (content_type.startswith("audio/") or file.filename.lower().endswith((".wav", ".mp3", ".m4a", ".aac", ".ogg"))):
        raise HTTPException(status_code=422, detail="Supported voice formats: WAV, MP3, M4A, AAC, OGG")

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded audio is empty")

    RAM_DISK_BUFFER.mkdir(parents=True, exist_ok=True)
    ext = Path(file.filename).suffix.lower() or ".wav"
    audio_path = RAM_DISK_BUFFER / f"voice_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex}{ext}"
    audio_path.write_bytes(raw)

    try:
        transcript = _transcribe_audio_to_text(audio_path)
        parsed = await _extract_voice_voucher(transcript)

        amount = parse_amount_from_text(str(parsed.get("amount", "0")))
        if amount <= 0:
            amount = parse_amount_from_text(transcript)
        if amount <= 0:
            raise HTTPException(status_code=422, detail="Voice command does not contain a valid amount")

        debit_name = _resolve_account_name(str(parsed.get("debit_account", "")), "Cash")
        credit_name = _resolve_account_name(str(parsed.get("credit_account", "")), "Sales Revenue")
        posted_date = parse_date_from_text(str(parsed.get("date", "")))
        vendor_name = str(parsed.get("vendor", "")).strip() or "Voice Counterparty"
        gstin = str(parsed.get("gstin", "")).strip().upper()
        description = str(parsed.get("description", "")).strip() or f"Voice command: {transcript[:120]}"

        with closing(get_conn()) as conn:
            try:
                check_period_lock(conn, posted_date)
                reference = next_journal_reference(conn, posted_date)
                debit_id = get_account_id_by_name(conn, debit_name)
                credit_id = get_account_id_by_name(conn, credit_name)
                created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

                conn.execute(
                    """
                    INSERT INTO journal_entries(
                        date, reference, description, company_state_code, counterparty_state_code,
                        counterparty_gstin, eco_gstin, supply_source, ims_status, vendor_legal_name,
                        vendor_gstr1_filed_at, status, reversal_of_id, is_filed, filed_at,
                        filed_export_hash, approved_by_1, approved_by_2, created_at
                    ) VALUES (?, ?, ?, NULL, NULL, ?, NULL, 'DIRECT', 'PENDING', ?, NULL, 'POSTED', NULL, 0, NULL, NULL, NULL, NULL, ?)
                    """,
                    (
                        posted_date.isoformat(),
                        reference,
                        description,
                        gstin or None,
                        vendor_name,
                        created_at,
                    ),
                )
                entry_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])

                conn.execute(
                    "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
                    (entry_id, debit_id, money_str(amount), "0.0000"),
                )
                update_account_balance(conn, debit_id, amount, Decimal("0"))

                conn.execute(
                    "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
                    (entry_id, credit_id, "0.0000", money_str(amount)),
                )
                update_account_balance(conn, credit_id, Decimal("0"), amount)
                fingerprint = stamp_entry_fingerprint(conn, entry_id)

                log_audit(
                    conn,
                    table_name="journal_entries",
                    record_id=entry_id,
                    action="VOICE_CMD_IMPORT",
                    old_value=None,
                    new_value={
                        "reference": reference,
                        "transcript": transcript,
                        "debit_account": debit_name,
                        "credit_account": credit_name,
                        "amount": money_str(amount),
                        "entry_fingerprint": fingerprint,
                        "actor_role": role,
                    },
                    user_id=admin_id,
                    high_priority=True,
                )
                conn.commit()
            except HTTPException:
                conn.rollback()
                raise
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                raise HTTPException(status_code=500, detail=f"Failed to post voice command: {exc}") from exc
    finally:
        audio_path.unlink(missing_ok=True)

    return {
        "status": "processed",
        "pipeline": "VOICE_TO_LEDGER",
        "transcript": transcript,
        "parsed_voucher": {
            "date": posted_date.isoformat(),
            "vendor": vendor_name,
            "gstin": gstin,
            "debit_account": debit_name,
            "credit_account": credit_name,
            "amount": money_str(amount),
            "description": description,
        },
        "entry_id": entry_id,
        "reference": reference,
        "entry_fingerprint": fingerprint,
    }


@app.post("/api/v1/ledger/nudge-vendor")
async def post_nudge_vendor(
    payload: VendorNudgeIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)

    gstin = validate_gstin(payload.gstin)
    vendor_name = (payload.vendor_name or "Vendor").strip() or "Vendor"
    invoice_reference = (payload.invoice_reference or "Unknown").strip() or "Unknown"
    invoice_amount = money_str(payload.invoice_amount) if payload.invoice_amount is not None else "0.0000"

    nudge = await _draft_vendor_nudge_message(
        vendor_name=vendor_name,
        gstin=gstin,
        invoice_reference=invoice_reference,
        invoice_amount=invoice_amount,
        mismatch_reason=(payload.mismatch_reason or "").strip(),
    )

    twilio_status = "not_attempted"
    if payload.phone_number:
        sid = os.getenv("TWILIO_ACCOUNT_SID")
        token = os.getenv("TWILIO_AUTH_TOKEN")
        from_whatsapp = os.getenv("TWILIO_WHATSAPP_FROM")
        if sid and token and from_whatsapp:
            try:
                from twilio.rest import Client  # type: ignore

                client = Client(sid, token)
                client.messages.create(
                    body=nudge["message"],
                    from_=from_whatsapp,
                    to=f"whatsapp:{payload.phone_number}",
                )
                twilio_status = "sent"
            except Exception as exc:  # noqa: BLE001
                twilio_status = f"failed: {exc}"
        else:
            twilio_status = "skipped: missing Twilio credentials"

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        log_audit(
            conn,
            table_name="vendor_trust_scores",
            record_id=0,
            action="VENDOR_NUDGE_GENERATED",
            old_value=None,
            new_value={
                "gstin": gstin,
                "vendor_name": vendor_name,
                "invoice_reference": invoice_reference,
                "invoice_amount": invoice_amount,
                "urgency": nudge["urgency"],
                "twilio_status": twilio_status,
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    if twilio_status == "sent":
        nudge_status = "SENT"
    elif str(twilio_status).startswith("failed"):
        nudge_status = "FAILED"
    else:
        nudge_status = "PENDING"

    return {
        "status": "generated",
        "gstin": gstin,
        "vendor_name": vendor_name,
        "subject": nudge["subject"],
        "urgency": nudge["urgency"],
        "nudge_status": nudge_status,
        "whatsapp_message": nudge["message"],
        "twilio_status": twilio_status,
    }


def _fetch_ledger_rows_for_bank_reconciliation(
    *,
    from_date: date | None,
    to_date: date | None,
) -> list[dict[str, Any]]:
    query = (
        """
        SELECT je.id,
               je.date,
               je.reference,
               je.description,
               MAX(CASE WHEN CAST(jl.debit AS REAL) > 0 THEN jl.debit ELSE jl.credit END) AS amount
        FROM journal_entries je
        JOIN journal_lines jl ON jl.entry_id = je.id
        WHERE je.status = 'POSTED'
        """
    )
    params: list[Any] = []
    if from_date is not None:
        query += " AND je.date >= ?"
        params.append(from_date.isoformat())
    if to_date is not None:
        query += " AND je.date <= ?"
        params.append(to_date.isoformat())

    query += " GROUP BY je.id, je.date, je.reference, je.description ORDER BY je.date DESC, je.id DESC"

    with closing(get_conn()) as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        return [dict(row) for row in rows]


def _parse_bank_statement_csv(raw: bytes) -> list[dict[str, Any]]:
    try:
        text_blob = raw.decode("utf-8")
    except UnicodeDecodeError:
        text_blob = raw.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text_blob))
    parsed: list[dict[str, Any]] = []
    for row in reader:
        if not row:
            continue
        date_value = str(
            row.get("date")
            or row.get("txn_date")
            or row.get("transaction_date")
            or row.get("value_date")
            or ""
        ).strip()
        amount_value = str(
            row.get("amount")
            or row.get("txn_amount")
            or row.get("debit")
            or row.get("credit")
            or "0"
        ).strip()
        reference = str(
            row.get("reference")
            or row.get("narration")
            or row.get("description")
            or row.get("remarks")
            or ""
        ).strip()

        amount = parse_amount_from_text(amount_value)
        if amount <= 0:
            continue

        parsed.append(
            {
                "date": date_value,
                "amount": money_str(amount),
                "reference": reference,
                "narration": reference,
            }
        )

    return parsed


@app.post("/api/v1/ledger/reconcile-bank")
async def reconcile_bank_statement(
    payload: BankReconciliationIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    banking_layer = ensure_banking_service()

    ledger_rows = _fetch_ledger_rows_for_bank_reconciliation(
        from_date=payload.from_date,
        to_date=payload.to_date,
    )
    if not ledger_rows:
        raise HTTPException(status_code=422, detail="No posted ledger entries found for requested window")

    bank_rows = [
        {
            "date": row.date,
            "amount": money_str(row.amount),
            "reference": row.reference,
            "narration": row.narration,
        }
        for row in payload.bank_rows
    ]

    reconciliation = await banking_layer.reconcile_statement_multi_pass(
        bank_rows=bank_rows,
        ledger_rows=ledger_rows,
        fuzzy_threshold=payload.fuzzy_threshold,
        amount_tolerance=money_str(payload.amount_tolerance),
        enable_ai=payload.enable_ai,
    )

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        log_audit(
            conn,
            table_name="reports",
            record_id=0,
            action="BANK_STATEMENT_RECONCILIATION",
            old_value=None,
            new_value={
                "engine": reconciliation.get("engine"),
                "bank_rows": len(bank_rows),
                "ledger_rows": len(ledger_rows),
                "matched": reconciliation.get("summary", {}).get("matched_count", 0),
                "match_rate_pct": reconciliation.get("match_rate_pct"),
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    return reconciliation


@app.post("/api/v1/ledger/reconcile-bank/csv")
async def reconcile_bank_statement_csv(
    file: UploadFile = File(...),
    from_date: date | None = None,
    to_date: date | None = None,
    fuzzy_threshold: float = 0.84,
    amount_tolerance: str = "1.0000",
    enable_ai: bool = True,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    banking_layer = ensure_banking_service()

    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing bank statement file name")
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=422, detail="Only CSV statements are currently supported")

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded statement file is empty")

    bank_rows = _parse_bank_statement_csv(raw)
    if not bank_rows:
        raise HTTPException(status_code=422, detail="No valid statement rows found in CSV")

    ledger_rows = _fetch_ledger_rows_for_bank_reconciliation(from_date=from_date, to_date=to_date)
    if not ledger_rows:
        raise HTTPException(status_code=422, detail="No posted ledger entries found for requested window")

    reconciliation = await banking_layer.reconcile_statement_multi_pass(
        bank_rows=bank_rows,
        ledger_rows=ledger_rows,
        fuzzy_threshold=fuzzy_threshold,
        amount_tolerance=amount_tolerance,
        enable_ai=enable_ai,
    )

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        log_audit(
            conn,
            table_name="reports",
            record_id=0,
            action="BANK_STATEMENT_RECONCILIATION_CSV",
            old_value=None,
            new_value={
                "engine": reconciliation.get("engine"),
                "file_name": file.filename,
                "bank_rows": len(bank_rows),
                "ledger_rows": len(ledger_rows),
                "matched": reconciliation.get("summary", {}).get("matched_count", 0),
                "match_rate_pct": reconciliation.get("match_rate_pct"),
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    return reconciliation


@app.post("/api/v1/ledger/reconcile-2b")
async def post_reconcile_2b(
    file: UploadFile = File(...),
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    _, _, compliance_layer, _ = ensure_service_layer()

    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing 2B file name")

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded 2B file is empty")

    OMNI_INGEST_DIR.mkdir(parents=True, exist_ok=True)
    ext = Path(file.filename).suffix.lower() or ".json"
    two_b_path = OMNI_INGEST_DIR / f"gstr2b_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex}{ext}"
    two_b_path.write_bytes(raw)

    records_2b = _extract_2b_records(two_b_path)
    if not records_2b:
        raise HTTPException(status_code=422, detail="No valid invoice records found in uploaded 2B payload")

    with closing(get_conn()) as conn:
        ledger_rows = conn.execute(
            """
            SELECT je.id,
                   je.reference,
                   je.date,
                   je.counterparty_gstin,
                   je.vendor_legal_name,
                   tl.tax_amount,
                   tl.taxable_value
            FROM tax_ledger tl
            JOIN journal_entries je ON je.id = tl.entry_id
            WHERE tl.supply_type = 'B2B'
              AND je.status = 'POSTED'
            ORDER BY je.date DESC, je.id DESC
            """
        ).fetchall()
    reconciliation = await compliance_layer.reconcile_gstr2b(
        two_b_path=two_b_path,
        ledger_rows=[dict(row) for row in ledger_rows],
    )
    ghost_invoices = reconciliation["ghost_invoices"]
    nexus_graph = reconciliation["nexus_graph"]

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        log_audit(
            conn,
            table_name="reports",
            record_id=0,
            action="GSTR_2B_RECONCILIATION",
            old_value=None,
            new_value={
                "uploaded_records": len(records_2b),
                "ledger_records": len(ledger_rows),
                "ghost_invoices": len(ghost_invoices),
                "nexus_risk_clusters": len(nexus_graph.get("risk_clusters", [])),
                "two_b_file": str(two_b_path),
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    return {
        "status": "reconciled",
        "uploaded_records": reconciliation["uploaded_records"],
        "ledger_records": reconciliation["ledger_records"],
        "ghost_invoices_count": len(ghost_invoices),
        "ghost_invoices": ghost_invoices,
        "anomaly_summary": reconciliation.get("anomaly_summary", {}),
        "nexus_graph": nexus_graph,
        "source_file": str(two_b_path),
    }


@app.get("/api/v1/ledger/nexus-graph/latest")
def get_latest_nexus_graph(
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin", "ca"})
    require_admin_id(x_admin_id)

    if not NEXUS_GRAPH_DIR.exists():
        raise HTTPException(status_code=404, detail="No nexus graph has been generated yet")

    candidates = sorted(NEXUS_GRAPH_DIR.glob("nexus_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise HTTPException(status_code=404, detail="No nexus graph has been generated yet")

    latest = candidates[0]
    payload = json.loads(latest.read_text(encoding="utf-8"))
    return {
        "status": "ok",
        "graph_file": str(latest),
        "generated_at": datetime.utcfromtimestamp(latest.stat().st_mtime).isoformat(timespec="seconds") + "Z",
        **payload,
    }


@app.get("/api/v1/ledger/export-tally/{entry_id}")
def export_tally_xml(
    entry_id: int,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> Response:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)

    with closing(get_conn()) as conn:
        xml_bytes, filename, export_path, reference, entry_status = generate_tally_export(conn, entry_id)

        log_audit(
            conn,
            table_name="journal_entries",
            record_id=entry_id,
            action="TALLY_XML_EXPORT",
            old_value=None,
            new_value={
                "reference": reference,
                "entry_status": entry_status,
                "export_file": str(export_path),
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=False,
        )
        conn.commit()

    return Response(
        content=xml_bytes,
        media_type="application/xml",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "X-Accord-Export-Path": str(export_path),
        },
    )


def process_receipt_batch_worker(
    staged_path: str,
    admin_id_val: int,
) -> dict[str, Any]:
    """M3-optimized worker for parallel receipt processing.
    
    This runs in a separate process via ProcessPoolExecutor.
    Each worker handles: OCR → Llava vision extraction → Journal posting
    """
    try:
        image_path = Path(staged_path)
        if not image_path.exists():
            return {"status": "failed", "error": "Staged image not found"}

        ocr_text = extract_text_with_tesseract(image_path)

        # Sync extraction call (blocking for worker)
        import asyncio as aio_batch
        loop = aio_batch.new_event_loop()
        aio_batch.set_event_loop(loop)
        try:
            extracted, model_response = loop.run_until_complete(extract_receipt_fields(image_path, ocr_text))
        finally:
            loop.close()

        imported_date = parse_date_from_text(str(extracted.get("date", "")))
        vendor_name = str(extracted.get("vendor", "")).strip() or "Receipt Vendor"
        gstin = str(extracted.get("gstin", "")).strip().upper()
        hsn = str(extracted.get("hsn", "")).strip()
        amount = parse_amount_from_text(str(extracted.get("total_amount", "0")))

        if amount <= 0:
            amount = parse_amount_from_text(ocr_text)
        if amount <= 0:
            return {"status": "failed", "error": "No valid amount detected"}

        with closing(get_conn()) as conn:
            try:
                check_period_lock(conn, imported_date)
                reference = next_journal_reference(conn, imported_date)
                purchases_id = get_account_id_by_name(conn, "Purchases")
                payable_id = get_account_id_by_name(conn, "Accounts Payable")

                created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                conn.execute(
                    """
                    INSERT INTO journal_entries(
                        date, reference, description, company_state_code, counterparty_state_code,
                        counterparty_gstin, eco_gstin, supply_source, ims_status, vendor_legal_name,
                        vendor_gstr1_filed_at, status, reversal_of_id, is_filed, filed_at,
                        filed_export_hash, approved_by_1, approved_by_2, created_at
                    ) VALUES (?, ?, ?, NULL, NULL, ?, NULL, 'DIRECT', 'PENDING', ?, NULL, 'POSTED', NULL, 0, NULL, NULL, NULL, NULL, ?)
                    """,
                    (
                        imported_date.isoformat(),
                        reference,
                        f"Vision batch import: {vendor_name}",
                        gstin or None,
                        vendor_name,
                        created_at,
                    ),
                )
                entry_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])

                conn.execute(
                    "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
                    (entry_id, purchases_id, money_str(amount), money_str(Decimal("0"))),
                )
                update_account_balance(conn, purchases_id, amount, Decimal("0"))

                conn.execute(
                    "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
                    (entry_id, payable_id, money_str(Decimal("0")), money_str(amount)),
                )
                update_account_balance(conn, payable_id, Decimal("0"), amount)
                fingerprint = stamp_entry_fingerprint(conn, entry_id)

                conn.execute(
                    """
                    INSERT INTO receipt_imports(entry_id, file_path, ocr_text, extracted_json, model_response, status, created_by, created_at)
                    VALUES (?, ?, ?, ?, ?, 'PROCESSED', ?, ?)
                    """,
                    (
                        entry_id,
                        str(image_path),
                        ocr_text or None,
                        json.dumps(extracted) if extracted else None,
                        model_response or None,
                        admin_id_val,
                        created_at,
                    ),
                )

                log_audit(
                    conn,
                    table_name="journal_entries",
                    record_id=entry_id,
                    action="VISION_BATCH_IMPORT",
                    old_value=None,
                    new_value={
                        "reference": reference,
                        "vendor": vendor_name,
                        "gstin": gstin,
                        "hsn": hsn,
                        "amount": money_str(amount),
                        "entry_fingerprint": fingerprint,
                        "receipt_file": str(image_path),
                    },
                    user_id=admin_id_val,
                    high_priority=True,
                )
                conn.commit()

                return {
                    "status": "processed",
                    "entry_id": entry_id,
                    "reference": reference,
                    "vendor": vendor_name,
                    "amount": money_str(amount),
                    "entry_fingerprint": fingerprint,
                }
            except HTTPException as hex_exc:
                conn.rollback()
                return {"status": "failed", "error": f"HTTP error: {hex_exc.detail}"}
            except Exception as exc:
                conn.rollback()
                return {"status": "failed", "error": f"Database error: {str(exc)}"}
    except Exception as exc:
        return {"status": "failed", "error": f"Processing error: {str(exc)}"}


@app.post("/api/v1/ledger/upload-photo-batch")
async def upload_photo_batch(
    files: List[UploadFile] = File(...),
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    """Iron-SIGHT Batch Vision Processor: M3-optimized parallel receipt ingestion.

    Drop 10-20 receipt photos → Accord auto-processes in parallel using
    4 performance cores (llava vision) + 4 efficiency cores (Tesseract OCR).
    Temporary buffering via 2GB RAM disk at /Volumes/AccordCache/receipt_buffer.
    Result: 10 journal entries posted to PostgreSQL in ~30 seconds.
    """
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)

    if not files or len(files) == 0:
        raise HTTPException(status_code=400, detail="No files provided for batch processing")

    if len(files) > 100:
        raise HTTPException(status_code=413, detail="Batch size limited to 100 images")

    # Validate all files upfront
    for file in files:
        if not file.filename:
            raise HTTPException(status_code=400, detail="Missing file name in batch")
        content_type = (file.content_type or "").lower()
        if not (
            content_type.startswith("image/")
            or file.filename.lower().endswith((".png", ".jpg", ".jpeg", ".webp"))
        ):
            raise HTTPException(status_code=422, detail=f"Invalid file type: {file.filename}")

    RECEIPT_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    RAM_DISK_BUFFER.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []

    # Sequential staging to RAM disk (CPU-friendly I/O)
    staged_images: list[Path] = []
    for file in files:
        try:
            raw = await file.read()
            if len(raw) == 0:
                failed.append({"filename": file.filename or "unknown", "error": "Empty file"})
                continue

            ext = Path(file.filename).suffix.lower() or ".jpg"
            staged_name = f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex}{ext}"
            staged_path = RAM_DISK_BUFFER / staged_name
            staged_path.write_bytes(raw)
            staged_images.append(staged_path)
        except Exception as exc:
            failed.append({"filename": file.filename or "unknown", "error": str(exc)})

    if not staged_images:
        raise HTTPException(status_code=400, detail="No valid images to process")

    # Iron-SIGHT parallel execution: ProcessPoolExecutor saturates M3 8-core
    with ProcessPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as executor:
        futures = [
            executor.submit(process_receipt_batch_worker, str(staged_path), admin_id) for staged_path in staged_images
        ]

        for future in futures:
            try:
                result = future.result(timeout=120)
                if result["status"] == "processed":
                    results.append(result)
                else:
                    failed.append(result)
            except Exception as exc:
                failed.append({"error": f"Executor timeout/error: {str(exc)}"})

    # Cleanup RAM disk buffer
    try:
        for staged_path in staged_images:
            if staged_path.exists():
                staged_path.unlink()
    except Exception:
        pass  # Non-critical cleanup failure

    return {
        "status": "batch_processed",
        "total_processed": len(results),
        "total_failed": len(failed),
        "results": results,
        "failed": failed if failed else None,
        "batch_integrity": f"{len(results)}/{len(results) + len(failed)} entries successfully posted",
        "processor": "Iron-SIGHT M3 Parallel Engine (8-core + RAM disk buffer)",
    }


@app.post("/api/v1/ledger/export-tally-bulk")
def export_tally_bulk(
    entry_ids: List[int],
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> Response:
    """Iron-SIGHT Bulk Tally Export: Aggregate 1000+ entries into master XML.

    Combines multiple journal entries into a single Tally-Prime compliant XML.
    Batch header includes entry count, timestamp, and SHA-256 integrity hash.
    Ready for bulk import into Tally Prime 4.0+.
    """
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)

    if not entry_ids or len(entry_ids) == 0:
        raise HTTPException(status_code=400, detail="No entry IDs provided")

    if len(entry_ids) > 10000:
        raise HTTPException(status_code=413, detail="Bulk export limited to 10000 entries")

    with closing(get_conn()) as conn:
        try:
            # Fetch all entries in order
            entries = conn.execute(
                f"""
                SELECT id, date, reference, description, status
                FROM journal_entries
                WHERE id IN ({','.join('?' * len(entry_ids))})
                ORDER BY date ASC, id ASC
                """,
                tuple(entry_ids),
            ).fetchall()

            if len(entries) == 0:
                raise HTTPException(status_code=404, detail="No entries found for export")

            if len(entries) != len(entry_ids):
                raise HTTPException(
                    status_code=400,
                    detail=f"Found {len(entries)} of {len(entry_ids)} requested entries",
                )

            # Build Tally-compliant master envelope
            envelope = ET.Element("ENVELOPE")
            header = ET.SubElement(envelope, "HEADER")
            ET.SubElement(header, "TALLYREQUEST").text = "Import Data"

            body = ET.SubElement(envelope, "BODY")
            import_data = ET.SubElement(body, "IMPORTDATA")
            request_desc = ET.SubElement(import_data, "REQUESTDESC")
            ET.SubElement(request_desc, "REPORTNAME").text = "Vouchers"

            request_data = ET.SubElement(import_data, "REQUESTDATA")
            tally_message = ET.SubElement(request_data, "TALLYMESSAGE")

            # Batch metadata for tracking
            batch_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            batch_id = f"ACCORD-BULK-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

            total_debit = Decimal("0")
            total_credit = Decimal("0")

            for entry in entries:
                lines = conn.execute(
                    """
                    SELECT jl.debit, jl.credit, a.name
                    FROM journal_lines jl
                    JOIN accounts a ON a.id = jl.account_id
                    WHERE jl.entry_id = ?
                    ORDER BY jl.id ASC
                    """,
                    (entry["id"],),
                ).fetchall()

                if not lines:
                    continue  # Skip entries with no lines

                voucher = ET.SubElement(
                    tally_message, "VOUCHER", {"VCHTYPE": "Journal", "ACTION": "Create"}
                )
                ET.SubElement(voucher, "DATE").text = str(entry["date"]).replace("-", "")
                ET.SubElement(voucher, "NARRATION").text = str(
                    entry["description"] or "Vision Ledger Batch Import"
                )
                ET.SubElement(voucher, "VOUCHERNUMBER").text = str(entry["reference"])
                ET.SubElement(voucher, "BATCHID").text = batch_id
                ET.SubElement(voucher, "BATCHTS").text = batch_ts

                for line in lines:
                    amount = money(line["debit"]) - money(line["credit"])
                    total_debit += money(line["debit"])
                    total_credit += money(line["credit"])

                    ledger_entry = ET.SubElement(voucher, "ALLLEDGERENTRIES.LIST")
                    ET.SubElement(ledger_entry, "LEDGERNAME").text = str(line["name"])
                    ET.SubElement(ledger_entry, "ISDEEMEDPOSITIVE").text = (
                        "Yes" if amount < 0 else "No"
                    )
                    ET.SubElement(ledger_entry, "AMOUNT").text = f"{amount:.4f}"

            # Check balance
            is_balanced = total_debit == total_credit
            integrity_hash = sha256(
                json.dumps(
                    {
                        "batch_id": batch_id,
                        "entries": len(entries),
                        "total_debit": str(total_debit),
                        "total_credit": str(total_credit),
                        "balanced": is_balanced,
                    }
                ).encode()
            ).hexdigest()

            # Add balance footer
            balance_elem = ET.SubElement(tally_message, "BATCHBALANCE")
            ET.SubElement(balance_elem, "TOTALDEBIT").text = f"{total_debit:.4f}"
            ET.SubElement(balance_elem, "TOTALCREDIT").text = f"{total_credit:.4f}"
            ET.SubElement(balance_elem, "BALANCED").text = "Yes" if is_balanced else "No"
            ET.SubElement(balance_elem, "INTEGRITYCHECK").text = integrity_hash

            xml_bytes = ET.tostring(envelope, encoding="utf-8", xml_declaration=True)
            TALLY_EXPORT_DIR.mkdir(parents=True, exist_ok=True)

            export_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            filename = f"Accord_Tally_Bulk_Export_{export_ts}_{len(entries)}_entries.xml"
            export_path = TALLY_EXPORT_DIR / filename
            export_path.write_bytes(xml_bytes)
            master_path = TALLY_EXPORT_DIR / "MASTER_VOUCHER.xml"
            master_path.write_bytes(xml_bytes)

            # Log audit
            log_audit(
                conn,
                table_name="journal_entries",
                record_id=entry_ids[0] if entry_ids else 0,
                action="TALLY_BULK_XML_EXPORT",
                old_value=None,
                new_value={
                    "batch_id": batch_id,
                    "entry_count": len(entries),
                    "total_debit": money_str(total_debit),
                    "total_credit": money_str(total_credit),
                    "balanced": is_balanced,
                    "integrity_check": integrity_hash,
                    "export_file": str(export_path),
                    "master_export_file": str(master_path),
                    "actor_role": role,
                },
                user_id=admin_id,
                high_priority=True,
            )
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception as exc:
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Bulk export failed: {exc}") from exc

    return Response(
        content=xml_bytes,
        media_type="application/xml",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "X-Accord-Export-Path": str(export_path),
            "X-Accord-Master-Export-Path": str(master_path),
            "X-Batch-ID": batch_id,
            "X-Batch-Entries": str(len(entries)),
            "X-Batch-Balanced": "true" if is_balanced else "false",
            "X-Batch-Integrity": integrity_hash,
        },
    )


@app.post("/api/v1/ledger/tally-sync-final")
def tally_sync_final(
    payload: TallySyncFinalIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> Response:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)

    entry_ids = sorted({int(item) for item in payload.entry_ids if int(item) > 0})
    if not entry_ids:
        raise HTTPException(status_code=422, detail="At least one valid entry_id is required")

    with closing(get_conn()) as conn:
        entries = conn.execute(
            f"""
            SELECT id, date, reference, description, status, voucher_type
            FROM journal_entries
            WHERE id IN ({','.join('?' * len(entry_ids))})
            ORDER BY date ASC, id ASC
            """,
            tuple(entry_ids),
        ).fetchall()
        if len(entries) != len(entry_ids):
            raise HTTPException(status_code=404, detail="Some requested entries were not found")

        envelope = ET.Element("ENVELOPE")
        header = ET.SubElement(envelope, "HEADER")
        ET.SubElement(header, "TALLYREQUEST").text = "Import Data"
        body = ET.SubElement(envelope, "BODY")
        import_data = ET.SubElement(body, "IMPORTDATA")
        request_desc = ET.SubElement(import_data, "REQUESTDESC")
        ET.SubElement(request_desc, "REPORTNAME").text = "Vouchers"
        request_data = ET.SubElement(import_data, "REQUESTDATA")
        tally_message = ET.SubElement(request_data, "TALLYMESSAGE")

        batch_material = []
        for entry in entries:
            lines = conn.execute(
                """
                SELECT jl.debit, jl.credit, a.name
                FROM journal_lines jl
                JOIN accounts a ON a.id = jl.account_id
                WHERE jl.entry_id = ?
                ORDER BY jl.id ASC
                """,
                (int(entry["id"]),),
            ).fetchall()
            if not lines:
                continue

            image_row = conn.execute(
                """
                SELECT file_path
                FROM receipt_imports
                WHERE entry_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(entry["id"]),),
            ).fetchone()
            image_b64 = ""
            image_name = ""
            if image_row is not None:
                image_path = Path(str(image_row["file_path"]))
                if image_path.exists():
                    image_b64 = base64.b64encode(image_path.read_bytes()).decode("utf-8")
                    image_name = image_path.name

            vch_type = voucher_service.ensure_golden_six(str(entry["voucher_type"] or "JOURNAL")).title()
            voucher = ET.SubElement(tally_message, "VOUCHER", {"VCHTYPE": vch_type, "ACTION": "Create"})
            ET.SubElement(voucher, "DATE").text = str(entry["date"]).replace("-", "")
            ET.SubElement(voucher, "VOUCHERNUMBER").text = str(entry["reference"])
            ET.SubElement(voucher, "NARRATION").text = str(entry["description"] or "Accord Tally Sync Final")

            udf_voucher = ET.SubElement(voucher, "UDF:Voucher")
            ET.SubElement(udf_voucher, "UDF:ENTRYID").text = str(entry["id"])
            ET.SubElement(udf_voucher, "UDF:VOUCHERTYPE").text = vch_type.upper()
            ET.SubElement(udf_voucher, "UDF:ATTACHMENTNAME").text = image_name
            ET.SubElement(udf_voucher, "UDF:ATTACHMENTBASE64").text = image_b64

            for line in lines:
                amount = money(line["debit"]) - money(line["credit"])
                ledger_entry = ET.SubElement(voucher, "ALLLEDGERENTRIES.LIST")
                ET.SubElement(ledger_entry, "LEDGERNAME").text = str(line["name"])
                ET.SubElement(ledger_entry, "ISDEEMEDPOSITIVE").text = "Yes" if amount < 0 else "No"
                ET.SubElement(ledger_entry, "AMOUNT").text = f"{amount:.4f}"

            batch_material.append(
                {
                    "id": int(entry["id"]),
                    "reference": str(entry["reference"]),
                    "voucher_type": vch_type.upper(),
                    "attachment": image_name,
                }
            )

        payload_hash = sha256(json.dumps(batch_material, sort_keys=True).encode("utf-8")).hexdigest()
        xml_bytes = ET.tostring(envelope, encoding="utf-8", xml_declaration=True)
        filename = f"Accord_Tally_Sync_Final_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xml"
        TALLY_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        export_path = TALLY_EXPORT_DIR / filename
        export_path.write_bytes(xml_bytes)

        log_audit(
            conn,
            table_name="journal_entries",
            record_id=entry_ids[0],
            action="TALLY_SYNC_FINAL",
            old_value=None,
            new_value={
                "entry_ids": entry_ids,
                "entries": len(batch_material),
                "payload_hash": payload_hash,
                "export_path": str(export_path),
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    return Response(
        content=xml_bytes,
        media_type="application/xml",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "X-Accord-Export-Path": str(export_path),
            "X-Accord-Payload-Hash": payload_hash,
        },
    )


@app.post("/api/v1/inventory/batches")
def upsert_inventory_batch(
    payload: InventoryBatchUpsertIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    _, _, _, inventory_layer = ensure_service_layer()

    try:
        return inventory_layer.upsert_batch(
            sku_code=payload.sku_code,
            sku_name=payload.sku_name,
            batch_code=payload.batch_code,
            hsn_code=validate_hsn_code_format(payload.hsn_code),
            gst_rate=money(payload.gst_rate),
            quantity=money(payload.quantity),
            unit_cost=money(payload.unit_cost),
            expiry_date=payload.expiry_date,
            created_by=admin_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.get("/api/v1/inventory/batches")
def get_inventory_batches(
    include_expired: bool = True,
    limit: int = 200,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin", "ca"})
    require_admin_id(x_admin_id)
    _, _, _, inventory_layer = ensure_service_layer()
    items = inventory_layer.list_batches(include_expired=include_expired, limit=limit)
    return {
        "status": "ok",
        "count": len(items),
        "items": items,
    }


@app.get("/api/accounts")
def get_accounts() -> list[dict[str, Any]]:
    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT id, name, type, balance
            FROM accounts
            ORDER BY type, name
            """
        ).fetchall()

    return [
        {
            **dict(row),
            "balance": money_str(row["balance"]),
        }
        for row in rows
    ]


@app.get("/api/ledger")
def get_ledger(limit: int = 50) -> list[dict[str, Any]]:
    safe_limit = min(max(limit, 1), 200)

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        enforce_auto_relock(conn, datetime.utcnow())
        conn.commit()

        entries = conn.execute(
            """
            SELECT id, date, reference, description, status, reversal_of_id, created_at
            FROM journal_entries
            ORDER BY date DESC, id DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

        result: list[dict[str, Any]] = []
        for entry in entries:
            lines = conn.execute(
                """
                SELECT jl.id, jl.entry_id, jl.account_id, a.name AS account_name, jl.debit, jl.credit
                FROM journal_lines jl
                JOIN accounts a ON a.id = jl.account_id
                WHERE jl.entry_id = ?
                ORDER BY jl.id ASC
                """,
                (entry["id"],),
            ).fetchall()

            period_label, period_locked = get_period_meta(conn, entry["date"])
            audit_header = get_audit_header(entry["reference"], period_label, period_locked, lines)

            result.append(
                {
                    **dict(entry),
                    "period_status": "LOCKED" if period_locked else "OPEN",
                    "is_auditable": entry["status"] == "POSTED" and period_locked,
                    "audit_header": audit_header,
                    "lines": [
                        {
                            **dict(line),
                            "debit": money_str(line["debit"]),
                            "credit": money_str(line["credit"]),
                        }
                        for line in lines
                    ],
                }
            )

    return result


@app.get("/api/v1/periods")
def get_periods() -> list[dict[str, Any]]:
    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        enforce_auto_relock(conn, datetime.utcnow())
        conn.commit()

        rows = conn.execute(
            """
            SELECT id, period_name, start_date, end_date, is_locked, unlocked_until
            FROM financial_periods
            ORDER BY start_date ASC
            """
        ).fetchall()
    return [
        {
            **dict(row),
            "is_locked": bool(row["is_locked"]),
        }
        for row in rows
    ]


@app.post("/api/journal", status_code=201)
def post_journal(payload: JournalEntryIn) -> dict[str, Any]:
    _, _ = validate_line_items(payload.lines)

    with closing(get_conn()) as conn:
        try:
            conn.execute("BEGIN")
            enforce_auto_relock(conn, datetime.utcnow())
            check_period_lock(conn, payload.date)

            if payload.is_b2b:
                if not payload.hsn_code:
                    raise HTTPException(status_code=400, detail="HSN code is mandatory for B2B transactions")
                hsn_code = validate_hsn_code_format(payload.hsn_code)
                hsn_rate = get_hsn_rate(conn, hsn_code)
            else:
                hsn_code = payload.hsn_code.strip() if payload.hsn_code else None
                hsn_rate = None

            supply_source = normalize_supply_source(payload.supply_source)
            ims_status = normalize_ims_status(payload.ims_status)
            vendor_legal_name = payload.vendor_legal_name.strip() if payload.vendor_legal_name else None
            vendor_filed_at = payload.vendor_gstr1_filed_at.isoformat() if payload.vendor_gstr1_filed_at else None

            company_state = payload.company_state_code.strip() if payload.company_state_code else None
            counterparty_state = payload.counterparty_state_code.strip() if payload.counterparty_state_code else None
            is_inter_state = bool(company_state and counterparty_state and company_state != counterparty_state)

            counterparty_gstin = validate_gstin(payload.counterparty_gstin) if payload.counterparty_gstin else None
            eco_gstin = validate_gstin(payload.eco_gstin) if payload.eco_gstin else None

            if payload.is_b2b and counterparty_gstin is None:
                raise HTTPException(status_code=422, detail="counterparty_gstin is mandatory for B2B transactions")

            if (not payload.is_b2b) and ims_status != "PENDING":
                raise HTTPException(status_code=422, detail="ims_status is only applicable for B2B transactions")

            if supply_source == "ECO" and eco_gstin is None:
                raise HTTPException(status_code=422, detail="eco_gstin is mandatory when supply_source is ECO")

            for line in payload.lines:
                if not account_exists(conn, line.account_id):
                    raise HTTPException(status_code=400, detail=f"Account {line.account_id} does not exist")

            reference = next_journal_reference(conn, payload.date)

            cursor = conn.execute(
                """
                INSERT INTO journal_entries(
                    date,
                    reference,
                    description,
                    company_state_code,
                    counterparty_state_code,
                    counterparty_gstin,
                    eco_gstin,
                    supply_source,
                    ims_status,
                    vendor_legal_name,
                    vendor_gstr1_filed_at,
                    status,
                    reversal_of_id,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'POSTED', NULL, ?)
                """,
                (
                    payload.date.isoformat(),
                    reference,
                    payload.description.strip(),
                    company_state,
                    counterparty_state,
                    counterparty_gstin,
                    eco_gstin,
                    supply_source,
                    ims_status,
                    vendor_legal_name,
                    vendor_filed_at,
                    datetime.utcnow().isoformat(timespec="seconds") + "Z",
                ),
            )
            entry_id = cursor.lastrowid

            taxable_value = compute_taxable_value(conn, payload.lines)
            tax_amount = money((taxable_value * (hsn_rate or Decimal("0"))) / Decimal("100"))

            for line in payload.lines:
                debit = money(line.debit)
                credit = money(line.credit)

                conn.execute(
                    """
                    INSERT INTO journal_lines(entry_id, account_id, debit, credit)
                    VALUES (?, ?, ?, ?)
                    """,
                    (entry_id, line.account_id, money_str(debit), money_str(credit)),
                )

                update_account_balance(conn, line.account_id, debit, credit)

            fingerprint = stamp_entry_fingerprint(conn, entry_id)

            conn.execute(
                """
                INSERT INTO tax_ledger(
                    entry_id,
                    hsn_code,
                    gst_rate_snapshot,
                    taxable_value,
                    tax_amount,
                    supply_type,
                    is_inter_state,
                    supply_source,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entry_id,
                    hsn_code,
                    money_str(hsn_rate if hsn_rate is not None else Decimal("0")),
                    money_str(taxable_value),
                    money_str(tax_amount),
                    "B2B" if payload.is_b2b else "B2CS",
                    1 if is_inter_state else 0,
                    supply_source,
                    datetime.utcnow().isoformat(timespec="seconds") + "Z",
                ),
            )

            log_audit(
                conn,
                table_name="journal_entries",
                record_id=entry_id,
                action="CREATE",
                old_value=None,
                new_value={
                    "entry_id": entry_id,
                    "reference": reference,
                    "date": payload.date.isoformat(),
                    "is_b2b": payload.is_b2b,
                    "hsn_code": hsn_code,
                    "hsn_rate": money_str(hsn_rate) if hsn_rate is not None else None,
                    "taxable_value": money_str(taxable_value),
                    "tax_amount": money_str(tax_amount),
                    "company_state_code": company_state,
                    "counterparty_state_code": counterparty_state,
                    "counterparty_gstin": counterparty_gstin,
                    "eco_gstin": eco_gstin,
                    "is_inter_state": is_inter_state,
                    "supply_source": supply_source,
                    "ims_status": ims_status,
                    "vendor_legal_name": vendor_legal_name,
                    "vendor_gstr1_filed_at": vendor_filed_at,
                    "entry_fingerprint": fingerprint,
                },
            )

            if payload.is_b2b and counterparty_gstin:
                recompute_vendor_trust(conn, counterparty_gstin)

            conn.commit()

        except HTTPException:
            conn.rollback()
            raise
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to post journal entry: {exc}") from exc

    return {
        "message": "Journal entry posted",
        "entry_id": entry_id,
        "reference": reference,
        "period_status": "OPEN",
        "is_auditable": False,
        "entry_fingerprint": fingerprint,
    }


@app.post("/api/journal/{entry_id}/reverse", status_code=201)
def reverse_journal(entry_id: int, payload: ReversalIn) -> dict[str, Any]:
    with closing(get_conn()) as conn:
        try:
            conn.execute("BEGIN")
            enforce_auto_relock(conn, datetime.utcnow())
            reversal_date = date.today()
            check_period_lock(conn, reversal_date)

            original = conn.execute(
                """
                SELECT id, date, reference, description, status
                FROM journal_entries
                WHERE id = ?
                """,
                (entry_id,),
            ).fetchone()

            if original is None:
                raise HTTPException(status_code=404, detail="Journal entry not found")

            if original["status"] == "REVERSED":
                raise HTTPException(status_code=400, detail="Journal entry is already reversed")

            original_lines = conn.execute(
                """
                SELECT account_id, debit, credit
                FROM journal_lines
                WHERE entry_id = ?
                ORDER BY id ASC
                """,
                (entry_id,),
            ).fetchall()

            reversal_ref = f"REV-{original['reference']}"
            cursor = conn.execute(
                """
                INSERT INTO journal_entries(date, reference, description, status, reversal_of_id, created_at)
                VALUES (?, ?, ?, 'POSTED', ?, ?)
                """,
                (
                    reversal_date.isoformat(),
                    reversal_ref[:100],
                    f"Reversal of entry {entry_id}: {payload.reason.strip()}",
                    entry_id,
                    datetime.utcnow().isoformat(timespec="seconds") + "Z",
                ),
            )
            reversal_id = cursor.lastrowid

            for line in original_lines:
                reversal_debit = money(line["credit"])
                reversal_credit = money(line["debit"])
                conn.execute(
                    """
                    INSERT INTO journal_lines(entry_id, account_id, debit, credit)
                    VALUES (?, ?, ?, ?)
                    """,
                    (reversal_id, line["account_id"], money_str(reversal_debit), money_str(reversal_credit)),
                )
                update_account_balance(conn, line["account_id"], reversal_debit, reversal_credit)

            reversal_fingerprint = stamp_entry_fingerprint(conn, reversal_id)

            conn.execute("UPDATE journal_entries SET status = 'REVERSED' WHERE id = ?", (entry_id,))

            log_audit(
                conn,
                table_name="journal_entries",
                record_id=entry_id,
                action="REVERSE",
                old_value={"status": "POSTED"},
                new_value={"status": "REVERSED", "reversal_entry_id": reversal_id, "reason": payload.reason.strip()},
            )

            log_audit(
                conn,
                table_name="journal_entries",
                record_id=reversal_id,
                action="CREATE",
                old_value=None,
                new_value={
                    "reversal_of_id": entry_id,
                    "reason": payload.reason.strip(),
                    "entry_fingerprint": reversal_fingerprint,
                },
            )

            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to reverse journal entry: {exc}") from exc

    return {
        "message": "Journal entry reversed",
        "entry_id": entry_id,
        "reversal_entry_id": reversal_id,
        "period_status": "OPEN",
        "is_auditable": False,
        "entry_fingerprint": reversal_fingerprint,
    }


@app.post("/api/v1/periods/{period_id}/lock")
def lock_period(period_id: int, payload: PeriodLockIn, x_role: str | None = Header(default=None, alias="X-Role")) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})

    with closing(get_conn()) as conn:
        try:
            conn.execute("BEGIN")
            enforce_auto_relock(conn, datetime.utcnow())
            row = conn.execute(
                """
                SELECT id, period_name, is_locked
                FROM financial_periods
                WHERE id = ?
                """,
                (period_id,),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Period not found")
            if row["is_locked"] == 1:
                raise HTTPException(status_code=400, detail="Period already locked")

            conn.execute("UPDATE financial_periods SET is_locked = 1, unlocked_until = NULL WHERE id = ?", (period_id,))
            log_audit(
                conn,
                table_name="financial_periods",
                record_id=period_id,
                action="LOCK",
                old_value={"is_locked": False},
                new_value={
                    "is_locked": True,
                    "period_name": row["period_name"],
                    "reason": payload.reason.strip(),
                    "role": role,
                },
            )
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to lock period: {exc}") from exc

    return {"message": "Period locked", "period_id": period_id}


@app.post("/api/v1/periods/{period_id}/unlock")
def unlock_period(period_id: int, payload: PeriodUnlockIn, x_role: str | None = Header(default=None, alias="X-Role")) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})

    with closing(get_conn()) as conn:
        try:
            conn.execute("BEGIN")
            enforce_auto_relock(conn, datetime.utcnow())
            row = conn.execute(
                """
                SELECT id, period_name, is_locked
                FROM financial_periods
                WHERE id = ?
                """,
                (period_id,),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="Period not found")
            if row["is_locked"] == 0:
                raise HTTPException(status_code=400, detail="Period is not locked")

            unlocked_until = datetime.utcnow().replace(microsecond=0) + timedelta(hours=payload.extension_hours)

            conn.execute(
                "UPDATE financial_periods SET is_locked = 0, unlocked_until = ? WHERE id = ?",
                (unlocked_until.isoformat(timespec="seconds") + "Z", period_id),
            )
            log_audit(
                conn,
                table_name="financial_periods",
                record_id=period_id,
                action="UNLOCK",
                old_value={"is_locked": True},
                new_value={
                    "is_locked": False,
                    "period_name": row["period_name"],
                    "admin_reason": payload.admin_reason.strip(),
                    "unlocked_until": unlocked_until.isoformat(timespec="seconds") + "Z",
                    "extension_hours": payload.extension_hours,
                    "role": role,
                },
                high_priority=True,
            )
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to unlock period: {exc}") from exc

    return {"message": "Period unlocked", "period_id": period_id}


@app.get("/api/v1/journal/{entry_id}/audit-trail")
def get_journal_audit_trail(entry_id: int) -> dict[str, Any]:
    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        enforce_auto_relock(conn, datetime.utcnow())
        conn.commit()

        entry = conn.execute(
            """
            SELECT je.id, je.date, je.reference, je.description, je.status, je.reversal_of_id, je.created_at,
                   fp.id AS period_id, fp.period_name, fp.is_locked AS period_is_locked
            FROM journal_entries je
            LEFT JOIN financial_periods fp
              ON fp.start_date <= je.date AND fp.end_date >= je.date
            WHERE je.id = ?
            """,
            (entry_id,),
        ).fetchone()

        if entry is None:
            raise HTTPException(status_code=404, detail="Journal entry not found")

        lines = conn.execute(
            """
            SELECT jl.id, jl.account_id, a.name AS account_name, jl.debit, jl.credit
            FROM journal_lines jl
            JOIN accounts a ON a.id = jl.account_id
            WHERE jl.entry_id = ?
            ORDER BY jl.id ASC
            """,
            (entry_id,),
        ).fetchall()

        reversal = conn.execute(
            """
            SELECT id, date, reference, status, created_at
            FROM journal_entries
            WHERE reversal_of_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (entry_id,),
        ).fetchone()

        audits = conn.execute(
            """
            SELECT id, table_name, record_id, user_id, action, high_priority, old_value, new_value, created_at
            FROM audit_edit_logs
            WHERE (table_name = 'journal_entries' AND record_id = ?)
               OR (table_name = 'journal_entries' AND record_id = ?)
               OR (table_name = 'financial_periods' AND record_id = ?)
            ORDER BY created_at ASC, id ASC
            """,
            (
                entry_id,
                reversal["id"] if reversal else -1,
                entry["period_id"] if entry["period_id"] is not None else -1,
            ),
        ).fetchall()

    period_locked = bool(entry["period_is_locked"])
    audit_header = get_audit_header(entry["reference"], entry["period_name"], period_locked, lines)

    return {
        "entry": {
            **dict(entry),
            "period_status": "LOCKED" if entry["period_is_locked"] else "OPEN",
            "is_auditable": entry["status"] == "POSTED" and period_locked,
            "audit_header": audit_header,
        },
        "lines": [
            {
                **dict(line),
                "debit": money_str(line["debit"]),
                "credit": money_str(line["credit"]),
            }
            for line in lines
        ],
        "reversal": dict(reversal) if reversal else None,
        "audit_logs": [
            {
                **dict(audit),
                "high_priority": bool(audit["high_priority"]),
            }
            for audit in audits
        ],
    }


def _resolve_month_window(month: str | None) -> tuple[date, date, str]:
    if month:
        try:
            base = datetime.strptime(month, "%Y-%m").date().replace(day=1)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="month must be in YYYY-MM format") from exc
    else:
        base = date.today().replace(day=1)

    if base.month == 12:
        next_month = date(base.year + 1, 1, 1)
    else:
        next_month = date(base.year, base.month + 1, 1)
    period_end = next_month - timedelta(days=1)
    return base, period_end, base.strftime("%b %Y")


def _fetch_monthly_ledger_entries(*, period_start: date, period_end: date) -> list[dict[str, Any]]:
    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT je.id,
                   je.date,
                   je.reference,
                   je.description,
                   COALESCE(SUM(CAST(jl.debit AS REAL)), 0) AS total_debit,
                   COALESCE(SUM(CAST(jl.credit AS REAL)), 0) AS total_credit
            FROM journal_entries je
            JOIN journal_lines jl ON jl.entry_id = je.id
            WHERE je.status = 'POSTED'
              AND je.date >= ?
              AND je.date <= ?
            GROUP BY je.id, je.date, je.reference, je.description
            ORDER BY je.date ASC, je.id ASC
            """,
            (period_start.isoformat(), period_end.isoformat()),
        ).fetchall()

    return [
        {
            "id": int(row["id"]),
            "date": str(row["date"]),
            "reference": str(row["reference"] or ""),
            "description": str(row["description"] or ""),
            "total_debit": money_str(row["total_debit"]),
            "total_credit": money_str(row["total_credit"]),
        }
        for row in rows
    ]


def _fetch_latest_market_context() -> dict[str, Any]:
    with closing(get_conn()) as conn:
        latest_market = conn.execute(
            """
            SELECT source_kind, analysis_json, created_at
            FROM market_trend_reports
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()

    if latest_market is None:
        return {
            "risk_level": "MEDIUM",
            "source_kind": "NONE",
            "created_at": "",
            "trend_summary": "No market trend reports available",
        }

    parsed_market: dict[str, Any] = {}
    try:
        parsed = json.loads(str(latest_market["analysis_json"] or "{}"))
        parsed_market = parsed if isinstance(parsed, dict) else {}
    except Exception:  # noqa: BLE001
        parsed_market = {}

    return {
        "risk_level": str(parsed_market.get("risk_level") or "MEDIUM").upper(),
        "source_kind": str(latest_market["source_kind"] or "UNKNOWN"),
        "created_at": str(latest_market["created_at"] or ""),
        "trend_summary": str(parsed_market.get("trend_summary") or "Market trend summary unavailable"),
    }


@app.get("/api/v1/reports/ca/monthly")
def get_ca_monthly_report(
    month: str | None = None,
    limit: int = 300,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    safe_limit = min(max(limit, 10), 2000)

    period_start, period_end, period_label = _resolve_month_window(month)
    entries = _fetch_monthly_ledger_entries(period_start=period_start, period_end=period_end)[:safe_limit]
    heatmap = get_ca_heatmap(limit=100, x_role=x_role, x_admin_id=x_admin_id)
    friday_summary = get_friday_summary(as_of_date=period_end)
    market_context = _fetch_latest_market_context()

    report_layer = ensure_report_service()
    payload = report_layer.build_ca_monthly_payload(
        period_start=period_start,
        period_end=period_end,
        ledger_entries=entries,
        heatmap=heatmap,
        friday_summary=friday_summary,
        market_context=market_context,
    )
    payload["report_title"] = f"Accord Monthly Risk & Compliance Report - {period_label}"

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        log_audit(
            conn,
            table_name="reports",
            record_id=0,
            action="CA_MONTHLY_REPORT_GENERATED",
            old_value=None,
            new_value={
                "month": month or period_start.strftime("%Y-%m"),
                "period_from": period_start.isoformat(),
                "period_to": period_end.isoformat(),
                "entries_count": len(entries),
                "generated_by": admin_id,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    return payload


@app.get("/api/v1/reports/variance-analysis")
async def get_variance_analysis(
    spv_id: str | None = None,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)

    analyzer = ensure_variance_analyzer_service()
    payload = analyzer.get_budget_vs_actual(spv_id=(spv_id or "SPV-DEFAULT"))
    markdown = await analyzer.generate_cfo_markdown(payload=payload)

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        log_audit(
            conn,
            table_name="reports",
            record_id=0,
            action="SPV_VARIANCE_ANALYSIS_GENERATED",
            old_value=None,
            new_value={
                "spv_id": payload.get("spv_id", "SPV-DEFAULT"),
                "period": payload.get("period", {}),
                "top_overruns": payload.get("top_overruns", []),
                "generated_by": admin_id,
            },
            user_id=admin_id,
            high_priority=False,
        )
        conn.commit()

    return {
        "status": "ok",
        "spv_id": payload.get("spv_id", "SPV-DEFAULT"),
        "period": payload.get("period", {}),
        "budget_vs_actual": payload.get("items", []),
        "analysis_markdown": markdown,
        "model": VARIANCE_MODEL,
    }


@app.get("/api/v1/reports/ca/monthly/pdf")
def get_ca_monthly_report_pdf(
    month: str | None = None,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> Response:
    payload = get_ca_monthly_report(month=month, x_role=x_role, x_admin_id=x_admin_id)
    report_layer = ensure_report_service()
    content = report_layer.generate_ca_monthly_pdf(payload)
    filename = f"Accord_CA_Monthly_Report_{(month or date.today().strftime('%Y-%m'))}.pdf"
    return Response(
        content=content,
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/api/v1/reports/ca/monthly/excel")
def get_ca_monthly_report_excel(
    month: str | None = None,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> Response:
    payload = get_ca_monthly_report(month=month, x_role=x_role, x_admin_id=x_admin_id)
    report_layer = ensure_report_service()
    content = report_layer.generate_ca_monthly_excel(payload)
    filename = f"Accord_CA_Monthly_Report_{(month or date.today().strftime('%Y-%m'))}.xlsx"
    return Response(
        content=content,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.get("/api/v1/reports/gstr1-preview")
def get_gstr1_preview(from_date: date | None = None, to_date: date | None = None) -> dict[str, Any]:
    report_from = from_date or date.today().replace(day=1)
    report_to = to_date or date.today()

    if report_from > report_to:
        raise HTTPException(status_code=400, detail="from_date cannot be later than to_date")

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        enforce_auto_relock(conn, datetime.utcnow())
        conn.commit()

        tax_rows = conn.execute(
            """
             SELECT je.reference,
                 je.date,
                 tl.supply_type,
                 tl.is_inter_state,
                 tl.supply_source,
                   COALESCE(tl.hsn_code, 'UNSPECIFIED') AS hsn_code,
                   COALESCE(hm.description, 'Unspecified HSN') AS description,
                   COALESCE(hm.uqc, 'NOS') AS uqc,
                   tl.gst_rate_snapshot,
                   tl.taxable_value,
                   tl.tax_amount
            FROM tax_ledger tl
            JOIN journal_entries je ON je.id = tl.entry_id
            LEFT JOIN hsn_master hm ON hm.code = tl.hsn_code
            WHERE je.date >= ?
              AND je.date <= ?
            ORDER BY je.date ASC, tl.id ASC
            """,
            (report_from.isoformat(), report_to.isoformat()),
        ).fetchall()

    b2b_rows: list[dict[str, Any]] = []
    b2cl_rows: list[dict[str, Any]] = []
    b2cs_bucket: dict[str, dict[str, Decimal | int]] = {}
    hsn_bucket: dict[tuple[str, str, str, str], dict[str, Decimal | int]] = {}
    eco_bucket: dict[tuple[str, str], dict[str, Decimal | int]] = {}

    for row in tax_rows:
        rate_key = money_str(row["gst_rate_snapshot"])
        rate_value = money(row["gst_rate_snapshot"])
        taxable = money(row["taxable_value"])
        tax_amount = money(row["tax_amount"])
        risk = slab_compliance_risk(rate_value)

        if row["supply_type"] == "B2B":
            b2b_rows.append(
                {
                    "reference_no": row["reference"],
                    "invoice_date": row["date"],
                    "hsn_code": row["hsn_code"],
                    "gst_rate": rate_key,
                    "taxable_value": money_str(taxable),
                    "tax_amount": money_str(tax_amount),
                    "compliance_risk": risk,
                }
            )

        if row["supply_type"] == "B2CS":
            if bool(row["is_inter_state"]) and taxable > B2CL_INTER_STATE_THRESHOLD:
                b2cl_rows.append(
                    {
                        "reference_no": row["reference"],
                        "invoice_date": row["date"],
                        "threshold": money_str(B2CL_INTER_STATE_THRESHOLD),
                        "taxable_value": money_str(taxable),
                        "tax_amount": money_str(tax_amount),
                        "gst_rate": rate_key,
                        "compliance_risk": risk,
                    }
                )
            else:
                bucket = b2cs_bucket.setdefault(
                    rate_key,
                    {
                        "gst_rate": rate_value,
                        "taxable_value": Decimal("0"),
                        "tax_amount": Decimal("0"),
                        "invoice_count": 0,
                    },
                )
                bucket["taxable_value"] = money(bucket["taxable_value"] + taxable)
                bucket["tax_amount"] = money(bucket["tax_amount"] + tax_amount)
                bucket["invoice_count"] = int(bucket["invoice_count"]) + 1

        if row["supply_source"] == "ECO":
            eco_key = (row["supply_type"], rate_key)
            eco = eco_bucket.setdefault(
                eco_key,
                {
                    "taxable_value": Decimal("0"),
                    "tax_amount": Decimal("0"),
                    "invoice_count": 0,
                    "compliance_risk": risk,
                },
            )
            eco["taxable_value"] = money(eco["taxable_value"] + taxable)
            eco["tax_amount"] = money(eco["tax_amount"] + tax_amount)
            eco["invoice_count"] = int(eco["invoice_count"]) + 1

        hsn_key = (str(row["hsn_code"]), str(row["description"]), normalize_uqc(str(row["uqc"])), rate_key)
        hsn_data = hsn_bucket.setdefault(
            hsn_key,
            {
                "taxable_value": Decimal("0"),
                "tax_amount": Decimal("0"),
                "invoice_count": 0,
            },
        )
        hsn_data["taxable_value"] = money(hsn_data["taxable_value"] + taxable)
        hsn_data["tax_amount"] = money(hsn_data["tax_amount"] + tax_amount)
        hsn_data["invoice_count"] = int(hsn_data["invoice_count"]) + 1

    b2cs_rows = sorted(b2cs_bucket.values(), key=lambda item: item["gst_rate"])
    hsn_rows = sorted(hsn_bucket.items(), key=lambda item: item[0][0])

    b2b_rows.sort(key=lambda item: (item["invoice_date"], item["reference_no"]))
    b2cl_rows.sort(key=lambda item: (item["invoice_date"], item["reference_no"]))

    return {
        "schema_version": "1.2-2026",
        "reporting_period": {
            "from_date": report_from.isoformat(),
            "to_date": report_to.isoformat(),
        },
        "sections": {
            "table_4_b2b": "Standard B2B invoices",
            "table_5_b2cl": "Inter-state B2C >= INR 100000",
            "table_7_b2cs": "Intra-state B2C and inter-state B2C below INR 100000",
            "table_12_hsn": "HSN summary with 6-digit compliance checks",
            "table_14a_15a": "E-commerce operator mediated supplies",
        },
        "table_4_b2b": b2b_rows,
        "table_5_b2cl": b2cl_rows,
        "table_7_b2cs": [
            {
                "gst_rate": money_str(row["gst_rate"]),
                "taxable_value": money_str(row["taxable_value"]),
                "tax_amount": money_str(row["tax_amount"]),
                "invoice_count": int(row["invoice_count"]),
                "compliance_risk": slab_compliance_risk(money(row["gst_rate"])),
            }
            for row in b2cs_rows
        ],
        "table_12_hsn": [
            {
                "hsn_code": key[0],
                "description": key[1],
                "uqc": key[2],
                "gst_rate": key[3],
                "taxable_value": money_str(data["taxable_value"]),
                "tax_amount": money_str(data["tax_amount"]),
                "invoice_count": int(data["invoice_count"]),
                "hsn_length_ok": len(key[0]) >= 6 and key[0].isdigit(),
                "compliance_risk": slab_compliance_risk(money(key[3])),
            }
            for key, data in hsn_rows
        ],
        "table_14a_15a": [
            {
                "supply_type": key[0],
                "gst_rate": key[1],
                "taxable_value": money_str(data["taxable_value"]),
                "tax_amount": money_str(data["tax_amount"]),
                "invoice_count": int(data["invoice_count"]),
                "compliance_risk": data["compliance_risk"],
            }
            for key, data in sorted(eco_bucket.items(), key=lambda item: (item[0][0], money(item[0][1])))
        ],
    }


@app.post("/api/v1/reports/gstr1-export")
def post_gstr1_export(payload: GstrExportIn) -> dict[str, Any]:
    report_from = payload.from_date or date.today().replace(day=1)
    report_to = payload.to_date or date.today()

    if report_from > report_to:
        raise HTTPException(status_code=400, detail="from_date cannot be later than to_date")

    risk_refs: set[str] = set()
    b2b_gstin_issues: set[str] = set()
    eco_gstin_issues: set[str] = set()

    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT je.reference,
                   je.counterparty_gstin,
                   je.eco_gstin,
                   tl.supply_type,
                   tl.supply_source,
                   tl.gst_rate_snapshot
            FROM tax_ledger tl
            JOIN journal_entries je ON je.id = tl.entry_id
            WHERE je.date >= ?
              AND je.date <= ?
            """,
            (report_from.isoformat(), report_to.isoformat()),
        ).fetchall()

        for row in rows:
            risk = slab_compliance_risk(money(row["gst_rate_snapshot"]))
            if risk is not None:
                risk_refs.add(str(row["reference"]))

            if row["supply_type"] == "B2B":
                try:
                    if row["counterparty_gstin"] is None:
                        raise HTTPException(status_code=422, detail="counterparty_gstin missing")
                    validate_gstin(str(row["counterparty_gstin"]))
                except HTTPException:
                    b2b_gstin_issues.add(str(row["reference"]))

            if row["supply_source"] == "ECO":
                try:
                    if row["eco_gstin"] is None:
                        raise HTTPException(status_code=422, detail="eco_gstin missing")
                    validate_gstin(str(row["eco_gstin"]))
                except HTTPException:
                    eco_gstin_issues.add(str(row["reference"]))

        if risk_refs or b2b_gstin_issues or eco_gstin_issues:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Export blocked: compliance risks detected.",
                    "risk_references": sorted(risk_refs),
                    "b2b_gstin_issues": sorted(b2b_gstin_issues),
                    "eco_gstin_issues": sorted(eco_gstin_issues),
                },
            )

        export_payload = get_gstr1_preview(from_date=report_from, to_date=report_to)
        canonical_payload = json.dumps(export_payload, sort_keys=True, separators=(",", ":"))
        payload_hash = sha256(canonical_payload.encode("utf-8")).hexdigest()
        period_id = resolve_period_id(conn, report_from, report_to)
        export_id = create_export_history(
            conn,
            report_type="GSTR-1",
            period_id=period_id,
            report_from=report_from,
            report_to=report_to,
            payload_hash=payload_hash,
            payload_fingerprint=payload_hash,
            status="GENERATED",
        )

        log_audit(
            conn,
            table_name="reports",
            record_id=export_id,
            action="GSTR1_EXPORT",
            old_value=None,
            new_value={
                "schema_version": export_payload["schema_version"],
                "from_date": report_from.isoformat(),
                "to_date": report_to.isoformat(),
                "payload_hash": payload_hash,
                "export_id": export_id,
                "exported_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            },
            high_priority=True,
        )
        conn.commit()

    return {
        "status": "success",
        "message": "GSTR-1 export generated",
        "export_id": export_id,
        "payload_hash": payload_hash,
        "certificate": {
            "algorithm": "SHA-256",
            "payload_hash": payload_hash,
        },
        "payload": export_payload,
    }


@app.get("/api/v1/reports/export-history")
def get_export_history(limit: int = 100) -> list[dict[str, Any]]:
    safe_limit = min(max(limit, 1), 500)

    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT id,
                   report_type,
                   period_id,
                   period_from,
                   period_to,
                   generated_at,
                   payload_hash,
                     payload_fingerprint,
                     approved_by_1,
                     approved_by_2,
                     last_verification_status,
                     last_verified_by,
                     last_verified_at,
                         security_hold_until,
                   status,
                   arn_number
            FROM export_history
            ORDER BY generated_at DESC, id DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

    return [dict(row) for row in rows]


@app.get("/api/v1/reports/marketing-signups")
def get_marketing_signups_report(limit: int = 200) -> dict[str, Any]:
    safe_limit = min(max(limit, 1), 1000)

    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT id,
                   name,
                   email,
                   provider,
                   source,
                   created_at,
                   updated_at
            FROM marketing_signups
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

    return {
        "count": len(rows),
        "rows": [dict(row) for row in rows],
    }


@app.post("/api/v1/market-intel/upload", dependencies=[Depends(rate_limit_heavy_task(seconds=10))])
async def post_market_intel_upload(
    file: UploadFile = File(...),
    source_kind: str = "ACCOUNTING_MARKET_FEED",
    period_hint: str | None = None,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing upload filename")

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    MARKET_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    MARKET_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    ext = Path(file.filename).suffix.lower()
    safe_base = re.sub(r"[^A-Za-z0-9._-]+", "-", Path(file.filename).stem).strip("-") or "market-intel"
    stored_name = f"{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex}_{safe_base}{ext}"
    stored_path = MARKET_UPLOAD_DIR / stored_name
    stored_path.write_bytes(raw)

    normalized_source_kind = re.sub(r"[^A-Z0-9_]+", "_", source_kind.upper()).strip("_") or "ACCOUNTING_MARKET_FEED"
    analysis = await analyze_market_trends_with_ollama(file_path=stored_path, source_kind=normalized_source_kind)
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        cursor = conn.execute(
            """
            INSERT INTO market_trend_reports(
                source_file_path,
                source_kind,
                period_hint,
                model_used,
                analysis_json,
                report_file_path,
                created_by,
                created_at
            ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?)
            """,
            (
                str(stored_path),
                normalized_source_kind,
                period_hint,
                RECON_MODEL,
                json.dumps(analysis),
                admin_id,
                now_iso,
            ),
        )
        report_id = int(cursor.lastrowid)

        report_path = MARKET_REPORT_DIR / f"market_trend_{report_id}_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.json"
        report_path.write_text(
            json.dumps(
                {
                    "report_id": report_id,
                    "source_file_path": str(stored_path),
                    "source_kind": normalized_source_kind,
                    "period_hint": period_hint,
                    "analysis": analysis,
                    "generated_at": now_iso,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        conn.execute(
            "UPDATE market_trend_reports SET report_file_path = ? WHERE id = ?",
            (str(report_path), report_id),
        )
        log_audit(
            conn,
            table_name="market_trend_reports",
            record_id=report_id,
            action="MARKET_TREND_ANALYZED",
            old_value=None,
            new_value={
                "source_file_path": str(stored_path),
                "source_kind": normalized_source_kind,
                "period_hint": period_hint,
                "report_file_path": str(report_path),
                "actor_role": role,
                "model_used": RECON_MODEL,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    return {
        "status": "ok",
        "report_id": report_id,
        "source_file_path": str(stored_path),
        "report_file_path": str(report_path),
        "source_kind": normalized_source_kind,
        "analysis": analysis,
        "model_used": RECON_MODEL,
        "generated_at": now_iso,
    }


@app.get("/api/v1/market-intel/reports")
def get_market_intel_reports(
    limit: int = 50,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin", "ca"})
    require_admin_id(x_admin_id)
    safe_limit = min(max(limit, 1), 500)

    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT id,
                   source_file_path,
                   source_kind,
                   period_hint,
                   model_used,
                   analysis_json,
                   report_file_path,
                   created_by,
                   created_at
            FROM market_trend_reports
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

    items: list[dict[str, Any]] = []
    for row in rows:
        analysis_payload: dict[str, Any] = {}
        if row["analysis_json"]:
            try:
                parsed = json.loads(str(row["analysis_json"]))
                analysis_payload = parsed if isinstance(parsed, dict) else {}
            except Exception:  # noqa: BLE001
                analysis_payload = {}
        items.append(
            {
                "id": int(row["id"]),
                "source_file_path": str(row["source_file_path"]),
                "source_kind": str(row["source_kind"]),
                "period_hint": row["period_hint"],
                "model_used": str(row["model_used"]),
                "report_file_path": str(row["report_file_path"] or ""),
                "created_by": int(row["created_by"]),
                "created_at": str(row["created_at"]),
                "analysis": analysis_payload,
            }
        )

    return {
        "status": "ok",
        "count": len(items),
        "items": items,
    }


@app.post("/api/v1/reports/verify-export")
async def post_verify_export(
    file: UploadFile = File(...),
    expected_export_hash: str | None = None,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    raw_bytes = await file.read()
    if not raw_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    payload_fingerprint = sha256(raw_bytes).hexdigest()
    now_ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    with closing(get_conn()) as conn:
        matched_by_fingerprint = conn.execute(
            """
            SELECT id,
                   report_type,
                     period_id,
                   period_from,
                   period_to,
                   generated_at,
                   payload_hash,
                   payload_fingerprint,
                   status
            FROM export_history
            WHERE payload_fingerprint = ?
            ORDER BY generated_at DESC, id DESC
            LIMIT 1
            """,
            (payload_fingerprint,),
        ).fetchone()

        matched_by_hash = None
        if expected_export_hash:
            matched_by_hash = conn.execute(
                """
                SELECT id,
                       report_type,
                      period_id,
                       period_from,
                       period_to,
                       generated_at,
                       payload_hash,
                       payload_fingerprint,
                       status
                FROM export_history
                WHERE payload_hash = ?
                ORDER BY generated_at DESC, id DESC
                LIMIT 1
                """,
                (expected_export_hash.strip(),),
            ).fetchone()

    result_status = "MATCHED"
    matched_export_id: int | None = int(matched_by_fingerprint["id"]) if matched_by_fingerprint is not None else None

    if matched_by_fingerprint is None:
        result_status = "TAMPERED"
    elif expected_export_hash and matched_by_fingerprint["payload_hash"] != expected_export_hash.strip():
        result_status = "TAMPERED"

    with closing(get_conn()) as conn:
        try:
            conn.execute("BEGIN")
            target_export_id: int | None = None
            target_period_id: int | None = None
            target_period_from: str | None = None
            target_period_to: str | None = None
            if expected_export_hash:
                row = conn.execute(
                    "SELECT id, period_id, period_from, period_to FROM export_history WHERE payload_hash = ? LIMIT 1",
                    (expected_export_hash.strip(),),
                ).fetchone()
                if row is not None:
                    target_export_id = int(row["id"])
                    target_period_id = int(row["period_id"]) if row["period_id"] is not None else None
                    target_period_from = row["period_from"]
                    target_period_to = row["period_to"]
            if target_export_id is None:
                target_export_id = matched_export_id
                if matched_by_fingerprint is not None:
                    target_period_id = int(matched_by_fingerprint["period_id"]) if matched_by_fingerprint["period_id"] is not None else None
                    target_period_from = matched_by_fingerprint["period_from"]
                    target_period_to = matched_by_fingerprint["period_to"]

            security_hold_until = (datetime.utcnow() + timedelta(hours=1)).isoformat(timespec="seconds") + "Z" if result_status == "TAMPERED" else None

            if target_export_id is not None:
                conn.execute(
                    """
                    UPDATE export_history
                    SET last_verification_status = ?,
                        last_verified_by = ?,
                        last_verified_at = ?
                    WHERE id = ?
                    """,
                    (result_status, admin_id, now_ts, target_export_id),
                )

            if security_hold_until is not None:
                if target_period_id is not None:
                    conn.execute(
                        """
                        UPDATE export_history
                        SET security_hold_until = ?
                        WHERE report_type = 'RULE37A_REVERSAL_CSV'
                          AND period_id = ?
                        """,
                        (security_hold_until, target_period_id),
                    )
                elif target_period_from is not None and target_period_to is not None:
                    conn.execute(
                        """
                        UPDATE export_history
                        SET security_hold_until = ?
                        WHERE report_type = 'RULE37A_REVERSAL_CSV'
                          AND period_from = ?
                          AND period_to = ?
                        """,
                        (security_hold_until, target_period_from, target_period_to),
                    )

            log_audit(
                conn,
                table_name="export_history",
                record_id=target_export_id or 0,
                action="EXPORT_VERIFY",
                old_value=None,
                new_value={
                    "status": result_status,
                    "expected_export_hash": expected_export_hash,
                    "payload_fingerprint": payload_fingerprint,
                    "matched_export_id": matched_export_id,
                    "file_name": file.filename,
                    "security_hold_until": security_hold_until,
                    "actor_role": role,
                },
                user_id=admin_id,
                high_priority=True,
            )
            conn.commit()
        except Exception:  # noqa: BLE001
            conn.rollback()

    if matched_by_fingerprint is None:
        return {
            "status": "TAMPERED",
            "matched": False,
            "payload_fingerprint": payload_fingerprint,
            "expected_export_hash": expected_export_hash,
            "reason": "No matching export fingerprint found in Accord export history.",
            "verified_by": admin_id,
            "verified_at": now_ts,
            "security_hold_applied": False,
        }

    if expected_export_hash and matched_by_fingerprint["payload_hash"] != expected_export_hash.strip():
        return {
            "status": "TAMPERED",
            "matched": False,
            "payload_fingerprint": payload_fingerprint,
            "expected_export_hash": expected_export_hash,
            "reason": "Fingerprint matches an export, but not the provided export hash.",
            "matched_export": {
                "export_id": int(matched_by_fingerprint["id"]),
                "report_type": matched_by_fingerprint["report_type"],
                "generated_at": matched_by_fingerprint["generated_at"],
            },
            "expected_export_record_found": matched_by_hash is not None,
            "verified_by": admin_id,
            "verified_at": now_ts,
            "security_hold_applied": True,
        }

    return {
        "status": "MATCHED",
        "matched": True,
        "payload_fingerprint": payload_fingerprint,
        "expected_export_hash": expected_export_hash,
        "verified_by": admin_id,
        "verified_at": now_ts,
        "export": {
            "export_id": int(matched_by_fingerprint["id"]),
            "report_type": matched_by_fingerprint["report_type"],
            "period_from": matched_by_fingerprint["period_from"],
            "period_to": matched_by_fingerprint["period_to"],
            "generated_at": matched_by_fingerprint["generated_at"],
            "export_hash": matched_by_fingerprint["payload_hash"],
            "status": matched_by_fingerprint["status"],
        },
    }


@app.get("/api/v1/insights/vendor-trust")
def get_vendor_trust(limit: int = 100) -> list[dict[str, Any]]:
    safe_limit = min(max(limit, 1), 500)
    with closing(get_conn()) as conn:
        gstins = conn.execute(
            """
            SELECT DISTINCT counterparty_gstin
            FROM journal_entries
            WHERE counterparty_gstin IS NOT NULL
            """
        ).fetchall()

        conn.execute("BEGIN")
        for row in gstins:
            recompute_vendor_trust(conn, str(row["counterparty_gstin"]))
        conn.commit()

        rows = conn.execute(
            """
            SELECT gstin,
                   legal_name,
                   filing_consistency_score,
                   avg_filing_delay_days,
                   last_gstr1_filed_at,
                   total_itc_at_risk,
                   updated_at
            FROM vendor_trust_scores
            ORDER BY filing_consistency_score ASC, total_itc_at_risk DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

    result: list[dict[str, Any]] = []
    with closing(get_conn()) as conn:
        for row in rows:
            gstin = str(row["gstin"])
            high_risk_row = conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM journal_entries je
                JOIN tax_ledger tl ON tl.entry_id = je.id
                WHERE tl.supply_type = 'B2B'
                  AND je.counterparty_gstin = ?
                  AND je.vendor_gstr1_filed_at IS NOT NULL
                  AND je.vendor_gstr1_filed_at > (
                      CASE
                          WHEN CAST(strftime('%m', je.date) AS INTEGER) = 12
                            THEN printf('%04d-01-11', CAST(strftime('%Y', je.date) AS INTEGER) + 1)
                          ELSE printf(
                              '%04d-%02d-11',
                              CAST(strftime('%Y', je.date) AS INTEGER),
                              CAST(strftime('%m', je.date) AS INTEGER) + 1
                          )
                      END
                  )
                """,
                (gstin,),
            ).fetchone()

            score = float(row["filing_consistency_score"])
            result.append(
                {
                    **dict(row),
                    "high_risk_delay_count": int(high_risk_row["cnt"]) if high_risk_row else 0,
                    "payment_advice": payment_advice_for_score(score),
                }
            )

    return result


@app.get("/api/v1/insights/reversal-risks")
def get_reversal_risks(as_of_date: date | None = None, min_credit_balance: Decimal | None = None) -> dict[str, Any]:
    as_of = as_of_date or date.today()
    with closing(get_conn()) as conn:
        return compute_reversal_risk(conn, as_of, min_credit_balance=min_credit_balance)


@app.post("/api/v1/insights/safe-harbor/certify")
def post_certify_safe_harbor(
    payload: SafeHarborCertifyIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    x_biometric_token: str | None = Header(default=None, alias="X-Biometric-Token"),
) -> dict[str, Any]:
    role = require_role(x_role, {"ca"})
    admin_id = require_admin_id(x_admin_id)
    require_biometric_signoff(x_biometric_token, admin_id, "SAFE_HARBOR_CERTIFY")
    as_of = payload.as_of_date or date.today()

    with closing(get_conn()) as conn:
        try:
            conn.execute("BEGIN")
            risk = compute_reversal_risk(conn, as_of, min_credit_balance=payload.min_credit_balance)
            safe_harbor = risk["rule_37a"]["safe_harbor"]

            cursor = conn.execute(
                """
                INSERT INTO safe_harbor_attestations(
                    as_of_date,
                    min_credit_balance,
                    reversal_amount,
                    liability_offset,
                    status,
                    legal_basis,
                    certified_by,
                    certified_role,
                    note,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    as_of.isoformat(),
                    money_str(payload.min_credit_balance),
                    risk["rule_37a"]["immediate_reversal_risk"],
                    safe_harbor["liability_offset"] or "0.0000",
                    safe_harbor["status"],
                    safe_harbor["legal_basis"],
                    admin_id,
                    role,
                    payload.note.strip() if payload.note else None,
                    datetime.utcnow().isoformat(timespec="seconds") + "Z",
                ),
            )

            attestation_id = int(cursor.lastrowid)
            log_audit(
                conn,
                table_name="safe_harbor_attestations",
                record_id=attestation_id,
                action="SAFE_HARBOR_CERTIFY",
                old_value=None,
                new_value={
                    "as_of_date": as_of.isoformat(),
                    "min_credit_balance": money_str(payload.min_credit_balance),
                    "status": safe_harbor["status"],
                    "legal_basis": safe_harbor["legal_basis"],
                    "actor_role": role,
                },
                user_id=admin_id,
                high_priority=True,
            )
            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to certify safe harbor: {exc}") from exc

    return {
        "message": "Safe Harbor attested by CA",
        "attestation_id": attestation_id,
        "as_of_date": as_of.isoformat(),
        "safe_harbor": safe_harbor,
    }


@app.get("/api/v1/journal/safe-harbor-certificate/{batch_id}")
def get_safe_harbor_certificate(
    batch_id: int,
    min_credit_balance: Decimal | None = None,
    as_of_date: date | None = None,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    x_biometric_token: str | None = Header(default=None, alias="X-Biometric-Token"),
) -> Response:
    require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    require_biometric_signoff(x_biometric_token, admin_id, "SAFE_HARBOR_CERTIFICATE")

    with closing(get_conn()) as conn:
        row = conn.execute(
            """
            SELECT je.id, je.reference, je.date, je.created_at, a.new_value
            FROM journal_entries je
            JOIN audit_edit_logs a
              ON a.table_name = 'journal_entries'
             AND a.record_id = je.id
             AND a.action = 'RULE_37A_REVERSAL'
            WHERE je.id = ?
            ORDER BY a.id DESC
            LIMIT 1
            """,
            (batch_id,),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Rule 37A reversal batch not found")

        lines = conn.execute(
            """
            SELECT account_id, debit, credit
            FROM journal_lines
            WHERE entry_id = ?
            ORDER BY id ASC
            """,
            (batch_id,),
        ).fetchall()
        if not lines:
            raise HTTPException(status_code=404, detail="Batch lines not found")

        reversal_meta = json.loads(row["new_value"]) if row["new_value"] else {}
        reversal_amount = money(reversal_meta.get("risk_amount", "0"))
        if reversal_amount <= 0:
            total_debit_row = conn.execute(
                "SELECT COALESCE(SUM(CAST(debit AS REAL)), 0) AS total_debit FROM journal_lines WHERE entry_id = ?",
                (batch_id,),
            ).fetchone()
            reversal_amount = money(total_debit_row["total_debit"] if total_debit_row else "0")

        if min_credit_balance is None:
            attestation = conn.execute(
                """
                SELECT min_credit_balance
                FROM safe_harbor_attestations
                ORDER BY id DESC
                LIMIT 1
                """,
            ).fetchone()
            mmb = money(attestation["min_credit_balance"] if attestation else "0")
        else:
            mmb = money(min_credit_balance)

        as_of = as_of_date or date.today()
        liability_offset = money(mmb - reversal_amount)
        safe_harbor_applicable = mmb >= reversal_amount
        legal_basis = "Sec_50(3)_Full_Cover" if safe_harbor_applicable else "Sec_50(3)_Standard_Interest"
        batch_integrity_hash = build_integrity_hash(str(row["reference"]), lines)

        pdf_content = build_safe_harbor_certificate_pdf(
            batch_id=batch_id,
            reference=str(row["reference"]),
            as_of_date=as_of.isoformat(),
            min_credit_balance=mmb,
            reversal_amount=reversal_amount,
            liability_offset=liability_offset,
            safe_harbor_applicable=safe_harbor_applicable,
            legal_basis=legal_basis,
            batch_integrity_hash=batch_integrity_hash,
            generated_by=admin_id,
        )

    filename = f"Accord_SafeHarbor_Certificate_{batch_id}.pdf"
    return Response(
        content=pdf_content,
        media_type="application/pdf",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "X-Accord-Batch-Integrity-Hash": batch_integrity_hash,
            "X-Accord-Legal-Basis": legal_basis,
            "X-Accord-Safe-Harbor": "true" if safe_harbor_applicable else "false",
        },
    )


@app.get("/api/v1/insights/vendor/{gstin}/payment-advice")
def get_vendor_payment_advice(gstin: str, invoice_amount: Decimal | None = None) -> dict[str, Any]:
    gstin = validate_gstin(gstin)

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        recompute_vendor_trust(conn, gstin)
        row = conn.execute(
            """
            SELECT gstin,
                   legal_name,
                   filing_consistency_score,
                   avg_filing_delay_days,
                   last_gstr1_filed_at,
                   total_itc_at_risk,
                   updated_at
            FROM vendor_trust_scores
            WHERE gstin = ?
            LIMIT 1
            """,
            (gstin,),
        ).fetchone()
        conn.commit()

    if row is None:
        raise HTTPException(status_code=404, detail="Vendor trust profile not found")

    total_itc_at_risk = money(row["total_itc_at_risk"])
    withholding_buffer = money(total_itc_at_risk + (total_itc_at_risk * Decimal("0.18")))
    score = float(row["filing_consistency_score"])
    advice = payment_advice_for_score(score)

    response: dict[str, Any] = {
        "gstin": row["gstin"],
        "legal_name": row["legal_name"],
        "trust_score": score,
        "payment_advice": advice,
        "total_itc_at_risk": money_str(total_itc_at_risk),
        "suggested_withholding": money_str(withholding_buffer),
        "formula": "total_itc_at_risk + (total_itc_at_risk * 0.18)",
    }

    if invoice_amount is not None:
        gross_invoice = money(invoice_amount)
        safe_payment = money(max(gross_invoice - withholding_buffer, Decimal("0")))
        response["invoice_amount"] = money_str(gross_invoice)
        response["safe_payment_amount"] = money_str(safe_payment)

    return response


@app.post("/api/v1/insights/ask-friday")
async def post_ask_friday(payload: AskFridayIn) -> dict[str, Any]:
    summary = get_friday_summary(as_of_date=payload.as_of_date, min_credit_balance=payload.min_credit_balance)

    system_prompt = (
        "You are an Indian Chartered Accountant. Based on this JSON data, "
        "what are the top 3 cash-flow risks this user faces today? "
        "Return concise, action-oriented bullets with INR impact where possible."
    )

    request_payload = {
        "model": payload.model,
        "stream": False,
        "messages": [
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "question": payload.question,
                        "friday_summary": summary,
                    },
                    separators=(",", ":"),
                ),
            },
        ],
    }

    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            ollama_res = await client.post(OLLAMA_CHAT_URL, json=request_payload)
        ollama_res.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(
            status_code=502,
            detail=f"Ollama request failed. Ensure ollama is running and model '{payload.model}' is pulled. Error: {exc}",
        ) from exc

    data = ollama_res.json()
    answer = data.get("message", {}).get("content")
    if not answer:
        raise HTTPException(status_code=502, detail="Ollama returned an empty response")

    return {
        "model": payload.model,
        "question": payload.question,
        "as_of_date": (payload.as_of_date or date.today()).isoformat(),
        "answer": answer,
        "source": "OLLAMA_LOCAL",
        "summary_snapshot": summary,
    }


@app.api_route(
    "/api/v1/insights/forensic-audit",
    methods=["GET", "POST"],
    dependencies=[Depends(rate_limit_heavy_task(seconds=10))],
)
async def get_forensic_audit(
    limit: int = 200,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    safe_limit = min(max(limit, 20), 2000)

    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT je.id, je.reference, je.date, je.description, je.entry_fingerprint,
                   GROUP_CONCAT(
                     CASE WHEN jl.debit != '0.0000'
                          THEN a.name || ':DR:' || jl.debit
                          ELSE a.name || ':CR:' || jl.credit END,
                     ' | '
                   ) AS line_summary
            FROM journal_entries je
            JOIN journal_lines jl ON jl.entry_id = je.id
            JOIN accounts a ON a.id = jl.account_id
            GROUP BY je.id, je.reference, je.date, je.description, je.entry_fingerprint
            ORDER BY je.id DESC
            LIMIT ?
            """,
            (safe_limit,),
        ).fetchall()

    dataset = [
        {
            "entry_id": int(row["id"]),
            "reference": row["reference"],
            "date": row["date"],
            "description": row["description"],
            "entry_fingerprint": row["entry_fingerprint"],
            "line_summary": row["line_summary"],
        }
        for row in rows
    ]

    prompt = (
        "Audit these journal rows for anomalies. Return strict JSON with keys: "
        "risk_score (0-100), flagged_entries (array of {entry_id, issue, severity}), summary. "
        "Focus on suspicious duplicates, round-tripping patterns, and mismatched narration/ledger intent.\n"
        f"Rows: {json.dumps(dataset, separators=(',', ':'))}"
    )

    model_used = FORENSIC_MODEL
    audit_raw = ""
    try:
        audit_raw = await run_ollama_generate(model=FORENSIC_MODEL, prompt=prompt)
    except Exception:  # noqa: BLE001
        model_used = RECON_MODEL
        audit_raw = await run_ollama_generate(model=RECON_MODEL, prompt=prompt)

    parsed = parse_structured_json(audit_raw)
    flagged_entries = parsed.get("flagged_entries") if isinstance(parsed.get("flagged_entries"), list) else []
    summary_text = str(parsed.get("summary", "No summary returned."))
    report_blocks = [
        f"MODEL: {model_used}",
        f"ENTRIES_SCANNED: {len(dataset)}",
        f"RISK_SCORE: {parsed.get('risk_score', 0)}",
        f"SUMMARY: {summary_text}",
    ]
    for flagged in flagged_entries[:50]:
        report_blocks.append(
            f"ENTRY #{flagged.get('entry_id', '-')}: {flagged.get('severity', 'MEDIUM')} | {flagged.get('issue', 'No issue details provided.')}"
        )
    report_text = "\n".join(report_blocks)
    report_fingerprint = sha256(report_text.encode("utf-8")).hexdigest()

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        log_audit(
            conn,
            table_name="reports",
            record_id=0,
            action="FORENSIC_AUDIT_RUN",
            old_value=None,
            new_value={
                "entries_scanned": len(dataset),
                "flagged_count": len(flagged_entries),
                "model": model_used,
                "report_fingerprint": report_fingerprint,
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    return {
        "status": "ok",
        "model": model_used,
        "entries_scanned": len(dataset),
        "risk_score": parsed.get("risk_score", 0),
        "summary": summary_text,
        "flagged_entries": flagged_entries,
        "audit_report": report_text,
        "report_fingerprint": report_fingerprint,
        "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


@app.post("/api/v1/gstn/ecl-bridge")
async def post_gstn_ecl_bridge(
    payload: GstnEclBridgeIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    gstin = validate_gstin(payload.gstin)
    as_of = payload.as_of_date or date.today()

    gstn_base_url = os.getenv("GSTN_ECL_BASE_URL", "").strip()
    gstn_token = os.getenv("GSTN_ECL_TOKEN", "").strip()

    if gstn_base_url:
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                external_res = await client.get(
                    f"{gstn_base_url.rstrip('/')}/ecl",
                    params={
                        "gstin": gstin,
                        "as_of_date": as_of.isoformat(),
                        "period": payload.period,
                    },
                    headers={"Authorization": f"Bearer {gstn_token}"} if gstn_token else {},
                )
            external_res.raise_for_status()
            ecl_payload = external_res.json()
            source = "GSTN_API"
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=502, detail=f"GSTN bridge failed: {exc}") from exc
    else:
        with closing(get_conn()) as conn:
            ecl_payload = {
                "source": "SIMULATED_LOCAL_LEDGER",
                "ecl": get_local_ecl_snapshot(conn),
            }
        source = "SIMULATED_LOCAL_LEDGER"

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        log_audit(
            conn,
            table_name="reports",
            record_id=0,
            action="GSTN_ECL_FETCH",
            old_value=None,
            new_value={
                "gstin": gstin,
                "as_of_date": as_of.isoformat(),
                "period": payload.period,
                "source": source,
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    return {
        "gstin": gstin,
        "as_of_date": as_of.isoformat(),
        "period": payload.period,
        "source": source,
        "ecl": ecl_payload,
    }


@app.post("/api/v1/statutory/gstr1/generate")
def post_statutory_generate_gstr1(
    payload: Gstr1GenerateIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    statutory_layer = ensure_statutory_service()

    start_date = payload.from_date or date.today().replace(day=1)
    end_date = payload.to_date or date.today()
    if end_date < start_date:
        raise HTTPException(status_code=422, detail="to_date must be on or after from_date")

    filing_gstin = validate_gstin(payload.gstin) if payload.gstin else ACCORD_GSTIN
    filing_period = payload.period or end_date.strftime("%m%Y")

    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT je.reference,
                   je.date,
                   COALESCE(je.counterparty_gstin, '') AS gstin,
                   COALESCE(je.counterparty_state_code, '') AS place_of_supply,
                   COALESCE(tl.supply_type, 'B2CS') AS supply_type,
                   COALESCE(tl.hsn_code, '') AS hsn_code,
                   COALESCE(hm.uqc, 'NOS') AS uqc,
                   COALESCE(tl.taxable_value, '0') AS taxable_value,
                   CASE
                       WHEN je.counterparty_state_code IS NOT NULL
                        AND je.company_state_code IS NOT NULL
                        AND je.counterparty_state_code != je.company_state_code
                       THEN COALESCE(tl.tax_amount, '0')
                       ELSE '0'
                   END AS igst,
                   CASE
                       WHEN je.counterparty_state_code IS NULL
                        OR je.company_state_code IS NULL
                        OR je.counterparty_state_code = je.company_state_code
                       THEN printf('%.4f', CAST(COALESCE(tl.tax_amount, '0') AS REAL) / 2.0)
                       ELSE '0'
                   END AS cgst,
                   CASE
                       WHEN je.counterparty_state_code IS NULL
                        OR je.company_state_code IS NULL
                        OR je.counterparty_state_code = je.company_state_code
                       THEN printf('%.4f', CAST(COALESCE(tl.tax_amount, '0') AS REAL) / 2.0)
                       ELSE '0'
                   END AS sgst,
                   '0.0000' AS cess,
                   '1.0000' AS quantity
            FROM tax_ledger tl
            JOIN journal_entries je ON je.id = tl.entry_id
            LEFT JOIN hsn_master hm ON hm.code = tl.hsn_code
            WHERE je.status = 'POSTED'
              AND je.date >= ?
              AND je.date <= ?
            ORDER BY je.id ASC
            """,
            (start_date.isoformat(), end_date.isoformat()),
        ).fetchall()

        ledger_data = [
            {
                "reference": str(row["reference"] or ""),
                "invoice_date": str(row["date"] or ""),
                "gstin": str(row["gstin"] or "").upper(),
                "place_of_supply": str(row["place_of_supply"] or ""),
                "supply_type": str(row["supply_type"] or "B2CS"),
                "hsn_code": str(row["hsn_code"] or ""),
                "uqc": str(row["uqc"] or "NOS"),
                "quantity": str(row["quantity"] or "1"),
                "taxable_value": str(row["taxable_value"] or "0"),
                "igst": str(row["igst"] or "0"),
                "cgst": str(row["cgst"] or "0"),
                "sgst": str(row["sgst"] or "0"),
                "cess": str(row["cess"] or "0"),
            }
            for row in rows
        ]

        generated = statutory_layer.generate_gstr1_json(
            ledger_data=ledger_data,
            gstin=filing_gstin,
            period=filing_period,
        )

        conn.execute("BEGIN")
        log_audit(
            conn,
            table_name="reports",
            record_id=0,
            action="GSTR1_SNIPER_GENERATED",
            old_value=None,
            new_value={
                "from_date": start_date.isoformat(),
                "to_date": end_date.isoformat(),
                "gstin": filing_gstin,
                "period": filing_period,
                "invoice_count": generated.get("summary", {}).get("invoice_count", 0),
                "fingerprint": generated.get("hardware_fingerprint", ""),
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    return {
        "status": "ok",
        "engine": "GSTR1_SNIPER_POLARS",
        "window": {"from": start_date.isoformat(), "to": end_date.isoformat()},
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "payload": generated,
    }


@app.post("/api/v1/statutory/gstr1/prepare")
def post_statutory_prepare_gstr1(
    payload: Gstr1PrepareIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    gst_layer = ensure_gst_service()

    filing_type = payload.filing_type.strip().upper()
    if filing_type != "GSTR-1":
        raise HTTPException(status_code=422, detail="Only GSTR-1 filing_type is supported in V2.5")

    period_start, period_end = gst_layer.period_bounds(payload.period)
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT je.id AS entry_id,
                   je.date,
                   COALESCE(je.reference, '') AS reference,
                   COALESCE(je.counterparty_gstin, '') AS gstin,
                   COALESCE(MAX(tl.supply_type), 'B2CS') AS supply_type,
                   COALESCE(je.currency_code, 'INR') AS currency_code,
                   COALESCE(je.exchange_rate, '1.0000') AS exchange_rate,
                   COALESCE(SUM(CAST(COALESCE(tl.taxable_value, '0') AS REAL)), 0) AS taxable_value
            FROM journal_entries je
            LEFT JOIN tax_ledger tl ON tl.entry_id = je.id
            WHERE je.status = 'POSTED'
              AND je.date >= ?
              AND je.date <= ?
            GROUP BY je.id, je.date, je.reference, je.counterparty_gstin, je.currency_code, je.exchange_rate
            ORDER BY je.id ASC
            """,
            (period_start.isoformat(), period_end.isoformat()),
        ).fetchall()

        normalized_rows = [
            {
                "entry_id": int(row["entry_id"]),
                "date": str(row["date"]),
                "reference": str(row["reference"]),
                "gstin": str(row["gstin"]),
                "supply_type": str(row["supply_type"]),
                "currency_code": str(row["currency_code"]),
                "exchange_rate": str(row["exchange_rate"]),
                "taxable_value": str(row["taxable_value"]),
            }
            for row in rows
        ]

        summary, issues, status = gst_layer.prepare_gstr1(normalized_rows)

        conn.execute("BEGIN")
        filing_cursor = conn.execute(
            """
            INSERT INTO gst_filings(period, filing_type, status, summary_data, created_at, updated_at, approved_by, approval_ts)
            VALUES (?, ?, ?, ?, ?, ?, NULL, NULL)
            """,
            (payload.period, filing_type, status, json.dumps(summary), now_iso, now_iso),
        )
        filing_id = int(filing_cursor.lastrowid)

        for issue in issues:
            conn.execute(
                """
                INSERT INTO gst_validation_issues(filing_id, entry_id, severity, issue_type, message, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    filing_id,
                    issue.get("entry_id"),
                    str(issue.get("severity") or "WARNING"),
                    str(issue.get("issue_type") or "UNCLASSIFIED"),
                    str(issue.get("message") or "Validation issue detected"),
                    now_iso,
                ),
            )

        blocker_count = sum(1 for issue in issues if str(issue.get("severity")) == "BLOCKER")
        warning_count = sum(1 for issue in issues if str(issue.get("severity")) == "WARNING")

        log_audit(
            conn,
            table_name="gst_filings",
            record_id=filing_id,
            action="FILING_PREPARE",
            old_value=None,
            new_value={
                "period": payload.period,
                "filing_type": filing_type,
                "status": status,
                "entries_scanned": len(normalized_rows),
                "blocker_count": blocker_count,
                "warning_count": warning_count,
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    return {
        "status": "ok",
        "filing": {
            "id": filing_id,
            "period": payload.period,
            "filing_type": filing_type,
            "filing_status": status,
            "summary": summary,
            "created_at": now_iso,
        },
        "issues": {
            "count": len(issues),
            "blockers": blocker_count,
            "warnings": warning_count,
            "items": issues,
        },
    }


@app.post("/api/v1/statutory/gstr1/approve")
def post_statutory_approve_gstr1(
    payload: Gstr1ApproveIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin"})
    admin_id = require_admin_id(x_admin_id)
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    with closing(get_conn()) as conn:
        filing = conn.execute(
            """
            SELECT id, period, filing_type, status, summary_data
            FROM gst_filings
            WHERE id = ?
            LIMIT 1
            """,
            (payload.filing_id,),
        ).fetchone()
        if filing is None:
            raise HTTPException(status_code=404, detail="Filing not found")

        blocker_row = conn.execute(
            """
            SELECT COUNT(*) AS blocker_count
            FROM gst_validation_issues
            WHERE filing_id = ?
              AND severity = 'BLOCKER'
            """,
            (payload.filing_id,),
        ).fetchone()
        blocker_count = int(blocker_row["blocker_count"]) if blocker_row is not None else 0
        if blocker_count > 0:
            raise HTTPException(status_code=400, detail="Unresolved blockers prevent approval")

        current_status = str(filing["status"])
        if current_status not in {"READY_FOR_REVIEW", "DRAFT"}:
            raise HTTPException(status_code=400, detail=f"Filing cannot be approved from status {current_status}")

        conn.execute("BEGIN")
        conn.execute(
            """
            UPDATE gst_filings
            SET status = 'APPROVED',
                approved_by = ?,
                approval_ts = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (admin_id, now_iso, now_iso, payload.filing_id),
        )

        log_audit(
            conn,
            table_name="gst_filings",
            record_id=payload.filing_id,
            action="FILING_APPROVED",
            old_value={"status": current_status},
            new_value={
                "status": "APPROVED",
                "approved_by": admin_id,
                "approved_at": now_iso,
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    return {
        "status": "ok",
        "filing_id": payload.filing_id,
        "filing_status": "APPROVED",
        "approved_by": admin_id,
        "approved_at": now_iso,
    }


@app.post("/api/v1/statutory/gstr1/file-success")
def post_statutory_gstr1_file_success(
    payload: Gstr1FiledIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    x_idempotency_key: str | None = Header(default=None, alias="X-Idempotency-Key"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    statutory_layer = ensure_statutory_service()
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    filing_id = 0
    with closing(get_conn()) as conn:
        filing = conn.execute(
            """
            SELECT id
            FROM gst_filings
            WHERE period = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (payload.period,),
        ).fetchone()
        if filing is not None:
            filing_id = int(filing["id"])

    idempotent_result = statutory_layer.execute_idempotent_filing(
        filing_id=filing_id,
        idempotency_key=x_idempotency_key,
    )

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        log_audit(
            conn,
            table_name="reports",
            record_id=0,
            action="GSTR1_FILED_SUCCESS",
            old_value=None,
            new_value={
                "period": payload.period,
                "fingerprint": payload.fingerprint,
                "filing_reference": payload.filing_reference,
                "filed_at": now_iso,
                "actor_role": role,
                "filing_id": filing_id,
                "idempotency": idempotent_result,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    return {
        "status": "ok",
        "action": "GSTR1_FILED_SUCCESS",
        "period": payload.period,
        "fingerprint": payload.fingerprint,
        "filed_at": now_iso,
        "idempotency": idempotent_result,
    }


@app.post("/api/v1/statutory/investor-mode/run")
def post_statutory_investor_mode_run(
    payload: InvestorModeIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin"})
    admin_id = require_admin_id(x_admin_id)
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    currency_layer = ensure_currency_service()
    posting_date = date.today()
    usd_amount = money("1200")
    usd_rate = money("83.1500")
    base_amount = currency_layer.convert_to_base(usd_amount, "USD", usd_rate)
    injected_entry_id = 0
    injected_reference = ""

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        check_period_lock(conn, posting_date)
        injected_reference = next_journal_reference(conn, posting_date)
        purchases_id = get_account_id_by_name(conn, "Purchases")
        payable_id = get_account_id_by_name(conn, "Accounts Payable")
        created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

        conn.execute(
            """
            INSERT INTO journal_entries(
                date,
                reference,
                description,
                company_state_code,
                counterparty_state_code,
                counterparty_gstin,
                eco_gstin,
                supply_source,
                ims_status,
                vendor_legal_name,
                vendor_gstr1_filed_at,
                status,
                reversal_of_id,
                is_filed,
                filed_at,
                filed_export_hash,
                approved_by_1,
                approved_by_2,
                voucher_type,
                currency_code,
                exchange_rate,
                created_at
            ) VALUES (?, ?, ?, '29', NULL, NULL, NULL, 'DIRECT', 'PENDING', ?, NULL, 'POSTED', NULL, 0, NULL, NULL, NULL, NULL, 'JOURNAL', 'USD', ?, ?)
            """,
            (
                posting_date.isoformat(),
                injected_reference,
                "Investor mode FX invoice draft with missing GSTIN",
                "Investor Demo Vendor",
                money_str(usd_rate),
                created_at,
            ),
        )
        injected_entry_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])

        conn.execute(
            "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
            (injected_entry_id, purchases_id, money_str(base_amount), "0.0000"),
        )
        update_account_balance(conn, purchases_id, base_amount, Decimal("0"))
        conn.execute(
            "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
            (injected_entry_id, payable_id, "0.0000", money_str(base_amount)),
        )
        update_account_balance(conn, payable_id, Decimal("0"), base_amount)

        tax_amount = money(base_amount * Decimal("0.18"))
        conn.execute(
            """
            INSERT INTO tax_ledger(
                entry_id,
                hsn_code,
                gst_rate_snapshot,
                taxable_value,
                tax_amount,
                supply_type,
                is_inter_state,
                supply_source,
                created_at
            ) VALUES (?, ?, ?, ?, ?, 'B2B', 0, 'DIRECT', ?)
            """,
            (
                injected_entry_id,
                "847100",
                "18.0000",
                money_str(base_amount),
                money_str(tax_amount),
                created_at,
            ),
        )
        fingerprint = stamp_entry_fingerprint(conn, injected_entry_id)
        log_audit(
            conn,
            table_name="journal_entries",
            record_id=injected_entry_id,
            action="INVESTOR_MODE_FX_INJECT",
            old_value=None,
            new_value={
                "reference": injected_reference,
                "currency_code": "USD",
                "transaction_amount": money_str(usd_amount),
                "exchange_rate": money_str(usd_rate),
                "base_amount": money_str(base_amount),
                "entry_fingerprint": fingerprint,
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    blocked_prepare = post_statutory_prepare_gstr1(
        Gstr1PrepareIn(period=payload.period, filing_type="GSTR-1"),
        x_role=role,
        x_admin_id=str(admin_id),
    )
    blocker_count = int(blocked_prepare.get("issues", {}).get("blockers", 0))

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        conn.execute(
            "UPDATE journal_entries SET counterparty_gstin = ?, vendor_legal_name = ? WHERE id = ?",
            (ACCORD_GSTIN, "Investor Demo Vendor Resolved", injected_entry_id),
        )
        log_audit(
            conn,
            table_name="journal_entries",
            record_id=injected_entry_id,
            action="INVESTOR_MODE_BLOCKER_RESOLVED",
            old_value={"counterparty_gstin": None},
            new_value={"counterparty_gstin": ACCORD_GSTIN},
            user_id=admin_id,
            high_priority=True,
        )
        recompute_vendor_trust(conn, ACCORD_GSTIN)
        conn.commit()

    ready_prepare = post_statutory_prepare_gstr1(
        Gstr1PrepareIn(period=payload.period, filing_type="GSTR-1"),
        x_role=role,
        x_admin_id=str(admin_id),
    )
    ready_filing_id = int(ready_prepare.get("filing", {}).get("id", 0))
    approved = post_statutory_approve_gstr1(
        Gstr1ApproveIn(filing_id=ready_filing_id),
        x_role=role,
        x_admin_id=str(admin_id),
    )

    def _run_prepare_probe() -> dict[str, Any]:
        started = datetime.utcnow().timestamp()
        try:
            post_statutory_prepare_gstr1(
                Gstr1PrepareIn(period=payload.period, filing_type="GSTR-1"),
                x_role=role,
                x_admin_id=str(admin_id),
            )
            elapsed_ms = (datetime.utcnow().timestamp() - started) * 1000.0
            return {"ok": True, "elapsed_ms": round(elapsed_ms, 2)}
        except Exception:  # noqa: BLE001
            elapsed_ms = (datetime.utcnow().timestamp() - started) * 1000.0
            return {"ok": False, "elapsed_ms": round(elapsed_ms, 2)}

    burst_started = datetime.utcnow().timestamp()
    with ThreadPoolExecutor(max_workers=min(25, payload.run_concurrency)) as executor:
        burst_results = list(executor.map(lambda _: _run_prepare_probe(), range(payload.run_concurrency)))
    burst_elapsed = datetime.utcnow().timestamp() - burst_started
    ok_count = sum(1 for item in burst_results if item["ok"])
    failed_count = payload.run_concurrency - ok_count
    avg_ms = round(sum(item["elapsed_ms"] for item in burst_results) / max(len(burst_results), 1), 2)

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        log_audit(
            conn,
            table_name="gst_filings",
            record_id=ready_filing_id,
            action="INVESTOR_MODE_RUN",
            old_value=None,
            new_value={
                "period": payload.period,
                "blocked_prepare_filing": blocked_prepare.get("filing", {}).get("id"),
                "blocker_count": blocker_count,
                "ready_prepare_filing": ready_filing_id,
                "concurrency_requested": payload.run_concurrency,
                "concurrency_ok": ok_count,
                "concurrency_failed": failed_count,
                "concurrency_avg_ms": avg_ms,
                "actor_role": role,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    return {
        "status": "ok",
        "mode": "INVESTOR_GOLDEN_PATH",
        "executed_at": now_iso,
        "sequence": {
            "fx_injection": {
                "entry_id": injected_entry_id,
                "reference": injected_reference,
                "currency_code": "USD",
                "transaction_amount": money_str(usd_amount),
                "exchange_rate": money_str(usd_rate),
                "base_amount": money_str(base_amount),
            },
            "prepare_blocked": {
                "filing_id": blocked_prepare.get("filing", {}).get("id"),
                "blockers": blocker_count,
            },
            "prepare_ready": {
                "filing_id": ready_filing_id,
                "blockers": int(ready_prepare.get("issues", {}).get("blockers", 0)),
            },
            "approval": approved,
            "concurrency_probe": {
                "requested": payload.run_concurrency,
                "ok": ok_count,
                "failed": failed_count,
                "avg_ms": avg_ms,
                "total_seconds": round(burst_elapsed, 3),
            },
        },
    }


@app.get("/api/v1/statutory/filing-audit")
def get_statutory_filing_audit(
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    require_role(x_role, {"admin", "ca"})
    require_admin_id(x_admin_id)

    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT id, action, new_value, created_at
            FROM audit_edit_logs
            WHERE action IN (
                'GSTR1_SNIPER_GENERATED',
                'GSTR1_FILED_SUCCESS',
                'FILING_PREPARE',
                'FILING_APPROVED',
                'INVESTOR_MODE_RUN'
            )
            ORDER BY created_at DESC, id DESC
            LIMIT 20
            """
        ).fetchall()

    logs: list[dict[str, Any]] = []
    for row in rows:
        payload = json.loads(row["new_value"]) if row["new_value"] else {}
        fingerprint = str(
            payload.get("fingerprint")
            or payload.get("hardware_fingerprint")
            or payload.get("report_fingerprint")
            or ""
        )
        logs.append(
            {
                "id": int(row["id"]),
                "timestamp": str(row["created_at"]),
                "action": str(row["action"]),
                "fingerprint": fingerprint,
                "details": payload,
            }
        )

    return {
        "status": "ok",
        "logs": logs,
    }


@app.post("/api/v1/statutory/portal-handshake")
def post_statutory_portal_handshake(
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    bridge = ensure_govt_bridge_service()

    latest_fingerprint = ""
    with closing(get_conn()) as conn:
        row = conn.execute(
            """
            SELECT new_value
            FROM audit_edit_logs
            WHERE action = 'GSTR1_SNIPER_GENERATED'
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
        if row and row["new_value"]:
            try:
                payload = json.loads(row["new_value"])
                latest_fingerprint = str(payload.get("fingerprint") or "")
            except Exception:  # noqa: BLE001
                latest_fingerprint = ""

    handshake_payload = {
        "initiated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "actor_role": role,
        "gstr1_fingerprint": latest_fingerprint,
    }
    handshake = bridge.simulate_gst_handshake(handshake_payload)

    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        log_audit(
            conn,
            table_name="reports",
            record_id=0,
            action="GSTN_PORTAL_HANDSHAKE",
            old_value=None,
            new_value={
                "actor_role": role,
                "transmission_id": handshake.get("transmission_id"),
                "payload_fingerprint": handshake.get("payload_fingerprint"),
                "gstr1_fingerprint": latest_fingerprint,
            },
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()

    return {
        "status": "ok",
        "bridge": "GOVT_PORTAL_BRIDGE",
        "handshake": handshake,
    }


@app.post("/api/v1/admin/seed-hsn")
def post_admin_seed_hsn(
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        seed_hsn_master(conn)
        log_audit(
            conn,
            table_name="hsn_master",
            record_id=0,
            action="ADMIN_SEED_HSN",
            old_value=None,
            new_value={"actor_role": role},
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()
    return {"message": "HSN master seeding completed"}


@app.post("/api/v1/admin/seed-periods")
def post_admin_seed_periods(
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    with closing(get_conn()) as conn:
        conn.execute("BEGIN")
        seed_periods(conn)
        log_audit(
            conn,
            table_name="financial_periods",
            record_id=0,
            action="ADMIN_SEED_PERIODS",
            old_value=None,
            new_value={"actor_role": role},
            user_id=admin_id,
            high_priority=True,
        )
        conn.commit()
    return {"message": "Financial periods seeding completed"}


@app.post("/api/v1/journal/generate-reversal-37a")
def post_generate_reversal_37a(
    payload: Reversal37AIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    as_of = payload.as_of_date or date.today()
    posting_date = payload.posting_date or date.today()

    with closing(get_conn()) as conn:
        try:
            conn.execute("BEGIN")
            enforce_auto_relock(conn, datetime.utcnow())
            check_period_lock(conn, posting_date)

            risk = compute_reversal_risk(conn, as_of, min_credit_balance=payload.min_credit_balance)
            risk_amount = money(risk["rule_37a"]["immediate_reversal_risk"])
            reversal_descriptor = (
                f"Rule 37A automated reversal for {risk['previous_financial_year']['start']} "
                f"to {risk['previous_financial_year']['end']}"
            )

            existing_recent = conn.execute(
                """
                SELECT je.id,
                       je.reference,
                       je.is_filed,
                       je.filed_at,
                       a.created_at
                FROM journal_entries je
                JOIN audit_edit_logs a
                  ON a.table_name = 'journal_entries'
                 AND a.record_id = je.id
                 AND a.action = 'RULE_37A_REVERSAL'
                WHERE je.description = ?
                  AND a.created_at >= ?
                ORDER BY a.created_at DESC, je.id DESC
                LIMIT 1
                """,
                (
                    reversal_descriptor,
                    (datetime.utcnow() - timedelta(hours=72)).isoformat(timespec="seconds") + "Z",
                ),
            ).fetchone()

            if existing_recent is not None:
                if bool(existing_recent["is_filed"]):
                    conn.commit()
                    return {
                        "status": "already_filed",
                        "message": "A filed Rule 37A batch already exists for this period. Skipping duplicate reversal.",
                        "existing_entry_id": int(existing_recent["id"]),
                        "existing_reference": existing_recent["reference"],
                        "filed_at": existing_recent["filed_at"],
                    }

                conn.commit()
                return {
                    "status": "pending_review",
                    "message": "A Rule 37A batch for this period already exists in the last 72 hours. Review and file/archive it before generating another.",
                    "existing_entry_id": int(existing_recent["id"]),
                    "existing_reference": existing_recent["reference"],
                    "generated_at": existing_recent["created_at"],
                }

            pending_export = conn.execute(
                """
                SELECT id, generated_at
                FROM export_history
                WHERE report_type = 'RULE37A_REVERSAL_CSV'
                  AND status = 'GENERATED'
                  AND generated_at >= ?
                ORDER BY generated_at DESC, id DESC
                LIMIT 1
                """,
                ((datetime.utcnow() - timedelta(hours=72)).isoformat(timespec="seconds") + "Z",),
            ).fetchone()

            if pending_export is not None:
                conn.commit()
                return {
                    "status": "pending_review",
                    "message": "Latest exported Rule 37A batch is locked for review. Mark it as filed before generating a new batch.",
                    "locked_export_id": int(pending_export["id"]),
                    "locked_export_generated_at": pending_export["generated_at"],
                }

            if risk_amount <= 0:
                conn.commit()
                return {
                    "status": "noop",
                    "message": "No Rule 37A reversal required for the selected period.",
                    "reversal_risk": risk["rule_37a"],
                }

            expense_account_id = get_account_id_by_name(conn, "Operating Expenses")
            gst_input_account_id = get_account_id_by_name(conn, "GST Input")
            reference = next_journal_reference(conn, posting_date)

            cursor = conn.execute(
                """
                INSERT INTO journal_entries(
                    date,
                    reference,
                    description,
                    supply_source,
                    ims_status,
                    status,
                    reversal_of_id,
                    created_at
                )
                VALUES (?, ?, ?, 'DIRECT', 'PENDING', 'POSTED', NULL, ?)
                """,
                (
                    posting_date.isoformat(),
                    reference,
                    reversal_descriptor,
                    datetime.utcnow().isoformat(timespec="seconds") + "Z",
                ),
            )
            entry_id = int(cursor.lastrowid)

            conn.execute(
                """
                INSERT INTO journal_lines(entry_id, account_id, debit, credit)
                VALUES (?, ?, ?, ?)
                """,
                (entry_id, expense_account_id, money_str(risk_amount), money_str(Decimal("0"))),
            )
            update_account_balance(conn, expense_account_id, risk_amount, Decimal("0"))

            conn.execute(
                """
                INSERT INTO journal_lines(entry_id, account_id, debit, credit)
                VALUES (?, ?, ?, ?)
                """,
                (entry_id, gst_input_account_id, money_str(Decimal("0")), money_str(risk_amount)),
            )
            update_account_balance(conn, gst_input_account_id, Decimal("0"), risk_amount)
            fingerprint = stamp_entry_fingerprint(conn, entry_id)

            log_audit(
                conn,
                table_name="journal_entries",
                record_id=entry_id,
                action="RULE_37A_REVERSAL",
                old_value=None,
                new_value={
                    "reference": reference,
                    "risk_amount": money_str(risk_amount),
                    "entry_fingerprint": fingerprint,
                    "source_references": risk["rule_37a"]["at_risk_references"],
                    "hard_stop": risk["rule_37a"]["hard_stop"],
                    "actor_role": role,
                },
                user_id=admin_id,
                high_priority=True,
            )

            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to generate Rule 37A reversal: {exc}") from exc

    return {
        "status": "generated",
        "message": "Rule 37A reversal journal generated",
        "entry_id": entry_id,
        "reference": reference,
        "reversal_amount": money_str(risk_amount),
        "source_references": risk["rule_37a"]["at_risk_references"],
        "entry_fingerprint": fingerprint,
    }


@app.get("/api/v1/journal/reversal-summary/recent")
def get_recent_reversal_summary(
    hours: int = 72,
    include_filed: bool = False,
    min_credit_balance: Decimal | None = None,
) -> dict[str, Any]:
    safe_hours = min(max(hours, 1), 168)
    cutoff = (datetime.utcnow() - timedelta(hours=safe_hours)).isoformat(timespec="seconds") + "Z"

    items: list[dict[str, Any]] = []
    total_amount = Decimal("0")

    with closing(get_conn()) as conn:
        rows = conn.execute(
            """
            SELECT a.id AS audit_id,
                   a.record_id AS entry_id,
                   a.created_at AS reversal_created_at,
                   a.user_id AS reversal_created_by,
                   a.new_value,
                   je.date,
                   je.reference,
                   je.description,
                   je.is_filed,
                   je.filed_at,
                   je.filed_export_hash,
                   je.approved_by_1,
                   je.approved_by_2,
                   COALESCE(SUM(CAST(jl.debit AS REAL)), 0) AS total_debit,
                   COALESCE(SUM(CAST(jl.credit AS REAL)), 0) AS total_credit
            FROM audit_edit_logs a
            JOIN journal_entries je ON je.id = a.record_id
            LEFT JOIN journal_lines jl ON jl.entry_id = je.id
            WHERE a.table_name = 'journal_entries'
              AND a.action = 'RULE_37A_REVERSAL'
              AND a.created_at >= ?
                            AND (? = 1 OR COALESCE(je.is_filed, 0) = 0)
                        GROUP BY a.id, a.record_id, a.created_at, a.new_value, je.date, je.reference, je.description, je.is_filed, je.filed_at, je.filed_export_hash, je.approved_by_1, je.approved_by_2
            ORDER BY a.created_at DESC, a.id DESC
            """,
                        (cutoff, 1 if include_filed else 0),
        ).fetchall()

        export_audits = conn.execute(
            """
            SELECT user_id, created_at, new_value
            FROM audit_edit_logs
            WHERE table_name = 'reports'
              AND action = 'RULE37A_EXPORT'
              AND created_at >= ?
            ORDER BY created_at DESC, id DESC
            """,
            (cutoff,),
        ).fetchall()

        entry_export_map: dict[int, dict[str, Any]] = {}
        for export_audit in export_audits:
            export_meta = json.loads(export_audit["new_value"]) if export_audit["new_value"] else {}
            export_hash = export_meta.get("export_hash")
            entry_ids = export_meta.get("entry_ids", [])
            if not export_hash or not isinstance(entry_ids, list):
                continue
            for raw_entry_id in entry_ids:
                try:
                    parsed_entry_id = int(raw_entry_id)
                except Exception:  # noqa: BLE001
                    continue
                if parsed_entry_id <= 0:
                    continue
                if parsed_entry_id not in entry_export_map:
                    entry_export_map[parsed_entry_id] = {
                        "export_hash": export_hash,
                        "exported_by": int(export_audit["user_id"]) if int(export_audit["user_id"]) > 0 else None,
                        "exported_at": export_audit["created_at"],
                    }

        export_rows = conn.execute(
            """
            SELECT id,
                   payload_hash,
                   approved_by_1,
                   approved_by_2,
                   last_verified_by,
                   last_verified_at,
                   generated_at
            FROM export_history
            WHERE report_type = 'RULE37A_REVERSAL_CSV'
              AND generated_at >= ?
            ORDER BY generated_at DESC, id DESC
            """,
            (cutoff,),
        ).fetchall()
        export_by_hash = {str(row["payload_hash"]): row for row in export_rows}

        for row in rows:
            details = json.loads(row["new_value"]) if row["new_value"] else {}
            risk_amount = money(details.get("risk_amount", "0"))
            secondary_required = risk_amount > DUAL_APPROVAL_THRESHOLD
            total_amount += risk_amount

            entry_id = int(row["entry_id"])
            export_hint = entry_export_map.get(entry_id)
            export_hash = row["filed_export_hash"] or (export_hint["export_hash"] if export_hint else None)
            export_meta = export_by_hash.get(str(export_hash)) if export_hash else None

            created_by = int(row["reversal_created_by"]) if int(row["reversal_created_by"]) > 0 else None
            exported_by = None
            exported_at = None
            verified_by = None
            verified_at = None
            approved_by_2 = int(row["approved_by_2"]) if row["approved_by_2"] is not None else None

            if export_meta is not None:
                exported_by = int(export_meta["approved_by_1"]) if export_meta["approved_by_1"] is not None else None
                exported_at = export_meta["generated_at"]
                verified_by = int(export_meta["last_verified_by"]) if export_meta["last_verified_by"] is not None else None
                verified_at = export_meta["last_verified_at"]
                if approved_by_2 is None and export_meta["approved_by_2"] is not None:
                    approved_by_2 = int(export_meta["approved_by_2"])
            elif export_hint is not None:
                exported_by = export_hint["exported_by"]
                exported_at = export_hint["exported_at"]

            items.append(
                {
                    "audit_id": int(row["audit_id"]),
                    "entry_id": entry_id,
                    "reference": row["reference"],
                    "entry_date": row["date"],
                    "description": row["description"],
                    "reversal_created_at": row["reversal_created_at"],
                    "reversal_amount": money_str(risk_amount),
                    "total_debit": money_str(row["total_debit"]),
                    "total_credit": money_str(row["total_credit"]),
                    "source_references": details.get("source_references", []),
                    "hard_stop": bool(details.get("hard_stop", False)),
                    "is_filed": bool(row["is_filed"]),
                    "filed_at": row["filed_at"],
                    "filed_export_hash": row["filed_export_hash"],
                    "approved_by_1": row["approved_by_1"],
                    "approved_by_2": approved_by_2,
                    "secondary_approval_required": secondary_required,
                    "waiting_second_admin": secondary_required and row["approved_by_1"] is not None and approved_by_2 is None,
                    "filing_state": "FILED" if bool(row["is_filed"]) else "PENDING_REVIEW",
                    "approval_timeline": {
                        "created_by": created_by,
                        "exported_by": exported_by,
                        "verified_by": verified_by,
                        "approved_by_2": approved_by_2,
                        "timestamps": {
                            "created_at": row["reversal_created_at"],
                            "exported_at": exported_at,
                            "verified_at": verified_at,
                            "approved_at": row["filed_at"] if bool(row["is_filed"]) else None,
                        },
                    },
                    "safe_harbor": {
                        "min_credit_balance": money_str(min_credit_balance) if min_credit_balance is not None else None,
                        "liability_offset": money_str(money(min_credit_balance) - risk_amount) if min_credit_balance is not None else None,
                        "status": (
                            "SAFE_HARBOR_APPLICABLE"
                            if min_credit_balance is not None and money(min_credit_balance) >= risk_amount
                            else "STANDARD_INTEREST_APPLIES"
                        ),
                        "legal_basis": (
                            "Sec_50(3)_Full_Cover"
                            if min_credit_balance is not None and money(min_credit_balance) >= risk_amount
                            else "Sec_50(3)_Standard_Interest"
                        ),
                    },
                }
            )

    return {
        "window_hours": safe_hours,
        "since": cutoff,
        "include_filed": include_filed,
        "ca_filing_map": {
            "target_return": "GSTR-3B",
            "target_table": "4(B)(2) - Others",
            "usage_note": "Use total_reversal_amount as reversal amount under Others.",
        },
        "count": len(items),
        "total_reversal_amount": money_str(total_amount),
        "entries": items,
    }


@app.get("/api/v1/journal/reversal-summary/recent/export")
def get_recent_reversal_summary_export(
    hours: int = 72,
    min_credit_balance: Decimal | None = None,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> Response:
    safe_hours = min(max(hours, 1), 168)
    cutoff = (datetime.utcnow() - timedelta(hours=safe_hours)).isoformat(timespec="seconds") + "Z"
    exporter_role = require_role(x_role, {"admin", "ca"})
    exporter_admin_id = require_admin_id(x_admin_id)

    report_from = (datetime.utcnow() - timedelta(hours=safe_hours)).date()
    report_to = date.today()

    with closing(get_conn()) as conn:
        period_id = resolve_period_id(conn, report_from, report_to)
        hold_row = conn.execute(
            """
            SELECT security_hold_until
            FROM export_history
            WHERE report_type = 'RULE37A_REVERSAL_CSV'
              AND security_hold_until IS NOT NULL
              AND security_hold_until > ?
              AND (
                    (? IS NOT NULL AND period_id = ?)
                 OR (? IS NULL AND period_id IS NULL AND period_from = ? AND period_to = ?)
              )
            ORDER BY security_hold_until DESC
            LIMIT 1
            """,
            (
                datetime.utcnow().isoformat(timespec="seconds") + "Z",
                period_id,
                period_id,
                period_id,
                report_from.isoformat(),
                report_to.isoformat(),
            ),
        ).fetchone()
        if hold_row is not None:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Export temporarily blocked after failed verification. Cooling-off in effect.",
                    "hold_until": hold_row["security_hold_until"],
                },
            )

        reversal_rows = conn.execute(
            """
            SELECT a.id AS audit_id,
                   a.record_id AS entry_id,
                   a.created_at AS reversal_created_at,
                   a.new_value,
                   je.date,
                   je.reference,
                   je.description
            FROM audit_edit_logs a
            JOIN journal_entries je ON je.id = a.record_id
            WHERE a.table_name = 'journal_entries'
              AND a.action = 'RULE_37A_REVERSAL'
              AND a.created_at >= ?
                            AND COALESCE(je.is_filed, 0) = 0
            ORDER BY a.created_at DESC, a.id DESC
            """,
            (cutoff,),
        ).fetchall()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "Date",
                "Original_Ref",
                "Reversal_Ref",
                "Vendor_GSTIN",
                "Taxable_Value",
                "IGST",
                "CGST",
                "SGST",
                "Reason",
                "GSTR3B_Table",
                "Legal_Basis",
            ]
        )

        exported_entry_ids: list[int] = []

        for row in reversal_rows:
            exported_entry_ids.append(int(row["entry_id"]))
            details = json.loads(row["new_value"]) if row["new_value"] else {}
            source_refs = details.get("source_references", []) or []
            reason = row["description"]
            row_risk_amount = money(details.get("risk_amount", "0"))
            row_safe_harbor = min_credit_balance is not None and money(min_credit_balance) >= row_risk_amount
            legal_basis = "Sec_50(3)_Full_Cover" if row_safe_harbor else "Sec_50(3)_Standard_Interest"

            if not source_refs:
                writer.writerow(
                    [
                        row["date"],
                        "",
                        row["reference"],
                        "",
                        "0.0000",
                        "0.0000",
                        "0.0000",
                        "0.0000",
                        reason,
                        "4(B)(2) - Others",
                        legal_basis,
                    ]
                )
                continue

            for original_ref in source_refs:
                orig = conn.execute(
                    """
                    SELECT je.reference,
                           je.counterparty_gstin,
                           tl.taxable_value,
                           tl.tax_amount,
                           tl.is_inter_state
                    FROM journal_entries je
                    LEFT JOIN tax_ledger tl ON tl.entry_id = je.id
                    WHERE je.reference = ?
                    ORDER BY tl.id ASC
                    LIMIT 1
                    """,
                    (original_ref,),
                ).fetchone()

                if orig is None:
                    writer.writerow(
                        [
                            row["date"],
                            original_ref,
                            row["reference"],
                            "",
                            "0.0000",
                            "0.0000",
                            "0.0000",
                            "0.0000",
                            reason,
                            "4(B)(2) - Others",
                            legal_basis,
                        ]
                    )
                    continue

                taxable_value = money(orig["taxable_value"] if orig["taxable_value"] is not None else "0")
                tax_amount = money(orig["tax_amount"] if orig["tax_amount"] is not None else "0")
                if bool(orig["is_inter_state"]):
                    igst = tax_amount
                    cgst = Decimal("0")
                    sgst = Decimal("0")
                else:
                    igst = Decimal("0")
                    cgst = money(tax_amount / Decimal("2"))
                    sgst = money(tax_amount / Decimal("2"))

                writer.writerow(
                    [
                        row["date"],
                        orig["reference"],
                        row["reference"],
                        orig["counterparty_gstin"] or "",
                        money_str(taxable_value),
                        money_str(igst),
                        money_str(cgst),
                        money_str(sgst),
                        reason,
                        "4(B)(2) - Others",
                        legal_basis,
                    ]
                )

    csv_content = output.getvalue()
    output.close()
    payload_fingerprint = sha256(csv_content.encode("utf-8")).hexdigest()
    version_hash = sha256(
        f"{payload_fingerprint}:{datetime.utcnow().isoformat(timespec='seconds')}Z".encode("utf-8")
    ).hexdigest()
    export_id: int | None = None

    with closing(get_conn()) as conn:
        try:
            conn.execute("BEGIN")
            export_id = create_export_history(
                conn,
                report_type="RULE37A_REVERSAL_CSV",
                period_id=period_id,
                report_from=report_from,
                report_to=report_to,
                payload_hash=version_hash,
                payload_fingerprint=payload_fingerprint,
                approved_by_1=exporter_admin_id,
                status="GENERATED",
            )
            log_audit(
                conn,
                table_name="reports",
                record_id=export_id,
                action="RULE37A_EXPORT",
                old_value=None,
                new_value={
                    "export_id": export_id,
                    "window_hours": safe_hours,
                    "entry_count": len(set(exported_entry_ids)),
                    "entry_ids": sorted(set(exported_entry_ids)),
                    "payload_fingerprint": payload_fingerprint,
                    "export_hash": version_hash,
                    "actor_role": exporter_role,
                },
                high_priority=True,
            )
            conn.commit()
        except Exception:  # noqa: BLE001
            conn.rollback()
            # CSV generation should still succeed even if export history logging fails.

    filename = f"Accord_Rule37A_Audit_{date.today().isoformat()}.csv"
    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={filename}",
            "X-Accord-Export-Id": str(export_id or ""),
            "X-Accord-Payload-Fingerprint": payload_fingerprint,
            "X-Accord-Export-Hash": version_hash,
            "X-Accord-Entry-Ids": ",".join(str(entry_id) for entry_id in sorted(set(exported_entry_ids))),
        },
    )


@app.post("/api/v1/journal/reversal-summary/approve")
def post_approve_reversal_summary(
    payload: ReversalApproveIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    x_biometric_token: str | None = Header(default=None, alias="X-Biometric-Token"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    require_biometric_signoff(x_biometric_token, admin_id, "REVERSAL_APPROVE")

    entry_ids = sorted(set(int(entry_id) for entry_id in payload.entry_ids if int(entry_id) > 0))
    if not entry_ids:
        raise HTTPException(status_code=422, detail="entry_ids must include at least one positive integer")

    with closing(get_conn()) as conn:
        try:
            conn.execute("BEGIN")

            placeholders = ",".join("?" for _ in entry_ids)
            rows = conn.execute(
                f"""
                SELECT je.id,
                       je.reference,
                       je.is_filed,
                       je.approved_by_1,
                       je.approved_by_2,
                       (
                           SELECT a.new_value
                           FROM audit_edit_logs a
                           WHERE a.table_name = 'journal_entries'
                             AND a.record_id = je.id
                             AND a.action = 'RULE_37A_REVERSAL'
                           ORDER BY a.id DESC
                           LIMIT 1
                       ) AS reversal_meta
                FROM journal_entries je
                WHERE je.id IN ({placeholders})
                  AND EXISTS (
                      SELECT 1
                      FROM audit_edit_logs a
                      WHERE a.table_name = 'journal_entries'
                        AND a.record_id = je.id
                        AND a.action = 'RULE_37A_REVERSAL'
                  )
                """,
                tuple(entry_ids),
            ).fetchall()

            found_ids = {int(row["id"]) for row in rows}
            missing_ids = [entry_id for entry_id in entry_ids if entry_id not in found_ids]
            if missing_ids:
                raise HTTPException(
                    status_code=404,
                    detail={
                        "message": "Some entry_ids are not valid Rule 37A reversal entries.",
                        "missing_entry_ids": missing_ids,
                    },
                )

            if any(bool(row["is_filed"]) for row in rows):
                raise HTTPException(status_code=409, detail="Cannot approve entries that are already filed")

            total_amount = Decimal("0")
            for row in rows:
                meta = json.loads(row["reversal_meta"]) if row["reversal_meta"] else {}
                total_amount += money(meta.get("risk_amount", "0"))

            secondary_required = total_amount > DUAL_APPROVAL_THRESHOLD

            approved_1_now: list[int] = []
            approved_2_now: list[int] = []
            already_approved: list[int] = []
            waiting_second_admin: list[int] = []

            for row in rows:
                entry_id = int(row["id"])
                first = row["approved_by_1"]
                second = row["approved_by_2"]

                if first is None:
                    conn.execute("UPDATE journal_entries SET approved_by_1 = ? WHERE id = ?", (admin_id, entry_id))
                    approved_1_now.append(entry_id)
                    continue

                if int(first) == admin_id and second is None and secondary_required:
                    waiting_second_admin.append(entry_id)
                    continue

                if second is None and int(first) != admin_id:
                    conn.execute("UPDATE journal_entries SET approved_by_2 = ? WHERE id = ?", (admin_id, entry_id))
                    approved_2_now.append(entry_id)
                    continue

                already_approved.append(entry_id)

            export_snapshot: dict[str, Any] | None = None
            if payload.export_hash:
                export_row = conn.execute(
                    """
                    SELECT id, approved_by_1, approved_by_2
                    FROM export_history
                    WHERE payload_hash = ?
                      AND report_type = 'RULE37A_REVERSAL_CSV'
                    LIMIT 1
                    """,
                    (payload.export_hash,),
                ).fetchone()
                if export_row is None:
                    raise HTTPException(status_code=404, detail="Export hash not found for Rule37A CSV")

                first_export = export_row["approved_by_1"]
                second_export = export_row["approved_by_2"]
                export_id = int(export_row["id"])

                if first_export is None:
                    conn.execute("UPDATE export_history SET approved_by_1 = ? WHERE id = ?", (admin_id, export_id))
                    first_export = admin_id
                elif int(first_export) != admin_id and second_export is None:
                    conn.execute("UPDATE export_history SET approved_by_2 = ? WHERE id = ?", (admin_id, export_id))
                    second_export = admin_id

                export_snapshot = {
                    "export_id": export_id,
                    "approved_by_1": first_export,
                    "approved_by_2": second_export,
                }

            log_audit(
                conn,
                table_name="journal_entries",
                record_id=entry_ids[0],
                action="RULE_37A_APPROVAL",
                old_value=None,
                new_value={
                    "entry_ids": entry_ids,
                    "approved_1_now": approved_1_now,
                    "approved_2_now": approved_2_now,
                    "already_approved": already_approved,
                    "waiting_second_admin": waiting_second_admin,
                    "secondary_required": secondary_required,
                    "total_reversal_amount": money_str(total_amount),
                    "note": payload.note.strip() if payload.note else None,
                    "actor_role": role,
                },
                user_id=admin_id,
                high_priority=True,
            )

            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to approve reversal batch: {exc}") from exc

    any_waiting = False
    for row in rows:
        first = row["approved_by_1"]
        second = row["approved_by_2"]
        if int(row["id"]) in approved_1_now:
            first = admin_id
        if int(row["id"]) in approved_2_now:
            second = admin_id
        if secondary_required and (first is None or second is None):
            any_waiting = True
            break

    return {
        "message": "Approval recorded",
        "threshold": money_str(DUAL_APPROVAL_THRESHOLD),
        "total_reversal_amount": money_str(total_amount),
        "secondary_required": secondary_required,
        "waiting_second_admin": any_waiting,
        "approved_1_now": approved_1_now,
        "approved_2_now": approved_2_now,
        "already_approved": already_approved,
        "export": export_snapshot,
    }


@app.post("/api/v1/journal/reversal-summary/archive")
def post_archive_reversal_summary(
    payload: ReversalArchiveIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    x_biometric_token: str | None = Header(default=None, alias="X-Biometric-Token"),
) -> dict[str, Any]:
    role = require_role(x_role, {"admin", "ca"})
    admin_id = require_admin_id(x_admin_id)
    require_biometric_signoff(x_biometric_token, admin_id, "REVERSAL_ARCHIVE")
    if not payload.export_hash:
        raise HTTPException(status_code=422, detail="export_hash is required to mark a batch as filed")
    entry_ids = sorted(set(int(entry_id) for entry_id in payload.entry_ids if int(entry_id) > 0))
    if not entry_ids:
        raise HTTPException(status_code=422, detail="entry_ids must include at least one positive integer")

    with closing(get_conn()) as conn:
        try:
            conn.execute("BEGIN")

            placeholders = ",".join("?" for _ in entry_ids)
            rows = conn.execute(
                f"""
                SELECT je.id,
                       je.reference,
                                             je.is_filed,
                                             je.approved_by_1,
                                             je.approved_by_2,
                                             (
                                                     SELECT a.new_value
                                                     FROM audit_edit_logs a
                                                     WHERE a.table_name = 'journal_entries'
                                                         AND a.record_id = je.id
                                                         AND a.action = 'RULE_37A_REVERSAL'
                                                     ORDER BY a.id DESC
                                                     LIMIT 1
                                             ) AS reversal_meta
                FROM journal_entries je
                JOIN audit_edit_logs a
                  ON a.table_name = 'journal_entries'
                 AND a.record_id = je.id
                 AND a.action = 'RULE_37A_REVERSAL'
                WHERE je.id IN ({placeholders})
                                GROUP BY je.id, je.reference, je.is_filed, je.approved_by_1, je.approved_by_2
                """,
                tuple(entry_ids),
            ).fetchall()

            found_ids = {int(row["id"]) for row in rows}
            missing_ids = [entry_id for entry_id in entry_ids if entry_id not in found_ids]
            if missing_ids:
                raise HTTPException(
                    status_code=404,
                    detail={
                        "message": "Some entry_ids are not valid Rule 37A reversal entries.",
                        "missing_entry_ids": missing_ids,
                    },
                )

            to_archive = [int(row["id"]) for row in rows if not bool(row["is_filed"])]
            skipped_already_filed = [int(row["id"]) for row in rows if bool(row["is_filed"])]
            batch_total = Decimal("0")
            for row in rows:
                meta = json.loads(row["reversal_meta"]) if row["reversal_meta"] else {}
                batch_total += money(meta.get("risk_amount", "0"))

            secondary_required = batch_total > DUAL_APPROVAL_THRESHOLD
            missing_first = [int(row["id"]) for row in rows if row["approved_by_1"] is None]
            missing_second = [int(row["id"]) for row in rows if secondary_required and row["approved_by_2"] is None]
            duplicate_signers = [
                int(row["id"])
                for row in rows
                if secondary_required and row["approved_by_1"] is not None and row["approved_by_1"] == row["approved_by_2"]
            ]
            if missing_first or missing_second or duplicate_signers:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "message": "Batch is not fully approved for filing.",
                        "secondary_required": secondary_required,
                        "missing_first_approval": missing_first,
                        "missing_second_approval": missing_second,
                        "duplicate_signers": duplicate_signers,
                    },
                )

            now_ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"

            if to_archive:
                placeholders_to_archive = ",".join("?" for _ in to_archive)
                conn.execute(
                    f"""
                    UPDATE journal_entries
                    SET is_filed = 1,
                        filed_at = ?,
                        filed_export_hash = COALESCE(?, filed_export_hash)
                    WHERE id IN ({placeholders_to_archive})
                    """,
                    (now_ts, payload.export_hash, *to_archive),
                )

            row_map = {int(row["id"]): row for row in rows}
            for entry_id in to_archive:
                row = row_map[entry_id]
                log_audit(
                    conn,
                    table_name="journal_entries",
                    record_id=entry_id,
                    action="RULE_37A_FILED",
                    old_value={"is_filed": False},
                    new_value={
                        "is_filed": True,
                        "reference": row["reference"],
                        "filed_at": now_ts,
                        "export_hash": payload.export_hash,
                        "note": payload.note.strip() if payload.note else None,
                        "actor_role": role,
                    },
                    user_id=admin_id,
                    high_priority=True,
                )

            if payload.export_hash:
                export_row = conn.execute(
                    """
                    SELECT id,
                           approved_by_1,
                           approved_by_2,
                              last_verification_status,
                              security_hold_until
                    FROM export_history
                    WHERE payload_hash = ?
                      AND report_type = 'RULE37A_REVERSAL_CSV'
                    LIMIT 1
                    """,
                    (payload.export_hash,),
                ).fetchone()
                if export_row is None:
                    raise HTTPException(status_code=404, detail="Export hash not found for Rule37A CSV")
                if export_row["security_hold_until"] is not None and export_row["security_hold_until"] > now_ts:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "message": "Archive blocked during verification cooling-off window.",
                            "hold_until": export_row["security_hold_until"],
                        },
                    )
                if export_row["last_verification_status"] != "MATCHED":
                    raise HTTPException(
                        status_code=409,
                        detail="Cannot mark as filed: export verification is missing or failed.",
                    )
                if export_row["approved_by_1"] is None:
                    raise HTTPException(status_code=409, detail="Export record is missing first approval")
                if secondary_required and export_row["approved_by_2"] is None:
                    raise HTTPException(status_code=409, detail="Export record is missing secondary approval")
                if secondary_required and export_row["approved_by_1"] == export_row["approved_by_2"]:
                    raise HTTPException(status_code=409, detail="Secondary approval must be from a different admin")

                conn.execute("UPDATE export_history SET status = 'FILED' WHERE id = ?", (int(export_row["id"]),))

            conn.commit()
        except HTTPException:
            conn.rollback()
            raise
        except Exception as exc:  # noqa: BLE001
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Failed to archive reversal summary batch: {exc}") from exc

    return {
        "message": "Batch marked as filed",
        "archived_count": len(to_archive),
        "archived_entry_ids": to_archive,
        "skipped_already_filed": skipped_already_filed,
        "export_hash": payload.export_hash,
        "total_reversal_amount": money_str(batch_total),
        "secondary_required": secondary_required,
    }


@app.get("/api/v1/insights/friday-summary")
def get_friday_summary(as_of_date: date | None = None, min_credit_balance: Decimal | None = None) -> dict[str, Any]:
    today = as_of_date or date.today()

    with closing(get_conn()) as conn:
        gstins = conn.execute(
            """
            SELECT DISTINCT counterparty_gstin
            FROM journal_entries
            WHERE counterparty_gstin IS NOT NULL
            """
        ).fetchall()

        conn.execute("BEGIN")
        for row in gstins:
            recompute_vendor_trust(conn, str(row["counterparty_gstin"]))
        conn.commit()

        ims_counts = conn.execute(
            """
            SELECT ims_status, COUNT(*) AS cnt
            FROM journal_entries
            WHERE counterparty_gstin IS NOT NULL
            GROUP BY ims_status
            """
        ).fetchall()

        itc_row = conn.execute(
            """
            SELECT COALESCE(SUM(CAST(total_itc_at_risk AS REAL)), 0) AS total_itc_at_risk
            FROM vendor_trust_scores
            """
        ).fetchone()

        risky_vendors = conn.execute(
            """
            SELECT gstin,
                   legal_name,
                   filing_consistency_score,
                   avg_filing_delay_days,
                   last_gstr1_filed_at,
                   total_itc_at_risk
            FROM vendor_trust_scores
            WHERE CAST(total_itc_at_risk AS REAL) > 0
            ORDER BY filing_consistency_score ASC, CAST(total_itc_at_risk AS REAL) DESC
            LIMIT 10
            """
        ).fetchall()

        reversal_risk = compute_reversal_risk(conn, today, min_credit_balance=min_credit_balance)

        missing_actor_rows = conn.execute(
            """
            SELECT table_name, record_id, action, created_at
            FROM audit_edit_logs
            WHERE high_priority = 1
              AND action IN ('RULE_37A_REVERSAL', 'RULE37A_EXPORT', 'EXPORT_VERIFY', 'RULE_37A_APPROVAL', 'RULE_37A_FILED')
                            AND (
                                        user_id = 0
                                 OR new_value IS NULL
                                 OR INSTR(new_value, '"actor_role"') = 0
                            )
              AND created_at >= ?
            ORDER BY created_at DESC, id DESC
            LIMIT 25
            """,
            ((datetime.utcnow() - timedelta(days=7)).isoformat(timespec="seconds") + "Z",),
        ).fetchall()

    ims_bucket = {"ACCEPTED": 0, "REJECTED": 0, "PENDING": 0}
    for row in ims_counts:
        status = normalize_ims_status(str(row["ims_status"]))
        ims_bucket[status] = int(row["cnt"])

    high_risk_friday = today.day >= 12 and ims_bucket["PENDING"] > 0
    critical_reversal_action = bool(reversal_risk["rule_37a"]["hard_stop"])
    security_risk_detected = len(missing_actor_rows) > 0

    return {
        "as_of_date": today.isoformat(),
        "critical_action_required": critical_reversal_action,
        "security_risk_detected": security_risk_detected,
        "dashboard_priority": "REVERSAL_RISKS" if critical_reversal_action else "IMS_ACTIONABLES",
        "reversal_risks": {
            **reversal_risk["rule_37a"],
            "status": "CRITICAL_ACTION_REQUIRED" if critical_reversal_action else "MONITOR",
        },
        "high_risk_friday": high_risk_friday,
        "ims_actionables": ims_bucket,
        "summary": {
            "total_itc_at_risk": money_str(itc_row["total_itc_at_risk"]),
            "vendors_at_risk_count": len(risky_vendors),
            "pending_invoices": ims_bucket["PENDING"],
            "total_potential_interest_savings": reversal_risk["rule_37a"]["projected_annual_interest_18pct"],
        },
        "vendor_risk_ranking": [
            {
                **dict(row),
                "payment_advice": payment_advice_for_score(float(row["filing_consistency_score"])),
            }
            for row in risky_vendors
        ],
        "deadline_context": {
            "deemed_acceptance_deadline_day": 14,
            "alert_day": 12,
            "message": "Resolve PENDING IMS actions before the 14th to protect ITC" if high_risk_friday else "IMS queue is within safe threshold",
        },
        "security_risks": {
            "missing_actor_identity_count": len(missing_actor_rows),
            "missing_actor_events": [dict(row) for row in missing_actor_rows],
        },
    }
