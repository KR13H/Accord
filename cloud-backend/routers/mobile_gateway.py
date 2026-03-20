from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from typing import Any
import json
import os
import uuid

import httpx
from fastapi import APIRouter, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
from services.realtime_events import publish_ca_event
from services.tally_algorithms_service import TallyAlgorithmsService

router = APIRouter(prefix="/api/v2/mobile", tags=["Mobile Gateway"])


# In-memory state for Phase A scaffolding. This keeps the router self-contained
# and avoids breaking the current DB migration path.
MOBILE_SESSIONS: dict[str, dict[str, Any]] = {}
SME_CA_LINKS: dict[int, dict[str, Any]] = {}
UPI_TRANSACTIONS: dict[str, dict[str, Any]] = {}
VOICE_STREAM_SESSIONS: dict[str, dict[str, Any]] = {}
ALGO_SERVICE = TallyAlgorithmsService()
VOICE_SYNC_URL = os.getenv("VOICE_SYNC_URL", "http://127.0.0.1:8000/api/v1/ledger/voice-sync").strip()


class MobileSessionIn(BaseModel):
    device_id: str = Field(min_length=6, max_length=128)
    platform: str = Field(default="android", min_length=2, max_length=20)
    app_version: str = Field(default="2.8.0", min_length=1, max_length=30)


class ConnectCAIn(BaseModel):
    sme_id: int = Field(gt=0)
    ca_id: int = Field(gt=0)
    sme_name: str = Field(min_length=2, max_length=120)
    ca_firm_name: str = Field(min_length=2, max_length=200)


class GstApprovalIn(BaseModel):
    filing_id: int = Field(gt=0)
    sme_id: int = Field(gt=0)
    approved_by_ca_id: int = Field(gt=0)


class UpiIntentIn(BaseModel):
    sme_id: int = Field(gt=0)
    filing_id: int = Field(gt=0)
    amount_inr: Decimal = Field(gt=0)
    challan_reference: str = Field(min_length=6, max_length=80)


class UpiWebhookIn(BaseModel):
    payment_id: str = Field(min_length=8, max_length=80)
    gateway_status: str = Field(min_length=2, max_length=30)
    utr: str | None = Field(default=None, max_length=40)


class InventoryLayerIn(BaseModel):
    batch_id: str = Field(min_length=1, max_length=80)
    quantity: Decimal = Field(gt=0)
    unit_cost: Decimal = Field(ge=0)


class InventoryValuationIn(BaseModel):
    method: str = Field(pattern="^(FIFO|LIFO)$")
    issue_quantity: Decimal = Field(gt=0)
    layers: list[InventoryLayerIn] = Field(min_length=1)


class GodownTransferIn(BaseModel):
    sku_code: str = Field(min_length=1, max_length=80)
    from_godown: str = Field(min_length=2, max_length=80)
    to_godown: str = Field(min_length=2, max_length=80)
    quantity: Decimal = Field(gt=0)


class BRSRowIn(BaseModel):
    reference: str = Field(min_length=1, max_length=120)
    amount: Decimal


class BRSReconcileIn(BaseModel):
    bank_rows: list[BRSRowIn] = Field(min_length=1)
    ledger_rows: list[BRSRowIn] = Field(min_length=1)


class VoiceSessionStartIn(BaseModel):
    sme_id: int = Field(gt=0)
    ca_id: int = Field(gt=0)
    language: str = Field(default="en-IN", min_length=2, max_length=20)


class VoiceChunkIn(BaseModel):
    session_id: str = Field(min_length=8, max_length=120)
    chunk_text: str = Field(min_length=1, max_length=1200)


class VoiceCommitIn(BaseModel):
    session_id: str = Field(min_length=8, max_length=120)
    currency_code: str | None = Field(default=None, min_length=3, max_length=3)
    exchange_rate: Decimal | None = Field(default=None, gt=0)


def require_mobile_actor(x_role: str | None, x_admin_id: str | None) -> int:
    role = (x_role or "").strip().lower()
    if role not in {"admin", "ca", "sme"}:
        raise HTTPException(status_code=403, detail="Mobile role required")
    if not x_admin_id or not str(x_admin_id).strip().isdigit():
        raise HTTPException(status_code=403, detail="Valid X-Admin-Id required")
    return int(str(x_admin_id).strip())


async def _forward_voice_sync(
    *,
    transcript: str,
    x_role: str,
    x_admin_id: str,
    currency_code: str | None = None,
    exchange_rate: Decimal | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"transcript": transcript}
    if currency_code:
        payload["currency_code"] = currency_code
    if exchange_rate is not None:
        payload["exchange_rate"] = f"{exchange_rate:.4f}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        res = await client.post(
            VOICE_SYNC_URL,
            json=payload,
            headers={
                "X-Role": x_role,
                "X-Admin-Id": x_admin_id,
                "Content-Type": "application/json",
            },
        )
    data = res.json()
    if not res.is_success:
        raise HTTPException(status_code=502, detail=data.get("detail") or "Voice sync pipeline unavailable")
    return data


@router.post("/auth/session")
def create_mobile_session(
    payload: MobileSessionIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    actor_id = require_mobile_actor(x_role, x_admin_id)
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    token = f"mob_{uuid.uuid4().hex}"

    MOBILE_SESSIONS[token] = {
        "device_id": payload.device_id.strip(),
        "platform": payload.platform.strip().lower(),
        "app_version": payload.app_version.strip(),
        "actor_id": actor_id,
        "created_at": now_iso,
        "last_seen_at": now_iso,
    }

    return {
        "status": "ok",
        "session_token": token,
        "ca_linked": actor_id in SME_CA_LINKS,
        "optimistic_ack_ms": 120,
        "created_at": now_iso,
    }


@router.post("/connect-ca")
def connect_sme_to_ca(
    payload: ConnectCAIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    _ = require_mobile_actor(x_role, x_admin_id)
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    SME_CA_LINKS[payload.sme_id] = {
        "sme_id": payload.sme_id,
        "sme_name": payload.sme_name.strip(),
        "ca_id": payload.ca_id,
        "ca_firm_name": payload.ca_firm_name.strip(),
        "linked_at": now_iso,
    }

    return {
        "status": "ok",
        "message": "SME linked to CA network",
        "link": SME_CA_LINKS[payload.sme_id],
    }


@router.get("/connections/{sme_id}")
def get_sme_connection(
    sme_id: int,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    _ = require_mobile_actor(x_role, x_admin_id)
    link = SME_CA_LINKS.get(sme_id)
    if link is None:
        raise HTTPException(status_code=404, detail="SME is not linked to a CA yet")
    return {"status": "ok", "link": link}


@router.post("/gst/approve")
def approve_gst_from_mobile(
    payload: GstApprovalIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    _ = require_mobile_actor(x_role, x_admin_id)
    link = SME_CA_LINKS.get(payload.sme_id)
    if link is None:
        raise HTTPException(status_code=404, detail="SME is not linked to CA network")
    if int(link["ca_id"]) != payload.approved_by_ca_id:
        raise HTTPException(status_code=403, detail="Approving CA does not match linked CA")

    return {
        "status": "ok",
        "filing_id": payload.filing_id,
        "approval_status": "APPROVED",
        "approved_by_ca_id": payload.approved_by_ca_id,
        "approved_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "push_notification": f"Your CA has approved filing {payload.filing_id}.",
    }


@router.post("/tax/upi/intent")
def create_upi_tax_intent(
    payload: UpiIntentIn,
    request: Request,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    _ = require_mobile_actor(x_role, x_admin_id)
    if payload.sme_id not in SME_CA_LINKS:
        raise HTTPException(status_code=404, detail="SME must be linked before UPI tax payment")

    payment_id = f"upi_{uuid.uuid4().hex[:18]}"
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    raw_seed = f"{payment_id}:{payload.filing_id}:{payload.amount_inr}:{payload.challan_reference}"
    checksum = sha256(raw_seed.encode("utf-8")).hexdigest()

    base_url = str(request.base_url).rstrip("/")
    webhook = f"{base_url}/api/v2/mobile/tax/upi/webhook"

    UPI_TRANSACTIONS[payment_id] = {
        "payment_id": payment_id,
        "sme_id": payload.sme_id,
        "filing_id": payload.filing_id,
        "amount_inr": f"{payload.amount_inr:.2f}",
        "challan_reference": payload.challan_reference,
        "status": "PENDING",
        "gateway": "RAZORPAY_OR_CASHFREE",
        "checksum": checksum,
        "created_at": now_iso,
        "updated_at": now_iso,
    }

    return {
        "status": "ok",
        "payment_id": payment_id,
        "workflow_state": "UPI_INTENT_CREATED",
        "upi_intent": {
            "intent_uri": f"upi://pay?pa=gst.gov@upi&pn=GST%20Portal&am={payload.amount_inr:.2f}&tn={payload.challan_reference}",
            "gateway": "RAZORPAY_OR_CASHFREE",
            "webhook": webhook,
        },
        "ledger_state": "TAX_PAYMENT_PENDING",
    }


@router.post("/tax/upi/webhook")
def handle_upi_webhook(payload: UpiWebhookIn) -> dict[str, Any]:
    txn = UPI_TRANSACTIONS.get(payload.payment_id)
    if txn is None:
        raise HTTPException(status_code=404, detail="Unknown payment_id")

    normalized = payload.gateway_status.strip().upper()
    ledger_state = "TAX_PAYMENT_PENDING"
    if normalized in {"SUCCESS", "CAPTURED", "PAID"}:
        txn["status"] = "PAID"
        ledger_state = "TAX_PAID"
    elif normalized in {"FAILED", "DECLINED"}:
        txn["status"] = "FAILED"
        ledger_state = "TAX_PAYMENT_FAILED"
    else:
        txn["status"] = normalized

    txn["utr"] = payload.utr or ""
    txn["updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    return {
        "status": "ok",
        "payment_id": payload.payment_id,
        "gateway_status": txn["status"],
        "ledger_state": ledger_state,
        "updated_at": txn["updated_at"],
    }


@router.get("/transactions")
def list_mobile_transactions(
    status: str = "ALL",
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    _ = require_mobile_actor(x_role, x_admin_id)
    normalized = status.strip().upper()

    rows = list(UPI_TRANSACTIONS.values())
    if normalized != "ALL":
        rows = [row for row in rows if str(row.get("status", "")).upper() == normalized]

    rows = sorted(rows, key=lambda item: str(item.get("updated_at", "")), reverse=True)
    return {"status": "ok", "count": len(rows), "items": rows}


@router.post("/algorithms/inventory/valuation")
def post_inventory_valuation(
    payload: InventoryValuationIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    _ = require_mobile_actor(x_role, x_admin_id)
    try:
        result = ALGO_SERVICE.inventory_valuation(
            method=payload.method,
            issue_quantity=payload.issue_quantity,
            layers=[layer.dict() for layer in payload.layers],
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return result


@router.post("/algorithms/godown/transfer")
def post_godown_transfer_plan(
    payload: GodownTransferIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    _ = require_mobile_actor(x_role, x_admin_id)
    try:
        result = ALGO_SERVICE.plan_stock_transfer(
            sku_code=payload.sku_code,
            from_godown=payload.from_godown,
            to_godown=payload.to_godown,
            quantity=payload.quantity,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return result


@router.post("/algorithms/brs/reconcile")
def post_brs_reconcile(
    payload: BRSReconcileIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    _ = require_mobile_actor(x_role, x_admin_id)
    return ALGO_SERVICE.reconcile_bank_statement(
        bank_rows=[row.dict() for row in payload.bank_rows],
        ledger_rows=[row.dict() for row in payload.ledger_rows],
    )


@router.post("/voice/session/start")
def post_voice_session_start(
    payload: VoiceSessionStartIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    actor_id = require_mobile_actor(x_role, x_admin_id)
    if payload.sme_id not in SME_CA_LINKS:
        raise HTTPException(status_code=404, detail="SME must be linked to CA before voice sync")
    if int(SME_CA_LINKS[payload.sme_id]["ca_id"]) != payload.ca_id:
        raise HTTPException(status_code=403, detail="Voice session CA mismatch")

    session_id = f"vsession_{uuid.uuid4().hex}"
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    VOICE_STREAM_SESSIONS[session_id] = {
        "session_id": session_id,
        "sme_id": payload.sme_id,
        "ca_id": payload.ca_id,
        "actor_id": actor_id,
        "chunks": [],
        "status": "LISTENING",
        "language": payload.language,
        "created_at": now_iso,
        "updated_at": now_iso,
        "committed_entry": None,
    }
    return {
        "status": "ok",
        "session_id": session_id,
        "draft_status": "QUEUED",
        "ws_stream": f"/api/v2/mobile/voice/stream/{session_id}",
        "created_at": now_iso,
    }


@router.post("/voice/session/chunk")
def post_voice_session_chunk(
    payload: VoiceChunkIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    _ = require_mobile_actor(x_role, x_admin_id)
    session = VOICE_STREAM_SESSIONS.get(payload.session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Voice session not found")
    chunk = payload.chunk_text.strip()
    if not chunk:
        raise HTTPException(status_code=422, detail="chunk_text cannot be blank")

    session["chunks"].append(chunk)
    session["updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    session["status"] = "STREAMING"
    return {
        "status": "ok",
        "session_id": payload.session_id,
        "chunks_received": len(session["chunks"]),
        "last_chunk": chunk,
    }


@router.post("/voice/session/commit")
async def post_voice_session_commit(
    payload: VoiceCommitIn,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    role = (x_role or "admin").strip().lower()
    admin_id = str(require_mobile_actor(x_role, x_admin_id))
    session = VOICE_STREAM_SESSIONS.get(payload.session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Voice session not found")

    transcript = " ".join(str(item).strip() for item in session["chunks"] if str(item).strip()).strip()
    if not transcript:
        raise HTTPException(status_code=422, detail="No transcript chunks available to commit")

    sync_result = await _forward_voice_sync(
        transcript=transcript,
        x_role=role,
        x_admin_id=admin_id,
        currency_code=payload.currency_code,
        exchange_rate=payload.exchange_rate,
    )
    session["status"] = "COMMITTED"
    session["updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    session["committed_entry"] = {
        "entry_id": sync_result.get("entry_id"),
        "reference": sync_result.get("reference"),
        "entry_fingerprint": sync_result.get("entry_fingerprint"),
    }
    await publish_ca_event(
        {
            "ca_id": int(session.get("ca_id") or 0),
            "type": "VOICE_ENTRY",
            "source": "MOBILE_COMMIT",
            "sme_id": int(session.get("sme_id") or 0),
            "summary": f"Voice entry posted: {sync_result.get('reference') or 'UNKNOWN'}",
            "reference": sync_result.get("reference"),
            "entry_id": sync_result.get("entry_id"),
            "entry_fingerprint": sync_result.get("entry_fingerprint"),
            "occurred_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        }
    )

    return {
        "status": "ok",
        "session_id": payload.session_id,
        "pipeline": "V2.4_VOICE_SYNC",
        "transcript": transcript,
        "ledger_result": sync_result,
    }


@router.get("/voice/session/{session_id}")
def get_voice_session(
    session_id: str,
    x_role: str | None = Header(default=None, alias="X-Role"),
    x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
) -> dict[str, Any]:
    _ = require_mobile_actor(x_role, x_admin_id)
    session = VOICE_STREAM_SESSIONS.get(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Voice session not found")
    return {"status": "ok", "session": session}


@router.websocket("/voice/stream/{session_id}")
async def ws_voice_stream(websocket: WebSocket, session_id: str) -> None:
    await websocket.accept()
    session = VOICE_STREAM_SESSIONS.get(session_id)
    if session is None:
        await websocket.send_json({"status": "error", "detail": "Voice session not found"})
        await websocket.close(code=4404)
        return

    await websocket.send_json({"status": "ok", "event": "connected", "session_id": session_id})
    try:
        while True:
            raw = await websocket.receive_text()
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_json({"status": "error", "detail": "Invalid JSON payload"})
                continue

            msg_type = str(message.get("type") or "chunk").strip().lower()
            if msg_type == "chunk":
                chunk = str(message.get("text") or "").strip()
                if not chunk:
                    await websocket.send_json({"status": "error", "detail": "Chunk text required"})
                    continue
                session["chunks"].append(chunk)
                session["status"] = "STREAMING"
                session["updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                await websocket.send_json(
                    {
                        "status": "ok",
                        "event": "chunk_accepted",
                        "chunks_received": len(session["chunks"]),
                    }
                )
                continue

            if msg_type == "commit":
                transcript = " ".join(str(item).strip() for item in session["chunks"] if str(item).strip()).strip()
                if not transcript:
                    await websocket.send_json({"status": "error", "detail": "No chunks to commit"})
                    continue
                role = str(message.get("role") or "admin").strip().lower() or "admin"
                admin_id = str(message.get("admin_id") or "1001").strip() or "1001"
                currency_code = str(message.get("currency_code") or "").strip().upper() or None
                exchange_rate = message.get("exchange_rate")
                exchange = Decimal(str(exchange_rate)) if exchange_rate is not None and str(exchange_rate).strip() else None
                try:
                    sync_result = await _forward_voice_sync(
                        transcript=transcript,
                        x_role=role,
                        x_admin_id=admin_id,
                        currency_code=currency_code,
                        exchange_rate=exchange,
                    )
                except HTTPException as exc:
                    await websocket.send_json({"status": "error", "detail": exc.detail})
                    continue

                session["status"] = "COMMITTED"
                session["updated_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
                session["committed_entry"] = {
                    "entry_id": sync_result.get("entry_id"),
                    "reference": sync_result.get("reference"),
                    "entry_fingerprint": sync_result.get("entry_fingerprint"),
                }
                await publish_ca_event(
                    {
                        "ca_id": int(session.get("ca_id") or 0),
                        "type": "VOICE_ENTRY",
                        "source": "MOBILE_WEBSOCKET_COMMIT",
                        "sme_id": int(session.get("sme_id") or 0),
                        "summary": f"Voice entry posted: {sync_result.get('reference') or 'UNKNOWN'}",
                        "reference": sync_result.get("reference"),
                        "entry_id": sync_result.get("entry_id"),
                        "entry_fingerprint": sync_result.get("entry_fingerprint"),
                        "occurred_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    }
                )
                await websocket.send_json({"status": "ok", "event": "committed", "ledger_result": sync_result})
                continue

            await websocket.send_json({"status": "error", "detail": "Unsupported message type"})
    except WebSocketDisconnect:
        return
