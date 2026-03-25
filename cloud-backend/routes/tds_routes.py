from __future__ import annotations

import io
import json
import sqlite3
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable

from fastapi import APIRouter, File, Header, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from services.ai_lease_generator import AiLeaseGenerator
from services.bank_reco_service import BankRecoService
from services.commission_service import CommissionService
from services.einvoice_generator import EInvoiceGenerator
from services.tds_service import TdsService


class LeaseDraftIn(BaseModel):
    tenant_id: str = Field(min_length=1, max_length=64)
    property_id: str = Field(min_length=1, max_length=64)
    language: str = Field(default="en", min_length=2, max_length=2)


def create_phase7_router(get_conn: Callable[[], sqlite3.Connection], require_role: Callable[..., str], require_admin_id: Callable[..., int]) -> APIRouter:
    router = APIRouter(prefix="/api/v1", tags=["phase7"])
    tds_service = TdsService(get_conn=get_conn)
    einvoice_service = EInvoiceGenerator(get_conn=get_conn)
    lease_service = AiLeaseGenerator(get_conn=get_conn)
    bank_reco_service = BankRecoService(get_conn=get_conn)
    commission_service = CommissionService(get_conn=get_conn)

    def require_ops_or_admin(x_role: str | None, x_admin_id: str | None) -> None:
        require_role(x_role, {"ops", "admin", "ca"})
        require_admin_id(x_admin_id)

    @router.get("/tds/pending")
    def get_pending_tds_dashboard(
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_ops_or_admin(x_role, x_admin_id)
        dashboard = tds_service.pending_dashboard()
        return {
            "status": "ok",
            **dashboard,
        }

    @router.get("/einvoice/{invoice_id}")
    def get_einvoice_json(
        invoice_id: str,
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_ops_or_admin(x_role, x_admin_id)
        try:
            payload = einvoice_service.generate_payload(invoice_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {
            "status": "ok",
            "invoice_id": invoice_id,
            "payload": payload,
        }

    @router.get("/einvoice/{invoice_id}/download")
    def download_einvoice_json(
        invoice_id: str,
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> StreamingResponse:
        require_ops_or_admin(x_role, x_admin_id)
        try:
            payload = einvoice_service.generate_payload(invoice_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        raw = json.dumps(payload, indent=2).encode("utf-8")
        return StreamingResponse(
            io.BytesIO(raw),
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename=einvoice-{invoice_id}.json"},
        )

    @router.post("/leases/draft")
    def post_draft_lease(
        payload: LeaseDraftIn,
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_ops_or_admin(x_role, x_admin_id)
        try:
            drafted = lease_service.generate(
                tenant_id=payload.tenant_id,
                property_id=payload.property_id,
                language=payload.language,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

        return {
            "status": "ok",
            "draft": drafted,
        }

    @router.post("/bank-reconciliation/upload")
    async def post_bank_reconciliation_upload(
        statement_file: UploadFile = File(...),
        fuzzy_threshold: int = 70,
        amount_tolerance: Decimal = Decimal("1.00"),
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_ops_or_admin(x_role, x_admin_id)
        payload = await statement_file.read()
        if not payload:
            raise HTTPException(status_code=422, detail="Uploaded statement_file is empty")
        result = bank_reco_service.reconcile(
            csv_bytes=payload,
            fuzzy_threshold=max(0, min(100, int(fuzzy_threshold))),
            amount_tolerance=amount_tolerance,
        )
        return {
            "status": "ok",
            "filename": statement_file.filename,
            **result,
        }

    @router.get("/commissions/pending")
    def get_pending_commissions(
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_ops_or_admin(x_role, x_admin_id)
        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            commission_service.ensure_schema(conn)
            rows = conn.execute(
                """
                SELECT id, broker_id, booking_id, commission_rate, amount, status, created_at, updated_at
                FROM broker_commissions
                WHERE status IN ('PENDING_ALLOCATION', 'READY_TO_PAY')
                ORDER BY created_at DESC
                """
            ).fetchall()
        return {
            "status": "ok",
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "count": len(rows),
            "items": [dict(row) for row in rows],
        }

    @router.get("/cashflow/predictive")
    def get_predictive_cashflow(
        months: int = 6,
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_ops_or_admin(x_role, x_admin_id)
        horizon = max(1, min(12, int(months)))
        milestone_splits = [
            ("Foundation", Decimal("0.20"), 30),
            ("Slab 1", Decimal("0.20"), 75),
            ("Slab 2", Decimal("0.20"), 120),
            ("Brickwork", Decimal("0.20"), 165),
            ("Possession", Decimal("0.20"), 210),
        ]

        with get_conn() as conn:
            conn.row_factory = sqlite3.Row
            booking_rows = conn.execute(
                """
                SELECT booking_id, booking_date, total_consideration
                FROM sales_bookings
                WHERE status = 'ACTIVE'
                """
            ).fetchall()
            historic_rows = conn.execute(
                """
                SELECT substr(created_at, 1, 7) AS month_key,
                       COALESCE(SUM(CAST(rera_amount AS REAL)), 0) AS rera_inflow
                FROM rera_allocation_events
                WHERE status = 'POSTED'
                GROUP BY substr(created_at, 1, 7)
                ORDER BY month_key ASC
                LIMIT 12
                """
            ).fetchall()

        projected: dict[str, Decimal] = {}
        now = datetime.utcnow().date()
        max_month_key = f"{now.year:04d}-{now.month:02d}"

        for booking in booking_rows:
            booking_date = datetime.utcnow().date()
            try:
                booking_date = datetime.fromisoformat(str(booking["booking_date"])).date()
            except Exception:
                pass
            consideration = Decimal(str(booking["total_consideration"] or "0"))
            for _, ratio, day_offset in milestone_splits:
                due_date = booking_date.fromordinal(booking_date.toordinal() + day_offset)
                month_key = f"{due_date.year:04d}-{due_date.month:02d}"
                if month_key < max_month_key:
                    continue
                projected[month_key] = projected.get(month_key, Decimal("0")) + (consideration * ratio * Decimal("0.70"))

        future_sorted = sorted(projected.items())[:horizon]
        return {
            "status": "ok",
            "historical": [
                {
                    "month": str(row["month_key"]),
                    "rera_inflow": f"{Decimal(str(row['rera_inflow'])).quantize(Decimal('0.01')):.2f}",
                }
                for row in historic_rows
            ],
            "projection": [
                {
                    "month": month,
                    "projected_rera_inflow": f"{amount.quantize(Decimal('0.01')):.2f}",
                }
                for month, amount in future_sorted
            ],
        }

    return router
