from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from fastapi import APIRouter, File, Header, HTTPException, UploadFile

from services.ai_invoice_parser import LocalAIParser
from services.gst_reconciliation import reconcile_itc


def create_invoice_router(get_conn: callable, require_role: callable, require_admin_id: callable) -> APIRouter:
    router = APIRouter(prefix="/api/v1", tags=["invoices", "compliance", "dashboard"])
    parser = LocalAIParser()

    def require_ops_or_admin(x_role: str | None, x_admin_id: str | None) -> None:
        require_role(x_role, {"ops", "admin", "ca"})
        require_admin_id(x_admin_id)

    @router.post("/invoices/parse")
    async def post_parse_invoice(
        file: UploadFile = File(...),
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_ops_or_admin(x_role, x_admin_id)

        content = await file.read()
        text_payload = ""
        content_type = str(file.content_type or "").lower()

        if file.filename and file.filename.lower().endswith(".txt"):
            text_payload = content.decode("utf-8", errors="ignore")
        elif content_type.startswith("text/"):
            text_payload = content.decode("utf-8", errors="ignore")
        else:
            # OCR step is mocked for now when input is image/PDF.
            text_payload = (
                "Vendor: Shree Durga Cement & Building Material\n"
                "GSTIN: 27AAKFD9821M1ZQ\n"
                "HSN Code: 2523\n"
                "Base Amount: 52980\n"
                "CGST: 4768.20\n"
                "SGST: 4768.20\n"
                "IGST: 0\n"
                "Total: 62517.00"
            )

        try:
            extracted = await parser.parse_invoice_text(text_payload)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=422, detail=f"invoice parsing failed: {exc}") from exc

        cgst = Decimal(str(extracted.get("cgst") or "0"))
        sgst = Decimal(str(extracted.get("sgst") or "0"))
        igst = Decimal(str(extracted.get("igst") or "0"))

        return {
            "status": "ok",
            "filename": file.filename,
            "content_type": file.content_type,
            "parsed_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "extracted": {
                **extracted,
                "total_tax": f"{(cgst + sgst + igst).quantize(Decimal('0.01')):.2f}",
            },
        }

    @router.get("/compliance/gst-reconciliation/mock")
    def get_mock_gst_reconciliation(
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_ops_or_admin(x_role, x_admin_id)
        internal = [
            {"gstin": "27AAKFD9821M1ZQ", "invoice_number": "INV-001", "total_tax": "9536.40"},
            {"gstin": "29AAGCS1234K1Z5", "invoice_number": "INV-211", "total_tax": "1200.00"},
        ]
        govt = [
            {"gstin": "27AAKFD9821M1ZQ", "invoice_number": "INV001", "total_tax": "9036.40"},
            {"gstin": "29AAGCS1234K1Z5", "invoice_number": "INV-211", "total_tax": "1200.00"},
        ]
        return reconcile_itc(internal, govt)

    @router.get("/organizations/{org_id}/spvs")
    def get_org_spvs(
        org_id: str,
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_ops_or_admin(x_role, x_admin_id)

        items: list[dict[str, Any]] = []
        try:
            with get_conn() as conn:
                rows = conn.execute(
                    """
                    SELECT id, legal_name, code
                    FROM spvs
                    WHERE parent_org_id = ?
                    ORDER BY legal_name ASC
                    """,
                    (org_id,),
                ).fetchall()
                items = [
                    {
                        "id": str(row["id"]),
                        "name": str(row["legal_name"]),
                        "code": str(row["code"]),
                    }
                    for row in rows
                ]
        except Exception:
            items = []

        if not items:
            items = [
                {"id": "SPV-NOIDA-1", "name": "Noida Residency SPV", "code": "NR1"},
                {"id": "SPV-GGN-2", "name": "Gurugram Heights SPV", "code": "GH2"},
            ]

        return {"status": "ok", "organization_id": org_id, "count": len(items), "items": items}

    @router.get("/dashboard/summary")
    def get_dashboard_summary(
        x_role: str | None = Header(default=None, alias="X-Role"),
        x_admin_id: str | None = Header(default=None, alias="X-Admin-Id"),
    ) -> dict[str, Any]:
        require_ops_or_admin(x_role, x_admin_id)

        total_bookings = 0
        total_consideration = Decimal("0")
        allocated_total = Decimal("0")
        pending_rent_due = Decimal("0")

        with get_conn() as conn:
            booking_row = conn.execute(
                "SELECT COUNT(1) AS c, COALESCE(SUM(CAST(total_consideration AS REAL)), 0) AS s FROM sales_bookings"
            ).fetchone()
            total_bookings = int(booking_row["c"]) if booking_row is not None else 0
            total_consideration = Decimal(str((booking_row["s"] if booking_row is not None else 0) or 0))

            alloc_row = conn.execute(
                "SELECT COALESCE(SUM(CAST(receipt_amount AS REAL)), 0) AS s FROM rera_allocation_events WHERE event_type = 'PAYMENT'"
            ).fetchone()
            allocated_total = Decimal(str((alloc_row["s"] if alloc_row is not None else 0) or 0))

            try:
                rent_row = conn.execute(
                    "SELECT COALESCE(SUM(CAST(amount AS REAL)), 0) AS s FROM rent_invoices WHERE status = 'OPEN'"
                ).fetchone()
                pending_rent_due = Decimal(str((rent_row["s"] if rent_row is not None else 0) or 0))
            except Exception:
                pending_rent_due = Decimal("0")

        awaiting = max(Decimal("0"), total_consideration - allocated_total)
        return {
            "status": "ok",
            "total_bookings": total_bookings,
            "funds_awaiting_rera_allocation": f"{awaiting.quantize(Decimal('0.01')):.2f}",
            "pending_rent_due": f"{pending_rent_due.quantize(Decimal('0.01')):.2f}",
        }

    return router
