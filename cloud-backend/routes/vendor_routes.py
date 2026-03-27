from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable

from fastapi import APIRouter, File, HTTPException, UploadFile

from services.ai_invoice_parser import LocalAIParser


def create_vendor_router(get_conn: Callable[[], sqlite3.Connection]) -> APIRouter:
    router = APIRouter(prefix="/api/v1/vendor", tags=["vendor", "invoices"])
    parser = LocalAIParser()

    def ensure_schema(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS vendor_invoice_submissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                vendor_link_id TEXT NOT NULL,
                filename TEXT,
                content_type TEXT,
                invoice_payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING_APPROVAL',
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_vendor_submissions_link ON vendor_invoice_submissions(vendor_link_id, created_at DESC)"
        )

    @router.post("/upload/{vendor_link_id}")
    async def post_vendor_invoice_upload(vendor_link_id: str, file: UploadFile = File(...)) -> dict[str, Any]:
        clean_link_id = vendor_link_id.strip()
        if not clean_link_id:
            raise HTTPException(status_code=422, detail="vendor_link_id is required")

        content = await file.read()
        text_payload = ""
        content_type = str(file.content_type or "").lower()

        if file.filename and file.filename.lower().endswith(".txt"):
            text_payload = content.decode("utf-8", errors="ignore")
        elif content_type.startswith("text/"):
            text_payload = content.decode("utf-8", errors="ignore")
        else:
            # OCR is intentionally mocked for image/PDF uploads in this phase.
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
        total_tax = f"{(cgst + sgst + igst).quantize(Decimal('0.01')):.2f}"

        created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        payload = {
            "vendor_link_id": clean_link_id,
            "filename": file.filename,
            "content_type": file.content_type,
            "parsed_at": created_at,
            "extracted": {
                **extracted,
                "total_tax": total_tax,
            },
        }

        with get_conn() as conn:
            ensure_schema(conn)
            cursor = conn.execute(
                """
                INSERT INTO vendor_invoice_submissions(vendor_link_id, filename, content_type, invoice_payload, status, created_at)
                VALUES (?, ?, ?, ?, 'PENDING_APPROVAL', ?)
                """,
                (
                    clean_link_id,
                    file.filename,
                    file.content_type,
                    json.dumps(payload),
                    created_at,
                ),
            )
            submission_id = int(cursor.lastrowid)

        return {
            "status": "ok",
            "submission_id": submission_id,
            "vendor_link_id": clean_link_id,
            "approval_status": "PENDING_APPROVAL",
            "extracted": payload["extracted"],
        }

    return router
