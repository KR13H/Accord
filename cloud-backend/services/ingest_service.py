from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from fastapi import HTTPException


class IngestService:
    """Handles Omni-reader style ingestion and mobile vision sync flows.

    This service is designed for camera-first ingestion into the accounting engine.
    """

    def __init__(
        self,
        *,
        ram_disk_buffer: Path,
        extract_text_with_tesseract: Callable[[Path], str],
        extract_receipt_fields: Callable[..., Any],
        parse_amount_from_text: Callable[[str], Any],
        money_str: Callable[[Any], str],
        accounting_service: Any,
    ) -> None:
        """Initializes ingest dependencies.

        Args:
            ram_disk_buffer: Temporary high-speed storage root.
            extract_text_with_tesseract: OCR extractor callable.
            extract_receipt_fields: Vision-model extractor callable.
            parse_amount_from_text: Numeric parser callable.
            money_str: Monetary formatter callable.
            accounting_service: Accounting service used for ledger posting.

        Hardware Impact:
            Uses RAM disk to reduce SSD churn and maximize ingest throughput.
        Logic Invariants:
            Rejects empty/invalid image payloads before ledger posting.
        Legal Context:
            Preserves source evidence path for later audit trace reconstruction.
        """
        self.ram_disk_buffer = ram_disk_buffer
        self.extract_text_with_tesseract = extract_text_with_tesseract
        self.extract_receipt_fields = extract_receipt_fields
        self.parse_amount_from_text = parse_amount_from_text
        self.money_str = money_str
        self.accounting_service = accounting_service

    async def mobile_sync(
        self,
        *,
        filename: str,
        content_type: str,
        raw: bytes,
        role: str,
        admin_id: int,
    ) -> dict[str, Any]:
        """Processes a mobile-captured image and syncs it to the ledger.

        Args:
            filename: Original uploaded filename.
            content_type: Uploaded MIME type.
            raw: Raw image bytes.
            role: Authenticated actor role.
            admin_id: Acting admin identity.

        Returns:
            Response payload with posting and extraction metadata.

        Hardware Impact:
            Saturates Neural Engine path when Llava-backed extraction is active.
        Logic Invariants:
            Requires a positive inferred amount before posting.
        Legal Context:
            Creates an immutable ledger trail for GST evidence and reconciliation.
        """
        if not filename:
            raise HTTPException(status_code=400, detail="Missing mobile capture file name")
        if not raw:
            raise HTTPException(status_code=400, detail="Uploaded mobile capture is empty")

        normalized_type = (content_type or "").lower()
        allowed = filename.lower().endswith((".png", ".jpg", ".jpeg", ".webp")) or normalized_type.startswith("image/")
        if not allowed:
            raise HTTPException(status_code=422, detail="Mobile sync supports image uploads only")

        self.ram_disk_buffer.mkdir(parents=True, exist_ok=True)
        ext = Path(filename).suffix.lower() or ".jpg"
        staged_name = f"mobile_sync_{Path(filename).stem}_{admin_id}{ext}"
        staged_path = self.ram_disk_buffer / staged_name
        staged_path.write_bytes(raw)

        try:
            ocr_text = self.extract_text_with_tesseract(staged_path)
            extracted, model_response = await self.extract_receipt_fields(staged_path, ocr_text)

            amount = self.parse_amount_from_text(str(extracted.get("total_amount", "0")))
            if amount <= 0:
                amount = self.parse_amount_from_text(ocr_text)
            if amount <= 0:
                raise HTTPException(status_code=422, detail="Unable to detect a valid amount from mobile capture")

            entry = self.accounting_service.post_extracted_entry(
                extracted=extracted,
                fallback_text=ocr_text,
                description_prefix="Mobile Ghost-Ledger",
                actor_role=role,
                admin_id=admin_id,
                source_file_path=staged_path,
                model_response=model_response,
                import_status="MOBILE_SYNC",
            )

            return {
                "status": "processed",
                "pipeline": "MOBILE_GHOST_LEDGER",
                "mobile_capture_active": True,
                "ram_disk_path": str(staged_path),
                "entry_id": entry.get("entry_id"),
                "reference": entry.get("reference"),
                "entry_fingerprint": entry.get("entry_fingerprint"),
                "extracted": {
                    "vendor": extracted.get("vendor", ""),
                    "gstin": extracted.get("gstin", ""),
                    "total_amount": self.money_str(amount),
                },
                "raw_context": {
                    "ocr_preview": ocr_text[:320],
                    "model_response": str(model_response)[:600],
                },
                "audit_meta": {
                    "actor_role": role,
                    "admin_id": admin_id,
                    "evidence_file": str(staged_path),
                    "extracted_snapshot": json.dumps(extracted),
                },
            }
        finally:
            staged_path.unlink(missing_ok=True)
