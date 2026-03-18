"""Iron-SIGHT M3 Parallel Vision Processing Endpoints
High-throughput OCR and Tally XML batch processing for enterprise accounting.
"""

from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from datetime import datetime
from decimal import Decimal
from typing import List, Any
import json
import asyncio
import uuid
from contextlib import closing

# NOTE: This file contains endpoint implementations to be integrated into main.py
# These endpoints enable:
# 1. Batch photo upload with parallel Tesseract + Llava processing (10 workers on M3)
# 2. Bulk Tally XML export aggregating 1000+ entries into master XML

async def upload_photo_batch_impl(
    files: List,  # UploadFile
    role: str,
    admin_id: int,
    get_conn,
    check_period_lock,
    next_journal_reference,
    get_account_id_by_name,
    extract_text_with_tesseract,
    extract_receipt_fields,
    parse_date_from_text,
    parse_amount_from_text,
    money_str,
    money,
    update_account_balance,
    log_audit,
    RECEIPT_STORAGE_DIR,
    RAM_DISK_BUFFER,
    MAX_PARALLEL_WORKERS,
) -> dict[str, Any]:
    """Iron-SIGHT Batch Vision Processor."""
    if not files or len(files) == 0:
        raise Exception("No files provided")
    
    if len(files) > 100:
        raise Exception("Batch size limited to 100")

    RECEIPT_STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    RAM_DISK_BUFFER.mkdir(parents=True, exist_ok=True)

    results: list[dict[str, Any]] = []
    failed: list[dict[str, str]] = []
    
    staged_images: list[tuple[Path, bytes]] = []
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
            staged_images.append((staged_path, raw))
        except Exception as exc:
            failed.append({"filename": file.filename or "unknown", "error": str(exc)})

    if not staged_images:
        raise Exception("No valid images to process")

    def process_receipt_worker(staged_path_str: str, admin_id_val: int):
        """Worker for parallel processing."""
        try:
            image_path = Path(staged_path_str)
            ocr_text = extract_text_with_tesseract(image_path)
            
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            extracted, model_response = loop.run_until_complete(
                extract_receipt_fields(image_path, ocr_text)
            )
            loop.close()
            
            imported_date = parse_date_from_text(str(extracted.get("date", "")))
            vendor_name = str(extracted.get("vendor", "")).strip() or "Receipt Vendor"
            gstin = str(extracted.get("gstin", "")).strip().upper()
            hsn = str(extracted.get("hsn", "")).strip()
            amount = parse_amount_from_text(str(extracted.get("total_amount", "0")))
            
            if amount <= 0:
                amount = parse_amount_from_text(ocr_text)
            if amount <= 0:
                return {"status": "failed", "error": "Unable to detect amount"}
            
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
                            date, reference, description, counterparty_gstin, supply_source,
                            ims_status, vendor_legal_name, status, created_at
                        ) VALUES (?, ?, ?, ?, 'DIRECT', 'PENDING', ?, 'POSTED', ?)
                        """,
                        (imported_date.isoformat(), reference, f"Vision batch: {vendor_name}",
                         gstin or None, vendor_name, created_at),
                    )
                    entry_id = int(conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
                    
                    conn.execute(
                        "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
                        (entry_id, purchases_id, money_str(amount), "0.0000"),
                    )
                    update_account_balance(conn, purchases_id, amount, Decimal("0"))
                    
                    conn.execute(
                        "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, ?)",
                        (entry_id, payable_id, "0.0000", money_str(amount)),
                    )
                    update_account_balance(conn, payable_id, Decimal("0"), amount)
                    
                    conn.execute(
                        """
                        INSERT INTO receipt_imports(entry_id, file_path, ocr_text, extracted_json,
                                                   model_response, status, created_by, created_at)
                        VALUES (?, ?, ?, ?, ?, 'PROCESSED', ?, ?)
                        """,
                        (entry_id, str(image_path), ocr_text or None,
                         json.dumps(extracted) if extracted else None,
                         model_response or None, admin_id_val, created_at),
                    )
                    
                    log_audit(conn, "journal_entries", entry_id, "VISION_BATCH_IMPORT", None,
                             {"reference": reference, "vendor": vendor_name, "amount": money_str(amount)},
                             user_id=admin_id_val, high_priority=True)
                    conn.commit()
                    
                    return {"status": "processed", "entry_id": entry_id, "reference": reference}
                except Exception as exc:
                    conn.rollback()
                    return {"status": "failed", "error": str(exc)}
        except Exception as exc:
            return {"status": "failed", "error": str(exc)}
    
    with ProcessPoolExecutor(max_workers=MAX_PARALLEL_WORKERS) as executor:
        futures = [
            executor.submit(process_receipt_worker, str(sp), admin_id)
            for sp, _ in staged_images
        ]
        
        for future in futures:
            try:
                result = future.result(timeout=120)
                if result["status"] == "processed":
                    results.append(result)
                else:
                    failed.append(result)
            except Exception as exc:
                failed.append({"error": str(exc)})
    
    try:
        for sp, _ in staged_images:
            if sp.exists():
                sp.unlink()
    except Exception:
        pass

    return {
        "status": "batch_processed",
        "total_processed": len(results),
        "total_failed": len(failed),
        "results": results,
        "failed": failed if failed else None,
    }
