from __future__ import annotations

import os
import sqlite3
from importlib import import_module
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from services.storage_service import get_storage_service

try:
    from workers.celery_app import celery
except Exception:  # noqa: BLE001
    celery = None

DB_PATH = Path(__file__).resolve().parents[1] / "ledger.db"
DEMANDS_DIR = Path(os.getenv("ACCORD_DEMANDS_DIR", "/app/demands"))
PUBLIC_DEMAND_BASE_URL = os.getenv("ACCORD_PUBLIC_DEMAND_BASE_URL", "https://example.com/demands").rstrip("/")


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def _money(value: str | Decimal) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"))


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def ensure_demand_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS project_construction_stages (
            project_id TEXT PRIMARY KEY,
            construction_stage TEXT NOT NULL,
            milestone_percent TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS demand_letters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id TEXT NOT NULL,
            booking_id TEXT NOT NULL,
            customer_name TEXT NOT NULL,
            milestone_percent TEXT NOT NULL,
            amount_due TEXT NOT NULL,
            pdf_path TEXT NOT NULL,
            created_at TEXT NOT NULL,
            whatsapp_status TEXT NOT NULL DEFAULT 'PENDING'
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_demand_letters_project ON demand_letters(project_id, created_at DESC)"
    )


def _write_pdf(
    *,
    project_id: str,
    booking_id: str,
    customer_name: str,
    construction_stage: str,
    milestone_percent: Decimal,
    amount_due: Decimal,
) -> Path:
    DEMANDS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    safe_booking = booking_id.replace("/", "-")
    file_path = DEMANDS_DIR / f"demand_{project_id}_{safe_booking}_{stamp}.pdf"

    pdf = canvas.Canvas(str(file_path), pagesize=A4)
    pdf.setTitle("Accord Demand Letter")
    pdf.setFont("Helvetica-Bold", 16)
    pdf.drawString(48, 800, "Accord Real Estate - Milestone Demand Letter")

    pdf.setFont("Helvetica", 11)
    pdf.drawString(48, 770, f"Date: {datetime.utcnow().date().isoformat()}")
    pdf.drawString(48, 750, f"Project ID: {project_id}")
    pdf.drawString(48, 730, f"Booking ID: {booking_id}")
    pdf.drawString(48, 710, f"Buyer: {customer_name}")

    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(48, 675, f"Construction Stage: {construction_stage}")
    pdf.drawString(48, 655, f"Milestone Percentage: {milestone_percent:.2f}%")
    pdf.drawString(48, 635, f"Amount Due: INR {amount_due:.2f}")

    pdf.setFont("Helvetica", 11)
    pdf.drawString(48, 595, "Please clear this demand within 7 working days from the letter date.")
    pdf.drawString(48, 575, "For queries, contact your relationship manager or Accord support.")
    pdf.drawString(48, 545, "This is a system-generated financial demand notice.")

    pdf.showPage()
    pdf.save()
    return file_path


def _notify_whatsapp(booking_id: str, media_url: str) -> str:
    try:
        module = import_module("services.whatsapp_service")
        send_demand_letter_notification = getattr(module, "send_demand_letter_notification")
        result = send_demand_letter_notification(
            booking_id=booking_id,
            media_url=media_url,
        )
        return str(result.get("status", "QUEUED"))
    except Exception:
        return "PENDING"


def _generate_milestone_demands_impl(project_id: str, construction_stage: str, milestone_percent: str | Decimal) -> dict[str, Any]:
    milestone = _money(milestone_percent)
    if milestone <= 0 or milestone > 100:
        raise ValueError("milestone_percent must be between 0 and 100")

    generated = 0
    created_at = _now_iso()

    storage = get_storage_service()

    with _conn() as conn:
        ensure_demand_schema(conn)
        conn.execute(
            """
            INSERT INTO project_construction_stages(project_id, construction_stage, milestone_percent, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(project_id) DO UPDATE SET
                construction_stage = excluded.construction_stage,
                milestone_percent = excluded.milestone_percent,
                updated_at = excluded.updated_at
            """,
            (project_id, construction_stage, f"{milestone:.2f}", created_at),
        )

        bookings = conn.execute(
            """
            SELECT booking_id, customer_name, total_consideration
            FROM sales_bookings
            WHERE project_id = ?
              AND UPPER(COALESCE(status, 'ACTIVE')) IN ('ACTIVE', 'BOOKED', 'SOLD')
            """,
            (project_id,),
        ).fetchall()

        for booking in bookings:
            total_consideration = _money(booking["total_consideration"])
            amount_due = (total_consideration * milestone / Decimal("100")).quantize(Decimal("0.01"))
            pdf_path = _write_pdf(
                project_id=project_id,
                booking_id=str(booking["booking_id"]),
                customer_name=str(booking["customer_name"] or "Customer"),
                construction_stage=construction_stage,
                milestone_percent=milestone,
                amount_due=amount_due,
            )
            stored = storage.put_bytes(
                key=f"demand-letters/{project_id}/{Path(pdf_path).name}",
                payload=pdf_path.read_bytes(),
                content_type="application/pdf",
            )
            media_url = stored.get("url") or f"{PUBLIC_DEMAND_BASE_URL}/{Path(pdf_path).name}"
            whatsapp_status = _notify_whatsapp(str(booking["booking_id"]), str(media_url))
            conn.execute(
                """
                INSERT INTO demand_letters(
                    project_id,
                    booking_id,
                    customer_name,
                    milestone_percent,
                    amount_due,
                    pdf_path,
                    created_at,
                    whatsapp_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project_id,
                    str(booking["booking_id"]),
                    str(booking["customer_name"] or "Customer"),
                    f"{milestone:.2f}",
                    f"{amount_due:.2f}",
                    str(stored.get("uri") or pdf_path),
                    created_at,
                    whatsapp_status,
                ),
            )
            pdf_path.unlink(missing_ok=True)
            generated += 1

        conn.commit()

    return {
        "status": "ok",
        "project_id": project_id,
        "construction_stage": construction_stage,
        "milestone_percent": f"{milestone:.2f}",
        "generated_letters": generated,
        "demands_dir": str(DEMANDS_DIR),
    }


if celery is not None:

    @celery.task(name="workers.demand_generator.generate_milestone_demands")
    def generate_milestone_demands(project_id: str, construction_stage: str, milestone_percent: str) -> dict[str, Any]:
        return _generate_milestone_demands_impl(project_id, construction_stage, milestone_percent)

else:

    def generate_milestone_demands(project_id: str, construction_stage: str, milestone_percent: str) -> dict[str, Any]:
        return _generate_milestone_demands_impl(project_id, construction_stage, milestone_percent)
