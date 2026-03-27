from __future__ import annotations

from datetime import datetime
from typing import Any


def reset_demo_environment(db: Any, *, statutory_service: Any | None = None) -> dict[str, Any]:
    """Resets deterministic demo state for investor-mode runs.

    Steps:
    1) Remove demo-tagged journal entries and linked rows.
    2) Reopen non-open alert states.
    3) Clear filing idempotency cache when available.
    """
    now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    demo_entry_rows = db.execute(
        """
        SELECT id
        FROM journal_entries
        WHERE reference LIKE 'DEMO/%'
           OR description LIKE '%demo%'
           OR vendor_legal_name = 'Demo Risk Vendor'
        """
    ).fetchall()
    demo_entry_ids = [int(row["id"]) for row in demo_entry_rows]

    deleted_tax_rows = 0
    deleted_line_rows = 0
    deleted_import_rows = 0
    deleted_hold_rows = 0
    deleted_entry_rows = 0

    if demo_entry_ids:
        marks = ",".join("?" for _ in demo_entry_ids)
        deleted_tax_rows = db.execute(
            f"DELETE FROM tax_ledger WHERE entry_id IN ({marks})",
            tuple(demo_entry_ids),
        ).rowcount
        deleted_line_rows = db.execute(
            f"DELETE FROM journal_lines WHERE entry_id IN ({marks})",
            tuple(demo_entry_ids),
        ).rowcount
        deleted_import_rows = db.execute(
            f"DELETE FROM receipt_imports WHERE entry_id IN ({marks})",
            tuple(demo_entry_ids),
        ).rowcount
        deleted_hold_rows = db.execute(
            f"DELETE FROM ca_payment_holds WHERE entry_id IN ({marks})",
            tuple(demo_entry_ids),
        ).rowcount
        deleted_entry_rows = db.execute(
            f"DELETE FROM journal_entries WHERE id IN ({marks})",
            tuple(demo_entry_ids),
        ).rowcount

    demo_alert_rows = db.execute(
        """
        SELECT id
        FROM ca_alert_events
        WHERE event_source IN ('AUTO_RULE_EVAL', 'MANUAL')
           OR rule_key = 'GST_RISK_CRITICAL'
        """
    ).fetchall()
    demo_alert_ids = [int(row["id"]) for row in demo_alert_rows]

    deleted_alert_holds = 0
    deleted_alert_rows = 0
    if demo_alert_ids:
        marks = ",".join("?" for _ in demo_alert_ids)
        deleted_alert_holds = db.execute(
            f"DELETE FROM ca_payment_holds WHERE alert_id IN ({marks})",
            tuple(demo_alert_ids),
        ).rowcount
        deleted_alert_rows = db.execute(
            f"DELETE FROM ca_alert_events WHERE id IN ({marks})",
            tuple(demo_alert_ids),
        ).rowcount

    reset_alerts = db.execute(
        """
        UPDATE ca_alert_events
        SET status = 'OPEN',
            acknowledged_by = NULL,
            acknowledged_at = NULL
        WHERE status IN ('ACKNOWLEDGED', 'CLOSED')
        """
    ).rowcount

    idempotency_cleared = 0
    if statutory_service is not None and hasattr(statutory_service, "clear_idempotency_cache"):
        idempotency_cleared = int(statutory_service.clear_idempotency_cache())

    return {
        "status": "ok",
        "signal": "SYSTEM_READY",
        "reset_at": now_iso,
        "reset_summary": {
            "deleted_journal_entries": deleted_entry_rows,
            "deleted_journal_lines": deleted_line_rows,
            "deleted_tax_rows": deleted_tax_rows,
            "deleted_receipt_import_rows": deleted_import_rows,
            "deleted_payment_holds": deleted_hold_rows,
            "deleted_alert_rows": deleted_alert_rows,
            "deleted_alert_holds": deleted_alert_holds,
            "alerts_reopened": reset_alerts,
            "idempotency_cache_cleared": idempotency_cleared,
        },
    }
