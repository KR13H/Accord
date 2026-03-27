from __future__ import annotations

import csv
import io
import sqlite3
from contextlib import closing
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable

from thefuzz import fuzz


def _normalize_text(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _money(value: Any) -> Decimal:
    cleaned = str(value or "0").replace(",", "")
    return Decimal(cleaned).quantize(Decimal("0.01"))


class BankRecoService:
    def __init__(self, *, get_conn: Callable[[], sqlite3.Connection]) -> None:
        self.get_conn = get_conn

    def _read_csv_rows(self, raw: bytes) -> list[dict[str, str]]:
        text = raw.decode("utf-8", errors="ignore")
        reader = csv.DictReader(io.StringIO(text))
        rows: list[dict[str, str]] = []
        for row in reader:
            normalized = {str(k or "").strip().lower(): str(v or "").strip() for k, v in row.items()}
            rows.append(normalized)
        return rows

    def _extract_bank_fields(self, row: dict[str, str]) -> tuple[str, str, str]:
        date_value = row.get("date") or row.get("txn date") or row.get("transaction date") or ""
        amount_value = row.get("amount") or row.get("debit") or row.get("credit") or "0"
        remarks_value = row.get("remarks") or row.get("narration") or row.get("description") or ""
        return date_value, amount_value, remarks_value

    def _ledger_candidates(self) -> list[dict[str, str]]:
        with closing(self.get_conn()) as conn:
            conn.row_factory = sqlite3.Row
            bookings = conn.execute(
                """
                SELECT booking_id, customer_name, total_consideration
                FROM sales_bookings
                ORDER BY created_at DESC
                LIMIT 1000
                """
            ).fetchall()
            rents = conn.execute(
                """
                SELECT id, tenant_id, amount
                FROM rent_invoices
                ORDER BY id DESC
                LIMIT 1000
                """
            ).fetchall()

        candidates: list[dict[str, str]] = []
        for row in bookings:
            candidates.append(
                {
                    "entity_type": "booking",
                    "entity_id": str(row["booking_id"]),
                    "display": f"{row['customer_name']} {row['booking_id']}",
                    "amount": str(row["total_consideration"]),
                }
            )
        for row in rents:
            candidates.append(
                {
                    "entity_type": "rent_invoice",
                    "entity_id": str(row["id"]),
                    "display": f"Tenant {row['tenant_id']} RINV-{row['id']}",
                    "amount": str(row["amount"]),
                }
            )
        return candidates

    def reconcile(self, *, csv_bytes: bytes, fuzzy_threshold: int = 70, amount_tolerance: Decimal = Decimal("1.00")) -> dict[str, Any]:
        bank_rows = self._read_csv_rows(csv_bytes)
        candidates = self._ledger_candidates()
        suggested: list[dict[str, Any]] = []
        unmatched: list[dict[str, Any]] = []

        for index, row in enumerate(bank_rows, start=1):
            date_raw, amount_raw, remarks_raw = self._extract_bank_fields(row)
            try:
                amount = _money(amount_raw)
            except Exception:
                amount = Decimal("0.00")

            normalized_remarks = _normalize_text(remarks_raw)
            best_match: dict[str, Any] | None = None

            for candidate in candidates:
                score = fuzz.partial_ratio(normalized_remarks, _normalize_text(candidate["display"]))
                candidate_amount = _money(candidate["amount"])
                amount_delta = abs(candidate_amount - amount)

                if score < fuzzy_threshold:
                    continue
                if amount_delta > amount_tolerance:
                    continue

                if best_match is None or score > int(best_match["score"]):
                    best_match = {
                        "entity_type": candidate["entity_type"],
                        "entity_id": candidate["entity_id"],
                        "display": candidate["display"],
                        "score": score,
                        "amount_delta": f"{amount_delta:.2f}",
                    }

            row_payload = {
                "row_number": index,
                "date": date_raw,
                "amount": f"{amount:.2f}",
                "remarks": remarks_raw,
                "parsed_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            }
            if best_match is None:
                unmatched.append(row_payload)
            else:
                suggested.append({**row_payload, "match": best_match})

        return {
            "suggested_matches": suggested,
            "unmatched_exceptions": unmatched,
        }
