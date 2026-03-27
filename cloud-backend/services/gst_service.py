from __future__ import annotations

import re
from datetime import date
from decimal import Decimal
from typing import Any, Callable


class GstService:
    """Compliance filing helpers for V2.5 maker-checker workflows."""

    GSTIN_PATTERN = re.compile(r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$")

    def __init__(
        self,
        *,
        parse_amount_from_text: Callable[[str | None], Decimal],
        money_str: Callable[[Any], str],
    ) -> None:
        self.parse_amount_from_text = parse_amount_from_text
        self.money_str = money_str

    def validate_gstin(self, gstin: str) -> bool:
        normalized = (gstin or "").strip().upper()
        return bool(self.GSTIN_PATTERN.fullmatch(normalized))

    def period_bounds(self, period: str) -> tuple[date, date]:
        """Parses YYYY-MM and returns inclusive start/end dates for filtering."""
        year_str, month_str = period.split("-")
        year = int(year_str)
        month = int(month_str)
        start = date(year, month, 1)
        if month == 12:
            end = date(year + 1, 1, 1).fromordinal(date(year + 1, 1, 1).toordinal() - 1)
        else:
            end = date(year, month + 1, 1).fromordinal(date(year, month + 1, 1).toordinal() - 1)
        return start, end

    def prepare_gstr1(self, rows: list[dict[str, Any]]) -> tuple[dict[str, str], list[dict[str, Any]], str]:
        issues: list[dict[str, Any]] = []
        taxable_total = Decimal("0")

        for row in rows:
            entry_id = int(row.get("entry_id") or 0)
            gstin = str(row.get("gstin") or "").strip().upper()
            supply_type = str(row.get("supply_type") or "B2CS").strip().upper()
            currency_code = str(row.get("currency_code") or "INR").strip().upper() or "INR"
            taxable_value = self.parse_amount_from_text(str(row.get("taxable_value") or "0"))
            exchange_rate = self.parse_amount_from_text(str(row.get("exchange_rate") or "1"))

            if currency_code != "INR" and exchange_rate <= 0:
                issues.append(
                    {
                        "entry_id": entry_id,
                        "severity": "BLOCKER",
                        "issue_type": "FX_MISMATCH",
                        "message": "Foreign-currency entry is missing a valid exchange rate",
                    }
                )

            if supply_type == "B2B":
                if not gstin:
                    issues.append(
                        {
                            "entry_id": entry_id,
                            "severity": "BLOCKER",
                            "issue_type": "MISSING_GSTIN",
                            "message": "B2B entry requires counterparty GSTIN",
                        }
                    )
                elif not self.validate_gstin(gstin):
                    issues.append(
                        {
                            "entry_id": entry_id,
                            "severity": "BLOCKER",
                            "issue_type": "INVALID_GSTIN",
                            "message": f"Invalid GSTIN format: {gstin}",
                        }
                    )

            fx_multiplier = exchange_rate if currency_code != "INR" else Decimal("1")
            taxable_total += taxable_value * fx_multiplier

        blocker_count = sum(1 for issue in issues if issue["severity"] == "BLOCKER")
        warning_count = sum(1 for issue in issues if issue["severity"] == "WARNING")
        status = "VALIDATION_FAILED" if blocker_count > 0 else "READY_FOR_REVIEW"

        summary = {
            "taxable": self.money_str(taxable_total),
            "igst": "0.0000",
            "cgst": "0.0000",
            "sgst": "0.0000",
            "total_entries": str(len(rows)),
            "blocker_count": str(blocker_count),
            "warning_count": str(warning_count),
        }
        return summary, issues, status
