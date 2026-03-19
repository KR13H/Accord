from __future__ import annotations

from decimal import Decimal
from typing import Any


MONEY_QUANT = Decimal("0.0001")
GOLDEN_SIX = {"CONTRA", "PAYMENT", "RECEIPT", "JOURNAL", "SALES", "PURCHASE"}


class VoucherService:
    """Classifies vouchers into the Golden Six taxonomy."""

    def _money_4(self, value: Any) -> Decimal:
        return Decimal(str(value or "0")).quantize(MONEY_QUANT)

    def classify_from_lines(self, lines: list[dict[str, Any]]) -> str:
        if not lines:
            return "JOURNAL"

        debit_names = {str(item.get("account_name", "")) for item in lines if self._money_4(item.get("debit")) > 0}
        credit_names = {str(item.get("account_name", "")) for item in lines if self._money_4(item.get("credit")) > 0}

        bank_cash = {"Cash", "Bank"}
        has_bank_cash_debit = bool(debit_names.intersection(bank_cash))
        has_bank_cash_credit = bool(credit_names.intersection(bank_cash))

        if has_bank_cash_debit and has_bank_cash_credit:
            return "CONTRA"
        if has_bank_cash_credit:
            return "PAYMENT"
        if has_bank_cash_debit:
            return "RECEIPT"
        if "Sales Revenue" in credit_names:
            return "SALES"
        if "Purchases" in debit_names:
            return "PURCHASE"
        return "JOURNAL"

    def ensure_golden_six(self, voucher_type: str) -> str:
        token = str(voucher_type or "").strip().upper()
        if token not in GOLDEN_SIX:
            return "JOURNAL"
        return token
