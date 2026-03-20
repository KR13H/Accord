from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any


class CurrencyService:
    """Date-aware currency helpers for base INR conversion and revaluation math."""

    BASE_CURRENCY = "INR"
    EXCHANGE_RATES = {
        "USD": Decimal("83.1500"),
        "AED": Decimal("22.6400"),
        "GBP": Decimal("105.4200"),
        "EUR": Decimal("90.1200"),
        "INR": Decimal("1.0000"),
    }

    @staticmethod
    def convert_to_base(amount: Decimal, currency_code: str, rate: Decimal | None = None) -> Decimal:
        normalized = (currency_code or "INR").strip().upper() or "INR"
        conversion_rate = rate or CurrencyService.EXCHANGE_RATES.get(normalized, Decimal("1.0000"))
        return (amount * conversion_rate).quantize(Decimal("0.0001"))

    @staticmethod
    def calculate_unrealized_gain_loss(
        book_amount_base: Decimal,
        current_rate: Decimal,
        foreign_amount: Decimal,
    ) -> Decimal:
        current_value_base = (foreign_amount * current_rate).quantize(Decimal("0.0001"))
        return (current_value_base - book_amount_base).quantize(Decimal("0.0001"))

    @staticmethod
    def get_rate(currency_code: str, as_of: datetime | None = None) -> Decimal:
        _ = as_of  # Placeholder for future date-indexed rate sources.
        return CurrencyService.EXCHANGE_RATES.get((currency_code or "INR").strip().upper(), Decimal("1.0000"))

    @staticmethod
    def rates_payload() -> dict[str, Any]:
        return {
            "base_currency": CurrencyService.BASE_CURRENCY,
            "rates": {code: f"{rate:.4f}" for code, rate in CurrencyService.EXCHANGE_RATES.items() if code != "INR"},
            "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "source": "ACCORD_SOVEREIGN_FX_ENGINE",
        }
