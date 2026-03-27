from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_HALF_UP


def calculate_late_fees(
    principal: Decimal,
    due_date: date,
    current_date: date,
    rate: Decimal,
    grace_days: int,
) -> Decimal:
    if principal <= 0:
        return Decimal("0.00")
    if grace_days < 0:
        raise ValueError("grace_days must be non-negative")
    if rate < 0:
        raise ValueError("rate must be non-negative")

    effective_due = due_date.toordinal() + grace_days
    days_late = current_date.toordinal() - effective_due
    if days_late <= 0:
        return Decimal("0.00")

    daily_rate = (rate / Decimal("365"))
    amount = Decimal(principal) * (Decimal("1") + daily_rate) ** Decimal(days_late)
    fee = amount - Decimal(principal)
    return fee.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
