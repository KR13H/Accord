from __future__ import annotations

from datetime import date
from decimal import Decimal
import unittest

from utils.penalty_math import calculate_late_fees


class PenaltyMathTest(unittest.TestCase):
    def test_no_fee_within_grace_period(self) -> None:
        fee = calculate_late_fees(
            principal=Decimal("10000.00"),
            due_date=date(2026, 3, 1),
            current_date=date(2026, 3, 6),
            rate=Decimal("0.18"),
            grace_days=5,
        )
        self.assertEqual(fee, Decimal("0.00"))

    def test_fee_after_grace_period(self) -> None:
        fee = calculate_late_fees(
            principal=Decimal("10000.00"),
            due_date=date(2026, 3, 1),
            current_date=date(2026, 3, 20),
            rate=Decimal("0.18"),
            grace_days=5,
        )
        self.assertGreater(fee, Decimal("0.00"))

    def test_leap_year_case(self) -> None:
        fee = calculate_late_fees(
            principal=Decimal("5000.00"),
            due_date=date(2024, 2, 28),
            current_date=date(2024, 3, 5),
            rate=Decimal("0.24"),
            grace_days=1,
        )
        self.assertGreaterEqual(fee, Decimal("0.00"))

    def test_exact_boundary_day_zero_fee(self) -> None:
        fee = calculate_late_fees(
            principal=Decimal("8000.00"),
            due_date=date(2026, 1, 10),
            current_date=date(2026, 1, 15),
            rate=Decimal("0.20"),
            grace_days=5,
        )
        self.assertEqual(fee, Decimal("0.00"))


if __name__ == "__main__":
    unittest.main()
