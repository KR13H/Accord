from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from decimal import Decimal

from services.rera_allocation_service import AllocationInput, ReraAllocationService


class ReraAllocationServiceTest(unittest.TestCase):
    def setUp(self) -> None:
        fd, self.db_path = tempfile.mkstemp(prefix="rera_alloc_", suffix=".db")
        os.close(fd)

        def _get_conn() -> sqlite3.Connection:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            return conn

        self.get_conn = _get_conn
        self.service = ReraAllocationService(get_conn=self.get_conn)

        conn = self.get_conn()
        try:
            self.service.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO sales_bookings(booking_id, project_id, customer_name, unit_code, status, created_at, updated_at)
                VALUES ('BK-001', 'PRJ-ALPHA', 'Aarav Sharma', 'A-1203', 'ACTIVE', '2026-03-20T00:00:00Z', '2026-03-20T00:00:00Z')
                """
            )
            conn.commit()
        finally:
            conn.close()

    def tearDown(self) -> None:
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def _count_rows(self, table: str) -> int:
        conn = self.get_conn()
        try:
            row = conn.execute(f"SELECT COUNT(1) AS c FROM {table}").fetchone()
            return int(row["c"])
        finally:
            conn.close()

    def test_default_70_30_split_payment(self) -> None:
        result = self.service.allocate(
            AllocationInput(
                booking_id="BK-001",
                payment_reference="PAY-1001",
                receipt_amount=Decimal("100000.00"),
                event_type="PAYMENT",
                actor_role="FINANCE_MANAGER",
            )
        )

        self.assertEqual(result.rera_amount, Decimal("70000.00"))
        self.assertEqual(result.operations_amount, Decimal("30000.00"))
        self.assertFalse(result.is_override)
        self.assertEqual(self._count_rows("rera_allocation_events"), 1)
        self.assertEqual(self._count_rows("rera_allocation_vouchers"), 2)

    def test_partial_payment_rounding_stays_balanced(self) -> None:
        result = self.service.allocate(
            AllocationInput(
                booking_id="BK-001",
                payment_reference="PAY-1002",
                receipt_amount=Decimal("55555.55"),
                event_type="PAYMENT",
            )
        )

        self.assertEqual(result.rera_amount, Decimal("38888.89"))
        self.assertEqual(result.operations_amount, Decimal("16666.66"))
        self.assertEqual(result.rera_amount + result.operations_amount, Decimal("55555.55"))

    def test_refund_generates_negative_voucher_movements(self) -> None:
        self.service.allocate(
            AllocationInput(
                booking_id="BK-001",
                payment_reference="RF-1001",
                receipt_amount=Decimal("1000.00"),
                event_type="REFUND",
            )
        )

        conn = self.get_conn()
        try:
            rows = conn.execute(
                "SELECT voucher_kind, amount FROM rera_allocation_vouchers ORDER BY id ASC"
            ).fetchall()
        finally:
            conn.close()

        self.assertEqual(rows[0]["voucher_kind"], "RERA_TRANSFER")
        self.assertEqual(rows[0]["amount"], "-700.00")
        self.assertEqual(rows[1]["voucher_kind"], "OPERATIONS_TRANSFER")
        self.assertEqual(rows[1]["amount"], "-300.00")

    def test_override_requires_reason(self) -> None:
        with self.assertRaises(ValueError):
            self.service.allocate(
                AllocationInput(
                    booking_id="BK-001",
                    payment_reference="PAY-1003",
                    receipt_amount=Decimal("1000.00"),
                    override_rera_ratio=Decimal("0.65"),
                )
            )

    def test_manual_override_persists_audit_flag(self) -> None:
        result = self.service.allocate(
            AllocationInput(
                booking_id="BK-001",
                payment_reference="PAY-1004",
                receipt_amount=Decimal("1000.00"),
                override_rera_ratio=Decimal("0.65"),
                override_reason="Court-approved temporary liquidity adjustment",
                actor_role="CFO",
            )
        )

        self.assertTrue(result.is_override)
        self.assertEqual(result.rera_amount, Decimal("650.00"))
        self.assertEqual(result.operations_amount, Decimal("350.00"))

        conn = self.get_conn()
        try:
            row = conn.execute(
                "SELECT is_override, override_reason, actor_role FROM rera_allocation_events WHERE id = ?",
                (result.event_id,),
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual(row["is_override"], 1)
        self.assertIn("liquidity", row["override_reason"].lower())
        self.assertEqual(row["actor_role"], "CFO")

    def test_rolls_back_when_split_fails_mid_transaction(self) -> None:
        with self.assertRaises(RuntimeError):
            self.service.allocate(
                AllocationInput(
                    booking_id="BK-001",
                    payment_reference="PAY-ROLLBACK",
                    receipt_amount=Decimal("1000.00"),
                ),
                fail_after_first_voucher=True,
            )

        self.assertEqual(self._count_rows("rera_allocation_events"), 0)
        self.assertEqual(self._count_rows("rera_allocation_vouchers"), 0)


if __name__ == "__main__":
    unittest.main()
