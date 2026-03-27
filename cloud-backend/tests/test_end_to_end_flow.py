from __future__ import annotations

from datetime import date
import uuid
import unittest

from fastapi.testclient import TestClient

import main
from services.wip_service import WIPCapitalization


class EndToEndFlowTest(unittest.TestCase):
    def test_booking_to_idempotent_allocation_and_wip_preview(self) -> None:
        main.init_db()
        client = TestClient(main.app)
        headers = {"X-Role": "admin", "X-Admin-Id": "1"}

        booking_id = f"BK-E2E-{uuid.uuid4().hex[:8]}"
        payment_reference = f"PAY-E2E-{uuid.uuid4().hex[:8]}"
        idempotency_key = f"idem-e2e-{uuid.uuid4().hex[:16]}"
        project_id = f"PRJ-E2E-{uuid.uuid4().hex[:6]}"
        unit_code = f"T1-{uuid.uuid4().hex[:4]}"

        create_booking_resp = client.post(
            "/api/v1/bookings",
            headers=headers,
            json={
                "booking_id": booking_id,
                "project_id": project_id,
                "spv_id": "SPV-E2E",
                "customer_name": "End To End Buyer",
                "unit_code": unit_code,
                "total_consideration": "12500000.00",
                "booking_date": date.today().isoformat(),
                "status": "ACTIVE",
            },
        )
        self.assertIn(create_booking_resp.status_code, {200, 201}, create_booking_resp.text)

        alloc_headers = {
            **headers,
            "X-Idempotency-Key": idempotency_key,
        }

        allocation_payload = {
            "booking_id": booking_id,
            "payment_reference": payment_reference,
            "event_type": "PAYMENT",
            "receipt_amount": "100000.00",
        }

        first_alloc_resp = client.post("/api/v1/rera/allocations", headers=alloc_headers, json=allocation_payload)
        self.assertIn(first_alloc_resp.status_code, {200, 201}, first_alloc_resp.text)
        first_data = first_alloc_resp.json()

        second_alloc_resp = client.post("/api/v1/rera/allocations", headers=alloc_headers, json=allocation_payload)
        self.assertIn(second_alloc_resp.status_code, {200, 201, 409}, second_alloc_resp.text)
        if second_alloc_resp.status_code in {200, 201}:
            self.assertEqual(first_data["event_id"], second_alloc_resp.json()["event_id"])

        wip = WIPCapitalization(get_conn=main.get_conn)
        wip_result = wip.capitalize_monthly_expenses(project_id=project_id, month=date.today().strftime("%Y-%m"))
        self.assertEqual(wip_result["status"], "prepared")

        with main.get_conn() as conn:
            row = conn.execute(
                """
                SELECT COUNT(1) AS c
                FROM rera_allocation_events
                WHERE booking_id = ? AND payment_reference = ?
                """,
                (booking_id, payment_reference),
            ).fetchone()
            self.assertEqual(int(row["c"]), 1)


if __name__ == "__main__":
    unittest.main()
