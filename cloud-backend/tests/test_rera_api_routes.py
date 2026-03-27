from __future__ import annotations

import uuid
from datetime import datetime
import unittest

from fastapi.testclient import TestClient

import main


class ReraApiRoutesTest(unittest.TestCase):
    def _seed_booking(self, booking_id: str) -> None:
        with main.get_conn() as conn:
            conn.execute(
                """
                INSERT INTO sales_bookings(booking_id, project_id, customer_name, unit_code, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    booking_id,
                    "PRJ-API",
                    "API Smoke Customer",
                    "A-101",
                    "ACTIVE",
                    datetime.utcnow().isoformat(timespec="seconds") + "Z",
                    datetime.utcnow().isoformat(timespec="seconds") + "Z",
                ),
            )
            conn.commit()

    def test_post_and_get_rera_allocations(self) -> None:
        main.init_db()
        booking_id = f"BK-API-{uuid.uuid4().hex[:10]}"
        payment_reference = f"PAY-API-{uuid.uuid4().hex[:10]}"

        self._seed_booking(booking_id)

        client = TestClient(main.app)
        headers = {"X-Role": "admin", "X-Admin-Id": "1"}

        post_resp = client.post(
            "/api/v1/rera/allocations",
            headers=headers,
            json={
                "booking_id": booking_id,
                "payment_reference": payment_reference,
                "event_type": "PAYMENT",
                "receipt_amount": "100000.00",
            },
        )
        self.assertEqual(post_resp.status_code, 200, post_resp.text)
        post_data = post_resp.json()
        self.assertEqual(post_data["status"], "ok")
        self.assertEqual(post_data["booking_id"], booking_id)
        self.assertEqual(post_data["allocation"]["rera_amount"], "70000.00")
        self.assertEqual(post_data["allocation"]["operations_amount"], "30000.00")

        get_resp = client.get(f"/api/v1/rera/allocations?booking_id={booking_id}&limit=10", headers=headers)
        self.assertEqual(get_resp.status_code, 200, get_resp.text)
        get_data = get_resp.json()
        self.assertEqual(get_data["status"], "ok")
        self.assertGreaterEqual(get_data["count"], 1)
        self.assertTrue(any(item["payment_reference"] == payment_reference for item in get_data["items"]))

    def test_rera_booking_crud(self) -> None:
        main.init_db()
        booking_id = f"BK-CRUD-{uuid.uuid4().hex[:8]}"
        headers = {"X-Role": "admin", "X-Admin-Id": "1"}
        client = TestClient(main.app)

        create_resp = client.post(
            "/api/v1/rera/bookings",
            headers=headers,
            json={
                "booking_id": booking_id,
                "project_id": "PRJ-CRUD",
                "customer_name": "Crud Customer",
                "unit_code": "B-202",
            },
        )
        self.assertEqual(create_resp.status_code, 200, create_resp.text)
        self.assertEqual(create_resp.json()["booking"]["booking_id"], booking_id)

        get_resp = client.get(f"/api/v1/rera/bookings/{booking_id}", headers=headers)
        self.assertEqual(get_resp.status_code, 200, get_resp.text)
        self.assertEqual(get_resp.json()["booking"]["project_id"], "PRJ-CRUD")

        list_resp = client.get("/api/v1/rera/bookings?status=ACTIVE&limit=20", headers=headers)
        self.assertEqual(list_resp.status_code, 200, list_resp.text)
        self.assertTrue(any(item["booking_id"] == booking_id for item in list_resp.json()["items"]))

        update_resp = client.put(
            f"/api/v1/rera/bookings/{booking_id}",
            headers=headers,
            json={"status": "CLOSED", "customer_name": "Crud Customer Updated"},
        )
        self.assertEqual(update_resp.status_code, 200, update_resp.text)
        self.assertEqual(update_resp.json()["booking"]["status"], "CLOSED")

        delete_resp = client.delete(f"/api/v1/rera/bookings/{booking_id}", headers=headers)
        self.assertEqual(delete_resp.status_code, 200, delete_resp.text)
        self.assertTrue(delete_resp.json()["deleted"])

        after_delete_resp = client.get(f"/api/v1/rera/bookings/{booking_id}", headers=headers)
        self.assertEqual(after_delete_resp.status_code, 404, after_delete_resp.text)

    def test_rera_allocation_idempotency_replay_and_conflict(self) -> None:
        main.init_db()
        booking_id = f"BK-IDEM-{uuid.uuid4().hex[:8]}"
        payment_reference = f"PAY-IDEM-{uuid.uuid4().hex[:8]}"
        self._seed_booking(booking_id)

        client = TestClient(main.app)
        headers = {
            "X-Role": "admin",
            "X-Admin-Id": "1",
            "X-Idempotency-Key": f"idem-{uuid.uuid4().hex[:18]}",
        }

        payload = {
            "booking_id": booking_id,
            "payment_reference": payment_reference,
            "event_type": "PAYMENT",
            "receipt_amount": "45000.00",
        }

        first_resp = client.post("/api/v1/rera/allocations", headers=headers, json=payload)
        self.assertEqual(first_resp.status_code, 200, first_resp.text)
        first_data = first_resp.json()
        self.assertEqual(first_data["idempotency"]["replayed"], False)

        second_resp = client.post("/api/v1/rera/allocations", headers=headers, json=payload)
        self.assertEqual(second_resp.status_code, 200, second_resp.text)
        second_data = second_resp.json()
        self.assertEqual(first_data["event_id"], second_data["event_id"])
        self.assertEqual(second_data["idempotency"]["replayed"], True)

        conflict_payload = {
            "booking_id": booking_id,
            "payment_reference": payment_reference,
            "event_type": "PAYMENT",
            "receipt_amount": "55000.00",
        }
        conflict_resp = client.post("/api/v1/rera/allocations", headers=headers, json=conflict_payload)
        self.assertEqual(conflict_resp.status_code, 409, conflict_resp.text)


if __name__ == "__main__":
    unittest.main()
