from __future__ import annotations

import math
from pathlib import Path
import sys

from fastapi.testclient import TestClient
from hypothesis import given, settings, strategies as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main


main.init_db()
client = TestClient(main.app)


def _ensure_customer_id() -> int:
    response = client.post(
        "/api/v1/sme/customers",
        json={"business_id": "SME-001", "name": "Fuzz Customer", "phone": "9999999999"},
    )
    assert response.status_code == 201
    return int(response.json()["customer"]["id"])


CUSTOMER_ID = _ensure_customer_id()


amount_strategy = st.one_of(
    st.floats(allow_nan=False, allow_infinity=False, width=64),
    st.integers(min_value=-(10**20), max_value=10**20),
    st.sampled_from(["NaN", "Infinity", "-Infinity"]),
    st.text(min_size=0, max_size=32),
)


@given(amount=amount_strategy)
@settings(max_examples=250, deadline=None)
def test_fuzz_udhaar_charge_no_500(amount):
    payload = {"amount": amount}
    response = client.post(f"/api/v1/sme/customers/{CUSTOMER_ID}/charge", json=payload)
    assert response.status_code != 500

    if isinstance(amount, (int, float)) and not isinstance(amount, bool) and math.isfinite(float(amount)) and float(amount) > 0:
        assert response.status_code in {200, 422}
    else:
        assert response.status_code == 422


name_strategy = st.one_of(
    st.just(""),
    st.text(min_size=1, max_size=200),
    st.just("🧪📦"),
    st.just("X" * 10000),
)


@given(item_name=name_strategy)
@settings(max_examples=200, deadline=None)
def test_fuzz_inventory_create_no_500(item_name):
    response = client.post(
        "/api/v1/sme/inventory/items",
        json={
            "business_id": "SME-001",
            "item_name": item_name,
            "sku": "FUZZ-SKU",
            "current_stock": 1,
            "minimum_stock_level": 0,
            "unit_price": 1,
        },
    )
    assert response.status_code != 500

    if isinstance(item_name, str) and 1 <= len(item_name) <= 120:
        assert response.status_code in {201, 422}
    else:
        assert response.status_code == 422
