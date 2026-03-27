from __future__ import annotations

import sqlite3
from datetime import date
from decimal import Decimal
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest
from services.sme_credit_service import create_customer, adjust_balance
from services.universal_accounting import record_transaction, get_daily_summary, get_transactions_between

@pytest.fixture()
def test_db():
    conn = sqlite3.connect(":memory:")
    yield conn
    conn.close()

@pytest.mark.parametrize("business_id, tx_type, amount, category, payment_method", [
    ("SME-001", "INCOME", Decimal("100.00"), "General", "Cash"),
    ("SME-002", "EXPENSE", Decimal("50.00"), "Travel", "UPI"),
])
def test_record_transaction(test_db, business_id, tx_type, amount, category, payment_method):
    conn = test_db
    transaction = record_transaction(lambda: conn, business_id=business_id, tx_type=tx_type, amount=amount, category=category, payment_method=payment_method)
    assert transaction["id"] > 0
    assert transaction["business_id"] == business_id
    assert transaction["type"] in ["INCOME", "EXPENSE"]
    assert float(transaction["amount"]) >= 0.00

@pytest.mark.parametrize("business_id, target_date", [
    ("SME-001", date.today()),
])
def test_get_daily_summary(test_db, business_id, target_date):
    conn = test_db
    summary = get_daily_summary(lambda: conn, business_id=business_id, target_date=target_date)
    assert "income_total" in summary
    assert "expense_total" in summary
    assert "net_total" in summary

@pytest.mark.parametrize("business_id, start_date, end_date", [
    ("SME-001", date(2022, 1, 1), date(2022, 1, 31)),
])
def test_get_transactions_between(test_db, business_id, start_date, end_date):
    conn = test_db
    transactions = get_transactions_between(lambda: conn, business_id=business_id, start_date=start_date, end_date=end_date)
    for transaction in transactions:
        assert "id" in transaction
        assert "business_id" in transaction
        assert "type" in transaction
        assert "amount" in transaction

@pytest.mark.parametrize("customer_id, amount, mode", [
    (1, Decimal("50.00"), "charge"),
    (2, Decimal("25.00"), "settle"),
])
def test_adjust_balance(test_db, customer_id, amount, mode):
    conn = test_db
    customer = create_customer(lambda: conn, business_id="SME-001", name="John Doe", phone=None)
    if mode == "settle":
        adjust_balance(lambda: conn, customer_id=customer["id"], amount=Decimal("100.00"), mode="charge")
    balance = adjust_balance(lambda: conn, customer_id=customer["id"], amount=amount, mode=mode)
    assert "id" in balance
    assert "business_id" in balance
    assert "name" in balance
    assert "outstanding_balance" in balance

@pytest.mark.parametrize("business_id", [
    None,
])
def test_record_transaction_missing_business_id(test_db, business_id):
    conn = test_db
    transaction = record_transaction(
        lambda: conn,
        business_id=business_id,
        tx_type="INCOME",
        amount=Decimal("100.00"),
        category="General",
        payment_method="Cash",
    )
    assert transaction["business_id"] == "SME-001"

@pytest.mark.parametrize("tx_type", [
    "Invalid",
])
def test_record_transaction_invalid_tx_type(test_db, tx_type):
    conn = test_db
    with pytest.raises(ValueError) as e:
        record_transaction(lambda: conn, business_id="SME-001", tx_type=tx_type, amount=Decimal("100.00"), category="General", payment_method="Cash")
    assert str(e.value) == "type must be INCOME or EXPENSE"

@pytest.mark.parametrize("amount", [
    Decimal("-1.00"),
])
def test_record_transaction_negative_amount(test_db, amount):
    conn = test_db
    with pytest.raises(ValueError) as e:
        record_transaction(lambda: conn, business_id="SME-001", tx_type="INCOME", amount=amount, category="General", payment_method="Cash")
    assert str(e.value) == "amount must be greater than 0"

@pytest.mark.parametrize("payment_method", [
    "Invalid",
])
def test_record_transaction_invalid_payment_method(test_db, payment_method):
    conn = test_db
    with pytest.raises(ValueError) as e:
        record_transaction(lambda: conn, business_id="SME-001", tx_type="INCOME", amount=Decimal("100.00"), category="General", payment_method=payment_method)
    assert str(e.value) == "payment_method must be Cash or UPI"

@pytest.mark.parametrize("customer_id, mode", [
    (1, "Invalid"),
])
def test_adjust_balance_invalid_mode(test_db, customer_id, mode):
    conn = test_db
    with pytest.raises(ValueError) as e:
        adjust_balance(lambda: conn, customer_id=customer_id, amount=Decimal("50.00"), mode=mode)
    assert str(e.value) == "mode must be charge or settle"

@pytest.mark.parametrize("amount", [
    Decimal("-1.00"),
])
def test_adjust_balance_negative_amount(test_db, amount):
    conn = test_db
    with pytest.raises(ValueError) as e:
        adjust_balance(lambda: conn, customer_id=1, amount=amount, mode="charge")
    assert str(e.value) == "amount must be greater than 0"
