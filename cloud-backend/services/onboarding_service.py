from __future__ import annotations

import random
import sqlite3
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Callable

from services.sme_credit_service import ensure_customer_schema
from services.sme_inventory_service import ensure_inventory_schema
from services.universal_accounting import ensure_sme_schema


DEMO_ITEMS = [
    {"item_name": "Asian Paints 1L", "factory_serial": "AP-1L-001", "unit_price": Decimal("399.00")},
    {"item_name": "Masking Tape", "factory_serial": "MT-24MM-009", "unit_price": Decimal("75.00")},
    {"item_name": "PVC Pipe 3m", "factory_serial": "PVC-3M-112", "unit_price": Decimal("540.00")},
    {"item_name": "Wall Putty 20kg", "factory_serial": "WP-20KG-510", "unit_price": Decimal("850.00")},
    {"item_name": "Cement Bag 50kg", "factory_serial": "CB-50KG-777", "unit_price": Decimal("420.00")},
]

DEMO_CUSTOMERS = [
    {"name": "Raj Electricals", "phone": "+919800000321"},
    {"name": "Sharma Contractors", "phone": "+919800000654"},
]


def inject_demo_data(get_conn: Callable[[], sqlite3.Connection], business_id: str) -> dict[str, int | str]:
    clean_business_id = (business_id or "").strip()
    if not clean_business_id:
        raise ValueError("business_id is required")

    with get_conn() as conn:
        conn.row_factory = sqlite3.Row
        ensure_inventory_schema(conn)
        ensure_customer_schema(conn)
        ensure_sme_schema(conn)

        existing = conn.execute(
            "SELECT COUNT(*) AS row_count FROM sme_transactions WHERE business_id = ?",
            (clean_business_id,),
        ).fetchone()
        if int(existing["row_count"] if existing else 0) > 0:
            return {"status": "skipped", "inventory_items": 0, "customers": 0, "transactions": 0}

        now = datetime.utcnow()
        randomizer = random.Random(f"accord-demo-{clean_business_id}")

        for item in DEMO_ITEMS:
            stock = Decimal(str(randomizer.randint(15, 80)))
            min_stock = Decimal(str(randomizer.randint(6, 20)))
            conn.execute(
                """
                INSERT INTO sme_inventory_items (
                    business_id, item_name, localized_name, factory_serial, system_serial,
                    is_system_generated, current_stock, minimum_stock_level, unit_price
                ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)
                """,
                (
                    clean_business_id,
                    item["item_name"],
                    item["item_name"],
                    item["factory_serial"],
                    item["factory_serial"],
                    f"{stock:.4f}",
                    f"{min_stock:.4f}",
                    f"{item['unit_price']:.4f}",
                ),
            )

        for customer in DEMO_CUSTOMERS:
            conn.execute(
                """
                INSERT INTO sme_customers (business_id, name, phone, outstanding_balance)
                VALUES (?, ?, ?, ?)
                """,
                (
                    clean_business_id,
                    customer["name"],
                    customer["phone"],
                    f"{Decimal(str(randomizer.randint(1000, 8500))):.2f}",
                ),
            )

        for day_offset in range(30):
            tx_day = now - timedelta(days=29 - day_offset)
            txn_count = randomizer.randint(2, 5)
            for _ in range(txn_count):
                is_income = randomizer.random() < 0.78
                tx_type = "INCOME" if is_income else "EXPENSE"
                category = randomizer.choice(["Paint", "Hardware", "UPI Payment", "Cash Sale", "Logistics", "Utilities"])
                payment_method = randomizer.choice(["Cash", "UPI"])
                amount = Decimal(str(randomizer.randint(800, 9500) if is_income else randomizer.randint(250, 2800)))
                tx_time = tx_day.replace(
                    hour=randomizer.randint(8, 21),
                    minute=randomizer.randint(0, 59),
                    second=randomizer.randint(0, 59),
                    microsecond=0,
                )

                conn.execute(
                    """
                    INSERT INTO sme_transactions (business_id, type, amount, category, payment_method, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        clean_business_id,
                        tx_type,
                        f"{amount:.4f}",
                        category,
                        payment_method,
                        tx_time.isoformat(timespec="seconds") + "Z",
                    ),
                )

        conn.commit()

    return {
        "status": "ok",
        "inventory_items": len(DEMO_ITEMS),
        "customers": len(DEMO_CUSTOMERS),
        "transactions": 30,
    }
