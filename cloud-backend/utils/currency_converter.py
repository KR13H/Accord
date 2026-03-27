from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Callable


MONEY_QUANT = Decimal("0.01")
RATE_QUANT = Decimal("0.0001")


def _money(value: Any) -> Decimal:
    return Decimal(str(value or "0")).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)


def _rate(value: Any) -> Decimal:
    return Decimal(str(value or "0")).quantize(RATE_QUANT, rounding=ROUND_HALF_UP)


class CurrencyConverter:
    def __init__(self, *, get_conn: Callable[[], sqlite3.Connection]) -> None:
        self.get_conn = get_conn

    def ensure_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS exchange_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                currency_code TEXT NOT NULL,
                as_of_date TEXT NOT NULL,
                inr_rate TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'MANUAL',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(currency_code, as_of_date)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_exchange_rates_lookup
            ON exchange_rates(currency_code, as_of_date DESC)
            """
        )
        conn.commit()

    def _seed_defaults_if_missing(self, conn: sqlite3.Connection, as_of_date: str) -> None:
        defaults = {
            "USD": "83.4500",
            "AED": "22.7200",
            "GBP": "106.1800",
        }
        for code, value in defaults.items():
            conn.execute(
                """
                INSERT OR IGNORE INTO exchange_rates(currency_code, as_of_date, inr_rate, source)
                VALUES (?, ?, ?, 'SEED_DEFAULT')
                """,
                (code, as_of_date, value),
            )

    def get_rate(self, *, currency_code: str, as_of_date: str | None = None) -> Decimal:
        code = (currency_code or "INR").strip().upper() or "INR"
        if code == "INR":
            return Decimal("1.0000")

        lookup_date = as_of_date or date.today().isoformat()
        with closing(self.get_conn()) as conn:
            conn.row_factory = sqlite3.Row
            self.ensure_schema(conn)
            self._seed_defaults_if_missing(conn, lookup_date)

            row = conn.execute(
                """
                SELECT inr_rate
                FROM exchange_rates
                WHERE currency_code = ? AND as_of_date <= ?
                ORDER BY as_of_date DESC
                LIMIT 1
                """,
                (code, lookup_date),
            ).fetchone()

        if row is None:
            raise ValueError(f"No exchange rate available for currency_code={code}")
        return _rate(row["inr_rate"])

    def convert_to_inr(
        self,
        *,
        currency_code: str,
        foreign_amount: Any,
        as_of_date: str | None = None,
    ) -> dict[str, Any]:
        code = (currency_code or "INR").strip().upper() or "INR"
        amount_foreign = _money(foreign_amount)
        if amount_foreign < 0:
            raise ValueError("foreign_amount must be non-negative")

        rate = self.get_rate(currency_code=code, as_of_date=as_of_date)
        inr_amount = (amount_foreign * rate).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)

        return {
            "inr_amount": inr_amount,
            "foreign_meta": {
                "currency_code": code,
                "foreign_amount": f"{amount_foreign:.2f}",
                "inr_rate": f"{rate:.4f}",
                "as_of_date": as_of_date or date.today().isoformat(),
            },
        }
