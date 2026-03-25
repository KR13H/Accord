from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Callable


MONEY_QUANT = Decimal("0.01")


def _money(value: Any) -> Decimal:
    return Decimal(str(value or "0")).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)


class CommissionService:
    def __init__(self, *, get_conn: Callable[[], sqlite3.Connection]) -> None:
        self.get_conn = get_conn
        self.schema_path = Path(__file__).resolve().parents[1] / "sql" / "broker_schema.sql"

    def ensure_schema(self, conn: sqlite3.Connection) -> None:
        if self.schema_path.exists():
            conn.executescript(self.schema_path.read_text(encoding="utf-8"))
        cols = conn.execute("PRAGMA table_info(sales_bookings)").fetchall()
        if not any(str(c["name"]) == "broker_id" for c in cols):
            conn.execute("ALTER TABLE sales_bookings ADD COLUMN broker_id TEXT")
        conn.commit()

    def _ensure_account(self, conn: sqlite3.Connection, *, name: str, account_type: str) -> int:
        row = conn.execute("SELECT id FROM accounts WHERE name = ?", (name,)).fetchone()
        if row is not None:
            return int(row["id"])
        cursor = conn.execute(
            "INSERT INTO accounts(name, type, balance) VALUES (?, ?, '0.0000')",
            (name, account_type),
        )
        return int(cursor.lastrowid)

    def _has_posted_allocation(self, conn: sqlite3.Connection, booking_id: str) -> bool:
        row = conn.execute(
            "SELECT id FROM rera_allocation_events WHERE booking_id = ? AND status = 'POSTED' ORDER BY id ASC LIMIT 1",
            (booking_id,),
        ).fetchone()
        return row is not None

    def create_commission_for_booking(self, booking_id: str, broker_id: str, rate: Decimal = Decimal("0.02")) -> dict[str, Any] | None:
        now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        conn: sqlite3.Connection | None = None
        try:
            conn = self.get_conn()
            conn.row_factory = sqlite3.Row
            self.ensure_schema(conn)

            booking = conn.execute(
                """
                SELECT booking_id, customer_name, total_consideration
                FROM sales_bookings
                WHERE booking_id = ?
                """,
                (booking_id.strip(),),
            ).fetchone()
            if booking is None:
                return None

            existing = conn.execute(
                "SELECT id, status, amount FROM broker_commissions WHERE booking_id = ? AND broker_id = ?",
                (booking_id.strip(), broker_id.strip()),
            ).fetchone()
            if existing is not None:
                return {
                    "commission_id": int(existing["id"]),
                    "status": str(existing["status"]),
                    "amount": str(existing["amount"]),
                }

            total = _money(booking["total_consideration"])
            amount = (total * rate).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
            status = "READY_TO_PAY" if self._has_posted_allocation(conn, booking_id.strip()) else "PENDING_ALLOCATION"

            conn.execute("BEGIN")

            payable_account = self._ensure_account(conn, name="Accounts Payable", account_type="Liability")
            expense_account = self._ensure_account(conn, name="Operating Expenses", account_type="Expense")

            reference = f"COM-{booking_id.strip()}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            entry = conn.execute(
                """
                INSERT INTO journal_entries(date, reference, description, created_at, voucher_type)
                VALUES (?, ?, ?, ?, 'JOURNAL')
                """,
                (
                    datetime.utcnow().date().isoformat(),
                    reference,
                    f"Broker commission payable for booking {booking_id.strip()}",
                    now_iso,
                ),
            )
            entry_id = int(entry.lastrowid)

            conn.execute(
                "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, '0.0000')",
                (entry_id, expense_account, f"{amount:.4f}"),
            )
            conn.execute(
                "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, '0.0000', ?)",
                (entry_id, payable_account, f"{amount:.4f}"),
            )

            cursor = conn.execute(
                """
                INSERT INTO broker_commissions(
                    broker_id, booking_id, commission_rate,
                    amount, status, entry_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    broker_id.strip(),
                    booking_id.strip(),
                    f"{rate:.4f}",
                    f"{amount:.2f}",
                    status,
                    entry_id,
                    now_iso,
                    now_iso,
                ),
            )

            conn.commit()
            return {
                "commission_id": int(cursor.lastrowid),
                "booking_id": booking_id.strip(),
                "broker_id": broker_id.strip(),
                "amount": f"{amount:.2f}",
                "status": status,
                "entry_id": entry_id,
            }
        except Exception:
            if conn is not None:
                conn.rollback()
            raise
        finally:
            if conn is not None:
                conn.close()

    def release_commissions_for_booking(self, booking_id: str) -> int:
        now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        with closing(self.get_conn()) as conn:
            conn.row_factory = sqlite3.Row
            self.ensure_schema(conn)
            if not self._has_posted_allocation(conn, booking_id.strip()):
                return 0
            cursor = conn.execute(
                """
                UPDATE broker_commissions
                SET status = 'READY_TO_PAY', updated_at = ?
                WHERE booking_id = ? AND status = 'PENDING_ALLOCATION'
                """,
                (now_iso, booking_id.strip()),
            )
            conn.commit()
            return int(cursor.rowcount)
