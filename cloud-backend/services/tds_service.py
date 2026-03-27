from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Callable


MONEY_QUANT = Decimal("0.01")
TDS_THRESHOLD = Decimal("5000000.00")
TDS_RATE = Decimal("0.01")


def _money(value: Any) -> Decimal:
    return Decimal(str(value or "0")).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class TdsResult:
    booking_id: str
    total_consideration: Decimal
    tds_amount: Decimal
    journal_entry_id: int | None
    status: str


class TdsService:
    def __init__(self, *, get_conn: Callable[[], sqlite3.Connection]) -> None:
        self.get_conn = get_conn

    def ensure_schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tds_obligations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                booking_id TEXT NOT NULL UNIQUE,
                customer_name TEXT,
                total_consideration TEXT NOT NULL,
                tds_rate TEXT NOT NULL,
                tds_amount TEXT NOT NULL,
                due_month TEXT NOT NULL,
                due_date TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING' CHECK(status IN ('PENDING', 'REMITTED')),
                journal_entry_id INTEGER NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(booking_id) REFERENCES sales_bookings(booking_id) ON DELETE RESTRICT,
                FOREIGN KEY(journal_entry_id) REFERENCES journal_entries(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_tds_obligations_status_due ON tds_obligations(status, due_date);
            CREATE INDEX IF NOT EXISTS idx_tds_obligations_month ON tds_obligations(due_month, status);
            """
        )

    def _ensure_account(self, conn: sqlite3.Connection, *, name: str, account_type: str) -> int:
        row = conn.execute("SELECT id FROM accounts WHERE name = ?", (name,)).fetchone()
        if row is not None:
            return int(row["id"])
        cursor = conn.execute(
            "INSERT INTO accounts(name, type, balance) VALUES (?, ?, '0.0000')",
            (name, account_type),
        )
        return int(cursor.lastrowid)

    def _compute_due_date(self, booking_date: str | None) -> tuple[str, str]:
        try:
            base = date.fromisoformat(str(booking_date)) if booking_date else date.today()
        except Exception:
            base = date.today()
        if base.month == 12:
            due = date(base.year + 1, 1, 7)
        else:
            due = date(base.year, base.month + 1, 7)
        return due.strftime("%Y-%m"), due.isoformat()

    def process_booking_for_tds(self, booking_id: str) -> TdsResult | None:
        now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        conn: sqlite3.Connection | None = None
        try:
            conn = self.get_conn()
            conn.row_factory = sqlite3.Row
            self.ensure_schema(conn)
            booking = conn.execute(
                """
                SELECT booking_id, customer_name, total_consideration, booking_date
                FROM sales_bookings
                WHERE booking_id = ?
                """,
                (booking_id.strip(),),
            ).fetchone()
            if booking is None:
                return None

            total = _money(booking["total_consideration"])
            if total <= TDS_THRESHOLD:
                return None

            existing = conn.execute(
                "SELECT booking_id, total_consideration, tds_amount, journal_entry_id, status FROM tds_obligations WHERE booking_id = ?",
                (booking_id.strip(),),
            ).fetchone()
            if existing is not None:
                return TdsResult(
                    booking_id=str(existing["booking_id"]),
                    total_consideration=_money(existing["total_consideration"]),
                    tds_amount=_money(existing["tds_amount"]),
                    journal_entry_id=int(existing["journal_entry_id"]) if existing["journal_entry_id"] is not None else None,
                    status=str(existing["status"]),
                )

            tds_amount = (total * TDS_RATE).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
            due_month, due_date = self._compute_due_date(booking["booking_date"])

            conn.execute("BEGIN")

            ar_account_id = self._ensure_account(conn, name="Accounts Receivable", account_type="Asset")
            tds_payable_id = self._ensure_account(conn, name="TDS Payable", account_type="Liability")

            reference = f"TDS-{booking_id.strip()}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            entry_cursor = conn.execute(
                """
                INSERT INTO journal_entries(date, reference, description, created_at, voucher_type)
                VALUES (?, ?, ?, ?, 'JOURNAL')
                """,
                (
                    datetime.utcnow().date().isoformat(),
                    reference,
                    f"TDS 194-IA accrual for booking {booking_id.strip()}",
                    now_iso,
                ),
            )
            entry_id = int(entry_cursor.lastrowid)

            conn.execute(
                "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, ?, '0.0000')",
                (entry_id, ar_account_id, f"{tds_amount:.4f}"),
            )
            conn.execute(
                "INSERT INTO journal_lines(entry_id, account_id, debit, credit) VALUES (?, ?, '0.0000', ?)",
                (entry_id, tds_payable_id, f"{tds_amount:.4f}"),
            )

            conn.execute(
                """
                INSERT INTO tds_obligations(
                    booking_id, customer_name, total_consideration,
                    tds_rate, tds_amount, due_month, due_date,
                    status, journal_entry_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?, ?)
                """,
                (
                    booking_id.strip(),
                    str(booking["customer_name"] or ""),
                    f"{total:.2f}",
                    f"{TDS_RATE:.4f}",
                    f"{tds_amount:.2f}",
                    due_month,
                    due_date,
                    entry_id,
                    now_iso,
                    now_iso,
                ),
            )

            conn.commit()
            return TdsResult(
                booking_id=booking_id.strip(),
                total_consideration=total,
                tds_amount=tds_amount,
                journal_entry_id=entry_id,
                status="PENDING",
            )
        except Exception:
            if conn is not None:
                conn.rollback()
            raise
        finally:
            if conn is not None:
                conn.close()

    def pending_dashboard(self) -> dict[str, Any]:
        with closing(self.get_conn()) as conn:
            conn.row_factory = sqlite3.Row
            self.ensure_schema(conn)
            totals = conn.execute(
                """
                SELECT COUNT(1) AS pending_count, COALESCE(SUM(CAST(tds_amount AS REAL)), 0) AS pending_amount
                FROM tds_obligations
                WHERE status = 'PENDING'
                """
            ).fetchone()
            by_month_rows = conn.execute(
                """
                SELECT due_month, COUNT(1) AS item_count, COALESCE(SUM(CAST(tds_amount AS REAL)), 0) AS month_amount
                FROM tds_obligations
                WHERE status = 'PENDING'
                GROUP BY due_month
                ORDER BY due_month ASC
                """
            ).fetchall()
            rows = conn.execute(
                """
                SELECT booking_id, customer_name, total_consideration, tds_rate, tds_amount, due_date, status
                FROM tds_obligations
                WHERE status = 'PENDING'
                ORDER BY due_date ASC
                """
            ).fetchall()

        return {
            "pending_count": int(totals["pending_count"] if totals is not None else 0),
            "pending_amount": f"{Decimal(str(totals['pending_amount'] if totals is not None else 0)).quantize(MONEY_QUANT):.2f}",
            "by_due_month": [
                {
                    "due_month": str(row["due_month"]),
                    "item_count": int(row["item_count"]),
                    "month_amount": f"{Decimal(str(row['month_amount'])).quantize(MONEY_QUANT):.2f}",
                }
                for row in by_month_rows
            ],
            "items": [dict(row) for row in rows],
        }
