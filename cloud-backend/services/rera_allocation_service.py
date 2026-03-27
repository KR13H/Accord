from __future__ import annotations

import logging
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Callable


MONEY_QUANT = Decimal("0.01")
DEFAULT_RERA_RATIO = Decimal("0.70")
HIGH_VALUE_ALERT_THRESHOLD = Decimal("1000000.00")
push_logger = logging.getLogger("accord.push")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_money(value: Any) -> Decimal:
    return Decimal(str(value or "0")).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)


def _to_ratio(value: Any) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class AllocationInput:
    booking_id: str
    payment_reference: str
    receipt_amount: Decimal
    event_type: str = "PAYMENT"
    override_rera_ratio: Decimal | None = None
    override_reason: str | None = None
    actor_role: str = "SYSTEM"


@dataclass(frozen=True)
class AllocationResult:
    event_id: int
    applied_ratio: Decimal
    rera_amount: Decimal
    operations_amount: Decimal
    is_override: bool


class ReraAllocationService:
    """Applies RERA-compliant 70/30 split for buyer collections with ACID safety."""

    def __init__(
        self,
        *,
        get_conn: Callable[[], sqlite3.Connection],
        now_iso: Callable[[], str] = utc_now_iso,
        high_value_alert_hook: Callable[[dict[str, Any]], None] | None = None,
        high_value_alert_threshold: Decimal = HIGH_VALUE_ALERT_THRESHOLD,
    ) -> None:
        self.get_conn = get_conn
        self.now_iso = now_iso
        self.high_value_alert_hook = high_value_alert_hook
        self.high_value_alert_threshold = Decimal(str(high_value_alert_threshold)).quantize(MONEY_QUANT)

    def _emit_high_value_alert(self, payload: dict[str, Any]) -> None:
        if self.high_value_alert_hook is None:
            return
        try:
            self.high_value_alert_hook(payload)
        except Exception as exc:  # noqa: BLE001
            # Notification failures should never break booking allocation posting.
            push_logger.warning("high-value allocation alert hook failed: %s", exc)

    def ensure_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sales_bookings (
                booking_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                customer_name TEXT,
                unit_code TEXT,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rera_allocation_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                booking_id TEXT NOT NULL,
                payment_reference TEXT NOT NULL,
                event_type TEXT NOT NULL CHECK(event_type IN ('PAYMENT', 'REFUND')),
                receipt_amount TEXT NOT NULL,
                applied_rera_ratio TEXT NOT NULL,
                rera_amount TEXT NOT NULL,
                operations_amount TEXT NOT NULL,
                is_override INTEGER NOT NULL DEFAULT 0 CHECK(is_override IN (0, 1)),
                override_reason TEXT,
                actor_role TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'POSTED' CHECK(status IN ('POSTED', 'FAILED')),
                created_at TEXT NOT NULL,
                FOREIGN KEY(booking_id) REFERENCES sales_bookings(booking_id) ON DELETE RESTRICT,
                UNIQUE(booking_id, payment_reference, event_type)
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rera_allocation_vouchers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                allocation_event_id INTEGER NOT NULL,
                voucher_kind TEXT NOT NULL CHECK(voucher_kind IN ('RERA_TRANSFER', 'OPERATIONS_TRANSFER')),
                from_account TEXT NOT NULL,
                to_account TEXT NOT NULL,
                amount TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(allocation_event_id) REFERENCES rera_allocation_events(id) ON DELETE CASCADE
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rera_allocation_idempotency (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                idempotency_key TEXT NOT NULL UNIQUE,
                request_hash TEXT NOT NULL,
                allocation_event_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(allocation_event_id) REFERENCES rera_allocation_events(id) ON DELETE SET NULL
            )
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_rera_allocation_events_booking
            ON rera_allocation_events(booking_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_rera_allocation_vouchers_event
            ON rera_allocation_vouchers(allocation_event_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_rera_allocation_idempotency_event
            ON rera_allocation_idempotency(allocation_event_id)
            """
        )
        conn.commit()

    def _compute_split(self, receipt_amount: Decimal, ratio: Decimal) -> tuple[Decimal, Decimal]:
        rera_amount = (receipt_amount * ratio).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
        operations_amount = (receipt_amount - rera_amount).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
        return rera_amount, operations_amount

    def allocate(
        self,
        payload: AllocationInput,
        *,
        fail_after_first_voucher: bool = False,
    ) -> AllocationResult:
        receipt_amount = _to_money(payload.receipt_amount)
        if receipt_amount <= 0:
            raise ValueError("receipt_amount must be greater than 0")

        event_type = payload.event_type.strip().upper()
        if event_type not in {"PAYMENT", "REFUND"}:
            raise ValueError("event_type must be PAYMENT or REFUND")

        applied_ratio = DEFAULT_RERA_RATIO
        is_override = False
        if payload.override_rera_ratio is not None:
            applied_ratio = _to_ratio(payload.override_rera_ratio)
            is_override = applied_ratio != DEFAULT_RERA_RATIO
            if is_override and not (payload.override_reason or "").strip():
                raise ValueError("override_reason is required when override_rera_ratio differs from 0.70")

        if applied_ratio <= 0 or applied_ratio >= 1:
            raise ValueError("applied ratio must be > 0 and < 1")

        rera_amount, operations_amount = self._compute_split(receipt_amount, applied_ratio)
        signed_multiplier = Decimal("1") if event_type == "PAYMENT" else Decimal("-1")
        now = self.now_iso()

        with closing(self.get_conn()) as conn:
            conn.row_factory = sqlite3.Row
            self.ensure_schema(conn)
            try:
                conn.execute("BEGIN")

                event_cursor = conn.execute(
                    """
                    INSERT INTO rera_allocation_events(
                        booking_id, payment_reference, event_type,
                        receipt_amount, applied_rera_ratio, rera_amount, operations_amount,
                        is_override, override_reason, actor_role, status, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'POSTED', ?)
                    """,
                    (
                        payload.booking_id,
                        payload.payment_reference,
                        event_type,
                        f"{receipt_amount:.2f}",
                        f"{applied_ratio:.4f}",
                        f"{rera_amount:.2f}",
                        f"{operations_amount:.2f}",
                        1 if is_override else 0,
                        (payload.override_reason or "").strip() or None,
                        payload.actor_role,
                        now,
                    ),
                )
                event_id = int(event_cursor.lastrowid)

                conn.execute(
                    """
                    INSERT INTO rera_allocation_vouchers(
                        allocation_event_id, voucher_kind, from_account, to_account, amount, created_at
                    ) VALUES (?, 'RERA_TRANSFER', 'Collections Clearing', 'RERA Designated Account', ?, ?)
                    """,
                    (event_id, f"{(rera_amount * signed_multiplier):.2f}", now),
                )

                if fail_after_first_voucher:
                    raise RuntimeError("simulated voucher failure")

                conn.execute(
                    """
                    INSERT INTO rera_allocation_vouchers(
                        allocation_event_id, voucher_kind, from_account, to_account, amount, created_at
                    ) VALUES (?, 'OPERATIONS_TRANSFER', 'Collections Clearing', 'Operating Bank Account', ?, ?)
                    """,
                    (event_id, f"{(operations_amount * signed_multiplier):.2f}", now),
                )

                conn.commit()

                if event_type == "PAYMENT" and receipt_amount >= self.high_value_alert_threshold:
                    self._emit_high_value_alert(
                        {
                            "event_id": event_id,
                            "booking_id": payload.booking_id,
                            "payment_reference": payload.payment_reference,
                            "event_type": event_type,
                            "receipt_amount": f"{receipt_amount:.2f}",
                            "rera_amount": f"{rera_amount:.2f}",
                            "operations_amount": f"{operations_amount:.2f}",
                            "created_at": now,
                        }
                    )

                return AllocationResult(
                    event_id=event_id,
                    applied_ratio=applied_ratio,
                    rera_amount=rera_amount,
                    operations_amount=operations_amount,
                    is_override=is_override,
                )
            except Exception:
                conn.rollback()
                raise
