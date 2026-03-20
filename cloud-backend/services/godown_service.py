from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from typing import Any, Callable


@dataclass
class StockPulse:
    sku_code: str
    godown_code: str
    available_qty: Decimal
    avg_daily_outflow: Decimal
    days_to_stockout: Decimal


class GodownService:
    """Multi-location stock service with Decimal(12,4) precision and transfer support."""

    def __init__(
        self,
        *,
        get_conn: Callable[[], Any],
        money: Callable[[Any], Decimal],
        money_str: Callable[[Any], str],
    ) -> None:
        self.get_conn = get_conn
        self.money = money
        self.money_str = money_str

    def ensure_schema(self, conn: Any) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS godowns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                godown_code TEXT NOT NULL UNIQUE,
                godown_name TEXT NOT NULL,
                state_code TEXT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS godown_stock_ledger (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku_code TEXT NOT NULL,
                godown_code TEXT NOT NULL,
                movement_type TEXT NOT NULL CHECK(movement_type IN ('IN', 'OUT', 'TRANSFER_IN', 'TRANSFER_OUT')),
                quantity TEXT NOT NULL,
                unit_cost TEXT NOT NULL,
                reference_no TEXT NOT NULL,
                voucher_type TEXT NOT NULL,
                ewaybill_draft_json TEXT NULL,
                created_by INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_godown_stock_ledger_sku_godown
            ON godown_stock_ledger(sku_code, godown_code, created_at);
            """
        )

    def upsert_godown(self, *, godown_code: str, godown_name: str, state_code: str | None) -> dict[str, Any]:
        code = godown_code.strip().upper()
        name = godown_name.strip()
        if not code or not name:
            raise ValueError("godown_code and godown_name are required")

        now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        with self.get_conn() as conn:
            self.ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO godowns(godown_code, godown_name, state_code, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(godown_code) DO UPDATE SET
                    godown_name = excluded.godown_name,
                    state_code = excluded.state_code,
                    updated_at = excluded.updated_at
                """,
                (code, name, (state_code or "").strip().upper() or None, now_iso, now_iso),
            )
            conn.commit()
        return {"status": "ok", "godown_code": code, "godown_name": name, "updated_at": now_iso}

    def current_stock(self, *, sku_code: str, godown_code: str) -> Decimal:
        with self.get_conn() as conn:
            self.ensure_schema(conn)
            row = conn.execute(
                """
                SELECT COALESCE(SUM(CASE WHEN movement_type IN ('IN', 'TRANSFER_IN') THEN CAST(quantity AS REAL) ELSE 0 END), 0)
                     - COALESCE(SUM(CASE WHEN movement_type IN ('OUT', 'TRANSFER_OUT') THEN CAST(quantity AS REAL) ELSE 0 END), 0)
                       AS qty
                FROM godown_stock_ledger
                WHERE sku_code = ? AND godown_code = ?
                """,
                (sku_code.strip().upper(), godown_code.strip().upper()),
            ).fetchone()
        return self.money(str(row["qty"]) if row is not None else "0")

    def transfer_between_godowns(
        self,
        *,
        sku_code: str,
        from_godown: str,
        to_godown: str,
        quantity: Decimal,
        unit_cost: Decimal,
        actor_id: int,
    ) -> dict[str, Any]:
        sku = sku_code.strip().upper()
        source = from_godown.strip().upper()
        target = to_godown.strip().upper()
        qty = self.money(quantity)
        cost = self.money(unit_cost)
        if qty <= 0:
            raise ValueError("quantity must be positive")
        if source == target:
            raise ValueError("from_godown and to_godown must differ")

        available = self.current_stock(sku_code=sku, godown_code=source)
        if available < qty:
            raise ValueError("insufficient source stock")

        ts = datetime.utcnow()
        stamp = ts.strftime("%Y%m%d%H%M%S")
        reference = f"GTR-{sku}-{stamp}"
        now_iso = ts.isoformat(timespec="seconds") + "Z"

        eway_payload = {
            "reference": reference,
            "sku_code": sku,
            "from_godown": source,
            "to_godown": target,
            "quantity": self.money_str(qty),
            "unit_cost": self.money_str(cost),
            "consignment_value": self.money_str(qty * cost),
            "drafted_at": now_iso,
        }
        eway_payload["digest"] = sha256(str(eway_payload).encode("utf-8")).hexdigest()

        with self.get_conn() as conn:
            self.ensure_schema(conn)
            conn.execute("BEGIN")
            conn.execute(
                """
                INSERT INTO godown_stock_ledger(
                    sku_code, godown_code, movement_type, quantity, unit_cost, reference_no,
                    voucher_type, ewaybill_draft_json, created_by, created_at
                ) VALUES (?, ?, 'TRANSFER_OUT', ?, ?, ?, 'INTER_GODOWN_TRANSFER', ?, ?, ?)
                """,
                (sku, source, self.money_str(qty), self.money_str(cost), reference, str(eway_payload), actor_id, now_iso),
            )
            conn.execute(
                """
                INSERT INTO godown_stock_ledger(
                    sku_code, godown_code, movement_type, quantity, unit_cost, reference_no,
                    voucher_type, ewaybill_draft_json, created_by, created_at
                ) VALUES (?, ?, 'TRANSFER_IN', ?, ?, ?, 'INTER_GODOWN_TRANSFER', ?, ?, ?)
                """,
                (sku, target, self.money_str(qty), self.money_str(cost), reference, str(eway_payload), actor_id, now_iso),
            )
            conn.commit()

        return {
            "status": "posted",
            "reference": reference,
            "eway_bill_draft": eway_payload,
            "source_balance": self.money_str(self.current_stock(sku_code=sku, godown_code=source)),
            "target_balance": self.money_str(self.current_stock(sku_code=sku, godown_code=target)),
        }

    def predict_stockout(self, *, days: int = 365, min_daily_outflow: Decimal = Decimal("0.0001")) -> list[dict[str, Any]]:
        safe_days = max(30, min(days, 730))
        since = (datetime.utcnow() - timedelta(days=safe_days)).isoformat(timespec="seconds") + "Z"
        pulses: list[dict[str, Any]] = []

        with self.get_conn() as conn:
            self.ensure_schema(conn)
            rows = conn.execute(
                """
                SELECT sku_code, godown_code,
                       SUM(CASE WHEN movement_type IN ('OUT', 'TRANSFER_OUT') THEN CAST(quantity AS REAL) ELSE 0 END) AS outflow,
                       SUM(CASE WHEN movement_type IN ('IN', 'TRANSFER_IN') THEN CAST(quantity AS REAL) ELSE 0 END) AS inflow
                FROM godown_stock_ledger
                WHERE created_at >= ?
                GROUP BY sku_code, godown_code
                """,
                (since,),
            ).fetchall()

        for row in rows:
            sku = str(row["sku_code"])
            godown = str(row["godown_code"])
            outflow = self.money(str(row["outflow"] or "0"))
            daily_outflow = self.money(outflow / Decimal(str(safe_days)))
            if daily_outflow < min_daily_outflow:
                continue
            available = self.current_stock(sku_code=sku, godown_code=godown)
            horizon = Decimal("9999") if daily_outflow == 0 else self.money(available / daily_outflow)
            pulses.append(
                {
                    "sku_code": sku,
                    "godown_code": godown,
                    "available_qty": self.money_str(available),
                    "avg_daily_outflow": self.money_str(daily_outflow),
                    "days_to_stockout": self.money_str(horizon),
                    "risk_band": "CRITICAL" if horizon <= Decimal("7") else "HIGH" if horizon <= Decimal("21") else "NORMAL",
                }
            )

        pulses.sort(key=lambda item: Decimal(item["days_to_stockout"]))
        return pulses
