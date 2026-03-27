from __future__ import annotations

import sqlite3
from contextlib import closing
from decimal import Decimal
from typing import Any


class SPVConsolidation:
    def __init__(self, *, get_conn: callable) -> None:
        self.get_conn = get_conn

    def _money(self, value: Any) -> Decimal:
        return Decimal(str(value or "0")).quantize(Decimal("0.0001"))

    def get_master_pl(self, parent_org_id: str, start_date: str, end_date: str) -> dict[str, Any]:
        with closing(self.get_conn()) as conn:
            conn.row_factory = sqlite3.Row
            spv_rows = conn.execute(
                "SELECT id, legal_name FROM spvs WHERE parent_org_id = ? ORDER BY legal_name ASC",
                (parent_org_id,),
            ).fetchall()
            spv_ids = [str(row["id"]) for row in spv_rows]

            if not spv_ids:
                return {
                    "parent_org_id": parent_org_id,
                    "period": {"start_date": start_date, "end_date": end_date},
                    "spv_count": 0,
                    "revenue": "0.0000",
                    "expenses": "0.0000",
                    "net_profit": "0.0000",
                    "by_spv": [],
                }

            placeholders = ",".join("?" for _ in spv_ids)
            rows = conn.execute(
                f"""
                SELECT
                    p.spv_id AS spv_id,
                    COALESCE(SUM(CASE WHEN a.type = 'Revenue' THEN CAST(jl.credit AS REAL) - CAST(jl.debit AS REAL) ELSE 0 END), 0) AS revenue,
                    COALESCE(SUM(CASE WHEN a.type = 'Expense' THEN CAST(jl.debit AS REAL) - CAST(jl.credit AS REAL) ELSE 0 END), 0) AS expenses
                FROM journal_entries je
                JOIN journal_lines jl ON jl.entry_id = je.id
                JOIN accounts a ON a.id = jl.account_id
                JOIN projects p ON p.id = je.project_id
                WHERE p.spv_id IN ({placeholders})
                  AND je.date BETWEEN ? AND ?
                GROUP BY p.spv_id
                """,
                (*spv_ids, start_date, end_date),
            ).fetchall()

        by_spv: list[dict[str, Any]] = []
        total_revenue = Decimal("0")
        total_expenses = Decimal("0")

        name_map = {str(row["id"]): str(row["legal_name"]) for row in spv_rows}
        for row in rows:
            revenue = self._money(row["revenue"])
            expenses = self._money(row["expenses"])
            net = (revenue - expenses).quantize(Decimal("0.0001"))
            total_revenue += revenue
            total_expenses += expenses
            spv_id = str(row["spv_id"])
            by_spv.append(
                {
                    "spv_id": spv_id,
                    "spv_name": name_map.get(spv_id, spv_id),
                    "revenue": f"{revenue:.4f}",
                    "expenses": f"{expenses:.4f}",
                    "net_profit": f"{net:.4f}",
                }
            )

        return {
            "parent_org_id": parent_org_id,
            "period": {"start_date": start_date, "end_date": end_date},
            "spv_count": len(spv_ids),
            "revenue": f"{total_revenue:.4f}",
            "expenses": f"{total_expenses:.4f}",
            "net_profit": f"{(total_revenue - total_expenses):.4f}",
            "by_spv": by_spv,
        }
