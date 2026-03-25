from __future__ import annotations

import sqlite3
from datetime import datetime
from decimal import Decimal
from typing import Any


class WIPCapitalization:
    def __init__(self, *, get_conn: callable) -> None:
        self.get_conn = get_conn

    def _money(self, value: Any) -> Decimal:
        return Decimal(str(value or "0")).quantize(Decimal("0.0001"))

    def summarize_direct_expenses(self, project_id: str, month: str) -> dict[str, Any]:
        start_date = f"{month}-01"
        end_date = f"{month}-31"
        with self.get_conn() as conn:
            conn.row_factory = sqlite3.Row
            cols = conn.execute("PRAGMA table_info(journal_entries);").fetchall()
            has_project_id = any(str(col["name"]) == "project_id" for col in cols)
            if not has_project_id:
                material = Decimal("0.0000")
                labor = Decimal("0.0000")
                total = Decimal("0.0000")
                return {
                    "project_id": project_id,
                    "month": month,
                    "material_total": f"{material:.4f}",
                    "labor_total": f"{labor:.4f}",
                    "capitalizable_total": f"{total:.4f}",
                }

            row = conn.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN a.name LIKE '%Material%' THEN CAST(jl.debit AS REAL) - CAST(jl.credit AS REAL) ELSE 0 END), 0) AS material_total,
                    COALESCE(SUM(CASE WHEN a.name LIKE '%Labor%' OR a.name LIKE '%Labour%' THEN CAST(jl.debit AS REAL) - CAST(jl.credit AS REAL) ELSE 0 END), 0) AS labor_total
                FROM journal_entries je
                JOIN journal_lines jl ON jl.entry_id = je.id
                JOIN accounts a ON a.id = jl.account_id
                WHERE je.project_id = ?
                  AND je.date BETWEEN ? AND ?
                """,
                (project_id, start_date, end_date),
            ).fetchone()

        material = self._money(row["material_total"] if row else "0")
        labor = self._money(row["labor_total"] if row else "0")
        total = (material + labor).quantize(Decimal("0.0001"))
        return {
            "project_id": project_id,
            "month": month,
            "material_total": f"{material:.4f}",
            "labor_total": f"{labor:.4f}",
            "capitalizable_total": f"{total:.4f}",
        }

    def capitalize_monthly_expenses(self, project_id: str, month: str) -> dict[str, Any]:
        summary = self.summarize_direct_expenses(project_id, month)
        now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"
        return {
            "status": "prepared",
            "journal_voucher": {
                "reference": f"WIP-{project_id}-{month}",
                "date": f"{month}-31",
                "description": "Monthly WIP capitalization",
                "lines": [
                    {
                        "account": f"Construction WIP - {project_id}",
                        "debit": summary["capitalizable_total"],
                        "credit": "0.0000",
                    },
                    {
                        "account": "Material Expense + Labor Expense",
                        "debit": "0.0000",
                        "credit": summary["capitalizable_total"],
                    },
                ],
                "generated_at": now_iso,
            },
            "summary": summary,
        }
