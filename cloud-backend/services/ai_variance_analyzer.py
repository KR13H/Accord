from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Awaitable, Callable


MONEY_QUANT = Decimal("0.01")


def _money(value: Any) -> Decimal:
    return Decimal(str(value or "0")).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)


class AiVarianceAnalyzer:
    def __init__(
        self,
        *,
        get_conn: Callable[[], sqlite3.Connection],
        run_ollama_generate: Callable[..., Awaitable[str]],
        model: str,
    ) -> None:
        self.get_conn = get_conn
        self.run_ollama_generate = run_ollama_generate
        self.model = model

    def ensure_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS spv_budget_lines (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                spv_id TEXT NOT NULL,
                category TEXT NOT NULL,
                monthly_budget_inr TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(spv_id, category)
            )
            """
        )
        conn.commit()

    def _seed_defaults_if_missing(self, conn: sqlite3.Connection, spv_id: str) -> None:
        defaults = {
            "steel": "2500000.00",
            "labor": "1800000.00",
            "cement": "900000.00",
            "electrical": "600000.00",
            "compliance": "350000.00",
        }
        now_iso = datetime.utcnow().isoformat(timespec="seconds") + "Z"

        for category, budget in defaults.items():
            conn.execute(
                """
                INSERT OR IGNORE INTO spv_budget_lines(spv_id, category, monthly_budget_inr, updated_at)
                VALUES (?, ?, ?, ?)
                """,
                (spv_id, category, budget, now_iso),
            )

    def _actual_for_category(self, conn: sqlite3.Connection, *, category: str, month_start: str, today: str) -> Decimal:
        keyword_map = {
            "steel": ["%steel%", "%rebar%"],
            "labor": ["%labor%", "%labour%", "%wages%"],
            "cement": ["%cement%"],
            "electrical": ["%electrical%", "%wiring%"],
            "compliance": ["%compliance%", "%rera%", "%legal%", "%audit%"],
        }
        patterns = keyword_map.get(category, [f"%{category.lower()}%"])
        clauses = " OR ".join("LOWER(a.name) LIKE ?" for _ in patterns)
        query = f"""
            SELECT COALESCE(SUM(CAST(jl.debit AS REAL) - CAST(jl.credit AS REAL)), 0) AS actual_total
            FROM journal_entries je
            JOIN journal_lines jl ON jl.entry_id = je.id
            JOIN accounts a ON a.id = jl.account_id
            WHERE a.type = 'Expense'
              AND je.date BETWEEN ? AND ?
              AND ({clauses})
        """
        row = conn.execute(query, (month_start, today, *patterns)).fetchone()
        return _money(row["actual_total"] if row is not None else "0")

    def get_budget_vs_actual(self, *, spv_id: str) -> dict[str, Any]:
        safe_spv = (spv_id or "SPV-DEFAULT").strip() or "SPV-DEFAULT"
        today = date.today().isoformat()
        month_start = date.today().replace(day=1).isoformat()

        with closing(self.get_conn()) as conn:
            conn.row_factory = sqlite3.Row
            self.ensure_schema(conn)
            self._seed_defaults_if_missing(conn, safe_spv)

            budget_rows = conn.execute(
                """
                SELECT category, monthly_budget_inr
                FROM spv_budget_lines
                WHERE spv_id = ?
                ORDER BY category ASC
                """,
                (safe_spv,),
            ).fetchall()

            lines: list[dict[str, Any]] = []
            for row in budget_rows:
                category = str(row["category"])
                budget = _money(row["monthly_budget_inr"])
                actual = self._actual_for_category(conn, category=category, month_start=month_start, today=today)
                variance = (actual - budget).quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)
                variance_pct = float((variance / budget) * Decimal("100")) if budget > 0 else 0.0
                lines.append(
                    {
                        "category": category,
                        "budget_inr": f"{budget:.2f}",
                        "actual_inr": f"{actual:.2f}",
                        "variance_inr": f"{variance:.2f}",
                        "variance_pct": round(variance_pct, 2),
                    }
                )

        lines.sort(key=lambda item: Decimal(str(item["variance_inr"])), reverse=True)
        return {
            "spv_id": safe_spv,
            "period": {"from": month_start, "to": today},
            "items": lines,
            "top_overruns": [item for item in lines if Decimal(str(item["variance_inr"])) > 0][:3],
        }

    async def generate_cfo_markdown(self, *, payload: dict[str, Any]) -> str:
        prompt = (
            "You are the Chief Financial Officer of an Indian real-estate enterprise.\n"
            "Analyze the Budget vs Actuals JSON and produce STRICT markdown only.\n"
            "Output exactly 3 paragraphs.\n"
            "Paragraph 1: Executive summary of current month variance for this SPV.\n"
            "Paragraph 2: Explain top 3 cost overruns with likely root causes (steel, labor, cement, etc.).\n"
            "Paragraph 3: Actionable financial mitigations with measurable targets.\n"
            "Do not add tables, lists, JSON, or disclaimers.\n"
            "Do not invent categories not present in payload.\n\n"
            f"Budget vs Actuals JSON:\n{payload}"
        )

        markdown = (await self.run_ollama_generate(model=self.model, prompt=prompt)).strip()
        if not markdown:
            raise RuntimeError("variance analysis model returned empty output")
        return markdown
