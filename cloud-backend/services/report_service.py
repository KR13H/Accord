from __future__ import annotations

from decimal import Decimal
from typing import Any, Callable


class ReportService:
    """Financial report engine for Balance Sheet, P&L, and ratio analysis."""

    def __init__(
        self,
        *,
        money: Callable[[Any], Decimal],
        money_str: Callable[[Any], str],
    ) -> None:
        self.money = money
        self.money_str = money_str

    def build_balance_sheet(self, accounts: list[dict[str, Any]]) -> dict[str, Any]:
        assets = Decimal("0")
        liabilities = Decimal("0")
        equity = Decimal("0")

        lines: list[dict[str, Any]] = []
        for row in accounts:
            name = str(row.get("name") or "")
            acc_type = str(row.get("type") or "")
            amount = self.money(str(row.get("balance") or "0"))
            lines.append({"name": name, "type": acc_type, "balance": self.money_str(amount)})
            if acc_type == "Asset":
                assets += amount
            elif acc_type == "Liability":
                liabilities += amount
            elif acc_type == "Equity":
                equity += amount

        return {
            "status": "ok",
            "report": "BALANCE_SHEET",
            "totals": {
                "assets": self.money_str(assets),
                "liabilities": self.money_str(liabilities),
                "equity": self.money_str(equity),
                "liabilities_plus_equity": self.money_str(liabilities + equity),
            },
            "lines": lines,
        }

    def build_profit_and_loss(self, accounts: list[dict[str, Any]]) -> dict[str, Any]:
        revenue = Decimal("0")
        expenses = Decimal("0")

        for row in accounts:
            acc_type = str(row.get("type") or "")
            amount = self.money(str(row.get("balance") or "0"))
            if acc_type == "Revenue":
                revenue += amount
            elif acc_type == "Expense":
                expenses += amount

        net_profit = revenue - expenses
        return {
            "status": "ok",
            "report": "PROFIT_AND_LOSS",
            "summary": {
                "revenue": self.money_str(revenue),
                "expenses": self.money_str(expenses),
                "net_profit": self.money_str(net_profit),
                "margin_pct": round((float(net_profit / revenue) * 100.0), 2) if revenue != 0 else 0.0,
            },
        }

    def build_ratio_analysis(self, accounts: list[dict[str, Any]]) -> dict[str, Any]:
        balance = self.build_balance_sheet(accounts)
        pnl = self.build_profit_and_loss(accounts)

        assets = self.money(balance["totals"]["assets"])
        liabilities = self.money(balance["totals"]["liabilities"])
        equity = self.money(balance["totals"]["equity"])
        current_ratio = float(assets / liabilities) if liabilities != 0 else 0.0
        debt_to_equity = float(liabilities / equity) if equity != 0 else 0.0

        return {
            "status": "ok",
            "report": "RATIO_ANALYSIS",
            "ratios": {
                "current_ratio": round(current_ratio, 4),
                "debt_to_equity": round(debt_to_equity, 4),
                "net_margin_pct": pnl["summary"]["margin_pct"],
            },
        }
