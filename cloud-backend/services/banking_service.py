from __future__ import annotations

import difflib
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable


@dataclass
class ReconciliationMatch:
    bank_reference: str
    ledger_reference: str
    confidence: float
    amount_delta: Decimal


class BankingService:
    """Banking and BRS automation service for statement-to-ledger reconciliation."""

    def __init__(
        self,
        *,
        parse_amount_from_text: Callable[[str | None], Decimal],
        money_str: Callable[[Any], str],
    ) -> None:
        self.parse_amount_from_text = parse_amount_from_text
        self.money_str = money_str

    def _score(self, left: str, right: str) -> float:
        return difflib.SequenceMatcher(None, left.upper().strip(), right.upper().strip()).ratio()

    def reconcile_statement(
        self,
        *,
        bank_rows: list[dict[str, Any]],
        ledger_rows: list[dict[str, Any]],
        threshold: float = 0.86,
    ) -> dict[str, Any]:
        safe_threshold = min(max(threshold, 0.5), 0.995)
        ledger_pool = list(ledger_rows)

        matched: list[dict[str, Any]] = []
        unmatched_bank: list[dict[str, Any]] = []
        unmatched_ledger_ids: set[int] = {
            int(item.get("id", idx + 1)) for idx, item in enumerate(ledger_pool)
        }

        for bank in bank_rows:
            bank_ref = str(bank.get("reference") or bank.get("narration") or "").strip()
            bank_amount = self.parse_amount_from_text(str(bank.get("amount") or "0"))
            best_score = 0.0
            best_index = -1
            best_delta = Decimal("0")

            for idx, ledger in enumerate(ledger_pool):
                ledger_ref = str(ledger.get("reference") or ledger.get("description") or "").strip()
                ledger_amount = self.parse_amount_from_text(str(ledger.get("amount") or "0"))
                delta = abs(bank_amount - ledger_amount)
                text_score = self._score(bank_ref, ledger_ref)
                amount_score = max(0.0, 1.0 - float(delta) / max(float(abs(bank_amount)) if bank_amount != 0 else 1.0, 1.0))
                score = (text_score * 0.65) + (amount_score * 0.35)
                if score > best_score:
                    best_score = score
                    best_index = idx
                    best_delta = delta

            if best_index >= 0 and best_score >= safe_threshold:
                ledger = ledger_pool.pop(best_index)
                ledger_id = int(ledger.get("id", 0))
                unmatched_ledger_ids.discard(ledger_id)
                matched.append(
                    {
                        "bank_reference": bank_ref,
                        "ledger_reference": str(ledger.get("reference") or ""),
                        "confidence": round(best_score, 4),
                        "amount_delta": self.money_str(best_delta),
                        "bank_amount": self.money_str(bank_amount),
                        "ledger_amount": self.money_str(self.parse_amount_from_text(str(ledger.get("amount") or "0"))),
                    }
                )
            else:
                unmatched_bank.append(
                    {
                        "reference": bank_ref,
                        "amount": self.money_str(bank_amount),
                    }
                )

        unmatched_ledger: list[dict[str, Any]] = [
            {
                "id": int(item.get("id", 0)),
                "reference": str(item.get("reference") or ""),
                "amount": self.money_str(self.parse_amount_from_text(str(item.get("amount") or "0"))),
            }
            for item in ledger_rows
            if int(item.get("id", 0)) in unmatched_ledger_ids
        ]

        total = max(1, len(bank_rows))
        match_rate = round((len(matched) / total) * 100, 2)
        return {
            "status": "ok",
            "engine": "MISTRAL_FUZZY_BRS",
            "threshold": safe_threshold,
            "match_rate_pct": match_rate,
            "matched": matched,
            "unmatched_bank": unmatched_bank,
            "unmatched_ledger": unmatched_ledger,
        }
