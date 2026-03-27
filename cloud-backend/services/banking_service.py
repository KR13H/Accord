from __future__ import annotations

import difflib
import json
import re
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
        run_ollama_generate: Callable[..., Any] | None = None,
        recon_model: str = "llama3.2",
    ) -> None:
        self.parse_amount_from_text = parse_amount_from_text
        self.money_str = money_str
        self.run_ollama_generate = run_ollama_generate
        self.recon_model = recon_model

    def _normalize_text(self, value: str) -> str:
        text = re.sub(r"[^A-Za-z0-9 ]+", " ", (value or "").upper())
        return " ".join(text.split())

    def _levenshtein_distance(self, left: str, right: str) -> int:
        if left == right:
            return 0
        if not left:
            return len(right)
        if not right:
            return len(left)

        prev = list(range(len(right) + 1))
        for i, lch in enumerate(left, start=1):
            curr = [i]
            for j, rch in enumerate(right, start=1):
                insertions = prev[j] + 1
                deletions = curr[j - 1] + 1
                substitutions = prev[j - 1] + (0 if lch == rch else 1)
                curr.append(min(insertions, deletions, substitutions))
            prev = curr
        return prev[-1]

    def _levenshtein_ratio(self, left: str, right: str) -> float:
        if not left and not right:
            return 1.0
        distance = self._levenshtein_distance(left, right)
        base = max(len(left), len(right), 1)
        return max(0.0, 1.0 - (distance / base))

    def _same_amount(self, left: Decimal, right: Decimal, tolerance: Decimal) -> bool:
        return abs(left - right) <= tolerance

    def _to_iso_date(self, value: Any) -> str:
        raw = str(value or "").strip()
        return raw[:10] if len(raw) >= 10 else raw

    def _score(self, left: str, right: str) -> float:
        return difflib.SequenceMatcher(None, left.upper().strip(), right.upper().strip()).ratio()

    def _best_candidate_for_bank_row(
        self,
        *,
        bank_row: dict[str, Any],
        ledger_pool: list[dict[str, Any]],
        amount_tolerance: Decimal,
    ) -> tuple[int, float, Decimal]:
        bank_ref = self._normalize_text(str(bank_row.get("reference") or bank_row.get("narration") or ""))
        bank_amount = self.parse_amount_from_text(str(bank_row.get("amount") or "0"))
        best_index = -1
        best_score = 0.0
        best_delta = Decimal("0")

        for idx, ledger in enumerate(ledger_pool):
            ledger_ref = self._normalize_text(str(ledger.get("reference") or ledger.get("description") or ""))
            ledger_amount = self.parse_amount_from_text(str(ledger.get("amount") or "0"))
            amount_delta = abs(bank_amount - ledger_amount)
            if amount_delta > amount_tolerance:
                continue

            lev_ratio = self._levenshtein_ratio(bank_ref, ledger_ref)
            seq_ratio = self._score(bank_ref, ledger_ref)
            score = (lev_ratio * 0.7) + (seq_ratio * 0.3)
            if score > best_score:
                best_score = score
                best_index = idx
                best_delta = amount_delta

        return best_index, best_score, best_delta

    async def _ai_pick_candidate(
        self,
        *,
        bank_row: dict[str, Any],
        candidates: list[dict[str, Any]],
    ) -> dict[str, Any] | None:
        if self.run_ollama_generate is None or not candidates:
            return None

        prompt = (
            "You are a strict reconciliation engine. "
            "Given one bank transaction and candidate ledger entries, return ONLY JSON with keys: "
            "ledger_id (int), confidence (0-1), reason (string), should_match (boolean). "
            "If no reliable match, set should_match false and ledger_id 0.\n"
            f"Bank row: {json.dumps(bank_row, default=str)}\n"
            f"Candidates: {json.dumps(candidates, default=str)}"
        )
        try:
            raw = await self.run_ollama_generate(model=self.recon_model, prompt=prompt)
            parsed = json.loads(str(raw).strip())
            if not isinstance(parsed, dict):
                return None
            if not bool(parsed.get("should_match")):
                return None
            ledger_id = int(parsed.get("ledger_id") or 0)
            confidence = float(parsed.get("confidence") or 0.0)
            if ledger_id <= 0 or confidence < 0.6:
                return None
            return {
                "ledger_id": ledger_id,
                "confidence": min(max(confidence, 0.0), 1.0),
                "reason": str(parsed.get("reason") or "AI semantic match"),
            }
        except Exception:
            return None

    async def reconcile_statement_multi_pass(
        self,
        *,
        bank_rows: list[dict[str, Any]],
        ledger_rows: list[dict[str, Any]],
        fuzzy_threshold: float = 0.84,
        amount_tolerance: str = "1.0000",
        enable_ai: bool = True,
    ) -> dict[str, Any]:
        safe_threshold = min(max(float(fuzzy_threshold), 0.5), 0.995)
        tolerance = self.parse_amount_from_text(str(amount_tolerance or "1.0000"))

        bank_pool = [dict(item) for item in bank_rows]
        ledger_pool = [dict(item) for item in ledger_rows]
        unmatched_ledger_ids: set[int] = {int(item.get("id") or 0) for item in ledger_pool}

        matched: list[dict[str, Any]] = []
        matched_ledger_ids: set[int] = set()

        # Pass 1: Deterministic exact match on amount + date.
        pass1_unmatched_bank: list[dict[str, Any]] = []
        for bank in bank_pool:
            bank_amount = self.parse_amount_from_text(str(bank.get("amount") or "0"))
            bank_date = self._to_iso_date(bank.get("date"))
            selected_idx = -1
            for idx, ledger in enumerate(ledger_pool):
                ledger_id = int(ledger.get("id") or 0)
                if ledger_id in matched_ledger_ids:
                    continue
                ledger_amount = self.parse_amount_from_text(str(ledger.get("amount") or "0"))
                ledger_date = self._to_iso_date(ledger.get("date"))
                if self._same_amount(bank_amount, ledger_amount, tolerance) and bank_date and bank_date == ledger_date:
                    selected_idx = idx
                    break

            if selected_idx >= 0:
                ledger = ledger_pool[selected_idx]
                ledger_id = int(ledger.get("id") or 0)
                matched_ledger_ids.add(ledger_id)
                unmatched_ledger_ids.discard(ledger_id)
                matched.append(
                    {
                        "pass": "exact_amount_date",
                        "confidence": 1.0,
                        "bank_reference": str(bank.get("reference") or bank.get("narration") or ""),
                        "ledger_reference": str(ledger.get("reference") or ledger.get("description") or ""),
                        "bank_amount": self.money_str(bank_amount),
                        "ledger_amount": self.money_str(ledger_amount),
                        "amount_delta": self.money_str(abs(bank_amount - ledger_amount)),
                        "bank_date": bank_date,
                        "ledger_date": ledger_date,
                        "ledger_id": ledger_id,
                        "reason": "Exact match on amount and date",
                    }
                )
            else:
                pass1_unmatched_bank.append(bank)

        # Pass 2: Levenshtein + sequence fuzzy match constrained by amount tolerance.
        pass2_unmatched_bank: list[dict[str, Any]] = []
        for bank in pass1_unmatched_bank:
            filtered_ledger = [row for row in ledger_pool if int(row.get("id") or 0) not in matched_ledger_ids]
            best_index, best_score, best_delta = self._best_candidate_for_bank_row(
                bank_row=bank,
                ledger_pool=filtered_ledger,
                amount_tolerance=tolerance,
            )
            if best_index >= 0 and best_score >= safe_threshold:
                ledger = filtered_ledger[best_index]
                ledger_id = int(ledger.get("id") or 0)
                bank_amount = self.parse_amount_from_text(str(bank.get("amount") or "0"))
                ledger_amount = self.parse_amount_from_text(str(ledger.get("amount") or "0"))
                matched_ledger_ids.add(ledger_id)
                unmatched_ledger_ids.discard(ledger_id)
                matched.append(
                    {
                        "pass": "fuzzy_levenshtein",
                        "confidence": round(best_score, 4),
                        "bank_reference": str(bank.get("reference") or bank.get("narration") or ""),
                        "ledger_reference": str(ledger.get("reference") or ledger.get("description") or ""),
                        "bank_amount": self.money_str(bank_amount),
                        "ledger_amount": self.money_str(ledger_amount),
                        "amount_delta": self.money_str(best_delta),
                        "bank_date": self._to_iso_date(bank.get("date")),
                        "ledger_date": self._to_iso_date(ledger.get("date")),
                        "ledger_id": ledger_id,
                        "reason": "Fuzzy description match with amount guard",
                    }
                )
            else:
                pass2_unmatched_bank.append(bank)

        # Pass 3: Optional AI semantic fallback for unresolved rows.
        pass3_unmatched_bank: list[dict[str, Any]] = []
        for bank in pass2_unmatched_bank:
            remaining = [row for row in ledger_pool if int(row.get("id") or 0) not in matched_ledger_ids]
            if not remaining or not enable_ai:
                pass3_unmatched_bank.append(bank)
                continue

            bank_amount = self.parse_amount_from_text(str(bank.get("amount") or "0"))
            narrowed = []
            for ledger in remaining:
                ledger_amount = self.parse_amount_from_text(str(ledger.get("amount") or "0"))
                if abs(bank_amount - ledger_amount) <= (tolerance * Decimal("2")):
                    narrowed.append(
                        {
                            "id": int(ledger.get("id") or 0),
                            "reference": str(ledger.get("reference") or ledger.get("description") or ""),
                            "date": self._to_iso_date(ledger.get("date")),
                            "amount": self.money_str(ledger_amount),
                        }
                    )

            ai_match = await self._ai_pick_candidate(bank_row=bank, candidates=narrowed[:8])
            if ai_match is None:
                pass3_unmatched_bank.append(bank)
                continue

            ledger_id = int(ai_match["ledger_id"])
            ledger = next((row for row in remaining if int(row.get("id") or 0) == ledger_id), None)
            if ledger is None:
                pass3_unmatched_bank.append(bank)
                continue

            ledger_amount = self.parse_amount_from_text(str(ledger.get("amount") or "0"))
            matched_ledger_ids.add(ledger_id)
            unmatched_ledger_ids.discard(ledger_id)
            matched.append(
                {
                    "pass": "ai_semantic",
                    "confidence": round(float(ai_match["confidence"]), 4),
                    "bank_reference": str(bank.get("reference") or bank.get("narration") or ""),
                    "ledger_reference": str(ledger.get("reference") or ledger.get("description") or ""),
                    "bank_amount": self.money_str(bank_amount),
                    "ledger_amount": self.money_str(ledger_amount),
                    "amount_delta": self.money_str(abs(bank_amount - ledger_amount)),
                    "bank_date": self._to_iso_date(bank.get("date")),
                    "ledger_date": self._to_iso_date(ledger.get("date")),
                    "ledger_id": ledger_id,
                    "reason": str(ai_match.get("reason") or "AI semantic match"),
                }
            )

        unmatched_ledger: list[dict[str, Any]] = [
            {
                "id": int(item.get("id", 0)),
                "reference": str(item.get("reference") or item.get("description") or ""),
                "amount": self.money_str(self.parse_amount_from_text(str(item.get("amount") or "0"))),
                "date": self._to_iso_date(item.get("date")),
            }
            for item in ledger_rows
            if int(item.get("id", 0)) in unmatched_ledger_ids
        ]

        unmatched_bank = [
            {
                "reference": str(row.get("reference") or row.get("narration") or ""),
                "amount": self.money_str(self.parse_amount_from_text(str(row.get("amount") or "0"))),
                "date": self._to_iso_date(row.get("date")),
            }
            for row in pass3_unmatched_bank
        ]

        total = max(1, len(bank_rows))
        return {
            "status": "ok",
            "engine": "ACCORD_BANK_RECON_V3",
            "passes": ["exact_amount_date", "fuzzy_levenshtein", "ai_semantic"],
            "fuzzy_threshold": safe_threshold,
            "amount_tolerance": self.money_str(tolerance),
            "match_rate_pct": round((len(matched) / total) * 100, 2),
            "matched": matched,
            "unmatched_bank": unmatched_bank,
            "unmatched_ledger": unmatched_ledger,
            "summary": {
                "bank_rows": len(bank_rows),
                "ledger_rows": len(ledger_rows),
                "matched_count": len(matched),
                "unmatched_bank_count": len(unmatched_bank),
                "unmatched_ledger_count": len(unmatched_ledger),
                "pass_breakdown": {
                    "exact_amount_date": len([m for m in matched if m.get("pass") == "exact_amount_date"]),
                    "fuzzy_levenshtein": len([m for m in matched if m.get("pass") == "fuzzy_levenshtein"]),
                    "ai_semantic": len([m for m in matched if m.get("pass") == "ai_semantic"]),
                },
            },
        }

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
