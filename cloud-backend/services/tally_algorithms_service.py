from __future__ import annotations

from decimal import Decimal
from difflib import SequenceMatcher
from typing import Any


class TallyAlgorithmsService:
    """Core accounting algorithms for Tally-grade workflows."""

    @staticmethod
    def inventory_valuation(*, method: str, issue_quantity: Decimal, layers: list[dict[str, Any]]) -> dict[str, Any]:
        qty_left = Decimal(str(issue_quantity))
        if qty_left <= 0:
            raise ValueError("issue_quantity must be positive")

        normalized_layers = []
        for layer in layers:
            normalized_layers.append(
                {
                    "batch_id": str(layer.get("batch_id") or ""),
                    "quantity": Decimal(str(layer.get("quantity") or "0")),
                    "unit_cost": Decimal(str(layer.get("unit_cost") or "0")),
                }
            )

        if method.upper() == "LIFO":
            normalized_layers = list(reversed(normalized_layers))

        consumed: list[dict[str, str]] = []
        cogs = Decimal("0")

        for layer in normalized_layers:
            if qty_left <= 0:
                break
            available = layer["quantity"]
            if available <= 0:
                continue

            take = min(available, qty_left)
            line_cost = take * layer["unit_cost"]
            cogs += line_cost
            qty_left -= take
            consumed.append(
                {
                    "batch_id": str(layer["batch_id"]),
                    "quantity": f"{take:.4f}",
                    "unit_cost": f"{layer['unit_cost']:.4f}",
                    "line_cost": f"{line_cost:.4f}",
                }
            )

        if qty_left > 0:
            raise ValueError("insufficient inventory for requested issue_quantity")

        return {
            "status": "ok",
            "method": method.upper(),
            "issued_quantity": f"{issue_quantity:.4f}",
            "cogs": f"{cogs:.4f}",
            "consumed_layers": consumed,
        }

    @staticmethod
    def plan_stock_transfer(*, sku_code: str, from_godown: str, to_godown: str, quantity: Decimal) -> dict[str, Any]:
        qty = Decimal(str(quantity))
        if qty <= 0:
            raise ValueError("quantity must be positive")
        return {
            "status": "ok",
            "sku_code": sku_code,
            "from_godown": from_godown,
            "to_godown": to_godown,
            "quantity": f"{qty:.4f}",
            "voucher_type": "INTER_GODOWN_TRANSFER",
            "ewaybill_required": True,
        }

    @staticmethod
    def reconcile_bank_statement(*, bank_rows: list[dict[str, Any]], ledger_rows: list[dict[str, Any]]) -> dict[str, Any]:
        unmatched_ledger = ledger_rows[:]
        matches: list[dict[str, Any]] = []

        for bank in bank_rows:
            b_ref = str(bank.get("reference") or bank.get("narration") or "").strip()
            b_amt = Decimal(str(bank.get("amount") or "0"))
            best_idx = -1
            best_score = 0.0

            for idx, led in enumerate(unmatched_ledger):
                l_ref = str(led.get("reference") or led.get("description") or "").strip()
                l_amt = Decimal(str(led.get("amount") or "0"))

                ref_score = SequenceMatcher(None, b_ref.upper(), l_ref.upper()).ratio()
                amount_delta = abs(b_amt - l_amt)
                amount_score = max(0.0, 1.0 - (float(amount_delta) / max(1.0, float(abs(b_amt)))))
                score = (ref_score * 0.7) + (amount_score * 0.3)

                if score > best_score:
                    best_score = score
                    best_idx = idx

            if best_idx >= 0 and best_score >= 0.86:
                led = unmatched_ledger.pop(best_idx)
                matches.append(
                    {
                        "bank_reference": b_ref,
                        "ledger_reference": str(led.get("reference") or ""),
                        "bank_amount": f"{b_amt:.4f}",
                        "ledger_amount": f"{Decimal(str(led.get('amount') or '0')):.4f}",
                        "confidence": round(best_score, 4),
                    }
                )

        return {
            "status": "ok",
            "match_count": len(matches),
            "unmatched_ledger_count": len(unmatched_ledger),
            "matches": matches,
        }
