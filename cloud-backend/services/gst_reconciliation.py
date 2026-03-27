from __future__ import annotations

from decimal import Decimal
from typing import Any

from thefuzz import fuzz


def _norm(value: Any) -> str:
    return str(value or "").strip().upper().replace(" ", "")


def _money(value: Any) -> Decimal:
    return Decimal(str(value or "0")).quantize(Decimal("0.01"))


def reconcile_itc(internal_invoices: list[dict], gstr2b_data: list[dict]) -> dict:
    matched: list[dict[str, Any]] = []
    discrepant: list[dict[str, Any]] = []
    missing_in_gstr: list[dict[str, Any]] = []

    used_gstr_indexes: set[int] = set()

    for internal in internal_invoices:
        i_gstin = _norm(internal.get("gstin"))
        i_inv = _norm(internal.get("invoice_number"))
        i_tax = _money(internal.get("total_tax"))

        best_idx = None
        best_score = -1
        for idx, gstr in enumerate(gstr2b_data):
            if idx in used_gstr_indexes:
                continue
            if _norm(gstr.get("gstin")) != i_gstin:
                continue

            g_inv = _norm(gstr.get("invoice_number"))
            score = 100 if g_inv == i_inv else fuzz.ratio(i_inv, g_inv)
            if score > best_score:
                best_score = score
                best_idx = idx

        if best_idx is None or best_score < 95:
            missing_in_gstr.append(
                {
                    "gstin": i_gstin,
                    "invoice_number": internal.get("invoice_number"),
                    "reason": "No government match with >=95 confidence",
                }
            )
            continue

        used_gstr_indexes.add(best_idx)
        gov = gstr2b_data[best_idx]
        g_tax = _money(gov.get("total_tax"))
        delta = (i_tax - g_tax).quantize(Decimal("0.01"))

        payload = {
            "gstin": i_gstin,
            "invoice_number_internal": internal.get("invoice_number"),
            "invoice_number_gstr": gov.get("invoice_number"),
            "match_confidence": best_score,
            "internal_total_tax": f"{i_tax:.2f}",
            "gstr_total_tax": f"{g_tax:.2f}",
            "tax_delta": f"{delta:.2f}",
            "status": "DISCREPANT" if abs(delta) > Decimal("1.00") else "MATCHED",
        }
        matched.append(payload)
        if payload["status"] == "DISCREPANT":
            discrepant.append(payload)

    return {
        "matched_count": len(matched),
        "discrepant_flags": discrepant,
        "actionable_missing": missing_in_gstr,
        "matched_records": matched,
    }
