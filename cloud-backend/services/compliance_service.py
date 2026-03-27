from __future__ import annotations

import difflib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable


@dataclass
class _MatchCandidate:
    entry_id: int
    gstin: str
    reference: str
    vendor_name: str
    date: str
    tax_amount: str
    taxable_value: str


class ComplianceService:
    """Implements tax compliance intelligence, including GSTR-2B reconciliation.

    The service performs fuzzy matching against tax ledger records and emits risk signals.
    """

    def __init__(
        self,
        *,
        extract_2b_records: Callable[[Any], list[dict[str, Any]]],
        build_nexus_graph: Callable[[list[dict[str, Any]]], dict[str, Any]],
        draft_vendor_nudge_message: Callable[..., Any],
        run_ollama_generate: Callable[..., Any],
        forensic_model: str,
        parse_amount_from_text: Callable[[str], Any],
        money_str: Callable[[Any], str],
    ) -> None:
        """Initializes compliance service dependencies.

        Args:
            extract_2b_records: Extractor for uploaded GSTR-2B datasets.
            build_nexus_graph: Nexus graph generator.
            draft_vendor_nudge_message: Nudge-message generator.
            run_ollama_generate: Async Ollama text-generation bridge.
            forensic_model: Model identifier used for risk categorization.
            parse_amount_from_text: Numeric parser callable.
            money_str: Monetary formatter callable.

        Hardware Impact:
            Uses thread workers for fuzzy scoring to utilize CPU efficiency cores.
        Logic Invariants:
            Matching first prioritizes strong keys, then fuzzy confidence fallback.
        Legal Context:
            Flags ghost invoices to mitigate GST ITC denial and Rule 37A exposure.
        """
        self.extract_2b_records = extract_2b_records
        self.build_nexus_graph = build_nexus_graph
        self.draft_vendor_nudge_message = draft_vendor_nudge_message
        self.run_ollama_generate = run_ollama_generate
        self.forensic_model = forensic_model
        self.parse_amount_from_text = parse_amount_from_text
        self.money_str = money_str

    def _fuzzy_score(self, left: str, right: str) -> float:
        """Computes fuzzy similarity ratio between two strings.

        Args:
            left: First normalized string.
            right: Second normalized string.

        Returns:
            Similarity ratio in range [0, 1].

        Hardware Impact:
            CPU-only lightweight sequence comparison.
        Logic Invariants:
            Normalized to deterministic score for equal inputs.
        Legal Context:
            Supports explainable matching confidence in audit reviews.
        """
        return difflib.SequenceMatcher(None, left, right).ratio()

    def _build_candidates(self, ledger_rows: list[dict[str, Any]]) -> list[_MatchCandidate]:
        """Normalizes ledger rows into match candidates.

        Args:
            ledger_rows: Raw ledger rows.

        Returns:
            Candidate list suitable for fast matching.

        Hardware Impact:
            Minimal CPU/memory transformation overhead.
        Logic Invariants:
            Candidate fields are normalized to uppercase where required.
        Legal Context:
            Preserves invoice identity fields used in reconciliation evidence.
        """
        candidates: list[_MatchCandidate] = []
        for row in ledger_rows:
            candidates.append(
                _MatchCandidate(
                    entry_id=int(row.get("id", 0)),
                    gstin=str(row.get("counterparty_gstin") or "").strip().upper(),
                    reference=str(row.get("reference") or "").strip(),
                    vendor_name=str(row.get("vendor_legal_name") or "Vendor").strip(),
                    date=str(row.get("date") or "").strip(),
                    tax_amount=self.money_str(self.parse_amount_from_text(str(row.get("tax_amount") or "0"))),
                    taxable_value=self.money_str(self.parse_amount_from_text(str(row.get("taxable_value") or "0"))),
                )
            )
        return candidates

    def _risk_rank(self, risk_level: str) -> int:
        """Maps risk labels to sortable priority ranks.

        Args:
            risk_level: Risk label text.

        Returns:
            Integer priority where larger is more severe.

        Hardware Impact:
            Constant-time branch logic.
        Logic Invariants:
            Unknown labels default to lowest risk rank.
        Legal Context:
            Ensures deterministic ordering for compliance escalation queues.
        """
        normalized = str(risk_level).strip().upper()
        if normalized == "CRITICAL":
            return 4
        if normalized == "HIGH":
            return 3
        if normalized == "MEDIUM":
            return 2
        return 1

    async def _categorize_risk(self, *, mismatch: dict[str, Any]) -> str:
        """Uses Mistral to categorize mismatch risk severity.

        Args:
            mismatch: Mismatch payload.

        Returns:
            Risk label such as HIGH, MEDIUM, or CRITICAL.

        Hardware Impact:
            Invokes local LLM inference; may warm CPU/GPU paths on Apple silicon.
        Logic Invariants:
            Falls back to deterministic label when model call fails.
        Legal Context:
            Prioritizes operational response to reduce GST compliance penalties.
        """
        prompt = (
            "You are an Accord GST risk classifier. "
            "Classify this mismatch as CRITICAL, HIGH, MEDIUM, or LOW and return one label only. "
            f"Mismatch: {mismatch}"
        )
        try:
            raw = await self.run_ollama_generate(model=self.forensic_model, prompt=prompt)
            normalized = str(raw).strip().upper()
            for label in ("CRITICAL", "HIGH", "MEDIUM", "LOW"):
                if label in normalized:
                    return label
        except Exception:
            pass
        tax_amount = self.parse_amount_from_text(str(mismatch.get("tax_amount") or "0"))
        if tax_amount >= 50000:
            return "CRITICAL"
        if tax_amount >= 15000:
            return "HIGH"
        if tax_amount >= 5000:
            return "MEDIUM"
        return "LOW"

    async def reconcile_gstr2b(
        self,
        *,
        two_b_path: Any,
        ledger_rows: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Runs GSTR-2B reconciliation with fuzzy matching and risk enrichment.

        Args:
            two_b_path: Uploaded 2B file path.
            ledger_rows: Tax-ledger records to reconcile.

        Returns:
            Reconciliation payload with ghost invoices and nexus graph.

        Hardware Impact:
            Fuzzy matching parallelized across CPU threads for throughput on M3 cores.
        Logic Invariants:
            Strong key match always supersedes fuzzy fallback.
        Legal Context:
            Detects ghost invoices that can trigger ITC denial under GST controls.
        """
        records_2b = self.extract_2b_records(two_b_path)
        two_b_index = set()
        for row in records_2b:
            gstin = str(row.get("gstin") or "").strip().upper()
            ref = str(row.get("invoice_reference") or "").strip()
            tax_amount = self.money_str(self.parse_amount_from_text(str(row.get("tax_amount") or "0")))
            dt = str(row.get("invoice_date") or "").strip()
            if ref:
                two_b_index.add(f"{gstin}|{ref}")
            two_b_index.add(f"{gstin}|{tax_amount}|{dt}")

        candidates = self._build_candidates(ledger_rows)

        def is_missing(candidate: _MatchCandidate) -> tuple[_MatchCandidate, bool, float]:
            strong_key = f"{candidate.gstin}|{candidate.reference}"
            weak_key = f"{candidate.gstin}|{candidate.tax_amount}|{candidate.date}"
            if strong_key in two_b_index or weak_key in two_b_index:
                return candidate, False, 1.0

            gstin_candidates = [
                r for r in records_2b if str(r.get("gstin") or "").strip().upper() == candidate.gstin
            ]
            if not gstin_candidates:
                return candidate, True, 0.0

            best = 0.0
            for row in gstin_candidates:
                ref_score = self._fuzzy_score(candidate.reference.upper(), str(row.get("invoice_reference") or "").upper())
                vendor_score = self._fuzzy_score(candidate.vendor_name.upper(), str(row.get("vendor_name") or "").upper())
                score = (ref_score * 0.75) + (vendor_score * 0.25)
                if score > best:
                    best = score
            return candidate, best < 0.86, best

        max_workers = min(8, max(1, len(candidates)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            matched = list(executor.map(is_missing, candidates))

        ghost_invoices: list[dict[str, Any]] = []
        risk_breakdown: dict[str, int] = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
        vendor_leaks: dict[str, Decimal] = {}
        total_potential_tax_leak = Decimal("0")

        for candidate, missing, confidence in matched:
            if not missing or not candidate.gstin:
                continue
            nudge = await self.draft_vendor_nudge_message(
                vendor_name=candidate.vendor_name,
                gstin=candidate.gstin,
                invoice_reference=candidate.reference,
                invoice_amount=candidate.taxable_value,
                mismatch_reason="Ledger invoice not found in uploaded GSTR-2B dataset",
            )
            mismatch = {
                "entry_id": candidate.entry_id,
                "reference": candidate.reference,
                "date": candidate.date,
                "gstin": candidate.gstin,
                "vendor_name": candidate.vendor_name,
                "tax_amount": candidate.tax_amount,
                "taxable_value": candidate.taxable_value,
                "risk": "GHOST_INVOICE",
                "fuzzy_match_confidence": round(confidence, 4),
                "confidence_band": "LOW" if confidence < 0.6 else ("MEDIUM" if confidence < 0.86 else "HIGH"),
                "nudge_template": nudge,
            }
            mismatch["risk_level"] = await self._categorize_risk(mismatch=mismatch)
            mismatch["recommended_action"] = (
                "WITHHOLD_AND_ESCALATE"
                if mismatch["risk_level"] == "CRITICAL"
                else "URGENT_VENDOR_NUDGE"
                if mismatch["risk_level"] == "HIGH"
                else "REVIEW_AND_TRACK"
                if mismatch["risk_level"] == "MEDIUM"
                else "MONITOR"
            )

            leak_amount = self.parse_amount_from_text(str(mismatch["tax_amount"]))
            total_potential_tax_leak += leak_amount
            vendor_key = f"{candidate.gstin}|{candidate.vendor_name}"
            vendor_leaks[vendor_key] = vendor_leaks.get(vendor_key, Decimal("0")) + leak_amount
            risk_breakdown[mismatch["risk_level"]] = risk_breakdown.get(mismatch["risk_level"], 0) + 1
            ghost_invoices.append(mismatch)

        ghost_invoices.sort(
            key=lambda row: (
                -self._risk_rank(str(row.get("risk_level", "LOW"))),
                -float(self.parse_amount_from_text(str(row.get("tax_amount") or "0"))),
            )
        )

        ranked_vendors = sorted(
            (
                {
                    "gstin": key.split("|", 1)[0],
                    "vendor_name": key.split("|", 1)[1],
                    "potential_tax_leak": self.money_str(amount),
                }
                for key, amount in vendor_leaks.items()
            ),
            key=lambda row: -float(self.parse_amount_from_text(str(row["potential_tax_leak"]))),
        )

        nexus_graph = self.build_nexus_graph(ledger_rows)
        return {
            "uploaded_records": len(records_2b),
            "ledger_records": len(ledger_rows),
            "ghost_invoices": ghost_invoices,
            "anomaly_summary": {
                "detection_engine": "GSTR2B_RADAR_MISTRAL_FUZZY",
                "total_potential_tax_leak": self.money_str(total_potential_tax_leak),
                "risk_breakdown": risk_breakdown,
                "top_leak_vendors": ranked_vendors[:5],
                "mismatch_rate_pct": round((len(ghost_invoices) / max(len(ledger_rows), 1)) * 100.0, 2),
            },
            "nexus_graph": nexus_graph,
        }
