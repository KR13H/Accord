from __future__ import annotations

from contextlib import closing
from decimal import Decimal
from typing import Any, Callable


GOLDEN_SIX_VOUCHERS = {"CONTRA", "PAYMENT", "RECEIPT", "JOURNAL", "SALES", "PURCHASE"}
MONEY_QUANT = Decimal("0.0001")


class AccountingService:
    """Handles ledger posting and accounting export orchestration.

    This service encapsulates double-entry operations and downstream export hooks.
    """

    def __init__(
        self,
        *,
        get_conn: Callable[[], Any],
        post_ledger_entry_from_extract: Callable[..., dict[str, Any]],
        generate_tally_export: Callable[..., tuple[bytes, str, Any, str, str]],
        run_ollama_generate: Callable[..., Any] | None = None,
        recon_model: str = "llama3.2",
    ) -> None:
        """Initializes accounting service dependencies.

        Args:
            get_conn: Connection factory for the ledger storage engine.
            post_ledger_entry_from_extract: Existing posting primitive for extracted vouchers.
            generate_tally_export: Existing Tally XML export generator.

        Hardware Impact:
            Mostly I/O bound around DB and filesystem operations.
        Logic Invariants:
            Ledger posting remains double-entry balanced through existing primitives.
        Legal Context:
            Preserves accounting immutability expectations for audit-ready books.
        """
        self.get_conn = get_conn
        self.post_ledger_entry_from_extract = post_ledger_entry_from_extract
        self.generate_tally_export = generate_tally_export
        self.run_ollama_generate = run_ollama_generate
        self.recon_model = recon_model

    def _money_4(self, value: Any) -> Decimal:
        """Normalizes values to four-decimal precision for deterministic comparisons."""
        return Decimal(str(value or "0")).quantize(MONEY_QUANT)

    def _infer_voucher_type_from_lines(self, conn: Any, entry_id: int) -> str:
        """Infers Golden-Six voucher type from journal lines and account classes.

        Args:
            conn: Active DB connection.
            entry_id: Journal entry identifier.

        Returns:
            One of CONTRA, PAYMENT, RECEIPT, JOURNAL, SALES, PURCHASE.

        Hardware Impact:
            Lightweight SQL reads with negligible CPU overhead.
        Logic Invariants:
            Always returns a valid Golden-Six voucher type.
        Legal Context:
            Enforces deterministic accounting classification for audit consistency.
        """
        rows = conn.execute(
            """
            SELECT a.name, a.type, jl.debit, jl.credit
            FROM journal_lines jl
            JOIN accounts a ON a.id = jl.account_id
            WHERE jl.entry_id = ?
            """,
            (entry_id,),
        ).fetchall()
        if not rows:
            return "JOURNAL"

        debit_names = {str(r["name"]) for r in rows if self._money_4(r["debit"]) > 0}
        credit_names = {str(r["name"]) for r in rows if self._money_4(r["credit"]) > 0}

        bank_cash = {"Cash", "Bank"}
        has_bank_cash_debit = len(debit_names.intersection(bank_cash)) > 0
        has_bank_cash_credit = len(credit_names.intersection(bank_cash)) > 0

        if has_bank_cash_debit and has_bank_cash_credit:
            return "CONTRA"
        if has_bank_cash_credit:
            return "PAYMENT"
        if has_bank_cash_debit:
            return "RECEIPT"
        if "Sales Revenue" in credit_names:
            return "SALES"
        if "Purchases" in debit_names:
            return "PURCHASE"
        return "JOURNAL"

    async def _ai_refine_voucher_type(self, *, summary: str, fallback_type: str) -> str:
        """Uses Llama for optional voucher type refinement.

        Args:
            summary: Lightweight ledger movement summary.
            fallback_type: Deterministic voucher type fallback.

        Returns:
            Refined or fallback Golden-Six voucher type.

        Hardware Impact:
            Optional local LLM call using Apple silicon acceleration.
        Logic Invariants:
            Never returns values outside Golden-Six set.
        Legal Context:
            Maintains explainable categorization with deterministic fallback.
        """
        allowed = GOLDEN_SIX_VOUCHERS
        if self.run_ollama_generate is None:
            return fallback_type
        prompt = (
            "Classify accounting voucher into exactly one token from: "
            "CONTRA, PAYMENT, RECEIPT, JOURNAL, SALES, PURCHASE. "
            f"Ledger summary: {summary}. Return token only."
        )
        try:
            raw = await self.run_ollama_generate(model=self.recon_model, prompt=prompt)
            token = str(raw).strip().upper()
            for candidate in allowed:
                if candidate in token:
                    return candidate
        except Exception:
            return fallback_type
        return fallback_type

    def classify_voucher_type(self, conn: Any, entry_id: int) -> str:
        """Public strict voucher classifier for non-async call sites.

        Returns one of the Golden Six voucher types.
        """
        resolved = self._infer_voucher_type_from_lines(conn, entry_id)
        if resolved not in GOLDEN_SIX_VOUCHERS:
            return "JOURNAL"
        return resolved

    async def post_extracted_entry(
        self,
        *,
        extracted: dict[str, Any],
        fallback_text: str,
        description_prefix: str,
        actor_role: str,
        admin_id: int,
        source_file_path: Any,
        model_response: str,
        import_status: str,
    ) -> dict[str, Any]:
        """Posts one extracted document into the ledger.

        Args:
            extracted: Structured voucher fields from OCR/LLM pipeline.
            fallback_text: Raw OCR fallback payload.
            description_prefix: Prefix for generated ledger description.
            actor_role: Authenticated role invoking the operation.
            admin_id: Acting admin identity.
            source_file_path: Source evidence file path.
            model_response: Raw model output for traceability.
            import_status: Pipeline status marker.

        Returns:
            Posted ledger entry metadata.

        Hardware Impact:
            Executes one transaction and associated index updates.
        Logic Invariants:
            Uses existing posting primitive that stamps fingerprint continuity.
        Legal Context:
            Maintains source-to-ledger traceability for statutory audits.
        """
        with closing(self.get_conn()) as conn:
            entry = self.post_ledger_entry_from_extract(
                conn=conn,
                extracted=extracted,
                fallback_text=fallback_text,
                description_prefix=description_prefix,
                actor_role=actor_role,
                admin_id=admin_id,
                source_file_path=source_file_path,
                model_response=model_response,
                import_status=import_status,
            )

            entry_id = int(entry.get("entry_id") or 0)
            if entry_id > 0:
                fallback_type = self._infer_voucher_type_from_lines(conn, entry_id)
                summary = (
                    f"description_prefix={description_prefix}; "
                    f"vendor={str(extracted.get('vendor', ''))}; "
                    f"amount={str(extracted.get('total_amount', '0'))}; "
                    f"status={import_status}"
                )
                voucher_type = await self._ai_refine_voucher_type(summary=summary, fallback_type=fallback_type)
                conn.execute("UPDATE journal_entries SET voucher_type = ? WHERE id = ?", (voucher_type, entry_id))
                entry["voucher_type"] = voucher_type

            conn.commit()
            return entry

    def export_tally(self, *, entry_id: int) -> tuple[bytes, str, Any, str, str]:
        """Exports a journal entry into Tally XML format.

        Args:
            entry_id: Journal entry identifier.

        Returns:
            Binary XML payload, filename, path, reference, and status.

        Hardware Impact:
            XML generation workload is CPU-light and disk-write bound.
        Logic Invariants:
            Export payload is derived from immutable journal state.
        Legal Context:
            Supports compliant filing interoperability with Tally workflows.
        """
        with closing(self.get_conn()) as conn:
            payload = self.generate_tally_export(conn, entry_id)
            conn.commit()
            return payload
