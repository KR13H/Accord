from __future__ import annotations

import json
import re
from datetime import date
from typing import Any, Awaitable, Callable


class VoiceService:
    """Transforms plain-language commands into structured ledger intents."""

    def __init__(
        self,
        *,
        mistral_generate: Callable[..., Awaitable[str]],
        model: str,
    ) -> None:
        self.mistral_generate = mistral_generate
        self.model = model

    def _heuristic_fallback(self, transcript: str) -> dict[str, Any]:
        text = transcript.strip()
        lowered = text.lower()

        amount_match = re.search(r"(\d[\d,]*(?:\.\d{1,4})?)", text)
        amount = amount_match.group(1).replace(",", "") if amount_match else "0"

        vendor = "Voice Counterparty"
        vendor_match = re.search(r"(?:to|from)\s+([A-Za-z][A-Za-z0-9 .&-]{1,80})", text, flags=re.IGNORECASE)
        if vendor_match:
            vendor = vendor_match.group(1).strip()

        is_sale = any(token in lowered for token in ["sold", "sale", "received payment", "invoice"])
        is_credit = "credit" in lowered or "udhar" in lowered

        account_dr = "Cash"
        account_cr = "Sales Revenue"
        if is_sale and is_credit:
            account_dr = "Accounts Receivable"
            account_cr = "Sales Revenue"
        elif is_sale and not is_credit:
            account_dr = "Cash"
            account_cr = "Sales Revenue"
        elif any(token in lowered for token in ["purchase", "bought", "procured"]):
            account_dr = "Purchases"
            account_cr = "Accounts Payable" if is_credit else "Cash"

        currency_code = "INR"
        if any(token in lowered for token in ["usd", "dollar", "$", "new york"]):
            currency_code = "USD"
        elif any(token in lowered for token in ["aed", "dirham", "dubai"]):
            currency_code = "AED"
        elif any(token in lowered for token in ["gbp", "pound", "london"]):
            currency_code = "GBP"
        elif any(token in lowered for token in ["eur", "euro"]):
            currency_code = "EUR"

        return {
            "date": date.today().isoformat(),
            "description": text[:180],
            "vendor": vendor,
            "gstin": "",
            "account_dr": account_dr,
            "account_cr": account_cr,
            "amount": amount,
            "currency_code": currency_code,
            "exchange_rate": "",
            "voucher_type": "JOURNAL",
            "confidence": "heuristic-fallback",
        }

    async def parse_command_to_ledger(self, transcript: str) -> dict[str, Any]:
        """Uses local Mistral/Ollama to extract a double-entry intent from transcript."""
        text = transcript.strip()
        if not text:
            return self._heuristic_fallback(transcript)

        prompt = (
            "Transform the following accounting voice command into strict JSON only. "
            "Return keys: date, description, vendor, gstin, account_dr, account_cr, amount, currency_code, exchange_rate, voucher_type, confidence. "
            "Use account names only from this list when possible: Cash, Bank, Sales Revenue, Purchases, "
            "Operating Expenses, Accounts Receivable, Accounts Payable. "
            "Amount must be a numeric string (up to 4 decimals). "
            "currency_code must be INR/USD/AED/GBP/EUR when inferable. "
            "Voucher type should be one of: JOURNAL, SALES, PURCHASE, RECEIPT, PAYMENT. "
            f"Command: {text}"
        )

        try:
            raw = await self.mistral_generate(model=self.model, prompt=prompt)
            candidate = json.loads(raw)
            required = {"account_dr", "account_cr", "amount"}
            if not required.issubset(set(candidate.keys())):
                return self._heuristic_fallback(transcript)
            return {
                "date": str(candidate.get("date") or date.today().isoformat()),
                "description": str(candidate.get("description") or text[:180]),
                "vendor": str(candidate.get("vendor") or "Voice Counterparty"),
                "gstin": str(candidate.get("gstin") or "").upper(),
                "account_dr": str(candidate.get("account_dr") or "Cash"),
                "account_cr": str(candidate.get("account_cr") or "Sales Revenue"),
                "amount": str(candidate.get("amount") or "0"),
                "currency_code": str(candidate.get("currency_code") or "INR").upper(),
                "exchange_rate": str(candidate.get("exchange_rate") or ""),
                "voucher_type": str(candidate.get("voucher_type") or "JOURNAL").upper(),
                "confidence": str(candidate.get("confidence") or "mistral"),
            }
        except Exception:
            return self._heuristic_fallback(transcript)
