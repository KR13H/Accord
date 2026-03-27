from __future__ import annotations

import json
import threading
import time
import uuid
from datetime import datetime
from datetime import date
from decimal import Decimal
from hashlib import sha256
from typing import Any, Callable

import polars as pl


class StatutoryService:
    """Statutory compliance helpers for GST summaries and filing payload preparation."""

    def __init__(
        self,
        *,
        parse_amount_from_text: Callable[[str | None], Decimal],
        money_str: Callable[[Any], str],
    ) -> None:
        self.parse_amount_from_text = parse_amount_from_text
        self.money_str = money_str
        self._idempotency_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._idempotency_ttl_seconds = 3600.0
        self._idempotency_lock = threading.Lock()

    def _cleanup_idempotency_cache(self, now_ts: float | None = None) -> None:
        now_value = now_ts if now_ts is not None else time.time()
        expired = [key for key, (expires_at, _) in self._idempotency_cache.items() if expires_at <= now_value]
        for key in expired:
            self._idempotency_cache.pop(key, None)

    def clear_idempotency_cache(self) -> int:
        with self._idempotency_lock:
            count = len(self._idempotency_cache)
            self._idempotency_cache.clear()
            return count

    def execute_idempotent_filing(self, *, filing_id: int, idempotency_key: str | None = None) -> dict[str, Any]:
        """Executes filing response generation with 1-hour idempotency semantics.

        When a valid idempotency key repeats within TTL, the previously generated
        response is returned verbatim and no new filing reference is minted.
        """
        safe_filing_id = max(int(filing_id), 0)
        supplied_key = (idempotency_key or "").strip()
        safe_key = supplied_key or f"AUTO:{safe_filing_id}:{int(time.time() // self._idempotency_ttl_seconds)}"

        with self._idempotency_lock:
            now_ts = time.time()
            self._cleanup_idempotency_cache(now_ts)
            cached = self._idempotency_cache.get(safe_key)
            if cached is not None:
                _, payload = cached
                return {
                    **payload,
                    "idempotency_key": safe_key,
                    "idempotency_replayed": True,
                }

            result = {
                "status": "SUCCESS",
                "filing_id": safe_filing_id,
                "ref": str(uuid.uuid4()),
                "processed_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            }
            self._idempotency_cache[safe_key] = (now_ts + self._idempotency_ttl_seconds, result)
            return {
                **result,
                "idempotency_key": safe_key,
                "idempotency_replayed": False,
            }

    def _normalize_row(self, row: dict[str, Any]) -> dict[str, Any]:
        gstin = str(row.get("gstin") or "").strip().upper()
        taxable_value = self.parse_amount_from_text(str(row.get("taxable_value") or "0"))
        igst = self.parse_amount_from_text(str(row.get("igst") or "0"))
        cgst = self.parse_amount_from_text(str(row.get("cgst") or "0"))
        sgst = self.parse_amount_from_text(str(row.get("sgst") or "0"))
        cess = self.parse_amount_from_text(str(row.get("cess") or "0"))
        invoice_value = self.parse_amount_from_text(str(row.get("invoice_value") or "0"))
        if invoice_value <= 0:
            invoice_value = taxable_value + igst + cgst + sgst + cess

        return {
            "reference": str(row.get("reference") or "").strip(),
            "invoice_date": str(row.get("invoice_date") or row.get("date") or "").strip(),
            "gstin": gstin,
            "place_of_supply": str(row.get("place_of_supply") or "").strip().upper(),
            "supply_type": str(row.get("supply_type") or "B2CS").strip().upper(),
            "hsn_code": str(row.get("hsn_code") or "").strip(),
            "uqc": str(row.get("uqc") or "NOS").strip().upper() or "NOS",
            "quantity": float(self.parse_amount_from_text(str(row.get("quantity") or "1"))),
            "taxable_value": float(taxable_value),
            "igst": float(igst),
            "cgst": float(cgst),
            "sgst": float(sgst),
            "cess": float(cess),
            "invoice_value": float(invoice_value),
        }

    def generate_gstr1_json(
        self,
        *,
        ledger_data: list[dict[str, Any]],
        gstin: str,
        period: str | None = None,
    ) -> dict[str, Any]:
        """Aggregates ledger rows into GSTR-1 payload using Polars SIMD operations."""
        normalized_rows = [self._normalize_row(row) for row in ledger_data]
        if not normalized_rows:
            payload = {
                "gstin": gstin,
                "fp": period or datetime.utcnow().strftime("%m%Y"),
                "b2b": [],
                "b2cl": [],
                "b2cs": [],
                "hsn": {"data": []},
                "summary": {
                    "invoice_count": 0,
                    "b2b_count": 0,
                    "b2cl_count": 0,
                    "b2cs_count": 0,
                },
            }
            canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            payload["hardware_fingerprint"] = sha256(canonical.encode("utf-8")).hexdigest()
            return payload

        df = pl.DataFrame(normalized_rows)

        b2b_df = (
            df.filter(pl.col("gstin") != "")
            .group_by(["gstin", "reference", "invoice_date"])
            .agg(
                [
                    pl.col("taxable_value").sum().round(2).alias("taxable_value"),
                    pl.col("igst").sum().round(2).alias("igst"),
                    pl.col("cgst").sum().round(2).alias("cgst"),
                    pl.col("sgst").sum().round(2).alias("sgst"),
                    pl.col("cess").sum().round(2).alias("cess"),
                    pl.col("invoice_value").sum().round(2).alias("invoice_value"),
                ]
            )
            .sort(["gstin", "invoice_date"])
        )

        b2cl_df = (
            df.filter((pl.col("gstin") == "") & (pl.col("invoice_value") >= 100000.0))
            .group_by(["place_of_supply", "reference", "invoice_date"])
            .agg(
                [
                    pl.col("taxable_value").sum().round(2).alias("taxable_value"),
                    pl.col("igst").sum().round(2).alias("igst"),
                    pl.col("invoice_value").sum().round(2).alias("invoice_value"),
                ]
            )
            .sort(["place_of_supply", "invoice_date"])
        )

        b2cs_df = (
            df.filter((pl.col("gstin") == "") & (pl.col("invoice_value") < 100000.0))
            .group_by(["place_of_supply"])
            .agg(
                [
                    pl.col("taxable_value").sum().round(2).alias("taxable_value"),
                    pl.col("igst").sum().round(2).alias("igst"),
                    pl.col("cgst").sum().round(2).alias("cgst"),
                    pl.col("sgst").sum().round(2).alias("sgst"),
                ]
            )
            .sort(["place_of_supply"])
        )

        hsn_df = (
            df.group_by(["hsn_code", "uqc"])
            .agg(
                [
                    pl.col("quantity").sum().round(4).alias("qty"),
                    pl.col("taxable_value").sum().round(2).alias("txval"),
                    pl.col("igst").sum().round(2).alias("iamt"),
                    pl.col("cgst").sum().round(2).alias("camt"),
                    pl.col("sgst").sum().round(2).alias("samt"),
                    pl.col("cess").sum().round(2).alias("csamt"),
                ]
            )
            .sort(["hsn_code"])
        )

        payload = {
            "gstin": gstin,
            "fp": period or datetime.utcnow().strftime("%m%Y"),
            "b2b": b2b_df.to_dicts(),
            "b2cl": b2cl_df.to_dicts(),
            "b2cs": b2cs_df.to_dicts(),
            "hsn": {"data": hsn_df.to_dicts()},
            "summary": {
                "invoice_count": int(df.height),
                "b2b_count": int(b2b_df.height),
                "b2cl_count": int(b2cl_df.height),
                "b2cs_count": int(b2cs_df.height),
            },
        }

        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        payload["hardware_fingerprint"] = sha256(canonical.encode("utf-8")).hexdigest()
        return payload

    def summarize_gstr1_window(self, tax_rows: list[dict[str, Any]], *, from_date: date, to_date: date) -> dict[str, Any]:
        taxable = Decimal("0")
        tax = Decimal("0")
        b2b = 0
        b2cs = 0

        for row in tax_rows:
            taxable += self.parse_amount_from_text(str(row.get("taxable_value") or "0"))
            tax += self.parse_amount_from_text(str(row.get("tax_amount") or "0"))
            supply_type = str(row.get("supply_type") or "").upper()
            if supply_type == "B2B":
                b2b += 1
            elif supply_type == "B2CS":
                b2cs += 1

        return {
            "status": "ok",
            "window": {"from": from_date.isoformat(), "to": to_date.isoformat()},
            "summary": {
                "taxable_value": self.money_str(taxable),
                "tax_amount": self.money_str(tax),
                "b2b_count": b2b,
                "b2cs_count": b2cs,
            },
        }
