from __future__ import annotations

import os
import sqlite3
from contextlib import closing
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable

import httpx


LANGUAGE_LABELS = {
    "en": "English",
    "hi": "Hindi",
    "pa": "Punjabi",
}


def _money(value: Any) -> str:
    return f"{Decimal(str(value or '0')).quantize(Decimal('0.01')):.2f}"


class AiLeaseGenerator:
    def __init__(self, *, get_conn: Callable[[], sqlite3.Connection]) -> None:
        self.get_conn = get_conn
        self.ollama_host = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")
        self.model = os.getenv("ACCORD_LEASE_MODEL", "llama3:8b")

    def ensure_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS leases (
                lease_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                property_id TEXT,
                monthly_rent TEXT NOT NULL,
                security_deposit TEXT NOT NULL DEFAULT '0.00',
                term_months INTEGER NOT NULL DEFAULT 11,
                next_billing_date TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'ACTIVE'
            )
            """
        )
        cols = conn.execute("PRAGMA table_info(leases)").fetchall()
        col_names = {str(row["name"]) for row in cols}
        if "property_id" not in col_names:
            conn.execute("ALTER TABLE leases ADD COLUMN property_id TEXT")
        if "security_deposit" not in col_names:
            conn.execute("ALTER TABLE leases ADD COLUMN security_deposit TEXT NOT NULL DEFAULT '0.00'")
        if "term_months" not in col_names:
            conn.execute("ALTER TABLE leases ADD COLUMN term_months INTEGER NOT NULL DEFAULT 11")
        conn.commit()

    def _fetch_lease(self, tenant_id: str, property_id: str) -> dict[str, str]:
        with closing(self.get_conn()) as conn:
            conn.row_factory = sqlite3.Row
            self.ensure_schema(conn)
            row = conn.execute(
                """
                SELECT lease_id, tenant_id, property_id, monthly_rent, security_deposit, term_months
                FROM leases
                WHERE tenant_id = ? AND property_id = ?
                ORDER BY lease_id DESC
                LIMIT 1
                """,
                (tenant_id, property_id),
            ).fetchone()

            if row is None:
                fallback = conn.execute(
                    """
                    SELECT AVG(CAST(amount AS REAL)) AS avg_rent
                    FROM rent_invoices
                    WHERE tenant_id = ?
                    """,
                    (tenant_id,),
                ).fetchone()
                if fallback is None or fallback["avg_rent"] is None:
                    raise ValueError("No lease/rent history found for tenant_id + property_id")
                return {
                    "lease_id": f"LEASE-{tenant_id}-{property_id}",
                    "tenant_id": tenant_id,
                    "property_id": property_id,
                    "monthly_rent": _money(fallback["avg_rent"]),
                    "security_deposit": _money(Decimal(str(fallback["avg_rent"])) * Decimal("2")),
                    "term_months": "11",
                }

            return {
                "lease_id": str(row["lease_id"]),
                "tenant_id": str(row["tenant_id"]),
                "property_id": str(row["property_id"] or property_id),
                "monthly_rent": _money(row["monthly_rent"]),
                "security_deposit": _money(row["security_deposit"]),
                "term_months": str(row["term_months"]),
            }

    def generate(self, *, tenant_id: str, property_id: str, language: str) -> dict[str, str]:
        lang = language.strip().lower()
        if lang not in LANGUAGE_LABELS:
            raise ValueError("language must be one of en, hi, pa")

        lease = self._fetch_lease(tenant_id.strip(), property_id.strip())
        language_name = LANGUAGE_LABELS[lang]

        prompt = (
            "You are a legal drafting assistant for Indian commercial leases.\n"
            "Do not invent unknown facts. Use only provided values.\n"
            "Return only markdown with headings and numbered clauses.\n"
            "Include clauses: Parties, Premises, Rent, Security Deposit, Term, Lock-in, Maintenance,"
            "Utilities, Default, Dispute Resolution, Governing Law (India).\n"
            f"Output language: {language_name}.\n\n"
            "Variables:\n"
            f"- Lease ID: {lease['lease_id']}\n"
            f"- Tenant ID: {lease['tenant_id']}\n"
            f"- Property ID: {lease['property_id']}\n"
            f"- Monthly Rent (INR): {lease['monthly_rent']}\n"
            f"- Security Deposit (INR): {lease['security_deposit']}\n"
            f"- Term (months): {lease['term_months']}\n"
            f"- Draft Date (UTC): {datetime.utcnow().date().isoformat()}\n"
        )

        try:
            with httpx.Client(timeout=60) as client:
                response = client.post(
                    f"{self.ollama_host}/api/generate",
                    json={
                        "model": self.model,
                        "prompt": prompt,
                        "stream": False,
                    },
                )
                response.raise_for_status()
                body = response.json()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"lease generation failed: {exc}") from exc

        markdown = str(body.get("response") or "").strip()
        if not markdown:
            raise RuntimeError("lease generation returned empty content")

        return {
            "tenant_id": lease["tenant_id"],
            "property_id": lease["property_id"],
            "language": lang,
            "markdown": markdown,
        }
