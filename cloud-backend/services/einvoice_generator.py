from __future__ import annotations

import os
import sqlite3
from contextlib import closing
from datetime import datetime
from decimal import Decimal
from typing import Any, Callable


def _money(value: Any) -> Decimal:
    return Decimal(str(value or "0")).quantize(Decimal("0.01"))


class EInvoiceGenerator:
    def __init__(self, *, get_conn: Callable[[], sqlite3.Connection]) -> None:
        self.get_conn = get_conn

    def _seller_details(self) -> dict[str, str]:
        return {
            "Gstin": os.getenv("ACCORD_SELLER_GSTIN", "29ABCDE1234F1Z5"),
            "LglNm": os.getenv("ACCORD_SELLER_LEGAL_NAME", "Accord Realty Technologies Pvt Ltd"),
            "TrdNm": os.getenv("ACCORD_SELLER_TRADE_NAME", "Accord Realty"),
            "Addr1": os.getenv("ACCORD_SELLER_ADDR1", "12 MG Road"),
            "Loc": os.getenv("ACCORD_SELLER_CITY", "Bengaluru"),
            "Pin": os.getenv("ACCORD_SELLER_PIN", "560001"),
            "Stcd": os.getenv("ACCORD_SELLER_STATE_CODE", "29"),
        }

    def _resolve_invoice_source(self, conn: sqlite3.Connection, invoice_id: str) -> dict[str, Any]:
        rent_row = None
        if invoice_id.isdigit():
            rent_row = conn.execute(
                """
                SELECT id, lease_id, tenant_id, invoice_date, due_date, amount, status
                FROM rent_invoices
                WHERE id = ?
                """,
                (int(invoice_id),),
            ).fetchone()

        if rent_row is not None:
            return {
                "kind": "rent",
                "invoice_number": f"RINV-{rent_row['id']}",
                "invoice_date": str(rent_row["invoice_date"]),
                "buyer_code": str(rent_row["tenant_id"]),
                "buyer_name": f"Tenant {rent_row['tenant_id']}",
                "description": f"Lease rental for lease {rent_row['lease_id']}",
                "amount": _money(rent_row["amount"]),
                "hsn": os.getenv("ACCORD_RENT_HSN", "997212"),
            }

        booking_row = conn.execute(
            """
            SELECT booking_id, customer_name, total_consideration, booking_date, unit_code
            FROM sales_bookings
            WHERE booking_id = ?
            """,
            (invoice_id,),
        ).fetchone()
        if booking_row is None:
            raise ValueError("invoice_id not found in rent_invoices or sales_bookings")

        return {
            "kind": "sale",
            "invoice_number": f"SINV-{booking_row['booking_id']}",
            "invoice_date": str(booking_row["booking_date"] or datetime.utcnow().date().isoformat()),
            "buyer_code": str(booking_row["booking_id"]),
            "buyer_name": str(booking_row["customer_name"] or f"Customer {booking_row['booking_id']}"),
            "description": f"Property booking for unit {booking_row['unit_code']}",
            "amount": _money(booking_row["total_consideration"]),
            "hsn": os.getenv("ACCORD_PROPERTY_HSN", "995411"),
        }

    def generate_payload(self, invoice_id: str) -> dict[str, Any]:
        with closing(self.get_conn()) as conn:
            conn.row_factory = sqlite3.Row
            source = self._resolve_invoice_source(conn, invoice_id.strip())

        tax_rate = Decimal(os.getenv("ACCORD_DEFAULT_GST_RATE", "18.0"))
        taxable = source["amount"]
        tax_amount = (taxable * tax_rate / Decimal("100")).quantize(Decimal("0.01"))
        total = (taxable + tax_amount).quantize(Decimal("0.01"))

        payload = {
            "Version": "1.1",
            "TranDtls": {
                "TaxSch": "GST",
                "SupTyp": "B2B",
            },
            "DocDtls": {
                "Typ": "INV",
                "No": source["invoice_number"],
                "Dt": datetime.fromisoformat(source["invoice_date"]).strftime("%d/%m/%Y"),
            },
            "SellerDtls": self._seller_details(),
            "BuyerDtls": {
                "Gstin": os.getenv("ACCORD_DEFAULT_BUYER_GSTIN", "URP"),
                "LglNm": source["buyer_name"],
                "Pos": os.getenv("ACCORD_DEFAULT_BUYER_POS", "29"),
                "Addr1": "N/A",
                "Loc": "India",
                "Pin": "000000",
                "Stcd": os.getenv("ACCORD_DEFAULT_BUYER_STATE", "29"),
            },
            "ItemList": [
                {
                    "SlNo": "1",
                    "PrdDesc": source["description"],
                    "IsServc": "Y" if source["kind"] == "rent" else "N",
                    "HsnCd": source["hsn"],
                    "Qty": 1,
                    "Unit": "NOS",
                    "UnitPrice": float(taxable),
                    "TotAmt": float(taxable),
                    "AssAmt": float(taxable),
                    "GstRt": float(tax_rate),
                    "IgstAmt": float(tax_amount),
                    "TotItemVal": float(total),
                }
            ],
            "ValDtls": {
                "AssVal": float(taxable),
                "IgstVal": float(tax_amount),
                "TotInvVal": float(total),
            },
        }
        return payload
