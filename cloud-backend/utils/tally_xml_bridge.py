from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping
import xml.etree.ElementTree as ET


def _parse_date(raw: str) -> str:
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:  # noqa: BLE001
        return datetime.utcnow().strftime("%Y%m%d")
    return parsed.strftime("%Y%m%d")


def _safe_amount(value: Any) -> Decimal:
    amount = Decimal(str(value))
    if amount < 0:
        return amount.copy_abs()
    return amount


def _append_ledger_line(voucher: ET.Element, ledger_name: str, amount: str, deemed_positive: bool) -> None:
    line = ET.SubElement(voucher, "ALLLEDGERENTRIES.LIST")
    ET.SubElement(line, "LEDGERNAME").text = ledger_name
    ET.SubElement(line, "ISDEEMEDPOSITIVE").text = "Yes" if deemed_positive else "No"
    ET.SubElement(line, "AMOUNT").text = amount


def generate_tally_xml(transactions: list[Mapping[str, Any]]) -> str:
    envelope = ET.Element("ENVELOPE")
    header = ET.SubElement(envelope, "HEADER")
    ET.SubElement(header, "TALLYREQUEST").text = "Import Data"

    body = ET.SubElement(envelope, "BODY")
    import_data = ET.SubElement(body, "IMPORTDATA")
    request_desc = ET.SubElement(import_data, "REQUESTDESC")
    ET.SubElement(request_desc, "REPORTNAME").text = "Vouchers"
    request_data = ET.SubElement(import_data, "REQUESTDATA")

    for tx in transactions:
        tx_type = str(tx.get("type") or "").upper()
        voucher_type = "Receipt" if tx_type == "INCOME" else "Payment"
        amount = _safe_amount(tx.get("amount") or "0")
        amount_str = f"{amount:.2f}"
        counter_amount = f"-{amount:.2f}"
        created_at = str(tx.get("created_at") or "")
        voucher_date = _parse_date(created_at)
        payment_method = str(tx.get("payment_method") or "Cash").title()
        category = str(tx.get("category") or "General")
        voucher_number = f"SME-{tx.get('id', '0')}"

        tally_message = ET.SubElement(request_data, "TALLYMESSAGE")
        voucher = ET.SubElement(
            tally_message,
            "VOUCHER",
            {
                "VCHTYPE": voucher_type,
                "ACTION": "Create",
            },
        )
        ET.SubElement(voucher, "DATE").text = voucher_date
        ET.SubElement(voucher, "VOUCHERTYPENAME").text = voucher_type
        ET.SubElement(voucher, "VOUCHERNUMBER").text = voucher_number
        ET.SubElement(voucher, "NARRATION").text = f"{category} via {payment_method}"

        if tx_type == "INCOME":
            _append_ledger_line(voucher, payment_method, amount_str, deemed_positive=False)
            _append_ledger_line(voucher, category, counter_amount, deemed_positive=True)
        else:
            _append_ledger_line(voucher, category, amount_str, deemed_positive=False)
            _append_ledger_line(voucher, payment_method, counter_amount, deemed_positive=True)

    xml_payload = ET.tostring(envelope, encoding="utf-8", xml_declaration=True)
    return xml_payload.decode("utf-8")
