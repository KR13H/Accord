from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal
from typing import Any


def _parse_gst_rate(category: str) -> Decimal | None:
    text = category or ""
    patterns = [
        r"gst\s*[@:-]?\s*(\d+(?:\.\d+)?)",
        r"(\d+(?:\.\d+)?)\s*%\s*gst",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            try:
                return Decimal(match.group(1))
            except Exception:  # noqa: BLE001
                return None
    return None


def _to_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001
        return Decimal("0")


def generate_gstr1_payload(
    transactions: list[dict[str, Any]],
    *,
    business_id: str,
    gstin: str,
    period_yyyymm: str,
) -> dict[str, Any]:
    b2b: list[dict[str, Any]] = []
    b2cs: list[dict[str, Any]] = []
    hsn_map: dict[str, dict[str, Any]] = {}

    for tx in transactions:
        if str(tx.get("type", "")).upper() != "INCOME":
            continue

        category = str(tx.get("category", "General"))
        gst_rate = _parse_gst_rate(category)
        if gst_rate is None:
            continue

        gross_amount = _to_decimal(tx.get("amount"))
        taxable_value = (gross_amount * Decimal("100") / (Decimal("100") + gst_rate)).quantize(Decimal("0.01"))
        tax_amount = (gross_amount - taxable_value).quantize(Decimal("0.01"))

        hsn = "9999"
        hsn_bucket = hsn_map.setdefault(
            hsn,
            {
                "num": 1,
                "hsn_sc": hsn,
                "desc": category,
                "uqc": "NOS",
                "qty": 0,
                "val": Decimal("0.00"),
                "txval": Decimal("0.00"),
                "iamt": Decimal("0.00"),
                "camt": Decimal("0.00"),
                "samt": Decimal("0.00"),
                "csamt": Decimal("0.00"),
            },
        )
        hsn_bucket["qty"] += 1
        hsn_bucket["val"] += gross_amount
        hsn_bucket["txval"] += taxable_value
        hsn_bucket["camt"] += tax_amount / Decimal("2")
        hsn_bucket["samt"] += tax_amount / Decimal("2")

        invoice = {
            "inum": str(tx.get("id", "0")),
            "idt": str(tx.get("created_at", ""))[:10],
            "val": float(gross_amount),
            "pos": "27",
            "rchrg": "N",
            "inv_typ": "R",
            "itms": [
                {
                    "num": 1,
                    "itm_det": {
                        "txval": float(taxable_value),
                        "rt": float(gst_rate),
                        "iamt": 0.0,
                        "camt": float((tax_amount / Decimal("2")).quantize(Decimal("0.01"))),
                        "samt": float((tax_amount / Decimal("2")).quantize(Decimal("0.01"))),
                        "csamt": 0.0,
                    },
                }
            ],
        }

        # Use simple heuristic: larger invoices treated as B2B.
        if gross_amount >= Decimal("2500"):
            b2b.append(
                {
                    "ctin": "27ABCDE1234F1Z5",
                    "inv": [invoice],
                }
            )
        else:
            b2cs.append(
                {
                    "sply_ty": "INTRA",
                    "typ": "OE",
                    "pos": "27",
                    "rt": float(gst_rate),
                    "txval": float(taxable_value),
                    "iamt": 0.0,
                    "camt": float((tax_amount / Decimal("2")).quantize(Decimal("0.01"))),
                    "samt": float((tax_amount / Decimal("2")).quantize(Decimal("0.01"))),
                    "csamt": 0.0,
                }
            )

    hsn_data = []
    for bucket in hsn_map.values():
        hsn_data.append(
            {
                "num": bucket["num"],
                "hsn_sc": bucket["hsn_sc"],
                "desc": bucket["desc"],
                "uqc": bucket["uqc"],
                "qty": bucket["qty"],
                "val": float(bucket["val"].quantize(Decimal("0.01"))),
                "txval": float(bucket["txval"].quantize(Decimal("0.01"))),
                "iamt": float(bucket["iamt"].quantize(Decimal("0.01"))),
                "camt": float(bucket["camt"].quantize(Decimal("0.01"))),
                "samt": float(bucket["samt"].quantize(Decimal("0.01"))),
                "csamt": float(bucket["csamt"].quantize(Decimal("0.01"))),
            }
        )

    return {
        "gstin": gstin,
        "fp": period_yyyymm,
        "gt": float(sum((float(tx.get("amount", 0) or 0) for tx in transactions), 0.0)),
        "cur_gt": float(sum((float(tx.get("amount", 0) or 0) for tx in transactions), 0.0)),
        "b2b": b2b,
        "b2cs": b2cs,
        "hsn": {"data": hsn_data},
        "doc_issue": {
            "doc_det": [
                {
                    "doc_num": 1,
                    "docs": [
                        {
                            "num": 1,
                            "from": 1,
                            "to": len(transactions),
                            "totnum": len(transactions),
                            "cancel": 0,
                            "net_issue": len(transactions),
                        }
                    ],
                }
            ]
        },
        "meta": {
            "business_id": business_id,
            "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "note": "Generated for local filing workflow. Validate before portal upload.",
        },
    }
