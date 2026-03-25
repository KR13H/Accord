from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from services.ai_invoice_parser import LocalAIParser


DEFAULT_MESSY_INVOICE = """
TAX INVOICE // CEMENT SUPPLY
Vendor: Shree Durga Cement & Building Material
GSTIN : 27AAKFD9821M1ZQ
Invoice No: CEM/24-25/00987
Date: 18-03-2026

Bill To: Krishna Infra Developers LLP
Site: Tower B, Sector 78

Items:
- OPC 53 Grade Cement (100 bags) x Rs 385 = Rs 38,500
- PPC Cement (40 bags) x Rs 362 = Rs 14,480
Subtotal before tax = Rs 52,980
CGST @ 9% = 4,768.20
SGST @ 9% = 4,768.20
IGST = NIL
Round off = +0.60
Grand TOTAL: Rs 62,517.00
HSN Code : 2523

Payment terms: NEFT in 7 days
Bank: HDFC A/c xxxxxx9912
""".strip()

REQUIRED_KEYS = {
    "vendor_name",
    "gstin",
    "hsn_code",
    "base_amount",
    "cgst",
    "sgst",
    "igst",
    "total",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run local Ollama invoice parsing demo against ai_invoice_parser.py",
    )
    parser.add_argument(
        "--input-file",
        type=str,
        default="",
        help="Optional path to a text file containing OCR invoice text",
    )
    parser.add_argument(
        "--raw-text",
        type=str,
        default="",
        help="Optional direct invoice text override",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the invoice text payload and exit without calling Ollama",
    )
    return parser.parse_args()


def resolve_invoice_text(args: argparse.Namespace) -> str:
    if args.raw_text.strip():
        return args.raw_text.strip()
    if args.input_file.strip():
        return Path(args.input_file.strip()).read_text(encoding="utf-8").strip()
    return DEFAULT_MESSY_INVOICE


def validate_payload(payload: dict) -> None:
    missing = REQUIRED_KEYS.difference(payload.keys())
    if missing:
        raise ValueError(f"Missing required keys in parser output: {sorted(missing)}")


async def run() -> int:
    args = parse_args()
    invoice_text = resolve_invoice_text(args)

    print("-" * 72)
    print("Accord Local AI Parser Demo")
    print("Input length:", len(invoice_text), "characters")
    print("-" * 72)

    if args.dry_run:
        print(invoice_text)
        print("-" * 72)
        print("Dry run complete. No Ollama request executed.")
        return 0

    parser = LocalAIParser()
    try:
        result = await parser.parse_invoice_text(invoice_text)
        validate_payload(result)
    except Exception as exc:  # noqa: BLE001
        print("Parser failed:", str(exc))
        print("Hint: ensure Ollama is running and model llama3 is available.")
        return 1

    print("Extracted JSON payload:")
    print(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True))
    print("-" * 72)
    print("Success: strict GST payload extracted.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
