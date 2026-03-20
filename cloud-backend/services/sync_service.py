from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


@dataclass
class SyncIssue:
    code: str
    message: str
    severity: str


class SyncService:
    """Tally XML sync service with export/import helper methods."""

    def __init__(
        self,
        *,
        money_str: Callable[[Any], str],
    ) -> None:
        self.money_str = money_str

    def parse_tally_vouchers(self, xml_bytes: bytes) -> dict[str, Any]:
        root = ET.fromstring(xml_bytes)
        vouchers = root.findall(".//VOUCHER")
        payload: list[dict[str, Any]] = []
        issues: list[dict[str, Any]] = []

        for idx, voucher in enumerate(vouchers, start=1):
            vno = str(voucher.findtext("VOUCHERNUMBER") or "").strip()
            vdate = str(voucher.findtext("DATE") or "").strip()
            narration = str(voucher.findtext("NARRATION") or "").strip()
            lines = voucher.findall("ALLLEDGERENTRIES.LIST")
            if not vno:
                issues.append({"code": "MISSING_VNO", "message": f"Voucher #{idx} missing VOUCHERNUMBER", "severity": "HIGH"})
            if not lines:
                issues.append({"code": "MISSING_LINES", "message": f"Voucher {vno or idx} has no lines", "severity": "CRITICAL"})

            parsed_lines = []
            for line in lines:
                parsed_lines.append(
                    {
                        "ledger_name": str(line.findtext("LEDGERNAME") or "").strip(),
                        "amount": str(line.findtext("AMOUNT") or "0").strip(),
                        "is_deemed_positive": str(line.findtext("ISDEEMEDPOSITIVE") or "No").strip(),
                    }
                )

            payload.append(
                {
                    "voucher_number": vno,
                    "voucher_date": vdate,
                    "narration": narration,
                    "line_count": len(parsed_lines),
                    "lines": parsed_lines,
                }
            )

        return {
            "status": "ok",
            "vouchers": payload,
            "issues": issues,
            "issue_count": len(issues),
        }

    def reverse_sync_from_file(self, xml_path: Path) -> dict[str, Any]:
        data = xml_path.read_bytes()
        parsed = self.parse_tally_vouchers(data)
        return {
            "status": "ok",
            "source": str(xml_path),
            "vouchers_ingested": len(parsed["vouchers"]),
            "issues": parsed["issues"],
            "stark_neon_overlay_ready": True,
        }


class PostgresSyncService:
    """Bridge primitive for sqlite to PostgreSQL data copy orchestration."""

    def __init__(self, *, sqlite_path: Path, database_url: str) -> None:
        self.sqlite_path = sqlite_path
        self.database_url = database_url

    def migration_plan(self) -> dict[str, Any]:
        cloud_enabled = self.database_url.startswith("postgresql://")
        return {
            "status": "ready" if cloud_enabled else "skipped",
            "sqlite_path": str(self.sqlite_path),
            "database_url": self.database_url,
            "steps": [
                "snapshot sqlite ledger",
                "create postgres schema",
                "bulk copy accounts/journal/tax tables",
                "rebuild indexes and constraints",
                "verify row counts and hash parity",
            ],
        }
