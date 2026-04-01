from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = CURRENT_DIR.parent
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from services.postgres_backup_service import run_pg_backup


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create encrypted PostgreSQL backup and upload to offsite storage")
    parser.add_argument("--dry-run", action="store_true", help="Validate flow without running pg_dump/upload")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_pg_backup(dry_run=bool(args.dry_run))
    print(json.dumps(result, indent=2))
    return 0 if result.get("status") in {"ok", "skipped"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
