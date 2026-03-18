from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from sqlalchemy import MetaData, create_engine, select


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify SQLite -> PostgreSQL migration consistency")
    parser.add_argument(
        "--sqlite-path",
        default=str(Path(__file__).with_name("ledger.db")),
        help="Path to source SQLite DB",
    )
    parser.add_argument(
        "--postgres-url",
        default=os.getenv("DATABASE_URL", "postgresql://accord:accord@localhost:5432/accord"),
        help="Target PostgreSQL URL",
    )
    parser.add_argument(
        "--sample-only",
        action="store_true",
        help="Verify only critical compliance tables",
    )
    return parser.parse_args()


def normalize_postgres_url(url: str) -> str:
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://") and not url.startswith("postgresql+psycopg://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


def row_hash(rows: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(row, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8"))
    return digest.hexdigest()


def fetch_rows(engine, table, order_cols: list[str]) -> list[dict[str, Any]]:
    stmt = select(table)
    if order_cols:
        stmt = stmt.order_by(*[table.c[name] for name in order_cols])
    with engine.connect() as conn:
        result = conn.execute(stmt).mappings().all()
    return [dict(row) for row in result]


def verify(sqlite_path: str, postgres_url: str, sample_only: bool) -> int:
    source_path = Path(sqlite_path)
    if not source_path.exists():
        print(f"ERROR: source sqlite db missing: {source_path}")
        return 1

    postgres_url = normalize_postgres_url(postgres_url)
    src_engine = create_engine(f"sqlite:///{source_path}", future=True)
    dst_engine = create_engine(postgres_url, future=True)

    src_meta = MetaData()
    src_meta.reflect(bind=src_engine)

    dst_meta = MetaData()
    dst_meta.reflect(bind=dst_engine)

    critical_tables = {
        "journal_entries",
        "journal_lines",
        "tax_ledger",
        "audit_edit_logs",
        "export_history",
        "vendor_trust_scores",
        "safe_harbor_attestations",
        "hsn_master",
        "financial_periods",
    }

    table_names = sorted(set(src_meta.tables).intersection(dst_meta.tables))
    if sample_only:
        table_names = [name for name in table_names if name in critical_tables]

    mismatches: list[dict[str, Any]] = []

    for name in table_names:
        src_table = src_meta.tables[name]
        dst_table = dst_meta.tables[name]

        src_pk = [col.name for col in src_table.primary_key.columns]
        dst_pk = [col.name for col in dst_table.primary_key.columns]
        order_cols = src_pk if src_pk and src_pk == dst_pk else [col.name for col in src_table.columns]

        src_rows = fetch_rows(src_engine, src_table, order_cols)
        dst_rows = fetch_rows(dst_engine, dst_table, order_cols)

        src_count = len(src_rows)
        dst_count = len(dst_rows)
        src_hash = row_hash(src_rows)
        dst_hash = row_hash(dst_rows)

        ok = src_count == dst_count and src_hash == dst_hash
        print(
            f"{name:<30} src={src_count:<8} dst={dst_count:<8} "
            f"hash={'MATCH' if src_hash == dst_hash else 'MISMATCH'}"
        )

        if not ok:
            mismatches.append(
                {
                    "table": name,
                    "src_count": src_count,
                    "dst_count": dst_count,
                    "src_hash": src_hash,
                    "dst_hash": dst_hash,
                }
            )

    if mismatches:
        print("\nMIGRATION VERIFICATION FAILED")
        print(json.dumps({"mismatches": mismatches}, indent=2))
        return 2

    print("\nMIGRATION VERIFIED: counts and row-hashes match")
    return 0


def main() -> int:
    args = parse_args()
    return verify(args.sqlite_path, args.postgres_url, args.sample_only)


if __name__ == "__main__":
    raise SystemExit(main())
