from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from sqlalchemy import MetaData, Table, create_engine, select, text
from sqlalchemy.exc import SQLAlchemyError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate Accord SQLite data into PostgreSQL")
    parser.add_argument(
        "--sqlite-path",
        default=str(Path(__file__).with_name("ledger.db")),
        help="Path to source SQLite database file",
    )
    parser.add_argument(
        "--postgres-url",
        default=os.getenv("DATABASE_URL", "postgresql://accord:accord@localhost:5432/accord"),
        help="Target PostgreSQL URL",
    )
    parser.add_argument(
        "--truncate-target",
        action="store_true",
        help="Truncate target PostgreSQL tables before inserting",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Batch size for inserts",
    )
    return parser.parse_args()


def normalize_postgres_url(url: str) -> str:
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql://") and not url.startswith("postgresql+psycopg://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


def truncate_tables(target_conn, metadata: MetaData) -> None:
    table_names = [table.name for table in metadata.sorted_tables]
    if not table_names:
        return
    quoted = ", ".join(f'"{name}"' for name in table_names)
    target_conn.execute(text(f"TRUNCATE TABLE {quoted} RESTART IDENTITY CASCADE"))


def iter_chunks(items: list[dict[str, Any]], size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def reset_sequences(target_conn, metadata: MetaData) -> None:
    for table in metadata.sorted_tables:
        pk_cols = [col for col in table.columns if col.primary_key]
        if len(pk_cols) != 1:
            continue
        pk = pk_cols[0]
        if str(pk.type).upper() not in {"INTEGER", "BIGINT", "SMALLINT"}:
            continue

        table_name = table.name
        col_name = pk.name
        target_conn.execute(
            text(
                f"""
                SELECT setval(
                    pg_get_serial_sequence('"{table_name}"', '{col_name}'),
                    COALESCE(MAX("{col_name}"), 1),
                    MAX("{col_name}") IS NOT NULL
                )
                FROM "{table_name}"
                """
            )
        )


def migrate(sqlite_path: str, postgres_url: str, truncate_target: bool, chunk_size: int) -> None:
    source_path = Path(sqlite_path)
    if not source_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {source_path}")

    postgres_url = normalize_postgres_url(postgres_url)
    source_engine = create_engine(f"sqlite:///{source_path}", future=True)
    target_engine = create_engine(postgres_url, future=True)

    source_meta = MetaData()
    source_meta.reflect(bind=source_engine)

    if not source_meta.tables:
        print("No tables discovered in SQLite source. Nothing to migrate.")
        return

    # Create schema on target using reflected metadata.
    source_meta.create_all(target_engine, checkfirst=True)

    table_report: list[dict[str, Any]] = []
    with source_engine.connect() as source_conn, target_engine.begin() as target_conn:
        if truncate_target:
            truncate_tables(target_conn, source_meta)

        for table in source_meta.sorted_tables:
            src_table = Table(table.name, source_meta, autoload_with=source_engine)
            rows = list(source_conn.execute(select(src_table)).mappings())
            payload = [dict(row) for row in rows]

            inserted = 0
            if payload:
                for chunk in iter_chunks(payload, chunk_size):
                    target_conn.execute(table.insert(), chunk)
                    inserted += len(chunk)

            table_report.append({"table": table.name, "rows_inserted": inserted})
            print(f"Migrated {inserted:>6} rows -> {table.name}")

        reset_sequences(target_conn, source_meta)

    print("\nMigration complete.")
    print(json.dumps({"tables": table_report}, indent=2))


def main() -> int:
    args = parse_args()
    try:
        migrate(
            sqlite_path=args.sqlite_path,
            postgres_url=args.postgres_url,
            truncate_target=args.truncate_target,
            chunk_size=max(args.chunk_size, 100),
        )
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}")
        return 1
    except SQLAlchemyError as exc:
        print(f"ERROR: SQLAlchemy migration failure: {exc}")
        return 2
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: unexpected failure: {exc}")
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
