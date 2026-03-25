from __future__ import annotations

import argparse
import os
import sqlite3
from datetime import datetime
from pathlib import Path


DEFAULT_DB_PATH = Path(__file__).with_name("ledger.db")
DEFAULT_DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DEFAULT_DB_PATH}")


def _normalize_database_url(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return f"sqlite:///{DEFAULT_DB_PATH}"
    if value.startswith("postgres://"):
        return value.replace("postgres://", "postgresql://", 1)
    return value


def _resolve_sqlite_path(database_url: str) -> Path:
    path_raw = database_url.replace("sqlite:///", "", 1)
    path = Path(path_raw)
    if not path.is_absolute():
        path = Path(__file__).with_name(path_raw)
    return path


def _ensure_platform_admins_table_sqlite(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS platform_admins (
            email TEXT PRIMARY KEY,
            admin_id INTEGER NOT NULL UNIQUE,
            display_name TEXT NOT NULL,
            role TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )


def _upsert_admin_sqlite(*, database_url: str, email: str, admin_id: int, display_name: str, role: str) -> None:
    db_path = _resolve_sqlite_path(database_url)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        _ensure_platform_admins_table_sqlite(conn)
        conn.execute("BEGIN")
        conn.execute("DELETE FROM platform_admins WHERE email = ? OR admin_id = ?", (email, admin_id))
        conn.execute(
            """
            INSERT INTO platform_admins(email, admin_id, display_name, role, is_active, created_at, updated_at)
            VALUES (?, ?, ?, ?, 1, ?, ?)
            """,
            (email, admin_id, display_name, role, now, now),
        )

        # Best-effort audit trail if table exists.
        try:
            conn.execute(
                """
                INSERT INTO audit_edit_logs(table_name, record_id, user_id, action, old_value, new_value, high_priority, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "platform_admins",
                    admin_id,
                    admin_id,
                    "SUPERADMIN_SEEDED",
                    None,
                    f'{{"email":"{email}","role":"{role}","display_name":"{display_name}"}}',
                    1,
                    now,
                ),
            )
        except Exception:
            pass

        conn.commit()


def _upsert_admin_postgres(*, database_url: str, email: str, admin_id: int, display_name: str, role: str) -> None:
    try:
        import psycopg
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("psycopg is required for PostgreSQL seeding") from exc

    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS platform_admins (
                    email TEXT PRIMARY KEY,
                    admin_id INTEGER NOT NULL UNIQUE,
                    display_name TEXT NOT NULL,
                    role TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            cur.execute("DELETE FROM platform_admins WHERE email = %s OR admin_id = %s", (email, admin_id))
            cur.execute(
                """
                INSERT INTO platform_admins(email, admin_id, display_name, role, is_active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, 1, %s, %s)
                """,
                (email, admin_id, display_name, role, now, now),
            )

            # Best-effort audit trail if table exists.
            try:
                cur.execute(
                    """
                    INSERT INTO audit_edit_logs(table_name, record_id, user_id, action, old_value, new_value, high_priority, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        "platform_admins",
                        admin_id,
                        admin_id,
                        "SUPERADMIN_SEEDED",
                        None,
                        f'{{"email":"{email}","role":"{role}","display_name":"{display_name}"}}',
                        1,
                        now,
                    ),
                )
            except Exception:
                pass
        conn.commit()


def seed_superadmin(*, database_url: str, email: str, admin_id: int, display_name: str, role: str) -> None:
    normalized = _normalize_database_url(database_url)
    if normalized.startswith("sqlite:///"):
        _upsert_admin_sqlite(
            database_url=normalized,
            email=email,
            admin_id=admin_id,
            display_name=display_name,
            role=role,
        )
        return

    if normalized.startswith("postgresql://"):
        _upsert_admin_postgres(
            database_url=normalized,
            email=email,
            admin_id=admin_id,
            display_name=display_name,
            role=role,
        )
        return

    raise ValueError("Unsupported DATABASE_URL. Use sqlite:///... or postgresql://...")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed Accord Superadmin into platform_admins table")
    parser.add_argument("--email", required=True, help="Superadmin email")
    parser.add_argument("--admin-id", type=int, default=1001, help="Numeric admin id used by headers")
    parser.add_argument("--display-name", default="Accord Superadmin", help="Display name")
    parser.add_argument("--role", default="SUPERADMIN", help="Role label")
    parser.add_argument(
        "--database-url",
        default=DEFAULT_DATABASE_URL,
        help="Database URL. Defaults to DATABASE_URL env or local sqlite ledger.db",
    )
    args = parser.parse_args()

    seed_superadmin(
        database_url=args.database_url,
        email=args.email.strip().lower(),
        admin_id=args.admin_id,
        display_name=args.display_name.strip(),
        role=args.role.strip().upper(),
    )

    print("Superadmin seeded successfully")
    print(f"email={args.email.strip().lower()}")
    print(f"admin_id={args.admin_id}")
    print(f"role={args.role.strip().upper()}")


if __name__ == "__main__":
    main()
