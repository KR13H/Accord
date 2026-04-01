from __future__ import annotations

import os
from pathlib import Path


DEFAULT_SQLITE_PATH = Path(__file__).resolve().parents[1] / "ledger.db"


def normalize_database_url(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return f"sqlite:///{DEFAULT_SQLITE_PATH}"
    if value.startswith("postgres://"):
        return value.replace("postgres://", "postgresql://", 1)
    return value


def get_database_url() -> str:
    return normalize_database_url(os.getenv("DATABASE_URL", f"sqlite:///{DEFAULT_SQLITE_PATH}"))


def is_postgres_url(database_url: str) -> bool:
    return database_url.startswith("postgresql://") or database_url.startswith("postgresql+psycopg://")


def resolve_sqlite_db_path(database_url: str) -> Path:
    if not database_url.startswith("sqlite:///"):
        return DEFAULT_SQLITE_PATH
    raw = database_url.replace("sqlite:///", "", 1)
    path = Path(raw)
    if not path.is_absolute():
        path = Path(__file__).resolve().parents[1] / raw
    return path


def sqlalchemy_database_url(database_url: str) -> str:
    if database_url.startswith("postgresql://"):
        return database_url.replace("postgresql://", "postgresql+psycopg://", 1)
    return database_url
