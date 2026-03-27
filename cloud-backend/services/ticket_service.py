from __future__ import annotations

import re
import sqlite3
from datetime import datetime
from pathlib import Path


TICKET_SCHEMA_PATH = Path(__file__).resolve().parents[1] / "sql" / "it_tickets.sql"
_TICKET_PATTERN = re.compile(r"^TKT-(\d+)$")


def _ensure_schema(db: sqlite3.Connection) -> None:
    if TICKET_SCHEMA_PATH.exists():
        db.executescript(TICKET_SCHEMA_PATH.read_text(encoding="utf-8"))
    else:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS it_tickets (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                priority TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'OPEN',
                created_at TEXT NOT NULL
            )
            """
        )
    db.commit()


def _next_ticket_id(db: sqlite3.Connection) -> str:
    rows = db.execute("SELECT id FROM it_tickets ORDER BY created_at DESC LIMIT 5000").fetchall()
    max_num = 1000
    for row in rows:
        value = str(row[0] if not isinstance(row, sqlite3.Row) else row["id"]).strip()
        match = _TICKET_PATTERN.match(value)
        if not match:
            continue
        max_num = max(max_num, int(match.group(1)))
    return f"TKT-{max_num + 1}"


def create_automated_ticket(db: sqlite3.Connection, user_id: int, summary: str, priority: str) -> str:
    clean_summary = (summary or "").strip() or "Support issue escalation from AI assistant"
    clean_priority = (priority or "medium").strip().lower()
    if clean_priority not in {"low", "medium", "high"}:
        clean_priority = "medium"

    _ensure_schema(db)
    ticket_id = _next_ticket_id(db)
    created_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    title = clean_summary[:96]

    db.execute(
        """
        INSERT INTO it_tickets(id, user_id, title, summary, priority, status, created_at)
        VALUES (?, ?, ?, ?, ?, 'OPEN', ?)
        """,
        (ticket_id, int(user_id), title, clean_summary, clean_priority, created_at),
    )
    db.commit()
    return ticket_id
