from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime
from functools import wraps
from typing import Any, Callable

from fastapi import Header, HTTPException


def _utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def ensure_idempotency_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS idempotency_keys (
            key TEXT PRIMARY KEY,
            route TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('PENDING', 'COMPLETED', 'FAILED')),
            request_hash TEXT NOT NULL,
            response_body TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def _hash_payload(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def require_idempotency(get_conn: Callable[[], sqlite3.Connection], route_name: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper(*args: Any, idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"), **kwargs: Any) -> Any:
            key = (idempotency_key or "").strip()
            if not key:
                raise HTTPException(
                    status_code=400,
                    detail="Idempotency-Key header is required for financial transactions",
                )

            payload = kwargs.get("payload")
            payload_hash = _hash_payload(payload.model_dump() if payload is not None else {})
            now_iso = _utc_now()

            with get_conn() as conn:
                conn.row_factory = sqlite3.Row
                ensure_idempotency_schema(conn)
                existing = conn.execute(
                    "SELECT key, status, request_hash, response_body FROM idempotency_keys WHERE key = ?",
                    (key,),
                ).fetchone()
                if existing is not None:
                    if str(existing["request_hash"]) != payload_hash:
                        raise HTTPException(status_code=409, detail="Idempotency key payload mismatch")
                    status = str(existing["status"])
                    if status == "COMPLETED":
                        return json.loads(str(existing["response_body"]))
                    if status == "PENDING":
                        raise HTTPException(status_code=409, detail="Request with this key is currently in progress")

                conn.execute(
                    """
                    INSERT OR REPLACE INTO idempotency_keys(key, route, status, request_hash, response_body, created_at, updated_at)
                    VALUES (?, ?, 'PENDING', ?, NULL, ?, ?)
                    """,
                    (key, route_name, payload_hash, now_iso, now_iso),
                )
                conn.commit()

            try:
                response = await func(*args, **kwargs)
            except Exception:
                with get_conn() as conn:
                    ensure_idempotency_schema(conn)
                    conn.execute(
                        "UPDATE idempotency_keys SET status = 'FAILED', updated_at = ? WHERE key = ?",
                        (_utc_now(), key),
                    )
                    conn.commit()
                raise

            with get_conn() as conn:
                ensure_idempotency_schema(conn)
                conn.execute(
                    """
                    UPDATE idempotency_keys
                    SET status = 'COMPLETED', response_body = ?, updated_at = ?
                    WHERE key = ?
                    """,
                    (json.dumps(response), _utc_now(), key),
                )
                conn.commit()
            return response

        return wrapper

    return decorator
