from __future__ import annotations

import base64
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI, Request

TARGET_SEGMENTS = ("/bookings", "/rera/allocations")
MUTATING_METHODS = {"POST", "PUT", "DELETE"}
AUDIT_SQL_PATH = Path(__file__).resolve().parents[1] / "sql" / "audit_logs.sql"


def _decode_actor_from_jwt(authorization: str | None) -> dict[str, Any]:
    if not authorization:
        return {}
    parts = authorization.strip().split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return {}

    token_parts = parts[1].split(".")
    if len(token_parts) < 2:
        return {}

    payload = token_parts[1]
    padding = "=" * ((4 - len(payload) % 4) % 4)

    try:
        decoded = base64.urlsafe_b64decode(payload + padding)
        data = json.loads(decoded.decode("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def _extract_user_id(request: Request) -> int:
    claims = _decode_actor_from_jwt(request.headers.get("Authorization"))
    candidate = claims.get("user_id") or claims.get("sub") or claims.get("admin_id") or request.headers.get("X-Admin-Id")

    try:
        user_id = int(str(candidate).strip())
    except Exception:  # noqa: BLE001
        return 0
    return user_id if user_id > 0 else 0


def _serialize_payload(raw_body: bytes) -> dict[str, Any]:
    if not raw_body:
        return {}
    text = raw_body.decode("utf-8", errors="replace").strip()
    if not text:
        return {}

    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
        return {"raw_json": parsed}
    except json.JSONDecodeError:
        return {"raw_body": text[:20000]}


def _should_audit(method: str, path: str) -> bool:
    if method.upper() not in MUTATING_METHODS:
        return False
    lowered = path.lower()
    return any(segment in lowered for segment in TARGET_SEGMENTS)


def _load_audit_sql() -> str:
    try:
        return AUDIT_SQL_PATH.read_text(encoding="utf-8")
    except Exception:  # noqa: BLE001
        return """
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL DEFAULT 0,
            action TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            payload_snapshot TEXT NOT NULL,
            timestamp TEXT NOT NULL
        );
        """


def register_audit_logger_middleware(app: FastAPI, get_conn: Callable[[], sqlite3.Connection]) -> None:
    schema_sql = _load_audit_sql()

    @app.middleware("http")
    async def audit_logger_middleware(request: Request, call_next):
        method = request.method.upper()
        path = request.url.path

        if not _should_audit(method, path):
            return await call_next(request)

        raw_body = await request.body()

        async def receive() -> dict[str, Any]:
            return {"type": "http.request", "body": raw_body, "more_body": False}

        request_with_body = Request(request.scope, receive)
        response = await call_next(request_with_body)

        if response.status_code >= 400:
            return response

        payload_snapshot = {
            "method": method,
            "path": path,
            "query": dict(request.query_params),
            "body": _serialize_payload(raw_body),
            "response_status": response.status_code,
        }

        action = f"{method}:{path}"
        user_id = _extract_user_id(request)

        with get_conn() as conn:
            conn.executescript(schema_sql)
            conn.execute(
                """
                INSERT INTO audit_logs(user_id, action, endpoint, payload_snapshot, timestamp)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    action,
                    path,
                    json.dumps(payload_snapshot, ensure_ascii=True),
                    datetime.utcnow().isoformat(timespec="seconds") + "Z",
                ),
            )
            conn.commit()

        return response
