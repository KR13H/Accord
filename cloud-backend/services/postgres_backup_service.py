from __future__ import annotations

import base64
import os
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from services.storage_service import get_storage_service
from utils.db_runtime import get_database_url, is_postgres_url


def _load_encryption_key() -> bytes:
    raw = os.getenv("ACCORD_BACKUP_ENCRYPTION_KEY", "").strip()
    if raw:
        try:
            key = base64.urlsafe_b64decode(raw + "=" * ((4 - len(raw) % 4) % 4))
            if len(key) == 32:
                return key
        except Exception:  # noqa: BLE001
            pass
    # Fallback for local dry-runs only; production must override.
    return b"accord-local-backup-key-32-bytes!!"[:32]


def run_pg_backup(*, dry_run: bool = False) -> dict[str, Any]:
    database_url = get_database_url()
    if not is_postgres_url(database_url):
        return {"status": "skipped", "detail": "DATABASE_URL is not PostgreSQL"}

    stamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    key_base = f"db-backups/{datetime.utcnow().strftime('%Y/%m/%d')}"
    file_key = f"{key_base}/accord-pg-{stamp}.dump.enc"

    with tempfile.TemporaryDirectory(prefix="accord-pg-backup-") as temp_dir:
        temp_path = Path(temp_dir)
        dump_path = temp_path / f"accord-pg-{stamp}.dump"
        encrypted_path = temp_path / f"accord-pg-{stamp}.dump.enc"

        if dry_run:
            dump_path.write_bytes(b"DRY_RUN_PG_DUMP")
        else:
            command = [
                "pg_dump",
                "--format=custom",
                "--no-owner",
                "--no-privileges",
                f"--file={dump_path}",
                database_url,
            ]
            completed = subprocess.run(command, capture_output=True, text=True, check=False)
            if completed.returncode != 0:
                return {
                    "status": "error",
                    "detail": "pg_dump failed",
                    "stderr": (completed.stderr or "")[-800:],
                }

        plaintext = dump_path.read_bytes()
        nonce = os.urandom(12)
        encrypted = AESGCM(_load_encryption_key()).encrypt(nonce, plaintext, None)
        encrypted_path.write_bytes(nonce + encrypted)

        if dry_run:
            return {
                "status": "ok",
                "mode": "dry-run",
                "encrypted_size_bytes": encrypted_path.stat().st_size,
                "planned_key": file_key,
            }

        storage = get_storage_service()
        uploaded = storage.put_bytes(
            key=file_key,
            payload=encrypted_path.read_bytes(),
            content_type="application/octet-stream",
        )
        return {
            "status": "ok",
            "mode": "live",
            "key": uploaded["key"],
            "uri": uploaded["uri"],
            "url": uploaded["url"],
            "encrypted_size_bytes": uploaded["size_bytes"],
        }
