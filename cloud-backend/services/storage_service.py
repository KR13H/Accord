from __future__ import annotations

import base64
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class StoredObject:
    backend: str
    uri: str
    url: str
    key: str
    content_type: str
    size_bytes: int


class LocalStorageBackend:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def put_bytes(self, *, key: str, payload: bytes, content_type: str) -> StoredObject:
        safe_key = key.lstrip("/")
        target = self.root / safe_key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        uri = f"file://{target}"
        return StoredObject(
            backend="local",
            uri=uri,
            url=uri,
            key=safe_key,
            content_type=content_type,
            size_bytes=len(payload),
        )


class S3StorageBackend:
    def __init__(self, *, bucket: str, region: str, endpoint_url: str | None = None) -> None:
        self.bucket = bucket
        self.region = region
        self.endpoint_url = endpoint_url

    def put_bytes(self, *, key: str, payload: bytes, content_type: str) -> StoredObject:
        import boto3

        client = boto3.client("s3", region_name=self.region, endpoint_url=self.endpoint_url)
        client.put_object(Bucket=self.bucket, Key=key, Body=payload, ContentType=content_type)

        public_base = os.getenv("ACCORD_S3_PUBLIC_BASE_URL", "").rstrip("/")
        if public_base:
            url = f"{public_base}/{key}"
        else:
            url = f"s3://{self.bucket}/{key}"

        return StoredObject(
            backend="s3",
            uri=f"s3://{self.bucket}/{key}",
            url=url,
            key=key,
            content_type=content_type,
            size_bytes=len(payload),
        )


class StorageService:
    def __init__(self) -> None:
        self.backend_name = os.getenv("ACCORD_STORAGE_BACKEND", "local").strip().lower() or "local"
        local_root = Path(os.getenv("ACCORD_LOCAL_STORAGE_ROOT", str(Path(__file__).resolve().parents[2] / "storage" / "assets")))

        if self.backend_name == "s3":
            bucket = os.getenv("ACCORD_S3_BUCKET", "").strip()
            region = os.getenv("ACCORD_S3_REGION", "ap-south-1").strip()
            endpoint = os.getenv("ACCORD_S3_ENDPOINT_URL", "").strip() or None
            if not bucket:
                self.backend_name = "local"
                self.backend = LocalStorageBackend(local_root)
            else:
                self.backend = S3StorageBackend(bucket=bucket, region=region, endpoint_url=endpoint)
        else:
            self.backend = LocalStorageBackend(local_root)

    def put_bytes(self, *, key: str, payload: bytes, content_type: str) -> dict[str, Any]:
        stored = self.backend.put_bytes(key=key, payload=payload, content_type=content_type)
        return {
            "backend": stored.backend,
            "uri": stored.uri,
            "url": stored.url,
            "key": stored.key,
            "content_type": stored.content_type,
            "size_bytes": stored.size_bytes,
        }

    def put_base64_image(self, *, key: str, image_base64: str, default_content_type: str = "image/jpeg") -> dict[str, Any]:
        raw = image_base64.strip()
        content_type = default_content_type
        if raw.startswith("data:") and ";base64," in raw:
            prefix, raw = raw.split(",", 1)
            content_type = prefix.replace("data:", "").replace(";base64", "").strip() or default_content_type
        payload = base64.b64decode(raw)
        return self.put_bytes(key=key, payload=payload, content_type=content_type)


_storage_service: StorageService | None = None


def get_storage_service() -> StorageService:
    global _storage_service
    if _storage_service is None:
        _storage_service = StorageService()
    return _storage_service
