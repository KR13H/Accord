from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any

try:
    import redis
except Exception:  # noqa: BLE001
    redis = None


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_CACHE_PREFIX = os.getenv("ACCORD_CACHE_PREFIX", "accord")


@lru_cache(maxsize=1)
def get_redis_client():
    if redis is None:
        return None
    try:
        client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        client.ping()
        return client
    except Exception:  # noqa: BLE001
        return None


def cache_get_json(key: str) -> Any | None:
    client = get_redis_client()
    if client is None:
        return None
    try:
        raw = client.get(key)
        if not raw:
            return None
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return None


def cache_set_json(key: str, value: Any, ttl_seconds: int) -> bool:
    client = get_redis_client()
    if client is None:
        return False
    try:
        client.setex(key, max(1, ttl_seconds), json.dumps(value, ensure_ascii=True, separators=(",", ":"), default=str))
        return True
    except Exception:  # noqa: BLE001
        return False


def namespaced_key(*parts: object) -> str:
    text = ":".join(str(part).strip() for part in parts if str(part).strip())
    return f"{REDIS_CACHE_PREFIX}:{text}"
