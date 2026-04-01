from __future__ import annotations

import time
from collections import defaultdict, deque
from threading import Lock

from fastapi import HTTPException, Request

from utils.redis_runtime import get_redis_client, namespaced_key


_local_lock = Lock()
_local_windows: dict[str, deque[float]] = defaultdict(deque)


def _allow_local(key: str, limit: int, window_seconds: int) -> bool:
    now = time.time()
    floor = now - max(1, window_seconds)
    with _local_lock:
        bucket = _local_windows[key]
        while bucket and bucket[0] < floor:
            bucket.popleft()
        if len(bucket) >= max(1, limit):
            return False
        bucket.append(now)
        return True


def _allow_redis(key: str, limit: int, window_seconds: int) -> bool:
    client = get_redis_client()
    if client is None:
        return _allow_local(key, limit, window_seconds)

    now = time.time()
    floor = now - max(1, window_seconds)
    try:
        pipe = client.pipeline(transaction=True)
        pipe.zremrangebyscore(key, 0, floor)
        pipe.zcard(key)
        pipe.expire(key, max(1, window_seconds))
        _, count, _ = pipe.execute()
        if int(count or 0) >= max(1, limit):
            return False

        member = f"{now}:{time.monotonic_ns()}"
        pipe = client.pipeline(transaction=True)
        pipe.zadd(key, {member: now})
        pipe.expire(key, max(1, window_seconds))
        pipe.execute()
        return True
    except Exception:  # noqa: BLE001
        return _allow_local(key, limit, window_seconds)


def build_rate_limit_dependency(scope: str, *, limit: int, window_seconds: int):
    async def _dependency(request: Request) -> bool:
        client_ip = request.client.host if request.client else "unknown"
        key = namespaced_key("ratelimit", scope, client_ip)
        allowed = _allow_redis(key, limit, window_seconds)
        if not allowed:
            raise HTTPException(status_code=429, detail="Rate limit exceeded. Please retry later.")
        return True

    return _dependency
