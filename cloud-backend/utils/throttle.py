from datetime import datetime, timedelta
from typing import Dict

from fastapi import HTTPException, Request

# Simple in-memory limiter for expensive actions.
# For multi-instance deployments, move this state to Redis.
_rate_limit_cache: Dict[str, datetime] = {}


def rate_limit_heavy_task(seconds: int = 5):
    """
    Dependency to prevent spamming expensive AI or sync endpoints.
    """

    async def _rate_limiter(request: Request):
        client_ip = request.client.host if request.client else "unknown"
        route_path = request.url.path
        cache_key = f"{client_ip}:{route_path}"

        now = datetime.now()
        last_request = _rate_limit_cache.get(cache_key)
        if last_request and now < last_request + timedelta(seconds=seconds):
            raise HTTPException(
                status_code=429,
                detail="Please wait a moment before trying this heavy action again.",
            )

        _rate_limit_cache[cache_key] = now
        return True

    return _rate_limiter
