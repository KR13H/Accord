from __future__ import annotations

import json
<<<<<<< HEAD
import os
from functools import lru_cache
from typing import Any

import logging
import os
import threading
import time
from typing import Any

try:
    import redis  # type: ignore
except Exception:  # noqa: BLE001
    redis = None


_logger = logging.getLogger("accord.redis")
REDIS_URL = os.getenv("ACCORD_REDIS_URL", os.getenv("REDIS_URL", "redis://localhost:6379/0"))
REDIS_CACHE_PREFIX = os.getenv("ACCORD_CACHE_PREFIX", "accord")

_memory_lock = threading.Lock()
_memory_store: dict[str, tuple[str, float | None]] = {}
_redis_client: Any | None = None


def _epoch_seconds() -> float:
    return time.time()


def _get_redis_client() -> Any | None:
    global _redis_client
    if redis is None:
        return None
    if _redis_client is not None:
        return _redis_client

    try:
        _redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        _redis_client.ping()
        return _redis_client
    except Exception as exc:  # noqa: BLE001
        _logger.warning("Redis unavailable, using in-memory cache fallback: %s", exc)
        _redis_client = None
        return None


def get_redis_client() -> Any | None:
    return _get_redis_client()


def namespaced_key(*parts: object) -> str:
    clean = [str(part).strip() for part in parts if str(part).strip()]
    return REDIS_CACHE_PREFIX + ":" + ":".join(clean)


def _set_memory(key: str, value: str, ttl_seconds: int | None) -> None:
    expires_at = None
    if ttl_seconds is not None and ttl_seconds > 0:
        expires_at = _epoch_seconds() + ttl_seconds
    with _memory_lock:
        _memory_store[key] = (value, expires_at)


def _get_memory(key: str) -> str | None:
    with _memory_lock:
        item = _memory_store.get(key)
        if item is None:
            return None
        value, expires_at = item
        if expires_at is not None and expires_at <= _epoch_seconds():
            _memory_store.pop(key, None)
            return None
        return value


def _ttl_memory(key: str) -> int | None:
    with _memory_lock:
        item = _memory_store.get(key)
        if item is None:
            return None
        _, expires_at = item
        if expires_at is None:
            return None
        remaining = int(expires_at - _epoch_seconds())
        if remaining <= 0:
            _memory_store.pop(key, None)
            return None
        return remaining


def _del_memory(key: str) -> None:
    with _memory_lock:
        _memory_store.pop(key, None)


def cache_set_json(key: str, payload: Any, ttl_seconds: int | None = None) -> bool:
    value = json.dumps(payload, ensure_ascii=True, default=str)
    client = _get_redis_client()
    if client is not None:
        try:
            if ttl_seconds is not None and ttl_seconds > 0:
                client.setex(key, ttl_seconds, value)
            else:
                client.set(key, value)
            return True
        except Exception as exc:  # noqa: BLE001
            _logger.warning("Redis set failed, falling back to memory cache: %s", exc)
    _set_memory(key, value, ttl_seconds)
    return True


def cache_get_json(key: str) -> Any | None:
    client = _get_redis_client()
    raw: str | None = None
    if client is not None:
        try:
            raw = client.get(key)
        except Exception as exc:  # noqa: BLE001
            _logger.warning("Redis get failed, falling back to memory cache: %s", exc)
    if raw is None:
        raw = _get_memory(key)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def cache_set_text(key: str, value: str, ttl_seconds: int | None = None) -> None:
    client = _get_redis_client()
    if client is not None:
        try:
            if ttl_seconds is not None and ttl_seconds > 0:
                client.setex(key, ttl_seconds, value)
            else:
                client.set(key, value)
            return
        except Exception as exc:  # noqa: BLE001
            _logger.warning("Redis set text failed, using memory fallback: %s", exc)
    _set_memory(key, value, ttl_seconds)


def cache_get_text(key: str) -> str | None:
    client = _get_redis_client()
    if client is not None:
        try:
            value = client.get(key)
            if isinstance(value, str):
                return value
        except Exception as exc:  # noqa: BLE001
            _logger.warning("Redis get text failed, using memory fallback: %s", exc)
    return _get_memory(key)


def cache_delete(key: str) -> None:
    client = _get_redis_client()
    if client is not None:
        try:
            client.delete(key)
            return
        except Exception as exc:  # noqa: BLE001
            _logger.warning("Redis delete failed, using memory fallback: %s", exc)
    _del_memory(key)


def cache_ttl_seconds(key: str) -> int | None:
    client = _get_redis_client()
    if client is not None:
        try:
            ttl = client.ttl(key)
            if ttl is None or ttl < 0:
                return None
            return int(ttl)
        except Exception as exc:  # noqa: BLE001
            _logger.warning("Redis ttl failed, using memory fallback: %s", exc)
    return _ttl_memory(key)


def otp_key(phone_number: str) -> str:
    return namespaced_key("otp", phone_number)


def set_otp_code(phone_number: str, code: str, ttl_seconds: int = 300) -> None:
    cache_set_text(otp_key(phone_number), code, ttl_seconds)


def get_otp_code(phone_number: str) -> str | None:
    return cache_get_text(otp_key(phone_number))


def delete_otp_code(phone_number: str) -> None:
    cache_delete(otp_key(phone_number))


def blocklist_key(token_jti: str) -> str:
    return namespaced_key("auth", "blocklist", token_jti)


def blocklist_token_jti(token_jti: str, ttl_seconds: int) -> None:
    cache_set_text(blocklist_key(token_jti), "1", ttl_seconds=max(60, ttl_seconds))


def is_token_jti_blocklisted(token_jti: str) -> bool:
    return cache_get_text(blocklist_key(token_jti)) is not None
    except Exception:  # noqa: BLE001
        return False


def namespaced_key(*parts: object) -> str:
    text = ":".join(str(part).strip() for part in parts if str(part).strip())
    return f"{REDIS_CACHE_PREFIX}:{text}"
=======
_logger = logging.getLogger("accord.redis")
_DEFAULT_REDIS_URL = os.getenv("ACCORD_REDIS_URL", os.getenv("REDIS_URL", "redis://localhost:6379/0"))

_memory_lock = threading.Lock()
_memory_store: dict[str, tuple[str, float | None]] = {}
_redis_client: Any | None = None


def _epoch_seconds() -> float:
    return time.time()


def _get_redis_client() -> Any | None:
    global _redis_client
    if redis is None:
        return None
    if _redis_client is not None:
        return _redis_client

    try:
        _redis_client = redis.Redis.from_url(_DEFAULT_REDIS_URL, decode_responses=True)
        _redis_client.ping()
        return _redis_client
    except Exception as exc:  # noqa: BLE001
        _logger.warning("Redis unavailable, using in-memory cache fallback: %s", exc)
        _redis_client = None
        return None


def namespaced_key(*parts: str) -> str:
    clean = [str(part).strip() for part in parts if str(part).strip()]
    return "accord:" + ":".join(clean)


def _set_memory(key: str, value: str, ttl_seconds: int | None) -> None:
    expires_at = None
    if ttl_seconds is not None and ttl_seconds > 0:
        expires_at = _epoch_seconds() + ttl_seconds
    with _memory_lock:
        _memory_store[key] = (value, expires_at)


def _get_memory(key: str) -> str | None:
    with _memory_lock:
        item = _memory_store.get(key)
        if item is None:
            return None
        value, expires_at = item
        if expires_at is not None and expires_at <= _epoch_seconds():
            _memory_store.pop(key, None)
            return None
        return value


def _ttl_memory(key: str) -> int | None:
    with _memory_lock:
        item = _memory_store.get(key)
        if item is None:
            return None
        _, expires_at = item
        if expires_at is None:
            return None
        remaining = int(expires_at - _epoch_seconds())
        if remaining <= 0:
            _memory_store.pop(key, None)
            return None
        return remaining


def _del_memory(key: str) -> None:
    with _memory_lock:
        _memory_store.pop(key, None)


def cache_set_json(key: str, payload: dict[str, Any], ttl_seconds: int | None = None) -> None:
    value = json.dumps(payload, ensure_ascii=True)
    client = _get_redis_client()
    if client is not None:
        try:
            if ttl_seconds is not None and ttl_seconds > 0:
                client.setex(key, ttl_seconds, value)
            else:
                client.set(key, value)
            return
        except Exception as exc:  # noqa: BLE001
            _logger.warning("Redis set failed, falling back to memory cache: %s", exc)
    _set_memory(key, value, ttl_seconds)


def cache_get_json(key: str) -> dict[str, Any] | None:
    client = _get_redis_client()
    raw: str | None = None
    if client is not None:
        try:
            raw = client.get(key)
        except Exception as exc:  # noqa: BLE001
            _logger.warning("Redis get failed, falling back to memory cache: %s", exc)
    if raw is None:
        raw = _get_memory(key)
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def cache_set_text(key: str, value: str, ttl_seconds: int | None = None) -> None:
    client = _get_redis_client()
    if client is not None:
        try:
            if ttl_seconds is not None and ttl_seconds > 0:
                client.setex(key, ttl_seconds, value)
            else:
                client.set(key, value)
            return
        except Exception as exc:  # noqa: BLE001
            _logger.warning("Redis set text failed, using memory fallback: %s", exc)
    _set_memory(key, value, ttl_seconds)


def cache_get_text(key: str) -> str | None:
    client = _get_redis_client()
    if client is not None:
        try:
            value = client.get(key)
            if isinstance(value, str):
                return value
        except Exception as exc:  # noqa: BLE001
            _logger.warning("Redis get text failed, using memory fallback: %s", exc)
    return _get_memory(key)


def cache_delete(key: str) -> None:
    client = _get_redis_client()
    if client is not None:
        try:
            client.delete(key)
            return
        except Exception as exc:  # noqa: BLE001
            _logger.warning("Redis delete failed, using memory fallback: %s", exc)
    _del_memory(key)


def cache_ttl_seconds(key: str) -> int | None:
    client = _get_redis_client()
    if client is not None:
        try:
            ttl = client.ttl(key)
            if ttl is None or ttl < 0:
                return None
            return int(ttl)
        except Exception as exc:  # noqa: BLE001
            _logger.warning("Redis ttl failed, using memory fallback: %s", exc)
    return _ttl_memory(key)


def otp_key(phone_number: str) -> str:
    return namespaced_key("otp", phone_number)


def set_otp_code(phone_number: str, code: str, ttl_seconds: int = 300) -> None:
    cache_set_text(otp_key(phone_number), code, ttl_seconds)


def get_otp_code(phone_number: str) -> str | None:
    return cache_get_text(otp_key(phone_number))


def delete_otp_code(phone_number: str) -> None:
    cache_delete(otp_key(phone_number))


def blocklist_key(token_jti: str) -> str:
    return namespaced_key("auth", "blocklist", token_jti)


def blocklist_token_jti(token_jti: str, ttl_seconds: int) -> None:
    cache_set_text(blocklist_key(token_jti), "1", ttl_seconds=max(60, ttl_seconds))


def is_token_jti_blocklisted(token_jti: str) -> bool:
    return cache_get_text(blocklist_key(token_jti)) is not None
>>>>>>> feature/hardware-pos-polish
