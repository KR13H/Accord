from __future__ import annotations

from utils.redis_runtime import cache_get_text, cache_set_text, namespaced_key


FEATURE_FLAG_PREFIX = "feature-flags"


def _flag_key(flag_name: str) -> str:
    clean_name = flag_name.strip().lower().replace(" ", "_")
    if not clean_name:
        raise ValueError("flag_name is required")
    return namespaced_key(FEATURE_FLAG_PREFIX, clean_name)


def is_feature_enabled(flag_name: str, default: bool = True) -> bool:
    value = cache_get_text(_flag_key(flag_name))
    if value is None:
        return default
    normalized = value.strip().lower()
    return normalized in {"1", "true", "on", "enabled", "yes"}


def set_feature_flag(flag_name: str, enabled: bool) -> bool:
    cache_set_text(_flag_key(flag_name), "1" if enabled else "0")
    return enabled
