# ipl_api/cache.py
from __future__ import annotations

import time
from typing import Any, Dict, Optional, Tuple

# Simple in-memory TTL cache (sufficient for single-instance deploys)
# key -> (expires_at_epoch, value)
_cache: Dict[str, Tuple[float, Any]] = {}


def make_key(namespace: str, key: str) -> str:
    """
    Enforce namespaced cache keys to avoid collisions.
    Example:
      make_key("standings", "2026") -> "standings:2026"
    """
    namespace = namespace.strip()
    key = key.strip()
    if not namespace or not key:
        raise ValueError("Cache namespace and key must be non-empty")
    return f"{namespace}:{key}"


def get(key: str) -> Optional[Any]:
    item = _cache.get(key)
    if not item:
        return None

    expires_at, value = item
    if time.time() > expires_at:
        _cache.pop(key, None)
        return None

    return value


def set(key: str, value: Any, ttl_seconds: int = 60) -> None:
    if ttl_seconds <= 0:
        # Do not cache if TTL is invalid
        return
    _cache[key] = (time.time() + ttl_seconds, value)


def clear() -> None:
    _cache.clear()


def debug_snapshot() -> Dict[str, float]:
    """
    Returns current cache keys with remaining TTL (seconds).
    Useful for debugging.
    """
    now = time.time()
    out: Dict[str, float] = {}
    for k, (exp, _) in _cache.items():
        out[k] = max(0.0, exp - now)
    return out

def make_key(*parts: str) -> str:
    return ":".join([str(p).strip() for p in parts if str(p).strip()])

