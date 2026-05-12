# ipl_api/cache.py
from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional, Tuple

_cache: Dict[str, Tuple[float, Any]] = {}

# File-based persistent cache
CACHE_DIR = os.environ.get("CACHE_DIR", "/tmp/ipl_cache")
os.makedirs(CACHE_DIR, exist_ok=True)


def _file_path(key: str) -> str:
    safe = key.replace(":", "_").replace("/", "_")
    return os.path.join(CACHE_DIR, f"{safe}.json")


def _load_from_disk(key: str) -> Optional[Tuple[float, Any]]:
    try:
        with open(_file_path(key), "r") as f:
            data = json.load(f)
        return data["expires_at"], data["value"]
    except Exception:
        return None


def _save_to_disk(key: str, expires_at: float, value: Any) -> None:
    try:
        with open(_file_path(key), "w") as f:
            json.dump({"expires_at": expires_at, "value": value}, f)
    except Exception:
        pass


def get(key: str) -> Optional[Any]:
    # Check in-memory first
    item = _cache.get(key)
    if not item:
        # Try disk
        item = _load_from_disk(key)
        if item:
            _cache[key] = item  # warm in-memory

    if not item:
        return None

    expires_at, value = item
    if time.time() > expires_at:
        _cache.pop(key, None)
        return None

    return value


def set(key: str, value: Any, ttl_seconds: int = 60) -> None:
    if ttl_seconds <= 0:
        return
    expires_at = time.time() + ttl_seconds
    _cache[key] = (expires_at, value)
    _save_to_disk(key, expires_at, value)


def clear() -> None:
    _cache.clear()
    try:
        for f in os.listdir(CACHE_DIR):
            os.remove(os.path.join(CACHE_DIR, f))
    except Exception:
        pass


def debug_snapshot() -> Dict[str, float]:
    now = time.time()
    out: Dict[str, float] = {}
    for k, (exp, _) in _cache.items():
        out[k] = max(0.0, exp - now)
    return out


def make_key(*parts: str) -> str:
    return ":".join([str(p).strip() for p in parts if str(p).strip()])