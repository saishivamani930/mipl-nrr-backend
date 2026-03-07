# ipl_api/cricketdata_client.py
from __future__ import annotations

from typing import Any, Dict, Optional
import requests

from ipl_api.config import (
    CRICKETDATA_API_KEY,
    CRICKETDATA_BASE_URL,
    REQUIRE_CRICKETDATA_API_KEY,
)


class CricketDataError(Exception):
    """Raised when CricAPI (cricapi.com) call fails or is misconfigured."""
    pass


def get_json(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Generic helper to call CricAPI endpoints.

    IMPORTANT:
    - CricAPI is optional (ESPN is primary).
    - Only allow usage when REQUIRE_CRICKETDATA_API_KEY=1.
    """
    if not REQUIRE_CRICKETDATA_API_KEY:
        raise CricketDataError("CricketData is disabled (set REQUIRE_CRICKETDATA_API_KEY=1 to enable).")

    if not CRICKETDATA_API_KEY:
        raise CricketDataError("CRICKETDATA_API_KEY is not configured")

    if not CRICKETDATA_BASE_URL.startswith("http"):
        raise CricketDataError("CRICKETDATA_BASE_URL must start with http/https")

    url = f"{CRICKETDATA_BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"
    query = dict(params or {})
    query["apikey"] = CRICKETDATA_API_KEY

    try:
        resp = requests.get(url, params=query, timeout=12)
    except requests.RequestException as e:
        raise CricketDataError(f"Network error: {e}") from e

    if resp.status_code != 200:
        raise CricketDataError(f"HTTP {resp.status_code}: {resp.text}")

    try:
        data = resp.json()
    except Exception as e:
        raise CricketDataError(f"Invalid JSON response: {e}") from e

    if data.get("status") != "success":
        raise CricketDataError(data.get("message", "Unknown API error"))

    return data
