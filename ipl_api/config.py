# ipl_api/config.py
from __future__ import annotations

import os
from dotenv import load_dotenv

# Load .env from project root
load_dotenv()


def _get_env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def _get_env_int(name: str, default: int) -> int:
    raw = _get_env(name, str(default))
    try:
        return int(raw)
    except ValueError:
        return default


# -------------------------
# CricketData (CricAPI) config (OPTIONAL)
# -------------------------
CRICKETDATA_API_KEY: str = _get_env("CRICKETDATA_API_KEY")
CRICKETDATA_BASE_URL: str = _get_env("CRICKETDATA_BASE_URL", "https://api.cricapi.com/v1")

# If 0, ESPN-only mode works even without CRICKETDATA_API_KEY
CRICKETDATA_ENABLED: bool = _get_env("CRICKETDATA_ENABLED", "0") == "1"


# -------------------------
# ESPN standings + fixtures config (WPL)
# -------------------------
# Use ESPN "series id" (can change per season)
WPL_SERIES_ID: str = _get_env("WPL_SERIES_ID", "1510059")  # default from your ESPN Cricinfo URL

# ESPN points table URL pattern (season-based)
ESPN_TABLE_URL_TEMPLATE: str = _get_env(
    "ESPN_TABLE_URL_TEMPLATE",
    "https://www.espn.in/cricket/table/series/{series_id}/season/{season}/womens-premier-league",
)

# ESPN fixtures/scores URL pattern (season-based)
ESPN_FIXTURES_URL_TEMPLATE: str = _get_env(
    "ESPN_FIXTURES_URL_TEMPLATE",
    "https://www.espn.in/cricket/scores/series/{series_id}/season/{season}/womens-premier-league",
)

# Cache TTLs
STANDINGS_CACHE_TTL_SECONDS: int = _get_env_int("STANDINGS_CACHE_TTL_SECONDS", 120)
FIXTURES_CACHE_TTL_SECONDS: int = _get_env_int("FIXTURES_CACHE_TTL_SECONDS", 900)


def validate_config() -> None:
    # Basic URL sanity
    if not CRICKETDATA_BASE_URL.startswith("http"):
        raise RuntimeError("CRICKETDATA_BASE_URL must start with http/https")

    # If enabled, enforce key
    if CRICKETDATA_ENABLED:
        if not CRICKETDATA_API_KEY or CRICKETDATA_API_KEY in {"DUMMY_KEY", "PASTE_YOUR_KEY_HERE"}:
            raise RuntimeError("CRICKETDATA_API_KEY missing/placeholder but CRICKETDATA_ENABLED=1")

    # ESPN templates must have placeholders
    if "{season}" not in ESPN_TABLE_URL_TEMPLATE or "{series_id}" not in ESPN_TABLE_URL_TEMPLATE:
        raise RuntimeError("ESPN_TABLE_URL_TEMPLATE must contain {series_id} and {season} placeholders.")

    if "{season}" not in ESPN_FIXTURES_URL_TEMPLATE or "{series_id}" not in ESPN_FIXTURES_URL_TEMPLATE:
        raise RuntimeError("ESPN_FIXTURES_URL_TEMPLATE must contain {series_id} and {season} placeholders.")

    # TTL validation
    if STANDINGS_CACHE_TTL_SECONDS <= 0:
        raise RuntimeError("STANDINGS_CACHE_TTL_SECONDS must be positive")

    if FIXTURES_CACHE_TTL_SECONDS <= 0:
        raise RuntimeError("FIXTURES_CACHE_TTL_SECONDS must be positive")
REQUIRE_CRICKETDATA_API_KEY: bool = CRICKETDATA_ENABLED
