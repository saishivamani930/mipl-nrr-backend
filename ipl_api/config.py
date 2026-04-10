# ipl_api/config.py
from __future__ import annotations

import os
import logging

logger = logging.getLogger(__name__)

# ── IPL Series ────────────────────────────────────────────────────────────────
IPL_SERIES_ID = int(os.environ.get("IPL_SERIES_ID", "8048"))  # ESPN series ID for IPL 2026

# ── ESPN URLs ─────────────────────────────────────────────────────────────────
ESPN_TABLE_URL_TEMPLATE = os.environ.get(
    "ESPN_TABLE_URL_TEMPLATE",
    "https://www.espn.in/cricket/table/series/{series_id}/season/{season}/indian-premier-league",
)

ESPN_FIXTURES_URL_TEMPLATE = os.environ.get(
    "ESPN_FIXTURES_URL_TEMPLATE",
    "https://www.espncricinfo.com/series/ipl-2026-{series_id}/match-schedule-fixtures",
)

ESPN_FIXTURES_SCHEDULE_URL_TEMPLATE = os.environ.get(
    "ESPN_FIXTURES_SCHEDULE_URL_TEMPLATE",
    "https://www.espn.in/cricket/fixtures/series/{series_id}/season/{season}/indian-premier-league",
)

# ── Cache TTLs ────────────────────────────────────────────────────────────────
STANDINGS_CACHE_TTL_SECONDS = int(os.environ.get("STANDINGS_CACHE_TTL_SECONDS", "300"))
FIXTURES_CACHE_TTL_SECONDS = int(os.environ.get("FIXTURES_CACHE_TTL_SECONDS", "300"))

# ── CricketData (cricapi.com) — optional, disabled by default ─────────────────
CRICKETDATA_API_KEY: str = os.environ.get("CRICKETDATA_API_KEY", "")
CRICKETDATA_BASE_URL: str = os.environ.get("CRICKETDATA_BASE_URL", "https://api.cricapi.com/v1")
REQUIRE_CRICKETDATA_API_KEY: bool = os.environ.get("REQUIRE_CRICKETDATA_API_KEY", "0") == "1"
CRICKETDATA_ENABLED: bool = os.environ.get("CRICKETDATA_ENABLED", "0") == "1"


def validate_config() -> None:
    """
    Called at startup to log config state and warn about missing optional keys.
    Does NOT raise — missing CricketData key is non-fatal since it's optional.
    """
    logger.info(f"[CONFIG] IPL_SERIES_ID={IPL_SERIES_ID}")
    logger.info(f"[CONFIG] STANDINGS_CACHE_TTL_SECONDS={STANDINGS_CACHE_TTL_SECONDS}")
    logger.info(f"[CONFIG] FIXTURES_CACHE_TTL_SECONDS={FIXTURES_CACHE_TTL_SECONDS}")
    logger.info(f"[CONFIG] CRICKETDATA_ENABLED={CRICKETDATA_ENABLED}")

    if CRICKETDATA_ENABLED and not CRICKETDATA_API_KEY:
        logger.warning(
            "[CONFIG] CRICKETDATA_ENABLED=1 but CRICKETDATA_API_KEY is not set. "
            "CricketData API calls will fail."
        )
    if not CRICKETDATA_ENABLED:
        logger.info("[CONFIG] CricketData is disabled (set CRICKETDATA_ENABLED=1 to enable).")