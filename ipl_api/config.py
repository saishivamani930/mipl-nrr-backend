# ipl_api/config.py
from __future__ import annotations

import os

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

# ── Cache ─────────────────────────────────────────────────────────────────────
FIXTURES_CACHE_TTL_SECONDS = int(os.environ.get("FIXTURES_CACHE_TTL_SECONDS", "300"))

# ── CricketData (cricapi.com) — optional, disabled by default ─────────────────
CRICKETDATA_API_KEY: str = os.environ.get("CRICKETDATA_API_KEY", "")
CRICKETDATA_BASE_URL: str = os.environ.get("CRICKETDATA_BASE_URL", "https://api.cricapi.com/v1")
REQUIRE_CRICKETDATA_API_KEY: bool = os.environ.get("REQUIRE_CRICKETDATA_API_KEY", "0") == "1"