from __future__ import annotations

import csv
import io
import logging
from datetime import datetime
from typing import Any, Dict, List

import requests

logger = logging.getLogger(__name__)

SHEET_ID = "1yA5GWwWKL70DtYbqH0QSDsNBQy26t7jLsngSSKZOeZQ"
SHEET_NAME = "fixtures"

SHEET_FIXTURES_CSV_URL = (
    f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
    f"/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}"
)


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def fetch_fixtures_from_sheet_sync(season: int = 2026) -> Dict[str, Any]:
    """
    Fetch IPL fixtures/results from Google Sheets.

    Expected columns:
    match_id,date,team1_code,team2_code,team1,team2,status,venue,winner_code,result
    """

    logger.info("[Sheets Fixtures] Fetching fixtures from Google Sheets...")

    resp = requests.get(
        SHEET_FIXTURES_CSV_URL,
        timeout=15,
        headers={
            "User-Agent": "Mozilla/5.0",
        },
    )
    resp.raise_for_status()

    reader = csv.DictReader(io.StringIO(resp.text))
    fixtures: List[Dict[str, Any]] = []

    parsed = 0
    skipped = 0

    for row in reader:
        match_id = _clean(row.get("match_id"))
        date = _clean(row.get("date"))
        team1_code = _clean(row.get("team1_code")).upper()
        team2_code = _clean(row.get("team2_code")).upper()
        team1 = _clean(row.get("team1"))
        team2 = _clean(row.get("team2"))
        status = _clean(row.get("status")).lower() or "upcoming"
        venue = _clean(row.get("venue"))
        winner_code = _clean(row.get("winner_code")).upper()
        result = _clean(row.get("result"))

        if not match_id or not date or not team1_code or not team2_code:
            skipped += 1
            continue

        fixture: Dict[str, Any] = {
            "match_id": match_id,
            "date": date,
            "team1_code": team1_code,
            "team2_code": team2_code,
            "team1": team1,
            "team2": team2,
            "status": status,
            "venue": venue,
        }

        if winner_code:
            fixture["winner_code"] = winner_code

        if result:
            fixture["result"] = result

        fixtures.append(fixture)
        parsed += 1

    fixtures.sort(key=lambda f: f.get("date", ""))

    logger.info(
        "[Sheets Fixtures] Parsed %d fixtures, skipped %d rows.",
        parsed,
        skipped,
    )

    return {
        "season": season,
        "source": "google_sheets_fixtures",
        "url_used": SHEET_FIXTURES_CSV_URL,
        "last_updated_utc": datetime.utcnow().isoformat() + "Z",
        "fixtures": fixtures,
        "fixtures_count": len(fixtures),
    }