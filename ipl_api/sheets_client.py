"""
sheets_client.py  (ipl_api/sheets_client.py)
--------------------------------------------
Fetches per-match innings data from a public Google Sheet (CSV export).
Replaces the Cricbuzz scraper entirely — no scraping, no delays.

Returns innings_map — the exact structure consumed by espn_standings.py:

    {
        "RCB-SRH": {"RCB": {"runs": 203, "balls": 94},  "SRH": {"runs": 201, "balls": 120}},
        "MI-KKR":  {"MI":  {"runs": 224, "balls": 115}, "KKR": {"runs": 220, "balls": 120}},
        ...
    }

The key is  f"{t1_code}-{t2_code}"  exactly matching how espn_standings.py
constructs pair_key:  pair_key = f"{t1}-{t2}"

Sheet columns (CSV export):
    match_key | winner_code | t1_code | t1_runs | t1_balls |
    t2_code   | t2_runs     | t2_balls | status
"""

import csv
import io
import logging

import httpx

logger = logging.getLogger(__name__)

SHEET_ID = "1yA5GWwWKL70DtYbqH0QSDsNBQy26t7jLsngSSKZOeZQ"
SHEET_GID = "0"  # first tab — change if your data lives on a different tab
SHEET_CSV_URL = (
    f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
    f"/export?format=csv&gid={SHEET_GID}"
)


def fetch_innings_from_sheet_sync() -> dict:
    """
    Download the Google Sheet CSV and return innings_map.

    innings_map shape:
        { "T1-T2": {"T1": {"runs": int, "balls": int},
                    "T2": {"runs": int, "balls": int}}, ... }

    Only rows with status == "completed" are included.
    Skips rows with missing/invalid data and logs a warning.

    Raises httpx.HTTPError if the sheet cannot be fetched.
    """
    logger.info("[Sheets] Fetching innings data from Google Sheets ...")

    resp = httpx.get(SHEET_CSV_URL, timeout=15, follow_redirects=True)
    resp.raise_for_status()

    innings_map: dict = {}
    reader = csv.DictReader(io.StringIO(resp.text))

    parsed = 0
    skipped = 0

    for row in reader:
        status = row.get("status", "").strip()
        if status != "completed":
            skipped += 1
            continue

        match_key = row.get("match_key", "").strip()   # e.g. "RCB-SRH-2026-03-28"
        t1 = row.get("t1_code", "").strip()
        t2 = row.get("t2_code", "").strip()

        if not t1 or not t2:
            logger.warning("[Sheets] Missing team code in row: %s", match_key)
            skipped += 1
            continue

        try:
            t1_runs  = int(row["t1_runs"])
            t1_balls = int(row["t1_balls"])
            t2_runs  = int(row["t2_runs"])
            t2_balls = int(row["t2_balls"])
        except (KeyError, ValueError) as exc:
            logger.warning("[Sheets] Bad numeric data in row %s: %s", match_key, exc)
            skipped += 1
            continue

        # pair_key matches how espn_standings.py builds it:  f"{t1}-{t2}"
        pair_key = f"{t1}-{t2}"
        innings_map[pair_key] = {
            t1: {"runs": t1_runs, "balls": t1_balls},
            t2: {"runs": t2_runs, "balls": t2_balls},
        }
        parsed += 1

    logger.info(
        "[Sheets] Parsed %d completed matches, skipped %d rows.", parsed, skipped
    )
    return innings_map