# ipl_api/sheets_client.py
import csv
import io
import logging

import httpx

logger = logging.getLogger(__name__)

SHEET_ID = "1yA5GWwWKL70DtYbqH0QSDsNBQy26t7jLsngSSKZOeZQ"
SHEET_CSV_URL = (
    f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
    f"/export?format=csv&gid=0"
)


def _parse_int(val: str) -> int | None:
    try:
        return int(val.strip())
    except (ValueError, AttributeError):
        return None


async def fetch_innings_from_sheet() -> dict[str, dict]:
    """
    Returns innings keyed by 'T1-T2' pair (e.g. 'RCB-SRH'):
    {
        'RCB-SRH': {
            'RCB': {'runs': 203, 'balls': 94},
            'SRH': {'runs': 201, 'balls': 120},
        },
        ...
    }
    Skips no_result rows and rows with missing data.
    """
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(SHEET_CSV_URL)
        resp.raise_for_status()

    reader = csv.DictReader(io.StringIO(resp.text))
    innings: dict[str, dict] = {}

    for row in reader:
        match_key = row.get("match_key", "").strip()   # e.g. RCB-SRH-2026-03-28
        status = row.get("status", "").strip()

        if not match_key or status == "no_result":
            continue

        t1 = row.get("t1_code", "").strip()
        t2 = row.get("t2_code", "").strip()
        t1_runs  = _parse_int(row.get("t1_runs", ""))
        t1_balls = _parse_int(row.get("t1_balls", ""))
        t2_runs  = _parse_int(row.get("t2_runs", ""))
        t2_balls = _parse_int(row.get("t2_balls", ""))

        if not t1 or not t2 or None in (t1_runs, t1_balls, t2_runs, t2_balls):
            logger.warning(f"[Sheets] Skipping {match_key}: missing innings data")
            continue

        pair_key = f"{t1}-{t2}"
        innings[pair_key] = {
            t1: {"runs": t1_runs, "balls": t1_balls},
            t2: {"runs": t2_runs, "balls": t2_balls},
        }

    logger.info(f"[Sheets] Loaded {len(innings)} completed matches from sheet")
    return innings


def fetch_innings_from_sheet_sync() -> dict[str, dict]:
    """Sync wrapper for use in non-async contexts."""
    import asyncio
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, fetch_innings_from_sheet())
                return future.result()
        else:
            return loop.run_until_complete(fetch_innings_from_sheet())
    except Exception as e:
        logger.error(f"[Sheets] fetch_innings_from_sheet_sync failed: {e}")
        return {}