from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from ipl_api.cache import get as cache_get, set as cache_set, make_key as cache_key
from ipl_api.config import (
    IPL_SERIES_ID,
    ESPN_FIXTURES_URL_TEMPLATE,
    ESPN_FIXTURES_SCHEDULE_URL_TEMPLATE,
    FIXTURES_CACHE_TTL_SECONDS,
)
from ipl_api.state_from_standings import normalize_team_code


class FixturesScrapeError(Exception):
    """Raised when ESPN fixtures scraping/parsing fails."""
    pass


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


# ── Full IPL 2026 league schedule (all 70 matches) ──
HARDCODED_IPL_2026_FIXTURES: List[Dict[str, Any]] = [
    {"match_id": "RCB-SRH-2026-03-28T19:30:00+05:30", "date": "2026-03-28T19:30:00+05:30", "team1": "Royal Challengers Bengaluru", "team2": "Sunrisers Hyderabad", "team1_code": "RCB", "team2_code": "SRH", "status": "upcoming", "venue": "Bengaluru"},
    {"match_id": "MI-KKR-2026-03-29T19:30:00+05:30", "date": "2026-03-29T19:30:00+05:30", "team1": "Mumbai Indians", "team2": "Kolkata Knight Riders", "team1_code": "MI", "team2_code": "KKR", "status": "upcoming", "venue": "Mumbai"},
    {"match_id": "RR-CSK-2026-03-30T19:30:00+05:30", "date": "2026-03-30T19:30:00+05:30", "team1": "Rajasthan Royals", "team2": "Chennai Super Kings", "team1_code": "RR", "team2_code": "CSK", "status": "upcoming", "venue": "Guwahati"},
    {"match_id": "PBKS-GT-2026-03-31T19:30:00+05:30", "date": "2026-03-31T19:30:00+05:30", "team1": "Punjab Kings", "team2": "Gujarat Titans", "team1_code": "PBKS", "team2_code": "GT", "status": "upcoming", "venue": "New Chandigarh"},
    {"match_id": "LSG-DC-2026-04-01T19:30:00+05:30", "date": "2026-04-01T19:30:00+05:30", "team1": "Lucknow Super Giants", "team2": "Delhi Capitals", "team1_code": "LSG", "team2_code": "DC", "status": "upcoming", "venue": "Lucknow"},
    {"match_id": "KKR-SRH-2026-04-02T19:30:00+05:30", "date": "2026-04-02T19:30:00+05:30", "team1": "Kolkata Knight Riders", "team2": "Sunrisers Hyderabad", "team1_code": "KKR", "team2_code": "SRH", "status": "upcoming", "venue": "Kolkata"},
    {"match_id": "CSK-PBKS-2026-04-03T19:30:00+05:30", "date": "2026-04-03T19:30:00+05:30", "team1": "Chennai Super Kings", "team2": "Punjab Kings", "team1_code": "CSK", "team2_code": "PBKS", "status": "upcoming", "venue": "Chennai"},
    {"match_id": "DC-MI-2026-04-04T15:30:00+05:30", "date": "2026-04-04T15:30:00+05:30", "team1": "Delhi Capitals", "team2": "Mumbai Indians", "team1_code": "DC", "team2_code": "MI", "status": "upcoming", "venue": "Delhi"},
    {"match_id": "GT-RR-2026-04-04T19:30:00+05:30", "date": "2026-04-04T19:30:00+05:30", "team1": "Gujarat Titans", "team2": "Rajasthan Royals", "team1_code": "GT", "team2_code": "RR", "status": "upcoming", "venue": "Ahmedabad"},
    {"match_id": "SRH-LSG-2026-04-05T15:30:00+05:30", "date": "2026-04-05T15:30:00+05:30", "team1": "Sunrisers Hyderabad", "team2": "Lucknow Super Giants", "team1_code": "SRH", "team2_code": "LSG", "status": "upcoming", "venue": "Hyderabad"},
    {"match_id": "RCB-CSK-2026-04-05T19:30:00+05:30", "date": "2026-04-05T19:30:00+05:30", "team1": "Royal Challengers Bengaluru", "team2": "Chennai Super Kings", "team1_code": "RCB", "team2_code": "CSK", "status": "upcoming", "venue": "Bengaluru"},
    {"match_id": "KKR-PBKS-2026-04-06T19:30:00+05:30", "date": "2026-04-06T19:30:00+05:30", "team1": "Kolkata Knight Riders", "team2": "Punjab Kings", "team1_code": "KKR", "team2_code": "PBKS", "status": "upcoming", "venue": "Kolkata"},
    {"match_id": "RR-MI-2026-04-07T19:30:00+05:30", "date": "2026-03-07T19:30:00+05:30", "team1": "Rajasthan Royals", "team2": "Mumbai Indians", "team1_code": "RR", "team2_code": "MI", "status": "upcoming", "venue": "Guwahati"},
    {"match_id": "DC-GT-2026-04-08T19:30:00+05:30", "date": "2026-04-08T19:30:00+05:30", "team1": "Delhi Capitals", "team2": "Gujarat Titans", "team1_code": "DC", "team2_code": "GT", "status": "upcoming", "venue": "Delhi"},
    {"match_id": "KKR-LSG-2026-04-09T19:30:00+05:30", "date": "2026-04-09T19:30:00+05:30", "team1": "Kolkata Knight Riders", "team2": "Lucknow Super Giants", "team1_code": "KKR", "team2_code": "LSG", "status": "upcoming", "venue": "Kolkata"},
    {"match_id": "RR-RCB-2026-04-10T19:30:00+05:30", "date": "2026-04-10T19:30:00+05:30", "team1": "Rajasthan Royals", "team2": "Royal Challengers Bengaluru", "team1_code": "RR", "team2_code": "RCB", "status": "upcoming", "venue": "Guwahati"},
    {"match_id": "PBKS-SRH-2026-04-11T15:30:00+05:30", "date": "2026-04-11T15:30:00+05:30", "team1": "Punjab Kings", "team2": "Sunrisers Hyderabad", "team1_code": "PBKS", "team2_code": "SRH", "status": "upcoming", "venue": "New Chandigarh"},
    {"match_id": "CSK-DC-2026-04-11T19:30:00+05:30", "date": "2026-04-11T19:30:00+05:30", "team1": "Chennai Super Kings", "team2": "Delhi Capitals", "team1_code": "CSK", "team2_code": "DC", "status": "upcoming", "venue": "Chennai"},
    {"match_id": "LSG-GT-2026-04-12T15:30:00+05:30", "date": "2026-04-12T15:30:00+05:30", "team1": "Lucknow Super Giants", "team2": "Gujarat Titans", "team1_code": "LSG", "team2_code": "GT", "status": "upcoming", "venue": "Lucknow"},
    {"match_id": "MI-RCB-2026-04-12T19:30:00+05:30", "date": "2026-04-12T19:30:00+05:30", "team1": "Mumbai Indians", "team2": "Royal Challengers Bengaluru", "team1_code": "MI", "team2_code": "RCB", "status": "upcoming", "venue": "Mumbai"},
    {"match_id": "SRH-RR-2026-04-13T19:30:00+05:30", "date": "2026-04-13T19:30:00+05:30", "team1": "Sunrisers Hyderabad", "team2": "Rajasthan Royals", "team1_code": "SRH", "team2_code": "RR", "status": "upcoming", "venue": "Hyderabad"},
    {"match_id": "CSK-KKR-2026-04-14T19:30:00+05:30", "date": "2026-04-14T19:30:00+05:30", "team1": "Chennai Super Kings", "team2": "Kolkata Knight Riders", "team1_code": "CSK", "team2_code": "KKR", "status": "upcoming", "venue": "Chennai"},
    {"match_id": "RCB-LSG-2026-04-15T19:30:00+05:30", "date": "2026-04-15T19:30:00+05:30", "team1": "Royal Challengers Bengaluru", "team2": "Lucknow Super Giants", "team1_code": "RCB", "team2_code": "LSG", "status": "upcoming", "venue": "Bengaluru"},
    {"match_id": "MI-PBKS-2026-04-16T19:30:00+05:30", "date": "2026-04-16T19:30:00+05:30", "team1": "Mumbai Indians", "team2": "Punjab Kings", "team1_code": "MI", "team2_code": "PBKS", "status": "upcoming", "venue": "Mumbai"},
    {"match_id": "GT-KKR-2026-04-17T19:30:00+05:30", "date": "2026-04-17T19:30:00+05:30", "team1": "Gujarat Titans", "team2": "Kolkata Knight Riders", "team1_code": "GT", "team2_code": "KKR", "status": "upcoming", "venue": "Ahmedabad"},
    {"match_id": "RCB-DC-2026-04-18T15:30:00+05:30", "date": "2026-04-18T15:30:00+05:30", "team1": "Royal Challengers Bengaluru", "team2": "Delhi Capitals", "team1_code": "RCB", "team2_code": "DC", "status": "upcoming", "venue": "Bengaluru"},
    {"match_id": "SRH-CSK-2026-04-18T19:30:00+05:30", "date": "2026-04-18T19:30:00+05:30", "team1": "Sunrisers Hyderabad", "team2": "Chennai Super Kings", "team1_code": "SRH", "team2_code": "CSK", "status": "upcoming", "venue": "Hyderabad"},
    {"match_id": "KKR-RR-2026-04-19T15:30:00+05:30", "date": "2026-04-19T15:30:00+05:30", "team1": "Kolkata Knight Riders", "team2": "Rajasthan Royals", "team1_code": "KKR", "team2_code": "RR", "status": "upcoming", "venue": "Kolkata"},
    {"match_id": "PBKS-LSG-2026-04-19T19:30:00+05:30", "date": "2026-04-19T19:30:00+05:30", "team1": "Punjab Kings", "team2": "Lucknow Super Giants", "team1_code": "PBKS", "team2_code": "LSG", "status": "upcoming", "venue": "New Chandigarh"},
    {"match_id": "GT-MI-2026-04-20T19:30:00+05:30", "date": "2026-04-20T19:30:00+05:30", "team1": "Gujarat Titans", "team2": "Mumbai Indians", "team1_code": "GT", "team2_code": "MI", "status": "upcoming", "venue": "Ahmedabad"},
    {"match_id": "SRH-DC-2026-04-21T19:30:00+05:30", "date": "2026-04-21T19:30:00+05:30", "team1": "Sunrisers Hyderabad", "team2": "Delhi Capitals", "team1_code": "SRH", "team2_code": "DC", "status": "upcoming", "venue": "Hyderabad"},
    {"match_id": "LSG-RR-2026-04-22T19:30:00+05:30", "date": "2026-04-22T19:30:00+05:30", "team1": "Lucknow Super Giants", "team2": "Rajasthan Royals", "team1_code": "LSG", "team2_code": "RR", "status": "upcoming", "venue": "Lucknow"},
    {"match_id": "MI-CSK-2026-04-23T19:30:00+05:30", "date": "2026-04-23T19:30:00+05:30", "team1": "Mumbai Indians", "team2": "Chennai Super Kings", "team1_code": "MI", "team2_code": "CSK", "status": "upcoming", "venue": "Mumbai"},
    {"match_id": "RCB-GT-2026-04-24T19:30:00+05:30", "date": "2026-04-24T19:30:00+05:30", "team1": "Royal Challengers Bengaluru", "team2": "Gujarat Titans", "team1_code": "RCB", "team2_code": "GT", "status": "upcoming", "venue": "Bengaluru"},
    {"match_id": "DC-PBKS-2026-04-25T15:30:00+05:30", "date": "2026-04-25T15:30:00+05:30", "team1": "Delhi Capitals", "team2": "Punjab Kings", "team1_code": "DC", "team2_code": "PBKS", "status": "upcoming", "venue": "Delhi"},
    {"match_id": "RR-SRH-2026-04-25T19:30:00+05:30", "date": "2026-04-25T19:30:00+05:30", "team1": "Rajasthan Royals", "team2": "Sunrisers Hyderabad", "team1_code": "RR", "team2_code": "SRH", "status": "upcoming", "venue": "Jaipur"},
    {"match_id": "GT-CSK-2026-04-26T15:30:00+05:30", "date": "2026-04-26T15:30:00+05:30", "team1": "Gujarat Titans", "team2": "Chennai Super Kings", "team1_code": "GT", "team2_code": "CSK", "status": "upcoming", "venue": "Ahmedabad"},
    {"match_id": "LSG-KKR-2026-04-26T19:30:00+05:30", "date": "2026-04-26T19:30:00+05:30", "team1": "Lucknow Super Giants", "team2": "Kolkata Knight Riders", "team1_code": "LSG", "team2_code": "KKR", "status": "upcoming", "venue": "Lucknow"},
    {"match_id": "DC-RCB-2026-04-27T19:30:00+05:30", "date": "2026-04-27T19:30:00+05:30", "team1": "Delhi Capitals", "team2": "Royal Challengers Bengaluru", "team1_code": "DC", "team2_code": "RCB", "status": "upcoming", "venue": "Delhi"},
    {"match_id": "PBKS-RR-2026-04-28T19:30:00+05:30", "date": "2026-04-28T19:30:00+05:30", "team1": "Punjab Kings", "team2": "Rajasthan Royals", "team1_code": "PBKS", "team2_code": "RR", "status": "upcoming", "venue": "New Chandigarh"},
    {"match_id": "MI-SRH-2026-04-29T19:30:00+05:30", "date": "2026-04-29T19:30:00+05:30", "team1": "Mumbai Indians", "team2": "Sunrisers Hyderabad", "team1_code": "MI", "team2_code": "SRH", "status": "upcoming", "venue": "Mumbai"},
    {"match_id": "GT-RCB-2026-04-30T19:30:00+05:30", "date": "2026-04-30T19:30:00+05:30", "team1": "Gujarat Titans", "team2": "Royal Challengers Bengaluru", "team1_code": "GT", "team2_code": "RCB", "status": "upcoming", "venue": "Ahmedabad"},
    {"match_id": "RR-DC-2026-05-01T19:30:00+05:30", "date": "2026-05-01T19:30:00+05:30", "team1": "Rajasthan Royals", "team2": "Delhi Capitals", "team1_code": "RR", "team2_code": "DC", "status": "upcoming", "venue": "Jaipur"},
    {"match_id": "CSK-MI-2026-05-02T19:30:00+05:30", "date": "2026-05-02T19:30:00+05:30", "team1": "Chennai Super Kings", "team2": "Mumbai Indians", "team1_code": "CSK", "team2_code": "MI", "status": "upcoming", "venue": "Chennai"},
    {"match_id": "SRH-KKR-2026-05-03T15:30:00+05:30", "date": "2026-05-03T15:30:00+05:30", "team1": "Sunrisers Hyderabad", "team2": "Kolkata Knight Riders", "team1_code": "SRH", "team2_code": "KKR", "status": "upcoming", "venue": "Hyderabad"},
    {"match_id": "GT-PBKS-2026-05-03T19:30:00+05:30", "date": "2026-05-03T19:30:00+05:30", "team1": "Gujarat Titans", "team2": "Punjab Kings", "team1_code": "GT", "team2_code": "PBKS", "status": "upcoming", "venue": "Ahmedabad"},
    {"match_id": "MI-LSG-2026-05-04T19:30:00+05:30", "date": "2026-05-04T19:30:00+05:30", "team1": "Mumbai Indians", "team2": "Lucknow Super Giants", "team1_code": "MI", "team2_code": "LSG", "status": "upcoming", "venue": "Mumbai"},
    {"match_id": "DC-CSK-2026-05-05T19:30:00+05:30", "date": "2026-05-05T19:30:00+05:30", "team1": "Delhi Capitals", "team2": "Chennai Super Kings", "team1_code": "DC", "team2_code": "CSK", "status": "upcoming", "venue": "Delhi"},
    {"match_id": "SRH-PBKS-2026-05-06T19:30:00+05:30", "date": "2026-05-06T19:30:00+05:30", "team1": "Sunrisers Hyderabad", "team2": "Punjab Kings", "team1_code": "SRH", "team2_code": "PBKS", "status": "upcoming", "venue": "Hyderabad"},
    {"match_id": "LSG-RCB-2026-05-07T19:30:00+05:30", "date": "2026-05-07T19:30:00+05:30", "team1": "Lucknow Super Giants", "team2": "Royal Challengers Bengaluru", "team1_code": "LSG", "team2_code": "RCB", "status": "upcoming", "venue": "Lucknow"},
    {"match_id": "DC-KKR-2026-05-08T19:30:00+05:30", "date": "2026-05-08T19:30:00+05:30", "team1": "Delhi Capitals", "team2": "Kolkata Knight Riders", "team1_code": "DC", "team2_code": "KKR", "status": "upcoming", "venue": "Delhi"},
    {"match_id": "RR-GT-2026-05-09T19:30:00+05:30", "date": "2026-05-09T19:30:00+05:30", "team1": "Rajasthan Royals", "team2": "Gujarat Titans", "team1_code": "RR", "team2_code": "GT", "status": "upcoming", "venue": "Jaipur"},
    {"match_id": "CSK-LSG-2026-05-10T15:30:00+05:30", "date": "2026-05-10T15:30:00+05:30", "team1": "Chennai Super Kings", "team2": "Lucknow Super Giants", "team1_code": "CSK", "team2_code": "LSG", "status": "upcoming", "venue": "Chennai"},
    {"match_id": "RCB-MI-2026-05-10T19:30:00+05:30", "date": "2026-05-10T19:30:00+05:30", "team1": "Royal Challengers Bengaluru", "team2": "Mumbai Indians", "team1_code": "RCB", "team2_code": "MI", "status": "upcoming", "venue": "Raipur"},
    {"match_id": "PBKS-DC-2026-05-11T19:30:00+05:30", "date": "2026-05-11T19:30:00+05:30", "team1": "Punjab Kings", "team2": "Delhi Capitals", "team1_code": "PBKS", "team2_code": "DC", "status": "upcoming", "venue": "Dharamshala"},
    {"match_id": "GT-SRH-2026-05-12T19:30:00+05:30", "date": "2026-05-12T19:30:00+05:30", "team1": "Gujarat Titans", "team2": "Sunrisers Hyderabad", "team1_code": "GT", "team2_code": "SRH", "status": "upcoming", "venue": "Ahmedabad"},
    {"match_id": "RCB-KKR-2026-05-13T19:30:00+05:30", "date": "2026-05-13T19:30:00+05:30", "team1": "Royal Challengers Bengaluru", "team2": "Kolkata Knight Riders", "team1_code": "RCB", "team2_code": "KKR", "status": "upcoming", "venue": "Raipur"},
    {"match_id": "PBKS-MI-2026-05-14T19:30:00+05:30", "date": "2026-05-14T19:30:00+05:30", "team1": "Punjab Kings", "team2": "Mumbai Indians", "team1_code": "PBKS", "team2_code": "MI", "status": "upcoming", "venue": "Dharamshala"},
    {"match_id": "LSG-CSK-2026-05-15T19:30:00+05:30", "date": "2026-05-15T19:30:00+05:30", "team1": "Lucknow Super Giants", "team2": "Chennai Super Kings", "team1_code": "LSG", "team2_code": "CSK", "status": "upcoming", "venue": "Lucknow"},
    {"match_id": "KKR-GT-2026-05-16T19:30:00+05:30", "date": "2026-05-16T19:30:00+05:30", "team1": "Kolkata Knight Riders", "team2": "Gujarat Titans", "team1_code": "KKR", "team2_code": "GT", "status": "upcoming", "venue": "Kolkata"},
    {"match_id": "PBKS-RCB-2026-05-17T15:30:00+05:30", "date": "2026-05-17T15:30:00+05:30", "team1": "Punjab Kings", "team2": "Royal Challengers Bengaluru", "team1_code": "PBKS", "team2_code": "RCB", "status": "upcoming", "venue": "Dharamshala"},
    {"match_id": "DC-RR-2026-05-17T19:30:00+05:30", "date": "2026-05-17T19:30:00+05:30", "team1": "Delhi Capitals", "team2": "Rajasthan Royals", "team1_code": "DC", "team2_code": "RR", "status": "upcoming", "venue": "Delhi"},
    {"match_id": "CSK-SRH-2026-05-18T19:30:00+05:30", "date": "2026-05-18T19:30:00+05:30", "team1": "Chennai Super Kings", "team2": "Sunrisers Hyderabad", "team1_code": "CSK", "team2_code": "SRH", "status": "upcoming", "venue": "Chennai"},
    {"match_id": "RR-LSG-2026-05-19T19:30:00+05:30", "date": "2026-05-19T19:30:00+05:30", "team1": "Rajasthan Royals", "team2": "Lucknow Super Giants", "team1_code": "RR", "team2_code": "LSG", "status": "upcoming", "venue": "Jaipur"},
    {"match_id": "KKR-MI-2026-05-20T19:30:00+05:30", "date": "2026-05-20T19:30:00+05:30", "team1": "Kolkata Knight Riders", "team2": "Mumbai Indians", "team1_code": "KKR", "team2_code": "MI", "status": "upcoming", "venue": "Kolkata"},
    {"match_id": "CSK-GT-2026-05-21T19:30:00+05:30", "date": "2026-05-21T19:30:00+05:30", "team1": "Chennai Super Kings", "team2": "Gujarat Titans", "team1_code": "CSK", "team2_code": "GT", "status": "upcoming", "venue": "Chennai"},
    {"match_id": "SRH-RCB-2026-05-22T19:30:00+05:30", "date": "2026-05-22T19:30:00+05:30", "team1": "Sunrisers Hyderabad", "team2": "Royal Challengers Bengaluru", "team1_code": "SRH", "team2_code": "RCB", "status": "upcoming", "venue": "Hyderabad"},
    {"match_id": "LSG-PBKS-2026-05-23T19:30:00+05:30", "date": "2026-05-23T19:30:00+05:30", "team1": "Lucknow Super Giants", "team2": "Punjab Kings", "team1_code": "LSG", "team2_code": "PBKS", "status": "upcoming", "venue": "Lucknow"},
    {"match_id": "MI-RR-2026-05-24T15:30:00+05:30", "date": "2026-05-24T15:30:00+05:30", "team1": "Mumbai Indians", "team2": "Rajasthan Royals", "team1_code": "MI", "team2_code": "RR", "status": "upcoming", "venue": "Mumbai"},
    {"match_id": "KKR-DC-2026-05-24T19:30:00+05:30", "date": "2026-05-24T19:30:00+05:30", "team1": "Kolkata Knight Riders", "team2": "Delhi Capitals", "team1_code": "KKR", "team2_code": "DC", "status": "upcoming", "venue": "Kolkata"},
]


def _extract_next_data_json(html: str) -> Dict[str, Any]:
    m = re.search(
        r"<script[^>]*\bid=['\"]__NEXT_DATA__['\"][^>]*>\s*(\{.*?\})\s*</script>",
        html,
        flags=re.DOTALL,
    )
    if not m:
        raise FixturesScrapeError("Could not find __NEXT_DATA__ in ESPN page HTML")
    raw = m.group(1).strip()
    try:
        return json.loads(raw)
    except Exception as e:
        raise FixturesScrapeError(f"Failed to parse __NEXT_DATA__ JSON: {e}") from e


def _walk(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v)


def _pick_competition_nodes(next_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    nodes: List[Dict[str, Any]] = []
    for d in _walk(next_data):
        if not isinstance(d, dict):
            continue
        if isinstance(d.get("competitors"), list) and "status" in d:
            nodes.append(d)
            continue
        comps = d.get("competitions")
        if isinstance(comps, list):
            for c in comps:
                if isinstance(c, dict) and isinstance(c.get("competitors"), list) and "status" in c:
                    nodes.append(c)
    return nodes


def _get_team_names_from_competitors(comp: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    comps = comp.get("competitors")
    if not isinstance(comps, list) or len(comps) < 2:
        return None

    def team_name(x: Dict[str, Any]) -> str:
        t = x.get("team") or {}
        return (t.get("displayName") or t.get("name") or "").strip()

    a = team_name(comps[0])
    b = team_name(comps[1])
    if not a or not b:
        return None
    return a, b


def _get_status_info(comp: Dict[str, Any]) -> Tuple[str, str, str]:
    """Return (name, state, detail) from the status block."""
    st = comp.get("status") or {}
    t = st.get("type") or {}
    return (
        str(t.get("name") or "").strip().upper(),
        str(t.get("state") or "").strip().lower(),
        str(t.get("detail") or "").strip().lower(),
    )


def _resolve_fixture_status(comp: Dict[str, Any]) -> str:
    """
    Return 'completed', 'live', or 'upcoming' based on ESPN competition node.
    ESPN uses state='post' for completed, state='in' for live, state='pre' for upcoming.
    """
    name, state, detail = _get_status_info(comp)

    if state == "post":
        return "completed"
    if state == "in" or "live" in detail or "in progress" in detail:
        return "live"
    if state in ("pre", "preview"):
        return "upcoming"
    if "FINAL" in name or "RESULT" in name or "COMPLETE" in name:
        return "completed"
    if "SCHEDULED" in name or "FIXTURE" in name:
        return "upcoming"
    # Fallback: check if match date has passed
    date_str = comp.get("date")
    if date_str:
        try:
            match_dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            if match_dt < datetime.now(match_dt.tzinfo):
                return "completed"
        except Exception:
            pass
    return "upcoming"


def _get_winner_code(comp: Dict[str, Any]) -> Optional[str]:
    """
    Extract the winner's team code from a completed match competition node.
    ESPN marks the winner competitor with winner=True or a note like 'W'.
    """
    comps = comp.get("competitors")
    if not isinstance(comps, list):
        return None

    for c in comps:
        if not isinstance(c, dict):
            continue
        # ESPN sets winner=True on the winning competitor
        if c.get("winner") is True:
            team = c.get("team") or {}
            name = (team.get("displayName") or team.get("name") or "").strip()
            if name:
                _, code = _team_name_to_code_and_name(name)
                return code or None
        # Some ESPN pages use a "note" field
        note = str(c.get("note") or "").strip().upper()
        if note == "W" or note == "WON":
            team = c.get("team") or {}
            name = (team.get("displayName") or team.get("name") or "").strip()
            if name:
                _, code = _team_name_to_code_and_name(name)
                return code or None

    return None


def _parse_start_time_utc(comp: Dict[str, Any]) -> Optional[str]:
    dt = comp.get("date")
    if not dt or not isinstance(dt, str):
        return None
    return dt.strip() or None


def _team_name_to_code_and_name(name: str) -> Tuple[str, str]:
    cleaned = " ".join(name.split()).strip()
    code = normalize_team_code(cleaned)
    return cleaned, code


def _to_fixture_dict(
    team1_name: str,
    team2_name: str,
    date_iso: Optional[str],
    venue: Optional[str],
    status: str = "upcoming",
    winner: Optional[str] = None,
    match_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    team1_name, team1_code = _team_name_to_code_and_name(team1_name)
    team2_name, team2_code = _team_name_to_code_and_name(team2_name)
    if not team1_name or not team2_name or not team1_code or not team2_code:
        return None
    if team1_code == team2_code:
        return None
    if not match_id:
        match_id = f"{team1_code}-{team2_code}-{date_iso or 'unknown'}"

    result: Dict[str, Any] = {
        "match_id": str(match_id),
        "date": date_iso or "",
        "team1": team1_name,
        "team2": team2_name,
        "team1_code": team1_code,
        "team2_code": team2_code,
        "status": status,
        "venue": venue,
    }
    # Only include winner when we actually know it
    if winner:
        result["winner"] = winner
    return result


def _extract_from_next_data(next_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract ALL matches (upcoming + live + completed) from ESPN __NEXT_DATA__.
    Previously this only collected upcoming matches — that was the root cause of
    completed matches never appearing in the schedule view.
    """
    comps = _pick_competition_nodes(next_data)
    fixtures: List[Dict[str, Any]] = []
    seen = set()

    for comp in comps:
        names = _get_team_names_from_competitors(comp)
        if not names:
            continue
        t1_name, t2_name = names

        status = _resolve_fixture_status(comp)
        winner = _get_winner_code(comp) if status == "completed" else None

        venue = None
        venue_obj = comp.get("venue")
        if isinstance(venue_obj, dict):
            venue = venue_obj.get("fullName") or venue_obj.get("name")

        match_id = comp.get("id")
        item = _to_fixture_dict(
            team1_name=t1_name,
            team2_name=t2_name,
            date_iso=_parse_start_time_utc(comp),
            venue=venue,
            status=status,
            winner=winner,
            match_id=str(match_id) if match_id is not None else None,
        )
        if not item:
            continue
        key = item["match_id"]
        if key in seen:
            continue
        seen.add(key)
        fixtures.append(item)

    fixtures.sort(key=lambda x: (x.get("date") or "", x["team1_code"], x["team2_code"]))
    return fixtures


def _scrape_url(url: str, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    print(f"[DEBUG] Fetching: {url}", file=sys.stderr)
    with requests.Session() as s:
        r = s.get(url, timeout=20, headers=headers, allow_redirects=True)
        r.raise_for_status()
        html = r.text
    print(f"[DEBUG] HTML length: {len(html)}", file=sys.stderr)
    try:
        next_data = _extract_next_data_json(html)
        fixtures = _extract_from_next_data(next_data)
        print(f"[DEBUG] Parsed via __NEXT_DATA__: {len(fixtures)} fixtures", file=sys.stderr)
        return fixtures
    except FixturesScrapeError as e:
        print(f"[DEBUG] __NEXT_DATA__ failed ({e})", file=sys.stderr)
        return []


def _mark_past_fixtures_completed(fixtures: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    For fixtures that are still marked 'upcoming' but whose scheduled date has
    already passed by more than 4 hours (giving time for a match to finish),
    upgrade their status to 'completed'.

    This is a last-resort fallback for when ESPN scraping fails entirely and we
    fall back to the hardcoded list — those always have status='upcoming'.
    """
    now = datetime.utcnow()
    result = []
    for f in fixtures:
        f = dict(f)
        if f.get("status") == "upcoming" and f.get("date"):
            try:
                # Parse ISO date (may have +05:30 offset)
                date_str = f["date"]
                # Convert offset-aware to UTC for comparison
                dt = datetime.fromisoformat(date_str)
                # Convert to UTC naive
                import datetime as dt_module
                if dt.tzinfo is not None:
                    utc_dt = dt.utctimetuple()
                    import calendar
                    utc_ts = calendar.timegm(utc_dt)
                    utc_naive = datetime.utcfromtimestamp(utc_ts)
                else:
                    utc_naive = dt
                # If match was scheduled >4 hours ago, mark as completed
                hours_past = (now - utc_naive).total_seconds() / 3600
                if hours_past > 4:
                    f["status"] = "completed"
            except Exception:
                pass
        result.append(f)
    return result


def fetch_espn_fixtures(season: int, *, use_cache: bool = True) -> Dict[str, Any]:
    if season <= 0:
        raise ValueError("season must be a positive integer")

    ckey = cache_key("fixtures", str(season))
    if use_cache:
        cached = cache_get(ckey)
        if cached is not None:
            return cached

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; IPL-NRR-Sim/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IN,en;q=0.9",
        "Connection": "keep-alive",
    }

    scraped_fixtures: List[Dict[str, Any]] = []
    url_used = ESPN_FIXTURES_SCHEDULE_URL_TEMPLATE.format(series_id=IPL_SERIES_ID, season=season)

    for url in [
        ESPN_FIXTURES_SCHEDULE_URL_TEMPLATE.format(series_id=IPL_SERIES_ID, season=season),
        ESPN_FIXTURES_URL_TEMPLATE.format(series_id=IPL_SERIES_ID, season=season),
    ]:
        try:
            result = _scrape_url(url, headers)
            if len(result) > len(scraped_fixtures):
                scraped_fixtures = result
                url_used = url
        except Exception as e:
            print(f"[DEBUG] Scrape failed for {url}: {e}", file=sys.stderr)

    # Build a lookup of scraped fixtures by a canonical team-pair key
    # so we can correctly override hardcoded status with live status
    scraped_by_teams: Dict[str, Dict[str, Any]] = {}
    for f in scraped_fixtures:
        key = f"{f['team1_code']}-{f['team2_code']}"
        scraped_by_teams[key] = f
        # Also index reverse direction in case ESPN flips home/away
        scraped_by_teams[f"{f['team2_code']}-{f['team1_code']}"] = f

    # Merge: scraped data first (has live/completed status + winner),
    # hardcoded fills in any matches ESPN hasn't published yet
    seen_ids: set = {f["match_id"] for f in scraped_fixtures}
    fixtures: List[Dict[str, Any]] = list(scraped_fixtures)

    added_from_hardcoded = 0
    for hf in HARDCODED_IPL_2026_FIXTURES:
        if hf["match_id"] in seen_ids:
            continue
        # Check if we have a scraped version under a different ID
        pair_key = f"{hf['team1_code']}-{hf['team2_code']}"
        if pair_key in scraped_by_teams:
            # Already captured under a different ID — skip duplicate
            continue
        seen_ids.add(hf["match_id"])
        fixtures.append(dict(hf))
        added_from_hardcoded += 1

    fixtures.sort(key=lambda x: (x.get("date") or "", x["team1_code"], x["team2_code"]))

    # Apply time-based completion fallback for fixtures that ESPN didn't return at all
    fixtures = _mark_past_fixtures_completed(fixtures)

    print(
        f"[DEBUG] Scraped: {len(scraped_fixtures)}, hardcoded added: {added_from_hardcoded}, total: {len(fixtures)}",
        file=sys.stderr,
    )

    resp = {
        "season": season,
        "source": "espn",
        "url_used": url_used,
        "last_updated_utc": _utc_now_iso(),
        "fixtures": fixtures,
        "fixtures_count": len(fixtures),
    }

    if use_cache:
        cache_set(ckey, resp, FIXTURES_CACHE_TTL_SECONDS)

    return resp