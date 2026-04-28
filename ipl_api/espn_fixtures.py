from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from ipl_api.config import (
    IPL_SERIES_ID,
    ESPN_FIXTURES_URL_TEMPLATE,
    ESPN_FIXTURES_SCHEDULE_URL_TEMPLATE,
    FIXTURES_CACHE_TTL_SECONDS,
)
from ipl_api.state_from_standings import normalize_team_code

from ipl_api.cricbuzz_fixtures import fetch_cricbuzz_ipl_results

class FixturesScrapeError(Exception):
    """Raised when ESPN fixtures scraping/parsing fails."""
    pass


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


# ── Full IPL 2026 league schedule (all 70 matches) ──
HARDCODED_IPL_2026_FIXTURES: List[Dict[str, Any]] = [
    {"match_id": "RCB-SRH-2026-03-28T19:30:00+05:30", "date": "2026-03-28T19:30:00+05:30", "team1_code": "RCB", "team2_code": "SRH", "team1": "Royal Challengers Bengaluru", "team2": "Sunrisers Hyderabad", "status": "completed", "venue": "Bengaluru", "winner_code": "RCB", "result": "Royal Challengers Bengaluru won by 6 wkts"},
    {"match_id": "MI-KKR-2026-03-29T19:30:00+05:30", "date": "2026-03-29T19:30:00+05:30", "team1_code": "MI", "team2_code": "KKR", "team1": "Mumbai Indians", "team2": "Kolkata Knight Riders", "status": "completed", "venue": "Mumbai", "winner_code": "MI", "result": "Mumbai Indians won by 6 wkts"},
    {"match_id": "RR-CSK-2026-03-30T19:30:00+05:30", "date": "2026-03-30T19:30:00+05:30", "team1_code": "RR", "team2_code": "CSK", "team1": "Rajasthan Royals", "team2": "Chennai Super Kings", "status": "completed", "venue": "Guwahati", "winner_code": "RR", "result": "Rajasthan Royals won by 8 wkts"},
    {"match_id": "PBKS-GT-2026-03-31T19:30:00+05:30", "date": "2026-03-31T19:30:00+05:30", "team1_code": "PBKS", "team2_code": "GT", "team1": "Punjab Kings", "team2": "Gujarat Titans", "status": "completed", "venue": "New Chandigarh", "winner_code": "PBKS", "result": "Punjab Kings won by 3 wkts"},
    {"match_id": "LSG-DC-2026-04-01T19:30:00+05:30", "date": "2026-04-01T19:30:00+05:30", "team1_code": "LSG", "team2_code": "DC", "team1": "Lucknow Super Giants", "team2": "Delhi Capitals", "status": "completed", "venue": "Lucknow", "winner_code": "DC", "result": "Delhi Capitals won by 6 wkts"},
    {"match_id": "KKR-SRH-2026-04-02T19:30:00+05:30", "date": "2026-04-02T19:30:00+05:30", "team1_code": "KKR", "team2_code": "SRH", "team1": "Kolkata Knight Riders", "team2": "Sunrisers Hyderabad", "status": "completed", "venue": "Kolkata", "winner_code": "SRH", "result": "Sunrisers Hyderabad won by 65 runs"},
    {"match_id": "CSK-PBKS-2026-04-03T19:30:00+05:30", "date": "2026-04-03T19:30:00+05:30", "team1_code": "CSK", "team2_code": "PBKS", "team1": "Chennai Super Kings", "team2": "Punjab Kings", "status": "completed", "venue": "Chennai", "winner_code": "PBKS", "result": "Punjab Kings won by 5 wkts"},
    {"match_id": "DC-MI-2026-04-04T15:30:00+05:30", "date": "2026-04-04T15:30:00+05:30", "team1_code": "DC", "team2_code": "MI", "team1": "Delhi Capitals", "team2": "Mumbai Indians", "status": "completed", "venue": "Delhi", "winner_code": "DC", "result": "Delhi Capitals won by 6 wkts"},
    {"match_id": "GT-RR-2026-04-04T19:30:00+05:30", "date": "2026-04-04T19:30:00+05:30", "team1_code": "GT", "team2_code": "RR", "team1": "Gujarat Titans", "team2": "Rajasthan Royals", "status": "completed", "venue": "Ahmedabad", "winner_code": "RR", "result": "Rajasthan Royals won by 6 runs"},
    {"match_id": "SRH-LSG-2026-04-05T15:30:00+05:30", "date": "2026-04-05T15:30:00+05:30", "team1_code": "SRH", "team2_code": "LSG", "team1": "Sunrisers Hyderabad", "team2": "Lucknow Super Giants", "status": "completed", "venue": "Hyderabad", "winner_code": "LSG", "result": "Lucknow Super Giants won by 5 wkts"},
    {"match_id": "RCB-CSK-2026-04-05T19:30:00+05:30", "date": "2026-04-05T19:30:00+05:30", "team1_code": "RCB", "team2_code": "CSK", "team1": "Royal Challengers Bengaluru", "team2": "Chennai Super Kings", "status": "completed", "venue": "Bengaluru", "winner_code": "RCB", "result": "Royal Challengers Bengaluru won by 43 runs"},
    {"match_id": "KKR-PBKS-2026-04-06T19:30:00+05:30", "date": "2026-04-06T19:30:00+05:30", "team1_code": "KKR", "team2_code": "PBKS", "team1": "Kolkata Knight Riders", "team2": "Punjab Kings", "status": "no_result", "venue": "Kolkata", "result": "No Result"},
    {"match_id": "RR-MI-2026-04-07T19:30:00+05:30", "date": "2026-04-07T19:30:00+05:30", "team1_code": "RR", "team2_code": "MI", "team1": "Rajasthan Royals", "team2": "Mumbai Indians", "status": "completed", "venue": "Guwahati", "winner_code": "RR", "result": "Rajasthan Royals won by 27 runs"},
    {"match_id": "DC-GT-2026-04-08T19:30:00+05:30", "date": "2026-04-08T19:30:00+05:30", "team1_code": "DC", "team2_code": "GT", "team1": "Delhi Capitals", "team2": "Gujarat Titans", "status": "completed", "venue": "Delhi", "winner_code": "GT", "result": "Gujarat Titans won by 1 run"},
    {"match_id": "KKR-LSG-2026-04-09T19:30:00+05:30", "date": "2026-04-09T19:30:00+05:30", "team1_code": "KKR", "team2_code": "LSG", "team1": "Kolkata Knight Riders", "team2": "Lucknow Super Giants", "status": "completed", "venue": "Kolkata", "winner_code": "LSG", "result": "Lucknow Super Giants won by 3 wkts"},
    {"match_id": "RR-RCB-2026-04-10T19:30:00+05:30", "date": "2026-04-10T19:30:00+05:30", "team1_code": "RR", "team2_code": "RCB", "team1": "Rajasthan Royals", "team2": "Royal Challengers Bengaluru", "status": "completed", "venue": "Guwahati", "winner_code": "RR", "result": "Rajasthan Royals won by 6 wkts"},
    {"match_id": "PBKS-SRH-2026-04-11T15:30:00+05:30", "date": "2026-04-11T15:30:00+05:30", "team1_code": "PBKS", "team2_code": "SRH", "team1": "Punjab Kings", "team2": "Sunrisers Hyderabad", "status": "completed", "venue": "New Chandigarh", "winner_code": "PBKS", "result": "Punjab Kings won by 6 wkts"},
    {"match_id": "CSK-DC-2026-04-11T19:30:00+05:30", "date": "2026-04-11T19:30:00+05:30", "team1_code": "CSK", "team2_code": "DC", "team1": "Chennai Super Kings", "team2": "Delhi Capitals", "status": "completed", "venue": "Chennai", "winner_code": "CSK", "result": "Chennai Super Kings won by 23 runs"},
    {"match_id": "LSG-GT-2026-04-12T15:30:00+05:30", "date": "2026-04-12T15:30:00+05:30", "team1_code": "LSG", "team2_code": "GT", "team1": "Lucknow Super Giants", "team2": "Gujarat Titans", "status": "completed", "venue": "Lucknow", "winner_code": "GT", "result": "Gujarat Titans won by 7 wkts"},
    {"match_id": "MI-RCB-2026-04-12T19:30:00+05:30", "date": "2026-04-12T19:30:00+05:30", "team1_code": "MI", "team2_code": "RCB", "team1": "Mumbai Indians", "team2": "Royal Challengers Bengaluru", "status": "completed", "venue": "Mumbai", "winner_code": "RCB", "result": "Royal Challengers Bengaluru won by 18 runs"},
    {"match_id": "SRH-RR-2026-04-13T19:30:00+05:30", "date": "2026-04-13T19:30:00+05:30", "team1_code": "SRH", "team2_code": "RR", "team1": "Sunrisers Hyderabad", "team2": "Rajasthan Royals", "status": "completed", "venue": "Hyderabad", "winner_code": "SRH", "result": "Sunrisers Hyderabad won by 57 runs"},
    {"match_id": "CSK-KKR-2026-04-14T19:30:00+05:30", "date": "2026-04-14T19:30:00+05:30", "team1_code": "CSK", "team2_code": "KKR", "team1": "Chennai Super Kings", "team2": "Kolkata Knight Riders", "status": "completed", "venue": "Chennai", "winner_code": "CSK", "result": "Chennai Super Kings won by 32 runs"},
    {"match_id": "RCB-LSG-2026-04-15T19:30:00+05:30", "date": "2026-04-15T19:30:00+05:30", "team1_code": "RCB", "team2_code": "LSG", "team1": "Royal Challengers Bengaluru", "team2": "Lucknow Super Giants", "status": "completed", "venue": "Bengaluru", "winner_code": "RCB", "result": "Royal Challengers Bengaluru won by 5 wkts"},
    {"match_id": "MI-PBKS-2026-04-16T19:30:00+05:30", "date": "2026-04-16T19:30:00+05:30", "team1_code": "MI", "team2_code": "PBKS", "team1": "Mumbai Indians", "team2": "Punjab Kings", "status": "completed", "venue": "Mumbai", "winner_code": "PBKS", "result": "Punjab Kings won by 7 wkts"},
    {"match_id": "GT-KKR-2026-04-17T19:30:00+05:30", "date": "2026-04-17T19:30:00+05:30", "team1_code": "GT", "team2_code": "KKR", "team1": "Gujarat Titans", "team2": "Kolkata Knight Riders", "status": "completed", "venue": "Ahmedabad", "winner_code": "GT", "result": "Gujarat Titans won by 5 wkts"},
    {"match_id": "RCB-DC-2026-04-18T15:30:00+05:30", "date": "2026-04-18T15:30:00+05:30", "team1_code": "RCB", "team2_code": "DC", "team1": "Royal Challengers Bengaluru", "team2": "Delhi Capitals", "status": "completed", "venue": "Bengaluru", "winner_code": "DC", "result": "Delhi Capitals won by 6 wkts"},
    {"match_id": "SRH-CSK-2026-04-18T19:30:00+05:30", "date": "2026-04-18T19:30:00+05:30", "team1_code": "SRH", "team2_code": "CSK", "team1": "Sunrisers Hyderabad", "team2": "Chennai Super Kings", "status": "completed", "venue": "Hyderabad", "winner_code": "SRH", "result": "Sunrisers Hyderabad won by 10 runs"},
    {"match_id": "KKR-RR-2026-04-19T15:30:00+05:30", "date": "2026-04-19T15:30:00+05:30", "team1_code": "KKR", "team2_code": "RR", "team1": "Kolkata Knight Riders", "team2": "Rajasthan Royals", "status": "completed", "venue": "Kolkata", "winner_code": "KKR", "result": "Kolkata Knight Riders won by 4 wkts"},
    {"match_id": "PBKS-LSG-2026-04-19T19:30:00+05:30", "date": "2026-04-19T19:30:00+05:30", "team1_code": "PBKS", "team2_code": "LSG", "team1": "Punjab Kings", "team2": "Lucknow Super Giants", "status": "completed", "venue": "New Chandigarh", "winner_code": "PBKS", "result": "Punjab Kings won by 54 runs"},
    {"match_id": "GT-MI-2026-04-20T19:30:00+05:30", "date": "2026-04-20T19:30:00+05:30", "team1_code": "GT", "team2_code": "MI", "team1": "Gujarat Titans", "team2": "Mumbai Indians", "status": "completed", "venue": "Ahmedabad", "winner_code": "MI", "result": "Mumbai Indians won by 99 runs"},
    {"match_id": "SRH-DC-2026-04-21T19:30:00+05:30", "date": "2026-04-21T19:30:00+05:30", "team1_code": "SRH", "team2_code": "DC", "team1": "Sunrisers Hyderabad", "team2": "Delhi Capitals", "status": "completed", "venue": "Hyderabad", "winner_code": "SRH", "result": "Sunrisers Hyderabad won by 47 runs"},
    {"match_id": "LSG-RR-2026-04-22T19:30:00+05:30", "date": "2026-04-22T19:30:00+05:30", "team1_code": "LSG", "team2_code": "RR", "team1": "Lucknow Super Giants", "team2": "Rajasthan Royals", "status": "completed", "venue": "Lucknow", "winner_code": "RR", "result": "Rajasthan Royals won by 40 runs"},
    {"match_id": "MI-CSK-2026-04-23T19:30:00+05:30", "date": "2026-04-23T19:30:00+05:30", "team1_code": "MI", "team2_code": "CSK", "team1": "Mumbai Indians", "team2": "Chennai Super Kings", "status": "completed", "venue": "Mumbai", "winner_code": "CSK", "result": "Chennai Super Kings won by 103 runs"},
    {"match_id": "RCB-GT-2026-04-24T19:30:00+05:30", "date": "2026-04-24T19:30:00+05:30", "team1_code": "RCB", "team2_code": "GT", "team1": "Royal Challengers Bengaluru", "team2": "Gujarat Titans", "status": "completed", "venue": "Bengaluru", "winner_code": "RCB", "result": "Royal Challengers Bengaluru won by 5 wkts"},
    {"match_id": "DC-PBKS-2026-04-25T15:30:00+05:30", "date": "2026-04-25T15:30:00+05:30", "team1_code": "DC", "team2_code": "PBKS", "team1": "Delhi Capitals", "team2": "Punjab Kings", "status": "completed", "venue": "Delhi", "winner_code": "PBKS", "result": "Punjab Kings won by 6 wkts"},
    {"match_id": "RR-SRH-2026-04-25T19:30:00+05:30", "date": "2026-04-25T19:30:00+05:30", "team1_code": "RR", "team2_code": "SRH", "team1": "Rajasthan Royals", "team2": "Sunrisers Hyderabad", "status": "completed", "venue": "Jaipur", "winner_code": "SRH", "result": "Sunrisers Hyderabad won by 5 wkts"},
    {"match_id": "GT-CSK-2026-04-26T15:30:00+05:30", "date": "2026-04-26T15:30:00+05:30", "team1_code": "GT", "team2_code": "CSK", "team1": "Gujarat Titans", "team2": "Chennai Super Kings", "status": "completed", "venue": "Chennai", "winner_code": "GT", "result": "Gujarat Titans won by 8 wkts"},
    {"match_id": "LSG-KKR-2026-04-26T19:30:00+05:30", "date": "2026-04-26T19:30:00+05:30", "team1_code": "LSG", "team2_code": "KKR", "team1": "Lucknow Super Giants", "team2": "Kolkata Knight Riders", "status": "no_result", "venue": "Lucknow", "result": "Match tied (KKR won the Super Over)"},
    {"match_id": "DC-RCB-2026-04-27T19:30:00+05:30", "date": "2026-04-27T19:30:00+05:30", "team1_code": "DC", "team2_code": "RCB", "team1": "Delhi Capitals", "team2": "Royal Challengers Bengaluru", "status": "completed", "venue": "Delhi", "winner_code": "RCB", "result": "Royal Challengers Bengaluru won by 9 wkts"},
    {"match_id": "PBKS-RR-2026-04-28T19:30:00+05:30", "date": "2026-04-28T19:30:00+05:30", "team1_code": "PBKS", "team2_code": "RR", "team1": "Punjab Kings", "team2": "Rajasthan Royals", "status": "upcoming", "venue": "New Chandigarh"},
    {"match_id": "MI-SRH-2026-04-29T19:30:00+05:30", "date": "2026-04-29T19:30:00+05:30", "team1_code": "MI", "team2_code": "SRH", "team1": "Mumbai Indians", "team2": "Sunrisers Hyderabad", "status": "upcoming", "venue": "Mumbai"},
    {"match_id": "GT-RCB-2026-04-30T19:30:00+05:30", "date": "2026-04-30T19:30:00+05:30", "team1_code": "GT", "team2_code": "RCB", "team1": "Gujarat Titans", "team2": "Royal Challengers Bengaluru", "status": "upcoming", "venue": "Ahmedabad"},
    {"match_id": "RR-DC-2026-05-01T19:30:00+05:30", "date": "2026-05-01T19:30:00+05:30", "team1_code": "RR", "team2_code": "DC", "team1": "Rajasthan Royals", "team2": "Delhi Capitals", "status": "upcoming", "venue": "Jaipur"},
    {"match_id": "CSK-MI-2026-05-02T19:30:00+05:30", "date": "2026-05-02T19:30:00+05:30", "team1_code": "CSK", "team2_code": "MI", "team1": "Chennai Super Kings", "team2": "Mumbai Indians", "status": "upcoming", "venue": "Chennai"},
    {"match_id": "SRH-KKR-2026-05-03T15:30:00+05:30", "date": "2026-05-03T15:30:00+05:30", "team1_code": "SRH", "team2_code": "KKR", "team1": "Sunrisers Hyderabad", "team2": "Kolkata Knight Riders", "status": "upcoming", "venue": "Hyderabad"},
    {"match_id": "GT-PBKS-2026-05-03T19:30:00+05:30", "date": "2026-05-03T19:30:00+05:30", "team1_code": "GT", "team2_code": "PBKS", "team1": "Gujarat Titans", "team2": "Punjab Kings", "status": "upcoming", "venue": "Ahmedabad"},
    {"match_id": "MI-LSG-2026-05-04T19:30:00+05:30", "date": "2026-05-04T19:30:00+05:30", "team1_code": "MI", "team2_code": "LSG", "team1": "Mumbai Indians", "team2": "Lucknow Super Giants", "status": "upcoming", "venue": "Mumbai"},
    {"match_id": "DC-CSK-2026-05-05T19:30:00+05:30", "date": "2026-05-05T19:30:00+05:30", "team1_code": "DC", "team2_code": "CSK", "team1": "Delhi Capitals", "team2": "Chennai Super Kings", "status": "upcoming", "venue": "Delhi"},
    {"match_id": "SRH-PBKS-2026-05-06T19:30:00+05:30", "date": "2026-05-06T19:30:00+05:30", "team1_code": "SRH", "team2_code": "PBKS", "team1": "Sunrisers Hyderabad", "team2": "Punjab Kings", "status": "upcoming", "venue": "Hyderabad"},
    {"match_id": "LSG-RCB-2026-05-07T19:30:00+05:30", "date": "2026-05-07T19:30:00+05:30", "team1_code": "LSG", "team2_code": "RCB", "team1": "Lucknow Super Giants", "team2": "Royal Challengers Bengaluru", "status": "upcoming", "venue": "Lucknow"},
    {"match_id": "DC-KKR-2026-05-08T19:30:00+05:30", "date": "2026-05-08T19:30:00+05:30", "team1_code": "DC", "team2_code": "KKR", "team1": "Delhi Capitals", "team2": "Kolkata Knight Riders", "status": "upcoming", "venue": "Delhi"},
    {"match_id": "RR-GT-2026-05-09T19:30:00+05:30", "date": "2026-05-09T19:30:00+05:30", "team1_code": "RR", "team2_code": "GT", "team1": "Rajasthan Royals", "team2": "Gujarat Titans", "status": "upcoming", "venue": "Jaipur"},
    {"match_id": "CSK-LSG-2026-05-10T15:30:00+05:30", "date": "2026-05-10T15:30:00+05:30", "team1_code": "CSK", "team2_code": "LSG", "team1": "Chennai Super Kings", "team2": "Lucknow Super Giants", "status": "upcoming", "venue": "Chennai"},
    {"match_id": "RCB-MI-2026-05-10T19:30:00+05:30", "date": "2026-05-10T19:30:00+05:30", "team1_code": "RCB", "team2_code": "MI", "team1": "Royal Challengers Bengaluru", "team2": "Mumbai Indians", "status": "upcoming", "venue": "Raipur"},
    {"match_id": "PBKS-DC-2026-05-11T19:30:00+05:30", "date": "2026-05-11T19:30:00+05:30", "team1_code": "PBKS", "team2_code": "DC", "team1": "Punjab Kings", "team2": "Delhi Capitals", "status": "upcoming", "venue": "Dharamshala"},
    {"match_id": "GT-SRH-2026-05-12T19:30:00+05:30", "date": "2026-05-12T19:30:00+05:30", "team1_code": "GT", "team2_code": "SRH", "team1": "Gujarat Titans", "team2": "Sunrisers Hyderabad", "status": "upcoming", "venue": "Ahmedabad"},
    {"match_id": "RCB-KKR-2026-05-13T19:30:00+05:30", "date": "2026-05-13T19:30:00+05:30", "team1_code": "RCB", "team2_code": "KKR", "team1": "Royal Challengers Bengaluru", "team2": "Kolkata Knight Riders", "status": "upcoming", "venue": "Raipur"},
    {"match_id": "PBKS-MI-2026-05-14T19:30:00+05:30", "date": "2026-05-14T19:30:00+05:30", "team1_code": "PBKS", "team2_code": "MI", "team1": "Punjab Kings", "team2": "Mumbai Indians", "status": "upcoming", "venue": "Dharamshala"},
    {"match_id": "LSG-CSK-2026-05-15T19:30:00+05:30", "date": "2026-05-15T19:30:00+05:30", "team1_code": "LSG", "team2_code": "CSK", "team1": "Lucknow Super Giants", "team2": "Chennai Super Kings", "status": "upcoming", "venue": "Lucknow"},
    {"match_id": "KKR-GT-2026-05-16T19:30:00+05:30", "date": "2026-05-16T19:30:00+05:30", "team1_code": "KKR", "team2_code": "GT", "team1": "Kolkata Knight Riders", "team2": "Gujarat Titans", "status": "upcoming", "venue": "Kolkata"},
    {"match_id": "PBKS-RCB-2026-05-17T15:30:00+05:30", "date": "2026-05-17T15:30:00+05:30", "team1_code": "PBKS", "team2_code": "RCB", "team1": "Punjab Kings", "team2": "Royal Challengers Bengaluru", "status": "upcoming", "venue": "Dharamshala"},
    {"match_id": "DC-RR-2026-05-17T19:30:00+05:30", "date": "2026-05-17T19:30:00+05:30", "team1_code": "DC", "team2_code": "RR", "team1": "Delhi Capitals", "team2": "Rajasthan Royals", "status": "upcoming", "venue": "Delhi"},
    {"match_id": "CSK-SRH-2026-05-18T19:30:00+05:30", "date": "2026-05-18T19:30:00+05:30", "team1_code": "CSK", "team2_code": "SRH", "team1": "Chennai Super Kings", "team2": "Sunrisers Hyderabad", "status": "upcoming", "venue": "Chennai"},
    {"match_id": "RR-LSG-2026-05-19T19:30:00+05:30", "date": "2026-05-19T19:30:00+05:30", "team1_code": "RR", "team2_code": "LSG", "team1": "Rajasthan Royals", "team2": "Lucknow Super Giants", "status": "upcoming", "venue": "Jaipur"},
    {"match_id": "KKR-MI-2026-05-20T19:30:00+05:30", "date": "2026-05-20T19:30:00+05:30", "team1_code": "KKR", "team2_code": "MI", "team1": "Kolkata Knight Riders", "team2": "Mumbai Indians", "status": "upcoming", "venue": "Kolkata"},
    {"match_id": "CSK-GT-2026-05-21T19:30:00+05:30", "date": "2026-05-21T19:30:00+05:30", "team1_code": "CSK", "team2_code": "GT", "team1": "Chennai Super Kings", "team2": "Gujarat Titans", "status": "upcoming", "venue": "Ahmedabad"},
    {"match_id": "SRH-RCB-2026-05-22T19:30:00+05:30", "date": "2026-05-22T19:30:00+05:30", "team1_code": "SRH", "team2_code": "RCB", "team1": "Sunrisers Hyderabad", "team2": "Royal Challengers Bengaluru", "status": "upcoming", "venue": "Hyderabad"},
    {"match_id": "LSG-PBKS-2026-05-23T19:30:00+05:30", "date": "2026-05-23T19:30:00+05:30", "team1_code": "LSG", "team2_code": "PBKS", "team1": "Lucknow Super Giants", "team2": "Punjab Kings", "status": "upcoming", "venue": "Lucknow"},
    {"match_id": "MI-RR-2026-05-24T15:30:00+05:30", "date": "2026-05-24T15:30:00+05:30", "team1_code": "MI", "team2_code": "RR", "team1": "Mumbai Indians", "team2": "Rajasthan Royals", "status": "upcoming", "venue": "Mumbai"},
    {"match_id": "KKR-DC-2026-05-24T19:30:00+05:30", "date": "2026-05-24T19:30:00+05:30", "team1_code": "KKR", "team2_code": "DC", "team1": "Kolkata Knight Riders", "team2": "Delhi Capitals", "status": "upcoming", "venue": "Kolkata"},
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
    st = comp.get("status") or {}
    t = st.get("type") or {}
    return (
        str(t.get("name") or "").strip().upper(),
        str(t.get("state") or "").strip().lower(),
        str(t.get("detail") or "").strip().lower(),
    )


def _resolve_fixture_status(comp: Dict[str, Any]) -> str:
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
    comps = comp.get("competitors")
    if not isinstance(comps, list):
        return None
    for c in comps:
        if not isinstance(c, dict):
            continue
        if c.get("winner") is True:
            team = c.get("team") or {}
            name = (team.get("displayName") or team.get("name") or "").strip()
            if name:
                _, code = _team_name_to_code_and_name(name)
                return code or None
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
    if winner:
        result["winner"] = winner
    return result


def _extract_from_next_data(next_data: Dict[str, Any]) -> List[Dict[str, Any]]:
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


def _extract_from_espn_api(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    fixtures: List[Dict[str, Any]] = []
    seen = set()

    events = data.get("events") or []
    for event in events:
        if not isinstance(event, dict):
            continue
        competitions = event.get("competitions") or []
        for comp in competitions:
            if not isinstance(comp, dict):
                continue
            competitors = comp.get("competitors") or []
            if len(competitors) < 2:
                continue

            def get_name(c: Dict[str, Any]) -> str:
                t = c.get("team") or {}
                return (t.get("displayName") or t.get("name") or "").strip()

            t1_name = get_name(competitors[0])
            t2_name = get_name(competitors[1])
            if not t1_name or not t2_name:
                continue

            status_obj = comp.get("status") or {}
            type_obj = status_obj.get("type") or {}
            state = str(type_obj.get("state") or "").lower()
            if state == "post":
                status = "completed"
            elif state == "in":
                status = "live"
            else:
                status = "upcoming"

            winner = None
            if status == "completed":
                for c in competitors:
                    if c.get("winner") is True:
                        t = c.get("team") or {}
                        name = (t.get("displayName") or t.get("name") or "").strip()
                        if name:
                            _, code = _team_name_to_code_and_name(name)
                            winner = code or None
                        break

            venue = None
            venue_obj = comp.get("venue") or {}
            venue = venue_obj.get("fullName") or venue_obj.get("name")

            date_iso = comp.get("date") or event.get("date") or ""
            match_id = str(comp.get("id") or event.get("id") or "")

            item = _to_fixture_dict(
                team1_name=t1_name,
                team2_name=t2_name,
                date_iso=date_iso,
                venue=venue,
                status=status,
                winner=winner,
                match_id=match_id or None,
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
        content_type = r.headers.get("content-type", "")
        if "application/json" in content_type or url.startswith("https://site.api.espn.com"):
            try:
                data = r.json()
                fixtures = _extract_from_espn_api(data)
                print(f"[DEBUG] Parsed via ESPN JSON API: {len(fixtures)} fixtures", file=sys.stderr)
                return fixtures
            except Exception as e:
                print(f"[DEBUG] ESPN JSON API parse failed ({e})", file=sys.stderr)
                return []
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
    now = datetime.now(timezone.utc)
    result = []
    for f in fixtures:
        f = dict(f)
        if f.get("status") == "upcoming" and f.get("date"):
            try:
                dt = datetime.fromisoformat(f["date"])
                if dt.tzinfo is not None:
                    dt_utc = dt.astimezone(timezone.utc)
                else:
                    dt_utc = dt.replace(tzinfo=timezone.utc)
                if (now - dt_utc).total_seconds() / 3600 > 4:
                    f["status"] = "completed"
            except Exception:
                pass
        result.append(f)
    return result


def fetch_espn_fixtures(season: int) -> dict:
    if season <= 0:
        raise ValueError("season must be a positive integer")

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; IPL-NRR-Sim/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IN,en;q=0.9",
        "Connection": "keep-alive",
    }

    scraped_fixtures = []
    url_used = ESPN_FIXTURES_SCHEDULE_URL_TEMPLATE.format(series_id=IPL_SERIES_ID, season=season)

    for url in [
        ESPN_FIXTURES_URL_TEMPLATE.format(series_id=IPL_SERIES_ID),
        ESPN_FIXTURES_SCHEDULE_URL_TEMPLATE.format(series_id=IPL_SERIES_ID, season=season),
    ]:
        try:
            result = _scrape_url(url, headers)
            if len(result) > len(scraped_fixtures):
                scraped_fixtures = result
                url_used = url
        except Exception as e:
            print(f"[DEBUG] Scrape failed for {url}: {e}", file=sys.stderr)

    # ── Cricbuzz: fetch result texts ──────────────────────────────────────────
    cricbuzz_map: dict = {}
    try:
        now_utc = datetime.utcnow()
        completed_pairs = []
        now_utc = datetime.now(timezone.utc)
        for hf in HARDCODED_IPL_2026_FIXTURES:
            try:
                dt = datetime.fromisoformat(hf["date"])
                if dt.tzinfo is not None:
                    dt_utc = dt.astimezone(timezone.utc)
                else:
                    dt_utc = dt.replace(tzinfo=timezone.utc)
                if (now_utc - dt_utc).total_seconds() / 3600 > 4:
                    date_only = hf["date"][:10]
                    completed_pairs.append(f"{hf['team1_code']}-{hf['team2_code']}-{date_only}")
            except Exception:
                pass

        print(f"[CB] Detected {len(completed_pairs)} completed matches to fetch", file=sys.stderr)
        cricbuzz_map = fetch_cricbuzz_ipl_results(completed_pairs=completed_pairs)
        print(f"[CB] Cricbuzz returned data for {len(cricbuzz_map)} keys", file=sys.stderr)
    except Exception as e:
        print(f"[CB] Cricbuzz fetch failed (non-fatal): {e}", file=sys.stderr)
        cricbuzz_map = {}

    # ── Build lookup of ESPN scraped fixtures by team-pair ────────────────────
    scraped_by_teams: dict = {}
    for f in scraped_fixtures:
        key = f"{f['team1_code']}-{f['team2_code']}"
        scraped_by_teams[key] = f
        scraped_by_teams[f"{f['team2_code']}-{f['team1_code']}"] = f

    # ── Merge: ESPN scraped + hardcoded fallback ──────────────────────────────
    seen_ids: set = {f["match_id"] for f in scraped_fixtures}
    fixtures = list(scraped_fixtures)

    added_from_hardcoded = 0
    for hf in HARDCODED_IPL_2026_FIXTURES:
        if hf["match_id"] in seen_ids:
            continue
        pair_key = f"{hf['team1_code']}-{hf['team2_code']}"
        if pair_key in scraped_by_teams:
            continue
        seen_ids.add(hf["match_id"])
        f = dict(hf)
        fixtures.append(f)
        added_from_hardcoded += 1

    fixtures.sort(key=lambda x: (x.get("date") or "", x["team1_code"], x["team2_code"]))

    # ── Time-based completion fallback ────────────────────────────────────────
    fixtures = _mark_past_fixtures_completed(fixtures)

    # ── Enrich completed fixtures with Cricbuzz result text + winner ──────────
    # Only enrich fixtures whose date has passed — never touch future matches
    
    for f in fixtures:
        try:
            dt = datetime.fromisoformat(f["date"])
            if dt.tzinfo is not None:
                dt_utc = dt.astimezone(timezone.utc)
            else:
                dt_utc = dt.replace(tzinfo=timezone.utc)
            if (datetime.now(timezone.utc) - dt_utc).total_seconds() / 3600 < 4:
                continue
        except Exception:
            continue

        # Skip if hardcoded data already has a trusted result
        if f.get("winner_code") or f.get("status") == "no_result":
            continue

        date_only = f["date"][:10]
        date_pair_key = f"{f['team1_code']}-{f['team2_code']}-{date_only}"
        date_reverse_key = f"{f['team2_code']}-{f['team1_code']}-{date_only}"

        cb = (
            cricbuzz_map.get(date_pair_key)
            or cricbuzz_map.get(date_reverse_key)
        )

        if not cb:
            continue

        cb_status = cb.get("status")  # "completed" or "no_result"

        if cb_status == "no_result":
            f["status"] = "no_result"
            f["result"] = cb.get("result", "No Result")
            f.pop("winner", None)
            f.pop("winner_code", None)
        elif cb_status == "completed":
            f["status"] = "completed"
            if cb.get("result"):
                f["result"] = cb["result"]
            if cb.get("winner") and not f.get("winner_code"):
                f["winner"] = cb["winner"]
                f["winner_code"] = cb["winner"]

        if cb.get("team1_score"):
            f["team1_score"] = cb["team1_score"]
        if cb.get("team2_score"):
            f["team2_score"] = cb["team2_score"]

    print(
        f"[DEBUG] Scraped: {len(scraped_fixtures)}, hardcoded added: {added_from_hardcoded}, "
        f"total: {len(fixtures)}, cricbuzz enriched: {sum(1 for f in fixtures if f.get('result'))}",
        file=sys.stderr,
    )

    return {
        "season": season,
        "source": "espn+cricbuzz",
        "url_used": url_used,
        "last_updated_utc": _utc_now_iso(),
        "fixtures": fixtures,
        "fixtures_count": len(fixtures),
    }