"""
cricbuzz_fixtures.py
--------------------
Scrapes IPL match data (results, scores, status) from Cricbuzz's live scores
page, which embeds full match data including result text like
"Royal Challengers Bengaluru won by 6 wkts" directly in matchInfo.status.

Usage:
    from ipl_api.cricbuzz_fixtures import fetch_cricbuzz_ipl_results
    results = fetch_cricbuzz_ipl_results()
    # Returns dict keyed by "{team1_code}-{team2_code}" with result info
"""

from __future__ import annotations

import json
import re
import sys
from typing import Any, Dict, List, Optional

import requests

# Cricbuzz series ID for IPL 2026
CRICBUZZ_IPL_SERIES_ID = 9241

# Team name → code mapping
CB_NAME_TO_CODE: Dict[str, str] = {
    "royal challengers bengaluru": "RCB",
    "royal challengers bangalore": "RCB",
    "chennai super kings": "CSK",
    "mumbai indians": "MI",
    "kolkata knight riders": "KKR",
    "sunrisers hyderabad": "SRH",
    "rajasthan royals": "RR",
    "delhi capitals": "DC",
    "punjab kings": "PBKS",
    "lucknow super giants": "LSG",
    "gujarat titans": "GT",
}

# Cricbuzz short name → code (from teamSName field)
CB_SHORT_TO_CODE: Dict[str, str] = {
    "RCB": "RCB", "CSK": "CSK", "MI": "MI", "KKR": "KKR",
    "SRH": "SRH", "RR": "RR", "DC": "DC", "PBKS": "PBKS",
    "LSG": "LSG", "GT": "GT",
}


def _name_to_code(name: str) -> Optional[str]:
    return CB_NAME_TO_CODE.get(name.strip().lower())


def _short_to_code(short: str) -> Optional[str]:
    return CB_SHORT_TO_CODE.get(short.strip().upper())


def _extract_next_data(html: str) -> Optional[Dict[str, Any]]:
    """Extract __NEXT_DATA__ JSON from Cricbuzz HTML."""
    m = re.search(
        r'<script[^>]*id=["\']__NEXT_DATA__["\'][^>]*>\s*(\{.*?\})\s*</script>',
        html,
        re.DOTALL,
    )
    if not m:
        # Cricbuzz pushes data via self.__next_f — try parsing from that
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None


def _find_ipl_matches_in_json(obj: Any, ipl_series_id: int) -> List[Dict[str, Any]]:
    """Recursively walk JSON to find IPL match objects."""
    results = []
    if isinstance(obj, dict):
        # Match object pattern: has matchInfo with seriesId
        match_info = obj.get("matchInfo")
        if isinstance(match_info, dict):
            if str(match_info.get("seriesId", "")) == str(ipl_series_id):
                results.append(obj)
        # Also check seriesAdWrapper pattern
        series = obj.get("seriesAdWrapper")
        if isinstance(series, dict) and str(series.get("seriesId", "")) == str(ipl_series_id):
            for m in series.get("matches", []):
                if isinstance(m, dict) and "matchInfo" in m:
                    results.append(m)
        for v in obj.values():
            results.extend(_find_ipl_matches_in_json(v, ipl_series_id))
    elif isinstance(obj, list):
        for item in obj:
            results.extend(_find_ipl_matches_in_json(item, ipl_series_id))
    return results


def _parse_ipl_match(match_obj: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Parse a single Cricbuzz match object into our fixture format.
    Returns None if it can't be parsed or isn't an IPL match.
    """
    info = match_obj.get("matchInfo", {})
    score = match_obj.get("matchScore", {})

    t1 = info.get("team1", {})
    t2 = info.get("team2", {})

    t1_name = t1.get("teamName", "")
    t2_name = t2.get("teamName", "")
    t1_short = t1.get("teamSName", "")
    t2_short = t2.get("teamSName", "")

    t1_code = _name_to_code(t1_name) or _short_to_code(t1_short)
    t2_code = _name_to_code(t2_name) or _short_to_code(t2_short)

    if not t1_code or not t2_code or t1_code == t2_code:
        return None

    state = str(info.get("state", "")).lower()
    status_text = str(info.get("status", "")).strip()
    match_desc = str(info.get("matchDesc", "")).strip()

    # Determine status
    if state in ("complete", "finished"):
        status = "completed"
    elif state in ("in progress", "innings break", "strategic timeout", "drinks"):
        status = "live"
    elif state == "preview":
        status = "upcoming"
    else:
        status = "upcoming"

    # Determine winner from status text for completed matches
    winner_code: Optional[str] = None
    result_text: Optional[str] = None

    if status == "completed":
        result_text = status_text  # e.g. "Royal Challengers Bengaluru won by 6 wkts"
        # Parse winner from result text
        lower_status = status_text.lower()
        for full_name, code in CB_NAME_TO_CODE.items():
            if lower_status.startswith(full_name) and "won" in lower_status:
                winner_code = code
                break
        # Fallback: check short name in stateTitle
        state_title = str(info.get("stateTitle", "")).strip()
        if not winner_code and state_title:
            for short, code in CB_SHORT_TO_CODE.items():
                if state_title.upper().startswith(short):
                    winner_code = code
                    break

    # Date (milliseconds timestamp)
    start_ts = info.get("startDate")
    date_iso: Optional[str] = None
    if start_ts:
        try:
            from datetime import datetime, timezone
            ts = int(str(start_ts)) / 1000
            date_iso = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except Exception:
            pass

    # Venue
    venue_info = info.get("venueInfo", {})
    venue = venue_info.get("ground") or venue_info.get("city")

    # Match number
    match_number: Optional[str] = None
    if match_desc:
        m = re.search(r"(\d+)(?:st|nd|rd|th)\s+match", match_desc, re.IGNORECASE)
        if m:
            match_number = m.group(1)

    # Scores for display
    t1_score = score.get("team1Score", {}).get("inngs1", {})
    t2_score = score.get("team2Score", {}).get("inngs1", {})

    result: Dict[str, Any] = {
        "team1_code": t1_code,
        "team2_code": t2_code,
        "team1": t1_name,
        "team2": t2_name,
        "status": status,
        "date": date_iso,
        "venue": venue,
        "match_number": match_number,
        "match_desc": match_desc,
    }

    if winner_code:
        result["winner"] = winner_code

    if result_text:
        result["result"] = result_text  # Full Cricbuzz result string

    if t1_score:
        result["team1_score"] = f"{t1_score.get('runs', 0)}/{t1_score.get('wickets', 0)} ({t1_score.get('overs', 0)})"

    if t2_score:
        result["team2_score"] = f"{t2_score.get('runs', 0)}/{t2_score.get('wickets', 0)} ({t2_score.get('overs', 0)})"

    return result


def fetch_cricbuzz_ipl_results(
    ipl_series_id: int = CRICBUZZ_IPL_SERIES_ID,
) -> Dict[str, Dict[str, Any]]:
    """
    Fetch IPL match results from Cricbuzz live scores page.

    Returns a dict keyed by canonical pair keys:
        "{team1_code}-{team2_code}" AND "{team2_code}-{team1_code}"
    Both point to the same match dict, making lookups easy regardless of
    which team is listed as team1 vs team2.

    Each match dict contains:
        team1_code, team2_code, status, result (result text), winner, date, venue, scores
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IN,en;q=0.9",
        "Referer": "https://www.cricbuzz.com",
    }

    url = "https://www.cricbuzz.com/cricket-series/9241/indian-premier-league-2026/matches"
    print(f"[CB] Fetching: {url}", file=sys.stderr)

    try:
        r = requests.get(url, headers=headers, timeout=20)
        r.raise_for_status()
        html = r.text
        print(f"[CB] HTML length: {len(html)}", file=sys.stderr)
    except Exception as e:
        print(f"[CB] Fetch failed: {e}", file=sys.stderr)
        return {}

    # The data is embedded in __next_f script tags as JSON fragments
    # Try extracting all JSON-like blobs containing matchInfo
    all_matches: List[Dict[str, Any]] = []

    # Strategy 1: __NEXT_DATA__
    next_data = _extract_next_data(html)
    if next_data:
        matches = _find_ipl_matches_in_json(next_data, ipl_series_id)
        all_matches.extend(matches)
        print(f"[CB] Found {len(matches)} IPL matches via __NEXT_DATA__", file=sys.stderr)

    # Strategy 2: self.__next_f.push fragments — extract large JSON blobs
    if not all_matches:
        # Find all self.__next_f.push([1,"..."]) blocks and look for matchInfo
        fragments = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html, re.DOTALL)
        for frag in fragments:
            # Unescape the JSON string
            try:
                unescaped = frag.encode().decode("unicode_escape")
            except Exception:
                unescaped = frag

            # Look for matchInfo blocks with our series ID
            if str(ipl_series_id) not in unescaped:
                continue

            # Try to extract JSON objects containing matchInfo
            for blob_match in re.finditer(r'\{"matchInfo":\{.*?\}(?:,"matchScore":\{.*?\})?\}', unescaped, re.DOTALL):
                try:
                    obj = json.loads(blob_match.group(0))
                    if str(obj.get("matchInfo", {}).get("seriesId", "")) == str(ipl_series_id):
                        all_matches.append(obj)
                except Exception:
                    pass

        print(f"[CB] Found {len(all_matches)} IPL matches via __next_f fragments", file=sys.stderr)

    # Strategy 3: direct regex on raw HTML for IPL match blocks
    if not all_matches:
        ipl_blocks = re.findall(
            r'\{"matchInfo":\{"matchId":\d+,"seriesId":' + str(ipl_series_id) + r'.*?\}(?:,"matchScore":\{.*?\})?\}',
            html,
            re.DOTALL,
        )
        for block in ipl_blocks:
            try:
                obj = json.loads(block)
                all_matches.append(obj)
            except Exception:
                pass
        print(f"[CB] Found {len(all_matches)} IPL matches via direct regex", file=sys.stderr)

    # Parse and deduplicate
    seen_pairs: set = set()
    result_map: Dict[str, Dict[str, Any]] = {}

    for raw_match in all_matches:
        parsed = _parse_ipl_match(raw_match)
        if not parsed:
            continue

        pair = f"{parsed['team1_code']}-{parsed['team2_code']}"
        rev_pair = f"{parsed['team2_code']}-{parsed['team1_code']}"

        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        seen_pairs.add(rev_pair)

        result_map[pair] = parsed
        result_map[rev_pair] = parsed

    print(f"[CB] Parsed {len(seen_pairs) // 2} unique IPL matches", file=sys.stderr)
    return result_map