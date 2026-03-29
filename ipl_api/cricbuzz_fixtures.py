from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

CRICBUZZ_IPL_SERIES_ID = 9241

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

CB_SHORT_TO_CODE: Dict[str, str] = {
    "RCB": "RCB", "CSK": "CSK", "MI": "MI", "KKR": "KKR",
    "SRH": "SRH", "RR": "RR", "DC": "DC", "PBKS": "PBKS",
    "LSG": "LSG", "GT": "GT",
}


def _name_to_code(name: str) -> Optional[str]:
    return CB_NAME_TO_CODE.get(name.strip().lower())


def _short_to_code(short: str) -> Optional[str]:
    return CB_SHORT_TO_CODE.get(short.strip().upper())


def _get_headers() -> Dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IN,en;q=0.9",
        "Referer": "https://www.cricbuzz.com",
    }


def _extract_next_f_json_objects(html: str, series_id: int) -> List[Dict[str, Any]]:
    """Extract all match JSON objects from __next_f fragments."""
    all_matches = []
    fragments = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html, re.DOTALL)

    for frag in fragments:
        try:
            unescaped = frag.encode().decode("unicode_escape")
        except Exception:
            unescaped = frag

        if str(series_id) not in unescaped:
            continue

        for blob_match in re.finditer(
            r'\{"matchInfo":\{.*?\}(?:,"matchScore":\{.*?\})?\}',
            unescaped,
            re.DOTALL,
        ):
            try:
                obj = json.loads(blob_match.group(0))
                if str(obj.get("matchInfo", {}).get("seriesId", "")) == str(series_id):
                    all_matches.append(obj)
            except Exception:
                pass

    return all_matches


def _fetch_all_match_ids() -> Dict[str, int]:
    """
    Fetch the series page to get Cricbuzz match IDs for all 70 IPL fixtures.
    Returns dict keyed by both "T1-T2" and "T2-T1".
    """
    url = f"https://www.cricbuzz.com/cricket-series/{CRICBUZZ_IPL_SERIES_ID}/indian-premier-league-2026/matches"
    print(f"[CB] Fetching series page for match IDs: {url}", file=sys.stderr)

    try:
        r = requests.get(url, headers=_get_headers(), timeout=20)
        r.raise_for_status()
        html = r.text
        print(f"[CB] Series page HTML length: {len(html)}", file=sys.stderr)
    except Exception as e:
        print(f"[CB] Series page fetch failed: {e}", file=sys.stderr)
        return {}

    match_id_map: Dict[str, int] = {}
    all_matches = _extract_next_f_json_objects(html, CRICBUZZ_IPL_SERIES_ID)

    seen = set()
    for obj in all_matches:
        info = obj.get("matchInfo", {})
        match_id = info.get("matchId")
        if not match_id:
            continue

        t1 = info.get("team1", {})
        t2 = info.get("team2", {})
        t1_code = _name_to_code(t1.get("teamName", "")) or _short_to_code(t1.get("teamSName", ""))
        t2_code = _name_to_code(t2.get("teamName", "")) or _short_to_code(t2.get("teamSName", ""))

        if not t1_code or not t2_code or t1_code == t2_code:
            continue

        pair = f"{t1_code}-{t2_code}"
        if pair in seen:
            continue
        seen.add(pair)

        match_id_map[pair] = int(match_id)
        match_id_map[f"{t2_code}-{t1_code}"] = int(match_id)

    print(f"[CB] Got match IDs for {len(match_id_map) // 2} fixtures from series page", file=sys.stderr)
    return match_id_map


def _fetch_scorecard_result(match_id: int) -> Optional[Dict[str, Any]]:
    """
    Fetch result for a single completed match from its Cricbuzz scorecard page.
    Returns dict with keys: status, winner, result
    """
    url = f"https://www.cricbuzz.com/live-cricket-scores/{match_id}/"
    print(f"[CB] Fetching scorecard: {url}", file=sys.stderr)

    try:
        r = requests.get(url, headers=_get_headers(), timeout=20)
        r.raise_for_status()
        html = r.text
    except Exception as e:
        print(f"[CB] Scorecard fetch failed for {match_id}: {e}", file=sys.stderr)
        return None

    # Strategy 1: extract from __next_f JSON fragments
    fragments = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html, re.DOTALL)
    for frag in fragments:
        try:
            unescaped = frag.encode().decode("unicode_escape")
        except Exception:
            unescaped = frag

        if "won" not in unescaped.lower():
            continue

        for pattern in [
            r'"status"\s*:\s*"([^"]*won[^"]*)"',
            r'"statusText"\s*:\s*"([^"]*won[^"]*)"',
            r'"result"\s*:\s*"([^"]*won[^"]*)"',
        ]:
            m = re.search(pattern, unescaped, re.IGNORECASE)
            if m:
                result_text = m.group(1).strip()
                winner_code = _parse_winner_from_result(result_text)
                if winner_code:
                    print(f"[CB] Match {match_id} result: {result_text}", file=sys.stderr)
                    return {"status": "completed", "winner": winner_code, "result": result_text}

    # Strategy 2: raw HTML fallback
    m = re.search(r'([A-Za-z ]{5,50}won by[^<"]{5,60})', html)
    if m:
        result_text = m.group(1).strip()
        winner_code = _parse_winner_from_result(result_text)
        if winner_code:
            print(f"[CB] Match {match_id} result (fallback): {result_text}", file=sys.stderr)
            return {"status": "completed", "winner": winner_code, "result": result_text}

    # Strategy 3: check for "tied" or "no result"
    if re.search(r'match tied|no result|abandoned', html, re.IGNORECASE):
        print(f"[CB] Match {match_id}: tied/no result", file=sys.stderr)
        return {"status": "completed", "winner": None, "result": "No result"}

    print(f"[CB] Could not parse result for match {match_id}", file=sys.stderr)
    return None


def _parse_winner_from_result(result_text: str) -> Optional[str]:
    """Extract winner code from result string like 'Royal Challengers Bengaluru won by 6 wkts'."""
    lower = result_text.lower()
    for full_name, code in CB_NAME_TO_CODE.items():
        if lower.startswith(full_name) and "won" in lower:
            return code
    # Fallback: short name at start
    for short, code in CB_SHORT_TO_CODE.items():
        if lower.startswith(short.lower()) and "won" in lower:
            return code
    return None


def fetch_cricbuzz_ipl_results(
    completed_pairs: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Fully automatic IPL result fetcher.

    1. Fetches series page to get match IDs for all 70 fixtures.
    2. For each pair in completed_pairs (matches whose date has passed),
       fetches the individual scorecard page to get result + winner.

    Args:
        completed_pairs: list of "T1-T2" pair keys for matches to fetch results for.
                         If None, only match IDs are returned with no scorecard fetching.

    Returns dict keyed by both "T1-T2" and "T2-T1".
    """
    # Step 1: get all match IDs from series page
    match_id_map = _fetch_all_match_ids()

    if not completed_pairs:
        # No completed matches to fetch — return empty result map
        # (match IDs are returned separately via match_id_map if needed)
        return {}

    result_map: Dict[str, Dict[str, Any]] = {}
    fetched: set = set()

    for pair in completed_pairs:
        # Normalise to canonical direction
        t1, t2 = pair.split("-", 1)
        canonical = f"{t1}-{t2}"
        reverse = f"{t2}-{t1}"

        # Already fetched
        if canonical in fetched:
            continue

        match_id = match_id_map.get(canonical) or match_id_map.get(reverse)
        if not match_id:
            print(f"[CB] No match ID found for {canonical} — skipping", file=sys.stderr)
            continue

        result = _fetch_scorecard_result(match_id)
        fetched.add(canonical)
        fetched.add(reverse)

        if result:
            result["team1_code"] = t1
            result["team2_code"] = t2
            result_map[canonical] = result
            result_map[reverse] = result

    print(f"[CB] Fetched results for {len(result_map) // 2} completed matches", file=sys.stderr)
    return result_map