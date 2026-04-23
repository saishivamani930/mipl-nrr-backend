from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

import random
import time

CRICBUZZ_IPL_SERIES_ID = 9241
KNOWN_MATCH_IDS: Dict[str, int] = {
    "RCB-SRH-2026-03-28": 149618,
    "MI-KKR-2026-03-29": 149629,
    "RR-CSK-2026-03-30": 149640,
    "PBKS-GT-2026-03-31": 149651,
    "LSG-DC-2026-04-01": 149662,
    "KKR-SRH-2026-04-02": 149673,
    "CSK-PBKS-2026-04-03": 149684,
    "DC-MI-2026-04-04": 149695,
    "GT-RR-2026-04-04": 149699,
    "SRH-LSG-2026-04-05": 149710,
    "RCB-CSK-2026-04-05": 149721,
    "KKR-PBKS-2026-04-06": 149732,
    "RR-MI-2026-04-07": 149743,
    "DC-GT-2026-04-08": 149746,
    "KKR-LSG-2026-04-09": 149757,
    "RR-RCB-2026-04-10": 149768,
    "PBKS-SRH-2026-04-11": 149779,
    "CSK-DC-2026-04-11": 149790,
    "LSG-GT-2026-04-12": 149801,
    "MI-RCB-2026-04-12": 149812,
    "SRH-RR-2026-04-13": 151752,
    "CSK-KKR-2026-04-14": 151763,
    "RCB-LSG-2026-04-15": 151774,
    "MI-PBKS-2026-04-16": 151785,
    "GT-KKR-2026-04-17": 151796,
    "RCB-DC-2026-04-18": 151807,
    "SRH-CSK-2026-04-18": 151818,
    "KKR-RR-2026-04-19": 151829,
    "PBKS-LSG-2026-04-19": 151840,
    "GT-MI-2026-04-20": 151845,
    "SRH-DC-2026-04-21": 151856,
    "LSG-RR-2026-04-22": 151867,
    "MI-CSK-2026-04-23": 151878,
    "RCB-GT-2026-04-24": 151889,
    "DC-PBKS-2026-04-25": 151891,
    "RR-SRH-2026-04-25": 151902,
    "GT-CSK-2026-04-26": 151913,
    "LSG-KKR-2026-04-26": 151924,
    "DC-RCB-2026-04-27": 151935,
    "PBKS-RR-2026-04-28": 151943,
    "MI-SRH-2026-04-29": 151954,
    "GT-RCB-2026-04-30": 151965,
    "RR-DC-2026-05-01": 151976,
    "CSK-MI-2026-05-02": 151987,
    "SRH-KKR-2026-05-03": 151998,
    "GT-PBKS-2026-05-03": 152009,
    "MI-LSG-2026-05-04": 152020,
    "DC-CSK-2026-05-05": 152031,
    "SRH-PBKS-2026-05-06": 152042,
    "LSG-RCB-2026-05-07": 152053,
    "DC-KKR-2026-05-08": 152064,
    "RR-GT-2026-05-09": 152075,
    "CSK-LSG-2026-05-10": 152086,
    "RCB-MI-2026-05-10": 152097,
    "PBKS-DC-2026-05-11": 152108,
    "GT-SRH-2026-05-12": 152119,
    "RCB-KKR-2026-05-13": 152130,
    "PBKS-MI-2026-05-14": 152141,
    "LSG-CSK-2026-05-15": 152152,
    "KKR-GT-2026-05-16": 152163,
    "PBKS-RCB-2026-05-17": 152174,
    "DC-RR-2026-05-17": 152185,
    "CSK-SRH-2026-05-18": 152196,
    "RR-LSG-2026-05-19": 152207,
    "KKR-MI-2026-05-20": 152218,
    "CSK-GT-2026-05-21": 152229,
    "SRH-RCB-2026-05-22": 152240,
    "LSG-PBKS-2026-05-23": 152241,
    "MI-RR-2026-05-24": 152252,
    "KKR-DC-2026-05-24": 152263,
}

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

    all_won_by = re.findall(r'([A-Za-z ]{5,50}won by[^<"\\]{5,60})', html)
    for result_text in all_won_by:
        result_text = result_text.strip()
        winner_code = _parse_winner_from_result(result_text)
        if winner_code:
            print(f"[CB] Match {match_id} result: {result_text}", file=sys.stderr)
            return {"status": "completed", "winner": winner_code, "result": result_text}

    if re.search(
        r'(?:^|[>\s])(match tied|no result|abandoned)(?:[<\s]|$)',
        html,
        re.IGNORECASE | re.MULTILINE,
    ):
        print(f"[CB] Match {match_id}: tied/no result/abandoned", file=sys.stderr)
        return {"status": "no_result", "winner": None, "result": "No result"}

    print(f"[CB] Could not parse result for match {match_id}", file=sys.stderr)
    return None


def _fetch_scorecard_innings(match_id: int) -> Optional[Dict[str, Any]]:
    """
    Fetch innings scores for a completed match.

    The scores appear in the page <meta> description tag in this exact format:
        DC 164/4 (18.1) vs MI\\n                162/6
    or in <title> / twitter:title as:
        DC 164/4 (18.1) vs MI 162/6

    We parse both and pick the first clean hit.
    """
    url = f"https://www.cricbuzz.com/live-cricket-scores/{match_id}/"
    try:
        r = requests.get(url, headers=_get_headers(), timeout=20)
        r.raise_for_status()
        html = r.text
    except Exception as e:
        print(f"[CB] Innings fetch failed for {match_id}: {e}", file=sys.stderr)
        return None

    def overs_to_balls(overs_str: str) -> int:
        s = str(overs_str).strip()
        if "." in s:
            full, partial = s.split(".")
            return int(full) * 6 + int(partial)
        return int(s) * 6

    # ----------------------------------------------------------------
    # Strategy 1: Parse from meta description / twitter:title tags.
    #
    # The debug output confirmed this exact format in chunk 0 + 1:
    #   "DC 164/4 (18.1) vs MI\n                162/6"
    # and in twitter:title:
    #   "DC 164/4 (18.1) vs MI\n                162/6"
    #
    # Pattern: TEAM RUNS/WKTS (OVERS) vs TEAM RUNS/WKTS
    # The second innings overs may or may not be present in meta tags.
    # ----------------------------------------------------------------

    # Extract all meta content values and the title — these reliably carry scores
    meta_contents = re.findall(r'content="([^"]{20,400})"', html, re.DOTALL)
    title_match = re.search(r'<title>([^<]+)</title>', html)
    if title_match:
        meta_contents.append(title_match.group(1))

    # Pattern for a score block: TEAM RUNS/WKTS (OVERS)
    # e.g. "DC 164/4 (18.1)" or "MI 162/6 (20)"
    score_block = re.compile(
        r'\b([A-Z]{2,5})\s+(\d{2,3})/(\d{1,2})\s*\(([\d.]+)\)',
    )

    for content in meta_contents:
        # Normalise whitespace (newlines + spaces in meta values)
        content_clean = re.sub(r'\s+', ' ', content)
        found = score_block.findall(content_clean)
        # Filter to known IPL team codes only
        valid = [(s, r, w, o) for s, r, w, o in found if _short_to_code(s)]
        if len(valid) >= 2:
            result = {}
            for short, runs_str, wkts_str, overs_str in valid[:2]:
                code = _short_to_code(short)
                if code and code not in result:
                    result[code] = {
                        "runs": int(runs_str),
                        "balls": overs_to_balls(overs_str),
                    }
            if len(result) == 2:
                codes = list(result.keys())
                print(
                    f"[CB] Match {match_id} innings (meta): "
                    f"{codes[0]} {result[codes[0]]} vs {codes[1]} {result[codes[1]]}",
                    file=sys.stderr,
                )
                return result

    # ----------------------------------------------------------------
    # Strategy 2: The second innings overs may be missing from meta
    # (e.g. "MI\n162/6" with no overs shown while match was live).
    # In that case, fall back to scanning the full HTML for score lines
    # like "DC<!-- --> 164/4<!-- --> (18.1" that React renders as
    # HTML comments between text nodes.
    # ----------------------------------------------------------------
    react_score = re.compile(
        r'\b([A-Z]{2,5})(?:<!-- -->)?\s+(\d{2,3})/(\d{1,2})(?:<!-- -->)?\s*\(([\d.]+)',
    )
    found2 = react_score.findall(html)
    valid2 = [(s, r, w, o) for s, r, w, o in found2 if _short_to_code(s)]
    if len(valid2) >= 2:
        result = {}
        for short, runs_str, wkts_str, overs_str in valid2[:2]:
            code = _short_to_code(short)
            if code and code not in result:
                result[code] = {
                    "runs": int(runs_str),
                    "balls": overs_to_balls(overs_str),
                }
        if len(result) == 2:
            codes = list(result.keys())
            print(
                f"[CB] Match {match_id} innings (react): "
                f"{codes[0]} {result[codes[0]]} vs {codes[1]} {result[codes[1]]}",
                file=sys.stderr,
            )
            return result

    # ----------------------------------------------------------------
    # Strategy 3: __next_f JSON fragments — look for inningsScore array
    # ----------------------------------------------------------------
    fragments = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html, re.DOTALL)
    search_targets = [html]
    for frag in fragments:
        try:
            search_targets.append(frag.encode().decode("unicode_escape"))
        except Exception:
            search_targets.append(frag)

    innings_array_pattern = re.compile(r'"inningsScore"\s*:\s*(\[[^\]]+\])', re.DOTALL)
    for target in search_targets:
        for m in innings_array_pattern.finditer(target):
            try:
                # Try to parse individual innings objects from the array
                obj_strs = re.findall(r'\{[^{}]+\}', m.group(1))
                result = {}
                for obj_str in obj_strs:
                    try:
                        obj = json.loads(obj_str)
                    except Exception:
                        # Manual field extraction fallback
                        t = re.search(r'"(?:batTeamName|shortName|teamSName)"\s*:\s*"([^"]+)"', obj_str)
                        r_val = re.search(r'"runs"\s*:\s*(\d+)', obj_str)
                        o_val = re.search(r'"overs"\s*:\s*([\d.]+)', obj_str)
                        if not (t and r_val and o_val):
                            continue
                        obj = {"batTeamName": t.group(1), "runs": int(r_val.group(1)), "overs": o_val.group(1)}

                    team_name = obj.get("batTeamName") or obj.get("shortName") or obj.get("teamSName", "")
                    runs_val = obj.get("runs")
                    overs_val = obj.get("overs")
                    if not team_name or runs_val is None or overs_val is None:
                        continue
                    code = _short_to_code(team_name) or _name_to_code(team_name)
                    if code and code not in result:
                        result[code] = {"runs": int(runs_val), "balls": overs_to_balls(overs_val)}

                if len(result) == 2:
                    codes = list(result.keys())
                    print(
                        f"[CB] Match {match_id} innings (JSON): "
                        f"{codes[0]} {result[codes[0]]} vs {codes[1]} {result[codes[1]]}",
                        file=sys.stderr,
                    )
                    return result
            except Exception:
                continue

    print(f"[CB] Could not parse innings for {match_id}", file=sys.stderr)
    return None


def _parse_winner_from_result(result_text: str) -> Optional[str]:
    """Extract winner code from result string like 'Royal Challengers Bengaluru won by 6 wkts'."""
    lower = result_text.lower()
    for full_name, code in CB_NAME_TO_CODE.items():
        if lower.startswith(full_name) and "won" in lower:
            return code
    for short, code in CB_SHORT_TO_CODE.items():
        if lower.startswith(short.lower()) and "won" in lower:
            return code
    return None


def fetch_cricbuzz_ipl_results(
    completed_pairs: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    Fetch IPL results from Cricbuzz.
    """
    match_id_map = _fetch_all_match_ids()

    if not completed_pairs:
        return {}

    result_map: Dict[str, Dict[str, Any]] = {}
    fetched: set = set()

    for pair in completed_pairs:
        parts = pair.split("-")
        if len(parts) < 2:
            continue
        if len(parts) == 5:
            t1, t2, match_date = parts[0], parts[1], "-".join(parts[2:])
        else:
            t1, t2, match_date = parts[0], parts[1], ""
        canonical = f"{t1}-{t2}"
        reverse = f"{t2}-{t1}"
        date_key = f"{canonical}-{match_date}" if match_date else ""
        reverse_date_key = f"{reverse}-{match_date}" if match_date else ""

        if canonical in fetched:
            continue

        cb_match_id = (
            KNOWN_MATCH_IDS.get(date_key)
            or KNOWN_MATCH_IDS.get(reverse_date_key)
            or match_id_map.get(canonical)
            or match_id_map.get(reverse)
        )
        if not cb_match_id:
            print(f"[CB] No match ID found for {canonical} — skipping", file=sys.stderr)
            continue

        time.sleep(random.uniform(0.5, 2.0))
        result = _fetch_scorecard_result(cb_match_id)
        fetched.add(canonical)
        fetched.add(reverse)

        if result:
            result["team1_code"] = t1
            result["team2_code"] = t2
            result["cb_match_id"] = cb_match_id
            result["match_date"] = pair

            mid_key = str(cb_match_id)
            result_map[mid_key] = result
            result_map[canonical] = result
            result_map[reverse] = result

    print(f"[CB] Fetched results for {len([k for k in result_map if '-' not in k])} completed matches", file=sys.stderr)
    return result_map


def fetch_cricbuzz_innings_aggregates(
    completed_pairs: List[str],
) -> Dict[str, Dict[str, Any]]:
    """
    For each completed match pair, fetch innings scores from Cricbuzz scorecards.
    Returns dict keyed by "T1-T2" (and "T2-T1") with innings data:
      { "SRH": {"runs": int, "balls": int}, "RCB": {"runs": int, "balls": int} }
    """
    match_id_map = _fetch_all_match_ids()
    _debug_dump_html(149695)  # TEMP - remove after
    aggregates: Dict[str, Dict[str, Any]] = {}
    fetched: set = set()

    for pair in completed_pairs:
        parts = pair.split("-")
        if len(parts) < 2:
            continue
        if len(parts) == 5:
            t1, t2, match_date = parts[0], parts[1], "-".join(parts[2:])
        else:
            t1, t2, match_date = parts[0], parts[1], ""

        canonical = f"{t1}-{t2}"
        if canonical in fetched:
            continue

        date_key = f"{canonical}-{match_date}" if match_date else ""
        reverse_date_key = f"{t2}-{t1}-{match_date}" if match_date else ""

        cb_match_id = (
            KNOWN_MATCH_IDS.get(date_key)
            or KNOWN_MATCH_IDS.get(reverse_date_key)
            or match_id_map.get(canonical)
            or match_id_map.get(f"{t2}-{t1}")
        )
        if not cb_match_id:
            print(f"[CB] No match ID for innings: {canonical}", file=sys.stderr)
            continue

        time.sleep(random.uniform(1.0, 3.0))
        innings = _fetch_scorecard_innings(cb_match_id)
        fetched.add(canonical)
        fetched.add(f"{t2}-{t1}")

        if innings:
            aggregates[canonical] = innings
            aggregates[f"{t2}-{t1}"] = innings

    print(f"[CB] Fetched innings for {len(aggregates) // 2} matches", file=sys.stderr)
    return aggregates


_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

def _debug_dump_html(match_id: int):
    r = requests.get(
        f"https://www.cricbuzz.com/live-cricket-scores/{match_id}/",
        headers=_get_headers(),
        timeout=20
    )
    html = r.text
    print(f"[CB DEBUG] HTML length: {len(html)}", file=sys.stderr)
    for i, start in enumerate([0, 3000, 6000, 10000, 15000]):
        print(f"[CB DEBUG chunk {i}]:\n{html[start:start+1500]}\n---", file=sys.stderr)

def _get_headers() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IN,en;q=0.9",
        "Referer": "https://www.cricbuzz.com",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "DNT": "1",
    }