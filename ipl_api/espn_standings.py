from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

import logging
logger = logging.getLogger(__name__)

from ipl_api.config import IPL_SERIES_ID, ESPN_TABLE_URL_TEMPLATE
from ipl_api.nrr_math import overs_to_balls

from io import StringIO

from ipl_api.cricbuzz_fixtures import fetch_cricbuzz_ipl_results
from datetime import datetime, timezone
from ipl_api.espn_fixtures import fetch_espn_fixtures, HARDCODED_IPL_2026_FIXTURES

from ipl_api import cache as _cache


class StandingsScrapeError(Exception):
    pass


IPL_TEAM_NAMES = {
    "Chennai Super Kings",
    "Mumbai Indians",
    "Royal Challengers Bengaluru",
    "Kolkata Knight Riders",
    "Sunrisers Hyderabad",
    "Rajasthan Royals",
    "Delhi Capitals",
    "Punjab Kings",
    "Lucknow Super Giants",
    "Gujarat Titans",
}

IPL_TEAM_CODES = {
    "CSK","MI","RCB","KKR","SRH","RR","DC","PBKS","LSG","GT"
}

ESPN_TABLE_URLS = [
    "https://www.espn.in/cricket/table/series/{series_id}/season/{season}/indian-premier-league",
    "https://www.espncricinfo.com/series/ipl-{season}-{series_id}/points-table-standings",
]


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = []
    for c in df.columns:
        if isinstance(c, tuple):
            c = " ".join([str(x) for x in c if x and str(x) != "nan"]).strip()
        cols.append(str(c).strip())
    df.columns = cols
    return df


def _parse_runs_overs_cell(val: Any) -> Optional[Tuple[int, int]]:
    if val is None:
        return None

    s = str(val).strip()
    if not s or s.lower() == "nan":
        return None

    m = re.match(r"^\s*(\d+)\s*/\s*([0-9]+(?:\.[0-5])?)\s*$", s)
    if not m:
        return None

    runs = int(m.group(1))
    overs_str = m.group(2)

    if "." not in overs_str:
        overs_str = overs_str + ".0"

    balls = overs_to_balls(overs_str)
    return runs, balls


def _clean_team_cell(raw: Any) -> Tuple[str, Optional[str]]:
    if raw is None:
        return "", None

    s = str(raw).strip()
    if not s or s.lower() == "nan":
        return "", None

    s = s.replace("Image", " ").strip()
    s = re.sub(r"(?<=\d)(?=[A-Za-z])", " ", s)
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"^\d+\s*", "", s).strip()

    tokens = s.split()

    if not tokens:
        return "", None

    code = None
    last = tokens[-1].upper()

    if re.fullmatch(r"[A-Z]{2,6}", last):
        code = last
        name = " ".join(tokens[:-1])
    else:
        name = s

    return name.strip(), code


def _score_team_values_for_ipl(df: pd.DataFrame) -> int:
    if "team" not in df.columns:
        return 0

    score = 0
    sample = df["team"].dropna().astype(str).head(12)

    for raw in sample:
        name, code = _clean_team_cell(raw)
        n = name.lower()
        c = (code or "").upper()

        if n in IPL_TEAM_NAMES:
            score += 8

        if c in IPL_TEAM_CODES:
            score += 6

        if "women" in n:
            score -= 30

    rows = len(df)

    if 8 <= rows <= 12:
        score += 10
    elif rows < 6:
        score -= 10

    return score


def _pick_points_table(tables: List[pd.DataFrame]) -> pd.DataFrame:
    best = None
    best_score = -10**9

    for t in tables:
        t = _flatten_columns(t.copy())
        cols = [str(c).lower() for c in t.columns]

        score = 0

        if any("team" in c for c in cols):
            score += 3
        if any("pts" in c or "points" in c for c in cols):
            score += 3
        if any("nrr" in c for c in cols):
            score += 3
        if any(c == "for" or c.endswith(" for") for c in cols):
            score += 2
        if any("against" in c for c in cols):
            score += 2
        if any(c in ("m","mat","matches") for c in cols):
            score += 1
        if any(c in ("w","won") for c in cols):
            score += 1
        if any(c in ("l","lost") for c in cols):
            score += 1

        temp = t.copy()

        for c in temp.columns:
            if "team" in str(c).lower():
                temp = temp.rename(columns={c:"team"})
                break

        score += _score_team_values_for_ipl(temp)

        if score > best_score:
            best_score = score
            best = temp

    return best if best is not None else tables[0]


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default

        s = str(x).strip()

        if not s or s.lower() == "nan":
            return default

        m = re.match(r"^(\d+)", s)

        if m:
            return int(m.group(1))

        return int(float(s))
    except:
        return default


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None

        s = str(x).strip()

        if not s or s.lower() == "nan":
            return None

        s = s.replace("−","-")
        return float(s)
    except:
        return None


def _maybe_split_points_nrr(row: pd.Series) -> Tuple[int, Optional[float]]:
    pts_raw = row.get("points")
    nrr_raw = row.get("nrr")

    pts_str = "" if pts_raw is None else str(pts_raw).strip().replace("−","-")
    nrr_val = _safe_float(nrr_raw) if nrr_raw is not None else None

    m = re.match(r"^(\d+)([-+]\d+(?:\.\d+)?)$", pts_str)

    if m and nrr_val is None:
        return int(m.group(1)), float(m.group(2))

    return _safe_int(pts_raw,0), nrr_val


def _fetch_html(url: str) -> str:
    """Fetch raw HTML from a URL with a browser-like User-Agent."""
    logger.info(f"[STANDINGS] Fetching HTML from: {url}")
    r = requests.get(
        url,
        timeout=20,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-IN,en;q=0.9",
        }
    )
    logger.info(f"[STANDINGS] HTTP {r.status_code} from {url} — content length: {len(r.text)} chars")
    r.raise_for_status()
    return r.text


def _parse_table_from_html(html: str, season: int) -> Optional[Dict[str, Any]]:
    """Try pd.read_html on HTML, pick best table, return standings dict or None."""
    try:
        tables = pd.read_html(StringIO(html))
        logger.info(f"[STANDINGS] pd.read_html found {len(tables)} table(s) in HTML")
    except Exception as e:
        logger.warning(f"[STANDINGS] pd.read_html failed: {type(e).__name__}: {str(e)}")
        return None

    if not tables:
        logger.warning("[STANDINGS] pd.read_html returned empty list")
        return None

    df = _pick_points_table(tables)
    df = _flatten_columns(df)
    logger.info(f"[STANDINGS] Best table columns: {list(df.columns)} | rows: {len(df)}")

    colmap = {}

    for c in df.columns:
        lc = str(c).lower()

        if "team" in lc:
            colmap[c] = "team"
        elif lc in ("mat","matches","m"):
            colmap[c] = "matches"
        elif lc in ("won","w"):
            colmap[c] = "won"
        elif lc in ("lost","l"):
            colmap[c] = "lost"
        elif lc in ("nr","n/r"):
            colmap[c] = "nr"
        elif lc in ("points","pts","pt"):
            colmap[c] = "points"
        elif "nrr" in lc:
            colmap[c] = "nrr"
        elif lc in ("for", "rf", "runs for", "score for", "f") or (lc.endswith(" for") and "against" not in lc):
            colmap[c] = "for"
        elif "against" in lc or lc in ("ra", "runs against", "score against", "a"):
            colmap[c] = "against"

    df = df.rename(columns=colmap)
    logger.info(f"[STANDINGS] Mapped columns: {list(df.columns)}")
    has_for = "for" in df.columns
    has_against = "against" in df.columns
    logger.info(f"[STANDINGS] has_for={has_for}, has_against={has_against}")

    teams = []

    for _, row in df.iterrows():
        team_name, team_code = _clean_team_cell(row.get("team"))

        if not team_name:
            continue

        points_val, nrr_val = _maybe_split_points_nrr(row)

        item: Dict[str, Any] = {
            "team": team_name,
            "code": team_code,
            "matches": _safe_int(row.get("matches")),
            "won": _safe_int(row.get("won")),
            "lost": _safe_int(row.get("lost")),
            "points": int(points_val),
            "nrr": nrr_val,
        }

        if "for" in df.columns:
            parsed = _parse_runs_overs_cell(row.get("for"))
            if parsed:
                item["runs_for"], item["balls_for"] = parsed

        if "against" in df.columns:
            parsed = _parse_runs_overs_cell(row.get("against"))
            if parsed:
                item["runs_against"], item["balls_against"] = parsed

        teams.append(item)

    if not teams:
        logger.warning(f"[STANDINGS] Parsed 0 valid teams from table. Raw df sample:\n{df.head(3).to_string()}")
        return None

    logger.info(f"[STANDINGS] Successfully parsed {len(teams)} teams")
    return {
        "season": season,
        "source": "espn",
        "last_updated_utc": datetime.utcnow().isoformat() + "Z",
        "teams": teams,
    }
def fetch_cricbuzz_points_table(season: int) -> Optional[Dict[str, Any]]:
    """
    Scrape the IPL points table from Cricbuzz.
    Returns the same shape as _parse_table_from_html(), or None on failure.
    Used as a fallback when all ESPN URLs fail.
    """
    from bs4 import BeautifulSoup
 
    CRICBUZZ_SERIES_ID = 9241
    url = f"https://www.cricbuzz.com/cricket-series/{CRICBUZZ_SERIES_ID}/indian-premier-league-{season}/points-table"
    logger.info(f"[STANDINGS] Trying Cricbuzz points table: {url}")
 
    try:
        html = _fetch_html(url)
    except Exception as e:
        logger.warning(f"[STANDINGS] Cricbuzz points table fetch failed: {e}")
        return None
 
    CB_CODE_MAP: Dict[str, str] = {
        "rcb": "RCB", "royal challengers bengaluru": "RCB", "royal challengers bangalore": "RCB",
        "csk": "CSK", "chennai super kings": "CSK",
        "mi": "MI",  "mumbai indians": "MI",
        "kkr": "KKR", "kolkata knight riders": "KKR",
        "srh": "SRH", "sunrisers hyderabad": "SRH",
        "rr": "RR",  "rajasthan royals": "RR",
        "dc": "DC",  "delhi capitals": "DC",
        "pbks": "PBKS", "punjab kings": "PBKS",
        "lsg": "LSG", "lucknow super giants": "LSG",
        "gt": "GT",  "gujarat titans": "GT",
    }
 
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception as e:
        logger.warning(f"[STANDINGS] BeautifulSoup parse failed: {e}")
        return None
 
    # Cricbuzz renders the points table as a series of team rows in a div/table.
    # Each row has: team name/short, M, W, L, NR, Pts, NRR
    # The selector targets the points-table section.
    rows = soup.select("div[class*='cb-srs-pnts'] table tbody tr")
 
    if not rows:
        # Fallback: try any table on the page
        tables = soup.find_all("table")
        logger.info(f"[STANDINGS] Cricbuzz: found {len(tables)} tables on page (fallback path)")
        if not tables:
            logger.warning("[STANDINGS] Cricbuzz: no tables found on page")
            return None
        # Use pd.read_html as a secondary parse
        result = _parse_table_from_html(html, season)
        if result:
            result["source"] = "cricbuzz"
        return result
 
    teams = []
    for row in rows:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cells) < 7:
            continue
 
        # Cricbuzz column order: Team, M, W, L, NR, Pts, NRR
        raw_name = cells[0]
        name_lower = raw_name.lower()
        code = CB_CODE_MAP.get(name_lower)
 
        if not code:
            # Try partial match
            for key, val in CB_CODE_MAP.items():
                if key in name_lower or name_lower in key:
                    code = val
                    break
 
        # Clean display name
        display_name = raw_name
        for full, c in CB_CODE_MAP.items():
            if c == code and len(full) > 3:
                display_name = full.title()
                break
 
        # Use the IPL_TEAM_NAMES mapping for canonical names
        canonical = {
            "RCB": "Royal Challengers Bengaluru", "CSK": "Chennai Super Kings",
            "MI": "Mumbai Indians", "KKR": "Kolkata Knight Riders",
            "SRH": "Sunrisers Hyderabad", "RR": "Rajasthan Royals",
            "DC": "Delhi Capitals", "PBKS": "Punjab Kings",
            "LSG": "Lucknow Super Giants", "GT": "Gujarat Titans",
        }
        team_display = canonical.get(code, display_name) if code else display_name
 
        try:
            matches  = int(cells[1])
            won      = int(cells[2])
            lost     = int(cells[3])
            nr       = int(cells[4]) if cells[4].isdigit() else 0
            points   = int(cells[5])
            nrr      = _safe_float(cells[6])
        except (ValueError, IndexError):
            logger.warning(f"[STANDINGS] Cricbuzz: failed to parse row cells: {cells}")
            continue
 
        teams.append({
            "team": team_display,
            "code": code,
            "matches": matches,
            "won": won,
            "lost": lost,
            "nr": nr,
            "points": points,
            "nrr": nrr,
        })
 
    if not teams:
        logger.warning("[STANDINGS] Cricbuzz: parsed 0 teams from points table rows")
        return None
 
    logger.info(f"[STANDINGS] ✅ Cricbuzz points table: parsed {len(teams)} teams")
    return {
        "season": season,
        "source": "cricbuzz",
        "last_updated_utc": datetime.utcnow().isoformat() + "Z",
        "teams": teams,
    }
 
def fetch_espn_points_table(season: int) -> Dict[str, Any]:
    """
    Scrape the IPL points table.
    Priority: ESPN URL 1 → ESPN URL 2 → ESPN URL 3 → Cricbuzz → computed from fixtures
    """
    last_error: Exception = StandingsScrapeError("No URLs tried")
 
    urls = [
        ESPN_TABLE_URL_TEMPLATE.format(series_id=IPL_SERIES_ID, season=season),
        f"https://www.espncricinfo.com/series/ipl-{season}-{IPL_SERIES_ID}/points-table-standings",
        f"https://www.espn.in/cricket/series/_/id/{IPL_SERIES_ID}/seasontype/2/standings",
    ]
 
    logger.info(f"[STANDINGS] Starting fetch for season={season} at {datetime.utcnow().isoformat()}Z")

    # ── Try Cricbuzz FIRST (has NRR directly) ────────────────────────────────
    try:
        cb_result = fetch_cricbuzz_points_table(season)
        if cb_result and cb_result.get("teams"):
            logger.info(f"[STANDINGS] ✅ Cricbuzz primary succeeded — {len(cb_result['teams'])} teams")
            return cb_result
        else:
            logger.warning("[STANDINGS] Cricbuzz primary returned no teams, falling back to ESPN")
    except Exception as e:
        logger.error(f"[STANDINGS] Cricbuzz primary failed: {e}, falling back to ESPN")
    # ── END ──────────────────────────────────────────────────────────────────

    for i, url in enumerate(urls, 1):
        try:
            logger.info(f"[STANDINGS] Trying URL {i}/{len(urls)}: {url}")
            html = _fetch_html(url)
            result = _parse_table_from_html(html, season)
            if result and result.get("teams"):
                logger.info(f"[STANDINGS] ✅ Success from URL {i}: {url} — {len(result['teams'])} teams parsed")
                return result
            else:
                logger.warning(f"[STANDINGS] ⚠️ URL {i} returned HTML but parsed 0 teams: {url}")
        except Exception as e:
            logger.error(f"[STANDINGS] ❌ URL {i} failed — {type(e).__name__}: {str(e)} | URL: {url}")
            last_error = e
            continue
 
    
 
    logger.error(f"[STANDINGS] 💀 All sources failed. Falling back to fixture-derived standings.")
    return compute_standings_from_fixtures(season)


def compute_standings_from_fixtures(season: int) -> Dict[str, Any]:
    """Derive points table from fixture data when ESPN scraping fails."""
    from ipl_api.cricbuzz_fixtures import fetch_cricbuzz_innings_aggregates

    try:
        fixture_data = fetch_espn_fixtures(season)
        fixtures = fixture_data.get("fixtures", [])
    except Exception:
        fixtures = HARDCODED_IPL_2026_FIXTURES

    TEAM_NAMES = {
        "RCB": "Royal Challengers Bengaluru", "CSK": "Chennai Super Kings",
        "MI": "Mumbai Indians", "KKR": "Kolkata Knight Riders",
        "SRH": "Sunrisers Hyderabad", "RR": "Rajasthan Royals",
        "DC": "Delhi Capitals", "PBKS": "Punjab Kings",
        "LSG": "Lucknow Super Giants", "GT": "Gujarat Titans",
    }

    teams: Dict[str, Dict[str, Any]] = {}
    for code, name in TEAM_NAMES.items():
        teams[code] = {
            "team": name, "code": code,
            "matches": 0, "won": 0, "lost": 0, "nr": 0,
            "points": 0, "nrr": None,
            "runs_for": 0, "balls_for": 0,
            "runs_against": 0, "balls_against": 0,
        }

    now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)

    # Collect completed pairs for innings fetch
    completed_pairs = []
    for f in fixtures:
        t1, t2, status = f.get("team1_code"), f.get("team2_code"), f.get("status")
        if not t1 or not t2 or t1 not in teams or t2 not in teams:
            continue
        try:
            dt = datetime.fromisoformat(f["date"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            if dt > now_utc:
                continue
        except Exception:
            if status == "upcoming":
                continue
        if status in ("completed", "live"):
            date_only = f["date"][:10]
            completed_pairs.append(f"{t1}-{t2}-{date_only}")

    # Fetch innings aggregates from Cricbuzz scorecards (cached for 10 min)
    innings_map: Dict[str, Any] = {}
    try:
        cache_key = _cache.make_key("innings_aggregates", str(season))
        innings_map = _cache.get(cache_key) or {}
        if not innings_map:
            innings_map = fetch_cricbuzz_innings_aggregates(completed_pairs)
            if innings_map:
                _cache.set(cache_key, innings_map, ttl_seconds=600)
            logger.info(f"[STANDINGS] Innings fetched for {len(innings_map)//2} matches")
        else:
            logger.info(f"[STANDINGS] Innings served from cache ({len(innings_map)//2} matches)")
    except Exception as e:
        logger.warning(f"[STANDINGS] Innings fetch failed (non-fatal): {e}")

    # Build standings from fixtures
    for f in fixtures:
        t1 = f.get("team1_code")
        t2 = f.get("team2_code")
        status = f.get("status")

        if not t1 or not t2 or t1 not in teams or t2 not in teams:
            continue

        try:
            dt = datetime.fromisoformat(f["date"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            if dt > now_utc:
                continue
        except Exception:
            if status == "upcoming":
                continue

        if status == "no_result":
            teams[t1]["matches"] += 1
            teams[t2]["matches"] += 1
            teams[t1]["points"] += 1
            teams[t2]["points"] += 1
            teams[t1]["nr"] += 1
            teams[t2]["nr"] += 1

        elif status in ("completed", "live"):
            winner = f.get("winner_code")
            if not winner:
                continue

            loser = t2 if winner == t1 else t1
            teams[winner]["matches"] += 1
            teams[loser]["matches"] += 1
            teams[winner]["won"] += 1
            teams[loser]["lost"] += 1
            teams[winner]["points"] += 2

            # Add innings aggregates if available
            pair_key = f"{t1}-{t2}"
            innings = innings_map.get(pair_key)
            if innings and t1 in innings and t2 in innings:
                # innings[tX]["runs"] and innings[tX]["balls"] are already keyed by team code
                # so this assignment is correct regardless of batting order
                teams[t1]["runs_for"]      += innings[t1]["runs"]
                teams[t1]["balls_for"]     += innings[t1]["balls"]
                teams[t1]["runs_against"]  += innings[t2]["runs"]
                teams[t1]["balls_against"] += innings[t2]["balls"]

                teams[t2]["runs_for"]      += innings[t2]["runs"]
                teams[t2]["balls_for"]     += innings[t2]["balls"]
                teams[t2]["runs_against"]  += innings[t1]["runs"]
                teams[t2]["balls_against"] += innings[t1]["balls"]

    # ── Calculate NRR from aggregates ──────────────────────────────────────
    for t in teams.values():
        rf = t["runs_for"]
        bf = t["balls_for"]
        ra = t["runs_against"]
        ba = t["balls_against"]
        if bf > 0 and ba > 0:
            t["nrr"] = round((rf / bf * 6) - (ra / ba * 6), 3)
        else:
            t["nrr"] = None  # No innings data yet — display as blank

    sorted_teams = sorted(
        teams.values(),
        key=lambda x: (x["points"], x["nrr"] or 0),
        reverse=True,
    )

    return {
        "season": season,
        "source": "computed_from_fixtures",
        "last_updated_utc": datetime.utcnow().isoformat() + "Z",
        "teams": sorted_teams,
    }