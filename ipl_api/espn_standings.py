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
        elif lc == "for":
            colmap[c] = "for"
        elif "against" in lc:
            colmap[c] = "against"

    df = df.rename(columns=colmap)
    logger.info(f"[STANDINGS] Mapped columns: {list(df.columns)}")

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


def fetch_espn_points_table(season: int) -> Dict[str, Any]:
    """
    Scrape the IPL points table from ESPN.

    Tries multiple URL patterns so that at least one page layout returns
    the table with NRR. The For/Against aggregate columns are captured when
    available but are not required — their absence only affects live-NRR
    simulation, not the standings display.
    """
    last_error: Exception = StandingsScrapeError("No URLs tried")

    urls = [
        ESPN_TABLE_URL_TEMPLATE.format(series_id=IPL_SERIES_ID, season=season),
        f"https://www.espncricinfo.com/series/ipl-{season}-{IPL_SERIES_ID}/points-table-standings",
        f"https://www.espn.in/cricket/series/_/id/{IPL_SERIES_ID}/seasontype/2/standings",
    ]

    logger.info(f"[STANDINGS] Starting fetch for season={season} at {datetime.utcnow().isoformat()}Z")

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

    logger.error(f"[STANDINGS] 💀 All {len(urls)} URLs failed at {datetime.utcnow().isoformat()}Z. Last error: {last_error}")
    raise StandingsScrapeError(f"All ESPN table URLs failed. Last error: {last_error}") from last_error