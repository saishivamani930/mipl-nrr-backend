from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

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


def fetch_espn_points_table(season: int) -> Dict[str, Any]:

    url = ESPN_TABLE_URL_TEMPLATE.format(series_id=IPL_SERIES_ID, season=season)

    try:

        r = requests.get(
            url,
            timeout=20,
            headers={"User-Agent":"Mozilla/5.0"}
        )

        r.raise_for_status()

    except Exception as e:
        raise StandingsScrapeError(f"ESPN fetch failed: {e}") from e

    try:

        tables = pd.read_html(StringIO(r.text))

    except Exception:

        return {
            "season":season,
            "teams":[]
        }

    if not tables:

        return {
            "season":season,
            "teams":[]
        }

    df = _pick_points_table(tables)
    df = _flatten_columns(df)

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

    teams = []

    for _, row in df.iterrows():

        team_name, team_code = _clean_team_cell(row.get("team"))

        if not team_name:
            continue

        points_val, nrr_val = _maybe_split_points_nrr(row)

        item = {
            "team":team_name,
            "code":team_code,
            "matches":_safe_int(row.get("matches")),
            "won":_safe_int(row.get("won")),
            "lost":_safe_int(row.get("lost")),
            "points":int(points_val),
            "nrr":nrr_val
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

    return {
        "season":season,
        "source":"espn",
        "last_updated_utc":datetime.utcnow().isoformat()+"Z",
        "teams":teams
    }