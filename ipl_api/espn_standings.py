# ipl_api/espn_standings.py
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

from ipl_api.config import WPL_SERIES_ID, ESPN_TABLE_URL_TEMPLATE
from ipl_api.nrr_math import overs_to_balls

from io import StringIO


class StandingsScrapeError(Exception):
    """Raised when ESPN standings scraping/parsing fails."""
    pass


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols: List[str] = []
    for c in df.columns:
        if isinstance(c, tuple):
            c = " ".join([str(x) for x in c if x and str(x) != "nan"]).strip()
        cols.append(str(c).strip())
    df.columns = cols
    return df


def _pick_points_table(tables: List[pd.DataFrame]) -> pd.DataFrame:
    """
    ESPN pages sometimes contain multiple HTML tables.
    We pick the best match by scoring expected points-table columns.
    """
    best = None
    best_score = -1

    for t in tables:
        t = _flatten_columns(t.copy())
        cols = [str(c).strip().lower() for c in t.columns]

        score = 0
        if any("team" in c for c in cols):
            score += 3
        if any("pts" in c or "points" in c or c in ("pt", "pts") for c in cols):
            score += 3
        if any("nrr" in c for c in cols):
            score += 3
        if any(c == "for" or c.endswith(" for") for c in cols):
            score += 2
        if any(c == "against" or "against" in c for c in cols):
            score += 2
        if any(c in ("m", "mat", "matches") or "mat" in c for c in cols):
            score += 1
        if any(c in ("w", "won") for c in cols):
            score += 1
        if any(c in ("l", "lost") for c in cols):
            score += 1
        if any(c in ("t", "tie", "tied") for c in cols):
            score += 1
        if any(c in ("n/r", "nr", "no result", "no results") or "no result" in c for c in cols):
            score += 1

        if score > best_score:
            best_score = score
            best = t

    return best if best is not None else _flatten_columns(tables[0].copy())


def _parse_runs_overs_cell(val: Any) -> Optional[Tuple[int, int]]:
    """
    Parse strings like:
      "831/90.3" -> (runs=831, balls=overs_to_balls("90.3"))
      "730/100"  -> (runs=730, balls=overs_to_balls("100.0"))
    """
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
    """
    ESPN team cell examples (WPL):
      "1Royal Challengers Bengaluru Women RCB-W"
      "1Image Mumbai Indians Women MI-W"
      "Delhi Capitals Women DC-W"

    Returns:
      (team_name="Royal Challengers Bengaluru Women", code="RCB-W")
    """
    if raw is None:
        return "", None

    s = str(raw).strip()
    if not s or s.lower() == "nan":
        return "", None

    s = s.replace("Image", " ").strip()

    # Insert spaces where ESPN concatenates tokens:
    s = re.sub(r"(?<=\d)(?=[A-Za-z])", " ", s)
    s = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", s)

    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"^\d+\s*", "", s).strip()

    tokens = s.split()
    if not tokens:
        return "", None

    code = None
    last = tokens[-1].strip().upper()

    # WPL codes commonly like "RCB-W", "MI-W", "DC-W", "GG", "UPW", etc.
    if re.fullmatch(r"[A-Z]{2,6}(?:-W)?", last):
        code = last
        name = " ".join(tokens[:-1]).strip()
    else:
        name = s

    name = name.strip()
    if not name:
        return "", code

    return name, code


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        sx = str(x).strip()
        if not sx or sx.lower() == "nan":
            return default

        m = re.match(r"^(\d+)", sx)
        if m:
            return int(m.group(1))

        return int(float(sx))
    except Exception:
        return default


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        sx = str(x).strip()
        if not sx or sx.lower() == "nan":
            return None
        sx = sx.replace("−", "-")
        return float(sx)
    except Exception:
        return None


def _maybe_split_points_nrr(row: pd.Series) -> Tuple[int, Optional[float]]:
    """
    Defensive fix: sometimes HTML renders like '4-0.483' inside points column.
    If points looks like '<int><sign><float>' and NRR is missing, split it.
    """
    pts_raw = row.get("points")
    nrr_raw = row.get("nrr")

    pts_str = "" if pts_raw is None else str(pts_raw).strip().replace("−", "-")
    nrr_val = _safe_float(nrr_raw) if nrr_raw is not None else None

    m = re.match(r"^(\d+)([-+]\d+(?:\.\d+)?)$", pts_str)
    if m and nrr_val is None:
        return int(m.group(1)), float(m.group(2))

    return _safe_int(pts_raw, 0), nrr_val


def fetch_espn_points_table(season: int) -> Dict[str, Any]:
    url = ESPN_TABLE_URL_TEMPLATE.format(series_id=WPL_SERIES_ID, season=season)

    try:
        r = requests.get(
            url,
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0 (compatible; WPL-NRR-Backend/1.0)"},
        )
        r.raise_for_status()
    except Exception as e:
        raise StandingsScrapeError(f"ESPN fetch failed: {e}") from e

    try:
        tables = pd.read_html(StringIO(r.text))
    except Exception:
        return {
            "season": season,
            "source": "espn",
            "last_updated_utc": datetime.utcnow().isoformat() + "Z",
            "teams": [],
            "note": "Points table not available yet (pre-season or page structure changed).",
        }

    if not tables:
        return {
            "season": season,
            "source": "espn",
            "last_updated_utc": datetime.utcnow().isoformat() + "Z",
            "teams": [],
            "note": "Points table not available yet (pre-season).",
        }

    df = _pick_points_table(tables)
    df = _flatten_columns(df)

    # Normalize column names
    colmap: Dict[str, str] = {}
    for c in df.columns:
        lc = str(c).strip().lower()

        if "team" in lc:
            colmap[c] = "team"
        elif lc in ("mat", "matches", "m") or "mat" in lc:
            colmap[c] = "matches"
        elif lc in ("won", "w"):
            colmap[c] = "won"
        elif lc in ("lost", "l"):
            colmap[c] = "lost"
        elif lc in ("tied", "tie", "t") or "tied" in lc:
            colmap[c] = "tied"
        elif lc in ("nr", "n/r", "no result", "no results") or "no result" in lc:
            colmap[c] = "nr"
        elif lc in ("pt", "pts", "points", "p") or "pts" in lc or "points" in lc:
            colmap[c] = "points"
        elif "nrr" in lc:
            colmap[c] = "nrr"
        elif lc == "for" or lc.endswith(" for"):
            colmap[c] = "for"
        elif lc == "against" or "against" in lc:
            colmap[c] = "against"

    df = df.rename(columns=colmap)

    required = {"team", "matches", "won", "lost", "points"}
    if not required.issubset(set(df.columns)):
        return {
            "season": season,
            "source": "espn",
            "last_updated_utc": datetime.utcnow().isoformat() + "Z",
            "teams": [],
            "note": f"Points table core columns not available for season={season}. Parsed columns={list(df.columns)}",
        }

    teams: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        team_name, team_code = _clean_team_cell(row.get("team"))
        if not team_name:
            continue

        points_val, nrr_val = _maybe_split_points_nrr(row)

        item: Dict[str, Any] = {
            "team": team_name,
            "code": team_code,
            "matches": _safe_int(row.get("matches"), 0),
            "won": _safe_int(row.get("won"), 0),
            "lost": _safe_int(row.get("lost"), 0),
            "nr": _safe_int(row.get("nr"), 0) if "nr" in df.columns else 0,
            "tied": _safe_int(row.get("tied"), 0) if "tied" in df.columns else 0,
            "points": int(points_val),
            "nrr": nrr_val if "nrr" in df.columns else None,
        }

        # Optional aggregates if present
        if "for" in df.columns:
            parsed = _parse_runs_overs_cell(row.get("for"))
            if parsed:
                item["runs_for"], item["balls_for"] = parsed

        if "against" in df.columns:
            parsed = _parse_runs_overs_cell(row.get("against"))
            if parsed:
                item["runs_against"], item["balls_against"] = parsed

        teams.append(item)

    resp: Dict[str, Any] = {
        "season": season,
        "source": "espn",
        "last_updated_utc": datetime.utcnow().isoformat() + "Z",
        "teams": teams,
    }

    if teams and not any(("runs_for" in t and "runs_against" in t) for t in teams):
        resp["note"] = f"For/Against aggregates not present/parsed. Parsed columns={list(df.columns)}"

    return resp
