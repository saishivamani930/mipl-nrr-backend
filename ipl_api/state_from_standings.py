# ipl_api/state_from_standings.py
from __future__ import annotations

from typing import Dict, Optional
import os
import re

from ipl_api.nrr_math import TeamAggregate
from ipl_api.points_table import TeamRow

DEBUG_STATE_BUILD = os.getenv("IPL_DEBUG_STATE_BUILD", "0") == "1"

_CODE_RE = re.compile(r"^[A-Z]{2,6}$")

_TEAM_NAME_MAP: Dict[str, str] = {
    "mumbai indians": "MI",
    "chennai super kings": "CSK",
    "royal challengers bengaluru": "RCB",
    "royal challengers bangalore": "RCB",
    "kolkata knight riders": "KKR",
    "delhi capitals": "DC",
    "sunrisers hyderabad": "SRH",
    "punjab kings": "PBKS",
    "rajasthan royals": "RR",
    "gujarat titans": "GT",
    "lucknow super giants": "LSG",
    "super kings": "CSK",
    "knight riders": "KKR",
    "super giants": "LSG",
    "royals": "RR",
    "titans": "GT",
    "capitals": "DC",
    "indians": "MI",
}

def normalize_team_code(team_raw: str) -> str:
    if team_raw is None:
        return ""

    s = str(team_raw).strip()
    if not s:
        return ""

    s = re.sub(r"^\d+\s*", "", s).strip()
    s = re.sub(r"\s+", " ", s).strip()

    tokens = s.split()
    if tokens:
        last = tokens[-1].strip().upper()
        if _CODE_RE.fullmatch(last) and last in {
            "MI", "CSK", "RCB", "KKR", "DC", "SRH", "PBKS", "RR", "GT", "LSG"
        }:
            return last

    s_lower = s.lower()
    if s_lower in _TEAM_NAME_MAP:
        return _TEAM_NAME_MAP[s_lower]

    best_key = ""
    best_code = ""
    for key, code in _TEAM_NAME_MAP.items():
        if key in s_lower and len(key) > len(best_key):
            best_key = key
            best_code = code
    if best_code:
        return best_code

    return s.strip().upper()


def _safe_int(x: object, default: int = 0) -> int:
    try:
        if x is None:
            return default
        sx = str(x).strip()
        if not sx or sx.lower() == "nan":
            return default
        return int(float(sx))
    except Exception:
        return default


def _to_int_or_none(x: object) -> Optional[int]:
    if x is None:
        return None
    try:
        sx = str(x).strip()
        if not sx or sx.lower() == "nan":
            return None
        return int(float(sx))
    except Exception:
        return None


def state_from_standings(standings: dict, require_aggregates: bool = False) -> Dict[str, TeamRow]:
    """
    Convert ESPN standings JSON -> internal state (IPL-only).

    Args:
        standings: Raw standings dict from ESPN scraper.
        require_aggregates: If True, raise ValueError when a team with matches > 0
            is missing runs/balls aggregates.  Set this to True only when calling
            from the /simulate endpoint.  For the /standings display endpoint, leave
            it False so the table always renders even when ESPN omits For/Against.
    """
    state: Dict[str, TeamRow] = {}
    teams = standings.get("teams", []) or []

    for t in teams:
        raw_code = (t.get("code") or "").strip()
        raw_team = (t.get("team") or "").strip()

        team_code = raw_code.upper() if raw_code else normalize_team_code(raw_team)
        team_code = team_code.strip().upper()
        if not team_code:
            continue

        matches = _safe_int(t.get("matches", 0), 0)
        won = _safe_int(t.get("won", 0), 0)
        lost = _safe_int(t.get("lost", 0), 0)
        nr = _safe_int(t.get("nr", 0), 0)
        tied = _safe_int(t.get("tied", 0), 0)
        points = _safe_int(t.get("points", 0), 0)

        rf = _to_int_or_none(t.get("runs_for"))
        bf = _to_int_or_none(t.get("balls_for"))
        ra = _to_int_or_none(t.get("runs_against"))
        ba = _to_int_or_none(t.get("balls_against"))

        if DEBUG_STATE_BUILD:
            print(
                "[STATE_BUILD]",
                "team_code=", team_code,
                "raw_team=", raw_team,
                "matches=", matches,
                "rf/bf=", rf, bf,
                "ra/ba=", ra, ba,
                "nrr=", t.get("nrr"),
            )

        if matches == 0:
            # Pre-season or no data: always safe to build with zeros
            agg = TeamAggregate(
                team=team_code,
                runs_for=rf or 0,
                balls_for=bf or 0,
                runs_against=ra or 0,
                balls_against=ba or 0,
            )
        elif rf is not None and bf is not None and ra is not None and ba is not None:
            # Full aggregates available — use them
            agg = TeamAggregate(
                team=team_code,
                runs_for=int(rf),
                balls_for=int(bf),
                runs_against=int(ra),
                balls_against=int(ba),
            )
        elif require_aggregates:
            # Caller explicitly requires aggregates (simulation path)
            raise ValueError(
                f"Cannot build live state for {team_code} because ESPN aggregates are missing. "
                f"Need runs_for, balls_for, runs_against, balls_against. "
                f"Got: runs_for={t.get('runs_for')}, balls_for={t.get('balls_for')}, "
                f"runs_against={t.get('runs_against')}, balls_against={t.get('balls_against')}. "
                f"This usually means ESPN page did not include 'For/Against' columns or parsing failed."
            )
        else:
            # Aggregates missing but caller doesn't need them (display path) — use zeros.
            # NRR shown will be the scraped NRR from ESPN, not recalculated.
            agg = TeamAggregate(
                team=team_code,
                runs_for=0,
                balls_for=0,
                runs_against=0,
                balls_against=0,
            )

        state[team_code] = TeamRow(
            team=team_code,
            played=matches,
            won=won,
            lost=lost,
            nr=nr,
            tied=tied,
            points=points,
            agg=agg,
        )

    return state