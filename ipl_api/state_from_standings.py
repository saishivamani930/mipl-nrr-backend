# ipl_api/state_from_standings.py
from __future__ import annotations

from typing import Dict, Optional
import os
import re

from ipl_api.nrr_math import TeamAggregate
from ipl_api.points_table import TeamRow

DEBUG_STATE_BUILD = os.getenv("IPL_DEBUG_STATE_BUILD", "0") == "1"

# Accept codes like: GG, UPW, RCB-W, DC-W, MI-W etc.
_CODE_RE = re.compile(r"^[A-Z]{2,6}(?:-[A-Z])?$")


def normalize_team_code(team_raw: str) -> str:
    """
    WPL-only:
    - Prefer trailing short code if present in the raw string (supports DC-W style).
    - Else attempt to find an end-code via regex.
    - Else return cleaned uppercase string.
    """
    if team_raw is None:
        return ""

    s = str(team_raw).strip()
    if not s:
        return ""

    # Remove leading digits (rank)
    s = re.sub(r"^\d+\s*", "", s).strip()
    s = re.sub(r"\s+", " ", s).strip()

    tokens = s.split()
    if tokens:
        last = tokens[-1].strip().upper()
        if _CODE_RE.fullmatch(last):
            return last

    m = re.search(r"([A-Z]{2,6}(?:-[A-Z])?)\s*$", s)
    if m:
        code = m.group(1).upper()
        if _CODE_RE.fullmatch(code):
            return code

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


def build_state_from_standings(standings: dict) -> Dict[str, TeamRow]:
    """
    Convert ESPN standings JSON -> internal state (WPL-only).

    Rules:
      - Prefer `code` from ESPN scraper if present.
      - Else derive code from team display string.
      - STRICT: Require true aggregates (runs/balls for & against) once matches > 0.
        Do NOT reconstruct aggregates from NRR (NRR is not enough information).
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

        # Pre-season / no matches: allow missing aggregates and set zeros.
        if matches == 0:
            agg = TeamAggregate(
                team=team_code,
                runs_for=rf or 0,
                balls_for=bf or 0,
                runs_against=ra or 0,
                balls_against=ba or 0,
            )

        else:
            # Matches started: aggregates must be present for correct NRR simulation.
            if rf is None or bf is None or ra is None or ba is None:
                raise ValueError(
                    f"Cannot build live state for {team_code} because ESPN aggregates are missing. "
                    f"Need runs_for, balls_for, runs_against, balls_against. "
                    f"Got: runs_for={t.get('runs_for')}, balls_for={t.get('balls_for')}, "
                    f"runs_against={t.get('runs_against')}, balls_against={t.get('balls_against')}. "
                    f"This usually means ESPN page did not include 'For/Against' columns or parsing failed."
                )

            agg = TeamAggregate(
                team=team_code,
                runs_for=int(rf),
                balls_for=int(bf),
                runs_against=int(ra),
                balls_against=int(ba),
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
