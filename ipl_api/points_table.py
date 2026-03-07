# ipl_api/points_table.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Literal

from ipl_api.nrr_math import TeamAggregate, nrr

ResultType = Literal["WIN", "NR", "TIE"]


@dataclass
class TeamRow:
    team: str
    played: int
    won: int
    lost: int
    nr: int
    tied: int
    points: int
    agg: TeamAggregate


def make_table_from_rows(rows: List[TeamRow]) -> Dict[str, TeamRow]:
    """Convenience helper: map team -> TeamRow"""
    return {r.team: r for r in rows}


def compute_sorted_table(rows: List[TeamRow]) -> List[dict]:
    """
    Returns points table sorted by:
    1) Points (desc)
    2) NRR (desc)
    """
    def key_fn(r: TeamRow):
        return (r.points, nrr(r.agg))

    sorted_rows = sorted(rows, key=key_fn, reverse=True)

    out: List[dict] = []
    for idx, r in enumerate(sorted_rows, start=1):
        out.append({
            "pos": idx,
            "team": r.team,
            "played": r.played,
            "won": r.won,
            "lost": r.lost,
            "nr": r.nr,
            "tied": r.tied,
            "points": r.points,
            "nrr": round(nrr(r.agg), 6),
            "runs_for": r.agg.runs_for,
            "balls_for": r.agg.balls_for,
            "runs_against": r.agg.runs_against,
            "balls_against": r.agg.balls_against,
        })
    return out


def apply_result(
    row_a: TeamRow,
    row_b: TeamRow,
    *,
    result: ResultType = "WIN",
    winner: Optional[str] = None,
) -> None:
    """
    Updates played/won/lost/nr/tied/points ONLY.
    Aggregates are updated separately via nrr_math.apply_match_* using real runs/balls.

    Rules:
    - WIN: winner must be row_a.team or row_b.team, points = 2 to winner
    - NR : both get 1 point, nr += 1
    - TIE: both get 1 point, tied += 1
    """
    row_a.played += 1
    row_b.played += 1

    if result == "NR":
        row_a.nr += 1
        row_b.nr += 1
        row_a.points += 1
        row_b.points += 1
        return

    if result == "TIE":
        row_a.tied += 1
        row_b.tied += 1
        row_a.points += 1
        row_b.points += 1
        return

    if result != "WIN":
        raise ValueError(f"Invalid result: {result}")

    if winner is None:
        raise ValueError("winner is required when result='WIN'")

    if winner == row_a.team:
        row_a.won += 1
        row_a.points += 2
        row_b.lost += 1
    elif winner == row_b.team:
        row_b.won += 1
        row_b.points += 2
        row_a.lost += 1
    else:
        raise ValueError("winner must be either team A or team B")
