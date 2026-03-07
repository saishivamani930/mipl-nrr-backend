# ipl_api/simulator.py
from __future__ import annotations

from typing import Dict, List, Optional, Literal

from ipl_api.nrr_math import (
    TeamAggregate,
    overs_to_balls,
    normalize_innings_balls,
    apply_match_batting_first,
)
from ipl_api.points_table import TeamRow, compute_sorted_table, apply_result

ResultType = Literal["WIN", "NR", "TIE"]


def create_mock_ipl_table() -> List[dict]:
    """
    Returns a LIST of dict rows (for qualification endpoint logic).
    Mock only.
    """
    return [
        {"pos": 1, "team": "KKR", "played": 13, "won": 8, "lost": 5, "nr": 0, "tied": 0, "points": 16, "nrr": 0.576923, "runs_for": 2200, "balls_for": 1560, "runs_against": 2050, "balls_against": 1560},
        {"pos": 2, "team": "GT",  "played": 13, "won": 8, "lost": 5, "nr": 0, "tied": 0, "points": 16, "nrr": 0.538462, "runs_for": 2180, "balls_for": 1560, "runs_against": 2040, "balls_against": 1560},
        {"pos": 3, "team": "CSK", "played": 13, "won": 7, "lost": 6, "nr": 0, "tied": 0, "points": 14, "nrr": 0.192308, "runs_for": 2100, "balls_for": 1560, "runs_against": 2050, "balls_against": 1560},
        {"pos": 4, "team": "RCB", "played": 13, "won": 7, "lost": 6, "nr": 0, "tied": 0, "points": 14, "nrr": 0.192308, "runs_for": 2150, "balls_for": 1560, "runs_against": 2100, "balls_against": 1560},
        {"pos": 5, "team": "LSG", "played": 13, "won": 7, "lost": 6, "nr": 0, "tied": 0, "points": 14, "nrr": 0.153846, "runs_for": 2120, "balls_for": 1560, "runs_against": 2080, "balls_against": 1560},
        {"pos": 6, "team": "MI",  "played": 13, "won": 6, "lost": 7, "nr": 0, "tied": 0, "points": 12, "nrr": -0.076923, "runs_for": 2080, "balls_for": 1560, "runs_against": 2100, "balls_against": 1560},
        {"pos": 7, "team": "DC",  "played": 13, "won": 6, "lost": 7, "nr": 0, "tied": 0, "points": 12, "nrr": -0.076923, "runs_for": 2070, "balls_for": 1560, "runs_against": 2090, "balls_against": 1560},
        {"pos": 8, "team": "RR",  "played": 13, "won": 6, "lost": 7, "nr": 0, "tied": 0, "points": 12, "nrr": -0.192308, "runs_for": 2050, "balls_for": 1560, "runs_against": 2100, "balls_against": 1560},
        {"pos": 9, "team": "SRH", "played": 13, "won": 5, "lost": 8, "nr": 0, "tied": 0, "points": 10, "nrr": -0.576923, "runs_for": 2000, "balls_for": 1560, "runs_against": 2150, "balls_against": 1560},
        {"pos": 10, "team": "PBKS", "played": 13, "won": 5, "lost": 8, "nr": 0, "tied": 0, "points": 10, "nrr": -0.538462, "runs_for": 1980, "balls_for": 1560, "runs_against": 2120, "balls_against": 1560},
    ]


def create_mock_ipl_state() -> Dict[str, TeamRow]:
    """
    Returns a STATE dict of TeamRow (for match simulation + NRR).
    Mock only.
    """
    return {
        "KKR": TeamRow("KKR", 13, 8, 5, 0, 0, 16, TeamAggregate("KKR", 2200, 1560, 2050, 1560)),
        "GT":  TeamRow("GT",  13, 8, 5, 0, 0, 16, TeamAggregate("GT", 2180, 1560, 2040, 1560)),
        "CSK": TeamRow("CSK", 13, 7, 6, 0, 0, 14, TeamAggregate("CSK", 2100, 1560, 2050, 1560)),
        "RCB": TeamRow("RCB", 13, 7, 6, 0, 0, 14, TeamAggregate("RCB", 2150, 1560, 2100, 1560)),
        "LSG": TeamRow("LSG", 13, 7, 6, 0, 0, 14, TeamAggregate("LSG", 2120, 1560, 2080, 1560)),
        "MI":  TeamRow("MI",  13, 6, 7, 0, 0, 12, TeamAggregate("MI", 2080, 1560, 2100, 1560)),
        "DC":  TeamRow("DC",  13, 6, 7, 0, 0, 12, TeamAggregate("DC", 2070, 1560, 2090, 1560)),
        "RR":  TeamRow("RR",  13, 6, 7, 0, 0, 12, TeamAggregate("RR", 2050, 1560, 2100, 1560)),
        "SRH": TeamRow("SRH", 13, 5, 8, 0, 0, 10, TeamAggregate("SRH", 2000, 1560, 2150, 1560)),
        "PBKS": TeamRow("PBKS", 13, 5, 8, 0, 0, 10, TeamAggregate("PBKS", 1980, 1560, 2120, 1560)),
    }


def simulate_match(
    state: Dict[str, TeamRow],
    team1: str,
    team2: str,
    team1_runs: Optional[int] = None,
    team1_overs: Optional[str] = None,
    team2_runs: Optional[int] = None,
    team2_overs: Optional[str] = None,
    team1_all_out: bool = False,
    team2_all_out: bool = False,
    *,
    result: ResultType = "WIN",
    winner: Optional[str] = None,
) -> List[dict]:
    """
    Canonical match simulator update.

    Conventions:
    - team1 bats first, team2 bats second.

    Rules:
    - NR: points split, NO aggregate update.
    - TIE: points split; if innings provided, aggregates update using actual runs/balls.
    - WIN: if innings provided, winner is derived from scores; else must be passed explicitly.
    """
    if team1 not in state or team2 not in state:
        raise ValueError("Unknown team(s) in match")
    if team1 == team2:
        raise ValueError("team1 and team2 must be different")

    row1 = state[team1]
    row2 = state[team2]

    if result == "NR":
        apply_result(row1, row2, result="NR", winner=None)
        return compute_sorted_table(list(state.values()))

    has_innings = (
        team1_runs is not None and team1_overs is not None and
        team2_runs is not None and team2_overs is not None
    )

    if has_innings:
        b1 = overs_to_balls(str(team1_overs))
        b2 = overs_to_balls(str(team2_overs))

        b1_norm = normalize_innings_balls(b1, bool(team1_all_out))
        b2_norm = normalize_innings_balls(b2, bool(team2_all_out))

        apply_match_batting_first(
            row1.agg,
            row2.agg,
            int(team1_runs),
            str(team1_overs),
            int(team2_runs),
            str(team2_overs),
            team1_balls_override=b1_norm,
            team2_balls_override=b2_norm,
        )

    if result == "TIE":
        if has_innings and int(team1_runs) != int(team2_runs):
            raise ValueError("result='TIE' requires equal scores when innings are provided")
        apply_result(row1, row2, result="TIE", winner=None)
        return compute_sorted_table(list(state.values()))

    if result != "WIN":
        raise ValueError(f"Invalid result: {result}")

    # result == "WIN"
    if has_innings:
        if int(team1_runs) > int(team2_runs):
            winner = team1
        elif int(team2_runs) > int(team1_runs):
            winner = team2
        else:
            raise ValueError("Scores are tied; use result='TIE'")

    if winner is None:
        raise ValueError("winner is required when result='WIN' and innings do not determine winner")

    apply_result(row1, row2, result="WIN", winner=winner)
    return compute_sorted_table(list(state.values()))
