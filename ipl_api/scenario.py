# ipl_api/scenario.py
from __future__ import annotations

import copy
import random
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Literal

from ipl_api.points_table import TeamRow, compute_sorted_table, apply_result
from ipl_api.simulator import simulate_match

ResultType = Literal["WIN", "NR", "TIE"]

# WPL playoffs: Top-3 qualify
PLAYOFF_SPOTS = 3


@dataclass
class Fixture:
    team1: str
    team2: str

    # Match outcome type:
    # - "WIN": normal match, needs a winner (either provided or simulated randomly)
    # - "NR" : no result/abandoned (points split, no NRR update)
    # - "TIE": tie with no winner (points split, NRR update only if innings provided)
    result: ResultType = "WIN"

    # Winner only used when result == "WIN"
    winner: Optional[str] = None

    # Optional detailed innings (if you want NRR changes).
    # If you provide these, we will call simulate_match() for NRR-accurate update.
    team1_runs: Optional[int] = None
    team1_overs: Optional[str] = None
    team2_runs: Optional[int] = None
    team2_overs: Optional[str] = None

    # Optional all-out flags (IPL NRR rule: all-out counts as full 20 overs)
    team1_all_out: bool = False
    team2_all_out: bool = False


def _apply_points_only(
    state: Dict[str, TeamRow],
    team1: str,
    team2: str,
    *,
    result: ResultType,
    winner: Optional[str],
) -> None:
    """
    Updates played/won/lost/nr/tied/points ONLY.
    NRR aggregates remain unchanged.

    This is useful for Monte Carlo qualification simulations (fast).
    """
    row1 = state[team1]
    row2 = state[team2]

    # apply_result handles validation for WIN/NR/TIE
    apply_result(row1, row2, result=result, winner=winner)


def _run_one_simulation(
    base_state: Dict[str, TeamRow],
    fixtures: List[Fixture],
    seed: Optional[int] = None,
) -> List[dict]:
    """
    Returns final sorted table after applying all fixtures once.
    """
    if seed is not None:
        random.seed(seed)

    state = copy.deepcopy(base_state)

    for fx in fixtures:
        if fx.team1 not in state or fx.team2 not in state:
            raise ValueError(f"Unknown team in fixture: {fx.team1} vs {fx.team2}")
        if fx.team1 == fx.team2:
            raise ValueError("Fixture teams must be different")

        if fx.result not in ("WIN", "NR", "TIE"):
            raise ValueError(f"Invalid fixture result: {fx.result}")

        # Decide winner only if result == WIN
        winner: Optional[str] = fx.winner
        if fx.result == "WIN":
            if winner is None:
                winner = random.choice([fx.team1, fx.team2])
            if winner not in (fx.team1, fx.team2):
                raise ValueError(f"winner must be either {fx.team1} or {fx.team2}")
        else:
            # NR/TIE ignore winner
            winner = None

        # If full innings provided -> do NRR-accurate update via simulate_match()
        has_innings = (
            fx.team1_runs is not None and fx.team1_overs is not None and
            fx.team2_runs is not None and fx.team2_overs is not None
        )

        if fx.result == "NR":
            # NR should NOT change NRR aggregates in IPL/WPL (no innings completed).
            # Points-only update is correct.
            _apply_points_only(state, fx.team1, fx.team2, result="NR", winner=None)
            continue

        if has_innings:
            # simulate_match should apply points + aggregates correctly.
            # It must support result="WIN"/"TIE" and winner when needed.
            simulate_match(
                state=state,
                team1=fx.team1,
                team2=fx.team2,
                team1_runs=int(fx.team1_runs),
                team1_overs=str(fx.team1_overs),
                team2_runs=int(fx.team2_runs),
                team2_overs=str(fx.team2_overs),
                team1_all_out=bool(fx.team1_all_out),
                team2_all_out=bool(fx.team2_all_out),
                result=fx.result,
                winner=winner,
            )
        else:
            # Points-only update (fast for Monte Carlo)
            _apply_points_only(state, fx.team1, fx.team2, result=fx.result, winner=winner)

    return compute_sorted_table(list(state.values()))


def monte_carlo_qualification(
    base_state: Dict[str, TeamRow],
    fixtures: List[Fixture],
    iterations: int = 2000,
) -> Dict[str, Any]:
    """
    Monte Carlo:
    - Random outcomes for fixtures where:
        - result == WIN and winner not provided -> random winner
    - Deterministic for fixtures where winner is provided
    - Supports NR/TIE via Fixture.result

    Notes:
    - If fixtures don’t include innings, this becomes "points-only":
      NRR tie-breaker will remain as base-state NRR (still used for sorting).
    """
    if iterations <= 0:
        raise ValueError("iterations must be > 0")

    top3_count: Dict[str, int] = {t: 0 for t in base_state.keys()}
    top2_count: Dict[str, int] = {t: 0 for t in base_state.keys()}
    rank_hist: Dict[str, Dict[int, int]] = {t: {} for t in base_state.keys()}

    for _ in range(iterations):
        final_table = _run_one_simulation(base_state, fixtures)

        teams_in_order = [row["team"] for row in final_table]

        for idx, team in enumerate(teams_in_order, start=1):
            rank_hist[team][idx] = rank_hist[team].get(idx, 0) + 1

        for team in teams_in_order[:PLAYOFF_SPOTS]:
            top3_count[team] += 1
        for team in teams_in_order[:2]:
            top2_count[team] += 1

    top3_prob = {t: top3_count[t] / iterations for t in top3_count}
    top2_prob = {t: top2_count[t] / iterations for t in top2_count}

    return {
        "iterations": iterations,
        "top3_probability": top3_prob,
        # Backward-compatible alias (kept to avoid breaking older frontend)
        "top4_probability": top3_prob,
        "top2_probability": top2_prob,
        "rank_histogram": rank_hist,
        "playoff_spots": PLAYOFF_SPOTS,
        "notes": (
            "If fixtures don’t include innings, this is points-only; "
            "NRR remains base-state NRR as tie-breaker."
        ),
    }   
