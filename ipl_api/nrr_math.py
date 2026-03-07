# ipl_api/nrr_math.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Union

MAX_BALLS_T20 = 20 * 6  # 120
OversLike = Union[str, int, float]


@dataclass
class TeamAggregate:
    """
    Aggregate stats needed for NRR.
    All overs are stored as BALLS (not float overs) to avoid mistakes.
    """
    team: str
    runs_for: int = 0
    balls_for: int = 0
    runs_against: int = 0
    balls_against: int = 0


def overs_to_balls(overs: OversLike) -> int:
    """
    Converts cricket overs notation to balls.

    Supported inputs:
    - "20.0", "19.4", "7.2" (string overs notation)
    - 20 (int overs)
    - 19.4 (float) -> treated as "19.4" (NOTE: float precision issues possible; strings preferred)

    Rule: ".x" means x balls (0-5). Example: 19.4 = 19*6 + 4 = 118 balls.
    """
    if overs is None:
        raise ValueError("Overs cannot be None")

    s = str(overs).strip()
    if not s:
        raise ValueError("Overs cannot be empty")

    # Allow plain integer overs "20"
    if "." not in s:
        ov_i = int(s)
        if ov_i < 0:
            raise ValueError(f"Invalid overs: {overs}")
        return ov_i * 6

    ov_part, ball_part = s.split(".", 1)
    ov_i = int(ov_part) if ov_part else 0

    # Some feeds show "19.0" etc.
    ball_part = ball_part.strip()
    if ball_part == "":
        balls_i = 0
    else:
        # overs notation always has 0..5 balls
        balls_i = int(ball_part)

    if ov_i < 0:
        raise ValueError(f"Invalid overs: {overs}")
    if balls_i < 0 or balls_i > 5:
        raise ValueError(f"Invalid overs format: {overs} (balls part must be 0-5)")

    return ov_i * 6 + balls_i


def balls_to_overs_float(balls: int) -> float:
    if balls <= 0:
        return 0.0
    return balls / 6.0


def run_rate(runs: int, balls: int) -> float:
    overs = balls_to_overs_float(balls)
    if overs == 0.0:
        return 0.0
    return runs / overs


def nrr(agg: TeamAggregate) -> float:
    """
    Net Run Rate = (runs_for / overs_for) - (runs_against / overs_against)
    """
    rr_for = run_rate(agg.runs_for, agg.balls_for)
    rr_against = run_rate(agg.runs_against, agg.balls_against)
    return rr_for - rr_against


def normalize_innings_balls(balls: int, all_out: bool) -> int:
    """
    IPL NRR rule: if a team is all-out, innings counts as full 20 overs (120 balls).
    Otherwise, use actual balls faced.

    Note:
    - For chases completed early (e.g., 18.3), use actual balls unless all-out.
    """
    if balls < 0:
        raise ValueError("Balls cannot be negative")
    if balls == 0:
        # 0 balls innings should not be applied to aggregates (e.g., abandoned/NR).
        # Caller should skip aggregate update in those cases.
        return 0
    return MAX_BALLS_T20 if all_out else balls


def innings_balls(overs: OversLike, *, all_out: bool) -> int:
    """
    Convenience helper: parse overs -> balls, then apply all-out normalization.
    """
    raw = overs_to_balls(overs)
    return normalize_innings_balls(raw, all_out)


def apply_match_batting_first(
    agg_team1: TeamAggregate,
    agg_team2: TeamAggregate,
    team1_runs: int,
    team1_overs: OversLike,
    team2_runs: int,
    team2_overs: OversLike,
    team1_balls_override: Optional[int] = None,
    team2_balls_override: Optional[int] = None,
) -> None:
    """
    Backward-compatible function (your current code already calls this).

    Updates aggregates for a match where:
    - Team1 batted first (team1_runs in team1_overs)
    - Team2 batted second (team2_runs in team2_overs)

    Overrides are used to apply all-out normalization (120 balls) if upstream already computed it.
    Prefer using apply_match(...) moving forward.
    """
    t1_balls = team1_balls_override if team1_balls_override is not None else overs_to_balls(team1_overs)
    t2_balls = team2_balls_override if team2_balls_override is not None else overs_to_balls(team2_overs)

    if t1_balls <= 0 or t2_balls <= 0:
        raise ValueError("Cannot apply match with <= 0 balls. For NR/abandoned, skip aggregate update.")

    # Team1 aggregates
    agg_team1.runs_for += int(team1_runs)
    agg_team1.balls_for += int(t1_balls)
    agg_team1.runs_against += int(team2_runs)
    agg_team1.balls_against += int(t2_balls)

    # Team2 aggregates
    agg_team2.runs_for += int(team2_runs)
    agg_team2.balls_for += int(t2_balls)
    agg_team2.runs_against += int(team1_runs)
    agg_team2.balls_against += int(t1_balls)


def apply_match(
    agg_team1: TeamAggregate,
    agg_team2: TeamAggregate,
    *,
    team1_runs: int,
    team1_overs: OversLike,
    team2_runs: int,
    team2_overs: OversLike,
    team1_all_out: bool = False,
    team2_all_out: bool = False,
) -> None:
    """
    Canonical aggregate updater for a completed innings match.

    - Applies IPL all-out normalization internally
    - Uses overs notation inputs for both innings
    - DOES NOT handle NR/abandoned: caller must not call this for NR.

    Assumption:
    - team1 is innings-1, team2 is innings-2 (batting order matters only for inputs, NRR is symmetric)
    """
    t1_balls = innings_balls(team1_overs, all_out=team1_all_out)
    t2_balls = innings_balls(team2_overs, all_out=team2_all_out)

    if t1_balls <= 0 or t2_balls <= 0:
        raise ValueError("Cannot apply match with <= 0 balls. For NR/abandoned, skip aggregate update.")

    apply_match_batting_first(
        agg_team1,
        agg_team2,
        team1_runs=int(team1_runs),
        team1_overs=str(team1_overs),
        team2_runs=int(team2_runs),
        team2_overs=str(team2_overs),
        team1_balls_override=t1_balls,
        team2_balls_override=t2_balls,
    )
