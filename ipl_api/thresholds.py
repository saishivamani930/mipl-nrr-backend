# ipl_api/thresholds.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Literal

from ipl_api.points_table import TeamRow
from ipl_api.simulator import simulate_match

Mode = Literal[
    "CHASE_LOSS_MIN_SCORE",     # (1) chasing + losing: min score to stay above competitor
    "DEFEND_WIN_MAX_OPP_SCORE", # (2) defending + winning: max opponent score allowed
    "CHASE_WIN_MAX_BALLS",      # (3) chasing + winning: max balls to stay above competitor
]

MAX_BALLS_T20 = 120


def _clone_state(state: Dict[str, TeamRow]) -> Dict[str, TeamRow]:
    """
    Deep-ish copy: TeamRow + TeamAggregate copied so we don't mutate the original base_state.
    """
    out: Dict[str, TeamRow] = {}
    for k, r in state.items():
        out[k] = TeamRow(
            team=r.team,
            played=r.played,
            won=r.won,
            lost=r.lost,
            nr=r.nr,
            tied=r.tied,
            points=r.points,
            agg=type(r.agg)(
                team=r.agg.team,
                runs_for=r.agg.runs_for,
                balls_for=r.agg.balls_for,
                runs_against=r.agg.runs_against,
                balls_against=r.agg.balls_against,
            ),
        )
    return out


def _pos_map(sorted_table: List[dict]) -> Dict[str, int]:
    return {row["team"]: int(row["pos"]) for row in sorted_table}


def _is_above(sorted_table: List[dict], focus: str, competitor: str) -> bool:
    pm = _pos_map(sorted_table)
    if focus not in pm or competitor not in pm:
        raise ValueError("focus/competitor not present in table after simulation")
    return pm[focus] < pm[competitor]  # smaller pos => above


def _balls_to_overs_str(balls: int) -> str:
    # balls=119 => "19.5"
    if balls <= 0:
        return "0.0"
    o = balls // 6
    b = balls % 6
    return f"{o}.{b}"


@dataclass
class ThresholdResult:
    ok: bool
    mode: Mode
    focus: str
    opponent: str
    competitor: str
    reason: str
    value: Optional[int] = None      # runs or balls depending on mode
    details: Optional[dict] = None


def chase_loss_min_score(
    *,
    base_state: Dict[str, TeamRow],
    chasing_team: str,
    opponent_team: str,
    target_team: str,
    target_score: int,
    assume_chase_balls: int = 120,
) -> ThresholdResult:
    """
    (1) Chasing team loses while chasing target_score.
    Find minimum runs (0..target_score-1) the chasing team must score
    (assuming they use assume_chase_balls) so they still stay above target_team.
    """
    focus = chasing_team.strip().upper()
    opponent = opponent_team.strip().upper()
    competitor = target_team.strip().upper()

    if target_score <= 0:
        return ThresholdResult(False, "CHASE_LOSS_MIN_SCORE", focus, opponent, competitor, "target_score must be > 0")

    if assume_chase_balls < 1 or assume_chase_balls > MAX_BALLS_T20:
        return ThresholdResult(False, "CHASE_LOSS_MIN_SCORE", focus, opponent, competitor, "assume_chase_balls must be 1..120")

    for t in (focus, opponent, competitor):
        if t not in base_state:
            return ThresholdResult(False, "CHASE_LOSS_MIN_SCORE", focus, opponent, competitor, f"Unknown team: {t}")

    chase_overs = _balls_to_overs_str(assume_chase_balls)

    lo, hi = 0, target_score - 1
    best: Optional[int] = None

    def check(x: int) -> bool:
        st = _clone_state(base_state)
        table = simulate_match(
            state=st,
            team1=opponent,         # bats first
            team2=focus,            # chases
            team1_runs=target_score,
            team1_overs="20.0",
            team2_runs=x,
            team2_overs=chase_overs,
            team1_all_out=False,
            team2_all_out=False,
        )
        return _is_above(table, focus, competitor)

    # If even scoring (target-1) doesn't keep them above competitor -> impossible
    if not check(hi):
        return ThresholdResult(
            ok=False,
            mode="CHASE_LOSS_MIN_SCORE",
            focus=focus,
            opponent=opponent,
            competitor=competitor,
            reason="Even scoring (target_score - 1) still drops below competitor after loss.",
            value=None,
        )

    while lo <= hi:
        mid = (lo + hi) // 2
        if check(mid):
            best = mid
            hi = mid - 1
        else:
            lo = mid + 1

    return ThresholdResult(
        ok=True,
        mode="CHASE_LOSS_MIN_SCORE",
        focus=focus,
        opponent=opponent,
        competitor=competitor,
        reason="Minimum runs (even in loss) to stay above competitor.",
        value=best,
        details={"assume_chase_balls": assume_chase_balls, "assume_chase_overs": chase_overs},
    )


def defend_win_max_opp_score(
    *,
    base_state: Dict[str, TeamRow],
    defending_team: str,
    opponent_team: str,
    target_team: str,
    defending_score: int,
    assume_opp_balls: int = 120,
) -> ThresholdResult:
    """
    (2) Defending team wins after setting defending_score.
    Find maximum runs opponent can score (0..defending_score-1), assuming opponent uses assume_opp_balls,
    so defending team still stays above target_team.
    """
    focus = defending_team.strip().upper()
    opponent = opponent_team.strip().upper()
    competitor = target_team.strip().upper()

    if defending_score <= 0:
        return ThresholdResult(False, "DEFEND_WIN_MAX_OPP_SCORE", focus, opponent, competitor, "defending_score must be > 0")

    if assume_opp_balls < 1 or assume_opp_balls > MAX_BALLS_T20:
        return ThresholdResult(False, "DEFEND_WIN_MAX_OPP_SCORE", focus, opponent, competitor, "assume_opp_balls must be 1..120")

    for t in (focus, opponent, competitor):
        if t not in base_state:
            return ThresholdResult(False, "DEFEND_WIN_MAX_OPP_SCORE", focus, opponent, competitor, f"Unknown team: {t}")

    opp_overs = _balls_to_overs_str(assume_opp_balls)

    lo, hi = 0, defending_score - 1
    best: Optional[int] = None

    def check(x: int) -> bool:
        st = _clone_state(base_state)
        table = simulate_match(
            state=st,
            team1=focus,            # bats first
            team2=opponent,         # chases and loses
            team1_runs=defending_score,
            team1_overs="20.0",
            team2_runs=x,
            team2_overs=opp_overs,
            team1_all_out=False,
            team2_all_out=False,
        )
        return _is_above(table, focus, competitor)

    # If even restricting opponent to 0 doesn't keep above -> impossible
    if not check(0):
        return ThresholdResult(
            ok=False,
            mode="DEFEND_WIN_MAX_OPP_SCORE",
            focus=focus,
            opponent=opponent,
            competitor=competitor,
            reason="Even restricting opponent to 0 still drops below competitor after win.",
            value=None,
        )

    while lo <= hi:
        mid = (lo + hi) // 2
        if check(mid):
            best = mid
            lo = mid + 1
        else:
            hi = mid - 1

    return ThresholdResult(
        ok=True,
        mode="DEFEND_WIN_MAX_OPP_SCORE",
        focus=focus,
        opponent=opponent,
        competitor=competitor,
        reason="Maximum opponent score allowed (while defending) to stay above competitor.",
        value=best,
        details={"assume_opp_balls": assume_opp_balls, "assume_opp_overs": opp_overs},
    )


def chase_win_max_balls(
    *,
    base_state: Dict[str, TeamRow],
    chasing_team: str,
    opponent_team: str,
    target_team: str,
    target_score: int,
) -> ThresholdResult:
    """
    (3) Chasing team wins chase of target_score.
    Opponent bats full 20 overs and makes target_score.
    Chasing team scores target_score+1 and wins.
    Find maximum balls chasing team can take and STILL stay above target_team.
    """
    focus = chasing_team.strip().upper()
    opponent = opponent_team.strip().upper()
    competitor = target_team.strip().upper()

    if target_score <= 0:
        return ThresholdResult(False, "CHASE_WIN_MAX_BALLS", focus, opponent, competitor, "target_score must be > 0")

    for t in (focus, opponent, competitor):
        if t not in base_state:
            return ThresholdResult(False, "CHASE_WIN_MAX_BALLS", focus, opponent, competitor, f"Unknown team: {t}")

    lo, hi = 1, MAX_BALLS_T20
    best: Optional[int] = None

    def check(balls: int) -> bool:
        st = _clone_state(base_state)
        focus_runs = target_score + 1  # must win
        table = simulate_match(
            state=st,
            team1=opponent,         # bats first
            team2=focus,            # chases and wins
            team1_runs=target_score,
            team1_overs="20.0",
            team2_runs=focus_runs,
            team2_overs=_balls_to_overs_str(balls),
            team1_all_out=False,
            team2_all_out=False,
        )
        return _is_above(table, focus, competitor)

    # If even winning in 1 ball doesn't get above -> impossible
    if not check(1):
        return ThresholdResult(
            ok=False,
            mode="CHASE_WIN_MAX_BALLS",
            focus=focus,
            opponent=opponent,
            competitor=competitor,
            reason="Even winning very fast does not keep focus above competitor.",
            value=None,
        )

    while lo <= hi:
        mid = (lo + hi) // 2
        if check(mid):
            best = mid
            lo = mid + 1
        else:
            hi = mid - 1

    return ThresholdResult(
        ok=True,
        mode="CHASE_WIN_MAX_BALLS",
        focus=focus,
        opponent=opponent,
        competitor=competitor,
        reason="Maximum balls allowed while chasing and winning to stay above competitor.",
        value=best,
        details={"overs_str": _balls_to_overs_str(best) if best else None},
    )
