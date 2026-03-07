# ipl_api/planner.py
from __future__ import annotations

import copy
import random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Literal

from ipl_api.points_table import TeamRow, compute_sorted_table
from ipl_api.simulator import simulate_match
from ipl_api.nrr_math import MAX_BALLS_T20, overs_to_balls

BattingFirstMode = Literal["team1", "team2", "toss"]
ResultType = Literal["WIN", "NR", "TIE"]


# -----------------------------
# Models
# -----------------------------
@dataclass(frozen=True)
class Fixture:
    """
    A remaining fixture between two teams.

    batting_first_mode:
      - "team1": team1 bats first always
      - "team2": team2 bats first always
      - "toss": random per simulation

    Outcome modeling:
      - nr_probability: chance match is NR (abandoned/no-result)
      - tie_probability: chance match is TIE (no winner)   [rare in IPL; kept for completeness]
      - remaining probability becomes WIN (normal match)

    Constraints:
      0 <= nr_probability <= 1
      0 <= tie_probability <= 1
      nr_probability + tie_probability <= 1
    """
    team1: str
    team2: str
    batting_first_mode: BattingFirstMode = "toss"

    nr_probability: float = 0.0
    tie_probability: float = 0.0


@dataclass
class MatchOutcomeMeta:
    """
    Stores per-team, per-fixture outcomes for requirement extraction.
    """
    fixture_index: int
    fixture: Fixture

    team: str
    played: bool

    # role for this simulation run
    batted_first: Optional[bool] = None

    # result type for this fixture
    result: Optional[ResultType] = None

    # win/loss signal only applicable when result == "WIN"
    won: Optional[bool] = None

    # win-side signals
    win_margin_runs: Optional[int] = None
    chase_balls_used: Optional[int] = None
    overshoot_runs: Optional[int] = None

    # loss-side “damage control” signals
    loss_margin_runs: Optional[int] = None
    loss_chase_runs_scored: Optional[int] = None
    loss_defend_opp_balls_used: Optional[int] = None


# -----------------------------
# Helpers
# -----------------------------
def _clamp(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, x))


def _percentile(values: List[int], p: float) -> Optional[int]:
    if not values:
        return None
    v = sorted(values)
    idx = int(round((len(v) - 1) * p))
    return v[_clamp(idx, 0, len(v) - 1)]


def _fixture_key(i: int, fx: Fixture) -> str:
    return f"{i+1}:{fx.team1} vs {fx.team2}"


def _resolve_batting_order(fx: Fixture, rng: random.Random) -> Tuple[str, str]:
    """
    Returns (bat_first, bat_second) for this simulation run.
    """
    if fx.batting_first_mode == "team1":
        return fx.team1, fx.team2
    if fx.batting_first_mode == "team2":
        return fx.team2, fx.team1
    return (fx.team1, fx.team2) if rng.random() < 0.5 else (fx.team2, fx.team1)


def _sample_result(fx: Fixture, rng: random.Random) -> ResultType:
    """
    Samples NR/TIE/WIN based on probabilities.
    """
    u = rng.random()
    nr_p = float(fx.nr_probability)
    tie_p = float(fx.tie_probability)

    if u < nr_p:
        return "NR"
    if u < nr_p + tie_p:
        return "TIE"
    return "WIN"


# -----------------------------
# Random cricket generators
# -----------------------------
def _rand_first_innings_runs(rng: random.Random) -> int:
    r = int(rng.gauss(170, 25))
    return _clamp(r, 120, 240)


def _rand_defend_margin(rng: random.Random) -> int:
    m = int(abs(rng.gauss(15, 18)))
    return _clamp(m, 1, 80)


def _rand_chase_balls_used(target_runs: int, rng: random.Random) -> int:
    base = int(90 + (target_runs - 150) * 0.8)
    noise = int(rng.gauss(0, 14))
    balls = base + noise
    return _clamp(balls, 1, MAX_BALLS_T20)


def _winning_ball_runs(rng: random.Random) -> int:
    return rng.choice([1, 1, 2, 2, 3, 4, 6])


def _build_innings_for_win(
    bat_first: str,
    bat_second: str,
    winner: str,
    rng: random.Random,
) -> Tuple[int, str, bool, int, str, bool, int]:
    """
    Returns:
      bf_runs, bf_overs_str, bf_all_out,
      bs_runs, bs_overs_str, bs_all_out,
      overshoot_runs (0 if not a chase win)
    """
    bf_runs = _rand_first_innings_runs(rng)
    bf_overs_str = "20.0"
    bf_all_out = False

    # occasional all-out before 20
    if rng.random() < 0.10:
        balls = _clamp(int(rng.gauss(112, 10)), 60, 119)
        bf_overs_str = f"{balls//6}.{balls%6}"
        bf_all_out = True

    target = bf_runs + 1

    if winner == bat_first:
        # defend
        margin = _rand_defend_margin(rng)
        bs_runs = _clamp(bf_runs - margin, 0, bf_runs)
        bs_all_out = (rng.random() < 0.20)

        if bs_all_out:
            balls2 = _clamp(int(rng.gauss(108, 16)), 30, MAX_BALLS_T20)
            bs_overs_str = f"{balls2//6}.{balls2%6}"
        else:
            bs_overs_str = "20.0"

        return bf_runs, bf_overs_str, bf_all_out, bs_runs, bs_overs_str, bs_all_out, 0

    # chase win by bat_second
    balls_used = _rand_chase_balls_used(target, rng)
    win_ball = _winning_ball_runs(rng)

    needed_last_ball = rng.choice([1, 2, 3, 4, 5, 6])
    if needed_last_ball > win_ball:
        needed_last_ball = win_ball

    runs_before = target - needed_last_ball
    bs_runs = runs_before + win_ball
    overshoot = max(0, bs_runs - target)

    bs_overs_str = f"{balls_used//6}.{balls_used%6}"
    bs_all_out = False

    return bf_runs, bf_overs_str, bf_all_out, bs_runs, bs_overs_str, bs_all_out, overshoot


def _build_innings_for_tie(rng: random.Random) -> Tuple[int, str, bool, int, str, bool]:
    """
    Creates a plausible tied match innings:
    - Both teams end on same runs.
    - Overs can be full 20.0 by default.
    - Optional all-out event is allowed but rare.
    """
    bf_runs = _rand_first_innings_runs(rng)
    bf_overs_str = "20.0"
    bf_all_out = False

    if rng.random() < 0.05:
        balls = _clamp(int(rng.gauss(110, 12)), 60, 119)
        bf_overs_str = f"{balls//6}.{balls%6}"
        bf_all_out = True

    # Second innings ties the score (not chase target+1)
    bs_runs = bf_runs
    bs_all_out = False

    # In a tie, second innings usually completes 20 overs or ends all-out exactly tied
    if rng.random() < 0.08:
        balls2 = _clamp(int(rng.gauss(115, 10)), 30, MAX_BALLS_T20)
        bs_overs_str = f"{balls2//6}.{balls2%6}"
        bs_all_out = True
    else:
        bs_overs_str = "20.0"

    return bf_runs, bf_overs_str, bf_all_out, bs_runs, bs_overs_str, bs_all_out


# -----------------------------
# Meta extraction
# -----------------------------
def _make_team_meta(
    fixture_index: int,
    fx: Fixture,
    team: str,
    bat_first: str,
    bat_second: str,
    result: ResultType,
    winner: Optional[str],
    bf_runs: Optional[int],
    bs_runs: Optional[int],
    bs_overs_str: Optional[str],
    overshoot_runs: Optional[int],
) -> MatchOutcomeMeta:
    played = team in (fx.team1, fx.team2)
    m = MatchOutcomeMeta(
        fixture_index=fixture_index,
        fixture=fx,
        team=team,
        played=played,
        result=result,
    )
    if not played:
        return m

    m.batted_first = (team == bat_first)

    # NR and TIE are not treated as win/loss for requirement extraction in this planner
    if result != "WIN":
        m.won = None
        return m

    if winner is None or bf_runs is None or bs_runs is None or bs_overs_str is None:
        m.won = None
        return m

    m.won = (winner == team)

    if m.won:
        if m.batted_first:
            m.win_margin_runs = bf_runs - bs_runs
        else:
            m.chase_balls_used = overs_to_balls(bs_overs_str)
            m.overshoot_runs = int(overshoot_runs or 0)
        return m

    # loss case
    if m.batted_first:
        m.loss_margin_runs = bs_runs - bf_runs
        m.loss_defend_opp_balls_used = overs_to_balls(bs_overs_str)
    else:
        m.loss_margin_runs = bf_runs - bs_runs
        m.loss_chase_runs_scored = bs_runs

    return m


# -----------------------------
# Requirement summarizers
# -----------------------------
def _summarize_overall(metas: List[MatchOutcomeMeta], confidence: float) -> Dict[str, Any]:
    win_defend_margins: List[int] = []
    win_chase_balls: List[int] = []
    win_chase_overshoot: List[int] = []

    loss_margins: List[int] = []
    lose_chase_runs: List[int] = []
    lose_defend_opp_balls: List[int] = []

    for m in metas:
        if not m.played:
            continue
        if m.result != "WIN":
            continue
        if m.won is None:
            continue

        if m.won is True:
            if m.win_margin_runs is not None:
                win_defend_margins.append(m.win_margin_runs)
            if m.chase_balls_used is not None:
                win_chase_balls.append(m.chase_balls_used)
            if m.overshoot_runs is not None:
                win_chase_overshoot.append(m.overshoot_runs)
        else:
            if m.loss_margin_runs is not None:
                loss_margins.append(m.loss_margin_runs)
            if m.loss_chase_runs_scored is not None:
                lose_chase_runs.append(m.loss_chase_runs_scored)
            if m.loss_defend_opp_balls_used is not None:
                lose_defend_opp_balls.append(m.loss_defend_opp_balls_used)

    min_win_margin = _percentile(win_defend_margins, 1.0 - confidence)
    max_chase_balls = _percentile(win_chase_balls, confidence)
    overshoot_p = _percentile(win_chase_overshoot, confidence)

    max_loss_margin = _percentile(loss_margins, confidence)
    min_runs_in_chase_loss = _percentile(lose_chase_runs, 1.0 - confidence)
    min_opp_balls_used = _percentile(lose_defend_opp_balls, 1.0 - confidence)

    return {
        "confidence": confidence,
        "if_win_defend": {"min_runs_margin": min_win_margin, "samples": len(win_defend_margins)},
        "if_win_chase": {
            "max_balls_used": max_chase_balls,
            "max_overs_used": (max_chase_balls / 6.0) if max_chase_balls is not None else None,
            "samples": len(win_chase_balls),
            "note": "Includes winning-ball overshoot (need 1, hit 6 => +6 counts).",
            "overshoot_runs_p70": overshoot_p,
        },
        "if_lose": {
            "max_loss_margin_runs": max_loss_margin,
            "samples": len(loss_margins),
            "note": "Derived from successful Top-3 simulations where team still qualified despite losing.",
        },
        "if_lose_chase": {
            "max_loss_margin_runs": max_loss_margin,
            "min_runs_scored": min_runs_in_chase_loss,
            "samples": {
                "loss_cases": len([x for x in metas if x.loss_chase_runs_scored is not None]),
                "runs_samples": len(lose_chase_runs),
            },
            "note": "If you lose while chasing, scoring more runs (even in defeat) can reduce NRR damage.",
        },
        "if_lose_defend": {
            "max_loss_margin_runs": max_loss_margin,
            "min_opponent_balls_used": min_opp_balls_used,
            "min_opponent_overs_used": (min_opp_balls_used / 6.0) if min_opp_balls_used is not None else None,
            "samples": {
                "loss_cases": len([x for x in metas if x.loss_defend_opp_balls_used is not None]),
                "balls_samples": len(lose_defend_opp_balls),
            },
            "note": "If you lose while defending, a slower chase reduces NRR damage.",
        },
    }


def _summarize_per_fixture(
    metas: List[MatchOutcomeMeta],
    fixtures: List[Fixture],
    confidence: float,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for i, fx in enumerate(fixtures):
        key = _fixture_key(i, fx)
        subset = [m for m in metas if m.fixture_index == i and m.played]
        if not subset:
            continue
        out[key] = _summarize_overall(subset, confidence)
    return out


# -----------------------------
# Simulation core
# -----------------------------
def _run_one(
    base_state: Dict[str, TeamRow],
    fixtures: List[Fixture],
    rng: random.Random,
    use_nrr: bool,
) -> Tuple[List[dict], List[MatchOutcomeMeta]]:
    state = copy.deepcopy(base_state)
    all_metas: List[MatchOutcomeMeta] = []
    teams = list(base_state.keys())

    for i, fx in enumerate(fixtures):
        if fx.team1 == fx.team2:
            raise ValueError(f"Invalid fixture (same team twice): {fx.team1}")
        if fx.team1 not in base_state or fx.team2 not in base_state:
            raise ValueError(f"Unknown team in fixture: {fx.team1} vs {fx.team2}")

        if fx.nr_probability < 0 or fx.tie_probability < 0:
            raise ValueError("nr_probability and tie_probability must be >= 0")
        if fx.nr_probability + fx.tie_probability > 1.0:
            raise ValueError("nr_probability + tie_probability must be <= 1.0")

        bat_first, bat_second = _resolve_batting_order(fx, rng)
        result = _sample_result(fx, rng)

        winner: Optional[str] = None
        bf_runs: Optional[int] = None
        bf_overs: Optional[str] = None
        bf_all_out: bool = False
        bs_runs: Optional[int] = None
        bs_overs: Optional[str] = None
        bs_all_out: bool = False
        overshoot: int = 0

        if result == "NR":
            # NR: no innings, no NRR change
            simulate_match(
                state=state,
                team1=bat_first,
                team2=bat_second,
                result="NR",
                winner=None,
            )
        elif result == "TIE":
            if use_nrr:
                bf_runs, bf_overs, bf_all_out, bs_runs, bs_overs, bs_all_out = _build_innings_for_tie(rng)
            else:
                bf_runs, bf_overs, bf_all_out = 160, "20.0", False
                bs_runs, bs_overs, bs_all_out = 160, "20.0", False

            simulate_match(
                state=state,
                team1=bat_first,
                team2=bat_second,
                team1_runs=bf_runs,
                team1_overs=bf_overs,
                team2_runs=bs_runs,
                team2_overs=bs_overs,
                team1_all_out=bf_all_out,
                team2_all_out=bs_all_out,
                result="TIE",
                winner=None,
            )
        else:
            # WIN
            winner = rng.choice([bat_first, bat_second])

            if use_nrr:
                (bf_runs, bf_overs, bf_all_out,
                 bs_runs, bs_overs, bs_all_out,
                 overshoot) = _build_innings_for_win(bat_first, bat_second, winner, rng)
            else:
                bf_runs, bf_overs, bf_all_out = 160, "20.0", False
                bs_runs, bs_overs, bs_all_out = (150 if winner == bat_first else 161), "20.0", False
                overshoot = 0

            simulate_match(
                state=state,
                team1=bat_first,
                team2=bat_second,
                team1_runs=bf_runs,
                team1_overs=bf_overs,
                team2_runs=bs_runs,
                team2_overs=bs_overs,
                team1_all_out=bf_all_out,
                team2_all_out=bs_all_out,
                result="WIN",
                winner=winner,
            )

        for t in teams:
            all_metas.append(
                _make_team_meta(
                    fixture_index=i,
                    fx=fx,
                    team=t,
                    bat_first=bat_first,
                    bat_second=bat_second,
                    result=result,
                    winner=winner,
                    bf_runs=bf_runs,
                    bs_runs=bs_runs,
                    bs_overs_str=bs_overs,
                    overshoot_runs=overshoot,
                )
            )

    final_table = compute_sorted_table(list(state.values()))
    return final_table, all_metas


# -----------------------------
# Public API
# -----------------------------
def monte_carlo_planner(
    base_state: Dict[str, TeamRow],
    fixtures: List[Fixture],
    focus_team: str,
    iterations: int = 5000,
    seed: Optional[int] = None,
    use_nrr: bool = True,
    confidence: float = 0.70,
) -> Dict[str, Any]:
    if focus_team not in base_state:
        raise ValueError(f"Unknown focus_team: {focus_team}")
    if not fixtures:
        raise ValueError(
            "fixtures list is empty. Monte Carlo requires remaining fixtures to simulate future outcomes."
        )
    if iterations <= 0:
        raise ValueError("iterations must be > 0")
    if not (0.5 <= confidence <= 0.95):
        raise ValueError("confidence should be between 0.50 and 0.95")

    # upfront fixture validation (single pass)
    for fx in fixtures:
        if fx.team1 == fx.team2:
            raise ValueError(f"Invalid fixture (same team twice): {fx.team1}")
        if fx.team1 not in base_state or fx.team2 not in base_state:
            raise ValueError(f"Unknown team in fixture: {fx.team1} vs {fx.team2}")
        if fx.nr_probability < 0 or fx.tie_probability < 0:
            raise ValueError("nr_probability and tie_probability must be >= 0")
        if fx.nr_probability + fx.tie_probability > 1.0:
            raise ValueError("nr_probability + tie_probability must be <= 1.0")

    rng = random.Random(seed)
    teams = list(base_state.keys())

    top3_count: Dict[str, int] = {t: 0 for t in teams}
    top2_count: Dict[str, int] = {t: 0 for t in teams}

    metas_when_team_top3: Dict[str, List[MatchOutcomeMeta]] = {t: [] for t in teams}

    for _ in range(iterations):
        final_table, all_metas = _run_one(base_state, fixtures, rng, use_nrr)

        order = [row["team"] for row in final_table]
        top3 = set(order[:3])
        top2 = set(order[:2])

        for t in top3:
            top3_count[t] += 1
        for t in top2:
            top2_count[t] += 1

        for t in top3:
            metas_when_team_top3[t].extend([m for m in all_metas if m.team == t])

    top3_prob = {t: top3_count[t] / iterations for t in teams}
    top2_prob = {t: top2_count[t] / iterations for t in teams}

    per_team_requirements: Dict[str, Any] = {}
    for t in teams:
        metas = metas_when_team_top3[t]
        per_team_requirements[t] = {
            "overall": _summarize_overall(metas, confidence),
            "per_fixture": _summarize_per_fixture(metas, fixtures, confidence),
            "qualified_samples": top3_count[t],
        }

    focus_team_requirements = per_team_requirements.get(
        focus_team,
        {"overall": {}, "per_fixture": {}, "qualified_samples": 0},
    )

    return {
        "iterations": iterations,
        "seed": seed,
        "use_nrr": use_nrr,
        "top3_probability": top3_prob,
        "top2_probability": top2_prob,
        "focus_team": focus_team,
        "focus_team_success_rate_top3": top3_prob.get(focus_team, 0.0),
        "requirements": per_team_requirements,
        "focus_team_requirements": focus_team_requirements,
        "notes": [
            "Monte Carlo simulates remaining fixtures and updates NRR using balls-based aggregates.",
            "NR outcomes (no result) split points (1 each) and do not change NRR aggregates.",
            "TIE outcomes split points (1 each); if innings are simulated, aggregates update accordingly.",
            "Batting order per fixture uses batting_first_mode: team1/team2/toss.",
            "Chase overshoot is modeled on the winning ball (need 1, hit 6 => +6 counts).",
        ],
    }
