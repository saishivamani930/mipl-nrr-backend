# main.py (IPL-only)
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Literal, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from ipl_api.cache import get as cache_get, set as cache_set
from ipl_api.cricketdata_client import get_json, CricketDataError

from ipl_api.config import (
    validate_config,
    STANDINGS_CACHE_TTL_SECONDS,
    FIXTURES_CACHE_TTL_SECONDS,
    CRICKETDATA_ENABLED,
    IPL_SERIES_ID,
)

from ipl_api.simulator import (
    create_mock_ipl_table,
    create_mock_ipl_state,
    simulate_match,
)

from ipl_api.qualification import Fixture as QualFixture, evaluate_qualification_bounds
from ipl_api.nrr_math import MAX_BALLS_T20, normalize_innings_balls, overs_to_balls

from ipl_api.planner import Fixture as PlanFixture, monte_carlo_planner

from ipl_api.espn_standings import StandingsScrapeError, fetch_espn_points_table
from ipl_api.state_from_standings import state_from_standings
from ipl_api.espn_fixtures import FixturesScrapeError, fetch_espn_fixtures


from ipl_api.thresholds import (
    chase_loss_min_score,
    defend_win_max_opp_score,
    chase_win_max_balls,
)

import sys
from fastapi.middleware.cors import CORSMiddleware


DEFAULT_SEASON = 2026

# Full name -> code mapping for all 10 IPL teams
IPL_NAME_TO_CODE: Dict[str, str] = {
    # Full names
    "chennai super kings": "CSK",
    "mumbai indians": "MI",
    "royal challengers bengaluru": "RCB",
    "royal challengers bangalore": "RCB",
    "kolkata knight riders": "KKR",
    "sunrisers hyderabad": "SRH",
    "rajasthan royals": "RR",
    "delhi capitals": "DC",
    "punjab kings": "PBKS",
    "lucknow super giants": "LSG",
    "gujarat titans": "GT",
    # ESPN nicknames
    "kings": "CSK",
    "indians": "MI",
    "mumbai indians": "MI",
    "riders": "KKR",
    "sunrisers": "SRH",
    "royals": "RR",
    "capitals": "DC",
    "delhi capitals": "DC",
    "giants": "LSG",
    "titans": "GT",
    "royal challengers": "RCB",
    # Full ESPN codes
    "sunrisers hyderabad": "SRH",
    "delhi capitals": "DC",
    "mumbai indians": "MI",
    "royal challengers bengaluru": "RCB",
}
def resolve_team_code(raw: str, state: Dict[str, Any]) -> str:
    """
    Accept either a code (MI, CSK) or full name (Mumbai Indians).
    Returns the code if found in state, else raises ValueError.
    """
    s = raw.strip()
    upper = s.upper()

    # Direct code match
    if upper in state:
        return upper

    # Full name match
    code = IPL_NAME_TO_CODE.get(s.lower())
    if code and code in state:
        return code

    # Partial match on state keys
    for key in state:
        if key in upper or upper in key:
            return key

    raise ValueError(f"Unknown team: {raw!r}. Known teams: {list(state.keys())}")

import logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


app = FastAPI(
    title="IPL NRR Scenario Simulator API",
    version="0.1.0",
    description="Backend service for IPL NRR simulation, qualification bounds, Monte Carlo planning, and NRR thresholds",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def on_startup():
    validate_config()
    try:
        _get_live_standings_cached(DEFAULT_SEASON)
    except Exception:
        pass


@app.get("/health")
@app.head("/health")
def health_check():
    return {"status": "ok", "time": datetime.utcnow().isoformat() + "Z"}


# -----------------------
# Helpers
# -----------------------
def _ensure_standings_non_empty(standings: Dict[str, Any], season: int) -> None:
    if not standings.get("teams"):
        standings["teams"] = [
            {"team": "Chennai Super Kings", "code": "CSK", "matches": 0, "won": 0, "lost": 0, "points": 0, "nrr": None},
            {"team": "Mumbai Indians", "code": "MI", "matches": 0, "won": 0, "lost": 0, "points": 0, "nrr": None},
            {"team": "Royal Challengers Bengaluru", "code": "RCB", "matches": 0, "won": 0, "lost": 0, "points": 0, "nrr": None},
            {"team": "Kolkata Knight Riders", "code": "KKR", "matches": 0, "won": 0, "lost": 0, "points": 0, "nrr": None},
            {"team": "Sunrisers Hyderabad", "code": "SRH", "matches": 0, "won": 0, "lost": 0, "points": 0, "nrr": None},
            {"team": "Rajasthan Royals", "code": "RR", "matches": 0, "won": 0, "lost": 0, "points": 0, "nrr": None},
            {"team": "Delhi Capitals", "code": "DC", "matches": 0, "won": 0, "lost": 0, "points": 0, "nrr": None},
            {"team": "Punjab Kings", "code": "PBKS", "matches": 0, "won": 0, "lost": 0, "points": 0, "nrr": None},
            {"team": "Lucknow Super Giants", "code": "LSG", "matches": 0, "won": 0, "lost": 0, "points": 0, "nrr": None},
            {"team": "Gujarat Titans", "code": "GT", "matches": 0, "won": 0, "lost": 0, "points": 0, "nrr": None},
        ]
        standings["note"] = "pre-season placeholder"


def _get_live_standings_cached(season: int) -> Dict[str, Any]:
    cache_key_fresh = f"ipl-standings:{season}:fresh"
    cache_key_stale = f"ipl-standings:{season}:stale"

    cached_fresh = cache_get(cache_key_fresh)
    if cached_fresh is not None:
        _ensure_standings_non_empty(cached_fresh, season)
        return cached_fresh

    try:
        data = fetch_espn_points_table(season)
        _ensure_standings_non_empty(data, season)

        cache_set(cache_key_fresh, data, ttl_seconds=STANDINGS_CACHE_TTL_SECONDS)
        cache_set(cache_key_stale, data, ttl_seconds=24 * 3600)
        return data

    except StandingsScrapeError as e:
        cached_stale = cache_get(cache_key_stale)
        if cached_stale is not None:
            _ensure_standings_non_empty(cached_stale, season)
            return cached_stale

        raise HTTPException(status_code=502, detail=f"Unable to fetch IPL standings: {str(e)}")


def _load_live_state_for_display(season: int):
    """
    Build internal state for display-only endpoints (standings, fixtures).
    Never raises on missing aggregates — teams will show NRR from ESPN,
    not from recalculated aggregates.
    """
    standings = _get_live_standings_cached(season)
    try:
        return state_from_standings(standings, require_aggregates=False)
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))


def _load_live_state(season: int):
    """
    Build internal state for simulation endpoints.
    Raises 502 if aggregates are missing (ESPN didn't return For/Against columns).
    """
    standings = _get_live_standings_cached(season)
    try:
        return state_from_standings(standings, require_aggregates=True)
    except ValueError as e:
        raise HTTPException(
            status_code=502,
            detail=(
                f"{str(e)} "
                "The points table is still displayed correctly — only NRR simulation "
                "requires the full For/Against aggregate data from ESPN."
            )
        )
    


# -----------------------
# Live standings endpoint
# -----------------------
@app.get("/api/standings")
def get_live_standings(season: int = DEFAULT_SEASON):
    cache_key_fresh = f"ipl-standings:{season}:fresh"
    cache_key_stale = f"ipl-standings:{season}:stale"

    cached_fresh = cache_get(cache_key_fresh)
    if cached_fresh is not None:
        _ensure_standings_non_empty(cached_fresh, season)
        cached_fresh["teams"] = sorted(
            cached_fresh.get("teams", []),
            key=lambda t: (-(t.get("points") or 0), -(t.get("nrr") or 0))
        )
        return {
            "source": cached_fresh.get("source", "espn"),
            "season": season,
            "stale": False,
            "data": cached_fresh,
        }

    try:
        data = fetch_espn_points_table(season)
        _ensure_standings_non_empty(data, season)

        cache_set(cache_key_fresh, data, ttl_seconds=STANDINGS_CACHE_TTL_SECONDS)
        cache_set(cache_key_stale, data, ttl_seconds=24 * 3600)

        data["teams"] = sorted(
            data.get("teams", []),
            key=lambda t: (-(t.get("points") or 0), -(t.get("nrr") or 0))
        )
        return {"source": "espn", "season": season, "stale": False, "data": data}

    except StandingsScrapeError as e:
        cached_stale = cache_get(cache_key_stale)
        if cached_stale is not None:
            cached_stale["teams"] = sorted(
                cached_stale.get("teams", []),
                key=lambda t: (-(t.get("points") or 0), -(t.get("nrr") or 0))
            )
            return {
                "source": "cache",
                "season": season,
                "stale": True,
                "warning": "Live scrape failed, serving cached data",
                "error": str(e),
                "data": cached_stale,
            }
        raise HTTPException(status_code=502, detail=f"Unable to fetch IPL standings: {str(e)}")


# -----------------------
# Live fixtures endpoint
# -----------------------
# Replace the entire /api/fixtures endpoint in main.py with this:

@app.get("/api/fixtures")
def get_live_fixtures(season: int = DEFAULT_SEASON):
    cache_key_fresh = f"ipl-fixtures:{season}:fresh"
    cache_key_stale = f"ipl-fixtures:{season}:stale"

    cached_fresh = cache_get(cache_key_fresh)
    if cached_fresh is not None:
        return {
            "source": cached_fresh.get("source", "espn"),
            "season": season,
            "stale": False,
            "data": cached_fresh,
        }

    try:
        data = fetch_espn_fixtures(season)
        # Cricbuzz enrichment is already handled inside fetch_espn_fixtures

        cache_set(cache_key_fresh, data, ttl_seconds=FIXTURES_CACHE_TTL_SECONDS)
        cache_set(cache_key_stale, data, ttl_seconds=24 * 3600)
        return {"source": "espn", "season": season, "stale": False, "data": data}

    except FixturesScrapeError as e:
        cached_stale = cache_get(cache_key_stale)
        if cached_stale is not None:
            return {
                "source": "cache",
                "season": season,
                "stale": True,
                "warning": "Live scrape failed, serving cached data",
                "error": str(e),
                "data": cached_stale,
            }
        raise HTTPException(status_code=502, detail=f"Unable to fetch fixtures: {str(e)}")


# -----------------------
# Optional: CricketData API ping
# -----------------------
@app.get("/api/ping-cricket")
def ping_cricket():
    if not CRICKETDATA_ENABLED:
        raise HTTPException(
            status_code=409,
            detail="CricketData API is disabled (CRICKETDATA_ENABLED=0).",
        )

    cache_key = "ping-cricket"
    cached = cache_get(cache_key)
    if cached is not None:
        return {"source": "cache", "data": cached}

    try:
        data = get_json("currentMatches")
        cache_set(cache_key, data, ttl_seconds=60)
        return {"source": "api", "data": data}
    except CricketDataError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")


# -----------------------
# Simulation Endpoint
# -----------------------
class SimulateRequest(BaseModel):
    team1: str = Field(..., description="Team batting first (e.g., MI or Mumbai Indians)")
    team2: str = Field(..., description="Team batting second (e.g., CSK or Chennai Super Kings)")
    team1_runs: int = Field(..., ge=0)
    team1_overs: str = Field(..., description="e.g. 20.0 or 19.4")
    team2_runs: int = Field(..., ge=0)
    team2_overs: str = Field(..., description="e.g. 20.0 or 18.2")
    team1_all_out: bool = Field(False)
    team2_all_out: bool = Field(False)
    result: Optional[str] = Field(None, description="WIN, TIE, or NR")
    winner: Optional[str] = Field(None, description="Winner team code when result=WIN")


@app.post("/api/simulate")
def simulate(req: SimulateRequest, source: Literal["mock", "live"] = "live", season: int = DEFAULT_SEASON):
    if source == "mock":
        state = create_mock_ipl_state()
        table_source = "mock_state"
    else:
        state = _load_live_state(season)
        table_source = "live_standings"

    try:
        t1 = resolve_team_code(req.team1, state)
        t2 = resolve_team_code(req.team2, state)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if t1 == t2:
        raise HTTPException(status_code=400, detail="team1 and team2 must be different")

    MAX_LEAGUE_MATCHES = 14

    def _get_played(row: Any) -> int:
        if hasattr(row, "played"):
            try:
                return int(row.played)
            except Exception:
                return 0
        if isinstance(row, dict):
            for k in ("played", "matches"):
                if k in row:
                    try:
                        return int(row[k])
                    except Exception:
                        return 0
        return 0

    t1_played = _get_played(state.get(t1))
    t2_played = _get_played(state.get(t2))

    if t1_played >= MAX_LEAGUE_MATCHES:
        raise HTTPException(status_code=400, detail=f"{t1} has already completed all {MAX_LEAGUE_MATCHES} league matches.")
    if t2_played >= MAX_LEAGUE_MATCHES:
        raise HTTPException(status_code=400, detail=f"{t2} has already completed all {MAX_LEAGUE_MATCHES} league matches.")

    try:
        b1 = overs_to_balls(req.team1_overs)
        b2 = overs_to_balls(req.team2_overs)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if b1 <= 0 or b1 > MAX_BALLS_T20:
        raise HTTPException(status_code=400, detail="team1_overs must be between 0.1 and 20.0")
    if b2 <= 0 or b2 > MAX_BALLS_T20:
        raise HTTPException(status_code=400, detail="team2_overs must be between 0.1 and 20.0")

    try:
        updated = simulate_match(
            state=state,
            team1=t1,
            team2=t2,
            team1_runs=req.team1_runs,
            team1_overs=req.team1_overs,
            team2_runs=req.team2_runs,
            team2_overs=req.team2_overs,
            team1_all_out=req.team1_all_out,
            team2_all_out=req.team2_all_out,
            result=req.result or "WIN",
            winner=resolve_team_code(req.winner, state) if req.winner else None,
        )
        return {"table_source": table_source, "season": season, "input": req.model_dump(), "updated_table": updated}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

# -----------------------
# Batch Simulation Endpoint
# -----------------------
class BatchSimulateRequest(BaseModel):
    matches: list[SimulateRequest] = Field(..., min_length=1)


@app.post("/api/simulate/batch")
def simulate_batch(req: BatchSimulateRequest, source: Literal["mock", "live"] = "live", season: int = DEFAULT_SEASON):
    if source == "mock":
        state = create_mock_ipl_state()
        table_source = "mock_state"
    else:
        state = _load_live_state(season)
        table_source = "live_standings"

    results = []
    current_state = state

    for i, match in enumerate(req.matches):
        try:
            t1 = resolve_team_code(match.team1, current_state)
            t2 = resolve_team_code(match.team2, current_state)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Match {i+1}: {str(e)}")

        if t1 == t2:
            raise HTTPException(status_code=400, detail=f"Match {i+1}: team1 and team2 must be different")

        try:
            b1 = overs_to_balls(match.team1_overs)
            b2 = overs_to_balls(match.team2_overs)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Match {i+1}: {str(e)}")

        if b1 <= 0 or b1 > MAX_BALLS_T20:
            raise HTTPException(status_code=400, detail=f"Match {i+1}: team1_overs must be between 0.1 and 20.0")
        if b2 <= 0 or b2 > MAX_BALLS_T20:
            raise HTTPException(status_code=400, detail=f"Match {i+1}: team2_overs must be between 0.1 and 20.0")

        try:
            updated = simulate_match(
                state=current_state,
                team1=t1,
                team2=t2,
                team1_runs=match.team1_runs,
                team1_overs=match.team1_overs,
                team2_runs=match.team2_runs,
                team2_overs=match.team2_overs,
                team1_all_out=match.team1_all_out,
                team2_all_out=match.team2_all_out,
                result=match.result or "WIN",
                winner=resolve_team_code(match.winner, current_state) if match.winner else None,
            )
        except ValueError as e:
            msg = str(e).lower()
            if "tied" in msg or "tie" in msg:
                raise HTTPException(status_code=400, detail=f"Match {i+1} is tied — please select a Super Over winner before simulating multiple matches together.")
            raise HTTPException(status_code=400, detail=f"Match {i+1}: {str(e)}")

        results.append({
            "match_index": i + 1,
            "team1": t1,
            "team2": t2,
            "updated_table": updated,
        })

        # Build next state from updated standings using simulate_match's own state builder
        try:
            new_standings = {"teams": []}
            for row in updated:
                new_standings["teams"].append({
                    "team": row.get("team", ""),
                    "code": row.get("code", row.get("team", "")),
                    "matches": row.get("matches", row.get("played", 0)),
                    "won": row.get("won", 0),
                    "lost": row.get("lost", 0),
                    "nr": row.get("nr", 0),
                    "tied": row.get("tied", 0),
                    "points": row.get("points", 0),
                    "runs_for": row.get("runs_for"),
                    "balls_for": row.get("balls_for"),
                    "runs_against": row.get("runs_against"),
                    "balls_against": row.get("balls_against"),
                })
            next_state = state_from_standings(new_standings)
            if next_state:
                current_state = next_state
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Batch state rebuild failed after match {i+1}: {str(e)}")
        except Exception as e:
            print("[BATCH ERROR] Failed to build next state:", str(e))

    return {
        "table_source": table_source,
        "season": season,
        "matches_simulated": len(results),
        "results": results,
    }
# -----------------------
# Qualification Bounds Endpoint
# -----------------------
class FixtureIn(BaseModel):
    team1: str
    team2: str


class QualificationRequest(BaseModel):
    fixtures: list[FixtureIn] = Field(default_factory=list)


@app.post("/api/qualification/bounds")
def qualification_bounds(req: QualificationRequest):
    table = create_mock_ipl_table()
    fixtures = [QualFixture(team1=f.team1.strip().upper(), team2=f.team2.strip().upper()) for f in req.fixtures]
    return {
        "table_source": "mock",
        "fixtures_count": len(fixtures),
        "result": evaluate_qualification_bounds(table, fixtures),
    }


# -----------------------
# Monte Carlo Planner Endpoint
# -----------------------
BattingFirstMode = Literal["team1", "team2", "toss"]


class PlanFixtureIn(BaseModel):
    team1: str
    team2: str
    batting_first_mode: BattingFirstMode = Field("toss")


class MonteCarloPlanRequest(BaseModel):
    focus_team: str = Field(..., description="Team to evaluate, e.g., MI or Mumbai Indians")
    fixtures: list[PlanFixtureIn] = Field(default_factory=list)
    iterations: int = Field(3000, ge=100, le=200000)
    seed: int | None = Field(None)
    use_nrr: bool = Field(True)
    confidence: float = Field(0.70, ge=0.50, le=0.95)


def _fixture_name(team1: str, team2: str) -> str:
    return f"{team1} vs {team2}"


def _fixture_label(team1: str, team2: str, mode: BattingFirstMode) -> str:
    if mode == "team1":
        return f"{team1} vs {team2} ( {team1} bats first )"
    if mode == "team2":
        return f"{team1} vs {team2} ( {team2} bats first )"
    return f"{team1} vs {team2} ( toss )"


@app.post("/api/plan/montecarlo")
def plan_montecarlo(
    req: MonteCarloPlanRequest,
    source: Literal["mock", "live"] = "live",
    season: int = DEFAULT_SEASON,
):
    if source == "mock":
        base_state = create_mock_ipl_state()
        table_source = "mock_state"
    else:
        base_state = _load_live_state(season)
        table_source = "live_standings"

    try:
        focus = resolve_team_code(req.focus_team, base_state)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    fixtures_in = list(req.fixtures)
    auto_fixtures_meta = None

    if not fixtures_in and source == "live":
        fx_cache_key = f"ipl-fixtures:{season}:fresh"
        cached_fx = cache_get(fx_cache_key)

        if cached_fx is None:
            try:
                cached_fx = fetch_espn_fixtures(season)
                cache_set(fx_cache_key, cached_fx, ttl_seconds=FIXTURES_CACHE_TTL_SECONDS)
                cache_set(f"ipl-fixtures:{season}:stale", cached_fx, ttl_seconds=24 * 3600)
            except FixturesScrapeError as e:
                raise HTTPException(status_code=502, detail=f"Unable to fetch fixtures: {str(e)}")

        auto_fixtures_meta = {
            "fixtures_count": cached_fx.get("fixtures_count", 0),
            "note": cached_fx.get("note"),
            "url_used": cached_fx.get("url_used"),
        }

        for f in cached_fx.get("fixtures", []):
            t1 = (f.get("team1") or "").strip().upper()
            t2 = (f.get("team2") or "").strip().upper()
            if t1 and t2:
                fixtures_in.append(PlanFixtureIn(team1=t1, team2=t2, batting_first_mode="toss"))

    if not fixtures_in:
        raise HTTPException(
            status_code=400,
            detail="No fixtures provided. Add fixtures or wait until ESPN publishes the schedule.",
        )

    planner_fixtures: list[PlanFixture] = []
    for f in fixtures_in:
        try:
            t1 = resolve_team_code(f.team1, base_state)
            t2 = resolve_team_code(f.team2, base_state)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        if t1 == t2:
            raise HTTPException(status_code=400, detail="Fixture team1 and team2 must be different")

        planner_fixtures.append(PlanFixture(team1=t1, team2=t2, batting_first_mode=f.batting_first_mode))

    try:
        result = monte_carlo_planner(
            base_state=base_state,
            fixtures=planner_fixtures,
            focus_team=focus,
            iterations=req.iterations,
            seed=req.seed,
            use_nrr=req.use_nrr,
            confidence=req.confidence,
        )

        fixtures_out = [
            {
                "fixture_index": i,
                "team1": fx.team1,
                "team2": fx.team2,
                "batting_first_mode": fx.batting_first_mode,
                "fixture_name": _fixture_name(fx.team1, fx.team2),
                "fixture_label": _fixture_label(fx.team1, fx.team2, fx.batting_first_mode),
            }
            for i, fx in enumerate(planner_fixtures)
        ]

        resp: Dict[str, Any] = {
            "table_source": table_source,
            "fixtures_count": len(planner_fixtures),
            "fixtures": fixtures_out,
            "result": result,
        }

        if auto_fixtures_meta is not None:
            resp["auto_fixtures_meta"] = auto_fixtures_meta

        return resp

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# -----------------------
# NRR Threshold Endpoints
# -----------------------
class ThresholdChaseLossRequest(BaseModel):
    season: int = Field(DEFAULT_SEASON)
    source: Literal["live"] = Field("live")
    chasing_team: str
    opponent_team: str
    target_team: str
    target_score: int = Field(..., ge=0)
    assume_chase_balls: int = Field(120, ge=1, le=120)


@app.post("/api/thresholds/chase-loss/min-score")
def api_chase_loss_min_score(req: ThresholdChaseLossRequest):
    state = _load_live_state(req.season)
    try:
        chasing = resolve_team_code(req.chasing_team, state)
        opp = resolve_team_code(req.opponent_team, state)
        target = resolve_team_code(req.target_team, state)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    out = chase_loss_min_score(
        base_state=state,
        chasing_team=chasing,
        opponent_team=opp,
        target_team=target,
        target_score=req.target_score,
        assume_chase_balls=req.assume_chase_balls,
    )
    return {"season": req.season, "input": req.model_dump(), "result": out}


class ThresholdDefendWinRequest(BaseModel):
    season: int = Field(DEFAULT_SEASON)
    source: Literal["live"] = Field("live")
    defending_team: str
    opponent_team: str
    target_team: str
    defending_score: int = Field(..., ge=0)
    opponent_balls: int = Field(120, ge=1, le=120)


@app.post("/api/thresholds/defend/max-opp-score")
def api_defend_win_max_opp_score(req: ThresholdDefendWinRequest):
    state = _load_live_state(req.season)
    try:
        defending = resolve_team_code(req.defending_team, state)
        opp = resolve_team_code(req.opponent_team, state)
        target = resolve_team_code(req.target_team, state)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    out = defend_win_max_opp_score(
        base_state=state,
        defending_team=defending,
        opponent_team=opp,
        target_team=target,
        defending_score=req.defending_score,
        assume_opp_balls=req.opponent_balls,
    )
    return {"season": req.season, "input": req.model_dump(), "result": out}


class ThresholdChaseWinBallsRequest(BaseModel):
    season: int = Field(DEFAULT_SEASON)
    source: Literal["live"] = Field("live")
    chasing_team: str
    opponent_team: str
    target_team: str
    target_score: int = Field(..., ge=0)


@app.post("/api/thresholds/chase-win/max-balls")
def api_chase_win_max_balls(req: ThresholdChaseWinBallsRequest):
    state = _load_live_state(req.season)
    try:
        chasing = resolve_team_code(req.chasing_team, state)
        opp = resolve_team_code(req.opponent_team, state)
        target = resolve_team_code(req.target_team, state)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    out = chase_win_max_balls(
        base_state=state,
        chasing_team=chasing,
        opponent_team=opp,
        target_team=target,
        target_score=req.target_score,
    )
    return {"season": req.season, "input": req.model_dump(), "result": out}

@app.post("/api/thresholds/defend-loss/max-balls")
async def threshold_defend_loss_max_balls(body: dict, source: str = "live", season: int = 2026):
    state = get_base_state(source, season)
    result = defend_loss_max_balls(
        base_state=state,
        defending_team=body["defending_team"],
        opponent_team=body["opponent_team"],
        target_team=body["target_team"],
        defending_score=int(body["defending_score"]),
    )
    return {"result": asdict(result)}

@app.get("/api/debug/cache")
def debug_cache():
    from ipl_api.cache import debug_snapshot
    return debug_snapshot()