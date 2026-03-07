# main.py (WPL-only)
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from ipl_api.cache import get as cache_get, set as cache_set
from ipl_api.cricketdata_client import get_json, CricketDataError

from ipl_api.config import (
    validate_config,
    STANDINGS_CACHE_TTL_SECONDS,
    FIXTURES_CACHE_TTL_SECONDS,
    CRICKETDATA_ENABLED,
)

from ipl_api.simulator import (
    create_mock_ipl_table,   # legacy name
    create_mock_ipl_state,   # legacy name
    simulate_match,
)

from ipl_api.qualification import Fixture as QualFixture, evaluate_qualification_bounds
from ipl_api.nrr_math import MAX_BALLS_T20, normalize_innings_balls, overs_to_balls

from ipl_api.planner import Fixture as PlanFixture, monte_carlo_planner

from ipl_api.espn_standings import StandingsScrapeError, fetch_espn_points_table
from ipl_api.state_from_standings import build_state_from_standings
from ipl_api.espn_fixtures import FixturesScrapeError, fetch_espn_fixtures

# Threshold features (your 3 requirements)
from ipl_api.thresholds import (
    chase_loss_min_score,
    defend_win_max_opp_score,
    chase_win_max_balls,
)

from fastapi.middleware.cors import CORSMiddleware

DEFAULT_SEASON = 2026

# -----------------------
# App (WPL-only)
# -----------------------
app = FastAPI(
    title="WPL NRR Scenario Simulator API",
    version="0.1.0",
    description="Backend service for WPL NRR simulation, qualification bounds, Monte Carlo planning, and NRR thresholds",
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


@app.get("/health")
def health_check():
    return {"status": "ok", "time": datetime.utcnow().isoformat() + "Z"}


# -----------------------
# Helpers
# -----------------------
def _ensure_standings_non_empty(standings: Dict[str, Any], season: int) -> None:
    if not standings.get("teams"):
        raise HTTPException(
            status_code=502,
            detail=(
                f"Standings scrape returned empty teams for season={season}. "
                f"Note={standings.get('note')}"
            ),
        )


def _ensure_season_started(standings: Dict[str, Any], season: int) -> None:
    teams = standings.get("teams") or []
    if not teams:
        return

    all_zero_matches = True
    for t in teams:
        try:
            if int(t.get("matches", 0)) > 0:
                all_zero_matches = False
                break
        except Exception:
            all_zero_matches = False
            break

    if all_zero_matches:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Season {season} has not started yet (all teams have matches=0). "
                f"Standings are placeholder/pre-season. Note={standings.get('note')}"
            ),
        )


def _get_live_standings_cached(season: int) -> Dict[str, Any]:
    """
    Cache-first standings fetch with fresh/stale fallback.
    Returns the standings dict (the same structure your ESPN scraper returns).
    """
    cache_key_fresh = f"wpl-standings:{season}:fresh"
    cache_key_stale = f"wpl-standings:{season}:stale"

    cached_fresh = cache_get(cache_key_fresh)
    if cached_fresh is not None:
        _ensure_standings_non_empty(cached_fresh, season)
        _ensure_season_started(cached_fresh, season)
        return cached_fresh

    try:
        data = fetch_espn_points_table(season)
        _ensure_standings_non_empty(data, season)
        _ensure_season_started(data, season)

        cache_set(cache_key_fresh, data, ttl_seconds=STANDINGS_CACHE_TTL_SECONDS)
        cache_set(cache_key_stale, data, ttl_seconds=24 * 3600)
        return data

    except StandingsScrapeError as e:
        cached_stale = cache_get(cache_key_stale)
        if cached_stale is not None:
            # Stale is acceptable for internal use (planner/thresholds) as fallback.
            _ensure_standings_non_empty(cached_stale, season)
            # season-start check still applies
            _ensure_season_started(cached_stale, season)
            return cached_stale

        raise HTTPException(status_code=502, detail=f"Unable to fetch WPL standings: {str(e)}")


def _load_live_state(season: int):
    standings = _get_live_standings_cached(season)
    try:
        return build_state_from_standings(standings)
    except ValueError as e:
        raise HTTPException(status_code=502, detail=str(e))



# -----------------------
# Live standings endpoint (ESPN scrape + cache)
# -----------------------
@app.get("/api/standings")
def get_live_standings(season: int = DEFAULT_SEASON):
    cache_key_fresh = f"wpl-standings:{season}:fresh"
    cache_key_stale = f"wpl-standings:{season}:stale"

    cached_fresh = cache_get(cache_key_fresh)
    if cached_fresh is not None:
        _ensure_standings_non_empty(cached_fresh, season)
        _ensure_season_started(cached_fresh, season)
        return {
            "source": cached_fresh.get("source", "espn"),
            "season": season,
            "stale": False,
            "data": cached_fresh,
        }

    try:
        data = fetch_espn_points_table(season)
        _ensure_standings_non_empty(data, season)
        _ensure_season_started(data, season)

        cache_set(cache_key_fresh, data, ttl_seconds=STANDINGS_CACHE_TTL_SECONDS)
        cache_set(cache_key_stale, data, ttl_seconds=24 * 3600)

        return {"source": "espn", "season": season, "stale": False, "data": data}

    except StandingsScrapeError as e:
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
        raise HTTPException(status_code=502, detail=f"Unable to fetch WPL standings: {str(e)}")


# -----------------------
# Live fixtures endpoint (ESPN scrape + cache)
# -----------------------
@app.get("/api/fixtures")
def get_live_fixtures(season: int = DEFAULT_SEASON):
    cache_key = f"wpl-fixtures:{season}:fresh"

    cached = cache_get(cache_key)
    if cached is not None:
        return {"source": cached.get("source", "espn"), "season": season, "stale": False, "data": cached}

    try:
        data = fetch_espn_fixtures(season)
        cache_set(cache_key, data, ttl_seconds=FIXTURES_CACHE_TTL_SECONDS)
        return {"source": "espn", "season": season, "stale": False, "data": data}
    except FixturesScrapeError as e:
        raise HTTPException(status_code=502, detail=f"Unable to fetch fixtures: {str(e)}")


# -----------------------
# Optional: CricketData API ping (guarded)
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
    team1: str = Field(..., description="Team batting first (e.g., DC-W)")
    team2: str = Field(..., description="Team batting second (e.g., GG)")
    team1_runs: int = Field(..., ge=0)
    team1_overs: str = Field(..., description="e.g. 20.0 or 19.4")
    team2_runs: int = Field(..., ge=0)
    team2_overs: str = Field(..., description="e.g. 20.0 or 18.2")
    team1_all_out: bool = Field(False, description="True if team1 got all-out")
    team2_all_out: bool = Field(False, description="True if team2 got all-out")


@app.post("/api/simulate")
def simulate(req: SimulateRequest, source: Literal["mock", "live"] = "mock", season: int = DEFAULT_SEASON):
    if source == "mock":
        state = create_mock_ipl_state()
        table_source = "mock_state"
    else:
        state = _load_live_state(season)
        table_source = "live_standings"

    t1 = req.team1.strip().upper()
    t2 = req.team2.strip().upper()

    if t1 not in state:
        raise HTTPException(status_code=400, detail=f"Unknown team1: {t1}")
    if t2 not in state:
        raise HTTPException(status_code=400, detail=f"Unknown team2: {t2}")
    if t1 == t2:
        raise HTTPException(status_code=400, detail="team1 and team2 must be different")

    try:
        b1 = overs_to_balls(req.team1_overs)
        b2 = overs_to_balls(req.team2_overs)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if b1 <= 0 or b1 > MAX_BALLS_T20:
        raise HTTPException(status_code=400, detail="team1_overs must be between 0.1 and 20.0")
    if b2 <= 0 or b2 > MAX_BALLS_T20:
        raise HTTPException(status_code=400, detail="team2_overs must be between 0.1 and 20.0")

    _ = normalize_innings_balls(b1, req.team1_all_out)
    _ = normalize_innings_balls(b2, req.team2_all_out)

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
        )
        return {"table_source": table_source, "season": season, "input": req.model_dump(), "updated_table": updated}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# -----------------------
# Qualification Bounds Endpoint (mock-table based)
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
    batting_first_mode: BattingFirstMode = Field("toss", description="team1/team2/toss")


class MonteCarloPlanRequest(BaseModel):
    focus_team: str = Field(..., description="Team to evaluate, e.g., DC-W")
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
    source: Literal["mock", "live"] = "mock",
    season: int = DEFAULT_SEASON,
):
    if source == "mock":
        base_state = create_mock_ipl_state()
        table_source = "mock_state"
    else:
        base_state = _load_live_state(season)
        table_source = "live_standings"

    focus = req.focus_team.strip().upper()
    if focus not in base_state:
        raise HTTPException(status_code=400, detail=f"Unknown focus_team: {req.focus_team}")

    fixtures_in = list(req.fixtures)
    auto_fixtures_meta = None

    if not fixtures_in and source == "live":
        fx_cache_key = f"wpl-fixtures:{season}:fresh"
        cached_fx = cache_get(fx_cache_key)

        if cached_fx is None:
            try:
                cached_fx = fetch_espn_fixtures(season)
                cache_set(fx_cache_key, cached_fx, ttl_seconds=FIXTURES_CACHE_TTL_SECONDS)
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
            detail=(
                "No fixtures provided, and live fixture scrape returned 0. "
                "Provide fixtures in request body OR wait until ESPN publishes the schedule for this season."
            ),
        )

    planner_fixtures: list[PlanFixture] = []
    for f in fixtures_in:
        t1 = f.team1.strip().upper()
        t2 = f.team2.strip().upper()

        if t1 not in base_state:
            raise HTTPException(status_code=400, detail=f"Unknown team in fixtures: {t1}")
        if t2 not in base_state:
            raise HTTPException(status_code=400, detail=f"Unknown team in fixtures: {t2}")
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
                "per_fixture_key": f"{i+1}:{fx.team1} vs {fx.team2}",
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
# (these call thresholds.py which expects base_state=... now)
# -----------------------
class ThresholdChaseLossRequest(BaseModel):
    season: int = Field(DEFAULT_SEASON, description="WPL season year")
    source: Literal["live"] = Field("live", description="Currently only live supported")

    chasing_team: str
    opponent_team: str
    target_team: str

    target_score: int = Field(..., ge=0, description="Opponent's score to chase (e.g., 150)")
    assume_chase_balls: int = Field(120, ge=1, le=120, description="Assume chasing side uses these balls (120 = full 20 ov)")


@app.post("/api/thresholds/chase-loss/min-score")
def api_chase_loss_min_score(req: ThresholdChaseLossRequest):
    season = req.season
    state = _load_live_state(season)

    chasing = req.chasing_team.strip().upper()
    opp = req.opponent_team.strip().upper()
    target = req.target_team.strip().upper()

    for x in (chasing, opp, target):
        if x not in state:
            raise HTTPException(status_code=400, detail=f"Unknown team: {x}")

    out = chase_loss_min_score(
        base_state=state,
        chasing_team=chasing,
        opponent_team=opp,
        target_team=target,
        target_score=req.target_score,
        assume_chase_balls=req.assume_chase_balls,
    )

    return {"season": season, "input": req.model_dump(), "result": out}


class ThresholdDefendWinRequest(BaseModel):
    season: int = Field(DEFAULT_SEASON, description="WPL season year")
    source: Literal["live"] = Field("live", description="Currently only live supported")

    defending_team: str
    opponent_team: str
    target_team: str

    defending_score: int = Field(..., ge=0, description="Score set by defending team")
    assume_opp_balls: int = Field(120, ge=1, le=120, description="Assume opponent uses these balls (120 = full 20 ov)")


@app.post("/api/thresholds/defend/max-opp-score")
def api_defend_win_max_opp_score(req: ThresholdDefendWinRequest):
    season = req.season
    state = _load_live_state(season)

    defending = req.defending_team.strip().upper()
    opp = req.opponent_team.strip().upper()
    target = req.target_team.strip().upper()

    for x in (defending, opp, target):
        if x not in state:
            raise HTTPException(status_code=400, detail=f"Unknown team: {x}")

    out = defend_win_max_opp_score(
        base_state=state,
        defending_team=defending,
        opponent_team=opp,
        target_team=target,
        defending_score=req.defending_score,
        assume_opp_balls=req.assume_opp_balls,
    )

    return {"season": season, "input": req.model_dump(), "result": out}


class ThresholdChaseWinBallsRequest(BaseModel):
    season: int = Field(DEFAULT_SEASON, description="WPL season year")
    source: Literal["live"] = Field("live", description="Currently only live supported")

    chasing_team: str
    opponent_team: str
    target_team: str

    target_score: int = Field(..., ge=0, description="Target to chase (e.g., 150)")


@app.post("/api/thresholds/chase-win/max-balls")
def api_chase_win_max_balls(req: ThresholdChaseWinBallsRequest):
    season = req.season
    state = _load_live_state(season)

    chasing = req.chasing_team.strip().upper()
    opp = req.opponent_team.strip().upper()
    target = req.target_team.strip().upper()

    for x in (chasing, opp, target):
        if x not in state:
            raise HTTPException(status_code=400, detail=f"Unknown team: {x}")

    out = chase_win_max_balls(
        base_state=state,
        chasing_team=chasing,
        opponent_team=opp,
        target_team=target,
        target_score=req.target_score,
    )

    return {"season": season, "input": req.model_dump(), "result": out}
