from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Literal


# -----------------------------
# Match result semantics
# -----------------------------
MatchResultType = Literal["WIN", "NR", "TIE"]


# -----------------------------
# Canonical Fixture
# -----------------------------
@dataclass(frozen=True)
class Fixture:
    match_id: str
    team1: str
    team2: str

    # Explicit result semantics
    result_type: Optional[MatchResultType] = None
    winner: Optional[str] = None

    # Optional completed-innings data
    team1_runs: Optional[int] = None
    team1_overs: Optional[str] = None
    team2_runs: Optional[int] = None
    team2_overs: Optional[str] = None


# -----------------------------
# Canonical TeamRow
# -----------------------------
@dataclass
class TeamRow:
    team: str

    played: int
    won: int
    lost: int
    nr: int

    points: int
    nrr: float

    runs_for: int
    balls_faced: int
    runs_against: int
    balls_bowled: int
