# ipl_api/qualification.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

# WPL playoffs: Top-3 qualify
PLAYOFF_SPOTS = 3


@dataclass(frozen=True)
class Fixture:
    team1: str
    team2: str


def _index_by_team(table: List[dict]) -> Dict[str, dict]:
    return {row["team"]: row for row in table}


def _current_points(table: List[dict]) -> Dict[str, int]:
    return {row["team"]: int(row.get("points", 0)) for row in table}


def _current_nrr(table: List[dict]) -> Dict[str, float]:
    # NRR may be missing for some rows in edge cases; treat as 0.0
    out: Dict[str, float] = {}
    for row in table:
        team = row["team"]
        try:
            out[team] = float(row.get("nrr", 0.0))
        except Exception:
            out[team] = 0.0
    return out


def _max_points(points: Dict[str, int], remaining_fixtures: List[Fixture]) -> Dict[str, int]:
    # Max points = current points + 2 * matches_left_for_team
    matches_left = {t: 0 for t in points.keys()}
    for fx in remaining_fixtures:
        if fx.team1 in matches_left:
            matches_left[fx.team1] += 1
        if fx.team2 in matches_left:
            matches_left[fx.team2] += 1

    return {t: points[t] + 2 * matches_left.get(t, 0) for t in points.keys()}


def _min_points(points: Dict[str, int]) -> Dict[str, int]:
    # Min points = current points (lose all remaining)
    return dict(points)


def _rank_snapshot_points_nrr(table: List[dict]) -> List[dict]:
    """
    Current table ranking snapshot using official tie-break order:
    1) Points desc
    2) NRR desc
    (Further tie-breakers like head-to-head are not modeled here.)
    """
    def key_fn(row: dict) -> Tuple[int, float, str]:
        pts = int(row.get("points", 0))
        try:
            nrr = float(row.get("nrr", 0.0))
        except Exception:
            nrr = 0.0
        # deterministic tie-breaker for stable ordering
        return (pts, nrr, str(row.get("team", "")))

    ranked = sorted(table, key=key_fn, reverse=True)

    # ensure pos is correct in the snapshot
    out = []
    for i, r in enumerate(ranked, start=1):
        rr = dict(r)
        rr["pos"] = i
        out.append(rr)
    return out


def _top3_cutoff_points(points: Dict[str, int]) -> int:
    # PLAYOFF_SPOTS-th highest points (points-only snapshot). For WPL, this is 3rd highest.
    sorted_pts = sorted(points.values(), reverse=True)
    if not sorted_pts:
        return 0
    idx = min(PLAYOFF_SPOTS - 1, len(sorted_pts) - 1)
    return int(sorted_pts[idx])


def _top3_cutoff_points_nrr(table: List[dict]) -> Tuple[int, float, Optional[str]]:
    """
    PLAYOFF_SPOTS-th place snapshot considering Points+NRR.
    Returns (points, nrr, team_name_at_cutoff).
    """
    ranked = _rank_snapshot_points_nrr(table)
    if not ranked:
        return (0, 0.0, None)
    idx = min(PLAYOFF_SPOTS - 1, len(ranked) - 1)
    r = ranked[idx]
    pts = int(r.get("points", 0))
    try:
        nrr = float(r.get("nrr", 0.0))
    except Exception:
        nrr = 0.0
    return (pts, nrr, str(r.get("team", "")))


# Backward-compatible helpers (do not remove; other modules may call these)
def _top4_cutoff_points(points: Dict[str, int]) -> int:
    # Compatibility name: now returns Top-3 cutoff for WPL.
    return _top3_cutoff_points(points)


def _top4_cutoff_points_nrr(table: List[dict]) -> Tuple[int, float, Optional[str]]:
    # Compatibility name: now returns Top-3 cutoff for WPL.
    return _top3_cutoff_points_nrr(table)


def _is_guaranteed_qualified(team: str, max_pts: Dict[str, int]) -> bool:
    """
    Conservative guarantee check using ONLY points bounds (no NRR).
    Team is not guaranteed if PLAYOFF_SPOTS or more teams can finish strictly above its BEST case.

    This remains intentionally conservative. Exact guarantees require scenario enumeration.
    """
    my_max = max_pts[team]
    can_finish_above = sum(1 for t, p in max_pts.items() if t != team and p > my_max)
    return can_finish_above <= (PLAYOFF_SPOTS - 1)


def _is_guaranteed_eliminated(team: str, min_pts: Dict[str, int], max_pts: Dict[str, int]) -> bool:
    """
    Conservative elimination check using ONLY points bounds (no NRR).
    Eliminated if even with best-case for team, PLAYOFF_SPOTS teams are guaranteed to finish above it on points.
    """
    my_max = max_pts[team]
    strictly_above_me_for_sure = sum(1 for t, p in min_pts.items() if t != team and p > my_max)
    return strictly_above_me_for_sure >= PLAYOFF_SPOTS


def evaluate_qualification_bounds(
    table: List[dict],
    remaining_fixtures: List[Fixture],
) -> Dict[str, dict]:
    """
    Returns per-team:
      - min_points, max_points
      - status (QUALIFIED/ELIMINATED/IN_CONTENTION) using conservative points-bounds only

    Adds:
      - current_rank_snapshot_points_nrr (for accurate current ranking snapshot using NRR)
      - cutoff snapshots (Top-3 for WPL)
    """
    points = _current_points(table)
    max_pts = _max_points(points, remaining_fixtures)
    min_pts = _min_points(points)

    results: Dict[str, dict] = {}
    for team in points.keys():
        if _is_guaranteed_eliminated(team, min_pts=min_pts, max_pts=max_pts):
            status = "ELIMINATED"
        elif _is_guaranteed_qualified(team, max_pts=max_pts):
            status = "QUALIFIED"
        else:
            status = "IN_CONTENTION"

        results[team] = {
            "min_points": int(min_pts[team]),
            "max_points": int(max_pts[team]),
            "status": status,
        }

    cutoff_pts_only = _top3_cutoff_points(points)
    cutoff_pts_nrr, cutoff_nrr_nrr, cutoff_team = _top3_cutoff_points_nrr(table)

    results["_meta"] = {
        "note": (
            "This endpoint is bounds-only and conservative (guarantees are points-only). "
            "NRR is used only for the current ranking snapshot/tie-break visibility. "
            "For actual probability-based qualification with NRR, use the Monte Carlo planner."
        ),
        # New correct keys (Top-3)
        "current_top3_cutoff_points_snapshot": cutoff_pts_only,
        "current_top3_cutoff_points_nrr_snapshot": {
            "points": int(cutoff_pts_nrr),
            "nrr": float(cutoff_nrr_nrr),
            "team_at_3rd": cutoff_team,
        },
        # Backward-compatible aliases (Top-4 keys kept, but represent Top-3 in WPL)
        "current_top4_cutoff_points_snapshot": cutoff_pts_only,
        "current_top4_cutoff_points_nrr_snapshot": {
            "points": int(cutoff_pts_nrr),
            "nrr": float(cutoff_nrr_nrr),
            "team_at_4th": cutoff_team,
        },
        "current_rank_snapshot_points_nrr": _rank_snapshot_points_nrr(table),
        "playoff_spots": PLAYOFF_SPOTS,
    }
    return results
