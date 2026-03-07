# ipl_api/espn_fixtures.py
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

from ipl_api.cache import get as cache_get, set as cache_set, make_key as cache_key
from ipl_api.config import (
    WPL_SERIES_ID,
    ESPN_FIXTURES_URL_TEMPLATE,
    FIXTURES_CACHE_TTL_SECONDS,
)
from ipl_api.state_from_standings import normalize_team_code


class FixturesScrapeError(Exception):
    """Raised when ESPN fixtures scraping/parsing fails."""
    pass


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _extract_next_data_json(html: str) -> Dict[str, Any]:
    """
    ESPN pages are often Next.js. The full state is inside:
      <script id="__NEXT_DATA__" type="application/json"> ... </script>

    IMPORTANT: regex must be NON-GREEDY.
    """
    m = re.search(
        r"<script[^>]*\bid=['\"]__NEXT_DATA__['\"][^>]*>\s*(\{.*?\})\s*</script>",
        html,
        flags=re.DOTALL,
    )
    if not m:
        raise FixturesScrapeError("Could not find __NEXT_DATA__ in ESPN page HTML")

    raw = m.group(1).strip()
    try:
        return json.loads(raw)
    except Exception as e:
        raise FixturesScrapeError(f"Failed to parse __NEXT_DATA__ JSON: {e}") from e


def _walk(obj: Any):
    """Generic JSON walker that yields dict nodes."""
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v)


def _pick_competition_nodes(next_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    We look for dicts that resemble ESPN competition items:
      - has "competitors" list AND "status"
      - or wrappers that contain "competitions" list of such items
    """
    nodes: List[Dict[str, Any]] = []
    for d in _walk(next_data):
        if not isinstance(d, dict):
            continue

        if isinstance(d.get("competitors"), list) and "status" in d:
            nodes.append(d)
            continue

        comps = d.get("competitions")
        if isinstance(comps, list):
            for c in comps:
                if isinstance(c, dict) and isinstance(c.get("competitors"), list) and "status" in c:
                    nodes.append(c)

    return nodes


def _get_team_names_from_competitors(comp: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    comps = comp.get("competitors")
    if not isinstance(comps, list) or len(comps) < 2:
        return None

    def team_name(x: Dict[str, Any]) -> str:
        t = x.get("team") or {}
        return (t.get("displayName") or t.get("name") or "").strip()

    a = team_name(comps[0])
    b = team_name(comps[1])
    if not a or not b:
        return None
    return a, b


def _status_fields(comp: Dict[str, Any]) -> Tuple[str, str, str]:
    """
    Returns: (status_name, status_state, status_detail)
    Example:
      STATUS_SCHEDULED, pre, "Match starts at ..."
    """
    st = comp.get("status") or {}
    t = st.get("type") or {}
    name = str(t.get("name") or "").strip()
    state = str(t.get("state") or "").strip()
    detail = str(t.get("detail") or "").strip()
    return name, state, detail


def _is_scheduled_or_pre(comp: Dict[str, Any]) -> bool:
    """
    Keep only not-yet-started fixtures.
    ESPN status shape differs, so handle common patterns.
    """
    name, state, detail = _status_fields(comp)

    name_u = name.upper()
    state_l = state.lower()
    detail_l = detail.lower()

    if "SCHEDULED" in name_u:
        return True
    if state_l == "pre":
        return True
    if "starts" in detail_l or "upcoming" in detail_l:
        return True

    return False


def _parse_start_time_utc(comp: Dict[str, Any]) -> Optional[str]:
    dt = comp.get("date")
    if not dt or not isinstance(dt, str):
        return None
    return dt.strip() or None


def fetch_espn_fixtures(season: int, *, use_cache: bool = True) -> Dict[str, Any]:
    """
    Fetch remaining/scheduled fixtures from ESPN.

    Future-proof behaviour:
    - If ESPN has not published fixtures for the season yet, returns fixtures_count=0 with note.
    - Does NOT crash the backend for future seasons (2026+).

    Cache:
    - In-memory TTL cache, keyed by season.
    """
    if season <= 0:
        raise ValueError("season must be a positive integer")

    ckey = cache_key("fixtures", str(season))
    if use_cache:
        cached = cache_get(ckey)
        if cached is not None:
            return cached

    url = ESPN_FIXTURES_URL_TEMPLATE.format(series_id=WPL_SERIES_ID, season=season)

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; IPL-NRR-Sim/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IN,en;q=0.9",
        "Connection": "keep-alive",
    }

    html: Optional[str] = None
    url_used: str = url
    last_err: Optional[str] = None

    with requests.Session() as s:
        try:
            r = s.get(url, timeout=15, headers=headers, allow_redirects=True)
            r.raise_for_status()
            html = r.text
        except Exception as e:
            last_err = str(e)

    if html is None:
        # Do not hide real connectivity failures
        raise FixturesScrapeError(f"ESPN fixtures fetch failed: {last_err}")

    # Next.js parse
    try:
        next_data = _extract_next_data_json(html)
    except FixturesScrapeError:
        resp = {
            "season": season,
            "source": "espn",
            "url_used": url_used,
            "last_updated_utc": _utc_now_iso(),
            "fixtures": [],
            "fixtures_count": 0,
            "note": "ESPN schedule page not published yet or page missing __NEXT_DATA__.",
        }
        if use_cache:
            cache_set(ckey, resp, FIXTURES_CACHE_TTL_SECONDS)
        return resp

    comps = _pick_competition_nodes(next_data)

    fixtures: List[Dict[str, Any]] = []
    seen = set()

    for comp in comps:
        if not _is_scheduled_or_pre(comp):
            continue

        names = _get_team_names_from_competitors(comp)
        if not names:
            continue
        t1_name, t2_name = names

        t1 = normalize_team_code(t1_name)
        t2 = normalize_team_code(t2_name)
        if not t1 or not t2 or t1 == t2:
            continue

        match_id = comp.get("id")
        start_time_utc = _parse_start_time_utc(comp)
        status_name, status_state, status_detail = _status_fields(comp)

        key = (t1, t2, start_time_utc or "", str(match_id or ""))
        if key in seen:
            continue
        seen.add(key)

        # Canonical fixture schema (extendable to results later)
        fixtures.append(
            {
                "team1": t1,
                "team2": t2,
                "start_time_utc": start_time_utc,
                "match_id": str(match_id) if match_id is not None else None,
                "status": {
                    "name": status_name,
                    "state": status_state,
                    "detail": status_detail,
                },
                # Placeholder fields for completed-result ingestion later:
                # "result": None, "winner": None
            }
        )

    # Deterministic ordering: by time then team codes (stable Monte Carlo inputs)
    def _sort_key(x: Dict[str, Any]) -> Tuple[str, str, str]:
        return (x.get("start_time_utc") or "", x["team1"], x["team2"])

    fixtures.sort(key=_sort_key)

    resp = {
        "season": season,
        "source": "espn",
        "url_used": url_used,
        "last_updated_utc": _utc_now_iso(),
        "fixtures": fixtures,
        "fixtures_count": len(fixtures),
    }

    if use_cache:
        cache_set(ckey, resp, FIXTURES_CACHE_TTL_SECONDS)

    return resp
