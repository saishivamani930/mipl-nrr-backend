from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

from ipl_api.cache import get as cache_get, set as cache_set, make_key as cache_key
from ipl_api.config import (
    IPL_SERIES_ID,
    ESPN_FIXTURES_URL_TEMPLATE,
    ESPN_FIXTURES_SCHEDULE_URL_TEMPLATE,
    FIXTURES_CACHE_TTL_SECONDS,
)
from ipl_api.state_from_standings import normalize_team_code


class FixturesScrapeError(Exception):
    """Raised when ESPN fixtures scraping/parsing fails."""
    pass


def _utc_now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _extract_next_data_json(html: str) -> Dict[str, Any]:
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
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _walk(v)


def _pick_competition_nodes(next_data: Dict[str, Any]) -> List[Dict[str, Any]]:
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
    st = comp.get("status") or {}
    t = st.get("type") or {}
    name = str(t.get("name") or "").strip()
    state = str(t.get("state") or "").strip()
    detail = str(t.get("detail") or "").strip()
    return name, state, detail


def _is_scheduled_or_pre(comp: Dict[str, Any]) -> bool:
    name, state, detail = _status_fields(comp)

    name_u = name.upper()
    state_l = state.lower()
    detail_l = detail.lower()

    if "SCHEDULED" in name_u:
        return True

    if state_l in ["pre", "preview"]:
        return True

    if any(x in detail_l for x in ["starts", "upcoming", "yet to begin"]):
        return True

    if state_l == "post":
        return False

    if comp.get("date"):
        return True

    return False


def _parse_start_time_utc(comp: Dict[str, Any]) -> Optional[str]:
    dt = comp.get("date")
    if not dt or not isinstance(dt, str):
        return None
    return dt.strip() or None


def _team_name_to_code_and_name(name: str) -> Tuple[str, str]:
    cleaned = " ".join(name.split()).strip()
    code = normalize_team_code(cleaned)
    return cleaned, code


def _to_fixture_dict(
    team1_name: str,
    team2_name: str,
    date_iso: Optional[str],
    venue: Optional[str],
    match_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    team1_name, team1_code = _team_name_to_code_and_name(team1_name)
    team2_name, team2_code = _team_name_to_code_and_name(team2_name)

    if not team1_name or not team2_name:
        return None
    if not team1_code or not team2_code:
        return None
    if team1_code == team2_code:
        return None

    if not match_id:
        match_id = f"{team1_code}-{team2_code}-{date_iso or 'unknown'}"

    return {
        "match_id": str(match_id),
        "date": date_iso or "",
        "team1": team1_name,
        "team2": team2_name,
        "team1_code": team1_code,
        "team2_code": team2_code,
        "status": "upcoming",
        "venue": venue,
    }


def _extract_from_next_data(next_data: Dict[str, Any]) -> List[Dict[str, Any]]:
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
        start_time_utc = _parse_start_time_utc(comp)

        venue = None
        venue_obj = comp.get("venue")
        if isinstance(venue_obj, dict):
            venue = venue_obj.get("fullName") or venue_obj.get("name")

        match_id = comp.get("id")
        item = _to_fixture_dict(
            team1_name=t1_name,
            team2_name=t2_name,
            date_iso=start_time_utc,
            venue=venue,
            match_id=str(match_id) if match_id is not None else None,
        )
        if not item:
            continue

        key = item["match_id"]
        if key in seen:
            continue
        seen.add(key)

        fixtures.append(item)

    fixtures.sort(key=lambda x: (x.get("date") or "", x["team1_code"], x["team2_code"]))
    return fixtures


def _parse_header_datetime_to_iso(header_line: str, detail_line: str) -> Optional[str]:
    try:
        m_time = re.search(r"(\d{1,2}:\d{2}\s*[AP]M)\s*IST", header_line, flags=re.I)
        m_date = re.search(r"([A-Z][a-z]{2})\s+(\d{1,2})\s+(\d{4})", detail_line)
        if not m_time or not m_date:
            return None

        time_part = m_time.group(1).upper().replace("  ", " ").strip()
        month_str = m_date.group(1)
        day = int(m_date.group(2))
        year = int(m_date.group(3))

        dt = datetime.strptime(f"{month_str} {day} {year} {time_part}", "%b %d %Y %I:%M %p")
        return dt.strftime("%Y-%m-%dT%H:%M:%S+05:30")
    except Exception:
        return None


def _extract_venue(detail_line: str) -> Optional[str]:
    m = re.search(r"\bat\s+([^,]+)", detail_line, flags=re.I)
    if not m:
        return None
    return m.group(1).strip()


def _looks_like_team_name(line: str) -> bool:
    if not line:
        return False

    bad = {
        "summary",
        "series home",
        "filter by:",
        "by team",
        "fixtures and results",
        "home",
        "scores",
        "series",
        "teams",
        "stats",
    }
    if line.lower() in bad:
        return False

    if re.search(r"\bmatch\b", line, flags=re.I):
        return False
    if re.search(r"\bIST\b", line):
        return False
    if re.search(r"^\d+(st|nd|rd|th)\s+Match", line, flags=re.I):
        return False

    words = line.split()
    if len(words) < 2:
        return False

    return True


def _extract_visible_html_fixtures(html: str) -> List[Dict[str, Any]]:
    """
    Fallback parser for the visible ESPN fixture cards when __NEXT_DATA__ is unavailable.
    """
    soup = BeautifulSoup(html, "html.parser")
    text_lines = [
        " ".join(s.strip().split())
        for s in soup.stripped_strings
        if s and " ".join(s.strip().split())
    ]

    fixtures: List[Dict[str, Any]] = []
    seen = set()

    header_re = re.compile(
        r"^(MON|TUE|WED|THU|FRI|SAT|SUN)\s+\d{1,2}/\d{1,2}\s*-\s*\d{1,2}:\d{2}\s*[AP]M\s*IST$",
        flags=re.I,
    )

    i = 0
    n = len(text_lines)

    while i < n:
        line = text_lines[i]

        if not header_re.match(line):
            i += 1
            continue

        header_line = line
        detail_line = text_lines[i + 1] if i + 1 < n else ""

        team_candidates: List[str] = []
        j = i + 2

        while j < n and len(team_candidates) < 2:
            candidate = text_lines[j]
            if header_re.match(candidate):
                break
            if _looks_like_team_name(candidate):
                team_candidates.append(candidate)
            j += 1

        if len(team_candidates) >= 2:
            team1_name = team_candidates[0]
            team2_name = team_candidates[1]
            date_iso = _parse_header_datetime_to_iso(header_line, detail_line)
            venue = _extract_venue(detail_line)
            item = _to_fixture_dict(
                team1_name=team1_name,
                team2_name=team2_name,
                date_iso=date_iso,
                venue=venue,
            )
            if item:
                key = (item["team1_code"], item["team2_code"], item["date"])
                if key not in seen:
                    seen.add(key)
                    fixtures.append(item)

        i = j

    fixtures.sort(key=lambda x: (x.get("date") or "", x["team1_code"], x["team2_code"]))
    return fixtures


def _scrape_url(url: str, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    """Fetch a single ESPN URL and return parsed fixtures."""
    print(f"[DEBUG] Fetching: {url}", file=sys.stderr)
    with requests.Session() as s:
        r = s.get(url, timeout=20, headers=headers, allow_redirects=True)
        r.raise_for_status()
        html = r.text

    print(f"[DEBUG] HTML length: {len(html)}", file=sys.stderr)

    try:
        next_data = _extract_next_data_json(html)
        fixtures = _extract_from_next_data(next_data)
        print(f"[DEBUG] Parsed via __NEXT_DATA__: {len(fixtures)} fixtures", file=sys.stderr)
        return fixtures
    except FixturesScrapeError as e:
        print(f"[DEBUG] __NEXT_DATA__ failed ({e}), trying HTML fallback", file=sys.stderr)
        fixtures = _extract_visible_html_fixtures(html)
        print(f"[DEBUG] Parsed via HTML fallback: {len(fixtures)} fixtures", file=sys.stderr)
        return fixtures


def fetch_espn_fixtures(season: int, *, use_cache: bool = True) -> Dict[str, Any]:
    if season <= 0:
        raise ValueError("season must be a positive integer")

    ckey = cache_key("fixtures", str(season))
    if use_cache:
        cached = cache_get(ckey)
        if cached is not None:
            return cached

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; IPL-NRR-Sim/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IN,en;q=0.9",
        "Connection": "keep-alive",
    }

    # ── PRIMARY: /fixtures/ page — shows the full upcoming schedule ──
    schedule_url = ESPN_FIXTURES_SCHEDULE_URL_TEMPLATE.format(
        series_id=IPL_SERIES_ID, season=season
    )
    url_used = schedule_url
    fixtures: List[Dict[str, Any]] = []
    seen_keys: set = set()
    note = None

    try:
        fixtures = _scrape_url(schedule_url, headers)
        seen_keys = {f["match_id"] for f in fixtures}
    except Exception as e:
        print(f"[DEBUG] Schedule (fixtures) URL failed: {e}", file=sys.stderr)
        note = f"Schedule page failed: {e}"

    # ── FALLBACK / SUPPLEMENT: /scores/ page — catches live/recent matches ──
    scores_url = ESPN_FIXTURES_URL_TEMPLATE.format(
        series_id=IPL_SERIES_ID, season=season
    )
    try:
        extra = _scrape_url(scores_url, headers)
        added = 0
        for f in extra:
            if f["match_id"] not in seen_keys:
                seen_keys.add(f["match_id"])
                fixtures.append(f)
                added += 1
        print(f"[DEBUG] Scores page added {added} extra fixtures", file=sys.stderr)
    except Exception as e:
        print(f"[DEBUG] Scores URL failed: {e}", file=sys.stderr)

    if not fixtures:
        raise FixturesScrapeError("No fixtures found from either ESPN schedule or scores page")

    fixtures.sort(key=lambda x: (x.get("date") or "", x["team1_code"], x["team2_code"]))

    print(f"[DEBUG] Total fixtures: {len(fixtures)}", file=sys.stderr)
    for f in fixtures:
        print(f"[DEBUG] {f.get('date','')[:10]} {f['team1_code']} vs {f['team2_code']}", file=sys.stderr)

    resp = {
        "season": season,
        "source": "espn",
        "url_used": url_used,
        "last_updated_utc": _utc_now_iso(),
        "fixtures": fixtures,
        "fixtures_count": len(fixtures),
    }

    if note:
        resp["note"] = note

    if use_cache:
        cache_set(ckey, resp, FIXTURES_CACHE_TTL_SECONDS)

    return resp