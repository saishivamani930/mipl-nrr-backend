from pathlib import Path
import re

p = Path("ipl_api/espn_standings.py")
text = p.read_text(encoding="utf-8")

backup = Path("ipl_api/espn_standings.py.bak_nextjs_points_table")
backup.write_text(text, encoding="utf-8")

new_func = r'''def fetch_cricbuzz_points_table(season: int) -> Optional[Dict[str, Any]]:
    """
    Scrape IPL points table from Cricbuzz.

    Cricbuzz now renders the points table through Next.js streamed data
    instead of a normal HTML <table>. So we parse pointsTableInfo from
    pointsTableData embedded inside self.__next_f.push scripts.
    """
    import json

    CRICBUZZ_SERIES_ID = 9241
    url = f"https://www.cricbuzz.com/cricket-series/{CRICBUZZ_SERIES_ID}/indian-premier-league-{season}/points-table"
    logger.info(f"[STANDINGS] Trying Cricbuzz points table: {url}")

    try:
        html = _fetch_html(url)
    except Exception as e:
        logger.warning(f"[STANDINGS] Cricbuzz points table fetch failed: {e}")
        return None

    def _extract_json_array(s: str, marker: str) -> Optional[str]:
        start = s.find(marker)
        if start == -1:
            return None

        start = s.find("[", start)
        if start == -1:
            return None

        depth = 0
        in_string = False
        escape = False

        for i in range(start, len(s)):
            ch = s[i]

            if escape:
                escape = False
                continue

            if ch == "\\":
                escape = True
                continue

            if ch == '"':
                in_string = not in_string
                continue

            if in_string:
                continue

            if ch == "[":
                depth += 1
            elif ch == "]":
                depth -= 1
                if depth == 0:
                    return s[start:i + 1]

        return None

    # Cricbuzz embeds JSON inside escaped Next.js stream strings.
    # Convert \" into " so normal JSON patterns can be searched.
    decoded = html.replace('\\"', '"').replace("\\\\/", "/")

    arr_text = _extract_json_array(decoded, '"pointsTableInfo":')
    if not arr_text:
        logger.warning("[STANDINGS] Cricbuzz Next.js pointsTableInfo not found")
        return None

    try:
        rows = json.loads(arr_text)
    except Exception as e:
        logger.warning(f"[STANDINGS] Cricbuzz pointsTableInfo JSON parse failed: {e}")
        return None

    TEAM_CANONICAL = {
        "RCB": "Royal Challengers Bengaluru",
        "CSK": "Chennai Super Kings",
        "MI": "Mumbai Indians",
        "KKR": "Kolkata Knight Riders",
        "SRH": "Sunrisers Hyderabad",
        "RR": "Rajasthan Royals",
        "DC": "Delhi Capitals",
        "PBKS": "Punjab Kings",
        "LSG": "Lucknow Super Giants",
        "GT": "Gujarat Titans",
    }

    teams = []

    for row in rows:
        code = str(row.get("teamName") or "").strip().upper()
        name = str(row.get("teamFullName") or TEAM_CANONICAL.get(code, code)).strip()

        if not code:
            continue

        nrr_raw = row.get("nrr")
        nrr_val = _safe_float(str(nrr_raw).replace("+", "")) if nrr_raw is not None else None

        teams.append({
            "team": TEAM_CANONICAL.get(code, name),
            "code": code,
            "matches": _safe_int(row.get("matchesPlayed")),
            "won": _safe_int(row.get("matchesWon")),
            "lost": _safe_int(row.get("matchesLost")),
            "nr": _safe_int(row.get("noRes")),
            "tied": _safe_int(row.get("matchesTied")),
            "points": _safe_int(row.get("points")),
            "nrr": nrr_val,
        })

    if not teams:
        logger.warning("[STANDINGS] Cricbuzz Next.js parser produced 0 teams")
        return None

    logger.info(f"[STANDINGS] ? Cricbuzz Next.js points table parsed {len(teams)} teams")

    return {
        "season": season,
        "source": "cricbuzz",
        "last_updated_utc": datetime.utcnow().isoformat() + "Z",
        "teams": teams,
    }
'''

pattern = r"def fetch_cricbuzz_points_table\(season: int\) -> Optional\[Dict\[str, Any\]\]:.*?(?=\ndef _enrich_with_innings_aggregates)"
new_text, count = re.subn(pattern, new_func + "\n", text, flags=re.S)

if count != 1:
    raise SystemExit(f"Patch failed: expected 1 replacement, got {count}")

p.write_text(new_text, encoding="utf-8")
print("Patched fetch_cricbuzz_points_table successfully")
