# dump_innings.py - fixed version
import sys, os, time, random
sys.path.insert(0, os.path.dirname(__file__))

from ipl_api.cricbuzz_fixtures import KNOWN_MATCH_IDS, _fetch_scorecard_innings
from ipl_api.espn_fixtures import HARDCODED_IPL_2026_FIXTURES
from datetime import datetime, timezone

def main():
    now_utc = datetime.now(timezone.utc)

    completed = []
    for f in HARDCODED_IPL_2026_FIXTURES:
        if f.get("status") not in ("completed", "no_result"):
            continue
        try:
            dt = datetime.fromisoformat(f["date"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            if dt > now_utc:
                continue
        except Exception:
            continue
        completed.append(f)

    print(f"Found {len(completed)} completed matches\n")
    print("Fetching innings data from Cricbuzz...\n")

    print("match_key\twinner_code\tt1_code\tt1_runs\tt1_balls\tt2_code\tt2_runs\tt2_balls\tstatus")

    missing = []
    for f in completed:
        t1 = f["team1_code"]
        t2 = f["team2_code"]
        date = f["date"][:10]
        status = f.get("status")
        winner = f.get("winner_code", "")
        match_key = f"{t1}-{t2}-{date}"

        if status == "no_result":
            print(f"{match_key}\t\t{t1}\t\t\t{t2}\t\t\tno_result")
            continue

        # Look up by date-specific key
        cb_id = KNOWN_MATCH_IDS.get(match_key) or KNOWN_MATCH_IDS.get(f"{t2}-{t1}-{date}")
        if not cb_id:
            missing.append(match_key)
            print(f"{match_key}\t{winner}\t{t1}\t???\t???\t{t2}\t???\t???\tcompleted  <-- NO MATCH ID")
            continue

        time.sleep(random.uniform(1.0, 2.0))
        innings = _fetch_scorecard_innings(cb_id)

        if innings and t1 in innings and t2 in innings:
            print(f"{match_key}\t{winner}\t{t1}\t{innings[t1]['runs']}\t{innings[t1]['balls']}\t{t2}\t{innings[t2]['runs']}\t{innings[t2]['balls']}\tcompleted")
        else:
            missing.append(match_key)
            print(f"{match_key}\t{winner}\t{t1}\t???\t???\t{t2}\t???\t???\tcompleted  <-- MISSING")

    if missing:
        print(f"\nMissing: {missing}")

if __name__ == "__main__":
    main()