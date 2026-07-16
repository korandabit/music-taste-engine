#!/usr/bin/env python3
"""self_affinity.py — surface Mark's self-replay affinity over music.db.

Mark scrobbles his own songs (artist "Mark Koranda"). Replay count of one's OWN
track is the strongest "this mattered to me" signal available. This script ranks
self-catalog tracks by play count and applies the engine's LTP gap logic
(gap_days=180) to flag self-songs Mark *returned to* after long absence.

Owned by music_taste_engine (consumption side). The audio project owns the
sw#### canonical-title mapping; if it hands over a clean
`sw#### <-> scrobble-track <-> playcount` table this can join on it.

Usage:
    python self_affinity.py [--db data/music.db] [--artist "Mark Koranda"]
                            [--gap-days 180] [--json]
"""
import argparse
import json
import sqlite3
from collections import defaultdict
from datetime import datetime

SELF_ARTIST = "Mark Koranda"
TS_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d")


def parse_ts(s: str):
    if not s:
        return None
    for f in TS_FORMATS:
        try:
            return datetime.strptime(s, f)
        except ValueError:
            continue
    return None


def self_affinity(db_path: str, artist: str, gap_days: int) -> dict:
    con = sqlite3.connect(db_path)
    rows = con.execute(
        "SELECT track, ts FROM plays WHERE artist = ?", (artist,)
    ).fetchall()
    con.close()

    by_track = defaultdict(list)
    for track, ts in rows:
        d = parse_ts(ts)
        if d:
            by_track[track].append(d)

    tracks = []
    for track, dates in by_track.items():
        dates = sorted(dates)
        gaps = [(dates[i + 1] - dates[i]).days for i in range(len(dates) - 1)]
        long_returns = sum(1 for g in gaps if g >= gap_days)
        tracks.append({
            "track": track,
            "plays": len(dates),
            "long_returns": long_returns,      # returns after >= gap_days silence
            "max_gap_days": max(gaps) if gaps else 0,
            "first_play": str(dates[0].date()),
            "last_play": str(dates[-1].date()),
        })
    tracks.sort(key=lambda t: (-t["plays"], -t["long_returns"]))

    years = defaultdict(int)
    for dates in by_track.values():
        for d in dates:
            years[d.year] += 1

    return {
        "artist": artist,
        "total_self_plays": sum(t["plays"] for t in tracks),
        "distinct_self_tracks": len(tracks),
        "gap_days": gap_days,
        "by_year": dict(sorted(years.items())),
        "tracks": tracks,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default="data/music.db")
    ap.add_argument("--artist", default=SELF_ARTIST)
    ap.add_argument("--gap-days", type=int, default=180)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    result = self_affinity(args.db, args.artist, args.gap_days)

    if args.json:
        print(json.dumps(result, indent=2))
        return

    print(f"Self-replay affinity — {result['artist']}")
    print(f"  {result['total_self_plays']} plays across "
          f"{result['distinct_self_tracks']} tracks "
          f"(gap_days={result['gap_days']})")
    print(f"  by year: {result['by_year']}")
    print(f"\n  {'plays':>5}  {'ret':>3}  {'maxgap':>6}  track")
    for t in result["tracks"][:25]:
        print(f"  {t['plays']:>5}  {t['long_returns']:>3}  "
              f"{t['max_gap_days']:>6}  {t['track']}")


if __name__ == "__main__":
    main()
