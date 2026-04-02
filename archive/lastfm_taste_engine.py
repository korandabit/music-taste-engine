"""
lastfm_taste_engine.py
─────────────────────
Generates all underlying sets and scalars for:
  - Listening autobiography
  - Long-delay true positive detection
  - Spring affinity scoring
  - 50-track rocketfuel playlist

Input:  Last.fm CSV export (benjaminbenben.com/lastfm-to-csv/)
        Columns: artist, album, track, date  (no header row)

Output: JSON file with all named sets + scalars
        Console: human-readable summary

Usage:
    python lastfm_taste_engine.py --csv edgarturtleblot.csv --out output.json
    python lastfm_taste_engine.py --csv edgarturtleblot.csv --playlist 50
"""

import argparse
import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


# ─── Config ───────────────────────────────────────────────────────────────────

SPRING_MONTHS    = {3, 4, 5}
SUMMER_MONTHS    = {6, 7, 8}
FALL_MONTHS      = {9, 10, 11}
WINTER_MONTHS    = {12, 1, 2}
LATE_NIGHT_HOURS = {22, 23, 0, 1, 2, 3}

SEASON_MAP = {m: "Spring" for m in SPRING_MONTHS}
SEASON_MAP.update({m: "Summer" for m in SUMMER_MONTHS})
SEASON_MAP.update({m: "Fall"   for m in FALL_MONTHS})
SEASON_MAP.update({m: "Winter" for m in WINTER_MONTHS})

DATE_FORMAT = "%d %b %Y %H:%M"

# Long-delay true positive thresholds
LONG_GAP_DAYS        = 180   # gap between plays to count as a "return"
MIN_RETURNS_FOR_LTP  = 2     # min qualifying returns to be a true positive
MIN_PLAYS_FOR_LTP    = 4     # min total plays
SPRING_RATIO_MIN     = 0.30  # min fraction of plays in spring months
REST_MIN_DAYS        = 45    # track must be rested at least this long

# Playlist scoring weights
W_RETURNS   = 0.35
W_SPRING    = 0.30
W_DEPTH     = 0.20
W_REST      = 0.15
REST_CAP    = 600   # days — cap rest contribution at this value

MAX_PER_ARTIST_PLAYLIST = 4


# ─── Parsing ──────────────────────────────────────────────────────────────────

def load_csv(path: str) -> list[dict]:
    """
    Load Last.fm CSV export.
    Returns list of dicts: {artist, album, track, date (datetime), ...derived fields}
    Drops rows with unparseable dates or pre-2005 timestamps (epoch artifacts).
    """
    plays = []
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            parts = line.split(",", 3)
            if len(parts) < 4:
                continue
            artist, album, track, raw_date = [p.strip().strip('"') for p in parts]
            try:
                dt = datetime.strptime(raw_date, DATE_FORMAT)
            except ValueError:
                continue
            if dt.year < 2005:
                continue
            plays.append({
                "artist": artist,
                "album":  album,
                "track":  track,
                "date":   dt,
            })
    return plays


def enrich(plays: list[dict], reference_date: datetime = None) -> list[dict]:
    """Add derived fields to every play."""
    ref = reference_date or max(p["date"] for p in plays)
    for p in plays:
        p["days_ago"] = (ref - p["date"]).days
        p["month"]    = p["date"].month
        p["year"]     = p["date"].year
        p["hour"]     = p["date"].hour
        p["season"]   = SEASON_MAP[p["date"].month]
        p["is_spring"]     = p["month"] in SPRING_MONTHS
        p["is_late_night"] = p["hour"] in LATE_NIGHT_HOURS
    return plays, ref


# ─── Aggregate helpers ────────────────────────────────────────────────────────

def count_by(plays, key_fn):
    """Return {key: count} dict sorted descending."""
    counts = defaultdict(int)
    for p in plays:
        counts[key_fn(p)] += 1
    return dict(sorted(counts.items(), key=lambda x: -x[1]))


def top_n(d: dict, n: int) -> list[tuple]:
    return list(d.items())[:n]


# ─── Temporal overview ────────────────────────────────────────────────────────

def temporal_overview(plays, ref):
    first = min(p["date"] for p in plays)
    span_days  = (ref - first).days
    span_years = span_days / 365.25

    return {
        "first_scrobble":    str(first),
        "last_scrobble":     str(ref),
        "total_plays":       len(plays),
        "unique_artists":    len({p["artist"] for p in plays}),
        "unique_tracks":     len({(p["artist"], p["track"]) for p in plays}),
        "unique_albums":     len({(p["artist"], p["album"]) for p in plays}),
        "span_days":         span_days,
        "span_years":        round(span_years, 1),
        "avg_plays_per_day": round(len(plays) / span_days, 1),
    }


# ─── Listening clock ──────────────────────────────────────────────────────────

def listening_clock(plays):
    by_hour = count_by(plays, lambda p: p["hour"])
    peak_hour = max(by_hour, key=by_hour.get)
    late_night = sum(1 for p in plays if p["is_late_night"])
    return {
        "plays_by_hour":       dict(sorted(by_hour.items())),
        "peak_hour":           peak_hour,
        "peak_hour_plays":     by_hour[peak_hour],
        "late_night_plays":    late_night,
        "late_night_pct":      round(100 * late_night / len(plays), 1),
    }


# ─── Seasonal distribution ────────────────────────────────────────────────────

def seasonal_distribution(plays):
    by_season = count_by(plays, lambda p: p["season"])
    total = len(plays)
    return {
        s: {"plays": c, "pct": round(100 * c / total, 1)}
        for s, c in by_season.items()
    }


# ─── Top lists ────────────────────────────────────────────────────────────────

def top_tracks(plays, n=20):
    counts = count_by(plays, lambda p: (p["artist"], p["track"]))
    return [{"artist": a, "track": t, "plays": c} for (a, t), c in top_n(counts, n)]


def top_albums(plays, n=20):
    counts = count_by(plays, lambda p: (p["artist"], p["album"]))
    return [{"artist": a, "album": b, "plays": c} for (a, b), c in top_n(counts, n)]


def top_artists(plays, n=20):
    counts = count_by(plays, lambda p: p["artist"])
    return [{"artist": a, "plays": c} for a, c in top_n(counts, n)]


def top_spring_tracks(plays, n=20):
    spring = [p for p in plays if p["is_spring"]]
    return top_tracks(spring, n)


# ─── Year-by-year ─────────────────────────────────────────────────────────────

def year_by_year(plays):
    by_year = defaultdict(list)
    for p in plays:
        by_year[p["year"]].append(p)

    result = {}
    for yr in sorted(by_year):
        yr_plays = by_year[yr]
        artist_counts = count_by(yr_plays, lambda p: p["artist"])
        top_artist    = list(artist_counts.items())[0]
        total         = len(yr_plays)

        # Obsessions: artists > 10% of year
        obsessions = [
            {"artist": a, "plays": c, "pct": round(100 * c / total, 1)}
            for a, c in artist_counts.items()
            if c / total > 0.10
        ]

        result[yr] = {
            "total_plays":  total,
            "top_artist":   {"artist": top_artist[0], "plays": top_artist[1]},
            "obsessions":   obsessions,
            "top_10_artists": [
                {"artist": a, "plays": c}
                for a, c in list(artist_counts.items())[:10]
            ],
        }
    return result


# ─── Artist timeline ──────────────────────────────────────────────────────────

def artist_timeline(plays, artist: str) -> dict:
    """
    Returns year-by-year play counts for a single artist.
    Useful for dormancy/return pattern visualization.
    """
    ap = [p for p in plays if p["artist"].lower() == artist.lower()]
    by_year = defaultdict(int)
    for p in ap:
        by_year[p["year"]] += 1
    return {
        "artist":       artist,
        "total_plays":  len(ap),
        "by_year":      dict(sorted(by_year.items())),
        "active_years": sorted(by_year.keys()),
        "peak_year":    max(by_year, key=by_year.get) if by_year else None,
    }


# ─── Long-delay true positives ────────────────────────────────────────────────

def long_delay_true_positives(plays, ref,
                               long_gap_days=LONG_GAP_DAYS,
                               min_returns=MIN_RETURNS_FOR_LTP,
                               min_plays=MIN_PLAYS_FOR_LTP):
    """
    For each (artist, track), compute:
      - All play dates sorted
      - Gaps between consecutive plays
      - Number of gaps > long_gap_days  ("long returns")
      - Max single gap
      - Days since last play
      - Spring ratio

    A "long-delay true positive" = track you reliably return to
    after extended absence. High long_returns = structurally important.
    """
    track_map = defaultdict(list)
    for p in plays:
        track_map[(p["artist"], p["track"])].append(p["date"])

    results = []
    for (artist, track), dates in track_map.items():
        dates = sorted(dates)
        if len(dates) < min_plays:
            continue

        gaps = [(dates[i+1] - dates[i]).days for i in range(len(dates) - 1)]
        long_returns  = sum(1 for g in gaps if g >= long_gap_days)
        max_gap       = max(gaps) if gaps else 0
        days_since    = (ref - dates[-1]).days
        spring_plays  = sum(1 for d in dates if d.month in SPRING_MONTHS)
        spring_ratio  = spring_plays / len(dates)
        lifespan_days = (dates[-1] - dates[0]).days

        if long_returns < min_returns:
            continue

        results.append({
            "artist":        artist,
            "track":         track,
            "total_plays":   len(dates),
            "long_returns":  long_returns,
            "max_gap_days":  max_gap,
            "days_since":    days_since,
            "spring_plays":  spring_plays,
            "spring_ratio":  round(spring_ratio, 3),
            "lifespan_days": lifespan_days,
            "first_play":    str(dates[0]),
            "last_play":     str(dates[-1]),
            "all_gaps":      gaps,           # full set — slice as needed
        })

    results.sort(key=lambda r: (-r["long_returns"], -r["max_gap_days"]))
    return results


# ─── Playlist scoring ─────────────────────────────────────────────────────────

def score_track(r, max_returns, max_log_plays):
    """
    Composite rocketfuel score for a single LTP record.
    All components normalized 0–1.
    """
    s_returns  = r["long_returns"]  / max_returns
    s_spring   = r["spring_ratio"]
    s_depth    = math.log1p(r["total_plays"]) / max_log_plays
    s_rest     = min(r["days_since"], REST_CAP) / REST_CAP

    return round(
        W_RETURNS * s_returns +
        W_SPRING  * s_spring  +
        W_DEPTH   * s_depth   +
        W_REST    * s_rest,
        4
    )


def build_playlist(ltp_records, n=50,
                   rest_min=REST_MIN_DAYS,
                   spring_min=SPRING_RATIO_MIN,
                   max_per_artist=MAX_PER_ARTIST_PLAYLIST):
    """
    From long-delay true positives, select top-N tracks for a playlist.

    Filters:
      - rested >= rest_min days
      - spring_ratio >= spring_min
      - min 4 plays (already enforced upstream)

    Caps artist representation at max_per_artist.
    """
    pool = [
        r for r in ltp_records
        if r["days_since"]   >= rest_min
        and r["spring_ratio"] >= spring_min
    ]

    if not pool:
        return []

    max_returns   = max(r["long_returns"]  for r in pool)
    max_log_plays = max(math.log1p(r["total_plays"]) for r in pool)

    for r in pool:
        r["score"] = score_track(r, max_returns, max_log_plays)

    pool.sort(key=lambda r: -r["score"])

    selected = []
    artist_counts = defaultdict(int)
    for r in pool:
        if artist_counts[r["artist"]] >= max_per_artist:
            continue
        selected.append(r)
        artist_counts[r["artist"]] += 1
        if len(selected) >= n:
            break

    return selected


# ─── Main ─────────────────────────────────────────────────────────────────────

def run(csv_path: str, playlist_size: int = 50, out_path: str = None,
        reference_date: datetime = None):

    print(f"Loading {csv_path} ...", flush=True)
    raw = load_csv(csv_path)
    plays, ref = enrich(raw, reference_date)
    print(f"  {len(plays):,} plays loaded. Reference date: {ref.strftime('%Y-%m-%d')}")

    print("Computing overview ...", flush=True)
    overview    = temporal_overview(plays, ref)
    clock       = listening_clock(plays)
    seasonal    = seasonal_distribution(plays)
    t_tracks    = top_tracks(plays, 25)
    t_albums    = top_albums(plays, 25)
    t_artists   = top_artists(plays, 25)
    t_spring    = top_spring_tracks(plays, 25)
    yby         = year_by_year(plays)

    print("Detecting long-delay true positives ...", flush=True)
    ltp = long_delay_true_positives(plays, ref)
    print(f"  {len(ltp):,} qualifying tracks found.")

    print(f"Building {playlist_size}-track playlist ...", flush=True)
    playlist = build_playlist(ltp, n=playlist_size)
    print(f"  {len(playlist)} tracks selected.")

    output = {
        "meta": {
            "csv":            csv_path,
            "reference_date": str(ref),
            "generated_at":   str(datetime.now()),
            "config": {
                "long_gap_days":       LONG_GAP_DAYS,
                "min_returns_for_ltp": MIN_RETURNS_FOR_LTP,
                "spring_months":       sorted(SPRING_MONTHS),
                "rest_min_days":       REST_MIN_DAYS,
                "spring_ratio_min":    SPRING_RATIO_MIN,
                "playlist_weights":    {
                    "returns": W_RETURNS,
                    "spring":  W_SPRING,
                    "depth":   W_DEPTH,
                    "rest":    W_REST,
                },
            },
        },
        "overview":         overview,
        "clock":            clock,
        "seasonal":         seasonal,
        "top_tracks":       t_tracks,
        "top_albums":       t_albums,
        "top_artists":      t_artists,
        "top_spring_tracks":t_spring,
        "year_by_year":     yby,
        "long_delay_true_positives": ltp,
        "playlist":         playlist,
    }

    if out_path:
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(output, fh, indent=2, default=str)
        print(f"\nOutput written to {out_path}")

    # ── Console summary ──────────────────────────────────────────────────────

    o = overview
    print(f"""
━━━ OVERVIEW ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  {o['total_plays']:,} plays · {o['unique_artists']:,} artists · {o['unique_tracks']:,} tracks
  {o['span_years']} years  ({o['first_scrobble'][:10]} → {o['last_scrobble'][:10]})
  avg {o['avg_plays_per_day']} plays/day

  Late night (10pm–4am): {clock['late_night_pct']}% of all plays
  Peak hour: {clock['peak_hour']}:00 ({clock['peak_hour_plays']} plays)

━━━ SEASONAL ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━""")
    for s, d in seasonal.items():
        print(f"  {s:<8} {d['plays']:>6,}  ({d['pct']}%)")

    print("\n━━━ TOP 10 ARTISTS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    for i, a in enumerate(t_artists[:10], 1):
        print(f"  {i:>2}. {a['artist']:<30} {a['plays']:>5,}")

    print("\n━━━ TOP 10 TRACKS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    for i, t in enumerate(t_tracks[:10], 1):
        print(f"  {i:>2}. {t['artist']} — {t['track']}  ({t['plays']}×)")

    print("\n━━━ LONG-DELAY TRUE POSITIVES (top 15) ━━━━━━━━━━")
    for i, r in enumerate(ltp[:15], 1):
        print(f"  {i:>2}. {r['artist']} — {r['track']}")
        print(f"       {r['total_plays']}× plays · {r['long_returns']} returns · "
              f"max gap {r['max_gap_days']}d · {int(r['spring_ratio']*100)}% spring · "
              f"{r['days_since']}d rest")

    print(f"\n━━━ PLAYLIST ({len(playlist)} tracks) ━━━━━━━━━━━━━━━━━━━━━━")
    for i, r in enumerate(playlist, 1):
        print(f"  {i:>2}. {r['artist']} — {r['track']}")
        print(f"       score {r['score']} · {r['long_returns']} returns · "
              f"{int(r['spring_ratio']*100)}% spring · {r['days_since']}d rest")

    return output


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Last.fm taste engine")
    parser.add_argument("--csv",      required=True,  help="Path to Last.fm CSV export")
    parser.add_argument("--out",      default=None,   help="Write JSON output to this path")
    parser.add_argument("--playlist", type=int, default=50, help="Playlist size (default 50)")
    parser.add_argument("--refdate",  default=None,
                        help="Reference date YYYY-MM-DD (default: latest scrobble)")
    args = parser.parse_args()

    ref = None
    if args.refdate:
        ref = datetime.strptime(args.refdate, "%Y-%m-%d")

    run(args.csv, playlist_size=args.playlist, out_path=args.out, reference_date=ref)
