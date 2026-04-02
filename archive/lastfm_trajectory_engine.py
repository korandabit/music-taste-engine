"""
lastfm_trajectory_engine.py
────────────────────────────
Artist- or catalog-level deep analysis on a Last.fm CSV export.
Extends lastfm_taste_engine.py with:

  1. Density-based listening epoch detection
  2. Per-epoch rate normalization (plays per 1000 total)
  3. Per-track trajectory classification (BURN / PERENNIAL / SLOW_BURN / etc.)
  4. Discovery-vs-rediscovery signature analysis
  5. Session-level behavioral fingerprints (ppd, repeat rate, late-night %)
  6. Early-binge → lifespan prediction correlations
  7. Cross-epoch trajectory tables
  8. Percentile-normalized popularity comparison (if reference data supplied)

Input:  Last.fm CSV (same format as taste engine: artist, album, track, date)
        --artist flag to filter to a single artist (optional; omit for full catalog)
        --popularity JSON for external popularity reference (optional)

Output: JSON with all computed sets; console summary

Usage:
    python lastfm_trajectory_engine.py --csv export.csv --artist "Muse" --out muse_deep.json
    python lastfm_trajectory_engine.py --csv export.csv --out full_deep.json
    python lastfm_trajectory_engine.py --csv export.csv --artist "Radiohead" --popularity radio_pop.json

Popularity JSON format (optional):
    {"track_name": popularity_int_0_to_100, ...}
"""

import argparse
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

# ─── Config ───────────────────────────────────────────────────────────────────

DATE_FORMAT = "%d %b %Y %H:%M"
MIN_YEAR = 2005  # drop pre-scrobble artifacts

# Epoch detection
DORMANT_MONTH_THRESHOLD = 30  # plays/month below this = dormant
MIN_EPOCH_MONTHS = 2          # minimum months to constitute an epoch

# Trajectory classification
LONG_GAP_DAYS = 180
MIN_PLAYS_TRAJECTORY = 5
MIN_SPAN_DAYS = 30

# Burn/perennial thresholds
BURN_Q1_THRESHOLD = 0.65
PERENNIAL_Q4_THRESHOLD = 0.15
PERENNIAL_MIN_RETURNS = 4
FLASH_BINGE_30D = 0.50
SLOW_BURN_Q4_EXCEEDS_Q1 = True

# Session analysis
LATE_NIGHT_HOURS = {22, 23, 0, 1, 2, 3}


# ─── Parsing ──────────────────────────────────────────────────────────────────

def load_csv(path: str) -> list[dict]:
    """Load Last.fm CSV. Returns list of {artist, album, track, ts}."""
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
                ts = datetime.strptime(raw_date.strip(), DATE_FORMAT)
            except ValueError:
                continue
            if ts.year < MIN_YEAR:
                continue
            plays.append({"artist": artist, "album": album, "track": track, "ts": ts})
    plays.sort(key=lambda x: x["ts"])
    return plays


# ─── Epoch Detection ─────────────────────────────────────────────────────────

def detect_epochs(plays: list[dict]) -> list[dict]:
    """
    Cluster contiguous months with >= DORMANT_MONTH_THRESHOLD plays into epochs.
    Returns list of {name, start, end, months, total, artist_plays (if filtered)}.
    """
    monthly = Counter((p["ts"].year, p["ts"].month) for p in plays)
    sorted_months = sorted(monthly.keys())

    epochs = []
    current = {"months": [], "total": 0}

    for ym in sorted_months:
        if monthly[ym] >= DORMANT_MONTH_THRESHOLD:
            current["months"].append(ym)
            current["total"] += monthly[ym]
        else:
            if len(current["months"]) >= MIN_EPOCH_MONTHS:
                current["start"] = current["months"][0]
                current["end"] = current["months"][-1]
                epochs.append(current)
            current = {"months": [], "total": 0}

    if len(current["months"]) >= MIN_EPOCH_MONTHS:
        current["start"] = current["months"][0]
        current["end"] = current["months"][-1]
        epochs.append(current)

    for i, e in enumerate(epochs):
        e["name"] = f"E{i+1}"

    return epochs


def in_epoch(ts, epoch):
    ym = (ts.year, ts.month)
    return epoch["start"] <= ym <= epoch["end"]


# ─── Track Analysis ──────────────────────────────────────────────────────────

def analyze_tracks(plays: list[dict], all_plays: list[dict], epochs: list[dict],
                   ref_date: datetime) -> list[dict]:
    """
    Per-track trajectory analysis. `plays` = filtered (artist), `all_plays` = full corpus.
    """
    track_dates = defaultdict(list)
    track_albums = defaultdict(set)
    for p in plays:
        track_dates[p["track"]].append(p["ts"])
        track_albums[p["track"]].add(p["album"])
    for t in track_dates:
        track_dates[t].sort()

    results = []

    for track, dates in track_dates.items():
        total = len(dates)
        if total < MIN_PLAYS_TRAJECTORY:
            continue

        first, last = dates[0], dates[-1]
        span = (last - first).days
        if span < MIN_SPAN_DAYS:
            continue

        # ── Temporal distribution ──
        burst_30 = sum(1 for d in dates if (d - first).days <= 30)
        burst_90 = sum(1 for d in dates if (d - first).days <= 90)

        q1_end = first + timedelta(days=span * 0.25)
        q4_start = first + timedelta(days=span * 0.75)
        q1 = sum(1 for d in dates if d <= q1_end) / total
        q4 = sum(1 for d in dates if d >= q4_start) / total

        # ── Gaps and returns ──
        gaps = [(dates[i] - dates[i-1]).days for i in range(1, len(dates))]
        long_returns = sum(1 for g in gaps if g > LONG_GAP_DAYS)

        rediscoveries = []
        for i, g in enumerate(gaps):
            if g > LONG_GAP_DAYS:
                return_date = dates[i+1]
                cluster = sum(1 for d in dates[i+1:] if (d - return_date).days <= 30)
                rediscoveries.append({
                    "gap_days": g,
                    "return_date": return_date.strftime("%Y-%m-%d"),
                    "return_year": return_date.year,
                    "cluster_size": cluster,
                })

        # ── Session fingerprint ──
        distinct_days = len(set(d.date() for d in dates))
        ppd = total / distinct_days if distinct_days else 0

        hour_groups = Counter((d.date(), d.hour) for d in dates)
        repeat_plays = sum(c - 1 for c in hour_groups.values() if c > 1)
        repeat_rate = repeat_plays / total

        late_night = sum(1 for d in dates if d.hour in LATE_NIGHT_HOURS) / total

        # ── Year distribution ──
        year_dist = Counter(d.year for d in dates)

        # ── Peak month ──
        month_counts = Counter((d.year, d.month) for d in dates)
        peak_month, peak_count = month_counts.most_common(1)[0]

        # ── Epoch rates ──
        epoch_rates = {}
        for epoch in epochs:
            ep_total = sum(1 for p in all_plays if in_epoch(p["ts"], epoch))
            ep_track = sum(1 for d in dates if in_epoch(d, epoch))
            rate = (ep_track / ep_total * 1000) if ep_total > 0 else 0
            epoch_rates[epoch["name"]] = {
                "plays": ep_track,
                "rate_per_1000": round(rate, 2),
                "epoch_total": ep_total,
            }

        # ── Trajectory classification ──
        trajectory = classify_trajectory(
            burst_30/total, burst_90/total, q1, q4, long_returns, len(rediscoveries)
        )

        # ── Days since last play ──
        days_since = (ref_date - last).days

        results.append({
            "track": track,
            "albums": sorted(track_albums[track]),
            "total_plays": total,
            "span_days": span,
            "first_play": first.strftime("%Y-%m-%d"),
            "last_play": last.strftime("%Y-%m-%d"),
            "days_since_last": days_since,
            "burst_30": burst_30,
            "burst_90": burst_90,
            "burst_ratio_30": round(burst_30 / total, 3),
            "burst_ratio_90": round(burst_90 / total, 3),
            "q1": round(q1, 3),
            "q4": round(q4, 3),
            "long_returns": long_returns,
            "rediscoveries": rediscoveries,
            "trajectory": trajectory,
            "session": {
                "distinct_days": distinct_days,
                "plays_per_active_day": round(ppd, 2),
                "repeat_rate": round(repeat_rate, 3),
                "late_night_pct": round(late_night, 3),
            },
            "peak_month": f"{peak_month[0]}-{peak_month[1]:02d}",
            "peak_month_plays": peak_count,
            "year_distribution": dict(sorted(year_dist.items())),
            "epoch_rates": epoch_rates,
        })

    results.sort(key=lambda x: -x["total_plays"])
    return results


def classify_trajectory(br30, br90, q1, q4, long_returns, n_rediscoveries):
    """Classify a track's temporal trajectory."""
    if br30 >= FLASH_BINGE_30D:
        return "FLASH_BINGE"
    if br90 >= 0.6:
        return "DISCOVERY_HEAVY"
    if n_rediscoveries >= 3 and q4 >= PERENNIAL_Q4_THRESHOLD:
        return "PERENNIAL_RETURN"
    if q4 >= q1 * 0.8 and n_rediscoveries >= 2:
        return "SLOW_BURN"
    if q1 >= BURN_Q1_THRESHOLD:
        return "FRONT_LOADED"
    if n_rediscoveries >= 2:
        return "REDISCOVERY"
    return "DIFFUSE"


# ─── Correlations ─────────────────────────────────────────────────────────────

def pearson_r(x, y):
    n = len(x)
    if n < 3:
        return 0
    mx, my = sum(x)/n, sum(y)/n
    sx = math.sqrt(sum((xi-mx)**2 for xi in x)/n)
    sy = math.sqrt(sum((yi-my)**2 for yi in y)/n)
    if sx == 0 or sy == 0:
        return 0
    return sum((xi-mx)*(yi-my) for xi, yi in zip(x, y)) / (n * sx * sy)


def compute_correlations(tracks: list[dict], min_plays: int = 20) -> dict:
    """Early-binge → lifespan/returns prediction."""
    eligible = [t for t in tracks if t["total_plays"] >= min_plays]
    if len(eligible) < 5:
        return {}

    bursts = [t["burst_ratio_30"] for t in eligible]
    totals = [t["total_plays"] for t in eligible]
    spans = [t["span_days"] for t in eligible]
    returns = [t["long_returns"] for t in eligible]

    return {
        "n": len(eligible),
        "min_plays": min_plays,
        "burst30_vs_total": round(pearson_r(bursts, totals), 3),
        "burst30_vs_span": round(pearson_r(bursts, spans), 3),
        "burst30_vs_returns": round(pearson_r(bursts, returns), 3),
    }


# ─── Discovery Latency ───────────────────────────────────────────────────────

def compute_discovery_latency(plays: list[dict], tracks: list[dict]) -> list[dict]:
    """For each track, how long after its album's first play did this track first appear?"""
    album_first = {}
    for p in plays:
        a = p["album"]
        if a not in album_first or p["ts"] < album_first[a]:
            album_first[a] = p["ts"]

    results = []
    for t in tracks:
        alb = t["albums"][0] if t["albums"] else None
        if alb and alb in album_first:
            first_track = datetime.strptime(t["first_play"], "%Y-%m-%d")
            delay = (first_track - album_first[alb]).days
            if delay > 60:
                results.append({
                    "track": t["track"],
                    "album": alb,
                    "album_first_play": album_first[alb].strftime("%Y-%m-%d"),
                    "track_first_play": t["first_play"],
                    "delay_days": delay,
                    "trajectory": t["trajectory"],
                    "q4": t["q4"],
                })

    results.sort(key=lambda x: -x["delay_days"])
    return results


# ─── Popularity Comparison ────────────────────────────────────────────────────

def percentile_compare(tracks: list[dict], pop_data: dict) -> list[dict]:
    """
    Percentile-rank both your plays and external popularity within their domains.
    pop_data: {track_name: int_0_to_100}
    """
    n = len(tracks)
    your_sorted = sorted(tracks, key=lambda x: -x["total_plays"])
    your_pct = {}
    for i, t in enumerate(your_sorted):
        your_pct[t["track"]] = round((1 - i / n) * 100, 1)

    results = []
    for track_name, world_pct in pop_data.items():
        if track_name in your_pct:
            delta = your_pct[track_name] - world_pct
            results.append({
                "track": track_name,
                "your_percentile": your_pct[track_name],
                "world_percentile": world_pct,
                "delta": round(delta, 1),
            })

    results.sort(key=lambda x: -x["delta"])
    return results


# ─── Output ───────────────────────────────────────────────────────────────────

def build_output(plays, all_plays, artist_filter, epochs, tracks, ref_date, pop_data=None):
    """Assemble complete output dict."""
    out = {
        "meta": {
            "ref_date": ref_date.strftime("%Y-%m-%d"),
            "artist_filter": artist_filter,
            "total_plays": len(all_plays),
            "filtered_plays": len(plays),
            "unique_tracks": len(set(p["track"] for p in plays)),
            "date_range": {
                "first": plays[0]["ts"].strftime("%Y-%m-%d") if plays else None,
                "last": plays[-1]["ts"].strftime("%Y-%m-%d") if plays else None,
            },
        },
        "epochs": [
            {
                "name": e["name"],
                "start": f"{e['start'][0]}-{e['start'][1]:02d}",
                "end": f"{e['end'][0]}-{e['end'][1]:02d}",
                "months": len(e["months"]),
                "total_plays": e["total"],
            }
            for e in epochs
        ],
        "tracks": tracks,
        "trajectory_summary": dict(Counter(t["trajectory"] for t in tracks)),
        "correlations": compute_correlations(tracks),
        "discovery_latency": compute_discovery_latency(plays, tracks),
    }

    if pop_data:
        out["popularity_comparison"] = percentile_compare(tracks, pop_data)

    # ── Aggregate stats by trajectory type ──
    type_stats = {}
    for traj_type in set(t["trajectory"] for t in tracks):
        subset = [t for t in tracks if t["trajectory"] == traj_type]
        type_stats[traj_type] = {
            "count": len(subset),
            "avg_plays": round(statistics.mean([t["total_plays"] for t in subset]), 1),
            "avg_span_days": round(statistics.mean([t["span_days"] for t in subset]), 0),
            "avg_q1": round(statistics.mean([t["q1"] for t in subset]), 3),
            "avg_q4": round(statistics.mean([t["q4"] for t in subset]), 3),
            "avg_returns": round(statistics.mean([t["long_returns"] for t in subset]), 1),
            "avg_ppd": round(statistics.mean([t["session"]["plays_per_active_day"] for t in subset]), 2),
            "avg_repeat_rate": round(statistics.mean([t["session"]["repeat_rate"] for t in subset]), 3),
            "avg_late_night": round(statistics.mean([t["session"]["late_night_pct"] for t in subset]), 3),
        }
    out["trajectory_type_stats"] = type_stats

    return out


def print_summary(out):
    """Human-readable console output."""
    meta = out["meta"]
    print(f"\n{'━' * 60}")
    print(f"  TRAJECTORY ENGINE — {meta['artist_filter'] or 'ALL ARTISTS'}")
    print(f"  {meta['filtered_plays']:,} plays · {meta['unique_tracks']} tracks")
    print(f"  {meta['date_range']['first']} → {meta['date_range']['last']}")
    print(f"{'━' * 60}")

    print(f"\n{'━' * 40} EPOCHS")
    for e in out["epochs"]:
        print(f"  {e['name']}: {e['start']} → {e['end']} | {e['months']}mo | {e['total_plays']:,} plays")

    print(f"\n{'━' * 40} TRAJECTORY DISTRIBUTION")
    for traj, count in sorted(out["trajectory_summary"].items(), key=lambda x: -x[1]):
        stats = out["trajectory_type_stats"][traj]
        print(f"  {traj:<20} n={count:>3}  avg_plays={stats['avg_plays']:>5.1f}  "
              f"avg_span={stats['avg_span_days']:>5.0f}d  ppd={stats['avg_ppd']:.1f}  "
              f"repeat={stats['avg_repeat_rate']:.0%}  q1={stats['avg_q1']:.2f}  q4={stats['avg_q4']:.2f}")

    if out["correlations"]:
        c = out["correlations"]
        print(f"\n{'━' * 40} BINGE → OUTCOME (n={c['n']}, ≥{c['min_plays']} plays)")
        print(f"  burst_30 → total_plays:  r = {c['burst30_vs_total']:+.3f}")
        print(f"  burst_30 → lifespan:     r = {c['burst30_vs_span']:+.3f}")
        print(f"  burst_30 → long_returns: r = {c['burst30_vs_returns']:+.3f}")

    print(f"\n{'━' * 40} TOP TRACKS BY TYPE")
    for traj in ["PERENNIAL_RETURN", "FRONT_LOADED", "SLOW_BURN", "REDISCOVERY", "FLASH_BINGE"]:
        subset = [t for t in out["tracks"] if t["trajectory"] == traj][:5]
        if subset:
            print(f"\n  {traj}:")
            for t in subset:
                s = t["session"]
                print(f"    {t['track']:<35} {t['total_plays']:>3}× | "
                      f"q1:{t['q1']:.0%} q4:{t['q4']:.0%} | "
                      f"ppd:{s['plays_per_active_day']:.1f} rpt:{s['repeat_rate']:.0%} "
                      f"late:{s['late_night_pct']:.0%} | returns:{t['long_returns']}")

    if out.get("popularity_comparison"):
        print(f"\n{'━' * 40} POPULARITY DELTA (top 10 you > world)")
        for r in out["popularity_comparison"][:10]:
            print(f"  {r['track']:<35} you:{r['your_percentile']:>5.1f}%  "
                  f"world:{r['world_percentile']:>5.1f}%  Δ={r['delta']:+.1f}")

    print()


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Last.fm trajectory analysis engine")
    ap.add_argument("--csv", required=True, help="Path to Last.fm CSV export")
    ap.add_argument("--artist", default=None, help="Filter to single artist (case-insensitive)")
    ap.add_argument("--out", default=None, help="Output JSON path")
    ap.add_argument("--refdate", default=None, help="Reference date YYYY-MM-DD (default: today)")
    ap.add_argument("--popularity", default=None, help="JSON file with {track: popularity_0_100}")
    ap.add_argument("--min-plays", type=int, default=5, help="Min plays for track analysis")
    args = ap.parse_args()

    ref_date = datetime.strptime(args.refdate, "%Y-%m-%d") if args.refdate else datetime.now()

    print(f"Loading {args.csv} ...")
    all_plays = load_csv(args.csv)
    print(f"  {len(all_plays):,} plays loaded. Ref date: {ref_date.date()}")

    if args.artist:
        artist_lower = args.artist.lower()
        plays = [p for p in all_plays if p["artist"].lower() == artist_lower]
        print(f"  Filtered to '{args.artist}': {len(plays):,} plays")
    else:
        plays = all_plays

    if not plays:
        print("No plays found. Exiting.")
        sys.exit(1)

    # Update min plays config
    global MIN_PLAYS_TRAJECTORY
    MIN_PLAYS_TRAJECTORY = args.min_plays

    print("Detecting epochs ...")
    epochs = detect_epochs(all_plays)
    print(f"  {len(epochs)} epochs found.")

    print("Analyzing tracks ...")
    tracks = analyze_tracks(plays, all_plays, epochs, ref_date)
    print(f"  {len(tracks)} tracks analyzed.")

    pop_data = None
    if args.popularity:
        with open(args.popularity) as f:
            pop_data = json.load(f)
        print(f"  Loaded {len(pop_data)} popularity entries.")

    out = build_output(plays, all_plays, args.artist, epochs, tracks, ref_date, pop_data)
    print_summary(out)

    if args.out:
        with open(args.out, "w") as f:
            json.dump(out, f, indent=2, default=str)
        print(f"Output written to {args.out}")


if __name__ == "__main__":
    main()
