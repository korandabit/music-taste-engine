"""
engine.py
─────────
Unified music analysis engine. Reads from music.db (built by consolidate.py).

Subcommands:
  signals   Compute Spotify behavioral signals -> writes spotify_signals table to music.db
  analyze   Full per-track analysis: trajectory, LTP, playlist scoring, correlations

Usage:
  python engine.py signals --db data/music.db --input data/StreamingHistory*.json
  python engine.py analyze --db data/music.db --out analysis.json
  python engine.py analyze --db data/music.db --artist "Radiohead" --out radiohead.json
  python engine.py analyze --db data/music.db --months 3,4,5 --n 50 --out spring.json
  python engine.py analyze --db data/music.db --gap-days 180 --epoch-min-plays 30 --min-plays 5
"""

import argparse
import glob as _glob
import json
import math
import sqlite3
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

# Ensure UTF-8 output on Windows where the default console encoding may be cp1252
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


# ── Config / constants ────────────────────────────────────────────────────────

LATE_NIGHT_HOURS = {22, 23, 0, 1, 2, 3}
SEASON_MAP = {
    1: "Winter", 2: "Winter", 3: "Spring", 4: "Spring",  5: "Spring",
    6: "Summer", 7: "Summer", 8: "Summer", 9: "Fall",   10: "Fall",
    11: "Fall",  12: "Winter",
}

# Trajectory classification thresholds
FLASH_BINGE_30D        = 0.50
DISCOVERY_HEAVY_90D    = 0.60
BURN_Q1_THRESHOLD      = 0.65
PERENNIAL_Q4_THRESHOLD = 0.15
PERENNIAL_MIN_RETURNS  = 4

# Playlist scoring weights
W_RETURNS = 0.35
W_SEASON  = 0.30
W_DEPTH   = 0.20
W_REST    = 0.15
REST_CAP  = 600  # days

# Signals defaults (signals subcommand)
_SIG_DATE_FMT    = "%Y-%m-%d %H:%M"
_SIG_SESSION_GAP = 30   # minutes
_SIG_SKIP_RATIO  = 0.40
_SIG_MIN_PLAYS   = 2


# ── DB helpers ────────────────────────────────────────────────────────────────

def open_db(path: str) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


def table_exists(con: sqlite3.Connection, name: str) -> bool:
    return con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone() is not None


# ── Layer 1: Load from DB ─────────────────────────────────────────────────────

def load_plays(con: sqlite3.Connection, artist: str | None = None) -> list[dict]:
    """Load plays from music.db. Returns list of dicts with ts as datetime, sorted by ts."""
    if artist:
        rows = con.execute(
            "SELECT source, ts, artist, album, track FROM plays WHERE LOWER(artist)=LOWER(?)",
            (artist,),
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT source, ts, artist, album, track FROM plays"
        ).fetchall()

    plays = []
    for r in rows:
        try:
            ts = datetime.strptime(r["ts"], "%Y-%m-%d %H:%M")
        except ValueError:
            continue
        plays.append({
            "source": r["source"],
            "ts":     ts,
            "artist": r["artist"] or "",
            "album":  r["album"] or "",
            "track":  r["track"] or "",
        })
    plays.sort(key=lambda p: p["ts"])
    return plays


def load_spotify_signals(con: sqlite3.Connection) -> dict:
    """Returns {(artist_lower, track_lower): row_dict}. Empty dict if table absent."""
    if not table_exists(con, "spotify_signals"):
        return {}
    return {
        (r["artist"].lower(), r["track"].lower()): dict(r)
        for r in con.execute("SELECT * FROM spotify_signals").fetchall()
    }


# ── Layer 2: Per-record derived fields ───────────────────────────────────────

def enrich(plays: list[dict], ref_date: datetime, target_months: set | None) -> list[dict]:
    for p in plays:
        ts = p["ts"]
        p["days_ago"]      = (ref_date - ts).days
        p["hour"]          = ts.hour
        p["month"]         = ts.month
        p["year"]          = ts.year
        p["season"]        = SEASON_MAP[ts.month]
        p["is_late_night"] = ts.hour in LATE_NIGHT_HOURS
        p["is_target"]     = (ts.month in target_months) if target_months else None
    return plays


# ── Layer 3: First aggregations ───────────────────────────────────────────────

def group_plays_by_track(plays: list[dict]) -> dict:
    """
    Returns {(artist, track): {"timestamps": [...sorted], "album": str}}.
    Primary album = most common album for that (artist, track) pair.
    """
    buckets: dict[tuple, list] = defaultdict(list)
    for p in plays:
        buckets[(p["artist"], p["track"])].append(p)

    result = {}
    for key, plist in buckets.items():
        timestamps = sorted(p["ts"] for p in plist)
        album_counts = Counter(p["album"] for p in plist if p["album"])
        primary_album = album_counts.most_common(1)[0][0] if album_counts else ""
        result[key] = {"timestamps": timestamps, "album": primary_album}
    return result


def count_by(plays: list[dict], key_fn) -> dict:
    counts: dict = defaultdict(int)
    for p in plays:
        counts[key_fn(p)] += 1
    return dict(sorted(counts.items(), key=lambda x: -x[1]))


# ── Layer 4: Simple aggregate metrics ────────────────────────────────────────

def temporal_overview(plays: list[dict], ref_date: datetime) -> dict:
    first    = min(p["ts"] for p in plays)
    span     = max((ref_date - first).days, 1)
    return {
        "first_scrobble":    first.strftime("%Y-%m-%d"),
        "last_scrobble":     ref_date.strftime("%Y-%m-%d"),
        "total_plays":       len(plays),
        "unique_artists":    len({p["artist"] for p in plays}),
        "unique_tracks":     len({(p["artist"], p["track"]) for p in plays}),
        "unique_albums":     len({(p["artist"], p["album"]) for p in plays}),
        "span_days":         span,
        "span_years":        round(span / 365.25, 1),
        "avg_plays_per_day": round(len(plays) / span, 1),
    }


def listening_clock(plays: list[dict]) -> dict:
    by_hour = count_by(plays, lambda p: p["hour"])
    peak    = max(by_hour, key=by_hour.get)
    late    = sum(1 for p in plays if p["is_late_night"])
    return {
        "plays_by_hour":    dict(sorted(by_hour.items())),
        "peak_hour":        peak,
        "peak_hour_plays":  by_hour[peak],
        "late_night_plays": late,
        "late_night_pct":   round(100 * late / len(plays), 1),
    }


def seasonal_distribution(plays: list[dict]) -> dict:
    by_season = count_by(plays, lambda p: p["season"])
    total     = len(plays)
    return {s: {"plays": c, "pct": round(100 * c / total, 1)} for s, c in by_season.items()}


def top_tracks(plays: list[dict], n: int = 20) -> list[dict]:
    counts = count_by(plays, lambda p: (p["artist"], p["track"]))
    return [{"artist": a, "track": t, "plays": c} for (a, t), c in list(counts.items())[:n]]


def top_albums(plays: list[dict], n: int = 20) -> list[dict]:
    counts = count_by(plays, lambda p: (p["artist"], p["album"]))
    return [{"artist": a, "album": b, "plays": c} for (a, b), c in list(counts.items())[:n]]


def top_artists(plays: list[dict], n: int = 20) -> list[dict]:
    counts = count_by(plays, lambda p: p["artist"])
    return [{"artist": a, "plays": c} for a, c in list(counts.items())[:n]]


def year_by_year(plays: list[dict]) -> dict:
    by_year: dict[int, list] = defaultdict(list)
    for p in plays:
        by_year[p["year"]].append(p)

    result = {}
    for yr in sorted(by_year):
        yr_plays      = by_year[yr]
        artist_counts = count_by(yr_plays, lambda p: p["artist"])
        top_artist    = next(iter(artist_counts.items()))
        total         = len(yr_plays)
        result[yr] = {
            "total_plays": total,
            "top_artist":  {"artist": top_artist[0], "plays": top_artist[1]},
            "obsessions":  [
                {"artist": a, "plays": c, "pct": round(100 * c / total, 1)}
                for a, c in artist_counts.items() if c / total > 0.10
            ],
            "top_10_artists": [
                {"artist": a, "plays": c} for a, c in list(artist_counts.items())[:10]
            ],
        }
    return result


# ── Layer 5: Gap analysis ─────────────────────────────────────────────────────

def compute_gaps(timestamps: list[datetime], gap_days: int = 180) -> dict:
    """Gap statistics for a sorted timestamp list. All values None/0 if < 2 plays."""
    if len(timestamps) < 2:
        return {"gaps": [], "mean": None, "median": None, "std": None,
                "skew": None, "pct_long": 0.0, "max_gap": 0}

    gaps = [(timestamps[i] - timestamps[i - 1]).days for i in range(1, len(timestamps))]
    n    = len(gaps)
    mean = statistics.mean(gaps)
    med  = statistics.median(gaps)
    std  = statistics.stdev(gaps) if n > 1 else 0.0
    skew = round((3 * (mean - med) / std), 3) if std > 0 else 0.0

    return {
        "gaps":     gaps,
        "mean":     round(mean, 1),
        "median":   round(med, 1),
        "std":      round(std, 1),
        "skew":     skew,
        "pct_long": round(sum(1 for g in gaps if g >= gap_days) / n, 4),
        "max_gap":  max(gaps),
    }


def chunk_segment(timestamps: list[datetime], tau: int) -> list[list[datetime]]:
    """Split timestamps into chunks where consecutive gap >= tau days."""
    if not timestamps:
        return []
    chunks: list[list[datetime]] = [[timestamps[0]]]
    for ts in timestamps[1:]:
        if (ts - chunks[-1][-1]).days >= tau:
            chunks.append([])
        chunks[-1].append(ts)
    return chunks


# ── Layer 6: Temporal distribution metrics ────────────────────────────────────

def compute_temporal(
    timestamps: list[datetime],
    ref_date: datetime,
    target_months: set | None = None,
) -> dict:
    """Burst ratios, quartile distribution, session fingerprint, target-season ratio."""
    n     = len(timestamps)
    first = timestamps[0]
    last  = timestamps[-1]
    span  = max((last - first).days, 1)

    burst_30 = sum(1 for d in timestamps if (d - first).days <= 30)
    burst_90 = sum(1 for d in timestamps if (d - first).days <= 90)

    q1_end   = first + timedelta(days=span * 0.25)
    q4_start = first + timedelta(days=span * 0.75)
    q1 = round(sum(1 for d in timestamps if d <= q1_end) / n, 4)
    q4 = round(sum(1 for d in timestamps if d >= q4_start) / n, 4)

    distinct_days = len(set(d.date() for d in timestamps))
    ppd           = round(n / distinct_days, 2) if distinct_days else 0

    hour_groups  = Counter((d.date(), d.hour) for d in timestamps)
    repeat_plays = sum(c - 1 for c in hour_groups.values() if c > 1)
    repeat_rate  = round(repeat_plays / n, 4)

    late_night_pct = round(sum(1 for d in timestamps if d.hour in LATE_NIGHT_HOURS) / n, 4)

    hour_counts = Counter(d.hour for d in timestamps)
    peak_hour   = hour_counts.most_common(1)[0][0]

    target_season_ratio = None
    if target_months:
        target_plays = sum(1 for d in timestamps if d.month in target_months)
        target_season_ratio = round(target_plays / n, 4)

    return {
        "burst_ratio_30":       round(burst_30 / n, 4),
        "burst_ratio_90":       round(burst_90 / n, 4),
        "q1":                   q1,
        "q4":                   q4,
        "peak_hour":            peak_hour,
        "late_night_pct":       late_night_pct,
        "repeat_rate":          repeat_rate,
        "distinct_days":        distinct_days,
        "plays_per_active_day": ppd,
        "target_season_ratio":  target_season_ratio,
    }


# ── Layer 7: Epoch detection ──────────────────────────────────────────────────

def detect_epochs(all_plays: list[dict], epoch_min_plays: int = 30) -> list[dict]:
    """
    Contiguous months with >= epoch_min_plays plays -> epoch.
    Requires >= 2 consecutive qualifying months to form an epoch.
    """
    monthly = Counter((p["ts"].year, p["ts"].month) for p in all_plays)
    current: dict = {"months": [], "total": 0}
    raw_epochs: list[dict] = []

    for ym in sorted(monthly.keys()):
        if monthly[ym] >= epoch_min_plays:
            current["months"].append(ym)
            current["total"] += monthly[ym]
        else:
            if len(current["months"]) >= 2:
                raw_epochs.append(current)
            current = {"months": [], "total": 0}

    if len(current["months"]) >= 2:
        raw_epochs.append(current)

    return [
        {
            "name":        f"E{i + 1}",
            "start":       f"{e['months'][0][0]}-{e['months'][0][1]:02d}",
            "end":         f"{e['months'][-1][0]}-{e['months'][-1][1]:02d}",
            "_start_ym":   e["months"][0],
            "_end_ym":     e["months"][-1],
            "months":      len(e["months"]),
            "total_plays": e["total"],
        }
        for i, e in enumerate(raw_epochs)
    ]


def compute_epoch_rates(
    timestamps: list[datetime],
    epochs: list[dict],
    all_plays_monthly: Counter,
) -> dict:
    """Per-epoch play rate (plays per 1000 total corpus plays) for a single track."""
    rates = {}
    for e in epochs:
        s, en    = e["_start_ym"], e["_end_ym"]
        ep_total = sum(c for ym, c in all_plays_monthly.items() if s <= ym <= en)
        ep_track = sum(1 for d in timestamps if s <= (d.year, d.month) <= en)
        rates[e["name"]] = {
            "plays":          ep_track,
            "rate_per_1000":  round(ep_track / ep_total * 1000, 2) if ep_total else 0,
        }
    return rates


# ── Layer 8: Trajectory classification ───────────────────────────────────────

def classify_trajectory(
    burst_ratio_30: float,
    burst_ratio_90: float,
    q1: float,
    q4: float,
    long_returns: int,
    n_rediscoveries: int,
) -> str:
    """Pure function. Priority: FLASH_BINGE->DISCOVERY_HEAVY->FRONT_LOADED->PERENNIAL_RETURN->SLOW_BURN->REDISCOVERY->DIFFUSE."""
    if burst_ratio_30 >= FLASH_BINGE_30D:
        return "FLASH_BINGE"
    if burst_ratio_90 >= DISCOVERY_HEAVY_90D:
        return "DISCOVERY_HEAVY"
    if q1 >= BURN_Q1_THRESHOLD:
        return "FRONT_LOADED"
    if n_rediscoveries >= 3 and q4 >= PERENNIAL_Q4_THRESHOLD:
        return "PERENNIAL_RETURN"
    if q4 >= q1 * 0.8 and n_rediscoveries >= 2:
        return "SLOW_BURN"
    if n_rediscoveries >= 2:
        return "REDISCOVERY"
    return "DIFFUSE"


# ── Layer 9: LTP qualification + playlist scoring ─────────────────────────────

def qualify_ltp(
    timestamps: list[datetime],
    gap_stats: dict,
    temporal: dict,
    ref_date: datetime,
    gap_days: int,
    min_plays: int,
    min_returns: int,
) -> dict | None:
    """Returns LTP sub-dict if track qualifies, else None."""
    if len(timestamps) < min_plays:
        return None
    long_returns = sum(1 for g in gap_stats["gaps"] if g >= gap_days)
    if long_returns < min_returns:
        return None
    return {
        "long_returns":        long_returns,
        "max_gap_days":        gap_stats["max_gap"],
        "days_since":          (ref_date - timestamps[-1]).days,
        "target_season_ratio": temporal["target_season_ratio"],
        "lifespan_days":       (timestamps[-1] - timestamps[0]).days,
    }


def _build_playlist(
    tracks: list[dict],
    n: int,
    target_months: set | None,
    rest_min_days: int,
    season_ratio_min: float,
    max_per_artist: int,
) -> list[dict]:
    """Score and select top-N LTP tracks for playlist output."""
    pool = [
        t for t in tracks
        if t["ltp"] is not None
        and t["ltp"]["days_since"] >= rest_min_days
        and (not target_months or (t["ltp"]["target_season_ratio"] or 0) >= season_ratio_min)
    ]
    if not pool:
        return []

    max_returns   = max(t["ltp"]["long_returns"] for t in pool)
    max_log_plays = max(math.log1p(t["total_plays"]) for t in pool)

    if max_returns == 0 or max_log_plays == 0:
        return []

    def score(t: dict) -> float:
        ltp = t["ltp"]
        s_returns = ltp["long_returns"] / max_returns
        s_depth   = math.log1p(t["total_plays"]) / max_log_plays
        s_rest    = min(ltp["days_since"], REST_CAP) / REST_CAP
        if target_months:
            s_season = ltp["target_season_ratio"] or 0
            return W_RETURNS * s_returns + W_SEASON * s_season + W_DEPTH * s_depth + W_REST * s_rest
        else:
            total_w = W_RETURNS + W_DEPTH + W_REST
            return (W_RETURNS / total_w) * s_returns + (W_DEPTH / total_w) * s_depth + (W_REST / total_w) * s_rest

    pool.sort(key=lambda t: -score(t))

    selected: list[dict] = []
    artist_counts: dict[str, int] = defaultdict(int)
    for t in pool:
        if artist_counts[t["artist"]] >= max_per_artist:
            continue
        entry = {
            "artist":     t["artist"],
            "track":      t["track"],
            "score":      round(score(t), 4),
            "long_returns": t["ltp"]["long_returns"],
            "days_since": t["ltp"]["days_since"],
        }
        if target_months:
            entry["target_season_ratio"] = t["ltp"]["target_season_ratio"]
        selected.append(entry)
        artist_counts[t["artist"]] += 1
        if len(selected) >= n:
            break

    return selected


# ── Layer 10: Cross-track analysis ────────────────────────────────────────────

def _pearson_r(x: list[float], y: list[float]) -> float:
    n = len(x)
    if n < 3:
        return 0.0
    mx, my = sum(x) / n, sum(y) / n
    sx = math.sqrt(sum((xi - mx) ** 2 for xi in x) / n)
    sy = math.sqrt(sum((yi - my) ** 2 for yi in y) / n)
    if sx == 0 or sy == 0:
        return 0.0
    return sum((xi - mx) * (yi - my) for xi, yi in zip(x, y)) / (n * sx * sy)


def compute_correlations(tracks: list[dict], min_plays: int = 20) -> dict:
    eligible = [t for t in tracks if t["total_plays"] >= min_plays]
    if len(eligible) < 5:
        return {}

    bursts  = [t["burst_ratio_30"] for t in eligible]
    totals  = [t["total_plays"]    for t in eligible]
    spans   = [t["span_days"]      for t in eligible]
    returns = [t["long_returns"]   for t in eligible]

    result: dict = {
        "n":                  len(eligible),
        "min_plays":          min_plays,
        "burst30_vs_total":   round(_pearson_r(bursts, totals), 3),
        "burst30_vs_span":    round(_pearson_r(bursts, spans), 3),
        "burst30_vs_returns": round(_pearson_r(bursts, returns), 3),
    }

    skew_eligible = [t for t in eligible if t["gap_skew"] is not None]
    if len(skew_eligible) >= 5:
        skews    = [t["gap_skew"]   for t in skew_eligible]
        spans2   = [t["span_days"]  for t in skew_eligible]
        returns2 = [t["long_returns"] for t in skew_eligible]
        result["gapskew_vs_span"]    = round(_pearson_r(skews, spans2), 3)
        result["gapskew_vs_returns"] = round(_pearson_r(skews, returns2), 3)

    return result


def compute_discovery_latency(tracks: list[dict], plays: list[dict]) -> list[dict]:
    """Tracks whose first listen came >60 days after their album's first appearance."""
    album_first: dict[str, datetime] = {}
    for p in plays:
        a = p["album"]
        if a and (a not in album_first or p["ts"] < album_first[a]):
            album_first[a] = p["ts"]

    results = []
    for t in tracks:
        alb = t.get("album", "")
        if alb and alb in album_first:
            first_track = datetime.strptime(t["first_play"], "%Y-%m-%d")
            delay = (first_track - album_first[alb]).days
            if delay > 60:
                results.append({
                    "artist":           t["artist"],
                    "track":            t["track"],
                    "album":            alb,
                    "album_first_play": album_first[alb].strftime("%Y-%m-%d"),
                    "track_first_play": t["first_play"],
                    "delay_days":       delay,
                    "trajectory":       t["trajectory"],
                    "q4":               t["q4"],
                })
    results.sort(key=lambda x: -x["delay_days"])
    return results


def compute_trajectory_stats(tracks: list[dict]) -> dict:
    stats = {}
    for traj in set(t["trajectory"] for t in tracks):
        subset = [t for t in tracks if t["trajectory"] == traj]
        stats[traj] = {
            "count":           len(subset),
            "avg_plays":       round(statistics.mean(t["total_plays"] for t in subset), 1),
            "avg_span_days":   round(statistics.mean(t["span_days"] for t in subset), 0),
            "avg_q1":          round(statistics.mean(t["q1"] for t in subset), 3),
            "avg_q4":          round(statistics.mean(t["q4"] for t in subset), 3),
            "avg_returns":     round(statistics.mean(t["long_returns"] for t in subset), 1),
            "avg_ppd":         round(statistics.mean(t["session"]["plays_per_active_day"] for t in subset), 2),
            "avg_repeat_rate": round(statistics.mean(t["session"]["repeat_rate"] for t in subset), 3),
            "avg_late_night":  round(statistics.mean(t["session"]["late_night_pct"] for t in subset), 3),
        }
    return stats


# ── Layer 11: Output assembly + analyze subcommand ───────────────────────────

def cmd_analyze(args: argparse.Namespace) -> dict:
    ref_date      = datetime.strptime(args.refdate, "%Y-%m-%d") if args.refdate else datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    target_months = set(int(m) for m in args.months.split(",")) if args.months else None
    gap_days      = args.gap_days
    min_plays     = args.min_plays
    min_returns   = args.min_returns

    print(f"Opening {args.db} ...")
    con          = open_db(args.db)
    spotify_sigs = load_spotify_signals(con)
    has_spotify  = bool(spotify_sigs)

    print("Loading plays ...")
    all_plays = load_plays(con)
    if args.artist:
        artist_lower = args.artist.lower()
        filtered = [p for p in all_plays if p["artist"].lower() == artist_lower]
        print(f"  {len(all_plays):,} total plays -> {len(filtered):,} for '{args.artist}'")
    else:
        filtered = all_plays

    if not filtered:
        print("No plays found.")
        con.close()
        return {}

    print(f"  ref_date={ref_date.date()}  gap_days={gap_days}  target_months={sorted(target_months) if target_months else None}")

    enrich(filtered, ref_date, target_months)

    # Layer 3 + 4: aggregations
    track_groups      = group_plays_by_track(filtered)
    all_plays_monthly = Counter((p["ts"].year, p["ts"].month) for p in all_plays)

    print("Computing overview ...")
    overview  = temporal_overview(filtered, ref_date)
    clock     = listening_clock(filtered)
    seasonal  = seasonal_distribution(filtered)
    t_tracks  = top_tracks(filtered, 25)
    t_albums  = top_albums(filtered, 25)
    t_artists = top_artists(filtered, 25)
    yby       = year_by_year(filtered)

    top_target = None
    if target_months:
        target_plays = [p for p in filtered if p["is_target"]]
        top_target = top_tracks(target_plays, 25) if target_plays else []

    # Layer 7: epochs (always from full corpus)
    print("Detecting epochs ...")
    epochs = detect_epochs(all_plays, args.epoch_min_plays)
    print(f"  {len(epochs)} epochs")

    # Per-track analysis
    print(f"Analyzing {len(track_groups):,} tracks ...")
    tracks:     list[dict] = []
    ltp_tracks: list[dict] = []

    for (artist, track), info in track_groups.items():
        timestamps = info["timestamps"]
        album      = info["album"]
        n          = len(timestamps)

        if n < min_plays:
            continue
        if (timestamps[-1] - timestamps[0]).days < 1:
            continue

        # Layer 5
        gap_stats    = compute_gaps(timestamps, gap_days)
        long_returns = sum(1 for g in gap_stats["gaps"] if g >= gap_days)

        rediscoveries = []
        for i, g in enumerate(gap_stats["gaps"]):
            if g >= gap_days:
                ret_date = timestamps[i + 1]
                cluster  = sum(1 for d in timestamps[i + 1:] if (d - ret_date).days <= 30)
                rediscoveries.append({
                    "gap_days":    g,
                    "return_date": ret_date.strftime("%Y-%m-%d"),
                    "return_year": ret_date.year,
                    "cluster_size": cluster,
                })

        # Layer 6
        temporal = compute_temporal(timestamps, ref_date, target_months)

        # Layer 7 (epoch rates)
        epoch_rates = compute_epoch_rates(timestamps, epochs, all_plays_monthly)

        # Layer 8
        trajectory = classify_trajectory(
            temporal["burst_ratio_30"], temporal["burst_ratio_90"],
            temporal["q1"], temporal["q4"],
            long_returns, len(rediscoveries),
        )

        # Layer 9 (LTP)
        ltp = qualify_ltp(timestamps, gap_stats, temporal, ref_date, gap_days, min_plays, min_returns)

        # Spotify enrichment
        sp_row = spotify_sigs.get((artist.lower(), track.lower()))
        spotify = {
            "skip_rate":              sp_row.get("skip_rate")              if sp_row else None,
            "completion_mean_ratio":  sp_row.get("completion_mean_ratio")  if sp_row else None,
            "opener_rate":            sp_row.get("opener_rate")            if sp_row else None,
            "within_session_repeats": sp_row.get("within_session_repeats") if sp_row else None,
        }

        rec = {
            "artist":        artist,
            "track":         track,
            "album":         album,
            "total_plays":   n,
            "span_days":     (timestamps[-1] - timestamps[0]).days,
            "first_play":    timestamps[0].strftime("%Y-%m-%d"),
            "last_play":     timestamps[-1].strftime("%Y-%m-%d"),
            "days_since":    (ref_date - timestamps[-1]).days,
            "burst_ratio_30": temporal["burst_ratio_30"],
            "burst_ratio_90": temporal["burst_ratio_90"],
            "q1":            temporal["q1"],
            "q4":            temporal["q4"],
            "long_returns":  long_returns,
            "gap_skew":      gap_stats["skew"],
            "rediscoveries": rediscoveries,
            "trajectory":    trajectory,
            "session": {
                "distinct_days":        temporal["distinct_days"],
                "plays_per_active_day": temporal["plays_per_active_day"],
                "repeat_rate":          temporal["repeat_rate"],
                "late_night_pct":       temporal["late_night_pct"],
            },
            "epoch_rates": epoch_rates,
            "ltp":         ltp,
            "spotify":     spotify,
        }
        tracks.append(rec)
        if ltp:
            ltp_tracks.append({"artist": artist, "track": track, "album": album, **ltp})

    tracks.sort(key=lambda t: -t["total_plays"])

    # Layer 10
    print("Cross-track analysis ...")
    trajectory_summary = dict(Counter(t["trajectory"] for t in tracks))
    trajectory_stats   = compute_trajectory_stats(tracks) if tracks else {}
    correlations       = compute_correlations(tracks)
    discovery_latency  = compute_discovery_latency(tracks, filtered)

    # Playlist
    playlist = None
    if args.n:
        playlist = _build_playlist(
            tracks, args.n, target_months,
            args.rest_min_days, args.season_ratio_min, args.max_per_artist,
        )
        print(f"  Playlist: {len(playlist)} tracks selected (from {len(ltp_tracks)} LTP qualifying)")

    # Strip internal-only fields from epoch output
    epochs_out = [{k: v for k, v in e.items() if not k.startswith("_")} for e in epochs]

    output = {
        "meta": {
            "db":            args.db,
            "ref_date":      ref_date.strftime("%Y-%m-%d"),
            "artist_filter": args.artist,
            "months_filter": sorted(target_months) if target_months else None,
            "has_spotify":   has_spotify,
            "config": {
                "gap_days":        gap_days,
                "min_plays":       min_plays,
                "min_returns":     min_returns,
                "epoch_min_plays": args.epoch_min_plays,
            },
        },
        "overview":            overview,
        "clock":               clock,
        "seasonal":            seasonal,
        "top_tracks":          t_tracks,
        "top_albums":          t_albums,
        "top_artists":         t_artists,
        "top_target_tracks":   top_target,
        "year_by_year":        yby,
        "epochs":              epochs_out,
        "tracks":              tracks,
        "trajectory_summary":  trajectory_summary,
        "trajectory_type_stats": trajectory_stats,
        "correlations":        correlations,
        "discovery_latency":   discovery_latency,
        "ltp_tracks":          ltp_tracks,
        "playlist":            playlist,
    }

    con.close()

    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            json.dump(output, fh, indent=2, default=str)
        print(f"Output: {args.out}")

    _print_analyze_summary(output)
    return output


def _print_analyze_summary(out: dict) -> None:
    o   = out["overview"]
    m   = out["meta"]
    print(f"""
--- OVERVIEW -----------------------------------------------
  {o['total_plays']:,} plays · {o['unique_artists']:,} artists · {o['unique_tracks']:,} tracks
  {o['span_years']} years  ({o['first_scrobble']} -> {o['last_scrobble']})
  avg {o['avg_plays_per_day']} plays/day  |  ref_date: {m['ref_date']}
  Spotify signals: {'yes' if m['has_spotify'] else 'no'}""")

    c = out["clock"]
    print(f"  Late night (10pm–4am): {c['late_night_pct']}%  |  Peak hour: {c['peak_hour']}:00")

    print("\n--- EPOCHS ------------------------------------------------")
    for e in out["epochs"]:
        print(f"  {e['name']}: {e['start']} -> {e['end']}  {e['months']}mo  {e['total_plays']:,} plays")

    print("\n--- TRAJECTORY DISTRIBUTION --------------------------------")
    for traj, cnt in sorted(out["trajectory_summary"].items(), key=lambda x: -x[1]):
        st = out["trajectory_type_stats"].get(traj, {})
        print(f"  {traj:<22} n={cnt:>4}  avg_plays={st.get('avg_plays', 0):>5.1f}  "
              f"avg_span={st.get('avg_span_days', 0):>5.0f}d  "
              f"q1={st.get('avg_q1', 0):.2f}  q4={st.get('avg_q4', 0):.2f}")

    if out["correlations"]:
        c2 = out["correlations"]
        print(f"\n--- BINGE->OUTCOME (n={c2['n']}, ≥{c2['min_plays']} plays) ----------------")
        print(f"  burst_30 -> total_plays:  r = {c2['burst30_vs_total']:+.3f}")
        print(f"  burst_30 -> lifespan:     r = {c2['burst30_vs_span']:+.3f}")
        print(f"  burst_30 -> long_returns: r = {c2['burst30_vs_returns']:+.3f}")

    print(f"\n--- LTP: {len(out['ltp_tracks'])} qualifying tracks -----------------------------")
    for r in out["ltp_tracks"][:10]:
        print(f"  {r['artist']} — {r['track']}")
        tsr = r['target_season_ratio']
        tsr_str = f"  {int(tsr*100)}% target" if tsr is not None else ""
        print(f"       {r['long_returns']} returns · max gap {r['max_gap_days']}d · {r['days_since']}d rest{tsr_str}")

    if out.get("playlist"):
        pl = out["playlist"]
        print(f"\n--- PLAYLIST ({len(pl)} tracks) ----------------------------------")
        for i, r in enumerate(pl, 1):
            tsr_str = f"  {int((r.get('target_season_ratio') or 0)*100)}% target" if "target_season_ratio" in r else ""
            print(f"  {i:>2}. {r['artist']} — {r['track']}")
            print(f"       score {r['score']} · {r['long_returns']} returns · {r['days_since']}d rest{tsr_str}")
    print()


# ── Signals subcommand (ported from spotify_signal_engine.py) ─────────────────

def _sig_load_history(paths: list[str]) -> list[dict]:
    plays = []
    for path in paths:
        with open(path, encoding="utf-8") as fh:
            records = json.load(fh)
        for r in records:
            raw = r.get("endTime", "")
            try:
                ts = datetime.strptime(raw.strip(), _SIG_DATE_FMT)
            except ValueError:
                continue
            plays.append({
                "ts":     ts,
                "artist": r.get("artistName", "").strip(),
                "track":  r.get("trackName", "").strip(),
                "ms":     r.get("msPlayed", 0),
            })
    plays.sort(key=lambda p: p["ts"])
    return plays


def _sig_tag_sessions(plays: list[dict], gap_minutes: int) -> list[dict]:
    if not plays:
        return plays
    gap = timedelta(minutes=gap_minutes)
    session_id    = 0
    session_start = 0
    plays[0]["session_id"] = 0

    for i in range(1, len(plays)):
        if plays[i]["ts"] - plays[i - 1]["ts"] > gap:
            size = i - session_start
            for j in range(session_start, i):
                plays[j]["session_size"] = size
                if size == 1 or j == session_start:
                    plays[j]["session_position"] = "opener"
                elif j == i - 1:
                    plays[j]["session_position"] = "closer"
                else:
                    plays[j]["session_position"] = "mid"
            session_id   += 1
            session_start = i
        plays[i]["session_id"] = session_id

    size = len(plays) - session_start
    for j in range(session_start, len(plays)):
        plays[j]["session_size"] = size
        if size == 1 or j == session_start:
            plays[j]["session_position"] = "opener"
        elif j == len(plays) - 1:
            plays[j]["session_position"] = "closer"
        else:
            plays[j]["session_position"] = "mid"
    return plays


def _sig_tag_completion(plays: list[dict], durations: dict | None) -> tuple[list[dict], str]:
    if durations:
        for p in plays:
            key = f"{p['artist']}|{p['track']}"
            dur = durations.get(key)
            if dur and dur > 0:
                ratio = min(p["ms"] / dur, 1.0)
                p["completion_ratio"] = round(ratio, 4)
                p["is_skip"] = 1 if ratio < _SIG_SKIP_RATIO else 0
            else:
                p["completion_ratio"] = None
                p["is_skip"] = None
        return plays, "absolute"

    track_ms = defaultdict(list)
    for p in plays:
        track_ms[(p["artist"], p["track"])].append(p["ms"])
    track_median = {k: statistics.median(v) for k, v in track_ms.items() if len(v) >= 2}

    for p in plays:
        median = track_median.get((p["artist"], p["track"]))
        if median is None or median == 0:
            p["completion_ratio"] = None
            p["is_skip"] = None
        else:
            ratio = min(p["ms"] / median, 1.0)
            p["completion_ratio"] = round(ratio, 4)
            p["is_skip"] = 1 if ratio < _SIG_SKIP_RATIO else 0
    return plays, "relative"


def _sig_aggregate(plays: list[dict], min_plays: int, refdate: datetime) -> list[dict]:
    by_track: dict = defaultdict(list)
    for p in plays:
        by_track[(p["artist"], p["track"])].append(p)

    results = []
    for (artist, track), tp in by_track.items():
        n = len(tp)
        if n < min_plays:
            continue
        tp_sorted = sorted(tp, key=lambda p: p["ts"])
        first_ts  = tp_sorted[0]["ts"]
        last_ts   = tp_sorted[-1]["ts"]
        span_days = (last_ts - first_ts).days

        total_ms     = sum(p["ms"] for p in tp)
        ratios       = [p["completion_ratio"] for p in tp if p.get("completion_ratio") is not None]
        non_null     = [p for p in tp if p.get("is_skip") is not None]
        skip_count   = sum(1 for p in tp if p.get("is_skip") == 1)
        skip_rate    = round(skip_count / len(non_null), 4) if non_null else None
        mean_ratio   = round(statistics.mean(ratios), 4) if ratios else None
        full_listens = sum(1 for r in ratios if r >= 0.90)
        full_listen_rate = round(full_listens / len(ratios), 4) if ratios else None

        openers = sum(1 for p in tp if p.get("session_position") == "opener")
        closers = sum(1 for p in tp if p.get("session_position") == "closer")
        mids    = sum(1 for p in tp if p.get("session_position") == "mid")
        opener_rate = round(openers / n, 4)

        session_counts: dict = defaultdict(int)
        for p in tp:
            session_counts[p["session_id"]] += 1
        sessions_with_repeat   = sum(1 for c in session_counts.values() if c > 1)
        within_session_repeats = sum(max(0, c - 1) for c in session_counts.values())
        max_repeats            = max(session_counts.values()) - 1

        hours      = [p["ts"].hour for p in tp]
        hour_dist: dict = defaultdict(int)
        for h in hours:
            hour_dist[str(h)] += 1
        peak_hour      = max(hour_dist, key=hour_dist.__getitem__)
        late_night     = sum(1 for h in hours if h in LATE_NIGHT_HOURS)
        late_night_pct = round(late_night / n, 4)

        cutoff_first = first_ts + timedelta(days=30)
        cutoff_last  = refdate - timedelta(days=30)
        plays_first_30d = sum(1 for p in tp if p["ts"] <= cutoff_first)
        plays_last_30d  = sum(1 for p in tp if p["ts"] >= cutoff_last)
        burst_ratio_30  = round(plays_first_30d / n, 4)

        results.append({
            "artist":               artist,
            "track":                track,
            "total_plays":          n,
            "total_ms_played":      total_ms,
            "first_play":           first_ts.strftime("%Y-%m-%d"),
            "last_play":            last_ts.strftime("%Y-%m-%d"),
            "span_days":            span_days,
            "completion_mean_ratio":  mean_ratio,
            "skip_count":             skip_count,
            "skip_rate":              skip_rate,
            "full_listen_rate":       full_listen_rate,
            "opener_count":           openers,
            "closer_count":           closers,
            "mid_count":              mids,
            "opener_rate":            opener_rate,
            "within_session_repeats": within_session_repeats,
            "same_session_repeats":   within_session_repeats,
            "max_repeats_in_session": max_repeats,
            "sessions_with_repeat":   sessions_with_repeat,
            "peak_hour":              int(peak_hour),
            "late_night_pct":         late_night_pct,
            "hour_distribution":      json.dumps(dict(sorted(hour_dist.items(), key=lambda x: int(x[0])))),
            "plays_first_30d":        plays_first_30d,
            "plays_last_30d":         plays_last_30d,
            "burst_ratio_30":         burst_ratio_30,
        })
    return results


_SIGNALS_SCHEMA = """
CREATE TABLE IF NOT EXISTS spotify_signals (
    id                    INTEGER PRIMARY KEY,
    artist                TEXT,
    track                 TEXT,
    total_plays           INTEGER,
    total_ms_played       INTEGER,
    first_play            TEXT,
    last_play             TEXT,
    span_days             INTEGER,
    completion_mean_ratio REAL,
    completion_source     TEXT,
    skip_count            INTEGER,
    skip_rate             REAL,
    full_listen_rate      REAL,
    opener_count          INTEGER,
    closer_count          INTEGER,
    mid_count             INTEGER,
    opener_rate           REAL,
    within_session_repeats   INTEGER,
    same_session_repeats     INTEGER,
    max_repeats_in_session   INTEGER,
    sessions_with_repeat     INTEGER,
    peak_hour             INTEGER,
    late_night_pct        REAL,
    hour_distribution     TEXT,
    plays_first_30d       INTEGER,
    plays_last_30d        INTEGER,
    burst_ratio_30        REAL
);
CREATE INDEX IF NOT EXISTS ss_artist ON spotify_signals(artist);
CREATE INDEX IF NOT EXISTS ss_track  ON spotify_signals(track);
"""


def cmd_signals(args: argparse.Namespace) -> None:
    # Expand globs (Windows shell may not expand)
    input_paths = []
    for pattern in args.input:
        expanded = _glob.glob(pattern)
        input_paths.extend(expanded if expanded else [pattern])

    refdate = (
        datetime.strptime(args.refdate, "%Y-%m-%d")
        if args.refdate else datetime.today()
    )
    durations = None
    if args.durations:
        with open(args.durations, encoding="utf-8") as fh:
            durations = json.load(fh)

    print(f"Loading {len(input_paths)} file(s) ...")
    plays = _sig_load_history(input_paths)
    print(f"  {len(plays):,} plays")
    if not plays:
        print("No plays found.")
        return

    date_range = (plays[0]["ts"].strftime("%Y-%m-%d"), plays[-1]["ts"].strftime("%Y-%m-%d"))

    print("Tagging sessions ...")
    plays = _sig_tag_sessions(plays, args.session_gap_minutes)
    n_sessions = max(p["session_id"] for p in plays) + 1
    print(f"  {n_sessions:,} sessions (gap={args.session_gap_minutes}min)")

    print("Computing completion ratios ...")
    plays, completion_source = _sig_tag_completion(plays, durations)
    print(f"  completion_source={completion_source}")

    print("Aggregating per track ...")
    rows = _sig_aggregate(plays, args.min_plays, refdate)
    print(f"  {len(rows):,} tracks (min_plays={args.min_plays})")

    print(f"Writing to {args.db} ...")
    con = sqlite3.connect(args.db)
    con.execute("DROP TABLE IF EXISTS spotify_signals")
    con.executescript(_SIGNALS_SCHEMA)
    for row in rows:
        row["completion_source"] = completion_source
    con.executemany("""
        INSERT INTO spotify_signals (
            artist, track, total_plays, total_ms_played,
            first_play, last_play, span_days,
            completion_mean_ratio, completion_source, skip_count, skip_rate, full_listen_rate,
            opener_count, closer_count, mid_count, opener_rate,
            within_session_repeats, same_session_repeats, max_repeats_in_session, sessions_with_repeat,
            peak_hour, late_night_pct, hour_distribution,
            plays_first_30d, plays_last_30d, burst_ratio_30
        ) VALUES (
            :artist, :track, :total_plays, :total_ms_played,
            :first_play, :last_play, :span_days,
            :completion_mean_ratio, :completion_source, :skip_count, :skip_rate, :full_listen_rate,
            :opener_count, :closer_count, :mid_count, :opener_rate,
            :within_session_repeats, :same_session_repeats, :max_repeats_in_session, :sessions_with_repeat,
            :peak_hour, :late_night_pct, :hour_distribution,
            :plays_first_30d, :plays_last_30d, :burst_ratio_30
        )
    """, rows)
    con.commit()
    con.close()
    print(f"Done.  date_range={date_range[0]} -> {date_range[1]}  sessions={n_sessions:,}  tracks={len(rows):,}  completion={completion_source}")


# ── CLI entry point ───────────────────────────────────────────────────────────

def main() -> None:
    root = argparse.ArgumentParser(prog="engine.py", description="Music data analysis engine")
    sub  = root.add_subparsers(dest="command", required=True)

    # ── signals ──
    p_sig = sub.add_parser("signals", help="Compute Spotify behavioral signals -> music.db")
    p_sig.add_argument("--db",                  default="data/music.db")
    p_sig.add_argument("--input",               nargs="+", required=True, help="StreamingHistory*.json file(s)")
    p_sig.add_argument("--durations",           default=None,  help="Optional Artist|Track->duration_ms JSON")
    p_sig.add_argument("--refdate",             default=None,  help="YYYY-MM-DD (default: today)")
    p_sig.add_argument("--min-plays",           type=int, default=_SIG_MIN_PLAYS)
    p_sig.add_argument("--session-gap-minutes", type=int, default=_SIG_SESSION_GAP)

    # ── analyze ──
    p_ana = sub.add_parser("analyze", help="Full per-track analysis")
    p_ana.add_argument("--db",              default="data/music.db")
    p_ana.add_argument("--out",             default=None,  help="Output JSON path")
    p_ana.add_argument("--artist",          default=None,  help="Filter to single artist (case-insensitive)")
    p_ana.add_argument("--months",          default=None,  help="Target months e.g. 3,4,5 for spring")
    p_ana.add_argument("--n",               type=int, default=None, help="Playlist size (enables playlist output)")
    p_ana.add_argument("--refdate",         default=None,  help="Reference date YYYY-MM-DD (default: today)")
    p_ana.add_argument("--gap-days",        type=int, default=180, help="LTP/rediscovery gap threshold (days)")
    p_ana.add_argument("--min-plays",       type=int, default=5,   help="Min plays for per-track analysis")
    p_ana.add_argument("--min-returns",     type=int, default=2,   help="Min long-gap returns for LTP")
    p_ana.add_argument("--epoch-min-plays", type=int, default=30,  help="Monthly plays threshold for epoch detection")
    p_ana.add_argument("--rest-min-days",   type=int, default=45,  help="Min days since last play for playlist")
    p_ana.add_argument("--season-ratio-min", type=float, default=0.30, help="Min target-season ratio for playlist")
    p_ana.add_argument("--max-per-artist",  type=int, default=4,   help="Max tracks per artist in playlist")

    args = root.parse_args()

    if args.command == "signals":
        cmd_signals(args)
    elif args.command == "analyze":
        cmd_analyze(args)


if __name__ == "__main__":
    main()
