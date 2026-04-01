"""
spotify_signal_engine.py
─────────────────────────
Upstream preprocessor for Spotify StreamingHistory exports.
Computes per-track behavioral primitives and writes them to music.db.

Signals computed (all unavailable from Last.fm):
  - Session position: opener_rate (unprompted preference signal)
  - Within-session repeats (compulsion signal)
  - Completion ratio via relative-median fallback (or absolute if durations supplied)
  - Skip rate (relative: play < 40% of track's own median msPlayed)
  - Temporal: peak hour, late-night %, hour distribution

Input:
  One or more StreamingHistory*.json files (Spotify export format)
  Schema per record: { endTime, artistName, trackName, msPlayed }

Output:
  Writes `spotify_signals` table to music.db (created/replaced on each run)

Usage:
  python spotify_signal_engine.py --input data/StreamingHistory*.json --db data/music.db
  python spotify_signal_engine.py --input data/StreamingHistory0.json --db data/music.db --min-plays 3
  python spotify_signal_engine.py --input data/StreamingHistory*.json --db data/music.db --durations data/track_durations.json
"""

import argparse
import glob
import json
import sqlite3
import statistics
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path


# ─── Config ───────────────────────────────────────────────────────────────────

DATE_FORMAT         = "%Y-%m-%d %H:%M"
LATE_NIGHT_HOURS    = {22, 23, 0, 1, 2, 3}
SESSION_GAP_MINUTES = 30       # default; overridable via CLI
MIN_PLAYS           = 2        # default; overridable via CLI
SKIP_RATIO          = 0.40     # play < 40% of track's median msPlayed = skip


# ─── Load ─────────────────────────────────────────────────────────────────────

def load_history(paths: list[str]) -> list[dict]:
    """Load and merge one or more StreamingHistory JSON files, sorted by endTime."""
    plays = []
    for path in paths:
        with open(path, encoding="utf-8") as fh:
            records = json.load(fh)
        for r in records:
            raw = r.get("endTime", "")
            try:
                ts = datetime.strptime(raw.strip(), DATE_FORMAT)
            except ValueError:
                continue
            ms = r.get("msPlayed", 0)
            plays.append({
                "ts":     ts,
                "artist": r.get("artistName", "").strip(),
                "track":  r.get("trackName", "").strip(),
                "ms":     ms,
            })
    plays.sort(key=lambda p: p["ts"])
    return plays


def load_durations(path: str) -> dict:
    """Optional: load { 'Artist|Track': duration_ms } map."""
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


# ─── Session reconstruction ────────────────────────────────────────────────────

def tag_sessions(plays: list[dict], gap_minutes: int) -> list[dict]:
    """
    Tag each play with session_id, session_position (opener/mid/closer),
    and session_size. Sessions are delimited by gaps > gap_minutes.
    """
    if not plays:
        return plays

    gap = timedelta(minutes=gap_minutes)
    session_id   = 0
    session_start = 0

    # First pass: assign session_id
    plays[0]["session_id"] = 0
    for i in range(1, len(plays)):
        if plays[i]["ts"] - plays[i - 1]["ts"] > gap:
            # Flush previous session
            size = i - session_start
            for j in range(session_start, i):
                plays[j]["session_size"] = size
                if size == 1:
                    plays[j]["session_position"] = "opener"
                elif j == session_start:
                    plays[j]["session_position"] = "opener"
                elif j == i - 1:
                    plays[j]["session_position"] = "closer"
                else:
                    plays[j]["session_position"] = "mid"
            session_id  += 1
            session_start = i
        plays[i]["session_id"] = session_id

    # Flush final session
    size = len(plays) - session_start
    for j in range(session_start, len(plays)):
        plays[j]["session_size"] = size
        if size == 1:
            plays[j]["session_position"] = "opener"
        elif j == session_start:
            plays[j]["session_position"] = "opener"
        elif j == len(plays) - 1:
            plays[j]["session_position"] = "closer"
        else:
            plays[j]["session_position"] = "mid"

    return plays


# ─── Completion ───────────────────────────────────────────────────────────────

def tag_completion(plays: list[dict], durations: dict | None) -> tuple[list[dict], str]:
    """
    Tags each play with completion_ratio and is_skip.

    If durations supplied: completion_ratio = ms / duration_ms (absolute).
    Otherwise: relative fallback — compute each track's median msPlayed,
    flag plays < SKIP_RATIO * median as skips. Tracks with only one play
    cannot be classified; is_skip = None for those.

    Returns (tagged plays, source_label).
    """
    if durations:
        for p in plays:
            key = f"{p['artist']}|{p['track']}"
            dur = durations.get(key)
            if dur and dur > 0:
                ratio = min(p["ms"] / dur, 1.0)
                p["completion_ratio"] = round(ratio, 4)
                p["is_skip"] = 1 if ratio < SKIP_RATIO else 0
            else:
                p["completion_ratio"] = None
                p["is_skip"] = None
        return plays, "absolute"

    # Relative fallback: median msPlayed per track
    track_ms = defaultdict(list)
    for p in plays:
        track_ms[(p["artist"], p["track"])].append(p["ms"])

    track_median = {
        k: statistics.median(v) for k, v in track_ms.items() if len(v) >= 2
    }

    for p in plays:
        key = (p["artist"], p["track"])
        median = track_median.get(key)
        if median is None or median == 0:
            p["completion_ratio"] = None
            p["is_skip"] = None
        else:
            ratio = min(p["ms"] / median, 1.0)
            p["completion_ratio"] = round(ratio, 4)
            p["is_skip"] = 1 if ratio < SKIP_RATIO else 0

    return plays, "relative"


# ─── Aggregation ──────────────────────────────────────────────────────────────

def aggregate(plays: list[dict], min_plays: int, refdate: datetime) -> list[dict]:
    """Aggregate per-play records into per-track signal rows."""

    by_track = defaultdict(list)
    for p in plays:
        by_track[(p["artist"], p["track"])].append(p)

    results = []
    for (artist, track), track_plays in by_track.items():
        n = len(track_plays)
        if n < min_plays:
            continue

        track_plays_sorted = sorted(track_plays, key=lambda p: p["ts"])
        first_ts  = track_plays_sorted[0]["ts"]
        last_ts   = track_plays_sorted[-1]["ts"]
        span_days = (last_ts - first_ts).days

        total_ms = sum(p["ms"] for p in track_plays)

        # ── Completion ──
        ratios    = [p["completion_ratio"] for p in track_plays if p.get("completion_ratio") is not None]
        skips     = [p for p in track_plays if p.get("is_skip") == 1]
        non_null  = [p for p in track_plays if p.get("is_skip") is not None]
        skip_count   = len(skips)
        skip_rate    = round(skip_count / len(non_null), 4) if non_null else None
        mean_ratio   = round(statistics.mean(ratios), 4) if ratios else None
        full_listens = sum(1 for r in ratios if r >= 0.90)
        full_listen_rate = round(full_listens / len(ratios), 4) if ratios else None

        # ── Session ──
        openers = sum(1 for p in track_plays if p.get("session_position") == "opener")
        closers = sum(1 for p in track_plays if p.get("session_position") == "closer")
        mids    = sum(1 for p in track_plays if p.get("session_position") == "mid")
        opener_rate = round(openers / n, 4)

        # Within-session repeats: tracks played more than once in the same session
        session_counts = defaultdict(int)
        for p in track_plays:
            session_counts[p["session_id"]] += 1
        sessions_with_repeat = sum(1 for c in session_counts.values() if c > 1)
        within_session_repeats = sum(max(0, c - 1) for c in session_counts.values())
        max_repeats = max(session_counts.values()) - 1

        # ── Temporal ──
        hours = [p["ts"].hour for p in track_plays]
        hour_dist = defaultdict(int)
        for h in hours:
            hour_dist[str(h)] += 1
        peak_hour    = max(hour_dist, key=hour_dist.__getitem__)
        late_night   = sum(1 for h in hours if h in LATE_NIGHT_HOURS)
        late_night_pct = round(late_night / n, 4)

        # ── Decay ──
        cutoff_first = first_ts + timedelta(days=30)
        cutoff_last  = refdate - timedelta(days=30)
        plays_first_30d = sum(1 for p in track_plays if p["ts"] <= cutoff_first)
        plays_last_30d  = sum(1 for p in track_plays if p["ts"] >= cutoff_last)
        burst_ratio_30  = round(plays_first_30d / n, 4)

        results.append({
            "artist":               artist,
            "track":                track,
            "total_plays":          n,
            "total_ms_played":      total_ms,
            "first_play":           first_ts.strftime("%Y-%m-%d"),
            "last_play":            last_ts.strftime("%Y-%m-%d"),
            "span_days":            span_days,
            # completion
            "completion_mean_ratio":  mean_ratio,
            "skip_count":             skip_count,
            "skip_rate":              skip_rate,
            "full_listen_rate":       full_listen_rate,
            # session
            "opener_count":           openers,
            "closer_count":           closers,
            "mid_count":              mids,
            "opener_rate":            opener_rate,
            "within_session_repeats": within_session_repeats,
            # repeat
            "same_session_repeats":   within_session_repeats,
            "max_repeats_in_session": max_repeats,
            "sessions_with_repeat":   sessions_with_repeat,
            # temporal
            "peak_hour":              int(peak_hour),
            "late_night_pct":         late_night_pct,
            "hour_distribution":      json.dumps(dict(sorted(hour_dist.items(), key=lambda x: int(x[0])))),
            # decay
            "plays_first_30d":  plays_first_30d,
            "plays_last_30d":   plays_last_30d,
            "burst_ratio_30":   burst_ratio_30,
        })

    return results


# ─── Database ─────────────────────────────────────────────────────────────────

SCHEMA = """
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


def write_to_db(db_path: Path, rows: list[dict], completion_source: str, meta: dict):
    con = sqlite3.connect(db_path)
    con.execute("DROP TABLE IF EXISTS spotify_signals")
    con.executescript(SCHEMA)

    for row in rows:
        row["completion_source"] = completion_source

    con.executemany("""
        INSERT INTO spotify_signals (
            artist, track, total_plays, total_ms_played,
            first_play, last_play, span_days,
            completion_mean_ratio, completion_source, skip_count, skip_rate, full_listen_rate,
            opener_count, closer_count, mid_count, opener_rate, within_session_repeats,
            same_session_repeats, max_repeats_in_session, sessions_with_repeat,
            peak_hour, late_night_pct, hour_distribution,
            plays_first_30d, plays_last_30d, burst_ratio_30
        ) VALUES (
            :artist, :track, :total_plays, :total_ms_played,
            :first_play, :last_play, :span_days,
            :completion_mean_ratio, :completion_source, :skip_count, :skip_rate, :full_listen_rate,
            :opener_count, :closer_count, :mid_count, :opener_rate, :within_session_repeats,
            :same_session_repeats, :max_repeats_in_session, :sessions_with_repeat,
            :peak_hour, :late_night_pct, :hour_distribution,
            :plays_first_30d, :plays_last_30d, :burst_ratio_30
        )
    """, rows)

    con.commit()
    con.close()


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",    nargs="+", required=True, help="StreamingHistory*.json file(s)")
    ap.add_argument("--db",       default="data/music.db",  help="music.db path")
    ap.add_argument("--durations",default=None,             help="Optional: Artist|Track → duration_ms JSON")
    ap.add_argument("--refdate",  default=None,             help="Reference date YYYY-MM-DD (default: today)")
    ap.add_argument("--min-plays",type=int, default=MIN_PLAYS, help="Min plays to include a track")
    ap.add_argument("--session-gap-minutes", type=int, default=SESSION_GAP_MINUTES)
    args = ap.parse_args()

    # Expand globs (shell may not expand on Windows)
    input_paths = []
    for pattern in args.input:
        expanded = glob.glob(pattern)
        input_paths.extend(expanded if expanded else [pattern])

    refdate = (
        datetime.strptime(args.refdate, "%Y-%m-%d")
        if args.refdate
        else datetime.today()
    )

    durations = load_durations(args.durations) if args.durations else None

    print(f"Loading {len(input_paths)} file(s)...")
    plays = load_history(input_paths)
    print(f"  {len(plays):,} plays loaded")

    if not plays:
        print("No plays found. Check --input paths.")
        return

    date_range = (plays[0]["ts"].strftime("%Y-%m-%d"), plays[-1]["ts"].strftime("%Y-%m-%d"))

    print("Tagging sessions...")
    plays = tag_sessions(plays, args.session_gap_minutes)
    n_sessions = max(p["session_id"] for p in plays) + 1
    print(f"  {n_sessions:,} sessions (gap={args.session_gap_minutes}min)")

    print("Computing completion ratios...")
    plays, completion_source = tag_completion(plays, durations)
    print(f"  completion_source={completion_source}")

    print("Aggregating per track...")
    rows = aggregate(plays, args.min_plays, refdate)
    print(f"  {len(rows):,} tracks (min_plays={args.min_plays})")

    meta = {
        "total_plays":    len(plays),
        "date_range":     date_range,
        "total_sessions": n_sessions,
        "tracks_included": len(rows),
        "completion_source": completion_source,
    }

    print(f"Writing to {args.db}...")
    write_to_db(Path(args.db), rows, completion_source, meta)

    print("Done.")
    print(f"  date_range: {date_range[0]} to {date_range[1]}")
    print(f"  sessions: {n_sessions:,}  tracks: {len(rows):,}  completion: {completion_source}")


if __name__ == "__main__":
    main()
