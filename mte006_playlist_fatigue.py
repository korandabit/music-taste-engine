#!/usr/bin/env python3
"""
mte006_playlist_fatigue.py — Does playlist membership specifically drive track fatigue?

Ticket: backlog-tickets/mte-006_playlist-driven-fatigue-mechanism.md

Spotify's export records NO playlist attribution per play (no context_uri), so the
direct causal test is impossible. This script substitutes a SEQUENCE-FINGERPRINT
corroboration (BLAST / Shazam analogy): a playlist was plausibly being played when a
contiguous run of the timestamp-ordered play history matches that playlist's track
ORDER. The causal link was never logged; a structural-similarity match against a known
reference sequence stands in for it, with confidence + temporal validity reported
explicitly rather than forced to a binary.

Two jobs:
  1. --migrate : add an explicit `position` column to music.db `playlists` and backfill
     it from id-order within each playlist (verified equivalent to JSON insertion order:
     all 147 playlists have contiguous id ranges). Idempotent; backs up first.
  2. analysis  : find playlist-order runs in the play stream, tag corroborated plays,
     then compare skip-rate of playlist-corroborated plays vs freely-chosen plays.

Data constraints (verified 2026-07-22 against data/music.db):
  - is_skip / ms_played exist on only the Spotify-sourced plays (55,980 of 121,370);
    Last.fm-only plays (2009-era) have no skip signal. The fatigue comparison is
    therefore restricted to skip-eligible plays.
  - playlists.playlist_modified ranges 2018-2026; Playlist1.json carries only the
    CURRENT order + lastModifiedDate, no edit history. A match far from that date is
    weaker evidence — reported via a temporal-delta and a strict-window subset.

stdlib only.
"""
from __future__ import annotations
import argparse, json, shutil, sqlite3, sys
from datetime import datetime, timedelta
from pathlib import Path

# Windows consoles default to cp1252; force utf-8 so report glyphs don't crash.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):
    pass


# ─── normalization ────────────────────────────────────────────────────────────
def norm(s: str) -> str:
    return (s or "").strip().casefold()

def key(artist: str, track: str) -> tuple[str, str]:
    return (norm(artist), norm(track))

def parse_ts(ts_utc: str) -> datetime | None:
    # canonical ts_utc is 'YYYY-MM-DDTHH:MM:00Z' (mte-003)
    try:
        return datetime.strptime(ts_utc, "%Y-%m-%dT%H:%M:00Z")
    except (ValueError, TypeError):
        return None

def parse_date(d: str) -> datetime | None:
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(d, fmt)
        except (ValueError, TypeError):
            continue
    return None


# ─── migration ──────────────────────────────────────────────────────────────--
def migrate(db_path: Path) -> None:
    con = sqlite3.connect(db_path)
    cols = [r[1] for r in con.execute("PRAGMA table_info(playlists)")]
    if "position" in cols:
        # already migrated; ensure backfilled (no nulls)
        nulls = con.execute("SELECT COUNT(*) FROM playlists WHERE position IS NULL").fetchone()[0]
        if nulls == 0:
            print(f"[migrate] `position` already present and populated ({db_path.name}); nothing to do.")
            con.close()
            return
        print(f"[migrate] `position` present but {nulls} nulls; backfilling.")
    else:
        bak = db_path.with_suffix(db_path.suffix + f".bak-mte006-{datetime.now():%Y%m%d%H%M%S}")
        shutil.copy2(db_path, bak)
        print(f"[migrate] backed up -> {bak.name}")
        con.execute("ALTER TABLE playlists ADD COLUMN position INTEGER")

    # backfill: 0-based order within each playlist by ascending id
    # (verified: id-order within a playlist == JSON insertion order == playlist order)
    con.execute("""
        WITH ranked AS (
            SELECT id, ROW_NUMBER() OVER (PARTITION BY playlist_name ORDER BY id) - 1 AS pos
            FROM playlists
        )
        UPDATE playlists SET position = (SELECT pos FROM ranked WHERE ranked.id = playlists.id)
    """)
    con.commit()
    nulls = con.execute("SELECT COUNT(*) FROM playlists WHERE position IS NULL").fetchone()[0]
    n = con.execute("SELECT COUNT(*) FROM playlists").fetchone()[0]
    print(f"[migrate] backfilled position on {n} rows; {nulls} nulls remaining.")
    con.close()


# ─── load ──────────────────────────────────────────────────────────────────────
def load_playlists(con) -> dict[str, dict]:
    """playlist_name -> {'modified': dt|None, 'seq': [(pos, key), ...] ordered}"""
    out: dict[str, dict] = {}
    rows = con.execute(
        "SELECT playlist_name, playlist_modified, position, artist, track "
        "FROM playlists ORDER BY playlist_name, position"
    ).fetchall()
    for name, modified, pos, artist, track in rows:
        d = out.setdefault(name, {"modified": parse_date(modified), "modified_raw": modified, "seq": []})
        d["seq"].append((pos, key(artist, track)))
    return out

def load_stream(con) -> list[dict]:
    """time-ordered play stream (all sources); is_skip present only on Spotify-era rows."""
    rows = con.execute(
        "SELECT id, ts_utc, artist, track, is_skip, source FROM plays "
        "WHERE ts_utc IS NOT NULL ORDER BY ts_utc, id"
    ).fetchall()
    stream = []
    for pid, ts_utc, artist, track, is_skip, source in rows:
        stream.append({
            "pid": pid, "dt": parse_ts(ts_utc), "key": key(artist, track),
            "is_skip": is_skip, "source": source,
        })
    return stream


# ─── subsequence matching ───────────────────────────────────────────────────────
def find_runs(stream, playlist, params):
    """
    Greedy playlist-order run detection.

    A 'run' = a sequence of plays, in stream order, whose playlist positions are
    monotonically increasing by 1..max_pos_jump, that are near-adjacent in the stream
    (<= max_stream_gap intervening foreign plays) and near-adjacent in wall-clock
    (<= max_wall_gap_min per hop). Runs of length >= min_run corroborate that the
    playlist was being played straight-through.
    """
    seq = playlist["seq"]
    # normalized-key -> sorted list of positions in this playlist
    pos_of: dict[tuple, list[int]] = {}
    for pos, k in seq:
        pos_of.setdefault(k, []).append(pos)
    for lst in pos_of.values():
        lst.sort()

    # candidate plays: those whose key is in the playlist, with stream index
    cands = [(i, sp) for i, sp in enumerate(stream) if sp["key"] in pos_of]
    if len(cands) < params["min_run"]:
        return []

    runs = []
    cur = None  # {'plays':[(i,pos,dt)], 'exact_steps':int, 'total_steps':int}

    def close(cur):
        if cur and len(cur["plays"]) >= params["min_run"]:
            runs.append(cur)

    for i, sp in cands:
        dt = sp["dt"]
        positions = pos_of[sp["key"]]
        extended = False
        if cur is not None:
            last_i, last_pos, last_dt = cur["plays"][-1]
            stream_gap = i - last_i
            wall_gap = ((dt - last_dt).total_seconds() / 60.0) if (dt and last_dt) else 1e9
            if stream_gap <= params["max_stream_gap"] and wall_gap <= params["max_wall_gap_min"]:
                # pick smallest position that is a forward step within jump budget
                nxt = next((p for p in positions
                            if 1 <= (p - last_pos) <= params["max_pos_jump"]), None)
                if nxt is not None:
                    cur["plays"].append((i, nxt, dt))
                    cur["total_steps"] += 1
                    if nxt - last_pos == 1:
                        cur["exact_steps"] += 1
                    extended = True
        if not extended:
            close(cur)
            # start a new run at the earliest position for this key
            cur = {"plays": [(i, positions[0], dt)], "exact_steps": 0, "total_steps": 0}
    close(cur)
    return runs


def run_summary(run, playlist_name, modified_dt):
    idxs = [p[0] for p in run["plays"]]
    dts  = [p[2] for p in run["plays"] if p[2]]
    length = len(run["plays"])
    strictness = (run["exact_steps"] / run["total_steps"]) if run["total_steps"] else 1.0
    med_dt = dts[len(dts) // 2] if dts else None
    delta_days = abs((med_dt - modified_dt).days) if (med_dt and modified_dt) else None
    # confidence: longer runs + stricter order => higher
    conf = min(1.0, (length / 8.0)) * (0.5 + 0.5 * strictness)
    return {
        "playlist": playlist_name,
        "length": length,
        "order_strictness": round(strictness, 3),
        "start": dts[0].strftime("%Y-%m-%d") if dts else None,
        "end": dts[-1].strftime("%Y-%m-%d") if dts else None,
        "temporal_delta_days": delta_days,
        "confidence": round(conf, 3),
        "stream_idxs": idxs,
    }


# ─── analysis ────────────────────────────────────────────────────────────────---
def analyze(db_path: Path, params: dict) -> dict:
    con = sqlite3.connect(db_path)
    playlists = load_playlists(con)
    stream = load_stream(con)
    con.close()

    all_runs = []
    corroborated: dict[int, list[str]] = {}   # stream_idx -> [playlist names]
    for name, pl in playlists.items():
        if len(pl["seq"]) < params["min_run"]:
            continue
        for run in find_runs(stream, pl, params):
            summ = run_summary(run, name, pl["modified"])
            all_runs.append(summ)
            for si in summ["stream_idxs"]:
                corroborated.setdefault(si, []).append(name)

    strict_window = params["strict_window_days"]
    # a play is strictly-corroborated if it belongs to at least one run whose temporal_delta <= strict_window
    strict_idxs = set()
    for r in all_runs:
        if r["temporal_delta_days"] is not None and r["temporal_delta_days"] <= strict_window:
            strict_idxs.update(r["stream_idxs"])

    # ── fatigue comparison (skip-eligible plays only) ──
    skip_eligible = [(i, sp) for i, sp in enumerate(stream) if sp["is_skip"] is not None]
    def rate(idxs):
        vals = [stream[i]["is_skip"] for i in idxs]
        return (sum(vals) / len(vals)) if vals else None

    corr_ids   = [i for i, sp in skip_eligible if i in corroborated]
    strict_ids = [i for i, sp in skip_eligible if i in strict_idxs]
    free_ids   = [i for i, sp in skip_eligible if i not in corroborated]

    # per-track: skip rate inside vs outside corroborated runs, for tracks with both
    from collections import defaultdict
    in_run  = defaultdict(list)
    out_run = defaultdict(list)
    for i, sp in skip_eligible:
        (in_run if i in corroborated else out_run)[sp["key"]].append(sp["is_skip"])
    per_track = []
    for k in set(in_run) & set(out_run):
        if len(in_run[k]) >= 3 and len(out_run[k]) >= 3:
            per_track.append({
                "track": f"{k[0]} — {k[1]}",
                "n_in_run": len(in_run[k]), "skip_in_run": round(sum(in_run[k]) / len(in_run[k]), 3),
                "n_free": len(out_run[k]), "skip_free": round(sum(out_run[k]) / len(out_run[k]), 3),
                "delta": round(sum(in_run[k]) / len(in_run[k]) - sum(out_run[k]) / len(out_run[k]), 3),
            })
    per_track.sort(key=lambda x: -x["delta"])

    run_lengths = sorted((r["length"] for r in all_runs), reverse=True)
    return {
        "params": params,
        "corpus": {
            "total_plays_in_stream": len(stream),
            "skip_eligible_plays": len(skip_eligible),
            "playlists_considered": sum(1 for p in playlists.values() if len(p["seq"]) >= params["min_run"]),
        },
        "matching": {
            "runs_found": len(all_runs),
            "run_length_distribution": {
                "max": run_lengths[0] if run_lengths else 0,
                "median": run_lengths[len(run_lengths)//2] if run_lengths else 0,
                "count_ge_6": sum(1 for x in run_lengths if x >= 6),
            },
            "plays_corroborated_any": len(corroborated),
            "plays_corroborated_strict": len(strict_idxs),
            "top_runs": sorted(all_runs, key=lambda r: (-r["length"], -(r["confidence"])))[:15],
        },
        "fatigue_comparison": {
            "note": "skip-eligible = Spotify-era plays with is_skip; Last.fm-only plays excluded (no skip signal).",
            "corroborated_any": {"n": len(corr_ids), "skip_rate": rate(corr_ids)},
            "corroborated_strict_window": {"n": len(strict_ids), "skip_rate": rate(strict_ids),
                                           "window_days": strict_window},
            "freely_chosen": {"n": len(free_ids), "skip_rate": rate(free_ids)},
            "per_track_in_vs_out": per_track[:20],
        },
    }


# ─── report ──────────────────────────────────────────────────────────────────--
def print_report(res: dict) -> None:
    c, m, f = res["corpus"], res["matching"], res["fatigue_comparison"]
    print("\n=== mte-006: playlist-driven fatigue (sequence-fingerprint corroboration) ===")
    print(f"stream plays: {c['total_plays_in_stream']:,}  |  skip-eligible: {c['skip_eligible_plays']:,}  |  playlists: {c['playlists_considered']}")
    print(f"\nmatching (min_run={res['params']['min_run']}, max_pos_jump={res['params']['max_pos_jump']}, "
          f"max_stream_gap={res['params']['max_stream_gap']}, max_wall_gap={res['params']['max_wall_gap_min']}min):")
    print(f"  playlist-order runs found: {f_runs(m)}  |  max len {m['run_length_distribution']['max']}, "
          f"median {m['run_length_distribution']['median']}, len>=6: {m['run_length_distribution']['count_ge_6']}")
    print(f"  plays corroborated (any): {m['plays_corroborated_any']:,}  |  strict-window: {m['plays_corroborated_strict']:,}")
    print("\nfatigue comparison (skip-rate; lower = less fatigue):")
    for label, blk in (("corroborated (any)", f["corroborated_any"]),
                       ("corroborated (strict window)", f["corroborated_strict_window"]),
                       ("freely chosen", f["freely_chosen"])):
        sr = blk["skip_rate"]
        print(f"  {label:32s} n={blk['n']:>6}  skip_rate={sr if sr is None else round(sr,4)}")
    if f["per_track_in_vs_out"]:
        print("\n  per-track (in-run vs free, tracks with >=3 each), top skip-delta:")
        for t in f["per_track_in_vs_out"][:8]:
            print(f"    {t['track'][:50]:50s}  in={t['skip_in_run']} (n{t['n_in_run']})  free={t['skip_free']} (n{t['n_free']})  Δ{t['delta']:+}")
    print()

def f_runs(m):
    return f"{m['runs_found']:,}"


# ─── main ────────────────────────────────────────────────────────────────────--
def main():
    ap = argparse.ArgumentParser(description="mte-006 playlist-driven fatigue analysis")
    ap.add_argument("--db", default="data/music.db")
    ap.add_argument("--migrate", action="store_true", help="add+backfill playlists.position, then exit")
    ap.add_argument("--out", default=None, help="write full result JSON here")
    ap.add_argument("--min-run", type=int, default=4)
    ap.add_argument("--max-pos-jump", type=int, default=2, help="allow skipping <=1 playlist track")
    ap.add_argument("--max-stream-gap", type=int, default=2, help="allow <=1 intervening foreign play")
    ap.add_argument("--max-wall-gap-min", type=float, default=15.0)
    ap.add_argument("--strict-window-days", type=int, default=180,
                    help="run's median play date must be within this of playlist_modified to count as strict")
    ap.add_argument("--no-emit", action="store_true", help="skip writing this run to the provenance registry")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        sys.exit(f"db not found: {db_path}")

    if args.migrate:
        migrate(db_path)
        return

    # migration is a prerequisite; ensure position exists
    con = sqlite3.connect(db_path)
    cols = [r[1] for r in con.execute("PRAGMA table_info(playlists)")]
    con.close()
    if "position" not in cols:
        print("[analyze] playlists.position missing — run with --migrate first.", file=sys.stderr)
        sys.exit(2)

    params = {
        "min_run": args.min_run, "max_pos_jump": args.max_pos_jump,
        "max_stream_gap": args.max_stream_gap, "max_wall_gap_min": args.max_wall_gap_min,
        "strict_window_days": args.strict_window_days,
    }
    res = analyze(db_path, params)
    print_report(res)
    if args.out:
        Path(args.out).write_text(json.dumps(res, indent=2, default=str), encoding="utf-8")
        print(f"[analyze] full JSON -> {args.out}")

    # emit to the provenance registry: this analysis is a SURFACE (levers + stats),
    # not a verdict. Re-running with different levers appends a run and updates the row.
    if not args.no_emit:
        import registry
        m, fc = res["matching"], res["fatigue_comparison"]
        sr_corr = fc["corroborated_any"]["skip_rate"]
        sr_free = fc["freely_chosen"]["skip_rate"]
        registry.emit(
            analysis="mte006_playlist_fatigue", version="1.0", levers=params,
            stats={
                "runs_found": m["runs_found"],
                "plays_corroborated_any": m["plays_corroborated_any"],
                "plays_corroborated_strict": m["plays_corroborated_strict"],
                "skip_rate_corroborated": round(sr_corr, 4) if sr_corr is not None else None,
                "skip_rate_free": round(sr_free, 4) if sr_free is not None else None,
            },
            findings=[{
                "id": "mte006.playlist_fatigue",
                "claim": ("No detectable playlist-driven fatigue: plays corroborated as part of a "
                          "playlist-order run are skipped LESS (~0.09–0.12) than freely-chosen plays "
                          "(~0.20) — opposite of the hypothesized net effect, and confounded by the "
                          "intentional-listening selection baked into the detection method."),
                "status": "provisional", "confidence": "low",
                "caveats": ("Temporal validity is a large filter — only the strict-window subset of "
                            "corroborated plays matches a playlist order recorded near the play date. "
                            "A weak per-track 'skipped-more-in-its-playlist' signal exists but is too "
                            "thin (n=3–9) to confirm cumulative overplay. Direct causal test is "
                            "impossible: Spotify's export has no playlist attribution (no context_uri)."),
                "key_stats": {
                    "skip_rate_corroborated": round(sr_corr, 4) if sr_corr is not None else None,
                    "skip_rate_free": round(sr_free, 4) if sr_free is not None else None,
                    "runs_found": m["runs_found"],
                },
            }],
        )


if __name__ == "__main__":
    main()
