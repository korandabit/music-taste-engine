#!/usr/bin/env python3
"""mte005_context_analysis.py — read recommendation_log.db as a mood-hypothesis log.

Ticket mte-005: each `playlist` invocation names a free-text `--context` (a mood
or state) and logs the tracks served as hypotheses about what fills that mood.
This script asks two questions of that log:

  1. Repeat-convergence: for repeat/similar contexts, do served tracks converge
     (same tracks keep getting picked) or diverge? -- ONLY answerable if a
     context has been run more than once. If every context has exactly one run,
     this question has no data to test and the script says so plainly instead
     of fabricating a convergence finding.
  2. Cross-run track overlap: which (artist, track) pairs appear across more
     than one run (weak/generic mood signal) vs. exactly one run (candidate
     functional-use-case signal, i.e. context-bound).

Owned by music_taste_engine. Reads recommendation_log.db only; never writes to
it. Matches the house style of self_affinity.py (stdlib-only, argparse, plain
print report + optional --json).

Usage:
    python mte005_context_analysis.py [--db recommendation_log.db] [--json]
"""
import argparse
import json
import sqlite3
import sys
from collections import defaultdict

# Windows console codepages (cp1252 etc.) can't render every character SQLite
# hands back (e.g. U+00B7 MIDDLE DOT, used below as a context separator).
# Reconfigure stdout to UTF-8 with a safe fallback so the report never crashes
# on print(); on the real bytes this is NOT mojibake -- it round-trips fine.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
except AttributeError:
    pass


def clean_context(s: str) -> str:
    """Normalize the context label for display. Investigation for mte-005 found
    the U+FFFD-looking glyphs seen in some terminals are NOT stored mojibake --
    the DB genuinely stores U+00B7 MIDDLE DOT ('.') as a separator (e.g.
    "spring . LTP . scored"); it just fails to render in some Windows consoles/
    fonts. No correction is applied to the underlying string; this function is
    kept as a hook in case a real encoding issue is found in a future run.
    """
    return s


def load(db_path: str):
    con = sqlite3.connect(db_path)
    recommended = con.execute(
        "SELECT ts, playlist_run, artist, track, context, plays, returns, "
        "rest_days, spring_pct FROM recommended"
    ).fetchall()
    feedback_count = con.execute("SELECT COUNT(*) FROM feedback").fetchone()[0]
    con.close()
    return recommended, feedback_count


def analyze(db_path: str) -> dict:
    rows, feedback_count = load(db_path)

    # --- per-run / per-context grouping ---
    runs = defaultdict(list)          # playlist_run -> list of row dicts
    run_context = {}                  # playlist_run -> context (raw)
    context_runs = defaultdict(set)   # context (raw) -> set of playlist_run

    for ts, run, artist, track, context, plays, returns, rest_days, spring_pct in rows:
        runs[run].append({
            "artist": artist, "track": track, "ts": ts,
            "plays": plays, "returns": returns,
            "rest_days": rest_days, "spring_pct": spring_pct,
        })
        run_context[run] = context
        context_runs[context].add(run)

    per_run_table = []
    for run, items in runs.items():
        per_run_table.append({
            "playlist_run": run,
            "context_raw": run_context[run],
            "context_clean": clean_context(run_context[run]),
            "n_tracks": len(items),
            "first_ts": next((it["ts"] for it in items if it["ts"]), None),
        })
    per_run_table.sort(key=lambda r: r["playlist_run"])

    # --- repeat-convergence testability check ---
    contexts_with_multiple_runs = {c: r for c, r in context_runs.items() if len(r) > 1}
    convergence_testable = len(contexts_with_multiple_runs) > 0

    # --- cross-run track overlap ---
    track_runs = defaultdict(set)
    for ts, run, artist, track, context, plays, returns, rest_days, spring_pct in rows:
        track_runs[(artist, track)].add(run)

    distinct_pairs = len(track_runs)
    cross_context_pairs = sorted(
        ((artist, track, sorted(rs)) for (artist, track), rs in track_runs.items() if len(rs) > 1),
        key=lambda x: (-len(x[2]), x[0], x[1]),
    )
    single_context_count = sum(1 for rs in track_runs.values() if len(rs) == 1)

    return {
        "db_path": db_path,
        "total_recommended_rows": len(rows),
        "feedback_rows": feedback_count,
        "distinct_runs": len(runs),
        "distinct_contexts": len(context_runs),
        "per_run": per_run_table,
        "convergence_testable": convergence_testable,
        "contexts_with_multiple_runs": {
            clean_context(c): sorted(r) for c, r in contexts_with_multiple_runs.items()
        },
        "distinct_track_pairs": distinct_pairs,
        "cross_context_pair_count": len(cross_context_pairs),
        "single_context_pair_count": single_context_count,
        "cross_context_pairs": [
            {"artist": a, "track": t, "runs": rs} for a, t, rs in cross_context_pairs
        ],
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default="recommendation_log.db")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    result = analyze(args.db)

    if args.json:
        print(json.dumps(result, indent=2))
        return

    print("mte-005 context/mood-hypothesis analysis")
    print(f"  db: {result['db_path']}")
    print(f"  {result['total_recommended_rows']} recommended rows, "
          f"{result['feedback_rows']} feedback rows")
    print(f"  {result['distinct_runs']} distinct playlist_run values, "
          f"{result['distinct_contexts']} distinct context values")

    print("\n  Repeat-convergence testability:")
    if result["convergence_testable"]:
        print("    TESTABLE -- contexts with >1 run:")
        for c, rs in result["contexts_with_multiple_runs"].items():
            print(f"      {c!r}: runs={rs}")
    else:
        print("    NOT TESTABLE -- every context has exactly one run (n=1 per context).")
        print("    There is no repeat structure to measure convergence/divergence over.")

    print("\n  Per-run shape:")
    print(f"  {'n_tracks':>8}  {'playlist_run':<22} context (cleaned)")
    for r in result["per_run"]:
        print(f"  {r['n_tracks']:>8}  {r['playlist_run']:<22} {r['context_clean']}")

    print("\n  Cross-run track overlap:")
    print(f"    {result['distinct_track_pairs']} distinct (artist, track) pairs served across all runs")
    print(f"    {result['cross_context_pair_count']} pairs appear in >1 run (cross-context / weak-signal)")
    print(f"    {result['single_context_pair_count']} pairs appear in exactly 1 run (context-bound / candidate functional-use signal)")
    if result["cross_context_pairs"]:
        print("\n    Cross-context pairs:")
        for p in result["cross_context_pairs"]:
            print(f"      {p['artist']} - {p['track']}  (runs: {', '.join(p['runs'])})")


if __name__ == "__main__":
    main()
