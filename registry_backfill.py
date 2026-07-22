#!/usr/bin/env python3
"""
registry_backfill.py — seed the provenance registry with findings we already hold.

MVP seeding. mte-006 auto-emits (it imports registry and calls emit on every run).
The findings below were established by analyses that do NOT yet auto-emit — they are
seeded here with their real current claims so the canon isn't empty. Each is stamped
with the CURRENT snapshot; to get a true per-run provenance stamp, re-run its analysis
once it's wired to emit (the "beyond" step). Numbers verified against the live db at
seed time (self-affinity re-run; peak-hour per mte-004; mte-005 per its analysis).

Run: `python registry.py backfill`
"""
from __future__ import annotations
from pathlib import Path
import registry

SEEDS = [
    dict(
        analysis="self_affinity", version="1.0",
        levers={"artist": "Mark Koranda", "gap_days": 180},
        stats={"total_self_plays": 1014, "distinct_self_tracks": 147},
        findings=[{
            "id": "self_affinity.self_replay",
            "claim": ("Mark replays his OWN catalog heavily — 1,014 plays across 147 distinct "
                      "self-recorded tracks (artist 'Mark Koranda'), a top-15 artist by playcount. "
                      "Self-replay is the strongest 'this mattered to me' signal available."),
            "status": "supported", "confidence": "high",
            "caveats": "Consumption-side only; content-feature output↔taste alignment (mte-001) is a separate, undecided axis.",
            "key_stats": {"total_self_plays": 1014, "distinct_self_tracks": 147},
        }],
    ),
    dict(
        analysis="engine_signals", version="1.0",
        levers={"tz": "America/Chicago"},
        stats={"peak_hour_local": "20:00", "late_night_pct_local": 0.154,
               "peak_hour_utc_naive": "02:00", "late_night_pct_utc": 0.428},
        findings=[{
            "id": "engine.peak_hour_local",
            "claim": ("Peak listening hour is ~8pm LOCAL (America/Chicago) with 15.4% late-night, "
                      "not 2am / 42.8% as UTC-naive hours wrongly implied. Time-of-day signals must be "
                      "read in local wall-clock (mte-004)."),
            "status": "supported", "confidence": "high",
            "caveats": "Requires tzdata on Windows for historically-correct DST; falls back to UTC with a warning if absent.",
            "key_stats": {"peak_hour_local": "20:00", "late_night_pct_local": 0.154},
        }],
    ),
    dict(
        analysis="mte005_context_analysis", version="1.0",
        levers={"source": "recommendation_log.db"},
        stats={"runs": 5, "distinct_contexts": 5, "cross_context_tracks": 2, "distinct_tracks": 168},
        findings=[{
            "id": "mte005.context_convergence",
            "claim": ("Whether repeat use of a mood/context converges on the same tracks is UNTESTABLE "
                      "as instrumented: 5 logged runs, 5 distinct contexts, zero repeats. Of 168 served "
                      "tracks only 2 (1.2%) cross contexts. The log is too lightly used to read yet."),
            "status": "untestable", "confidence": "high",
            "caveats": "Blocked by mte-007: recommendation_log needs populated run timestamps + an outcome/feedback channel before convergence is measurable.",
            "key_stats": {"runs": 5, "cross_context_tracks": 2},
        }],
    ),
]


def backfill(db_path: Path = registry.DEFAULT_DB) -> None:
    for i, s in enumerate(SEEDS):
        registry.emit(**s, db_path=db_path, render_after=(i == len(SEEDS) - 1))
    print(f"[backfill] seeded {len(SEEDS)} findings from established analyses.")


if __name__ == "__main__":
    backfill()
