# CATALOG:
# id: mte-004
# kind: ticket
# status: done
# origin: discovered while resolving mte-003 — engine.py time-of-day signals compute hour-of-day from UTC timestamps, but "late night" / "peak hour" are HUMAN wall-clock concepts.
# htttw_contribution: 0 — analysis-correctness.
# judgment_applied: capture-at-source
# provenance: filed 2026-07-01
# produces: local-time conversion at analysis time for time-of-day reads
# effort: M
# tags: data-correctness, timezone, signals, analyze
# cross-links: mte-003

# Convert UTC → local at analysis time for time-of-day signals

## Problem
Now that `ts`/`ts_utc` are confirmed UTC (mte-003), the time-of-day metrics in `engine.py` read UTC hours directly:
- `analyze`: `p["hour"] = ts.hour`, `is_late_night = ts.hour in LATE_NIGHT_HOURS` (engine.py ~136-140)
- `signals`: `late_night_pct` / `peak_hour` / `hour_distribution` computed on UTC hours (~315-331)

"Late night" and "peak hour" are human wall-clock concepts. Computed on UTC, they are shifted by the user's UTC offset — e.g. an 11pm-local listen is counted in the wrong hour bucket. This does not corrupt stored data (mte-003 fixed the store), but it biases the interpretation.

## Move
- Add an analysis-time UTC→local conversion (a `--tz` flag, default the host/user zone), applied where hour-of-day is derived, using `zoneinfo` for correct historical DST.
- Leave the stored columns UTC-canonical (correct); convert only at read/analysis time.
- Re-verify `late_night_pct` / `peak_hour` shift as expected after conversion.

## Done-when
Time-of-day signals reflect the user's local wall-clock, with UTC remaining the canonical store. ✓

## Resolution (2026-07-16)
- Added `--tz` flag (default `America/Chicago`, per Mark; user-confirmed) to `signals`, `analyze`, `playlist`, `profile`. Uses stdlib `zoneinfo` for historically-correct DST; falls back to UTC (with a stderr warning) if `tzdata` isn't installed, since Windows has no built-in IANA db.
- `resolve_tz()` / `to_local()` helpers convert only where hour-of-day is derived: `enrich()` (`hour`, `is_late_night`), `compute_temporal()` (`peak_hour`, `late_night_pct`, hour-repeat `repeat_rate`), and `_sig_aggregate()` (signals `peak_hour`/`late_night_pct`/`hour_distribution`). Stored `ts`/`ts_utc` columns and day-level math (span, quartiles, burst ratios, `days_ago`) are untouched — still UTC-canonical per mte-003.
- Verified the shift is real on the live corpus: UTC-naive peak hour was 2am / 42.8% late-night; with `--tz America/Chicago` peak hour is 8pm / 15.4% late-night — consistent with the ~5-6h CST/CDT offset.
- Smoke-tested `analyze`, `profile` against `data/music.db`; no regressions.
