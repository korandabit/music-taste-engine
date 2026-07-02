# CATALOG:
# id: mte-004
# kind: ticket
# status: open
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
Time-of-day signals reflect the user's local wall-clock, with UTC remaining the canonical store.
