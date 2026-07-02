# CATALOG:
# id: mte-003
# kind: ticket
# status: done
# origin: external data-profile flag routed via _index index-0061 / twin_spine — plays.ts stored as minute-truncated strings suspected to MIX timezones (Spotify UTC vs Last.fm naive-local) under one undocumented column, corrupting time-of-day/seasonal analysis and blocking precise cross-dataset joins.
# htttw_contribution: 0 — data-correctness / self-describing schema.
# judgment_applied: verify-premise-before-fix, reversibility-first, no-blind-conversion, non-breaking-delta
# provenance: filed+resolved 2026-07-01
# produces: self-describing canonical UTC column (plays.ts_utc, ISO-8601 Z) + consolidate.py emits it going forward + verified-UTC provenance note
# effort: M
# tags: data-correctness, timezone, schema, twin-spine, cross-project
# cross-links: mte-001, mte-004, _index index-0061, twin_spine music_db adapter

# Make plays.ts timezone-consistent (add canonical ts_utc)

## Finding (premise corrected)
The external profile assumed Spotify rows are UTC and Last.fm rows are naive-local — i.e. mixed timezones. **Empirically false for this corpus.** Nearest-match offset between the two sources for identical `(artist, track)` plays clusters at ~0h (3,563+1,286 matches at 0.0/-0.1h, minute-truncation jitter) with NO seasonal DST spike across 2009-2026. Both sources are UTC: Spotify standard-export `endTime` is documented UTC, and the Last.fm CSV export (`edgarturtleblot.csv`) is UTC too. A local→UTC conversion of Last.fm (the profile's proposed fix) would have SHIFTED correct data by hours and corrupted it. Not done.

The real defects that DID hold:
1. `ts` carries no timezone marker — not self-describing; a consumer (twin_spine) cannot know it is UTC.
2. Minute precision only — and this is a SOURCE limitation. Both raw exports are minute-grain standard format; no extended/seconds data exists anywhere in `data/`. No seconds to recover.

## Move (done)
- Backed up `data/music.db` → `data/music.db.bak-20260701` (reversible; regenerable via build.sh).
- Added `plays.ts_utc TEXT` — canonical UTC ISO-8601 with explicit `Z`, minute precision (seconds always `:00`). Indexed (`plays_ts_utc`).
- Migrated the live 121,370-row db in place (ts values are UTC wall-clock → reinterpreted as UTC ISO). 0 nulls, 0 mismatches, row count preserved. In-place delta chosen over full rebuild because the live corpus (121,370 rows, incl. `data/df26 spotify/`) has different provenance than build.sh's inputs (84,967 rows); a rebuild would have silently changed the corpus.
- Kept legacy `ts` (naive `%Y-%m-%d %H:%M`) untouched for engine.py back-compat (engine.py:96 parses it strictly).
- Updated `consolidate.py`: schema, INSERT, and module docstring emit + document `ts_utc` on all future rebuilds.

## Verify
- Last.fm `20 Mar 2026 18:59` → `2026-03-20T18:59:00Z` ✓
- Spotify `2014-12-09 19:46` (UTC endTime) → `2014-12-09T19:46:00Z` ✓
- `engine.py profile` reads the migrated db unchanged ✓
- Fresh `consolidate.py` rebuild carries the new schema + populated ts_utc ✓

## Done-when
`plays` has a self-describing canonical UTC column, consolidate emits it, live db migrated, engine unbroken. ✓
