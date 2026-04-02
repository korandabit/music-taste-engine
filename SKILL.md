---
name: music-data-engine
description: >
  Unified music listening data analysis across Last.fm and Spotify.
  **consolidate.py** — One-time ingest: merges Last.fm CSV + Spotify exports into
  music.db (SQLite). Run once, or after adding new exports.
  **engine.py** — All analysis. Two subcommands: `signals` computes Spotify behavioral
  signals (skip rate, completion ratio, session position) and writes them to music.db.
  `analyze` reads music.db and produces a full JSON output: temporal overview,
  listening clock, seasonal distribution, top lists, year-by-year obsessions, epoch
  detection, per-track trajectory classification (FLASH_BINGE / DISCOVERY_HEAVY /
  FRONT_LOADED / PERENNIAL_RETURN / SLOW_BURN / REDISCOVERY / DIFFUSE), LTP detection
  (long-delay true positives — tracks returned to after 180d+ gaps), playlist scoring,
  binge->lifespan correlations, and Spotify enrichment fields when available.
  Trigger when user uploads music data files or asks about listening history,
  music taste, playlist generation, artist deep-dive, trajectory analysis,
  or Spotify library/playlist data.
---

# Music Data Engine -- Skill Instructions

## Asset locations

| File | Location |
|---|---|
| Last.fm CSV | `data/edgarturtleblot.csv` |
| Spotify streaming history | `data/StreamingHistory0.json` (and any `StreamingHistory*.json`) |
| Spotify library | `data/YourLibrary.json` |
| Spotify playlists | `data/Playlist1.json` |
| SQLite database | `data/music.db` (pre-built; rebuild with `consolidate.py`) |
| Analysis engine | `engine.py` |
| Ingest script | `consolidate.py` |

The `data/music.db` is pre-built and ships with the skill bundle. Rebuild only if new
exports are added.

## Which script to use

| Goal | Command |
|---|---|
| Rebuild database from raw exports | `consolidate.py` |
| Add/refresh Spotify behavioral signals | `engine.py signals` |
| Full listening autobiography + trajectory | `engine.py analyze` |
| Artist deep-dive | `engine.py analyze --artist "Name"` |
| Seasonal playlist | `engine.py analyze --months 3,4,5 --n 50` |

## Step 0 -- Consolidate data sources (one-time setup)

Only needed when adding new exports. The pre-built `data/music.db` is already current.

```bash
python consolidate.py --csv data/edgarturtleblot.csv --spotify-dir data/ --out data/music.db
```

Expected output:
```
82,384 plays (Last.fm)
2,952 plays (Spotify)
lastfm: 82,015 | lastfm+spotify: 369 | spotify: 2,583
total: 84,967
2,170 saved tracks, 68 saved albums
4,179 playlist items
```

### music.db schema

**`plays`** (written by consolidate.py):
```sql
id, source, ts, artist, album, track, ms_played, is_skip
```
`source` is one of `lastfm`, `spotify`, `lastfm+spotify`

**`spotify_signals`** (written by `engine.py signals`):
```sql
artist, track, total_plays, total_ms_played, first_play, last_play, span_days,
completion_mean_ratio, completion_source, skip_count, skip_rate, full_listen_rate,
opener_count, closer_count, mid_count, opener_rate,
within_session_repeats, max_repeats_in_session, sessions_with_repeat,
peak_hour, late_night_pct, hour_distribution,
plays_first_30d, plays_last_30d, burst_ratio_30
```

**`library_tracks`, `library_albums`, `playlists`**: Spotify saved items (unchanged).

### Querying music.db

```python
import sqlite3, json
con = sqlite3.connect("data/music.db")
con.row_factory = sqlite3.Row

# Total plays by source
for r in con.execute("SELECT source, COUNT(*) n FROM plays GROUP BY source"):
    print(r["source"], r["n"])

# Top 10 artists all-time
for r in con.execute("SELECT artist, COUNT(*) n FROM plays GROUP BY artist ORDER BY n DESC LIMIT 10"):
    print(r["artist"], r["n"])

# Plays per year
for r in con.execute("SELECT SUBSTR(ts,1,4) yr, COUNT(*) n FROM plays GROUP BY yr ORDER BY yr"):
    print(r["yr"], r["n"])

# Join Spotify signals
for r in con.execute("""
    SELECT p.artist, p.track, COUNT(*) plays, s.skip_rate, s.completion_mean_ratio
    FROM plays p
    LEFT JOIN spotify_signals s ON LOWER(p.artist)=LOWER(s.artist) AND LOWER(p.track)=LOWER(s.track)
    GROUP BY p.artist, p.track
    ORDER BY plays DESC LIMIT 10
"""):
    print(dict(r))
```

## Step 1 -- Dependency check

```bash
python3 -c "import json, math, statistics, collections, datetime, argparse, pathlib, sqlite3; print('OK')"
```

Always passes -- stdlib only, no pip installs required.

## Step 2a -- Add Spotify behavioral signals (optional, one-time)

Only needed if `spotify_signals` table is absent or you want to refresh it.

```bash
python engine.py signals --db data/music.db --input data/StreamingHistory*.json
```

With multiple history files:
```bash
python engine.py signals --db data/music.db --input data/StreamingHistory0.json data/StreamingHistory1.json
```

Expected output: ~540 tracks, 217 sessions, completion_source=relative

When signals are present, `analyze` adds a `spotify` sub-dict to each track record with
`skip_rate`, `completion_mean_ratio`, `opener_rate`, `within_session_repeats`. If absent,
those fields are `null` -- analysis is never gated on Spotify data.

## Step 2b -- Run analysis

```bash
# Full catalog
python engine.py analyze --db data/music.db --out analysis.json

# Single artist
python engine.py analyze --db data/music.db --artist "Radiohead" --out radiohead.json

# Spring playlist (50 tracks)
python engine.py analyze --db data/music.db --months 3,4,5 --n 50 --out spring.json

# Tune thresholds
python engine.py analyze --db data/music.db --gap-days 180 --epoch-min-plays 30 --min-plays 5

# Historical reference date
python engine.py analyze --db data/music.db --refdate 2020-01-01 --out retro.json
```

### Output keys (`analyze`)

| Key | Description |
|---|---|
| `meta` | db path, ref_date, artist_filter, months_filter, has_spotify, config |
| `overview` | total_plays, unique_artists/tracks/albums, span_days, span_years, avg_plays_per_day |
| `clock` | plays_by_hour, peak_hour, late_night_pct |
| `seasonal` | per-season play counts and percentages |
| `top_tracks` | top 25 by play count |
| `top_albums` | top 25 by play count |
| `top_artists` | top 25 by play count |
| `top_target_tracks` | top 25 tracks in target months (only when --months set) |
| `year_by_year` | per year: total_plays, top_artist, obsessions (>10%), top_10_artists |
| `epochs` | detected high-density listening periods with play counts |
| `tracks` | per-track analysis records (see schema below) |
| `trajectory_summary` | count per trajectory type |
| `trajectory_type_stats` | avg plays/span/q1/q4/returns/ppd per type |
| `correlations` | burst_30 vs total/span/returns; gap_skew vs span/returns |
| `discovery_latency` | tracks first heard >60 days after their album's debut |
| `ltp_tracks` | all LTP-qualifying tracks (long-delay true positives) |
| `playlist` | scored/ranked tracks (only when --n set) |

### Per-track schema (`tracks` array)

```json
{
  "artist": "...",
  "track": "...",
  "album": "...",
  "total_plays": 47,
  "span_days": 3200,
  "first_play": "2010-03-12",
  "last_play": "2025-11-01",
  "days_since": 152,
  "burst_ratio_30": 0.21,
  "burst_ratio_90": 0.38,
  "q1": 0.40,
  "q4": 0.27,
  "long_returns": 5,
  "gap_skew": 1.24,
  "rediscoveries": [
    {"gap_days": 412, "return_date": "2013-05-01", "return_year": 2013, "cluster_size": 4}
  ],
  "trajectory": "PERENNIAL_RETURN",
  "session": {
    "distinct_days": 38,
    "plays_per_active_day": 1.24,
    "repeat_rate": 0.06,
    "late_night_pct": 0.44
  },
  "epoch_rates": {
    "E1": {"plays": 3, "rate_per_1000": 0.37},
    "E2": {"plays": 21, "rate_per_1000": 0.45}
  },
  "ltp": {
    "long_returns": 5,
    "max_gap_days": 412,
    "days_since": 152,
    "target_season_ratio": 0.47,
    "lifespan_days": 3200
  },
  "spotify": {
    "skip_rate": 0.05,
    "completion_mean_ratio": 0.91,
    "opener_rate": 0.12,
    "within_session_repeats": 2
  }
}
```

`ltp` is `null` if the track doesn't qualify. `spotify` fields are `null` if
`spotify_signals` table is absent.

### Trajectory types

| Type | Signature |
|---|---|
| `FLASH_BINGE` | >= 50% of plays in first 30 days |
| `DISCOVERY_HEAVY` | >= 60% of plays in first 90 days |
| `FRONT_LOADED` | >= 65% of plays in first quarter of lifespan |
| `PERENNIAL_RETURN` | >= 3 rediscoveries AND >= 15% of plays in final quarter |
| `SLOW_BURN` | q4 >= 80% of q1 AND >= 2 rediscoveries (gradual deepening) |
| `REDISCOVERY` | >= 2 rediscoveries (periodic revival, not sustained) |
| `DIFFUSE` | no dominant pattern |

Priority is checked in order: FLASH_BINGE first, DIFFUSE last.

### Key methodological concepts

**Long-delay true positive (LTP)**: A track you reliably return to after extended absence.
Qualifies if it has >= `--min-returns` (default 2) gaps of >= `--gap-days` (default 180) days
between consecutive plays, and >= `--min-plays` (default 5) total plays.

**Epoch detection**: Contiguous months where the full corpus has >= `--epoch-min-plays`
(default 30) plays/month form a listening epoch. Always computed from the full corpus, even
in `--artist` mode. Per-track `epoch_rates` show plays-per-1000-corpus-plays in each epoch.

**Burst ratio**: Fraction of total plays occurring within the first 30 (or 90) days of
a track's lifespan. `burst_ratio_30 = 0.80` means 80% of all listens were in the first month.

**Gap skew**: Pearson skewness of the gap distribution (3 * (mean - median) / std). Positive
skew = most gaps are short, with occasional long ones (binge + rare returns). Negative skew =
gaps are unusually consistent.

**Rediscovery**: A gap >= `--gap-days` days followed by a return. `cluster_size` is how many
plays occurred within 30 days of the return.

**Quartile distribution (q1/q4)**: Fraction of plays in the first/last quarter of the track's
lifespan. `q1=0.70` = front-loaded. `q4=0.40` = sustained recent interest.

**Season weight in playlist scoring**: When `--months` is provided, `target_season_ratio`
(fraction of plays in target months) contributes 30% of the playlist score. When `--months`
is omitted, that weight is redistributed proportionally to the other components.

## Step 3 -- Interpreting results

### Listening autobiography

- `overview`: span, volume, avg engagement
- `year_by_year`: obsessions (artists > 10% of that year's plays) reveal phase shifts
- `epochs`: high-density listening periods; gap between epochs = life transitions
- `clock` + `seasonal`: listening context (late night = focused listening vs. background)

### Trajectory analysis

Read `trajectory_summary` first for the catalog-level distribution, then `trajectory_type_stats`
for averages per type.

For a single artist (`--artist`), focus on:
- Which trajectory types dominate (perennial vs front-loaded = deep catalog vs. album cycles)
- `discovery_latency`: deep cuts found long after initial album exposure
- `correlations`: whether early binges predict longevity (negative `burst30_vs_span` = binge
  tracks tend not to last; positive = sustained engagement follows initial bursts)

### Playlist output

With `--n 50 --months 3,4,5`, the playlist is filtered to LTP tracks that:
- Have been rested >= `--rest-min-days` (default 45) days
- Have >= `--season-ratio-min` (default 0.30) of plays in target months
- Score highest on: returns (35%), season affinity (30%), depth (20%), rest (15%)
- Capped at `--max-per-artist` (default 4) tracks per artist

## Tunable thresholds

### consolidate.py (in script header)

| Constant | Default | Effect |
|---|---|---|
| `MERGE_WINDOW_SEC` | 300 | ±5 min window for Last.fm/Spotify merge matching |
| `SKIP_MS_THRESHOLD` | 30000 | Spotify plays under 30s marked as skips |

### engine.py analyze (CLI flags)

| Flag | Default | Effect |
|---|---|---|
| `--gap-days` | 180 | Gap threshold for LTP detection and rediscovery tagging |
| `--min-plays` | 5 | Minimum plays for a track to appear in per-track analysis |
| `--min-returns` | 2 | Minimum long-gap returns to qualify as LTP |
| `--epoch-min-plays` | 30 | Monthly plays threshold for epoch detection |
| `--rest-min-days` | 45 | Minimum days since last play for playlist inclusion |
| `--season-ratio-min` | 0.30 | Minimum fraction of plays in target months for playlist |
| `--max-per-artist` | 4 | Max tracks per artist in playlist output |
| `--refdate` | today | Reference date for days_since, burst ratios |
| `--months` | (none) | Target months e.g. `3,4,5` for spring; omit for season-agnostic run |

### engine.py signals (CLI flags)

| Flag | Default | Effect |
|---|---|---|
| `--min-plays` | 2 | Min Spotify plays for a track to appear in signals |
| `--session-gap-minutes` | 30 | Gap that defines session boundary |

## Expected runtime

| Operation | Time |
|---|---|
| `consolidate.py` (full rebuild) | ~5s |
| `engine.py signals` | <1s |
| `engine.py analyze` (full catalog, 84k plays) | ~15s |
| `engine.py analyze --artist "..."` | ~5s |
