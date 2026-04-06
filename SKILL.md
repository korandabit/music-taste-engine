---
name: music-data-engine
description: >
  Behavioral music analysis from Last.fm/Spotify history: trajectory
  classification (binge vs perennial vs rediscovery), long-delay true
  positives, epoch-normalized engagement, behavioral playlist scoring.
  Trigger on: listening habits, music taste, favorite artists/tracks,
  playlists, listening trends, "what do I actually love".
  Tool calls — full analysis: `engine.py analyze --db data/music.db --out
  out.json`. Single artist: add `--artist "Radiohead"`. Spring seasonal: add
  `--months 3,4,5`. Playlist (run profile first): `engine.py profile --db
  data/music.db` then `engine.py playlist --db data/music.db --n 50 --months
  3,4,5 --energy high --context "road trip"`. Playlist supports `--min-rest`,
  `--max-skip-rate`, `--require-saved`, `--max-per-artist`. Spotify signals
  refresh (rare): `engine.py signals --db data/music.db --spotify-dir data`.
---

# Music Data Engine -- Skill Instructions

A behavioral music analysis engine. Unlike streaming-service dashboards that
show top tracks and play counts, this tool classifies *how* you listen —
identifying trajectory archetypes (binge vs. perennial vs. rediscovery),
long-delay true positives (tracks you reliably return to after months of
absence), and epoch-normalized engagement patterns across years of history.
Playlists are scored on your own behavioral signals, not collaborative
filtering or audio features.

The bundle ships a pre-built `music.db`. Users analyzing their own data
rebuild this database from Last.fm and/or Spotify exports using
`consolidate.py` (see "Getting your data" below).

## Bundle contents

| File | Purpose |
|---|---|
| `music.db` | Pre-built SQLite database — all play history, signals, library, playlists |
| `engine.py` | Analysis engine (subcommands: `analyze`, `profile`, `playlist`, `signals`) |
| `consolidate.py` | Upstream ingest script (only needed if rebuilding music.db from raw exports) |

Raw source files (Last.fm CSV, Spotify JSON exports) are **not included** — they
were consumed during the upstream build step. `consolidate.py` is bundled for
reference and for users who want to rebuild the database from their own exports.

## Getting your data

To analyze your own listening history, you need at least one of the following
exports. More data = richer analysis; the tool works best with 5+ years of
history but produces useful results from 2+ years.

**Spotify (recommended starting point):**
1. Go to spotify.com/account → Privacy settings → "Request your data"
2. Select **Extended Streaming History** (not the basic "Account data" option —
   that only covers the last year). Spotify sends a download link within 5–30 days.
3. The download contains two folders: `Spotify Extended Streaming History/`
   (the play-by-play JSON files) and `Spotify Account Data/` (library, playlists).

**Last.fm (adds depth if you've been scrobbling):**
1. Export via lastfm-to-csv or Benjamin Benben's exporter (search "last.fm
   export CSV"). The CSV should have columns: `artist, album, track, date`.
2. Last.fm data extends the timeline and provides album info that Spotify's
   standard export lacks.

**Spotify-only mode:** If you only have Spotify data, consolidate handles it:
```bash
python consolidate.py --spotify-dir "Spotify Extended Streaming History" \
  --meta-dir "Spotify Account Data" --out music.db
```
All analysis features work. Trajectory classification and LTP detection are
fully functional. The only gap: no Last.fm album metadata on older plays, so
`top_albums` may be thinner for pre-2017 history.

**Corpus size expectations:**

| Corpus | What works well | What's thin |
|---|---|---|
| 10+ years, 50k+ plays | Everything — trajectory, epochs, LTP, deep rediscovery patterns | — |
| 5–10 years, 20k–50k plays | Trajectory, LTP, seasonal analysis, playlists | Epoch detection may find fewer epochs |
| 2–5 years, 5k–20k plays | Basic trajectory, playlists, top-N, clock/seasonal | LTP needs long gaps → fewer qualifiers; raise `--min-plays` to 3 |
| < 2 years or < 5k plays | Top-N, clock/seasonal, playlist (with relaxed filters) | Trajectory types collapse toward DIFFUSE; LTP effectively disabled |

## Quick reference

| Goal | Command |
|---|---|
| Full listening autobiography + trajectory | `engine.py analyze` |
| Standalone markdown report | `engine.py analyze --summary report.md` |
| Artist deep-dive | `engine.py analyze --artist "Name"` |
| Corpus feasibility map (run before playlist) | `engine.py profile` |
| Zero-friction playlist → tuneyourmusic | `engine.py playlist` |

All commands take `--db music.db` (the pre-built database in the bundle).

**Upstream (requires raw exports not in bundle):**

| Goal | Command |
|---|---|
| Rebuild database from raw exports | `consolidate.py` |
| Add/refresh Spotify behavioral signals | `engine.py signals` |

## Step 0 -- Consolidate (upstream — raw exports not in bundle)

The bundled `music.db` is already built. This section documents how it was
created, for users who want to rebuild from their own exports.

**Spotify-only** (most common for new users):
```bash
# Standard export (last ~1 year)
python consolidate.py --spotify-dir /path/to/spotify/ --out music.db

# Extended Streaming History (full history — recommended)
python consolidate.py \
  --spotify-dir "path/to/Spotify Extended Streaming History" \
  --meta-dir "path/to/Spotify Account Data" \
  --out music.db
```

**Spotify + Last.fm** (adds timeline depth and album metadata):
```bash
# Standard Spotify export
python consolidate.py --csv lastfm_export.csv --spotify-dir /path/to/spotify/ --out music.db

# Extended Streaming History
python consolidate.py \
  --csv lastfm_export.csv \
  --spotify-dir "path/to/Spotify Extended Streaming History" \
  --meta-dir "path/to/Spotify Account Data" \
  --out music.db
```

`--spotify-dir` globs both `StreamingHistory*.json` (standard) and
`Streaming_History_Audio_*.json` (extended) automatically.
`--meta-dir` is where `YourLibrary.json` and `Playlist1.json` live.
Defaults to `--spotify-dir` when omitted.

Current corpus stats:
```
82,384 plays (Last.fm)
55,980 plays (Spotify Extended, 13 files, 2014–2026)
lastfm: 65,390 | lastfm+spotify: 16,994 | spotify: 38,986
total: 121,370
2,440 saved tracks, 108 saved albums
5,947 playlist items
```

### music.db schema

**`plays`** (written by consolidate.py):
```sql
id, source, ts, artist, album, track, ms_played, is_skip
```
`source` is one of `lastfm`, `spotify`, `lastfm+spotify`.
`ms_played` and `is_skip` are populated only on Spotify-sourced rows.
Extended Streaming History provides `album` on Spotify rows; standard export does not.

**`spotify_signals`** (written by `engine.py signals`):
```sql
artist, track, total_plays, total_ms_played, first_play, last_play, span_days,
completion_mean_ratio, completion_source, skip_count, skip_rate, full_listen_rate,
opener_count, closer_count, mid_count, opener_rate,
within_session_repeats, max_repeats_in_session, sessions_with_repeat,
peak_hour, late_night_pct, hour_distribution,
plays_first_30d, plays_last_30d, burst_ratio_30
```

**`library_tracks`, `library_albums`, `playlists`**: Spotify saved items (from `YourLibrary.json` / `Playlist1.json`).

### Querying music.db

```python
import sqlite3, json
con = sqlite3.connect("music.db")
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

Always passes — stdlib only, no pip installs required.

## Step 2a -- Spotify behavioral signals (upstream — raw JSON not in bundle)

The bundled `music.db` already contains a populated `spotify_signals` table.
This section documents how signals are computed, for users who want to refresh
from their own Spotify JSON exports.

```bash
# Extended Streaming History (pass all per-year files)
python engine.py signals --db music.db \
  --input path/to/Streaming_History_Audio_2014.json \
          path/to/Streaming_History_Audio_2015.json ...

# Standard export
python engine.py signals --db music.db --input path/to/StreamingHistory*.json
```

Both standard (`endTime` / `artistName` / `msPlayed`) and extended
(`ts` / `master_metadata_*` / `ms_played` / `skipped`) formats are handled automatically.
Podcasts, audiobooks, and incognito plays are filtered out of the extended format.

Current corpus signals (Extended Streaming History 2014–2026):
```
55,980 plays | 3,800 sessions | 7,739 tracks | completion_source=relative
```

When signals are present, `analyze` and `playlist` add a `spotify` sub-dict to each
track record with `skip_rate`, `completion_mean_ratio`, `opener_rate`,
`within_session_repeats`. If absent, those fields are `null` — analysis is never gated
on Spotify data.

## Step 2b -- Run analysis (primary workflow)

```bash
# Full catalog
python engine.py analyze --db music.db --out analysis.json

# Single artist
python engine.py analyze --db music.db --artist "Radiohead" --out radiohead.json

# Spring playlist via analyze (LTP-gated, season-weighted)
python engine.py analyze --db music.db --months 3,4,5 --n 50 --out spring.json

# Tune thresholds
python engine.py analyze --db music.db --gap-days 180 --epoch-min-plays 30 --min-plays 5

# Historical reference date
python engine.py analyze --db music.db --refdate 2020-01-01 --out retro.json

# Standalone markdown report (no Claude Code needed)
python engine.py analyze --db music.db --summary my_listening_report.md
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
  },
  "saved": true
}
```

`ltp` is `null` if the track doesn't qualify. `spotify` fields are `null` if
`spotify_signals` table is absent. `saved` is `true` if the track is in `library_tracks`.

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

These concepts are the analytical core of the engine — they distinguish it
from play-count dashboards and surface-level listening stats.

**Long-delay true positive (LTP)**: A track you reliably return to after extended absence.
Qualifies if it has >= `--min-returns` (default 2) gaps of >= `--gap-days` (default 180) days
between consecutive plays, and >= `--min-plays` (default 5) total plays. This is a different
signal from "most played" — a track with 200 plays in one month is not an LTP, but a track
with 30 plays spread across five returns over eight years is. LTP detection answers: *what
do you genuinely love vs. what did you merely binge?*

**Trajectory classification**: Each track is assigned one of seven behavioral archetypes
(see Trajectory types above) based on burst ratios, quartile distribution, and return
patterns. The taxonomy captures *how* engagement unfolds over time — not just how much.
No consumer music tool currently provides this.

**Epoch detection**: Contiguous months where the full corpus has >= `--epoch-min-plays`
(default 30) plays/month form a listening epoch. Always computed from the full corpus, even
in `--artist` mode. Per-track `epoch_rates` show plays-per-1000-corpus-plays in each epoch.
This normalizes for the fact that people listen in phases — a track getting 5 plays in a
low-activity month means more than 5 plays in a 500-play month.

**Burst ratio**: Fraction of total plays occurring within the first 30 (or 90) days of
a track's lifespan. `burst_ratio_30 = 0.80` means 80% of all listens were in the first month.

**Gap skew**: Pearson skewness of the gap distribution (3 * (mean - median) / std). Positive
skew = most gaps are short, with occasional long ones (binge + rare returns). Negative skew =
gaps are unusually consistent.

**Rediscovery**: A gap >= `--gap-days` days followed by a return. `cluster_size` is how many
plays occurred within 30 days of the return.

**Quartile distribution (q1/q4)**: Fraction of plays in the first/last quarter of the track's
lifespan. `q1=0.70` = front-loaded. `q4=0.40` = sustained recent interest.

**`saved` flag**: `true` if the track appears in `library_tracks` (Spotify saved/liked).
Indicates explicit preference; used as a scoring multiplier in `playlist`.

**Behavioral playlist scoring**: Unlike collaborative filtering ("users like you also liked")
or audio-feature matching ("similar BPM/valence"), playlists here are scored on *your own*
longitudinal listening patterns — periodicity of returns, completion rate, rest time, and
trajectory archetype. The playlist answers: *what should I hear again based on how I
actually listen?*

## Step 2c -- Profile (corpus feasibility map)

Run `profile` before `playlist` to understand what parameter combinations are viable
for this specific corpus. The output tells the calling LLM how large each candidate pool
is, where the skip signal is meaningful, and which seasons have strong representation.

```bash
python engine.py profile --db music.db
# or save to file
python engine.py profile --db music.db --out profile.json
```

### Profile output schema

```json
{
  "ref_date": "2026-04-04",
  "corpus": {
    "tracks_analyzed": 6208,
    "tracks_saved": 1896,
    "artists": 1304,
    "artists_multi_track": 501
  },
  "candidate_pools": {
    "rest_14d":  {"all": 6110, "saved": 1852},
    "rest_30d":  {"all": 6064, "saved": 1831},
    "rest_90d":  {"all": 5770, "saved": 1708},
    "rest_180d": {"all": 5412, "saved": 1587},
    "rest_365d": {"all": 4826, "saved": 1386}
  },
  "trajectory_distribution": {
    "DIFFUSE": 3397, "FRONT_LOADED": 1246,
    "FLASH_BINGE": 1143, "DISCOVERY_HEAVY": 422
  },
  "skip_signal": {
    "tracks_with_signal": 3874,
    "tracks_total": 6208,
    "coverage_pct": 62.4,
    "p25": 0.0, "p50": 0.17, "p75": 0.29
  },
  "skip_cutoffs": {
    "max_30pct": 3045,
    "max_50pct": 3874,
    "max_70pct": 3874
  },
  "season_affinity": {
    "spring": {"all": 3525, "saved": 1178},
    "summer": {"all": 3741, "saved": 1113},
    "fall":   {"all": 3339, "saved": 925},
    "winter": {"all": 2668, "saved": 906}
  },
  "playlist_guidance": {
    "recommended_min_rest": 14,
    "saved_viable_at_rest_90d": true,
    "skip_signal_meaningful": true,
    "strongest_season": "summer"
  }
}
```

**How to read `playlist_guidance`:**
- `recommended_min_rest`: lowest threshold where `all` pool >= 50 tracks — safe floor for `--min-rest`
- `saved_viable_at_rest_90d`: if `true`, `--require-saved --min-rest 90` will yield a full playlist
- `skip_signal_meaningful`: if `true`, `--max-skip-rate` is a meaningful filter (>= 100 tracks have signal)
- `strongest_season`: season with most tracks meeting 20% affinity threshold — default best choice for `--months`

## Step 2d -- Playlist

Produces a scored, ready-to-transfer tracklist. Reads only from `music.db`.

```bash
# Default: 20 tracks, medium energy, 30d rest minimum
python engine.py playlist --db music.db

# With context label and energy profile
python engine.py playlist --db music.db --n 25 --context "Sunday drive" --energy low

# Saved tracks only, rested 6 months
python engine.py playlist --db music.db --n 20 --require-saved --min-rest 180

# Season-filtered
python engine.py playlist --db music.db --n 30 --months 3,4,5 --context "spring"

# Save JSON alongside stdout
python engine.py playlist --db music.db --n 20 --out playlist.json
```

### Output format

```
=== SUNDAY DRIVE (20 tracks) ===

The National — Mistaken for Strangers
Grizzly Bear — A Simple Answer
...

────────────────────────────────────────────────────
Transfer to Spotify / Apple Music / Tidal / etc:
  https://www.tuneyourmusic.com/transfer

Paste the list above, pick your destination, go.
────────────────────────────────────────────────────
```

### Scoring model

Candidate pool: all tracks passing hard filters (`--min-rest`, `--max-skip-rate`,
`--require-saved`, `--season-ratio-min` when `--months` set). No LTP gate.

Score = `(w_periodicity × periodicity + w_engagement × engagement + w_depth × depth + w_rest × rest)`
`× skip_multiplier × saved_multiplier × trajectory_weight`

| Component | Signal | Normalisation |
|---|---|---|
| `periodicity` | `long_returns / span_years` | caps at 5 returns/year |
| `engagement` | `completion_mean_ratio` if available, else `repeat_rate × 2` | 0–1 |
| `depth` | `log(total_plays)` | caps at log(300) |
| `rest` | `days_since` | caps at 730 days |

`skip_multiplier` = `1 - skip_rate × 0.6` (soft penalty; only when signal available).
`saved_multiplier` = 1.15 for library-saved tracks.

**Energy profiles** (set component weights):

| Profile | periodicity | engagement | depth | rest |
|---|---|---|---|---|
| `low` | 0.40 | 0.15 | 0.20 | 0.25 |
| `medium` | 0.30 | 0.25 | 0.20 | 0.25 |
| `high` | 0.15 | 0.35 | 0.30 | 0.20 |

**Trajectory multipliers** (applied after weighted sum):

| Trajectory | Multiplier |
|---|---|
| `PERENNIAL_RETURN` | 1.30 |
| `REDISCOVERY` | 1.20 |
| `SLOW_BURN` | 1.15 |
| `DIFFUSE` | 0.90 |
| `DISCOVERY_HEAVY` | 0.85 |
| `FRONT_LOADED` | 0.80 |
| `FLASH_BINGE` | 0.65 |

### Input hook space (operator guide)

Map user requests to `playlist` parameters using `profile` output as ground truth
for what's viable.

| User says | Parameter |
|---|---|
| "chill", "background", "long drive", "Sunday morning" | `--energy low` |
| "pumped", "workout", "high energy" | `--energy high` |
| "stuff I haven't heard in a while" | `--min-rest 180` |
| "my favorites", "saved tracks", "stuff I love" | `--require-saved` |
| "spring / summer / fall / winter mood" | `--months 3,4,5` / `6,7,8` / `9,10,11` / `12,1,2` |
| playlist size | `--n` |
| free-form description | `--context "..."` (passes through to output header) |

**Before choosing `--min-rest`**: check `candidate_pools` in `profile`. If user wants 25
tracks and `rest_180d.all` is only 30, drop to `rest_90d` or omit the constraint.

**Before using `--require-saved`**: check `saved_viable_at_rest_90d`. If `false`, warn
the user that the saved pool at 90d rest is thin (<20 tracks) and suggest dropping the filter.

**For `--max-skip-rate`**: use `skip_signal.p75` from `profile` as the ceiling.
Setting it above p75 is effectively no filter. Setting it at p50 keeps only the
lower half of skip-rate tracks.

## Step 3 -- Interpreting results

### profile → playlist workflow

The recommended two-step flow when generating a playlist from a user request:

1. Run `engine.py profile --db music.db` — read `candidate_pools`,
   `skip_signal`, `season_affinity`, and `playlist_guidance`
2. Map the user's request to parameters using the input hook space above
3. Validate parameter viability against pool sizes (adjust `--n` or relax filters if pool is thin)
4. Run `engine.py playlist` with chosen parameters
5. Return the inline tracklist and the tuneyourmusic.com/transfer link to the user

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

### Playlist output (via `analyze --n`)

With `--n 50 --months 3,4,5`, the playlist is filtered to LTP tracks that:
- Have been rested >= `--rest-min-days` (default 45) days
- Have >= `--season-ratio-min` (default 0.30) of plays in target months
- Score highest on: returns (35%), season affinity (30%), depth (20%), rest (15%)
- Capped at `--max-per-artist` (default 4) tracks per artist

For zero-friction real-world use, prefer the `playlist` subcommand over `analyze --n`.

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
| `--summary` | (none) | Write a standalone markdown report to this path |

**Auto-calibration:** When the corpus has < 20k plays or < 3 year span, `analyze`
automatically lowers `--min-plays`, `--min-returns`, and `--epoch-min-plays` so that
trajectory classification and LTP detection remain meaningful on thinner data. Explicitly
set values are never overridden. The output `meta.corpus_calibration` key shows what was
adjusted and why (`null` when no calibration needed).

**Summary report:** `--summary report.md` produces a human-readable markdown file with
overview table, epoch timeline, trajectory distribution with type glossary,
binge→outcome correlation interpretation, top LTP tracks, and notable rediscoveries.
Useful for sharing results without requiring Claude Code or JSON tooling.

### engine.py playlist (CLI flags)

| Flag | Default | Effect |
|---|---|---|
| `--n` | 20 | Number of tracks |
| `--context` | (none) | Free-text label shown in output header |
| `--energy` | medium | Scoring profile: `high` / `medium` / `low` |
| `--months` | (none) | Season filter e.g. `3,4,5` |
| `--min-rest` | 30 | Min days since last play |
| `--max-skip-rate` | 0.70 | Exclude tracks above this skip rate (use profile p75 as guide) |
| `--require-saved` | false | Only include library-saved tracks |
| `--max-per-artist` | 3 | Max tracks per artist |
| `--season-ratio-min` | 0.20 | Min target-season ratio when `--months` set |
| `--min-plays` | 5 | Min plays to consider a track |
| `--refdate` | today | Reference date |
| `--out` | (none) | Optional JSON output path |

### engine.py profile (CLI flags)

| Flag | Default | Effect |
|---|---|---|
| `--min-plays` | 5 | Min plays for a track to be included in feasibility counts |
| `--refdate` | today | Reference date |
| `--out` | (none) | Optional JSON output path (otherwise prints to stdout) |

### engine.py signals (CLI flags)

| Flag | Default | Effect |
|---|---|---|
| `--min-plays` | 2 | Min Spotify plays for a track to appear in signals |
| `--session-gap-minutes` | 30 | Gap that defines session boundary |

## Expected runtime

| Operation | Time |
|---|---|
| `consolidate.py` (full rebuild, 121k plays) | ~10s |
| `engine.py signals` (55k plays, 13 files) | ~3s |
| `engine.py profile` | ~8s |
| `engine.py playlist` | ~8s |
| `engine.py analyze` (full catalog, 121k plays) | ~25s |
| `engine.py analyze --artist "..."` | ~5s |
