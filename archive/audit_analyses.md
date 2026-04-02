# Analyses & Metrics Audit

Ordered epistemically: raw data → per-record derivations → first aggregations → second-order metrics → scoring/classification → cross-track analysis.

---

## Layer 0 — Actual input to each script

`music.db` exists and is the intended unified store, but **no analysis script reads from it**. The data flow is:

```
edgarturtleblot.csv ──────────────────────────────► lastfm_taste_engine.py
                    ──────────────────────────────► lastfm_trajectory_engine.py
                    ──┐
StreamingHistory*.json ─┤── consolidate.py ──► music.db  (write-only from analysis perspective)
YourLibrary.json ───────┤
Playlist1.json ─────────┘

StreamingHistory*.json ──── spotify_signal_engine.py ──► music.db:spotify_signals
```

`music.db` is written by two scripts and never read by any. The analysis layer runs entirely off the raw CSV and JSON exports. This means `spotify_signal_engine.py`'s output (`spotify_signals` table) is currently disconnected from the analysis pipeline.

---

### `edgarturtleblot.csv` — input to taste_engine and trajectory_engine

No header row. Fields, comma-delimited with max 3 splits:

```
artist, album, track, "DD Mon YYYY HH:MM"
```

- `artist`, `album`, `track`: strings, may be empty or quoted
- date: fixed format `"%d %b %Y %H:%M"` — no timezone, no seconds
- ~82k rows

Known artifacts: pre-2005 rows from Last.fm's epoch default (Unix 0 → 1970-01-01). All dropped at parse time (`dt.year < 2005`).

### `StreamingHistory*.json` — input to spotify_signal_engine (and consolidate)

Array of objects:

```json
{ "endTime": "YYYY-MM-DD HH:MM", "artistName": "", "trackName": "", "msPlayed": 0 }
```

- `endTime`: minute-resolution, no timezone
- `msPlayed`: integer milliseconds — only field capturing play duration
- No `albumName`, no track identifier beyond string name
- ~2,952 rows (one export file)

### `YourLibrary.json`, `Playlist1.json` — input to consolidate only

Static reference data; no timestamps on items. Written to `library_tracks`, `library_albums`, `playlists` tables in `music.db`. Not used by any analysis script.

---

## Layer 1 — Parsing decisions and cleaning

### Date parsing
- Last.fm: `datetime.strptime(raw, "%d %b %Y %H:%M")` — ValueError rows silently dropped
- Spotify: `datetime.strptime(endTime, "%Y-%m-%d %H:%M")` — ValueError rows silently dropped
- Neither source has timezone info; all datetimes are naive and treated as local time

### Pre-2005 drop
Applied to Last.fm only. Assumption: Last.fm epoch artifact rows cluster before 2005. Any real scrobbles before 2005 are also dropped.

### Spotify skip detection (consolidate.py)
`is_skip = 1 if ms_played < 30_000 else 0`
Applied at load time, before merge. Hard 30-second threshold — no track-length context.

### Last.fm + Spotify merge (consolidate.py)
Attempts to join each Spotify play to a Last.fm scrobble:
- **Key**: `(artist.lower(), track.lower())` — exact string match after lowercasing. No fuzzy matching; "(Remastered)" suffixes or variant spellings will not match.
- **Window**: `abs(spotify_ts - lastfm_ts) < 300 seconds` (±5 min)
- **Scope**: Last.fm plays indexed only for years overlapping the Spotify date range
- **Assignment**: closest-gap candidate wins; each Last.fm row matched at most once (greedy)
- **Result**: matched Last.fm rows gain `ms_played` and `is_skip`; matched Spotify rows are dropped; unmatched from both sources kept as separate rows
- **Source tag**: `"lastfm"`, `"lastfm+spotify"`, or `"spotify"`

---

## Layer 2 — Per-record derived fields (`enrich`, taste engine only)

Applied to every play dict after loading:

| Field | Computation |
|---|---|
| `days_ago` | `(ref_date - play.date).days` |
| `month` | `play.date.month` |
| `year` | `play.date.year` |
| `hour` | `play.date.hour` |
| `season` | lookup: months 3–5 → Spring, 6–8 → Summer, 9–11 → Fall, 12/1/2 → Winter |
| `is_spring` | `month in {3, 4, 5}` |
| `is_late_night` | `hour in {22, 23, 0, 1, 2, 3}` |

**`ref_date`**: `max(play.date)` across all plays, unless `--refdate` overridden. Consequence: `days_ago` is correct only relative to the most recent scrobble. A CSV that is months old will inflate all `days_ago` values.

Trajectory engine does not call `enrich`; it derives equivalent fields inline during track analysis.

---

## Layer 3 — First aggregations (grouping)

These are the base groupings everything else is built from.

### Per-track date list
```
track_map: (artist, track) → [datetime, ...]  sorted ascending
```
Used by: LTP detection, trajectory analysis, all gap metrics.

### Per-period play counts
```
by_hour:   hour (0–23)         → count
by_season: season (string)     → count
by_year:   year                → [plays]
by_artist: artist              → count
by_album:  (artist, album)     → count
by_track:  (artist, track)     → count
```
Used by: temporal overview, top lists, year-by-year obsessions.

### Monthly play counts (trajectory engine)
```
monthly: (year, month) → count
```
Used by: epoch detection.

---

## Layer 4 — Simple aggregate metrics (built on layer 3)

### Temporal overview
| Metric | Computation |
|---|---|
| `span_days` | `(ref_date - first_play).days` |
| `span_years` | `span_days / 365.25` |
| `avg_plays_per_day` | `total_plays / span_days` |
| `unique_tracks` | `len(distinct (artist, track) tuples)` |
| `unique_albums` | `len(distinct (artist, album) tuples)` |

### Listening clock
| Metric | Computation |
|---|---|
| `peak_hour` | `argmax(by_hour)` |
| `late_night_pct` | `count(hour in {22,23,0,1,2,3}) / total * 100` |

### Seasonal distribution
`pct = season_count / total * 100` per season.

### Top-N lists
Frequency sort of `by_track`, `by_album`, `by_artist`. `top_spring_tracks`: filters plays to `is_spring == True` first, then counts.

### Year-by-year obsessions
Per year: any artist where `artist_plays / year_total > 0.10`. Threshold is fixed; a year with very few total plays will produce many "obsessions."

---

## Layer 5 — Gap analysis (built on layer 3 track date lists)

Per-track, requires sorted `[datetime, ...]`:

| Metric | Computation |
|---|---|
| `gaps` | `[(dates[i+1] - dates[i]).days for i in range(len-1)]` |
| `long_returns` | `count(g >= 180)` in gaps |
| `max_gap_days` | `max(gaps)` |
| `lifespan_days` | `(dates[-1] - dates[0]).days` |
| `days_since` | `(ref_date - dates[-1]).days` |
| `spring_plays` | `count(d.month in {3,4,5})` over all play dates |
| `spring_ratio` | `spring_plays / total_plays` |

**Assumption baked in**: a 180-day gap is semantically "absence." This threshold is not normalized for the listener's overall activity level. A 6-month gap during a year the user barely listened to anything is treated the same as a 6-month gap in a dense listening year.

---

## Layer 6 — Temporal distribution metrics (built on layer 3 date lists + layer 5 span)

Per-track, requires `dates`, `first_play`, `span_days`, `total`:

| Metric | Computation |
|---|---|
| `burst_30` | `count(d where (d - first).days <= 30)` |
| `burst_90` | `count(d where (d - first).days <= 90)` |
| `burst_ratio_30/90` | `burst_N / total` |
| `q1` | `count(d <= first + span*0.25) / total` |
| `q4` | `count(d >= first + span*0.75) / total` |

**Q1/Q4 boundary assumption**: span is measured from `first` to `last` play, not wall-clock calendar time. A track played 10 times in 2010 and then once in 2020 has `span = 3650d`; the Q4 boundary (`span * 0.75 = 2737d` after first) is in 2017. Any plays in 2018–2020 land in Q4. The quartile fractions tell you about temporal distribution within the track's own lifespan, not within your overall listening history.

### Rediscoveries (built on gap analysis + burst logic)
For each gap `> 180 days`:
```
cluster_size = count(plays within 30d of the return date)
```
Output per rediscovery: `{gap_days, return_date, return_year, cluster_size}`.

---

## Layer 7 — Epoch detection (trajectory engine, built on layer 3 monthly counts)

Contiguous months with `plays/month >= 30` are clustered into epochs. An epoch requires `>= 2` consecutive qualifying months.

**Epoch rate normalization**: for each track × epoch:
```
rate_per_1000 = (track plays in epoch / all plays in epoch) * 1000
```
Purpose: normalize for overall listening volume so a track heavily played in a low-volume epoch is not penalized relative to a dense epoch.

**Assumption**: 30 plays/month is a global constant, not scaled to listener volume. A listener who averages 500 plays/month will have far more epochs (most months qualify) than one who averages 50/month. The threshold has no self-calibrating logic.

---

## Layer 8 — Session reconstruction (spotify_signal_engine)

Requires chronologically sorted plays. Gap > `SESSION_GAP_MINUTES` (default 30) separates sessions.

Per play tagged with:
- `session_id`: integer, incrementing at each gap
- `session_position`: `"opener"` (first in session), `"mid"`, `"closer"` (last); single-play sessions → `"opener"` only
- `session_size`: total plays in that session

### Completion tagging (two modes)

**Absolute** (requires external `{Artist|Track: duration_ms}` JSON):
```
completion_ratio = min(ms_played / duration_ms, 1.0)
is_skip = 1 if ratio < 0.40
```

**Relative fallback** (default, no external data):
```
track_median = median(ms_played across all plays of that track, min 2 plays)
completion_ratio = min(ms_played / track_median, 1.0)
is_skip = 1 if ratio < 0.40
```
Single-play tracks: `completion_ratio = None`, `is_skip = None`.

**Critical assumption in relative mode**: the median is used as a proxy for track duration. This is only valid if most plays are full listens. For a heavily-skipped track, the median is depressed, which makes even short plays look "complete." The mode is self-defeating for skip-heavy tracks.

---

## Layer 9 — Per-track Spotify aggregation (built on layers 8)

Per `(artist, track)` group, requires session-tagged and completion-tagged play records:

| Signal | Computation |
|---|---|
| `total_ms_played` | `sum(ms_played)` |
| `completion_mean_ratio` | `mean(completion_ratio)` for non-null |
| `skip_rate` | `count(is_skip==1) / count(is_skip is not None)` |
| `full_listen_rate` | `count(ratio >= 0.90) / count(ratio not None)` |
| `opener_rate` | `count(session_position == "opener") / total` |
| `within_session_repeats` | `Σ max(0, session_count - 1)` per session |
| `max_repeats_in_session` | `max(session_count) - 1` |
| `sessions_with_repeat` | `count(sessions where track appears >= 2×)` |
| `late_night_pct` | `count(hour in {22,23,0,1,2,3}) / total` |
| `peak_hour` | `argmax(hour_distribution)` |
| `plays_first_30d` | `count(ts <= first_play + 30d)` |
| `plays_last_30d` | `count(ts >= refdate - 30d)` |
| `burst_ratio_30` | `plays_first_30d / total` |

**Note**: `within_session_repeats` and `same_session_repeats` are stored as separate DB columns but set to the same value — schema redundancy.

**Comparison with trajectory engine `repeat_rate`**: trajectory engine groups by `(date, hour)` bucket, not session. Two plays of the same track at the same hour on the same day count as a repeat regardless of whether they are in the same session. The two metrics are conceptually similar but not equivalent.

---

## Layer 10 — Trajectory classification (built on layers 5, 6)

Pure function on `(br30, br90, q1, q4, long_returns, n_rediscoveries)`. Priority-ordered — first matching rule wins:

| Trajectory | Condition |
|---|---|
| `FLASH_BINGE` | `br30 >= 0.50` |
| `DISCOVERY_HEAVY` | `br90 >= 0.60` |
| `PERENNIAL_RETURN` | `n_rediscoveries >= 3 AND q4 >= 0.15` |
| `SLOW_BURN` | `q4 >= q1 * 0.80 AND n_rediscoveries >= 2` |
| `FRONT_LOADED` | `q1 >= 0.65` |
| `REDISCOVERY` | `n_rediscoveries >= 2` |
| `DIFFUSE` | fallback |

**Priority order is load-bearing**: a track with `br30=0.51` and `n_rediscoveries=5` is classified `FLASH_BINGE`, not `PERENNIAL_RETURN`. The ordering embeds a judgment about which signal dominates.

**SLOW_BURN edge case**: condition is `q4 >= q1 * 0.80` — a relative comparison. A track with `q1=0.02, q4=0.02` satisfies this (`0.02 >= 0.016`). The condition does not require q4 to be meaningfully large in absolute terms, only that it is not much smaller than q1. Combined with `n_rediscoveries >= 2`, this is narrower in practice, but the relative threshold is unintuitive.

---

## Layer 11 — Playlist scoring (built on layers 4, 5)

### LTP qualification gate (taste engine)
A track enters the LTP pool if: `total_plays >= 4` AND `long_returns >= 2`.

### Playlist filter gate (applied after LTP qualification)
`days_since >= 45` AND `spring_ratio >= 0.30`

**Overfit note**: the spring filter hard-gates any track below 30% spring plays. This is only semantically appropriate if the intended use is "songs to listen to in spring." The threshold and target season are both configurable but default to spring. A catalog audit run at any other time of year will be filtered by the same spring criterion unless the constant is changed.

### Playlist score

All components normalized 0–1 against pool max before weighting:

| Component | Normalization | Weight |
|---|---|---|
| `s_returns` | `long_returns / max(long_returns in pool)` | 0.35 |
| `s_spring` | `spring_ratio` (already 0–1) | 0.30 |
| `s_depth` | `log1p(total_plays) / max(log1p(total_plays) in pool)` | 0.20 |
| `s_rest` | `min(days_since, 600) / 600` | 0.15 |

`score = Σ weight_i * s_i`

`log1p` on depth compresses the long tail of play counts. `REST_CAP = 600` means tracks rested more than ~1.6 years are treated identically for the rest component. Artist cap of 4 tracks applied greedily post-sort.

---

## Layer 12 — Cross-track analysis (built on layer 10 output sets)

### Pearson correlations (trajectory engine)
Filters to tracks with `total_plays >= 20` (hardcoded, not CLI-tunable). Requires `n >= 5` such tracks.

Computed (population formula `/ n`, not sample `/ n-1`):
- `burst_ratio_30` vs. `total_plays`
- `burst_ratio_30` vs. `span_days`
- `burst_ratio_30` vs. `long_returns`

Guard `n >= 3` inside `pearson_r` but the caller only invokes if `n >= 5`.

### Discovery latency (trajectory engine)
For each track: `delay_days = track.first_play - album_first_play_in_corpus`. Only emits records where `delay > 60 days`.

**Assumption**: uses `t["albums"][0]` — first entry in a sorted set of album names. If a track appears on multiple albums (original + compilation + remaster), the album chosen is alphabetically first, which may not be the album the user first encountered.

Also: `first_play` in track record is stored as `"%Y-%m-%d"` (date only), then parsed back without time — potential ±1 day error relative to the album's `album_first_play` which retains full timestamp precision.

### Popularity comparison (trajectory engine, optional)
Requires external `{track_name: 0–100}` JSON. Your percentile computed as rank position: `(1 - i/n) * 100` where `i` is 0-indexed descending rank by `total_plays`. Delta = `your_percentile - world_percentile`.

**Assumption**: the external 0–100 values are comparable in meaning to a percentile rank. If the external source provides raw play counts or a non-percentile score, the delta is uninterpretable.

### Trajectory type stats
`mean(total_plays, span_days, q1, q4, long_returns, ppd, repeat_rate, late_night_pct)` grouped by trajectory type. No new derivations; summary over layer-10 outputs.

---

## Cross-script notes

### Duplicate `load_csv`
taste engine and trajectory engine each define an identical `load_csv`. Not shared.

### `burst_ratio_30` definition consistency
taste engine does not compute it. trajectory engine and spotify_signal_engine both define it as `plays_in_first_30d / total` — consistent.

### Reference date handling inconsistency
| Script | Default `ref_date` |
|---|---|
| `taste_engine` | `max(play.date)` — latest scrobble |
| `trajectory_engine` | `datetime.now()` — today |
| `spotify_signal_engine` | `datetime.today()` — today |

`days_since` from taste engine and trajectory engine are not directly comparable for the same track unless the CSV's latest scrobble happens to be today.

### What the pipeline is overfit to
- **Spring season**: the LTP qualification, playlist filter, and spring_ratio score component are all oriented toward spring listening. The season constants are configurable but the default use case is "spring playlist."
- **Long-term scrobble behavior**: minimum span of 30 days and minimum 5 plays exclude tracks encountered recently or casually. The analysis describes long-term relationship with music, not current taste.
- **Last.fm-sourced data**: album data comes only from Last.fm. Spotify-only plays have no album, which breaks `compute_discovery_latency` for them.
- **Single-user corpus**: normalization (percentiles, epoch rates, obsession threshold) is always relative to the same listener's own history, not external benchmarks.
