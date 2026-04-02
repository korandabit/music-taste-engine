# Rewrite Plan: Unified Music Analysis Engine

## Decisions locked in

9. **No `playlist` subcommand.** Playlist output is `--n` on `analyze`. If `--months` is set, seasonal weight applies; if not, that component drops out. Not a separate concept.
10. **Packaging target: SKILL.md skill bundle.** Output JSON structure is the same as a tool_use schema would require, so an API-level tool_use wrapper can be layered on later without redesign. What that defers: API-level agentic chaining (model calls analyze → reads output → calls next tool programmatically) and plug-in to third-party Claude applications without manual wrapping. Not a blocker now; not foreclosed later.

1. **SQLite primary.** All analysis reads from `music.db`. Raw CSV/JSON are ingest inputs only, not analysis inputs.
2. **Single `engine.py`.** One file, subcommand CLI, functions organized by layer (parse → aggregate → metrics → classify → score). Modular-convertible by design: pure functions, explicit parameters, no global state except CLI entry point.
3. **`consolidate.py` stays separate.** It has a different lifecycle (one-time or after new exports). It becomes the ingest step: accepts `--csv` and/or `--spotify-dir`, produces `music.db`. No changes to its job, but it gets cleaned up.
4. **No taste/trajectory split.** One `analyze` subcommand (or equivalent) that produces everything. Internal functions are still distinct but not surfaced as separate commands unless there is clear functional gain.
5. **Season is a parameter, no default.** `--months 3,4,5` or `--season spring/summer/fall/winter` as shorthand. If omitted, season-dependent metrics (spring_ratio, seasonal playlist scoring) are omitted from output or marked null. No hardcoded `SPRING_MONTHS` constant.
6. **Reference date: `--refdate` everywhere, default `today`.** Consistent across all subcommands. No "max scrobble" default.
7. **Gap threshold: `--gap-days`, default 180.** Single named parameter passed explicitly through the call chain.
8. **Spotify signals are optional enrichment.** If `spotify_signals` table exists in `music.db`, analysis joins on `(artist, track)` and includes behavioral fields (skip_rate, completion_mean_ratio, opener_rate, within_session_repeats). If absent, those fields are null in output. Analysis is never gated on Spotify data being present.

---

## Architecture

### Data flow

```
[User exports]
  edgarturtleblot.csv         ──┐
  StreamingHistory*.json      ──┤──► consolidate.py ──► music.db
  YourLibrary.json            ──┤
  Playlist1.json              ──┘

  StreamingHistory*.json  ──► engine.py signals ──► music.db:spotify_signals

music.db ──► engine.py analyze ──► analysis JSON
music.db ──► engine.py playlist ──► playlist JSON
```

### `engine.py` subcommands

| Subcommand | Input | Output | Replaces |
|---|---|---|---|
| `signals` | `music.db` + `StreamingHistory*.json` | writes `spotify_signals` to `music.db` | `spotify_signal_engine.py` |
| `analyze` | `music.db` | analysis JSON | `lastfm_taste_engine.py` + `lastfm_trajectory_engine.py` |
| `playlist` | `music.db` (requires prior `analyze` or inline) | playlist JSON | playlist section of taste engine |

`analyze` produces the full output: temporal overview, top lists, year-by-year, LTP detection, per-track trajectory, epoch detection, session fingerprints, correlations, discovery latency. One JSON document. Spotify enrichment fields present if `spotify_signals` table exists.

### `engine.py` internal structure (sections in order)

```
# ── Config / constants (CLI-overridable only) ──
# ── DB helpers (connection, parameterized queries) ──
# ── Layer 1: Load from DB ──
# ── Layer 2: Per-record derived fields ──
# ── Layer 3: First aggregations ──
# ── Layer 4: Simple aggregate metrics ──
# ── Layer 5: Gap analysis ──
# ── Layer 6: Temporal distribution metrics ──
# ── Layer 7: Epoch detection ──
# ── Layer 8: Trajectory classification (pure function) ──
# ── Layer 9: LTP qualification + playlist scoring ──
# ── Layer 10: Cross-track analysis ──
# ── Layer 11: Output assembly ──
# ── CLI entry point ──
```

### `music.db` schema (consolidated view)

**`plays`** (written by `consolidate.py`):
```sql
id, source, ts, artist, album, track, ms_played, is_skip
```
source ∈ `{'lastfm', 'spotify', 'lastfm+spotify'}`

**`spotify_signals`** (written by `engine.py signals`):
```sql
artist, track, total_plays, total_ms_played, first_play, last_play, span_days,
completion_mean_ratio, completion_source, skip_count, skip_rate, full_listen_rate,
opener_count, closer_count, mid_count, opener_rate,
within_session_repeats, max_repeats_in_session, sessions_with_repeat,
peak_hour, late_night_pct, hour_distribution,
plays_first_30d, plays_last_30d, burst_ratio_30
```

**`library_tracks`, `library_albums`, `playlists`** (written by `consolidate.py`): unchanged.

### Key behavioral changes from current

| Current | New |
|---|---|
| Analysis reads CSV directly | Analysis reads `music.db:plays` |
| `spotify_signal_engine.py` output is a dead end | `engine.py signals` writes to DB; `analyze` joins it |
| `load_csv` duplicated in two scripts | Single implementation in `consolidate.py`; engine loads from DB |
| `SPRING_MONTHS` hardcoded constant | `--months` CLI arg; omit for season-agnostic run |
| `ref_date = max(scrobble)` in taste engine | `ref_date = today` everywhere, `--refdate` override |
| `W_SEASON` baked into scoring | Season weight only applied when `--months` specified |
| `DORMANT_MONTH_THRESHOLD = 30` hardcoded | `--epoch-min-plays` CLI arg, default 30 |
| `burst_ratio_30` definition inconsistent across scripts | Single implementation in Layer 6 |
| Trajectory and taste are separate scripts | Single `analyze` command, single output document |

### CLI interface

```bash
# One-time ingest
python consolidate.py --csv data/edgarturtleblot.csv --spotify-dir data/ --out data/music.db

# Spotify behavioral signals (run after consolidate, re-run when new exports added)
python engine.py signals --db data/music.db --input data/StreamingHistory*.json

# Full analysis
python engine.py analyze --db data/music.db --out analysis.json
python engine.py analyze --db data/music.db --artist "Radiohead" --out radiohead.json
python engine.py analyze --db data/music.db --months 3,4,5 --out spring_analysis.json

# Playlist (requires --months for season scoring to be meaningful)
python engine.py playlist --db data/music.db --months 3,4,5 --n 50 --out playlist.json

# Tunable thresholds
python engine.py analyze --db data/music.db --gap-days 180 --epoch-min-plays 30 --min-plays 5
```

### Output JSON structure (`analyze`)

```json
{
  "meta": { "db", "ref_date", "artist_filter", "months_filter", "config" },
  "overview": { "total_plays", "unique_artists", "unique_tracks", "span_days", "span_years", "avg_plays_per_day" },
  "clock": { "plays_by_hour", "peak_hour", "late_night_pct" },
  "seasonal": { "Spring": {"plays", "pct"}, "..." },
  "top_tracks": ["..."],
  "top_albums": ["..."],
  "top_artists": ["..."],
  "year_by_year": { "2010": { "total_plays", "top_artist", "obsessions" }, "...": "..." },
  "epochs": ["..."],
  "tracks": [
    {
      "artist", "track", "total_plays", "span_days", "first_play", "last_play", "days_since",
      "burst_ratio_30", "burst_ratio_90", "q1", "q4",
      "long_returns", "rediscoveries",
      "trajectory",
      "session": { "distinct_days", "plays_per_active_day", "repeat_rate", "late_night_pct" },
      "epoch_rates": { "E1": { "plays", "rate_per_1000" }, "...": "..." },
      "ltp": { "long_returns", "max_gap_days", "days_since", "target_season_ratio", "lifespan_days" },
      "spotify": { "skip_rate", "completion_mean_ratio", "opener_rate", "within_session_repeats" }
    }
  ],
  "trajectory_summary": { "FLASH_BINGE": 3, "...": "..." },
  "trajectory_type_stats": { "...": "..." },
  "correlations": { "...": "..." },
  "discovery_latency": ["..."],
  "ltp_tracks": ["..."]
}
```

Note: `spotify` fields are null per-track when `spotify_signals` table is absent.

---

## Implementation sequence

### Step 1: `consolidate.py` cleanup
No functional changes. Scope: remove dead code, unify date parsing, change hardcoded `StreamingHistory0.json` filename to `--spotify-dir` directory glob that picks up all `StreamingHistory*.json` files.

### Step 2: `engine.py` scaffold
CLI skeleton using `argparse` subparsers. All three subcommands (`signals`, `analyze`, `playlist`) as stubs that connect to the DB and exit cleanly. DB connection helper with context manager. Shared arg parsing for `--db`, `--refdate`, `--out`.

### Step 3: Layers 1–4 (load, derive, aggregate, metrics)
- Layer 1: `load_plays(db, artist=None)` — parameterized query, returns list of row dicts
- Layer 2: `enrich_record(row, ref_date)` — adds `days_ago`, `hour`, `month`, `year`, `season` (null if no months arg)
- Layer 3: `group_by_track(rows)` — keyed by `(artist, track)`, sorted list of timestamps per track
- Layer 4: `compute_simple_metrics(track_plays, ref_date)` — total_plays, span_days, first_play, last_play, days_since, distinct_days, plays_per_active_day

### Step 4: Layers 5–6 (gap analysis, temporal distribution)
- Layer 5: `compute_gaps(timestamps)` — sorted gaps, mean, median, std, skew, pct_long (fraction > gap_days threshold), max_gap; also `chunk_segment(timestamps, tau)` for trajectory
- Layer 6: `compute_temporal(timestamps, ref_date)` — burst_ratio_30, burst_ratio_90, quartile distribution (q1/q4), late_night_pct, peak_hour, repeat_rate

Single canonical implementation for metrics that currently diverge across scripts (burst_ratio_30 in particular).

### Step 5: Layers 7–8 (epoch detection, trajectory classification)
- Layer 7: `detect_epochs(all_plays, epoch_min_plays)` — monthly play counts, identifies dormant gaps, assigns epoch labels E1..En with date ranges and play totals; `compute_epoch_rates(track_timestamps, epochs)` for per-track epoch density
- Layer 8: `classify_trajectory(metrics)` — pure function, takes a dict of pre-computed metrics, returns trajectory label. Priority order: FLASH_BINGE → DISCOVERY_HEAVY → FRONT_LOADED → PERENNIAL_RETURN → SLOW_BURN → REDISCOVERY → DIFFUSE. No side effects.

### Step 6: Layer 9 (LTP qualification, playlist scoring)
- `qualify_ltp(track_metrics, gap_days, months)` — returns ltp dict or None. `months` param controls target_season_ratio computation; if None, target_season_ratio is null.
- `score_playlist(track_metrics, ltp, months)` — composite score. Season weight (`W_SEASON`) only contributes when `months` is not None; redistributed to other weights otherwise.

### Step 7: Layer 10 (cross-track analysis)
- `compute_correlations(track_list)` — burst_30 vs total/span/returns; gap_skew vs span/returns; guard n≥5
- `compute_discovery_latency(track_list)` — days from first_play to peak play density
- `compute_trajectory_summary(track_list)` — counts and type-level stats (mean plays, mean span per trajectory class)

### Step 8: `signals` subcommand
Port `spotify_signal_engine.py` into `engine.py`. Reads `StreamingHistory*.json` via glob from `--input`. Computes all spotify_signals fields. Writes (or replaces) `spotify_signals` table in `music.db`. No analysis logic; pure ingest+aggregate.

### Step 9: Spotify join in `analyze`
After per-track metrics are computed, `LEFT JOIN spotify_signals` on `(artist, track)`. Attach as `spotify` sub-dict in track record. Fields are null if no row found or if table absent (detect via `sqlite_master` query at startup).

### Step 10: Output assembly + smoke test
- Layer 11: `assemble_output(...)` — constructs the full JSON dict from all computed components
- Wire `analyze` subcommand end-to-end
- Wire `playlist` subcommand: runs analyze inline (or accepts pre-computed track list), applies `score_playlist`, returns top-N ranked
- Run smoke tests against bundled data; verify output matches expected shape

Smoke test targets:
```bash
python engine.py analyze --db data/music.db --out /tmp/smoke_analyze.json
python -c "import json; d=json.load(open('/tmp/smoke_analyze.json')); print(d['overview']['total_plays'], 'plays,', len(d['tracks']), 'tracks')"

python engine.py playlist --db data/music.db --months 3,4,5 --n 50 --out /tmp/smoke_playlist.json
python -c "import json; d=json.load(open('/tmp/smoke_playlist.json')); print(len(d['playlist']), 'tracks')"
```

### Step 11: SKILL.md rewrite
Update against new interface: new CLI flags, new output schema, `signals` subcommand, Spotify enrichment behavior. Remove references to old scripts. Update expected runtimes.

### Step 12: Archive old scripts
```bash
git checkout -b rewrite/unified-engine
mkdir archive
git mv lastfm_taste_engine.py lastfm_trajectory_engine.py spotify_signal_engine.py archive/
```

Old scripts remain in `archive/` on the branch. Merged to main when smoke tests pass.

---

## Open question

Is there a meaningful distinction between `analyze` (per-track analysis, trajectory, epochs) and `playlist` (scoring + ranked selection) as separate subcommands, or should `playlist` be a flag on `analyze`?

The case for separate subcommands: the user may want to run analysis once and generate playlists with different `--months` or `--n` without re-running the full per-track analysis. This is only valuable if analysis is slow (it isn't, currently) or if output is cached between runs.

The case for a flag: playlist scoring is downstream of analysis and adds negligible compute. Separating them creates two output files and two invocations for what is conceptually one operation.

Default recommendation: keep them separate subcommands for now. `analyze` writes the full track-level output; `playlist` is a thin scoring pass over the same DB. If the analysis result is needed as input to playlist, add `--analysis-json` as an optional input to `playlist` to skip re-querying.
