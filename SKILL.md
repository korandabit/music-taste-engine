---
name: music-data-engine
description: >
  Unified music listening data analysis across Last.fm and Spotify.
  **consolidate.py** — One-time setup: merges Last.fm CSV + Spotify exports into
  a single SQLite database (music.db) with a unified play timeline, saved library,
  and playlist data.
  **lastfm_taste_engine.py** — Listening autobiography, long-delay true positives
  (tracks returned to after 180d+ gaps), seasonal affinity, year-by-year obsessions,
  season-aware 50-track playlist. Reads the Last.fm CSV directly.
  **lastfm_trajectory_engine.py** — Artist/catalog deep analysis: event-gap chunk
  segmentation (tunable τ), per-chunk attention share, per-track trajectory
  classification (BURN / PERENNIAL / SLOW_BURN / FLASH_BINGE / REDISCOVERY),
  session fingerprints, binge→lifespan correlations, gap distribution stats.
  Reads the Last.fm CSV directly.
  Trigger when user uploads music data files or asks about listening history,
  music taste, playlist generation, artist deep-dive, trajectory analysis,
  or Spotify library/playlist data.
---

# Music Data Engine — Skill Instructions

## Asset locations

```
/mnt/skills/user/music-data-engine/
  SKILL.md                           ← this file
  consolidate.py                     ← merges all sources into music.db (run once)
  lastfm_taste_engine.py             ← autobiography + playlist engine
  lastfm_trajectory_engine.py        ← trajectory + behavioral analysis engine
  data/
    edgarturtleblot.csv              ← Last.fm export
    StreamingHistory0.json           ← Spotify play history (2019–2020)
    Playlist1.json                   ← Spotify playlists (73)
    YourLibrary.json                 ← Spotify saved tracks + albums
    music.db                         ← consolidated SQLite DB (pre-built)
```

All files are **read-only here**. Always copy to working directory:

```bash
cp /mnt/skills/user/music-data-engine/consolidate.py /home/claude/
cp /mnt/skills/user/music-data-engine/lastfm_taste_engine.py /home/claude/
cp /mnt/skills/user/music-data-engine/lastfm_trajectory_engine.py /home/claude/
cp -r /mnt/skills/user/music-data-engine/data /home/claude/
```

`music.db` is pre-built and ready to query. Re-run `consolidate.py` only if the
source files have changed (e.g. a new Spotify or Last.fm export was added).

---

## Which script to use

| Question type | Script | Why |
|---------------|--------|-----|
| Setup / first run | `consolidate.py` | Merges all sources into `music.db`; `music.db` is pre-built so skip unless source files changed |
| "Analyze my listening history" | Both engines + raw CSV groupby | See §TEMPORAL DECOMPOSITION — engine outputs alone are insufficient for time-series analysis |
| "Generate a playlist" | `lastfm_taste_engine.py` | Season-aware LTP playlist with scoring |
| "How do I feel about [artist]?" | `lastfm_trajectory_engine.py` | Per-track trajectory classification, chunk shares, behavioral fingerprints |
| "What are my burn vs lasting tracks?" | `lastfm_trajectory_engine.py` | BURN/PERENNIAL classification with session metrics |
| "How did my taste change over time?" | `lastfm_trajectory_engine.py` | Chunk segmentation + cross-chunk attention shares |
| "Compare my taste to popularity" | `lastfm_trajectory_engine.py` | With `--popularity` JSON |
| "What's in my Spotify library/playlists?" | Query `music.db` directly | `library_tracks`, `library_albums`, `playlists` tables |
| "Skip rate / listening duration?" | Query `music.db` plays table | `ms_played` and `is_skip` fields from Spotify (source='spotify' or 'lastfm+spotify') |
| "Deep dive on [specific pattern]" | Both engines + `music.db` | Engines for temporal/trajectory analysis; DB for cross-source joins |

---

## Step 0 — Consolidate data sources (one-time setup)

`music.db` is pre-built. Skip this step unless source files have changed.

```bash
cd /home/claude
python3 consolidate.py --data-dir data --out data/music.db
```

Output summary printed to console: play counts by source, merged row count, DB size.

### music.db schema

```
plays (id, source, ts, artist, album, track, ms_played, is_skip)
  source: 'lastfm' | 'spotify' | 'lastfm+spotify'
  ts: 'YYYY-MM-DD HH:MM'
  ms_played, is_skip: NULL for lastfm-only rows

library_tracks (id, artist, album, track)   ← Spotify saved tracks
library_albums (id, artist, album)           ← Spotify saved albums
playlists (id, playlist_name, playlist_modified, artist, album, track)
```

### Querying music.db

```python
import sqlite3, json
con = sqlite3.connect("data/music.db")
con.row_factory = sqlite3.Row

# Full play timeline
rows = con.execute("SELECT * FROM plays ORDER BY ts").fetchall()

# Skip rate from Spotify-sourced plays
cur = con.execute("""
    SELECT
        COUNT(*) as total,
        SUM(is_skip) as skips,
        ROUND(AVG(ms_played)/60000.0, 1) as avg_min
    FROM plays WHERE is_skip IS NOT NULL
""")
print(dict(cur.fetchone()))

# All tracks in a specific playlist
cur = con.execute(
    "SELECT artist, track FROM playlists WHERE playlist_name = ?", ("golong",)
)
for row in cur: print(row["artist"], "-", row["track"])

# Cross-source: saved tracks that also appear in Last.fm top 100
cur = con.execute("""
    SELECT lt.artist, lt.track, COUNT(*) as plays
    FROM library_tracks lt
    JOIN plays p ON lower(lt.artist)=lower(p.artist) AND lower(lt.track)=lower(p.track)
    GROUP BY lt.artist, lt.track
    ORDER BY plays DESC LIMIT 20
""")
for row in cur: print(row["plays"], row["artist"], "-", row["track"])
```

---

## Step 1 — Dependency check

```bash
python3 -c "import json, math, statistics, collections, datetime, argparse, pathlib, sqlite3; print('OK')"
```

All stdlib. No pip installs required for any script.

---

## Step 2a — Run the taste engine (autobiography + playlist)

```bash
cd /home/claude
python3 lastfm_taste_engine.py --csv edgarturtleblot.csv --out taste_output.json --playlist 50
```

With explicit reference date:
```bash
python3 lastfm_taste_engine.py --csv edgarturtleblot.csv --out taste_output.json --refdate YYYY-MM-DD
```

### Output keys

| Key | What it is |
|-----|-----------|
| `overview` | Total plays, span, unique counts, avg/day |
| `clock` | Plays by hour, peak hour, late-night % |
| `seasonal` | Play counts + % by season |
| `top_tracks` / `top_albums` / `top_artists` | Top 25 each |
| `top_target_season_tracks` | Top 25 tracks in TARGET_SEASON_MONTHS |
| `year_by_year` | Per-year: total, top artist, obsessions, top-10 |
| `long_delay_true_positives` | Full LTP set with gap_stats per track |
| `playlist` | Scored 50-track playlist |

---

## Step 2b — Run the trajectory engine (deep analysis)

**Single artist, default τ (data-derived):**
```bash
cd /home/claude
python3 lastfm_trajectory_engine.py \
  --csv edgarturtleblot.csv \
  --artist "Muse" \
  --out muse_trajectory.json \
  --refdate YYYY-MM-DD
```

**Full catalog (no artist filter):**
```bash
python3 lastfm_trajectory_engine.py \
  --csv edgarturtleblot.csv \
  --out full_trajectory.json
```

**Explicit τ — tune chunk granularity:**
```bash
# Fine-grained: session-level chunks (~1–3 day gaps mark boundaries)
python3 lastfm_trajectory_engine.py --csv export.csv --tau 2 --out sessions.json

# Medium: streak-level (week-scale gaps)
python3 lastfm_trajectory_engine.py --csv export.csv --tau 14 --out streaks.json

# Coarse: era-level (months of silence mark boundaries)
python3 lastfm_trajectory_engine.py --csv export.csv --tau 60 --out eras.json
```

**Multi-resolution — detect interacting scopes:**
Run the same CSV at multiple τ values and compare `chunks` across outputs.
A track's `chunk_rates` at τ=2 reveals binge session intensity;
at τ=60 it reveals which life-eras it was active in.
```bash
for tau in 2 14 60; do
  python3 lastfm_trajectory_engine.py \
    --csv export.csv --artist "Radiohead" \
    --tau $tau --out radiohead_t${tau}.json
done
```

**Adjust τ percentile (data-derived default):**
```bash
# More conservative split (fewer, larger chunks)
python3 lastfm_trajectory_engine.py --csv export.csv --tau-percentile 0.90

# More aggressive split (more, smaller chunks)
python3 lastfm_trajectory_engine.py --csv export.csv --tau-percentile 0.70
```

**With popularity comparison:**
```bash
# Create a JSON file: {"track_name": popularity_0_to_100, ...}
python3 lastfm_trajectory_engine.py \
  --csv edgarturtleblot.csv \
  --artist "Muse" \
  --popularity muse_pop.json \
  --out muse_trajectory.json
```

**Adjust minimum plays threshold:**
```bash
python3 lastfm_trajectory_engine.py --csv export.csv --artist "Radiohead" --min-plays 10
```

### Output keys

| Key | What it is |
|-----|-----------|
| `meta` | Run config, date range, play counts, τ used |
| `chunks` | All detected listening chunks with density/breadth/top_artist |
| `tracks` | Full per-track analysis (see schema below) |
| `trajectory_summary` | Count of tracks by trajectory type |
| `trajectory_type_stats` | Aggregate stats per trajectory type |
| `correlations` | burst_30 → total/span/returns; gap_skew → span/returns (if n≥5) |
| `discovery_latency` | Late-discovered tracks with delay measurements |
| `popularity_comparison` | Percentile-normalized comparison (if supplied) |

### Per-track schema

```json
{
  "track": "Guiding Light",
  "albums": ["The Resistance"],
  "total_plays": 121,
  "span_days": 5522,
  "first_play": "2009-09-09",
  "last_play": "2024-10-23",
  "days_since_last": 516,
  "burst_30": 18,
  "burst_90": 25,
  "burst_ratio_30": 0.149,
  "burst_ratio_90": 0.207,
  "q1": 0.802,
  "q4": 0.058,
  "long_returns": 6,
  "rediscoveries": [
    {"gap_days": 412, "return_date": "2013-12-14", "return_year": 2013, "cluster_size": 3}
  ],
  "trajectory": "FRONT_LOADED",
  "gap_stats": {
    "mean_days": 42.3,
    "median_days": 18.0,
    "std_days": 61.2,
    "skew": 1.8,
    "pct_long": 0.083,
    "n_gaps": 120
  },
  "chunk_rates": [
    {
      "chunk_name": "C4",
      "chunk_start": "2010-02-14",
      "chunk_end": "2010-04-01",
      "chunk_total": 847,
      "track_plays": 36,
      "share": 0.0425,
      "density_rank": 0.91
    }
  ],
  "session": {
    "distinct_days": 69,
    "plays_per_active_day": 1.75,
    "repeat_rate": 0.364,
    "late_night_pct": 0.446
  },
  "peak_month": "2010-03",
  "peak_month_plays": 18,
  "year_distribution": {"2009": 26, "2010": 36, "...": "..."}
}
```

**`chunk_rates` field:** list of chunks in which this track appeared, sorted chronologically.
`share` = track plays / chunk total plays — the normalized attention metric, corrected for
your overall listening volume in that period. `density_rank` = this chunk's density
percentile among all chunks (0 = quietest, 1 = most intense); lets you read whether a
track tended to appear during high-activity or low-activity periods.

### Trajectory types

| Type | Definition | Behavioral signature |
|------|-----------|---------------------|
| `FLASH_BINGE` | ≥50% of plays in first 30 days | Highest ppd, highest repeat rate. Short-lived. |
| `DISCOVERY_HEAVY` | ≥60% in first 90 days | Intense initial engagement, moderate lifespan |
| `FRONT_LOADED` | ≥65% in first quartile of lifespan | "Bright burns." High session intensity, decaying rate |
| `PERENNIAL_RETURN` | ≥3 rediscoveries + q4 ≥ 15% | Low repeat rate, spread across many distinct days. Durable. |
| `SLOW_BURN` | q4 ≥ q1×0.8, ≥2 rediscoveries | Grows over time. Often late-discovered tracks. |
| `REDISCOVERY` | ≥2 gap-return events, moderate distribution | Gap-then-cluster pattern without sustained perennial quality |
| `DIFFUSE` | None of the above | No clear temporal shape |

### Key methodological concepts

**Chunk segmentation and attention share** (`chunks`, `chunk_rates`): The listening
history is segmented into contiguous chunks by gap threshold τ — a data-derived value
(default: 80th percentile of inter-play gaps) that lets natural breaks in your behavior
define the unit, not the calendar. Each chunk is its own denominator: `share` =
track plays in chunk / chunk total plays. This normalizes for your listening volume,
so a track commanding 4% of attention during a 2000-play chunk is directly comparable
to 4% during a 200-play chunk.

τ is fully tunable. Small τ (2–3 days) produces session-level chunks; large τ (60+ days)
produces era-level chunks. Running the engine at multiple τ values reveals interacting
scopes: a track may dominate individual sessions (high share at τ=2) but disappear
entirely across eras (zero chunks at τ=60), or vice versa — a persistent background
presence that never peaks.

`density_rank` in each chunk_rates entry (0–1 percentile of chunk density) tells you
whether a track's appearances cluster in your high-activity or low-activity periods —
a further dimension of its retrieval signature.

**Gap distribution as retrieval signature** (`gap_stats`): The distribution of
inter-play gaps — not just their count — encodes what a track *does* for the listener.
A gap represents the interval between a retrieval event (playing now) and the next
retrieval demand (playing again). The shape of this distribution is the track's
phenomenological fingerprint:

| Pattern | mean | skew | Interpretation |
|---------|------|------|----------------|
| Consistent cadence | low–mid | near 0 | Stable, context-independent effect; reliable retrieval |
| Variable symmetric | any | near 0, high std | Mood-cycling; effect is real but not time-locked |
| Binge-then-ignore | low mean | high + | Front-loaded effect; re-sought immediately then abandoned |
| Latent/contextual | high mean | very high + | Context-dependent; retrieved only under specific memory-cue conditions |

`pct_long` (fraction of gaps > 180 days) is the direct rate of "return after
absence" — a track-level signal of durability independent of total play count.

`gap_skew_vs_returns` and `gap_skew_vs_span` in the correlations block show
whether high-skew tracks (context-dependent) tend to have more or fewer long-term
returns in your catalog — this is listener-specific and interpretively significant.

**Session fingerprint**: `plays_per_active_day` and `repeat_rate` distinguish
burn behavior (dense sessions, re-plays) from perennial behavior (one play per
day, spread across months). Late-night percentage adds context for solitary/
immersive vs. background/social listening.

**Early-binge prediction**: `burst_ratio_30` (fraction of total plays in first
30 days) correlates positively with total count but *negatively* with lifespan
and return frequency. The strength of this effect is artist-specific and
listener-specific (illustrative values: Muse r=-0.43, Radiohead r=-0.19 — yours
will differ). Report the actual r values from the run rather than citing these.

**Late discovery effect**: Tracks first played ≥60 days after their album's
first play tend to have higher q4 (recent-quartile) concentration — they
avoided the binge-decay cycle.

---

## Step 3 — Interpreting results

### For autobiography (taste engine)

```python
import json
with open("taste_output.json") as f:
    data = json.load(f)

# Quick overview
ov = data["overview"]
print(f"{ov['total_plays']:,} plays over {ov['span_years']} years")

# Top artists
for a in data["top_artists"][:10]:
    print(a["artist"], a["plays"])
```

### For trajectory analysis

```python
import json
with open("muse_trajectory.json") as f:
    data = json.load(f)

# Trajectory distribution
for traj, stats in data["trajectory_type_stats"].items():
    print(f"{traj}: n={stats['count']} avg_ppd={stats['avg_ppd']}")

# Find perennials
perennials = [t for t in data["tracks"] if t["trajectory"] == "PERENNIAL_RETURN"]
for t in sorted(perennials, key=lambda x: -x["q4"])[:10]:
    print(f"{t['track']}: q4={t['q4']:.0%}, returns={t['long_returns']}")

# Cross-chunk attention share for a specific track
track = next(t for t in data["tracks"] if t["track"] == "Hysteria")
for cr in track["chunk_rates"]:
    print(f"  {cr['chunk_name']} ({cr['chunk_start']}): "
          f"share={cr['share']:.1%} plays={cr['track_plays']}/{cr['chunk_total']} "
          f"density_rank={cr['density_rank']:.2f}")

# Binge → outcome correlations
c = data["correlations"]
print(f"burst_30 → lifespan: r={c['burst30_vs_span']}")
if "gap_skew_vs_returns" in c:
    print(f"gap_skew → returns:  r={c['gap_skew_vs_returns']}")
    print(f"gap_skew → lifespan: r={c['gap_skew_vs_span']}")

# Gap distribution fingerprints — find context-dependent (high skew) tracks
# High skew = binge-ignore or latent/contextual retrieval pattern
latent = [t for t in data["tracks"]
          if t["gap_stats"]["n_gaps"] >= 5 and (t["gap_stats"]["skew"] or 0) > 2.0]
for t in sorted(latent, key=lambda x: -x["gap_stats"]["skew"])[:10]:
    gs = t["gap_stats"]
    print(f"{t['track']}: skew={gs['skew']} median={gs['median_days']}d pct_long={gs['pct_long']:.0%}")

# Stable retrievers: low skew, meaningful play count
stable = [t for t in data["tracks"]
          if t["gap_stats"]["n_gaps"] >= 5
          and abs(t["gap_stats"]["skew"] or 99) < 0.8
          and t["total_plays"] >= 10]
for t in sorted(stable, key=lambda x: x["gap_stats"]["std_days"])[:10]:
    gs = t["gap_stats"]
    print(f"{t['track']}: skew={gs['skew']} mean={gs['mean_days']}d std={gs['std_days']}d")
```

---

## Tunable thresholds

### Taste engine (in script header)

| Constant | Default | Effect |
|----------|---------|--------|
| `LONG_GAP_DAYS` | 180 | Gap threshold for LTP |
| `MIN_RETURNS_FOR_LTP` | 2 | Min returns for LTP set |
| `TARGET_SEASON_MONTHS` | `{3,4,5}` | Target season month set (change to tune playlist to any season) |
| `TARGET_SEASON_RATIO_MIN` | 0.30 | Min fraction of plays in target season to qualify for playlist (set to 0.0 to disable) |
| `W_SEASON / W_RETURNS / W_DEPTH / W_REST` | 0.30/0.35/0.20/0.15 | Playlist scoring weights; W_SEASON boosts tracks matching target season |

### Trajectory engine (in script or via CLI)

| Constant / Flag | Default | Effect |
|----------|---------|--------|
| `--tau` | derived | Gap threshold in days for chunk segmentation. Omit to use data-derived default. |
| `--tau-percentile` | 0.80 | Percentile of inter-play gap distribution used to derive τ when `--tau` not set. Lower = more chunks; higher = fewer. |
| `LONG_GAP_DAYS` | 180 | Gap threshold for rediscovery detection |
| `BURN_Q1_THRESHOLD` | 0.65 | Q1 fraction to classify as FRONT_LOADED |
| `PERENNIAL_Q4_THRESHOLD` | 0.15 | Q4 fraction for PERENNIAL_RETURN |
| `PERENNIAL_MIN_RETURNS` | 4 | Min 180d+ returns for perennial |
| `FLASH_BINGE_30D` | 0.50 | 30-day burst ratio for flash binge |

---

## Expected runtime

| Scrobble count | Taste engine | Trajectory (1 artist) | Trajectory (full) |
|----------------|-------------|----------------------|-------------------|
| <20k           | <5s         | <3s                  | <10s              |
| 20k–100k       | 5–30s       | 3–15s                | 30–120s           |
| 100k+          | 30–90s      | 15–60s               | 2–5min            |

No external API calls. Fully offline. All stdlib.
