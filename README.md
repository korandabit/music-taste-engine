# Music Taste Engine

Behavioral music analysis from your Last.fm and Spotify listening history.

Unlike streaming dashboards that show top tracks and play counts, this tool classifies *how* you listen — trajectory archetypes (binge vs. perennial vs. rediscovery), long-delay true positives (tracks you reliably return to after months of absence), and epoch-normalized engagement across years of history. Playlists are scored on your own behavioral signals, not collaborative filtering or audio features.

Zero dependencies (Python stdlib + SQLite). No API keys, no cloud, no account. Your data stays local.

## What it does

- **Trajectory classification**: every track gets one of seven archetypes (FLASH_BINGE, DISCOVERY_HEAVY, FRONT_LOADED, PERENNIAL_RETURN, SLOW_BURN, REDISCOVERY, DIFFUSE) based on burst ratios, quartile distribution, and return patterns
- **Long-delay true positive (LTP) detection**: identifies tracks you genuinely love vs. merely binged — a track with 200 plays in one month is not an LTP, but 30 plays spread across five returns over eight years is
- **Epoch detection**: finds high-density listening periods and normalizes per-track rates against them
- **Behavioral playlist scoring**: scores on periodicity of returns, completion rate, rest time, and trajectory archetype — answers *what should I hear again based on how I actually listen?*
- **Standalone reports**: `--summary report.md` produces a human-readable markdown report without needing any other tooling

## Getting your data

You need at least one export source. More data = richer analysis; works best with 5+ years but produces useful results from 2+.

### Spotify (recommended starting point)

1. Go to [spotify.com/account](https://spotify.com/account) → Privacy settings → "Request your data"
2. Select **Extended Streaming History** (not the basic "Account data" option — that only covers the last year). Spotify sends a download link within 5–30 days.
3. The download contains two folders: `Spotify Extended Streaming History/` (play-by-play JSON files) and `Spotify Account Data/` (library, playlists).

### Last.fm (adds depth if you've been scrobbling)

1. Export via lastfm-to-csv or Benjamin Benben's exporter (search "last.fm export CSV"). The CSV should have columns: `artist, album, track, date`.
2. Last.fm data extends the timeline and provides album info that Spotify's standard export lacks.

## Quick start

```bash
# 1. Build the database (Spotify-only)
python consolidate.py \
  --spotify-dir "Spotify Extended Streaming History" \
  --meta-dir "Spotify Account Data" \
  --out music.db

# Or with Last.fm
python consolidate.py \
  --csv lastfm_export.csv \
  --spotify-dir "Spotify Extended Streaming History" \
  --meta-dir "Spotify Account Data" \
  --out music.db

# 2. Compute Spotify behavioral signals
python engine.py signals --db music.db --input "Spotify Extended Streaming History"/*.json

# 3. Run analysis
python engine.py analyze --db music.db --out analysis.json

# 4. Get a standalone report
python engine.py analyze --db music.db --summary my_listening_report.md

# 5. Generate a playlist
python engine.py profile --db music.db          # check what's viable
python engine.py playlist --db music.db --n 25 --energy low --context "Sunday drive"
```

## Corpus size expectations

Auto-calibration adjusts thresholds for smaller corpora, but analysis depth scales with data:

| Corpus | What works well | What's thin |
|---|---|---|
| 10+ years, 50k+ plays | Everything — trajectory, epochs, LTP, deep rediscovery patterns | — |
| 5–10 years, 20k–50k plays | Trajectory, LTP, seasonal analysis, playlists | Epoch detection may find fewer epochs |
| 2–5 years, 5k–20k plays | Basic trajectory, playlists, top-N, clock/seasonal | LTP needs long gaps; fewer qualifiers |
| < 2 years or < 5k plays | Top-N, clock/seasonal, playlist (with relaxed filters) | Trajectory types collapse toward DIFFUSE; LTP effectively disabled |

## Claude Code skill

This repo includes a `SKILL.md` for use as a [Claude Code skill](https://docs.anthropic.com/en/docs/claude-code). When invoked as a skill, Claude drives the analysis pipeline conversationally — running `profile` to assess what's viable, then `playlist` with appropriate parameters based on your request. The `profile → playlist` loop is purpose-built for this agentic workflow.

## Scripts

| Script | Purpose |
|---|---|
| `consolidate.py` | Merges Last.fm CSV + Spotify JSON exports into `music.db` (SQLite) |
| `engine.py` | Analysis engine with subcommands: `analyze`, `profile`, `playlist`, `signals` |

See `SKILL.md` for full CLI reference, output schemas, and tunable thresholds.
