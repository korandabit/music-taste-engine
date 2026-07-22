# mte-005 findings: recommendation_log.db as a mood-hypothesis log

Last updated: 2026-07-22

## Core finding (data-availability, read this first)

The ticket's primary question — for repeat/similar contexts, do served tracks
**converge** (same tracks keep getting picked) or **diverge**? — is **not
answerable from the current log**. `recommendation_log.db` has exactly **one
`playlist_run` per distinct `context`** (5 runs, 5 contexts, no repeats and no
near-duplicate context labels). Convergence/divergence is a measure over
*repeated* use of the same or similar context; with n=1 per context there is no
repeat structure to measure it over. This is stated here rather than inferred
or filled in with a plausible-sounding answer.

What *is* answerable from the existing 5 runs is the secondary question the
ticket poses as optional: cross-run track overlap (weak/generic mood signal
vs. context-bound functional-use signal). That is reported below, with the
explicit caveat that 5 runs is too thin a sample to generalize from.

## Raw data shape

`recommendation_log.db` has two tables:

- `recommended` — columns `(ts, playlist_run, artist, track, context, plays,
  returns, rest_days, spring_pct)`, **170 rows**.
- `feedback` — columns `(ts, playlist_run, artist, track, signal)`, **0 rows**.
  No feedback has ever been logged; there is no signal here to cross with the
  recommended table.

`ts` is `NULL` for 4 of the 5 runs; only `playlist-2026-06-16` has a populated
timestamp (`2026-06-16T17:33:10`). Run-level chronology of the other 4 runs is
not recoverable from this column.

**Encoding note (not mojibake, corrects an initial misread):** three context
strings contain a character that displays as `<0xEF><0xBF><0xBD>`-style boxes
in some Windows consoles/fonts. Byte-level inspection shows the actual stored
character is **U+00B7 MIDDLE DOT ("·")**, used as a separator (e.g.
`"spring · LTP · scored"`). This is valid, intentional data — a console/font
rendering limitation, not stored corruption. No cleanup was applied to the
underlying strings; the analysis script reconfigures stdout to UTF-8 so the
report renders correctly.

## Per-run descriptive table

| n_tracks | playlist_run | context (as stored) |
|---:|---|---|
| 20 | compulsion | compulsion-pattern |
| 40 | low-play-vibe-40 | low-play · warm/big/inspiring/broody |
| 55 | melancholic-piano | piano / atmospheric · vibe |
| 5 | playlist-2026-06-16 | playlist |
| 50 | spring-2026 | spring · LTP · scored |

5 distinct `playlist_run` values, 5 distinct `context` values — a strict 1:1
mapping. No context label repeats or near-repeats (e.g. no "drive" vs "Sunday
drive" pair) exists in the current log.

## Cross-run track overlap

Across all 170 rows there are **168 distinct `(artist, track)` pairs**. Of
those:

- **2 pairs (1.2%) appear in more than one run** — cross-context, i.e. a
  weaker/more generic mood signal:
  - Neutral Milk Hotel — "Oh Comely" (runs: `melancholic-piano`, `spring-2026`)
  - Poppy Ackroyd — "Feathers" (runs: `melancholic-piano`, `spring-2026`)
- **166 pairs (98.8%) appear in exactly one run** — context-bound, i.e.
  candidate functional-use-case signal by the ticket's framing.

**Caveat:** with only 5 runs and largely non-overlapping track pools (driven
partly by each run's own scoring parameters, e.g. `--energy`/`--n`/season
filters), a 98.8% single-context rate is expected structurally and should not
yet be read as evidence of strong functional binding — there isn't enough
repeat volume per context to distinguish "this track is genuinely tied to this
mood" from "this track just hasn't had a second chance to be served under a
different context." The two cross-context tracks are the only concrete,
non-speculative data point available: both were served under both
`melancholic-piano` and `spring-2026`, the two largest/most emotionally
adjacent runs (55 and 50 tracks respectively), which is consistent with either
reading (generically high-affinity tracks, or contexts that are less distinct
from each other than their labels suggest).

## Script output (actual run, 2026-07-22)

Produced by `python3 mte005_context_analysis.py --db recommendation_log.db`:

```
mte-005 context/mood-hypothesis analysis
  db: D:/code/music_taste_engine/recommendation_log.db
  170 recommended rows, 0 feedback rows
  5 distinct playlist_run values, 5 distinct context values

  Repeat-convergence testability:
    NOT TESTABLE -- every context has exactly one run (n=1 per context).
    There is no repeat structure to measure convergence/divergence over.

  Per-run shape:
  n_tracks  playlist_run           context (cleaned)
        20  compulsion             compulsion-pattern
        40  low-play-vibe-40       low-play · warm/big/inspiring/broody
        55  melancholic-piano      piano / atmospheric · vibe
         5  playlist-2026-06-16    playlist
        50  spring-2026            spring · LTP · scored

  Cross-run track overlap:
    168 distinct (artist, track) pairs served across all runs
    2 pairs appear in >1 run (cross-context / weak-signal)
    166 pairs appear in exactly 1 run (context-bound / candidate functional-use signal)

    Cross-context pairs:
      Neutral Milk Hotel - Oh Comely  (runs: melancholic-piano, spring-2026)
      Poppy Ackroyd - Feathers  (runs: melancholic-piano, spring-2026)
```

## What would make this testable

The ticket's primary question (context repeat → convergence/divergence)
requires actual repeat usage of the same or similar context string through the
`playlist` tool. Nothing needs to be re-instrumented for that — the log
schema already supports it — it just hasn't happened yet in the 5 runs on
record. A future re-run of this script (unchanged) will pick up repeat
structure automatically once it exists.

Two related gaps surfaced during this analysis that are out of this ticket's
scope but worth flagging separately:

1. `ts` is `NULL` for 4 of 5 runs, so run chronology (which run came first,
   how much time elapsed between runs) can't be reconstructed from the log
   as currently populated — this would matter for interpreting "repeat use"
   once it exists.
2. `feedback` has 0 rows — the recommendation feedback loop's second table is
   entirely unused, so no served-track outcome signal exists to cross-check
   against the mood hypothesis.
