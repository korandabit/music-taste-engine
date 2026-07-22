# mte-006 findings — does playlist membership drive track fatigue?

Last updated: 2026-07-22
Script: `mte006_playlist_fatigue.py` (re-runnable, stdlib only)
Full result JSON: run with `--out <path>`

Ticket: `backlog-tickets/mte-006_playlist-driven-fatigue-mechanism.md`

---

## 0. The question and why the direct test is impossible

Mark's hypothesis: getting "sick of a song" may be driven, in part, by that song being
in a **playlist** that — for the song specifically — got overplayed. The direct causal
test needs to know which plays came from a playlist. **Spotify's export does not record
this** (no `context_uri` / playlist id on any play). Confirmed against the raw exports.
So the direct test is unavailable, full stop.

Substitute (the ticket's chosen method): **sequence-fingerprint corroboration**. If a
contiguous run of the timestamp-ordered play history matches a playlist's track *order*,
the playlist was plausibly being played then. Same shape as BLAST local alignment or
Shazam fingerprinting — the causal link was never logged, so a structural match against a
known reference sequence substitutes, with confidence and temporal validity reported
explicitly rather than forced to a binary.

## 1. Raw data shape (verified 2026-07-22 against `data/music.db`)

| quantity | value | note |
|---|---:|---|
| plays in time-ordered stream | 121,370 | all sources, ordered by canonical `ts_utc` |
| **skip-eligible plays** | **55,980** | only Spotify-sourced rows carry `is_skip`/`ms_played`; Last.fm-only (2009-era) plays have **no skip signal** |
| playlists | 147 | `playlist_modified` ranges 2018–2026 |
| playlists usable (≥ min_run tracks) | 130 | short playlists can't produce a length-4 run |

Two structural constraints fall straight out of the raw shape:
- The **fatigue comparison can only use the 55,980 skip-eligible plays.** Any statement
  about skipping is silent on the 2009–2014 Last.fm-only history.
- `Playlist1.json` carries only the **current** order + `lastModifiedDate`, no edit
  history — so a match far from that date is weaker evidence (§4).

## 2. First-order derivation — an explicit playlist order

The `playlists` table had **no `position` column**; order was only implicit in
autoincrement `id`. Verified all 147 playlists have **contiguous id ranges** (so id-order
== JSON insertion order == playlist order), then added an explicit 0-based `position`
column and backfilled it (`--migrate`, backup taken). `consolidate.py` now emits
`position` on all future rebuilds. Relying on autoincrement ordering is no longer needed.

Matching key = `(artist, track)` casefolded + stripped.

## 3. First aggregation — playlist-order runs in the stream

A **run** = plays, in stream order, whose playlist positions increase by 1–2 (allowing
one skipped track), that are near-adjacent in the stream (≤1 intervening foreign play)
and in wall-clock (≤15 min/hop). Runs of length ≥ 4 corroborate straight-through play.

Defaults: `min_run=4, max_pos_jump=2, max_stream_gap=2, max_wall_gap=15min`.

| quantity | value |
|---|---:|
| playlist-order runs found | **1,936** |
| run length — max / median | 56 / 6 |
| runs of length ≥ 6 | 987 |
| distinct plays corroborated (any run) | **12,284** |

**The method works.** Despite no attribution field, playlist-order structure is clearly
detectable in the raw timestamp stream — e.g. a 56-long run through "2015 Spotify Radio"
at order-strictness 0.98, a 32-long run through "Muse sun". Corroboration is real, not
noise.

## 4. Second-order — the temporal-validity filter (large)

`Playlist1.json` order reflects the playlist's state at `lastModifiedDate` only. A play
matched years from that date may be matching an order that didn't exist yet. Each run
carries `temporal_delta_days` (median play date vs modified date); the **strict window**
counts only runs within 180 days.

| corroboration | plays |
|---|---:|
| any run | 12,284 |
| **strict window (≤180d of modified date)** | **1,655** |

This is the single biggest caveat and it is **large**: ~87% of corroborated plays match a
playlist order recorded far from when they were played. Examples — `Muse sun` (Δ1 day,
strong), `2017.3 Disco Summer` (Δ18d, strong) vs `2015 Spotify Radio` (Δ1849d, weak). The
strict subset is the defensible core.

## 5. Higher-order — the fatigue comparison (the actual question)

Skip-rate of playlist-corroborated plays vs freely-chosen plays (skip-eligible only;
lower skip-rate = less fatigue/more engagement):

| group | n | skip_rate |
|---|---:|---:|
| corroborated (any run) | 6,088 | **0.094** |
| corroborated (strict ≤180d window) | 1,336 | **0.119** |
| freely chosen | 49,892 | **0.197** |

### What this says — and does not

**At the population level, the data does NOT support "playlist membership drives
fatigue."** The direction is the opposite: plays corroborated as part of a playlist run
are skipped **about half as often** as freely-chosen plays (0.09–0.12 vs 0.20).

**But this is confounded and must not be read as "playlists prevent fatigue" either.**
Straight-through playlist play is *intentional, engaged* listening by construction — you
skip less when you deliberately queued the sequence. So the low corroborated skip-rate is
partly a **selection artifact of the detection method** (the method can only see playlists
that were being actively played in order), not clean evidence about the fatigue mechanism.
The cumulative-overplay mechanism Mark proposed operates on a longer timescale than a
single session's skip rate and is **not isolable** from this signal.

### The per-track angle (suggestive, thin)

For tracks appearing ≥3× both in-run and freely, sorted by skip-delta, a handful skip
*more* in their playlist context than outside it — the shape the fatigue hypothesis
predicts:

| track | in-run skip (n) | free skip (n) |
|---|---|---|
| San Fermin — Belong | 1.00 (3) | 0.22 (9) |
| Beach House — Girl of the Year | 0.67 (3) | 0.14 (7) |
| Thom Yorke — Knife Edge | 0.67 (3) | 0.14 (7) |

These are **candidates, not confirmation** — n=3–9 per cell. They're the seed of a future,
better-powered test, not a result.

## 6. Verdict (done-when satisfied)

- ✅ The subsequence-match pass exists (`mte006_playlist_fatigue.py`), is re-runnable, and
  tags plays as playlist-corroborated with a per-run **confidence** and
  **temporal_delta_days**.
- ✅ Match-confidence and temporal-validity limits are documented in its output (§4; the
  strict-window subset is the defensible core).
- ✅ The corroborated-vs-freely-chosen fatigue comparison is reported (§5).

**Finding:** the mechanism Mark hypothesized is **not detectable as a net fatigue effect**
in Spotify's data — corroborated plays are skipped *less*, and that comparison is
confounded by the intentional-listening selection baked into the detection method. A
weak per-track "skipped-more-in-its-playlist" signal exists for a few tracks but is far
too thin to confirm cumulative playlist-driven overplay. Consistent with the ticket's own
anticipation that the honest answer could be "no cleanly separable difference."

## 7. Deferrals surfaced (filed, not dropped)

- The exclusion logic in `playlist` assumes **recency-of-recommendation** causes fatigue;
  this hypothesis proposed a distinct **cumulative-overplay** mechanism. This analysis
  found no aggregate evidence for the second, so no change to the exclusion model is
  warranted on this basis. Recorded here rather than acted on.
