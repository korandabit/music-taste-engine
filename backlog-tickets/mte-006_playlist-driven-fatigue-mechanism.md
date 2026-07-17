# CATALOG:
# id: mte-006
# kind: ticket
# status: open
# origin: Mark's unpacked scratch note (2026-04-30 todo notes.txt / mte-005 origin), statement 2, 2026-07-17.
# htttw_contribution: 0 — analysis-feature idea, data-availability-constrained.
# judgment_applied: capture-at-source, verify-premise-before-fix (checked raw Spotify export schema before scoping)
# provenance: captured 2026-07-17
# produces: either (a) a playlist-membership-vs-fatigue proxy analysis, or (b) a recorded finding that the mechanism isn't testable with available data
# effort: M (mostly a data-availability question, not implementation)
# tags: playlist, fatigue, skip-rate, exclusion-logic, data-availability
# cross-links: mte-005, 1-self/2-journal/2023--24 continued/2026-07-17 what does a good music-listening life look like.txt (statement 3, out-of-app)

# Does playlist membership specifically drive track fatigue ("sick of a song")?

## Idea (Mark's parse, 2026-07-17)
"Because the individual tracks are in a playlist there's an adjacent hypothesis: the over/under for specific tracks. This hypothesis is a key one in the mte generally — e.g. don't serve me songs I'm sick of. Here is a question of upstream mechanism: does getting sick of a song happen in part because it's in a playlist that, for the song specifically, got overplayed?"

## Data-availability finding (checked 2026-07-17, before scoping further)
Confirmed against the raw exports in `data/` (`StreamingHistory0.json`, `df26 spotify/StreamingHistory_music_0.json`): the standard Spotify export has only `endTime`/`artistName`/`trackName`/`msPlayed` — no playlist-context field. Spotify's richer "Extended Streaming History" format (referenced elsewhere in this project but not present in this bundle) adds `reason_start`/`reason_end`/`shuffle`/`skip`, but its known schema still has **no `context_uri` or playlist identifier** — Spotify does not record which plays came from a playlist versus organic listening. The direct causal test this hypothesis wants is not answerable from Spotify's own data, full stop.

## What's actually testable (sequence-fingerprint corroboration, Mark's refinement 2026-07-17)
Don't require explicit attribution — corroborate a playlist was being played by matching
contiguous (or sliced) runs of the timestamp-ordered streaming history against a playlist's
track order. Same reasoning shape as Shazam-style audio fingerprinting, BLAST-style local
sequence alignment, or plagiarism/document-fingerprint matching: the causal link was never
logged, so a structural-similarity test against known reference sequences substitutes for it,
with confidence/ambiguity reported explicitly rather than forced to a binary label.

**Data confirmed available (checked 2026-07-17):** `Playlist1.json` has 148 playlists with
ordered `items` arrays (`trackName`/`artistName`/`albumName`, in playlist order).
`music.db`'s `playlists` table flattens this to `(playlist_name, artist, album, track)`,
order currently only recoverable via row-`id` sequence — **no explicit `position` column**;
would need one added to `consolidate.py`'s schema before this is safe to build on (relying on
autoincrement-id ordering is fragile).

**Guardrails to keep this from becoming a can of worms:**
- **Match strictness** — require exact contiguous subsequence (small documented fuzziness,
  e.g. one skip, at most) not loose "resembles." Loose matching on short/generic playlists
  (e.g. an album subset that's just... the album) manufactures false positives.
- **Temporal validity** — `Playlist1.json` only has current-state order + `lastModifiedDate`,
  not edit history. A match is only meaningful for plays reasonably near that date; matching
  a 2011 play against a playlist's 2026 order is a category error, not evidence.
- **Ambiguity is reported, not resolved** — when a subsequence matches multiple playlists,
  report candidate playlists plural. Dedup/which-playlist is an intermediate variable, not
  load-bearing for the core hypothesis (was this run playlist-constrained at all; do
  playlist-constrained listens show different fatigue/skip curves than freely-chosen ones —
  why do some songs resist repeat-fatigue better than others).
- This bears on the tool's existing exclusion logic (`playlist --no-log` / recommendation_log
  exclusion), which assumes recency-of-recommendation causes fatigue. This hypothesis proposes
  a distinct mechanism (cumulative playlist-driven overplay). Both could be true, contributing
  differently; the current tool only models the first.

## Move
1. Add an explicit `position` column to `playlists` in `consolidate.py` (schema + INSERT), migrate `music.db`.
2. Build the subsequence-match pass: for each playlist's ordered track list, scan the
   timestamp-ordered streaming history per artist/track for contiguous (near-)matching runs.
3. Tag matched plays as playlist-corroborated (with candidate playlist(s) + match confidence),
   respecting the temporal-validity guardrail.
4. Compare skip-rate / fatigue trend for playlist-corroborated plays vs. freely-chosen plays.

## Done-when
The subsequence-match pass exists, its match-confidence and temporal-validity limits are
documented in its output, and the playlist-corroborated vs. freely-chosen fatigue comparison
is reported (even if the finding is "no detectable difference").
