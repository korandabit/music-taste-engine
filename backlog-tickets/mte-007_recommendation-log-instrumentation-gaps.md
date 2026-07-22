# CATALOG:
# id: mte-007
# kind: ticket
# status: open
# origin: surfaced 2026-07-22 while resolving mte-005 (reading recommendation_log.db as a mood-hypothesis log). Two instrumentation gaps make the log unable to answer the questions it was meant to seed.
# htttw_contribution: 0 — tool instrumentation / feedback-loop plumbing.
# judgment_applied: capture-at-source
# provenance: found by mte-005 analysis subagent, verified against recommendation_log.db 2026-07-22
# produces: a recommendation_log that can support (a) run chronology and (b) served-track outcome signals
# effort: S
# tags: recommendation-log, feedback-loop, instrumentation, playlist
# cross-links: mte-005, mte-006, music-engine-feedback-loops.md

# recommendation_log.db instrumentation gaps (blocks mood-convergence + outcome testing)

## Finding (verified against recommendation_log.db, 2026-07-22)
Two gaps, both discovered resolving [[mte-005]]:

1. **`recommended.ts` is NULL for 4 of the 5 logged `playlist_run`s.** Only one run
   carries a timestamp. Run chronology (which run came first, elapsed time between repeat
   contexts) therefore cannot be reconstructed from the log. This does not matter yet
   (there are no repeat contexts — see mte-005), but it is a hard blocker the moment
   repeat-context data starts to accrue: convergence-over-repeat-use (mte-005's primary,
   currently untestable) needs run ordering.

2. **`feedback` table has 0 rows.** The recommendation feedback loop's outcome-signal
   table (the "did this served track land / get skipped / get saved" channel described in
   `music-engine-feedback-loops.md`) is entirely unpopulated. Without it there is no
   served-track *outcome* to cross-check against the mood hypothesis — the loop logs what
   was recommended but never what happened.

## Why this matters
mte-005 showed the log already *is* a rough mood-hypothesis record, but can't yet be read
back as a confirmable one: no chronology (gap 1) and no outcomes (gap 2). Both are cheap
instrumentation fixes on the `playlist` logging path, not analysis work.

## Move
- Gap 1: ensure every `recommended` row is written with a populated `ts` (run timestamp)
  at log time in the `playlist` subcommand's logging path.
- Gap 2: decide + implement how `feedback` rows get written (manual `feedback` subcommand,
  or ingest from a later listening pass) so served-track outcomes can be recorded. This is
  the human-in-the-loop step the feedback-loop design already anticipates.

## Done-when
New `playlist` runs log a populated `ts`, and there is a defined, working path by which
`feedback` rows are recorded for served tracks (even if manual).
