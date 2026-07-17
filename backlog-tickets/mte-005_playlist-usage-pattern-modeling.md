# CATALOG:
# id: mte-005
# kind: ticket
# status: open
# origin: Mark's scratch note "2026-04-30 todo notes.txt" (root of repo, orphaned uncommitted WIP flagged by mte-002); unpacked into three scoped statements by Mark on 2026-07-17. This ticket is statement 1. Statement 2 is [[mte-006]]. Statement 3 (managing a lifetime of listening patterns) is explicitly left un-ticketed — reflective/theory-shaped, no clean data hypothesis, not this app's scope.
# htttw_contribution: 0 — analysis-feature idea.
# judgment_applied: capture-at-source
# provenance: note dated 2026-04-30 11:19; captured as ticket 2026-07-16; rescoped 2026-07-17
# produces: an analysis of recommendation_log.db context/track co-occurrence — does the "mood/state" a playlist request names actually predict which tracks fill it, consistently?
# effort: S
# tags: playlist, usage-modeling, feedback-loop, recommendation-log
# cross-links: mte-006, 1-self/2-journal/2023--24 continued/2026-07-17 what does a good music-listening life look like.txt (statement 3, out-of-app)

# Read recommendation_log.db as a mood-hypothesis log, not just an exclusion list

## Idea (Mark's parse, 2026-07-17)
"The make-use behavior is the skill call. I'm pointing to, latently, that the tool is working and then saying now let's treat it as a behavior signal. Whenever I use the tool, what I'm doing is asking for a mood or state to be filled with music. Each individual track's membership [in the resulting playlist] is itself a hypothesis."

## Why this is cheap
`recommendation_log.db` already logs a free-text `--context` label and `run_id` per `playlist` invocation (see `music-engine-feedback-loops.md`), plus which tracks were served. That log already *is* a rough record of "I asked for mood X, here's what the tool hypothesized would fill it" — nothing new needs to be instrumented, it just hasn't been read back as data.

## Move
- Group logged runs by `context` (exact + fuzzy-similar labels, e.g. "Sunday drive" vs "drive").
- For repeat/similar contexts, check whether served tracks converge (same handful of tracks keep getting selected for "driving") or diverge (mood is being filled inconsistently) — a rough measure of whether the tool's mood model is stable per context.
- Optional: surface tracks that appear across *many different* contexts (weak mood-signal, maybe just generically high-affinity) vs. tracks tightly bound to one context (strong functional-use-case signal) — this is the seed of a "functional use-case" taxonomy grounded in Mark's own request history rather than guessed content features.

## Done-when
An analysis exists (script or query) that answers: does context → track selection show consistent hypothesis-confirmation over repeat use, or is it noisy?
