# CATALOG:
# id: mte-005
# kind: ticket
# status: open
# origin: Mark's scratch note "2026-04-30 todo notes.txt" (root of repo, orphaned uncommitted WIP flagged by mte-002) — captured at source per capture-at-source convention rather than left as a loose txt file.
# htttw_contribution: 0 — analysis-feature idea.
# judgment_applied: capture-at-source
# provenance: note dated 2026-04-30 11:19; captured as ticket 2026-07-16
# produces: a playlist-usage-pattern model (over-use / under-use grouping) that captures functional listening use-cases
# effort: M
# tags: playlist, usage-modeling, idea
# cross-links: none yet

# Model playlist make-use behavior: over/under-use groupings, functional use-cases

## Idea (verbatim from source note)
"Model the playlist make-use behavior as a grouping that predicts over and under use, attempts to capture functional use-cases for music. What does a good music-listening life look like?"

## Move (not yet scoped)
Underspecified — needs a design pass before implementation:
- Define "over-use" / "under-use" against some baseline (e.g. plays-per-day vs. a track's historical rate, or plays vs. playlist-cohort average).
- Define candidate "functional use-cases" (e.g. focus/background, hype, wind-down, driving) — likely requires either manual labeling of a sample or inferring clusters from existing signals (tempo/energy if content features become available via mte-001, or purely behavioral signals: session position, skip rate, time-of-day, repeat rate already in `spotify_signals`).
- Decide whether this lives in `engine.py` as a new subcommand or as a standalone script (pattern: `self_affinity.py`).

## Done-when
Either (a) a scoped design decision exists (what defines over/under-use, what the use-case taxonomy is) with a concrete next move, or (b) the analysis ships.
