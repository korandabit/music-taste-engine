# CATALOG:
# id: mte-002
# kind: ticket
# status: open
# origin: _index session-close audit 2026-06-21 — uncommitted changes found in this repo from a prior session that ended dirty
# htttw_contribution: 0 — git hygiene.
# judgment_applied: route-to-owner-box, no-blind-commit
# provenance: filed-by monorepo-steward, 2026-06-21
# produces: clean working tree (committed/reverted; db + scratch gitignored if appropriate)
# effort: S
# tags: git-hygiene, orphaned-wip, uncommitted
# gloss: a prior session left this repo dirty — verify the diff and commit if coherent or revert if abandoned.

# Resolve orphaned uncommitted WIP

A prior session ended with this repo dirty. Files (git status 2026-06-21):
- `self_affinity.py` — code; verify + test, then commit or revert.
- `recommendation_log.db` — a runtime DB; likely should be gitignored, not committed.
- `2026-04-30 todo notes.txt` — scratch; commit, relocate, or gitignore.

**Move:** `git diff` → commit `self_affinity.py` if coherent; gitignore the db + scratch if they're not artifacts. **Done-when:** working tree clean.
**Cross-links:** mte-001, _index index-0010 (commit-before-close), ADR-0013.
