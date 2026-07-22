# CATALOG:
# id: mte-008
# kind: ticket
# status: done
# origin: found 2026-07-22 while smoke-testing a consolidate.py schema change for mte-006. The documented Spotify-only fast-path is broken at the CLI contract level.
# htttw_contribution: 0 — CLI/doc-vs-code drift.
# judgment_applied: capture-at-source, verify-premise-before-fix
# provenance: verified against consolidate.py 2026-07-22
# produces: either a working Spotify-only path (--csv optional) or corrected docs
# effort: S
# tags: consolidate, cli, doc-drift, spotify-only, regression
# cross-links: mte-006

# consolidate.py Spotify-only mode is broken (--csv is required, docs say optional)

## Finding (verified against consolidate.py, 2026-07-22)
Multiple docs advertise a Spotify-only fast path with `--csv` omitted:
- `CLAUDE.md`: "`consolidate.py` CLI: `--csv` (optional) + `--spotify-dir` (required or
  optional). Spotify-only mode works when `--csv` is omitted."
- `SKILL.md` / `README.md`: a Spotify-only invocation without `--csv`.

But in the code:
- `consolidate.py:316` — `ap.add_argument("--csv", required=True, ...)`. argparse rejects
  any invocation without `--csv` outright.
- `consolidate.py:322,328` — `csv_path = Path(args.csv)` then `load_lastfm(csv_path)` is
  called unconditionally, with no `if args.csv` guard.

So the Spotify-only path does not work as documented — it errors at the arg parser before
any loading happens. This is a doc-vs-code contradiction (and likely a regression: the
unified-engine design notes explicitly list Spotify-only as a locked, working mode).

## Reproduce
`python3 consolidate.py --spotify-dir data --meta-dir data --out /tmp/x.db`
→ `error: the following arguments are required: --csv`

## Move (decide direction first)
- **If Spotify-only is intended to work** (design says yes): set `--csv` to optional
  (`required=False, default=None`), and guard the load — `lastfm = load_lastfm(csv_path)
  if args.csv else []` — plus verify the merge/write path tolerates an empty Last.fm set.
- **If not:** correct CLAUDE.md, SKILL.md, README to drop the Spotify-only claim.
The design intent (unified-engine locked decisions) points at the first.

## Done-when
Either `consolidate.py` runs Spotify-only with `--csv` omitted and produces a valid db, or
the three docs no longer advertise a Spotify-only path. Code and docs agree.

## Resolution (2026-07-22)
Took the "make code match docs" branch (design intent already locked Spotify-only as a
supported mode).

**Code (`consolidate.py`):**
- `--csv` argparse arg: `required=True` → `required=False, default=None`, help text updated
  to note it can be omitted for Spotify-only mode.
- `main()`: `csv_path = Path(args.csv)` unconditional → `csv_path = Path(args.csv) if
  args.csv else None`; the Last.fm load is now guarded: `if csv_path: lastfm =
  load_lastfm(csv_path) ... else: lastfm = []` (with a console line noting Spotify-only
  mode).
- Traced downstream: `merge_plays` is only invoked when `spotify` is non-empty
  (`plays = merge_plays(lastfm, spotify) if spotify else lastfm`), so an empty `lastfm`
  list never causes a divide-by-zero or index error there — `lf_index` just stays empty
  and every Spotify play falls through to `unmatched_spotify`. `write_db` iterates lists
  generically and has no Last.fm-specific branch. No further code changes needed.

**Verification:**
- Spotify-only: `python3 consolidate.py --spotify-dir data --meta-dir data --out
  <scratch>/sponly.db` → succeeded, 2,952 Spotify-sourced plays, 2,170 saved tracks, 68
  saved albums, 4,179 playlist items. `SELECT source, COUNT(*) FROM plays GROUP BY
  source` → `[('spotify', 2952)]` (no Last.fm rows, as expected).
- Regression: `python3 consolidate.py --csv data/edgarturtleblot.csv --spotify-dir data
  --meta-dir data --out <scratch>/full.db` → 84,967 total plays (82,015 lastfm + 369
  lastfm+spotify + 2,583 spotify), matching CLAUDE.md's documented expected result.
- `playlists.position` (mte-006 schema addition) on the Spotify-only db: 4,179 total
  rows, 0 with `position IS NULL`. Survives the fix.

**Docs:** Re-checked `CLAUDE.md`, `README.md`, `SKILL.md` Spotify-only invocations
against the fixed CLI signature (`--spotify-dir`, `--meta-dir`, `--out`, no `--csv`). All
three already matched the correct signature — they were accurate in intent, only the code
was wrong. No doc edits were needed.
