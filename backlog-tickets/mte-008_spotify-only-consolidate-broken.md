# CATALOG:
# id: mte-008
# kind: ticket
# status: open
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
