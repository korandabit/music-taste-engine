# CATALOG:
# id: mte-009
# kind: fyi
# status: open-fyi
# origin: filed from the `audio` project 2026-08-01 while executing audio-0006 (joining Mark's own
#   Last.fm scrobbles to his song registry). Measured directly against data/edgarturtleblot.csv.
# htttw_contribution: 0 — data-semantics note, no manuscript relevance.
# judgment_applied: touch-another-project v1/v2 (ADR-0003) — `audio` only CONSUMES this export and
#   changed nothing here; this is the v2 recommendation routed to the owner. Fully declinable.
# provenance: filed-by audio (2026-08-01), counts measured from data/edgarturtleblot.csv (98,550 rows)
# consumes: data/edgarturtleblot.csv; consolidate.py; SKILL.md (top_albums, discovery_latency)
# produces: (proposed) a documented caveat, or an exclusion, for user-authored album strings
# effort: S
# tags: data-quality, lastfm, album-column, self-scrobbles, declinable, fyi
# gloss: the Last.fm `album` field is user-authored for locally-tagged files — on Mark's own 1,062
#   scrobbles it is an era/place label ("Grad 2014", "307 in Augusta"), not an album

# The `album` column is partly hand-authored era/place metadata, not album titles

**FYI, not a work-ask. Declinable at your discretion (ADR-0003).** The `audio` project read this
export read-only and changed nothing in this repo.

## What was measured
`data/edgarturtleblot.csv` has 98,550 rows. 1,062 of them are Mark's own music
(`Mark Koranda` 1014, `Ortiz and Koranda` 29, `Mark Koranda ft. Andy` 19). Across those rows the
`album` column takes 26 distinct values, and they are **eras and places, not records**:

`Grad 2014` (179), `Spring 2012` (115), `Shallowford Court` (61), `2006 Penn` (50),
`Musical Notes II` (47), `Firefly` (46), `St. Thomas the Tree` (44), `Yakbomb surplus` (30),
`Fall 2011` (17), `2013 Demo` (16), `307 in Augusta` (9), `Grad 2013` (7), `2013 Grad` (5) …
plus 329 blank.

Three consequences worth knowing:

1. **The labels leak beyond his own artist rows.** `Shallowford Court` appears on 37 rows by *other*
   artists, `Firefly` on 16, `Yakbomb surplus` on 10 — he tagged whatever local files were in that
   folder at the time. So this is not cleanly separable by `artist LIKE '%Koranda%'`.
2. **`Unnamed Album` (2,832 rows) is a null sentinel**, not an album title.
3. **The labels have spelling variants** that will split any group-by:
   `Grad 2013` / `2013 Grad`, `St. Thomas the Tree` / `St thomas the tree`,
   `Don't Say Please` / `Post Don't Say Please`.

## Why it might matter here
`SKILL.md` exposes `top_albums` (top 25 by play count) and `discovery_latency` ("tracks first heard
>60 days after their album's debut"). Both treat `album` as a release identity. For the ~1,200
locally-tagged rows that premise does not hold: an "album debut" for `Grad 2014` is a folder-naming
date, not a release. At 1.2% of the corpus this is very unlikely to move a top-25 list, so this may
be entirely beneath your threshold — hence `fyi`, not `ticket`.

## Options (yours to pick, or to decline)
- Do nothing; note the caveat only if a per-album analysis ever gets used at fine grain.
- Document the semantics in `SKILL.md` near the `plays.album` column description.
- Add a `library_local` / `user_tagged` flag on `plays` so album-shaped analyses can exclude them.

## Where the consumer lives
`D:/Dropbox/audio/songwriting/_registry/lastfm-crossmap.json` +
`D:/Dropbox/audio/songwriting/_registry/lastfm-crossmap.md` — audio treats these strings as an
`era_tags[]` dimension (a 20-year hand-built index of his own eras), which is exactly why the
semantics came up. Generator: `D:/Dropbox/audio/tools/build_lastfm_crossmap.py`. Audio ticket:
`D:/Dropbox/audio/backlog-tickets/audio-0006_lastfm-own-scrobbles-second-oracle.md`.

## Done when
The owner records a disposition (adopt / document / decline). No build is asked of this project.
