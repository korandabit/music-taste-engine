"""
consolidate.py
──────────────
Merges all music data sources into a single SQLite database.

Sources:
  data/edgarturtleblot.csv        Last.fm export (98k plays, ~2006–present)
  data/StreamingHistory0.json     Spotify play history (2,952 plays, 2019–2020)
  data/Playlist1.json             Spotify playlists (73 playlists)
  data/YourLibrary.json           Spotify saved tracks + albums
  data/Inferences.json            SKIPPED (ad-targeting labels, not music data)

Output:
  data/music.db (SQLite)

Tables:
  plays           Unified play timeline. Spotify plays within ±5 min of a Last.fm
                  scrobble on the same (artist, track) are merged into one row,
                  gaining album from Last.fm and ms_played/is_skip from Spotify.
                  All other plays remain as separate rows with their source intact.
  library_tracks  Spotify saved tracks
  library_albums  Spotify saved albums
  playlists       Spotify playlist items (flattened)

Usage:
  python consolidate.py [--data-dir DATA_DIR] [--out OUT_DB]
"""

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path


# ─── Config ───────────────────────────────────────────────────────────────────

LASTFM_DATE_FORMAT  = "%d %b %Y %H:%M"
SPOTIFY_DATE_FORMAT = "%Y-%m-%d %H:%M"
MIN_YEAR            = 2005   # drop Last.fm pre-scrobble artifacts
MERGE_WINDOW_SEC    = 300    # ±5 min to consider a Spotify play == a Last.fm scrobble
SKIP_MS_THRESHOLD   = 30_000 # Spotify plays under 30s counted as skips


# ─── Parsing ──────────────────────────────────────────────────────────────────

def load_lastfm(path: Path) -> list[dict]:
    plays = []
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            parts = line.strip().split(",", 3)
            if len(parts) < 4:
                continue
            artist, album, track, raw_date = [p.strip().strip('"') for p in parts]
            try:
                dt = datetime.strptime(raw_date, LASTFM_DATE_FORMAT)
            except ValueError:
                continue
            if dt.year < MIN_YEAR:
                continue
            plays.append({
                "source":    "lastfm",
                "ts":        dt,
                "artist":    artist,
                "album":     album,
                "track":     track,
                "ms_played": None,
                "is_skip":   None,
            })
    return plays


def load_spotify_plays(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as fh:
        raw = json.load(fh)
    plays = []
    for r in raw:
        try:
            dt = datetime.strptime(r["endTime"], SPOTIFY_DATE_FORMAT)
        except (ValueError, KeyError):
            continue
        ms = r.get("msPlayed", 0)
        plays.append({
            "source":    "spotify",
            "ts":        dt,
            "artist":    r.get("artistName", "").strip(),
            "album":     None,
            "track":     r.get("trackName", "").strip(),
            "ms_played": ms,
            "is_skip":   1 if ms < SKIP_MS_THRESHOLD else 0,
        })
    return plays


def load_library(path: Path) -> tuple[list[dict], list[dict]]:
    with open(path, encoding="utf-8") as fh:
        d = json.load(fh)
    tracks = [
        {"artist": t.get("artist", ""), "album": t.get("album", ""), "track": t.get("track", "")}
        for t in d.get("tracks", [])
    ]
    albums = [
        {"artist": a.get("artist", ""), "album": a.get("album", "")}
        for a in d.get("albums", [])
    ]
    return tracks, albums


def load_playlists(path: Path) -> list[dict]:
    with open(path, encoding="utf-8") as fh:
        d = json.load(fh)
    rows = []
    for pl in d.get("playlists", []):
        name     = pl.get("name", "")
        modified = pl.get("lastModifiedDate", "")
        for item in pl.get("items", []):
            t = item.get("track")
            if not t:
                continue
            rows.append({
                "playlist_name":     name,
                "playlist_modified": modified,
                "artist":            t.get("artistName", ""),
                "album":             t.get("albumName", ""),
                "track":             t.get("trackName", ""),
            })
    return rows


# ─── Merge ────────────────────────────────────────────────────────────────────

def merge_plays(lastfm_plays: list[dict], spotify_plays: list[dict]) -> list[dict]:
    """
    Attempts to join each Spotify play to a Last.fm scrobble of the same
    (artist, track) within MERGE_WINDOW_SEC. Matched Last.fm rows gain
    ms_played + is_skip; matched Spotify rows are dropped (not double-counted).
    Unmatched rows from both sources are kept as-is.
    """
    # Index Last.fm plays in the Spotify date range by (artist_lower, track_lower)
    sp_start = min(p["ts"] for p in spotify_plays)
    sp_end   = max(p["ts"] for p in spotify_plays)

    lf_index = defaultdict(list)  # key → [(ts, list_index)]
    for i, p in enumerate(lastfm_plays):
        if sp_start.year <= p["ts"].year <= sp_end.year:
            key = (p["artist"].lower(), p["track"].lower())
            lf_index[key].append((p["ts"], i))

    matched_lf_indices = set()
    matched_sp_indices = set()

    for j, sp in enumerate(spotify_plays):
        key = (sp["artist"].lower(), sp["track"].lower())
        candidates = lf_index.get(key, [])
        best_i   = None
        best_gap = MERGE_WINDOW_SEC + 1
        for ts, i in candidates:
            if i in matched_lf_indices:
                continue
            gap = abs((sp["ts"] - ts).total_seconds())
            if gap < best_gap:
                best_gap = gap
                best_i   = i
        if best_i is not None:
            # Merge: enrich the Last.fm row with Spotify fields
            lastfm_plays[best_i]["source"]    = "lastfm+spotify"
            lastfm_plays[best_i]["ms_played"] = sp["ms_played"]
            lastfm_plays[best_i]["is_skip"]   = sp["is_skip"]
            matched_lf_indices.add(best_i)
            matched_sp_indices.add(j)

    # Collect unmatched Spotify plays
    unmatched_spotify = [sp for j, sp in enumerate(spotify_plays) if j not in matched_sp_indices]

    all_plays = lastfm_plays + unmatched_spotify
    all_plays.sort(key=lambda p: p["ts"])
    return all_plays


# ─── Database ─────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS plays (
    id          INTEGER PRIMARY KEY,
    source      TEXT NOT NULL,
    ts          TEXT NOT NULL,
    artist      TEXT,
    album       TEXT,
    track       TEXT,
    ms_played   INTEGER,
    is_skip     INTEGER
);

CREATE INDEX IF NOT EXISTS plays_ts     ON plays(ts);
CREATE INDEX IF NOT EXISTS plays_artist ON plays(artist);
CREATE INDEX IF NOT EXISTS plays_track  ON plays(track);

CREATE TABLE IF NOT EXISTS library_tracks (
    id      INTEGER PRIMARY KEY,
    artist  TEXT,
    album   TEXT,
    track   TEXT
);

CREATE TABLE IF NOT EXISTS library_albums (
    id      INTEGER PRIMARY KEY,
    artist  TEXT,
    album   TEXT
);

CREATE TABLE IF NOT EXISTS playlists (
    id                INTEGER PRIMARY KEY,
    playlist_name     TEXT,
    playlist_modified TEXT,
    artist            TEXT,
    album             TEXT,
    track             TEXT
);
"""


def write_db(db_path: Path, plays, lib_tracks, lib_albums, playlist_rows):
    if db_path.exists():
        db_path.unlink()

    con = sqlite3.connect(db_path)
    con.executescript(SCHEMA)

    con.executemany(
        "INSERT INTO plays (source, ts, artist, album, track, ms_played, is_skip) "
        "VALUES (:source, :ts, :artist, :album, :track, :ms_played, :is_skip)",
        [
            {**p, "ts": p["ts"].strftime("%Y-%m-%d %H:%M")}
            for p in plays
        ],
    )

    con.executemany(
        "INSERT INTO library_tracks (artist, album, track) VALUES (:artist, :album, :track)",
        lib_tracks,
    )

    con.executemany(
        "INSERT INTO library_albums (artist, album) VALUES (:artist, :album)",
        lib_albums,
    )

    con.executemany(
        "INSERT INTO playlists (playlist_name, playlist_modified, artist, album, track) "
        "VALUES (:playlist_name, :playlist_modified, :artist, :album, :track)",
        playlist_rows,
    )

    con.commit()
    con.close()


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data", help="Directory containing source files")
    ap.add_argument("--out",      default="data/music.db", help="Output SQLite path")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    out_path = Path(args.out)

    print("Loading Last.fm CSV...")
    lastfm = load_lastfm(data_dir / "edgarturtleblot.csv")
    print(f"  {len(lastfm):,} plays")

    print("Loading Spotify streaming history...")
    spotify = load_spotify_plays(data_dir / "StreamingHistory0.json")
    print(f"  {len(spotify):,} plays")

    print("Merging play timelines...")
    plays = merge_plays(lastfm, spotify)
    sources = {}
    for p in plays:
        sources[p["source"]] = sources.get(p["source"], 0) + 1
    for src, n in sorted(sources.items()):
        print(f"  {src}: {n:,}")
    print(f"  total: {len(plays):,}")

    print("Loading Spotify library...")
    lib_tracks, lib_albums = load_library(data_dir / "YourLibrary.json")
    print(f"  {len(lib_tracks):,} saved tracks, {len(lib_albums):,} saved albums")

    print("Loading Spotify playlists...")
    playlist_rows = load_playlists(data_dir / "Playlist1.json")
    print(f"  {len(playlist_rows):,} playlist items across playlists")

    print(f"Writing {out_path}...")
    write_db(out_path, plays, lib_tracks, lib_albums, playlist_rows)

    size_kb = out_path.stat().st_size // 1024
    print(f"Done. {out_path} ({size_kb:,} KB)")


if __name__ == "__main__":
    main()
