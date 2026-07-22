#!/usr/bin/env python3
"""
registry.py — provenance substrate for music_taste_engine analyses.

The problem this solves: analyses here used to conclude into prose and evaporate.
A run's levers, its stats, and the data snapshot it stood on emitted nowhere, so
nothing self-described and nothing knew when it was stale. This is the thin layer
every analysis emits to so the ecosystem self-manages instead of needing babysitting.

The unit is a RUN. When an analysis finishes it calls `emit(...)` with:
  - analysis id + version
  - levers   : the knobs it ran with (the tunable surface)
  - stats    : the numbers it produced (the surface, not a verdict)
  - findings : zero or more CLAIMS, each a stable id + human sentence + status
It is stamped with the current data SNAPSHOT (a fingerprint of music.db + source
file freshness). Two append-only, tracked jsonl stores are the source of truth:

  analysis_runs.jsonl   one line per run (full history; never rewritten)
  findings.jsonl        current claims (latest run per finding id wins; history in runs)

Rendered glance-surfaces (regenerated, tracked):
  FINDINGS.md   the claims canon — what we know, traceable to analysis+snapshot+date
  PIPELINE.md   data sources (integrated/un-integrated + freshness) and analyses
                (last run, levers, key stats, STALE? vs current snapshot)

Staleness is derived, not stored: a finding is stale iff the data snapshot it ran
on differs from the current one. Glance at PIPELINE.md → see what needs re-running.

stdlib only. Import and call `emit`; or run the CLI (snapshot / render / status / backfill).
"""
from __future__ import annotations
import hashlib, json, sqlite3, sys
from datetime import datetime, timezone
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except (AttributeError, ValueError):
    pass

ROOT = Path(__file__).resolve().parent
RUNS_PATH     = ROOT / "analysis_runs.jsonl"
FINDINGS_PATH = ROOT / "findings.jsonl"
FINDINGS_MD   = ROOT / "FINDINGS.md"
PIPELINE_MD   = ROOT / "PIPELINE.md"
DEFAULT_DB    = ROOT / "data" / "music.db"

# Known data-source classification (what consolidate.py ingests vs what sits idle).
# Kept explicit here so PIPELINE.md renders the integration map without guessing.
DATA_SOURCES = {
    "data/edgarturtleblot.csv":                    ("integrated", "Last.fm scrobbles"),
    "data/StreamingHistory0.json":                 ("integrated", "Spotify plays (2020 export)"),
    "data/YourLibrary.json":                       ("integrated", "Spotify saved tracks/albums"),
    "data/Playlist1.json":                         ("integrated", "Spotify playlists (2020)"),
    "data/df26 spotify/StreamingHistory_music_0.json": ("integrated", "Spotify plays (2026 export)"),
    "data/df26 spotify/YourLibrary.json":          ("integrated", "Spotify saved (2026)"),
    "data/df26 spotify/Playlist1.json":            ("integrated", "Spotify playlists (2026)"),
    "data/df26 spotify/SearchQueries.json":        ("un-integrated", "search intent — behavioral, not yet ingested"),
    "data/df26 spotify/Wrapped2025.json":          ("un-integrated", "Spotify's own top-N summary — aggregation ground-truth"),
    "data/df26 spotify/YourSoundCapsule.json":     ("un-integrated", "Spotify's own listening summary — ground-truth"),
    "data/df26 spotify/Marquee.json":              ("skipped", "artist-marketing exposure (not your action)"),
    "data/df26 spotify/Identifiers.json":          ("skipped", "account metadata"),
    "data/df26 spotify/Identity.json":             ("skipped", "account metadata"),
    "data/Inferences.json":                        ("skipped", "ad-targeting labels (not music)"),
    "data/df26 spotify/Inferences.json":           ("skipped", "ad-targeting labels (not music)"),
    "recommendation_log.db":                       ("output", "tool outflow — what was served; awaits roundtrip reintegration"),
}

STATUS_VALUES = {"provisional", "supported", "refuted", "untestable", "blocked"}


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ─── snapshot ────────────────────────────────────────────────────────────────
def snapshot(db_path: Path = DEFAULT_DB) -> dict:
    """Fingerprint the current data state: db signature + source-file freshness.

    The signature (row counts + ts range) is what staleness compares on — it moves
    only when the underlying corpus changes. File mtimes are reported for the human
    freshness read but excluded from the hash (touching a file shouldn't fake a rerun).
    """
    sig: dict = {"db": str(db_path.relative_to(ROOT)) if db_path.is_relative_to(ROOT) else str(db_path)}
    if db_path.exists():
        con = sqlite3.connect(db_path)
        try:
            sig["plays_total"] = con.execute("SELECT COUNT(*) FROM plays").fetchone()[0]
            sig["plays_by_source"] = dict(con.execute(
                "SELECT source, COUNT(*) FROM plays GROUP BY source ORDER BY source").fetchall())
            sig["ts_range"] = list(con.execute(
                "SELECT MIN(ts_utc), MAX(ts_utc) FROM plays").fetchone())
            sig["playlists"] = con.execute("SELECT COUNT(*) FROM playlists").fetchone()[0]
            sig["library_tracks"] = con.execute("SELECT COUNT(*) FROM library_tracks").fetchone()[0]
        except sqlite3.OperationalError as e:
            sig["error"] = str(e)
        finally:
            con.close()
    else:
        sig["error"] = "db not found"

    canon = json.dumps({k: sig[k] for k in sorted(sig) if k != "db"}, sort_keys=True)
    fingerprint = hashlib.sha1(canon.encode()).hexdigest()[:12]

    sources = {}
    for rel, (state, desc) in DATA_SOURCES.items():
        p = ROOT / rel
        mtime = (datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d")
                 if p.exists() else None)
        sources[rel] = {"state": state, "desc": desc, "present": p.exists(), "mtime": mtime}

    return {"fingerprint": fingerprint, "signature": sig, "sources": sources, "taken_at": _now()}


# ─── emit ────────────────────────────────────────────────────────────────────
def emit(analysis: str, version: str, levers: dict, stats: dict,
         findings: list[dict], db_path: Path = DEFAULT_DB, render_after: bool = True) -> dict:
    """Append a run to analysis_runs.jsonl and upsert its findings into findings.jsonl.

    findings: list of {id, claim, status, confidence?, caveats?}. `id` is a stable
    key (e.g. "mte006.playlist_fatigue"); a new run supersedes the prior claim of the
    same id, with the superseded version retained in the runs history.
    """
    snap = snapshot(db_path)
    run = {
        "run_id": f"{analysis}@{_now()}",
        "analysis": analysis, "version": version, "run_date": _now(),
        "snapshot_fp": snap["fingerprint"], "snapshot_sig": snap["signature"],
        "levers": levers, "stats": stats,
        "finding_ids": [f["id"] for f in findings],
    }
    _append(RUNS_PATH, run)

    existing = {f["id"]: f for f in _read(FINDINGS_PATH)}
    for f in findings:
        if f.get("status") not in STATUS_VALUES:
            raise ValueError(f"finding {f.get('id')!r} status {f.get('status')!r} not in {STATUS_VALUES}")
        prior = existing.get(f["id"])
        existing[f["id"]] = {
            "id": f["id"], "analysis": analysis, "claim": f["claim"],
            "status": f["status"], "confidence": f.get("confidence"),
            "caveats": f.get("caveats"), "levers": levers,
            "key_stats": f.get("key_stats", {}),
            "snapshot_fp": snap["fingerprint"], "run_date": run["run_date"],
            "run_count": (prior["run_count"] + 1) if prior else 1,
        }
    _write(FINDINGS_PATH, [existing[k] for k in sorted(existing)])
    if render_after:
        render(db_path)
    return run


# ─── jsonl io ────────────────────────────────────────────────────────────────
def _append(path: Path, rec: dict) -> None:
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")

def _read(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(ln) for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]

def _write(path: Path, recs: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        for r in recs:
            fh.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")


# ─── render ──────────────────────────────────────────────────────────────────
def render(db_path: Path = DEFAULT_DB) -> None:
    snap = snapshot(db_path)
    cur_fp = snap["fingerprint"]
    findings = _read(FINDINGS_PATH)
    runs = _read(RUNS_PATH)
    today = datetime.now().strftime("%Y-%m-%d")

    # ── FINDINGS.md ──
    L = [f"# Findings canon — music_taste_engine",
         f"", f"Last rendered: {today} · current data snapshot `{cur_fp}` · "
         f"generated by `registry.py` (do not hand-edit).", ""]
    L += ["What we've learned, each claim traceable to the analysis, data snapshot, and",
          "date that produced it. **STALE** = the finding ran on a snapshot other than the",
          "one now on disk; re-run its analysis to refresh.", "",
          "| status | finding | claim | analysis | ran | stale? |",
          "|---|---|---|---|---|---|"]
    order = {"refuted": 0, "supported": 1, "provisional": 2, "untestable": 3, "blocked": 4}
    for f in sorted(findings, key=lambda x: (order.get(x["status"], 9), x["id"])):
        stale = "**STALE**" if f.get("snapshot_fp") != cur_fp else "ok"
        claim = f["claim"].replace("\n", " ")
        claim = (claim[:110] + "…") if len(claim) > 111 else claim
        conf = f' ({f["confidence"]})' if f.get("confidence") else ""
        L.append(f"| {f['status']}{conf} | `{f['id']}` | {claim} | `{f['analysis']}` "
                 f"| {f['run_date'][:10]} | {stale} |")
    L += ["", "## Caveats", ""]
    for f in sorted(findings, key=lambda x: x["id"]):
        if f.get("caveats"):
            L.append(f"- `{f['id']}` — {f['caveats']}")
    FINDINGS_MD.write_text("\n".join(L) + "\n", encoding="utf-8")

    # ── PIPELINE.md ──
    P = [f"# Pipeline status — music_taste_engine", "",
         f"Last rendered: {today} · current data snapshot `{cur_fp}` · "
         f"generated by `registry.py` (do not hand-edit).", "",
         "The self-management surface: what data is integrated, how fresh it is, and",
         "which analyses have run against the current corpus vs. gone stale.", "",
         "## Data snapshot (current)", ""]
    sg = snap["signature"]
    if "plays_total" in sg:
        P.append(f"- **plays**: {sg['plays_total']:,} — " +
                 ", ".join(f"{k} {v:,}" for k, v in sg["plays_by_source"].items()))
        P.append(f"- **ts range**: {sg['ts_range'][0]} → {sg['ts_range'][1]}")
        P.append(f"- **playlists**: {sg['playlists']:,} · **library tracks**: {sg['library_tracks']:,}")
    P += ["", "## Data sources (integration + freshness)", "",
          "| source | state | last export | note |", "|---|---|---|---|"]
    order2 = {"integrated": 0, "un-integrated": 1, "output": 2, "skipped": 3}
    for rel, meta in sorted(snap["sources"].items(),
                            key=lambda kv: (order2.get(kv[1]["state"], 9), kv[0])):
        if not meta["present"]:
            continue
        P.append(f"| `{rel}` | {meta['state']} | {meta['mtime'] or '—'} | {meta['desc']} |")

    P += ["", "## Analyses (last run vs current snapshot)", "",
          "| analysis | last run | on snapshot | stale? | levers | key stats |",
          "|---|---|---|---|---|---|"]
    latest: dict = {}
    for r in runs:
        latest[r["analysis"]] = r  # jsonl is append-order → last wins
    for name, r in sorted(latest.items()):
        stale = "**STALE**" if r.get("snapshot_fp") != cur_fp else "ok"
        lev = ", ".join(f"{k}={v}" for k, v in list(r.get("levers", {}).items())[:5]) or "—"
        st = ", ".join(f"{k}={v}" for k, v in list(r.get("stats", {}).items())[:5]) or "—"
        P.append(f"| `{name}` | {r['run_date'][:10]} | `{r.get('snapshot_fp','?')}` | {stale} "
                 f"| {lev} | {st} |")
    P += ["", "_Levers are the tunable knobs each analysis exposes; re-running with different",
          "values appends a run and updates this row (surface, not verdict)._"]
    PIPELINE_MD.write_text("\n".join(P) + "\n", encoding="utf-8")
    print(f"[registry] rendered FINDINGS.md + PIPELINE.md (snapshot {cur_fp}, "
          f"{len(findings)} findings, {len(latest)} analyses)")


# ─── status ──────────────────────────────────────────────────────────────────
def status(db_path: Path = DEFAULT_DB) -> None:
    snap = snapshot(db_path)
    findings = _read(FINDINGS_PATH)
    stale = [f for f in findings if f.get("snapshot_fp") != snap["fingerprint"]]
    print(f"current snapshot: {snap['fingerprint']}")
    print(f"findings: {len(findings)}  |  stale: {len(stale)}")
    for f in stale:
        print(f"  STALE  {f['id']}  (ran on {f.get('snapshot_fp')})")
    unintegrated = [r for r, m in snap["sources"].items()
                    if m["state"] == "un-integrated" and m["present"]]
    if unintegrated:
        print("un-integrated data present:", ", ".join(unintegrated))


def main():
    import argparse
    ap = argparse.ArgumentParser(description="registry.py — analysis provenance substrate")
    ap.add_argument("cmd", choices=["snapshot", "render", "status", "backfill"])
    ap.add_argument("--db", default=str(DEFAULT_DB))
    args = ap.parse_args()
    db = Path(args.db)
    if args.cmd == "snapshot":
        print(json.dumps(snapshot(db), indent=2, default=str))
    elif args.cmd == "render":
        render(db)
    elif args.cmd == "status":
        status(db)
    elif args.cmd == "backfill":
        from registry_backfill import backfill
        backfill(db)


if __name__ == "__main__":
    main()
