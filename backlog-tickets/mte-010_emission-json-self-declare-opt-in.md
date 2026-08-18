# CATALOG:
# id: mte-010
# kind: fyi
# status: open
# origin: monorepo-steward :: _index/backlog-tickets/index-0014 p2 (unified emission front) — propagating a now-live opt-in mechanism to every surface-owning project
# htttw_contribution: 0 — infra opt-in, no word-arc claim
# judgment_applied: ADR-0003 — this is an FYI, not a work-ask. Adopt or decline is music_taste_engine's call, not the steward's.
# provenance: filed-by monorepo-steward, 2026-08-18
# consumes: _index/tools/emission_discover.py `load_self_declared()`; _index/front/README.md
# produces: nothing required — informational
# effort: S if adopted
# tags: emission, front, fyi, opt-in

# emission.json self-declare is now live — music_taste_engine can opt in (or ignore this)

`_index/tools/emission_discover.py` scans D:/code by framework signature to build the unified
emission front (`_index/front/index.html`). music_taste_engine is currently listed by that
scan. As of 2026-08-18, a project can instead **self-declare** its surfaces by dropping an
`emission.json` at its own root — this now **overrides the scan for that project entirely**
(self-declared beats central), so a misdetected launch command or an undetectable surface can
be corrected without touching `_index`'s tree.

Schema:
```json
{"surfaces": [{"rel": "path/to/app.py", "type": "streamlit-app", "launch": "streamlit run path/to/app.py"}]}
```

**No action needed** if the scan already lists music_taste_engine correctly — this is purely
opt-in. Full detail: `_index/front/README.md`.
