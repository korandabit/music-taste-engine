# Feedback Loops: skill-creator × music-data-engine

## What skill-creator actually gives you

Three mechanisms, in ascending value:

| Mechanism | What it is | Music engine relevance |
|---|---|---|
| **Eval loop** | Test prompts → graded outputs → revise SKILL.md → repeat | Medium — useful for tuning *how Claude invokes the engine* (which flags, which flow) |
| **Description optimizer** | Automated trigger-rate optimization (only in Claude Code) | Low — trigger is already solid |
| **Logging / state persistence** | Not built-in — but the pattern is: write outputs to a file the skill reads next session | **High** — this is the self-updating mechanism |

---

## The recommendation logging mechanism (concrete)

The skill-creator pattern that matters here is **bundled state**: the skill directory can include files that persist across sessions. The music engine already does this with `music.db`. You extend that same pattern.

### What you'd add

```
music-data-engine/
├── SKILL.md
├── music.db          ← already exists
├── engine.py
└── recommendation_log.db   ← NEW
```

`recommendation_log.db` schema:

```sql
CREATE TABLE recommended (
  ts           TEXT,          -- when recommended
  artist       TEXT,
  track        TEXT,
  context      TEXT,          -- "Sunday drive", "spring", etc.
  energy       TEXT,          -- low/medium/high
  playlist_run TEXT           -- unique run ID
);

CREATE TABLE feedback (
  ts           TEXT,
  artist       TEXT,
  track        TEXT,
  signal       TEXT           -- 'played', 'skipped', 'loved' (user-provided)
);
```

### How it closes the loop

**Session N** — Claude runs `engine.py playlist`, logs output to `recommendation_log.db`

**Session N+1** — Before scoring, engine queries log:
- Tracks recommended in last 90d → boost `min-rest` effectively (already heard them "virtually")
- Tracks user marked skipped → apply as soft signal (similar to `skip_rate` but for recommendations)
- Tracks user marked loved → candidate for `PERENNIAL_RETURN` reclassification

This doesn't require rewriting `engine.py`. It's a **pre-filter and post-weight layer** the SKILL.md instructs Claude to run via inline SQL before invoking `playlist`.

---

## What skill-creator eval loops actually help with

The eval loop is most useful for testing *Claude's behavior when invoking the engine*, not the engine itself:

- Does Claude run `profile` before `playlist` consistently? (it should, per the skill)
- Does Claude correctly map "stuff I haven't heard in a while" → `--min-rest 180`?
- Does Claude warn when saved pool is thin?

These are the regressions that matter. You'd write ~10 test prompts, grade whether the right flags were used, iterate on SKILL.md phrasing.

---

## Priority order for actual improvement

1. **Recommendation log** — highest signal-to-effort; pure SQL + one new table
2. **Eval loop on Claude's invocation behavior** — useful after any SKILL.md revision
3. **Description optimizer** — only if trigger rate becomes a problem (it isn't now)

---

## The self-updating recommendation in practice

User says: *"make me a playlist"*

Claude:
1. Queries `recommendation_log.db` — finds 12 tracks recommended last 3 sessions
2. Passes those as `--exclude-tracks` (or equivalent inline filter) to `playlist`
3. Logs new output back to `recommendation_log.db`
4. Over time: the log IS the feedback loop — no Spotify API, no external service

The missing piece: **you need to add `--exclude-tracks` support to `engine.py`**, or Claude handles exclusion post-hoc by filtering the JSON output before presenting it.
