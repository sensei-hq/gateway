# Checkpoint

**Slice:** SP-6 s3 `human-as-Agent` — the last SP-6 slice, on `feat/sp-6-s3-human-as-agent`
(from `develop` at `771af25`). All 7 tasks + the whole-slice review's 16 fixes are in.
**A RE-review of those fixes found 6 more, NONE of them fixed yet.**

## Done

- Tasks 1–7 (`fa070dd` … `f482d05`), then the whole-slice review's 16 findings (`d2e8145`,
  `18d22e2`, `9b12470`, `02d6794`, `72fa55d`, + docs `4423c83`/`576bc1f`).
- **1590 passed / 0 failed / 7 ignored, exit 0**; clippy `-D warnings` and `fmt --check` exit 0.
- **The Postgres e2e was RE-RUN against current code** (it had gone stale behind 5 fix commits):
  7 passed / 0 failed, **zero skips**, `AgentAwaited=1` + `AgentAnswered=1` in `journal_events`.

## ⬜ Remaining — 6 findings from the re-review of the fixes, none fixed

1. **HIGH — the post-redaction clamp eats the ASK.** `human.rs:240` clamps from the END and
   `HumanQuestion::compose` (`human.rs:67-81`) puts `## Task` LAST, so a redaction that grows
   the authored half deletes the node input entirely: the human gets standing instructions +
   32 KiB of context and no statement of what to decide. Reintroduces the defect `## Task`
   exists to prevent; breaks §5.4's one-directional rule. **Also unguarded** — deleting the
   clamp leaves the whole suite green (mutation M14b).
   *Fix:* keep the pieces separate through redaction — bound the CONTEXT against the budget
   left after the redacted authored + task, so only the context half is ever cut.
   *Red test needs a real upstream producing >`MAX_HUMAN_CONTEXT_BYTES`; the authored half
   alone cannot reach the 36864 bound (4096 cap × ~1.67 growth ≈ 6.8 KB). No existing gateway
   helper returns a large canned output — that is the missing piece.*
2. MEDIUM — `--as` post-redaction ordering unguarded; reversing it leaves `sensei-torii` green.
   Mirror `an_answer_that_only_exceeds_the_cap_after_redaction_is_rejected` for the actor.
3. MEDIUM — spec §5.5 maps `run_map`→`fanout.rs:183` and `run_consolidate`→`:269`; both
   BACKWARDS (183 ∈ `run_consolidate`, 269 ∈ `run_map`). Drop the bare line numbers.
4. MEDIUM — `agents-skills-tools.md` and `durable-journal.md` still have ZERO mention of s3,
   though it changed `AgentDefinition`, `from_frontmatter` and `Registry::validate`, and added
   two `JournalEvent` variants. Three `SP-6-2` status markers need bumping to `SP-6-3`.
5. 7 LOW (stale doc sentences asserting the old single-4096 bound; `truncate_prompt_to_bound`
   overruns for a tiny `max`; `assemble_prompt` now has zero production callers).
6. 7 NEW rustdoc warnings — "links to private item" on `question` (`render.rs`) and `answer`
   (`cmd/human.rs:250`). Fix as code spans, not by widening visibility.

## Next command

Fix finding 1 first — red test, then the compose/clamp change:

```
sed -n '60,90p' crates/orchestrator/src/executor/human.rs   # compose puts ## Task last
```

## Known-broken

Nothing failing. The HIGH above is a latent data-loss path that fires only when redaction
grows a question past 36864 bytes. `$DATABASE_URL` is REMOTE Supabase — never run the DB
suite against it; use a throwaway container.
