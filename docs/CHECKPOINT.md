# Checkpoint

**Slice:** SP-6 s3 `human-as-Agent` — the last SP-6 slice, on `feat/sp-6-s3-human-as-agent`
(from `develop` at `771af25`). All 7 tasks + the whole-slice review's 16 fixes are in.
A RE-review of those fixes found 6 more; **the HIGH is fixed (`b6d4df0`), 5 remain.**

## Done

- Tasks 1–7 (`fa070dd` … `f482d05`), then the whole-slice review's 16 findings (`d2e8145`,
  `18d22e2`, `9b12470`, `02d6794`, `72fa55d`, + docs `4423c83`/`576bc1f`).
- **1592 passed / 0 failed / 7 ignored, exit 0**; clippy `-D warnings` and `fmt --check` exit 0.
- **The Postgres e2e was RE-RUN against current code** (it had gone stale behind 5 fix commits):
  7 passed / 0 failed, **zero skips**, `AgentAwaited=1` + `AgentAnswered=1` in `journal_events`.

## ⬜ Remaining — 6 findings from the re-review of the fixes, none fixed

1. ~~HIGH — the clamp ate the ASK~~ **FIXED `b6d4df0`.** `HumanQuestion` now records
   `task_bytes` and `redact_and_clamp` reserves the tail, so only `## Context` is ever cut.
   Mutation-verified: restoring the shipped clamp turns the guard red.
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

Merged to `develop`. Remaining findings are MEDIUM-and-below; pick them up before the
develop→main PR, or fold them into SP-6's close-out.

## Known-broken

Nothing failing; the HIGH is fixed. What remains is 3 MEDIUM (all documentation or an
unguarded ordering), 7 LOW and 7 rustdoc warnings. `$DATABASE_URL` is REMOTE Supabase — never run the DB
suite against it; use a throwaway container.
