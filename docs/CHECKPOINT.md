# Checkpoint

**Slice:** SP-6 s3 `human-as-Agent` (merged to `develop` at `4026a90`). The whole-slice
adversarial re-review's **15 findings are all CLOSED** — 14 confirmed and fixed, 1 a
duplicate (#4 and #13 are the same `authored_bytes` gap).

## Done

- `98b83e7` fix(orchestrator) — the THIRD kind-swap direction. A node bearing another
  kind's awaited record, driven as `AwaitSignal`, re-paused with `resume_after: None`
  forever while `run signal` refused it and `run agent answer` reported exit 0. New
  `Fold::signal_asks` + `has_signal_ask`, the counterpart of `menu_for`/`prompt_for`.
- `b700339` fix(orchestrator) — `redact_and_clamp` redacted the question as TWO passes
  split at `## Task`, so a `PatternRedactor` whole-match spanning the boundary (the PEM
  rule) reached `journal_events` in the clear. Whole-string pass; split located in the
  REDACTED text via a shared `TASK_MARKER`.
- `b8a4665` fix(torii) — `list-paused`'s `agent:` cell cut from the FRONT, so for any
  question over ~290 chars the operator saw standing instructions and never the ask.
  `render::question_cell` reserves the tail exactly as the journal clamp does.
- `ad3686e` test(orchestrator) — three surviving mutations pinned: the `## Task` term in
  `authored_bytes`, `MAX_HUMAN_CONTEXT_BYTES` (its `truncated` marker assertion is now
  ANCHORED inside `## Context`), and the even budget split across dependencies.
- `fffecdd` test(torii) — FIRST-wins on the question at both torii sites (both survived
  mutation; only the executor's copy was guarded).
- `feb6d27` docs — six sites still said the executor journals the prompt UNREDACTED and/or
  that the whole question is bounded at 4096; plus the overview's s3 branch parenthetical.

**Measured:** `cargo test --workspace` **1601 passed / 0 failed / 7 ignored, exit 0**.
`clippy -D warnings` exit 0. `fmt --check` exit 0. `cargo doc` "links to private item"
back to **24** (the baseline). Postgres e2e re-run against a throwaway container on 55432
(removed afterwards): **7 passed, zero skips**; `orchestrator-store --features postgres`
**66 passed**; the durable `AgentAwaited.prompt` inspected in `journal_events`.

## Remaining

The re-review's **7 LOW** only: `truncate_prompt_to_bound` overruns for a tiny `max`;
`assemble_prompt` has zero production callers; assorted prose. (The "stale single-4096
bound" LOW is now closed by `feb6d27`.)

## Next command

Pick up the 7 LOW, or open the develop→main PR. `main` is strict — merge `origin/main`
into `develop` first or the PR sits BEHIND.

## Known-broken

Nothing. `$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it; use a
throwaway container (`docker run -p 55432:5432 postgres:16`, then
`psql < database/_apply_all.sql`).
