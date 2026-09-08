# Checkpoint

**Slice: SP-7b.1 — minors pass. COMPLETE, Tasks 1–5.** Plan
`docs/superpowers/plans/2026-09-08-sp-7b-1-minors.md`. Parent SP-7b (its own `Task 1–8`,
`AC1–AC12`) is ON MAIN via PR #54, merged 2026-09-08 — `main` = `24d1868`.

## Done — `SP-7b.1 Task N`, not the parent slice's `Task 1–8`

- **Task 1** `4901bef` a duplicated tool name dropped every copy of the schema — matched by NAME,
  so a tool listed twice lost both while the note still said "1 of 3 omitted". Drops by POSITION
  now.
- **Task 2** `3f88cb7` guard the journaled `dropped_deps` — hard-wiring it to `0` left the whole
  workspace green. A real drop needs a HEADING wider than its share: a 1200-character node id.
- **Task 3** `194da8c` `torii run status` reports a budgeted turn. The fourth disclosure channel
  stopped at a worker `warn`. Additive, byte-identity pinned both ways.
- **Task 4** `4a45f2c` spec §5.2 corrected — it named the human path's wrapper as the model path's
  renderer, and claimed a test gap a 1.5 MB-vs-10-byte fixture had closed. §5.3 re-derived, HOLDS.
- **Task 5** `c205b4b` blackboard D5 false in BOTH halves: the `summary` branch cannot be entered
  (zero producers; `resolve_context` loads unconditionally), and the `PromptOverBudget` halt its
  deferral rested on was deleted by SP-7a, then reversed by SP-7b. AC7, §6 and §10 corrected —
  amended, not rewritten. Docs only.

Before this pass, in neither slice's numbering: `c177a72` the clamp-signal flake — a poisoned
`tracing` callsite, fixed so the branch was not sent to review red-capable.

## Verified

`cargo test --workspace` **1760 passed / 0 failed, real exit 0** across 35 suites (baseline 1755 at
`main` + this pass's five tests) · `clippy --all-targets -D warnings` 0 · `fmt --check` 0. Every
code fix reddens its own test alone under mutation.

## Next — nothing is open in SP-7b.1; two choices

1. **Open the develop→main PR** — the four minors, the D5 correction, plan + checkpoints.
   `develop` already contains main's merge commits, so it will not sit BEHIND.
2. **SP-7c** — no spec, named nowhere in the repo, so it begins at `/sensei:design`. The plan's
   closing section carries the two findings for that conversation: the multimodal window blind spot
   (`attachments` uncounted by the estimator) and compaction's §4.5 drift precondition.

**Sensei daemon NOT running — this file is the record.**
