# Checkpoint

**Slice: SP-6 s4 — the human loop gate.** Spec (`1633a96`) and plan (`f7641c1`) approved.
**Tasks 1–4 of 14 DONE**, every review round closed. **Task 5 (the question seam) is next.**

Everything through SP-6 is on `main` (PR #49, `78c5138`); `develop` is ahead by this slice.
`$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it; use a throwaway
container on an `lsof`-checked free port.

## Done

- **Task 1** (`91e3bbc` + `6a378f5`) — `GateSpec::Human { agent, menu }` + `LoopGateOption`.
- **Task 2** (`7cf61a5` + `bbf4002`) — `validate_dag` rejects a loop-gate menu that cannot
  converge (empty, empty/duplicate names, no stopping option), recursing into nests.
- **Task 3** (`e542c2a` + `2d254ad`) — `LoopGateAwaited`/`LoopGateDecided` + the two `label`
  arms in `executor/tests.rs`; its review killed three false doc claims and a vacuous test.
- **Task 4** (`c03777c` + `d749b4e`) — the fold: ask FIRST-wins (into the SHARED `deadlines`
  map, `None` folded THROUGH), decision LAST-wins, mutation-verified eleven ways. It DISCHARGED
  `support.rs:262`'s fourth-writer instruction, which now asks the same of a FIFTH.
- **Out of band, Tasks 4→5** (`7bea6cb` + its review follow-up) — `LoopGateDecided.actor` is now
  a required `String`; a journal-shape change, cheap only before Task 6, the first writer.

## Remaining

Tasks 5–14. Standing obligations: **expiry ordering (Task 8) must be mutation-proven** — it reddens
only if the arm is "simplified" into s3's answer-first order; Task 12 must route the loop branch
through `actor_or_user`, now the ONLY guard left against a blank audit row; Task 14 owns
`docs/features/orchestrator/{durable-journal,README}.md`. **Doc-link baseline: 16** (24 was stale).

## Next command

Task 5 — extract `human_question_for`, the seam `drive_agent`'s human branch and Task 6's
`run_human_loop_gate` share: `cargo test -p sensei-orchestrator the_human_question_seam_composes`.
`fanout.rs:432` still carries the temporary `fail_loop` STUB arm (not a panic — `GateSpec::Human`
is reachable from untrusted planner output, and a panic poisons the worker). Task 7 deletes it.

## Known-broken

Nothing. The **sensei daemon is not running**, so this file is the only durable record.
