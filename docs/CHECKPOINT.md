# Checkpoint

**Slice: SP-6 s4 — the human loop gate.** Spec (`1633a96`) and plan (`f7641c1`) approved.
**Tasks 1–3 of 14 DONE**, all three review rounds closed. **Task 4 (the fold) is next.**

Everything through SP-6 is on `main` (PR #49, `78c5138`); `develop` is ahead by this slice.
`$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it; use a throwaway
container on an `lsof`-checked free port.

## Done

- **Task 1** (`91e3bbc` + `6a378f5`) — `GateSpec::Human { agent, menu }` + `LoopGateOption`.
- **Task 2** (`7cf61a5` + `bbf4002`) — `validate_dag` rejects a loop gate that cannot
  converge (empty menu, empty/duplicate names, no stopping option), recursing into nests.
- **Task 3** (`e542c2a` + this commit) — `LoopGateAwaited`/`LoopGateDecided` + the two
  `label` arms in `executor/tests.rs`. The review corrected three false doc claims (the
  drift-vector list, "`GateDecided` carries a `GateOutcome`", the `actor: Option`
  rationale), replaced a vacuous round-trip test, and fixed Task 3's own plan steps.

## Remaining

Tasks 4–14. Standing obligations, none discharged: `support.rs:262`'s instruction for the
**fourth** `Fold::deadlines` writer is Task 4's; **expiry ordering (Task 8) must be
mutation-proven** — it reddens only if the arm is "simplified" into s3's answer-first order;
`LoopGateDecided.actor` is now a required `String` (promoted before Task 6, the first writer;
spec §4 + the plan's Task 3 Step 1 carry the reasoning), so Task 12's remaining half is only
to route the loop branch through `actor_or_user`; Task 14 also owns
`docs/features/orchestrator/{durable-journal,README}.md`, which drift with this slice.
**Doc-link baseline: 16** (re-measured with Task 14 Step 3's command; the plan's 24 was stale).

## Next command

Task 4 — fold the two events (`LoopGateAwaited` FIRST-wins into the shared `deadlines` map,
`LoopGateDecided` LAST-wins): `cargo test -p sensei-orchestrator loop_gate`.
`fanout.rs` still carries the temporary `fail_loop` STUB arm (not a panic — `GateSpec::Human`
is reachable from untrusted planner output, and a panic there poisons the worker through
`Scheduler::tick`). Task 7 deletes it.

## Known-broken

Nothing. The **sensei daemon is not running**, so this file is the only durable record.
