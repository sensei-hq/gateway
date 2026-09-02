# Checkpoint

**Slice: SP-6 s4 — the human loop gate.** Spec (`1633a96`) and plan (`f7641c1`) approved.
**Task 1 of 14 DONE** (`91e3bbc` + `6a378f5`), both reviews closed. Task 2 is next.

Everything through SP-6 + its follow-through is on `main` (PR #49, `78c5138`, 2026-09-02);
`develop` is ahead by docs only. CI-verified on run 33636750338 that the PG suites really
ran (orchestrator 368/0, store 69/0) — the old blind spot was 48 tests early-returning,
which libtest counts as PASS.

## Done

s4 **spec** (20 ACs) and **plan** (14 tasks, red-first, AC→task table). Four decisions: an
enumerated **menu** not free text; **`GateSpec::Human { agent, menu }`** with the menu on
the GRAPH, so `validate_dag` can statically reject a loop that cannot converge; a new
**`LoopGateAwaited`/`LoopGateDecided`** pair, because `GateOutcome{Complete,Fail}` cannot
express "continue"; and **expiry read BEFORE the decision**, inverting s3 — "continue"
authorizes another iteration of spend, so it is an approval in s2's sense.

## Remaining

All 14 build tasks. Three preconditions **verified against the code**, not inherited:
`RESERVED_GATE_ID` is enforced in `feasible` (`plan.rs:147`); `validate_dag` recurses at
block 2c; `support.rs:262`'s standing instruction for the **fourth** `Fold::deadlines`
writer is task 4's job. Two tests must be **mutation-proven**: nested validation (task 2)
and expiry ordering (task 8) — the latter reddens if the arm is "simplified" into s3's.

## Next command

Task 2 — `validate_dag` rejects a human loop gate that cannot converge:

```
cargo test -p sensei-orchestrator-core human_loop_gate
```

Expect 6 failures. `fanout.rs` carries a temporary `fail_loop` STUB arm (not a panic —
`GateSpec::Human` is reachable from untrusted planner output, and a panic there poisons
the worker through `Scheduler::tick`). Task 7 deletes it.

## Known-broken

Nothing. `$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it; use a
throwaway container on an `lsof`-checked free port, removed afterwards. The **sensei daemon
is not running**, so this file is the only durable record; `sensei start` restores it.
