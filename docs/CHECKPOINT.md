# Checkpoint

**Slice: SP-6 s4 — the human loop gate.** Spec (`1633a96`) and plan (`f7641c1`) written,
committed, approved. **NO CODE YET — task 1 of 14 is next.**

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

```
cargo test -p sensei-orchestrator-core a_human_gate_spec_round_trips_through_serde
```

Expect a COMPILE ERROR — that is the red. The new variant breaks the one exhaustive
`match` on `GateSpec` (`fanout.rs`); add task 1 step 6's temporary `unreachable!` arm,
delete it in task 7.

## Known-broken

Nothing. `$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it; use a
throwaway container on an `lsof`-checked free port, removed afterwards. The **sensei daemon
is not running**, so this file is the only durable record; `sensei start` restores it.
