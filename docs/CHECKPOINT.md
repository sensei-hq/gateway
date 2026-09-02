# Checkpoint

**PR #49 is MERGED** — `78c5138` on `main`, 2026-09-02, 6 commits. `develop` and `main` are
now **identical (0/0)**. Everything through SP-6 plus the SP-6 follow-through is on `main`.

## Done

PR #49 closed the last of SP-6's review debt and a CI blind spot:

- `e987865` fix — the 9 code LOWs the SP-6 s3 + budget reviews left open, red-first.
- `9e68537` docs — 5 stale claims (item 12's false `load_since` claim is in the SP-DATA-5
  **spec**, not `scheduler.rs`).
- `171ccf5` test — a skipped Postgres test is **`ignored`, not counted as passed**. The
  blind spot was **48 tests**: every DB test early-returned and libtest counts that as a
  PASS. Workspace 1623/7 → **1575/55**; `55 − 7 = 48` closes exactly.
- `62f6cbd` docs — the 25th `cargo doc` private-item link was ours; back to the 24 baseline.
- `a530916` ci — digest-pinned `postgres:16` service container.
- `4287d91` docs — checkpoint.

**CI-verified on the merged run (33636750338), not just locally:** the PG suites really
ran — orchestrator **368/0**, store **69/0**; the only `ignored` in the whole run is one
pre-existing `cloud-providers` doctest. Both required checks green; merge state was CLEAN.

## Remaining

Nothing from SP-6. **Next slice: SP-6 s4, the human loop gate** — "a human decides whether
the loop continues", named by the s3 review as *the most valuable rejected site and the
obvious next slice*. Status: **spec (`1633a96`) and plan (`f7641c1`) both written,
committed and approved. NO CODE YET — task 1 of 14 is the next thing to do.**

Spec: `docs/superpowers/specs/2026-09-02-sp-6-s4-human-loop-gate-design.md` (20 ACs).
The four decisions it turns on, all made: an enumerated **menu** rather than free text; a
new **`GateSpec::Human { agent, menu }`** carrying the menu on the GRAPH (so `validate_dag`
can statically reject a menu with no stopping option); a new **`LoopGateAwaited`/
`LoopGateDecided`** event pair with `LoopGateOption { name, stops }`, because
`GateOutcome{Complete,Fail}` cannot express "continue"; and **expiry read BEFORE the
decision**, inverting s3 — "continue" authorizes another iteration of spend, so it is an
approval in the sense s2 built its ordering for.

What is already true (verified, not assumed): `GateSpec::Agent` **exists and works** for a
MODEL-backed agent (`fanout.rs:552`, driving the gate at `"{loop}/{i}/__gate__"`). The
slice is the HUMAN backing at that site, which `drive_agent`'s `!top_level` arm refuses
today — `fanout.rs:555` passes a literal `false`, and `non_top_level_sites` carries a
`"GateSpec::Agent"` row asserting the refusal.

Plan: `docs/superpowers/plans/2026-09-02-sp-6-s4-human-loop-gate.md` — 14 tasks, red-first,
with an AC→task coverage table. Order: types (1) → `validate_dag` (2) → journal (3) → fold
(4) → the shared question seam (5) → `run_human_loop_gate` (6) → wire `fanout.rs` (7) →
expiry ordering (8) → the three refusals (9) → bounds/redaction (10) → zero-spend/resume
(11) → torii (12) → PG e2e (13) → whole-slice review (14).

**Three preconditions VERIFIED against the code, not inherited:** `RESERVED_GATE_ID` is
enforced in `feasible` (`plan.rs:147`), so an untrusted planner cannot forge a gate node;
`validate_dag` recurses at block 2c, so a node-walking block fires at every nesting level;
and `support.rs:262` carries a standing instruction that the **fourth** writer of
`Fold::deadlines` must update three doc lists — task 4 discharges it and re-words it to
"a fifth".

**Two tests are mutation-proven, not assumed:** the nested-validation test (task 2) and
the expiry-ordering test (task 8) — the latter is the one that reddens if the arm is
"simplified" into s3's shape.

## Next command

Build task 1 (`GateSpec::Human` + `LoopGateOption`) red-first:

```
cargo test -p sensei-orchestrator-core a_human_gate_spec_round_trips_through_serde
```

Expect a COMPILE ERROR first — that is the red. Task 1 step 6 warns that the new variant
breaks the one exhaustive `match` on `GateSpec` (`fanout.rs`); add the temporary
`unreachable!` arm it specifies and delete it in task 7.

## Known-broken

Nothing. `$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it; use a
throwaway container on an `lsof`-checked free port, and remove it afterwards.

The **sensei daemon is not running**, so `/sensei:checkpoint` could not write the durable
record; this file is the only copy. `sensei start` to restore it.
