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
obvious next slice*. Status: **spec written and committed (`1633a96`), awaiting user
review. No code yet, no plan yet.**

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

Open design questions, none answered yet:

1. **Expiry ordering** — s2 `HumanGate` reads expiry BEFORE the decision (an approval must
   not self-approve late); s3 human-agent reads the answer FIRST (work product). A
   continue/stop decision looks like an approval, so it likely inverts s3 — but that must
   be argued, not inherited.
2. **What expiry DOES** — fail the loop, or converge it? Failing is louder.
3. **Per-iteration re-asking** is the point here, not the bug s3 fixed. The refusal must
   narrow to this one site without reopening the `Subgraph`-wrapper bypass.
4. **`torii` surface** — does `run agent answer --node "{loop}/{i}/__gate__"` serve it, or
   does the three-way cross-refusal need a fourth arm?

## Next command

After the user signs off on the spec: write the implementation plan to
`docs/superpowers/plans/2026-09-02-sp-6-s4-human-loop-gate.md`, then build it red-first.

Two preconditions the plan must RE-VERIFY rather than inherit from the spec: that
`RESERVED_GATE_ID` (`orchestrator-core/src/plan.rs:22`) still blocks a planner-authored
`__gate__` node, and that `validate_dag` really recurses into `Loop`/`Subgraph` bodies for
the new variant.

## Known-broken

Nothing. `$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it; use a
throwaway container on an `lsof`-checked free port, and remove it afterwards.

The **sensei daemon is not running**, so `/sensei:checkpoint` could not write the durable
record; this file is the only copy. `sensei start` to restore it.
