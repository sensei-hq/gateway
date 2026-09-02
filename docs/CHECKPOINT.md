# Checkpoint

**Slice: SP-6 s4 — the human loop gate.** Spec (`1633a96`) and plan (`f7641c1`) approved.
**Tasks 1–7 of 14 DONE**, every review round closed. **Task 8 is next.**

Everything through SP-6 is on `main` (PR #49, `78c5138`); `develop` is ahead by this slice.
`$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it; use a throwaway
container on an `lsof`-checked free port.

## Done

- **Tasks 1–4** (`91e3bbc`/`6a378f5`, `7cf61a5`/`bbf4002`, `e542c2a`/`2d254ad`,
  `c03777c`/`d749b4e`) — `GateSpec::Human` + `LoopGateOption`; `validate_dag` rejects a menu
  that cannot converge, at every depth; the `LoopGateAwaited`/`LoopGateDecided` variants;
  the fold (ask FIRST-wins into the SHARED `deadlines` map, decision LAST-wins).
- **Out of band, 4→5** (`7bea6cb`) — `LoopGateDecided.actor` narrowed to a required `String`.
- **Task 5** (`9286702` + `7c671ea`) — the `human_question_for` seam.
- **Tasks 6+7** (`6a20fea` + this commit's review fixes) — `run_human_loop_gate` at
  `{loop}/{i}/__gate__` and `run_loop`'s third gate arm. The Task 7 `fail_loop` STUB is
  GONE. The review found a **Critical**: `run_loop` re-derives every gate on every drive, so
  once wall-clock passed iteration 0's deadline an ALREADY-HONOURED decision reported
  `Expired` and killed the whole `Loop` — any multi-iteration loop with a finite SLA, and
  even a loop that had already converged. Fixed with a **third journal variant**,
  `LoopGateSettled{node,option}` (design §4/§5.2 step 0b/§5.7/AC12b): honour → journal →
  read back, the success mirror of the failure path, ordered settled → clock → decision so
  AC8 still holds. Also: `newly_journaled` REVERSED out (`fail_loop` is idempotent on the
  fold instead, and self-healing); `cascade_skip_from` fold-guarded; three missing tests
  landed (AC7 menu-from-journal, the fourth kind-swap sibling, AC8's ordering).

## Remaining

Tasks 8–14. Standing obligations: **AC8's test already exists** — Task 8 confirms it still
reddens against the s3-shaped hoist and adds AC10 + AC12b coverage, with the clock advancing
ACROSS iterations. **Task 12 has a new precondition**: `signal_states` does not fold
`LoopGateAwaited`, so a run paused on a human loop gate is invisible to `run list-paused`
and no verb can decide it — close that before anything authors a `GateSpec::Human`. Task 12
must also route the loop branch through `actor_or_user`. Task 14 owns
`docs/features/orchestrator/durable-journal.md` (README.md is already at SP-6-4).
**Doc-link baseline: 16.**

## Next command

`cargo test -p sensei-orchestrator --lib human_loop_gate` (11 passing), then Task 8.

## Known-broken

Nothing. The **sensei daemon is not running**, so this file is the only durable record.
