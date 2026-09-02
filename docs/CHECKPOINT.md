# Checkpoint

**Slice: SP-6 s4 — the human loop gate.** Spec (`1633a96`), plan (`f7641c1`).
**Tasks 1–9 of 14 DONE**, every review round closed. **Task 10 is next.**

Everything through SP-6 is on `main` (PR #49, `78c5138`); `develop` is ahead by this slice.
`$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it. The sensei daemon is
not running, so this file is the only durable record.

## Done

- **Tasks 1–5** — `GateSpec::Human` + `LoopGateOption`; `validate_dag` rejects a menu that
  cannot converge, at every depth; the `LoopGateAwaited`/`LoopGateDecided` variants; the
  fold (ask FIRST-wins into the SHARED `deadlines` map, decision LAST-wins); the
  `human_question_for` seam (`9286702`, `7c671ea`).
- **Tasks 6+7** (`6a20fea`, `eba6083`, `e177e3c`) — the arm at `{loop}/{i}/__gate__` and
  `run_loop`'s third gate arm; `LoopGateSettled` closed the Critical (settled → clock →
  decision, so AC8 still holds).
- **Task 8** (`b4a1d44`) — AC8's hoist mutation re-proven (reddens the ordering test ALONE),
  plus the boundary AC8/AC12b could not see: a settled gate replays while a LATER
  iteration's gate still expires, discriminated by which gate the failure names.
- **Task 9** (`be42bad`) — the loud refusals: unmatched option on the LIVE arm, a
  model-backed role in `GateSpec::Human` (at the ask AND on a drive that does not ask — step
  2's SLA read was load-bearing and untested), and `GateSpec::Agent` still refusing while
  naming `GateSpec::Human`. `non_top_level_sites` unchanged; it now drives the AC13 test.

## Remaining

Tasks 10–14. **Task 12 precondition**: `signal_states` does not fold `LoopGateAwaited`, so a
run paused on a human loop gate is invisible to `run list-paused` and no verb can decide it —
close that before anything authors a `GateSpec::Human`; also route its loop branch through
`actor_or_user`. Task 14 owns `durable-journal.md`; doc-link baseline 16.

## Next command

`cargo test -p sensei-orchestrator --lib human_loop_gate` (19 passing), then Task 10.

## Known-broken

Nothing. Open question, logged in the plan, NOT a blocker: `validate_dag` does not reserve
the `__gate__` segment, so an authored one in a `Loop` body collides with the gate path.
