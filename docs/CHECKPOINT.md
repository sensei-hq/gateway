# Checkpoint

**Slice: SP-6 s4 — the human loop gate.** Spec (`1633a96`), plan (`f7641c1`).
**Tasks 1–7 of 14 DONE**, every review round closed. **Task 8 is next.**

Everything through SP-6 is on `main` (PR #49, `78c5138`); `develop` is ahead by this slice.
`$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it. The sensei daemon is
not running, so this file is the only durable record.

## Done

- **Tasks 1–4** — `GateSpec::Human` + `LoopGateOption`; `validate_dag` rejects a menu that
  cannot converge, at every depth; the `LoopGateAwaited`/`LoopGateDecided` variants; the
  fold (ask FIRST-wins into the SHARED `deadlines` map, decision LAST-wins).
- **Task 5** (`9286702`, `7c671ea`) — the `human_question_for` seam.
- **Tasks 6+7** (`6a20fea`, `eba6083`) — `run_human_loop_gate` at `{loop}/{i}/__gate__` and
  `run_loop`'s third gate arm. `eba6083` closed the review's Critical (`run_loop` re-derives
  every gate on every drive, so a stale deadline killed an already-honoured decision) with
  `LoopGateSettled{node,option}`: settled → clock → decision, so AC8 still holds.
- **This commit** — `eba6083`'s four verify findings: a mis-attached rustdoc, a false
  reachability clause, three red-first tests, this file's length.

## Remaining

Tasks 8–14. **Task 8**: AC8's test already exists — confirm it still reddens against the
s3-shaped hoist, then add AC10 + AC12b coverage with the clock advancing ACROSS iterations.
**Task 12 precondition**: `signal_states` does not fold `LoopGateAwaited`, so a run paused on
a human loop gate is invisible to `run list-paused` and no verb can decide it — close that
before anything authors a `GateSpec::Human`; also route its loop branch through
`actor_or_user`. Task 14 owns `durable-journal.md`; doc-link baseline 16.

## Next command

`cargo test -p sensei-orchestrator --lib human_loop_gate` (14 passing), then Task 8.

## Known-broken

Nothing. Open question, logged in the plan, NOT a blocker: `validate_dag` does not reserve
the `__gate__` segment, so an authored one in a `Loop` body collides with the gate path.
