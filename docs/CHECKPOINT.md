# Checkpoint

**Slice: SP-6 s4 — the human loop gate.** Spec (`1633a96`), plan (`f7641c1`).
**Tasks 1–9 of 14 DONE**, every review round closed. **Task 10 is next.**
Everything through SP-6 is on `main` (PR #49, `78c5138`); `develop` is ahead by this slice.
`$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it. The sensei daemon is
down, so this file is the only durable record.

## Done

- **Tasks 1–7** (`6a20fea`, `eba6083`, `e177e3c`) — the types, `validate_dag`'s
  non-converging-menu refusal at every depth, the two journal variants, the fold, the
  `human_question_for` seam, the arm at `{loop}/{i}/__gate__`, `run_loop`'s third gate arm.
  `LoopGateSettled` closed the Critical (settled → clock → decision, so AC8 still holds).
- **Task 8** (`b4a1d44`) — the boundary AC8/AC12b could not see: a settled gate replays while
  a LATER iteration's gate still expires, told apart by which gate the failure names.
- **Task 9** (`be42bad`) — the loud refusals: unmatched option on the LIVE arm; a
  model-backed role at the ask AND on a drive that does not ask (step 2's SLA read was
  load-bearing and untested); `GateSpec::Agent` refusing while naming `GateSpec::Human`.
- **Tasks 8–9 review fixes** — step 1's SETTLED half (a `config push` editing or deleting the
  gate role must not retroactively kill a converged loop) was prose in three places and
  guarded by nothing; red-first tested now, as is "no `LoopGateSettled` for an unmatched
  option". `non_top_level_sites`' "two tests in two modules" was false (one); the table test
  now pins the site list by value, making it true. Three false doc claims corrected: AC8's
  hoist at HEAD is **18 passed / 2 failed** (13/1 was `e177e3c`'s 14-test module); expiry
  fails an EXPIRED gate, decided or not ("undecided" ×4); §7 now admits an ANSWERED gate dies
  too if no drive precedes the deadline — Task 12's `decide` must refuse that at the CLI.

## Remaining

Tasks 10–14. **Task 12 preconditions**: `signal_states` does not fold `LoopGateAwaited` (run
invisible to `run list-paused`, no verb can decide it); route its loop branch through
`actor_or_user`; refuse a decision at/after the journaled deadline. Task 14 owns
`durable-journal.md`; doc-link baseline 16.

**Next:** `cargo test -p sensei-orchestrator --lib human_loop_gate` (20 passing), then
Task 10. **Known-broken:** nothing; the plan's open question is not a blocker
(`validate_dag` does not reserve the `__gate__` segment, so an authored one in a `Loop` body
collides with the gate path).
