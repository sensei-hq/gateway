# Checkpoint

**Slice: SP-6 s4 — the human loop gate.** Spec (`1633a96`), plan (`f7641c1`).
**Tasks 1–11 of 14 DONE**, every review round closed including the 12 findings on Tasks
10–11. **Task 12 is next.**
Everything through SP-6 is on `main` (PR #49, `78c5138`); `develop` is ahead by this slice.
`$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it. The sensei daemon is
down, so this file is the only durable record.

## Done

- **Tasks 1–7** (`6a20fea`, `eba6083`, `e177e3c`) — the types, `validate_dag`'s
  non-converging-menu refusal at every depth, the two journal variants, the fold, the
  `human_question_for` seam, the arm at `{loop}/{i}/__gate__`, `run_loop`'s third gate arm.
  `LoopGateSettled` closed the Critical (settled → clock → decision, so AC8 still holds).
- **Tasks 8–9** (`b4a1d44`, `be42bad`, `c7142c2`) — the AC8/AC12b boundary in one run; the
  three loud refusals; the review fixes (step 1's settled half vs a `config push`).
- **Task 10** (`7969c1e`) — bounds + redaction, red-first by MUTATION since all three held.
- **Task 11** (`33f7823`) — AC11 zero spend; AC12 as
  `a_decided_loop_gate_replays_from_its_settlement_without_re_asking` (3 drives, third at
  +45m INSIDE a 1h SLA).
- **Tasks 10–11 review round** (`e7ff7f3`, `6db5ab1`, `18a71a0`, `7ac6da0`) — 12 findings.
  The re-pause arm (`Waiting` + menu + NO decision, every wake before an answer) was reached
  by NO test: two new tests, four mutations. The MENU and the pause reason leaked plaintext
  while `prompt` was scrubbed on the same write: redacted at the append, and a menu whose
  names COLLIDE once redacted now fails loudly rather than inverting a decision. Four false
  doc claims corrected against measurement (the CAS is NOT load-bearing — it is wired now
  and asserted; the by-node effect list; two mutation blast radii; the phantom
  `newly_journaled`).

## Remaining

Tasks 12–14. **Task 12 preconditions**: `signal_states` does not fold `LoopGateAwaited`;
route its loop branch through `actor_or_user`; refuse a decision at/after the journaled
deadline; **and REDACT `actor` before appending** — the executor never touches that field, so
AC16's actor half is torii's alone and `cmd::gate::decide` does not redact today
(`cmd::human::answer` does). Task 12 must also recite the PUBLISHED (scrubbed) option names.
Task 14 owns `durable-journal.md`; doc-link baseline 16.

**Next:** `cargo test --workspace` (1622 passing, 0 failed, 55 ignored), then Task 12.
**Known-broken:** nothing.
