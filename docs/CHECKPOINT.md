# Checkpoint

**Slice: SP-6 s4 — the human loop gate.** Spec (`1633a96`), plan (`f7641c1`).
**Tasks 1–11 of 14 DONE**, every review round closed. **Task 12 is next.**
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
- **Task 10** (`7969c1e`) — bounds + redaction. Authored half fails loud, `## Context` half
  truncates, journaled question redacted. All three already held, so each went red-first by
  MUTATION (disable the byte check / pass the iteration output as the seam's `input` / swap
  the redactor for the identity). New fixture `exec_with_body_output` — no CAS, or a 37 KiB
  output becomes a `ContentRef` and the truncation never happens.
- **Task 11** (`33f7823`) — AC11 zero spend (effect list BY NODE `["lp/0"]`); AC12 landed as
  `a_decided_loop_gate_replays_from_its_settlement_without_re_asking` (3 drives, third at
  +45m INSIDE a 1h SLA) because the sketch duplicated `a_stopping_decision_converges_the_loop`.

## Remaining

Tasks 12–14. **Task 12 preconditions**: `signal_states` does not fold `LoopGateAwaited`;
route its loop branch through `actor_or_user`; refuse a decision at/after the journaled
deadline; **and REDACT `actor` before appending** — Task 10 proved the executor never touches
that field, so AC16's second half is torii's alone and `cmd::gate::decide` does not redact
today (`cmd::human::answer` does). Task 14 owns `durable-journal.md`; doc-link baseline 16.

**Next:** `cargo test --workspace` (1618 passing, 0 failed), then Task 12.
**Known-broken:** nothing.
