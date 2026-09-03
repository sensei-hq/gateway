# Checkpoint

**Slice: SP-6 s4 — the human loop gate.** Spec (`1633a96`), plan (`f7641c1`).
**Tasks 1–12 of 14 DONE.** **Task 13 (cross-process Postgres e2e, AC19) is next.**
Everything through SP-6 is on `main` (PR #49, `78c5138`); `develop` is ahead by this slice.
`$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it; Task 13 carries its own
throwaway-container recipe. The sensei daemon is down, so this file is the only durable record.

## Done

- **Tasks 1–7** (`6a20fea`, `eba6083`, `e177e3c`) — the types, `validate_dag`'s
  non-converging-menu refusal at every depth, the two journal variants, the fold, the
  `human_question_for` seam, the arm at `{loop}/{i}/__gate__`, `run_loop`'s third gate arm.
  `LoopGateSettled` closed the Critical (settled → clock → decision, so AC8 still holds).
- **Tasks 8–9** (`b4a1d44`, `be42bad`, `c7142c2`) — the AC8/AC12b boundary in one run; the
  three loud refusals; the review fixes (step 1's settled half vs a `config push`).
- **Tasks 10–11** (`7969c1e`, `33f7823`) — bounds + redaction, red-first by MUTATION since all
  three held; AC11 zero spend; AC12 replay from the settlement at +45m inside a 1h SLA.
- **Tasks 10–11 review round** (`e7ff7f3`, `6db5ab1`, `18a71a0`, `7ac6da0`, `374542a`) — 12
  findings: the re-pause arm was reached by NO test; the MENU and the pause reason leaked
  plaintext while `prompt` was scrubbed on the same write; four false doc claims corrected.
- **Task 12 — the torii operator surface (AC17/AC18).** `gate_menu` returns
  `PublishedMenu::{Human,Loop}`; `decide` is factored over the option NAMES and branches only
  at the append, because the two events are not interchangeable. `run signal` and `run agent
  answer` refuse a loop gate — the matrix is four KINDS over three verbs. `list-paused` renders
  it as the fourth `AwaitingNode` shape (`options` AND `question`) and drops a SETTLED gate.
  Four things beyond the plan's sketch, all recorded in its Step 3 "AS BUILT": `LoopGateSettled`
  folds as the node's terminal marker (else every decided iteration stays listed and
  re-decidable); `--note` refused, not dropped; `actor_or_user` applied inside `decide` so no
  blank audit row is reachable from the library entry point; the agent header keyed on the PAIR
  (else a loop gate advertises the one verb it refuses). 14 tests, all red first; the two
  correct-but-unguarded properties mutation-proved.

## Remaining

Tasks 13–14. Task 14 owns `durable-journal.md`; doc-link baseline 16.

**Next:** Task 13 in `crates/torii/tests/e2e_pg.rs`. `cargo test --workspace` = 1636 passing,
0 failed. **Known-broken:** nothing.
