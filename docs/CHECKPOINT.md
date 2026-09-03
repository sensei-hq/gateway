# Checkpoint

**Slice: SP-6 s4 — the human loop gate.** Spec (`1633a96`), plan (`f7641c1`). **Tasks 1–13 of
14 DONE. Task 14 (whole-slice verification + review) is next.** Everything through SP-6 is on
`main` (PR #49, `78c5138`); `develop` is ahead by this slice. `$DATABASE_URL` is REMOTE Supabase
— never run a suite against it; Task 13's recipe (a throwaway `postgres:16` on 55432 +
`database/_apply_all.sql`) is the only way. The sensei daemon is down, so this is the record.

## Done

- **Tasks 1–11** (`6a20fea`…`374542a`) — types, `validate_dag`'s non-converging-menu refusal at
  every depth, the two journal variants, the fold, the `human_question_for` seam, the arm at
  `{loop}/{i}/__gate__`, `run_loop`'s third arm, expiry-before-decision (AC8/AC12b), the three
  loud refusals, bounds + redaction, AC11 zero spend, AC12 replay. `LoopGateSettled` closed the
  Critical; review found 12 more (an arm no test reached, two plaintext leaks).
- **Task 12 — the torii operator surface (AC17/AC18)** (`8f09e25`, `66f7916`, `2a1d079`).
  `gate_menu` returns `PublishedMenu::{Human,Loop}`; `decide` branches only at the append;
  `signal`/`agent answer` refuse a loop gate (four KINDS over three verbs); `list-paused` renders
  the fourth `AwaitingNode` shape (`options` AND `question`) and drops a SETTLED gate. Its review
  found 19 more — four behaviour fixes + 10 guards.
- **Task 13 — the cross-process Postgres e2e (AC19)** (`788930a`). `n1 → lp → n2` on a real
  `postgres:16`: A pays for `n1` + iteration 0 and pauses; a second pool's `list-paused` finds the
  SYNTHESIZED gate node, menu and question with no graph in hand; `gate decide` on a third; a
  fresh worker converges through `worker serve --once`, zero re-spend. Red step = 4 MUTATIONS.
- **`2a1d079` VERIFIED by mutation** (`890ee18`) — 10 reverts in a throwaway worktree. All four
  behaviour fixes and the redaction/bounds/help guards redden. TWO could not fail and are fixed:
  `question_cell`'s empty-label `<=` (the over-count revert renders 293/300 and stayed green;
  the comment had the direction backwards) and the help guard's bare-word `note` key (clap
  prints `--note` regardless). Both plan AS BUILT claims corrected.
- **Two carried findings closed** — the re-pause's PUBLISHED menu asserted against a graph
  renamed between drives (`b85279a`, red-first); the "last line of defence" / "one place" claims
  now name the JOURNAL (`4cfc709`) — the intake is `run submit --graph`, not `config push`, and
  `scheduled_runs.graph` / `config_agents` hold the plaintext first.

## Remaining — Task 14 (owns `durable-journal.md`; doc-link baseline 16)

**Next:** Task 14 step 1. DB-free `cargo test --workspace` = **1646 passed, 0 failed, 56
ignored**; clippy `-D warnings` and `fmt --check` exit 0. Postgres suites not re-run since
`788930a`, which nothing after it touches: `e2e_pg` **8/0/0**, store **69/0/0**, `orchestrator
--features postgres-tests postgres_e2e` **5/0/0**. **Broken:** none.
