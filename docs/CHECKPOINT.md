# Checkpoint

**Slice: SP-6 s4 — the human loop gate.** Spec (`1633a96`), plan (`f7641c1`).
**Tasks 1–13 of 14 DONE.** **Task 14 (whole-slice verification + review) is next.**
Everything through SP-6 is on `main` (PR #49, `78c5138`); `develop` is ahead by this slice.
`$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it; Task 13's recipe (a
throwaway `postgres:16` on 55432 + `database/_apply_all.sql`) is the only way. The sensei
daemon is down, so this file is the only durable record.

## Done

- **Tasks 1–7** (`6a20fea`, `eba6083`, `e177e3c`) — types, `validate_dag`'s non-converging-menu
  refusal at every depth, the two journal variants, the fold, the `human_question_for` seam, the
  arm at `{loop}/{i}/__gate__`, `run_loop`'s third arm. `LoopGateSettled` closed the Critical.
- **Tasks 8–11 + review** (`b4a1d44`…`374542a`) — the AC8/AC12b expiry-before-decision line;
  the three loud refusals; bounds + redaction (red-first by MUTATION); AC11 zero spend; AC12
  replay at +45m inside a 1h SLA. Then 12 findings: an arm reached by NO test, the MENU and the
  pause reason leaking plaintext, four false doc claims.
- **Task 12 — the torii operator surface (AC17/AC18)** (`8f09e25`, `66f7916`, `2a1d079`).
  `gate_menu` returns `PublishedMenu::{Human,Loop}`; `decide` is factored over the option NAMES
  and branches only at the append; `signal`/`agent answer` refuse a loop gate (four KINDS over
  three verbs); `list-paused` renders the fourth `AwaitingNode` shape (`options` AND `question`)
  and drops a SETTLED gate. Its review found 19 more — four behaviour fixes (`answer` ANSWERED
  a menu-bearing node; `awaiting_nodes` disagreed with `gate_menu` and now CALLS it; the orphan
  message claimed a loop gate's decision would be re-folded; stale 3-kind help) + 10 guards.
- **Task 13 — the cross-process Postgres e2e (AC19)** (`788930a`). `n1 → lp → n2` on a real
  `postgres:16`: A pays for `n1` + iteration 0 and pauses; a second pool's `list-paused`
  discovers the SYNTHESIZED gate node, menu and question with no graph in hand; a bare `wake` is
  shown NOT to be a decision; `gate decide --option ship` on a third; a fresh worker converges
  through the real `worker serve --once`, `converged: true, iterations: 1` (not the `max_iters`
  cap), zero re-spend per node, `LoopGateSettled` durable. Passed first time, so the red step is
  4 reverted MUTATIONS; a 5th (the step-1 replay) stayed green — unreachable from this fixture.

## Remaining — Task 14 (owns `durable-journal.md`; doc-link baseline 16)

**Next:** Task 14 step 1. DB-free `cargo test --workspace` = **1646 passed, 0 failed, 56
ignored**; clippy `-D warnings` and `fmt --check` exit 0. Against the throwaway Postgres:
`e2e_pg` **8/0/0**, store `--all-features` **69/0/0**, `orchestrator --features postgres-tests
postgres_e2e` **5/0/0**. **Known-broken:** nothing.
