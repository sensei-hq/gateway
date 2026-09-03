# Checkpoint

**Slice: SP-6 s4 — the human loop gate.** Spec (`1633a96`), plan (`f7641c1`).
**Tasks 1–12 of 14 DONE.** **Task 13 (cross-process Postgres e2e, AC19) is next.**
Everything through SP-6 is on `main` (PR #49, `78c5138`); `develop` is ahead by this slice.
`$DATABASE_URL` is REMOTE Supabase — never run the DB suite against it; Task 13 has its own
throwaway-container recipe. The sensei daemon is down, so this file is the only durable record.

## Done

- **Tasks 1–7** (`6a20fea`, `eba6083`, `e177e3c`) — types, `validate_dag`'s non-converging-menu
  refusal at every depth, the two journal variants, the fold, the `human_question_for` seam, the
  arm at `{loop}/{i}/__gate__`, `run_loop`'s third gate arm. `LoopGateSettled` closed the Critical.
- **Tasks 8–9** (`b4a1d44`, `be42bad`, `c7142c2`) — the AC8/AC12b boundary in one run; the three
  loud refusals; the step-1-vs-`config push` review fix.
- **Tasks 10–11 + review** (`7969c1e`, `33f7823`, `e7ff7f3`…`374542a`) — bounds + redaction
  (red-first by MUTATION), AC11 zero spend, AC12 replay at +45m inside a 1h SLA; then 12 findings
  (an arm reached by NO test; the MENU and pause reason leaking plaintext; four false doc claims).
- **Task 12 — the torii operator surface (AC17/AC18)** (`8f09e25`, `66f7916`). `gate_menu` returns
  `PublishedMenu::{Human,Loop}`; `decide` is factored over the option NAMES and branches only at
  the append. `signal`/`agent answer` refuse a loop gate — four KINDS over three verbs.
  `list-paused` renders the fourth `AwaitingNode` shape (`options` AND `question`) and drops a
  SETTLED gate. Beyond the sketch: `LoopGateSettled` as terminal marker, `--note` refused not
  dropped, `actor_or_user` inside `decide`, the agent header keyed on the PAIR.
- **Task 12 review round** — 19 findings. Four behaviour fixes: `cmd::human::answer` read the
  QUESTION first and so ANSWERED a menu-bearing node (exit 0) — it now matches `gate_menu` first
  like every other surface; `awaiting_nodes` re-derived the kind and disagreed with `gate_menu` on
  a `GateAwaited` + `LoopGateAwaited` journal, so it now CALLS it (ONE resolver); the post-append
  orphan message claimed a re-`start` would fold a loop gate's decision, which is false (its
  reader replays the settlement) — split per kind; the `gate`/`agent` help was never swept for the
  fourth kind. Plus ten mutation-proved guards for correct-but-unguarded properties: the second
  `redact_question` call site, the new cell's `one_line`/`cap_chars`, the `## Task` reserve's
  empty label, the header widening both ways, the `--json` fourth shape, the per-kind refusal
  wording, both `Measured` discriminants.

## Remaining — Tasks 13–14 (14 owns `durable-journal.md`; doc-link baseline 16)

**Next:** Task 13 in `crates/torii/tests/e2e_pg.rs`. `cargo test --workspace` = 1646 passing,
0 failed. **Known-broken:** nothing.
