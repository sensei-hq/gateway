# Checkpoint

**Slice: SP-6 s4 — the human loop gate. COMPLETE.** All 14 tasks, the whole-slice review's 24
findings, and the release gate are closed. Everything through SP-6 s3 is on `main` (PR #49,
`78c5138`); `develop` is ahead by this slice and is **unpushed — the coordinator pushes**.
The sensei daemon is NOT running, so this file is the only durable record of this gate.

## Shipped — `GateSpec::Human { agent, menu }`, the FOURTH waiting kind

A `Loop` whose stop decision a person makes, from a graph-level menu, once per iteration, at the
reserved `"{loop}/{i}/__gate__"` path. Three new journal variants (`LoopGateAwaited` FIRST-wins,
`LoopGateDecided` LAST-wins, `LoopGateSettled` FIRST-wins) ⇒ `FORMAT_VERSION` stays 1.
`validate_dag` rejects a menu that can never converge and now bans a bare
`__plan__`/`__gate__`/`__select__` node id at every depth. Operator surface: `torii run gate
decide --node "{loop}/{i}/__gate__" --option <name>`; `run signal`/`run agent answer` refuse it.

## Release gate — measured, real exit codes

- DB-free `cargo test --workspace`: **1658 passed / 0 failed / 56 ignored**, exit 0.
  `clippy --workspace --all-targets -D warnings` exit 0; `fmt --all --check` exit 0.
- Against a throwaway `postgres:16` on **55432** (schema from `database/_apply_all.sql`;
  container removed, port free): workspace **1707/0/7** (the 7 are live-provider tests, no DB
  test among them) · `orchestrator --features postgres-tests` **406/0/0** · `orchestrator-store
  --features postgres` **69/0/0** · `torii --test e2e_pg` **8/0/0**. **0 ignored in every DB
  suite.**
- `cargo doc --workspace --no-deps --document-private-items`: **16** unresolved links on a
  forced full re-document of all 11 crates — exactly the baseline, no new broken links.

## Docs swept

The overview s4 entry gained the menu-redaction leak, the §5.8 shared-path changes and the
missing "human at the other four refused positions" carry-forward; `agents-skills-tools.md`
gained s4's second use site for a human-backed role (and `run_agent` → `drive_agent`, a
function that does not exist); `execution-graph.md` scopes "top-level only" to the NODE KIND;
`durable-executor.md` no longer lists SP-6 or the SP-DATA-3 scheduler as unbuilt.

**Remaining: none. Known-broken: none.** Next: the coordinator pushes `develop`, then opens the
develop→main PR — `main`'s ruleset is strict, so merge `origin/main` into `develop` first or
the PR sits BEHIND and cannot land.
