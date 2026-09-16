# Checkpoint

**Slice: SP-OPS-1 consolidation**, on `develop` ahead of `main`. Plan
`docs/superpowers/plans/2026-09-16-sp-ops-1-consolidation.md`; grounding
`docs/analysis/2026-09-16-agentic-execution-capability-survey.md`.

## Done

**The capability survey** (`555d383`) — six blind lenses: the durable core is strong, the
composition root is empty (12 seams wired to nothing), plus 3 live bugs.
**SP-OPS-1.2 — `Expand.planner` is now REQUIRED in JSON** (`c3f200a`). `#[serde(default)]` resolved
a missing field to `PlannerRef::Injected`, the *test* variant, so every hand-written `Expand` graph
parsed fine then died mid-run with "no planner wired". `Select` is no better a default — no
planning-area content ships. Replaced the test pinning the old behaviour (it encoded the bug).
Durable graphs unaffected: no `skip_serializing_if`, pinned by a round-trip test.
**1833 / 0** with live Postgres; both green-on-arrival guards mutation-verified (`#[serde(skip)]`
→ 3 `panicked at`, 0 compile errors).

## Next — SP-OPS-1.1, then 1.3

**1.1 run-scope the blackboard** (§2.3, the severe one: re-running the same graph aborts the second
mid-drive, after paying). Shape chosen to avoid a journal-format bump — do NOT make
`Scope::Run(RunId)` (`Scope` is serialized inside `JournalEvent::ContextWrite`). Instead add a
`run: RunId` **param** to `ContextStore::{put,get,insert_ref}` + a `run_id` **column**, PK
`(run_id, scope_kind, scope_id, ctx_key)`. Red test goes against `InMemoryContextStore` first —
same defect (`stores.rs:55`), no DB needed. dbd is **pre-release** (no migrations dir) ⇒ edit DDL +
`dbd reconcile`, never hand-write a migration.

**1.3 bounded transient retry** (§2.2). Fix is in `classify_gateway_error`, NOT `Scheduler::record`
— `RunOutcome.failed` is a bare `(NodeId, String)`, so `record` has no signal. Needs an attempt
bound or it becomes the poison-run loop.

**1.4 lease / 1.5 reconciler** — design calls, not started blind.

## Verified

`DATABASE_URL="postgres://Jerry@localhost:5432/postgres"` (the `orchestrator` schema is in the
`postgres` db, not `sensei`). `cargo test --workspace --locked` **1833 / 0** real exit 0 · clippy
**0.1.98** `-D warnings` 0 · fmt 0. Homebrew `rustc` 1.97 SHADOWS rustup's 1.98. **Ripgrep counts
here: wrong 4/4.** sensei daemon DOWN ⇒ this file is the record.
