# Checkpoint

**Slice: SP-OPS-1 consolidation**, on `develop` ahead of `main`. Plan
`docs/superpowers/plans/2026-09-16-sp-ops-1-consolidation.md`; grounding
`docs/analysis/2026-09-16-agentic-execution-capability-survey.md`.

## Done — 2 of 3 bugs fixed, both mutation-verified

**Survey** (`555d383`): durable core strong, composition root empty (12 seams wired to nothing).
**SP-OPS-1.2 — `Expand.planner` REQUIRED in JSON** (`c3f200a`). `#[serde(default)]` resolved a
missing field to `PlannerRef::Injected`, the *test* variant, so hand-written `Expand` graphs parsed
then died mid-run. `Select` is no better a default (no planning content ships). Replaced the test
that pinned the old behaviour. Durable graphs safe — no `skip_serializing_if`, pinned by a test.
**SP-OPS-1.1 — run-scoped the blackboard** (`d28992e`, comment fix `3482d53`). `run` is now a param
on `ContextStore::{put,get,insert_ref}` + a `run_id` column, PK
`(run_id, scope_kind, scope_id, ctx_key)`. Deliberately NOT `Scope::Run(RunId)` — `Scope` is
serialized inside `ContextWrite`, so that would bump `FORMAT_VERSION`; every journal still loads.
Within-run collisions stay loud (asserted beside every cross-run test). Schema via
`dbd reconcile --allow-destructive`; the 249 dev rows were residue with no recoverable run id
(backup `/tmp/sp-ops-1-backup/`).

## Next — SP-OPS-1.3, then two design calls

**1.3 bounded transient retry** (§2.2). A transient 500 permanently kills a run: the executor
documents retry-on-resume (`mod.rs:303`) but `Scheduler::record` terminalizes `failed` and
`claim_due` never re-selects it. Fix is in `classify_gateway_error` — NOT `record`, which sees only
a bare `(NodeId, String)`. **Needs an attempt bound** or it becomes the poison-run loop (`RunPaused`
has no fold guard, so each wake grows the journal). Open: N, backoff curve, per-node vs per-run.

**1.4 lease** (§2.5) renew · shrink `CLAIM_BATCH` · per-run lock. **1.5 reconciler** (§2.6) ship
one for `fs_write` · refuse Mutation tools without one · document. Both need a call.

## Verified

`DATABASE_URL="postgres://Jerry@localhost:5432/postgres"` (schema is in the `postgres` db).
**1836 / 0** workspace · **71 / 0** store `--features postgres` · **8 / 0** `-p sensei-torii --test
e2e_pg` · clippy **0.1.98** 0 · fmt 0. Homebrew shadows rustup — prepend
`~/.rustup/toolchains/stable-aarch64-apple-darwin/bin` or `clippy-driver` resolves to 1.97.
**Mutations must compile**: `uuid` is a dev-dep of orchestrator-store, so `uuid::Uuid::nil()` in
lib code fails to build and fakes a red. Use `Default::default()`.
