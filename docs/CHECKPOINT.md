# Checkpoint

**Slice: SP-OPS-1 consolidation**, on `develop` ahead of `main`. Plan + grounding:
`docs/superpowers/plans/2026-09-16-sp-ops-1-consolidation.md` ·
`docs/analysis/2026-09-16-agentic-execution-capability-survey.md`.

## Done — all 3 bugs, every fix mutation-verified

**Survey** (`555d383`): durable core strong, composition root empty (12 seams wired to nothing).
**1.2 `Expand.planner` REQUIRED** (`c3f200a`) — the serde default resolved to the *test* variant
`Injected`, so hand-written graphs parsed then died mid-run.
**1.1 run-scoped blackboard** (`d28992e`, `3482d53`) — `run` param on `ContextStore` + `run_id`
column, PK `(run_id, scope_kind, scope_id, ctx_key)`. NOT `Scope::Run(RunId)`: `Scope` is
serialized in `ContextWrite`, so that would bump `FORMAT_VERSION`. Within-run collisions stay loud.
**1.3 bounded transient retry** (`741a93e` kernel/gateway, `2f3fc8c` orchestrator).
## 1.3 — read before changing the default

**It is OPT-IN, default 1 = off, byte-identical — so §2.2's bug still bites by default.** The
suite showed why: 35 tests inject a 500 to mean "this node fails" and all began retrying.
`retryable` is `any(HardFailure)`, an *unclassified* provider error IS a non-limit fault, and the
genuinely terminal cases (auth/credits) exhaust as `AllGated` → HOTL pause and never reach this
path — so enabling it retries **essentially every provider failure**. Flipping the default is one
line plus re-expecting those 35 tests: a deliberate call. Two traps, both pinned: a `Pause`
appends no `NodeFailed` (count never advances ⇒ retries forever, hence a distinct `Retry`
disposition appending both), and `failed`/`failure_messages` collapse an identical repeated
failure — `Fold::attempts` counts ROWS.

## Next — 1.4 then 1.5 (decided, not started)

**1.4** per-run `pg_try_advisory_lock` held for the drive — makes concurrent double-drive
structurally impossible. **1.5** ship an `fs_write` reconciler (the file is the evidence);
`shell` stays unreconcilable and must park loudly.

## Verified

`DATABASE_URL="postgres://Jerry@localhost:5432/postgres"`. **1841 / 0** workspace · **71 / 0** store
`--features postgres` · **8 / 0** e2e_pg · clippy **0.1.98** 0 · fmt 0. Prepend
`~/.rustup/toolchains/stable-aarch64-apple-darwin/bin` or `clippy-driver` resolves to 1.97.
**Commit before mutating** — `git checkout` reverts uncommitted fixes too (hit again today).
**Mutations must compile**: `uuid` is a dev-dep of orchestrator-store; use `Default::default()`.
