# Checkpoint

**SP-OPS-1 consolidation COMPLETE** — all five increments, every fix mutation-verified. On
`develop`, ahead of `main`. Plan + grounding:
`docs/superpowers/plans/2026-09-16-sp-ops-1-consolidation.md` ·
`docs/analysis/2026-09-16-agentic-execution-capability-survey.md`.

## Done

**Survey** (`555d383`) — six blind lenses: durable core strong, composition root empty (12 seams
wired to nothing), 3 live bugs.
**1.2 `Expand.planner` REQUIRED** (`c3f200a`) — the serde default resolved to the *test* variant.
**1.1 run-scoped blackboard** (`d28992e`, `3482d53`) — `run` param + `run_id` column. NOT
`Scope::Run(RunId)`: `Scope` is serialized in `ContextWrite`, so that would bump `FORMAT_VERSION`.
**1.3 bounded transient retry** (`741a93e`, `2f3fc8c`) — **OPT-IN, default off.**
**1.4 per-run drive lock** (`4d4d02b`, `a70d55c`) — `pg_try_advisory_lock` on a **detached**
connection; the lock excludes a LIVE driver, the lease still recovers a DEAD one.
**1.5 `fs_write` reconciler** (`1383def`, `a941c20`) — always `NotApplied`: `std::fs::write`
truncates, so a re-run is idempotent.

## Carry-forwards — deliberate, not forgotten

1. **1.3's default leaves §2.2's bug live.** `retryable` is `any(HardFailure)` and an
   unclassified provider error IS one, while auth/credits exhaust as `AllGated` → HOTL pause and
   never reach that path — so enabling it retries *essentially every* provider failure. Flipping
   it is one line plus re-expecting 35 tests that inject a 500 to mean "this node fails".
2. **`shell` still has no reconciler** — not idempotent, nothing generic can decide whether it
   ran, so a crash mid-`shell` still parks for a human.
3. Untouched: no metrics/spans/correlation id · no network API · streaming stops at the orchestrator · `latest_snapshot` written never read · no tenant dimension · 3 LOW.

**Next:** survey §4 — wire `with_hooks` + the 5 discovery tools (cheap, high value), or
start Bar-B (network API / streaming / metrics / tenancy).

## Verified

`DATABASE_URL="postgres://Jerry@localhost:5432/postgres"`. **1845 / 0** workspace · **73 / 0**
store `--features postgres` · **8 / 0** e2e_pg · clippy **0.1.98** 0 · fmt 0 · rustdoc 0. Prepend
`~/.rustup/toolchains/stable-aarch64-apple-darwin/bin` else `clippy-driver` resolves to 1.97.
**Commit before mutating** (`git checkout` reverted uncommitted work 3× today) and **check the
mutation applied** — a failed `str.replace` assert leaves a false green.
