# Checkpoint

**`main` = `e6658d6`. SP-REG-0/3/5 + torii docs merged (#59–#66). Issue #56 CLOSED.**

## Done

**The capability survey — RUN.** `docs/analysis/2026-09-16-agentic-execution-capability-survey.md`.
Six blind lenses. Every §2 finding re-verified by hand; §3 is reported-not-re-derived. The embedder
lens compiled and ran a real external consumer, so its findings are empirical.

**Verdict: the durable core is strong; the composition root is empty. 12 seams are built, tested
and wired to nothing** (§2.1 enumerates them; `with_concurrency` is never called even in tests).
`boot.rs` is the highest-leverage file in the repo.

## Open — 3 verified BUGS, by reachability

1. **A transient provider 500 permanently kills a run.** Executor documents retry-on-resume
   (`mod.rs:303`); `Scheduler::record` terminalizes `failed`, `claim_due` never re-selects it.
   Fix in `record`, not the classifier.
2. **The blackboard is not run-scoped.** `Scope::Run => ("run", String::new())`
   (`postgres.rs:323`); the DDL comment says "run id". **Re-running the same graph aborts the
   second mid-drive, after paying.** Known + dodged at `e2e_pg.rs:141`.
3. **`Expand` via JSON is dead by default** (SP-REG-0's sibling). `PlannerRef::Injected` is
   `#[default]` + `#[serde(default)]` but is the *test* variant; `with_planner` has 0 production
   callers. **Fix the default, not the wiring.**

Plus: concurrent double-drive (60s lease never renewed; 64 runs stamped at one instant, driven
serially; 2 lenses found it independently, the header calls it safe) · no reconciler ships while 2
Mutation tools do ⇒ crash-mid-mutation parks forever · 3 LOW open on main.

## Next

Consolidation slice before any new feature: bugs 1–3 are small, independent, each with an obvious
red test; 4–5 need a design call. With an empty composition root, new features become dead seams.

## Verified

`cargo test --workspace --locked` **1831 / 0** real exit 0 · clippy **0.1.98** `-D warnings` 0 ·
fmt 0 · rustdoc 0 · `cargo audit` 0. Homebrew `rustc` 1.97 SHADOWS rustup's 1.98. **Ripgrep counts
here: wrong 4/4 — read matches, never count.**
