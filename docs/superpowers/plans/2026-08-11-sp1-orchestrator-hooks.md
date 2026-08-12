# SP-1 OrchestratorHooks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A best-effort `OrchestratorHooks` observability seam — no-op-default callbacks at run/node/agent/context lifecycle, never affecting execution or determinism, no double-count on resume.

**Architecture:** Run/node/context hooks fire from inside `Executor::append` (matched on the just-journaled `JournalEvent`) — can't-miss + replay-suppressed for free (a resumed completed prefix isn't re-appended). Agent hooks fire explicitly on the live path in `drive_agent`/`agent_turn_output`/`record_tool_effect`. Opt-in: `hooks: Option<Arc<dyn OrchestratorHooks>>`, default `None` ⇒ zero firing and byte-identical (the event is cloned for the match ONLY when hooks are wired).

**Tech Stack:** Rust; `sensei-orchestrator-core` (trait), `sensei-orchestrator` (executor). `async-trait`, `tokio::test`.

**Design:** `docs/superpowers/specs/2026-08-11-sp1-orchestrator-hooks-design.md`.

**Conventions (non-negotiable):** TDD (failing test → watch fail → minimal code). `cargo fmt --all` before every commit (pre-commit = fmt-check + workspace `clippy -D warnings`, NO tests — always run `cargo test --workspace` yourself before committing a behavior change). Verify REAL exit codes, never a piped `| tail`. **After any `git checkout <file>` (e.g. a mutation-verify), re-run the full suite before committing — `git checkout` reverts uncommitted fixes too.**

---

## File structure

- `crates/orchestrator-core/src/hooks.rs` — the `OrchestratorHooks` trait (new).
- `crates/orchestrator-core/src/lib.rs` — module + export.
- `crates/orchestrator/src/executor/mod.rs` — `Executor.hooks` + `with_hooks`; `append` fires run/node/context hooks.
- `crates/orchestrator/src/executor/agent.rs` — `drive_agent`/`agent_turn_output`/`record_tool_effect` fire agent hooks on the live path.
- `crates/orchestrator/src/executor/tests.rs` — `RecordingHooks` spy + acceptance tests.
- `docs/features/orchestrator/hooks.md`, `README.md` — status.

---

## Task 1: `OrchestratorHooks` trait (core)

**Files:**
- Create: `crates/orchestrator-core/src/hooks.rs`
- Modify: `crates/orchestrator-core/src/lib.rs`

- [ ] **Step 1: Write the failing test** — create `hooks.rs` with the trait + a `#[cfg(test)]` proving a no-op default impl and a recording impl both work:

```rust
//! Best-effort observability hooks (§15). No-op defaults; a wired impl observes
//! run/node/agent/context lifecycle. Hooks never affect execution/determinism.

use crate::context::{ContextKey, Scope};
use crate::ids::{NodeId, RunId};

/// Observation callbacks fired by the executor at lifecycle points. Every method
/// defaults to a no-op, so an impl overrides only what it cares about. Best-effort
/// (§11.1.3): a hook must not depend on execution and should be fast (it is awaited
/// inline). Anything execution depends on is a durable effect, not a hook.
#[async_trait::async_trait]
pub trait OrchestratorHooks: Send + Sync {
    async fn on_run_started(&self, _run: RunId) {}
    async fn on_run_completed(&self, _run: RunId) {}
    async fn on_run_paused(&self, _run: RunId, _reason: &str) {}
    async fn on_node_started(&self, _run: RunId, _node: &NodeId) {}
    async fn on_node_completed(&self, _run: RunId, _node: &NodeId) {}
    async fn on_node_failed(&self, _run: RunId, _node: &NodeId, _error: &str) {}
    async fn on_node_skipped(&self, _run: RunId, _node: &NodeId) {}
    async fn on_agent_started(&self, _run: RunId, _node: &NodeId, _agent: &str, _chain: &str) {}
    async fn on_agent_turn(&self, _run: RunId, _node: &NodeId, _turn: usize) {}
    async fn on_agent_tool_call(&self, _run: RunId, _node: &NodeId, _tool: &str) {}
    async fn on_context_write(&self, _run: RunId, _scope: &Scope, _key: &ContextKey) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct Spy(Arc<Mutex<Vec<String>>>);
    #[async_trait::async_trait]
    impl OrchestratorHooks for Spy {
        async fn on_node_started(&self, _run: RunId, node: &NodeId) {
            self.0.lock().unwrap().push(format!("started({})", node.0));
        }
    }

    /// A default (no-op) impl and a partial override both compile and behave.
    #[tokio::test]
    async fn default_is_noop_and_override_records() {
        struct NoOp;
        impl OrchestratorHooks for NoOp {}
        let run = RunId(uuid::Uuid::new_v4());
        NoOp.on_run_started(run).await; // no panic, returns ()

        let log = Arc::new(Mutex::new(Vec::new()));
        let spy = Spy(log.clone());
        spy.on_node_started(run, &NodeId("n1".into())).await;
        spy.on_run_started(run).await; // defaulted no-op → not recorded
        assert_eq!(*log.lock().unwrap(), vec!["started(n1)".to_string()]);
    }
}
```

> Implementer: `orchestrator-core` already depends on `async-trait`, `uuid`, `tokio` (dev). If `tokio` isn't a dev-dependency of `orchestrator-core`, either add it under `[dev-dependencies]` or make the test a plain sync check of the recording impl via `futures::executor::block_on` — prefer adding `tokio = { version = "1", features = ["macros", "rt"] }` to `[dev-dependencies]` to match the other crates.

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator-core default_is_noop_and_override 2>&1 | grep -E "error|test result"`
Expected: FAIL (module `hooks` not declared).

- [ ] **Step 3: Wire the module + export.** In `lib.rs` add `pub mod hooks;` (alphabetical among the `pub mod` lines) and `pub use hooks::OrchestratorHooks;` (with the other `pub use`s).

- [ ] **Step 4: Run to verify pass + core suite**

Run: `cargo test -p sensei-orchestrator-core > /tmp/t.log 2>&1; echo "EXIT=$?"; grep "test result" /tmp/t.log`
Expected: `EXIT=0`.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator-core): OrchestratorHooks trait (hooks)"
```

---

## Task 2: Executor wiring + run/node/context hooks from `append`

**Files:**
- Modify: `crates/orchestrator/src/executor/mod.rs`
- Modify: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the failing tests** in `tests.rs`. First the spy + import:

```rust
use orchestrator_core::OrchestratorHooks; // add to the core `use` block

/// A hooks spy: each fired hook appends a "label(args)" string.
#[derive(Clone, Default)]
struct RecordingHooks(Arc<std::sync::Mutex<Vec<String>>>);
impl RecordingHooks {
    fn log(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
    fn push(&self, s: String) {
        self.0.lock().unwrap().push(s);
    }
}
#[async_trait::async_trait]
impl OrchestratorHooks for RecordingHooks {
    async fn on_run_started(&self, _r: RunId) { self.push("run_started".into()); }
    async fn on_run_completed(&self, _r: RunId) { self.push("run_completed".into()); }
    async fn on_run_paused(&self, _r: RunId, reason: &str) { self.push(format!("run_paused({reason})")); }
    async fn on_node_started(&self, _r: RunId, n: &NodeId) { self.push(format!("node_started({})", n.0)); }
    async fn on_node_completed(&self, _r: RunId, n: &NodeId) { self.push(format!("node_completed({})", n.0)); }
    async fn on_node_failed(&self, _r: RunId, n: &NodeId, _e: &str) { self.push(format!("node_failed({})", n.0)); }
    async fn on_node_skipped(&self, _r: RunId, n: &NodeId) { self.push(format!("node_skipped({})", n.0)); }
    async fn on_agent_started(&self, _r: RunId, n: &NodeId, agent: &str, chain: &str) { self.push(format!("agent_started({},{agent},{chain})", n.0)); }
    async fn on_agent_turn(&self, _r: RunId, n: &NodeId, turn: usize) { self.push(format!("agent_turn({},{turn})", n.0)); }
    async fn on_agent_tool_call(&self, _r: RunId, n: &NodeId, tool: &str) { self.push(format!("agent_tool_call({},{tool})", n.0)); }
    async fn on_context_write(&self, _r: RunId, _s: &orchestrator_core::Scope, k: &orchestrator_core::ContextKey) { self.push(format!("context_write({})", k.0)); }
}
```

```rust
/// Acceptance §9.1 — run + node lifecycle fires in order.
#[tokio::test]
async fn hooks_fire_run_and_node_lifecycle_in_order() {
    let hooks = RecordingHooks::default();
    let (gw, _c) = recording_gateway().await;
    let (graph, _n1, _n2) = two_node_graph("a", "b");
    Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_hooks(Arc::new(hooks.clone()))
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert_eq!(
        hooks.log(),
        vec![
            "run_started",
            "node_started(n1)", "node_completed(n1)",
            "node_started(n2)", "node_completed(n2)",
            "run_completed",
        ]
    );
}

/// Acceptance §9.3 — a failed node fires on_node_failed; a hard-dependent fires
/// on_node_skipped.
#[tokio::test]
async fn hooks_fire_failure_and_cascade_skip() {
    let hooks = RecordingHooks::default();
    let (gw, _c) = content_gated_gateway().await;
    let mc = |p: &str| NodeKind::ModelCall { chain: "c".into(), payload: serde_json::json!({ "prompt": p }) };
    let graph = Graph {
        nodes: vec![
            Node { id: NodeId("f".into()), kind: mc("FAIL"), deps: vec![] },
            Node { id: NodeId("h".into()), kind: mc("ok"), deps: vec![Dep::hard("f")] },
        ],
    };
    Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_hooks(Arc::new(hooks.clone()))
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run yields an outcome");
    let log = hooks.log();
    assert!(log.contains(&"node_failed(f)".to_string()), "{log:?}");
    assert!(log.contains(&"node_skipped(h)".to_string()), "{log:?}");
    assert!(!log.contains(&"run_completed".to_string()), "a failed run does not complete");
}

/// Acceptance §9.7 — no hooks wired ⇒ identical journal (hooks change nothing).
#[tokio::test]
async fn no_hooks_wired_is_byte_identical() {
    let (gw1, _c1) = recording_gateway().await;
    let (gw2, _c2) = recording_gateway().await;
    let (graph, _n1, _n2) = two_node_graph("a", "b");
    let j1 = InMemoryJournal::new();
    let j2 = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    Executor::new(Arc::new(gw1), Arc::new(j1.clone()), "v1").run(run, &graph).await.unwrap();
    Executor::new(Arc::new(gw2), Arc::new(j2.clone()), "v1")
        .with_hooks(Arc::new(RecordingHooks::default()))
        .run(run, &graph).await.unwrap();
    let labels = |j: &InMemoryJournal| async move {
        j.load(run).await.unwrap().iter().map(|(_, e)| label(e)).collect::<Vec<_>>()
    };
    assert_eq!(labels(&j1).await, labels(&j2).await, "hooks change no journaled event");
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator hooks_fire_run_and_node 2>&1 | grep -E "no method|error\[|test result"`
Expected: FAIL (`with_hooks` does not exist).

- [ ] **Step 3: Add the field + builder + `append` firing.** In `mod.rs`:
  - Add `use orchestrator_core::{… OrchestratorHooks …};` to the core import block.
  - Add the field to `Executor` (after `context`):
    ```rust
    /// Best-effort observability hooks (§15). `None` ⇒ no firing (byte-identical).
    hooks: Option<Arc<dyn OrchestratorHooks>>,
    ```
    Add `hooks: None,` in `new()`.
  - Add the builder (near `with_context_store`):
    ```rust
    /// Attach best-effort observability hooks (§15). Fired at run/node/agent/
    /// context lifecycle; never affect execution/determinism; no double-count on
    /// resume. No hooks ⇒ zero firing (byte-identical).
    pub fn with_hooks(mut self, hooks: Arc<dyn OrchestratorHooks>) -> Self {
        self.hooks = Some(hooks);
        self
    }
    ```
  - Change `append` to fire the matching hook AFTER a successful journal write,
    cloning the event ONLY when hooks are wired (so the no-hooks path is
    byte-identical + allocation-free):
    ```rust
    async fn append(&self, run: RunId, event: JournalEvent) -> Result<Seq, OrchestratorError> {
        // Clone for the post-journal hook match only when hooks are wired.
        let hook_event = self.hooks.as_ref().map(|_| event.clone());
        let seq = self
            .journal
            .append(run, event)
            .await
            .map_err(OrchestratorError::Journal)?;
        if let (Some(h), Some(ev)) = (&self.hooks, &hook_event) {
            match ev {
                JournalEvent::RunStarted { .. } => h.on_run_started(run).await,
                JournalEvent::RunCompleted => h.on_run_completed(run).await,
                JournalEvent::RunPaused { reason, .. } => h.on_run_paused(run, reason).await,
                JournalEvent::NodeStarted { node } => h.on_node_started(run, node).await,
                JournalEvent::NodeCompleted { node } => h.on_node_completed(run, node).await,
                JournalEvent::NodeFailed { node, error } => h.on_node_failed(run, node, error).await,
                JournalEvent::NodeSkipped { node } => h.on_node_skipped(run, node).await,
                JournalEvent::ContextWrite { scope, key, .. } => h.on_context_write(run, scope, key).await,
                _ => {}
            }
        }
        Ok(seq)
    }
    ```

- [ ] **Step 4: Run to verify pass + full suite**

Run: `cargo test -p sensei-orchestrator > /tmp/t.log 2>&1; echo "EXIT=$?"; grep -E "test result:" /tmp/t.log | head -1`
Expected: `EXIT=0`.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator): with_hooks + run/node/context hooks from append (hooks)"
```

---

## Task 3: Agent hooks (live path in `drive_agent`)

**Files:**
- Modify: `crates/orchestrator/src/executor/agent.rs`
- Modify: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Failing test** in `tests.rs`:

```rust
/// Acceptance §9.2 — agent lifecycle: an Agent node that makes one tool call then
/// finishes fires agent_started, agent_turn(0), agent_tool_call, agent_turn(1),
/// plus the generic node_started/node_completed.
#[tokio::test]
async fn hooks_fire_agent_lifecycle() {
    let hooks = RecordingHooks::default();
    let (gw, _c) = scripted_gateway(vec![
        tool_call_response("t1", "calc", "{\"op\":\"add\",\"a\":2,\"b\":3}"),
        final_response("the answer is 5"),
    ]).await;
    let graph = Graph { nodes: vec![agent_node("n1", "a", "add 2 and 3")] };
    Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools())
        .with_hooks(Arc::new(hooks.clone()))
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    let log = hooks.log();
    assert!(log.contains(&"agent_started(n1,a,c)".to_string()), "{log:?}");
    assert!(log.contains(&"agent_turn(n1,0)".to_string()), "{log:?}");
    assert!(log.contains(&"agent_tool_call(n1,calc)".to_string()), "{log:?}");
    assert!(log.contains(&"agent_turn(n1,1)".to_string()), "{log:?}");
    assert!(log.contains(&"node_completed(n1)".to_string()), "{log:?}");
}
```

> Implementer: `tool_agent_registry`'s agent is `"a"` on chain `"c"`, so `agent_started(n1,a,c)`. Confirm the agent name passed is `agent_ref.0` and the chain is `agent.chain`.

- [ ] **Step 2: Run to confirm failure** (agent hooks not fired yet).

- [ ] **Step 3: Fire the agent hooks on the live path.** In `agent.rs`:
  - **`on_agent_started`** — in `drive_agent`, right after computing `node_started` (line ~77, `let mut node_started = fold.started.contains(node_id);`), gated so it fires once (only when the node hasn't already started, i.e. not a resume replay):
    ```rust
    if !node_started && let Some(h) = &self.hooks {
        h.on_agent_started(run, node_id, &agent_ref.0, &ar.chain).await;
    }
    ```
    (`ar.chain` is the resolved chain; `agent_ref.0` the agent name. If the borrow
    checker objects to using `ar` here, capture `chain` before building `ar`, or
    read `&agent.chain`.)
  - **`on_agent_turn`** — in `agent_turn_output`, on the LIVE branch only (after the
    memo-miss, i.e. the code path that reaches `dispatch_model_turn`), before
    dispatch:
    ```rust
    if let Some(h) = &self.hooks {
        h.on_agent_turn(ar.run, ar.node_id, turn).await;
    }
    ```
    Place it after the `node_started`/`NodeStarted` block and before
    `dispatch_model_turn`, so a memoized replay (which returns earlier) never fires it.
  - **`on_agent_tool_call`** — in `record_tool_effect` (the live tool-execution
    helper), before/at `self.tools.execute(&call.name, args)`:
    ```rust
    if let Some(h) = &self.hooks {
        h.on_agent_tool_call(ar.run, ar.node_id, &call.name).await;
    }
    ```
    (A memo-hit tool replays via `materialize` and never calls `record_tool_effect`,
    so this is live-only.)

- [ ] **Step 4: Run to verify pass + full suite + clippy**

Run: `cargo test -p sensei-orchestrator > /tmp/t.log 2>&1; echo "EXIT=$?"; grep -E "test result:" /tmp/t.log | head -1`
Run: `cargo clippy --workspace --all-targets -- -D warnings > /tmp/c.log 2>&1; echo "CLIPPY=$?"`
Expected: `EXIT=0`, `CLIPPY=0`.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator): agent lifecycle hooks on the live path (hooks)"
```

---

## Task 4: Resume-does-not-re-fire (headline) + pause + context

**Files:**
- Modify: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Resume-no-refire test (acceptance §9.6).** `n1(model) → n2(model, hard-dep n1)`. Seed with `failing_after_gateway(1)`: n1 succeeds, n2 fails (no RunCompleted). Resume on a fresh gateway with a spy attached to the RESUME executor: n1 replays from the memo (no re-append ⇒ no `node_started(n1)`/`node_completed(n1)`), only n2's tail fires. Assert the resume spy contains `node_started(n2)`/`node_completed(n2)`/`run_completed` and does NOT contain any `n1` node hook.

```rust
#[tokio::test]
async fn hooks_do_not_refire_for_replayed_prefix_on_resume() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (graph, _n1, _n2) = two_node_graph("a", "b");
    // Seed: n1 ok, n2 fails (failing_after 1) → no RunCompleted.
    let (gw1, _c1) = failing_after_gateway(1).await;
    Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1").run(run, &graph).await.expect("seed");
    // Resume with a fresh spy: only n2's tail should fire.
    let hooks = RecordingHooks::default();
    let (gw2, _c2) = recording_gateway().await;
    Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_hooks(Arc::new(hooks.clone()))
        .start(run, &graph).await.expect("resume");
    let log = hooks.log();
    assert!(!log.iter().any(|l| l.contains("(n1)")), "n1 (replayed) fires no hook on resume: {log:?}");
    assert!(log.contains(&"node_started(n2)".to_string()) && log.contains(&"node_completed(n2)".to_string()), "{log:?}");
    assert!(log.contains(&"run_completed".to_string()));
}
```

- [ ] **Step 2: Pause test (acceptance §9.4).** Reuse the slice-4 in-doubt seed
  (`seed_in_doubt_note`) + `resume_in_doubt`-style resume with an
  `AlwaysIndeterminate` reconciler and a spy: assert the spy contains
  `run_paused(...)` and NOT `run_completed`. (Model on the existing
  `in_doubt_indeterminate_pauses_without_applying` test; attach `.with_hooks`.)

- [ ] **Step 3: Context-write test (acceptance §9.5).** Reuse the blackboard
  `completed_node_publishes_a_context_ref_to_the_blackboard` shape with a
  `ContextStore` + a spy; assert the spy contains `context_write(n1)`.

- [ ] **Step 4: Run + full suite + clippy**

Run: `cargo test -p sensei-orchestrator hooks_ > /tmp/t.log 2>&1; echo "EXIT=$?"; grep "test result" /tmp/t.log`
Run: `cargo clippy --workspace --all-targets -- -D warnings > /tmp/c.log 2>&1; echo "CLIPPY=$?"`
Expected: `EXIT=0`, `CLIPPY=0`.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "test(orchestrator): hooks resume-no-refire + pause + context-write (hooks)"
```

---

## Task 5: Docs + memory

**Files:**
- Modify: `docs/features/orchestrator/hooks.md`, `docs/features/orchestrator/README.md`

- [ ] **Step 1: Flip `hooks.md`** to `partial`: record the wired trait
  (run/node/agent/context hooks, append-centric firing, replay-suppressed,
  opt-in/byte-identical) and the deferred set (plan_expanded, stream_chunk,
  model_attempt, usage/cost, run_resumed, fold-time replay firing, HookError
  channel + panic isolation, node kind, tool_result). Update the README status row
  for hooks to `Partial (SP-1)` with a one-line summary.

- [ ] **Step 2: Full workspace gate**

Run: `cargo test --workspace > /tmp/ws.log 2>&1; echo "WS=$?"; grep -Eo "[0-9]+ passed" /tmp/ws.log | awk '{s+=$1} END{print s" passed"}'`
Run: `cargo clippy --workspace --all-targets -- -D warnings > /tmp/c.log 2>&1; echo "CLIPPY=$?"`
Expected: `WS=0`, `CLIPPY=0`.

- [ ] **Step 3: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "docs(orchestrator): OrchestratorHooks wired (SP-1 hooks COMPLETE)"
```

---

## Notes for the implementer

- **`append`-centric firing is the correctness lever:** every run/node/context hook fires exactly where its journal event is appended, so a resumed completed prefix (not re-appended, fold-guarded) never re-fires — no explicit replay flag needed. Do NOT scatter node-hook calls into the run_* methods; keep them in `append`.
- **Agent hooks fire on the LIVE path only** (`drive_agent` gated on `!node_started`; `agent_turn` on the live model-dispatch branch; `agent_tool_call` in `record_tool_effect`) so a memoized replay skips them.
- **Zero overhead when unwired:** clone the event for the match only when `self.hooks.is_some()`; the no-hooks path must stay byte-identical (an acceptance test asserts identical journals).
- Hooks return `()` and are awaited inline — best-effort; the HookError channel + panic isolation are deferred. A hook must not depend on execution.
- Pre-commit runs `make lint` (fmt + clippy, NO tests). Run `cargo test --workspace` yourself before committing. After any `git checkout <file>`, re-run the full suite before committing. Branch: `feat/sp1-hooks` off `develop`.
