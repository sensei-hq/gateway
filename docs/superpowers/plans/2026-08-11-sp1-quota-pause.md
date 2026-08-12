# SP-1 quota→pause Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Map the gateway's terminal chain-gated result to a durable pause — `GatewayError::AllGated{resume_after: Some(t)}` → `RunPaused`/`RunOutcome.paused` (resumable); `AllGated{None}` (all gates terminal) → fail-fast with the human-action hint.

**Architecture:** A pure `classify_gateway_error(&GatewayError) -> GatewayDisposition{Pause{resume_after,reason}|Fail(String)}`. The executor's gateway-`Err` sites (top-level `ModelCall` in `run_node`; agent turns in `dispatch_model_turn`) route `Pause` into the EXISTING pause channels (`NodeExec::Paused` / `ToolOutcome::Paused → AgentStep::Paused`, `RunPaused`, `RunOutcome.paused`), and `Fail` into today's `NodeFailed`. A gated call records no `EffectRecorded`, so a resume simply re-attempts the node (quota may have reset).

**Tech Stack:** Rust; `sensei-orchestrator` (executor + test_support). The gateway's cooldown gate cools a router on any `Timeout` fault (validated), so a warm-up `execute` on a single-candidate timeout chain makes the next `execute` return `AllGated{Some}` — the integration-test fixture.

**Design:** `docs/superpowers/specs/2026-08-11-sp1-quota-pause-design.md`. The warm-up recipe was spiked and confirmed (`execute` after a Timeout warm-up → `AllGated{resume_after: Some, skipped:["r:m — router cooling down"], human_action: None}`).

**Conventions (non-negotiable):** TDD (failing test → watch fail → minimal code). `cargo fmt --all` before every commit (pre-commit = fmt-check + workspace `clippy -D warnings`, NO tests — run `cargo test --workspace` yourself before committing). Verify REAL exit codes, never a piped `| tail`. Commit a fix BEFORE any `git checkout`-based mutation-verify (a checkout reverts uncommitted fixes).

---

## File structure

- `crates/orchestrator/src/test_support.rs` — `TimeoutAdapter` + `timeout_gateway()` (warm-up fixture).
- `crates/orchestrator/src/executor/support.rs` — `GatewayDisposition` + `classify_gateway_error`.
- `crates/orchestrator/src/executor/mod.rs` — `run_node` ModelCall gateway-`Err` arm routes `Pause`.
- `crates/orchestrator/src/executor/agent.rs` — `dispatch_model_turn`/`agent_turn_output` return `ToolOutcome`; gateway-`Err` routes `Pause`; `drive_agent` maps `Paused`.
- `crates/orchestrator/src/executor/tests.rs` — fixture test + acceptance tests.
- `docs/features/orchestrator/{durable-executor.md,README.md}` — note the mapping.

---

## Task 1: warm-up fixture (`timeout_gateway`) + fixture test

**Files:**
- Modify: `crates/orchestrator/src/test_support.rs`, `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Add the fixture** to `test_support.rs` (imports `GatewayError`, `Model`, `ChatModel`, etc. are already in scope):

```rust
/// A chat adapter (`id = "r"`) that always times out — a transport fault that
/// cools its router. Warm-gating a single-candidate chain (one `execute`) makes
/// the NEXT `execute` return `AllGated{resume_after: Some(_)}`.
pub struct TimeoutAdapter;
impl Model for TimeoutAdapter {
    fn id(&self) -> &str { "r" }
}
#[async_trait]
impl ChatModel for TimeoutAdapter {
    async fn chat(&self, _cfg: &RouterConfig, _req: &ChatRequest) -> Result<ChatResponse, GatewayError> {
        Err(GatewayError::Timeout { adapter: "r".into(), model: "m".into(), duration_ms: 1 })
    }
}

/// A single-candidate gateway whose only router times out. One warm-up `execute`
/// cools it; the next `execute` finds the sole candidate gated → `AllGated`.
pub async fn timeout_gateway() -> Gateway {
    let adapters = AdapterRegistry::new();
    adapters.register_chat(Arc::new(TimeoutAdapter)).await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    Gateway::new(single_chain_config(), adapters, cb)
}
```

- [ ] **Step 2: Write the fixture test** (acceptance §6.1) in `tests.rs`:

```rust
/// Acceptance §6.1 — the warm-up fixture yields a genuine AllGated: a first
/// (warm-up) execute times out and cools the sole router; the second execute is
/// all-gated with a timed resume_after.
#[tokio::test]
async fn warmup_gateway_yields_allgated_with_resume_after() {
    use crate::test_support::timeout_gateway;
    let gw = timeout_gateway().await;
    let req = support::build_request("c", &serde_json::json!({ "prompt": "x" }));
    let _warm = gw.execute(&req).await; // times out → cools router "r"
    let second = gw.execute(&req).await;
    assert!(
        matches!(
            second,
            Err(kernel::types::error::GatewayError::AllGated { resume_after: Some(_), .. })
        ),
        "second execute is AllGated with a timed resume_after: {second:?}"
    );
}
```

> Implementer: `support` is `crate::executor::support`; `build_request(chain, payload)` is `pub(crate)`. If `kernel` isn't already a path in `tests.rs`, use the fully-qualified `kernel::types::error::GatewayError`.

- [ ] **Step 3: Run — expect PASS** (the fixture is validated; this pins it):

Run: `cargo test -p sensei-orchestrator warmup_gateway_yields_allgated 2>&1 | grep "test result"`
Expected: `ok`.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "test(orchestrator): timeout_gateway warm-up fixture yields AllGated (quota-pause)"
```

---

## Task 2: `classify_gateway_error` (pure policy)

**Files:**
- Modify: `crates/orchestrator/src/executor/support.rs`

- [ ] **Step 1: Failing test** in `support.rs` (`#[cfg(test)] mod tests` — add if absent):

```rust
#[test]
fn classify_gateway_error_pauses_only_on_timed_allgated() {
    use kernel::types::error::{GatewayError, HumanAction};
    let t = chrono::DateTime::from_timestamp(1_000_000_000, 0).unwrap();
    // Timed AllGated → Pause (reason names the instant).
    match classify_gateway_error(&GatewayError::AllGated { resume_after: Some(t), skipped: vec![], human_action: None }) {
        GatewayDisposition::Pause { resume_after, reason } => {
            assert_eq!(resume_after, t);
            assert!(reason.contains(&t.to_string()));
        }
        d => panic!("expected Pause, got {d:?}"),
    }
    // Terminal AllGated → Fail (message carries the human-action hint).
    let none = GatewayError::AllGated { resume_after: None, skipped: vec![], human_action: Some(HumanAction::TopUpCredits) };
    let none_msg = none.to_string();
    assert!(matches!(classify_gateway_error(&none), GatewayDisposition::Fail(m) if m == none_msg));
    // Other errors → Fail.
    let quota = GatewayError::BudgetExceeded { estimated: 1.0, remaining: 0.0 };
    assert!(matches!(classify_gateway_error(&quota), GatewayDisposition::Fail(_)));
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator classify_gateway_error_pauses 2>&1 | grep -E "cannot find|error\[|test result"`
Expected: FAIL (`classify_gateway_error`/`GatewayDisposition` do not exist).

- [ ] **Step 3: Implement** in `support.rs` (add `use kernel::types::error::GatewayError;` if needed; derive `Debug` on the enum for the test's `panic!`):

```rust
/// How the executor should treat a gateway error (§11.2): a timed chain-gate is a
/// durable pause; everything else fails (a terminal gate carries its human-action
/// hint in the message).
#[derive(Debug)]
pub(crate) enum GatewayDisposition {
    Pause {
        resume_after: chrono::DateTime<chrono::Utc>,
        reason: String,
    },
    Fail(String),
}

/// Classify a gateway error: only `AllGated{resume_after: Some(t)}` pauses (to
/// `t`); every other error — including `AllGated{None}` (all gates terminal) —
/// fails, its `Display` carrying the reason/human-action hint.
pub(crate) fn classify_gateway_error(err: &GatewayError) -> GatewayDisposition {
    match err {
        GatewayError::AllGated { resume_after: Some(t), .. } => GatewayDisposition::Pause {
            resume_after: *t,
            reason: format!("all candidates gated; resume after {t}"),
        },
        other => GatewayDisposition::Fail(other.to_string()),
    }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p sensei-orchestrator classify_gateway_error_pauses 2>&1 | grep "test result"` → `ok`.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator): classify_gateway_error — AllGated{Some}→Pause, else Fail (quota-pause)"
```

---

## Task 3: `run_node` ModelCall pauses on a timed gate

**Files:**
- Modify: `crates/orchestrator/src/executor/mod.rs`
- Modify: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Failing tests** in `tests.rs`:

```rust
/// Acceptance §6.3 — a top-level ModelCall node whose chain is all-gated (timed)
/// PAUSES: RunOutcome.paused set, RunPaused{resume_after:Some} journaled, no
/// RunCompleted, and on_run_paused fires.
#[tokio::test]
async fn modelcall_node_pauses_on_a_timed_gate() {
    use crate::test_support::timeout_gateway;
    let hooks = RecordingHooks::default();
    let gw = timeout_gateway().await;
    let req = support::build_request("c", &serde_json::json!({ "prompt": "warm" }));
    let _ = gw.execute(&req).await; // warm-up cools router "r"
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![Node { id: NodeId("n1".into()), kind: model_call("c", "go"), deps: vec![] }],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_hooks(Arc::new(hooks.clone()))
        .run(run, &graph)
        .await
        .expect("run yields an outcome");
    let pause = out.paused.expect("the all-gated node pauses");
    assert_eq!(pause.node, NodeId("n1".into()));
    assert!(out.failed.is_none(), "a timed gate pauses, does not fail: {:?}", out.failed);
    let events = journal.load(run).await.unwrap();
    assert!(events.iter().any(|(_, e)| matches!(e, JournalEvent::RunPaused { resume_after: Some(_), .. })));
    assert!(!events.iter().any(|(_, e)| matches!(e, JournalEvent::RunCompleted)));
    assert!(hooks.log().iter().any(|l| l.starts_with("run_paused(")), "{:?}", hooks.log());
}
```

- [ ] **Step 2: Run to confirm failure** (the node currently `NodeFailed`s on any gateway error, so `out.paused` is `None`).

- [ ] **Step 3: Implement** — in `run_node`'s `NodeKind::ModelCall` arm, replace the `Err(error)` handling with a classify branch:

```rust
                    Err(error) => match support::classify_gateway_error(&error) {
                        support::GatewayDisposition::Pause { resume_after, reason } => {
                            self.append(
                                run,
                                JournalEvent::RunPaused {
                                    reason: reason.clone(),
                                    resume_after: Some(resume_after),
                                },
                            )
                            .await?;
                            Ok(NodeExec::Paused { reason })
                        }
                        support::GatewayDisposition::Fail(message) => {
                            self.append(
                                run,
                                JournalEvent::NodeFailed { node: node.id.clone(), error: message.clone() },
                            )
                            .await?;
                            Ok(NodeExec::Failed { message, output: None })
                        }
                    },
```

(Add `classify_gateway_error`, `GatewayDisposition` to the `use support::{...}` line in `mod.rs`.)

- [ ] **Step 4: Terminal-gate-fails is covered by the classifier unit test** (§6.5): `AllGated{None}` → `Fail` → the same `NodeFailed` path as any gateway error (already exercised by existing failure tests). No separate integration test (constructing a real terminal lockout via the gateway is out of scope; the policy is unit-tested in Task 2).

- [ ] **Step 5: Run + full suite**

Run: `cargo test -p sensei-orchestrator > /tmp/t.log 2>&1; echo "EXIT=$?"; grep -E "test result:" /tmp/t.log | head -1`
Expected: `EXIT=0`.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator): ModelCall node pauses on a timed chain-gate (quota-pause)"
```

---

## Task 4: agent turns pause on a timed gate

**Files:**
- Modify: `crates/orchestrator/src/executor/agent.rs`
- Modify: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Failing test** in `tests.rs`:

```rust
/// Acceptance §6.4 — an Agent node whose turn is all-gated (timed) pauses.
#[tokio::test]
async fn agent_node_pauses_on_a_timed_gate() {
    use crate::test_support::timeout_gateway;
    let gw = timeout_gateway().await;
    let req = support::build_request("c", &serde_json::json!({ "prompt": "warm" }));
    let _ = gw.execute(&req).await; // warm-up cools router "r"
    let graph = Graph { nodes: vec![agent_node("n1", "a", "go")] };
    let out = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(ToolRegistry::default()))
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run yields an outcome");
    assert!(out.paused.is_some(), "the agent's gated turn pauses: {:?}", out.failed);
    assert!(out.failed.is_none());
}
```

- [ ] **Step 2: Run to confirm failure** (agent turn currently maps the gateway error to `NodeFailed`/`AgentStep::Failed`).

- [ ] **Step 3: Implement — widen the model-turn return to `ToolOutcome`.**
  - `dispatch_model_turn` (`agent.rs`): change its return to `Result<ToolOutcome<serde_json::Value>, OrchestratorError>`. Its `Ok(response)` arm returns `Ok(ToolOutcome::Ok(output))`. Its `Err(error)` arm classifies:
    ```rust
    Err(error) => match crate::executor::support::classify_gateway_error(&error) {
        crate::executor::support::GatewayDisposition::Pause { resume_after, reason } => {
            self.append(run, JournalEvent::RunPaused { reason: reason.clone(), resume_after: Some(resume_after) }).await?;
            Ok(ToolOutcome::Paused(reason))
        }
        crate::executor::support::GatewayDisposition::Fail(message) => {
            self.append(run, JournalEvent::NodeFailed { node: node_id.clone(), error: message.clone() }).await?;
            Ok(ToolOutcome::Failed(message))
        }
    },
    ```
    (`dispatch_model_turn` takes `node_id: &NodeId` — use it for `NodeFailed`.)
  - `agent_turn_output` (`agent.rs`): change its return to `Result<ToolOutcome<serde_json::Value>, OrchestratorError>`. The memo-hit path → `Ok(ToolOutcome::Ok(self.materialize(output).await?))`; the over-budget path → `Ok(ToolOutcome::Failed(message))`; the tail returns `dispatch_model_turn(...)` directly.
  - `drive_agent` (`agent.rs`): update the call site:
    ```rust
    let turn_output = match self.agent_turn_output(&ar, turn, &messages, &mut node_started).await? {
        ToolOutcome::Ok(output) => output,
        ToolOutcome::Failed(failure) => return Ok(AgentStep::Failed(failure)),
        ToolOutcome::Paused(reason) => return Ok(AgentStep::Paused(reason)),
    };
    ```
  `AgentStep::Paused` already flows to `NodeExec::Paused` (top-level / Consolidate) and `MapChildPaused` (Map/Loop children) — no further wiring.

- [ ] **Step 4: Run + full suite + clippy**

Run: `cargo test -p sensei-orchestrator > /tmp/t.log 2>&1; echo "EXIT=$?"; grep -E "test result:" /tmp/t.log | head -1`
Run: `cargo clippy --workspace --all-targets -- -D warnings > /tmp/c.log 2>&1; echo "CLIPPY=$?"`
Expected: `EXIT=0`, `CLIPPY=0`.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator): agent turns pause on a timed chain-gate (quota-pause)"
```

---

## Task 5: resume re-attempt + docs + memory

**Files:**
- Modify: `crates/orchestrator/src/executor/tests.rs`
- Modify: `docs/features/orchestrator/durable-executor.md`, `docs/features/orchestrator/README.md`

- [ ] **Step 1: Resume-reattempt test (acceptance §6.6).** A ModelCall node paused on a warmed-up (cooled) `timeout_gateway`, then resumed with a FRESH un-gated gateway (`recording_gateway`) sharing the SAME journal → the node re-attempts, succeeds, and the run completes.

```rust
#[tokio::test]
async fn a_paused_gated_run_reattempts_and_completes_on_resume() {
    use crate::test_support::timeout_gateway;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![Node { id: NodeId("n1".into()), kind: model_call("c", "go"), deps: vec![] }],
    };
    // Pause: warm-up cools the gateway, the node is all-gated → paused.
    let gw = timeout_gateway().await;
    let req = support::build_request("c", &serde_json::json!({ "prompt": "warm" }));
    let _ = gw.execute(&req).await;
    let o1 = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1").run(run, &graph).await.expect("run");
    assert!(o1.paused.is_some(), "first run pauses");
    // Resume on a fresh, un-gated gateway → n1 re-attempts and completes.
    let (gw2, _c2) = recording_gateway().await;
    let o2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1").start(run, &graph).await.expect("resume");
    assert!(o2.failed.is_none() && o2.paused.is_none(), "resume completes: {:?}", o2.paused);
    assert!(journal.load(run).await.unwrap().iter().any(|(_, e)| matches!(e, JournalEvent::RunCompleted)));
}
```

- [ ] **Step 2: Full workspace gate**

Run: `cargo test --workspace > /tmp/ws.log 2>&1; echo "WS=$?"; grep -Eo "[0-9]+ passed" /tmp/ws.log | awk '{s+=$1} END{print s" passed"}'`
Run: `cargo clippy --workspace --all-targets -- -D warnings > /tmp/c.log 2>&1; echo "CLIPPY=$?"`
Expected: `WS=0`, `CLIPPY=0`.

- [ ] **Step 3: Docs.** In `durable-executor.md`, add a line under the gateway-mapping / resilience notes: `AllGated{resume_after:Some}` → durable `RunPaused` (resumable, re-attempts on resume); `AllGated{None}` → fail-fast with the human-action hint. Note this completes the SP-1 walking skeleton's quota→pause. Update the README if it has a matching row.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator): quota→pause resume re-attempt + docs (SP-1 quota→pause COMPLETE)"
```

---

## Notes for the implementer

- **The whole policy is `classify_gateway_error`** (pure). The wiring at each gateway-`Err` site just calls it and routes `Pause`/`Fail` into the existing channels — do not scatter `AllGated` matching across sites.
- **Pause channels already exist** (`NodeExec::Paused`, `ToolOutcome::Paused`, `AgentStep::Paused`, `RunPaused`, `RunOutcome.paused`, `on_run_paused`). Agent-turn pause reuses `ToolOutcome` (already defined for the Mutation path); a Map/Loop agent child that pauses flows to `MapChildPaused` → whole-Map/Loop pause (existing).
- **Resume-safety is free:** a gated call journals no `EffectRecorded`, so the node re-attempts on resume (no memo, no determinism fence). The paused node is not in `fold.completed`, so a fresh drive re-runs it.
- **`resume_after`** goes into the journaled `RunPaused` only; `RunOutcome.paused`/`PauseInfo` stay `{node, reason}` (the durable scheduler that consumes `resume_after` is deferred).
- **Deferred:** ModelCall bodies in Map/Consolidate/Loop pausing on a gate (they fail); the durable re-arm scheduler; `RateLimit`→`Timer` backoff.
- Pre-commit runs fmt + clippy (NO tests); run `cargo test --workspace` before committing. Branch: `feat/sp1-quota-pause` off `develop`.
