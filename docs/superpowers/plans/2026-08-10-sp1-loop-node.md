# SP-1 Loop Node Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `NodeKind::Loop` — iterate a body until a deterministic gate says Stop or `max_iters` is hit, resume-safe, never a bare fail.

**Architecture:** `run_loop` (in `fanout.rs`, beside `run_map`/`run_consolidate`) drives `for i in 0..max_iters`, running `body` (reuse `MapBody`) once per iteration at path `"{loop}/{i}"` (distinct `effect_id` per iteration, mirror Map children). Iteration `i>0`'s input is `i-1`'s output (refine thread). A pure `LoopGate::should_stop(output)` decides Stop/Continue — so a resume recomputes it from memoized outputs with no gate journaling. Cap-without-stop completes best-effort (`converged: false`); a body failure fails the Loop. Reuse `run_map_child_modelcall` for a `ModelCall` body; `drive_agent` for an `Agent` body.

**Tech Stack:** Rust; `sensei-orchestrator-core` (graph types), `sensei-orchestrator` (executor). `serde_json`, `tokio::test`.

**Design:** `docs/superpowers/specs/2026-08-10-sp1-loop-node-design.md`.

**Conventions (non-negotiable):** TDD (failing test → watch fail → minimal code). `cargo fmt --all` before every commit (pre-commit = fmt-check + workspace `clippy -D warnings`). Verify REAL exit codes, never a piped `| tail`. Each task ends green + clippy-clean.

---

## File structure

- `crates/orchestrator-core/src/graph.rs` — `NodeKind::Loop`, `LoopGate` + `should_stop`.
- `crates/orchestrator-core/src/lib.rs` — export `LoopGate`.
- `crates/orchestrator/src/executor/mod.rs` — `run_node` dispatch arm for `Loop`.
- `crates/orchestrator/src/executor/fanout.rs` — `run_loop`.
- `crates/orchestrator/src/executor/tests.rs` — acceptance tests.
- `docs/features/orchestrator/{execution-graph.md,README.md}` — status.

---

## Task 1: `NodeKind::Loop` + `LoopGate` (core)

**Files:**
- Modify: `crates/orchestrator-core/src/graph.rs`, `crates/orchestrator-core/src/lib.rs`

- [ ] **Step 1: Write the failing test** in `graph.rs` `#[cfg(test)] mod tests` (add the mod if absent; else append):

```rust
#[test]
fn loop_gate_should_stop_is_pure_over_output() {
    use super::LoopGate;
    let text = LoopGate::TextContains("DONE".into());
    assert!(text.should_stop(&serde_json::json!({ "text": "all DONE here" })));
    assert!(!text.should_stop(&serde_json::json!({ "text": "keep going" })));
    assert!(!text.should_stop(&serde_json::json!({ "other": "DONE" }))); // only checks `text`
    let field = LoopGate::FieldTrue("done".into());
    assert!(field.should_stop(&serde_json::json!({ "done": true })));
    assert!(!field.should_stop(&serde_json::json!({ "done": false })));
    assert!(!field.should_stop(&serde_json::json!({ "done": "true" }))); // strict: JSON true only
    assert!(!field.should_stop(&serde_json::json!({})));
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator-core loop_gate_should_stop 2>&1 | grep -E "error\[|cannot find|test result"`
Expected: FAIL (`LoopGate` does not exist).

- [ ] **Step 3: Add the types.** In `graph.rs`, add the `LoopGate` enum + impl (near `Aggregation`/`MapBody`), matching the file's derive style (check an existing enum, typically `#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]`):

```rust
/// A deterministic Stop condition for a [`NodeKind::Loop`], evaluated as a pure
/// function of one iteration's body output — so a resume recomputes the identical
/// decision with no gate journaling (§10.3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LoopGate {
    /// Stop when `output["text"]` contains this marker substring.
    TextContains(String),
    /// Stop when `output[field] == true` (strict JSON `true`).
    FieldTrue(String),
}

impl LoopGate {
    /// Whether this iteration's `output` satisfies the Stop condition.
    pub fn should_stop(&self, output: &serde_json::Value) -> bool {
        match self {
            LoopGate::TextContains(marker) => output
                .get("text")
                .and_then(|v| v.as_str())
                .is_some_and(|t| t.contains(marker.as_str())),
            LoopGate::FieldTrue(field) => output.get(field) == Some(&serde_json::Value::Bool(true)),
        }
    }
}
```

Add the `Loop` variant to `NodeKind` (after `Consolidate`):

```rust
    /// Iterate `body` (a `MapBody`) at path `"{loop}/{i}"`, feeding each
    /// iteration's output into the next as input (refine), until `gate` says Stop
    /// or `max_iters` is reached. Cap-without-Stop completes best-effort
    /// (`converged: false`), never a bare fail (§10.3). A body failure fails the
    /// Loop. Output: `{ iterations, converged, output }`.
    Loop {
        body: MapBody,
        input: serde_json::Value,
        gate: LoopGate,
        max_iters: usize,
    },
```

In `lib.rs`, add `LoopGate` to the graph re-export:
`pub use graph::{Aggregation, Dep, EdgeKind, Graph, LoopGate, MapBody, Node, NodeKind};`

- [ ] **Step 4: Run to verify pass + core suite**

Run: `cargo test -p sensei-orchestrator-core > /tmp/t.log 2>&1; echo "EXIT=$?"; grep "test result" /tmp/t.log`
Expected: `EXIT=0`.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator-core): NodeKind::Loop + LoopGate (loop node)"
```

---

## Task 2: `run_loop` + dispatch — stop / cap / refine / body-failure

**Files:**
- Modify: `crates/orchestrator/src/executor/fanout.rs` (`run_loop`)
- Modify: `crates/orchestrator/src/executor/mod.rs` (`run_node` dispatch + `NodeKind` import if needed)
- Modify: `crates/orchestrator/src/executor/tests.rs` (behavior tests)

- [ ] **Step 1: Write the failing tests** in `tests.rs` (the Loop node is built inline in each test — no helper needed). First import `LoopGate`: add it to the existing `use orchestrator_core::{…}` block in `tests.rs`.

```rust
/// Acceptance §9.1 — stop on gate: a Loop whose body emits the marker at
/// iteration 1 completes with iterations=2, converged=true, and ran the body twice.
#[tokio::test]
async fn loop_stops_when_the_gate_fires() {
    let (gw, calls) = scripted_gateway(vec![
        final_response("keep going"),
        final_response("we are DONE"),
    ]).await;
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("L".into()),
            kind: NodeKind::Loop {
                body: MapBody::ModelCall { chain: "c".into() },
                input: serde_json::json!({ "prompt": "start" }),
                gate: LoopGate::TextContains("DONE".into()),
                max_iters: 5,
            },
            deps: vec![],
        }],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    assert!(out.failed.is_none(), "{:?}", out.failed);
    let l = &out.outputs[&NodeId("L".into())];
    assert_eq!(l["iterations"], 2, "stopped at the 2nd iteration");
    assert_eq!(l["converged"], true);
    assert_eq!(l["output"]["text"], "we are DONE");
    assert_eq!(calls.lock().unwrap().len(), 2, "body ran exactly twice");
}

/// Acceptance §9.2 — cap without stop: the gate never fires, so the Loop runs
/// exactly max_iters and completes best-effort with converged=false (NOT failed).
#[tokio::test]
async fn loop_caps_at_max_iters_and_completes_unconverged() {
    let (gw, calls) = recording_gateway().await; // always "canned-response", never "STOP"
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("L".into()),
            kind: NodeKind::Loop {
                body: MapBody::ModelCall { chain: "c".into() },
                input: serde_json::json!({ "prompt": "go" }),
                gate: LoopGate::TextContains("STOP".into()),
                max_iters: 3,
            },
            deps: vec![],
        }],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    assert!(out.failed.is_none(), "cap is best-effort, not a failure: {:?}", out.failed);
    let l = &out.outputs[&NodeId("L".into())];
    assert_eq!(l["iterations"], 3);
    assert_eq!(l["converged"], false, "hit the cap without converging");
    assert_eq!(calls.lock().unwrap().len(), 3, "ran exactly max_iters times");
}

/// Acceptance §9.4 — a body failure fails the whole Loop (no silent finalize).
#[tokio::test]
async fn loop_body_failure_fails_the_loop() {
    let (gw, _c) = content_gated_gateway().await; // fails any prompt containing FAIL
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("L".into()),
            kind: NodeKind::Loop {
                body: MapBody::ModelCall { chain: "c".into() },
                input: serde_json::json!({ "prompt": "FAIL" }),
                gate: LoopGate::TextContains("never".into()),
                max_iters: 3,
            },
            deps: vec![],
        }],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run yields an outcome");
    let (node, msg) = out.failed.as_ref().expect("the loop fails on a body failure");
    assert_eq!(node.0, "L");
    assert!(msg.contains("iteration 0"), "names the failing iteration: {msg}");
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator loop_stops_when_the_gate_fires 2>&1 | grep -E "error\[|non-exhaustive|test result"`
Expected: FAIL — `run_node` has no `Loop` arm (non-exhaustive `match` on `NodeKind`), fixed in Step 3; then the tests fail (no `run_loop`).

- [ ] **Step 3: Implement `run_loop`** in `fanout.rs` (after `run_consolidate`):

```rust
    /// Run a `Loop` node (§10.3): iterate `body` at `"{loop}/{i}"`, threading each
    /// iteration's output into the next as input, until `gate` says Stop or
    /// `max_iters` is reached. Cap-without-Stop completes best-effort
    /// (`converged: false`); a body failure fails the Loop; an Agent-body pause
    /// pauses the Loop. Resume replays completed iterations (memo-hit, no re-spend)
    /// and recomputes the (pure) gate, stopping at the same iteration.
    pub(super) async fn run_loop(
        &self,
        run: RunId,
        loop_node: &orchestrator_core::Node,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        let NodeKind::Loop { body, input, gate, max_iters } = &loop_node.kind else {
            unreachable!("run_loop is only dispatched for a Loop node");
        };
        if !fold.started.contains(&loop_node.id) {
            self.append(run, JournalEvent::NodeStarted { node: loop_node.id.clone() }).await?;
        }

        let mut current_input = input.clone();
        let mut last_output = serde_json::Value::Null;
        let mut converged = false;
        let mut ran = 0usize;
        for i in 0..*max_iters {
            let path = format!("{}/{}", loop_node.id.0, i);
            let result = match body {
                MapBody::ModelCall { chain } => {
                    self.run_map_child_modelcall(run, &path, chain, &current_input, fold).await?
                }
                MapBody::Agent(agent_ref) => {
                    match self
                        .drive_agent(run, &NodeId(path.clone()), agent_ref, &current_input, &[], fold)
                        .await?
                    {
                        AgentStep::Completed(o) => Ok(o),
                        AgentStep::Failed(m) => Err(m),
                        AgentStep::Paused(reason) => return Ok(NodeExec::Paused { reason }),
                    }
                }
            };
            let output = match result {
                Ok(o) => o,
                Err(message) => {
                    let msg = format!("loop {:?} failed at iteration {i}: {message}", loop_node.id);
                    self.append(run, JournalEvent::NodeFailed {
                        node: loop_node.id.clone(),
                        error: msg.clone(),
                    }).await?;
                    return Ok(NodeExec::Failed { message: msg, output: None });
                }
            };
            ran = i + 1;
            last_output = output.clone();
            if gate.should_stop(&output) {
                converged = true;
                break;
            }
            current_input = output; // refine: feed this iteration's output forward
        }

        let out = serde_json::json!({
            "iterations": ran,
            "converged": converged,
            "output": last_output,
        });
        if !fold.completed.contains(&loop_node.id) {
            self.append(run, JournalEvent::NodeCompleted { node: loop_node.id.clone() }).await?;
        }
        Ok(NodeExec::Completed(out))
    }
```

Add the dispatch arm in `mod.rs` `run_node` (beside the `Map`/`Consolidate` arms):

```rust
            NodeKind::Loop { .. } => self.run_loop(run, node, fold).await,
```

Ensure `LoopGate`/`MapBody` are in scope where needed (fanout.rs already imports `MapBody`; `LoopGate` is only used in tests + core).

- [ ] **Step 4: Run to verify pass + full suite**

Run: `cargo test -p sensei-orchestrator > /tmp/t.log 2>&1; echo "EXIT=$?"; grep -E "test result:" /tmp/t.log | head -1`
Expected: `EXIT=0`.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator): run_loop — gated iteration + cap/finalize + body-failure (loop node)"
```

---

## Task 3: Refine thread (Agent body)

**Files:**
- Modify: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Failing test** — prove iteration `i>0` receives `i-1`'s output as input. Use an `Agent` body on the content-gated chain, whose adapter echoes `ok:{first user message}`; `render_input` turns the prior output JSON into the next user message, so iteration 1's output embeds iteration 0's output text.

```rust
/// Acceptance §9.3 — refine thread: iteration i>0 receives i-1's output as its
/// input. With an Agent body on the content-gated chain (echoes `ok:{input}`),
/// iteration 1's output text embeds iteration 0's output ("ok:start").
#[tokio::test]
async fn loop_threads_each_iterations_output_into_the_next() {
    let (gw, _c) = content_gated_gateway().await; // returns "ok:{first user message}"
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("L".into()),
            kind: NodeKind::Loop {
                body: MapBody::Agent(AgentRef("a".into())),
                input: serde_json::json!("start"),
                gate: LoopGate::TextContains("NEVER".into()),
                max_iters: 2,
            },
            deps: vec![],
        }],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(ToolRegistry::default()))
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{:?}", out.failed);
    let l = &out.outputs[&NodeId("L".into())];
    assert_eq!(l["iterations"], 2);
    let final_text = l["output"]["text"].as_str().unwrap();
    assert!(
        final_text.contains("ok:start"),
        "iteration 1's output embeds iteration 0's output (refine thread): {final_text}"
    );
}
```

> Implementer: confirm `content_gated_gateway` returns `format!("ok:{prompt}")` where `prompt` is the first user message (it does — see `test_support.rs`). The agent runtime's `render_input` serializes a non-string JSON input; iteration 0's input is the string `"start"` → user message `start` → output `ok:start`; iteration 1's input is iteration 0's output `{model,text:"ok:start"}` → `render_input` serializes it → the user message (and thus `ok:{…}`) contains `ok:start`. If the exact substring differs, assert on `"start"` appearing in the final output instead.

- [ ] **Step 2: Run to confirm failure, then pass** (this exercises existing `run_loop` — if the refine thread were broken, iteration 1 would not embed iteration 0's output).

Run: `cargo test -p sensei-orchestrator loop_threads_each_iterations_output 2>&1 | grep -E "test result|assertion"`
Expected: PASS (the thread is implemented in Task 2; this test pins it). If it fails, fix the `current_input = output` threading in `run_loop`.

- [ ] **Step 3: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "test(orchestrator): loop refine-thread (agent body) (loop node)"
```

---

## Task 4: Resume + determinism

**Files:**
- Modify: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Resume re-spends nothing (acceptance §9.5).** `Loop L → ModelCall n2 (hard-dep L)`. Seed with `failing_after_gateway(2)`: L runs `max_iters=2` (calls 1,2 succeed), then n2 fails (call 3) → no `RunCompleted`. Resume on a fresh `recording_gateway`: L's 2 iterations memo-hit (0 calls), n2 runs live (1 call). Assert the resume gateway saw exactly 1 call and n2's iteration effects were not re-recorded.

```rust
#[tokio::test]
async fn loop_resume_replays_completed_iterations_without_respending() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![
            Node {
                id: NodeId("L".into()),
                kind: NodeKind::Loop {
                    body: MapBody::ModelCall { chain: "c".into() },
                    input: serde_json::json!({ "prompt": "go" }),
                    gate: LoopGate::TextContains("STOP".into()), // never fires → cap at 2
                    max_iters: 2,
                },
                deps: vec![],
            },
            Node {
                id: NodeId("n2".into()),
                kind: model_call("c", "after"),
                deps: vec![Dep::hard("L")],
            },
        ],
    };
    // Seed: L's 2 iterations succeed (calls 1,2); n2 fails (call 3, failing_after 2).
    let (gw1, _c1) = failing_after_gateway(2).await;
    let o1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .run(run, &graph).await.expect("seed");
    assert!(o1.failed.is_some(), "n2 fails, L completed → no RunCompleted");
    // Resume: L replays (memo), n2 runs live → exactly 1 gateway call.
    let (gw2, calls2) = recording_gateway().await;
    let o2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .start(run, &graph).await.expect("resume");
    assert!(o2.failed.is_none(), "{:?}", o2.failed);
    assert_eq!(calls2.lock().unwrap().len(), 1, "resume re-spent only n2 (L's iterations memoized)");
}
```

- [ ] **Step 2: Determinism halt (acceptance §9.6).** Seed a partial run as above; rewrite iteration 0's body `EffectRecorded` `input_hash` in a fresh journal (mirror the slice-4 `changed_tool_input_on_resume…` / blackboard tamper pattern — match on `EffectRecorded { effect_id, .. }` where `effect_id == effect_id("L/0", 0, 0)` and replace `input_hash` with `"TAMPERED"`). Resume → L replays iteration 0 → memo mismatch → `DeterminismViolation { node: "L/0" }`, gateway untouched.

```rust
#[tokio::test]
async fn loop_resume_halts_on_a_tampered_iteration() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![
            Node {
                id: NodeId("L".into()),
                kind: NodeKind::Loop {
                    body: MapBody::ModelCall { chain: "c".into() },
                    input: serde_json::json!({ "prompt": "go" }),
                    gate: LoopGate::TextContains("STOP".into()),
                    max_iters: 2,
                },
                deps: vec![],
            },
            Node { id: NodeId("n2".into()), kind: model_call("c", "after"), deps: vec![Dep::hard("L")] },
        ],
    };
    let (gw1, _c1) = failing_after_gateway(2).await;
    Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1").run(run, &graph).await.expect("seed");
    // Tamper iteration 0's body effect input_hash.
    let target = effect_id("L/0", 0, 0);
    let tampered = InMemoryJournal::new();
    for (_, e) in journal.load(run).await.unwrap() {
        let e = match e {
            JournalEvent::EffectRecorded { effect_id, node, class, seq, output, observation, .. }
                if effect_id == target =>
                JournalEvent::EffectRecorded { effect_id, node, class, seq, output, observation, input_hash: "TAMPERED".into() },
            other => other,
        };
        tampered.append(run, e).await.unwrap();
    }
    let (gw2, calls2) = recording_gateway().await;
    let err = Executor::new(Arc::new(gw2), Arc::new(tampered.clone()), "v1")
        .start(run, &graph).await.expect_err("tampered iteration halts the resume");
    assert!(matches!(&err, OrchestratorError::DeterminismViolation { node, .. } if node.0 == "L/0"), "got {err:?}");
    assert_eq!(calls2.lock().unwrap().len(), 0, "a determinism violation never touches the gateway");
}
```

- [ ] **Step 3: Run + full suite + clippy**

Run: `cargo test -p sensei-orchestrator loop_resume > /tmp/t.log 2>&1; echo "EXIT=$?"; grep "test result" /tmp/t.log`
Run: `cargo clippy --workspace --all-targets -- -D warnings > /tmp/c.log 2>&1; echo "CLIPPY=$?"`
Expected: `EXIT=0`, `CLIPPY=0`.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "test(orchestrator): loop resume replay + determinism halt (loop node)"
```

---

## Task 5: Real reference-chain e2e + docs + memory

**Files:**
- Modify: `crates/orchestrator/src/executor/tests.rs`
- Modify: `docs/features/orchestrator/execution-graph.md`, `docs/features/orchestrator/README.md`

- [ ] **Step 1: e2e test** — a `Loop { body: ModelCall(chain: "research.bulk") }` driven through the REAL demo-catalog gateway (`demo_reference_gateway`), gate that never fires, `max_iters: 2` → falls over cloud entries to `llama3.1-local` each iteration, completes `converged: false` after 2 local calls. Proves the Loop drives the real reference chain with fallover.

```rust
#[tokio::test]
async fn loop_drives_the_real_reference_chain_each_iteration() {
    let (gw, calls) = demo_reference_gateway().await;
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("L".into()),
            kind: NodeKind::Loop {
                body: MapBody::ModelCall { chain: "research.bulk".into() },
                input: serde_json::json!({ "prompt": "iterate" }),
                gate: LoopGate::TextContains("NEVER".into()),
                max_iters: 2,
            },
            deps: vec![],
        }],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("e2e run");
    assert!(out.failed.is_none(), "{:?}", out.failed);
    let l = &out.outputs[&NodeId("L".into())];
    assert_eq!(l["iterations"], 2);
    assert_eq!(l["converged"], false);
    assert_eq!(l["output"]["model"], "llama3.1-local", "each iteration fell over to local: {l}");
    assert_eq!(calls.lock().unwrap().len(), 2, "2 iterations each hit the local adapter once");
}
```

- [ ] **Step 2: Full workspace gate**

Run: `cargo test --workspace > /tmp/ws.log 2>&1; echo "WS=$?"; grep -Eo "[0-9]+ passed" /tmp/ws.log | awk '{s+=$1} END{print s" passed"}'`
Run: `cargo clippy --workspace --all-targets -- -D warnings > /tmp/c.log 2>&1; echo "CLIPPY=$?"`
Expected: `WS=0`, `CLIPPY=0`.

- [ ] **Step 3: Docs.** In `execution-graph.md` (create/update the Loop row) and the orchestrator `README.md`, record `Loop` as implemented (walking skeleton): deterministic gate, `max_iters` backstop, `converged` finalize, refine thread, resume-replay. List deferred (Subgraph bodies, LLM gate agent, budget backstop, PlanDelta, nested caps).

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator): real reference-chain Loop e2e + docs (SP-1 Loop COMPLETE)"
```

---

## Notes for the implementer

- **Reuse, don't duplicate:** a `ModelCall` loop-body iteration is exactly `run_map_child_modelcall(run, "{loop}/{i}", chain, &input, fold)` — call it, don't re-implement the memo/record logic. An `Agent` body mirrors `run_map`'s Agent child arm.
- **Determinism rests on the pure gate + per-iteration paths.** The gate must stay a pure function of the body output (no clock/randomness), and each iteration must use the path `"{loop}/{i}"` so its effects get distinct ids. Do not journal gate decisions — they recompute on resume.
- **Never a bare fail on the cap:** hitting `max_iters` without a Stop completes with `converged: false`, NOT `NodeExec::Failed`. A body *failure* is the only fail path (§9.4). `max_iters == 0` completes degenerately (`iterations: 0, converged: false, output: null`) — harmless.
- **Fold-guard the Loop's own `NodeStarted`/`NodeCompleted`** (like `run_map`) so a resumed replay of a completed Loop does not re-journal them.
- **Exhaustive `match` on `NodeKind`:** adding the `Loop` variant (Task 1) breaks `run_node`'s match — add the dispatch arm (Task 2). The compiler flags any other exhaustive match to fix.
- Pre-commit runs `make lint`; `cargo fmt --all` before each commit. Branch: `feat/sp1-loop` off `develop`.
