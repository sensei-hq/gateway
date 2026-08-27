# SP-3 slice 5 — Coordinator + loops-of-graphs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend `NodeKind::Loop` to drive a **graph** body per iteration — `Subgraph` (fresh re-run) or `Expand` (plan+execute, the coordinator) — with a **gate-agent** option alongside the pure `LoopGate`. Reuse `drive_nested` + an extracted `drive_expand_with`; the slice-3 caps backstop a loop-of-expands for free.

**Architecture:** `LoopBody{ModelCall,Agent,Subgraph,Expand{planner}}` + `GateSpec{Pure(LoopGate),Agent{agent,stop_when}}`. Each iteration drives its body at `"{loop}/{i}"`; `Expand` iterations re-plan over the refine-threaded output and journal `PlanExpanded{"{loop}/{i}"}` (caps charged). The gate-agent runs at `"{loop}/{i}/__gate__"` (journaled answer + pure `stop_when`). Resume replays iterations + gate decisions and stops at the same iteration. This is the FINAL SP-3 slice.

**Tech Stack:** Rust, `orchestrator-core` (types) + `orchestrator` (executor) + `orchestrator-store` (`InMemoryJournal`), `async_trait`, `tokio`, `serde_json`.

**Design spec:** `docs/superpowers/specs/2026-08-14-sp3-coordinator-loops-of-graphs-design.md` · **Overview:** `docs/superpowers/orchestrator-overview.md`

**Conventions (ops memory):** `cargo fmt --all` before every commit (pre-commit hook = fmt-check + `clippy -D warnings`, runs **no** tests). Always run `cargo test` yourself; read the **real** exit code (never a piped `| tail`/`grep`; multi-filter needs `-- a b`). `make clean` between stages if disk is tight (target/ ≈ 11G). `NodeId` is private in `graph.rs` — import from `crate::ids`; `AgentRef` = `crate::registry::AgentRef`.

---

## File Structure

- `crates/orchestrator/src/executor/expand.rs` **(modify)** — key `expand_failed`/`drive_planner_agent` by `&NodeId`; extract `drive_expand_with`; `run_expand` delegates.
- `crates/orchestrator-core/src/graph.rs` **(modify)** — `LoopBody`, `GateSpec`; `NodeKind::Loop` fields; `validate_dag` recursion into `LoopBody::Subgraph`.
- `crates/orchestrator-core/src/lib.rs` **(modify)** — re-export `LoopBody`, `GateSpec`.
- `crates/orchestrator/src/executor/fanout.rs` **(modify)** — `run_loop` body dispatch (Subgraph/Expand) + gate (Pure/Agent).
- `crates/orchestrator/src/executor/{mod.rs,plan.rs(core),tests.rs}` + `graph.rs` tests **(modify)** — the ~12 `NodeKind::Loop{…}` construction-site migration.

---

## Task 1: Extract `drive_expand_with` (path-keyed; behavior-preserving)

**Files:** Modify `crates/orchestrator/src/executor/expand.rs`.

**Note:** `run_expand` currently keys everything by `node` (`node.id`, `expand_failed(node)`, `drive_planner_agent(node)`). To reuse the Expand pipeline at an arbitrary path (a Loop iteration `"{loop}/{i}"`), key it by a `&NodeId`. This is a pure refactor — `run_expand`'s behavior is byte-identical.

- [ ] **Step 1: Re-key `expand_failed` and `drive_planner_agent` by `&NodeId`**

In `expand.rs`, change `expand_failed`'s signature from `node: &Node` to `node_id: &NodeId` (it only uses `node.id`):

```rust
    async fn expand_failed(
        &self,
        run: RunId,
        node_id: &NodeId,
        message: String,
    ) -> Result<NodeExec, OrchestratorError> {
        self.append(run, JournalEvent::NodeFailed { node: node_id.clone(), error: message.clone() }).await?;
        Ok(NodeExec::Failed { message, output: None })
    }
```

Change `drive_planner_agent`'s signature from `node: &Node` to `node_id: &NodeId`, and inside it replace every `node.id.0` with `node_id.0`, every `node.id` with `node_id.clone()`, and every `self.expand_failed(run, node, …)` with `self.expand_failed(run, node_id, …)`. (The plan-node path becomes `format!("{}/__plan__", node_id.0)`.)

- [ ] **Step 2: Extract `drive_expand_with`**

Add to `impl Executor` in `expand.rs`:

```rust
    /// The Expand pipeline keyed by an arbitrary `path` (a node id OR a Loop iteration
    /// path `"{loop}/{i}"`): resume via the fold's expansion/selection at `path`, else
    /// produce (Injected/Agent/Select) → `feasible` → cap-check → journal
    /// `PlanExpanded{node: path}` → `drive_nested`. `run_expand` and a Loop-`Expand`
    /// body iteration share this.
    pub(super) async fn drive_expand_with(
        &self,
        run: RunId,
        path: &NodeId,
        input: &serde_json::Value,
        planner: &orchestrator_core::PlannerRef,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        let g = match fold.expansions.get(path) {
            Some(journaled) => journaled.clone(),
            None => {
                let produced = match planner {
                    orchestrator_core::PlannerRef::Injected => {
                        let Some(p) = &self.planner else {
                            return self.expand_failed(run, path, format!("expand {}: no planner wired", path.0)).await;
                        };
                        match p.plan(input).await {
                            Ok(graph) => PlannedGraph { graph, node_plans: std::collections::HashMap::new() },
                            Err(e) => return self.expand_failed(run, path, format!("expand {} planner failed: {e}", path.0)).await,
                        }
                    }
                    orchestrator_core::PlannerRef::Agent(agent_ref) => {
                        match self.drive_planner_agent(run, path, agent_ref, input, fold).await? {
                            PlanOutcome::Plan(p) => p,
                            PlanOutcome::Terminal(ne) => return Ok(ne),
                        }
                    }
                    orchestrator_core::PlannerRef::Select => {
                        let agent = match fold.selections.get(path) {
                            Some(a) => a.clone(),
                            None => {
                                let candidates = self.planner_candidates();
                                if candidates.is_empty() {
                                    return self.expand_failed(run, path, format!("expand {}: no planner agents (area==planning)", path.0)).await;
                                }
                                let Some(selector) = &self.selector else {
                                    return self.expand_failed(run, path, format!("expand {}: Select planner but no selector wired", path.0)).await;
                                };
                                let a = match selector.select(input, &candidates).await {
                                    Ok(a) => a,
                                    Err(e) => return self.expand_failed(run, path, format!("expand {} selector: {e}", path.0)).await,
                                };
                                if !candidates.contains(&a) {
                                    return self.expand_failed(run, path, format!("expand {} selector picked non-candidate {}", path.0, a.0)).await;
                                }
                                self.append(run, JournalEvent::PlannerSelected { node: path.clone(), agent: a.clone() }).await?;
                                a
                            }
                        };
                        match self.drive_planner_agent(run, path, &agent, input, fold).await? {
                            PlanOutcome::Plan(p) => p,
                            PlanOutcome::Terminal(ne) => return Ok(ne),
                        }
                    }
                };
                if let Err(errs) = orchestrator_core::feasible(&produced, &self.registry, self.max_nodes) {
                    return self.expand_failed(run, path, format!("expand {} infeasible plan: {errs:?}", path.0)).await;
                }
                self.check_expansion_budget(&produced.graph)?;
                self.append(run, JournalEvent::PlanExpanded {
                    node: path.clone(), subgraph: produced.graph.clone(), node_plans: produced.node_plans,
                }).await?;
                produced.graph
            }
        };
        self.drive_nested(run, "expand", &path.0, &g, fold).await
    }
```

- [ ] **Step 3: `run_expand` delegates**

Replace the entire body of `run_expand` (keep its signature + doc) with:

```rust
        let NodeKind::Expand { input, planner } = &node.kind else {
            unreachable!("run_expand on non-Expand node");
        };
        self.drive_expand_with(run, &node.id, input, planner, fold).await
```

- [ ] **Step 4: Fix the other `expand_failed`/`drive_planner_agent` callers**

Grep `expand_failed(` and `drive_planner_agent(` in `expand.rs` — every caller now passes `&node.id` (or the `path`) instead of `node`/`&Node`. (After Steps 2–3 the only callers are inside `drive_expand_with`, already using `path`, and `drive_planner_agent`'s internal `expand_failed` calls, already using `node_id`.)

- [ ] **Step 5: Run the slice-4A/4B suites (behavior-preserving)**

Run: `cargo test -p sensei-orchestrator -- expand_ planner_agent_ select_ journaled_planner_agent unresolvable_planner_agent llm_planner_selector` — Expected: ALL pass unchanged (run_expand now delegates; behavior byte-identical). Verify real exit 0.

- [ ] **Step 6: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/expand.rs
git commit -m "refactor(orchestrator): SP-3 s5 (1/5) — extract path-keyed drive_expand_with (shared by run_expand + Loop-Expand)"
```

---

## Task 2: `LoopBody` + `GateSpec` types + migration + Subgraph body + Pure gate

**Files:** Modify `crates/orchestrator-core/src/graph.rs`, `lib.rs`; `crates/orchestrator/src/executor/fanout.rs`; the ~12 `NodeKind::Loop{…}` construction sites (`graph.rs` tests, `plan.rs`, `executor/{mod.rs,fanout.rs,tests.rs}`).

- [ ] **Step 1: Add the types + migrate the `Loop` variant**

In `crates/orchestrator-core/src/graph.rs`, add:

```rust
/// What a `Loop` runs per iteration (SP-3 s5). Leaf variants mirror `MapBody`; the
/// two graph variants drive a nested graph per iteration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoopBody {
    ModelCall { chain: String },
    Agent(crate::registry::AgentRef),
    Subgraph(Box<Graph>),
    Expand { planner: PlannerRef },
}

/// A `Loop`'s stop decision (SP-3 s5). `Pure` = the SP-1 pure predicate (no journaling);
/// `Agent` = a gate-agent over the iteration output, then a pure `stop_when` over the
/// agent's answer (the agent turn is journaled ⇒ resume replays it).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GateSpec {
    Pure(LoopGate),
    Agent { agent: crate::registry::AgentRef, stop_when: LoopGate },
}
```

Change the `NodeKind::Loop` variant fields from `body: MapBody, … gate: LoopGate` to `body: LoopBody, … gate: GateSpec`. Re-export `LoopBody, GateSpec` from `lib.rs`.

- [ ] **Step 2: Migrate the ~12 `NodeKind::Loop{…}` construction sites**

Grep `NodeKind::Loop {` across `crates` (12 sites: 2 in graph.rs, 1 plan.rs, 1 fanout.rs, 1 mod.rs, 7 tests.rs). At each, mechanically change `body: MapBody::ModelCall{chain} → body: LoopBody::ModelCall{chain}`, `body: MapBody::Agent(a) → body: LoopBody::Agent(a)`, and `gate: <LoopGate expr> → gate: GateSpec::Pure(<LoopGate expr>)`. Behavior-identical.

- [ ] **Step 3: `validate_dag` recurses into a `LoopBody::Subgraph`**

In `graph.rs::validate_dag`, add (alongside the Subgraph/Branch recursion): for every `NodeKind::Loop { body: LoopBody::Subgraph(g), .. }`, `g.validate_dag()?` (a `LoopBody::Expand` has no static graph — no recursion).

- [ ] **Step 4: Migrate `run_loop` to `LoopBody`/`GateSpec` + add the Subgraph body arm**

In `crates/orchestrator/src/executor/fanout.rs`, `run_loop`'s destructure is now `NodeKind::Loop { body, input, gate, max_iters }` (body: `&LoopBody`, gate: `&GateSpec`). Replace the per-iteration body `match body` with:

```rust
            let path = format!("{}/{}", loop_node.id.0, i);
            let output_res: Result<serde_json::Value, String> = match body {
                LoopBody::ModelCall { chain } => {
                    self.run_map_child_modelcall(run, &path, chain, &current_input, fold).await?
                }
                LoopBody::Agent(agent_ref) => match self
                    .drive_agent(run, &NodeId(path.clone()), agent_ref, &current_input, &[], fold, None).await?
                {
                    AgentStep::Completed(o) => Ok(o),
                    AgentStep::Failed(m) => Err(m),
                    AgentStep::Paused(reason) => return Ok(NodeExec::Paused { reason }),
                },
                LoopBody::Subgraph(g) => match self.drive_nested(run, "loop", &path, g, fold).await? {
                    NodeExec::Completed(o) => Ok(o),
                    NodeExec::Failed { message, .. } => Err(message),
                    NodeExec::Paused { reason } => return Ok(NodeExec::Paused { reason }),
                },
                // TEMPORARY (Task 2): Expand body lands in Task 3 (uses drive_expand_with).
                LoopBody::Expand { .. } => unreachable!("Loop Expand body implemented in slice-5 Task 3"),
            };
            let output = match output_res {
                Ok(o) => o,
                Err(message) => {
                    // existing SP-1 block, unchanged: a body-iteration failure fails the Loop.
                    let msg = format!("loop {:?} failed at iteration {i}: {message}", loop_node.id);
                    self.append(run, JournalEvent::NodeFailed { node: loop_node.id.clone(), error: msg.clone() }).await?;
                    return Ok(NodeExec::Failed { message: msg, output: None });
                }
            };
```

`drive_nested` takes `(run, kind_label, prefix, graph, fold)` — `prefix = &path`, `kind_label = "loop"`; `g` is `&Box<Graph>` (deref-coerces to `&Graph`).

Replace the gate check `if gate.should_stop(&output)` with an **inline match** (a helper returning `bool` can't signal a gate-agent pause/failure — the Agent arm must be able to `return` from `run_loop`). Task 2 implements the `Pure` arm and stubs `Agent` (Task 4 fills it):

```rust
            let stop = match gate {
                GateSpec::Pure(g) => g.should_stop(&output),
                // TEMPORARY (Task 2): gate-agent lands in Task 4 (async drive + early-return
                // for its pause/failure, which is why the gate is inline, not a bool helper).
                GateSpec::Agent { .. } => unreachable!("gate-agent implemented in slice-5 Task 4"),
            };
            if stop {
                converged = true;
                break;
            }
```

Add the refine arm for the graph bodies (in the existing `current_input = match body {…}`):

```rust
            current_input = match body {
                LoopBody::ModelCall { .. } => serde_json::json!({ "prompt": text }),
                LoopBody::Agent(_) => text,
                // Subgraph body does not thread (fresh re-run); Expand threads the whole output.
                LoopBody::Subgraph(_) => current_input,
                LoopBody::Expand { .. } => output.clone(),
            };
```

(where `text` is the existing `output.get("text").cloned().unwrap_or_else(|| output.clone())`; for `Expand` we thread the whole `output` — the sink map — as the next planner input.)

- [ ] **Step 5: Write the migration + Subgraph-body tests**

In `crates/orchestrator/src/executor/tests.rs`, add:

```rust
/// AC2 — a Loop over a Subgraph body drives the graph fresh each iteration; a pure gate
/// cannot match the nested sink map (§4.3), so it runs max_iters → best-effort
/// {iterations, converged:false, output: <sink map>}. Graph-body convergence is AC5/AC10.
#[tokio::test]
async fn loop_over_a_subgraph_body_iterates_and_stops() {
    // Subgraph: a single node "s1" whose ModelCall output {model,text} carries a "done"
    // marker so a FieldTrue-ish gate can stop. Use a recording gateway (deterministic body).
    let (gateway, _c) = recording_gateway().await;
    let inner = Graph { nodes: vec![mc("s1", None)] };
    let loop_node = Node {
        id: NodeId("lp".into()),
        kind: NodeKind::Loop {
            body: orchestrator_core::LoopBody::Subgraph(Box::new(inner)),
            input: serde_json::json!({}),
            gate: orchestrator_core::GateSpec::Pure(LoopGate::TextContains("zzz-never".into())),
            max_iters: 2,
        },
        deps: vec![],
    };
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph_of(vec![loop_node])).await.expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    let o = &out.outputs[&NodeId("lp".into())];
    assert_eq!(o["converged"], false, "gate never matched → best-effort");
    assert_eq!(o["iterations"], 2, "ran max_iters");
    // inner nodes journaled under "lp/0/…" and "lp/1/…"
}
```

(Use the existing helpers `mc`, `recording_gateway`, `InMemoryJournal`, and a `graph_of(nodes)`/inline `Graph{nodes}` — match the file's idiom. If `graph_of` doesn't exist, inline `Graph { nodes: vec![loop_node] }`.) Do **not** add a pure-gate "converges" test for a Subgraph body: a pure `LoopGate` cannot match a nested sink map `{sink_id: {model, text}}` (§4.3), so any such test would have to seed a production-impossible bare-`true` sink output and hand-compute an effect hash — brittle and vacuous. Graph-body convergence is body-agnostic (the `converged=true`/`break` path is already covered by `loop_stops_when_the_gate_fires`) and its semantic form is the **gate-agent's** job, exercised by AC5 (`loop_gate_agent_decides_stop`, Task 4) and AC10 (coordinator, Task 5). Instead, in `crates/orchestrator-core/src/graph.rs` `mod tests`, add `validate_dag_rejects_a_cycle_in_a_loop_subgraph_body` (model on `validate_dag_recurses_into_subgraphs`, reusing its 2-node a↔b `nested_cycle`): wrap it in `NodeKind::Loop { body: LoopBody::Subgraph(Box::new(nested_cycle)), input: json!({}), gate: GateSpec::Pure(LoopGate::TextContains("x".into())), max_iters: 1 }` → assert `validate_dag()` returns `Err(InvalidGraph(_))`. This covers the new `LoopBody::Subgraph` recursion.

- [ ] **Step 6: Run the migrated Loop suite + the new tests**

Run: `cargo test -p sensei-orchestrator -- loop_ ` — Expected: all existing `loop_*` tests (migrated) + the two new Subgraph-body tests PASS. Verify real exit 0. Also `cargo test -p sensei-orchestrator-core --lib` (validate_dag recursion).

- [ ] **Step 7: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-core/src/graph.rs crates/orchestrator-core/src/lib.rs \
        crates/orchestrator-core/src/plan.rs crates/orchestrator/src/executor/fanout.rs \
        crates/orchestrator/src/executor/mod.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-3 s5 (2/5) — LoopBody/GateSpec + migration + Subgraph body (fresh re-run) + validate_dag recursion"
```

---

## Task 3: Expand body (the coordinator core) + refine

**Files:** Modify `crates/orchestrator/src/executor/fanout.rs`, `tests.rs`.

- [ ] **Step 1: Replace the Expand-body stub with `drive_expand_with`**

In `run_loop`'s body `match`, replace the `LoopBody::Expand { .. } => unreachable!(...)` arm with:

```rust
                LoopBody::Expand { planner } => {
                    match self.drive_expand_with(run, &NodeId(path.clone()), &current_input, planner, fold).await? {
                        NodeExec::Completed(o) => Ok(o),
                        NodeExec::Failed { message, .. } => Err(message),
                        NodeExec::Paused { reason } => return Ok(NodeExec::Paused { reason }),
                    }
                }
```

(The refine arm `LoopBody::Expand { .. } => output.clone()` was already added in Task 2 Step 4 — each iteration re-plans over the prior output.)

- [ ] **Step 2: Write the Expand-body (coordinator refine) + failure/pause/caps tests**

In `tests.rs`, append:

```rust
/// AC3 — a Loop over an Expand body: each iteration plans+executes; the refine-thread
/// feeds iteration i's output into iteration i+1's planner input. A FixedPlanner emits a
/// single-ModelCall plan; assert the loop runs max_iters and the 2nd iteration's planner
/// saw the 1st iteration's output.
#[tokio::test]
async fn loop_over_an_expand_body_refines_across_iterations() {
    // Planner (FixedPlanner) emits {graph:{nodes:[n1 ModelCall]}}. Each iteration:
    // drive_expand_with("{lp}/{i}", current_input, planner) → plan → run n1 → sink map;
    // refine: current_input = that sink map (fed to iteration i+1's planner).
    let plan = Graph { nodes: vec![mc("n1", None)] };
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_planner(Arc::new(FixedPlanner(plan)));
    let loop_node = Node {
        id: NodeId("lp".into()),
        kind: NodeKind::Loop {
            body: orchestrator_core::LoopBody::Expand { planner: orchestrator_core::PlannerRef::Injected },
            input: serde_json::json!({ "goal": "g" }),
            gate: orchestrator_core::GateSpec::Pure(LoopGate::TextContains("zzz-never".into())),
            max_iters: 2,
        },
        deps: vec![],
    };
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &Graph { nodes: vec![loop_node] }).await.expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    let o = &out.outputs[&NodeId("lp".into())];
    assert_eq!(o["iterations"], 2);
    // Each iteration journaled its own PlanExpanded under "lp/0" and "lp/1".
    // (The refine is exercised structurally: iteration 1's drive_expand_with input = iter 0's sink map.)
}
```

Also add:
- `loop_expand_body_iteration_failure_fails_the_loop` (AC7): a `FixedPlanner` whose plan's node fails on the gateway (`failing_after_gateway(0)`) → the Loop is `Failed`.
- `loop_of_expands_respects_max_expansions_cap` (AC8): a Loop-Expand with `max_iters=3` under `.with_max_expansions(1)` → the 2nd iteration's expansion breaches the cap → `GlobalCapExceeded` (hard `Err`). (`exec.run(...)` returns `Err` — assert `matches!(res, Err(OrchestratorError::GlobalCapExceeded{..}))`.)

- [ ] **Step 3: Run the Expand-body tests + regressions**

Run: `cargo test -p sensei-orchestrator -- loop_ expand_` — Expected: the new Expand-body tests + all Task-2 loop tests + slice-4A expand tests PASS. Verify real exit 0.

- [ ] **Step 4: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/fanout.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-3 s5 (3/5) — Loop Expand body (plan+execute per iteration, input refine); caps compose"
```

---

## Task 4: The gate-agent

**Files:** Modify `crates/orchestrator/src/executor/fanout.rs`, `tests.rs`.

- [ ] **Step 1: Implement the `GateSpec::Agent` branch in the inline gate `match`**

Replace the `GateSpec::Agent { .. } => unreachable!(...)` arm (in `run_loop`'s inline `let stop = match gate {…}`) with a drive of the gate-agent. Because the arm is inline in `run_loop`, its pause/failure can `return` directly:

```rust
                GateSpec::Agent { agent, stop_when } => {
                    let gate_path = NodeId(format!("{}/__gate__", path));
                    match self.drive_agent(run, &gate_path, agent, &output, &[], fold, None).await? {
                        // The gate-agent's answer is a journaled Pure effect; a pure predicate
                        // over it decides stop (resume replays the same decision).
                        AgentStep::Completed(ans) => stop_when.should_stop(&ans),
                        // A gate-agent failure fails the Loop (like a body-iteration failure).
                        AgentStep::Failed(m) => {
                            let msg = format!("loop {:?} gate agent failed at iteration {i}: {m}", loop_node.id);
                            self.append(run, JournalEvent::NodeFailed { node: loop_node.id.clone(), error: msg.clone() }).await?;
                            return Ok(NodeExec::Failed { message: msg, output: None });
                        }
                        // A gate-agent pause (in-doubt Mutation / quota) pauses the Loop.
                        AgentStep::Paused(reason) => return Ok(NodeExec::Paused { reason }),
                    }
                }
```

(The arm yields a `bool` on `Completed` and early-returns on Failed/Paused — matching the body-drive handling. `run`, `path`, `fold`, `loop_node`, `i` are all in scope inside `run_loop`'s iteration loop.)

- [ ] **Step 2: Write the gate-agent tests (AC5, AC6)**

In `tests.rs`, append:
- `loop_gate_agent_decides_stop` (AC5): a Loop (leaf `ModelCall` body for simplicity) with `GateSpec::Agent { agent: <a scripted agent that answers "STOP" on iteration 2>, stop_when: LoopGate::TextContains("STOP") }`. Use a `scripted_gateway` whose responses drive: iter0 body, iter0 gate-agent ("keep going"), iter1 body, iter1 gate-agent ("STOP"). Assert `converged:true`, `iterations:2`, and the gate-agent turns journaled under `"lp/0/__gate__"`/`"lp/1/__gate__"`.
- `loop_gate_agent_decision_replays_on_resume` (AC6): run to a partial completion, resume, assert the gate-agent decision replays from the memo (gateway not re-called for the gate turns) and the Loop stops at the same iteration. (Mirror the 4A/4B resume-truncation idiom; mutation-verify if cheap.)

- [ ] **Step 3: Run the gate-agent tests + regressions**

Run: `cargo test -p sensei-orchestrator -- loop_gate_agent loop_ ` — Expected: PASS. Verify real exit 0.

- [ ] **Step 4: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/fanout.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-3 s5 (4/5) — gate-agent (journaled Continue|Stop over the iteration output + pure stop_when)"
```

---

## Task 5: End-to-end coordinator + full-suite gate

**Files:** Modify `crates/orchestrator/src/executor/tests.rs`.

- [ ] **Step 1: Write the coordinator e2e (AC10)**

`coordinator_loop_expand_body_with_gate_agent_converges`: a `Loop{ body: Expand{planner: Agent(<a planner agent>)}, gate: Agent{ agent: <a gate agent>, stop_when: TextContains("DONE") } }`, driven through a `scripted_gateway` whose sequence realizes: iter0 planner turn → plan JSON → the plan's node → gate-agent ("not yet"); iter1 planner turn → plan → node → gate-agent ("DONE"). Assert the loop converges (`converged:true`), `on_plan_expanded` fired per iteration (a hook counter == 2), and the final output carries the converged result. (Reuse the 4A/4B planner-agent + gateway-scripting helpers; build the exact response sequence carefully — the Select/Agent planner path + the spliced plan node + the gate-agent each consume one scripted response per iteration.)

- [ ] **Step 2: Run the e2e**

Run: `cargo test -p sensei-orchestrator coordinator_loop_expand_body_with_gate_agent_converges` — Expected: PASS (real exit 0). If the scripted sequence is off, debug + fix to a REAL passing e2e and report the correction.

- [ ] **Step 3: Full-workspace gate (AC11 + additive)**

Run: `cargo test --workspace` — read the REAL exit code directly (not piped). Report exact pass/fail totals; confirm 0 failures (prior baseline ~1022 + the s5 additions).

- [ ] **Step 4: Lint gate**

Run: `cargo fmt --all --check` (exit 0) + `cargo clippy --workspace --all-targets -- -D warnings` (exit 0).

- [ ] **Step 5: Commit (do NOT push — coordinator pushes after the final review)**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): SP-3 s5 (5/5) — coordinator e2e (Loop Expand body + gate-agent converges); full-suite green"
```

---

## Acceptance Criteria → Task map (self-review)

| Spec AC | Task | Test |
|---|---|---|
| 1 migration behavior-preserving | 2 | migrated `loop_*` suite green |
| 2 Subgraph body iterates (best-effort; convergence → AC5/AC10) | 2 | `loop_over_a_subgraph_body_iterates_and_stops`, `validate_dag_rejects_a_cycle_in_a_loop_subgraph_body` |
| 3 Expand body refines | 3 | `loop_over_an_expand_body_refines_across_iterations` |
| 4 `drive_expand_with` behavior-preserving | 1 | slice-4A `expand_*` + 4B `select_*` green |
| 5 gate-agent decides stop | 4 | `loop_gate_agent_decides_stop` |
| 6 resume replays gate decision | 4 | `loop_gate_agent_decision_replays_on_resume` |
| 7 failure / pause propagation | 3 | `loop_expand_body_iteration_failure_fails_the_loop` (+ a Subgraph in-doubt-pause case if cheap) |
| 8 caps compose | 3 | `loop_of_expands_respects_max_expansions_cap` |
| 9 cap-without-stop → best-effort | 2 | `loop_over_a_subgraph_body_iterates_and_stops` (converged:false at max_iters) |
| 10 e2e coordinator | 5 | `coordinator_loop_expand_body_with_gate_agent_converges` |
| 11 additive | 5 | `cargo test --workspace` green |

**Coverage note (flag, don't silently drop):** AC7's *pause* arm (an in-doubt Mutation inside a Subgraph/Expand loop body → Loop `Paused`) has no dedicated test above — add `loop_subgraph_body_pause_pauses_the_loop` in Task 3 if the in-doubt-mutation fixture (see `an_in_doubt_mutation_in_a_subgraph_pauses_the_run`) is cheaply reusable; if skipped, say so in the report.

---

## Post-implementation

- Update `docs/features/orchestrator/execution-graph.md` (Loop now drives graph bodies + gate-agent) + flip the overview index `SP-3 s5` row to ✅ done.
- **SP-3 is COMPLETE** — update the memory topic file + `MEMORY.md`: the hierarchical executor (Subgraph/Branch/Expand/Planner/Selector/Coordinator-loops) is done; next phases = SP-4 (permission/effect enforcement + sandbox), SP-DATA (PostgresJournal + durable scheduler + budget model), SP-6 (HITL).
- Carry-forward deferred (spec §6): budget-primary-backstop + reserved synthesis budget + finalize-synthesize; replan-on-failure; Subgraph-body cross-iteration blackboard threading; tier-downgrade-on-resume replan.
