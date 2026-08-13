# SP-3 slice 2 — Branch node Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `NodeKind::Branch { on, arms: Vec<(BranchCond, Graph)>, default: Graph }` — a deterministic conditional that tests predecessor `on`'s memoized output with a pure `BranchCond` and runs the first matching arm (else `default`) as a nested graph, with no branch-decision journaling.

**Architecture:** `BranchCond::matches` mirrors `LoopGate::should_stop` (pure predicate). `run_branch` reads `prior_outputs[on]` (like `run_consolidate` reads `over`), selects an arm, and drives it as a nested graph under `{branch}/{label}/…` by reusing slice-1's `namespace_graph`/`sink_outputs`/drive/`NodeExec`-mapping. The decision is pure over `on`'s memoized output, so resume recomputes the same arm — the decision is never journaled.

**Tech Stack:** Rust workspace (`orchestrator-core`, `orchestrator`); `serde`/`serde_json`; `cargo test`/`clippy`. Spec: `docs/superpowers/specs/2026-08-12-sp3-branch-node-design.md`.

**House rules (every task):**
- Pre-commit = `make lint` (fmt-check + workspace `clippy -D warnings`), NO tests → always `cargo fmt --all` then `cargo test --workspace` before committing.
- Verify the REAL exit code (never a piped `| tail`); single positional test filter.
- Commit a fix BEFORE any `git checkout`-based mutation-verify.
- Branch `feat/sp3-branch-node` (created; spec committed at `37ca7a1`). Crate `-p` names: `sensei-orchestrator-core`, `sensei-orchestrator`.

**Verified anchors:**
- `LoopGate` (graph.rs): `should_stop(&self, output) -> bool` pure predicate — the style to mirror.
- `NodeKind` enum (graph.rs); `Dep { on: NodeId, kind: EdgeKind }`; `EdgeKind::Hard`/`Soft` (use `matches!(d.kind, EdgeKind::Hard)` — do NOT assume `EdgeKind: PartialEq`).
- `validate_dag` (graph.rs) sections: distinct ids (builds `ids: HashSet<&NodeId>`); deps-reference-declared; per-kind sanity (Loop max_iters, `// 2c.` Subgraph recursion); Kahn acyclic.
- `run_node` (mod.rs) dispatch ends: `NodeKind::Loop { .. } => self.run_loop(...); NodeKind::Subgraph { .. } => self.run_subgraph(run, node, fold).await,`; `NodeKind::Consolidate { .. } => self.run_consolidate(run, node, prior_outputs, fold).await` — `run_node` threads `prior_outputs: &HashMap<NodeId, serde_json::Value>`.
- `subgraph.rs`: private `fn namespace_graph(prefix, graph) -> Graph` (line 15) and `fn sink_outputs(graph, prefix, outputs) -> Value` (line 50); `run_subgraph` does the depth cap (`node.id.0.matches('/').count() + 1 > self.max_depth` → `GlobalCapExceeded`), `Box::pin(self.drive(...))`, and paused/failed/`Completed(sink_outputs)` mapping.
- `NodeExec::{ Completed(Value), Failed { message, output }, Paused { reason } }`; `RunOutcome { failed: Option<(NodeId,String)>, skipped, outputs, paused }`.
- A `ModelCall` node's output = `{ "model":…, "text": <content>, … }`; `recording_gateway`'s canned content = `"canned-response"` (so a ModelCall on chain `"c"` → `outputs[node]["text"] == "canned-response"`). `failing_after_gateway(0)` fails immediately. Helpers `mc(id, dep)`, `subgraph_node(id, inner)`, `agent_registry`, `agent_node`, `InMemoryJournal` exist in tests.rs.
- error.rs: `InvalidGraph(String)` (22), `MapChildPaused` (74), `GlobalCapExceeded` (76) — add `BranchInputMissing` near 76.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `crates/orchestrator-core/src/graph.rs` | node kinds + validation | `BranchCond` + `matches`; `NodeKind::Branch`; `validate_dag` Branch recursion + `on` checks. |
| `crates/orchestrator-core/src/lib.rs` | exports | export `BranchCond`. |
| `crates/orchestrator-core/src/error.rs` | errors | `BranchInputMissing { branch, on }`. |
| `crates/orchestrator/src/executor/subgraph.rs` | shared helpers | promote `namespace_graph`/`sink_outputs` to `pub(super)`. |
| `crates/orchestrator/src/executor/branch.rs` (new) | branch execution | `run_branch`. |
| `crates/orchestrator/src/executor/mod.rs` | dispatch | `mod branch;`; `NodeKind::Branch` arm. |
| `crates/orchestrator/src/executor/tests.rs` | tests | selection, only-selected-runs, validate, resume, propagation, e2e. |
| `docs/features/orchestrator/execution-graph.md` | feature doc | Branch note. |

---

## Task 1: `BranchCond` + `matches` (core, additive)

Additive predicate type — no `NodeKind` change, zero ripple.

**Files:** Modify `crates/orchestrator-core/src/graph.rs` (type + impl + tests); `crates/orchestrator-core/src/lib.rs` (export).

- [ ] **Step 1: Write the failing test** (add to `graph.rs` tests):
```rust
    #[test]
    fn branch_cond_matches_each_variant() {
        let out = serde_json::json!({ "status": "b", "done": true, "text": "hello world" });
        assert!(BranchCond::FieldEquals("status".into(), serde_json::json!("b")).matches(&out));
        assert!(!BranchCond::FieldEquals("status".into(), serde_json::json!("a")).matches(&out));
        assert!(BranchCond::FieldTrue("done".into()).matches(&out));
        assert!(!BranchCond::FieldTrue("missing".into()).matches(&out));
        assert!(!BranchCond::FieldTrue("status".into()).matches(&out)); // "b" is not `true`
        assert!(BranchCond::TextContains("world".into()).matches(&out));
        assert!(!BranchCond::TextContains("zzz".into()).matches(&out));
        // TextContains only inspects `text`.
        assert!(!BranchCond::TextContains("b".into()).matches(&serde_json::json!({ "status": "b" })));
    }
```

- [ ] **Step 2: Run to confirm failure** — `cargo test -p sensei-orchestrator-core branch_cond_matches_each_variant` → FAIL to compile (`BranchCond` not found). RED.

- [ ] **Step 3: Add the type + impl** (graph.rs, near `LoopGate`):
```rust
/// A pure predicate over a predecessor node's output, selecting a `Branch` arm
/// (mirrors `LoopGate`). Evaluated in arm order; first match wins.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BranchCond {
    /// `output[field] == value` (strict JSON equality) — switch on a discriminant.
    FieldEquals(String, serde_json::Value),
    /// `output[field] == true` (strict JSON `true`).
    FieldTrue(String),
    /// `output["text"]` contains this substring.
    TextContains(String),
}

impl BranchCond {
    /// Whether `output` satisfies this condition.
    pub fn matches(&self, output: &serde_json::Value) -> bool {
        match self {
            BranchCond::FieldEquals(f, v) => output.get(f) == Some(v),
            BranchCond::FieldTrue(f) => output.get(f) == Some(&serde_json::Value::Bool(true)),
            BranchCond::TextContains(s) => output
                .get("text")
                .and_then(|v| v.as_str())
                .is_some_and(|t| t.contains(s.as_str())),
        }
    }
}
```

- [ ] **Step 4: Export** — in `lib.rs`, add `BranchCond` to `pub use graph::{…}` (keep alphabetical: it sorts before `ChainBinding`… actually it's in the `graph` re-export line `pub use graph::{Aggregation, Dep, EdgeKind, Graph, LoopGate, MapBody, Node, NodeKind};` → add `BranchCond` after `Aggregation`).

- [ ] **Step 5: Run green + commit** — `cargo test -p sensei-orchestrator-core branch_cond_matches_each_variant` PASS; `cargo test --workspace` (all pass); `cargo fmt --all`; clippy exit 0.
```bash
git add -A
git commit -m "feat(orchestrator): SP-3 slice 2 (1/3) — BranchCond + matches (pure predicate)

BranchCond{FieldEquals|FieldTrue|TextContains} + matches(output) mirroring LoopGate.
Additive core type — no NodeKind change, no ripple."
```

---

## Task 2: `NodeKind::Branch` + `run_branch` + recursive validate

Adds the variant (breaks `run_node`'s exhaustive match → variant + arm + `run_branch` land together, one green commit), the validation, and the execution path reusing slice-1 helpers.

**Files:** `graph.rs` (variant + validate), `error.rs` (BranchInputMissing), `subgraph.rs` (promote 2 helpers), new `branch.rs`, `mod.rs` (mod + arm), `tests.rs`.

- [ ] **Step 1: Write the failing tests** (add to `tests.rs`). Helper to build a branch + `on`:
```rust
    // A one-node arm whose sink id names the arm (so we can see which arm ran).
    fn arm(inner_id: &str) -> Graph { Graph { nodes: vec![mc(inner_id, None)] } }
    // Outer: on(ModelCall "c") → branch (Hard-dep on `on`).
    fn branch_graph(arms: Vec<(BranchCond, Graph)>, default: Graph) -> Graph {
        Graph { nodes: vec![
            mc("on", None),
            Node { id: NodeId("br".into()),
                   kind: NodeKind::Branch { on: NodeId("on".into()), arms, default },
                   deps: vec![Dep { on: NodeId("on".into()), kind: EdgeKind::Hard }] },
        ] }
    }

#[tokio::test]
async fn branch_selects_first_matching_arm() {
    use orchestrator_core::BranchCond;
    let (gateway, _c) = recording_gateway().await; // `on` output = {"text":"canned-response"}
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let br = NodeId("br".into());
    // Arm 0 does NOT match, arm 1 matches ("canned-response" contains "canned").
    let graph = branch_graph(
        vec![
            (BranchCond::TextContains("zzz-nope".into()), arm("armA_out")),
            (BranchCond::TextContains("canned".into()), arm("armB_out")),
        ],
        arm("armDefault_out"),
    );
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    let b = &out.outputs[&br];
    assert!(b.get("armB_out").is_some(), "arm 1 (first match) ran: {b}");
    assert!(b.get("armA_out").is_none() && b.get("armDefault_out").is_none(), "others didn't: {b}");
}

#[tokio::test]
async fn branch_earlier_matching_arm_wins_over_later() {
    use orchestrator_core::BranchCond;
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let br = NodeId("br".into());
    // Both match "canned-response"; arm 0 must win.
    let graph = branch_graph(
        vec![
            (BranchCond::TextContains("canned".into()), arm("armA_out")),
            (BranchCond::TextContains("response".into()), arm("armB_out")),
        ],
        arm("armDefault_out"),
    );
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    let b = &out.outputs[&br];
    assert!(b.get("armA_out").is_some() && b.get("armB_out").is_none(), "earlier arm wins: {b}");
}

#[tokio::test]
async fn branch_runs_default_when_no_arm_matches() {
    use orchestrator_core::BranchCond;
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let br = NodeId("br".into());
    let graph = branch_graph(
        vec![(BranchCond::TextContains("zzz".into()), arm("armA_out"))],
        arm("armDefault_out"),
    );
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    let b = &out.outputs[&br];
    assert!(b.get("armDefault_out").is_some() && b.get("armA_out").is_none(), "default ran: {b}");
}

#[tokio::test]
async fn branch_journals_only_the_selected_arm() {
    use orchestrator_core::BranchCond;
    let (gateway, _c) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let run = RunId(uuid::Uuid::new_v4());
    let graph = branch_graph(
        vec![
            (BranchCond::TextContains("zzz".into()), arm("armA_out")),
            (BranchCond::TextContains("canned".into()), arm("armB_out")),
        ],
        arm("armDefault_out"),
    );
    exec.run(run, &graph).await.expect("run");
    // Only arm 1 ("br/1/armB_out") is journaled; arm 0 / default never ran.
    let labels: Vec<String> = journal.load(run).await.unwrap().iter().filter_map(|(_, e)| match e {
        JournalEvent::NodeStarted { node } => Some(node.0.clone()),
        _ => None,
    }).collect();
    assert!(labels.iter().any(|l| l == "br/1/armB_out"), "selected arm journaled: {labels:?}");
    assert!(!labels.iter().any(|l| l.contains("armA_out") || l.contains("armDefault_out")),
        "unselected arms not journaled: {labels:?}");
}

#[test]
fn validate_dag_rejects_bad_branch() {
    use orchestrator_core::BranchCond;
    // on not a Hard dep → InvalidGraph.
    let no_dep = Graph { nodes: vec![
        mc("on", None),
        Node { id: NodeId("br".into()),
               kind: NodeKind::Branch { on: NodeId("on".into()),
                   arms: vec![(BranchCond::FieldTrue("x".into()), Graph { nodes: vec![mc("a", None)] })],
                   default: Graph { nodes: vec![mc("d", None)] } },
               deps: vec![] }, // MISSING the Hard dep on `on`
    ] };
    assert!(matches!(no_dep.validate_dag(), Err(OrchestratorError::InvalidGraph(_))));
    // on undeclared → InvalidGraph.
    let undeclared = Graph { nodes: vec![
        Node { id: NodeId("br".into()),
               kind: NodeKind::Branch { on: NodeId("ghost".into()),
                   arms: vec![], default: Graph { nodes: vec![mc("d", None)] } },
               deps: vec![Dep { on: NodeId("ghost".into()), kind: EdgeKind::Hard }] },
    ] };
    assert!(matches!(undeclared.validate_dag(), Err(OrchestratorError::InvalidGraph(_))));
    // nested cycle in an arm → InvalidGraph (recursion).
    let cyc = Graph { nodes: vec![ mc("on", None),
        Node { id: NodeId("br".into()),
               kind: NodeKind::Branch { on: NodeId("on".into()),
                   arms: vec![(BranchCond::FieldTrue("x".into()), Graph { nodes: vec![
                       Node { id: NodeId("a".into()), kind: NodeKind::ModelCall { chain: "c".into(), payload: serde_json::json!(0) }, deps: vec![Dep { on: NodeId("b".into()), kind: EdgeKind::Hard }] },
                       Node { id: NodeId("b".into()), kind: NodeKind::ModelCall { chain: "c".into(), payload: serde_json::json!(0) }, deps: vec![Dep { on: NodeId("a".into()), kind: EdgeKind::Hard }] },
                   ] })],
                   default: Graph { nodes: vec![mc("d", None)] } },
               deps: vec![Dep { on: NodeId("on".into()), kind: EdgeKind::Hard }] },
    ] };
    assert!(matches!(cyc.validate_dag(), Err(OrchestratorError::InvalidGraph(_))));
}
```

- [ ] **Step 2: Run to confirm failure** — `cargo test -p sensei-orchestrator branch_selects_first_matching_arm` → FAIL to compile (`NodeKind::Branch` not found). RED.

- [ ] **Step 3: Add the `Branch` variant** (graph.rs, after `Subgraph`):
```rust
    /// A deterministic conditional (SP-3): test predecessor `on`'s output, run the
    /// first arm whose `BranchCond` matches (else `default`) as a nested graph under
    /// `"{branch}/{label}/…"`. The decision is pure over `on`'s memoized output, so
    /// resume recomputes the same arm — no branch journaling. Static this slice.
    Branch {
        on: NodeId,
        arms: Vec<(BranchCond, Graph)>,
        default: Graph,
    },
```
(A `Vec`/`Graph` are `Vec`-backed → the recursive enum is finite without boxing.)

- [ ] **Step 4: Extend `validate_dag`** — after the `// 2c.` Subgraph recursion block, add:
```rust
        // 2d. A `Branch`: `on` must be a declared node AND a Hard dep of the branch
        // (so it runs first and a failed `on` cascade-skips the branch); each arm's
        // and the default's nested graph must be a valid DAG (recursive).
        for node in &self.nodes {
            if let NodeKind::Branch { on, arms, default } = &node.kind {
                if !ids.contains(on) {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "branch {:?} tests undeclared node {:?}", node.id, on)));
                }
                if !node.deps.iter().any(|d| &d.on == on && matches!(d.kind, EdgeKind::Hard)) {
                    return Err(OrchestratorError::InvalidGraph(format!(
                        "branch {:?} must Hard-depend on its `on` node {:?}", node.id, on)));
                }
                for (_, g) in arms {
                    g.validate_dag()?;
                }
                default.validate_dag()?;
            }
        }
```
(`ids` is the `HashSet<&NodeId>` from section 1 — confirm it's still in scope at this point; if `validate_dag` dropped `ids` before here, rebuild it or move this block up next to `2c`.)

- [ ] **Step 5: Add the error variant** (error.rs, after `GlobalCapExceeded`):
```rust
    #[error("branch {branch:?} has no decision value — its `on` node {on:?} produced no output")]
    BranchInputMissing { branch: NodeId, on: NodeId },
```

- [ ] **Step 6: Promote the shared helpers** — in `subgraph.rs`, change `fn namespace_graph` → `pub(super) fn namespace_graph` and `fn sink_outputs` → `pub(super) fn sink_outputs`.

- [ ] **Step 7: Create `crates/orchestrator/src/executor/branch.rs`:**
```rust
//! The `Branch` node: a deterministic conditional (SP-3 slice 2). Test predecessor
//! `on`'s memoized output with a pure `BranchCond`, run the first matching arm (else
//! `default`) as a nested graph under `"{branch}/{label}/…"` — reusing the subgraph
//! namespace/drive/sink machinery. Pure over `on`'s output ⇒ resume recomputes the
//! same arm, no branch journaling.

use std::collections::HashMap;

use orchestrator_core::{Graph, Node, NodeId, NodeKind, OrchestratorError, RunId};

use super::subgraph::{namespace_graph, sink_outputs};
use super::{Executor, Fold, NodeExec};

impl Executor {
    pub(super) async fn run_branch(
        &self,
        run: RunId,
        node: &Node,
        prior_outputs: &HashMap<NodeId, serde_json::Value>,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        let NodeKind::Branch { on, arms, default } = &node.kind else {
            unreachable!("run_branch on non-Branch node");
        };
        // Decision value = predecessor `on`'s memoized output (Branch Hard-deps on
        // `on`, so a successful `on` has published it here; validate + cascade-skip
        // make the miss defensive).
        let value = prior_outputs.get(on).ok_or_else(|| OrchestratorError::BranchInputMissing {
            branch: node.id.clone(),
            on: on.clone(),
        })?;
        // Pure selection: first matching arm, else default.
        let (label, selected): (String, &Graph) = arms
            .iter()
            .enumerate()
            .find(|(_, (cond, _))| cond.matches(value))
            .map(|(i, (_, g))| (i.to_string(), g))
            .unwrap_or_else(|| ("default".to_string(), default));
        // Depth cap (self-DoS backstop), consistent with run_subgraph.
        let depth = node.id.0.matches('/').count();
        if depth + 1 > self.max_depth {
            return Err(OrchestratorError::GlobalCapExceeded {
                cap: "max_depth".into(),
                limit: self.max_depth,
            });
        }
        let prefix = format!("{}/{}", node.id.0, label);
        let inner = namespace_graph(&prefix, selected);
        let nested = Box::pin(self.drive(run, &inner, fold)).await?;
        if let Some(p) = nested.paused {
            return Ok(NodeExec::Paused {
                reason: format!("branch {} arm {} paused: {}", node.id.0, label, p.reason),
            });
        }
        if let Some((n, msg)) = nested.failed {
            return Ok(NodeExec::Failed {
                message: format!("branch {} arm {} failed at {}: {}", node.id.0, label, n.0, msg),
                output: None,
            });
        }
        Ok(NodeExec::Completed(sink_outputs(selected, &prefix, &nested.outputs)))
    }
}
```
(`matches` on `BranchCond` requires no extra import — it's a method. `namespace_graph`/`sink_outputs` are imported from `super::subgraph`.)

- [ ] **Step 8: Wire it** — in `mod.rs`: add `mod branch;` beside the other `mod` decls; in `run_node`, after the `NodeKind::Subgraph { .. } =>` arm, add:
```rust
            NodeKind::Branch { .. } => self.run_branch(run, node, prior_outputs, fold).await,
```

- [ ] **Step 9: Run green + commit** — the new tests (single filter) PASS; `cargo test --workspace` (all pass); `cargo fmt --all`; clippy exit 0.
```bash
git add -A
git commit -m "feat(orchestrator): SP-3 slice 2 (2/3) — NodeKind::Branch + run_branch (deterministic conditional)

Branch{on,arms,default}: read prior_outputs[on], select first matching BranchCond arm
(else default), drive it as a nested graph under {branch}/{label}/… (reuses promoted
namespace_graph/sink_outputs). validate_dag: on must be a declared Hard dep + recurse
into arms/default. Output = selected arm's sink map. BranchInputMissing (defensive).
Failed/Paused mappings included (propagation tested in Task 3)."
```

---

## Task 3: Determinism/resume + `on`-failure cascade-skip + propagation + e2e + docs

`run_branch`'s Failed/Paused mappings exist (Task 2); this task adds their behavior tests, the resume test, the `on`-failure test, a nested-agent e2e, and the doc.

**Files:** `tests.rs`; `docs/features/orchestrator/execution-graph.md`.

- [ ] **Step 1: Determinism/resume test** — a run whose Branch selected an arm, then a downstream outer node fails; resume recomputes the same arm and replays it from the memo (no re-spend), and NO branch-decision event is journaled. Model on the slice-1 `subgraph_inner_nodes_replay_from_memo_on_resume` / `a_run_with_a_completed_subgraph_and_a_failing_tail_resumes_correctly` (grep them). Outer graph: `on → br(Branch, Hard-dep on) → d(ModelCall, Hard-dep br)`; run 1 (`failing_after_gateway(N)` tuned so `on`+branch-arm complete and `d` fails) leaves a partial; resume on `recording_gateway` completes `d` and does NOT re-drive the branch's arm (gateway call count for the arm's inner node unchanged). Assert resume completes and there is NO extra journal event for the branch decision (the only branch-related events are the arm's node events, present from run 1). Name it `branch_replays_the_same_arm_on_resume_without_respend`. (If the exact `failing_after_gateway` count is fiddly, tune against the harness; the load-bearing asserts: resume completes + the arm's inner ModelCall not re-called.)

- [ ] **Step 2: `on`-failure cascade-skips the Branch** —
```rust
#[tokio::test]
async fn a_failed_on_cascade_skips_the_branch() {
    use orchestrator_core::BranchCond;
    let (gateway, _c) = failing_after_gateway(0).await; // `on` fails immediately
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let graph = branch_graph(
        vec![(BranchCond::TextContains("x".into()), arm("armA_out"))],
        arm("armDefault_out"),
    );
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("outcome");
    assert!(out.failed.is_some(), "on failed: {out:?}");
    assert!(out.skipped.contains(&NodeId("br".into())), "branch cascade-skipped (never decided): {out:?}");
}
```

- [ ] **Step 3: Arm-failure + arm-pause propagation** — (a) an arm whose inner node fails → Branch `Failed` → an outer hard-dependent cascade-skipped. Build a graph where the selected arm's inner node fails (use `failing_after_gateway` tuned so `on` succeeds but the arm's ModelCall fails — OR make the selected arm's inner node a ModelCall on an unknown chain so it fails). Assert `out.failed.is_some()` and the outer hard-dependent is skipped. (b) Pause: adapt the slice-1 `an_in_doubt_mutation_in_a_subgraph_pauses_the_run` test — wrap the mutation-bearing agent node inside a Branch's SELECTED arm (a one-node Graph). Assert `out.paused.is_some()`, no `RunCompleted`. Name them `a_failing_node_in_the_selected_arm_fails_the_branch` and `an_in_doubt_mutation_in_a_branch_arm_pauses_the_run`. If the pause harness is heavy, reuse the slice-1 test's setup verbatim, changing only the graph shape (Subgraph → Branch with the agent node as the sole arm + a trivial default that isn't selected).

- [ ] **Step 4: Nested-agent e2e** —
```rust
#[tokio::test]
async fn branch_drives_a_nested_agent_arm_end_to_end() {
    use orchestrator_core::BranchCond;
    let (gateway, _c) = recording_gateway().await;
    let registry = agent_registry("c");
    let br = NodeId("br".into());
    // Selected arm is a nested Agent node.
    let agent_arm = Graph { nodes: vec![agent_node("agent_out", "a", "hi")] };
    let graph = Graph { nodes: vec![
        mc("on", None),
        Node { id: br.clone(), kind: NodeKind::Branch {
            on: NodeId("on".into()),
            arms: vec![(BranchCond::TextContains("canned".into()), agent_arm)],
            default: Graph { nodes: vec![mc("armDefault_out", None)] },
        }, deps: vec![Dep { on: NodeId("on".into()), kind: EdgeKind::Hard }] },
    ] };
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1").with_registry(registry);
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    assert!(out.outputs[&br].get("agent_out").is_some(), "nested agent arm ran: {}", out.outputs[&br]);
}
```

- [ ] **Step 5: Run tests + update the feature doc.** In `docs/features/orchestrator/execution-graph.md`, add a Branch bullet next to Subgraph:
```markdown
- **`Branch { on, arms, default }`** (SP-3 slice 2) — a deterministic conditional:
  tests predecessor `on`'s memoized output with a pure `BranchCond`
  (`FieldEquals`/`FieldTrue`/`TextContains`, first match wins, required `default`) and
  runs the selected arm as a nested graph under `{branch}/{label}/…` (reusing the
  Subgraph machinery). The decision is recomputed on resume (no branch journaling);
  only the selected arm runs. `on` must be a declared Hard dep (a failed `on`
  cascade-skips the branch). Output = the selected arm's sink map; failure/pause
  propagate like `Subgraph`.
```

- [ ] **Step 6: Run green + commit** — `cargo test --workspace` (all pass); `cargo fmt --all`; clippy exit 0.
```bash
git add -A
git commit -m "feat(orchestrator): SP-3 slice 2 (3/3) — branch determinism/resume + propagation + e2e + docs

Resume recomputes the same arm from on's memoized output (no re-spend, no branch
event); a failed on cascade-skips the branch; arm failure → Branch Failed → outer
cascade-skip; in-doubt Mutation in an arm → run pauses; e2e drives a nested Agent arm.
execution-graph.md documents Branch."
```

---

## Self-Review

**1. Spec coverage** (against `2026-08-12-sp3-branch-node-design.md` §7):
- §7.1 `matches` → Task 1 `branch_cond_matches_each_variant`.
- §7.2 first-match/order → Task 2 `branch_selects_first_matching_arm` + `branch_earlier_matching_arm_wins_over_later`.
- §7.3 default → Task 2 `branch_runs_default_when_no_arm_matches`.
- §7.4 only-selected-arm-journaled → Task 2 `branch_journals_only_the_selected_arm`.
- §7.5 determinism/resume → Task 3 `branch_replays_the_same_arm_on_resume_without_respend`.
- §7.6 validate (recursion + on-undeclared + on-not-Hard-dep) → Task 2 `validate_dag_rejects_bad_branch`.
- §7.7 on-failure cascade-skip → Task 3 `a_failed_on_cascade_skips_the_branch`.
- §7.8 failure/pause propagation → Task 3 `a_failing_node_in_the_selected_arm_fails_the_branch` + `an_in_doubt_mutation_in_a_branch_arm_pauses_the_run`.
- §7.9 e2e → Task 3 `branch_drives_a_nested_agent_arm_end_to_end`.
- §7.10 additive → Task 2 Step 9 (workspace green; new arm only).
All covered.

**2. Placeholder scan:** No TBD/TODO; every code step complete; test steps reference the real harness (`recording_gateway` canned `"canned-response"`, `failing_after_gateway`, the slice-1 subgraph/pause tests) with instructions to bind to actual signatures.

**3. Type consistency:** `BranchCond::{FieldEquals(String,Value),FieldTrue(String),TextContains(String)}` + `matches(&self, output) -> bool`; `NodeKind::Branch { on: NodeId, arms: Vec<(BranchCond, Graph)>, default: Graph }`; `run_branch(&self, run, node, prior_outputs, fold) -> Result<NodeExec,_>`; `BranchInputMissing { branch: NodeId, on: NodeId }`; promoted `pub(super) namespace_graph`/`sink_outputs`; `matches!(d.kind, EdgeKind::Hard)`. All consistent across tasks; sink key = arm's inner node local id (per slice-1 `sink_outputs`).

**4. Green-per-commit:** Task 1 additive (`BranchCond` only). Task 2 lands the variant + `run_node` arm + `run_branch` together (compiles). Task 3 is tests + docs over green.
