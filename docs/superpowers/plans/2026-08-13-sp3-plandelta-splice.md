# SP-3 slice 3 — runtime PlanDelta / graph splicing (Expand node) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `NodeKind::Expand` — a node that produces a nested subgraph at runtime via an injected `Planner`, journals it as `PlanExpanded`, drives it under the node's path, and reconstructs it from the journal on resume (never re-planning) — plus the `max_expansions`/`max_nodes` self-DoS caps and the shared `drive_nested` helper.

**Architecture:** An `Expand` node's graph is *impure* (planner-produced), so unlike `Subgraph`/`Branch` it is journaled (`PlanExpanded{node, subgraph}`) and reconstructed from the journal on resume. Execution reuses the slice-1/2 nested-drive machinery via a newly-extracted `drive_nested` helper (depth-cap → `namespace_graph` → `drive` → `sink_outputs`). Caps are enforced by a run-scoped counter seeded from the journal on resume.

**Tech Stack:** Rust, `orchestrator-core` (zero-I/O types) + `orchestrator` (executor), `async_trait`, `tokio`, `serde_json`, `orchestrator-store::InMemoryJournal` for tests.

**Design spec:** `docs/superpowers/specs/2026-08-13-sp3-plandelta-splice-design.md`

**Conventions (from the ops memory):**
- Run `cargo fmt --all` before every commit (the pre-commit hook is fmt-check + `clippy -D warnings` — it runs **no** tests).
- Always run tests yourself; verify the **real** exit code (never a piped `| tail`).
- `crates/orchestrator/src/executor/` is a directory module (`mod.rs`, `subgraph.rs`, `branch.rs`, `tests.rs`, …).

---

## File Structure

- `crates/orchestrator-core/src/planner.rs` **(create)** — the `Planner` trait (injected seam).
- `crates/orchestrator-core/src/lib.rs` **(modify)** — `pub mod planner;` + re-export.
- `crates/orchestrator-core/src/graph.rs` **(modify)** — `NodeKind::Expand { input }`.
- `crates/orchestrator-core/src/journal.rs` **(modify)** — `JournalEvent::PlanExpanded { node, subgraph }` + roundtrip test.
- `crates/orchestrator/src/executor/mod.rs` **(modify)** — `Fold.expansions`; `Executor` fields `planner`/`max_expansions`/`max_nodes`/`expansion_counters`; setters; `ExpansionCounters`; `check_expansion_budget`; `with_expansion_seed`; per-run reset/seed in `run_inner`/`start_inner`; `Expand` dispatch arm.
- `crates/orchestrator/src/executor/support.rs` **(modify)** — fold `PlanExpanded` into `Fold.expansions` + unit test.
- `crates/orchestrator/src/executor/subgraph.rs` **(modify)** — add `drive_nested`; slim `run_subgraph` to call it.
- `crates/orchestrator/src/executor/branch.rs` **(modify)** — slim `run_branch` to call `drive_nested`.
- `crates/orchestrator/src/executor/expand.rs` **(create)** — `run_expand` + `expand_failed`.
- `crates/orchestrator/src/executor/tests.rs` **(modify)** — Expand test helpers + acceptance tests.

---

## Task 1: Core types — `Planner` trait, `NodeKind::Expand`, `PlanExpanded` event, fold

**Files:**
- Create: `crates/orchestrator-core/src/planner.rs`
- Modify: `crates/orchestrator-core/src/lib.rs:5-32`
- Modify: `crates/orchestrator-core/src/graph.rs:9-67` (the `NodeKind` enum)
- Modify: `crates/orchestrator-core/src/journal.rs:1-6` (imports), `:45-122` (the `JournalEvent` enum), `:202-282` (tests)
- Modify: `crates/orchestrator/src/executor/mod.rs:94-115` (`Fold`)
- Modify: `crates/orchestrator/src/executor/support.rs:73-144` (fold match), `:326-369` (tests)

- [ ] **Step 1: Add the `Planner` trait**

Create `crates/orchestrator-core/src/planner.rs`:

```rust
//! The injected planner seam (SP-3 slice 3): a node's runtime graph producer.
//! Slice 3 ships test/stub impls; slice 4 drops in the LLM-backed planner agent.

use async_trait::async_trait;

use crate::error::OrchestratorError;
use crate::graph::Graph;

/// Produces a nested subgraph at runtime for a [`NodeKind::Expand`](crate::graph::NodeKind::Expand)
/// node. The returned `Graph` carries LOCAL ids (namespaced under the node at drive
/// time). Returning `Err` — or a graph that fails `validate_dag` — is a node-level
/// failure the executor maps to `Failed`, never a panic.
#[async_trait]
pub trait Planner: Send + Sync {
    async fn plan(&self, input: &serde_json::Value) -> Result<Graph, OrchestratorError>;
}
```

- [ ] **Step 2: Export the trait from the core crate**

In `crates/orchestrator-core/src/lib.rs`, add the module (after `pub mod journal;`, keeping alpha-ish order — place after `mod journal;`):

```rust
pub mod planner;
```

and add the re-export (after the `pub use journal::{...};` block):

```rust
pub use planner::Planner;
```

- [ ] **Step 3: Add the `Expand` node kind**

In `crates/orchestrator-core/src/graph.rs`, add this variant to `enum NodeKind` (immediately after the `Branch { … }` variant, before the closing `}` of the enum):

```rust
    /// A node that produces a nested subgraph AT RUNTIME (impure), drives it under
    /// `"{expand}/…"`, and folds its sink map as output (SP-3 slice 3). Unlike
    /// `Subgraph` (static) and `Branch` (pure decision), the produced graph comes
    /// from an injected `Planner`, so it is journaled as `PlanExpanded` and
    /// reconstructed from the journal on resume — never re-planned. `input` is a
    /// static `Value` this slice (author-provided); slice 4/5 threads it from a
    /// predecessor's output. No sibling-id references, so `namespace_graph`'s
    /// `other => other.clone()` arm and `validate_dag` need no `Expand` case.
    Expand { input: serde_json::Value },
```

- [ ] **Step 4: Add the `PlanExpanded` journal event + import**

In `crates/orchestrator-core/src/journal.rs`, add the `Graph` import near the top (after `use crate::effect::{EffectClass, EffectId};`):

```rust
use crate::graph::Graph;
```

Add this variant to `enum JournalEvent` (after the `MapCompacted { … }` variant, before `ContextWrite`):

```rust
    /// A runtime graph expansion (§7.2/§7.6/§10.3): node `node` produced `subgraph`.
    /// Journaled BEFORE the nested graph is driven, so a crash mid-expansion resumes
    /// with the identical structure. The resume fold reconstructs the spliced graph
    /// from this — the memo, but for graph structure. `subgraph` carries LOCAL ids
    /// (namespaced under `node` at drive time), so the event is position-independent.
    PlanExpanded { node: NodeId, subgraph: Graph },
```

- [ ] **Step 5: Write the failing roundtrip test**

In `crates/orchestrator-core/src/journal.rs`, add to `mod tests`:

```rust
    #[test]
    fn plan_expanded_event_roundtrips() {
        use crate::graph::{Graph, Node, NodeKind};
        let e = JournalEvent::PlanExpanded {
            node: NodeId("e".into()),
            subgraph: Graph {
                nodes: vec![Node {
                    id: NodeId("n1".into()),
                    kind: NodeKind::ModelCall {
                        chain: "c".into(),
                        payload: serde_json::json!({ "prompt": "hi" }),
                    },
                    deps: vec![],
                }],
            },
        };
        let s = serde_json::to_string(&e).unwrap();
        let back: JournalEvent = serde_json::from_str(&s).unwrap();
        match back {
            JournalEvent::PlanExpanded { node, subgraph } => {
                assert_eq!(node, NodeId("e".into()));
                assert_eq!(subgraph.nodes.len(), 1);
            }
            other => panic!("expected PlanExpanded, got {other:?}"),
        }
    }
```

- [ ] **Step 6: Run it to verify it fails (compile error until Steps 3–4 land)**

Run: `cargo test -p sensei-orchestrator-core plan_expanded_event_roundtrips`
Expected: PASS once Steps 3–4 compile (the variant now exists). If Steps 3–4 were skipped: FAIL to compile (`no variant PlanExpanded`).

- [ ] **Step 7: Add `Fold.expansions`**

In `crates/orchestrator/src/executor/mod.rs`, add this field to `struct Fold` (after the `context` field, before the closing `}`):

```rust
    /// Runtime graph expansions folded from `PlanExpanded` events (§4.4). The
    /// structural analog of `memo`: on resume, `run_expand` replays the journaled
    /// subgraph for a node found here — never re-invoking the planner.
    expansions: HashMap<NodeId, Graph>,
```

(`Graph` and `HashMap` and `NodeId` are already imported in `mod.rs`.)

- [ ] **Step 8: Fold `PlanExpanded` into `Fold.expansions`**

In `crates/orchestrator/src/executor/support.rs`, in `fold_journal`'s `match event { … }`, add this arm (before the final `_ => {}`):

```rust
            JournalEvent::PlanExpanded { node, subgraph } => {
                fold.expansions.insert(node.clone(), subgraph.clone());
            }
```

- [ ] **Step 9: Write the failing fold unit test**

In `crates/orchestrator/src/executor/support.rs`, add to `mod tests`:

```rust
    #[test]
    fn fold_journal_captures_plan_expansions() {
        use orchestrator_core::{Graph, JournalEvent, Node, NodeId, NodeKind};
        let subgraph = Graph {
            nodes: vec![Node {
                id: NodeId("n1".into()),
                kind: NodeKind::ModelCall {
                    chain: "c".into(),
                    payload: serde_json::json!(0),
                },
                deps: vec![],
            }],
        };
        let events = vec![(
            0u64,
            JournalEvent::PlanExpanded {
                node: NodeId("e".into()),
                subgraph: subgraph.clone(),
            },
        )];
        let (fold, _last, _completed) = fold_journal(&events);
        assert_eq!(
            fold.expansions.get(&NodeId("e".into())).map(|g| g.nodes.len()),
            Some(1),
            "PlanExpanded folds into fold.expansions"
        );
    }
```

- [ ] **Step 9b: Satisfy the executor's exhaustive matches (temporary stubs)**

Adding the two core variants forces two *exhaustive* `match`es in the `sensei-orchestrator` crate to handle them, or the crate won't compile (and the pre-commit `clippy --workspace` will reject the commit). Add the minimal arms:

In `crates/orchestrator/src/executor/mod.rs`, in `run_node`'s `match &node.kind { … }`, add a **temporary** arm after the `NodeKind::Branch { .. } =>` arm. Task 3 Step 4 replaces this exact line with the real dispatch:

```rust
            // TEMPORARY (SP-3 slice 3, Task 1): exhaustiveness stub. Task 3 replaces
            // this with `=> self.run_expand(run, node, fold).await`. No Task-1 test
            // constructs an `Expand` node, so this is never reached in this commit.
            NodeKind::Expand { .. } => unreachable!("Expand execution lands in Task 3"),
```

In `crates/orchestrator/src/executor/tests.rs`, in the `label` helper's `match event { … }` (an exhaustive match over `JournalEvent`), add this **permanent** arm (place it after the `MapCompacted` arm):

```rust
        JournalEvent::PlanExpanded { node, .. } => format!("PlanExpanded({})", node.0),
```

- [ ] **Step 10: Run the tests to verify they pass**

Run: `cargo test -p sensei-orchestrator-core plan_expanded_event_roundtrips && cargo test -p sensei-orchestrator fold_journal_captures_plan_expansions`
Expected: both PASS. Verify the real exit code is 0 (do not pipe through `tail`).

- [ ] **Step 11: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-core/src/planner.rs crates/orchestrator-core/src/lib.rs \
        crates/orchestrator-core/src/graph.rs crates/orchestrator-core/src/journal.rs \
        crates/orchestrator/src/executor/mod.rs crates/orchestrator/src/executor/support.rs \
        crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-3 slice 3 (1/6) — Planner trait, NodeKind::Expand, PlanExpanded event + fold"
```

---

## Task 2: Extract the `drive_nested` helper (behavior-preserving refactor)

**Files:**
- Modify: `crates/orchestrator/src/executor/subgraph.rs:80-134` (`run_subgraph`; add `drive_nested`)
- Modify: `crates/orchestrator/src/executor/branch.rs:27-85` (`run_branch`)

**Note:** This retires the slice-1/2 duplication (the memory's deferred `drive_nested` carryover). The `kind_label` parameter keeps **`Subgraph` messages byte-identical**; **`Branch`** changes intentionally — its pause/fail message reads `"branch {node}/{label} …"` (was `"branch {node} arm {label} …"`) and its depth bound is now the exact `"{node}/{label}"`-derived level (one segment deeper than before, the accurate bound slice 2 §4.2 called a backstop). No existing test asserts a Branch pause/fail message or a Branch depth boundary (Branch tests assert journaled path labels like `"br/1/armB_out"`, which are unchanged), so no test breaks.

- [ ] **Step 1: Add `drive_nested` to `subgraph.rs`**

In `crates/orchestrator/src/executor/subgraph.rs`, add this method to the existing `impl Executor { … }` block (place it directly above `run_subgraph`):

```rust
    /// The shared nested-drive tail for `Subgraph`/`Branch`/`Expand` (SP-3): enforce
    /// the depth cap on `prefix`, namespace `graph` under it, drive it in the SAME
    /// run, and fold the outcome into a `NodeExec` (paused/failed carried up, else the
    /// sink map). `kind_label` only tags the pause/fail message; `prefix` is the path
    /// the nested nodes are namespaced under (`"{node}"` for subgraph/expand,
    /// `"{node}/{label}"` for a branch arm).
    pub(super) async fn drive_nested(
        &self,
        run: RunId,
        kind_label: &str,
        prefix: &str,
        graph: &Graph,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        // Depth cap (self-DoS backstop): the path segment count is the nesting level.
        let depth = prefix.matches('/').count();
        if depth + 1 > self.max_depth {
            return Err(OrchestratorError::GlobalCapExceeded {
                cap: "max_depth".into(),
                limit: self.max_depth,
            });
        }
        let inner = namespace_graph(prefix, graph);
        // `Box::pin` breaks the recursive `async fn` cycle (run_node → run_* →
        // drive_nested → drive → run_node): heap indirection keeps the future finite.
        let nested = Box::pin(self.drive(run, &inner, fold)).await?;
        if let Some(p) = nested.paused {
            return Ok(NodeExec::Paused {
                reason: format!("{kind_label} {prefix} paused: {}", p.reason),
            });
        }
        if let Some((n, msg)) = nested.failed {
            return Ok(NodeExec::Failed {
                message: format!("{kind_label} {prefix} failed at {}: {msg}", n.0),
                output: None,
            });
        }
        Ok(NodeExec::Completed(sink_outputs(graph, prefix, &nested.outputs)))
    }
```

- [ ] **Step 2: Slim `run_subgraph` to call it**

In `crates/orchestrator/src/executor/subgraph.rs`, replace the entire body of `run_subgraph` (keep its doc comment and signature) with:

```rust
    pub(super) async fn run_subgraph(
        &self,
        run: RunId,
        node: &Node,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        let NodeKind::Subgraph { graph } = &node.kind else {
            unreachable!("run_subgraph on non-Subgraph node");
        };
        // `graph` is `&Box<Graph>`; deref-coerces to the `&Graph` param.
        self.drive_nested(run, "subgraph", &node.id.0, graph, fold)
            .await
    }
```

- [ ] **Step 3: Slim `run_branch` to call it**

In `crates/orchestrator/src/executor/branch.rs`, replace the entire body of `run_branch` (keep its doc comment and signature) with:

```rust
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
        let value = prior_outputs
            .get(on)
            .ok_or_else(|| OrchestratorError::BranchInputMissing {
                branch: node.id.clone(),
                on: on.clone(),
            })?;
        let (label, selected): (String, &Graph) = arms
            .iter()
            .enumerate()
            .find(|(_, (cond, _))| cond.matches(value))
            .map(|(i, (_, g))| (i.to_string(), g))
            .unwrap_or_else(|| ("default".to_string(), default));
        let prefix = format!("{}/{}", node.id.0, label);
        self.drive_nested(run, "branch", &prefix, selected, fold)
            .await
    }
```

Then remove the now-unused imports from `branch.rs`: delete the line `use super::subgraph::{namespace_graph, sink_outputs};` (both are used only by `drive_nested` now). Keep `use super::{Executor, Fold, NodeExec};` and the `orchestrator_core` import line (its `Graph`, `Node`, `NodeId`, `NodeKind`, `OrchestratorError`, `RunId` are all still used).

- [ ] **Step 4: Verify the refactor compiles and existing nested tests pass**

Run: `cargo test -p sensei-orchestrator -- subgraph branch` (multiple name filters must follow `--`; a bare `cargo test -p … subgraph branch` errors)
Expected: PASS — every existing `subgraph_*` and `branch_*` test still green (Subgraph messages/depth unchanged; Branch tests assert path labels/outputs, not messages/depth).

If a Branch test unexpectedly fails on a message substring, update that assertion from the old `"branch <id> arm <label>"` wording to `"branch <id>/<label>"` and re-run. (None are expected — the current Branch tests do not assert pause/fail message text.)

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/subgraph.rs crates/orchestrator/src/executor/branch.rs
git commit -m "refactor(orchestrator): SP-3 slice 3 (2/6) — extract shared drive_nested (subgraph+branch)"
```

---

## Task 3: `run_expand` (fresh path) + dispatch + `with_planner`

**Files:**
- Create: `crates/orchestrator/src/executor/expand.rs`
- Modify: `crates/orchestrator/src/executor/mod.rs:16-26` (module list + imports), `:30-64` (fields), `:130-152` (`new`), `:195-244` (setters), `:705-713` (dispatch)
- Test: `crates/orchestrator/src/executor/tests.rs` (append at end)

- [ ] **Step 1: Write the failing tests (AC1, AC2, AC5, AC6, AC7)**

In `crates/orchestrator/src/executor/tests.rs`, append a new section:

```rust
// ---------------------------------------------------------------------------
// SP-3 slice 3 — `NodeKind::Expand`: a node whose nested DAG is produced at
// runtime by an injected `Planner`, journaled as `PlanExpanded`, and driven
// under the node's path.
// ---------------------------------------------------------------------------

/// A `Planner` that always returns a fixed graph (the produced plan under test).
struct FixedPlanner(Graph);
#[async_trait::async_trait]
impl orchestrator_core::Planner for FixedPlanner {
    async fn plan(&self, _input: &serde_json::Value) -> Result<Graph, OrchestratorError> {
        Ok(self.0.clone())
    }
}
/// A `Planner` that always errors — exercises the planner-failure path.
struct ErrPlanner;
#[async_trait::async_trait]
impl orchestrator_core::Planner for ErrPlanner {
    async fn plan(&self, _input: &serde_json::Value) -> Result<Graph, OrchestratorError> {
        Err(OrchestratorError::InvalidGraph("planner boom".into()))
    }
}

fn expand_node(id: &str, deps: Vec<Dep>) -> Node {
    Node {
        id: NodeId(id.into()),
        kind: NodeKind::Expand {
            input: serde_json::json!({}),
        },
        deps,
    }
}

#[tokio::test]
async fn expand_drives_a_produced_plan_and_returns_the_sink_map() {
    let (gateway, _c) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let planner = Arc::new(FixedPlanner(Graph {
        nodes: vec![mc("n1", None), mc("n2", Some("n1"))],
    }));
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_planner(planner);
    let e = NodeId("e".into());
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![expand_node("e", vec![])],
    };
    let out = exec.run(run, &graph).await.expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    assert!(out.outputs[&e].get("n2").is_some(), "sink map has n2: {}", out.outputs[&e]);
    assert!(out.outputs[&e].get("n1").is_none(), "n1 is not a sink");

    // AC2: PlanExpanded precedes the nested effects.
    let events = journal.load(run).await.unwrap();
    let pe = events
        .iter()
        .position(|(_, ev)| matches!(ev, JournalEvent::PlanExpanded { .. }))
        .expect("PlanExpanded journaled");
    let first_rec = events
        .iter()
        .position(|(_, ev)| matches!(ev, JournalEvent::EffectRecorded { .. }))
        .expect("nested effects journaled");
    assert!(pe < first_rec, "PlanExpanded precedes the nested effects");
}

#[tokio::test]
async fn expand_planner_error_fails_the_node_and_cascade_skips_hard_dependents() {
    let (gateway, _c) = recording_gateway().await;
    // e (Expand, ErrPlanner) → d (Hard-dep e) ; s (Soft-dep e).
    let graph = Graph {
        nodes: vec![
            expand_node("e", vec![]),
            mc_dep("d", Dep::hard("e")),
            mc_dep("s", Dep::soft("e")),
        ],
    };
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_planner(Arc::new(ErrPlanner));
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    assert!(
        matches!(&out.failed, Some((n, _)) if n == &NodeId("e".into())),
        "expand failed: {out:?}"
    );
    assert!(out.skipped.contains(&NodeId("d".into())), "hard-dependent skipped");
    assert!(out.completed.contains(&NodeId("s".into())), "soft-dependent still ran");
}

#[tokio::test]
async fn expand_invalid_plan_fails_the_node_without_journaling_an_expansion() {
    let (gateway, _c) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    // A cyclic produced graph (a → b → a): validate_dag rejects it.
    let cyclic = Graph {
        nodes: vec![mc_dep("a", Dep::hard("b")), mc_dep("b", Dep::hard("a"))],
    };
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_planner(Arc::new(FixedPlanner(cyclic)));
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![expand_node("e", vec![])],
    };
    let out = exec.run(run, &graph).await.expect("run yields an outcome");
    assert!(out.failed.is_some(), "invalid plan fails the expand: {out:?}");
    let events = journal.load(run).await.unwrap();
    assert!(
        !events.iter().any(|(_, ev)| matches!(ev, JournalEvent::PlanExpanded { .. })),
        "no PlanExpanded journaled for an invalid plan (validated before append)"
    );
}

#[tokio::test]
async fn expand_with_no_planner_fails_loud() {
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let graph = Graph {
        nodes: vec![expand_node("e", vec![])],
    };
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    assert!(
        matches!(&out.failed, Some((n, m)) if n == &NodeId("e".into()) && m.contains("no planner")),
        "expand with no planner fails loud: {out:?}"
    );
}
```

Add this small helper near the top-of-file helpers (after `fn mc(…)`, around line 4765) — a `ModelCall` node with an explicit dep, reused by several slice-3 tests:

```rust
fn mc_dep(id: &str, dep: Dep) -> Node {
    Node {
        id: NodeId(id.into()),
        kind: NodeKind::ModelCall {
            chain: "c".into(),
            payload: serde_json::json!({ "prompt": id }),
        },
        deps: vec![dep],
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail (compile error — `with_planner`/`Expand` arm missing)**

Run: `cargo test -p sensei-orchestrator expand_` (read the real output/exit code — do NOT pipe through `head`/`tail`/`grep`)
Expected: FAIL to compile (`no method with_planner`, `no run_expand` dispatch). This confirms the tests exercise the new surface.

- [ ] **Step 3: Add executor fields, `ExpansionCounters`, and setters**

In `crates/orchestrator/src/executor/mod.rs`:

Add the module declaration next to the others (with `mod branch; mod subgraph;`):

```rust
mod expand;
```

Add `Planner` to the `use orchestrator_core::{ … }` import list (append it, keeping the list sorted-ish):

```rust
    OrchestratorHooks, Planner, Registry, RegistryHandle, RunId, Scope, Seq, SystemClock, effect_id,
```

Add these fields to `struct Executor` (after the `handle` field, before the closing `}`):

```rust
    /// The injected planner an `Expand` node produces its subgraph from (SP-3
    /// slice 3). `None` ⇒ an `Expand` node fails loudly (byte-identical for graphs
    /// without `Expand`).
    planner: Option<Arc<dyn Planner>>,
    /// Max runtime expansions (`PlanDelta`s) per run — a self-DoS cap. Default 32.
    max_expansions: usize,
    /// Max cumulative spliced-node count per run — a self-DoS cap. Default 512.
    max_nodes: usize,
    /// Run-scoped expansion counters (seeded from the journal on resume) the caps
    /// are checked against. Reset per run by `run_inner`/`start_inner`.
    expansion_counters: Arc<ExpansionCounters>,
```

Add the counters struct (place it just after the `Fold` struct definition):

```rust
/// Run-scoped tallies for the expansion caps (§4.5). Only ever mutated from the
/// sequential top-level drive loop (a `Map`'s concurrency wraps `ModelCall`/`Agent`
/// bodies, never an `Expand`), so `Relaxed` ordering is sufficient — the check is a
/// self-DoS backstop, not a synchronization primitive.
#[derive(Default)]
struct ExpansionCounters {
    expansions: std::sync::atomic::AtomicUsize,
    nodes: std::sync::atomic::AtomicUsize,
}
```

In `Executor::new`, add to the struct literal (after `handle: None,`):

```rust
            planner: None,
            max_expansions: 32,
            max_nodes: 512,
            expansion_counters: Arc::new(ExpansionCounters::default()),
```

Add the setters (place them next to `with_max_depth`):

```rust
    /// Attach the planner an `Expand` node produces its subgraph from (SP-3 slice 3).
    pub fn with_planner(mut self, planner: Arc<dyn Planner>) -> Self {
        self.planner = Some(planner);
        self
    }

    /// Set the max runtime expansions (`PlanDelta`s) per run (self-DoS cap; default 32).
    pub fn with_max_expansions(mut self, n: usize) -> Self {
        self.max_expansions = n;
        self
    }

    /// Set the max cumulative spliced-node count per run (self-DoS cap; default 512).
    pub fn with_max_nodes(mut self, n: usize) -> Self {
        self.max_nodes = n;
        self
    }
```

- [ ] **Step 4: Replace the Task 1 stub with the real `Expand` dispatch arm**

In `crates/orchestrator/src/executor/mod.rs`, in `run_node`'s `match &node.kind { … }`, **replace** the temporary Task 1 stub arm (the `NodeKind::Expand { .. } => unreachable!("Expand execution lands in Task 3")` line and its comment) with the real dispatch:

```rust
            NodeKind::Expand { .. } => self.run_expand(run, node, fold).await,
```

- [ ] **Step 5: Implement `run_expand` (fresh path only, so far)**

Create `crates/orchestrator/src/executor/expand.rs`:

```rust
//! The `Expand` node (SP-3 slice 3): produce a nested subgraph at runtime via the
//! injected `Planner`, journal it as `PlanExpanded`, and drive it under the node's
//! path — reusing the shared `drive_nested`. A planner error, an invalid produced
//! graph, or no planner is a node `Failed` (journaled `NodeFailed`); a cap breach is
//! a hard `Err` (self-DoS). The resume path (reuse a journaled expansion, never
//! re-plan) lands in Task 4.

use orchestrator_core::{JournalEvent, Node, NodeKind, OrchestratorError, RunId};

use super::{Executor, Fold, NodeExec};

impl Executor {
    /// Drive an `Expand` node: produce → validate → cap-check → journal → drive.
    pub(super) async fn run_expand(
        &self,
        run: RunId,
        node: &Node,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        let NodeKind::Expand { input } = &node.kind else {
            unreachable!("run_expand on non-Expand node");
        };
        let Some(planner) = &self.planner else {
            return self
                .expand_failed(run, node, format!("expand {}: no planner wired", node.id.0))
                .await;
        };
        let g = match planner.plan(input).await {
            Ok(g) => g,
            Err(e) => {
                return self
                    .expand_failed(run, node, format!("expand {} planner failed: {e}", node.id.0))
                    .await;
            }
        };
        if let Err(e) = g.validate_dag() {
            return self
                .expand_failed(run, node, format!("expand {} produced an invalid plan: {e}", node.id.0))
                .await;
        }
        // Caps are a self-DoS backstop → hard `Err` (halts the run), NOT a node
        // Failed. Checked BEFORE journaling the expansion.
        self.check_expansion_budget(&g)?;
        self.append(
            run,
            JournalEvent::PlanExpanded {
                node: node.id.clone(),
                subgraph: g.clone(),
            },
        )
        .await?;
        self.drive_nested(run, "expand", &node.id.0, &g, fold).await
    }

    /// Journal a `NodeFailed` for an `Expand` node then return `Failed` — so a
    /// planner/plan failure is durable + surfaced (no silent failure) and
    /// cascade-skips hard-dependents, matching the `ModelCall` gateway-fail path.
    async fn expand_failed(
        &self,
        run: RunId,
        node: &Node,
        message: String,
    ) -> Result<NodeExec, OrchestratorError> {
        self.append(
            run,
            JournalEvent::NodeFailed {
                node: node.id.clone(),
                error: message.clone(),
            },
        )
        .await?;
        Ok(NodeExec::Failed {
            message,
            output: None,
        })
    }
}
```

- [ ] **Step 6: Add `check_expansion_budget` to the executor**

In `crates/orchestrator/src/executor/mod.rs`, add this method to `impl Executor` (place it near `run_node`):

```rust
    /// Enforce the expansion caps (§4.5) against the run-scoped counters, then tally
    /// the new expansion. A breach is a hard `Err` (self-DoS backstop); on success the
    /// counters advance by one expansion + `g.nodes.len()` nodes.
    fn check_expansion_budget(&self, g: &Graph) -> Result<(), OrchestratorError> {
        use std::sync::atomic::Ordering::Relaxed;
        if self.expansion_counters.expansions.load(Relaxed) + 1 > self.max_expansions {
            return Err(OrchestratorError::GlobalCapExceeded {
                cap: "max_expansions".into(),
                limit: self.max_expansions,
            });
        }
        if self.expansion_counters.nodes.load(Relaxed) + g.nodes.len() > self.max_nodes {
            return Err(OrchestratorError::GlobalCapExceeded {
                cap: "max_nodes".into(),
                limit: self.max_nodes,
            });
        }
        self.expansion_counters.expansions.fetch_add(1, Relaxed);
        self.expansion_counters.nodes.fetch_add(g.nodes.len(), Relaxed);
        Ok(())
    }
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `cargo test -p sensei-orchestrator expand_`
Expected: `expand_drives_a_produced_plan_and_returns_the_sink_map`, `expand_planner_error_fails_the_node_and_cascade_skips_hard_dependents`, `expand_invalid_plan_fails_the_node_without_journaling_an_expansion`, `expand_with_no_planner_fails_loud` all PASS. Verify the real exit code is 0.

- [ ] **Step 8: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/mod.rs crates/orchestrator/src/executor/expand.rs \
        crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-3 slice 3 (3/6) — run_expand fresh path + Planner wiring + Expand dispatch"
```

---

## Task 4: Resume — reconstruct the journaled expansion, never re-plan

**Files:**
- Modify: `crates/orchestrator/src/executor/expand.rs` (`run_expand`)
- Test: `crates/orchestrator/src/executor/tests.rs` (append)

- [ ] **Step 1: Write the failing determinism tests (AC3, AC4)**

In `crates/orchestrator/src/executor/tests.rs`, append:

```rust
/// AC3: after a crash mid-plan, a resume reconstructs the JOURNALED plan and never
/// re-invokes the planner — even one rigged to return a different graph.
#[tokio::test]
async fn expand_resume_uses_the_journaled_plan_not_a_re_plan() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let e = NodeId("e".into());
    let graph = Graph {
        nodes: vec![expand_node("e", vec![])],
    };

    // Run 1: planner returns plan A (n1 → n2). Gateway succeeds on inner n1 (call 1),
    // fails on inner n2 (call 2). PlanExpanded{A} + e/n1 are journaled; the run fails.
    let plan_a = Graph {
        nodes: vec![mc("n1", None), mc("n2", Some("n1"))],
    };
    let (gw1, calls1) = failing_after_gateway(1).await;
    let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_planner(Arc::new(FixedPlanner(plan_a)));
    let o1 = exec1.run(run, &graph).await.expect("run 1 yields an outcome");
    assert!(o1.failed.is_some(), "run 1 fails inside the plan: {o1:?}");
    assert_eq!(calls1.lock().unwrap().len(), 2, "run 1 hit the gateway for n1 and the failing n2");

    // Run 2: a DIFFERENT planner (would return `zzz`) + an always-succeeding gateway
    // over the SAME journal. Resume must reuse plan A from the journal.
    let plan_b = Graph {
        nodes: vec![mc("zzz", None)],
    };
    let (gw2, calls2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_planner(Arc::new(FixedPlanner(plan_b)));
    let o2 = exec2.start(run, &graph).await.expect("resume completes");
    assert!(o2.failed.is_none(), "resume completes: {:?}", o2.failed);
    assert!(o2.outputs[&e].get("n2").is_some(), "journaled plan A used (n2 present): {}", o2.outputs[&e]);
    assert!(o2.outputs[&e].get("zzz").is_none(), "the re-plan graph (zzz) was NOT used");

    // The proof: run-2's gateway saw EXACTLY ONE call, for the failed inner n2 — n1
    // replayed from the memo, the planner was never re-invoked.
    let recorded2 = calls2.lock().unwrap().clone();
    assert_eq!(recorded2.len(), 1, "resume re-called the gateway only for n2: {recorded2:?}");
    assert_eq!(recorded2[0].1, "n2", "the single resume call carried n2's prompt");
}

/// AC4: a completed `Expand` whose OUTER tail fails resumes without re-planning or
/// re-spending on the plan's inner nodes.
#[tokio::test]
async fn expand_completed_then_failing_tail_resumes_without_replan() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    // e (Expand, plan = single node "n1") → d (Hard-dep e).
    let graph = Graph {
        nodes: vec![expand_node("e", vec![]), mc_dep("d", Dep::hard("e"))],
    };

    // Run 1: e's plan completes (inner n1 = call 1), then d fails (call 2).
    let (gw1, _c1) = failing_after_gateway(1).await;
    let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_planner(Arc::new(FixedPlanner(Graph { nodes: vec![mc("n1", None)] })));
    let o1 = exec1.run(run, &graph).await.expect("run 1");
    assert!(o1.failed.is_some(), "tail d failed: {o1:?}");

    // Run 2: a planner that would produce a DIFFERENT plan (`other`) + a succeeding
    // gateway. Resume replays e from the journal (no re-plan) and re-drives only d.
    let (gw2, calls2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_planner(Arc::new(FixedPlanner(Graph { nodes: vec![mc("other", None)] })));
    let o2 = exec2.start(run, &graph).await.expect("resume completes");
    assert!(o2.failed.is_none(), "resume completes: {o2:?}");
    assert!(
        o2.outputs[&NodeId("e".into())].get("n1").is_some(),
        "e replayed the journaled plan (n1), not the re-plan: {}",
        o2.outputs[&NodeId("e".into())]
    );
    let recorded2 = calls2.lock().unwrap().clone();
    assert_eq!(recorded2.len(), 1, "resume re-called the gateway only for d: {recorded2:?}");
    assert_eq!(recorded2[0].1, "d", "the single resume call carried d's prompt");
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p sensei-orchestrator -- expand_resume_uses_the_journaled_plan_not_a_re_plan expand_completed_then_failing_tail_resumes_without_replan`
Expected: FAIL. Because `run_expand` currently always re-plans, run 2 uses plan B (`zzz`/`other`), so `o2.outputs[&e].get("n2")` is `None` (and/or the gateway call count/prompt is wrong).

- [ ] **Step 3: Add the resume branch to `run_expand`**

In `crates/orchestrator/src/executor/expand.rs`, replace the **entire body** of `run_expand` (keep the signature; replace everything between the method's `{` and `}`) with the following — a `match` on `fold.expansions` that short-circuits on a journaled expansion and only otherwise produces/validates/caps/journals, with a single shared `drive_nested` tail:

```rust
        let NodeKind::Expand { input } = &node.kind else {
            unreachable!("run_expand on non-Expand node");
        };
        // RESUME: a node with a journaled `PlanExpanded` reuses that subgraph — the
        // planner is NOT re-invoked (determinism §4.4). FRESH: produce → validate →
        // cap-check → journal (in that order), then drive.
        let g = match fold.expansions.get(&node.id) {
            Some(journaled) => journaled.clone(),
            None => {
                let Some(planner) = &self.planner else {
                    return self
                        .expand_failed(run, node, format!("expand {}: no planner wired", node.id.0))
                        .await;
                };
                let produced = match planner.plan(input).await {
                    Ok(produced) => produced,
                    Err(e) => {
                        return self
                            .expand_failed(run, node, format!("expand {} planner failed: {e}", node.id.0))
                            .await;
                    }
                };
                if let Err(e) = produced.validate_dag() {
                    return self
                        .expand_failed(
                            run,
                            node,
                            format!("expand {} produced an invalid plan: {e}", node.id.0),
                        )
                        .await;
                }
                self.check_expansion_budget(&produced)?;
                self.append(
                    run,
                    JournalEvent::PlanExpanded {
                        node: node.id.clone(),
                        subgraph: produced.clone(),
                    },
                )
                .await?;
                produced
            }
        };
        self.drive_nested(run, "expand", &node.id.0, &g, fold).await
```

The `expand_failed` helper is unchanged. After this replacement `run_expand` has exactly one `drive_nested` tail serving both the resume (`Some`) and fresh (`None`) branches.

- [ ] **Step 4: Run to verify they pass (and no regressions in Task 3's tests)**

Run: `cargo test -p sensei-orchestrator expand_`
Expected: all `expand_*` tests PASS (Task 3's four + Task 4's two). Verify the real exit code is 0.

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/expand.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-3 slice 3 (4/6) — resume reconstructs the journaled expansion (no re-plan)"
```

---

## Task 5: Caps — per-run counters, resume seeding

**Files:**
- Modify: `crates/orchestrator/src/executor/mod.rs` (`run_inner`, `start_inner`; add `with_expansion_seed`)
- Test: `crates/orchestrator/src/executor/tests.rs` (append)

- [ ] **Step 1: Write the failing cap tests (AC8, AC9)**

In `crates/orchestrator/src/executor/tests.rs`, append:

```rust
/// AC8: more expansions than `max_expansions` → a hard `GlobalCapExceeded` halt.
#[tokio::test]
async fn expand_max_expansions_cap_halts_loud() {
    // e1 → e2 (sequential): two expansions. Each plan is a single node.
    let graph = Graph {
        nodes: vec![
            expand_node("e1", vec![]),
            expand_node("e2", vec![Dep::hard("e1")]),
        ],
    };
    let planner = Arc::new(FixedPlanner(Graph { nodes: vec![mc("x", None)] }));

    // Limit 1: the 2nd expansion breaches the cap → Err.
    let (gw, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_planner(planner.clone())
        .with_max_expansions(1);
    let err = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect_err("cap halts");
    assert!(
        matches!(&err, OrchestratorError::GlobalCapExceeded { cap, .. } if cap == "max_expansions"),
        "max_expansions breach: {err:?}"
    );

    // Limit 2: both expansions fit → ok.
    let (gw2, _c2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(InMemoryJournal::new()), "v1")
        .with_planner(planner)
        .with_max_expansions(2);
    let out = exec2.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("within cap");
    assert!(out.failed.is_none(), "{out:?}");
}

/// AC9: `max_nodes` is cumulative AND spans resume — the counter is seeded from the
/// journal, so a resumed expansion is charged against nodes counted before the crash.
#[tokio::test]
async fn expand_max_nodes_cap_spans_resume() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    // e1 (plan = 2 nodes) → d (tail) → e2 (plan = 2 nodes). max_nodes = 3.
    let graph = Graph {
        nodes: vec![
            expand_node("e1", vec![]),
            mc_dep("d", Dep::hard("e1")),
            expand_node("e2", vec![Dep::hard("d")]),
        ],
    };
    let plan2 = Graph {
        nodes: vec![mc("p", None), mc("q", Some("p"))],
    };

    // Run 1: e1 expands (2 nodes: calls 1,2), then d fails (call 3). e2 never runs.
    // Journal carries PlanExpanded{e1} (2 nodes).
    let (gw1, _c1) = failing_after_gateway(2).await;
    let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_planner(Arc::new(FixedPlanner(plan2.clone())))
        .with_max_nodes(3);
    let o1 = exec1.run(run, &graph).await.expect("run 1 yields an outcome");
    assert!(o1.failed.is_some(), "run 1 fails at d: {o1:?}");

    // Run 2: resume seeds the node counter from the journal (=2). e1 replays (no
    // re-count); d succeeds; e2 expands → 2 + 2 = 4 > 3 → cap. Without seeding, e2
    // alone (2 nodes) would fit, so this assertion is what proves the cap SPANS resume.
    let (gw2, _c2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_planner(Arc::new(FixedPlanner(plan2)))
        .with_max_nodes(3);
    let err = exec2.start(run, &graph).await.expect_err("resume breaches max_nodes");
    assert!(
        matches!(&err, OrchestratorError::GlobalCapExceeded { cap, .. } if cap == "max_nodes"),
        "max_nodes breach spans resume: {err:?}"
    );
}
```

- [ ] **Step 2: Run to verify AC9 fails (AC8 already passes from Task 3's `check_expansion_budget`)**

Run: `cargo test -p sensei-orchestrator -- expand_max_expansions_cap_halts_loud expand_max_nodes_cap_spans_resume`
Expected: `expand_max_expansions_cap_halts_loud` PASSES (fresh-run counters start at 0, so the cap already works within a single run); `expand_max_nodes_cap_spans_resume` FAILS — the resume does not yet seed the counter, so e2's 2 nodes fit under 3 and the run completes instead of erroring.

- [ ] **Step 3: Add the per-run seed helper**

In `crates/orchestrator/src/executor/mod.rs`, add this method to `impl Executor` (near `pinned`):

```rust
    /// A per-run clone with FRESH expansion counters seeded to `(expansions, nodes)`
    /// — 0/0 for a fresh `run`, or the journal's expansion tally for a resume — so the
    /// caps span the crash seam and every nested `run_expand` shares one counter.
    fn with_expansion_seed(mut self, expansions: usize, nodes: usize) -> Self {
        use std::sync::atomic::AtomicUsize;
        self.expansion_counters = Arc::new(ExpansionCounters {
            expansions: AtomicUsize::new(expansions),
            nodes: AtomicUsize::new(nodes),
        });
        self
    }
```

- [ ] **Step 4: Reset the counters on a fresh run**

In `crates/orchestrator/src/executor/mod.rs`, change `run_inner` to drive off a fresh-counter clone. Replace its body:

```rust
    async fn run_inner(&self, run: RunId, graph: &Graph) -> Result<RunOutcome, OrchestratorError> {
        graph.validate_dag()?;
        let this = self.clone().with_expansion_seed(0, 0);
        this.append(
            run,
            JournalEvent::RunStarted {
                version: this.version.clone(),
            },
        )
        .await?;
        let outcome = this.drive(run, graph, &Fold::default()).await?;
        this.finalize_run(run, &outcome).await?;
        Ok(outcome)
    }
```

- [ ] **Step 5: Seed the counters from the journal on resume**

In `crates/orchestrator/src/executor/mod.rs`, in `start_inner`, replace the tail (from `self.rehydrate_context(&fold).await?;` through the `Ok(outcome)` return) with a seeded clone:

```rust
        // Seed the expansion counters from the journaled expansions so the caps span
        // the crash seam, then rehydrate + resume off that per-run clone.
        let seed_nodes: usize = fold.expansions.values().map(|g| g.nodes.len()).sum();
        let this = self
            .clone()
            .with_expansion_seed(fold.expansions.len(), seed_nodes);
        this.rehydrate_context(&fold).await?;
        let outcome = this.drive(run, graph, &fold).await?;
        this.finalize_run(run, &outcome).await?;
        Ok(outcome)
```

- [ ] **Step 6: Run to verify both cap tests pass**

Run: `cargo test -p sensei-orchestrator -- expand_max_expansions_cap_halts_loud expand_max_nodes_cap_spans_resume`
Expected: both PASS. Verify the real exit code is 0.

- [ ] **Step 7: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/mod.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-3 slice 3 (5/6) — max_expansions/max_nodes caps, journal-seeded across resume"
```

---

## Task 6: Propagation, end-to-end, and the full-suite gate

**Files:**
- Test: `crates/orchestrator/src/executor/tests.rs` (append)

**Note:** Nested-failure propagation flows through the shared `drive_nested`; the in-doubt-Mutation **pause** path is identical to the already-tested `an_in_doubt_mutation_in_a_subgraph_pauses_the_run` (same helper), so AC10 covers fail-propagation directly and adds one pause test that wraps a mutating agent in an `Expand` plan.

- [ ] **Step 1: Write the propagation + e2e tests (AC10, AC12)**

In `crates/orchestrator/src/executor/tests.rs`, append:

```rust
/// AC10 (failure): a failing node inside the produced plan fails the Expand node and
/// cascade-skips its outer hard-dependent.
#[tokio::test]
async fn a_failing_node_in_the_expand_plan_fails_the_expand() {
    // The plan's single inner node fails on the gateway's first call.
    let (gateway, _c) = failing_after_gateway(0).await;
    let graph = Graph {
        nodes: vec![expand_node("e", vec![]), mc_dep("d", Dep::hard("e"))],
    };
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_planner(Arc::new(FixedPlanner(Graph { nodes: vec![mc("boom", None)] })));
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    assert!(
        matches!(&out.failed, Some((n, _)) if n == &NodeId("e".into())),
        "nested failure fails the expand: {out:?}"
    );
    assert!(out.skipped.contains(&NodeId("d".into())), "outer hard-dependent skipped");
}

/// AC10 (pause): an in-doubt Mutation inside the Expand's plan pauses the whole run —
/// mirrors `an_in_doubt_mutation_in_a_subgraph_pauses_the_run`, wrapping the mutating
/// agent in an `Expand` plan instead of a `Subgraph`.
#[tokio::test]
async fn an_in_doubt_mutation_in_an_expand_plan_pauses_the_run() {
    let run = RunId(uuid::Uuid::new_v4());
    let mk_recorder = |sink: Arc<std::sync::Mutex<Vec<String>>>| {
        let recorder = AgentDefinition {
            name: "recorder".into(),
            area: "research".into(),
            kind: "reasoning".into(),
            chain: Some("research.bulk".into()),
            chains: std::collections::HashMap::new(),
            grants: std::collections::HashMap::new(),
            tools: vec!["record_note".into()],
            skills: vec![],
            system_prompt: "Record.".into(),
        };
        (
            Arc::new(
                Registry::default()
                    .with_agent(recorder)
                    .with_tool(RecordNote::new(sink.clone()).spec()),
            ),
            Arc::new(ToolRegistry::default().with_tool(Arc::new(RecordNote::new(sink)))),
        )
    };
    // The mutation-bearing agent lives inside an Expand plan (inner node "n1").
    let plan = Graph {
        nodes: vec![agent_node("n1", "recorder", "item-0")],
    };
    let graph = Graph {
        nodes: vec![expand_node("e", vec![])],
    };

    // Seed: run to completion, then truncate to the nested agent's record_note
    // EffectIntent (drops its EffectRecorded) → in-doubt on resume.
    let full = InMemoryJournal::new();
    let (seed_reg, seed_tools) = mk_recorder(Arc::new(std::sync::Mutex::new(Vec::new())));
    let (gw_s, _c) = demo_reference_tool_gateway().await;
    Executor::new(Arc::new(gw_s), Arc::new(full.clone()), "v1")
        .with_registry(seed_reg)
        .with_tools(seed_tools)
        .with_planner(Arc::new(FixedPlanner(plan.clone())))
        .run(run, &graph)
        .await
        .expect("seed Expand run completes");
    let events = full.load(run).await.unwrap();
    let cut = events
        .iter()
        .position(|(_, e)| matches!(e, JournalEvent::EffectIntent { .. }))
        .expect("the nested agent journaled a record_note EffectIntent");
    let seeded = InMemoryJournal::new();
    for (_, e) in &events[..=cut] {
        seeded.append(run, e.clone()).await.unwrap();
    }

    // Resume with an Indeterminate reconciler + a FRESH empty sink → the nested
    // Mutation is in-doubt → the nested agent pauses → the Expand pauses → the run pauses.
    let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let (reg, tools) = mk_recorder(sink.clone());
    let reconcilers =
        ReconcileRegistry::default().with_provider("record_note", Arc::new(AlwaysIndeterminate));
    let (gw_r, _c2) = demo_reference_tool_gateway().await;
    let outcome = Executor::new(Arc::new(gw_r), Arc::new(seeded.clone()), "v1")
        .with_registry(reg)
        .with_tools(tools)
        .with_reconcilers(Arc::new(reconcilers))
        .with_planner(Arc::new(FixedPlanner(plan)))
        .start(run, &graph)
        .await
        .expect("resume yields an outcome");

    let pause = outcome.paused.expect("the in-doubt nested Mutation pauses the whole run");
    assert_eq!(pause.node, NodeId("e".into()), "the Expand node is the pause point");
    let resumed = seeded.load(run).await.unwrap();
    assert!(
        resumed.iter().any(|(_, e)| matches!(e, JournalEvent::RunPaused { .. })),
        "RunPaused is journaled"
    );
    assert!(
        !resumed.iter().any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "the run must NOT complete over an unresolved in-doubt Intent"
    );
    assert!(sink.lock().unwrap().is_empty(), "a paused in-doubt Mutation applies no side effect");
}

/// AC12 (end-to-end): an Expand whose produced plan is a nested `Agent` node drives it
/// through the gateway; the agent's output is the Expand's sink.
#[tokio::test]
async fn expand_drives_a_produced_agent_plan_end_to_end() {
    let (gateway, _c) = recording_gateway().await;
    let registry = agent_registry("c");
    let e = NodeId("e".into());
    let graph = Graph {
        nodes: vec![expand_node("e", vec![])],
    };
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(registry)
        .with_planner(Arc::new(FixedPlanner(Graph {
            nodes: vec![agent_node("n1", "a", "hi")],
        })));
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    assert!(
        out.outputs[&e].get("n1").is_some(),
        "nested agent output is the expand sink: {}",
        out.outputs[&e]
    );
}
```

- [ ] **Step 2: Run the new tests to verify they pass**

Run: `cargo test -p sensei-orchestrator -- a_failing_node_in_the_expand_plan_fails_the_expand an_in_doubt_mutation_in_an_expand_plan_pauses_the_run expand_drives_a_produced_agent_plan_end_to_end`
Expected: all three PASS. Verify the real exit code is 0.

- [ ] **Step 3: Full-workspace gate (AC11 + AC13: no regressions, additive)**

Run: `cargo test --workspace`
Expected: PASS — the whole suite (was 967 tests) plus the new slice-3 tests, all green. Verify the **real** exit code is 0 (do NOT pipe through `tail`/`grep`).

- [ ] **Step 4: Lint gate (mirror the pre-commit hook)**

Run: `cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings`
Expected: both exit 0 (no diff, no warnings).

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): SP-3 slice 3 (6/6) — nested fail/pause propagation + agent e2e; full-suite green"
```

- [ ] **Step 6: Push**

```bash
cd /Users/Jerry/Developer/gateway
git push origin develop
```

Then verify the push landed (never trust a piped exit code):

Run: `git rev-parse HEAD origin/develop`
Expected: the two hashes are identical.

---

## Acceptance Criteria → Task map (self-review)

| Spec AC | Task | Test |
|---|---|---|
| 1 (produced plan → sink map) | 3 | `expand_drives_a_produced_plan_and_returns_the_sink_map` |
| 2 (PlanExpanded before nested work) | 3 | same test (position assertion) |
| 3 (resume reconstructs, no re-plan) | 4 | `expand_resume_uses_the_journaled_plan_not_a_re_plan` |
| 4 (completed expand + failing tail resumes) | 4 | `expand_completed_then_failing_tail_resumes_without_replan` |
| 5 (planner error → Failed) | 3 | `expand_planner_error_fails_the_node_and_cascade_skips_hard_dependents` |
| 6 (invalid plan → Failed, no PlanExpanded) | 3 | `expand_invalid_plan_fails_the_node_without_journaling_an_expansion` |
| 7 (no planner → Failed) | 3 | `expand_with_no_planner_fails_loud` |
| 8 (max_expansions cap) | 5 | `expand_max_expansions_cap_halts_loud` |
| 9 (max_nodes cap spans resume) | 5 | `expand_max_nodes_cap_spans_resume` |
| 10 (nested fail/pause propagation) | 6 | `a_failing_node_in_the_expand_plan_fails_the_expand`, `an_in_doubt_mutation_in_an_expand_plan_pauses_the_run` |
| 11 (drive_nested behavior-preserving) | 2 | existing `subgraph_*` / `branch_*` suite |
| 12 (end-to-end) | 6 | `expand_drives_a_produced_agent_plan_end_to_end` |
| 13 (additive) | 6 | `cargo test --workspace` |

---

## Post-implementation

- Update the sensei checkpoint / `MEMORY.md` topic file (`sensei-orchestrator-design.md`): slice 3 done; SP-3 remaining = slice 4 (Planner agent) → slice 5 (Coordinator + loops-of-graphs). Note the `drive_nested` helper now exists (3 callers) and the `Planner` seam is ready for the slice-4 LLM impl.
- Deferred items to carry forward (from the spec §6): real planner agent (slice 4), `Expand.input` from a predecessor/blackboard, input-hash fence on `PlanExpanded`, sibling splice (only if a real need appears), loops-of-graphs (slice 5).
