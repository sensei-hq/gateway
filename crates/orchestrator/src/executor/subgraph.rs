//! The `Subgraph` node on the executor: drive a nested DAG under the node's path
//! in the SAME run (SP-3 slice 1). Namespacing the inner ids makes nested effects
//! nest via `effect_id`; `drive` (reused) + the fold give resume-without-re-spend.

use std::collections::{HashMap, HashSet};

use orchestrator_core::{Dep, Graph, Node, NodeId, NodeKind, OrchestratorError, RunId};

use super::{Executor, Fold, NodeExec};

/// Clone `graph` with every inner node id (and each `Dep.on`) rewritten to
/// `"{prefix}/{id}"`, plus the two *sibling references* that name another node in
/// this same graph: `Consolidate.over` and `Branch.on`. A nested `Subgraph`'s inner
/// graph and a `Branch`'s arm/`default` graphs are NOT rewritten here (they hit the
/// `other`/clone paths) — they are namespaced lazily when their `run_subgraph` /
/// `run_branch` runs, under the already-namespaced node id (so a `Branch`'s selected
/// arm ends up under `"{prefix}/{branch}/{label}/…"`).
pub(super) fn namespace_graph(prefix: &str, graph: &Graph) -> Graph {
    let ns = |id: &NodeId| NodeId(format!("{prefix}/{}", id.0));
    Graph {
        nodes: graph
            .nodes
            .iter()
            .map(|n| Node {
                id: ns(&n.id),
                kind: match &n.kind {
                    NodeKind::Consolidate {
                        over,
                        min_viable,
                        body,
                    } => NodeKind::Consolidate {
                        over: ns(over),
                        min_viable: *min_viable,
                        body: body.clone(),
                    },
                    NodeKind::Branch { on, arms, default } => NodeKind::Branch {
                        on: ns(on),
                        arms: arms.clone(),
                        default: default.clone(),
                    },
                    other => other.clone(),
                },
                deps: n
                    .deps
                    .iter()
                    .map(|d| Dep {
                        on: ns(&d.on),
                        kind: d.kind,
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// The subgraph's output: `{ sink_id: output }` for each sink (a node referenced by
/// no other node's `Dep`) that produced an output.
pub(super) fn sink_outputs(
    graph: &Graph,
    prefix: &str,
    outputs: &HashMap<NodeId, serde_json::Value>,
) -> serde_json::Value {
    let referenced: HashSet<&NodeId> = graph
        .nodes
        .iter()
        .flat_map(|n| n.deps.iter().map(|d| &d.on))
        .collect();
    let mut map = serde_json::Map::new();
    for n in &graph.nodes {
        if !referenced.contains(&n.id) {
            let key = NodeId(format!("{prefix}/{}", n.id.0));
            if let Some(v) = outputs.get(&key) {
                map.insert(n.id.0.clone(), v.clone());
            }
        }
    }
    serde_json::Value::Object(map)
}

impl Executor {
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
        Ok(NodeExec::Completed(sink_outputs(
            graph,
            prefix,
            &nested.outputs,
        )))
    }

    /// Drive a `Subgraph` node's nested DAG under `"{node}/…"` and fold the nested
    /// outcome into this node's `NodeExec`: paused ⇒ `Paused`, failed ⇒ `Failed`,
    /// else `Completed(sink map)`.
    ///
    /// Known limitation (fresh-vs-terminal asymmetry, shared with `Map`/`Loop`
    /// synthesized outputs): the `Completed` sink map is NOT journaled as an
    /// `EffectRecorded`, so a terminal re-`start` reconstructs `outputs` from the
    /// journal's per-node effects — the namespaced inner nodes (`"{node}/…"`), not
    /// the sink map under `node.id`. Captured by
    /// `re_starting_a_completed_subgraph_run_returns_the_folded_outcome`; not fixed
    /// in slice 1.
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
}
