//! The `Subgraph` node on the executor: drive a nested DAG under the node's path
//! in the SAME run (SP-3 slice 1). Namespacing the inner ids makes nested effects
//! nest via `effect_id`; `drive` (reused) + the fold give resume-without-re-spend.

use std::collections::{HashMap, HashSet};

use orchestrator_core::{Dep, Graph, Node, NodeId, NodeKind, OrchestratorError, RunId};

use super::{Executor, Fold, NodeExec};

/// Clone `graph` with every inner node id (and each `Dep.on`, and `Consolidate.over`)
/// rewritten to `"{prefix}/{id}"`. A nested `Subgraph`'s own inner graph is NOT
/// rewritten here (the `other => other.clone()` arm) — it is namespaced when its
/// `run_subgraph` runs, under the already-namespaced prefix.
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
        // Depth cap (self-DoS backstop): the path segment count is the current
        // nesting level; a top-level subgraph node has 0 segments (level 1). Reject
        // loud if the nested nodes would exceed max_depth. Conservative — the path
        // count also includes Map/Loop child nesting.
        let depth = node.id.0.matches('/').count();
        if depth + 1 > self.max_depth {
            return Err(OrchestratorError::GlobalCapExceeded {
                cap: "max_depth".into(),
                limit: self.max_depth,
            });
        }
        let NodeKind::Subgraph { graph } = &node.kind else {
            unreachable!("run_subgraph on non-Subgraph node");
        };
        let inner = namespace_graph(&node.id.0, graph);
        // `Box::pin` breaks the recursive `async fn` cycle
        // (run_node → run_subgraph → drive → run_node): a recursive async call
        // needs heap indirection to keep the future's size finite.
        let nested = Box::pin(self.drive(run, &inner, fold)).await?;
        if let Some(p) = nested.paused {
            return Ok(NodeExec::Paused {
                reason: format!("subgraph {} paused: {}", node.id.0, p.reason),
            });
        }
        if let Some((n, msg)) = nested.failed {
            return Ok(NodeExec::Failed {
                message: format!("subgraph {} failed at {}: {}", node.id.0, n.0, msg),
                output: None,
            });
        }
        Ok(NodeExec::Completed(sink_outputs(
            graph,
            &node.id.0,
            &nested.outputs,
        )))
    }
}
