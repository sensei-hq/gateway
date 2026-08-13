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
                message: format!(
                    "branch {} arm {} failed at {}: {}",
                    node.id.0, label, n.0, msg
                ),
                output: None,
            });
        }
        Ok(NodeExec::Completed(sink_outputs(
            selected,
            &prefix,
            &nested.outputs,
        )))
    }
}
