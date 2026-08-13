//! The `Branch` node: a deterministic conditional (SP-3 slice 2). Test predecessor
//! `on`'s memoized output with a pure `BranchCond`, run the first matching arm (else
//! `default`) as a nested graph under `"{branch}/{label}/…"` — reusing the subgraph
//! namespace/drive/sink machinery. Pure over `on`'s output ⇒ resume recomputes the
//! same arm, no branch journaling.

use std::collections::HashMap;

use orchestrator_core::{Graph, Node, NodeId, NodeKind, OrchestratorError, RunId};

use super::{Executor, Fold, NodeExec};

impl Executor {
    /// Drive the selected arm as a nested graph and fold its outcome into a
    /// `NodeExec` (`Completed` = the arm's sink map; paused/failed carried up).
    ///
    /// Output note: the `Completed` sink map's keys are the SELECTED arm's local
    /// sink ids, so they VARY by arm. A downstream consumer that needs a stable key
    /// should give every arm a common sink node id (e.g. `result`).
    ///
    /// Known limitation (shared with [`run_subgraph`](Self::run_subgraph)): the
    /// synthesized sink map is NOT journaled as an `EffectRecorded`, so a terminal
    /// re-`start` reconstructs the arm's per-node outputs (the namespaced inner ids
    /// `"{branch}/{label}/…"`), not the sink map under `node.id` — the same
    /// fresh-vs-terminal asymmetry documented on `run_subgraph`.
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
}
