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
                    .expand_failed(
                        run,
                        node,
                        format!("expand {} planner failed: {e}", node.id.0),
                    )
                    .await;
            }
        };
        if let Err(e) = g.validate_dag() {
            return self
                .expand_failed(
                    run,
                    node,
                    format!("expand {} produced an invalid plan: {e}", node.id.0),
                )
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
