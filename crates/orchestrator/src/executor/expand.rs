//! The `Expand` node (SP-3 slice 3): produce a nested subgraph at runtime via the
//! injected `Planner`, journal it as `PlanExpanded`, and drive it under the node's
//! path — reusing the shared `drive_nested`. A planner error, an invalid produced
//! graph, or no planner is a node `Failed` (journaled `NodeFailed`); a cap breach is
//! a hard `Err` (self-DoS). On resume, a node with a journaled `PlanExpanded` reuses
//! that subgraph from the fold — the planner is never re-invoked (deterministic).

use orchestrator_core::{JournalEvent, Node, NodeKind, OrchestratorError, RunId};

use super::{Executor, Fold, NodeExec};

impl Executor {
    /// Drive an `Expand` node: produce → validate → cap-check → journal → drive.
    ///
    /// Known limitation (fresh-vs-terminal asymmetry, shared with `run_subgraph`/
    /// `run_branch` and the `Map`/`Loop` synthesized outputs): the `Completed` sink
    /// map is NOT journaled as an `EffectRecorded`, so a terminal re-`start`
    /// reconstructs `outputs` from the journal's per-node effects — the namespaced
    /// inner nodes (`"{node}/…"`), not the sink map under `node.id`. The `PlanExpanded`
    /// event only reconstructs the graph *structure*, not the folded sink output.
    pub(super) async fn run_expand(
        &self,
        run: RunId,
        node: &Node,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        let NodeKind::Expand { input, planner } = &node.kind else {
            unreachable!("run_expand on non-Expand node");
        };
        // `planner` is bound for Task 4 (the `PlannerRef::Agent` dispatch); this
        // slice only drives the injected/slice-3 path. Task 4 removes this line.
        let _ = planner;
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
                            .expand_failed(
                                run,
                                node,
                                format!("expand {} planner failed: {e}", node.id.0),
                            )
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
                        node_plans: std::collections::HashMap::new(),
                    },
                )
                .await?;
                produced
            }
        };
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
