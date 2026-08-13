//! The `Expand` node (SP-3 slice 3/4A): produce a nested subgraph at runtime — via
//! the injected `Planner` (`PlannerRef::Injected`) or a journaled ReAct planner agent
//! (`PlannerRef::Agent`, slice 4A) — run the pure `feasible` gate, journal it as
//! `PlanExpanded`, and drive it under the node's path (reusing the shared
//! `drive_nested`). A planner error, an infeasible plan, an unparseable/failed planner
//! agent, or no wired planner is a node `Failed` (journaled `NodeFailed`); a paused
//! planner turn pauses the run; a cap breach is a hard `Err` (self-DoS). On resume, a
//! node with a journaled `PlanExpanded` reuses that subgraph from the fold — the
//! planner is never re-invoked (deterministic).

use orchestrator_core::{
    JournalEvent, Node, NodeId, NodeKind, OrchestratorError, PlannedGraph, RunId,
};

use super::{AgentStep, Executor, Fold, NodeExec};

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
        // RESUME: a node with a journaled `PlanExpanded` reuses that subgraph — the
        // planner is NOT re-invoked (determinism §4.4). FRESH: produce (the injected
        // `Planner` trait, or a journaled ReAct planner agent) → feasibility →
        // cap-check → journal (in that order), then drive.
        let g = match fold.expansions.get(&node.id) {
            Some(journaled) => journaled.clone(),
            None => {
                let produced = match planner {
                    orchestrator_core::PlannerRef::Injected => {
                        // Slice-3 path: the injected `Planner` trait (no metadata).
                        let Some(p) = &self.planner else {
                            return self
                                .expand_failed(
                                    run,
                                    node,
                                    format!("expand {}: no planner wired", node.id.0),
                                )
                                .await;
                        };
                        match p.plan(input).await {
                            Ok(graph) => PlannedGraph {
                                graph,
                                node_plans: std::collections::HashMap::new(),
                            },
                            Err(e) => {
                                return self
                                    .expand_failed(
                                        run,
                                        node,
                                        format!("expand {} planner failed: {e}", node.id.0),
                                    )
                                    .await;
                            }
                        }
                    }
                    orchestrator_core::PlannerRef::Agent(agent_ref) => {
                        // A journaled ReAct planner sub-run under `"{expand}/__plan__"`:
                        // its turns are Pure effects (replayed from the memo on a
                        // mid-plan resume); its final answer parses as the produced plan.
                        let plan_node = NodeId(format!("{}/__plan__", node.id.0));
                        match self
                            .drive_agent(run, &plan_node, agent_ref, input, &[], fold, None)
                            .await
                        {
                            Ok(AgentStep::Completed(out)) => {
                                let text =
                                    out.get("text").and_then(|v| v.as_str()).unwrap_or_default();
                                match orchestrator_core::parse_plan(text) {
                                    Ok(p) => p,
                                    Err(e) => {
                                        return self
                                            .expand_failed(
                                                run,
                                                node,
                                                format!("expand {} plan parse: {e:?}", node.id.0),
                                            )
                                            .await;
                                    }
                                }
                            }
                            Ok(AgentStep::Failed(msg)) => {
                                return self
                                    .expand_failed(
                                        run,
                                        node,
                                        format!("expand {} planner agent failed: {msg}", node.id.0),
                                    )
                                    .await;
                            }
                            Ok(AgentStep::Paused(r)) => {
                                return Ok(NodeExec::Paused {
                                    reason: format!("planner {} paused: {r}", node.id.0),
                                });
                            }
                            // An unresolvable planner agent (unknown agent) is a config
                            // error → node Failed, not a hard halt.
                            Err(e) => {
                                return self
                                    .expand_failed(
                                        run,
                                        node,
                                        format!("expand {} planner unavailable: {e}", node.id.0),
                                    )
                                    .await;
                            }
                        }
                    }
                };
                // Feasibility subsumes the slice-3 `validate_dag` (its Structural check),
                // and additionally rejects reserved ids, over-cap plans, and dangling
                // agent/skill/tool refs — the deterministic gate before journaling.
                if let Err(errs) =
                    orchestrator_core::feasible(&produced, &self.registry, self.max_nodes)
                {
                    return self
                        .expand_failed(
                            run,
                            node,
                            format!("expand {} infeasible plan: {errs:?}", node.id.0),
                        )
                        .await;
                }
                self.check_expansion_budget(&produced.graph)?;
                self.append(
                    run,
                    JournalEvent::PlanExpanded {
                        node: node.id.clone(),
                        subgraph: produced.graph.clone(),
                        node_plans: produced.node_plans,
                    },
                )
                .await?;
                produced.graph
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
