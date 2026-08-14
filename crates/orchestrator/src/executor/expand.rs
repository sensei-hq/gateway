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
    /// Drive a resolved planner agent's sub-run under `"{node}/__plan__"` and parse its
    /// final answer as a `PlannedGraph`. Returns `Terminal` for the node-level outcomes
    /// (unknown agent / parse error / agent Failed → `Failed`; agent Paused → `Paused`),
    /// so callers (`Agent` and `Select` arms) short-circuit uniformly. A fatal
    /// `drive_agent` error (`DeterminismViolation`, journal) `?`-propagates as a hard halt.
    pub(super) async fn drive_planner_agent(
        &self,
        run: RunId,
        node: &Node,
        agent_ref: &orchestrator_core::AgentRef,
        input: &serde_json::Value,
        fold: &Fold,
    ) -> Result<PlanOutcome, OrchestratorError> {
        if self.registry.agent(&agent_ref.0).is_none() {
            return Ok(PlanOutcome::Terminal(
                self.expand_failed(
                    run,
                    node,
                    format!("expand {} unknown planner agent {}", node.id.0, agent_ref.0),
                )
                .await?,
            ));
        }
        let plan_node = NodeId(format!("{}/__plan__", node.id.0));
        match self
            .drive_agent(run, &plan_node, agent_ref, input, &[], fold, None)
            .await?
        {
            AgentStep::Completed(out) => {
                let text = out.get("text").and_then(|v| v.as_str()).unwrap_or_default();
                match orchestrator_core::parse_plan(text) {
                    Ok(p) => Ok(PlanOutcome::Plan(p)),
                    Err(e) => Ok(PlanOutcome::Terminal(
                        self.expand_failed(
                            run,
                            node,
                            format!("expand {} plan parse: {e:?}", node.id.0),
                        )
                        .await?,
                    )),
                }
            }
            AgentStep::Failed(msg) => Ok(PlanOutcome::Terminal(
                self.expand_failed(
                    run,
                    node,
                    format!("expand {} planner agent failed: {msg}", node.id.0),
                )
                .await?,
            )),
            AgentStep::Paused(r) => Ok(PlanOutcome::Terminal(NodeExec::Paused {
                reason: format!("planner {} paused: {r}", node.id.0),
            })),
        }
    }

    /// Drive an `Expand` node: produce → `feasible` → cap-check → journal → drive.
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
                        match self
                            .drive_planner_agent(run, node, agent_ref, input, fold)
                            .await?
                        {
                            PlanOutcome::Plan(p) => p,
                            PlanOutcome::Terminal(ne) => return Ok(ne),
                        }
                    }
                    orchestrator_core::PlannerRef::Select => {
                        // RESUME: reuse the recorded pick; the selector is NOT re-invoked.
                        let agent = match fold.selections.get(&node.id) {
                            Some(a) => a.clone(),
                            None => {
                                let candidates = self.planner_candidates();
                                if candidates.is_empty() {
                                    return self
                                        .expand_failed(
                                            run,
                                            node,
                                            format!(
                                                "expand {}: no planner agents (area==planning)",
                                                node.id.0
                                            ),
                                        )
                                        .await;
                                }
                                let Some(selector) = &self.selector else {
                                    return self
                                        .expand_failed(
                                            run,
                                            node,
                                            format!(
                                                "expand {}: Select planner but no selector wired",
                                                node.id.0
                                            ),
                                        )
                                        .await;
                                };
                                let a = match selector.select(input, &candidates).await {
                                    Ok(a) => a,
                                    Err(e) => {
                                        return self
                                            .expand_failed(
                                                run,
                                                node,
                                                format!("expand {} selector: {e}", node.id.0),
                                            )
                                            .await;
                                    }
                                };
                                if !candidates.contains(&a) {
                                    return self
                                        .expand_failed(
                                            run,
                                            node,
                                            format!(
                                                "expand {} selector picked non-candidate {}",
                                                node.id.0, a.0
                                            ),
                                        )
                                        .await;
                                }
                                self.append(
                                    run,
                                    JournalEvent::PlannerSelected {
                                        node: node.id.clone(),
                                        agent: a.clone(),
                                    },
                                )
                                .await?;
                                a
                            }
                        };
                        match self
                            .drive_planner_agent(run, node, &agent, input, fold)
                            .await?
                        {
                            PlanOutcome::Plan(p) => p,
                            PlanOutcome::Terminal(ne) => return Ok(ne),
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

/// The result of driving a planner: a produced plan, or a terminal `NodeExec` the
/// planner sub-run already decided (Failed/Paused).
pub(super) enum PlanOutcome {
    Plan(PlannedGraph),
    Terminal(NodeExec),
}
