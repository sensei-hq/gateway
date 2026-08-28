//! The `Expand` node (SP-3 slice 3/4A/4B): produce a nested subgraph at runtime — via
//! the injected `Planner` (`PlannerRef::Injected`), a journaled ReAct planner agent
//! (`PlannerRef::Agent`, slice 4A), or a registry-driven selection
//! (`PlannerRef::Select`, slice 4B: the executor's injected `PlannerSelector` picks a
//! `planning`-area agent from the sorted candidate library, journaled as
//! `PlannerSelected` and reused on resume, then that agent runs the 4A path) — run the
//! pure `feasible` gate, journal it as `PlanExpanded`, and drive it under the node's
//! path (reusing the shared `drive_nested`). A planner error, an infeasible plan, an
//! unparseable/failed planner agent, no wired planner/selector, or a selector error /
//! non-candidate pick is a node `Failed` (journaled `NodeFailed`); a paused planner turn
//! pauses the run; a cap breach is a hard `Err` (self-DoS). On resume, a node with a
//! journaled `PlanExpanded` reuses that subgraph from the fold; a `Select` node that
//! crashed after `PlannerSelected` but before `PlanExpanded` reuses the recorded pick —
//! the planner/selector is never re-invoked (deterministic).

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
        node_id: &NodeId,
        agent_ref: &orchestrator_core::AgentRef,
        input: &serde_json::Value,
        fold: &Fold,
    ) -> Result<PlanOutcome, OrchestratorError> {
        if self.registry.agent(&agent_ref.0).is_none() {
            return Ok(PlanOutcome::Terminal(
                self.expand_failed(
                    run,
                    node_id,
                    format!("expand {} unknown planner agent {}", node_id.0, agent_ref.0),
                )
                .await?,
            ));
        }
        let plan_node = NodeId(format!("{}/__plan__", node_id.0));
        match self
            .drive_agent(run, &plan_node, agent_ref, input, &[], fold, None, false)
            .await?
        {
            AgentStep::Completed(out) => {
                let text = out.get("text").and_then(|v| v.as_str()).unwrap_or_default();
                match orchestrator_core::parse_plan(text) {
                    Ok(p) => Ok(PlanOutcome::Plan(p)),
                    Err(e) => Ok(PlanOutcome::Terminal(
                        self.expand_failed(
                            run,
                            node_id,
                            format!("expand {} plan parse: {e:?}", node_id.0),
                        )
                        .await?,
                    )),
                }
            }
            AgentStep::Failed(msg) => Ok(PlanOutcome::Terminal(
                self.expand_failed(
                    run,
                    node_id,
                    format!("expand {} planner agent failed: {msg}", node_id.0),
                )
                .await?,
            )),
            AgentStep::Paused(r) => Ok(PlanOutcome::Terminal(NodeExec::Paused {
                reason: format!("planner {} paused: {r}", node_id.0),
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
        self.drive_expand_with(run, &node.id, input, planner, fold)
            .await
    }

    /// The Expand pipeline keyed by an arbitrary `path` (a node id OR a Loop iteration
    /// path `"{loop}/{i}"`): resume via the fold's expansion/selection at `path`, else
    /// produce (Injected/Agent/Select) → `feasible` → cap-check → journal
    /// `PlanExpanded{node: path}` → `drive_nested`. `run_expand` and a Loop-`Expand`
    /// body iteration share this.
    pub(super) async fn drive_expand_with(
        &self,
        run: RunId,
        path: &NodeId,
        input: &serde_json::Value,
        planner: &orchestrator_core::PlannerRef,
        fold: &Fold,
    ) -> Result<NodeExec, OrchestratorError> {
        // RESUME: a path with a journaled `PlanExpanded` reuses that subgraph — the
        // planner is NOT re-invoked (determinism §4.4). FRESH: produce (the injected
        // `Planner` trait, or a journaled ReAct planner agent) → feasibility →
        // cap-check → journal (in that order), then drive.
        let g = match fold.expansions.get(path) {
            Some(journaled) => journaled.clone(),
            None => {
                let produced = match planner {
                    orchestrator_core::PlannerRef::Injected => {
                        // Slice-3 path: the injected `Planner` trait (no metadata).
                        let Some(p) = &self.planner else {
                            return self
                                .expand_failed(
                                    run,
                                    path,
                                    format!("expand {}: no planner wired", path.0),
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
                                        path,
                                        format!("expand {} planner failed: {e}", path.0),
                                    )
                                    .await;
                            }
                        }
                    }
                    orchestrator_core::PlannerRef::Agent(agent_ref) => {
                        match self
                            .drive_planner_agent(run, path, agent_ref, input, fold)
                            .await?
                        {
                            PlanOutcome::Plan(p) => p,
                            PlanOutcome::Terminal(ne) => return Ok(ne),
                        }
                    }
                    orchestrator_core::PlannerRef::Select => {
                        // RESUME: reuse the recorded pick; the selector is NOT re-invoked.
                        let agent = match fold.selections.get(path) {
                            Some(a) => a.clone(),
                            None => {
                                let candidates = self.planner_candidates();
                                if candidates.is_empty() {
                                    return self
                                        .expand_failed(
                                            run,
                                            path,
                                            format!(
                                                "expand {}: no planner agents (area==planning)",
                                                path.0
                                            ),
                                        )
                                        .await;
                                }
                                let Some(selector) = &self.selector else {
                                    return self
                                        .expand_failed(
                                            run,
                                            path,
                                            format!(
                                                "expand {}: Select planner but no selector wired",
                                                path.0
                                            ),
                                        )
                                        .await;
                                };
                                // The selector reaches a model ONLY through this lent
                                // capability, so its call is gated on the run's budget,
                                // charged to the live meter and journaled to the ledger
                                // like every other producer.
                                let dispatch = super::dispatch::SelectorDispatch::new(
                                    self,
                                    run,
                                    path.clone(),
                                    fold,
                                );
                                let selected = selector.select(input, &candidates, &dispatch).await;
                                // The lent capability makes the EXECUTOR's own integrity
                                // failures — a memo `DeterminismViolation`, an unreadable
                                // `ContentDigestMiss` — reachable through an arbitrary
                                // selector's return value. At every other producer that
                                // check is a hard halt; it is one here too. Read BEFORE
                                // the selector's own result, so a selector that swallows
                                // or rewraps the error cannot downgrade an inconsistent
                                // journal into a soft `NodeFailed` and leave the drive
                                // running (and spending) on the strength of it.
                                if let Some(fatal) = dispatch.take_fatal() {
                                    return Err(fatal);
                                }
                                let a = match selected {
                                    Ok(a) => a,
                                    Err(e) => {
                                        // A budget refusal is NOT a node failure: it is
                                        // already journaled, and pause-vs-fail is the
                                        // chokepoint's call, not this arm's.
                                        return match dispatch.take_refusal() {
                                            Some(super::dispatch::RefusalKind::Paused(reason)) => {
                                                Ok(NodeExec::Paused { reason })
                                            }
                                            Some(super::dispatch::RefusalKind::Failed(message)) => {
                                                Ok(NodeExec::Failed {
                                                    message,
                                                    output: None,
                                                })
                                            }
                                            None => {
                                                self.expand_failed(
                                                    run,
                                                    path,
                                                    format!("expand {} selector: {e}", path.0),
                                                )
                                                .await
                                            }
                                        };
                                    }
                                };
                                if !candidates.contains(&a) {
                                    return self
                                        .expand_failed(
                                            run,
                                            path,
                                            format!(
                                                "expand {} selector picked non-candidate {}",
                                                path.0, a.0
                                            ),
                                        )
                                        .await;
                                }
                                self.append(
                                    run,
                                    JournalEvent::PlannerSelected {
                                        node: path.clone(),
                                        agent: a.clone(),
                                    },
                                )
                                .await?;
                                a
                            }
                        };
                        match self
                            .drive_planner_agent(run, path, &agent, input, fold)
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
                            path,
                            format!("expand {} infeasible plan: {errs:?}", path.0),
                        )
                        .await;
                }
                self.check_expansion_budget(&produced.graph)?;
                self.append(
                    run,
                    JournalEvent::PlanExpanded {
                        node: path.clone(),
                        subgraph: produced.graph.clone(),
                        node_plans: produced.node_plans,
                    },
                )
                .await?;
                produced.graph
            }
        };
        self.drive_nested(run, "expand", &path.0, &g, fold).await
    }

    /// Journal a `NodeFailed` for an `Expand` node then return `Failed` — so a
    /// planner/plan failure is durable + surfaced (no silent failure) and
    /// cascade-skips hard-dependents, matching the `ModelCall` gateway-fail path.
    async fn expand_failed(
        &self,
        run: RunId,
        node_id: &NodeId,
        message: String,
    ) -> Result<NodeExec, OrchestratorError> {
        self.append(
            run,
            JournalEvent::NodeFailed {
                node: node_id.clone(),
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
