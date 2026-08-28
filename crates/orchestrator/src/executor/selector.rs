//! `LlmPlannerSelector` (SP-3 s4B): a light reasoning call that picks one planner agent
//! from the candidate menu for a goal. Its RESULT is journaled (`PlannerSelected`), so a
//! mid-plan crash before that event re-runs one cheap call and after it, resume reuses
//! the recorded agent.
//!
//! Its SPEND is journaled too, as of the budget-completeness pass. This selector
//! originally held its own `Arc<Gateway>` and called `execute()` directly, which made it
//! the one model-call producer outside SP-DATA-5's metered chokepoint: it spent past the
//! operator's cap and left no ledger entry, so the overshoot was invisible on resume as
//! well. It now reaches a provider ONLY through the borrowed
//! [`ModelDispatch`](orchestrator_core::ModelDispatch) the executor lends it — a
//! capability it cannot widen, so the bypass is unrepresentable rather than merely
//! discouraged.

use std::sync::Arc;

use orchestrator_core::{AgentRef, ModelDispatch, OrchestratorError, PlannerSelector, Registry};

/// Picks a planner via one metered completion; parses the response content as the chosen
/// agent name (validated against `candidates` by the caller). The menu it renders
/// describes each candidate's capability (`name (area/kind)`, looked up in the registry)
/// so the reasoning call sees what each planner IS, not just its name.
///
/// Holds no provider handle: the model arrives as a borrowed capability per call.
pub struct LlmPlannerSelector {
    registry: Arc<Registry>,
    chain: String,
}

impl LlmPlannerSelector {
    pub fn new(registry: Arc<Registry>, chain: impl Into<String>) -> Self {
        Self {
            registry,
            chain: chain.into(),
        }
    }
}

#[async_trait::async_trait]
impl PlannerSelector for LlmPlannerSelector {
    async fn select(
        &self,
        goal: &serde_json::Value,
        candidates: &[AgentRef],
        dispatch: &dyn ModelDispatch,
    ) -> Result<AgentRef, OrchestratorError> {
        let menu = candidates
            .iter()
            .map(|a| match self.registry.agent(&a.0) {
                Some(def) => format!("- {} ({}/{})", a.0, def.area, def.kind),
                None => format!("- {}", a.0),
            })
            .collect::<Vec<_>>()
            .join("\n");
        let system = "Choose the single best planner agent for the goal. \
            Answer with ONLY the exact agent name from the list.";
        let user = format!("Goal:\n{goal}\n\nPlanner agents:\n{menu}");
        let content = dispatch.complete(system, &user, Some(&self.chain)).await?;
        let name = content.trim().to_string();
        if name.is_empty() {
            return Err(OrchestratorError::Gateway(
                "planner selector returned empty content".into(),
            ));
        }
        Ok(AgentRef(name))
    }
}
