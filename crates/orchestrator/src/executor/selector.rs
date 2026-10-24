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

use orchestrator_core::{
    AgentRef, ChoiceOption, ChoiceQuestion, ModelDispatch, OrchestratorError, PlannerSelector,
    Registry,
};

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

/// `DecisionPlannerSelector` — the same choice as [`LlmPlannerSelector`], asked as a
/// System One **decision** instead of a text completion.
///
/// The menu is the candidate set (one `ChoiceOption` per planner, keyed by the agent
/// name), so the answer arrives as an option key rather than prose that has to be
/// trimmed and hoped over — the failure class `LlmPlannerSelector` carries by design,
/// where "I recommend beta because…" produces no usable selection at all. The caller
/// still validates the answer against the menu (`expand.rs`): a provider can invent a
/// key, and a redactor can change one.
/// `confidence` is NOT consulted — it is probability concentration, not correctness
/// (see [`ChoiceAnswer`](orchestrator_core::ChoiceAnswer)), so gating on it would make
/// the selector's behaviour depend on an uncalibrated number.
///
/// It reaches a provider only through the lent [`ModelDispatch`], exactly like the
/// text selector: the decision is gated on the run's budget, charged to the live meter,
/// journaled to the durable ledger and replayed from its memo on a re-drive. Its chain
/// must be a **Decision** chain (the gateway serves `Decision` capability models —
/// `nimble`, `clef`, the Laya checkpoints, …), not a chat chain.
///
/// A single candidate short-circuits without a call. That is not only cheaper: a
/// System One choice needs at least two options, so a one-option menu would be an
/// `InvalidRequest` at the gateway rather than an answer.
pub struct DecisionPlannerSelector {
    registry: Arc<Registry>,
    chain: String,
}

impl DecisionPlannerSelector {
    pub fn new(registry: Arc<Registry>, chain: impl Into<String>) -> Self {
        Self {
            registry,
            chain: chain.into(),
        }
    }
}

#[async_trait::async_trait]
impl PlannerSelector for DecisionPlannerSelector {
    async fn select(
        &self,
        goal: &serde_json::Value,
        candidates: &[AgentRef],
        dispatch: &dyn ModelDispatch,
    ) -> Result<AgentRef, OrchestratorError> {
        match candidates {
            [] => {
                return Err(OrchestratorError::RegistryLoad(
                    "no planner candidates to select from".into(),
                ));
            }
            [only] => return Ok(only.clone()),
            _ => {}
        }

        let options = candidates
            .iter()
            .map(|a| ChoiceOption {
                key: a.0.clone(),
                // The same `area/kind` the text selector renders, so the two menus
                // describe a candidate identically even though they ask differently.
                description: self
                    .registry
                    .agent(&a.0)
                    .map(|def| format!("{}/{}", def.area, def.kind)),
            })
            .collect();
        let question = ChoiceQuestion {
            state: decision_state(goal),
            instructions: "Choose the single planner agent best suited to the goal. \
                The option keys are the agent names."
                .into(),
            options,
        };
        let answer = dispatch.choose(&question, Some(&self.chain)).await?;
        Ok(AgentRef(answer.choice))
    }
}

/// The decision `state` for a goal: the goal itself when the gateway accepts it as one
/// (a non-blank string, an object or an array), else wrapped so it is valid.
///
/// A bare scalar — a number, a bool, `null` — is rejected by the gateway's structural
/// validation before any candidate is tried, and an `Expand` input is author-supplied
/// `serde_json::Value`, so "the goal is an object" is a convention rather than a type.
/// Unwrapping the common case keeps the model's `state` from being `{"goal":{"goal":…}}`.
fn decision_state(goal: &serde_json::Value) -> serde_json::Value {
    match goal {
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => goal.clone(),
        serde_json::Value::String(s) if !s.trim().is_empty() => goal.clone(),
        _ => serde_json::json!({ "goal": goal }),
    }
}
