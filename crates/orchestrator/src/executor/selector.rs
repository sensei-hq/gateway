//! `LlmPlannerSelector` (SP-3 s4B): a light reasoning call that picks one planner agent
//! from the candidate menu for a goal. A black box — its call is NOT a journaled effect;
//! only its result is (`PlannerSelected`), so a mid-plan crash before that event re-runs
//! one cheap call, and after it, resume reuses the recorded agent.

use std::sync::Arc;

use gateway::Gateway;
use kernel::types::capability::Capability;
use kernel::types::request::{InferenceRequest, Message, MessageRole, Payload};
use orchestrator_core::{AgentRef, OrchestratorError, PlannerSelector, Registry};

/// Picks a planner via one `gateway.execute` over `chain`; parses the response content
/// as the chosen agent name (validated against `candidates` by the caller). The menu it
/// renders describes each candidate's capability (`name (area/kind)`, looked up in the
/// registry) so the reasoning call sees what each planner IS, not just its name.
pub struct LlmPlannerSelector {
    gateway: Arc<Gateway>,
    registry: Arc<Registry>,
    chain: String,
}

impl LlmPlannerSelector {
    pub fn new(gateway: Arc<Gateway>, registry: Arc<Registry>, chain: impl Into<String>) -> Self {
        Self {
            gateway,
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
        let req = InferenceRequest {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some(self.chain.clone()),
            payload: Payload::Chat {
                messages: vec![Message::text(MessageRole::User, user)],
                system: Some(system.to_string()),
                max_tokens: None,
                temperature: None,
                tools: Vec::new(),
            },
            budget: None,
            auth: None,
            panel: None,
            consensus: None,
            allow_fallback: true,
            credentials: Default::default(),
        };
        let resp = self
            .gateway
            .execute(&req)
            .await
            .map_err(|e| OrchestratorError::Gateway(e.to_string()))?;
        let name = resp.content.unwrap_or_default().trim().to_string();
        if name.is_empty() {
            return Err(OrchestratorError::Gateway(
                "planner selector returned empty content".into(),
            ));
        }
        Ok(AgentRef(name))
    }
}
