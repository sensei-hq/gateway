//! The injected planner seam (SP-3 slice 3): a node's runtime graph producer.
//! Slice 3 ships test/stub impls; slice 4 drops in the LLM-backed planner agent.

use async_trait::async_trait;

use crate::error::OrchestratorError;
use crate::graph::Graph;

/// Produces a nested subgraph at runtime for a [`NodeKind::Expand`](crate::graph::NodeKind::Expand)
/// node. The returned `Graph` carries LOCAL ids (namespaced under the node at drive
/// time). Returning `Err` — or a graph that fails `validate_dag` — is a node-level
/// failure the executor maps to `Failed`, never a panic.
#[async_trait]
pub trait Planner: Send + Sync {
    async fn plan(&self, input: &serde_json::Value) -> Result<Graph, OrchestratorError>;
}

use crate::registry::AgentRef;

/// The agent `area` marking a candidate planner for `PlannerRef::Select` (§4.4).
pub const PLANNER_AREA: &str = "planning";

/// Picks WHICH planner agent runs for a goal, from the candidate library (agents
/// whose `area == PLANNER_AREA`). Injected on the executor like [`Planner`]; slice 4B.
#[async_trait::async_trait]
pub trait PlannerSelector: Send + Sync {
    /// Choose one of `candidates` (the sorted planner library) for `goal`. Returning
    /// `Err`, or an agent not in `candidates`, is a node-level failure the executor
    /// maps to `Failed` — never a panic.
    async fn select(
        &self,
        goal: &serde_json::Value,
        candidates: &[AgentRef],
    ) -> Result<AgentRef, OrchestratorError>;
}

/// A pure, deterministic selector: the configured `default` when it is a candidate,
/// else the first candidate (candidates arrive sorted by name). Goal-independent.
pub struct RulePlannerSelector {
    default: Option<AgentRef>,
}
impl RulePlannerSelector {
    pub fn new(default: Option<AgentRef>) -> Self {
        Self { default }
    }
}
#[async_trait::async_trait]
impl PlannerSelector for RulePlannerSelector {
    async fn select(
        &self,
        _goal: &serde_json::Value,
        candidates: &[AgentRef],
    ) -> Result<AgentRef, OrchestratorError> {
        if let Some(d) = &self.default
            && candidates.contains(d)
        {
            return Ok(d.clone());
        }
        candidates.first().cloned().ok_or_else(|| {
            OrchestratorError::RegistryLoad("no planner candidates to select from".into())
        })
    }
}

#[cfg(test)]
mod selector_tests {
    use super::*;
    use crate::registry::AgentRef;

    #[tokio::test]
    async fn rule_selector_prefers_a_configured_default_when_a_candidate() {
        let s = RulePlannerSelector::new(Some(AgentRef("beta".into())));
        let cands = vec![AgentRef("alpha".into()), AgentRef("beta".into())];
        let got = s.select(&serde_json::json!({}), &cands).await.unwrap();
        assert_eq!(got, AgentRef("beta".into()));
    }

    #[tokio::test]
    async fn rule_selector_falls_back_to_first_candidate_when_default_absent_or_noncandidate() {
        // default not among candidates → first (candidates arrive sorted).
        let s = RulePlannerSelector::new(Some(AgentRef("ghost".into())));
        let cands = vec![AgentRef("alpha".into()), AgentRef("beta".into())];
        assert_eq!(
            s.select(&serde_json::json!({}), &cands).await.unwrap(),
            AgentRef("alpha".into())
        );
        // no default → first.
        let s2 = RulePlannerSelector::new(None);
        assert_eq!(
            s2.select(&serde_json::json!({}), &cands).await.unwrap(),
            AgentRef("alpha".into())
        );
    }

    #[tokio::test]
    async fn rule_selector_errors_on_empty_candidates() {
        let s = RulePlannerSelector::new(None);
        assert!(s.select(&serde_json::json!({}), &[]).await.is_err());
    }
}
