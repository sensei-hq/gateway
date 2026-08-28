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

/// Path segment reserved for a selector's own model call (`"{expand}/__select__"`),
/// the sibling of [`RESERVED_PLAN_ID`](crate::plan::RESERVED_PLAN_ID) and
/// [`RESERVED_GATE_ID`](crate::plan::RESERVED_GATE_ID). The selector's spend is
/// journaled under this path so it reaches the run's durable ledger without colliding
/// with the Expand node's own effects.
///
/// Reserved against plan node ids for the same reason `__gate__` is: a plan node with
/// this id namespaces to exactly this path, and the SP-3 s5 review showed such a
/// collision makes a resumed run fail `DeterminismViolation`.
pub const RESERVED_SELECT_ID: &str = "__select__";

/// The ONLY route a [`PlannerSelector`] has to a model.
///
/// This exists so a selector cannot hold a provider handle of its own. SP-DATA-5 put
/// every model call behind one metered chokepoint that gates on the run's budget,
/// charges the ledger and journals the spend — but the selector was written before it
/// and kept its own gateway, so it spent past the operator's cap and journaled
/// nothing. Lending the capability instead of letting the selector own one makes that
/// bypass unrepresentable rather than merely discouraged.
///
/// Deliberately narrow and gateway-free: `orchestrator-core` depends on no provider
/// crate (see [`TokenUsage`](crate::budget::TokenUsage), mirrored locally for the same
/// reason), so the port speaks text in and text out. The implementor owns building the
/// request, gating, charging and journaling.
#[async_trait]
pub trait ModelDispatch: Send + Sync {
    /// Run one metered text completion and return the model's text.
    ///
    /// `Err` is terminal for the selector: the caller must propagate it rather than
    /// falling back to a provider of its own. A refusal (an exhausted or unmeterable
    /// budget) also arrives as `Err`, already journaled by the implementor.
    async fn complete(
        &self,
        system: &str,
        user: &str,
        chain: Option<&str>,
    ) -> Result<String, OrchestratorError>;
}

/// Picks WHICH planner agent runs for a goal, from the candidate library (agents
/// whose `area == PLANNER_AREA`). Injected on the executor like [`Planner`]; slice 4B.
#[async_trait::async_trait]
pub trait PlannerSelector: Send + Sync {
    /// Choose one of `candidates` (the sorted planner library) for `goal`. Returning
    /// `Err`, or an agent not in `candidates`, is a node-level failure the executor
    /// maps to `Failed` — never a panic.
    ///
    /// `dispatch` is the only provider access an implementation gets; a selector that
    /// needs no model (see [`RulePlannerSelector`]) simply ignores it.
    async fn select(
        &self,
        goal: &serde_json::Value,
        candidates: &[AgentRef],
        dispatch: &dyn ModelDispatch,
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
        _dispatch: &dyn ModelDispatch,
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

    /// A dispatcher that must never be reached. `RulePlannerSelector` is documented as
    /// pure and goal-independent, so every one of these tests doubles as the proof that
    /// it spends no tokens: give it a model it is forbidden to call.
    struct NoDispatch;
    #[async_trait]
    impl ModelDispatch for NoDispatch {
        async fn complete(
            &self,
            _system: &str,
            _user: &str,
            _chain: Option<&str>,
        ) -> Result<String, OrchestratorError> {
            panic!("RulePlannerSelector is pure — it must never dispatch a model call")
        }
    }

    #[tokio::test]
    async fn rule_selector_prefers_a_configured_default_when_a_candidate() {
        let s = RulePlannerSelector::new(Some(AgentRef("beta".into())));
        let cands = vec![AgentRef("alpha".into()), AgentRef("beta".into())];
        let got = s
            .select(&serde_json::json!({}), &cands, &NoDispatch)
            .await
            .unwrap();
        assert_eq!(got, AgentRef("beta".into()));
    }

    #[tokio::test]
    async fn rule_selector_falls_back_to_first_candidate_when_default_absent_or_noncandidate() {
        // default not among candidates → first (candidates arrive sorted).
        let s = RulePlannerSelector::new(Some(AgentRef("ghost".into())));
        let cands = vec![AgentRef("alpha".into()), AgentRef("beta".into())];
        assert_eq!(
            s.select(&serde_json::json!({}), &cands, &NoDispatch)
                .await
                .unwrap(),
            AgentRef("alpha".into())
        );
        // no default → first.
        let s2 = RulePlannerSelector::new(None);
        assert_eq!(
            s2.select(&serde_json::json!({}), &cands, &NoDispatch)
                .await
                .unwrap(),
            AgentRef("alpha".into())
        );
    }

    #[tokio::test]
    async fn rule_selector_errors_on_empty_candidates() {
        let s = RulePlannerSelector::new(None);
        assert!(
            s.select(&serde_json::json!({}), &[], &NoDispatch)
                .await
                .is_err()
        );
    }
}
