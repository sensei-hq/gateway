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
///
/// **Two shapes, one accounting rule.** [`complete`](Self::complete) is a text
/// completion; [`choose`](Self::choose) is a System One **decision** — typed questions
/// answered with probabilities instead of prose (gh#72). Both route through the same
/// implementor chokepoint, so a decision call is gated, charged, journaled and replayed
/// exactly like a completion. A selector picks whichever shape its chain serves; neither
/// is privileged here.
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

    /// Run one metered System One **choice** decision: pick one of
    /// [`ChoiceQuestion::options`] for a shared `state`.
    ///
    /// The same contract as [`complete`](Self::complete) — `Err` is terminal, a budget
    /// refusal arrives already journaled, and the implementor owns building, gating,
    /// charging and journaling. Unlike `complete` the answer is not text: it is the
    /// chosen option's key plus a confidence, so a caller gets a value it can match on
    /// instead of prose to parse. It must still check that the key is one it offered —
    /// a provider can invent one, and redaction can change one.
    ///
    /// **Only the `choice` question shape is exposed, and only because a consumer
    /// exists.** System One also serves `noul` (a probability) and `score` (an ordered
    /// level); neither has a consumer in this crate yet, and a port that speaks shapes
    /// nobody asks for is a surface with no test behind it. The implementor maps this
    /// onto the full typed question set; extending the port is additive when a consumer
    /// needs another shape.
    /// `order` is the caller's **measured** preference over the models this call may run
    /// on — which model answers this kind of question best (see [`ChoiceOrder`]). An
    /// implementor must APPLY it or fail: answering without it would silently discard the
    /// caller's evidence, which is a routing decision the caller did not make. `None` is
    /// the whole of the previous behaviour, byte for byte.
    async fn choose(
        &self,
        question: &ChoiceQuestion,
        chain: Option<&str>,
        order: Option<&ChoiceOrder>,
    ) -> Result<ChoiceAnswer, OrchestratorError>;
}

/// One option a [`ChoiceQuestion`] offers: a stable key the caller can match on, and an
/// optional human-readable description the model sees beside it.
///
/// The key IS the answer contract — it is what comes back in [`ChoiceAnswer::choice`] —
/// so callers should not have to reinterpret a description, and the implementor must
/// pass both through unaltered (the description only helps the model choose).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChoiceOption {
    pub key: String,
    pub description: Option<String>,
}

/// A System One `choice` decision: one shared `state`, one instruction, and the menu of
/// options to choose from.
///
/// `state` is deliberately a `serde_json::Value` rather than a string so a caller can
/// pass structured evidence (a goal, a finding, a diff) without flattening it first;
/// the implementor is responsible for handing it to the provider in a form the provider
/// accepts.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ChoiceQuestion {
    pub state: serde_json::Value,
    pub instructions: String,
    pub options: Vec<ChoiceOption>,
}

/// The answer to a [`ChoiceQuestion`]: the winning option's key and the probability mass
/// concentrated on it.
///
/// `confidence` is **concentration, not correctness** — it is uncalibrated, a confident
/// answer can be wrong, and nothing should treat it as accuracy or gate on it as if it
/// were (the gateway's `DecisionAnswer::Choice` carries the same warning).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ChoiceAnswer {
    pub choice: String,
    pub confidence: f64,
}

/// One position in a [`ChoiceOrder`]: a model reference.
///
/// A ref matches a candidate iff every PRESENT field equals the candidate's; an absent
/// field is a wildcard, so `ChoiceRef { model: Some("x".into()), .. }` means "x, on
/// whichever router serves it". This mirrors the gateway's own candidate-ref grammar
/// for the same reason [`TokenUsage`](crate::budget::TokenUsage) is mirrored here: this
/// crate depends on no gateway type, so the port speaks the shape and the implementor
/// translates it.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChoiceRef {
    pub router: Option<String>,
    pub model: Option<String>,
}

/// A caller-supplied, **measured** preference over the models a decision may run on:
/// which one answers this kind of question best, best first.
///
/// **It orders survivors; it never admits or resurrects.** Whatever the implementor's
/// gates skipped — a closed breaker, a cooldown, an exhausted budget, a window too small
/// — stays skipped however highly it is ranked. A ref naming a model the deployment does
/// not serve is inert: a measurement's roster drifts from a tenant's availability by
/// design, and refusing the call over the difference would make the ranking unusable
/// wherever it is most useful.
///
/// It carries provenance because an order without one cannot be explained after the
/// fact: a run's trace should be able to say which category's measurement, from which
/// artifact revision, produced this sequence. Nothing in the ordering reads those fields.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChoiceOrder {
    /// What this order was measured for, e.g. `"planner.select"`. Free-form here — the
    /// implementor records it and never matches on it.
    pub category: String,
    /// Best first.
    pub preferred: Vec<ChoiceRef>,
    /// The artifact the rows came from, e.g. `"catalog.model_rankings"`. `None` means the
    /// caller does not know, and that is reported as unknown rather than invented.
    pub artifact: Option<String>,
    /// The artifact's monotonic revision, when it has one.
    pub revision: Option<u64>,
    /// Unix seconds the artifact was generated, when it says.
    pub generated_at: Option<i64>,
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
/// else the FIRST candidate — so the caller's ordering IS the policy. Goal-independent.
///
/// **What "first" means (SP-REG-3).** `Executor::planner_candidates` is the only
/// producer of that slice, and it orders an agent marked `default_planner` ahead of the
/// rest, which keep name order. So the first candidate is the DESIGNATED planner when a
/// registry designates one, and the alphabetically-first planner only when none does.
/// This said "candidates arrive sorted by name" until the marker shipped; a maintainer
/// answering "which planner runs with no `default` configured?" from that sentence
/// would have got the wrong answer, and re-derived a name-only sort on the next
/// refactor — silently un-shipping the designation.
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
        async fn choose(
            &self,
            _question: &ChoiceQuestion,
            _chain: Option<&str>,
            _order: Option<&ChoiceOrder>,
        ) -> Result<ChoiceAnswer, OrchestratorError> {
            panic!("RulePlannerSelector is pure — it must never dispatch a decision call")
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
