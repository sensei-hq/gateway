use super::{AdmissionGate, CandidateView, GateVerdict, SelectionCtx};
use crate::skip_reason::SkipReason;

/// Gate: the candidate's `context_window` must be able to hold this request's estimated
/// input.
///
/// The sixth gate, and the one that makes the orchestrator's pre-dispatch check
/// unnecessary. That check tested the prompt against `min_context_window(chain)` — the
/// SMALLEST window in the chain — and failed the node terminally, so a chain of
/// `[gpt-4o 128k, fallback 8k]` refused a 20k prompt the primary would have served. Here
/// the question is asked per CANDIDATE, which is the only place it has a correct answer.
///
/// The estimate read is `input_tokens_pessimistic`, NOT the cost gate's `input_tokens`:
/// the cost figure omits tool schemas and divides by 4, so judging on it would admit
/// exactly the requests this gate exists to catch. See
/// `engine::util::estimate_input_tokens_pessimistic`.
///
/// `None` admits, matching `BudgetGate`'s treatment of a model with no pricing: an absent
/// estimate is not evidence of a problem, and a gate that skipped on missing data would
/// refuse every request that did not carry one.
///
/// Strictly `>`: a request that exactly fills the window is admitted. It leaves no room
/// for output, but bounding output is the SP-DATA-5 clamp's job (it caps `max_tokens` by
/// the window), and duplicating that judgement here would skip candidates the clamp can
/// still serve. Please do not "fix" this to `>=`.
pub struct ContextWindowGate;

impl AdmissionGate for ContextWindowGate {
    fn name(&self) -> &'static str {
        "context_window"
    }

    fn evaluate(&self, c: &CandidateView<'_>, x: &SelectionCtx<'_>) -> GateVerdict {
        match x.input_tokens_pessimistic {
            Some(est) if est > c.model_config.context_window => {
                GateVerdict::Skip(SkipReason::OverContextWindow {
                    estimated: est,
                    window: c.model_config.context_window,
                })
            }
            _ => GateVerdict::Admit,
        }
    }
}

#[cfg(test)]
mod tests {
    // `super::*` already carries `AdmissionGate` / `CandidateView` / `GateVerdict` /
    // `SelectionCtx` / `SkipReason` through this module's own imports.
    use super::*;
    use crate::gates::EndpointHealthRead;
    use crate::types::capability::Capability;
    use crate::types::config::{GatewayConfig, ModelConfig, RouterConfig};
    use std::collections::HashMap;
    use std::time::Instant;

    struct NeverOpen;
    impl EndpointHealthRead for NeverOpen {
        fn open_until(&self, _endpoint: &str) -> Option<Instant> {
            None
        }
    }

    struct NeverCooling;
    impl crate::gates::RouterHealthRead for NeverCooling {
        fn cooling_until(&self, _router: &str) -> Option<Instant> {
            None
        }
    }

    struct NeverLocked;
    impl crate::gates::lockout::ModelLockoutRead for NeverLocked {
        fn locked(&self, _endpoint: &str) -> Option<crate::gates::lockout::LockView> {
            None
        }
    }

    fn model_with_window(context_window: u32) -> ModelConfig {
        ModelConfig {
            id: "some-model".to_string(),
            api_model_id: None,
            provider: "anthropic".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window,
            max_output_tokens: 4096,
            pricing: None,
            catalog: None,
        }
    }

    fn test_router_config() -> RouterConfig {
        RouterConfig {
            url: "http://localhost".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        }
    }

    fn cand<'a>(mc: &'a ModelConfig, rc: &'a RouterConfig) -> CandidateView<'a> {
        CandidateView {
            model: "some-model",
            router: "anthropic",
            endpoint: "anthropic:some-model".to_string(),
            model_config: mc,
            router_config: rc,
        }
    }

    /// `input_tokens` (the COST estimate) and `input_tokens_pessimistic` (this gate's)
    /// are taken separately so a test can set them to different values and pin which one
    /// the gate reads.
    fn ctx<'a>(
        cfg: &'a GatewayConfig,
        health: &'a dyn EndpointHealthRead,
        input_tokens: Option<u32>,
        input_tokens_pessimistic: Option<u32>,
    ) -> SelectionCtx<'a> {
        SelectionCtx {
            capability: Capability::TextChat,
            budget: None,
            input_tokens,
            input_tokens_pessimistic,
            health,
            now: Instant::now(),
            config: cfg,
            router_health: &NeverCooling,
            model_lockout: &NeverLocked,
        }
    }

    /// AC5 — a missing estimate ADMITS. The gate is not a filter on absent data.
    ///
    /// Mirrors `BudgetGate`, which admits a model with no pricing: an absent estimate is
    /// not evidence of a problem, and skipping on it would refuse every request that did
    /// not carry one — which is every caller that reaches selection by a path other than
    /// `engine::execute`.
    #[test]
    fn no_estimate_admits() {
        let mc = model_with_window(8_192);
        let rc = test_router_config();
        let cfg = GatewayConfig::default();
        let health = NeverOpen;
        assert!(matches!(
            ContextWindowGate.evaluate(&cand(&mc, &rc), &ctx(&cfg, &health, None, None)),
            GateVerdict::Admit
        ));
    }

    /// A request inside the window admits; one over it is skipped with BOTH numbers.
    ///
    /// `8_192` admitting and `8_193` skipping pins the boundary as `est > window`, not
    /// `est >= window`. A request that exactly fills the window leaves no room for
    /// output — but bounding output is the SP-DATA-5 clamp's job, and skipping here for
    /// that reason would refuse candidates the clamp can still serve.
    #[test]
    fn over_window_skips_and_under_window_admits() {
        let mc = model_with_window(8_192);
        let rc = test_router_config();
        let cfg = GatewayConfig::default();
        let health = NeverOpen;
        assert!(
            matches!(
                ContextWindowGate.evaluate(&cand(&mc, &rc), &ctx(&cfg, &health, None, Some(8_192))),
                GateVerdict::Admit
            ),
            "a request that exactly fills the window must be admitted"
        );
        match ContextWindowGate.evaluate(&cand(&mc, &rc), &ctx(&cfg, &health, None, Some(8_193))) {
            GateVerdict::Skip(SkipReason::OverContextWindow { estimated, window }) => {
                assert_eq!(estimated, 8_193);
                assert_eq!(window, 8_192);
            }
            GateVerdict::Skip(other) => panic!("expected an OverContextWindow skip, got {other}"),
            GateVerdict::Admit => panic!("8193 tokens must not be admitted to an 8192 window"),
        }
    }

    /// The gate reads the PESSIMISTIC estimate, never the cost one.
    ///
    /// The two fields exist precisely because they differ — the cost estimate omits tool
    /// schemas and divides by 4 — so a gate reading `input_tokens` would compile, pass
    /// the other two tests, and still admit exactly the over-window requests this slice
    /// was written to catch. Both directions are asserted, because reading the wrong
    /// field fails in both.
    #[test]
    fn the_gate_reads_the_pessimistic_estimate_not_the_cost_one() {
        let mc = model_with_window(8_192);
        let rc = test_router_config();
        let cfg = GatewayConfig::default();
        let health = NeverOpen;

        // Cost estimate says "tiny"; the pessimistic one says "over". Must skip.
        match ContextWindowGate
            .evaluate(&cand(&mc, &rc), &ctx(&cfg, &health, Some(1), Some(20_000)))
        {
            GateVerdict::Skip(SkipReason::OverContextWindow { estimated, .. }) => {
                assert_eq!(
                    estimated, 20_000,
                    "the skip must record the pessimistic figure it judged on"
                );
            }
            _ => panic!("a pessimistic estimate of 20000 must skip an 8192 window"),
        }

        // Cost estimate says "over"; the pessimistic one is absent. Must admit — AC5
        // applies to the field the gate owns, and the cost estimate is not its business.
        assert!(matches!(
            ContextWindowGate.evaluate(&cand(&mc, &rc), &ctx(&cfg, &health, Some(20_000), None)),
            GateVerdict::Admit
        ));
    }
}
