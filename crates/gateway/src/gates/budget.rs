use super::{AdmissionGate, CandidateView, GateVerdict, SelectionCtx};
use crate::skip_reason::SkipReason;
use crate::types::config::ModelConfig;

/// Gate: the candidate's estimated cost must not exceed the caller's budget.
///
/// Estimate = `input_tokens * input_per_1k / 1000 + max_output_tokens *
/// output_per_1k / 1000`, mirroring `ModelSelectionService::estimate_cost` in
/// `selection.rs`. A model with no `pricing` has no estimate, so it always
/// admits (free/unmetered).
pub struct BudgetGate;

impl BudgetGate {
    fn estimate(mc: &ModelConfig, input_tokens: u32) -> Option<f64> {
        let p = mc.pricing.as_ref()?;
        Some(
            input_tokens as f64 * p.input_per_1k / 1000.0
                + mc.max_output_tokens as f64 * p.output_per_1k / 1000.0,
        )
    }
}

impl AdmissionGate for BudgetGate {
    fn name(&self) -> &'static str {
        "budget"
    }

    fn evaluate(&self, c: &CandidateView<'_>, x: &SelectionCtx<'_>) -> GateVerdict {
        match (
            x.budget,
            Self::estimate(c.model_config, x.input_tokens.unwrap_or(0)),
        ) {
            (Some(b), Some(est)) if est > b => GateVerdict::Skip(SkipReason::OverBudget {
                estimated: est,
                budget: b,
            }),
            _ => GateVerdict::Admit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gates::EndpointHealthRead;
    use crate::types::capability::Capability;
    use crate::types::config::{GatewayConfig, ModelPricing, RouterConfig};
    use std::collections::HashMap;
    use std::time::Instant;

    struct NeverOpen;
    impl EndpointHealthRead for NeverOpen {
        fn open_until(&self, _endpoint: &str) -> Option<Instant> {
            None
        }
    }

    /// Router health port stub for tests that don't exercise cooldown.
    struct NeverCooling;
    impl crate::gates::RouterHealthRead for NeverCooling {
        fn cooling_until(&self, _router: &str) -> Option<Instant> {
            None
        }
    }

    fn priced_model_config() -> ModelConfig {
        ModelConfig {
            id: "claude-haiku".to_string(),
            api_model_id: None,
            provider: "anthropic".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: 200000,
            max_output_tokens: 8192,
            pricing: Some(ModelPricing {
                input_per_1k: 0.0008,
                output_per_1k: 0.004,
                per_request: None,
            }),
        }
    }

    fn unpriced_model_config() -> ModelConfig {
        ModelConfig {
            id: "gemma3:27b".to_string(),
            api_model_id: None,
            provider: "ollama".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: 128000,
            max_output_tokens: 8192,
            pricing: None,
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

    fn ctx<'a>(
        gateway_config: &'a GatewayConfig,
        health: &'a dyn EndpointHealthRead,
        budget: Option<f64>,
        input_tokens: Option<u32>,
    ) -> SelectionCtx<'a> {
        SelectionCtx {
            capability: Capability::TextChat,
            budget,
            input_tokens,
            health,
            now: Instant::now(),
            config: gateway_config,
            router_health: &NeverCooling,
        }
    }

    #[test]
    fn skips_when_estimate_exceeds_budget() {
        let model_config = priced_model_config();
        let router_config = test_router_config();
        let cand = CandidateView {
            model: "claude-haiku",
            router: "anthropic",
            endpoint: "anthropic:claude-haiku".to_string(),
            model_config: &model_config,
            router_config: &router_config,
        };
        let gateway_config = GatewayConfig::default();
        let health = NeverOpen;
        let x = ctx(&gateway_config, &health, Some(0.001), Some(1000));

        let verdict = BudgetGate.evaluate(&cand, &x);
        assert!(matches!(
            verdict,
            GateVerdict::Skip(SkipReason::OverBudget { .. })
        ));
    }

    #[test]
    fn admits_when_no_budget_specified() {
        let model_config = priced_model_config();
        let router_config = test_router_config();
        let cand = CandidateView {
            model: "claude-haiku",
            router: "anthropic",
            endpoint: "anthropic:claude-haiku".to_string(),
            model_config: &model_config,
            router_config: &router_config,
        };
        let gateway_config = GatewayConfig::default();
        let health = NeverOpen;
        let x = ctx(&gateway_config, &health, None, Some(1000));

        let verdict = BudgetGate.evaluate(&cand, &x);
        assert!(matches!(verdict, GateVerdict::Admit));
    }

    #[test]
    fn admits_when_model_has_no_pricing() {
        let model_config = unpriced_model_config();
        let router_config = test_router_config();
        let cand = CandidateView {
            model: "gemma3:27b",
            router: "ollama",
            endpoint: "ollama:gemma3:27b".to_string(),
            model_config: &model_config,
            router_config: &router_config,
        };
        let gateway_config = GatewayConfig::default();
        let health = NeverOpen;
        // Even a tiny budget can't be exceeded by an unpriced (free) model.
        let x = ctx(&gateway_config, &health, Some(0.0001), Some(1_000_000));

        let verdict = BudgetGate.evaluate(&cand, &x);
        assert!(matches!(verdict, GateVerdict::Admit));
    }
}
