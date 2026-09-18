use crate::gates::performance::EndpointPerformanceRead;
use crate::random::RandomSource;
use crate::selection::SelectedModel;

/// What an ordering strategy may consult beyond the candidates themselves.
pub struct StrategyCtx<'a> {
    pub perf: &'a dyn EndpointPerformanceRead,
    pub rng: &'a dyn RandomSource,
    /// The minimum count of the counter appropriate to the metric being sorted
    /// on, before a metric sort trusts it as "measured" rather than falling
    /// back: `EndpointStats::verdict_samples` for `success_rate`,
    /// `EndpointStats::samples` for latency, `EndpointStats::throughput_samples`
    /// for throughput. These three counters are independent (see their doc on
    /// `EndpointStats`) — comparing this threshold against the wrong one is a
    /// live hazard, not a cosmetic mismatch: a gate written as
    /// `stats.samples >= min_samples` before trusting `success_rate` would
    /// treat a `StreamAcquired`-only endpoint (`verdict_samples == 0`,
    /// `success_rate == 0.0`) as measured-and-totally-unreliable, zeroing out
    /// a healthy endpoint that has simply never completed a request.
    pub min_samples: u32,
}

/// Orders admitted candidates. The single ordering seam.
pub trait RoutingStrategy: Send + Sync {
    fn order(&self, admitted: &mut Vec<SelectedModel>, ctx: &StrategyCtx<'_>);
}

/// Strict ascending priority, stable. Retained as the explicit baseline every
/// other strategy is compared against in tests.
pub struct PriorityStrategy;
impl RoutingStrategy for PriorityStrategy {
    fn order(&self, admitted: &mut Vec<SelectedModel>, _ctx: &StrategyCtx<'_>) {
        admitted.sort_by_key(|m| m.priority); // stable; identical to resolve_chain's sort today
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::config::{ModelConfig, RouterConfig};
    use std::collections::HashMap;

    // Build a minimal SelectedModel with the given model id + priority, mirroring
    // the field construction used in selection.rs's own tests.
    fn sm(model: &str, priority: u8) -> SelectedModel {
        let router_config = RouterConfig {
            url: "http://localhost".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        };
        let model_config = ModelConfig {
            id: model.to_string(),
            api_model_id: None,
            provider: "test".to_string(),
            family: None,
            capabilities: vec![],
            context_window: 0,
            max_output_tokens: 0,
            pricing: None,
            catalog: None,
        };
        SelectedModel {
            model: model.to_string(),
            router: "test".to_string(),
            router_config,
            model_config,
            api_model_id: model.to_string(),
            priority,
            cost_estimate: None,
        }
    }

    fn test_ctx<'a>(
        perf: &'a dyn EndpointPerformanceRead,
        rng: &'a dyn RandomSource,
    ) -> StrategyCtx<'a> {
        StrategyCtx {
            perf,
            rng,
            min_samples: 3,
        }
    }

    #[test]
    fn priority_strategy_sorts_ascending_by_priority() {
        let perf = crate::gates::performance::NoPerformance;
        let rng = crate::random::SplitMix64::seeded(1);
        let mut v = vec![sm("b", 2), sm("a", 1)];
        PriorityStrategy.order(&mut v, &test_ctx(&perf, &rng));
        assert_eq!(v[0].model, "a");
        assert_eq!(v[1].model, "b");
    }

    #[test]
    fn priority_strategy_is_stable_for_equal_priority() {
        let perf = crate::gates::performance::NoPerformance;
        let rng = crate::random::SplitMix64::seeded(1);
        let mut v = vec![sm("first", 1), sm("second", 1)];
        PriorityStrategy.order(&mut v, &test_ctx(&perf, &rng));
        assert_eq!(v[0].model, "first"); // stable: equal keys keep input order
        assert_eq!(v[1].model, "second");
    }
}
