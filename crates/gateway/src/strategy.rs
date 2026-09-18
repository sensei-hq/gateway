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

/// The default (SP-ROUTE-1). Groups by `priority`, orders groups ascending, and
/// within each group draws a full permutation weighted by `(1/cost²) × reliability`.
///
/// Grouping is what makes price weighting coherent here. OpenRouter weights
/// across PROVIDERS of one model, which are interchangeable; a gateway chain
/// holds different MODELS, which are not. Equal priority is the one signal an
/// operator has for "these are interchangeable", so that is the only scope
/// weighting is applied at. With distinct priorities every group is a singleton
/// and this is exactly `PriorityStrategy` — which is why it is safe as a default.
pub struct GroupedWeightedStrategy;

enum Weight {
    /// Costs nothing, or so little that `1/cost²` overflows — indistinguishable
    /// at that price. Always ahead of anything priced.
    Free,
    Draw(f64),
    /// Weight zero — unreachable by a draw, so it goes last.
    Zero,
}

fn classify(cost: f64, reliability: f64) -> Weight {
    if cost <= 0.0 {
        return Weight::Free;
    }
    let base = 1.0 / (cost * cost);
    if !base.is_finite() {
        return Weight::Free;
    }
    let w = base * reliability;
    if w > 0.0 {
        Weight::Draw(w)
    } else {
        Weight::Zero
    }
}

impl RoutingStrategy for GroupedWeightedStrategy {
    fn order(&self, admitted: &mut Vec<SelectedModel>, ctx: &StrategyCtx<'_>) {
        // Stable, so equal priorities end up adjacent IN CHAIN ORDER — the input
        // order the free/zero buckets below preserve.
        admitted.sort_by_key(|m| m.priority);

        let mut rest = std::mem::take(admitted);
        let mut out = Vec::with_capacity(rest.len());
        while !rest.is_empty() {
            let p = rest[0].priority;
            let split = rest
                .iter()
                .position(|m| m.priority != p)
                .unwrap_or(rest.len());
            let group: Vec<SelectedModel> = rest.drain(..split).collect();
            out.extend(order_group(group, ctx));
        }
        *admitted = out;
    }
}

fn order_group(group: Vec<SelectedModel>, ctx: &StrategyCtx<'_>) -> Vec<SelectedModel> {
    let mut free = Vec::new();
    let mut zero = Vec::new();
    let mut pool: Vec<(f64, SelectedModel)> = Vec::new();

    for m in group {
        let cost = m.cost_estimate.as_ref().map(|c| c.estimated).unwrap_or(0.0);
        // `success_rate` reads 0.0 BOTH when every attempt failed and when no
        // attempt has cast a verdict yet. `verdict_samples` is the only way to
        // tell those apart, and the difference is not cosmetic: without the
        // filter a cold process weighs every candidate 0.0 and a healthy fleet
        // routes as though every provider were dead.
        let reliability = ctx
            .perf
            .stats(&m.endpoint_key())
            .filter(|s| s.verdict_samples > 0)
            .map(|s| s.success_rate)
            .unwrap_or(1.0);
        match classify(cost, reliability) {
            Weight::Free => free.push(m),
            Weight::Zero => zero.push(m),
            Weight::Draw(w) => pool.push((w, m)),
        }
    }

    let mut out = free;
    while !pool.is_empty() {
        let total: f64 = pool.iter().map(|(w, _)| *w).sum();
        let mut u = (ctx.rng.next_u64() as f64 / u64::MAX as f64) * total;
        let mut idx = pool.len() - 1;
        for (i, (w, _)) in pool.iter().enumerate() {
            u -= *w;
            if u <= 0.0 {
                idx = i;
                break;
            }
        }
        out.push(pool.remove(idx).1);
    }
    out.extend(zero);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gates::performance::{EndpointPerformanceRead, EndpointStats, NoPerformance};
    use crate::random::SplitMix64;
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

    fn sm_cost(model: &str, priority: u8, cost: Option<f64>) -> SelectedModel {
        let mut m = sm(model, priority);
        m.cost_estimate = cost.map(|estimated| crate::types::cost::CostEstimate {
            estimated,
            minimum: estimated,
            maximum: estimated,
            currency: "USD".to_string(),
            model: model.to_string(),
        });
        m
    }

    fn names(v: &[SelectedModel]) -> Vec<String> {
        v.iter().map(|m| m.model.clone()).collect()
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

    /// AC1 — THE safety test for the whole feature. With distinct priorities
    /// every group is a singleton, so the weighted default must be
    /// indistinguishable from `PriorityStrategy` on every chain that exists
    /// today (`assemble()` reassigns ascending 1-based priorities by position,
    /// so it never ties).
    ///
    /// Across MANY seeds: a single seed would pass against a strategy that
    /// happened to shuffle the same way once.
    #[test]
    fn distinct_priorities_select_identically_to_priority_order_on_every_seed() {
        for seed in 0..256u64 {
            let mut weighted = vec![
                sm_cost("c", 3, Some(0.5)),
                sm_cost("a", 1, Some(9.0)),
                sm_cost("b", 2, None),
            ];
            let mut baseline = weighted.clone();

            let rng = SplitMix64::seeded(seed);
            GroupedWeightedStrategy.order(&mut weighted, &test_ctx(&NoPerformance, &rng));
            PriorityStrategy.order(&mut baseline, &test_ctx(&NoPerformance, &rng));

            assert_eq!(
                names(&weighted),
                names(&baseline),
                "seed {seed}: a distinct-priority chain must not be perturbed"
            );
        }
    }

    /// AC2 — the WEIGHTING, not merely "it varies". A test asserting only that
    /// order changes would pass against uniform shuffling.
    ///
    /// At 1 vs 3, weights are 1/1 and 1/9, so the cheap model leads 9 times in 10.
    #[test]
    fn a_tied_group_is_weighted_by_inverse_square_price() {
        let rng = SplitMix64::seeded(0xC0FFEE);
        let mut cheap_first = 0;
        const N: usize = 4000;
        for _ in 0..N {
            let mut v = vec![
                sm_cost("dear", 1, Some(3.0)),
                sm_cost("cheap", 1, Some(1.0)),
            ];
            GroupedWeightedStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));
            if v[0].model == "cheap" {
                cheap_first += 1;
            }
        }
        let share = cheap_first as f64 / N as f64;
        assert!(
            (share - 0.9).abs() < 0.03,
            "expected the 1-unit model to lead ~9/10, got {share}"
        );
    }

    /// AC3 — free beats every price, on every seed. `1/0²` is undefined; the
    /// limit is "always first", and that is what this pins.
    ///
    /// BOTH input orders, and that half is load-bearing rather than thorough.
    /// A zero cost that escapes the free bucket becomes `Draw(inf)`, and the
    /// draw loop's arithmetic then degenerates: `total` is `inf`, `u` is `inf`
    /// (or `NaN` on a zero draw), and every `u <= 0.0` comparison against a
    /// non-finite `u` is false — so the loop never breaks and silently falls
    /// through to its `idx = pool.len() - 1` initialiser. With `free` written
    /// last that fallback picks it ANYWAY, and a single-order test passes with
    /// the entire free classification deleted (verified: both the `cost <= 0.0`
    /// and the `!base.is_finite()` guard can be removed together and a
    /// `free`-last fixture still goes green). Asserting the reversed order too
    /// means no fixed-index fallback can satisfy both.
    #[test]
    fn a_free_candidate_leads_its_group_on_every_seed() {
        for seed in 0..256u64 {
            for free_first in [false, true] {
                let priced = sm_cost("priced", 1, Some(0.000_001));
                let free = sm_cost("free", 1, None);
                let mut v = if free_first {
                    vec![free, priced]
                } else {
                    vec![priced, free]
                };
                let rng = SplitMix64::seeded(seed);
                GroupedWeightedStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));
                assert_eq!(v[0].model, "free", "seed {seed}, free_first {free_first}");
            }
        }
    }

    /// AC3, other half — a candidate whose every observed attempt failed has
    /// weight zero and can never be DRAWN, so it goes last rather than being
    /// dropped. The breaker, not the router, is what removes a candidate.
    #[test]
    fn a_zero_reliability_candidate_goes_last_but_is_never_dropped() {
        struct DeadEndpoint;
        impl EndpointPerformanceRead for DeadEndpoint {
            fn stats(&self, endpoint: &str) -> Option<EndpointStats> {
                endpoint.ends_with(":dead").then_some(EndpointStats {
                    samples: 10,
                    throughput_samples: 0,
                    // MUST be non-zero, or this fixture means "no verdicts yet"
                    // rather than "every attempt failed" — which is exactly the
                    // distinction `verdict_samples` exists to make.
                    verdict_samples: 10,
                    mean_latency_ms: 100.0,
                    mean_tokens_per_sec: 0.0,
                    success_rate: 0.0,
                })
            }
        }
        for seed in 0..64u64 {
            let mut v = vec![sm_cost("dead", 1, Some(0.1)), sm_cost("live", 1, Some(9.0))];
            let rng = SplitMix64::seeded(seed);
            GroupedWeightedStrategy.order(&mut v, &test_ctx(&DeadEndpoint, &rng));
            assert_eq!(names(&v), vec!["live", "dead"], "seed {seed}");
        }
    }

    /// The other side of `verdict_samples`, and the one that would take down a
    /// healthy fleet rather than one endpoint.
    ///
    /// An endpoint with live samples but NO verdict yet reports
    /// `success_rate: 0.0` — byte-identical to one whose every attempt failed.
    /// Reading that number without checking `verdict_samples` gives it weight
    /// zero, dropping it into the never-drawn bucket. On a cold process that is
    /// EVERY candidate, so a healthy fleet would route as though every provider
    /// were dead. Unmeasured must weigh 1.0.
    #[test]
    fn an_endpoint_with_no_verdict_yet_is_weighted_as_healthy() {
        struct AcquiredOnly;
        impl EndpointPerformanceRead for AcquiredOnly {
            fn stats(&self, _endpoint: &str) -> Option<EndpointStats> {
                Some(EndpointStats {
                    samples: 5,
                    throughput_samples: 0,
                    verdict_samples: 0, // no verdict has been cast
                    mean_latency_ms: 100.0,
                    mean_tokens_per_sec: 0.0,
                    success_rate: 0.0, // the fallback, NOT "everything failed"
                })
            }
        }
        // Prices 1 and 3, so with reliability 1.0 the cheap one leads ~9/10.
        // If unmeasured endpoints were weighed 0.0 instead, BOTH would be
        // zero-weight and the ratio would collapse to stable input order.
        let rng = SplitMix64::seeded(0xFEED);
        let mut cheap_first = 0;
        const N: usize = 2000;
        for _ in 0..N {
            let mut v = vec![
                sm_cost("dear", 1, Some(3.0)),
                sm_cost("cheap", 1, Some(1.0)),
            ];
            GroupedWeightedStrategy.order(&mut v, &test_ctx(&AcquiredOnly, &rng));
            if v[0].model == "cheap" {
                cheap_first += 1;
            }
        }
        let share = cheap_first as f64 / N as f64;
        assert!(
            (share - 0.9).abs() < 0.04,
            "unmeasured must weigh 1.0, so price weighting still applies; got {share}"
        );
    }

    /// Groups never interleave: a priority-2 candidate cannot precede a
    /// priority-1 one however cheap it is. This is the claim that keeps the
    /// weighting from inverting an operator's authored intent.
    #[test]
    fn a_cheaper_lower_priority_candidate_never_jumps_its_group() {
        for seed in 0..64u64 {
            let mut v = vec![
                sm_cost("dear_first", 1, Some(100.0)),
                sm_cost("cheap_second", 2, Some(0.01)),
            ];
            let rng = SplitMix64::seeded(seed);
            GroupedWeightedStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));
            assert_eq!(v[0].model, "dear_first", "seed {seed}");
        }
    }
}
