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
        //
        // Gated at `ctx.min_samples`, not merely at `> 0`, and against
        // `verdict_samples` specifically — the counter `StrategyCtx::min_samples`
        // names for `success_rate`. At `> 0` a SINGLE failed request drives
        // `success_rate` to 0.0, which is `Weight::Zero`, which is last in the
        // group until the whole window rolls — one unlucky attempt banishing a
        // healthy endpoint. Below the threshold a candidate weighs 1.0: the same
        // "unmeasured is healthy" rule, applied consistently rather than only at
        // exactly zero samples.
        let reliability = ctx
            .perf
            .stats(&m.endpoint_key())
            .filter(|s| s.verdict_samples >= ctx.min_samples)
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
        // `classify` guards `base`, but this guards their SUM, and the two are
        // not the same claim. Two candidates at ~1e-154 each yield a perfectly
        // finite `1e308`, pass every guard in `classify`, and only overflow when
        // ADDED — at which point `u` is `inf` (or `NaN` on a zero draw), every
        // `u <= 0.0` is false, the loop below never breaks, and the draw
        // silently collapses to its `idx` initialiser: the same candidate every
        // single time. At that magnitude the weights are indistinguishable
        // anyway, so fall back to CHAIN ORDER — the operator's authored tiebreak,
        // and the same rule the free/zero buckets follow — rather than to a
        // fixed index. Pinned by
        // `an_overflowing_weight_sum_falls_back_to_chain_order_in_both_input_orders`.
        if !total.is_finite() {
            out.extend(pool.drain(..).map(|(_, m)| m));
            break;
        }
        let mut u = (ctx.rng.next_u64() as f64 / u64::MAX as f64) * total;
        // Unreachable by construction, and deliberately the LAST index rather
        // than the first. `u` starts in `[0, total]` and the loop subtracts
        // every weight, whose sum IS `total`, so the final subtraction drives it
        // to `<= 0` and breaks — the initialiser survives only a float-rounding
        // tie on that last element, where the last index is also the correct
        // answer. Do not expect a mutation to `0` to be caught: with the
        // overflow guard above intercepting the non-finite case, no input
        // reaches this line. Before that guard it WAS reachable, and it silently
        // returned the last candidate on every draw — which is the defect the
        // guard fixes, not one this default should paper over.
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

    /// Every endpoint has failed every verdict it cast, with a CONFIGURABLE
    /// number of verdicts — so a fixture can sit either side of
    /// `ctx.min_samples` while holding `success_rate` fixed at 0.0. That is the
    /// only way to pin the threshold: the rate alone cannot distinguish
    /// "measured and dead" from "not judged yet".
    struct FailingWith {
        verdict_samples: u32,
    }
    impl EndpointPerformanceRead for FailingWith {
        fn stats(&self, _endpoint: &str) -> Option<EndpointStats> {
            Some(EndpointStats {
                samples: 10,
                throughput_samples: 0,
                verdict_samples: self.verdict_samples,
                mean_latency_ms: 100.0,
                mean_tokens_per_sec: 0.0,
                success_rate: 0.0,
            })
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
    ///
    /// FIVE distinct priorities, not three. The group loop is a `while` over a
    /// drained remainder, and a defect that truncates it — capping the
    /// iterations, or draining the remainder early — silently drops every group
    /// past the cap, i.e. most of the fallback chain. Three groups is inside the
    /// plausible off-by-a-few range, so a three-entry fixture cannot see it.
    #[test]
    fn distinct_priorities_select_identically_to_priority_order_on_every_seed() {
        for seed in 0..256u64 {
            let mut weighted = vec![
                sm_cost("c", 3, Some(0.5)),
                sm_cost("a", 1, Some(9.0)),
                sm_cost("b", 2, None),
                sm_cost("d", 4, Some(2.0)),
                sm_cost("e", 5, None),
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

    /// Groups of MIXED size — the shape AC1 structurally cannot host, because
    /// AC1 is an equality against `PriorityStrategy` and that equality only
    /// holds while every group is a singleton. Priorities `[1, 2, 2, 3]`: the
    /// singletons are pinned to exact slots, and the tie pair must stay wholly
    /// inside the slots between them however the draw falls.
    #[test]
    fn a_tie_group_is_confined_to_its_own_slots_between_singletons() {
        for seed in 0..64u64 {
            let mut v = vec![
                sm_cost("solo_first", 1, Some(50.0)),
                sm_cost("tied_dear", 2, Some(8.0)),
                sm_cost("tied_cheap", 2, Some(0.5)),
                sm_cost("solo_last", 3, Some(0.01)),
            ];
            let rng = SplitMix64::seeded(seed);
            GroupedWeightedStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));
            let got = names(&v);
            assert_eq!(got[0], "solo_first", "seed {seed}: priority 1 holds slot 0");
            assert_eq!(got[3], "solo_last", "seed {seed}: priority 3 holds slot 3");
            let mut tied = vec![got[1].clone(), got[2].clone()];
            tied.sort();
            assert_eq!(
                tied,
                vec!["tied_cheap".to_string(), "tied_dear".to_string()],
                "seed {seed}: the priority-2 pair must occupy slots 1-2 and nothing else"
            );
        }
    }

    /// AC2 — the WEIGHTING, not merely "it varies". A test asserting only that
    /// order changes would pass against uniform shuffling.
    ///
    /// At 1 vs 3, weights are 1/1 and 1/9, so the cheap model leads 9 times in 10.
    ///
    /// Pinned to an EXACT count rather than a tolerance around 0.9. This test
    /// only looks statistical: it owns a seeded RNG, consumes it from one
    /// thread, and does plain IEEE-754 arithmetic, so the count is
    /// bit-deterministic and reproduces exactly across runs and thread counts. A
    /// ±0.03 band implied a looseness that does not exist and cost real strength
    /// — it admitted any exponent whose share landed inside it, so
    /// `1/cost.powf(1.8)` (share 0.8784) passed as "inverse square". 3572/4000
    /// is 0.893; the ~9/10 rationale is the doc above, and the number is the
    /// pin.
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
        assert_eq!(
            cheap_first, 3572,
            "the 1-unit model leads ~9/10 against a 3-unit one under an INVERSE \
             SQUARE weight; this count is bit-deterministic, so a change to it \
             means the weighting exponent moved"
        );
    }

    /// AC3 — free beats every price, on every seed. `1/0²` is undefined; the
    /// limit is "always first", and that is what this pins.
    ///
    /// BOTH input orders, and that half is load-bearing rather than thorough.
    /// A zero cost that escapes the free bucket becomes a non-finite weight, and
    /// the draw loop degenerates to a fixed order — the `idx` initialiser before
    /// the overflow guard existed, chain order after it. Either way the result
    /// is a FIXED position, so a single-order fixture that happens to agree with
    /// it goes green with the entire free classification deleted (verified: both
    /// the `cost <= 0.0` and the `!base.is_finite()` guard can be removed
    /// together and a one-permutation fixture still passes). Asserting both
    /// orders means no fixed position can satisfy the test — only genuinely
    /// bucketing `free` ahead of the draw can.
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
        assert_eq!(
            cheap_first, 1815,
            "unmeasured must weigh a NON-ZERO constant, so price weighting still \
             applies; bit-deterministic, so a change means the fallback moved"
        );
    }

    /// What the test above CANNOT see, and the regression that actually hurts.
    ///
    /// Both candidates there are unmeasured, so whatever the fallback is it
    /// multiplies both weights equally and cancels out of the ratio —
    /// `unwrap_or(0.5)`, or any other non-zero constant, produces the identical
    /// count. That test therefore pins "non-zero", not "1.0", despite its name.
    ///
    /// The value only becomes observable when a cold endpoint is weighed against
    /// a PROVEN one at the same price. It must be `1.0` — equal footing, so the
    /// two are drawn 50/50. At `0.5` the cold endpoint is drawn half as often as
    /// the proven one, so a newly added endpoint is starved of exactly the
    /// traffic it needs to cast its first verdict and can never climb out.
    #[test]
    fn a_cold_endpoint_is_drawn_as_often_as_a_proven_one_at_the_same_price() {
        struct ProvenAndCold;
        impl EndpointPerformanceRead for ProvenAndCold {
            fn stats(&self, endpoint: &str) -> Option<EndpointStats> {
                let proven = endpoint.ends_with(":proven");
                Some(EndpointStats {
                    samples: 10,
                    throughput_samples: 0,
                    // The proven one is measured and perfect; the cold one has
                    // never cast a verdict, so it falls back.
                    verdict_samples: if proven { 10 } else { 0 },
                    mean_latency_ms: 100.0,
                    mean_tokens_per_sec: 0.0,
                    success_rate: if proven { 1.0 } else { 0.0 },
                })
            }
        }
        let rng = SplitMix64::seeded(0xA11CE);
        let mut cold_first = 0;
        const N: usize = 2000;
        for _ in 0..N {
            // IDENTICAL price, so price cancels and only reliability can move
            // the ratio off 50/50.
            let mut v = vec![
                sm_cost("proven", 1, Some(2.0)),
                sm_cost("cold", 1, Some(2.0)),
            ];
            GroupedWeightedStrategy.order(&mut v, &test_ctx(&ProvenAndCold, &rng));
            if v[0].model == "cold" {
                cold_first += 1;
            }
        }
        assert_eq!(
            cold_first, 982,
            "a cold endpoint must weigh 1.0 — the same as a proven one — so the \
             two split 50/50 (982/2000 = 0.491). A 0.5 fallback would starve the \
             new endpoint at ~1/3"
        );
    }

    /// `ctx.min_samples` is the threshold `StrategyCtx` documents for
    /// `success_rate`, and this is the half that matters operationally: BELOW
    /// it, a candidate is not yet judged.
    ///
    /// At `> 0` instead of `>= min_samples`, ONE failed request drives
    /// `success_rate` to 0.0, which is `Weight::Zero`, which is dead last in the
    /// group until the entire observation window rolls over. A single unlucky
    /// attempt would banish a healthy endpoint.
    #[test]
    fn a_candidate_below_the_verdict_threshold_is_not_yet_judged() {
        // min_samples is 3 in `test_ctx`, so 2 verdicts is BELOW the threshold.
        let perf = FailingWith { verdict_samples: 2 };
        let rng = SplitMix64::seeded(0xB0B);
        let mut flaky_first = 0;
        const N: usize = 2000;
        for _ in 0..N {
            // The flaky one is also the CHEAP one, so if it is treated as
            // unjudged (reliability 1.0) it leads ~9/10; if it is treated as
            // measured-and-dead it is `Weight::Zero` and leads exactly never.
            let mut v = vec![
                sm_cost("other", 1, Some(3.0)),
                sm_cost("flaky", 1, Some(1.0)),
            ];
            GroupedWeightedStrategy.order(&mut v, &test_ctx(&perf, &rng));
            if v[0].model == "flaky" {
                flaky_first += 1;
            }
        }
        assert_eq!(
            flaky_first, 1798,
            "below `min_samples` a candidate weighs 1.0 and competes on price \
             (1798/2000 = 0.899); a `> 0` gate would make this exactly 0"
        );
    }

    /// The other direction of the same boundary: AT `min_samples` the verdict IS
    /// trusted, so a candidate that has failed every one of them is `Weight::Zero`
    /// and goes last on every seed. Pins the threshold as `>=` rather than `>`.
    #[test]
    fn a_candidate_at_the_verdict_threshold_is_judged() {
        let perf = FailingWith {
            verdict_samples: 3, // exactly `test_ctx`'s min_samples
        };
        for seed in 0..64u64 {
            let mut v = vec![
                sm_cost("other", 1, Some(3.0)),
                sm_cost("flaky", 1, Some(1.0)),
            ];
            let rng = SplitMix64::seeded(seed);
            GroupedWeightedStrategy.order(&mut v, &test_ctx(&perf, &rng));
            assert_eq!(
                names(&v),
                vec!["other", "flaky"],
                "seed {seed}: at exactly min_samples the 0% verdict is trusted, \
                 so the cheap-but-dead candidate is last despite its price"
            );
        }
    }

    /// Two FREE candidates keep chain order. Without this, `free.push(m)` →
    /// `free.insert(0, m)` is unobservable — every other fixture has at most one
    /// free candidate — and the strategy's own claim that the buckets "preserve
    /// input order" is unasserted.
    #[test]
    fn two_free_candidates_keep_chain_order() {
        for seed in 0..64u64 {
            let mut v = vec![
                sm_cost("free_first", 1, None),
                sm_cost("free_second", 1, None),
            ];
            let rng = SplitMix64::seeded(seed);
            GroupedWeightedStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));
            assert_eq!(
                names(&v),
                vec!["free_first", "free_second"],
                "seed {seed}: free candidates are not drawn, so chain order is \
                 the operator's tiebreak and must survive"
            );
        }
    }

    /// Two ZERO-weight candidates keep chain order, for the same reason — and
    /// `zero.insert(0, m)` is likewise invisible to every single-zero fixture.
    #[test]
    fn two_zero_weight_candidates_keep_chain_order() {
        let perf = FailingWith {
            verdict_samples: 10,
        };
        for seed in 0..64u64 {
            let mut v = vec![
                sm_cost("dead_first", 1, Some(1.0)),
                sm_cost("dead_second", 1, Some(2.0)),
            ];
            let rng = SplitMix64::seeded(seed);
            GroupedWeightedStrategy.order(&mut v, &test_ctx(&perf, &rng));
            assert_eq!(
                names(&v),
                vec!["dead_first", "dead_second"],
                "seed {seed}: zero-weight candidates cannot be drawn, so chain \
                 order decides their relative position — including against price"
            );
        }
    }

    /// The branch `Weight::Free`'s own docstring describes — "or so little that
    /// `1/cost²` overflows" — and which nothing else reaches. A cost of `1e-200`
    /// is finite and positive, but squaring it UNDERFLOWS to `0.0`, so
    /// `1.0 / 0.0` is `inf` and only the `!base.is_finite()` guard catches it.
    /// Deleting that guard while leaving `cost <= 0.0` in place is invisible to
    /// every other fixture here, whose free candidates are all exactly zero.
    ///
    /// Both input orders, for the reason given on
    /// `a_free_candidate_leads_its_group_on_every_seed`: an escaped non-finite
    /// weight lands in a FIXED position, which one permutation would agree with
    /// by luck.
    #[test]
    fn a_vanishingly_cheap_candidate_is_treated_as_free_in_both_input_orders() {
        for seed in 0..64u64 {
            for tiny_first in [false, true] {
                let tiny = sm_cost("tiny", 1, Some(1.0e-200));
                let normal = sm_cost("normal", 1, Some(1.0));
                let mut v = if tiny_first {
                    vec![tiny, normal]
                } else {
                    vec![normal, tiny]
                };
                let rng = SplitMix64::seeded(seed);
                GroupedWeightedStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));
                assert_eq!(
                    v[0].model, "tiny",
                    "seed {seed}, tiny_first {tiny_first}: a cost whose square \
                     underflows is indistinguishable from free and leads"
                );
            }
        }
    }

    /// Q1 — the overflow is in the SUM, which no guard in `classify` covers.
    ///
    /// A cost of `1e-154` squares to `1e-308`, which is subnormal but
    /// representable, so `base` is `1e308`: finite, positive, and past every
    /// guard in `classify`. TWO of them sum to `2e308`, which is not
    /// representable — `total` is `inf`, `u` is `inf` (or `NaN` on a zero draw),
    /// every `u <= 0.0` is false, and the selection loop falls through to its
    /// `idx = pool.len() - 1` initialiser, picking the LAST candidate first,
    /// every single draw. The overflow guard replaces that with chain order.
    ///
    /// Both permutations, because both the bug and the fix are deterministic:
    /// the bug REVERSES chain order and the fix PRESERVES it, so a single
    /// permutation cannot tell them apart from a coin that landed the same way.
    #[test]
    fn an_overflowing_weight_sum_falls_back_to_chain_order_in_both_input_orders() {
        for seed in 0..64u64 {
            for swapped in [false, true] {
                let a = sm_cost("a", 1, Some(1.0e-154));
                let b = sm_cost("b", 1, Some(1.0e-154));
                let (mut v, expected) = if swapped {
                    (vec![b, a], vec!["b", "a"])
                } else {
                    (vec![a, b], vec!["a", "b"])
                };
                let rng = SplitMix64::seeded(seed);
                GroupedWeightedStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));
                assert_eq!(
                    names(&v),
                    expected,
                    "seed {seed}, swapped {swapped}: an overflowing weight sum \
                     must fall back to chain order, not to a fixed index"
                );
            }
        }
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
