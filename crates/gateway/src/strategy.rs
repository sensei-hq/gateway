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

/// What one [`RoutingStrategy::order`] call did, beyond permuting the
/// candidates — the input to the trace's `RoutingDecision` (SP-ROUTE-1 AC10).
///
/// **Returned BY `order` rather than read back off the strategy afterwards, and
/// that is the whole design.** The trace has to report the weight and the
/// reliability the ordering ACTUALLY used, and the two other shapes both fail
/// that requirement:
///
/// - **Recomputing them at the trace site** is a second derivation of the same
///   numbers from a LIVE port. `PerformanceStore::stats` recomputes its means
///   from `Instant::now()` on every call, so an attempt completing between the
///   sort and the trace makes the trace describe an ordering that never
///   happened. That is the same live-read hazard that panicked model selection
///   in Task 9, in a shape that fails silently instead of loudly.
/// - **Stashing them on the strategy for a later `explain(&self)`** needs
///   interior mutability (`order` takes `&self`, and strategies are
///   `Send + Sync`) and introduces temporal coupling: called before `order`, or
///   after a second `order`, it returns something stale with no way to tell.
///
/// A return value has neither problem. It cannot exist without the call that
/// produced the ordering, and it cannot outlive it.
///
/// **What the return shape does NOT buy, stated because the obvious reading
/// over-claims.** "Returned by `order`" guarantees only that the report was
/// PRODUCED INSIDE the call that produced the ordering — so it cannot be stale
/// and cannot describe a different call. It does not make the values
/// tamper-proof: the report is an independent channel, and a caller is free to
/// discard it and fabricate a plausible-looking substitute. That was verified,
/// not assumed — a re-derivation at the engine's attachment site keeping
/// strategy/order/cost while nulling `reliability` and `weight` passed the
/// entire suite until
/// `engine::tests::the_response_carries_the_weights_the_draw_actually_used`
/// was written.
///
/// The PER-VALUE guarantee comes from one local discipline in [`order_group`],
/// and it is the load-bearing line for anyone editing that function: [`Weight`]
/// is `Copy`, `classified` is bound ONCE, and that same binding is both
/// `.recorded()` into this report and `match`ed into the free/zero/draw
/// buckets. Classify twice — even with identical-looking arguments — and the
/// report becomes a second derivation over a live port, which is the thing this
/// type exists to prevent.
#[derive(Debug, Default, PartialEq)]
pub struct OrderingReport {
    /// A metric sort had two or more candidates to order but fewer than two
    /// MEASURED ones, so it could not express any ordering preference and the
    /// result is exactly priority order.
    ///
    /// `< 2` measured, not `== 0`: with a single measured candidate the sort
    /// writes that candidate back into the slot it already occupied and nothing
    /// moves, so the output is priority order just as surely as with none.
    /// Conversely two measured candidates that happen to already agree are NOT
    /// degraded — the sort would have reordered them had they disagreed. The
    /// claim is about the INFORMATION the sort had, not about whether the
    /// permutation changed.
    ///
    /// Requires two or more ADMITTED candidates for the same reason read the
    /// other way. A one-candidate chain has exactly one ordering, so no reading
    /// could have changed it and nothing was degraded for want of one; saying
    /// otherwise is a false alarm, not a conservative one.
    ///
    /// Always `false` for a strategy that consults no sample counts
    /// ([`PriorityStrategy`], [`PriceStrategy`], [`GroupedWeightedStrategy`]):
    /// they cannot degrade for want of something they never read.
    pub degraded: bool,
    /// One entry per candidate the strategy WEIGHED. Empty for a strategy that
    /// weighs nothing, which is every strategy but the default.
    ///
    /// Keyed by endpoint rather than positional, because the caller re-ranks
    /// the candidates after `order` returns (the `order` routing preference),
    /// so a position is not a stable join.
    pub weights: Vec<CandidateWeight>,
}

/// The draw inputs one candidate was weighed with, exactly as the strategy used
/// them.
#[derive(Debug, Clone, PartialEq)]
pub struct CandidateWeight {
    /// `"{router}:{model}"`.
    pub endpoint: String,
    /// The MEASURED windowed success rate, or `None` when the endpoint carried
    /// fewer than [`StrategyCtx::min_samples`] verdicts.
    ///
    /// `None` is emphatically not `0.0`: an unmeasured candidate is WEIGHED as
    /// healthy (`1.0`), so recording `0.0` here would both misreport the input
    /// and contradict the weight recorded beside it.
    pub reliability: Option<f64>,
    /// The draw weight, or `None` when the candidate never entered the draw —
    /// the free bucket, where `1/cost²` is undefined or overflows and the
    /// candidate leads its group outright.
    ///
    /// `Some(0.0)` is a different and meaningful answer: a real weight of zero,
    /// which can never be drawn and therefore goes last.
    pub weight: Option<f64>,
}

/// Orders admitted candidates. The single ordering seam.
pub trait RoutingStrategy: Send + Sync {
    /// Order `admitted` in place, and report what was used to do it.
    ///
    /// See [`OrderingReport`] for why the explanation is this call's RETURN
    /// VALUE rather than a second method on the strategy.
    fn order(&self, admitted: &mut Vec<SelectedModel>, ctx: &StrategyCtx<'_>) -> OrderingReport;

    /// A stable identifier for the strategy that actually ran, for the trace.
    ///
    /// Deliberately NOT defaulted. The alternative to asking the strategy is a
    /// second `match` on `SortKey` at the trace site, which agrees with
    /// `ModelSelectionService::strategy_for` on the day it is written and is
    /// free to drift from it afterwards; this way there is exactly one `match`
    /// in the crate and the name is read off the object that did the work.
    /// A default would reintroduce the same failure one step along — a new
    /// strategy would silently trace as whatever the default said.
    ///
    /// The two metric sorts share one type, so this is a property of the VALUE
    /// rather than of the type: `MetricStrategy` reports its own metric.
    fn name(&self) -> &'static str;
}

/// A reference to a strategy is a strategy.
///
/// Test-only, and it exists for one reason: since Task 10,
/// `ModelSelectionService::strategy_for` resolves a strategy PER REQUEST and
/// returns it owned, so the test-only `strategy_override` field cannot be
/// handed back directly — it would have to be moved out of `&self`. This lets
/// `strategy_for` return a BORROW of the override instead, keeping the probe
/// mechanism (`selection::tests::the_builders_install_the_ports_the_strategy_sees`)
/// working without widening the production surface.
///
/// **It makes the test build's trait-resolution universe differ from
/// production's**: under `cfg(test)` `&T` satisfies `RoutingStrategy`, so a
/// call that only compiles through this blanket impl would compile in tests and
/// fail the release build. Nothing in `src/` relies on it today (production
/// `strategy_for` returns owned strategies), and that is the property to
/// preserve if this impl is ever widened.
#[cfg(test)]
impl<T: RoutingStrategy + ?Sized> RoutingStrategy for &T {
    fn order(&self, admitted: &mut Vec<SelectedModel>, ctx: &StrategyCtx<'_>) -> OrderingReport {
        (**self).order(admitted, ctx)
    }
    /// Forwarded, NOT reported as a wrapper name — an override must announce
    /// the strategy it wraps or the probe becomes invisible in the trace.
    fn name(&self) -> &'static str {
        (**self).name()
    }
}

/// Strict ascending priority, stable. Retained as the explicit baseline every
/// other strategy is compared against in tests.
pub struct PriorityStrategy;
impl RoutingStrategy for PriorityStrategy {
    fn order(&self, admitted: &mut Vec<SelectedModel>, _ctx: &StrategyCtx<'_>) -> OrderingReport {
        admitted.sort_by_key(|m| m.priority); // stable; identical to resolve_chain's sort today
        // Reads no port and weighs nothing, so there is nothing to explain
        // beyond the authored priorities the caller already has.
        OrderingReport::default()
    }
    fn name(&self) -> &'static str {
        "priority"
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

#[derive(Clone, Copy)]
enum Weight {
    /// Costs nothing, or so little that `1/cost²` overflows — indistinguishable
    /// at that price. Always ahead of anything priced.
    Free,
    Draw(f64),
    /// Weight zero — unreachable by a draw, so it goes last.
    Zero,
}

impl Weight {
    /// How this classification reads in a [`CandidateWeight`]: the draw weight
    /// when there is one, and `None` for the free bucket, which bypasses the
    /// draw entirely rather than entering it with some particular number.
    fn recorded(self) -> Option<f64> {
        match self {
            Weight::Free => None,
            Weight::Draw(w) => Some(w),
            Weight::Zero => Some(0.0),
        }
    }
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
    fn order(&self, admitted: &mut Vec<SelectedModel>, ctx: &StrategyCtx<'_>) -> OrderingReport {
        // Stable, so equal priorities end up adjacent IN CHAIN ORDER — the input
        // order the free/zero buckets below preserve.
        admitted.sort_by_key(|m| m.priority);

        let mut rest = std::mem::take(admitted);
        let mut out = Vec::with_capacity(rest.len());
        // Accumulated ACROSS groups, so the report covers every candidate and
        // not merely the last group's.
        let mut weights = Vec::with_capacity(rest.len());
        while !rest.is_empty() {
            let p = rest[0].priority;
            let split = rest
                .iter()
                .position(|m| m.priority != p)
                .unwrap_or(rest.len());
            let group: Vec<SelectedModel> = rest.drain(..split).collect();
            out.extend(order_group(group, ctx, &mut weights));
        }
        *admitted = out;
        OrderingReport {
            // Weighs every candidate it is given, whatever the store holds, so
            // there is no sample count it could fall short of.
            degraded: false,
            weights,
        }
    }
    fn name(&self) -> &'static str {
        "grouped_weighted"
    }
}

/// Orders one equal-priority group, appending one [`CandidateWeight`] per
/// candidate to `weights` as it goes.
///
/// The recording happens HERE, beside the classification, rather than being
/// re-derived by the caller afterwards — see [`OrderingReport`]. `ctx.perf` is a
/// live window, so a second read would not be guaranteed to return what this
/// one did.
fn order_group(
    group: Vec<SelectedModel>,
    ctx: &StrategyCtx<'_>,
    weights: &mut Vec<CandidateWeight>,
) -> Vec<SelectedModel> {
    let mut free = Vec::new();
    let mut zero = Vec::new();
    let mut pool: Vec<(f64, SelectedModel)> = Vec::new();

    for m in group {
        let cost = m.cost_estimate.as_ref().map(|c| c.estimated).unwrap_or(0.0);
        // Bound once and reused for both the port lookup and the record, so the
        // recorded endpoint is by construction the key the reading was taken
        // under — and so a selection does not allocate the same string twice.
        let endpoint = m.endpoint_key();
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
        //
        // Split into MEASURED and the value actually used, rather than collapsed
        // by `unwrap_or` in one expression: the trace needs to say which of the
        // two it is (`None` vs `Some(0.0)`) and the draw needs the number. One
        // read of the port serves both, so the recorded reliability cannot
        // disagree with the one the weight was computed from.
        let measured = ctx
            .perf
            .stats(&endpoint)
            .filter(|s| s.verdict_samples >= ctx.min_samples)
            .map(|s| s.success_rate);
        let reliability = measured.unwrap_or(1.0);
        let classified = classify(cost, reliability);
        weights.push(CandidateWeight {
            endpoint,
            reliability: measured,
            weight: classified.recorded(),
        });
        match classified {
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

/// `sort: price` — ascending estimated cost across every candidate, free first,
/// ties broken by authored priority so no pair is left to price alone; equal
/// price and equal priority keep chain order.
///
/// This deliberately overrides `priority` entirely, unlike
/// [`GroupedWeightedStrategy`], which only ever reorders WITHIN a priority
/// group. That is the documented meaning of an explicit `sort`: load balancing
/// switches off and the router tries candidates strictly in the named order.
///
/// `cost_estimate: None` is treated as free, matching `BudgetGate`'s reading
/// that an unpriced model costs nothing.
pub struct PriceStrategy;

/// The sort key for one candidate.
///
/// A NON-FINITE price is fenced to `+inf` so it sorts LAST. This is not
/// hypothetical: `estimate_cost` sums `input_cost + output_cost` over three
/// unvalidated `f64`s from config, so `1e308` against `-1e308` produces `NaN`.
/// A `NaN` compares `None` against everything, `unwrap_or(Equal)` turns that
/// into "equal to all", and an intransitive comparator makes `sort_by` PANIC —
/// which the `.then(priority)` tiebreak does NOT rescue, since equal priorities
/// are exactly the load-balancing case an explicit `sort` permits.
///
/// `+inf` rather than `0.0` deliberately: `None` is free, but an unusable price
/// is not a price at all, and treating it as free would let a broken price win
/// the CHEAPEST slot — a budget hazard rather than a neutral default.
fn price_key(m: &SelectedModel) -> f64 {
    let c = m.cost_estimate.as_ref().map(|c| c.estimated).unwrap_or(0.0);
    if c.is_finite() { c } else { f64::INFINITY }
}

impl RoutingStrategy for PriceStrategy {
    fn order(&self, admitted: &mut Vec<SelectedModel>, _ctx: &StrategyCtx<'_>) -> OrderingReport {
        admitted.sort_by(|a, b| {
            price_key(a)
                .partial_cmp(&price_key(b))
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.priority.cmp(&b.priority))
        });
        // Sorts on the price the trace already carries per candidate, and reads
        // no port — so it has no weight and no sample count to report.
        OrderingReport::default()
    }
    fn name(&self) -> &'static str {
        "price"
    }
}

/// Which observed quantity a [`MetricStrategy`] sorts on.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    Latency,
    Throughput,
}

/// `sort: latency | throughput`.
///
/// **Reorders only what it knows about.** A fresh process has no observations,
/// and "observed 400ms" against "never measured" are not comparable quantities
/// — so rather than impute a value, this sorts the MEASURED subset among the
/// indices that subset already occupies. Unmeasured candidates never move.
///
/// Three properties, no magic constants: zero observations ⇒ pure priority
/// order; full observations ⇒ a complete metric sort; partial ⇒ a monotone
/// interpolation between them.
pub struct MetricStrategy {
    metric: Metric,
}

impl MetricStrategy {
    pub fn latency() -> Self {
        Self {
            metric: Metric::Latency,
        }
    }
    pub fn throughput() -> Self {
        Self {
            metric: Metric::Throughput,
        }
    }

    /// `None` ⇒ not measured for THIS metric.
    ///
    /// Each arm reads the counter that belongs to ITS metric, and no other.
    /// Latency reads `samples`; throughput reads `throughput_samples`. The
    /// three counters on `EndpointStats` are independent — neither a superset
    /// nor a subset of one another — so an endpoint can carry plenty of
    /// observations of one kind and none of another, and each mean is `0.0`
    /// when its own counter is zero. Read the wrong one and that `0.0`
    /// fallback is mistaken for a measurement, which sorts the endpoint that
    /// was NEVER observed straight to the front.
    ///
    /// A non-finite reading is fenced to `None` — not a measurement, so it
    /// takes the unmeasured hold-its-index path. Without this, a `NaN` compares
    /// `None` against every other candidate, `unwrap_or(Equal)` turns that into
    /// "equal to all", and the resulting intransitive comparator makes
    /// `sort_by` panic.
    fn value(&self, m: &SelectedModel, ctx: &StrategyCtx<'_>) -> Option<f64> {
        let s = ctx.perf.stats(&m.endpoint_key())?;
        let v = match self.metric {
            Metric::Latency => (s.samples >= ctx.min_samples).then_some(s.mean_latency_ms),
            // Negated so an ASCENDING sort puts the highest rate first.
            Metric::Throughput => {
                (s.throughput_samples >= ctx.min_samples).then_some(-s.mean_tokens_per_sec)
            }
        }?;
        v.is_finite().then_some(v)
    }
}

impl RoutingStrategy for MetricStrategy {
    fn order(&self, admitted: &mut Vec<SelectedModel>, ctx: &StrategyCtx<'_>) -> OrderingReport {
        // The baseline every unmeasured candidate keeps.
        admitted.sort_by_key(|m| m.priority);

        // ONE read per candidate, taken BEFORE any comparison, then sort the
        // SNAPSHOT. `ctx.perf` is a LIVE rolling window that
        // `PerformanceRecorder::on_outcome` mutates from every concurrently
        // completing attempt, and `stats()` recomputes its means from
        // `Instant::now()` on every call. Calling `value()` from INSIDE the
        // comparator lets a candidate's key change between two comparisons,
        // which makes the comparator intransitive — and `sort_by` detects that
        // and PANICS, inside model selection. Measured against the real
        // `PerformanceStore` with four writer threads: 89 panics in 719
        // selections at n=40, and none at n=12, because the total-order check
        // only runs once the input outgrows insertion sort.
        //
        // Snapshotting also drops the cost from O(n log n) mutex acquisitions,
        // full-window rescans and `endpoint_key()` allocations per selection to
        // O(n) — 12 reads for a 12-candidate chain rather than 144.
        let mut subset: Vec<(f64, SelectedModel)> = Vec::new();
        let mut slots: Vec<usize> = Vec::new();
        for (i, m) in admitted.iter().enumerate() {
            if let Some(v) = self.value(m, ctx) {
                slots.push(i);
                subset.push((v, m.clone()));
            }
        }
        // Computed from the snapshot, BEFORE the sort consumes it. Fewer than
        // two measured candidates leaves the output identical to the priority
        // order established above — with one, the loop below writes that
        // candidate back into the slot it already held — so the caller asked
        // for a metric sort and is silently receiving priority order. That is
        // the fact the trace exists to surface; see `OrderingReport::degraded`
        // for why the boundary is `< 2` rather than `== 0`.
        //
        // Gated on there being two candidates to ORDER in the first place, and
        // that half is not a mere empty-guard. A one-candidate chain has
        // exactly one ordering, so no quantity of samples would have changed
        // the answer and nothing was lost for want of them; `subset.len() < 2`
        // alone is a tautology there and fires even for a candidate measured
        // far past `min_samples`. Reporting that as degraded sends a reader
        // hunting for missing observations that were never the cause. Subsumes
        // the empty case (`0 >= 2` is false). Pinned by
        // `selection::tests::a_single_candidate_chain_is_never_degraded_for_want_of_samples`.
        let degraded = admitted.len() >= 2 && subset.len() < 2;
        // STABLE, and that is load-bearing: candidates whose readings are equal
        // fall back to the authored priority order established above, which is
        // what makes a partial set of observations a monotone interpolation
        // between priority order and metric order rather than an arbitrary
        // shuffle of the ties.
        subset.sort_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        debug_assert_eq!(slots.len(), subset.len());
        for (&slot, (_, m)) in slots.iter().zip(subset) {
            admitted[slot] = m;
        }
        OrderingReport {
            degraded,
            // Sorts on an observed mean and never on a draw, so there is no
            // weight to report.
            weights: Vec::new(),
        }
    }
    /// A property of the VALUE, not the type — the two metric sorts share
    /// `MetricStrategy`, so reporting a single type-wide name would make
    /// `sort: latency` and `sort: throughput` indistinguishable in the trace.
    fn name(&self) -> &'static str {
        match self.metric {
            Metric::Latency => "latency",
            Metric::Throughput => "throughput",
        }
    }
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
    /// every group is a singleton, so the weighted default is indistinguishable
    /// from `PriorityStrategy`.
    ///
    /// Scope, stated precisely: this covers every chain whose admitted
    /// candidates have DISTINCT priorities — every chain in this repo today, and
    /// every `assemble()` output up to 254 entries. Not "every catalog chain by
    /// construction": `dedup_and_prioritize` assigns position via
    /// `u8::try_from(pos + 1).unwrap_or(u8::MAX)`, which SATURATES, so a chain
    /// longer than 254 entries ties at 255 and is genuinely randomised here. A
    /// hand-authored chain can tie too — `GatewayBuilder::add_chain` and the
    /// `Deserialize` impl pass `priority` through verbatim and no validation
    /// rule forbids a repeat. A tie is the opt-in to load balancing, so neither
    /// is a defect; they are simply not covered by THIS test's claim.
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

    /// AC8 — `sort: price` orders across the WHOLE chain and deliberately
    /// overrides `priority`. That is what "load balancing switches off and the
    /// router tries providers strictly in that order" means.
    ///
    /// Every candidate here is placed so that priority order and price order
    /// DISAGREE — a strategy that quietly respected priority would return the
    /// input unchanged and pass a weaker test.
    #[test]
    fn price_sort_overrides_priority_across_the_whole_chain() {
        let rng = SplitMix64::seeded(1);
        let mut v = vec![
            sm_cost("dear_but_first", 1, Some(10.0)),
            sm_cost("cheap_but_last", 9, Some(0.1)),
            sm_cost("free_but_middle", 5, None),
        ];
        PriceStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));
        assert_eq!(
            names(&v),
            vec!["free_but_middle", "cheap_but_last", "dear_but_first"]
        );
    }

    /// Equal prices fall back to authored priority, so the order is TOTAL —
    /// no pair is left to input-order chance.
    #[test]
    fn price_sort_breaks_ties_on_priority() {
        let rng = SplitMix64::seeded(1);
        let mut v = vec![
            sm_cost("second", 2, Some(1.0)),
            sm_cost("first", 1, Some(1.0)),
        ];
        PriceStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));
        assert_eq!(names(&v), vec!["first", "second"]);
    }

    /// An unpriced candidate is FREE, matching `BudgetGate`'s reading and
    /// `GroupedWeightedStrategy`'s. Asserted in BOTH input orders so no
    /// fixed-index or stable-sort coincidence can satisfy it — that exact
    /// coincidence hid a broken free-first classification in Task 7.
    #[test]
    fn price_sort_puts_an_unpriced_candidate_first_in_both_input_orders() {
        for unpriced_first in [true, false] {
            let rng = SplitMix64::seeded(7);
            let mut v = if unpriced_first {
                vec![
                    sm_cost("unpriced", 5, None),
                    sm_cost("cheap", 1, Some(0.001)),
                ]
            } else {
                vec![
                    sm_cost("cheap", 1, Some(0.001)),
                    sm_cost("unpriced", 5, None),
                ]
            };
            PriceStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));
            assert_eq!(
                names(&v),
                vec!["unpriced", "cheap"],
                "unpriced_first {unpriced_first}: an unpriced candidate costs nothing and leads"
            );
        }
    }

    /// `PriceStrategy` is DETERMINISTIC — it must not consume the RNG at all.
    /// Two runs with the same source must agree, and so must runs with
    /// different seeds. A strategy that drew would pass the first check and
    /// fail the second.
    #[test]
    fn price_sort_is_deterministic_and_consumes_no_randomness() {
        let fixture = || {
            vec![
                sm_cost("c", 1, Some(3.0)),
                sm_cost("a", 2, Some(1.0)),
                sm_cost("b", 3, Some(2.0)),
            ]
        };
        let expected = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        for seed in [1u64, 2, 12345, u64::MAX] {
            let rng = SplitMix64::seeded(seed);
            let mut v = fixture();
            PriceStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));
            assert_eq!(
                names(&v),
                expected,
                "seed {seed}: price order is not a draw"
            );
            // The source must be untouched: its FIRST draw must still be the
            // first draw of a fresh source with the same seed.
            assert_eq!(
                rng.next_u64(),
                SplitMix64::seeded(seed).next_u64(),
                "seed {seed}: PriceStrategy must not consume the RNG"
            );
        }
    }

    /// (endpoint suffix, samples, throughput_samples, mean_latency_ms, mean_tokens_per_sec)
    struct FixedStats(&'static [(&'static str, u32, u32, f64, f64)]);
    impl EndpointPerformanceRead for FixedStats {
        fn stats(&self, endpoint: &str) -> Option<EndpointStats> {
            self.0.iter().find(|(e, ..)| endpoint.ends_with(e)).map(
                |(_, samples, tput, latency, tps)| EndpointStats {
                    samples: *samples,
                    throughput_samples: *tput,
                    verdict_samples: *samples,
                    mean_latency_ms: *latency,
                    mean_tokens_per_sec: *tps,
                    success_rate: 1.0,
                },
            )
        }
    }

    /// AC7 — with NO observations a metric sort is exactly priority order.
    /// Matches `IntraTierStrategy::is_dynamic`'s convention of degrading to
    /// `Priority` rather than inventing numbers.
    #[test]
    fn a_metric_sort_with_no_observations_is_priority_order() {
        let rng = SplitMix64::seeded(1);
        let mut v = vec![sm_cost("b", 2, None), sm_cost("a", 1, None)];
        MetricStrategy::latency().order(&mut v, &test_ctx(&NoPerformance, &rng));
        assert_eq!(names(&v), vec!["a", "b"]);
    }

    /// AC7 — with FULL observations it is a complete metric sort, and it
    /// OVERRIDES priority. `fast` is authored second; if the sort silently
    /// respected priority this would return the input unchanged.
    #[test]
    fn latency_sort_orders_measured_candidates_ascending() {
        let perf = FixedStats(&[(":slow", 5, 5, 900.0, 10.0), (":fast", 5, 5, 100.0, 90.0)]);
        let rng = SplitMix64::seeded(1);
        let mut v = vec![sm_cost("slow", 1, None), sm_cost("fast", 2, None)];
        MetricStrategy::latency().order(&mut v, &test_ctx(&perf, &rng));
        assert_eq!(
            names(&v),
            vec!["fast", "slow"],
            "fast must overtake despite priority 2"
        );
    }

    /// Throughput is DESCENDING — more tokens per second is better. The mirror
    /// of the latency test, and it must not accidentally share its direction.
    #[test]
    fn throughput_sort_orders_measured_candidates_descending() {
        let perf = FixedStats(&[(":slow", 5, 5, 100.0, 10.0), (":fast", 5, 5, 900.0, 90.0)]);
        let rng = SplitMix64::seeded(1);
        let mut v = vec![sm_cost("slow", 1, None), sm_cost("fast", 2, None)];
        MetricStrategy::throughput().order(&mut v, &test_ctx(&perf, &rng));
        assert_eq!(
            names(&v),
            vec!["fast", "slow"],
            "higher tok/s leads — note `slow` has the BETTER latency here, so a \
             latency comparator would give the opposite answer"
        );
    }

    /// AC7, the interesting half — an UNMEASURED candidate holds its INDEX.
    /// It is neither promoted nor demoted, because "observed 400ms" and "never
    /// measured" are not comparable quantities.
    #[test]
    fn an_unmeasured_candidate_holds_its_index() {
        // Only slots 0 and 2 are measured; slot 1 is not.
        let perf = FixedStats(&[(":slow", 5, 5, 900.0, 1.0), (":fast", 5, 5, 100.0, 1.0)]);
        let rng = SplitMix64::seeded(1);
        let mut v = vec![
            sm_cost("slow", 1, None),
            sm_cost("unmeasured", 2, None),
            sm_cost("fast", 3, None),
        ];
        MetricStrategy::latency().order(&mut v, &test_ctx(&perf, &rng));
        assert_eq!(
            names(&v),
            vec!["fast", "unmeasured", "slow"],
            "the measured pair swaps within slots 0 and 2; the unmeasured one does not move"
        );
    }

    /// Below `ctx.min_samples` a candidate is NOT measured, so a single lucky
    /// observation cannot reorder a chain.
    #[test]
    fn a_candidate_below_min_samples_is_not_measured() {
        // min_samples is 3 in `test_ctx`; `fast` has 1.
        let perf = FixedStats(&[(":fast", 1, 1, 10.0, 99.0)]);
        let rng = SplitMix64::seeded(1);
        let mut v = vec![sm_cost("slow", 1, None), sm_cost("fast", 2, None)];
        MetricStrategy::latency().order(&mut v, &test_ctx(&perf, &rng));
        assert_eq!(
            names(&v),
            vec!["slow", "fast"],
            "1 sample < min 3 ⇒ no reorder"
        );
    }

    /// The boundary, pinned in BOTH directions. At exactly `min_samples` a
    /// candidate IS measured; one below it is not. Without both halves an
    /// off-by-one in either direction survives.
    #[test]
    fn the_min_samples_boundary_is_pinned_in_both_directions() {
        let rng = SplitMix64::seeded(1);
        // Exactly at the threshold (3) ⇒ measured ⇒ `fast` overtakes.
        let at = FixedStats(&[(":fast", 3, 3, 10.0, 1.0), (":slow", 3, 3, 900.0, 1.0)]);
        let mut v = vec![sm_cost("slow", 1, None), sm_cost("fast", 2, None)];
        MetricStrategy::latency().order(&mut v, &test_ctx(&at, &rng));
        assert_eq!(names(&v), vec!["fast", "slow"], "3 >= 3 is measured");

        // One below (2) ⇒ unmeasured ⇒ nothing moves.
        let below = FixedStats(&[(":fast", 2, 2, 10.0, 1.0), (":slow", 2, 2, 900.0, 1.0)]);
        let mut v = vec![sm_cost("slow", 1, None), sm_cost("fast", 2, None)];
        MetricStrategy::latency().order(&mut v, &test_ctx(&below, &rng));
        assert_eq!(names(&v), vec!["slow", "fast"], "2 < 3 is unmeasured");
    }

    /// THE counter-confusion test. An endpoint with ample LATENCY samples but
    /// no token counts must not look measured to a THROUGHPUT sort — it would
    /// sort on a mean over zero observations. Reading `samples` instead of
    /// `throughput_samples` is exactly the bug Task 4 shipped.
    #[test]
    fn a_throughput_sort_ignores_an_endpoint_with_no_token_counts() {
        // `latency_only` has 9 latency samples but 0 throughput samples, and a
        // mean_tokens_per_sec of 0.0 — which is the "never measured" fallback,
        // not a measurement. `real` has 5 of each.
        let perf = FixedStats(&[
            (":latency_only", 9, 0, 10.0, 0.0),
            (":real", 5, 5, 900.0, 50.0),
        ]);
        let rng = SplitMix64::seeded(1);
        let mut v = vec![sm_cost("latency_only", 1, None), sm_cost("real", 2, None)];
        MetricStrategy::throughput().order(&mut v, &test_ctx(&perf, &rng));
        assert_eq!(
            names(&v),
            vec!["latency_only", "real"],
            "only `real` is measured for throughput, so it is the only one that may \
             move — and it is already in the one measured slot. Reading `samples` \
             instead would make `latency_only` measured at 0.0 tok/s and demote it."
        );
    }

    // --- Task 8/9 review fixes -------------------------------------------

    /// `sm_cost` builds `router: "test"`, so a candidate `mNN` keys as
    /// `test:mNN`. Recovers the index a fixture varies its reading by.
    fn endpoint_idx(endpoint: &str) -> usize {
        endpoint
            .rsplit(':')
            .next()
            .unwrap()
            .trim_start_matches('m')
            .parse()
            .unwrap()
    }

    /// A reading computed per endpoint by a closure. `FixedStats` takes a
    /// `'static` slice, which cannot express a fixture whose SIZE varies — and
    /// the drift, non-finite and stability tests below all sweep `n`.
    struct FnStats<F>(F);
    impl<F: Fn(&str) -> Option<EndpointStats> + Send + Sync> EndpointPerformanceRead for FnStats<F> {
        fn stats(&self, endpoint: &str) -> Option<EndpointStats> {
            (self.0)(endpoint)
        }
    }

    /// Counts `stats()` calls. The readings are real and distinct, so the sort
    /// does genuine work — a fixture reporting "unmeasured" would keep the
    /// count low by doing nothing, and prove nothing.
    #[derive(Default)]
    struct CountingStats {
        calls: std::sync::atomic::AtomicUsize,
    }
    impl EndpointPerformanceRead for CountingStats {
        fn stats(&self, endpoint: &str) -> Option<EndpointStats> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Some(EndpointStats {
                samples: 9,
                throughput_samples: 9,
                verdict_samples: 9,
                mean_latency_ms: (100 - endpoint_idx(endpoint)) as f64,
                mean_tokens_per_sec: 1.0,
                success_rate: 1.0,
            })
        }
    }

    /// All THREE counters and BOTH means set independently. `FixedStats` ties
    /// `verdict_samples` to `samples` and every fixture using it sets
    /// `samples == throughput_samples`, so it structurally cannot pin WHICH
    /// counter an arm reads. This can.
    ///
    /// (suffix, samples, throughput_samples, verdict_samples, latency, tok/s)
    struct IndependentStats(&'static [(&'static str, u32, u32, u32, f64, f64)]);
    impl EndpointPerformanceRead for IndependentStats {
        fn stats(&self, endpoint: &str) -> Option<EndpointStats> {
            self.0.iter().find(|(e, ..)| endpoint.ends_with(e)).map(
                |(_, samples, tput, verdicts, latency, tps)| EndpointStats {
                    samples: *samples,
                    throughput_samples: *tput,
                    verdict_samples: *verdicts,
                    mean_latency_ms: *latency,
                    mean_tokens_per_sec: *tps,
                    success_rate: 1.0,
                },
            )
        }
    }

    /// C1 — the store is read ONCE per candidate, before any comparison.
    ///
    /// Reading from inside the comparator is not merely slow, it is the
    /// Critical below: `stats()` on the real `PerformanceStore` takes a mutex,
    /// rescans the whole window and allocates, and a comparison-driven read
    /// count is `O(n log n)` of that on every single selection.
    #[test]
    fn a_metric_sort_reads_each_endpoint_at_most_once() {
        const N: usize = 12;
        let perf = CountingStats::default();
        let rng = SplitMix64::seeded(1);
        let mut v: Vec<SelectedModel> = (0..N)
            .map(|i| sm_cost(&format!("m{i:02}"), (i + 1) as u8, None))
            .collect();
        MetricStrategy::latency().order(&mut v, &test_ctx(&perf, &rng));

        // Latency is `100 - i`, so ascending latency is DESCENDING index — the
        // sort really did reorder, and the count below is not the count of a
        // no-op.
        let expected: Vec<String> = (0..N).rev().map(|i| format!("m{i:02}")).collect();
        assert_eq!(
            names(&v),
            expected,
            "the readings must actually drive a sort"
        );

        let calls = perf.calls.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            calls <= N,
            "each candidate's reading must be taken ONCE, before sorting; got \
             {calls} reads for {N} candidates"
        );
    }

    /// C1, the defect itself — a reading that CHANGES between comparisons makes
    /// the comparator intransitive, and `sort_by` PANICS on that.
    ///
    /// This is not hypothetical. `ctx.perf` in production is the shared
    /// `PerformanceStore`; `stats()` recomputes its means from `Instant::now()`
    /// over a ring that `PerformanceRecorder::on_outcome` mutates from every
    /// concurrently completing attempt. Measured against the REAL store with
    /// four writer threads, the pre-fix code panicked model selection in 89 of
    /// 719 selections at n=40 ("user-provided comparison function does not
    /// correctly implement a total order"). At n=12 it never panicked — the
    /// cliff is n≈20, where the sort stops using insertion sort and starts
    /// running its total-order check, so a small fixture cannot see this.
    ///
    /// The fixture drifts deterministically rather than racing, so the test is
    /// reproducible; the sweep crosses the cliff in both directions because the
    /// panic is non-monotone in `n`.
    #[test]
    fn a_drifting_reading_never_panics_the_sort() {
        for n in [3usize, 25, 40, 60] {
            let seq = std::sync::atomic::AtomicU64::new(0);
            let perf = FnStats(move |_endpoint: &str| {
                let k = seq.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Some(EndpointStats {
                    samples: 9,
                    throughput_samples: 9,
                    verdict_samples: 9,
                    // A different answer every time the SAME endpoint is asked.
                    mean_latency_ms: (k.wrapping_mul(2_654_435_761) % 1_000) as f64,
                    mean_tokens_per_sec: (k.wrapping_mul(40_503) % 1_000) as f64,
                    success_rate: 1.0,
                })
            });
            let rng = SplitMix64::seeded(1);
            let mut v: Vec<SelectedModel> = (0..n)
                .map(|i| sm_cost(&format!("m{i:02}"), (i + 1) as u8, None))
                .collect();

            // Panics here if the reading is taken from inside the comparator.
            MetricStrategy::latency().order(&mut v, &test_ctx(&perf, &rng));

            let mut got = names(&v);
            got.sort();
            let mut expected: Vec<String> = (0..n).map(|i| format!("m{i:02}")).collect();
            expected.sort();
            assert_eq!(
                got, expected,
                "n={n}: every candidate must survive the sort exactly once"
            );
        }
    }

    /// I1 — the latency arm reads `samples`, and NEITHER of the other two
    /// counters.
    ///
    /// The Task 9 fixtures all set `samples == throughput_samples` and derived
    /// `verdict_samples` from `samples`, so `Metric::Latency` reading either of
    /// the other counters passed every one of them. The failure that ships
    /// green: `tput_only` has NO latency observation, so its `mean_latency_ms`
    /// is `0.0` — the never-measured fallback, not a measurement. Read the
    /// wrong counter and it is "measured at 0.0ms", which sorts FIRST, so
    /// `sort: latency` routes all traffic to the one endpoint whose latency was
    /// never observed. That is the Task 4 bug in the untested direction.
    #[test]
    fn a_latency_sort_uses_the_latency_counter_only() {
        let perf = IndependentStats(&[
            // No latency samples at all ⇒ 0.0 is the FALLBACK, not a reading.
            // Ample throughput AND verdict samples, so either wrong counter
            // promotes it.
            (":tput_only", 0, 9, 9, 0.0, 50.0),
            // The mirror: latency observed, no token counts, NO verdict cast.
            // The zero `verdict_samples` is what kills the verdict mutation.
            (":lat_only", 9, 0, 0, 100.0, 0.0),
            (":slow", 9, 9, 9, 900.0, 1.0),
        ]);
        let rng = SplitMix64::seeded(1);
        let mut v = vec![
            sm_cost("slow", 1, None),
            sm_cost("tput_only", 2, None),
            sm_cost("lat_only", 3, None),
        ];
        MetricStrategy::latency().order(&mut v, &test_ctx(&perf, &rng));
        assert_eq!(
            names(&v),
            vec!["lat_only", "tput_only", "slow"],
            "`tput_only` is unmeasured FOR LATENCY and must hold slot 1 rather \
             than lead on a 0.0 fallback; `lat_only` is measured at 100ms and \
             must beat `slow` at 900ms despite having no token counts and no \
             verdict"
        );
    }

    /// I2 — a non-finite reading is not a measurement.
    ///
    /// A `NaN` compares `None` against everything, so `unwrap_or(Equal)` makes
    /// it EQUAL to every other candidate — an intransitive comparator, which
    /// `sort_by` panics on. Fencing it at the source reclassifies it as
    /// unmeasured, which routes it down the already-tested hold-its-index path.
    ///
    /// MIXED `NaN`/finite, deliberately: an all-`NaN` fixture measures nothing,
    /// because everything comparing Equal to everything IS a consistent order
    /// and does not panic. Swept across `n` because the panic is non-monotone
    /// in it.
    #[test]
    fn a_non_finite_reading_is_not_a_measurement() {
        for n in [3usize, 25, 30, 60] {
            let nan_at = n / 2;
            let perf = FnStats(move |endpoint: &str| {
                let i = endpoint_idx(endpoint);
                Some(EndpointStats {
                    samples: 9,
                    throughput_samples: 9,
                    verdict_samples: 9,
                    mean_latency_ms: if i == nan_at {
                        f64::NAN
                    } else {
                        (n - i) as f64
                    },
                    mean_tokens_per_sec: 1.0,
                    success_rate: 1.0,
                })
            });
            let rng = SplitMix64::seeded(1);
            let mut v: Vec<SelectedModel> = (0..n)
                .map(|i| sm_cost(&format!("m{i:02}"), (i + 1) as u8, None))
                .collect();
            MetricStrategy::latency().order(&mut v, &test_ctx(&perf, &rng));

            // Latency is `n - i`, so ascending latency is descending index. The
            // NaN candidate is not a measurement, so it holds its INDEX and the
            // finite ones sort among the remaining slots.
            let mut expected: Vec<String> = (0..n)
                .rev()
                .filter(|&i| i != nan_at)
                .map(|i| format!("m{i:02}"))
                .collect();
            expected.insert(nan_at, format!("m{nan_at:02}"));
            assert_eq!(names(&v), expected, "n={n}");
        }
    }

    /// I2, the `PriceStrategy` half — and it needs no exotic fixture to reach.
    /// `estimate_cost` is `input_cost + output_cost` over three unvalidated
    /// `f64`s, so `input_per_1k: 1e308` with `output_per_1k: -1e308` yields
    /// `inf + -inf` = `NaN` straight from config.
    ///
    /// ALL-EQUAL priorities, which is the point: `.then(a.priority.cmp(&b))`
    /// does NOT rescue an intransitive comparator, and equal priority is
    /// exactly the load-balancing case an explicit `sort` is allowed to have.
    ///
    /// The fence maps non-finite to `+inf` so an unusable price sorts LAST.
    /// `None` means free, but a NON-FINITE price is not a price at all, and
    /// treating it as free would let a broken price win the CHEAPEST slot — a
    /// budget hazard, not a neutral default.
    #[test]
    fn a_non_finite_price_sorts_last_and_never_panics() {
        for n in [3usize, 25, 30, 60] {
            let nan_at = n / 2;
            let rng = SplitMix64::seeded(1);
            let mut v: Vec<SelectedModel> = (0..n)
                .map(|i| {
                    let cost = if i == nan_at {
                        f64::NAN
                    } else {
                        (n - i) as f64
                    };
                    sm_cost(&format!("m{i:02}"), 1, Some(cost))
                })
                .collect();
            PriceStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));

            let mut expected: Vec<String> = (0..n)
                .rev()
                .filter(|&i| i != nan_at)
                .map(|i| format!("m{i:02}"))
                .collect();
            expected.push(format!("m{nan_at:02}"));
            assert_eq!(names(&v), expected, "n={n}: an unusable price sorts last");
        }
    }

    /// I3 — candidates with EQUAL readings fall back to authored priority.
    ///
    /// That fallback is the whole "partial ⇒ a monotone interpolation between
    /// priority order and metric order" story, and nothing pinned it: adding a
    /// REVERSED priority tiebreak to the subset comparator passed all 27 Task 9
    /// tests. Eight buckets of eight share an identical reading, so within a
    /// bucket only the tiebreak can decide.
    #[test]
    fn equal_metric_values_keep_authored_priority_order() {
        const N: usize = 64;
        let perf = FnStats(|endpoint: &str| {
            Some(EndpointStats {
                samples: 9,
                throughput_samples: 9,
                verdict_samples: 9,
                // Eight buckets of eight, each bucket wholly tied.
                mean_latency_ms: ((endpoint_idx(endpoint) / 8) * 100) as f64,
                mean_tokens_per_sec: 1.0,
                success_rate: 1.0,
            })
        });
        let rng = SplitMix64::seeded(1);
        // Authored in REVERSE, so input order cannot be mistaken for the answer.
        let mut v: Vec<SelectedModel> = (0..N)
            .rev()
            .map(|i| sm_cost(&format!("m{i:02}"), (i + 1) as u8, None))
            .collect();
        MetricStrategy::latency().order(&mut v, &test_ctx(&perf, &rng));
        let expected: Vec<String> = (0..N).map(|i| format!("m{i:02}")).collect();
        assert_eq!(
            names(&v),
            expected,
            "within a tied bucket the authored priority decides, so the full \
             ascending order must be recovered from a reversed input"
        );
    }

    /// Mi1 — `None` means free, i.e. EQUAL to an explicit `Some(0.0)` with
    /// priority deciding between them. Every earlier fixture pitted `None`
    /// against a POSITIVE price, so `unwrap_or(-1.0)` — "unpriced beats even
    /// free" — passed them all.
    ///
    /// Two unpriced candidates, so their relative order is pinned too.
    #[test]
    fn unpriced_ties_with_an_explicit_zero_price() {
        for unpriced_first in [true, false] {
            let rng = SplitMix64::seeded(7);
            let mut v = if unpriced_first {
                vec![
                    sm_cost("unpriced_late", 9, None),
                    sm_cost("unpriced_mid", 5, None),
                    sm_cost("explicit_zero_early", 1, Some(0.0)),
                ]
            } else {
                vec![
                    sm_cost("explicit_zero_early", 1, Some(0.0)),
                    sm_cost("unpriced_mid", 5, None),
                    sm_cost("unpriced_late", 9, None),
                ]
            };
            PriceStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));
            assert_eq!(
                names(&v),
                vec!["explicit_zero_early", "unpriced_mid", "unpriced_late"],
                "unpriced_first {unpriced_first}: unpriced is free, so it TIES \
                 an explicit 0.0 and authored priority decides — it does not \
                 outrank it"
            );
        }
    }
}
