use crate::circuit_breaker::CircuitBreakerManager;
use crate::gates::budget::BudgetGate;
use crate::gates::capability::CapabilityGate;
use crate::gates::circuit_breaker_gate::CircuitBreakerGate;
use crate::gates::cooldown::ConnectionCooldownGate;
use crate::gates::{
    AdmissionGate, CandidateView, EndpointHealthRead, GateVerdict, RouterHealthRead, SelectionCtx,
};
use crate::skip_reason::SkipReason;
use crate::strategy::RoutingStrategy;
use crate::types::capability::Capability;
use crate::types::config::{
    ChainEntry, FallbackChainConfig, GatewayConfig, ModelConfig, RouterConfig,
};
use crate::types::cost::CostEstimate;
use crate::types::request::RoutingPreferences;
use crate::types::trace::{RoutedCandidate, RoutingDecision};
use std::time::Instant;

/// Criteria used to resolve which model(s) to try.
#[derive(Debug, Clone)]
pub struct SelectionCriteria {
    pub capability: Capability,
    pub model: Option<String>,
    pub router: Option<String>,
    pub chain: Option<String>,
    pub budget: Option<f64>,
    pub input_tokens: Option<u32>,
    /// The pessimistic input estimate, read only by the
    /// [`crate::gates::context_window::ContextWindowGate`].
    ///
    /// A SECOND field beside `input_tokens` rather than a replacement for it, for the
    /// reason argued in `engine::util::estimate_input_tokens_pessimistic`: the cost gate
    /// and the window gate want opposite biases over the same payload, so collapsing
    /// them to one number is precisely what that argument rules out. `None` admits every
    /// candidate — a caller that reaches selection without an estimate is not making a
    /// claim about size, and refusing it would be a filter on missing data.
    pub input_tokens_pessimistic: Option<u32>,
    /// Per-request routing preferences (SP-ROUTE-1). `None` ⇒ default routing.
    pub preferences: Option<RoutingPreferences>,
}

/// The `"{router}:{model}"` key every read/write performance, health, cooldown,
/// and lockout port is keyed by. ONE function, so a separator change (or a
/// Task 9 lookup that needs a fourth call site) touches one place instead of
/// silently drifting between the two `CandidateView` construction sites below
/// and the two engine call sites (`engine::execute`, `engine::stream`) that
/// used to each spell `format!("{}:{}", ...)` out by hand.
pub(crate) fn endpoint_key(router: &str, model: &str) -> String {
    format!("{router}:{model}")
}

/// A model that passed all validation checks and is ready for execution.
#[derive(Debug, Clone)]
pub struct SelectedModel {
    pub model: String,
    pub router: String,
    pub router_config: RouterConfig,
    pub model_config: ModelConfig,
    pub api_model_id: String,
    pub priority: u8,
    pub cost_estimate: Option<CostEstimate>,
}

impl SelectedModel {
    /// The same `"{router}:{model}"` key `admit` built while gating this
    /// candidate — see [`endpoint_key`].
    pub(crate) fn endpoint_key(&self) -> String {
        endpoint_key(&self.router, &self.model)
    }
}

/// A candidate that was considered but rejected during validation.
#[derive(Debug, Clone)]
pub struct SkippedCandidate {
    pub model: String,
    pub router: String,
    pub reason: SkipReason,
}

/// The result of model selection, containing the chosen model plus diagnostics.
#[derive(Debug)]
pub struct SelectionResult {
    pub selected: Option<SelectedModel>,
    pub all_candidates: Vec<SelectedModel>,
    pub skipped: Vec<SkippedCandidate>,
    pub chain: Option<FallbackChainConfig>,
    /// Why the candidates came out in this order (SP-ROUTE-1 AC10).
    ///
    /// `Some` on the chain and capability paths — the ones that RUN a
    /// [`RoutingStrategy`] — and `None` on the direct and not-found paths,
    /// which order nothing. A direct `router` + `model` request names its one
    /// candidate outright; there is no decision to explain.
    pub decision: Option<RoutingDecision>,
}

/// Resolves which model(s) to use for a given request via 3-tier resolution
/// (direct, named chain, capability). Structural resolution (router/model
/// lookup) happens per path; the shared admission pipeline then runs the
/// ordered [`AdmissionGate`]s (routing policy, capability, connection cooldown,
/// circuit breaker, model lockout, budget, context window) and the
/// [`RoutingStrategy`] orders the admitted candidates. The list below is the one
/// place these are registered — keep every enumeration in this file in step with it.
pub struct ModelSelectionService<'a> {
    config: &'a GatewayConfig,
    /// Ordered admission gates: routing policy, capability, connection cooldown,
    /// circuit breaker, model lockout, budget, context window.
    gates: Vec<Box<dyn AdmissionGate>>,
    /// Endpoint health read port (the circuit breaker implements it).
    health: &'a dyn EndpointHealthRead,
    /// Router health read port (the connection cooldown store implements it).
    router_health: &'a dyn RouterHealthRead,
    /// Endpoint model-lockout read port (the model-lockout store implements it).
    model_lockout: &'a dyn crate::gates::lockout::ModelLockoutRead,
    /// Forces [`Self::strategy_for`]'s answer, bypassing `sort` resolution.
    ///
    /// Test-only. There is no production field holding "the" strategy any more:
    /// since Task 10 the strategy is resolved PER REQUEST from
    /// `criteria.preferences.sort`, so a fixed field would be exactly the thing
    /// that cannot express the feature. This exists so a test can install a
    /// probe strategy that reads the `StrategyCtx` back out (see
    /// [`tests::the_builders_install_the_ports_the_strategy_sees`]) — the one
    /// claim no real strategy can make about itself.
    #[cfg(test)]
    strategy_override: Option<Box<dyn RoutingStrategy>>,
    /// Performance read port. Defaults to the null port so a caller that never
    /// wires performance behaves exactly as before this slice.
    perf: &'a dyn crate::gates::performance::EndpointPerformanceRead,
    /// Randomness for weighted ordering (Task 7 onward).
    rng: &'a dyn crate::random::RandomSource,
    min_samples: u32,
}

static NO_PERF: crate::gates::performance::NoPerformance = crate::gates::performance::NoPerformance;
/// A FIXED seed, reached only by callers that construct the service directly.
///
/// This does NOT give a test a reproducible draw: the static is process-wide and
/// cargo runs tests in parallel, so the sequence is fixed but which test receives
/// which value is not. A test that depends on specific draws MUST pass its own
/// `SplitMix64::seeded(n)` via `with_random`. Both production paths
/// (`engine::execute`, `engine::stream`) pass the gateway's entropy-seeded source
/// via `with_random` from Task 10 — see
/// `engine::tests::production_selection_never_uses_the_fixed_seed_default`, the
/// tripwire that catches a caller that forgets to.
static DEFAULT_RNG: crate::random::SplitMix64 = crate::random::SplitMix64::seeded(0x5EED_5EED);

impl<'a> ModelSelectionService<'a> {
    pub fn new(
        config: &'a GatewayConfig,
        circuit_breaker: &'a CircuitBreakerManager,
        router_health: &'a dyn RouterHealthRead,
        model_lockout: &'a dyn crate::gates::lockout::ModelLockoutRead,
    ) -> Self {
        Self {
            config,
            gates: vec![
                // FIRST, deliberately — see `RoutingPolicyGate`'s doc comment.
                Box::new(crate::gates::routing_policy::RoutingPolicyGate),
                Box::new(CapabilityGate),
                Box::new(ConnectionCooldownGate),
                Box::new(CircuitBreakerGate),
                Box::new(crate::gates::lockout::ModelLockoutGate),
                Box::new(BudgetGate),
                // LAST. The vector is ordered and `admit` returns the FIRST skip, so this
                // position decides which reason a multiply-gated candidate reports — and
                // that is a behaviour, not a presentation detail: `gate_status()` makes
                // `CircuitOpen`/`Cooling`/a timed lockout `Timed`, which becomes
                // `AllGated { resume_after: Some(t) }` and a TIMED pause at the
                // orchestrator's `classify_gateway_error`, while `OverContextWindow` is
                // `Terminal` — `resume_after: None`, which since the M1 reversal is the
                // indefinite HOTL pause rather than a `NodeFailed`.
                //
                // **After the three HEALTH gates (cooldown, breaker, lockout), and that
                // is the load-bearing half.** A candidate that is both over-window and
                // circuit-open must report the BREAKER, because that one clears BY
                // ITSELF. Reporting the window instead swaps a pause the scheduler wakes
                // on its own for one that waits on a human who has nothing to do — the
                // breaker would have closed unaided — so a transient provider outage
                // stalls the run until somebody notices. (Before the M1 reversal the same
                // mistake killed the run outright, which is why this comment used to say
                // "permanently dead". The ordering is load-bearing either way.) Pinned by
                // `a_health_skip_is_reported_ahead_of_the_window_for_the_same_candidate`
                // and, at the engine boundary where the two pause KINDS are visible, by
                // `engine::tests::an_over_window_candidate_whose_breaker_is_open_still_lets_the_run_pause`.
                //
                // **After `BudgetGate` too, and that half is a JUDGEMENT with a cost.**
                // An earlier version of this comment claimed every gate ahead of this one
                // is "either structural or health", and that is simply false: `OverBudget`
                // is `Terminal(RaiseBudget)` and a `CreditsExhausted`/auth lockout is
                // `Terminal(TopUpCredits/RotateCredential)`. `all_gated_error` keeps the
                // FIRST terminal remedy it meets, so a request that is over budget AND
                // over every window is reported as `RaiseBudget` and says nothing about
                // the window: the operator raises the cap, retries, and only then learns
                // the prompt does not fit. Accepted deliberately — money is the
                // irreversible lever, and a caller that has set a cap wants to hear about
                // the cap first — but it is a two-step diagnosis, not a free ordering,
                // and `a_budget_skip_is_reported_ahead_of_the_window` pins it so the
                // choice cannot drift by accident.
                Box::new(crate::gates::context_window::ContextWindowGate),
            ],
            health: circuit_breaker,
            router_health,
            model_lockout,
            #[cfg(test)]
            strategy_override: None,
            perf: &NO_PERF,
            rng: &DEFAULT_RNG,
            // The SAME constant `ResilienceConfig::default` uses, not a second
            // literal `3` that happens to agree with it. Two copies of a default
            // are a drift hazard with no upside: a tuned default would move one
            // and silently leave a caller who never calls `with_performance`
            // (every unit test in this file) judging on the old threshold.
            min_samples: crate::resilience::DEFAULT_MIN_SAMPLES,
        }
    }

    /// The strategy for THIS request. An explicit `sort` replaces the default;
    /// `order` (applied in [`Self::resolve_chain`]) is layered on top of
    /// whichever ran.
    ///
    /// Resolved per request rather than held in a field, which is the whole of
    /// Task 10: `sort` is a property of the CALLER's request, so a service-wide
    /// strategy could only ever express the default. The `None` arm is what
    /// keeps this additive — a request carrying no `sort` gets
    /// [`crate::strategy::GroupedWeightedStrategy`], byte-identically to before
    /// this task.
    ///
    /// Each arm is pinned by `tests::each_sort_value_resolves_to_its_own_strategy`
    /// on a fixture whose price, latency, throughput and priority orders are four
    /// mutually distinct permutations — the only shape in which an arm returning
    /// the WRONG strategy (or falling through to the default) is observable.
    fn strategy_for(&self, criteria: &SelectionCriteria) -> Box<dyn RoutingStrategy + '_> {
        use crate::types::request::SortKey;
        // Test-only, and deliberately ahead of the `sort` match: a probe
        // strategy must see every request regardless of what it asks for.
        #[cfg(test)]
        if let Some(s) = &self.strategy_override {
            return Box::new(&**s);
        }
        match criteria.preferences.as_ref().and_then(|p| p.sort) {
            Some(SortKey::Price) => Box::new(crate::strategy::PriceStrategy),
            Some(SortKey::Latency) => Box::new(crate::strategy::MetricStrategy::latency()),
            Some(SortKey::Throughput) => Box::new(crate::strategy::MetricStrategy::throughput()),
            None => Box::new(crate::strategy::GroupedWeightedStrategy),
        }
    }

    /// Performance read port, plus the minimum live-sample count a metric sort
    /// requires before treating a candidate as measured (Task 8/9).
    pub fn with_performance(
        mut self,
        perf: &'a dyn crate::gates::performance::EndpointPerformanceRead,
        min_samples: u32,
    ) -> Self {
        self.perf = perf;
        self.min_samples = min_samples;
        self
    }

    /// Randomness for weighted ordering (Task 7 onward).
    pub fn with_random(mut self, rng: &'a dyn crate::random::RandomSource) -> Self {
        self.rng = rng;
        self
    }

    /// Whether this service still holds the fixed-seed `DEFAULT_RNG` — i.e.
    /// `with_random` was never called (or silently discarded its argument).
    /// Pointer identity against the process-wide static, not a value
    /// comparison: two *different* `SplitMix64::seeded(0x5EED_5EED)` instances
    /// would compare unequal here, which is exactly what this needs to detect
    /// — "is this THE default" rather than "is this seeded the same as it".
    /// Test-only; see `engine::tests::production_selection_never_uses_the_fixed_seed_default`.
    #[cfg(test)]
    pub(crate) fn uses_default_rng(&self) -> bool {
        std::ptr::eq(
            self.rng as *const _ as *const (),
            &DEFAULT_RNG as *const _ as *const (),
        )
    }

    /// Select the first valid candidate.
    pub fn select(&self, criteria: &SelectionCriteria) -> SelectionResult {
        let mut result = self.resolve_candidates(criteria);
        result.selected = result.all_candidates.first().cloned();
        result
    }

    /// Select all valid candidates (for fallback chains).
    pub fn select_all(&self, criteria: &SelectionCriteria) -> SelectionResult {
        let mut result = self.resolve_candidates(criteria);
        result.selected = result.all_candidates.first().cloned();
        result
    }

    /// Estimate the cost for a model given the criteria.
    fn estimate_cost(
        &self,
        model_config: &ModelConfig,
        criteria: &SelectionCriteria,
    ) -> Option<CostEstimate> {
        let pricing = model_config.pricing.as_ref()?;
        let input_tokens = criteria.input_tokens.unwrap_or(0);
        let max_output_tokens = model_config.max_output_tokens;

        let input_cost = input_tokens as f64 * pricing.input_per_1k / 1000.0;
        let output_cost = max_output_tokens as f64 * pricing.output_per_1k / 1000.0;
        let estimated = input_cost + output_cost;

        Some(CostEstimate {
            estimated,
            minimum: input_cost, // minimum: only input, no output
            maximum: estimated,  // maximum: full output budget used
            currency: "USD".to_string(),
            model: model_config.id.clone(),
        })
    }

    /// Shared admission path: run each gate in order over a structurally
    /// resolved candidate. On the first `Skip(reason)` return the reason; on
    /// all-Admit build the `SelectedModel`, attaching the full `CostEstimate`
    /// (the `BudgetGate` independently computes an f64 from the same formula).
    fn admit(
        &self,
        cand: CandidateView<'_>,
        api_model_id: String,
        priority: u8,
        criteria: &SelectionCriteria,
    ) -> Result<SelectedModel, SkipReason> {
        let ctx = SelectionCtx {
            capability: criteria.capability.clone(),
            budget: criteria.budget,
            input_tokens: criteria.input_tokens,
            input_tokens_pessimistic: criteria.input_tokens_pessimistic,
            health: self.health,
            now: Instant::now(),
            config: self.config,
            router_health: self.router_health,
            model_lockout: self.model_lockout,
            preferences: criteria.preferences.as_ref(),
        };
        for gate in &self.gates {
            if let GateVerdict::Skip(reason) = gate.evaluate(&cand, &ctx) {
                return Err(reason);
            }
        }

        let cost_estimate = self.estimate_cost(cand.model_config, criteria);
        Ok(SelectedModel {
            model: cand.model.to_string(),
            router: cand.router.to_string(),
            router_config: cand.router_config.clone(),
            model_config: cand.model_config.clone(),
            api_model_id,
            priority,
            cost_estimate,
        })
    }

    /// Core resolution: determine candidates based on the 3-tier strategy,
    /// then validate each one through the pipeline.
    fn resolve_candidates(&self, criteria: &SelectionCriteria) -> SelectionResult {
        // Tier 1: Direct (router + model specified)
        if criteria.router.is_some() || (criteria.model.is_some() && criteria.chain.is_none()) {
            return self.resolve_direct(criteria);
        }

        // Tier 2: Named chain
        if let Some(chain_name) = &criteria.chain {
            if let Some(chain) = self.config.chains.get(chain_name) {
                return self.resolve_chain(chain, criteria);
            }
            return SelectionResult {
                selected: None,
                all_candidates: vec![],
                skipped: vec![],
                chain: None,
                // No chain resolved ⇒ no strategy ran ⇒ nothing to explain.
                decision: None,
            };
        }

        // Tier 3: Capability — find chain matching the capability
        self.resolve_by_capability(criteria)
    }

    /// Tier 1: Direct resolution — validate a single router+model pair.
    fn resolve_direct(&self, criteria: &SelectionCriteria) -> SelectionResult {
        let router_name = criteria.router.clone().unwrap_or_default();
        let model_name = criteria.model.clone().unwrap_or_default();

        match self.validate_direct(&router_name, &model_name, criteria) {
            Ok(selected) => SelectionResult {
                selected: None, // filled by caller
                all_candidates: vec![selected],
                skipped: vec![],
                chain: None,
                // A direct request names its one candidate outright; no
                // strategy ordered anything, so there is no decision to record.
                decision: None,
            },
            Err(reason) => SelectionResult {
                selected: None,
                all_candidates: vec![],
                skipped: vec![SkippedCandidate {
                    model: model_name,
                    router: router_name,
                    reason,
                }],
                chain: None,
                decision: None,
            },
        }
    }

    /// Structural resolution for tier 1: router-first. Look up the router
    /// BEFORE the model (empty/missing → `RouterNotFound`, disabled →
    /// `RouterDisabled`), then the model (missing → `ModelNotFound`). No
    /// provider fallback. `priority = 1`; `api_model_id` is 2-level
    /// (model_config override else model id). The shared gate pipeline
    /// (routing policy, capability, connection cooldown, circuit breaker, model
    /// lockout, budget, context window) runs in [`Self::admit`].
    fn validate_direct(
        &self,
        router_name: &str,
        model_name: &str,
        criteria: &SelectionCriteria,
    ) -> Result<SelectedModel, SkipReason> {
        // Validate router exists and is enabled (router-first).
        let router_config = self
            .config
            .routers
            .get(router_name)
            .ok_or(SkipReason::RouterNotFound)?;
        if !router_config.enabled {
            return Err(SkipReason::RouterDisabled);
        }

        // Validate model exists.
        let model_config = self
            .config
            .models
            .get(model_name)
            .ok_or(SkipReason::ModelNotFound)?;

        let api_model_id = model_config
            .api_model_id
            .clone()
            .unwrap_or_else(|| model_name.to_string());

        let cand = CandidateView {
            model: model_name,
            router: router_name,
            endpoint: endpoint_key(router_name, model_name),
            model_config,
            router_config,
        };
        self.admit(cand, api_model_id, 1, criteria)
    }

    /// Tier 2/3: Walk chain entries, structurally resolving + gating each, then
    /// order the admitted candidates via the strategy (SP-0: priority
    /// ascending, stable — identical to the previous hardcoded entry sort,
    /// since a stable sort of the admitted subset preserves the same order).
    fn resolve_chain(
        &self,
        chain: &FallbackChainConfig,
        criteria: &SelectionCriteria,
    ) -> SelectionResult {
        let mut all_candidates = Vec::new();
        let mut skipped = Vec::new();

        for entry in &chain.models {
            match self.validate_chain_entry(entry, criteria) {
                Ok(candidate) => all_candidates.push(candidate),
                Err(candidate) => skipped.push(candidate),
            }
        }

        let ctx = crate::strategy::StrategyCtx {
            perf: self.perf,
            rng: self.rng,
            min_samples: self.min_samples,
        };
        // Bound rather than called inline, because the decision below needs the
        // NAME of the very object that produced this ordering. The alternative
        // — a second `match` on `SortKey` at the trace site — compiles, agrees
        // on the day it is written, and is then free to drift from
        // `strategy_for` forever after. This way there is one `match` in the
        // crate and the reported name is the strategy that did the work.
        let strategy = self.strategy_for(criteria);
        let report = strategy.order(&mut all_candidates, &ctx);

        // `order` is layered ON TOP of whichever strategy ran, and gives exactly
        // the specified semantics: named candidates lead in ref order,
        // candidates sharing a ref keep the strategy's relative order, and
        // unmatched candidates (rank `usize::MAX`) follow as fallbacks in the
        // strategy's order. `order` therefore never FILTERS — `only`/`ignore`
        // (the `RoutingPolicyGate`) are the knobs that restrict, and they have
        // already run by here.
        //
        // **The key is `(rank, position-in-the-strategy's-output)`, and the
        // second component is what makes this correct rather than merely
        // correct-today.** Keyed on `rank` alone the specified answer holds only
        // because the sort is STABLE, which made `sort_by_key` →
        // `sort_unstable_by_key` a live defect that the whole suite missed: an
        // operator's authored sequence was silently scrambled for exactly the
        // long chains where writing one out is worth the trouble. Carrying the
        // index makes every key DISTINCT, so the tie-break is in the data and
        // any correct sort — stable or not — produces the specified order.
        //
        // The rank is computed ONCE PER CANDIDATE, before sorting, not inside
        // the comparator: `sort_by_key` invokes its key function once per
        // COMPARISON, so the scan over `refs` would run `O(n log n · |refs|)`
        // times per selection instead of `O(n · |refs|)`. (`sort_by_cached_key`
        // fixes that much on its own but cannot express this key, which needs
        // each element's index.) Do NOT be tempted to consult a port here
        // either: Task 9 shipped a panic by reading a live store from inside a
        // comparator — `stats()` recomputes from `Instant::now()`, so a key
        // could change between two comparisons, and `sort_by` detects the
        // resulting intransitivity and panics inside model selection.
        if let Some(refs) = criteria.preferences.as_ref().and_then(|p| p.order.as_ref()) {
            // FIRST match wins: `[{model: charlie}, {router: north}]` reads
            // "charlie, then the rest of north", so a candidate matching both
            // takes the earlier rank. `rposition` would give charlie the LATER
            // one, tying it with the rest of north and losing its lead.
            let rank_of = |m: &SelectedModel| {
                refs.iter()
                    .position(|r| {
                        r.router.as_deref().is_none_or(|x| x == m.router)
                            && r.model.as_deref().is_none_or(|x| x == m.model)
                    })
                    .unwrap_or(usize::MAX)
            };
            let mut ranked: Vec<(usize, usize, SelectedModel)> =
                std::mem::take(&mut all_candidates)
                    .into_iter()
                    .enumerate()
                    .map(|(i, m)| (rank_of(&m), i, m))
                    .collect();
            ranked.sort_by_key(|(rank, i, _)| (*rank, *i));
            all_candidates = ranked.into_iter().map(|(_, _, m)| m).collect();
        }

        // AC10 — record WHY the candidates came out in this order.
        //
        // Built HERE, after the `order` re-rank, and that placement is
        // load-bearing: the decision must describe the sequence the engine will
        // actually try. Built beside `strategy.order(...)` above — the obvious
        // place — it would record the strategy's output and then be silently
        // contradicted by the re-rank, which is a trace that lies about the one
        // thing it exists to explain.
        //
        // `reliability` and `weight` are READ OUT OF `report`, never recomputed.
        // `ctx.perf` is a live rolling window whose means are recalculated from
        // `Instant::now()` on every call, so a second read is not guaranteed to
        // return what the ordering used — and a trace describing an ordering
        // that never happened fails silently, unlike the Task 9 comparator
        // panic that came from the same live-read hazard.
        let decision = RoutingDecision {
            strategy: strategy.name().to_string(),
            degraded: report.degraded,
            order: all_candidates
                .iter()
                .map(|m| {
                    let endpoint = m.endpoint_key();
                    let weighed = report.weights.iter().find(|w| w.endpoint == endpoint);
                    RoutedCandidate {
                        priority: m.priority,
                        cost: m.cost_estimate.as_ref().map(|c| c.estimated),
                        // `None` both when the strategy weighed nothing and
                        // when it weighed this candidate against no measurement
                        // — see `RoutedCandidate`, which documents the
                        // difference the `strategy` field resolves.
                        reliability: weighed.and_then(|w| w.reliability),
                        weight: weighed.and_then(|w| w.weight),
                        endpoint,
                    }
                })
                .collect(),
        };

        SelectionResult {
            selected: None, // filled by caller
            all_candidates,
            skipped,
            chain: Some(chain.clone()),
            decision: Some(decision),
        }
    }

    /// Structural resolution for a single chain entry: model-first. Look up the
    /// model (missing → `ModelNotFound`), resolve the router from the entry
    /// (falling back to the model's provider), then validate it (missing →
    /// `RouterNotFound`, disabled → `RouterDisabled`). `priority = entry.priority`;
    /// `api_model_id` is 3-level (entry override → model_config → model id). The
    /// shared gate pipeline (routing policy, capability, connection cooldown,
    /// circuit breaker, model lockout, budget, context window) runs in
    /// [`Self::admit`].
    fn validate_chain_entry(
        &self,
        entry: &ChainEntry,
        criteria: &SelectionCriteria,
    ) -> Result<SelectedModel, SkippedCandidate> {
        let model_name = &entry.model;

        // Look up the model config (model-first).
        let model_config = self.config.models.get(model_name).ok_or_else(|| {
            let router_name = entry
                .router
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            SkippedCandidate {
                model: model_name.clone(),
                router: router_name,
                reason: SkipReason::ModelNotFound,
            }
        })?;

        // Resolve router: chain entry router, else model's provider.
        let router_name = entry
            .router
            .clone()
            .unwrap_or_else(|| model_config.provider.clone());

        // Validate router exists and is enabled.
        let router_config =
            self.config
                .routers
                .get(&router_name)
                .ok_or_else(|| SkippedCandidate {
                    model: model_name.clone(),
                    router: router_name.clone(),
                    reason: SkipReason::RouterNotFound,
                })?;

        if !router_config.enabled {
            return Err(SkippedCandidate {
                model: model_name.clone(),
                router: router_name,
                reason: SkipReason::RouterDisabled,
            });
        }

        // Resolve API model ID: chain entry override, else model config, else model id.
        let api_model_id = entry
            .api_model_id
            .clone()
            .or_else(|| model_config.api_model_id.clone())
            .unwrap_or_else(|| model_name.clone());

        let cand = CandidateView {
            model: model_name,
            router: &router_name,
            endpoint: endpoint_key(&router_name, model_name),
            model_config,
            router_config,
        };
        self.admit(cand, api_model_id, entry.priority, criteria)
            .map_err(|reason| SkippedCandidate {
                model: model_name.clone(),
                router: router_name.clone(),
                reason,
            })
    }

    /// Tier 3: resolve by capability when the caller pinned neither a model
    /// nor a chain.
    ///
    /// Several chains can share a capability (e.g. `classify`, `reasoning`,
    /// `summarize` are all `TextChat`). `config.chains` is a `HashMap`, whose
    /// iteration order is not stable across runs — picking "the first match"
    /// would be non-deterministic (#80). Instead, pick the lowest chain id
    /// among the matches: a stable, if arbitrary, default. Callers that need a
    /// specific chain should pin it by name (tier 2) rather than rely on this.
    fn resolve_by_capability(&self, criteria: &SelectionCriteria) -> SelectionResult {
        let chosen = self
            .config
            .chains
            .values()
            .filter(|c| c.capability == criteria.capability)
            .min_by(|a, b| a.id.cmp(&b.id));

        match chosen {
            Some(chain) => self.resolve_chain(chain, criteria),
            None => SelectionResult {
                selected: None,
                all_candidates: vec![],
                skipped: vec![],
                chain: None,
                // No chain matched the capability ⇒ no strategy ran.
                decision: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit_breaker::{CircuitBreakerConfig, CircuitBreakerManager};
    use crate::random::RandomSource;
    use crate::types::config::{
        ChainEntry, FallbackChainConfig, FallbackTrigger, ModelConfig, ModelPricing, RouterConfig,
    };
    use std::collections::HashMap;

    fn test_config() -> GatewayConfig {
        let mut routers = HashMap::new();
        routers.insert(
            "ollama".to_string(),
            RouterConfig {
                url: "http://localhost:11434".to_string(),
                api_key_env: None,
                api_key: None,
                enabled: true,
                timeout_ms: None,
                headers: HashMap::new(),
            },
        );
        routers.insert(
            "anthropic".to_string(),
            RouterConfig {
                url: "https://api.anthropic.com".to_string(),
                api_key_env: Some("ANTHROPIC_API_KEY".to_string()),
                api_key: None,
                enabled: true,
                timeout_ms: None,
                headers: HashMap::new(),
            },
        );

        let mut models = HashMap::new();
        models.insert(
            "gemma3:27b".to_string(),
            ModelConfig {
                id: "gemma3:27b".to_string(),
                api_model_id: None,
                provider: "ollama".to_string(),
                family: None,
                capabilities: vec![
                    Capability::TextChat,
                    Capability::TextComplete,
                    Capability::TextEmbed,
                ],
                context_window: 128000,
                max_output_tokens: 8192,
                pricing: None,
                catalog: None,
            },
        );
        models.insert(
            "all-minilm".to_string(),
            ModelConfig {
                id: "all-minilm".to_string(),
                api_model_id: None,
                provider: "ollama".to_string(),
                family: None,
                capabilities: vec![Capability::TextEmbed],
                context_window: 512,
                max_output_tokens: 0,
                pricing: None,
                catalog: None,
            },
        );
        models.insert(
            "claude-haiku".to_string(),
            ModelConfig {
                id: "claude-haiku".to_string(),
                api_model_id: Some("claude-haiku-4-5-20251001".to_string()),
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
                catalog: None,
            },
        );

        let mut chains = HashMap::new();
        chains.insert(
            "embed_chain".to_string(),
            FallbackChainConfig {
                id: "embed_chain".to_string(),
                capability: Capability::TextEmbed,
                models: vec![ChainEntry {
                    model: "all-minilm".to_string(),
                    router: None,
                    api_model_id: None,
                    priority: 1,
                }],
                fallback_triggers: vec![],
            },
        );
        chains.insert(
            "chat_chain".to_string(),
            FallbackChainConfig {
                id: "chat_chain".to_string(),
                capability: Capability::TextChat,
                models: vec![
                    ChainEntry {
                        model: "gemma3:27b".to_string(),
                        router: None,
                        api_model_id: None,
                        priority: 1,
                    },
                    ChainEntry {
                        model: "claude-haiku".to_string(),
                        router: None,
                        api_model_id: None,
                        priority: 2,
                    },
                ],
                fallback_triggers: vec![FallbackTrigger::Timeout, FallbackTrigger::ProviderError],
            },
        );

        GatewayConfig {
            routers,
            models,
            chains,
            constraints: Default::default(),
            panels: Default::default(),
            consensus: Default::default(),
        }
    }

    fn test_cb() -> CircuitBreakerManager {
        CircuitBreakerManager::new(CircuitBreakerConfig::default())
    }

    #[test]
    fn tier1_direct_selection() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("gemma3:27b".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });

        assert!(result.selected.is_some());
        let selected = result.selected.unwrap();
        assert_eq!(selected.model, "gemma3:27b");
        assert_eq!(selected.router, "ollama");
    }

    #[test]
    fn tier1_direct_unknown_router() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("gemma3:27b".to_string()),
            router: Some("nonexistent".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::RouterNotFound
        ));
    }

    #[test]
    fn tier2_chain_selection() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });

        assert_eq!(result.all_candidates.len(), 2);
        assert_eq!(result.all_candidates[0].model, "gemma3:27b");
        assert_eq!(result.all_candidates[0].priority, 1);
        assert_eq!(result.all_candidates[1].model, "claude-haiku");
        assert_eq!(result.all_candidates[1].priority, 2);
        assert!(result.chain.is_some());
    }

    #[test]
    fn tier3_capability_selection() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextEmbed,
            model: None,
            router: None,
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });

        assert!(result.selected.is_some());
        let selected = result.selected.unwrap();
        assert_eq!(selected.model, "all-minilm");
        assert_eq!(selected.router, "ollama");
    }

    #[test]
    fn tier3_capability_is_deterministic_lowest_chain_id() {
        // Two chains share TextChat; tier-3 must pick the lowest id ("aaa_chain")
        // every time, not whatever the HashMap happens to yield first (#80).
        let mut config = test_config();
        config.chains.insert(
            "aaa_chain".to_string(),
            FallbackChainConfig {
                id: "aaa_chain".to_string(),
                capability: Capability::TextChat,
                models: vec![ChainEntry {
                    model: "claude-haiku".to_string(),
                    router: None,
                    api_model_id: None,
                    priority: 1,
                }],
                fallback_triggers: vec![],
            },
        );
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);
        // Run several times: a HashMap-order bug would flake; min_by is stable.
        for _ in 0..10 {
            let result = svc.select(&SelectionCriteria {
                capability: Capability::TextChat,
                model: None,
                router: None,
                chain: None,
                budget: None,
                input_tokens: None,
                input_tokens_pessimistic: None,
                preferences: None,
            });
            assert_eq!(result.chain.as_ref().unwrap().id, "aaa_chain");
        }
    }

    #[test]
    fn skips_disabled_router() {
        let mut config = test_config();
        config.routers.get_mut("ollama").unwrap().enabled = false;
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });

        assert!(result.selected.is_some());
        let selected = result.selected.unwrap();
        assert_eq!(selected.model, "claude-haiku");
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::RouterDisabled
        ));
        assert_eq!(result.skipped[0].model, "gemma3:27b");
    }

    #[test]
    fn skips_wrong_capability() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::AudioTranscribe,
            model: Some("gemma3:27b".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::UnsupportedCapability(_)
        ));
    }

    #[test]
    fn skips_circuit_breaker_open() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();

        // Open the circuit breaker for ollama:gemma3:27b
        let endpoint = "ollama:gemma3:27b";
        cb.can_execute(endpoint); // initialize
        for _ in 0..5 {
            cb.record_failure(endpoint);
        }
        assert!(!cb.can_execute(endpoint)); // confirm open

        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });

        assert!(result.selected.is_some());
        let selected = result.selected.unwrap();
        assert_eq!(selected.model, "claude-haiku");
        assert!(
            result
                .skipped
                .iter()
                .any(|s| s.model == "gemma3:27b"
                    && matches!(s.reason, SkipReason::CircuitOpen { .. }))
        );
    }

    #[test]
    fn skips_over_budget() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: Some(0.001),
            input_tokens: Some(1000),
            input_tokens_pessimistic: None,
            preferences: None,
        });

        // gemma3:27b has no pricing -> passes budget (free)
        // claude-haiku has pricing -> estimate = 0.0008 + 0.004*8192/1000 = 0.0008 + 32.768 ≈ 33.5488
        // which is way over budget 0.001
        assert!(
            result
                .all_candidates
                .iter()
                .any(|c| c.model == "gemma3:27b")
        );
        assert!(result.skipped.iter().any(
            |s| s.model == "claude-haiku" && matches!(s.reason, SkipReason::OverBudget { .. })
        ));
    }

    #[test]
    fn api_model_id_override() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("claude-haiku".to_string()),
            router: Some("anthropic".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });

        assert!(result.selected.is_some());
        let selected = result.selected.unwrap();
        assert_eq!(selected.api_model_id, "claude-haiku-4-5-20251001");
    }

    #[test]
    fn no_chain_for_capability() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::AudioTranscribe,
            model: None,
            router: None,
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });

        assert!(result.selected.is_none());
        assert!(result.all_candidates.is_empty());
    }

    #[test]
    fn chain_not_found() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("nonexistent_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });

        assert!(result.selected.is_none());
        assert!(result.all_candidates.is_empty());
        assert!(result.chain.is_none());
    }

    #[test]
    fn direct_model_not_found() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("nonexistent_model".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::ModelNotFound
        ));
    }

    #[test]
    fn direct_model_wrong_capability() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        // all-minilm only supports TextEmbed, not AudioTranscribe
        let result = svc.select(&SelectionCriteria {
            capability: Capability::AudioTranscribe,
            model: Some("all-minilm".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::UnsupportedCapability(_)
        ));
    }

    #[test]
    fn direct_circuit_breaker_open() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();

        // Open the breaker for this direct endpoint
        let endpoint = "ollama:gemma3:27b";
        cb.can_execute(endpoint); // init
        for _ in 0..5 {
            cb.record_failure(endpoint);
        }
        assert!(!cb.can_execute(endpoint));

        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("gemma3:27b".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::CircuitOpen { .. }
        ));
    }

    #[test]
    fn direct_over_budget() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        // claude-haiku has pricing, set budget very low
        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("claude-haiku".to_string()),
            router: Some("anthropic".to_string()),
            chain: None,
            budget: Some(0.0001),
            input_tokens: Some(1000),
            input_tokens_pessimistic: None,
            preferences: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::OverBudget { .. }
        ));
    }

    #[test]
    fn direct_router_disabled() {
        let mut config = test_config();
        config.routers.get_mut("ollama").unwrap().enabled = false;
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("gemma3:27b".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::RouterDisabled
        ));
    }

    #[test]
    fn chain_entry_router_fallback_to_provider() {
        // embed_chain has entries with router=None, so it should fall back
        // to model.provider ("ollama")
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextEmbed,
            model: None,
            router: None,
            chain: Some("embed_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });

        assert_eq!(result.all_candidates.len(), 1);
        // Router should be resolved from provider
        assert_eq!(result.all_candidates[0].router, "ollama");
    }

    #[test]
    fn chain_entry_model_not_found() {
        let mut config = test_config();
        // Add a chain that references a non-existent model
        config.chains.insert(
            "bad_chain".to_string(),
            FallbackChainConfig {
                id: "bad_chain".to_string(),
                capability: Capability::TextChat,
                models: vec![
                    ChainEntry {
                        model: "ghost_model".to_string(),
                        router: Some("ollama".to_string()),
                        api_model_id: None,
                        priority: 1,
                    },
                    ChainEntry {
                        model: "gemma3:27b".to_string(),
                        router: None,
                        api_model_id: None,
                        priority: 2,
                    },
                ],
                fallback_triggers: vec![],
            },
        );
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("bad_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });

        // ghost_model should be skipped, gemma3:27b should be selected
        assert_eq!(result.all_candidates.len(), 1);
        assert_eq!(result.all_candidates[0].model, "gemma3:27b");
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::ModelNotFound
        ));
    }

    #[test]
    fn chain_entry_router_not_found() {
        let mut config = test_config();
        // Add a chain entry that specifies a non-existent router
        config.chains.insert(
            "bad_router_chain".to_string(),
            FallbackChainConfig {
                id: "bad_router_chain".to_string(),
                capability: Capability::TextChat,
                models: vec![
                    ChainEntry {
                        model: "gemma3:27b".to_string(),
                        router: Some("nonexistent_router".to_string()),
                        api_model_id: None,
                        priority: 1,
                    },
                    ChainEntry {
                        model: "claude-haiku".to_string(),
                        router: None,
                        api_model_id: None,
                        priority: 2,
                    },
                ],
                fallback_triggers: vec![],
            },
        );
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("bad_router_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });

        // gemma3:27b with nonexistent router should be skipped
        assert!(
            result
                .skipped
                .iter()
                .any(|s| s.model == "gemma3:27b" && matches!(s.reason, SkipReason::RouterNotFound))
        );
        // claude-haiku should still be available
        assert_eq!(result.all_candidates.len(), 1);
        assert_eq!(result.all_candidates[0].model, "claude-haiku");
    }

    #[test]
    fn direct_both_router_and_model_unknown_reports_router_first() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);
        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("ghost".into()),
            router: Some("nope".into()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });
        // Current behavior: direct validates the router first.
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::RouterNotFound
        ));
    }

    #[test]
    fn direct_model_only_no_router_is_router_not_found_today() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);
        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("gemma3:27b".into()),
            router: None,
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        });
        // Direct does NOT provider-fallback today → empty router → "router not found".
        assert!(result.selected.is_none());
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::RouterNotFound
        ));
    }

    // -----------------------------------------------------------------------------
    // SP-7a — the `ContextWindowGate` seen through the whole selection service.
    //
    // The gate's own unit tests (`gates/context_window.rs`) call `evaluate` directly, so
    // they pass whether or not the gate is in `ModelSelectionService::new`'s vector.
    // These do not: they go through `select_all`, which is the only place registration
    // is observable.
    // -----------------------------------------------------------------------------

    /// A `TextChat` chain of two models differing ONLY in context window — AC1's chain,
    /// and the smallest config in which the window question has two different answers.
    ///
    /// `small` is deliberately given priority **1** and `big` priority 2, so the model
    /// that CANNOT hold a large request is the one selection would otherwise return
    /// first. A test that ordered them the other way would still pass with the gate
    /// deleted.
    fn two_model_chain_windows(big: u32, small: u32) -> GatewayConfig {
        let mut routers = HashMap::new();
        routers.insert(
            "r".to_string(),
            RouterConfig {
                url: "http://localhost".to_string(),
                api_key_env: None,
                api_key: None,
                enabled: true,
                timeout_ms: None,
                headers: HashMap::new(),
            },
        );

        let mut models = HashMap::new();
        for (id, context_window) in [("big", big), ("small", small)] {
            models.insert(
                id.to_string(),
                ModelConfig {
                    id: id.to_string(),
                    api_model_id: None,
                    provider: "r".to_string(),
                    family: None,
                    capabilities: vec![Capability::TextChat],
                    context_window,
                    max_output_tokens: 4096,
                    // No pricing, so the `BudgetGate` admits both unconditionally and
                    // the only gate that can separate these two is the window one.
                    pricing: None,
                    catalog: None,
                },
            );
        }

        let mut chains = HashMap::new();
        chains.insert(
            "win_chain".to_string(),
            FallbackChainConfig {
                id: "win_chain".to_string(),
                capability: Capability::TextChat,
                models: vec![
                    ChainEntry {
                        model: "small".to_string(),
                        router: Some("r".to_string()),
                        api_model_id: None,
                        priority: 1,
                    },
                    ChainEntry {
                        model: "big".to_string(),
                        router: Some("r".to_string()),
                        api_model_id: None,
                        priority: 2,
                    },
                ],
                fallback_triggers: vec![],
            },
        );

        GatewayConfig {
            routers,
            models,
            chains,
            constraints: Default::default(),
            panels: Default::default(),
            consensus: Default::default(),
        }
    }

    /// Criteria over `win_chain` carrying only the PESSIMISTIC estimate.
    ///
    /// `input_tokens` (the cost figure) stays `None` on purpose: with no pricing in the
    /// fixture the `BudgetGate` ignores it anyway, and leaving it empty means a wiring
    /// that fed the window gate the cost field would admit everything and redden these
    /// tests instead of quietly agreeing with them.
    fn criteria_with_pessimistic(est: Option<u32>) -> SelectionCriteria {
        SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("win_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: est,
            preferences: None,
        }
    }

    /// AC1 — a heterogeneous chain serves a prompt only its larger model can hold.
    ///
    /// This is the whole slice in one assertion. Before it, the orchestrator refused
    /// this request outright against the chain's 8k MINIMUM and never asked the 128k
    /// model. The gate has to be REGISTERED for this to hold; calling it directly, as
    /// its own unit tests do, cannot tell whether it runs.
    #[test]
    fn a_chain_serves_a_prompt_only_its_larger_model_can_hold() {
        let config = two_model_chain_windows(128_000, 8_192);
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&criteria_with_pessimistic(Some(20_000)));
        let admitted: Vec<String> = result
            .all_candidates
            .iter()
            .map(|c| c.model.clone())
            .collect();
        assert!(
            admitted.contains(&"big".to_string()),
            "the 128k model holds 20k and must be admitted: {admitted:?}"
        );
        assert!(
            !admitted.contains(&"small".to_string()),
            "the 8k model cannot hold 20k and must be skipped: {admitted:?}"
        );
        assert_eq!(
            result.selected.map(|s| s.model),
            Some("big".to_string()),
            "and the request must actually be routed to it — `small` is the \
             priority-1 entry, so this is only true because the gate removed it"
        );
    }

    /// AC3 — over EVERY window is an all-gated selection, recorded with a typed reason
    /// per candidate rather than degrading to a bare `NoCandidates`.
    ///
    /// What the CALLER then receives is asserted at the engine boundary
    /// (`engine::tests::a_request_over_every_window_is_all_gated_with_the_numbers`),
    /// because `all_gated_error` lives there — and it is a terminal failure, not a
    /// pause. Here the claim is narrower and is the one selection owns: every candidate
    /// is skipped, and each skip says which window it lost to.
    #[test]
    fn a_prompt_over_every_window_gates_every_candidate() {
        let config = two_model_chain_windows(128_000, 8_192);
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&criteria_with_pessimistic(Some(200_000)));
        assert!(
            result.all_candidates.is_empty(),
            "nothing in the chain can hold 200k: {:?}",
            result
                .all_candidates
                .iter()
                .map(|c| &c.model)
                .collect::<Vec<_>>()
        );
        let windows: Vec<u32> = result
            .skipped
            .iter()
            .filter_map(|s| match s.reason {
                SkipReason::OverContextWindow { window, .. } => Some(window),
                _ => None,
            })
            .collect();
        assert_eq!(
            windows,
            vec![8_192, 128_000],
            "BOTH candidates must be skipped for the window, each naming its OWN — a \
             single chain-wide figure is exactly what this slice replaced: {:?}",
            result.skipped
        );
    }

    /// The gate's POSITION in the vector, pinned by its consequence.
    ///
    /// `admit` returns the FIRST skip, so where `ContextWindowGate` sits decides which
    /// reason a multiply-gated candidate reports — and that is not cosmetic. A skip
    /// reason's `gate_status()` is what `all_gated_error` aggregates: `CircuitOpen` is
    /// `Timed`, which becomes `AllGated { resume_after: Some(t) }` and a self-healing
    /// TIMED pause at `classify_gateway_error`; `OverContextWindow` is `Terminal`, which
    /// becomes `resume_after: None` — the indefinite HOTL pause since the M1 reversal, and
    /// a `NodeFailed` before it. So a candidate that is both over-window and circuit-open
    /// must surface the BREAKER, or a transient provider outage leaves every run whose
    /// prompt is also too large for that entry waiting on a human who has nothing to fix.
    ///
    /// Nothing enforced this before: moving `Box::new(ContextWindowGate)` from last to
    /// FIRST in `ModelSelectionService::new` left the whole workspace green, while
    /// changing this candidate's reported reason from `CircuitOpen` to
    /// `OverContextWindow`. The engine-level consequence is asserted in
    /// `engine::tests::an_over_window_candidate_whose_breaker_is_open_still_lets_the_run_pause`;
    /// this is the same claim where the ordering actually lives.
    #[test]
    fn a_health_skip_is_reported_ahead_of_the_window_for_the_same_candidate() {
        let config = two_model_chain_windows(128_000, 8_192);
        let cb = test_cb();
        // Trip `small`'s breaker Open. It is ALSO the candidate that cannot hold the
        // request below, which is the whole point: two gates fire on one candidate.
        cb.can_execute("r:small");
        for _ in 0..5 {
            cb.record_failure("r:small");
        }
        assert!(
            !cb.can_execute("r:small"),
            "the fixture needs the breaker genuinely open"
        );
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&criteria_with_pessimistic(Some(20_000)));
        let small = result
            .skipped
            .iter()
            .find(|s| s.model == "small")
            .expect("`small` is skipped twice over and must be recorded");
        assert!(
            matches!(small.reason, SkipReason::CircuitOpen { .. }),
            "the BREAKER must win: it is `Timed`, so the caller can pause and retry, \
             where `OverContextWindow` is `Terminal` and kills the run. Registering the \
             window gate ahead of the health gates inverts this: {:?}",
            small.reason
        );
    }

    /// The budget half of the same ordering, pinned because it is a JUDGEMENT rather
    /// than a forced move.
    ///
    /// Both `OverBudget` and `OverContextWindow` are `Terminal`, so unlike the breaker
    /// case neither ordering costs a pause — what it costs is a round trip. Reporting
    /// budget first means an operator whose request is over budget AND over every window
    /// raises the cap, retries, and only then learns the prompt does not fit; reporting
    /// the window first means the mirror image. Money-first is the shipped choice (see
    /// the registration comment), and this test is what makes reversing it a deliberate
    /// edit instead of an accident of vector order.
    #[test]
    fn a_budget_skip_is_reported_ahead_of_the_window() {
        let mut config = two_model_chain_windows(128_000, 8_192);
        // Price both models so the `BudgetGate` has an estimate to judge. The figures
        // only have to exceed the caller's budget; `max_output_tokens` alone (4096 at
        // $1/1k) puts every candidate over a $0.01 cap.
        for m in config.models.values_mut() {
            m.pricing = Some(crate::types::config::ModelPricing {
                input_per_1k: 1.0,
                output_per_1k: 1.0,
                per_request: None,
            });
        }
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&SelectionCriteria {
            budget: Some(0.01),
            // The COST gate reads this one; the window gate reads the other. Both
            // candidates are over both.
            input_tokens: Some(20_000),
            ..criteria_with_pessimistic(Some(200_000))
        });
        assert!(result.all_candidates.is_empty(), "everything is gated");
        for s in &result.skipped {
            assert!(
                matches!(s.reason, SkipReason::OverBudget { .. }),
                "money is reported first, deliberately — see the registration comment \
                 in `ModelSelectionService::new` for the accepted cost: {} reported {:?}",
                s.model,
                s.reason
            );
        }
    }

    /// AC4 — an in-window request selects byte-identically to one carrying no estimate.
    /// The additivity guarantee: registering a sixth gate must not perturb any request
    /// that fits.
    #[test]
    fn an_in_window_request_selects_unchanged() {
        let config = two_model_chain_windows(128_000, 8_192);
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let names = |r: &SelectionResult| -> Vec<String> {
            r.all_candidates.iter().map(|c| c.model.clone()).collect()
        };
        let with = svc.select_all(&criteria_with_pessimistic(Some(1_000)));
        let without = svc.select_all(&criteria_with_pessimistic(None));
        assert_eq!(
            names(&with),
            names(&without),
            "a request that fits every window must select the same candidates, in the \
             same order, as one carrying no estimate at all"
        );
        assert_eq!(
            names(&with),
            vec!["small".to_string(), "big".to_string()],
            "and that order is the chain's own priority order, unchanged"
        );
        assert!(
            with.skipped.is_empty() && without.skipped.is_empty(),
            "an in-window request records no skips at all"
        );
    }

    /// AC5 — a candidate that is BOTH excluded by policy and circuit-open reports
    /// the POLICY, and the selection does not become pausable.
    ///
    /// This is the test the gate's position exists for. Moving `RoutingPolicyGate`
    /// from first to last flips the reported reason to `CircuitOpen`, whose
    /// `gate_status()` is `Timed` — which would make `all_gated_error` return
    /// `AllGated { resume_after: Some(..) }` and park a run waiting on a breaker for
    /// a candidate the caller had already excluded.
    #[test]
    fn a_policy_exclusion_is_reported_ahead_of_an_open_breaker() {
        let config = test_config();
        let cb = test_cb();
        cb.can_execute("ollama:gemma3:27b");
        for _ in 0..5 {
            cb.record_failure("ollama:gemma3:27b");
        }
        assert!(
            !cb.can_execute("ollama:gemma3:27b"),
            "fixture needs it open"
        );

        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: Some(crate::types::request::RoutingPreferences {
                ignore: Some(crate::types::request::CandidateSet {
                    routers: vec!["ollama".to_string()],
                    models: vec![],
                }),
                ..Default::default()
            }),
        });

        let skipped = result
            .skipped
            .iter()
            .find(|s| s.model == "gemma3:27b")
            .expect("the excluded candidate must be recorded");
        assert!(
            matches!(skipped.reason, SkipReason::ExcludedByPolicy),
            "policy must win over the open breaker: {:?}",
            skipped.reason
        );
        assert!(
            matches!(
                skipped.reason.gate_status(),
                crate::skip_reason::GateStatus::Structural
            ),
            "and it must contribute nothing to resume_after"
        );
        assert_eq!(
            result.selected.as_ref().map(|s| s.model.as_str()),
            Some("claude-haiku"),
            "the candidate the filter did NOT name must still be selected"
        );
        assert_eq!(
            result.all_candidates.len(),
            1,
            "exactly one candidate survives the ignore"
        );
    }

    /// AC5, other half — excluding EVERY candidate is terminal, not a pause.
    #[test]
    fn excluding_every_candidate_is_terminal_not_pausable() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: Some(crate::types::request::RoutingPreferences {
                only: Some(crate::types::request::CandidateSet {
                    routers: vec!["nonexistent".to_string()],
                    models: vec![],
                }),
                ..Default::default()
            }),
        });

        assert!(result.all_candidates.is_empty());
        assert_eq!(
            result.skipped.len(),
            2,
            "both chain candidates must be recorded as skipped"
        );
        assert!(
            result
                .skipped
                .iter()
                .all(|s| matches!(s.reason, SkipReason::ExcludedByPolicy))
        );
        assert!(
            crate::engine::exhaustion::all_gated_error(&result.skipped, &[]).is_none(),
            "an all-structural exhaustion must NOT become AllGated — no deadline \
             and no human remedy makes an excluded candidate eligible"
        );
    }

    /// Wiring the ports must not perturb a selection — and this must hold
    /// whether or not they are wired, since production wires them (Task 10) and
    /// unit tests do not.
    ///
    /// Task 7 replaced the registered default with `GroupedWeightedStrategy`,
    /// and this test survived unchanged for a REASON worth stating rather than
    /// leaving as a coincidence: `seam_chain`'s two entries have DISTINCT
    /// priorities, so every group is a singleton and AC1 makes the weighted
    /// strategy indistinguishable from `PriorityStrategy` here. It therefore
    /// says nothing about which of the two is registered — swapping the default
    /// back leaves it green. That claim belongs to
    /// [`the_registered_default_weights_a_tied_group`], which uses a TIED chain,
    /// the only shape that can observe the difference.
    ///
    /// The chain here lists its entries in REVERSE priority order (claude-haiku,
    /// priority 2, first; gemma3:27b, priority 1, second) — deliberately, so this
    /// test cannot pass by accident. `test_config()`'s own `chat_chain` already
    /// lists its entries in priority order, so a `PriorityStrategy` reduced to a
    /// no-op would still satisfy an assertion built on it: the input order and the
    /// sorted order coincide. Reversing them here means only an actual sort
    /// produces `["gemma3:27b", "claude-haiku"]`; a no-op strategy would yield
    /// `["claude-haiku", "gemma3:27b"]` instead.
    ///
    /// This test does NOT prove the ctx's individual ports (`rng`/`perf`/
    /// `min_samples`) actually reach the strategy. `GroupedWeightedStrategy`
    /// does read `ctx.rng` and `ctx.perf` — unlike the `PriorityStrategy` that
    /// was registered when this test was written — but it reads them only inside
    /// a group, and every group here is a singleton, so a ctx built wholly from
    /// wrong defaults still changes nothing. That claim belongs to
    /// [`the_builders_install_the_ports_the_strategy_sees`], which uses a probe
    /// strategy that reads the ctx back out.
    #[test]
    fn widening_the_strategy_seam_leaves_selection_unchanged() {
        let mut config = test_config();
        config.chains.insert(
            "seam_chain".to_string(),
            FallbackChainConfig {
                id: "seam_chain".to_string(),
                capability: Capability::TextChat,
                models: vec![
                    ChainEntry {
                        model: "claude-haiku".to_string(),
                        router: None,
                        api_model_id: None,
                        priority: 2,
                    },
                    ChainEntry {
                        model: "gemma3:27b".to_string(),
                        router: None,
                        api_model_id: None,
                        priority: 1,
                    },
                ],
                fallback_triggers: vec![],
            },
        );
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();

        let criteria = SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("seam_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        };

        let bare = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);
        let got_bare: Vec<String> = bare
            .select_all(&criteria)
            .all_candidates
            .iter()
            .map(|c| c.model.clone())
            .collect();

        let store =
            crate::gates::performance::PerformanceStore::new(8, std::time::Duration::from_secs(60));
        let rng = crate::random::SplitMix64::seeded(999);
        let wired = ModelSelectionService::new(&config, &cb, &cooldown, &lockout)
            .with_performance(&store, 3)
            .with_random(&rng);
        let got_wired: Vec<String> = wired
            .select_all(&criteria)
            .all_candidates
            .iter()
            .map(|c| c.model.clone())
            .collect();

        assert_eq!(
            got_bare,
            vec!["gemma3:27b".to_string(), "claude-haiku".to_string()],
            "priority order (ascending), NOT the chain's declared (reversed) order — \
             proves the strategy actually sorted rather than passing the input through"
        );
        assert_eq!(
            got_bare, got_wired,
            "wiring the ports must not perturb a distinct-priority selection"
        );
    }

    /// The one test that pins WHICH strategy `new()` registers.
    ///
    /// Every other strategy test constructs `GroupedWeightedStrategy` directly
    /// and calls `.order(...)` on it, which proves the strategy works and
    /// nothing at all about whether the service installs it. And the
    /// service-level tests cannot help: AC1 makes the two strategies
    /// indistinguishable on distinct priorities BY DESIGN, and every chain in
    /// this file has distinct priorities. So reverting the single highest-risk
    /// line of the slice — `Box::new(GroupedWeightedStrategy)` back to
    /// `Box::new(PriorityStrategy)` — passed the entire suite.
    ///
    /// A TIED group is the only shape that can observe the swap. This one is
    /// deterministic rather than RNG-dependent: `gemma3:27b` has no pricing, so
    /// its `cost_estimate` is `None` and it is classified free, which leads its
    /// group ahead of any priced candidate on every possible draw. The chain
    /// lists the PRICED model first, so a stable `PriorityStrategy` — equal
    /// keys, input order preserved — would leave `claude-haiku` in front.
    #[test]
    fn the_registered_default_weights_a_tied_group() {
        let mut config = test_config();
        config.chains.insert(
            "tied_chain".to_string(),
            FallbackChainConfig {
                id: "tied_chain".to_string(),
                capability: Capability::TextChat,
                models: vec![
                    // Priced, and listed FIRST: PriorityStrategy would keep it here.
                    ChainEntry {
                        model: "claude-haiku".to_string(),
                        router: None,
                        api_model_id: None,
                        priority: 1,
                    },
                    // `pricing: None` ⇒ free ⇒ leads its tie group, every draw.
                    ChainEntry {
                        model: "gemma3:27b".to_string(),
                        router: None,
                        api_model_id: None,
                        priority: 1,
                    },
                ],
                fallback_triggers: vec![],
            },
        );
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();

        let criteria = SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("tied_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        };

        let rng = crate::random::SplitMix64::seeded(4242);
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout).with_random(&rng);
        let got: Vec<String> = svc
            .select_all(&criteria)
            .all_candidates
            .iter()
            .map(|c| c.model.clone())
            .collect();

        assert_eq!(
            got,
            vec!["gemma3:27b".to_string(), "claude-haiku".to_string()],
            "the free candidate leads its tie group — PriorityStrategy would \
             keep the chain's declared order, so this fails if `new()` registers it"
        );
    }

    /// Records what the `StrategyCtx` it was handed actually carried, so a test
    /// can read the ports back out rather than infer them from `PriorityStrategy`'s
    /// output — which ignores `ctx` entirely and therefore cannot tell "wired
    /// correctly" from "silently discarded".
    #[derive(Default)]
    struct ProbeStrategy {
        draw: std::sync::Mutex<Option<u64>>,
        min_samples: std::sync::Mutex<Option<u32>>,
        perf_samples: std::sync::Mutex<Option<u32>>,
    }
    impl crate::strategy::RoutingStrategy for std::sync::Arc<ProbeStrategy> {
        fn order(
            &self,
            admitted: &mut Vec<SelectedModel>,
            ctx: &crate::strategy::StrategyCtx<'_>,
        ) -> crate::strategy::OrderingReport {
            *self.draw.lock().unwrap() = Some(ctx.rng.next_u64());
            *self.min_samples.lock().unwrap() = Some(ctx.min_samples);
            *self.perf_samples.lock().unwrap() =
                ctx.perf.stats("probe-endpoint").map(|s| s.samples);
            admitted.sort_by_key(|m| m.priority);
            crate::strategy::OrderingReport::default()
        }
        /// Self-announcing, so a trace taken while an override is installed
        /// says so instead of impersonating the strategy it displaced.
        fn name(&self) -> &'static str {
            "probe"
        }
    }

    /// The Critical fix: `widening_the_strategy_seam_leaves_selection_unchanged`
    /// is green in exactly the world where `with_random`/`with_performance`
    /// silently discard their arguments, because it only ever exercises
    /// `PriorityStrategy`, which never reads `ctx`. This test installs a probe
    /// strategy (via the crate-private `strategy` field — `mod tests` is a child
    /// of `selection`) that reads the ctx back out, so a builder that drops its
    /// argument is directly observable.
    ///
    /// `min_samples: 11` deliberately differs from the default (`3`), and
    /// `perf_samples: Some(1)` deliberately differs from the null port's
    /// (`None`) — each assertion must be unreachable from what `new()` already
    /// installs, or a builder that does nothing would still pass it.
    #[test]
    fn the_builders_install_the_ports_the_strategy_sees() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();

        let store =
            crate::gates::performance::PerformanceStore::new(8, std::time::Duration::from_secs(60));
        store.record(
            "probe-endpoint",
            crate::gates::performance::Sample {
                at: std::time::Instant::now(),
                latency_ms: Some(10),
                tokens_per_sec: None,
                success: Some(true),
            },
        );
        let rng = crate::random::SplitMix64::seeded(1234);
        // Computed from a SEPARATE, freshly-seeded instance — not `rng` itself —
        // so this is a real prediction of `rng`'s first draw, not a tautology.
        let expected_first_draw = crate::random::SplitMix64::seeded(1234).next_u64();

        let probe = std::sync::Arc::new(ProbeStrategy::default());
        let mut svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout)
            .with_performance(&store, 11)
            .with_random(&rng);
        svc.strategy_override = Some(Box::new(std::sync::Arc::clone(&probe)));

        let criteria = SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: None,
        };
        let _ = svc.select_all(&criteria);

        assert_eq!(
            *probe.draw.lock().unwrap(),
            Some(expected_first_draw),
            "resolve_chain's ctx must carry the `rng` passed to `with_random`, not \
             `DEFAULT_RNG` or anything else"
        );
        assert_eq!(
            *probe.min_samples.lock().unwrap(),
            Some(11),
            "resolve_chain's ctx must carry the `min_samples` passed to \
             `with_performance`, not the default `3`"
        );
        assert_eq!(
            *probe.perf_samples.lock().unwrap(),
            Some(1),
            "resolve_chain's ctx must carry the `perf` passed to `with_performance`, \
             not the null port (which would report `None`)"
        );

        // The override must beat an EXPLICIT `sort` too, not just an absent one.
        //
        // `strategy_for` consults the override ahead of the `sort` match, and
        // that placement was unpinned. Moving it after the match genuinely does
        // not matter (the match is side-effect-free), but narrowing it — e.g.
        // gating on `sort.is_none()` — compiles, survives every other test, and
        // makes the probe silently stop observing any request carrying a `sort`.
        // Every future test built on this probe would then be quietly blind on
        // exactly the requests SP-ROUTE-1 exists to serve.
        *probe.min_samples.lock().unwrap() = None;
        let _ = svc.select_all(&SelectionCriteria {
            preferences: Some(crate::types::request::RoutingPreferences {
                sort: Some(crate::types::request::SortKey::Price),
                ..Default::default()
            }),
            ..criteria
        });
        assert_eq!(
            *probe.min_samples.lock().unwrap(),
            Some(11),
            "the override must run for a request carrying `sort: price` as well — \
             a probe that only sees unsorted requests observes nothing this slice \
             is about"
        );
    }

    // -----------------------------------------------------------------------------
    // SP-ROUTE-1 Task 10 — per-request strategy resolution (`sort`) and the
    // `order` re-rank, seen through the whole selection service.
    //
    // Every strategy test in `strategy.rs` constructs its strategy directly and
    // calls `.order(...)`, which proves the strategy works and says NOTHING about
    // which one a given `sort` resolves to. These go through `select_all`, the
    // only place resolution is observable — Task 7's Critical (reverting the
    // registered default passed the whole suite, because a test proving two
    // strategies AGREE cannot catch a swap between them) in Task 10's clothing.
    // -----------------------------------------------------------------------------

    /// A three-model `TextChat` chain built so PRIORITY order, PRICE order,
    /// LATENCY order and THROUGHPUT order are four MUTUALLY DISTINCT
    /// permutations — the only shape in which a `sort` that resolves to the
    /// wrong strategy, or silently to the default, is observable at all.
    ///
    /// | model   | router | priority | price | latency | tok/s |
    /// |---------|--------|----------|-------|---------|-------|
    /// | alpha   | north  | 1        | 3.0   | 20 ms   | 10    |
    /// | bravo   | south  | 2        | 1.0   | 30 ms   | 50    |
    /// | charlie | north  | 3        | 2.0   | 10 ms   | 90    |
    ///
    /// ⇒ default `[alpha, bravo, charlie]`, price `[bravo, charlie, alpha]`,
    /// latency `[charlie, alpha, bravo]`, throughput `[charlie, bravo, alpha]`.
    ///
    /// Every priority is DISTINCT, which makes the default
    /// (`GroupedWeightedStrategy`) exactly priority order by AC1 — so the
    /// default's answer here is deterministic and no draw can perturb it.
    ///
    /// `alpha` and `charlie` share the router `north` while `bravo` sits alone on
    /// `south`, deliberately: a router-only `CandidateRef` is then a wildcard
    /// over TWO models that are NOT adjacent in the default order, so the
    /// wildcard's effect cannot be mistaken for the priority sort's.
    ///
    /// Price is not asserted directly but comes out of `estimate_cost`: with
    /// `input_tokens: None` the estimate is `max_output_tokens * output_per_1k /
    /// 1000`, so at 1000 output tokens `output_per_1k` IS the dollar price.
    fn sort_chain() -> GatewayConfig {
        let mut routers = HashMap::new();
        for id in ["north", "south"] {
            routers.insert(
                id.to_string(),
                RouterConfig {
                    url: "http://localhost".to_string(),
                    api_key_env: None,
                    api_key: None,
                    enabled: true,
                    timeout_ms: None,
                    headers: HashMap::new(),
                },
            );
        }

        let mut models = HashMap::new();
        let mut entries = Vec::new();
        for (id, router, priority, price) in [
            ("alpha", "north", 1u8, 3.0),
            ("bravo", "south", 2, 1.0),
            ("charlie", "north", 3, 2.0),
        ] {
            models.insert(
                id.to_string(),
                ModelConfig {
                    id: id.to_string(),
                    api_model_id: None,
                    provider: router.to_string(),
                    family: None,
                    capabilities: vec![Capability::TextChat],
                    context_window: 128_000,
                    max_output_tokens: 1_000,
                    pricing: Some(ModelPricing {
                        input_per_1k: 0.0,
                        output_per_1k: price,
                        per_request: None,
                    }),
                    catalog: None,
                },
            );
            entries.push(ChainEntry {
                model: id.to_string(),
                router: Some(router.to_string()),
                api_model_id: None,
                priority,
            });
        }

        let mut chains = HashMap::new();
        chains.insert(
            "sort_chain".to_string(),
            FallbackChainConfig {
                id: "sort_chain".to_string(),
                capability: Capability::TextChat,
                models: entries,
                fallback_triggers: vec![],
            },
        );

        GatewayConfig {
            routers,
            models,
            chains,
            constraints: Default::default(),
            panels: Default::default(),
            consensus: Default::default(),
        }
    }

    /// The metrics [`sort_chain`] is designed around, keyed off the model suffix
    /// of the `"{router}:{model}"` endpoint key so it survives the two-router
    /// layout.
    ///
    /// All three counters sit ABOVE the service's `min_samples`, and each mean is
    /// set independently of the others — a fixture that tied `throughput_samples`
    /// to `samples` could not tell a throughput sort reading the wrong counter
    /// from one reading the right one.
    struct SortChainStats;
    impl crate::gates::performance::EndpointPerformanceRead for SortChainStats {
        fn stats(&self, endpoint: &str) -> Option<crate::gates::performance::EndpointStats> {
            let (mean_latency_ms, mean_tokens_per_sec) = match endpoint {
                "north:alpha" => (20.0, 10.0),
                "south:bravo" => (30.0, 50.0),
                "north:charlie" => (10.0, 90.0),
                _ => return None,
            };
            Some(crate::gates::performance::EndpointStats {
                samples: 9,
                throughput_samples: 9,
                verdict_samples: 9,
                mean_latency_ms,
                mean_tokens_per_sec,
                success_rate: 1.0,
            })
        }
    }

    fn sort_criteria(prefs: Option<RoutingPreferences>) -> SelectionCriteria {
        SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("sort_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: prefs,
        }
    }

    /// Resolve [`sort_chain`] under `prefs` and return the admitted models in
    /// the order selection would actually try them.
    ///
    /// Its own seeded RNG, never `DEFAULT_RNG`: that static is process-wide and
    /// cargo runs tests in parallel, so which draw a test receives would
    /// otherwise depend on who else ran. (Every group here is a singleton, so no
    /// draw can move anything — but the rule holds regardless of whether this
    /// particular fixture is sensitive to it.)
    fn sort_chain_order(prefs: Option<RoutingPreferences>) -> Vec<String> {
        let config = sort_chain();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let perf = SortChainStats;
        let rng = crate::random::SplitMix64::seeded(1010);
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout)
            .with_performance(&perf, 3)
            .with_random(&rng);
        svc.select_all(&sort_criteria(prefs))
            .all_candidates
            .iter()
            .map(|c| c.model.clone())
            .collect()
    }

    fn model_ref(model: &str) -> crate::types::request::CandidateRef {
        crate::types::request::CandidateRef {
            router: None,
            model: Some(model.to_string()),
        }
    }

    fn router_ref(router: &str) -> crate::types::request::CandidateRef {
        crate::types::request::CandidateRef {
            router: Some(router.to_string()),
            model: None,
        }
    }

    /// Each `sort` value must resolve to a DIFFERENT strategy — and the test must
    /// be able to tell them apart.
    ///
    /// This is Task 7's Critical in Task 10's clothing. There, a test proving
    /// `GroupedWeightedStrategy` and `PriorityStrategy` AGREE on a
    /// distinct-priority chain stayed green when the registered default was
    /// reverted. Here the equivalent risk is a `sort` arm returning the wrong
    /// strategy — or falling through to the default — so the fixture is built so
    /// all four answers differ, and the FULL sequence is asserted for each.
    ///
    /// The pairwise `assert_ne!` sweep at the end pins the fixture's own premise
    /// rather than trusting it: if a future edit made two of these orders
    /// coincide, the four `assert_eq!`s above would still pass while silently
    /// losing the power to catch a swap between those two arms.
    #[test]
    fn each_sort_value_resolves_to_its_own_strategy() {
        use crate::types::request::SortKey;
        let sorted = |sort: Option<SortKey>| {
            sort_chain_order(Some(RoutingPreferences {
                sort,
                ..Default::default()
            }))
        };

        let default = sorted(None);
        let price = sorted(Some(SortKey::Price));
        let latency = sorted(Some(SortKey::Latency));
        let throughput = sorted(Some(SortKey::Throughput));

        assert_eq!(
            default,
            vec!["alpha", "bravo", "charlie"],
            "no `sort` ⇒ the registered default (`GroupedWeightedStrategy`), which \
             on a distinct-priority chain is exactly priority order"
        );
        assert_eq!(
            price,
            vec!["bravo", "charlie", "alpha"],
            "`sort: price` ⇒ ascending estimated cost (1.0, 2.0, 3.0), overriding \
             priority entirely"
        );
        assert_eq!(
            latency,
            vec!["charlie", "alpha", "bravo"],
            "`sort: latency` ⇒ ascending mean latency (10ms, 20ms, 30ms)"
        );
        assert_eq!(
            throughput,
            vec!["charlie", "bravo", "alpha"],
            "`sort: throughput` ⇒ DESCENDING tok/s (90, 50, 10) — note this is \
             not the latency order, so swapping the two arms is caught here"
        );

        let all = [
            ("default", &default),
            ("price", &price),
            ("latency", &latency),
            ("throughput", &throughput),
        ];
        for (i, (name_a, a)) in all.iter().enumerate() {
            for (name_b, b) in all.iter().skip(i + 1) {
                assert_ne!(
                    a, b,
                    "the fixture must keep all four answers distinct or this test \
                     loses the power to tell `{name_a}` from `{name_b}`"
                );
            }
        }
    }

    /// The resolved strategy must NAME itself.
    ///
    /// Task 11 needs the applied strategy's name for the trace. The obvious way
    /// to get it — a second `match` on `SortKey` at the trace site — is a drift
    /// hazard precisely because it compiles and agrees on the day it is written;
    /// reading the name off the strategy that actually ran cannot drift, because
    /// there is only one `match`.
    ///
    /// Behaviour is pinned by `each_sort_value_resolves_to_its_own_strategy`;
    /// this pins the LABEL, which a behaviour test cannot see. Both are needed —
    /// a strategy can be correctly named and wrongly ordered, or the reverse.
    #[test]
    fn the_resolved_strategy_names_itself() {
        use crate::types::request::SortKey;
        let config = sort_chain();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let name_for = |sort: Option<SortKey>| {
            svc.strategy_for(&sort_criteria(Some(RoutingPreferences {
                sort,
                ..Default::default()
            })))
            .name()
        };

        assert_eq!(name_for(None), "grouped_weighted");
        assert_eq!(name_for(Some(SortKey::Price)), "price");
        assert_eq!(name_for(Some(SortKey::Latency)), "latency");
        assert_eq!(
            name_for(Some(SortKey::Throughput)),
            "throughput",
            "the two metric sorts share a type, so each must report its OWN \
             metric rather than the type's name"
        );
        assert_eq!(
            svc.strategy_for(&sort_criteria(None)).name(),
            "grouped_weighted",
            "absent PREFERENCES resolve the same as an absent `sort` — the two \
             are different `None`s and both must reach the default"
        );
    }

    /// AC6 — `order` sequences the candidates it names; unmatched candidates
    /// follow as FALLBACKS rather than being dropped. `only` is the knob that
    /// restricts.
    #[test]
    fn order_sequences_named_candidates_and_keeps_the_rest_as_fallbacks() {
        // ONE named candidate leads, and the two it did NOT name follow in the
        // strategy's order — all three still present.
        assert_eq!(
            sort_chain_order(Some(RoutingPreferences {
                order: Some(vec![model_ref("charlie")]),
                ..Default::default()
            })),
            vec!["charlie", "alpha", "bravo"],
            "`charlie` is named and leads from LAST place; `alpha` and `bravo` are \
             unnamed and follow as fallbacks in the strategy's order — naming one \
             candidate must not drop the others"
        );

        // TWO named candidates lead in REF order, which here is the REVERSE of
        // the order the strategy put them in — so this cannot pass against an
        // implementation that merely kept the strategy's sequence for them.
        assert_eq!(
            sort_chain_order(Some(RoutingPreferences {
                order: Some(vec![model_ref("charlie"), model_ref("bravo")]),
                ..Default::default()
            })),
            vec!["charlie", "bravo", "alpha"],
            "named candidates lead in the order the REFS list them (charlie then \
             bravo), not the order the strategy produced (bravo then charlie)"
        );

        // A ref matching nothing is inert: every candidate ranks `usize::MAX`,
        // the sort is stable, so the strategy's order survives untouched and
        // nothing is filtered out. `order` never restricts.
        assert_eq!(
            sort_chain_order(Some(RoutingPreferences {
                order: Some(vec![model_ref("ghost")]),
                ..Default::default()
            })),
            vec!["alpha", "bravo", "charlie"],
            "an `order` naming a candidate that is not in the chain must leave \
             the selection exactly as the strategy left it — and must not drop \
             the candidates it failed to name"
        );
    }

    /// A candidate matching MORE THAN ONE `order` ref takes its rank from the
    /// FIRST match.
    ///
    /// Nothing pinned this: every other `order` test uses refs that partition
    /// the chain, so only "matched" and "not matched" were ever exercised and
    /// `.position(…)` → `.rposition(…)` survived the whole suite. The overlap is
    /// not exotic — it is the most natural way to combine the two ref forms:
    /// `[{model: charlie}, {router: north}]` means "charlie first, then the rest
    /// of north", and charlie is on north, so it matches both.
    ///
    /// Under `rposition` charlie takes its LAST match (rank 1), ties with alpha,
    /// and the sort leaves alpha in front — silently inverting the one
    /// instruction the caller was most explicit about.
    #[test]
    fn the_first_matching_order_ref_sets_a_candidates_rank() {
        assert_eq!(
            sort_chain_order(Some(RoutingPreferences {
                order: Some(vec![model_ref("charlie"), router_ref("north")]),
                ..Default::default()
            })),
            vec!["charlie", "alpha", "bravo"],
            "charlie matches ref 0 AND ref 1; the FIRST match wins, so it leads \
             alpha — which matches only ref 1. Under `rposition` both rank 1 and \
             charlie loses the lead the caller named it for"
        );
    }

    /// A ref naming BOTH axes is an AND, so it matches a specific endpoint and
    /// not merely either half of it.
    ///
    /// No other test sets both fields, so a ref that ignored one axis — or
    /// treated the pair as an OR — would be invisible. `bravo` lives on `south`,
    /// so `{router: north, model: bravo}` names an endpoint that does not exist
    /// and must match NOTHING, leaving the strategy's order untouched.
    #[test]
    fn an_order_ref_naming_both_axes_matches_only_that_endpoint() {
        let both = |router: &str, model: &str| crate::types::request::CandidateRef {
            router: Some(router.to_string()),
            model: Some(model.to_string()),
        };

        assert_eq!(
            sort_chain_order(Some(RoutingPreferences {
                order: Some(vec![both("north", "bravo")]),
                ..Default::default()
            })),
            vec!["alpha", "bravo", "charlie"],
            "bravo is on `south`, so this pair names no real endpoint and must \
             match nothing — an OR, or a ref that ignored the router axis, would \
             promote bravo here"
        );
        assert_eq!(
            sort_chain_order(Some(RoutingPreferences {
                order: Some(vec![both("north", "charlie")]),
                ..Default::default()
            })),
            vec!["charlie", "alpha", "bravo"],
            "and the pair that DOES name a real endpoint matches it — without \
             this half the assertion above would also pass against a ref that \
             matched nothing at all"
        );
    }

    /// A ref naming only a router is a wildcard over its models.
    #[test]
    fn an_order_ref_with_only_a_router_is_a_wildcard_over_its_models() {
        // `north` hosts alpha AND charlie. Both lead, keeping the strategy's
        // relative order between them, and `charlie` jumps `bravo` — the half a
        // single-model router could not show.
        assert_eq!(
            sort_chain_order(Some(RoutingPreferences {
                order: Some(vec![router_ref("north")]),
                ..Default::default()
            })),
            vec!["alpha", "charlie", "bravo"],
            "a router-only ref matches EVERY model on that router: both of \
             north's lead, so charlie climbs past south's bravo, and the two \
             sharing the ref keep the strategy's order between them"
        );

        // The other router, so the assertion above cannot be satisfied by the
        // default order with extra steps: `south` hosts only `bravo`, which must
        // climb from the middle to the front.
        assert_eq!(
            sort_chain_order(Some(RoutingPreferences {
                order: Some(vec![router_ref("south")]),
                ..Default::default()
            })),
            vec!["bravo", "alpha", "charlie"],
            "south hosts only bravo, which leads; north's two follow as \
             fallbacks in the strategy's order"
        );
    }

    /// Candidates SHARING one `order` ref keep the strategy's order between
    /// them — the property that makes the re-rank a STABLE sort rather than
    /// merely a sort.
    ///
    /// **Both the WIDTH and the INTERLEAVING of this fixture are load-bearing,
    /// and each was chosen by measurement rather than taste.** The mutation this
    /// exists to catch is `sort_by_key` → `sort_unstable_by_key`, and the
    /// shapes that CANNOT see it are worth recording so nobody "simplifies" the
    /// fixture back into one of them:
    ///
    /// - **Too narrow.** `sort_unstable_by_key` delegates to insertion sort on
    ///   small inputs, which is stable in practice. Measured: an alternating
    ///   fixture is preserved at n = 4…32 and only diverges from n = 40. The
    ///   three-candidate `order` tests above are therefore all blind to it — the
    ///   entire 415-test gateway suite passed with the unstable variant in place.
    /// - **All ranks equal.** A ref matching EVERY candidate looks like the
    ///   sharpest form of the claim and is in fact the weakest: pdqsort's
    ///   equal-partition path is itself order-preserving, so an all-rank-0
    ///   fixture is preserved at every width measured (n = 3…1000). Verified
    ///   directly — this test was first written that way at n = 40 and passed
    ///   under the mutation.
    ///
    /// What does see it is MIXED ranks interleaved through the strategy's
    /// output: half the chain matches the ref (rank 0) and half does not
    /// (`usize::MAX`), alternating. n = 64 sits clear of the measured n = 40
    /// boundary rather than on it, since that threshold is a stdlib
    /// implementation detail and may move.
    #[test]
    fn candidates_sharing_an_order_ref_keep_the_strategys_order() {
        const N: u8 = 64;

        let mut routers = HashMap::new();
        for id in ["named", "other"] {
            routers.insert(
                id.to_string(),
                RouterConfig {
                    url: "http://localhost".to_string(),
                    api_key_env: None,
                    api_key: None,
                    enabled: true,
                    timeout_ms: None,
                    headers: HashMap::new(),
                },
            );
        }

        // Alternating routers, so the two rank classes interleave rather than
        // arriving already grouped — a pre-grouped input is what the stable and
        // unstable sorts agree on.
        let router_of = |i: u8| if i % 2 == 1 { "named" } else { "other" };

        let mut models = HashMap::new();
        let mut entries = Vec::new();
        for i in 1..=N {
            let id = format!("m{i:02}");
            models.insert(
                id.clone(),
                ModelConfig {
                    id: id.clone(),
                    api_model_id: None,
                    provider: router_of(i).to_string(),
                    family: None,
                    capabilities: vec![Capability::TextChat],
                    context_window: 128_000,
                    max_output_tokens: 1_000,
                    pricing: None,
                    catalog: None,
                },
            );
            entries.push(ChainEntry {
                model: id,
                router: Some(router_of(i).to_string()),
                api_model_id: None,
                // DISTINCT, so the strategy's answer is deterministic priority
                // order and the only thing that can scramble it is the re-rank.
                priority: i,
            });
        }
        let mut chains = HashMap::new();
        chains.insert(
            "wide_chain".to_string(),
            FallbackChainConfig {
                id: "wide_chain".to_string(),
                capability: Capability::TextChat,
                models: entries,
                fallback_triggers: vec![],
            },
        );
        let config = GatewayConfig {
            routers,
            models,
            chains,
            constraints: Default::default(),
            panels: Default::default(),
            consensus: Default::default(),
        };

        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let rng = crate::random::SplitMix64::seeded(4040);
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout).with_random(&rng);

        let got: Vec<String> = svc
            .select_all(&SelectionCriteria {
                capability: Capability::TextChat,
                model: None,
                router: None,
                chain: Some("wide_chain".to_string()),
                budget: None,
                input_tokens: None,
                input_tokens_pessimistic: None,
                preferences: Some(RoutingPreferences {
                    order: Some(vec![router_ref("named")]),
                    ..Default::default()
                }),
            })
            .all_candidates
            .iter()
            .map(|c| c.model.clone())
            .collect();

        // Every `named` candidate leads, then every `other` — each class in the
        // strategy's (priority) order, which for this chain is model-id order.
        let expected: Vec<String> = (1..=N)
            .filter(|i| router_of(*i) == "named")
            .chain((1..=N).filter(|i| router_of(*i) == "other"))
            .map(|i| format!("m{i:02}"))
            .collect();
        assert_eq!(
            got, expected,
            "the {N} candidates fall into two rank classes, and WITHIN each class \
             the strategy's order must survive the re-rank intact"
        );
    }

    /// `order` wins for the candidates it names; `sort` orders the unnamed tail.
    /// Combining them is legal and composable.
    #[test]
    fn order_and_sort_compose_with_order_winning_for_named_candidates() {
        use crate::types::request::SortKey;
        let composed = sort_chain_order(Some(RoutingPreferences {
            sort: Some(SortKey::Throughput),
            order: Some(vec![model_ref("alpha")]),
            ..Default::default()
        }));
        let sort_only = sort_chain_order(Some(RoutingPreferences {
            sort: Some(SortKey::Throughput),
            ..Default::default()
        }));
        let order_only = sort_chain_order(Some(RoutingPreferences {
            order: Some(vec![model_ref("alpha")]),
            ..Default::default()
        }));

        assert_eq!(
            composed,
            vec!["alpha", "charlie", "bravo"],
            "`order` wins for `alpha` — the WORST candidate by throughput (10 \
             tok/s), so it can only be leading because it was named — while \
             `sort` still orders the unnamed tail: charlie (90) ahead of bravo \
             (50), where priority order would have put bravo first"
        );
        assert_ne!(
            composed, sort_only,
            "if `order` were ignored the answer would collapse to the throughput \
             sort, so these two must differ for the assertion above to mean \
             anything"
        );
        assert_ne!(
            composed, order_only,
            "and if `sort` were ignored it would collapse to the re-ranked \
             DEFAULT order — the tail is what tells them apart"
        );
    }

    // -----------------------------------------------------------------------------
    // Task 11 — AC10: the routing DECISION.
    //
    // The default strategy is a weighted draw. Without the strategy that ran and
    // the weights behind it, "why did it pick the expensive one" has no answer in
    // a bug report at all, and a weighted router is unfalsifiable in production.
    // -----------------------------------------------------------------------------

    /// A per-endpoint reading over [`sort_chain`] where an endpoint ABSENT from
    /// the slice is genuinely unmeasured (`stats` → `None`).
    ///
    /// That absence is the point, and it is what [`SortChainStats`] cannot
    /// express — it answers for all three. Every counter is set to the same `n`
    /// here because these tests vary MEASURED-vs-NOT, not which counter a sort
    /// reads (`a_throughput_sort_ignores_an_endpoint_with_no_token_counts` in
    /// `strategy.rs` owns that claim).
    ///
    /// `(endpoint, samples, mean_latency_ms, success_rate)`.
    struct ChainReadings(&'static [(&'static str, u32, f64, f64)]);
    impl crate::gates::performance::EndpointPerformanceRead for ChainReadings {
        fn stats(&self, endpoint: &str) -> Option<crate::gates::performance::EndpointStats> {
            self.0.iter().find(|(e, ..)| *e == endpoint).map(
                |(_, samples, mean_latency_ms, success_rate)| {
                    crate::gates::performance::EndpointStats {
                        samples: *samples,
                        throughput_samples: *samples,
                        verdict_samples: *samples,
                        mean_latency_ms: *mean_latency_ms,
                        mean_tokens_per_sec: 0.0,
                        success_rate: *success_rate,
                    }
                },
            )
        }
    }

    /// Resolve [`sort_chain`] under `perf` + `prefs` and hand back the WHOLE
    /// result, so a test can compare the recorded decision against the very
    /// candidate list it claims to describe. [`sort_chain_order`] throws that
    /// away.
    ///
    /// Its own seeded source, never the process-wide `DEFAULT_RNG` — see
    /// [`sort_chain_order`] for why.
    fn sort_chain_select(
        perf: &dyn crate::gates::performance::EndpointPerformanceRead,
        prefs: Option<RoutingPreferences>,
    ) -> SelectionResult {
        chain_select(&sort_chain(), perf, prefs)
    }

    /// As [`sort_chain_select`], but over a caller-supplied config — so a test
    /// can vary the number of ADMITTED candidates, which `sort_chain` fixes at
    /// three and which `degraded` turns out to depend on.
    fn chain_select(
        config: &GatewayConfig,
        perf: &dyn crate::gates::performance::EndpointPerformanceRead,
        prefs: Option<RoutingPreferences>,
    ) -> SelectionResult {
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let rng = crate::random::SplitMix64::seeded(1010);
        ModelSelectionService::new(config, &cb, &cooldown, &lockout)
            .with_performance(perf, 3)
            .with_random(&rng)
            .select_all(&sort_criteria(prefs))
    }

    /// [`sort_chain`] cut down to its single `alpha` entry.
    ///
    /// One admitted candidate has exactly one ordering, so no metric sort can
    /// reorder it and no quantity of samples would change that. That makes it
    /// the one shape in which "degraded FOR WANT OF SAMPLES" can be a false
    /// alarm rather than a report.
    fn single_model_chain() -> GatewayConfig {
        let mut config = sort_chain();
        config
            .chains
            .get_mut("sort_chain")
            .expect("fixture chain")
            .models
            .retain(|e| e.model == "alpha");
        config
    }

    /// The weights follow their ENDPOINT through the `order` re-rank — the one
    /// claim that justifies keying the report by endpoint rather than by
    /// position.
    ///
    /// `OrderingReport::weights` comes out of the strategy in the strategy's
    /// order; `all_candidates` is then re-ranked by the `order` preference.
    /// Every other weight assertion in this file uses a fixture whose three
    /// candidates sit in three DISTINCT priority groups, so push order and
    /// output order coincide and the join key is never exercised — a positional
    /// join passes all of them.
    ///
    /// Here the two orders deliberately disagree. `charlie` moves from last to
    /// first, so a positional join hands it `alpha`'s record: a never-measured
    /// endpoint would be reported `reliability: Some(0.0)` — the
    /// measured-and-dead versus never-measured conflation `RoutedCandidate`
    /// calls load-bearing — and the weight shown for the winning candidate
    /// would belong to a different provider entirely.
    #[test]
    fn the_recorded_weight_follows_its_endpoint_through_the_order_re_rank() {
        // Only `alpha` is measured, and it failed every verdict.
        let perf = ChainReadings(&[("north:alpha", 9, 0.0, 0.0)]);
        let result = sort_chain_select(
            &perf,
            Some(RoutingPreferences {
                order: Some(vec![model_ref("charlie")]),
                ..Default::default()
            }),
        );
        let decision = result
            .decision
            .expect("a chain resolution always records a decision");
        assert_eq!(
            decision
                .order
                .iter()
                .map(|c| c.endpoint.clone())
                .collect::<Vec<_>>(),
            vec!["north:charlie", "north:alpha", "south:bravo"],
            "fixture premise: the re-rank moves `charlie` from last to first, so \
             a POSITION is not a stable join key for the weights"
        );
        let of = |endpoint: &str| {
            decision
                .order
                .iter()
                .find(|c| c.endpoint == endpoint)
                .unwrap_or_else(|| panic!("{endpoint} must be admitted by this fixture"))
        };

        assert_eq!(
            of("north:charlie").reliability,
            None,
            "`charlie` was never measured. A positional join would hand it \
             `alpha`'s reading and report `Some(0.0)` — a healthy endpoint \
             described as one that has failed everything"
        );
        assert_eq!(
            of("north:charlie").weight,
            Some(0.25),
            "`1/2² × 1.0` — charlie's OWN price, not the leading slot's"
        );
        assert_eq!(of("north:alpha").reliability, Some(0.0));
        assert_eq!(
            of("north:alpha").weight,
            Some(0.0),
            "and `alpha`'s own record travels with `alpha` to its new slot"
        );
        assert_eq!(of("south:bravo").reliability, None);
        assert_eq!(of("south:bravo").weight, Some(1.0));
    }

    /// Every path that does NOT run a strategy records `None`, and that is a
    /// claim worth pinning rather than an absence.
    ///
    /// The tempting later edit is to fill these in "for uniformity" with an
    /// empty `RoutingDecision`. That would be strictly worse than the absence
    /// it replaced: `{"strategy": "", "degraded": false, "order": []}` asserts
    /// that a strategy ran and ordered nothing, on the four paths where nothing
    /// ran at all. A reader cannot tell it from a chain whose every candidate
    /// was gated out.
    #[test]
    fn the_paths_that_order_nothing_record_no_decision() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);
        let select = |model: Option<&str>, router: Option<&str>, chain: Option<&str>, cap| {
            svc.select(&SelectionCriteria {
                capability: cap,
                model: model.map(str::to_string),
                router: router.map(str::to_string),
                chain: chain.map(str::to_string),
                budget: None,
                input_tokens: None,
                input_tokens_pessimistic: None,
                preferences: None,
            })
        };

        // Tier 1, admitted. The candidate is named outright, so nothing chose
        // between alternatives.
        let direct = select(
            Some("gemma3:27b"),
            Some("ollama"),
            None,
            Capability::TextChat,
        );
        assert!(
            direct.selected.is_some(),
            "fixture premise: the direct path resolved a candidate, so this is \
             not `None` merely because the request failed"
        );
        assert_eq!(
            direct.decision, None,
            "no strategy ordered anything — an empty decision would claim one ran"
        );

        // Tier 1, rejected.
        let direct_missing = select(Some("ghost"), Some("ollama"), None, Capability::TextChat);
        assert!(direct_missing.selected.is_none());
        assert_eq!(direct_missing.decision, None);

        // Tier 2, chain not found.
        let no_chain = select(None, None, Some("ghost_chain"), Capability::TextChat);
        assert_eq!(no_chain.decision, None);

        // Tier 3, no chain serves the capability.
        let no_capability = select(None, None, None, Capability::AudioTranscribe);
        assert!(no_capability.all_candidates.is_empty());
        assert_eq!(no_capability.decision, None);
    }

    /// A one-candidate chain is never `degraded`, and that is not a corner
    /// case — it is the shape where the flag is a FALSE ALARM.
    ///
    /// `degraded` claims a metric sort found too few samples to reorder
    /// anything. With one admitted candidate there is exactly one ordering: no
    /// quantity of samples would have produced a different answer, so nothing
    /// was lost for want of them. A `subset.len() < 2` test alone is a
    /// tautology here — it fires even for a candidate measured far past
    /// `min_samples` — and it would send an operator hunting for missing
    /// observations that were never the cause.
    ///
    /// Both halves asserted, because the flag must be false for a reason that
    /// is about the CHAIN's size rather than about the reading: `alpha` is
    /// measured at 9 samples in the first case and entirely unmeasured in the
    /// second, and neither is degraded.
    #[test]
    fn a_single_candidate_chain_is_never_degraded_for_want_of_samples() {
        use crate::types::request::SortKey;
        let config = single_model_chain();
        let degraded = |perf: &dyn crate::gates::performance::EndpointPerformanceRead| {
            let result = chain_select(
                &config,
                perf,
                Some(RoutingPreferences {
                    sort: Some(SortKey::Latency),
                    ..Default::default()
                }),
            );
            assert_eq!(
                result.all_candidates.len(),
                1,
                "fixture premise: exactly one candidate is admitted"
            );
            result
                .decision
                .expect("a chain resolution always records a decision")
                .degraded
        };

        assert!(
            !degraded(&SortChainStats),
            "`alpha` is measured at 9 samples — three times `min_samples` — so \
             reporting a shortage of samples is simply false"
        );
        assert!(
            !degraded(&crate::gates::performance::NoPerformance),
            "and unmeasured is no different: a second observation could not have \
             reordered a list of one, so nothing degraded for want of it"
        );
    }

    /// AC10 — the recorded strategy is the one that actually RAN.
    ///
    /// Read off `RoutingStrategy::name()` on the object `strategy_for` returned
    /// and `order` was then called on, NOT re-derived from `criteria.sort` at
    /// the trace site. That difference is not stylistic: a second `match` on
    /// `SortKey` agrees on the day it is written and is free to drift
    /// afterwards, and a trace naming the wrong strategy is worse than no trace
    /// — it sends the reader of a bug report to the wrong code.
    ///
    /// All four arms, because an arm that silently returns the DEFAULT still
    /// produces a perfectly valid-looking name.
    #[test]
    fn each_sort_value_records_the_name_of_the_strategy_that_ran() {
        use crate::types::request::SortKey;
        let recorded = |sort: Option<SortKey>| {
            sort_chain_select(
                &SortChainStats,
                Some(RoutingPreferences {
                    sort,
                    ..Default::default()
                }),
            )
            .decision
            .expect("a chain resolution always runs a strategy, so it always records one")
            .strategy
        };

        assert_eq!(recorded(None), "grouped_weighted");
        assert_eq!(recorded(Some(SortKey::Price)), "price");
        assert_eq!(recorded(Some(SortKey::Latency)), "latency");
        assert_eq!(
            recorded(Some(SortKey::Throughput)),
            "throughput",
            "the two metric sorts share one type, so each must report its OWN \
             metric — a type-wide name would make `sort: latency` and \
             `sort: throughput` indistinguishable in the trace"
        );
    }

    /// AC10 — the recorded order IS the candidate order, element for element,
    /// and it is recorded AFTER the `order` re-rank rather than before it.
    ///
    /// TWO independent anchors per case, deliberately. Element-for-element
    /// equality on its own is the classic assertion that stays green when
    /// neither side works: a decision built by mapping over `all_candidates` is
    /// trivially equal to it however wrong both are. So each case also pins the
    /// ABSOLUTE `(endpoint, priority)` sequence the strategy is specified to
    /// produce.
    ///
    /// The `order` case is the structural one. `order` re-ranks AFTER the
    /// strategy runs, so a decision built at the obvious place — right beside
    /// the `strategy.order(...)` call — records `[alpha, bravo, charlie]` while
    /// the engine goes on to try `[charlie, alpha, bravo]`. That trace would be
    /// a lie about the one thing it exists to explain.
    #[test]
    fn the_recorded_order_is_the_candidate_order_element_for_element() {
        use crate::types::request::SortKey;
        let case = |label: &str, prefs: RoutingPreferences, expected: Vec<(&str, u8)>| {
            let result = sort_chain_select(&SortChainStats, Some(prefs));
            let actual: Vec<(String, u8)> = result
                .all_candidates
                .iter()
                .map(|m| (m.endpoint_key(), m.priority))
                .collect();
            let expected: Vec<(String, u8)> = expected
                .into_iter()
                .map(|(e, p)| (e.to_string(), p))
                .collect();
            assert_eq!(
                actual, expected,
                "{label}: fixture premise — this is the order the engine will \
                 actually try, and the decision has to match THIS"
            );

            let recorded: Vec<(String, u8)> = result
                .decision
                .as_ref()
                .expect("a chain resolution always records a decision")
                .order
                .iter()
                .map(|c| (c.endpoint.clone(), c.priority))
                .collect();
            assert_eq!(
                recorded, expected,
                "{label}: the recorded order must be the candidate order, \
                 element for element"
            );
        };

        case(
            "default",
            RoutingPreferences::default(),
            vec![("north:alpha", 1), ("south:bravo", 2), ("north:charlie", 3)],
        );
        case(
            "price",
            RoutingPreferences {
                sort: Some(SortKey::Price),
                ..Default::default()
            },
            vec![("south:bravo", 2), ("north:charlie", 3), ("north:alpha", 1)],
        );
        case(
            "latency",
            RoutingPreferences {
                sort: Some(SortKey::Latency),
                ..Default::default()
            },
            vec![("north:charlie", 3), ("north:alpha", 1), ("south:bravo", 2)],
        );
        case(
            "order re-rank",
            RoutingPreferences {
                order: Some(vec![model_ref("charlie")]),
                ..Default::default()
            },
            vec![("north:charlie", 3), ("north:alpha", 1), ("south:bravo", 2)],
        );
    }

    /// `reliability: None` (never measured) and `Some(0.0)` (measured, and
    /// every verdict failed) are DIFFERENT facts and the trace must not flatten
    /// them.
    ///
    /// Flattening is not a cosmetic loss. A reader of a trace where every
    /// candidate reports `0.0` cannot tell a cold process from a dead fleet,
    /// which is precisely the confusion `verdict_samples` was added to
    /// `EndpointStats` to end — and it would be re-introduced one layer up, in
    /// the artefact the operator actually reads.
    ///
    /// The `assert_ne!` is the load-bearing one: two `assert_eq!`s could both be
    /// satisfied by a constant if the fixture were weaker, and this states the
    /// claim (they DIFFER) directly.
    #[test]
    fn an_unmeasured_candidates_reliability_is_none_not_a_measured_zero() {
        // `north:alpha` has 9 verdicts and failed every one of them; the other
        // two endpoints have no reading at all.
        let perf = ChainReadings(&[("north:alpha", 9, 0.0, 0.0)]);
        let result = sort_chain_select(&perf, None);
        let decision = result
            .decision
            .expect("a chain resolution always records a decision");
        let of = |endpoint: &str| {
            decision
                .order
                .iter()
                .find(|c| c.endpoint == endpoint)
                .unwrap_or_else(|| panic!("{endpoint} must be admitted by this fixture"))
        };

        assert_eq!(
            of("north:alpha").reliability,
            Some(0.0),
            "9 verdicts is past `min_samples` 3 and every one failed, so this is \
             MEASURED at zero — a fact about the endpoint"
        );
        assert_eq!(
            of("south:bravo").reliability,
            None,
            "never observed, so there is no success rate to report — `None`, \
             which is a fact about the OBSERVATION, not about the endpoint"
        );
        assert_ne!(
            of("north:alpha").reliability,
            of("south:bravo").reliability,
            "measured-and-dead must not read identically to never-measured"
        );

        // And the weights those two facts produced, which is how the difference
        // reaches the routing itself rather than only the trace.
        assert_eq!(
            of("north:alpha").weight,
            Some(0.0),
            "reliability 0.0 multiplies the price weight to zero: drawable never, \
             so last in its group"
        );
        assert_eq!(
            of("south:bravo").weight,
            Some(1.0),
            "unmeasured weighs 1.0, so at a price of 1.0 the draw weight is \
             `1/1² × 1.0`. A `None`→`0.0` flattening here would make this 0.0 \
             and strand a healthy endpoint"
        );
    }

    /// `degraded` is true exactly when a metric sort could not express an
    /// ordering for want of samples, so the result is priority order.
    ///
    /// Both directions, and the `< 2` boundary in both — a ONE-measured sort
    /// writes that candidate back into its own slot and moves nothing, so it is
    /// degraded just as surely as a zero-measured one, while a TWO-measured
    /// sort is not degraded even if the pair happened to already agree.
    /// The claim is about the INFORMATION the sort had.
    #[test]
    fn a_metric_sort_records_whether_it_degraded_for_want_of_samples() {
        use crate::types::request::SortKey;
        let degraded = |perf: &dyn crate::gates::performance::EndpointPerformanceRead,
                        sort: Option<SortKey>| {
            sort_chain_select(
                perf,
                Some(RoutingPreferences {
                    sort,
                    ..Default::default()
                }),
            )
            .decision
            .expect("a chain resolution always records a decision")
            .degraded
        };
        let cold = crate::gates::performance::NoPerformance;

        assert!(
            degraded(&cold, Some(SortKey::Latency)),
            "a cold store measures nothing, so the latency sort reorders nothing \
             and the caller silently receives priority order — the single most \
             important thing for a trace to say out loud"
        );
        assert!(
            degraded(
                &ChainReadings(&[("north:charlie", 9, 10.0, 1.0)]),
                Some(SortKey::Latency)
            ),
            "ONE measured candidate of three is written back into its own slot, \
             so the output is priority order too — `< 2`, not `== 0`"
        );

        let two = ChainReadings(&[("north:alpha", 9, 30.0, 1.0), ("south:bravo", 9, 20.0, 1.0)]);
        assert!(
            !degraded(&two, Some(SortKey::Latency)),
            "TWO measured candidates give the sort a preference it can express"
        );
        // ...and here it DID express one, so the line above is not merely a
        // claim about a sort that had nothing to do.
        assert_eq!(
            sort_chain_select(
                &two,
                Some(RoutingPreferences {
                    sort: Some(SortKey::Latency),
                    ..Default::default()
                })
            )
            .all_candidates
            .iter()
            .map(|m| m.model.clone())
            .collect::<Vec<_>>(),
            vec!["bravo", "alpha", "charlie"],
            "bravo (20ms) overtakes alpha (30ms) despite the worse priority, \
             while the unmeasured charlie holds its index"
        );

        assert!(
            !degraded(&SortChainStats, Some(SortKey::Latency)),
            "all three measured is a complete metric sort"
        );
        assert!(
            !degraded(&cold, None),
            "the weighted default consults no SAMPLE COUNT, so it cannot degrade \
             for want of them however cold the store is"
        );
        assert!(
            !degraded(&cold, Some(SortKey::Price)),
            "nor can a price sort, which reads config and never the store"
        );
    }
}
