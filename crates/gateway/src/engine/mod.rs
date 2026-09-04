use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::adapters::AdapterRegistry;
use crate::circuit_breaker::CircuitBreakerManager;
use crate::dispatch::{
    from_chat_response, from_embed_response, from_image_response, from_stt_response,
    from_tts_response, from_video_response, to_chat_request, to_embed_request, to_image_request,
    to_stt_request, to_tts_request, to_video_request,
};
use crate::pruning::{Availability, ChainWarning};
use crate::selection::{ModelSelectionService, SelectionCriteria};
use crate::store::{CallStatus, GatewayStore, InferenceCall, UsageTotals};
use crate::types::capability::Capability;
use crate::types::config::{GatewayConfig, QuotaLimit, Window};
use crate::types::cost::{Cost, TokenUsage};
use crate::types::error::GatewayError;
use crate::types::request::{InferenceRequest, InferenceResponse, StreamEvent};
use crate::types::trace::{Attempt, AttemptStatus};

mod consensus;
mod dispatch;
mod execute;
mod exhaustion;
mod panel;
mod stream;
mod util;
/// The window estimator, re-exported out of the crate for the orchestrator's budget
/// clamp. Its own doc carries why the surface is worth it: the clamp bounds `max_tokens`
/// by [`Gateway::min_serving_context_window`] on this figure, and a SECOND estimator
/// computing "the same" number was a live defect for a day. One function, two callers.
pub use util::estimate_input_tokens_pessimistic;
use util::{
    call_estimate, estimate_input_tokens, request_input_text, stream_error_code, usage_value,
    window_start,
};

/// Core gateway orchestrator.
///
/// Resolves model candidates via [`ModelSelectionService`], walks fallback
/// chains, records attempts, and integrates the circuit breaker.
pub struct Gateway {
    config: Arc<RwLock<GatewayConfig>>,
    pub(crate) adapters: AdapterRegistry,
    circuit_breaker: CircuitBreakerManager,
    /// Optional persistence for recorded calls (burn-rate / quota). `None` ⇒
    /// nothing is recorded and behaviour is exactly as before the AUTH track.
    store: Option<Arc<dyn GatewayStore>>,
    /// Optional readiness probe (the local engine's provisioning supervisor).
    /// Consulted only at chain exhaustion to degrade a still-provisioning model
    /// to [`GatewayError::ModelNotReady`]. `None` ⇒ behaviour is byte-identical
    /// to before this seam.
    probe: Option<Arc<dyn kernel::ReadinessProbe>>,
    /// Write-side health recorders fanned out on every attempt outcome
    /// (currently just the circuit breaker sink; more land in later plans).
    recorders: Vec<Arc<dyn crate::gates::HealthRecorder>>,
    /// Router-level connection cooldown read/write state: read side wired into
    /// selection, write side is [`crate::gates::cooldown::ConnectionCooldownSink`]
    /// in `recorders` — both share this one store (see [`Gateway::new`]).
    cooldown: crate::gates::cooldown::ConnectionCooldownStore,
    /// Endpoint model-lockout read/write state: read side wired into selection,
    /// write side is [`crate::gates::lockout::ModelLockoutSink`] in `recorders` —
    /// both share this one store (see [`Gateway::new`]) so the gate skips what
    /// the sink locked.
    model_lockout: crate::gates::lockout::ModelLockoutStore,
    /// Best-effort lockout observers the sink fires into (§5c: the gateway
    /// announces, the caller persists). Registered via [`Gateway::with_observer`]
    /// and shared — one Arc-backed registry — with the
    /// [`crate::gates::lockout::ModelLockoutSink`] in `recorders` (see
    /// [`Gateway::new`]) so a registered observer sees every lock the sink writes.
    lockout_observers: crate::gates::lockout::LockoutBroadcaster,
}

impl Gateway {
    pub fn new(
        config: GatewayConfig,
        adapters: AdapterRegistry,
        circuit_breaker: CircuitBreakerManager,
    ) -> Self {
        // Built before `recorders` so the gate's read-side field and the sink's
        // write-side handle share the SAME store (Arc-backed `Clone`).
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let model_lockout = crate::gates::lockout::ModelLockoutStore::new();
        // Empty registry the sink fires into (a no-op until `with_observer`
        // registers one). Built here and retained on the `Gateway` so the
        // read/write sides share ONE Arc-backed registry: `with_observer`
        // registers into this handle, and the sink below is handed a clone of
        // the SAME registry — so a registered observer sees every lock the
        // sink writes (§5c: the gateway announces, the caller persists).
        let lockout_observers = crate::gates::lockout::LockoutBroadcaster::new();
        // Built AFTER `model_lockout` so the sink's write handle and the gate's
        // read-side field share the SAME Arc-backed store (the gate skips what
        // the sink locked). The default config reproduces today's constants
        // (30s cooldown, 60s/1h/6h lockout) — `with_resilience` rebuilds this
        // from a tuned config while preserving the same shared handles.
        let recorders = build_recorders(
            &circuit_breaker,
            &cooldown,
            &model_lockout,
            &lockout_observers,
            &crate::resilience::ResilienceConfig::default(),
        );
        Self {
            config: Arc::new(RwLock::new(config)),
            adapters,
            circuit_breaker,
            store: None,
            probe: None,
            recorders,
            cooldown,
            model_lockout,
            lockout_observers,
        }
    }

    /// Attach a [`GatewayStore`] so each terminal call is persisted (enabling
    /// burn-rate/spend queries and, on the AUTH track, quota enforcement).
    /// Builder-style; without it the gateway records nothing.
    pub fn with_store(mut self, store: Arc<dyn GatewayStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Register a best-effort lockout observer (the caller persists what the
    /// gateway announces). Builder-style; the core never persists (§5c). The
    /// observer is fired on every lock the [`crate::gates::lockout::ModelLockoutSink`]
    /// writes, since both share one Arc-backed registry (see [`Gateway::new`]).
    pub fn with_observer(
        self,
        observer: Arc<dyn crate::gates::lockout::SelectionObserver>,
    ) -> Self {
        self.lockout_observers.register(observer);
        self
    }

    /// Tune the health gates (cooldown/lockout durations, eviction cap, and
    /// deterministic per-endpoint jitter). Builder-style; rebuilds the recorder pipeline from
    /// `resilience` while preserving the SAME Arc-backed stores/observers/breaker,
    /// so the read-side gates keep reading what the sinks write. Absent ⇒
    /// [`ResilienceConfig::default`] (today's behavior). Construction-time only —
    /// NOT hot-swappable via `update_config` (which carries routing config, not
    /// resilience policy).
    ///
    /// [`ResilienceConfig::default`]: crate::resilience::ResilienceConfig::default
    pub fn with_resilience(mut self, resilience: crate::resilience::ResilienceConfig) -> Self {
        self.recorders = build_recorders(
            &self.circuit_breaker,
            &self.cooldown,
            &self.model_lockout,
            &self.lockout_observers,
            &resilience,
        );
        self
    }

    /// Re-seed a persisted lockout on this instance (caller → gateway, §5c).
    /// Tenant scoping is the caller's — this touches only this instance's
    /// in-memory store; the gateway itself persists nothing.
    pub fn apply_lockout(
        &self,
        endpoint: &str,
        reason: crate::gates::lockout::LockReason,
        until: Option<Instant>,
    ) {
        self.model_lockout.set(endpoint, reason, until, 0);
    }

    /// Clear a lockout (caller-driven suspend release / manual override).
    pub fn clear_lockout(&self, endpoint: &str) {
        self.model_lockout.clear(endpoint);
    }

    /// The smallest `context_window` among a chain's models (read-only; folds the
    /// chain's `ChainEntry`s against the model table). `None` if the chain is
    /// unknown or has no resolvable models.
    ///
    /// **It has NO production caller, and that is the point of the sibling below.** Its
    /// last one was the SP-DATA-5 budget clamp (`executor/dispatch.rs`), which subtracted
    /// its input estimate from this figure to get a `max_tokens` ceiling respecting
    /// `prompt + max_tokens <= context_window`. That bound was safe and far too strong:
    /// on a `[128k, 8k]` chain a 20k prompt gave `8192 − 20000`, saturated to 0, fell
    /// under `MIN_OUTPUT_TOKENS`, and the run was refused inside the orchestrator BEFORE
    /// `Gateway::execute` — so [`gates::context_window::ContextWindowGate`], which would
    /// have admitted the 128k entry, never ran. The clamp now takes
    /// [`Self::min_serving_context_window`] instead.
    ///
    /// [`gates::context_window::ContextWindowGate`]: crate::gates::context_window::ContextWindowGate
    ///
    /// **It is NOT how a candidate's window is judged, and this doc said the opposite
    /// until SP-7a's review.** It read "used by the agent runtime to budget a prompt to
    /// the model it might fall over to — selection is untouched": both halves are now
    /// false. SP-7a deleted the agent runtime's pre-dispatch check (this accessor is
    /// exactly the chain-minimum guess that slice exists to stop trusting), and selection
    /// is emphatically no longer untouched — the gate asks the window question per
    /// CANDIDATE, which is the only place it has a correct answer. Reach for the gate.
    ///
    /// **Why it is still here with nothing calling it**, stated so the next reader does
    /// not have to re-derive it: it is a `pub` read accessor on a library type, deleting
    /// it is a breaking change to that surface for no gain, and it is the contrast the
    /// sibling's argument is made against — its own test pins the chain-minimum answer, so
    /// the suite holds both numbers for one chain and the difference between the two folds
    /// stays legible. (What catches a CLAMP reverting to this fold is not that test but
    /// `a_budgeted_run_serves_a_prompt_only_the_larger_model_can_hold` in the
    /// orchestrator, which is where the consequence lives.) If a third accessor ever wants
    /// this fold, that is the moment to reconsider; a new caller reaching for it should
    /// read [`Self::min_serving_context_window`] first and say why the weaker bound is
    /// right.
    pub async fn min_context_window(&self, chain: &str) -> Option<u32> {
        let cfg = self.config.read().await;
        let chain = cfg.chains.get(chain)?;
        chain
            .models
            .iter()
            .filter_map(|entry| cfg.models.get(&entry.model))
            .map(|m| m.context_window)
            .min()
    }

    /// The smallest `context_window` among a chain's models that can **hold `est`** —
    /// i.e. the minimum over `{ m in chain : m.context_window >= est }`. `None` when no
    /// entry qualifies, and `None` if the chain is unknown or has no resolvable models.
    ///
    /// [`Self::min_context_window`] with one filter, and the filter is the whole slice.
    ///
    /// **One production caller: the SP-DATA-5 budget clamp** (`executor/dispatch.rs`),
    /// which subtracts `est` from this to bound `max_tokens` so the provider's
    /// `prompt + max_tokens <= context_window` rule holds. The clamp sets `max_tokens`
    /// BEFORE selection, so it must pick a value safe for whichever candidate wins
    /// without knowing which — and the set above is exactly the set
    /// [`gates::context_window::ContextWindowGate`] admits.
    ///
    /// [`gates::context_window::ContextWindowGate`]: crate::gates::context_window::ContextWindowGate
    ///
    /// # Why the bound is sound
    ///
    /// Let `S = { m in chain : m.context_window >= est }`, **for the one `est` the caller
    /// and the gate share** — see the section below, which is not a caveat but the
    /// premise every bullet here rests on.
    ///
    /// - Selection can only return a member of `S` — the gate skips every non-member,
    ///   on that same `est`.
    /// - This returns `min { m.context_window : m in S }`.
    /// - Every `m in S` has `m.context_window >= min(S)`, so bounding output by
    ///   `min(S) − est` leaves at least that much room in EVERY admissible candidate,
    ///   including the one that wins.
    /// - Every member satisfies `context_window >= est`, so `min(S) − est >= 0`. The
    ///   saturation that produced the original defect — `8192 − 20000 → 0`, under the
    ///   output floor, refused — cannot occur.
    ///
    /// `S` empty ⇒ `None` ⇒ the caller contributes no window bound at all, and selection
    /// will admit nothing, so the request is refused by the gate with per-candidate
    /// diagnostics rather than by an upstream guess. That handover is deliberate: two
    /// components refusing one condition in two vocabularies means the one that fires
    /// first is the one that misdirects.
    ///
    /// # The premise: ONE `est`, and it shipped violated
    ///
    /// Every bullet above says `est`, and for a day the two halves computed two different
    /// numbers. The clamp had its own estimator (`dispatch::est_input_tokens`) applying
    /// `ceil` per string and summing over CHARACTERS; the gate judged on
    /// [`crate::estimate_input_tokens_pessimistic`], summing BYTES and applying `ceil`
    /// once. On ASCII `Σ ceil(Lᵢ/3) >= ceil(Σ Lᵢ/3)`, so the clamp's figure was the
    /// LARGER one and `S_clamp` a strict SUBSET of the set selection drew from — which
    /// falsifies the first bullet outright and, with it, the `S`-empty handover ("nothing
    /// can serve it" was decided on a number the gate did not use). Both reachable
    /// consequences ended at a provider 400 on a budgeted run: an empty `S_clamp` against
    /// a non-empty `S_gate` dropped the window term entirely, and a `S_clamp` missing the
    /// small candidate bound by a big model's window and sent it to the small one.
    ///
    /// It is closed structurally rather than by agreement: the clamp calls the gate's own
    /// estimator, on the very `Payload` it is about to dispatch, so `est_clamp` and
    /// `est_gate` are one value and cannot drift. That is what the `pub` on
    /// [`crate::estimate_input_tokens_pessimistic`] is for, and it is why aligning the two
    /// formulas by hand was rejected — the drift ran the OTHER way on multi-byte text
    /// (bytes here, chars there), so neither figure bounded the other and no slack
    /// constant could have covered it. Pinned across the crate boundary by the
    /// orchestrator's `the_clamp_bounds_max_tokens_by_the_estimate_the_gate_will_judge_by`
    /// and `the_serving_window_bound_is_safe_for_the_smallest_candidate_the_gate_admits`.
    ///
    /// # The coupling that remains, stated plainly
    ///
    /// Soundness still depends on the gate being REGISTERED. Remove it and this bound
    /// could name a window belonging to a model selection would then return without it
    /// fitting. That is a real dependency between two crates, and it is why "bound by the
    /// chain's LARGEST window" — the other one-call change that fixes the reported
    /// symptom — was rejected: this version degrades to OVER-bounding if the coupling
    /// breaks, because `min(S) >= min(chain)`, where the largest would under-bound and
    /// send a big model's `max_tokens` to a small one.
    ///
    /// The boundary is `>=`, mirroring the gate's `est > window` skip exactly. A request
    /// of precisely `window` tokens is held by that model, so it belongs to `S`; the two
    /// must agree on the edge or the set this reasons over stops being the set selection
    /// draws from.
    ///
    /// **The one degenerate case, stated so it is not mistaken for a guarantee.** For any
    /// `est > 0` a mis-configured `context_window: 0` entry is simply filtered out and no
    /// longer drags the caller's bound down — a genuine improvement over the plain `min`,
    /// which such an entry poisoned for every request on the chain. At `est == 0` (an
    /// empty prompt) that entry qualifies, `0 >= 0`, and the bound is `Some(0)` again.
    /// Not special-cased here: `collect_validation_errors`' Rule 6 rejects a zero
    /// `context_window` outright, so this is narrowed to the documented unchecked
    /// `Gateway::new` / `update_config` path rather than defended against per reader.
    ///
    /// Bounding by the SELECTED candidate instead — strictly more precise than any
    /// chain-wide fold — remains the clamp spec's §8 item. It needs the run's budget
    /// plumbed into the gateway, across a boundary the two crates keep clean.
    pub async fn min_serving_context_window(&self, chain: &str, est: u32) -> Option<u32> {
        let cfg = self.config.read().await;
        let chain = cfg.chains.get(chain)?;
        chain
            .models
            .iter()
            .filter_map(|entry| cfg.models.get(&entry.model))
            .map(|m| m.context_window)
            .filter(|window| *window >= est)
            .min()
    }

    /// The LARGEST `context_window` among a chain's models. `None` if the chain is unknown or
    /// has no resolvable models.
    ///
    /// The SP-7b context budget's target, and the fold is `max` for a reason worth stating
    /// because every sibling accessor here folds `min`. Those bound a value that must be safe
    /// for whichever candidate selection eventually returns, so they take the worst case. This
    /// one answers a different question — "how much prompt could ANY model on this chain hold?"
    /// — and shrinking a prompt to that figure is the LEAST cutting that still fits somebody.
    ///
    /// Safe despite being the most permissive fold, because it does not decide admission:
    /// [`gates::context_window::ContextWindowGate`] still asks per candidate afterwards, so a
    /// prompt budgeted to 128k on a `[128k, 8k]` chain simply gets the 8k entry skipped and
    /// lands on the 128k one — which is exactly SP-7a's designed behaviour. This is NOT the
    /// "bound by the chain's largest" alternative the SP-7a follow-on spec rejected for the
    /// CLAMP (§3): that one was rejected because `max_tokens` must be safe for the SELECTED
    /// candidate and nothing re-checks it after selection, so a `max` fold there would hand a
    /// big model's output allowance to a small one. Here the input is being shrunk so that at
    /// least one candidate can hold it, and the per-candidate check still runs.
    ///
    /// [`gates::context_window::ContextWindowGate`]: crate::gates::context_window::ContextWindowGate
    pub async fn max_context_window(&self, chain: &str) -> Option<u32> {
        let cfg = self.config.read().await;
        let chain = cfg.chains.get(chain)?;
        chain
            .models
            .iter()
            .filter_map(|entry| cfg.models.get(&entry.model))
            .map(|m| m.context_window)
            .max()
    }

    /// The smallest `max_output_tokens` among a chain's models (read-only; the output
    /// twin of [`Self::min_context_window`], folded the same way). `None` if the chain
    /// is unknown or has no resolvable models.
    ///
    /// Added for the SP-DATA-5 budget clamp, which sets `max_tokens` on a budgeted
    /// request from what the remaining budget affords. That number is a BUDGET figure
    /// and knows nothing about the model: with a whole-run cap of 100k it can easily
    /// exceed a model's own output limit, and the providers do not treat that kindly —
    /// Anthropic rejects a `max_tokens` above the model's maximum with a 400, and the
    /// adapters in this repo forward the value verbatim
    /// (`cloud-providers/src/anthropic/mod.rs`, `gemini.rs`, `bedrock/mod.rs`,
    /// `openai_compat/mod.rs`). So the clamp bounds itself by this before emitting.
    ///
    /// `min` over the chain rather than the selected model's own figure, for exactly the
    /// reason [`Self::min_serving_context_window`] takes a `min`: the caller sets
    /// `max_tokens` before selection, a request that fails over lands on a DIFFERENT
    /// entry, and a value the fallback model would reject turns a survivable failover
    /// into a hard 400. The smallest limit in the chain is the only one safe for every
    /// entry in it.
    ///
    /// **But it is a `min` over the WHOLE chain, where its window sibling folds over a
    /// SUBSET, and that asymmetry is forced rather than chosen.** The serving-window bound
    /// can narrow to `{ m : m.context_window >= est }` only because
    /// [`gates::context_window::ContextWindowGate`] skips exactly the complement on
    /// exactly that `est` — so the fold and selection reason over one set. There is no
    /// counterpart gate for the output limit: nothing skips a candidate for declaring a
    /// small `max_output_tokens`, so every entry stays reachable and the plain chain
    /// minimum is the only fold safe for all of them. Adding a filter here without adding
    /// the gate that justifies it would send a value the surviving entries reject.
    ///
    /// [`gates::context_window::ContextWindowGate`]: crate::gates::context_window::ContextWindowGate
    ///
    /// (This paragraph named `min_context_window` until the serving-window follow-on. That
    /// accessor is still the plain chain fold and still `pub`, but it has no production
    /// caller: the clamp's window term is the serving one now, so citing it here pointed a
    /// reader at the shape the clamp had stopped using.)
    ///
    /// **The cost of that `min` on a HETEROGENEOUS chain, stated plainly.** A budgeted
    /// run on `[gpt-4o 16384, small-fallback 4096]` has its replies capped at 4096 on the
    /// PRIMARY, from the very first call and however much budget remains — while the same
    /// run unbudgeted sends `max_tokens: None` and, on an `openai_compat` router, gets the
    /// selected model's own 16384. So this is a narrowing that is independent of how near
    /// the cap the run is, which is a different effect from the clamp's "replies get
    /// shorter as the budget runs down". Visible rather than silent (the clamp emits a
    /// `tracing::info!` naming both bounds), and recorded in the clamp design's §6. The
    /// alternative — bounding by the SELECTED model after selection — needs the clamp to
    /// move downstream of the selector and is §8 work.
    ///
    /// **`Some(0)` is possible and is a config bug, not a limit.** One entry with
    /// `max_output_tokens: 0` makes the whole chain's ceiling zero, and the budget clamp
    /// would then emit `max_tokens: Some(0)` — "generate nothing" — on every budgeted
    /// `Chat` call. `collect_validation_errors` rejects such a model, so it cannot arrive
    /// through `GatewayBuilder::build`, `Gateway::try_new` or `try_update_config`; the
    /// unchecked `Gateway::new` / `update_config` pair validate nothing by their own
    /// documented design, so this is narrowed rather than made unrepresentable.
    ///
    /// Selection, gates and `execute` are untouched — this is a read of the same config
    /// table they read.
    pub async fn min_max_output_tokens(&self, chain: &str) -> Option<u32> {
        let cfg = self.config.read().await;
        let chain = cfg.chains.get(chain)?;
        chain
            .models
            .iter()
            .filter_map(|entry| cfg.models.get(&entry.model))
            .map(|m| m.max_output_tokens)
            .min()
    }

    /// Attach a readiness probe (the local engine's provisioning supervisor).
    /// Builder-style, mirroring [`Self::with_store`]. When set, chain exhaustion
    /// consults the probe and degrades a still-provisioning candidate to a
    /// terminal [`GatewayError::ModelNotReady`] instead of the generic
    /// `AllAttemptsFailed`; the selection algorithm is otherwise untouched.
    pub fn with_readiness(mut self, probe: Arc<dyn kernel::ReadinessProbe>) -> Self {
        self.probe = Some(probe);
        self
    }

    /// Prune permanently-unavailable candidates from the live config's chains
    /// and return the dropped-and-why report. Write-locks the config, applies
    /// [`crate::pruning::prune_unavailable`], and returns the warnings. The
    /// `judge` encodes availability the library can't see from config alone
    /// (e.g. "cloud router has no API key"); disabled/unknown routers and
    /// unknown models are dropped without it. Provisioning (`Pending`)
    /// candidates are kept.
    pub async fn prune_unavailable(
        &self,
        judge: impl Fn(&str, &str) -> Availability,
    ) -> Vec<ChainWarning> {
        let mut cfg = self.config.write().await;
        crate::pruning::prune_unavailable(&mut cfg, judge)
    }

    /// Best-effort persist a recorded call. Metering must never take down the
    /// hot path, so a store error is logged and swallowed. No-op without a store.
    async fn record_call(&self, call: InferenceCall) {
        if let Some(store) = &self.store
            && let Err(e) = store.insert_inference_call(&call).await
        {
            tracing::warn!(error = %e, "failed to record inference call (metering is best-effort)");
        }
    }

    /// Pre-flight subscription-quota check (AUTH). Refuses the call with
    /// [`GatewayError::QuotaExceeded`] before any provider is contacted when the
    /// request's subject has exhausted an applicable configured limit.
    ///
    /// No-op (`Ok`) when there is no store, no `request.auth`, or no matching
    /// tier constraints — so unauthenticated callers and unconfigured tiers
    /// behave exactly as before. Soft/advisory under concurrency (D3): usage is
    /// read then checked, so concurrent calls can overshoot by ~the in-flight
    /// count. `OutputTokens`/`CostUsdMillis` have no pre-call estimate, so those
    /// caps engage once the recorded usage already crosses the line.
    async fn check_quota(
        &self,
        config: &GatewayConfig,
        request: &InferenceRequest,
        input_tokens: u32,
    ) -> Result<(), GatewayError> {
        let Some(store) = &self.store else {
            return Ok(());
        };
        let Some(auth) = &request.auth else {
            return Ok(());
        };

        // Resolve the tier's constraints: the named tier, else the default.
        let constraints = auth
            .tier
            .as_deref()
            .and_then(|t| config.constraints.tiers.get(t))
            .or(config.constraints.default.as_ref());
        let Some(constraints) = constraints else {
            return Ok(());
        };

        // Effective limits = tier-wide quota + this capability's overrides.
        let mut limits: Vec<&QuotaLimit> = constraints.quota.iter().collect();
        if let Some(extra) = constraints.per_capability.get(&request.capability) {
            limits.extend(extra.iter());
        }
        if limits.is_empty() {
            return Ok(());
        }

        // One usage read per distinct window, reused across its limits.
        let now = Utc::now();
        let windows: HashSet<Window> = limits.iter().map(|l| l.window).collect();
        let mut usage_by_window: HashMap<Window, UsageTotals> = HashMap::new();
        for w in windows {
            let usage = store
                .get_usage_since(auth.subject_id, window_start(now, w))
                .await?;
            usage_by_window.insert(w, usage);
        }

        for limit in &limits {
            let used = usage_value(&usage_by_window[&limit.window], limit.unit);
            let this_call = call_estimate(limit.unit, input_tokens);
            if used.saturating_add(this_call) > limit.limit {
                return Err(GatewayError::QuotaExceeded {
                    unit: limit.unit,
                    window: limit.window,
                    limit: limit.limit,
                    used,
                });
            }
        }
        Ok(())
    }

    /// Like [`Gateway::new`], but validates `config` first.
    ///
    /// Returns [`GatewayError::InvalidConfig`] if the config fails the same
    /// rules enforced by [`GatewayBuilder`](crate::config::GatewayBuilder)
    /// (at least one router, non-empty router URLs, chain model references
    /// resolve, model providers have a router). `new` remains unchecked.
    pub fn try_new(
        config: GatewayConfig,
        adapters: AdapterRegistry,
        circuit_breaker: CircuitBreakerManager,
    ) -> Result<Self, GatewayError> {
        crate::config::validate_config(&config)?;
        Ok(Self::new(config, adapters, circuit_breaker))
    }

    /// Replace the gateway configuration at runtime.
    pub async fn update_config(&self, config: GatewayConfig) {
        let mut guard = self.config.write().await;
        *guard = config;
    }

    /// Like [`Gateway::update_config`], but validates `config` before swapping.
    ///
    /// Returns [`GatewayError::InvalidConfig`] (leaving the current config in
    /// place) if validation fails. `update_config` remains unchecked.
    pub async fn try_update_config(&self, config: GatewayConfig) -> Result<(), GatewayError> {
        crate::config::validate_config(&config)?;
        self.update_config(config).await;
        Ok(())
    }

    /// Return a sorted list of all registered adapter ids.
    pub async fn list_adapters(&self) -> Vec<String> {
        self.adapters.list().await
    }

    /// Flat list of all configured models, each entry router-qualified.
    pub async fn list_models(&self) -> Result<Vec<serde_json::Value>, GatewayError> {
        let config = self.config.read().await;
        let mut out = Vec::with_capacity(config.models.len());
        for (id, m) in config.models.iter() {
            out.push(serde_json::json!({
                "id":               id,
                "api_model_id":     m.api_model_id,
                "provider":         m.provider,
                "capabilities":     m.capabilities,
                "context_window":   m.context_window,
                "max_output_tokens": m.max_output_tokens,
            }));
        }
        Ok(out)
    }

    /// Models reachable through a specific router. Walks fallback chains
    /// for any entry whose `router` matches, plus any model whose default
    /// provider matches the router id (single-provider routers).
    pub async fn list_models_for_router(
        &self,
        router_id: &str,
    ) -> Result<Vec<serde_json::Value>, GatewayError> {
        let config = self.config.read().await;
        let mut model_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
        // Single-provider routers: model.provider == router id.
        for (id, m) in config.models.iter() {
            if m.provider == router_id {
                model_ids.insert(id.clone());
            }
        }
        // Explicit chain router pins.
        for chain in config.chains.values() {
            for entry in &chain.models {
                if entry.router.as_deref() == Some(router_id) {
                    model_ids.insert(entry.model.clone());
                }
            }
        }
        let mut out = Vec::with_capacity(model_ids.len());
        for id in model_ids {
            if let Some(m) = config.models.get(&id) {
                out.push(serde_json::json!({
                    "id":               id,
                    "api_model_id":     m.api_model_id,
                    "provider":         m.provider,
                    "capabilities":     m.capabilities,
                }));
            }
        }
        Ok(out)
    }

    /// Whether the gateway has any configuration (routers, models, chains).
    /// Returns false if the config is empty — callers should not attempt
    /// execute() until config has been set via update_config().
    pub async fn is_configured(&self) -> bool {
        let config = self.config.read().await;
        !config.routers.is_empty() || !config.models.is_empty() || !config.chains.is_empty()
    }

    /// Re-resolve `api_key` for every router from a caller-supplied
    /// resolver function. Used after a key is set/cleared so the next
    /// request picks up the change without a daemon restart.
    pub async fn refresh_router_keys<F>(&self, resolver: F)
    where
        F: Fn(&str) -> Option<String>,
    {
        let mut config = self.config.write().await;
        for (id, router) in config.routers.iter_mut() {
            router.api_key = resolver(id);
        }
        // A refreshed credential may fix an auth/credits lock on any of that
        // router's endpoints, so clear its terminal locks — the model is
        // eligible again next request. Timed (rate/quota) locks are unrelated
        // to the key and are left intact. `model_lockout` is a separate,
        // Arc-backed field, so this coexists with the `config` write guard.
        for id in config.routers.keys() {
            self.model_lockout.clear_terminal_for_router(id);
        }
    }

    /// Dispatch one attempt's outcome to every registered recorder (reliable
    /// write-side) and return the earliest `Instant` any recorder just wrote as
    /// this endpoint's unavailability deadline, or `None` if none did.
    pub(super) fn record_outcome(
        &self,
        endpoint: &str,
        router: &str,
        success: bool,
        error: Option<&GatewayError>,
    ) -> Option<std::time::Instant> {
        dispatch_outcome(&self.recorders, endpoint, router, success, error)
    }
}

/// Build the write-side health recorder pipeline from a [`ResilienceConfig`].
///
/// The single sink-construction site, shared by [`Gateway::new`] (with
/// [`ResilienceConfig::default`]) and [`Gateway::with_resilience`]. Each sink is
/// handed a **clone** of the caller's Arc-backed store/observers/breaker, so the
/// rebuilt sinks write to the SAME shared handles the read-side gates read —
/// only the tunable durations (cooldown base, lockout policy) change.
///
/// [`ResilienceConfig`]: crate::resilience::ResilienceConfig
/// [`ResilienceConfig::default`]: crate::resilience::ResilienceConfig::default
fn build_recorders(
    breaker: &CircuitBreakerManager,
    cooldown: &crate::gates::cooldown::ConnectionCooldownStore,
    model_lockout: &crate::gates::lockout::ModelLockoutStore,
    observers: &crate::gates::lockout::LockoutBroadcaster,
    resilience: &crate::resilience::ResilienceConfig,
) -> Vec<Arc<dyn crate::gates::HealthRecorder>> {
    vec![
        Arc::new(crate::gates::circuit_breaker_gate::CircuitBreakerSink::new(
            breaker.clone(),
        )),
        Arc::new(crate::gates::cooldown::ConnectionCooldownSink::new(
            cooldown.clone(),
            resilience.cooldown_base,
            resilience.eviction_cap,
            resilience.jitter_fraction,
        )),
        Arc::new(crate::gates::lockout::ModelLockoutSink::new(
            model_lockout.clone(),
            resilience.lockout.clone(),
            observers.clone(),
            resilience.eviction_cap,
            resilience.jitter_fraction,
        )),
    ]
}

/// Dispatch an attempt outcome to every recorder. Free fn so the `'static`
/// stream closure can own a cloned recorder set (where `&self` is unavailable).
/// Returns the earliest deadline any recorder just wrote (min-fanned), or `None`.
pub(super) fn dispatch_outcome(
    recorders: &[std::sync::Arc<dyn crate::gates::HealthRecorder>],
    endpoint: &str,
    router: &str,
    success: bool,
    error: Option<&crate::types::error::GatewayError>,
) -> Option<std::time::Instant> {
    let o = crate::gates::AttemptOutcome {
        endpoint,
        router,
        success,
        error,
    };
    recorders.iter().filter_map(|r| r.on_outcome(&o)).min()
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod min_window_tests {
    use super::*;
    use crate::adapters::AdapterRegistry;
    use crate::circuit_breaker::{CircuitBreakerConfig, CircuitBreakerManager};
    use kernel::types::capability::Capability;
    use kernel::types::config::{
        ChainEntry, FallbackChainConfig, GatewayConfig, ModelConfig, RouterConfig,
    };
    use std::collections::HashMap;

    fn model(id: &str, window: u32) -> ModelConfig {
        model_with_output(id, window, 1024)
    }

    fn model_with_output(id: &str, window: u32, max_output_tokens: u32) -> ModelConfig {
        ModelConfig {
            id: id.into(),
            api_model_id: None,
            provider: "r".into(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: window,
            max_output_tokens,
            pricing: None,
            catalog: None,
        }
    }

    /// Build a two-model chain `"c"` from a pair of `ModelConfig`s, for the chain-wide
    /// accessor tests below — the three `min` folds and, since SP-7b, the `max` one.
    /// Factored out so they cannot drift apart on topology and disagree for a reason that
    /// has nothing to do with what they read.
    fn two_model_chain(a: ModelConfig, b: ModelConfig) -> GatewayConfig {
        let mut routers = HashMap::new();
        routers.insert(
            "r".into(),
            RouterConfig {
                url: "http://x".into(),
                api_key_env: None,
                api_key: None,
                enabled: true,
                timeout_ms: None,
                headers: HashMap::new(),
            },
        );
        let entries = vec![
            ChainEntry {
                model: a.id.clone(),
                router: Some("r".into()),
                api_model_id: None,
                priority: 1,
            },
            ChainEntry {
                model: b.id.clone(),
                router: Some("r".into()),
                api_model_id: None,
                priority: 2,
            },
        ];
        let mut models = HashMap::new();
        models.insert(a.id.clone(), a);
        models.insert(b.id.clone(), b);
        let mut chains = HashMap::new();
        chains.insert(
            "c".into(),
            FallbackChainConfig {
                id: "c".into(),
                capability: Capability::TextChat,
                models: entries,
                fallback_triggers: Vec::new(),
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

    /// The output half of the SP-DATA-5 clamp's ceiling — **one of two terms, not the
    /// ceiling itself.** The clamp takes `min(this, min_serving_context_window − est)`,
    /// so a test that pins only this number says nothing about which term binds; the
    /// window half is pinned by
    /// `min_serving_context_window_is_the_smallest_window_that_can_hold_the_estimate`
    /// here and by the orchestrator's
    /// `the_serving_window_bound_is_safe_for_the_smallest_candidate_the_gate_admits`.
    ///
    /// `min` over the chain rather than the selected model's own figure, for the same
    /// reason the window accessor takes a `min`: the caller does not know which entry
    /// the request will land on, and a fallback to the smaller model must not carry a
    /// `max_tokens` that model would reject. Unlike the window accessor it cannot narrow
    /// to a serving SUBSET — see the production doc for why the absent gate is what
    /// forbids it.
    ///
    /// The unknown-chain leg is asserted too, because it is the leg the clamp treats as
    /// "no ceiling from here" — a `Some(0)` or a panic there would silently refuse or
    /// truncate every budgeted call on an unregistered chain.
    #[tokio::test]
    async fn min_max_output_tokens_is_the_smallest_output_limit_in_the_chain() {
        let gw = Gateway::new(
            two_model_chain(
                model_with_output("big", 200_000, 8_192),
                model_with_output("small", 8_000, 4_096),
            ),
            AdapterRegistry::new(),
            CircuitBreakerManager::new(CircuitBreakerConfig::default()),
        );

        assert_eq!(gw.min_max_output_tokens("c").await, Some(4_096));
        assert_eq!(gw.min_max_output_tokens("nope").await, None);
    }

    #[tokio::test]
    async fn min_context_window_is_the_smallest_model_in_the_chain() {
        let gw = Gateway::new(
            two_model_chain(model("big", 200_000), model("small", 8_000)),
            AdapterRegistry::new(),
            CircuitBreakerManager::new(CircuitBreakerConfig::default()),
        );

        assert_eq!(gw.min_context_window("c").await, Some(8_000));
        assert_eq!(gw.min_context_window("nope").await, None);
    }

    /// **AC1** — the SP-7b context budget's target is the chain's LARGEST window.
    ///
    /// Shrinking a prompt to the largest window is the least cutting that still fits something,
    /// and it stays safe because this fold does not decide admission: `ContextWindowGate` asks
    /// the window question per CANDIDATE afterwards, so a smaller entry is skipped rather than
    /// handed a prompt it cannot hold.
    ///
    /// The heterogeneous fixture is load-bearing: on a homogeneous chain `max` and `min` agree,
    /// so a `min` fold would pass the first assertion for the wrong reason. The `assert_ne!`
    /// pins that property of the fixture rather than trusting the two literals to stay apart.
    ///
    /// Filed beside `min_context_window_is_the_smallest_model_in_the_chain` on the SAME
    /// `two_model_chain` topology, so the two folds answer 200 000 and 8 000 for ONE chain —
    /// the max-vs-min contrast the accessor's own doc argues for, legible in one place, on the
    /// shared fixture that exists to stop these tests drifting apart on topology.
    #[tokio::test]
    async fn max_context_window_is_the_largest_window_in_the_chain() {
        let gw = Gateway::new(
            two_model_chain(model("big", 200_000), model("small", 8_000)),
            AdapterRegistry::new(),
            CircuitBreakerManager::new(CircuitBreakerConfig::default()),
        );

        assert_eq!(
            gw.max_context_window("c").await,
            Some(200_000),
            "the LARGEST window, not the smallest — the smallest is what min_context_window answers"
        );
        assert_ne!(
            gw.max_context_window("c").await,
            gw.min_context_window("c").await,
            "and the fixture must stay heterogeneous or this test cannot distinguish the folds"
        );
        assert_eq!(
            gw.max_context_window("nope").await,
            None,
            "an unknown chain has no answer"
        );

        // AC1's second `None` leg, and the reason it is asserted separately: the
        // unknown-chain case above returns at `chains.get(chain)?`, BEFORE the fold, so it
        // says nothing about what the fold does with nothing to fold. Only a chain whose
        // entries resolve to no `ModelConfig` reaches `max()` on an empty iterator. The
        // doc's "or has no resolvable models" is a claim about this path alone.
        let mut orphaned = two_model_chain(model("big", 200_000), model("small", 8_000));
        orphaned.models.clear();
        let gw_orphaned = Gateway::new(
            orphaned,
            AdapterRegistry::new(),
            CircuitBreakerManager::new(CircuitBreakerConfig::default()),
        );

        assert_eq!(
            gw_orphaned.max_context_window("c").await,
            None,
            "a registered chain whose entries resolve to no model has no answer either"
        );
    }

    /// **AC1 + AC2** — the smallest window at or above the estimate, `None` when nothing
    /// qualifies, `None` for an unknown chain.
    ///
    /// The straddle case is the one that matters and the one
    /// `min_context_window_is_the_smallest_model_in_the_chain` cannot express: on the
    /// SAME chain and the SAME request the two accessors give 8 000 and 200 000, and the
    /// gap between them is the whole defect. 8 000 is the number the SP-DATA-5 clamp
    /// subtracted the estimate from, saturating to 0 and refusing a request the 200k
    /// entry serves happily.
    ///
    /// **The `est == window` boundary is admitted**, matching
    /// `ContextWindowGate`'s `est > window` skip. The two must agree exactly or the
    /// bound stops being sound: admit one more candidate than the gate does and the
    /// clamp can bound by a window belonging to a model selection will never return;
    /// admit one fewer and a request the gate would serve is bounded by a larger window
    /// than the winner's. Pinned here rather than left to the `>=` reading well, because
    /// `>` compiles just as happily.
    ///
    /// **AC2's own claim — it never returns a window BELOW `est`** — is stated as a
    /// property over every case rather than as another literal, so a future entry added
    /// to the fixture is covered by it automatically.
    #[tokio::test]
    async fn min_serving_context_window_is_the_smallest_window_that_can_hold_the_estimate() {
        let gw = Gateway::new(
            two_model_chain(model("big", 200_000), model("small", 8_000)),
            AdapterRegistry::new(),
            CircuitBreakerManager::new(CircuitBreakerConfig::default()),
        );

        // Straddling the two: only `big` qualifies, so `big`'s window is the answer —
        // where the plain chain minimum answers 8 000.
        assert_eq!(
            gw.min_serving_context_window("c", 20_000).await,
            Some(200_000)
        );
        // Under both: every entry qualifies and the answer collapses to the chain
        // minimum. This is the no-regression case, AC7's shape at the accessor level.
        assert_eq!(gw.min_serving_context_window("c", 1_000).await, Some(8_000));
        // Exactly `small`'s window: admitted, because `ContextWindowGate` skips only on
        // `est > window`. One token more and `small` drops out of the set.
        assert_eq!(gw.min_serving_context_window("c", 8_000).await, Some(8_000));
        assert_eq!(
            gw.min_serving_context_window("c", 8_001).await,
            Some(200_000)
        );
        // Over every entry: the empty set, reported as `None` rather than as some
        // fallback figure. The clamp turns this into "no window term", which hands the
        // refusal to the gate.
        assert_eq!(gw.min_serving_context_window("c", 200_001).await, None);
        // An unknown chain, exactly as the two `min` accessors treat it.
        assert_eq!(gw.min_serving_context_window("nope", 1).await, None);

        // AC2 as a property: whatever it returns, it can hold the estimate it was asked
        // about. This is what makes `window − est` unable to saturate to 0 — the way the
        // chain minimum failed.
        for est in [0u32, 1, 7_999, 8_000, 8_001, 20_000, 199_999, 200_000] {
            if let Some(w) = gw.min_serving_context_window("c", est).await {
                assert!(
                    w >= est,
                    "a serving window must be able to hold the estimate it was chosen \
                     for: {w} < {est}"
                );
            }
        }
    }

    /// **AC7** — on a HOMOGENEOUS chain the serving bound is the chain minimum, for
    /// every estimate that any entry can hold.
    ///
    /// `min(S) == min(chain)` whenever every entry qualifies, and this is the claim that
    /// makes the change additive for the single-model and equal-window configs that make
    /// up most of the fixtures in this repo. Asserted as the two accessors AGREEING
    /// rather than as a literal, because the point is the relationship, not the number.
    #[tokio::test]
    async fn min_serving_context_window_matches_the_chain_minimum_on_a_homogeneous_chain() {
        let gw = Gateway::new(
            two_model_chain(model("a", 8_000), model("b", 8_000)),
            AdapterRegistry::new(),
            CircuitBreakerManager::new(CircuitBreakerConfig::default()),
        );

        for est in [0u32, 1, 4_000, 7_999, 8_000] {
            assert_eq!(
                gw.min_serving_context_window("c", est).await,
                gw.min_context_window("c").await,
                "every entry holds {est}, so the serving set is the whole chain and the \
                 two bounds must be the same number"
            );
        }
        // Past the shared window the sets diverge — the serving set empties while the
        // chain minimum keeps answering 8 000. Asserted so the agreement above reads as
        // "when every entry qualifies" rather than as "always".
        assert_eq!(gw.min_serving_context_window("c", 8_001).await, None);
        assert_eq!(gw.min_context_window("c").await, Some(8_000));
    }
}
