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
mod panel;
mod stream;
mod util;
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
    /// the write side (sink) lands in a later task — for now the store is only
    /// ever empty, so the [`crate::gates::lockout::ModelLockoutGate`] always admits.
    model_lockout: crate::gates::lockout::ModelLockoutStore,
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
        let recorders: Vec<Arc<dyn crate::gates::HealthRecorder>> = vec![
            Arc::new(crate::gates::circuit_breaker_gate::CircuitBreakerSink::new(
                circuit_breaker.clone(),
            )),
            Arc::new(crate::gates::cooldown::ConnectionCooldownSink::new(
                cooldown.clone(),
                crate::gates::cooldown::DEFAULT_CONNECTION_COOLDOWN,
            )),
        ];
        Self {
            config: Arc::new(RwLock::new(config)),
            adapters,
            circuit_breaker,
            store: None,
            probe: None,
            recorders,
            cooldown,
            model_lockout,
        }
    }

    /// Attach a [`GatewayStore`] so each terminal call is persisted (enabling
    /// burn-rate/spend queries and, on the AUTH track, quota enforcement).
    /// Builder-style; without it the gateway records nothing.
    pub fn with_store(mut self, store: Arc<dyn GatewayStore>) -> Self {
        self.store = Some(store);
        self
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
    }

    /// Dispatch one attempt's outcome to every registered recorder (reliable write-side).
    pub(super) fn record_outcome(
        &self,
        endpoint: &str,
        router: &str,
        success: bool,
        error: Option<&GatewayError>,
    ) {
        dispatch_outcome(&self.recorders, endpoint, router, success, error);
    }
}

/// Dispatch an attempt outcome to every recorder. Free fn so the `'static`
/// stream closure can own a cloned recorder set (where `&self` is unavailable).
pub(super) fn dispatch_outcome(
    recorders: &[std::sync::Arc<dyn crate::gates::HealthRecorder>],
    endpoint: &str,
    router: &str,
    success: bool,
    error: Option<&crate::types::error::GatewayError>,
) {
    let o = crate::gates::AttemptOutcome {
        endpoint,
        router,
        success,
        error,
    };
    for r in recorders {
        r.on_outcome(&o);
    }
}

#[cfg(test)]
mod tests;
