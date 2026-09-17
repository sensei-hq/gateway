use crate::skip_reason::SkipReason;
use crate::types::capability::Capability;
use crate::types::config::{GatewayConfig, ModelConfig, RouterConfig};
use crate::types::error::GatewayError;
use crate::types::request::RoutingPreferences;
use std::time::Instant;

pub mod budget;
pub mod capability;
pub mod circuit_breaker_gate;
pub mod context_window;
pub mod cooldown;
pub mod lockout;
pub mod performance;
pub mod routing_policy;

/// Read port for endpoint health (the circuit breaker implements it in Task 4;
/// cooldown/lockout ports arrive in later SP-0 plans).
pub trait EndpointHealthRead: Send + Sync {
    /// `Some(until)` if the endpoint is currently open/unavailable with a retry time.
    fn open_until(&self, endpoint: &str) -> Option<Instant>;
}

/// Read port for router-level health (connection cooldown; more router ports later).
pub trait RouterHealthRead: Send + Sync {
    /// `Some(until)` if the router is currently cooling down.
    fn cooling_until(&self, router: &str) -> Option<Instant>;
}

/// A resolved candidate ready for gating (structural resolution already succeeded).
pub struct CandidateView<'a> {
    pub model: &'a str,
    pub router: &'a str,
    pub endpoint: String, // "router:model" opaque key
    pub model_config: &'a ModelConfig,
    pub router_config: &'a RouterConfig,
}

pub struct SelectionCtx<'a> {
    pub capability: Capability,
    pub budget: Option<f64>,
    pub input_tokens: Option<u32>,
    /// The pessimistic estimate, for [`context_window::ContextWindowGate`] only.
    ///
    /// A SECOND field rather than a replacement for `input_tokens`: the cost gate and the
    /// window gate want opposite biases over the same payload (an under-count is
    /// optimistic pricing for one and an admitted-but-doesn't-fit candidate for the
    /// other — see `engine::util::estimate_input_tokens_pessimistic`), and collapsing
    /// them to one number is exactly what that reasoning rules out.
    pub input_tokens_pessimistic: Option<u32>,
    pub health: &'a dyn EndpointHealthRead,
    pub now: Instant,
    pub config: &'a GatewayConfig,
    pub router_health: &'a dyn RouterHealthRead,
    pub model_lockout: &'a dyn crate::gates::lockout::ModelLockoutRead,
    /// The request's routing preferences, read by [`routing_policy::RoutingPolicyGate`].
    /// `None` ⇒ no filtering.
    pub preferences: Option<&'a RoutingPreferences>,
}

pub enum GateVerdict {
    Admit,
    Skip(SkipReason),
}

pub trait AdmissionGate: Send + Sync {
    fn name(&self) -> &'static str;
    fn evaluate(&self, cand: &CandidateView<'_>, ctx: &SelectionCtx<'_>) -> GateVerdict;
}

/// What an `AttemptOutcome` is an observation OF. Read only by
/// `PerformanceRecorder`; the health recorders (breaker/cooldown/lockout)
/// ignore it — `success`/`error` mean exactly what they always have to them.
///
/// This exists because one streaming attempt produces TWO `AttemptOutcome`
/// dispatches (acquisition, then completion), and naively treating both as
/// full observations pools two unrelated time spans into one latency mean and
/// lets a single attempt cast two reliability votes — an endpoint that fails
/// every stream mid-way would converge on `success_rate == 0.5` forever,
/// because the acquisition success is counted alongside the mid-stream
/// failure. `AttemptPhase` is how `PerformanceRecorder` tells which of the
/// two spans `duration_ms` is, and which outcome (if either) is the verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptPhase {
    /// A complete request/response. `duration_ms` is time-to-response and the
    /// verdict is final. Every non-streaming attempt, and every setup failure
    /// (streaming or not — no completion dispatch follows a setup failure).
    Complete,
    /// A stream was obtained. `duration_ms` is time-to-first-response — the
    /// same quantity as `Complete`'s, hence comparable — but the verdict is
    /// NOT final, because a completion outcome for this same attempt always
    /// follows.
    StreamAcquired,
    /// A stream ended. `duration_ms` is GENERATION time, which is not
    /// comparable to the other two phases' `duration_ms`, so it contributes no
    /// latency observation. The verdict is final.
    StreamCompleted,
}

/// A single attempt's outcome, fed to the write-side recorders. `endpoint` is the
/// opaque "router:model" key (matches the read-side breaker keying). `router` is
/// carried separately since `endpoint` can't be split reliably back into
/// router/model (model ids contain `:`, e.g. `gemma3:27b`) — the cooldown sink
/// needs it to key the router-level store. `error` is carried for later
/// recorders (cooldown/lockout classify it); the breaker sink uses only `success`.
pub struct AttemptOutcome<'a> {
    pub endpoint: &'a str,
    pub router: &'a str,
    pub success: bool,
    pub error: Option<&'a GatewayError>,
    /// Wall time for this attempt/phase, in ms. Its meaning depends on
    /// `phase` and the three meanings are NOT interchangeable: time-to-response
    /// for `Complete`, time-to-first-response for `StreamAcquired` (the same
    /// quantity as `Complete`'s), or generation time for `StreamCompleted`
    /// (a different quantity, comparable to neither). `PerformanceRecorder`
    /// reads `phase` to decide whether this value is a latency observation at
    /// all, rather than pooling unlike spans into one mean.
    pub duration_ms: u64,
    /// Output tokens, when the attempt produced a countable response. `None`
    /// for a setup failure or a stream-acquisition dispatch.
    pub output_tokens: Option<u32>,
    /// What this outcome observes — see [`AttemptPhase`]. Read only by
    /// `PerformanceRecorder`.
    pub phase: AttemptPhase,
}

/// Reliable write-side reducer: updates authoritative health state from an attempt
/// outcome. NOT best-effort — distinct from the future `SelectionObserver`.
pub trait HealthRecorder: Send + Sync {
    /// Update authoritative health state from an attempt outcome, and return the
    /// `Instant` until which this recorder now considers the endpoint unavailable
    /// **if this outcome just made it so** (breaker next_retry / cooldown until /
    /// timed lock deadline); `None` otherwise. The engine mins these into
    /// `AllGated.resume_after` (design C4). Reliable write-side — NOT best-effort.
    fn on_outcome(&self, outcome: &AttemptOutcome<'_>) -> Option<std::time::Instant>;
}

#[cfg(test)]
mod tests {
    use super::*;
    struct AlwaysSkip;
    impl AdmissionGate for AlwaysSkip {
        fn name(&self) -> &'static str {
            "always_skip"
        }
        fn evaluate(&self, _c: &CandidateView<'_>, _x: &SelectionCtx<'_>) -> GateVerdict {
            GateVerdict::Skip(crate::skip_reason::SkipReason::RouterDisabled)
        }
    }
    #[test]
    fn gate_can_skip() {
        let g = AlwaysSkip;
        assert_eq!(g.name(), "always_skip");
    }
}
