use crate::skip_reason::SkipReason;
use crate::types::capability::Capability;
use crate::types::config::{GatewayConfig, ModelConfig, RouterConfig};
use crate::types::error::GatewayError;
use std::time::Instant;

pub mod budget;
pub mod capability;
pub mod circuit_breaker_gate;

/// Read port for endpoint health (the circuit breaker implements it in Task 4;
/// cooldown/lockout ports arrive in later SP-0 plans).
pub trait EndpointHealthRead: Send + Sync {
    /// `Some(until)` if the endpoint is currently open/unavailable with a retry time.
    fn open_until(&self, endpoint: &str) -> Option<Instant>;
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
    pub health: &'a dyn EndpointHealthRead,
    pub now: Instant,
    pub config: &'a GatewayConfig,
}

pub enum GateVerdict {
    Admit,
    Skip(SkipReason),
}

pub trait AdmissionGate: Send + Sync {
    fn name(&self) -> &'static str;
    fn evaluate(&self, cand: &CandidateView<'_>, ctx: &SelectionCtx<'_>) -> GateVerdict;
}

/// A single attempt's outcome, fed to the write-side recorders. `endpoint` is the
/// opaque "router:model" key (matches the read-side breaker keying). `error` is
/// carried for later recorders (cooldown/lockout classify it); the breaker sink
/// uses only `success`.
pub struct AttemptOutcome<'a> {
    pub endpoint: &'a str,
    pub success: bool,
    pub error: Option<&'a GatewayError>,
}

/// Reliable write-side reducer: updates authoritative health state from an attempt
/// outcome. NOT best-effort — distinct from the future `SelectionObserver`.
pub trait HealthRecorder: Send + Sync {
    fn on_outcome(&self, outcome: &AttemptOutcome<'_>);
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
