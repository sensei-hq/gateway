//! Readiness vocabulary + the `ReadinessProbe` port. The routing engine
//! (`gateway`) consults readiness through this trait object; the local engine
//! (`local-engine`) implements it on its `ProvisioningSupervisor`. Compile-time
//! dependency points at `kernel` only; the runtime call is `dyn` dispatch.
use serde::{Deserialize, Serialize};

/// The lifecycle of a local model's provisioning, newest-value semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum ProvisionPhase {
    Absent,
    Queued,
    Downloading { done: u64, total: Option<u64> },
    Verifying,
    Loading,
    Ready,
    Failed { error: String },
}

impl ProvisionPhase {
    /// True while a job is running (Queued / Downloading / Verifying / Loading).
    /// Ready / Absent / Failed are terminal-or-idle.
    pub fn is_in_flight(&self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Downloading { .. } | Self::Verifying | Self::Loading
        )
    }
}

/// A phase transition for one model — what a consumer relays to a progress UI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvisionEvent {
    pub model: String,
    pub phase: ProvisionPhase,
}

/// Port the routing engine consults for a model's readiness. Implemented by the
/// local engine's supervisor; consumed by `gateway` via `Arc<dyn ReadinessProbe>`.
#[async_trait::async_trait]
pub trait ReadinessProbe: Send + Sync {
    async fn phase(&self, model: &str) -> ProvisionPhase;
    async fn status_all(&self) -> Vec<(String, ProvisionPhase)>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_in_flight_is_true_for_queued_download_verify_load_only() {
        assert!(ProvisionPhase::Queued.is_in_flight());
        assert!(ProvisionPhase::Downloading { done: 1, total: Some(10) }.is_in_flight());
        assert!(ProvisionPhase::Verifying.is_in_flight());
        assert!(ProvisionPhase::Loading.is_in_flight());
        assert!(!ProvisionPhase::Absent.is_in_flight());
        assert!(!ProvisionPhase::Ready.is_in_flight());
        assert!(!ProvisionPhase::Failed { error: "x".into() }.is_in_flight());
    }

    #[test]
    fn phase_roundtrips_through_json() {
        let p = ProvisionPhase::Downloading { done: 5, total: Some(100) };
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(serde_json::from_str::<ProvisionPhase>(&json).unwrap(), p);
    }

    struct FakeProbe;
    #[async_trait::async_trait]
    impl ReadinessProbe for FakeProbe {
        async fn phase(&self, _m: &str) -> ProvisionPhase {
            ProvisionPhase::Ready
        }
        async fn status_all(&self) -> Vec<(String, ProvisionPhase)> {
            vec![]
        }
    }

    #[tokio::test]
    async fn readiness_probe_is_object_safe_and_callable_via_dyn() {
        let probe: std::sync::Arc<dyn ReadinessProbe> = std::sync::Arc::new(FakeProbe);
        assert_eq!(probe.phase("m").await, ProvisionPhase::Ready);
    }
}
