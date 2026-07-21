pub mod adapters;
pub mod budget;
pub mod circuit_breaker;
pub mod config;
mod dispatch;
pub mod engine;
pub mod facade;
pub mod pruning;
pub mod purpose;
pub mod selection;
pub mod store;
pub use kernel::types;

pub use config::GatewayBuilder;
pub use engine::Gateway;
pub use facade::{Facade, FacadeBuilder};
pub use pruning::{Availability, ChainWarning, prune_unavailable};
pub use purpose::{ModelHint, Purpose, PurposeBuilder, PurposeResult, StepBuilder, StepInput};
pub use types::capability::Capability;
pub use types::error::GatewayError;
pub use types::request::{InferenceRequest, InferenceResponse};

// Readiness vocabulary (owned by `kernel`) surfaced here so clients touch only
// `gateway` — `with_readiness` takes a `ReadinessProbe`, and consumers relay
// `ProvisionEvent`/`ProvisionPhase` onto their own status surface.
pub use kernel::{ProvisionEvent, ProvisionPhase, ReadinessProbe};

/// Local provisioning-engine surface (feature `local`), re-exported so clients
/// wire the supervisor without depending on `local-engine` directly.
#[cfg(feature = "local")]
pub mod local {
    pub use local_engine::{
        EnsureOpts, ProvisionError, ProvisionHandle, ProvisionPlan, ProvisioningSupervisor,
        ScriptedPlan,
    };
}
