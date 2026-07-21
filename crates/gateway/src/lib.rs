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
// Model-registry vocabulary (`ModelEntry`/`ModelSource`/`ModelFormat`/`ModelResolver`/
// `ResolveError`) surfaced so a consumer that only depends on `sensei-gateway` can
// still name the types resolvers produce and adapters consume.
pub use kernel::registry;

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

/// Local engine surface (feature `local` + the `local-*` engine pass-throughs),
/// re-exported so a consumer can depend on ONLY `sensei-gateway` and still build,
/// resolve, pull, and coldboot local models — no direct `local-engine` /
/// `local-providers` dependency.
#[cfg(feature = "local")]
pub mod local {
    // Provisioning supervisor + streaming handle.
    pub use local_engine::{
        EnsureOpts, ProvisionError, ProvisionHandle, ProvisionPlan, ProvisioningSupervisor,
        ScriptedPlan,
    };
    // Resolvers (Managed / Ollama / External / Chained).
    pub use local_engine::registry::{
        ChainedResolver, ExternalResolver, ManagedResolver, OllamaResolver,
    };
    // Hugging Face pull (feature `local-hf-download`).
    #[cfg(feature = "local-hf-download")]
    pub use local_engine::registry::{
        FitReport, HfHubPuller, ModelPuller, PullError, PullSpec, PullingResolver,
    };
    // In-process engine adapters (features `local-llama-cpp` / `-fastembed` / `-ort`).
    #[cfg(feature = "local-llama-cpp")]
    pub use local_engine::{EmbeddedLlamaAdapter, LlamaCppAdapter, LlamaCppConfig, LlamaCppMode};
    #[cfg(feature = "local-fastembed")]
    pub use local_engine::{FastembedAdapter, FastembedConfig};
    #[cfg(feature = "local-ort")]
    pub use local_engine::{OrtAdapter, OrtConfig, OrtPoolingStrategy};
}
