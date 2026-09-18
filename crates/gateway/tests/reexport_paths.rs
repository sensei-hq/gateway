//! Compile-time guard: every `gateway::…` path that downstream consumers
//! depend on must keep resolving after the kernel split.
//! Compiling this file IS the assertion.
#![allow(unused_imports)]

use gateway::adapters::capability::{
    ChatModel, EmbedModel, ImageModel, Model, SttModel, TtsModel, VideoModel,
};
use gateway::adapters::{AdapterRegistry, RegisterInto};
use gateway::types::config::RouterConfig;
use gateway::types::cost::{Cost, CostEstimate, TokenUsage};
use gateway::types::error::GatewayError;
use gateway::types::io::{ChatRequest, ChatResponse, EmbedRequest, EmbedResponse};
use gateway::types::request::{Message, MessageRole, StreamChunk};
// SP-ROUTE-1's request surface, reached the same way `Message`/`StreamChunk`
// are — through `types::request`, NOT through a new crate-root re-export.
// `docs/llms/recipes.md` documents exactly these paths, so a rename that broke
// them would otherwise only be caught by a reader.
use gateway::types::request::{CandidateRef, CandidateSet, RoutingPreferences, SortKey};
use gateway::types::trace::{Attempt, RoutedCandidate, RoutingDecision};
use gateway::{Capability, InferenceRequest, InferenceResponse};

// SP-ROUTE-1's operator knob, reached as `docs/llms/configuration.md` spells it.
use gateway::resilience::ResilienceConfig;

#[cfg(feature = "cloud")]
#[allow(unused_imports)]
use gateway::adapters::{
    anthropic::AnthropicAdapter, bedrock::BedrockAdapter, openai::OpenAIAdapter,
};

// Model-registry vocabulary via the facade (no direct `kernel` dependency needed).
use gateway::registry::{ModelEntry, ModelFormat, ModelResolver, ModelSource, ResolveError};

// The local-engine surface, proving a consumer can reach it through `sensei-gateway`
// alone (feature `local` + the `local-*` engine pass-throughs).
#[cfg(feature = "local")]
use gateway::local::{
    ChainedResolver, EnsureOpts, ExternalResolver, ManagedResolver, OllamaResolver,
    ProvisionHandle, ProvisionPlan, ProvisioningSupervisor, ScriptedPlan,
};
#[cfg(feature = "local-llama-cpp")]
use gateway::local::{EmbeddedLlamaAdapter, LlamaCppAdapter, LlamaCppConfig, LlamaCppMode};
#[cfg(feature = "local-fastembed")]
use gateway::local::{FastembedAdapter, FastembedConfig};
#[cfg(feature = "local-hf-download")]
use gateway::local::{FitReport, HfHubPuller, ModelPuller, PullError, PullSpec, PullingResolver};
#[cfg(feature = "local-ort")]
use gateway::local::{OrtAdapter, OrtConfig, OrtPoolingStrategy};

#[test]
fn reexport_paths_resolve() {
    // The `use` block above proves the paths resolve; nothing to assert at runtime.
}

/// The documented way to set an operator knob, compiled from OUTSIDE the
/// defining crate — which is the only place the constraint is visible.
///
/// `ResilienceConfig` is `#[non_exhaustive]`, and that attribute forbids EVERY
/// struct expression downstream, **functional-update syntax included**. The
/// pattern three docs used to print —
/// `ResilienceConfig { min_samples: 5, ..Default::default() }` — does not
/// compile for any consumer:
///
/// Verified by writing it here and compiling, not assumed:
///
/// ```text
/// error[E0639]: cannot create non-exhaustive struct using struct expression
///    --> crates/gateway/tests/reexport_paths.rs
///     |
///     |       let cfg = ResilienceConfig {
///     |  _______________^
///     | |         min_samples: 5,
///     | |         ..Default::default()
///     | |     };
///     | |_____^
/// ```
///
/// Nothing caught that for a whole slice because the suite's only use of the
/// broken shape is INSIDE `sensei-gateway`, where `#[non_exhaustive]` does not
/// apply, and this file — the one external-crate compile surface — never named
/// the type. `default()` then field assignment is the shape that works, and
/// compiling it here is the assertion; `docs/llms/configuration.md`,
/// `docs/llms/upgrading.md` and `docs/features/routing/provider-preferences.md`
/// all print exactly this.
///
/// Note that `clippy::field_reassign_with_default` does NOT fire here — it
/// exempts `#[non_exhaustive]` types, because the struct literal it would
/// otherwise suggest is the form that cannot compile.
#[test]
fn non_exhaustive_config_is_built_the_way_the_docs_say() {
    let mut resilience = ResilienceConfig::default();
    resilience.min_samples = 5;
    assert_eq!(resilience.min_samples, 5);
}
