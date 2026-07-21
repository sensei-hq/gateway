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
use gateway::types::trace::Attempt;
use gateway::{Capability, InferenceRequest, InferenceResponse};

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
