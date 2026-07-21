//! `sensei-local-engine` — the local model engine: resolvers that map a stable
//! model id to on-disk bytes (managed / Ollama / external, composed via
//! `ChainedResolver`) plus Hugging Face pull (`hf-download`). Model vocabulary
//! (`ModelEntry`/`ModelResolver`/…) lives in `kernel::registry`.
pub mod registry;
pub mod supervisor;

pub use supervisor::{
    EnsureOpts, ProvisionError, ProvisionHandle, ProvisionPlan, ProvisioningSupervisor,
    ScriptedPlan,
};

// In-process inference adapters live in `local-providers`; re-export the ones the
// supervisor coldboots so a consumer reaching the engine through `sensei-gateway`'s
// `local` wing can name them without depending on `local-providers` directly.
#[cfg(feature = "llama-cpp")]
pub use local_providers::adapters::{
    EmbeddedLlamaAdapter, LlamaCppAdapter, LlamaCppConfig, LlamaCppMode,
};
#[cfg(feature = "fastembed")]
pub use local_providers::adapters::{FastembedAdapter, FastembedConfig};
#[cfg(feature = "ort")]
pub use local_providers::adapters::{OrtAdapter, OrtConfig, OrtPoolingStrategy};
