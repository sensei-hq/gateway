pub mod anthropic;
pub mod async_job;
pub mod base;
pub mod bedrock;
pub mod capability;
pub mod fal;
pub mod flux;
pub mod gemini;
pub mod grok;
pub mod kling;
pub mod luma;
pub mod noop;
pub mod ollama;
pub mod openai;
pub mod recraft;
pub mod replicate;
pub mod runway;
pub mod stability;
pub mod together;

pub use capability::{ChatModel, EmbedModel, ImageModel, Model, SttModel, TtsModel, VideoModel};

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use tokio::sync::RwLock;

use crate::types::capability::Capability;
use crate::types::config::RouterConfig;
use crate::types::error::GatewayError;
use crate::types::request::{InferenceRequest, InferenceResponse, StreamChunk};

/// Abstraction over an LLM inference provider (Anthropic, OpenAI, Ollama, etc.).
///
/// Each adapter translates the gateway's unified request types into the
/// provider-specific wire format and back again.
#[async_trait]
pub trait InferenceAdapter: Send + Sync {
    /// Unique identifier for this adapter (e.g. "anthropic", "openai", "noop").
    fn id(&self) -> &str;

    /// Whether this adapter can handle the given capability.
    fn supports(&self, capability: &Capability) -> bool;

    /// Execute a single inference request and return the full response.
    async fn execute(
        &self,
        config: &RouterConfig,
        request: &InferenceRequest,
    ) -> Result<InferenceResponse, GatewayError>;

    /// Execute a streaming inference request, returning a stream of chunks.
    async fn stream(
        &self,
        config: &RouterConfig,
        request: &InferenceRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>;
}

/// Thread-safe registry of inference adapters keyed by their id.
#[derive(Clone)]
pub struct AdapterRegistry {
    adapters: Arc<RwLock<HashMap<String, Arc<dyn InferenceAdapter>>>>,
}

impl AdapterRegistry {
    pub fn new() -> Self {
        Self {
            adapters: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register an adapter. Overwrites any existing adapter with the same id.
    pub async fn register(&self, adapter: Arc<dyn InferenceAdapter>) {
        let id = adapter.id().to_string();
        self.adapters.write().await.insert(id, adapter);
    }

    /// Look up an adapter by id.
    pub async fn get(&self, id: &str) -> Option<Arc<dyn InferenceAdapter>> {
        self.adapters.read().await.get(id).cloned()
    }

    /// Return a sorted list of all registered adapter ids.
    pub async fn list(&self) -> Vec<String> {
        let guard = self.adapters.read().await;
        let mut ids: Vec<String> = guard.keys().cloned().collect();
        ids.sort();
        ids
    }

    /// Remove an adapter by id. Returns `true` if it existed.
    pub async fn unregister(&self, id: &str) -> bool {
        self.adapters.write().await.remove(id).is_some()
    }
}

impl Default for AdapterRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Capability-segregated registry (target model — see
// docs/design/adapter-capability-traits.md). Additive during the migration;
// the engine switches onto it and the legacy `AdapterRegistry` above is
// deleted in Phase 4, at which point this is renamed `AdapterRegistry`.
// ---------------------------------------------------------------------------

/// Registry with one map per capability. The same concrete `Arc` is registered
/// into each map it qualifies for (a concrete `Arc` coerces to each `dyn
/// *Model` independently), so a chat+embed adapter lives in both maps.
#[derive(Clone, Default)]
pub struct CapabilityRegistry {
    chat: Arc<RwLock<HashMap<String, Arc<dyn ChatModel>>>>,
    embed: Arc<RwLock<HashMap<String, Arc<dyn EmbedModel>>>>,
    stt: Arc<RwLock<HashMap<String, Arc<dyn SttModel>>>>,
    tts: Arc<RwLock<HashMap<String, Arc<dyn TtsModel>>>>,
    image: Arc<RwLock<HashMap<String, Arc<dyn ImageModel>>>>,
    video: Arc<RwLock<HashMap<String, Arc<dyn VideoModel>>>>,
}

macro_rules! capability_map_accessors {
    ($field:ident, $reg:ident, $get:ident, $trait:ident) => {
        /// Register an adapter under this capability (overwrites same id).
        pub async fn $reg(&self, a: Arc<dyn $trait>) {
            self.$field.write().await.insert(a.id().to_string(), a);
        }
        /// Look up an adapter for this capability by id.
        pub async fn $get(&self, id: &str) -> Option<Arc<dyn $trait>> {
            self.$field.read().await.get(id).cloned()
        }
    };
}

impl CapabilityRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    capability_map_accessors!(chat, register_chat, chat, ChatModel);
    capability_map_accessors!(embed, register_embed, embed, EmbedModel);
    capability_map_accessors!(stt, register_stt, stt, SttModel);
    capability_map_accessors!(tts, register_tts, tts, TtsModel);
    capability_map_accessors!(image, register_image, image, ImageModel);
    capability_map_accessors!(video, register_video, video, VideoModel);
}

/// One-call registration: an adapter inserts itself into every capability map
/// it implements. Consumers call
/// `Arc::new(MyAdapter::new()?).register_into(&reg).await` instead of calling
/// each `register_<cap>` by hand.
#[async_trait]
pub trait RegisterInto: Send + Sync {
    async fn register_into(self: Arc<Self>, reg: &CapabilityRegistry);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::noop::NoopAdapter;

    #[tokio::test]
    async fn same_adapter_registers_into_multiple_capability_maps() {
        use crate::types::config::RouterConfig;
        use crate::types::io::{ChatRequest, ChatResponse, EmbedRequest, EmbedResponse};

        struct Dual;
        impl Model for Dual {
            fn id(&self) -> &str {
                "dual"
            }
        }
        #[async_trait]
        impl ChatModel for Dual {
            async fn chat(
                &self,
                _c: &RouterConfig,
                _r: &ChatRequest,
            ) -> Result<ChatResponse, GatewayError> {
                Ok(ChatResponse::default())
            }
        }
        #[async_trait]
        impl EmbedModel for Dual {
            async fn embed(
                &self,
                _c: &RouterConfig,
                _r: &EmbedRequest,
            ) -> Result<EmbedResponse, GatewayError> {
                Ok(EmbedResponse::default())
            }
        }
        #[async_trait]
        impl RegisterInto for Dual {
            async fn register_into(self: Arc<Self>, reg: &CapabilityRegistry) {
                reg.register_chat(self.clone()).await;
                reg.register_embed(self).await;
            }
        }

        // Explicit per-capability registration: same Arc into both maps.
        let reg = CapabilityRegistry::new();
        let dual = Arc::new(Dual);
        reg.register_chat(dual.clone()).await;
        reg.register_embed(dual).await;
        assert!(reg.chat("dual").await.is_some());
        assert!(reg.embed("dual").await.is_some());
        assert!(reg.image("dual").await.is_none());

        // One-call RegisterInto lands the adapter in exactly its maps.
        let reg2 = CapabilityRegistry::new();
        Arc::new(Dual).register_into(&reg2).await;
        assert!(reg2.chat("dual").await.is_some());
        assert!(reg2.embed("dual").await.is_some());
        assert!(reg2.stt("dual").await.is_none());
    }

    #[tokio::test]
    async fn registry_register_and_get() {
        let registry = AdapterRegistry::new();
        let adapter: Arc<dyn InferenceAdapter> = Arc::new(NoopAdapter);

        registry.register(adapter).await;

        let found = registry.get("noop").await;
        assert!(found.is_some());
        assert_eq!(found.unwrap().id(), "noop");

        let missing = registry.get("nonexistent").await;
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn registry_list() {
        let registry = AdapterRegistry::new();
        registry.register(Arc::new(NoopAdapter)).await;

        let ids = registry.list().await;
        assert_eq!(ids, vec!["noop".to_string()]);
    }

    #[tokio::test]
    async fn registry_unregister() {
        let registry = AdapterRegistry::new();
        registry.register(Arc::new(NoopAdapter)).await;

        assert!(registry.unregister("noop").await);
        assert!(!registry.unregister("noop").await);
        assert!(registry.get("noop").await.is_none());
    }
}
