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
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Capability-segregated registry (see docs/design/adapter-capability-traits.md).
// One `dyn` object cannot be several traits at once, so storage is one map per
// capability. `supports(cap)` is structural: membership in the capability's map.
// ---------------------------------------------------------------------------

/// Registry with one map per capability. The same concrete `Arc` is registered
/// into each map it qualifies for (a concrete `Arc` coerces to each `dyn
/// *Model` independently), so a chat+embed adapter lives in both maps.
#[derive(Clone, Default)]
pub struct AdapterRegistry {
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

impl AdapterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    capability_map_accessors!(chat, register_chat, chat, ChatModel);
    capability_map_accessors!(embed, register_embed, embed, EmbedModel);
    capability_map_accessors!(stt, register_stt, stt, SttModel);
    capability_map_accessors!(tts, register_tts, tts, TtsModel);
    capability_map_accessors!(image, register_image, image, ImageModel);
    capability_map_accessors!(video, register_video, video, VideoModel);

    /// Sorted, de-duplicated union of adapter ids across every capability map.
    /// An adapter registered under several capabilities appears once.
    pub async fn list(&self) -> Vec<String> {
        let mut ids: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        ids.extend(self.chat.read().await.keys().cloned());
        ids.extend(self.embed.read().await.keys().cloned());
        ids.extend(self.stt.read().await.keys().cloned());
        ids.extend(self.tts.read().await.keys().cloned());
        ids.extend(self.image.read().await.keys().cloned());
        ids.extend(self.video.read().await.keys().cloned());
        ids.into_iter().collect()
    }
}

/// One-call registration: an adapter inserts itself into every capability map
/// it implements. Consumers call
/// `Arc::new(MyAdapter::new()?).register_into(&reg).await` instead of calling
/// each `register_<cap>` by hand.
#[async_trait]
pub trait RegisterInto: Send + Sync {
    async fn register_into(self: Arc<Self>, reg: &AdapterRegistry);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::noop::NoopAdapter;

    #[tokio::test]
    async fn same_adapter_registers_into_multiple_capability_maps() {
        use crate::types::config::RouterConfig;
        use crate::types::error::GatewayError;
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
            async fn register_into(self: Arc<Self>, reg: &AdapterRegistry) {
                reg.register_chat(self.clone()).await;
                reg.register_embed(self).await;
            }
        }

        // Explicit per-capability registration: same Arc into both maps.
        let reg = AdapterRegistry::new();
        let dual = Arc::new(Dual);
        reg.register_chat(dual.clone()).await;
        reg.register_embed(dual).await;
        assert!(reg.chat("dual").await.is_some());
        assert!(reg.embed("dual").await.is_some());
        assert!(reg.image("dual").await.is_none());

        // One-call RegisterInto lands the adapter in exactly its maps.
        let reg2 = AdapterRegistry::new();
        Arc::new(Dual).register_into(&reg2).await;
        assert!(reg2.chat("dual").await.is_some());
        assert!(reg2.embed("dual").await.is_some());
        assert!(reg2.stt("dual").await.is_none());
    }

    #[tokio::test]
    async fn registry_registers_and_lists_by_capability() {
        // NoopAdapter registers into every capability map via `RegisterInto`.
        let reg = AdapterRegistry::new();
        Arc::new(NoopAdapter).register_into(&reg).await;

        // Structural lookup: present under a capability, absent for unknown id.
        assert!(reg.chat("noop").await.is_some());
        assert!(reg.chat("nonexistent").await.is_none());

        // `list()` is the sorted, de-duplicated union across all maps.
        assert_eq!(reg.list().await, vec!["noop".to_string()]);
    }
}
