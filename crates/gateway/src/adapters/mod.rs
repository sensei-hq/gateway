pub mod anthropic;
pub mod async_job;
pub mod base;
pub mod bedrock;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::noop::NoopAdapter;

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
