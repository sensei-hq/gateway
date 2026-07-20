//! Multiplexing in-process adapter (#79).
//!
//! Where [`LlamaCppAdapter`] holds exactly one model in one mode, this adapter
//! presents the embedded llama.cpp runtime as a **single router**
//! (`embedded-llama`) that serves many models across capabilities — the same
//! way the `ollama` router does. It owns a lazily-populated map of per-model
//! [`LlamaCppAdapter`] workers and dispatches each request to the right one.
//!
//! - **Which model**: taken from `request.model` (the gateway engine injects
//!   the chain entry's resolved model id here).
//! - **Which mode**: derived from the payload — `Payload::Embed` → an
//!   embedding context, `Payload::Chat` → a generation context. The two need
//!   distinct llama.cpp contexts (`with_embeddings`+pooling vs causal), so the
//!   worker cache is keyed by `(model_id, mode)`. Model *weights* are still
//!   shared process-wide via [`LlamaCppAdapter`]'s `cached_model`, so a second
//!   mode of the same GGUF is a context build, not a re-read of the file.
//! - **Where the bytes are**: resolved through a [`ModelResolver`]
//!   (Managed → Ollama → External), so a model already pulled by Ollama is
//!   reused in place and nothing has to be shipped with the binary.

use crate::adapters::llama_cpp::{LlamaCppAdapter, LlamaCppConfig, shared_backend};
use kernel::registry::ModelResolver;
use async_trait::async_trait;
use futures::Stream;
use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;
use kernel::types::io::{ChatRequest, ChatResponse, EmbedRequest, EmbedResponse};
use kernel::types::request::StreamChunk;
use llama_cpp_2::llama_backend::LlamaBackend;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

/// The llama.cpp context configuration a request implies. Part of the worker
/// cache key because embedding and generation contexts are not interchangeable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum WorkerMode {
    Embedding,
    Generation,
}

/// Build the per-model worker config for a given mode. `n_ctx`/pooling/etc.
/// come from [`LlamaCppConfig`]'s mode-specific builders.
fn worker_config(model_id: &str, mode: WorkerMode) -> LlamaCppConfig {
    match mode {
        WorkerMode::Embedding => LlamaCppConfig::embed(model_id),
        WorkerMode::Generation => LlamaCppConfig::chat(model_id),
    }
}

/// Single-router, multi-model embedded adapter. See module docs.
pub struct EmbeddedLlamaAdapter {
    adapter_id: String,
    backend: Arc<LlamaBackend>,
    resolver: Arc<dyn ModelResolver>,
    /// Lazily-built per-(model, mode) workers. An async mutex because lookups
    /// happen inside `chat`/`embed`/`chat_stream`; never held across the
    /// blocking model load (that runs on a `spawn_blocking` thread).
    workers: tokio::sync::Mutex<HashMap<(String, WorkerMode), Arc<LlamaCppAdapter>>>,
}

impl EmbeddedLlamaAdapter {
    /// Construct with an explicit backend. Most callers should prefer
    /// [`Self::with_shared_backend`].
    pub fn new(
        adapter_id: impl Into<String>,
        backend: Arc<LlamaBackend>,
        resolver: Arc<dyn ModelResolver>,
    ) -> Self {
        Self {
            adapter_id: adapter_id.into(),
            backend,
            resolver,
            workers: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Construct using the process-wide [`shared_backend`]. Fails only if the
    /// llama.cpp backend can't initialise.
    pub fn with_shared_backend(
        adapter_id: impl Into<String>,
        resolver: Arc<dyn ModelResolver>,
    ) -> Result<Self, GatewayError> {
        Ok(Self::new(adapter_id, shared_backend()?, resolver))
    }

    fn err(&self, message: impl Into<String>) -> GatewayError {
        GatewayError::ProviderError {
            adapter: self.adapter_id.clone(),
            message: message.into(),
            status: None,
        }
    }

    /// Get (or lazily build) the worker for a `(model_id, mode)` pair.
    ///
    /// On a cache miss the model bytes are resolved via the [`ModelResolver`]
    /// and the worker is loaded on a blocking thread (native model load). The
    /// async mutex is released around that load, so concurrent first-requests
    /// for *different* models don't serialise; two racing first-requests for
    /// the *same* key may both load, but the second insert is deduped (and the
    /// shared `cached_model` weight cache makes the duplicate cheap).
    async fn worker_for(
        &self,
        model_id: &str,
        mode: WorkerMode,
    ) -> Result<Arc<LlamaCppAdapter>, GatewayError> {
        let key = (model_id.to_string(), mode);
        {
            let workers = self.workers.lock().await;
            if let Some(w) = workers.get(&key) {
                return Ok(w.clone());
            }
        }

        let entry = self
            .resolver
            .resolve(model_id)
            .await
            .map_err(|e| self.err(format!("resolve '{model_id}': {e}")))?
            .ok_or_else(|| GatewayError::ModelUnavailable {
                adapter: self.adapter_id.clone(),
                model: model_id.to_string(),
            })?;

        let backend = self.backend.clone();
        let cfg = worker_config(model_id, mode);
        let adapter_id = self.adapter_id.clone();
        let loaded =
            tokio::task::spawn_blocking(move || LlamaCppAdapter::load(backend, &entry, cfg))
                .await
                .map_err(|e| self.err(format!("worker load join: {e}")))?
                .map_err(|e| self.err(format!("load '{model_id}': {e}")))?;
        let _ = adapter_id;
        let worker = Arc::new(loaded);

        let mut workers = self.workers.lock().await;
        // Re-check: a concurrent caller may have inserted while we loaded.
        if let Some(w) = workers.get(&key) {
            return Ok(w.clone());
        }
        workers.insert(key, worker.clone());
        Ok(worker)
    }
}

// ---------------------------------------------------------------------------
// Capability traits. The non-streaming paths call each worker's trait-free
// `generate` / `embed`; the stream path delegates to the worker's own
// `ChatModel::chat_stream`.
// ---------------------------------------------------------------------------

impl kernel::adapters::capability::Model for EmbeddedLlamaAdapter {
    fn id(&self) -> &str {
        &self.adapter_id
    }
}

#[async_trait]
impl kernel::adapters::capability::ChatModel for EmbeddedLlamaAdapter {
    async fn chat(
        &self,
        _config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        let model_id = req.model.as_deref().ok_or_else(|| {
            self.err("embedded-llama requires request.model (the model id to load)")
        })?;
        let worker = self.worker_for(model_id, WorkerMode::Generation).await?;
        let content = worker.generate(
            &req.messages,
            req.system.as_deref(),
            req.max_tokens,
            req.temperature,
        )?;
        Ok(ChatResponse {
            content: Some(content),
            tool_calls: Vec::new(),
            usage: None,
            model: Some(model_id.to_string()),
            degraded: false,
        })
    }

    async fn chat_stream(
        &self,
        config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        let model_id = req.model.as_deref().ok_or_else(|| {
            self.err("embedded-llama requires request.model (the model id to load)")
        })?;
        let worker = self.worker_for(model_id, WorkerMode::Generation).await?;
        // Delegate to the worker's typed chat_stream. `req.model` matches the
        // worker's configured model_id (both derive from `model_id`), so the
        // worker's model-mismatch guard passes.
        worker.chat_stream(config, req).await
    }
}

#[async_trait]
impl kernel::adapters::capability::EmbedModel for EmbeddedLlamaAdapter {
    async fn embed(
        &self,
        _config: &RouterConfig,
        req: &EmbedRequest,
    ) -> Result<EmbedResponse, GatewayError> {
        let model_id = req.model.as_deref().ok_or_else(|| {
            self.err("embedded-llama requires request.model (the model id to load)")
        })?;
        let worker = self.worker_for(model_id, WorkerMode::Embedding).await?;
        let embeddings = worker.embed(&req.texts)?;
        Ok(EmbedResponse {
            embeddings,
            usage: None,
            degraded: false,
        })
    }
}

#[async_trait]
impl kernel::adapters::RegisterInto for EmbeddedLlamaAdapter {
    async fn register_into(self: Arc<Self>, reg: &kernel::adapters::AdapterRegistry) {
        reg.register_chat(self.clone()).await;
        reg.register_embed(self).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kernel::registry::{ModelEntry, ModelFormat, ModelSource};
    use kernel::types::request::{Message, MessageRole};
    // ExternalResolver is a concrete resolver from the local engine; see the
    // local-engine dev-dependency in Cargo.toml (Amendment A of the split plan).
    use local_engine::registry::ExternalResolver;

    #[test]
    fn worker_config_picks_mode_specific_defaults() {
        let embed = worker_config("all-minilm", WorkerMode::Embedding);
        assert_eq!(embed.model_id, "all-minilm");
        assert_eq!(embed.n_ctx, 512);
        let chat = worker_config("gemma2:2b", WorkerMode::Generation);
        assert_eq!(chat.model_id, "gemma2:2b");
        assert_eq!(chat.n_ctx, 4096);
    }

    fn ext_entry(id: &str, path: &str) -> ModelEntry {
        ModelEntry {
            id: id.into(),
            name: id.into(),
            format: ModelFormat::Gguf,
            source: ModelSource::External { path: path.into() },
            sha256: None,
            size_bytes: None,
        }
    }

    /// Unknown model id surfaces as `ModelUnavailable`, not a panic. Uses an
    /// empty resolver so no model file is needed.
    #[tokio::test]
    async fn chat_unknown_model_is_model_unavailable() {
        use kernel::adapters::capability::ChatModel;
        let resolver = Arc::new(ExternalResolver::new());
        // No real backend needed: resolution fails before any load. We still
        // need a backend to construct, so this test is gated on llama init
        // succeeding — skip the assertion if the backend is unavailable in CI.
        let Ok(adapter) = EmbeddedLlamaAdapter::with_shared_backend("embedded-llama", resolver)
        else {
            return;
        };
        let req = kernel::types::io::ChatRequest {
            model: Some("nope".into()),
            messages: vec![Message::text(MessageRole::User, "hi")],
            system: None,
            max_tokens: Some(8),
            temperature: None,
            tools: Vec::new(),
        };
        let cfg = RouterConfig {
            url: "embedded://embedded-llama".into(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: std::collections::HashMap::new(),
        };
        let err = adapter.chat(&cfg, &req).await.unwrap_err();
        match err {
            GatewayError::ModelUnavailable { ref model, .. } => assert_eq!(model, "nope"),
            other => panic!("expected ModelUnavailable, got {other:?}"),
        }
    }

    /// End-to-end: one adapter serves a chat model AND an embed model,
    /// selecting by `request.model`. Ignored by default; run with both GGUFs:
    ///   LLAMA_TEST_CHAT_GGUF=… LLAMA_TEST_GGUF=… \
    ///     cargo test -p gateway-embedded --features llama-cpp \
    ///     one_adapter_serves_chat_and_embed -- --ignored
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires LLAMA_TEST_CHAT_GGUF + LLAMA_TEST_GGUF env vars"]
    async fn one_adapter_serves_chat_and_embed_by_model_id() {
        use kernel::adapters::capability::{ChatModel, EmbedModel};
        let chat = std::env::var("LLAMA_TEST_CHAT_GGUF").expect("LLAMA_TEST_CHAT_GGUF");
        let embed = std::env::var("LLAMA_TEST_GGUF").expect("LLAMA_TEST_GGUF");

        let resolver = ExternalResolver::new();
        resolver.register(ext_entry("chat-model", &chat)).await;
        resolver.register(ext_entry("embed-model", &embed)).await;

        let adapter =
            EmbeddedLlamaAdapter::with_shared_backend("embedded-llama", Arc::new(resolver))
                .expect("backend");
        let cfg = RouterConfig {
            url: "embedded://embedded-llama".into(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: std::collections::HashMap::new(),
        };

        // Chat through the chat model.
        let chat_req = kernel::types::io::ChatRequest {
            model: Some("chat-model".into()),
            messages: vec![Message::text(MessageRole::User, "Reply: pong")],
            system: None,
            max_tokens: Some(16),
            temperature: Some(0.0),
            tools: Vec::new(),
        };
        let chat_resp = adapter.chat(&cfg, &chat_req).await.expect("chat");
        assert!(chat_resp.content.is_some());

        // Embed through the embed model — same adapter instance.
        let embed_req = kernel::types::io::EmbedRequest {
            model: Some("embed-model".into()),
            texts: vec!["hello world".into()],
        };
        let embed_resp = adapter.embed(&cfg, &embed_req).await.expect("embed");
        let embs = embed_resp.embeddings;
        assert_eq!(embs.len(), 1);
        assert!(!embs[0].is_empty());
    }
}
