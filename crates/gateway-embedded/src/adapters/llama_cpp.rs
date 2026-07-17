//! In-process inference adapter built on [`llama_cpp_2`].
//!
//! Why this exists: calling a local Ollama daemon over HTTP for short
//! embedding queries spends 5–9x more time in protocol overhead than on
//! the actual inference (measured in `rust-embedding-bench`). Loading the
//! same GGUF in-process and calling [`llama_cpp_2`] directly recovers that
//! latency. This adapter is the same idea behind the `InferenceAdapter`
//! trait that the rest of the gateway already speaks.
//!
//! Design — one adapter holds one model and is configured at load time
//! into one of two modes:
//! - [`LlamaCppMode::Embedding`] supports [`Capability::TextEmbed`]
//!   via a single-shot encode + per-sequence pooled vector read.
//! - [`LlamaCppMode::Generation`] supports [`Capability::TextChat`] /
//!   [`Capability::TextComplete`] via an autoregressive decode loop with
//!   token sampling. Uses the model's bundled chat template to format
//!   messages.
//!
//! The adapter is loaded with a specific [`ModelEntry`]; requests whose
//! `model` field disagrees return [`GatewayError::ModelUnavailable`].
//! Streaming is not yet implemented — `stream()` returns an error.
//!
//! Concurrency model:
//! - The [`llama_cpp_2`] [`LlamaModel`] is read-only and shared.
//! - The [`LlamaContext`] carries mutable state (kv cache, scratch
//!   buffers) and is wired through a `std::sync::Mutex`. Concurrent
//!   `execute()` calls serialise on the mutex; the work is fully blocking
//!   on llama.cpp's native side anyway.
//!
//! Lifetime gymnastics: a [`LlamaContext<'b>`] holds an `&'b LlamaModel`
//! reference. To store both in one struct without a self-referential
//! crate, two invariants have to hold:
//!
//! 1. The [`LlamaModel`] must live at a fixed address that survives moves
//!    of the surrounding struct. Inner holds it in a [`Box`] so the
//!    reference inside [`LlamaContext`] points at a heap-stable address.
//!    A direct `model: LlamaModel` field would dangle the moment Inner is
//!    moved (e.g., into [`Arc::new`]). NRVO can hide that on Rust 1.x but
//!    is not a soundness guarantee — relying on it caused EXC_BAD_ACCESS
//!    in `llama_n_embd` once the adapter started living behind `Arc<Inner>`.
//! 2. The context's lifetime is extended to `'static` via
//!    [`std::mem::transmute`], and Rust's struct drop order (declaration
//!    order) guarantees the context drops before the model it borrows from.
//!    The [`Inner::context`] field is declared before [`Inner::model`]
//!    for that reason.

use crate::math::l2_normalize_in_place;
use crate::registry::{ModelEntry, ModelSource};
use async_trait::async_trait;
use futures::Stream;
use gateway::adapters::InferenceAdapter;
use gateway::types::capability::Capability;
use gateway::types::config::RouterConfig;
use gateway::types::error::GatewayError;
use gateway::types::io::{ChatRequest, ChatResponse, EmbedRequest, EmbedResponse};
use gateway::types::request::{
    InferenceRequest, InferenceResponse, Message, MessageRole, Payload, StreamChunk,
};
use llama_cpp_2::{
    context::{
        LlamaContext,
        params::{LlamaContextParams, LlamaPoolingType},
    },
    llama_backend::LlamaBackend,
    llama_batch::LlamaBatch,
    model::{AddBos, LlamaChatMessage, LlamaModel, params::LlamaModelParams},
    sampling::LlamaSampler,
    token::LlamaToken,
};
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

/// Discriminates between the two very different ways a llama.cpp context is
/// used. The choice is made at adapter load time because the context params
/// (`with_embeddings`, `with_pooling_type`) differ, and we cannot
/// re-purpose an embedding context for generation or vice versa.
#[derive(Debug, Clone)]
pub enum LlamaCppMode {
    /// Encode-only path used for BERT-class embedding models. Reads
    /// per-sequence pooled vectors after a single `encode()` call.
    Embedding { pooling: LlamaPoolingType },
    /// Autoregressive `decode()` loop with token sampling. Produces a
    /// text completion via the model's chat template.
    Generation {
        /// Cap on the number of new tokens per request when the caller
        /// doesn't specify `max_tokens`.
        default_max_tokens: u32,
        /// Temperature used when the caller doesn't specify one. `0.0`
        /// means greedy (always pick the highest-probability token).
        default_temperature: f32,
        /// Seed for the distribution sampler. The harness wants the same
        /// adapter to be reproducible across runs by default.
        seed: u32,
    },
}

/// Construction-time configuration for [`LlamaCppAdapter`].
#[derive(Debug, Clone)]
pub struct LlamaCppConfig {
    /// Adapter id surfaced to the gateway. Defaults to `"llama-cpp"`.
    pub adapter_id: String,
    /// Stable model id this adapter serves. Requests must specify this id
    /// (or send `model = None`).
    pub model_id: String,
    /// Embedding vs generation mode.
    pub mode: LlamaCppMode,
    /// Max sequence length the context will accept.
    pub n_ctx: u32,
    /// Threads used for both prompt processing and decode/encode.
    pub n_threads: i32,
    /// Maximum number of distinct sequences in a single batch. Embedding
    /// mode benefits from a generous value (concurrent batched embeds);
    /// generation typically uses 1.
    pub n_seq_max: u32,
}

impl LlamaCppConfig {
    /// Convenience builder for the embedding case — short context, mean
    /// pooling, room for up to 64 concurrent sequences in one batch.
    pub fn embed(model_id: impl Into<String>) -> Self {
        Self {
            adapter_id: "llama-cpp".into(),
            model_id: model_id.into(),
            mode: LlamaCppMode::Embedding {
                pooling: LlamaPoolingType::Mean,
            },
            n_ctx: 512,
            n_threads: 1,
            n_seq_max: 64,
        }
    }

    /// Convenience builder for the chat/generation case — 4k context,
    /// greedy decoding by default, single-sequence (no batched chat).
    pub fn chat(model_id: impl Into<String>) -> Self {
        Self {
            adapter_id: "llama-cpp".into(),
            model_id: model_id.into(),
            mode: LlamaCppMode::Generation {
                default_max_tokens: 512,
                default_temperature: 0.0,
                seed: 42,
            },
            n_ctx: 4096,
            n_threads: 1,
            n_seq_max: 1,
        }
    }
}

/// Newtype around [`LlamaContext`] that asserts `Send`.
///
/// llama-cpp-2 deliberately omits `Send`/`Sync` from [`LlamaContext`]
/// because it carries mutable native state. We restore `Send` here under
/// a stronger contract: the context is only ever accessed through the
/// surrounding `Mutex`, so at any moment exactly one thread is using it.
/// llama.cpp's own requirement is "don't share a context across threads
/// concurrently" — serialised use from different threads sequentially is
/// fine.
struct SyncContext(LlamaContext<'static>);

// SAFETY: see `SyncContext` doc — the surrounding `Mutex` ensures one
// thread at a time accesses the context, which is llama.cpp's actual
// safety invariant.
unsafe impl Send for SyncContext {}

/// In-process inference adapter backed by a single loaded GGUF model.
///
/// See module docs for the lifetime safety invariant — `context` must be
/// declared before `model` so it drops first.
/// Process-wide singleton [`LlamaBackend`].
///
/// `LlamaBackend::init()` is allowed to be called only once per
/// process; a second call returns `BackendAlreadyInitialized`. Callers
/// that need a backend (the test suite, sensei's `register_llama_cpp_*`
/// helpers, anything else that loads multiple LlamaCpp adapters) should
/// go through this function instead of calling `init()` directly so
/// they get the same `Arc<LlamaBackend>` back every time.
///
/// The first-call error is cached in the `OnceLock`, so subsequent
/// callers see the same error rather than blowing up on a misleading
/// "already initialized" message.
pub fn shared_backend() -> Result<Arc<LlamaBackend>, GatewayError> {
    use std::sync::OnceLock;
    static BACKEND: OnceLock<Result<Arc<LlamaBackend>, String>> = OnceLock::new();
    let cached = BACKEND.get_or_init(|| {
        LlamaBackend::init()
            .map(Arc::new)
            .map_err(|e| format!("LlamaBackend::init: {e}"))
    });
    cached.clone().map_err(|e| GatewayError::ProviderError {
        adapter: "llama-cpp".into(),
        message: e,
        status: None,
    })
}

/// Process-wide cache of loaded [`LlamaModel`] weights, keyed by the
/// canonicalised on-disk path. Entries are `Weak<LlamaModel>` so a
/// model that no [`LlamaCppAdapter`] is holding gets dropped and the
/// next [`cached_model`] call re-reads the file. Held in a
/// `RwLock` so the common path (cache hit) only takes a read lock.
fn model_cache() -> &'static std::sync::RwLock<
    std::collections::HashMap<std::path::PathBuf, std::sync::Weak<LlamaModel>>,
> {
    use std::sync::OnceLock;
    static CACHE: OnceLock<
        std::sync::RwLock<
            std::collections::HashMap<std::path::PathBuf, std::sync::Weak<LlamaModel>>,
        >,
    > = OnceLock::new();
    CACHE.get_or_init(|| std::sync::RwLock::new(std::collections::HashMap::new()))
}

/// Get an `Arc<LlamaModel>` for the GGUF at `path`, loading from disk
/// only on a cache miss.
///
/// The key is the path as given. Two callers that pass the same
/// `&Path` reuse the same model; symlinks or different relative paths
/// that point at the same file currently get separate cache entries.
/// `canonicalize` would be safer, but it requires the file to exist
/// at lookup time and would add an `std::fs` round-trip per call —
/// not worth it for the expected usage pattern (a handful of adapters
/// per process).
///
/// Loading is protected with the write-lock held so a thundering
/// herd doesn't race to read the same multi-GB file. A second caller
/// that arrives while the first is loading blocks on the write lock,
/// then sees the freshly-inserted entry.
fn cached_model(
    backend: &Arc<LlamaBackend>,
    path: &std::path::Path,
) -> Result<Arc<LlamaModel>, String> {
    // Cheap-path: a read lock + an upgrade. Returns immediately when
    // a live model is already cached.
    {
        let cache = model_cache()
            .read()
            .map_err(|e| format!("model cache poisoned: {e}"))?;
        if let Some(weak) = cache.get(path)
            && let Some(arc) = weak.upgrade()
        {
            return Ok(arc);
        }
    }
    // Slow path: take the write lock. Re-check inside the lock —
    // another thread may have populated the entry while we were
    // upgrading the lock.
    let mut cache = model_cache()
        .write()
        .map_err(|e| format!("model cache poisoned: {e}"))?;
    if let Some(weak) = cache.get(path)
        && let Some(arc) = weak.upgrade()
    {
        return Ok(arc);
    }
    let params = LlamaModelParams::default();
    let model = LlamaModel::load_from_file(backend.as_ref(), path, &params)
        .map_err(|e| format!("model load: {e}"))?;
    let arc = Arc::new(model);
    cache.insert(path.to_path_buf(), Arc::downgrade(&arc));
    Ok(arc)
}

/// Shared engine state for [`LlamaCppAdapter`]. Held behind an
/// [`Arc`] so streaming generation can clone it cheaply and hand the
/// clone to a `spawn_blocking` worker that produces tokens
/// incrementally. The non-streaming embed / generate paths still
/// access it via `self.inner` like before.
///
/// Field order is the load-bearing invariant for the LlamaContext
/// `'static` transmute trick — `context` must drop before `model`
/// and `_backend`. Don't reorder.
///
/// `model` is held in an [`Arc`] so two things hold simultaneously:
///
/// 1. The address `&*model` is heap-stable across moves of `Inner`
///    (Arc's inner T sits behind an indirection — the same property
///    a Box gives). The reference inside the [`LlamaContext`] points
///    at that heap address, so moves of the struct don't dangle it.
/// 2. Multiple [`LlamaCppAdapter`] instances for the same on-disk
///    GGUF can share the same loaded model via [`cached_model`] —
///    each new instance clones the Arc instead of re-loading the
///    file from disk. The KV-cache-carrying `LlamaContext` is still
///    per-adapter (each adapter calls `model.new_context(...)`
///    separately), so isolation between adapters is preserved.
struct Inner {
    context: Mutex<SyncContext>,
    model: Arc<LlamaModel>,
    _backend: Arc<LlamaBackend>,
}

pub struct LlamaCppAdapter {
    config: LlamaCppConfig,
    inner: Arc<Inner>,
}

impl LlamaCppAdapter {
    /// Load a GGUF model and build a context around it.
    ///
    /// The model weights are cached process-wide by [`cached_model`].
    /// Repeat calls for the same on-disk path reuse the loaded
    /// `LlamaModel` — a multi-GB disk read on first load becomes a
    /// pointer clone on subsequent loads. The cache only holds
    /// `Weak<LlamaModel>` entries, so a model with no live
    /// `LlamaCppAdapter` referencing it gets dropped and the next
    /// load re-reads the file.
    pub fn load(
        backend: Arc<LlamaBackend>,
        entry: &ModelEntry,
        config: LlamaCppConfig,
    ) -> Result<Self, GatewayError> {
        let path = entry.source.path();
        let model = cached_model(&backend, path)
            .map_err(|e| Self::provider_err(&config, format!("model load: {e}")))?;

        // n_batch sizes the per-batch token budget. n_ctx works for both
        // single-sequence chat and small-batch embedding.
        let n_batch = config.n_ctx;
        let (with_embeddings, pooling) = match config.mode {
            LlamaCppMode::Embedding { pooling } => (true, pooling),
            LlamaCppMode::Generation { .. } => (false, LlamaPoolingType::Unspecified),
        };
        let ctx_params = LlamaContextParams::default()
            .with_n_ctx(NonZeroU32::new(config.n_ctx))
            .with_n_batch(n_batch)
            .with_n_ubatch(n_batch)
            .with_n_seq_max(config.n_seq_max)
            .with_n_threads(config.n_threads)
            .with_n_threads_batch(config.n_threads)
            .with_embeddings(with_embeddings)
            .with_pooling_type(pooling);

        let context = model
            .new_context(backend.as_ref(), ctx_params)
            .map_err(|e| Self::provider_err(&config, format!("context create: {e}")))?;

        // SAFETY: `context` holds an `&LlamaModel` reference into the
        // heap allocation owned by the `Box<LlamaModel>` we built above.
        // That address is stable across moves of `Inner`. The struct
        // declares `context` before `model`/`_backend`, so on drop the
        // context is destroyed first and the model outlives every use
        // of it. Both invariants are documented at the top of this file.
        let context: LlamaContext<'static> = unsafe { std::mem::transmute(context) };

        let adapter = Self {
            config,
            inner: Arc::new(Inner {
                context: Mutex::new(SyncContext(context)),
                model,
                _backend: backend,
            }),
        };

        Ok(adapter)
    }

    /// Note about model source: we accept any [`ModelEntry`] regardless of
    /// `ModelSource` variant — the adapter just needs the on-disk path. The
    /// source kind matters at the registry layer, not at load time.
    pub fn check_source_supported(entry: &ModelEntry) -> Result<(), GatewayError> {
        if !entry.source.path().exists() {
            return Err(GatewayError::ProviderError {
                adapter: "llama-cpp".into(),
                message: format!(
                    "model bytes not found at {:?} (source: {:?})",
                    entry.source.path(),
                    match &entry.source {
                        ModelSource::Managed { .. } => "Managed",
                        ModelSource::Ollama { .. } => "Ollama",
                        ModelSource::External { .. } => "External",
                    }
                ),
                status: None,
            });
        }
        Ok(())
    }

    fn provider_err(config: &LlamaCppConfig, message: impl Into<String>) -> GatewayError {
        GatewayError::ProviderError {
            adapter: config.adapter_id.clone(),
            message: message.into(),
            status: None,
        }
    }

    fn err(&self, message: impl Into<String>) -> GatewayError {
        Self::provider_err(&self.config, message)
    }

    /// Public, trait-free embedding entry point — easier to use from tests
    /// or from sensei-internal callers that already have a borrow on the
    /// adapter and don't need the full [`InferenceRequest`] envelope.
    pub fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, GatewayError> {
        if !matches!(self.config.mode, LlamaCppMode::Embedding { .. }) {
            return Err(self.err("adapter not configured for embedding (mode != Embedding)"));
        }
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        if texts.len() as u32 > self.config.n_seq_max {
            return Err(self.err(format!(
                "batch of {} exceeds n_seq_max={}",
                texts.len(),
                self.config.n_seq_max
            )));
        }

        let mut guard = self
            .inner
            .context
            .lock()
            .map_err(|_| self.err("context mutex poisoned"))?;
        let ctx = &mut guard.0;

        let mut all_tokens: Vec<Vec<LlamaToken>> = Vec::with_capacity(texts.len());
        let mut total_tokens: usize = 0;
        for text in texts {
            let tokens = self
                .inner
                .model
                .str_to_token(text, AddBos::Always)
                .map_err(|e| self.err(format!("tokenize: {e}")))?;
            total_tokens += tokens.len();
            all_tokens.push(tokens);
        }

        // Match the rust-embedding-bench llama_runner verbatim: mark only
        // the last token of each sequence as an output. llama.cpp's
        // embedding override picks up the rest. Marking every token as
        // output was reproducibly returning empty embeddings on the first
        // post-load batch on macOS with Metal.
        let mut batch = LlamaBatch::new(total_tokens.max(1), texts.len() as i32);
        for (seq_id, tokens) in all_tokens.iter().enumerate() {
            let last = tokens.len().saturating_sub(1);
            for (pos, &token) in tokens.iter().enumerate() {
                batch
                    .add(token, pos as i32, &[seq_id as i32], pos == last)
                    .map_err(|e| self.err(format!("batch.add: {e}")))?;
            }
        }

        ctx.clear_kv_cache();
        ctx.encode(&mut batch)
            .map_err(|e| self.err(format!("encode: {e}")))?;

        let mut results = Vec::with_capacity(texts.len());
        for seq_id in 0..texts.len() {
            let emb = ctx
                .embeddings_seq_ith(seq_id as i32)
                .map_err(|e| self.err(format!("read embedding[{seq_id}]: {e}")))?;
            if emb.is_empty() {
                // Defensive: encode() succeeded but the pooling layer left
                // this sequence's slot empty. Empirically this happens
                // when callers exercise different batch shapes against the
                // same context — e.g. a batch=1 call followed by a batch=N
                // call — under Metal. Surface as an error so the caller
                // can choose to retry or rebuild the adapter.
                return Err(self.err(format!(
                    "empty embedding for seq {seq_id} (batch_size={}, model={})",
                    texts.len(),
                    self.config.model_id
                )));
            }
            let mut owned = emb.to_vec();
            l2_normalize_in_place(&mut owned);
            results.push(owned);
        }
        Ok(results)
    }

    /// Public, trait-free chat-generation entry point. Takes the same
    /// fields the gateway's [`Payload::Chat`] carries — messages, an
    /// optional system prompt, optional `max_tokens` / `temperature`
    /// overrides — and returns the generated text.
    pub fn generate(
        &self,
        messages: &[Message],
        system: Option<&str>,
        max_tokens: Option<u32>,
        temperature: Option<f32>,
    ) -> Result<String, GatewayError> {
        let (default_max, default_temp, seed) = match self.config.mode {
            LlamaCppMode::Generation {
                default_max_tokens,
                default_temperature,
                seed,
            } => (default_max_tokens, default_temperature, seed),
            _ => {
                return Err(self.err("adapter not configured for generation (mode != Generation)"));
            }
        };
        let max_new = max_tokens.unwrap_or(default_max).max(1);
        let temperature = temperature.unwrap_or(default_temp);

        // Build prompt via the model's bundled chat template.
        let chat = build_chat_messages(messages, system)?;
        let template = self
            .inner
            .model
            .chat_template(None)
            .map_err(|e| self.err(format!("chat template lookup: {e}")))?;
        let prompt = self
            .inner
            .model
            .apply_chat_template(&template, &chat, true)
            .map_err(|e| self.err(format!("apply chat template: {e}")))?;

        let prompt_tokens = self
            .inner
            .model
            .str_to_token(&prompt, AddBos::Always)
            .map_err(|e| self.err(format!("tokenize prompt: {e}")))?;

        // Reject prompts that don't leave room for the requested completion.
        if (prompt_tokens.len() as u32).saturating_add(max_new) > self.config.n_ctx {
            return Err(self.err(format!(
                "prompt ({} tokens) + max_new ({}) exceeds n_ctx ({})",
                prompt_tokens.len(),
                max_new,
                self.config.n_ctx
            )));
        }

        let mut guard = self
            .inner
            .context
            .lock()
            .map_err(|_| self.err("context mutex poisoned"))?;
        let ctx = &mut guard.0;
        ctx.clear_kv_cache();

        // Feed the prompt: mark only the last token for logits since we
        // only need to sample from there.
        let mut batch = LlamaBatch::new(prompt_tokens.len().max(1), 1);
        let last_prompt = prompt_tokens.len().saturating_sub(1);
        for (pos, &token) in prompt_tokens.iter().enumerate() {
            batch
                .add(token, pos as i32, &[0], pos == last_prompt)
                .map_err(|e| self.err(format!("batch.add (prompt): {e}")))?;
        }
        ctx.decode(&mut batch)
            .map_err(|e| self.err(format!("decode (prompt): {e}")))?;

        let mut sampler = if temperature <= 0.0 {
            LlamaSampler::greedy()
        } else {
            LlamaSampler::chain_simple([LlamaSampler::temp(temperature), LlamaSampler::dist(seed)])
        };

        // We accumulate raw bytes and UTF-8-decode at the end so multibyte
        // codepoints that span two tokens (common in non-Latin scripts and
        // emoji) don't get split into invalid UTF-8 fragments.
        let mut generated_bytes: Vec<u8> = Vec::new();
        let start_pos = prompt_tokens.len() as i32;
        for (offset, _) in (0..max_new).enumerate() {
            let next_pos = start_pos + offset as i32;
            // Sample from the logits at the last batch position.
            let token = sampler.sample(ctx, batch.n_tokens() - 1);
            sampler.accept(token);

            if self.inner.model.is_eog_token(token) {
                break;
            }

            let bytes = self
                .inner
                .model
                .token_to_piece_bytes(token, 32, false, None)
                .map_err(|e| self.err(format!("token_to_piece_bytes: {e}")))?;
            generated_bytes.extend_from_slice(&bytes);

            // Feed the sampled token back in for the next step.
            batch.clear();
            batch
                .add(token, next_pos, &[0], true)
                .map_err(|e| self.err(format!("batch.add (step): {e}")))?;
            ctx.decode(&mut batch)
                .map_err(|e| self.err(format!("decode (step): {e}")))?;
        }

        Ok(String::from_utf8_lossy(&generated_bytes).into_owned())
    }
}

/// Convert gateway's `Message` list (plus optional system prompt) into the
/// `LlamaChatMessage` shape that llama-cpp-2's chat template applier wants.
fn build_chat_messages(
    messages: &[Message],
    system: Option<&str>,
) -> Result<Vec<LlamaChatMessage>, GatewayError> {
    let mut chat = Vec::with_capacity(messages.len() + 1);
    if let Some(sys) = system {
        chat.push(
            LlamaChatMessage::new("system".to_string(), sys.to_string())
                .map_err(|e| chat_msg_err(&format!("system: {e}")))?,
        );
    }
    for msg in messages {
        let role = match msg.role {
            MessageRole::System => "system",
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::Tool => "tool",
        };
        chat.push(
            LlamaChatMessage::new(role.to_string(), msg.as_text().to_string())
                .map_err(|e| chat_msg_err(&format!("{role}: {e}")))?,
        );
    }
    Ok(chat)
}

fn chat_msg_err(detail: &str) -> GatewayError {
    GatewayError::ProviderError {
        adapter: "llama-cpp".into(),
        message: format!("chat message: {detail}"),
        status: None,
    }
}

fn response_with_embeddings(model_id: &str, embeddings: Vec<Vec<f32>>) -> InferenceResponse {
    InferenceResponse {
        success: true,
        content: None,
        embeddings: Some(embeddings),
        transcription: None,
        audio: None,
        images: None,
        videos: None,
        model: Some(model_id.to_string()),
        usage: None,
        tool_calls: Vec::new(),
        estimated_cost: None,
        actual_cost: None,
        attempts: vec![],
    }
}

fn response_with_content(model_id: &str, content: String) -> InferenceResponse {
    InferenceResponse {
        success: true,
        content: Some(content),
        embeddings: None,
        transcription: None,
        audio: None,
        images: None,
        videos: None,
        model: Some(model_id.to_string()),
        usage: None,
        tool_calls: Vec::new(),
        estimated_cost: None,
        actual_cost: None,
        attempts: vec![],
    }
}

#[async_trait]
impl InferenceAdapter for LlamaCppAdapter {
    fn id(&self) -> &str {
        &self.config.adapter_id
    }

    fn supports(&self, capability: &Capability) -> bool {
        matches!(
            (&self.config.mode, capability),
            (LlamaCppMode::Embedding { .. }, Capability::TextEmbed)
                | (LlamaCppMode::Generation { .. }, Capability::TextChat)
                | (LlamaCppMode::Generation { .. }, Capability::TextComplete)
        )
    }

    async fn execute(
        &self,
        _config: &RouterConfig,
        request: &InferenceRequest,
    ) -> Result<InferenceResponse, GatewayError> {
        if let Some(requested) = &request.model
            && requested != &self.config.model_id
        {
            return Err(GatewayError::ModelUnavailable {
                adapter: self.config.adapter_id.clone(),
                model: requested.clone(),
            });
        }

        match &request.payload {
            Payload::Embed { texts } => {
                let embeddings = self.embed(texts)?;
                Ok(response_with_embeddings(&self.config.model_id, embeddings))
            }
            Payload::Chat {
                messages,
                system,
                max_tokens,
                temperature,
                tools: _,
            } => {
                let content =
                    self.generate(messages, system.as_deref(), *max_tokens, *temperature)?;
                Ok(response_with_content(&self.config.model_id, content))
            }
            _ => Err(self.err(
                "LlamaCppAdapter supports Payload::Embed (Embedding mode) \
                 and Payload::Chat (Generation mode) only",
            )),
        }
    }

    async fn stream(
        &self,
        _config: &RouterConfig,
        request: &InferenceRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        if let Some(requested) = &request.model
            && requested != &self.config.model_id
        {
            return Err(GatewayError::ModelUnavailable {
                adapter: self.config.adapter_id.clone(),
                model: requested.clone(),
            });
        }

        let Payload::Chat {
            messages,
            system,
            max_tokens,
            temperature,
            tools: _,
        } = &request.payload
        else {
            return Err(self.err("LlamaCppAdapter streaming only supports Payload::Chat"));
        };

        // Resolve generation settings (mode-checked).
        let (default_max, default_temp, seed) = match self.config.mode {
            LlamaCppMode::Generation {
                default_max_tokens,
                default_temperature,
                seed,
            } => (default_max_tokens, default_temperature, seed),
            _ => {
                return Err(self.err("adapter not configured for streaming (mode != Generation)"));
            }
        };
        let max_new = max_tokens.unwrap_or(default_max).max(1);
        let temperature = temperature.unwrap_or(default_temp);

        // Build the prompt synchronously on the calling task — cheap.
        // The heavy autoregressive loop runs inside spawn_blocking.
        let chat = build_chat_messages(messages, system.as_deref())?;
        let template = self
            .inner
            .model
            .chat_template(None)
            .map_err(|e| self.err(format!("chat template lookup: {e}")))?;
        let prompt = self
            .inner
            .model
            .apply_chat_template(&template, &chat, true)
            .map_err(|e| self.err(format!("apply chat template: {e}")))?;
        let prompt_tokens = self
            .inner
            .model
            .str_to_token(&prompt, AddBos::Always)
            .map_err(|e| self.err(format!("tokenize prompt: {e}")))?;
        if (prompt_tokens.len() as u32).saturating_add(max_new) > self.config.n_ctx {
            return Err(self.err(format!(
                "prompt ({} tokens) + max_new ({}) exceeds n_ctx ({})",
                prompt_tokens.len(),
                max_new,
                self.config.n_ctx
            )));
        }

        // Channel sized so a fast producer can stay ahead of a slow
        // consumer (e.g. SSE serialisation). 32 chunks ≈ 32 token-aligned
        // text increments — small enough to keep memory bounded.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<StreamChunk, GatewayError>>(32);
        let inner = Arc::clone(&self.inner);
        let adapter_id = self.config.adapter_id.clone();

        tokio::task::spawn_blocking(move || {
            run_streaming_generation(
                inner,
                adapter_id,
                prompt_tokens,
                max_new,
                temperature,
                seed,
                tx,
            );
        });

        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}

// ---------------------------------------------------------------------------
// Capability-segregated traits (target model — see
// docs/design/adapter-capability-traits.md). Additive alongside the existing
// `InferenceAdapter` impl during the migration. The traits and `RegisterInto`
// are referenced by full path to avoid the `id()` clash with
// `InferenceAdapter::id` during the bridge.
//
// One `LlamaCppAdapter` is loaded in exactly one mode (Embedding or
// Generation), so it registers into both the chat and embed maps but only
// one is functional per instance — the off-mode path returns the same
// `ProviderError` the trait-free `generate`/`embed` helpers already raise.
// ---------------------------------------------------------------------------

impl gateway::adapters::capability::Model for LlamaCppAdapter {
    fn id(&self) -> &str {
        &self.config.adapter_id
    }
}

#[async_trait]
impl gateway::adapters::capability::ChatModel for LlamaCppAdapter {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        if let Some(requested) = &req.model
            && requested != &self.config.model_id
        {
            return Err(GatewayError::ModelUnavailable {
                adapter: self.config.adapter_id.clone(),
                model: requested.clone(),
            });
        }

        let content = self.generate(
            &req.messages,
            req.system.as_deref(),
            req.max_tokens,
            req.temperature,
        )?;
        // llama.cpp generation carries no tool calls and the local decode
        // loop doesn't surface a token count, so usage stays `None` — same
        // as the legacy `response_with_content` path.
        Ok(ChatResponse {
            content: Some(content),
            tool_calls: Vec::new(),
            usage: None,
            model: Some(self.config.model_id.clone()),
        })
    }

    async fn chat_stream(
        &self,
        _cfg: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        if let Some(requested) = &req.model
            && requested != &self.config.model_id
        {
            return Err(GatewayError::ModelUnavailable {
                adapter: self.config.adapter_id.clone(),
                model: requested.clone(),
            });
        }

        // Resolve generation settings (mode-checked).
        let (default_max, default_temp, seed) = match self.config.mode {
            LlamaCppMode::Generation {
                default_max_tokens,
                default_temperature,
                seed,
            } => (default_max_tokens, default_temperature, seed),
            _ => {
                return Err(self.err("adapter not configured for streaming (mode != Generation)"));
            }
        };
        let max_new = req.max_tokens.unwrap_or(default_max).max(1);
        let temperature = req.temperature.unwrap_or(default_temp);

        // Build the prompt synchronously on the calling task — cheap. The
        // heavy autoregressive loop runs inside spawn_blocking.
        let chat = build_chat_messages(&req.messages, req.system.as_deref())?;
        let template = self
            .inner
            .model
            .chat_template(None)
            .map_err(|e| self.err(format!("chat template lookup: {e}")))?;
        let prompt = self
            .inner
            .model
            .apply_chat_template(&template, &chat, true)
            .map_err(|e| self.err(format!("apply chat template: {e}")))?;
        let prompt_tokens = self
            .inner
            .model
            .str_to_token(&prompt, AddBos::Always)
            .map_err(|e| self.err(format!("tokenize prompt: {e}")))?;
        if (prompt_tokens.len() as u32).saturating_add(max_new) > self.config.n_ctx {
            return Err(self.err(format!(
                "prompt ({} tokens) + max_new ({}) exceeds n_ctx ({})",
                prompt_tokens.len(),
                max_new,
                self.config.n_ctx
            )));
        }

        // Channel sized so a fast producer can stay ahead of a slow consumer
        // (e.g. SSE serialisation). 32 chunks ≈ 32 token-aligned text
        // increments — small enough to keep memory bounded.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<StreamChunk, GatewayError>>(32);
        let inner = Arc::clone(&self.inner);
        let adapter_id = self.config.adapter_id.clone();

        tokio::task::spawn_blocking(move || {
            run_streaming_generation(
                inner,
                adapter_id,
                prompt_tokens,
                max_new,
                temperature,
                seed,
                tx,
            );
        });

        Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
    }
}

#[async_trait]
impl gateway::adapters::capability::EmbedModel for LlamaCppAdapter {
    async fn embed(
        &self,
        _cfg: &RouterConfig,
        req: &EmbedRequest,
    ) -> Result<EmbedResponse, GatewayError> {
        if let Some(requested) = &req.model
            && requested != &self.config.model_id
        {
            return Err(GatewayError::ModelUnavailable {
                adapter: self.config.adapter_id.clone(),
                model: requested.clone(),
            });
        }

        // Inherent `LlamaCppAdapter::embed` (the trait is referenced by full
        // path, not `use`d, so this method call binds to the inherent one).
        let embeddings = self.embed(&req.texts)?;
        Ok(EmbedResponse {
            embeddings,
            usage: None,
        })
    }
}

#[async_trait]
impl gateway::adapters::RegisterInto for LlamaCppAdapter {
    async fn register_into(
        self: std::sync::Arc<Self>,
        reg: &gateway::adapters::CapabilityRegistry,
    ) {
        reg.register_chat(self.clone()).await;
        reg.register_embed(self).await;
    }
}

/// Streaming generation body — runs in a `spawn_blocking` worker.
/// Sends a [`StreamChunk`] for every UTF-8-valid prefix increment of
/// the accumulated bytes, plus a final empty-content chunk with
/// `finish_reason` set to `"stop"` (EOS) or `"length"` (hit max_new).
///
/// Why UTF-8-prefix-based chunking and not "one chunk per token":
/// tokenizers can split multi-byte codepoints across tokens (common in
/// CJK / emoji / non-Latin scripts). Naively decoding each token in
/// isolation produces invalid UTF-8 fragments and either errors out or
/// emits U+FFFD replacement characters. Accumulating bytes and only
/// emitting the longest valid UTF-8 prefix per step preserves the
/// original text exactly.
fn run_streaming_generation(
    inner: Arc<Inner>,
    adapter_id: String,
    prompt_tokens: Vec<LlamaToken>,
    max_new: u32,
    temperature: f32,
    seed: u32,
    tx: tokio::sync::mpsc::Sender<Result<StreamChunk, GatewayError>>,
) {
    let err = |message: String| GatewayError::ProviderError {
        adapter: adapter_id.clone(),
        message,
        status: None,
    };

    // Lock the context for the duration of this generation. Other
    // concurrent calls on the same adapter will queue; this matches
    // the non-streaming generate() path.
    let mut guard = match inner.context.lock() {
        Ok(g) => g,
        Err(_) => {
            let _ = tx.blocking_send(Err(err("context mutex poisoned".into())));
            return;
        }
    };
    let ctx = &mut guard.0;
    ctx.clear_kv_cache();

    let mut batch = LlamaBatch::new(prompt_tokens.len().max(1), 1);
    let last_prompt = prompt_tokens.len().saturating_sub(1);
    for (pos, &token) in prompt_tokens.iter().enumerate() {
        if let Err(e) = batch.add(token, pos as i32, &[0], pos == last_prompt) {
            let _ = tx.blocking_send(Err(err(format!("batch.add (prompt): {e}"))));
            return;
        }
    }
    if let Err(e) = ctx.decode(&mut batch) {
        let _ = tx.blocking_send(Err(err(format!("decode (prompt): {e}"))));
        return;
    }

    let mut sampler = if temperature <= 0.0 {
        LlamaSampler::greedy()
    } else {
        LlamaSampler::chain_simple([LlamaSampler::temp(temperature), LlamaSampler::dist(seed)])
    };

    // UTF-8-safe streaming buffer. `emitted` tracks how many bytes
    // have already been shipped as chunks; we only ship bytes that
    // form a valid UTF-8 prefix.
    let mut buf: Vec<u8> = Vec::new();
    let mut emitted: usize = 0;
    let start_pos = prompt_tokens.len() as i32;
    let mut finish_reason = "length";

    for (offset, _) in (0..max_new).enumerate() {
        let next_pos = start_pos + offset as i32;
        let token = sampler.sample(ctx, batch.n_tokens() - 1);
        sampler.accept(token);

        if inner.model.is_eog_token(token) {
            finish_reason = "stop";
            break;
        }

        let bytes = match inner.model.token_to_piece_bytes(token, 32, false, None) {
            Ok(b) => b,
            Err(e) => {
                let _ = tx.blocking_send(Err(err(format!("token_to_piece_bytes: {e}"))));
                return;
            }
        };
        buf.extend_from_slice(&bytes);

        // Find the longest valid UTF-8 prefix beyond `emitted`.
        let valid_end = match std::str::from_utf8(&buf) {
            Ok(_) => buf.len(),
            Err(e) => e.valid_up_to(),
        };
        if valid_end > emitted {
            // Safety: we just confirmed [0..valid_end] is valid UTF-8.
            let new_text = std::str::from_utf8(&buf[emitted..valid_end])
                .expect("valid prefix")
                .to_string();
            emitted = valid_end;
            if tx
                .blocking_send(Ok(StreamChunk {
                    content: new_text,
                    finish_reason: None,
                    usage: None,
                    tool_calls: Vec::new(),
                }))
                .is_err()
            {
                // Receiver dropped — caller cancelled the stream. Stop early.
                return;
            }
        }

        batch.clear();
        if let Err(e) = batch.add(token, next_pos, &[0], true) {
            let _ = tx.blocking_send(Err(err(format!("batch.add (step): {e}"))));
            return;
        }
        if let Err(e) = ctx.decode(&mut batch) {
            let _ = tx.blocking_send(Err(err(format!("decode (step): {e}"))));
            return;
        }
    }

    // Flush any trailing valid bytes the loop didn't emit (rare —
    // would only happen if the final iteration appended bytes that
    // sat past `emitted` but were valid prefix).
    let valid_end = match std::str::from_utf8(&buf) {
        Ok(_) => buf.len(),
        Err(e) => e.valid_up_to(),
    };
    if valid_end > emitted
        && let Ok(s) = std::str::from_utf8(&buf[emitted..valid_end])
    {
        let _ = tx.blocking_send(Ok(StreamChunk {
            content: s.to_string(),
            finish_reason: None,
            usage: None,
            tool_calls: Vec::new(),
        }));
    }

    // Final chunk carries finish_reason. Empty content keeps the
    // contract that all real text was already streamed.
    let _ = tx.blocking_send(Ok(StreamChunk {
        content: String::new(),
        finish_reason: Some(finish_reason.into()),
        usage: None,
        tool_calls: Vec::new(),
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{ModelFormat, ModelSource};
    use std::path::PathBuf;

    /// Test-only wrapper around the public [`super::shared_backend`].
    /// Panics on init failure — tests run inside a single process so
    /// `LlamaBackend::init` either works once or not at all.
    fn shared_backend() -> Arc<LlamaBackend> {
        super::shared_backend().expect("shared_backend")
    }

    fn external_entry(path: impl Into<PathBuf>) -> ModelEntry {
        let path = path.into();
        ModelEntry {
            id: "test-model".into(),
            name: "test".into(),
            format: ModelFormat::Gguf,
            source: ModelSource::External { path },
            sha256: None,
            size_bytes: None,
        }
    }

    #[test]
    fn check_source_supported_rejects_missing_file_with_provider_error() {
        let entry = external_entry("/definitely/does/not/exist/x.gguf");
        let err = LlamaCppAdapter::check_source_supported(&entry).unwrap_err();
        match err {
            GatewayError::ProviderError { message, .. } => {
                assert!(message.contains("not found"), "got: {message}");
            }
            other => panic!("expected ProviderError, got {other:?}"),
        }
    }

    #[test]
    fn check_source_supported_accepts_existing_file() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let entry = external_entry(tmp.path());
        LlamaCppAdapter::check_source_supported(&entry).expect("present file");
    }

    /// Loading the same GGUF path twice reuses the cached
    /// `Arc<LlamaModel>` — pointer equality is the cheap end-to-end
    /// proof. Gated on `LLAMA_TEST_GGUF` because the cache hit only
    /// matters once a real model file is available.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires LLAMA_TEST_GGUF env var pointing at a BERT-class embedding GGUF"]
    async fn cached_model_reuses_arc_across_load_calls() {
        let path =
            std::env::var("LLAMA_TEST_GGUF").expect("LLAMA_TEST_GGUF must point at a GGUF file");
        // Use the test-module helper, which already unwraps to an
        // Arc<LlamaBackend> (tests panic on init failure).
        let backend = shared_backend();
        let path = std::path::PathBuf::from(path);

        let a = cached_model(&backend, &path).expect("first load");
        let b = cached_model(&backend, &path).expect("second load");

        // Same underlying model — Arc::ptr_eq is exact pointer
        // equality, so any miss falls out cleanly.
        assert!(
            Arc::ptr_eq(&a, &b),
            "second load must return the cached Arc, not reload from disk",
        );

        // After dropping all strong refs, the cache should re-load
        // on the next call. We can't directly assert "weights were
        // re-read from disk" without instrumenting the loader, but
        // we can at least check that the third Arc is distinct from
        // the first (because the original Arc backing `a`/`b` has
        // been dropped between the second and third load).
        drop(a);
        drop(b);
        let c = cached_model(&backend, &path).expect("third load after drop");
        // `c` is necessarily a fresh Arc — its inner pointer may or
        // may not match the original allocation (depends on the
        // allocator). The contract worth asserting is just that the
        // call succeeded.
        let _ = c;
    }

    #[test]
    fn embed_config_builder_sets_sensible_defaults_for_minilm() {
        let cfg = LlamaCppConfig::embed("all-minilm");
        assert_eq!(cfg.model_id, "all-minilm");
        assert_eq!(cfg.adapter_id, "llama-cpp");
        assert_eq!(cfg.n_ctx, 512);
        assert_eq!(cfg.n_seq_max, 64);
        match cfg.mode {
            LlamaCppMode::Embedding { pooling } => {
                assert!(matches!(pooling, LlamaPoolingType::Mean))
            }
            _ => panic!("expected Embedding mode"),
        }
    }

    #[test]
    fn chat_config_builder_sets_sensible_defaults_for_generation() {
        let cfg = LlamaCppConfig::chat("llama-3.2");
        assert_eq!(cfg.model_id, "llama-3.2");
        assert_eq!(cfg.adapter_id, "llama-cpp");
        assert_eq!(cfg.n_ctx, 4096);
        assert_eq!(cfg.n_seq_max, 1);
        match cfg.mode {
            LlamaCppMode::Generation {
                default_max_tokens,
                default_temperature,
                ..
            } => {
                assert_eq!(default_max_tokens, 512);
                assert_eq!(default_temperature, 0.0);
            }
            _ => panic!("expected Generation mode"),
        }
    }

    #[test]
    fn build_chat_messages_translates_roles_and_prepends_system() {
        let msgs = vec![
            Message::text(MessageRole::User, "hi"),
            Message::text(MessageRole::Assistant, "hello"),
        ];
        let chat = build_chat_messages(&msgs, Some("you are a test bot")).unwrap();
        assert_eq!(chat.len(), 3, "system + user + assistant");
        // LlamaChatMessage doesn't expose role/content getters, but the
        // count + non-error construction is what we need here.
    }

    #[test]
    fn build_chat_messages_rejects_content_with_null_bytes() {
        let msgs = vec![Message::text(MessageRole::User, "has\0null")];
        let err = build_chat_messages(&msgs, None).unwrap_err();
        match err {
            GatewayError::ProviderError { message, .. } => {
                assert!(message.contains("user"), "got: {message}");
            }
            other => panic!("expected ProviderError, got {other:?}"),
        }
    }

    /// End-to-end embedding against a real GGUF. Ignored by default so
    /// `cargo test --features llama-cpp` doesn't require a model file on
    /// disk. Run with:
    ///
    ///     LLAMA_TEST_GGUF=$HOME/.ollama/models/blobs/sha256-... \
    ///       cargo test -p gateway-embedded --features llama-cpp -- --ignored
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires LLAMA_TEST_GGUF env var pointing at a BERT-class embedding GGUF"]
    async fn embed_against_real_model_returns_unit_length_vectors() {
        let path = std::env::var("LLAMA_TEST_GGUF")
            .expect("LLAMA_TEST_GGUF must point at a BERT GGUF file");
        let entry = external_entry(path);

        let backend = shared_backend();
        let adapter = LlamaCppAdapter::load(backend, &entry, LlamaCppConfig::embed("test-model"))
            .expect("load model");

        let texts = vec![
            "hello world".to_string(),
            "the quick brown fox jumps over the lazy dog".to_string(),
        ];
        let embeddings = adapter.embed(&texts).expect("embed");

        assert_eq!(embeddings.len(), 2);
        for (i, v) in embeddings.iter().enumerate() {
            assert!(!v.is_empty(), "embedding[{i}] dim should be > 0");
            let mag: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            assert!(
                (mag - 1.0).abs() < 1e-4,
                "embedding[{i}]: expected unit-length, got |v|={mag}",
            );
        }
    }

    /// End-to-end chat against a real generative GGUF (e.g. llama-3.2,
    /// qwen2.5, etc.). Ignored by default. Run with:
    ///
    ///     LLAMA_TEST_CHAT_GGUF=$HOME/.ollama/models/blobs/sha256-... \
    ///       cargo test -p gateway-embedded --features llama-cpp -- --ignored
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires LLAMA_TEST_CHAT_GGUF env var pointing at a generative GGUF with a chat template"]
    async fn generate_against_real_model_returns_non_empty_text() {
        let path = std::env::var("LLAMA_TEST_CHAT_GGUF")
            .expect("LLAMA_TEST_CHAT_GGUF must point at a generative GGUF");
        let entry = external_entry(path);

        let backend = shared_backend();
        let mut cfg = LlamaCppConfig::chat("test-chat-model");
        // Keep the test fast: cap to 16 new tokens.
        if let LlamaCppMode::Generation {
            default_max_tokens, ..
        } = &mut cfg.mode
        {
            *default_max_tokens = 16;
        }
        let adapter = LlamaCppAdapter::load(backend, &entry, cfg).expect("load model");

        let messages = vec![Message::text(
            MessageRole::User,
            "Reply with the single word: pong.".to_string(),
        )];
        let text = adapter
            .generate(&messages, None, Some(16), Some(0.0))
            .expect("generate");

        assert!(!text.is_empty(), "expected non-empty generation");
        assert!(
            text.len() < 256,
            "expected short response under max_tokens cap, got {text:?}"
        );
    }

    /// End-to-end streaming chat against a real generative GGUF.
    /// Walks the returned `Stream<Item = Result<StreamChunk, _>>` and
    /// asserts that (a) at least one non-empty content chunk is
    /// emitted, (b) the final chunk carries a `finish_reason`, and
    /// (c) the concatenated content is non-empty and bounded by the
    /// max_tokens cap. Ignored by default — same env var as the
    /// non-streaming chat test.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "requires LLAMA_TEST_CHAT_GGUF env var pointing at a generative GGUF with a chat template"]
    async fn stream_against_real_model_emits_chunks_with_finish_reason() {
        use futures::StreamExt;
        use gateway::types::config::RouterConfig;

        let path = std::env::var("LLAMA_TEST_CHAT_GGUF")
            .expect("LLAMA_TEST_CHAT_GGUF must point at a generative GGUF");
        let entry = external_entry(path);

        let backend = shared_backend();
        let mut cfg = LlamaCppConfig::chat("test-chat-model");
        if let LlamaCppMode::Generation {
            default_max_tokens, ..
        } = &mut cfg.mode
        {
            *default_max_tokens = 16;
        }
        let adapter = LlamaCppAdapter::load(backend, &entry, cfg).expect("load model");

        let request = InferenceRequest {
            capability: Capability::TextChat,
            model: Some("test-chat-model".into()),
            router: None,
            chain: None,
            payload: Payload::Chat {
                messages: vec![Message::text(
                    MessageRole::User,
                    "Reply with the single word: pong.".to_string(),
                )],
                system: None,
                max_tokens: Some(16),
                temperature: Some(0.0),
                tools: Vec::new(),
            },
            budget: None,
        };

        let router_cfg = RouterConfig {
            url: "embedded://llama-cpp".into(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: std::collections::HashMap::new(),
        };

        let mut stream = adapter.stream(&router_cfg, &request).await.expect("stream");

        let mut accumulated = String::new();
        let mut content_chunks = 0usize;
        let mut finish_reason: Option<String> = None;
        while let Some(item) = stream.next().await {
            let chunk = item.expect("chunk result");
            if !chunk.content.is_empty() {
                content_chunks += 1;
                accumulated.push_str(&chunk.content);
            }
            if chunk.finish_reason.is_some() {
                finish_reason = chunk.finish_reason;
            }
        }

        assert!(
            content_chunks > 0,
            "expected at least one non-empty content chunk"
        );
        assert!(
            finish_reason.is_some(),
            "expected the final chunk to carry a finish_reason"
        );
        let reason = finish_reason.unwrap();
        assert!(
            reason == "stop" || reason == "length",
            "unexpected finish_reason: {reason}"
        );
        assert!(
            !accumulated.is_empty() && accumulated.len() < 256,
            "expected short non-empty accumulated content, got {accumulated:?}"
        );
    }
}
