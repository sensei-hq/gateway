//! Canonical OpenAI-compatible chat / embed / streaming core.
//!
//! Every OpenAI-compatible cloud adapter (openai, ollama, grok,
//! together, huggingface, …) speaks the same `/v1/chat/completions` and
//! `/v1/embeddings` wire format. Rather than re-declare the wire types
//! and request/response plumbing in each adapter, this module owns the
//! full-featured variant once (tools + multimodal + streaming-with-tools)
//! and exposes three `pub(crate)` entry points — [`chat`], [`chat_stream`],
//! and [`embed`] — that speak the gateway's typed
//! [`io`](kernel::types::io) request/response structs and encapsulate the
//! HTTP.
//!
//! Adapters keep their own `struct`, `Model::id`, base-url / default-model
//! consts, and any non-OpenAI-compat capabilities (image / audio); their
//! `ChatModel`/`EmbedModel` methods become thin delegations to the entry
//! points here. See `docs/design/hf-inference-adapter.md` §3.

use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;

use futures::Stream;
use futures::stream::StreamExt;
use reqwest::Client;

use crate::base::{JsonEndpoint, error_from_response, http_json, resolve_api_key};
use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;
use kernel::types::io::{ChatRequest, ChatResponse};
use kernel::types::request::{StreamChunk, ToolCall};

mod convert;
use convert::{
    ChatCompletionRequest, ChatCompletionResponse, EmbedRequest, EmbedResponse, OpenAiStreamState,
    build_chat_messages, build_tools, from_openai_tool_call, process_stream_bytes,
    usage_from_response,
};

/// Boxed streaming type shared by the capability traits.
pub(crate) type ChunkStream = Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>;

/// Adapter label used in error mapping on the streaming path (the
/// non-streaming path routes errors through [`http_json`], which labels
/// them `"http"`). The concrete adapter id isn't threaded through the
/// shared core, so this is a generic placeholder — no test asserts on it.
const ADAPTER: &str = "openai_compat";

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

/// Non-streaming chat completion against `{base_url}/v1/chat/completions`.
///
/// Model = `req.model` else `default_model`. Auth = bearer from
/// [`resolve_api_key`] (omitted when absent, e.g. local Ollama) plus any
/// `cfg.headers`. Forwards tools + multimodal attachments and parses
/// content + tool_calls + usage back out.
pub(crate) async fn chat(
    client: &Client,
    base_url: &str,
    default_model: &str,
    cfg: &RouterConfig,
    req: &ChatRequest,
) -> Result<ChatResponse, GatewayError> {
    let api_key = resolve_api_key(cfg);
    let model = req
        .model
        .clone()
        .unwrap_or_else(|| default_model.to_string());

    let body = ChatCompletionRequest {
        model: model.clone(),
        messages: build_chat_messages(&req.messages, &req.system),
        max_tokens: req.max_tokens,
        temperature: req.temperature,
        stream: false,
        tools: build_tools(&req.tools),
    };

    let resp: ChatCompletionResponse = http_json(
        client,
        JsonEndpoint {
            base_url,
            path: "/v1/chat/completions",
            api_key: api_key.as_deref(),
            extra_headers: &cfg.headers,
        },
        &body,
    )
    .await?;

    let first = resp.choices.first();
    let content = first.and_then(|c| c.message.content.clone());
    let tool_calls: Vec<ToolCall> = first
        .and_then(|c| c.message.tool_calls.as_ref())
        .map(|tcs| tcs.iter().map(from_openai_tool_call).collect())
        .unwrap_or_default();
    let usage = usage_from_response(&resp.usage);

    Ok(ChatResponse {
        content,
        tool_calls,
        usage,
        model: Some(model),
        degraded: false,
    })
}

/// Streaming chat completion. Same request-building as [`chat`] with
/// `stream: true`; parses the SSE `data:` frames into [`StreamChunk`]s,
/// accumulating fragmented tool-call arguments per index and emitting the
/// assembled calls on the terminal `finish_reason` chunk.
pub(crate) async fn chat_stream(
    client: &Client,
    base_url: &str,
    default_model: &str,
    cfg: &RouterConfig,
    req: &ChatRequest,
) -> Result<ChunkStream, GatewayError> {
    let api_key = resolve_api_key(cfg);
    let model = req
        .model
        .clone()
        .unwrap_or_else(|| default_model.to_string());

    let body = ChatCompletionRequest {
        model,
        messages: build_chat_messages(&req.messages, &req.system),
        max_tokens: req.max_tokens,
        temperature: req.temperature,
        stream: true,
        tools: build_tools(&req.tools),
    };

    let url = format!("{}/v1/chat/completions", base_url.trim_end_matches('/'));
    let mut request = client.post(&url).json(&body);
    if let Some(key) = &api_key {
        request = request.bearer_auth(key);
    }
    for (k, v) in &cfg.headers {
        request = request.header(k.as_str(), v.as_str());
    }

    let response = request.send().await?;
    let status = response.status();

    if !status.is_success() {
        return Err(error_from_response(ADAPTER, response).await);
    }

    let byte_stream: Pin<Box<dyn Stream<Item = _> + Send>> = Box::pin(response.bytes_stream());
    let initial = OpenAiStreamState {
        byte_stream,
        line_buf: String::new(),
        tool_calls: BTreeMap::new(),
        pending: VecDeque::new(),
        eof: false,
    };

    let stream = futures::stream::unfold(initial, |mut state| async move {
        loop {
            if let Some(item) = state.pending.pop_front() {
                return Some((item, state));
            }
            if state.eof {
                return None;
            }
            match state.byte_stream.next().await {
                Some(Ok(bytes)) => process_stream_bytes(&mut state, &bytes),
                Some(Err(e)) => {
                    state.pending.push_back(Err(GatewayError::ProviderError {
                        adapter: ADAPTER.into(),
                        message: format!("{ADAPTER} stream error: {e}"),
                        status: None,
                    }));
                    state.eof = true;
                }
                None => state.eof = true,
            }
        }
    });

    Ok(Box::pin(stream))
}

/// Batch embeddings against `{base_url}/v1/embeddings`.
///
/// Model = `req.model` else `default_model`. Auth = bearer from
/// [`resolve_api_key`] (omitted when absent) plus any `cfg.headers`.
pub(crate) async fn embed(
    client: &Client,
    base_url: &str,
    default_model: &str,
    cfg: &RouterConfig,
    req: &kernel::types::io::EmbedRequest,
) -> Result<kernel::types::io::EmbedResponse, GatewayError> {
    let api_key = resolve_api_key(cfg);
    let model = req
        .model
        .clone()
        .unwrap_or_else(|| default_model.to_string());

    let body = EmbedRequest {
        model,
        input: req.texts.clone(),
    };

    let resp: EmbedResponse = http_json(
        client,
        JsonEndpoint {
            base_url,
            path: "/v1/embeddings",
            api_key: api_key.as_deref(),
            extra_headers: &cfg.headers,
        },
        &body,
    )
    .await?;

    let embeddings: Vec<Vec<f32>> = resp.data.into_iter().map(|d| d.embedding).collect();
    let usage = usage_from_response(&resp.usage);

    Ok(kernel::types::io::EmbedResponse {
        embeddings,
        usage,
        degraded: false,
    })
}

/// Generate the standard OpenAI-compatible [`ChatModel`](kernel::adapters::capability::ChatModel)
/// impl for an adapter whose `chat`/`chat_stream` just require an API key then delegate to
/// [`chat`]/[`chat_stream`] with a default model — the "thin delegation" contract described in
/// this module's header. The adapter must have a `client: reqwest::Client` field.
///
/// - `$adapter` — the adapter type.
/// - `model = $model` — default model id used when the request doesn't pin one.
/// - `name = $name` — adapter name for the `Authentication` error when the key is missing.
///
/// ```ignore
/// crate::impl_openai_compat_chat!(OpenAIAdapter, model = DEFAULT_MODEL, name = "openai");
/// ```
#[macro_export]
macro_rules! impl_openai_compat_chat {
    ($adapter:ty, model = $model:expr, name = $name:literal) => {
        #[async_trait::async_trait]
        impl kernel::adapters::capability::ChatModel for $adapter {
            async fn chat(
                &self,
                config: &kernel::types::config::RouterConfig,
                req: &kernel::types::io::ChatRequest,
            ) -> ::std::result::Result<
                kernel::types::io::ChatResponse,
                kernel::types::error::GatewayError,
            > {
                // Require a key up front (the shared core treats it as optional, since local
                // providers need none); a missing one short-circuits to Authentication.
                $crate::base::require_api_key(config, $name)?;
                $crate::openai_compat::chat(&self.client, &config.url, $model, config, req).await
            }

            async fn chat_stream(
                &self,
                config: &kernel::types::config::RouterConfig,
                req: &kernel::types::io::ChatRequest,
            ) -> ::std::result::Result<
                ::std::pin::Pin<
                    Box<
                        dyn futures::Stream<
                                Item = ::std::result::Result<
                                    kernel::types::request::StreamChunk,
                                    kernel::types::error::GatewayError,
                                >,
                            > + Send,
                    >,
                >,
                kernel::types::error::GatewayError,
            > {
                $crate::base::require_api_key(config, $name)?;
                $crate::openai_compat::chat_stream(&self.client, &config.url, $model, config, req)
                    .await
            }
        }
    };
}
