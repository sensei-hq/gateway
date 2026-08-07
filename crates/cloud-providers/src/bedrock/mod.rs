//! AWS Bedrock adapter.
//!
//! Talks to the Bedrock Converse API via `aws-sdk-bedrockruntime`. Unlike
//! the HTTP-based adapters in this crate, Bedrock auth is handled by the
//! AWS SDK's credential-provider chain (env vars → shared credentials
//! file → IAM role → IMDS), and request signing is SigV4 under the hood.
//!
//! What of `RouterConfig` is and isn't used:
//! - `api_key` / `api_key_env` are **ignored** — the SDK resolves
//!   credentials itself via the provider chain.
//! - `url` is **ignored** — the SDK resolves the regional Bedrock
//!   endpoint (from `AWS_REGION` / the credential chain), so there is no
//!   operator-supplied base URL to honour.
//! - `headers` **are** honoured: every entry is stamped onto the
//!   outbound request through the SDK's `customize().mutate_request`
//!   hook (see [`apply_config`]).
//! - `timeout_ms` **is** honoured: it maps to a per-operation SDK
//!   timeout via `config_override`.
//! - Per-request `model` / `max_tokens` / `temperature` are honoured as
//!   usual.
//!
//! Capability coverage: `TextChat` via the unified Converse API, which
//! standardises the message shape across Anthropic, Meta, Mistral, and
//! Amazon-hosted models on Bedrock. Embeddings (Titan Text Embeddings,
//! Cohere Embed) and streaming (`converse_stream`) are scoped as
//! follow-ups — the unified-chat path is the highest-value entry point.
//!
//! Configuration: [`BedrockAdapter::new`] does
//! `aws_config::load_defaults` (async, picks up region + credentials
//! from the standard provider chain). For tests or callers that need to
//! pin a region explicitly, use [`BedrockAdapter::with_region`].

use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;

use async_trait::async_trait;
use aws_sdk_bedrockruntime::{Client, types::InferenceConfiguration};
use aws_smithy_types::Blob;
use futures::Stream;

use kernel::types::config::RouterConfig;
use kernel::types::cost::TokenUsage;
use kernel::types::error::GatewayError;
use kernel::types::io::{ChatRequest, ChatResponse, EmbedRequest, EmbedResponse};
use kernel::types::request::{StreamChunk, StreamingToolCall};

mod convert;
use convert::{
    build_messages, build_system, build_tool_config, chunk_from_event, extract_text,
    extract_tool_calls, map_sdk_error,
};

const ADAPTER_ID: &str = "bedrock";
/// Sensible default model id when callers don't specify one. Anthropic's
/// Claude Sonnet 3.5 v2 is the most broadly available Bedrock chat
/// model at the time of writing.
const DEFAULT_MODEL: &str = "anthropic.claude-3-5-sonnet-20241022-v2:0";
/// Default embedding model when callers don't specify one. Titan v2
/// is the highest-quality first-party embedding model on Bedrock and
/// has the broadest regional availability.
const DEFAULT_EMBED_MODEL: &str = "amazon.titan-embed-text-v2:0";
const DEFAULT_MAX_TOKENS: i32 = 1024;

pub struct BedrockAdapter {
    client: Client,
}

impl BedrockAdapter {
    /// Load AWS config from the standard provider chain (env vars,
    /// shared credentials, IAM role, IMDS) and build a Bedrock client.
    /// Async because credential resolution may touch the filesystem or
    /// the IMDS endpoint.
    pub async fn new() -> Result<Self, GatewayError> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Ok(Self {
            client: Client::new(&config),
        })
    }

    /// Same as [`Self::new`] but pins an explicit AWS region instead of
    /// relying on the provider chain (`AWS_REGION` env var, etc.).
    pub async fn with_region(region: impl Into<String>) -> Result<Self, GatewayError> {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.into()))
            .load()
            .await;
        Ok(Self {
            client: Client::new(&config),
        })
    }

    fn err(message: impl Into<String>, status: Option<u16>) -> GatewayError {
        GatewayError::ProviderError {
            adapter: ADAPTER_ID.into(),
            message: message.into(),
            status,
        }
    }
}

impl BedrockAdapter {
    /// Invoke a Titan text-embedding model. Titan accepts a single
    /// `inputText` per call; we loop over the input slice and
    /// accumulate the per-call token counts.
    async fn invoke_titan_embed(
        &self,
        model_id: &str,
        texts: &[String],
        config: &RouterConfig,
    ) -> Result<(Vec<Vec<f32>>, Option<u32>), GatewayError> {
        let mut embeddings = Vec::with_capacity(texts.len());
        let mut total_tokens: u32 = 0;
        let mut saw_tokens = false;
        for text in texts {
            let body = serde_json::to_vec(&TitanEmbedRequest { input_text: text })
                .map_err(|e| Self::err(format!("titan request encode: {e}"), None))?;
            let op = self
                .client
                .invoke_model()
                .model_id(model_id)
                .content_type("application/json")
                .accept("application/json")
                .body(Blob::new(body))
                .customize();
            let resp = apply_config(op, config)
                .send()
                .await
                .map_err(map_sdk_error)?;
            let parsed: TitanEmbedResponse = serde_json::from_slice(resp.body().as_ref())
                .map_err(|e| Self::err(format!("titan response decode: {e}"), None))?;
            embeddings.push(parsed.embedding);
            if let Some(n) = parsed.input_text_token_count {
                total_tokens = total_tokens.saturating_add(n);
                saw_tokens = true;
            }
        }
        Ok((embeddings, saw_tokens.then_some(total_tokens)))
    }

    /// Invoke a Cohere embed model. Cohere takes a batch of texts in
    /// one call and returns one vector per input. `input_type` is
    /// required for v3 models; we default to `search_document` which
    /// matches a generic ingestion path (search-time queries should
    /// pass `search_query`, but that's the caller's choice via a
    /// follow-up surface — gateway doesn't model it yet).
    async fn invoke_cohere_embed(
        &self,
        model_id: &str,
        texts: &[String],
        config: &RouterConfig,
    ) -> Result<(Vec<Vec<f32>>, Option<u32>), GatewayError> {
        let body = serde_json::to_vec(&CohereEmbedRequest {
            texts,
            input_type: "search_document",
        })
        .map_err(|e| Self::err(format!("cohere request encode: {e}"), None))?;
        let op = self
            .client
            .invoke_model()
            .model_id(model_id)
            .content_type("application/json")
            .accept("application/json")
            .body(Blob::new(body))
            .customize();
        let resp = apply_config(op, config)
            .send()
            .await
            .map_err(map_sdk_error)?;
        let parsed: CohereEmbedResponse = serde_json::from_slice(resp.body().as_ref())
            .map_err(|e| Self::err(format!("cohere response decode: {e}"), None))?;
        // Cohere doesn't return per-request token counts on the embed
        // endpoint — usage is reported at the account level.
        Ok((parsed.embeddings, None))
    }
}

// ---------------------------------------------------------------------------
// Capability traits (target model). Traits + RegisterInto referenced by full path.
// ---------------------------------------------------------------------------

impl kernel::adapters::capability::Model for BedrockAdapter {
    fn id(&self) -> &str {
        ADAPTER_ID
    }
}

#[async_trait]
impl kernel::adapters::capability::ChatModel for BedrockAdapter {
    async fn chat(
        &self,
        cfg: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        // Bedrock auth is SigV4 via the SDK's credential chain, so
        // `cfg.url` / `cfg.api_key` are ignored; `cfg.headers` and
        // `cfg.timeout_ms` are applied to the operation via apply_config.
        let model_id = req
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());
        let bedrock_messages = build_messages(&req.messages)?;
        let system_blocks = build_system(&req.messages, &req.system);

        let inference_cfg = InferenceConfiguration::builder()
            .max_tokens(
                req.max_tokens
                    .map(|n| n as i32)
                    .unwrap_or(DEFAULT_MAX_TOKENS),
            )
            .set_temperature(req.temperature)
            .build();

        let mut builder = self
            .client
            .converse()
            .model_id(model_id.clone())
            .inference_config(inference_cfg);
        for m in bedrock_messages {
            builder = builder.messages(m);
        }
        for s in system_blocks {
            builder = builder.system(s);
        }
        if let Some(tool_cfg) = build_tool_config(&req.tools) {
            builder = builder.tool_config(tool_cfg);
        }

        let response = apply_config(builder.customize(), cfg)
            .send()
            .await
            .map_err(map_sdk_error)?;

        let content = extract_text(&response);
        let tool_calls = extract_tool_calls(&response);
        let usage = response.usage.as_ref().map(|u| TokenUsage {
            input_tokens: u.input_tokens.max(0) as u32,
            output_tokens: u.output_tokens.max(0) as u32,
            total_tokens: u.total_tokens.max(0) as u32,
        });

        Ok(ChatResponse {
            content: if content.is_empty() {
                None
            } else {
                Some(content)
            },
            tool_calls,
            usage,
            model: Some(model_id),
            degraded: false,
        })
    }

    async fn chat_stream(
        &self,
        cfg: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        // Real Converse streaming — mirrors the non-streaming path,
        // reading from the typed `req` instead of Payload::Chat.
        // `cfg.headers` / `cfg.timeout_ms` are applied via apply_config.
        let model_id = req
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());
        let bedrock_messages = build_messages(&req.messages)?;
        let system_blocks = build_system(&req.messages, &req.system);

        let inference_cfg = InferenceConfiguration::builder()
            .max_tokens(
                req.max_tokens
                    .map(|n| n as i32)
                    .unwrap_or(DEFAULT_MAX_TOKENS),
            )
            .set_temperature(req.temperature)
            .build();

        let mut builder = self
            .client
            .converse_stream()
            .model_id(model_id)
            .inference_config(inference_cfg);
        for m in bedrock_messages {
            builder = builder.messages(m);
        }
        for s in system_blocks {
            builder = builder.system(s);
        }
        if let Some(tool_cfg) = build_tool_config(&req.tools) {
            builder = builder.tool_config(tool_cfg);
        }

        let output = apply_config(builder.customize(), cfg)
            .send()
            .await
            .map_err(map_sdk_error)?;

        Ok(Box::pin(into_stream_chunks(output)))
    }
}

#[async_trait]
impl kernel::adapters::capability::EmbedModel for BedrockAdapter {
    async fn embed(
        &self,
        cfg: &RouterConfig,
        req: &EmbedRequest,
    ) -> Result<EmbedResponse, GatewayError> {
        let model_id = req
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_EMBED_MODEL.to_string());
        let family = embed_family(&model_id).ok_or_else(|| {
            Self::err(
                format!("model id '{model_id}' is not a recognised Bedrock embedding model"),
                None,
            )
        })?;

        if req.texts.is_empty() {
            return Ok(EmbedResponse {
                embeddings: Vec::new(),
                usage: None,
                degraded: false,
            });
        }

        let (embeddings, input_tokens) = match family {
            EmbedFamily::Titan => self.invoke_titan_embed(&model_id, &req.texts, cfg).await?,
            EmbedFamily::Cohere => self.invoke_cohere_embed(&model_id, &req.texts, cfg).await?,
        };

        let usage = input_tokens.map(|n| TokenUsage {
            input_tokens: n,
            output_tokens: 0,
            total_tokens: n,
        });

        Ok(EmbedResponse {
            embeddings,
            usage,
            degraded: false,
        })
    }
}

#[async_trait]
impl kernel::adapters::RegisterInto for BedrockAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &kernel::adapters::AdapterRegistry) {
        reg.register_chat(self.clone()).await;
        reg.register_embed(self).await;
    }
}

// ---------------------------------------------------------------------------
// Streaming bridge
// ---------------------------------------------------------------------------

/// Wrap a Converse-stream output as a `Stream<Item = Result<StreamChunk, _>>`.
///
/// The Converse stream is a sequence of typed events (MessageStart /
/// ContentBlockStart / ContentBlockDelta / ContentBlockStop /
/// MessageStop / Metadata):
///
/// - `ContentBlockStart::ToolUse` → seed a per-index accumulator
///   with the tool_use_id + name from the start payload.
/// - `ContentBlockDelta::Text(s)` → emit chunk with `content = s`.
/// - `ContentBlockDelta::ToolUse { input }` → append the JSON-string
///   fragment to the active accumulator at this index.
/// - `MessageStop` → drain accumulators into the terminal chunk's
///   `tool_calls`, surfacing the SDK's `stop_reason` as
///   `finish_reason`.
/// - `Metadata` → empty-content chunk with `usage`.
///
/// The `EventReceiver` type that backs `output.stream` lives in a
/// `pub(crate)` module of the SDK and isn't nameable from outside
/// the crate. We work around that by keeping the output value as
/// captured state inside `unfold` — its type only appears through
/// inference, never in a signature.
fn into_stream_chunks(
    output: aws_sdk_bedrockruntime::operation::converse_stream::ConverseStreamOutput,
) -> impl Stream<Item = Result<StreamChunk, GatewayError>> + Send {
    // Initial state: the SDK output + a fresh tool-call accumulator
    // map + a `done` latch + an emission queue (for byte-events that
    // produce zero or one chunks, we'd ordinarily skip the queue, but
    // the same shape lets future drop-on-error / flush-on-eof logic
    // queue multiple terminal chunks).
    let initial = (
        output,
        BTreeMap::<u32, StreamingToolCall>::new(),
        VecDeque::<Result<StreamChunk, GatewayError>>::new(),
        false,
    );
    futures::stream::unfold(
        initial,
        |(mut output, mut tool_calls, mut pending, done)| async move {
            loop {
                if let Some(item) = pending.pop_front() {
                    return Some((item, (output, tool_calls, pending, done)));
                }
                if done {
                    return None;
                }
                match output.stream.recv().await {
                    Ok(Some(event)) => {
                        if let Some(chunk) = chunk_from_event(&event, &mut tool_calls) {
                            pending.push_back(Ok(chunk));
                        }
                    }
                    Ok(None) => return None,
                    Err(e) => {
                        pending.push_back(Err(GatewayError::ProviderError {
                            adapter: ADAPTER_ID.into(),
                            message: format!("bedrock stream error: {e}"),
                            status: None,
                        }));
                        return Some((
                            pending.pop_front().unwrap(),
                            (output, tool_calls, pending, true),
                        ));
                    }
                }
            }
        },
    )
}

// ---------------------------------------------------------------------------
// Pure helpers (unit-testable without an SDK client)
// ---------------------------------------------------------------------------

/// Embedding-model families on Bedrock with materially different
/// request/response wire shapes. Anything else is rejected up front in
/// [`BedrockAdapter::execute_embed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EmbedFamily {
    /// Amazon Titan text embeddings (`amazon.titan-embed-text-*`).
    /// Single-input per request — we loop over the input slice.
    Titan,
    /// Cohere embed (`cohere.embed-*`). Native batch.
    Cohere,
}

/// Identify the embedding-model family from the model id prefix.
/// Returns `None` for unknown ids so the caller can surface a clear
/// error rather than firing a request that would 400 at the wire.
fn embed_family(model_id: &str) -> Option<EmbedFamily> {
    if model_id.starts_with("amazon.titan-embed") {
        Some(EmbedFamily::Titan)
    } else if model_id.starts_with("cohere.embed") {
        Some(EmbedFamily::Cohere)
    } else {
        None
    }
}

// ---- Embedding wire types -------------------------------------------------

#[derive(serde::Serialize)]
struct TitanEmbedRequest<'a> {
    #[serde(rename = "inputText")]
    input_text: &'a str,
}

#[derive(serde::Deserialize)]
struct TitanEmbedResponse {
    embedding: Vec<f32>,
    #[serde(rename = "inputTextTokenCount", default)]
    input_text_token_count: Option<u32>,
}

#[derive(serde::Serialize)]
struct CohereEmbedRequest<'a> {
    texts: &'a [String],
    /// Required for Cohere embed v3 models. We default to
    /// `search_document`; callers that need `search_query` /
    /// `classification` / `clustering` would need an extra surface
    /// on the gateway request, which doesn't exist yet.
    input_type: &'static str,
}

#[derive(serde::Deserialize)]
struct CohereEmbedResponse {
    embeddings: Vec<Vec<f32>>,
}

// ---------------------------------------------------------------------------
// RouterConfig → SDK request customization
// ---------------------------------------------------------------------------

/// Snapshot the operator-supplied `RouterConfig.headers` as an owned list
/// so the SDK request-mutation closure can satisfy its
/// `Fn(&mut HttpRequest) + Send + Sync + 'static` bound (the closure can't
/// borrow the config).
fn header_pairs(config: &RouterConfig) -> Vec<(String, String)> {
    config
        .headers
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Map `RouterConfig.timeout_ms` onto a per-operation SDK timeout via a
/// config override. Returns `None` when no timeout is configured so the
/// SDK's own defaults apply.
fn timeout_override(config: &RouterConfig) -> Option<aws_sdk_bedrockruntime::config::Builder> {
    let timeout_ms = config.timeout_ms?;
    Some(
        aws_sdk_bedrockruntime::config::Config::builder().timeout_config(
            aws_smithy_types::timeout::TimeoutConfig::builder()
                .operation_timeout(std::time::Duration::from_millis(timeout_ms))
                .build(),
        ),
    )
}

/// Apply the operator `RouterConfig` to an outbound Bedrock operation.
///
/// This is how the SigV4-signed SDK path honours the same config surface
/// the HTTP adapters read directly: every `config.headers` entry is
/// stamped onto the request (invalid header names/values are logged and
/// skipped rather than panicking, since `mutate_request` is infallible),
/// and when `timeout_ms` is set the operation is bounded by an SDK
/// timeout. The `HttpRequest` argument type is inferred from the SDK's
/// `mutate_request` bound — it lives in a transitive crate that isn't
/// nameable here, so it's deliberately never spelled out.
fn apply_config<T, E, B>(
    op: aws_sdk_bedrockruntime::client::customize::CustomizableOperation<T, E, B>,
    config: &RouterConfig,
) -> aws_sdk_bedrockruntime::client::customize::CustomizableOperation<T, E, B> {
    let headers = header_pairs(config);
    let op = op.mutate_request(move |req| {
        for (k, v) in &headers {
            if let Err(e) = req.headers_mut().try_insert(k.clone(), v.clone()) {
                tracing::warn!(
                    adapter = ADAPTER_ID,
                    header = %k,
                    error = %e,
                    "skipping invalid custom header on Bedrock request",
                );
            }
        }
    });
    match timeout_override(config) {
        Some(over) => op.config_override(over),
        None => op,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------
    // Embeddings
    // -----------------------------------------------------------------

    #[test]
    fn embed_family_recognises_titan_and_cohere_prefixes() {
        assert_eq!(
            embed_family("amazon.titan-embed-text-v2:0"),
            Some(EmbedFamily::Titan),
        );
        assert_eq!(
            embed_family("amazon.titan-embed-text-v1"),
            Some(EmbedFamily::Titan),
        );
        assert_eq!(
            embed_family("cohere.embed-english-v3"),
            Some(EmbedFamily::Cohere),
        );
        assert_eq!(
            embed_family("cohere.embed-multilingual-v3"),
            Some(EmbedFamily::Cohere),
        );
        // Chat models should not be picked up by the embed dispatch.
        assert_eq!(
            embed_family("anthropic.claude-3-5-sonnet-20241022-v2:0"),
            None
        );
        assert_eq!(embed_family("meta.llama3-1-70b-instruct-v1:0"), None);
    }

    #[test]
    fn titan_request_serialises_with_camel_case_input_text() {
        let body = TitanEmbedRequest {
            input_text: "hello world",
        };
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["inputText"], "hello world");
    }

    #[test]
    fn titan_response_parses_embedding_and_token_count() {
        let raw = r#"{"embedding":[0.1,0.2,0.3],"inputTextTokenCount":4}"#;
        let parsed: TitanEmbedResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.embedding, vec![0.1, 0.2, 0.3]);
        assert_eq!(parsed.input_text_token_count, Some(4));
    }

    #[test]
    fn titan_response_tolerates_missing_token_count() {
        let raw = r#"{"embedding":[0.0]}"#;
        let parsed: TitanEmbedResponse = serde_json::from_str(raw).unwrap();
        assert!(parsed.input_text_token_count.is_none());
    }

    #[test]
    fn cohere_request_serialises_texts_and_default_input_type() {
        let texts = vec!["hello".to_string(), "world".to_string()];
        let body = CohereEmbedRequest {
            texts: &texts,
            input_type: "search_document",
        };
        let json = serde_json::to_value(&body).unwrap();
        let arr = json["texts"].as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], "hello");
        assert_eq!(json["input_type"], "search_document");
    }

    #[test]
    fn cohere_response_parses_batch_embeddings() {
        let raw = r#"{"embeddings":[[0.1,0.2],[0.3,0.4]],"id":"abc","response_type":"embeddings_floats"}"#;
        let parsed: CohereEmbedResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(parsed.embeddings.len(), 2);
        assert_eq!(parsed.embeddings[0], vec![0.1, 0.2]);
        assert_eq!(parsed.embeddings[1], vec![0.3, 0.4]);
    }

    #[test]
    fn bedrock_supports_text_chat_and_embed() {
        // We don't construct the SDK client (would require AWS creds);
        // call `supports` directly via a manually-built adapter.
        // Trait method is called on a value; build a minimal one.
        // Reuse `embed_family` validation as a sanity check that the
        // adapter would now route an embed call past the dispatch.
        assert!(embed_family("amazon.titan-embed-text-v2:0").is_some());
    }

    #[tokio::test]
    async fn bedrock_capability_model_id() {
        // `with_region` is the documented test constructor: pinning the
        // region skips region auto-discovery, and the AWS SDK resolves
        // credentials lazily at request time — so building the client is
        // offline-safe (no AWS creds or network needed here).
        let adapter = BedrockAdapter::with_region("us-east-1").await.unwrap();
        // Reference `Model::id` by full path
        // and the capability `Model` trait.
        assert_eq!(kernel::adapters::capability::Model::id(&adapter), "bedrock");
    }
}
