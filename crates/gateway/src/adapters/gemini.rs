//! Google Gemini adapter.
//!
//! Talks the Gemini generative-language REST API at
//! `https://generativelanguage.googleapis.com/v1beta`. Unlike OpenAI- /
//! Anthropic-compatible providers, Gemini has its own wire format:
//!
//! - Role names: `user` / `model` (not `assistant`)
//! - Messages are nested as `{role, parts: [{text}]}`
//! - System prompt is a top-level `systemInstruction` field, not a message
//! - Auth is `x-goog-api-key: <key>` header (or `?key=<key>` query
//!   string; header is preferred)
//! - Endpoint encodes the model id: `/models/{id}:generateContent`
//! - Embeddings use a separate endpoint with `embedding.values`
//!
//! Streaming uses `:streamGenerateContent?alt=sse` and Server-Sent
//! Events. Each SSE `data:` payload is the same JSON shape as a non-
//! streaming response — a list of candidates whose `parts[].text` is
//! an incremental delta — so the chunk parser reuses
//! [`GeminiChatResponse`] plus a per-candidate `finishReason` field.
//! The final chunk carries `usageMetadata` and `finishReason: "STOP"`.

use std::collections::HashMap;
use std::pin::Pin;

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::adapters::InferenceAdapter;
use crate::adapters::base::{build_client, resolve_api_key};
use crate::types::capability::Capability;
use crate::types::config::RouterConfig;
use crate::types::cost::TokenUsage;
use crate::types::error::GatewayError;
use crate::types::request::{
    InferenceRequest, InferenceResponse, MediaAttachment, MediaSource, Message, MessageContent,
    MessageRole, Payload, StreamChunk, ToolCall, ToolDefinition,
};

const ADAPTER_ID: &str = "gemini";
const DEFAULT_MODEL: &str = "gemini-2.0-flash";
const DEFAULT_EMBED_MODEL: &str = "text-embedding-004";
const DEFAULT_MAX_TOKENS: u32 = 1024;

// ---------------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------------

/// A single part of a Gemini message. Each part holds exactly one of
/// the variants below — text, a function call (assistant emits when
/// invoking a tool), a function response (caller sends back a tool
/// result), an inline media payload (base64), or a file reference
/// (URL). Gemini accepts the flat shape with absent fields omitted.
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
struct GeminiPart {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    text: Option<String>,
    #[serde(
        default,
        rename = "functionCall",
        skip_serializing_if = "Option::is_none"
    )]
    function_call: Option<GeminiFunctionCall>,
    #[serde(
        default,
        rename = "functionResponse",
        skip_serializing_if = "Option::is_none"
    )]
    function_response: Option<GeminiFunctionResponse>,
    /// Inline base64-encoded media payload (images today; audio /
    /// video for models that accept them).
    #[serde(
        default,
        rename = "inlineData",
        skip_serializing_if = "Option::is_none"
    )]
    inline_data: Option<GeminiInlineData>,
    /// External file reference. `file_uri` is typically a `gs://`
    /// Cloud Storage URI; HTTPS URLs work only when the resource is
    /// publicly accessible to Gemini at request time.
    #[serde(
        default,
        rename = "fileData",
        skip_serializing_if = "Option::is_none"
    )]
    file_data: Option<GeminiFileData>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct GeminiInlineData {
    #[serde(rename = "mimeType")]
    mime_type: String,
    data: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct GeminiFileData {
    #[serde(default, rename = "mimeType", skip_serializing_if = "Option::is_none")]
    mime_type: Option<String>,
    #[serde(rename = "fileUri")]
    file_uri: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct GeminiFunctionCall {
    name: String,
    /// Argument object — a JSON value, not a string. We round-trip
    /// this against the gateway's JSON-string `ToolCall::arguments`.
    args: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct GeminiFunctionResponse {
    name: String,
    /// Caller's response to a prior function call. Must be a JSON
    /// object — if the gateway tool result is a JSON document we use
    /// it directly, otherwise we wrap it as `{"content": <str>}`.
    response: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct GeminiContent {
    role: &'static str,
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Serialize)]
struct GeminiSystemInstruction {
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Serialize, Default)]
struct GeminiGenerationConfig {
    #[serde(rename = "maxOutputTokens", skip_serializing_if = "Option::is_none")]
    max_output_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
}

#[derive(Debug, Serialize)]
struct GeminiChatRequest {
    contents: Vec<GeminiContent>,
    #[serde(rename = "systemInstruction", skip_serializing_if = "Option::is_none")]
    system_instruction: Option<GeminiSystemInstruction>,
    #[serde(rename = "generationConfig", skip_serializing_if = "Option::is_none")]
    generation_config: Option<GeminiGenerationConfig>,
    /// Tool definitions for this turn. Gemini wraps function lists
    /// inside `{functionDeclarations: [...]}` envelopes — typically
    /// one envelope per request. We always emit a single envelope.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<GeminiTool>,
}

#[derive(Debug, Serialize)]
struct GeminiTool {
    #[serde(rename = "functionDeclarations")]
    function_declarations: Vec<GeminiFunctionDeclaration>,
}

#[derive(Debug, Serialize)]
struct GeminiFunctionDeclaration {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    /// JSON Schema describing the call's arguments object.
    parameters: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct GeminiChatResponse {
    #[serde(default)]
    candidates: Vec<GeminiCandidate>,
    #[serde(rename = "usageMetadata", default)]
    usage_metadata: Option<GeminiUsageMetadata>,
}

#[derive(Debug, Deserialize)]
struct GeminiCandidate {
    #[serde(default)]
    content: Option<GeminiResponseContent>,
    /// Streaming responses set this on the final chunk (`"STOP"`,
    /// `"MAX_TOKENS"`, `"SAFETY"`, …). Non-streaming responses also
    /// emit it on the single candidate.
    #[serde(rename = "finishReason", default)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GeminiResponseContent {
    /// Response parts share the same wire shape as the parts we emit —
    /// reuse [`GeminiPart`] so tool-use blocks deserialise alongside
    /// text without a second type.
    #[serde(default)]
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Deserialize)]
struct GeminiUsageMetadata {
    #[serde(rename = "promptTokenCount", default)]
    prompt_token_count: u32,
    #[serde(rename = "candidatesTokenCount", default)]
    candidates_token_count: u32,
    #[serde(rename = "totalTokenCount", default)]
    total_token_count: u32,
}

#[derive(Debug, Serialize)]
struct GeminiEmbedRequestItem {
    model: String,
    content: GeminiContent,
}

#[derive(Debug, Serialize)]
struct GeminiBatchEmbedRequest {
    requests: Vec<GeminiEmbedRequestItem>,
}

#[derive(Debug, Deserialize)]
struct GeminiBatchEmbedResponse {
    #[serde(default)]
    embeddings: Vec<GeminiEmbeddingValues>,
}

#[derive(Debug, Deserialize)]
struct GeminiEmbeddingValues {
    #[serde(default)]
    values: Vec<f32>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Gemini uses `user` / `model` rather than OpenAI/Anthropic's
/// `user` / `assistant`. Tool and system messages get mapped to `user`
/// (system is hoisted into `systemInstruction` separately).
fn gemini_role(role: &MessageRole) -> &'static str {
    match role {
        MessageRole::Assistant => "model",
        MessageRole::User | MessageRole::Tool | MessageRole::System => "user",
    }
}

/// Convert gateway messages into Gemini `contents`, skipping system-role
/// messages (which are hoisted into `systemInstruction` by the caller).
///
/// Each gateway message becomes one [`GeminiContent`]. The parts list
/// composes text + function-call / function-response blocks from
/// `Message.content` and `Message.tool_calls`. Empty text bodies are
/// elided so a pure-tool-call assistant turn doesn't ship an empty
/// text part.
fn build_contents(messages: &[Message]) -> Vec<GeminiContent> {
    messages
        .iter()
        .filter(|m| m.role != MessageRole::System)
        .map(|m| GeminiContent {
            role: gemini_role(&m.role),
            parts: build_parts(m),
        })
        .collect()
}

/// Compose the [`GeminiPart`] list for one gateway message.
///
/// Order: text first (when non-empty), then one part per
/// [`MediaAttachment`], then any tool-call parts. Tool-result
/// messages emit a single functionResponse part and skip the rest.
fn build_parts(m: &Message) -> Vec<GeminiPart> {
    let mut parts: Vec<GeminiPart> = Vec::new();
    match &m.content {
        MessageContent::Text { text } => {
            if !text.is_empty() {
                parts.push(GeminiPart {
                    text: Some(text.clone()),
                    ..Default::default()
                });
            }
            for att in &m.attachments {
                if let Some(part) = attachment_to_part(att) {
                    parts.push(part);
                }
            }
        }
        MessageContent::ToolResult {
            tool_call_id,
            content,
        } => {
            parts.push(GeminiPart {
                function_response: Some(GeminiFunctionResponse {
                    // Gemini matches results back to calls by function
                    // name, not id. The gateway id may have a "#N"
                    // disambiguator suffix added on the way in; strip
                    // it to recover the original function name.
                    name: function_name_from_tool_call_id(tool_call_id),
                    response: tool_result_response_value(content),
                }),
                ..Default::default()
            });
        }
    }
    for tc in &m.tool_calls {
        parts.push(GeminiPart {
            function_call: Some(GeminiFunctionCall {
                name: tc.name.clone(),
                args: parse_tool_input(&tc.arguments),
            }),
            ..Default::default()
        });
    }
    parts
}

/// Translate a gateway [`MediaAttachment`] into a Gemini part. Base64
/// sources go via `inlineData`; URL sources via `fileData.fileUri`
/// (Gemini fetches the URI on its side — works for `gs://` Cloud
/// Storage objects and publicly accessible HTTPS URLs). Returns
/// `None` for variants we don't yet model.
fn attachment_to_part(att: &MediaAttachment) -> Option<GeminiPart> {
    match att {
        MediaAttachment::Image { source, mime_type } => match source {
            MediaSource::Base64 { data } => Some(GeminiPart {
                inline_data: Some(GeminiInlineData {
                    // Gemini requires mimeType on inline data. We
                    // default to image/jpeg when unspecified — same
                    // conservative choice the OpenAI / Anthropic
                    // adapters make for the equivalent shape.
                    mime_type: mime_type
                        .clone()
                        .unwrap_or_else(|| "image/jpeg".to_string()),
                    data: data.clone(),
                }),
                ..Default::default()
            }),
            MediaSource::Url { url } => Some(GeminiPart {
                file_data: Some(GeminiFileData {
                    mime_type: mime_type.clone(),
                    file_uri: url.clone(),
                }),
                ..Default::default()
            }),
        },
    }
}

/// Strip any `#N` disambiguator suffix that `extract_tool_calls`
/// appends when the model emits the same function name multiple times
/// in one turn. A raw function name (no suffix) round-trips unchanged.
fn function_name_from_tool_call_id(id: &str) -> String {
    id.split('#').next().unwrap_or(id).to_string()
}

/// Build the JSON value Gemini expects in a `functionResponse.response`
/// field. Gemini wants an object — if the gateway tool result is
/// already a JSON document we surface it directly; otherwise wrap it
/// as `{"content": <string>}` so the model still sees structured data.
fn tool_result_response_value(content: &str) -> serde_json::Value {
    serde_json::from_str::<serde_json::Value>(content)
        .ok()
        .filter(|v| v.is_object() || v.is_array())
        .unwrap_or_else(|| serde_json::json!({ "content": content }))
}

/// Parse a JSON-string `arguments` payload back into a JSON value for
/// Gemini's `functionCall.args` field. Malformed input degrades to an
/// empty object — Gemini would reject a string here, and dropping the
/// args is safer than panicking.
fn parse_tool_input(args: &str) -> serde_json::Value {
    if args.is_empty() {
        return serde_json::json!({});
    }
    serde_json::from_str(args).unwrap_or_else(|_| serde_json::json!({}))
}

/// Convert gateway [`ToolDefinition`]s into Gemini's
/// `tools: [{functionDeclarations: [...]}]` envelope. We always emit
/// a single envelope — Gemini does support multiple, but the gateway
/// model is a flat list of definitions per request.
fn build_tools(tools: &[ToolDefinition]) -> Vec<GeminiTool> {
    if tools.is_empty() {
        return Vec::new();
    }
    vec![GeminiTool {
        function_declarations: tools
            .iter()
            .map(|t| GeminiFunctionDeclaration {
                name: t.name.clone(),
                description: t.description.clone(),
                parameters: t.input_schema.clone(),
            })
            .collect(),
    }]
}

/// Pull `functionCall` parts out of a [`GeminiChatResponse`] and
/// convert them into gateway [`ToolCall`]s. Gemini doesn't carry a
/// per-call id — we synthesise one from the function name. When the
/// same function name appears more than once in a turn, the second
/// and later calls get a `#N` suffix so the caller can disambiguate;
/// [`function_name_from_tool_call_id`] strips that on the way out.
/// Arguments (a JSON object on the wire) are re-serialised into the
/// gateway's JSON-string `ToolCall::arguments` form for parity with
/// OpenAI / Anthropic.
fn extract_tool_calls(resp: &GeminiChatResponse) -> Vec<ToolCall> {
    let mut out: Vec<ToolCall> = Vec::new();
    let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for candidate in &resp.candidates {
        let Some(content) = &candidate.content else {
            continue;
        };
        for part in &content.parts {
            let Some(fc) = &part.function_call else {
                continue;
            };
            let n = counts.entry(fc.name.clone()).or_insert(0);
            let id = if *n == 0 {
                fc.name.clone()
            } else {
                format!("{}#{}", fc.name, *n)
            };
            *n += 1;
            let arguments = serde_json::to_string(&fc.args).unwrap_or_default();
            out.push(ToolCall {
                id,
                name: fc.name.clone(),
                arguments,
            });
        }
    }
    out
}

/// Resolve the system instruction: prefer the explicit `system` field on
/// the Chat payload; fall back to concatenating any `MessageRole::System`
/// messages in the conversation.
fn extract_system_instruction(
    messages: &[Message],
    system: &Option<String>,
) -> Option<GeminiSystemInstruction> {
    if let Some(s) = system.as_deref() {
        return Some(GeminiSystemInstruction {
            parts: vec![GeminiPart {
                text: Some(s.to_string()),
                ..Default::default()
            }],
        });
    }
    let system_parts: Vec<String> = messages
        .iter()
        .filter(|m| m.role == MessageRole::System)
        .map(|m| m.as_text().to_string())
        .collect();
    if system_parts.is_empty() {
        None
    } else {
        Some(GeminiSystemInstruction {
            parts: system_parts
                .into_iter()
                .map(|text| GeminiPart {
                    text: Some(text),
                    ..Default::default()
                })
                .collect(),
        })
    }
}

/// Strip a leading `models/` prefix so callers can specify either form.
fn normalize_model_id(model: &str) -> &str {
    model.strip_prefix("models/").unwrap_or(model)
}

fn resolve_chat_model(request: &InferenceRequest) -> String {
    let raw = request.model.as_deref().unwrap_or(DEFAULT_MODEL);
    normalize_model_id(raw).to_string()
}

fn resolve_embed_model(request: &InferenceRequest) -> String {
    let raw = request.model.as_deref().unwrap_or(DEFAULT_EMBED_MODEL);
    normalize_model_id(raw).to_string()
}

fn extract_text(resp: &GeminiChatResponse) -> String {
    resp.candidates
        .iter()
        .filter_map(|c| c.content.as_ref())
        .flat_map(|c| c.parts.iter())
        .filter_map(|p| p.text.as_deref())
        .collect::<Vec<_>>()
        .join("")
}

fn usage_from_gemini(usage: &Option<GeminiUsageMetadata>) -> Option<TokenUsage> {
    usage.as_ref().map(|u| TokenUsage {
        input_tokens: u.prompt_token_count,
        output_tokens: u.candidates_token_count,
        total_tokens: if u.total_token_count > 0 {
            u.total_token_count
        } else {
            u.prompt_token_count + u.candidates_token_count
        },
    })
}

/// Run a JSON POST against a Gemini endpoint with the `x-goog-api-key`
/// header and a content-type of `application/json`, applying any
/// `RouterConfig.headers` overrides. Returns the parsed JSON or maps the
/// HTTP status to the right [`GatewayError`] variant.
async fn gemini_post<Req: Serialize, Resp: serde::de::DeserializeOwned>(
    client: &Client,
    url: &str,
    api_key: &str,
    body: &Req,
    extra_headers: &HashMap<String, String>,
) -> Result<Resp, GatewayError> {
    let mut req = client
        .post(url)
        .header("x-goog-api-key", api_key)
        .header("content-type", "application/json")
        .json(body);
    for (k, v) in extra_headers {
        req = req.header(k.as_str(), v.as_str());
    }
    let resp = req.send().await?;
    let status = resp.status();
    if !status.is_success() {
        let body_text = resp.text().await.unwrap_or_default();
        return Err(match status.as_u16() {
            401 | 403 => GatewayError::Authentication {
                adapter: ADAPTER_ID.into(),
                message: body_text,
            },
            429 => GatewayError::RateLimit {
                adapter: ADAPTER_ID.into(),
                retry_after_ms: None,
            },
            _ => GatewayError::ProviderError {
                adapter: ADAPTER_ID.into(),
                message: body_text,
                status: Some(status.as_u16()),
            },
        });
    }
    resp.json::<Resp>().await.map_err(|e| GatewayError::ProviderError {
        adapter: ADAPTER_ID.into(),
        message: format!("failed to parse response: {e}"),
        status: Some(status.as_u16()),
    })
}

/// Parse a single SSE `data: …` line emitted by
/// `:streamGenerateContent?alt=sse` into a [`StreamChunk`].
///
/// Returns `None` for empty / non-`data:` lines (keep-alive comments,
/// blank separators). Returns `Some(Err(…))` if the line looks like a
/// `data:` payload but the JSON does not parse — the caller surfaces
/// this as a provider error so a malformed stream is not silently
/// dropped.
///
/// Gemini sends the same payload shape as a non-streaming response, so
/// the parser reuses [`GeminiChatResponse`]. The chunk's `content` is
/// the concatenation of text parts across all candidates (matching
/// [`extract_text`] semantics); `finish_reason` is taken from the
/// first candidate that carries one. `usage` is sourced from
/// `usageMetadata`, which Gemini only sets on the terminal chunk.
///
/// Tool calling: unlike OpenAI / Anthropic / Bedrock, Gemini does
/// **not** fragment `functionCall.args` across stream chunks — each
/// `functionCall` part arrives complete, typically on the same
/// terminal chunk that carries `finishReason`. So no per-call
/// accumulator is needed; [`extract_tool_calls`] runs directly on
/// each parsed chunk and the resulting `ToolCall`s ride on whatever
/// `StreamChunk` carries them.
fn parse_stream_line(line: &str) -> Option<Result<StreamChunk, GatewayError>> {
    let line = line.trim();
    let payload = line.strip_prefix("data:")?.trim();
    if payload.is_empty() {
        return None;
    }
    let parsed: GeminiChatResponse = match serde_json::from_str(payload) {
        Ok(v) => v,
        Err(e) => {
            return Some(Err(GatewayError::ProviderError {
                adapter: ADAPTER_ID.into(),
                message: format!("failed to parse SSE chunk: {e}"),
                status: None,
            }));
        }
    };
    let content = extract_text(&parsed);
    let finish_reason = parsed
        .candidates
        .iter()
        .find_map(|c| c.finish_reason.clone());
    let usage = usage_from_gemini(&parsed.usage_metadata);
    let tool_calls = extract_tool_calls(&parsed);
    Some(Ok(StreamChunk {
        content,
        finish_reason,
        usage,
        tool_calls,
    }))
}

// ---------------------------------------------------------------------------
// Adapter
// ---------------------------------------------------------------------------

pub struct GeminiAdapter {
    client: Client,
}

impl GeminiAdapter {
    pub fn new() -> Result<Self, GatewayError> {
        Ok(Self {
            client: Client::new(),
        })
    }

    pub fn from_config(config: &RouterConfig) -> Result<Self, GatewayError> {
        Ok(Self {
            client: build_client(config)?,
        })
    }

    fn missing_key_err() -> GatewayError {
        GatewayError::Authentication {
            adapter: ADAPTER_ID.into(),
            message: "missing API key — set the env var specified in api_key_env (e.g. GEMINI_API_KEY)"
                .into(),
        }
    }
}

#[async_trait]
impl InferenceAdapter for GeminiAdapter {
    fn id(&self) -> &str {
        ADAPTER_ID
    }

    fn supports(&self, capability: &Capability) -> bool {
        matches!(capability, Capability::TextChat | Capability::TextEmbed)
    }

    async fn execute(
        &self,
        config: &RouterConfig,
        request: &InferenceRequest,
    ) -> Result<InferenceResponse, GatewayError> {
        let api_key = resolve_api_key(config).ok_or_else(Self::missing_key_err)?;

        match &request.payload {
            Payload::Chat {
                messages,
                system,
                max_tokens,
                temperature,
                tools,
            } => {
                let model = resolve_chat_model(request);
                let url = format!(
                    "{}/models/{}:generateContent",
                    config.url.trim_end_matches('/'),
                    model
                );

                let generation_config = (max_tokens.is_some() || temperature.is_some()).then(|| {
                    GeminiGenerationConfig {
                        max_output_tokens: Some(max_tokens.unwrap_or(DEFAULT_MAX_TOKENS)),
                        temperature: *temperature,
                    }
                });

                let body = GeminiChatRequest {
                    contents: build_contents(messages),
                    system_instruction: extract_system_instruction(messages, system),
                    generation_config,
                    tools: build_tools(tools),
                };

                let resp: GeminiChatResponse =
                    gemini_post(&self.client, &url, &api_key, &body, &config.headers).await?;

                let content = extract_text(&resp);
                let tool_calls = extract_tool_calls(&resp);
                let usage = usage_from_gemini(&resp.usage_metadata);

                Ok(InferenceResponse {
                    success: true,
                    content: if content.is_empty() { None } else { Some(content) },
                    embeddings: None,
                    transcription: None,
                    audio: None,
                    images: None,
                    videos: None,
                    model: Some(model),
                    usage,
                    tool_calls,
                    estimated_cost: None,
                    actual_cost: None,
                    attempts: vec![],
                })
            }

            Payload::Embed { texts } => {
                if texts.is_empty() {
                    return Ok(InferenceResponse {
                        success: true,
                        content: None,
                        embeddings: Some(vec![]),
                        transcription: None,
                        audio: None,
                        images: None,
                        videos: None,
                        model: request.model.clone(),
                        tool_calls: Vec::new(),
                        usage: None,
                        estimated_cost: None,
                        actual_cost: None,
                        attempts: vec![],
                    });
                }
                let model = resolve_embed_model(request);
                let url = format!(
                    "{}/models/{}:batchEmbedContents",
                    config.url.trim_end_matches('/'),
                    model
                );
                let qualified_model = format!("models/{model}");
                let body = GeminiBatchEmbedRequest {
                    requests: texts
                        .iter()
                        .map(|t| GeminiEmbedRequestItem {
                            model: qualified_model.clone(),
                            content: GeminiContent {
                                role: "user",
                                parts: vec![GeminiPart {
                                    text: Some(t.clone()),
                                    ..Default::default()
                                }],
                            },
                        })
                        .collect(),
                };

                let resp: GeminiBatchEmbedResponse =
                    gemini_post(&self.client, &url, &api_key, &body, &config.headers).await?;

                let embeddings: Vec<Vec<f32>> =
                    resp.embeddings.into_iter().map(|e| e.values).collect();

                Ok(InferenceResponse {
                    success: true,
                    content: None,
                    embeddings: Some(embeddings),
                    transcription: None,
                    audio: None,
                    images: None,
                    videos: None,
                    model: Some(model),
                    usage: None,
                    tool_calls: Vec::new(),
                    estimated_cost: None,
                    actual_cost: None,
                    attempts: vec![],
                })
            }

            _ => Err(GatewayError::ProviderError {
                adapter: ADAPTER_ID.into(),
                message: "Gemini supports Payload::Chat and Payload::Embed only".into(),
                status: None,
            }),
        }
    }

    async fn stream(
        &self,
        config: &RouterConfig,
        request: &InferenceRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        let Payload::Chat {
            messages,
            system,
            max_tokens,
            temperature,
            tools,
        } = &request.payload
        else {
            return Err(GatewayError::ProviderError {
                adapter: ADAPTER_ID.into(),
                message: "Gemini streaming is only supported for Payload::Chat".into(),
                status: None,
            });
        };

        let api_key = resolve_api_key(config).ok_or_else(Self::missing_key_err)?;
        let model = resolve_chat_model(request);
        let url = format!(
            "{}/models/{}:streamGenerateContent?alt=sse",
            config.url.trim_end_matches('/'),
            model
        );

        let generation_config = (max_tokens.is_some() || temperature.is_some()).then(|| {
            GeminiGenerationConfig {
                max_output_tokens: Some(max_tokens.unwrap_or(DEFAULT_MAX_TOKENS)),
                temperature: *temperature,
            }
        });

        let body = GeminiChatRequest {
            contents: build_contents(messages),
            system_instruction: extract_system_instruction(messages, system),
            generation_config,
            tools: build_tools(tools),
        };

        let mut req = self
            .client
            .post(&url)
            .header("x-goog-api-key", &api_key)
            .header("content-type", "application/json")
            .json(&body);
        for (k, v) in &config.headers {
            req = req.header(k.as_str(), v.as_str());
        }

        let response = req.send().await?;
        let status = response.status();
        if !status.is_success() {
            let body_text = response.text().await.unwrap_or_default();
            return Err(match status.as_u16() {
                401 | 403 => GatewayError::Authentication {
                    adapter: ADAPTER_ID.into(),
                    message: body_text,
                },
                429 => GatewayError::RateLimit {
                    adapter: ADAPTER_ID.into(),
                    retry_after_ms: None,
                },
                _ => GatewayError::ProviderError {
                    adapter: ADAPTER_ID.into(),
                    message: body_text,
                    status: Some(status.as_u16()),
                },
            });
        }

        let byte_stream = response.bytes_stream();
        let stream = byte_stream
            .map(|result| -> Result<Vec<Result<StreamChunk, GatewayError>>, GatewayError> {
                let bytes = result?;
                let text = String::from_utf8_lossy(&bytes);
                Ok(text.lines().filter_map(parse_stream_line).collect())
            })
            .map(|result| match result {
                Ok(chunks) => futures::stream::iter(chunks),
                Err(e) => futures::stream::iter(vec![Err(e)]),
            })
            .flatten();

        Ok(Box::pin(stream))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn user(content: &str) -> Message {
        Message::text(MessageRole::User, content)
    }

    fn assistant(content: &str) -> Message {
        Message::text(MessageRole::Assistant, content)
    }

    fn system_msg(content: &str) -> Message {
        Message::text(MessageRole::System, content)
    }

    #[test]
    fn id_and_supports_match_chat_and_embed() {
        let a = GeminiAdapter::new().unwrap();
        assert_eq!(a.id(), "gemini");
        assert!(a.supports(&Capability::TextChat));
        assert!(a.supports(&Capability::TextEmbed));
        assert!(!a.supports(&Capability::ImageGenerate));
        assert!(!a.supports(&Capability::VideoGenerate));
    }

    #[test]
    fn gemini_role_maps_assistant_to_model_and_others_to_user() {
        assert_eq!(gemini_role(&MessageRole::User), "user");
        assert_eq!(gemini_role(&MessageRole::Assistant), "model");
        assert_eq!(gemini_role(&MessageRole::Tool), "user");
        assert_eq!(gemini_role(&MessageRole::System), "user");
    }

    #[test]
    fn build_contents_skips_system_messages_and_uses_gemini_role_names() {
        let msgs = vec![
            system_msg("be concise"),
            user("hi"),
            assistant("hello"),
            user("how are you"),
        ];
        let contents = build_contents(&msgs);
        assert_eq!(contents.len(), 3, "system message should be skipped");
        assert_eq!(contents[0].role, "user");
        assert_eq!(contents[1].role, "model");
        assert_eq!(contents[2].role, "user");
        assert_eq!(contents[0].parts[0].text.as_deref(), Some("hi"));
    }

    #[test]
    fn extract_system_prefers_explicit_field_over_message_role() {
        let msgs = vec![system_msg("inline rules"), user("hi")];
        let explicit = Some("explicit rules".to_string());
        let si = extract_system_instruction(&msgs, &explicit).unwrap();
        assert_eq!(si.parts.len(), 1);
        assert_eq!(si.parts[0].text.as_deref(), Some("explicit rules"));
    }

    #[test]
    fn extract_system_falls_back_to_system_role_messages_when_no_explicit_field() {
        let msgs = vec![system_msg("rule one"), system_msg("rule two"), user("hi")];
        let si = extract_system_instruction(&msgs, &None).unwrap();
        assert_eq!(si.parts.len(), 2);
        assert_eq!(si.parts[0].text.as_deref(), Some("rule one"));
        assert_eq!(si.parts[1].text.as_deref(), Some("rule two"));
    }

    #[test]
    fn extract_system_returns_none_when_no_system_present() {
        let msgs = vec![user("hi")];
        assert!(extract_system_instruction(&msgs, &None).is_none());
    }

    #[test]
    fn normalize_model_id_strips_models_prefix_when_present() {
        assert_eq!(normalize_model_id("gemini-2.0-flash"), "gemini-2.0-flash");
        assert_eq!(
            normalize_model_id("models/gemini-2.0-flash"),
            "gemini-2.0-flash"
        );
        assert_eq!(
            normalize_model_id("text-embedding-004"),
            "text-embedding-004"
        );
    }

    #[test]
    fn resolve_chat_model_uses_default_when_request_omits_model() {
        let req = InferenceRequest {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: None,
            payload: Payload::Chat {
                messages: vec![user("hi")],
                system: None,
                max_tokens: None,
                temperature: None,
                tools: Vec::new(),
            },
            budget: None,
        };
        assert_eq!(resolve_chat_model(&req), DEFAULT_MODEL);
    }

    #[test]
    fn extract_text_concatenates_parts_across_candidates() {
        let resp = GeminiChatResponse {
            candidates: vec![GeminiCandidate {
                content: Some(GeminiResponseContent {
                    parts: vec![
                        GeminiPart {
                            text: Some("hello".into()),
                            ..Default::default()
                        },
                        GeminiPart::default(),
                        GeminiPart {
                            text: Some(" world".into()),
                            ..Default::default()
                        },
                    ],
                }),
                finish_reason: None,
            }],
            usage_metadata: None,
        };
        assert_eq!(extract_text(&resp), "hello world");
    }

    #[test]
    fn usage_from_gemini_computes_total_when_field_is_zero() {
        let u = Some(GeminiUsageMetadata {
            prompt_token_count: 3,
            candidates_token_count: 5,
            total_token_count: 0,
        });
        let got = usage_from_gemini(&u).unwrap();
        assert_eq!(got.input_tokens, 3);
        assert_eq!(got.output_tokens, 5);
        assert_eq!(got.total_tokens, 8);
    }

    #[test]
    fn usage_from_gemini_uses_explicit_total_when_provided() {
        let u = Some(GeminiUsageMetadata {
            prompt_token_count: 3,
            candidates_token_count: 5,
            total_token_count: 9, // includes overhead the response decided to attribute
        });
        let got = usage_from_gemini(&u).unwrap();
        assert_eq!(got.total_tokens, 9);
    }

    #[test]
    fn missing_api_key_surfaces_authentication_error() {
        let err = GeminiAdapter::missing_key_err();
        assert!(matches!(err, GatewayError::Authentication { .. }));
    }

    #[test]
    fn parse_stream_line_returns_none_for_empty_or_non_data_lines() {
        assert!(parse_stream_line("").is_none());
        assert!(parse_stream_line("   ").is_none());
        assert!(parse_stream_line(": keep-alive comment").is_none());
        // `data:` with empty payload (Gemini sometimes emits these
        // between events) should be skipped, not parsed.
        assert!(parse_stream_line("data:").is_none());
        assert!(parse_stream_line("data:   ").is_none());
    }

    #[test]
    fn parse_stream_line_extracts_incremental_text_from_candidate_parts() {
        let line = r#"data: {"candidates":[{"content":{"parts":[{"text":"Hello"}],"role":"model"},"index":0}]}"#;
        let chunk = parse_stream_line(line).expect("Some").expect("Ok");
        assert_eq!(chunk.content, "Hello");
        assert!(chunk.finish_reason.is_none());
        assert!(chunk.usage.is_none());
    }

    #[test]
    fn parse_stream_line_carries_finish_reason_on_terminal_chunk() {
        let line = r#"data: {"candidates":[{"content":{"parts":[{"text":"."}],"role":"model"},"finishReason":"STOP","index":0}],"usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":6,"totalTokenCount":10}}"#;
        let chunk = parse_stream_line(line).expect("Some").expect("Ok");
        assert_eq!(chunk.content, ".");
        assert_eq!(chunk.finish_reason.as_deref(), Some("STOP"));
        let usage = chunk.usage.expect("usage present on terminal chunk");
        assert_eq!(usage.input_tokens, 4);
        assert_eq!(usage.output_tokens, 6);
        assert_eq!(usage.total_tokens, 10);
    }

    #[test]
    fn parse_stream_line_yields_provider_error_on_bad_json() {
        let line = "data: {not json";
        let err = parse_stream_line(line)
            .expect("Some")
            .expect_err("Err on bad json");
        assert!(matches!(err, GatewayError::ProviderError { .. }));
    }

    #[test]
    fn parse_stream_line_handles_data_payload_without_space_after_colon() {
        // SSE allows `data:payload` (no leading space) in addition to
        // the more common `data: payload`.
        let line = r#"data:{"candidates":[{"content":{"parts":[{"text":"hi"}],"role":"model"}}]}"#;
        let chunk = parse_stream_line(line).expect("Some").expect("Ok");
        assert_eq!(chunk.content, "hi");
    }

    #[test]
    fn parse_stream_line_surfaces_function_call_parts_as_tool_calls() {
        // Gemini doesn't fragment `functionCall.args` across chunks —
        // each call arrives complete, typically on the terminal chunk
        // alongside `finishReason`. extract_tool_calls runs directly
        // on the parsed chunk; no per-call accumulator needed.
        let line = r#"data: {"candidates":[{"content":{"parts":[{"functionCall":{"name":"get_weather","args":{"city":"Berlin"}}}],"role":"model"},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":6,"totalTokenCount":10}}"#;
        let chunk = parse_stream_line(line).expect("Some").expect("Ok");
        assert_eq!(chunk.finish_reason.as_deref(), Some("STOP"));
        assert!(chunk.usage.is_some());
        assert_eq!(chunk.tool_calls.len(), 1);
        let call = &chunk.tool_calls[0];
        assert_eq!(call.id, "get_weather");
        assert_eq!(call.name, "get_weather");
        let parsed_args: serde_json::Value =
            serde_json::from_str(&call.arguments).expect("valid JSON args");
        assert_eq!(parsed_args, serde_json::json!({"city": "Berlin"}));
    }

    #[test]
    fn parse_stream_line_disambiguates_repeated_function_names_within_a_chunk() {
        let line = r#"data: {"candidates":[{"content":{"parts":[
            {"functionCall":{"name":"search","args":{"q":"rust"}}},
            {"functionCall":{"name":"search","args":{"q":"gemini"}}}
        ],"role":"model"},"finishReason":"STOP"}]}"#;
        let chunk = parse_stream_line(line).expect("Some").expect("Ok");
        assert_eq!(chunk.tool_calls.len(), 2);
        assert_eq!(chunk.tool_calls[0].id, "search");
        // Second call gets the #1 disambiguator suffix.
        assert_eq!(chunk.tool_calls[1].id, "search#1");
    }

    #[test]
    fn chat_request_serialises_with_camel_case_keys() {
        let body = GeminiChatRequest {
            contents: vec![GeminiContent {
                role: "user",
                parts: vec![GeminiPart {
                    text: Some("hi".into()),
                    ..Default::default()
                }],
            }],
            system_instruction: Some(GeminiSystemInstruction {
                parts: vec![GeminiPart {
                    text: Some("be brief".into()),
                    ..Default::default()
                }],
            }),
            generation_config: Some(GeminiGenerationConfig {
                max_output_tokens: Some(64),
                temperature: Some(0.2),
            }),
            tools: Vec::new(),
        };
        let json = serde_json::to_value(&body).unwrap();
        assert!(json.get("systemInstruction").is_some());
        assert!(json.get("generationConfig").is_some());
        let gc = &json["generationConfig"];
        assert_eq!(gc["maxOutputTokens"], 64);
        // tools omitted when empty
        assert!(json.get("tools").is_none());
    }

    #[test]
    fn build_tools_emits_function_declarations_envelope() {
        let defs = vec![
            ToolDefinition {
                name: "get_weather".into(),
                description: Some("Look up the weather for a city.".into()),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {"city": {"type": "string"}},
                    "required": ["city"],
                }),
            },
            ToolDefinition {
                name: "ping".into(),
                description: None,
                input_schema: serde_json::json!({"type": "object"}),
            },
        ];
        let json = serde_json::to_value(build_tools(&defs)).unwrap();
        let outer = json.as_array().unwrap();
        // Gemini's `tools` is an array of envelopes; we emit exactly one.
        assert_eq!(outer.len(), 1);
        let decls = outer[0]["functionDeclarations"].as_array().unwrap();
        assert_eq!(decls.len(), 2);
        assert_eq!(decls[0]["name"], "get_weather");
        assert_eq!(decls[0]["description"], "Look up the weather for a city.");
        assert_eq!(decls[0]["parameters"]["type"], "object");
        // description omitted when None
        assert!(decls[1].get("description").is_none());
    }

    #[test]
    fn build_tools_returns_empty_vec_for_empty_definitions() {
        assert!(build_tools(&[]).is_empty());
    }

    #[test]
    fn build_parts_emits_function_response_for_tool_result() {
        let m = Message::tool_result("get_weather", "{\"temp\":72}");
        let parts = build_parts(&m);
        assert_eq!(parts.len(), 1);
        let fr = parts[0].function_response.as_ref().unwrap();
        assert_eq!(fr.name, "get_weather");
        // JSON tool result is surfaced directly (object), not wrapped.
        assert_eq!(fr.response, serde_json::json!({"temp": 72}));
    }

    #[test]
    fn build_parts_strips_disambiguator_suffix_from_tool_call_id() {
        // extract_tool_calls appends `#N` to the second/third/... calls
        // with the same function name. On the way back out we recover
        // the bare function name.
        let m = Message::tool_result("get_weather#1", "{}");
        let parts = build_parts(&m);
        assert_eq!(
            parts[0].function_response.as_ref().unwrap().name,
            "get_weather"
        );
    }

    #[test]
    fn build_parts_wraps_non_json_tool_result_in_content_envelope() {
        // Gemini wants an object in functionResponse.response. A raw
        // string degrades to `{"content": "..."}`.
        let m = Message::tool_result("ping", "all good");
        let parts = build_parts(&m);
        let fr = parts[0].function_response.as_ref().unwrap();
        assert_eq!(fr.response, serde_json::json!({"content": "all good"}));
    }

    #[test]
    fn build_parts_emits_inline_data_part_for_base64_image_attachment() {
        let msg = Message::text(MessageRole::User, "what's in this?")
            .with_attachment(MediaAttachment::image_base64("Zm9v", "image/png"));
        let parts = build_parts(&msg);
        assert_eq!(parts.len(), 2);
        // First part = text.
        assert_eq!(parts[0].text.as_deref(), Some("what's in this?"));
        // Second part = inlineData.
        let inline = parts[1]
            .inline_data
            .as_ref()
            .expect("inline_data populated");
        assert_eq!(inline.mime_type, "image/png");
        assert_eq!(inline.data, "Zm9v");
        let json = serde_json::to_value(&parts[1]).unwrap();
        assert_eq!(json["inlineData"]["mimeType"], "image/png");
        assert_eq!(json["inlineData"]["data"], "Zm9v");
    }

    #[test]
    fn build_parts_emits_file_data_part_for_url_image_attachment() {
        let msg = Message::text(MessageRole::User, "see this")
            .with_attachment(MediaAttachment::image_url("https://ex.com/cat.jpg"));
        let parts = build_parts(&msg);
        let json = serde_json::to_value(&parts[1]).unwrap();
        // URL sources land in fileData.fileUri — Gemini fetches the
        // URI itself (works for gs:// and publicly accessible HTTPS).
        assert_eq!(json["fileData"]["fileUri"], "https://ex.com/cat.jpg");
        // No mime_type on the gateway side → field omitted on the wire.
        assert!(json["fileData"].get("mimeType").is_none());
    }

    #[test]
    fn build_parts_defaults_inline_mime_type_to_image_jpeg_when_unspecified() {
        let msg = Message::text(MessageRole::User, "x").with_attachment(
            MediaAttachment::Image {
                source: MediaSource::Base64 {
                    data: "AAAA".into(),
                },
                mime_type: None,
            },
        );
        let parts = build_parts(&msg);
        assert_eq!(
            parts[1].inline_data.as_ref().unwrap().mime_type,
            "image/jpeg",
        );
    }

    #[test]
    fn build_parts_emits_attachment_only_when_text_empty() {
        let msg = Message::text(MessageRole::User, "")
            .with_attachment(MediaAttachment::image_url("https://ex.com/x.png"));
        let parts = build_parts(&msg);
        assert_eq!(parts.len(), 1);
        assert!(parts[0].text.is_none());
        assert!(parts[0].file_data.is_some());
    }

    #[test]
    fn build_parts_emits_function_call_for_assistant_tool_calls() {
        let msg = Message {
            role: MessageRole::Assistant,
            content: MessageContent::Text {
                text: "Looking…".into(),
            },
            tool_calls: vec![ToolCall {
                id: "get_weather".into(),
                name: "get_weather".into(),
                arguments: "{\"city\":\"Berlin\"}".into(),
            }],
            attachments: vec![],
        };
        let parts = build_parts(&msg);
        assert_eq!(parts.len(), 2, "text part + function_call part");
        assert_eq!(parts[0].text.as_deref(), Some("Looking…"));
        let fc = parts[1].function_call.as_ref().unwrap();
        assert_eq!(fc.name, "get_weather");
        assert_eq!(fc.args, serde_json::json!({"city": "Berlin"}));
    }

    #[test]
    fn extract_tool_calls_assigns_disambiguator_to_repeat_function_names() {
        let resp = GeminiChatResponse {
            candidates: vec![GeminiCandidate {
                content: Some(GeminiResponseContent {
                    parts: vec![
                        GeminiPart {
                            function_call: Some(GeminiFunctionCall {
                                name: "search".into(),
                                args: serde_json::json!({"q": "rust"}),
                            }),
                            ..Default::default()
                        },
                        GeminiPart {
                            function_call: Some(GeminiFunctionCall {
                                name: "search".into(),
                                args: serde_json::json!({"q": "gemini"}),
                            }),
                            ..Default::default()
                        },
                    ],
                }),
                finish_reason: None,
            }],
            usage_metadata: None,
        };
        let calls = extract_tool_calls(&resp);
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].id, "search");
        assert_eq!(calls[1].id, "search#1");
        // Arguments round-trip as JSON-encoded strings.
        let p1: serde_json::Value = serde_json::from_str(&calls[0].arguments).unwrap();
        assert_eq!(p1, serde_json::json!({"q": "rust"}));
    }

    #[test]
    fn extract_tool_calls_returns_empty_when_no_function_call_parts() {
        let resp = GeminiChatResponse {
            candidates: vec![GeminiCandidate {
                content: Some(GeminiResponseContent {
                    parts: vec![GeminiPart {
                        text: Some("hello".into()),
                        ..Default::default()
                    }],
                }),
                finish_reason: Some("STOP".into()),
            }],
            usage_metadata: None,
        };
        assert!(extract_tool_calls(&resp).is_empty());
    }

    #[test]
    fn parse_tool_input_handles_empty_and_malformed_inputs() {
        assert_eq!(parse_tool_input(""), serde_json::json!({}));
        assert_eq!(
            parse_tool_input("{\"city\":\"Berlin\"}"),
            serde_json::json!({"city": "Berlin"})
        );
        // Malformed JSON degrades to an empty object — Gemini requires
        // an object here, so this is the safest fallback.
        assert_eq!(parse_tool_input("not json"), serde_json::json!({}));
    }
}
