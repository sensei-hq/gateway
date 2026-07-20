use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use futures::stream::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::base::{build_client, resolve_api_key};
use kernel::types::config::RouterConfig;
use kernel::types::cost::TokenUsage;
use kernel::types::error::GatewayError;
use kernel::types::io::{ChatRequest, ChatResponse};
use kernel::types::request::{
    MediaAttachment, MediaSource, Message, MessageContent, MessageRole, StreamChunk,
    StreamingToolCall, ToolCall, ToolDefinition,
};

// ---------------------------------------------------------------------------
// Wire types — Anthropic Messages API request/response structs
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct AnthropicRequest {
    model: String,
    messages: Vec<AnthropicMessage>,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    stream: bool,
    /// Tool definitions the model may call. Anthropic uses the same
    /// JSON Schema shape as OpenAI but at the top level — no
    /// `{type: "function"}` envelope.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<AnthropicTool>,
}

#[derive(Debug, Serialize)]
struct AnthropicTool {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    input_schema: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct AnthropicMessage {
    role: String,
    /// Anthropic accepts either a string or an array of content blocks
    /// for `content`. We always emit the array form so tool_use and
    /// tool_result blocks can coexist with text without special-cases.
    content: Vec<OutContentBlock>,
}

/// Outbound content block. Anthropic distinguishes blocks by a top-level
/// `type` field; `serde(tag = "type")` reproduces that shape with no
/// runtime cost.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum OutContentBlock {
    Text {
        text: String,
    },
    /// Mirrored back when continuing a multi-turn tool-calling
    /// conversation. `input` is a JSON object — we deserialize the
    /// gateway-side `ToolCall::arguments` string into it.
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    /// Caller's response to a prior tool call. `tool_use_id` links it
    /// back to the originating `tool_use` block.
    ToolResult {
        tool_use_id: String,
        content: String,
    },
    /// Vision input. Anthropic accepts either a base64-encoded inline
    /// payload (with `media_type`) or a URL (since 2024) — the
    /// [`AnthropicImageSource`] discriminator picks between them.
    Image {
        source: AnthropicImageSource,
    },
}

/// Anthropic image source — the inner `source` field of an
/// `{type: "image"}` content block. Both base64 and URL inputs map
/// onto a tagged shape with `type: "base64"|"url"`.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AnthropicImageSource {
    /// Inline image bytes. `media_type` is required on base64 sources
    /// — Anthropic uses it to pick the decoder.
    Base64 { media_type: String, data: String },
    /// HTTPS URL fetched by Anthropic on the way in.
    Url { url: String },
}

#[derive(Debug, Deserialize)]
struct AnthropicResponse {
    content: Vec<ContentBlock>,
    #[allow(dead_code)]
    stop_reason: Option<String>,
    usage: AnthropicUsage,
}

/// Inbound content block. We parse blocks loosely (single struct with
/// optional fields) rather than as a tagged enum so unknown block types
/// don't error the whole response — the model may emit blocks we don't
/// yet model (image, document, …) and we want to skip those gracefully.
#[derive(Debug, Deserialize)]
struct ContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    #[serde(default)]
    text: Option<String>,
    /// Tool-use block fields.
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    input: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct AnthropicUsage {
    #[serde(default)]
    input_tokens: u32,
    #[serde(default)]
    output_tokens: u32,
}

// Streaming event types

#[derive(Debug, Deserialize)]
struct StreamEvent {
    #[serde(rename = "type")]
    event_type: String,
    /// Per-block index — present on content_block_start /
    /// content_block_delta / content_block_stop.
    #[serde(default)]
    index: Option<u32>,
    /// Body of a content_block_start event. Carries the block's
    /// type-specific shape (text / tool_use / …).
    #[serde(default)]
    content_block: Option<StreamContentBlockStart>,
    #[serde(default)]
    delta: Option<StreamDelta>,
    #[serde(default)]
    usage: Option<AnthropicUsage>,
}

#[derive(Debug, Deserialize)]
struct StreamContentBlockStart {
    #[serde(rename = "type")]
    block_type: String,
    /// `tool_use` block fields.
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StreamDelta {
    /// Discriminator for the kind of delta this is —
    /// `"text_delta"`, `"input_json_delta"`, etc. Absent on
    /// `message_delta` events.
    #[serde(default, rename = "type")]
    delta_type: Option<String>,
    /// `text_delta` payload.
    #[serde(default)]
    text: Option<String>,
    /// `input_json_delta` payload — incremental JSON fragment for an
    /// in-progress tool_use block's `input`.
    #[serde(default)]
    partial_json: Option<String>,
    /// `message_delta` payload — terminal stop reason.
    #[serde(default)]
    stop_reason: Option<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const DEFAULT_MODEL: &str = "claude-haiku-4-5-20250414";
const ANTHROPIC_VERSION: &str = "2023-06-01";
const DEFAULT_MAX_TOKENS: u32 = 1024;

/// Extract system prompt from:
/// 1. The explicit `system` field on the Chat payload
/// 2. Any message with role System (first one wins)
fn extract_system(messages: &[Message], system: &Option<String>) -> Option<String> {
    if system.is_some() {
        return system.clone();
    }
    messages
        .iter()
        .find(|m| m.role == MessageRole::System)
        .map(|m| m.as_text().to_string())
}

/// Build Anthropic message array.
///
/// System-role gateway messages are filtered out (hoisted into the
/// top-level `system` field by the caller). Everything else maps onto
/// Anthropic's `{role: "user" | "assistant", content: [blocks]}`
/// shape.
///
/// The block layout is:
///
/// - [`MessageContent::Text`] becomes a single `{type: "text"}` block.
///   Empty text is dropped so assistant turns that only emit tool
///   calls don't ship an empty block (Anthropic accepts that, but it
///   reads as noise).
/// - [`MessageContent::ToolResult`] becomes a single
///   `{type: "tool_result", tool_use_id, content}` block. Anthropic
///   uses `tool_use_id` (singular) rather than OpenAI's
///   `tool_call_id`.
/// - Any `tool_calls` on the gateway message become extra
///   `{type: "tool_use", id, name, input}` blocks. `input` is a JSON
///   object — we parse the gateway's JSON-string arguments back into
///   one, falling back to a string node on malformed input.
fn build_messages(messages: &[Message]) -> Vec<AnthropicMessage> {
    messages
        .iter()
        .filter(|m| m.role != MessageRole::System)
        .map(|m| {
            // Anthropic only knows "user" / "assistant"; gateway's
            // Tool role maps to "user" because that's where tool
            // result blocks live in the conversation.
            let role = match m.role {
                MessageRole::Assistant => "assistant",
                MessageRole::User | MessageRole::Tool => "user",
                MessageRole::System => unreachable!(), // filtered above
            };
            AnthropicMessage {
                role: role.to_string(),
                content: build_content_blocks(m),
            }
        })
        .collect()
}

/// Compose the `Vec<OutContentBlock>` for a single gateway message.
///
/// Order: text first (when non-empty), then one image block per
/// [`MediaAttachment::Image`], then tool_use blocks. Tool-result
/// messages emit a single tool_result block and skip everything
/// else.
fn build_content_blocks(m: &Message) -> Vec<OutContentBlock> {
    let mut blocks: Vec<OutContentBlock> = Vec::new();
    match &m.content {
        MessageContent::Text { text } => {
            if !text.is_empty() {
                blocks.push(OutContentBlock::Text { text: text.clone() });
            }
            for att in &m.attachments {
                if let Some(block) = attachment_to_block(att) {
                    blocks.push(block);
                }
            }
        }
        MessageContent::ToolResult {
            tool_call_id,
            content,
        } => {
            blocks.push(OutContentBlock::ToolResult {
                tool_use_id: tool_call_id.clone(),
                content: content.clone(),
            });
        }
    }
    for tc in &m.tool_calls {
        blocks.push(OutContentBlock::ToolUse {
            id: tc.id.clone(),
            name: tc.name.clone(),
            input: parse_tool_input(&tc.arguments),
        });
    }
    blocks
}

/// Translate a gateway [`MediaAttachment`] into an Anthropic content
/// block. Returns `None` for variants we don't yet model.
///
/// Base64 sources without a `mime_type` default to `image/jpeg` —
/// Anthropic requires `media_type` on inline payloads, so we have to
/// pick something. JPEG matches the most common case (camera roll
/// uploads).
fn attachment_to_block(att: &MediaAttachment) -> Option<OutContentBlock> {
    match att {
        MediaAttachment::Image { source, mime_type } => {
            let source = match source {
                MediaSource::Url { url } => AnthropicImageSource::Url { url: url.clone() },
                MediaSource::Base64 { data } => AnthropicImageSource::Base64 {
                    media_type: mime_type
                        .clone()
                        .unwrap_or_else(|| "image/jpeg".to_string()),
                    data: data.clone(),
                },
            };
            Some(OutContentBlock::Image { source })
        }
    }
}

/// Parse a JSON-string `arguments` payload back into a JSON value for
/// Anthropic's `tool_use.input` field. Malformed input degrades to a
/// JSON string node — the provider will reject it, but we surface a
/// readable error instead of a serialize panic.
fn parse_tool_input(args: &str) -> serde_json::Value {
    if args.is_empty() {
        return serde_json::json!({});
    }
    serde_json::from_str(args).unwrap_or_else(|_| serde_json::Value::String(args.to_string()))
}

/// Convert gateway [`ToolDefinition`]s into Anthropic's top-level
/// `tools` array. Anthropic uses the same `input_schema` field as the
/// gateway type, so this is a near-identity mapping.
fn build_tools(tools: &[ToolDefinition]) -> Vec<AnthropicTool> {
    tools
        .iter()
        .map(|t| AnthropicTool {
            name: t.name.clone(),
            description: t.description.clone(),
            input_schema: t.input_schema.clone(),
        })
        .collect()
}

/// Pull `tool_use` blocks out of the response and convert them into
/// gateway [`ToolCall`]s. `input` (JSON object) is re-serialized into
/// the gateway's JSON-string `arguments` shape for round-trip parity
/// with OpenAI.
fn extract_tool_calls(content: &[ContentBlock]) -> Vec<ToolCall> {
    content
        .iter()
        .filter(|b| b.block_type == "tool_use")
        .filter_map(|b| {
            let id = b.id.clone()?;
            let name = b.name.clone()?;
            let arguments = b
                .input
                .as_ref()
                .map(|v| serde_json::to_string(v).unwrap_or_default())
                .unwrap_or_default();
            Some(ToolCall {
                id,
                name,
                arguments,
            })
        })
        .collect()
}

fn usage_from_anthropic(usage: &AnthropicUsage) -> TokenUsage {
    TokenUsage {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        total_tokens: usage.input_tokens + usage.output_tokens,
    }
}

/// Extract text from content blocks, joining all text blocks.
fn extract_text(content: &[ContentBlock]) -> String {
    content
        .iter()
        .filter(|b| b.block_type == "text")
        .filter_map(|b| b.text.as_deref())
        .collect::<Vec<_>>()
        .join("")
}

/// Header name for the Anthropic API version. Operators can override the
/// default [`ANTHROPIC_VERSION`] by setting this key in
/// `RouterConfig.headers` — so the version can be bumped via config
/// without a rebuild.
const ANTHROPIC_VERSION_HEADER: &str = "anthropic-version";

/// Apply the fixed Anthropic headers plus any operator-supplied
/// `config.headers` to an outbound request builder.
///
/// `anthropic-version` defaults to [`ANTHROPIC_VERSION`] but an operator
/// can override it via `config.headers`. The version is resolved once up
/// front (caller value wins) and skipped during the `config.headers`
/// pass, so exactly one `anthropic-version` header is emitted rather than
/// a duplicate. Every other custom header is forwarded verbatim, matching
/// the openai/gemini/grok adapters.
fn apply_request_headers(
    builder: reqwest::RequestBuilder,
    api_key: &str,
    config: &RouterConfig,
) -> reqwest::RequestBuilder {
    let anthropic_version = config
        .headers
        .get(ANTHROPIC_VERSION_HEADER)
        .map(String::as_str)
        .unwrap_or(ANTHROPIC_VERSION);
    let mut builder = builder
        .header("x-api-key", api_key)
        .header(ANTHROPIC_VERSION_HEADER, anthropic_version)
        .header("content-type", "application/json");
    for (k, v) in &config.headers {
        // anthropic-version is already applied above with the caller's
        // value winning; skip it here so we don't emit a duplicate.
        if k.eq_ignore_ascii_case(ANTHROPIC_VERSION_HEADER) {
            continue;
        }
        builder = builder.header(k.as_str(), v.as_str());
    }
    builder
}

// ---------------------------------------------------------------------------
// AnthropicAdapter
// ---------------------------------------------------------------------------

/// Adapter for the Anthropic Messages API.
///
/// Supports chat completions via `POST /v1/messages`. Anthropic does not expose
/// an embedding endpoint, so only the Chat capability is supported.
pub struct AnthropicAdapter {
    client: Client,
}

impl AnthropicAdapter {
    pub fn new() -> Result<Self, GatewayError> {
        Ok(Self {
            client: Client::new(),
        })
    }

    /// Create an adapter from a pre-built client (e.g. with timeout from config).
    pub fn from_config(config: &RouterConfig) -> Result<Self, GatewayError> {
        Ok(Self {
            client: build_client(config)?,
        })
    }
}

/// Persistent state for the Anthropic SSE stream pipeline. Lives
/// across HTTP byte chunks so that:
///
/// - SSE lines that get split across HTTP chunks reassemble correctly
///   (`line_buf` holds a partial trailing line).
/// - `tool_use` blocks track their accumulated `input_json_delta`
///   fragments per `index` until `content_block_stop` finalises them
///   and `message_stop` flushes them onto the terminal chunk.
struct AnthropicStreamState {
    byte_stream: Pin<Box<dyn Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>,
    line_buf: String,
    tool_calls: BTreeMap<u32, StreamingToolCall>,
    pending: VecDeque<Result<StreamChunk, GatewayError>>,
    eof: bool,
}

fn process_stream_bytes(state: &mut AnthropicStreamState, bytes: &[u8]) {
    state.line_buf.push_str(&String::from_utf8_lossy(bytes));
    while let Some(newline_pos) = state.line_buf.find('\n') {
        let mut line = state.line_buf.drain(..=newline_pos).collect::<String>();
        line.truncate(line.trim_end().len());
        process_sse_line(state, line.trim());
    }
}

/// Process a single SSE line. Anthropic uses both `event:` lines
/// (which we ignore — the type discriminator is duplicated inside
/// the `data:` payload) and `data:` lines that carry the JSON event
/// body.
fn process_sse_line(state: &mut AnthropicStreamState, line: &str) {
    if line.is_empty() || line.starts_with("event:") {
        return;
    }
    let payload = line.strip_prefix("data: ").unwrap_or(line);
    let event = match serde_json::from_str::<StreamEvent>(payload) {
        Ok(v) => v,
        Err(_) => return,
    };
    match event.event_type.as_str() {
        // Opening a content block. The `tool_use` shape gives us the
        // id + name; subsequent input_json_delta events append to the
        // accumulator for this index.
        "content_block_start" => {
            let (Some(index), Some(block)) = (event.index, event.content_block.as_ref()) else {
                return;
            };
            if block.block_type == "tool_use"
                && let (Some(id), Some(name)) = (block.id.as_ref(), block.name.as_ref())
            {
                state
                    .tool_calls
                    .insert(index, StreamingToolCall::new(id, name));
            }
        }
        // Two flavours of delta share this event:
        // - text_delta → user-facing text content
        // - input_json_delta → argument fragment for the tool_use
        //   block at this index
        "content_block_delta" => {
            let Some(delta) = event.delta.as_ref() else {
                return;
            };
            match delta.delta_type.as_deref() {
                Some("text_delta") => {
                    let content = delta.text.clone().unwrap_or_default();
                    if !content.is_empty() {
                        state.pending.push_back(Ok(StreamChunk {
                            content,
                            finish_reason: None,
                            usage: None,
                            tool_calls: Vec::new(),
                        }));
                    }
                }
                Some("input_json_delta") => {
                    if let (Some(idx), Some(frag)) = (event.index, delta.partial_json.as_ref())
                        && let Some(acc) = state.tool_calls.get_mut(&idx)
                    {
                        acc.push_arguments(frag);
                    }
                }
                _ => {}
            }
        }
        // content_block_stop is purely framing — we keep the
        // accumulator alive until message_stop so all calls
        // finalise together onto the terminal chunk.
        "content_block_stop" => {}
        // message_delta carries the terminal stop_reason and (on
        // some traffic) per-turn usage deltas. We emit a chunk that
        // surfaces stop_reason and any accumulated tool_calls.
        "message_delta" => {
            let stop_reason = event
                .delta
                .as_ref()
                .and_then(|d| d.stop_reason.clone())
                .or_else(|| Some("end_turn".to_string()));
            let usage = event.usage.as_ref().map(usage_from_anthropic);
            let tool_calls: Vec<ToolCall> = std::mem::take(&mut state.tool_calls)
                .into_values()
                .filter_map(StreamingToolCall::finalize)
                .collect();
            state.pending.push_back(Ok(StreamChunk {
                content: String::new(),
                finish_reason: stop_reason,
                usage,
                tool_calls,
            }));
        }
        // message_stop closes the stream. Drain any accumulators that
        // are still pending (defensive — message_delta normally fires
        // first and clears them).
        "message_stop" => {
            let tool_calls: Vec<ToolCall> = std::mem::take(&mut state.tool_calls)
                .into_values()
                .filter_map(StreamingToolCall::finalize)
                .collect();
            // Only emit if we still have something to surface — by
            // default message_delta has already carried the
            // finish_reason and tool calls.
            if !tool_calls.is_empty() {
                state.pending.push_back(Ok(StreamChunk {
                    content: String::new(),
                    finish_reason: Some("stop".to_string()),
                    usage: None,
                    tool_calls,
                }));
            }
        }
        // ping / message_start / others — framing only.
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Capability traits (target model). Traits + RegisterInto referenced by full path. The
// SSE pipeline (process_stream_bytes / process_sse_line / AnthropicStreamState)
// is shared verbatim with the legacy stream() method above.
// ---------------------------------------------------------------------------

impl kernel::adapters::capability::Model for AnthropicAdapter {
    fn id(&self) -> &str {
        "anthropic"
    }
}

#[async_trait]
impl kernel::adapters::capability::ChatModel for AnthropicAdapter {
    async fn chat(
        &self,
        config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        let api_key = resolve_api_key(config).ok_or_else(|| GatewayError::Authentication {
            adapter: "anthropic".into(),
            message: "missing API key — set the env var specified in api_key_env".into(),
        })?;

        let model = req
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());
        let extracted_system = extract_system(&req.messages, &req.system);

        let body = AnthropicRequest {
            model: model.clone(),
            messages: build_messages(&req.messages),
            max_tokens: req.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
            system: extracted_system,
            temperature: req.temperature,
            stream: false,
            tools: build_tools(&req.tools),
        };

        let url = format!("{}/v1/messages", config.url.trim_end_matches('/'));
        let resp = apply_request_headers(self.client.post(&url), &api_key, config)
            .json(&body)
            .send()
            .await?;

        let status = resp.status();

        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            return Err(match status.as_u16() {
                401 | 403 => GatewayError::Authentication {
                    adapter: "anthropic".into(),
                    message: body_text,
                },
                429 => GatewayError::RateLimit {
                    adapter: "anthropic".into(),
                    retry_after_ms: None,
                },
                _ => GatewayError::ProviderError {
                    adapter: "anthropic".into(),
                    message: body_text,
                    status: Some(status.as_u16()),
                },
            });
        }

        let anthropic_resp: AnthropicResponse =
            resp.json().await.map_err(|e| GatewayError::ProviderError {
                adapter: "anthropic".into(),
                message: format!("failed to parse response: {}", e),
                status: Some(status.as_u16()),
            })?;

        let content = extract_text(&anthropic_resp.content);
        let tool_calls = extract_tool_calls(&anthropic_resp.content);
        let usage = usage_from_anthropic(&anthropic_resp.usage);

        Ok(ChatResponse {
            content: if content.is_empty() {
                None
            } else {
                Some(content)
            },
            tool_calls,
            usage: Some(usage),
            model: Some(model),
            degraded: false,
        })
    }

    async fn chat_stream(
        &self,
        config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        let api_key = resolve_api_key(config).ok_or_else(|| GatewayError::Authentication {
            adapter: "anthropic".into(),
            message: "missing API key — set the env var specified in api_key_env".into(),
        })?;

        let model = req
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());
        let extracted_system = extract_system(&req.messages, &req.system);

        let body = AnthropicRequest {
            model,
            messages: build_messages(&req.messages),
            max_tokens: req.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
            system: extracted_system,
            temperature: req.temperature,
            stream: true,
            // Streaming + tool calling is deferred — Anthropic emits
            // `input_json_delta` events for tool arguments that need
            // accumulation in the stream layer. v1 ships tools through
            // execute() only.
            tools: Vec::new(),
        };

        let url = format!("{}/v1/messages", config.url.trim_end_matches('/'));
        let response = apply_request_headers(self.client.post(&url), &api_key, config)
            .json(&body)
            .send()
            .await?;

        let status = response.status();

        if !status.is_success() {
            let body_text = response.text().await.unwrap_or_default();
            return Err(match status.as_u16() {
                401 | 403 => GatewayError::Authentication {
                    adapter: "anthropic".into(),
                    message: body_text,
                },
                429 => GatewayError::RateLimit {
                    adapter: "anthropic".into(),
                    retry_after_ms: None,
                },
                _ => GatewayError::ProviderError {
                    adapter: "anthropic".into(),
                    message: body_text,
                    status: Some(status.as_u16()),
                },
            });
        }

        let byte_stream: Pin<Box<dyn Stream<Item = _> + Send>> = Box::pin(response.bytes_stream());
        let initial = AnthropicStreamState {
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
                            adapter: "anthropic".into(),
                            message: format!("anthropic stream error: {e}"),
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
}

#[async_trait]
impl kernel::adapters::RegisterInto for AnthropicAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &kernel::adapters::AdapterRegistry) {
        reg.register_chat(self).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anthropic_id_and_supports() {
        let adapter = AnthropicAdapter::new().unwrap();
        assert_eq!(
            kernel::adapters::capability::Model::id(&adapter),
            "anthropic"
        );
    }

    fn config_with_headers(headers: std::collections::HashMap<String, String>) -> RouterConfig {
        RouterConfig {
            url: "https://api.anthropic.com".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers,
        }
    }

    #[test]
    fn apply_request_headers_forwards_custom_headers_and_defaults_version() {
        let headers =
            std::collections::HashMap::from([("x-custom".to_string(), "abc".to_string())]);
        let config = config_with_headers(headers);
        let req = apply_request_headers(
            Client::new().post("https://api.anthropic.com/v1/messages"),
            "sk-test",
            &config,
        )
        .build()
        .unwrap();
        let h = req.headers();
        assert_eq!(h.get("x-api-key").unwrap(), "sk-test");
        assert_eq!(h.get("content-type").unwrap(), "application/json");
        // The custom header reaches the outbound request.
        assert_eq!(h.get("x-custom").unwrap(), "abc");
        // No override present → the const fallback is used.
        assert_eq!(h.get("anthropic-version").unwrap(), ANTHROPIC_VERSION);
    }

    #[test]
    fn apply_request_headers_lets_config_override_the_anthropic_version() {
        let headers = std::collections::HashMap::from([
            ("anthropic-version".to_string(), "2099-01-01".to_string()),
            ("x-extra".to_string(), "1".to_string()),
        ]);
        let config = config_with_headers(headers);
        let req = apply_request_headers(
            Client::new().post("https://api.anthropic.com/v1/messages"),
            "sk-test",
            &config,
        )
        .build()
        .unwrap();
        let h = req.headers();
        // The operator's version wins over the compiled-in default…
        assert_eq!(h.get("anthropic-version").unwrap(), "2099-01-01");
        // …and exactly one anthropic-version header is emitted (no dupe).
        assert_eq!(h.get_all("anthropic-version").iter().count(), 1);
        assert_eq!(h.get("x-extra").unwrap(), "1");
    }

    #[test]
    fn anthropic_capability_model_id() {
        let adapter = AnthropicAdapter::new().unwrap();
        // Reference `Model::id` by full path
        // the capability Model trait.
        assert_eq!(
            kernel::adapters::capability::Model::id(&adapter),
            "anthropic"
        );
    }

    #[test]
    fn chat_request_builds_expected_anthropic_body() {
        // Mirrors build_anthropic_request_basic but drives the wire body from
        // a capability-layer ChatRequest — exercising the same resolve-model /
        // extract_system / build_messages / build_tools path chat() uses,
        // without a network round-trip. Unlike chat_stream, the non-streaming
        // chat path forwards tools.
        let req = ChatRequest {
            model: Some("claude-haiku-4-5-20250414".to_string()),
            messages: vec![Message::text(MessageRole::User, "Hello")],
            system: Some("Be terse.".to_string()),
            max_tokens: Some(256),
            temperature: Some(0.5),
            tools: vec![ToolDefinition {
                name: "get_weather".into(),
                description: None,
                input_schema: serde_json::json!({"type": "object"}),
            }],
        };

        let model = req
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());
        let body = AnthropicRequest {
            model: model.clone(),
            messages: build_messages(&req.messages),
            max_tokens: req.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
            system: extract_system(&req.messages, &req.system),
            temperature: req.temperature,
            stream: false,
            tools: build_tools(&req.tools),
        };

        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["model"], "claude-haiku-4-5-20250414");
        assert_eq!(json["max_tokens"], 256);
        assert_eq!(json["system"], "Be terse.");
        assert_eq!(json["stream"], false);
        let blocks = json["messages"][0]["content"].as_array().unwrap();
        assert_eq!(blocks[0]["type"], "text");
        assert_eq!(blocks[0]["text"], "Hello");
        assert_eq!(json["tools"][0]["name"], "get_weather");
    }

    #[test]
    fn build_anthropic_request_basic() {
        let messages = vec![Message::text(MessageRole::User, "Hello")];

        let anthropic_messages = build_messages(&messages);
        let body = AnthropicRequest {
            model: "claude-haiku-4-5-20250414".to_string(),
            messages: anthropic_messages,
            max_tokens: 1024,
            system: None,
            temperature: Some(0.7),
            stream: false,
            tools: Vec::new(),
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["model"], "claude-haiku-4-5-20250414");
        assert_eq!(json["max_tokens"], 1024);
        assert_eq!(json["stream"], false);
        assert!(json.get("system").is_none()); // skipped when None
        assert!(json.get("tools").is_none()); // skipped when empty

        let msgs = json["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["role"], "user");
        // Content is now an array of blocks rather than a flat string.
        let blocks = msgs[0]["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["type"], "text");
        assert_eq!(blocks[0]["text"], "Hello");
    }

    #[test]
    fn build_anthropic_request_with_system() {
        let messages = vec![Message::text(MessageRole::User, "Hi")];
        let system = Some("You are helpful.".to_string());

        let extracted = extract_system(&messages, &system);
        let anthropic_messages = build_messages(&messages);

        let body = AnthropicRequest {
            model: "claude-haiku-4-5-20250414".to_string(),
            messages: anthropic_messages,
            max_tokens: 512,
            system: extracted,
            temperature: None,
            stream: false,
            tools: Vec::new(),
        };

        let json = serde_json::to_value(&body).unwrap();

        // System prompt is at the top level, not in messages
        assert_eq!(json["system"], "You are helpful.");

        let msgs = json["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["role"], "user");
        // No system message in the messages array
        assert!(msgs.iter().all(|m| m["role"] != "system"));
    }

    #[test]
    fn build_anthropic_request_filters_system_messages() {
        let messages = vec![
            Message::text(MessageRole::System, "Be concise."),
            Message::text(MessageRole::User, "Hello"),
            Message::text(MessageRole::Assistant, "Hi!"),
            Message::tool_result("tc_1", "result: 42"),
        ];

        let extracted = extract_system(&messages, &None);
        assert_eq!(extracted.as_deref(), Some("Be concise."));

        let anthropic_messages = build_messages(&messages);

        // System message is removed, 3 remaining
        assert_eq!(anthropic_messages.len(), 3);
        assert_eq!(anthropic_messages[0].role, "user");
        assert!(matches!(
            anthropic_messages[0].content.as_slice(),
            [OutContentBlock::Text { text }] if text == "Hello",
        ));
        assert_eq!(anthropic_messages[1].role, "assistant");
        assert!(matches!(
            anthropic_messages[1].content.as_slice(),
            [OutContentBlock::Text { text }] if text == "Hi!",
        ));
        // Tool result message: role mapped to "user", body becomes a
        // single `tool_result` block carrying the tool_use_id.
        assert_eq!(anthropic_messages[2].role, "user");
        assert!(matches!(
            anthropic_messages[2].content.as_slice(),
            [OutContentBlock::ToolResult { tool_use_id, content }]
                if tool_use_id == "tc_1" && content == "result: 42",
        ));
    }

    #[test]
    fn parse_anthropic_response() {
        let json = r#"{
            "content": [{"type": "text", "text": "Hello!"}],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 10, "output_tokens": 5}
        }"#;

        let resp: AnthropicResponse = serde_json::from_str(json).unwrap();

        assert_eq!(resp.content.len(), 1);
        assert_eq!(resp.content[0].block_type, "text");
        assert_eq!(resp.content[0].text.as_deref(), Some("Hello!"));
        assert_eq!(resp.stop_reason.as_deref(), Some("end_turn"));
        assert_eq!(resp.usage.input_tokens, 10);
        assert_eq!(resp.usage.output_tokens, 5);

        let text = extract_text(&resp.content);
        assert_eq!(text, "Hello!");

        let usage = usage_from_anthropic(&resp.usage);
        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.output_tokens, 5);
        assert_eq!(usage.total_tokens, 15);
    }

    #[test]
    fn parse_anthropic_response_multiple_blocks() {
        let json = r#"{
            "content": [
                {"type": "text", "text": "Hello, "},
                {"type": "tool_use", "id": "tc_1", "name": "get_weather"},
                {"type": "text", "text": "world!"}
            ],
            "stop_reason": "end_turn",
            "usage": {"input_tokens": 20, "output_tokens": 10}
        }"#;

        let resp: AnthropicResponse = serde_json::from_str(json).unwrap();

        assert_eq!(resp.content.len(), 3);

        // Only text blocks are extracted and joined
        let text = extract_text(&resp.content);
        assert_eq!(text, "Hello, world!");
    }

    #[test]
    fn parse_stream_content_delta() {
        let json =
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hi"}}"#;

        let event: StreamEvent = serde_json::from_str(json).unwrap();

        assert_eq!(event.event_type, "content_block_delta");
        assert!(event.delta.is_some());
        assert_eq!(event.delta.unwrap().text.as_deref(), Some("Hi"));
    }

    #[test]
    fn parse_stream_message_delta() {
        let json = r#"{"type":"message_delta","usage":{"output_tokens":15}}"#;

        let event: StreamEvent = serde_json::from_str(json).unwrap();

        assert_eq!(event.event_type, "message_delta");
        assert!(event.usage.is_some());

        let usage = event.usage.unwrap();
        assert_eq!(usage.output_tokens, 15);
        assert_eq!(usage.input_tokens, 0); // default
    }

    fn empty_anthropic_stream_state() -> AnthropicStreamState {
        AnthropicStreamState {
            byte_stream: Box::pin(futures::stream::empty()),
            line_buf: String::new(),
            tool_calls: BTreeMap::new(),
            pending: VecDeque::new(),
            eof: false,
        }
    }

    #[test]
    fn process_sse_line_emits_text_delta_chunks() {
        let mut state = empty_anthropic_stream_state();
        process_sse_line(
            &mut state,
            r#"data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hi"}}"#,
        );
        assert_eq!(state.pending.len(), 1);
        let chunk = state.pending.pop_front().unwrap().unwrap();
        assert_eq!(chunk.content, "Hi");
        assert!(chunk.tool_calls.is_empty());
    }

    #[test]
    fn process_sse_line_accumulates_tool_use_blocks_across_input_json_deltas() {
        let mut state = empty_anthropic_stream_state();
        // 1) tool_use block opens.
        process_sse_line(
            &mut state,
            r#"data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_01","name":"get_weather","input":{}}}"#,
        );
        // 2) Input arrives in two fragments.
        process_sse_line(
            &mut state,
            r#"data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"ci"}}"#,
        );
        process_sse_line(
            &mut state,
            r#"data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"ty\":\"Berlin\"}"}}"#,
        );
        // No chunks emitted yet — accumulator is open.
        assert!(state.pending.is_empty());

        // 3) Block stop (framing, no emission).
        process_sse_line(
            &mut state,
            r#"data: {"type":"content_block_stop","index":1}"#,
        );
        assert!(state.pending.is_empty());

        // 4) message_delta carries the stop reason and drains the
        //    accumulator into the terminal chunk's tool_calls.
        process_sse_line(
            &mut state,
            r#"data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":7}}"#,
        );
        assert_eq!(state.pending.len(), 1);
        let chunk = state.pending.pop_front().unwrap().unwrap();
        assert_eq!(chunk.finish_reason.as_deref(), Some("tool_use"));
        assert_eq!(chunk.tool_calls.len(), 1);
        let call = &chunk.tool_calls[0];
        assert_eq!(call.id, "toolu_01");
        assert_eq!(call.name, "get_weather");
        assert_eq!(call.arguments, r#"{"city":"Berlin"}"#);
        assert!(chunk.usage.is_some());
    }

    #[test]
    fn process_sse_line_handles_multiple_parallel_tool_calls() {
        let mut state = empty_anthropic_stream_state();
        process_sse_line(
            &mut state,
            r#"data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_a","name":"f1"}}"#,
        );
        process_sse_line(
            &mut state,
            r#"data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_b","name":"f2"}}"#,
        );
        // Empty args for both (default to "{}" on finalise).
        process_sse_line(
            &mut state,
            r#"data: {"type":"message_delta","delta":{"stop_reason":"tool_use"}}"#,
        );
        let chunk = state.pending.pop_front().unwrap().unwrap();
        // BTreeMap sorts by index, so toolu_a (index 0) first.
        assert_eq!(chunk.tool_calls[0].id, "toolu_a");
        assert_eq!(chunk.tool_calls[1].id, "toolu_b");
    }

    #[test]
    fn process_sse_line_drops_text_block_start_without_emission() {
        let mut state = empty_anthropic_stream_state();
        process_sse_line(
            &mut state,
            r#"data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
        );
        // Text block start is framing — no chunk, no accumulator.
        assert!(state.pending.is_empty());
        assert!(state.tool_calls.is_empty());
    }

    #[test]
    fn process_sse_line_skips_event_marker_and_pings() {
        let mut state = empty_anthropic_stream_state();
        process_sse_line(&mut state, "event: ping");
        process_sse_line(&mut state, r#"data: {"type":"ping"}"#);
        process_sse_line(&mut state, r#"data: {"type":"message_start"}"#);
        assert!(state.pending.is_empty());
    }

    #[test]
    fn process_stream_bytes_reassembles_lines_split_across_byte_chunks() {
        let mut state = empty_anthropic_stream_state();
        process_stream_bytes(&mut state, br#"data: {"type":"content_block_de"#);
        assert!(state.pending.is_empty());
        process_stream_bytes(
            &mut state,
            br#"lta","index":0,"delta":{"type":"text_delta","text":"X"}}"#,
        );
        process_stream_bytes(&mut state, b"\n");
        assert_eq!(state.pending.len(), 1);
        let chunk = state.pending.pop_front().unwrap().unwrap();
        assert_eq!(chunk.content, "X");
    }

    #[test]
    fn build_tools_passes_json_schema_through_unchanged() {
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
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["name"], "get_weather");
        assert_eq!(arr[0]["description"], "Look up the weather for a city.");
        assert_eq!(arr[0]["input_schema"]["type"], "object");
        // Anthropic puts the schema at the top level (no `function:` wrapper).
        assert!(arr[0].get("function").is_none());
        // description omitted when None
        assert!(arr[1].get("description").is_none());
    }

    #[test]
    fn build_messages_emits_tool_result_block_for_tool_role() {
        let msgs = vec![Message::tool_result("tu_01", "{\"temp\":72}")];
        let json = serde_json::to_value(build_messages(&msgs)).unwrap();
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["role"], "user");
        let blocks = arr[0]["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["type"], "tool_result");
        // Anthropic uses tool_use_id (singular), unlike OpenAI's tool_call_id.
        assert_eq!(blocks[0]["tool_use_id"], "tu_01");
        assert_eq!(blocks[0]["content"], "{\"temp\":72}");
    }

    #[test]
    fn build_messages_appends_url_image_block_for_image_attachment() {
        let msg = Message::text(MessageRole::User, "what's in this?")
            .with_attachment(MediaAttachment::image_url("https://ex.com/cat.jpg"));
        let json = serde_json::to_value(build_messages(&[msg])).unwrap();
        let blocks = json[0]["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["type"], "text");
        assert_eq!(blocks[0]["text"], "what's in this?");
        assert_eq!(blocks[1]["type"], "image");
        assert_eq!(blocks[1]["source"]["type"], "url");
        assert_eq!(blocks[1]["source"]["url"], "https://ex.com/cat.jpg");
    }

    #[test]
    fn build_messages_appends_base64_image_block_with_media_type() {
        let msg = Message::text(MessageRole::User, "see this")
            .with_attachment(MediaAttachment::image_base64("Zm9v", "image/png"));
        let json = serde_json::to_value(build_messages(&[msg])).unwrap();
        let blocks = json[0]["content"].as_array().unwrap();
        assert_eq!(blocks[1]["type"], "image");
        assert_eq!(blocks[1]["source"]["type"], "base64");
        assert_eq!(blocks[1]["source"]["media_type"], "image/png");
        assert_eq!(blocks[1]["source"]["data"], "Zm9v");
    }

    #[test]
    fn build_messages_defaults_media_type_for_base64_without_one() {
        // Anthropic requires media_type on base64 sources. We default
        // to image/jpeg when the gateway attachment doesn't specify
        // one — matches the most common case (camera roll uploads).
        let msg = Message::text(MessageRole::User, "x").with_attachment(MediaAttachment::Image {
            source: MediaSource::Base64 {
                data: "AAAA".into(),
            },
            mime_type: None,
        });
        let json = serde_json::to_value(build_messages(&[msg])).unwrap();
        assert_eq!(json[0]["content"][1]["source"]["media_type"], "image/jpeg");
    }

    #[test]
    fn build_messages_emits_image_only_block_when_text_is_empty() {
        let msg = Message::text(MessageRole::User, "")
            .with_attachment(MediaAttachment::image_url("https://ex.com/x.png"));
        let json = serde_json::to_value(build_messages(&[msg])).unwrap();
        let blocks = json[0]["content"].as_array().unwrap();
        // Empty text would be noise; the image block stands alone.
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0]["type"], "image");
    }

    #[test]
    fn build_messages_emits_tool_use_block_for_assistant_tool_calls() {
        let msg = Message {
            role: MessageRole::Assistant,
            content: MessageContent::Text {
                text: "Let me check.".into(),
            },
            tool_calls: vec![ToolCall {
                id: "tu_01".into(),
                name: "get_weather".into(),
                arguments: "{\"city\":\"Berlin\"}".into(),
            }],
            attachments: vec![],
        };
        let json = serde_json::to_value(build_messages(&[msg])).unwrap();
        let blocks = json[0]["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 2, "text block + tool_use block");
        assert_eq!(blocks[0]["type"], "text");
        assert_eq!(blocks[0]["text"], "Let me check.");
        assert_eq!(blocks[1]["type"], "tool_use");
        assert_eq!(blocks[1]["id"], "tu_01");
        assert_eq!(blocks[1]["name"], "get_weather");
        // Arguments were a JSON string in the gateway type; Anthropic
        // wants the input as an object, so we parse on the way out.
        assert_eq!(blocks[1]["input"]["city"], "Berlin");
    }

    #[test]
    fn build_messages_drops_empty_text_block_when_only_tool_calls_present() {
        let msg = Message {
            role: MessageRole::Assistant,
            content: MessageContent::Text {
                text: String::new(),
            },
            tool_calls: vec![ToolCall {
                id: "tu_01".into(),
                name: "noop".into(),
                arguments: "{}".into(),
            }],
            attachments: vec![],
        };
        let json = serde_json::to_value(build_messages(&[msg])).unwrap();
        let blocks = json[0]["content"].as_array().unwrap();
        assert_eq!(blocks.len(), 1, "empty text block should be elided");
        assert_eq!(blocks[0]["type"], "tool_use");
    }

    #[test]
    fn parse_tool_input_handles_empty_and_malformed_inputs() {
        // Empty string → empty object.
        assert_eq!(parse_tool_input(""), serde_json::json!({}));
        // Valid JSON → parsed object.
        assert_eq!(
            parse_tool_input("{\"city\":\"Berlin\"}"),
            serde_json::json!({"city": "Berlin"})
        );
        // Malformed JSON → string node (provider will reject, but no panic).
        assert_eq!(
            parse_tool_input("not json"),
            serde_json::Value::String("not json".into())
        );
    }

    #[test]
    fn extract_tool_calls_pulls_tool_use_blocks_from_response() {
        let blocks = vec![
            ContentBlock {
                block_type: "text".into(),
                text: Some("I'll check the weather.".into()),
                id: None,
                name: None,
                input: None,
            },
            ContentBlock {
                block_type: "tool_use".into(),
                text: None,
                id: Some("tu_01".into()),
                name: Some("get_weather".into()),
                input: Some(serde_json::json!({"city": "Berlin"})),
            },
        ];
        let calls = extract_tool_calls(&blocks);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "tu_01");
        assert_eq!(calls[0].name, "get_weather");
        // The JSON object input round-trips into the gateway's string
        // arguments form for parity with OpenAI.
        let parsed: serde_json::Value = serde_json::from_str(&calls[0].arguments).unwrap();
        assert_eq!(parsed, serde_json::json!({"city": "Berlin"}));
        // extract_text on the same blocks still returns just the text.
        assert_eq!(extract_text(&blocks), "I'll check the weather.");
    }

    #[test]
    fn extract_tool_calls_drops_tool_use_blocks_missing_id_or_name() {
        // Malformed `tool_use` block lacking the id field — skip rather
        // than panic so a partial response still yields the rest.
        let blocks = vec![ContentBlock {
            block_type: "tool_use".into(),
            text: None,
            id: None,
            name: Some("get_weather".into()),
            input: Some(serde_json::json!({})),
        }];
        assert!(extract_tool_calls(&blocks).is_empty());
    }

    #[test]
    fn anthropic_response_deserializes_mixed_text_and_tool_use_blocks() {
        let raw = r#"{
            "content": [
                {"type": "text", "text": "Looking that up."},
                {"type": "tool_use", "id": "tu_01", "name": "get_weather",
                 "input": {"city": "Berlin"}}
            ],
            "stop_reason": "tool_use",
            "usage": {"input_tokens": 12, "output_tokens": 4}
        }"#;
        let resp: AnthropicResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(resp.content.len(), 2);
        let calls = extract_tool_calls(&resp.content);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].name, "get_weather");
    }

    #[tokio::test]
    #[ignore]
    async fn anthropic_chat_integration() {
        use kernel::adapters::capability::ChatModel;
        // Requires ANTHROPIC_API_KEY env var
        let adapter = AnthropicAdapter::new().unwrap();
        let config = RouterConfig {
            url: "https://api.anthropic.com".to_string(),
            api_key_env: Some("ANTHROPIC_API_KEY".to_string()),
            api_key: None,
            enabled: true,
            timeout_ms: Some(30000),
            headers: std::collections::HashMap::new(),
        };
        let req = kernel::types::io::ChatRequest {
            model: Some("claude-haiku-4-5-20250414".to_string()),
            messages: vec![Message::text(
                MessageRole::User,
                "Say hello in one sentence.",
            )],
            system: None,
            max_tokens: Some(64),
            temperature: Some(0.3),
            tools: Vec::new(),
        };

        let response = adapter.chat(&config, &req).await.unwrap();
        assert!(response.content.is_some());
        assert!(!response.content.unwrap().is_empty());
        assert!(response.usage.is_some());
    }
}
