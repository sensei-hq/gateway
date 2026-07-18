//! Canonical OpenAI-compatible chat / embed / streaming core.
//!
//! Every OpenAI-compatible cloud adapter (openai, ollama, grok,
//! together, huggingface, …) speaks the same `/v1/chat/completions` and
//! `/v1/embeddings` wire format. Rather than re-declare the wire types
//! and request/response plumbing in each adapter, this module owns the
//! full-featured variant once (tools + multimodal + streaming-with-tools)
//! and exposes three `pub(crate)` entry points — [`chat`], [`chat_stream`],
//! and [`embed`] — that speak the gateway's typed
//! [`io`](crate::types::io) request/response structs and encapsulate the
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
use serde::{Deserialize, Serialize};

use super::base::{http_json, resolve_api_key};
use crate::types::config::RouterConfig;
use crate::types::cost::TokenUsage;
use crate::types::error::GatewayError;
use crate::types::io::{ChatRequest, ChatResponse};
use crate::types::request::{
    MediaAttachment, MediaSource, Message, MessageContent, MessageRole, StreamChunk,
    StreamingToolCall, ToolCall, ToolDefinition,
};

/// Boxed streaming type shared by the capability traits.
pub(crate) type ChunkStream = Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>;

/// Adapter label used in error mapping on the streaming path (the
/// non-streaming path routes errors through [`http_json`], which labels
/// them `"http"`). The concrete adapter id isn't threaded through the
/// shared core, so this is a generic placeholder — no test asserts on it.
const ADAPTER: &str = "openai_compat";

// ---------------------------------------------------------------------------
// Wire types — OpenAI chat/embed request/response structs
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    stream: bool,
    /// Tool / function definitions the model may call. Wrapped in
    /// `{type: "function", function: {…}}` per OpenAI's wire shape;
    /// omitted entirely when no tools are configured for this turn.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<ChatTool>,
}

#[derive(Debug, Serialize)]
struct ChatTool {
    #[serde(rename = "type")]
    tool_type: &'static str, // always "function" for now
    function: ChatToolFunction,
}

#[derive(Debug, Serialize)]
struct ChatToolFunction {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    parameters: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ChatMessage {
    role: String,
    /// Polymorphic body: a plain string for text-only turns, or an
    /// array of typed content parts (text + image_url) for
    /// multimodal turns. OpenAI also accepts `null` content on
    /// assistant turns that carry only tool calls, so the field is
    /// optional + skip-if-none.
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<ChatContent>,
    /// Tool calls emitted by an assistant turn (mirrored back from a
    /// prior response when continuing the conversation).
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<OpenAiToolCall>>,
    /// Required on `role: "tool"` messages; links the tool result back
    /// to the originating call.
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<String>,
}

/// OpenAI's `content` field is polymorphic — string or array of parts.
/// `serde(untagged)` reproduces that shape: text-only turns serialise
/// as a bare JSON string, multimodal turns as an array.
#[derive(Debug, Serialize)]
#[serde(untagged)]
enum ChatContent {
    Text(String),
    Parts(Vec<ContentPart>),
}

/// One typed entry of a multipart `content` array. Today only `text`
/// and `image_url` are modelled; OpenAI also accepts `input_audio`
/// and `file` entries which can land as separate variants when those
/// capabilities are wired through the gateway.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ContentPart {
    Text { text: String },
    ImageUrl { image_url: ImageUrl },
}

#[derive(Debug, Serialize)]
struct ImageUrl {
    /// HTTPS URL or a `data:` URL with base64-encoded bytes. OpenAI
    /// also supports an optional `detail: "low" | "high" | "auto"`
    /// field — we don't surface it yet on the gateway request side.
    url: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct OpenAiToolCall {
    id: String,
    #[serde(rename = "type")]
    tool_type: String,
    function: OpenAiToolCallFunction,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct OpenAiToolCallFunction {
    name: String,
    /// JSON-encoded argument object — OpenAI emits this as a string,
    /// not a structured object. We keep that shape and round-trip it
    /// verbatim through [`ToolCall::arguments`].
    arguments: String,
}

#[derive(Debug, Serialize)]
struct EmbedRequest {
    model: String,
    input: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
    usage: Option<UsageResponse>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatResponseMessage,
    #[allow(dead_code)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChatResponseMessage {
    content: Option<String>,
    /// Tool calls the assistant decided to emit. Absent for plain
    /// text replies; present (and possibly non-empty) when tools were
    /// advertised on the request.
    #[serde(default)]
    tool_calls: Option<Vec<OpenAiToolCall>>,
}

#[derive(Debug, Deserialize)]
struct UsageResponse {
    prompt_tokens: Option<u32>,
    completion_tokens: Option<u32>,
    total_tokens: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct EmbedResponse {
    data: Vec<EmbedData>,
    usage: Option<UsageResponse>,
}

#[derive(Debug, Deserialize)]
struct EmbedData {
    embedding: Vec<f32>,
}

#[derive(Debug, Deserialize)]
struct StreamChatResponse {
    choices: Vec<StreamChoice>,
    usage: Option<UsageResponse>,
}

#[derive(Debug, Deserialize)]
struct StreamChoice {
    delta: StreamDelta,
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StreamDelta {
    content: Option<String>,
    /// Tool-call deltas. OpenAI emits one of these objects per
    /// in-progress call, keyed by `index`. The `id` and `function.name`
    /// arrive on the first delta for a given index; subsequent deltas
    /// only carry argument fragments (`function.arguments`).
    #[serde(default)]
    tool_calls: Option<Vec<StreamToolCallDelta>>,
}

#[derive(Debug, Deserialize)]
struct StreamToolCallDelta {
    /// Per-call index — same call across deltas share the same index.
    index: u32,
    #[serde(default)]
    id: Option<String>,
    #[allow(dead_code)]
    #[serde(default, rename = "type")]
    tool_type: Option<String>,
    #[serde(default)]
    function: Option<StreamToolCallFunctionDelta>,
}

#[derive(Debug, Deserialize)]
struct StreamToolCallFunctionDelta {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn role_to_string(role: &MessageRole) -> &'static str {
    match role {
        MessageRole::System => "system",
        MessageRole::User => "user",
        MessageRole::Assistant => "assistant",
        MessageRole::Tool => "tool",
    }
}

fn build_chat_messages(messages: &[Message], system: &Option<String>) -> Vec<ChatMessage> {
    let mut out = Vec::new();
    if let Some(sys) = system {
        out.push(ChatMessage {
            role: "system".to_string(),
            content: Some(ChatContent::Text(sys.clone())),
            tool_calls: None,
            tool_call_id: None,
        });
    }
    for m in messages {
        match &m.content {
            MessageContent::ToolResult {
                tool_call_id,
                content,
            } => {
                // Tool results carry the linking id at the OpenAI level
                // and force role=tool regardless of the gateway role
                // (we treat MessageRole::Tool as canonical here).
                out.push(ChatMessage {
                    role: "tool".to_string(),
                    content: Some(ChatContent::Text(content.clone())),
                    tool_calls: None,
                    tool_call_id: Some(tool_call_id.clone()),
                });
            }
            MessageContent::Text { text } => {
                // Assistant turns with tool_calls can have empty / null
                // content; serialize `None` so OpenAI accepts the turn.
                let tool_calls = if m.tool_calls.is_empty() {
                    None
                } else {
                    Some(m.tool_calls.iter().map(to_openai_tool_call).collect())
                };
                let content_field = build_chat_content(text, &m.attachments, tool_calls.is_some());
                out.push(ChatMessage {
                    role: role_to_string(&m.role).to_string(),
                    content: content_field,
                    tool_calls,
                    tool_call_id: None,
                });
            }
        }
    }
    out
}

/// Pick the right `content` shape for a single chat message.
///
/// - No attachments + non-empty text → bare string.
/// - No attachments + empty text + tool_calls present → `None`
///   (assistant tool-call turn with no text body; OpenAI accepts
///   null/omitted content here).
/// - Attachments present → array of typed parts. Text comes first
///   (if non-empty), then one `image_url` part per attachment.
fn build_chat_content(
    text: &str,
    attachments: &[MediaAttachment],
    has_tool_calls: bool,
) -> Option<ChatContent> {
    if attachments.is_empty() {
        if text.is_empty() && has_tool_calls {
            return None;
        }
        return Some(ChatContent::Text(text.to_string()));
    }
    let mut parts: Vec<ContentPart> = Vec::with_capacity(attachments.len() + 1);
    if !text.is_empty() {
        parts.push(ContentPart::Text {
            text: text.to_string(),
        });
    }
    for att in attachments {
        if let Some(part) = attachment_to_part(att) {
            parts.push(part);
        }
    }
    Some(ChatContent::Parts(parts))
}

/// Translate a gateway [`MediaAttachment`] into an OpenAI content
/// part. Base64 sources become a `data:` URL — OpenAI accepts that
/// form everywhere it accepts an `image_url`. URL sources pass
/// through verbatim. Returns `None` for variants we don't yet model.
fn attachment_to_part(att: &MediaAttachment) -> Option<ContentPart> {
    match att {
        MediaAttachment::Image { source, mime_type } => {
            let url = match source {
                MediaSource::Url { url } => url.clone(),
                MediaSource::Base64 { data } => {
                    // OpenAI's image_url accepts a data URL; the MIME
                    // type defaults to `image/jpeg` when unspecified
                    // because the wire shape requires one.
                    let mime = mime_type.as_deref().unwrap_or("image/jpeg");
                    format!("data:{mime};base64,{data}")
                }
            };
            Some(ContentPart::ImageUrl {
                image_url: ImageUrl { url },
            })
        }
    }
}

/// Convert a gateway [`ToolDefinition`] into OpenAI's wire shape
/// (`{type: "function", function: {name, description?, parameters}}`).
fn build_tools(tools: &[ToolDefinition]) -> Vec<ChatTool> {
    tools
        .iter()
        .map(|t| ChatTool {
            tool_type: "function",
            function: ChatToolFunction {
                name: t.name.clone(),
                description: t.description.clone(),
                parameters: t.input_schema.clone(),
            },
        })
        .collect()
}

/// Mirror a gateway [`ToolCall`] onto an [`OpenAiToolCall`] so it can be
/// echoed back to the provider when the caller continues a multi-turn
/// tool-calling conversation.
fn to_openai_tool_call(tc: &ToolCall) -> OpenAiToolCall {
    OpenAiToolCall {
        id: tc.id.clone(),
        tool_type: "function".to_string(),
        function: OpenAiToolCallFunction {
            name: tc.name.clone(),
            arguments: tc.arguments.clone(),
        },
    }
}

/// Convert OpenAI's wire [`OpenAiToolCall`] back to a gateway
/// [`ToolCall`]. Non-function tool types (none today, but the API leaves
/// the door open) are filtered upstream by the caller.
fn from_openai_tool_call(tc: &OpenAiToolCall) -> ToolCall {
    ToolCall {
        id: tc.id.clone(),
        name: tc.function.name.clone(),
        arguments: tc.function.arguments.clone(),
    }
}

fn usage_from_response(usage: &Option<UsageResponse>) -> Option<TokenUsage> {
    usage.as_ref().map(|u| {
        let input = u.prompt_tokens.unwrap_or(0);
        let output = u.completion_tokens.unwrap_or(0);
        let total = u.total_tokens.unwrap_or(input + output);
        TokenUsage {
            input_tokens: input,
            output_tokens: output,
            total_tokens: total,
        }
    })
}

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
        base_url,
        "/v1/chat/completions",
        &body,
        api_key.as_deref(),
        &cfg.headers,
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
        let body_text = response.text().await.unwrap_or_default();
        return Err(match status.as_u16() {
            401 | 403 => GatewayError::Authentication {
                adapter: ADAPTER.into(),
                message: body_text,
            },
            429 => GatewayError::RateLimit {
                adapter: ADAPTER.into(),
                retry_after_ms: None,
            },
            _ => GatewayError::ProviderError {
                adapter: ADAPTER.into(),
                message: body_text,
                status: Some(status.as_u16()),
            },
        });
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
    req: &crate::types::io::EmbedRequest,
) -> Result<crate::types::io::EmbedResponse, GatewayError> {
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
        base_url,
        "/v1/embeddings",
        &body,
        api_key.as_deref(),
        &cfg.headers,
    )
    .await?;

    let embeddings: Vec<Vec<f32>> = resp.data.into_iter().map(|d| d.embedding).collect();
    let usage = usage_from_response(&resp.usage);

    Ok(crate::types::io::EmbedResponse {
        embeddings,
        usage,
        degraded: false,
    })
}

// ---------------------------------------------------------------------------
// Streaming SSE state machine
// ---------------------------------------------------------------------------

/// Persistent state for the OpenAI SSE stream pipeline. Lives across
/// HTTP byte chunks so that:
///
/// - SSE lines split across two HTTP chunks reassemble correctly
///   (`line_buf` accumulates a partial trailing line).
/// - Tool-call argument fragments accumulate per `index` until the
///   chunk carrying `finish_reason` arrives, where we drain the
///   accumulators into a final `StreamChunk.tool_calls`.
/// - Errors and emissions are queued in `pending`, so a single byte
///   chunk can produce zero or more gateway chunks.
struct OpenAiStreamState {
    byte_stream: Pin<Box<dyn Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>,
    line_buf: String,
    tool_calls: BTreeMap<u32, StreamingToolCall>,
    pending: VecDeque<Result<StreamChunk, GatewayError>>,
    eof: bool,
}

/// Drive one byte chunk through the SSE line splitter. Each complete
/// line is handed to [`process_sse_line`]; an incomplete trailing
/// line stays in `line_buf` for the next call.
fn process_stream_bytes(state: &mut OpenAiStreamState, bytes: &[u8]) {
    state.line_buf.push_str(&String::from_utf8_lossy(bytes));
    while let Some(newline_pos) = state.line_buf.find('\n') {
        let mut line = state.line_buf.drain(..=newline_pos).collect::<String>();
        line.truncate(line.trim_end().len());
        process_sse_line(state, line.trim());
    }
}

/// Handle a single SSE `data:` line. Drops empty lines, the
/// `[DONE]` sentinel, and any line that fails to parse — emitting
/// chunks into `state.pending` when a parsed event carries data the
/// caller cares about.
fn process_sse_line(state: &mut OpenAiStreamState, line: &str) {
    if line.is_empty() || line == "data: [DONE]" {
        return;
    }
    let payload = line.strip_prefix("data: ").unwrap_or(line);
    let parsed = match serde_json::from_str::<StreamChatResponse>(payload) {
        Ok(v) => v,
        Err(_) => return,
    };
    let usage = usage_from_response(&parsed.usage);
    let Some(choice) = parsed.choices.first() else {
        return;
    };

    // Absorb any tool-call deltas into the per-index accumulator. id +
    // function.name arrive on the first delta for an index; subsequent
    // deltas only carry argument fragments.
    if let Some(deltas) = choice.delta.tool_calls.as_ref() {
        for d in deltas {
            let acc = state.tool_calls.entry(d.index).or_default();
            if let Some(id) = &d.id {
                acc.id = Some(id.clone());
            }
            if let Some(func) = &d.function {
                if let Some(name) = &func.name {
                    acc.name = Some(name.clone());
                }
                if let Some(args) = &func.arguments {
                    acc.push_arguments(args);
                }
            }
        }
    }

    let content = choice.delta.content.clone().unwrap_or_default();

    // If finish_reason arrives, materialise any accumulated calls onto
    // this chunk so the caller sees them alongside the terminal
    // finish_reason. Otherwise we'd lose them — OpenAI doesn't repeat
    // tool_calls on the close.
    let finish_reason = choice.finish_reason.clone();
    let tool_calls = if finish_reason.is_some() {
        std::mem::take(&mut state.tool_calls)
            .into_values()
            .filter_map(StreamingToolCall::finalize)
            .collect()
    } else {
        Vec::new()
    };

    // Empty content + no finish + no usage + no tool_calls is a pure
    // framing event; don't surface it.
    if content.is_empty() && finish_reason.is_none() && usage.is_none() && tool_calls.is_empty() {
        return;
    }

    state.pending.push_back(Ok(StreamChunk {
        content,
        finish_reason,
        usage,
        tool_calls,
    }));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_chat_request() {
        let messages = vec![Message::text(MessageRole::User, "Hello")];
        let system = Some("You are helpful.".to_string());
        let chat_messages = build_chat_messages(&messages, &system);

        let body = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: chat_messages,
            max_tokens: Some(1024),
            temperature: Some(0.7),
            stream: false,
            tools: Vec::new(),
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["model"], "gpt-4o");
        assert_eq!(json["stream"], false);
        assert_eq!(json["max_tokens"], 1024);
        assert!(
            json["temperature"].as_f64().unwrap() > 0.69
                && json["temperature"].as_f64().unwrap() < 0.71
        );

        let msgs = json["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0]["role"], "system");
        assert_eq!(msgs[0]["content"], "You are helpful.");
        assert_eq!(msgs[1]["role"], "user");
        assert_eq!(msgs[1]["content"], "Hello");
        // tools omitted when empty
        assert!(json.get("tools").is_none());
        // tool_calls / tool_call_id omitted on plain messages
        assert!(msgs[1].get("tool_calls").is_none());
        assert!(msgs[1].get("tool_call_id").is_none());
    }

    #[test]
    fn build_tools_wraps_each_definition_in_function_envelope() {
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
        assert_eq!(arr[0]["type"], "function");
        assert_eq!(arr[0]["function"]["name"], "get_weather");
        assert_eq!(
            arr[0]["function"]["description"],
            "Look up the weather for a city."
        );
        assert_eq!(arr[0]["function"]["parameters"]["type"], "object");
        // description omitted entirely when None
        assert!(arr[1]["function"].get("description").is_none());
    }

    #[test]
    fn build_chat_messages_maps_tool_result_to_role_tool_with_tool_call_id() {
        let msgs = vec![Message::tool_result("call_abc", "{\"weather\":\"sunny\"}")];
        let out = build_chat_messages(&msgs, &None);
        let json = serde_json::to_value(&out).unwrap();
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["role"], "tool");
        assert_eq!(arr[0]["tool_call_id"], "call_abc");
        assert_eq!(arr[0]["content"], "{\"weather\":\"sunny\"}");
        assert!(arr[0].get("tool_calls").is_none());
    }

    #[test]
    fn build_chat_messages_echoes_assistant_tool_calls_back_to_the_wire() {
        let msg = Message {
            role: MessageRole::Assistant,
            content: MessageContent::Text {
                text: String::new(),
            },
            tool_calls: vec![ToolCall {
                id: "call_1".into(),
                name: "get_weather".into(),
                arguments: "{\"city\":\"Berlin\"}".into(),
            }],
            attachments: vec![],
        };
        let out = build_chat_messages(&[msg], &None);
        let json = serde_json::to_value(&out).unwrap();
        let arr = json.as_array().unwrap();
        assert_eq!(arr[0]["role"], "assistant");
        // Empty text body + non-empty tool_calls → content serialized as null/omitted
        assert!(arr[0].get("content").is_none());
        let calls = arr[0]["tool_calls"].as_array().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["id"], "call_1");
        assert_eq!(calls[0]["type"], "function");
        assert_eq!(calls[0]["function"]["name"], "get_weather");
        assert_eq!(calls[0]["function"]["arguments"], "{\"city\":\"Berlin\"}");
    }

    #[test]
    fn build_chat_messages_keeps_string_content_when_no_attachments() {
        // Text-only turns must serialise content as a bare string, not
        // an array — OpenAI accepts both but mixing shapes when
        // unnecessary needlessly enlarges every request.
        let msgs = vec![Message::text(MessageRole::User, "hello")];
        let json = serde_json::to_value(build_chat_messages(&msgs, &None)).unwrap();
        assert_eq!(json[0]["content"], "hello");
        assert!(
            json[0]["content"].as_array().is_none(),
            "text-only must stay a string"
        );
    }

    #[test]
    fn build_chat_messages_emits_array_content_when_attachment_present() {
        let msg = Message::text(MessageRole::User, "what's in this?")
            .with_attachment(MediaAttachment::image_url("https://ex.com/cat.jpg"));
        let json = serde_json::to_value(build_chat_messages(&[msg], &None)).unwrap();
        let parts = json[0]["content"].as_array().expect("array form");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0]["type"], "text");
        assert_eq!(parts[0]["text"], "what's in this?");
        assert_eq!(parts[1]["type"], "image_url");
        assert_eq!(parts[1]["image_url"]["url"], "https://ex.com/cat.jpg");
    }

    #[test]
    fn build_chat_messages_omits_empty_text_part_when_only_attachment_present() {
        let mut msg = Message::text(MessageRole::User, "");
        msg.attachments
            .push(MediaAttachment::image_url("https://ex.com/x.png"));
        let json = serde_json::to_value(build_chat_messages(&[msg], &None)).unwrap();
        let parts = json[0]["content"].as_array().unwrap();
        // Empty text would just be noise; the image_url part is the
        // entire body.
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0]["type"], "image_url");
    }

    #[test]
    fn build_chat_messages_encodes_base64_attachment_as_data_url() {
        // OpenAI accepts base64 only via a `data:<mime>;base64,<data>`
        // URL — same field as the URL case. We synthesise the data
        // URL from the gateway's MediaSource::Base64.
        let msg = Message::text(MessageRole::User, "see this")
            .with_attachment(MediaAttachment::image_base64("Zm9v", "image/png"));
        let json = serde_json::to_value(build_chat_messages(&[msg], &None)).unwrap();
        let parts = json[0]["content"].as_array().unwrap();
        assert_eq!(parts[1]["type"], "image_url");
        let url = parts[1]["image_url"]["url"].as_str().unwrap();
        assert!(url.starts_with("data:image/png;base64,"), "got: {url}");
        assert!(url.ends_with("Zm9v"));
    }

    #[test]
    fn build_chat_messages_defaults_mime_type_for_base64_without_one() {
        // Base64 without a mime type still has to ship something —
        // OpenAI requires a media type in the data URL. We default to
        // image/jpeg, which matches OpenAI's own conservative default
        // for the legacy `image_url` shape.
        let msg = Message::text(MessageRole::User, "x").with_attachment(MediaAttachment::Image {
            source: MediaSource::Base64 {
                data: "AAAA".into(),
            },
            mime_type: None,
        });
        let json = serde_json::to_value(build_chat_messages(&[msg], &None)).unwrap();
        let url = json[0]["content"][1]["image_url"]["url"].as_str().unwrap();
        assert!(url.starts_with("data:image/jpeg;base64,"));
    }

    #[test]
    fn chat_request_includes_tools_when_supplied() {
        let messages = vec![Message::text(MessageRole::User, "What's the weather?")];
        let tools = vec![ToolDefinition {
            name: "get_weather".into(),
            description: Some("Look up the weather.".into()),
            input_schema: serde_json::json!({"type": "object"}),
        }];
        let body = ChatCompletionRequest {
            model: "gpt-4o".to_string(),
            messages: build_chat_messages(&messages, &None),
            max_tokens: None,
            temperature: None,
            stream: false,
            tools: build_tools(&tools),
        };
        let json = serde_json::to_value(&body).unwrap();
        let arr = json["tools"].as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["function"]["name"], "get_weather");
    }

    #[test]
    fn from_openai_tool_call_strips_function_envelope() {
        let wire = OpenAiToolCall {
            id: "call_1".into(),
            tool_type: "function".into(),
            function: OpenAiToolCallFunction {
                name: "get_weather".into(),
                arguments: "{\"city\":\"Berlin\"}".into(),
            },
        };
        let tc = from_openai_tool_call(&wire);
        assert_eq!(tc.id, "call_1");
        assert_eq!(tc.name, "get_weather");
        assert_eq!(tc.arguments, "{\"city\":\"Berlin\"}");
    }

    #[test]
    fn chat_response_message_deserializes_tool_calls_field() {
        let raw = r#"{
            "content": null,
            "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {"name": "get_weather", "arguments": "{\"city\":\"Berlin\"}"}
            }]
        }"#;
        let msg: ChatResponseMessage = serde_json::from_str(raw).unwrap();
        assert!(msg.content.is_none());
        let calls = msg.tool_calls.unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "call_1");
        assert_eq!(calls[0].function.name, "get_weather");
    }

    #[test]
    fn build_embed_request() {
        let body = EmbedRequest {
            model: "text-embedding-3-small".to_string(),
            input: vec!["hello world".to_string(), "foo bar".to_string()],
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["model"], "text-embedding-3-small");
        let input = json["input"].as_array().unwrap();
        assert_eq!(input.len(), 2);
        assert_eq!(input[0], "hello world");
        assert_eq!(input[1], "foo bar");
    }

    #[test]
    fn parse_chat_response() {
        let json = r#"{
            "choices": [{
                "message": {"content": "Hello there!"},
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 12,
                "completion_tokens": 8,
                "total_tokens": 20
            }
        }"#;

        let resp: ChatCompletionResponse = serde_json::from_str(json).unwrap();

        assert_eq!(resp.choices.len(), 1);
        assert_eq!(
            resp.choices[0].message.content.as_deref(),
            Some("Hello there!"),
        );
        assert_eq!(resp.choices[0].finish_reason.as_deref(), Some("stop"));

        let usage = usage_from_response(&resp.usage).unwrap();
        assert_eq!(usage.input_tokens, 12);
        assert_eq!(usage.output_tokens, 8);
        assert_eq!(usage.total_tokens, 20);
    }

    #[test]
    fn parse_embed_response() {
        let json = r#"{
            "data": [
                {"embedding": [0.1, 0.2, 0.3], "index": 0},
                {"embedding": [0.4, 0.5, 0.6], "index": 1}
            ],
            "usage": {
                "prompt_tokens": 8,
                "total_tokens": 8
            }
        }"#;

        let resp: EmbedResponse = serde_json::from_str(json).unwrap();

        assert_eq!(resp.data.len(), 2);
        assert_eq!(resp.data[0].embedding, vec![0.1, 0.2, 0.3]);
        assert_eq!(resp.data[1].embedding, vec![0.4, 0.5, 0.6]);

        let usage = usage_from_response(&resp.usage).unwrap();
        assert_eq!(usage.input_tokens, 8);
        assert_eq!(usage.total_tokens, 8);
        // completion_tokens absent in embed responses
        assert_eq!(usage.output_tokens, 0);
    }

    #[test]
    fn parse_stream_chunk() {
        let json = r#"{"choices":[{"delta":{"content":"Hi"},"finish_reason":null}]}"#;

        let resp: StreamChatResponse = serde_json::from_str(json).unwrap();

        assert_eq!(resp.choices.len(), 1);
        assert_eq!(resp.choices[0].delta.content.as_deref(), Some("Hi"));
        assert!(resp.choices[0].finish_reason.is_none());
    }

    fn empty_stream_state() -> OpenAiStreamState {
        OpenAiStreamState {
            byte_stream: Box::pin(futures::stream::empty()),
            line_buf: String::new(),
            tool_calls: BTreeMap::new(),
            pending: VecDeque::new(),
            eof: false,
        }
    }

    #[test]
    fn process_sse_line_buffers_text_content_into_chunks() {
        let mut state = empty_stream_state();
        process_sse_line(
            &mut state,
            r#"data: {"choices":[{"delta":{"content":"Hello"},"finish_reason":null}]}"#,
        );
        assert_eq!(state.pending.len(), 1);
        let chunk = state.pending.pop_front().unwrap().unwrap();
        assert_eq!(chunk.content, "Hello");
        assert!(chunk.finish_reason.is_none());
        assert!(chunk.tool_calls.is_empty());
    }

    #[test]
    fn process_sse_line_skips_empty_and_done_sentinels() {
        let mut state = empty_stream_state();
        process_sse_line(&mut state, "");
        process_sse_line(&mut state, "data: [DONE]");
        // Pure-framing chunks (no content, no finish, no usage, no tool_calls)
        // are also dropped.
        process_sse_line(
            &mut state,
            r#"data: {"choices":[{"delta":{},"finish_reason":null}]}"#,
        );
        assert!(state.pending.is_empty());
    }

    #[test]
    fn process_sse_line_accumulates_tool_call_fragments_across_deltas() {
        let mut state = empty_stream_state();
        // First delta: id + name arrive, args start.
        process_sse_line(
            &mut state,
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"ci"}}]},"finish_reason":null}]}"#,
        );
        // Argument fragment continues.
        process_sse_line(
            &mut state,
            r#"data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"ty\":\"Berlin\"}"}}]},"finish_reason":null}]}"#,
        );
        // No chunks emitted yet — finish_reason hasn't arrived.
        assert!(state.pending.is_empty());

        // Final delta with finish_reason="tool_calls" triggers
        // assembly + emission.
        process_sse_line(
            &mut state,
            r#"data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}"#,
        );
        assert_eq!(state.pending.len(), 1);
        let chunk = state.pending.pop_front().unwrap().unwrap();
        assert_eq!(chunk.finish_reason.as_deref(), Some("tool_calls"));
        assert_eq!(chunk.tool_calls.len(), 1);
        let call = &chunk.tool_calls[0];
        assert_eq!(call.id, "call_1");
        assert_eq!(call.name, "get_weather");
        // Fragments concatenated into a single valid JSON args string.
        assert_eq!(call.arguments, r#"{"city":"Berlin"}"#);
    }

    #[test]
    fn process_sse_line_handles_multiple_parallel_tool_calls_by_index() {
        let mut state = empty_stream_state();
        process_sse_line(
            &mut state,
            r#"data: {"choices":[{"delta":{"tool_calls":[
                {"index":0,"id":"call_a","function":{"name":"f1","arguments":"{}"}},
                {"index":1,"id":"call_b","function":{"name":"f2","arguments":"{}"}}
            ]},"finish_reason":null}]}"#,
        );
        process_sse_line(
            &mut state,
            r#"data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}"#,
        );
        let chunk = state.pending.pop_front().unwrap().unwrap();
        assert_eq!(chunk.tool_calls.len(), 2);
        // Sorted by index (BTreeMap) — call_a first, call_b second.
        assert_eq!(chunk.tool_calls[0].id, "call_a");
        assert_eq!(chunk.tool_calls[1].id, "call_b");
    }

    #[test]
    fn process_stream_bytes_reassembles_lines_split_across_chunks() {
        let mut state = empty_stream_state();
        // First HTTP byte chunk: half of a data line, no newline.
        process_stream_bytes(&mut state, br#"data: {"choices":[{"delta":{"conten"#);
        assert!(state.pending.is_empty(), "no complete line yet");
        // Second HTTP byte chunk: rest of the line + newline.
        process_stream_bytes(&mut state, b"t\":\"Hi\"},\"finish_reason\":null}]}\n");
        assert_eq!(state.pending.len(), 1);
        let chunk = state.pending.pop_front().unwrap().unwrap();
        assert_eq!(chunk.content, "Hi");
    }
}
