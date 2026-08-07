//! Pure wire-format conversion between gateway types and the Bedrock
//! Converse SDK types.
//!
//! Everything here is a total function of its inputs — content-block ↔
//! SDK-type mapping (request messages in, `ConverseResponse` / stream
//! events out), plus SDK-error → [`GatewayError`] mapping. None of it
//! touches HTTP, auth, or adapter state (the `Client`, credentials, and
//! `RouterConfig` plumbing all stay in `super`), which is what makes it
//! unit-testable without an SDK client.

use super::*;

use aws_sdk_bedrockruntime::{
    operation::converse::ConverseOutput as ConverseResponse,
    types::{
        ContentBlock, ContentBlockDelta, ConversationRole, ConverseOutput as ConverseOutputUnion,
        ConverseStreamOutput, ImageBlock, ImageFormat, ImageSource, Message, S3Location,
        SystemContentBlock, Tool, ToolConfiguration, ToolInputSchema, ToolResultBlock,
        ToolResultContentBlock, ToolSpecification, ToolUseBlock,
    },
};
use aws_smithy_types::{Document, Number};
use base64::Engine;

use kernel::types::request::{
    MediaAttachment, MediaSource, Message as GwMessage, MessageContent, MessageRole, ToolCall,
    ToolDefinition,
};

// ---------------------------------------------------------------------------
// Streaming: SDK event → StreamChunk
// ---------------------------------------------------------------------------

/// Map a single Converse stream event to a [`StreamChunk`] (when
/// there's something to surface) and update the per-index tool-call
/// accumulator map.
pub(super) fn chunk_from_event(
    event: &ConverseStreamOutput,
    tool_calls: &mut BTreeMap<u32, StreamingToolCall>,
) -> Option<StreamChunk> {
    match event {
        // Opening a content block — only ToolUse seeds an
        // accumulator. The SDK exposes the tool_use_id + name on the
        // start event; subsequent deltas only carry input fragments.
        ConverseStreamOutput::ContentBlockStart(ev) => {
            if let Some(aws_sdk_bedrockruntime::types::ContentBlockStart::ToolUse(tu)) = ev.start()
            {
                tool_calls.insert(
                    ev.content_block_index().max(0) as u32,
                    StreamingToolCall::new(tu.tool_use_id(), tu.name()),
                );
            }
            None
        }
        ConverseStreamOutput::ContentBlockDelta(ev) => {
            let delta = ev.delta()?;
            match delta {
                ContentBlockDelta::Text(text) => Some(StreamChunk {
                    content: text.clone(),
                    finish_reason: None,
                    usage: None,
                    tool_calls: Vec::new(),
                }),
                ContentBlockDelta::ToolUse(tu) => {
                    let idx = ev.content_block_index().max(0) as u32;
                    if let Some(acc) = tool_calls.get_mut(&idx) {
                        acc.push_arguments(tu.input());
                    }
                    None
                }
                // ToolResult / Image / Reasoning / Citation deltas
                // aren't surfaced in v1.
                _ => None,
            }
        }
        // ContentBlockStop is framing — we hold accumulators alive
        // until MessageStop so all tool calls finalise together on
        // the terminal chunk.
        ConverseStreamOutput::ContentBlockStop(_) => None,
        ConverseStreamOutput::MessageStop(ev) => {
            let calls: Vec<ToolCall> = std::mem::take(tool_calls)
                .into_values()
                .filter_map(StreamingToolCall::finalize)
                .collect();
            Some(StreamChunk {
                content: String::new(),
                finish_reason: Some(ev.stop_reason().as_str().to_string()),
                usage: None,
                tool_calls: calls,
            })
        }
        ConverseStreamOutput::Metadata(ev) => ev.usage().map(|u| StreamChunk {
            content: String::new(),
            finish_reason: None,
            usage: Some(TokenUsage {
                input_tokens: u.input_tokens.max(0) as u32,
                output_tokens: u.output_tokens.max(0) as u32,
                total_tokens: u.total_tokens.max(0) as u32,
            }),
            tool_calls: Vec::new(),
        }),
        // MessageStart and any Unknown variant — routine framing.
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Request: gateway messages → Bedrock content blocks
// ---------------------------------------------------------------------------

fn role_to_bedrock(role: &MessageRole) -> Option<ConversationRole> {
    match role {
        MessageRole::User | MessageRole::Tool => Some(ConversationRole::User),
        MessageRole::Assistant => Some(ConversationRole::Assistant),
        MessageRole::System => None, // hoisted into SystemContentBlock
    }
}

/// Convert gateway messages into Bedrock Messages. System-role messages
/// are dropped here — they're hoisted into the `system` parameter by
/// [`build_system`].
///
/// Each gateway message becomes one Bedrock [`Message`] with a content
/// list composed by [`build_content_blocks`]: text + tool_use blocks
/// from `Message.content` and `Message.tool_calls`, or a tool_result
/// block for `MessageContent::ToolResult`. Empty content lists are
/// dropped — Bedrock rejects messages without content blocks.
pub(super) fn build_messages(messages: &[GwMessage]) -> Result<Vec<Message>, GatewayError> {
    let mut out = Vec::new();
    for m in messages {
        let Some(role) = role_to_bedrock(&m.role) else {
            continue;
        };
        let blocks = build_content_blocks(m)?;
        if blocks.is_empty() {
            continue;
        }
        let mut builder = Message::builder().role(role);
        for block in blocks {
            builder = builder.content(block);
        }
        out.push(
            builder
                .build()
                .map_err(|e| BedrockAdapter::err(format!("build Bedrock message: {e}"), None))?,
        );
    }
    Ok(out)
}

/// Compose the [`ContentBlock`] list for one gateway message.
///
/// Bedrock packs the entire message body — text, tool_use emissions,
/// and tool_result responses — into a single Vec of content blocks
/// per message. The block order mirrors what we'd expect to see on
/// the wire: text first, tool_use blocks last.
///
/// Returns `Err` when an attachment can't be honoured as a hard failure
/// (e.g. an image whose base64 fails to decode) — see
/// [`attachment_to_block`].
fn build_content_blocks(m: &GwMessage) -> Result<Vec<ContentBlock>, GatewayError> {
    let mut blocks: Vec<ContentBlock> = Vec::new();
    match &m.content {
        MessageContent::Text { text } => {
            if !text.is_empty() {
                blocks.push(ContentBlock::Text(text.clone()));
            }
            for att in &m.attachments {
                if let Some(block) = attachment_to_block(att)? {
                    blocks.push(block);
                }
            }
        }
        MessageContent::ToolResult {
            tool_call_id,
            content,
        } => {
            // Bedrock wraps tool result content in a list of
            // ToolResultContentBlocks. We emit a single block; JSON
            // bodies surface as Json blocks so the model can introspect
            // structure, plain strings as Text.
            let inner = match serde_json::from_str::<serde_json::Value>(content) {
                Ok(v) if v.is_object() || v.is_array() => {
                    ToolResultContentBlock::Json(json_to_document(v))
                }
                _ => ToolResultContentBlock::Text(content.clone()),
            };
            match ToolResultBlock::builder()
                .tool_use_id(tool_call_id.clone())
                .content(inner)
                .build()
            {
                Ok(block) => blocks.push(ContentBlock::ToolResult(block)),
                Err(_) => {
                    // ToolResultBlock requires tool_use_id; we always
                    // set it above, so a build error here is structural
                    // and we drop the block rather than panic.
                }
            }
        }
    }
    for tc in &m.tool_calls {
        match ToolUseBlock::builder()
            .tool_use_id(tc.id.clone())
            .name(tc.name.clone())
            .input(json_to_document(parse_tool_input(&tc.arguments)))
            .build()
        {
            Ok(block) => blocks.push(ContentBlock::ToolUse(block)),
            Err(_) => {
                // All three required fields are populated above; skip
                // on the impossible build-error path.
            }
        }
    }
    Ok(blocks)
}

/// Parse the gateway's JSON-string `arguments` payload into a JSON
/// value. Empty / malformed input becomes an empty object — Bedrock
/// requires a JSON object for tool inputs.
fn parse_tool_input(args: &str) -> serde_json::Value {
    if args.is_empty() {
        return serde_json::json!({});
    }
    serde_json::from_str(args).unwrap_or_else(|_| serde_json::json!({}))
}

/// Translate a gateway [`MediaAttachment`] into a Bedrock content
/// block. Bedrock's Converse API only accepts two source shapes:
///
/// - Inline bytes (`ImageSource::Bytes(Blob)`) — base64 sources land
///   here after decode.
/// - S3 reference (`ImageSource::S3Location`) — `s3://` URLs are
///   translated; the bucketOwner field stays unset (relies on the
///   credential's own permissions).
///
/// HTTPS URLs are dropped with a `tracing::warn` log — Bedrock won't
/// fetch them and there's no useful default the adapter can pick
/// without round-tripping to fetch the bytes itself. Callers that
/// need to attach a remote HTTPS image should download it client-side
/// and pass it as `MediaAttachment::image_base64`.
///
/// Returns `Ok(None)` for attachments that are intentionally skipped
/// (HTTPS URLs, structural SDK build failures) and `Err` for a payload
/// the caller should fix — specifically an image whose base64 fails to
/// decode. Dropping an undecodable image silently would ship the model a
/// prompt that references an image it never received, so we surface it.
fn attachment_to_block(att: &MediaAttachment) -> Result<Option<ContentBlock>, GatewayError> {
    let MediaAttachment::Image { source, mime_type } = att;
    let format = image_format_from_mime(mime_type.as_deref()).unwrap_or(ImageFormat::Jpeg);
    let image_source = match source {
        MediaSource::Base64 { data } => {
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(data.as_bytes())
                .map_err(|e| {
                    BedrockAdapter::err(format!("invalid base64 image attachment: {e}"), None)
                })?;
            ImageSource::Bytes(Blob::new(decoded))
        }
        MediaSource::Url { url } => {
            if url.starts_with("s3://") {
                // S3Location.uri wants the full `s3://bucket/key` URI.
                match S3Location::builder().uri(url.clone()).build() {
                    Ok(loc) => ImageSource::S3Location(loc),
                    // Structural build failure (uri is always set) — drop
                    // rather than fail the whole request.
                    Err(_) => return Ok(None),
                }
            } else {
                tracing::warn!(
                    adapter = ADAPTER_ID,
                    url = %url,
                    "dropping URL image attachment — Bedrock Converse only accepts inline bytes or s3:// references; pass base64 instead",
                );
                return Ok(None);
            }
        }
    };
    match ImageBlock::builder()
        .format(format)
        .source(image_source)
        .build()
    {
        Ok(block) => Ok(Some(ContentBlock::Image(block))),
        // Structural build failure — format + source are both set above.
        Err(_) => Ok(None),
    }
}

/// Map a MIME type string to Bedrock's `ImageFormat` enum. Returns
/// `None` for unknown / missing inputs so the caller can fall back
/// to a sensible default.
fn image_format_from_mime(mime: Option<&str>) -> Option<ImageFormat> {
    match mime?.to_ascii_lowercase().as_str() {
        "image/jpeg" | "image/jpg" => Some(ImageFormat::Jpeg),
        "image/png" => Some(ImageFormat::Png),
        "image/gif" => Some(ImageFormat::Gif),
        "image/webp" => Some(ImageFormat::Webp),
        _ => None,
    }
}

/// Convert gateway [`ToolDefinition`]s into a Bedrock
/// [`ToolConfiguration`]. Returns `None` for an empty definition
/// list so the caller can skip setting `tool_config` on the request
/// entirely (Bedrock rejects an empty toolConfig).
pub(super) fn build_tool_config(tools: &[ToolDefinition]) -> Option<ToolConfiguration> {
    if tools.is_empty() {
        return None;
    }
    let mut builder = ToolConfiguration::builder();
    for t in tools {
        let mut spec = ToolSpecification::builder()
            .name(t.name.clone())
            .input_schema(ToolInputSchema::Json(json_to_document(
                t.input_schema.clone(),
            )));
        if let Some(desc) = &t.description {
            spec = spec.description(desc.clone());
        }
        if let Ok(built) = spec.build() {
            builder = builder.tools(Tool::ToolSpec(built));
        }
    }
    builder.build().ok()
}

// ---------------------------------------------------------------------------
// Response: Bedrock ConverseResponse → gateway content / ToolCall
// ---------------------------------------------------------------------------

/// Pull `tool_use` blocks out of the Converse response and convert
/// them into gateway [`ToolCall`]s. Bedrock natively carries an id
/// per call, so we surface it directly; arguments are re-serialised
/// into the gateway's JSON-string form for parity with OpenAI.
pub(super) fn extract_tool_calls(response: &ConverseResponse) -> Vec<ToolCall> {
    let Some(output) = response.output.as_ref() else {
        return Vec::new();
    };
    let ConverseOutputUnion::Message(msg) = output else {
        return Vec::new();
    };
    msg.content
        .iter()
        .filter_map(|cb| match cb {
            ContentBlock::ToolUse(tu) => Some(ToolCall {
                id: tu.tool_use_id().to_string(),
                name: tu.name().to_string(),
                arguments: serde_json::to_string(&document_to_json(tu.input())).unwrap_or_default(),
            }),
            _ => None,
        })
        .collect()
}

/// Recursively convert a [`serde_json::Value`] into an
/// [`aws_smithy_types::Document`]. Both have the same JSON-shaped
/// tree — only the number wrapping differs.
fn json_to_document(v: serde_json::Value) -> Document {
    match v {
        serde_json::Value::Null => Document::Null,
        serde_json::Value::Bool(b) => Document::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                Document::Number(Number::PosInt(u))
            } else if let Some(i) = n.as_i64() {
                Document::Number(Number::NegInt(i))
            } else if let Some(f) = n.as_f64() {
                Document::Number(Number::Float(f))
            } else {
                Document::Null
            }
        }
        serde_json::Value::String(s) => Document::String(s),
        serde_json::Value::Array(arr) => {
            Document::Array(arr.into_iter().map(json_to_document).collect())
        }
        serde_json::Value::Object(map) => Document::Object(
            map.into_iter()
                .map(|(k, v)| (k, json_to_document(v)))
                .collect(),
        ),
    }
}

/// Inverse of [`json_to_document`]. Floats that don't fit a JSON
/// number (NaN / infinity) degrade to Null, mirroring serde_json's
/// own behaviour.
fn document_to_json(d: &Document) -> serde_json::Value {
    match d {
        Document::Null => serde_json::Value::Null,
        Document::Bool(b) => serde_json::Value::Bool(*b),
        Document::Number(n) => match n {
            Number::PosInt(u) => serde_json::Value::Number((*u).into()),
            Number::NegInt(i) => serde_json::Value::Number((*i).into()),
            Number::Float(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
        },
        Document::String(s) => serde_json::Value::String(s.clone()),
        Document::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(document_to_json).collect())
        }
        Document::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), document_to_json(v)))
                .collect(),
        ),
    }
}

/// Build the system-prompt blocks. The explicit `system` field on the
/// payload wins; otherwise concatenate any `MessageRole::System`
/// messages into separate blocks.
pub(super) fn build_system(
    messages: &[GwMessage],
    system: &Option<String>,
) -> Vec<SystemContentBlock> {
    if let Some(s) = system.as_deref() {
        return vec![SystemContentBlock::Text(s.to_string())];
    }
    messages
        .iter()
        .filter(|m| m.role == MessageRole::System)
        .map(|m| SystemContentBlock::Text(m.as_text().to_string()))
        .collect()
}

/// Pull all text out of the Converse response, concatenating across
/// content blocks. Bedrock can return a mix of `Text`, `ToolUse`,
/// `Image`, etc.; we surface only the text in this commit.
pub(super) fn extract_text(response: &ConverseResponse) -> String {
    let Some(output) = response.output.as_ref() else {
        return String::new();
    };
    let ConverseOutputUnion::Message(msg) = output else {
        return String::new();
    };
    msg.content
        .iter()
        .filter_map(|cb| match cb {
            ContentBlock::Text(t) => Some(t.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

// ---------------------------------------------------------------------------
// SDK error → GatewayError
// ---------------------------------------------------------------------------

/// Map AWS SDK errors to the closest [`GatewayError`] variant. We don't
/// have access to HTTP status codes once the SDK has parsed the
/// response, so the matching is service-error-name based.
pub(super) fn map_sdk_error<E: std::error::Error + Send + Sync + 'static>(
    err: aws_sdk_bedrockruntime::error::SdkError<E>,
) -> GatewayError {
    use aws_sdk_bedrockruntime::error::SdkError;
    let message = match &err {
        SdkError::ServiceError(svc) => format!("service error: {}", svc.err()),
        SdkError::DispatchFailure(_) => "dispatch failure (network)".into(),
        SdkError::TimeoutError(_) => "timeout".into(),
        SdkError::ResponseError(_) => "response parse error".into(),
        SdkError::ConstructionFailure(_) => "request construction failure".into(),
        _ => "unknown SDK error".into(),
    };
    // ThrottlingException is the SigV4 / Bedrock rate-limit signal; we
    // best-effort match on the rendered message rather than poking at
    // the SDK's typed errors (which differ per operation).
    if message.contains("ThrottlingException") || message.contains("TooManyRequests") {
        return GatewayError::RateLimit {
            adapter: ADAPTER_ID.into(),
            retry_after_ms: None,
        };
    }
    if message.contains("AccessDenied")
        || message.contains("UnauthorizedOperation")
        || message.contains("missing credentials")
    {
        return GatewayError::Authentication {
            adapter: ADAPTER_ID.into(),
            message,
        };
    }
    GatewayError::ProviderError {
        adapter: ADAPTER_ID.into(),
        message,
        status: None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn user(content: &str) -> GwMessage {
        GwMessage::text(MessageRole::User, content)
    }

    fn assistant(content: &str) -> GwMessage {
        GwMessage::text(MessageRole::Assistant, content)
    }

    fn system_msg(content: &str) -> GwMessage {
        GwMessage::text(MessageRole::System, content)
    }

    #[test]
    fn role_to_bedrock_maps_user_and_assistant_and_drops_system() {
        assert!(matches!(
            role_to_bedrock(&MessageRole::User),
            Some(ConversationRole::User)
        ));
        assert!(matches!(
            role_to_bedrock(&MessageRole::Tool),
            Some(ConversationRole::User)
        ));
        assert!(matches!(
            role_to_bedrock(&MessageRole::Assistant),
            Some(ConversationRole::Assistant)
        ));
        assert!(role_to_bedrock(&MessageRole::System).is_none());
    }

    #[test]
    fn build_messages_skips_system_role_and_preserves_order() {
        let msgs = vec![
            system_msg("be concise"),
            user("hi"),
            assistant("hello"),
            user("how are you"),
        ];
        let bedrock_msgs = build_messages(&msgs).unwrap();
        assert_eq!(bedrock_msgs.len(), 3);
        assert!(matches!(bedrock_msgs[0].role, ConversationRole::User));
        assert!(matches!(bedrock_msgs[1].role, ConversationRole::Assistant));
        assert!(matches!(bedrock_msgs[2].role, ConversationRole::User));
        // Each message has a single text content block carrying the
        // original content string.
        match &bedrock_msgs[0].content[0] {
            ContentBlock::Text(t) => assert_eq!(t, "hi"),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[test]
    fn build_system_prefers_explicit_field_over_messages() {
        let msgs = vec![system_msg("inline rules"), user("hi")];
        let explicit = Some("explicit rules".to_string());
        let blocks = build_system(&msgs, &explicit);
        assert_eq!(blocks.len(), 1);
        match &blocks[0] {
            SystemContentBlock::Text(t) => assert_eq!(t, "explicit rules"),
            other => panic!("expected Text, got {other:?}"),
        }
    }

    #[test]
    fn build_system_falls_back_to_system_role_messages() {
        let msgs = vec![system_msg("rule one"), system_msg("rule two"), user("hi")];
        let blocks = build_system(&msgs, &None);
        assert_eq!(blocks.len(), 2);
    }

    #[test]
    fn build_system_returns_empty_when_no_system_present() {
        let msgs = vec![user("hi")];
        let blocks = build_system(&msgs, &None);
        assert!(blocks.is_empty());
    }

    #[test]
    fn empty_messages_produce_empty_bedrock_list() {
        let msgs: Vec<GwMessage> = vec![];
        assert!(build_messages(&msgs).unwrap().is_empty());
    }

    // -----------------------------------------------------------------
    // Tool calling
    // -----------------------------------------------------------------

    #[test]
    fn json_to_document_round_trips_through_document_to_json() {
        let original = serde_json::json!({
            "city": "Berlin",
            "limit": 5,
            "negative": -3,
            "ratio": 0.75,
            "flag": true,
            "tags": ["a", "b"],
            "nested": {"deep": null},
        });
        let doc = json_to_document(original.clone());
        let back = document_to_json(&doc);
        assert_eq!(back, original);
    }

    #[test]
    fn parse_tool_input_handles_empty_and_malformed_inputs() {
        assert_eq!(parse_tool_input(""), serde_json::json!({}));
        assert_eq!(
            parse_tool_input("{\"city\":\"Berlin\"}"),
            serde_json::json!({"city": "Berlin"})
        );
        // Malformed JSON degrades to an empty object — Bedrock would
        // reject a string here, so empty-object is the safer fallback.
        assert_eq!(parse_tool_input("not json"), serde_json::json!({}));
    }

    #[test]
    fn build_tool_config_returns_none_for_empty_definition_list() {
        assert!(build_tool_config(&[]).is_none());
    }

    #[test]
    fn build_tool_config_wraps_each_definition_in_tool_spec() {
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
        let cfg = build_tool_config(&defs).expect("non-empty config");
        let tools = cfg.tools();
        assert_eq!(tools.len(), 2);
        let Tool::ToolSpec(spec0) = &tools[0] else {
            panic!("expected ToolSpec variant");
        };
        assert_eq!(spec0.name(), "get_weather");
        assert_eq!(spec0.description(), Some("Look up the weather for a city."));
        // input_schema is the JSON-Schema document we passed through.
        let ToolInputSchema::Json(doc) = spec0.input_schema().unwrap() else {
            panic!("expected Json schema variant");
        };
        let schema = document_to_json(doc);
        assert_eq!(schema["type"], "object");
        // Second tool: description should be absent.
        let Tool::ToolSpec(spec1) = &tools[1] else {
            panic!("expected ToolSpec variant");
        };
        assert_eq!(spec1.name(), "ping");
        assert!(spec1.description().is_none());
    }

    #[test]
    fn build_content_blocks_for_tool_result_emits_tool_result_with_tool_use_id() {
        let m = GwMessage::tool_result("tu_01", "{\"temp\":72}");
        let blocks = build_content_blocks(&m).unwrap();
        assert_eq!(blocks.len(), 1);
        let ContentBlock::ToolResult(block) = &blocks[0] else {
            panic!("expected ToolResult content block");
        };
        assert_eq!(block.tool_use_id(), "tu_01");
        // JSON tool result surfaces as a Json content block (object).
        let inner = &block.content[0];
        let ToolResultContentBlock::Json(doc) = inner else {
            panic!("expected Json inner block for JSON tool result");
        };
        assert_eq!(document_to_json(doc), serde_json::json!({"temp": 72}));
    }

    #[test]
    fn build_content_blocks_wraps_non_json_tool_result_in_text_inner_block() {
        let m = GwMessage::tool_result("tu_01", "all good");
        let blocks = build_content_blocks(&m).unwrap();
        let ContentBlock::ToolResult(block) = &blocks[0] else {
            panic!("expected ToolResult content block");
        };
        let ToolResultContentBlock::Text(t) = &block.content[0] else {
            panic!("expected Text inner block for plain-string tool result");
        };
        assert_eq!(t, "all good");
    }

    #[test]
    fn image_format_from_mime_table() {
        assert_eq!(
            image_format_from_mime(Some("image/jpeg")),
            Some(ImageFormat::Jpeg)
        );
        assert_eq!(
            image_format_from_mime(Some("image/jpg")),
            Some(ImageFormat::Jpeg)
        );
        assert_eq!(
            image_format_from_mime(Some("image/png")),
            Some(ImageFormat::Png)
        );
        assert_eq!(
            image_format_from_mime(Some("image/gif")),
            Some(ImageFormat::Gif)
        );
        assert_eq!(
            image_format_from_mime(Some("image/webp")),
            Some(ImageFormat::Webp)
        );
        // Case-insensitive matches.
        assert_eq!(
            image_format_from_mime(Some("IMAGE/PNG")),
            Some(ImageFormat::Png)
        );
        // Unsupported / missing → None so caller falls back.
        assert!(image_format_from_mime(Some("image/bmp")).is_none());
        assert!(image_format_from_mime(None).is_none());
    }

    #[test]
    fn build_content_blocks_decodes_base64_image_to_inline_bytes() {
        // base64("foo") = "Zm9v" → bytes [0x66, 0x6f, 0x6f]
        let msg = GwMessage::text(MessageRole::User, "what's in this?")
            .with_attachment(MediaAttachment::image_base64("Zm9v", "image/png"));
        let blocks = build_content_blocks(&msg).unwrap();
        assert_eq!(blocks.len(), 2);
        let ContentBlock::Image(img) = &blocks[1] else {
            panic!("expected Image block, got {:?}", blocks[1]);
        };
        assert!(matches!(img.format(), ImageFormat::Png));
        let ImageSource::Bytes(blob) = img.source().expect("source set") else {
            panic!("expected Bytes source for base64 attachment");
        };
        assert_eq!(blob.as_ref(), b"foo");
    }

    #[test]
    fn build_content_blocks_emits_s3_location_for_s3_url_attachment() {
        let msg =
            GwMessage::text(MessageRole::User, "look").with_attachment(MediaAttachment::Image {
                source: MediaSource::Url {
                    url: "s3://my-bucket/path/img.jpg".into(),
                },
                mime_type: Some("image/jpeg".into()),
            });
        let blocks = build_content_blocks(&msg).unwrap();
        let ContentBlock::Image(img) = &blocks[1] else {
            panic!("expected Image block");
        };
        let ImageSource::S3Location(loc) = img.source().unwrap() else {
            panic!("expected S3Location source for s3:// URL");
        };
        assert_eq!(loc.uri(), "s3://my-bucket/path/img.jpg");
    }

    #[test]
    fn build_content_blocks_drops_https_url_image_with_warning() {
        // Bedrock Converse can't fetch HTTPS URLs — they get dropped
        // rather than silently shipping an attachment Bedrock won't
        // understand. The text part still goes through.
        let msg = GwMessage::text(MessageRole::User, "see this")
            .with_attachment(MediaAttachment::image_url("https://ex.com/cat.jpg"));
        let blocks = build_content_blocks(&msg).unwrap();
        assert_eq!(blocks.len(), 1);
        match &blocks[0] {
            ContentBlock::Text(t) => assert_eq!(t, "see this"),
            other => panic!("expected lone Text block, got {other:?}"),
        }
    }

    #[test]
    fn build_content_blocks_defaults_image_format_to_jpeg_when_mime_missing() {
        let msg = GwMessage::text(MessageRole::User, "x").with_attachment(MediaAttachment::Image {
            source: MediaSource::Base64 {
                data: "AAAA".into(),
            },
            mime_type: None,
        });
        let blocks = build_content_blocks(&msg).unwrap();
        let ContentBlock::Image(img) = &blocks[1] else {
            panic!("expected Image block");
        };
        assert!(matches!(img.format(), ImageFormat::Jpeg));
    }

    #[test]
    fn build_content_blocks_errors_on_invalid_base64_image() {
        // A base64 payload the decoder can't parse must surface an error
        // rather than being silently dropped — otherwise the prompt would
        // reference an image Bedrock never received.
        let msg =
            GwMessage::text(MessageRole::User, "look").with_attachment(MediaAttachment::Image {
                source: MediaSource::Base64 {
                    data: "!!!! not base64 !!!!".into(),
                },
                mime_type: Some("image/png".into()),
            });
        let err = build_content_blocks(&msg).unwrap_err();
        assert!(
            matches!(err, GatewayError::ProviderError { .. }),
            "expected ProviderError, got: {err:?}",
        );
    }

    #[test]
    fn build_content_blocks_emits_tool_use_block_for_assistant_tool_calls() {
        let msg = GwMessage {
            role: MessageRole::Assistant,
            content: MessageContent::Text {
                text: "Looking…".into(),
            },
            tool_calls: vec![ToolCall {
                id: "tu_01".into(),
                name: "get_weather".into(),
                arguments: "{\"city\":\"Berlin\"}".into(),
            }],
            attachments: vec![],
        };
        let blocks = build_content_blocks(&msg).unwrap();
        // Text first, tool_use last.
        assert_eq!(blocks.len(), 2);
        let ContentBlock::Text(t) = &blocks[0] else {
            panic!("expected leading Text block");
        };
        assert_eq!(t, "Looking…");
        let ContentBlock::ToolUse(tu) = &blocks[1] else {
            panic!("expected trailing ToolUse block");
        };
        assert_eq!(tu.tool_use_id(), "tu_01");
        assert_eq!(tu.name(), "get_weather");
        assert_eq!(
            document_to_json(tu.input()),
            serde_json::json!({"city": "Berlin"})
        );
    }

    #[test]
    fn build_content_blocks_elides_empty_text_when_only_tool_calls_present() {
        let msg = GwMessage {
            role: MessageRole::Assistant,
            content: MessageContent::Text {
                text: String::new(),
            },
            tool_calls: vec![ToolCall {
                id: "tu_01".into(),
                name: "ping".into(),
                arguments: "{}".into(),
            }],
            attachments: vec![],
        };
        let blocks = build_content_blocks(&msg).unwrap();
        assert_eq!(blocks.len(), 1);
        assert!(matches!(blocks[0], ContentBlock::ToolUse(_)));
    }

    #[test]
    fn extract_tool_calls_returns_empty_when_response_is_text_only() {
        let response = ConverseResponse::builder()
            .output(ConverseOutputUnion::Message(
                Message::builder()
                    .role(ConversationRole::Assistant)
                    .content(ContentBlock::Text("just text".into()))
                    .build()
                    .unwrap(),
            ))
            .stop_reason(aws_sdk_bedrockruntime::types::StopReason::EndTurn)
            .usage(
                aws_sdk_bedrockruntime::types::TokenUsage::builder()
                    .input_tokens(1)
                    .output_tokens(1)
                    .total_tokens(2)
                    .build()
                    .unwrap(),
            )
            .metrics(
                aws_sdk_bedrockruntime::types::ConverseMetrics::builder()
                    .latency_ms(0)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        assert!(extract_tool_calls(&response).is_empty());
    }

    // -----------------------------------------------------------------
    // Streaming (chunk_from_event mapping)
    // -----------------------------------------------------------------

    #[test]
    fn chunk_from_text_delta_event_emits_content_only_chunk() {
        let ev = ConverseStreamOutput::ContentBlockDelta(
            aws_sdk_bedrockruntime::types::ContentBlockDeltaEvent::builder()
                .delta(ContentBlockDelta::Text("Hello".into()))
                .content_block_index(0)
                .build()
                .unwrap(),
        );
        let chunk =
            chunk_from_event(&ev, &mut BTreeMap::new()).expect("text delta should produce a chunk");
        assert_eq!(chunk.content, "Hello");
        assert!(chunk.finish_reason.is_none());
        assert!(chunk.usage.is_none());
    }

    #[test]
    fn chunk_from_message_stop_event_carries_finish_reason() {
        let ev = ConverseStreamOutput::MessageStop(
            aws_sdk_bedrockruntime::types::MessageStopEvent::builder()
                .stop_reason(aws_sdk_bedrockruntime::types::StopReason::EndTurn)
                .build()
                .unwrap(),
        );
        let chunk = chunk_from_event(&ev, &mut BTreeMap::new())
            .expect("MessageStop should produce a chunk");
        assert_eq!(chunk.content, "");
        // StopReason::EndTurn renders as "end_turn" via the SDK's as_str().
        assert_eq!(chunk.finish_reason.as_deref(), Some("end_turn"));
        assert!(chunk.usage.is_none());
    }

    #[test]
    fn chunk_from_metadata_event_carries_token_usage() {
        let ev = ConverseStreamOutput::Metadata(
            aws_sdk_bedrockruntime::types::ConverseStreamMetadataEvent::builder()
                .usage(
                    aws_sdk_bedrockruntime::types::TokenUsage::builder()
                        .input_tokens(7)
                        .output_tokens(3)
                        .total_tokens(10)
                        .build()
                        .unwrap(),
                )
                .metrics(
                    aws_sdk_bedrockruntime::types::ConverseStreamMetrics::builder()
                        .latency_ms(0)
                        .build()
                        .unwrap(),
                )
                .build(),
        );
        let chunk =
            chunk_from_event(&ev, &mut BTreeMap::new()).expect("Metadata should produce a chunk");
        assert_eq!(chunk.content, "");
        assert!(chunk.finish_reason.is_none());
        let usage = chunk.usage.expect("usage present on metadata chunk");
        assert_eq!(usage.input_tokens, 7);
        assert_eq!(usage.output_tokens, 3);
        assert_eq!(usage.total_tokens, 10);
    }

    #[test]
    fn chunk_from_silent_events_returns_none() {
        // MessageStart / ContentBlockStart / ContentBlockStop carry no
        // user-visible content — they're framing only.
        let start = ConverseStreamOutput::MessageStart(
            aws_sdk_bedrockruntime::types::MessageStartEvent::builder()
                .role(ConversationRole::Assistant)
                .build()
                .unwrap(),
        );
        assert!(chunk_from_event(&start, &mut BTreeMap::new()).is_none());

        let block_start = ConverseStreamOutput::ContentBlockStart(
            aws_sdk_bedrockruntime::types::ContentBlockStartEvent::builder()
                .content_block_index(0)
                .build()
                .unwrap(),
        );
        assert!(chunk_from_event(&block_start, &mut BTreeMap::new()).is_none());

        let block_stop = ConverseStreamOutput::ContentBlockStop(
            aws_sdk_bedrockruntime::types::ContentBlockStopEvent::builder()
                .content_block_index(0)
                .build()
                .unwrap(),
        );
        assert!(chunk_from_event(&block_stop, &mut BTreeMap::new()).is_none());
    }

    #[test]
    fn chunk_from_non_text_delta_returns_none_in_v1() {
        // Tool-use argument deltas don't surface yet — they'll need
        // accumulation in the stream layer when tool-streaming lands.
        let ev = ConverseStreamOutput::ContentBlockDelta(
            aws_sdk_bedrockruntime::types::ContentBlockDeltaEvent::builder()
                .delta(ContentBlockDelta::ToolUse(
                    aws_sdk_bedrockruntime::types::ToolUseBlockDelta::builder()
                        .input("{\"city\":\"Be")
                        .build()
                        .unwrap(),
                ))
                .content_block_index(0)
                .build()
                .unwrap(),
        );
        assert!(chunk_from_event(&ev, &mut BTreeMap::new()).is_none());
    }

    #[test]
    fn chunk_from_event_seeds_accumulator_on_tool_use_block_start() {
        let mut accs: BTreeMap<u32, StreamingToolCall> = BTreeMap::new();
        let start = ConverseStreamOutput::ContentBlockStart(
            aws_sdk_bedrockruntime::types::ContentBlockStartEvent::builder()
                .content_block_index(1)
                .start(aws_sdk_bedrockruntime::types::ContentBlockStart::ToolUse(
                    aws_sdk_bedrockruntime::types::ToolUseBlockStart::builder()
                        .tool_use_id("tu_01")
                        .name("get_weather")
                        .build()
                        .unwrap(),
                ))
                .build()
                .unwrap(),
        );
        assert!(chunk_from_event(&start, &mut accs).is_none());
        // The accumulator now exists at the start event's index.
        let acc = accs.get(&1).expect("accumulator seeded");
        assert_eq!(acc.id.as_deref(), Some("tu_01"));
        assert_eq!(acc.name.as_deref(), Some("get_weather"));
        assert!(acc.arguments_buffer.is_empty());
    }

    #[test]
    fn chunk_from_event_appends_tool_use_input_fragments() {
        let mut accs: BTreeMap<u32, StreamingToolCall> = BTreeMap::new();
        accs.insert(0, StreamingToolCall::new("tu_01", "get_weather"));
        let delta = ConverseStreamOutput::ContentBlockDelta(
            aws_sdk_bedrockruntime::types::ContentBlockDeltaEvent::builder()
                .content_block_index(0)
                .delta(ContentBlockDelta::ToolUse(
                    aws_sdk_bedrockruntime::types::ToolUseBlockDelta::builder()
                        .input("{\"ci")
                        .build()
                        .unwrap(),
                ))
                .build()
                .unwrap(),
        );
        assert!(chunk_from_event(&delta, &mut accs).is_none());
        assert_eq!(accs.get(&0).unwrap().arguments_buffer, "{\"ci");
    }

    #[test]
    fn chunk_from_event_drains_accumulators_into_message_stop_terminal_chunk() {
        let mut accs: BTreeMap<u32, StreamingToolCall> = BTreeMap::new();
        let mut acc = StreamingToolCall::new("tu_01", "get_weather");
        acc.push_arguments(r#"{"city":"Berlin"}"#);
        accs.insert(0, acc);
        let ev = ConverseStreamOutput::MessageStop(
            aws_sdk_bedrockruntime::types::MessageStopEvent::builder()
                .stop_reason(aws_sdk_bedrockruntime::types::StopReason::ToolUse)
                .build()
                .unwrap(),
        );
        let chunk = chunk_from_event(&ev, &mut accs).expect("MessageStop emits chunk");
        assert_eq!(chunk.finish_reason.as_deref(), Some("tool_use"));
        assert_eq!(chunk.tool_calls.len(), 1);
        assert_eq!(chunk.tool_calls[0].id, "tu_01");
        assert_eq!(chunk.tool_calls[0].arguments, r#"{"city":"Berlin"}"#);
        // Accumulators drained.
        assert!(accs.is_empty());
    }

    #[test]
    fn extract_tool_calls_pulls_tool_use_blocks_and_serialises_arguments() {
        let tu = ToolUseBlock::builder()
            .tool_use_id("tu_42")
            .name("get_weather")
            .input(json_to_document(serde_json::json!({"city": "Berlin"})))
            .build()
            .unwrap();
        let response = ConverseResponse::builder()
            .output(ConverseOutputUnion::Message(
                Message::builder()
                    .role(ConversationRole::Assistant)
                    .content(ContentBlock::Text("Looking up…".into()))
                    .content(ContentBlock::ToolUse(tu))
                    .build()
                    .unwrap(),
            ))
            .stop_reason(aws_sdk_bedrockruntime::types::StopReason::ToolUse)
            .usage(
                aws_sdk_bedrockruntime::types::TokenUsage::builder()
                    .input_tokens(10)
                    .output_tokens(5)
                    .total_tokens(15)
                    .build()
                    .unwrap(),
            )
            .metrics(
                aws_sdk_bedrockruntime::types::ConverseMetrics::builder()
                    .latency_ms(0)
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();
        let calls = extract_tool_calls(&response);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "tu_42");
        assert_eq!(calls[0].name, "get_weather");
        let parsed: serde_json::Value = serde_json::from_str(&calls[0].arguments).unwrap();
        assert_eq!(parsed, serde_json::json!({"city": "Berlin"}));
        // extract_text on the same response still returns just the text.
        assert_eq!(extract_text(&response), "Looking up…");
    }
}
