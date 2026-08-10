//! Response direction: Bedrock `ConverseResponse` / Converse stream
//! events / SDK errors → gateway types.

use std::collections::BTreeMap;

use aws_sdk_bedrockruntime::{
    operation::converse::ConverseOutput as ConverseResponse,
    types::{
        ContentBlock, ContentBlockDelta, ConverseOutput as ConverseOutputUnion,
        ConverseStreamOutput,
    },
};

use kernel::types::cost::TokenUsage;
use kernel::types::error::GatewayError;
use kernel::types::request::{StreamChunk, StreamingToolCall, ToolCall};

use super::super::ADAPTER_ID;
use super::document_to_json;

// ---------------------------------------------------------------------------
// Streaming: SDK event → StreamChunk
// ---------------------------------------------------------------------------

/// Map a single Converse stream event to a [`StreamChunk`] (when
/// there's something to surface) and update the per-index tool-call
/// accumulator map.
pub(crate) fn chunk_from_event(
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
// Response: Bedrock ConverseResponse → gateway content / ToolCall
// ---------------------------------------------------------------------------

/// Pull `tool_use` blocks out of the Converse response and convert
/// them into gateway [`ToolCall`]s. Bedrock natively carries an id
/// per call, so we surface it directly; arguments are re-serialised
/// into the gateway's JSON-string form for parity with OpenAI.
pub(crate) fn extract_tool_calls(response: &ConverseResponse) -> Vec<ToolCall> {
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

/// Pull all text out of the Converse response, concatenating across
/// content blocks. Bedrock can return a mix of `Text`, `ToolUse`,
/// `Image`, etc.; we surface only the text in this commit.
pub(crate) fn extract_text(response: &ConverseResponse) -> String {
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
pub(crate) fn map_sdk_error<E: std::error::Error + Send + Sync + 'static>(
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

#[cfg(test)]
mod tests {
    use super::super::json_to_document;
    use super::*;
    use aws_sdk_bedrockruntime::types::{ConversationRole, Message, ToolUseBlock};

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
