use chrono::{DateTime, Utc};

use crate::store::UsageTotals;
use crate::types::config::{MeterUnit, Window};
use crate::types::error::GatewayError;
use crate::types::request::Payload;

/// Map a [`GatewayError`] to a short, stable `code` string for a
/// [`StreamEvent::Error`]. `ProviderError` reports its HTTP status when
/// present (most useful to a consumer), otherwise a variant discriminant.
pub(super) fn stream_error_code(err: &GatewayError) -> String {
    match err {
        GatewayError::Authentication { .. } => "authentication".to_string(),
        GatewayError::RateLimit { .. } => "rate_limit".to_string(),
        GatewayError::BudgetExceeded { .. } => "budget_exceeded".to_string(),
        GatewayError::QuotaExceeded { .. } => "quota_exceeded".to_string(),
        GatewayError::Timeout { .. } => "timeout".to_string(),
        GatewayError::ProviderError { status, .. } => status
            .map(|s| s.to_string())
            .unwrap_or_else(|| "provider_error".to_string()),
        GatewayError::ModelUnavailable { .. } => "model_unavailable".to_string(),
        GatewayError::Unsupported { .. } => "unsupported".to_string(),
        GatewayError::NoCandidates { .. } => "no_candidates".to_string(),
        GatewayError::NotConfigured => "not_configured".to_string(),
        GatewayError::AllAttemptsFailed { .. } => "all_attempts_failed".to_string(),
        GatewayError::AllGated { .. } => "all_gated".to_string(),
        GatewayError::ModelNotReady { .. } => "model_not_ready".to_string(),
        GatewayError::InvalidConfig(_) => "invalid_config".to_string(),
        GatewayError::Network(_) => "network".to_string(),
        GatewayError::Serialization(_) => "serialization".to_string(),
    }
}

/// Rolling-window start for a [`Window`]: `now − period`. Week = 7 days,
/// Month ≈ 30 days (rolling, not calendar-aligned — see the AUTH design D2).
pub(super) fn window_start(now: DateTime<Utc>, w: Window) -> DateTime<Utc> {
    let period = match w {
        Window::Day => chrono::Duration::days(1),
        Window::Week => chrono::Duration::days(7),
        Window::Month => chrono::Duration::days(30),
    };
    now - period
}

/// Read a subject's aggregated usage for a given meter unit.
pub(super) fn usage_value(u: &UsageTotals, unit: MeterUnit) -> u64 {
    match unit {
        MeterUnit::Requests => u.requests,
        MeterUnit::InputTokens => u.input_tokens,
        MeterUnit::OutputTokens => u.output_tokens,
        MeterUnit::TotalTokens => u.total_tokens,
        MeterUnit::CostUsdMillis => u.cost_usd_millis,
    }
}

/// This call's contribution to a meter unit, known pre-flight. Requests count 1;
/// input/total tokens use the request estimate; output tokens and dollars are
/// unknown before the call (0), so those caps are enforced against usage already
/// on record (a deliberate soft guard — see the AUTH design D3).
pub(super) fn call_estimate(unit: MeterUnit, input_tokens: u32) -> u64 {
    match unit {
        MeterUnit::Requests => 1,
        MeterUnit::InputTokens | MeterUnit::TotalTokens => input_tokens as u64,
        MeterUnit::OutputTokens | MeterUnit::CostUsdMillis => 0,
    }
}

/// Estimate input token count from the request payload.
///
/// Rough heuristic: 1 token ~ 4 characters.
pub(super) fn estimate_input_tokens(payload: &Payload) -> u32 {
    match payload {
        Payload::Chat {
            messages, system, ..
        } => {
            let msg_chars: usize = messages.iter().map(|m| m.as_text().len()).sum();
            let sys_chars: usize = system.as_ref().map(|s| s.len()).unwrap_or(0);
            ((msg_chars + sys_chars) / 4) as u32
        }
        Payload::Embed { texts } => {
            let total_chars: usize = texts.iter().map(|t| t.len()).sum();
            (total_chars / 4) as u32
        }
        // STT has no meaningful text input to estimate.
        Payload::Stt { .. } => 0,
        // For TTS, rough heuristic on text length.
        Payload::Tts { text, .. } => (text.len() / 4) as u32,
        // Image generation: estimate based on prompt length.
        Payload::ImageGenerate { prompt, .. } => (prompt.len() / 4) as u32,
        // Video generation: estimate based on prompt length.
        Payload::VideoGenerate { prompt, .. } => (prompt.len() / 4) as u32,
    }
}

/// A deliberately pessimistic input estimate, for the CONTEXT-WINDOW gate only.
///
/// Two differences from [`estimate_input_tokens`], and both push in the same direction:
///
/// 1. **It counts tool schemas.** The cost estimator sums messages + system and stops. An
///    agent's activated schemas routinely outweigh its prompt, so omitting them is not a
///    rounding error — it is most of the payload on exactly the requests this gate exists
///    to catch.
/// 2. **`chars / 3`, not `chars / 4`.** The `/4` figure is the rough one for English
///    prose; JSON tokenizes nearer 3 chars/token, and schemas are pure JSON. Rounding is
///    up (`div_ceil`) rather than truncating, so a payload with any content at all
///    estimates at least one token.
///
/// Kept SEPARATE rather than widening the shared estimator, because the two gates want
/// opposite biases over the same payload: an under-count is optimistic pricing for the
/// cost gate and an admitted-but-doesn't-fit candidate for this one. Changing the shared
/// figure would silently make every `BudgetGate` decision more conservative — a real
/// improvement, and a different slice's call.
///
/// Every non-chat arm mirrors [`estimate_input_tokens`]'s handling at `/3` rather than
/// collapsing to `0`. A `_ => 0` catch-all would make this function *under*-count a Tts
/// or ImageGenerate payload relative to the cost estimate, inverting the one ordering the
/// gate's safety rests on. `Stt` is 0 in both: audio bytes are not characters, and
/// counting them would skip every candidate for a 30-second clip.
///
/// The accepted cost, stated plainly: a pessimistic figure can skip a model the prompt
/// would actually have fitted, sending the request to a larger, likely costlier
/// candidate. That is the cheaper error — the alternative is a provider 400 — and it is
/// visible, because the skip records both numbers.
//
// TEMPORARY, and it must not survive the slice: nothing in a non-test build calls this
// until SP-7a Task 5 computes it in `engine::execute` and puts it on `SelectionCriteria`.
// `clippy -D warnings` (the pre-commit gate) rejects the dead function in between, and the
// alternative — dragging Task 5's plumbing into Task 3 so the estimator lands already
// wired — would fuse two commits whose reviews are about different things. Task 5 DELETES
// this attribute; if you are reading it after Task 5 landed, the plumbing is missing.
#[allow(dead_code)]
pub(super) fn estimate_input_tokens_pessimistic(payload: &Payload) -> u32 {
    let chars: usize = match payload {
        Payload::Chat {
            messages,
            system,
            tools,
            ..
        } => {
            let msg_chars: usize = messages.iter().map(|m| m.as_text().len()).sum();
            let sys_chars: usize = system.as_ref().map(|s| s.len()).unwrap_or(0);
            // `to_string()` on the schema is the closest cheap stand-in for what the
            // adapter actually puts on the wire: providers all receive the JSON Schema
            // document verbatim (see `ToolDefinition`), so its serialized length is the
            // thing occupying the window, not the struct's in-memory size.
            let tool_chars: usize = tools
                .iter()
                .map(|t| {
                    t.name.len()
                        + t.description.as_ref().map(|d| d.len()).unwrap_or(0)
                        + t.input_schema.to_string().len()
                })
                .sum();
            msg_chars + sys_chars + tool_chars
        }
        Payload::Embed { texts } => texts.iter().map(|t| t.len()).sum(),
        // STT has no meaningful text input to estimate — mirrors `estimate_input_tokens`.
        Payload::Stt { .. } => 0,
        Payload::Tts { text, .. } => text.len(),
        Payload::ImageGenerate { prompt, .. } | Payload::VideoGenerate { prompt, .. } => {
            prompt.len()
        }
    };
    // Saturate rather than wrap. `estimate_input_tokens` uses `as u32`, which is harmless
    // there because an overflowed cost estimate only mis-prices; here a wrap would turn a
    // 4-GiB payload into a tiny number and ADMIT it, which is precisely the failure this
    // estimate exists to prevent. `u32::MAX` skips every candidate instead, loudly.
    u32::try_from(chars.div_ceil(3)).unwrap_or(u32::MAX)
}

/// Extract the user-facing prompt text from a request payload, for addressing a
/// consensus workflow (which takes a plain prompt). Chat → its messages' text
/// joined by newlines; text-bearing media payloads → their prompt/text. Returns
/// `None` when there is no text to run a consensus over (embed / stt).
pub(super) fn request_input_text(payload: &Payload) -> Option<String> {
    let text = match payload {
        Payload::Chat { messages, .. } => messages
            .iter()
            .map(|m| m.as_text())
            .collect::<Vec<_>>()
            .join("\n"),
        Payload::Tts { text, .. } => text.clone(),
        Payload::ImageGenerate { prompt, .. } | Payload::VideoGenerate { prompt, .. } => {
            prompt.clone()
        }
        Payload::Embed { .. } | Payload::Stt { .. } => return None,
    };
    (!text.trim().is_empty()).then_some(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::request::{Message, MessageRole, ToolDefinition};

    fn chat(tools: Vec<ToolDefinition>) -> Payload {
        Payload::Chat {
            messages: vec![Message::text(
                MessageRole::User,
                "hello there, this is a prompt",
            )],
            system: Some("you are a helpful assistant".into()),
            max_tokens: None,
            temperature: None,
            tools,
        }
    }

    fn fs_write_tool() -> ToolDefinition {
        ToolDefinition {
            name: "fs_write".into(),
            description: Some("Write a file to the workspace".into()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "contents": { "type": "string" }
                },
                "required": ["path", "contents"]
            }),
        }
    }

    /// AC6 + AC7 — the window estimate is >= the cost estimate for the same payload, and
    /// strictly greater once tool schemas are present.
    ///
    /// `estimate_input_tokens` counts messages + system at `chars/4`. It omits tool
    /// schemas entirely — and an agent's activated schemas routinely outweigh its prompt.
    /// For COST that is optimistic pricing; for a WINDOW it admits a candidate the
    /// request does not fit, which is the failure the gate exists to prevent. So this one
    /// counts the schemas and uses the JSON-ish `chars/3` rather than the prose `chars/4`.
    #[test]
    fn the_pessimistic_estimate_counts_tools_and_never_undercuts_the_cost_estimate() {
        let no_tools = chat(Vec::new());
        assert!(
            estimate_input_tokens_pessimistic(&no_tools) >= estimate_input_tokens(&no_tools),
            "must never undercut the cost estimate even with no tools: {} < {}",
            estimate_input_tokens_pessimistic(&no_tools),
            estimate_input_tokens(&no_tools)
        );

        let with_tools = chat(vec![fs_write_tool()]);
        assert!(
            estimate_input_tokens_pessimistic(&with_tools)
                > estimate_input_tokens_pessimistic(&no_tools),
            "adding a tool schema must raise the estimate — the schemas are exactly what \
             the cost estimator omits: {} !> {}",
            estimate_input_tokens_pessimistic(&with_tools),
            estimate_input_tokens_pessimistic(&no_tools)
        );

        // And the cost estimator is genuinely blind to them: this is the asymmetry that
        // makes a second function necessary rather than a nicety. If this ever stops
        // holding, `estimate_input_tokens` grew a tools term and §4's argument for two
        // estimates needs re-reading, not this assertion deleting.
        assert_eq!(
            estimate_input_tokens(&with_tools),
            estimate_input_tokens(&no_tools),
            "the COST estimate must still ignore tool schemas — widening it silently \
             makes every BudgetGate decision more conservative, which is a different \
             slice's call"
        );
    }

    /// AC7 + AC10 — the "never undercuts" guarantee holds for EVERY payload kind, not
    /// just chat.
    ///
    /// The obvious way to write the pessimistic estimator is `Chat`/`Embed` arms and
    /// `_ => 0`, and it is wrong: a Tts or ImageGenerate payload would then estimate 0
    /// against a cost estimate of `chars/4`, breaking the ordering the gate relies on to
    /// be safe. Non-chat payloads mirror `estimate_input_tokens` arm for arm, at `/3`.
    /// Stt stays 0 in both because there is no text to measure — audio bytes are not
    /// characters, and counting them would skip every candidate for a 30-second clip.
    #[test]
    fn the_pessimistic_estimate_never_undercuts_the_cost_estimate_for_any_payload_kind() {
        let payloads = vec![
            ("chat", chat(vec![fs_write_tool()])),
            (
                "embed",
                Payload::Embed {
                    texts: vec!["some text to embed".into(), "and another".into()],
                },
            ),
            (
                "stt",
                Payload::Stt {
                    audio: vec![0u8; 1000],
                    language: None,
                    format: "wav".to_string(),
                },
            ),
            (
                "tts",
                Payload::Tts {
                    text: "Hello world, this is a test!".to_string(),
                    voice: None,
                    speed: None,
                    output_format: crate::types::request::AudioFormat::Mp3,
                },
            ),
            (
                "image",
                Payload::ImageGenerate {
                    prompt: "A beautiful sunset over mountains".to_string(),
                    size: None,
                    quality: None,
                    style: None,
                    n: 1,
                },
            ),
            (
                "video",
                Payload::VideoGenerate {
                    prompt: "A timelapse of a blooming flower".to_string(),
                    duration_secs: Some(10),
                    resolution: Some("1080p".to_string()),
                },
            ),
        ];
        for (name, p) in &payloads {
            let pess = estimate_input_tokens_pessimistic(p);
            let cost = estimate_input_tokens(p);
            assert!(
                pess >= cost,
                "{name}: the window estimate undercuts the cost estimate ({pess} < \
                 {cost}), which would admit a candidate the request does not fit"
            );
        }
    }

    #[test]
    fn estimate_input_tokens_stt() {
        let payload = Payload::Stt {
            audio: vec![0u8; 1000],
            language: None,
            format: "wav".to_string(),
        };
        assert_eq!(estimate_input_tokens(&payload), 0);
    }

    #[test]
    fn estimate_input_tokens_tts() {
        let payload = Payload::Tts {
            text: "Hello world, this is a test!".to_string(),
            voice: None,
            speed: None,
            output_format: crate::types::request::AudioFormat::Mp3,
        };
        let expected = ("Hello world, this is a test!".len() / 4) as u32;
        assert_eq!(estimate_input_tokens(&payload), expected);
    }

    #[test]
    fn estimate_input_tokens_image_generate() {
        let payload = Payload::ImageGenerate {
            prompt: "A beautiful sunset over mountains".to_string(),
            size: None,
            quality: None,
            style: None,
            n: 1,
        };
        let expected = ("A beautiful sunset over mountains".len() / 4) as u32;
        assert_eq!(estimate_input_tokens(&payload), expected);
    }

    #[test]
    fn estimate_input_tokens_video_generate() {
        let payload = Payload::VideoGenerate {
            prompt: "A timelapse of a blooming flower".to_string(),
            duration_secs: Some(10),
            resolution: Some("1080p".to_string()),
        };
        let expected = ("A timelapse of a blooming flower".len() / 4) as u32;
        assert_eq!(estimate_input_tokens(&payload), expected);
    }
}
