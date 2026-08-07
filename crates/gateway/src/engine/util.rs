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
