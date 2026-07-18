//! Capability-typed request/response structs used by the segregated
//! adapter traits. Internal to the gateway crate — the public API still
//! speaks `InferenceRequest`/`InferenceResponse`; the engine translates
//! at the boundary (see `crate::dispatch`).

use super::cost::TokenUsage;
use super::request::{AudioFormat, ImageResult, Message, ToolCall, ToolDefinition, VideoResult};

#[derive(Debug, Clone)]
pub struct ChatRequest {
    pub model: Option<String>,
    pub messages: Vec<Message>,
    pub system: Option<String>,
    pub max_tokens: Option<u32>,
    pub temperature: Option<f32>,
    pub tools: Vec<ToolDefinition>,
}

#[derive(Debug, Clone, Default)]
pub struct ChatResponse {
    pub content: Option<String>,
    pub tool_calls: Vec<ToolCall>,
    pub usage: Option<TokenUsage>,
    pub model: Option<String>,
    /// `true` = a placeholder/degraded reply (e.g. the no-provider fallback), not a real provider result.
    pub degraded: bool,
}

#[derive(Debug, Clone)]
pub struct EmbedRequest {
    pub model: Option<String>,
    pub texts: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct EmbedResponse {
    pub embeddings: Vec<Vec<f32>>,
    pub usage: Option<TokenUsage>,
    /// `true` = a placeholder/degraded reply (e.g. the no-provider fallback), not a real provider result.
    pub degraded: bool,
}

#[derive(Debug, Clone)]
pub struct SttRequest {
    pub model: Option<String>,
    pub audio: Vec<u8>,
    pub language: Option<String>,
    pub format: String,
}

#[derive(Debug, Clone, Default)]
pub struct SttResponse {
    pub transcription: String,
    pub usage: Option<TokenUsage>,
    /// `true` = a placeholder/degraded reply (e.g. the no-provider fallback), not a real provider result.
    pub degraded: bool,
}

#[derive(Debug, Clone)]
pub struct TtsRequest {
    pub model: Option<String>,
    pub text: String,
    pub voice: Option<String>,
    pub speed: Option<f32>,
    pub output_format: AudioFormat,
}

#[derive(Debug, Clone, Default)]
pub struct TtsResponse {
    pub audio: Vec<u8>,
    /// `true` = a placeholder/degraded reply (e.g. the no-provider fallback), not a real provider result.
    pub degraded: bool,
}

#[derive(Debug, Clone)]
pub struct ImageRequest {
    pub model: Option<String>,
    pub prompt: String,
    pub size: Option<String>,
    pub quality: Option<String>,
    pub style: Option<String>,
    pub n: u8,
}

#[derive(Debug, Clone, Default)]
pub struct ImageResponse {
    pub images: Vec<ImageResult>,
    /// `true` = a placeholder/degraded reply (e.g. the no-provider fallback), not a real provider result.
    pub degraded: bool,
}

#[derive(Debug, Clone)]
pub struct VideoRequest {
    pub model: Option<String>,
    pub prompt: String,
    pub duration_secs: Option<u32>,
    pub resolution: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct VideoResponse {
    pub videos: Vec<VideoResult>,
    /// `true` = a placeholder/degraded reply (e.g. the no-provider fallback), not a real provider result.
    pub degraded: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chat_response_default_is_empty() {
        let r = ChatResponse::default();
        assert!(r.content.is_none());
        assert!(r.tool_calls.is_empty());
    }

    #[test]
    fn embed_request_holds_texts() {
        let r = EmbedRequest {
            model: None,
            texts: vec!["a".into(), "b".into()],
        };
        assert_eq!(r.texts.len(), 2);
    }
}
