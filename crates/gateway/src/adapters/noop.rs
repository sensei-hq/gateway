use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use futures::stream;

use super::InferenceAdapter;
use crate::types::capability::Capability;
use crate::types::config::RouterConfig;
use crate::types::error::GatewayError;
use crate::types::io::{
    ChatRequest, ChatResponse, EmbedRequest, EmbedResponse, ImageRequest, ImageResponse,
    SttRequest, SttResponse, TtsRequest, TtsResponse, VideoRequest, VideoResponse,
};
use crate::types::request::{InferenceRequest, InferenceResponse, StreamChunk};
use crate::types::trace::{Attempt, AttemptStatus};

/// Graceful-degradation adapter that never errors.
///
/// Returns `Ok` with `success: false` for every request, explaining that no
/// real inference provider is available. This is used as the last-resort
/// fallback so the gateway always produces a response rather than an error.
pub struct NoopAdapter;

impl NoopAdapter {
    fn unavailable_message(capability: &Capability) -> String {
        format!(
            "No inference provider available for capability {:?}. \
             Install Ollama or configure an API key.",
            capability,
        )
    }

    fn failed_attempt(capability: &Capability) -> Attempt {
        Attempt {
            sequence: 1,
            adapter: "noop".to_string(),
            model: "none".to_string(),
            api_model_id: "none".to_string(),
            status: AttemptStatus::Failed,
            duration_ms: 0,
            tokens: None,
            cost: None,
            error: Some(Self::unavailable_message(capability)),
            fallback_triggered: false,
        }
    }
}

#[async_trait]
impl InferenceAdapter for NoopAdapter {
    fn id(&self) -> &str {
        "noop"
    }

    fn supports(&self, _capability: &Capability) -> bool {
        true
    }

    async fn execute(
        &self,
        _config: &RouterConfig,
        request: &InferenceRequest,
    ) -> Result<InferenceResponse, GatewayError> {
        Ok(InferenceResponse {
            success: false,
            content: Some(Self::unavailable_message(&request.capability)),
            embeddings: None,
            transcription: None,
            audio: None,
            images: None,
            videos: None,
            model: None,
            tool_calls: Vec::new(),
            usage: None,
            estimated_cost: None,
            actual_cost: None,
            attempts: vec![Self::failed_attempt(&request.capability)],
        })
    }

    async fn stream(
        &self,
        _config: &RouterConfig,
        request: &InferenceRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        let chunk = StreamChunk {
            content: Self::unavailable_message(&request.capability),
            finish_reason: Some("no_provider".to_string()),
            usage: None,
            tool_calls: Vec::new(),
        };
        Ok(Box::pin(stream::once(async move { Ok(chunk) })))
    }
}

// ---------------------------------------------------------------------------
// Capability traits (target model). noop is the catch-all: implements every
// capability and returns the same "no provider" placeholder as the execute
// path. NOTE: the typed responses have no `success` field, so the old
// `success: false` degraded-signal cannot be carried here — the engine decides
// success at the Phase 4 dispatch boundary. Traits referenced by full path to
// avoid the id() clash with InferenceAdapter during the bridge.
// ---------------------------------------------------------------------------

impl crate::adapters::capability::Model for NoopAdapter {
    fn id(&self) -> &str {
        "noop"
    }
}

#[async_trait]
impl crate::adapters::capability::ChatModel for NoopAdapter {
    async fn chat(
        &self,
        _config: &RouterConfig,
        _req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        Ok(ChatResponse {
            content: Some(Self::unavailable_message(&Capability::TextChat)),
            tool_calls: Vec::new(),
            usage: None,
            model: None,
        })
    }

    async fn chat_stream(
        &self,
        _config: &RouterConfig,
        _req: &ChatRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        let chunk = StreamChunk {
            content: Self::unavailable_message(&Capability::TextChat),
            finish_reason: Some("no_provider".to_string()),
            usage: None,
            tool_calls: Vec::new(),
        };
        Ok(Box::pin(stream::once(async move { Ok(chunk) })))
    }
}

#[async_trait]
impl crate::adapters::capability::EmbedModel for NoopAdapter {
    async fn embed(
        &self,
        _config: &RouterConfig,
        _req: &EmbedRequest,
    ) -> Result<EmbedResponse, GatewayError> {
        Ok(EmbedResponse::default())
    }
}

#[async_trait]
impl crate::adapters::capability::SttModel for NoopAdapter {
    async fn transcribe(
        &self,
        _config: &RouterConfig,
        _req: &SttRequest,
    ) -> Result<SttResponse, GatewayError> {
        Ok(SttResponse {
            transcription: Self::unavailable_message(&Capability::AudioTranscribe),
            usage: None,
        })
    }
}

#[async_trait]
impl crate::adapters::capability::TtsModel for NoopAdapter {
    async fn speak(
        &self,
        _config: &RouterConfig,
        _req: &TtsRequest,
    ) -> Result<TtsResponse, GatewayError> {
        Ok(TtsResponse::default())
    }
}

#[async_trait]
impl crate::adapters::capability::ImageModel for NoopAdapter {
    async fn generate_image(
        &self,
        _config: &RouterConfig,
        _req: &ImageRequest,
    ) -> Result<ImageResponse, GatewayError> {
        Ok(ImageResponse::default())
    }
}

#[async_trait]
impl crate::adapters::capability::VideoModel for NoopAdapter {
    async fn generate_video(
        &self,
        _config: &RouterConfig,
        _req: &VideoRequest,
    ) -> Result<VideoResponse, GatewayError> {
        Ok(VideoResponse::default())
    }
}

#[async_trait]
impl crate::adapters::RegisterInto for NoopAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &crate::adapters::CapabilityRegistry) {
        reg.register_chat(self.clone()).await;
        reg.register_embed(self.clone()).await;
        reg.register_stt(self.clone()).await;
        reg.register_tts(self.clone()).await;
        reg.register_image(self.clone()).await;
        reg.register_video(self).await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::types::request::{Message, MessageRole, Payload};
    use futures::StreamExt;

    fn test_config() -> RouterConfig {
        RouterConfig {
            url: "http://localhost".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        }
    }

    fn test_request() -> InferenceRequest {
        InferenceRequest {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: None,
            payload: Payload::Chat {
                messages: vec![Message::text(MessageRole::User, "hello".to_string())],
                system: None,
                max_tokens: None,
                temperature: None,
                tools: Vec::new(),
            },
            budget: None,
        }
    }

    #[tokio::test]
    async fn noop_chatmodel_returns_canned_content() {
        use crate::adapters::capability::{ChatModel, Model};
        let adapter = NoopAdapter;
        assert_eq!(Model::id(&adapter), "noop");
        let req = crate::types::io::ChatRequest {
            model: None,
            messages: vec![Message::text(MessageRole::User, "hi".to_string())],
            system: None,
            max_tokens: None,
            temperature: None,
            tools: Vec::new(),
        };
        let resp = adapter.chat(&test_config(), &req).await.unwrap();
        assert!(resp.content.unwrap().contains("No inference provider"));
    }

    #[test]
    fn noop_supports_all_capabilities() {
        let adapter = NoopAdapter;
        assert!(adapter.supports(&Capability::TextChat));
        assert!(adapter.supports(&Capability::TextEmbed));
        assert!(adapter.supports(&Capability::TextRerank));
        assert!(adapter.supports(&Capability::AudioTranscribe));
    }

    #[tokio::test]
    async fn noop_execute_returns_unsuccessful() {
        let adapter = NoopAdapter;
        let response = adapter
            .execute(&test_config(), &test_request())
            .await
            .unwrap();

        assert!(!response.success);
        assert!(
            response
                .content
                .as_ref()
                .unwrap()
                .contains("No inference provider")
        );
        assert_eq!(response.attempts.len(), 1);
        assert_eq!(response.attempts[0].status, AttemptStatus::Failed);
    }

    #[tokio::test]
    async fn noop_execute_never_errors() {
        let adapter = NoopAdapter;
        let result = adapter.execute(&test_config(), &test_request()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn noop_stream_returns_single_chunk() {
        let adapter = NoopAdapter;
        let mut stream = adapter
            .stream(&test_config(), &test_request())
            .await
            .unwrap();

        let first = stream.next().await;
        assert!(first.is_some());
        let chunk = first.unwrap().unwrap();
        assert!(chunk.content.contains("No inference provider"));

        let second = stream.next().await;
        assert!(second.is_none());
    }
}
