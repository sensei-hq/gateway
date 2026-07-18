use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use futures::stream;

use crate::types::capability::Capability;
use crate::types::config::RouterConfig;
use crate::types::error::GatewayError;
use crate::types::io::{
    ChatRequest, ChatResponse, EmbedRequest, EmbedResponse, ImageRequest, ImageResponse,
    SttRequest, SttResponse, TtsRequest, TtsResponse, VideoRequest, VideoResponse,
};
use crate::types::request::StreamChunk;

/// Graceful-degradation adapter that never errors.
///
/// Implements every capability and returns an `Ok` "no inference provider
/// available" placeholder for each, so the gateway always produces a response
/// rather than an error. This is the last-resort fallback.
///
/// NOTE: the typed capability responses carry no `success` field, so the old
/// `success: false` degraded signal is no longer expressed here — the engine's
/// dispatch currently reports `success: true`. Reinstating a degraded/`success`
/// signal is a tracked follow-up (see the gateway roadmap / observability pass).
pub struct NoopAdapter;

impl NoopAdapter {
    fn unavailable_message(capability: &Capability) -> String {
        format!(
            "No inference provider available for capability {:?}. \
             Install Ollama or configure an API key.",
            capability,
        )
    }
}

// ---------------------------------------------------------------------------
// Capability traits (target model). noop is the catch-all: implements every
// capability and returns the same "no provider" placeholder as the execute
// path. NOTE: the typed responses have no `success` field, so the old
// `success: false` degraded-signal cannot be carried here — the engine decides
// success at the Phase 4 dispatch boundary. Traits referenced by full path.
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
    async fn register_into(self: std::sync::Arc<Self>, reg: &crate::adapters::AdapterRegistry) {
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
    use crate::adapters::capability::{ChatModel, Model};
    use crate::types::io::ChatRequest;
    use crate::types::request::{Message, MessageRole};
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

    fn test_chat_request() -> ChatRequest {
        ChatRequest {
            model: None,
            messages: vec![Message::text(MessageRole::User, "hello".to_string())],
            system: None,
            max_tokens: None,
            temperature: None,
            tools: Vec::new(),
        }
    }

    #[tokio::test]
    async fn noop_chatmodel_returns_canned_content() {
        let adapter = NoopAdapter;
        assert_eq!(Model::id(&adapter), "noop");
        let resp = adapter
            .chat(&test_config(), &test_chat_request())
            .await
            .unwrap();
        assert!(resp.content.unwrap().contains("No inference provider"));
    }

    #[tokio::test]
    async fn noop_chat_stream_returns_single_chunk() {
        let adapter = NoopAdapter;
        let mut stream = adapter
            .chat_stream(&test_config(), &test_chat_request())
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
