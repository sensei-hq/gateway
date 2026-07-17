use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use futures::stream;

use super::InferenceAdapter;
use crate::types::capability::Capability;
use crate::types::config::RouterConfig;
use crate::types::error::GatewayError;
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
