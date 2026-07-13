//! Integration tests for the Grok (xAI) adapter using wiremock.
//!
//! Grok is an OpenAI-compatible chat provider that additionally exposes
//! Whisper-style STT (`/v1/audio/transcriptions`) and TTS
//! (`/v1/audio/speech`) endpoints. `execute()` dispatches on the payload
//! variant:
//!   - `Chat` -> POST `/v1/chat/completions` (via the shared `http_json`
//!     helper), parses `choices[0].message.content` + `usage`.
//!   - `Stt`  -> multipart POST `/v1/audio/transcriptions`, parses `{text}`.
//!   - `Tts`  -> POST `/v1/audio/speech`, returns the raw audio bytes.
//!   - `Embed` / `ImageGenerate` / `VideoGenerate` -> ProviderError
//!     (xAI offers no such API), returned *before* any HTTP call.
//!
//! All auth uses a Bearer token; every endpoint maps 401/403 ->
//! Authentication, 429 -> RateLimit, and any other non-success status ->
//! ProviderError { status: Some(..), .. }.

use std::collections::HashMap;

use gateway::types::capability::Capability;
use gateway::types::config::RouterConfig;
use gateway::types::error::GatewayError;
use gateway::types::request::{AudioFormat, InferenceRequest, Message, MessageRole, Payload};

use gateway::adapters::InferenceAdapter;
use gateway::adapters::grok::GrokAdapter;

use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const DEFAULT_CHAT_MODEL: &str = "grok-4-fast";
const DEFAULT_AUDIO_MODEL: &str = "grok-2-audio";

fn router_config(url: &str) -> RouterConfig {
    RouterConfig {
        url: url.to_string(),
        api_key: Some("test-key".into()),
        api_key_env: None,
        enabled: true,
        timeout_ms: Some(5000),
        headers: HashMap::new(),
    }
}

fn chat_request() -> InferenceRequest {
    InferenceRequest {
        capability: Capability::TextChat,
        model: None,
        router: None,
        chain: None,
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, "Hello, world!")],
            system: Some("You are a helpful assistant.".to_string()),
            max_tokens: Some(128),
            temperature: Some(0.5),
            tools: Vec::new(),
        },
        budget: None,
    }
}

fn stt_request(format: &str) -> InferenceRequest {
    InferenceRequest {
        capability: Capability::AudioTranscribe,
        model: None,
        router: None,
        chain: None,
        payload: Payload::Stt {
            audio: vec![0xFF, 0xFB, 0x90, 0x00],
            language: Some("en".to_string()),
            format: format.to_string(),
        },
        budget: None,
    }
}

fn tts_request() -> InferenceRequest {
    InferenceRequest {
        capability: Capability::AudioGenerate,
        model: None,
        router: None,
        chain: None,
        payload: Payload::Tts {
            text: "Hello world".to_string(),
            voice: Some("Ara".to_string()),
            speed: None,
            output_format: AudioFormat::Mp3,
        },
        budget: None,
    }
}

fn embed_request() -> InferenceRequest {
    InferenceRequest {
        capability: Capability::TextEmbed,
        model: None,
        router: None,
        chain: None,
        payload: Payload::Embed {
            texts: vec!["hello world".to_string()],
        },
        budget: None,
    }
}

// ---------------------------------------------------------------------------
// Chat — happy path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grok_chat_happy_path() {
    let server = MockServer::start().await;

    let canned = serde_json::json!({
        "choices": [{
            "message": {"content": "Greetings, human!"},
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 5,
            "total_tokens": 15
        }
    });

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&canned))
        .mount(&server)
        .await;

    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = chat_request();

    let response = adapter.execute(&config, &request).await.unwrap();

    assert!(response.success);
    assert_eq!(response.content.as_deref(), Some("Greetings, human!"));
    assert_eq!(response.model.as_deref(), Some(DEFAULT_CHAT_MODEL));

    let usage = response.usage.expect("expected usage");
    assert_eq!(usage.input_tokens, 10);
    assert_eq!(usage.output_tokens, 5);
    assert_eq!(usage.total_tokens, 15);
}

// ---------------------------------------------------------------------------
// Chat — error mappings (routed through the shared http_json helper)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grok_chat_401_maps_to_authentication() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(401)
                .set_body_json(serde_json::json!({"error": {"message": "invalid api key"}})),
        )
        .mount(&server)
        .await;

    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = chat_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::Authentication { .. }),
        "expected Authentication error, got: {err:?}",
    );
}

#[tokio::test]
async fn grok_chat_403_maps_to_authentication() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;

    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = chat_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::Authentication { .. }),
        "expected Authentication error, got: {err:?}",
    );
}

#[tokio::test]
async fn grok_chat_429_maps_to_rate_limit() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_json(serde_json::json!({"error": {"message": "rate limited"}})),
        )
        .mount(&server)
        .await;

    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = chat_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::RateLimit { .. }),
        "expected RateLimit error, got: {err:?}",
    );
}

#[tokio::test]
async fn grok_chat_500_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal server error"))
        .mount(&server)
        .await;

    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = chat_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(
            err,
            GatewayError::ProviderError {
                status: Some(500),
                ..
            }
        ),
        "expected ProviderError with status 500, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// STT (AudioTranscribe) — happy path + error mappings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grok_stt_happy_path() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/audio/transcriptions"))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"text": "hello from grok"})),
        )
        .mount(&server)
        .await;

    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = stt_request("mp3");

    let response = adapter.execute(&config, &request).await.unwrap();

    assert!(response.success);
    assert_eq!(response.transcription.as_deref(), Some("hello from grok"));
    assert_eq!(response.model.as_deref(), Some(DEFAULT_AUDIO_MODEL));
    assert!(response.content.is_none());
}

#[tokio::test]
async fn grok_stt_unsupported_format_returns_provider_error() {
    let server = MockServer::start().await;

    // No mocks mounted: the adapter must reject the format before any HTTP call.
    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = stt_request("ogg");

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { status: None, .. }),
        "expected ProviderError with no status for unsupported audio format, got: {err:?}",
    );
}

#[tokio::test]
async fn grok_stt_401_maps_to_authentication() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/audio/transcriptions"))
        .respond_with(ResponseTemplate::new(401).set_body_string("invalid api key"))
        .mount(&server)
        .await;

    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = stt_request("wav");

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::Authentication { .. }),
        "expected Authentication error, got: {err:?}",
    );
}

#[tokio::test]
async fn grok_stt_429_maps_to_rate_limit() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/audio/transcriptions"))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .mount(&server)
        .await;

    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = stt_request("webm");

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::RateLimit { .. }),
        "expected RateLimit error, got: {err:?}",
    );
}

#[tokio::test]
async fn grok_stt_500_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/audio/transcriptions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = stt_request("m4a");

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(
            err,
            GatewayError::ProviderError {
                status: Some(500),
                ..
            }
        ),
        "expected ProviderError with status 500, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// TTS (AudioGenerate) — happy path + error mappings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grok_tts_happy_path() {
    let server = MockServer::start().await;

    let audio_bytes: Vec<u8> = vec![0x49, 0x44, 0x33, 0x04]; // "ID3\x04"

    Mock::given(method("POST"))
        .and(path("/v1/audio/speech"))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(audio_bytes.clone()))
        .mount(&server)
        .await;

    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = tts_request();

    let response = adapter.execute(&config, &request).await.unwrap();

    assert!(response.success);
    assert_eq!(response.audio.as_deref(), Some(audio_bytes.as_slice()));
    assert_eq!(response.model.as_deref(), Some(DEFAULT_AUDIO_MODEL));
    assert!(response.transcription.is_none());
}

#[tokio::test]
async fn grok_tts_403_maps_to_authentication() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/audio/speech"))
        .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;

    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = tts_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::Authentication { .. }),
        "expected Authentication error, got: {err:?}",
    );
}

#[tokio::test]
async fn grok_tts_429_maps_to_rate_limit() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/audio/speech"))
        .respond_with(ResponseTemplate::new(429).set_body_string("slow down"))
        .mount(&server)
        .await;

    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = tts_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::RateLimit { .. }),
        "expected RateLimit error, got: {err:?}",
    );
}

#[tokio::test]
async fn grok_tts_500_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/audio/speech"))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
        .mount(&server)
        .await;

    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = tts_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(
            err,
            GatewayError::ProviderError {
                status: Some(500),
                ..
            }
        ),
        "expected ProviderError with status 500, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// Unsupported capabilities — return before any HTTP call
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grok_embed_returns_provider_error() {
    let server = MockServer::start().await;

    // No mocks mounted: xAI offers no embeddings API, so the adapter returns
    // a ProviderError with no status before making any HTTP call.
    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = embed_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { status: None, .. }),
        "expected ProviderError with no status for embed, got: {err:?}",
    );
}

#[tokio::test]
async fn grok_image_generate_returns_provider_error() {
    let server = MockServer::start().await;

    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = InferenceRequest {
        capability: Capability::ImageGenerate,
        model: None,
        router: None,
        chain: None,
        payload: Payload::ImageGenerate {
            prompt: "a red fox".to_string(),
            size: None,
            quality: None,
            style: None,
            n: 1,
        },
        budget: None,
    };

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { status: None, .. }),
        "expected ProviderError with no status for image generation, got: {err:?}",
    );
}

#[tokio::test]
async fn grok_video_generate_returns_provider_error() {
    let server = MockServer::start().await;

    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = InferenceRequest {
        capability: Capability::VideoGenerate,
        model: None,
        router: None,
        chain: None,
        payload: Payload::VideoGenerate {
            prompt: "a timelapse".to_string(),
            duration_secs: None,
            resolution: None,
        },
        budget: None,
    };

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::ProviderError { status: None, .. }),
        "expected ProviderError with no status for video generation, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// Missing API key -> Authentication (literal + env both absent)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grok_missing_api_key_returns_authentication() {
    let server = MockServer::start().await;

    let adapter = GrokAdapter::new().unwrap();
    let config = RouterConfig {
        url: server.uri(),
        api_key: None,
        api_key_env: None,
        enabled: true,
        timeout_ms: Some(5000),
        headers: HashMap::new(),
    };
    let request = chat_request();

    let err = adapter.execute(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::Authentication { .. }),
        "expected Authentication error for missing key, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// stream() — SSE chat happy path + error mapping
// ---------------------------------------------------------------------------

#[tokio::test]
async fn grok_stream_chat_collects_content() {
    use futures::StreamExt;

    let server = MockServer::start().await;

    // Minimal OpenAI-style SSE body: two content deltas then [DONE].
    let sse_body = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5}}\n\n",
        "data: [DONE]\n\n",
    );

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(sse_body),
        )
        .mount(&server)
        .await;

    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = chat_request();

    let mut stream = adapter.stream(&config, &request).await.unwrap();
    let mut collected = String::new();
    let mut finish_reasons = Vec::new();

    while let Some(result) = stream.next().await {
        let chunk = result.unwrap();
        collected.push_str(&chunk.content);
        if let Some(fr) = chunk.finish_reason {
            finish_reasons.push(fr);
        }
    }

    assert_eq!(collected, "Hello world");
    assert_eq!(finish_reasons, vec!["stop".to_string()]);
}

#[tokio::test]
async fn grok_stream_wrong_payload_returns_provider_error() {
    let server = MockServer::start().await;

    // No mocks mounted: streaming only supports chat payloads, so the adapter
    // returns before any HTTP call.
    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = tts_request();

    // `stream()`'s Ok variant (a boxed Stream) is not Debug, so unwrap_err()
    // won't compile — extract the error via `.err()` instead.
    let err = adapter
        .stream(&config, &request)
        .await
        .err()
        .expect("expected stream() to error");
    assert!(
        matches!(err, GatewayError::ProviderError { status: None, .. }),
        "expected ProviderError for non-chat stream payload, got: {err:?}",
    );
}

#[tokio::test]
async fn grok_stream_500_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("stream boom"))
        .mount(&server)
        .await;

    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = chat_request();

    let err = adapter
        .stream(&config, &request)
        .await
        .err()
        .expect("expected stream() to error");
    assert!(
        matches!(
            err,
            GatewayError::ProviderError {
                status: Some(500),
                ..
            }
        ),
        "expected ProviderError with status 500 from stream(), got: {err:?}",
    );
}
