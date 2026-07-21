//! Integration tests for the Grok (xAI) adapter using wiremock.
//!
//! Grok is an OpenAI-compatible chat provider that additionally exposes
//! Whisper-style STT (`/v1/audio/transcriptions`) and TTS
//! (`/v1/audio/speech`) endpoints via the capability traits:
//!   - `chat()`       -> POST `/v1/chat/completions`, parses
//!     `choices[0].message.content` + `usage`.
//!   - `transcribe()` -> multipart POST `/v1/audio/transcriptions`,
//!     parses `{text}`.
//!   - `speak()`      -> POST `/v1/audio/speech`, returns the raw audio bytes.
//!
//! All auth uses a Bearer token; every endpoint maps 401/403 ->
//! Authentication, 429 -> RateLimit, and any other non-success status ->
//! ProviderError { status: Some(..), .. }.

use std::collections::HashMap;

use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;
use kernel::types::io::{ChatRequest, SttRequest, TtsRequest};
use kernel::types::request::{AudioFormat, Message, MessageRole};

use cloud_providers::grok::GrokAdapter;
use kernel::adapters::capability::{ChatModel, SttModel, TtsModel};

use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const DEFAULT_CHAT_MODEL: &str = "grok-4-fast";

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

fn chat_request() -> ChatRequest {
    ChatRequest {
        model: None,
        messages: vec![Message::text(MessageRole::User, "Hello, world!")],
        system: Some("You are a helpful assistant.".to_string()),
        max_tokens: Some(128),
        temperature: Some(0.5),
        tools: Vec::new(),
    }
}

fn stt_request(format: &str) -> SttRequest {
    SttRequest {
        model: None,
        audio: vec![0xFF, 0xFB, 0x90, 0x00],
        language: Some("en".to_string()),
        format: format.to_string(),
    }
}

fn tts_request() -> TtsRequest {
    TtsRequest {
        model: None,
        text: "Hello world".to_string(),
        voice: Some("Ara".to_string()),
        speed: None,
        output_format: AudioFormat::Mp3,
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

    let response = adapter.chat(&config, &request).await.unwrap();

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

    let err = adapter.chat(&config, &request).await.unwrap_err();
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

    let err = adapter.chat(&config, &request).await.unwrap_err();
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

    let err = adapter.chat(&config, &request).await.unwrap_err();
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

    let err = adapter.chat(&config, &request).await.unwrap_err();
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
// STT (transcribe) — happy path + error mappings
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

    let response = adapter.transcribe(&config, &request).await.unwrap();

    assert_eq!(response.transcription, "hello from grok");
}

#[tokio::test]
async fn grok_stt_unsupported_format_returns_provider_error() {
    let server = MockServer::start().await;

    // No mocks mounted: the adapter must reject the format before any HTTP call.
    let adapter = GrokAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = stt_request("ogg");

    let err = adapter.transcribe(&config, &request).await.unwrap_err();
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

    let err = adapter.transcribe(&config, &request).await.unwrap_err();
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

    let err = adapter.transcribe(&config, &request).await.unwrap_err();
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

    let err = adapter.transcribe(&config, &request).await.unwrap_err();
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
// TTS (speak) — happy path + error mappings
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

    let response = adapter.speak(&config, &request).await.unwrap();

    assert_eq!(response.audio, audio_bytes);
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

    let err = adapter.speak(&config, &request).await.unwrap_err();
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

    let err = adapter.speak(&config, &request).await.unwrap_err();
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

    let err = adapter.speak(&config, &request).await.unwrap_err();
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

    let err = adapter.chat(&config, &request).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::Authentication { .. }),
        "expected Authentication error for missing key, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// chat_stream() — SSE chat happy path + error mapping
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

    let mut stream = adapter.chat_stream(&config, &request).await.unwrap();
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
        .chat_stream(&config, &request)
        .await
        .err()
        .expect("expected chat_stream() to error");
    assert!(
        matches!(
            err,
            GatewayError::ProviderError {
                status: Some(500),
                ..
            }
        ),
        "expected ProviderError with status 500 from chat_stream(), got: {err:?}",
    );
}
