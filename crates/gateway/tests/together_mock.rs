//! Integration tests for the Together AI adapter using wiremock.
//!
//! `TogetherAdapter` is an OpenAI-compatible provider with Bearer-token
//! auth implementing `ChatModel` + `ImageModel`:
//!
//! - `chat()` -> POST `{base}/chat/completions`, response parsed from
//!   `choices[].message.content` + `usage`.
//! - `generate_image()` -> POST `{base}/images/generations`, response
//!   parsed from `data[].url` / `data[].b64_json`.
//!
//! `chat_stream()` opens a real SSE stream against `{base}/chat/completions`,
//! yielding `data: ` chunks.
//!
//! Status mappings for both endpoints: 401/403 -> Authentication,
//! 429 -> RateLimit, everything else -> ProviderError.
//!
//! `base_url()` trims a trailing `/` from `config.url`; `server.uri()`
//! has no trailing slash, so the mocked paths are exactly
//! `/chat/completions`, `/images/generations`, etc.

use std::collections::HashMap;

use futures::StreamExt;

use gateway::types::config::RouterConfig;
use gateway::types::error::GatewayError;
use gateway::types::io::{ChatRequest, ImageRequest};
use gateway::types::request::{Message, MessageRole};

use gateway::adapters::capability::{ChatModel, ImageModel};
use gateway::adapters::together::TogetherAdapter;

use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
        model: Some("meta-llama/Llama-3.3-70B-Instruct-Turbo".to_string()),
        messages: vec![Message::text(MessageRole::User, "Hello, world!")],
        system: Some("You are helpful.".to_string()),
        max_tokens: Some(128),
        temperature: Some(0.5),
        tools: Vec::new(),
    }
}

fn image_request() -> ImageRequest {
    ImageRequest {
        model: None,
        prompt: "a red fox in a snowy forest".to_string(),
        size: Some("512x512".to_string()),
        quality: None,
        style: None,
        n: 1,
    }
}

// ---------------------------------------------------------------------------
// Chat happy path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn together_chat_happy_path() {
    let server = MockServer::start().await;

    let canned = serde_json::json!({
        "choices": [{
            "message": {"content": "Hello from mock Together!"},
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 11,
            "completion_tokens": 7,
            "total_tokens": 18
        }
    });

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&canned))
        .mount(&server)
        .await;

    let adapter = TogetherAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = chat_request();

    let response = adapter.chat(&config, &request).await.unwrap();

    assert_eq!(
        response.content.as_deref(),
        Some("Hello from mock Together!")
    );
    assert_eq!(
        response.model.as_deref(),
        Some("meta-llama/Llama-3.3-70B-Instruct-Turbo"),
    );

    let usage = response.usage.expect("expected usage in chat response");
    assert_eq!(usage.input_tokens, 11);
    assert_eq!(usage.output_tokens, 7);
    assert_eq!(usage.total_tokens, 18);
}

// A chat request with no explicit model falls back to the default chat
// model, and `usage` is optional (absent in this response body).
#[tokio::test]
async fn together_chat_default_model_and_no_usage() {
    let server = MockServer::start().await;

    let canned = serde_json::json!({
        "choices": [{
            "message": {"content": "defaulted"},
            "finish_reason": "stop"
        }]
    });

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&canned))
        .mount(&server)
        .await;

    let adapter = TogetherAdapter::new().unwrap();
    let config = router_config(&server.uri());

    let mut request = chat_request();
    request.model = None; // exercise the DEFAULT_CHAT_MODEL branch

    let response = adapter.chat(&config, &request).await.unwrap();

    assert_eq!(response.content.as_deref(), Some("defaulted"));
    assert_eq!(
        response.model.as_deref(),
        Some("meta-llama/Llama-3.3-70B-Instruct-Turbo"),
    );
    assert!(response.usage.is_none());
}

// ---------------------------------------------------------------------------
// Chat error mappings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn together_chat_401_maps_to_authentication() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(401).set_body_string("invalid api key"))
        .mount(&server)
        .await;

    let adapter = TogetherAdapter::new().unwrap();
    let config = router_config(&server.uri());

    let err = adapter.chat(&config, &chat_request()).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::Authentication { .. }),
        "expected Authentication error, got: {err:?}",
    );
}

#[tokio::test]
async fn together_chat_403_maps_to_authentication() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;

    let adapter = TogetherAdapter::new().unwrap();
    let config = router_config(&server.uri());

    let err = adapter.chat(&config, &chat_request()).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::Authentication { .. }),
        "expected Authentication error, got: {err:?}",
    );
}

#[tokio::test]
async fn together_chat_429_maps_to_rate_limit() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .mount(&server)
        .await;

    let adapter = TogetherAdapter::new().unwrap();
    let config = router_config(&server.uri());

    let err = adapter.chat(&config, &chat_request()).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::RateLimit { .. }),
        "expected RateLimit error, got: {err:?}",
    );
}

#[tokio::test]
async fn together_chat_500_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal server error"))
        .mount(&server)
        .await;

    let adapter = TogetherAdapter::new().unwrap();
    let config = router_config(&server.uri());

    let err = adapter.chat(&config, &chat_request()).await.unwrap_err();
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

// A 200 response whose body is not valid chat JSON exercises the
// parse-failure branch, which yields a ProviderError.
#[tokio::test]
async fn together_chat_bad_json_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&server)
        .await;

    let adapter = TogetherAdapter::new().unwrap();
    let config = router_config(&server.uri());

    let err = adapter.chat(&config, &chat_request()).await.unwrap_err();
    assert!(
        matches!(
            err,
            GatewayError::ProviderError {
                status: Some(200),
                ..
            }
        ),
        "expected ProviderError from parse failure, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// Image generation happy path + error mappings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn together_image_happy_path() {
    let server = MockServer::start().await;

    let canned = serde_json::json!({
        "data": [
            {"url": "https://together.ai/output/image1.png"}
        ]
    });

    Mock::given(method("POST"))
        .and(path("/v1/images/generations"))
        .and(header("authorization", "Bearer test-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&canned))
        .mount(&server)
        .await;

    let adapter = TogetherAdapter::new().unwrap();
    let config = router_config(&server.uri());

    let response = adapter
        .generate_image(&config, &image_request())
        .await
        .unwrap();

    let images = response.images;
    assert_eq!(images.len(), 1);
    assert_eq!(
        images[0].url.as_deref(),
        Some("https://together.ai/output/image1.png"),
    );
    assert!(images[0].b64_json.is_none());
}

// Cover the b64_json branch of the image response mapping.
#[tokio::test]
async fn together_image_b64_json_variant() {
    let server = MockServer::start().await;

    let canned = serde_json::json!({
        "data": [
            {"b64_json": "aGVsbG8="}
        ]
    });

    Mock::given(method("POST"))
        .and(path("/v1/images/generations"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&canned))
        .mount(&server)
        .await;

    let adapter = TogetherAdapter::new().unwrap();
    let config = router_config(&server.uri());

    let response = adapter
        .generate_image(&config, &image_request())
        .await
        .unwrap();

    let images = response.images;
    assert_eq!(images.len(), 1);
    assert!(images[0].url.is_none());
    assert_eq!(images[0].b64_json.as_deref(), Some("aGVsbG8="));
}

#[tokio::test]
async fn together_image_401_maps_to_authentication() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/images/generations"))
        .respond_with(ResponseTemplate::new(401).set_body_string("invalid api key"))
        .mount(&server)
        .await;

    let adapter = TogetherAdapter::new().unwrap();
    let config = router_config(&server.uri());

    let err = adapter
        .generate_image(&config, &image_request())
        .await
        .unwrap_err();
    assert!(
        matches!(err, GatewayError::Authentication { .. }),
        "expected Authentication error, got: {err:?}",
    );
}

#[tokio::test]
async fn together_image_429_maps_to_rate_limit() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/images/generations"))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .mount(&server)
        .await;

    let adapter = TogetherAdapter::new().unwrap();
    let config = router_config(&server.uri());

    let err = adapter
        .generate_image(&config, &image_request())
        .await
        .unwrap_err();
    assert!(
        matches!(err, GatewayError::RateLimit { .. }),
        "expected RateLimit error, got: {err:?}",
    );
}

#[tokio::test]
async fn together_image_500_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/images/generations"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let adapter = TogetherAdapter::new().unwrap();
    let config = router_config(&server.uri());

    let err = adapter
        .generate_image(&config, &image_request())
        .await
        .unwrap_err();
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

// A 200 image response with an unparseable body hits the image
// parse-failure branch.
#[tokio::test]
async fn together_image_bad_json_maps_to_provider_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/images/generations"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&server)
        .await;

    let adapter = TogetherAdapter::new().unwrap();
    let config = router_config(&server.uri());

    let err = adapter
        .generate_image(&config, &image_request())
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            GatewayError::ProviderError {
                status: Some(200),
                ..
            }
        ),
        "expected ProviderError from image parse failure, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// Missing API key -> Authentication (no HTTP call needed)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn together_missing_api_key_returns_authentication() {
    let server = MockServer::start().await;

    // No literal key and an env var that does not exist.
    let config = RouterConfig {
        url: server.uri(),
        api_key: None,
        api_key_env: Some("__NONEXISTENT_TOGETHER_KEY_FOR_TEST__".to_string()),
        enabled: true,
        timeout_ms: Some(5000),
        headers: HashMap::new(),
    };

    let adapter = TogetherAdapter::new().unwrap();

    let err = adapter.chat(&config, &chat_request()).await.unwrap_err();
    assert!(
        matches!(err, GatewayError::Authentication { .. }),
        "expected Authentication error for missing key, got: {err:?}",
    );
}

// ---------------------------------------------------------------------------
// from_config constructor
// ---------------------------------------------------------------------------

#[tokio::test]
async fn together_from_config_executes_chat() {
    let server = MockServer::start().await;

    let canned = serde_json::json!({
        "choices": [{"message": {"content": "via from_config"}, "finish_reason": "stop"}]
    });

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&canned))
        .mount(&server)
        .await;

    let config = router_config(&server.uri());
    let adapter = TogetherAdapter::from_config(&config).unwrap();

    let response = adapter.chat(&config, &chat_request()).await.unwrap();
    assert_eq!(response.content.as_deref(), Some("via from_config"));
}

// ---------------------------------------------------------------------------
// Streaming (real SSE body)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn together_stream_happy_path() {
    let server = MockServer::start().await;

    // OpenAI-compatible SSE: one content delta per event, terminated by
    // `data: [DONE]`.
    let sse_body = concat!(
        "data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\" world\"},\"finish_reason\":null}]}\n\n",
        "data: {\"choices\":[{\"delta\":{\"content\":\"\"},\"finish_reason\":\"stop\"}],",
        "\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5}}\n\n",
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

    let adapter = TogetherAdapter::new().unwrap();
    let config = router_config(&server.uri());

    let mut stream = adapter.chat_stream(&config, &chat_request()).await.unwrap();

    let mut collected = String::new();
    let mut saw_finish = false;
    while let Some(result) = stream.next().await {
        let chunk = result.expect("stream chunk should be Ok");
        collected.push_str(&chunk.content);
        if chunk.finish_reason.as_deref() == Some("stop") {
            saw_finish = true;
        }
    }

    assert_eq!(collected, "Hello world");
    assert!(saw_finish, "expected a chunk carrying finish_reason=stop");
}

#[tokio::test]
async fn together_stream_401_maps_to_authentication() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(401).set_body_string("invalid api key"))
        .mount(&server)
        .await;

    let adapter = TogetherAdapter::new().unwrap();
    let config = router_config(&server.uri());

    let result = adapter.chat_stream(&config, &chat_request()).await;
    assert!(result.is_err(), "chat_stream() must surface auth failure");
    let err = result.err().unwrap();
    assert!(
        matches!(err, GatewayError::Authentication { .. }),
        "expected Authentication error from chat_stream(), got: {err:?}",
    );
}
