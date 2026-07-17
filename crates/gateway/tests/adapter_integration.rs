//! Integration tests for adapter execute() methods using wiremock.
//!
//! These tests spin up an in-process HTTP mock server and verify that
//! adapters correctly serialize requests, parse responses, and handle
//! error conditions.

use std::collections::HashMap;

use gateway::types::capability::Capability;
use gateway::types::config::RouterConfig;
use gateway::types::error::GatewayError;
use gateway::types::request::{InferenceRequest, Message, MessageRole, Payload};

use gateway::adapters::InferenceAdapter;
use gateway::adapters::anthropic::AnthropicAdapter;
use gateway::adapters::ollama::OllamaAdapter;
use gateway::adapters::openai::OpenAIAdapter;

use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn router_config(url: &str) -> RouterConfig {
    RouterConfig {
        url: url.to_string(),
        api_key_env: None,
        api_key: None,
        enabled: true,
        timeout_ms: Some(5000),
        headers: HashMap::new(),
    }
}

fn router_config_with_key(url: &str, env_var: &str) -> RouterConfig {
    RouterConfig {
        url: url.to_string(),
        api_key_env: Some(env_var.to_string()),
        api_key: None,
        enabled: true,
        timeout_ms: Some(5000),
        headers: HashMap::new(),
    }
}

fn chat_request(model: &str) -> InferenceRequest {
    InferenceRequest {
        capability: Capability::TextChat,
        model: Some(model.to_string()),
        router: None,
        chain: None,
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, "Hello, world!")],
            system: None,
            max_tokens: Some(128),
            temperature: Some(0.5),
            tools: Vec::new(),
        },
        budget: None,
    }
}

fn embed_request(model: &str) -> InferenceRequest {
    InferenceRequest {
        capability: Capability::TextEmbed,
        model: Some(model.to_string()),
        router: None,
        chain: None,
        payload: Payload::Embed {
            texts: vec!["hello world".to_string()],
        },
        budget: None,
    }
}

// ---------------------------------------------------------------------------
// Ollama mock tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ollama_chat_mock() {
    let server = MockServer::start().await;

    let canned = serde_json::json!({
        "choices": [{
            "message": {"content": "Hello from mock Ollama!"},
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 8,
            "total_tokens": 18
        }
    });

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&canned))
        .mount(&server)
        .await;

    let adapter = OllamaAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = chat_request("gemma3:27b");

    let response = adapter.execute(&config, &request).await.unwrap();
    assert!(response.success);
    assert_eq!(response.content.as_deref(), Some("Hello from mock Ollama!"),);
    assert!(response.usage.is_some());
    let usage = response.usage.unwrap();
    assert_eq!(usage.input_tokens, 10);
    assert_eq!(usage.output_tokens, 8);
    assert_eq!(usage.total_tokens, 18);
}

#[tokio::test]
async fn ollama_embed_mock() {
    let server = MockServer::start().await;

    let canned = serde_json::json!({
        "data": [
            {"embedding": [0.1, 0.2, 0.3, 0.4, 0.5], "index": 0}
        ],
        "usage": {
            "prompt_tokens": 4,
            "total_tokens": 4
        }
    });

    Mock::given(method("POST"))
        .and(path("/v1/embeddings"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&canned))
        .mount(&server)
        .await;

    let adapter = OllamaAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = embed_request("all-minilm");

    let response = adapter.execute(&config, &request).await.unwrap();
    assert!(response.success);
    assert!(response.embeddings.is_some());
    let embeddings = response.embeddings.unwrap();
    assert_eq!(embeddings.len(), 1);
    assert_eq!(embeddings[0], vec![0.1, 0.2, 0.3, 0.4, 0.5]);
}

#[tokio::test]
async fn ollama_chat_error_500() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(
            ResponseTemplate::new(500)
                .set_body_json(serde_json::json!({"error": "internal server error"})),
        )
        .mount(&server)
        .await;

    let adapter = OllamaAdapter::new().unwrap();
    let config = router_config(&server.uri());
    let request = chat_request("gemma3:27b");

    let result = adapter.execute(&config, &request).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
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
// OpenAI mock tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn openai_chat_mock() {
    let server = MockServer::start().await;

    // Set env var for the test
    let env_key = "__TEST_OPENAI_KEY_MOCK__";
    unsafe {
        std::env::set_var(env_key, "sk-test-mock-key");
    }

    let canned = serde_json::json!({
        "choices": [{
            "message": {"content": "Hello from mock OpenAI!"},
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 12,
            "completion_tokens": 6,
            "total_tokens": 18
        }
    });

    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(header("authorization", "Bearer sk-test-mock-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&canned))
        .mount(&server)
        .await;

    let adapter = OpenAIAdapter::new().unwrap();
    let config = router_config_with_key(&server.uri(), env_key);
    let request = chat_request("gpt-4o-mini");

    let response = adapter.execute(&config, &request).await.unwrap();
    assert!(response.success);
    assert_eq!(response.content.as_deref(), Some("Hello from mock OpenAI!"),);
    let usage = response.usage.unwrap();
    assert_eq!(usage.input_tokens, 12);
    assert_eq!(usage.output_tokens, 6);

    // Cleanup
    unsafe {
        std::env::remove_var(env_key);
    }
}

#[tokio::test]
async fn openai_embed_mock() {
    let server = MockServer::start().await;

    let env_key = "__TEST_OPENAI_KEY_EMBED__";
    unsafe {
        std::env::set_var(env_key, "sk-test-embed-key");
    }

    let canned = serde_json::json!({
        "data": [
            {"embedding": [0.01, 0.02, 0.03], "index": 0}
        ],
        "usage": {
            "prompt_tokens": 3,
            "total_tokens": 3
        }
    });

    Mock::given(method("POST"))
        .and(path("/v1/embeddings"))
        .and(header("authorization", "Bearer sk-test-embed-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&canned))
        .mount(&server)
        .await;

    let adapter = OpenAIAdapter::new().unwrap();
    let config = router_config_with_key(&server.uri(), env_key);
    let request = embed_request("text-embedding-3-small");

    let response = adapter.execute(&config, &request).await.unwrap();
    assert!(response.success);
    let embeddings = response.embeddings.unwrap();
    assert_eq!(embeddings.len(), 1);
    assert_eq!(embeddings[0], vec![0.01, 0.02, 0.03]);

    unsafe {
        std::env::remove_var(env_key);
    }
}

// ---------------------------------------------------------------------------
// Anthropic mock tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn anthropic_chat_mock() {
    let server = MockServer::start().await;

    let env_key = "__TEST_ANTHROPIC_KEY_MOCK__";
    unsafe {
        std::env::set_var(env_key, "sk-ant-test-mock-key");
    }

    let canned = serde_json::json!({
        "content": [{"type": "text", "text": "Hello from mock Anthropic!"}],
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 15, "output_tokens": 7}
    });

    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(header("x-api-key", "sk-ant-test-mock-key"))
        .and(header("anthropic-version", "2023-06-01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&canned))
        .mount(&server)
        .await;

    let adapter = AnthropicAdapter::new().unwrap();
    let config = router_config_with_key(&server.uri(), env_key);
    let request = chat_request("claude-haiku-4-5-20250414");

    let response = adapter.execute(&config, &request).await.unwrap();
    assert!(response.success);
    assert_eq!(
        response.content.as_deref(),
        Some("Hello from mock Anthropic!"),
    );
    let usage = response.usage.unwrap();
    assert_eq!(usage.input_tokens, 15);
    assert_eq!(usage.output_tokens, 7);
    assert_eq!(usage.total_tokens, 22);

    unsafe {
        std::env::remove_var(env_key);
    }
}

#[tokio::test]
async fn anthropic_chat_401_auth_error() {
    let server = MockServer::start().await;

    let env_key = "__TEST_ANTHROPIC_KEY_401__";
    unsafe {
        std::env::set_var(env_key, "bad-key");
    }

    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(
            ResponseTemplate::new(401)
                .set_body_json(serde_json::json!({"error": {"message": "invalid api key"}})),
        )
        .mount(&server)
        .await;

    let adapter = AnthropicAdapter::new().unwrap();
    let config = router_config_with_key(&server.uri(), env_key);
    let request = chat_request("claude-haiku-4-5-20250414");

    let result = adapter.execute(&config, &request).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, GatewayError::Authentication { .. }),
        "expected Authentication error, got: {err:?}",
    );

    unsafe {
        std::env::remove_var(env_key);
    }
}

#[tokio::test]
async fn anthropic_chat_429_rate_limit() {
    let server = MockServer::start().await;

    let env_key = "__TEST_ANTHROPIC_KEY_429__";
    unsafe {
        std::env::set_var(env_key, "rate-limited-key");
    }

    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(
            ResponseTemplate::new(429)
                .set_body_json(serde_json::json!({"error": {"message": "rate limited"}})),
        )
        .mount(&server)
        .await;

    let adapter = AnthropicAdapter::new().unwrap();
    let config = router_config_with_key(&server.uri(), env_key);
    let request = chat_request("claude-haiku-4-5-20250414");

    let result = adapter.execute(&config, &request).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, GatewayError::RateLimit { .. }),
        "expected RateLimit error, got: {err:?}",
    );

    unsafe {
        std::env::remove_var(env_key);
    }
}

// ---------------------------------------------------------------------------
// Ollama live integration tests (require running Ollama)
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn ollama_embed_integration() {
    let adapter = OllamaAdapter::new().unwrap();
    let config = RouterConfig {
        url: "http://localhost:11434".to_string(),
        api_key_env: None,
        api_key: None,
        enabled: true,
        timeout_ms: Some(30000),
        headers: HashMap::new(),
    };
    let request = InferenceRequest {
        capability: Capability::TextEmbed,
        model: Some("all-minilm".to_string()),
        router: None,
        chain: None,
        payload: Payload::Embed {
            texts: vec!["hello world".to_string(), "test embedding".to_string()],
        },
        budget: None,
    };

    let response = adapter.execute(&config, &request).await.unwrap();
    assert!(response.success);
    let embeddings = response.embeddings.unwrap();
    assert_eq!(embeddings.len(), 2);
    assert!(!embeddings[0].is_empty());
    assert!(!embeddings[1].is_empty());
}

#[tokio::test]
#[ignore]
async fn ollama_chat_streaming_integration() {
    use futures::StreamExt;

    let adapter = OllamaAdapter::new().unwrap();
    let config = RouterConfig {
        url: "http://localhost:11434".to_string(),
        api_key_env: None,
        api_key: None,
        enabled: true,
        timeout_ms: Some(60000),
        headers: HashMap::new(),
    };
    let request = chat_request("llama3.2:latest");

    let mut stream = adapter.stream(&config, &request).await.unwrap();
    let mut collected = String::new();

    while let Some(result) = stream.next().await {
        let chunk = result.unwrap();
        collected.push_str(&chunk.content);
    }

    assert!(
        !collected.is_empty(),
        "Expected non-empty streaming response"
    );
}
