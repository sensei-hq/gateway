use std::pin::Pin;

use async_trait::async_trait;
use futures::stream::StreamExt;
use futures::Stream;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::base::{build_client, http_json, resolve_api_key};
use super::InferenceAdapter;
use crate::types::capability::Capability;
use crate::types::config::RouterConfig;
use crate::types::cost::TokenUsage;
use crate::types::error::GatewayError;
use crate::types::request::{
    InferenceRequest, InferenceResponse, Message, MessageRole, Payload, StreamChunk,
};

// ---------------------------------------------------------------------------
// Wire types — OpenAI-compatible request/response structs
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct ChatCompletionRequest {
    model: String,
    messages: Vec<ChatMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    stream: bool,
}

#[derive(Debug, Serialize)]
struct ChatMessage {
    role: String,
    content: String,
    }

#[derive(Debug, Serialize)]
struct EmbedRequest {
    model: String,
    input: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ChatCompletionResponse {
    choices: Vec<ChatChoice>,
    usage: Option<UsageResponse>,
}

#[derive(Debug, Deserialize)]
struct ChatChoice {
    message: ChatResponseMessage,
    #[allow(dead_code)]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChatResponseMessage {
    content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UsageResponse {
    prompt_tokens: Option<u32>,
    completion_tokens: Option<u32>,
    total_tokens: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct EmbedResponse {
    data: Vec<EmbedData>,
    usage: Option<UsageResponse>,
}

#[derive(Debug, Deserialize)]
struct EmbedData {
    embedding: Vec<f32>,
}

#[derive(Debug, Deserialize)]
struct StreamChatResponse {
    choices: Vec<StreamChoice>,
    usage: Option<UsageResponse>,
}

#[derive(Debug, Deserialize)]
struct StreamChoice {
    delta: StreamDelta,
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StreamDelta {
    content: Option<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const DEFAULT_MODEL: &str = "gemma3:27b";

fn role_to_string(role: &MessageRole) -> &'static str {
    match role {
        MessageRole::System => "system",
        MessageRole::User => "user",
        MessageRole::Assistant => "assistant",
        MessageRole::Tool => "tool",
    }
}

fn resolve_model(request: &InferenceRequest) -> String {
    request
        .model
        .clone()
        .unwrap_or_else(|| DEFAULT_MODEL.to_string())
}

fn build_chat_messages(
    messages: &[Message],
    system: &Option<String>,
) -> Vec<ChatMessage> {
    let mut out = Vec::new();
    if let Some(sys) = system {
        out.push(ChatMessage { role: "system".to_string(), content: sys.clone() });
    }
    for m in messages {
        out.push(ChatMessage { role: role_to_string(&m.role).to_string(), content: m.as_text().to_string() });
    }
    out
}

fn usage_from_response(usage: &Option<UsageResponse>) -> Option<TokenUsage> {
    usage.as_ref().map(|u| {
        let input = u.prompt_tokens.unwrap_or(0);
        let output = u.completion_tokens.unwrap_or(0);
        let total = u.total_tokens.unwrap_or(input + output);
        TokenUsage {
            input_tokens: input,
            output_tokens: output,
            total_tokens: total,
        }
    })
}

// ---------------------------------------------------------------------------
// OllamaAdapter
// ---------------------------------------------------------------------------

/// Adapter for Ollama's OpenAI-compatible inference endpoints.
///
/// Ollama exposes `/v1/chat/completions` and `/v1/embeddings` that follow the
/// OpenAI wire format, so no auth is typically required for local instances.
pub struct OllamaAdapter {
    client: Client,
}

/// Default per-request timeout when the adapter is built without explicit
/// config. A bare `reqwest::Client` has NO timeout, so a wedged Ollama
/// connection (accepted but never answered) hangs the caller forever; this
/// bounds it. Configured callers (`from_config`) override via
/// `RouterConfig::timeout_ms`.
const DEFAULT_TIMEOUT_SECS: u64 = 120;

impl OllamaAdapter {
    pub fn new() -> Result<Self, GatewayError> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .build()
            .map_err(|e| GatewayError::ProviderError {
                adapter: "ollama".into(),
                message: e.to_string(),
                status: None,
            })?;
        Ok(Self { client })
    }

    /// Create an adapter from a pre-built client (e.g. with timeout from config).
    pub fn from_config(config: &RouterConfig) -> Result<Self, GatewayError> {
        Ok(Self {
            client: build_client(config)?,
        })
    }
}

#[async_trait]
impl InferenceAdapter for OllamaAdapter {
    fn id(&self) -> &str {
        "ollama"
    }

    fn supports(&self, capability: &Capability) -> bool {
        matches!(
            capability,
            Capability::TextChat | Capability::TextComplete | Capability::TextEmbed
        )
    }

    async fn execute(
        &self,
        config: &RouterConfig,
        request: &InferenceRequest,
    ) -> Result<InferenceResponse, GatewayError> {
        let api_key = resolve_api_key(config);
        let model = resolve_model(request);

        match &request.payload {
            Payload::Chat {
                messages,
                system,
                max_tokens,
                temperature,
                tools: _,
            } => {
                let body = ChatCompletionRequest {
                    model: model.clone(),
                    messages: build_chat_messages(messages, system),
                    max_tokens: *max_tokens,
                    temperature: *temperature,
                    stream: false,
                };

                let resp: ChatCompletionResponse = http_json(
                    &self.client,
                    &config.url,
                    "/v1/chat/completions",
                    &body,
                    api_key.as_deref(),
                    &config.headers,
                )
                .await?;

                let content = resp
                    .choices
                    .first()
                    .and_then(|c| c.message.content.clone());
                let usage = usage_from_response(&resp.usage);

                Ok(InferenceResponse {
                    success: true,
                    content,
                    embeddings: None,
                    transcription: None,
                    audio: None,
                    images: None,
                    videos: None,
                    model: Some(model),
                    usage,
                    tool_calls: Vec::new(),
                    estimated_cost: None,
                    actual_cost: None,
                    attempts: vec![],
                })
            }
            Payload::Embed { texts } => {
                let body = EmbedRequest {
                    model: model.clone(),
                    input: texts.clone(),
                };

                let resp: EmbedResponse = http_json(
                    &self.client,
                    &config.url,
                    "/v1/embeddings",
                    &body,
                    api_key.as_deref(),
                    &config.headers,
                )
                .await?;

                let embeddings: Vec<Vec<f32>> =
                    resp.data.into_iter().map(|d| d.embedding).collect();
                let usage = usage_from_response(&resp.usage);

                Ok(InferenceResponse {
                    success: true,
                    content: None,
                    embeddings: Some(embeddings),
                    transcription: None,
                    audio: None,
                    images: None,
                    videos: None,
                    model: Some(model),
                    usage,
                    tool_calls: Vec::new(),
                    estimated_cost: None,
                    actual_cost: None,
                    attempts: vec![],
                })
            }
            _ => Err(GatewayError::ProviderError {
                adapter: "ollama".into(),
                message: "Ollama only supports chat and embed payloads".into(),
                status: None,
            }),
        }
    }

    async fn stream(
        &self,
        config: &RouterConfig,
        request: &InferenceRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        let Payload::Chat {
            messages,
            system,
            max_tokens,
            temperature,
            tools: _,
        } = &request.payload
        else {
            return Err(GatewayError::ProviderError {
                adapter: "ollama".into(),
                message: "streaming is only supported for chat payloads".into(),
                status: None,
            });
        };

        let api_key = resolve_api_key(config);
        let model = resolve_model(request);

        let body = ChatCompletionRequest {
            model,
            messages: build_chat_messages(messages, system),
            max_tokens: *max_tokens,
            temperature: *temperature,
            stream: true,
        };

        let url = format!(
            "{}/v1/chat/completions",
            config.url.trim_end_matches('/')
        );
        let mut req = self.client.post(&url).json(&body);

        if let Some(key) = &api_key {
            req = req.bearer_auth(key);
        }
        for (k, v) in &config.headers {
            req = req.header(k.as_str(), v.as_str());
        }

        let response = req.send().await?;
        let status = response.status();

        if !status.is_success() {
            let body_text = response.text().await.unwrap_or_default();
            return Err(GatewayError::ProviderError {
                adapter: "ollama".into(),
                message: body_text,
                status: Some(status.as_u16()),
            });
        }

        let byte_stream = response.bytes_stream();

        let stream = byte_stream
            .map(|result| -> Result<Vec<StreamChunk>, GatewayError> {
                let bytes = result?;
                let text = String::from_utf8_lossy(&bytes);
                let mut chunks = Vec::new();

                for line in text.lines() {
                    let line = line.trim();
                    if line.is_empty() || line == "data: [DONE]" {
                        continue;
                    }
                    let json_str = line.strip_prefix("data: ").unwrap_or(line);
                    if let Ok(parsed) = serde_json::from_str::<StreamChatResponse>(json_str)
                        && let Some(choice) = parsed.choices.first()
                    {
                        let content =
                            choice.delta.content.clone().unwrap_or_default();
                        let usage = usage_from_response(&parsed.usage);
                        chunks.push(StreamChunk {
                            content,
                            finish_reason: choice.finish_reason.clone(),
                            usage,
                            tool_calls: Vec::new(),
                        });
                    }
                }

                Ok(chunks)
            })
            .map(|result| -> futures::stream::Iter<std::vec::IntoIter<Result<StreamChunk, GatewayError>>> {
                match result {
                    Ok(chunks) => {
                        futures::stream::iter(chunks.into_iter().map(Ok).collect::<Vec<_>>())
                    }
                    Err(e) => futures::stream::iter(vec![Err(e)]),
                }
            })
            .flatten();

        Ok(Box::pin(stream))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ollama_id_and_supports() {
        let adapter = OllamaAdapter::new().unwrap();
        assert_eq!(adapter.id(), "ollama");

        assert!(adapter.supports(&Capability::TextChat));
        assert!(adapter.supports(&Capability::TextComplete));
        assert!(adapter.supports(&Capability::TextEmbed));

        assert!(!adapter.supports(&Capability::AudioTranscribe));
        assert!(!adapter.supports(&Capability::AudioGenerate));
        assert!(!adapter.supports(&Capability::ImageGenerate));
        assert!(!adapter.supports(&Capability::VideoGenerate));
    }

    #[test]
    fn build_chat_request_body() {
        let messages = vec![
            Message::text(MessageRole::User, "Hello".to_string()),
        ];
        let system = Some("You are helpful.".to_string());
        let chat_messages = build_chat_messages(&messages, &system);

        let body = ChatCompletionRequest {
            model: "gemma3:27b".to_string(),
            messages: chat_messages,
            max_tokens: Some(1024),
            temperature: Some(0.7),
            stream: false,
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["model"], "gemma3:27b");
        assert_eq!(json["stream"], false);
        assert_eq!(json["max_tokens"], 1024);

        let msgs = json["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0]["role"], "system");
        assert_eq!(msgs[0]["content"], "You are helpful.");
        assert_eq!(msgs[1]["role"], "user");
        assert_eq!(msgs[1]["content"], "Hello");
    }

    #[test]
    fn parse_chat_response() {
        let json = r#"{
            "choices": [{
                "message": {"content": "Hello!"},
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15
            }
        }"#;

        let resp: ChatCompletionResponse = serde_json::from_str(json).unwrap();

        assert_eq!(resp.choices.len(), 1);
        assert_eq!(
            resp.choices[0].message.content.as_deref(),
            Some("Hello!"),
        );
        assert_eq!(resp.choices[0].finish_reason.as_deref(), Some("stop"));

        let usage = resp.usage.unwrap();
        assert_eq!(usage.prompt_tokens, Some(10));
        assert_eq!(usage.completion_tokens, Some(5));
        assert_eq!(usage.total_tokens, Some(15));
    }

    #[test]
    fn parse_embed_response() {
        let json = r#"{
            "data": [
                {"embedding": [0.1, 0.2, 0.3], "index": 0}
            ],
            "usage": {
                "prompt_tokens": 5,
                "total_tokens": 5
            }
        }"#;

        let resp: EmbedResponse = serde_json::from_str(json).unwrap();

        assert_eq!(resp.data.len(), 1);
        assert_eq!(resp.data[0].embedding, vec![0.1, 0.2, 0.3]);

        let usage = resp.usage.unwrap();
        assert_eq!(usage.prompt_tokens, Some(5));
        assert_eq!(usage.total_tokens, Some(5));
        // completion_tokens absent in embed responses
        assert_eq!(usage.completion_tokens, None);
    }

    #[test]
    fn parse_stream_chunk() {
        let json = r#"{"choices":[{"delta":{"content":"Hi"},"finish_reason":null}]}"#;

        let resp: StreamChatResponse = serde_json::from_str(json).unwrap();

        assert_eq!(resp.choices.len(), 1);
        assert_eq!(resp.choices[0].delta.content.as_deref(), Some("Hi"));
        assert!(resp.choices[0].finish_reason.is_none());
    }

    #[tokio::test]
    #[ignore]
    async fn ollama_chat_integration() {
        let adapter = OllamaAdapter::new().unwrap();
        let config = RouterConfig {
            url: "http://localhost:11434".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: Some(60000),
            headers: std::collections::HashMap::new(),
        };
        let request = InferenceRequest {
            capability: Capability::TextChat,
            model: Some("llama3.2:latest".to_string()),
            router: None,
            chain: None,
            payload: Payload::Chat {
                messages: vec![Message::text(MessageRole::User, "Say hello in one sentence.".to_string())],
                system: None,
                max_tokens: Some(64),
                temperature: Some(0.3),
                tools: Vec::new(),
            },
            budget: None,
        };

        let response = adapter.execute(&config, &request).await.unwrap();
        assert!(response.success);
        assert!(response.content.is_some());
        assert!(!response.content.unwrap().is_empty());
    }

    #[tokio::test]
    async fn execute_times_out_against_a_silent_server() {
        // A server that accepts the connection but never sends a response. A
        // no-timeout client (the old Client::new()) would hang here forever and
        // wedge the worker; with a per-request timeout the call must return an
        // error promptly instead. Uses std::net so no tokio "net" feature is
        // required.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            let mut held = Vec::new();
            // Hold each accepted connection open, never write a response.
            for s in listener.incoming().flatten() {
                held.push(s);
            }
        });

        let config = RouterConfig {
            url: format!("http://{}", addr),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: Some(300),
            headers: std::collections::HashMap::new(),
        };
        let adapter = OllamaAdapter::from_config(&config).unwrap();
        let request = InferenceRequest {
            capability: Capability::TextEmbed,
            model: Some("all-minilm".to_string()),
            router: None,
            chain: None,
            payload: Payload::Embed { texts: vec!["hello".to_string()] },
            budget: None,
        };

        let start = std::time::Instant::now();
        let result = adapter.execute(&config, &request).await;
        let elapsed = start.elapsed();
        assert!(result.is_err(), "a silent server must produce an error, not hang");
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "request must time out promptly, took {elapsed:?}"
        );
    }
}
