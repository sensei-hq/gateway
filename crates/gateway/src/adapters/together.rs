use std::pin::Pin;

use async_trait::async_trait;
use futures::stream::StreamExt;
use futures::Stream;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::base::{build_client, resolve_api_key};
use super::InferenceAdapter;
use crate::types::capability::Capability;
use crate::types::config::RouterConfig;
use crate::types::cost::TokenUsage;
use crate::types::error::GatewayError;
use crate::types::request::{
    ImageResult, InferenceRequest, InferenceResponse, Message, MessageRole, Payload, StreamChunk,
};

// ---------------------------------------------------------------------------
// Wire types — chat (OpenAI-compatible, defined independently)
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

// Wire types — streaming

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

// Wire types — image generation (OpenAI-compatible)

#[derive(Debug, Serialize)]
struct ImageGenerateRequest {
    model: String,
    prompt: String,
    n: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ImageGenerateResponse {
    data: Vec<ImageData>,
}

#[derive(Debug, Deserialize)]
struct ImageData {
    url: Option<String>,
    b64_json: Option<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const BASE_URL: &str = "https://api.together.xyz/v1";
const DEFAULT_CHAT_MODEL: &str = "meta-llama/Llama-3.3-70B-Instruct-Turbo";
const DEFAULT_IMAGE_MODEL: &str = "black-forest-labs/FLUX.1-schnell-Free";

fn require_api_key(config: &RouterConfig) -> Result<String, GatewayError> {
    resolve_api_key(config).ok_or_else(|| GatewayError::Authentication {
        adapter: "together".into(),
        message: "missing API key — set the env var specified in api_key_env".into(),
    })
}

fn base_url(config: &RouterConfig) -> &str {
    let url = config.url.trim_end_matches('/');
    if url.is_empty() {
        BASE_URL
    } else {
        url
    }
}

fn role_to_string(role: &MessageRole) -> &'static str {
    match role {
        MessageRole::System => "system",
        MessageRole::User => "user",
        MessageRole::Assistant => "assistant",
        MessageRole::Tool => "tool",
    }
}

fn build_chat_messages(messages: &[Message], system: &Option<String>) -> Vec<ChatMessage> {
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
// TogetherAdapter
// ---------------------------------------------------------------------------

/// Adapter for Together AI — supports both TextChat and ImageGenerate.
///
/// Uses Bearer-token authentication. OpenAI-compatible API format.
pub struct TogetherAdapter {
    client: Client,
}

impl TogetherAdapter {
    pub fn new() -> Result<Self, GatewayError> {
        Ok(Self {
            client: Client::new(),
        })
    }

    pub fn from_config(config: &RouterConfig) -> Result<Self, GatewayError> {
        Ok(Self {
            client: build_client(config)?,
        })
    }
}

#[async_trait]
impl InferenceAdapter for TogetherAdapter {
    fn id(&self) -> &str {
        "together"
    }

    fn supports(&self, capability: &Capability) -> bool {
        matches!(
            capability,
            Capability::TextChat | Capability::ImageGenerate
        )
    }

    async fn execute(
        &self,
        config: &RouterConfig,
        request: &InferenceRequest,
    ) -> Result<InferenceResponse, GatewayError> {
        let api_key = require_api_key(config)?;
        let url_base = base_url(config);

        match &request.payload {
            Payload::Chat {
                messages,
                system,
                max_tokens,
                temperature,
                tools: _,
            } => {
                let model = request
                    .model
                    .clone()
                    .unwrap_or_else(|| DEFAULT_CHAT_MODEL.to_string());

                let body = ChatCompletionRequest {
                    model: model.clone(),
                    messages: build_chat_messages(messages, system),
                    max_tokens: *max_tokens,
                    temperature: *temperature,
                    stream: false,
                };

                let url = format!("{url_base}/chat/completions");
                let mut req = self
                    .client
                    .post(&url)
                    .json(&body)
                    .bearer_auth(&api_key);

                for (k, v) in &config.headers {
                    req = req.header(k.as_str(), v.as_str());
                }

                let response = req.send().await?;
                let status = response.status();

                if !status.is_success() {
                    let body_text = response.text().await.unwrap_or_default();
                    return Err(match status.as_u16() {
                        401 | 403 => GatewayError::Authentication {
                            adapter: "together".into(),
                            message: body_text,
                        },
                        429 => GatewayError::RateLimit {
                            adapter: "together".into(),
                            retry_after_ms: None,
                        },
                        _ => GatewayError::ProviderError {
                            adapter: "together".into(),
                            message: body_text,
                            status: Some(status.as_u16()),
                        },
                    });
                }

                let resp: ChatCompletionResponse =
                    response.json().await.map_err(|e| GatewayError::ProviderError {
                        adapter: "together".into(),
                        message: format!("failed to parse chat response: {e}"),
                        status: Some(status.as_u16()),
                    })?;

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
            Payload::ImageGenerate {
                prompt,
                size,
                n,
                ..
            } => {
                let model = request
                    .model
                    .clone()
                    .unwrap_or_else(|| DEFAULT_IMAGE_MODEL.to_string());

                let body = ImageGenerateRequest {
                    model: model.clone(),
                    prompt: prompt.clone(),
                    n: *n,
                    size: size.clone().or_else(|| Some("1024x1024".to_string())),
                };

                let url = format!("{url_base}/images/generations");
                let mut req = self
                    .client
                    .post(&url)
                    .json(&body)
                    .bearer_auth(&api_key);

                for (k, v) in &config.headers {
                    req = req.header(k.as_str(), v.as_str());
                }

                let response = req.send().await?;
                let status = response.status();

                if !status.is_success() {
                    let body_text = response.text().await.unwrap_or_default();
                    return Err(match status.as_u16() {
                        401 | 403 => GatewayError::Authentication {
                            adapter: "together".into(),
                            message: body_text,
                        },
                        429 => GatewayError::RateLimit {
                            adapter: "together".into(),
                            retry_after_ms: None,
                        },
                        _ => GatewayError::ProviderError {
                            adapter: "together".into(),
                            message: body_text,
                            status: Some(status.as_u16()),
                        },
                    });
                }

                let image_resp: ImageGenerateResponse =
                    response.json().await.map_err(|e| GatewayError::ProviderError {
                        adapter: "together".into(),
                        message: format!("failed to parse image response: {e}"),
                        status: Some(status.as_u16()),
                    })?;

                let images: Vec<ImageResult> = image_resp
                    .data
                    .into_iter()
                    .map(|d| ImageResult {
                        url: d.url,
                        b64_json: d.b64_json,
                        revised_prompt: None,
                    })
                    .collect();

                Ok(InferenceResponse {
                    success: true,
                    content: None,
                    embeddings: None,
                    transcription: None,
                    audio: None,
                    images: Some(images),
                    videos: None,
                    model: Some(model),
                    usage: None,
                    tool_calls: Vec::new(),
                    estimated_cost: None,
                    actual_cost: None,
                    attempts: vec![],
                })
            }
            _ => Err(GatewayError::ProviderError {
                adapter: "together".into(),
                message: "only Chat and ImageGenerate payloads are supported".into(),
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
                adapter: "together".into(),
                message: "streaming is only supported for chat payloads".into(),
                status: None,
            });
        };

        let api_key = require_api_key(config)?;
        let url_base = base_url(config);
        let model = request
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_CHAT_MODEL.to_string());

        let body = ChatCompletionRequest {
            model,
            messages: build_chat_messages(messages, system),
            max_tokens: *max_tokens,
            temperature: *temperature,
            stream: true,
        };

        let url = format!("{url_base}/chat/completions");
        let mut req = self.client.post(&url).json(&body).bearer_auth(&api_key);

        for (k, v) in &config.headers {
            req = req.header(k.as_str(), v.as_str());
        }

        let response = req.send().await?;
        let status = response.status();

        if !status.is_success() {
            let body_text = response.text().await.unwrap_or_default();
            return Err(match status.as_u16() {
                401 | 403 => GatewayError::Authentication {
                    adapter: "together".into(),
                    message: body_text,
                },
                429 => GatewayError::RateLimit {
                    adapter: "together".into(),
                    retry_after_ms: None,
                },
                _ => GatewayError::ProviderError {
                    adapter: "together".into(),
                    message: body_text,
                    status: Some(status.as_u16()),
                },
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
                        let content = choice.delta.content.clone().unwrap_or_default();
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
    fn together_id_and_supports() {
        let adapter = TogetherAdapter::new().unwrap();
        assert_eq!(adapter.id(), "together");

        assert!(adapter.supports(&Capability::TextChat));
        assert!(adapter.supports(&Capability::ImageGenerate));
        assert!(!adapter.supports(&Capability::TextEmbed));
        assert!(!adapter.supports(&Capability::VideoGenerate));
    }

    #[test]
    fn build_image_request() {
        let body = ImageGenerateRequest {
            model: DEFAULT_IMAGE_MODEL.to_string(),
            prompt: "A sunset over mountains".to_string(),
            n: 1,
            size: Some("1024x1024".to_string()),
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["model"], DEFAULT_IMAGE_MODEL);
        assert_eq!(json["prompt"], "A sunset over mountains");
        assert_eq!(json["n"], 1);
        assert_eq!(json["size"], "1024x1024");
    }

    #[test]
    fn parse_image_response() {
        let json = r#"{
            "data": [
                {"url": "https://together.ai/output/image1.png"}
            ]
        }"#;

        let resp: ImageGenerateResponse = serde_json::from_str(json).unwrap();

        assert_eq!(resp.data.len(), 1);
        assert_eq!(
            resp.data[0].url.as_deref(),
            Some("https://together.ai/output/image1.png"),
        );
    }

    #[test]
    fn build_chat_request() {
        let messages = vec![Message::text(MessageRole::User, "Hello".to_string())];
        let system = Some("You are helpful.".to_string());
        let chat_messages = build_chat_messages(&messages, &system);

        let body = ChatCompletionRequest {
            model: DEFAULT_CHAT_MODEL.to_string(),
            messages: chat_messages,
            max_tokens: Some(1024),
            temperature: Some(0.7),
            stream: false,
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["model"], DEFAULT_CHAT_MODEL);
        assert_eq!(json["stream"], false);
        let msgs = json["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0]["role"], "system");
        assert_eq!(msgs[0]["content"], "You are helpful.");
        assert_eq!(msgs[1]["role"], "user");
        assert_eq!(msgs[1]["content"], "Hello");
    }

    #[tokio::test]
    async fn missing_api_key_returns_auth_error() {
        let adapter = TogetherAdapter::new().unwrap();
        let config = RouterConfig {
            url: "https://api.together.xyz/v1".to_string(),
            api_key_env: Some("__NONEXISTENT_TOGETHER_KEY_FOR_TEST__".to_string()),
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: std::collections::HashMap::new(),
        };
        let request = InferenceRequest {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: None,
            payload: Payload::Chat {
                messages: vec![Message::text(MessageRole::User, "Hello".to_string())],
                system: None,
                max_tokens: Some(64),
                temperature: None,
                tools: Vec::new(),
            },
            budget: None,
        };

        let result = adapter.execute(&config, &request).await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(
            matches!(err, GatewayError::Authentication { .. }),
            "expected Authentication error, got: {err:?}",
        );
    }
}
