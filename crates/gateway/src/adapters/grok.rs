use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use futures::stream::StreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::base::{build_client, http_json, resolve_api_key};
use crate::types::config::RouterConfig;
use crate::types::cost::TokenUsage;
use crate::types::error::GatewayError;
use crate::types::io::{ChatRequest, ChatResponse, SttRequest, SttResponse, TtsResponse};
use crate::types::request::{Message, MessageRole, StreamChunk};

// ---------------------------------------------------------------------------
// Wire types — Grok chat request/response structs (OpenAI-compatible)
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

// Voice wire types — Whisper-compatible STT + TTS

#[derive(Debug, Deserialize)]
struct WhisperResponse {
    text: String,
}

#[derive(Debug, Serialize)]
struct TtsRequest {
    model: String,
    input: String,
    voice: String,
    response_format: String,
}

// Streaming wire types

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

const DEFAULT_CHAT_MODEL: &str = "grok-4-fast";
const DEFAULT_AUDIO_MODEL: &str = "grok-2-audio";
const DEFAULT_VOICE: &str = "Ara";

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
        out.push(ChatMessage {
            role: "system".to_string(),
            content: sys.clone(),
        });
    }
    for m in messages {
        out.push(ChatMessage {
            role: role_to_string(&m.role).to_string(),
            content: m.as_text().to_string(),
        });
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

/// Require an API key, returning an Authentication error when missing.
fn require_api_key(config: &RouterConfig) -> Result<String, GatewayError> {
    resolve_api_key(config).ok_or_else(|| GatewayError::Authentication {
        adapter: "grok".into(),
        message: "missing API key — set the env var specified in api_key_env".into(),
    })
}

// ---------------------------------------------------------------------------
// GrokAdapter
// ---------------------------------------------------------------------------

/// Adapter for the xAI Grok API.
///
/// Uses Bearer-token authentication (`Authorization: Bearer {key}`) and targets
/// `https://api.x.ai/v1`. The chat endpoint is OpenAI-compatible; STT and TTS
/// endpoints mirror the OpenAI Whisper / TTS format.
pub struct GrokAdapter {
    client: Client,
}

impl GrokAdapter {
    pub fn new() -> Result<Self, GatewayError> {
        Ok(Self {
            client: Client::new(),
        })
    }

    /// Create an adapter from a pre-built client (e.g. with timeout from config).
    pub fn from_config(config: &RouterConfig) -> Result<Self, GatewayError> {
        Ok(Self {
            client: build_client(config)?,
        })
    }
}

// ---------------------------------------------------------------------------
// Capability traits (target model). Traits + RegisterInto referenced by full path. The io
// `TtsRequest` is full-pathed in `speak` too — it collides with the local Grok
// TTS wire struct of the same name, so the unqualified `TtsRequest` in the body
// keeps resolving to the wire struct.
// ---------------------------------------------------------------------------

impl crate::adapters::capability::Model for GrokAdapter {
    fn id(&self) -> &str {
        "grok"
    }
}

#[async_trait]
impl crate::adapters::capability::ChatModel for GrokAdapter {
    async fn chat(
        &self,
        config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        let api_key = require_api_key(config)?;
        let model = req
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_CHAT_MODEL.to_string());

        let body = ChatCompletionRequest {
            model: model.clone(),
            messages: build_chat_messages(&req.messages, &req.system),
            max_tokens: req.max_tokens,
            temperature: req.temperature,
            stream: false,
        };

        let resp: ChatCompletionResponse = http_json(
            &self.client,
            &config.url,
            "/v1/chat/completions",
            &body,
            Some(&api_key),
            &config.headers,
        )
        .await?;

        let content = resp.choices.first().and_then(|c| c.message.content.clone());
        let usage = usage_from_response(&resp.usage);

        Ok(ChatResponse {
            content,
            tool_calls: Vec::new(),
            usage,
            model: Some(model),
            degraded: false,
        })
    }

    async fn chat_stream(
        &self,
        config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        let api_key = require_api_key(config)?;
        let model = req
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_CHAT_MODEL.to_string());

        let body = ChatCompletionRequest {
            model,
            messages: build_chat_messages(&req.messages, &req.system),
            max_tokens: req.max_tokens,
            temperature: req.temperature,
            stream: true,
        };

        let url = format!("{}/v1/chat/completions", config.url.trim_end_matches('/'));
        let mut request = self.client.post(&url).json(&body).bearer_auth(&api_key);

        for (k, v) in &config.headers {
            request = request.header(k.as_str(), v.as_str());
        }

        let response = request.send().await?;
        let status = response.status();

        if !status.is_success() {
            let body_text = response.text().await.unwrap_or_default();
            return Err(match status.as_u16() {
                401 | 403 => GatewayError::Authentication {
                    adapter: "grok".into(),
                    message: body_text,
                },
                429 => GatewayError::RateLimit {
                    adapter: "grok".into(),
                    retry_after_ms: None,
                },
                _ => GatewayError::ProviderError {
                    adapter: "grok".into(),
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
            .map(
                |result| -> futures::stream::Iter<
                    std::vec::IntoIter<Result<StreamChunk, GatewayError>>,
                > {
                    match result {
                        Ok(chunks) => {
                            futures::stream::iter(chunks.into_iter().map(Ok).collect::<Vec<_>>())
                        }
                        Err(e) => futures::stream::iter(vec![Err(e)]),
                    }
                },
            )
            .flatten();

        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl crate::adapters::capability::SttModel for GrokAdapter {
    async fn transcribe(
        &self,
        config: &RouterConfig,
        req: &SttRequest,
    ) -> Result<SttResponse, GatewayError> {
        let api_key = require_api_key(config)?;
        let model = req
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_AUDIO_MODEL.to_string());

        let mime = match req.format.as_str() {
            "mp3" => "audio/mpeg",
            "wav" => "audio/wav",
            "webm" => "audio/webm",
            "m4a" => "audio/mp4",
            other => {
                return Err(GatewayError::ProviderError {
                    adapter: "grok".into(),
                    message: format!("unsupported audio format: {other}"),
                    status: None,
                });
            }
        };

        let file_part = reqwest::multipart::Part::bytes(req.audio.clone())
            .file_name(format!("audio.{}", req.format))
            .mime_str(mime)
            .map_err(|e| GatewayError::ProviderError {
                adapter: "grok".into(),
                message: format!("failed to build multipart: {e}"),
                status: None,
            })?;

        let mut form = reqwest::multipart::Form::new()
            .part("file", file_part)
            .text("model", model.clone());

        if let Some(lang) = &req.language {
            form = form.text("language", lang.clone());
        }

        let url = format!(
            "{}/v1/audio/transcriptions",
            config.url.trim_end_matches('/')
        );
        let mut request = self.client.post(&url).multipart(form).bearer_auth(&api_key);

        for (k, v) in &config.headers {
            request = request.header(k.as_str(), v.as_str());
        }

        let response = request.send().await?;
        let status = response.status();

        if !status.is_success() {
            let body_text = response.text().await.unwrap_or_default();
            return Err(match status.as_u16() {
                401 | 403 => GatewayError::Authentication {
                    adapter: "grok".into(),
                    message: body_text,
                },
                429 => GatewayError::RateLimit {
                    adapter: "grok".into(),
                    retry_after_ms: None,
                },
                _ => GatewayError::ProviderError {
                    adapter: "grok".into(),
                    message: body_text,
                    status: Some(status.as_u16()),
                },
            });
        }

        let whisper_resp: WhisperResponse =
            response
                .json()
                .await
                .map_err(|e| GatewayError::ProviderError {
                    adapter: "grok".into(),
                    message: format!("failed to parse whisper response: {e}"),
                    status: Some(status.as_u16()),
                })?;

        Ok(SttResponse {
            transcription: whisper_resp.text,
            usage: None,
            degraded: false,
        })
    }
}

#[async_trait]
impl crate::adapters::capability::TtsModel for GrokAdapter {
    async fn speak(
        &self,
        config: &RouterConfig,
        req: &crate::types::io::TtsRequest,
    ) -> Result<TtsResponse, GatewayError> {
        let api_key = require_api_key(config)?;
        let model = req
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_AUDIO_MODEL.to_string());

        let body = TtsRequest {
            model: model.clone(),
            input: req.text.clone(),
            voice: req
                .voice
                .clone()
                .unwrap_or_else(|| DEFAULT_VOICE.to_string()),
            response_format: req.output_format.to_string(),
        };

        let url = format!("{}/v1/audio/speech", config.url.trim_end_matches('/'));
        let mut request = self.client.post(&url).json(&body).bearer_auth(&api_key);

        for (k, v) in &config.headers {
            request = request.header(k.as_str(), v.as_str());
        }

        let response = request.send().await?;
        let status = response.status();

        if !status.is_success() {
            let body_text = response.text().await.unwrap_or_default();
            return Err(match status.as_u16() {
                401 | 403 => GatewayError::Authentication {
                    adapter: "grok".into(),
                    message: body_text,
                },
                429 => GatewayError::RateLimit {
                    adapter: "grok".into(),
                    retry_after_ms: None,
                },
                _ => GatewayError::ProviderError {
                    adapter: "grok".into(),
                    message: body_text,
                    status: Some(status.as_u16()),
                },
            });
        }

        let audio_bytes = response
            .bytes()
            .await
            .map_err(|e| GatewayError::ProviderError {
                adapter: "grok".into(),
                message: format!("failed to read TTS audio bytes: {e}"),
                status: None,
            })?;

        Ok(TtsResponse {
            audio: audio_bytes.to_vec(),
            degraded: false,
        })
    }
}

#[async_trait]
impl crate::adapters::RegisterInto for GrokAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &crate::adapters::AdapterRegistry) {
        reg.register_chat(self.clone()).await;
        reg.register_stt(self.clone()).await;
        reg.register_tts(self).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grok_id_and_supports() {
        let adapter = GrokAdapter::new().unwrap();
        assert_eq!(crate::adapters::capability::Model::id(&adapter), "grok");

        // Supported capabilities

        // NOT supported
    }

    #[test]
    fn build_chat_request() {
        let messages = vec![Message::text(MessageRole::User, "Hello".to_string())];

        let chat_messages = build_chat_messages(&messages, &None);

        let body = ChatCompletionRequest {
            model: "grok-4-fast".to_string(),
            messages: chat_messages,
            max_tokens: Some(1024),
            temperature: Some(0.7),
            stream: false,
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["model"], "grok-4-fast");
        assert_eq!(json["stream"], false);
        assert_eq!(json["max_tokens"], 1024);
        assert!(
            json["temperature"].as_f64().unwrap() > 0.69
                && json["temperature"].as_f64().unwrap() < 0.71
        );

        let msgs = json["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["role"], "user");
        assert_eq!(msgs[0]["content"], "Hello");
    }

    #[test]
    fn build_chat_request_with_system() {
        let messages = vec![Message::text(MessageRole::User, "Hi".to_string())];
        let system = Some("You are a helpful assistant.".to_string());

        let chat_messages = build_chat_messages(&messages, &system);

        let body = ChatCompletionRequest {
            model: "grok-4-fast".to_string(),
            messages: chat_messages,
            max_tokens: None,
            temperature: None,
            stream: false,
        };

        let json = serde_json::to_value(&body).unwrap();

        let msgs = json["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0]["role"], "system");
        assert_eq!(msgs[0]["content"], "You are a helpful assistant.");
        assert_eq!(msgs[1]["role"], "user");
        assert_eq!(msgs[1]["content"], "Hi");

        // Optional fields should be absent when None
        assert!(json.get("max_tokens").is_none());
        assert!(json.get("temperature").is_none());
    }

    #[test]
    fn parse_chat_response() {
        let json = r#"{
            "choices": [{
                "message": {"content": "Greetings, human!"},
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
            Some("Greetings, human!"),
        );
        assert_eq!(resp.choices[0].finish_reason.as_deref(), Some("stop"));

        let usage = usage_from_response(&resp.usage).unwrap();
        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.output_tokens, 5);
        assert_eq!(usage.total_tokens, 15);
    }

    #[test]
    fn parse_stream_chunk() {
        let json = r#"{"choices":[{"delta":{"content":"world"},"finish_reason":null}]}"#;

        let resp: StreamChatResponse = serde_json::from_str(json).unwrap();

        assert_eq!(resp.choices.len(), 1);
        assert_eq!(resp.choices[0].delta.content.as_deref(), Some("world"));
        assert!(resp.choices[0].finish_reason.is_none());
    }

    #[test]
    fn build_tts_request() {
        let body = TtsRequest {
            model: DEFAULT_AUDIO_MODEL.to_string(),
            input: "Hello world".to_string(),
            voice: DEFAULT_VOICE.to_string(),
            response_format: "mp3".to_string(),
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["model"], "grok-2-audio");
        assert_eq!(json["input"], "Hello world");
        assert_eq!(json["voice"], "Ara");
        assert_eq!(json["response_format"], "mp3");
    }

    #[test]
    fn parse_whisper_response() {
        let json = r#"{"text":"hello"}"#;
        let resp: WhisperResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.text, "hello");
    }

    #[tokio::test]
    #[ignore]
    async fn grok_chat_integration() {
        use crate::adapters::capability::ChatModel;
        // Requires XAI_API_KEY env var
        let adapter = GrokAdapter::new().unwrap();
        let config = RouterConfig {
            url: "https://api.x.ai".to_string(),
            api_key_env: Some("XAI_API_KEY".to_string()),
            api_key: None,
            enabled: true,
            timeout_ms: Some(30000),
            headers: std::collections::HashMap::new(),
        };
        let req = crate::types::io::ChatRequest {
            model: Some("grok-4-fast".to_string()),
            messages: vec![Message::text(
                MessageRole::User,
                "Say hello in one sentence.".to_string(),
            )],
            system: None,
            max_tokens: Some(64),
            temperature: Some(0.3),
            tools: Vec::new(),
        };

        let response = adapter.chat(&config, &req).await.unwrap();
        assert!(response.content.is_some());
        assert!(!response.content.unwrap().is_empty());
        assert!(response.usage.is_some());
    }

    #[tokio::test]
    async fn grok_chatmodel_id_and_missing_key() {
        use crate::adapters::capability::{ChatModel, Model};

        let adapter = GrokAdapter::new().unwrap();
        assert_eq!(Model::id(&adapter), "grok");

        let config = RouterConfig {
            url: "https://api.x.ai".to_string(),
            api_key_env: Some("__NONEXISTENT_XAI_KEY_FOR_TEST__".to_string()),
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: std::collections::HashMap::new(),
        };
        let req = crate::types::io::ChatRequest {
            model: Some("grok-4-fast".to_string()),
            messages: vec![Message::text(MessageRole::User, "Hello".to_string())],
            system: None,
            max_tokens: Some(64),
            temperature: None,
            tools: Vec::new(),
        };

        // The typed chat path resolves the API key before any network call, so a
        // missing key yields an Authentication error without hitting the wire.
        let result = adapter.chat(&config, &req).await;
        assert!(
            matches!(result, Err(GatewayError::Authentication { .. })),
            "expected Authentication error from typed chat path, got: {result:?}",
        );
    }
}
