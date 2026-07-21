use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::base::{build_client, error_from_response, resolve_api_key};
use crate::openai_compat;
use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;
use kernel::types::io::{ChatRequest, ChatResponse, SttRequest, SttResponse, TtsResponse};
use kernel::types::request::StreamChunk;

// ---------------------------------------------------------------------------
// Wire types — Grok audio (STT + TTS) request/response structs.
//
// The chat / streaming wire types + helpers live in the shared
// `openai_compat` module (this adapter delegates its `ChatModel` methods
// there). Only Grok's non-OpenAI-compat surfaces (Whisper-style STT, TTS)
// keep their wire types here.
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const DEFAULT_CHAT_MODEL: &str = "grok-4-fast";
const DEFAULT_AUDIO_MODEL: &str = "grok-2-audio";
const DEFAULT_VOICE: &str = "Ara";

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

impl kernel::adapters::capability::Model for GrokAdapter {
    fn id(&self) -> &str {
        "grok"
    }
}

#[async_trait]
impl kernel::adapters::capability::ChatModel for GrokAdapter {
    async fn chat(
        &self,
        config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        // Grok requires a bearer key: a missing one short-circuits to an
        // Authentication error before any request is built. The shared
        // core treats the key as optional (local providers need none), so
        // the contract is enforced here.
        require_api_key(config)?;
        openai_compat::chat(&self.client, &config.url, DEFAULT_CHAT_MODEL, config, req).await
    }

    async fn chat_stream(
        &self,
        config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        require_api_key(config)?;
        openai_compat::chat_stream(&self.client, &config.url, DEFAULT_CHAT_MODEL, config, req).await
    }
}

#[async_trait]
impl kernel::adapters::capability::SttModel for GrokAdapter {
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
            return Err(error_from_response("grok", response).await);
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
impl kernel::adapters::capability::TtsModel for GrokAdapter {
    async fn speak(
        &self,
        config: &RouterConfig,
        req: &kernel::types::io::TtsRequest,
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
            return Err(error_from_response("grok", response).await);
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
impl kernel::adapters::RegisterInto for GrokAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &kernel::adapters::AdapterRegistry) {
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
    use kernel::types::request::{Message, MessageRole};

    #[test]
    fn grok_id_and_supports() {
        let adapter = GrokAdapter::new().unwrap();
        assert_eq!(kernel::adapters::capability::Model::id(&adapter), "grok");

        // Supported capabilities

        // NOT supported
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
        use kernel::adapters::capability::ChatModel;
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
        let req = kernel::types::io::ChatRequest {
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
        use kernel::adapters::capability::{ChatModel, Model};

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
        let req = kernel::types::io::ChatRequest {
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
