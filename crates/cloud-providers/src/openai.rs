use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::base::{build_client, resolve_api_key};
use crate::openai_compat;
use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;
use kernel::types::io::{
    ChatRequest, ChatResponse, ImageRequest, ImageResponse, SttRequest, SttResponse, TtsResponse,
};
use kernel::types::request::{ImageResult, StreamChunk};

// ---------------------------------------------------------------------------
// Wire types — OpenAI image / audio (STT + TTS) request/response structs.
//
// The chat / embed / streaming wire types + helpers live in the shared
// `openai_compat` module (this adapter delegates its `ChatModel` /
// `EmbedModel` methods there). Only OpenAI's non-OpenAI-compat surfaces
// (Whisper, TTS, DALL·E) keep their wire types here.
// ---------------------------------------------------------------------------

// Voice wire types — Whisper (STT) + TTS

#[derive(Debug, Deserialize)]
struct WhisperResponse {
    text: String,
}

#[derive(Debug, Serialize)]
struct TtsRequest {
    model: String,
    input: String,
    voice: String,
    speed: f32,
    response_format: String,
}

// Image generation wire types

#[derive(Debug, Serialize)]
struct ImageGenerateRequest {
    model: String,
    prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    quality: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    style: Option<String>,
    n: u8,
}

#[derive(Debug, Deserialize)]
struct ImageGenerateResponse {
    data: Vec<ImageData>,
}

#[derive(Debug, Deserialize)]
struct ImageData {
    url: Option<String>,
    b64_json: Option<String>,
    revised_prompt: Option<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const DEFAULT_MODEL: &str = "gpt-4o-mini";

/// Require an API key, returning an Authentication error when missing.
fn require_api_key(config: &RouterConfig) -> Result<String, GatewayError> {
    resolve_api_key(config).ok_or_else(|| GatewayError::Authentication {
        adapter: "openai".into(),
        message: "missing API key — set the env var specified in api_key_env".into(),
    })
}

// ---------------------------------------------------------------------------
// OpenAIAdapter
// ---------------------------------------------------------------------------

/// Adapter for the official OpenAI API.
///
/// Uses Bearer-token authentication (`Authorization: Bearer {key}`) and targets
/// `POST /v1/chat/completions` and `POST /v1/embeddings`.
pub struct OpenAIAdapter {
    client: Client,
    /// Adapter id surfaced through [`Model::id`]. Defaults to
    /// `"openai"` via [`Self::new`] / [`Self::from_config`]; the
    /// [`Self::with_id`] / [`Self::from_config_with_id`] constructors let
    /// a single OpenAI-compatible implementation register under a
    /// different name (`"openrouter"`, `"vercel"`, `"nvidia"`, …) so the
    /// gateway engine — which looks adapters up by router id — can route
    /// to a per-router `RouterConfig` (custom URL, API key) while reusing
    /// the same wire format.
    id: String,
}

impl OpenAIAdapter {
    pub fn new() -> Result<Self, GatewayError> {
        Self::with_id("openai")
    }

    /// Build an OpenAI-compatible adapter registered under a custom id.
    /// The id should match the corresponding [`RouterConfig`]'s key in
    /// [`GatewayConfig::routers`], since the gateway engine dispatches by
    /// router id.
    pub fn with_id(id: impl Into<String>) -> Result<Self, GatewayError> {
        Ok(Self {
            client: Client::new(),
            id: id.into(),
        })
    }

    /// Create an adapter from a pre-built client (e.g. with timeout from config).
    pub fn from_config(config: &RouterConfig) -> Result<Self, GatewayError> {
        Self::from_config_with_id("openai", config)
    }

    /// Same as [`Self::from_config`] but with a caller-supplied adapter id.
    pub fn from_config_with_id(
        id: impl Into<String>,
        config: &RouterConfig,
    ) -> Result<Self, GatewayError> {
        Ok(Self {
            client: build_client(config)?,
            id: id.into(),
        })
    }
}

// ---------------------------------------------------------------------------
// Capability traits (target model). Traits + RegisterInto referenced by full path. The
// adapter id is a struct field, so Model::id derives from
// returning `&self.id` (openai / openrouter / vercel / …).
// ---------------------------------------------------------------------------

impl kernel::adapters::capability::Model for OpenAIAdapter {
    fn id(&self) -> &str {
        &self.id
    }
}

#[async_trait]
impl kernel::adapters::capability::ChatModel for OpenAIAdapter {
    async fn chat(
        &self,
        config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        // OpenAI requires a key: a missing one short-circuits to an
        // Authentication error before any request is built. The shared
        // core treats the key as optional (local providers like Ollama
        // need none), so the contract is enforced here.
        require_api_key(config)?;
        openai_compat::chat(&self.client, &config.url, DEFAULT_MODEL, config, req).await
    }

    async fn chat_stream(
        &self,
        config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        require_api_key(config)?;
        openai_compat::chat_stream(&self.client, &config.url, DEFAULT_MODEL, config, req).await
    }
}

#[async_trait]
impl kernel::adapters::capability::EmbedModel for OpenAIAdapter {
    async fn embed(
        &self,
        config: &RouterConfig,
        req: &kernel::types::io::EmbedRequest,
    ) -> Result<kernel::types::io::EmbedResponse, GatewayError> {
        require_api_key(config)?;
        openai_compat::embed(&self.client, &config.url, DEFAULT_MODEL, config, req).await
    }
}

#[async_trait]
impl kernel::adapters::capability::SttModel for OpenAIAdapter {
    async fn transcribe(
        &self,
        config: &RouterConfig,
        req: &SttRequest,
    ) -> Result<SttResponse, GatewayError> {
        let api_key = require_api_key(config)?;
        let model = req
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());

        let mime = match req.format.as_str() {
            "mp3" => "audio/mpeg",
            "wav" => "audio/wav",
            "webm" => "audio/webm",
            "m4a" => "audio/mp4",
            other => {
                return Err(GatewayError::ProviderError {
                    adapter: "openai".into(),
                    message: format!("unsupported audio format: {other}"),
                    status: None,
                });
            }
        };

        let file_part = reqwest::multipart::Part::bytes(req.audio.clone())
            .file_name(format!("audio.{}", req.format))
            .mime_str(mime)
            .map_err(|e| GatewayError::ProviderError {
                adapter: "openai".into(),
                message: format!("failed to build multipart: {e}"),
                status: None,
            })?;

        let mut form = reqwest::multipart::Form::new()
            .part("file", file_part)
            .text("model", model);

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
                    adapter: "openai".into(),
                    message: body_text,
                },
                429 => GatewayError::RateLimit {
                    adapter: "openai".into(),
                    retry_after_ms: None,
                },
                _ => GatewayError::ProviderError {
                    adapter: "openai".into(),
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
                    adapter: "openai".into(),
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
impl kernel::adapters::capability::TtsModel for OpenAIAdapter {
    async fn speak(
        &self,
        config: &RouterConfig,
        req: &kernel::types::io::TtsRequest,
    ) -> Result<TtsResponse, GatewayError> {
        let api_key = require_api_key(config)?;
        let model = req
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());

        let body = TtsRequest {
            model,
            input: req.text.clone(),
            voice: req.voice.clone().unwrap_or_else(|| "alloy".to_string()),
            speed: req.speed.unwrap_or(1.0),
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
                    adapter: "openai".into(),
                    message: body_text,
                },
                429 => GatewayError::RateLimit {
                    adapter: "openai".into(),
                    retry_after_ms: None,
                },
                _ => GatewayError::ProviderError {
                    adapter: "openai".into(),
                    message: body_text,
                    status: Some(status.as_u16()),
                },
            });
        }

        let audio_bytes = response
            .bytes()
            .await
            .map_err(|e| GatewayError::ProviderError {
                adapter: "openai".into(),
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
impl kernel::adapters::capability::ImageModel for OpenAIAdapter {
    async fn generate_image(
        &self,
        config: &RouterConfig,
        req: &ImageRequest,
    ) -> Result<ImageResponse, GatewayError> {
        let api_key = require_api_key(config)?;
        let image_model = req.model.clone().unwrap_or_else(|| "dall-e-3".to_string());

        let body = ImageGenerateRequest {
            model: image_model,
            prompt: req.prompt.clone(),
            size: req.size.clone(),
            quality: req.quality.clone(),
            style: req.style.clone(),
            n: req.n,
        };

        let url = format!("{}/v1/images/generations", config.url.trim_end_matches('/'));
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
                    adapter: "openai".into(),
                    message: body_text,
                },
                429 => GatewayError::RateLimit {
                    adapter: "openai".into(),
                    retry_after_ms: None,
                },
                _ => GatewayError::ProviderError {
                    adapter: "openai".into(),
                    message: body_text,
                    status: Some(status.as_u16()),
                },
            });
        }

        let image_resp: ImageGenerateResponse =
            response
                .json()
                .await
                .map_err(|e| GatewayError::ProviderError {
                    adapter: "openai".into(),
                    message: format!("failed to parse image generation response: {e}"),
                    status: Some(status.as_u16()),
                })?;

        let images: Vec<ImageResult> = image_resp
            .data
            .into_iter()
            .map(|d| ImageResult {
                url: d.url,
                b64_json: d.b64_json,
                revised_prompt: d.revised_prompt,
            })
            .collect();

        Ok(ImageResponse {
            images,
            degraded: false,
        })
    }
}

#[async_trait]
impl kernel::adapters::RegisterInto for OpenAIAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &kernel::adapters::AdapterRegistry) {
        reg.register_chat(self.clone()).await;
        reg.register_embed(self.clone()).await;
        reg.register_stt(self.clone()).await;
        reg.register_tts(self.clone()).await;
        reg.register_image(self).await;
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
    fn openai_id_and_supports() {
        let adapter = OpenAIAdapter::new().unwrap();
        assert_eq!(kernel::adapters::capability::Model::id(&adapter), "openai");
    }

    #[test]
    fn openai_supports_voice() {
        let _adapter = OpenAIAdapter::new().unwrap();
    }

    #[test]
    fn with_id_overrides_default_id() {
        let openrouter = OpenAIAdapter::with_id("openrouter").unwrap();
        assert_eq!(
            kernel::adapters::capability::Model::id(&openrouter),
            "openrouter"
        );

        let vercel = OpenAIAdapter::with_id("vercel").unwrap();
        assert_eq!(kernel::adapters::capability::Model::id(&vercel), "vercel");

        // Capability set is identical to the default-id adapter — the
        // wire format isn't changing, only which RouterConfig the
        // engine pairs it with.
    }

    #[test]
    fn new_and_from_config_default_to_openai_id() {
        let by_new = OpenAIAdapter::new().unwrap();
        assert_eq!(kernel::adapters::capability::Model::id(&by_new), "openai");

        let mut cfg_headers: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        cfg_headers.insert("X-Test".into(), "true".into());
        let cfg = kernel::types::config::RouterConfig {
            url: "https://example.com".into(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: Some(1000),
            headers: cfg_headers,
        };
        let from_cfg = OpenAIAdapter::from_config(&cfg).unwrap();
        assert_eq!(kernel::adapters::capability::Model::id(&from_cfg), "openai");

        let from_cfg_renamed = OpenAIAdapter::from_config_with_id("vercel", &cfg).unwrap();
        assert_eq!(
            kernel::adapters::capability::Model::id(&from_cfg_renamed),
            "vercel"
        );
    }

    #[test]
    fn parse_whisper_response() {
        let json = r#"{"text":"Hello world"}"#;
        let resp: WhisperResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.text, "Hello world");
    }

    #[test]
    fn build_tts_request() {
        let body = TtsRequest {
            model: "tts-1".to_string(),
            input: "Hello world".to_string(),
            voice: "alloy".to_string(),
            speed: 1.0,
            response_format: "mp3".to_string(),
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["model"], "tts-1");
        assert_eq!(json["input"], "Hello world");
        assert_eq!(json["voice"], "alloy");
        assert!((json["speed"].as_f64().unwrap() - 1.0).abs() < f64::EPSILON);
        assert_eq!(json["response_format"], "mp3");
    }

    #[test]
    fn build_image_generate_request() {
        let body = ImageGenerateRequest {
            model: "dall-e-3".to_string(),
            prompt: "A sunset over mountains".to_string(),
            size: Some("1792x1024".to_string()),
            quality: Some("hd".to_string()),
            style: Some("vivid".to_string()),
            n: 1,
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["model"], "dall-e-3");
        assert_eq!(json["prompt"], "A sunset over mountains");
        assert_eq!(json["size"], "1792x1024");
        assert_eq!(json["quality"], "hd");
        assert_eq!(json["style"], "vivid");
        assert_eq!(json["n"], 1);
    }

    #[test]
    fn parse_image_generate_response() {
        let json = r#"{
            "data": [
                {
                    "url": "https://oaidalleapiprodscus.blob.core.windows.net/image1.png",
                    "revised_prompt": "A breathtaking sunset over snow-capped mountains"
                },
                {
                    "b64_json": "iVBORw0KGgo=",
                    "revised_prompt": "Another sunset variation"
                }
            ]
        }"#;

        let resp: ImageGenerateResponse = serde_json::from_str(json).unwrap();

        assert_eq!(resp.data.len(), 2);
        assert_eq!(
            resp.data[0].url.as_deref(),
            Some("https://oaidalleapiprodscus.blob.core.windows.net/image1.png"),
        );
        assert!(resp.data[0].b64_json.is_none());
        assert_eq!(
            resp.data[0].revised_prompt.as_deref(),
            Some("A breathtaking sunset over snow-capped mountains"),
        );
        assert!(resp.data[1].url.is_none());
        assert_eq!(resp.data[1].b64_json.as_deref(), Some("iVBORw0KGgo="));
        assert_eq!(
            resp.data[1].revised_prompt.as_deref(),
            Some("Another sunset variation"),
        );
    }

    #[tokio::test]
    #[ignore]
    async fn openai_chat_integration() {
        use kernel::adapters::capability::ChatModel;
        // Requires OPENAI_API_KEY env var
        let adapter = OpenAIAdapter::new().unwrap();
        let config = RouterConfig {
            url: "https://api.openai.com".to_string(),
            api_key_env: Some("OPENAI_API_KEY".to_string()),
            api_key: None,
            enabled: true,
            timeout_ms: Some(30000),
            headers: std::collections::HashMap::new(),
        };
        let req = ChatRequest {
            model: Some("gpt-4o-mini".to_string()),
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

    #[test]
    fn capability_model_id_mirrors_inference_adapter_id() {
        // The capability-trait Model::id must return the same id the
        // `Model::id` does — including the custom-id
        // constructors used to register under openrouter / vercel / etc.
        let default = OpenAIAdapter::new().unwrap();
        assert_eq!(kernel::adapters::capability::Model::id(&default), "openai");

        let openrouter = OpenAIAdapter::with_id("openrouter").unwrap();
        assert_eq!(
            kernel::adapters::capability::Model::id(&openrouter),
            "openrouter"
        );
    }

    #[tokio::test]
    async fn chat_capability_missing_api_key_returns_auth_error() {
        use kernel::adapters::capability::ChatModel;
        // Mirror of `missing_api_key_returns_auth_error`, but driving the
        // typed ChatModel::chat path instead of execute(). No network is
        // touched — the missing key short-circuits before any request.
        let adapter = OpenAIAdapter::new().unwrap();
        let config = RouterConfig {
            url: "https://api.openai.com".to_string(),
            api_key_env: Some("__NONEXISTENT_OPENAI_KEY_FOR_TEST__".to_string()),
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: std::collections::HashMap::new(),
        };
        let req = ChatRequest {
            model: Some("gpt-4o-mini".to_string()),
            messages: vec![Message::text(MessageRole::User, "Hello".to_string())],
            system: None,
            max_tokens: Some(64),
            temperature: None,
            tools: Vec::new(),
        };

        let result = adapter.chat(&config, &req).await;
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), GatewayError::Authentication { .. }),
            "expected Authentication error from typed chat path",
        );
    }
}
