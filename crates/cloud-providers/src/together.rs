use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::base::{build_client, resolve_api_key};
use crate::openai_compat;
use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;
use kernel::types::io::{ChatRequest, ChatResponse, ImageRequest, ImageResponse};
use kernel::types::request::{ImageResult, StreamChunk};

// ---------------------------------------------------------------------------
// Wire types — image generation (OpenAI-compatible)
//
// The chat / streaming wire types + helpers live in the shared
// `openai_compat` module (this adapter delegates its `ChatModel` methods
// there). Only the image-generation surface keeps its wire types here.
// ---------------------------------------------------------------------------

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

const BASE_URL: &str = "https://api.together.xyz";
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
    if url.is_empty() { BASE_URL } else { url }
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

// ---------------------------------------------------------------------------
// Capability traits. Traits + RegisterInto referenced by full path.
// ---------------------------------------------------------------------------

impl kernel::adapters::capability::Model for TogetherAdapter {
    fn id(&self) -> &str {
        "together"
    }
}

#[async_trait]
impl kernel::adapters::capability::ChatModel for TogetherAdapter {
    async fn chat(
        &self,
        config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError> {
        // Together requires a key: a missing one short-circuits to an
        // Authentication error before any request is built. The shared
        // core treats the key as optional (local providers need none), so
        // the contract is enforced here, mirroring the OpenAI adapter.
        require_api_key(config)?;
        openai_compat::chat(
            &self.client,
            base_url(config),
            DEFAULT_CHAT_MODEL,
            config,
            req,
        )
        .await
    }

    async fn chat_stream(
        &self,
        config: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        require_api_key(config)?;
        openai_compat::chat_stream(
            &self.client,
            base_url(config),
            DEFAULT_CHAT_MODEL,
            config,
            req,
        )
        .await
    }
}

#[async_trait]
impl kernel::adapters::capability::ImageModel for TogetherAdapter {
    async fn generate_image(
        &self,
        config: &RouterConfig,
        req: &ImageRequest,
    ) -> Result<ImageResponse, GatewayError> {
        let api_key = require_api_key(config)?;
        let url_base = base_url(config);
        let model = req
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_IMAGE_MODEL.to_string());

        let body = ImageGenerateRequest {
            model,
            prompt: req.prompt.clone(),
            n: req.n,
            size: req.size.clone().or_else(|| Some("1024x1024".to_string())),
        };

        let url = format!("{url_base}/v1/images/generations");
        let mut http_req = self.client.post(&url).json(&body).bearer_auth(&api_key);

        for (k, v) in &config.headers {
            http_req = http_req.header(k.as_str(), v.as_str());
        }

        let response = http_req.send().await?;
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
            response
                .json()
                .await
                .map_err(|e| GatewayError::ProviderError {
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

        Ok(ImageResponse {
            images,
            degraded: false,
        })
    }
}

#[async_trait]
impl kernel::adapters::RegisterInto for TogetherAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &kernel::adapters::AdapterRegistry) {
        reg.register_chat(self.clone()).await;
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
    fn together_id_and_supports() {
        let adapter = TogetherAdapter::new().unwrap();
        assert_eq!(kernel::adapters::capability::Model::id(&adapter), "together");
    }

    #[test]
    fn together_capability_model_id() {
        // Typed-trait identity mirrors the legacy `id()` above via full path,
        // using the capability `Model::id`.
        let adapter = TogetherAdapter::new().unwrap();
        assert_eq!(kernel::adapters::capability::Model::id(&adapter), "together");
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

    #[tokio::test]
    async fn missing_api_key_returns_auth_error() {
        use kernel::adapters::capability::ChatModel;
        let adapter = TogetherAdapter::new().unwrap();
        let config = RouterConfig {
            url: "https://api.together.xyz/v1".to_string(),
            api_key_env: Some("__NONEXISTENT_TOGETHER_KEY_FOR_TEST__".to_string()),
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: std::collections::HashMap::new(),
        };
        let req = kernel::types::io::ChatRequest {
            model: None,
            messages: vec![Message::text(MessageRole::User, "Hello".to_string())],
            system: None,
            max_tokens: Some(64),
            temperature: None,
            tools: Vec::new(),
        };

        let result = adapter.chat(&config, &req).await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(
            matches!(err, GatewayError::Authentication { .. }),
            "expected Authentication error, got: {err:?}",
        );
    }
}
