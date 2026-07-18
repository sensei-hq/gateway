use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use reqwest::Client;
use serde::Deserialize;

use super::base::{build_client, resolve_api_key};
use super::capability::{ImageModel, Model};
use super::{AdapterRegistry, RegisterInto};
use crate::types::config::RouterConfig;
use crate::types::error::GatewayError;
use crate::types::io::{ImageRequest, ImageResponse};
use crate::types::request::ImageResult;

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct StabilityJsonResponse {
    image: String,
    #[allow(dead_code)]
    seed: Option<u64>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const BASE_URL: &str = "https://api.stability.ai/v2beta";
const DEFAULT_MODEL: &str = "sd3.5-large";

fn require_api_key(config: &RouterConfig) -> Result<String, GatewayError> {
    resolve_api_key(config).ok_or_else(|| GatewayError::Authentication {
        adapter: "stability".into(),
        message: "missing API key — set the env var specified in api_key_env".into(),
    })
}

fn base_url(config: &RouterConfig) -> &str {
    let url = config.url.trim_end_matches('/');
    if url.is_empty() { BASE_URL } else { url }
}

fn size_to_aspect_ratio(size: &Option<String>) -> &'static str {
    match size.as_deref() {
        Some("1792x1024") | Some("1024x576") => "16:9",
        Some("1024x1792") | Some("576x1024") => "9:16",
        Some("1024x1024") | None => "1:1",
        _ => "1:1",
    }
}

// ---------------------------------------------------------------------------
// StabilityAdapter
// ---------------------------------------------------------------------------

/// Adapter for Stability AI (Stable Diffusion 3.x) image generation.
///
/// Uses Bearer-token authentication and multipart form uploads.
pub struct StabilityAdapter {
    client: Client,
}

impl StabilityAdapter {
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
// Capability traits (see docs/design/adapter-capability-traits.md).
// Referenced by full path.
// ---------------------------------------------------------------------------

impl Model for StabilityAdapter {
    fn id(&self) -> &str {
        "stability"
    }
}

#[async_trait]
impl ImageModel for StabilityAdapter {
    async fn generate_image(
        &self,
        config: &RouterConfig,
        req: &ImageRequest,
    ) -> Result<ImageResponse, GatewayError> {
        let api_key = require_api_key(config)?;
        let model = req
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());
        let url_base = base_url(config);
        let aspect_ratio = size_to_aspect_ratio(&req.size);

        let form = reqwest::multipart::Form::new()
            .text("prompt", req.prompt.clone())
            .text("model", model.clone())
            .text("output_format", "png")
            .text("aspect_ratio", aspect_ratio.to_string());

        let url = format!("{url_base}/stable-image/generate/sd3");
        let mut http_req = self.client.post(&url).multipart(form).bearer_auth(&api_key);

        for (k, v) in &config.headers {
            http_req = http_req.header(k.as_str(), v.as_str());
        }

        let response = http_req.send().await?;
        let status = response.status();

        if !status.is_success() {
            let body_text = response.text().await.unwrap_or_default();
            return Err(match status.as_u16() {
                401 | 403 => GatewayError::Authentication {
                    adapter: "stability".into(),
                    message: body_text,
                },
                429 => GatewayError::RateLimit {
                    adapter: "stability".into(),
                    retry_after_ms: None,
                },
                _ => GatewayError::ProviderError {
                    adapter: "stability".into(),
                    message: body_text,
                    status: Some(status.as_u16()),
                },
            });
        }

        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        let b64 = if content_type.starts_with("application/json") {
            let json_resp: StabilityJsonResponse =
                response
                    .json()
                    .await
                    .map_err(|e| GatewayError::ProviderError {
                        adapter: "stability".into(),
                        message: format!("failed to parse stability response: {e}"),
                        status: Some(status.as_u16()),
                    })?;
            json_resp.image
        } else {
            // image/* — raw bytes
            let bytes = response
                .bytes()
                .await
                .map_err(|e| GatewayError::ProviderError {
                    adapter: "stability".into(),
                    message: format!("failed to read image bytes: {e}"),
                    status: None,
                })?;
            STANDARD.encode(&bytes)
        };

        Ok(ImageResponse {
            images: vec![ImageResult {
                b64_json: Some(b64),
                url: None,
                revised_prompt: None,
            }],
            degraded: false,
        })
    }
}

#[async_trait]
impl RegisterInto for StabilityAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &AdapterRegistry) {
        reg.register_image(self).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stability_id_and_supports() {
        let adapter = StabilityAdapter::new().unwrap();
        assert_eq!(
            crate::adapters::capability::Model::id(&adapter),
            "stability"
        );
    }

    #[test]
    fn map_size_to_aspect_ratio() {
        assert_eq!(size_to_aspect_ratio(&Some("1792x1024".to_string())), "16:9");
        assert_eq!(size_to_aspect_ratio(&Some("1024x576".to_string())), "16:9");
        assert_eq!(size_to_aspect_ratio(&Some("1024x1792".to_string())), "9:16");
        assert_eq!(size_to_aspect_ratio(&Some("576x1024".to_string())), "9:16");
        assert_eq!(size_to_aspect_ratio(&Some("1024x1024".to_string())), "1:1");
        assert_eq!(size_to_aspect_ratio(&None), "1:1");
        assert_eq!(size_to_aspect_ratio(&Some("512x512".to_string())), "1:1");
    }

    #[test]
    fn parse_stability_json_response() {
        let json = r#"{"image":"abc123","seed":42}"#;
        let resp: StabilityJsonResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.image, "abc123");
        assert_eq!(resp.seed, Some(42));
    }

    #[tokio::test]
    async fn generate_image_missing_api_key_returns_auth_error() {
        let adapter = StabilityAdapter::new().unwrap();
        let config = RouterConfig {
            url: "https://api.stability.ai/v2beta".to_string(),
            api_key_env: Some("__NONEXISTENT_STABILITY_KEY_FOR_TEST__".to_string()),
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: std::collections::HashMap::new(),
        };
        let request = ImageRequest {
            model: None,
            prompt: "A cat".to_string(),
            size: None,
            quality: None,
            style: None,
            n: 1,
        };

        let result = adapter.generate_image(&config, &request).await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(
            matches!(err, GatewayError::Authentication { .. }),
            "expected Authentication error, got: {err:?}",
        );
    }
}
