use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::base::{build_client, error_from_response, resolve_api_key};
use kernel::adapters::capability::{ImageModel, Model};
use kernel::adapters::{AdapterRegistry, RegisterInto};
use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;
use kernel::types::io::{ImageRequest, ImageResponse};
use kernel::types::request::ImageResult;

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct RecraftImageRequest {
    prompt: String,
    model: String,
    n: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<String>,
    style: String,
}

#[derive(Debug, Deserialize)]
struct RecraftImageResponse {
    data: Vec<RecraftImageData>,
}

#[derive(Debug, Deserialize)]
struct RecraftImageData {
    url: Option<String>,
    #[serde(default)]
    b64_json: Option<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const BASE_URL: &str = "https://external.api.recraft.ai/v1";
const DEFAULT_MODEL: &str = "recraftv3";

fn require_api_key(config: &RouterConfig) -> Result<String, GatewayError> {
    resolve_api_key(config).ok_or_else(|| GatewayError::Authentication {
        adapter: "recraft".into(),
        message: "missing API key — set the env var specified in api_key_env".into(),
    })
}

fn base_url(config: &RouterConfig) -> &str {
    let url = config.url.trim_end_matches('/');
    if url.is_empty() { BASE_URL } else { url }
}

// ---------------------------------------------------------------------------
// RecraftAdapter
// ---------------------------------------------------------------------------

/// Adapter for Recraft image generation.
///
/// Uses Bearer-token authentication. OpenAI-compatible response format.
pub struct RecraftAdapter {
    client: Client,
}

impl RecraftAdapter {
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

impl Model for RecraftAdapter {
    fn id(&self) -> &str {
        "recraft"
    }
}

#[async_trait]
impl ImageModel for RecraftAdapter {
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

        let body = RecraftImageRequest {
            prompt: req.prompt.clone(),
            model: model.clone(),
            n: req.n,
            size: req.size.clone().or_else(|| Some("1024x1024".to_string())),
            style: "realistic_image".to_string(),
        };

        let url = format!("{url_base}/images/generations");
        let mut http_req = self.client.post(&url).json(&body).bearer_auth(&api_key);

        for (k, v) in &config.headers {
            http_req = http_req.header(k.as_str(), v.as_str());
        }

        let response = http_req.send().await?;
        let status = response.status();

        if !status.is_success() {
            return Err(error_from_response("recraft", response).await);
        }

        let recraft_resp: RecraftImageResponse =
            response
                .json()
                .await
                .map_err(|e| GatewayError::ProviderError {
                    adapter: "recraft".into(),
                    message: format!("failed to parse recraft response: {e}"),
                    status: Some(status.as_u16()),
                })?;

        let images: Vec<ImageResult> = recraft_resp
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
impl RegisterInto for RecraftAdapter {
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
    fn recraft_id_and_supports() {
        let adapter = RecraftAdapter::new().unwrap();
        // `id` comes from the `Model`
        // trait, so the call must be disambiguated.
        assert_eq!(kernel::adapters::capability::Model::id(&adapter), "recraft");
        assert_eq!(Model::id(&adapter), "recraft");
    }

    #[test]
    fn build_recraft_request() {
        let body = RecraftImageRequest {
            prompt: "A sunset over mountains".to_string(),
            model: "recraftv3".to_string(),
            n: 1,
            size: Some("1024x1024".to_string()),
            style: "realistic_image".to_string(),
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["prompt"], "A sunset over mountains");
        assert_eq!(json["model"], "recraftv3");
        assert_eq!(json["n"], 1);
        assert_eq!(json["size"], "1024x1024");
        assert_eq!(json["style"], "realistic_image");
    }

    #[test]
    fn parse_recraft_response() {
        let json = r#"{
            "data": [
                {"url": "https://recraft.ai/output/image1.png"},
                {"url": "https://recraft.ai/output/image2.png"}
            ]
        }"#;

        let resp: RecraftImageResponse = serde_json::from_str(json).unwrap();

        assert_eq!(resp.data.len(), 2);
        assert_eq!(
            resp.data[0].url.as_deref(),
            Some("https://recraft.ai/output/image1.png"),
        );
        assert_eq!(
            resp.data[1].url.as_deref(),
            Some("https://recraft.ai/output/image2.png"),
        );
    }

    #[tokio::test]
    async fn generate_image_missing_api_key_returns_auth_error() {
        let adapter = RecraftAdapter::new().unwrap();
        let config = RouterConfig {
            url: "https://external.api.recraft.ai/v1".to_string(),
            api_key_env: Some("__NONEXISTENT_RECRAFT_KEY_FOR_TEST__".to_string()),
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
