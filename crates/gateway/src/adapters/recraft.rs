use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::InferenceAdapter;
use super::base::{build_client, resolve_api_key};
use crate::types::capability::Capability;
use crate::types::config::RouterConfig;
use crate::types::error::GatewayError;
use crate::types::request::{
    ImageResult, InferenceRequest, InferenceResponse, Payload, StreamChunk,
};

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

fn resolve_model(request: &InferenceRequest) -> String {
    request
        .model
        .clone()
        .unwrap_or_else(|| DEFAULT_MODEL.to_string())
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

#[async_trait]
impl InferenceAdapter for RecraftAdapter {
    fn id(&self) -> &str {
        "recraft"
    }

    fn supports(&self, capability: &Capability) -> bool {
        matches!(capability, Capability::ImageGenerate)
    }

    async fn execute(
        &self,
        config: &RouterConfig,
        request: &InferenceRequest,
    ) -> Result<InferenceResponse, GatewayError> {
        let Payload::ImageGenerate {
            prompt, size, n, ..
        } = &request.payload
        else {
            return Err(GatewayError::ProviderError {
                adapter: "recraft".into(),
                message: "only ImageGenerate payload is supported".into(),
                status: None,
            });
        };

        let api_key = require_api_key(config)?;
        let model = resolve_model(request);
        let url_base = base_url(config);

        let body = RecraftImageRequest {
            prompt: prompt.clone(),
            model: model.clone(),
            n: *n,
            size: size.clone().or_else(|| Some("1024x1024".to_string())),
            style: "realistic_image".to_string(),
        };

        let url = format!("{url_base}/images/generations");
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
                    adapter: "recraft".into(),
                    message: body_text,
                },
                429 => GatewayError::RateLimit {
                    adapter: "recraft".into(),
                    retry_after_ms: None,
                },
                _ => GatewayError::ProviderError {
                    adapter: "recraft".into(),
                    message: body_text,
                    status: Some(status.as_u16()),
                },
            });
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

        Ok(InferenceResponse {
            success: true,
            content: None,
            embeddings: None,
            transcription: None,
            audio: None,
            images: Some(images),
            videos: None,
            model: Some(model),
            tool_calls: Vec::new(),
            usage: None,
            estimated_cost: None,
            actual_cost: None,
            attempts: vec![],
        })
    }

    async fn stream(
        &self,
        _config: &RouterConfig,
        _request: &InferenceRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>
    {
        Err(GatewayError::ProviderError {
            adapter: "recraft".into(),
            message: "streaming is not supported for image generation".into(),
            status: None,
        })
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
        assert_eq!(adapter.id(), "recraft");

        assert!(adapter.supports(&Capability::ImageGenerate));
        assert!(!adapter.supports(&Capability::TextChat));
        assert!(!adapter.supports(&Capability::VideoGenerate));
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
    async fn missing_api_key_returns_auth_error() {
        let adapter = RecraftAdapter::new().unwrap();
        let config = RouterConfig {
            url: "https://external.api.recraft.ai/v1".to_string(),
            api_key_env: Some("__NONEXISTENT_RECRAFT_KEY_FOR_TEST__".to_string()),
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: std::collections::HashMap::new(),
        };
        let request = InferenceRequest {
            capability: Capability::ImageGenerate,
            model: None,
            router: None,
            chain: None,
            payload: Payload::ImageGenerate {
                prompt: "A cat".to_string(),
                size: None,
                quality: None,
                style: None,
                n: 1,
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
