use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::base::{build_client, resolve_api_key};
use crate::adapters::async_job::{JobConfig, poll_until_complete};
use crate::types::config::RouterConfig;
use crate::types::error::GatewayError;
use crate::types::io::{ImageRequest, ImageResponse};
use crate::types::request::ImageResult;

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct FluxImageRequest {
    prompt: String,
    width: u32,
    height: u32,
}

#[derive(Debug, Deserialize)]
struct FluxSubmitResponse {
    id: String,
}

#[derive(Debug, Deserialize)]
struct FluxPollResponse {
    status: String,
    result: Option<FluxResult>,
}

#[derive(Debug, Deserialize)]
struct FluxResult {
    sample: String,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const BASE_URL: &str = "https://api.bfl.ai/v1";
const DEFAULT_MODEL: &str = "flux-pro-1.1";

fn require_api_key(config: &RouterConfig) -> Result<String, GatewayError> {
    resolve_api_key(config).ok_or_else(|| GatewayError::Authentication {
        adapter: "flux".into(),
        message: "missing API key — set the env var specified in api_key_env".into(),
    })
}

fn base_url(config: &RouterConfig) -> &str {
    let url = config.url.trim_end_matches('/');
    if url.is_empty() { BASE_URL } else { url }
}

/// Parse "WIDTHxHEIGHT" into (width, height), defaulting to (1024, 1024).
fn parse_size(size: &Option<String>) -> (u32, u32) {
    size.as_deref()
        .and_then(|s| {
            let parts: Vec<&str> = s.split('x').collect();
            if parts.len() == 2 {
                let w = parts[0].parse::<u32>().ok()?;
                let h = parts[1].parse::<u32>().ok()?;
                Some((w, h))
            } else {
                None
            }
        })
        .unwrap_or((1024, 1024))
}

// ---------------------------------------------------------------------------
// FluxAdapter
// ---------------------------------------------------------------------------

/// Adapter for Black Forest Labs FLUX image generation.
///
/// Uses `x-key` header for authentication (not Bearer).
/// Async: submits a job, then polls for result.
pub struct FluxAdapter {
    client: Client,
}

impl FluxAdapter {
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
// Capability traits (see docs/design/adapter-capability-traits.md). Traits +
// RegisterInto are referenced by full path.
// ---------------------------------------------------------------------------

impl crate::adapters::capability::Model for FluxAdapter {
    fn id(&self) -> &str {
        "flux"
    }
}

#[async_trait]
impl crate::adapters::capability::ImageModel for FluxAdapter {
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
        let (width, height) = parse_size(&req.size);

        let body = FluxImageRequest {
            prompt: req.prompt.clone(),
            width,
            height,
        };

        // 1. Submit job
        let submit_url = format!("{url_base}/{model}");
        let resp = self
            .client
            .post(&submit_url)
            .json(&body)
            .header("x-key", &api_key)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            return Err(match status.as_u16() {
                401 | 403 => GatewayError::Authentication {
                    adapter: "flux".into(),
                    message: body_text,
                },
                429 => GatewayError::RateLimit {
                    adapter: "flux".into(),
                    retry_after_ms: None,
                },
                _ => GatewayError::ProviderError {
                    adapter: "flux".into(),
                    message: body_text,
                    status: Some(status.as_u16()),
                },
            });
        }

        let submit_resp: FluxSubmitResponse =
            resp.json().await.map_err(|e| GatewayError::ProviderError {
                adapter: "flux".into(),
                message: format!("failed to parse submit response: {e}"),
                status: Some(status.as_u16()),
            })?;

        // 2. Poll until ready
        let task_id = submit_resp.id;
        let poll_url = format!("{url_base}/get_result?id={task_id}");
        let job_config = JobConfig::default();
        let client = &self.client;
        let api_key_ref = &api_key;

        let final_result = poll_until_complete(&job_config, || async {
            let resp = client
                .get(&poll_url)
                .header("x-key", api_key_ref)
                .send()
                .await?;

            if !resp.status().is_success() {
                let body_text = resp.text().await.unwrap_or_default();
                return Err(GatewayError::ProviderError {
                    adapter: "flux".into(),
                    message: body_text,
                    status: None,
                });
            }

            let poll_resp: FluxPollResponse =
                resp.json().await.map_err(|e| GatewayError::ProviderError {
                    adapter: "flux".into(),
                    message: format!("failed to parse poll response: {e}"),
                    status: None,
                })?;

            match poll_resp.status.as_str() {
                "Ready" => Ok(Some(poll_resp)),
                "Error" | "Failed" => Err(GatewayError::ProviderError {
                    adapter: "flux".into(),
                    message: "FLUX task failed".to_string(),
                    status: None,
                }),
                _ => Ok(None), // Pending, Processing
            }
        })
        .await?;

        // 3. Extract sample URL
        let sample_url = final_result.result.map(|r| r.sample);

        Ok(ImageResponse {
            images: vec![ImageResult {
                url: sample_url,
                b64_json: None,
                revised_prompt: None,
            }],
        })
    }
}

#[async_trait]
impl crate::adapters::RegisterInto for FluxAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &crate::adapters::AdapterRegistry) {
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
    fn flux_id_and_supports() {
        let adapter = FluxAdapter::new().unwrap();
        assert_eq!(crate::adapters::capability::Model::id(&adapter), "flux");
    }

    #[test]
    fn flux_capability_model_id() {
        let adapter = FluxAdapter::new().unwrap();
        // Reference `Model::id` by full path
        // and the capability `Model` trait.
        assert_eq!(crate::adapters::capability::Model::id(&adapter), "flux");
    }

    #[test]
    fn parse_size_to_dimensions() {
        assert_eq!(parse_size(&Some("1024x768".to_string())), (1024, 768));
        assert_eq!(parse_size(&Some("512x512".to_string())), (512, 512));
        assert_eq!(parse_size(&None), (1024, 1024));
        assert_eq!(parse_size(&Some("invalid".to_string())), (1024, 1024));
    }

    #[test]
    fn parse_flux_submit_response() {
        let json = r#"{"id":"abc-123-task"}"#;
        let resp: FluxSubmitResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.id, "abc-123-task");
    }

    #[test]
    fn parse_flux_result_ready() {
        let json = r#"{"status":"Ready","result":{"sample":"https://bfl.ai/output/image.png"}}"#;
        let resp: FluxPollResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.status, "Ready");
        let result = resp.result.unwrap();
        assert_eq!(result.sample, "https://bfl.ai/output/image.png");
    }
}
