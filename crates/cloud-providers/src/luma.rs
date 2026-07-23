use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::async_job::{JobConfig, poll_until_complete};
use crate::base::{build_client, get_json_bearer, post_json_bearer, resolve_api_key};
use kernel::types::config::RouterConfig;
use kernel::types::error::GatewayError;
use kernel::types::io::{VideoRequest, VideoResponse};
use kernel::types::request::VideoResult;

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct LumaGenerationRequest {
    prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resolution: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LumaGenerationResponse {
    id: String,
    #[allow(dead_code)]
    state: String,
}

#[derive(Debug, Deserialize)]
struct LumaGenerationStatus {
    #[allow(dead_code)]
    id: String,
    state: String,
    assets: Option<LumaAssets>,
    failure_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LumaAssets {
    video: Option<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const BASE_URL: &str = "https://api.lumalabs.ai/dream-machine/v1";
const DEFAULT_MODEL: &str = "ray-2";

fn require_api_key(config: &RouterConfig) -> Result<String, GatewayError> {
    resolve_api_key(config).ok_or_else(|| GatewayError::Authentication {
        adapter: "luma".into(),
        message: "missing API key — set the env var specified in api_key_env".into(),
    })
}

fn base_url(config: &RouterConfig) -> &str {
    let url = config.url.trim_end_matches('/');
    if url.is_empty() { BASE_URL } else { url }
}

// ---------------------------------------------------------------------------
// LumaAdapter
// ---------------------------------------------------------------------------

/// Adapter for the Luma AI Dream Machine video generation API.
pub struct LumaAdapter {
    client: Client,
}

impl LumaAdapter {
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

impl kernel::adapters::capability::Model for LumaAdapter {
    fn id(&self) -> &str {
        "luma"
    }
}

#[async_trait]
impl kernel::adapters::capability::VideoModel for LumaAdapter {
    async fn generate_video(
        &self,
        config: &RouterConfig,
        req: &VideoRequest,
    ) -> Result<VideoResponse, GatewayError> {
        let api_key = require_api_key(config)?;
        let model = req
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());
        let url_base = base_url(config);

        // 1. Submit generation
        let body = LumaGenerationRequest {
            prompt: req.prompt.clone(),
            model: Some(model.clone()),
            resolution: req.resolution.clone(),
            duration: req.duration_secs.map(|d| format!("{d}s")),
        };

        let submit_url = format!("{url_base}/generations");
        let generation: LumaGenerationResponse =
            post_json_bearer(&self.client, &submit_url, &api_key, "luma", &body).await?;

        // 2. Poll until complete
        let generation_id = generation.id;
        let poll_url = format!("{url_base}/generations/{generation_id}");
        let job_config = JobConfig::from_config(config);
        let client = &self.client;
        let api_key_ref = &api_key;

        let gen_status = poll_until_complete(&job_config, || async {
            let status: LumaGenerationStatus =
                get_json_bearer(client, &poll_url, api_key_ref, "luma").await?;

            match status.state.as_str() {
                "completed" => Ok(Some(status)),
                "failed" => Err(GatewayError::ProviderError {
                    adapter: "luma".into(),
                    message: status
                        .failure_reason
                        .unwrap_or_else(|| "generation failed".to_string()),
                    status: None,
                }),
                _ => Ok(None), // queued, dreaming
            }
        })
        .await?;

        // 3. Extract video URL
        let video_url = gen_status.assets.and_then(|a| a.video);

        Ok(VideoResponse {
            videos: vec![VideoResult {
                url: video_url,
                duration_secs: req.duration_secs.map(|d| d as f32),
            }],
            degraded: false,
        })
    }
}

#[async_trait]
impl kernel::adapters::RegisterInto for LumaAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &kernel::adapters::AdapterRegistry) {
        reg.register_video(self).await;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn luma_id_and_supports() {
        let adapter = LumaAdapter::new().unwrap();
        assert_eq!(kernel::adapters::capability::Model::id(&adapter), "luma");
    }

    #[test]
    fn luma_capability_model_id() {
        let adapter = LumaAdapter::new().unwrap();
        // Reference `Model::id` by full path
        // and the capability `Model` trait.
        assert_eq!(kernel::adapters::capability::Model::id(&adapter), "luma");
    }

    #[test]
    fn build_luma_request() {
        let body = LumaGenerationRequest {
            prompt: "A timelapse of a city skyline".to_string(),
            model: Some("ray-2".to_string()),
            resolution: Some("1080p".to_string()),
            duration: Some("5s".to_string()),
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["prompt"], "A timelapse of a city skyline");
        assert_eq!(json["model"], "ray-2");
        assert_eq!(json["resolution"], "1080p");
        assert_eq!(json["duration"], "5s");
    }

    #[test]
    fn parse_luma_generation_response() {
        let json = r#"{"id":"gen-abc-123","state":"queued"}"#;
        let resp: LumaGenerationResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.id, "gen-abc-123");
        assert_eq!(resp.state, "queued");
    }

    #[test]
    fn parse_luma_status_completed() {
        let json = r#"{
            "id": "gen-abc-123",
            "state": "completed",
            "assets": {
                "video": "https://storage.lumalabs.ai/video1.mp4"
            },
            "failure_reason": null
        }"#;

        let status: LumaGenerationStatus = serde_json::from_str(json).unwrap();

        assert_eq!(status.state, "completed");
        let assets = status.assets.unwrap();
        assert_eq!(
            assets.video.as_deref(),
            Some("https://storage.lumalabs.ai/video1.mp4"),
        );
        assert!(status.failure_reason.is_none());
    }
}
