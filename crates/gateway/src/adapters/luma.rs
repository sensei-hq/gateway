use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::base::{build_client, resolve_api_key};
use super::InferenceAdapter;
use crate::adapters::async_job::{poll_until_complete, JobConfig};
use crate::types::capability::Capability;
use crate::types::config::RouterConfig;
use crate::types::error::GatewayError;
use crate::types::request::{
    InferenceRequest, InferenceResponse, Payload, StreamChunk, VideoResult,
};

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

fn resolve_model(request: &InferenceRequest) -> String {
    request
        .model
        .clone()
        .unwrap_or_else(|| DEFAULT_MODEL.to_string())
}

fn base_url(config: &RouterConfig) -> &str {
    let url = config.url.trim_end_matches('/');
    if url.is_empty() {
        BASE_URL
    } else {
        url
    }
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

#[async_trait]
impl InferenceAdapter for LumaAdapter {
    fn id(&self) -> &str {
        "luma"
    }

    fn supports(&self, capability: &Capability) -> bool {
        matches!(capability, Capability::VideoGenerate)
    }

    async fn execute(
        &self,
        config: &RouterConfig,
        request: &InferenceRequest,
    ) -> Result<InferenceResponse, GatewayError> {
        let Payload::VideoGenerate {
            prompt,
            duration_secs,
            resolution,
        } = &request.payload
        else {
            return Err(GatewayError::ProviderError {
                adapter: "luma".into(),
                message: "only VideoGenerate payload is supported".into(),
                status: None,
            });
        };

        let api_key = require_api_key(config)?;
        let model = resolve_model(request);
        let url_base = base_url(config);

        // 1. Submit generation
        let body = LumaGenerationRequest {
            prompt: prompt.clone(),
            model: Some(model.clone()),
            resolution: resolution.clone(),
            duration: duration_secs.map(|d| format!("{d}s")),
        };

        let submit_url = format!("{url_base}/generations");
        let resp = self
            .client
            .post(&submit_url)
            .json(&body)
            .bearer_auth(&api_key)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            return Err(GatewayError::ProviderError {
                adapter: "luma".into(),
                message: body_text,
                status: Some(status.as_u16()),
            });
        }

        let generation: LumaGenerationResponse =
            resp.json().await.map_err(|e| GatewayError::ProviderError {
                adapter: "luma".into(),
                message: format!("failed to parse generation response: {e}"),
                status: Some(status.as_u16()),
            })?;

        // 2. Poll until complete
        let generation_id = generation.id;
        let poll_url = format!("{url_base}/generations/{generation_id}");
        let job_config = JobConfig::default();
        let client = &self.client;
        let api_key_ref = &api_key;

        let gen_status = poll_until_complete(&job_config, || async {
            let resp = client
                .get(&poll_url)
                .bearer_auth(api_key_ref)
                .send()
                .await?;

            if !resp.status().is_success() {
                let body_text = resp.text().await.unwrap_or_default();
                return Err(GatewayError::ProviderError {
                    adapter: "luma".into(),
                    message: body_text,
                    status: None,
                });
            }

            let status: LumaGenerationStatus =
                resp.json().await.map_err(|e| GatewayError::ProviderError {
                    adapter: "luma".into(),
                    message: format!("failed to parse generation status: {e}"),
                    status: None,
                })?;

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

        Ok(InferenceResponse {
            success: true,
            content: None,
            embeddings: None,
            transcription: None,
            audio: None,
            images: None,
            videos: Some(vec![VideoResult {
                url: video_url,
                duration_secs: duration_secs.map(|d| d as f32),
            }]),
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
            adapter: "luma".into(),
            message: "streaming is not supported for video generation".into(),
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
    fn luma_id_and_supports() {
        let adapter = LumaAdapter::new().unwrap();
        assert_eq!(adapter.id(), "luma");

        assert!(adapter.supports(&Capability::VideoGenerate));
        assert!(!adapter.supports(&Capability::TextChat));
        assert!(!adapter.supports(&Capability::TextEmbed));
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
