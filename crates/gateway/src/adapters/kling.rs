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
struct KlingVideoRequest {
    prompt: String,
    model_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    aspect_ratio: Option<String>,
}

#[derive(Debug, Deserialize)]
struct KlingTaskResponse {
    data: KlingTaskData,
}

#[derive(Debug, Deserialize)]
struct KlingTaskData {
    task_id: String,
}

#[derive(Debug, Deserialize)]
struct KlingTaskStatus {
    data: KlingStatusData,
}

#[derive(Debug, Deserialize)]
struct KlingStatusData {
    task_status: String,
    task_result: Option<KlingResult>,
}

#[derive(Debug, Deserialize)]
struct KlingResult {
    videos: Option<Vec<KlingVideo>>,
}

#[derive(Debug, Deserialize)]
struct KlingVideo {
    url: String,
    duration: Option<f32>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const BASE_URL: &str = "https://api.klingai.com/v1";
const DEFAULT_MODEL: &str = "kling-v2";

fn require_api_key(config: &RouterConfig) -> Result<String, GatewayError> {
    resolve_api_key(config).ok_or_else(|| GatewayError::Authentication {
        adapter: "kling".into(),
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
// KlingAdapter
// ---------------------------------------------------------------------------

/// Adapter for the Kling AI video generation API.
pub struct KlingAdapter {
    client: Client,
}

impl KlingAdapter {
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
impl InferenceAdapter for KlingAdapter {
    fn id(&self) -> &str {
        "kling"
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
                adapter: "kling".into(),
                message: "only VideoGenerate payload is supported".into(),
                status: None,
            });
        };

        let api_key = require_api_key(config)?;
        let model = resolve_model(request);
        let url_base = base_url(config);

        // Map resolution to aspect ratio if provided
        let aspect_ratio = resolution.as_deref().map(|r| match r {
            "1080p" | "720p" | "1920x1080" | "1280x720" => "16:9".to_string(),
            "square" | "1:1" => "1:1".to_string(),
            other => other.to_string(),
        });

        // 1. Submit task
        let body = KlingVideoRequest {
            prompt: prompt.clone(),
            model_name: model.clone(),
            duration: duration_secs.map(|d| d.to_string()),
            aspect_ratio,
        };

        let submit_url = format!("{url_base}/videos/text2video");
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
                adapter: "kling".into(),
                message: body_text,
                status: Some(status.as_u16()),
            });
        }

        let task: KlingTaskResponse =
            resp.json().await.map_err(|e| GatewayError::ProviderError {
                adapter: "kling".into(),
                message: format!("failed to parse task response: {e}"),
                status: Some(status.as_u16()),
            })?;

        // 2. Poll until complete
        let task_id = task.data.task_id;
        let poll_url = format!("{url_base}/videos/text2video/{task_id}");
        let job_config = JobConfig::default();
        let client = &self.client;
        let api_key_ref = &api_key;

        let task_status = poll_until_complete(&job_config, || async {
            let resp = client
                .get(&poll_url)
                .bearer_auth(api_key_ref)
                .send()
                .await?;

            if !resp.status().is_success() {
                let body_text = resp.text().await.unwrap_or_default();
                return Err(GatewayError::ProviderError {
                    adapter: "kling".into(),
                    message: body_text,
                    status: None,
                });
            }

            let status: KlingTaskStatus =
                resp.json().await.map_err(|e| GatewayError::ProviderError {
                    adapter: "kling".into(),
                    message: format!("failed to parse task status: {e}"),
                    status: None,
                })?;

            match status.data.task_status.as_str() {
                "succeed" => Ok(Some(status)),
                "failed" => Err(GatewayError::ProviderError {
                    adapter: "kling".into(),
                    message: "video generation task failed".to_string(),
                    status: None,
                }),
                _ => Ok(None), // submitted, processing
            }
        })
        .await?;

        // 3. Extract video URL and duration
        let video = task_status
            .data
            .task_result
            .and_then(|r| r.videos)
            .and_then(|v| v.into_iter().next());

        let (video_url, video_duration) = match video {
            Some(v) => (Some(v.url), v.duration),
            None => (None, None),
        };

        Ok(InferenceResponse {
            success: true,
            content: None,
            embeddings: None,
            transcription: None,
            audio: None,
            images: None,
            videos: Some(vec![VideoResult {
                url: video_url,
                duration_secs: video_duration.or(duration_secs.map(|d| d as f32)),
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
            adapter: "kling".into(),
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
    fn kling_id_and_supports() {
        let adapter = KlingAdapter::new().unwrap();
        assert_eq!(adapter.id(), "kling");

        assert!(adapter.supports(&Capability::VideoGenerate));
        assert!(!adapter.supports(&Capability::TextChat));
        assert!(!adapter.supports(&Capability::TextEmbed));
    }

    #[test]
    fn build_kling_request() {
        let body = KlingVideoRequest {
            prompt: "A rocket launching into space".to_string(),
            model_name: "kling-v2".to_string(),
            duration: Some("5".to_string()),
            aspect_ratio: Some("16:9".to_string()),
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["prompt"], "A rocket launching into space");
        assert_eq!(json["model_name"], "kling-v2");
        assert_eq!(json["duration"], "5");
        assert_eq!(json["aspect_ratio"], "16:9");
    }

    #[test]
    fn parse_kling_task_response() {
        let json = r#"{"data":{"task_id":"task-xyz-789"}}"#;
        let resp: KlingTaskResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.data.task_id, "task-xyz-789");
    }

    #[test]
    fn parse_kling_status_succeeded() {
        let json = r#"{
            "data": {
                "task_status": "succeed",
                "task_result": {
                    "videos": [
                        {
                            "url": "https://cdn.klingai.com/video1.mp4",
                            "duration": 5.0
                        }
                    ]
                }
            }
        }"#;

        let status: KlingTaskStatus = serde_json::from_str(json).unwrap();

        assert_eq!(status.data.task_status, "succeed");
        let result = status.data.task_result.unwrap();
        let videos = result.videos.unwrap();
        assert_eq!(videos.len(), 1);
        assert_eq!(videos[0].url, "https://cdn.klingai.com/video1.mp4");
        assert!((videos[0].duration.unwrap() - 5.0).abs() < f32::EPSILON);
    }
}
