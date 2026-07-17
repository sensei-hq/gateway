use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use super::InferenceAdapter;
use super::base::{build_client, resolve_api_key};
use crate::adapters::async_job::{JobConfig, poll_until_complete};
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
struct RunwayTaskRequest {
    model: String,
    #[serde(rename = "taskType")]
    task_type: String,
    #[serde(rename = "textPrompt")]
    text_prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct RunwayTaskResponse {
    id: String,
}

#[derive(Debug, Deserialize)]
struct RunwayTaskStatus {
    status: String,
    output: Option<Vec<String>>,
    failure: Option<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const BASE_URL: &str = "https://api.runwayml.com/v1";
const DEFAULT_MODEL: &str = "gen-4";

fn require_api_key(config: &RouterConfig) -> Result<String, GatewayError> {
    resolve_api_key(config).ok_or_else(|| GatewayError::Authentication {
        adapter: "runway".into(),
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
// RunwayAdapter
// ---------------------------------------------------------------------------

/// Adapter for the Runway ML video generation API.
pub struct RunwayAdapter {
    client: Client,
}

impl RunwayAdapter {
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
impl InferenceAdapter for RunwayAdapter {
    fn id(&self) -> &str {
        "runway"
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
            ..
        } = &request.payload
        else {
            return Err(GatewayError::ProviderError {
                adapter: "runway".into(),
                message: "only VideoGenerate payload is supported".into(),
                status: None,
            });
        };

        let api_key = require_api_key(config)?;
        let model = resolve_model(request);
        let url_base = base_url(config);

        // 1. Submit task
        let body = RunwayTaskRequest {
            model: model.clone(),
            task_type: "text-to-video".to_string(),
            text_prompt: prompt.clone(),
            duration: *duration_secs,
        };

        let submit_url = format!("{url_base}/tasks");
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
                adapter: "runway".into(),
                message: body_text,
                status: Some(status.as_u16()),
            });
        }

        let task: RunwayTaskResponse =
            resp.json().await.map_err(|e| GatewayError::ProviderError {
                adapter: "runway".into(),
                message: format!("failed to parse task response: {e}"),
                status: Some(status.as_u16()),
            })?;

        // 2. Poll until complete
        let task_id = task.id;
        let poll_url = format!("{url_base}/tasks/{task_id}");
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
                    adapter: "runway".into(),
                    message: body_text,
                    status: None,
                });
            }

            let status: RunwayTaskStatus =
                resp.json().await.map_err(|e| GatewayError::ProviderError {
                    adapter: "runway".into(),
                    message: format!("failed to parse task status: {e}"),
                    status: None,
                })?;

            match status.status.as_str() {
                "SUCCEEDED" => Ok(Some(status)),
                "FAILED" => Err(GatewayError::ProviderError {
                    adapter: "runway".into(),
                    message: status.failure.unwrap_or_else(|| "task failed".to_string()),
                    status: None,
                }),
                _ => Ok(None), // PENDING, RUNNING
            }
        })
        .await?;

        // 3. Extract video URL
        let video_url = task_status.output.and_then(|urls| urls.into_iter().next());

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
            adapter: "runway".into(),
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
    fn runway_id_and_supports() {
        let adapter = RunwayAdapter::new().unwrap();
        assert_eq!(adapter.id(), "runway");

        assert!(adapter.supports(&Capability::VideoGenerate));
        assert!(!adapter.supports(&Capability::TextChat));
        assert!(!adapter.supports(&Capability::TextEmbed));
    }

    #[test]
    fn build_runway_task_request() {
        let body = RunwayTaskRequest {
            model: "gen-4".to_string(),
            task_type: "text-to-video".to_string(),
            text_prompt: "A sunset over the ocean".to_string(),
            duration: Some(5),
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["model"], "gen-4");
        assert_eq!(json["taskType"], "text-to-video");
        assert_eq!(json["textPrompt"], "A sunset over the ocean");
        assert_eq!(json["duration"], 5);
    }

    #[test]
    fn parse_runway_task_response() {
        let json = r#"{"id":"task-abc-123"}"#;
        let resp: RunwayTaskResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.id, "task-abc-123");
    }

    #[test]
    fn parse_runway_status_succeeded() {
        let json = r#"{
            "status": "SUCCEEDED",
            "output": ["https://cdn.runwayml.com/video1.mp4", "https://cdn.runwayml.com/video2.mp4"],
            "failure": null
        }"#;

        let status: RunwayTaskStatus = serde_json::from_str(json).unwrap();

        assert_eq!(status.status, "SUCCEEDED");
        let output = status.output.unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(output[0], "https://cdn.runwayml.com/video1.mp4");
        assert!(status.failure.is_none());
    }
}
