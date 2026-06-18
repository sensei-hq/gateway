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
    ImageResult, InferenceRequest, InferenceResponse, Payload, StreamChunk, VideoResult,
};

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct ReplicatePredictionRequest {
    model: String,
    input: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct ReplicatePredictionResponse {
    id: String,
    status: String,
    output: Option<serde_json::Value>,
    error: Option<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const BASE_URL: &str = "https://api.replicate.com/v1";
const DEFAULT_MODEL: &str = "tencent/hunyuan-video";

fn require_api_key(config: &RouterConfig) -> Result<String, GatewayError> {
    resolve_api_key(config).ok_or_else(|| GatewayError::Authentication {
        adapter: "replicate".into(),
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

/// Extract video URL from Replicate output, which can be a string or array of strings.
fn extract_video_url(output: &serde_json::Value) -> Option<String> {
    // Output can be a single URL string
    if let Some(url) = output.as_str() {
        return Some(url.to_string());
    }
    // Or an array of URLs — take the first
    if let Some(arr) = output.as_array() {
        return arr.first().and_then(|v| v.as_str()).map(|s| s.to_string());
    }
    None
}

// ---------------------------------------------------------------------------
// ReplicateAdapter
// ---------------------------------------------------------------------------

/// Adapter for the Replicate prediction API (video generation).
pub struct ReplicateAdapter {
    client: Client,
}

impl ReplicateAdapter {
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
impl InferenceAdapter for ReplicateAdapter {
    fn id(&self) -> &str {
        "replicate"
    }

    fn supports(&self, capability: &Capability) -> bool {
        matches!(
            capability,
            Capability::VideoGenerate | Capability::ImageGenerate
        )
    }

    async fn execute(
        &self,
        config: &RouterConfig,
        request: &InferenceRequest,
    ) -> Result<InferenceResponse, GatewayError> {
        let api_key = require_api_key(config)?;
        let url_base = base_url(config);

        // --- ImageGenerate branch ---
        if let Payload::ImageGenerate { prompt, .. } = &request.payload {
            let model = request
                .model
                .clone()
                .unwrap_or_else(|| "black-forest-labs/flux-schnell".to_string());

            let input = serde_json::json!({ "prompt": prompt });
            let body = ReplicatePredictionRequest {
                model: model.clone(),
                input,
            };

            let submit_url = format!("{url_base}/predictions");
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
                    adapter: "replicate".into(),
                    message: body_text,
                    status: Some(status.as_u16()),
                });
            }

            let prediction: ReplicatePredictionResponse =
                resp.json().await.map_err(|e| GatewayError::ProviderError {
                    adapter: "replicate".into(),
                    message: format!("failed to parse prediction response: {e}"),
                    status: Some(status.as_u16()),
                })?;

            let prediction_id = prediction.id;
            let poll_url = format!("{url_base}/predictions/{prediction_id}");
            let job_config = JobConfig::default();
            let client = &self.client;
            let api_key_ref = &api_key;

            let final_prediction = poll_until_complete(&job_config, || async {
                let resp = client
                    .get(&poll_url)
                    .bearer_auth(api_key_ref)
                    .send()
                    .await?;

                if !resp.status().is_success() {
                    let body_text = resp.text().await.unwrap_or_default();
                    return Err(GatewayError::ProviderError {
                        adapter: "replicate".into(),
                        message: body_text,
                        status: None,
                    });
                }

                let pred: ReplicatePredictionResponse =
                    resp.json().await.map_err(|e| GatewayError::ProviderError {
                        adapter: "replicate".into(),
                        message: format!("failed to parse prediction status: {e}"),
                        status: None,
                    })?;

                match pred.status.as_str() {
                    "succeeded" => Ok(Some(pred)),
                    "failed" | "canceled" => Err(GatewayError::ProviderError {
                        adapter: "replicate".into(),
                        message: pred
                            .error
                            .unwrap_or_else(|| "prediction failed".to_string()),
                        status: None,
                    }),
                    _ => Ok(None),
                }
            })
            .await?;

            // Output is array of URL strings
            let images: Vec<ImageResult> = final_prediction
                .output
                .as_ref()
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str())
                        .map(|url| ImageResult {
                            url: Some(url.to_string()),
                            b64_json: None,
                            revised_prompt: None,
                        })
                        .collect()
                })
                .unwrap_or_default();

            return Ok(InferenceResponse {
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
            });
        }

        // --- VideoGenerate branch ---
        let Payload::VideoGenerate {
            prompt,
            duration_secs,
            ..
        } = &request.payload
        else {
            return Err(GatewayError::ProviderError {
                adapter: "replicate".into(),
                message: "only VideoGenerate and ImageGenerate payloads are supported".into(),
                status: None,
            });
        };

        let model = resolve_model(request);

        // 1. Create prediction
        let mut input = serde_json::json!({ "prompt": prompt });
        if let Some(dur) = duration_secs {
            input["duration"] = serde_json::json!(dur);
        }

        let body = ReplicatePredictionRequest {
            model: model.clone(),
            input,
        };

        let submit_url = format!("{url_base}/predictions");
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
                adapter: "replicate".into(),
                message: body_text,
                status: Some(status.as_u16()),
            });
        }

        let prediction: ReplicatePredictionResponse =
            resp.json().await.map_err(|e| GatewayError::ProviderError {
                adapter: "replicate".into(),
                message: format!("failed to parse prediction response: {e}"),
                status: Some(status.as_u16()),
            })?;

        // 2. Poll until complete
        let prediction_id = prediction.id;
        let poll_url = format!("{url_base}/predictions/{prediction_id}");
        let job_config = JobConfig::default();
        let client = &self.client;
        let api_key_ref = &api_key;

        let final_prediction = poll_until_complete(&job_config, || async {
            let resp = client
                .get(&poll_url)
                .bearer_auth(api_key_ref)
                .send()
                .await?;

            if !resp.status().is_success() {
                let body_text = resp.text().await.unwrap_or_default();
                return Err(GatewayError::ProviderError {
                    adapter: "replicate".into(),
                    message: body_text,
                    status: None,
                });
            }

            let pred: ReplicatePredictionResponse =
                resp.json().await.map_err(|e| GatewayError::ProviderError {
                    adapter: "replicate".into(),
                    message: format!("failed to parse prediction status: {e}"),
                    status: None,
                })?;

            match pred.status.as_str() {
                "succeeded" => Ok(Some(pred)),
                "failed" | "canceled" => Err(GatewayError::ProviderError {
                    adapter: "replicate".into(),
                    message: pred
                        .error
                        .unwrap_or_else(|| "prediction failed".to_string()),
                    status: None,
                }),
                _ => Ok(None), // starting, processing
            }
        })
        .await?;

        // 3. Extract video URL from output
        let video_url = final_prediction.output.as_ref().and_then(extract_video_url);

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
            usage: None,
            tool_calls: Vec::new(),
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
            adapter: "replicate".into(),
            message: "streaming is not supported for image/video generation".into(),
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
    fn replicate_id_and_supports() {
        let adapter = ReplicateAdapter::new().unwrap();
        assert_eq!(adapter.id(), "replicate");

        assert!(adapter.supports(&Capability::VideoGenerate));
        assert!(!adapter.supports(&Capability::TextChat));
        assert!(!adapter.supports(&Capability::TextEmbed));
    }

    #[test]
    fn replicate_supports_image_generate() {
        let adapter = ReplicateAdapter::new().unwrap();
        assert!(adapter.supports(&Capability::ImageGenerate));
        assert!(adapter.supports(&Capability::VideoGenerate));
    }

    #[test]
    fn build_replicate_prediction_request() {
        let body = ReplicatePredictionRequest {
            model: "tencent/hunyuan-video".to_string(),
            input: serde_json::json!({"prompt": "A dog surfing a wave"}),
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["model"], "tencent/hunyuan-video");
        assert_eq!(json["input"]["prompt"], "A dog surfing a wave");
    }

    #[test]
    fn parse_replicate_prediction_response() {
        let json = r#"{
            "id": "pred-abc-123",
            "status": "starting",
            "output": null,
            "error": null
        }"#;

        let resp: ReplicatePredictionResponse = serde_json::from_str(json).unwrap();

        assert_eq!(resp.id, "pred-abc-123");
        assert_eq!(resp.status, "starting");
        assert!(resp.output.is_none());
        assert!(resp.error.is_none());
    }

    #[test]
    fn parse_replicate_succeeded_with_output() {
        // Test with string output
        let json = r#"{
            "id": "pred-abc-123",
            "status": "succeeded",
            "output": "https://replicate.delivery/video1.mp4",
            "error": null
        }"#;

        let resp: ReplicatePredictionResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.status, "succeeded");
        let url = extract_video_url(resp.output.as_ref().unwrap()).unwrap();
        assert_eq!(url, "https://replicate.delivery/video1.mp4");

        // Test with array output
        let json_array = r#"{
            "id": "pred-xyz-456",
            "status": "succeeded",
            "output": ["https://replicate.delivery/video1.mp4", "https://replicate.delivery/video2.mp4"],
            "error": null
        }"#;

        let resp2: ReplicatePredictionResponse = serde_json::from_str(json_array).unwrap();
        let url2 = extract_video_url(resp2.output.as_ref().unwrap()).unwrap();
        assert_eq!(url2, "https://replicate.delivery/video1.mp4");
    }
}
