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
use crate::types::io::{ImageRequest, ImageResponse, VideoRequest, VideoResponse};
use crate::types::request::{
    ImageResult, InferenceRequest, InferenceResponse, Payload, StreamChunk, VideoResult,
};

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct FalVideoRequest {
    prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    duration: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    aspect_ratio: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FalQueueResponse {
    request_id: String,
}

#[derive(Debug, Deserialize)]
struct FalStatusResponse {
    status: String,
}

#[derive(Debug, Deserialize)]
struct FalResultResponse {
    video: Option<FalVideo>,
}

#[derive(Debug, Deserialize)]
struct FalVideo {
    url: String,
}

// Image-specific wire types

#[derive(Debug, Serialize)]
struct FalImageRequest {
    prompt: String,
}

#[derive(Debug, Deserialize)]
struct FalImageResult {
    images: Vec<FalImage>,
}

#[derive(Debug, Deserialize)]
struct FalImage {
    url: String,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const BASE_URL: &str = "https://queue.fal.run";
const DEFAULT_MODEL: &str = "fal-ai/veo3";

fn require_api_key(config: &RouterConfig) -> Result<String, GatewayError> {
    resolve_api_key(config).ok_or_else(|| GatewayError::Authentication {
        adapter: "fal".into(),
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
// FalAdapter
// ---------------------------------------------------------------------------

/// Adapter for the fal.ai unified inference API (video generation).
///
/// fal.ai uses `Authorization: Key {api_key}` instead of Bearer tokens.
pub struct FalAdapter {
    client: Client,
}

impl FalAdapter {
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
impl InferenceAdapter for FalAdapter {
    fn id(&self) -> &str {
        "fal"
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

        // --- ImageGenerate branch ---
        if let Payload::ImageGenerate { prompt, .. } = &request.payload {
            let model = request
                .model
                .clone()
                .unwrap_or_else(|| "fal-ai/flux-pro/v1.1".to_string());
            let url_base = base_url(config);
            let auth_header = format!("Key {api_key}");

            let body = FalImageRequest {
                prompt: prompt.clone(),
            };

            let submit_url = format!("{url_base}/{model}");
            let resp = self
                .client
                .post(&submit_url)
                .json(&body)
                .header("Authorization", &auth_header)
                .send()
                .await?;

            let status = resp.status();
            if !status.is_success() {
                let body_text = resp.text().await.unwrap_or_default();
                return Err(GatewayError::ProviderError {
                    adapter: "fal".into(),
                    message: body_text,
                    status: Some(status.as_u16()),
                });
            }

            let queue: FalQueueResponse =
                resp.json().await.map_err(|e| GatewayError::ProviderError {
                    adapter: "fal".into(),
                    message: format!("failed to parse queue response: {e}"),
                    status: Some(status.as_u16()),
                })?;

            let request_id = queue.request_id;
            let status_url = format!("{url_base}/requests/{request_id}/status");
            let job_config = JobConfig::default();
            let client = &self.client;
            let auth_ref = &auth_header;

            poll_until_complete(&job_config, || async {
                let resp = client
                    .get(&status_url)
                    .header("Authorization", auth_ref)
                    .send()
                    .await?;

                if !resp.status().is_success() {
                    let body_text = resp.text().await.unwrap_or_default();
                    return Err(GatewayError::ProviderError {
                        adapter: "fal".into(),
                        message: body_text,
                        status: None,
                    });
                }

                let fal_status: FalStatusResponse =
                    resp.json().await.map_err(|e| GatewayError::ProviderError {
                        adapter: "fal".into(),
                        message: format!("failed to parse status response: {e}"),
                        status: None,
                    })?;

                match fal_status.status.as_str() {
                    "COMPLETED" => Ok(Some(())),
                    "FAILED" => Err(GatewayError::ProviderError {
                        adapter: "fal".into(),
                        message: "fal.ai request failed".to_string(),
                        status: None,
                    }),
                    _ => Ok(None),
                }
            })
            .await?;

            let result_url = format!("{url_base}/requests/{request_id}");
            let resp = self
                .client
                .get(&result_url)
                .header("Authorization", &auth_header)
                .send()
                .await?;

            let resp_status = resp.status();
            if !resp_status.is_success() {
                let body_text = resp.text().await.unwrap_or_default();
                return Err(GatewayError::ProviderError {
                    adapter: "fal".into(),
                    message: body_text,
                    status: Some(resp_status.as_u16()),
                });
            }

            let result: FalImageResult =
                resp.json().await.map_err(|e| GatewayError::ProviderError {
                    adapter: "fal".into(),
                    message: format!("failed to parse image result: {e}"),
                    status: None,
                })?;

            let images: Vec<ImageResult> = result
                .images
                .into_iter()
                .map(|img| ImageResult {
                    url: Some(img.url),
                    b64_json: None,
                    revised_prompt: None,
                })
                .collect();

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
                adapter: "fal".into(),
                message: "only VideoGenerate and ImageGenerate payloads are supported".into(),
                status: None,
            });
        };

        let model = resolve_model(request);
        let url_base = base_url(config);
        let auth_header = format!("Key {api_key}");

        // 1. Submit to queue
        let body = FalVideoRequest {
            prompt: prompt.clone(),
            duration: *duration_secs,
            aspect_ratio: None,
        };

        let submit_url = format!("{url_base}/{model}");
        let resp = self
            .client
            .post(&submit_url)
            .json(&body)
            .header("Authorization", &auth_header)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            return Err(GatewayError::ProviderError {
                adapter: "fal".into(),
                message: body_text,
                status: Some(status.as_u16()),
            });
        }

        let queue: FalQueueResponse =
            resp.json().await.map_err(|e| GatewayError::ProviderError {
                adapter: "fal".into(),
                message: format!("failed to parse queue response: {e}"),
                status: Some(status.as_u16()),
            })?;

        // 2. Poll status until COMPLETED
        let request_id = queue.request_id;
        let status_url = format!("{url_base}/requests/{request_id}/status");
        let job_config = JobConfig::default();
        let client = &self.client;
        let auth_ref = &auth_header;

        poll_until_complete(&job_config, || async {
            let resp = client
                .get(&status_url)
                .header("Authorization", auth_ref)
                .send()
                .await?;

            if !resp.status().is_success() {
                let body_text = resp.text().await.unwrap_or_default();
                return Err(GatewayError::ProviderError {
                    adapter: "fal".into(),
                    message: body_text,
                    status: None,
                });
            }

            let fal_status: FalStatusResponse =
                resp.json().await.map_err(|e| GatewayError::ProviderError {
                    adapter: "fal".into(),
                    message: format!("failed to parse status response: {e}"),
                    status: None,
                })?;

            match fal_status.status.as_str() {
                "COMPLETED" => Ok(Some(())),
                "FAILED" => Err(GatewayError::ProviderError {
                    adapter: "fal".into(),
                    message: "fal.ai request failed".to_string(),
                    status: None,
                }),
                _ => Ok(None), // IN_QUEUE, IN_PROGRESS
            }
        })
        .await?;

        // 3. Fetch result
        let result_url = format!("{url_base}/requests/{request_id}");
        let resp = self
            .client
            .get(&result_url)
            .header("Authorization", &auth_header)
            .send()
            .await?;

        let resp_status = resp.status();
        if !resp_status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            return Err(GatewayError::ProviderError {
                adapter: "fal".into(),
                message: body_text,
                status: Some(resp_status.as_u16()),
            });
        }

        let result: FalResultResponse =
            resp.json().await.map_err(|e| GatewayError::ProviderError {
                adapter: "fal".into(),
                message: format!("failed to parse result response: {e}"),
                status: None,
            })?;

        let video_url = result.video.map(|v| v.url);

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
            adapter: "fal".into(),
            message: "streaming is not supported for image/video generation".into(),
            status: None,
        })
    }
}

// ---------------------------------------------------------------------------
// Capability traits (target model — see
// docs/design/adapter-capability-traits.md). Additive alongside the legacy
// `InferenceAdapter` impl above, which stays until the engine cuts over.
// The capability traits are referenced by full path so their `Model::id`
// method does not collide with `InferenceAdapter::id` in method resolution.
// ---------------------------------------------------------------------------

impl crate::adapters::capability::Model for FalAdapter {
    fn id(&self) -> &str {
        "fal"
    }
}

#[async_trait]
impl crate::adapters::capability::ImageModel for FalAdapter {
    async fn generate_image(
        &self,
        config: &RouterConfig,
        req: &ImageRequest,
    ) -> Result<ImageResponse, GatewayError> {
        let api_key = require_api_key(config)?;
        let model = req
            .model
            .clone()
            .unwrap_or_else(|| "fal-ai/flux-pro/v1.1".to_string());
        let url_base = base_url(config);
        let auth_header = format!("Key {api_key}");

        let body = FalImageRequest {
            prompt: req.prompt.clone(),
        };

        let submit_url = format!("{url_base}/{model}");
        let resp = self
            .client
            .post(&submit_url)
            .json(&body)
            .header("Authorization", &auth_header)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            return Err(GatewayError::ProviderError {
                adapter: "fal".into(),
                message: body_text,
                status: Some(status.as_u16()),
            });
        }

        let queue: FalQueueResponse =
            resp.json().await.map_err(|e| GatewayError::ProviderError {
                adapter: "fal".into(),
                message: format!("failed to parse queue response: {e}"),
                status: Some(status.as_u16()),
            })?;

        let request_id = queue.request_id;
        let status_url = format!("{url_base}/requests/{request_id}/status");
        let job_config = JobConfig::default();
        let client = &self.client;
        let auth_ref = &auth_header;

        poll_until_complete(&job_config, || async {
            let resp = client
                .get(&status_url)
                .header("Authorization", auth_ref)
                .send()
                .await?;

            if !resp.status().is_success() {
                let body_text = resp.text().await.unwrap_or_default();
                return Err(GatewayError::ProviderError {
                    adapter: "fal".into(),
                    message: body_text,
                    status: None,
                });
            }

            let fal_status: FalStatusResponse =
                resp.json().await.map_err(|e| GatewayError::ProviderError {
                    adapter: "fal".into(),
                    message: format!("failed to parse status response: {e}"),
                    status: None,
                })?;

            match fal_status.status.as_str() {
                "COMPLETED" => Ok(Some(())),
                "FAILED" => Err(GatewayError::ProviderError {
                    adapter: "fal".into(),
                    message: "fal.ai request failed".to_string(),
                    status: None,
                }),
                _ => Ok(None),
            }
        })
        .await?;

        let result_url = format!("{url_base}/requests/{request_id}");
        let resp = self
            .client
            .get(&result_url)
            .header("Authorization", &auth_header)
            .send()
            .await?;

        let resp_status = resp.status();
        if !resp_status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            return Err(GatewayError::ProviderError {
                adapter: "fal".into(),
                message: body_text,
                status: Some(resp_status.as_u16()),
            });
        }

        let result: FalImageResult =
            resp.json().await.map_err(|e| GatewayError::ProviderError {
                adapter: "fal".into(),
                message: format!("failed to parse image result: {e}"),
                status: None,
            })?;

        let images: Vec<ImageResult> = result
            .images
            .into_iter()
            .map(|img| ImageResult {
                url: Some(img.url),
                b64_json: None,
                revised_prompt: None,
            })
            .collect();

        Ok(ImageResponse { images })
    }
}

#[async_trait]
impl crate::adapters::capability::VideoModel for FalAdapter {
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
        let auth_header = format!("Key {api_key}");
        let duration_secs = req.duration_secs;

        // 1. Submit to queue
        let body = FalVideoRequest {
            prompt: req.prompt.clone(),
            duration: duration_secs,
            aspect_ratio: None,
        };

        let submit_url = format!("{url_base}/{model}");
        let resp = self
            .client
            .post(&submit_url)
            .json(&body)
            .header("Authorization", &auth_header)
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            return Err(GatewayError::ProviderError {
                adapter: "fal".into(),
                message: body_text,
                status: Some(status.as_u16()),
            });
        }

        let queue: FalQueueResponse =
            resp.json().await.map_err(|e| GatewayError::ProviderError {
                adapter: "fal".into(),
                message: format!("failed to parse queue response: {e}"),
                status: Some(status.as_u16()),
            })?;

        // 2. Poll status until COMPLETED
        let request_id = queue.request_id;
        let status_url = format!("{url_base}/requests/{request_id}/status");
        let job_config = JobConfig::default();
        let client = &self.client;
        let auth_ref = &auth_header;

        poll_until_complete(&job_config, || async {
            let resp = client
                .get(&status_url)
                .header("Authorization", auth_ref)
                .send()
                .await?;

            if !resp.status().is_success() {
                let body_text = resp.text().await.unwrap_or_default();
                return Err(GatewayError::ProviderError {
                    adapter: "fal".into(),
                    message: body_text,
                    status: None,
                });
            }

            let fal_status: FalStatusResponse =
                resp.json().await.map_err(|e| GatewayError::ProviderError {
                    adapter: "fal".into(),
                    message: format!("failed to parse status response: {e}"),
                    status: None,
                })?;

            match fal_status.status.as_str() {
                "COMPLETED" => Ok(Some(())),
                "FAILED" => Err(GatewayError::ProviderError {
                    adapter: "fal".into(),
                    message: "fal.ai request failed".to_string(),
                    status: None,
                }),
                _ => Ok(None), // IN_QUEUE, IN_PROGRESS
            }
        })
        .await?;

        // 3. Fetch result
        let result_url = format!("{url_base}/requests/{request_id}");
        let resp = self
            .client
            .get(&result_url)
            .header("Authorization", &auth_header)
            .send()
            .await?;

        let resp_status = resp.status();
        if !resp_status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            return Err(GatewayError::ProviderError {
                adapter: "fal".into(),
                message: body_text,
                status: Some(resp_status.as_u16()),
            });
        }

        let result: FalResultResponse =
            resp.json().await.map_err(|e| GatewayError::ProviderError {
                adapter: "fal".into(),
                message: format!("failed to parse result response: {e}"),
                status: None,
            })?;

        let video_url = result.video.map(|v| v.url);

        Ok(VideoResponse {
            videos: vec![VideoResult {
                url: video_url,
                duration_secs: duration_secs.map(|d| d as f32),
            }],
        })
    }
}

#[async_trait]
impl crate::adapters::RegisterInto for FalAdapter {
    async fn register_into(self: std::sync::Arc<Self>, reg: &crate::adapters::CapabilityRegistry) {
        reg.register_image(self.clone()).await;
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
    fn fal_id_and_supports() {
        let adapter = FalAdapter::new().unwrap();
        assert_eq!(adapter.id(), "fal");

        assert!(adapter.supports(&Capability::VideoGenerate));
        assert!(!adapter.supports(&Capability::TextChat));
        assert!(!adapter.supports(&Capability::TextEmbed));
    }

    #[test]
    fn fal_capability_model_id() {
        let adapter = FalAdapter::new().unwrap();
        // Full path avoids the `id()` ambiguity between `InferenceAdapter`
        // and the capability `Model` trait.
        assert_eq!(crate::adapters::capability::Model::id(&adapter), "fal");
    }

    #[test]
    fn build_fal_request() {
        let body = FalVideoRequest {
            prompt: "A cat playing piano".to_string(),
            duration: Some(5),
            aspect_ratio: Some("16:9".to_string()),
        };

        let json = serde_json::to_value(&body).unwrap();

        assert_eq!(json["prompt"], "A cat playing piano");
        assert_eq!(json["duration"], 5);
        assert_eq!(json["aspect_ratio"], "16:9");
    }

    #[test]
    fn parse_fal_queue_response() {
        let json = r#"{"request_id":"req-abc-123"}"#;
        let resp: FalQueueResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.request_id, "req-abc-123");
    }

    #[test]
    fn fal_supports_image_generate() {
        let adapter = FalAdapter::new().unwrap();
        assert!(adapter.supports(&Capability::ImageGenerate));
        assert!(adapter.supports(&Capability::VideoGenerate));
    }

    #[test]
    fn parse_fal_image_result() {
        let json = r#"{"images":[{"url":"https://fal.media/image1.png"}]}"#;
        let result: FalImageResult = serde_json::from_str(json).unwrap();
        assert_eq!(result.images.len(), 1);
        assert_eq!(result.images[0].url, "https://fal.media/image1.png");
    }

    #[test]
    fn parse_fal_status_and_result() {
        // Status response
        let status_json = r#"{"status":"COMPLETED"}"#;
        let status: FalStatusResponse = serde_json::from_str(status_json).unwrap();
        assert_eq!(status.status, "COMPLETED");

        // Result response
        let result_json = r#"{"video":{"url":"https://fal.media/video1.mp4"}}"#;
        let result: FalResultResponse = serde_json::from_str(result_json).unwrap();
        let video = result.video.unwrap();
        assert_eq!(video.url, "https://fal.media/video1.mp4");
    }
}
