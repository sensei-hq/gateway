//! Capability-segregated adapter traits. Each provider implements only
//! the traits for capabilities it supports; the registry stores one map
//! per capability. See `docs/design/adapter-capability-traits.md`.

use std::pin::Pin;

use async_trait::async_trait;
use futures::Stream;

use crate::types::config::RouterConfig;
use crate::types::error::GatewayError;
use crate::types::io::{
    ChatRequest, ChatResponse, EmbedRequest, EmbedResponse, ImageRequest, ImageResponse,
    SttRequest, SttResponse, TtsRequest, TtsResponse, VideoRequest, VideoResponse,
};
use crate::types::request::StreamChunk;

/// Identity shared by every adapter regardless of capability.
pub trait Model: Send + Sync {
    fn id(&self) -> &str;
}

type ChunkStream = Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>;

#[async_trait]
pub trait ChatModel: Model {
    async fn chat(
        &self,
        cfg: &RouterConfig,
        req: &ChatRequest,
    ) -> Result<ChatResponse, GatewayError>;

    /// Opt-in streaming. Providers that stream override this; the rest
    /// inherit `Unsupported`.
    async fn chat_stream(
        &self,
        _cfg: &RouterConfig,
        _req: &ChatRequest,
    ) -> Result<ChunkStream, GatewayError> {
        Err(GatewayError::Unsupported {
            adapter: self.id().to_string(),
            what: "streaming".into(),
        })
    }
}

#[async_trait]
pub trait EmbedModel: Model {
    async fn embed(
        &self,
        cfg: &RouterConfig,
        req: &EmbedRequest,
    ) -> Result<EmbedResponse, GatewayError>;
}

#[async_trait]
pub trait SttModel: Model {
    async fn transcribe(
        &self,
        cfg: &RouterConfig,
        req: &SttRequest,
    ) -> Result<SttResponse, GatewayError>;
}

#[async_trait]
pub trait TtsModel: Model {
    async fn speak(
        &self,
        cfg: &RouterConfig,
        req: &TtsRequest,
    ) -> Result<TtsResponse, GatewayError>;
}

#[async_trait]
pub trait ImageModel: Model {
    async fn generate_image(
        &self,
        cfg: &RouterConfig,
        req: &ImageRequest,
    ) -> Result<ImageResponse, GatewayError>;
}

#[async_trait]
pub trait VideoModel: Model {
    async fn generate_video(
        &self,
        cfg: &RouterConfig,
        req: &VideoRequest,
    ) -> Result<VideoResponse, GatewayError>;
}
