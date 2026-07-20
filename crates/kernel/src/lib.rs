//! `sensei-kernel` — the shared vocabulary of the sensei gateway: config, IO,
//! cost, trace, and error types, the capability traits, and the adapter
//! registry. This crate depends on nothing else in the workspace; every other
//! gateway crate depends on it.

pub mod adapters;
pub mod registry;
pub mod types;

pub use adapters::capability::{
    ChatModel, EmbedModel, ImageModel, Model, SttModel, TtsModel, VideoModel,
};
pub use adapters::{AdapterRegistry, RegisterInto};
pub use registry::{ModelEntry, ModelFormat, ModelResolver, ModelSource, ResolveError};
pub use types::capability::Capability;
pub use types::error::GatewayError;
pub use types::request::{InferenceRequest, InferenceResponse};
