pub mod adapters;
pub mod budget;
pub mod circuit_breaker;
pub mod config;
// Boundary conversions are consumed by the engine once it switches to
// capability dispatch (Phase 4); allow dead_code during the bridge.
#[allow(dead_code)]
mod dispatch;
pub mod engine;
pub mod purpose;
pub mod selection;
pub mod store;
pub mod types;

pub use config::GatewayBuilder;
pub use engine::Gateway;
pub use purpose::{ModelHint, Purpose, PurposeBuilder, PurposeResult, StepBuilder, StepInput};
pub use types::capability::Capability;
pub use types::error::GatewayError;
pub use types::request::{InferenceRequest, InferenceResponse};
