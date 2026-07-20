//! Compile-time guard: every `gateway::…` path that downstream consumers (and
//! `gateway-embedded`) depend on must keep resolving after the kernel split.
//! Compiling this file IS the assertion.
#![allow(unused_imports)]

use gateway::adapters::capability::{
    ChatModel, EmbedModel, ImageModel, Model, SttModel, TtsModel, VideoModel,
};
use gateway::adapters::{AdapterRegistry, RegisterInto};
use gateway::types::config::RouterConfig;
use gateway::types::cost::{Cost, CostEstimate, TokenUsage};
use gateway::types::error::GatewayError;
use gateway::types::io::{ChatRequest, ChatResponse, EmbedRequest, EmbedResponse};
use gateway::types::request::{Message, MessageRole, StreamChunk};
use gateway::types::trace::Attempt;
use gateway::{Capability, InferenceRequest, InferenceResponse};

#[test]
fn reexport_paths_resolve() {
    // The `use` block above proves the paths resolve; nothing to assert at runtime.
}
