pub mod noop;

// Capability traits + registry live in `kernel`; re-export under the historical
// `gateway::adapters::…` paths so internal code + downstream compile unchanged.
pub use kernel::adapters::capability;
pub use kernel::adapters::capability::{
    ChatModel, EmbedModel, ImageModel, Model, SttModel, TtsModel, VideoModel,
};
pub use kernel::adapters::{AdapterRegistry, RegisterInto};

// Cloud provider adapters live in the `cloud-providers` crate, compiled only
// with the (default) `cloud` feature. Re-export them under their historical
// `gateway::adapters::<provider>::…` paths so cloud consumers are unaffected.
#[cfg(feature = "cloud")]
pub use cloud_providers::{
    anthropic, async_job, base, bedrock, fal, flux, gemini, grok, huggingface,
    kling, luma, ollama, openai, openai_compat, recraft, replicate, runway,
    stability, together,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::noop::NoopAdapter;
    use std::sync::Arc;

    #[tokio::test]
    async fn registry_registers_and_lists_via_reexport() {
        let reg = AdapterRegistry::new();
        reg.register(Arc::new(NoopAdapter)).await;
        assert!(reg.chat("noop").await.is_some());
        assert!(reg.chat("nonexistent").await.is_none());
        assert_eq!(reg.list().await, vec!["noop".to_string()]);
    }
}
