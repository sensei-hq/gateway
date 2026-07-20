pub mod anthropic;
pub mod async_job;
pub mod base;
pub mod bedrock;
pub mod fal;
pub mod flux;
pub mod gemini;
pub mod grok;
pub mod huggingface;
pub mod kling;
pub mod luma;
pub mod noop;
pub mod ollama;
pub mod openai;
pub mod openai_compat;
pub mod recraft;
pub mod replicate;
pub mod runway;
pub mod stability;
pub mod together;

// The capability traits + registry now live in `kernel`. Re-export them under
// their historical `gateway::adapters::…` paths so both this crate's adapters
// (`crate::adapters::…`) and downstream consumers compile unchanged.
pub use kernel::adapters::capability;
pub use kernel::adapters::capability::{
    ChatModel, EmbedModel, ImageModel, Model, SttModel, TtsModel, VideoModel,
};
pub use kernel::adapters::{AdapterRegistry, RegisterInto};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::noop::NoopAdapter;
    use std::sync::Arc;

    #[tokio::test]
    async fn registry_registers_and_lists_via_reexport() {
        // Exercises the re-exported registry with a real gateway adapter, so the
        // shim (not just the kernel copy) is covered.
        let reg = AdapterRegistry::new();
        reg.register(Arc::new(NoopAdapter)).await;
        assert!(reg.chat("noop").await.is_some());
        assert!(reg.chat("nonexistent").await.is_none());
        assert_eq!(reg.list().await, vec!["noop".to_string()]);
    }
}
