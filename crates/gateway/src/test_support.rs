//! Crate-internal test fixtures shared across more than one module's test
//! suite. Kept intentionally minimal — most modules still own their local
//! fixtures (mirroring the per-file style used throughout this crate); only
//! data literals that are genuinely identical across files live here, so a
//! change to the shared fixture can't drift between copies.

use crate::types::capability::Capability;
use crate::types::config::{ChainEntry, FallbackChainConfig, FallbackTrigger};

/// The standard single-entry "chat_chain" -> "noop" fallback chain, used by
/// both `engine.rs`'s and `purpose.rs`'s noop-adapter gateway config fixtures.
pub(crate) fn noop_chat_chain() -> FallbackChainConfig {
    FallbackChainConfig {
        id: "chat_chain".to_string(),
        capability: Capability::TextChat,
        models: vec![ChainEntry {
            model: "noop".to_string(),
            router: Some("noop".to_string()),
            api_model_id: None,
            priority: 1,
        }],
        fallback_triggers: vec![
            FallbackTrigger::RateLimit,
            FallbackTrigger::Timeout,
            FallbackTrigger::ProviderError,
        ],
    }
}
