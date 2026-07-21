//! Chain viability pruning: drop chain candidates that are *permanently*
//! unavailable (disabled/unknown router, unknown model, or a caller-judged
//! `Unavailable` such as a cloud router with no API key) and return a structured
//! report of what was dropped and why. Candidates that are merely still
//! provisioning (`Pending`) are kept — they become available when the
//! supervisor registers them. Pure config logic: no I/O, no adapter lookups, no
//! Keychain (the caller's `judge` encodes key presence and the like).

use crate::types::config::GatewayConfig;

/// The caller's verdict for a `(router, model)` pair the library can't judge
/// from config alone (e.g. "cloud router has no API key" — a fact the library
/// never touches).
pub enum Availability {
    Available,
    Pending,
    Unavailable { reason: String },
}

/// One dropped chain candidate and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainWarning {
    pub chain: String,
    pub router: String,
    pub model: String,
    pub reason: String,
}

/// Prune permanently-unavailable candidates from every chain in `config`,
/// returning the warnings. Config-only signals — unknown model, unknown router,
/// disabled router — are treated as `Unavailable` without consulting `judge`;
/// everything else is passed to `judge`. `Pending`/`Available` candidates are
/// kept. A chain that empties is **retained** (an honest `NoCandidates` at
/// execute time) rather than deleted. The `(router, model)` resolution mirrors
/// the selection engine: an entry's explicit `router` wins, else the model's
/// `provider`.
pub fn prune_unavailable(
    config: &mut GatewayConfig,
    judge: impl Fn(&str, &str) -> Availability,
) -> Vec<ChainWarning> {
    let mut warnings = Vec::new();
    // Split the borrow: mutate `chains` while reading `routers`/`models`.
    let GatewayConfig {
        routers,
        models,
        chains,
        ..
    } = config;

    for (chain_id, chain) in chains.iter_mut() {
        chain.models.retain(|entry| {
            let model = &entry.model;

            // Unknown model → permanently unavailable (config-only).
            let Some(model_cfg) = models.get(model) else {
                warnings.push(ChainWarning {
                    chain: chain_id.clone(),
                    router: entry.router.clone().unwrap_or_else(|| "unknown".into()),
                    model: model.clone(),
                    reason: format!("unknown model '{model}'"),
                });
                return false;
            };

            // Effective router: entry override, else the model's provider.
            let router = entry
                .router
                .clone()
                .unwrap_or_else(|| model_cfg.provider.clone());

            // Unknown router → permanently unavailable (config-only).
            let Some(router_cfg) = routers.get(&router) else {
                warnings.push(ChainWarning {
                    chain: chain_id.clone(),
                    router: router.clone(),
                    model: model.clone(),
                    reason: format!("unknown router '{router}'"),
                });
                return false;
            };

            // Disabled router → permanently unavailable (config-only).
            if !router_cfg.enabled {
                warnings.push(ChainWarning {
                    chain: chain_id.clone(),
                    router,
                    model: model.clone(),
                    reason: "router disabled".into(),
                });
                return false;
            }

            // Everything else is the caller's call.
            match judge(&router, model) {
                Availability::Available | Availability::Pending => true,
                Availability::Unavailable { reason } => {
                    warnings.push(ChainWarning {
                        chain: chain_id.clone(),
                        router,
                        model: model.clone(),
                        reason,
                    });
                    false
                }
            }
        });
    }

    warnings
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::capability::Capability;
    use crate::types::config::{
        ChainEntry, FallbackChainConfig, ModelConfig, RouterConfig,
    };
    use std::collections::HashMap;

    fn router(enabled: bool) -> RouterConfig {
        RouterConfig {
            url: "https://example".into(),
            api_key_env: None,
            api_key: None,
            enabled,
            timeout_ms: None,
            headers: HashMap::new(),
        }
    }

    fn model(id: &str, provider: &str) -> ModelConfig {
        ModelConfig {
            id: id.into(),
            api_model_id: None,
            provider: provider.into(),
            capabilities: vec![Capability::TextChat],
            context_window: 8192,
            max_output_tokens: 1024,
            pricing: None,
        }
    }

    fn entry(model: &str, router: Option<&str>) -> ChainEntry {
        ChainEntry {
            model: model.into(),
            router: router.map(|s| s.into()),
            api_model_id: None,
            priority: 1,
        }
    }

    fn config_with(
        routers: Vec<(&str, RouterConfig)>,
        models: Vec<ModelConfig>,
        chain_entries: Vec<ChainEntry>,
    ) -> GatewayConfig {
        GatewayConfig {
            routers: routers.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
            models: models.into_iter().map(|m| (m.id.clone(), m)).collect(),
            chains: HashMap::from([(
                "c".to_string(),
                FallbackChainConfig {
                    id: "c".into(),
                    capability: Capability::TextChat,
                    models: chain_entries,
                    fallback_triggers: vec![],
                },
            )]),
            ..Default::default()
        }
    }

    fn models_left(cfg: &GatewayConfig) -> Vec<String> {
        cfg.chains["c"].models.iter().map(|e| e.model.clone()).collect()
    }

    #[test]
    fn disabled_router_candidate_is_dropped_with_warning() {
        let mut cfg = config_with(
            vec![("anthropic", router(false))],
            vec![model("claude", "anthropic")],
            vec![entry("claude", None)],
        );
        let w = prune_unavailable(&mut cfg, |_, _| Availability::Available);
        assert!(models_left(&cfg).is_empty());
        assert_eq!(w.len(), 1);
        assert_eq!(
            w[0],
            ChainWarning {
                chain: "c".into(),
                router: "anthropic".into(),
                model: "claude".into(),
                reason: "router disabled".into(),
            }
        );
    }

    #[test]
    fn unknown_router_candidate_is_dropped() {
        // Model exists but names a provider with no router config.
        let mut cfg = config_with(vec![], vec![model("m", "ghost")], vec![entry("m", None)]);
        let w = prune_unavailable(&mut cfg, |_, _| Availability::Available);
        assert!(models_left(&cfg).is_empty());
        assert_eq!(w.len(), 1);
        assert_eq!(w[0].router, "ghost");
        assert_eq!(w[0].reason, "unknown router 'ghost'");
    }

    #[test]
    fn unknown_model_candidate_is_dropped() {
        let mut cfg = config_with(
            vec![("anthropic", router(true))],
            vec![],
            vec![entry("nope", Some("anthropic"))],
        );
        let w = prune_unavailable(&mut cfg, |_, _| Availability::Available);
        assert!(models_left(&cfg).is_empty());
        assert_eq!(w.len(), 1);
        assert_eq!(w[0].model, "nope");
        assert_eq!(w[0].router, "anthropic"); // entry.router used for the warning
        assert_eq!(w[0].reason, "unknown model 'nope'");
    }

    #[test]
    fn judge_unavailable_is_dropped_with_judge_reason() {
        let mut cfg = config_with(
            vec![("anthropic", router(true))],
            vec![model("claude", "anthropic")],
            vec![entry("claude", None)],
        );
        let w = prune_unavailable(&mut cfg, |r, _| {
            if r == "anthropic" {
                Availability::Unavailable { reason: "no api key".into() }
            } else {
                Availability::Available
            }
        });
        assert!(models_left(&cfg).is_empty());
        assert_eq!(w.len(), 1);
        assert_eq!(w[0].reason, "no api key");
    }

    #[test]
    fn judge_pending_is_kept() {
        let mut cfg = config_with(
            vec![("local", router(true))],
            vec![model("gemma", "local")],
            vec![entry("gemma", None)],
        );
        let w = prune_unavailable(&mut cfg, |_, _| Availability::Pending);
        assert_eq!(models_left(&cfg), vec!["gemma".to_string()]);
        assert!(w.is_empty(), "provisioning candidate must be kept");
    }

    #[test]
    fn judge_available_is_kept() {
        let mut cfg = config_with(
            vec![("openai", router(true))],
            vec![model("gpt", "openai")],
            vec![entry("gpt", None)],
        );
        let w = prune_unavailable(&mut cfg, |_, _| Availability::Available);
        assert_eq!(models_left(&cfg), vec!["gpt".to_string()]);
        assert!(w.is_empty());
    }

    #[test]
    fn entry_router_overrides_model_provider() {
        // Model's provider is "openai" but the chain entry pins router "azure".
        // The warning reports "azure" — proving effective-router resolution used
        // the entry override, not the model's provider (the router the warning
        // records is the exact one handed to the judge).
        let mut cfg = config_with(
            vec![("azure", router(true)), ("openai", router(true))],
            vec![model("gpt", "openai")],
            vec![entry("gpt", Some("azure"))],
        );
        let w = prune_unavailable(&mut cfg, |_, _| Availability::Unavailable {
            reason: "x".into(),
        });
        assert_eq!(w.len(), 1);
        assert_eq!(w[0].router, "azure");
    }

    #[test]
    fn emptied_chain_is_retained_not_deleted() {
        let mut cfg = config_with(
            vec![("anthropic", router(false))],
            vec![model("claude", "anthropic")],
            vec![entry("claude", None)],
        );
        prune_unavailable(&mut cfg, |_, _| Availability::Available);
        assert!(cfg.chains.contains_key("c"), "empty chain must be retained");
        assert!(cfg.chains["c"].models.is_empty());
    }

    #[test]
    fn mixed_chain_keeps_available_drops_unavailable() {
        let mut cfg = config_with(
            vec![("openai", router(true)), ("anthropic", router(true))],
            vec![model("gpt", "openai"), model("claude", "anthropic")],
            vec![entry("claude", None), entry("gpt", None)],
        );
        let w = prune_unavailable(&mut cfg, |r, _| {
            if r == "anthropic" {
                Availability::Unavailable { reason: "no api key".into() }
            } else {
                Availability::Available
            }
        });
        assert_eq!(models_left(&cfg), vec!["gpt".to_string()]);
        assert_eq!(w.len(), 1);
        assert_eq!(w[0].model, "claude");
    }
}
