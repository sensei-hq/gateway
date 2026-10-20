//! `bench.toml`: the models under test, each becoming its own single-entry
//! gateway chain so a run exercises exactly one model, with no fallback.

use std::collections::HashMap;

use gateway::Capability;
use gateway::types::config::{
    ChainEntry, FallbackChainConfig, GatewayConfig, ModelConfig, RouterConfig,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct BenchConfig {
    pub models: Vec<ModelSpec>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelSpec {
    pub id: String,
    pub router: String,
    pub url: String,
    pub api_model_id: String,
    pub context_window: u32,
    #[serde(default)]
    pub api_key_env: Option<String>,
}

/// The id of the one-model chain built for `m`.
pub fn chain_id(m: &ModelSpec) -> String {
    format!("bench-{}", m.id)
}

impl BenchConfig {
    pub fn from_toml(s: &str) -> anyhow::Result<Self> {
        Ok(toml::from_str(s)?)
    }

    /// A gateway config holding only `m`: its router, the model, and a chain of
    /// that one model with no fallback triggers.
    pub fn gateway_config_for(&self, m: &ModelSpec) -> GatewayConfig {
        let router = RouterConfig {
            url: m.url.clone(),
            api_key_env: m.api_key_env.clone(),
            api_key: None,
            enabled: true,
            timeout_ms: Some(120_000),
            headers: HashMap::new(),
        };
        let model = ModelConfig {
            id: m.id.clone(),
            api_model_id: Some(m.api_model_id.clone()),
            provider: m.router.clone(),
            family: None,
            capabilities: vec![Capability::Decision],
            context_window: m.context_window,
            max_output_tokens: 1,
            pricing: None,
            catalog: None,
        };
        let chain = FallbackChainConfig {
            id: chain_id(m),
            capability: Capability::Decision,
            models: vec![ChainEntry {
                model: m.id.clone(),
                router: Some(m.router.clone()),
                api_model_id: None,
                priority: 1,
            }],
            fallback_triggers: vec![],
        };
        GatewayConfig {
            routers: HashMap::from([(m.router.clone(), router)]),
            models: HashMap::from([(m.id.clone(), model)]),
            chains: HashMap::from([(chain_id(m), chain)]),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gateway::FacadeBuilder;

    #[test]
    fn the_example_config_parses_into_six_models_with_single_entry_chains() {
        let cfg = BenchConfig::from_toml(include_str!("../bench.example.toml")).unwrap();
        assert_eq!(cfg.models.len(), 6);
        let m = &cfg.models[0];
        let g = cfg.gateway_config_for(m);
        let chain = &g.chains[&chain_id(m)];
        assert_eq!(chain.models.len(), 1, "no fallback: one model per chain");
        assert!(chain.fallback_triggers.is_empty());
        assert_eq!(g.routers.len(), 1, "only this model's router");
    }

    #[tokio::test]
    async fn every_example_model_yields_a_config_the_facade_builds() {
        let cfg = BenchConfig::from_toml(include_str!("../bench.example.toml")).unwrap();
        for m in &cfg.models {
            let g = cfg.gateway_config_for(m);
            let _facade = FacadeBuilder::new(g).build().await;
        }
    }
}
