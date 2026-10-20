//! `bench.toml`: the models under test, each becoming its own single-entry
//! gateway chain so a run exercises exactly one model, with no fallback.

use std::collections::HashMap;

use gateway::types::config::{
    ChainEntry, FallbackChainConfig, GatewayConfig, ModelConfig, RouterConfig,
};
use gateway::{Capability, GatewayBuilder};
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
    /// that one model with no fallback triggers. Built through
    /// [`GatewayBuilder`], so the gateway's own validation rules apply; a failure
    /// names the model.
    pub fn gateway_config_for(&self, m: &ModelSpec) -> anyhow::Result<GatewayConfig> {
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
        GatewayBuilder::new()
            .add_router(&m.router, router)
            .add_model(model)
            .add_chain(chain)
            .build()
            .map_err(|errors| {
                anyhow::anyhow!(
                    "model '{}': invalid gateway config: {}",
                    m.id,
                    errors.join("; ")
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example() -> BenchConfig {
        BenchConfig::from_toml(include_str!("../bench.example.toml")).unwrap()
    }

    #[test]
    fn the_example_config_parses_into_six_models_with_single_entry_chains() {
        let cfg = example();
        assert_eq!(cfg.models.len(), 6);
        let m = &cfg.models[0];
        let g = cfg.gateway_config_for(m).unwrap();
        let chain = &g.chains[&chain_id(m)];
        assert_eq!(chain.models.len(), 1, "no fallback: one model per chain");
        assert!(chain.fallback_triggers.is_empty());
        assert_eq!(g.routers.len(), 1, "only this model's router");
    }

    #[test]
    fn every_example_model_passes_gateway_validation() {
        let cfg = example();
        for m in &cfg.models {
            cfg.gateway_config_for(m)
                .unwrap_or_else(|e| panic!("{}: {e:#}", m.id));
        }
    }

    #[test]
    fn an_invalid_model_is_an_error_naming_the_model() {
        let cfg = example();
        let mut m = cfg.models[0].clone();
        m.url = String::new();
        let err = cfg.gateway_config_for(&m).unwrap_err().to_string();
        assert!(
            err.contains("laya-english"),
            "error must name the model: {err}"
        );
        assert!(
            err.contains("empty URL"),
            "error must carry the reason: {err}"
        );
    }
}
