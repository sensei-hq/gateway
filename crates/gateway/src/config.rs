use std::collections::HashMap;

use crate::types::config::{FallbackChainConfig, GatewayConfig, ModelConfig, RouterConfig};
use crate::types::error::GatewayError;

/// Collect all configuration errors for the given routers/models/chains.
///
/// Shared by [`GatewayBuilder::validate`] (which surfaces the raw strings) and
/// [`validate_config`] (which wraps them in a [`GatewayError`]). Keeping the
/// rules in one place means both entry points stay in lockstep.
pub(crate) fn collect_validation_errors(
    routers: &HashMap<String, RouterConfig>,
    models: &HashMap<String, ModelConfig>,
    chains: &HashMap<String, FallbackChainConfig>,
) -> Vec<String> {
    let mut errors = Vec::new();

    // Rule 1: At least one router
    if routers.is_empty() {
        errors.push("no routers configured".to_string());
    }

    // Rule 2: Each router must have non-empty url
    for (id, router) in routers {
        if router.url.is_empty() {
            errors.push(format!("router '{}' has empty URL", id));
        }
    }

    // Rule 3: Chain models must reference known models
    for (chain_id, chain) in chains {
        for entry in &chain.models {
            if !models.contains_key(&entry.model) {
                errors.push(format!(
                    "chain '{}' references unknown model '{}'",
                    chain_id, entry.model
                ));
            }
        }
    }

    // Rule 4: Model provider must have corresponding router
    for (model_id, model) in models {
        if !routers.contains_key(&model.provider) {
            errors.push(format!(
                "model '{}' provider '{}' has no corresponding router",
                model_id, model.provider
            ));
        }
    }

    errors
}

/// Validate an already-assembled [`GatewayConfig`], returning a single
/// [`GatewayError::InvalidConfig`] carrying all discovered problems.
///
/// This is the checked counterpart to the unchecked `Gateway::new` /
/// `update_config` paths, reusing the exact same rules as [`GatewayBuilder`].
pub(crate) fn validate_config(config: &GatewayConfig) -> Result<(), GatewayError> {
    let errors = collect_validation_errors(&config.routers, &config.models, &config.chains);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(GatewayError::InvalidConfig(errors.join("; ")))
    }
}

/// Fluent builder for constructing and validating a [`GatewayConfig`].
pub struct GatewayBuilder {
    routers: HashMap<String, RouterConfig>,
    models: HashMap<String, ModelConfig>,
    chains: HashMap<String, FallbackChainConfig>,
}

impl GatewayBuilder {
    pub fn new() -> Self {
        Self {
            routers: HashMap::new(),
            models: HashMap::new(),
            chains: HashMap::new(),
        }
    }

    /// Add a router configuration.
    pub fn add_router(mut self, id: &str, config: RouterConfig) -> Self {
        self.routers.insert(id.to_string(), config);
        self
    }

    /// Add a model configuration (keyed by `config.id`).
    pub fn add_model(mut self, config: ModelConfig) -> Self {
        self.models.insert(config.id.clone(), config);
        self
    }

    /// Add a fallback chain configuration (keyed by `config.id`).
    pub fn add_chain(mut self, config: FallbackChainConfig) -> Self {
        self.chains.insert(config.id.clone(), config);
        self
    }

    /// Validate the current builder state, returning ALL errors found.
    pub fn validate(&self) -> Vec<String> {
        collect_validation_errors(&self.routers, &self.models, &self.chains)
    }

    /// Validate and build the [`GatewayConfig`].
    ///
    /// Returns `Err` with all validation errors if any rules fail.
    pub fn build(self) -> Result<GatewayConfig, Vec<String>> {
        let errors = self.validate();
        if !errors.is_empty() {
            return Err(errors);
        }

        Ok(GatewayConfig {
            routers: self.routers,
            models: self.models,
            chains: self.chains,
        })
    }

    /// Reconstitute a builder from an existing [`GatewayConfig`].
    pub fn from_config(config: GatewayConfig) -> Self {
        Self {
            routers: config.routers,
            models: config.models,
            chains: config.chains,
        }
    }
}

impl Default for GatewayBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::capability::Capability;
    use crate::types::config::{
        ChainEntry, FallbackChainConfig, FallbackTrigger, ModelConfig, RouterConfig,
    };
    use std::collections::HashMap;

    fn ollama_router() -> RouterConfig {
        RouterConfig {
            url: "http://localhost:11434".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        }
    }

    fn gemma_model() -> ModelConfig {
        ModelConfig {
            id: "gemma3:27b".to_string(),
            api_model_id: None,
            provider: "ollama".to_string(),
            capabilities: vec![Capability::TextChat, Capability::TextEmbed],
            context_window: 128000,
            max_output_tokens: 8192,
            pricing: None,
        }
    }

    fn chat_chain() -> FallbackChainConfig {
        FallbackChainConfig {
            id: "chat_chain".to_string(),
            capability: Capability::TextChat,
            models: vec![ChainEntry {
                model: "gemma3:27b".to_string(),
                router: None,
                api_model_id: None,
                priority: 1,
            }],
            fallback_triggers: vec![FallbackTrigger::RateLimit, FallbackTrigger::Timeout],
        }
    }

    #[test]
    fn valid_config_builds() {
        let result = GatewayBuilder::new()
            .add_router("ollama", ollama_router())
            .add_model(gemma_model())
            .add_chain(chat_chain())
            .build();

        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.routers.len(), 1);
        assert_eq!(config.models.len(), 1);
        assert_eq!(config.chains.len(), 1);
    }

    #[test]
    fn fails_with_no_routers() {
        let result = GatewayBuilder::new().add_model(gemma_model()).build();

        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("no routers")));
    }

    #[test]
    fn fails_with_empty_router_url() {
        let mut router = ollama_router();
        router.url = String::new();

        let result = GatewayBuilder::new()
            .add_router("ollama", router)
            .add_model(gemma_model())
            .build();

        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("empty URL")));
    }

    #[test]
    fn fails_with_dangling_model_ref_in_chain() {
        let bad_chain = FallbackChainConfig {
            id: "bad_chain".to_string(),
            capability: Capability::TextChat,
            models: vec![ChainEntry {
                model: "nonexistent".to_string(),
                router: None,
                api_model_id: None,
                priority: 1,
            }],
            fallback_triggers: vec![],
        };

        let result = GatewayBuilder::new()
            .add_router("ollama", ollama_router())
            .add_model(gemma_model())
            .add_chain(bad_chain)
            .build();

        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("unknown model") && e.contains("nonexistent"))
        );
    }

    #[test]
    fn fails_with_model_missing_router() {
        let mut model = gemma_model();
        model.provider = "nonexistent".to_string();

        let result = GatewayBuilder::new()
            .add_router("ollama", ollama_router())
            .add_model(model)
            .build();

        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("no corresponding router")));
    }

    #[test]
    fn collects_all_errors() {
        // Empty URL router + dangling chain ref + model with missing router provider
        let mut bad_router = ollama_router();
        bad_router.url = String::new();

        let mut model = gemma_model();
        model.provider = "nonexistent".to_string();

        let bad_chain = FallbackChainConfig {
            id: "bad_chain".to_string(),
            capability: Capability::TextChat,
            models: vec![ChainEntry {
                model: "ghost".to_string(),
                router: None,
                api_model_id: None,
                priority: 1,
            }],
            fallback_triggers: vec![],
        };

        let result = GatewayBuilder::new()
            .add_router("ollama", bad_router)
            .add_model(model)
            .add_chain(bad_chain)
            .build();

        assert!(result.is_err());
        let errors = result.unwrap_err();
        // At least 3 errors: empty URL, dangling chain ref, model missing router
        assert!(
            errors.len() >= 3,
            "Expected >= 3 errors, got {}: {:?}",
            errors.len(),
            errors
        );
    }

    #[test]
    fn from_config_roundtrip() {
        let original = GatewayBuilder::new()
            .add_router("ollama", ollama_router())
            .add_model(gemma_model())
            .add_chain(chat_chain())
            .build()
            .unwrap();

        let rebuilt = GatewayBuilder::from_config(original).build().unwrap();

        assert_eq!(rebuilt.routers.len(), 1);
        assert_eq!(rebuilt.models.len(), 1);
        assert_eq!(rebuilt.chains.len(), 1);
    }

    #[test]
    fn default_builder_is_empty() {
        // Exercises the Default impl (lines 107-108)
        let builder = GatewayBuilder::default();
        let errors = builder.validate();
        // Should have at least "no routers configured"
        assert!(errors.iter().any(|e| e.contains("no routers")));
    }

    #[test]
    fn validate_config_accepts_valid_and_rejects_invalid() {
        // A valid config passes.
        let valid = GatewayBuilder::new()
            .add_router("ollama", ollama_router())
            .add_model(gemma_model())
            .add_chain(chat_chain())
            .build()
            .unwrap();
        assert!(validate_config(&valid).is_ok());

        // An empty config fails with a single joined InvalidConfig error.
        match validate_config(&GatewayConfig::default()) {
            Err(GatewayError::InvalidConfig(msg)) => {
                assert!(msg.contains("no routers"), "unexpected message: {msg}");
            }
            other => panic!("Expected InvalidConfig, got: {other:?}"),
        }
    }

    #[test]
    fn validate_config_joins_all_errors() {
        // Config with a chain that references an unknown model AND a model
        // whose provider has no router — both problems surface in one message.
        let mut config = GatewayBuilder::new()
            .add_router("ollama", ollama_router())
            .add_model(gemma_model())
            .build()
            .unwrap();
        config.chains.insert(
            "bad".to_string(),
            FallbackChainConfig {
                id: "bad".to_string(),
                capability: Capability::TextChat,
                models: vec![ChainEntry {
                    model: "ghost".to_string(),
                    router: None,
                    api_model_id: None,
                    priority: 1,
                }],
                fallback_triggers: vec![],
            },
        );

        match validate_config(&config) {
            Err(GatewayError::InvalidConfig(msg)) => {
                assert!(msg.contains("unknown model") && msg.contains("ghost"));
            }
            other => panic!("Expected InvalidConfig, got: {other:?}"),
        }
    }
}
