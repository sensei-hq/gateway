use crate::circuit_breaker::CircuitBreakerManager;
use crate::types::capability::Capability;
use crate::types::config::{FallbackChainConfig, GatewayConfig, ModelConfig, RouterConfig};
use crate::types::cost::CostEstimate;

/// Criteria used to resolve which model(s) to try.
#[derive(Debug, Clone)]
pub struct SelectionCriteria {
    pub capability: Capability,
    pub model: Option<String>,
    pub router: Option<String>,
    pub chain: Option<String>,
    pub budget: Option<f64>,
    pub input_tokens: Option<u32>,
}

/// A model that passed all validation checks and is ready for execution.
#[derive(Debug, Clone)]
pub struct SelectedModel {
    pub model: String,
    pub router: String,
    pub router_config: RouterConfig,
    pub model_config: ModelConfig,
    pub api_model_id: String,
    pub priority: u8,
    pub cost_estimate: Option<CostEstimate>,
}

/// A candidate that was considered but rejected during validation.
#[derive(Debug, Clone)]
pub struct SkippedCandidate {
    pub model: String,
    pub router: String,
    pub reason: String,
}

/// The result of model selection, containing the chosen model plus diagnostics.
#[derive(Debug)]
pub struct SelectionResult {
    pub selected: Option<SelectedModel>,
    pub all_candidates: Vec<SelectedModel>,
    pub skipped: Vec<SkippedCandidate>,
    pub chain: Option<FallbackChainConfig>,
}

/// Resolves which model(s) to use for a given request via 3-tier resolution
/// (direct, named chain, capability) with a validation pipeline per candidate.
pub struct ModelSelectionService<'a> {
    config: &'a GatewayConfig,
    circuit_breaker: &'a CircuitBreakerManager,
}

impl<'a> ModelSelectionService<'a> {
    pub fn new(config: &'a GatewayConfig, circuit_breaker: &'a CircuitBreakerManager) -> Self {
        Self {
            config,
            circuit_breaker,
        }
    }

    /// Select the first valid candidate.
    pub fn select(&self, criteria: &SelectionCriteria) -> SelectionResult {
        let mut result = self.resolve_candidates(criteria);
        result.selected = result.all_candidates.first().cloned();
        result
    }

    /// Select all valid candidates (for fallback chains).
    pub fn select_all(&self, criteria: &SelectionCriteria) -> SelectionResult {
        let mut result = self.resolve_candidates(criteria);
        result.selected = result.all_candidates.first().cloned();
        result
    }

    /// Estimate the cost for a model given the criteria.
    fn estimate_cost(
        &self,
        model_config: &ModelConfig,
        criteria: &SelectionCriteria,
    ) -> Option<CostEstimate> {
        let pricing = model_config.pricing.as_ref()?;
        let input_tokens = criteria.input_tokens.unwrap_or(0);
        let max_output_tokens = model_config.max_output_tokens;

        let input_cost = input_tokens as f64 * pricing.input_per_1k / 1000.0;
        let output_cost = max_output_tokens as f64 * pricing.output_per_1k / 1000.0;
        let estimated = input_cost + output_cost;

        Some(CostEstimate {
            estimated,
            minimum: input_cost, // minimum: only input, no output
            maximum: estimated,  // maximum: full output budget used
            currency: "USD".to_string(),
            model: model_config.id.clone(),
        })
    }

    /// Core resolution: determine candidates based on the 3-tier strategy,
    /// then validate each one through the pipeline.
    fn resolve_candidates(&self, criteria: &SelectionCriteria) -> SelectionResult {
        // Tier 1: Direct (router + model specified)
        if criteria.router.is_some() || (criteria.model.is_some() && criteria.chain.is_none()) {
            return self.resolve_direct(criteria);
        }

        // Tier 2: Named chain
        if let Some(chain_name) = &criteria.chain {
            if let Some(chain) = self.config.chains.get(chain_name) {
                return self.resolve_chain(chain, criteria);
            }
            return SelectionResult {
                selected: None,
                all_candidates: vec![],
                skipped: vec![],
                chain: None,
            };
        }

        // Tier 3: Capability — find chain matching the capability
        self.resolve_by_capability(criteria)
    }

    /// Tier 1: Direct resolution — validate a single router+model pair.
    fn resolve_direct(&self, criteria: &SelectionCriteria) -> SelectionResult {
        let mut skipped = Vec::new();

        let router_name = criteria.router.clone().unwrap_or_default();
        let model_name = criteria.model.clone().unwrap_or_default();

        // Validate router exists and is enabled
        let router_config = match self.config.routers.get(&router_name) {
            Some(rc) => rc,
            None => {
                skipped.push(SkippedCandidate {
                    model: model_name.clone(),
                    router: router_name.clone(),
                    reason: "router not found".to_string(),
                });
                return SelectionResult {
                    selected: None,
                    all_candidates: vec![],
                    skipped,
                    chain: None,
                };
            }
        };

        if !router_config.enabled {
            skipped.push(SkippedCandidate {
                model: model_name.clone(),
                router: router_name.clone(),
                reason: "router disabled".to_string(),
            });
            return SelectionResult {
                selected: None,
                all_candidates: vec![],
                skipped,
                chain: None,
            };
        }

        // Validate model exists and supports capability
        let model_config = match self.config.models.get(&model_name) {
            Some(mc) => mc,
            None => {
                skipped.push(SkippedCandidate {
                    model: model_name.clone(),
                    router: router_name.clone(),
                    reason: "model not found".to_string(),
                });
                return SelectionResult {
                    selected: None,
                    all_candidates: vec![],
                    skipped,
                    chain: None,
                };
            }
        };

        if !model_config.capabilities.contains(&criteria.capability) {
            skipped.push(SkippedCandidate {
                model: model_name.clone(),
                router: router_name.clone(),
                reason: format!("does not support {:?}", criteria.capability),
            });
            return SelectionResult {
                selected: None,
                all_candidates: vec![],
                skipped,
                chain: None,
            };
        }

        // Circuit breaker check
        let endpoint = format!("{}:{}", router_name, model_name);
        if !self.circuit_breaker.can_execute(&endpoint) {
            skipped.push(SkippedCandidate {
                model: model_name.clone(),
                router: router_name.clone(),
                reason: "circuit breaker open".to_string(),
            });
            return SelectionResult {
                selected: None,
                all_candidates: vec![],
                skipped,
                chain: None,
            };
        }

        // Cost estimation and budget check
        let cost_estimate = self.estimate_cost(model_config, criteria);
        if let (Some(budget), Some(est)) = (criteria.budget, &cost_estimate)
            && est.estimated > budget
        {
            skipped.push(SkippedCandidate {
                model: model_name.clone(),
                router: router_name.clone(),
                reason: format!(
                    "over budget (estimated {:.4}, budget {:.4})",
                    est.estimated, budget
                ),
            });
            return SelectionResult {
                selected: None,
                all_candidates: vec![],
                skipped,
                chain: None,
            };
        }

        let api_model_id = model_config
            .api_model_id
            .clone()
            .unwrap_or_else(|| model_name.clone());

        let selected = SelectedModel {
            model: model_name,
            router: router_name,
            router_config: router_config.clone(),
            model_config: model_config.clone(),
            api_model_id,
            priority: 1,
            cost_estimate,
        };

        SelectionResult {
            selected: None, // filled by caller
            all_candidates: vec![selected],
            skipped,
            chain: None,
        }
    }

    /// Tier 2/3: Walk chain entries sorted by priority, validating each.
    fn resolve_chain(
        &self,
        chain: &FallbackChainConfig,
        criteria: &SelectionCriteria,
    ) -> SelectionResult {
        let mut all_candidates = Vec::new();
        let mut skipped = Vec::new();

        // Sort entries by priority
        let mut entries = chain.models.clone();
        entries.sort_by_key(|e| e.priority);

        for entry in &entries {
            let model_name = &entry.model;

            // Look up the model config
            let model_config = match self.config.models.get(model_name) {
                Some(mc) => mc,
                None => {
                    let router_name = entry
                        .router
                        .clone()
                        .unwrap_or_else(|| "unknown".to_string());
                    skipped.push(SkippedCandidate {
                        model: model_name.clone(),
                        router: router_name,
                        reason: "model not found".to_string(),
                    });
                    continue;
                }
            };

            // Resolve router: chain entry router, else model's provider
            let router_name = entry
                .router
                .clone()
                .unwrap_or_else(|| model_config.provider.clone());

            // Validate router exists and is enabled
            let router_config = match self.config.routers.get(&router_name) {
                Some(rc) => rc,
                None => {
                    skipped.push(SkippedCandidate {
                        model: model_name.clone(),
                        router: router_name,
                        reason: "router not found".to_string(),
                    });
                    continue;
                }
            };

            if !router_config.enabled {
                skipped.push(SkippedCandidate {
                    model: model_name.clone(),
                    router: router_name,
                    reason: "router disabled".to_string(),
                });
                continue;
            }

            // Validate capability
            if !model_config.capabilities.contains(&criteria.capability) {
                skipped.push(SkippedCandidate {
                    model: model_name.clone(),
                    router: router_name,
                    reason: format!("does not support {:?}", criteria.capability),
                });
                continue;
            }

            // Circuit breaker check
            let endpoint = format!("{}:{}", router_name, model_name);
            if !self.circuit_breaker.can_execute(&endpoint) {
                skipped.push(SkippedCandidate {
                    model: model_name.clone(),
                    router: router_name,
                    reason: "circuit breaker open".to_string(),
                });
                continue;
            }

            // Cost estimation and budget check
            let cost_estimate = self.estimate_cost(model_config, criteria);
            if let (Some(budget), Some(est)) = (criteria.budget, &cost_estimate)
                && est.estimated > budget
            {
                skipped.push(SkippedCandidate {
                    model: model_name.clone(),
                    router: router_name,
                    reason: format!(
                        "over budget (estimated {:.4}, budget {:.4})",
                        est.estimated, budget
                    ),
                });
                continue;
            }

            // Resolve API model ID: chain entry override, else model config, else model id
            let api_model_id = entry
                .api_model_id
                .clone()
                .or_else(|| model_config.api_model_id.clone())
                .unwrap_or_else(|| model_name.clone());

            all_candidates.push(SelectedModel {
                model: model_name.clone(),
                router: router_name,
                router_config: router_config.clone(),
                model_config: model_config.clone(),
                api_model_id,
                priority: entry.priority,
                cost_estimate,
            });
        }

        SelectionResult {
            selected: None, // filled by caller
            all_candidates,
            skipped,
            chain: Some(chain.clone()),
        }
    }

    /// Tier 3: resolve by capability when the caller pinned neither a model
    /// nor a chain.
    ///
    /// Several chains can share a capability (e.g. `classify`, `reasoning`,
    /// `summarize` are all `TextChat`). `config.chains` is a `HashMap`, whose
    /// iteration order is not stable across runs — picking "the first match"
    /// would be non-deterministic (#80). Instead, pick the lowest chain id
    /// among the matches: a stable, if arbitrary, default. Callers that need a
    /// specific chain should pin it by name (tier 2) rather than rely on this.
    fn resolve_by_capability(&self, criteria: &SelectionCriteria) -> SelectionResult {
        let chosen = self
            .config
            .chains
            .values()
            .filter(|c| c.capability == criteria.capability)
            .min_by(|a, b| a.id.cmp(&b.id));

        match chosen {
            Some(chain) => self.resolve_chain(chain, criteria),
            None => SelectionResult {
                selected: None,
                all_candidates: vec![],
                skipped: vec![],
                chain: None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::circuit_breaker::{CircuitBreakerConfig, CircuitBreakerManager};
    use crate::types::config::{
        ChainEntry, FallbackChainConfig, FallbackTrigger, ModelConfig, ModelPricing, RouterConfig,
    };
    use std::collections::HashMap;
    use std::time::Duration;

    fn test_config() -> GatewayConfig {
        let mut routers = HashMap::new();
        routers.insert(
            "ollama".to_string(),
            RouterConfig {
                url: "http://localhost:11434".to_string(),
                api_key_env: None,
                api_key: None,
                enabled: true,
                timeout_ms: None,
                headers: HashMap::new(),
            },
        );
        routers.insert(
            "anthropic".to_string(),
            RouterConfig {
                url: "https://api.anthropic.com".to_string(),
                api_key_env: Some("ANTHROPIC_API_KEY".to_string()),
                api_key: None,
                enabled: true,
                timeout_ms: None,
                headers: HashMap::new(),
            },
        );

        let mut models = HashMap::new();
        models.insert(
            "gemma3:27b".to_string(),
            ModelConfig {
                id: "gemma3:27b".to_string(),
                api_model_id: None,
                provider: "ollama".to_string(),
                capabilities: vec![
                    Capability::TextChat,
                    Capability::TextComplete,
                    Capability::TextEmbed,
                ],
                context_window: 128000,
                max_output_tokens: 8192,
                pricing: None,
            },
        );
        models.insert(
            "all-minilm".to_string(),
            ModelConfig {
                id: "all-minilm".to_string(),
                api_model_id: None,
                provider: "ollama".to_string(),
                capabilities: vec![Capability::TextEmbed],
                context_window: 512,
                max_output_tokens: 0,
                pricing: None,
            },
        );
        models.insert(
            "claude-haiku".to_string(),
            ModelConfig {
                id: "claude-haiku".to_string(),
                api_model_id: Some("claude-haiku-4-5-20251001".to_string()),
                provider: "anthropic".to_string(),
                capabilities: vec![Capability::TextChat],
                context_window: 200000,
                max_output_tokens: 8192,
                pricing: Some(ModelPricing {
                    input_per_1k: 0.0008,
                    output_per_1k: 0.004,
                    per_request: None,
                }),
            },
        );

        let mut chains = HashMap::new();
        chains.insert(
            "embed_chain".to_string(),
            FallbackChainConfig {
                id: "embed_chain".to_string(),
                capability: Capability::TextEmbed,
                models: vec![ChainEntry {
                    model: "all-minilm".to_string(),
                    router: None,
                    api_model_id: None,
                    priority: 1,
                }],
                fallback_triggers: vec![],
            },
        );
        chains.insert(
            "chat_chain".to_string(),
            FallbackChainConfig {
                id: "chat_chain".to_string(),
                capability: Capability::TextChat,
                models: vec![
                    ChainEntry {
                        model: "gemma3:27b".to_string(),
                        router: None,
                        api_model_id: None,
                        priority: 1,
                    },
                    ChainEntry {
                        model: "claude-haiku".to_string(),
                        router: None,
                        api_model_id: None,
                        priority: 2,
                    },
                ],
                fallback_triggers: vec![FallbackTrigger::Timeout, FallbackTrigger::ProviderError],
            },
        );

        GatewayConfig {
            routers,
            models,
            chains,
            constraints: Default::default(),
        }
    }

    fn test_cb() -> CircuitBreakerManager {
        CircuitBreakerManager::new(CircuitBreakerConfig {
            threshold: 5,
            timeout: Duration::from_secs(300),
            half_open_max_requests: 3,
        })
    }

    #[test]
    fn tier1_direct_selection() {
        let config = test_config();
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("gemma3:27b".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
        });

        assert!(result.selected.is_some());
        let selected = result.selected.unwrap();
        assert_eq!(selected.model, "gemma3:27b");
        assert_eq!(selected.router, "ollama");
    }

    #[test]
    fn tier1_direct_unknown_router() {
        let config = test_config();
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("gemma3:27b".to_string()),
            router: Some("nonexistent".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(result.skipped[0].reason.contains("router not found"));
    }

    #[test]
    fn tier2_chain_selection() {
        let config = test_config();
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: None,
            input_tokens: None,
        });

        assert_eq!(result.all_candidates.len(), 2);
        assert_eq!(result.all_candidates[0].model, "gemma3:27b");
        assert_eq!(result.all_candidates[0].priority, 1);
        assert_eq!(result.all_candidates[1].model, "claude-haiku");
        assert_eq!(result.all_candidates[1].priority, 2);
        assert!(result.chain.is_some());
    }

    #[test]
    fn tier3_capability_selection() {
        let config = test_config();
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextEmbed,
            model: None,
            router: None,
            chain: None,
            budget: None,
            input_tokens: None,
        });

        assert!(result.selected.is_some());
        let selected = result.selected.unwrap();
        assert_eq!(selected.model, "all-minilm");
        assert_eq!(selected.router, "ollama");
    }

    #[test]
    fn tier3_capability_is_deterministic_lowest_chain_id() {
        // Two chains share TextChat; tier-3 must pick the lowest id ("aaa_chain")
        // every time, not whatever the HashMap happens to yield first (#80).
        let mut config = test_config();
        config.chains.insert(
            "aaa_chain".to_string(),
            FallbackChainConfig {
                id: "aaa_chain".to_string(),
                capability: Capability::TextChat,
                models: vec![ChainEntry {
                    model: "claude-haiku".to_string(),
                    router: None,
                    api_model_id: None,
                    priority: 1,
                }],
                fallback_triggers: vec![],
            },
        );
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);
        // Run several times: a HashMap-order bug would flake; min_by is stable.
        for _ in 0..10 {
            let result = svc.select(&SelectionCriteria {
                capability: Capability::TextChat,
                model: None,
                router: None,
                chain: None,
                budget: None,
                input_tokens: None,
            });
            assert_eq!(result.chain.as_ref().unwrap().id, "aaa_chain");
        }
    }

    #[test]
    fn skips_disabled_router() {
        let mut config = test_config();
        config.routers.get_mut("ollama").unwrap().enabled = false;
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: None,
            input_tokens: None,
        });

        assert!(result.selected.is_some());
        let selected = result.selected.unwrap();
        assert_eq!(selected.model, "claude-haiku");
        assert_eq!(result.skipped.len(), 1);
        assert!(result.skipped[0].reason.contains("router disabled"));
        assert_eq!(result.skipped[0].model, "gemma3:27b");
    }

    #[test]
    fn skips_wrong_capability() {
        let config = test_config();
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::AudioTranscribe,
            model: Some("gemma3:27b".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(result.skipped[0].reason.contains("does not support"));
    }

    #[test]
    fn skips_circuit_breaker_open() {
        let config = test_config();
        let cb = test_cb();

        // Open the circuit breaker for ollama:gemma3:27b
        let endpoint = "ollama:gemma3:27b";
        cb.can_execute(endpoint); // initialize
        for _ in 0..5 {
            cb.record_failure(endpoint);
        }
        assert!(!cb.can_execute(endpoint)); // confirm open

        let svc = ModelSelectionService::new(&config, &cb);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: None,
            input_tokens: None,
        });

        assert!(result.selected.is_some());
        let selected = result.selected.unwrap();
        assert_eq!(selected.model, "claude-haiku");
        assert!(
            result
                .skipped
                .iter()
                .any(|s| s.model == "gemma3:27b" && s.reason.contains("circuit breaker"))
        );
    }

    #[test]
    fn skips_over_budget() {
        let config = test_config();
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: Some(0.001),
            input_tokens: Some(1000),
        });

        // gemma3:27b has no pricing -> passes budget (free)
        // claude-haiku has pricing -> estimate = 0.0008 + 0.004*8192/1000 = 0.0008 + 32.768 ≈ 33.5488
        // which is way over budget 0.001
        assert!(
            result
                .all_candidates
                .iter()
                .any(|c| c.model == "gemma3:27b")
        );
        assert!(
            result
                .skipped
                .iter()
                .any(|s| s.model == "claude-haiku" && s.reason.contains("over budget"))
        );
    }

    #[test]
    fn api_model_id_override() {
        let config = test_config();
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("claude-haiku".to_string()),
            router: Some("anthropic".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
        });

        assert!(result.selected.is_some());
        let selected = result.selected.unwrap();
        assert_eq!(selected.api_model_id, "claude-haiku-4-5-20251001");
    }

    #[test]
    fn no_chain_for_capability() {
        let config = test_config();
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::AudioTranscribe,
            model: None,
            router: None,
            chain: None,
            budget: None,
            input_tokens: None,
        });

        assert!(result.selected.is_none());
        assert!(result.all_candidates.is_empty());
    }

    #[test]
    fn chain_not_found() {
        let config = test_config();
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("nonexistent_chain".to_string()),
            budget: None,
            input_tokens: None,
        });

        assert!(result.selected.is_none());
        assert!(result.all_candidates.is_empty());
        assert!(result.chain.is_none());
    }

    #[test]
    fn direct_model_not_found() {
        let config = test_config();
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("nonexistent_model".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(result.skipped[0].reason.contains("model not found"));
    }

    #[test]
    fn direct_model_wrong_capability() {
        let config = test_config();
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);

        // all-minilm only supports TextEmbed, not AudioTranscribe
        let result = svc.select(&SelectionCriteria {
            capability: Capability::AudioTranscribe,
            model: Some("all-minilm".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(result.skipped[0].reason.contains("does not support"));
    }

    #[test]
    fn direct_circuit_breaker_open() {
        let config = test_config();
        let cb = test_cb();

        // Open the breaker for this direct endpoint
        let endpoint = "ollama:gemma3:27b";
        cb.can_execute(endpoint); // init
        for _ in 0..5 {
            cb.record_failure(endpoint);
        }
        assert!(!cb.can_execute(endpoint));

        let svc = ModelSelectionService::new(&config, &cb);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("gemma3:27b".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(result.skipped[0].reason.contains("circuit breaker"));
    }

    #[test]
    fn direct_over_budget() {
        let config = test_config();
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);

        // claude-haiku has pricing, set budget very low
        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("claude-haiku".to_string()),
            router: Some("anthropic".to_string()),
            chain: None,
            budget: Some(0.0001),
            input_tokens: Some(1000),
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(result.skipped[0].reason.contains("over budget"));
    }

    #[test]
    fn direct_router_disabled() {
        let mut config = test_config();
        config.routers.get_mut("ollama").unwrap().enabled = false;
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("gemma3:27b".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(result.skipped[0].reason.contains("router disabled"));
    }

    #[test]
    fn chain_entry_router_fallback_to_provider() {
        // embed_chain has entries with router=None, so it should fall back
        // to model.provider ("ollama")
        let config = test_config();
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextEmbed,
            model: None,
            router: None,
            chain: Some("embed_chain".to_string()),
            budget: None,
            input_tokens: None,
        });

        assert_eq!(result.all_candidates.len(), 1);
        // Router should be resolved from provider
        assert_eq!(result.all_candidates[0].router, "ollama");
    }

    #[test]
    fn chain_entry_model_not_found() {
        let mut config = test_config();
        // Add a chain that references a non-existent model
        config.chains.insert(
            "bad_chain".to_string(),
            FallbackChainConfig {
                id: "bad_chain".to_string(),
                capability: Capability::TextChat,
                models: vec![
                    ChainEntry {
                        model: "ghost_model".to_string(),
                        router: Some("ollama".to_string()),
                        api_model_id: None,
                        priority: 1,
                    },
                    ChainEntry {
                        model: "gemma3:27b".to_string(),
                        router: None,
                        api_model_id: None,
                        priority: 2,
                    },
                ],
                fallback_triggers: vec![],
            },
        );
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("bad_chain".to_string()),
            budget: None,
            input_tokens: None,
        });

        // ghost_model should be skipped, gemma3:27b should be selected
        assert_eq!(result.all_candidates.len(), 1);
        assert_eq!(result.all_candidates[0].model, "gemma3:27b");
        assert_eq!(result.skipped.len(), 1);
        assert!(result.skipped[0].reason.contains("model not found"));
    }

    #[test]
    fn chain_entry_router_not_found() {
        let mut config = test_config();
        // Add a chain entry that specifies a non-existent router
        config.chains.insert(
            "bad_router_chain".to_string(),
            FallbackChainConfig {
                id: "bad_router_chain".to_string(),
                capability: Capability::TextChat,
                models: vec![
                    ChainEntry {
                        model: "gemma3:27b".to_string(),
                        router: Some("nonexistent_router".to_string()),
                        api_model_id: None,
                        priority: 1,
                    },
                    ChainEntry {
                        model: "claude-haiku".to_string(),
                        router: None,
                        api_model_id: None,
                        priority: 2,
                    },
                ],
                fallback_triggers: vec![],
            },
        );
        let cb = test_cb();
        let svc = ModelSelectionService::new(&config, &cb);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("bad_router_chain".to_string()),
            budget: None,
            input_tokens: None,
        });

        // gemma3:27b with nonexistent router should be skipped
        assert!(
            result
                .skipped
                .iter()
                .any(|s| s.model == "gemma3:27b" && s.reason.contains("router not found"))
        );
        // claude-haiku should still be available
        assert_eq!(result.all_candidates.len(), 1);
        assert_eq!(result.all_candidates[0].model, "claude-haiku");
    }
}
