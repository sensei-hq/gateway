use crate::circuit_breaker::CircuitBreakerManager;
use crate::gates::budget::BudgetGate;
use crate::gates::capability::CapabilityGate;
use crate::gates::circuit_breaker_gate::CircuitBreakerGate;
use crate::gates::cooldown::ConnectionCooldownGate;
use crate::gates::{
    AdmissionGate, CandidateView, EndpointHealthRead, GateVerdict, RouterHealthRead, SelectionCtx,
};
use crate::skip_reason::SkipReason;
use crate::strategy::{PriorityStrategy, RoutingStrategy};
use crate::types::capability::Capability;
use crate::types::config::{
    ChainEntry, FallbackChainConfig, GatewayConfig, ModelConfig, RouterConfig,
};
use crate::types::cost::CostEstimate;
use std::time::Instant;

/// Criteria used to resolve which model(s) to try.
#[derive(Debug, Clone)]
pub struct SelectionCriteria {
    pub capability: Capability,
    pub model: Option<String>,
    pub router: Option<String>,
    pub chain: Option<String>,
    pub budget: Option<f64>,
    pub input_tokens: Option<u32>,
    /// The pessimistic input estimate, read only by the
    /// [`crate::gates::context_window::ContextWindowGate`].
    ///
    /// A SECOND field beside `input_tokens` rather than a replacement for it, for the
    /// reason argued in `engine::util::estimate_input_tokens_pessimistic`: the cost gate
    /// and the window gate want opposite biases over the same payload, so collapsing
    /// them to one number is precisely what that argument rules out. `None` admits every
    /// candidate — a caller that reaches selection without an estimate is not making a
    /// claim about size, and refusing it would be a filter on missing data.
    pub input_tokens_pessimistic: Option<u32>,
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
    pub reason: SkipReason,
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
/// (direct, named chain, capability). Structural resolution (router/model
/// lookup) happens per path; the shared admission pipeline then runs the
/// ordered [`AdmissionGate`]s (capability, connection cooldown, circuit breaker,
/// model lockout, budget, context window) and the [`RoutingStrategy`] orders the
/// admitted candidates. The list below is the one place these are registered — keep
/// every enumeration in this file in step with it.
pub struct ModelSelectionService<'a> {
    config: &'a GatewayConfig,
    /// Ordered admission gates: capability, connection cooldown, circuit breaker,
    /// model lockout, budget, context window.
    gates: Vec<Box<dyn AdmissionGate>>,
    /// Endpoint health read port (the circuit breaker implements it).
    health: &'a dyn EndpointHealthRead,
    /// Router health read port (the connection cooldown store implements it).
    router_health: &'a dyn RouterHealthRead,
    /// Endpoint model-lockout read port (the model-lockout store implements it).
    model_lockout: &'a dyn crate::gates::lockout::ModelLockoutRead,
    /// Orders admitted candidates (SP-0: priority ascending, stable).
    strategy: Box<dyn RoutingStrategy>,
}

impl<'a> ModelSelectionService<'a> {
    pub fn new(
        config: &'a GatewayConfig,
        circuit_breaker: &'a CircuitBreakerManager,
        router_health: &'a dyn RouterHealthRead,
        model_lockout: &'a dyn crate::gates::lockout::ModelLockoutRead,
    ) -> Self {
        Self {
            config,
            gates: vec![
                Box::new(CapabilityGate),
                Box::new(ConnectionCooldownGate),
                Box::new(CircuitBreakerGate),
                Box::new(crate::gates::lockout::ModelLockoutGate),
                Box::new(BudgetGate),
                // LAST. The vector is ordered and `admit` returns the FIRST skip, so this
                // position decides which reason a multiply-gated candidate reports — and
                // that is a behaviour, not a presentation detail: `gate_status()` makes
                // `CircuitOpen`/`Cooling`/a timed lockout `Timed`, which becomes
                // `AllGated { resume_after: Some(t) }` and a TIMED pause at the
                // orchestrator's `classify_gateway_error`, while `OverContextWindow` is
                // `Terminal` — `resume_after: None`, which since the M1 reversal is the
                // indefinite HOTL pause rather than a `NodeFailed`.
                //
                // **After the three HEALTH gates (cooldown, breaker, lockout), and that
                // is the load-bearing half.** A candidate that is both over-window and
                // circuit-open must report the BREAKER, because that one clears BY
                // ITSELF. Reporting the window instead swaps a pause the scheduler wakes
                // on its own for one that waits on a human who has nothing to do — the
                // breaker would have closed unaided — so a transient provider outage
                // stalls the run until somebody notices. (Before the M1 reversal the same
                // mistake killed the run outright, which is why this comment used to say
                // "permanently dead". The ordering is load-bearing either way.) Pinned by
                // `a_health_skip_is_reported_ahead_of_the_window_for_the_same_candidate`
                // and, at the engine boundary where the two pause KINDS are visible, by
                // `engine::tests::an_over_window_candidate_whose_breaker_is_open_still_lets_the_run_pause`.
                //
                // **After `BudgetGate` too, and that half is a JUDGEMENT with a cost.**
                // An earlier version of this comment claimed every gate ahead of this one
                // is "either structural or health", and that is simply false: `OverBudget`
                // is `Terminal(RaiseBudget)` and a `CreditsExhausted`/auth lockout is
                // `Terminal(TopUpCredits/RotateCredential)`. `all_gated_error` keeps the
                // FIRST terminal remedy it meets, so a request that is over budget AND
                // over every window is reported as `RaiseBudget` and says nothing about
                // the window: the operator raises the cap, retries, and only then learns
                // the prompt does not fit. Accepted deliberately — money is the
                // irreversible lever, and a caller that has set a cap wants to hear about
                // the cap first — but it is a two-step diagnosis, not a free ordering,
                // and `a_budget_skip_is_reported_ahead_of_the_window` pins it so the
                // choice cannot drift by accident.
                Box::new(crate::gates::context_window::ContextWindowGate),
            ],
            health: circuit_breaker,
            router_health,
            model_lockout,
            strategy: Box::new(PriorityStrategy),
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

    /// Shared admission path: run each gate in order over a structurally
    /// resolved candidate. On the first `Skip(reason)` return the reason; on
    /// all-Admit build the `SelectedModel`, attaching the full `CostEstimate`
    /// (the `BudgetGate` independently computes an f64 from the same formula).
    fn admit(
        &self,
        cand: CandidateView<'_>,
        api_model_id: String,
        priority: u8,
        criteria: &SelectionCriteria,
    ) -> Result<SelectedModel, SkipReason> {
        let ctx = SelectionCtx {
            capability: criteria.capability.clone(),
            budget: criteria.budget,
            input_tokens: criteria.input_tokens,
            input_tokens_pessimistic: criteria.input_tokens_pessimistic,
            health: self.health,
            now: Instant::now(),
            config: self.config,
            router_health: self.router_health,
            model_lockout: self.model_lockout,
        };
        for gate in &self.gates {
            if let GateVerdict::Skip(reason) = gate.evaluate(&cand, &ctx) {
                return Err(reason);
            }
        }

        let cost_estimate = self.estimate_cost(cand.model_config, criteria);
        Ok(SelectedModel {
            model: cand.model.to_string(),
            router: cand.router.to_string(),
            router_config: cand.router_config.clone(),
            model_config: cand.model_config.clone(),
            api_model_id,
            priority,
            cost_estimate,
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
        let router_name = criteria.router.clone().unwrap_or_default();
        let model_name = criteria.model.clone().unwrap_or_default();

        match self.validate_direct(&router_name, &model_name, criteria) {
            Ok(selected) => SelectionResult {
                selected: None, // filled by caller
                all_candidates: vec![selected],
                skipped: vec![],
                chain: None,
            },
            Err(reason) => SelectionResult {
                selected: None,
                all_candidates: vec![],
                skipped: vec![SkippedCandidate {
                    model: model_name,
                    router: router_name,
                    reason,
                }],
                chain: None,
            },
        }
    }

    /// Structural resolution for tier 1: router-first. Look up the router
    /// BEFORE the model (empty/missing → `RouterNotFound`, disabled →
    /// `RouterDisabled`), then the model (missing → `ModelNotFound`). No
    /// provider fallback. `priority = 1`; `api_model_id` is 2-level
    /// (model_config override else model id). The shared gate pipeline
    /// (capability, connection cooldown, circuit breaker, model lockout, budget,
    /// context window) runs in [`Self::admit`].
    fn validate_direct(
        &self,
        router_name: &str,
        model_name: &str,
        criteria: &SelectionCriteria,
    ) -> Result<SelectedModel, SkipReason> {
        // Validate router exists and is enabled (router-first).
        let router_config = self
            .config
            .routers
            .get(router_name)
            .ok_or(SkipReason::RouterNotFound)?;
        if !router_config.enabled {
            return Err(SkipReason::RouterDisabled);
        }

        // Validate model exists.
        let model_config = self
            .config
            .models
            .get(model_name)
            .ok_or(SkipReason::ModelNotFound)?;

        let api_model_id = model_config
            .api_model_id
            .clone()
            .unwrap_or_else(|| model_name.to_string());

        let cand = CandidateView {
            model: model_name,
            router: router_name,
            endpoint: format!("{router_name}:{model_name}"),
            model_config,
            router_config,
        };
        self.admit(cand, api_model_id, 1, criteria)
    }

    /// Tier 2/3: Walk chain entries, structurally resolving + gating each, then
    /// order the admitted candidates via the strategy (SP-0: priority
    /// ascending, stable — identical to the previous hardcoded entry sort,
    /// since a stable sort of the admitted subset preserves the same order).
    fn resolve_chain(
        &self,
        chain: &FallbackChainConfig,
        criteria: &SelectionCriteria,
    ) -> SelectionResult {
        let mut all_candidates = Vec::new();
        let mut skipped = Vec::new();

        for entry in &chain.models {
            match self.validate_chain_entry(entry, criteria) {
                Ok(candidate) => all_candidates.push(candidate),
                Err(candidate) => skipped.push(candidate),
            }
        }

        self.strategy.order(&mut all_candidates);

        SelectionResult {
            selected: None, // filled by caller
            all_candidates,
            skipped,
            chain: Some(chain.clone()),
        }
    }

    /// Structural resolution for a single chain entry: model-first. Look up the
    /// model (missing → `ModelNotFound`), resolve the router from the entry
    /// (falling back to the model's provider), then validate it (missing →
    /// `RouterNotFound`, disabled → `RouterDisabled`). `priority = entry.priority`;
    /// `api_model_id` is 3-level (entry override → model_config → model id). The
    /// shared gate pipeline (capability, connection cooldown, circuit breaker, model
    /// lockout, budget, context window) runs in [`Self::admit`].
    fn validate_chain_entry(
        &self,
        entry: &ChainEntry,
        criteria: &SelectionCriteria,
    ) -> Result<SelectedModel, SkippedCandidate> {
        let model_name = &entry.model;

        // Look up the model config (model-first).
        let model_config = self.config.models.get(model_name).ok_or_else(|| {
            let router_name = entry
                .router
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            SkippedCandidate {
                model: model_name.clone(),
                router: router_name,
                reason: SkipReason::ModelNotFound,
            }
        })?;

        // Resolve router: chain entry router, else model's provider.
        let router_name = entry
            .router
            .clone()
            .unwrap_or_else(|| model_config.provider.clone());

        // Validate router exists and is enabled.
        let router_config =
            self.config
                .routers
                .get(&router_name)
                .ok_or_else(|| SkippedCandidate {
                    model: model_name.clone(),
                    router: router_name.clone(),
                    reason: SkipReason::RouterNotFound,
                })?;

        if !router_config.enabled {
            return Err(SkippedCandidate {
                model: model_name.clone(),
                router: router_name,
                reason: SkipReason::RouterDisabled,
            });
        }

        // Resolve API model ID: chain entry override, else model config, else model id.
        let api_model_id = entry
            .api_model_id
            .clone()
            .or_else(|| model_config.api_model_id.clone())
            .unwrap_or_else(|| model_name.clone());

        let cand = CandidateView {
            model: model_name,
            router: &router_name,
            endpoint: format!("{router_name}:{model_name}"),
            model_config,
            router_config,
        };
        self.admit(cand, api_model_id, entry.priority, criteria)
            .map_err(|reason| SkippedCandidate {
                model: model_name.clone(),
                router: router_name.clone(),
                reason,
            })
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
                family: None,
                capabilities: vec![
                    Capability::TextChat,
                    Capability::TextComplete,
                    Capability::TextEmbed,
                ],
                context_window: 128000,
                max_output_tokens: 8192,
                pricing: None,
                catalog: None,
            },
        );
        models.insert(
            "all-minilm".to_string(),
            ModelConfig {
                id: "all-minilm".to_string(),
                api_model_id: None,
                provider: "ollama".to_string(),
                family: None,
                capabilities: vec![Capability::TextEmbed],
                context_window: 512,
                max_output_tokens: 0,
                pricing: None,
                catalog: None,
            },
        );
        models.insert(
            "claude-haiku".to_string(),
            ModelConfig {
                id: "claude-haiku".to_string(),
                api_model_id: Some("claude-haiku-4-5-20251001".to_string()),
                provider: "anthropic".to_string(),
                family: None,
                capabilities: vec![Capability::TextChat],
                context_window: 200000,
                max_output_tokens: 8192,
                pricing: Some(ModelPricing {
                    input_per_1k: 0.0008,
                    output_per_1k: 0.004,
                    per_request: None,
                }),
                catalog: None,
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
            panels: Default::default(),
            consensus: Default::default(),
        }
    }

    fn test_cb() -> CircuitBreakerManager {
        CircuitBreakerManager::new(CircuitBreakerConfig::default())
    }

    #[test]
    fn tier1_direct_selection() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("gemma3:27b".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
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
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("gemma3:27b".to_string()),
            router: Some("nonexistent".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::RouterNotFound
        ));
    }

    #[test]
    fn tier2_chain_selection() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
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
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextEmbed,
            model: None,
            router: None,
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
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
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);
        // Run several times: a HashMap-order bug would flake; min_by is stable.
        for _ in 0..10 {
            let result = svc.select(&SelectionCriteria {
                capability: Capability::TextChat,
                model: None,
                router: None,
                chain: None,
                budget: None,
                input_tokens: None,
                input_tokens_pessimistic: None,
            });
            assert_eq!(result.chain.as_ref().unwrap().id, "aaa_chain");
        }
    }

    #[test]
    fn skips_disabled_router() {
        let mut config = test_config();
        config.routers.get_mut("ollama").unwrap().enabled = false;
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
        });

        assert!(result.selected.is_some());
        let selected = result.selected.unwrap();
        assert_eq!(selected.model, "claude-haiku");
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::RouterDisabled
        ));
        assert_eq!(result.skipped[0].model, "gemma3:27b");
    }

    #[test]
    fn skips_wrong_capability() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::AudioTranscribe,
            model: Some("gemma3:27b".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::UnsupportedCapability(_)
        ));
    }

    #[test]
    fn skips_circuit_breaker_open() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();

        // Open the circuit breaker for ollama:gemma3:27b
        let endpoint = "ollama:gemma3:27b";
        cb.can_execute(endpoint); // initialize
        for _ in 0..5 {
            cb.record_failure(endpoint);
        }
        assert!(!cb.can_execute(endpoint)); // confirm open

        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
        });

        assert!(result.selected.is_some());
        let selected = result.selected.unwrap();
        assert_eq!(selected.model, "claude-haiku");
        assert!(
            result
                .skipped
                .iter()
                .any(|s| s.model == "gemma3:27b"
                    && matches!(s.reason, SkipReason::CircuitOpen { .. }))
        );
    }

    #[test]
    fn skips_over_budget() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: Some(0.001),
            input_tokens: Some(1000),
            input_tokens_pessimistic: None,
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
        assert!(result.skipped.iter().any(
            |s| s.model == "claude-haiku" && matches!(s.reason, SkipReason::OverBudget { .. })
        ));
    }

    #[test]
    fn api_model_id_override() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("claude-haiku".to_string()),
            router: Some("anthropic".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
        });

        assert!(result.selected.is_some());
        let selected = result.selected.unwrap();
        assert_eq!(selected.api_model_id, "claude-haiku-4-5-20251001");
    }

    #[test]
    fn no_chain_for_capability() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::AudioTranscribe,
            model: None,
            router: None,
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
        });

        assert!(result.selected.is_none());
        assert!(result.all_candidates.is_empty());
    }

    #[test]
    fn chain_not_found() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("nonexistent_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
        });

        assert!(result.selected.is_none());
        assert!(result.all_candidates.is_empty());
        assert!(result.chain.is_none());
    }

    #[test]
    fn direct_model_not_found() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("nonexistent_model".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::ModelNotFound
        ));
    }

    #[test]
    fn direct_model_wrong_capability() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        // all-minilm only supports TextEmbed, not AudioTranscribe
        let result = svc.select(&SelectionCriteria {
            capability: Capability::AudioTranscribe,
            model: Some("all-minilm".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::UnsupportedCapability(_)
        ));
    }

    #[test]
    fn direct_circuit_breaker_open() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();

        // Open the breaker for this direct endpoint
        let endpoint = "ollama:gemma3:27b";
        cb.can_execute(endpoint); // init
        for _ in 0..5 {
            cb.record_failure(endpoint);
        }
        assert!(!cb.can_execute(endpoint));

        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("gemma3:27b".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::CircuitOpen { .. }
        ));
    }

    #[test]
    fn direct_over_budget() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        // claude-haiku has pricing, set budget very low
        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("claude-haiku".to_string()),
            router: Some("anthropic".to_string()),
            chain: None,
            budget: Some(0.0001),
            input_tokens: Some(1000),
            input_tokens_pessimistic: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::OverBudget { .. }
        ));
    }

    #[test]
    fn direct_router_disabled() {
        let mut config = test_config();
        config.routers.get_mut("ollama").unwrap().enabled = false;
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("gemma3:27b".to_string()),
            router: Some("ollama".to_string()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
        });

        assert!(result.selected.is_none());
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::RouterDisabled
        ));
    }

    #[test]
    fn chain_entry_router_fallback_to_provider() {
        // embed_chain has entries with router=None, so it should fall back
        // to model.provider ("ollama")
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextEmbed,
            model: None,
            router: None,
            chain: Some("embed_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
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
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("bad_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
        });

        // ghost_model should be skipped, gemma3:27b should be selected
        assert_eq!(result.all_candidates.len(), 1);
        assert_eq!(result.all_candidates[0].model, "gemma3:27b");
        assert_eq!(result.skipped.len(), 1);
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::ModelNotFound
        ));
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
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("bad_router_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
        });

        // gemma3:27b with nonexistent router should be skipped
        assert!(
            result
                .skipped
                .iter()
                .any(|s| s.model == "gemma3:27b" && matches!(s.reason, SkipReason::RouterNotFound))
        );
        // claude-haiku should still be available
        assert_eq!(result.all_candidates.len(), 1);
        assert_eq!(result.all_candidates[0].model, "claude-haiku");
    }

    #[test]
    fn direct_both_router_and_model_unknown_reports_router_first() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);
        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("ghost".into()),
            router: Some("nope".into()),
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
        });
        // Current behavior: direct validates the router first.
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::RouterNotFound
        ));
    }

    #[test]
    fn direct_model_only_no_router_is_router_not_found_today() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);
        let result = svc.select(&SelectionCriteria {
            capability: Capability::TextChat,
            model: Some("gemma3:27b".into()),
            router: None,
            chain: None,
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
        });
        // Direct does NOT provider-fallback today → empty router → "router not found".
        assert!(result.selected.is_none());
        assert!(matches!(
            result.skipped[0].reason,
            SkipReason::RouterNotFound
        ));
    }

    // -----------------------------------------------------------------------------
    // SP-7a — the `ContextWindowGate` seen through the whole selection service.
    //
    // The gate's own unit tests (`gates/context_window.rs`) call `evaluate` directly, so
    // they pass whether or not the gate is in `ModelSelectionService::new`'s vector.
    // These do not: they go through `select_all`, which is the only place registration
    // is observable.
    // -----------------------------------------------------------------------------

    /// A `TextChat` chain of two models differing ONLY in context window — AC1's chain,
    /// and the smallest config in which the window question has two different answers.
    ///
    /// `small` is deliberately given priority **1** and `big` priority 2, so the model
    /// that CANNOT hold a large request is the one selection would otherwise return
    /// first. A test that ordered them the other way would still pass with the gate
    /// deleted.
    fn two_model_chain_windows(big: u32, small: u32) -> GatewayConfig {
        let mut routers = HashMap::new();
        routers.insert(
            "r".to_string(),
            RouterConfig {
                url: "http://localhost".to_string(),
                api_key_env: None,
                api_key: None,
                enabled: true,
                timeout_ms: None,
                headers: HashMap::new(),
            },
        );

        let mut models = HashMap::new();
        for (id, context_window) in [("big", big), ("small", small)] {
            models.insert(
                id.to_string(),
                ModelConfig {
                    id: id.to_string(),
                    api_model_id: None,
                    provider: "r".to_string(),
                    family: None,
                    capabilities: vec![Capability::TextChat],
                    context_window,
                    max_output_tokens: 4096,
                    // No pricing, so the `BudgetGate` admits both unconditionally and
                    // the only gate that can separate these two is the window one.
                    pricing: None,
                    catalog: None,
                },
            );
        }

        let mut chains = HashMap::new();
        chains.insert(
            "win_chain".to_string(),
            FallbackChainConfig {
                id: "win_chain".to_string(),
                capability: Capability::TextChat,
                models: vec![
                    ChainEntry {
                        model: "small".to_string(),
                        router: Some("r".to_string()),
                        api_model_id: None,
                        priority: 1,
                    },
                    ChainEntry {
                        model: "big".to_string(),
                        router: Some("r".to_string()),
                        api_model_id: None,
                        priority: 2,
                    },
                ],
                fallback_triggers: vec![],
            },
        );

        GatewayConfig {
            routers,
            models,
            chains,
            constraints: Default::default(),
            panels: Default::default(),
            consensus: Default::default(),
        }
    }

    /// Criteria over `win_chain` carrying only the PESSIMISTIC estimate.
    ///
    /// `input_tokens` (the cost figure) stays `None` on purpose: with no pricing in the
    /// fixture the `BudgetGate` ignores it anyway, and leaving it empty means a wiring
    /// that fed the window gate the cost field would admit everything and redden these
    /// tests instead of quietly agreeing with them.
    fn criteria_with_pessimistic(est: Option<u32>) -> SelectionCriteria {
        SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("win_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: est,
        }
    }

    /// AC1 — a heterogeneous chain serves a prompt only its larger model can hold.
    ///
    /// This is the whole slice in one assertion. Before it, the orchestrator refused
    /// this request outright against the chain's 8k MINIMUM and never asked the 128k
    /// model. The gate has to be REGISTERED for this to hold; calling it directly, as
    /// its own unit tests do, cannot tell whether it runs.
    #[test]
    fn a_chain_serves_a_prompt_only_its_larger_model_can_hold() {
        let config = two_model_chain_windows(128_000, 8_192);
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&criteria_with_pessimistic(Some(20_000)));
        let admitted: Vec<String> = result
            .all_candidates
            .iter()
            .map(|c| c.model.clone())
            .collect();
        assert!(
            admitted.contains(&"big".to_string()),
            "the 128k model holds 20k and must be admitted: {admitted:?}"
        );
        assert!(
            !admitted.contains(&"small".to_string()),
            "the 8k model cannot hold 20k and must be skipped: {admitted:?}"
        );
        assert_eq!(
            result.selected.map(|s| s.model),
            Some("big".to_string()),
            "and the request must actually be routed to it — `small` is the \
             priority-1 entry, so this is only true because the gate removed it"
        );
    }

    /// AC3 — over EVERY window is an all-gated selection, recorded with a typed reason
    /// per candidate rather than degrading to a bare `NoCandidates`.
    ///
    /// What the CALLER then receives is asserted at the engine boundary
    /// (`engine::tests::a_request_over_every_window_is_all_gated_with_the_numbers`),
    /// because `all_gated_error` lives there — and it is a terminal failure, not a
    /// pause. Here the claim is narrower and is the one selection owns: every candidate
    /// is skipped, and each skip says which window it lost to.
    #[test]
    fn a_prompt_over_every_window_gates_every_candidate() {
        let config = two_model_chain_windows(128_000, 8_192);
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&criteria_with_pessimistic(Some(200_000)));
        assert!(
            result.all_candidates.is_empty(),
            "nothing in the chain can hold 200k: {:?}",
            result
                .all_candidates
                .iter()
                .map(|c| &c.model)
                .collect::<Vec<_>>()
        );
        let windows: Vec<u32> = result
            .skipped
            .iter()
            .filter_map(|s| match s.reason {
                SkipReason::OverContextWindow { window, .. } => Some(window),
                _ => None,
            })
            .collect();
        assert_eq!(
            windows,
            vec![8_192, 128_000],
            "BOTH candidates must be skipped for the window, each naming its OWN — a \
             single chain-wide figure is exactly what this slice replaced: {:?}",
            result.skipped
        );
    }

    /// The gate's POSITION in the vector, pinned by its consequence.
    ///
    /// `admit` returns the FIRST skip, so where `ContextWindowGate` sits decides which
    /// reason a multiply-gated candidate reports — and that is not cosmetic. A skip
    /// reason's `gate_status()` is what `all_gated_error` aggregates: `CircuitOpen` is
    /// `Timed`, which becomes `AllGated { resume_after: Some(t) }` and a self-healing
    /// TIMED pause at `classify_gateway_error`; `OverContextWindow` is `Terminal`, which
    /// becomes `resume_after: None` — the indefinite HOTL pause since the M1 reversal, and
    /// a `NodeFailed` before it. So a candidate that is both over-window and circuit-open
    /// must surface the BREAKER, or a transient provider outage leaves every run whose
    /// prompt is also too large for that entry waiting on a human who has nothing to fix.
    ///
    /// Nothing enforced this before: moving `Box::new(ContextWindowGate)` from last to
    /// FIRST in `ModelSelectionService::new` left the whole workspace green, while
    /// changing this candidate's reported reason from `CircuitOpen` to
    /// `OverContextWindow`. The engine-level consequence is asserted in
    /// `engine::tests::an_over_window_candidate_whose_breaker_is_open_still_lets_the_run_pause`;
    /// this is the same claim where the ordering actually lives.
    #[test]
    fn a_health_skip_is_reported_ahead_of_the_window_for_the_same_candidate() {
        let config = two_model_chain_windows(128_000, 8_192);
        let cb = test_cb();
        // Trip `small`'s breaker Open. It is ALSO the candidate that cannot hold the
        // request below, which is the whole point: two gates fire on one candidate.
        cb.can_execute("r:small");
        for _ in 0..5 {
            cb.record_failure("r:small");
        }
        assert!(
            !cb.can_execute("r:small"),
            "the fixture needs the breaker genuinely open"
        );
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&criteria_with_pessimistic(Some(20_000)));
        let small = result
            .skipped
            .iter()
            .find(|s| s.model == "small")
            .expect("`small` is skipped twice over and must be recorded");
        assert!(
            matches!(small.reason, SkipReason::CircuitOpen { .. }),
            "the BREAKER must win: it is `Timed`, so the caller can pause and retry, \
             where `OverContextWindow` is `Terminal` and kills the run. Registering the \
             window gate ahead of the health gates inverts this: {:?}",
            small.reason
        );
    }

    /// The budget half of the same ordering, pinned because it is a JUDGEMENT rather
    /// than a forced move.
    ///
    /// Both `OverBudget` and `OverContextWindow` are `Terminal`, so unlike the breaker
    /// case neither ordering costs a pause — what it costs is a round trip. Reporting
    /// budget first means an operator whose request is over budget AND over every window
    /// raises the cap, retries, and only then learns the prompt does not fit; reporting
    /// the window first means the mirror image. Money-first is the shipped choice (see
    /// the registration comment), and this test is what makes reversing it a deliberate
    /// edit instead of an accident of vector order.
    #[test]
    fn a_budget_skip_is_reported_ahead_of_the_window() {
        let mut config = two_model_chain_windows(128_000, 8_192);
        // Price both models so the `BudgetGate` has an estimate to judge. The figures
        // only have to exceed the caller's budget; `max_output_tokens` alone (4096 at
        // $1/1k) puts every candidate over a $0.01 cap.
        for m in config.models.values_mut() {
            m.pricing = Some(crate::types::config::ModelPricing {
                input_per_1k: 1.0,
                output_per_1k: 1.0,
                per_request: None,
            });
        }
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&SelectionCriteria {
            budget: Some(0.01),
            // The COST gate reads this one; the window gate reads the other. Both
            // candidates are over both.
            input_tokens: Some(20_000),
            ..criteria_with_pessimistic(Some(200_000))
        });
        assert!(result.all_candidates.is_empty(), "everything is gated");
        for s in &result.skipped {
            assert!(
                matches!(s.reason, SkipReason::OverBudget { .. }),
                "money is reported first, deliberately — see the registration comment \
                 in `ModelSelectionService::new` for the accepted cost: {} reported {:?}",
                s.model,
                s.reason
            );
        }
    }

    /// AC4 — an in-window request selects byte-identically to one carrying no estimate.
    /// The additivity guarantee: registering a sixth gate must not perturb any request
    /// that fits.
    #[test]
    fn an_in_window_request_selects_unchanged() {
        let config = two_model_chain_windows(128_000, 8_192);
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let names = |r: &SelectionResult| -> Vec<String> {
            r.all_candidates.iter().map(|c| c.model.clone()).collect()
        };
        let with = svc.select_all(&criteria_with_pessimistic(Some(1_000)));
        let without = svc.select_all(&criteria_with_pessimistic(None));
        assert_eq!(
            names(&with),
            names(&without),
            "a request that fits every window must select the same candidates, in the \
             same order, as one carrying no estimate at all"
        );
        assert_eq!(
            names(&with),
            vec!["small".to_string(), "big".to_string()],
            "and that order is the chain's own priority order, unchanged"
        );
        assert!(
            with.skipped.is_empty() && without.skipped.is_empty(),
            "an in-window request records no skips at all"
        );
    }
}
