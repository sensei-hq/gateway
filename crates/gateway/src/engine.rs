use std::sync::Arc;
use std::time::Instant;

use tokio::sync::RwLock;

use crate::adapters::AdapterRegistry;
use crate::circuit_breaker::CircuitBreakerManager;
use crate::dispatch::{
    from_chat_response, from_embed_response, from_image_response, from_stt_response,
    from_tts_response, from_video_response, to_chat_request, to_embed_request, to_image_request,
    to_stt_request, to_tts_request, to_video_request,
};
use crate::selection::{ModelSelectionService, SelectionCriteria};
use crate::types::capability::Capability;
use crate::types::config::GatewayConfig;
use crate::types::cost::Cost;
use crate::types::error::GatewayError;
use crate::types::request::{InferenceRequest, InferenceResponse, Payload};
use crate::types::trace::{Attempt, AttemptStatus};

/// Core gateway orchestrator.
///
/// Resolves model candidates via [`ModelSelectionService`], walks fallback
/// chains, records attempts, and integrates the circuit breaker.
pub struct Gateway {
    config: Arc<RwLock<GatewayConfig>>,
    pub(crate) adapters: AdapterRegistry,
    circuit_breaker: CircuitBreakerManager,
}

impl Gateway {
    pub fn new(
        config: GatewayConfig,
        adapters: AdapterRegistry,
        circuit_breaker: CircuitBreakerManager,
    ) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            adapters,
            circuit_breaker,
        }
    }

    /// Execute an inference request, walking the fallback chain on failure.
    ///
    /// Returns `GatewayError::NotConfigured` if no config has been set.
    pub async fn execute(
        &self,
        request: &InferenceRequest,
    ) -> Result<InferenceResponse, GatewayError> {
        // 1. Clone config from RwLock
        let config = self.config.read().await.clone();

        if config.routers.is_empty() && config.models.is_empty() && config.chains.is_empty() {
            return Err(GatewayError::NotConfigured);
        }

        // 2. Build SelectionCriteria from request
        let input_tokens = estimate_input_tokens(&request.payload);
        let criteria = SelectionCriteria {
            capability: request.capability.clone(),
            model: request.model.clone(),
            router: request.router.clone(),
            chain: request.chain.clone(),
            budget: request.budget,
            input_tokens: Some(input_tokens),
        };

        // 3. Select all candidates
        let svc = ModelSelectionService::new(&config, &self.circuit_breaker);
        let result = svc.select_all(&criteria);

        // 4. No candidates?
        if result.all_candidates.is_empty() {
            return Err(GatewayError::NoCandidates {
                capability: request.capability.clone(),
            });
        }

        // 5. Get fallback triggers from chain (or empty)
        let fallback_triggers = result
            .chain
            .as_ref()
            .map(|c| c.fallback_triggers.as_slice())
            .unwrap_or(&[]);

        // 6. Walk candidates in order
        let mut attempts: Vec<Attempt> = Vec::new();

        for (sequence, candidate) in (1_u8..).zip(result.all_candidates.iter()) {
            let start = Instant::now();

            // Resolve the model to send. Inject the selected candidate's resolved
            // `api_model_id` when the caller didn't pin one, so chain/registry
            // selection actually drives the provider model — otherwise the adapter
            // falls back to its own built-in default. A caller-pinned
            // `request.model` takes precedence.
            let model = if request.model.is_some() {
                request.model.clone()
            } else {
                Some(candidate.api_model_id.clone())
            };
            let endpoint = format!("{}:{}", candidate.router, candidate.model);
            let cfg = &candidate.router_config;

            // Dispatch by capability to the matching registry map + typed method,
            // translating at the boundary via `crate::dispatch`. `None` means no
            // adapter is registered for this router+capability (or the capability
            // has no dispatch route) — handled the same as the legacy
            // no-adapter-registered path below.
            let outcome: Option<Result<InferenceResponse, GatewayError>> = match request.capability
            {
                Capability::TextChat | Capability::TextComplete => {
                    match self.adapters.chat(&candidate.router).await {
                        Some(m) => Some(match to_chat_request(request, model) {
                            Ok(r) => m.chat(cfg, &r).await.map(from_chat_response),
                            Err(e) => Err(e),
                        }),
                        None => None,
                    }
                }
                Capability::TextEmbed => match self.adapters.embed(&candidate.router).await {
                    Some(m) => Some(match to_embed_request(request, model) {
                        Ok(r) => m.embed(cfg, &r).await.map(from_embed_response),
                        Err(e) => Err(e),
                    }),
                    None => None,
                },
                Capability::AudioTranscribe => match self.adapters.stt(&candidate.router).await {
                    Some(m) => Some(match to_stt_request(request, model) {
                        Ok(r) => m.transcribe(cfg, &r).await.map(from_stt_response),
                        Err(e) => Err(e),
                    }),
                    None => None,
                },
                Capability::AudioGenerate => match self.adapters.tts(&candidate.router).await {
                    Some(m) => Some(match to_tts_request(request, model) {
                        Ok(r) => m.speak(cfg, &r).await.map(from_tts_response),
                        Err(e) => Err(e),
                    }),
                    None => None,
                },
                Capability::ImageGenerate => match self.adapters.image(&candidate.router).await {
                    Some(m) => Some(match to_image_request(request, model) {
                        Ok(r) => m.generate_image(cfg, &r).await.map(from_image_response),
                        Err(e) => Err(e),
                    }),
                    None => None,
                },
                Capability::VideoGenerate => match self.adapters.video(&candidate.router).await {
                    Some(m) => Some(match to_video_request(request, model) {
                        Ok(r) => m.generate_video(cfg, &r).await.map(from_video_response),
                        Err(e) => Err(e),
                    }),
                    None => None,
                },
                // Reserved capabilities have no payload / trait / dispatch route
                // yet — surface an honest "not yet supported" rather than the
                // misleading "no adapter registered". Exhaustive (no `_`) so a new
                // `Capability` variant forces a routing decision here at compile time.
                Capability::TextRerank
                | Capability::TextModerate
                | Capability::ImageEdit
                | Capability::ImageAnalyze => Some(Err(GatewayError::Unsupported {
                    adapter: candidate.router.clone(),
                    what: "capability not yet supported (reserved)".to_string(),
                })),
            };

            let outcome = match outcome {
                Some(o) => o,
                None => {
                    attempts.push(Attempt {
                        sequence,
                        adapter: candidate.router.clone(),
                        model: candidate.model.clone(),
                        api_model_id: candidate.api_model_id.clone(),
                        status: AttemptStatus::Failed,
                        duration_ms: start.elapsed().as_millis() as u64,
                        tokens: None,
                        cost: None,
                        error: Some(format!(
                            "no adapter registered for router '{}'",
                            candidate.router
                        )),
                        fallback_triggered: false,
                    });
                    continue;
                }
            };

            match outcome {
                Ok(mut response) => {
                    let duration_ms = start.elapsed().as_millis() as u64;
                    self.circuit_breaker.record_success(&endpoint);

                    // Fill cost: the pre-call estimate from selection, and the
                    // actual dollar cost from the returned token usage × the
                    // model's pricing (when both are known).
                    response.estimated_cost = candidate.cost_estimate.clone();
                    if let (Some(pricing), Some(usage)) = (
                        candidate.model_config.pricing.as_ref(),
                        response.usage.clone(),
                    ) {
                        response.actual_cost = Some(Cost::from_usage(
                            &usage,
                            pricing.input_per_1k,
                            pricing.output_per_1k,
                        ));
                    }

                    attempts.push(Attempt {
                        sequence,
                        adapter: candidate.router.clone(),
                        model: candidate.model.clone(),
                        api_model_id: candidate.api_model_id.clone(),
                        status: AttemptStatus::Success,
                        duration_ms,
                        tokens: response.usage.clone(),
                        cost: response.actual_cost.as_ref().map(|c| c.total_cost),
                        error: None,
                        fallback_triggered: false,
                    });

                    response.attempts = attempts;
                    response.model = Some(candidate.model.clone());
                    return Ok(response);
                }
                Err(err) => {
                    let duration_ms = start.elapsed().as_millis() as u64;
                    self.circuit_breaker.record_failure(&endpoint);

                    let should_fallback = err.should_trigger_fallback(fallback_triggers);

                    attempts.push(Attempt {
                        sequence,
                        adapter: candidate.router.clone(),
                        model: candidate.model.clone(),
                        api_model_id: candidate.api_model_id.clone(),
                        status: AttemptStatus::Failed,
                        duration_ms,
                        tokens: None,
                        cost: None,
                        error: Some(err.to_string()),
                        fallback_triggered: should_fallback,
                    });

                    if should_fallback {
                        continue;
                    } else {
                        break;
                    }
                }
            }
        }

        // 7. All candidates exhausted
        let errors = attempts
            .iter()
            .filter_map(|a| {
                a.error
                    .as_ref()
                    .map(|e| format!("[{}:{}] {}", a.adapter, a.model, e))
            })
            .collect::<Vec<_>>()
            .join("; ");
        Err(GatewayError::AllAttemptsFailed {
            attempts: attempts.len(),
            errors,
        })
    }

    /// Replace the gateway configuration at runtime.
    pub async fn update_config(&self, config: GatewayConfig) {
        let mut guard = self.config.write().await;
        *guard = config;
    }

    /// Return a sorted list of all registered adapter ids.
    pub async fn list_adapters(&self) -> Vec<String> {
        self.adapters.list().await
    }

    /// Flat list of all configured models, each entry router-qualified.
    pub async fn list_models(&self) -> Result<Vec<serde_json::Value>, GatewayError> {
        let config = self.config.read().await;
        let mut out = Vec::with_capacity(config.models.len());
        for (id, m) in config.models.iter() {
            out.push(serde_json::json!({
                "id":               id,
                "api_model_id":     m.api_model_id,
                "provider":         m.provider,
                "capabilities":     m.capabilities,
                "context_window":   m.context_window,
                "max_output_tokens": m.max_output_tokens,
            }));
        }
        Ok(out)
    }

    /// Models reachable through a specific router. Walks fallback chains
    /// for any entry whose `router` matches, plus any model whose default
    /// provider matches the router id (single-provider routers).
    pub async fn list_models_for_router(
        &self,
        router_id: &str,
    ) -> Result<Vec<serde_json::Value>, GatewayError> {
        let config = self.config.read().await;
        let mut model_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
        // Single-provider routers: model.provider == router id.
        for (id, m) in config.models.iter() {
            if m.provider == router_id {
                model_ids.insert(id.clone());
            }
        }
        // Explicit chain router pins.
        for chain in config.chains.values() {
            for entry in &chain.models {
                if entry.router.as_deref() == Some(router_id) {
                    model_ids.insert(entry.model.clone());
                }
            }
        }
        let mut out = Vec::with_capacity(model_ids.len());
        for id in model_ids {
            if let Some(m) = config.models.get(&id) {
                out.push(serde_json::json!({
                    "id":               id,
                    "api_model_id":     m.api_model_id,
                    "provider":         m.provider,
                    "capabilities":     m.capabilities,
                }));
            }
        }
        Ok(out)
    }

    /// Whether the gateway has any configuration (routers, models, chains).
    /// Returns false if the config is empty — callers should not attempt
    /// execute() until config has been set via update_config().
    pub async fn is_configured(&self) -> bool {
        let config = self.config.read().await;
        !config.routers.is_empty() || !config.models.is_empty() || !config.chains.is_empty()
    }

    /// Re-resolve `api_key` for every router from a caller-supplied
    /// resolver function. Used after a key is set/cleared so the next
    /// request picks up the change without a daemon restart.
    pub async fn refresh_router_keys<F>(&self, resolver: F)
    where
        F: Fn(&str) -> Option<String>,
    {
        let mut config = self.config.write().await;
        for (id, router) in config.routers.iter_mut() {
            router.api_key = resolver(id);
        }
    }
}

/// Estimate input token count from the request payload.
///
/// Rough heuristic: 1 token ~ 4 characters.
fn estimate_input_tokens(payload: &Payload) -> u32 {
    match payload {
        Payload::Chat {
            messages, system, ..
        } => {
            let msg_chars: usize = messages.iter().map(|m| m.as_text().len()).sum();
            let sys_chars: usize = system.as_ref().map(|s| s.len()).unwrap_or(0);
            ((msg_chars + sys_chars) / 4) as u32
        }
        Payload::Embed { texts } => {
            let total_chars: usize = texts.iter().map(|t| t.len()).sum();
            (total_chars / 4) as u32
        }
        // STT has no meaningful text input to estimate.
        Payload::Stt { .. } => 0,
        // For TTS, rough heuristic on text length.
        Payload::Tts { text, .. } => (text.len() / 4) as u32,
        // Image generation: estimate based on prompt length.
        Payload::ImageGenerate { prompt, .. } => (prompt.len() / 4) as u32,
        // Video generation: estimate based on prompt length.
        Payload::VideoGenerate { prompt, .. } => (prompt.len() / 4) as u32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::noop::NoopAdapter;
    use crate::circuit_breaker::CircuitBreakerConfig;
    use crate::types::capability::Capability;
    use crate::types::config::{
        ChainEntry, FallbackChainConfig, FallbackTrigger, GatewayConfig, ModelConfig, RouterConfig,
    };
    use crate::types::request::{Message, MessageRole, Payload};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    fn test_config_with_noop() -> GatewayConfig {
        let mut routers = HashMap::new();
        routers.insert(
            "noop".to_string(),
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
        models.insert(
            "noop".to_string(),
            ModelConfig {
                id: "noop".to_string(),
                api_model_id: None,
                provider: "noop".to_string(),
                capabilities: vec![Capability::TextChat, Capability::TextEmbed],
                context_window: 4096,
                max_output_tokens: 1024,
                pricing: None,
            },
        );

        let mut chains = HashMap::new();
        chains.insert(
            "chat_chain".to_string(),
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
            },
        );

        GatewayConfig {
            routers,
            models,
            chains,
        }
    }

    fn test_gateway() -> Gateway {
        let config = test_config_with_noop();
        let adapters = AdapterRegistry::new();
        let cb = CircuitBreakerManager::new(CircuitBreakerConfig {
            threshold: 5,
            timeout: Duration::from_secs(300),
            half_open_max_requests: 3,
        });

        Gateway::new(config, adapters, cb)
    }

    async fn register_noop(gw: &Gateway) {
        use crate::adapters::RegisterInto;
        // NoopAdapter implements every capability trait + RegisterInto, so this
        // lands it in all six capability maps in one call.
        Arc::new(NoopAdapter).register_into(&gw.adapters).await;
    }

    fn chat_request() -> InferenceRequest {
        InferenceRequest {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: None,
            payload: Payload::Chat {
                messages: vec![Message::text(MessageRole::User, "Hello, world!")],
                system: None,
                max_tokens: None,
                temperature: None,
                tools: Vec::new(),
            },
            budget: None,
        }
    }

    // Adapter that records the model it receives via `chat`, so we can assert the
    // engine injects the chain-selected api_model_id rather than passing None.
    struct RecordingAdapter {
        seen_model: Arc<std::sync::Mutex<Option<String>>>,
    }

    impl crate::adapters::capability::Model for RecordingAdapter {
        fn id(&self) -> &str {
            "noop"
        }
    }

    #[async_trait::async_trait]
    impl crate::adapters::capability::ChatModel for RecordingAdapter {
        async fn chat(
            &self,
            _cfg: &RouterConfig,
            req: &crate::types::io::ChatRequest,
        ) -> Result<crate::types::io::ChatResponse, GatewayError> {
            *self.seen_model.lock().unwrap() = req.model.clone();
            Ok(crate::types::io::ChatResponse {
                content: Some("ok".to_string()),
                tool_calls: Vec::new(),
                usage: None,
                model: req.model.clone(),
            })
        }
    }

    #[tokio::test]
    async fn chain_selection_injects_resolved_api_model_id() {
        // Model whose api_model_id ("noop-v2") differs from its registry id
        // ("noop"); the chain entry leaves api_model_id None so it must resolve
        // from the model config. The caller pins no model.
        let mut routers = HashMap::new();
        routers.insert(
            "noop".to_string(),
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
        models.insert(
            "noop".to_string(),
            ModelConfig {
                id: "noop".to_string(),
                api_model_id: Some("noop-v2".to_string()),
                provider: "noop".to_string(),
                capabilities: vec![Capability::TextChat],
                context_window: 4096,
                max_output_tokens: 1024,
                pricing: None,
            },
        );
        let mut chains = HashMap::new();
        chains.insert(
            "chat_chain".to_string(),
            FallbackChainConfig {
                id: "chat_chain".to_string(),
                capability: Capability::TextChat,
                models: vec![ChainEntry {
                    model: "noop".to_string(),
                    router: Some("noop".to_string()),
                    api_model_id: None,
                    priority: 1,
                }],
                fallback_triggers: vec![],
            },
        );
        let config = GatewayConfig {
            routers,
            models,
            chains,
        };
        let cb = CircuitBreakerManager::new(CircuitBreakerConfig {
            threshold: 5,
            timeout: Duration::from_secs(300),
            half_open_max_requests: 3,
        });
        let gw = Gateway::new(config, AdapterRegistry::new(), cb);

        let seen_model = Arc::new(std::sync::Mutex::new(None));
        gw.adapters
            .register_chat(Arc::new(RecordingAdapter {
                seen_model: seen_model.clone(),
            }))
            .await;

        let request = InferenceRequest {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            payload: Payload::Chat {
                messages: vec![Message::text(MessageRole::User, "hi")],
                system: None,
                max_tokens: None,
                temperature: None,
                tools: Vec::new(),
            },
            budget: None,
        };
        gw.execute(&request).await.unwrap();

        assert_eq!(
            seen_model.lock().unwrap().clone(),
            Some("noop-v2".to_string()),
            "adapter should receive the chain-resolved api_model_id, not None or a default"
        );
    }

    #[tokio::test]
    async fn execute_fills_actual_cost_from_usage_and_pricing() {
        use crate::types::config::ModelPricing;
        use crate::types::cost::TokenUsage;

        struct UsageAdapter;
        impl crate::adapters::capability::Model for UsageAdapter {
            fn id(&self) -> &str {
                "priced"
            }
        }
        #[async_trait::async_trait]
        impl crate::adapters::capability::ChatModel for UsageAdapter {
            async fn chat(
                &self,
                _cfg: &RouterConfig,
                _req: &crate::types::io::ChatRequest,
            ) -> Result<crate::types::io::ChatResponse, GatewayError> {
                Ok(crate::types::io::ChatResponse {
                    content: Some("ok".to_string()),
                    tool_calls: Vec::new(),
                    usage: Some(TokenUsage {
                        input_tokens: 1000,
                        output_tokens: 500,
                        total_tokens: 1500,
                    }),
                    model: Some("priced".to_string()),
                })
            }
        }

        let mut routers = HashMap::new();
        routers.insert(
            "priced".to_string(),
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
        models.insert(
            "priced".to_string(),
            ModelConfig {
                id: "priced".to_string(),
                api_model_id: None,
                provider: "priced".to_string(),
                capabilities: vec![Capability::TextChat],
                context_window: 4096,
                max_output_tokens: 1024,
                pricing: Some(ModelPricing {
                    input_per_1k: 0.0008,
                    output_per_1k: 0.004,
                    per_request: None,
                }),
            },
        );
        let config = GatewayConfig {
            routers,
            models,
            chains: HashMap::new(),
        };
        let cb = CircuitBreakerManager::new(CircuitBreakerConfig {
            threshold: 5,
            timeout: Duration::from_secs(300),
            half_open_max_requests: 3,
        });
        let gw = Gateway::new(config, AdapterRegistry::new(), cb);
        gw.adapters.register_chat(Arc::new(UsageAdapter)).await;

        let request = InferenceRequest {
            capability: Capability::TextChat,
            model: Some("priced".to_string()),
            router: Some("priced".to_string()),
            chain: None,
            payload: Payload::Chat {
                messages: vec![Message::text(MessageRole::User, "hi")],
                system: None,
                max_tokens: None,
                temperature: None,
                tools: Vec::new(),
            },
            budget: None,
        };
        let response = gw.execute(&request).await.unwrap();

        // input 1000/1000 * 0.0008 = 0.0008; output 500/1000 * 0.004 = 0.002; total 0.0028
        let cost = response
            .actual_cost
            .expect("actual_cost should be computed from usage x pricing");
        assert!(
            (cost.total_cost - 0.0028).abs() < 1e-9,
            "got {}",
            cost.total_cost
        );
        assert_eq!(cost.input_tokens, 1000);
        // The recorded attempt carries the same dollar cost.
        assert_eq!(response.attempts[0].cost, Some(cost.total_cost));
    }

    #[tokio::test]
    async fn reserved_capability_returns_unsupported_not_no_adapter() {
        // A model may declare a reserved capability (e.g. ImageEdit); a request
        // for it must surface an honest "not yet supported" error rather than the
        // misleading "no adapter registered".
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
        models.insert(
            "m".to_string(),
            ModelConfig {
                id: "m".to_string(),
                api_model_id: None,
                provider: "r".to_string(),
                capabilities: vec![Capability::ImageEdit],
                context_window: 4096,
                max_output_tokens: 1024,
                pricing: None,
            },
        );
        let config = GatewayConfig {
            routers,
            models,
            chains: HashMap::new(),
        };
        let cb = CircuitBreakerManager::new(CircuitBreakerConfig {
            threshold: 5,
            timeout: Duration::from_secs(300),
            half_open_max_requests: 3,
        });
        let gw = Gateway::new(config, AdapterRegistry::new(), cb);

        let request = InferenceRequest {
            capability: Capability::ImageEdit,
            model: Some("m".to_string()),
            router: Some("r".to_string()),
            chain: None,
            // No ImageEdit payload exists; any constructible payload works — the
            // reserved arm errors before the payload is inspected.
            payload: Payload::ImageGenerate {
                prompt: "x".to_string(),
                size: None,
                quality: None,
                style: None,
                n: 1,
            },
            budget: None,
        };
        let msg = gw.execute(&request).await.unwrap_err().to_string();
        assert!(
            msg.contains("not yet supported") || msg.contains("reserved"),
            "expected an honest reserved-capability error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn execute_with_noop_adapter() {
        let gw = test_gateway();
        register_noop(&gw).await;

        let response = gw.execute(&chat_request()).await.unwrap();

        // The typed capability path carries no `success` flag, so the noop's
        // old `success: false` degraded signal is no longer propagated — the
        // engine reports success for any Ok outcome (see NoopAdapter comment).
        // The canned "No inference provider" content is still returned.
        assert!(response.success);
        assert!(
            response
                .content
                .as_ref()
                .unwrap()
                .contains("No inference provider")
        );
    }

    #[tokio::test]
    async fn execute_no_candidates_errors() {
        let gw = test_gateway();
        register_noop(&gw).await;

        // VoiceStt has no chain configured
        let request = InferenceRequest {
            capability: Capability::AudioTranscribe,
            model: None,
            router: None,
            chain: None,
            payload: Payload::Chat {
                messages: vec![Message::text(MessageRole::User, "transcribe".to_string())],
                system: None,
                max_tokens: None,
                temperature: None,
                tools: Vec::new(),
            },
            budget: None,
        };

        let result = gw.execute(&request).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            GatewayError::NoCandidates { capability } => {
                assert_eq!(capability, Capability::AudioTranscribe);
            }
            other => panic!("Expected NoCandidates, got: {other}"),
        }
    }

    #[tokio::test]
    async fn execute_with_direct_model() {
        let gw = test_gateway();
        register_noop(&gw).await;

        let request = InferenceRequest {
            capability: Capability::TextChat,
            model: Some("noop".to_string()),
            router: Some("noop".to_string()),
            chain: None,
            payload: Payload::Chat {
                messages: vec![Message::text(MessageRole::User, "Hello".to_string())],
                system: None,
                max_tokens: None,
                temperature: None,
                tools: Vec::new(),
            },
            budget: None,
        };

        let response = gw.execute(&request).await.unwrap();
        assert_eq!(response.model, Some("noop".to_string()));
    }

    #[tokio::test]
    async fn execute_records_attempts() {
        let gw = test_gateway();
        register_noop(&gw).await;

        let response = gw.execute(&chat_request()).await.unwrap();

        assert!(!response.attempts.is_empty());
        assert_eq!(response.attempts[0].adapter, "noop");
    }

    #[tokio::test]
    async fn execute_update_config_takes_effect() {
        let gw = test_gateway();
        register_noop(&gw).await;

        // First: verify execute works
        let response = gw.execute(&chat_request()).await;
        assert!(response.is_ok());

        // Update config to empty — no routers, no models, no chains
        gw.update_config(GatewayConfig::default()).await;

        // Now execute should fail with NotConfigured (empty config)
        let result = gw.execute(&chat_request()).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), GatewayError::NotConfigured));
    }

    // --- FailingAdapter for error/fallback path tests ---

    struct FailingAdapter {
        error: GatewayError,
    }

    impl crate::adapters::capability::Model for FailingAdapter {
        fn id(&self) -> &str {
            "failing"
        }
    }

    #[async_trait::async_trait]
    impl crate::adapters::capability::ChatModel for FailingAdapter {
        async fn chat(
            &self,
            _cfg: &RouterConfig,
            _req: &crate::types::io::ChatRequest,
        ) -> Result<crate::types::io::ChatResponse, GatewayError> {
            // Clone the configured error to return each time.
            match &self.error {
                GatewayError::ProviderError {
                    adapter,
                    message,
                    status,
                } => Err(GatewayError::ProviderError {
                    adapter: adapter.clone(),
                    message: message.clone(),
                    status: *status,
                }),
                GatewayError::Authentication { adapter, message } => {
                    Err(GatewayError::Authentication {
                        adapter: adapter.clone(),
                        message: message.clone(),
                    })
                }
                GatewayError::RateLimit {
                    adapter,
                    retry_after_ms,
                } => Err(GatewayError::RateLimit {
                    adapter: adapter.clone(),
                    retry_after_ms: *retry_after_ms,
                }),
                GatewayError::Timeout {
                    adapter,
                    model,
                    duration_ms,
                } => Err(GatewayError::Timeout {
                    adapter: adapter.clone(),
                    model: model.clone(),
                    duration_ms: *duration_ms,
                }),
                _ => Err(GatewayError::ProviderError {
                    adapter: "failing".into(),
                    message: "generic failure".into(),
                    status: None,
                }),
            }
        }
    }

    /// Config with a failing adapter as primary and noop as fallback.
    fn test_config_with_failing_and_noop() -> GatewayConfig {
        let mut routers = HashMap::new();
        routers.insert(
            "failing".to_string(),
            RouterConfig {
                url: "http://localhost".to_string(),
                api_key_env: None,
                api_key: None,
                enabled: true,
                timeout_ms: None,
                headers: HashMap::new(),
            },
        );
        routers.insert(
            "noop".to_string(),
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
        models.insert(
            "fail-model".to_string(),
            ModelConfig {
                id: "fail-model".to_string(),
                api_model_id: None,
                provider: "failing".to_string(),
                capabilities: vec![Capability::TextChat],
                context_window: 4096,
                max_output_tokens: 1024,
                pricing: None,
            },
        );
        models.insert(
            "noop".to_string(),
            ModelConfig {
                id: "noop".to_string(),
                api_model_id: None,
                provider: "noop".to_string(),
                capabilities: vec![Capability::TextChat],
                context_window: 4096,
                max_output_tokens: 1024,
                pricing: None,
            },
        );

        let mut chains = HashMap::new();
        chains.insert(
            "chat_chain".to_string(),
            FallbackChainConfig {
                id: "chat_chain".to_string(),
                capability: Capability::TextChat,
                models: vec![
                    ChainEntry {
                        model: "fail-model".to_string(),
                        router: Some("failing".to_string()),
                        api_model_id: None,
                        priority: 1,
                    },
                    ChainEntry {
                        model: "noop".to_string(),
                        router: Some("noop".to_string()),
                        api_model_id: None,
                        priority: 2,
                    },
                ],
                fallback_triggers: vec![
                    FallbackTrigger::ProviderError,
                    FallbackTrigger::Timeout,
                    FallbackTrigger::RateLimit,
                ],
            },
        );

        GatewayConfig {
            routers,
            models,
            chains,
        }
    }

    /// Helper: gateway with a failing adapter registered.
    fn test_gateway_with_chain() -> Gateway {
        let config = test_config_with_failing_and_noop();
        let adapters = AdapterRegistry::new();
        let cb = CircuitBreakerManager::new(CircuitBreakerConfig {
            threshold: 5,
            timeout: Duration::from_secs(300),
            half_open_max_requests: 3,
        });
        Gateway::new(config, adapters, cb)
    }

    async fn register_failing(gw: &Gateway, error: GatewayError) {
        gw.adapters
            .register_chat(Arc::new(FailingAdapter { error }))
            .await;
    }

    #[tokio::test]
    async fn execute_fallback_on_provider_error() {
        let gw = test_gateway_with_chain();
        register_failing(
            &gw,
            GatewayError::ProviderError {
                adapter: "failing".into(),
                message: "server error".into(),
                status: Some(500),
            },
        )
        .await;
        register_noop(&gw).await;

        let response = gw.execute(&chat_request()).await.unwrap();
        // Should have fallen back to noop after failing adapter errors
        assert_eq!(response.model, Some("noop".to_string()));
        assert!(response.attempts.len() >= 2);
        // First attempt should be failed
        assert_eq!(
            response.attempts[0].status,
            crate::types::trace::AttemptStatus::Failed
        );
        assert!(response.attempts[0].fallback_triggered);
        // Second attempt should be the noop success
        assert_eq!(
            response.attempts[1].status,
            crate::types::trace::AttemptStatus::Success
        );
    }

    #[tokio::test]
    async fn execute_stops_on_auth_error() {
        // Authentication error should NOT trigger fallback — it breaks the loop
        let gw = test_gateway_with_chain();
        register_failing(
            &gw,
            GatewayError::Authentication {
                adapter: "failing".into(),
                message: "bad key".into(),
            },
        )
        .await;
        register_noop(&gw).await;

        let result = gw.execute(&chat_request()).await;
        // Should be AllAttemptsFailed because auth error is not a fallback trigger
        assert!(result.is_err());
        match result.unwrap_err() {
            GatewayError::AllAttemptsFailed { attempts, .. } => {
                assert_eq!(attempts, 1);
            }
            other => panic!("Expected AllAttemptsFailed, got: {other}"),
        }
    }

    #[tokio::test]
    async fn execute_all_fail_returns_error() {
        // Both adapters are failing — all candidates fail
        let mut routers = HashMap::new();
        routers.insert(
            "failing".to_string(),
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
        models.insert(
            "fail-model".to_string(),
            ModelConfig {
                id: "fail-model".to_string(),
                api_model_id: None,
                provider: "failing".to_string(),
                capabilities: vec![Capability::TextChat],
                context_window: 4096,
                max_output_tokens: 1024,
                pricing: None,
            },
        );

        let mut chains = HashMap::new();
        chains.insert(
            "chat_chain".to_string(),
            FallbackChainConfig {
                id: "chat_chain".to_string(),
                capability: Capability::TextChat,
                models: vec![ChainEntry {
                    model: "fail-model".to_string(),
                    router: Some("failing".to_string()),
                    api_model_id: None,
                    priority: 1,
                }],
                fallback_triggers: vec![FallbackTrigger::ProviderError],
            },
        );

        let config = GatewayConfig {
            routers,
            models,
            chains,
        };
        let adapters = AdapterRegistry::new();
        let cb = CircuitBreakerManager::new(CircuitBreakerConfig {
            threshold: 5,
            timeout: Duration::from_secs(300),
            half_open_max_requests: 3,
        });
        let gw = Gateway::new(config, adapters, cb);
        register_failing(
            &gw,
            GatewayError::ProviderError {
                adapter: "failing".into(),
                message: "error".into(),
                status: Some(500),
            },
        )
        .await;

        let result = gw.execute(&chat_request()).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            GatewayError::AllAttemptsFailed { attempts, .. } => {
                assert_eq!(attempts, 1);
            }
            other => panic!("Expected AllAttemptsFailed, got: {other}"),
        }
    }

    #[tokio::test]
    async fn execute_adapter_not_found() {
        // Config references a router "ghost" but no adapter is registered for it
        let mut routers = HashMap::new();
        routers.insert(
            "ghost".to_string(),
            RouterConfig {
                url: "http://localhost".to_string(),
                api_key_env: None,
                api_key: None,
                enabled: true,
                timeout_ms: None,
                headers: HashMap::new(),
            },
        );
        routers.insert(
            "noop".to_string(),
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
        models.insert(
            "ghost-model".to_string(),
            ModelConfig {
                id: "ghost-model".to_string(),
                api_model_id: None,
                provider: "ghost".to_string(),
                capabilities: vec![Capability::TextChat],
                context_window: 4096,
                max_output_tokens: 1024,
                pricing: None,
            },
        );
        models.insert(
            "noop".to_string(),
            ModelConfig {
                id: "noop".to_string(),
                api_model_id: None,
                provider: "noop".to_string(),
                capabilities: vec![Capability::TextChat],
                context_window: 4096,
                max_output_tokens: 1024,
                pricing: None,
            },
        );

        let mut chains = HashMap::new();
        chains.insert(
            "chat_chain".to_string(),
            FallbackChainConfig {
                id: "chat_chain".to_string(),
                capability: Capability::TextChat,
                models: vec![
                    ChainEntry {
                        model: "ghost-model".to_string(),
                        router: Some("ghost".to_string()),
                        api_model_id: None,
                        priority: 1,
                    },
                    ChainEntry {
                        model: "noop".to_string(),
                        router: Some("noop".to_string()),
                        api_model_id: None,
                        priority: 2,
                    },
                ],
                fallback_triggers: vec![FallbackTrigger::ProviderError],
            },
        );

        let config = GatewayConfig {
            routers,
            models,
            chains,
        };
        let adapters = AdapterRegistry::new();
        let cb = CircuitBreakerManager::new(CircuitBreakerConfig {
            threshold: 5,
            timeout: Duration::from_secs(300),
            half_open_max_requests: 3,
        });
        let gw = Gateway::new(config, adapters, cb);
        // Only register noop — "ghost" has no adapter
        register_noop(&gw).await;

        let response = gw.execute(&chat_request()).await.unwrap();
        // Ghost adapter was skipped, noop should have handled it
        assert_eq!(response.model, Some("noop".to_string()));
        assert!(response.attempts.len() >= 2);
        assert!(
            response.attempts[0]
                .error
                .as_ref()
                .unwrap()
                .contains("no adapter registered")
        );
    }

    #[test]
    fn estimate_input_tokens_stt() {
        let payload = Payload::Stt {
            audio: vec![0u8; 1000],
            language: None,
            format: "wav".to_string(),
        };
        assert_eq!(estimate_input_tokens(&payload), 0);
    }

    #[test]
    fn estimate_input_tokens_tts() {
        let payload = Payload::Tts {
            text: "Hello world, this is a test!".to_string(),
            voice: None,
            speed: None,
            output_format: crate::types::request::AudioFormat::Mp3,
        };
        let expected = ("Hello world, this is a test!".len() / 4) as u32;
        assert_eq!(estimate_input_tokens(&payload), expected);
    }

    #[test]
    fn estimate_input_tokens_image_generate() {
        let payload = Payload::ImageGenerate {
            prompt: "A beautiful sunset over mountains".to_string(),
            size: None,
            quality: None,
            style: None,
            n: 1,
        };
        let expected = ("A beautiful sunset over mountains".len() / 4) as u32;
        assert_eq!(estimate_input_tokens(&payload), expected);
    }

    #[test]
    fn estimate_input_tokens_video_generate() {
        let payload = Payload::VideoGenerate {
            prompt: "A timelapse of a blooming flower".to_string(),
            duration_secs: Some(10),
            resolution: Some("1080p".to_string()),
        };
        let expected = ("A timelapse of a blooming flower".len() / 4) as u32;
        assert_eq!(estimate_input_tokens(&payload), expected);
    }
}
