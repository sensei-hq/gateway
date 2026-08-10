//! Portable, attribute-derived tier + chain presets consumed by
//! [`crate::catalog::assemble`].
//!
//! Everything here is **pure config data** (no I/O, no persistence): four
//! reference tiers whose membership is derived from model attributes, three
//! named chains that reference those tiers, and a small merge helper that drops
//! the presets onto operator-supplied routers/models. See the design
//! `docs/superpowers/specs/2026-08-07-reference-chains-design.md` (§2 tiers,
//! §3 chains, §4 API).

use std::collections::HashMap;

use kernel::types::capability::Capability;
use kernel::types::config::{
    AuthType, CatalogMeta, ConstraintsConfig, CostBand, FallbackTrigger, FreeTier, FreeType,
    Locality, ModelConfig, ModelPricing, RouterConfig, TosVerdict,
};

use crate::catalog::{
    CatalogConfig, ChainRef, FreeMatch, IntraTierStrategy, TierChain, TierConfig, TierPredicate,
};

/// A derive-only tier: no curated members, membership resolved purely from the
/// attribute predicate.
fn derived(strategy: IntraTierStrategy, pred: TierPredicate) -> TierConfig {
    TierConfig {
        strategy,
        members: Vec::new(),
        derive: Some(pred),
    }
}

/// The four portable reference tiers, each derived from model attributes
/// (design §2). All are membership-by-predicate — a catalog joins them purely by
/// tagging its models, never by editing a curated id list:
/// * `free` — any declared free tier, ordered by [`IntraTierStrategy::Priority`];
/// * `cost-optimized` — the `Low` cost band, ordered cheapest-first by `Cost`;
/// * `fallback-specialty` — locally-hosted models, `Priority`;
/// * `premium-reasoning` — OAuth-CLI models tagged `"reasoning"`, `Priority`.
pub fn reference_tiers() -> HashMap<String, TierConfig> {
    HashMap::from([
        (
            "free".into(),
            derived(
                IntraTierStrategy::Priority,
                TierPredicate {
                    free: Some(FreeMatch::Any),
                    ..Default::default()
                },
            ),
        ),
        (
            "cost-optimized".into(),
            derived(
                IntraTierStrategy::Cost,
                TierPredicate {
                    cost_band: Some(CostBand::Low),
                    ..Default::default()
                },
            ),
        ),
        (
            "fallback-specialty".into(),
            derived(
                IntraTierStrategy::Priority,
                TierPredicate {
                    locality: Some(Locality::Local),
                    ..Default::default()
                },
            ),
        ),
        (
            "premium-reasoning".into(),
            derived(
                IntraTierStrategy::Priority,
                TierPredicate {
                    auth_type: Some(AuthType::OauthCli),
                    tags: Some(vec!["reasoning".into()]),
                    ..Default::default()
                },
            ),
        ),
    ])
}

/// A `TextChat` chain over the given tier/model refs, with the standard
/// fallback triggers (rate-limit, timeout, provider-error).
fn chain(refs: Vec<ChainRef>) -> TierChain {
    TierChain {
        capability: Capability::TextChat,
        refs,
        fallback_triggers: vec![
            FallbackTrigger::RateLimit,
            FallbackTrigger::Timeout,
            FallbackTrigger::ProviderError,
        ],
    }
}

/// The three reference chains, authored purely as tier-refs (design §3):
/// * `research.bulk` — `free → cost-optimized → fallback-specialty`;
/// * `plan.frontier` — `premium-reasoning → cost-optimized`;
/// * `code.exec` — `cost-optimized → premium-reasoning`.
pub fn reference_chains() -> HashMap<String, TierChain> {
    let t = |id: &str| ChainRef::Tier(id.to_string());
    HashMap::from([
        (
            "research.bulk".into(),
            chain(vec![
                t("free"),
                t("cost-optimized"),
                t("fallback-specialty"),
            ]),
        ),
        (
            "plan.frontier".into(),
            chain(vec![t("premium-reasoning"), t("cost-optimized")]),
        ),
        (
            "code.exec".into(),
            chain(vec![t("cost-optimized"), t("premium-reasoning")]),
        ),
    ])
}

/// Drop the reference tiers + chains onto operator-supplied `routers`/`models`/
/// `constraints`, yielding a [`CatalogConfig`] ready for
/// [`crate::catalog::assemble`]. The reference ids are a base; a caller can
/// post-hoc override by editing the returned config.
pub fn with_reference_tiers_and_chains(
    routers: HashMap<String, RouterConfig>,
    models: HashMap<String, ModelConfig>,
    constraints: ConstraintsConfig,
) -> CatalogConfig {
    CatalogConfig {
        routers,
        models,
        tiers: reference_tiers(),
        chains: reference_chains(),
        constraints,
    }
}

/// A single-provider router at `url`. Cloud/BYOK routers name the environment
/// variable their key is read from (`key_env`); a keyless local runner (Ollama)
/// or an OAuth-CLI router passes `None`. Always `enabled`.
fn router(url: &str, key_env: Option<&str>) -> RouterConfig {
    RouterConfig {
        url: url.into(),
        api_key_env: key_env.map(str::to_string),
        api_key: None,
        enabled: true,
        timeout_ms: None,
        headers: std::collections::HashMap::new(),
    }
}

/// A TextChat model whose `provider` equals its router id, carrying the given
/// `catalog` attribute metadata and optional `pricing` (from which the cost band
/// derives when `catalog.cost_band` is absent).
fn tagged(
    id: &str,
    provider: &str,
    catalog: CatalogMeta,
    pricing: Option<ModelPricing>,
) -> ModelConfig {
    ModelConfig {
        id: id.into(),
        api_model_id: None,
        provider: provider.into(),
        capabilities: vec![Capability::TextChat],
        context_window: 8192,
        max_output_tokens: 4096,
        pricing,
        family: None,
        catalog: Some(catalog),
    }
}

/// A small, **illustrative** catalog that instantiates the reference tiers +
/// chains with four representative tagged models — a runnable starting
/// *template*, **not** a fixed roster. These models/routers are meant to be
/// edited: a real deployment supplies its own routers/models (each with its own
/// `CatalogMeta` tags), and tier membership then derives automatically, so the
/// reference chains pick the new models up with no chain edits.
///
/// Each model populates exactly one reference tier, so the three chains expand
/// to concrete, runnable candidates:
/// * `llama3.1-local` — router `ollama` (keyless, local) → `fallback-specialty`;
/// * `groq-llama-free` — router `groq` (BYOK, keyless free tier) → `free`;
/// * `deepseek-chat` — router `deepseek` (BYOK, low pricing) → `cost-optimized`;
/// * `claude-code` — router `claude-cli` (OAuth-CLI, tagged `"reasoning"`) →
///   `premium-reasoning`.
///
/// Pure config — no I/O. Feed the result straight to
/// [`crate::catalog::assemble`].
pub fn demo_catalog() -> CatalogConfig {
    let routers = HashMap::from([
        ("ollama".into(), router("http://localhost:11434", None)),
        (
            "groq".into(),
            router("https://api.groq.com/openai/v1", Some("GROQ_API_KEY")),
        ),
        (
            "deepseek".into(),
            router("https://api.deepseek.com", Some("DEEPSEEK_API_KEY")),
        ),
        // OAuth-CLI: authenticated via the local `claude` CLI's session, no key env.
        (
            "claude-cli".into(),
            router("https://api.anthropic.com", None),
        ),
    ]);

    let models = HashMap::from([
        (
            "llama3.1-local".into(),
            tagged(
                "llama3.1-local",
                "ollama",
                CatalogMeta {
                    locality: Some(Locality::Local),
                    auth_type: Some(AuthType::Keyless),
                    ..Default::default()
                },
                None,
            ),
        ),
        (
            "groq-llama-free".into(),
            tagged(
                "groq-llama-free",
                "groq",
                CatalogMeta {
                    free: Some(FreeTier {
                        free_type: FreeType::Keyless,
                        monthly_tokens: None,
                        credit_tokens: None,
                        pool_key: None,
                        tos: TosVerdict::Ok,
                        trains_on_prompts: false,
                    }),
                    auth_type: Some(AuthType::ApiKey),
                    ..Default::default()
                },
                None,
            ),
        ),
        (
            "deepseek-chat".into(),
            tagged(
                "deepseek-chat",
                "deepseek",
                CatalogMeta {
                    auth_type: Some(AuthType::ApiKey),
                    ..Default::default()
                },
                // Blended 0.001/1k → `CostBand::Low` → joins `cost-optimized`.
                Some(ModelPricing {
                    input_per_1k: 0.0005,
                    output_per_1k: 0.0005,
                    per_request: None,
                }),
            ),
        ),
        (
            "claude-code".into(),
            tagged(
                "claude-code",
                "claude-cli",
                CatalogMeta {
                    auth_type: Some(AuthType::OauthCli),
                    tags: vec!["reasoning".into()],
                    ..Default::default()
                },
                None,
            ),
        ),
    ]);

    with_reference_tiers_and_chains(routers, models, ConstraintsConfig::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{assemble, cost_band, tier_members};
    use kernel::types::config::{CatalogMeta, FreeTier, FreeType, ModelPricing, TosVerdict};

    // ---- fixtures ----------------------------------------------------------

    /// A bare TextChat model with the given id, no catalog, no pricing.
    fn base(id: &str) -> ModelConfig {
        ModelConfig {
            id: id.into(),
            api_model_id: None,
            provider: "p".into(),
            capabilities: vec![Capability::TextChat],
            context_window: 8000,
            max_output_tokens: 1000,
            pricing: None,
            family: None,
            catalog: None,
        }
    }

    /// A model whose catalog declares a keyless free tier (matches `free`).
    fn free_model(id: &str) -> ModelConfig {
        ModelConfig {
            catalog: Some(CatalogMeta {
                free: Some(FreeTier {
                    free_type: FreeType::Keyless,
                    monthly_tokens: None,
                    credit_tokens: None,
                    pool_key: None,
                    tos: TosVerdict::Ok,
                    trains_on_prompts: false,
                }),
                ..Default::default()
            }),
            ..base(id)
        }
    }

    /// A cheaply-priced model (blended 0.001 → `CostBand::Low`); no catalog, so
    /// it matches ONLY the cost-optimized predicate.
    fn cheap_model(id: &str) -> ModelConfig {
        ModelConfig {
            pricing: Some(ModelPricing {
                input_per_1k: 0.0005,
                output_per_1k: 0.0005,
                per_request: None,
            }),
            ..base(id)
        }
    }

    /// A locally-hosted model (matches `fallback-specialty`).
    fn local_model(id: &str) -> ModelConfig {
        ModelConfig {
            catalog: Some(CatalogMeta {
                locality: Some(Locality::Local),
                ..Default::default()
            }),
            ..base(id)
        }
    }

    /// An OAuth-CLI, reasoning-tagged model (matches `premium-reasoning`).
    fn oauth_reasoning_model(id: &str) -> ModelConfig {
        ModelConfig {
            catalog: Some(CatalogMeta {
                auth_type: Some(AuthType::OauthCli),
                tags: vec!["reasoning".into()],
                ..Default::default()
            }),
            ..base(id)
        }
    }

    fn models_map(items: Vec<ModelConfig>) -> HashMap<String, ModelConfig> {
        items.into_iter().map(|m| (m.id.clone(), m)).collect()
    }

    fn tier_ref_ids(chain: &TierChain) -> Vec<&str> {
        chain
            .refs
            .iter()
            .map(|r| match r {
                ChainRef::Tier(id) => id.as_str(),
                ChainRef::Model(_) => panic!("reference chains are authored as tier-refs only"),
            })
            .collect()
    }

    // ---- The 4 reference tiers: derive fields + strategy -------------------

    #[test]
    fn reference_tiers_have_the_right_predicates_and_strategies() {
        let tiers = reference_tiers();
        assert_eq!(tiers.len(), 4, "exactly four reference tiers");

        let free = &tiers["free"];
        assert_eq!(free.strategy, IntraTierStrategy::Priority);
        assert!(free.members.is_empty(), "reference tiers are derive-only");
        assert_eq!(
            free.derive.as_ref().expect("free has a derive").free,
            Some(FreeMatch::Any),
        );

        let cost = &tiers["cost-optimized"];
        assert_eq!(cost.strategy, IntraTierStrategy::Cost);
        assert!(cost.members.is_empty());
        assert_eq!(
            cost.derive.as_ref().expect("cost has a derive").cost_band,
            Some(CostBand::Low),
        );

        let spec = &tiers["fallback-specialty"];
        assert_eq!(spec.strategy, IntraTierStrategy::Priority);
        assert!(spec.members.is_empty());
        assert_eq!(
            spec.derive
                .as_ref()
                .expect("specialty has a derive")
                .locality,
            Some(Locality::Local),
        );

        let prem = &tiers["premium-reasoning"];
        assert_eq!(prem.strategy, IntraTierStrategy::Priority);
        assert!(prem.members.is_empty());
        let pd = prem.derive.as_ref().expect("premium has a derive");
        assert_eq!(pd.auth_type, Some(AuthType::OauthCli));
        assert_eq!(pd.tags, Some(vec!["reasoning".to_string()]));
    }

    // ---- Membership: each reference tier's predicate resolves correctly ----

    #[test]
    fn reference_tiers_membership_resolves_each_predicate_to_its_model() {
        let tiers = reference_tiers();
        let models = models_map(vec![
            free_model("groq-free"),
            cheap_model("deepseek-cheap"),
            local_model("llama-local"),
            oauth_reasoning_model("claude-premium"),
        ]);

        assert_eq!(
            tier_members("free", &tiers["free"], &models).unwrap(),
            vec!["groq-free".to_string()],
        );
        assert_eq!(
            tier_members("cost-optimized", &tiers["cost-optimized"], &models).unwrap(),
            vec!["deepseek-cheap".to_string()],
        );
        assert_eq!(
            tier_members("fallback-specialty", &tiers["fallback-specialty"], &models).unwrap(),
            vec!["llama-local".to_string()],
        );
        assert_eq!(
            tier_members("premium-reasoning", &tiers["premium-reasoning"], &models).unwrap(),
            vec!["claude-premium".to_string()],
        );
    }

    // ---- The 3 reference chains: refs + capability + triggers --------------

    #[test]
    fn reference_chains_have_the_right_refs_capability_and_triggers() {
        let chains = reference_chains();
        assert_eq!(chains.len(), 3, "exactly three reference chains");

        let expected_triggers = vec![
            FallbackTrigger::RateLimit,
            FallbackTrigger::Timeout,
            FallbackTrigger::ProviderError,
        ];

        for (id, refs) in [
            (
                "research.bulk",
                vec!["free", "cost-optimized", "fallback-specialty"],
            ),
            ("plan.frontier", vec!["premium-reasoning", "cost-optimized"]),
            ("code.exec", vec!["cost-optimized", "premium-reasoning"]),
        ] {
            let chain = &chains[id];
            assert_eq!(chain.capability, Capability::TextChat, "{id} capability");
            assert_eq!(chain.fallback_triggers, expected_triggers, "{id} triggers");
            assert_eq!(tier_ref_ids(chain), refs, "{id} refs");
        }
    }

    // ---- The illustrative demo catalog: shape --------------------------------

    #[test]
    fn demo_catalog_has_the_illustrative_tagged_models_and_routers() {
        let demo = demo_catalog();

        // Four single-provider routers; each model's provider equals its router.
        assert_eq!(demo.routers.len(), 4, "four demo routers");
        for id in ["ollama", "groq", "deepseek", "claude-cli"] {
            assert!(demo.routers.contains_key(id), "router {id} present");
            assert!(demo.routers[id].enabled, "router {id} enabled");
        }
        // ollama is keyless/local; the cloud BYOK routers name their key env.
        assert_eq!(demo.routers["ollama"].url, "http://localhost:11434");
        assert_eq!(demo.routers["ollama"].api_key_env, None);
        assert_eq!(
            demo.routers["groq"].api_key_env.as_deref(),
            Some("GROQ_API_KEY"),
        );
        assert_eq!(
            demo.routers["deepseek"].api_key_env.as_deref(),
            Some("DEEPSEEK_API_KEY"),
        );

        assert_eq!(demo.models.len(), 4, "four demo models");
        for (id, provider) in [
            ("llama3.1-local", "ollama"),
            ("groq-llama-free", "groq"),
            ("deepseek-chat", "deepseek"),
            ("claude-code", "claude-cli"),
        ] {
            assert_eq!(demo.models[id].provider, provider, "{id} provider = router");
        }

        // llama3.1-local — locally hosted, unpriced.
        let llama = demo.models["llama3.1-local"].catalog.as_ref().unwrap();
        assert_eq!(llama.locality, Some(Locality::Local));
        assert!(demo.models["llama3.1-local"].pricing.is_none());

        // groq-llama-free — a keyless free tier.
        let groq = demo.models["groq-llama-free"].catalog.as_ref().unwrap();
        assert_eq!(groq.free.as_ref().unwrap().free_type, FreeType::Keyless);

        // deepseek-chat — low pricing derives CostBand::Low.
        assert_eq!(cost_band(&demo.models["deepseek-chat"]), CostBand::Low);

        // claude-code — OAuth-CLI, reasoning-tagged.
        let claude = demo.models["claude-code"].catalog.as_ref().unwrap();
        assert_eq!(claude.auth_type, Some(AuthType::OauthCli));
        assert_eq!(claude.tags, vec!["reasoning".to_string()]);
    }

    // ---- The demo catalog expands its reference chains concretely ------------

    #[test]
    fn demo_catalog_expands_reference_chains_into_concrete_candidates() {
        let g = assemble(demo_catalog()).unwrap();

        let ids = |chain: &str| -> Vec<String> {
            g.chains[chain]
                .models
                .iter()
                .map(|e| e.model.clone())
                .collect()
        };

        // free → cost-optimized → fallback-specialty, ascending priority.
        assert_eq!(
            ids("research.bulk"),
            ["groq-llama-free", "deepseek-chat", "llama3.1-local"],
        );
        // premium-reasoning → cost-optimized.
        assert_eq!(ids("plan.frontier"), ["claude-code", "deepseek-chat"]);

        // premium-reasoning resolves (via auth_type + tags) to exactly claude-code.
        assert_eq!(
            tier_members(
                "premium-reasoning",
                &reference_tiers()["premium-reasoning"],
                &demo_catalog().models,
            )
            .unwrap(),
            vec!["claude-code".to_string()],
        );
    }

    // ---- Portability: add a tagged model, every chain updates (no chain edit)-

    #[test]
    fn demo_catalog_adding_a_tagged_model_updates_plan_frontier_no_chain_edit() {
        // A real deployment can drop in a SECOND OAuth-CLI, reasoning-tagged
        // model WITHOUT editing any chain: it joins `premium-reasoning` purely by
        // its attributes, so `plan.frontier` picks it up automatically.
        let mut demo = demo_catalog();
        demo.routers
            .insert("codex-cli".into(), router("https://api.openai.com", None));
        demo.models.insert(
            "codex-cli".into(),
            tagged(
                "codex-cli",
                "codex-cli",
                CatalogMeta {
                    auth_type: Some(AuthType::OauthCli),
                    tags: vec!["reasoning".into()],
                    ..Default::default()
                },
                None,
            ),
        );

        let g = assemble(demo).unwrap();
        let frontier: Vec<String> = g.chains["plan.frontier"]
            .models
            .iter()
            .map(|e| e.model.clone())
            .collect();

        assert!(
            frontier.contains(&"codex-cli".to_string()),
            "the added reasoning model joins plan.frontier with no chain edit: {frontier:?}",
        );
        // The original reasoning model is still present — both are members now.
        assert!(frontier.contains(&"claude-code".to_string()));
    }

    // ---- Runnable end-to-end: the reference chain drives real SP-0 fallover --

    #[tokio::test]
    async fn demo_reference_chain_drives_runnable_local_fallover() {
        use std::sync::Arc;

        use async_trait::async_trait;

        use crate::adapters::RegisterInto;
        use crate::adapters::capability::{ChatModel, Model};
        use crate::circuit_breaker::{CircuitBreakerConfig, CircuitBreakerManager};
        use crate::engine::Gateway;
        use crate::types::config::RouterConfig as GwRouterConfig;
        use crate::types::error::GatewayError;
        use crate::types::io::{ChatRequest, ChatResponse};
        use crate::types::request::{Message, MessageRole, Payload};
        use kernel::adapters::AdapterRegistry;
        use kernel::types::request::InferenceRequest;

        // A local stand-in for a live Ollama runner: registered under the router
        // id `"ollama"`, it serves TextChat with no network I/O (the runnable
        // proxy for the local model at the tail of `research.bulk`).
        struct LocalOllama;
        impl Model for LocalOllama {
            fn id(&self) -> &str {
                "ollama"
            }
        }
        #[async_trait]
        impl ChatModel for LocalOllama {
            async fn chat(
                &self,
                _cfg: &GwRouterConfig,
                _req: &ChatRequest,
            ) -> Result<ChatResponse, GatewayError> {
                Ok(ChatResponse {
                    content: Some("served by local llama3.1".into()),
                    tool_calls: Vec::new(),
                    usage: None,
                    model: None,
                    degraded: false,
                })
            }
        }
        #[async_trait]
        impl RegisterInto for LocalOllama {
            async fn register_into(self: Arc<Self>, reg: &AdapterRegistry) {
                reg.register_chat(self).await;
            }
        }

        let config = assemble(demo_catalog()).expect("demo assembles");
        // Sanity: the concrete chain the selector will walk, in priority order —
        // the local model is LAST, so reaching it proves genuine fallover.
        let ids: Vec<&str> = config.chains["research.bulk"]
            .models
            .iter()
            .map(|e| e.model.as_str())
            .collect();
        assert_eq!(ids, ["groq-llama-free", "deepseek-chat", "llama3.1-local"]);

        let gw = Gateway::new(
            config,
            AdapterRegistry::new(),
            CircuitBreakerManager::new(CircuitBreakerConfig::default()),
        );
        // Register an adapter for the LOCAL router ONLY — groq/deepseek have none,
        // so they fall over.
        Arc::new(LocalOllama).register_into(&gw.adapters).await;

        let request = InferenceRequest {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("research.bulk".to_string()),
            payload: Payload::Chat {
                messages: vec![Message::text(MessageRole::User, "hello")],
                system: None,
                max_tokens: None,
                temperature: None,
                tools: Vec::new(),
            },
            budget: None,
            auth: None,
            panel: None,
            consensus: None,
            allow_fallback: true,
            credentials: Default::default(),
        };

        let response = gw.execute(&request).await.expect("executes");
        // The assembled REFERENCE chain fell over groq (no adapter) → deepseek
        // (no adapter) → llama3.1-local (served by the local adapter).
        assert_eq!(response.model, Some("llama3.1-local".to_string()));
        // Reached the local model via GENUINE fallover: two prior no-adapter
        // attempts precede the successful terminal one.
        assert_eq!(
            response.attempts.len(),
            3,
            "walked groq → deepseek → llama-local",
        );
        assert_eq!(response.attempts.last().unwrap().model, "llama3.1-local");
    }
}
