use std::collections::{HashMap, HashSet};

use kernel::types::capability::Capability;
use kernel::types::config::{
    ChainEntry, ConstraintsConfig, FallbackChainConfig, FallbackTrigger, GatewayConfig,
    ModelConfig, RouterConfig,
};

use super::tiers::{AssembleError, TierConfig, order_members, tier_members};

/// Authoring-time catalog: routers, models (with optional `CatalogMeta`), named
/// tiers, and tier-ref chains. [`assemble`] expands it into a runtime
/// [`GatewayConfig`] whose chains are concrete [`FallbackChainConfig`]s — the
/// exact shape the SP-0 selector already consumes. Pure config → config; no I/O,
/// no persistence.
pub struct CatalogConfig {
    pub routers: HashMap<String, RouterConfig>,
    pub models: HashMap<String, ModelConfig>,
    pub tiers: HashMap<String, TierConfig>,
    pub chains: HashMap<String, TierChain>,
    pub constraints: ConstraintsConfig,
}

/// A chain authored in terms of tier references and/or concrete model entries.
/// [`assemble`] flattens `refs` (in order) into a concrete fallback chain.
pub struct TierChain {
    pub capability: Capability,
    pub refs: Vec<ChainRef>,
    pub fallback_triggers: Vec<FallbackTrigger>,
}

/// One leg of a [`TierChain`]: either a whole tier (expanded to its ordered
/// members) or a single concrete model entry (passed through, validated).
pub enum ChainRef {
    /// Expand the named tier's ordered membership into this position.
    Tier(String),
    /// A concrete model entry, spliced in as-is (its `priority` is reassigned by
    /// final position during assembly).
    Model(ChainEntry),
}

/// Expand an authoring [`CatalogConfig`] into a runtime [`GatewayConfig`].
///
/// For each chain (walked in **sorted id order** for deterministic error
/// output), each `ref` is resolved in order:
/// * `Tier(id)` → `order_members(tier_members(id, …), strategy)`; a missing tier
///   yields [`AssembleError::UnknownTierRef`], a curated member absent from the
///   catalog yields [`AssembleError::UnknownModelInTier`], and a tier resolving
///   to zero members yields [`AssembleError::EmptyTierAfterResolution`].
/// * `Model(entry)` → validated against `catalog.models`; a missing model yields
///   [`AssembleError::UnknownModelRef`].
///
/// The flattened entries are **deduped by model id (first occurrence wins)** and
/// assigned ascending `priority` by final position (1-based). `routers`,
/// `models`, and `constraints` pass through unchanged; `panels`/`consensus`
/// default to empty (SP-CAT does not author them).
///
/// **Fails loud, collecting ALL errors:** every misconfigured ref across every
/// chain is recorded; the function returns `Err(errors)` only after inspecting
/// the whole catalog (mirrors `GatewayBuilder::validate`), never early-returning
/// on the first problem.
pub fn assemble(catalog: CatalogConfig) -> Result<GatewayConfig, Vec<AssembleError>> {
    let mut errors: Vec<AssembleError> = Vec::new();
    let mut chains: HashMap<String, FallbackChainConfig> = HashMap::new();

    // Deterministic iteration: sorted chain ids so the error vector (and any
    // nondeterminism-sensitive output) is stable across runs.
    let mut chain_ids: Vec<&String> = catalog.chains.keys().collect();
    chain_ids.sort();

    for chain_id in chain_ids {
        let tc = &catalog.chains[chain_id];
        let mut entries: Vec<ChainEntry> = Vec::new();

        for r in &tc.refs {
            match r {
                ChainRef::Tier(tier_id) => match catalog.tiers.get(tier_id) {
                    None => errors.push(AssembleError::UnknownTierRef(tier_id.clone())),
                    Some(tier) => match tier_members(tier_id, tier, &catalog.models) {
                        Err(e) => errors.push(e),
                        Ok(members) => {
                            let ordered = order_members(members, &catalog.models, tier.strategy);
                            if ordered.is_empty() {
                                errors
                                    .push(AssembleError::EmptyTierAfterResolution(tier_id.clone()));
                            } else {
                                for model_id in ordered {
                                    entries.push(ChainEntry {
                                        model: model_id,
                                        router: None,
                                        api_model_id: None,
                                        priority: 0, // reassigned by position below
                                    });
                                }
                            }
                        }
                    },
                },
                ChainRef::Model(entry) => {
                    if catalog.models.contains_key(&entry.model) {
                        entries.push(entry.clone());
                    } else {
                        errors.push(AssembleError::UnknownModelRef(entry.model.clone()));
                    }
                }
            }
        }

        // Dedup by model id (first occurrence wins), then assign ascending
        // 1-based priority by final position (saturating so a >255-entry chain
        // degrades gracefully rather than wrapping).
        let mut seen: HashSet<String> = HashSet::new();
        let mut models: Vec<ChainEntry> = Vec::new();
        for entry in entries {
            if seen.insert(entry.model.clone()) {
                models.push(entry);
            }
        }
        for (pos, entry) in models.iter_mut().enumerate() {
            entry.priority = u8::try_from(pos + 1).unwrap_or(u8::MAX);
        }

        chains.insert(
            chain_id.clone(),
            FallbackChainConfig {
                id: chain_id.clone(),
                capability: tc.capability.clone(),
                models,
                fallback_triggers: tc.fallback_triggers.clone(),
            },
        );
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    Ok(GatewayConfig {
        routers: catalog.routers,
        models: catalog.models,
        chains,
        constraints: catalog.constraints,
        panels: Default::default(),
        consensus: Default::default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::RegisterInto;
    use crate::adapters::noop::NoopAdapter;
    use crate::circuit_breaker::{CircuitBreakerConfig, CircuitBreakerManager};
    use crate::engine::Gateway;
    use crate::types::request::{Message, MessageRole, Payload};
    use kernel::adapters::AdapterRegistry;
    use kernel::types::config::{
        CatalogMeta, FreeTier, FreeType, ModelConfig, RouterConfig, TosVerdict,
    };
    use kernel::types::request::InferenceRequest;
    use std::sync::Arc;

    use super::super::tiers::{FreeMatch, IntraTierStrategy, TierPredicate};

    // ---- fixtures ----------------------------------------------------------

    /// A bare TextChat model with the given id and provider, no catalog/pricing.
    fn model(id: &str, provider: &str) -> ModelConfig {
        ModelConfig {
            id: id.into(),
            api_model_id: None,
            provider: provider.into(),
            capabilities: vec![Capability::TextChat],
            context_window: 8000,
            max_output_tokens: 1000,
            pricing: None,
            family: None,
            catalog: None,
        }
    }

    /// A model tagged with a recurring-daily free tier (for `derive` matching).
    fn free_model(id: &str, provider: &str) -> ModelConfig {
        ModelConfig {
            catalog: Some(CatalogMeta {
                free: Some(FreeTier {
                    free_type: FreeType::RecurringDaily,
                    monthly_tokens: None,
                    credit_tokens: None,
                    pool_key: None,
                    tos: TosVerdict::Ok,
                    trains_on_prompts: false,
                }),
                ..Default::default()
            }),
            ..model(id, provider)
        }
    }

    fn map<T>(items: Vec<(&str, T)>) -> HashMap<String, T> {
        items.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    }

    fn priority_tier(members: &[&str]) -> TierConfig {
        TierConfig {
            strategy: IntraTierStrategy::Priority,
            members: members.iter().map(|s| s.to_string()).collect(),
            derive: None,
        }
    }

    fn chain(caps: Capability, refs: Vec<ChainRef>) -> TierChain {
        TierChain {
            capability: caps,
            refs,
            fallback_triggers: vec![FallbackTrigger::RateLimit, FallbackTrigger::Timeout],
        }
    }

    fn tier(name: &str) -> ChainRef {
        ChainRef::Tier(name.to_string())
    }

    /// Extract `(model_id, priority)` for a chain's concrete entries.
    fn chain_shape<'a>(cfg: &'a GatewayConfig, id: &str) -> Vec<(&'a str, u8)> {
        cfg.chains[id]
            .models
            .iter()
            .map(|e| (e.model.as_str(), e.priority))
            .collect()
    }

    // ---- Expansion + ascending priority ------------------------------------

    #[test]
    fn expands_tier_refs_into_concrete_chain_with_ascending_priority() {
        let catalog = CatalogConfig {
            routers: HashMap::new(),
            models: map(vec![
                ("f1", model("f1", "p")),
                ("f2", model("f2", "p")),
                ("c1", model("c1", "p")),
            ]),
            tiers: map(vec![
                ("free", priority_tier(&["f1", "f2"])),
                ("cost_optimized", priority_tier(&["c1"])),
            ]),
            chains: map(vec![(
                "research_bulk",
                chain(
                    Capability::TextChat,
                    vec![tier("free"), tier("cost_optimized")],
                ),
            )]),
            constraints: Default::default(),
        };

        let cfg = assemble(catalog).expect("assembles");
        assert_eq!(
            chain_shape(&cfg, "research_bulk"),
            vec![("f1", 1), ("f2", 2), ("c1", 3)],
        );
        // capability + fallback_triggers carried through onto the concrete chain.
        assert_eq!(cfg.chains["research_bulk"].capability, Capability::TextChat);
        assert_eq!(
            cfg.chains["research_bulk"].fallback_triggers,
            vec![FallbackTrigger::RateLimit, FallbackTrigger::Timeout],
        );
    }

    #[test]
    fn adding_a_model_to_a_tier_updates_every_chain_no_chain_edit() {
        let build = |free_members: &[&str]| CatalogConfig {
            routers: HashMap::new(),
            models: map(vec![
                ("f1", model("f1", "p")),
                ("f2", model("f2", "p")),
                ("f3", model("f3", "p")),
            ]),
            tiers: map(vec![("free", priority_tier(free_members))]),
            // The chain is IDENTICAL in both assemblies — only the tier changes.
            chains: map(vec![(
                "research_bulk",
                chain(Capability::TextChat, vec![tier("free")]),
            )]),
            constraints: Default::default(),
        };

        let before = assemble(build(&["f1", "f2"])).expect("assembles");
        assert_eq!(
            chain_shape(&before, "research_bulk"),
            vec![("f1", 1), ("f2", 2)],
        );

        // Add f3 to the tier's members; the chain definition is untouched.
        let after = assemble(build(&["f1", "f2", "f3"])).expect("assembles");
        assert_eq!(
            chain_shape(&after, "research_bulk"),
            vec![("f1", 1), ("f2", 2), ("f3", 3)],
        );
    }

    #[test]
    fn derived_tier_membership_flows_through_into_the_chain() {
        // `free` tier has NO curated members — membership is purely derived from
        // the free-tier predicate, and those models land in the expanded chain.
        let derive_tier = TierConfig {
            strategy: IntraTierStrategy::Priority,
            members: vec![],
            derive: Some(TierPredicate {
                free: Some(FreeMatch::Any),
                ..Default::default()
            }),
        };
        let catalog = CatalogConfig {
            routers: HashMap::new(),
            models: map(vec![
                ("g_free", free_model("g_free", "p")),
                ("a_free", free_model("a_free", "p")),
                ("paid", model("paid", "p")),
            ]),
            tiers: map(vec![("free", derive_tier)]),
            chains: map(vec![(
                "research_bulk",
                chain(Capability::TextChat, vec![tier("free")]),
            )]),
            constraints: Default::default(),
        };

        let cfg = assemble(catalog).expect("assembles");
        // Derived matches are sorted by id (deterministic); `paid` excluded.
        assert_eq!(
            chain_shape(&cfg, "research_bulk"),
            vec![("a_free", 1), ("g_free", 2)],
        );
    }

    #[test]
    fn cross_tier_duplicate_appears_once_first_tier_wins_position() {
        // `shared` is in BOTH tiers; it must appear once, at its FIRST (free)
        // position — not again when `cost_optimized` is expanded.
        let catalog = CatalogConfig {
            routers: HashMap::new(),
            models: map(vec![
                ("f1", model("f1", "p")),
                ("shared", model("shared", "p")),
                ("c1", model("c1", "p")),
            ]),
            tiers: map(vec![
                ("free", priority_tier(&["f1", "shared"])),
                ("cost_optimized", priority_tier(&["shared", "c1"])),
            ]),
            chains: map(vec![(
                "research_bulk",
                chain(
                    Capability::TextChat,
                    vec![tier("free"), tier("cost_optimized")],
                ),
            )]),
            constraints: Default::default(),
        };

        let cfg = assemble(catalog).expect("assembles");
        assert_eq!(
            chain_shape(&cfg, "research_bulk"),
            vec![("f1", 1), ("shared", 2), ("c1", 3)],
        );
        // `shared` appears exactly once.
        assert_eq!(
            cfg.chains["research_bulk"]
                .models
                .iter()
                .filter(|e| e.model == "shared")
                .count(),
            1,
        );
    }

    #[test]
    fn concrete_and_tier_refs_mix_in_order() {
        let catalog = CatalogConfig {
            routers: HashMap::new(),
            models: map(vec![
                ("c_special", model("c_special", "p")),
                ("f1", model("f1", "p")),
                ("f2", model("f2", "p")),
            ]),
            tiers: map(vec![("free", priority_tier(&["f1", "f2"]))]),
            chains: map(vec![(
                "research_bulk",
                chain(
                    Capability::TextChat,
                    vec![
                        ChainRef::Model(ChainEntry {
                            model: "c_special".into(),
                            router: Some("special_router".into()),
                            api_model_id: Some("c-special-v9".into()),
                            priority: 99, // reassigned by position
                        }),
                        tier("free"),
                    ],
                ),
            )]),
            constraints: Default::default(),
        };

        let cfg = assemble(catalog).expect("assembles");
        assert_eq!(
            chain_shape(&cfg, "research_bulk"),
            vec![("c_special", 1), ("f1", 2), ("f2", 3)],
        );
        // The concrete entry keeps its router / api_model_id; only priority is
        // re-derived by position.
        let special = &cfg.chains["research_bulk"].models[0];
        assert_eq!(special.router.as_deref(), Some("special_router"));
        assert_eq!(special.api_model_id.as_deref(), Some("c-special-v9"));
    }

    // ---- Loud failure: collect ALL errors ----------------------------------

    #[test]
    fn collects_all_errors_across_all_chains_not_just_the_first() {
        // Four distinct failure modes across four chains. Chains iterate in
        // SORTED id order, so the collected error vector is deterministic.
        let catalog = CatalogConfig {
            routers: HashMap::new(),
            models: map(vec![("real", model("real", "p"))]),
            tiers: map(vec![
                // Resolves to zero members (empty curated, no derive).
                ("empty", priority_tier(&[])),
                // Curated member absent from the catalog.
                ("bad_curated", priority_tier(&["ghost_model"])),
            ]),
            chains: map(vec![
                (
                    "a_unknown_tier",
                    chain(Capability::TextChat, vec![tier("nonexistent_tier")]),
                ),
                (
                    "b_unknown_model",
                    chain(
                        Capability::TextChat,
                        vec![ChainRef::Model(ChainEntry {
                            model: "missing_model".into(),
                            router: None,
                            api_model_id: None,
                            priority: 1,
                        })],
                    ),
                ),
                (
                    "c_empty_tier",
                    chain(Capability::TextChat, vec![tier("empty")]),
                ),
                (
                    "d_bad_curated",
                    chain(Capability::TextChat, vec![tier("bad_curated")]),
                ),
            ]),
            constraints: Default::default(),
        };

        let errors = assemble(catalog).expect_err("must fail loud");
        // ALL four collected — proves it is NOT first-error-only.
        assert_eq!(
            errors,
            vec![
                AssembleError::UnknownTierRef("nonexistent_tier".into()),
                AssembleError::UnknownModelRef("missing_model".into()),
                AssembleError::EmptyTierAfterResolution("empty".into()),
                AssembleError::UnknownModelInTier {
                    tier: "bad_curated".into(),
                    model: "ghost_model".into(),
                },
            ],
        );
        assert!(errors.len() >= 2, "collect-all pins >= 2 errors");
    }

    #[test]
    fn unknown_tier_ref_alone_fails() {
        let catalog = CatalogConfig {
            routers: HashMap::new(),
            models: HashMap::new(),
            tiers: HashMap::new(),
            chains: map(vec![(
                "x",
                chain(Capability::TextChat, vec![tier("ghost")]),
            )]),
            constraints: Default::default(),
        };
        assert_eq!(
            assemble(catalog).unwrap_err(),
            vec![AssembleError::UnknownTierRef("ghost".into())],
        );
    }

    // ---- Backward-compat ---------------------------------------------------

    #[test]
    fn no_tiers_all_concrete_equals_hand_authored_gateway_config() {
        let router = RouterConfig {
            url: "http://localhost".into(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        };
        let routers = map(vec![("noop", router)]);
        let models = map(vec![
            ("m1", model("m1", "noop")),
            ("m2", model("m2", "noop")),
        ]);

        let catalog = CatalogConfig {
            routers: routers.clone(),
            models: models.clone(),
            tiers: HashMap::new(),
            chains: map(vec![(
                "plain",
                chain(
                    Capability::TextChat,
                    vec![
                        ChainRef::Model(ChainEntry {
                            model: "m1".into(),
                            router: Some("noop".into()),
                            api_model_id: None,
                            priority: 1,
                        }),
                        ChainRef::Model(ChainEntry {
                            model: "m2".into(),
                            router: Some("noop".into()),
                            api_model_id: None,
                            priority: 2,
                        }),
                    ],
                ),
            )]),
            constraints: Default::default(),
        };

        let assembled = assemble(catalog).expect("assembles");

        // Hand-authored equivalent GatewayConfig.
        let mut hand_chains = HashMap::new();
        hand_chains.insert(
            "plain".to_string(),
            FallbackChainConfig {
                id: "plain".into(),
                capability: Capability::TextChat,
                models: vec![
                    ChainEntry {
                        model: "m1".into(),
                        router: Some("noop".into()),
                        api_model_id: None,
                        priority: 1,
                    },
                    ChainEntry {
                        model: "m2".into(),
                        router: Some("noop".into()),
                        api_model_id: None,
                        priority: 2,
                    },
                ],
                fallback_triggers: vec![FallbackTrigger::RateLimit, FallbackTrigger::Timeout],
            },
        );
        let hand = GatewayConfig {
            routers,
            models,
            chains: hand_chains,
            constraints: Default::default(),
            panels: Default::default(),
            consensus: Default::default(),
        };

        // serde_json has preserve_order OFF ⇒ map equality is order-insensitive,
        // so this compares content, not HashMap iteration order.
        assert_eq!(
            serde_json::to_value(&assembled).unwrap(),
            serde_json::to_value(&hand).unwrap(),
        );
    }

    // ---- Dynamic-strategy stub honesty -------------------------------------

    #[test]
    fn headroom_strategy_tier_assembles_in_priority_order() {
        assert!(IntraTierStrategy::Headroom.is_dynamic());

        let headroom_tier = TierConfig {
            strategy: IntraTierStrategy::Headroom,
            members: vec!["f1".into(), "f2".into()],
            derive: None,
        };
        let catalog = CatalogConfig {
            routers: HashMap::new(),
            models: map(vec![("f1", model("f1", "p")), ("f2", model("f2", "p"))]),
            tiers: map(vec![("free", headroom_tier)]),
            chains: map(vec![(
                "research_bulk",
                chain(Capability::TextChat, vec![tier("free")]),
            )]),
            constraints: Default::default(),
        };

        // Dynamic strategy stubs to Priority (order_members warns) and assembles.
        let cfg = assemble(catalog).expect("assembles despite dynamic strategy");
        assert_eq!(
            chain_shape(&cfg, "research_bulk"),
            vec![("f1", 1), ("f2", 2)],
        );
    }

    // ---- End-to-end: assembled config drives real SP-0 selection -----------

    #[tokio::test]
    async fn assembled_chain_drives_real_gateway_execution() {
        // A `free` tier of two noop-servable models, referenced by a
        // `research_bulk` chain. `provider = "noop"` makes the engine resolve the
        // (router: None) chain entries to the "noop" router → the NoopAdapter.
        let router = RouterConfig {
            url: "http://localhost".into(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        };
        let catalog = CatalogConfig {
            routers: map(vec![("noop", router)]),
            models: map(vec![
                ("free_primary", model("free_primary", "noop")),
                ("free_backup", model("free_backup", "noop")),
            ]),
            tiers: map(vec![(
                "free",
                priority_tier(&["free_primary", "free_backup"]),
            )]),
            chains: map(vec![(
                "research_bulk",
                chain(Capability::TextChat, vec![tier("free")]),
            )]),
            constraints: Default::default(),
        };

        let config = assemble(catalog).expect("assembles");
        // Sanity: the concrete chain the selector will consume.
        assert_eq!(
            chain_shape(&config, "research_bulk"),
            vec![("free_primary", 1), ("free_backup", 2)],
        );

        let gw = Gateway::new(
            config,
            AdapterRegistry::new(),
            CircuitBreakerManager::new(CircuitBreakerConfig::default()),
        );
        Arc::new(NoopAdapter).register_into(&gw.adapters).await;

        let request = InferenceRequest {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("research_bulk".to_string()),
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
        // The assembled concrete chain drove SP-0 selection: the FIRST free model
        // served the request (unchanged selection pipeline).
        assert_eq!(response.model, Some("free_primary".to_string()));
    }
}
