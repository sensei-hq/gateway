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
    AuthType, ConstraintsConfig, CostBand, FallbackTrigger, Locality, ModelConfig, RouterConfig,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::tier_members;
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
}
