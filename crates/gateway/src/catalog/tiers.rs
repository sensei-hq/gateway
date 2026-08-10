use kernel::types::capability::Capability;
use kernel::types::config::{AuthType, CostBand, FreeType, Locality, ModelConfig};
use std::collections::HashMap;

/// How a tier orders its members once membership is resolved.
///
/// `Priority`/`Cost` are pure functions of config. `Headroom`/`LeastUsed` need
/// *live usage* (remaining free budget / recent call counts), which requires the
/// persistence layer that SP-CAT deliberately holds off — so they stub to
/// `Priority` with a loud warning. Callers detect the degrade via
/// [`IntraTierStrategy::is_dynamic`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IntraTierStrategy {
    Priority,
    Cost,
    Headroom,
    LeastUsed,
}

impl IntraTierStrategy {
    /// Dynamic strategies need live usage (persistence, held off) → they stub to
    /// `Priority`. Callers use this to know the ordering degraded (rather than
    /// the stub being silent).
    pub fn is_dynamic(self) -> bool {
        matches!(
            self,
            IntraTierStrategy::Headroom | IntraTierStrategy::LeastUsed
        )
    }
}

/// How a tier's `derive` predicate matches a model's free-tier terms.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FreeMatch {
    /// Any declared free tier, regardless of shape.
    Any,
    /// A specific free-tier recurrence/shape.
    Type(FreeType),
}

/// An attribute predicate for deriving tier membership. A model matches iff every
/// PRESENT field matches (logical AND); an absent field is don't-care.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct TierPredicate {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub free: Option<FreeMatch>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_type: Option<AuthType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_band: Option<CostBand>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capability: Option<Capability>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locality: Option<Locality>,
    /// Required tags: a model matches iff its catalog carries EVERY listed tag
    /// (AND/subset). Absent ⇒ don't-care; a model with no catalog (or no tags)
    /// matches only an absent or empty tag predicate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
}

/// A tier: an intra-tier ordering strategy plus its membership sources —
/// a curated id list (ordered) unioned with an optional attribute predicate.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TierConfig {
    pub strategy: IntraTierStrategy,
    /// Curated model ids, in order. Each must exist in the catalog.
    #[serde(default)]
    pub members: Vec<String>,
    /// Optional attribute predicate; matching models join the tier after the
    /// curated members.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derive: Option<TierPredicate>,
}

/// Errors from tier resolution / assembly. `assemble` (Task 5) collects these
/// across every chain before returning, so one misconfigured catalog surfaces
/// all of its problems at once rather than only the first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssembleError {
    /// A curated tier member references a model id absent from the catalog.
    UnknownModelInTier { tier: String, model: String },
    /// A chain's `Tier(id)` ref names a tier absent from the catalog.
    UnknownTierRef(String),
    /// A chain's concrete `Model(entry)` ref names a model absent from the catalog.
    UnknownModelRef(String),
    /// A referenced tier resolved to zero members (empty curated list and no
    /// derived matches) — a chain leg that would contribute nothing.
    EmptyTierAfterResolution(String),
}

impl std::fmt::Display for AssembleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AssembleError::UnknownModelInTier { tier, model } => {
                write!(f, "tier '{tier}' references unknown model '{model}'")
            }
            AssembleError::UnknownTierRef(tier) => {
                write!(f, "chain references unknown tier '{tier}'")
            }
            AssembleError::UnknownModelRef(model) => {
                write!(f, "chain references unknown model '{model}'")
            }
            AssembleError::EmptyTierAfterResolution(tier) => {
                write!(f, "tier '{tier}' resolved to zero members")
            }
        }
    }
}

impl std::error::Error for AssembleError {}

/// A model matches a predicate iff every PRESENT field matches (AND); an absent
/// field is don't-care. Pure; each field's check is evaluated lazily.
fn matches(pred: &TierPredicate, model: &ModelConfig) -> bool {
    let free_ok = match &pred.free {
        None => true,
        Some(fm) => {
            let f = model.catalog.as_ref().and_then(|c| c.free.as_ref());
            match fm {
                FreeMatch::Any => f.is_some(),
                FreeMatch::Type(t) => f.map(|x| x.free_type) == Some(*t),
            }
        }
    };
    let auth_ok = pred
        .auth_type
        .is_none_or(|a| model.catalog.as_ref().and_then(|c| c.auth_type) == Some(a));
    let band_ok = pred.cost_band.is_none_or(|b| super::cost_band(model) == b);
    let cap_ok = pred
        .capability
        .as_ref()
        .is_none_or(|c| model.capabilities.contains(c));
    let loc_ok = pred
        .locality
        .is_none_or(|l| model.catalog.as_ref().and_then(|c| c.locality) == Some(l));
    let tags_ok = pred.tags.as_ref().is_none_or(|req| {
        let have = model
            .catalog
            .as_ref()
            .map(|c| c.tags.as_slice())
            .unwrap_or(&[]);
        req.iter().all(|t| have.contains(t)) // every required tag present (AND); absent pred = pass
    });
    free_ok && auth_ok && band_ok && cap_ok && loc_ok && tags_ok
}

/// Explicit ordering rank for a cost band (ascending = cheaper first).
fn band_rank(model: &ModelConfig) -> u8 {
    match super::cost_band(model) {
        CostBand::Free => 0,
        CostBand::Low => 1,
        CostBand::Mid => 2,
        CostBand::High => 3,
    }
}

/// Blended `$/1k` pricing (`input_per_1k + output_per_1k`); `0.0` when unpriced.
fn blended_price(model: &ModelConfig) -> f64 {
    model
        .pricing
        .as_ref()
        .map_or(0.0, |p| p.input_per_1k + p.output_per_1k)
}

/// The `(band_rank, blended_price)` sort key for a model id; a missing id sorts
/// last so `order_members` stays total and pure over arbitrary ids.
fn cost_key(id: &str, models: &HashMap<String, ModelConfig>) -> (u8, f64) {
    match models.get(id) {
        Some(m) => (band_rank(m), blended_price(m)),
        None => (u8::MAX, f64::INFINITY),
    }
}

/// Resolve a tier's members: curated ids (in order; each must exist) ∪ derived
/// matches (sorted by id for determinism), deduped by id with the curated entry
/// winning. Returns member ids in membership order.
pub fn tier_members(
    tier_id: &str,
    tier: &TierConfig,
    models: &HashMap<String, ModelConfig>,
) -> Result<Vec<String>, AssembleError> {
    let mut out: Vec<String> = Vec::new();
    for id in &tier.members {
        if !models.contains_key(id) {
            return Err(AssembleError::UnknownModelInTier {
                tier: tier_id.into(),
                model: id.clone(),
            });
        }
        if !out.contains(id) {
            out.push(id.clone());
        }
    }
    if let Some(pred) = &tier.derive {
        let mut derived: Vec<String> = models
            .iter()
            .filter(|(_, m)| matches(pred, m))
            .map(|(id, _)| id.clone())
            .collect();
        derived.sort(); // determinism: no HashMap-iteration order leak
        for id in derived {
            if !out.contains(&id) {
                out.push(id);
            }
        }
    }
    Ok(out)
}

/// Order a tier's resolved members. `Priority` keeps membership order; `Cost`
/// sorts ascending by `(cost_band, blended pricing, id)`. `Headroom`/`LeastUsed`
/// need live usage (persistence, held off) → they fall back to `Priority` and
/// emit a `tracing::warn!` (callers also detect the degrade via
/// [`IntraTierStrategy::is_dynamic`]). Pure apart from the warning.
pub fn order_members(
    mut ids: Vec<String>,
    models: &HashMap<String, ModelConfig>,
    strategy: IntraTierStrategy,
) -> Vec<String> {
    match strategy {
        IntraTierStrategy::Priority => ids,
        IntraTierStrategy::Cost => {
            // Stable, total order: ascending band, then blended price, then id.
            ids.sort_by(|a, b| {
                let (ba, pa) = cost_key(a, models);
                let (bb, pb) = cost_key(b, models);
                ba.cmp(&bb).then(pa.total_cmp(&pb)).then_with(|| a.cmp(b))
            });
            ids
        }
        IntraTierStrategy::Headroom | IntraTierStrategy::LeastUsed => {
            tracing::warn!(
                ?strategy,
                "dynamic intra-tier strategy not live in config-driven mode; using Priority"
            );
            ids
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kernel::types::capability::Capability;
    use kernel::types::config::{
        AuthType, CatalogMeta, CostBand, FreeTier, FreeType, Locality, ModelConfig, ModelPricing,
        TosVerdict,
    };
    use std::collections::HashMap;

    /// Terse builder: a bare model with the given id, no catalog, no pricing,
    /// no capabilities. Callers override fields via struct-update syntax.
    fn base(id: &str) -> ModelConfig {
        ModelConfig {
            id: id.into(),
            api_model_id: None,
            provider: "p".into(),
            capabilities: vec![],
            context_window: 8000,
            max_output_tokens: 1000,
            pricing: None,
            family: None,
            catalog: None,
        }
    }

    /// Minimal free-tier terms carrying only the recurrence shape.
    fn free(free_type: FreeType) -> FreeTier {
        FreeTier {
            free_type,
            monthly_tokens: None,
            credit_tokens: None,
            pool_key: None,
            tos: TosVerdict::Ok,
            trains_on_prompts: false,
        }
    }

    /// A blended-priced model (`input == output == half`) to land a cost band.
    fn priced(id: &str, blended: f64) -> ModelConfig {
        ModelConfig {
            pricing: Some(ModelPricing {
                input_per_1k: blended / 2.0,
                output_per_1k: blended / 2.0,
                per_request: None,
            }),
            ..base(id)
        }
    }

    fn map(models: Vec<ModelConfig>) -> HashMap<String, ModelConfig> {
        models.into_iter().map(|m| (m.id.clone(), m)).collect()
    }

    fn ids(xs: &[&str]) -> Vec<String> {
        xs.iter().map(|s| s.to_string()).collect()
    }

    // ---- Curated membership ------------------------------------------------

    #[test]
    fn curated_membership_resolves_in_order() {
        let models = map(vec![base("a"), base("b"), base("c")]);
        let tier = TierConfig {
            strategy: IntraTierStrategy::Priority,
            members: ids(&["c", "a"]),
            derive: None,
        };
        assert_eq!(tier_members("t", &tier, &models).unwrap(), ids(&["c", "a"]));
    }

    #[test]
    fn unknown_curated_id_errors() {
        let models = map(vec![base("a")]);
        let tier = TierConfig {
            strategy: IntraTierStrategy::Priority,
            members: ids(&["a", "ghost"]),
            derive: None,
        };
        assert_eq!(
            tier_members("mytier", &tier, &models).unwrap_err(),
            AssembleError::UnknownModelInTier {
                tier: "mytier".into(),
                model: "ghost".into(),
            }
        );
    }

    // ---- Derived membership: each attribute independently ------------------

    fn derive_only(pred: TierPredicate) -> TierConfig {
        TierConfig {
            strategy: IntraTierStrategy::Priority,
            members: vec![],
            derive: Some(pred),
        }
    }

    fn with_catalog(id: &str, meta: CatalogMeta) -> ModelConfig {
        ModelConfig {
            catalog: Some(meta),
            ..base(id)
        }
    }

    #[test]
    fn derived_match_free_any() {
        let freed = with_catalog(
            "freed",
            CatalogMeta {
                free: Some(free(FreeType::RecurringDaily)),
                ..Default::default()
            },
        );
        let models = map(vec![base("paid"), freed]);
        let tier = derive_only(TierPredicate {
            free: Some(FreeMatch::Any),
            ..Default::default()
        });
        assert_eq!(tier_members("t", &tier, &models).unwrap(), ids(&["freed"]));
    }

    #[test]
    fn derived_match_free_type() {
        let daily = with_catalog(
            "daily",
            CatalogMeta {
                free: Some(free(FreeType::RecurringDaily)),
                ..Default::default()
            },
        );
        let monthly = with_catalog(
            "monthly",
            CatalogMeta {
                free: Some(free(FreeType::RecurringMonthly)),
                ..Default::default()
            },
        );
        let models = map(vec![daily, monthly, base("none")]);
        let tier = derive_only(TierPredicate {
            free: Some(FreeMatch::Type(FreeType::RecurringDaily)),
            ..Default::default()
        });
        assert_eq!(tier_members("t", &tier, &models).unwrap(), ids(&["daily"]));
    }

    #[test]
    fn derived_match_auth_type() {
        let oauth = with_catalog(
            "oauth",
            CatalogMeta {
                auth_type: Some(AuthType::OauthCli),
                ..Default::default()
            },
        );
        let apikey = with_catalog(
            "apikey",
            CatalogMeta {
                auth_type: Some(AuthType::ApiKey),
                ..Default::default()
            },
        );
        let models = map(vec![oauth, apikey, base("none")]);
        let tier = derive_only(TierPredicate {
            auth_type: Some(AuthType::OauthCli),
            ..Default::default()
        });
        assert_eq!(tier_members("t", &tier, &models).unwrap(), ids(&["oauth"]));
    }

    #[test]
    fn derived_match_cost_band_uses_derivation() {
        // `high` gets its band from pricing (blended 0.10 → High); `cheap` has no
        // pricing → Free. Matching High therefore proves `matches` runs the
        // `cost_band()` derivation, not just a stored override.
        let high = priced("high", 0.10);
        let models = map(vec![high, base("cheap")]);
        let tier = derive_only(TierPredicate {
            cost_band: Some(CostBand::High),
            ..Default::default()
        });
        assert_eq!(tier_members("t", &tier, &models).unwrap(), ids(&["high"]));
    }

    #[test]
    fn derived_match_capability() {
        let vision = ModelConfig {
            capabilities: vec![Capability::ImageAnalyze, Capability::TextChat],
            ..base("vision")
        };
        let chat = ModelConfig {
            capabilities: vec![Capability::TextChat],
            ..base("chat")
        };
        let models = map(vec![vision, chat]);
        let tier = derive_only(TierPredicate {
            capability: Some(Capability::ImageAnalyze),
            ..Default::default()
        });
        assert_eq!(tier_members("t", &tier, &models).unwrap(), ids(&["vision"]));
    }

    #[test]
    fn derived_match_locality() {
        let local = with_catalog(
            "local",
            CatalogMeta {
                locality: Some(Locality::Local),
                ..Default::default()
            },
        );
        let cloud = with_catalog(
            "cloud",
            CatalogMeta {
                locality: Some(Locality::Cloud),
                ..Default::default()
            },
        );
        let models = map(vec![local, cloud]);
        let tier = derive_only(TierPredicate {
            locality: Some(Locality::Local),
            ..Default::default()
        });
        assert_eq!(tier_members("t", &tier, &models).unwrap(), ids(&["local"]));
    }

    #[test]
    fn derived_multi_field_predicate_is_and() {
        // Only `both` satisfies free=Any AND locality=Cloud.
        let both = with_catalog(
            "both",
            CatalogMeta {
                free: Some(free(FreeType::RecurringDaily)),
                locality: Some(Locality::Cloud),
                ..Default::default()
            },
        );
        let free_local = with_catalog(
            "free_local",
            CatalogMeta {
                free: Some(free(FreeType::RecurringDaily)),
                locality: Some(Locality::Local),
                ..Default::default()
            },
        );
        let cloud_paid = with_catalog(
            "cloud_paid",
            CatalogMeta {
                locality: Some(Locality::Cloud),
                ..Default::default()
            },
        );
        let models = map(vec![both, free_local, cloud_paid]);
        let tier = derive_only(TierPredicate {
            free: Some(FreeMatch::Any),
            locality: Some(Locality::Cloud),
            ..Default::default()
        });
        assert_eq!(tier_members("t", &tier, &models).unwrap(), ids(&["both"]));
    }

    // ---- Derived membership: tags (AND / subset) ---------------------------

    #[test]
    fn derived_match_tags_single() {
        // A model carrying the required tag matches; models missing it (a
        // different tag, or no catalog at all) are excluded.
        let reasoning = with_catalog(
            "reasoning",
            CatalogMeta {
                tags: vec!["reasoning".into()],
                ..Default::default()
            },
        );
        let other = with_catalog(
            "other",
            CatalogMeta {
                tags: vec!["chat".into()],
                ..Default::default()
            },
        );
        let models = map(vec![reasoning, other, base("bare")]);
        let tier = derive_only(TierPredicate {
            tags: Some(vec!["reasoning".into()]),
            ..Default::default()
        });
        assert_eq!(
            tier_members("t", &tier, &models).unwrap(),
            ids(&["reasoning"])
        );
    }

    #[test]
    fn derived_match_tags_requires_all() {
        // A two-tag predicate is an AND/subset check: a model must carry BOTH
        // tags; one with only a single required tag is excluded.
        let both = with_catalog(
            "both",
            CatalogMeta {
                tags: vec!["reasoning".into(), "frontier".into()],
                ..Default::default()
            },
        );
        let one = with_catalog(
            "one",
            CatalogMeta {
                tags: vec!["reasoning".into()],
                ..Default::default()
            },
        );
        let models = map(vec![both, one]);
        let tier = derive_only(TierPredicate {
            tags: Some(vec!["reasoning".into(), "frontier".into()]),
            ..Default::default()
        });
        assert_eq!(tier_members("t", &tier, &models).unwrap(), ids(&["both"]));
    }

    #[test]
    fn derived_match_tags_and_auth_type() {
        // Combined AND across fields: tag "reasoning" AND auth OauthCli. The
        // tagged+oauth model matches; a tagged model with a different auth_type
        // is excluded (the `premium-reasoning` tier shape).
        let premium = with_catalog(
            "premium",
            CatalogMeta {
                tags: vec!["reasoning".into()],
                auth_type: Some(AuthType::OauthCli),
                ..Default::default()
            },
        );
        let tagged_apikey = with_catalog(
            "tagged_apikey",
            CatalogMeta {
                tags: vec!["reasoning".into()],
                auth_type: Some(AuthType::ApiKey),
                ..Default::default()
            },
        );
        let models = map(vec![premium, tagged_apikey]);
        let tier = derive_only(TierPredicate {
            auth_type: Some(AuthType::OauthCli),
            tags: Some(vec!["reasoning".into()]),
            ..Default::default()
        });
        assert_eq!(
            tier_members("t", &tier, &models).unwrap(),
            ids(&["premium"])
        );
    }

    // ---- Union + dedup (curated wins) --------------------------------------

    #[test]
    fn union_dedup_curated_first() {
        // `shared` is BOTH curated and a derived match; `derived_only` matches
        // only via derive; `curated_only` joins only via the curated list.
        let shared = with_catalog(
            "shared",
            CatalogMeta {
                free: Some(free(FreeType::RecurringDaily)),
                ..Default::default()
            },
        );
        let derived_only = with_catalog(
            "derived_only",
            CatalogMeta {
                free: Some(free(FreeType::RecurringDaily)),
                ..Default::default()
            },
        );
        let models = map(vec![shared, derived_only, base("curated_only")]);
        let tier = TierConfig {
            strategy: IntraTierStrategy::Priority,
            members: ids(&["curated_only", "shared"]),
            derive: Some(TierPredicate {
                free: Some(FreeMatch::Any),
                ..Default::default()
            }),
        };
        let got = tier_members("t", &tier, &models).unwrap();
        // Curated first (in curated order), then derived (sorted by id) minus dupes.
        assert_eq!(got, ids(&["curated_only", "shared", "derived_only"]));
        // `shared` appears exactly once.
        assert_eq!(got.iter().filter(|x| x.as_str() == "shared").count(), 1);
    }

    // ---- Determinism -------------------------------------------------------

    #[test]
    fn derived_membership_is_deterministic() {
        // Ids inserted out of order; derived matches must come back sorted and
        // identical across repeated calls (no HashMap-iteration order leak).
        let mut v = vec![];
        for id in ["m3", "m1", "m2", "m5", "m4"] {
            v.push(with_catalog(
                id,
                CatalogMeta {
                    free: Some(free(FreeType::RecurringDaily)),
                    ..Default::default()
                },
            ));
        }
        let models = map(v);
        let tier = derive_only(TierPredicate {
            free: Some(FreeMatch::Any),
            ..Default::default()
        });
        let first = tier_members("t", &tier, &models).unwrap();
        assert_eq!(first, ids(&["m1", "m2", "m3", "m4", "m5"]));
        for _ in 0..10 {
            assert_eq!(tier_members("t", &tier, &models).unwrap(), first);
        }
    }

    // ---- Ordering ----------------------------------------------------------

    #[test]
    fn order_priority_keeps_membership_order() {
        let models = map(vec![base("a"), base("b"), base("c")]);
        let membership = ids(&["c", "a", "b"]);
        assert_eq!(
            order_members(membership.clone(), &models, IntraTierStrategy::Priority),
            membership
        );
    }

    #[test]
    fn order_cost_ascending_band_dominates_id() {
        // z_free (Free, id sorts LAST) must still lead on band; then Low/Mid/High.
        let models = map(vec![
            base("z_free"),       // Free (no pricing)
            priced("low", 0.001), // Low
            priced("mid", 0.01),  // Mid
            priced("high", 0.10), // High
        ]);
        let scrambled = ids(&["high", "mid", "low", "z_free"]);
        assert_eq!(
            order_members(scrambled, &models, IntraTierStrategy::Cost),
            ids(&["z_free", "low", "mid", "high"])
        );
    }

    #[test]
    fn order_cost_secondary_price_then_id_tiebreak() {
        // All three land in the SAME band (Mid), isolating the price then id keys.
        let cheap = priced("b_cheap", 0.003); // Mid, price 0.003
        let same = priced("x_same", 0.003); // Mid, price 0.003 (id tiebreak vs cheap)
        let dear = priced("a_dear", 0.010); // Mid, price 0.010
        let models = map(vec![cheap, same, dear]);
        let scrambled = ids(&["a_dear", "x_same", "b_cheap"]);
        // price asc: 0.003 group {b_cheap, x_same} by id, then 0.010 a_dear.
        assert_eq!(
            order_members(scrambled, &models, IntraTierStrategy::Cost),
            ids(&["b_cheap", "x_same", "a_dear"])
        );
    }

    #[test]
    fn order_cost_is_deterministic() {
        let models = map(vec![
            priced("d", 0.05),
            base("a"),
            priced("c", 0.001),
            priced("b", 0.05),
        ]);
        let input = ids(&["a", "b", "c", "d"]);
        let once = order_members(input.clone(), &models, IntraTierStrategy::Cost);
        for _ in 0..10 {
            assert_eq!(
                order_members(input.clone(), &models, IntraTierStrategy::Cost),
                once
            );
        }
    }

    #[test]
    fn dynamic_strategies_stub_to_priority_and_are_detectable() {
        // Membership order (expensive first) deliberately DIFFERS from Cost order,
        // so a stub returning Priority is distinguishable from silent Cost sorting.
        let models = map(vec![priced("expensive", 0.10), base("free_one")]);
        let membership = ids(&["expensive", "free_one"]);
        let priority = order_members(membership.clone(), &models, IntraTierStrategy::Priority);
        let cost = order_members(membership.clone(), &models, IntraTierStrategy::Cost);
        assert_ne!(
            priority, cost,
            "fixture must distinguish Priority from Cost"
        );

        for strat in [IntraTierStrategy::Headroom, IntraTierStrategy::LeastUsed] {
            // Stub yields Priority order …
            assert_eq!(order_members(membership.clone(), &models, strat), priority);
            // … and NOT a silent Cost sort …
            assert_ne!(order_members(membership.clone(), &models, strat), cost);
            // … and is flagged dynamic so callers know it degraded.
            assert!(strat.is_dynamic());
        }
        assert!(!IntraTierStrategy::Priority.is_dynamic());
        assert!(!IntraTierStrategy::Cost.is_dynamic());
    }
}
