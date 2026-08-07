use crate::types::config::{FreeType, ModelConfig};
use std::collections::{BTreeMap, HashMap};

/// Pool-deduped free-tier headline over a catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreeTierTotals {
    /// Sum of recurring free budgets, counting each `pool_key` ONCE (the MAX
    /// declared within the pool). Excludes one-time/credit and uncapped.
    pub steady_recurring_tokens: u64,
    /// `OneTimeInitial`/`RecurringCredit` `credit_tokens` — reported, not steady.
    pub one_time_tokens: u64,
    /// `RecurringUncapped` model ids (listed, never summed) — SORTED.
    pub uncapped_providers: Vec<String>,
    /// `pool_key` → budget counted once (MAX within pool) — SORTED by key.
    pub pools: Vec<(String, u64)>,
}

/// Pool-deduped free-tier headline. Steady recurring = Σ recurring free budgets,
/// counting each `pool_key` ONCE (the MAX declared within the pool, so an
/// under-declared duplicate can't shrink it); `RecurringUncapped` and
/// one-time/credit are EXCLUDED from steady and reported separately;
/// `Discontinued` and `Keyless` are ignored for the token headline —
/// `Keyless` because it carries no documented token budget. Deterministic
/// (`pools`/`uncapped_providers` are sorted; no HashMap-iteration order leaks
/// into the result). See `docs/features/catalog/free-tier-catalog.md`.
pub fn free_tier_totals(models: &HashMap<String, ModelConfig>) -> FreeTierTotals {
    let mut non_pooled_sum: u64 = 0;
    let mut one_time_tokens: u64 = 0;
    let mut uncapped_providers: Vec<String> = Vec::new();
    // BTreeMap keeps pools sorted by key for deterministic output.
    let mut pools: BTreeMap<String, u64> = BTreeMap::new();

    for (id, model) in models {
        let Some(free) = model.catalog.as_ref().and_then(|c| c.free.as_ref()) else {
            continue;
        };
        match free.free_type {
            FreeType::RecurringDaily | FreeType::RecurringMonthly => {
                let budget = free.monthly_tokens.unwrap_or(0);
                match free.pool_key.as_ref() {
                    // Count each pool once at its MAX declared budget.
                    Some(pool) => {
                        let entry = pools.entry(pool.clone()).or_insert(0);
                        *entry = (*entry).max(budget);
                    }
                    None => non_pooled_sum = non_pooled_sum.saturating_add(budget),
                }
            }
            FreeType::RecurringUncapped => uncapped_providers.push(id.clone()),
            FreeType::OneTimeInitial | FreeType::RecurringCredit => {
                one_time_tokens = one_time_tokens.saturating_add(free.credit_tokens.unwrap_or(0));
            }
            // No documented steady token budget → excluded from the headline.
            FreeType::Keyless | FreeType::Discontinued => {}
        }
    }

    uncapped_providers.sort();
    let pool_sum: u64 = pools
        .values()
        .copied()
        .fold(0u64, |acc, b| acc.saturating_add(b));
    let steady_recurring_tokens = non_pooled_sum.saturating_add(pool_sum);

    FreeTierTotals {
        steady_recurring_tokens,
        one_time_tokens,
        uncapped_providers,
        pools: pools.into_iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kernel::types::capability::Capability;
    use kernel::types::config::{CatalogMeta, FreeTier, FreeType, ModelConfig, TosVerdict};
    use std::collections::HashMap;

    /// Terse fixture builder: a free-tier model carrying only the fields the
    /// totals headline reads.
    fn free_model(
        id: &str,
        free_type: FreeType,
        monthly: Option<u64>,
        credit: Option<u64>,
        pool: Option<&str>,
    ) -> ModelConfig {
        ModelConfig {
            id: id.into(),
            api_model_id: None,
            provider: "p".into(),
            capabilities: vec![Capability::TextChat],
            context_window: 8000,
            max_output_tokens: 1000,
            pricing: None,
            family: None,
            catalog: Some(CatalogMeta {
                free: Some(FreeTier {
                    free_type,
                    monthly_tokens: monthly,
                    credit_tokens: credit,
                    pool_key: pool.map(|s| s.to_string()),
                    tos: TosVerdict::Ok,
                    trains_on_prompts: false,
                }),
                ..Default::default()
            }),
        }
    }

    fn map(models: Vec<ModelConfig>) -> HashMap<String, ModelConfig> {
        models.into_iter().map(|m| (m.id.clone(), m)).collect()
    }

    #[test]
    fn free_tier_totals_pool_dedup_and_exclusions() {
        let models = map(vec![
            // f1, f2 share pool "gemini-flash" @ 60M each → pool counted ONCE (60M).
            free_model(
                "f1",
                FreeType::RecurringDaily,
                Some(60_000_000),
                None,
                Some("gemini-flash"),
            ),
            free_model(
                "f2",
                FreeType::RecurringDaily,
                Some(60_000_000),
                None,
                Some("gemini-flash"),
            ),
            // d1: non-pooled recurring → +10M steady.
            free_model("d1", FreeType::RecurringDaily, Some(10_000_000), None, None),
            // o1: one-time signup credit → one_time, NOT steady.
            free_model("o1", FreeType::OneTimeInitial, None, Some(25_000_000), None),
            // u1: uncapped → listed, never summed.
            free_model("u1", FreeType::RecurringUncapped, None, None, None),
            // x1: discontinued → ignored entirely (even with a token budget).
            free_model("x1", FreeType::Discontinued, Some(99_000_000), None, None),
        ]);
        let t = free_tier_totals(&models);
        assert_eq!(t.steady_recurring_tokens, 60_000_000 + 10_000_000);
        assert_eq!(t.one_time_tokens, 25_000_000);
        assert_eq!(t.uncapped_providers, vec!["u1".to_string()]);
        assert!(
            t.pools
                .iter()
                .any(|(k, v)| k == "gemini-flash" && *v == 60_000_000)
        );
        // Determinism: repeated calls yield identical (sorted) output.
        assert_eq!(free_tier_totals(&models).pools, t.pools);
        assert_eq!(
            free_tier_totals(&models).uncapped_providers,
            t.uncapped_providers
        );
    }

    #[test]
    fn pool_dedup_uses_max_within_a_pool() {
        // Two models share pool "p" with 40M and 60M → the pool contributes its
        // MAX (60M), so an under-declared duplicate can't shrink the pool.
        let models = map(vec![
            free_model(
                "f_a",
                FreeType::RecurringDaily,
                Some(40_000_000),
                None,
                Some("p"),
            ),
            free_model(
                "f_b",
                FreeType::RecurringDaily,
                Some(60_000_000),
                None,
                Some("p"),
            ),
        ]);
        assert_eq!(
            free_tier_totals(&models).steady_recurring_tokens,
            60_000_000
        );
    }

    // ---- Docs-drift gate --------------------------------------------------
    // These constants MUST equal `free_tier_totals` computed from
    // `testdata/example_free_catalog.json`. Editing the fixture without
    // updating these (or vice-versa) fails the build — the
    // `free-tier-catalog.md` "docs-counts" scenario at unit granularity. No
    // external fetch/network. Fixture composition:
    //   gemini-flash-a (pool "gemini-flash", 50M) + gemini-flash-b (same pool,
    //   40M) → pool counted once at MAX 50M; solo-daily (non-pooled) 20M →
    //   steady 70M; signup-credit one-time 25M; uncapped-pro listed.
    const EXAMPLE_STEADY_TOKENS: u64 = 70_000_000;
    const EXAMPLE_ONE_TIME_TOKENS: u64 = 25_000_000;
    const EXAMPLE_UNCAPPED_PROVIDERS: &[&str] = &["uncapped-pro"];
    const EXAMPLE_POOLS: &[(&str, u64)] = &[("gemini-flash", 50_000_000)];

    #[test]
    fn example_catalog_totals_match_documented_headline() {
        let raw = include_str!("testdata/example_free_catalog.json");
        let models: Vec<ModelConfig> =
            serde_json::from_str(raw).expect("example_free_catalog.json parses");
        let models: HashMap<String, ModelConfig> =
            models.into_iter().map(|m| (m.id.clone(), m)).collect();
        let t = free_tier_totals(&models);
        assert_eq!(t.steady_recurring_tokens, EXAMPLE_STEADY_TOKENS);
        assert_eq!(t.one_time_tokens, EXAMPLE_ONE_TIME_TOKENS);
        assert_eq!(
            t.uncapped_providers,
            EXAMPLE_UNCAPPED_PROVIDERS
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            t.pools,
            EXAMPLE_POOLS
                .iter()
                .map(|(k, v)| (k.to_string(), *v))
                .collect::<Vec<_>>()
        );
    }
}
