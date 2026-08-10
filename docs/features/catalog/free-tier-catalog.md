---
title: Free-Tier Catalog
doctype: feature
module: catalog
status: implemented
phase: 1
spec: SP-CAT
source: crates/kernel/src/types/config.rs (CatalogMeta) + crates/gateway/src/catalog/totals.rs (free_tier_totals)
---

# Free-Tier Catalog

> **Status: Implemented (Phase 1 · SP-CAT).** Design §12.2. Reference: OmniRoute `freeModelCatalog`.

First-class free-tier metadata on catalog models, so chains can prefer free
models and usage can be tracked against documented limits.

## Fields (per model)

- `free_type`: `recurring-daily | recurring-monthly | recurring-credit | recurring-uncapped | one-time-initial | keyless | discontinued`
- `monthly_tokens` / `credit_tokens` — documented budget
- `pool_key` — shared-quota pools counted once (e.g. a provider's Flash family)
- `tos` — ToS verdict (`ok | caution | ambiguous`) for proxy/relay use
- `trains_on_prompts` — privacy cost surfaced next to the quota

Totals are **pool-deduped**: each shared pool is counted once; one-time signup
credits and permanently-free-but-uncapped providers are reported separately and
never inflate the steady headline.

## Scenarios

```gherkin
Feature: Free-tier catalog
  Scenario: Pool-dedup counts a shared pool once
    Given two models sharing pool_key "gemini-flash" with 60M tokens each
    When steady recurring tokens are computed
    Then the pool contributes 60M once (not 120M)

  Scenario: One-time credits are excluded from the steady total
    Given a model with free_type "one-time-initial" and 25M credit_tokens
    Then the steady recurring total excludes it (counted only in first-month)

  Scenario: Uncapped free providers are listed, never summed
    Given a model with free_type "recurring-uncapped"
    Then it appears in the catalog but is excluded from the token headline

  Scenario: A unit test fails if docs drift from computed totals
    Given the documented headline differs from free_tier_totals()
    Then the drift gate fails the build
```

## Notes

- The **drift gate is a unit test** — `example_catalog_totals_match_documented_headline`
  in `crates/gateway/src/catalog/totals.rs` recomputes `free_tier_totals()` over
  the checked-in `testdata/example_free_catalog.json` and asserts it against the
  documented headline (no external fetch, no network).
- Catalog *data* is config (pure). Live *usage* against these limits is the
  [data-tier](../../features/README.md) metering store — a separate, deliberately
  **held-off** persistence layer (SP-DATA, Phase 4).
- Feeds [tiers & chains](tiers-and-chains.md) (the `free` tier) and [governance/usage-metering](../governance/usage-metering.md).
