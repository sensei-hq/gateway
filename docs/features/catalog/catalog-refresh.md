---
title: Catalog Refresh
doctype: feature
module: catalog
status: partial
phase: 1
spec: SP-CAT
source: crates/gateway/src/catalog/totals.rs (re-audit + drift gate); external loader deferred (SP-DATA)
---

# Catalog Refresh

> **Status: Partial (Phase 1 · SP-CAT).** The **re-audit + totals drift gate** is
> implemented as a unit test over checked-in catalog data. External
> fetch/import (a DB `config_loader`) is **deferred to the persistence layer
> (SP-DATA), which is deliberately held off.**

A mechanism to re-audit and reload the catalog (models / providers / routers /
free-tier data) without a code change — free tiers change constantly, so the
catalog must be refreshable and its totals gated against drift.

## Approach

- **Re-audit + drift gate (implemented):** `free_tier_totals()` recomputes the
  pool-deduped headline over the catalog, and the
  `example_catalog_totals_match_documented_headline` unit test asserts it against
  the documented totals over `testdata/example_free_catalog.json` — drift fails
  the build. Pure, in-memory, no network.
- **Staging + import (deferred to SP-DATA, held off):** load candidate rows into a
  staging schema, validate, then import into `catalog.*` (would reuse torii's
  `import_models` / `import_providers` / `import_routers` / `import_fallback_chains`
  procedures via a DB `config_loader`). Needs the persistence layer.
- **Trigger (deferred):** CLI (`refresh`) and/or a periodic job — part of the
  external-loader work above.

## Scenarios

```gherkin
Feature: Catalog refresh
  # DEFERRED (SP-DATA, held off): needs the external DB config_loader.
  Scenario: A refresh imports new providers without code changes
    Given a staging load adds provider "navy" with a free tier
    When refresh runs and validation passes
    Then catalog.providers contains "navy" and its models are routable

  # DEFERRED (SP-DATA, held off): needs the external DB config_loader.
  Scenario: Invalid staging rows are rejected, not imported
    Given a staging row with a model referencing an unknown router
    Then the import fails validation and catalog.* is unchanged

  # IMPLEMENTED: pure re-audit over catalog data (free_tier_totals + drift gate).
  Scenario: A discontinued free tier drops out of the steady total
    Given a provider's free_type is set to "discontinued" on refresh
    Then its tokens are excluded from the recurring headline
```

## Notes

- The **re-audit half** (recompute totals + drift gate) is pure config and is
  implemented today. The **external-loader half** (fetch/import into `catalog.*`
  via a DB `config_loader`) is **deferred to the persistence layer (SP-DATA),
  which is deliberately held off.**
- Refresh updates *catalog data* (pure config); it never touches live usage
  counters (those live in the held-off data-tier, Phase 4).
