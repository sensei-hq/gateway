---
title: Catalog Refresh
doctype: feature
module: catalog
status: planned
phase: 1
spec: SP-CAT
source: import/loader (torii staging procedures)
---

# Catalog Refresh

> **Status: Planned (Phase 1 · SP-CAT).** Reuses torii's staging `import_*` procedures.

A mechanism to re-audit and reload the catalog (models / providers / routers /
free-tier data) without a code change — free tiers change constantly, so the
catalog must be refreshable and its totals CI-gated.

## Approach

- **Staging + import:** load candidate rows into a staging schema, validate, then import into `catalog.*` (reuses torii's `import_models` / `import_providers` / `import_routers` / `import_fallback_chains` procedures).
- **Trigger:** CLI (`refresh`) and/or a periodic job.
- **CI gate:** documented free-tier totals must match `computeFreeModelTotals()`; drift fails the build.

## Scenarios

```gherkin
Feature: Catalog refresh
  Scenario: A refresh imports new providers without code changes
    Given a staging load adds provider "navy" with a free tier
    When refresh runs and validation passes
    Then catalog.providers contains "navy" and its models are routable

  Scenario: Invalid staging rows are rejected, not imported
    Given a staging row with a model referencing an unknown router
    Then the import fails validation and catalog.* is unchanged

  Scenario: A discontinued free tier drops out of the steady total
    Given a provider's free_type is set to "discontinued" on refresh
    Then its tokens are excluded from the recurring headline
```

## Notes

- Refresh updates *catalog data* (pure config); it does not touch live usage counters (data-tier, Phase 4).
