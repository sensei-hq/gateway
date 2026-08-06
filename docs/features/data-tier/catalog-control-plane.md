---
title: Catalog Control-Plane
doctype: feature
module: data-tier
status: planned
phase: 4
spec: SP-DATA
source: torii catalog/config schemas (extracted)
---

# Catalog Control-Plane

> **Status: Planned (Phase 4 · SP-DATA).** Extract of torii's `catalog` + `config` schemas + loader.

The persistent catalog (providers / models / model_endpoints /
model_capabilities / routers / chains / chain_models / chain_bindings /
routing_policies / provider_health) plus the loader that assembles it into a
runtime `GatewayConfig`, and config versioning. Extracted from torii as a
user-agnostic subsystem (tenancy optional).

## Scenarios

```gherkin
Feature: Catalog control-plane
  Scenario: The loader assembles a runtime config from the catalog
    Given catalog rows for providers, models, routers, and chains
    When config_loader runs
    Then it emits a GatewayConfig with only routers whose keys are present

  Scenario: Standalone (no tenant) uses the default scope
    Given the subsystem runs without a tenant context
    Then catalog resolution uses the default/platform scope (no RLS required)

  Scenario: A catalog change bumps the config version
    Given a chain is edited
    Then config_version increments (see config-versioning)
```

## Notes

- Reuses torii's staging `import_*` procedures for [catalog refresh](../catalog/catalog-refresh.md).
- Shares the `catalog.*` schema with the [vault](../vault/README.md) and torii.
