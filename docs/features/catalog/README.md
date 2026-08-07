---
title: Catalog — Module Reference
doctype: module
module: catalog
status: partial
---

# Catalog

The model/provider/router catalog and how it becomes runtime config: model
resolution, `GatewayConfig`, plus the Phase-1 enhancements that add free-tier
metadata, a tiers dimension, a refresh mechanism, and config versioning.

## Status

| Feature | Status | Source | Notes |
|---|---|---|---|
| [Model registry](model-registry.md) | Implemented | `crates/local-engine/src` | `ModelResolver`/`ChainedResolver`; managed/Ollama/external |
| [Configuration](configuration.md) | Implemented | `crates/kernel/src/types/config.rs` | `GatewayConfig`/`RouterConfig`; runtime updates |
| [Free-tier catalog](free-tier-catalog.md) | Implemented (SP-CAT) | `crates/kernel/src/types/config.rs` (`CatalogMeta`) + `crates/gateway/src/catalog/totals.rs` | `free_type`/`pool_key`/`monthly_tokens`/`tos`/`trains_on_prompts`; pool-deduped totals + drift gate |
| [Tiers & chains](tiers-and-chains.md) | Implemented (SP-CAT) | `crates/gateway/src/catalog/{tiers,assemble}.rs` | tiers as a dimension; chains compose tier-refs. `headroom`/`least-used` stub to `priority` (deferred) |
| [Catalog refresh](catalog-refresh.md) | Partial (SP-CAT) | `crates/gateway/src/catalog/totals.rs` | re-audit + totals drift gate implemented; external DB `config_loader` deferred (SP-DATA) |
| [Config versioning](config-versioning.md) | Planned (Phase 4 · SP-DATA) | data-tier | `config_versions` + bump; ties to the replay version-fence — deferred |

## Notes

- **Persistence is a separate, deliberately held-off layer — the catalog is
  config-driven only.** SP-CAT is pure config → config: a `CatalogConfig`
  (models with `CatalogMeta`, tiers, tier-ref chains) runs through
  `catalog::assemble` into a concrete `GatewayConfig`, all in memory. The DB
  `config_loader` (external fetch/import), the live-usage intra-tier strategies
  (`headroom`/`least-used`), config versioning, and expiration tracking are all
  **deferred** to that held-off persistence layer (SP-DATA, Phase 4).
- Free-tier *catalog data* is config (pure); stateful *usage tracking* lives in the [data-tier](../../features/README.md) (Phase 4, held off).
- The tiers × chains model supersedes the earlier flat "tiers = chains" idea (design D13).
