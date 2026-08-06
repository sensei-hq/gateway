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
| [Free-tier catalog](free-tier-catalog.md) | Planned (Phase 1 · SP-CAT) | catalog data | `free_type`/`pool_key`/`monthly_tokens`/`tos`/`trains_on_prompts` |
| [Tiers & chains](tiers-and-chains.md) | Planned (Phase 1 · SP-CAT) | catalog + selection | tiers as a dimension; chains compose tiers (tier-refs) |
| [Catalog refresh](catalog-refresh.md) | Planned (Phase 1 · SP-CAT) | import/loader | re-audit models/providers/routers; CI-gated totals |
| [Config versioning](config-versioning.md) | Planned (Phase 4 · SP-DATA) | data-tier | `config_versions` + bump; ties to the replay version-fence |

## Notes

- Free-tier *catalog data* is config (pure); stateful *usage tracking* lives in the [data-tier](../../features/README.md) (Phase 4).
- The tiers × chains model supersedes the earlier flat "tiers = chains" idea (design D13).
