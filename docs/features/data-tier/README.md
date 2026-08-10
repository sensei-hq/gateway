---
title: Data-tier — Module Reference
doctype: module
module: data-tier
status: planned
---

# Data-tier

A decoupled, user-agnostic subsystem — **extracted from torii** — that manages
catalog metadata, refresh, and usage tracking over the DB-agnostic seam. Scoped
to model / chain / flow / config management; torii's user / tenancy / governance
stay in torii (tenancy made optional/injectable). Design §12.2 / SP-DATA (D12).

## Status

| Feature | Status | Source | Notes |
|---|---|---|---|
| [Catalog control-plane](catalog-control-plane.md) | Planned (Phase 4 · SP-DATA) | torii `catalog`/`config` schemas | schema + import/loader + config versioning |
| [Management API](management-api.md) | Planned (Phase 4 · SP-DATA) | new (CLI + API) | configure catalog; observe usage/lockouts/expirations |
| [Metering store](metering-store.md) | Planned (Phase 4 · SP-DATA) | torii `metering` schema | usage counters + reset windows + predicted lockout |

## Notes

- Wraps the pure DB-agnostic seam (`GatewayStore`/`VaultStore`-style); shared `catalog.*`/`keyvault.*` schema across the sensei-hq family.
- The pure gateway core stays stateless; all stateful tracking lives here.
