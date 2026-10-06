---
title: Data-tier — Module Reference
doctype: module
module: data-tier
status: superseded
---

# Data-tier — superseded: torii owns persistence

The plan recorded here — a decoupled data-tier **extracted from torii** into the gateway for catalog
metadata, refresh, metering and config management (SP-DATA Phase 4, design D12) — is **cancelled**.
`sensei-hq/torii` `docs/DECISIONS.md` §11 (ratified 2026-09-17):

> *Gateway will always be a library, but torii will be the web interface and the persistence. Moving
> these to gateway will break its usage as a library.*

## Who owns what

| | Gateway (this repo — a library) | Torii (`sensei-hq/torii` — the product) |
|---|---|---|
| Routing engine, adapters, health gates, catalog **types** + pure `assemble()` | ✓ | consumes |
| Persistence **traits** (`GatewayStore`, the orchestrator store traits, a future catalog/metering seam) | ✓ | implements |
| In-memory implementations (tests, development, embedding) | ✓ | — |
| Every Postgres schema, migration, tenancy, RLS | — | ✓ |
| Catalog / config / metering **data**, registry content, staging, versioning, publish | — | ✓ |
| The operator CLI/worker (`torii`, now torii `crates/cli`) | — (moved) | ✓ |

The move is tracked by the epic [sensei-hq/gateway#76](https://github.com/sensei-hq/gateway/issues/76)
(TM-1…TM-9) and **complete**: the gateway made its seams movable (v0.9.0), torii implemented them
over a tenant-scoped schema and took the CLI, and the gateway deleted its Postgres adapters,
`database/` and `crates/torii` (TM-9, v0.10.0).

## The original pages (historical)

| Page | Was | Now |
|---|---|---|
| [Catalog control-plane](catalog-control-plane.md) | Planned (Phase 4 · SP-DATA) | torii — `catalog` / `config` schemas, `config_loader.rs` |
| [Management API](management-api.md) | Planned (Phase 4 · SP-DATA) | torii — its admin surface |
| [Metering store](metering-store.md) | Planned (Phase 4 · SP-DATA) | torii — `metering` schema |
