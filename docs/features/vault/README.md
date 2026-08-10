---
title: Vault — Module Reference
doctype: module
module: vault
status: implemented
---

# Vault

BYOK (bring-your-own-key) credential vault: per-tenant envelope-encrypted
provider credentials, resolved into per-request keys the engine reads at
dispatch. The gateway stays tenant-agnostic.

## Status

| Feature | Status | Source | Notes |
|---|---|---|---|
| [BYOK vault](byok-vault.md) | Implemented | `crates/vault` | AEAD envelope (KEK→DEK), `VaultStore` seam, `PostgresVaultStore` (`keyvault.*`) |

## Notes

- The `PostgresVaultStore` writes to `keyvault.tenant_keys` / `keyvault.router_credentials` and joins `catalog.routers` — the same schema the [data-tier](../../features/README.md) (torii extraction) owns.
- OAuth tokens are marked with the `oauth:` prefix so an OAuth-aware adapter sends `Authorization: Bearer`.
