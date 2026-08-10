---
title: BYOK Vault
doctype: feature
module: vault
status: implemented
source: crates/vault/src
---

# BYOK Vault

Per-tenant, envelope-encrypted storage of provider credentials. `Vault<K, S>` is
generic over a `KekProvider` and a `VaultStore`; each credential is AEAD-sealed
under a per-tenant DEK, AAD-bound to `(tenant, router)`. The `VaultStore`
persists only opaque sealed bytes — it never sees plaintext.

## How credentials reach the engine

`TenantKeyCache` memoizes each tenant's decrypted `router → key` map (via
`resolve_tenant_keys`); the consumer injects them per request into
`InferenceRequest.credentials`. The engine reads `request.credentials` at
dispatch and stays tenant-agnostic. A `None` vault → empty map → env/platform
keys.

## Scenarios

```gherkin
Feature: BYOK vault
  Scenario: A sealed credential round-trips per tenant
    Given tenant T stores a credential for router "anthropic"
    When T's keys are resolved
    Then the decrypted map contains anthropic → the original key
    And the stored bytes are ciphertext (the store never holds plaintext)

  Scenario: AAD binding prevents cross-router reuse
    Given a credential sealed for (tenant T, router "anthropic")
    When it is opened under AAD for router "openai"
    Then decryption fails (AAD mismatch)

  Scenario: OAuth tokens carry the oauth: prefix
    Given a stored OAuth credential
    Then the resolved value is prefixed "oauth:" so the adapter sends Bearer auth

  Scenario: No vault falls back to environment keys
    Given a gateway configured without a vault
    Then request.credentials is empty and env/platform keys are used
```

## Notes

- `PostgresVaultStore` (the strategos/torii adapter) targets `keyvault.tenant_keys` / `keyvault.router_credentials` and joins `catalog.routers` (schema-qualified).
- Rotation (`rotate_dek` / `apply_dek_rewraps`) re-wraps DEKs without re-encrypting every credential.
