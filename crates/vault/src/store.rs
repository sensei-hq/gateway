//! `VaultStore` — the storage seam the [`Vault`](crate::vault::Vault) sits on.
//!
//! The store persists **opaque sealed bytes** keyed by `(tenant)` (the DEK) and
//! `(tenant, router)` (the credential); it does **no** crypto and never sees plaintext.
//! That keeps the crate schema-agnostic: strategos backs it with its `tenant_keys` /
//! `router_credentials` tables, and sensei supplies an adapter for its own schema — both
//! carry the same RLS invariant (deny-all + `service_role`-only).
//!
//! The Postgres adapter lives behind the crate's `sqlx` feature (V3b); this module is the
//! feature-free trait so consumers can implement it against any backend (or a fake, as the
//! `Vault` tests do).

use async_trait::async_trait;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("vault store backend error: {0}")]
    Backend(String),
}

/// One active credential row, as the store hands it back for resolution: the router's id,
/// its name (what the engine matches on), and the sealed blob (decrypted by the `Vault`).
#[derive(Debug, Clone)]
pub struct StoredCredential {
    pub router_id: Uuid,
    pub router_name: String,
    pub sealed: Vec<u8>,
}

/// Storage of sealed vault material. Implementations persist bytes only — all sealing and
/// unsealing happens in the [`Vault`](crate::vault::Vault). Backends must enforce
/// RLS deny-all + `service_role`-only so these rows are never client-readable.
#[async_trait]
pub trait VaultStore: Send + Sync {
    /// The tenant's sealed DEK, if one has been provisioned.
    async fn get_encrypted_dek(&self, tenant: Uuid) -> Result<Option<Vec<u8>>, StoreError>;

    /// Insert the sealed DEK **iff absent** (idempotent; an existing DEK is never
    /// overwritten — that would orphan every credential sealed under it). Returns whether
    /// a row was inserted.
    async fn insert_dek_if_absent(
        &self,
        tenant: Uuid,
        sealed_dek: &[u8],
        actor: &str,
    ) -> Result<bool, StoreError>;

    /// Store or replace the active credential for `(tenant, router)` (one active row per
    /// pair); reactivates a previously-revoked row. Returns the row id.
    async fn upsert_credential(
        &self,
        tenant: Uuid,
        router: Uuid,
        sealed: &[u8],
        label: Option<&str>,
        actor: &str,
    ) -> Result<Uuid, StoreError>;

    /// Deactivate the active credential for `(tenant, router)` (revoke).
    async fn deactivate_credential(
        &self,
        tenant: Uuid,
        router: Uuid,
        actor: &str,
    ) -> Result<(), StoreError>;

    /// The active sealed credential for `(tenant, router)`, if any.
    async fn get_active_credential(
        &self,
        tenant: Uuid,
        router: Uuid,
    ) -> Result<Option<Vec<u8>>, StoreError>;

    /// Every active credential for the tenant (for building the per-request key map).
    async fn list_active_credentials(
        &self,
        tenant: Uuid,
    ) -> Result<Vec<StoredCredential>, StoreError>;
}
