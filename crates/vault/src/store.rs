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

/// A stored DEK blob — the tenant's current DEK (`archived = false`) or a superseded one
/// (`archived = true`) — with its version. Used by KEK rotation, which must re-wrap **every**
/// DEK (current + archived) under the new KEK; the sealed bytes carry no plaintext.
#[derive(Debug, Clone)]
pub struct DekBlob {
    pub tenant_id: Uuid,
    pub dek_version: i32,
    pub archived: bool,
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

    // --- V4: rotation + migration ---------------------------------------------------------

    /// Atomically rotate the tenant's DEK: archive the current DEK, install `new_sealed_dek`
    /// as the current DEK at the next version, and replace each listed credential's sealed
    /// bytes (each already re-sealed under the new DEK, addressed by its `router_id`). This
    /// **must** be one transaction — a partial rotation would strand active credentials under
    /// an archived DEK. Returns the new `dek_version`.
    async fn rotate_dek(
        &self,
        tenant: Uuid,
        new_sealed_dek: &[u8],
        resealed: &[(Uuid, Vec<u8>)],
        actor: &str,
    ) -> Result<i32, StoreError>;

    /// Replace one active credential's sealed bytes in place (no active-state change), for the
    /// AAD re-seal migration. Addresses the active row by `(tenant, router)`.
    async fn update_credential_blob(
        &self,
        tenant: Uuid,
        router: Uuid,
        sealed: &[u8],
        actor: &str,
    ) -> Result<(), StoreError>;

    /// Every DEK blob across all tenants — current and archived — for KEK re-wrap.
    async fn list_all_dek_blobs(&self) -> Result<Vec<DekBlob>, StoreError>;

    /// Write the re-wrapped sealed bytes for many DEK blobs (KEK rotation) in **one
    /// transaction**, so a crash can't leave some DEKs under the old KEK and some under the
    /// new. Each entry pairs a blob (from [`list_all_dek_blobs`](VaultStore::list_all_dek_blobs))
    /// with its new sealed bytes. `dek_version` is untouched — the DEK material is unchanged.
    async fn apply_dek_rewraps(&self, rewraps: &[(DekBlob, Vec<u8>)]) -> Result<(), StoreError>;
}
