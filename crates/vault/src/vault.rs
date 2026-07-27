//! `Vault<K, S>` — the credential-vault orchestrator: it seals/unseals provider
//! credentials under a per-tenant **DEK** (itself sealed under the **KEK** from `K`) and
//! delegates storage of the sealed bytes to `S`.
//!
//! Every credential is AAD-bound to its `(tenant, router)` (V1), so a sealed blob is
//! useless in any other row. The DEK is auto-provisioned on first store and never
//! overwritten. Decrypted secrets live in [`Zeroizing`] and are never logged.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::crypto::{self, CryptoError};
use crate::kek::{KekError, KekProvider};
use crate::store::{StoreError, VaultStore};

#[derive(Debug, thiserror::Error)]
pub enum VaultError {
    #[error(transparent)]
    Crypto(#[from] CryptoError),
    #[error(transparent)]
    Kek(#[from] KekError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("no DEK provisioned for tenant {0}")]
    NoDek(Uuid),
    #[error("vault unavailable (no KEK configured)")]
    NoVault,
    #[error("oauth bundle codec error")]
    Codec,
}

/// The sealed shape of an OAuth credential — an `{access,refresh,expires,scopes}` bundle
/// serialized to JSON and sealed (AAD-bound) under the tenant DEK, exactly like an api_key.
/// Only `access_token` is required; the paste-token (`setup-token`) path leaves the rest
/// `None` (long-lived, no refresh). `expires_at_ms` is unix epoch milliseconds.
#[derive(Serialize, Deserialize)]
struct OAuthBundle {
    access_token: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expires_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scopes: Option<String>,
}

/// A resolved OAuth credential: the bearer access token (in [`Zeroizing`]) + its optional
/// expiry (epoch millis), for the injection path and — later — the refresh worker.
pub struct OAuthToken {
    pub access_token: Zeroizing<String>,
    pub expires_at_ms: Option<i64>,
}

/// Seals/unseals provider credentials under a per-tenant DEK (sealed under `K`'s KEK),
/// storing the sealed bytes via `S`. Generic so both strategos (Postgres store) and sensei
/// (its own store) share one hardened implementation.
pub struct Vault<K, S> {
    kek: K,
    store: S,
}

impl<K: KekProvider, S: VaultStore> Vault<K, S> {
    pub fn new(kek: K, store: S) -> Self {
        Self { kek, store }
    }

    /// AAD binding a credential to its `(tenant, router)`: the two UUIDs concatenated.
    fn aad(tenant: Uuid, router: Uuid) -> [u8; 32] {
        let mut aad = [0u8; 32];
        aad[..16].copy_from_slice(tenant.as_bytes());
        aad[16..].copy_from_slice(router.as_bytes());
        aad
    }

    /// Resolve + decrypt the tenant DEK (sealed under the KEK).
    async fn dek(&self, tenant: Uuid) -> Result<Zeroizing<[u8; 32]>, VaultError> {
        let sealed = self
            .store
            .get_encrypted_dek(tenant)
            .await?
            .ok_or(VaultError::NoDek(tenant))?;
        let kek = self.kek.kek()?;
        Ok(crypto::unseal_dek(&kek, &sealed)?)
    }

    /// Provision a per-tenant DEK if absent — a fresh random key sealed under the KEK.
    /// Idempotent: an existing DEK is never overwritten (that would orphan its credentials).
    pub async fn ensure_tenant_dek(&self, tenant: Uuid, actor: &str) -> Result<(), VaultError> {
        if self.store.get_encrypted_dek(tenant).await?.is_some() {
            return Ok(());
        }
        let dek = crypto::generate_dek();
        let kek = self.kek.kek()?;
        let sealed = crypto::seal_dek(&kek, &dek)?;
        self.store
            .insert_dek_if_absent(tenant, &sealed, actor)
            .await?;
        Ok(())
    }

    /// Store (or rotate) a provider credential for `(tenant, router)`, AAD-bound.
    /// Auto-provisions the tenant DEK on first use. Returns the stored row id.
    pub async fn store_router_key(
        &self,
        tenant: Uuid,
        router: Uuid,
        plaintext: &str,
        label: Option<&str>,
        actor: &str,
    ) -> Result<Uuid, VaultError> {
        self.ensure_tenant_dek(tenant, actor).await?;
        let dek = self.dek(tenant).await?;
        let sealed =
            crypto::seal_credential(&dek, &Self::aad(tenant, router), plaintext.as_bytes())?;
        Ok(self
            .store
            .upsert_credential(tenant, router, &sealed, label, actor)
            .await?)
    }

    /// Revoke (deactivate) the active credential for `(tenant, router)`.
    pub async fn revoke_router_key(
        &self,
        tenant: Uuid,
        router: Uuid,
        actor: &str,
    ) -> Result<(), VaultError> {
        Ok(self
            .store
            .deactivate_credential(tenant, router, actor)
            .await?)
    }

    /// Resolve + decrypt the active credential for `(tenant, router)`, if any.
    pub async fn resolve_router_key(
        &self,
        tenant: Uuid,
        router: Uuid,
    ) -> Result<Option<Zeroizing<String>>, VaultError> {
        let Some(sealed) = self.store.get_active_credential(tenant, router).await? else {
            return Ok(None);
        };
        let dek = self.dek(tenant).await?;
        Ok(Some(crypto::unseal_credential(
            &dek,
            &Self::aad(tenant, router),
            &sealed,
        )?))
    }

    /// Decrypt every active credential for the tenant into a `router_name → key` map
    /// (what the engine matches on). The DEK is resolved once, only if rows exist.
    pub async fn resolve_tenant_keys(
        &self,
        tenant: Uuid,
    ) -> Result<HashMap<String, String>, VaultError> {
        let rows = self.store.list_active_credentials(tenant).await?;
        let mut out = HashMap::new();
        if rows.is_empty() {
            return Ok(out);
        }
        let dek = self.dek(tenant).await?;
        for row in rows {
            let key =
                crypto::unseal_credential(&dek, &Self::aad(tenant, row.router_id), &row.sealed)?;
            out.insert(row.router_name, key.to_string());
        }
        Ok(out)
    }

    // --- V4: rotation + migration -------------------------------------------------------------

    /// Rotate the tenant's **DEK**: generate a fresh DEK, re-seal every active credential under
    /// it (AAD preserved), archive the old DEK, and swap atomically via
    /// [`VaultStore::rotate_dek`]. Because the swap and every re-seal share one transaction,
    /// active rows are always at the current DEK afterward — the per-request `resolve` path is
    /// unchanged and never touches the archive. Returns the new `dek_version`. Errors with
    /// [`VaultError::NoDek`] if the tenant has no DEK yet.
    pub async fn rotate_dek(&self, tenant: Uuid, actor: &str) -> Result<i32, VaultError> {
        let old = self.dek(tenant).await?;
        let new_dek = crypto::generate_dek();
        let rows = self.store.list_active_credentials(tenant).await?;
        let mut resealed = Vec::with_capacity(rows.len());
        for row in rows {
            let aad = Self::aad(tenant, row.router_id);
            let pt = crypto::unseal_credential(&old, &aad, &row.sealed)?;
            resealed.push((
                row.router_id,
                crypto::seal_credential(&new_dek, &aad, pt.as_bytes())?,
            ));
        }
        let kek = self.kek.kek()?;
        let new_sealed_dek = crypto::seal_dek(&kek, &new_dek)?;
        Ok(self
            .store
            .rotate_dek(tenant, &new_sealed_dek, &resealed, actor)
            .await?)
    }

    /// Rotate the master **KEK**: re-wrap every DEK (current + archived, all tenants) from the
    /// current KEK to `new_kek`, in one transaction. Credentials are untouched — they're sealed
    /// under the DEK, not the KEK. Returns the number of DEK blobs re-wrapped.
    ///
    /// This is an operational migration, not a live call: after it commits, restart the process
    /// with a [`KekProvider`](crate::kek::KekProvider) serving `new_kek`. This `Vault` still holds
    /// the old KEK and can no longer resolve the re-wrapped DEKs.
    pub async fn rotate_kek(&self, new_kek: &[u8; 32]) -> Result<usize, VaultError> {
        let old_kek = self.kek.kek()?;
        let blobs = self.store.list_all_dek_blobs().await?;
        let mut rewraps = Vec::with_capacity(blobs.len());
        for blob in blobs {
            let dek = crypto::unseal_dek(&old_kek, &blob.sealed)?;
            let sealed = crypto::seal_dek(new_kek, &dek)?;
            rewraps.push((blob, sealed));
        }
        let n = rewraps.len();
        self.store.apply_dek_rewraps(&rewraps).await?;
        Ok(n)
    }

    /// One-time migration for credentials sealed by the pre-crate inline vault, which used an
    /// **empty AAD**. Re-seals each active credential under the tenant DEK bound to
    /// `tenant‖router`. Idempotent: a row already AAD-bound opens under the real AAD and is
    /// skipped. Returns the number of rows actually re-sealed.
    ///
    /// Must run before/with the strategos migration onto the crate (V5): the crate resolves with
    /// AAD, so an un-migrated (empty-AAD) row would fail to unseal.
    pub async fn reseal_without_aad(&self, tenant: Uuid, actor: &str) -> Result<usize, VaultError> {
        let dek = self.dek(tenant).await?;
        let rows = self.store.list_active_credentials(tenant).await?;
        let mut n = 0;
        for row in rows {
            let aad = Self::aad(tenant, row.router_id);
            // Already AAD-bound? Then it opens under the real AAD — leave it untouched.
            if crypto::unseal_credential(&dek, &aad, &row.sealed).is_ok() {
                continue;
            }
            // Legacy empty-AAD row: open with the empty AAD, re-seal bound to (tenant, router).
            let pt = crypto::unseal_credential(&dek, b"", &row.sealed)?;
            let sealed = crypto::seal_credential(&dek, &aad, pt.as_bytes())?;
            self.store
                .update_credential_blob(tenant, row.router_id, &sealed, actor)
                .await?;
            n += 1;
        }
        Ok(n)
    }

    // --- OAuth credentials (F3 OAuth) ---------------------------------------------------------

    /// Store (or rotate) an OAuth credential for `(tenant, router)`: seal the
    /// `{access,refresh,expires,scopes}` bundle (AAD-bound, like an api_key) under the tenant
    /// DEK and persist it as the active `credential_type='oauth'` row. Auto-provisions the DEK.
    /// The paste-token path passes just `access_token` (rest `None`). Returns the row id.
    #[allow(clippy::too_many_arguments)]
    pub async fn store_oauth(
        &self,
        tenant: Uuid,
        router: Uuid,
        access_token: &str,
        refresh_token: Option<&str>,
        expires_at_ms: Option<i64>,
        scopes: Option<&str>,
        client_id: Option<&str>,
        actor: &str,
    ) -> Result<Uuid, VaultError> {
        self.ensure_tenant_dek(tenant, actor).await?;
        let dek = self.dek(tenant).await?;
        let bundle = OAuthBundle {
            access_token: access_token.to_owned(),
            refresh_token: refresh_token.map(str::to_owned),
            expires_at_ms,
            scopes: scopes.map(str::to_owned),
        };
        // Serialize into a Zeroizing buffer so the plaintext JSON is wiped after sealing.
        let json = Zeroizing::new(serde_json::to_vec(&bundle).map_err(|_| VaultError::Codec)?);
        let sealed = crypto::seal_credential(&dek, &Self::aad(tenant, router), &json)?;
        Ok(self
            .store
            .store_oauth(
                tenant,
                router,
                &sealed,
                expires_at_ms,
                scopes,
                client_id,
                actor,
            )
            .await?)
    }

    /// Resolve + decrypt the active OAuth credential for `(tenant, router)` → its bearer access
    /// token + expiry, if any. A blob relocated to another `(tenant, router)` fails the AAD check.
    pub async fn resolve_oauth(
        &self,
        tenant: Uuid,
        router: Uuid,
    ) -> Result<Option<OAuthToken>, VaultError> {
        let Some(sealed) = self.store.get_active_oauth(tenant, router).await? else {
            return Ok(None);
        };
        let dek = self.dek(tenant).await?;
        let json = crypto::unseal_credential(&dek, &Self::aad(tenant, router), &sealed)?;
        let bundle: OAuthBundle = serde_json::from_str(&json).map_err(|_| VaultError::Codec)?;
        Ok(Some(OAuthToken {
            access_token: Zeroizing::new(bundle.access_token),
            expires_at_ms: bundle.expires_at_ms,
        }))
    }

    /// Revoke (deactivate) the active OAuth credential for `(tenant, router)`.
    pub async fn revoke_oauth(
        &self,
        tenant: Uuid,
        router: Uuid,
        actor: &str,
    ) -> Result<(), VaultError> {
        Ok(self.store.deactivate_oauth(tenant, router, actor).await?)
    }

    /// Decrypt every active OAuth credential for the tenant into a `router_name → access_token`
    /// map (what the engine matches on). The DEK is resolved once, only if rows exist.
    pub async fn resolve_tenant_oauth(
        &self,
        tenant: Uuid,
    ) -> Result<HashMap<String, String>, VaultError> {
        let rows = self.store.list_active_oauth(tenant).await?;
        let mut out = HashMap::new();
        if rows.is_empty() {
            return Ok(out);
        }
        let dek = self.dek(tenant).await?;
        for row in rows {
            let json =
                crypto::unseal_credential(&dek, &Self::aad(tenant, row.router_id), &row.sealed)?;
            let bundle: OAuthBundle = serde_json::from_str(&json).map_err(|_| VaultError::Codec)?;
            out.insert(row.router_name, bundle.access_token);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kek::StaticKekProvider;
    use crate::test_support::MemoryStore;

    fn vault() -> Vault<StaticKekProvider, MemoryStore> {
        Vault::new(StaticKekProvider::new([7u8; 32]), MemoryStore::default())
    }

    #[tokio::test]
    async fn store_auto_provisions_dek_and_round_trips() {
        let v = vault();
        let (t, r) = (Uuid::new_v4(), Uuid::new_v4());
        v.store_router_key(t, r, "sk-ant-AAA", Some("byok"), "tester")
            .await
            .unwrap();
        assert_eq!(v.store.dek_count(), 1, "DEK auto-provisioned");
        assert_eq!(
            v.resolve_router_key(t, r).await.unwrap().unwrap().as_str(),
            "sk-ant-AAA"
        );
    }

    #[tokio::test]
    async fn rotate_replaces_the_key() {
        let v = vault();
        let (t, r) = (Uuid::new_v4(), Uuid::new_v4());
        v.store_router_key(t, r, "sk-old", None, "tester")
            .await
            .unwrap();
        v.store_router_key(t, r, "sk-new", None, "tester")
            .await
            .unwrap();
        assert_eq!(
            v.resolve_router_key(t, r).await.unwrap().unwrap().as_str(),
            "sk-new"
        );
        assert_eq!(v.store.dek_count(), 1, "rotate reuses the same DEK");
    }

    #[tokio::test]
    async fn revoke_makes_it_unresolvable() {
        let v = vault();
        let (t, r) = (Uuid::new_v4(), Uuid::new_v4());
        v.store_router_key(t, r, "sk-x", None, "tester")
            .await
            .unwrap();
        v.revoke_router_key(t, r, "tester").await.unwrap();
        assert!(v.resolve_router_key(t, r).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn resolve_tenant_keys_maps_by_router_name() {
        let v = vault();
        let (t, r) = (Uuid::new_v4(), Uuid::new_v4());
        v.store.name_router(r, "anthropic");
        v.store_router_key(t, r, "sk-ant", None, "tester")
            .await
            .unwrap();
        let map = v.resolve_tenant_keys(t).await.unwrap();
        assert_eq!(map.get("anthropic").map(String::as_str), Some("sk-ant"));
    }

    #[tokio::test]
    async fn tenants_are_isolated() {
        // Two tenants store a key for the same router; each resolves only its own (distinct
        // DEKs + AAD). Cross-tenant read never returns the other's secret.
        let v = vault();
        let r = Uuid::new_v4();
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        v.store_router_key(a, r, "sk-A", None, "tester")
            .await
            .unwrap();
        v.store_router_key(b, r, "sk-B", None, "tester")
            .await
            .unwrap();
        assert_eq!(
            v.resolve_router_key(a, r).await.unwrap().unwrap().as_str(),
            "sk-A"
        );
        assert_eq!(
            v.resolve_router_key(b, r).await.unwrap().unwrap().as_str(),
            "sk-B"
        );
        assert_eq!(v.store.dek_count(), 2, "each tenant has its own DEK");
    }

    #[tokio::test]
    async fn resolve_missing_is_none_not_error() {
        let v = vault();
        assert!(
            v.resolve_router_key(Uuid::new_v4(), Uuid::new_v4())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn rotate_dek_reseals_and_still_resolves() {
        let v = vault();
        let (t, r) = (Uuid::new_v4(), Uuid::new_v4());
        v.store_router_key(t, r, "sk-x", None, "tester")
            .await
            .unwrap();
        let before = v.store.get_encrypted_dek(t).await.unwrap().unwrap();

        let new_ver = v.rotate_dek(t, "rotator").await.unwrap();
        assert_eq!(new_ver, 2, "version bumped from the seeded 1");

        let after = v.store.get_encrypted_dek(t).await.unwrap().unwrap();
        assert_ne!(before, after, "the tenant DEK actually changed");
        // Old DEK archived; the credential still resolves under the new DEK (AAD preserved).
        assert_eq!(v.store.archive.lock().unwrap().len(), 1, "old DEK archived");
        assert_eq!(
            v.resolve_router_key(t, r).await.unwrap().unwrap().as_str(),
            "sk-x"
        );
    }

    #[tokio::test]
    async fn rotate_dek_without_a_dek_errors() {
        let v = vault();
        assert!(matches!(
            v.rotate_dek(Uuid::new_v4(), "rotator").await,
            Err(VaultError::NoDek(_))
        ));
    }

    #[tokio::test]
    async fn rotate_kek_rewraps_dek_so_only_the_new_kek_resolves() {
        // One store, two vaults: old KEK writes + rotates, new KEK reads. Credentials are
        // untouched by KEK rotation — only the DEK's wrapping changes.
        let store = MemoryStore::default();
        let old = Vault::new(StaticKekProvider::new([7u8; 32]), store.clone());
        let new = Vault::new(StaticKekProvider::new([9u8; 32]), store.clone());
        let (t, r) = (Uuid::new_v4(), Uuid::new_v4());
        old.store_router_key(t, r, "sk-x", None, "tester")
            .await
            .unwrap();

        let n = old.rotate_kek(&[9u8; 32]).await.unwrap();
        assert_eq!(n, 1, "one current DEK re-wrapped");

        assert_eq!(
            new.resolve_router_key(t, r)
                .await
                .unwrap()
                .unwrap()
                .as_str(),
            "sk-x",
            "the new-KEK vault resolves the re-wrapped DEK"
        );
        assert!(
            old.resolve_router_key(t, r).await.is_err(),
            "the old-KEK vault can no longer unseal the re-wrapped DEK"
        );
    }

    #[tokio::test]
    async fn reseal_without_aad_migrates_legacy_rows_idempotently() {
        // Simulate a row from the pre-crate inline vault: sealed under the DEK with an EMPTY AAD.
        let v = vault();
        let (t, r) = (Uuid::new_v4(), Uuid::new_v4());
        v.ensure_tenant_dek(t, "seed").await.unwrap();
        let dek = v.dek(t).await.unwrap();
        let legacy = crypto::seal_credential(&dek, b"", b"sk-legacy").unwrap();
        v.store
            .upsert_credential(t, r, &legacy, None, "seed")
            .await
            .unwrap();

        // Pre-migration: the crate resolves with a (tenant‖router) AAD, so the legacy row fails.
        assert!(v.resolve_router_key(t, r).await.is_err());

        assert_eq!(v.reseal_without_aad(t, "migrator").await.unwrap(), 1);
        assert_eq!(
            v.resolve_router_key(t, r).await.unwrap().unwrap().as_str(),
            "sk-legacy"
        );
        // Idempotent: a second pass finds nothing left to migrate.
        assert_eq!(v.reseal_without_aad(t, "migrator").await.unwrap(), 0);
    }

    #[tokio::test]
    async fn oauth_round_trips_and_coexists_with_api_key() {
        let v = vault();
        let (t, r) = (Uuid::new_v4(), Uuid::new_v4());
        v.store.name_router(r, "anthropic");
        // An api_key and an oauth credential for the SAME (tenant, router) coexist.
        v.store_router_key(t, r, "sk-ant-static", None, "tester")
            .await
            .unwrap();
        v.store_oauth(t, r, "oauth-access-XYZ", None, None, None, None, "tester")
            .await
            .unwrap();

        assert_eq!(
            v.resolve_oauth(t, r)
                .await
                .unwrap()
                .unwrap()
                .access_token
                .as_str(),
            "oauth-access-XYZ"
        );
        assert_eq!(
            v.resolve_router_key(t, r).await.unwrap().unwrap().as_str(),
            "sk-ant-static",
            "the api_key credential is untouched by the oauth one"
        );
        assert_eq!(
            v.resolve_tenant_oauth(t)
                .await
                .unwrap()
                .get("anthropic")
                .map(String::as_str),
            Some("oauth-access-XYZ")
        );

        // Revoke the oauth credential only.
        v.revoke_oauth(t, r, "tester").await.unwrap();
        assert!(v.resolve_oauth(t, r).await.unwrap().is_none());
        assert!(
            v.resolve_router_key(t, r).await.unwrap().is_some(),
            "revoking oauth leaves the api_key credential active"
        );
    }

    #[tokio::test]
    async fn oauth_expiry_round_trips_from_the_bundle() {
        let v = vault();
        let (t, r) = (Uuid::new_v4(), Uuid::new_v4());
        v.store_oauth(
            t,
            r,
            "acc",
            Some("refresh"),
            Some(1_900_000_000_000),
            Some("chat"),
            None,
            "t",
        )
        .await
        .unwrap();
        let tok = v.resolve_oauth(t, r).await.unwrap().unwrap();
        assert_eq!(tok.access_token.as_str(), "acc");
        assert_eq!(tok.expires_at_ms, Some(1_900_000_000_000));
    }
}
