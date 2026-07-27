//! `Vault<K, S>` — the credential-vault orchestrator: it seals/unseals provider
//! credentials under a per-tenant **DEK** (itself sealed under the **KEK** from `K`) and
//! delegates storage of the sealed bytes to `S`.
//!
//! Every credential is AAD-bound to its `(tenant, router)` (V1), so a sealed blob is
//! useless in any other row. The DEK is auto-provisioned on first store and never
//! overwritten. Decrypted secrets live in [`Zeroizing`] and are never logged.

use std::collections::HashMap;

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kek::StaticKekProvider;
    use crate::store::{DekBlob, StoredCredential};
    use std::sync::{Arc, Mutex};

    /// `(sealed blob, is_active)` for a `(tenant, router)` pair.
    type CredCell = (Vec<u8>, bool);
    /// `(sealed DEK, version)` for a tenant.
    type DekCell = (Vec<u8>, i32);
    /// Superseded DEKs, keyed by `(tenant, version)` → sealed bytes.
    type ArchiveMap = Arc<Mutex<HashMap<(Uuid, i32), Vec<u8>>>>;

    /// An in-memory `VaultStore` for testing the `Vault` logic without a database. `Arc`-backed
    /// so a `.clone()` shares state — a test can drive two `Vault`s (old/new KEK) over one store.
    #[derive(Default, Clone)]
    struct MemoryStore {
        deks: Arc<Mutex<HashMap<Uuid, DekCell>>>,
        archive: ArchiveMap,
        creds: Arc<Mutex<HashMap<(Uuid, Uuid), CredCell>>>,
        names: Arc<Mutex<HashMap<Uuid, String>>>, // router_id -> name
    }

    impl MemoryStore {
        fn name_router(&self, id: Uuid, name: &str) {
            self.names.lock().unwrap().insert(id, name.to_string());
        }
        fn dek_count(&self) -> usize {
            self.deks.lock().unwrap().len()
        }
    }

    #[async_trait::async_trait]
    impl VaultStore for MemoryStore {
        async fn get_encrypted_dek(&self, tenant: Uuid) -> Result<Option<Vec<u8>>, StoreError> {
            Ok(self
                .deks
                .lock()
                .unwrap()
                .get(&tenant)
                .map(|(s, _)| s.clone()))
        }
        async fn insert_dek_if_absent(
            &self,
            tenant: Uuid,
            sealed: &[u8],
            _actor: &str,
        ) -> Result<bool, StoreError> {
            let mut d = self.deks.lock().unwrap();
            if d.contains_key(&tenant) {
                return Ok(false);
            }
            d.insert(tenant, (sealed.to_vec(), 1));
            Ok(true)
        }
        async fn upsert_credential(
            &self,
            tenant: Uuid,
            router: Uuid,
            sealed: &[u8],
            _label: Option<&str>,
            _actor: &str,
        ) -> Result<Uuid, StoreError> {
            self.creds
                .lock()
                .unwrap()
                .insert((tenant, router), (sealed.to_vec(), true));
            Ok(Uuid::new_v4())
        }
        async fn deactivate_credential(
            &self,
            tenant: Uuid,
            router: Uuid,
            _actor: &str,
        ) -> Result<(), StoreError> {
            if let Some(e) = self.creds.lock().unwrap().get_mut(&(tenant, router)) {
                e.1 = false;
            }
            Ok(())
        }
        async fn get_active_credential(
            &self,
            tenant: Uuid,
            router: Uuid,
        ) -> Result<Option<Vec<u8>>, StoreError> {
            Ok(self
                .creds
                .lock()
                .unwrap()
                .get(&(tenant, router))
                .filter(|(_, active)| *active)
                .map(|(s, _)| s.clone()))
        }
        async fn list_active_credentials(
            &self,
            tenant: Uuid,
        ) -> Result<Vec<StoredCredential>, StoreError> {
            let names = self.names.lock().unwrap();
            Ok(self
                .creds
                .lock()
                .unwrap()
                .iter()
                .filter(|((t, _), (_, active))| *t == tenant && *active)
                .map(|((_, r), (s, _))| StoredCredential {
                    router_id: *r,
                    router_name: names.get(r).cloned().unwrap_or_default(),
                    sealed: s.clone(),
                })
                .collect())
        }

        async fn rotate_dek(
            &self,
            tenant: Uuid,
            new_sealed_dek: &[u8],
            resealed: &[(Uuid, Vec<u8>)],
            _actor: &str,
        ) -> Result<i32, StoreError> {
            let mut deks = self.deks.lock().unwrap();
            let (old_sealed, old_ver) = deks
                .get(&tenant)
                .cloned()
                .ok_or_else(|| StoreError::Backend(format!("no DEK for {tenant}")))?;
            self.archive
                .lock()
                .unwrap()
                .insert((tenant, old_ver), old_sealed);
            let new_ver = old_ver + 1;
            deks.insert(tenant, (new_sealed_dek.to_vec(), new_ver));
            drop(deks);
            let mut creds = self.creds.lock().unwrap();
            for (router, sealed) in resealed {
                if let Some(cell) = creds.get_mut(&(tenant, *router)) {
                    cell.0 = sealed.clone();
                }
            }
            Ok(new_ver)
        }

        async fn update_credential_blob(
            &self,
            tenant: Uuid,
            router: Uuid,
            sealed: &[u8],
            _actor: &str,
        ) -> Result<(), StoreError> {
            if let Some(cell) = self.creds.lock().unwrap().get_mut(&(tenant, router)) {
                cell.0 = sealed.to_vec();
            }
            Ok(())
        }

        async fn list_all_dek_blobs(&self) -> Result<Vec<DekBlob>, StoreError> {
            let mut out: Vec<DekBlob> = self
                .deks
                .lock()
                .unwrap()
                .iter()
                .map(|(t, (s, v))| DekBlob {
                    tenant_id: *t,
                    dek_version: *v,
                    archived: false,
                    sealed: s.clone(),
                })
                .collect();
            out.extend(
                self.archive
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|((t, v), s)| DekBlob {
                        tenant_id: *t,
                        dek_version: *v,
                        archived: true,
                        sealed: s.clone(),
                    }),
            );
            Ok(out)
        }

        async fn apply_dek_rewraps(
            &self,
            rewraps: &[(DekBlob, Vec<u8>)],
        ) -> Result<(), StoreError> {
            for (blob, sealed) in rewraps {
                if blob.archived {
                    self.archive
                        .lock()
                        .unwrap()
                        .insert((blob.tenant_id, blob.dek_version), sealed.clone());
                } else if let Some(cell) = self.deks.lock().unwrap().get_mut(&blob.tenant_id) {
                    cell.0 = sealed.clone();
                }
            }
            Ok(())
        }
    }

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
}
