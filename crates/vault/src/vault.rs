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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kek::StaticKekProvider;
    use crate::store::StoredCredential;
    use std::sync::Mutex;

    /// `(sealed blob, is_active)` for a `(tenant, router)` pair.
    type CredCell = (Vec<u8>, bool);

    /// An in-memory `VaultStore` for testing the `Vault` logic without a database.
    #[derive(Default)]
    struct MemoryStore {
        deks: Mutex<HashMap<Uuid, Vec<u8>>>,
        creds: Mutex<HashMap<(Uuid, Uuid), CredCell>>,
        names: Mutex<HashMap<Uuid, String>>, // router_id -> name
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
            Ok(self.deks.lock().unwrap().get(&tenant).cloned())
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
            d.insert(tenant, sealed.to_vec());
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
}
