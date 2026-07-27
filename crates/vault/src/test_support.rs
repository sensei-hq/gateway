//! Shared test fixtures — an in-memory [`VaultStore`] used by both the `vault` and `cache`
//! unit tests, so the `Vault`/`TenantKeyCache` logic is exercised without a database.

#![cfg(test)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use uuid::Uuid;

use crate::store::{DekBlob, StoreError, StoredCredential, VaultStore};

/// `(sealed blob, is_active)` for a `(tenant, router)` pair.
type CredCell = (Vec<u8>, bool);
/// `(sealed DEK, version)` for a tenant.
type DekCell = (Vec<u8>, i32);
/// Superseded DEKs, keyed by `(tenant, version)` → sealed bytes.
type ArchiveMap = Arc<Mutex<HashMap<(Uuid, i32), Vec<u8>>>>;

/// An in-memory `VaultStore` for tests. `Arc`-backed so a `.clone()` shares state — a test can
/// drive two `Vault`s (old/new KEK) over one store.
#[derive(Default, Clone)]
pub(crate) struct MemoryStore {
    pub(crate) deks: Arc<Mutex<HashMap<Uuid, DekCell>>>,
    pub(crate) archive: ArchiveMap,
    pub(crate) creds: Arc<Mutex<HashMap<(Uuid, Uuid), CredCell>>>,
    pub(crate) oauth: Arc<Mutex<HashMap<(Uuid, Uuid), CredCell>>>,
    pub(crate) names: Arc<Mutex<HashMap<Uuid, String>>>, // router_id -> name
}

impl MemoryStore {
    pub(crate) fn name_router(&self, id: Uuid, name: &str) {
        self.names.lock().unwrap().insert(id, name.to_string());
    }
    pub(crate) fn dek_count(&self) -> usize {
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

    async fn apply_dek_rewraps(&self, rewraps: &[(DekBlob, Vec<u8>)]) -> Result<(), StoreError> {
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

    async fn store_oauth(
        &self,
        tenant: Uuid,
        router: Uuid,
        sealed: &[u8],
        _expires_at_ms: Option<i64>,
        _scopes: Option<&str>,
        _client_id: Option<&str>,
        _actor: &str,
    ) -> Result<Uuid, StoreError> {
        self.oauth
            .lock()
            .unwrap()
            .insert((tenant, router), (sealed.to_vec(), true));
        Ok(Uuid::new_v4())
    }

    async fn get_active_oauth(
        &self,
        tenant: Uuid,
        router: Uuid,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self
            .oauth
            .lock()
            .unwrap()
            .get(&(tenant, router))
            .filter(|(_, active)| *active)
            .map(|(s, _)| s.clone()))
    }

    async fn deactivate_oauth(
        &self,
        tenant: Uuid,
        router: Uuid,
        _actor: &str,
    ) -> Result<(), StoreError> {
        if let Some(e) = self.oauth.lock().unwrap().get_mut(&(tenant, router)) {
            e.1 = false;
        }
        Ok(())
    }

    async fn list_active_oauth(&self, tenant: Uuid) -> Result<Vec<StoredCredential>, StoreError> {
        let names = self.names.lock().unwrap();
        Ok(self
            .oauth
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
