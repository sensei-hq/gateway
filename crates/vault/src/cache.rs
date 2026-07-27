//! `TenantKeyCache<K, S>` — the per-request BYOK cache both consumers sit behind.
//!
//! Caches each tenant's decrypted `router_name → api_key` map, filled on miss from the
//! [`Vault`] and invalidated on any credential write. Small (a few short strings per tenant)
//! — no eviction at v1 scale. A `None` vault means BYOK is disabled (no KEK): [`get`] returns
//! an empty map so the engine falls back to platform/env keys. Sharing this here keeps the
//! decrypt→cache→invalidate discipline in one place for the strategos gateway (per-request
//! key injection) and the sensei daemon alike.
//!
//! [`get`]: TenantKeyCache::get

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use uuid::Uuid;

use crate::kek::KekProvider;
use crate::store::VaultStore;
use crate::vault::{Vault, VaultError};

/// Memoizes each tenant's decrypted BYOK key map in front of a [`Vault`]. `None` vault ⇒
/// BYOK disabled. Generic over the same `K`/`S` as the `Vault` so both consumers reuse it.
pub struct TenantKeyCache<K, S> {
    vault: Option<Vault<K, S>>,
    /// `router_name → api_key` per tenant.
    cache: RwLock<HashMap<Uuid, Arc<HashMap<String, String>>>>,
    /// `router_name → oauth access_token` per tenant (raw tokens; the consumer marks them for
    /// the credentials channel). Separate map so api_key and oauth resolve independently.
    oauth_cache: RwLock<HashMap<Uuid, Arc<HashMap<String, String>>>>,
}

impl<K: KekProvider, S: VaultStore> TenantKeyCache<K, S> {
    /// `None` ⇒ BYOK disabled (no KEK); [`get`](Self::get) then yields an empty map.
    pub fn new(vault: Option<Vault<K, S>>) -> Self {
        Self {
            vault,
            cache: RwLock::new(HashMap::new()),
            oauth_cache: RwLock::new(HashMap::new()),
        }
    }

    /// The tenant's decrypted BYOK keys (`router_name → api_key`), memoized. Empty when BYOK
    /// is disabled or the tenant stored none. A vault error (e.g. a DEK sealed under a
    /// non-matching KEK) is surfaced so the caller can fall back to platform keys — a bad
    /// BYOK setup never denies inference.
    pub async fn get(&self, tenant: Uuid) -> Result<Arc<HashMap<String, String>>, VaultError> {
        let Some(vault) = &self.vault else {
            return Ok(Arc::new(HashMap::new()));
        };
        if let Some(hit) = self.cache.read().await.get(&tenant).cloned() {
            return Ok(hit);
        }
        let map = Arc::new(vault.resolve_tenant_keys(tenant).await?);
        self.cache.write().await.insert(tenant, map.clone());
        Ok(map)
    }

    /// The tenant's decrypted OAuth access tokens (`router_name → access_token`), memoized.
    /// Raw tokens — the caller marks them for the credentials channel (e.g. the `oauth:` prefix).
    /// Same fail-safe contract as [`get`](Self::get).
    pub async fn get_oauth(
        &self,
        tenant: Uuid,
    ) -> Result<Arc<HashMap<String, String>>, VaultError> {
        let Some(vault) = &self.vault else {
            return Ok(Arc::new(HashMap::new()));
        };
        if let Some(hit) = self.oauth_cache.read().await.get(&tenant).cloned() {
            return Ok(hit);
        }
        let map = Arc::new(vault.resolve_tenant_oauth(tenant).await?);
        self.oauth_cache.write().await.insert(tenant, map.clone());
        Ok(map)
    }

    /// Drop the tenant's cached maps (both api_key and oauth) so the next read re-decrypts —
    /// called after any credential write.
    pub async fn invalidate(&self, tenant: Uuid) {
        self.cache.write().await.remove(&tenant);
        self.oauth_cache.write().await.remove(&tenant);
    }

    /// Store/rotate a BYOK api_key then invalidate the cache. Errors if BYOK is disabled.
    pub async fn store(
        &self,
        tenant: Uuid,
        router: Uuid,
        label: Option<&str>,
        key: &str,
        actor: &str,
    ) -> Result<Uuid, VaultError> {
        let id = self
            .vault()?
            .store_router_key(tenant, router, key, label, actor)
            .await?;
        self.invalidate(tenant).await;
        Ok(id)
    }

    /// Revoke a BYOK api_key then invalidate the cache. Errors if BYOK is disabled.
    pub async fn revoke(&self, tenant: Uuid, router: Uuid, actor: &str) -> Result<(), VaultError> {
        self.vault()?
            .revoke_router_key(tenant, router, actor)
            .await?;
        self.invalidate(tenant).await;
        Ok(())
    }

    /// Store/rotate an OAuth credential then invalidate the cache. Errors if BYOK is disabled.
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
        let id = self
            .vault()?
            .store_oauth(
                tenant,
                router,
                access_token,
                refresh_token,
                expires_at_ms,
                scopes,
                client_id,
                actor,
            )
            .await?;
        self.invalidate(tenant).await;
        Ok(id)
    }

    /// Revoke an OAuth credential then invalidate the cache. Errors if BYOK is disabled.
    pub async fn revoke_oauth(
        &self,
        tenant: Uuid,
        router: Uuid,
        actor: &str,
    ) -> Result<(), VaultError> {
        self.vault()?.revoke_oauth(tenant, router, actor).await?;
        self.invalidate(tenant).await;
        Ok(())
    }

    fn vault(&self) -> Result<&Vault<K, S>, VaultError> {
        self.vault.as_ref().ok_or(VaultError::NoVault)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kek::StaticKekProvider;
    use crate::test_support::MemoryStore;

    fn key(m: &HashMap<String, String>) -> Option<&str> {
        m.get("openai").map(String::as_str)
    }

    #[tokio::test]
    async fn none_vault_yields_empty_and_write_errors() {
        let c: TenantKeyCache<StaticKekProvider, MemoryStore> = TenantKeyCache::new(None);
        assert!(c.get(Uuid::new_v4()).await.unwrap().is_empty());
        assert!(matches!(
            c.store(Uuid::new_v4(), Uuid::new_v4(), None, "k", "t")
                .await,
            Err(VaultError::NoVault)
        ));
    }

    #[tokio::test]
    async fn get_memoizes_until_invalidate() {
        // The store on this cache, plus a side Vault over the SAME store to mutate the DB
        // behind the cache's back — proving the cached map is served until `invalidate`.
        let store = MemoryStore::default();
        store.name_router(Uuid::from_u128(1), "openai");
        let router = Uuid::from_u128(1);
        let c = TenantKeyCache::new(Some(Vault::new(
            StaticKekProvider::new([7u8; 32]),
            store.clone(),
        )));
        let side = Vault::new(StaticKekProvider::new([7u8; 32]), store.clone());
        let t = Uuid::new_v4();

        c.store(t, router, None, "kA", "t").await.unwrap();
        assert_eq!(key(&c.get(t).await.unwrap()), Some("kA"));

        // Mutate behind the cache — still serves the memoized value.
        side.store_router_key(t, router, "kA2", None, "t")
            .await
            .unwrap();
        assert_eq!(
            key(&c.get(t).await.unwrap()),
            Some("kA"),
            "stale until invalidate"
        );

        c.invalidate(t).await;
        assert_eq!(key(&c.get(t).await.unwrap()), Some("kA2"), "refreshed");
    }

    #[tokio::test]
    async fn tenants_are_isolated_in_cache() {
        let store = MemoryStore::default();
        store.name_router(Uuid::from_u128(3), "openai");
        let router = Uuid::from_u128(3);
        let c = TenantKeyCache::new(Some(Vault::new(StaticKekProvider::new([7u8; 32]), store)));
        let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
        c.store(a, router, None, "kA", "t").await.unwrap();
        c.store(b, router, None, "kB", "t").await.unwrap();
        assert_eq!(key(&c.get(a).await.unwrap()), Some("kA"));
        assert_eq!(key(&c.get(b).await.unwrap()), Some("kB"));
    }

    #[tokio::test]
    async fn oauth_cache_serves_and_reflects_writes() {
        let store = MemoryStore::default();
        store.name_router(Uuid::from_u128(9), "anthropic");
        let router = Uuid::from_u128(9);
        let c = TenantKeyCache::new(Some(Vault::new(StaticKekProvider::new([7u8; 32]), store)));
        let t = Uuid::new_v4();

        // Empty before any oauth write (and this caches the empty map).
        assert!(c.get_oauth(t).await.unwrap().is_empty());
        c.store_oauth(t, router, "tok-1", None, None, None, None, "x")
            .await
            .unwrap();
        // store_oauth invalidated the cache → the write is now visible.
        assert_eq!(
            c.get_oauth(t)
                .await
                .unwrap()
                .get("anthropic")
                .map(String::as_str),
            Some("tok-1")
        );
        // The api_key map is independent and untouched.
        assert!(c.get(t).await.unwrap().is_empty());

        c.revoke_oauth(t, router, "x").await.unwrap();
        assert!(c.get_oauth(t).await.unwrap().is_empty());
    }
}
