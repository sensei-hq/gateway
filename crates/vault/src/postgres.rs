//! Postgres backing for the vault, behind the `sqlx` feature: a [`VaultStore`] over
//! strategos' `core.tenant_keys` + `public.router_credentials`, and a [`KekProvider`]
//! that reads the KEK from **Supabase Vault** (closes gap #1 / torii#17 — the KEK never
//! sits raw in the process env).
//!
//! Both run as the trusted `service_role` connection; the tables carry RLS deny-all +
//! `service_role`-only (`secrets.sql`), so these rows are never client-readable. sensei
//! supplies its own [`VaultStore`] for its schema (epic V6) rather than reusing this one.

use async_trait::async_trait;
use base64::Engine;
use sqlx::PgPool;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::kek::{KekError, KekProvider};
use crate::store::{DekBlob, StoreError, StoredCredential, VaultStore};

fn store_err(e: sqlx::Error) -> StoreError {
    StoreError::Backend(e.to_string())
}

/// `VaultStore` over strategos' `core.tenant_keys` (DEK) + `public.router_credentials`
/// (`credential_type = 'api_key'`). Stores opaque sealed bytes only — the [`Vault`] does
/// all crypto.
///
/// [`Vault`]: crate::vault::Vault
pub struct PostgresVaultStore {
    pool: PgPool,
}

impl PostgresVaultStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl VaultStore for PostgresVaultStore {
    async fn get_encrypted_dek(&self, tenant: Uuid) -> Result<Option<Vec<u8>>, StoreError> {
        sqlx::query_scalar("select encrypted_dek from core.tenant_keys where tenant_id = $1")
            .bind(tenant)
            .fetch_optional(&self.pool)
            .await
            .map_err(store_err)
    }

    async fn insert_dek_if_absent(
        &self,
        tenant: Uuid,
        sealed_dek: &[u8],
        actor: &str,
    ) -> Result<bool, StoreError> {
        // Never overwrite an existing DEK (would orphan every credential sealed under it).
        let r = sqlx::query(
            "insert into core.tenant_keys (tenant_id, encrypted_dek, dek_version, modified_by) \
             values ($1, $2, 1, $3) on conflict (tenant_id) do nothing",
        )
        .bind(tenant)
        .bind(sealed_dek)
        .bind(actor)
        .execute(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(r.rows_affected() > 0)
    }

    async fn upsert_credential(
        &self,
        tenant: Uuid,
        router: Uuid,
        sealed: &[u8],
        label: Option<&str>,
        actor: &str,
    ) -> Result<Uuid, StoreError> {
        sqlx::query_scalar(
            "insert into public.router_credentials \
               (tenant_id, id, router_id, encrypted_api_key, key_label, is_active, \
                credential_type, modified_by) \
             values ($1, gen_random_uuid(), $2, $3, $4, true, 'api_key', $5) \
             on conflict (tenant_id, router_id) where is_active do update set \
               encrypted_api_key = excluded.encrypted_api_key, \
               key_label = excluded.key_label, \
               is_active = true, \
               credential_type = 'api_key', \
               modified_at = now(), \
               modified_by = excluded.modified_by \
             returning id",
        )
        .bind(tenant)
        .bind(router)
        .bind(sealed)
        .bind(label)
        .bind(actor)
        .fetch_one(&self.pool)
        .await
        .map_err(store_err)
    }

    async fn deactivate_credential(
        &self,
        tenant: Uuid,
        router: Uuid,
        actor: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "update public.router_credentials set is_active = false, modified_by = $3 \
             where tenant_id = $1 and router_id = $2 \
               and credential_type = 'api_key' and is_active = true",
        )
        .bind(tenant)
        .bind(router)
        .bind(actor)
        .execute(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(())
    }

    async fn get_active_credential(
        &self,
        tenant: Uuid,
        router: Uuid,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        sqlx::query_scalar(
            "select encrypted_api_key from public.router_credentials \
             where tenant_id = $1 and router_id = $2 \
               and is_active = true and credential_type = 'api_key'",
        )
        .bind(tenant)
        .bind(router)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_err)
    }

    async fn list_active_credentials(
        &self,
        tenant: Uuid,
    ) -> Result<Vec<StoredCredential>, StoreError> {
        let rows: Vec<(Uuid, String, Vec<u8>)> = sqlx::query_as(
            "select k.router_id, r.name, k.encrypted_api_key \
             from public.router_credentials k \
             join config.routers r on r.id = k.router_id \
             where k.tenant_id = $1 and k.is_active = true and k.credential_type = 'api_key'",
        )
        .bind(tenant)
        .fetch_all(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(rows
            .into_iter()
            .map(|(router_id, router_name, sealed)| StoredCredential {
                router_id,
                router_name,
                sealed,
            })
            .collect())
    }

    async fn rotate_dek(
        &self,
        tenant: Uuid,
        new_sealed_dek: &[u8],
        resealed: &[(Uuid, Vec<u8>)],
        actor: &str,
    ) -> Result<i32, StoreError> {
        // One transaction: archive the old DEK, install the new one, re-seal every active
        // credential. `for update` serializes concurrent rotations of the same tenant.
        let mut tx = self.pool.begin().await.map_err(store_err)?;
        let (old_version, old_sealed): (i32, Vec<u8>) = sqlx::query_as(
            "select dek_version, encrypted_dek from core.tenant_keys where tenant_id = $1 for update",
        )
        .bind(tenant)
        .fetch_one(&mut *tx)
        .await
        .map_err(store_err)?;
        let new_version = old_version + 1;

        sqlx::query(
            "insert into core.tenant_key_archive \
               (tenant_id, dek_version, encrypted_dek, modified_by) \
             values ($1, $2, $3, $4) on conflict (tenant_id, dek_version) do nothing",
        )
        .bind(tenant)
        .bind(old_version)
        .bind(&old_sealed)
        .bind(actor)
        .execute(&mut *tx)
        .await
        .map_err(store_err)?;

        sqlx::query(
            "update core.tenant_keys set encrypted_dek = $2, dek_version = $3, \
               modified_at = now(), modified_by = $4 where tenant_id = $1",
        )
        .bind(tenant)
        .bind(new_sealed_dek)
        .bind(new_version)
        .bind(actor)
        .execute(&mut *tx)
        .await
        .map_err(store_err)?;

        for (router, sealed) in resealed {
            sqlx::query(
                "update public.router_credentials set encrypted_api_key = $3, \
                   modified_at = now(), modified_by = $4 \
                 where tenant_id = $1 and router_id = $2 \
                   and is_active = true and credential_type = 'api_key'",
            )
            .bind(tenant)
            .bind(router)
            .bind(sealed)
            .bind(actor)
            .execute(&mut *tx)
            .await
            .map_err(store_err)?;
        }

        tx.commit().await.map_err(store_err)?;
        Ok(new_version)
    }

    async fn update_credential_blob(
        &self,
        tenant: Uuid,
        router: Uuid,
        sealed: &[u8],
        actor: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "update public.router_credentials set encrypted_api_key = $3, \
               modified_at = now(), modified_by = $4 \
             where tenant_id = $1 and router_id = $2 \
               and is_active = true and credential_type = 'api_key'",
        )
        .bind(tenant)
        .bind(router)
        .bind(sealed)
        .bind(actor)
        .execute(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(())
    }

    async fn list_all_dek_blobs(&self) -> Result<Vec<DekBlob>, StoreError> {
        let current: Vec<(Uuid, i32, Vec<u8>)> =
            sqlx::query_as("select tenant_id, dek_version, encrypted_dek from core.tenant_keys")
                .fetch_all(&self.pool)
                .await
                .map_err(store_err)?;
        let archived: Vec<(Uuid, i32, Vec<u8>)> = sqlx::query_as(
            "select tenant_id, dek_version, encrypted_dek from core.tenant_key_archive",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(store_err)?;
        let mut out = Vec::with_capacity(current.len() + archived.len());
        out.extend(current.into_iter().map(|(t, v, s)| DekBlob {
            tenant_id: t,
            dek_version: v,
            archived: false,
            sealed: s,
        }));
        out.extend(archived.into_iter().map(|(t, v, s)| DekBlob {
            tenant_id: t,
            dek_version: v,
            archived: true,
            sealed: s,
        }));
        Ok(out)
    }

    async fn apply_dek_rewraps(&self, rewraps: &[(DekBlob, Vec<u8>)]) -> Result<(), StoreError> {
        // One transaction so a crash can't leave DEKs split across the old and new KEK.
        let mut tx = self.pool.begin().await.map_err(store_err)?;
        for (blob, sealed) in rewraps {
            if blob.archived {
                sqlx::query(
                    "update core.tenant_key_archive set encrypted_dek = $3 \
                     where tenant_id = $1 and dek_version = $2",
                )
                .bind(blob.tenant_id)
                .bind(blob.dek_version)
                .bind(sealed)
                .execute(&mut *tx)
                .await
                .map_err(store_err)?;
            } else {
                sqlx::query("update core.tenant_keys set encrypted_dek = $2 where tenant_id = $1")
                    .bind(blob.tenant_id)
                    .bind(sealed)
                    .execute(&mut *tx)
                    .await
                    .map_err(store_err)?;
            }
        }
        tx.commit().await.map_err(store_err)?;
        Ok(())
    }
}

/// A [`KekProvider`] whose KEK is read from **Supabase Vault** (`vault.decrypted_secrets`)
/// once at [`connect`](Self::connect) and cached. The secret stores the base64 of the
/// 32-byte KEK; the platform-managed vault key decrypts it, so the KEK is never in the
/// gateway's process env and not present in a plain DB dump.
pub struct SupabaseVaultKekProvider(Zeroizing<[u8; 32]>);

impl SupabaseVaultKekProvider {
    /// Read the base64 32-byte KEK stored under `secret_name` in Supabase Vault, decode it,
    /// and cache the bytes. Fails closed if the secret is missing or malformed.
    pub async fn connect(pool: &PgPool, secret_name: &str) -> Result<Self, KekError> {
        let b64: Option<String> = sqlx::query_scalar(
            "select decrypted_secret from vault.decrypted_secrets where name = $1",
        )
        .bind(secret_name)
        .fetch_optional(pool)
        .await
        .map_err(|e| KekError::Backend(e.to_string()))?;
        let b64 = b64
            .ok_or_else(|| KekError::Backend(format!("vault secret `{secret_name}` not found")))?;
        let bytes = Zeroizing::new(
            base64::engine::general_purpose::STANDARD
                .decode(b64.trim())
                .map_err(|e| KekError::Invalid(format!("base64: {e}")))?,
        );
        let arr: [u8; 32] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| KekError::Invalid(format!("expected 32 bytes, got {}", bytes.len())))?;
        Ok(Self(Zeroizing::new(arr)))
    }
}

impl KekProvider for SupabaseVaultKekProvider {
    fn kek(&self) -> Result<Zeroizing<[u8; 32]>, KekError> {
        Ok(self.0.clone())
    }
}

#[cfg(test)]
mod tests {
    //! Hits local Supabase (55322). Ignored by default — run with:
    //!   cargo test -p sensei-vault --features sqlx -- --ignored
    use super::*;
    use crate::kek::StaticKekProvider;
    use crate::vault::Vault;
    use sqlx::postgres::PgPoolOptions;

    async fn pool() -> PgPool {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgresql://postgres:postgres@127.0.0.1:55322/postgres".into());
        PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("connect local Supabase (55322)")
    }

    #[tokio::test]
    #[ignore = "requires local Supabase (55322)"]
    async fn postgres_store_round_trips_via_vault() {
        let pool = pool().await;
        let vault = Vault::new(
            StaticKekProvider::new([13u8; 32]),
            PostgresVaultStore::new(pool.clone()),
        );
        let tenant = Uuid::new_v4();
        let router: Uuid =
            sqlx::query_scalar("select id from config.routers where name = 'openai'")
                .fetch_one(&pool)
                .await
                .expect("openai router seeded");
        sqlx::query(
            "insert into core.tenants (id, name, slug, modified_by) \
             values ($1, 'vault-crate-test', $2, 'test')",
        )
        .bind(tenant)
        .bind(format!("vault-crate-{tenant}"))
        .execute(&pool)
        .await
        .unwrap();

        // store → resolve; the blob at rest is sealed bytea, not the plaintext.
        vault
            .store_router_key(tenant, router, "sk-crate-AAA", Some("byok"), "tester")
            .await
            .unwrap();
        assert_eq!(
            vault
                .resolve_router_key(tenant, router)
                .await
                .unwrap()
                .unwrap()
                .as_str(),
            "sk-crate-AAA"
        );
        let sealed: Vec<u8> = sqlx::query_scalar(
            "select encrypted_api_key from public.router_credentials \
             where tenant_id = $1 and router_id = $2 and is_active",
        )
        .bind(tenant)
        .bind(router)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            !sealed.windows(3).any(|w| w == b"sk-"),
            "plaintext must not appear at rest"
        );

        // rotate → new value; revoke → gone.
        vault
            .store_router_key(tenant, router, "sk-crate-BBB", None, "tester")
            .await
            .unwrap();
        assert_eq!(
            vault
                .resolve_router_key(tenant, router)
                .await
                .unwrap()
                .unwrap()
                .as_str(),
            "sk-crate-BBB"
        );
        vault
            .revoke_router_key(tenant, router, "tester")
            .await
            .unwrap();
        assert!(
            vault
                .resolve_router_key(tenant, router)
                .await
                .unwrap()
                .is_none()
        );

        // cleanup (FK-safe order).
        for stmt in [
            "delete from public.router_credentials where tenant_id = $1",
            "delete from core.tenant_keys where tenant_id = $1",
            "delete from core.tenants where id = $1",
        ] {
            sqlx::query(stmt).bind(tenant).execute(&pool).await.unwrap();
        }
    }

    #[tokio::test]
    #[ignore = "requires local Supabase (55322)"]
    async fn supabase_vault_kek_provider_reads_the_kek() {
        let pool = pool().await;
        let raw = [0x5au8; 32];
        let name = format!("vault_crate_kek_{}", Uuid::new_v4().simple());
        let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
        sqlx::query("select vault.create_secret($1, $2, 'crate test kek')")
            .bind(&b64)
            .bind(&name)
            .execute(&pool)
            .await
            .unwrap();

        let provider = SupabaseVaultKekProvider::connect(&pool, &name)
            .await
            .unwrap();
        assert_eq!(*provider.kek().unwrap(), raw);

        // The provider's KEK actually drives an envelope round-trip.
        let dek = *crate::crypto::generate_dek();
        let sealed = crate::crypto::seal_dek(&provider.kek().unwrap(), &dek).unwrap();
        assert_eq!(
            *crate::crypto::unseal_dek(&provider.kek().unwrap(), &sealed).unwrap(),
            dek
        );

        sqlx::query("delete from vault.secrets where name = $1")
            .bind(&name)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[ignore = "requires local Supabase (55322)"]
    async fn postgres_rotation_and_reseal_round_trip() {
        let pool = pool().await;
        let kek = [13u8; 32];
        let vault = Vault::new(
            StaticKekProvider::new(kek),
            PostgresVaultStore::new(pool.clone()),
        );
        let tenant = Uuid::new_v4();
        let openai: Uuid =
            sqlx::query_scalar("select id from config.routers where name = 'openai'")
                .fetch_one(&pool)
                .await
                .expect("openai router seeded");
        let anthropic: Uuid =
            sqlx::query_scalar("select id from config.routers where name = 'anthropic'")
                .fetch_one(&pool)
                .await
                .expect("anthropic router seeded");
        sqlx::query(
            "insert into core.tenants (id, name, slug, modified_by) \
             values ($1, 'vault-rot-test', $2, 'test')",
        )
        .bind(tenant)
        .bind(format!("vault-rot-{tenant}"))
        .execute(&pool)
        .await
        .unwrap();

        // Seed one crate-sealed (AAD-bound) credential; auto-provisions the tenant DEK.
        vault
            .store_router_key(tenant, openai, "sk-openai", Some("byok"), "tester")
            .await
            .unwrap();

        // Seed a *legacy* (empty-AAD) credential the way the pre-crate inline vault would have:
        // unseal the tenant DEK, seal with an empty AAD, insert an active row directly.
        let sealed_dek: Vec<u8> =
            sqlx::query_scalar("select encrypted_dek from core.tenant_keys where tenant_id = $1")
                .bind(tenant)
                .fetch_one(&pool)
                .await
                .unwrap();
        let dek = crate::crypto::unseal_dek(&kek, &sealed_dek).unwrap();
        let legacy = crate::crypto::seal_credential(&dek, b"", b"sk-legacy").unwrap();
        sqlx::query(
            "insert into public.router_credentials \
               (tenant_id, router_id, encrypted_api_key, is_active, credential_type, modified_by) \
             values ($1, $2, $3, true, 'api_key', 'seed')",
        )
        .bind(tenant)
        .bind(anthropic)
        .bind(&legacy)
        .execute(&pool)
        .await
        .unwrap();

        // AAD migration: the legacy row is unresolvable until re-sealed, then resolves.
        assert!(vault.resolve_router_key(tenant, anthropic).await.is_err());
        assert_eq!(
            vault.reseal_without_aad(tenant, "migrator").await.unwrap(),
            1
        );
        assert_eq!(
            vault
                .resolve_router_key(tenant, anthropic)
                .await
                .unwrap()
                .unwrap()
                .as_str(),
            "sk-legacy"
        );

        // DEK rotation: version bumps, old DEK archived, both credentials still resolve.
        assert_eq!(vault.rotate_dek(tenant, "rotator").await.unwrap(), 2);
        let archived: i64 =
            sqlx::query_scalar("select count(*) from core.tenant_key_archive where tenant_id = $1")
                .bind(tenant)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(archived, 1, "prior DEK archived");
        let keys = vault.resolve_tenant_keys(tenant).await.unwrap();
        assert_eq!(keys.get("openai").map(String::as_str), Some("sk-openai"));
        assert_eq!(keys.get("anthropic").map(String::as_str), Some("sk-legacy"));

        // KEK re-wrap SQL (both the current + archived branches). `Vault::rotate_kek` itself is
        // global (one master KEK over every tenant) and unit-tested; here we drive the store's
        // transactional `apply_dek_rewraps` scoped to THIS tenant, since the shared DB holds
        // other tenants' DEKs sealed under the real deployment KEK (not this test's).
        let store = PostgresVaultStore::new(pool.clone());
        let mine: Vec<DekBlob> = store
            .list_all_dek_blobs()
            .await
            .unwrap()
            .into_iter()
            .filter(|b| b.tenant_id == tenant)
            .collect();
        assert_eq!(mine.len(), 2, "current v2 + archived v1 for this tenant");
        let new_kek = [14u8; 32];
        let mut rewraps = Vec::with_capacity(mine.len());
        for b in mine {
            let dek = crate::crypto::unseal_dek(&kek, &b.sealed).unwrap();
            let sealed = crate::crypto::seal_dek(&new_kek, &dek).unwrap();
            rewraps.push((b, sealed));
        }
        store.apply_dek_rewraps(&rewraps).await.unwrap();

        let new_vault = Vault::new(
            StaticKekProvider::new(new_kek),
            PostgresVaultStore::new(pool.clone()),
        );
        assert_eq!(
            new_vault
                .resolve_router_key(tenant, openai)
                .await
                .unwrap()
                .unwrap()
                .as_str(),
            "sk-openai"
        );
        assert!(
            vault.resolve_router_key(tenant, openai).await.is_err(),
            "old KEK no longer unseals the re-wrapped DEK"
        );

        // cleanup (FK-safe order).
        for stmt in [
            "delete from public.router_credentials where tenant_id = $1",
            "delete from core.tenant_key_archive where tenant_id = $1",
            "delete from core.tenant_keys where tenant_id = $1",
            "delete from core.tenants where id = $1",
        ] {
            sqlx::query(stmt).bind(tenant).execute(&pool).await.unwrap();
        }
    }
}
