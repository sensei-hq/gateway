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
use crate::store::{StoreError, StoredCredential, VaultStore};

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
             on conflict (tenant_id, router_id) do update set \
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
}
