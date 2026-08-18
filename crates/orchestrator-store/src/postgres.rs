//! SP-DATA-1: Postgres adapters for the run-state seams (`ExecutionJournal`/`ContentStore`/
//! `ContextStore`). Schema-agnostic — runs against the dbd-managed `orchestrator.*` schema.
//! Uses sqlx RUNTIME queries (not the compile-time `query!` macros) so the crate builds with
//! no database. Feature-gated: default builds don't pull sqlx.

use sqlx::postgres::{PgPool, PgPoolOptions};

/// Connect a pool to `database_url` (the dbd-applied `orchestrator.*` schema must exist).
pub async fn connect(database_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(8)
        .connect(database_url)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests require a live PG at $DATABASE_URL with the dbd schema applied (the Docker harness).
    /// Absent DATABASE_URL, they skip (so a bare `cargo test --features postgres` without a DB
    /// doesn't fail spuriously).
    fn db_url() -> Option<String> {
        std::env::var("DATABASE_URL").ok()
    }

    #[tokio::test]
    async fn connects_and_the_schema_exists() {
        let Some(url) = db_url() else { return };
        let pool = connect(&url).await.expect("connect");
        let (n,): (i64,) = sqlx::query_as(
            "select count(*) from information_schema.tables where table_schema='orchestrator'",
        )
        .fetch_one(&pool)
        .await
        .expect("query");
        assert!(n >= 5, "expected the 5 orchestrator.* tables, saw {n}");
    }
}
