//! `torii` — the operator control plane for the sensei orchestrator.
//!
//! The crate is a lib+bin pair: `src/main.rs` is only the clap surface plus
//! `dispatch`, and everything it calls lives here. The split exists so an
//! integration test can drive the REAL command implementations — notably
//! [`cmd::worker::serve`], whose single-tick contract the cross-process e2e
//! asserts — instead of re-implementing them and asserting on the copy.

pub mod boot;
pub mod cmd;
pub mod diff;
pub mod errors;
pub mod render;

/// Test-only helpers: serialization for the durable config tables, and the shared
/// `DATABASE_URL` guard every DB-gated unit test in this crate goes through.
#[cfg(test)]
pub(crate) mod test_guard {
    /// The single choke point for every DB-gated unit test in this crate: `Some(url)` to
    /// run, `None` — plus a VISIBLE skip notice naming the test — to skip.
    ///
    /// WHOLE-SLICE FIX 6: a silent early return made a skipped DB suite indistinguishable
    /// from a green one (same test count, same "ok"), so a CI job that loses the variable
    /// reported a fully-passing database suite that touched nothing.
    ///
    /// Written to the process's REAL stderr rather than through `eprintln!`: libtest
    /// captures the print macros and replays them only for a FAILING test, so an
    /// `eprintln!` notice would be invisible in exactly the green run it exists to
    /// annotate. `std::io::stderr()` writes fd 2 directly, bypassing that capture.
    ///
    /// Under libtest the current thread's name IS the test's path, which is what lets one
    /// helper at the choke point still name the test that skipped.
    pub(crate) fn db_url() -> Option<String> {
        let url = database_url_raw();
        if url.is_none() {
            use std::io::Write;
            let name = std::thread::current()
                .name()
                .unwrap_or("<unnamed test>")
                .to_string();
            // Formatted first, then ONE `write_all`: `Stderr` is unbuffered, so a
            // `writeln!` emits a separate syscall per format fragment and a parallel
            // test's output interleaves mid-line.
            let line = format!("SKIP {name}: {} not set\n", crate::boot::ENV_DATABASE_URL);
            let _ = std::io::stderr().write_all(line.as_bytes());
        }
        url
    }

    /// The raw lookup with no side effects, shared by `db_url()` (the announcing choke
    /// point every test's own skip check goes through) and `config_guard`'s advisory-lock
    /// connection below (which needs the value too but must NOT print a second SKIP line).
    fn database_url_raw() -> Option<String> {
        std::env::var(crate::boot::ENV_DATABASE_URL)
            .ok()
            .filter(|s| !s.trim().is_empty())
    }

    /// `orchestrator.config_*` is GLOBAL and every durable write is replace-all, so two
    /// tests writing it concurrently clobber each other's seeded content and move the
    /// generation the other just measured. It lives at crate root because the writers are
    /// spread across modules — `cmd::config`'s push tests and `boot`'s heavy-tier probe —
    /// and a guard held by only some of them serializes nothing.
    static CONFIG_TABLES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    // ---- Cross-PROCESS isolation (SP-DATA-4.1 #3) -----------------------------------------
    //
    // The mutex above serializes only THIS process's tests. `sensei-orchestrator-store`'s
    // own test suite hits the very same global `config_*` tables from a SEPARATE `cargo`
    // invocation, and an in-process mutex cannot serialize across processes. Not reachable
    // through `cargo test --workspace` (cargo runs test binaries one at a time), but `cargo
    // nextest` runs tests in separate processes IN PARALLEL and would defeat this mutex
    // entirely — a trap laid for whoever adopts it. Forcing two concurrent `cargo`
    // invocations (this crate + `orchestrator-store`) reproduced a failure roughly one round
    // in four before this fix.
    //
    // Postgres session-level advisory locks close that gap: held by a CONNECTION, released
    // on disconnect, visible across processes.
    /// Shared with `crates/orchestrator-store/src/postgres.rs`'s `tests::config_guard`. Both
    /// sites MUST use this exact key.
    const ADVISORY_CONFIG_TABLES: i64 = 0x5350_4441_5441_3401; // "SPDATA4" + 01

    /// RAII: holds BOTH the in-process mutex guard and, when a database is configured, the
    /// dedicated connection holding the cross-process advisory lock. Dropping releases
    /// both — the connection's `Drop` closes the socket, which Postgres treats as a
    /// disconnect and releases the session-level lock immediately, so a PANICKING test
    /// still releases it without any explicit unlock (which wouldn't run on unwind anyway).
    pub(crate) struct ConfigGuard {
        _mutex: tokio::sync::MutexGuard<'static, ()>,
        _conn: Option<sqlx::PgConnection>,
    }

    /// The in-process mutex is taken FIRST, before the advisory lock: same-process tests
    /// then queue on the cheap local mutex rather than on Postgres, and this never holds a
    /// database connection while merely waiting on that local mutex.
    ///
    /// Lock ORDER: this crate never takes any OTHER advisory lock (it has no
    /// `scheduled_runs` guard), so there is no path here that could acquire two advisory
    /// locks in different orders and deadlock against `orchestrator-store`'s own
    /// `scheduler_guard`. If a future test in this crate ever needs both, fix a single
    /// global order (config-tables before scheduled-runs) in BOTH crates.
    pub(crate) async fn config_guard() -> ConfigGuard {
        let mutex = CONFIG_TABLES.lock().await;
        let conn = acquire_advisory(ADVISORY_CONFIG_TABLES).await;
        ConfigGuard {
            _mutex: mutex,
            _conn: conn,
        }
    }

    /// Open a DEDICATED connection and hold `pg_advisory_lock(key)` on it. Deliberately NOT
    /// a connection borrowed from a pool: returning a pooled connection while the
    /// session-level lock is still held would leak the lock to whichever caller borrows
    /// that connection next. `None` when no database is configured — the test that follows
    /// immediately checks `db_url()` itself and skips, so nothing here needs to run.
    ///
    /// Blocking `pg_advisory_lock` (not `pg_try_advisory_lock`) is correct: a test should
    /// WAIT for its turn, not fail because another process got there first. An indefinite
    /// block only matters if a lock is somehow orphaned, and a session-level lock requires a
    /// LIVE connection to hold it — a crashed process's socket is reclaimed by the OS, which
    /// Postgres notices and releases the lock. As a defensive bound anyway (e.g. a wedged
    /// connection the OS hasn't yet reaped), the wait is capped by a `statement_timeout` on
    /// this dedicated connection rather than left truly infinite, so a genuinely stuck case
    /// fails loudly instead of hanging CI forever.
    async fn acquire_advisory(key: i64) -> Option<sqlx::PgConnection> {
        use sqlx::Connection;
        let url = database_url_raw()?;
        let mut conn = sqlx::PgConnection::connect(&url)
            .await
            .expect("advisory-lock connection: DATABASE_URL is set, so this must succeed");
        sqlx::query("set statement_timeout = '30s'")
            .execute(&mut conn)
            .await
            .expect("set statement_timeout");
        sqlx::query("select pg_advisory_lock($1)")
            .bind(key)
            .execute(&mut conn)
            .await
            .expect("pg_advisory_lock must succeed once connected");
        Some(conn)
    }
}
