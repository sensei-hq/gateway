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

/// Test-only serialization for the durable config tables.
#[cfg(test)]
pub(crate) mod test_guard {
    /// `orchestrator.config_*` is GLOBAL and every durable write is replace-all, so two
    /// tests writing it concurrently clobber each other's seeded content and move the
    /// generation the other just measured. It lives at crate root because the writers are
    /// spread across modules — `cmd::config`'s push tests and `boot`'s heavy-tier probe —
    /// and a guard held by only some of them serializes nothing.
    ///
    /// Cross-PROCESS isolation is deliberately NOT attempted: cargo runs test binaries one
    /// at a time, so a process-wide guard is sufficient today. Anything that changes that
    /// (`cargo nextest`, which runs each test in its own process) would need a real
    /// advisory lock in the database instead.
    static CONFIG_TABLES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    pub(crate) async fn config_guard() -> tokio::sync::MutexGuard<'static, ()> {
        CONFIG_TABLES.lock().await
    }
}
