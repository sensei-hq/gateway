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
        let url = std::env::var(crate::boot::ENV_DATABASE_URL)
            .ok()
            .filter(|s| !s.trim().is_empty());
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
