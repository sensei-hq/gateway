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
    /// **The SECOND layer, since the conditional-ignore gate.** The first is
    /// `#[cfg_attr(not(have_database_url), ignore = "...")]` on every test below, driven by
    /// this package's `build.rs` — that is what makes a database-less run report these as
    /// `ignored` rather than as PASSED, which is what the runtime early-return made them.
    /// This helper still runs, and still announces, for the case the cfg cannot see: the
    /// variable present at BUILD time and gone at run time.
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

    /// The raw lookup with no side effects. `db_url()` above stays the single place that
    /// ANNOUNCES a skip; the guard's advisory-lock connection reads the same variable
    /// through its own copy of this (and must NOT print a second SKIP line for one test).
    fn database_url_raw() -> Option<String> {
        std::env::var(crate::boot::ENV_DATABASE_URL)
            .ok()
            .filter(|s| !s.trim().is_empty())
    }

    /// The guard itself lives in `orchestrator_store::test_guard` — the ONE implementation,
    /// shared with `orchestrator-store`'s own suite AND with `sensei-orchestrator`'s
    /// `postgres_e2e` (which has no `sqlx` dependency and so could not hold a copy at all; it
    /// held NONE, which is what made the advisory lock worthless for everyone else). This was
    /// a second private copy of the same construction, keyed by a `const` that had to be kept
    /// equal to the store crate's by comment alone; re-exporting is what makes drift
    /// impossible rather than merely discouraged.
    ///
    /// Re-exported under the old names so every `crate::test_guard::config_guard()` call site
    /// is unchanged. See the shared module for the two isolation layers, panic safety, and
    /// re-entrancy.
    pub(crate) use orchestrator_store::test_guard::config_guard;
}

#[cfg(test)]
mod test_guard_agrees_with_the_shared_one {
    /// The shared guard opens its OWN advisory-lock connection from `DATABASE_URL`. If this
    /// crate ever read a DIFFERENT variable, the lock would be taken on one database while
    /// the test it is isolating ran against another — silently isolating nothing. Cheap to
    /// pin, impossible to notice otherwise.
    #[test]
    fn the_guard_and_this_crate_read_the_same_env_var() {
        assert_eq!(
            crate::boot::ENV_DATABASE_URL,
            orchestrator_store::test_guard::ENV_DATABASE_URL,
            "torii's DB tests and the guard isolating them must point at the SAME database"
        );
    }
}
