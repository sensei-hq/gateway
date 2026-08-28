//! Turns the presence of `DATABASE_URL` into a `cfg`, so the Postgres-backed tests can be
//! `#[ignore]`d when there is no database instead of silently returning early.
//!
//! **The defect this closes.** Those tests were `DATABASE_URL`-guarded at RUNTIME: absent a
//! database each one printed a `SKIP` line to stderr and RETURNED — and libtest counted the
//! return as a PASS. So `cargo test --workspace` reported the same green number whether it
//! had exercised Postgres or touched nothing at all, and CI's `build · test` job (which sets
//! no `DATABASE_URL`) reported 48 passing database tests it had never run. A Postgres
//! regression could land on `main` behind a green check.
//!
//! **Why a build script rather than a plain `#[ignore]` or a feature.** Both of those are
//! static, and either one would break the command a developer with a live database actually
//! runs: a plain `#[ignore]` needs `-- --ignored` to run at all, and a `required-features`
//! test target makes `cargo test -p sensei-torii --test e2e_pg` fail outright. The
//! requirement is a CONDITIONAL ignore — ignored when there is no database, ordinary tests
//! when there is — and a build-time cfg is the only mechanism libtest offers for that.
//!
//! `rerun-if-env-changed` is what makes it track the variable: setting `DATABASE_URL` and
//! re-running rebuilds the test target with the cfg on, so the same command that skipped
//! now runs. The runtime `db_url()` guard stays as the second layer, for the case where the
//! variable is set at build time and gone at run time.
fn main() {
    println!("cargo::rerun-if-env-changed=DATABASE_URL");
    println!("cargo::rustc-check-cfg=cfg(have_database_url)");
    if std::env::var("DATABASE_URL").is_ok_and(|v| !v.trim().is_empty()) {
        println!("cargo::rustc-cfg=have_database_url");
    }
}
