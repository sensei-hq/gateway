//! Turns the presence of `DATABASE_URL` into a `cfg`, so the Postgres-backed tests can be
//! `#[ignore]`d when there is no database instead of silently returning early.
//!
//! The reasoning is written out once, in `crates/torii/build.rs`. In short: a runtime
//! `DATABASE_URL` guard makes libtest count a skipped database test as a PASS, so a green
//! `cargo test --workspace` said nothing about whether Postgres was exercised; and a
//! CONDITIONAL ignore is the only shape that keeps `cargo test -p sensei-orchestrator-store --features ...`
//! running the full suite when a database IS configured, which a static `#[ignore]` or a
//! `required-features` target would not.
//!
//! Five lines duplicated per package rather than shared through a build-dependency crate:
//! a build script is per-package by construction, so sharing would add a workspace member
//! and a `[build-dependencies]` edge to save nothing. The canonical explanation is the one
//! link above.
fn main() {
    println!("cargo::rerun-if-env-changed=DATABASE_URL");
    println!("cargo::rustc-check-cfg=cfg(have_database_url)");
    if std::env::var("DATABASE_URL").is_ok_and(|v| !v.trim().is_empty()) {
        println!("cargo::rustc-cfg=have_database_url");
    }
}
