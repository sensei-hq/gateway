//! TM-9 (gateway#82): the gateway is a library; torii owns persistence (torii
//! `docs/DECISIONS.md` §11). The orchestrator crates ship the persistence TRAITS, the in-memory
//! stores and the conformance suite — no database driver, no Postgres feature, no schema, and
//! no operator CLI. Tenant-scoped Postgres stores and the `torii` CLI live in sensei-hq/torii.

use std::path::{Path, PathBuf};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn no_orchestrator_crate_depends_on_a_database_driver_or_has_a_postgres_feature() {
    for krate in [
        "orchestrator-core",
        "orchestrator",
        "orchestrator-store",
        "orchestrator-testkit",
    ] {
        let manifest = root().join("crates").join(krate).join("Cargo.toml");
        // Dependency and feature lines only — a comment may say where persistence went.
        let text: String = std::fs::read_to_string(&manifest)
            .unwrap()
            .lines()
            .filter(|l| !l.trim_start().starts_with('#'))
            .collect::<Vec<_>>()
            .join("\n");
        // (`sensei-orchestrator`'s `test-support` is its test doubles, exported to torii's e2e —
        // not persistence; `orchestrator-store`'s went with `test_guard`, which needs sqlx.)
        for forbidden in ["sqlx", "postgres"] {
            assert!(
                !text.contains(forbidden),
                "{} still mentions {forbidden:?} — persistence belongs to torii",
                manifest.display()
            );
        }
    }
}

#[test]
fn the_workspace_carries_no_operator_cli_and_no_schema() {
    let workspace = std::fs::read_to_string(root().join("Cargo.toml")).unwrap();
    assert!(
        !workspace.contains("crates/torii"),
        "the torii CLI moved to sensei-hq/torii (crates/cli)"
    );
    assert!(
        !root().join("crates/torii").exists(),
        "crates/torii is gone"
    );
    assert!(
        !root().join("database").exists(),
        "the orchestrator schema lives in torii's database/ (registry.*, runs.*)"
    );
}
