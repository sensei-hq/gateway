//! TM-9 (gateway#82): the gateway is a library; torii owns persistence (torii
//! `docs/DECISIONS.md` §11). The orchestrator crates ship the persistence TRAITS, the in-memory
//! stores and the conformance suite — no database driver, no Postgres feature, no schema, and
//! no operator CLI. Tenant-scoped Postgres stores and the `torii` CLI live in sensei-hq/torii.

use std::collections::{HashMap, HashSet, VecDeque};
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

/// The RESOLVED graph, not manifest text: a driver arriving through any crate an orchestrator
/// crate depends on (sensei-gateway, sensei-kernel, …) — or under another name — counts too.
/// Dev-dependencies are followed from the orchestrator crates themselves only; a dependency's
/// own dev-dependencies are never built for them.
#[test]
fn no_database_driver_is_reachable_from_any_orchestrator_crate() {
    const ROOTS: [&str; 4] = [
        "sensei-orchestrator-core",
        "sensei-orchestrator",
        "sensei-orchestrator-store",
        "sensei-orchestrator-testkit",
    ];
    const DRIVERS: [&str; 7] = [
        "sqlx",
        "tokio-postgres",
        "postgres",
        "diesel",
        "rusqlite",
        "sea-orm",
        "mysql",
    ];
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let out = std::process::Command::new(cargo)
        .args(["metadata", "--format-version", "1", "--manifest-path"])
        .arg(root().join("Cargo.toml"))
        .output()
        .expect("cargo metadata");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let meta: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let name: HashMap<&str, &str> = meta["packages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| (p["id"].as_str().unwrap(), p["name"].as_str().unwrap()))
        .collect();
    let nodes: HashMap<&str, &serde_json::Value> = meta["resolve"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| (n["id"].as_str().unwrap(), n))
        .collect();

    // BFS, remembering how each package was reached so a failure names the path.
    let mut via: HashMap<&str, Option<&str>> = HashMap::new();
    let mut queue: VecDeque<(&str, bool)> = VecDeque::new();
    for (id, n) in &name {
        if ROOTS.contains(n) {
            via.insert(id, None);
            queue.push_back((id, true));
        }
    }
    assert_eq!(
        via.len(),
        ROOTS.len(),
        "every orchestrator crate is in the graph"
    );
    let mut seen: HashSet<&str> = HashSet::new();
    while let Some((id, is_root)) = queue.pop_front() {
        if !seen.insert(id) {
            continue;
        }
        for d in nodes[id]["deps"].as_array().unwrap() {
            let kinds: Vec<Option<&str>> = d["dep_kinds"]
                .as_array()
                .unwrap()
                .iter()
                .map(|k| k["kind"].as_str())
                .collect();
            let built = is_root || kinds.iter().any(|k| *k != Some("dev"));
            let dep = d["pkg"].as_str().unwrap();
            if built && !via.contains_key(dep) {
                via.insert(dep, Some(id));
                queue.push_back((dep, false));
            }
        }
    }
    for id in &seen {
        let n = name[id];
        if DRIVERS
            .iter()
            .any(|d| n == *d || n.starts_with(&format!("{d}-")))
        {
            let mut path = vec![n];
            let mut cur = *id;
            while let Some(Some(parent)) = via.get(cur) {
                path.push(name[parent]);
                cur = parent;
            }
            panic!(
                "a database driver is reachable from the orchestrator: {} — persistence belongs \
                 to torii",
                path.join(" <- ")
            );
        }
    }
}
