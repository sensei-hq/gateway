//! TM-5 (gateway#81): the memory backend boots and drives a run with NO database — the
//! development/CI backend, and the seam the move to torii plugs its own stores into.

use std::process::Command;

use orchestrator_core::{Graph, Node, NodeId, NodeKind};

fn torii() -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_torii"));
    // The point of the test: there is no database at all.
    c.env_remove("DATABASE_URL");
    c.env_remove("TORII_FENCE_VERSION");
    c.env_remove("TORII_BACKEND");
    c.env_remove("TORII_REGISTRY_DIR");
    c
}

/// A registry dir (one agent bound to chain `c`), a gateway config defining `c`, and a graph
/// whose only node waits for a human — so the run needs no model and no network to reach a
/// durable pause.
fn fixtures(dir: &std::path::Path) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    let reg = dir.join("registry");
    std::fs::create_dir_all(reg.join("agents")).unwrap();
    std::fs::write(
        reg.join("agents/researcher.md"),
        "---\nname: researcher\narea: research\nkind: lead\nchain: c\ntools: []\nskills: []\n---\nYou research.\n",
    )
    .unwrap();

    let gw = dir.join("gateway.json");
    std::fs::write(
        &gw,
        r#"{"routers":{"ollama":{"url":"http://127.0.0.1:11434"}},
            "chains":{"c":{"id":"c","capability":"text_chat","models":[],"fallback_triggers":[]}}}"#,
    )
    .unwrap();

    let graph = dir.join("graph.json");
    let g = Graph {
        nodes: vec![Node {
            id: NodeId("gate".into()),
            kind: NodeKind::AwaitSignal { timeout: None },
            deps: vec![],
        }],
    };
    std::fs::write(&graph, serde_json::to_string(&g).unwrap()).unwrap();
    (reg, gw, graph)
}

#[test]
fn run_submit_on_the_memory_backend_needs_no_database() {
    let dir = tempfile::tempdir().unwrap();
    let (reg, gw, graph) = fixtures(dir.path());
    let out = torii()
        .env("TORII_BACKEND", "memory")
        .env("TORII_REGISTRY_DIR", &reg)
        .env("TORII_FENCE_VERSION", "v1")
        .args(["run", "submit", "--graph"])
        .arg(&graph)
        .arg("--gateway-config")
        .arg(&gw)
        .output()
        .expect("spawn torii");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "exit {:?}\nstdout: {stdout}\nstderr: {stderr}",
        out.status.code()
    );
    assert!(stdout.contains("submitted: "), "{stdout}");
    assert!(
        stdout.contains("paused: ") && stdout.contains("at node gate"),
        "the run reached its durable pause on the memory backend: {stdout}"
    );
}

#[test]
fn the_default_backend_is_still_postgres_and_still_needs_a_database_url() {
    let out = torii()
        .args(["run", "list-paused"])
        .output()
        .expect("spawn torii");
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("DATABASE_URL"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn an_unknown_backend_is_refused_naming_the_choices() {
    let out = torii()
        .env("TORII_BACKEND", "mysql")
        .args(["run", "list-paused"])
        .output()
        .expect("spawn torii");
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("mysql") && err.contains("memory") && err.contains("postgres"),
        "{err}"
    );
}
