//! Doc ↔ code sync for the two references SP-DEC-1's review found incomplete.
//!
//! Both documents claim completeness — the matrices say they are "derived from
//! each adapter's `RegisterInto` impl", the fallback table answers "which errors
//! can vs. cannot advance the walk" — so each is checked against the source it
//! claims to be derived from, not against a hand-kept list.

use std::path::{Path, PathBuf};

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(rel: &str) -> String {
    std::fs::read_to_string(repo().join(rel)).unwrap_or_else(|e| panic!("{rel}: {e}"))
}

/// Every `.rs` file under `dir`, recursively.
fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// The adapter MODULE (matrix rows are named by module: `llama_cpp`,
/// `embedded_llama`, …) of every file with a `RegisterInto` impl. A module in
/// a directory (`anthropic/mod.rs`, `llama_cpp/mod.rs`) is named by the
/// directory.
fn registering_modules(src: &str) -> Vec<String> {
    let mut files = Vec::new();
    rust_files(&repo().join(src), &mut files);
    let mut modules: Vec<String> = files
        .into_iter()
        .filter(|f| {
            std::fs::read_to_string(f)
                .unwrap()
                .contains("RegisterInto for ")
        })
        .map(|f| {
            let stem = f.file_stem().unwrap().to_string_lossy().into_owned();
            if stem == "mod" {
                f.parent()
                    .unwrap()
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .into_owned()
            } else {
                stem
            }
        })
        .collect();
    modules.sort();
    modules.dedup();
    modules
}

/// Matrix row names for a module: `systemone` is the generic adapter the
/// facade registers as the `openrouter` and `typesafe` routers.
fn row_names(module: &str) -> Vec<&str> {
    match module {
        "systemone" => vec!["openrouter", "typesafe"],
        other => vec![other],
    }
}

#[test]
fn both_capability_matrices_have_a_row_for_every_registering_adapter() {
    let mut modules = registering_modules("crates/cloud-providers/src");
    modules.extend(registering_modules("crates/local-providers/src/adapters"));
    assert!(modules.len() > 15, "scan found too little: {modules:?}");

    for doc in [
        "docs/features/README.md",
        "docs/features/inference/capabilities-and-adapters.md",
    ] {
        let text = read(doc);
        let missing: Vec<&str> = modules
            .iter()
            .flat_map(|m| row_names(m))
            .filter(|row| !text.contains(&format!("| `{row}`")))
            .collect();
        assert!(
            missing.is_empty(),
            "{doc} claims to derive its matrix from RegisterInto but has no row for {missing:?}"
        );
    }
}

/// Variant names of `GatewayError`, read from its definition.
fn gateway_error_variants() -> Vec<String> {
    let src = read("crates/kernel/src/types/error.rs");
    let body = src
        .split("pub enum GatewayError {")
        .nth(1)
        .expect("GatewayError definition")
        .split("\n}\n")
        .next()
        .unwrap();
    body.lines()
        .filter_map(|l| {
            // Variant lines are indented exactly four spaces and start upper-case.
            let rest = l.strip_prefix("    ")?;
            let first = rest.chars().next()?;
            if !first.is_ascii_uppercase() {
                return None;
            }
            let name: String = rest.chars().take_while(|c| c.is_alphanumeric()).collect();
            Some(name)
        })
        .collect()
}

#[test]
fn the_fallback_table_answers_for_every_gateway_error_variant() {
    let variants = gateway_error_variants();
    assert!(
        variants.contains(&"InvalidRequest".to_string()) && variants.len() >= 15,
        "variant scan looks wrong: {variants:?}"
    );
    let doc = read("docs/features/routing/fallback-chains.md");
    let missing: Vec<&String> = variants
        .iter()
        .filter(|v| {
            !doc.lines()
                .any(|l| l.starts_with(&format!("| `{v}")) && l.contains('|'))
        })
        .collect();
    assert!(
        missing.is_empty(),
        "fallback-chains.md's 'which errors can advance the walk' table has no row for {missing:?}"
    );
}
