//! gh#73 — the `llama-cpp-2` requirement's floor must exclude every version
//! that cannot build.
//!
//! A caret requirement makes its floor a *valid* resolution, so any downstream
//! lockfile written while the floor was current stays pinned to it. Two ends
//! were broken:
//!
//! - `< 0.1.151` vendors a cpp-httplib that does not compile against OpenSSL 4
//!   (`X509_get_subject_name` now returns `const X509_NAME *`).
//! - `0.1.158` moved tokenization onto `LlamaVocab`; the adapter is written
//!   against that API, so anything older no longer compiles here either.

const MIN_PATCH: u64 = 158;

#[test]
fn llama_cpp_2_floor_excludes_unbuildable_versions() {
    let manifest = include_str!("../Cargo.toml");
    let line = manifest
        .lines()
        .find(|l| l.trim_start().starts_with("llama-cpp-2"))
        .expect("llama-cpp-2 dependency line");
    let version = line
        .split("version = \"")
        .nth(1)
        .and_then(|rest| rest.split('"').next())
        .expect("llama-cpp-2 declares a version");

    let parts: Vec<u64> = version
        .trim_start_matches(['^', '='])
        .split('.')
        .map(|p| p.parse().expect("numeric version component"))
        .collect();
    assert_eq!(
        parts[..2],
        [0, 1],
        "llama-cpp-2 left 0.1.x — revisit this guard: {version}"
    );
    assert!(
        parts[2] >= MIN_PATCH,
        "llama-cpp-2 floor {version} admits versions that cannot build (gh#73); need >= 0.1.{MIN_PATCH}"
    );
}
