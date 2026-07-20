# Cloud-Providers Extraction Implementation Plan (PR 2 of the workspace re-layering)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Extract the concrete cloud provider adapters + their HTTP/job/OpenAI-compat helpers out of `gateway` into a new `cloud-providers` crate (published `sensei-cloud-providers`, imported as `cloud_providers`), gated behind a **default** `cloud` feature — so the AWS Bedrock SDK + `reqwest` leave `gateway`'s core and a `--no-default-features` build of `gateway` is a lean, provider-agnostic routing engine.

**Architecture:** Behaviour-preserving relocation. 16 adapter modules + `base.rs`/`async_job.rs`/`openai_compat.rs` move to `cloud-providers` (which depends on `kernel`). `gateway` drops the adapter-only deps, gains an optional `cloud-providers` dep behind `cloud` (in `default`), and re-exports the adapters under their historical `gateway::adapters::<provider>::…` paths when `cloud` is on. `noop.rs` + `adapters/mod.rs` stay in `gateway`. Adapter construction/registration is already consumer-driven, so there is no wiring to rework. Green at every task; the existing test suite (unit tests move with their files; wiremock mock tests move to `cloud-providers/tests/`) is the safety net.

**Tech Stack:** Rust edition 2024, cargo workspace at `/Users/Jerry/Developer/strategos/gateway`. Package names carry the `sensei-` prefix; import names stay short via `[lib] name` + `package =` aliases.

**Reference spec:** `docs/design/provisioning-supervisor-and-workspace-layering.md` (§2, §9 step 2).

---

## Conventions for every task

- Paths relative to repo root `/Users/Jerry/Developer/strategos/gateway`. Run `cargo`/`git` from there.
- Work on branch `refactor/cloud-providers` (already created off `develop`). Do NOT create/switch branches.
- Commit ONLY intended files by explicit path (never `git add -A`; never touch `site/`). Do NOT push or tag — the controller tags after each task and handles landing.
- SAFETY (concurrent sessions can touch this checkout): at the start of each task, verify `git status` is clean and HEAD is the expected SHA; if not, STOP and report BLOCKED.
- "Green" for a package `P`: `cargo build -p P`, `cargo test -p P`, `cargo clippy -p P -- -D warnings` all pass.
- The native toolchain is present in this environment, so engine-feature builds of `gateway-embedded` are not a concern here (this PR doesn't touch it).
- `[lib] name` maps: `sensei-kernel`→`kernel`, `sensei-gateway`→`gateway`, `sensei-cloud-providers`→`cloud_providers`.

---

## Task 1: Scaffold the `cloud-providers` crate

**Files:** Create `crates/cloud-providers/Cargo.toml`, `crates/cloud-providers/src/lib.rs`; modify root `Cargo.toml`.

- [ ] **Step 1: Add to workspace.** Root `Cargo.toml` `members`:
```toml
members = ["crates/kernel", "crates/cloud-providers", "crates/gateway", "crates/gateway-embedded"]
```

- [ ] **Step 2: `crates/cloud-providers/Cargo.toml`** — best-effort dep set (the moved code uses these; Task 2 adds any the compiler flags as missing and prunes any unused):
```toml
[package]
name = "sensei-cloud-providers"
version = "0.3.1"
edition = "2024"
description = "Cloud LLM provider adapters (OpenAI, Anthropic, Bedrock, Gemini, …) for the sensei gateway"
license = "MIT"

[lib]
name = "cloud_providers"

[dependencies]
kernel = { package = "sensei-kernel", path = "../kernel" }
aws-config = { version = "1", default-features = false, features = ["default-https-client", "rt-tokio", "credentials-process", "sso", "behavior-version-latest"] }
aws-sdk-bedrockruntime = { version = "1", default-features = false, features = ["default-https-client", "rt-tokio"] }
aws-smithy-types = "1"
async-stream = "0.3"
async-trait = "0.1"
base64 = "0.22"
bytes = "1"
futures = "0.3"
pin-project-lite = "0.2"
reqwest = { version = "0.12", default-features = false, features = ["json", "multipart", "stream", "rustls-tls"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["full"] }
tracing = "0.1"

[dev-dependencies]
wiremock = "0.6"
tokio = { version = "1", features = ["full", "test-util"] }
```

- [ ] **Step 3: Placeholder `crates/cloud-providers/src/lib.rs`:**
```rust
//! `sensei-cloud-providers` — cloud LLM provider adapters for the sensei
//! gateway. Each adapter implements the `kernel` capability traits; construction
//! is caller-driven (register into a `kernel::adapters::AdapterRegistry`).
```

- [ ] **Step 4:** `cargo build -p sensei-cloud-providers` → compiles (empty lib; unused deps are not a hard error).

- [ ] **Step 5: Commit.**
```bash
git add Cargo.toml crates/cloud-providers/Cargo.toml crates/cloud-providers/src/lib.rs
git commit -m "chore(cloud-providers): scaffold empty sensei-cloud-providers crate

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Move the adapter modules + helpers into `cloud-providers`

**Files:** `git mv` 19 files `crates/gateway/src/adapters/<f>.rs` → `crates/cloud-providers/src/<f>.rs`; create `crates/cloud-providers/src/lib.rs` module list; rewrite intra-crate paths.

The 19 files: `anthropic, bedrock, openai, gemini, grok, huggingface, ollama, together, kling, luma, replicate, runway, fal, flux, recraft, stability` (16 adapters) + `base, async_job, openai_compat` (3 helpers). **Do NOT move `noop.rs` or `mod.rs`.**

- [ ] **Step 1: Move the files.**
```bash
for f in anthropic bedrock openai gemini grok huggingface ollama together \
         kling luma replicate runway fal flux recraft stability \
         base async_job openai_compat; do
  git mv "crates/gateway/src/adapters/$f.rs" "crates/cloud-providers/src/$f.rs"
done
```

- [ ] **Step 2: Declare the modules** in `crates/cloud-providers/src/lib.rs` (append after the doc comment):
```rust
pub mod anthropic;
pub mod async_job;
pub mod base;
pub mod bedrock;
pub mod fal;
pub mod flux;
pub mod gemini;
pub mod grok;
pub mod huggingface;
pub mod kling;
pub mod luma;
pub mod ollama;
pub mod openai;
pub mod openai_compat;
pub mod recraft;
pub mod replicate;
pub mod runway;
pub mod stability;
pub mod together;
```
Note: `openai_compat` is currently `pub(crate)`-scoped internally — declaring it `pub mod` here is fine (its items may stay `pub(crate)`; adapters reference it via `crate::openai_compat::…`).

- [ ] **Step 3: Rewrite intra-crate paths** in all 19 moved files. These modules previously lived under `gateway::adapters`, so their paths must be re-pointed. Apply (BSD-sed compatible — `\b` is unsupported on macOS sed, use the char-class form):
```bash
cd crates/cloud-providers/src
# types + kernel-owned items now come from `kernel`
sed -i '' -E 's/([^a-zA-Z0-9_]|^)crate::types::/\1kernel::types::/g' *.rs
sed -i '' -E 's/([^a-zA-Z0-9_]|^)crate::adapters::capability/\1kernel::adapters::capability/g' *.rs
# sibling helpers moved to this crate root: crate::adapters::X / super::X  ->  crate::X
sed -i '' -E 's/crate::adapters::(base|async_job|openai_compat)/crate::\1/g' *.rs
sed -i '' -E 's/(^|[^a-zA-Z0-9_])super::(base|async_job|openai_compat|capability)/\1crate::\2/g' *.rs
cd -
```
Then **manually resolve the remainder** the compiler flags. Known cases to handle by hand or targeted edit:
- Trait/registry imports that were `use crate::adapters::{AdapterRegistry, RegisterInto, Model, ChatModel, …}` → `use kernel::adapters::{…}` (or `kernel::adapters::capability::{…}` for the capability traits). `super::capability::X` → `kernel::adapters::capability::X`.
- Any `super::base` / `super::async_job` / `super::openai_compat` not caught above → `crate::…`.
- `crate::adapters::noop` references (if any — unlikely) stay pointing at gateway's noop? No: cloud adapters must not depend on `noop`. If found, report it (a cloud adapter shouldn't reference noop).
- Inside `base.rs`/`async_job.rs`/`openai_compat.rs`: the same `crate::types::` → `kernel::types::` rewrite applies (covered by the first sed).

- [ ] **Step 4: Build cloud-providers to green**, adding any missing dep the compiler names and pruning obviously-unused ones (`cargo build -p sensei-cloud-providers 2>&1`). Iterate until:
```
cargo build -p sensei-cloud-providers
cargo test  -p sensei-cloud-providers          # in-file #[cfg(test)] unit tests move with the files
cargo clippy -p sensei-cloud-providers -- -D warnings
```
all pass. Grep to confirm no stale references remain:
```bash
grep -rn 'crate::types::\|crate::adapters::' crates/cloud-providers/src/ || echo "clean"
```
(Expected: `clean` — everything shared now comes from `kernel::…`, sibling helpers via `crate::…`.)

- [ ] **Step 5: Commit** (the moved files + cloud-providers lib.rs/Cargo.toml; gateway is still broken here because `adapters/mod.rs` still `pub mod`-s the now-missing files — Task 3 fixes gateway; that's expected mid-relocation, so DO NOT build `-p sensei-gateway` yet). Commit only cloud-providers paths:
```bash
git add crates/cloud-providers Cargo.toml
git commit -m "refactor(cloud-providers): relocate cloud adapters + helpers from gateway

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Slim `gateway` + add the `cloud` feature

**Files:** `crates/gateway/src/adapters/mod.rs`, `crates/gateway/Cargo.toml`.

- [ ] **Step 1: Rewrite `crates/gateway/src/adapters/mod.rs`** — drop the moved `pub mod` declarations, keep `noop` + the kernel re-exports, add the feature-gated cloud re-export:
```rust
pub mod noop;

// Capability traits + registry live in `kernel`; re-export under the historical
// `gateway::adapters::…` paths so internal code + downstream compile unchanged.
pub use kernel::adapters::capability;
pub use kernel::adapters::capability::{
    ChatModel, EmbedModel, ImageModel, Model, SttModel, TtsModel, VideoModel,
};
pub use kernel::adapters::{AdapterRegistry, RegisterInto};

// Cloud provider adapters live in the `cloud-providers` crate, compiled only
// with the (default) `cloud` feature. Re-export them under their historical
// `gateway::adapters::<provider>::…` paths so cloud consumers are unaffected.
#[cfg(feature = "cloud")]
pub use cloud_providers::{
    anthropic, async_job, base, bedrock, fal, flux, gemini, grok, huggingface,
    kling, luma, ollama, openai, openai_compat, recraft, replicate, runway,
    stability, together,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::noop::NoopAdapter;
    use std::sync::Arc;

    #[tokio::test]
    async fn registry_registers_and_lists_via_reexport() {
        let reg = AdapterRegistry::new();
        reg.register(Arc::new(NoopAdapter)).await;
        assert!(reg.chat("noop").await.is_some());
        assert!(reg.chat("nonexistent").await.is_none());
        assert_eq!(reg.list().await, vec!["noop".to_string()]);
    }
}
```

- [ ] **Step 2: Edit `crates/gateway/Cargo.toml`.** Remove the adapter-only deps: `aws-config`, `aws-sdk-bedrockruntime`, `aws-smithy-types`, `reqwest`, `base64`, `bytes`. Add the optional cloud dep + feature block (keep `async-trait`, `async-stream`, `chrono`, `futures`, `kernel`, `pin-project-lite`, `serde`, `serde_json`, `thiserror`, `tokio`, `tracing`, `uuid`):
```toml
[features]
default = ["cloud"]
cloud = ["dep:cloud-providers"]

[dependencies]
cloud-providers = { package = "sensei-cloud-providers", path = "../cloud-providers", optional = true }
# … the retained deps …
```
(Keep the existing dev-dependencies. If `cargo build -p sensei-gateway --no-default-features` later errors that a removed dep was still used by gateway core, re-add exactly that dep — but per the recon, none of the six are used outside the moved adapters.)

- [ ] **Step 3: Verify both feature configurations.**
```
cargo build -p sensei-gateway                        # default (cloud on): re-exports resolve
cargo build -p sensei-gateway --no-default-features   # lean: no cloud, no aws/reqwest
cargo test  -p sensei-gateway
cargo clippy -p sensei-gateway --all-targets -- -D warnings
cargo clippy -p sensei-gateway --no-default-features -- -D warnings
```
All pass. (Default build compiles `cloud-providers`; the lean build must NOT.)

- [ ] **Step 4: Confirm the dependency split.**
```bash
cargo tree -p sensei-gateway --no-default-features | grep -i 'aws-\|reqwest' || echo "lean gateway: no aws/reqwest"
cargo tree -p sensei-gateway | grep -ci 'aws-'   # default: >0 (via cloud-providers)
```
Expected: lean build has no `aws-`/`reqwest`; default build pulls them via `cloud-providers`.

- [ ] **Step 5: Commit.**
```bash
git add crates/gateway/src/adapters/mod.rs crates/gateway/Cargo.toml
git commit -m "refactor(gateway): move cloud adapters behind default 'cloud' feature -> cloud-providers

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Move the wiremock mock tests to `cloud-providers`

**Files:** `git mv` 9 test files `crates/gateway/tests/<t>.rs` → `crates/cloud-providers/tests/<t>.rs`; fix imports.

The 9 files: `adapter_integration, grok_mock, kling_mock, luma_mock, fal_mock, flux_mock, replicate_mock, runway_mock, together_mock`. (Leave `reexport_paths.rs` in `gateway`.)

- [ ] **Step 1: Move.**
```bash
mkdir -p crates/cloud-providers/tests
for t in adapter_integration grok_mock kling_mock luma_mock fal_mock flux_mock replicate_mock runway_mock together_mock; do
  git mv "crates/gateway/tests/$t.rs" "crates/cloud-providers/tests/$t.rs"
done
```

- [ ] **Step 2: Fix imports** in the moved test files:
```bash
cd crates/cloud-providers/tests
sed -i '' -E 's/gateway::adapters::(capability)/kernel::adapters::\1/g' *.rs
sed -i '' -E 's/gateway::adapters::/cloud_providers::/g' *.rs
sed -i '' -E 's/([^a-zA-Z0-9_]|^)gateway::types::/\1kernel::types::/g' *.rs
sed -i '' -E 's/([^a-zA-Z0-9_]|^)gateway::(GatewayError|Capability|InferenceRequest|InferenceResponse)/\1kernel::\2/g' *.rs
cd -
grep -rn 'gateway::' crates/cloud-providers/tests/ || echo "no stray gateway:: refs"
```
Resolve any remaining `gateway::…` the grep finds (these tests should reference `cloud_providers::…` for adapters and `kernel::…` for shared types).

- [ ] **Step 3: Run the moved tests.**
```
cargo test -p sensei-cloud-providers
```
Expected: all mock/integration tests pass (they were green as `gateway` tests before the move).

- [ ] **Step 4: Commit.**
```bash
git add crates/cloud-providers/tests crates/gateway/tests
git commit -m "test(cloud-providers): relocate provider mock/integration tests

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Lock re-export paths + tooling + workspace verify

**Files:** `crates/gateway/tests/reexport_paths.rs`, `Makefile`, `README.md`.

- [ ] **Step 1: Extend the re-export guard** — add a feature-gated block to `crates/gateway/tests/reexport_paths.rs` that names a representative cloud path so the `cloud` re-export is locked:
```rust
#[cfg(feature = "cloud")]
#[allow(unused_imports)]
use gateway::adapters::{
    anthropic::AnthropicAdapter, bedrock::BedrockAdapter, openai::OpenAIAdapter,
};
```
(Append below the existing imports; keep the existing `reexport_paths_resolve` test.)

- [ ] **Step 2: Verify the guard both ways.**
```
cargo test -p sensei-gateway --test reexport_paths                     # cloud on
cargo test -p sensei-gateway --test reexport_paths --no-default-features
```
Both compile + pass (the cloud block is cfg'd out in the lean run).

- [ ] **Step 3: Makefile.** Add `crates/cloud-providers/Cargo.toml` to the `bump` target's version-sed + `git add` (same pattern as `kernel`). Leave `--workspace` and `cov` targets as-is. Verify: `grep -n 'crates/.*Cargo.toml' Makefile` shows all four crates in `bump`.

- [ ] **Step 4: README.** Add a `cloud-providers` (`sensei-cloud-providers`) row to the crates table (cloud provider adapters, feature `cloud`). Note the `cloud` default feature + `--no-default-features` for a lean gateway. Match existing table format.

- [ ] **Step 5: Full workspace gate.**
```
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```
All green.

- [ ] **Step 6: Commit.**
```bash
git add crates/gateway/tests/reexport_paths.rs Makefile README.md
git commit -m "chore: lock cloud re-export paths; account for cloud-providers in tooling/docs

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-review checklist (controller, before landing)

- [ ] **Spec coverage:** §9 step 2 — cloud adapters extracted, AWS SDK behind the default `cloud` feature, `--no-default-features` gateway is lean (verified via `cargo tree`).
- [ ] **Behaviour preservation:** moved files are renames (content-identical bar the mechanical path rewrites); full suite green; the `gateway::adapters::<provider>` paths still resolve with `cloud` on.
- [ ] **No placeholders / dangling refs:** grep confirms no `crate::types::`/`crate::adapters::` left in cloud-providers, no stray `gateway::` in the moved tests.
- [ ] **Dep hygiene:** the six adapter-only deps are gone from `gateway`; `cloud-providers` deps are the used set (prune unused via inspection or `cargo machete` if available).
- [ ] Whole-branch code review, then merge to `develop` (per PR-1 flow).

## Open decision (confirm)

- **`cloud` is a default feature** (existing cloud consumers unaffected; opt out with `--no-default-features` for a lean routing core). Flip to opt-in only if you want lean-by-default at the cost of a downstream feature addition.
