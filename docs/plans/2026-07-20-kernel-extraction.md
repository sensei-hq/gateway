# Kernel Extraction Implementation Plan (PR 1 of the workspace re-layering)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract a new `kernel` crate (published name `sensei-kernel`, imported as `kernel`) holding the shared vocabulary — everything under `types/`, the capability traits, and `AdapterRegistry`/`RegisterInto` — out of `gateway`, so `gateway-embedded` (and, later, `cloud-providers`/`local-*`) depend on the lean kernel instead of the whole routing engine + AWS SDK.

**Architecture:** Behaviour-preserving relocation. Files move to `crates/kernel`; `gateway` gains a `kernel` dependency and re-exports the moved items from `lib.rs` / `adapters/mod.rs`, so every internal `crate::types::…` / `crate::adapters::…` path and every downstream `gateway::…` path keeps resolving unchanged (only ~2 files' `use` lines change in `gateway`). `gateway-embedded` re-points its imports from `gateway::…` to `kernel::…` and drops the `gateway` dep. Done coexist-then-verify with `build` + `test` + `clippy` green at every task boundary; the existing test suite is the safety net.

**Tech Stack:** Rust (edition 2024), cargo workspace at `/Users/Jerry/Developer/strategos/gateway`; `serde`, `thiserror`, `async-trait`, `futures`, `tokio`, `chrono`, `uuid`, `base64`, `reqwest`.

**Reference spec:** `docs/design/provisioning-supervisor-and-workspace-layering.md` (§2, §9 step 1).

---

## Conventions for every task

- All paths are relative to the repo root `/Users/Jerry/Developer/strategos/gateway`.
- Run cargo from the repo root. `gateway-embedded` engine features pull heavy native deps (C++ toolchains); **do not** enable them for the default test runs. Feature-gated adapter bodies are compile-checked in Task 5's dedicated step.
- "Green" after a code step means, for the affected package `P`: `cargo build -p P` and `cargo test -p P` pass and `cargo clippy -p P -- -D warnings` is clean.
- Preserve git history with `git mv` for whole-file moves.
- Commit after each task with the message shown. **Do not push or tag** — the maintainer pushes/tags (PR-only landing).
- **Naming scope for this PR:** the *new* crate is born as `sensei-kernel` (imported via the alias `kernel = { package = "sensei-kernel", path = "../kernel" }`). Renaming the *existing* `gateway` / `gateway-embedded` packages to `sensei-*` is intentionally deferred (Task 7 is an optional, self-contained rename) so this PR's cargo `-p` targets and coordinates stay stable. Deviation from spec §9: the `ModelEntry`/`ModelResolver` vocabulary is **not** moved here — it moves to `kernel` in PR 3, when `local-providers` first needs it.

---

## Task 1: Scaffold the `sensei-kernel` crate

**Files:**
- Create: `crates/kernel/Cargo.toml`
- Create: `crates/kernel/src/lib.rs`
- Modify: `Cargo.toml` (workspace members)

- [ ] **Step 1: Add the crate to the workspace.** Edit the root `Cargo.toml` `members` list:

```toml
[workspace]
members = ["crates/kernel", "crates/gateway", "crates/gateway-embedded"]
resolver = "2"
```

(Leave the existing `[profile.release]` block untouched.)

- [ ] **Step 2: Write `crates/kernel/Cargo.toml`.** These are exactly the external crates the moved modules use (see the plan's inventory). `reqwest` is required solely for `GatewayError::Network(#[from] reqwest::Error)`, so it is declared with no default features and no TLS backend.

```toml
[package]
name = "sensei-kernel"
version = "0.3.1"
edition = "2024"
description = "Shared types, capability traits, and the adapter registry for the sensei gateway"
license = "MIT"

[lib]
name = "kernel"

[dependencies]
async-trait = "0.1"
base64 = "0.22"
chrono = { version = "0.4", features = ["serde"] }
futures = "0.3"
reqwest = { version = "0.12", default-features = false }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
tokio = { version = "1", features = ["sync"] }
uuid = { version = "1", features = ["v4", "serde"] }
```

- [ ] **Step 3: Write a placeholder `crates/kernel/src/lib.rs`.**

```rust
//! `sensei-kernel` — the shared vocabulary of the sensei gateway: config, IO,
//! cost, trace, and error types, the capability traits, and the adapter
//! registry. This crate depends on nothing else in the workspace; every other
//! gateway crate depends on it.
```

- [ ] **Step 4: Verify the empty crate builds.**

Run: `cargo build -p sensei-kernel`
Expected: `Compiling sensei-kernel v0.3.1` … `Finished` (no errors).

- [ ] **Step 5: Commit.**

```bash
git add Cargo.toml crates/kernel/Cargo.toml crates/kernel/src/lib.rs
git commit -m "chore(kernel): scaffold empty sensei-kernel crate"
```

---

## Task 2: Move `types/` into the kernel

**Files:**
- Move: `crates/gateway/src/types/` → `crates/kernel/src/types/` (all 8 files)
- Modify: `crates/kernel/src/lib.rs`
- Modify: `crates/gateway/Cargo.toml`
- Modify: `crates/gateway/src/lib.rs`

- [ ] **Step 1: Move the module (preserving history).**

```bash
git mv crates/gateway/src/types crates/kernel/src/types
```

- [ ] **Step 2: Expose `types` from the kernel.** Replace `crates/kernel/src/lib.rs` with:

```rust
//! `sensei-kernel` — the shared vocabulary of the sensei gateway: config, IO,
//! cost, trace, and error types, the capability traits, and the adapter
//! registry. This crate depends on nothing else in the workspace; every other
//! gateway crate depends on it.

pub mod types;

pub use types::capability::Capability;
pub use types::error::GatewayError;
pub use types::request::{InferenceRequest, InferenceResponse};
```

The moved files reference each other only via `super::` / `crate::types::…`, which resolve unchanged inside the kernel.

- [ ] **Step 3: Verify the kernel compiles with the moved types + their tests.**

Run: `cargo test -p sensei-kernel`
Expected: PASS — the `types/*` serde round-trip and redaction tests (e.g. `router_config_debug_redacts_api_key`, `constraints_config_roundtrip_with_per_capability`) run under the kernel.

- [ ] **Step 4: Add the kernel dependency to `gateway`.** In `crates/gateway/Cargo.toml`, under `[dependencies]`, add (keep every existing dependency):

```toml
kernel = { package = "sensei-kernel", path = "../kernel" }
```

- [ ] **Step 5: Re-export `types` from `gateway`.** In `crates/gateway/src/lib.rs`, replace the line `pub mod types;` with `pub use kernel::types;`. The file's module block becomes:

```rust
pub mod adapters;
pub mod budget;
pub mod circuit_breaker;
pub mod config;
mod dispatch;
pub mod engine;
pub mod purpose;
pub mod selection;
pub mod store;
pub use kernel::types;

pub use config::GatewayBuilder;
pub use engine::Gateway;
pub use purpose::{ModelHint, Purpose, PurposeBuilder, PurposeResult, StepBuilder, StepInput};
pub use types::capability::Capability;
pub use types::error::GatewayError;
pub use types::request::{InferenceRequest, InferenceResponse};
```

Because `pub use kernel::types;` establishes the name `types` at the gateway crate root, every existing `crate::types::…` inside `gateway` and every downstream `gateway::types::…` continues to resolve — no other `gateway` file changes in this task.

- [ ] **Step 6: Verify `gateway` still builds and passes (via the re-export).**

Run: `cargo build -p gateway && cargo test -p gateway && cargo clippy -p gateway -- -D warnings`
Expected: build + all existing `gateway` tests PASS; clippy clean. (`gateway-embedded` is untouched here — it still depends on `gateway`, whose `gateway::types::…` re-export keeps it compiling.)

- [ ] **Step 7: Commit.**

```bash
git add crates/kernel/src crates/gateway/Cargo.toml crates/gateway/src/lib.rs
git commit -m "refactor(kernel): move types/* into sensei-kernel; gateway re-exports them"
```

---

## Task 3: Move the capability traits + `AdapterRegistry`/`RegisterInto` into the kernel

**Files:**
- Move: `crates/gateway/src/adapters/capability.rs` → `crates/kernel/src/adapters/capability.rs`
- Create: `crates/kernel/src/adapters/mod.rs`
- Modify: `crates/kernel/src/lib.rs`
- Modify: `crates/gateway/src/adapters/mod.rs`

- [ ] **Step 1: Move `capability.rs` (preserving history).**

```bash
mkdir -p crates/kernel/src/adapters
git mv crates/gateway/src/adapters/capability.rs crates/kernel/src/adapters/capability.rs
```

Its `use crate::types::{config, error, io, request}::…` lines now resolve inside the kernel (types live there) — no edit needed.

- [ ] **Step 2: Create `crates/kernel/src/adapters/mod.rs`** with the registry + `RegisterInto` (relocated verbatim from `gateway`'s old `adapters/mod.rs`), declaring the `capability` submodule and re-exporting the traits. Tests use an inline `Dual` dummy (no `NoopAdapter` in the kernel):

```rust
pub mod capability;

pub use capability::{ChatModel, EmbedModel, ImageModel, Model, SttModel, TtsModel, VideoModel};

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Capability-segregated registry (see docs/design/adapter-capability-traits.md).
// One `dyn` object cannot be several traits at once, so storage is one map per
// capability. `supports(cap)` is structural: membership in the capability's map.
// ---------------------------------------------------------------------------

/// Registry with one map per capability. The same concrete `Arc` is registered
/// into each map it qualifies for (a concrete `Arc` coerces to each `dyn
/// *Model` independently), so a chat+embed adapter lives in both maps.
#[derive(Clone, Default)]
pub struct AdapterRegistry {
    chat: Arc<RwLock<HashMap<String, Arc<dyn ChatModel>>>>,
    embed: Arc<RwLock<HashMap<String, Arc<dyn EmbedModel>>>>,
    stt: Arc<RwLock<HashMap<String, Arc<dyn SttModel>>>>,
    tts: Arc<RwLock<HashMap<String, Arc<dyn TtsModel>>>>,
    image: Arc<RwLock<HashMap<String, Arc<dyn ImageModel>>>>,
    video: Arc<RwLock<HashMap<String, Arc<dyn VideoModel>>>>,
}

macro_rules! capability_map_accessors {
    ($field:ident, $reg:ident, $get:ident, $trait:ident) => {
        /// Register an adapter under this capability (overwrites same id).
        pub async fn $reg(&self, a: Arc<dyn $trait>) {
            self.$field.write().await.insert(a.id().to_string(), a);
        }
        /// Look up an adapter for this capability by id.
        pub async fn $get(&self, id: &str) -> Option<Arc<dyn $trait>> {
            self.$field.read().await.get(id).cloned()
        }
    };
}

impl AdapterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    capability_map_accessors!(chat, register_chat, chat, ChatModel);
    capability_map_accessors!(embed, register_embed, embed, EmbedModel);
    capability_map_accessors!(stt, register_stt, stt, SttModel);
    capability_map_accessors!(tts, register_tts, tts, TtsModel);
    capability_map_accessors!(image, register_image, image, ImageModel);
    capability_map_accessors!(video, register_video, video, VideoModel);

    /// Register an adapter into every capability map it implements, in one call:
    /// `registry.register(Arc::new(MyAdapter::new()?)).await`. This is the primary
    /// registration entry point — it delegates to the adapter's [`RegisterInto`]
    /// impl, so a chat+embed adapter lands in both maps. Use the per-capability
    /// `register_chat` / `register_embed` / … only when you need finer control.
    pub async fn register<A: RegisterInto + 'static>(&self, adapter: Arc<A>) {
        adapter.register_into(self).await;
    }

    /// Sorted, de-duplicated union of adapter ids across every capability map.
    /// An adapter registered under several capabilities appears once.
    pub async fn list(&self) -> Vec<String> {
        let mut ids: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        ids.extend(self.chat.read().await.keys().cloned());
        ids.extend(self.embed.read().await.keys().cloned());
        ids.extend(self.stt.read().await.keys().cloned());
        ids.extend(self.tts.read().await.keys().cloned());
        ids.extend(self.image.read().await.keys().cloned());
        ids.extend(self.video.read().await.keys().cloned());
        ids.into_iter().collect()
    }
}

/// Lets an adapter insert itself into every capability map it implements.
/// **Custom adapters implement this**; callers usually don't invoke it directly
/// — [`AdapterRegistry::register`] is the ergonomic entry point and delegates here.
#[async_trait]
pub trait RegisterInto: Send + Sync {
    async fn register_into(self: Arc<Self>, reg: &AdapterRegistry);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::config::RouterConfig;
    use crate::types::error::GatewayError;
    use crate::types::io::{ChatRequest, ChatResponse, EmbedRequest, EmbedResponse};

    struct Dual;
    impl Model for Dual {
        fn id(&self) -> &str {
            "dual"
        }
    }
    #[async_trait]
    impl ChatModel for Dual {
        async fn chat(&self, _c: &RouterConfig, _r: &ChatRequest) -> Result<ChatResponse, GatewayError> {
            Ok(ChatResponse::default())
        }
    }
    #[async_trait]
    impl EmbedModel for Dual {
        async fn embed(&self, _c: &RouterConfig, _r: &EmbedRequest) -> Result<EmbedResponse, GatewayError> {
            Ok(EmbedResponse::default())
        }
    }
    #[async_trait]
    impl RegisterInto for Dual {
        async fn register_into(self: Arc<Self>, reg: &AdapterRegistry) {
            reg.register_chat(self.clone()).await;
            reg.register_embed(self).await;
        }
    }

    #[tokio::test]
    async fn same_adapter_registers_into_multiple_capability_maps() {
        // Explicit per-capability registration: same Arc into both maps.
        let reg = AdapterRegistry::new();
        let dual = Arc::new(Dual);
        reg.register_chat(dual.clone()).await;
        reg.register_embed(dual).await;
        assert!(reg.chat("dual").await.is_some());
        assert!(reg.embed("dual").await.is_some());
        assert!(reg.image("dual").await.is_none());

        // One-call RegisterInto lands the adapter in exactly its maps.
        let reg2 = AdapterRegistry::new();
        Arc::new(Dual).register_into(&reg2).await;
        assert!(reg2.chat("dual").await.is_some());
        assert!(reg2.embed("dual").await.is_some());
        assert!(reg2.stt("dual").await.is_none());
    }

    #[tokio::test]
    async fn registry_lists_by_capability() {
        let reg = AdapterRegistry::new();
        reg.register(Arc::new(Dual)).await;
        assert!(reg.chat("dual").await.is_some());
        assert!(reg.chat("nonexistent").await.is_none());
        assert_eq!(reg.list().await, vec!["dual".to_string()]);
    }
}
```

Note: `tokio::test` requires the `macros` + `rt` runtimes. Add them as a dev-dependency in the next step.

- [ ] **Step 3: Add the kernel's test runtime.** Append to `crates/kernel/Cargo.toml`:

```toml
[dev-dependencies]
tokio = { version = "1", features = ["macros", "rt-multi-thread", "sync"] }
```

- [ ] **Step 4: Declare `adapters` in the kernel and re-export its surface.** Update `crates/kernel/src/lib.rs`:

```rust
//! `sensei-kernel` — the shared vocabulary of the sensei gateway: config, IO,
//! cost, trace, and error types, the capability traits, and the adapter
//! registry. This crate depends on nothing else in the workspace; every other
//! gateway crate depends on it.

pub mod adapters;
pub mod types;

pub use adapters::capability::{
    ChatModel, EmbedModel, ImageModel, Model, SttModel, TtsModel, VideoModel,
};
pub use adapters::{AdapterRegistry, RegisterInto};
pub use types::capability::Capability;
pub use types::error::GatewayError;
pub use types::request::{InferenceRequest, InferenceResponse};
```

- [ ] **Step 5: Verify the kernel builds + tests pass.**

Run: `cargo test -p sensei-kernel`
Expected: PASS — `same_adapter_registers_into_multiple_capability_maps` and `registry_lists_by_capability` run in the kernel.

- [ ] **Step 6: Replace `gateway`'s `adapters/mod.rs` registry/traits with re-exports.** Edit `crates/gateway/src/adapters/mod.rs`: **keep** the `pub mod anthropic; … pub mod together;` cloud-adapter submodule declarations, **delete** the old `pub use capability::{…}` line, the `use std::…` / `use async_trait::…` / `use tokio::sync::RwLock;` imports, the `AdapterRegistry` struct, the `capability_map_accessors!` macro, the `impl AdapterRegistry`, the `RegisterInto` trait, and the old `#[cfg(test)] mod tests` (its coverage is re-added below). The top of the file becomes:

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
pub mod noop;
pub mod ollama;
pub mod openai;
pub mod openai_compat;
pub mod recraft;
pub mod replicate;
pub mod runway;
pub mod stability;
pub mod together;

// The capability traits + registry now live in `kernel`. Re-export them under
// their historical `gateway::adapters::…` paths so both this crate's adapters
// (`crate::adapters::…`) and downstream consumers compile unchanged.
pub use kernel::adapters::capability;
pub use kernel::adapters::capability::{
    ChatModel, EmbedModel, ImageModel, Model, SttModel, TtsModel, VideoModel,
};
pub use kernel::adapters::{AdapterRegistry, RegisterInto};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::noop::NoopAdapter;
    use std::sync::Arc;

    #[tokio::test]
    async fn registry_registers_and_lists_via_reexport() {
        // Exercises the re-exported registry with a real gateway adapter, so the
        // shim (not just the kernel copy) is covered.
        let reg = AdapterRegistry::new();
        reg.register(Arc::new(NoopAdapter)).await;
        assert!(reg.chat("noop").await.is_some());
        assert!(reg.chat("nonexistent").await.is_none());
        assert_eq!(reg.list().await, vec!["noop".to_string()]);
    }
}
```

(The `base` submodule stays; if `base` was not in the original module list, keep the original set exactly and only remove `pub mod capability;`. Verify the submodule list matches the files present in `crates/gateway/src/adapters/`.)

- [ ] **Step 7: Verify `gateway` builds + tests pass.**

Run: `cargo build -p gateway && cargo test -p gateway && cargo clippy -p gateway -- -D warnings`
Expected: PASS — every `crate::adapters::AdapterRegistry`, `crate::adapters::RegisterInto`, `crate::adapters::capability::…`, and `crate::adapters::{ChatModel,…}` in the cloud adapters resolves through the re-exports.

- [ ] **Step 8: Commit.**

```bash
git add crates/kernel crates/gateway/src/adapters/mod.rs crates/gateway/Cargo.toml
git commit -m "refactor(kernel): move capability traits + AdapterRegistry into sensei-kernel"
```

---

## Task 4: Lock the downstream re-export surface with a path-smoke test

**Files:**
- Create: `crates/gateway/tests/reexport_paths.rs`

- [ ] **Step 1: Write a compile-time smoke test** that names every `gateway::…` path `gateway-embedded` and other downstream consumers rely on, so a future accidental removal of a shim fails CI.

```rust
//! Compile-time guard: every `gateway::…` path that downstream consumers (and
//! `gateway-embedded`) depend on must keep resolving after the kernel split.
//! Compiling this file IS the assertion.
#![allow(unused_imports)]

use gateway::adapters::capability::{
    ChatModel, EmbedModel, ImageModel, Model, SttModel, TtsModel, VideoModel,
};
use gateway::adapters::{AdapterRegistry, RegisterInto};
use gateway::types::config::RouterConfig;
use gateway::types::error::GatewayError;
use gateway::types::io::{ChatRequest, ChatResponse, EmbedRequest, EmbedResponse};
use gateway::types::request::{Message, MessageRole, StreamChunk};
use gateway::{Capability, InferenceRequest, InferenceResponse};

#[test]
fn reexport_paths_resolve() {
    // The `use` block above proves the paths resolve; nothing to assert at runtime.
}
```

- [ ] **Step 2: Run the smoke test.**

Run: `cargo test -p gateway --test reexport_paths`
Expected: PASS (compiles + the trivial test passes).

- [ ] **Step 3: Commit.**

```bash
git add crates/gateway/tests/reexport_paths.rs
git commit -m "test(gateway): lock kernel re-export paths with a compile-time guard"
```

---

## Task 5: Re-point `gateway-embedded` at the kernel (drop the `gateway` dep)

**Files:**
- Modify: `crates/gateway-embedded/Cargo.toml`
- Modify: `crates/gateway-embedded/src/adapters/embedded_llama.rs`
- Modify: `crates/gateway-embedded/src/adapters/llama_cpp.rs`
- Modify: `crates/gateway-embedded/src/adapters/fastembed.rs`
- Modify: `crates/gateway-embedded/src/adapters/ort.rs`

- [ ] **Step 1: Swap the dependency.** In `crates/gateway-embedded/Cargo.toml`, replace `gateway = { path = "../gateway" }` with:

```toml
kernel = { package = "sensei-kernel", path = "../kernel" }
```

(Leave every other dependency and the `[features]` block unchanged.)

- [ ] **Step 2: Re-point the imports.** In the four adapter files, replace the `gateway::` path prefix with `kernel::` for every distinct path the recon found. The exact substitutions (all four files use a subset of these):

```
gateway::adapters::AdapterRegistry          → kernel::adapters::AdapterRegistry
gateway::adapters::RegisterInto             → kernel::adapters::RegisterInto
gateway::adapters::capability::Model        → kernel::adapters::capability::Model
gateway::adapters::capability::ChatModel    → kernel::adapters::capability::ChatModel
gateway::adapters::capability::EmbedModel   → kernel::adapters::capability::EmbedModel
gateway::types::config::RouterConfig        → kernel::types::config::RouterConfig
gateway::types::error::GatewayError         → kernel::types::error::GatewayError
gateway::types::io::ChatRequest             → kernel::types::io::ChatRequest
gateway::types::io::ChatResponse            → kernel::types::io::ChatResponse
gateway::types::io::EmbedRequest            → kernel::types::io::EmbedRequest
gateway::types::io::EmbedResponse           → kernel::types::io::EmbedResponse
gateway::types::request::Message            → kernel::types::request::Message
gateway::types::request::MessageRole        → kernel::types::request::MessageRole
gateway::types::request::StreamChunk        → kernel::types::request::StreamChunk
```

A safe mechanical pass (review the diff afterward — no other crate is named `gateway` inside these files):

```bash
sed -i '' 's/\bgateway::/kernel::/g' \
  crates/gateway-embedded/src/adapters/embedded_llama.rs \
  crates/gateway-embedded/src/adapters/llama_cpp.rs \
  crates/gateway-embedded/src/adapters/fastembed.rs \
  crates/gateway-embedded/src/adapters/ort.rs
git diff --stat crates/gateway-embedded/src/adapters/
```

- [ ] **Step 3: Verify the default (no-engine-feature) build + tests.**

Run: `cargo build -p gateway-embedded && cargo test -p gateway-embedded && cargo clippy -p gateway-embedded -- -D warnings`
Expected: PASS. (Default build has no engine adapters compiled; the registry/resolver/math code compiles against `kernel`.)

- [ ] **Step 4: Compile-check the feature-gated adapter bodies** (these carry the swapped imports; they only compile with the native toolchain present). If a C/C++ toolchain + the ONNX runtime are available:

Run: `cargo build -p gateway-embedded --features llama-cpp,fastembed,ort`
Expected: compiles clean. If the toolchain is unavailable in this environment, record that a reviewer with it must run this command before merge (do not mark the task done otherwise).

- [ ] **Step 5: Confirm the AWS SDK is gone from `gateway-embedded`.**

Run: `cargo tree -p gateway-embedded | grep -i 'aws-' || echo "no aws deps"`
Expected: `no aws deps` (the whole point — the embedded crate no longer pulls the Bedrock SDK transitively).

- [ ] **Step 6: Commit.**

```bash
git add crates/gateway-embedded/Cargo.toml crates/gateway-embedded/src/adapters
git commit -m "refactor(embedded): depend on sensei-kernel directly; drop the gateway dep"
```

---

## Task 6: Workspace-wide verification

**Files:**
- Modify (if needed): `Makefile`, `README.md`

- [ ] **Step 1: Full workspace build + test + lint.**

Run: `cargo build --workspace && cargo test --workspace && cargo clippy --workspace -- -D warnings`
Expected: all three PASS across `sensei-kernel`, `gateway`, `gateway-embedded`.

- [ ] **Step 2: Check the `Makefile` for crate enumerations.** Search for hardcoded crate lists (build/test/tag/bump targets):

Run: `grep -nE 'gateway-embedded|-p gateway|crates/' Makefile`
If any target enumerates the crates (e.g. a `bump` or per-crate publish/tag target), add `sensei-kernel` / `crates/kernel` alongside `gateway` and `gateway-embedded`. Apply the edit to match the existing pattern in that target.

- [ ] **Step 3: Update `README.md` crate references.** If the README lists the workspace crates or their versions, add `sensei-kernel` (imported as `kernel`) and note it as the shared-types foundation. Match the README's existing wording.

- [ ] **Step 4: Re-run the full verification after any Makefile/README edits.**

Run: `cargo build --workspace && cargo test --workspace`
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add -A
git commit -m "chore: wire sensei-kernel into workspace tooling + docs"
```

---

## Task 7 (optional — may be split into its own PR): rename the `gateway` package to `sensei-gateway`

Do this only if you want the public crate coordinates prefixed now. It changes `gateway`'s package name (downstream must add a `package =` alias at their next tag bump). `gateway-embedded`'s rename is deferred to PR 3, where it is split into `sensei-local-providers` / `sensei-local-engine`.

**Files:**
- Modify: `crates/gateway/Cargo.toml`

- [ ] **Step 1: Rename the package, keep the import name.** In `crates/gateway/Cargo.toml`:

```toml
[package]
name = "sensei-gateway"
version = "0.3.1"
edition = "2024"
# … keep description / license …

[lib]
name = "gateway"
```

`[lib] name = "gateway"` keeps the crate importable/doctestable as `gateway` within this repo.

- [ ] **Step 2: Verify (note the new `-p` target).**

Run: `cargo build -p sensei-gateway && cargo test -p sensei-gateway && cargo clippy -p sensei-gateway -- -D warnings`
Expected: PASS, including the `reexport_paths` doctest/integration test (still uses `use gateway::…` via the `[lib] name`).

- [ ] **Step 3: Note the downstream migration** in the PR description: senseid must change its dependency to `gateway = { package = "sensei-gateway", git = "…", tag = "…" }` (code stays `use gateway::…`). Do not edit senseid here.

- [ ] **Step 4: Commit.**

```bash
git add crates/gateway/Cargo.toml
git commit -m "chore(gateway): publish as sensei-gateway (import name unchanged)"
```

---

## Self-review checklist (run before handing off)

- [ ] **Spec coverage:** §9 step 1 (extract kernel) is fully covered by Tasks 1–6; the `sensei-` naming decision (§2) is applied to the new crate (Task 1) and optionally to `gateway` (Task 7); the model-management vocabulary deferral is documented in Conventions.
- [ ] **No placeholders:** every code step shows complete code; every run step shows the exact command + expected result.
- [ ] **Type/name consistency:** the crate is `sensei-kernel` / imported `kernel` throughout; re-export lists in `kernel/src/lib.rs`, `gateway/src/adapters/mod.rs`, and `crates/gateway/tests/reexport_paths.rs` name the same seven capability traits + `AdapterRegistry`/`RegisterInto`.
- [ ] **Green at every boundary:** Tasks 2, 3, 5, 6 each end with a build+test(+clippy) gate for the package they touch.
