# Local Split Implementation Plan (PR 3 of the workspace re-layering)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Split `gateway-embedded` into two single-purpose crates — `local-providers` (published `sensei-local-providers`, import `local_providers`: the in-process inference adapters + `math`) and `local-engine` (published `sensei-local-engine`, import `local_engine`: the model resolvers + HF pull) — after lifting the shared model-registry vocabulary into `kernel`. Then **retire `gateway-embedded`**. This completes the symmetric layering: `cloud-providers ∥ local-providers`, driven by the routing engine (`gateway`) and (in PR 4) the local engine.

**Architecture:** Behaviour-preserving relocation. The model-registry vocabulary (`ModelFormat`/`ModelSource`/`ModelEntry`/`ResolveError`/`ModelResolver`) moves into a new `kernel::registry` module so both new crates share it via `kernel`. The adapters (`llama_cpp`/`embedded_llama`/`fastembed`/`ort`) + `math` move to `local-providers`; the concrete resolvers (`managed`/`ollama`/`external`/`ChainedResolver`) + `pull` move to `local-engine`. Both new crates depend only on `kernel` (the supervisor that couples them is PR 4). `gateway-embedded` is deleted. Subdir structure (`adapters/`, `registry/`) is preserved so the only path rewrite is `crate::registry::` → `kernel::registry::`. Green per-crate at each task; `gateway-embedded` is dropped from the workspace as soon as its files start moving, so `cargo build --workspace` stays valid.

**Tech Stack:** Rust edition 2024, cargo workspace at `/Users/Jerry/Developer/strategos/gateway`. `sensei-` package prefix, short import names via `[lib] name` + `package =` aliases. Native toolchain present (engine features build here).

**Reference spec:** `docs/design/provisioning-supervisor-and-workspace-layering.md` (§2, §9 step 3).

---

## Conventions for every task

- Paths relative to repo root. Run `cargo`/`git` from there. Branch `refactor/local-split` (created off `develop`). Do NOT create/switch branches.
- Commit ONLY intended files by explicit path (never `git add -A`; never touch `site/`). Do NOT push or tag — the controller tags after each task and lands the branch.
- SAFETY: at each task start, verify `git status` clean + HEAD is the expected SHA; if not, STOP + report BLOCKED.
- `[lib] name` maps: `sensei-kernel`→`kernel`, `sensei-cloud-providers`→`cloud_providers`, `sensei-gateway`→`gateway`, `sensei-local-providers`→`local_providers`, `sensei-local-engine`→`local_engine`.
- "Green" for package `P`: `cargo build/test/clippy -p P -- -D warnings` pass.

---

## Task 1: Lift the model-registry vocabulary into `kernel`

**Files:** create `crates/kernel/src/registry.rs`; modify `crates/kernel/src/lib.rs`.

- [ ] **Step 1: Create `crates/kernel/src/registry.rs`** containing, copied verbatim from `crates/gateway-embedded/src/registry/mod.rs`, the vocabulary ONLY: `ModelFormat`, `ModelSource` (+ its `impl` with `path()`), `ModelEntry`, `ResolveError`, and the `ModelResolver` trait — plus the vocab-only tests (`model_source_path_returns_the_bytes_path_for_each_variant`, `model_entry_roundtrips_through_json_preserving_source_kind`, `model_source_serializes_kind_as_external_tag`, `ollama_source_carries_manifest_digest_and_blob_path`). Do NOT copy `ChainedResolver` or the `Chained*`/`empty_chain` tests (those use `ExternalResolver` and stay in `local-engine`). Required imports at top: `use serde::{Deserialize, Serialize}; use std::path::{Path, PathBuf};` (drop `HashSet`/`Arc` — only `ChainedResolver` used those).
  - **CHANGE:** the `ResolveError::Pull` variant is currently `#[cfg(feature = "hf-download")]`. `kernel` has no such feature, so **remove that `#[cfg(...)]` attribute** — the variant becomes always-present:
    ```rust
    /// A model pull (download or resource pre-flight) failed while resolving
    /// through a pulling resolver. Carries the actionable message.
    #[error("pull: {0}")]
    Pull(String),
    ```

- [ ] **Step 2: Declare + re-export in `crates/kernel/src/lib.rs`.** Add `pub mod registry;` and a convenience re-export:
```rust
pub use registry::{ModelEntry, ModelFormat, ModelResolver, ModelSource, ResolveError};
```
(Place alongside the existing module declarations / re-exports.)

- [ ] **Step 3: Verify.** `cargo test -p sensei-kernel` — the moved vocab tests pass under `kernel`. `cargo build --workspace` still green (`gateway-embedded` is untouched and still compiles with its own copy of the vocab — a temporary duplicate that disappears when `gateway-embedded` is deleted in Task 4).

- [ ] **Step 4: Commit.**
```bash
git add crates/kernel/src/registry.rs crates/kernel/src/lib.rs
git commit -m "feat(kernel): lift model-registry vocabulary (ModelEntry/ModelResolver/…) into kernel

Un-gates ResolveError::Pull (kernel has no hf-download feature). Shared by the
local-providers + local-engine crates (PR 3 of the workspace re-layering).

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Create `local-providers` (adapters + math)

**Files:** create `crates/local-providers/Cargo.toml` + `src/lib.rs`; `git mv` the adapters + `math`; modify root `Cargo.toml`.

- [ ] **Step 1: Root `Cargo.toml` members** — remove `crates/gateway-embedded`, add `crates/local-providers` (and, anticipating Task 3, you may add `crates/local-engine` now or in Task 3):
```toml
members = ["crates/kernel", "crates/cloud-providers", "crates/gateway", "crates/local-providers"]
```
Removing `gateway-embedded` from the members now means the (soon-gutted) crate is not built by `--workspace` — intentional; it's deleted in Task 4.

- [ ] **Step 2: `crates/local-providers/Cargo.toml`** (best-effort deps; Task-2 compile prunes/adds):
```toml
[package]
name = "sensei-local-providers"
version = "0.3.1"
edition = "2024"
description = "In-process inference adapters (llama.cpp, fastembed, ONNX Runtime) for the sensei gateway"
license = "MIT"

[lib]
name = "local_providers"

[features]
default = []
llama-cpp = ["dep:llama-cpp-2"]
fastembed = ["dep:fastembed"]
ort = ["dep:ort", "dep:tokenizers", "dep:ndarray"]

[dependencies]
kernel = { package = "sensei-kernel", path = "../kernel" }
async-trait = "0.1"
futures = "0.3"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
tokio = { version = "1", features = ["fs", "rt", "sync"] }
tokio-stream = "0.1"
tracing = "0.1"
llama-cpp-2 = { version = "0.1.146", default-features = false, optional = true }
fastembed = { version = "5", optional = true }
ort = { version = "2.0.0-rc.12", features = ["download-binaries", "ndarray"], optional = true }
tokenizers = { version = "0.21", optional = true }
ndarray = { version = "0.16", optional = true }

[dev-dependencies]
tempfile = "3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

- [ ] **Step 3: Move the files** (preserve subdir so intra-adapter paths like `super::llama_cpp` / `crate::math` stay valid):
```bash
mkdir -p crates/local-providers/src
git mv crates/gateway-embedded/src/adapters crates/local-providers/src/adapters
git mv crates/gateway-embedded/src/math.rs  crates/local-providers/src/math.rs
```

- [ ] **Step 4: `crates/local-providers/src/lib.rs`:**
```rust
//! `sensei-local-providers` — in-process inference adapters (llama.cpp,
//! fastembed, ONNX Runtime). Each implements the `kernel` capability traits and
//! loads a `kernel::registry::ModelEntry`; construction is caller-driven.
pub mod adapters;
pub mod math;
```

- [ ] **Step 5: Re-point registry-vocab paths** in the moved adapter files (the ONLY expected rewrite — the capability traits already use `kernel::` from PR 1):
```bash
cd crates/local-providers/src
sed -i '' -E 's/([^a-zA-Z0-9_]|^)crate::registry::/\1kernel::registry::/g' adapters/*.rs
cd -
grep -rn 'crate::registry::' crates/local-providers/src/ || echo "no stray crate::registry refs"
```
Resolve anything the compiler still flags (e.g. a `use crate::registry::{ModelEntry, ModelResolver};` → `use kernel::registry::{…}`). `math` is self-contained (`crate::math` stays valid).

- [ ] **Step 6: Build to green** (default + each engine feature, native toolchain present):
```
cargo build -p sensei-local-providers
cargo test  -p sensei-local-providers
cargo clippy -p sensei-local-providers -- -D warnings
cargo build -p sensei-local-providers --features llama-cpp,fastembed,ort
cargo clippy -p sensei-local-providers --features llama-cpp,fastembed,ort -- -D warnings
```
Add any dep the compiler names; prune trivially-unused. Also confirm `cargo build --workspace` is green (gateway-embedded no longer a member).

- [ ] **Step 7: Commit** (moved files + new crate + root Cargo.toml):
```bash
git add crates/local-providers Cargo.toml
git commit -m "refactor(local-providers): extract in-process adapters + math from gateway-embedded

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Create `local-engine` (resolvers + pull)

**Files:** create `crates/local-engine/Cargo.toml` + `src/lib.rs`; `git mv` the `registry/` resolvers; modify root `Cargo.toml`; edit the moved `registry/mod.rs`.

- [ ] **Step 1: Root `Cargo.toml` members** — add `crates/local-engine`:
```toml
members = ["crates/kernel", "crates/cloud-providers", "crates/gateway", "crates/local-providers", "crates/local-engine"]
```

- [ ] **Step 2: `crates/local-engine/Cargo.toml`** (best-effort; compile prunes/adds):
```toml
[package]
name = "sensei-local-engine"
version = "0.3.1"
edition = "2024"
description = "Model resolvers (managed / Ollama / external) + Hugging Face pull for the sensei gateway"
license = "MIT"

[lib]
name = "local_engine"

[features]
default = []
hf-download = ["dep:hf-hub", "dep:reqwest", "dep:sysinfo"]

[dependencies]
kernel = { package = "sensei-kernel", path = "../kernel" }
async-trait = "0.1"
futures = "0.3"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
thiserror = "2"
tokio = { version = "1", features = ["fs", "rt", "sync"] }
tracing = "0.1"
hf-hub = { version = "0.4", default-features = false, features = ["tokio", "rustls-tls"], optional = true }
reqwest = { version = "0.12", default-features = false, features = ["rustls-tls"], optional = true }
sysinfo = { version = "0.39", default-features = false, features = ["disk", "system"], optional = true }

[dev-dependencies]
tempfile = "3"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

- [ ] **Step 3: Move the resolver files:**
```bash
mkdir -p crates/local-engine/src
git mv crates/gateway-embedded/src/registry crates/local-engine/src/registry
```

- [ ] **Step 4: `crates/local-engine/src/lib.rs`:**
```rust
//! `sensei-local-engine` — the local model engine: resolvers that map a stable
//! model id to on-disk bytes (managed / Ollama / external, composed via
//! `ChainedResolver`) plus Hugging Face pull (`hf-download`). Model vocabulary
//! (`ModelEntry`/`ModelResolver`/…) lives in `kernel::registry`.
pub mod registry;
```

- [ ] **Step 5: Edit the moved `crates/local-engine/src/registry/mod.rs`** — it currently DEFINES the vocab (now in `kernel`). Replace the vocab definitions (`ModelFormat`, `ModelSource`+impl, `ModelEntry`, `ResolveError`, `ModelResolver` trait) and their tests with a re-export from `kernel`, KEEPING `ChainedResolver`, its impl, the resolver submodule declarations/re-exports, and the `ChainedResolver`/`empty_chain` tests. The top becomes:
```rust
//! Model registry resolvers … (keep the existing module doc)
pub mod external;
pub mod managed;
pub mod ollama;
#[cfg(feature = "hf-download")]
pub mod pull;

pub use external::ExternalResolver;
pub use managed::ManagedResolver;
pub use ollama::OllamaResolver;
#[cfg(feature = "hf-download")]
pub use pull::{FitReport, HfHubPuller, ModelPuller, PullError, PullSpec, PullingResolver};

// Vocabulary lives in the kernel; re-export so `super::ModelEntry` etc. in the
// resolver submodules keep resolving, and downstream keeps its paths.
pub use kernel::registry::{ModelEntry, ModelFormat, ModelResolver, ModelSource, ResolveError};

use std::collections::HashSet;
use std::sync::Arc;

// … keep `ChainedResolver` struct + impl + its #[cfg(test)] tests verbatim …
```
The resolver submodules (`external.rs`/`managed.rs`/`ollama.rs`/`pull.rs`) reference `super::{ModelEntry, ModelResolver, ResolveError, …}` — these resolve via the re-export, so **no edits** to those files are expected. `pull.rs` constructs `ResolveError::Pull(…)` (now un-gated in kernel) — still valid. If any submodule used `crate::registry::` explicitly, rewrite to `super::` (which resolves via the re-export) or `kernel::registry::`.

- [ ] **Step 6: Build to green** (default + `hf-download`):
```
cargo build -p sensei-local-engine
cargo test  -p sensei-local-engine
cargo clippy -p sensei-local-engine -- -D warnings
cargo build -p sensei-local-engine --features hf-download
cargo test  -p sensei-local-engine --features hf-download   # excludes #[ignore] network tests
cargo clippy -p sensei-local-engine --features hf-download -- -D warnings
```
Add/prune deps as the compiler dictates. `grep -rn 'crate::registry::' crates/local-engine/src/ || echo clean`.

- [ ] **Step 7: Commit:**
```bash
git add crates/local-engine Cargo.toml
git commit -m "refactor(local-engine): extract resolvers + HF pull from gateway-embedded

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Delete `gateway-embedded` + tooling + workspace verify

**Files:** delete `crates/gateway-embedded/`; modify `Makefile`, `README.md`.

- [ ] **Step 1: Confirm nothing in the workspace references it** (should be only the now-empty dir):
```bash
grep -rn 'gateway-embedded\|gateway_embedded' crates/ Cargo.toml Makefile | grep -v 'crates/gateway-embedded/' || echo "no workspace refs"
```
Expect `no workspace refs` (senseid is external and out of scope). If any workspace file still references it, STOP and report.

- [ ] **Step 2: Delete the crate.**
```bash
git rm -r crates/gateway-embedded
```
(It should contain only the gutted `Cargo.toml` + `src/lib.rs` + empty `src/adapters`/`src/registry` dirs after Tasks 2–3.)

- [ ] **Step 3: Makefile.** In the `bump` target, replace `crates/gateway-embedded/Cargo.toml` with BOTH `crates/local-providers/Cargo.toml` and `crates/local-engine/Cargo.toml` (in the version-sed lines AND the `git add`). Update the "four crates" comments to "five crates". Show `grep -nE 'crates/.*Cargo.toml|crates share one version' Makefile` after.

- [ ] **Step 4: README.** Replace the `gateway-embedded` crates-table row with two rows: `local-providers` (`sensei-local-providers`) — in-process inference adapters (llama.cpp/fastembed/ort); and `local-engine` (`sensei-local-engine`) — model resolvers + HF pull. Update the crate count ("four"→"five") in the versioning line. Match the table format. (senseid migration note: it must switch its `gateway-embedded` dep to `local-providers` + `local-engine`.)

- [ ] **Step 5: Full workspace gate.**
```
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo build -p sensei-local-providers --features llama-cpp,fastembed,ort
cargo build -p sensei-local-engine --features hf-download
```
All green. Confirm `gateway-embedded` is gone: `test ! -d crates/gateway-embedded && echo "removed"`.

- [ ] **Step 6: Commit.**
```bash
git add -u && git add Makefile README.md
git commit -m "chore: retire gateway-embedded; wire local-providers + local-engine into tooling/docs

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```
(`git add -u` here stages the `git rm` deletions; still no blanket `git add -A`, and `site/` is untouched. Verify `git status` shows only the intended paths before committing.)

---

## Self-review checklist (controller, before landing)

- [ ] **Spec coverage:** §9 step 3 — vocab lifted to `kernel`; `gateway-embedded` split into `local-providers` + `local-engine`; `gateway-embedded` retired. Dependency graph acyclic (both new crates → `kernel` only).
- [ ] **Behaviour preservation:** moved files are renames; the only source rewrite is `crate::registry::`→`kernel::registry::` + the `registry/mod.rs` vocab→re-export swap + the un-gated `ResolveError::Pull`. Full suite (incl. engine + hf-download features) green.
- [ ] **No placeholders / dangling refs:** grep confirms no stray `crate::registry::` and no workspace `gateway-embedded` refs.
- [ ] **Dep hygiene:** each new crate's deps are the used set (prune unused).
- [ ] Whole-branch code review, then merge to `develop`.

## Note

Downstream (senseid) currently pins `gateway-embedded`; after this PR it must depend on `local-providers` + `local-engine` (or wait for PR 4's gateway facade to re-export the local engine under a `local` feature). Out of scope here.
