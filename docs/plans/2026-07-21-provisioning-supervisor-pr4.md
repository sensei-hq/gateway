# Provisioning Supervisor + Chain Pruning Implementation Plan (PR 4 of the workspace re-layering)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Deliver gh issue #4 — a library-owned async `ProvisioningSupervisor` that resolves/pulls/coldboots/registers local models in the background and streams progress, plus chain-viability pruning with a warning report, plus a `ModelNotReady` degradation signal — on top of the five-crate layout that PR 1–3 established.

**Architecture:** Behaviour-additive. The `kernel` gains the shared **readiness vocabulary + port** (`ProvisionPhase`/`ProvisionEvent`/`ReadinessProbe`) and a terminal `GatewayError::ModelNotReady`. `local-engine` gains the `ProvisioningSupervisor` (a `watch`-channel readiness state machine + progress-emitting pull + coldboot/registration) and `impl ReadinessProbe`. `gateway` gains pure-config chain pruning, an optional `with_readiness(probe)` seam (mirroring `with_store`), a terminal `ModelNotReady` consultation at exhaustion, and the batteries-included facade builder. The two engines never depend on each other — they meet only through the kernel `ReadinessProbe` trait object (compile-time dep points to `kernel`; runtime call is `dyn` dispatch). Green per-crate at each task; `cargo build --workspace` stays valid after every task.

**Tech Stack:** Rust edition 2024, cargo workspace at `/Users/Jerry/Developer/strategos/gateway`. `tokio` (`sync::watch`, `Semaphore`, `spawn`), `tokio-stream` (`WatchStream`), `async-trait`, `futures`. `sensei-` package prefix, short import names via `[lib] name` + `package =` aliases.

**Reference spec:** `docs/design/provisioning-supervisor-and-workspace-layering.md` — §1 (goal), §3 (`ProvisioningSupervisor`), §4 (pruning), §5 (readiness port + seam), §6 (degradation + `ModelNotReady`), §7 (testing), §8 (public API), §9 step 4. Read it before starting; this plan operationalises it and is authoritative on task/step ordering, but the spec is authoritative on design rationale and exact type shapes.

---

## Conventions for every task

- Paths relative to repo root. Run `cargo`/`git` from there. Branch off `develop` (do NOT commit PR-4 work directly to `develop`).
- Commit ONLY intended files by explicit path (never `git add -A`; never touch `site/`). Do NOT push or tag.
- SAFETY: at each task start, verify `git status` clean + HEAD is the expected SHA; if not, STOP + report BLOCKED.
- `[lib] name` maps: `sensei-kernel`→`kernel`, `sensei-cloud-providers`→`cloud_providers`, `sensei-gateway`→`gateway`, `sensei-local-providers`→`local_providers`, `sensei-local-engine`→`local_engine`.
- "Green" for package `P`: `cargo build/test/clippy -p P -- -D warnings` pass. TDD: write the failing test first, watch it fail, implement, watch it pass, commit.
- Ground-truth anchors (verified 2026-07-21): `Gateway::with_store` at `crates/gateway/src/engine.rs:55`; `execute` at `:160`; terminal `AllAttemptsFailed` construction at `engine.rs:482`; stream error-code map (`AllAttemptsFailed => "all_attempts_failed"`) at `engine.rs:883`. `AdapterRegistry` is `Clone` over `Arc<RwLock<..>>` at `crates/gateway/src/adapters/mod.rs:40`; late per-request lookup at `engine.rs:244`; `register_*` at `adapters/mod.rs:53`/`:80`. `GatewayError` lives in `kernel` (moved in PR 1) and its fallback classifier match has **no `_` arm** — adding a variant surfaces every match site at compile time.

---

## File structure

**`crates/kernel/`**
- Create `src/readiness.rs` — `ProvisionPhase` (+ `is_in_flight`, serde), `ProvisionEvent`, `ReadinessProbe` trait. One responsibility: the readiness vocabulary + port.
- Modify `src/lib.rs` — `pub mod readiness;` + convenience re-exports.
- Modify `src/types/error.rs` (wherever `GatewayError` is defined in kernel) — add the `ModelNotReady` variant (Task 4).

**`crates/gateway/`**
- Create `src/pruning.rs` — `Availability`, `ChainWarning`, `prune_unavailable` free fn + `Gateway::prune_unavailable` method. Pure config logic, no I/O.
- Modify `src/engine.rs` — `with_readiness`, the optional `probe` field, the terminal `ModelNotReady` consultation, the stream error-code arm.
- Modify the fallback-trigger classifier (the `should_trigger_fallback`-equivalent match) + `src/lib.rs` re-exports.
- Create `src/facade.rs` (or extend the builder module) — the batteries-included builder (Task 5), feature-gated `cloud`/`local`.

**`crates/local-engine/`**
- Create `src/supervisor.rs` — `ProvisioningSupervisor`, `ModelSlot`, `EnsureOpts`, `ProvisionHandle`, `ProvisionError`, `ProvisionPlan`, `impl ReadinessProbe`. The readiness state machine + streaming + dedup + coldboot orchestration.
- Modify `src/registry/pull.rs` — add `pull_with_progress(spec, &mut dyn FnMut(u64, Option<u64>))` alongside `pull` (feature `hf-download`).
- Modify `src/lib.rs` — `pub mod supervisor;` + re-exports.
- Modify `Cargo.toml` — add `tokio-stream`, `local-providers` dep (already a dev-dep from PR3; promote to a normal dep for coldboot), and confirm feature forwarding.

---

## Task 1: Readiness vocabulary + port in `kernel`

**Files:** create `crates/kernel/src/readiness.rs`; modify `crates/kernel/src/lib.rs`. Pure additions — nothing else in the workspace references these yet, so the workspace stays green.

- [ ] **Step 1: Write the failing tests** in `crates/kernel/src/readiness.rs` (`#[cfg(test)]`):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_in_flight_is_true_for_queued_download_verify_load_only() {
        assert!(ProvisionPhase::Queued.is_in_flight());
        assert!(ProvisionPhase::Downloading { done: 1, total: Some(10) }.is_in_flight());
        assert!(ProvisionPhase::Verifying.is_in_flight());
        assert!(ProvisionPhase::Loading.is_in_flight());
        assert!(!ProvisionPhase::Absent.is_in_flight());
        assert!(!ProvisionPhase::Ready.is_in_flight());
        assert!(!ProvisionPhase::Failed { error: "x".into() }.is_in_flight());
    }

    #[test]
    fn phase_roundtrips_through_json() {
        let p = ProvisionPhase::Downloading { done: 5, total: Some(100) };
        let json = serde_json::to_string(&p).unwrap();
        assert_eq!(serde_json::from_str::<ProvisionPhase>(&json).unwrap(), p);
    }

    struct FakeProbe;
    #[async_trait::async_trait]
    impl ReadinessProbe for FakeProbe {
        async fn phase(&self, _m: &str) -> ProvisionPhase { ProvisionPhase::Ready }
        async fn status_all(&self) -> Vec<(String, ProvisionPhase)> { vec![] }
    }

    #[tokio::test]
    async fn readiness_probe_is_object_safe_and_callable_via_dyn() {
        let probe: std::sync::Arc<dyn ReadinessProbe> = std::sync::Arc::new(FakeProbe);
        assert_eq!(probe.phase("m").await, ProvisionPhase::Ready);
    }
}
```

- [ ] **Step 2: Run to verify they fail.** `cargo test -p sensei-kernel readiness` → FAIL (`ProvisionPhase` undefined).

- [ ] **Step 3: Implement `crates/kernel/src/readiness.rs`** (types verbatim from spec §3/§5):

```rust
//! Readiness vocabulary + the `ReadinessProbe` port. The routing engine
//! (`gateway`) consults readiness through this trait object; the local engine
//! (`local-engine`) implements it on its `ProvisioningSupervisor`. Compile-time
//! dependency points at `kernel` only; the runtime call is `dyn` dispatch.
use serde::{Deserialize, Serialize};

/// The lifecycle of a local model's provisioning, newest-value semantics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum ProvisionPhase {
    Absent,
    Queued,
    Downloading { done: u64, total: Option<u64> },
    Verifying,
    Loading,
    Ready,
    Failed { error: String },
}

impl ProvisionPhase {
    /// True while a job is running (Queued / Downloading / Verifying / Loading).
    /// Ready / Absent / Failed are terminal-or-idle.
    pub fn is_in_flight(&self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Downloading { .. } | Self::Verifying | Self::Loading
        )
    }
}

/// A phase transition for one model — what a consumer relays to a progress UI.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvisionEvent {
    pub model: String,
    pub phase: ProvisionPhase,
}

/// Port the routing engine consults for a model's readiness. Implemented by the
/// local engine's supervisor; consumed by `gateway` via `Arc<dyn ReadinessProbe>`.
#[async_trait::async_trait]
pub trait ReadinessProbe: Send + Sync {
    async fn phase(&self, model: &str) -> ProvisionPhase;
    async fn status_all(&self) -> Vec<(String, ProvisionPhase)>;
}
```

- [ ] **Step 4: Declare + re-export in `crates/kernel/src/lib.rs`.** Add `pub mod readiness;` and, alongside the existing re-exports:

```rust
pub use readiness::{ProvisionEvent, ProvisionPhase, ReadinessProbe};
```

- [ ] **Step 5: Verify.** `cargo test -p sensei-kernel` (new tests pass), `cargo clippy -p sensei-kernel -- -D warnings`, `cargo build --workspace` (unaffected).

- [ ] **Step 6: Commit.**

```bash
git add crates/kernel/src/readiness.rs crates/kernel/src/lib.rs
git commit -m "feat(kernel): add readiness vocabulary + ReadinessProbe port (ProvisionPhase/ProvisionEvent)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Chain viability pruning in `gateway`

**Files:** create `crates/gateway/src/pruning.rs`; modify `crates/gateway/src/lib.rs` (declare + re-export) and `crates/gateway/src/engine.rs` (the `Gateway::prune_unavailable` convenience method). Pure config logic — no probe yet, independent of Tasks 1/3.

Read first: how `GatewayConfig`/chains/`RouterConfig` are shaped (kernel `types/config.rs`) and the `router.enabled == false` check the selection path already uses (spec cites `selection.rs:148`).

- [ ] **Step 1: Write failing table-driven tests** in `crates/gateway/src/pruning.rs`. Cover: disabled router → dropped + warn; unknown router → dropped + warn; unknown model → dropped + warn; judge `Unavailable` (e.g. no key) → dropped + warn; judge `Pending` → **kept**; judge `Available` → kept; a chain that empties is **retained** (not deleted); warning fields (`chain`/`router`/`model`/`reason`) populated. Build the `GatewayConfig` fixtures with the existing builder/literals. Assert on the mutated config + the returned `Vec<ChainWarning>`.

- [ ] **Step 2: Run to verify fail.** `cargo test -p sensei-gateway pruning` → FAIL.

- [ ] **Step 3: Implement `crates/gateway/src/pruning.rs`** (signatures verbatim from spec §4):

```rust
use kernel::types::config::GatewayConfig; // adjust to the real path

pub enum Availability { Available, Pending, Unavailable { reason: String } }

pub struct ChainWarning {
    pub chain: String,
    pub router: String,
    pub model: String,
    pub reason: String,
}

/// Drop permanently-unavailable chain candidates, keep provisioning ones,
/// return the dropped-and-why report. Library treats pure-config signals
/// (disabled/unknown router, unknown model) as Unavailable without the judge.
pub fn prune_unavailable(
    config: &mut GatewayConfig,
    judge: impl Fn(&str, &str) -> Availability,
) -> Vec<ChainWarning> {
    // For each chain, retain(|entry| ...) the model list: classify (router,model);
    //  - disabled/unknown router or unknown model  -> Unavailable (config-only)
    //  - else -> judge(router, model)
    // Drop on Unavailable (push a ChainWarning); keep on Pending/Available.
    // Do NOT delete an emptied chain — leave it empty (honest NoCandidates later).
    todo!("implement per spec §4 — see the tests for exact behavior")
}
```

Replace the `todo!` with the real implementation guided by the tests (the tests are the contract). The `Gateway::prune_unavailable` convenience (in `engine.rs`) write-locks the shared config, calls the free fn, returns the warnings:

```rust
pub async fn prune_unavailable(
    &self,
    judge: impl Fn(&str, &str) -> Availability,
) -> Vec<ChainWarning> {
    let mut cfg = self.config.write().await; // match the real config lock type
    crate::pruning::prune_unavailable(&mut cfg, judge)
}
```

- [ ] **Step 4: Declare + re-export.** `pub mod pruning;` in `lib.rs`; `pub use pruning::{Availability, ChainWarning, prune_unavailable};`.

- [ ] **Step 5: Verify.** `cargo test -p sensei-gateway pruning`, `cargo clippy -p sensei-gateway -- -D warnings`, `cargo build --workspace`.

- [ ] **Step 6: Commit.**

```bash
git add crates/gateway/src/pruning.rs crates/gateway/src/lib.rs crates/gateway/src/engine.rs
git commit -m "feat(gateway): chain viability pruning (prune_unavailable + ChainWarning)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: `ProvisioningSupervisor` in `local-engine`

**Files:** create `crates/local-engine/src/supervisor.rs`; modify `src/registry/pull.rs` (add `pull_with_progress`), `src/lib.rs`, `Cargo.toml`. Green: `local-engine` default features (supervisor tests use stubbed `ProvisionPlan`s — no hf-hub, no engine). This is the bulk; work strictly TDD, one behaviour per step.

Cargo.toml: add `tokio-stream = "0.1"`; promote `local-engine`'s access to `local-providers` from dev-dep to a normal, feature-gated dep (coldboot loads a `local_providers` adapter). Keep runtime deps otherwise minimal.

- [ ] **Step 1: Add `pull_with_progress` (feature `hf-download`)** in `registry/pull.rs`. Write a fixture test first (no network): a fake puller/callback path that asserts the callback is invoked with monotonic `done` and the known `total`. Then implement the progress-emitting variant alongside `pull` per spec §3 (bridge to `FnMut(u64, Option<u64>)`; `check_fit` already knows `total`). The real-network end-to-end assertion is a separate `#[ignore]` test (Step 7).

```rust
#[cfg(feature = "hf-download")]
pub async fn pull_with_progress(
    &self,
    spec: &PullSpec,
    on_progress: &mut dyn FnMut(u64, Option<u64>),
) -> Result<ModelEntry, PullError> { /* per spec §3 */ }
```

- [ ] **Step 2: Define the supervisor types + `ensure` dedup (no engine features).** Write failing tests for: (a) `ensure` on `Absent` transitions to `Queued` and spawns exactly one job; (b) two concurrent `ensure` calls for the same id share one job (dedup via the slot); (c) `status`/`status_all` reflect the current phase. Use a **test `ProvisionPlan`** that drives phases through injected steps without touching hf-hub or an engine.

```rust
// crates/local-engine/src/supervisor.rs — types per spec §3
pub struct EnsureOpts { pub wait: bool }
pub enum ProvisionError { Failed(String) }

pub struct ProvisionHandle { /* watch::Receiver<ProvisionPhase> + model id */ }
impl ProvisionHandle {
    pub fn events(&self) -> impl futures::Stream<Item = kernel::ProvisionEvent>; // WatchStream
    pub async fn wait_ready(self) -> Result<(), ProvisionError>;
    pub fn phase(&self) -> kernel::ProvisionPhase;
}

struct ModelSlot {
    tx: tokio::sync::watch::Sender<kernel::ProvisionPhase>,
    job: tokio::task::JoinHandle<()>,
}

pub struct ProvisioningSupervisor {
    slots: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, ModelSlot>>>,
    plans: std::collections::HashMap<String, ProvisionPlan>,
    sem: std::sync::Arc<tokio::sync::Semaphore>, // caps concurrent downloads
    // registry handle + AdapterRegistry handle for coldboot/registration
}

impl ProvisioningSupervisor {
    pub fn ensure(&self, model: &str, opts: EnsureOpts) -> ProvisionHandle; // spec §3 idempotency table
    pub async fn status(&self, model: &str) -> kernel::ProvisionPhase;
    pub async fn status_all(&self) -> Vec<(String, kernel::ProvisionPhase)>;
}
```

Implement `ensure` per the spec §3 idempotency table: `Ready`→handle at Ready; in-flight→subscribe (or await if `wait`); `Absent`/`Failed`→`Queued` + spawn + subscribe (or await). One `watch` channel per model; the slot is the dedup key.

- [ ] **Step 3: Phase-transition + `wait_ready` tests.** Failing tests: the `watch` stream observes the ordered phase sequence a stubbed plan emits (e.g. `Queued → Downloading{..} → Verifying → Loading → Ready`); a late subscriber immediately sees the current phase; `wait_ready` resolves `Ok` on `Ready` and `Err` on `Failed`. Implement the job runner that `send_replace`s phases through the slot's sender and bridges `pull_with_progress` → `Downloading { done, total }`.

- [ ] **Step 4: `impl ReadinessProbe` (spec §5).** Failing test: a `ProvisioningSupervisor` used as `Arc<dyn kernel::ReadinessProbe>` returns the slot's current phase from `phase()` and the full map from `status_all()`. Implement:

```rust
#[async_trait::async_trait]
impl kernel::ReadinessProbe for ProvisioningSupervisor {
    async fn phase(&self, model: &str) -> kernel::ProvisionPhase { /* slot or Absent */ }
    async fn status_all(&self) -> Vec<(String, kernel::ProvisionPhase)> { /* snapshot */ }
}
```

- [ ] **Step 5: Feature-gated coldboot jobs.** Behind `llama-cpp`/`fastembed`/`ort` + `hf-download`, wire the real job bodies (spec §3): Managed/HF GGUF (`check_fit` → `pull_with_progress` → coldboot `EmbeddedLlamaAdapter`/`LlamaCppAdapter` → `registry.register`); fastembed/ort (`load` → `register_embed`); Ollama (probe/reuse → register). The default-feature build compiles none of these bodies; guard with `#[cfg(feature = ...)]`.

- [ ] **Step 6: Declare + re-export + green.** `pub mod supervisor;` in `lib.rs`; re-export `ProvisioningSupervisor, EnsureOpts, ProvisionHandle, ProvisionError, ProvisionPlan`. Run:

```
cargo test  -p sensei-local-engine
cargo clippy -p sensei-local-engine -- -D warnings
cargo build  -p sensei-local-engine --features hf-download,llama-cpp,fastembed,ort
cargo clippy -p sensei-local-engine --features hf-download,llama-cpp,fastembed,ort -- -D warnings
cargo build --workspace
```

- [ ] **Step 7: `#[ignore]` real pull-with-progress test** (network): a tiny public GGUF; assert monotonic `Downloading { done }` then `Ready`. Document the env/run line. Then commit.

```bash
git add crates/local-engine
git commit -m "feat(local-engine): ProvisioningSupervisor (watch-streamed readiness) + pull_with_progress

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: `ModelNotReady` degradation + `with_readiness` seam in `gateway`

**Files:** modify `crates/kernel/.../error.rs` (add the variant), `crates/gateway/src/engine.rs` (probe field, `with_readiness`, terminal consultation, stream arm), the fallback-trigger classifier match, `crates/gateway/src/lib.rs`. Adding the variant to `kernel::GatewayError` breaks every exhaustive match in `gateway` (no `_` arm) — the compiler enumerates them; this task fixes them all so the workspace ends green.

- [ ] **Step 1: Add the variant to `kernel::GatewayError`** (spec §6):

```rust
#[error("model '{model}' not ready: {phase:?}")]
ModelNotReady { model: String, phase: crate::readiness::ProvisionPhase },
```

Run `cargo build -p sensei-gateway` → it FAILS with non-exhaustive `match` errors; those failures are your task list for Steps 2–3.

- [ ] **Step 2: Fallback classifier — `ModelNotReady` never triggers fallback.** In the fallback-trigger match (the `should_trigger_fallback`-equivalent in `error.rs`), add `GatewayError::ModelNotReady { .. } => false`. Write a unit test asserting it returns `false`.

- [ ] **Step 3: Stream error-code arm.** At `engine.rs:883` (the `AllAttemptsFailed => "all_attempts_failed"` map), add `GatewayError::ModelNotReady { .. } => "model_not_ready".to_string()`. Test the mapping.

- [ ] **Step 4: `with_readiness` + probe field.** Mirror `with_store` (`engine.rs:55`): add `probe: Option<Arc<dyn kernel::ReadinessProbe>>` to `Gateway`, default `None`, builder `with_readiness(mut self, probe) -> Self`. Test that a gateway with no probe behaves byte-identically to today (existing tests still pass).

- [ ] **Step 5: Terminal consultation at exhaustion.** Failing test with a **fake `ReadinessProbe`**: a chain whose only candidate is `is_in_flight()` returns `ModelNotReady { model, phase }`; a chain with a ready cloud fallback succeeds; **no probe attached → `AllAttemptsFailed` exactly as today**. Implement at `engine.rs:482`: before building `AllAttemptsFailed`, if `self.probe` is `Some`, consult it for the attempted candidates in priority order and return `ModelNotReady` for the first whose phase `is_in_flight()`. Selection algorithm and the candidate walk are otherwise untouched.

- [ ] **Step 6: Re-export + green.** `pub use pruning::…` already done; ensure `gateway::GatewayError` (re-exported from kernel) exposes the new variant. Run `cargo test -p sensei-gateway`, `cargo clippy -p sensei-gateway -- -D warnings`, `cargo build --workspace`, and the re-export path-smoke test (`crates/gateway/tests/reexport_paths.rs`).

- [ ] **Step 7: Commit** (kernel + gateway together — the variant and its handling land atomically):

```bash
git add crates/kernel crates/gateway
git commit -m "feat(gateway): ModelNotReady degradation + with_readiness probe seam

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Facade builder + feature wiring in `gateway`

**Files:** create/extend `crates/gateway/src/facade.rs` (or the existing builder module); modify `crates/gateway/Cargo.toml` (features `cloud` → `cloud-providers`, `local` → `local-engine`) and `src/lib.rs` (curated re-exports). Green: the `--features` matrix.

- [ ] **Step 1: Confirm the feature wiring.** `gateway`'s `[features]` already gates `cloud-providers` behind `cloud` (from PR 2). Add `local = ["dep:local-engine"]`. `local-engine` and `cloud-providers` are optional deps arriving only via the enabled wing.

- [ ] **Step 2: Batteries-included builder (spec §2/§5 composition root).** A constructor that: builds the `AdapterRegistry`, registers the cloud providers (feature `cloud`), constructs the `ProvisioningSupervisor` from a `HashMap<String, ProvisionPlan>` (feature `local`), and calls `gateway.with_readiness(Arc::new(supervisor))`. Keep the low-level `Gateway::new`/`with_store`/`with_readiness` public for hand-wiring. Test (feature `local` + a stub plan) that the composed gateway reports readiness through the probe and prunes with a judge.

- [ ] **Step 3: Curated re-exports.** Ensure the facade re-exports what clients need without them adding `local-engine`/`cloud-providers` directly: `gateway::{Gateway, GatewayError, ProvisionPhase, ProvisionEvent, Availability, ChainWarning}` and `gateway::adapters::AdapterRegistry`. A `local`-gated re-export surfaces `ProvisioningSupervisor`/`EnsureOpts`/`ProvisionHandle` under e.g. `gateway::local::…`.

- [ ] **Step 4: Full feature-matrix gate.**

```
cargo build --workspace
cargo test  --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo build -p sensei-gateway --no-default-features                       # lean core, no providers
cargo build -p sensei-gateway --features cloud
cargo build -p sensei-gateway --features local
cargo build -p sensei-gateway --features cloud,local
cargo build -p sensei-local-engine --features hf-download,llama-cpp,fastembed,ort
```

All green; `gateway::…` re-export paths still compile.

- [ ] **Step 5: Commit.**

```bash
git add crates/gateway
git commit -m "feat(gateway): batteries-included facade builder wiring cloud providers + local engine

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Self-review checklist (before landing)

- [ ] **Spec coverage:** §3 supervisor (T3) · §4 pruning (T2) · §5 readiness port + seam (T1 port, T4 seam) · §6 degradation/`ModelNotReady` (T4) · §8 public API additions all present. §7 testing strategy mapped: kernel phase/serde/fake-probe (T1), pruning table (T2), supervisor dedup/phases/wait_ready/status_all (T3), degradation with fake probe (T4/T5), `#[ignore]` real pull (T3 Step 7), feature matrix (T5 Step 4).
- [ ] **No cross-engine dep:** `local-engine` and `cloud-providers` never name each other; the only engine↔engine meeting point is `kernel::ReadinessProbe`. Dependency graph still acyclic (both wings → `kernel`; `gateway` → wings via features).
- [ ] **No-probe parity:** with no probe attached, `execute()` is byte-identical to today (guarded by an explicit test).
- [ ] **`ModelNotReady` never falls back:** exhaustive match (no `_`) forces the `=> false` decision; test asserts it.
- [ ] **Placeholder scan:** the two `todo!()`s in Task 2 Step 3 are TDD scaffolds — the tests are the contract; they must be replaced with real impls before that task's commit. No `todo!` survives into a commit.
- [ ] Whole-branch code review, then land to `develop` per the release workflow.

## Notes

- Downstream (senseid) wires the composition root: constructs the supervisor from operator config, calls `with_readiness`, maps `status`/`status_all` + pruning warnings onto its status endpoint, and relays `ProvisionHandle::events()` to clients. Out of scope here (spec §11 non-goals: HTTP endpoints, SSE relay, CLI flags, daemon startup sequencing, the availability predicate that feeds the judge).
- Adjust every `crate::…`/`kernel::types::…` path in this plan to the real module locations when you open the files (the config/error module paths moved during PR 1); the compiler and the cited anchors are the source of truth.
