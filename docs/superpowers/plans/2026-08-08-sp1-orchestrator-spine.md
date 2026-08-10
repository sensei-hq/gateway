# SP-1 (slice 1) — Durable-Executor Spine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Stand up the orchestrator's durable-execution spine — a deterministic executor over a durable journal that **resumes without re-spending tokens** — as 3 new workspace crates, wired to the real gateway via a reference chain. Design: `docs/superpowers/specs/2026-08-08-sp1-orchestrator-spine-design.md`.

**Architecture:** `sensei-orchestrator-core` (zero-I/O types: graph/effect/journal), `sensei-orchestrator` (the `Executor`, links `sensei-gateway`), `sensei-orchestrator-store` (`InMemoryJournal`). A **linear** graph of **`ModelCall` (Pure)** effects; each node → a plain `InferenceRequest` → `Arc<Gateway>::execute`; memoized by structural `effect_id` + input-hash; resume folds the journal and skips completed effects. No agent runtime, no fan-out, no persistence beyond the in-memory journal (all deferred to later slices).

**Tech Stack:** Rust (edition 2024), gateway workspace. Conventions (from `crates/gateway/Cargo.toml`): `[package] name = "sensei-X"`, `[lib] name = "X"`; path deps `kernel = { package = "sensei-kernel", path = "../kernel" }`. Contract per commit: `cargo build --workspace` + `cargo test --workspace` green; `make check` clean (fmt + clippy `-D warnings`).

---

## File Structure

- **`crates/orchestrator-core/`** — `Cargo.toml` (lib `orchestrator_core`), `src/lib.rs` + modules: `ids.rs` (`RunId`/`NodeId`/`Seq`), `effect.rs` (`EffectClass`/`EffectId`/`effect_id`), `graph.rs` (`Node`/`NodeKind`/`Graph`), `journal.rs` (`JournalEvent`/`ExecutionJournal`), `error.rs`. [T1]
- **`crates/orchestrator-store/`** — `Cargo.toml` (lib `orchestrator_store`), `src/lib.rs` (`InMemoryJournal`). [T2]
- **`crates/orchestrator/`** — `Cargo.toml` (lib `orchestrator`, deps `sensei-gateway`), `src/lib.rs` + `executor.rs` (`Executor::run` [T3], `::start` [T4]), `src/test_support.rs` (recording/failing test adapter helpers). [T3–T5]
- **`Cargo.toml`** (root) — add the 3 crates to `members`. [T1]
- **`docs/features/orchestrator/durable-executor.md`** + README. [T5]

---

### Task 1: Scaffold 3 crates + `orchestrator-core` types

**Files:** root `Cargo.toml`; new `crates/orchestrator-core/**`, `crates/orchestrator/{Cargo.toml,src/lib.rs}`, `crates/orchestrator-store/{Cargo.toml,src/lib.rs}`.

- [ ] **Step 1: Failing test** (in `orchestrator-core`):
```rust
#[test]
fn effect_id_is_structural_and_stable() {
    let a = effect_id("", 0, 0);
    let b = effect_id("", 0, 0);
    let c = effect_id("", 0, 1);
    assert_eq!(a, b);                 // same structural coords → same id (deterministic)
    assert_ne!(a, c);                 // different local_index → different id
    assert_ne!(effect_id("p", 0, 0), a);      // parent_path matters
    assert_ne!(effect_id("", 1, 0), a);       // loop_iteration matters
}
#[test]
fn journal_event_roundtrips() {
    let e = JournalEvent::EffectRecorded {
        node: NodeId("n1".into()), effect_id: effect_id("", 0, 0),
        class: EffectClass::Pure, input_hash: "abc".into(), seq: 1,
        output: serde_json::json!({"text":"hi"}),
    };
    let s = serde_json::to_string(&e).unwrap();
    let back: JournalEvent = serde_json::from_str(&s).unwrap();
    assert!(matches!(back, JournalEvent::EffectRecorded { .. }));
}
```
- [ ] **Step 2: Scaffold** the 3 crates. Root `Cargo.toml` `members` gains `"crates/orchestrator-core"`, `"crates/orchestrator"`, `"crates/orchestrator-store"`.
  - `orchestrator-core/Cargo.toml`: `[package] name="sensei-orchestrator-core" version="0.1.0" edition="2024" license="MIT"`, `[lib] name="orchestrator_core"`; deps: `async-trait="0.1"`, `chrono={version="0.4",features=["serde"]}`, `serde={version="1",features=["derive"]}`, `serde_json="1"`, `thiserror="2"`, `uuid={version="1",features=["v4","serde"]}`, `sha2="0.10"`.
  - `orchestrator-store/Cargo.toml`: name `sensei-orchestrator-store`, lib `orchestrator_store`; deps: `orchestrator-core={package="sensei-orchestrator-core",path="../orchestrator-core"}`, `async-trait="0.1"`; dev-deps `tokio={version="1",features=["full","test-util"]}`.
  - `orchestrator/Cargo.toml`: name `sensei-orchestrator`, lib `orchestrator`; deps: `orchestrator-core={package="sensei-orchestrator-core",path="../orchestrator-core"}`, `gateway={package="sensei-gateway",path="../gateway"}`, `kernel={package="sensei-kernel",path="../kernel"}`, `async-trait="0.1"`, `chrono={version="0.4",features=["serde"]}`, `serde={version="1",features=["derive"]}`, `serde_json="1"`, `sha2="0.10"`, `thiserror="2"`, `tokio={version="1",features=["full"]}`, `tracing="0.1"`, `uuid={version="1",features=["v4","serde"]}`; dev-deps `orchestrator-store={package="sensei-orchestrator-store",path="../orchestrator-store"}`, `tokio={..."test-util"}`. (`orchestrator`/`orchestrator-store` `src/lib.rs` may be near-empty stubs this task — just enough to build.)
- [ ] **Step 3:** `cargo build --workspace` → confirm the new-type test FAILs to compile (types missing), the crates otherwise build.
- [ ] **Step 4: Implement `orchestrator-core`** (all public types `#[derive(Debug, Clone, Serialize, Deserialize)]`; ids also `PartialEq, Eq, Hash`):
  - `ids.rs`: `RunId(pub uuid::Uuid)`, `NodeId(pub String)`, `pub type Seq = u64;`.
  - `effect.rs`: `EffectClass { Pure, Observation, Mutation }` (Copy, Eq); `EffectId(pub String)` (Eq, Hash); `pub fn effect_id(parent_path: &str, loop_iteration: u64, local_index: usize) -> EffectId` = `EffectId(hex(sha256(format!("{parent_path}|{loop_iteration}|{local_index}"))))`.
  - `graph.rs`: `NodeKind::ModelCall { chain: String, payload: serde_json::Value }`; `Node { id: NodeId, kind: NodeKind, deps: Vec<NodeId> }`; `Graph { nodes: Vec<Node> }` + `impl Graph { pub fn validate_linear(&self) -> Result<(), OrchestratorError> }` (each node after the first deps on exactly the prior node id; distinct ids).
  - `journal.rs`: `JournalEvent` enum (the §3 variants); `#[async_trait] pub trait ExecutionJournal: Send + Sync { async fn append(&self, run: RunId, event: JournalEvent) -> Result<Seq, JournalError>; async fn load(&self, run: RunId) -> Result<Vec<(Seq, JournalEvent)>, JournalError>; }`.
  - `error.rs`: `#[derive(Debug, thiserror::Error)] pub enum JournalError { ... }`; `pub enum OrchestratorError { Journal(JournalError), VersionFenceMismatch{recorded:String,current:String}, DeterminismViolation{node:NodeId, effect_id:EffectId}, InvalidGraph(String), Gateway(String) }` (+ `Display`/`Error`).
  - `lib.rs`: `pub mod ...; pub use ...;`.
- [ ] **Step 5: Verify** — `cargo test -p sensei-orchestrator-core` green; `cargo build --workspace` green; `cargo clippy --workspace --all-targets -- -D warnings` clean; `cargo fmt --all --check` clean.
- [ ] **Step 6: Commit:** `feat(orchestrator-core): scaffold orchestrator crates + core graph/effect/journal types`.

---

### Task 2: `InMemoryJournal` (`orchestrator-store`)

**Files:** `crates/orchestrator-store/src/lib.rs`.

- [ ] **Step 1: Failing tests:** append two events for a run → `load` returns them in ascending `Seq`; `Seq` is monotonic across appends/runs; two clones of the same `InMemoryJournal` (Arc-shared) see each other's appends; distinct `RunId`s are isolated.
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement:**
```rust
#[derive(Clone, Default)]
pub struct InMemoryJournal {
    runs: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<RunId, Vec<(Seq, JournalEvent)>>>>,
    next_seq: std::sync::Arc<std::sync::atomic::AtomicU64>,
}
#[async_trait::async_trait]
impl ExecutionJournal for InMemoryJournal {
    async fn append(&self, run: RunId, event: JournalEvent) -> Result<Seq, JournalError> {
        let seq = self.next_seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.runs.lock().unwrap_or_else(|e| e.into_inner()).entry(run).or_default().push((seq, event));
        Ok(seq)
    }
    async fn load(&self, run: RunId) -> Result<Vec<(Seq, JournalEvent)>, JournalError> {
        Ok(self.runs.lock().unwrap_or_else(|e| e.into_inner()).get(&run).cloned().unwrap_or_default())
    }
}
```
  (`RunId` needs `Hash, Eq` — add to its derives in Task 1 if missing. Events are stored append-order which is already `Seq`-ascending; `load` returns them as-is.)
- [ ] **Step 4: Verify** — store tests green; `cargo test --workspace` green; clippy/fmt clean.
- [ ] **Step 5: Commit:** `feat(orchestrator-store): InMemoryJournal (Arc-shared ExecutionJournal)`.

---

### Task 3: `Executor::run` — fresh linear run through the gateway

**Files:** `crates/orchestrator/src/{lib.rs,executor.rs,test_support.rs}`.

- [ ] **Step 1: Failing test** (in `orchestrator`): build a real `Gateway` with a **recording test adapter** (records each `InferenceRequest`'s model/prompt into a shared `Vec`, returns a canned success), a config whose chain `"c"` resolves to that adapter's model, an `InMemoryJournal`, and an `Executor { gateway, journal, version: "v1" }`. `run` a 2-node linear graph `[n1: ModelCall{chain:"c"}, n2: ModelCall{chain:"c"}]` → assert `RunOutcome` is success, the adapter recorded **2** calls, and `journal.load` contains `RunStarted, NodeStarted(n1), EffectRecorded(n1), NodeCompleted(n1), NodeStarted(n2), EffectRecorded(n2), NodeCompleted(n2), RunCompleted` in order. (Build the test adapter + gateway in `test_support.rs`, modeled on the gateway's own `NoopAdapter`/reference-chains test harness — read `crates/gateway/src/engine/tests.rs` + `catalog/presets.rs`'s runnable test for the pattern.)
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement** `executor.rs`:
```rust
pub struct Executor { gateway: std::sync::Arc<gateway::Gateway>, journal: std::sync::Arc<dyn ExecutionJournal>, version: String }
pub struct RunOutcome { pub completed: Vec<NodeId>, pub failed: Option<(NodeId, String)>, pub outputs: std::collections::HashMap<NodeId, serde_json::Value> }

impl Executor {
    pub fn new(gateway: Arc<gateway::Gateway>, journal: Arc<dyn ExecutionJournal>, version: impl Into<String>) -> Self { ... }

    pub async fn run(&self, run: RunId, graph: &Graph) -> Result<RunOutcome, OrchestratorError> {
        graph.validate_linear().map_err(...)?;
        self.append(run, JournalEvent::RunStarted { version: self.version.clone() }).await?;
        self.drive(run, graph, &Default::default()).await   // no prior memo
    }
    // shared node loop; `memo`: effect_id → (input_hash, output) from a fold (empty for a fresh run)
    async fn drive(&self, run, graph, memo: &HashMap<EffectId, (String, serde_json::Value)>) -> Result<RunOutcome, OrchestratorError> { ... }
}
```
  - `drive` walks nodes in order (skip those already `NodeCompleted` in the fold — Task 4). For each `ModelCall { chain, payload }`: `let eid = effect_id("", 0, index); let ih = input_hash(chain, payload);`
    - if `memo.get(&eid)` is `Some((h, out))`: if `h == ih` → **memoize** (record `out` into outputs, no gateway call, no new EffectRecorded — it's already journaled); else → `Err(DeterminismViolation{node, eid})`.
    - else: `append(NodeStarted)`; build `InferenceRequest`; `gateway.execute(&req).await`:
      - `Ok(resp)` → `let out = json!({ "model": resp.model, "text": <resp text> });` `append(EffectRecorded{node, eid, Pure, ih, seq, out})?` then `append(NodeCompleted)?`; push to outputs.
      - `Err(e)` → `append(NodeFailed{node, e.to_string()})?`; set `RunOutcome.failed` and STOP (return the partial outcome).
  - After all nodes complete: `append(RunCompleted)?`.
  - `input_hash(chain, payload) = hex(sha256(format!("{chain}|{}", serde_json::to_string(payload)?)))`.
  - **Build `InferenceRequest`** (read `kernel::types::request::InferenceRequest` for exact fields): `capability: TextChat`, `chain: Some(node.chain)`, `payload: node.payload`, `allow_fallback: true`, everything else `None`/default. The `resp` text extraction: read `InferenceResponse` for where the text lives (`.content`/message) — extract a string for the memoized output.
  - `self.append(..)` wraps `journal.append` and maps `JournalError` → `OrchestratorError::Journal` (**strict** — a journal error aborts the run loudly).
- [ ] **Step 4: Verify** — the run test passes (2 recorded calls, correct journal event order); `cargo test --workspace` green; clippy/fmt clean.
- [ ] **Step 5: Commit:** `feat(orchestrator): Executor::run — linear ModelCall graph through the gateway (journaled)`.

---

### Task 4: `Executor::start` (resume/fold) + acceptance tests

**Files:** `crates/orchestrator/src/executor.rs` (+ tests).

- [ ] **Step 1: Failing tests** (the headline + guards):
  - **Resume without re-spend:** shared `InMemoryJournal`; Run 1 = `run` a 2-node graph with a gateway whose adapter **succeeds on the 1st call, errors on the 2nd** (a fail-on-Nth test adapter) → n1 `EffectRecorded`, n2 `NodeFailed`, `RunOutcome.failed == Some(n2)`. Run 2 = a **fresh** `Executor` on the **same journal** with an adapter that succeeds → `start(run, graph)` → `RunCompleted`. **Assert the n1 request was sent to a gateway exactly once across both runs** (the run-2 adapter records 0 calls for n1 / only n2's call) — proving n1 was memoized, not re-spent. (Use two separate recording adapters, one per run, and assert run-2's adapter recorded exactly the n2 call.)
  - **Determinism violation:** resume a journal where n1 is `EffectRecorded`, but call `start` with a graph whose n1 `payload` differs → `Err(DeterminismViolation{node: n1, ..})` (never silently re-run/memoize).
  - **Version fence:** journal recorded `RunStarted{version:"v1"}`; `start` with an `Executor` of `version:"v2"` → `Err(VersionFenceMismatch{recorded:"v1",current:"v2"})`.
  - **Strict journal:** an `ExecutionJournal` whose `append` returns `Err` → `run` returns `Err(OrchestratorError::Journal(..))` (the error is surfaced, not swallowed; the run does not silently continue).
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement** `Executor::start`:
```rust
    pub async fn start(&self, run: RunId, graph: &Graph) -> Result<RunOutcome, OrchestratorError> {
        graph.validate_linear()?;
        let events = self.journal.load(run).await.map_err(OrchestratorError::Journal)?;
        if events.is_empty() { return self.run(run, graph).await; }        // nothing to resume
        // version fence
        if let Some(JournalEvent::RunStarted { version }) = first RunStarted {
            if version != self.version { return Err(VersionFenceMismatch{recorded: version, current: self.version.clone()}); }
        }
        // fold: completed node ids + memo (effect_id → (input_hash, output)) from EffectRecorded
        // continue via `drive` with the memo (drive skips nodes whose effect_id is in memo with matching input_hash,
        //   halts on mismatch, and appends the RunCompleted once the tail finishes).
        self.drive(run, graph, &memo).await
    }
```
  - The fold builds `memo: HashMap<EffectId, (input_hash, output)>` from every `EffectRecorded`, and a set of completed `NodeId`s. `drive` uses `memo` (Task 3 already handles the memoize / determinism-violation branches); ensure `drive` does NOT re-append `RunStarted` and appends `RunCompleted` only if not already present.
- [ ] **Step 4: Verify** — all four tests pass; the no-re-spend one is the load-bearing proof (run-2 adapter recorded exactly one call, for n2). `cargo test --workspace` green; clippy/fmt clean. Confirm: memoized n1 emits NO gateway call and NO duplicate `EffectRecorded` on resume.
- [ ] **Step 5: Commit:** `feat(orchestrator): Executor::start — resume/fold memoizes completed ModelCalls (no re-spend) + fences`.

---

### Task 5: Real end-to-end (reference chain) + docs

**Files:** `crates/orchestrator/` (a test), `docs/features/orchestrator/durable-executor.md`, `docs/features/orchestrator/README.md`.

- [ ] **Step 1: Failing/real test:** `gateway::catalog::demo_catalog()` → `assemble()` → build a `Gateway` with a noop-style adapter registered for the `ollama` router (reuse the reference-chains runnable-test pattern) → `Executor.run` a **1-node** graph `[n1: ModelCall{chain:"research.bulk"}]` → the walk falls over the credential-gated cloud entries to the local model → `RunOutcome` succeeds and `outputs[n1]` reflects the local model (`llama3.1-local`). Proves the durable executor drives the **real** gateway + a **reference chain** end-to-end.
- [ ] **Step 2:** run → PASS (it exercises shipped code; if it fails, that's a real integration bug — STOP + report BLOCKED, don't weaken).
- [ ] **Step 3: Docs** — create `docs/features/orchestrator/durable-executor.md` (`doctype: feature`, `module: orchestrator`, `status: partial` (slice 1 of SP-1), `spec: SP-1`, `source: crates/orchestrator*/`): the effect-class/journal/resume model (slice-1 scope), the crate layout, and Gherkin scenarios — resume-without-re-spend, determinism-violation-halt, version-fence-refuses-resume, strict-journal-fails-loud. Mark deferred slices (agent runtime, fan-out/CAS, Observation/Mutation, Postgres) clearly. Add/update `docs/features/orchestrator/README.md` with a status row.
- [ ] **Step 4: Verify** — `cargo test --workspace` green; `make check` clean; frontmatter intact.
- [ ] **Step 5: Commit:** `feat(orchestrator): real end-to-end reference-chain run + durable-executor docs`.

---

## Self-Review

- **Spec coverage** (`2026-08-08-sp1-orchestrator-spine-design.md`): 3 crates §2 → T1/T2/T3; core types §3 → T1; executor + effect_id + input-hash + version-fence §4 → T3/T4; `InMemoryJournal` §5 → T2; the resume-no-respend + determinism + version + strict tests §6 → T4; gateway boundary + real e2e §7 → T5. Every §6 acceptance test is a task step.
- **No persistence / config-driven:** the only store is `InMemoryJournal`; `ExecutionJournal` is the seam a `PostgresJournal` implements later (held off). No DB, no I/O beyond the gateway call.
- **Additive / SRP:** the gateway, kernel, and catalog crates are untouched; the orchestrator is new crates consuming `Gateway::execute`. The 3-crate split mirrors `kernel → engine → store`.
- **No silent failures:** journal `append` errors are strict (`OrchestratorError::Journal`, surfaced — T4 strict test); a determinism mismatch or version mismatch **halts** rather than silently re-running/memoizing (T4); node failures are journaled AND surfaced in `RunOutcome`.
- **Type consistency:** `effect_id`/`EffectId` (T1) key the memo in `drive` (T3) used by `run`/`start` (T3/T4); `JournalEvent` (T1) is produced by the executor and consumed by the fold; `InMemoryJournal` (T2) impls the T1 trait; `Executor` holds `Arc<gateway::Gateway>` (§9.1).
- **Sequencing (each green + committed):** 1 core types (compile-only) → 2 store (impls the trait) → 3 `run` (fresh path, needs a test gateway) → 4 `start` (resume, reuses `drive`'s memo branch) → 5 real e2e + docs. No broken intermediate; `drive` is written in T3 with the memo branches T4 exercises (memo empty in T3, populated in T4) — introduced with its first consumer.
- **Placeholder scan:** Cargo.toml deps pinned to the workspace versions; `effect_id`/`input_hash` formulas concrete; the `InferenceRequest`/`InferenceResponse` field wiring is specified as "read the type" (a real dependency the implementer resolves against the kernel types, not a placeholder).

## Execution Handoff

Subagent-driven in an isolated worktree off `develop`; per-task spec + code-quality review (T3 `run`, T4 resume/fences, T5 e2e get the full treatment — the durable-core correctness is the whole point); final whole-branch review; `finishing-a-development-branch` → merge to `develop`. Then **SP-1 slice 2** — the agent/skill/tool registry + prompt-assembly runtime (the §9.1 `AgentInvocation → InferenceRequest` compilation) layered on this spine. Persistence (`PostgresJournal`) stays a separate held-off layer.
