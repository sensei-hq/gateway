---
title: SP-1 (slice 1) — Durable-Executor Spine (design)
doctype: spec
spec: SP-1
slice: 1
phase: 3
status: approved
related:
  - docs/superpowers/specs/2026-08-06-sensei-orchestrator-design.md   # master orchestrator design (§5 crates, §7 durable core, §9 agent runtime, §10 graph, §11 resilience)
  - docs/superpowers/specs/2026-08-07-reference-chains-design.md      # the chains this rides
---

# SP-1 (slice 1) — Durable-Executor Spine

**Goal:** the smallest end-to-end **walking skeleton** that proves the orchestrator's novel core — a **deterministic executor over a durable journal that resumes without re-spending tokens** — wired to the real gateway via a reference chain. No agent/skill/tool registry, no planner, no fan-out yet: those layer cleanly on top in later slices.

**Master design:** `docs/superpowers/specs/2026-08-06-sensei-orchestrator-design.md` (§5 crates, §7 durable execution core, §9.1 gateway boundary, §10 graph, §11 resilience). This spec **scopes slice 1** and defers the rest to named later slices.

**Approved decisions (brainstorm 2026-08-08):**
1. **Spine bootstrap first** (not the full fan-out skeleton) — a single **linear** graph.
2. **Raw `ModelCall` nodes** (no agent runtime/registry yet) — a node directly builds an `InferenceRequest`.
3. **Crash seam = a mid-run node-2 failure, then resume** on the same journal.
4. **Crates live in the gateway workspace**, `sensei-orchestrator-*`.

---

## 1. Scope

**In slice 1:** 3 crates; core graph/effect/journal types; an `InMemoryJournal`; a deterministic executor that runs a **linear** graph of **`ModelCall` (Pure)** effects through the long-lived `Arc<Gateway>`; structural `effect_id` + input-hash memoization; a **version-fence**; **resume/fold** that memoizes completed `ModelCall`s (no token re-spend); the **§9.1 gateway boundary** (a node compiles a plain `InferenceRequest`, no agent metadata). CAS/blackboard omitted — small outputs stored inline in the journal.

**Deferred (stated, not silent) to later SP-1 slices:**
- **Slice 2:** agent/skill/tool **registry** (md+frontmatter) + prompt-assembly **agent runtime** + ReAct loop (the §9.1 `AgentInvocation → InferenceRequest` compilation).
- **Slice 3:** `Map` bounded **fan-out** + `hard`/`soft` edges + `quorum`/`Consolidate` + `ContextStore` blackboard + `ContentStore`/**CAS** (journal/payload split, snapshots, compaction).
- **Slice 4:** **`Observation`/`Mutation`** effects + two-phase journaling + `in_doubt → reconcile` (the mutation-safety core).
- **Later:** planner/`PlanDelta`/loops-of-graphs, streaming, hooks + `HumanGate`/`AwaitSignal`, `PostgresJournal`. **No persistence** beyond the in-memory journal (consistent with the config-driven/no-persistence directive; `PostgresJournal` is a separate held-off layer).

## 2. Crates (gateway workspace, `sensei-*`)

| Crate | Slice-1 content |
|---|---|
| `sensei-orchestrator-core` | Zero-I/O: `RunId`/`NodeId`/`Seq`, `EffectClass`, `EffectId`, `Graph`/`Node`/`NodeKind`, `JournalEvent`, `ExecutionJournal` trait, error types. Depends on `sensei-kernel` (for `Capability`/request payload types if needed) but NOT `sensei-gateway`. |
| `sensei-orchestrator` | The `Executor` (run + resume), the `ModelCall` effect → gateway. Links `sensei-gateway`. |
| `sensei-orchestrator-store` | `InMemoryJournal` (`impl ExecutionJournal`). |

## 3. Core types (`sensei-orchestrator-core`)

```rust
pub struct RunId(pub uuid::Uuid);
pub struct NodeId(pub String);          // stable, author-assigned (e.g. "n1")
pub type Seq = u64;                     // global monotonic; stamps every EffectRecorded

pub enum EffectClass { Pure, Observation, Mutation }   // only Pure exercised in slice 1

/// Structural, iteration-aware id: hash(parent_path ‖ loop_iteration ‖ local_index).
/// Linear slice-1 graph ⇒ loop_iteration = 0, parent_path = "", local_index = node position.
pub struct EffectId(pub String);
pub fn effect_id(parent_path: &str, loop_iteration: u64, local_index: usize) -> EffectId;

pub struct Node { pub id: NodeId, pub kind: NodeKind, pub deps: Vec<NodeId> } // slice-1: deps = the single prior node (hard)
pub enum NodeKind {
    ModelCall { chain: String, payload: serde_json::Value }, // → InferenceRequest{chain, payload}
}
pub struct Graph { pub nodes: Vec<Node> }   // slice-1: validated linear (each node deps on the prior)

pub enum JournalEvent {
    RunStarted { version: String },
    NodeStarted { node: NodeId },
    EffectRecorded { node: NodeId, effect_id: EffectId, class: EffectClass, input_hash: String, seq: Seq, output: serde_json::Value },
    NodeCompleted { node: NodeId },
    NodeFailed { node: NodeId, error: String },   // structured error preserved; string here is the surfaced form
    RunCompleted,
    RunPaused { reason: String, resume_after: Option<chrono::DateTime<chrono::Utc>> }, // for the AllGated durable pause (wired in slice 4/later)
}

#[async_trait::async_trait]
pub trait ExecutionJournal: Send + Sync {
    async fn append(&self, run: RunId, event: JournalEvent) -> Result<Seq, JournalError>; // STRICT: errors fatal/pause
    async fn load(&self, run: RunId) -> Result<Vec<(Seq, JournalEvent)>, JournalError>;
}

pub enum OrchestratorError { Journal(JournalError), DeterminismViolation { node: NodeId, ... }, VersionFenceMismatch { recorded: String, current: String }, Gateway(String), ... }
```

Design fidelity: `effect_id` is structural + iteration-aware (§7.2); `EffectRecorded` binds an **input-hash** so a changed input on replay halts (§7.2); the journal `append` is **strict** — a journal-write error is fatal/pause, never swallowed (§11.1.2); `Seq` is a global monotonic order for deterministic fold (§7.6).

## 4. Executor (`sensei-orchestrator`)

```rust
pub struct Executor { gateway: Arc<Gateway>, journal: Arc<dyn ExecutionJournal>, version: String }
impl Executor {
    /// Fresh run: append RunStarted{version}, then drive the linear graph.
    pub async fn run(&self, run: RunId, graph: &Graph) -> Result<RunOutcome, OrchestratorError>;
    /// Resume: load+fold the journal, memoize completed effects, continue from the first incomplete node.
    pub async fn start(&self, run: RunId, graph: &Graph) -> Result<RunOutcome, OrchestratorError>;
}
```

- **Per `ModelCall` node** (a **Pure** effect): compute `effect_id` (structural) + `input_hash = hash(chain ‖ payload)`. If the fold shows this `effect_id` already `EffectRecorded` with a **matching** `input_hash` → **memoize** (use the recorded output, do NOT call the gateway). If recorded with a **different** input_hash → **halt** `DeterminismViolation` (never memoize a mismatch). Else: `NodeStarted` → build `InferenceRequest { chain, payload }` → `gateway.execute()` → on success `EffectRecorded{output}` (fsync/append strict) → `NodeCompleted`; on gateway error `NodeFailed{error}` and stop the run (surfaced in `RunOutcome`, structured error preserved).
- **Version-fence:** `run` appends `RunStarted{version}`; `start` reads the recorded version and, if it differs from `self.version`, halts `VersionFenceMismatch` (a registry/config change refuses silent resume — §7.2).
- **`run` vs `start`:** `run` is the fresh entry (asserts no prior `RunStarted`); `start` is the resume entry (folds first). Both drive the same node loop; `start` simply seeds it from the fold. (They may share one internal driver.)

## 5. Journal store (`sensei-orchestrator-store`)

`InMemoryJournal { events: Arc<Mutex<HashMap<RunId, Vec<(Seq, JournalEvent)>>>>, next_seq: Arc<AtomicU64> }` — `append` assigns the next `Seq` and pushes; `load` returns the run's events in `Seq` order. **Arc-shared**, so a fresh `Executor` constructed with the *same* `InMemoryJournal` sees a prior run's events — this is the crash/resume seam (no real crash needed; a new executor on the shared journal *is* the resumed process).

## 6. Acceptance test — resume without re-spend (the headline)

1. Build a 2-node linear graph `[n1: ModelCall(chainA), n2: ModelCall(chainA)]`, a **call-counting mock gateway** (records each `InferenceRequest`), and one shared `InMemoryJournal`.
2. **Run 1** with a gateway that **succeeds on n1, errors on n2**: n1 → gateway call, `EffectRecorded`; n2 → gateway call, **fails** → `NodeFailed`, run stops. (n1's output is durably journaled.)
3. **Run 2** = a fresh `Executor` on the **same journal**, gateway now succeeds on n2: `start(run)` folds → n1's `effect_id` is `EffectRecorded` with a matching input_hash → **memoized, gateway NOT called for n1** → n2 runs → gateway call → `EffectRecorded` → `RunCompleted`.
4. **Assert:** the mock gateway received **n1's request exactly once** (across both runs) and n2's twice (run1 fail + run2 success) — i.e. the completed `ModelCall` was **not re-spent** on resume. Also assert the journal ends with `RunCompleted`.

Plus: a **determinism-violation** test (resume with n1's payload changed → `DeterminismViolation` halt, not a silent re-run/memoize); a **version-fence** test (resume with a different `version` → `VersionFenceMismatch`); a **strict-journal** test (an `append` error fails/pauses the run, never swallowed); and a **real end-to-end** test (`demo_catalog` → `assemble` → `Gateway` → a 1-node `ModelCall` on `research.bulk` served by the local model, via the same noop-adapter pattern as the reference-chains test).

## 7. Design boundaries

- **No persistence** beyond the in-memory journal; `PostgresJournal` is a separate held-off layer (config-driven directive). The `ExecutionJournal` trait is the seam it will implement later.
- **Gateway is a long-lived pure client** (§9.1): built once, held as `Arc<Gateway>`; the executor passes only `request.chain` (+ later BYOK `credentials`); no per-run create/close, no agent metadata in the request.
- **No silent failures** (§11.1): every node error is journaled AND surfaced in `RunOutcome`; journal-write errors are strict; structured errors are preserved (no string flattening beyond the surfaced form).
- **SRP / additive:** the gateway + catalog crates are untouched; the orchestrator is new crates that *consume* `Gateway::execute`.
- Slice 1 is a **walking skeleton**: intentionally linear + Pure-only so the durable-execution mechanics are proven in isolation before agents, fan-out, and mutations layer on.
