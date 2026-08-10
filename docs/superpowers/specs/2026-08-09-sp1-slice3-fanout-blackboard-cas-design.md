---
title: SP-1 (slice 3) — Fan-out · Blackboard · CAS (design)
doctype: spec
spec: SP-1
slice: 3
phase: 3
status: approved
related:
  - docs/superpowers/specs/2026-08-06-sensei-orchestrator-design.md   # master (§7.4 CAS/snapshots/compaction, §8 blackboard, §10 execution model, §11 resilience)
  - docs/superpowers/specs/2026-08-08-sp1-orchestrator-spine-design.md # slice 1 — durable spine
  - docs/superpowers/specs/2026-08-08-sp1-slice2-agent-runtime-design.md # slice 2 — agent runtime (Map children reuse drive_agent)
---

# SP-1 (slice 3) — Fan-out · Blackboard · CAS

**Goal:** turn the linear agent runtime into a **partial-failure-tolerant parallel DAG** — bounded `Map` fan-out with `hard`/`soft` edges and quorum/`Consolidate` aggregation — backed by a scoped **`ContextStore` blackboard** and a content-addressed **`ContentStore` (CAS)** with snapshots + compaction. Additive on the slice-1/2 spine; the durable "resume-without-re-spend" property extends across concurrent fan-out.

**Master design:** `docs/superpowers/specs/2026-08-06-sensei-orchestrator-design.md` (§7.4 journal/payload split + snapshots + compaction, §8 shared-context blackboard, §10 execution model — nodes/edges/Map/Consolidate/aggregation, §11 no-silent-failures). This spec scopes slice 3 and defers the rest to named later slices.

**Approved decisions (brainstorm 2026-08-09):**
1. **Full slice 3** — all three subsystems (fan-out execution model + blackboard + CAS), composed.
2. **Internal `Map` fan-out** — `Map` is ONE DAG node that fans out INTERNALLY over its items (concurrently, bounded), each child reusing `drive_agent` (or a `ModelCall`/tool template); graph-splicing/`PlanExpanded` of children as first-class nodes is deferred (consistent with slice-2's internal-loop `Agent` node).
3. **Reject same-key blackboard collisions** — a second write to an existing `(scope,key)` is a loud error; fan-out children write **distinct** item-keyed entries. (seq-LWW / merge policies deferred.)
4. **Full CAS** — journal/payload split + size threshold + content-addressing, **plus** snapshots (resume = latest snapshot + replay-the-tail) and compaction (terminal `Map` children → `{status,digest,cost}`).
5. **Typed edges (`Dep`)** — `Node.deps: Vec<Dep>` where `Dep { on: NodeId, kind: EdgeKind::{Hard, Soft} }` (replaces the bare `Vec<NodeId>`; mechanical migration of slice-1/2 literals, mostly in shared test helpers). Cascade-skip fires **only** across `Hard` edges.
6. **Snapshots at round boundaries** — a snapshot is written after each wave of ready nodes completes (matches §7.4; the cadence is tunable, not per-pause-only).

---

## 1. Scope

**In slice 3:**
- **Execution model:** a **DAG scheduler** (ready-node dispatch under a bounded global concurrency cap) replacing the in-order linear drive; `NodeKind::{Map, Consolidate}`; typed `Dep`/`EdgeKind`; `validate_dag`; cascade-skip across hard edges (`NodeSkipped`).
- **Blackboard:** `ContextStore` (scopes `Run`/`Node`), refs-not-blobs, journaled `ContextWrite{seq}`, reject-on-collision, explicit read-miss.
- **CAS:** `ContentStore` (content-addressed, size-threshold split, lazy blob load), snapshots, compaction.
- **Real e2e:** a `Map` running the reference-chain **agent** per item → `Consolidate` synthesizes, through the demo-catalog gateway with fallover.

**Deferred (stated, not silent):**
- **Slice 4:** Observation/Mutation tool effects + two-phase + `in_doubt→reconcile`.
- **Later:** planner/`PlanDelta`, `Loop`-of-subgraphs, `Branch`, `HumanGate`/`AwaitSignal`, streaming; blackboard `Plan`/`Agent` scopes + seq-LWW/merge conflict policies; **provider-aware** per-provider concurrency caps (slice 3 uses one global cap); `PostgresJournal`/persistence (held-off layer). Graph-splicing of `Map` children as first-class nodes.

## 2. Crates & placement (additive)

| Crate | Slice-3 additions |
|---|---|
| `sensei-orchestrator-core` | `graph.rs`: `NodeKind::{Map, Consolidate}`, `Dep { on: NodeId, kind: EdgeKind }`, `validate_dag` (acyclic; deps reference declared nodes); `context.rs`: `Scope`, `ContextKey`, `ContextRef`, `ContextStore` trait; `content.rs`: `Digest`, `ContentRef`, `ContentStore` trait; new `JournalEvent` variants: `NodeSkipped{node}`, `ContextWrite{scope,key,ref,seq}`, `MapExpanded{node, child_count}`, `SnapshotWritten{seq}`. |
| `sensei-orchestrator-store` | `InMemoryContextStore`, `InMemoryContentStore` (Arc-shared `HashMap<Digest,Bytes>`), snapshot storage keyed by `RunId`. |
| `sensei-orchestrator` | `executor.rs`: the DAG scheduler + `Map`/`Consolidate` handlers + cascade-skip + snapshot/compaction hooks + CAS split on effect output; `agent/` unchanged (a `Map` child reuses `drive_agent`). |

No new crate; the 3-crate split holds. Existing gateway/kernel/catalog crates untouched.

## 3. Execution model

### 3.1 Typed edges & DAG validation
```rust
pub enum EdgeKind { Hard, Soft }
pub struct Dep { pub on: NodeId, pub kind: EdgeKind }
impl Dep { pub fn hard(on: impl Into<NodeId>) -> Self; pub fn soft(on: impl Into<NodeId>) -> Self; }
pub struct Node { pub id: NodeId, pub kind: NodeKind, pub deps: Vec<Dep> }   // was Vec<NodeId>
```
`validate_dag`: node ids distinct; every `Dep.on` references a declared node; the hard+soft dep graph is **acyclic** (topological order exists). `validate_linear` is kept as the special case slice-1/2 graphs still satisfy (a line is a valid DAG).

### 3.2 Scheduler (ready-node dispatch, bounded concurrency)
- A node is **ready** when every **Hard** dep is `Completed` and every **Soft** dep is `terminal` (`Completed` **or** `Skipped`/`Failed`).
- The executor dispatches all ready nodes concurrently, capped by `Executor.concurrency` (default 8; a `tokio::sync::Semaphore`). After a wave completes it re-computes ready nodes (a **round**), writes a snapshot (§5.2), and continues until no nodes remain.
- **Behavior preservation:** a linear graph has exactly one ready node at a time → the same sequential `NodeStarted/EffectRecorded/NodeCompleted/RunCompleted` order as slice-1/2. All slice-1/2 executor tests stay green.

### 3.3 Cascade-skip
- If a node ends `Failed` **or** `Skipped`, any node with a **Hard** dep on it becomes `NodeSkipped{node}` (journaled) and is added to the terminal set; its own hard-dependents cascade. **Soft** deps never cascade — a `Consolidate` that soft-depends on a `Map` still runs when some children failed. Skips surface in `RunOutcome.skipped`.

### 3.4 `Map` — internal bounded fan-out
```rust
NodeKind::Map { body: MapBody, over: Vec<serde_json::Value>, concurrency: usize, aggregation: Aggregation }
enum MapBody { Agent(AgentRef), ModelCall { chain: String } }
enum Aggregation { FailFast, BestEffort, Quorum { min_count: Option<usize>, min_fraction: Option<f64> } }
```
- For each `item` (index `i`) in `over`, run `body` with `item` as input, concurrently, bounded by `min(map.concurrency, executor.concurrency)`. Each child is a durable sub-run with a **structural** path `"{map.id}/{i}"` — an `Agent` child's turns nest as `effect_id("{map.id}/{i}", turn, local)` (slice-2 machinery, memoized on resume); a `ModelCall` child is one Pure effect at `effect_id("{map.id}/{i}", 0, 0)`.
- On first dispatch the executor journals `MapExpanded{node, child_count}` (the child manifest is fixed by `over` — deterministic, not order-dependent).
- **Output:** `{ results: Vec<{ index, ok?: Value, error?: String }>, manifest: { ok: usize, failed: usize } }` — results indexed by `i` (deterministic order regardless of completion order).
- **Aggregation → Map status:** `FailFast` = first child error fails the Map (remaining children still recorded, but the node is `Failed`); `BestEffort` = the Map is always `Completed` (failures live in the manifest); `Quorum` = `Completed` iff `ok >= min_count` and/or `ok/total >= min_fraction`, else `Failed` (loud, manifest attached).

### 3.5 `Consolidate` — aggregate the survivors
```rust
NodeKind::Consolidate { over: NodeId, min_viable: usize, body: MapBody }
```
- **Soft**-depends on `over` (the `Map`) so it runs even if the Map ended `Failed` under `BestEffort`/`Quorum`. Reads the Map's **successful** results.
- **Min-viable-input gate:** if `ok_results.len() < min_viable` → `ConsolidateStarved{node, have, need}` (loud halt), never a silent empty synthesis.
- Runs `body` (typically an `Agent`) over the collected results (passed as its input); its output is the node output. The Map's failure manifest is carried through to `RunOutcome`, never dropped.

## 4. ContextStore blackboard (§8)

```rust
enum Scope { Run, Node(NodeId) }                 // Plan/Agent scopes deferred
struct ContextKey(String);
struct ContextRef { key, scope, content: ContentRef, summary: Option<String> }
#[async_trait] trait ContextStore {
    async fn put(&self, scope, key, value: serde_json::Value) -> Result<ContextRef, OrchestratorError>; // reject on (scope,key) collision
    async fn get(&self, scope, key) -> Result<Option<ContextRef>, OrchestratorError>;                   // resolves Node → Run
    async fn load(&self, r: &ContextRef) -> Result<serde_json::Value, OrchestratorError>;               // lazy blob fetch via CAS
}
```
- **Refs, not blobs:** `put` stores the value in the `ContentStore` (or inline if under threshold) and journals `ContextWrite{scope,key,ref,seq}`. `get` resolves **up** the scope chain (`Node(n)` → `Run`); **read-miss is an explicit `Ok(None)`**, never a silent empty value.
- **Reject-on-collision:** a `put` to an existing `(scope,key)` → `ContextKeyCollision{scope,key}` (loud). Fan-out children write distinct `result.<i>` keys, so collisions signal a real bug.
- **Determinism:** the fold rebuilds the store from `ContextWrite` events in `Seq` order; distinct keys make concurrent-write interleaving irrelevant. **Secrets/tokens are never written** to the durable store — a documented invariant of the runtime (it never calls `put` with credential values); credential-shape *detection* inside the store is out of scope.

## 5. ContentStore / CAS (§7.4)

### 5.1 Split by threshold + content-addressing
```rust
struct Digest(String);                            // sha256 hex of the content
struct ContentRef { digest: Digest, size: usize, summary: Option<String> }
#[async_trait] trait ContentStore {
    async fn put(&self, bytes: &[u8]) -> Result<Digest, OrchestratorError>;   // dedupes: same content → same digest
    async fn get(&self, d: &Digest) -> Result<Vec<u8>, OrchestratorError>;    // digest-miss → loud error, never empty
}
```
- An effect output (or context value) whose serialized size **> `cas_threshold`** (default e.g. 4 KiB) is `put` into the `ContentStore`; the journal's `EffectRecorded`/`ContextWrite` carries a **`ContentRef`** instead of the inline value. Small outputs stay **inline** (slice-1/2 behavior). Identical content dedupes to one digest.
- **The fold never deserializes blobs** — it reads refs (digest/size/summary); a node re-reads content lazily via `ContentStore::get`. A digest-miss on read is a loud error (no silent empty).

### 5.2 Snapshots
- After each **round** (§3.2) the executor writes `SnapshotWritten{seq}` + a `Snapshot { completed, skipped, memo_digests, context_refs, map_manifests }` to the store (keyed by `RunId`, latest wins).
- **Resume** = load the latest `Snapshot` + replay only the journal tail (events with `Seq >` the snapshot's) → bounds fold cost for wide/long runs. A run with no snapshot folds from the start (slice-1/2 path).

### 5.3 Compaction
- Once a `Map`'s children are all terminal **and** its `Consolidate` is `Completed`, the per-child journal records collapse to `{ index, status, digest }` — the full child transcript remains retrievable from CAS by digest but leaves the hot fold path. Compaction is journaled (auditable) and **never** drops a digest (content stays addressable). *(Per-child `cost` in the compacted record — master §7.4 — is deferred: the orchestrator has no cost plumbing from the gateway's `estimated_cost`/`actual_cost` yet; adding it is a later, additive change.)*

## 6. Determinism · resume · no-silent-failures

- **Determinism under concurrency:** effect-ids are **structural** (child index / turn), the memo is **effect-id-keyed**, `Map`/`Consolidate` aggregation is **order-independent**, and blackboard keys are **distinct** — so concurrent `Seq` interleaving never changes a memoized output or a final result. The determinism fence + version fence are unchanged.
- **Resume across fan-out:** folding (from the latest snapshot) rebuilds completed/skipped sets, the memo, the context store, and each `Map`'s child manifest; **completed children/agents memoize with zero re-spend** (the slice-1/2 headline extends across fan-out). A partial `Map` re-dispatches only its incomplete children.
- **No silent failures:** cascade-skip is journaled (`NodeSkipped`) and surfaced (`RunOutcome.skipped`); `Map` failures live in the manifest **and** surface; `Quorum`/`min_viable` shortfalls halt loud; blackboard collision + read-miss + CAS digest-miss are loud; journal writes stay strict.

## 7. Acceptance tests

1. **DAG behavior-preservation** — all slice-1/2 executor tests pass unchanged (linear = one ready node at a time; identical journal order).
2. **Map + BestEffort** — 5-item `Map`, 2 children scripted to fail → Map `Completed`, manifest `{ok:3, failed:2}`, results indexed correctly.
3. **Quorum** — `Quorum{min_fraction:0.6}` over 5 with 2 failures → `Completed`; with 3 failures → `Failed` (loud, manifest attached).
4. **Cascade-skip** — a node with a **hard** dep on a failed node is `NodeSkipped`; a node **soft**-depending on it still runs. Assert `RunOutcome.skipped`.
5. **Consolidate min-viable** — `Consolidate` over the survivors runs and synthesizes; below `min_viable` → `ConsolidateStarved` loud halt.
6. **Blackboard** — distinct-key writes resolve `Node→Run`; same `(scope,key)` twice → `ContextKeyCollision`; `get` miss → `Ok(None)`.
7. **CAS split + dedupe** — an over-threshold output is stored in the `ContentStore` (journal holds a `ContentRef`); two identical outputs share one digest; the fold reads refs without loading blobs.
8. **Snapshot resume (headline)** — a run that dies mid-fan-out resumes from the latest snapshot + tail and **re-spends nothing** for completed children (assert each completed child's effect-id appears in exactly one `EffectRecorded`, and the run-2 gateway is called only for the unfinished children).
9. **Compaction** — after `Consolidate`, the `Map`'s per-child records are collapsed to `{index,status,digest}` (assert the compacted shape; content still fetchable by digest).
10. **Real e2e** — a `Map{ body: Agent("researcher"), over: [3 items], aggregation: BestEffort }` → `Consolidate` over the results, driving the demo-catalog gateway; each child falls over the cloud entries to `llama3.1-local`; `Consolidate`'s agent synthesizes. Proves fan-out of real agents through a real reference chain end-to-end.

## 8. Design boundaries

- **Additive / SRP:** slice-1/2 `ModelCall`/`Agent` paths are behavior-preserved (linear scheduling is byte-identical); the gateway/kernel/catalog are untouched; the DAG scheduler, blackboard, and CAS are new orchestrator/store code.
- **Config-driven / no persistence:** the `ContextStore`/`ContentStore`/snapshot stores are in-memory (`Arc`-shared, the crash/resume seam); `PostgresJournal` + persistent CAS stay a separate held-off layer implementing these same traits.
- **Pure-only tools still** (Observation/Mutation → slice 4): a `Map`/`Consolidate` body is an `Agent` (Pure ReAct loop) or a `ModelCall`; large *outputs* exercise CAS via the size threshold (the big-Observation driver arrives in slice 4).
- Slice 3 is the **fan-out walking skeleton**: it proves partial-failure-tolerant parallelism + a durable blackboard/CAS in isolation before planner loops, mutations, and persistence layer on.
