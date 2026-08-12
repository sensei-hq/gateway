# SP-1 slice 4 — Observation · Mutation · two-phase · in-doubt→reconcile (design)

**Status:** brainstorm approved (2026-08-10).
**Master design:** `docs/superpowers/specs/2026-08-06-sensei-orchestrator-design.md` (§7.1 effect classes, §7.2 effect_id, §7.3 two-phase + `in_doubt→reconcile`, §7.4 journal/CAS split, §7.6 journal trait, §11 no-silent-failures).
**Predecessors:** slice 1 (durable spine), slice 2 (agent ReAct runtime), slice 3 (fan-out · blackboard · CAS) — all merged to `develop`.

## 1. Goal

Turn the **Pure-effects-only** executor into a real agentic processor: agents that **read the world** (`Observation`) and **change it safely** (`Mutation`). This is the effect taxonomy (§7.1) — the durability invariant slices 1–3 deferred.

Today every tool call is `Pure` (memoize by input-hash; replay identically on resume). The `EffectClass::{Pure, Observation, Mutation}` types exist, but the tool runtime rejects non-Pure with `ToolEffectDeferred`, and the journal has no `EffectIntent`. Slice 4 builds the two missing execution paths.

## 2. Scope (the D8 walking-skeleton cut)

Per master **D8** ("deep-research mini: fan-out Observation effects…; Mutation/two-phase ship as core types, unit-tested"):

- **Observation — fully executable.** Memoize with **TTL + provenance** `{source, fetched_at, content_hash}`; **re-read past TTL**; staleness is journaled, never silent.
- **Mutation — the two-phase mechanism + in-doubt detection + reconcile as a trait.** `EffectIntent → side effect → EffectRecorded`; on resume, `Intent`-without-`Recorded` = **in-doubt** → a `ReconcileProvider` decides. A **test/stub provider** ships; **real reconcile providers, idempotency-key providers, saga/compensation, sandbox + workspace isolation, secret redaction → SP-4**.
- **Demo/fake tools only** (deterministic, in-memory). Real `fs`/`web`/`shell` tools + permission/sandbox model → SP-4. Full HITL signal delivery (`AwaitSignal`/`HumanGate` mailbox) → SP-6; slice 4 emits a durable `RunPaused` an operator resolves out-of-band.
- **Additive / behavior-preserving:** the Pure path (slices 1–3) is byte-identical; the DAG scheduler, fan-out, blackboard/CAS, snapshots, and compaction are untouched.

## 3. Approach

**Dispatch by `EffectClass` in the tool-execution path.** Tools execute in exactly one place — `run_agent_tools` (executor/agent.rs). Extend it to switch on `tool.spec().effect_class`:
- `Pure` → today's memoize path (unchanged).
- `Observation` → memoize-with-TTL (clock-gated; re-read when stale).
- `Mutation` → two-phase, with in-doubt reconcile on resume.

The per-effect logic is factored into a helper (`execute_tool_effect`) so a future standalone `Tool` node (SP-3) reuses it without re-implementing the two-phase/TTL rules.

## 4. Crate additions

| Crate | Additions |
|---|---|
| `sensei-orchestrator-core` | `journal.rs`: `JournalEvent::EffectIntent { node, effect_id, idempotency_key, args_hash, seq }`; `ObservationMeta { fetched_at: DateTime<Utc>, ttl_secs: u64, source: String }` carried by `EffectRecorded` (`observation: Option<ObservationMeta>`, `None` for Pure/Mutation). `effect.rs`/`registry.rs`: `ToolSpec.ttl: Option<u64>` (Observation TTL seconds), `ToolSpec.source: Option<String>`. `clock.rs`: `Clock` trait + `SystemClock`. `reconcile.rs`: `ReconcileOutcome { Confirmed(serde_json::Value), NotApplied, Indeterminate }`, `ReconcileProvider` trait, `idempotency_key(effect_id, args_hash) -> String`. |
| `sensei-orchestrator` | `executor/agent.rs`: dispatch `run_agent_tools` by effect class; `execute_tool_effect` helper (Pure/Observation/Mutation). `executor/mod.rs`: `Executor.clock: Arc<dyn Clock>` (`with_clock`, default `SystemClock`), `Executor.reconcilers: Arc<ReconcileRegistry>` (`with_reconcilers`). `Fold` gains `intents: HashSet<EffectId>`. `RunOutcome.paused: Option<PauseInfo>`. `agent/tools.rs`: allow Observation/Mutation classes (drop the `ToolEffectDeferred` gate); demo `Search` (Observation) + `RecordNote` (Mutation) tools; a `ReconcileRegistry` (name → `Arc<dyn ReconcileProvider>`). |

No new crate; the 3-crate split holds. `PostgresJournal`/persistence stays a held-off layer (SP-DATA).

## 5. Core types

### 5.1 Two-phase journaling (§7.3)
```rust
// New journal event, appended BEFORE a Mutation side effect (fsync-semantics; in-mem now).
JournalEvent::EffectIntent { node: NodeId, effect_id: EffectId, idempotency_key: String, args_hash: String, seq: Seq }
```
The idempotency key defaults to `sha256(effect_id ‖ args_hash)` (deterministic, automatic — same effect + same args ⇒ same key). A Mutation tool MAY override via a provider hook; author-supplied keys land with real providers in SP-4.

### 5.2 Clock seam (deterministic TTL)
```rust
pub trait Clock: Send + Sync { fn now(&self) -> chrono::DateTime<chrono::Utc>; }
pub struct SystemClock;               // default
// tests inject a FixedClock/AdvanceableClock in the orchestrator crate.
```
`Executor::with_clock(Arc<dyn Clock>)` — default `SystemClock`. **Pure effects are unaffected and stay strictly deterministic.** Only Observation freshness and provenance `fetched_at` read the clock, so resume behavior is a pure function of `(journal, clock)`.

### 5.3 Observation provenance + TTL (§7.1)
`EffectRecorded` gains `observation: Option<ObservationMeta>` where
```rust
ObservationMeta { fetched_at: DateTime<Utc>, ttl_secs: u64, source: String }
```
`content_hash` (the third element of the §7.1 provenance triple) = `sha256` of the recorded output value — the CAS digest when the output is split, computed inline otherwise — so it is derived, not a separate `ObservationMeta` field. `ttl`/`source` come from `ToolSpec`.

### 5.4 Reconcile (§7.3)
```rust
pub enum ReconcileOutcome { Confirmed(serde_json::Value), NotApplied, Indeterminate }
#[async_trait] pub trait ReconcileProvider: Send + Sync {
    async fn reconcile(&self, idempotency_key: &str, args: &serde_json::Value) -> Result<ReconcileOutcome, OrchestratorError>;
}
```
Registered per-tool in a `ReconcileRegistry` (name → provider), a sibling of `ToolRegistry`. A Mutation tool without a registered provider defaults to `Indeterminate` (fail-safe: pause rather than guess).

### 5.5 Pause outcome
`RunOutcome.paused: Option<PauseInfo { node: NodeId, reason: String }>`. The in-doubt halt emits `JournalEvent::RunPaused { reason, resume_after: None }` and returns with `paused` set and **no `RunCompleted`** (the run stays resumable). Full HITL signal plumbing is SP-6; here an operator resolves the intent out-of-band and re-runs.

## 6. Executor mechanics

### 6.1 Observation
- **Live:** execute the tool → output; `EffectRecorded { …, observation: Some(ObservationMeta{ fetched_at: clock.now(), ttl_secs, source }) }`.
- **Resume (memo hit):** input-hash determinism fence first (as today). Then freshness: if `clock.now() ≤ fetched_at + ttl` → **replay memoized**. Else → **re-read** (execute live again; append a fresh `EffectRecorded` that supersedes; the re-read is itself journaled — **staleness is never silent**). A `ttl_secs == 0` (or `ttl: None`) Observation always re-reads on resume (opt-out of memoization).

### 6.2 Mutation
- **Live:** append `EffectIntent{effect_id, idempotency_key, args_hash}` → execute the tool (side effect) → append `EffectRecorded{output}`.
- **Resume:**
  - `Intent` **and** `Recorded` present ⇒ completed ⇒ **memoize** the output (safe — it finished).
  - `Intent` **without** `Recorded` ⇒ **in-doubt** ⇒ do NOT execute; call the tool's `ReconcileProvider`:
    - `Confirmed(output)` ⇒ append `EffectRecorded` (memoize), continue.
    - `NotApplied` ⇒ re-run: the existing `Intent` still stands (same idempotency key), so just execute the side effect and append `EffectRecorded` (the world is unchanged; safe to apply).
    - `Indeterminate` ⇒ emit `RunPaused{InDoubt}`; return `paused`; no `RunCompleted`; **never blind-apply or blind-memoize.**
  - No `Intent` ⇒ never ran ⇒ execute fresh.

### 6.3 Fold
`Fold` gains `intents: HashSet<EffectId>` (from `EffectIntent` events). In-doubt = `effect_id ∈ intents` and `effect_id ∉ memo` (no `Recorded`). Folded in `Seq` order alongside the existing memo/started/completed.

## 7. Determinism · resume · no-silent-failures

- **Pure** — strictly deterministic replay (unchanged). **Observation** — deterministic given `(journal, clock)`; a re-read past TTL is journaled and input-hash-fenced. **Mutation** — never blindly replayed or re-run: completed → memoize, in-doubt → reconcile, else → fresh two-phase.
- **No silent failures:** two-phase intent-before-effect; in-doubt → reconcile or loud `RunPaused` (never guess); staleness journaled; every reconcile outcome is explicit; a missing reconcile provider is fail-safe `Indeterminate` (pause), not assume-applied. Journal writes stay strict.
- **CAS/snapshot/compaction:** Observation/Mutation outputs flow through the existing `split_output`/`EffectOutput` CAS split and snapshot machinery unchanged. (Compaction stays ModelCall-Map-only, as in slice 3.)

## 8. Demo tools + acceptance

**Demo tools** (fake, deterministic; real I/O + sandbox → SP-4):
- `Search` (`Observation`, `ttl_secs` configurable) — returns canned results keyed by query.
- `RecordNote` (`Mutation`) — appends to an in-memory sink (the "side effect"); its test `ReconcileProvider` queries the sink by idempotency key.

**Acceptance tests:**
1. **Observation within TTL** — resume replays the memoized read with **no re-execution** (call-counter proves zero re-reads).
2. **Observation past TTL** — advance the injected clock beyond `fetched_at+ttl`; resume **re-reads** (one new execution), fresh provenance, staleness journaled.
3. **Mutation happy path** — `EffectIntent→EffectRecorded` journaled in order; resume with `Recorded` present **memoizes** (side effect applied exactly once; sink asserted).
4. **In-doubt → Confirmed** — crash after `Intent`, before `Recorded`; resume reconciles `Confirmed` ⇒ memoized, **side effect NOT repeated** (sink applied exactly once).
5. **In-doubt → NotApplied** — resume reconciles `NotApplied` ⇒ re-runs the mutation (sink applied exactly once total).
6. **In-doubt → Indeterminate → RunPaused** — resume reconciles `Indeterminate` (or no provider) ⇒ `RunPaused{InDoubt}`, `outcome.paused` set, **no `RunCompleted`**, sink **not** applied.
7. **No-silent-failure** assertions on every branch; a determinism-violation on a changed Observation/Mutation input still halts loud.
8. **Real-gateway e2e** — a `Map{ body: Agent }` whose agent calls the `Search` Observation over the demo reference chain, `Quorum`-aggregated → `Consolidate`, plus one `RecordNote` Mutation node — proving Observation fan-out + Mutation two-phase + resume in one run through the real gateway.

## 9. Design boundaries (deferred, stated)

- **SP-4:** real reconcile providers (query-by-idempotency-key against real services), author-supplied idempotency keys, saga/compensation, tool permission model + sandbox + workspace isolation, secret redaction, real `fs`/`shell`/`web` Mutation tools.
- **SP-6:** `AwaitSignal`/`HumanGate` + signal-mailbox delivery + pause-expiry; slice 4 emits a durable `RunPaused` resolved out-of-band.
- **SP-DATA:** `PostgresJournal` + persistent CAS (the two-phase `EffectIntent` fsync is in-memory here). Snapshots exclude in-doubt reconstruction beyond the tail (unchanged from slice 3).
- **Not in slice 4:** `Loop`/`Subgraph`/`Branch` nodes, `OrchestratorHooks`, quota→pause wiring, blackboard executor-wiring — separate SP-1 gaps tracked independently.
