# SP-1 Slice 4 — Observation · Mutation · Two-Phase Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the Pure-only executor with the full effect taxonomy — `Observation` (memoize with TTL + provenance, re-read when stale) and `Mutation` (two-phase `Intent→effect→Recorded` with `in_doubt→reconcile`) — per the D8 walking-skeleton cut.

**Architecture:** Dispatch by `EffectClass` in the one place tools execute (`run_agent_tools`), factored into `execute_tool_effect`. An injected `Clock` gates Observation freshness; a `ReconcileRegistry` resolves in-doubt Mutations on resume; a durable `RunPaused` halts on `Indeterminate`. Pure path stays byte-identical.

**Tech Stack:** Rust (workspace crates `sensei-orchestrator-core`, `sensei-orchestrator`, `sensei-orchestrator-store`), `chrono`, `sha2`, `async-trait`, `tokio`, `serde_json`.

**Spec:** `docs/superpowers/specs/2026-08-10-sp1-slice4-observation-mutation-design.md`.

**Conventions (every task):** `cargo fmt --all` before each commit (pre-commit hook = fmt-check + `cargo clippy --workspace --all-targets -- -D warnings`). Verify test outcomes by the real command exit code, never a piped `| tail`. TDD: watch each test fail before implementing.

---

## File structure

| File | Responsibility |
|---|---|
| `crates/orchestrator-core/src/clock.rs` (new) | `Clock` trait + `SystemClock` |
| `crates/orchestrator-core/src/reconcile.rs` (new) | `ReconcileOutcome`, `ReconcileProvider`, `idempotency_key` |
| `crates/orchestrator-core/src/journal.rs` (mod) | `EffectIntent` event; `ObservationMeta`; `EffectRecorded.observation` |
| `crates/orchestrator-core/src/registry.rs` (mod) | `ToolSpec.ttl` + `ToolSpec.source` |
| `crates/orchestrator-core/src/lib.rs` (mod) | re-exports |
| `crates/orchestrator/src/executor/mod.rs` (mod) | `Executor.clock`/`reconcilers` + builders; `Fold.intents`; `RunOutcome.paused`; `NodeExec::Paused`; `drive` pause handling |
| `crates/orchestrator/src/executor/agent.rs` (mod) | `execute_tool_effect` dispatch; Observation/Mutation paths; `AgentStep::Paused` |
| `crates/orchestrator/src/executor/support.rs` (mod) | `fold_journal` folds `EffectIntent`; `content_hash` helper |
| `crates/orchestrator/src/agent/tools.rs` (mod) | drop non-Pure gate; `ReconcileRegistry`; demo `Search`/`RecordNote` |
| `crates/orchestrator/src/executor/tests.rs` (mod) | acceptance tests |

---

## Task 1: `Clock` trait (core)

**Files:**
- Create: `crates/orchestrator-core/src/clock.rs`
- Modify: `crates/orchestrator-core/src/lib.rs`

- [ ] **Step 1: Write the failing test** in `crates/orchestrator-core/src/clock.rs`

```rust
//! The wall-clock seam (§7.1). Only Observation freshness + provenance read it,
//! so a resume is a pure function of `(journal, clock)`; Pure effects never do.

use chrono::{DateTime, Utc};

/// A source of "now". Injected into the executor so tests can control TTL time.
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

/// The default clock — real wall time.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_advances() {
        let c = SystemClock;
        let a = c.now();
        let b = c.now();
        assert!(b >= a, "monotonic non-decreasing");
    }
}
```

- [ ] **Step 2: Wire the module.** In `crates/orchestrator-core/src/lib.rs` add `pub mod clock;` and to the re-export block add `pub use clock::{Clock, SystemClock};`.

- [ ] **Step 3: Run & verify**

Run: `cargo test -p sensei-orchestrator-core clock 2>&1 | grep "test result"`
Expected: `test result: ok. 1 passed`

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-core/src/clock.rs crates/orchestrator-core/src/lib.rs
git commit -m "feat(orchestrator-core): Clock trait + SystemClock (slice 4)"
```

---

## Task 2: `EffectIntent` event + `ObservationMeta` on `EffectRecorded` (core)

**Files:**
- Modify: `crates/orchestrator-core/src/journal.rs`, `crates/orchestrator-core/src/lib.rs`

- [ ] **Step 1: Write the failing roundtrip test.** In `journal.rs` `mod tests`, add:

```rust
#[test]
fn effect_intent_and_observation_meta_roundtrip() {
    let intent = JournalEvent::EffectIntent {
        node: NodeId("n1".into()),
        effect_id: effect_id("n1", 0, 1),
        idempotency_key: "k".into(),
        args_hash: "h".into(),
        seq: 0,
    };
    let s = serde_json::to_string(&intent).unwrap();
    assert!(matches!(
        serde_json::from_str::<JournalEvent>(&s).unwrap(),
        JournalEvent::EffectIntent { .. }
    ));

    let obs = ObservationMeta {
        fetched_at: chrono::Utc::now(),
        ttl_secs: 60,
        source: "search".into(),
    };
    let rec = JournalEvent::EffectRecorded {
        node: NodeId("n1".into()),
        effect_id: effect_id("n1", 0, 1),
        class: EffectClass::Observation,
        input_hash: "h".into(),
        seq: 0,
        output: EffectOutput::Inline(serde_json::json!({"x":1})),
        observation: Some(obs),
    };
    assert!(matches!(
        serde_json::from_str::<JournalEvent>(&serde_json::to_string(&rec).unwrap()).unwrap(),
        JournalEvent::EffectRecorded { observation: Some(_), .. }
    ));
}
```

- [ ] **Step 2: Run to confirm it fails**

Run: `cargo test -p sensei-orchestrator-core effect_intent_and_observation 2>&1 | grep -E "error|test result"`
Expected: compile error — `ObservationMeta` and the `observation` field / `EffectIntent` variant don't exist.

- [ ] **Step 3: Add the types.** In `journal.rs`, add above `JournalEvent`:

```rust
/// Provenance + freshness of an `Observation` effect (§7.1). `content_hash` (the
/// third provenance element) is derived — the recorded output's digest — not stored.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObservationMeta {
    pub fetched_at: chrono::DateTime<chrono::Utc>,
    pub ttl_secs: u64,
    pub source: String,
}
```

Add the `observation` field to the `EffectRecorded` variant (after `output`):

```rust
        /// Set only for `Observation` effects (§7.1): freshness + provenance so a
        /// resume can decide replay-vs-re-read. `None` for Pure/Mutation.
        observation: Option<ObservationMeta>,
```

Add the new variant after `EffectRecorded`:

```rust
    /// The intent phase of a two-phase `Mutation` (§7.3), appended BEFORE the side
    /// effect. On resume an `EffectIntent` with no matching `EffectRecorded` is
    /// IN-DOUBT → reconcile, never blind re-run or blind memoize.
    EffectIntent {
        node: NodeId,
        effect_id: EffectId,
        idempotency_key: String,
        args_hash: String,
        seq: Seq,
    },
```

- [ ] **Step 4: Fix the existing roundtrip test + re-export.** In the existing `journal_event_roundtrips` test, add `observation: None,` to its `EffectRecorded`. In `lib.rs` re-export block, add `ObservationMeta` to the `journal::{...}` re-export.

- [ ] **Step 5: Fix every other `EffectRecorded` construction site** (the compiler lists them). Add `observation: None,` at each. Sites: `crates/orchestrator/src/executor/agent.rs` (`dispatch_model_turn`, `run_agent_tools`), `executor/fanout.rs` (`run_consolidate`, `run_map_child_modelcall`), `executor/mod.rs` (`run_node` ModelCall), and any test that builds `EffectRecorded` (`executor/tests.rs` `start_halts_on_determinism_violation…`).

- [ ] **Step 6: Run all core + orchestrator tests**

Run: `cargo test -p sensei-orchestrator-core -p sensei-orchestrator > /tmp/t.log 2>&1; echo "EXIT=$?"; grep "test result" /tmp/t.log`
Expected: `EXIT=0`, all green (Pure path unchanged — `observation: None` everywhere).

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat(orchestrator-core): EffectIntent event + ObservationMeta on EffectRecorded (slice 4)"
```

---

## Task 3: Reconcile types (core)

**Files:**
- Create: `crates/orchestrator-core/src/reconcile.rs`
- Modify: `crates/orchestrator-core/src/lib.rs`

- [ ] **Step 1: Write the file with a failing test** in `reconcile.rs`:

```rust
//! Reconciliation of an in-doubt `Mutation` on resume (§7.3): decide whether the
//! side effect already applied, so the executor never blind-re-runs or blind-memoizes.

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::effect::EffectId;
use crate::error::OrchestratorError;

/// The verdict of reconciling an in-doubt Mutation against the world.
#[derive(Debug, Clone, PartialEq)]
pub enum ReconcileOutcome {
    /// It DID apply — here is the recorded output; memoize it, do not re-run.
    Confirmed(serde_json::Value),
    /// It did NOT apply — the world is unchanged; safe to run the effect now.
    NotApplied,
    /// Cannot determine — the executor must pause loud (never guess).
    Indeterminate,
}

/// A per-tool reconciler queried when a Mutation is in-doubt on resume.
#[async_trait]
pub trait ReconcileProvider: Send + Sync {
    async fn reconcile(
        &self,
        idempotency_key: &str,
        args: &serde_json::Value,
    ) -> Result<ReconcileOutcome, OrchestratorError>;
}

/// The default idempotency key for a Mutation effect: `sha256(effect_id | args_hash)`
/// — deterministic, so a resumed intent maps to the same key.
pub fn idempotency_key(effect_id: &EffectId, args_hash: &str) -> String {
    let mut h = Sha256::new();
    h.update(format!("{}|{}", effect_id.0, args_hash).as_bytes());
    format!("{:x}", h.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effect::effect_id;

    #[test]
    fn idempotency_key_is_stable_and_input_bound() {
        let e = effect_id("n", 0, 1);
        assert_eq!(idempotency_key(&e, "a"), idempotency_key(&e, "a"));
        assert_ne!(idempotency_key(&e, "a"), idempotency_key(&e, "b"));
    }
}
```

> Note: confirm `EffectId` is a newtype with a public `.0: String` field (it is — used in `effect_id`). If not, use its accessor.

- [ ] **Step 2: Wire the module.** In `lib.rs`: `pub mod reconcile;` and re-export `pub use reconcile::{ReconcileOutcome, ReconcileProvider, idempotency_key};`.

- [ ] **Step 3: Run & verify**

Run: `cargo test -p sensei-orchestrator-core reconcile 2>&1 | grep "test result"`
Expected: `test result: ok. 1 passed`

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/orchestrator-core/src/reconcile.rs crates/orchestrator-core/src/lib.rs
git commit -m "feat(orchestrator-core): ReconcileOutcome/Provider + idempotency_key (slice 4)"
```

---

## Task 4: `ToolSpec` gains `ttl` + `source` (core)

**Files:**
- Modify: `crates/orchestrator-core/src/registry.rs`

- [ ] **Step 1: Add the fields.** In `ToolSpec`, add after `effect_class`:

```rust
    /// TTL (seconds) for an `Observation` tool's memoized read; `None` = never
    /// memoize (always re-read on resume). Ignored for Pure/Mutation.
    pub ttl_secs: Option<u64>,
    /// Provenance `source` label recorded with an Observation. Defaults to the tool name.
    pub source: Option<String>,
```

- [ ] **Step 2: Fix construction sites.** The compiler lists `ToolSpec { .. }` literals (in `registry.rs` tests, `crates/orchestrator/src/agent/tools.rs` `Calc::spec` + the test `Reader`/`Observation` specs, `agent/prompt.rs` tests). Add `ttl_secs: None, source: None,` to each Pure/existing spec.

- [ ] **Step 3: Run core + orchestrator**

Run: `cargo test -p sensei-orchestrator-core -p sensei-orchestrator > /tmp/t.log 2>&1; echo "EXIT=$?"; grep -c "test result: ok" /tmp/t.log`
Expected: `EXIT=0`.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat(orchestrator-core): ToolSpec.ttl_secs + source for Observation (slice 4)"
```

---

## Task 5: Executor `clock` + `reconcilers`; allow non-Pure tools

**Files:**
- Modify: `crates/orchestrator/src/executor/mod.rs`, `crates/orchestrator/src/agent/tools.rs`

- [ ] **Step 1: `ReconcileRegistry` + drop the gate.** In `agent/tools.rs`: add a registry and stop rejecting non-Pure. Replace the `ToolRegistry::execute` gate so it executes ANY class (the two-phase/TTL wrapping is the executor's job — `execute` just runs the tool). Add:

```rust
/// Name → reconcile provider, queried when a Mutation is in-doubt on resume.
#[derive(Default, Clone)]
pub struct ReconcileRegistry {
    providers: std::collections::HashMap<String, std::sync::Arc<dyn orchestrator_core::ReconcileProvider>>,
}
impl ReconcileRegistry {
    pub fn with_provider(mut self, name: impl Into<String>, p: std::sync::Arc<dyn orchestrator_core::ReconcileProvider>) -> Self {
        self.providers.insert(name.into(), p);
        self
    }
    pub fn get(&self, name: &str) -> Option<&std::sync::Arc<dyn orchestrator_core::ReconcileProvider>> {
        self.providers.get(name)
    }
}
```

In `ToolRegistry::execute`, DELETE the `if class != Pure { return Err(ToolEffectDeferred) }` block — it now runs any class. Keep the unknown-tool loud error. Update `Tool` trait doc (remove "MUST be Pure").

- [ ] **Step 2: Executor fields + builders.** In `executor/mod.rs`, add to `Executor`:

```rust
    clock: std::sync::Arc<dyn orchestrator_core::Clock>,
    reconcilers: std::sync::Arc<crate::agent::tools::ReconcileRegistry>,
```

In `new`, init `clock: std::sync::Arc::new(orchestrator_core::SystemClock)` and `reconcilers: std::sync::Arc::new(Default::default())`. Add builders:

```rust
    pub fn with_clock(mut self, clock: std::sync::Arc<dyn orchestrator_core::Clock>) -> Self { self.clock = clock; self }
    pub fn with_reconcilers(mut self, r: std::sync::Arc<crate::agent::tools::ReconcileRegistry>) -> Self { self.reconcilers = r; self }
```

- [ ] **Step 3: Test the gate is gone.** In `agent/tools.rs` `mod tests`, replace the old `agent_rejects_a_non_pure_tool_loudly` assertion at the ToolRegistry level with a test that a non-Pure tool now EXECUTES:

```rust
#[test]
fn tool_registry_executes_a_non_pure_tool() {
    struct Obs;
    impl Tool for Obs {
        fn spec(&self) -> ToolSpec { ToolSpec { name: "obs".into(), description: None, input_schema: serde_json::json!({}), effect_class: EffectClass::Observation, ttl_secs: Some(60), source: None } }
        fn call(&self, _a: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> { Ok(serde_json::json!({"ok": true})) }
    }
    let reg = ToolRegistry::default().with_tool(std::sync::Arc::new(Obs));
    assert_eq!(reg.execute("obs", serde_json::json!({})).unwrap(), serde_json::json!({"ok": true}));
}
```

> The executor-level test `agent_rejects_a_non_pure_tool_loudly` in `executor/tests.rs` asserts the OLD behavior — it will be replaced by the Observation/Mutation acceptance tests (Task 11). For now, update it to expect success or delete it; note the change in the commit.

- [ ] **Step 4: Run**

Run: `cargo test -p sensei-orchestrator > /tmp/t.log 2>&1; echo "EXIT=$?"; grep "test result" /tmp/t.log`
Expected: `EXIT=0` (fix/replace the one executor test that asserted the deferral).

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat(orchestrator): Executor clock + reconcilers; allow non-Pure tools (slice 4)"
```

---

## Task 6: `Fold.intents`, `RunOutcome.paused`, `NodeExec::Paused` (pause propagation)

**Files:**
- Modify: `executor/mod.rs`, `executor/support.rs`, `executor/agent.rs`

- [ ] **Step 1: Types.** In `executor/mod.rs`:
  - Add to `Fold`: `intents: std::collections::HashSet<EffectId>,`.
  - Add to `RunOutcome`: `pub paused: Option<PauseInfo>,` and define:

```rust
/// A durable pause (§7.3): the run halted resumable (no `RunCompleted`) — e.g. an
/// in-doubt Mutation whose reconcile was Indeterminate.
#[derive(Debug, Clone)]
pub struct PauseInfo {
    pub node: NodeId,
    pub reason: String,
}
```
  - Add a `NodeExec::Paused { node: NodeId, reason: String }` variant.
  - Add an `AgentStep::Paused { reason: String }` variant (in `executor/mod.rs` where `AgentStep` is defined).

- [ ] **Step 2: Fold `EffectIntent`.** In `executor/support.rs` `fold_journal`, add an arm:

```rust
            JournalEvent::EffectIntent { effect_id, .. } => {
                fold.intents.insert(effect_id.clone());
            }
```
Also add a `pub(crate) fn content_hash(value: &serde_json::Value) -> String` here (sha256 hex of `serde_json::to_vec(value)`), for Observation provenance.

- [ ] **Step 3: Propagate Paused through dispatch.** In `executor/mod.rs` `run_node`, the `NodeKind::Agent` arm maps `AgentStep::Paused { reason }` → `NodeExec::Paused { node: node.id.clone(), reason }`. Same in `fanout.rs` `run_map` child + `run_consolidate` Agent arms (a paused child/consolidate propagates as `NodeExec::Paused`).

- [ ] **Step 4: `drive` handles Paused.** In `executor/mod.rs` `apply_node_result`, add a `NodeExec::Paused { node, reason }` arm: set `state.outcome.paused = Some(PauseInfo { node, reason })`, mark the node terminal, and DO NOT cascade. In `drive`, after the loop, suppress `RunCompleted` when `state.outcome.paused.is_some()` (same as `failed`): change the guard to `if state.outcome.failed.is_none() && state.outcome.paused.is_none()`.

- [ ] **Step 5: Compile-only checkpoint** (no behavior yet — Paused is unreachable until Task 9).

Run: `cargo build -p sensei-orchestrator 2>&1 | grep -E "^error" | head; echo done`
Expected: no errors (add `observation: None` / exhaustive-match arms as the compiler demands).

- [ ] **Step 6: Run existing tests (behavior-preserving)**

Run: `cargo test -p sensei-orchestrator > /tmp/t.log 2>&1; echo "EXIT=$?"; grep "test result" /tmp/t.log`
Expected: `EXIT=0`.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat(orchestrator): Fold.intents + RunOutcome.paused + NodeExec/AgentStep::Paused plumbing (slice 4)"
```

---

## Task 7: Observation execution (TTL memo / re-read)

**Files:**
- Modify: `executor/agent.rs` (`run_agent_tools` → dispatch by class)

- [ ] **Step 1: Write the failing test** in `executor/tests.rs` (`mod tests`). Uses an in-orchestrator `AdvanceableClock` test helper (define it in `tests.rs`) and a demo `Search` tool (Task 10 defines the real one; for this task, a local counting Observation tool inline). Assert: run once (records with `fetched_at`), resume with a fresh clock → **replays** (0 re-executions); resume with clock advanced past TTL → **re-reads** (1 more execution).

```rust
// (concrete test: builds an Agent whose one tool call is an Observation with
// ttl_secs=60; run 1 records it; resume via start() with the SAME journal and a
// FixedClock < fetched_at+60 → the counting tool is NOT called again; a second
// resume with clock > fetched_at+60 → the tool IS called once more, and a new
// EffectRecorded supersedes. Full code written against the Task-10 Search tool.)
```

- [ ] **Step 2: Run to confirm failure** (Observation currently memoizes like Pure — no TTL re-read).

Run: `cargo test -p sensei-orchestrator observation_ 2>&1 | grep -E "FAILED|test result"`
Expected: FAIL (re-read past TTL does not happen yet).

- [ ] **Step 3: Implement `execute_tool_effect` dispatch.** In `run_agent_tools`, replace the per-tool body with a call to a new helper `self.execute_tool_effect(ar, teid, call, &args).await?` that switches on `self.tools.spec_of(&call.name).effect_class` (add a `spec_of` accessor to `ToolRegistry`). Pure arm = current logic (memo-hit replay / execute+record with `observation: None`). Observation arm:
  - memo hit → determinism-fence on `tih`; then read the recorded `ObservationMeta`: if `self.clock.now() <= fetched_at + Duration::seconds(ttl_secs)` → replay materialized output; else fall through to a live re-read.
  - live (miss or stale) → execute tool → `let output = ...; let meta = ObservationMeta { fetched_at: self.clock.now(), ttl_secs, source }`; `split_output`; append `EffectRecorded { …, class: Observation, observation: Some(meta) }`.
  - `ttl_secs == None` → treat as always-stale (never replay).

> The `ObservationMeta` for a memo hit must be reachable: extend `Fold.memo`'s value to carry it, OR fold a side map `observations: HashMap<EffectId, ObservationMeta>` in `fold_journal` (recommended — keeps `memo` unchanged). Add `Fold.observations` and populate it from `EffectRecorded.observation`.

- [ ] **Step 4: Run to verify pass**

Run: `cargo test -p sensei-orchestrator observation_ > /tmp/t.log 2>&1; echo "EXIT=$?"; grep "test result" /tmp/t.log`
Expected: `EXIT=0`.

- [ ] **Step 5: Full suite (Pure unaffected)**

Run: `cargo test -p sensei-orchestrator > /tmp/t.log 2>&1; echo "EXIT=$?"; grep -c "test result: ok" /tmp/t.log`
Expected: `EXIT=0`.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat(orchestrator): Observation tool effects — TTL memoize + re-read on staleness (slice 4)"
```

---

## Task 8: Mutation two-phase (live path)

**Files:**
- Modify: `executor/agent.rs` (`execute_tool_effect` Mutation arm)

- [ ] **Step 1: Failing test** in `executor/tests.rs`: a fresh run with a Mutation tool (`RecordNote`) journals `EffectIntent` **then** `EffectRecorded` for the tool effect (assert the exact order via `label`), and the side-effect sink is applied exactly once. (No resume yet.)

- [ ] **Step 2: Run to confirm failure** (Mutation currently records no Intent).

- [ ] **Step 3: Implement the Mutation arm** in `execute_tool_effect`:
  - `Intent`+`Recorded` present (memo hit) → replay materialized (safe — completed).
  - else if `teid ∈ ar.fold.intents` (Intent-without-Recorded) → **in-doubt path (Task 9)**.
  - else (never ran) → compute `key = idempotency_key(&teid, &tih)`; append `EffectIntent { node, effect_id: teid, idempotency_key: key, args_hash: tih }`; execute tool; `split_output`; append `EffectRecorded { class: Mutation, observation: None }`.

- [ ] **Step 4: Run to verify pass; then full suite.**

Run: `cargo test -p sensei-orchestrator mutation_two_phase > /tmp/t.log 2>&1; echo "EXIT=$?"; grep "test result" /tmp/t.log`
Expected: `EXIT=0`.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat(orchestrator): Mutation two-phase — EffectIntent → side effect → EffectRecorded (slice 4)"
```

---

## Task 9: In-doubt reconcile on resume + `RunPaused`

**Files:**
- Modify: `executor/agent.rs` (in-doubt branch), `executor/mod.rs` (emit `RunPaused`)

- [ ] **Step 1: Three failing tests** in `executor/tests.rs` — crash after `Intent` before `Recorded` (seed the journal directly, like `start_halts_on_determinism_violation…`), then resume:
  1. reconcile `Confirmed(v)` → `EffectRecorded` appended, tool NOT executed (sink applied once total), run completes.
  2. reconcile `NotApplied` → tool executed once (sink once total), `EffectRecorded` appended, completes.
  3. reconcile `Indeterminate` (or no provider) → `outcome.paused` is `Some`, journal ends without `RunCompleted`, sink NOT applied, a `RunPaused` event present.

- [ ] **Step 2: Run to confirm failure.**

- [ ] **Step 3: Implement the in-doubt branch** in `execute_tool_effect` Mutation arm: look up `self.reconcilers.get(&call.name)`; `None` ⇒ treat as `Indeterminate`. Call `reconcile(&key, &args)`:
  - `Confirmed(output)` → `split_output`; append `EffectRecorded { class: Mutation }`; use `output` as the tool result.
  - `NotApplied` → execute tool; append `EffectRecorded` (the standing Intent covers it).
  - `Indeterminate` → append `JournalEvent::RunPaused { reason: format!("mutation in-doubt: {key}"), resume_after: None }`; return the pause up the stack as `Ok(Paused(reason))` (extend `run_agent_tools`' inner result to a 3-way `Continue(Vec<Message>) | Failed(String) | Paused(String)`; `drive_agent` maps `Paused` → `AgentStep::Paused`).

  `key` here is recomputed as `idempotency_key(&teid, &tih)` (deterministic — matches the recorded Intent's key).

- [ ] **Step 4: Run the three tests; then full suite.**

Run: `cargo test -p sensei-orchestrator in_doubt > /tmp/t.log 2>&1; echo "EXIT=$?"; grep "test result" /tmp/t.log`
Expected: `EXIT=0`.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A
git commit -m "feat(orchestrator): in-doubt Mutation reconcile on resume (Confirmed/NotApplied/Indeterminate→RunPaused) (slice 4)"
```

---

## Task 10: Demo tools + test reconcile provider

**Files:**
- Modify: `crates/orchestrator/src/agent/tools.rs`

- [ ] **Step 1: Implement `Search` (Observation) and `RecordNote` (Mutation).** `Search`: `spec` with `effect_class: Observation, ttl_secs: Some(60), source: Some("search")`; `call` returns canned results for the query arg + increments a shared call counter (Arc<AtomicUsize>) exposed for tests. `RecordNote`: `effect_class: Mutation`; `call` appends the note arg to a shared `Arc<Mutex<Vec<String>>>` sink and returns `{"recorded": <note>}`. Add a `NoteReconciler { sink }` implementing `ReconcileProvider`: `Confirmed` if the sink already contains the note (keyed by idempotency args), else `NotApplied` — plus a configurable `AlwaysIndeterminate` provider for the pause test.

- [ ] **Step 2: Unit tests** for each tool's `call` (Search counts + returns canned; RecordNote appends to sink; NoteReconciler returns Confirmed/NotApplied correctly).

- [ ] **Step 3: Run & commit**

```bash
cargo test -p sensei-orchestrator agent::tools > /tmp/t.log 2>&1; echo "EXIT=$?"; grep "test result" /tmp/t.log
cargo fmt --all && git add -A && git commit -m "feat(orchestrator): demo Search/RecordNote tools + reconcile providers (slice 4)"
```

---

## Task 11: Wire the acceptance tests to the demo tools

**Files:**
- Modify: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Flesh out Tasks 7–9 tests** against the real `Search`/`RecordNote` tools + `AdvanceableClock` (a test `Clock` backed by `Arc<Mutex<DateTime<Utc>>>` with an `advance(secs)`). Ensure the six spec acceptance scenarios (§8.1–8.6) each have a test asserting exactly one behavior, and add the no-silent-failure assertion (a changed Observation/Mutation input on resume still halts with `DeterminismViolation`).

- [ ] **Step 2: Run the full orchestrator suite + clippy**

Run: `cargo test -p sensei-orchestrator > /tmp/t.log 2>&1; echo "EXIT=$?"; grep -c "test result: ok" /tmp/t.log`
Run: `cargo clippy -p sensei-orchestrator --all-targets -- -D warnings 2>&1 | tail -1; echo "CLIPPY=$?"`
Expected: `EXIT=0`, `CLIPPY=0`.

- [ ] **Step 3: Commit**

```bash
cargo fmt --all && git add -A && git commit -m "test(orchestrator): slice-4 acceptance — Observation TTL + Mutation two-phase + in-doubt (slice 4)"
```

---

## Task 12: Real-gateway e2e + docs + memory

**Files:**
- Modify: `crates/orchestrator/src/executor/tests.rs`, `docs/superpowers/…/orchestrator/*` (feature doc), memory index.

- [ ] **Step 1: e2e test** (§8.8): a `Map { body: Agent("researcher") }` whose agent's ReAct loop calls the `Search` Observation, `Quorum`-aggregated → `Consolidate`, plus one top-level `RecordNote` Mutation node — driven through the demo-catalog gateway (`demo_reference_gateway`) with an injected clock + reconcilers. Assert: Observations recorded with provenance, the Mutation two-phased, run completes.

- [ ] **Step 2: Full workspace gate**

Run: `cargo test --workspace > /tmp/ws.log 2>&1; echo "WS=$?"; grep -Eo "[0-9]+ passed" /tmp/ws.log | awk '{s+=$1} END{print s" passed"}'`
Run: `cargo clippy --workspace --all-targets -- -D warnings > /tmp/c.log 2>&1; echo "CLIPPY=$?"`
Expected: `WS=0`, `CLIPPY=0`.

- [ ] **Step 3: Docs + memory.** Flip the orchestrator feature doc for effect classes to `implemented` (Observation/Mutation-mechanism); note SP-4/SP-6 deferrals. Update the memory index (`MEMORY.md`) that slice 4 landed + the effect-taxonomy state.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all && git add -A && git commit -m "feat(orchestrator): SP-1 slice 4 real e2e Map(Observation)→Consolidate + Mutation; docs (slice 4 COMPLETE)"
```

---

## Notes for the implementer

- **Behavior-preservation gate:** after Tasks 2, 4, 6 the full slice-1/2/3 suite MUST stay green (Pure path byte-identical; `observation: None` everywhere). If a journal-order assertion breaks, you changed the Pure path — revert and reconsider.
- **Pause propagation is the trickiest bit:** `Paused` must thread `run_agent_tools → AgentStep::Paused → NodeExec::Paused → apply_node_result → outcome.paused → suppress RunCompleted`. Grep for every `match` on `AgentStep`/`NodeExec` and add the arm; the compiler's exhaustiveness check is your checklist.
- **Reconcile key determinism:** the key computed on resume (`idempotency_key(teid, tih)`) MUST equal the one recorded in the `EffectIntent`; both derive from `(effect_id, args_hash)`, and `args_hash = tool_input_hash(name, arguments)` — keep that identical on both paths.
- **`start()` folds Intents:** `fold_journal` must run over the FULL journal (it does), so `Fold.intents`/`Fold.observations` are populated before `drive` replays.
