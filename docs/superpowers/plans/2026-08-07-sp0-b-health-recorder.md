# SP-0 (b) — HealthRecorder Write-Side Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Introduce the reliable **write-side** seam of the selection policy pipeline — a `HealthRecorder` trait + `AttemptOutcome` + a behavior-preserving `CircuitBreakerSink` — and replace the engine's four direct breaker `record_success`/`record_failure` calls with a recorder **fan-out**. Breaker-only for now, so behavior is identical; later plans (c) connection-cooldown and (d) model-lockout add recorders here without touching the engine again.

**Architecture:** This is the counterpart to the read-side `AdmissionGate` pipeline (already merged). `HealthRecorder` is a **reliable state reducer** (NOT the best-effort `SelectionObserver` — that separate observation seam arrives with the `on_lockout` callback in plan (d)). The `Gateway` gains `recorders: Vec<Arc<dyn HealthRecorder>>`, defaulted to `[CircuitBreakerSink]` wrapping a clone of the same `CircuitBreakerManager` the selection gate reads — so read and write reference one shared breaker state (behavior-preserving). Post-#39 the outcome sites are `engine/execute.rs::attempt_candidate` and `engine/stream.rs`.

**Tech Stack:** Rust, `crates/gateway`. Contract: the 185 gateway lib tests stay green + `make check` clean on every commit (pre-commit hook runs fmt + clippy `-D warnings`). Behavior-preserving — the `CircuitBreakerSink` calls exactly `record_success`/`record_failure`, so no test changes.

---

## File Structure

- **Modify `crates/gateway/src/gates/mod.rs`** — add `HealthRecorder` trait + `AttemptOutcome`.
- **Modify `crates/gateway/src/gates/circuit_breaker_gate.rs`** — add `CircuitBreakerSink` (alongside the existing `CircuitBreakerGate`; both concern the breaker).
- **Modify `crates/gateway/src/engine/mod.rs`** — `Gateway.recorders` field + default wiring in `new`; a `pub(super)` `record_outcome` fan-out helper (or a free fn usable from the stream closure).
- **Modify `crates/gateway/src/engine/execute.rs`** — replace the two `self.circuit_breaker.record_success/failure` calls in `attempt_candidate` with the fan-out.
- **Modify `crates/gateway/src/engine/stream.rs`** — replace the two cloned-breaker `record_*` calls with the fan-out (clone the recorders into the stream closure).

---

### Task 1: `HealthRecorder` trait + `AttemptOutcome` + `CircuitBreakerSink`

**Files:** `gates/mod.rs`, `gates/circuit_breaker_gate.rs` (+ tests).

- [ ] **Step 1: Write the failing test** (in `gates/circuit_breaker_gate.rs`):

```rust
#[test]
fn circuit_breaker_sink_records_success_and_failure() {
    use crate::circuit_breaker::{CircuitBreakerConfig, CircuitBreakerManager};
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig { threshold: 1, ..Default::default() });
    cb.can_execute("r:m"); // init
    let sink = CircuitBreakerSink::new(cb.clone());
    // a failure through the sink opens the breaker (threshold 1)
    sink.on_outcome(&AttemptOutcome { endpoint: "r:m", success: false, error: None });
    assert_eq!(cb.get_state("r:m").name(), "open");
    // a success through the sink (on a fresh endpoint) leaves it closed
    cb.can_execute("r:n");
    sink.on_outcome(&AttemptOutcome { endpoint: "r:n", success: true, error: None });
    assert_eq!(cb.get_state("r:n").name(), "closed");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p sensei-gateway circuit_breaker_sink`
Expected: FAIL — `HealthRecorder`/`AttemptOutcome`/`CircuitBreakerSink` not defined.

- [ ] **Step 3: Implement**

In `gates/mod.rs`:
```rust
use crate::types::error::GatewayError;

/// A single attempt's outcome, fed to the write-side recorders. `endpoint` is the
/// opaque "router:model" key (matches the read-side breaker keying). `error` is
/// carried for later recorders (cooldown/lockout classify it); the breaker sink
/// uses only `success`.
pub struct AttemptOutcome<'a> {
    pub endpoint: &'a str,
    pub success: bool,
    pub error: Option<&'a GatewayError>,
}

/// Reliable write-side reducer: updates authoritative health state from an
/// attempt outcome. NOT best-effort (that is the separate `SelectionObserver`,
/// added with the `on_lockout` callback in plan (d)).
pub trait HealthRecorder: Send + Sync {
    fn on_outcome(&self, outcome: &AttemptOutcome<'_>);
}
```
In `gates/circuit_breaker_gate.rs`:
```rust
use super::{AttemptOutcome, HealthRecorder};
use crate::circuit_breaker::CircuitBreakerManager;

/// Behavior-preserving breaker write-side: maps success→record_success,
/// failure→record_failure on the shared `CircuitBreakerManager`.
pub struct CircuitBreakerSink {
    breaker: CircuitBreakerManager,
}
impl CircuitBreakerSink {
    pub fn new(breaker: CircuitBreakerManager) -> Self { Self { breaker } }
}
impl HealthRecorder for CircuitBreakerSink {
    fn on_outcome(&self, o: &AttemptOutcome<'_>) {
        if o.success { self.breaker.record_success(o.endpoint); }
        else { self.breaker.record_failure(o.endpoint); }
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p sensei-gateway circuit_breaker_sink` → PASS. Then `cargo test -p sensei-gateway` → 185 unchanged. `cargo clippy -p sensei-gateway --all-targets -- -D warnings` + `cargo fmt --all --check` clean.

- [ ] **Step 5: Commit**

```bash
git add crates/gateway/src/gates/mod.rs crates/gateway/src/gates/circuit_breaker_gate.rs
git commit -m "feat(gateway): HealthRecorder write-side trait + AttemptOutcome + CircuitBreakerSink"
```

---

### Task 2: Wire `recorders` into `Gateway` + fan out `execute`'s outcome calls

**Files:** `engine/mod.rs`, `engine/execute.rs`.

- [ ] **Step 1: Regression contract** — the existing `execute` tests (`execute_records_attempts`, `execute_fallback_on_provider_error`, `skips_circuit_breaker_open`, `exhaustion_*`, etc.) are the behavior gate; no new test needed (behavior-preserving). Optionally add a test that a custom `HealthRecorder` registered on the gateway receives an `AttemptOutcome` per attempt (proves the fan-out fires) — keep it minimal.

- [ ] **Step 2: Add the field + default wiring + fan-out helper**

In `engine/mod.rs`:
```rust
use std::sync::Arc;
use crate::gates::{AttemptOutcome, CircuitBreakerSink, HealthRecorder};

pub struct Gateway {
    // ...existing fields...
    circuit_breaker: CircuitBreakerManager,
    recorders: Vec<Arc<dyn HealthRecorder>>,   // NEW; default = [CircuitBreakerSink]
    // ...
}

impl Gateway {
    pub fn new(config: GatewayConfig, adapters: AdapterRegistry, circuit_breaker: CircuitBreakerManager) -> Self {
        let recorders: Vec<Arc<dyn HealthRecorder>> =
            vec![Arc::new(CircuitBreakerSink::new(circuit_breaker.clone()))];
        Self { config: /*...*/, adapters, circuit_breaker, recorders, store: None, probe: None }
    }

    /// Dispatch one attempt's outcome to every registered recorder (reliable).
    pub(super) fn record_outcome(&self, endpoint: &str, success: bool, error: Option<&GatewayError>) {
        let o = AttemptOutcome { endpoint, success, error };
        for r in &self.recorders { r.on_outcome(&o); }
    }
}
```
The `circuit_breaker` field stays — it's the read side (`ModelSelectionService::new(&config, &self.circuit_breaker)`); the sink wraps a **clone** of the same manager (shared Arc-backed state), so read and write see the same breaker. Confirm `CircuitBreakerManager: Clone` (it is).

- [ ] **Step 3: Replace `execute.rs`'s two calls**

In `engine/execute.rs::attempt_candidate`, replace:
- `self.circuit_breaker.record_success(&endpoint);` → `self.record_outcome(&endpoint, true, None);`
- `self.circuit_breaker.record_failure(&endpoint);` → `self.record_outcome(&endpoint, false, Some(&err));` (pass the error if in scope at that site; else `None` — check what's available; the breaker sink ignores it, so `None` is behavior-identical if the error isn't handy).

- [ ] **Step 4: Verify**

`cargo test -p sensei-gateway` → 185 green (esp. `skips_circuit_breaker_open` / `direct_circuit_breaker_open` / `execute_records_attempts` / `execute_fallback_on_provider_error` — the breaker still opens/closes identically). `cargo clippy -p sensei-gateway --all-targets -- -D warnings` + `cargo fmt --all --check` clean.

- [ ] **Step 5: Commit**

```bash
git add crates/gateway/src/engine/mod.rs crates/gateway/src/engine/execute.rs
git commit -m "refactor(gateway): fan out execute's breaker outcomes through the recorder pipeline"
```

---

### Task 3: Fan out `execute_stream`'s outcome calls

**Files:** `engine/stream.rs`.

- [ ] **Step 1:** In `engine/stream.rs`, the stream currently clones `self.circuit_breaker` into the async closure and calls `record_success`/`record_failure`. Replace this: clone `self.recorders.clone()` (a `Vec<Arc<dyn HealthRecorder>>` — cheap Arc clones) into the closure, and dispatch via a small free helper (since `self` isn't available inside the `'static` stream closure):
```rust
// engine/mod.rs (or stream.rs) — a free fn the closure can own
pub(super) fn dispatch_outcome(recorders: &[Arc<dyn HealthRecorder>], endpoint: &str, success: bool, error: Option<&GatewayError>) {
    let o = AttemptOutcome { endpoint, success, error };
    for r in recorders { r.on_outcome(&o); }
}
```
Have `Gateway::record_outcome` (Task 2) delegate to this free fn to avoid duplication. In the stream closure, replace `circuit_breaker.record_success(&endpoint)` → `dispatch_outcome(&recorders, &endpoint, true, None)` and the failure likewise. Remove the now-unused `circuit_breaker` clone from the closure if nothing else uses it (the selection call earlier still uses `&self.circuit_breaker`, which is fine — that's before the closure).

- [ ] **Step 2: Verify** `cargo test -p sensei-gateway` → 185 green (streaming tests: `per_call_credential_reaches_the_stream_adapter`, and any breaker-in-stream coverage). clippy/fmt clean.

- [ ] **Step 3: Commit**

```bash
git add crates/gateway/src/engine/stream.rs crates/gateway/src/engine/mod.rs
git commit -m "refactor(gateway): fan out execute_stream's breaker outcomes through the recorder pipeline"
```

---

## Self-Review

- **Spec coverage:** implements SP-0 design §H1 (reliable `HealthRecorder`, not observer), §5 (recorder wiring), §2 (`AttemptOutcome`). `CircuitBreakerSink` is the only recorder (behavior-preserving); connection-cooldown (plan c) and model-lockout (plan d) register additional recorders here with no engine change. The best-effort `SelectionObserver`/`on_lockout` seam is explicitly deferred to plan (d).
- **Behavior preservation:** the sink calls exactly `record_success`/`record_failure` on the same shared breaker; both outcome sites (`execute` + `execute_stream`) route through the fan-out; 185 tests unchanged.
- **Type consistency:** `AttemptOutcome`/`HealthRecorder` (Task 1) used by `CircuitBreakerSink` (Task 1), `Gateway.record_outcome`/`dispatch_outcome` (Tasks 2–3). `endpoint: &str` (the "router:model" key) — will grow to an `EndpointKey` when the health-store/cooldown work lands (plan c); `error: Option<&GatewayError>` is carried now (breaker ignores it) so plan (c)'s `ProviderSignal`/classify slots in without reshaping the type at the call sites.
- **Sequencing:** 1 (types+sink) → 2 (wire + execute) → 3 (stream); each green + committed independently.

## Execution Handoff

Subagent-driven (recommended): fresh subagent per task + spec/quality review, in an isolated worktree off `develop`. After all tasks: final review, then `superpowers:finishing-a-development-branch` (merge to `develop`), then plan (c) — adapter-boundary `ProviderSignal`+`classify` + connection-cooldown gate/recorder — builds on this recorder seam.
