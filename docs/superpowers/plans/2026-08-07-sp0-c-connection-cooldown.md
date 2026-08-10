# SP-0 (c) — Connection Cooldown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add the first real health gate — **connection cooldown**: after a transport-level fault (`Network` / `Timeout`) on a router, briefly skip *all* of that router's models on subsequent selections (so a down provider isn't hammered model-by-model). Uses the read-side gate pipeline (foundation) and the write-side recorder pipeline (plan b).

**Scope note (refinement of the design):** `ProviderSignal` capture + `classify()` are **deferred to plan (d)** (model lockout), where they're consumed — defining them here would be unused (YAGNI). Connection cooldown reacts to the **error variant** (`Network`/`Timeout`), which needs no `ProviderSignal`.

**Architecture:** A `RouterHealthRead` read port + an in-memory `ConnectionCooldownStore` (per-router `Instant`), shared between a new `ConnectionCooldownGate` (skips a candidate whose router is cooling) and a `ConnectionCooldownSink` (`HealthRecorder`; on a `Network`/`Timeout` outcome, starts the router's cooldown). This mirrors the endpoint-level breaker (`EndpointHealthRead` + `CircuitBreakerGate` + `CircuitBreakerSink`) at **router** granularity. Behavior stays identical until a real transport fault occurs (existing tests never do), so the suite stays green; cooldown is a *next-request* skip (the current request's fallover is unchanged).

**Tech Stack:** Rust, `crates/gateway`. Contract: existing 187 gateway lib tests stay green + `make check` clean per commit (fmt + clippy `-D warnings`). New cooldown behavior is proven by new tests.

---

## File Structure

- **Modify `crates/gateway/src/skip_reason.rs`** — add `SkipReason::Cooling { until: Instant }` variant (+ Display, + `until()` returns `Some(until)`).
- **Modify `crates/gateway/src/gates/mod.rs`** — add `RouterHealthRead` trait; add `router_health: &dyn RouterHealthRead` to `SelectionCtx`; add `router: &str` to `AttemptOutcome`.
- **Create `crates/gateway/src/gates/cooldown.rs`** — `ConnectionCooldownStore` (impl `RouterHealthRead` + `start`), `ConnectionCooldownGate` (impl `AdmissionGate`), `ConnectionCooldownSink` (impl `HealthRecorder`).
- **Modify `crates/gateway/src/selection.rs`** — `ModelSelectionService` holds a `router_health` port; `new(config, cb, router_health)`; insert `ConnectionCooldownGate` into the gate vec (after `CapabilityGate`, before `CircuitBreakerGate`); pass `router_health` into `SelectionCtx`.
- **Modify `crates/gateway/src/engine/mod.rs`** — `Gateway.cooldown: ConnectionCooldownStore`; register `ConnectionCooldownSink` in `recorders`; pass `&self.cooldown` to `ModelSelectionService::new`.
- **Modify `crates/gateway/src/engine/execute.rs`, `stream.rs`** — pass `router` (from `SelectedModel.router`) in the `AttemptOutcome`; update the `ModelSelectionService::new(...)` call sites.

---

### Task 1: `SkipReason::Cooling` + `RouterHealthRead` + `ConnectionCooldownStore`

**Files:** `skip_reason.rs`, `gates/mod.rs`, new `gates/cooldown.rs` (+ tests).

- [ ] **Step 1: Failing tests** (in `gates/cooldown.rs`):
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};
    #[test]
    fn store_reports_cooling_until_it_expires() {
        let s = ConnectionCooldownStore::new();
        assert!(s.cooling_until("r").is_none());               // unknown → not cooling
        let until = Instant::now() + Duration::from_secs(60);
        s.start("r", until);
        assert_eq!(s.cooling_until("r"), Some(until));         // cooling
        // (gate compares against `now`; store just records the instant)
    }
}
```
- [ ] **Step 2:** `cargo test -p sensei-gateway cooldown` → FAIL (types missing).
- [ ] **Step 3: Implement**
  - `skip_reason.rs`: add variant `Cooling { until: Instant }`; Display → `"router cooling down"`; if a `until()` method exists on `SkipReason` (check — the foundation removed it; if absent, skip), no change. If there's a place that matches all variants exhaustively, add the arm.
  - `gates/mod.rs`: add
    ```rust
    /// Read port for router-level health (connection cooldown; more router ports later).
    pub trait RouterHealthRead: Send + Sync {
        /// `Some(until)` if the router is currently cooling down.
        fn cooling_until(&self, router: &str) -> Option<Instant>;
    }
    ```
  - `gates/cooldown.rs`: 
    ```rust
    use super::{RouterHealthRead};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    /// In-memory per-router cooldown state (read by the gate, written by the sink).
    /// Arc-backed + Clone so the gate's read reference and the sink's owned copy share state.
    #[derive(Clone, Default)]
    pub struct ConnectionCooldownStore { cooling: Arc<Mutex<HashMap<String, Instant>>> }
    impl ConnectionCooldownStore {
        pub fn new() -> Self { Self::default() }
        pub fn start(&self, router: &str, until: Instant) {
            self.cooling.lock().unwrap_or_else(|e| e.into_inner()).insert(router.to_string(), until);
        }
    }
    impl RouterHealthRead for ConnectionCooldownStore {
        fn cooling_until(&self, router: &str) -> Option<Instant> {
            self.cooling.lock().unwrap_or_else(|e| e.into_inner()).get(router).copied()
        }
    }
    ```
  - Add `pub mod cooldown;` to `gates/mod.rs`.
- [ ] **Step 4:** `cargo test -p sensei-gateway cooldown` PASS; full suite 187 unchanged; clippy/fmt clean.
- [ ] **Step 5: Commit:** `feat(gateway): SkipReason::Cooling + RouterHealthRead + ConnectionCooldownStore`.

---

### Task 2: `ConnectionCooldownGate` + wire into selection (read side)

**Files:** `gates/cooldown.rs`, `gates/mod.rs` (`SelectionCtx.router_health`), `selection.rs`, `engine/mod.rs`, `engine/execute.rs`, `engine/stream.rs`.

- [ ] **Step 1: Failing gate test** (in `gates/cooldown.rs`): build a `ConnectionCooldownStore`, `start("r", now+60s)`, build a `CandidateView` on router `"r"` and a `SelectionCtx` with `router_health = &store`, `now`; assert `ConnectionCooldownGate.evaluate(...)` returns `Skip(Cooling{..})`; with a router NOT cooling → `Admit`; with an expired cooldown (`start("r", now-1s)`) → `Admit`.
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement**
  - `gates/mod.rs`: add `pub router_health: &'a dyn RouterHealthRead,` to `SelectionCtx`.
  - `gates/cooldown.rs`:
    ```rust
    use super::{AdmissionGate, CandidateView, GateVerdict, SelectionCtx};
    use crate::skip_reason::SkipReason;
    pub struct ConnectionCooldownGate;
    impl AdmissionGate for ConnectionCooldownGate {
        fn name(&self) -> &'static str { "connection_cooldown" }
        fn evaluate(&self, c: &CandidateView<'_>, x: &SelectionCtx<'_>) -> GateVerdict {
            match x.router_health.cooling_until(c.router) {
                Some(until) if until > x.now => GateVerdict::Skip(SkipReason::Cooling { until }),
                _ => GateVerdict::Admit,
            }
        }
    }
    ```
  - `selection.rs`: `ModelSelectionService` gains `router_health: &'a dyn RouterHealthRead`; `new(config, cb, router_health)` stores it and inserts `Box::new(ConnectionCooldownGate)` into the gate vec **after `CapabilityGate`, before `CircuitBreakerGate`** (order: capability → cooldown → breaker → budget); the `SelectionCtx { ... }` in `admit` gets `router_health: self.router_health`.
  - `engine/mod.rs`: add `cooldown: ConnectionCooldownStore` to `Gateway`; init `ConnectionCooldownStore::new()` in `new`; (recorder registration is Task 3).
  - `engine/execute.rs` + `stream.rs`: update the two `ModelSelectionService::new(&config, &self.circuit_breaker)` call sites → `ModelSelectionService::new(&config, &self.circuit_breaker, &self.cooldown)` (`&ConnectionCooldownStore` coerces to `&dyn RouterHealthRead`).
- [ ] **Step 4: Verify** — gate test passes; full suite 187 green (nothing writes cooldowns yet, so the gate never skips in existing tests → behavior unchanged). clippy/fmt clean.
- [ ] **Step 5: Commit:** `feat(gateway): ConnectionCooldownGate wired into selection (router cooldown read-side)`.

---

### Task 3: `ConnectionCooldownSink` + wire into the recorder pipeline (write side) + integration test

**Files:** `gates/cooldown.rs`, `gates/mod.rs` (`AttemptOutcome.router`), `engine/mod.rs`, `engine/execute.rs`, `stream.rs`.

- [ ] **Step 1: Implement**
  - `gates/mod.rs`: add `pub router: &'a str,` to `AttemptOutcome` (the breaker sink ignores it; the cooldown sink uses it — the `endpoint` "router:model" can't be split reliably because model ids contain `:` e.g. `gemma3:27b`).
  - `gates/cooldown.rs`:
    ```rust
    use super::{AttemptOutcome, HealthRecorder};
    use crate::types::error::GatewayError;
    use std::time::{Duration, Instant};

    /// Default router cooldown after a transport fault. (Operator-configurable via
    /// ResilienceConfig in plan (f); a constant for now.)
    pub const DEFAULT_CONNECTION_COOLDOWN: Duration = Duration::from_secs(30);

    pub struct ConnectionCooldownSink { store: ConnectionCooldownStore, cooldown: Duration }
    impl ConnectionCooldownSink {
        pub fn new(store: ConnectionCooldownStore, cooldown: Duration) -> Self { Self { store, cooldown } }
    }
    impl HealthRecorder for ConnectionCooldownSink {
        fn on_outcome(&self, o: &AttemptOutcome<'_>) {
            // Transport-level fault → cool the whole router. Network = connection failure;
            // Timeout = endpoint unreachable/too slow.
            if matches!(o.error, Some(GatewayError::Network(_)) | Some(GatewayError::Timeout { .. })) {
                self.store.start(o.router, Instant::now() + self.cooldown);
            }
        }
    }
    ```
  - `engine/mod.rs`: register the sink — `recorders` becomes `[CircuitBreakerSink(cb.clone()), ConnectionCooldownSink::new(cooldown.clone(), DEFAULT_CONNECTION_COOLDOWN)]`. Update `record_outcome`/`dispatch_outcome` (plan b) signatures to take `router` and set `AttemptOutcome.router`.
  - `engine/execute.rs` + `stream.rs`: at the outcome sites, pass the candidate's router (`&candidate.router` / the `SelectedModel.router` in scope) into `record_outcome`/`dispatch_outcome`.
- [ ] **Step 2: Integration test** (in `engine/tests.rs`): register a `failing` adapter that returns `GatewayError::Timeout { .. }` on router A, plus a working noop on router B, in a chain [A-model, B-model]. First `execute()` → A times out (walk breaks per current Timeout semantics, OR falls over if Timeout is a configured trigger — either way the **sink cools router A**). Then assert `gw.cooldown.cooling_until("A") > now`. Then a second `execute()` (or a direct `select`) → A's model is skipped with `SkipReason::Cooling` and B serves it. Then simulate expiry (start a past cooldown, or use a 0-duration sink in a variant) → A admitted again. Keep it deterministic (you control the store; you can also unit-test the sink directly: `sink.on_outcome(&AttemptOutcome{ router:"A", endpoint:"A:m", success:false, error:Some(&GatewayError::Timeout{..}) })` then assert `store.cooling_until("A")` is set).
- [ ] **Step 3: Verify** — new cooldown behavior proven; full suite green (existing 187 + new); a `Network`/`Timeout` fault now cools the router, non-transport errors (ProviderError/Auth) do NOT (assert one). clippy/fmt clean.
- [ ] **Step 4: Commit:** `feat(gateway): ConnectionCooldownSink — cool a router on transport faults, skip it next selection`.

---

## Self-Review

- **Spec coverage:** SP-0 design §12 (connection cooldown, router granularity), §2.2 gate order (capability → cooldown → breaker → budget). `ProviderSignal`/`classify` explicitly deferred to plan (d) with its consumer.
- **Behavior preservation:** existing tests exercise no `Network`/`Timeout`-driven cooldown, and cooldown only affects *subsequent* selection, so the 187 tests stay green; new behavior has its own tests. The cooldown store is shared (Arc-backed) between the gate (read) and sink (write) — same pattern as the breaker.
- **Type consistency:** `RouterHealthRead::cooling_until` (Task 1) read by `ConnectionCooldownGate` (Task 2) via `SelectionCtx.router_health` (Task 2), written by `ConnectionCooldownSink` (Task 3) via `ConnectionCooldownStore::start`; `AttemptOutcome.router` (Task 3) supplies the key. `SkipReason::Cooling { until }` (Task 1) produced by the gate (Task 2).
- **Sequencing:** 1 (types+store) → 2 (gate, read-side wiring; harmless since nothing writes yet) → 3 (sink, write-side + behavior + tests). Each green + committed.
- **Deferred (not this plan):** operator-config of the cooldown duration/jitter (plan f, `ResilienceConfig`); escalation/backoff on repeat (the design's escalation is a lockout concern, plan d); eviction of expired store entries (harmless to leave; add with the bounded-map work in d).

## Execution Handoff

Subagent-driven in an isolated worktree off `develop`; per-task spec+quality review; final review; `finishing-a-development-branch` → merge to `develop`; then plan (d) — model lockout + `ProviderSignal`/`classify` + `on_lockout` — builds on this router-health pattern.
