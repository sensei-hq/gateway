---
title: Selection Policy Pipeline & Health Gates (SP-0)
doctype: design
module: routing
status: planned
phase: 1
spec: SP-0
feature:
  - ../features/routing/connection-cooldown.md
  - ../features/routing/model-lockout.md
  - ../features/routing/quota-demote-to-tier.md
source: crates/gateway/src/selection.rs, crates/gateway/src/circuit_breaker.rs, crates/gateway/src/engine.rs
---

# Selection Policy Pipeline & Health Gates (SP-0)

**Goal.** Add three model/router health gates — **connection cooldown**, **model
lockout** (per-reason), and **quota demote-to-tier** — to gateway selection,
by refactoring the current inline validation into a **composable policy
pipeline** so future gates/strategies compose without editing a monolith.

**Non-goals (SP-0).** Free-tier catalog + `tiers` dimension (SP-CAT), live usage
metering + `headroom`/predicted-lockout (SP-DATA), persisted (multi-instance)
gate state. SP-0 scaffolds the seams these plug into but ships only in-memory
gates and keeps existing behavior otherwise unchanged.

**Coordination.** Touches `selection.rs`/`circuit_breaker.rs`/`engine.rs` — the
same hot path as issue #39 (engine/llama_cpp complexity refactor). Do #39 first
or land this as part of it; this spec assumes the current structure.

---

## 1. Current state (what we're refactoring)

`ModelSelectionService` holds `&GatewayConfig` + `&CircuitBreakerManager`.
`validate_chain_entry` and its near-duplicate `validate_direct` run a fixed
inline sequence — *model exists → router resolve+enabled → capability →
circuit breaker → budget* — each producing a stringly-typed
`SkippedCandidate { model, router, reason: String }`. The write side lives in
the engine (`circuit_breaker.record_failure`/`record_success`).

Problems for extension: adding cooldown/lockout means more inline `if`s in two
functions; reasons are untyped (so `resume_after` can't be derived); read
(gate) and write (react) are entangled with concrete `CircuitBreakerManager`.

## 2. Target design (D14)

Four separated concerns behind trait seams:

```rust
// ── Typed skip reason (replaces `reason: String`) ───────────────────────────
pub enum LockReason { RateLimit, QuotaExhausted, CreditsExhausted, Auth }

pub enum SkipReason {
    ModelNotFound,
    RouterNotFound,
    RouterDisabled,
    UnsupportedCapability(Capability),
    OverBudget { estimated: f64, budget: f64 },
    CircuitOpen,
    Cooling   { until: Instant },                 // connection cooldown
    LockedOut { reason: LockReason, until: Option<Instant> }, // model lockout (None = terminal)
    BelowMinCapability,                            // reserved (SP-CAT quality floor)
}

pub enum GateVerdict { Admit, Skip(SkipReason) }

// ── Admission: one gate = one responsibility (Chain of Responsibility) ───────
pub trait AdmissionGate: Send + Sync {
    fn name(&self) -> &'static str;
    fn evaluate(&self, cand: &CandidateView<'_>, ctx: &SelectionCtx<'_>) -> GateVerdict;
}

// ── Ordering: per-tier task-aware ordering (Strategy) — SP-CAT uses it fully ─
pub trait RoutingStrategy: Send + Sync {
    fn order(&self, admitted: &mut Vec<SelectedModel>, ctx: &SelectionCtx<'_>);
}

// ── Reaction: update state from an attempt outcome (Observer) ────────────────
pub struct AttemptOutcome<'a> { pub error: Option<&'a GatewayError>, pub retry_after: Option<Duration> }
pub trait OutcomeSink: Send + Sync {
    fn on_outcome(&self, cand: &SelectedModel, outcome: &AttemptOutcome<'_>);
}

// ── State: ephemeral health, swappable (Ports & Adapters) ────────────────────
pub trait HealthStore: Send + Sync {
    fn endpoint_state(&self, endpoint: &str) -> EndpointHealth;   // breaker/lockout
    fn router_state(&self, router: &str) -> RouterHealth;         // cooldown
    fn record_endpoint(&self, endpoint: &str, ev: HealthEvent);
    fn record_router(&self, router: &str, ev: HealthEvent);
}
```

`CandidateView` is a cheap borrow of the resolved `(model_config, router_config,
endpoint, cost_estimate)`; `SelectionCtx` carries `criteria` + a `&dyn
HealthStore` + `now: Instant` (clock injected for deterministic tests).

### 2.1 `validate_*` collapses to build-then-run

Both `validate_direct` and `validate_chain_entry` become one path:

```
fn admit(&self, cand: CandidateView, ctx) -> Result<SelectedModel, SkippedCandidate> {
    for gate in self.gates {                       // ordered pipeline
        if let GateVerdict::Skip(reason) = gate.evaluate(&cand, ctx) {
            return Err(SkippedCandidate { model, router, reason });   // typed
        }
    }
    Ok(cand.into_selected())
}
```

The two entry points differ only in how they build the `CandidateView` (direct:
caller-pinned router; chain: entry router → model.provider, entry
`api_model_id` override). This removes the duplication.

### 2.2 The SP-0 gate set (ordered; cheap → stateful → costly)

| Order | Gate | Verdict | Notes |
|---|---|---|---|
| 1 | `ModelExistsGate` | `ModelNotFound` | structural |
| 2 | `RouterGate` | `RouterNotFound` / `RouterDisabled` | structural |
| 3 | `CapabilityGate` | `UnsupportedCapability` | structural |
| 4 | `ConnectionCooldownGate` **(new)** | `Cooling{until}` | reads `HealthStore.router_state` |
| 5 | `CircuitBreakerGate` | `CircuitOpen` | wraps existing `CircuitBreakerManager` (now a `HealthStore` impl) |
| 6 | `ModelLockoutGate` **(new)** | `LockedOut{reason, until}` | reads `HealthStore.endpoint_state` |
| 7 | `BudgetGate` | `OverBudget` | last: don't spend cost-estimation on already-skipped candidates |

Order is data (a `Vec<Arc<dyn AdmissionGate>>`), so inserting/reordering is a
registration change, not a code edit.

### 2.3 The SP-0 sinks

| Sink | Reacts to | Effect |
|---|---|---|
| `CircuitBreakerSink` | any failure / success | existing breaker `record_failure`/`record_success` |
| `ConnectionCooldownSink` **(new)** | `GatewayError::Network` / connect timeout | start router cooldown (jittered backoff) |
| `ModelLockoutSink` **(new)** | `RateLimit` / quota / credits / auth errors | lock endpoint by reason (see §3.2) |

The engine's existing `record_failure`/`record_success` calls become one call:
`self.sinks.on_outcome(&selected, &outcome)`, fanning out to all sinks.

## 3. New feature behavior

### 3.1 Connection cooldown (`ConnectionCooldownGate` + `Sink`)
- **Trigger:** `AttemptOutcome.error` is `Network` (or connect-phase `Timeout`).
- **Effect:** `HealthStore.record_router(router, Cooldown{until: now + backoff})`; backoff jittered, escalating on repeat, clamped to `max_cooldown_ms`.
- **Gate:** if `router_state(router).cooling_until > now` → `Skip(Cooling{until})`, skipping **all** of that router's models in one shot.

### 3.2 Model lockout (`ModelLockoutGate` + `Sink`) — per-reason
- **Classification** (a small pure `classify(error, status, body) -> LockReason` fn, unit-tested): 429 → `RateLimit`; 403/quota-body → `QuotaExhausted`; credits-exhausted body → `CreditsExhausted`; 401 → `Auth`. Text-pattern fallback for non-standard 400/403 bodies.
- **Cooldown by reason:** `RateLimit` → ~60s; `QuotaExhausted` → next reset boundary (00:00 / monthly), else ~1h; `CreditsExhausted` → `until = None` (terminal); `Auth` → `until = None` (until credential change). Honor an exact upstream reset hint verbatim (not clamped to `max_cooldown_ms`).
- **Escalation:** repeated failure after release escalates the window (an escalation counter that outlives the cooldown), clamped to `max_cooldown_ms` (except exact reset hints).
- **Gate:** `endpoint_state(endpoint).locked_until > now` → `Skip(LockedOut{reason, until})`.
- **Bounded:** the lockout map has an eviction cap (LRU) so it can't leak.

### 3.3 Quota demote-to-tier (emergent + engine aggregation)
- **Emergent:** because `ModelLockoutGate` *skips* a quota-hit model rather than terminating, the chain naturally falls over to the next entry/tier. No new gate.
- **Engine aggregation:** when the walk exhausts all candidates, compute
  `resume_after = min(until)` over the *timed* skip reasons (`Cooling{until}`,
  `LockedOut{Some(until)}`). Return a terminal error carrying `resume_after`
  (and the structured skip set). Terminal (`until: None`) reasons surface a
  human-action hint, not a wake-up time.

```rust
// engine, exhaustion path
let resume_after = result.skipped.iter().filter_map(|s| s.reason.until()).min();
Err(GatewayError::AllGated { resume_after, skipped: result.skipped })
```
(Or extend `AllAttemptsFailed` with `resume_after: Option<Instant>` — see §5.)

## 4. Config additions

`GatewayConfig` gains an optional `resilience` block (all defaulted; absent →
current behavior). Per-reason durations, escalation, and caps live here so they
are operator-tunable, not hard-coded:

```rust
pub struct ResilienceConfig {
    pub connection_cooldown: CooldownConfig,   // base_ms, max_ms, jitter, max_backoff_steps
    pub model_lockout: LockoutConfig,          // rate_limit_ms, quota_ms, max_cooldown_ms, eviction_cap
    // circuit_breaker already exists (CircuitBreakerConfig)
}
```

## 5. Builder & wiring (composition, not subclasses)

`GatewayBuilder` gains optional-layer registration; presets are recipes:

```rust
GatewayBuilder::new(config)
    .with_gate(Arc::new(ConnectionCooldownGate::new(store.clone())))
    .with_sink(Arc::new(ModelLockoutSink::new(store.clone())))
    .with_resilience(ResiliencePreset::Standard)   // registers the §2.2/§2.3 set
    .build();
```
- `ModelSelectionService` takes `gates: &[Arc<dyn AdmissionGate>]` + `strategy: &dyn RoutingStrategy` + `store: &dyn HealthStore` instead of `&CircuitBreakerManager`.
- `CircuitBreakerManager` implements `HealthStore` (endpoint half) so it's reused, not rewritten; the breaker gate/sink wrap it.
- Default preset preserves today's behavior + adds cooldown/lockout; a `Minimal` preset = structural gates only.

## 6. Migration (strangler-fig, TDD, small commits)

1. Introduce `SkipReason` enum + `SkippedCandidate.reason: SkipReason`; update call sites/tests (behavior-preserving). Commit.
2. Extract the 5 existing inline checks into gates; `ModelSelectionService` runs the pipeline; `CircuitBreakerManager` implements `HealthStore`. All existing `selection.rs` tests still pass (now asserting typed reasons). Commit.
3. Add `HealthStore` router half + `ConnectionCooldownGate`/`Sink`. Commit.
4. Add `ModelLockoutGate`/`Sink` + `classify()` + per-reason cooldowns. Commit.
5. Engine: dispatch outcomes to the sink pipeline; aggregate `resume_after` on exhaustion; extend the terminal error. Commit.
6. Builder `.with_gate/.with_sink/.with_resilience` + `ResilienceConfig`. Commit.

Each step keeps the suite green (never merge on red).

## 7. Acceptance criteria

The Gherkin scenarios in the feature docs are the acceptance spec:
- [routing/connection-cooldown](../features/routing/connection-cooldown.md#scenarios)
- [routing/model-lockout](../features/routing/model-lockout.md#scenarios)
- [routing/quota-demote-to-tier](../features/routing/quota-demote-to-tier.md#scenarios)

Plus refactor-safety: every existing `selection.rs` test passes unchanged in
behavior (typed reasons substituted for string matches).

## 8. Testing strategy

- **Per-gate unit tests** (each gate in isolation with a fake `HealthStore` + injected clock) — one test per Gherkin scenario.
- **`classify()` table test** — status/body → `LockReason`.
- **Pipeline test** — ordered fake gates prove first-skip-wins and reason propagation.
- **Escalation test** — repeated failures grow the cooldown; exact reset hint not clamped.
- **Engine exhaustion test** — all-gated → terminal error with correct `resume_after = min(until)`; terminal reason → no wake-up time.
- Injected `now: Instant` (no `Instant::now()` inside gates) keeps time-based tests deterministic.

## 9. Why this stays clean as features grow

- New feature (e.g. `predicted-lockout`, `headroom`, `MinCapabilityGate`) = a new small gate/strategy/sink file + one registration line; **no existing file changes** (OCP).
- Each unit has one responsibility (SRP), depends on trait seams (DIP), and implements only the traits it needs (ISP) — a structural check is just a gate; lockout is a gate + a sink.
- Composition is via the builder (situation presets) + per-route strategy config — reasoning and research route differently in the *same* instance, no inheritance.
