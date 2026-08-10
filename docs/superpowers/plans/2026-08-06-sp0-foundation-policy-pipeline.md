# SP-0 Foundation — Selection Policy Pipeline (typed reasons + gate extraction) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor gateway model selection from an inline validation sequence into a composable **policy pipeline** (typed `SkipReason` + ordered `AdmissionGate`s + a `RoutingStrategy`), behavior-preserving, so cooldown/lockout/demote (later plans) plug in as new gates without touching existing code.

**Architecture:** `ModelSelectionService` becomes a pipeline runner: `resolve → run ordered gates → order`. Existing checks (capability, breaker, budget) become individual `AdmissionGate`s over narrow read ports; `CircuitBreakerManager` implements the endpoint read port; the hardcoded priority sort becomes `PriorityStrategy`. This is the first of the SP-0 sequence (design: `docs/design/selection-policy-pipeline.md`); the write-side recorder, connection-cooldown, model-lockout, engine `resume_after`/`AllGated` (post-#39), and builder wiring are subsequent plans.

**Tech Stack:** Rust, `crates/gateway` (`sensei-gateway`) + `crates/kernel`. Tests are `#[cfg(test)]` modules run with `cargo test -p sensei-gateway`. Lint gate on commit: `cargo fmt --all --check` + `cargo clippy --workspace --all-targets -- -D warnings` (the repo pre-commit hook runs these).

**Scope guard:** This plan is **behavior-preserving** — no new health behavior, no engine record-site changes beyond the `ModelSelectionService` constructor call. It ends with the identical routing behavior, expressed as a pipeline. It is `#39`-independent except a 2-line constructor-call change at the two `ModelSelectionService::new` sites.

---

## File Structure

- **Create `crates/gateway/src/skip_reason.rs`** — the typed `SkipReason` enum + `SkippedCandidate { model, router, reason: SkipReason }`. One responsibility: the vocabulary of "why a candidate was rejected."
- **Create `crates/gateway/src/gates/mod.rs`** — `AdmissionGate` trait, `GateVerdict`, `CandidateView`, `SelectionCtx`, and the endpoint read port `EndpointHealthRead`.
- **Create `crates/gateway/src/gates/capability.rs`, `budget.rs`, `circuit_breaker_gate.rs`** — one gate per file (SRP).
- **Create `crates/gateway/src/strategy.rs`** — `RoutingStrategy` trait + `PriorityStrategy`.
- **Modify `crates/gateway/src/selection.rs`** — `ModelSelectionService` runs the pipeline; `resolve` emits structural `SkipReason`s; delete the inline check duplication.
- **Modify `crates/gateway/src/circuit_breaker.rs`** — `impl EndpointHealthRead for CircuitBreakerManager`.
- **Modify `crates/gateway/src/lib.rs`** — `mod skip_reason; mod gates; mod strategy;` and re-exports as needed.
- **Modify `crates/gateway/src/engine.rs`** — update the two `ModelSelectionService::new(...)` call sites to the new constructor (mechanical).

---

### Task 1: Typed `SkipReason` (replace the stringly-typed reason)

**Files:**
- Create: `crates/gateway/src/skip_reason.rs`
- Modify: `crates/gateway/src/lib.rs` (add `mod skip_reason;`)
- Modify: `crates/gateway/src/selection.rs` (re-export/import; change `SkippedCandidate.reason` type)
- Test: `crates/gateway/src/skip_reason.rs` (`#[cfg(test)]`)

- [ ] **Step 1: Write the failing test**

```rust
// crates/gateway/src/skip_reason.rs
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn skip_reason_renders_and_classifies() {
        assert_eq!(SkipReason::RouterDisabled.to_string(), "router disabled");
        assert!(matches!(
            SkipReason::UnsupportedCapability(crate::types::capability::Capability::TextEmbed),
            SkipReason::UnsupportedCapability(_)
        ));
        // structural reasons are not "timed" (no wake-up instant)
        assert!(SkipReason::ModelNotFound.until().is_none());
        assert!(!SkipReason::RouterNotFound.is_terminal());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p sensei-gateway skip_reason`
Expected: FAIL — `skip_reason` module / `SkipReason` not found.

- [ ] **Step 3: Write minimal implementation**

```rust
// crates/gateway/src/skip_reason.rs
use crate::types::capability::Capability;
use std::time::Instant;

/// Why a candidate was rejected during selection. Structural variants come from
/// resolution; the timed/health variants (CircuitOpen/Cooling/LockedOut) are
/// added by later SP-0 plans. `until()`/`is_terminal()` let the engine aggregate
/// a wake-up time on full exhaustion.
#[derive(Debug, Clone)]
pub enum SkipReason {
    ModelNotFound,
    RouterNotFound,
    RouterDisabled,
    UnsupportedCapability(Capability),
    OverBudget { estimated: f64, budget: f64 },
    CircuitOpen { until: Instant },
}

impl SkipReason {
    /// Earliest instant this candidate could become eligible again, if timed.
    pub fn until(&self) -> Option<Instant> {
        match self {
            SkipReason::CircuitOpen { until } => Some(*until),
            _ => None,
        }
    }
    /// A reason that will never clear on its own (needs config/human action).
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            SkipReason::ModelNotFound | SkipReason::RouterNotFound | SkipReason::UnsupportedCapability(_)
        )
    }
}

impl std::fmt::Display for SkipReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SkipReason::ModelNotFound => write!(f, "model not found"),
            SkipReason::RouterNotFound => write!(f, "router not found"),
            SkipReason::RouterDisabled => write!(f, "router disabled"),
            SkipReason::UnsupportedCapability(c) => write!(f, "does not support {c:?}"),
            SkipReason::OverBudget { estimated, budget } => {
                write!(f, "over budget (estimated {estimated:.4}, budget {budget:.4})")
            }
            SkipReason::CircuitOpen { .. } => write!(f, "circuit breaker open"),
        }
    }
}
```
Add `mod skip_reason;` to `crates/gateway/src/lib.rs` and `pub use skip_reason::SkipReason;` if the crate re-exports selection types.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p sensei-gateway skip_reason`
Expected: PASS.

- [ ] **Step 5: Migrate `SkippedCandidate.reason` to `SkipReason` and fix call sites**

In `selection.rs`, change `pub reason: String` → `pub reason: SkipReason`, and replace every `reason: "router not found".to_string()` etc. with the enum variant. Update the existing `selection.rs` tests that assert `reason.contains("...")` to match the typed variant, e.g.:
```rust
assert!(matches!(result.skipped[0].reason, SkipReason::RouterNotFound));
```
Keep the Display text identical to today's strings so any external log format is unchanged.

- [ ] **Step 6: Run the full selection suite**

Run: `cargo test -p sensei-gateway selection`
Expected: PASS (all existing selection tests green with typed reasons).

- [ ] **Step 7: Commit**

```bash
git add crates/gateway/src/skip_reason.rs crates/gateway/src/lib.rs crates/gateway/src/selection.rs
git commit -m "refactor(gateway): typed SkipReason replaces stringly-typed selection reason"
```

---

### Task 2: Pin direct↔chain parity BEFORE merging the two validate paths

**Files:**
- Modify: `crates/gateway/src/selection.rs` (`#[cfg(test)]` — add tests only; no production change)

Rationale (design §0/M6): `validate_direct` is router-first and does *not* fall back to `model.provider`; `validate_chain_entry` is model-first and does. Merging them naively changes behavior for two uncovered cases. Pin them now so the Task-4 refactor can't silently regress.

- [ ] **Step 1: Write the failing/characterization tests**

```rust
#[test]
fn direct_both_router_and_model_unknown_reports_router_first() {
    let config = test_config();
    let cb = test_cb();
    let svc = ModelSelectionService::new(&config, &cb);
    let result = svc.select(&SelectionCriteria {
        capability: Capability::TextChat,
        model: Some("ghost".into()), router: Some("nope".into()),
        chain: None, budget: None, input_tokens: None,
    });
    // Current behavior: direct validates router first.
    assert!(matches!(result.skipped[0].reason, SkipReason::RouterNotFound));
}

#[test]
fn direct_model_only_no_router_is_router_not_found_today() {
    let config = test_config();
    let cb = test_cb();
    let svc = ModelSelectionService::new(&config, &cb);
    let result = svc.select(&SelectionCriteria {
        capability: Capability::TextChat,
        model: Some("gemma3:27b".into()), router: None,
        chain: None, budget: None, input_tokens: None,
    });
    // Direct does NOT provider-fallback today → empty router → "router not found".
    assert!(result.selected.is_none());
    assert!(matches!(result.skipped[0].reason, SkipReason::RouterNotFound));
}
```

- [ ] **Step 2: Run to verify they pass against current code** (characterization — they encode today's behavior)

Run: `cargo test -p sensei-gateway selection::tests::direct_`
Expected: PASS. If either FAILS, stop — current behavior differs from the assumption; record the real behavior in the test and in the design doc before proceeding.

- [ ] **Step 3: Commit**

```bash
git add crates/gateway/src/selection.rs
git commit -m "test(gateway): pin direct-vs-chain selection parity before pipeline refactor"
```

---

### Task 3: `AdmissionGate` trait + `CandidateView` + `SelectionCtx` + endpoint read port

**Files:**
- Create: `crates/gateway/src/gates/mod.rs`
- Modify: `crates/gateway/src/lib.rs` (`mod gates;`)
- Test: `crates/gateway/src/gates/mod.rs` (`#[cfg(test)]`)

- [ ] **Step 1: Write the failing test** (a fake gate proves the verdict contract)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    struct AlwaysSkip;
    impl AdmissionGate for AlwaysSkip {
        fn name(&self) -> &'static str { "always_skip" }
        fn evaluate(&self, _c: &CandidateView<'_>, _x: &SelectionCtx<'_>) -> GateVerdict {
            GateVerdict::Skip(crate::skip_reason::SkipReason::RouterDisabled)
        }
    }
    #[test]
    fn gate_can_skip() {
        let g = AlwaysSkip;
        assert_eq!(g.name(), "always_skip");
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p sensei-gateway gates`
Expected: FAIL — `gates` module missing.

- [ ] **Step 3: Write minimal implementation**

```rust
// crates/gateway/src/gates/mod.rs
use crate::skip_reason::SkipReason;
use crate::types::config::{GatewayConfig, ModelConfig, RouterConfig};
use crate::types::capability::Capability;
use std::time::Instant;

pub mod capability;
pub mod budget;
pub mod circuit_breaker_gate;

/// Read port for endpoint health (the circuit breaker implements it; cooldown /
/// lockout ports arrive in later SP-0 plans).
pub trait EndpointHealthRead: Send + Sync {
    /// `Some(until)` if the endpoint is currently open/unavailable with a retry time.
    fn open_until(&self, endpoint: &str) -> Option<Instant>;
}

/// A resolved candidate ready for gating (structural resolution already succeeded).
pub struct CandidateView<'a> {
    pub model: &'a str,
    pub router: &'a str,
    pub endpoint: String,              // "router:model" (opaque key; grows later)
    pub model_config: &'a ModelConfig,
    pub router_config: &'a RouterConfig,
}

pub struct SelectionCtx<'a> {
    pub capability: Capability,
    pub budget: Option<f64>,
    pub input_tokens: Option<u32>,
    pub health: &'a dyn EndpointHealthRead,
    pub now: Instant,                  // injected clock (deterministic tests)
    pub config: &'a GatewayConfig,
}

pub enum GateVerdict { Admit, Skip(SkipReason) }

pub trait AdmissionGate: Send + Sync {
    fn name(&self) -> &'static str;
    fn evaluate(&self, cand: &CandidateView<'_>, ctx: &SelectionCtx<'_>) -> GateVerdict;
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p sensei-gateway gates`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/gateway/src/gates/mod.rs crates/gateway/src/lib.rs
git commit -m "feat(gateway): AdmissionGate trait + CandidateView/SelectionCtx + endpoint read port"
```

---

### Task 4: The three structural/existing gates (capability, budget, circuit breaker)

**Files:**
- Create: `crates/gateway/src/gates/capability.rs`, `budget.rs`, `circuit_breaker_gate.rs`
- Modify: `crates/gateway/src/circuit_breaker.rs` (`impl EndpointHealthRead`)
- Test: each gate file (`#[cfg(test)]`)

- [ ] **Step 1: Write failing tests (one per gate)**

```rust
// gates/capability.rs
#[test]
fn skips_unsupported_capability() {
    // build a CandidateView for a TextEmbed-only model, ctx.capability = AudioTranscribe
    // assert matches!(gate.evaluate(&cand, &ctx), GateVerdict::Skip(SkipReason::UnsupportedCapability(_)))
}
// gates/budget.rs
#[test]
fn skips_over_budget_and_computes_cost_lazily() {
    // model with pricing; ctx.budget very small, input_tokens = 1000
    // assert Skip(OverBudget { .. })
}
// gates/circuit_breaker_gate.rs
#[test]
fn skips_when_breaker_open() {
    // a fake EndpointHealthRead returning Some(now+60s) → Skip(CircuitOpen { until })
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `cargo test -p sensei-gateway gates::`
Expected: FAIL (gate types missing).

- [ ] **Step 3: Implement the gates + breaker port**

```rust
// gates/capability.rs
pub struct CapabilityGate;
impl AdmissionGate for CapabilityGate {
    fn name(&self) -> &'static str { "capability" }
    fn evaluate(&self, c: &CandidateView<'_>, x: &SelectionCtx<'_>) -> GateVerdict {
        if c.model_config.capabilities.contains(&x.capability) { GateVerdict::Admit }
        else { GateVerdict::Skip(SkipReason::UnsupportedCapability(x.capability)) }
    }
}

// gates/budget.rs — cost estimation moves here (lazy; only for admitted-so-far candidates)
pub struct BudgetGate;
impl BudgetGate {
    fn estimate(mc: &ModelConfig, input_tokens: u32) -> Option<f64> {
        let p = mc.pricing.as_ref()?;
        Some(input_tokens as f64 * p.input_per_1k / 1000.0
             + mc.max_output_tokens as f64 * p.output_per_1k / 1000.0)
    }
}
impl AdmissionGate for BudgetGate {
    fn name(&self) -> &'static str { "budget" }
    fn evaluate(&self, c: &CandidateView<'_>, x: &SelectionCtx<'_>) -> GateVerdict {
        match (x.budget, Self::estimate(c.model_config, x.input_tokens.unwrap_or(0))) {
            (Some(b), Some(est)) if est > b => GateVerdict::Skip(SkipReason::OverBudget { estimated: est, budget: b }),
            _ => GateVerdict::Admit,
        }
    }
}

// gates/circuit_breaker_gate.rs
pub struct CircuitBreakerGate;
impl AdmissionGate for CircuitBreakerGate {
    fn name(&self) -> &'static str { "circuit_breaker" }
    fn evaluate(&self, c: &CandidateView<'_>, x: &SelectionCtx<'_>) -> GateVerdict {
        match x.health.open_until(&c.endpoint) {
            Some(until) => GateVerdict::Skip(SkipReason::CircuitOpen { until }),
            None => GateVerdict::Admit,
        }
    }
}
```
```rust
// circuit_breaker.rs — implement the read port (no behavior change; derives from get_state)
impl crate::gates::EndpointHealthRead for CircuitBreakerManager {
    fn open_until(&self, endpoint: &str) -> Option<std::time::Instant> {
        // can_execute() has the HalfOpen transition side effect; the gate must be a
        // pure read, so derive from state: Open&&now<next_retry → Some(next_retry).
        match self.get_state(endpoint) {
            BreakerState::Open { next_retry } if std::time::Instant::now() < next_retry => Some(next_retry),
            _ => None,
        }
    }
}
```
> Note: preserves today's semantics (Open before timeout blocks; HalfOpen/Closed admit). The existing `can_execute` stays for the engine record path until the write-side plan.

- [ ] **Step 4: Run to verify they pass**

Run: `cargo test -p sensei-gateway gates::`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/gateway/src/gates/ crates/gateway/src/circuit_breaker.rs
git commit -m "feat(gateway): capability/budget/circuit-breaker gates + breaker read port"
```

---

### Task 5: `PriorityStrategy` (the one ordering seam)

**Files:**
- Create: `crates/gateway/src/strategy.rs`
- Modify: `crates/gateway/src/lib.rs` (`mod strategy;`)
- Test: `crates/gateway/src/strategy.rs`

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn priority_strategy_sorts_ascending_by_priority() {
    let mut v = vec![sm("b", 2), sm("a", 1)]; // helper builds SelectedModel with priority
    PriorityStrategy.order(&mut v);
    assert_eq!(v[0].model, "a");
    assert_eq!(v[1].model, "b");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p sensei-gateway strategy`
Expected: FAIL.

- [ ] **Step 3: Implement**

```rust
// crates/gateway/src/strategy.rs
use crate::selection::SelectedModel;
pub trait RoutingStrategy: Send + Sync { fn order(&self, admitted: &mut Vec<SelectedModel>); }
pub struct PriorityStrategy;
impl RoutingStrategy for PriorityStrategy {
    fn order(&self, admitted: &mut Vec<SelectedModel>) {
        admitted.sort_by_key(|m| m.priority); // stable; identical to today's resolve_chain sort
    }
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p sensei-gateway strategy`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/gateway/src/strategy.rs crates/gateway/src/lib.rs
git commit -m "feat(gateway): PriorityStrategy as the single ordering seam"
```

---

### Task 6: `ModelSelectionService` runs the pipeline (resolve → gates → order)

**Files:**
- Modify: `crates/gateway/src/selection.rs`
- Modify: `crates/gateway/src/engine.rs` (two `ModelSelectionService::new` call sites)

- [ ] **Step 1: Keep the existing selection suite as the regression gate**

The full `selection.rs` test module (tier1/2/3, skips_disabled_router, skips_circuit_breaker_open, skips_over_budget, api_model_id_override, parity tests from Task 2) is the behavior contract. No new test needed here beyond re-running it green after the refactor.

- [ ] **Step 2: Refactor `resolve_candidates` to build views, run gates, then order**

```rust
// selection.rs (sketch)
pub struct ModelSelectionService<'a> {
    config: &'a GatewayConfig,
    gates: Vec<Box<dyn AdmissionGate>>,     // [Capability, CircuitBreaker, Budget]
    health: &'a dyn EndpointHealthRead,
    strategy: Box<dyn RoutingStrategy>,
}
impl<'a> ModelSelectionService<'a> {
    pub fn new(config: &'a GatewayConfig, cb: &'a CircuitBreakerManager) -> Self {
        Self {
            config,
            gates: vec![Box::new(CapabilityGate), Box::new(CircuitBreakerGate), Box::new(BudgetGate)],
            health: cb,                       // CircuitBreakerManager: EndpointHealthRead
            strategy: Box::new(PriorityStrategy),
        }
    }
    fn admit(&self, view: CandidateView<'_>, ctx: &SelectionCtx<'_>) -> Result<SelectedModel, SkippedCandidate> {
        for g in &self.gates {
            if let GateVerdict::Skip(reason) = g.evaluate(&view, ctx) {
                return Err(SkippedCandidate { model: view.model.into(), router: view.router.into(), reason });
            }
        }
        Ok(view.into_selected(/* priority, api_model_id resolved by the resolver */))
    }
    // resolve_direct / resolve_chain now build a CandidateView (emitting structural
    // SkipReasons on failure) and call `admit`; `resolve_chain` calls
    // `self.strategy.order(&mut all_candidates)` instead of the hardcoded sort.
}
```
Preserve parity (Task 2): the **direct** view builder sets router = pinned (no provider fallback), `priority = 1`, `api_model_id` from model-config only; the **chain** view builder resolves `entry.router → model.provider`, `priority = entry.priority`, `api_model_id = entry → model → id`.

- [ ] **Step 3: Update the two engine constructor call sites**

In `engine.rs`, `ModelSelectionService::new(&config, &self.circuit_breaker)` is unchanged in signature (still `(config, cb)`), so the call sites need no change unless the ctor signature changed — confirm both `engine.rs` sites compile.

- [ ] **Step 4: Run the whole gateway suite**

Run: `cargo test -p sensei-gateway`
Expected: PASS — identical routing behavior, now via the pipeline. Pay special attention to `skips_circuit_breaker_open`, `skips_over_budget`, `skips_disabled_router`, and the Task-2 parity tests.

- [ ] **Step 5: Verify no behavior drift on the reason strings**

Run: `cargo test -p sensei-gateway` and confirm the `Display` output of each `SkipReason` still matches the old substrings (a downstream log-format guard).

- [ ] **Step 6: Commit**

```bash
git add crates/gateway/src/selection.rs crates/gateway/src/engine.rs
git commit -m "refactor(gateway): ModelSelectionService runs the gate pipeline (behavior-preserving)"
```

---

## Self-Review

- **Spec coverage:** implements design `selection-policy-pipeline.md` §2.1 (resolve→gate→order), §2.2 (gate order), the `PriorityStrategy` one-ordering-seam (M4), typed `SkipReason` (§2), `CandidateView` fallible-resolver + lazy cost (H4), breaker-as-read-port (H2, endpoint half). **Deferred to later plans (by design):** `HealthRecorder`/write-side, `Cooling`/`LockedOut` variants + cooldown/lockout gates, adapter-boundary `classify` (C2), engine `resume_after`/`AllGated` (C3/C4, post-#39), `SelectionObserver`/`on_lockout` + `apply_lockout` (§5c), builder `.with_*` + `ResilienceConfig`, `RoutingStrategy` tier tag (M4, SP-CAT). These are named here so the deferral is explicit, not a gap.
- **Placeholder scan:** none — every code step shows the code; sketches in Task 6 are marked as sketches over the concrete types in `selection.rs`, and the test suite is the contract.
- **Type consistency:** `SkipReason` (Task 1) is used by gates (Task 4) and the service (Task 6); `EndpointHealthRead::open_until` (Task 3) is implemented by the breaker (Task 4) and consumed by `CircuitBreakerGate` (Task 4) and `SelectionCtx` (Task 3); `RoutingStrategy::order(&mut Vec<SelectedModel>)` (Task 5) matches its call in Task 6.
- **Sequencing:** 1 → 2 (pin parity) → 3 (trait) → 4 (gates) → 5 (strategy) → 6 (wire) — each compiles and tests green on its own; each commit is behavior-preserving.

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-08-06-sp0-foundation-policy-pipeline.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — dispatch a fresh subagent per task, review between tasks, fast iteration (REQUIRED SUB-SKILL: superpowers:subagent-driven-development).

**2. Inline Execution** — execute tasks in this session with checkpoints (REQUIRED SUB-SKILL: superpowers:executing-plans).

**Next plans in the SP-0 sequence** (each its own `docs/superpowers/plans/…` file, each shippable green): (b) write-side `HealthRecorder` fan-out at the four engine outcome sites (breaker-only, behavior-preserving); (c) adapter-boundary `ProviderSignal` + `classify` + connection-cooldown gate/recorder; (d) model-lockout gate/recorder (per-reason, provenance clamp, escalation guard, `on_lockout`/`apply_lockout`); (e) **post-#39** engine `resume_after` (wall-clock) + `AllGated` + both dispatch paths; (f) builder `.with_gate/.with_recorder/.with_observer/.with_resilience` + `ResilienceConfig`.
