# SP-0 (d) — Model Lockout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Add the second real health gate — **model lockout**: after a *provider-side* limit signal (429 / 403-quota / credits / 401-auth) on a specific `router:model`, classify it and temporarily (or terminally) remove **that one model** from selection so the candidate walk falls over to the next entry on subsequent requests. The gateway **announces** each lockout via an `on_lockout` callback (the caller persists — the gateway is tenant-agnostic and never persists) and accepts `apply_lockout`/`clear_lockout` to re-seed or suspend.

**Architecture:** Mirrors connection-cooldown (SP-0 c) at **endpoint** (`router:model`) granularity, adding *reason classification* on top:
- **Classifiable errors** — enrich the shared adapter boundary (`cloud-providers/base.rs`) so a 403 preserves its status + body (no longer collapsed into `Authentication`) and a 429 carries `Retry-After`. This is the design's "classify at the adapter boundary" decision (C2).
- **`classify(&GatewayError) -> Option<LockReason>`** — one pure fn, table-tested, the single source of truth for "is this a provider limit, and which".
- **`ModelLockoutStore`** (in-mem, per-endpoint, Arc-backed/Clone) read by a new **`ModelLockoutGate`** and written by a new **`ModelLockoutSink`** (`HealthRecorder`) — the same shared-store pattern as `ConnectionCooldownStore`.
- **`SelectionObserver::on_lockout`** callback + `Gateway::apply_lockout`/`clear_lockout` — the tenant-agnostic durability seam (design §5c): OUT (gateway → caller persists) and IN (caller → gateway re-seeds).

**Deferred to later plans (NOT this one) — noted so the gaps aren't silent:**
- **In-flight walk recoverable-classification (design §3.1)** — making a 403-quota fall over on the *same* request (not just the next one). Today a 403-quota still hard-fails the current request until this lands; the *next* request skips the locked model. This is an engine hot-path change grouped with **plan (e)**.
- **`resume_after` / `GatewayError::AllGated` (design §3.3)** — the terminal "all candidates gated" error carrying a wall-clock wake-up. **Plan (e).** Consequently `HealthRecorder::on_outcome` keeps returning `()` here (the design's `Option<Instant>` C4 return arrives with (e), which needs it for post-walk aggregation).
- **Injected calendar clock** for exact quota reset boundaries, and **injected seedable jitter RNG** for escalation — **plan (e)/(f)**. Here quota uses a fixed default duration and escalation is deterministic (no jitter).
- **Builder consolidation** (`.with_gate`/`.with_recorder`/`.with_resilience`) — **plan (f)**. Here `ModelSelectionService::new` gains a 4th positional read-port arg and `Gateway` gains a `with_observer` builder method, consistent with how (c) added `router_health`.
- **`EndpointKey { router, model }`** opaque key — the store keys by the existing `endpoint: &str` (`"router:model"`) string, exactly like the circuit breaker. Introducing `EndpointKey` is a cross-cutting refactor left for (f).

**Tech Stack:** Rust, `crates/gateway` + `crates/cloud-providers`. Contract per commit: existing **191 gateway lib tests** stay green (except assertions deliberately updated in Task 1/4, called out inline), the `cloud-providers` suite stays green (Task 1 updates two boundary assertions), and `make check` clean (fmt + clippy `-D warnings`). New behavior is proven by new tests.

---

## File Structure

- **Modify `crates/cloud-providers/src/base.rs`** — `map_status_error` gains a `retry_after_ms: Option<u64>` param; **401 → `Authentication`, 403 → `ProviderError { status: Some(403) }`** (stop collapsing 403 into `Authentication`); `http_json` + `error_from_response` parse the `Retry-After` header into `RateLimit.retry_after_ms`. (Task 1)
- **Create `crates/gateway/src/gates/lockout.rs`** — everything lockout, cohesive in one file (mirrors `gates/cooldown.rs`): `LockReason`, `classify()`, `ModelLockoutStore` (+ `ModelLockoutRead`), `LockView`, `ModelLockoutGate`, `ModelLockoutPolicy`, `ModelLockoutSink`, `SelectionObserver`, `LockoutBroadcaster`. (Tasks 2–6)
- **Modify `crates/gateway/src/skip_reason.rs`** — add `SkipReason::LockedOut { reason: LockReason, until: Option<Instant> }` (+ Display). (Task 3)
- **Modify `crates/gateway/src/gates/mod.rs`** — `pub mod lockout;`; add `model_lockout: &'a dyn ModelLockoutRead` to `SelectionCtx`. (Tasks 2–4)
- **Modify `crates/gateway/src/selection.rs`** — `ModelSelectionService` holds a `model_lockout` port; `new(config, cb, router_health, model_lockout)`; insert `ModelLockoutGate` into the gate vec **after `CircuitBreakerGate`, before `BudgetGate`** (order: capability → cooldown → breaker → lockout → budget); pass `model_lockout` into `SelectionCtx`; update all in-file test call sites. (Task 4)
- **Modify `crates/gateway/src/engine/mod.rs`** — `Gateway.model_lockout: ModelLockoutStore` + `Gateway.lockout_observers: LockoutBroadcaster` (both Arc-backed, built in `new`, shared into the sink); register `ModelLockoutSink` in `recorders`; `with_observer`, `apply_lockout`, `clear_lockout`; `refresh_router_keys` clears terminal locks. (Tasks 4–6)
- **Modify `crates/gateway/src/engine/execute.rs:54`, `stream.rs:77`** — pass `&self.model_lockout` as the 4th arg to `ModelSelectionService::new`. (Task 4)
- **`crates/gateway/src/engine/tests.rs`** — integration tests (Tasks 5–6). Outcome sites (`execute.rs:264/337`, `stream.rs:182/270`) are **unchanged** — `AttemptOutcome` already carries `endpoint`/`error`/`success`, which is all the sink needs.

---

### Task 1: Classifiable adapter boundary — preserve 403 status+body, parse `Retry-After`

**Why:** classification (Task 2) can only distinguish a 403-quota (recoverable, demote) from a 401-auth (terminal) if the boundary preserves the 403 **status + body**. Today `base.rs` collapses **both** 401 and 403 into `GatewayError::Authentication { message }` (dropping the status) and always sets `RateLimit.retry_after_ms = None`. This task makes the error carry what `classify` needs. **Deliberate behavior change:** a 403 now surfaces as `ProviderError { status: Some(403) }` instead of `Authentication`. Rationale: a 403 is provider-side and often a quota/permission signal that *should* be classifiable and (later) fall over; a genuine 401 (bad/missing key) stays `Authentication`.

**Files:** `crates/cloud-providers/src/base.rs` (+ its tests).

- [ ] **Step 1: Update the failing boundary tests** (edit the two existing tests + add one).
  - Rename/rewrite `map_status_error_maps_401_403_to_authentication` → `map_status_error_maps_401_to_authentication_403_to_provider_error`:
```rust
#[test]
fn map_status_error_maps_401_to_authentication_403_to_provider_error() {
    match map_status_error("acme", 401, "bad key".into(), None) {
        GatewayError::Authentication { adapter, message } => {
            assert_eq!(adapter, "acme");
            assert_eq!(message, "bad key");
        }
        other => panic!("expected Authentication for 401, got {other:?}"),
    }
    match map_status_error("acme", 403, "quota exceeded".into(), None) {
        GatewayError::ProviderError { adapter, message, status } => {
            assert_eq!(adapter, "acme");
            assert_eq!(message, "quota exceeded");
            assert_eq!(status, Some(403));
        }
        other => panic!("expected ProviderError for 403, got {other:?}"),
    }
}
```
  - Update `map_status_error_maps_429_to_rate_limit` to pass the new arg and assert the header is threaded:
```rust
#[test]
fn map_status_error_429_threads_retry_after() {
    match map_status_error("acme", 429, "slow down".into(), Some(1500)) {
        GatewayError::RateLimit { adapter, retry_after_ms } => {
            assert_eq!(adapter, "acme");
            assert_eq!(retry_after_ms, Some(1500));
        }
        other => panic!("expected RateLimit, got {other:?}"),
    }
}
```
  - Add a pure test for the header parser (Step 3 introduces `parse_retry_after_ms`):
```rust
#[test]
fn parse_retry_after_seconds_and_missing() {
    assert_eq!(parse_retry_after_ms(Some("2")), Some(2000));
    assert_eq!(parse_retry_after_ms(Some("0")), Some(0));
    assert_eq!(parse_retry_after_ms(Some("not-a-number")), None); // HTTP-date form unsupported → None (falls back to policy backoff)
    assert_eq!(parse_retry_after_ms(None), None);
}
```
- [ ] **Step 2:** `cargo test -p sensei-cloud-providers base` → FAIL (arity/variant mismatch, missing `parse_retry_after_ms`).
- [ ] **Step 3: Implement** in `base.rs`:
  - Add the parser (integer-seconds form only; HTTP-date is rare for these providers and returns `None` → the lockout policy falls back to its backoff, which is safe):
```rust
/// Parse a `Retry-After` header value (integer seconds) to milliseconds.
/// The HTTP-date form is not supported (returns `None`); callers then fall
/// back to the lockout policy's synthetic backoff.
fn parse_retry_after_ms(header: Option<&str>) -> Option<u64> {
    header?.trim().parse::<u64>().ok().map(|secs| secs * 1000)
}
```
  - Change `map_status_error`'s signature and body:
```rust
fn map_status_error(
    adapter: &str,
    status: u16,
    body_text: String,
    retry_after_ms: Option<u64>,
) -> GatewayError {
    match status {
        401 => GatewayError::Authentication { adapter: adapter.into(), message: body_text },
        429 => GatewayError::RateLimit { adapter: adapter.into(), retry_after_ms },
        // 403 (and every other non-success) preserves the status + body so the
        // lockout classifier can disambiguate quota/credits/forbidden.
        code => GatewayError::ProviderError { adapter: adapter.into(), message: body_text, status: Some(code) },
    }
}
```
  - `error_from_response` reads the header before consuming the body:
```rust
pub async fn error_from_response(adapter: &str, response: reqwest::Response) -> GatewayError {
    let status = response.status().as_u16();
    let retry_after_ms = parse_retry_after_ms(
        response.headers().get(reqwest::header::RETRY_AFTER).and_then(|v| v.to_str().ok()),
    );
    let body_text = response.text().await.unwrap_or_default();
    map_status_error(adapter, status, body_text, retry_after_ms)
}
```
  - In `http_json`, capture the header before `response.text()` and replace the inline 401/403/429 block with the shared mapper so the two paths stay identical:
```rust
    if !status.is_success() {
        let retry_after_ms = parse_retry_after_ms(
            response.headers().get(reqwest::header::RETRY_AFTER).and_then(|v| v.to_str().ok()),
        );
        let body_text = response.text().await.unwrap_or_default();
        let message = extract_error_message(&body_text).unwrap_or(body_text.clone());
        // Preserve the extracted `message` for RateLimit/Auth/ProviderError alike.
        return Err(match status.as_u16() {
            401 => GatewayError::Authentication { adapter: "http".into(), message },
            429 => GatewayError::RateLimit { adapter: "http".into(), retry_after_ms },
            code => GatewayError::ProviderError { adapter: "http".into(), message, status: Some(code) },
        });
    }
```
- [ ] **Step 4: Sweep for 403-dependent handling.** `git grep -n 'GatewayError::Authentication' -- crates/` and confirm nothing keyed *behavior* on a 403 arriving as `Authentication` (e.g. an auth-refresh trigger). If a call site matched `Authentication` to mean "403 forbidden", adjust it to also consider `ProviderError { status: Some(403) }`. Report findings; if a per-adapter test asserts `403 → Authentication`, update it to `ProviderError { status: Some(403) }` (same rationale).
- [ ] **Step 5: Verify** — `cargo test -p sensei-cloud-providers` green; `cargo test --workspace` green (fix any per-adapter 403 assertions surfaced in Step 4); clippy `-D warnings` + fmt clean.
- [ ] **Step 6: Commit:** `feat(cloud-providers): preserve 403 status+body and parse Retry-After at the adapter boundary`.

---

### Task 2: `LockReason` + `classify()` — the one pure classifier

**Files:** create `crates/gateway/src/gates/lockout.rs`; `crates/gateway/src/gates/mod.rs` (`pub mod lockout;`).

- [ ] **Step 1: Failing table test** (in `gates/lockout.rs`):
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::error::GatewayError;

    fn provider(status: u16, msg: &str) -> GatewayError {
        GatewayError::ProviderError { adapter: "a".into(), message: msg.into(), status: Some(status) }
    }

    #[test]
    fn classify_maps_provider_limits() {
        // 429 → rate limit (recoverable)
        let rl = GatewayError::RateLimit { adapter: "a".into(), retry_after_ms: Some(1000) };
        assert_eq!(classify(&rl), Some(LockReason::RateLimit));
        assert!(LockReason::RateLimit.is_recoverable());

        // 403 + quota body → quota (recoverable)
        assert_eq!(classify(&provider(403, "You exceeded your quota")), Some(LockReason::QuotaExhausted));
        assert!(LockReason::QuotaExhausted.is_recoverable());

        // 403 + credits/billing body → credits (terminal)
        assert_eq!(classify(&provider(403, "insufficient credits, please add billing")), Some(LockReason::CreditsExhausted));
        assert!(LockReason::CreditsExhausted.is_terminal());

        // 402 Payment Required → credits (terminal)
        assert_eq!(classify(&provider(402, "payment required")), Some(LockReason::CreditsExhausted));

        // 403 bare forbidden (no quota/credit keywords) → auth (terminal)
        assert_eq!(classify(&provider(403, "forbidden")), Some(LockReason::Auth));

        // 401 → auth (terminal)
        let auth = GatewayError::Authentication { adapter: "a".into(), message: "bad key".into() };
        assert_eq!(classify(&auth), Some(LockReason::Auth));
        assert!(LockReason::Auth.is_terminal());

        // Non-limit signals → None (no lockout)
        assert_eq!(classify(&provider(500, "boom")), None);
        assert_eq!(classify(&GatewayError::Timeout { adapter: "a".into(), model: "m".into(), duration_ms: 1 }), None);
        assert_eq!(classify(&GatewayError::ModelUnavailable { adapter: "a".into(), model: "m".into() }), None);
    }
}
```
- [ ] **Step 2:** `cargo test -p sensei-gateway lockout` → FAIL (types missing).
- [ ] **Step 3: Implement** (top of `gates/lockout.rs`):
```rust
use crate::types::error::GatewayError;

/// Why a `router:model` is (or should be) locked out. **Provider-side only** —
/// distinct from the caller's subscription `GatewayError::QuotaExceeded`, which
/// is a subject hard-stop and never produces a lockout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockReason {
    /// 429 — recovers fast (short cooldown, honoring Retry-After if present).
    RateLimit,
    /// Provider quota for this model exhausted (403 + quota body) — recovers at a
    /// reset boundary; demotes to the next tier.
    QuotaExhausted,
    /// Out of credits / billing (403 credit body, or 402) — terminal until a
    /// human tops up.
    CreditsExhausted,
    /// 401, or a bare 403 forbidden — terminal until the credential changes.
    Auth,
}

impl LockReason {
    /// Recoverable reasons demote-and-retry (the model comes back). Terminal
    /// reasons need human action (top-up / rotate key) and carry `until: None`.
    pub fn is_recoverable(&self) -> bool {
        matches!(self, LockReason::RateLimit | LockReason::QuotaExhausted)
    }
    pub fn is_terminal(&self) -> bool {
        !self.is_recoverable()
    }
}

/// Classify a provider error into a lockout reason, or `None` when it is not a
/// provider limit signal. The single source of truth shared by the (future)
/// in-flight walk and the next-request `ModelLockoutSink` — they cannot disagree.
/// Reads the boundary-enriched error (Task 1): 403 preserves status+body; 429
/// carries retry-after.
pub fn classify(err: &GatewayError) -> Option<LockReason> {
    match err {
        GatewayError::RateLimit { .. } => Some(LockReason::RateLimit),
        GatewayError::Authentication { .. } => Some(LockReason::Auth), // 401
        GatewayError::ProviderError { status: Some(402), .. } => Some(LockReason::CreditsExhausted),
        GatewayError::ProviderError { status: Some(403), message, .. } => Some(classify_403_body(message)),
        _ => None,
    }
}

/// A 403 is ambiguous: quota vs credits vs a plain forbidden. Disambiguate by
/// body keywords (providers throttle via non-standard 403 bodies). Order
/// matters: credits/billing before quota, since "you have exceeded your credit
/// limit" contains both senses and the billing sense is terminal.
fn classify_403_body(message: &str) -> LockReason {
    let m = message.to_ascii_lowercase();
    if m.contains("credit") || m.contains("billing") || m.contains("payment") || m.contains("insufficient") {
        LockReason::CreditsExhausted
    } else if m.contains("quota") || m.contains("exceed") || m.contains("exhaust") || m.contains("rate limit") {
        LockReason::QuotaExhausted
    } else {
        LockReason::Auth // bare forbidden → terminal (preserves pre-(d) 403 semantics)
    }
}
```
  - Add `pub mod lockout;` to `gates/mod.rs`.
- [ ] **Step 4:** `cargo test -p sensei-gateway lockout` PASS; full suite 191 unchanged; clippy/fmt clean.
- [ ] **Step 5: Commit:** `feat(gateway): LockReason + classify() — pure provider-limit classifier`.

---

### Task 3: `SkipReason::LockedOut` + `ModelLockoutRead` + `ModelLockoutStore` (read infra)

**Files:** `skip_reason.rs`, `gates/lockout.rs`.

- [ ] **Step 1: Failing tests** (in `gates/lockout.rs`):
```rust
#[test]
fn store_records_and_reads_locks() {
    let s = ModelLockoutStore::new();
    assert!(s.locked("r:m").is_none());                     // unknown → not locked
    let until = std::time::Instant::now() + std::time::Duration::from_secs(60);
    s.set("r:m", LockReason::RateLimit, Some(until));
    let v = s.locked("r:m").expect("locked");
    assert_eq!(v.reason, LockReason::RateLimit);
    assert_eq!(v.until, Some(until));
    // terminal lock: until = None
    s.set("r:x", LockReason::Auth, None);
    assert_eq!(s.locked("r:x").unwrap().until, None);
    // clear removes it
    s.clear("r:m");
    assert!(s.locked("r:m").is_none());
}
```
- [ ] **Step 2:** run → FAIL (types missing).
- [ ] **Step 3: Implement**
  - `skip_reason.rs`: add the variant + Display arm (import `LockReason`):
```rust
    LockedOut { reason: crate::gates::lockout::LockReason, until: Option<Instant> },
```
```rust
    SkipReason::LockedOut { reason, .. } => write!(f, "model locked out ({reason:?})"),
```
  - `gates/lockout.rs` (store + read port):
```rust
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// A single endpoint's lockout state. `until = None` is terminal. (The sink's
/// `escalation` field is added in Task 5, where a reader exists — introducing it
/// here would trip `-D warnings` dead-code.)
#[derive(Debug, Clone, Copy)]
pub(crate) struct LockEntry {
    pub reason: LockReason,
    pub until: Option<Instant>,
}

/// What the gate reads for a candidate: the active lock's reason + deadline.
#[derive(Debug, Clone, Copy)]
pub struct LockView {
    pub reason: LockReason,
    pub until: Option<Instant>, // None = terminal
}

/// Read port for endpoint model-lockout state (read by the gate).
pub trait ModelLockoutRead: Send + Sync {
    /// The endpoint's lock entry (reason + deadline), or `None` if not tracked.
    /// Expiry is NOT applied here — the gate compares `until` to its injected
    /// `now` (mirrors `RouterHealthRead::cooling_until`), and the entry is
    /// retained past expiry so escalation memory survives a release.
    fn locked(&self, endpoint: &str) -> Option<LockView>;
}

/// In-memory per-endpoint (`"router:model"`) lockout state, Arc-backed + Clone so
/// the gate's read reference, the sink's owned copy, and `Gateway`'s
/// apply/clear share one map. Same pattern as `ConnectionCooldownStore`.
#[derive(Clone, Default)]
pub struct ModelLockoutStore {
    locks: Arc<Mutex<HashMap<String, LockEntry>>>,
}

impl ModelLockoutStore {
    pub fn new() -> Self {
        Self::default()
    }
    /// Insert/replace the endpoint's lock. (Task 5 adds an `escalation` param +
    /// `get` when the sink needs them.)
    pub fn set(&self, endpoint: &str, reason: LockReason, until: Option<Instant>) {
        self.locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(endpoint.to_string(), LockEntry { reason, until });
    }
    /// Remove the endpoint's lock (success / `clear_lockout`).
    pub fn clear(&self, endpoint: &str) {
        self.locks.lock().unwrap_or_else(|e| e.into_inner()).remove(endpoint);
    }
}

impl ModelLockoutRead for ModelLockoutStore {
    fn locked(&self, endpoint: &str) -> Option<LockView> {
        self.locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(endpoint)
            .map(|e| LockView { reason: e.reason, until: e.until })
    }
}
```
- [ ] **Step 4:** `cargo test -p sensei-gateway lockout` PASS; full suite 191 unchanged; clippy/fmt clean.
- [ ] **Step 5: Commit:** `feat(gateway): SkipReason::LockedOut + ModelLockoutRead + ModelLockoutStore`.

---

### Task 4: `ModelLockoutGate` + wire into selection (read side)

**Files:** `gates/lockout.rs`, `gates/mod.rs` (`SelectionCtx.model_lockout`), `selection.rs`, `engine/mod.rs`, `engine/execute.rs`, `engine/stream.rs`.

- [ ] **Step 1: Failing gate test** (in `gates/lockout.rs`) — build a store, `set("r:m", Auth, None, 0)` (terminal) and a timed lock, a `CandidateView` with `endpoint: "r:m"`, and a `SelectionCtx` with `model_lockout = &store`, `now`; assert:
  - terminal lock (`until = None`) → `Skip(LockedOut { until: None, .. })`;
  - timed lock with `until > now` → `Skip(LockedOut { until: Some(_), .. })`;
  - timed lock with `until <= now` (expired) → `Admit`;
  - unknown endpoint → `Admit`.
  Reuse the `CandidateView`/`SelectionCtx` construction from `gates/cooldown.rs::gate_reads_cooldown_store` (a `FakeEndpointHealth`, an empty `ConnectionCooldownStore` for `router_health`, and now the lockout `store` for `model_lockout`).
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement**
  - `gates/mod.rs`: add `pub model_lockout: &'a dyn crate::gates::lockout::ModelLockoutRead,` to `SelectionCtx` (after `router_health`).
  - `gates/lockout.rs`:
```rust
use super::{AdmissionGate, CandidateView, GateVerdict, SelectionCtx};
use crate::skip_reason::SkipReason;

/// Gate: the candidate's `router:model` must not be under an active lockout —
/// terminal (`until = None`) or timed (`until > now`). An expired timed lock
/// admits (the entry lingers only for the sink's escalation memory).
pub struct ModelLockoutGate;

impl AdmissionGate for ModelLockoutGate {
    fn name(&self) -> &'static str {
        "model_lockout"
    }
    fn evaluate(&self, c: &CandidateView<'_>, x: &SelectionCtx<'_>) -> GateVerdict {
        match x.model_lockout.locked(&c.endpoint) {
            Some(v) => match v.until {
                None => GateVerdict::Skip(SkipReason::LockedOut { reason: v.reason, until: None }),
                Some(until) if until > x.now => {
                    GateVerdict::Skip(SkipReason::LockedOut { reason: v.reason, until: Some(until) })
                }
                _ => GateVerdict::Admit, // expired timed lock
            },
            None => GateVerdict::Admit,
        }
    }
}
```
  - `selection.rs`: `ModelSelectionService` gains `model_lockout: &'a dyn crate::gates::lockout::ModelLockoutRead`; `new(config, circuit_breaker, router_health, model_lockout)` stores it and inserts `Box::new(ModelLockoutGate)` into the gate vec **after `CircuitBreakerGate`, before `BudgetGate`** (final order: `CapabilityGate, ConnectionCooldownGate, CircuitBreakerGate, ModelLockoutGate, BudgetGate` — matches design §2.2); the `SelectionCtx { .. }` in `admit` gets `model_lockout: self.model_lockout`.
  - Update **every** `ModelSelectionService::new(&config, &cb, &cooldown)` call site in `selection.rs`'s own tests → add a 4th arg. Add one shared helper at the top of the test module to avoid churn:
```rust
    fn test_lockout() -> crate::gates::lockout::ModelLockoutStore {
        crate::gates::lockout::ModelLockoutStore::new()
    }
```
    then each call becomes `ModelSelectionService::new(&config, &cb, &cooldown, &test_lockout())` — **bind the store to a local first** so it outlives the borrow, e.g. `let lockout = test_lockout(); let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);` (a temporary would be dropped while `svc` holds the `&dyn` borrow — the borrow checker will reject the inline form).
  - `engine/mod.rs`: add `model_lockout: crate::gates::lockout::ModelLockoutStore` to `Gateway`; init `ModelLockoutStore::new()` in `new` (recorder registration is Task 5).
  - `engine/execute.rs:54` + `stream.rs:77`: `ModelSelectionService::new(&config, &self.circuit_breaker, &self.cooldown, &self.model_lockout)`.
- [ ] **Step 4: Verify** — gate test passes; full suite **191 green** (nothing writes locks yet → the gate never skips in existing tests → behavior unchanged, exactly like (c) Task 2). clippy/fmt clean.
- [ ] **Step 5: Commit:** `feat(gateway): ModelLockoutGate wired into selection (endpoint lockout read-side)`.

---

### Task 5: `ModelLockoutSink` + policy + wire into recorders (write side) + integration test

**Files:** `gates/lockout.rs`, `engine/mod.rs`, `engine/tests.rs`.

- [ ] **Step 1: Failing sink unit tests** (in `gates/lockout.rs`) covering the policy matrix:
  - a 429 with `retry_after_ms: Some(2000)` → locks with `until ≈ now + 2s` (honored exactly, **not** clamped even if below/above defaults);
  - a 429 with no retry-after → locks timed with the base cooldown; a **second** failure after that lock has expired → a **strictly longer** window (escalation++), clamped to `max_cooldown`;
  - a 403 quota body → timed lock (`quota_default`);
  - a 401 / 403-credits → terminal lock (`until: None`);
  - a **success** → `clear` (lock + escalation gone);
  - a non-limit error (500 `ProviderError`) and a `Timeout` → **no** lock;
  - "403 after 429 upgrades the lock": pre-seed `RateLimit` timed lock, feed a 403-quota → reason becomes `QuotaExhausted`.
  Drive the sink directly: `sink.on_outcome(&AttemptOutcome { endpoint: "r:m", router: "r", success: false, error: Some(&err) })` then assert via `store.locked("r:m")`.
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement** in `gates/lockout.rs`. **First, restore the escalation plumbing deferred from Task 3** (now that the sink reads it): add `pub escalation: u32` back to `LockEntry`; change `ModelLockoutStore::set` to `set(&self, endpoint, reason, until, escalation: u32)` (construct `LockEntry { reason, until, escalation }`); re-add `pub(crate) fn get(&self, endpoint) -> Option<LockEntry>` (`.get(endpoint).copied()`). Update the Task-3 `store_records_and_reads_locks` test's two `set(...)` calls to pass a 4th arg `0`. `apply_lockout` (Task 6) will call `set(.., 0)`. Then add the policy + sink:
```rust
use super::{AttemptOutcome, HealthRecorder};
use std::time::Duration;

/// Per-reason lockout durations. Operator-configurable via `ResilienceConfig`
/// in plan (f); defaults here. (Quota uses a fixed default; the calendar-clock
/// reset-boundary refinement is plan (e).)
#[derive(Debug, Clone)]
pub struct ModelLockoutPolicy {
    pub rate_limit_base: Duration,
    pub quota_default: Duration,
    pub max_cooldown: Duration,
}

impl Default for ModelLockoutPolicy {
    fn default() -> Self {
        Self {
            rate_limit_base: Duration::from_secs(60),
            quota_default: Duration::from_secs(3600),
            max_cooldown: Duration::from_secs(6 * 3600),
        }
    }
}

/// Best-effort, isolated observer of gateway health decisions. The gateway
/// **announces** a lockout; the caller **persists** it (the tenant-agnostic core
/// never persists — design §5c). Defined here because the sink fires it; the
/// `Gateway::with_observer` wiring lands in Task 6. More methods
/// (`on_candidate_skipped`, `on_attempt`) arrive with later observability work.
pub trait SelectionObserver: Send + Sync {
    /// `until = None` ⇒ terminal (surface a human-action hint — top-up / rotate
    /// key — not a wake-up time).
    fn on_lockout(&self, endpoint: &str, reason: LockReason, until: Option<Instant>);
}

/// Arc-backed, Clone registry shared between `Gateway` (which registers via
/// `with_observer`, Task 6) and the sink (which fires). Same shared-handle
/// pattern as `ModelLockoutStore`. Empty until an observer is registered, so
/// `fire` is a no-op in (d)'s own tests unless one is added.
#[derive(Clone, Default)]
pub struct LockoutBroadcaster {
    observers: Arc<Mutex<Vec<Arc<dyn SelectionObserver>>>>,
}

impl LockoutBroadcaster {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register(&self, obs: Arc<dyn SelectionObserver>) {
        self.observers.lock().unwrap_or_else(|e| e.into_inner()).push(obs);
    }
    /// Fire best-effort. Clone the handles out before calling so an observer
    /// can't deadlock by re-entering the registry.
    pub fn fire(&self, endpoint: &str, reason: LockReason, until: Option<Instant>) {
        let observers = self.observers.lock().unwrap_or_else(|e| e.into_inner()).clone();
        for o in observers {
            o.on_lockout(endpoint, reason, until);
        }
    }
}

/// Write side: classify a failed outcome and lock the endpoint accordingly; a
/// success clears it. Terminal reasons (`Auth`/`Credits`) lock with `until: None`.
/// Timed reasons escalate **once per lock→release→relock cycle** (not per
/// concurrent failure): escalation grows only when the prior lock had already
/// expired. A real `Retry-After` is honored exactly (never clamped); synthetic
/// backoff is clamped to `max_cooldown`.
pub struct ModelLockoutSink {
    store: ModelLockoutStore,
    policy: ModelLockoutPolicy,
    observers: LockoutBroadcaster,
}

impl ModelLockoutSink {
    pub fn new(store: ModelLockoutStore, policy: ModelLockoutPolicy, observers: LockoutBroadcaster) -> Self {
        Self { store, policy, observers }
    }
}

impl HealthRecorder for ModelLockoutSink {
    fn on_outcome(&self, o: &AttemptOutcome<'_>) {
        if o.success {
            self.store.clear(o.endpoint); // success clears lock + escalation
            return;
        }
        let Some(err) = o.error else { return };
        let Some(reason) = classify(err) else { return };

        let now = std::time::Instant::now();
        let prior = self.store.get(o.endpoint);
        // Generation guard: escalate only on a genuine relock (prior lock expired),
        // not on a concurrent failure during an active lock.
        let escalation = match prior {
            Some(p) if matches!(p.until, Some(u) if u <= now) => p.escalation.saturating_add(1),
            Some(p) => p.escalation, // still-active or terminal prior → keep
            None => 0,
        };

        let until = self.deadline(reason, retry_after(err), escalation, now);
        self.store.set(o.endpoint, reason, until, escalation);
        self.observers.fire(o.endpoint, reason, until); // no-op until Task 6 registers observers
    }
}

impl ModelLockoutSink {
    /// Compute the lock deadline. Terminal → `None`. A real retry-after is exact
    /// (never clamped). Otherwise synthetic backoff = base * 2^escalation, clamped.
    fn deadline(
        &self,
        reason: LockReason,
        retry_after: Option<Duration>,
        escalation: u32,
        now: std::time::Instant,
    ) -> Option<std::time::Instant> {
        if reason.is_terminal() {
            return None;
        }
        if let Some(exact) = retry_after {
            return Some(now + exact); // honored verbatim, never clamped (Until::Exact)
        }
        let base = match reason {
            LockReason::RateLimit => self.policy.rate_limit_base,
            LockReason::QuotaExhausted => self.policy.quota_default,
            _ => unreachable!("terminal handled above"),
        };
        let factor = 1u32.checked_shl(escalation.min(16)).unwrap_or(u32::MAX);
        let backoff = base.saturating_mul(factor).min(self.policy.max_cooldown);
        Some(now + backoff)
    }
}

/// A real `Retry-After` (429 only, here) → exact duration; otherwise `None`.
fn retry_after(err: &GatewayError) -> Option<Duration> {
    match err {
        GatewayError::RateLimit { retry_after_ms: Some(ms), .. } => Some(Duration::from_millis(*ms)),
        _ => None,
    }
}
```
  - **`engine/mod.rs`:** build the store + broadcaster in `new` (before `recorders`, mirroring `cooldown`), register the sink, keep both handles on `Gateway`:
```rust
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let model_lockout = crate::gates::lockout::ModelLockoutStore::new();
        let lockout_observers = crate::gates::lockout::LockoutBroadcaster::new();
        let recorders: Vec<Arc<dyn crate::gates::HealthRecorder>> = vec![
            Arc::new(crate::gates::circuit_breaker_gate::CircuitBreakerSink::new(circuit_breaker.clone())),
            Arc::new(crate::gates::cooldown::ConnectionCooldownSink::new(
                cooldown.clone(),
                crate::gates::cooldown::DEFAULT_CONNECTION_COOLDOWN,
            )),
            Arc::new(crate::gates::lockout::ModelLockoutSink::new(
                model_lockout.clone(),
                Default::default(),
                lockout_observers.clone(),
            )),
        ];
```
    and add `model_lockout,` + `lockout_observers,` to the struct literal and to the `Gateway` struct definition (the `model_lockout` field was added in Task 4; add `lockout_observers: crate::gates::lockout::LockoutBroadcaster` now). `SelectionObserver` + `LockoutBroadcaster` are defined in this task's `gates/lockout.rs` block above (the sink needs `.fire()` to compile); Task 6 only adds the `Gateway::with_observer` wiring that calls `register`. The registry is empty here, so `fire` is a no-op unless a test registers an observer.
- [ ] **Step 4: Integration test** (in `engine/tests.rs`) — the real end-to-end proof, modeled on (c)'s `timeout_cools_router_and_next_selection_skips_it`:
  - Register a `failing` adapter that returns `GatewayError::ProviderError { status: Some(403), message: "quota exceeded", .. }` on router `A`, plus a working `noop` on router `B`, in a chain `[A-model (priority 1), B-model (priority 2)]` whose `fallback_triggers` include `ProviderError` (so the *current* request falls over — the in-flight 403-quota-specific fallover is plan (e); here the chain's ProviderError trigger drives it).
  - First `execute()` → `A` returns the 403 → chain falls over → served by `B`; the **sink locks `A:A-model`**. Assert `response.model` is the B model, and `gw` locked A: expose a test accessor or assert via a second call.
  - Second `execute()` → assert `response.model` is B **and** the trace shows `A-model` skipped with `SkipReason::LockedOut { reason: QuotaExhausted, .. }` (A is skipped at selection now, not attempted). This is the non-vacuous proof: it would fail if the sink didn't fire or the store weren't shared.
  - Add a direct assertion that a **success** clears a prior lock: `gw.apply_lockout` isn't available until Task 6, so here drive it via the sink path or, if simpler, defer the success-clears end-to-end assertion to Task 6 (where `apply_lockout`/`clear_lockout` exist) and keep the success-clears **unit** test (Step 1) as the proof for this task.
  - Confirm a **non-limit** failure does not lock: a chain where `A` returns `ProviderError { status: Some(500) }` → after fallover, `A` is **not** locked (second call still attempts A first). 
- [ ] **Step 5: Verify** — new lockout behavior proven end-to-end; full suite green (191 existing + new); clippy/fmt clean. Explicitly confirm: terminal reasons store `until: None`; success clears; 500/Timeout do not lock.
- [ ] **Step 6: Commit:** `feat(gateway): ModelLockoutSink — lock a model on a classified provider limit, skip it next selection`.

---

### Task 6: `on_lockout` callback + `apply_lockout`/`clear_lockout` + terminal-lock lifecycle (tenant-agnostic seam)

**Files:** `gates/lockout.rs` (`SelectionObserver`, `LockoutBroadcaster::register`), `engine/mod.rs` (`with_observer`, `apply_lockout`, `clear_lockout`, `refresh_router_keys`), `engine/tests.rs`.

- [ ] **Step 1: Failing tests**
  - **Callback (in `engine/tests.rs`):** a test `SelectionObserver` that pushes `(endpoint, reason, until.is_some())` into an `Arc<Mutex<Vec<_>>>`. Build a gateway `.with_observer(obs.clone())`, drive a 403-quota failover (as Task 5), assert the observer recorded `("A:A-model", QuotaExhausted, true)`. The gateway **fired**; the test (playing the caller) is what "persists".
  - **Re-seed (in `engine/tests.rs`):** on a fresh gateway, `gw.apply_lockout("A:A-model", LockReason::QuotaExhausted, Some(Instant::now() + 3600s))`; a request routed to that chain skips `A-model` (`LockedOut`) and is served by `B`. `gw.clear_lockout("A:A-model")` → `A-model` is eligible again.
  - **Terminal-lock lifecycle (in `engine/tests.rs`):** `gw.apply_lockout("A:A-model", LockReason::Auth, None)` (terminal) → A skipped; then `gw.refresh_router_keys(|_| Some("new-key".into())).await` → the terminal Auth lock on A is cleared and A is eligible again (design §5b scenario "fixing the credential clears a terminal auth lock").
- [ ] **Step 2:** run → FAIL (methods missing).
- [ ] **Step 3: Implement**
  - `SelectionObserver` + `LockoutBroadcaster` already exist (defined in Task 5's `gates/lockout.rs`, including `register`). No change to them here — Task 6 only wires them into `Gateway`.
  - `gates/lockout.rs` — add a router-scoped terminal clear on `ModelLockoutStore` for the refresh lifecycle:
```rust
    /// Clear terminal (`Auth`/`Credits`) locks on all of `router`'s endpoints
    /// (keys `"{router}:*"`). Used after `refresh_router_keys` — a new credential
    /// may fix an auth/credits lock. Timed locks (rate/quota) are left intact.
    pub fn clear_terminal_for_router(&self, router: &str) {
        let prefix = format!("{router}:");
        self.locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|k, e| !(k.starts_with(&prefix) && e.reason.is_terminal()));
    }
```
    (Router ids are config keys without `:`, so `"{router}:"` is an unambiguous prefix even though the model segment may contain `:`.)
  - `engine/mod.rs` — **first, retain the observer registry (deferred from Task 5, which gave the sink a local broadcaster to avoid a dead field):** add `lockout_observers: crate::gates::lockout::LockoutBroadcaster,` to the `Gateway` struct; in `new`, bind `let lockout_observers = crate::gates::lockout::LockoutBroadcaster::new();` and pass `lockout_observers.clone()` into `ModelLockoutSink::new(...)` (replacing Task 5's inline `LockoutBroadcaster::new()`), and add `lockout_observers,` to the struct literal. Now `with_observer` registers into the SAME Arc-backed broadcaster the sink fires. Then the methods:
```rust
    /// Register a best-effort lockout observer (the caller persists what the
    /// gateway announces). Builder-style; the core never persists (§5c).
    pub fn with_observer(self, observer: Arc<dyn crate::gates::lockout::SelectionObserver>) -> Self {
        self.lockout_observers.register(observer);
        self
    }

    /// Re-seed a persisted lockout on this instance (caller → gateway, §5c).
    /// Tenant scoping is the caller's — this touches only this instance's state.
    pub fn apply_lockout(
        &self,
        endpoint: &str,
        reason: crate::gates::lockout::LockReason,
        until: Option<Instant>,
    ) {
        self.model_lockout.set(endpoint, reason, until, 0);
    }

    /// Clear a lockout (caller-driven suspend release / manual override).
    pub fn clear_lockout(&self, endpoint: &str) {
        self.model_lockout.clear(endpoint);
    }
```
    and extend `refresh_router_keys` (after the key-resolve loop) to clear terminal locks for every router whose key was refreshed:
```rust
        for id in config.routers.keys() {
            self.model_lockout.clear_terminal_for_router(id);
        }
```
    (`self.model_lockout` is `Clone`/Arc-backed, so this is fine under the `config` write guard.)
- [ ] **Step 4: Verify** — callback fires with the right `(endpoint, reason, until)`; `apply_lockout` re-seed skips the model; `clear_lockout` restores it; `refresh_router_keys` clears a terminal Auth lock (timed locks survive — add that assertion). Full suite green; clippy/fmt clean. Confirm the gateway itself performs **no** persistence (only the callback + in-mem store).
- [ ] **Step 5: Commit:** `feat(gateway): on_lockout callback + apply_lockout/clear_lockout + terminal-lock lifecycle (tenant-agnostic)`.

---

## Self-Review

- **Spec coverage** (`docs/design/selection-policy-pipeline.md` §3.1/§3.2/§5b/§5c, migration step 4; feature docs `routing/model-lockout.md`, `routing/quota-demote-to-tier.md`):
  - 429/403-quota/credits/401 classification at the adapter boundary → Task 1 (boundary) + Task 2 (`classify`). ✔
  - Per-reason durations, exact-retry-after-honored / synthetic-backoff-clamped, escalation once-per-cycle, success-resets → Task 5 (`deadline` + generation guard + `clear`). ✔ Gherkin: "rate-limited locked briefly", "quota until reset (default here)", "escalation grows & is clamped", "exact upstream reset honored", "credits terminal", "403 after 429 upgrades", "401 terminal", "success clears". ✔
  - Tenant-agnostic `on_lockout` (gateway announces, caller persists) + `apply_lockout`/`clear_lockout` re-seed + `refresh_router_keys` clears terminal → Task 6. Gherkin: "gateway announces; caller persists", "caller re-seeds on fresh instance", "fixing credential clears terminal auth lock", "core is tenant-agnostic". ✔
  - Demote-to-tier: **next-request** demote is emergent from the gate skipping a locked model (Tasks 4–5). The **same-request** in-flight demote (§3.1) and the `AllGated{resume_after}` terminal error (§3.3) are **explicitly deferred to plan (e)** — stated in the header and re-stated here so the gap is not silent.
- **Behavior preservation:** existing gateway tests write no locks, and the gate only affects a locked endpoint on a *subsequent* selection, so the 191 stay green through Tasks 2–4 (Task 4 wires the gate against an always-empty store, exactly like (c) Task 2). Task 1's one behavior change (403 → `ProviderError` not `Authentication`) is pinned by updated `base.rs` tests + a workspace-wide sweep (Step 4/5).
- **Type consistency:** `LockReason` (Task 2) is consumed by `SkipReason::LockedOut` (Task 3), `LockView`/`ModelLockoutRead` (Task 3) read by `ModelLockoutGate` (Task 4) via `SelectionCtx.model_lockout` (Task 4), written by `ModelLockoutSink` (Task 5) via `ModelLockoutStore::set`/`clear`/`get`, announced through `LockoutBroadcaster`/`SelectionObserver` (Tasks 5→6). `ModelSelectionService::new` grows one 4th positional arg (Task 4), updated at both engine call sites + all in-file tests. `classify` (Task 2) is the single reason source for the sink (and, later, the in-flight walk in (e)).
- **Sequencing (each green + committed, no broken intermediate):** 1 classifiable errors (cloud-providers) → 2 pure `classify` (unused, fine) → 3 skip-reason+store (unused, fine) → 4 gate wired (empty store admits → behavior-preserving) → 5 sink writes (behavior-additive, integration-proven) → 6 callback/control (additive). Store + broadcaster are shared Arc-backed handles built once in `Gateway::new` and cloned into the sink (same discipline that made (c)'s shared-store correct).
- **Placeholder scan:** none — every code step shows the code; test bodies are given or precisely specified against an existing analog ((c)'s cooldown tests). The one intentional forward-reference (`LockoutBroadcaster` needed by Task 5's `engine/mod.rs`) is resolved by landing a minimal `LockoutBroadcaster` in Task 5 and enriching it in Task 6 (called out in Task 5 Step 3).
- **Deferred (not this plan), re-stated:** in-flight §3.1 walk rewire + `resume_after`/`AllGated` + `on_outcome -> Option<Instant>` → **(e)**; injected calendar clock (exact quota reset boundary) + seedable jitter RNG + `ResilienceConfig`/builder consolidation + `EndpointKey` + true LRU eviction cap → **(e)/(f)**. Escalation memory is retained past expiry (not pruned) in (d); bounded eviction lands with `ResilienceConfig`.

## Execution Handoff

Subagent-driven in an isolated worktree off `develop`; per-task spec + code-quality review (the behavior-adding Tasks 1, 5, 6 get the full treatment); final whole-branch review; `finishing-a-development-branch` → merge to `develop`. Then plan **(e)** — engine post-walk `resume_after` + `GatewayError::AllGated` + in-flight recoverable-classification (§3.1) — builds directly on this classifier + lockout store.
