# SP-0 (f) — `ResilienceConfig` + builder + bounded eviction + jitter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Complete SP-0 (health gates) by making the three gates **operator-tunable, bounded, and spread**. Replace the hardcoded constants (`DEFAULT_CONNECTION_COOLDOWN = 30s`, `ModelLockoutPolicy::default` = 60s/1h/6h) with a single `ResilienceConfig` applied via a `Gateway::with_resilience(cfg)` builder; add **bounded eviction** so the in-memory health stores can't leak; and add **deterministic per-endpoint jitter** to spread retry storms. All default to today's exact behavior.

**Architecture:** `ResilienceConfig` is a construction-time input (NOT a `GatewayConfig` field — `GatewayConfig` is hot-swapped via `update_config`, and the sinks are built once at `Gateway::new`, so a config-field would silently ignore hot-swaps). `Gateway::with_resilience(mut self, cfg)` rebuilds the recorder set from a shared `build_recorders` helper, preserving the SAME Arc-backed stores/observers/breaker (so the gates keep reading what the sinks write). The cooldown/lockout **sinks** own the tunables (base durations, eviction cap, jitter fraction); the stores stay dumb maps read by the gates. Jitter is deterministic (a stable hash of the endpoint key) — same endpoint → same offset (no flakiness), different endpoints → spread (thundering-herd mitigation); a real upstream `Retry-After` is never jittered.

**Tech Stack:** Rust, `crates/gateway`. Contract per commit: existing **228 gateway lib tests** stay green (defaults preserve behavior); `cargo test --workspace` green; `make check` clean. New behavior proven by new tests.

**Builds on:** (c)/(d)/(e) — `ConnectionCooldownSink`, `ModelLockoutSink`/`ModelLockoutPolicy`, `ModelLockoutStore`/`ConnectionCooldownStore`, and the `build`-once-at-`new` recorder pipeline in `Gateway::new`.

**Deferred (NOT this plan) — stated so the boundaries are honest:**
- **`.with_gate` / `.with_recorder` custom composition** — YAGNI: there is no external consumer that adds a custom gate/recorder yet, and the internal gate set is fixed and correct. Only `.with_resilience` (tunes the existing pipeline) is built. Add the open-composition hooks when a real consumer needs them.
- **Injected calendar clock for an EXACT quota reset boundary** — the quota lockout stays a fixed `quota_default` (~1h). Rationale: the approximation is **self-correcting** — a consumer that retries at `resume_after` and is still quota'd simply re-locks for another `quota_default`; it never over-serves. A true "reset at 00:00 UTC" needs a wall-clock `Clock` seam with no current consumer, so it waits for SP-DATA / a consumer that needs precise boundary timing.
- **Seedable jitter RNG** — replaced by deterministic hash jitter (better: spreads across endpoints AND is flake-free, so no seed/injection plumbing is needed).
- **Opaque `EndpointKey`** — the `"router:model"` string key works throughout; the newtype refactor is future-proofing with no current payoff.

---

## File Structure

- **Create `crates/gateway/src/resilience.rs`** — `ResilienceConfig` (+ `Default` = today's constants) [Task 1]; `pub(crate) fn deterministic_jitter(key, base, fraction) -> Duration` [Task 4]. Add `pub mod resilience;` to `lib.rs`.
- **Modify `crates/gateway/src/engine/mod.rs`** — a `build_recorders(breaker, cooldown, lockout, observers, &ResilienceConfig)` helper used by `new` (with `ResilienceConfig::default()`) and a new `Gateway::with_resilience(mut self, ResilienceConfig) -> Self`. [Task 2]
- **Modify `crates/gateway/src/gates/cooldown.rs`** — `ConnectionCooldownSink` gains `base` (from config, replacing the const) [T2], `eviction_cap` [T3], `jitter_fraction` [T4]; `ConnectionCooldownStore::evict_expired_over_cap` [T3]. Keep `DEFAULT_CONNECTION_COOLDOWN` as the value `ResilienceConfig::default()` uses (or move the literal into the default).
- **Modify `crates/gateway/src/gates/lockout.rs`** — `ModelLockoutSink` gains `eviction_cap` [T3] + `jitter_fraction` [T4]; `ModelLockoutStore::evict_expired_over_cap` [T3]; `ModelLockoutPolicy` reused by `ResilienceConfig`.
- **Modify `docs/features/routing/{model-lockout,README}.md`** — honesty annotations + SP-0-complete status. [Task 5]

---

### Task 1: `ResilienceConfig` struct + `Default` (today's constants)

**Files:** create `crates/gateway/src/resilience.rs`; `crates/gateway/src/lib.rs` (`pub mod resilience;`).

- [ ] **Step 1: Failing test** (in `resilience.rs`):
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[test]
    fn default_matches_todays_hardcoded_behavior() {
        let r = ResilienceConfig::default();
        assert_eq!(r.cooldown_base, Duration::from_secs(30)); // == old DEFAULT_CONNECTION_COOLDOWN
        assert_eq!(r.lockout.rate_limit_base, Duration::from_secs(60));
        assert_eq!(r.lockout.quota_default, Duration::from_secs(3600));
        assert_eq!(r.lockout.max_cooldown, Duration::from_secs(6 * 3600));
        assert_eq!(r.jitter_fraction, 0.0);      // off ⇒ behavior-preserving
        assert!(r.eviction_cap >= 1024);         // bounded but generous
    }
}
```
- [ ] **Step 2:** `cargo test -p sensei-gateway resilience` → FAIL.
- [ ] **Step 3: Implement** `resilience.rs`:
```rust
use crate::gates::lockout::ModelLockoutPolicy;
use std::time::Duration;

/// Max entries retained per in-memory health store before EXPIRED entries are
/// evicted. Generous — normal operation (fewer distinct endpoints) never trips it;
/// it only bounds leakage from many short-lived endpoints. Active/terminal gates
/// are never dropped.
pub const DEFAULT_EVICTION_CAP: usize = 4096;

/// Operator-tunable resilience policy applied at construction via
/// [`crate::engine::Gateway::with_resilience`]. `Default` reproduces the
/// pre-(f) hardcoded behavior exactly, so an absent config changes nothing.
#[derive(Debug, Clone)]
pub struct ResilienceConfig {
    /// Base router cooldown after a transport fault (`Network`/`Timeout`).
    pub cooldown_base: Duration,
    /// Per-reason model-lockout durations + escalation clamp.
    pub lockout: ModelLockoutPolicy,
    /// Per-store retention cap; over it, expired entries are evicted (Task 3).
    pub eviction_cap: usize,
    /// Deterministic jitter fraction in `[0.0, 1.0)` added to SYNTHETIC timed
    /// deadlines to spread retries across endpoints (Task 4). `0.0` ⇒ off
    /// (today's behavior). A real upstream `Retry-After` is never jittered.
    pub jitter_fraction: f64,
}

impl Default for ResilienceConfig {
    fn default() -> Self {
        Self {
            cooldown_base: Duration::from_secs(30),
            lockout: ModelLockoutPolicy::default(),
            eviction_cap: DEFAULT_EVICTION_CAP,
            jitter_fraction: 0.0,
        }
    }
}
```
  Add `pub mod resilience;` to `lib.rs` (near the other `pub mod`s; keep fmt-clean ordering).
- [ ] **Step 4: Verify** — `cargo test -p sensei-gateway resilience` PASS; `cargo test -p sensei-gateway --lib` still **228**; `cargo test --workspace` green; clippy `-D warnings` + fmt clean. (Nothing consumes `ResilienceConfig` yet; `pub` struct/fields aren't dead-code-linted.)
- [ ] **Step 5: Commit:** `feat(gateway): ResilienceConfig — operator-tunable health-gate policy (default = today)`.

---

### Task 2: `Gateway::with_resilience` + `build_recorders` (durations become configurable)

**Files:** `engine/mod.rs`, `gates/cooldown.rs`.

- [ ] **Step 1: Failing test** (in `engine/tests.rs`): a gateway built `.with_resilience(ResilienceConfig { cooldown_base: Duration::from_millis(50), ..Default::default() })` cools a router for only ~50ms — assert a `Timeout`-driven cooldown expires quickly (drive a timeout outcome, confirm `gw.cooldown.cooling_until(router)` ≈ now+50ms, well under the 30s default). Also assert a gateway with NO `.with_resilience` still uses 30s (the default path is unchanged).
- [ ] **Step 2:** run → FAIL (no `with_resilience`).
- [ ] **Step 3: Implement.**
  - `gates/cooldown.rs`: `ConnectionCooldownSink::new` takes the base as a param already (`cooldown: Duration`); keep that. (No signature change this task — the value now flows from config.) Keep `DEFAULT_CONNECTION_COOLDOWN` but it's now only referenced by `ResilienceConfig::default`'s `cooldown_base` literal (Task 1 inlined `30`); you may leave the const for back-compat or delete it if unused — if deleting, remove its now-dead reference. Simplest: keep `ConnectionCooldownSink::new(store, base)` and let `build_recorders` pass `resilience.cooldown_base`.
  - `engine/mod.rs`: extract a free helper (near `dispatch_outcome`):
```rust
fn build_recorders(
    breaker: &CircuitBreakerManager,
    cooldown: &crate::gates::cooldown::ConnectionCooldownStore,
    model_lockout: &crate::gates::lockout::ModelLockoutStore,
    observers: &crate::gates::lockout::LockoutBroadcaster,
    resilience: &crate::resilience::ResilienceConfig,
) -> Vec<Arc<dyn crate::gates::HealthRecorder>> {
    vec![
        Arc::new(crate::gates::circuit_breaker_gate::CircuitBreakerSink::new(breaker.clone())),
        Arc::new(crate::gates::cooldown::ConnectionCooldownSink::new(
            cooldown.clone(),
            resilience.cooldown_base,
        )),
        Arc::new(crate::gates::lockout::ModelLockoutSink::new(
            model_lockout.clone(),
            resilience.lockout.clone(),
            observers.clone(),
        )),
    ]
}
```
  - `Gateway::new`: replace the inline `recorders` vec with `let recorders = build_recorders(&circuit_breaker, &cooldown, &model_lockout, &lockout_observers, &crate::resilience::ResilienceConfig::default());`.
  - Add the builder (near `with_store`/`with_observer`):
```rust
    /// Tune the health gates (cooldown/lockout durations, eviction cap, jitter).
    /// Builder-style; rebuilds the recorder pipeline from `resilience` while
    /// preserving the SAME Arc-backed stores/observers/breaker, so the read-side
    /// gates keep reading what the sinks write. Absent ⇒ [`ResilienceConfig::default`]
    /// (today's behavior). Construction-time only — not hot-swappable via
    /// `update_config` (which carries routing config, not resilience policy).
    pub fn with_resilience(mut self, resilience: crate::resilience::ResilienceConfig) -> Self {
        self.recorders = build_recorders(
            &self.circuit_breaker,
            &self.cooldown,
            &self.model_lockout,
            &self.lockout_observers,
            &resilience,
        );
        self
    }
```
- [ ] **Step 4: Verify** — the custom-cooldown test passes; default path unchanged; `cargo test -p sensei-gateway --lib` (228 + your new test) + `cargo test --workspace` green; clippy/fmt clean.
- [ ] **Step 5: Commit:** `feat(gateway): Gateway::with_resilience rebuilds the recorder pipeline from config`.

---

### Task 3: Bounded eviction (health stores can't leak)

**Files:** `gates/cooldown.rs`, `gates/lockout.rs`, `engine/mod.rs` (`build_recorders` threads the cap).

- [ ] **Step 1: Failing tests** (in `cooldown.rs` + `lockout.rs`):
  - `ConnectionCooldownStore`: insert `cap + 5` entries where several are already expired (`until` in the past) and a couple active (`until` future); call `evict_expired_over_cap(cap)`; assert the expired ones are gone, the active ones remain, and `len() <= cap` OR (if all active) all active preserved. Add a `len()` test accessor if needed.
  - `ModelLockoutStore`: same — expired **timed** entries evicted over cap; **terminal** (`until: None`) and **active timed** entries never evicted.
  - Sink-level: a `ConnectionCooldownSink`/`ModelLockoutSink` built with a tiny `eviction_cap` prunes expired entries on write (drive several writes; assert the map stays bounded and never drops an active gate).
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement.**
  - `ConnectionCooldownStore::evict_expired_over_cap(&self, cap: usize)`:
```rust
    /// When the map exceeds `cap`, drop entries whose cooldown has already
    /// elapsed (`until <= now`). Active cooldowns are never dropped, so the cap
    /// is soft (bounded by concurrently-active routers). Prevents unbounded
    /// growth from many short-lived routers.
    pub fn evict_expired_over_cap(&self, cap: usize) {
        let mut m = self.cooling.lock().unwrap_or_else(|e| e.into_inner());
        if m.len() <= cap { return; }
        let now = Instant::now();
        m.retain(|_, until| *until > now);
    }
```
  - `ModelLockoutStore::evict_expired_over_cap(&self, cap: usize)`: same shape, but keep terminal (`until: None`) and active (`Some(u) where u > now`); drop only expired timed (`Some(u) where u <= now`):
```rust
    pub fn evict_expired_over_cap(&self, cap: usize) {
        let mut m = self.locks.lock().unwrap_or_else(|e| e.into_inner());
        if m.len() <= cap { return; }
        let now = Instant::now();
        m.retain(|_, e| match e.until { Some(u) => u > now, None => true }); // keep active + terminal
    }
```
  - `ConnectionCooldownSink` + `ModelLockoutSink`: add an `eviction_cap: usize` field; `new(..., eviction_cap)`; after each `store.start(...)`/`store.set(...)` write, call `self.store.evict_expired_over_cap(self.eviction_cap)`. (Only on a WRITE — reads/successes don't grow the map. On success the lockout sink `clear`s, which shrinks.)
  - `build_recorders`: pass `resilience.eviction_cap` to both sink constructors.
- [ ] **Step 4: Verify** — eviction tests pass (expired dropped over cap; active/terminal never dropped); default `eviction_cap = 4096` means existing tests never trip eviction ⇒ `cargo test -p sensei-gateway --lib` + `cargo test --workspace` green; clippy/fmt clean. Confirm an active lock/cooldown is NEVER evicted (the load-bearing assertion).
- [ ] **Step 5: Commit:** `feat(gateway): bounded eviction of expired health-store entries (eviction_cap)`.

---

### Task 4: Deterministic per-endpoint jitter (spread retry storms)

**Files:** `resilience.rs` (`deterministic_jitter`), `gates/cooldown.rs`, `gates/lockout.rs`, `engine/mod.rs`.

- [ ] **Step 1: Failing tests**:
  - `resilience.rs` `deterministic_jitter`: `fraction == 0.0` ⇒ `Duration::ZERO`; `base.is_zero()` ⇒ `ZERO`; for `fraction = 0.5, base = 60s`: the SAME key returns the SAME offset across calls (deterministic — no flakiness), the offset is within `[0, 30s)`, and two DIFFERENT keys generally differ (assert at least two distinct sample keys produce different offsets).
  - `cooldown.rs` sink: with `jitter_fraction = 0.0` the cooldown `until` is exactly `now + base` (behavior-preserving); with `jitter_fraction > 0` the `until` is `now + base + jitter(router)` (within `[base, base*(1+fraction))`), and deterministic for a given router.
  - `lockout.rs` sink: jitter applies to the SYNTHETIC backoff only — a real `Retry-After` (429 with `retry_after_ms`) yields `now + exact` with NO jitter; a synthetic rate/quota backoff gets `+ jitter(endpoint)`.
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement.**
  - `resilience.rs`:
```rust
/// Deterministic per-key jitter in `[0, base * fraction)`. Uses `DefaultHasher`
/// (fixed keys ⇒ stable across runs, unlike `RandomState`), so the SAME key
/// always gets the SAME offset (flake-free tests) while DIFFERENT keys spread
/// out (thundering-herd mitigation). `fraction <= 0` or `base == 0` ⇒ zero.
pub(crate) fn deterministic_jitter(key: &str, base: Duration, fraction: f64) -> Duration {
    if fraction <= 0.0 || base.is_zero() {
        return Duration::ZERO;
    }
    let span_ms = (base.as_millis() as f64 * fraction.min(1.0)) as u64;
    if span_ms == 0 {
        return Duration::ZERO;
    }
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut h);
    Duration::from_millis(h.finish() % span_ms)
}
```
  - `ConnectionCooldownSink`: add `jitter_fraction: f64`; on a transport fault, `let until = Instant::now() + self.cooldown + crate::resilience::deterministic_jitter(o.router, self.cooldown, self.jitter_fraction);` (return that `until` from `on_outcome`, per (e)'s `Option<Instant>` contract).
  - `ModelLockoutSink`: add `jitter_fraction: f64`; in `deadline`, apply jitter ONLY on the synthetic-backoff branch (NOT the `retry_after` exact branch): after computing `backoff`, `let jittered = backoff + crate::resilience::deterministic_jitter(endpoint, backoff, self.jitter_fraction);` then `Some(now + jittered)`. (You'll need the `endpoint` key in `deadline` — thread `o.endpoint` in, or compute jitter in `on_outcome` around the `deadline` result for the synthetic case. Keep the exact-`Retry-After` path jitter-free.)
  - `build_recorders`: pass `resilience.jitter_fraction` to both sink constructors.
- [ ] **Step 4: Verify** — jitter tests pass; `jitter_fraction = 0.0` (the default) leaves every existing timing assertion unchanged ⇒ `cargo test -p sensei-gateway --lib` + `cargo test --workspace` green; clippy/fmt clean. Confirm an exact `Retry-After` is NEVER jittered.
- [ ] **Step 5: Commit:** `feat(gateway): deterministic per-endpoint jitter on synthetic backoff (spreads retry storms)`.

---

### Task 5: Honesty follow-ups + SP-0-complete docs

**Files:** `engine/exhaustion.rs`, `docs/features/routing/model-lockout.md`, `docs/features/routing/README.md`.

- [ ] **Step 1: Defensive comment** — in `engine/exhaustion.rs::contribution_for`, annotate the `Some(r) if r.is_recoverable() => match written_until { None => HardFailure }` arm: a recoverable classification always produces a timed lock (the sink writes `Some(deadline)`), so `written_until == None` here is a defensive fallback, not a live path. One-line comment; no logic change.
- [ ] **Step 2: Doc honesty (`model-lockout.md`)** — the "quota-exhausted model is locked until its reset window / `locked_until` is the next reset boundary, not a fixed 60s" scenario describes TARGET behavior; the implementation locks for a fixed `quota_default` (~1h). Annotate the scenario/prose to say the exact calendar reset boundary is **deferred** (the ~1h default is a self-correcting approximation), so the doc doesn't imply calendar-precise timing is implemented. Keep the deferral pointer.
- [ ] **Step 3: Doc — ordering note** — in `quota-demote-to-tier.md` (or `model-lockout.md` Notes), add a one-line note: subscription `QuotaExceeded` (subject hard-stop, `check_quota`) is raised before selection and is distinct from provider limits; if a subject is over-quota AND its providers are also all-gated, the all-gated check currently surfaces first (both mean "cannot serve now") — a deliberate precedence call, revisit if the subject hard-stop should always win.
- [ ] **Step 4: SP-0-complete status (`README.md`)** — in `docs/features/routing/README.md`, mark the health-gate features (circuit breaker, connection cooldown, model lockout, quota demote-to-tier) and the resilience config as **implemented**, completing SP-0. Match the existing status-table convention in that README (check a sibling implemented row). Do NOT claim the (f)-deferred items (calendar clock, EndpointKey, open `.with_gate`/`.with_recorder`) as done.
- [ ] **Step 5: Verify** — `cargo test -p sensei-gateway --lib` + `cargo test --workspace` green (Step 1 is a comment only); `make check` clean; the docs render (frontmatter intact, `doctype: feature` preserved).
- [ ] **Step 6: Commit:** `docs(gateway): SP-0 complete — resilience config status + honesty annotations`.

---

## Self-Review

- **Spec coverage** (design §4 config + §5 wiring): `ResilienceConfig` (per-reason durations, cooldown base, eviction cap, jitter) → Tasks 1–4; applied via a builder that preserves the shared stores → Task 2. `.with_gate`/`.with_recorder` open-composition, injected calendar clock, seedable-RNG, and `EndpointKey` are **explicitly deferred** with rationale (header) — the design's builder endgame is satisfied for the tunables that exist; the open hooks wait for a consumer.
- **Behavior preservation:** every default reproduces today's constants (`cooldown_base=30s`, lockout 60s/1h/6h, `jitter_fraction=0.0` ⇒ no jitter, `eviction_cap=4096` ⇒ never trips in existing tests). The 228 tests stay green through all tasks; new behavior (custom cooldown, eviction, jitter) has its own tests. The load-bearing safety invariant — **eviction never drops an active or terminal gate** — is pinned in Task 3.
- **Silent-failure avoidance:** `ResilienceConfig` is a construction-time builder input, NOT a `GatewayConfig` field, precisely so a hot-swap via `update_config` can't silently fail to apply it. Documented on `with_resilience`.
- **Type consistency:** `ResilienceConfig` (Task 1) reuses `ModelLockoutPolicy`; `build_recorders` (Task 2) is the ONE sink-construction site fed by `new` (default) and `with_resilience`; `eviction_cap` (Task 3) and `jitter_fraction` (Task 4) each add one sink field WITH its consumer (no dead-code window). `deterministic_jitter` is introduced in Task 4 where it's first used (a `pub(crate)` fn would trip dead-code if defined earlier).
- **Jitter correctness:** `DefaultHasher::new()` uses fixed keys ⇒ deterministic across runs (unlike `RandomState`), so tests aren't flaky; jitter applies only to SYNTHETIC backoff, never a real `Retry-After` (consistent with (e)'s exact-honored rule).
- **Sequencing (each green + committed):** 1 config vocab (unused) → 2 builder + configurable durations (behavior-preserving default) → 3 eviction (param + use together) → 4 jitter (param + use together) → 5 docs + comment. No broken intermediate; sink signature grows one param per task at the single `build_recorders` call site + sink tests.
- **Placeholder scan:** none — new types/fns/signatures shown in full; test bodies specified against existing sink-test analogues.

## Execution Handoff

Subagent-driven in an isolated worktree off `develop`; per-task spec + code-quality review (Tasks 3 and 4 — eviction and jitter — get the full treatment as they change timing/retention); final whole-branch review; `finishing-a-development-branch` → merge to `develop`. **This completes SP-0 (health gates).** Next begins the phased program: **SP-CAT** (free-tier catalog + tiers) → reference chains → **SP-1** orchestrator → **SP-DATA** — per `docs/superpowers/specs/2026-08-06-sensei-orchestrator-design.md` and the roadmap in `[[sensei-orchestrator-design]]` memory.
