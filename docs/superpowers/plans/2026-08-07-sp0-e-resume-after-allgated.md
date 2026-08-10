# SP-0 (e) — `resume_after` / `AllGated` + in-flight classification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Turn the health gates (breaker / cooldown / lockout) into **durability**. Two coupled changes:
1. **In-flight classification (design §3.1):** wire `classify()` into the candidate walk so a *recoverable* provider limit (429 / 403-quota) falls over to the next candidate **on the same request**, and a *terminal* one (401 / credits) stops — the single classifier now drives both the in-walk fallover AND the next-request lockout, so they can't disagree.
2. **`resume_after` / `AllGated` (design §3.3):** when **every** candidate is gated (skipped by a health gate at selection, or attempted-and-classified-as-a-limit), return a new terminal `GatewayError::AllGated { resume_after, skipped, human_action }` carrying a **wall-clock** earliest-eligibility time — the signal a durable consumer (the orchestrator) needs to pause-and-resume instead of treating quota as fatal. All-terminal (auth/credits/over-budget) → `resume_after = None` + a `human_action` hint → fail-fast, never pause forever.

**Architecture:** Both `execute` and `execute_stream` already walk `select_all`'s admitted candidates and keep the gated ones in `result.skipped` (each a `SkippedCandidate { reason: SkipReason }`). This plan: (a) adds a `SkipReason::gate_status()` classifier (`Timed(Instant)` / `Terminal(HumanAction)` / `Structural`); (b) makes `HealthRecorder::on_outcome` **return the `Instant` it wrote** so the walk can attribute a just-locked/cooled/tripped endpoint; (c) classifies each attempt's error in-walk for the fallover decision + a per-attempt "gate contribution"; (d) at exhaustion, aggregates selection-skips + attempt-contributions into `AllGated` (or preserves `NoCandidates`/`AllAttemptsFailed` when a candidate hard-failed or the chain is merely misconfigured). Wall-clock conversion anchors monotonic `Instant`s to `Utc::now()`.

**Invariant that keeps it sound:** `AllGated` ⟺ **every** candidate was gated (health-skip or classified-limit) and **none** hard-failed. A single unclassified failure (500 / network) ⇒ `AllAttemptsFailed` (unchanged). An all-structural selection (ModelNotFound / wrong capability) ⇒ `NoCandidates` (unchanged). This is why §3.1 and §3.3 ship together: without §3.1, a recoverable error would `Stop` the walk early and "exhaustion" wouldn't mean "all gated".

**Tech Stack:** Rust, `crates/gateway` + `crates/kernel`. Contract per commit: existing gateway lib tests stay green except the deliberate §3.1 fallover-behavior updates in Task 3 (called out inline); `cargo test --workspace` green; `make check` clean (fmt + clippy `-D warnings`). New behavior proven by new tests + the feature-doc Gherkins in Task 6.

**Builds on:** (d)'s `classify(&GatewayError) -> Option<LockReason>` + `LockReason::{is_recoverable,is_terminal}` + the `ModelLockoutSink`/`ConnectionCooldownSink`/`CircuitBreakerSink` (whose returns this plan starts consuming).

**Deferred (NOT this plan):** injected calendar clock for exact quota reset boundaries + seedable jitter (still (f)); `ResilienceConfig`/builder (f); opaque `EndpointKey` + bounded-LRU eviction (f); structured (non-string) `skipped` detail on `AllGated` (Vec<String> diagnostics here — enough for the orchestrator, which acts on `resume_after`/`human_action`).

---

## File Structure

- **Modify `crates/kernel/src/types/error.rs`** — add `HumanAction` enum + `GatewayError::AllGated { resume_after: Option<DateTime<Utc>>, skipped: Vec<String>, human_action: Option<HumanAction> }` (+ `Display`, + `should_trigger_fallback` arm `false`, + `is_retryable` arm `false`). (Task 1)
- **Modify `crates/kernel/src/types/request.rs`** — add `resume_after: Option<DateTime<Utc>>` to `StreamEvent::Error`. (Task 5)
- **Modify `crates/gateway/src/skip_reason.rs`** — add `GateStatus` enum + `SkipReason::gate_status(&self) -> GateStatus`. (Task 1)
- **Modify `crates/gateway/src/gates/mod.rs`** — `HealthRecorder::on_outcome(&self, o) -> Option<Instant>`. (Task 2)
- **Modify `crates/gateway/src/gates/{circuit_breaker_gate,cooldown,lockout}.rs`** — each sink returns the `Instant` it wrote (or `None`); update sink unit tests. (Task 2)
- **Modify `crates/gateway/src/engine/mod.rs`** — `dispatch_outcome`/`record_outcome` return `Option<Instant>` (min over recorders). (Task 2)
- **Create `crates/gateway/src/engine/exhaustion.rs`** (or a section in `util.rs`) — `GateContribution` enum, `instant_to_utc`, and `build_exhaustion_error(skipped, contributions) -> GatewayError`. (Task 4)
- **Modify `crates/gateway/src/engine/execute.rs`** — §3.1 fallover in `attempt_candidate`; thread the contribution accumulator; build `AllGated` at the selection-empty and attempted-exhaustion sites. (Tasks 3–4)
- **Modify `crates/gateway/src/engine/stream.rs`** — retain the `GatewayError` through the setup match (closing the cooldown/lockout stream-setup gap as a bonus); §3.1 fallover; `Err(AllGated)` on selection-empty; terminal `StreamEvent::Error { resume_after }` on stream exhaustion. (Task 5)
- **`crates/gateway/src/engine/tests.rs`** + **`docs/features/routing/quota-demote-to-tier.md`** — acceptance Gherkins + status flip. (Task 6)

---

### Task 1: Vocabulary — `AllGated` + `HumanAction` + `SkipReason::gate_status()`

**Files:** `crates/kernel/src/types/error.rs`, `crates/gateway/src/skip_reason.rs`. Pure additions, no wiring — behavior-preserving.

- [ ] **Step 1: Failing tests.**
  - In `error.rs` tests: construct `AllGated { resume_after: None, skipped: vec![], human_action: Some(HumanAction::TopUpCredits) }`; assert `!err.should_trigger_fallback(&all_triggers)`, `!err.is_retryable()`, and `err.to_string()` contains "all candidates gated".
  - In `skip_reason.rs` tests: assert `SkipReason::Cooling { until }.gate_status()` is `GateStatus::Timed(until)`; `SkipReason::CircuitOpen { until }.gate_status()` is `Timed(until)`; `LockedOut { reason: LockReason::QuotaExhausted, until: Some(u) }` → `Timed(u)`; `LockedOut { reason: LockReason::Auth, until: None }` → `Terminal(HumanAction::RotateCredential)`; `LockedOut { reason: LockReason::CreditsExhausted, until: None }` → `Terminal(HumanAction::TopUpCredits)`; `OverBudget { .. }` → `Terminal(HumanAction::RaiseBudget)`; `ModelNotFound` / `RouterNotFound` / `RouterDisabled` / `UnsupportedCapability(_)` → `Structural`.
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement.**
  - `error.rs` (kernel already imports/uses `chrono` with serde):
```rust
/// A caller-actionable remedy attached to a terminal `AllGated` when no candidate
/// has a timed retry (all locks are terminal / over budget). Guides the caller —
/// the gateway never acts on it (tenant-agnostic; the caller owns credentials/budget).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HumanAction {
    /// Provider credits/billing exhausted — top up.
    TopUpCredits,
    /// Auth failed / credential invalid — rotate the key.
    RotateCredential,
    /// Every candidate was over budget — raise the budget.
    RaiseBudget,
}
```
    Add the variant (place near `AllAttemptsFailed`):
```rust
    /// Every candidate was gated (health-locked / cooling / breaker-open / over
    /// budget) — none was attemptable. `resume_after` is the **wall-clock**
    /// earliest eligibility (min over timed gates); `None` ⇒ all gates are
    /// terminal ⇒ `human_action` carries the remedy and the caller must not
    /// pause forever. `skipped` is human-readable diagnostics. Distinct from
    /// `AllAttemptsFailed` (a candidate genuinely failed) and `NoCandidates`
    /// (nothing was even configured/eligible). Never triggers fallback.
    #[error("all candidates gated{}", resume_after.map(|t| format!(", resume after {t}")).unwrap_or_else(|| ", human action required".into()))]
    AllGated {
        resume_after: Option<chrono::DateTime<chrono::Utc>>,
        skipped: Vec<String>,
        human_action: Option<HumanAction>,
    },
```
    Add `GatewayError::AllGated { .. } => false,` to BOTH `should_trigger_fallback`'s terminal-arm group and `is_retryable`'s non-match (it's not retryable — a durable pause is the caller's job, not an immediate retry). Confirm the `Display`/`thiserror` derive compiles with the inline format.
  - `skip_reason.rs`:
```rust
use kernel::types::error::HumanAction;

/// How a skip participates in exhaustion aggregation (Task 4). `Timed` gates
/// clear on their own at `Instant`; `Terminal` gates need caller action;
/// `Structural` skips (misconfig / wrong capability) are not "gated" and don't
/// make a run pausable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateStatus {
    Timed(Instant),
    Terminal(HumanAction),
    Structural,
}

impl SkipReason {
    pub fn gate_status(&self) -> GateStatus {
        match self {
            SkipReason::Cooling { until } | SkipReason::CircuitOpen { until } => {
                GateStatus::Timed(*until)
            }
            SkipReason::LockedOut { until: Some(until), .. } => GateStatus::Timed(*until),
            SkipReason::LockedOut { reason, until: None } => {
                GateStatus::Terminal(match reason {
                    crate::gates::lockout::LockReason::CreditsExhausted => HumanAction::TopUpCredits,
                    // Auth (terminal) → rotate; rate/quota are never terminal (until is Some).
                    _ => HumanAction::RotateCredential,
                })
            }
            SkipReason::OverBudget { .. } => GateStatus::Terminal(HumanAction::RaiseBudget),
            SkipReason::ModelNotFound
            | SkipReason::RouterNotFound
            | SkipReason::RouterDisabled
            | SkipReason::UnsupportedCapability(_) => GateStatus::Structural,
        }
    }
}
```
    (Note: `kernel` is a dependency of `gateway`; `use kernel::types::error::HumanAction;` — check the existing import style in `skip_reason.rs`, which already uses `crate::types::...` re-exports; use whichever path resolves. The gateway re-exports kernel types under `crate::types::*`, so prefer `crate::types::error::HumanAction` for consistency.)
- [ ] **Step 4: Verify** — new tests pass; `cargo test -p sensei-gateway --lib` + `cargo test -p sensei-kernel` green (existing counts + new); `cargo test --workspace` green; clippy `-D warnings` + fmt clean. Nothing wired yet ⇒ existing behavior unchanged.
- [ ] **Step 5: Commit:** `feat(kernel,gateway): AllGated error + HumanAction + SkipReason::gate_status()`.

---

### Task 2: `HealthRecorder::on_outcome -> Option<Instant>` (recorders attribute what they wrote)

**Files:** `gates/mod.rs`, `gates/{circuit_breaker_gate,cooldown,lockout}.rs`, `engine/mod.rs`. Behavior-preserving — the returned `Instant` is ignored at the call sites until Tasks 3–5.

- [ ] **Step 1: Update the sink unit tests** to assert the return:
  - `ConnectionCooldownSink`: a `Timeout`/`Network` outcome returns `Some(until ≈ now + cooldown)`; a non-transport / success returns `None`.
  - `ModelLockoutSink`: a 429/quota outcome returns `Some(until)` (the same instant it stored); a terminal (auth/credits) outcome returns `None` (terminal has no resume time); success / non-limit returns `None`.
  - `CircuitBreakerSink`: a failure that trips the breaker to Open returns `Some(next_retry)`; a failure below threshold, or a success, returns `None`.
- [ ] **Step 2:** run → FAIL (return type is `()`).
- [ ] **Step 3: Implement.**
  - `gates/mod.rs`: change the trait to `fn on_outcome(&self, outcome: &AttemptOutcome<'_>) -> Option<std::time::Instant>;` and update its doc: "returns the `Instant` until which this recorder now considers the endpoint unavailable, if this outcome just made it so — used by the engine to build `AllGated.resume_after` (design C4)."
  - `cooldown.rs` `ConnectionCooldownSink::on_outcome`: on a transport fault compute `let until = Instant::now() + self.cooldown; self.store.start(o.router, until); Some(until)`; else `None`.
  - `lockout.rs` `ModelLockoutSink::on_outcome`: return the deadline it set — `Some(until)` when it wrote a **timed** lock, `None` for a terminal lock (`until: None`), `None` when it cleared/ignored. (The existing body already computes `until: Option<Instant>` from `deadline(...)`; return it — but for a terminal lock `deadline` is `None`, and that's exactly the `None` return.) On `success` (clear) → `None`.
  - `circuit_breaker_gate.rs` `CircuitBreakerSink::on_outcome`: after `record_failure`/`record_success`, return `match self.breaker.get_state(endpoint) { BreakerState::Open { next_retry } => Some(next_retry), _ => None }` on the failure path (so a just-opened breaker contributes its `next_retry`); `None` on success. **Use `get_state` (pure read), NOT `open_until`/`can_execute`** — those carry the Open→HalfOpen side effect and must not fire from the sink.
  - `engine/mod.rs`: `dispatch_outcome` returns `Option<Instant>` = the **minimum** non-`None` over the recorders:
```rust
pub(super) fn dispatch_outcome(
    recorders: &[std::sync::Arc<dyn crate::gates::HealthRecorder>],
    endpoint: &str,
    router: &str,
    success: bool,
    error: Option<&crate::types::error::GatewayError>,
) -> Option<std::time::Instant> {
    let o = crate::gates::AttemptOutcome { endpoint, router, success, error };
    recorders.iter().filter_map(|r| r.on_outcome(&o)).min()
}
```
    and `record_outcome` returns `dispatch_outcome(...)` likewise (change its signature to `-> Option<Instant>`).
  - Call sites that ignore it for now: `execute.rs:269` (success) → `let _ = self.record_outcome(...)`; `execute.rs:342` and `stream.rs:187/275` similarly `let _ = ...` (Tasks 3–5 consume the failure-path return). Keep them compiling and behavior-identical.
- [ ] **Step 4: Verify** — sink tests assert the returns; `cargo test -p sensei-gateway --lib` green; `cargo test --workspace` green; clippy/fmt clean. Behavior unchanged (returns ignored at call sites).
- [ ] **Step 5: Commit:** `feat(gateway): HealthRecorder::on_outcome returns the Instant it wrote (min-fanned)`.

---

### Task 3: In-flight classification (§3.1) — recoverable limits fall over on the same request

**Files:** `engine/execute.rs`. **Deliberate behavior change** (pinned by tests): a recoverable provider limit now falls over regardless of the chain's configured `fallback_triggers`; a terminal one stops.

- [ ] **Step 1: Failing tests** (in `engine/tests.rs`):
  - **429 without a `RateLimit` trigger now falls over** (was: stopped). Chain `[A (returns 429), B (noop)]` with `fallback_triggers: []` (or a set NOT containing `RateLimit`). Assert `execute()` is served by B (previously this returned A's error). If an existing test asserted the old "429 stops without trigger" behavior, UPDATE it to the new fallover expectation and note it in the commit.
  - **403-quota falls over even without a `ProviderError` trigger.** Chain `[A (returns `ProviderError { status: Some(403), message: "quota exceeded" }`), B]`, `fallback_triggers: []` → served by B.
  - **401 auth still stops** (terminal). Chain `[A (returns `Authentication`), B]`, any triggers → returns the auth error (does NOT try B), matching today. (There is an existing `execute_stops_on_auth_error` test — confirm it stays green.)
  - **403 credits stops** (terminal). `ProviderError { status: Some(403), message: "insufficient credits" }` → does not fall over.
  - **Unclassified 500 still uses `should_trigger_fallback`** — with `ProviderError` in triggers → falls over; without → stops. (Pin both, proving the `None => should_trigger_fallback` branch is intact.)
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement** in `attempt_candidate`'s `Err(err)` branch — replace `let should_fallback = err.should_trigger_fallback(fallback_triggers);` with the classifier-first decision:
```rust
        // Classify drives the in-flight fallover so the walk and the next-request
        // lockout agree (design §3.1): a recoverable provider limit (429 /
        // 403-quota) falls over on THIS request; a terminal one (401 / credits)
        // stops; a non-limit error keeps the configured trigger semantics.
        let should_fallback = match crate::gates::lockout::classify(&err) {
            Some(reason) => reason.is_recoverable(),
            None => err.should_trigger_fallback(fallback_triggers),
        };
```
  Leave the `record_outcome` call, the `Attempt` push (`fallback_triggered: should_fallback`), and the `Done`/`FallBack`/`Stop` return exactly as they are. (The accumulator that feeds `AllGated` is Task 4 — do NOT add it here, to avoid an unused-value warning.)
- [ ] **Step 4: Verify** — the new fallover tests pass; audit the full suite for any test whose expectation flipped due to the change and update ONLY those whose old expectation encoded "recoverable-limit-without-trigger stops" (each such update is the intended §3.1 behavior — call them out in the commit body). `cargo test -p sensei-gateway --lib` + `cargo test --workspace` green; clippy/fmt clean. If a flip appears in a test that ISN'T about this behavior, STOP and report (it means the change reached further than intended).
- [ ] **Step 5: Commit:** `feat(gateway): classify()-driven in-flight fallover — recoverable limits demote on the same request (§3.1)`.

---

### Task 4: `AllGated` at exhaustion (`execute`)

**Files:** create `engine/exhaustion.rs`; modify `engine/execute.rs`, `engine/mod.rs` (module decl).

- [ ] **Step 1: Failing tests** (in `engine/tests.rs`) — engine-level "served-by"/terminal-error assertions:
  - **All-gated-at-selection → `AllGated` with `resume_after`.** Pre-lock every candidate: `gw.apply_lockout("A:a-model", QuotaExhausted, Some(now+3600s))` and `apply_lockout("B:b-model", QuotaExhausted, Some(now+1800s))` for a chain `[A, B]`. `execute()` → `Err(AllGated { resume_after: Some(t), .. })` where `t ≈ now+1800s` (the MIN). Assert `resume_after` is `Some` and within a tolerance window of the nearer expiry.
  - **All terminal → `resume_after: None` + `human_action`.** `apply_lockout(A, Auth, None)` + `apply_lockout(B, CreditsExhausted, None)` → `Err(AllGated { resume_after: None, human_action: Some(_), .. })`. (human_action is one of the terminal remedies — assert it's `Some`.)
  - **Mixed terminal + timed → `resume_after = min over timed only`.** `apply_lockout(A, CreditsExhausted, None)` (terminal) + `apply_lockout(B, QuotaExhausted, Some(now+1800s))` (timed) → `resume_after ≈ now+1800s` (terminal A excluded from the min).
  - **All breaker-open → `resume_after` from `next_retry`.** Trip both endpoints' breakers (record_failure ×threshold) → `execute()` → `AllGated` with `resume_after` from the breakers' `next_retry`.
  - **Attempted-exhaustion, all recoverable → `AllGated`.** Chain `[A (429), B (429)]` triggers `[]` (both fall over via §3.1, both get locked) → after the walk, `Err(AllGated { resume_after: Some(_), .. })` (from the just-written lockouts' returned instants). 1st request already yields AllGated because both were attempted+locked.
  - **A hard failure keeps `AllAttemptsFailed`.** Chain `[A (429→locked), B (500 unclassified)]` → `Err(AllAttemptsFailed { .. })` (B hard-failed ⇒ not all-gated). Pin this so AllGated doesn't over-fire.
  - **All-structural stays `NoCandidates`.** Chain of only `ModelNotFound` entries → `Err(NoCandidates)` (unchanged).
  - **All-over-budget → `AllGated { resume_after: None, human_action: Some(RaiseBudget) }`** — a **deliberate change** from today's `NoCandidates` (OverBudget is `Terminal(RaiseBudget)`, per design §3.3's raise-budget remedy). If an existing test asserts `NoCandidates` for an all-over-budget chain, UPDATE it to `AllGated`/`RaiseBudget` and note it in the commit.
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement.**
  - `engine/exhaustion.rs`:
```rust
use crate::selection::SkippedCandidate;
use crate::skip_reason::GateStatus;
use crate::types::error::{GatewayError, HumanAction};
use chrono::{DateTime, Utc};
use std::time::Instant;

/// One attempted candidate's contribution to exhaustion aggregation.
pub(super) enum GateContribution {
    /// Attempted, failed with a recoverable limit that locked it until `Instant`.
    Timed(Instant),
    /// Attempted, failed with a terminal limit needing caller action.
    Terminal(HumanAction),
    /// Attempted, failed with a non-limit fault (500 / network / unclassified).
    HardFailure,
}

/// Anchor a monotonic `Instant` deadline to wall-clock. Past/elapsed ⇒ now.
pub(super) fn instant_to_utc(until: Instant) -> DateTime<Utc> {
    let remaining = until.saturating_duration_since(Instant::now());
    Utc::now() + chrono::Duration::from_std(remaining).unwrap_or_else(|_| chrono::Duration::zero())
}

/// Build the terminal error at chain exhaustion from the selection-time skips and
/// the attempted-candidate contributions. `AllGated` iff every candidate was
/// gated (health-skip or classified limit) and none hard-failed; else the caller
/// falls back to its existing `AllAttemptsFailed`/`NoCandidates`. Returns `None`
/// to mean "not all-gated — use the existing terminal error".
pub(super) fn all_gated_error(
    skipped: &[SkippedCandidate],
    contributions: &[GateContribution],
) -> Option<GatewayError> {
    // A hard failure among attempts ⇒ not all-gated.
    if contributions.iter().any(|c| matches!(c, GateContribution::HardFailure)) {
        return None;
    }

    let mut timed: Vec<Instant> = Vec::new();
    let mut human: Option<HumanAction> = None;
    let mut diagnostics: Vec<String> = Vec::new();
    let mut any_gate = false;

    for s in skipped {
        match s.reason.gate_status() {
            GateStatus::Timed(u) => { any_gate = true; timed.push(u); }
            GateStatus::Terminal(h) => { any_gate = true; human.get_or_insert(h); }
            GateStatus::Structural => {} // misconfig noise — ignored for the decision
        }
        diagnostics.push(format!("{}:{} — {}", s.router, s.model, s.reason));
    }
    for c in contributions {
        match c {
            GateContribution::Timed(u) => { any_gate = true; timed.push(*u); }
            GateContribution::Terminal(h) => { any_gate = true; human.get_or_insert(*h); }
            GateContribution::HardFailure => {}
        }
    }

    if !any_gate {
        return None; // nothing was gated (all structural / no candidates)
    }

    let resume_after = timed.into_iter().min().map(instant_to_utc);
    // If there's a timed retry, prefer it (pause); else surface the human action.
    let human_action = if resume_after.is_some() { None } else { human };
    Some(GatewayError::AllGated { resume_after, skipped: diagnostics, human_action })
}
```
    Map a classified attempt error to a `GateContribution` (a helper used by `attempt_candidate`; put it in `exhaustion.rs`):
```rust
/// A failed attempt's contribution, from its error + the `Instant` the recorder
/// pipeline just wrote (for a recoverable limit that locked it).
pub(super) fn contribution_for(
    err: &GatewayError,
    written_until: Option<Instant>,
) -> GateContribution {
    use crate::gates::lockout::{classify, LockReason};
    match classify(err) {
        Some(r) if r.is_recoverable() => match written_until {
            Some(u) => GateContribution::Timed(u),
            None => GateContribution::HardFailure, // recoverable but nothing locked ⇒ treat as failure
        },
        Some(LockReason::CreditsExhausted) => GateContribution::Terminal(HumanAction::TopUpCredits),
        Some(_) => GateContribution::Terminal(HumanAction::RotateCredential), // Auth
        None => GateContribution::HardFailure,
    }
}
```
  - `engine/mod.rs`: add `mod exhaustion;`.
  - `engine/execute.rs`:
    - `attempt_candidate` gains a param `contributions: &mut Vec<GateContribution>`; in the `Err(err)` branch, capture the recorder return and push the contribution:
      ```rust
      let written_until = self.record_outcome(&endpoint, &candidate.router, false, Some(&err));
      // ... existing should_fallback + Attempt push ...
      contributions.push(super::exhaustion::contribution_for(&err, written_until));
      ```
      (Replace the earlier `let _ = self.record_outcome(...)` on the failure path with the captured `written_until`.) The success path keeps `let _ = self.record_outcome(&endpoint, &candidate.router, true, None);`.
    - `execute`: build `let mut contributions: Vec<GateContribution> = Vec::new();` before the walk; pass `&mut contributions` into `attempt_candidate`.
    - **Selection-empty site** (currently `return Err(NoCandidates)`): replace with
      ```rust
      if result.all_candidates.is_empty() {
          if let Some(gated) = super::exhaustion::all_gated_error(&result.skipped, &[]) {
              return Err(gated);
          }
          return Err(GatewayError::NoCandidates { capability: request.capability.clone() });
      }
      ```
    - **Attempted-exhaustion site** (currently builds `AllAttemptsFailed`): before constructing it (after the readiness-probe block), try `all_gated_error(&result.skipped, &contributions)`; if `Some(gated)`, record the failed terminal call (as today) then `return Err(gated)`; else fall through to the existing `AllAttemptsFailed`. Keep the `record_call` best-effort write in both branches.
      - **GUARD (soundness — landed in `fa2d243`):** only aggregate to `AllGated` at this site when `attempts.len() == max_attempts` — i.e. the walk actually attempted every candidate it was willing to try. A *terminal* error (`Auth`/`Credits`) `Stop`s the walk on a non-last candidate, leaving admitted-but-untried candidates that are *ready to serve now*; returning `AllGated` (pause) there is a bug. The guard makes such an early stop fall through to `AllAttemptsFailed` (existing behavior), while a walk that attempts everything (all fall over, or the *last* candidate stops) stays eligible. The `HardFailure` veto inside `all_gated_error` is independent and still applies. (The selection-empty site is NOT guarded — it has no attempts and aggregates purely from `result.skipped`.)
- [ ] **Step 4: Verify** — all Task-4 tests pass; `cargo test -p sensei-gateway --lib` + `cargo test --workspace` green; clippy/fmt clean. Confirm: `NoCandidates` still returned for all-structural; `AllAttemptsFailed` still returned when a candidate hard-failed; `resume_after` excludes terminal gates and is the min over timed.
- [ ] **Step 5: Commit:** `feat(gateway): AllGated{resume_after} at execute exhaustion — durable pause when every candidate is gated`.

---

### Task 5: `execute_stream` — retain the error, §3.1 fallover, `AllGated` + terminal `resume_after`

**Files:** `crates/kernel/src/types/request.rs` (`StreamEvent::Error.resume_after`), `engine/stream.rs`.

- [ ] **Step 1: Failing tests** (in `engine/tests.rs`, streaming):
  - **Selection-empty (all pre-locked) → `Err(AllGated)`** returned before the stream (mirrors `execute`). Pre-lock both candidates, `execute_stream()` → `Err(AllGated { resume_after: Some(_), .. })`.
  - **§3.1 in stream:** chain `[A (429), B (noop)]` triggers `[]` → the stream switches to B and streams (a `ProviderSwitch` then B's chunks), rather than terminating on A. (Previously a 429 without a `RateLimit` trigger would terminate.)
  - **Stream-setup exhaustion, all recoverable → terminal `StreamEvent::Error { resume_after: Some(_) }`.** Chain `[A (429), B (429)]` triggers `[]` → both fall over at setup, terminal `StreamEvent::Error` carries `resume_after`.
  - **Bonus — the stream-setup cool/lock gap closes:** after a stream setup failure on a `Timeout`/429, the router/model is now cooled/locked (assert via `gw.cooldown.cooling_until` / `gw.model_lockout.get`). This proves the error is retained through the setup path (previously it was dropped to `None`).
- [ ] **Step 2:** run → FAIL.
- [ ] **Step 3: Implement.**
  - `request.rs`: add `resume_after: Option<chrono::DateTime<chrono::Utc>>` to `StreamEvent::Error`. Update EVERY existing `StreamEvent::Error { code, message }` construction (in `stream.rs` and any tests) to `StreamEvent::Error { code, message, resume_after: None }`. (`git grep -n 'StreamEvent::Error'` to find them all.)
  - `stream.rs`, in the setup match: retain the real error. Replace the `fail_code`/`fail_message`/`fail_should_fallback`/`fail_record_cb` locals' population so the `GatewayError` is kept, e.g. add `let mut fail_error: Option<GatewayError> = None;` and in the `Err(e)` arms set `fail_error = Some(e)` (compute `fail_code`/`fail_message` from it), and set `fail_should_fallback` via the SAME classifier-first rule as `execute`:
    ```rust
    let should_fallback = match crate::gates::lockout::classify(&e) {
        Some(reason) => reason.is_recoverable(),
        None => e.should_trigger_fallback(&fallback_triggers),
    };
    ```
    Then at the outcome dispatch (`fail_record_cb` block), pass the retained error and CAPTURE the written instant:
    ```rust
    let written_until = if let Some(err) = &fail_error {
        super::dispatch_outcome(&recorders, &endpoint, &candidate.router, false, Some(err))
    } else {
        super::dispatch_outcome(&recorders, &endpoint, &candidate.router, false, None)
    };
    ```
    Push a `GateContribution` (from `fail_error` + `written_until` via `exhaustion::contribution_for`) into a `Vec` accrued across the stream loop. (This both closes the cool/lock gap AND feeds the terminal aggregation.) Update the stale comment at the old drop site.
  - Selection-empty (currently `Err(NoCandidates)`): mirror `execute` — `all_gated_error(&result.skipped, &[])` else `NoCandidates`. NOTE: `result.skipped` must be captured BEFORE `result.all_candidates` is moved into `candidates` (reorder: read `result.skipped` into an owned `Vec` first, since the stream closure moves owned state).
  - Stream-exhaustion terminal (the in-loop `yield StreamEvent::Error { .. }` that fires when `!(has_more && fail_should_fallback)`): apply the **same guard as `execute`** (Task 4). Only treat it as `AllGated` when the walk reached the LAST candidate — i.e. `!has_more` (an early non-fallback stop with `has_more == true` leaves untried ready candidates → plain terminal `Error`, NOT `AllGated`). So: `if !has_more { match all_gated_error(&skipped_owned, &contributions) { Some(GatewayError::AllGated { resume_after, .. }) => yield StreamEvent::Error { code: "all_gated".into(), message, resume_after }, _ => yield StreamEvent::Error { code: fail_code, message: fail_message, resume_after: None } } } else { yield StreamEvent::Error { code: fail_code, message: fail_message, resume_after: None } }`. (The owned `skipped` + `contributions` must be captured into the `'static` stream closure alongside the other moved state; `contributions` is accrued across the loop via `exhaustion::contribution_for`.)
- [ ] **Step 4: Verify** — streaming tests pass, incl. the gap-closing cool/lock assertions; `cargo test -p sensei-gateway --lib` + `cargo test --workspace` green; clippy/fmt clean. Confirm mid-stream (post-first-byte) errors are UNCHANGED (still terminal `StreamEvent::Error { resume_after: None }`, no fallback).
- [ ] **Step 5: Commit:** `feat(gateway): execute_stream AllGated + §3.1 fallover + retained error closes the stream-setup cool/lock gap`.

---

### Task 6: Acceptance Gherkins + doc status

**Files:** `engine/tests.rs`, `docs/features/routing/quota-demote-to-tier.md`, `docs/features/routing/model-lockout.md`.

- [ ] **Step 1:** Add engine tests named for the `quota-demote-to-tier.md` "additional" scenarios not yet covered by Tasks 3–5, asserting the observable behavior:
  - "A provider 403 quota falls over on the SAME request" (§3.1 — likely already covered in Task 3; if so, reference it, don't duplicate).
  - "All tiers gated returns a terminal error carrying resume_after" (Task 4).
  - "A durable consumer pauses and resumes at resume_after" — assert the `AllGated.resume_after` is a usable future `DateTime<Utc>` (the orchestrator's pause input; there's no orchestrator here, so assert the field's shape/value, not a pause).
  - "Mixed terminal + timed exhaustion → resume_after = min over timed only; human action for the terminal" (Task 4).
  - "All candidates terminal → resume_after None + human action, never pause forever" (Task 4).
  - "All candidates circuit-open → resume_after from breaker next_retry" (Task 4).
  - "Subscription quota exhaustion does NOT demote" — assert a `GatewayError::QuotaExceeded` (subject/tier, from `check_quota`) still returns `QuotaExceeded` and does NOT become `AllGated` and does NOT attempt other models. (This guards the hard-stop-vs-provider-limit distinction end-to-end.)
- [ ] **Step 2:** Run each → green (some may already pass from Tasks 3–5; this task closes any remaining scenario with an explicit named test).
- [ ] **Step 3:** Flip status in the feature docs: in `quota-demote-to-tier.md` and `model-lockout.md` frontmatter/prose, mark the now-implemented scenarios (demote-to-tier, resume_after/AllGated, in-flight §3.1) as implemented (Phase 1 · SP-0 (d)+(e)); leave genuinely-deferred ones (calendar-clock exact reset boundary, jitter, bounded-LRU eviction) marked planned with a pointer to (f). Do NOT claim the deferred items as done.
- [ ] **Step 4: Verify** — `cargo test -p sensei-gateway --lib` + `cargo test --workspace` green; clippy/fmt clean; `make check` clean.
- [ ] **Step 5: Commit:** `test(gateway): quota-demote-to-tier + AllGated acceptance scenarios; docs: flip SP-0 (d)+(e) status`.

---

## Self-Review

- **Spec coverage** (design §3.1/§3.3, §7 acceptance): in-flight recoverable fallover → Task 3; `AllGated{resume_after(min over timed, wall-clock), skipped, human_action}` with all-terminal fail-fast → Tasks 1+4; both `execute` and `execute_stream` → Tasks 4+5; the subscription-`QuotaExceeded`-does-NOT-demote hard-stop preserved (it's raised by `check_quota` before selection and is a distinct variant `all_gated_error` never produces) → Task 6. The soundness invariant (AllGated ⟺ every candidate gated, none hard-failed) is enforced in `all_gated_error` (HardFailure ⇒ `None` ⇒ existing error).
- **Behavior preservation & the one deliberate change:** Tasks 1–2 are pure additions (returns ignored). Task 3 is the intended §3.1 change (recoverable limits fall over without an explicit trigger; terminal ones stop) — pinned by new tests, and any existing test that encoded the old behavior is updated with rationale in the commit. `NoCandidates` (all-structural) and `AllAttemptsFailed` (a hard failure) are explicitly preserved (Task 4 tests pin both).
- **Type consistency:** `HumanAction` (kernel, Task 1) is used by `GateStatus`/`gate_status` (Task 1), `contribution_for`/`all_gated_error` (Task 4), and `AllGated` (Task 1). `HealthRecorder::on_outcome -> Option<Instant>` (Task 2) feeds `dispatch_outcome`→`record_outcome`→`attempt_candidate`'s `written_until`→`contribution_for` (Task 4) and the stream path (Task 5). `GateContribution` (Task 4) is produced in both `execute` and `execute_stream`. `StreamEvent::Error.resume_after` (Task 5) mirrors `AllGated.resume_after`.
- **Sequencing (each green + committed):** 1 vocabulary (unused) → 2 recorder returns (ignored) → 3 in-flight fallover (observable, pinned; accumulator NOT yet added to avoid dead-code) → 4 AllGated for execute (adds + consumes the accumulator together) → 5 stream (same, + StreamEvent field + gap-closing) → 6 acceptance + docs. No broken intermediate; the accumulator is introduced with its consumer (the (d) lesson).
- **Wall-clock correctness:** `instant_to_utc` anchors a monotonic `Instant` to `Utc::now()` via the remaining duration (past ⇒ now); `resume_after` is "earliest eligibility, retry not guaranteed" (design §3.3). Tests assert a tolerance window, not an exact instant, to avoid wall-clock flakiness.
- **Bonus fixed:** retaining the `GatewayError` through the stream setup path (Task 5) closes the known "stream setup failures don't cool/lock" gap flagged in (c)/(d).
- **Placeholder scan:** none — new types/signatures shown in full; test bodies specified against existing analogues (the (d) lockout integration tests + `apply_lockout`). `skipped` is `Vec<String>` diagnostics by deliberate choice (structured detail deferred; the orchestrator acts on `resume_after`/`human_action`).
- **Deferred, re-stated:** calendar-clock exact reset boundary + jitter, `ResilienceConfig`/builder, `EndpointKey`, bounded-LRU eviction, structured `skipped` → **(f)**.

## Execution Handoff

Subagent-driven in an isolated worktree off `develop`; per-task spec + code-quality review (behavior-adding Tasks 3, 4, 5 get the full treatment); final whole-branch review; `finishing-a-development-branch` → merge to `develop`. Then **(f)** — builder `.with_*` + `ResilienceConfig` presets + bounded-LRU eviction + injected clocks — the last SP-0 slice, after which SP-0 (health gates) is complete and the phased program (SP-CAT free-tier catalog → reference chains → SP-1 orchestrator → SP-DATA) begins.
