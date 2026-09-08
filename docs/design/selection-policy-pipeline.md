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
source: crates/gateway/src/selection.rs, crates/gateway/src/circuit_breaker.rs, crates/gateway/src/engine.rs, crates/cloud-providers/src/base.rs, crates/kernel/src/types/error.rs
review: 4 parallel design reviews (coverage · SRP/consistency · adversarial correctness · sequencing) — resolutions in §0
---

# Selection Policy Pipeline & Health Gates (SP-0)

**Goal.** Add three provider health gates — **connection cooldown**, **model
lockout** (per-reason), and **quota demote-to-tier** — via a
composable policy pipeline, so future gates/strategies compose without a
monolith, *and* so a provider limit actually causes fallover on the request that
hits it (not just the next one).

**Non-goals (SP-0).** Free-tier catalog + `tiers` (SP-CAT); live usage metering +
`headroom`/predicted-lockout (SP-DATA); persisted/multi-instance health state;
the *stateful* part of expiration tracking and the orchestrator's cumulative
retry-wait budget (both moved out of SP-0 — §0/R-S3). SP-0 keeps only the
reactive `401 → auth-lock` signal from expiration tracking.

**Coordination.** Land **issue #39 (engine split) first** (or on a shared
branch). SP-0 migration steps 1–4 are `selection.rs`/`circuit_breaker.rs`/new
files (parallel-safe); step 5 edits the exact `engine.rs` outcome/exhaustion
sites #39 relocates.

---

## 0. Review resolutions (traceability)

Four parallel functional/adversarial design reviews ran before this revision.
Each finding and its resolution:

| # | Finding (reviewer) | Resolution in this design |
|---|---|---|
| C1 | Gates only affect *next* request; in-flight fallover is `should_trigger_fallback` over a frozen list. `Network`/403-quota never fall over → demote fails on the triggering request. | **One classifier drives both** the in-flight walk *and* the lockout write (§3.1). A classified **recoverable** provider limit (`RateLimit`/`QuotaExhausted`) falls over in-walk; **terminal** (`CreditsExhausted`/`Auth`) breaks. The recorder writes the lockout for next time. |
| C2 | `classify()` has no inputs — status (401/403 both → `Authentication`), body, and `retry_after_ms` (always `None`) are discarded before selection reacts. | Classify at the **adapter boundary** (`base.rs`): attach `ProviderSignal { status, retry_after, body_snippet(redacted) }` to the outcome; `classify()` is a pure fn over that (§3.2). Scope expands to `base.rs` + `error.rs`. |
| C3 | `resume_after: Instant` isn't durable; reset boundaries are wall-clock. | `Instant` for internal math + tests; **exported `resume_after` is `DateTime<Utc>`** (§3.3). Reset boundaries use an injected calendar clock. |
| C4 | `resume_after` read from selection-time `skipped` → `None` on the exhausting request. | Aggregate from **post-walk** health state (recorder returns the `until` it wrote; fold over attempted-failed + selection-time timed skips) (§3.3). |
| H1 | `OutcomeSink` tagged "Observer" but mutates correctness state — collides with orchestrator journal≠hooks (D5). | **Rename → `HealthRecorder`** (reliable, never best-effort). Add a *separate* best-effort **`SelectionObserver`** seam mirroring `OrchestratorHooks` (carries the fallback trail for `on_agent_model_attempt`) (§2). |
| H2 | `HealthStore` god-trait; read+write+3 concerns in one; "breaker implements the endpoint half" impossible. | Split into narrow ports: `EndpointHealthRead` + `RouterHealthRead` (gates), `HealthRecorder` (write). Breaker implements the endpoint read + recorder (§2). |
| H4 | `CandidateView` "resolved" contradicts structural gates; eager cost defeats "budget last". | View construction is a **fallible resolver** emitting structural skips; pipeline starts at `Capability`; **cost is lazy** in `BudgetGate` (§2.1). |
| H5 | Only `execute` wired; `execute_stream` diverges. | Both dispatch paths route through `recorders.on_outcome` (§5, step 2/5). |
| H6 | Config-swap/key-rotation never clears terminal lockouts → dead until restart. | `try_update_config`/`refresh_router_keys` clear/evict affected health state (§5b). |
| H7 | Escalation never resets on success; concurrent fan-out lost-update jumps to max. | Reset/decay on success (mirror breaker); increment **once per lock→release→relock cycle** via a generation guard (§3.2). |
| M1 | Overloading `AllAttemptsFailed`; terminal-only exhaustion could pause forever. | New `GatewayError::AllGated { resume_after: Option<DateTime<Utc>>, skipped, human_action: Option<HumanAction> }`. Terminal-only ⇒ ~~**fail-fast human-action, never pause**~~ → **REVERSED 2026-09-04, see below** (§3.3). |
| M2 | `CircuitOpen` carries no `until` → excluded from `resume_after`. | `CircuitOpen { until }` (breaker exposes `next_retry`); breaker adopts the injected clock (§2, §8). |
| M3 | Can't distinguish exact reset from estimate on re-eval. | `Until::{ Exact(t), Backoff(t) }` — clamp only `Backoff` (§3.2). |
| M4 | Per-tier strategy needs a tier tag; hardcoded `sort_by_key(priority)` fights a strategy. | `RoutingStrategy` is the **one** ordering seam; SP-0 ships `PriorityStrategy` replacing the hardcoded sort; `SelectedModel`/`ChainEntry` gain an optional `tier`/`segment` marker (populated by SP-CAT); `ctx` is an extensible struct (§2). |
| M6 | Direct↔chain merge traps (check order, provider fallback, api_model_id, priority). | Parity pinned by tests **before** the merge; direct builder sets `no provider-fallback`, `priority=1`, `entry_api_model_id=None` (§6 step 1b). |
| key | (Superseded by user direction) Cross-tenant leak concern assumed a shared instance. | Gateway is **tenant-agnostic**: per-tenant isolation = **one `Gateway` entity + config per tenant**, not per-credential keys. Lockout/cooldown keyed by plain `router:model` (per-instance). Plus an **external lockout/suspend control seam** so the tenant-aware caller drives lockout on *its* instance (§5c). |
| S1 | #39 coupling. | #39 first; steps 1–4 parallel-safe, step 5 waits (§6). |
| S2 | Sink halves dormant until a step-5 big-bang. | Recorder fan-out pulled into **step 2** (breaker-only, behavior-preserving) (§6). |
| S3 | SP-0 scope drift (expiration stateful, retry-budget). | Moved out (non-goals); §16 + governance README reconciled. |
| S4 | Is metering an OutcomeSink? | No — metering stays a **separate** write path (`GatewayStore`); predicted-lockout (SP-DATA) *reads* metering and *writes* a lockout via `HealthRecorder` (§9). |
| S5 | Multi-instance `resume_after` authority. | Documented: authoritative only when one instance owns the subject's traffic; fleet-wide correctness deferred to persisted state (SP-DATA) (§9). |

### M1 REVERSED — 2026-09-04

**"Terminal-only ⇒ fail-fast human-action, never pause" no longer holds.** A terminal-only
`AllGated` that carries a `human_action` is now a durable, indefinite pause — SP-DATA-3's
HOTL class — and only an `AllGated` carrying *neither* a `resume_after` nor a
`human_action` fails.

M1 feared a run pausing forever on something nothing would ever clear. That fear was
right, and `human_action` turns out to be the exact discriminator for it: a `Some(_)` IS
the statement that a named party can end the pause. The `None` arm still fails, so the
fear is answered rather than ignored.

What forced the reversal is that "fail fast" was not, in this system, a recoverable
outcome. SP-7a moved window-fit into `ContextWindowGate` and deleted the orchestrator's
pre-dispatch halt, so an over-every-window run began arriving here as
`AllGated { resume_after: None, human_action: Some(UseLargerContextWindow) }` — and
`classify_gateway_error` failed the node, making the run terminal. **No supported command
revives a terminal run:** `SchedulerStore::force_wake` matches `status = 'paused'`,
`torii run wake` reports "not queued", and `run submit` refuses a used id. Every completed
node's memo, journaled mutation and spent token stayed durable and unreachable behind
hand-written SQL against `scheduled_runs`. A terminal state that names a human remedy no
command can act on is incoherent — and it was incoherent for every gate that produces one
(auth lockout, capability, budget), which is why the fix is not window-specific.

The pause deliberately carries **no deadline**, so nothing wakes it on a timer into the
identical refusal. `list_paused` surfaces it; `force_wake` clears it once the operator has
acted on the remedy the reason names.

Implemented in `classify_gateway_error` (`orchestrator/src/executor/support.rs`, whose doc
carries this argument) and pinned by
`a_budgeted_over_window_run_pauses_recoverably_rather_than_dying` (the journal row) and
`classify_gateway_error_pauses_on_a_deadline_or_a_human_action_and_fails_on_neither` (all
three arms, including a `TopUpCredits` remedy, so the rule is not read as window-specific).

---

## 1. Current state (what we refactor)

`ModelSelectionService` holds `&GatewayConfig` + `&CircuitBreakerManager`.
`validate_chain_entry` and its near-duplicate `validate_direct` run a fixed
inline sequence (model → router → capability → breaker → budget) with a
stringly-typed reason. `select_all` resolves the candidate list **once**;
`engine.rs` walks that frozen list and decides fallover via
`GatewayError::should_trigger_fallback`. The write side is
`circuit_breaker.record_*` at four engine sites (2 in `execute`, 2 in
`execute_stream`). Adapters collapse 401 **and** 403 to
`GatewayError::Authentication` and never populate `RateLimit.retry_after_ms`
(`base.rs`).

## 2. Target design

Concerns and their seams:

```rust
// ── Provider signal captured at the ADAPTER boundary (C2) ───────────────────
pub struct ProviderSignal { pub status: Option<u16>, pub retry_after: Option<Duration>, pub body_snippet: Option<String> } // body redacted

pub enum LockReason { RateLimit, QuotaExhausted, CreditsExhausted, Auth }   // provider-side only
impl LockReason { fn is_recoverable(&self) -> bool /* RateLimit|QuotaExhausted */ }

// classify is a PURE fn over the captured signal (unit-tested table)
fn classify(sig: &ProviderSignal) -> Option<LockReason>;

// ── Typed skip reason; timed variants carry provenance-tagged wall-anchored Until
pub enum Until { Exact(Instant), Backoff(Instant) }   // clamp only Backoff (M3)
pub enum SkipReason {
    ModelNotFound, RouterNotFound, RouterDisabled, UnsupportedCapability(Capability),
    OverBudget { estimated: f64, budget: f64 },
    CircuitOpen { until: Instant },                    // M2
    Cooling    { until: Instant },
    LockedOut  { reason: LockReason, until: Option<Until> },  // None = terminal
}
impl SkipReason { fn until(&self) -> Option<Instant>; fn is_terminal(&self) -> bool; }

pub enum GateVerdict { Admit, Skip(SkipReason) }

// ── Admission: read-only gates (chain of responsibility) ────────────────────
pub trait AdmissionGate: Send + Sync {
    fn name(&self) -> &'static str;
    fn evaluate(&self, cand: &CandidateView<'_>, ctx: &SelectionCtx<'_>) -> GateVerdict;
}
// ── Ordering: the ONE ordering seam (Strategy); SP-0 ships PriorityStrategy ──
pub trait RoutingStrategy: Send + Sync { fn order(&self, admitted: &mut Vec<SelectedModel>, ctx: &SelectionCtx<'_>); }
// ── Reaction: RELIABLE state reducer (NOT an observer) ──────────────────────
pub struct AttemptOutcome<'a> { pub endpoint: EndpointKey, pub error: Option<&'a GatewayError>, pub signal: Option<ProviderSignal>, pub success: bool }
pub trait HealthRecorder: Send + Sync { fn on_outcome(&self, o: &AttemptOutcome<'_>) -> Option<Instant>; } // returns the `until` it wrote (C4)
// ── Health state: NARROW read ports (H2) ────────────────────────────────────
pub trait EndpointHealthRead: Send + Sync { fn endpoint_state(&self, k: &EndpointKey) -> EndpointHealth; }
pub trait RouterHealthRead:   Send + Sync { fn router_state(&self, router: &str) -> RouterHealth; }
// ── Best-effort observability (mirrors OrchestratorHooks; H1) ────────────────
pub trait SelectionObserver: Send + Sync { fn on_candidate_skipped(&self, _:&SkippedCandidate) {} fn on_attempt(&self, _:&AttemptInfo) {} } // no-op defaults, isolated-but-logged
```

`EndpointKey { router, model }` is opaque (no hardcoded `format!`) so its shape
can grow if ever needed. The gateway is **tenant-agnostic** — the key carries no
credential/tenant dimension (§5c). `SelectionCtx` is a struct (config, read
ports, injected `now`, injected calendar clock, optional usage source for
SP-DATA) — extensible without a trait break.

### 2.1 `validate_*` collapses to resolve-then-gate-then-order

```
resolve(cand_source) -> Result<CandidateView, SkipReason>   // structural skips: ModelNotFound/RouterNotFound/RouterDisabled
   → run gates [Capability, ConnectionCooldown, CircuitBreaker, ModelLockout, Budget(lazy cost)]
   → PriorityStrategy.order(admitted)                        // the one ordering seam (replaces hardcoded sort)
```
Direct and chain differ only in how `resolve` builds the view (§6 step 1b pins
parity). Cost estimation moves off `ModelSelectionService` into `BudgetGate`.

### 2.2 Gate order & keys
`Capability → ConnectionCooldown(router,cred) → CircuitBreaker(endpoint) →
ModelLockout(endpoint) → Budget(lazy)`. Health reads key by `EndpointKey`
(`router:model`) / `router` for cooldown. Tenant isolation is by instance, not by key (§5c).

## 3. Behavior

### 3.1 One classifier, two mechanisms (C1 — the core fix)
On a provider response the adapter attaches a `ProviderSignal`. `classify(sig)`
yields a `LockReason`. Then:
- **In-flight walk:** if `reason.is_recoverable()` (rate-limit / quota) → this counts as a fallover trigger and the walk continues to the next candidate **on this request**; if terminal (credits / auth) → the walk breaks. This *replaces* relying on `should_trigger_fallback` misclassifying 403-quota as `Authentication`. (403+quota-body ⇒ `QuotaExhausted` recoverable; 401 ⇒ `Auth` terminal.)
- **Next request:** the `HealthRecorder` writes a lockout/cooldown keyed by `EndpointKey`, so the gate skips it next time.
Both use the **same** `classify()` — they cannot disagree.

### 3.2 Model lockout
Per-reason durations (`rate_limit`≈60s; `quota_exhausted`→next reset boundary via the calendar clock, else ~1h; `credits`/`auth`→`until: None` terminal). `retry_after` from a real header ⇒ `Until::Exact` (never clamped); synthetic backoff ⇒ `Until::Backoff` (clamped to `max_cooldown_ms`). Escalation counter **resets/decays on success** and increments **at most once per lock→release→relock cycle** (generation guard; not once per concurrent failure). Bounded map with LRU eviction that never evicts an entry mid-lock.

### 3.3 Quota demote-to-tier + exhaustion
Demote is emergent (recoverable classification falls over in-walk, §3.1). On full exhaustion the engine builds `resume_after` from **post-walk** state: `min` over `Until` of (a) selection-time timed skips **and** (b) the `until` each `HealthRecorder.on_outcome` returned for attempted-failed candidates, plus `CircuitOpen.until`. Result → `GatewayError::AllGated { resume_after: Option<DateTime<Utc>>, skipped, human_action }`:
- some timed ⇒ `resume_after = Some(min)` → orchestrator durable pause (wall-clock).
- all terminal / none timed ⇒ `resume_after = None` + `human_action` (top-up-credits / rotate-key / raise-budget) → ~~**fail-fast, never pause forever**~~ → **the INDEFINITE HOTL pause** (M1 reversed 2026-09-04, above): never auto-woken, surfaced by `list_paused`, cleared by an operator's `force_wake` once they have acted on the remedy. Only an `AllGated` with neither a `resume_after` nor a `human_action` fails — that is the "pause forever on nothing" case M1 was right to fear.
`resume_after` means "earliest eligibility, retry not guaranteed." Subscription `GatewayError::QuotaExceeded` (subject/tier) stays a **hard stop**, never demotes, never carries `resume_after`.

## 4. Config
`GatewayConfig.resilience` (defaulted; absent ⇒ today's behavior): `connection_cooldown` (base/max/jitter/steps), `model_lockout` (per-reason durations, `max_cooldown_ms`, eviction_cap). Jitter RNG and calendar clock are **injected** (seedable) for deterministic tests (§8).

## 5. Wiring (composition, not subclasses)
`GatewayBuilder` gains `.with_gate` / `.with_recorder` / `.with_observer` / `.with_resilience(preset)`. `ModelSelectionService` takes `gates`, `strategy`, and the **read ports**; the engine owns the `HealthRecorder`s and the `SelectionObserver`s. `CircuitBreakerManager` implements `EndpointHealthRead` + `HealthRecorder` (endpoint concern only) — reused, not rewritten.

### 5b Lifecycle (H6)
`try_update_config` and `refresh_router_keys` clear `Auth`/`Credits` (terminal) lockouts for affected endpoints and evict health entries for removed/renamed `router:model`. Lockout lifecycle is specified relative to config lifecycle (not inherited implicitly from the breaker).

### 5c Tenant-agnostic core + lockout callback (caller persists)
**Neither the gateway nor the orchestrator has any concept of tenants** — tenancy is entirely a wrapper above them. The gateway keeps lockout/cooldown state **in-memory for its instance lifetime only** and **never persists** (consistent with the pure-core principle; the only persistence seams are the caller-implemented `GatewayStore`/`VaultStore`). Durability and any per-tenant scoping are the **caller's** responsibility, via a bidirectional seam:
- **OUT (gateway → caller):** when the gateway marks a model locked / timed-out, it fires a **lockout callback** the caller persists — `on_lockout(EndpointKey, reason, until)` on `SelectionObserver` (best-effort, isolated). The gateway announces; it does not persist.
- **IN (caller → gateway):** the caller re-seeds persisted lockouts on a fresh instance, or suspends a model from its own signals — `Gateway::apply_lockout(ep, reason, until)` / `clear_lockout(ep)`.

```rust
trait SelectionObserver { fn on_lockout(&self, _:&EndpointKey, _:LockReason, _:Option<Until>) {} /* + on_candidate_skipped, on_attempt */ }
impl Gateway { pub fn apply_lockout(&self, ep:&EndpointKey, reason:LockReason, until:Option<Until>); pub fn clear_lockout(&self, ep:&EndpointKey); }
```
A suspension applies only to **this** instance's in-memory state — never system-wide, never per-tenant (the core doesn't know tenants). A multi-tenant wrapper runs **one gateway entity per tenant** and persists/re-seeds each independently.

## 6. Migration (strangler-fig · TDD · #39-first · small green commits)
1. **Typed `SkipReason`** (replace string reasons; update assertions). 1b. **Pin direct↔chain parity** with tests for both-unknown-direct and model-only-no-router **before** merging the two validate paths.
2. **Extract gates** + introduce the **`HealthRecorder` fan-out at all four outcome sites** with breaker-only (behavior-preserving, S2); `CircuitBreakerManager` implements the endpoint read + recorder; `PriorityStrategy` becomes the one ordering seam.
3. **Adapter boundary:** capture `ProviderSignal` (status/retry_after/body); `classify()` pure fn (+ table test); stop collapsing 403-quota into `Authentication`. **ConnectionCooldown** gate+recorder (live end-to-end).
4. **ModelLockout** gate+recorder (per-reason, provenance clamp, escalation guard, tenant-agnostic `router:model` key) + the `on_lockout` callback and `apply_lockout`/`clear_lockout` control (§5c). Wire recoverable classification into the in-flight walk (§3.1).
5. **Engine (post-#39):** post-walk `resume_after` (wall-clock) + `AllGated` + `human_action`; both `execute` and `execute_stream`.
6. **Builder** `.with_*` + `resilience` config + presets.

## 7. Acceptance criteria
The Gherkin `## Scenarios` in the three feature docs — **expanded** with the review's missing scenarios (subscription-quota-does-not-demote; success-clears-lockout+escalation; two-reasons-racing; cooldown-vs-lockout precedence; mixed terminal+timed exhaustion; all-breaker-open resume_after; escalation-grows vs clamp split; 401 auth terminal; exact-reset-below-synthetic honored). Plus refactor-safety: every existing `selection.rs` test passes with typed reasons; direct↔chain parity tests green.

## 8. Testing
- **Engine-level "served-by" tests** (assert `response.model`/served fallback), not only gate-verdict tests — a gate-isolation test alone is vacuous against "served by next model" (R1).
- Per-gate unit tests (fake read ports + injected `now`); `classify()` table test.
- **Injected seedable jitter RNG** (so "escalates" isn't flaky) and **injected calendar clock** (reset boundaries deterministic).
- `resume_after` wall-clock assertions with ≥2 distinct expiries + one terminal (proves `min` over timed only, terminal excluded).
- Escalation: reset-on-success; once-per-cycle under simulated concurrent failures.
- Lifecycle: `refresh_router_keys` clears an `Auth` lock.

## 9. Notes / boundaries
- **Metering is a separate write path** (`GatewayStore`), not a `HealthRecorder`; predicted-lockout (SP-DATA) reads metering and writes a lockout via `HealthRecorder` — the `ModelLockoutGate` is unchanged.
- **Read ports stay sync/in-memory** (hot path); "SP-DATA persists" = write-behind + warm-load, never sync DB on the gate path.
- **Tenancy & durability:** the gateway *and* orchestrator cores have **no tenant concept** — a wrapper runs one gateway entity per tenant. In-memory health state is per-instance; **durability/re-seed across restarts is the caller's job** via the `on_lockout` callback + `apply_lockout` (§5c). `resume_after` is authoritative for the instance that owns the traffic.
- **SRP/OCP:** a new gate/strategy/recorder is a new small file + one registration; existing units untouched. Gateway now has the *same* two-seam shape as the orchestrator (reliable recorder + best-effort observer) and the same narrow-trait + builder idiom.
