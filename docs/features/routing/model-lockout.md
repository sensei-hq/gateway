---
title: Model Lockout
doctype: feature
module: routing
status: implemented
phase: 1
spec: SP-0
source: crates/gateway/src/selection.rs, crates/gateway/src/circuit_breaker.rs
---

# Model Lockout

> **Status: Implemented (Phase 1 · SP-0 — complete).** The lockout mechanism (keyed
> `router:model` skip gate), limit-signal classification (429 →
> `rate_limit`, 403/quota → `quota_exhausted`, credits → terminal, auth →
> terminal), escalating backoff, the `on_lockout` callback, and
> `apply_lockout` / `clear_lockout` re-seed are live; the terminal
> `AllGated { resume_after }` it feeds landed in **(e)** (see
> [quota demote-to-tier](quota-demote-to-tier.md)). **(f)** landed the
> operator-tunable `ResilienceConfig` (`Gateway::with_resilience`), a bounded
> eviction cap on the lockout map, and deterministic per-endpoint jitter —
> completing SP-0 (health gates). Still deferred (post-SP-0 / SP-DATA): the
> calendar-clock exact reset boundary (the fixed ~1h quota default is a
> self-correcting approximation) and an opaque `EndpointKey` (the `router:model`
> string key is used throughout). Design in [`../../superpowers/specs/2026-08-06-sensei-orchestrator-design.md`](../../superpowers/specs/2026-08-06-sensei-orchestrator-design.md) §12/§12.1. Reference implementation: OmniRoute `accountFallback`.

A selection gate that temporarily removes a **single model** from a chain after a
limit signal, so the candidate walk falls over to the next entry instead of
retrying a model that cannot currently succeed. Sits alongside the existing
[circuit breaker](circuit-breaker.md) (endpoint-level) and
[connection cooldown](connection-cooldown.md) (router-level) in the
`validate_chain_entry` pipeline.

## Behavior

- **Keyed** per `router:model` — a `ModelLockoutEntry { reason, failure_count, last_cooldown_ms, locked_until, escalation_count }`. The gateway is **tenant-agnostic** (no tenant/credential dimension); per-tenant isolation = one gateway entity per tenant (see the SP-0 design §5c). Durability is the caller's: the gateway fires an `on_lockout` callback and the caller persists it.
- **Cooldown by reason**, not one fixed value:
  - `rate_limit` (429) → short (~60s); recovers fast.
  - `quota_exhausted` (403 / quota body) → a longer window. _Target:_ the next reset boundary (tomorrow 00:00 / monthly reset). _Implemented:_ a fixed `quota_default` (~1h) — the exact calendar reset boundary is deferred to SP-0 (f)+/SP-DATA. The fixed default is self-correcting: a consumer that retries while still quota'd simply re-locks for another `quota_default` and never over-serves.
  - `credits_exhausted` → terminal until a human tops up (surface as a human-action hint, do not auto-retry).
  - `auth` / `expired` → locked until the credential changes.
- **Escalating backoff** whose window outlives the cooldown: a model that fails again right after its lockout expires keeps escalating instead of resetting to base. Clamp to an operator `max_cooldown_ms` — **except** honor a real upstream reset hint exactly (never clamp a genuine "Resets in 92h" down to the cap).
- **Classification** of limit signals: 429→`rate_limit`, 403/quota-body→`quota_exhausted`, credits→terminal; includes text-pattern detection for providers that throttle via non-standard 400/403 bodies.
- **Bounded** map with an eviction cap (`eviction_cap`, default 4096) so lockout state cannot leak: over the cap, expired timed entries are evicted on write, while active and terminal locks are never dropped. _(Implemented in SP-0 (f).)_

## Interaction with the chain

A locked-out model becomes a `SkippedCandidate`; the walk continues to the next
entry. The run only surfaces a terminal error when **every** entry in the chain
is gated — and that terminal error carries `resume_after = min(expiry across
locked models)`, which lets a durable consumer (the orchestrator) pause and
resume at a concrete wake-up time rather than treating quota as fatal. This is
the mechanism behind [quota demote-to-tier](quota-demote-to-tier.md).

## Scenarios

```gherkin
Feature: Model lockout
  A model returning a limit signal is temporarily removed from the chain so the
  candidate walk falls over instead of retrying a model that cannot succeed.

  Scenario: Rate-limited model is locked out briefly; the chain falls over
    Given a chain [modelA, modelB]
    And modelA returns HTTP 429
    When the request is routed
    Then modelA is locked out with reason "rate_limit"
    And the request is served by modelB
    And modelA's lockout expires after the rate_limit cooldown (~60s)

  # Target behavior. The implementation locks for a fixed quota_default (~1h),
  # NOT a calendar-precise reset boundary (deferred to SP-0 (f)+/SP-DATA); the
  # fixed default is self-correcting — a still-quota'd retry simply re-locks.
  Scenario: Quota-exhausted model is locked until its reset window
    Given modelA returns HTTP 403 with a quota-exhausted body
    When the request is routed
    Then modelA is locked out with reason "quota_exhausted"
    And its locked_until is the next reset boundary, not a fixed 60s

  Scenario: Repeated failure escalates the cooldown
    Given modelA was just released from a lockout
    And modelA fails again immediately
    Then the new cooldown is longer than the previous one
    And the cooldown is clamped to max_cooldown_ms

  Scenario: A genuine upstream reset hint is honored exactly, not clamped
    Given modelA returns an upstream reset hint of 92 hours
    Then modelA's locked_until reflects ~92h
    And it is NOT clamped down to max_cooldown_ms

  Scenario: Credits exhausted is terminal, not a timed lockout
    Given modelA returns a credits-exhausted signal
    Then modelA is marked terminal-until-topped-up
    And it is surfaced as a human-action hint, not auto-retried
```

## Notes

- In-memory / per-process today (like the circuit breaker). A future seam can persist lockout state for multi-instance sharing.
- Complemented by proactive [expiration tracking](../governance/expiration-tracking.md), which skips a credential *before* it fails.
- **Provider-side, not subscription quota.** Lockout reacts to *upstream* limit signals (a model's 429/403/credits). The caller's own subscription `GatewayError::QuotaExceeded` (subject/tier — [governance/subscription-quota](../governance/subscription-quota.md)) is a separate **hard stop**, not a lockout, and must not fall over. Keep the two as distinct reason/error types.

### Scenarios — added from design review

```gherkin
Feature: Model lockout (additional)
  Scenario: A successful attempt clears prior lockout and escalation
    Given modelA was rate-limited and its lockout has since expired
    When modelA succeeds on the next request
    Then modelA's lockout entry and escalation count are cleared
    And a later single failure starts from the base cooldown, not an escalated one

  Scenario: Escalation grows on repeated post-release failure
    Given modelA failed, was released, and fails again immediately (max_cooldown high)
    Then the second cooldown window is strictly longer than the first

  Scenario: Escalation is clamped; an exact upstream reset is not
    Given modelA has escalated past max_cooldown_ms
    Then locked_until - now equals max_cooldown_ms exactly
    And a genuine upstream reset hint (Until::Exact) is honored verbatim, never clamped

  Scenario: A 403 quota after a 429 upgrades the lock
    Given modelA is locked "rate_limit" until now+60s
    And modelA then returns HTTP 403 with a quota body
    Then modelA's lock reason becomes "quota_exhausted" until the reset boundary

  Scenario: Auth failure locks the model terminally
    Given modelA returns HTTP 401
    Then modelA is locked with reason "auth" and until = None (terminal)
    And it surfaces a credential-action hint, not a wake-up time

  Scenario: Fixing the credential clears a terminal auth lock
    Given modelA is terminally auth-locked
    When refresh_router_keys installs a working key for its router
    Then modelA's auth lock is cleared and it is eligible again

  Scenario: The gateway announces a lockout; the caller persists it
    Given modelA hits a provider quota
    Then the gateway fires on_lockout(modelA, quota_exhausted, until = T)
    And the caller persists the lockout (the gateway itself never persists)

  Scenario: A caller re-seeds a persisted lockout on a fresh instance
    Given the caller persisted "modelA locked until T"
    When it starts a new gateway instance and calls apply_lockout(modelA, quota_exhausted, T)
    Then modelA is skipped until T on the new instance

  Scenario: The gateway core is tenant-agnostic
    Given two tenants each run their own gateway entity
    When tenant T1's gateway locks modelA
    Then tenant T2's gateway is unaffected (no shared state; no tenant concept in core)
```
