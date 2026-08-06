---
title: Model Lockout
doctype: feature
module: routing
status: planned
phase: 1
spec: SP-0
source: crates/gateway/src/selection.rs, crates/gateway/src/circuit_breaker.rs
---

# Model Lockout

> **Status: Planned (Phase 1 · SP-0).** Design in [`../../superpowers/specs/2026-08-06-sensei-orchestrator-design.md`](../../superpowers/specs/2026-08-06-sensei-orchestrator-design.md) §12/§12.1. Reference implementation: OmniRoute `accountFallback`.

A selection gate that temporarily removes a **single model** from a chain after a
limit signal, so the candidate walk falls over to the next entry instead of
retrying a model that cannot currently succeed. Sits alongside the existing
[circuit breaker](circuit-breaker.md) (endpoint-level) and
[connection cooldown](connection-cooldown.md) (router-level) in the
`validate_chain_entry` pipeline.

## Behavior

- **Keyed** per `router:model:credential` — a `ModelLockoutEntry { reason, failure_count, last_cooldown_ms, locked_until, escalation_count }`.
- **Cooldown by reason**, not one fixed value:
  - `rate_limit` (429) → short (~60s); recovers fast.
  - `quota_exhausted` (403 / quota body) → until the next reset boundary (tomorrow 00:00 / monthly reset), else ~1h.
  - `credits_exhausted` → terminal until a human tops up (surface as a human-action hint, do not auto-retry).
  - `auth` / `expired` → locked until the credential changes.
- **Escalating backoff** whose window outlives the cooldown: a model that fails again right after its lockout expires keeps escalating instead of resetting to base. Clamp to an operator `max_cooldown_ms` — **except** honor a real upstream reset hint exactly (never clamp a genuine "Resets in 92h" down to the cap).
- **Classification** of limit signals: 429→`rate_limit`, 403/quota-body→`quota_exhausted`, credits→terminal; includes text-pattern detection for providers that throttle via non-standard 400/403 bodies.
- **Bounded** map with an eviction cap so lockout state cannot leak.

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
