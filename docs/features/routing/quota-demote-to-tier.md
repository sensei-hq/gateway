---
title: Quota Demote-to-Tier
doctype: feature
module: routing
status: implemented
phase: 1
spec: SP-0
source: crates/gateway/src/engine.rs, crates/kernel/src/types/error.rs
---

# Quota Demote-to-Tier

> **Status: Implemented (Phase 1 · SP-0 — complete).** Demote-to-tier, in-flight
> §3.1 recoverable fallover, and the terminal `AllGated { resume_after,
> human_action }` (both `execute` and `execute_stream`) are live. **(f)** landed
> the operator-tunable `ResilienceConfig` (`Gateway::with_resilience`), bounded
> eviction of lockout/cooldown state, and deterministic per-endpoint jitter —
> completing SP-0 (health gates). Still deferred (post-SP-0 / SP-DATA): the
> calendar-clock exact reset boundary (the base quota window is a self-correcting
> approximation) and an opaque `EndpointKey`. Design in
> [`../../superpowers/specs/2026-08-06-sensei-orchestrator-design.md`](../../superpowers/specs/2026-08-06-sensei-orchestrator-design.md) §11.2/§12.

Changes how a **provider-side** quota/limit on a specific model behaves in the
candidate walk. Today an upstream quota/rate-limit for a model surfaces as a
terminal error with no fallover. With [model lockout](model-lockout.md), that
provider limit instead **locks out that model and demotes to the next
entry/tier**, so the chain keeps trying other tiers before giving up.

> **Not the same as subscription quota.** The caller's per-subject/tier
> `GatewayError::QuotaExceeded { unit, window, limit, used }` (subscription
> metering — [governance/subscription-quota](../governance/subscription-quota.md),
> design note `docs/design/subscription-quota-auth.md`) is a **hard stop that
> correctly does NOT demote** — no other model escapes the caller's own quota.
> Demote-to-tier applies only to *provider-side* limits (distinct reason/error
> types; do not conflate).

## Behavior

- On `quota_exhausted` for a model: lock it out (until its reset boundary — currently a base window; the exact calendar-clock boundary is deferred to SP-0 (f)) and continue the walk — do **not** terminate the request.
- The request only fails when **every** entry in the chain is gated (locked out / cooling down / breaker-open / over-budget).
- The terminal error then carries `resume_after = min(expiry across gated entries)` — a concrete wake-up time.

## Why it matters

- A single free-tier model hitting its daily cap no longer kills a run; the chain falls to the next tier (e.g. `free → cost-optimized → fallback-specialty`).
- The `resume_after` signal is exactly what a durable consumer (the orchestrator) needs to **pause and resume** at the right time rather than treating quota as fatal (design §11.2 — `QuotaExceeded` → durable pause).

## Scenarios

```gherkin
Feature: Quota demote-to-tier
  Quota exhaustion demotes to the next tier instead of terminating the request.

  Scenario: Quota on a free-tier model falls over to the next tier
    Given a chain [free-tier model, cost-optimized model]
    And the free-tier model is quota_exhausted
    When the request is routed
    Then the free-tier model is skipped
    And the request is served by the cost-optimized model

  Scenario: All tiers gated returns a terminal error carrying resume_after
    Given every model in the chain is locked out or cooling down
    When the request is routed
    Then the gateway returns a terminal error
    And the error carries resume_after = min(expiry across gated models)

  Scenario: A durable consumer pauses and resumes at resume_after
    Given a terminal error with resume_after = T
    Then the orchestrator records a durable pause with wake-up time T
    And it does not treat quota exhaustion as a fatal failure
```

## Notes

- Distinguish `quota_exhausted` (resets on a window → demote + `resume_after`) from `credits_exhausted` (terminal until top-up → human-action hint). See [model lockout](model-lockout.md) classification.
- Depends on the tier model in [catalog/tiers-and-chains](../catalog/tiers-and-chains.md) for "next tier" semantics.
- **Precedence with the subscription hard-stop.** The subscription `GatewayError::QuotaExceeded` (subject hard-stop, raised by `check_quota` before the candidate walk) is distinct from provider-side limits. When a subject is over-quota AND its providers are also all-gated at selection, the all-gated path currently surfaces first — the empty-candidate `AllGated` check runs before `check_quota`. Both mean "cannot serve now", so this is a deliberate precedence, to revisit if the subject hard-stop should always win.

### Scenarios — added from design review

```gherkin
Feature: Quota demote-to-tier (additional)
  Scenario: Subscription quota exhaustion does NOT demote
    Given a chain [modelA, modelB]
    And the caller's subscription quota is exhausted (GatewayError::QuotaExceeded)
    When the request is routed
    Then the gateway returns QuotaExceeded
    And modelB is NOT attempted and no model is locked out

  Scenario: A provider 403 quota falls over on the SAME request
    Given a chain [free model, cost-optimized model]
    And the free model returns HTTP 403 with a quota body
    When the request is routed
    Then the free model's limit is classified recoverable and the walk falls over
    And the cost-optimized model serves the request on this same request

  Scenario: Mixed terminal + timed exhaustion
    Given modelA is credits_exhausted (terminal) and modelB is rate_limited until T
    When every candidate is gated
    Then AllGated.resume_after = T (min over timed only)
    And a human-action hint is surfaced for modelA
    And resume_after ignores the terminal model

  Scenario: All candidates terminal → the indefinite HOTL pause (M1 reversed 2026-09-04)
    Given every candidate is credits_exhausted or auth-locked (until = None)
    When every candidate is gated
    Then AllGated.resume_after is None
    And a human_action names the remedy
    And the orchestrator pauses the run INDEFINITELY rather than failing it
    And nothing wakes it on a timer — only an operator's force_wake, after acting

  Scenario: All candidates gated with no remedy at all → fail fast
    Given every candidate is gated with neither a timed until nor a human_action
    When every candidate is gated
    Then the run FAILS — a pause nobody can clear is worse than a failure

  Scenario: All candidates circuit-open → resume_after from breaker next_retry
    Given every endpoint's breaker is Open with next_retry = T
    Then AllGated.resume_after = min(next_retry)
```
