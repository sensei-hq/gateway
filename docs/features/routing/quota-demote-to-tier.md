---
title: Quota Demote-to-Tier
doctype: feature
module: routing
status: planned
phase: 1
spec: SP-0
source: crates/gateway/src/engine.rs, crates/kernel/src/types/error.rs
---

# Quota Demote-to-Tier

> **Status: Planned (Phase 1 · SP-0).** Design in [`../../superpowers/specs/2026-08-06-sensei-orchestrator-design.md`](../../superpowers/specs/2026-08-06-sensei-orchestrator-design.md) §11.2/§12.

Changes how a quota-exhausted model behaves in the candidate walk. Today
`QuotaExceeded` is **terminal** — it never falls over (see
[fallback-chains](fallback-chains.md)). With [model lockout](model-lockout.md),
a quota hit instead **locks out that model and demotes to the next entry/tier**,
so the chain keeps trying cheaper/other tiers before giving up.

## Behavior

- On `quota_exhausted` for a model: lock it out (until its reset boundary) and continue the walk — do **not** terminate the request.
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
