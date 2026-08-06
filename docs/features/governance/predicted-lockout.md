---
title: Predicted Lockout
doctype: feature
module: governance
status: planned
phase: 4
spec: SP-DATA
source: data-tier + crates/gateway/src/selection.rs
---

# Predicted Lockout

> **Status: Planned (Phase 4 · SP-DATA).** Needs live [usage metering](usage-metering.md).

Pre-emptively lock out a model when metering shows its remaining quota is
effectively exhausted — before the provider returns a 429 — so a fan-out doesn't
waste a round-trip (and a rate-limit storm) discovering the limit.

## Scenarios

```gherkin
Feature: Predicted lockout
  Scenario: A model at its quota cap is skipped without a call
    Given metering shows model f1's daily pool at 100% used
    When a request would route to f1
    Then f1 is a SkippedCandidate (no upstream call is made)
    And the chain falls over to the next tier

  Scenario: Headroom returns after the reset window
    Given f1 was predicted-locked at its cap
    When the pool's reset boundary passes
    Then f1 is eligible again
```

## Notes

- Complements reactive [model lockout](../routing/model-lockout.md): reactive handles the 429 you didn't predict; predicted avoids the ones you can.
