---
title: Usage Metering
doctype: feature
module: governance
status: planned
phase: 4
spec: SP-DATA
source: data-tier (metering store)
---

# Usage Metering

> **Status: Planned (Phase 4 · SP-DATA).** Data-tier over the DB-agnostic seam.

Live counters of consumption against free-tier and paid limits, with reset
windows, at per-key / per-model / per-pool granularity. Feeds free-tier-aware
routing (`headroom`) and [predicted lockout](predicted-lockout.md).

## Scenarios

```gherkin
Feature: Usage metering
  Scenario: A successful call increments the pool counter
    Given model f1 belongs to pool "gemini-flash"
    When a call to f1 consumes 1,000 tokens
    Then the pool's used counter increases by 1,000

  Scenario: Counters reset at the window boundary
    Given a daily free tier used to 90%
    When the reset boundary (00:00) passes
    Then the used counter is 0 and full headroom is restored

  Scenario: Headroom routing prefers the model with the most remaining quota
    Given two free models with 10% and 80% headroom
    When the tier's strategy is headroom
    Then the 80%-headroom model is tried first
```

## Notes

- Stateful → lives in the data-tier, not the pure gateway core.
- Reset windows come from the [free-tier catalog](../catalog/free-tier-catalog.md) and paid budgets.
