---
title: Metering Store
doctype: feature
module: data-tier
status: planned
phase: 4
spec: SP-DATA
source: torii metering schema (extracted)
---

# Metering Store

> **Status: Planned (Phase 4 · SP-DATA).** Extract of torii's `metering` schema.

The durable store of usage counters (per key / model / pool) with reset windows,
plus daily rollups. It backs [governance/usage-metering](../governance/usage-metering.md)
and feeds [predicted lockout](../governance/predicted-lockout.md) and the
`headroom` routing strategy.

## Scenarios

```gherkin
Feature: Metering store
  Scenario: Usage is recorded through the DB-agnostic seam
    Given a call consumes tokens
    Then a usage row is written via the store trait (backend-swappable)

  Scenario: A daily rollup aggregates raw usage
    Given raw usage across a day
    When the rollup runs
    Then usage_daily reflects the aggregated totals per model/pool

  Scenario: Store failure does not break inference
    Given the metering store write fails
    Then the request still succeeds and the failure is logged (best-effort)
```

## Notes

- Metering is best-effort like `GatewayStore` (losing a counter is acceptable); the orchestrator's durable journal is strict — different masters, different guarantees.
