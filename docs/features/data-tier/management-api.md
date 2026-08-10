---
title: Management API
doctype: feature
module: data-tier
status: planned
phase: 4
spec: SP-DATA
source: new (CLI + API)
---

# Management API

> **Status: Planned (Phase 4 · SP-DATA).** Modeled on strategos's CLI + Hono API.

A CLI + API to configure the catalog (models / chains / flows / config) and to
observe tracking state (usage, lockouts, expirations, free-tier budget).

## Scenarios

```gherkin
Feature: Management API
  Scenario: Add a model to a tier via the CLI
    Given the CLI adds model f3 to the free tier
    Then f3 appears in every chain referencing the free tier

  Scenario: Observe current lockouts
    Given some models are locked out
    When the operator queries availability
    Then each lockout's model, reason, and remaining time are returned

  Scenario: Read the free-tier budget
    When the operator queries the free-tier summary
    Then pool-deduped steady tokens and per-pool headroom are returned
```

## Notes

- Read paths surface [metering store](metering-store.md) + [routing](../routing/README.md) gate state; write paths update the [catalog control-plane](catalog-control-plane.md).
