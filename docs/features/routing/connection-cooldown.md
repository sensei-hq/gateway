---
title: Connection Cooldown
doctype: feature
module: routing
status: planned
phase: 1
spec: SP-0
source: crates/gateway/src/selection.rs
---

# Connection Cooldown

> **Status: Planned (Phase 1 · SP-0).** Design in [`../../superpowers/specs/2026-08-06-sensei-orchestrator-design.md`](../../superpowers/specs/2026-08-06-sensei-orchestrator-design.md) §12.

A selection gate at **router / connection** granularity. On a connection-level
fault (network error, connect timeout — i.e. the provider endpoint itself is
unreachable), the whole router is put on a short backoff so the candidate walk
skips **all** of that router's models at once, instead of hammering each model
of a down provider and tripping its per-endpoint [circuit breaker](circuit-breaker.md)
separately.

## Behavior

- **Granularity:** router/connection (coarser than the endpoint-level circuit breaker, coarser than per-model [lockout](model-lockout.md)).
- **Trigger:** connection-level faults (`Network`, connect timeout).
- **Duration:** a backoff window (jittered; may escalate), distinct from the breaker's failure-count threshold.
- **Effect:** every `ChainEntry` whose router is cooling down becomes a `SkippedCandidate`; the walk falls over to entries on other routers.

## Why separate from the circuit breaker

The circuit breaker opens per `router:model` after N consecutive failures — so a
provider outage would otherwise have to trip the breaker independently for opus,
sonnet, haiku, … Connection cooldown short-circuits that: one connection fault
cools the whole router once.

## Scenarios

```gherkin
Feature: Connection cooldown
  A connection-level fault cools the whole router so the walk skips all of its
  models at once instead of hammering each one.

  Scenario: A network fault cools the whole router
    Given router R hosts [modelA, modelB] and router S hosts [modelC]
    And a request to modelA fails with a network/connect error
    When the request is routed
    Then router R is put on connection cooldown
    And both modelA and modelB are skipped as candidates
    And the request is served by modelC on router S

  Scenario: Cooldown expires and the router becomes eligible again
    Given router R is on connection cooldown
    When the cooldown window elapses
    Then router R's models are eligible candidates again
```

## Notes

- In-memory / per-process today. Future seam for multi-instance sharing (shared with [model lockout](model-lockout.md)).
- Backoff uses decorrelated jitter to avoid synchronized retry storms across a fan-out.
