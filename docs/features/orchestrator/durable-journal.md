---
title: Durable Journal
doctype: feature
module: orchestrator
status: planned
phase: 3
spec: SP-1
source: orchestrator-core (new)
---

# Durable Journal

> **Status: Planned (Phase 3 · SP-1).** Design §7.

A step-journal that makes a run resumable. Every nondeterministic/expensive op
is an **effect** classed by idempotency — **pure** (memoize forever),
**observation** (memoize + TTL + provenance), **mutation** (two-phase +
idempotency key + reconcile). Resume folds the journal, memoizes completed
effects (no token re-spend), and continues from the first incomplete node.

## Scenarios

```gherkin
Feature: Durable journal
  Scenario: Resume does not re-spend tokens on a pure effect
    Given a run whose first model call (pure) is journaled as completed
    When the process crashes and resumes
    Then the model call is memoized (not re-issued) and its output is reused

  Scenario: A mutation crashed between intent and record is reconciled
    Given a mutation effect with an EffectIntent but no EffectRecorded
    When the run resumes
    Then it neither blindly re-runs nor memoizes; it runs the reconcile path

  Scenario: Loop iterations get distinct effect ids
    Given a loop body re-entered for iteration 2
    Then iteration 2's effects do not memoize iteration 1's recorded outputs

  Scenario: Input-hash divergence halts loudly
    Given config changed so an effect's recomputed input hash differs
    Then resume halts with a determinism-violation (no silent memoize)

  Scenario: Quota exhaustion pauses with a wake-up time
    Given the gateway returns terminal quota with resume_after = T
    Then the run records a durable pause and resumes at T
```

## Notes

- Journal (correctness) is strict; hooks (observability) are best-effort — see [hooks](hooks.md).
- Large payloads live in a content-addressed store, not the control-flow log (design §7.4).
