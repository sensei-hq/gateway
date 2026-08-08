---
title: Durable Executor (spine)
doctype: feature
module: orchestrator
status: partial
phase: 3
spec: SP-1
source: crates/orchestrator*
---

# Durable Executor (spine)

> **Status: Partial (Phase 3 · SP-1, slice 1).** The durable-execution *spine*:
> a deterministic executor over a durable journal that **resumes without
> re-spending tokens**. Plan
> [`../../superpowers/plans/2026-08-08-sp1-orchestrator-spine.md`](../../superpowers/plans/2026-08-08-sp1-orchestrator-spine.md);
> design [`../../superpowers/specs/2026-08-08-sp1-orchestrator-spine-design.md`](../../superpowers/specs/2026-08-08-sp1-orchestrator-spine-design.md).
> This is slice 1 of SP-1 — a linear `ModelCall` graph of **Pure** effects. The
> agent runtime, fan-out/quorum, non-pure effects, and persistence beyond the
> in-memory journal are [deferred](#deferred).

The executor drives a graph of nodes, each a call to the real
[gateway](../routing/README.md), and journals every step so a crashed run can
resume from where it stopped — replaying already-recorded effects from the
journal instead of re-issuing (and re-paying for) them.

## The effect-class model

Every nondeterministic or expensive operation is an **effect**, classed by
idempotency. Slice 1 implements only the first class; the other two are typed
and reserved so the journal format is forward-compatible.

| Class | Meaning | Slice-1 status |
|---|---|---|
| **Pure** | Deterministic given its input; memoize forever (e.g. a model call). | **Live** — the only class the executor records/replays. |
| **Observation** | A read whose value can drift; memoize with TTL + provenance. | Typed only; [deferred](#deferred) to slice 4. |
| **Mutation** | An external write; two-phase (intent → record) + idempotency key + reconcile. | Typed only; [deferred](#deferred) to slice 4. |

## Journal, effect id, and memoization

- **`ExecutionJournal`** is an append-only log of `JournalEvent`s per `RunId`
  (`RunStarted` · `NodeStarted` · `EffectRecorded` · `NodeCompleted` ·
  `NodeFailed` · `RunCompleted`). It is the seam a `PostgresJournal` implements
  later; slice 1 ships only the in-memory `InMemoryJournal`.
- **Structural `effect_id`** = `sha256_hex("{parent_path}|{loop_iteration}|{local_index}")`.
  It is derived from a node's *position*, not its content, so the same node
  across a crash/resume maps to the same recorded effect. (Loop iterations get
  distinct ids via `loop_iteration` — reserved for a later slice.)
- **Input-hash memoization** — each effect also records
  `input_hash = sha256_hex("{chain}|{json(payload)}")`. On resume, a node whose
  `effect_id` is in the folded memo is replayed **only if** its recomputed
  input hash matches the recorded one; a mismatch is a determinism violation and
  **halts** (never a silent re-run or re-memoize).
- **Version fence** — `RunStarted` records the executor `version`. A resume by
  an executor of a different version is refused (`VersionFenceMismatch`) rather
  than folding a journal it may misinterpret.

## Resume / fold

`Executor::run` starts a fresh run (`RunStarted` + drive every node with an
empty memo). `Executor::start` is the resume entry point: it loads the journal
and

- **empty** ⇒ delegates to `run` (a fresh start);
- **version mismatch** ⇒ refuses with `VersionFenceMismatch`;
- **already terminal** (`RunCompleted` present) ⇒ returns the folded outcome
  without re-driving (no second `RunCompleted`);
- **partial** ⇒ folds every `EffectRecorded` into a memo keyed by `effect_id`,
  then drives the tail — replaying the completed prefix with **no gateway call
  and no duplicate journal events**, and appending `RunCompleted` once.

Journal `append` errors are **strict**: a backend write failure aborts the run
loudly as `OrchestratorError::Journal`, never swallowed. Node failures are both
journaled (`NodeFailed`) and surfaced in `RunOutcome.failed`, halting the run.

## Crate layout

A three-crate split mirroring the gateway's `kernel → engine → store`:

| Crate | Lib | Role |
|---|---|---|
| `sensei-orchestrator-core` | `orchestrator_core` | Zero-I/O types: `Graph`/`Node`/`NodeKind`, `EffectClass`/`effect_id`, `JournalEvent`/`ExecutionJournal`, errors. |
| `sensei-orchestrator` | `orchestrator` | The `Executor` (`run`/`start`/`drive`); links `sensei-gateway`. |
| `sensei-orchestrator-store` | `orchestrator_store` | `InMemoryJournal` (Arc-shared `ExecutionJournal`). |

## Gateway boundary (§9.1)

The orchestrator holds an `Arc<gateway::Gateway>` and consumes it through one
seam: each `ModelCall { chain, payload }` compiles into a plain
`InferenceRequest` (`TextChat` over the named chain, `allow_fallback: true`) and
runs via `Gateway::execute`. The gateway, kernel, and catalog crates are
untouched — the executor is additive. Chain expansion and SP-0 fallover are the
gateway's job; the executor records whichever candidate the gateway served.

## Scenarios

```gherkin
Feature: Durable executor (spine)

  Scenario: Resume without re-spending tokens
    Given a run whose first ModelCall (pure) is journaled as completed
    And whose second node failed mid-run before RunCompleted
    When a fresh executor resumes the run on the same journal
    Then the first node is memoized from the journal, not re-called
    And the gateway is invoked only for the tail node
    And the run finishes with a single RunCompleted

  Scenario: Determinism violation halts the resume
    Given a journal with node n1 recorded for one payload
    When the run resumes with n1's payload changed (input hash differs)
    Then it halts with a DeterminismViolation for n1
    And the gateway is never called

  Scenario: Version fence refuses the resume
    Given a journal whose RunStarted recorded version "v1"
    When an executor of version "v2" tries to resume it
    Then it refuses with VersionFenceMismatch { recorded: "v1", current: "v2" }
    And the gateway is never called

  Scenario: Strict journal fails loud
    Given an ExecutionJournal whose append always errors
    When a run is started
    Then the run aborts with OrchestratorError::Journal
    And no gateway call is made (the error is surfaced, not swallowed)

  Scenario: Reference chain drives end-to-end to the local model
    Given the assembled demo catalog and a local adapter for the "ollama" router only
    And a one-node graph whose ModelCall targets the reference chain "research.bulk"
    When the executor runs it
    Then the chain falls over groq-llama-free and deepseek-chat (no adapter)
    And llama3.1-local serves the call
    And the run completes with the node's output model recorded as "llama3.1-local"
```

## Deferred

Held off to later SP-1 slices (and beyond); slice 1 ships none of these:

- **Slice 2** — the agent/skill/tool registry (md + frontmatter) and the
  prompt-assembly runtime (`AgentInvocation → InferenceRequest` compilation).
- **Slice 3** — `Map` fan-out, quorum, and the CAS blackboard / shared context.
- **Slice 4** — the **Observation** and **Mutation** effect classes, two-phase
  intent→record, and the crash-in-doubt reconcile path.
- **Later** — planner / loops, streaming, hooks (best-effort progress), and a
  `PostgresJournal`. There is **no persistence beyond the in-memory journal** in
  this slice; `ExecutionJournal` is the seam a durable store implements later.
