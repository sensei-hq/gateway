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
> in-memory journal are [deferred](#deferred). **Slice 2:** `NodeKind::Agent`
> now rides this same spine — each ReAct turn is a Pure effect with an
> iteration-aware `effect_id`, so resume/memoization extends into the agent
> loop (see [agents-skills-tools](agents-skills-tools.md)). **Slice 3:** `Map`
> fan-out, `Quorum`, `Consolidate`, snapshots, and CAS split-on-output.
> **Slice 4:** all three effect classes are now live — **Observation** (TTL
> memoize / re-read via an injected `Clock`) and **Mutation** (two-phase
> `EffectIntent → EffectRecorded` + in-doubt reconcile → durable `RunPaused`);
> see the [effect-class table](#the-effect-class-model). Real reconcile providers,
> a sandbox/permission model, and a `PostgresJournal` remain [deferred](#deferred).

The executor drives a graph of nodes, each a call to the real
[gateway](../routing/README.md), and journals every step so a crashed run can
resume from where it stopped — replaying already-recorded effects from the
journal instead of re-issuing (and re-paying for) them.

## The effect-class model

Every nondeterministic or expensive operation is an **effect**, classed by
idempotency. As of slice 4 all three classes are live; the executor dispatches a
tool call on its `ToolSpec.effect_class`.

| Class | Meaning | Status |
|---|---|---|
| **Pure** | Deterministic given its input; memoize forever (e.g. a model call). | **Live** (slice 1) — memoize-forever; replayed on resume. |
| **Observation** | A read whose value can drift; memoize with TTL + provenance. | **Live** (slice 4) — a memo hit replays while fresh (`fetched_at + ttl_secs` per the injected `Clock`), else re-reads and records a superseding `EffectRecorded` with fresh `ObservationMeta{fetched_at, ttl_secs, source}`. *Note:* a stale re-read that returns a **different** value inside a multi-turn agent changes a later already-memoized turn's transcript, so that turn's input-hash mismatches and the resume halts loud with `DeterminismViolation` — safe (never a silent proceed), but a genuinely drifting Observation feeding later turns is a sharp edge to revisit if it bites. |
| **Mutation** | An external write; two-phase (intent → record) + idempotency key + reconcile. | **Live** (slice 4) — journals `EffectIntent{idempotency_key, args_hash}` **before** the side effect, then `EffectRecorded`. On resume an Intent without a Recorded is **in-doubt** → a per-tool `ReconcileProvider` decides: `Confirmed`⇒record (don't re-run), `NotApplied`⇒run once under the standing Intent, `Indeterminate`/absent⇒durable `RunPaused` (never guess). |

**Secret redaction (SP-4 s2).** Effect **outputs** (tool results, model-turn text, and the
reconcile-`Confirmed` output) are scrubbed of secrets by an opt-in injected `Redactor`
(`Executor::with_redactor`; default `PatternRedactor` → `[REDACTED]`) **at production, before
both journaling and the agent/downstream-return** — so durable plaintext credentials never land
in the journal/CAS and the model never sees a secret (anti-exfiltration). The redactor is pure ⇒
`live == journaled == replayed`; default-off ⇒ byte-identical. Best-effort by shape.

**Exactly-once / idempotency keys (SP-4 s5).** A Mutation's `idempotency_key` (author-supplied via
`Tool::idempotency_key(args)`, else the structural `sha256(effect_id|args_hash)`) is journaled in the
`EffectIntent` AND threaded to the tool via `call_ctx(args, &ToolContext{idempotency_key, effect_id})`,
so a tool can send **the same key it journaled** to an external API for provider-side dedup. On an
in-doubt resume, `reconcile_in_doubt` reads the **journaled** key from the fold (not a recompute) and
queries the `ReconcileProvider` by it — so the side effect applies **exactly once** across a crash
(`Confirmed`⇒record without re-running; `NotApplied`⇒run once under the standing Intent; absent
provider⇒`RunPaused`, R3). Default tools use the structural key ⇒ byte-identical to before.

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

## Gateway error → pause vs fail (§11.2)

When `Gateway::execute` returns a terminal error, `classify_gateway_error` decides:
a **fully-gated chain with a timed re-eligibility** (`GatewayError::AllGated
{ resume_after: Some(t) }` — every candidate health-locked/cooling/breaker-open/
over-budget, with a min wall-clock retry) becomes a **durable pause**: journal
`RunPaused { reason, resume_after: Some(t) }`, set `RunOutcome.paused`, suppress
`RunCompleted` — the run stays resumable. A gated call journals **no**
`EffectRecorded`, so a **resume simply re-attempts** the node (the quota window may
have reset → it succeeds and records; still gated → it pauses again) — no memo, no
determinism fence. `AllGated { resume_after: None }` (all gates terminal) and every
other gateway error **fail-fast** (the `Display` carries the human-action hint:
top-up credits / rotate credential / raise budget) — never a pause-forever. Wired
at the top-level `ModelCall` node and every agent turn (`dispatch_model_turn`);
agent children of a `Map`/`Loop` pause the whole Map/Loop via `MapChildPaused`.
**Deferred:** ModelCall *bodies* inside `Map`/`Consolidate`/`Loop` pausing on a
gate (they fail); `RateLimit`→journaled-`Timer` backoff. (~~the durable scheduler
that re-arms at `resume_after`~~ **shipped in SP-DATA-3** — `Scheduler` over a
`scheduled_runs` `SchedulerStore` wakes a paused run at its deadline in any
process, exactly-once.)

## Deferred

Held off to later SP-1 slices (and beyond); slice 1 ships none of these:

- **Slice 2** — the agent/skill/tool registry (md + frontmatter) and the
  prompt-assembly runtime (`AgentInvocation → InferenceRequest` compilation).
- ~~**Slice 3** — `Map` fan-out, quorum, and the CAS blackboard / shared context.~~ **Done.**
- ~~**Slice 4** — the **Observation** and **Mutation** effect classes, two-phase
  intent→record, and the crash-in-doubt reconcile path.~~ **Done** (see the
  [effect-class table](#the-effect-class-model)). Still deferred within slice 4:
  - **SP-4** — real reconcile providers (query-by-idempotency-key against real
    services), author-supplied idempotency keys, saga/compensation, and the tool
    permission model + sandbox + workspace isolation. Slice 4 ships demo tools
    (`Search`/`RecordNote`) and a sink-backed test reconciler only.
  - ~~**SP-6** — `AwaitSignal`/`HumanGate` + signal delivery + pause-expiry; slice 4
    emits a durable `RunPaused` resolved out-of-band, with no re-arm mechanism yet.~~
    **Done — all four SP-6 slices.** `AwaitSignal` (s1), `HumanGate` (s2),
    human-as-`Agent` (s3) and `GateSpec::Human`, the human LOOP GATE (s4), are the
    four waiting kinds; `torii run signal` / `run gate decide` / `run agent answer`
    are the three delivery verbs; SP-DATA-3's scheduler is the re-arm mechanism, and
    each waiting kind journals its own ABSOLUTE deadline so an expiry cannot be
    pushed forward by a resume. See
    [execution-graph](execution-graph.md) and [durable-journal](durable-journal.md).
- ~~`Loop` (loops of graphs)~~ **Done** (deterministic gate + `max_iters`; see
  [execution-graph](execution-graph.md)). ~~`OrchestratorHooks`~~ **Done** (see
  [hooks](hooks.md)). ~~quota→pause~~ **Done** (see [gateway-error mapping](#gateway-error--pause-vs-fail-112)).
  This **completes the SP-1 walking skeleton**.
- **Later** — planner, runtime `PlanDelta`/`Subgraph`/`Branch`, streaming, and a
  `PostgresJournal`. There is **no persistence beyond the in-memory journal** yet;
  `ExecutionJournal` is the seam a durable store implements later. The two-phase
  `EffectIntent` fsync is in-memory here (**SP-DATA**).
