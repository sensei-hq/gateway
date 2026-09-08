---
title: SP-1 quota→pause — AllGated → durable RunPaused
doctype: design
module: orchestrator
spec: SP-1
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§11.2 gateway→executor mapping)
date: 2026-08-11
---

# SP-1 quota→pause — map `AllGated` to a durable `RunPaused` (walking skeleton)

## 1. Goal

Wire the gateway's terminal chain-gated result into the orchestrator's durable
pause: a run whose model chain is fully gated (quota/health-exhausted) **pauses**
(resumable) instead of failing. Design §11.2:
`GatewayError::AllGated { resume_after: Some(t) }` → **durable pause** to `t`;
`resume_after: None` (all gates terminal) → **fail-fast** with the `human_action`
hint (never pause forever — *reversed 2026-09-04: a named `human_action` now pauses indefinitely as the HOTL class; see M1 in `docs/design/selection-policy-pipeline.md`*). This is the last SP-1 walking-skeleton gap.

## 2. Background

- `GatewayError::AllGated { resume_after: Option<DateTime<Utc>>, skipped, human_action }`
  is what `Gateway::execute` returns when every candidate was skipped by a health
  gate (cooldown / breaker / lockout / budget) — `resume_after: Some` = the min
  wall-clock re-eligibility across timed gates; `None` = all gates terminal
  (`human_action` carries the remedy: `TopUpCredits`/`RotateCredential`/`RaiseBudget`).
- The pause machinery **already exists** (slices 4 + hooks): `NodeExec::Paused`,
  `AgentStep::Paused`, `ToolOutcome::Paused`, `JournalEvent::RunPaused { reason,
  resume_after }` (already carries `resume_after`), `RunOutcome.paused: Option<PauseInfo>`
  (suppresses `RunCompleted`), and the `on_run_paused` hook (fires from `append`).
  This slice mostly **routes** `AllGated` into those channels.
- The executor calls `self.gateway.execute(&request)` and today maps ANY `Err`
  to `NodeFailed`/`NodeExec::Failed` (or an inner `Err(String)`).

## 3. Design

### 3.1 The classifier (pure policy)

```rust
enum GatewayDisposition {
    Pause { resume_after: chrono::DateTime<chrono::Utc>, reason: String },
    Fail(String),
}

fn classify_gateway_error(err: &GatewayError) -> GatewayDisposition {
    match err {
        GatewayError::AllGated { resume_after: Some(t), .. } => GatewayDisposition::Pause {
            resume_after: *t,
            reason: format!("all candidates gated; resume after {t}"),
        },
        // AllGated{None} (terminal — human action) and every other gateway error
        // fail-fast; their Display already carries the reason/hint.
        other => GatewayDisposition::Fail(other.to_string()),
    }
}
```

**Only `AllGated { resume_after: Some(_) }` pauses.** `AllGated { None }` (all
gates terminal) fails with its human-action hint (its `Display` renders "…,
human action required"). `QuotaExceeded`, `BudgetExceeded`, `AllAttemptsFailed`,
etc. all fail as today. The whole policy is this one pure function.

### 3.2 Wiring — route `Pause` into the existing channels

- **`run_node` ModelCall node** (`mod.rs`): on a gateway `Err`, `classify`:
  - `Pause { resume_after, reason }` → journal `RunPaused { reason, resume_after: Some(t) }`
    → return `NodeExec::Paused { reason }` (→ `apply_node_result` sets
    `RunOutcome.paused`, suppresses `RunCompleted`).
  - `Fail(msg)` → today's `NodeFailed` + `NodeExec::Failed`.
- **Agent turns** (`dispatch_model_turn`, `agent.rs`): widen its inner return to
  the existing `ToolOutcome<Value>`. On a gateway `Err`, `classify`:
  - `Pause` → journal `RunPaused` → `ToolOutcome::Paused(reason)`.
  - `Fail(msg)` → today's `NodeFailed` → `ToolOutcome::Failed(msg)`.
  Thread the `ToolOutcome` up through `agent_turn_output → drive_agent`, which maps
  `Paused → AgentStep::Paused`. That **already** flows to `NodeExec::Paused`
  (top-level agents), the Consolidate-agent-body arm, and — for `Map`/`Loop` agent
  children — `MapChildPaused` → whole-Map/Loop pause (all built in prior slices).
  One change, broad coverage.

### 3.3 Resume-safety (free)

A gated call produced **no `EffectRecorded`** (it never got model output), so a
resume simply **re-attempts** the node against the gateway — the quota window may
have reset (→ succeeds + records) or still be gated (→ pauses again). There is no
memo entry to replay and no determinism fence to trip; a pause is a clean retry
point. (No new state is needed; the pause node is not in `fold.completed`, so a
resume re-drives it.)

### 3.4 `resume_after` capture

The structured `resume_after` lands in the **journaled `RunPaused`** event (its
durable home — a future durable scheduler reads it to re-arm). `RunOutcome.paused`
/ `PauseInfo` stay `{ node, reason }` (the `reason` string names the resume time);
threading `resume_after` through the whole pause-chain (`ToolOutcome`/`AgentStep`/
`NodeExec`/`PauseInfo`) is deferred until a consumer (the scheduler) needs it.

## 4. Testing approach (testability note)

`Gateway::execute` returns `AllGated` only when its health gates skip every
candidate — it is built from gate contributions, not returned by an adapter, and
the `cooldown`/`model_lockout` stores are **private** (can't be seeded from the
orchestrator crate). So the integration test uses a **warm-up fixture** (public
APIs only): build a `Gateway` with a single-candidate chain whose adapter always
times out (with `Timeout` a configured fallback trigger) or always auth-fails;
issue **one direct `gw.execute` warm-up** to trip the router's cooldown (timed →
`AllGated{Some}`) or endpoint lockout (terminal → `AllGated{None}`); THEN hand the
same `Arc<Gateway>` to the executor, whose node call now finds the only candidate
gated → `execute → AllGated`.

**De-risk first:** the plan's Task 1 is a spike that asserts the warm-up fixture
actually yields `execute → AllGated{Some}` / `{None}` before any wiring is built.
If the warm-up recipe proves unreliable, fall back to a small `#[cfg(feature =
"test-util")]` seed on the gateway crate (a justified, isolated test seam). The
`classify_gateway_error` **policy** is unit-tested directly (construct `AllGated`
in memory) and does not depend on the fixture.

## 5. Deferred (stated)

- ModelCall **bodies** inside `Map`/`Consolidate`/`Loop` pausing on a gateway gate
  (they fail, as a Map-child failure does today — a follow-up adds
  `MapChildPaused`-on-gateway-pause for ModelCall children).
- The **durable scheduler** that re-arms a paused run at `resume_after` (SP-1.4 /
  SP-DATA); this slice records the pause + `resume_after`, resumed out-of-band.
- `RateLimit{retry_after_ms}` at tool level → journaled `Timer` backoff (§11.2 row 4).
- Threading `resume_after` into `RunOutcome.paused`/`PauseInfo`.

## 6. Acceptance criteria (TDD)

1. **Fixture spike.** `all_gated_gateway()` (warm-up) → `gw.execute(...)` returns
   `GatewayError::AllGated { resume_after: Some(_), .. }`; a lockout variant returns
   `AllGated { resume_after: None, human_action: Some(_), .. }`. (Proves the fixture
   before any wiring.)
2. **Classifier (policy).** `classify_gateway_error`: `AllGated{Some(t)}` → `Pause`
   (reason names `t`); `AllGated{None, human_action}` → `Fail` (message carries the
   hint); `QuotaExceeded`/`BudgetExceeded`/other → `Fail`.
3. **ModelCall node pauses.** A top-level `ModelCall` node whose gateway returns
   `AllGated{Some}` → `RunOutcome.paused` is `Some(node)`, the journal has
   `RunPaused { resume_after: Some(_) }` and NO `RunCompleted`, and `on_run_paused`
   fires (spy).
4. **Agent node pauses.** An `Agent` node whose turn gates (`AllGated{Some}`) →
   `RunOutcome.paused` set, no `RunCompleted`.
5. **Terminal gate fails, not pauses.** `AllGated{None}` → the node **fails**
   (`RunOutcome.failed`), NOT paused; the failure message carries the human-action
   hint; a hard-dependent cascade-skips.
6. **Resume re-attempts.** A run paused on `AllGated{Some}` (warm-up cooled), then
   resumed with a fresh (un-gated) gateway → the node re-attempts, succeeds, and the
   run completes (`RunCompleted`) with no `DeterminismViolation`.
7. **Opt-in unaffected.** A non-gated run is byte-identical (this slice only adds a
   branch on the gateway-`Err` path; success is untouched).
