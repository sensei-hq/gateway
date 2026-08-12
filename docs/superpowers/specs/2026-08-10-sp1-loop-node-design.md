---
title: SP-1 Loop node — iterate-a-body-with-a-gate
doctype: design
module: orchestrator
spec: SP-1
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§10.3 loops of graphs, §11 resilience)
date: 2026-08-10
---

# SP-1 Loop node — "loops of graphs" (walking skeleton)

## 1. Goal

Add `NodeKind::Loop`: iterate a body until a **deterministic gate** says Stop or
a `max_iters` backstop is hit, resume-safe and **never a bare fail** (design
§10.3). This is the walking-skeleton cut of the design's "loops of graphs" — it
delivers refine/replan iteration (each pass builds on the last) without the full
`Subgraph`/`PlanDelta` machinery, which is deferred.

## 2. Background

- `effect_id(parent_path, loop_iteration, local_index)` already exists; per-slice
  code uses the middle arg as an agent `turn` and `0` for top-level nodes. This
  slice encodes the loop iteration in the **path** (`"{loop}/{i}"`, mirroring Map
  children `"{map}/{i}"`) so each iteration's effects get distinct ids — no
  cross-iteration memo collision (the design's "loop iteration is part of the id"
  invariant, satisfied via the path).
- `Map`/`Consolidate` (bodies `MapBody = ModelCall{chain} | Agent(AgentRef)`) are
  the pattern to mirror: an internal per-item/iteration sub-run, fold-guarded
  control events, resume via memo replay.
- The gateway **cost budget axis is dormant** (`budget: None`), so `max_iters` is
  the concrete termination backstop this slice; a cost/timeout backstop is
  deferred.
- `Subgraph`/`Branch`/`PlanDelta` are unimplemented and out of scope.

## 3. Types (`orchestrator-core`, `graph.rs`)

```rust
NodeKind::Loop {
    body: MapBody,             // ModelCall{chain} | Agent(AgentRef) — reuse; defer Subgraph
    input: serde_json::Value,  // iteration 0's input
    gate: LoopGate,
    max_iters: usize,          // termination backstop (>= 1)
}

/// A deterministic Stop condition, evaluated as a pure function of one
/// iteration's body output — so a resume recomputes the identical decision with
/// no gate journaling.
pub enum LoopGate {
    /// Stop when `output["text"]` contains this marker substring. (Fits the
    /// `{model, text}` shape a ModelCall/Agent body produces.)
    TextContains(String),
    /// Stop when `output[field] == true` (a structured body output).
    FieldTrue(String),
}
```

`LoopGate` gets a helper `fn should_stop(&self, output: &serde_json::Value) -> bool`
(pure). `TextContains(m)` → `output["text"].as_str().is_some_and(|t| t.contains(m))`;
`FieldTrue(f)` → `output[f] == json!(true)`.

## 4. Iteration model

`run_loop` drives `for i in 0..max_iters`:
1. Run `body` once at path `"{loop}/{i}"` with the iteration's input.
   - Iteration 0's input is `Loop.input`.
   - Iteration `i>0`'s input is **iteration `i-1`'s output** — the refine thread.
   - `Agent` body → `drive_agent(run, &NodeId("{loop}/{i}"), agent_ref, &input, &[], fold)`.
   - `ModelCall` body → one Pure effect `effect_id("{loop}/{i}", 0, 0)` (mirror
     `run_map_child_modelcall`).
2. Evaluate `gate.should_stop(&output)`.
   - **Stop** → the Loop completes, `converged: true`, `output` = this iteration's output.
   - **Continue** → feed `output` forward as the next iteration's input.
3. If the loop exhausts `max_iters` without a Stop → complete **best-effort**,
   `converged: false`, `output` = the last iteration's output (design §10.3 — never
   a bare fail; `converged` surfaces non-convergence honestly, not silently).

A body iteration that **fails** (its `drive_agent`/ModelCall returns a node
failure) fails the whole Loop: `NodeExec::Failed { message: "loop {id} failed at
iteration {i}: …", output: None }` — the no-silent-failure path (cascade-skip
applies), distinct from the finalize-on-cap path.

## 5. Output shape

```json
{ "iterations": <n>, "converged": <bool>, "output": <final iteration's body output> }
```

`iterations` = how many iterations ran (1-based count). A downstream node reads
`converged` to branch on convergence.

## 6. Journaling, resume & determinism

- The Loop's own `NodeStarted` (once) and `NodeCompleted` (on stop/cap) are
  fold-guarded via `fold.started`/`fold.completed`, exactly like `run_map`.
- Each iteration's body journals its own effects at `"{loop}/{i}"` (a ModelCall
  body: `NodeStarted`→`EffectRecorded`→`NodeCompleted`, fold-guarded; an Agent
  body: its full ReAct lifecycle via `drive_agent`).
- **No new journal event.** The iteration count and every gate decision are
  reconstructable on resume from the memoized body outputs: re-driving replays
  each completed iteration's body (memo-hit, zero gateway/re-spend), recomputes
  its (pure) gate decision, and advances; the first not-yet-completed iteration
  runs live. The stop point is therefore identical across a resume — a pure
  function of the journal.
- **Determinism fence:** an Agent body's per-turn hash and a ModelCall body's
  input-hash already fence each iteration; a tampered/edited completed iteration
  halts with `DeterminismViolation` (never a silent divergence). Blackboard
  publish for the Loop node's own output happens in `apply_node_result` (§ shared
  context), fold-guarded as for any node.

## 7. Interaction with existing mechanisms

- `run_node` gains a `NodeKind::Loop => self.run_loop(...)` arm; `run_loop` lives
  in `fanout.rs` alongside `run_map`/`run_consolidate` (same shape).
- Any exhaustive `match` on `NodeKind` must gain a `Loop` arm (compiler-enforced):
  at least `run_node`. `consolidate_compaction_target`/`project_agent_outputs` use
  `let else`/single-arm matches and are unaffected. `validate_dag` treats a Loop
  as any node (deps validated normally).
- The Loop body's iteration sub-runs do **not** auto-publish to the blackboard
  (only top-level nodes publish, as with Map children); only the Loop node's
  aggregate output publishes.

## 8. Deferred (stated)

- Full `Subgraph` / multi-node loop bodies; `Branch`; `PlanDelta` runtime graph
  expansion + `PlanExpanded`.
- LLM/fuzzy **gate agent** (a Continue/Stop model call per iteration) — this slice
  is a deterministic gate only.
- **Cost/timeout budget** backstop + reserved-synthesis-budget (budget axis is
  dormant); `max_iters` is the only backstop here.
- Nested-loop / global total-node / expansion-depth caps (self-DoS guard beyond a
  single Loop's `max_iters`).
- Blackboard context reads for loop-body iterations (state threads via the
  refine-input this slice).
- `AwaitSignal`/`HumanGate` inside a loop.

## 9. Acceptance criteria (TDD)

1. **Stop on gate.** A Loop whose body emits the stop marker at iteration k
   completes with `iterations == k+1`, `converged == true`, `output` = iteration
   k's output; the body ran exactly `k+1` times (call-count asserted).
2. **Cap without stop.** A Loop whose gate never fires runs exactly `max_iters`
   times, then completes with `converged == false` (NOT failed), `output` = the
   last iteration's output.
3. **Refine thread.** Iteration `i>0` receives iteration `i-1`'s output as input
   (assert via a body/echo that surfaces its input, e.g. the input appears in the
   next output).
4. **Body failure fails the Loop.** An iteration whose body fails (e.g. a
   content-gated `FAIL`) yields `NodeExec::Failed` naming the iteration; the run
   does not `RunCompleted`; cascade-skip applies to hard-dependents.
5. **Resume re-spends nothing.** A Loop that dies after iteration `j` completes;
   on resume the first `j+1` iterations memo-hit (zero gateway calls for them),
   the gate decisions recompute identically, the tail runs live, and the Loop
   stops at the same iteration.
6. **Determinism halt.** A completed iteration's body effect tampered under a
   resume halts with `DeterminismViolation`, never a silent divergence.
7. **No-store / opt-in unaffected.** A graph with no Loop node is byte-identical;
   a Loop with a `ModelCall` body works without any registry/tools wired.
