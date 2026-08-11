---
title: SP-1 OrchestratorHooks — best-effort observability
doctype: design
module: orchestrator
spec: SP-1
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§15 hooks, §11.1.3 hook-error isolation)
date: 2026-08-11
---

# SP-1 OrchestratorHooks — best-effort progress/observability (walking skeleton)

## 1. Goal

A best-effort observability seam: an `OrchestratorHooks` trait (no-op-default
methods) the executor fires at run / node / agent / context lifecycle points, for
progress UIs and test spies. Hooks are **observation only** — they never affect
execution, journaling, or determinism, and never double-count on resume. This is
the walking-skeleton cut of design §15; several richer hooks defer (§8).

## 2. Background

- The executor already journals the lifecycle events the hooks mirror
  (`RunStarted`/`RunCompleted`/`RunPaused`, `NodeStarted`/`NodeCompleted`/
  `NodeFailed`/`NodeSkipped`, `ContextWrite`), all through one method:
  `Executor::append`. Agent per-turn/per-tool activity is journaled as
  `EffectRecorded` inside `drive_agent`.
- On resume, completed nodes replay from the memo **without re-appending** their
  events (fold-guarded), and memoized turns/tools skip the live path — this is the
  lever that gives hooks replay-suppression for free (§4).
- Hooks are NOT the journal: anything execution depends on (a HITL notification)
  is a durable outbox effect, never a best-effort hook (design §15).

## 3. The trait (`orchestrator-core`, new `hooks.rs`)

`#[async_trait]`, every method a `()` no-op default, `Send + Sync`:

```rust
#[async_trait::async_trait]
pub trait OrchestratorHooks: Send + Sync {
    async fn on_run_started(&self, _run: RunId) {}
    async fn on_run_completed(&self, _run: RunId) {}
    async fn on_run_paused(&self, _run: RunId, _reason: &str) {}
    async fn on_node_started(&self, _run: RunId, _node: &NodeId) {}
    async fn on_node_completed(&self, _run: RunId, _node: &NodeId) {}
    async fn on_node_failed(&self, _run: RunId, _node: &NodeId, _error: &str) {}
    async fn on_node_skipped(&self, _run: RunId, _node: &NodeId) {}
    async fn on_agent_started(&self, _run: RunId, _node: &NodeId, _agent: &str, _chain: &str) {}
    async fn on_agent_turn(&self, _run: RunId, _node: &NodeId, _turn: usize) {}
    async fn on_agent_tool_call(&self, _run: RunId, _node: &NodeId, _tool: &str) {}
    async fn on_context_write(&self, _run: RunId, _scope: &Scope, _key: &ContextKey) {}
}
```

Exported from `orchestrator-core::lib`. Wired via `Executor::with_hooks(Arc<dyn
OrchestratorHooks>)` (field `hooks: Option<Arc<dyn OrchestratorHooks>>`, default
`None` ⇒ no firing).

## 4. Firing strategy (the key idea)

**Run / node / context hooks fire from inside `Executor::append`**, matched on the
`JournalEvent` just successfully journaled:

| JournalEvent | Hook |
|---|---|
| `RunStarted` | `on_run_started` |
| `RunCompleted` | `on_run_completed` |
| `RunPaused{reason}` | `on_run_paused(reason)` |
| `NodeStarted{node}` | `on_node_started(node)` |
| `NodeCompleted{node}` | `on_node_completed(node)` |
| `NodeFailed{node,error}` | `on_node_failed(node, error)` |
| `NodeSkipped{node}` | `on_node_skipped(node)` |
| `ContextWrite{scope,key}` | `on_context_write(scope, key)` |

All other events (`EffectRecorded`, `EffectIntent`, `MapExpanded`, `MapCompacted`)
are no-ops for hooks. Firing happens **after** the journal append succeeds (a
failed journal write surfaces its error and fires no hook).

Why `append`-centric: (a) **can't-miss** — every journaled lifecycle event fires
its hook, no scattered call sites to forget; (b) **replay-suppression for free** —
on resume the completed prefix replays from the memo *without* re-appending its
events (fold-guarded), so those hooks simply don't re-fire; no need to thread
hooks into the pure `fold_journal`.

**Agent hooks fire explicitly in `drive_agent`**, on the **live** path only:
- `on_agent_started(node, agent, chain)` — once, gated on the same
  `!node_started` condition that guards the `NodeStarted` append (so a resume
  replay doesn't re-fire it).
- `on_agent_turn(node, turn)` — in the live model-dispatch path
  (`dispatch_model_turn`), NOT on a memoized-turn replay.
- `on_agent_tool_call(node, tool)` — when a tool is **live-executed**
  (`record_tool_effect`), NOT on a memo replay.

Because each agent hook sits on the live branch, a resumed run's memoized
turns/tools skip it — same replay-suppression property as the `append` path.

## 5. Non-silent / best-effort

Methods return `()` (fire-and-forget); a wired hook is `await`ed inline at its
site. This slice is minimal: a slow hook blocks execution at that point (a real
impl should be fast or spawn its own task). The design's non-silent **hook-error
channel** (`HookError` event / diagnostics surface) and **panic isolation**
(§11.1.3) are DEFERRED — noted, not built. No hook can affect execution or
journaling in this slice because the no-op-default methods return `()` and are
called after the journal write.

## 6. Determinism & resume safety

Hooks touch neither the journal, the memo, nor any effect output, so they cannot
change what is journaled or replayed — execution is byte-identical whether or not
hooks are wired (an acceptance test). Replay-suppression (§4) means a resume fires
hooks only for **newly** journaled events / live turns, never for the replayed
completed prefix.

## 7. Interaction with existing mechanisms

- `append` gains a post-journal match that fires the wired hook; it still returns
  the authoritative `Seq`. The event is cloned for the match (small cost) so the
  journal still consumes the original.
- Map/Loop **agent** children/iterations (sub-runs at `"{map}/{i}"`/`"{loop}/{i}"`)
  append `NodeStarted`/`NodeCompleted`, so they fire `on_node_started`/`completed`
  for those child paths — acceptable (a UI sees child progress). ModelCall
  children/iterations append only `EffectRecorded` (no node hooks).
- No new journal event; the control-flow log is byte-identical to before.

## 8. Deferred (stated)

- `on_plan_expanded` (no `PlanDelta`), `on_agent_stream_chunk` (no
  `execute_stream`), `on_agent_model_attempt` (needs the gateway's attempts trail
  bubbled), `usage`/`cost` on completion (budget axis dormant), `on_run_resumed`,
  `on_agent_tool_result`.
- `on_node_started{kind}` — carrying the node kind (would force explicit firing at
  ~15 sites, losing the `append` integration); a UI maps node→kind from the graph.
- Fold-time `replay: true` firing (so a fresh UI attaching to a resumed run can
  re-render the completed prefix) + the `replay` arg on every method.
- The `HookError`/diagnostics channel + panic isolation (§11.1.3).

## 9. Acceptance criteria (TDD)

A test `RecordingHooks` spy (an `Arc<Mutex<Vec<String>>>` of `"event(args)"`
labels) implements `OrchestratorHooks`.

1. **Run + node lifecycle.** A 2-node linear run fires, in order:
   `run_started`, `node_started(n1)`, `node_completed(n1)`, `node_started(n2)`,
   `node_completed(n2)`, `run_completed`.
2. **Agent lifecycle.** An `Agent` node that makes one tool call then finishes
   fires `agent_started(node, agent, chain)`, `agent_turn(node, 0)`,
   `agent_tool_call(node, tool)`, `agent_turn(node, 1)` (the final turn), plus the
   generic `node_started`/`node_completed`.
3. **Failure + cascade.** A failed node fires `on_node_failed`; a hard-dependent
   fires `on_node_skipped`.
4. **Pause.** An in-doubt Mutation resume that pauses fires `on_run_paused(reason)`
   and NOT `on_run_completed`.
5. **Context write.** With a `ContextStore` wired, a completed node's publish fires
   `on_context_write(Run, node)`.
6. **Resume does not re-fire (headline).** A run that dies mid-way, then resumes:
   the spy attached to the RESUME sees hooks only for the newly-run tail — zero
   `node_started`/`node_completed`/`agent_turn` for the replayed completed prefix.
7. **Opt-in / byte-identical.** With no hooks wired, the journal event sequence and
   outputs are identical to before (hooks change nothing).
