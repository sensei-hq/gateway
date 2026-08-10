---
title: SP-1 Blackboard wiring — ContextStore into the executor
doctype: design
module: orchestrator
spec: SP-1
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§8 shared context, §9 agent runtime)
date: 2026-08-10
---

# SP-1 Blackboard wiring — shared-context `ContextStore` into the executor

## 1. Goal

Wire the slice-3 `ContextStore` blackboard into the durable executor so that a
node's output is **published** to a shared, scoped blackboard, and a downstream
`Agent` node's prompt **automatically includes its dependencies' outputs**. This
delivers the design's headline cross-role handoff — "fable plans → opus refines":
the refining agent reads the planner's output from the blackboard rather than
having it threaded by hand. The wiring is resume-safe and stores **refs, not
blobs**.

This is the fourth SP-1 gap (after Observation/Mutation): the `ContextStore`
types and in-memory store shipped in slice 3 but the executor never called them
(`put`/`get`/`ContextWrite` were unwired). This slice wires them.

## 2. Background — what exists vs. what's missing

**Exists (slice 3, `orchestrator-core` + `orchestrator-store`):**
- `Scope { Run, Node(NodeId) }`, `ContextKey(String)`, `ContextRef { key, scope, content: ContentRef, summary }`.
- `ContextStore` trait: `put(scope, key, value) -> ContextRef` (writes bytes to
  the CAS, keeps a ref; **rejects a write to an existing `(scope, key)`** with
  `ContextKeyCollision` — no last-write-wins), `get(scope, key) -> Option<ContextRef>`
  (resolves **up** the scope chain `Node → Run`; a miss is explicit `Ok(None)`),
  `load(&ContextRef) -> Value` (lazy CAS fetch + deserialize).
- `InMemoryContextStore` implementing the trait.
- The executor already carries an **optional injected** `content: Arc<dyn ContentStore>`
  + `cas_threshold` (slice 3, via `with_content_store`).

**Missing (this slice):**
- The executor never constructs or calls a `ContextStore`.
- No `ContextWrite` journal event, so writes are neither durable nor folded.
- `assemble_prompt` has no context section; an agent sees only its own `input`.

## 3. Design decisions

- **D1 — Executor-managed / implicit.** The executor publishes node outputs and
  injects dependency outputs into prompts. No agent-facing `read_context`/
  `write_context` tools this slice (deferred). Zero agent-authoring burden;
  deterministic by construction (D2).
- **D2 — Dependency-scoped reads (the determinism mechanism).** An `Agent` node
  reads the Run-scoped outputs of its **declared dependencies** only — never "all
  Run entries". The DAG scheduler runs a node only after every dependency is
  terminal, so each dependency's `ContextWrite` is journaled (with a lower `Seq`)
  **before** the node's first turn. On resume the same dependency entries are
  present and identical, so the assembled prompt — and thus the turn's
  `agent_input_hash` — is byte-identical, and memoized turns replay cleanly.
  Reading unscoped Run state would be non-deterministic (entries from
  not-yet-terminal siblings could appear on resume but not on the original run).
- **D3 — Opt-in, additive.** `Executor.context: Option<Arc<dyn ContextStore>>` +
  `with_context_store(...)`, mirroring `with_content_store`. **No context store
  wired ⇒ every blackboard step is a no-op ⇒ slice-4 behavior is byte-identical**
  (preserves the "no store ⇒ unchanged" property established in slice 3).
- **D4 — Refs, not blobs; fold rehydrates.** A write is `ctx.put(...)` (bytes to
  the CAS, ref kept) + a journaled `ContextWrite { scope, key, content: ContentRef }`.
  On resume the fold collects `ContextWrite` events and rehydrates the injected
  store via a new `ContextStore::insert_ref(ContextRef)` — **no blob
  materialization** (the CAS persists across the crash seam; blobs load lazily on
  read). Matches design §8 "the fold rebuilds the store from journaled writes
  without materializing payloads".
- **D5 — Budgeting via the existing halt.** Each dependency contributes its
  `summary` if present, else its full value, rendered into the prompt's context
  section. The **existing** per-turn window budget (`PromptOverBudget`, a loud
  halt) covers overflow — never silent truncation, consistent with the
  no-silent-failures invariant. Active summarize/select is deferred.
- **D6 — Scope this slice = `Run`, key = `node.id`.** Uniform: any completed node
  publishes to `Scope::Run` under `key = node.id`, so any downstream node reads
  any upstream dependency uniformly by its id. `Scope::Node` stays typed but
  unused. Node ids are unique, so `put`'s collision-reject never fires on the
  auto-publish path (a collision would be a real bug and should halt loud).

## 4. Data-model & API changes

`orchestrator-core`:
- `JournalEvent::ContextWrite { scope: Scope, key: ContextKey, content: ContentRef, seq: Seq }`
  — the durable record of a blackboard publish (design §7 event list).
- `ContextStore::insert_ref(&self, r: ContextRef) -> Result<(), OrchestratorError>`
  — rehydrate an entry from a folded ref without touching the CAS. `InMemoryContextStore`
  implements it (idempotent-safe: re-inserting an identical `(scope, key)` on a
  fold replay must not error — folds are replayed wholesale).

`orchestrator` (`Executor`):
- `context: Option<Arc<dyn ContextStore>>` + `with_context_store(Arc<dyn ContextStore>)`.
- `Fold.context_writes: Vec<ContextWrite>` (or a `HashMap<(Scope, ContextKey), ContextRef>`),
  folded from `ContextWrite` events; used to rehydrate the store on resume.
- `assemble_prompt(...)` gains a `context: &[(ContextKey, serde_json::Value)]`
  (resolved dependency outputs) rendered into the system prompt as a `## Context`
  section, between skills and tool schemas (design §9.2: body + skills +
  **context** + tools). Context is part of the assembled system prompt, hence an
  input to `agent_input_hash` — already the determinism fence.

## 5. Write path

On a **top-level** node's successful completion — in `apply_node_result`, which
handles `ModelCall`/`Agent`/`Map`/`Consolidate` node results (Map *children*
complete inside `run_map` and do **not** publish this slice, per §9) — if a
`ContextStore` is wired and the node has not already published (fold-guarded,
like every control append), the executor:
1. `let r = ctx.put(Scope::Run, ContextKey(node.id.0), output).await?;`
2. `self.append(run, JournalEvent::ContextWrite { scope: Run, key, content: r.content, seq: 0 }).await?;`

`put` writes the output bytes to the CAS and returns a `ContextRef`. A duplicate
`(Run, node.id)` — impossible on the auto-publish path (unique ids) — surfaces
`ContextKeyCollision` loudly rather than silently overwriting.

## 6. Read path (prompt assembly)

When driving an `Agent` node, before the first turn the executor resolves its
context: for each declared dependency `dep` of the node, `ctx.get(Run, dep.on)` →
if `Some(ref)`, `ctx.load(&ref)` (or use `ref.summary` when present per D5) →
collect `(key, value)`. These are passed to `assemble_prompt` and rendered into
the `## Context` section. Reads are ordered by the node's declared dependency
order (deterministic). A soft-dependency that did not complete yields `Ok(None)`
and is simply omitted (its terminal state is fold-stable, so its
presence/absence is deterministic).

The resolved context is assembled **once per node** (invariant across the ReAct
turns, like the system prompt), so it lives in the `AgentRun` context bundle.

## 7. Resume / fold

`fold_journal` folds `ContextWrite` events into the fold. On resume, before
scheduling, the executor rehydrates the injected `ContextStore` by
`insert_ref`-ing every folded entry (idempotent). Because the CAS
(`ContentStore`) is Arc-shared across the crash/resume seam (like the journal),
the refs still resolve and blobs load lazily. Reads during a resumed node's
prompt assembly then see the identical dependency entries → identical prompt →
memoized turns replay with zero re-spend.

## 8. Determinism argument (the crux)

A resumed run must produce byte-identical prompts so memoized `ModelCall` turns
replay (no re-spend) and the determinism fence never spuriously fires. The
guarantee rests on **D2 + the DAG scheduler**:

1. The scheduler dispatches a node only after all its dependencies are terminal.
2. A completed dependency journals its `ContextWrite` before the dependent node's
   first turn (lower `Seq`).
3. The dependent reads **only its declared dependencies** (D2), whose entries are
   therefore guaranteed present and value-stable across a resume.
4. The context is an input to `agent_input_hash`, so an *edited* upstream output
   (a genuine change) halts loudly rather than mixing new context into a memoized
   old turn — the same fence that already guards skills/agent-defs (§9.1).

Reading unscoped Run state would break (1)–(3): a sibling that had not completed
on the original run but has on resume would inject new context and diverge. Hence
dependency-scoping is a correctness requirement, not a convenience.

## 9. Interaction with existing mechanisms

- **`prior_outputs`** (threaded into `run_node`/`run_consolidate`) stays as-is;
  the blackboard is additive for `Agent` prompt context, not a rewrite of
  `Consolidate` survivor collection. (Unifying them onto the blackboard is
  deferred.)
- **CAS split (slice 3)** is orthogonal: `EffectOutput` still governs how effect
  outputs are journaled; the blackboard independently `put`s node outputs to the
  CAS as context refs.
- **Map children** do **not** auto-publish this slice: publishing happens in
  `apply_node_result` (top-level nodes only), and the read side is scoped to
  top-level declared dependencies. A downstream node reads the Map's *aggregate*
  output (`{results, manifest}`) under `Run/"{map}"`. Child-granular publish/read
  is deferred.

## 10. Deferred (stated)

- Agent-facing `read_context` / `write_context` tools (explicit info-needs).
- Active summarize/select budgeting (beyond the existing over-budget halt).
- `Scope::Node` / `Scope::Plan` reads and writes; per-agent private scratch.
- TTL / as-of freshness stamps on entries (design §8 freshness).
- Replacing `prior_outputs` threading with blackboard reads.
- Concurrent-write conflict policies beyond reject (seq-ordered LWW / merge node).
- Secret redaction on writes (secrets are never written on the auto-publish path
  since node outputs are model text/tool results, but an explicit-write slice
  must enforce it).

## 11. Acceptance criteria (TDD)

1. **No store ⇒ byte-identical.** With no `ContextStore` wired, a graph's journal
   event sequence and outputs are identical to slice 4 (no `ContextWrite`).
2. **Publish + journal.** A completed node publishes to `Run/node.id`; the journal
   carries a `ContextWrite` whose `content` is a `ContentRef` (never an inline
   blob), and the blob round-trips from the CAS.
3. **Cross-role handoff.** In `A(model) → B(agent, hard-dep A)`, B's assembled
   prompt provably contains A's output (assert the resolved context / the request
   messages), and B does not receive a non-dependency's output.
4. **Collision is loud.** A forced duplicate `(Run, key)` write surfaces
   `ContextKeyCollision` (halt), never a silent overwrite.
5. **Resume re-spends nothing.** `A → B(agent)` where B dies mid-loop: on resume
   the fold rehydrates the blackboard from `ContextWrite`, B's completed turns
   replay from the memo (zero gateway calls for them), and the run completes.
6. **Determinism halt.** If A's published output is tampered under B on resume
   (context changes), B's turn `agent_input_hash` mismatches and the resume halts
   loud with `DeterminismViolation` — never a silent mix.
7. **Over-budget is loud.** A dependency output large enough to bust the per-turn
   window halts with `PromptOverBudget`, never silent truncation.
