# SP-1 Blackboard Wiring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the slice-3 `ContextStore` blackboard into the executor so a node's output is published to a shared scoped blackboard and a downstream `Agent`'s prompt automatically includes its dependencies' outputs — resume-safe, refs-not-blobs.

**Architecture:** Additive/opt-in (`Executor.context: Option<Arc<dyn ContextStore>>`, no store ⇒ byte-identical to slice 4). Writes: `apply_node_result` publishes each completed **top-level** node's output to `Scope::Run`/`key=node.id` via `ctx.put`, journaling a new `JournalEvent::ContextWrite`. Reads: `run_node`'s Agent arm resolves the node's declared-dependency outputs and injects them into the system prompt (a `## Context` section in `assemble_prompt`). Determinism rests on dependency-scoping + the DAG scheduler (a dep's `ContextWrite` is journaled before the dependent runs). Resume: `fold_journal` folds `ContextWrite`; `start` rehydrates the store via a new `ContextStore::insert_ref` before driving.

**Tech Stack:** Rust; `sensei-orchestrator-core` (types/traits), `sensei-orchestrator-store` (in-mem impls), `sensei-orchestrator` (executor). `async-trait`, `serde_json`, `tokio::test`.

**Design:** `docs/superpowers/specs/2026-08-10-sp1-blackboard-wiring-design.md`.

**Conventions (non-negotiable):** TDD (write the failing test, watch it fail, minimal code to green). `cargo fmt --all` before every commit (pre-commit hook = fmt-check + workspace `clippy -D warnings`). Verify REAL exit codes, never a piped `| tail`. Each task ends green + clippy-clean.

---

## File structure

- `crates/orchestrator-core/src/journal.rs` — add `JournalEvent::ContextWrite`.
- `crates/orchestrator-core/src/context.rs` — add `ContextStore::insert_ref`.
- `crates/orchestrator-store/src/stores.rs` — impl `InMemoryContextStore::insert_ref`.
- `crates/orchestrator/src/executor/mod.rs` — `Executor.context` + `with_context_store`; `Fold.context`; thread `fold` into `apply_node_result`; `publish_context`/`resolve_context`/`rehydrate_context`; call rehydrate in `start`; publish in the `Completed` arm.
- `crates/orchestrator/src/executor/support.rs` — fold `ContextWrite` in `fold_journal`.
- `crates/orchestrator/src/agent/prompt.rs` — `assemble_prompt` gains a `context` param + `## Context` rendering.
- `crates/orchestrator/src/executor/agent.rs` — `drive_agent` gains a `context` param, passes it to `assemble_prompt`.
- `crates/orchestrator/src/executor/fanout.rs` — `run_map`/`run_consolidate` pass `&[]` context to `drive_agent`.
- `crates/orchestrator/src/executor/tests.rs` — `label()` gains a `ContextWrite` arm; acceptance tests.
- `docs/features/orchestrator/{shared-context.md,README.md}` — status flip.

---

## Task 1: `ContextWrite` journal event (core)

**Files:**
- Modify: `crates/orchestrator-core/src/journal.rs`
- Test: same file `#[cfg(test)] mod tests`

- [ ] **Step 1: Write the failing test** in `journal.rs` tests:

```rust
#[test]
fn context_write_event_roundtrips() {
    use crate::content::{ContentRef, Digest};
    use crate::context::{ContextKey, Scope};
    let e = JournalEvent::ContextWrite {
        scope: Scope::Run,
        key: ContextKey("n1".into()),
        content: ContentRef { digest: Digest("d".into()), size: 3, summary: None },
        summary: None,
        seq: 0,
    };
    let s = serde_json::to_string(&e).unwrap();
    assert!(matches!(
        serde_json::from_str::<JournalEvent>(&s).unwrap(),
        JournalEvent::ContextWrite { .. }
    ));
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator-core context_write_event_roundtrips 2>&1 | grep -E "error\[|cannot find|test result"`
Expected: FAIL to compile (`ContextWrite` variant does not exist).

- [ ] **Step 3: Add the variant.** In `journal.rs`, add imports at the top of the event's needs (the file already imports `ContentRef`? verify — it imports `crate::content::{Digest, EffectOutput}`; add `ContentRef`) and, in the `JournalEvent` enum (after `MapCompacted`, before `RunCompleted`), add:

```rust
    /// A shared-scope blackboard publish (§8). Journaled so a resume rebuilds the
    /// `ContextStore` (as refs, no blob load) via `ContextStore::insert_ref`. The
    /// `content` is a CAS ref — never an inline blob.
    ContextWrite {
        scope: crate::context::Scope,
        key: crate::context::ContextKey,
        content: ContentRef,
        summary: Option<String>,
        seq: Seq,
    },
```

Update the top-of-file `use` to `use crate::content::{ContentRef, Digest, EffectOutput};`.

- [ ] **Step 4: Keep dependents compiling — add the `label()` arm NOW.** `crates/orchestrator/src/executor/tests.rs::label` is an **exhaustive** match over `JournalEvent`; adding the variant breaks it, and it must compile before any later `cargo test -p sensei-orchestrator`. In `tests.rs::label`, add before the `RunCompleted` arm:

```rust
        JournalEvent::ContextWrite { key, .. } => format!("ContextWrite({})", key.0),
```

- [ ] **Step 5: Run to verify pass (core + workspace compiles)**

Run: `cargo test -p sensei-orchestrator-core context_write 2>&1 | grep "test result"`
Run: `cargo build --workspace --tests 2>&1 | grep -E "error|Finished"`
Expected: core test `ok`; workspace compiles (`Finished`).

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator-core): ContextWrite journal event + label arm (blackboard)"
```

---

## Task 2: `ContextStore::insert_ref` (core trait + in-mem impl)

**Files:**
- Modify: `crates/orchestrator-core/src/context.rs` (trait method)
- Modify: `crates/orchestrator-store/src/stores.rs` (impl + test)

- [ ] **Step 1: Write the failing test** in `stores.rs` tests (near the existing context-store test at ~line 166):

```rust
#[tokio::test]
async fn insert_ref_rehydrates_an_entry_without_recomputing_the_cas() {
    use orchestrator_core::{ContentRef, ContextKey, ContextRef, Digest, Scope};
    let content = std::sync::Arc::new(InMemoryContentStore::new());
    // Seed the CAS with a blob and build a ref to it.
    let bytes = serde_json::to_vec(&serde_json::json!({"v":1})).unwrap();
    let digest = ContentStore::put(&*content, &bytes).await.unwrap();
    let r = ContextRef {
        key: ContextKey("k".into()),
        scope: Scope::Run,
        content: ContentRef { digest, size: bytes.len(), summary: None },
        summary: None,
    };
    let store = InMemoryContextStore::new(content);
    store.insert_ref(r.clone()).await.unwrap();
    // get resolves it; load returns the seeded value.
    let got = store.get(Scope::Run, ContextKey("k".into())).await.unwrap().expect("present");
    assert_eq!(store.load(&got).await.unwrap(), serde_json::json!({"v":1}));
    // insert_ref is idempotent (fold replays wholesale) — no collision error.
    store.insert_ref(r).await.unwrap();
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator-store insert_ref_rehydrates 2>&1 | grep -E "error\[|no method|test result"`
Expected: FAIL (`insert_ref` does not exist).

- [ ] **Step 3: Add the trait method.** In `context.rs`, in the `ContextStore` trait (after `load`), add:

```rust
    /// Rehydrate an entry from an already-journaled ref (resume fold), WITHOUT
    /// touching the CAS. Idempotent: a fold replays every write, so re-inserting
    /// an identical `(scope, key)` must not error (unlike [`put`](Self::put)).
    async fn insert_ref(&self, r: ContextRef) -> Result<(), OrchestratorError>;
```

- [ ] **Step 4: Implement it** in `stores.rs` (`impl ContextStore for InMemoryContextStore`, after `load`):

```rust
    async fn insert_ref(&self, r: ContextRef) -> Result<(), OrchestratorError> {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.insert((r.scope.clone(), r.key.clone()), r);
        Ok(())
    }
```

- [ ] **Step 5: Run to verify pass + full core/store suites**

Run: `cargo test -p sensei-orchestrator-store > /tmp/t.log 2>&1; echo "EXIT=$?"; grep "test result" /tmp/t.log`
Expected: `EXIT=0`.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator): ContextStore::insert_ref for resume rehydration (blackboard)"
```

---

## Task 3: Executor `context` seam + `with_context_store` (no behavior yet)

**Files:**
- Modify: `crates/orchestrator/src/executor/mod.rs`

- [ ] **Step 1: Write the failing test** in `crates/orchestrator/src/executor/tests.rs` (append near other builder usages):

```rust
#[tokio::test]
async fn with_context_store_builder_is_wired_and_no_store_is_byte_identical() {
    use orchestrator_store::{InMemoryContentStore, InMemoryContextStore};
    // Builder compiles + returns Self; a run with a store wired still completes
    // (behavior asserted in later tasks). Here we only pin the seam exists.
    let ctx = Arc::new(InMemoryContextStore::new(Arc::new(InMemoryContentStore::new())));
    let (gw, _c) = recording_gateway().await;
    let _exec = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_context_store(ctx);
}
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator with_context_store_builder 2>&1 | grep -E "no method|error\[|test result"`
Expected: FAIL (`with_context_store` does not exist).

- [ ] **Step 3: Add the field + builder.** In `mod.rs`: add `use orchestrator_core::{... ContextStore ...}` to the core import block (add `ContextStore`, `ContextKey`, `ContextRef`, `Scope` — some used in later tasks; add now to avoid churn). Add the field to the `Executor` struct (after `reconcilers`):

```rust
    /// The scoped blackboard (§8) node outputs publish to and agent prompts read
    /// dependency context from. Optional/injected — no store wired ⇒ every
    /// blackboard step is a no-op (slice-4 behavior byte-identical).
    context: Option<Arc<dyn ContextStore>>,
```

In `new()` add `context: None,`. Add the builder (near `with_content_store`):

```rust
    /// Wire the scoped blackboard (§8): completed node outputs publish to it, and
    /// an `Agent` node's prompt is assembled with its dependencies' outputs read
    /// from it. Injected (shared across the crash/resume seam). Requires a
    /// `ContentStore` (entries are CAS refs).
    pub fn with_context_store(mut self, context: Arc<dyn ContextStore>) -> Self {
        self.context = Some(context);
        self
    }
```

- [ ] **Step 4: Run to verify pass + full suite (unchanged)**

Run: `cargo test -p sensei-orchestrator > /tmp/t.log 2>&1; echo "EXIT=$?"; grep -c "test result: ok" /tmp/t.log`
Expected: `EXIT=0`.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator): Executor.context seam + with_context_store (blackboard, inert)"
```

---

## Task 4: Publish on completion + fold + rehydrate + `label` arm

**Files:**
- Modify: `crates/orchestrator/src/executor/mod.rs` (`Fold.context`, `apply_node_result` signature+publish, `publish_context`/`rehydrate_context`, `start` rehydrate)
- Modify: `crates/orchestrator/src/executor/support.rs` (`fold_journal`)
- Modify: `crates/orchestrator/src/executor/tests.rs` (`label()` arm + tests)

- [ ] **Step 1: Write the failing tests** in `tests.rs`.

```rust
/// A completed node publishes to Run/node.id; the journal carries a ContextWrite
/// whose content is a CAS ref (never inline), and the blob round-trips.
#[tokio::test]
async fn completed_node_publishes_a_context_ref_to_the_blackboard() {
    use orchestrator_store::{InMemoryContentStore, InMemoryContextStore};
    let content = Arc::new(InMemoryContentStore::new());
    let ctx = Arc::new(InMemoryContextStore::new(content.clone()));
    let (gw, _c) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (graph, n1, _n2) = two_node_graph("a", "b");
    let out = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_content_store(content)
        .with_context_store(ctx.clone())
        .run(run, &graph)
        .await
        .expect("run");
    assert!(out.failed.is_none());
    // n1 published a ContextWrite carrying a ref (not an inline blob).
    let events = journal.load(run).await.unwrap();
    let wrote = events.iter().any(|(_, e)| matches!(e,
        JournalEvent::ContextWrite { key, .. } if key.0 == n1.0));
    assert!(wrote, "n1's completion journaled a ContextWrite: {:?}",
        events.iter().map(|(_, e)| label(e)).collect::<Vec<_>>());
    // The blackboard resolves n1's entry and its blob round-trips.
    let got = ctx.get(orchestrator_core::Scope::Run, orchestrator_core::ContextKey(n1.0.clone()))
        .await.unwrap().expect("n1 present on the blackboard");
    assert_eq!(ctx.load(&got).await.unwrap()["text"], "canned-response");
}

/// No context store wired ⇒ NO ContextWrite events ⇒ byte-identical to slice 4.
#[tokio::test]
async fn no_context_store_journals_no_context_writes() {
    let (gw, _c) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (graph, _n1, _n2) = two_node_graph("a", "b");
    Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .run(run, &graph).await.expect("run");
    assert!(journal.load(run).await.unwrap().iter()
        .all(|(_, e)| !matches!(e, JournalEvent::ContextWrite { .. })),
        "no store ⇒ no ContextWrite");
}
```

(The `label()` `ContextWrite` arm was already added in Task 1 Step 4, so this compiles.)

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator completed_node_publishes 2>&1 | grep -E "error\[|assert|test result"`
Expected: FAIL — the test compiles but no `ContextWrite` is journaled yet (publish not implemented).

- [ ] **Step 3a: `Fold.context`.** In `mod.rs` `Fold`, add:

```rust
    /// Blackboard entries folded from `ContextWrite` events (§8). On resume the
    /// store is rehydrated from these (refs, no blob load), and a completed node
    /// whose key is already here is NOT re-published (guard against a memoized
    /// replay colliding).
    context: HashMap<(Scope, ContextKey), ContextRef>,
```

- [ ] **Step 3b: Fold it** in `support.rs::fold_journal` (add an arm before `_ => {}`):

```rust
            JournalEvent::ContextWrite { scope, key, content, summary, .. } => {
                fold.context.insert(
                    (scope.clone(), key.clone()),
                    orchestrator_core::ContextRef {
                        key: key.clone(),
                        scope: scope.clone(),
                        content: content.clone(),
                        summary: summary.clone(),
                    },
                );
            }
```

(Add `ContextRef` etc. to `support.rs`'s `orchestrator_core::{...}` import, or fully-qualify as above.)

- [ ] **Step 3c: Publish helper + guard.** In `mod.rs`, add:

```rust
    /// Publish a completed node's output to the blackboard (§8): `put` to Run/key
    /// (bytes → CAS, ref kept) + journal `ContextWrite`. Fold-guarded — a memoized
    /// replay on resume (key already in `fold.context`) is skipped, so it never
    /// re-`put`s (which would collide) or re-journals. No store ⇒ no-op.
    async fn publish_context(
        &self,
        run: RunId,
        node_id: &NodeId,
        output: &serde_json::Value,
        fold: &Fold,
    ) -> Result<(), OrchestratorError> {
        let Some(ctx) = &self.context else { return Ok(()) };
        let key = ContextKey(node_id.0.clone());
        if fold.context.contains_key(&(Scope::Run, key.clone())) {
            return Ok(());
        }
        let r = ctx.put(Scope::Run, key, output.clone()).await?;
        self.append(
            run,
            JournalEvent::ContextWrite {
                scope: r.scope.clone(),
                key: r.key.clone(),
                content: r.content.clone(),
                summary: r.summary.clone(),
                seq: 0,
            },
        )
        .await?;
        Ok(())
    }

    /// Rehydrate the injected blackboard from folded `ContextWrite`s (resume) —
    /// insert_ref only, no blob load; the CAS persists across the crash seam.
    async fn rehydrate_context(&self, fold: &Fold) -> Result<(), OrchestratorError> {
        let Some(ctx) = &self.context else { return Ok(()) };
        for r in fold.context.values() {
            ctx.insert_ref(r.clone()).await?;
        }
        Ok(())
    }
```

- [ ] **Step 3d: Thread `fold` into `apply_node_result` + publish.** Change its signature to add `fold: &Fold`, update the call site in `drive` (`self.apply_node_result(run, graph, node, result, fold, &mut state)`), and in the `NodeExec::Completed` arm, after `state.outcome.outputs.insert(...)`, add:

```rust
                self.publish_context(run, &node.id, &output, fold).await?;
```

(Note: the `output` is moved into `outputs.insert`; capture a clone first or reorder — insert takes `output` by value, so call `publish_context(run, &node.id, &output, fold)` BEFORE the `insert`, or clone. Prefer: publish first, then insert.)

- [ ] **Step 3e: Rehydrate on resume.** In `start()`, in the partial-resume path, immediately before `self.drive(run, graph, &fold).await`, add:

```rust
        self.rehydrate_context(&fold).await?;
```

- [ ] **Step 4: Run to verify pass + full suite**

Run: `cargo test -p sensei-orchestrator > /tmp/t.log 2>&1; echo "EXIT=$?"; grep -E "test result:" /tmp/t.log | head -1`
Expected: `EXIT=0` (publish + no-store tests green; all prior green).

- [ ] **Step 5: Collision test (acceptance 4).** Add:

```rust
/// A duplicate (Run, key) publish surfaces ContextKeyCollision loudly. Force it
/// by pre-seeding the store with n1's key before the run reaches n1.
#[tokio::test]
async fn duplicate_context_key_publish_is_a_loud_collision() {
    use orchestrator_store::{InMemoryContentStore, InMemoryContextStore};
    let content = Arc::new(InMemoryContentStore::new());
    let ctx = Arc::new(InMemoryContextStore::new(content.clone()));
    // Pre-seed Run/"n1" WITHOUT a ContextWrite (so the fold-guard does not skip).
    ctx.put(orchestrator_core::Scope::Run, orchestrator_core::ContextKey("n1".into()),
            serde_json::json!({"pre":"seeded"})).await.unwrap();
    let (gw, _c) = recording_gateway().await;
    let (graph, _n1, _n2) = two_node_graph("a", "b");
    let err = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_content_store(content)
        .with_context_store(ctx)
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect_err("duplicate publish collides");
    assert!(matches!(err, OrchestratorError::ContextKeyCollision { .. }), "got {err:?}");
}
```

Run: `cargo test -p sensei-orchestrator duplicate_context_key 2>&1 | grep "test result"` → `ok`.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings 2>&1 | grep -E "warning|error" || true
git add -A && git commit -m "feat(orchestrator): publish node outputs to the blackboard + fold/rehydrate (blackboard)"
```

---

## Task 5: Read path — resolve deps + `assemble_prompt` context section

**Files:**
- Modify: `crates/orchestrator/src/agent/prompt.rs` (`assemble_prompt` + its tests)
- Modify: `crates/orchestrator/src/executor/agent.rs` (`drive_agent` param)
- Modify: `crates/orchestrator/src/executor/fanout.rs` (pass `&[]`)
- Modify: `crates/orchestrator/src/executor/mod.rs` (`resolve_context`, Agent-arm call)

- [ ] **Step 1: Write the failing test** in `tests.rs`:

```rust
/// Cross-role handoff: in A(model) → B(agent, hard-dep A), B's assembled system
/// prompt contains A's output (read from the blackboard), proving the handoff.
#[tokio::test]
async fn agent_prompt_includes_its_dependency_output_from_the_blackboard() {
    use orchestrator_store::{InMemoryContentStore, InMemoryContextStore};
    let content = Arc::new(InMemoryContentStore::new());
    let ctx = Arc::new(InMemoryContextStore::new(content.clone()));
    // A recording gateway captures the exact request B sends; its system prompt
    // must contain A's canned output text.
    let (gw, calls) = recording_gateway().await;
    let graph = Graph {
        nodes: vec![
            Node { id: NodeId("A".into()), kind: model_call("c", "plan"), deps: vec![] },
            agent_node_with_deps("B", "a", "refine", vec![Dep::hard("A")]),
        ],
    };
    Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(ToolRegistry::default()))
        .with_content_store(content)
        .with_context_store(ctx)
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    // The recording gateway logs (model, first-user-message); assert B's request
    // carried A's output. NOTE: recording_gateway records the FIRST user message;
    // the dependency context lands in the SYSTEM prompt. If the harness does not
    // expose system, assert via a scripted gateway that echoes system, OR extend
    // the recorded tuple. Simplest: use a gateway that fails if system lacks the
    // marker (see Step 1b).
    let _ = calls;
}
```

> **Step 1a — harness note for the implementer:** `recording_gateway` records `(model, first_user_message)` only. To assert the **system** prompt contains A's output, either (a) add a tiny test adapter that returns the system prompt back as its `content` (so `outcome.outputs["B"]["text"]` contains A's output), or (b) extend `RecordedCall`/the recording adapter to also capture `req.system`. Prefer (a) — a local `EchoSystemAdapter` in `tests.rs` whose `chat` returns `ChatResponse { content: Some(req.system.clone().unwrap_or_default()), tool_calls: vec![], model: None, .. }`. Then assert `outcome.outputs["B"]["text"].as_str().contains("canned-response")` (A's output) and does NOT contain a non-dependency's output. Also add an `agent_node_with_deps(id, agent, input, deps)` helper (like `agent_node` but with `deps`).

Rewrite the test to use the echo adapter:

```rust
#[tokio::test]
async fn agent_prompt_includes_its_dependency_output_from_the_blackboard() {
    use orchestrator_store::{InMemoryContentStore, InMemoryContextStore};
    let content = Arc::new(InMemoryContentStore::new());
    let ctx = Arc::new(InMemoryContextStore::new(content.clone()));
    let (gw, _c) = echo_system_gateway().await; // returns req.system as the answer
    let graph = Graph {
        nodes: vec![
            Node { id: NodeId("A".into()), kind: model_call("c", "plan"), deps: vec![] },
            agent_node_with_deps("B", "a", "refine", vec![Dep::hard("A")]),
        ],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(ToolRegistry::default()))
        .with_content_store(content)
        .with_context_store(ctx)
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    let b_text = out.outputs[&NodeId("B".into())]["text"].as_str().unwrap();
    assert!(b_text.contains("canned-response"),
        "B's system prompt (echoed) includes A's blackboard output: {b_text}");
    assert!(b_text.contains("## Context"), "the context section is present: {b_text}");
}
```

Add the helpers to `tests.rs`:

```rust
fn agent_node_with_deps(id: &str, agent: &str, input: &str, deps: Vec<Dep>) -> Node {
    Node { id: NodeId(id.into()),
           kind: NodeKind::Agent { agent: AgentRef(agent.into()), input: serde_json::json!(input) },
           deps }
}
```

For `echo_system_gateway`, add to `test_support.rs` an adapter returning `req.system` as `content` (model `None`), plus `pub async fn echo_system_gateway() -> (Gateway, CallLog)` registered on the single chain `"c"` (mirror `scripted_gateway`'s wiring). Import it in `tests.rs`.

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator agent_prompt_includes_its_dependency 2>&1 | grep -E "error\[|assert|test result"`
Expected: FAIL — `assemble_prompt` has no context param / no `## Context` section yet, so B's system lacks A's output.

- [ ] **Step 3a: `assemble_prompt` context param.** In `prompt.rs`, change the signature to:

```rust
pub fn assemble_prompt(
    registry: &Registry,
    agent: &AgentDefinition,
    context: &[(orchestrator_core::ContextKey, serde_json::Value)],
) -> Result<(String, Vec<ToolDefinition>), OrchestratorError> {
```

After composing body+skills and BEFORE building tools, append the context section ONLY when non-empty (so a no-dep agent's prompt is byte-identical to before):

```rust
    if !context.is_empty() {
        system.push_str("\n\n## Context");
        for (key, value) in context {
            let rendered = match value {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            system.push_str(&format!("\n\n### {}\n{}", key.0, rendered));
        }
    }
```

Update the existing `prompt.rs` unit-test calls to pass `&[]` (three call sites: `assemble_composes...`, `over_budget...`, and any other). Add a new unit test:

```rust
#[test]
fn assemble_appends_a_context_section_when_present() {
    let (reg, agent) = registry();
    let ctx = vec![(orchestrator_core::ContextKey("A".into()), serde_json::json!("PRIOR"))];
    let (system, _tools) = assemble_prompt(&reg, &agent, &ctx).unwrap();
    assert!(system.contains("## Context") && system.contains("### A") && system.contains("PRIOR"));
    // Empty context ⇒ no section (byte-identical to the no-context prompt).
    let (plain, _) = assemble_prompt(&reg, &agent, &[]).unwrap();
    assert!(!plain.contains("## Context"));
}
```

- [ ] **Step 3b: `drive_agent` context param.** In `agent.rs`, add `context: &[(ContextKey, serde_json::Value)]` to `drive_agent`'s signature (after `input`), import `ContextKey`, and change line ~62 to `assemble_prompt(&self.registry, agent, context)?`. Update the three `drive_agent` call sites:
  - `mod.rs` run_node Agent arm → `self.drive_agent(run, &node.id, agent, input, &self.resolve_context(node).await?, fold)`.
  - `fanout.rs` run_map child → add `&[]` before `fold`.
  - `fanout.rs` run_consolidate agent body → add `&[]` before `fold`.

- [ ] **Step 3c: `resolve_context`.** In `mod.rs`, add:

```rust
    /// Resolve a node's dependency context from the blackboard (§8, D2): the
    /// Run-scoped output of each DECLARED dependency, in declared order. Reads are
    /// dependency-scoped (not all-Run) so a resume is replay-stable — a dep's
    /// `ContextWrite` is journaled before this node runs. No store ⇒ empty.
    async fn resolve_context(
        &self,
        node: &orchestrator_core::Node,
    ) -> Result<Vec<(ContextKey, serde_json::Value)>, OrchestratorError> {
        let Some(ctx) = &self.context else { return Ok(Vec::new()) };
        let mut out = Vec::new();
        for dep in &node.deps {
            let key = ContextKey(dep.on.0.clone());
            if let Some(r) = ctx.get(Scope::Run, key.clone()).await? {
                out.push((key, ctx.load(&r).await?));
            }
        }
        Ok(out)
    }
```

- [ ] **Step 4: Run to verify pass + full suite + clippy**

Run: `cargo test -p sensei-orchestrator > /tmp/t.log 2>&1; echo "EXIT=$?"; grep -E "test result:" /tmp/t.log | head -1`
Run: `cargo clippy --workspace --all-targets -- -D warnings > /tmp/c.log 2>&1; echo "CLIPPY=$?"`
Expected: `EXIT=0`, `CLIPPY=0`.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "feat(orchestrator): agent prompts read dependency context from the blackboard (blackboard)"
```

---

## Task 6: Resume + determinism + over-budget acceptance tests

**Files:**
- Modify: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Resume re-spends nothing (acceptance 5).** `A(model) → B(agent)` where B dies at turn 1; resume with the SAME (Arc-shared) journal + context store + a fresh gateway serving only B's final turn. Assert B completes, the resume gateway saw exactly 1 call (B's tail), and B's turn-0 model effect appears in exactly one `EffectRecorded` across both runs (memoized). Model on `seed_in_doubt`-style: seed with a scripted gateway giving B turn-0 a tool_call then script-exhausted; resume with `final_response`. Reuse the `Calc` tool + `tool_agent_registry` pattern. Rehydration must repopulate A's entry so B's prompt is identical.

```rust
#[tokio::test]
async fn resume_rehydrates_the_blackboard_and_respends_nothing() {
    use orchestrator_store::{InMemoryContentStore, InMemoryContextStore};
    let content = Arc::new(InMemoryContentStore::new());
    let ctx = Arc::new(InMemoryContextStore::new(content.clone()));
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![
            Node { id: NodeId("A".into()), kind: model_call("c", "plan"), deps: vec![] },
            agent_node_with_deps("B", "a", "refine", vec![Dep::hard("A")]),
        ],
    };
    // Seed: A completes (+publishes), B's turn 0 (calc) records, turn 1 script-exhausted → fail.
    let (gw1, _c1) = scripted_gateway(vec![
        final_response("A-done"),                                   // A (ModelCall)
        tool_call_response("t1", "calc", "{\"op\":\"add\",\"a\":1,\"b\":1}"), // B turn 0
    ]).await;
    let o1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_registry(tool_agent_registry()).with_tools(calc_tools())
        .with_content_store(content.clone()).with_context_store(ctx.clone())
        .run(run, &graph).await.expect("seed");
    assert!(o1.failed.is_some(), "B fails at turn 1");
    // Resume with a FRESH store sharing the SAME CAS content (persisted blobs) but
    // an EMPTY entries map → forces rehydration to matter.
    let ctx2 = Arc::new(InMemoryContextStore::new(content.clone()));
    let (gw2, calls2) = scripted_gateway(vec![final_response("the answer is 2")]).await;
    let o2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_registry(tool_agent_registry()).with_tools(calc_tools())
        .with_content_store(content).with_context_store(ctx2)
        .start(run, &graph).await.expect("resume");
    assert!(o2.failed.is_none(), "{:?}", o2.failed);
    assert_eq!(calls2.lock().unwrap().len(), 1, "resume re-spent only B's tail turn");
}
```

> Implementer: if B's assembled prompt (with A's rehydrated context) does not match the seed's, the memoized turn-0 hash mismatches → this test fails with `DeterminismViolation`. That failure would PROVE rehydration is required and correct; getting it to pass proves rehydration reproduces the identical prompt. Confirm the pass is real (not vacuous) by asserting `calls2 == 1`.

- [ ] **Step 2: Determinism halt on tampered upstream (acceptance 6).** Seed A→B partial (as above), then rewrite A's `ContextWrite` content into a fresh journal (point it at a DIFFERENT CAS blob with different text — mirror the slice-4 `changed_tool_input...` tamper pattern), resume → B's turn-0 `agent_input_hash` mismatches → `DeterminismViolation`, gateway untouched.

- [ ] **Step 3: Over-budget halt (acceptance 7).** A publishes a large output; B (agent) hard-deps A; the chain's `min_context_window` is small enough that A's context busts it → `PromptOverBudget` (loud), no silent truncation. Model on the slice-2 `agent_node_halts_over_budget...` test (a big-window agent + a tiny window). Assert `outcome.failed` names B with a `PromptOverBudget`-derived message, OR the run errors with `PromptOverBudget` (match the existing over-budget test's exact surfacing).

- [ ] **Step 4: Run all three + full suite + clippy**

Run: `cargo test -p sensei-orchestrator > /tmp/t.log 2>&1; echo "EXIT=$?"; grep -E "test result:" /tmp/t.log | head -1`
Run: `cargo clippy --workspace --all-targets -- -D warnings > /tmp/c.log 2>&1; echo "CLIPPY=$?"`
Expected: `EXIT=0`, `CLIPPY=0`.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "test(orchestrator): blackboard acceptance — resume rehydrate, determinism, over-budget (blackboard)"
```

---

## Task 7: Docs + memory

**Files:**
- Modify: `docs/features/orchestrator/shared-context.md`, `docs/features/orchestrator/README.md`
- Modify: memory index (outside the repo)

- [ ] **Step 1: Flip `shared-context.md`.** Set the status to reflect the wired mechanism (executor-managed implicit writes/reads, dependency-scoped, refs-not-blobs, resume-rehydrated); list deferred (agent-driven tools, active summarize/select, Node/Plan scope, TTL, prior_outputs unification). Update the README status row for shared-context to `Partial (SP-1)` with a one-line mechanism summary.

- [ ] **Step 2: Full workspace gate**

Run: `cargo test --workspace > /tmp/ws.log 2>&1; echo "WS=$?"; grep -Eo "[0-9]+ passed" /tmp/ws.log | awk '{s+=$1} END{print s" passed"}'`
Run: `cargo clippy --workspace --all-targets -- -D warnings > /tmp/c.log 2>&1; echo "CLIPPY=$?"`
Expected: `WS=0`, `CLIPPY=0`.

- [ ] **Step 3: Commit**

```bash
cargo fmt --all
git add -A && git commit -m "docs(orchestrator): shared-context blackboard wired (SP-1 blackboard COMPLETE)"
```

---

## Notes for the implementer

- **Determinism is the load-bearing property.** Reads are dependency-scoped (D2) precisely so a resumed run reproduces byte-identical prompts; if you broaden reads to all-Run entries, the resume test (Task 6.1) will flake with `DeterminismViolation`. Keep reads scoped to `node.deps`.
- **Publish-guard.** The `fold.context` guard in `publish_context` prevents a memoized replay from re-`put`ting (which would collide) — do not remove it. Publish BEFORE `outputs.insert(output)` (or clone) since `insert` moves `output`.
- **No store ⇒ byte-identical** is acceptance 1 and the safety net: every blackboard step early-returns on `self.context.is_none()`, and `assemble_prompt` only adds the `## Context` section when context is non-empty — so all pre-existing agent/model tests stay green.
- **`label()` is exhaustive** — the `ContextWrite` arm (Task 4) is required to compile.
- Pre-commit runs `make lint` (fmt-check + workspace clippy `-D warnings`); run `cargo fmt --all` before each commit.
- **Branch:** this is `feat/sp1-blackboard`, stacked on `feat/sp1-slice4-effects`. Hold the whole implementation until PR #44 merges to `develop`, then rebase onto `develop` before starting.
