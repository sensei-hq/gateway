# SP-4 slice 5 — Exactly-once (idempotency-key core) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Thread the Mutation `idempotency_key` to the tool at execution (so a real tool can send it to an external API for provider-side dedup), support author-supplied keys, and make in-doubt reconcile query by the *journaled* key.

**Architecture:** Two additive default methods on the `Tool` trait — `call_ctx(args, &ToolContext)` (default delegates to `call`) and `idempotency_key(args) -> Option<String>` (default `None`) — plus a `ToolContext{idempotency_key, effect_id}`. The executor computes the effective key (author override, else structural), journals it in the `EffectIntent`, threads it via `call_ctx`, folds `intents` from a set to a `teid→key` map, and has `reconcile_in_doubt` read the journaled key. A demo keyed-store tool + `StatusQueryReconciler` prove provider-side exactly-once. Default (no override) is byte-identical.

**Tech Stack:** Rust workspace crates `sensei-orchestrator` (the `Tool` trait + `Executor`) and `sensei-orchestrator-core` (`ReconcileProvider`/`idempotency_key`, unchanged). Design: `docs/superpowers/specs/2026-08-15-sp4-exactly-once-idempotency-design.md`.

---

## File Structure

- `crates/orchestrator/src/agent/tools.rs` **(modify)** — `ToolContext` + `Tool::call_ctx`/`idempotency_key` defaults; `ToolRegistry::{idempotency_key_of, execute_ctx}`. (NOTE: the `Tool` trait lives here, in the orchestrator crate — NOT core.)
- `crates/orchestrator/src/executor/mod.rs` **(modify)** — `Fold.intents` `HashSet<EffectId>` → `HashMap<EffectId, String>`.
- `crates/orchestrator/src/executor/support.rs` **(modify)** — `fold_journal` folds the `EffectIntent.idempotency_key` into the map.
- `crates/orchestrator/src/executor/agent.rs` **(modify)** — effective-key compute + threading (`mutation_tool_effect`, `record_tool_effect`, `execute_tool_effect`, `reconcile_in_doubt`).
- `crates/orchestrator/src/executor/tests.rs` **(modify)** — executor threading + exactly-once e2e tests.

House rules: `cargo fmt --all` before every commit (pre-commit hook = fmt-check + workspace `clippy -D warnings`, runs NO tests). Verify REAL exit codes — read cargo's `test result:` line, never pipe to `tail`/`grep` to decide pass/fail. Do NOT push (the coordinator pushes after the whole-slice review).

---

## Task 1: `Tool` trait additions + `ToolRegistry` accessors

**Files:**
- Modify: `crates/orchestrator/src/agent/tools.rs` (`Tool` trait ~16-19; `impl ToolRegistry` ~28-52; add `ToolContext`; tests in `mod tests`)

- [ ] **Step 1: Write the failing tests**

In `crates/orchestrator/src/agent/tools.rs` `#[cfg(test)] mod tests`, add:
```rust
    #[test]
    fn idempotency_key_defaults_none_and_override_uses_args() {
        assert_eq!(Calc.idempotency_key(&serde_json::json!({})), None);
        // a tool that derives a domain key from args
        struct Keyed;
        impl Tool for Keyed {
            fn spec(&self) -> ToolSpec {
                ToolSpec {
                    name: "keyed".into(),
                    description: None,
                    input_schema: serde_json::json!({}),
                    effect_class: EffectClass::Mutation,
                    ttl_secs: None,
                    source: None,
                    permissions: Permissions::default(),
                    activation: Activation::default(),
                }
            }
            fn call(&self, _a: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
                Ok(serde_json::json!({}))
            }
            fn idempotency_key(&self, args: &serde_json::Value) -> Option<String> {
                args.get("ref").and_then(|v| v.as_str()).map(str::to_string)
            }
        }
        assert_eq!(
            Keyed.idempotency_key(&serde_json::json!({ "ref": "bk-42" })),
            Some("bk-42".to_string())
        );
    }

    #[test]
    fn call_ctx_defaults_to_call_and_registry_threads_ctx() {
        use orchestrator_core::effect::effect_id;
        let reg = ToolRegistry::default().with_tool(std::sync::Arc::new(Calc));
        let ctx = ToolContext {
            idempotency_key: "k1".into(),
            effect_id: effect_id("n", 0, 0),
        };
        // Calc has no call_ctx override → default delegates to call → same result.
        let via_ctx = reg
            .execute_ctx("calc", serde_json::json!({ "a": 2, "b": 3 }), &ctx)
            .unwrap();
        let via_plain = reg.execute("calc", serde_json::json!({ "a": 2, "b": 3 })).unwrap();
        assert_eq!(via_ctx, via_plain);
        // idempotency_key_of reads the tool; unknown → None.
        assert_eq!(reg.idempotency_key_of("calc", &serde_json::json!({})), None);
        assert_eq!(reg.idempotency_key_of("nope", &serde_json::json!({})), None);
    }
```
(Verify `Calc`'s `call`/output shape and the `effect_id` import path by grepping — `Calc` is a unit struct returning a computed value; match the existing `Calc` test's expectations for `execute("calc", {a,b})`.)

- [ ] **Step 2: Run to verify FAIL**

Run: `cargo test -p sensei-orchestrator --lib idempotency_key_defaults_none_and_override_uses_args` and `... call_ctx_defaults_to_call_and_registry_threads_ctx`
Expected: FAIL to compile (`ToolContext`, `call_ctx`, `idempotency_key`, `execute_ctx`, `idempotency_key_of` undefined). Read the real error, no piping.

- [ ] **Step 3: Add `ToolContext` + the trait defaults**

In `tools.rs`, add `EffectId` to the `orchestrator_core` import (`use orchestrator_core::{..., EffectId, ...};`), then above the `Tool` trait add:
```rust
/// Per-call execution context for a tool (SP-4 s5). Carries the idempotency key the
/// executor journaled in the `EffectIntent` (so a tool can send it to an external API
/// for provider-side dedup) + the effect id for correlation.
pub struct ToolContext {
    pub idempotency_key: String,
    pub effect_id: EffectId,
}
```
Extend the `Tool` trait (keep `spec`/`call` unchanged; do NOT touch `required` from s1):
```rust
    /// Execute with the per-call context (SP-4 s5). Default ignores `ctx` and delegates
    /// to `call` ⇒ existing tools are byte-identical. Override to send
    /// `ctx.idempotency_key` to an external API for provider-side dedup.
    fn call_ctx(
        &self,
        args: serde_json::Value,
        _ctx: &ToolContext,
    ) -> Result<serde_json::Value, OrchestratorError> {
        self.call(args)
    }

    /// Author-supplied idempotency key for THIS call — MUST be PURE over `args` (stable
    /// across resume). Default `None` ⇒ the executor uses the structural key
    /// `sha256(effect_id | args_hash)`. Override for a domain key (booking ref, payment token).
    fn idempotency_key(&self, _args: &serde_json::Value) -> Option<String> {
        None
    }
```

- [ ] **Step 4: Add the `ToolRegistry` accessors**

In `impl ToolRegistry`, add:
```rust
    /// Execute a tool with its per-call context (SP-4 s5). Unknown → loud `UnknownTool`.
    pub fn execute_ctx(
        &self,
        name: &str,
        args: serde_json::Value,
        ctx: &ToolContext,
    ) -> Result<serde_json::Value, OrchestratorError> {
        let tool = self
            .tools
            .get(name)
            .ok_or_else(|| OrchestratorError::UnknownTool(name.to_string()))?;
        tool.call_ctx(args, ctx)
    }

    /// The author-supplied idempotency key for a call, if the tool overrides it (else None
    /// ⇒ the executor uses the structural key). Unknown tool → None.
    pub fn idempotency_key_of(&self, name: &str, args: &serde_json::Value) -> Option<String> {
        self.tools.get(name).and_then(|t| t.idempotency_key(args))
    }
```

- [ ] **Step 5: Run to verify PASS + lint**

Run: `cargo test -p sensei-orchestrator --lib` → the 2 new tests pass + whole lib green (the new defaults change no existing tool). Read the real `test result: ok. N passed; 0 failed`, exit 0. `cargo fmt --all`; confirm `cargo clippy --workspace --all-targets -- -D warnings` exits 0.

- [ ] **Step 6: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/agent/tools.rs
git commit -m "feat(orchestrator): SP-4 s5 (1/4) — Tool::call_ctx + idempotency_key defaults + ToolContext + registry accessors"
```

---

## Task 2: Fold `intents` set → `teid→key` map

**Files:**
- Modify: `crates/orchestrator/src/executor/mod.rs` (`Fold.intents` ~128)
- Modify: `crates/orchestrator/src/executor/support.rs` (`fold_journal` EffectIntent arm ~101)
- Modify: `crates/orchestrator/src/executor/agent.rs` (`mutation_tool_effect` `intents.contains` ~396)

- [ ] **Step 1: Write the failing test**

In `crates/orchestrator/src/executor/support.rs` (or wherever the fold tests live — grep `fold_journal_` in `support.rs`/`tests.rs` and colocate), add:
```rust
    #[test]
    fn fold_captures_the_intent_idempotency_key() {
        use orchestrator_core::{effect::effect_id, JournalEvent, NodeId};
        let eid = effect_id("n1", 0, 1);
        let events = vec![(
            orchestrator_core::Seq(0),
            JournalEvent::EffectIntent {
                node: NodeId("n1".into()),
                effect_id: eid.clone(),
                idempotency_key: "the-key".into(),
                args_hash: "h".into(),
                seq: 0,
            },
        )];
        let fold = fold_journal(&events);
        assert_eq!(fold.intents.get(&eid), Some(&"the-key".to_string()));
    }
```
(Match the actual `fold_journal` signature + `Seq`/`JournalEvent`/`NodeId` import paths — grep an existing `fold_journal_*` test in this file and mirror its construction exactly.)

- [ ] **Step 2: Run to verify FAIL**

Run: `cargo test -p sensei-orchestrator --lib fold_captures_the_intent_idempotency_key`
Expected: FAIL to compile — `fold.intents` is a `HashSet` (`.get(&eid)` returns `Option<&EffectId>`, not the key). Read the real error.

- [ ] **Step 3: Change `Fold.intents` to a map**

In `mod.rs` (~128), change:
```rust
    intents: std::collections::HashSet<EffectId>,
```
to:
```rust
    /// Effect ids that journaled an `EffectIntent` → the journaled idempotency key
    /// (§7.3, SP-4 s5). An id here with no matching `EffectRecorded` is in-doubt on
    /// resume; reconcile queries the provider by THIS key.
    intents: std::collections::HashMap<EffectId, String>,
```

- [ ] **Step 4: Fold the key + fix the `contains` call site**

In `support.rs` `fold_journal` (~101), change the `EffectIntent` arm from
`JournalEvent::EffectIntent { effect_id, .. } => { fold.intents.insert(effect_id.clone()); }`
to:
```rust
            JournalEvent::EffectIntent {
                effect_id,
                idempotency_key,
                ..
            } => {
                fold.intents.insert(effect_id.clone(), idempotency_key.clone());
            }
```
In `agent.rs` `mutation_tool_effect` (~396), change `if ar.fold.intents.contains(teid)` to `if ar.fold.intents.contains_key(teid)`.

- [ ] **Step 5: Run to verify PASS + no regressions**

Run: `cargo test -p sensei-orchestrator --lib fold_captures_the_intent_idempotency_key` → PASS. Then `cargo test -p sensei-orchestrator` → whole suite green (the map is behavior-preserving — same membership; existing in-doubt/reconcile tests unchanged). Read the real `test result:` line, exit 0. `cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings` exit 0.

- [ ] **Step 6: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/mod.rs crates/orchestrator/src/executor/support.rs crates/orchestrator/src/executor/agent.rs
git commit -m "feat(orchestrator): SP-4 s5 (2/4) — fold intents set→map (teid→journaled idempotency_key)"
```

---

## Task 3: Effective-key compute + threading + reconcile reads the journaled key

**Files:**
- Modify: `crates/orchestrator/src/executor/agent.rs` (`mutation_tool_effect` ~388; `record_tool_effect` ~527; `execute_tool_effect` Pure/Obs call ~378; `reconcile_in_doubt` ~425/463)
- Modify: `crates/orchestrator/src/executor/tests.rs` (threading tests)

- [ ] **Step 1: Thread an idempotency key into `record_tool_effect`**

`record_tool_effect` currently signs `(&self, ar, teid, call, args, tih, record: (EffectClass, Option<ObservationMeta>))` and runs `self.tools.execute(&call.name, args)`. Add an `idempotency_key: &str` param and route through `execute_ctx`:
```rust
    async fn record_tool_effect(
        &self,
        ar: &AgentRun<'_>,
        teid: &EffectId,
        call: &ToolCall,
        args: serde_json::Value,
        tih: &str,
        idempotency_key: &str,
        record: (EffectClass, Option<ObservationMeta>),
    ) -> Result<ToolOutcome<serde_json::Value>, OrchestratorError> {
        let (class, observation) = record;
        if let Some(h) = &self.hooks {
            h.on_agent_tool_call(ar.run, ar.node_id, &call.name).await;
        }
        let ctx = crate::agent::tools::ToolContext {
            idempotency_key: idempotency_key.to_string(),
            effect_id: teid.clone(),
        };
        match self.tools.execute_ctx(&call.name, args, &ctx) {
            // ... UNCHANGED from here: Ok(result) => { let result = self.redact(&result); ... }
        }
    }
```
(Keep the existing `Ok`/`Err` arms — including the s2 `let result = self.redact(&result);` — verbatim; only the signature gained `idempotency_key` and `execute` became `execute_ctx` with the `ctx`.)

- [ ] **Step 2: Update the 3 `record_tool_effect` call sites**

- `execute_tool_effect` Pure/Observation branch (~378) — pass the **structural** key (Pure/Obs have no Intent; a non-idempotent tool ignores it):
  ```rust
                self.record_tool_effect(
                    ar, teid, call, args, &tih,
                    &idempotency_key(teid, &tih),
                    (class, observation),
                ).await
  ```
- `mutation_tool_effect` (~414) — compute the **effective** key, journal it, pass it:
  ```rust
        if ar.fold.intents.contains_key(teid) {
            return self.reconcile_in_doubt(ar, teid, call, args, tih).await;
        }
        let key = self
            .tools
            .idempotency_key_of(&call.name, &args)
            .unwrap_or_else(|| idempotency_key(teid, tih));
        self.append(
            ar.run,
            JournalEvent::EffectIntent {
                node: ar.node_id.clone(),
                effect_id: teid.clone(),
                idempotency_key: key.clone(),
                args_hash: tih.to_string(),
                seq: 0,
            },
        )
        .await?;
        self.record_tool_effect(ar, teid, call, args, tih, &key, (EffectClass::Mutation, None))
            .await
  ```
  (Update the doc-comment above the Intent — it currently says the executor "recomputes" the key in reconcile; now reconcile READS it.)
- `reconcile_in_doubt` (~433, ~463) — **read the journaled key** from the fold, use it for reconcile AND the NotApplied re-run:
  ```rust
        let key = ar
            .fold
            .intents
            .get(teid)
            .cloned()
            .unwrap_or_else(|| idempotency_key(teid, tih));
        let verdict = match self.reconcilers.get(&call.name) {
            Some(provider) => provider.reconcile(&key, &args).await?,
            None => ReconcileOutcome::Indeterminate,
        };
        match verdict {
            // Confirmed arm UNCHANGED (redact + split + append + Ok).
            ReconcileOutcome::NotApplied => {
                self.record_tool_effect(ar, teid, call, args, tih, &key, (EffectClass::Mutation, None))
                    .await
            }
            // Indeterminate arm UNCHANGED (uses `key` in the reason).
        }
  ```

- [ ] **Step 3: Write the threading tests**

In `tests.rs`, add (STUDY the existing tool tests — grep `RecordNote`, `ScopedWriter`, `scripted_gateway`, and how a Mutation tool's `EffectIntent` is inspected in the journal; a tiny local tool that records the `ctx.idempotency_key` it received is the cleanest probe):
- `tool_receives_the_journaled_idempotency_key` (AC2/AC3): a local Mutation tool whose `call_ctx` records the received `ctx.idempotency_key` into a shared cell (and delegates output to `call`). Run an agent that calls it; assert the recorded key == the journaled `EffectIntent.idempotency_key` for that effect (grep how tests read `JournalEvent::EffectIntent`).
- `author_supplied_key_is_journaled_and_threaded` (AC1/AC2): a local Mutation tool that overrides `idempotency_key(args)` to return `args["ref"]`; call it with `{"ref":"bk-42"}`; assert the `EffectIntent.idempotency_key` journaled is `bk-42` AND the tool's `call_ctx` received `bk-42`.
- `default_tool_journals_the_structural_key` (AC1 additive): a tool with NO `idempotency_key` override → the journaled `EffectIntent.idempotency_key` equals `orchestrator_core::idempotency_key(teid, tih)` (byte-identical to today).

- [ ] **Step 4: Run + regressions**

Run: `cargo test -p sensei-orchestrator` → the new tests pass + the full suite green (existing reconcile/mutation tests byte-identical — default tools use the structural key; reconcile now reads the same journaled structural key). Read the real `test result:` line, exit 0. `cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings` exit 0.

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/agent.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-4 s5 (3/4) — effective key (author|structural) journaled + threaded via call_ctx; reconcile reads the journaled key"
```

---

## Task 4: Exactly-once e2e (keyed store + StatusQueryReconciler) + full-suite gate

**Files:**
- Modify: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Add the demo keyed-store tool + `StatusQueryReconciler`**

In `tests.rs` (near the other demo tools/reconcilers — grep `RecordNote`/`NoteReconciler`/`ReconcileProvider` to mirror), add a shared "external system" store + an idempotent tool + a status-query reconciler:
```rust
    type Store = std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, serde_json::Value>>>;

    /// Demo Mutation tool with provider-side idempotency: writes to a keyed "external
    /// system" under `ctx.idempotency_key`; re-applying the same key is a no-op that
    /// returns the recorded output. `calls` counts REAL applications (dedup hits don't count).
    struct IdempotentStore { store: Store, calls: std::sync::Arc<std::sync::atomic::AtomicUsize> }
    impl Tool for IdempotentStore {
        fn spec(&self) -> ToolSpec {
            ToolSpec {
                name: "store".into(),
                description: None,
                input_schema: serde_json::json!({}),
                effect_class: EffectClass::Mutation,
                ttl_secs: None,
                source: None,
                permissions: Permissions::default(),
                activation: Activation::default(),
            }
        }
        fn call(&self, _a: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
            Err(OrchestratorError::Tool { tool: "store".into(), message: "needs ctx".into() })
        }
        fn call_ctx(&self, args: serde_json::Value, ctx: &crate::agent::tools::ToolContext)
            -> Result<serde_json::Value, OrchestratorError> {
            let mut s = self.store.lock().unwrap();
            if let Some(existing) = s.get(&ctx.idempotency_key) {
                return Ok(existing.clone()); // provider-side dedup: no second effect
            }
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let out = serde_json::json!({ "stored": args });
            s.insert(ctx.idempotency_key.clone(), out.clone());
            Ok(out)
        }
    }

    struct StatusQueryReconciler { store: Store }
    #[async_trait::async_trait]
    impl orchestrator_core::ReconcileProvider for StatusQueryReconciler {
        async fn reconcile(&self, idempotency_key: &str, _args: &serde_json::Value)
            -> Result<orchestrator_core::ReconcileOutcome, OrchestratorError> {
            match self.store.lock().unwrap().get(idempotency_key) {
                Some(out) => Ok(orchestrator_core::ReconcileOutcome::Confirmed(out.clone())),
                None => Ok(orchestrator_core::ReconcileOutcome::NotApplied),
            }
        }
    }
```

- [ ] **Step 2: Exactly-once e2e tests (AC5)**

Add two tests, mirroring the existing in-doubt-resume idiom (grep `seed_in_doubt`/`in_doubt`/`resume` + how a reconciler is wired via `with_reconcilers`):
- `exactly_once_confirmed_by_key_does_not_double_apply`: run 1 applies the effect (the tool writes `store[key]`, `calls==1`) and journals the `EffectIntent`, but is truncated BEFORE the `EffectRecorded` (in-doubt seed). Resume with a FRESH executor sharing the SAME `store` + a `StatusQueryReconciler` (same store) + the `calls` counter. Assert: the run completes, the reconciler returns `Confirmed` (store has the key) → the effect is recorded WITHOUT re-running (`calls` stays `1`, store has exactly one entry), no `DeterminismViolation`.
- `exactly_once_not_applied_runs_the_effect_once`: seed in-doubt where the side effect did NOT apply before the crash (store EMPTY at resume). Resume → reconciler `NotApplied` → `record_tool_effect` runs the tool once (`calls==1`, store now has the key). Still exactly once.

(For the seed: the cleanest is to run the effect live to its `EffectIntent`+`EffectRecorded`, then truncate the journal to just past the `EffectIntent` — mirror the existing `seed_in_doubt`-style helper. For the NotApplied case, seed the `EffectIntent` WITHOUT letting the tool apply — e.g. a separate seed run whose store is a throwaway, then resume over a fresh empty store; adapt to whatever the existing in-doubt helpers make cheap, and SAY in the report how you seeded each.)

- [ ] **Step 3: Absent-provider still pauses (AC7, preserved)**

Add `author_key_no_provider_still_pauses`: a Mutation tool with an author key, in-doubt on resume, NO `ReconcileProvider` registered → `Indeterminate` → the run pauses (`RunPaused`, `outcome.paused` set, no `RunCompleted`). (Mirror the existing `in_doubt_indeterminate_pauses`-style test; this proves the R3 human-reconciliation path is unchanged.)

- [ ] **Step 4: Determinism / additive + full-suite gate (AC6)**

Run: `cargo test --workspace` — read the REAL exit code + aggregate DIRECTLY (write to a file + `echo $?`; do NOT pipe to tail/grep to decide pass). Confirm 0 failed; report the total (baseline before this slice ~1060 + the s5 additions). Then `cargo fmt --all --check` (exit 0) + `cargo clippy --workspace --all-targets -- -D warnings` (exit 0). Confirm the existing reconcile suite (the SP-1 s4 `in_doubt_*` tests) is byte-identical green (proves additivity).

- [ ] **Step 5: Commit (do NOT push — the coordinator pushes after the whole-slice review)**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): SP-4 s5 (4/4) — exactly-once e2e (Confirmed-by-key no double-apply + NotApplied runs-once) + absent-provider pause; full-suite green"
```

---

## Acceptance Criteria → Task map (self-review)

| Spec AC | Task | Test |
|---|---|---|
| 1 idempotency_key default vs override (journaled) | 1, 3 | `idempotency_key_defaults_none_and_override_uses_args`, `author_supplied_key_is_journaled_and_threaded`, `default_tool_journals_the_structural_key` |
| 2 call_ctx default delegates; override gets ctx | 1, 3 | `call_ctx_defaults_to_call_and_registry_threads_ctx`, `tool_receives_the_journaled_idempotency_key` |
| 3 one key end-to-end (Intent==call_ctx==reconcile) | 3 | `tool_receives_the_journaled_idempotency_key` (+ e2e in 4) |
| 4 fold intents map | 2 | `fold_captures_the_intent_idempotency_key` + existing in-doubt tests |
| 5 exactly-once via status-query | 4 | `exactly_once_confirmed_by_key_does_not_double_apply`, `exactly_once_not_applied_runs_the_effect_once` |
| 6 determinism / additive | 3, 4 | default/structural tests + `cargo test --workspace` (existing reconcile suite byte-identical) |
| 7 absent provider still pauses (R3) | 4 | `author_key_no_provider_still_pauses` |

**Deferred (spec §6, NOT in this plan):** saga/compensation; retry-under-key; real provider API integrations; richer `ToolContext` (deadline/cancellation); author-key purity/version fence.

**Self-review notes:** (1) every spec §7 AC maps to a task. (2) No placeholders — all code shown; the executor tests (Task 3/4) give structure + assertions + a pointer to the existing in-doubt/tool harness to mirror (idiom-heavy, same approach as prior slices). (3) Type consistency: `ToolContext{idempotency_key: String, effect_id: EffectId}`, `Tool::{call_ctx, idempotency_key}`, `ToolRegistry::{execute_ctx, idempotency_key_of}`, `Fold.intents: HashMap<EffectId,String>`, `record_tool_effect(..., idempotency_key: &str, ...)`, and `ReconcileProvider::reconcile(idempotency_key: &str, args: &Value)` (the REAL signature — takes the key + args, verified from `reconcile.rs`) all match across tasks + the real code.
