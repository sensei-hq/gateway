# SP-3 slice 4B — Planner selector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `PlannerRef::Select` + an injected `PlannerSelector` that picks *which* planner agent (from the `area == "planning"` role library) runs for a goal, with a pure `RulePlannerSelector` and a light-LLM `LlmPlannerSelector`; the choice is journaled (`PlannerSelected`) for resume-stability, and the chosen agent runs the slice-4A journaled-planner path.

**Architecture:** `run_expand`'s new `Select` arm enumerates sorted `planning`-area candidates, calls the injected selector, validates the pick ∈ candidates, journals `PlannerSelected{node, agent}` (+ fires `on_planner_selected`), then drives that agent via an extracted `drive_planner_agent` helper (shared with the 4A `Agent` path). On resume, `fold.selections` reuses the recorded agent (selector not re-invoked). Failures (no candidates / no selector / selector Err / non-candidate pick) → node `Failed` (resumable), reusing 4A's two-tier model.

**Tech Stack:** Rust, `orchestrator-core` (trait + Rule selector + event/types) + `orchestrator` (executor + Llm selector) + `orchestrator-store` (`InMemoryJournal`), `async_trait`, `tokio`, `serde_json`.

**Design spec:** `docs/superpowers/specs/2026-08-13-sp3-planner-selector-design.md` · **Overview:** `docs/superpowers/orchestrator-overview.md`

**Conventions (ops memory):** `cargo fmt --all` before every commit (pre-commit hook = fmt-check + `clippy -D warnings`, runs **no** tests). Always run `cargo test` yourself; read the **real** exit code (never a piped `| tail`/`grep`; multi-filter needs `-- a b`). `NodeId` is private in `graph.rs` — import from `crate::ids` (or `super::*` in a core test); `AgentRef` is `crate::registry::AgentRef`.

---

## File Structure

- `crates/orchestrator-core/src/planner.rs` **(modify)** — `PlannerSelector` trait, `PLANNER_AREA`, `RulePlannerSelector`.
- `crates/orchestrator-core/src/graph.rs` **(modify)** — `PlannerRef::Select` variant (Task 3).
- `crates/orchestrator-core/src/journal.rs` **(modify)** — `JournalEvent::PlannerSelected`.
- `crates/orchestrator-core/src/hooks.rs` **(modify)** — `on_planner_selected`.
- `crates/orchestrator-core/src/lib.rs` **(modify)** — re-exports.
- `crates/orchestrator/src/executor/mod.rs` **(modify)** — `Fold.selections`; `selector` field + `with_planner_selector`; `planner_candidates`; fire `on_planner_selected` in `append`.
- `crates/orchestrator/src/executor/support.rs` **(modify)** — fold `PlannerSelected`.
- `crates/orchestrator/src/executor/expand.rs` **(modify)** — extract `drive_planner_agent`; the `Select` arm.
- `crates/orchestrator/src/executor/selector.rs` **(create)** — `LlmPlannerSelector`.
- `crates/orchestrator/src/executor/tests.rs` **(modify)** — acceptance tests.

---

## Task 1: Core plumbing — selector trait + Rule impl + PlannerSelected event + fold + hook

**Files:**
- Modify: `crates/orchestrator-core/src/planner.rs`, `journal.rs`, `hooks.rs`, `lib.rs`
- Modify: `crates/orchestrator/src/executor/mod.rs` (`Fold.selections`), `support.rs` (fold), `tests.rs` (`label` helper arm)

- [ ] **Step 1: Add the `PlannerSelector` trait + `RulePlannerSelector` (with failing tests first)**

In `crates/orchestrator-core/src/planner.rs`, append the tests (they drive the API):

```rust
#[cfg(test)]
mod selector_tests {
    use super::*;
    use crate::registry::AgentRef;

    #[tokio::test]
    async fn rule_selector_prefers_a_configured_default_when_a_candidate() {
        let s = RulePlannerSelector::new(Some(AgentRef("beta".into())));
        let cands = vec![AgentRef("alpha".into()), AgentRef("beta".into())];
        let got = s.select(&serde_json::json!({}), &cands).await.unwrap();
        assert_eq!(got, AgentRef("beta".into()));
    }

    #[tokio::test]
    async fn rule_selector_falls_back_to_first_candidate_when_default_absent_or_noncandidate() {
        // default not among candidates → first (candidates arrive sorted).
        let s = RulePlannerSelector::new(Some(AgentRef("ghost".into())));
        let cands = vec![AgentRef("alpha".into()), AgentRef("beta".into())];
        assert_eq!(s.select(&serde_json::json!({}), &cands).await.unwrap(), AgentRef("alpha".into()));
        // no default → first.
        let s2 = RulePlannerSelector::new(None);
        assert_eq!(s2.select(&serde_json::json!({}), &cands).await.unwrap(), AgentRef("alpha".into()));
    }

    #[tokio::test]
    async fn rule_selector_errors_on_empty_candidates() {
        let s = RulePlannerSelector::new(None);
        assert!(s.select(&serde_json::json!({}), &[]).await.is_err());
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p sensei-orchestrator-core --lib selector_tests` — Expected: FAIL to compile (`PlannerSelector`/`RulePlannerSelector` missing).

- [ ] **Step 3: Implement the trait + `PLANNER_AREA` + `RulePlannerSelector`**

In `crates/orchestrator-core/src/planner.rs`, add (below the existing `Planner` trait):

```rust
use crate::registry::AgentRef;

/// The agent `area` marking a candidate planner for `PlannerRef::Select` (§4.4).
pub const PLANNER_AREA: &str = "planning";

/// Picks WHICH planner agent runs for a goal, from the candidate library (agents
/// whose `area == PLANNER_AREA`). Injected on the executor like [`Planner`]; slice 4B.
#[async_trait::async_trait]
pub trait PlannerSelector: Send + Sync {
    /// Choose one of `candidates` (the sorted planner library) for `goal`. Returning
    /// `Err`, or an agent not in `candidates`, is a node-level failure the executor
    /// maps to `Failed` — never a panic.
    async fn select(
        &self,
        goal: &serde_json::Value,
        candidates: &[AgentRef],
    ) -> Result<AgentRef, OrchestratorError>;
}

/// A pure, deterministic selector: the configured `default` when it is a candidate,
/// else the first candidate (candidates arrive sorted by name). Goal-independent.
pub struct RulePlannerSelector {
    default: Option<AgentRef>,
}
impl RulePlannerSelector {
    pub fn new(default: Option<AgentRef>) -> Self {
        Self { default }
    }
}
#[async_trait::async_trait]
impl PlannerSelector for RulePlannerSelector {
    async fn select(
        &self,
        _goal: &serde_json::Value,
        candidates: &[AgentRef],
    ) -> Result<AgentRef, OrchestratorError> {
        if let Some(d) = &self.default
            && candidates.contains(d)
        {
            return Ok(d.clone());
        }
        candidates.first().cloned().ok_or_else(|| {
            OrchestratorError::RegistryLoad("no planner candidates to select from".into())
        })
    }
}
```

(Existing `planner.rs` imports `OrchestratorError`; `async_trait` is already used by the `Planner` trait.)

- [ ] **Step 4: Add the `PlannerSelected` journal event**

In `crates/orchestrator-core/src/journal.rs`, add to `enum JournalEvent` (after `PlanExpanded`):

```rust
    /// The planner-selection decision (SP-3 s4B): node `node` selected planner
    /// `agent`. Journaled BEFORE driving the planner, so a mid-plan resume reuses the
    /// same planner — the memo for the selection (symmetric with `PlanExpanded`).
    PlannerSelected { node: NodeId, agent: crate::registry::AgentRef },
```

- [ ] **Step 5: Add the `on_planner_selected` hook**

In `crates/orchestrator-core/src/hooks.rs`, add the import `use crate::registry::AgentRef;` and the method (after `on_plan_expanded`):

```rust
    async fn on_planner_selected(&self, _run: RunId, _node: &NodeId, _agent: &AgentRef) {}
```

- [ ] **Step 6: `Fold.selections` + fold the event + fire the hook + `label` arm + re-exports**

In `crates/orchestrator/src/executor/mod.rs`, add to `struct Fold` (after `expansions`):

```rust
    /// Planner selections folded from `PlannerSelected` (§4.5). On resume the `Select`
    /// arm reuses the recorded agent — the selector is NOT re-invoked.
    selections: std::collections::HashMap<NodeId, orchestrator_core::AgentRef>,
```

In `crates/orchestrator/src/executor/support.rs`, in `fold_journal`'s match, add (before `_ => {}`):

```rust
            JournalEvent::PlannerSelected { node, agent } => {
                fold.selections.insert(node.clone(), agent.clone());
            }
```

In `crates/orchestrator/src/executor/mod.rs`, in `append`'s hook-match (after the `PlanExpanded` arm):

```rust
                JournalEvent::PlannerSelected { node, agent } => {
                    h.on_planner_selected(run, node, agent).await
                }
```

In `crates/orchestrator/src/executor/tests.rs`, add a `label` helper arm for the new event (keep the exhaustive match compiling):

```rust
        JournalEvent::PlannerSelected { node, agent } => {
            format!("PlannerSelected({}->{})", node.0, agent.0)
        }
```

In `crates/orchestrator-core/src/lib.rs`, extend the `pub use planner::{…}` re-export line to add `PLANNER_AREA, PlannerSelector, RulePlannerSelector`.

**Exhaustiveness note:** adding the `PlannerSelected` variant forces every *exhaustive* `JournalEvent` match to grow an arm. The known ones are handled above (`fold_journal` in support.rs, the `label` helper in tests.rs). If the compiler flags any OTHER exhaustive `match` on `JournalEvent` (the `append` hook match has a `_ => {}` and does not force one), add the minimal arm and note it in your report.

- [ ] **Step 7: Add a `PlannerSelected` roundtrip test**

In `crates/orchestrator-core/src/journal.rs` `mod tests`:

```rust
    #[test]
    fn planner_selected_event_roundtrips() {
        let e = JournalEvent::PlannerSelected {
            node: NodeId("e".into()),
            agent: crate::registry::AgentRef("planner".into()),
        };
        let s = serde_json::to_string(&e).unwrap();
        match serde_json::from_str::<JournalEvent>(&s).unwrap() {
            JournalEvent::PlannerSelected { node, agent } => {
                assert_eq!(node, NodeId("e".into()));
                assert_eq!(agent.0, "planner");
            }
            other => panic!("expected PlannerSelected, got {other:?}"),
        }
    }
```

- [ ] **Step 8: Run all Task-1 tests + confirm the executor crate still compiles**

Run: `cargo test -p sensei-orchestrator-core --lib -- selector_tests planner_selected_event_roundtrips` — Expected: PASS.
Run: `cargo test -p sensei-orchestrator -- expand_` — Expected: slice-4A `expand_*` tests still PASS (additive). Verify real exit 0.

- [ ] **Step 9: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-core/src/planner.rs crates/orchestrator-core/src/journal.rs \
        crates/orchestrator-core/src/hooks.rs crates/orchestrator-core/src/lib.rs \
        crates/orchestrator/src/executor/mod.rs crates/orchestrator/src/executor/support.rs \
        crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-3 s4B (1/5) — PlannerSelector trait + RulePlannerSelector + PlannerSelected event/fold/hook"
```

---

## Task 2: Extract `drive_planner_agent` (behavior-preserving refactor)

**Files:**
- Modify: `crates/orchestrator/src/executor/expand.rs`

**Note:** This lifts the slice-4A `PlannerRef::Agent` block (the pre-check → `drive_agent` under `"{expand}/__plan__"` → `parse_plan`) out of `run_expand` into a helper returning a `PlanOutcome`, so Task 3's `Select` arm can reuse it. Behavior-preserving: the `Agent` path drives, fails, and pauses exactly as before.

- [ ] **Step 1: Add the `PlanOutcome` enum + `drive_planner_agent` helper**

In `crates/orchestrator/src/executor/expand.rs`, add (inside `impl Executor`, above `run_expand`), and add `PlannedGraph`/`AgentStep` are already imported):

```rust
    /// Drive a resolved planner agent's sub-run under `"{node}/__plan__"` and parse its
    /// final answer as a `PlannedGraph`. Returns `Terminal` for the node-level outcomes
    /// (unknown agent / parse error / agent Failed → `Failed`; agent Paused → `Paused`),
    /// so callers (`Agent` and `Select` arms) short-circuit uniformly. A fatal
    /// `drive_agent` error (`DeterminismViolation`, journal) `?`-propagates as a hard halt.
    pub(super) async fn drive_planner_agent(
        &self,
        run: RunId,
        node: &Node,
        agent_ref: &orchestrator_core::AgentRef,
        input: &serde_json::Value,
        fold: &Fold,
    ) -> Result<PlanOutcome, OrchestratorError> {
        if self.registry.agent(&agent_ref.0).is_none() {
            return Ok(PlanOutcome::Terminal(
                self.expand_failed(run, node, format!("expand {} unknown planner agent {}", node.id.0, agent_ref.0)).await?,
            ));
        }
        let plan_node = NodeId(format!("{}/__plan__", node.id.0));
        match self.drive_agent(run, &plan_node, agent_ref, input, &[], fold, None).await? {
            AgentStep::Completed(out) => {
                let text = out.get("text").and_then(|v| v.as_str()).unwrap_or_default();
                match orchestrator_core::parse_plan(text) {
                    Ok(p) => Ok(PlanOutcome::Plan(p)),
                    Err(e) => Ok(PlanOutcome::Terminal(
                        self.expand_failed(run, node, format!("expand {} plan parse: {e:?}", node.id.0)).await?,
                    )),
                }
            }
            AgentStep::Failed(msg) => Ok(PlanOutcome::Terminal(
                self.expand_failed(run, node, format!("expand {} planner agent failed: {msg}", node.id.0)).await?,
            )),
            AgentStep::Paused(r) => Ok(PlanOutcome::Terminal(NodeExec::Paused {
                reason: format!("planner {} paused: {r}", node.id.0),
            })),
        }
    }
```

Add the enum at the bottom of `expand.rs` (module scope, after the `impl`):

```rust
/// The result of driving a planner: a produced plan, or a terminal `NodeExec` the
/// planner sub-run already decided (Failed/Paused).
pub(super) enum PlanOutcome {
    Plan(PlannedGraph),
    Terminal(NodeExec),
}
```

- [ ] **Step 2: Rewire `run_expand`'s `Agent` arm to call the helper**

In `crates/orchestrator/src/executor/expand.rs`, replace the entire `orchestrator_core::PlannerRef::Agent(agent_ref) => { … }` arm (lines ~70–128) with:

```rust
                    orchestrator_core::PlannerRef::Agent(agent_ref) => {
                        match self.drive_planner_agent(run, node, agent_ref, input, fold).await? {
                            PlanOutcome::Plan(p) => p,
                            PlanOutcome::Terminal(ne) => return Ok(ne),
                        }
                    }
```

(The `Injected` arm is unchanged — it still produces a `PlannedGraph` directly. The common tail — `feasible` → `check_expansion_budget` → `PlanExpanded` → `drive_nested` — is unchanged.)

- [ ] **Step 3: Run the slice-4A expand suite (behavior-preserving)**

Run: `cargo test -p sensei-orchestrator -- expand_ planner_agent_ journaled_planner_agent unresolvable_planner_agent` — Expected: all slice-4A `Agent`/`Injected` tests PASS unchanged (produce+splice, invalid, unresolvable, resume, pause, determinism-halt, Map/Consolidate). Verify real exit 0.

- [ ] **Step 4: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/expand.rs
git commit -m "refactor(orchestrator): SP-3 s4B (2/5) — extract drive_planner_agent (shared by Agent + Select)"
```

---

## Task 3: `PlannerRef::Select` + the selection arm + executor wiring

**Files:**
- Modify: `crates/orchestrator-core/src/graph.rs`, `lib.rs` (already re-exports `PlannerRef`)
- Modify: `crates/orchestrator/src/executor/mod.rs` (field/setter/`planner_candidates`), `expand.rs` (Select arm)
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the failing Select tests**

In `crates/orchestrator/src/executor/tests.rs`, append. A `FixedSelector` picks a named agent; a planner agent emits a plan. Helpers reuse Task-4A `planner_registry`/`expand_agent_node` patterns.

```rust
/// A stub selector that always returns a fixed agent (tests the Select flow).
struct FixedSelector(AgentRef);
#[async_trait::async_trait]
impl orchestrator_core::PlannerSelector for FixedSelector {
    async fn select(&self, _goal: &serde_json::Value, _cands: &[AgentRef]) -> Result<AgentRef, OrchestratorError> {
        Ok(self.0.clone())
    }
}
/// A registry with two `planning`-area planner agents (both emit a plan via the gateway).
fn two_planner_registry() -> Arc<Registry> {
    let mk = |name: &str| AgentDefinition {
        name: name.into(), area: "planning".into(), kind: "reasoning".into(),
        chain: Some("c".into()), chains: std::collections::HashMap::new(),
        grants: std::collections::HashMap::new(), tools: vec![], skills: vec![],
        system_prompt: format!("planner {name}"),
    };
    Arc::new(Registry::default().with_agent(mk("alpha")).with_agent(mk("beta")))
}
fn expand_select_node(id: &str, deps: Vec<Dep>) -> Node {
    Node {
        id: NodeId(id.into()),
        kind: NodeKind::Expand { input: serde_json::json!({ "goal": "g" }), planner: orchestrator_core::PlannerRef::Select },
        deps,
    }
}

#[tokio::test]
async fn select_drives_the_chosen_planner_and_journals_the_selection() {
    let plan_json = r#"{"graph":{"nodes":[{"id":"n1","kind":{"ModelCall":{"chain":"c","payload":{"prompt":"n1"}}},"deps":[]}]}}"#;
    let (gateway, _c) = scripted_gateway(vec![final_response(plan_json)]).await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(two_planner_registry())
        .with_planner_selector(Arc::new(FixedSelector(AgentRef("beta".into()))));
    let run = RunId(uuid::Uuid::new_v4());
    let e = NodeId("e".into());
    let graph = Graph { nodes: vec![expand_select_node("e", vec![])] };
    let out = exec.run(run, &graph).await.expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    assert!(out.outputs[&e].get("n1").is_some(), "chosen planner produced+spliced a plan");
    // PlannerSelected{e -> beta} journaled, and the planner ran under "e/__plan__".
    let evs = journal.load(run).await.unwrap();
    assert!(evs.iter().any(|(_, ev)| matches!(ev, JournalEvent::PlannerSelected { node, agent } if node.0=="e" && agent.0=="beta")),
        "PlannerSelected journaled for beta");
}

#[tokio::test]
async fn select_with_no_candidates_fails_the_node() {
    let (gateway, _c) = recording_gateway().await;
    // registry has an agent but NOT area=="planning".
    let reg = Arc::new(Registry::default().with_agent(AgentDefinition {
        name: "coder".into(), area: "coding".into(), kind: "exec".into(),
        chain: Some("c".into()), chains: std::collections::HashMap::new(),
        grants: std::collections::HashMap::new(), tools: vec![], skills: vec![], system_prompt: "c".into(),
    }));
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(reg).with_planner_selector(Arc::new(FixedSelector(AgentRef("x".into()))));
    let graph = Graph { nodes: vec![expand_select_node("e", vec![])] };
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    assert!(matches!(&out.failed, Some((n, _)) if n == &NodeId("e".into())), "no planning agents → Failed: {out:?}");
}

#[tokio::test]
async fn select_with_no_selector_wired_fails_the_node() {
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(two_planner_registry()); // no selector
    let graph = Graph { nodes: vec![expand_select_node("e", vec![])] };
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    assert!(matches!(&out.failed, Some((n, _)) if n == &NodeId("e".into())), "no selector → Failed: {out:?}");
}

#[tokio::test]
async fn select_picking_a_non_candidate_fails_the_node() {
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(two_planner_registry())
        .with_planner_selector(Arc::new(FixedSelector(AgentRef("ghost".into())))); // not a candidate
    let graph = Graph { nodes: vec![expand_select_node("e", vec![])] };
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    assert!(matches!(&out.failed, Some((n, _)) if n == &NodeId("e".into())), "non-candidate pick → Failed: {out:?}");
}
```

- [ ] **Step 2: Run to verify they fail (compile error — Select/with_planner_selector missing)**

Run: `cargo test -p sensei-orchestrator select_drives_the_chosen_planner_and_journals_the_selection` (read the real output, no piping) — Expected: FAIL to compile (`PlannerRef::Select`, `with_planner_selector` missing).

- [ ] **Step 3: Add the `PlannerRef::Select` variant**

In `crates/orchestrator-core/src/graph.rs`, add to `enum PlannerRef` (after `Injected`):

```rust
    /// Registry-driven: the executor's configured `PlannerSelector` picks a planner
    /// agent (from `area == PLANNER_AREA` candidates) for the goal (slice 4B).
    Select,
```

- [ ] **Step 4: Add the executor field, setter, and `planner_candidates`**

In `crates/orchestrator/src/executor/mod.rs`, add the import to the `orchestrator_core::{…}` use list: `PlannerSelector`, `PLANNER_AREA`, `AgentRef`.

Add the field to `struct Executor` (after `planner`):

```rust
    /// The injected selector a `PlannerRef::Select` node uses to pick a planner agent
    /// (slice 4B). `None` ⇒ a `Select` node fails loudly.
    selector: Option<Arc<dyn PlannerSelector>>,
```

In `Executor::new`, add `selector: None,` to the struct literal (near `planner: None,`).

Add the setter (next to `with_planner`):

```rust
    /// Attach the planner selector a `PlannerRef::Select` node uses (slice 4B).
    pub fn with_planner_selector(mut self, selector: Arc<dyn PlannerSelector>) -> Self {
        self.selector = Some(selector);
        self
    }
```

Add the candidate helper to `impl Executor` (near `run_expand`/`check_expansion_budget`):

```rust
    /// The sorted planner library: registry agents whose `area == PLANNER_AREA`, as
    /// `AgentRef`s (sorted by name for deterministic selection).
    fn planner_candidates(&self) -> Vec<AgentRef> {
        let mut c: Vec<AgentRef> = self
            .registry
            .agents()
            .filter(|a| a.area == PLANNER_AREA)
            .map(|a| AgentRef(a.name.clone()))
            .collect();
        c.sort_by(|x, y| x.0.cmp(&y.0));
        c
    }
```

- [ ] **Step 5: Add the `Select` arm in `run_expand`**

In `crates/orchestrator/src/executor/expand.rs`, add to `run_expand`'s `match planner { … }` (a new arm after `Agent`):

```rust
                    orchestrator_core::PlannerRef::Select => {
                        // RESUME: reuse the recorded pick; the selector is NOT re-invoked.
                        let agent = match fold.selections.get(&node.id) {
                            Some(a) => a.clone(),
                            None => {
                                let candidates = self.planner_candidates();
                                if candidates.is_empty() {
                                    return self.expand_failed(run, node, format!("expand {}: no planner agents (area==planning)", node.id.0)).await;
                                }
                                let Some(selector) = &self.selector else {
                                    return self.expand_failed(run, node, format!("expand {}: Select planner but no selector wired", node.id.0)).await;
                                };
                                let a = match selector.select(input, &candidates).await {
                                    Ok(a) => a,
                                    Err(e) => return self.expand_failed(run, node, format!("expand {} selector: {e}", node.id.0)).await,
                                };
                                if !candidates.contains(&a) {
                                    return self.expand_failed(run, node, format!("expand {} selector picked non-candidate {}", node.id.0, a.0)).await;
                                }
                                self.append(run, JournalEvent::PlannerSelected { node: node.id.clone(), agent: a.clone() }).await?;
                                a
                            }
                        };
                        match self.drive_planner_agent(run, node, &agent, input, fold).await? {
                            PlanOutcome::Plan(p) => p,
                            PlanOutcome::Terminal(ne) => return Ok(ne),
                        }
                    }
```

Update the `expand.rs` `use orchestrator_core::{…}` line to add `AgentRef` if the arm references it (it uses `a.0`; `AgentRef` is already the type of `agent`, imported transitively — add it if the compiler asks).

- [ ] **Step 6: Run the Select tests + the resume test**

Add the resume test to `tests.rs`:

```rust
/// Resume reuses the journaled pick; the selector is NOT re-invoked even if it would
/// now pick differently. Mutation-verified: a selector that flips its choice on resume
/// is ignored because PlannerSelected pinned the original.
#[tokio::test]
async fn select_resume_reuses_the_recorded_pick() {
    let plan_json = r#"{"graph":{"nodes":[{"id":"n1","kind":{"ModelCall":{"chain":"c","payload":{"prompt":"n1"}}},"deps":[]}]}}"#;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph { nodes: vec![expand_select_node("e", vec![]), mc_dep("d", Dep::hard("e"))] };
    // Run 1: selector picks beta; beta's plan (n1) runs; then d fails (no 2nd scripted response).
    {
        let (gw, _c) = scripted_gateway(vec![final_response(plan_json), final_response("n1 out")]).await;
        let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
            .with_registry(two_planner_registry())
            .with_planner_selector(Arc::new(FixedSelector(AgentRef("beta".into()))));
        let o1 = exec.run(run, &graph).await.expect("run1");
        assert!(o1.failed.is_some(), "tail d failed: {o1:?}");
    }
    // Run 2: a selector that would pick ALPHA + a fresh recording gateway. Resume must
    // reuse beta (journaled) and re-drive only d.
    let (gw2, calls2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_registry(two_planner_registry())
        .with_planner_selector(Arc::new(FixedSelector(AgentRef("alpha".into()))));
    let o2 = exec2.start(run, &graph).await.expect("resume");
    assert!(o2.failed.is_none(), "resume completes: {o2:?}");
    let recorded2 = calls2.lock().unwrap().clone();
    assert_eq!(recorded2.len(), 1, "resume re-called the gateway only for d (planner not re-run): {recorded2:?}");
    assert_eq!(recorded2[0].1, "d");
    // Exactly one PlannerSelected (beta), from run 1 — resume did not re-select.
    let sel: Vec<String> = journal.load(run).await.unwrap().iter().filter_map(|(_, ev)| match ev {
        JournalEvent::PlannerSelected { agent, .. } => Some(agent.0.clone()), _ => None }).collect();
    assert_eq!(sel, vec!["beta".to_string()], "one selection, beta, never re-selected to alpha: {sel:?}");
}
```

Run: `cargo test -p sensei-orchestrator -- select_ ` — Expected: all 5 Select tests PASS. Then `cargo test -p sensei-orchestrator -- expand_ planner_agent_` — slice-4A tests still green. Verify real exit 0.

- [ ] **Step 7: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-core/src/graph.rs crates/orchestrator/src/executor/mod.rs \
        crates/orchestrator/src/executor/expand.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-3 s4B (3/5) — PlannerRef::Select arm + selector wiring + planner_candidates (journaled pick, resume-reused)"
```

---

## Task 4: `LlmPlannerSelector` (one light reasoning call)

**Files:**
- Create: `crates/orchestrator/src/executor/selector.rs`
- Modify: `crates/orchestrator/src/executor/mod.rs` (add `mod selector;`)
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the failing selector test**

In `crates/orchestrator/src/executor/tests.rs`, append:

```rust
#[tokio::test]
async fn llm_planner_selector_picks_the_named_agent_from_the_menu() {
    use crate::executor::selector::LlmPlannerSelector;
    // Scripted gateway returns the chosen agent name as the response content.
    let (gateway, _c) = scripted_gateway(vec![final_response("beta")]).await;
    let sel = LlmPlannerSelector::new(Arc::new(gateway), "select.chain");
    let cands = vec![AgentRef("alpha".into()), AgentRef("beta".into())];
    let got = sel.select(&serde_json::json!({ "goal": "do X" }), &cands).await.expect("select");
    assert_eq!(got, AgentRef("beta".into()));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p sensei-orchestrator llm_planner_selector_picks_the_named_agent_from_the_menu` — Expected: FAIL to compile (`selector` module / `LlmPlannerSelector` missing).

- [ ] **Step 3: Implement `LlmPlannerSelector`**

Create `crates/orchestrator/src/executor/selector.rs`:

```rust
//! `LlmPlannerSelector` (SP-3 s4B): a light reasoning call that picks one planner agent
//! from the candidate menu for a goal. A black box — its call is NOT a journaled effect;
//! only its result is (`PlannerSelected`), so a mid-plan crash before that event re-runs
//! one cheap call, and after it, resume reuses the recorded agent.

use std::sync::Arc;

use gateway::Gateway;
use kernel::types::capability::Capability;
use kernel::types::request::{InferenceRequest, Message, MessageRole, Payload};
use orchestrator_core::{AgentRef, OrchestratorError, PlannerSelector};

/// Picks a planner via one `gateway.execute` over `chain`; parses the response content
/// as the chosen agent name (validated against `candidates` by the caller).
pub struct LlmPlannerSelector {
    gateway: Arc<Gateway>,
    chain: String,
}

impl LlmPlannerSelector {
    pub fn new(gateway: Arc<Gateway>, chain: impl Into<String>) -> Self {
        Self { gateway, chain: chain.into() }
    }
}

#[async_trait::async_trait]
impl PlannerSelector for LlmPlannerSelector {
    async fn select(
        &self,
        goal: &serde_json::Value,
        candidates: &[AgentRef],
    ) -> Result<AgentRef, OrchestratorError> {
        let menu = candidates.iter().map(|a| format!("- {}", a.0)).collect::<Vec<_>>().join("\n");
        let system = "Choose the single best planner agent for the goal. \
            Answer with ONLY the exact agent name from the list.";
        let user = format!("Goal:\n{goal}\n\nPlanner agents:\n{menu}");
        let req = InferenceRequest {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some(self.chain.clone()),
            payload: Payload::Chat {
                messages: vec![Message::text(MessageRole::User, user)],
                system: Some(system.to_string()),
                max_tokens: None,
                temperature: None,
                tools: Vec::new(),
            },
            budget: None,
            auth: None,
            panel: None,
            consensus: None,
            allow_fallback: true,
            credentials: Default::default(),
        };
        let resp = self
            .gateway
            .execute(&req)
            .await
            .map_err(|e| OrchestratorError::Gateway(e.to_string()))?;
        let name = resp.content.unwrap_or_default().trim().to_string();
        Ok(AgentRef(name))
    }
}
```

In `crates/orchestrator/src/executor/mod.rs`, add `mod selector;` (with the other `mod` lines). A private `mod selector;` is sufficient for the in-crate test path `crate::executor::selector::LlmPlannerSelector` (the `tests` module is a descendant of `executor`, so it can see `executor`'s private `selector` module, and `LlmPlannerSelector` is `pub`). For **external** users to construct it, also add a public re-export — `pub use executor::selector::LlmPlannerSelector;` in `crates/orchestrator/src/lib.rs` (this requires `pub(crate) mod selector;` so the crate root can name it; use `pub(crate) mod selector;` + the lib.rs re-export).

- [ ] **Step 4: Run to verify it passes**

Run: `cargo test -p sensei-orchestrator llm_planner_selector_picks_the_named_agent_from_the_menu` — Expected: PASS. Verify real exit 0. (If `OrchestratorError::Gateway` is not a variant, use the actual gateway-error variant — grep `OrchestratorError::` in the executor for the existing gateway-error mapping and match it; report if you deviate.)

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/selector.rs crates/orchestrator/src/executor/mod.rs \
        crates/orchestrator/src/lib.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-3 s4B (4/5) — LlmPlannerSelector (one light reasoning call over the candidate menu)"
```

---

## Task 5: End-to-end + on_planner_selected + full-suite gate

**Files:**
- Test: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the e2e + hook test**

In `crates/orchestrator/src/executor/tests.rs`, append:

```rust
/// End-to-end: goal → Select (LlmPlannerSelector picks a planner from the menu) → that
/// planner agent emits a plan → executed; PlannerSelected + on_planner_selected +
/// on_plan_expanded all observed.
#[tokio::test]
async fn select_end_to_end_with_llm_selector_and_hook() {
    use crate::executor::selector::LlmPlannerSelector;
    use std::sync::{Arc as StdArc, Mutex};
    let plan_json = r#"{"graph":{"nodes":[{"id":"n1","kind":{"ModelCall":{"chain":"c","payload":{"prompt":"n1"}}},"deps":[]}]}}"#;
    // Scripted gateway: call 1 = selector picks "beta"; call 2 = beta's planner turn → plan;
    // call 3 = the spliced plan node n1.
    let (gateway, _c) = scripted_gateway(vec![
        final_response("beta"),
        final_response(plan_json),
        final_response("n1 out"),
    ]).await;
    let selected = StdArc::new(Mutex::new(Vec::<String>::new()));
    struct Spy(StdArc<Mutex<Vec<String>>>);
    #[async_trait::async_trait]
    impl OrchestratorHooks for Spy {
        async fn on_planner_selected(&self, _run: RunId, node: &NodeId, agent: &AgentRef) {
            self.0.lock().unwrap().push(format!("{}->{}", node.0, agent.0));
        }
    }
    let gw = Arc::new(gateway);
    let exec = Executor::new(gw.clone(), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(two_planner_registry())
        .with_planner_selector(Arc::new(LlmPlannerSelector::new(gw, "c")))
        .with_hooks(Arc::new(Spy(selected.clone())));
    let e = NodeId("e".into());
    let graph = Graph { nodes: vec![expand_select_node("e", vec![])] };
    let out = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    assert!(out.outputs[&e].get("n1").is_some(), "selected planner produced+spliced a plan");
    assert_eq!(*selected.lock().unwrap(), vec!["e->beta".to_string()], "on_planner_selected fired for beta");
}
```

- [ ] **Step 2: Run the e2e test**

Run: `cargo test -p sensei-orchestrator select_end_to_end_with_llm_selector_and_hook` — Expected: PASS. Verify real exit 0.

- [ ] **Step 3: Full-workspace gate (additive + no regressions)**

Run: `cargo test --workspace` — read the REAL exit code directly (not piped). Report exact pass/fail totals; confirm 0 failures (prior baseline ~1008 + the s4B additions).

- [ ] **Step 4: Lint gate**

Run: `cargo fmt --all --check` (exit 0) + `cargo clippy --workspace --all-targets -- -D warnings` (exit 0).

- [ ] **Step 5: Commit (do NOT push — coordinator pushes after the final review)**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): SP-3 s4B (5/5) — select e2e (LlmPlannerSelector + on_planner_selected); full-suite green"
```

---

## Acceptance Criteria → Task map (self-review)

| Spec AC | Task | Test |
|---|---|---|
| 1 Select drives chosen planner + journals PlannerSelected + hook | 3, 5 | `select_drives_the_chosen_planner_and_journals_the_selection`, e2e |
| 2 resume reuses recorded pick (selector not re-invoked) | 3 | `select_resume_reuses_the_recorded_pick` (mutation-verified) |
| 3 RulePlannerSelector deterministic | 1 | `rule_selector_*` (3 tests) |
| 4 LlmPlannerSelector picks from menu | 4 | `llm_planner_selector_picks_the_named_agent_from_the_menu` |
| 5 failure taxonomy (no cands/no selector/err/non-candidate) | 3 | `select_with_no_candidates_fails_the_node`, `select_with_no_selector_wired_fails_the_node`, `select_picking_a_non_candidate_fails_the_node` |
| 6 on_planner_selected fires, replay-suppressed | 5 | e2e Spy (fires once); replay-suppression structural (shared append seam) |
| 7 end-to-end | 5 | `select_end_to_end_with_llm_selector_and_hook` |
| 8 additive (drive_planner_agent behavior-preserving) | 2 | slice-4A `expand_*`/`planner_agent_*` green + workspace suite |

**Coverage note (flag, don't silently drop):** AC5's "selector `Err`" arm has no dedicated test above (the three failure tests cover no-candidates / no-selector / non-candidate). The implementer should add a `select_selector_error_fails_the_node` (a stub selector returning `Err`) in Task 3 if cheap; if skipped, say so in the report.

---

## Post-implementation

- Update `docs/features/orchestrator/` + flip the overview index `SP-3 s4B` row to ✅ done.
- Update the memory topic file + `MEMORY.md`: s4B done; SP-3 remaining = slice 5 (Coordinator + loops-of-graphs + replan).
- Carry-forward deferred (spec §6): goal-category planner rules; multi-vote selection; configurable planner-role; demo selector/library registration.
