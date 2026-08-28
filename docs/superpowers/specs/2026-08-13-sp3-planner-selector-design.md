---
title: SP-3 slice 4B — Planner selector (goal → role-library planner agent)
doctype: design
module: orchestrator
spec: SP-3
status: approved
companion: ./2026-08-13-sp3-planner-agent-design.md (slice 4A — PlannerRef::{Agent,Injected}, the journaled planner-agent drive, feasible, on_plan_expanded); ./2026-08-11-sp2-role-chain-resolution-design.md (the (area,kind) role model + resolve_chain); ../orchestrator-overview.md (§1 shape, §3 decision log)
date: 2026-08-13
---

# SP-3 slice 4B — Planner selector

## 1. Goal

Add `PlannerRef::Select` + an injected **`PlannerSelector`** that picks *which* planner
agent runs for a goal, from the **role-classified library** (registry agents whose
`area == "planning"`). Ships two impls — **`RulePlannerSelector`** (pure, deterministic)
and **`LlmPlannerSelector`** (one light reasoning call). The chosen agent then runs
through the **exact slice-4A `PlannerRef::Agent` path** (journaled `drive_agent` under
`"{expand}/__plan__"` → parse → `feasible` → caps → `PlanExpanded` → `drive_nested`),
resolving its own chain (planner→reasoning) via `resolve_chain`. The *selection* is made
resume-stable by journaling a **`PlannerSelected{node, agent}`** event (the memo for the
selection decision, symmetric with `PlanExpanded`). This completes the planner story:
slice 4A picks *how* a named planner produces a plan; 4B picks *which* planner.

## 2. SP-3 slicing (context)

1. Subgraph ✅ · 2. Branch ✅ · 3. PlanDelta/Expand ✅ · 4. Planner agent — **4A** (core +
   journaled planner) ✅ → **4B this slice** (selector).
5. Coordinator + loops-of-graphs + caps/replan hardening.

## 3. Background & impact review

- **Reuse-ready:** slice 4A's `run_expand` fresh arm already resolves an `AgentRef` and
  drives it via `drive_agent` under `"{expand}/__plan__"` → `parse_plan` → the shared
  `feasible`/`check_expansion_budget`/`PlanExpanded`/`drive_nested` tail. 4B only changes
  *which* `AgentRef` — it reuses that whole drive. The `Planner` trait is injected on the
  `Executor` (`planner: Option<Arc<dyn Planner>>` + `with_planner`); `PlannerSelector`
  mirrors it exactly. `Registry` has `agents()` (slice-4A enumeration accessor) + each
  `AgentDefinition` carries `(area, kind)`. `on_plan_expanded` (fired from `append`,
  replay-suppressed) is the template for `on_planner_selected`. `fold.expansions` +
  `PlanExpanded` are the template for `fold.selections` + `PlannerSelected`.
- **Impact: additive.** New `PlannerRef::Select` variant; `PlannerSelector` trait +
  `RulePlannerSelector` (`orchestrator-core`); `LlmPlannerSelector` (`orchestrator`, holds
  a gateway); `JournalEvent::PlannerSelected`; `Fold.selections`; `on_planner_selected`
  hook; a `Select` arm in `run_expand`; and a **behavior-preserving extraction** of the
  4A planner-agent-drive into a shared helper both `Agent(ref)` and `Select` call. Ripples:
  the `Expand` destructure + any `PlannerRef` match gains the `Select` arm (the enum is
  non-exhaustive-forcing); `#[serde(default)]` keeps `Injected` the default so slice-4A/3
  graphs deserialize unchanged.

## 4. Design

### 4.1 Types (`orchestrator-core`)

```rust
// graph.rs — the third variant
pub enum PlannerRef {
    Agent(AgentRef),
    #[default]
    Injected,
    Select,                       // registry-driven: the configured selector picks a planner
}

// planner.rs — the injected seam (mirrors `Planner`)
/// The role convention marking an agent a candidate planner (§4.4).
pub const PLANNER_AREA: &str = "planning";

#[async_trait::async_trait]
pub trait PlannerSelector: Send + Sync {
    /// Pick one planner agent for `goal` from `candidates` (the sorted planner
    /// library). Returning `Err`, or an agent not in `candidates`, is a node-level
    /// failure the executor maps to `Failed` (never a panic).
    ///
    /// `dispatch` is the only provider access an implementation gets; a selector that
    /// needs no model (see `RulePlannerSelector`) simply ignores it.
    async fn select(
        &self,
        goal: &serde_json::Value,
        candidates: &[AgentRef],
        dispatch: &dyn ModelDispatch,
    ) -> Result<AgentRef, OrchestratorError>;
}
```

> **`dispatch` was added after this spec was written**, by the budget-completeness slice
> (`65dffb8`), which shipped with no design or plan doc of its own — so until this correction
> (2026-08-28) the parameter appeared in NO spec, and this block still showed the two-argument
> form. It is the change that made `LlmPlannerSelector`'s one call a METERED, refusable dispatch
> rather than a direct gateway call: the selector no longer holds a gateway, it is LENT one, and
> `SelectorDispatch` is what makes it the fifth budgeted producer alongside the ReAct turn, the
> `ModelCall` node, the `Map`-item call and the `Consolidate` synthesis. `orchestrator-core`'s
> `planner.rs` is the source of truth for the signature.

### 4.2 Journal + fold + hook

```rust
// journal.rs
/// The planner-selection decision (§4.5): node `node` selected planner `agent`.
/// Journaled BEFORE driving the planner, so a mid-plan crash resumes with the same
/// planner — the memo for the selection, symmetric with `PlanExpanded`.
PlannerSelected { node: NodeId, agent: AgentRef },
```
- `Fold.selections: HashMap<NodeId, AgentRef>`, folded from `PlannerSelected` in
  `fold_journal` (`support.rs`) — the structural analog of `fold.expansions`.
- `OrchestratorHooks::on_planner_selected(&self, run, node: &NodeId, agent: &AgentRef) {}`
  (no-op default), fired from inside `append` when `PlannerSelected` is journaled — can't-
  miss + replay-suppressed for free, so a UX shows *which* planner was chosen per goal.

### 4.3 Execution — the `Select` arm + the extracted drive helper

Extract slice-4A's "resolve an `AgentRef` → drive it → produce a `PlannedGraph` (or a
terminal outcome)" body into a shared helper:

```rust
/// The outcome of driving a planner agent: a produced plan, or a terminal NodeExec
/// (Failed/Paused) already decided by the sub-run.
enum PlanOutcome { Plan(PlannedGraph), Terminal(NodeExec) }

async fn drive_planner_agent(
    &self, run, node, agent_ref: &AgentRef, input, fold,
) -> Result<PlanOutcome, OrchestratorError> {
    // (slice-4A body, verbatim) pre-check agent exists → drive_agent under
    // "{node}/__plan__" → Completed(out) => parse_plan(text) => Plan / expand_failed,
    // Failed => Terminal(Failed), Paused => Terminal(Paused); fatal Err ?-propagates.
}
```

`run_expand`'s fresh (`None`) arm dispatches on `planner`:
- **`Injected`** → the slice-4A trait path → `PlanOutcome::Plan` (or terminal).
- **`Agent(agent_ref)`** → `self.drive_planner_agent(run, node, agent_ref, input, fold)`.
- **`Select`** →
  ```rust
  let agent = match fold.selections.get(&node.id) {
      Some(a) => a.clone(),                              // RESUME: reuse recorded pick
      None => {
          let candidates = self.planner_candidates();    // sorted planning-area AgentRefs
          if candidates.is_empty() {
              return self.expand_failed(run, node, "no planner agents (area==planning)").await;
          }
          let Some(selector) = &self.selector else {
              return self.expand_failed(run, node, "Select planner but no selector wired").await;
          };
          let a = match selector.select(input, &candidates).await {
              Ok(a) => a,
              Err(e) => return self.expand_failed(run, node, format!("selector: {e}")).await,
          };
          if !candidates.contains(&a) {                  // anti-hallucination
              return self.expand_failed(run, node, format!("selector picked non-candidate {}", a.0)).await;
          }
          self.append(run, JournalEvent::PlannerSelected { node: node.id.clone(), agent: a.clone() }).await?;
          a
      }
  };
  // drive the resolved planner (shared helper), then handle its PlanOutcome:
  match self.drive_planner_agent(run, node, &agent, input, fold).await? {
      PlanOutcome::Plan(p)      => p,                 // → the common tail below
      PlanOutcome::Terminal(ne) => return Ok(ne),     // planner Failed/Paused, already journaled
  }
  ```
  The `Agent(agent_ref)` and `Injected` arms produce a `PlanOutcome` the same way, so all
  three arms converge on one `match` → the common tail (`feasible` →
  `check_expansion_budget` → `PlanExpanded` → the outer `drive_nested`) runs unchanged over
  the produced `PlannedGraph`.

`planner_candidates()` = `registry.agents().filter(|a| a.area == PLANNER_AREA)
.map(|a| AgentRef(a.name.clone())).sorted_by(name)` — deterministic. The extraction is
**behavior-preserving for `Agent`/`Injected`** (same drive, same tail); only the new
`Select` arm is added.

### 4.4 The two selectors

- **`RulePlannerSelector`** (`orchestrator-core`, pure): holds an optional default
  `AgentRef`. `select` returns the default **if it is a candidate**, else the **first
  candidate by sorted name** (candidates arrive sorted, so `candidates[0]`). Goal-
  independent, fully deterministic. (Richer goal→planner rules are deferred, §6.)
- **`LlmPlannerSelector { gateway: Arc<Gateway>, registry: Arc<Registry>, chain: String }`**
  (`orchestrator`, needs the gateway so it lives in the executor crate, not zero-I/O core;
  holds an `Arc<Registry>` to render a **capability** menu): one `gateway.execute` over
  `chain` — system = "choose the single best planner for the goal from the menu; answer with
  the exact agent name", user = the goal + a rendered candidate menu **`- {name} ({area}/{kind})`**
  (looked up from the registry per candidate; `AgentDefinition` has no free-text description
  field, so role = `area/kind`). Parses the chosen name into an `AgentRef` (empty content →
  loud `Err`). It is a **black box** — its internal call is *not* a journaled effect; only
  its result is (`PlannerSelected`), so a crash *before* that event re-runs one cheap selector
  call (no divergence — no planner turns exist yet), and after it, resume reuses the recorded
  agent. A malformed/unknown/chatty answer surfaces as the executor's `∉ candidates` → node
  `Failed`.

### 4.5 Determinism / resume

- **Rule** is pure over the config-fenced registry ⇒ recomputes the identical agent on
  resume (a registry hot-reload bumps the config generation → the version fence already
  refuses a cross-generation resume, SP-2 slice 5). **Llm**'s choice is pinned by the
  journaled `PlannerSelected`. Either way the selected agent — and therefore every
  `"{expand}/__plan__"` effect — is **stable across a mid-plan resume**.
- Resume ordering: `run_expand` first checks `fold.expansions` (PlanExpanded present →
  reuse the graph, selector + planner both skipped). Only when PlanExpanded is *absent*
  (crashed mid-plan) does the `Select` arm consult `fold.selections` to reuse the pick and
  replay the planner turns from the memo (no re-select, no re-spend).

### 4.6 Failure taxonomy (reuses 4A's two-tier)

No candidates / no selector wired / selector `Err` / picked-a-non-candidate → node
`Failed` (journaled `NodeFailed`, cascade-skip, resumable, no `PlanExpanded`). Planner
sub-run failure/pause/cap-breach behave exactly as slice 4A (Failed / Paused / hard `Err`).
Nothing new is a hard halt.

## 5. Decisions

- **D1 — bare `PlannerRef::Select`, registry-driven** (approved): candidates = agents with
  `area == "planning"`; adding a focused planner = registering such an agent (no graph
  edit). Rejected: explicit per-node candidate lists (hard-codes the set in the graph);
  per-node role filter (extra surface, not needed yet).
- **D2 — journal `PlannerSelected{node, agent}`** (approved): uniform for Rule + Llm;
  folded into `fold.selections`; resume reuses the pick; `on_planner_selected` hook for the
  flow-tracking UX. Symmetric with `PlanExpanded`. Rejected: journal only the Llm pick
  (asymmetric, two paths); no journaling (a mid-plan Llm re-select diverges/halts).
- **D3 — `PlannerSelector` trait injected on the executor** (`with_planner_selector`),
  mirroring the `Planner` seam. Trait + `RulePlannerSelector` in core; `LlmPlannerSelector`
  in the executor crate (needs the gateway).
- **D4 — `RulePlannerSelector` = configured-default-if-candidate else first-by-sorted-name**
  (approved): deterministic, zero-config-capable. Goal-dependent rules deferred.
- **D5 — anti-hallucination `∉ candidates` → node Failed**: the executor validates the
  selector's pick against the candidate set (guards an Llm returning a made-up/non-planner
  name).
- **D6 — extract `drive_planner_agent`** (behavior-preserving): both `Agent` and `Select`
  reuse the whole 4A journaled-planner drive; selection only resolves *which* agent.

## 6. Deferred (stated)

- Goal-category / keyword → planner rules (a richer `RulePlannerSelector`); multi-vote or
  weighted/consensus selection (blind panel of selectors); selecting **non-planner** roles
  (coder/reviewer/judge are chosen by the *planner* for plan nodes, not by this selector);
  a configurable planner-role (vs the fixed `PLANNER_AREA = "planning"` convention); a
  reusable demo selector + planner-library registration (types are `pub`; wired in tests).
- **Known limitations (record for the Coordinator/replan slice):**
  (a) a **quota-gated selector call** maps to node `Failed`, not the durable auto-resuming
  `Pause` that `ModelCall`/`Agent` get via `classify_gateway_error(AllGated{resume_after})` —
  the `PlannerSelector` trait returns `Result<AgentRef>` (no pause channel), so a timed gate
  degrades to a resumable node failure rather than a scheduled retry; (b) the answer parse is
  **strict exact-name** — a chatty answer degrades to `∉ candidates` → node `Failed` (a node
  retry re-invokes the selector); a lenient "candidate whose name appears in the response"
  parse is a cheap future hardening (no determinism cost — only the result is journaled).

## 7. Acceptance criteria (TDD)

1. **`PlannerRef::Select` drives the selector-chosen planner.** With ≥2 `planning`-area
   agents + a stub selector picking a specific one, `run_expand` drives that agent's
   sub-run (journaled under `"{expand}/__plan__"`), journals `PlannerSelected{node, agent}`
   + fires `on_planner_selected`, produces + splices the plan.
2. **Resume reuses the recorded pick.** A run that journaled `PlannerSelected` then crashed
   mid-plan, resumed with a selector rigged to pick a *different* agent, reuses the
   **journaled** agent (selector NOT re-invoked; planner turns replay from the memo,
   gateway not re-called). Mutation-verified.
3. **`RulePlannerSelector` is deterministic** — configured default (when a candidate) wins;
   else the first candidate by sorted name; goal-independent; repeated calls identical.
4. **`LlmPlannerSelector` picks from a menu** — over a scripted gateway returning an agent
   name, `select` returns that `AgentRef`; its call is not journaled as an effect.
5. **Failure taxonomy** — no `planning` agents → node `Failed`; `Select` with no selector
   wired → `Failed`; selector `Err` → `Failed`; selector picks a non-candidate → `Failed`
   (all resumable, no `PlanExpanded`).
6. **`on_planner_selected` fires once, replay-suppressed** — fired with `(node, agent)`
   when `PlannerSelected` is journaled; a resume over a completed prefix does not re-fire;
   unwired hooks ⇒ byte-identical.
7. **End-to-end** — goal → `Select` (Llm or stub selector picks a planner) → that planner
   agent (uses `validate_plan`, emits a plan) → executed through the test gateway → result;
   `PlannerSelected` + `on_planner_selected` + `on_plan_expanded` all observed.
8. **Additive** — the extracted `drive_planner_agent` is behavior-preserving: all slice-4A
   `Agent`/`Injected` tests pass unchanged; slice-3 `expand_*` tests byte-identical.
