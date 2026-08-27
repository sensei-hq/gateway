---
title: SP-3 slice 2 — Branch node (deterministic conditional)
doctype: design
module: orchestrator
spec: SP-3
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§10 execution graph, §220 node kinds); ./2026-08-12-sp3-subgraph-node-design.md (slice 1 — nested-graph execution, namespace/drive/sink helpers); SP-1 Loop (LoopGate pure predicate, no-gate-journaling)
date: 2026-08-12
---

# SP-3 slice 2 — `Branch` node (deterministic conditional)

## 1. Goal

Add `NodeKind::Branch { on, arms, default }` — a **deterministic conditional** that
tests the output of a predecessor node with a pure predicate and runs exactly one of
N nested-graph arms (or the required `default`). Static control flow this slice
(author-provided arms); it composes with slice 1's `Subgraph` machinery. The decision
is a pure function of memoized state, so resume recomputes the same arm with **no
branch-decision journaling** (the `LoopGate` no-gate-journaling property).

## 2. SP-3 slicing (context)

1. `Subgraph` node (slice 1 — done).
2. **This slice** — `Branch` node (deterministic conditional).
3. runtime `PlanDelta` / `PlanExpanded` (graph splicing) + node/expansion caps.
4. Planner agent (validated plan).
5. Coordinator + loops-of-graphs + caps/replan hardening.

## 3. Background & impact review

- **Reuse-ready primitives:** `LoopGate::should_stop(&self, output) -> bool` (pure
  predicate over a memoized output, no journaling — `graph.rs`); `run_consolidate`
  reads a predecessor's output via `prior_outputs.get(over)` (`fanout.rs`), the exact
  mechanism Branch uses for `on`; slice-1 `run_subgraph` + its `namespace_graph` /
  `sink_outputs` helpers (`executor/subgraph.rs`) drive a nested graph under a path,
  map the outcome → `NodeExec`, and enforce the `max_depth` cap.
- **`run_node` signature** already threads `prior_outputs: &HashMap<NodeId, Value>`
  (Consolidate uses it), so `run_branch` gets it for free.
- **Impact: additive.** New `BranchCond` + `NodeKind::Branch`, a `run_branch` arm,
  recursion in `validate_dag`, and one or two error variants. Existing node kinds,
  `drive`, and all current tests are byte-identical (a new match arm). The two
  `subgraph.rs` helpers are promoted `pub(super)` and reused (no duplication).

## 4. Design

### 4.1 Types (`orchestrator-core`, `graph.rs`)

```rust
/// A pure predicate over a predecessor node's output (mirrors `LoopGate`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BranchCond {
    /// `output[field] == value` (strict JSON equality) — switch on a discriminant.
    FieldEquals(String, serde_json::Value),
    /// `output[field] == true` (strict JSON `true`).
    FieldTrue(String),
    /// `output["text"]` contains this substring.
    TextContains(String),
}

impl BranchCond {
    pub fn matches(&self, output: &serde_json::Value) -> bool {
        match self {
            BranchCond::FieldEquals(f, v) => output.get(f) == Some(v),
            BranchCond::FieldTrue(f) => output.get(f) == Some(&serde_json::Value::Bool(true)),
            BranchCond::TextContains(s) => output
                .get("text")
                .and_then(|v| v.as_str())
                .is_some_and(|t| t.contains(s.as_str())),
        }
    }
}
```

`NodeKind::Branch` (added to the enum):
```rust
    /// A deterministic conditional: test predecessor `on`'s output, run the first
    /// arm whose `BranchCond` matches (else `default`) as a nested graph under
    /// `"{branch}/{label}/…"`. The decision is pure over `on`'s memoized output, so
    /// resume recomputes the same arm — no branch journaling. Static this slice.
    Branch {
        on: NodeId,
        arms: Vec<(BranchCond, Graph)>,
        default: Graph,
    },
```
(`Graph` is `{ nodes: Vec<Node> }` — a `Vec` is a fixed-size pointer, so this recursive
enum is finite without boxing; the `Vec<(BranchCond, Graph)>` and `default: Graph`
are sized. Slice-1 `Subgraph` boxed defensively; Branch does not need to.)

### 4.2 Execution — `run_branch` (`executor/branch.rs`)

Dispatched via a new `run_node` arm `NodeKind::Branch { .. } => self.run_branch(run,
node, prior_outputs, fold).await`:

```rust
async fn run_branch(&self, run, node, prior_outputs, fold) -> Result<NodeExec, _> {
    let NodeKind::Branch { on, arms, default } = &node.kind else { unreachable!() };
    // 1. The decision value = predecessor `on`'s memoized output.
    let value = prior_outputs.get(on).ok_or_else(|| OrchestratorError::BranchInputMissing {
        branch: node.id.clone(), on: on.clone(),
    })?;
    // 2. Pure selection: first matching arm, else default. Label the path.
    let (label, selected): (String, &Graph) = arms.iter().enumerate()
        .find(|(_, (cond, _))| cond.matches(value))
        .map(|(i, (_, g))| (i.to_string(), g))
        .unwrap_or_else(|| ("default".into(), default));
    // 3. Drive the selected arm as a nested graph under "{branch}/{label}".
    let prefix = format!("{}/{}", node.id.0, label);
    // depth cap on `prefix` (path-segment count), namespace_graph(prefix, selected),
    // self.drive, then map paused/failed/Completed(sink_outputs) — identical to run_subgraph.
}
```

`namespace_graph` and `sink_outputs` (currently private in `subgraph.rs`) are promoted
to `pub(super)` and reused verbatim; the depth-cap check and the outcome→`NodeExec`
mapping mirror `run_subgraph`. Only the selected arm's nodes are ever driven/journaled.

### 4.3 Determinism / resume (the crux)

`value` is `on`'s output — a **completed, memoized** node (Branch hard-deps on `on`,
§4.4), stable across resume. `matches` is pure, so the selected `(label, arm)` is
identical on replay → the arm's nodes replay from the memo under the same
`"{branch}/{label}/…"` paths, no re-spend. The **branch decision is never journaled**
(recomputed each drive, like `LoopGate`). Unselected arms are never executed, so they
leave no journal footprint. (A config edit that changes the arms/predicate between
runs is a caller-graph change — same contract as any node; a divergent decision on
resume re-runs the new arm, never silently corrupts.)

### 4.4 Validation (`validate_dag`, recursive)

For every `NodeKind::Branch { on, arms, default }`:
- `on` is a **declared node** in the outer graph, **and** `on` is one of the Branch's
  `deps` with a **Hard** edge — guaranteeing the scheduler runs `on` first and that a
  **failed `on` cascade-skips the Branch** (it never decides on a missing output).
  Otherwise loud `InvalidGraph`.
- **Recurse** into each arm's `Graph` and `default` (a nested cycle / dangling dep →
  loud `InvalidGraph`), exactly like `Subgraph`.

### 4.5 Output + propagation

Output = the selected arm's **sink-outputs map** (same shape as `Subgraph`, so a
Branch composes downstream identically; the arm taken is observable via the journaled
`"{branch}/{label}/…"` paths). A nested node **failure** → Branch `Failed` → outer
cascade-skip (hard); a nested **pause** (in-doubt Mutation / quota) → Branch `Paused`
→ run pauses (no `RunCompleted`) — reused from the slice-1 mapping.

## 5. Decisions

- **D1 — arm body = nested `Graph`** (approved): reuses slice-1 machinery; an arm can
  be a multi-node path; output composes like a `Subgraph`.
- **D2 — `BranchCond` = `FieldEquals` + `FieldTrue` + `TextContains`** (approved (a)):
  mirrors `LoopGate` plus the switch-on-discriminant case; arms evaluated in order,
  first match wins.
- **D3 — required `default`** (approved (b)): every Branch has a well-defined else
  path — no silent dead-end.
- **D4 — `on` is a declared node AND a Hard dep** (approved (c)): schedule-ordered +
  cascade-skip on `on`'s failure.
- **D5 — output = selected arm's sink map** (approved (d)); the chosen arm is
  observable via the journal path, not the output value.
- **D6 — no branch-decision journaling** — pure recomputation over `on`'s memoized
  output (the determinism property).

## 6. Deferred (stated)

- Richer predicates (regex, numeric/ordering compares) — later.
- Branching on the Branch's *own* input rather than a predecessor's output.
- `PlanDelta`-produced branches (slice 3); Loop-over-Branch composition (falls out of
  slices 1/2/5 naturally, no special work).

## 7. Acceptance criteria (TDD)

1. **`BranchCond::matches`** unit tests: `FieldEquals` (match on a discriminant field;
   non-match), `FieldTrue` (true vs false/absent), `TextContains` (substring vs not).
2. **Selection — first match wins.** A Branch whose `on` output has `{status:"b"}` and
   arms `[(FieldEquals("status","a"), A), (FieldEquals("status","b"), B)]` runs arm B
   (its sink output), not A; arms are order-sensitive (an earlier matching arm wins
   over a later one).
3. **Default when no arm matches.** `on` output matches no arm → the `default` arm
   runs (its output present).
4. **Only the selected arm runs.** The journal contains effects under
   `"{branch}/{label}/…"` for the selected arm only; the unselected arms leave no
   `NodeStarted`/`EffectRecorded`.
5. **Determinism / resume.** A run whose Branch selected arm B, resumed, recomputes B
   from `on`'s memoized output and replays B's nodes from the memo (gateway not
   re-called); no branch event is journaled.
6. **Recursive `validate_dag`.** A Branch whose arm/`default` graph has a nested cycle
   → loud `InvalidGraph`; a Branch whose `on` is undeclared, or not a Hard dep of the
   Branch → loud `InvalidGraph`.
7. **`on` failure cascade-skips the Branch.** If `on` fails, the Branch (Hard-dep on
   `on`) is cascade-skipped — never runs, never errors on a missing decision value.
8. **Failure / pause propagation.** A failing node inside the selected arm → Branch
   `Failed` → its outer hard-dependent cascade-skipped; a nested in-doubt Mutation →
   Branch `Paused` → run pauses (no `RunCompleted`).
9. **End-to-end.** A Branch over a predecessor `Agent`/`ModelCall` output drives the
   selected arm (a nested `Agent`/`ModelCall`) through the test gateway to completion;
   the arm's sink output is the Branch's output.
10. **Additive.** Existing node kinds + all current tests are byte-identical.
