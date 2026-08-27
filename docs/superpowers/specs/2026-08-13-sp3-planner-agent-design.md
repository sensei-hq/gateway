---
title: SP-3 slice 4A — Planner agent (self-describing validated plan + journaled ReAct planner)
doctype: design
module: orchestrator
spec: SP-3
status: approved
companion: ./2026-08-13-sp3-plandelta-splice-design.md (slice 3 — Expand node, injected Planner trait, PlanExpanded, drive_nested, caps); ./2026-08-08-sp1-slice2-agent-runtime-design.md (drive_agent ReAct loop, ToolRegistry); ./2026-08-11-sp1-orchestrator-hooks-design.md (OrchestratorHooks fired from append); ../orchestrator-overview.md (§1 shape, §3 decision log)
date: 2026-08-13
---

# SP-3 slice 4A — Planner agent (core + journaled ReAct planner)

## 1. Goal

Turn slice-3's injected `Planner` seam into a real **LLM planner** that produces a
**validated, right-sized, self-describing** plan graph — driven as a **journaled ReAct
agent sub-run** so a mid-plan crash resumes without re-spending the planner's tokens.
Concretely, this slice ships:

1. **Self-describing plan nodes** — `Node.plan: Option<NodePlan{label, description, needs}>`
   so a produced graph is legible for visualization / tracking / resume, and a node's
   `needs` doubles as **planner-selected activation** (the SP-2 Q4 deferral).
2. A pure **feasibility check** + a **`validate_plan`** primitive (parse → `validate_dag`
   → feasibility → caps) surfaced both as a planner tool and as `run_expand`'s
   authoritative gate.
3. The **journaled ReAct planner**: `run_expand` drives a planner **agent** (`drive_agent`)
   under `"{expand}/__plan__"` with **Pure discovery tools** (`list_agents`/`list_chains`/
   `list_skills`/`list_tools`/`validate_plan`); the agent emits the plan as **native serde
   `Graph` JSON**.
4. The **`on_plan_expanded`** hook (previously deferred — "no PlanDelta") so a UX gets the
   labeled plan in real time.

**Slice 4B (next, separate spec):** `PlannerSelector` (deterministic Rule + light-LLM) +
`PlannerRef::Select` — pick *which* planner agent for a goal from the role-classified
library. This slice ships `PlannerRef::{Agent, Injected}` only.

## 2. SP-3 slicing (context)

1. Subgraph (done) · 2. Branch (done) · 3. PlanDelta/Expand (done).
4. **Planner agent** — **4A this slice** (core + journaled planner) → **4B** (selector).
5. Coordinator + loops-of-graphs + caps/replan hardening.

## 3. Background & impact review

- **Reuse-ready:** `drive_agent(run, node_id: &NodeId, agent_ref, input, context, fold,
  phase) -> AgentStep{Completed(Value)|Failed(String)|Paused(String)}` (agent.rs) drives a
  durable ReAct loop under an arbitrary node-id path — exactly what the planner needs (path
  `"{expand}/__plan__"`). `ToolRegistry` + the `Tool` trait (`fn spec()->ToolSpec` + an
  async call) run Pure/Observation/Mutation tools; a tool holds state via `Arc` (e.g.
  `RecordNote{sink: Arc<Mutex<..>>}`), so a discovery tool holds an `Arc<Registry>`
  snapshot. `Graph`/`Node`/`NodeKind` already round-trip serde (they're journaled in
  `PlanExpanded`). `validate_dag` already recurses into Subgraph/Branch arms and rejects
  cycles/dangling deps. Slice-3 `run_expand` already journals `PlanExpanded` + reconstructs
  it on resume; `OrchestratorHooks` fire from inside `append` (can't-miss + replay-
  suppressed).
- **Impact:** additive; core `Node`/`Graph` are **UNCHANGED** (the plan metadata is a
  journaled **side-map**, not a `Node` field — chosen to avoid a 94‑literal churn for
  metadata the executor never reads; §5 D1). Two small ripples: (a) `NodeKind::Expand` gains
  `#[serde(default)] planner: PlannerRef` — the `expand_node` test helper + the `run_expand`
  destructure add `planner` (slice‑3 behavior byte‑identical: default `Injected` = the trait
  path); (b) `JournalEvent::PlanExpanded` gains `#[serde(default)] node_plans:
  HashMap<NodeId, NodePlan>` — ~5 construction sites (slice‑3 ones pass an empty map). New:
  `NodePlan`/`NodeNeeds`/`PlannerRef`/`PlanError`/`PlannedGraph`, `feasible`/`parse_plan`,
  the discovery tools + `validate_plan`, the `run_expand` agent‑backed branch, and
  `OrchestratorHooks::on_plan_expanded`.

## 4. Design

### 4.1 Self-describing plan metadata (`orchestrator-core`, `graph.rs`)

```rust
/// Human-meaningful plan metadata for one node — powers visualization + progress
/// tracking, and declares the node's requirements. Carried in the journaled
/// `PlanExpanded.node_plans` side-map (keyed by the plan's LOCAL node id), NOT a
/// `Node` field (core `Node`/`Graph` stay unchanged; D1).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodePlan {
    pub label: String,                 // short title (UX + progress)
    #[serde(default)]
    pub description: Option<String>,   // rationale / what this step does
    #[serde(default)]
    pub needs: NodeNeeds,              // declared requirements (viz + feasibility + activation)
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NodeNeeds {
    #[serde(default)] pub skills: Vec<String>,   // by registry name
    #[serde(default)] pub tools:  Vec<String>,   // by registry name
    #[serde(default)] pub agents: Vec<String>,   // by registry name
    #[serde(default)] pub self_discover: bool,   // node finds its own tools/skills at runtime
}
```

The metadata rides the journal in **`JournalEvent::PlanExpanded { node, subgraph,
#[serde(default)] node_plans: HashMap<NodeId, NodePlan> }`** — so the plan artifact (graph +
its per-node metadata) is journaled *together* and surfaced together via `on_plan_expanded`.
Keys are the plan's **local** node ids (as emitted), so a UX joins namespaced progress events
(`"{expand}/{id}"`) to plan metadata by stripping the prefix. **`needs` is the planner-
selected activation set** (the SP-2 slice-4 deferral): a node's declared skills/tools are its
activated set; `self_discover: true` falls back to keyword/retrieval activation. Slice A only
*validates + journals* `needs`; wiring it INTO `assemble_prompt`'s activation (which would
need the local↔namespaced join) is a stated follow-up (§6) — the executor's activation path
is untouched here.

### 4.2 Plan encoding — native serde `PlannedGraph` JSON

The planner emits the plan as **JSON that deserializes into a thin wrapper**:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlannedGraph {
    pub graph: Graph,                                    // the executable graph (native serde)
    #[serde(default)]
    pub node_plans: HashMap<NodeId, NodePlan>,           // per-node metadata (local ids)
}
```

`parse_plan(text) = serde_json::from_str::<PlannedGraph>(text)` — near-zero translation (the
`graph` is the exact shape `PlanExpanded` journals). A parse error is a `PlanError::Parse`.

**Reserved id:** `"__plan__"` is reserved for the planner sub-run's path segment. Because a
plan node with local id `X` is namespaced to `"{expand}/X"` at splice time, a plan node
named `__plan__` would collide with the planner sub-run's `"{expand}/__plan__"`. `feasible`
rejects any plan node id equal to `__plan__`.

### 4.3 Feasibility + the `validate_plan` primitive

```rust
pub enum PlanError {
    Parse(String),
    Structural(String),            // from validate_dag (cycle/dangling/etc.)
    UnknownAgent(String), UnknownSkill(String), UnknownTool(String),
    ReservedNodeId(String),        // "__plan__"
    TooManyNodes { count: usize, limit: usize },
}

/// Pure feasibility over a planned graph + a registry snapshot. Structural (via
/// validate_dag, which recurses into Subgraph/Branch/Expand arms) + registry-
/// resolvable refs (Agent nodes + each NodePlan.needs) + reserved-id + a node-count
/// pre-check. Returns ALL errors so the planner can fix them in one pass.
pub fn feasible(
    plan: &PlannedGraph, registry: &Registry, max_nodes: usize,
) -> Result<(), Vec<PlanError>>;
```

Checks: `plan.graph.validate_dag()` (structure); every `NodeKind::Agent{agent}` and every
`plan.node_plans[*].needs.{agents,skills,tools}` resolves in the registry; no node id is
`__plan__`; node count ≤ `max_nodes` (a cheap pre-check — the authoritative per-expansion
cap is still `check_expansion_budget` at splice time). **Chain-existence is best-effort this
slice** (a bad `ModelCall.chain` / resolved agent chain surfaces at runtime as a gateway
error → node `Failed`); a gateway `chains()` accessor for exact feasibility is deferred (§6).

`validate_plan(text) = parse_plan(text).and_then(|p| feasible(&p, registry, max_nodes))`,
returning `Ok` or the `Vec<PlanError>` rendered as structured JSON. It is used **twice**: as
the planner's `validate_plan` **tool** (inner self-correction) and as `run_expand`'s
**authoritative final gate** (defense in depth) before journaling `PlanExpanded`.

### 4.4 The journaled ReAct planner + `PlannerRef`

`NodeKind::Expand` becomes:
```rust
Expand {
    input: serde_json::Value,
    #[serde(default)]                 // absent ⇒ Injected ⇒ slice-3 behavior
    planner: PlannerRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum PlannerRef {
    Agent(AgentRef),                  // journaled ReAct planner agent (this slice)
    #[default]
    Injected,                         // slice-3 injected `Planner` trait (test/deterministic)
    // Select                         // slice 4B — goal-based selection from the role library
}
```

`run_expand` fresh path dispatches on `planner`:
- **`Injected`** → the slice-3 path (`self.planner.plan(input)`), unchanged.
- **`Agent(agent_ref)`** → drive the planner as a **journaled sub-run**:
  ```rust
  let plan_node = NodeId(format!("{}/__plan__", node.id.0));
  match self.drive_agent(run, &plan_node, agent_ref, input, &[], fold, None).await? {
      AgentStep::Completed(out) => {
          let text = out.get("text").and_then(|v| v.as_str()).unwrap_or_default();
          let plan = parse_plan(text)?;                      // → expand_failed on Err
          feasible(&plan, &self.registry, self.max_nodes)?;  // → expand_failed on Err(Vec)
          self.check_expansion_budget(&plan.graph)?;         // hard Err on cap breach
          self.append(run, PlanExpanded {
              node: node.id.clone(), subgraph: plan.graph.clone(), node_plans: plan.node_plans,
          }).await?;
          plan.graph
      }
      AgentStep::Failed(msg)  => return self.expand_failed(run, node, format!("planner agent failed: {msg}")).await,
      AgentStep::Paused(r)    => return Ok(NodeExec::Paused { reason: format!("planner {} paused: {r}", node.id.0) }),
  }
  ```
  then `drive_nested(run, "expand", &node.id.0, &graph, fold)` (slice-3 tail). An
  **unresolvable planner `AgentRef`** (not in the registry) is a config error mapped to
  `expand_failed` (node `Failed`), mirroring slice-3's "no planner wired" — never a hard
  `Err` halt.

The planner agent's ReAct turns + tool calls are journaled Pure effects **under
`"{expand}/__plan__/…"`** (via `drive_agent`'s existing per-turn journaling). The **plan
nodes** are namespaced under `"{expand}/…"` by `drive_nested` — disjoint from the planner
path (guaranteed by the reserved `__plan__` id).

### 4.5 Discovery tools (Pure, `agent/tools.rs`)

New built-in `Tool`s, each holding an `Arc<Registry>` snapshot (all `effect_class: Pure` ⇒
memoized like any Pure tool effect; deterministic given the snapshot):
- `list_agents` — names + `(area,kind)` + description.
- `list_skills` — names + descriptions.
- `list_tools` — names + descriptions + effect_class.
- `list_chains` — the chains the **registry** knows (agent `chain` + `(area,kind)` bindings);
  best-effort menu (the full gateway catalog isn't registry-visible — consistent with §4.3).
- `validate_plan(draft_json)` — runs `parse_plan` + `feasible` (holds `Arc<Registry>` +
  `max_nodes`), returns `Ok` / structured errors.

These ship as **reference/demo registrations** (like the demo catalog/agents) so the e2e
runs; users register their own `planner` agent and grant it these tools.

### 4.6 Determinism / resume

Reuses the slice-3 `PlanExpanded` seam end-to-end:
- **Fresh:** planner sub-run turns journaled → plan → `PlanExpanded` → `drive_nested`.
- **Resume, `PlanExpanded` present** (`fold.expansions.get(node.id)` hit): reuse the
  journaled graph, **skip planning entirely** (the journaled planner turns are inert — never
  replayed, because `run_expand` short-circuits before `drive_agent`).
- **Resume, crashed mid-plan** (no `PlanExpanded`): re-drive the planner agent — **completed
  turns replay from the memo, no re-spend** — finish → `PlanExpanded` → splice.
- **Planner pause** (e.g. quota gate *during planning*) → the Expand node is `Paused` → the
  run pauses resumable (the planner sub-run is itself durable).

### 4.7 Right-sizing (planner prompt policy, not machinery)

The executor already runs a flat linear list natively. Right-sizing lives in the planner
**agent's system prompt / planning skill**: *emit the simplest structure that satisfies the
goal — the ladder is (1) single `Agent`, (2) `Map`+`Consolidate`, (3) `Branch`/`Subgraph`/
`Loop`, (4) `Expand`; use a higher tier only when the task demonstrably needs parallelism,
branching, iteration, or runtime discovery.* No node is forced into a complex shape; the
degenerate plan is one `Agent` node. (A soft `validate_plan` lint flagging needless single-
child nesting is deferred, §6.)

### 4.8 Observability — `on_plan_expanded`

Add to `OrchestratorHooks` (no-op default): `async fn on_plan_expanded(&self, run: RunId,
node: &NodeId, graph: &Graph, node_plans: &HashMap<NodeId, NodePlan>) {}`. Fire it from
inside `append` when a `PlanExpanded` is journaled (same site + pattern as the other hooks)
⇒ can't-miss **and replay-suppressed for free** (a resumed completed prefix doesn't
re-append `PlanExpanded`). Opt-in ⇒ byte-identical when no hooks are wired. A UX receives the
graph **plus its per-node labels** the instant the plan is produced, then the existing
node-lifecycle hooks animate progress over it.

### 4.9 Caps interaction (recursive planning)

A planner-emitted `Expand` charges the run-scoped `max_expansions`/`max_nodes`/`max_depth`
(slice 3, seeded across resume) — the backstop against planner-emits-planner. The planner's
own ReAct turns are bounded by `max_steps` and do **not** charge the expansion budget.

## 5. Decisions

- **D1 — self-describing `NodePlan{label, description, needs}` as a journaled side-map**
  (`PlanExpanded.node_plans: HashMap<NodeId, NodePlan>`, keyed by local node id), **not** a
  `Node` field (approved): legibility for viz/tracking/resume with **zero churn** to core
  `Node`/`Graph` (a `Node.plan` field would ripple to 94 literals for metadata the executor
  never reads). Trade-off: a UX joins namespaced progress events to plan metadata by prefix;
  per-node activation-from-`needs` (deferred, §6) would need that join.
- **D2 — `needs` = planner-selected activation** (skills/tools/agents; `self_discover` for
  runtime discovery). Settles the SP-2 Q4 "planner-selected" deferral at the *schema* level;
  activating from it in `assemble_prompt` is a stated small follow-up (§6).
- **D3 — full palette** (approved): the planner may emit every node kind incl. recursive
  `Expand`; the slice-3 caps are the only backstop.
- **D4 — journaled ReAct planner sub-run** (approved): `drive_agent` under
  `"{expand}/__plan__"` ⇒ durable deliberation, mid-plan crash replays turns from the memo;
  reuses the agent runtime + tool runtime.
- **D5 — `PlannerRef::{Agent, Injected}`** (default `Injected`): agent-backed LLM planner vs
  the slice-3 trait (test/deterministic). One enum change now; `Select` is purely additive in
  4B. Mechanical ripple: slice-3 `Expand{input}` literals get `planner: Injected` +
  behavior byte-identical.
- **D6 — native serde `Graph` JSON** encoding (zero translation); `"__plan__"` reserved.
- **D7 — ReAct planner + Pure discovery tools + `validate_plan`** for inner self-correction;
  `run_expand` re-runs `parse_plan`+`feasible`+caps as the authoritative gate.
- **D8 — feasibility = registry refs + structure + caps; chain-existence best-effort**
  this slice (bad chain → runtime gateway error → node `Failed`).
- **D9 — `on_plan_expanded` hook** for real-time flow tracking; fired from `append`,
  replay-suppressed, opt-in byte-identical.
- **D10 — two-tier failure preserved** (slice 3): planner-agent `Failed` / parse / feasibility
  → node `Failed`; planner-agent `Paused` → node `Paused`; cap breach → hard `Err`.

## 6. Deferred (stated)

- **Slice 4B** — `PlannerSelector` (deterministic Rule + light-LLM, journaled) +
  `PlannerRef::Select` (goal-based selection from the role-classified library).
- **Activate from `needs`** — wire `NodeNeeds` into `assemble_prompt`'s activation (this
  slice validates + journals `needs` but keeps the executor's activation path untouched).
- **First-class consensus** — a `Consensus`/`Vote` node or wiring the gateway's
  `execute_consensus`; today consensus = `Map{blind voters} → Consolidate{evaluator}`.
- **HITL** — `HumanGate`/`AwaitSignal` nodes + human-as-`Agent` (SP-6); the pause/resume +
  event substrate already supports them.
- **Exact chain feasibility** — a gateway `chains()` accessor (vs best-effort registry menu).
- **`validate_plan` needless-nesting lint** (right-sizing nudge); retrieval/semantic
  activation for `self_discover` (SP-7); input-hash fence on `PlanExpanded` (the journaled
  output graph still wins on resume, so unneeded even though the planner now reads `input`).

## 7. Acceptance criteria (TDD)

1. **Schema round-trips + backward-compat.** `NodePlan`/`NodeNeeds`/`PlannedGraph` serde
   round-trip; a `PlanExpanded` JSON without `node_plans` deserializes (empty map); an
   `Expand` JSON without `planner` deserializes (`PlannerRef::Injected`). Slice-3 Expand
   tests pass with the mechanical `planner: Injected` addition + `PlanExpanded` gaining
   `node_plans: {}` (behavior byte-identical); core `Node`/`Graph` literals are untouched.
2. **`feasible` catches each class.** Dangling agent/skill/tool ref → `UnknownAgent/Skill/
   Tool`; duplicate id / nested cycle → `Structural`; a node id `"__plan__"` → `ReservedNodeId`;
   over-`max_nodes` → `TooManyNodes`; all errors returned together; a clean plan → `Ok`.
3. **`validate_plan` primitive/tool.** A malformed draft → structured errors; a good draft →
   `Ok`. As a tool it's `Pure` (memoized: a second identical call in the same run replays).
4. **Journaled planner agent produces + splices a plan.** With `PlannerRef::Agent`, a scripted
   gateway whose planner agent emits a plan JSON (a 2-node line) → `run_expand` parses →
   journals `PlanExpanded` → `drive_nested` runs the plan → sink map. The planner's turns are
   journaled under `"{expand}/__plan__/…"`; the plan nodes under `"{expand}/…"` (disjoint).
5. **Resume mid-plan replays turns (no re-spend).** Crash after ≥1 planner turn but before
   `PlanExpanded`; resume replays the completed planner turn(s) from the memo (gateway not
   re-called for them), finishes planning, produces the plan. (Mutation-verified: breaking the
   memo re-calls the gateway.)
6. **Resume post-`PlanExpanded` never re-plans.** A run that journaled `PlanExpanded` then
   failed downstream, resumed with a planner agent rigged to emit a *different* plan, reuses
   the journaled plan (planner agent not re-invoked).
7. **Planner failure → node Failed → cascade-skip.** A planner agent that fails, or emits
   invalid/infeasible JSON that reaches `run_expand`'s gate → Expand `Failed` → hard-dependent
   skipped, soft-dependent runs; no `PlanExpanded` journaled.
8. **Planner pause → run pauses.** A quota gate during planning → the planner sub-run pauses →
   Expand `Paused` → `RunOutcome.paused` set, no `RunCompleted`, resumable.
9. **`on_plan_expanded` fires once, labeled, replay-suppressed.** Fires once with the graph
   (incl. `NodePlan` labels) when a plan is journaled; a resume over a completed prefix does
   NOT re-fire; unwired hooks ⇒ byte-identical.
10. **Right-sizing — flat single-Agent.** A simple-goal e2e where the planner emits ONE
    `Agent` node (no wrapper) → driven to completion; the produced graph has exactly one node.
11. **Full-palette — Map+Consolidate.** The planner emits a tier-2 `Map{Agent}`→
    `Consolidate{Agent}` plan → driven to completion (the deep-research shape).
12. **End-to-end.** goal → planner agent (uses `list_*` + `validate_plan`) → plan →
    executed through the test gateway → result; `on_plan_expanded` observed; caps respected.
13. **Additive.** With `PlannerRef::Injected` / no planner agent, slice-3 Expand behavior is
    byte-identical (only the mechanical field additions differ).
