---
title: sensei-orchestrator — Architecture & Decision Log (living overview)
doctype: overview
module: orchestrator
status: living
date: 2026-08-13
---

# sensei-orchestrator — Architecture & Decision Log

> **Living overview.** The single place to see (1) the current end‑to‑end **shape** of the
> agentic orchestra as‑built, (2) a consolidated **decision log** across every slice
> (what was asked + what was confirmed), and (3) an **index** to the authoritative
> per‑slice specs. Keep it current: when a slice lands or a design decision is made,
> add a line here. The per‑slice design specs remain canonical for detail; this doc is
> the map.
>
> Full original architecture (as of 2026‑08‑06, predates SP‑3): [master spec](specs/2026-08-06-sensei-orchestrator-design.md).
> Feature/status table: [`../features/orchestrator/README.md`](../features/orchestrator/README.md).

---

## 1. The shape — `goal → result`

The orchestrator is a **deterministic engine** (deterministic on *replay*, via the durable
journal — not because its steps are pure) that turns a **goal** into a **result**, emitting
a legible **event stream** the whole way so the run can be tracked, resumed, and (later)
paused for humans.

```
        goal (run input)
          │
          ▼
   ┌──────────────────┐   Slice B (planner selection): pick WHICH planner agent
   │ planner selector │   from the role‑classified library — deterministic rule
   │  (Rule | Llm)    │   OR a light reasoning call (journaled Pure effect).
   └──────────────────┘
          │  selected planner agent (role → reasoning chain via resolve_chain)
          ▼
   ┌──────────────────┐   Slice A: a journaled ReAct planner sub‑run under
   │  planner agent   │   "{expand}/__plan__". Pure discovery tools
   │  (drive_agent)   │   (list_agents/chains/skills/tools) + validate_plan.
   └──────────────────┘   Emits a right‑sized, self‑describing plan (native serde Graph).
          │  plan Graph (nodes carry NodePlan{label, description, needs})
          ▼
   ┌──────────────────┐   validate_dag + feasible() + caps → journal PlanExpanded
   │  splice (Expand) │   {node, subgraph} → reconstructed from journal on resume
   └──────────────────┘   (never re‑planned). This is the "memo for graph structure".
          │
          ▼
   ┌───────────────────────────────────────────────────────────┐
   │  executor / coordinator                                    │
   │  ready‑node scheduler over typed edges (Hard/Soft),        │
   │  cascade‑skip on failure; node kinds:                      │
   │    ModelCall · Agent · Map · Consolidate · Loop ·          │
   │    Subgraph · Branch · Expand                              │
   │  on a DURABLE JOURNAL:                                     │
   │    effect classes Pure / Observation / Mutation;           │
   │    two‑phase + in‑doubt→reconcile for Mutations;           │
   │    effect_id memo ⇒ resume WITHOUT re‑spending tokens;     │
   │    version fence; snapshots; compaction; CAS blob split.   │
   └───────────────────────────────────────────────────────────┘
          │  events: OrchestratorHooks + journal
          │  (run/node/agent/context lifecycle, on_plan_expanded, RunPaused)
          ▼
        result (sink outputs)     ── + real‑time flow‑tracking UX / alerts / HITL
                                     built ON the event stream (consumers, not core)

   under everything: the GATEWAY (sensei‑gateway) — chains, fallback, health gates
   (breaker/cooldown/lockout), budget/quota, panels/consensus, credentials.
   config: the REGISTRY (agents/skills/tools + role→chain bindings) loaded via a
   pluggable ConfigSource, hot‑reloadable, tenant‑agnostic, config‑driven (no DB yet).
```

**Load‑bearing invariants (hold across every slice):**

- **No silent failures.** Every error takes exactly one explicit path (`fail | pause | replan | retry | in_doubt→reconcile`), journaled + surfaced.
- **Deterministic replay / no re‑spend.** A completed effect is memoized by structural `effect_id` + input‑hash; resume replays it with zero gateway calls. Impure steps (LLM calls, the planner, an LLM judge) are made replay‑safe by **journaling their output once**, not by being pure functions.
- **Tenant‑agnostic core.** The gateway *and* orchestrator cores have no tenant concept; multi‑tenancy is a wrapper (one entity per tenant).
- **Config‑driven, no persistence yet.** Everything runs on in‑memory config (`CatalogConfig → assemble → GatewayConfig`; `ConfigSource → Registry`). Durable persistence (`PostgresJournal`, `config_versions`, durable scheduler) is a held‑off **SP‑DATA** layer.
- **Opt‑in policy, light core.** Blackboard/hooks/CAS/reconcilers/planner are injected; unwired ⇒ the light path is byte‑identical.

## 2. The complexity ladder (planner right‑sizing)

The planner emits the **lowest tier that satisfies the goal** — a simple atomic task stays a
single‑level list; nesting/fan‑out/branching/runtime‑expansion appear only when the task
demonstrably needs them.

| Tier | Shape | Node kinds |
|---|---|---|
| 1 | single agent, single task | one `Agent` node (+ `NodePlan`) |
| 2 | parallel multi‑agent + consolidation | `Map{Agent bodies}` → `Consolidate{Agent}` |
| 3 | conditional · nested · iterative | `Branch` · `Subgraph` · `Loop` |
| 4 | runtime sub‑planning | `Expand` (recursive; caps‑bounded) |

Consensus/voting is **not** a tier of its own — it's `Map{blind voters} → Consolidate{evaluator}`
(bias isolation is already guaranteed: Map children run isolated and can't read siblings).

## 3. Decision log (asked → confirmed)

Grouped by phase. Each line is a confirmed decision; the per‑slice spec (§4 index) is canonical.

### SP‑0 — gateway health gates (merged; on `main` via PR #45)
- **Composable policy pipeline**, not subclasses: `AdmissionGate` chain + `RoutingStrategy` + `HealthRecorder` + `SelectionObserver`, composed via a builder.
- **Classification at the adapter boundary** (status/Retry‑After/body preserved); one pure `classify()` drives both in‑walk demote‑to‑tier and next‑request lockout. Deliberate `403 ≠ Authentication`.
- **Tenant‑agnostic health:** the gateway *announces* (`on_lockout`), the **caller persists** (`apply_lockout`/`clear_lockout`); the gateway never persists.
- **Durable pause vocabulary:** `GatewayError::AllGated{resume_after, skipped, human_action}` — timed ⇒ wall‑clock pause; all‑terminal ⇒ fail‑fast with a `HumanAction` hint. Invariant: `AllGated ⟺ every candidate gated AND none hard‑failed`.
- **`ResilienceConfig` at construction** (not a hot‑swappable `GatewayConfig` field), bounded eviction (expired‑only), deterministic keyed jitter (synthetic backoff only; default off).

### SP‑CAT + reference chains (merged)
- Free‑tier **catalog metadata** as optional `ModelConfig.catalog` (kernel, backward‑compatible); **tiers** = curated members ∪ attribute‑derived predicate (deterministic sorted); **pure `catalog::assemble() → GatewayConfig`** (tier‑ref chains → concrete `FallbackChainConfig`s; SP‑0 selection untouched). `cost_band` derived‑from‑pricing (override wins); `Headroom`/`LeastUsed` stub→`Priority` (need live usage).
- Portable **reference tiers + chains** (`research.bulk`, `plan.frontier`, `code.exec`) ship as editable reference data; chains stay user‑owned.
- **⚠️ USER DIRECTIVE:** the app stays **config‑driven, no persistence** — SP‑DATA is a separate, held‑off layer.

### SP‑1 — durable walking skeleton (merged)
- **slice 1 (spine):** 3 crates (`orchestrator-core` zero‑I/O · `orchestrator` engine · `orchestrator-store`). Linear Pure `ModelCall` graph; structural `effect_id(parent_path ‖ loop_iteration ‖ local_index)`; resume/fold memo ⇒ **no token re‑spend**; version fence; terminal‑guard.
- **slice 2 (agent runtime):** in‑memory `Registry` (frontmatter subset) + `NodeKind::Agent`; ReAct loop where **each turn is a Pure `ModelCall` effect** and each Pure tool call a Pure effect ⇒ resume‑without‑re‑spend extends *into* the loop; **two‑registry split** (core specs vs executable `ToolRegistry`); per‑turn window budget (halt‑loud, never truncate).
- **slice 3 (fan‑out · blackboard · CAS):** typed edges `EdgeKind{Hard,Soft}` + `validate_dag`; **ready‑node scheduler**; `Map` (internal bounded fan‑out) + `Consolidate` (+ `Aggregation::Quorum`, `min_viable` starve‑guard); **cascade‑skip on hard edges only** (soft‑dependents still run; failure suppresses `RunCompleted`); CAS split‑on‑output (ref‑or‑inline, lazy materialize); round snapshots (out‑of‑band); compaction primitive.
- **slice 4 (effects):** `Pure / Observation / Mutation` dispatched by `ToolSpec.effect_class`; Observation = memoize‑with‑TTL+provenance; Mutation = **two‑phase `EffectIntent`→`EffectRecorded`** + **`in_doubt→reconcile`** (`Confirmed | NotApplied | Indeterminate→RunPaused`); pause propagates out of a Map child loud (`MapChildPaused`).
- **blackboard wiring:** executor‑managed; a node publishes its output to `Run/node.id`; an Agent reads its **Hard deps' outputs only** (determinism: hard deps are completed+published+stable before the dependent runs). Resume rehydrates from journaled `ContextWrite`s.
- **Loop:** `NodeKind::Loop{body, input, gate, max_iters}`; **pure `LoopGate`** (no gate journaling); refine thread feeds prior text forward; cap‑without‑stop ⇒ best‑effort `{converged:false}`, never a bare fail.
- **Hooks:** `OrchestratorHooks` (no‑op defaults) fired from inside `append` ⇒ can't‑miss **and replay‑suppressed for free**; opt‑in ⇒ byte‑identical.
- **quota→pause:** pure `classify_gateway_error` — only `AllGated{resume_after:Some}` pauses (durable `RunPaused`); everything else fails. **Completes the SP‑1 skeleton.**

### SP‑2 — registry (merged; closes the registry phase)
- **slice 1 (ConfigSource):** `ConfigSource` is *the* extension trait (async, domain objects, no serialization in the contract); `Registry::from_config` is the single assembly point (dup names fail loud); `FilesystemConfigSource` isolates all md/JSON parsing.
- **slice 2 (role→chain):** `resolve_chain(agent, phase)` = per‑phase `chains[phase]` → explicit `chain` → `(area,kind)` binding → loud. **`(area,kind)` role table** with `chain` as optional override; **phase is a node attribute** (`Agent{…, phase}`), not a mid‑loop transition; **multi‑tenancy by composition** (per‑tenant Executor/Gateway/ConfigSource; no `tenant_id` in core).
- **slice 3 (tool permissions):** two‑sided **declaration** model (`ToolSpec.permissions` needs vs `AgentDefinition.grants`); `covers()` predicate; secure‑default deny/empty; central auditable `grants.json`; **runtime enforcement deferred to SP‑4** (declarations only; inert this slice).
- **slice 4 (activation policy, Q4):** **definition‑level** activation (`Activation{Always | OnKeywords}`); `assemble_prompt(…, query)` includes a skill/tool only when active; **query = node input, matched once per run** (determinism); planner‑selected & retrieval‑ranked activation **deferred** (planner‑selected → SP‑3; semantic → SP‑7). Orthogonal to permissions (security boundary) vs activation (prompt disclosure).
- **slice 5 (hot‑reload):** `RegistryHandle` (swappable `Arc<Registry>` + monotonic generation); `reload` is **atomic, validated, last‑good** (load+validate before the write lock); per‑run **pin** at entry; config generation folded into the fence version (`"{base}#cfg{gen}"`) ⇒ one config generation per run.

### SP‑3 — hierarchical executor (COMPLETE on `develop`)
- **slice 1 (Subgraph):** `NodeKind::Subgraph{graph}` driven as a nested DAG in the **same run** via `namespace_graph` + `drive`; output = **sink‑outputs map**; recursive `validate_dag`; **`max_depth`** cap. Fixed two reuse bugs: `finalize_run` moved out of `drive` (no premature `RunCompleted`); `ModelCall` effect‑id node‑scoped (no nested/outer collision).
- **slice 2 (Branch):** `NodeKind::Branch{on, arms, default}` + pure `BranchCond`; runs the first matching arm as a nested graph; **decision is pure over `on`'s memoized output ⇒ no branch journaling**; `on` must be a **Hard dep**. Lesson: any node kind referencing a sibling id (`Consolidate.over`, `Branch.on`) **must** be rewritten in `namespace_graph`.
- **slice 3 (PlanDelta / Expand):** `NodeKind::Expand{input}` + injected **`Planner` trait**; the produced graph is **journaled as `PlanExpanded` and reconstructed on resume — never re‑planned** ("memo for graph structure"). **`drive_nested`** extracted (3rd caller; retires Subgraph/Branch duplication). **Two‑tier failure:** planner/invalid‑plan/no‑planner → node `Failed`; cap breach → hard `Err`. **Caps** `max_expansions`/`max_nodes` seeded from the journal so they **span resume**.
- **slice 4A (planner agent):** `PlannerRef::{Agent, Injected}`; a **journaled ReAct planner sub‑run** at `"{expand}/__plan__"` via `drive_agent` → `PlannedGraph{graph, node_plans}`; pure `feasible()`/`parse_plan()`/`validate_plan` gate; `NodePlan{label, description, needs}` carried as the `PlanExpanded.node_plans` **side‑map**; Pure discovery tools; `on_plan_expanded` hook. Review caught a swallowed `DeterminismViolation` (→ `?`‑propagate) and a **feasibility gap** on nested `MapBody::Agent` refs (→ `check_agent_refs` recurses into Map/Consolidate/Loop bodies).
- **slice 4B (planner selector):** `PlannerRef::Select` + injected **`PlannerSelector`** (`RulePlannerSelector` pure + `LlmPlannerSelector` one‑call capability menu `name (area/kind)`); picks a **`planning`‑area** agent for the goal; **`PlannerSelected{node, agent}` journaled + folded** (resume reuses the pick) + `on_planner_selected` hook; anti‑hallucination `∉candidates`; extracted `drive_planner_agent`.
- **slice 5 (coordinator + loops‑of‑graphs) — CLOSES SP‑3:** `Loop.body: MapBody→**LoopBody**{ModelCall, Agent, Subgraph, Expand}` + `gate: LoopGate→**GateSpec**{Pure, Agent}`. **Subgraph body** = fresh re‑run each iteration (gate decides stop); **Expand body** = plan+execute+**refine** (thread the iteration output into the next planning input — the coordinator core); **gate‑agent** at reserved `"{loop}/{i}/__gate__"` (journaled answer → pure `stop_when`; this is the **graph‑body** convergence path, since a pure gate can't match a nested sink map — pure gate = leaf convergence). **`drive_expand_with`** extracted (shared by `run_expand` + Loop‑Expand); the run‑scoped caps **compose per‑iteration**. **The coordinator** = `Loop{ body: Expand{planner}, gate: Agent{…} }` — plan→execute→gate→replan, native + resume‑safe. Final whole‑slice review caught + fixed **`__gate__` not reserved in `feasible`** (an untrusted planner could emit a `__gate__` node → journal collision → resume `DeterminismViolation`) and an **unproven Expand refine** (input‑ignoring test planners) → added an input‑sensitive refine test.

### Planner brainstorm — SP‑3 slice 4 (design agreed 2026‑08‑13; **BUILT** — slices 4A + 4B merged)
*Split into **Slice A** (core + journaled planner) and **Slice B** (selector); the confirmed decisions below are recorded as the design rationale.*
- **Determinism framing (confirmed):** the engine is deterministic on *replay* (journal), not because the planner/consensus are pure. The planner and any LLM judge are non‑deterministic computations made replay‑safe by journaling their output.
- **Consensus (clarified):** a **blind, independent fan‑out + evaluator** (voters don't share context / see each other's answers — bias isolation). Already expressible as `Map{voters} → Consolidate{evaluator}` (Map guarantees isolation); a pure evaluator is deterministic, an LLM judge is journaled‑replay‑safe. **Not** the planner engine; a first‑class `Consensus`/`Vote` node or wiring the gateway's `execute_consensus` is optional/future.
- **Plan palette (confirmed):** **FULL** — the planner may emit every node kind incl. recursive `Expand`; the slice‑3 caps are the only backstop.
- **Planner mechanism (confirmed):** **ReAct planner with discovery tools** (`list_agents/chains/skills/tools` + `validate_plan`), grounded in what actually exists.
- **Deliberation durability (confirmed):** **journaled planner sub‑run** — `run_expand` drives the planner via `drive_agent` under `"{expand}/__plan__"`; ReAct turns are Pure effects ⇒ a mid‑plan crash **replays turns from the memo**. `PlannerRef::{Agent, Injected}` selects trait‑vs‑agent.
- **Self‑describing plan (confirmed):** `NodePlan{label, description, needs}` carried as a journaled **side‑map** `PlanExpanded.node_plans: HashMap<NodeId, NodePlan>` (keyed by local node id) — **not** a `Node` field (avoids a 94‑literal churn; core `Node`/`Graph` unchanged). `needs{skills,tools,agents,self_discover}` doubles as **planner‑selected activation** (the SP‑2 Q4 deferral); surfaced with the graph via `on_plan_expanded` so the UX/tracking see labels.
- **Right‑sizing (confirmed):** planner emits the lowest tier of the ladder (§2); flat single‑Agent for simple tasks.
- **Agent selection (confirmed):** a **role‑classified agent library** (= the SP‑2 registry) + a **selector** that picks the planner — **deterministic rule OR light‑LLM** (journaled) — → **Slice B**.
- **Decomposition (confirmed):** Slice A = `NodePlan` schema + `feasible()` + `validate_plan` primitive + journaled planner agent + discovery tools + `on_plan_expanded` hook; Slice B = `PlannerSelector` (Rule + Llm) + `PlannerRef::Select`.
- **Observability (confirmed):** the journal + `OrchestratorHooks` + `NodePlan` are the **flow‑tracking feed**; add the (previously‑deferred) **`on_plan_expanded`** hook so a UX gets the labeled plan in real time. **HITL** (`HumanGate`/`AwaitSignal` = SP‑6) and **human‑as‑Agent** are designed‑for consumers on the existing pause/resume + event substrate — deferred.
- **Feasibility (confirmed default):** checks registry‑resolvable refs (agents/skills/tools) + structure (`validate_dag`, distinct/non‑reserved ids, `"__plan__"` reserved) + caps; **chain‑existence is best‑effort in Slice A** (bad chain → runtime gateway error → node `Failed`).

### SP‑4 — permission/effect ENFORCEMENT + isolation (in progress on `develop`)
- **slice 1 (tool permission enforcement):** turns the inert SP‑2‑s3 `Permissions`/`covers()` **declarations** into runtime **authorization**. The gate lives at the single chokepoint `execute_tool_effect` (all Pure/Observation/Mutation calls route through it): a call is denied unless `tool ∈ agent.tools` AND `agent.grants[tool].covers(tool.required(args))`. **`Tool::required(&self,args)->Permissions`** (default = static `spec().permissions`) reports a call's **concrete** needs, so the grant is a *runtime ceiling* (narrower than the tool's max surface) — the load‑time full‑surface `validate` check is **dropped** (D3; a narrow grant is now legal, enforced per‑call). `covers()` hardened: **component‑aware paths** (`/work` ⊄ `/workspace‑secret`, empty‑path reject, `..` reject) + **host wildcards** (`*.example.com`). A denial is recorded as a **Pure `EffectRecorded`** (no tool run; **no `EffectIntent`** for a Mutation) and fed back to the agent as a **terse** tool‑result error (never enumerates the grant → confused‑deputy defense) ⇒ a resume replays the denial from the memo, tool never re‑invoked (mutation‑verified `effect_recorded_count==1`). **Trust boundary (stated):** this AUTHORIZES (stops the agent invoking un‑granted tools / an honest tool being *asked* to exceed the grant); it does NOT CONFINE a tool that under‑reports its `required` — runtime confinement + resource‑cap *killing* = the sandbox (slice 4). Whole‑slice review READY‑TO‑PUSH (single verified chokepoint; in‑doubt reconcile on resume re‑runs the gate ⇒ a revoked grant denies rather than re‑runs the side effect; additive — empty‑permission tools byte‑identical). **NEXT SP‑4 slices (provisional): s2 secret redaction · s3 workspace isolation · s4 sandbox + cred broker · s5 exactly‑once hardening.**
- **slice 2 (secret redaction):** scrub secrets from effect **outputs** before they are journaled or fed back to the agent, via a pure injected **`Redactor`** (`orchestrator-core`) — default **`PatternRedactor`** (curated secret‑shape patterns → `[REDACTED]`: OpenAI `sk‑`/`sk‑proj‑`, Anthropic, AWS, GitHub `gh[opsru]_`/`github_pat_`, Slack, Google, Stripe `sk_live_`, Bearer, PEM, `key=value` assignments; ReDoS‑safe `regex` automata). Opt‑in `Executor::with_redactor` (default off ⇒ **byte‑identical**). **The determinism crux:** redact **at production, before BOTH journaling and the agent/downstream‑return** (a journal‑only scrub would replay `[REDACTED]` on resume while live fed the secret → spurious `DeterminismViolation`); the redactor is **pure** ⇒ live == journaled == replayed, memo replays the redacted value. So the model never sees the secret (anti‑exfiltration) — credential *use* stays the sandbox/broker (s4). **Leaf sites:** the tool result (`record_tool_effect`) + **every model‑output producer via a single shared `model_output` chokepoint** (`dispatch_model_turn` [+`tool_calls` intact] · `ModelCall` node · `Map`‑item · `Consolidate`) + **the reconcile‑in‑doubt `Confirmed` output**; CAS blobs (`split_output` runs *after* redaction), `Map`/`Consolidate` sink maps, and `ContextWrite` inherit transitively. **Two plaintext leaks the reviews caught + fixed:** the Task‑2 review found redaction hit only 1 of 4 model‑output producers (→ `model_output` chokepoint); the **whole‑slice review found the reconcile‑in‑doubt `Confirmed` path** journaled+fed‑back a reconciler's Mutation output **unredacted** — a *silent* durable plaintext write on resume (no memo to fence) — fixed by redacting the Confirmed output (mutation‑verified). **Best‑effort by shape** (§4.4 — misses novel formats; precise known‑value/vault redaction is a future `Redactor` impl). **Carry‑forward:** reversible tokenization/crypto‑shred (SP‑DATA); entropy heuristic; redactor‑version in the fence; input‑side/plan‑structure redaction; a `leak_exec` test‑helper cleanup.
- **slice 5 (exactly-once — idempotency-key core):** make the SP-1-s4 two-phase Mutation mechanism deliver **real provider-side exactly-once** by closing the one gap — the `idempotency_key` was journaled but never reached the tool, so a real tool couldn't send it to an external API to dedupe on. **Additive `Tool` seams:** `call_ctx(args, &ToolContext{idempotency_key, effect_id})` (default → `call`) + `idempotency_key(args) -> Option<String>` (author override, default `None` → structural `sha256(effect_id|args_hash)`); `ToolRegistry::{execute_ctx, idempotency_key_of}`. **Executor:** `mutation_tool_effect` computes the **effective** key (author else structural), journals it in the `EffectIntent`, threads it to the tool via `call_ctx` (the tool sends the SAME key it journaled to its provider); `reconcile_in_doubt` **READS the journaled key** from `fold.intents` (changed `HashSet<EffectId>`→`HashMap<EffectId,String>`, D3) instead of recomputing — so on in-doubt resume the provider is queried by the exact key used at execution (robust for author keys). **Exactly-once proven both ways** (demo keyed "external system" store + `StatusQueryReconciler`): Confirmed-by-key records without re-invoking the tool (a raw `invocations` counter — distinct from the dedup-aware `calls` — pins "tool NOT re-invoked on resume"); NotApplied runs the effect once under the standing Intent (no 2nd Intent); an author-keyed variant proves `bk-77` journaled + reconcile-queried-by-it. **Absent provider → `Indeterminate` → `RunPaused`** (R3 mandatory-human-reconciliation, unchanged). **Additive:** default tools journal the structural key exactly as before, reconcile reads that same key ⇒ byte-identical (the whole SP-1-s4 in-doubt suite green); `ToolRegistry::execute` gated `#[cfg(test)]` (production dispatches only via `execute_ctx` — a call_ctx-bypass footgun retired). **Trust boundary:** makes exactly-once ACHIEVABLE (mechanism + key); true exactly-once still needs the provider to honor the key. **Carry-forward:** saga/compensation; retry-under-key; real provider API integrations; richer `ToolContext`; author-key purity/version fence. **⚡ SP-4 status: s1 ✅ · s2 ✅ · s5 ✅ · s3 workspace isolation (premature — needs real fs tools) + s4 sandbox/cred-broker (real confined tool execution) remain.**

## 4. Index — per‑slice specs (canonical detail)

| Phase / slice | Spec | Status |
|---|---|---|
| Master architecture | [specs/2026-08-06-sensei-orchestrator-design.md](specs/2026-08-06-sensei-orchestrator-design.md) | living (2026‑08‑06 baseline) |
| Features & approach | [specs/2026-08-06-sensei-orchestrator-features-and-approach.md](specs/2026-08-06-sensei-orchestrator-features-and-approach.md) | reference |
| SP‑0 health gates | [../design/selection-policy-pipeline.md](../design/selection-policy-pipeline.md) + `plans/2026-08-06/07-sp0-*` | ✅ merged (main) |
| SP‑CAT catalog | [specs/2026-08-07-sp-cat-catalog-design.md](specs/2026-08-07-sp-cat-catalog-design.md) | ✅ merged |
| Reference chains | [specs/2026-08-07-reference-chains-design.md](specs/2026-08-07-reference-chains-design.md) | ✅ merged |
| SP‑1 s1 spine | [specs/2026-08-08-sp1-orchestrator-spine-design.md](specs/2026-08-08-sp1-orchestrator-spine-design.md) | ✅ merged |
| SP‑1 s2 agent runtime | [specs/2026-08-08-sp1-slice2-agent-runtime-design.md](specs/2026-08-08-sp1-slice2-agent-runtime-design.md) | ✅ merged |
| SP‑1 s3 fan‑out/CAS | [specs/2026-08-09-sp1-slice3-fanout-blackboard-cas-design.md](specs/2026-08-09-sp1-slice3-fanout-blackboard-cas-design.md) | ✅ merged |
| SP‑1 blackboard wiring | [specs/2026-08-10-sp1-blackboard-wiring-design.md](specs/2026-08-10-sp1-blackboard-wiring-design.md) | ✅ merged |
| SP‑1 Loop | [specs/2026-08-10-sp1-loop-node-design.md](specs/2026-08-10-sp1-loop-node-design.md) | ✅ merged |
| SP‑1 s4 effects | [specs/2026-08-10-sp1-slice4-observation-mutation-design.md](specs/2026-08-10-sp1-slice4-observation-mutation-design.md) | ✅ merged |
| SP‑1 hooks | [specs/2026-08-11-sp1-orchestrator-hooks-design.md](specs/2026-08-11-sp1-orchestrator-hooks-design.md) | ✅ merged |
| SP‑1 quota→pause | [specs/2026-08-11-sp1-quota-pause-design.md](specs/2026-08-11-sp1-quota-pause-design.md) | ✅ merged |
| SP‑2 s1 ConfigSource | [specs/2026-08-11-sp2-config-source-design.md](specs/2026-08-11-sp2-config-source-design.md) | ✅ merged |
| SP‑2 s2 role→chain | [specs/2026-08-11-sp2-role-chain-resolution-design.md](specs/2026-08-11-sp2-role-chain-resolution-design.md) | ✅ merged |
| SP‑2 s3 tool permissions | [specs/2026-08-12-sp2-tool-permissions-design.md](specs/2026-08-12-sp2-tool-permissions-design.md) | ✅ merged |
| SP‑2 s4 activation | [specs/2026-08-12-sp2-activation-policy-design.md](specs/2026-08-12-sp2-activation-policy-design.md) | ✅ merged |
| SP‑2 s5 hot‑reload | [specs/2026-08-12-sp2-hot-reload-design.md](specs/2026-08-12-sp2-hot-reload-design.md) | ✅ merged |
| SP‑3 s1 Subgraph | [specs/2026-08-12-sp3-subgraph-node-design.md](specs/2026-08-12-sp3-subgraph-node-design.md) | ✅ merged (develop) |
| SP‑3 s2 Branch | [specs/2026-08-12-sp3-branch-node-design.md](specs/2026-08-12-sp3-branch-node-design.md) | ✅ merged (develop) |
| SP‑3 s3 PlanDelta/Expand | [specs/2026-08-13-sp3-plandelta-splice-design.md](specs/2026-08-13-sp3-plandelta-splice-design.md) | ✅ merged (develop) |
| SP‑3 s4A planner (core+agent) | [specs/2026-08-13-sp3-planner-agent-design.md](specs/2026-08-13-sp3-planner-agent-design.md) · [plan](plans/2026-08-13-sp3-planner-agent.md) | ✅ merged (develop `b15fa6b`) |
| SP‑3 s4B planner selector | [specs/2026-08-13-sp3-planner-selector-design.md](specs/2026-08-13-sp3-planner-selector-design.md) · [plan](plans/2026-08-13-sp3-planner-selector.md) | ✅ merged (develop `3b1774b`) |
| SP‑3 s5 coordinator + loops‑of‑graphs | [specs/2026-08-14-sp3-coordinator-loops-of-graphs-design.md](specs/2026-08-14-sp3-coordinator-loops-of-graphs-design.md) | ✅ merged |
| SP‑4 s1 tool permission enforcement | [specs/2026-08-14-sp4-permission-enforcement-design.md](specs/2026-08-14-sp4-permission-enforcement-design.md) · [plan](plans/2026-08-14-sp4-permission-enforcement.md) | ✅ merged (develop) |
| SP‑4 s2 secret redaction | [specs/2026-08-14-sp4-secret-redaction-design.md](specs/2026-08-14-sp4-secret-redaction-design.md) · [plan](plans/2026-08-14-sp4-secret-redaction.md) | ✅ merged (develop) |
| SP‑4 s5 exactly-once (idempotency-key core) | [specs/2026-08-15-sp4-exactly-once-idempotency-design.md](specs/2026-08-15-sp4-exactly-once-idempotency-design.md) · [plan](plans/2026-08-15-sp4-exactly-once-idempotency.md) | ✅ merged (develop) |

Program phases (from the master spec §16): **SP‑0** gateway enrichment → **SP‑CAT/chains** → **SP‑1** durable skeleton → **SP‑2** registry → **SP‑3** hierarchical executor → **SP‑4** permission/effect *enforcement* + sandbox → **SP‑DATA** persistence/control‑plane → **SP‑6** HITL.

## 5. Deferred / forward‑looking (tracked)

- **SP‑4 — enforcement & isolation:** turn the SP‑2 permission *declarations* into runtime gating; path‑component‑aware matching (not raw prefix), host wildcards, callable‑tool gating to declared+granted, sandbox/workspace isolation, secret redaction.
- **SP‑DATA — persistence/control‑plane:** `PostgresJournal` + persistent CAS; `PostgresConfigSource`; durable **`config_versions`/`bump_config_version`** (cross‑process fence — today's generation is in‑process); the **durable scheduler** that re‑arms a paused run at `resume_after`; management CLI/API (torii‑derived). Note: the effect‑id scheme change (slice‑3 s1) is a journal‑format break — a fence bump is required once journals persist.
- **SP‑6 — HITL / human‑in‑the‑loop:** `HumanGate`/`AwaitSignal` nodes + signal delivery + pause‑expiry; **human‑as‑Agent** (a human‑backed agent in the role library whose execution is a pause‑for‑input). The pause/resume + event substrate already supports these.
- **SP‑7 — prompt budgeting:** active summarize/select + retrieval‑ranked/semantic activation (today: over‑budget halts loud, never truncates).
- **Perf / correctness follow‑ups:** "8c" snapshot‑consuming tail‑only fold (+ subgraph‑nested snapshots); agent‑child compaction; **terminal‑resume asymmetry** (re‑`start`ing an already‑terminal Subgraph/Branch/Expand/Map/Loop run reconstructs namespaced inner outputs, not the synthesized sink map — documented known‑limit); a gateway `chains()` accessor for exact plan feasibility.
- **SP‑3 s5 (coordinator) carry‑forward:** the **budget‑primary loop backstop** + reserved‑synthesis budget + finalize‑synthesize (the cost/token budget axis is dormant, so `max_iters` + the run‑scoped node caps are the only backstops); **replan‑on‑failure** (a failed iteration caught to replan rather than failing the Loop); **Subgraph‑body cross‑iteration state** (plan‑scope blackboard threading — a `Subgraph` body currently re‑runs fresh); tier‑downgrade‑on‑resume replan. Test thinness (non‑blocking, from the final review): no dedicated Expand‑body *pause* test (arm identical to the tested Subgraph pause) and no *composed* coordinator‑resume e2e (Expand body + gate‑agent replaying together — each memo is proven separately).
