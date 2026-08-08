---
title: SP-1 (slice 2) — Agent Runtime (design)
doctype: spec
spec: SP-1
slice: 2
phase: 3
status: approved
related:
  - docs/superpowers/specs/2026-08-06-sensei-orchestrator-design.md   # master (§6 registry, §9 agent runtime, §9.1 gateway boundary, §10 graph, §11 resilience)
  - docs/superpowers/specs/2026-08-08-sp1-orchestrator-spine-design.md # slice 1 — the durable spine this layers on
  - docs/superpowers/specs/2026-08-07-reference-chains-design.md       # the chains an agent's role resolves to
---

# SP-1 (slice 2) — Agent Runtime

**Goal:** turn an **agent definition** (md + frontmatter: role→chain, skills, tools, system-prompt body) into a **durable, resumable ReAct loop** that runs through the real gateway — layered on the slice-1 durable spine, changing nothing about how slice 1 works. The novel win of slice 1 (a deterministic executor that **resumes without re-spending tokens**) extends *inside* the agent loop: each model turn and each Pure tool call is a memoized effect, so a crash mid-loop resumes without re-spending completed turns.

**Master design:** `docs/superpowers/specs/2026-08-06-sensei-orchestrator-design.md` (§6 registry & config model, §9 agent runtime, §9.1 gateway boundary, §10 graph, §11 resilience). This spec **scopes slice 2** and defers the rest to named later slices.

**Approved decisions (brainstorm 2026-08-08):**
1. **Registry + prompt + ReAct, Pure tools only** — the full agent runtime (registry, prompt assembly, `AgentInvocation → InferenceRequest` compilation, ReAct loop), with tool **execution** limited to **Pure** (deterministic, memoize-forever) tools. Real-world **Observation/Mutation** tools + their safety (TTL/provenance/two-phase/reconcile) stay **slice 4** — mirrors how slice 1 proved durable execution with Pure-only effects.
2. **`NodeKind::Agent`; each ReAct turn is a Pure effect** — the Executor drives an `Agent` node by running the loop internally; each model turn is a Pure `ModelCall` effect with an **iteration-aware** `effect_id` (`loop_iteration = turn#`, the parameter slice 1 already threads). Resume folds mid-loop → completed turns memoized, **zero re-spend** — reuses the entire slice-1 spine.
3. **In-memory typed registry + `from_frontmatter(&str)` parser** — the md+frontmatter **format** is exercised via a pure string→struct parser; **no directory-walking filesystem I/O** yet (a `load_dir` is a thin later add). Stays pure/fixture-free like slice 1 and reference-chains.
4. **Prompt budget = detect-and-halt** — budget to the **minimum `context_window`** across the resolved chain; over-budget → a journaled explicit outcome that **halts loud** (`PromptOverBudget`). The summarize/select **strategy** is deferred; the "never silent truncation" **invariant** is honored now.
5. **Budgeting reads the chain via `Gateway::min_context_window(chain)`** — one additive, read-only accessor on the gateway (folds `min` over the chain's models' `context_window`); selection/execute untouched. *(Alternative considered: declare the window in registry config to avoid touching the gateway — rejected as duplicating catalog truth; revisit only if the accessor proves awkward at plan time.)*
6. **Max-steps halts loud** (`AgentMaxStepsExceeded`), not best-effort finalize — consistent with the walking-skeleton "no silent stop" discipline. (`Loop`-node best-effort finalize is a later, distinct concern.)

---

## 1. Scope

**In slice 2:**
- A **registry** (`sensei-orchestrator-core`, zero-I/O): `AgentDefinition` / `SkillDef` / `ToolSpec` types + a `from_frontmatter(&str)` parser + `Registry` with loud validation of dangling skill/tool refs.
- A new graph node `NodeKind::Agent { agent, input }` (core), driven by the Executor.
- An **agent runtime** (`sensei-orchestrator`): prompt assembly (body + skills + tool schemas), min-window budget check, and the **ReAct loop** where each turn is a Pure `ModelCall` effect and each Pure tool call is a Pure tool effect — all journaled and memoized through the slice-1 journal.
- A **Pure-only tool runtime**: a `Tool` trait + a demo deterministic tool (`calc`), executed in the orchestrator (the gateway only returns `tool_calls`).
- The **§9.1 gateway boundary**: the runtime compiles a plain `InferenceRequest{ chain, payload: Chat{ system, messages, tools } }` — no agent metadata enters the gateway.

**Deferred (stated, not silent):**
- **Slice 3:** `Map` bounded fan-out + `hard`/`soft` edges + quorum/`Consolidate` + `ContextStore` blackboard (§8) + `ContentStore`/CAS; **subagents**; per-phase `chains`; streaming.
- **Slice 4:** **Observation/Mutation** tool effects + TTL/provenance + two-phase journaling + `in_doubt → reconcile` (the tool-safety core). A ReAct tool that declares a non-Pure class is **rejected loudly** here.
- **Later:** filesystem `Registry::load_dir` (the parser exists; directory walking deferred); the summarize/select budgeting **strategy** (detection + halt only now); planner/`PlanDelta`; `HumanGate`; `PostgresJournal`. **No persistence** beyond the in-memory journal (config-driven directive).

## 2. Crates & placement (additive)

| Crate | Slice-2 additions |
|---|---|
| `sensei-orchestrator-core` | `registry.rs` (`AgentRef`/`AgentDefinition`/`SkillDef`/`ToolSpec`/`Registry` + `from_frontmatter`); one `graph.rs` variant `NodeKind::Agent`; new `OrchestratorError` variants (§7). Frontmatter YAML parse is pure string→struct (no I/O). |
| `sensei-orchestrator` | `agent/` module — `runtime.rs` (ReAct loop), `prompt.rs` (assembly + budget), `tools.rs` (`Tool` trait + `calc` demo). `Executor` gains `registry: Arc<Registry>` and a `max_steps` param (default `8`); `drive()` dispatches `Agent` nodes. |
| `sensei-gateway` | **One** additive read-only accessor: `Gateway::min_context_window(&self, chain: &str) -> Option<u32>` (min over the chain's models' `context_window`). No change to selection/gates/execute. |
| `sensei-orchestrator-store` | Unchanged (`InMemoryJournal` already carries arbitrary `EffectRecorded` outputs). |

No new crate; the 3-crate split holds. Existing crates stay behavior-preserving (purely additive).

## 3. Registry types (`sensei-orchestrator-core`, zero-I/O)

```rust
pub struct AgentRef(pub String);

pub struct AgentDefinition {
    pub name: String,
    pub area: String,          // coding | research | …   (informational in slice 2)
    pub kind: String,          // reasoning | …           (informational in slice 2)
    pub chain: String,         // role → a named gateway fallback chain
    pub tools: Vec<String>,    // ToolSpec names this agent may call
    pub skills: Vec<String>,   // SkillDef names composed into the system prompt
    pub system_prompt: String, // the markdown body
}

pub struct SkillDef { pub name: String, pub description: Option<String>, pub body: String }

pub struct ToolSpec {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: serde_json::Value,
    pub effect_class: EffectClass,   // slice 2: must be Pure
}

pub struct Registry { /* agents/skills: name→def; tools: name→ToolSpec */ }
```

- `AgentDefinition::from_frontmatter(&str)` / `SkillDef::from_frontmatter(&str)` split `---\n<yaml>\n---\n<body>`; YAML → typed fields, remaining body → `system_prompt`/`body`. Malformed frontmatter → a loud parse error (never a silent default).
- `Registry::validate()` fails loud (collect-all, like `assemble`) when an agent references an **unknown skill or tool** — `UnknownSkillRef`/`UnknownToolRef` (§7).
- Per-phase `chains`, `subagents` → **deferred** (slice 3); slice 2 carries only the single `chain`.
- The **executable** `Tool` (the `call` fn) lives in the runtime crate (§6); `ToolSpec` here is the pure schema/metadata the model sees and the registry validates.

## 4. `NodeKind::Agent` + the ReAct loop (`sensei-orchestrator`)

```rust
pub enum NodeKind {
    ModelCall { chain: String, payload: serde_json::Value },   // slice 1
    Agent     { agent: AgentRef, input: serde_json::Value },   // slice 2
}
```

When `drive()` reaches an `Agent` node:

1. **Resolve** the def from `self.registry` (unknown → `UnknownAgent`, loud).
2. **Assemble** (§5) → `system: String`, `tools: Vec<ToolDefinition>`, seed `messages` with a single user `Message` built from `input`.
3. **Budget check** (§5) → over-budget halts **before any spend**.
4. Journal **`NodeStarted{node}` once**, then run the **ReAct loop**, `turn = 0, 1, 2, …` (per-turn `EffectRecorded`; a single `NodeCompleted{node}` at the final answer):
   - `eid = effect_id(parent_path = node.id, loop_iteration = turn, local_index = 0)`; `input_hash = hash(chain ‖ system ‖ messages ‖ tools)`.
   - **Memoize/fence — identical to slice 1:** fold has `eid` `EffectRecorded` with a **matching** `input_hash` → reuse its output, **no gateway call**; **mismatch** → `DeterminismViolation{node, eid}` (halt).
   - Else: build `InferenceRequest{ capability: TextChat, chain, payload: Payload::Chat{ system: Some(system), messages, tools, .. }, allow_fallback: true }` → `gateway.execute()`:
     - `Ok(resp)` → `EffectRecorded{ node, eid, class: Pure, input_hash, output: { model, text, tool_calls } }`.
     - `Err(e)` → `NodeFailed{ node, error }`, stop the run (surfaced in `RunOutcome`; completed turns remain journaled → resume memoizes them).
   - **No `tool_calls`** → final answer → `NodeCompleted{ node }` with `output = { model, text }` (same shape as a `ModelCall` output; the full turn-by-turn transcript is recoverable from the journal's `EffectRecorded` trail); break.
   - **Has `tool_calls`** → for each call at index `k`: a **Pure tool effect** `effect_id(node.id, turn, k + 1)`, `input_hash = hash(name ‖ arguments)` → execute via §6 → `EffectRecorded{ class: Pure, output: result }` (memoized on resume). Then append the assistant message (carrying `tool_calls`) and one `Message::tool_result(call.id, result)` per call to `messages`; continue the loop.
   - **`turn == max_steps`** → `AgentMaxStepsExceeded{node}`: journal `NodeFailed` + return the structured error (no silent truncation of the reasoning trace). `max_steps` is an `Executor` construction param (like `version`) with a small default constant (e.g. `8`) — a run-level backstop, not per-agent config in slice 2.

**Transcript reconstruction on resume is deterministic:** turns are folded in order; a memoized turn contributes its recorded assistant message, and its memoized Pure tool effects contribute the same `tool_result` messages — so a later turn's `messages` (and thus its `input_hash`) recompute identically and memoize. No turn is re-spent.

**Version-fence is automatic.** A changed skill body or agent prompt changes `system` → changes the turn's `input_hash` → a resume **halts with `DeterminismViolation`** rather than mixing new instructions into a memoized old result (§9.1's "editing a skill bumps the fence", for free — no separate version field).

## 5. Prompt assembly & budget (`agent/prompt.rs`)

- `system = agent.system_prompt` then, for each listed skill (in order), `"\n\n" + skill.body`. Skills are **referenced by name**, resolved to bodies at assembly (§6.3 master). *(Conditional/progressive skill activation — Q4 in the master — is out of scope; slice 2 composes all listed skills.)*
- `tools = agent.tools.map(|n| registry.tool(n) → ToolDefinition{ name, description, input_schema })`.
- `messages` = one user `Message` rendered from the node's `input`: a JSON **string** passes through as the message text; a non-string value is JSON-serialized to text (a deterministic rendering, so it feeds the turn's `input_hash` stably).
- **Budget:** `est = est_tokens(system) + est_tokens(messages) + est_tokens(tool schemas)` via a **documented heuristic `est_tokens(s) ≈ s.chars().count() / 4`** (explicitly *not* a real tokenizer; a conservative approximation, replaceable later). `min_win = gateway.min_context_window(chain)` (`None` → treat as "unknown", skip the check with a `tracing::warn!` — never a hard failure on missing metadata). `est > min_win` → `PromptOverBudget{ node, est, min_win }`: journal `NodeFailed` + return the structured error, **before** any gateway call.

## 6. Tool runtime — Pure only (`agent/tools.rs`)

```rust
pub trait Tool: Send + Sync {
    fn spec(&self) -> ToolSpec;                                  // effect_class must be Pure in slice 2
    fn call(&self, args: serde_json::Value) -> Result<serde_json::Value, ToolError>;
}
```

- The runtime holds `HashMap<String, Arc<dyn Tool>>` (built alongside the `Registry`; the `ToolSpec`s it exposes are what the registry validates and the prompt advertises).
- Executing a model `tool_call`: look up by `name` (unknown → `UnknownTool`, loud); assert `spec().effect_class == Pure` (Observation/Mutation → `ToolEffectDeferred{name, class}` — an **honest slice-4 boundary, not a silent skip**); parse `arguments` (JSON string → value); `call(args)`; a `ToolError` → `OrchestratorError::Tool` (surfaced + journaled `NodeFailed`).
- **Demo Pure tool** `calc`: `{ op: "add"|"mul"|…, a, b } → { result }` — deterministic, so memoize-forever is correct. Used by the ReAct acceptance tests.

## 7. Error handling — no silent failures (§11.1)

New `OrchestratorError` variants (structured; `NodeFailed{error}` carries the surfaced string, consistent with slice 1):

| Variant | Raised when | Path |
|---|---|---|
| `UnknownAgent{name}` / `UnknownSkillRef{..}` / `UnknownTool{name}` | node/agent references a missing def | loud error (registry `validate` catches skill/tool refs up front) |
| `PromptOverBudget{ node, est, min_win }` | assembled prompt exceeds the chain's smallest window | journal `NodeFailed`, halt before spend |
| `ToolEffectDeferred{ name, class }` | model calls a non-Pure tool | loud error (slice-4 boundary) |
| `AgentMaxStepsExceeded{ node }` | ReAct loop hits `max_steps` | journal `NodeFailed`, halt |
| `Tool(ToolError)` | a Pure tool's `call` fails | journal `NodeFailed`, surface |
| `DeterminismViolation` / `VersionFenceMismatch` / `Journal` / `Gateway` | (slice-1 machinery, reused) | halt / surface (strict journal) |

Journal-write errors stay **strict** (abort). Determinism/version halts never memoize a mismatch. Every node error is journaled **and** surfaced in `RunOutcome`.

## 8. Acceptance tests

1. **Registry parse + validation** — `from_frontmatter` yields an `AgentDefinition` (name/area/kind/chain/tools/skills + body); an agent citing an unknown skill/tool → `validate()` error.
2. **Prompt assembly** — listed skills compose into `system` in order; `tools` compile to `payload.tools`; role → `request.chain`.
3. **Budget halt** — a tiny `min_win` (stub or a small-window demo model) → `PromptOverBudget`, **zero gateway calls**.
4. **ReAct loop (durable)** — a **scripted** test adapter emits a `calc` tool_call on turn 0 and a final text on turn 1 → runtime executes `calc` (Pure), feeds the `tool_result` back, receives the final answer. Assert: 2 model Pure effects (iteration-aware `effect_id`s) + 1 Pure tool effect journaled; final output correct.
5. **Resume without re-spend — inside the loop (headline)** — shared `InMemoryJournal`; **run 1** adapter succeeds turn 0 (+`calc`) then errors turn 1 → `NodeFailed`. **Run 2** = a fresh `Executor` on the same journal, adapter now succeeds → turn 0's model call **and** its `calc` effect are **memoized (0 gateway calls, 0 tool re-exec for turn 0)** → `RunCompleted`. **Assert turn 0's request reached a gateway exactly once across both runs** (run-2 adapter records only turn 1) — proving resume extends into the ReAct loop.
6. **Determinism fence on an edited skill** — resume a journal whose agent's skill body changed → turn 0's `input_hash` differs → `DeterminismViolation` (never a silent re-run/memoize).
7. **Non-Pure tool rejected** — an agent/tool declaring `Observation` invoked by the model → `ToolEffectDeferred` (honest deferral).
8. **Real end-to-end** — a **demo registry** (a `research` agent, no tools, chain `research.bulk`) + `gateway::catalog::demo_catalog()` → `assemble` → `Gateway` (noop adapter on the `ollama` router) → `Executor.run` a 1-node `Agent` graph → the walk falls over the cloud entries to `llama3.1-local`, one turn, no tool call → `RunOutcome` succeeds with the local model's output. Extends the slice-1 real-e2e to an `Agent` node.

Plus a **strict-journal** reuse test (an `append` error during a turn aborts loudly) to confirm the invariant survives the loop.

## 9. Design boundaries

- **Additive / SRP:** slice 1's `ModelCall` path is byte-identical; the gateway gains only a read-only accessor; the catalog/kernel crates are untouched. The agent runtime *consumes* the spine — it does not fork it.
- **Gateway is a long-lived pure client** (§9.1): the runtime compiles `AgentInvocation → InferenceRequest` (skills → `system`; tools → `payload.tools`; chain → `request.chain`); **no agent metadata enters the gateway**; tool **execution stays in the orchestrator**.
- **Pure-only tools** keep replay correctness intact (memoize-forever is correct only for deterministic ops); Observation/Mutation and their TTL/two-phase/reconcile safety are the explicit subject of **slice 4**.
- **No persistence** beyond the in-memory journal; `PostgresJournal` is a separate held-off layer. The registry is in-memory typed config (parseable from md+frontmatter strings); `load_dir` filesystem loading is deferred.
- Slice 2 is a **walking skeleton**: it proves an *agent definition* becomes a *durable, resumable, tool-using loop* through the real gateway — before fan-out, blackboard, subagents, and real-world tool effects layer on.
