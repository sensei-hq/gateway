---
title: sensei-orchestrator — Resilient Agentic Execution Framework
doctype: spec
status: design
date: 2026-08-06
author: Jerry Thomas (with Claude Code)
scope: whole architecture; the first implementation plan is scoped to the walking-skeleton slice only (see §16–17)
---

# sensei-orchestrator — Resilient Agentic Execution Framework

---

## 1. Summary

Build a **resilient agentic execution framework** in Rust, as new crate(s) in the existing `gateway` workspace, that **wraps `sensei-gateway`** and calls `Gateway::execute`/`execute_stream` directly. It orchestrates configurable **agents** (with **skills**, **tools**, and role/tier **chains**) over a **hierarchical, runtime-expandable graph**, on top of a **durable step-journal** that makes runs **resumable** after crashes, token/quota exhaustion, and rate limits — **without re-spending tokens** and **without silently corrupting state**.

The framework is *not coding-specific*; it targets any agentic domain (coding, workflow automation such as trip planning, deep research, …). It reuses proven design from strategos's TypeScript agent layer (coordinator / planner / dispatcher / hook-based persistence) but reimplements it natively against the Rust gateway.

## 2. Context

### 2.1 The gateway it wraps (`sensei-gateway`)
A Rust **library** (no server) already providing the resilient *inference* layer:
- Ordered **fallback chains** (`FallbackChainConfig`: `ChainEntry`s by priority; per-chain `fallback_triggers`).
- **Trigger-conditional fallback** — only on `RateLimit | Timeout | ProviderError | ModelUnavailable | BudgetExceeded`. `QuotaExceeded` is **terminal, never falls over** (see §12 for how model-lockout changes this).
- Per-endpoint **circuit breaker** (in-memory/per-process), **budget + subscription-quota** gating, **panels/consensus** (multi-model fan-out + debate→judge).
- One `InferenceRequest` / `InferenceResponse` for all modalities; `InferenceResponse.attempts` carries the full fallback trail.
- `RateLimit{retry_after_ms}` is *surfaced* but the gateway does **not** sleep/retry. Streaming fallback is **pre-first-byte only** (a mid-stream error is terminal).
- The gateway **does not execute tools** — it only returns `tool_calls`. Tool execution is the orchestrator's job.
- Two DB-agnostic seams already exist and set the pattern: `GatewayStore` (metering/trace, best-effort) and `VaultStore` (BYOK credentials; `PostgresVaultStore` → `keyvault.*` + `catalog.routers`).

### 2.2 Prior art (`strategos`, TypeScript)
Already contains a working agent framework we mine for design: `AgentCoordinator` (ReAct loop + plan-driven DAG), an LLM `planner` (self-correcting JSON + validation + Kahn cycle detection + feasibility), an `analyst` (HITL goal clarification), a `TaskDispatcher` (dependency-ordered parallel graph, cascade-skip, dependency-output passing), and **14 lifecycle hooks over 4 swappable backends**. Gaps vs. our goal: no durable/resumable engine, no skills, no agent-as-config, no model-tier routing, no first-class loops.

### 2.3 Non-goals (v1)
- Not a distributed multi-worker scheduler (single-process executor + durable resume; multi-instance is a future seam).
- Not a UI. Progress is exposed via hooks; a UI can subscribe.
- Not a replacement for the gateway's inference resilience — we compose it.

## 3. Decision log (from brainstorming)

| # | Decision | Rationale |
|---|---|---|
| D1 | **Rust crate(s) in the gateway workspace**, calling `Gateway::execute` directly; canonical engine going forward. | Invest in the Rust core; one resilient language; direct crate linkage (gateway has no server). |
| D2 | **Durable step-journal + replay** (Temporal/DBOS-style, scoped to agents). | The defining "resilient / resume on tokens-exhausted" requirement; determinism required anyway for replay. |
| D3 | **Hierarchical, runtime-expandable graph.** | Matches planner→coordinator→sub-agents + "loops of graphs"; agents discover info that shapes later work. |
| D4 | Architecture doc first, then scope the first **plan** to the walking-skeleton slice. | "Think through it deeply and lets plan." |
| D5 | **Progress hooks are first-class**, and **hooks ≠ journal**. | Observability requirement; keep correctness (journal) and observability (hooks) separate. |
| D6 | **No silent failures** is a hard invariant; errors are structured end-to-end; **journal-write errors are strict**; hook errors isolated-but-logged. | The #1 pain point of the older system. |
| D7 | **Core mechanism + opt-in policy.** Core always provides mechanisms; workloads turn on expensive policies via config/tool declarations. | A coding agent shouldn't pay for booking-grade machinery. |
| D8 | Walking skeleton target = **deep-research mini** (fan-out `Observation` effects, soft edges + quorum, journal/CAS split, resume). `Mutation`/two-phase ship as core *types*, unit-tested. | Hits the most core invariants (partial-failure, CAS, resume) with least surface. |
| D9 | **Circuit breaker, connection cooldown, model lockout** live in the **gateway core** selection pipeline, not the orchestrator. | Model/router *health* is every consumer's concern; sharpens the gateway/orchestrator boundary. |
| D10 | Adopt OmniRoute-style **free-tier catalog metadata + per-reason lockout/expiry tracking**, and add a **catalog control-plane** (CLI/API over providers/models/routers/chains, modeled on strategos) with **free-tier-aware routing**. | Free tier becomes a first-class model tier (big cost win for fan-out workloads); accurate `resume_after`; the gateway gains the management surface it lacks. |
| D11 | **Four-phase program** (§16): (1) gateway enrichment — free-tier catalog + refresh + health gates; (2) a few reference chains (free-tier "research team"); (3) agentic executor (agents/skills/tools); (4) data-tier + API wrapper (metadata/refresh/tracking) over the DB-agnostic seam. **Gateway core stays pure; tracking lives in the data-tier.** Chains stay user-managed. | User-directed sequencing: free tiers land early for the executor to ride; the pure core + executor ship before the heavier management/tracking layer. |
| D12 | **Reuse torii's DB metadata layer** for the Phase-4 data-tier (SP-DATA): extract its `catalog` + `config` + `metering` schemas, the staging **import/loader (refresh)** procedures, `config_loader.rs`, and `config_versions`/`bump_config_version` — scoped to **model / chain / flow management + config tools**. **Decouple from torii's user/tenancy/governance** (`core`/`governance`/`audit`/`content`/`device`/RBAC/budgets stay in torii); make tenancy optional/injectable so the data-tier is an **independent, reusable subsystem** consumed by the gateway, the orchestrator, and torii alike. | Torii already wraps the same `sensei-gateway` crate with a full catalog (providers/models/routers/chains/endpoints/capabilities/routing-policies) + metering + the shared `keyvault.*`/`catalog.*` schema. Reuse over rebuild. |
| D13 | **Tiers × chains are orthogonal and compose** (§6.2). Tiers = first-class catalog dimension (curated or **attribute-derived** from `auth_type`/cost/capability/`free_type`/locality) each with an **intra-tier routing strategy**; chains = ordered lists of **tier-refs** (or concrete models). Static membership expands at config-assembly; dynamic intra-tier ordering resolved at request time via SP-0 lockout + SP-DATA usage. Supersedes the earlier flat "tiers = chains". | One catalog edit updates every chain that references the tier; unifies premium/cost/fallback/free tiers + OmniRoute intra-tier strategies + torii `routing_policies`/`auth_type`; keeps the gateway core pure (expansion in the data-tier). |
| D14 | **Selection = composable policy pipeline; composed via the builder (not subclasses); strategy is task-aware per tier.** Admission = ordered `AdmissionGate` chain-of-responsibility with a typed `SkipReason` (timed variants carry `until` → `resume_after`); ordering = per-tier `RoutingStrategy` (Strategy); reaction = `OutcomeSink` (Observer); state behind a swappable `HealthStore`. Optional features are registered on `GatewayBuilder` (`.with_gate/.with_sink/.with_resilience(preset)`) — no inheritance. Routing strategy is bound per tier/chain: reasoning → `QualityFirst` (+ a `MinCapabilityGate` floor), research/bulk → `headroom`/`fill-first`, cost-optimized → `least-used`/`cost`; cost is never the global default. | No monolith (new feature = new small gate/sink + one registration); SOLID (SRP/OCP/DIP/ISP); kills the `validate_direct`/`validate_chain_entry` duplication; one instance serves reasoning + research chains concurrently; matches the gateway's existing builder + trait-store idiom. |

## 4. Boundary: gateway vs. orchestrator

- **Gateway core owns model/router health & availability:** fallback chains, circuit breaker, **connection cooldown**, **model lockout**, budget/quota gating, panels/consensus, credential resolution. (See §12 for the new health gates — gateway work.)
- **Orchestrator owns durable execution:** the graph, the deterministic executor, the effect journal + replay/resume, the shared context/blackboard, tool execution, agent runtime, and the agent/skill/tool registry.

The orchestrator treats each `Gateway::execute` as one **effect**; the gateway internally does intra-call chain fallover (now including cooldown/lockout skips) and returns either a success (with `attempts` trail) or a terminal error carrying `resume_after` when the whole chain is gated.

**Pure core, data-tier wrapper.** The gateway core stays *pure/stateless* (it receives config, never persists — strategos's founding principle, and the reason `GatewayStore`/`VaultStore` are seams, not built-ins). Free-tier *catalog data* is config; all stateful *tracking* (usage counters, lockout/expiration state, spend) lives in a **data-tier that wraps the DB-agnostic seam** (`GatewayStore`-style), built in Phase 4 (§16). This keeps the hot path pure and swappable across storage backends, and is why the free-tier catalog (Phase 1) and the tracking/management layer (Phase 4) are deliberately separated.

## 5. Crate layout

Mirrors the gateway's own `kernel → engine → adapters` layering; package prefix follows the `sensei-*` family convention.

| Crate | Role | Analogue |
|---|---|---|
| `orchestrator-core` | Zero-I/O foundation: graph/node/edge types, `AgentDefinition`/`SkillDefinition`/`ToolDefinition`, **effect model & classes**, `ExecutionJournal` + `ContextStore` + `ContentStore` (CAS) **traits**, structured error types. No async I/O beyond trait signatures. | `sensei-kernel` |
| `orchestrator` | The engine: deterministic executor, agent runtime, planner/coordinator, resume/replay driver, prompt assembly, tool runtime. Links `sensei-gateway`. | `sensei-gateway` |
| `orchestrator-store` | Concrete seam impls: `InMemoryJournal` + in-mem CAS (v1/tests); `PostgresJournal` + CAS backend (later, `orchestrator.*` schema, schema-qualified like `keyvault.*`/`catalog.*`). | vault/store adapters |

```
   external config (md+frontmatter) ──▶ Registry (agents · skills · tools) ; chains reused from gateway catalog
                                             │ referenced by
   ┌──────────────────────────────────────────▼───────────────────────────────┐
   │ orchestrator (engine)                                                     │
   │  hierarchical graph · deterministic executor · runtime PlanDelta          │
   │  durable journal + effect memoization + resume/replay + snapshots         │
   │  agent runtime (prompt assembly, ReAct, tool exec) · planner/coordinator  │
   └───────┬───────────────────────────────────────────┬──────────────────────┘
           │ each Agent/Tool node = effect(s)            │ reads/writes (refs)
   ┌───────▼───────────────────┐             ┌───────────▼──────────────────────┐
   │ sensei-gateway (core)     │             │ ContextStore (blackboard, scoped) │
   │ chains·fallback·breaker·  │             │ + ContentStore (CAS for blobs)    │
   │ COOLDOWN·LOCKOUT·quota    │             └───────────────────────────────────┘
   └───────────────────────────┘
   ┌────────────────────────────────────────────────────────────────────────────┐
   │ orchestrator-store: ExecutionJournal / ContextStore / ContentStore impls     │
   │ InMemory (v1) · Postgres+CAS (later)                                         │
   └────────────────────────────────────────────────────────────────────────────┘
```

## 6. Registry & config model

**Format:** markdown + YAML frontmatter (matches Claude Code and strategos doc conventions; "agent outline/markdown"). External dir, e.g. `~/.config/sensei/orchestrator/{agents,skills}/*.md` + `tools.yaml`. **Chains are reused from the gateway catalog**, not redefined.

### 6.1 AgentDefinition (frontmatter = base props; markdown body = system prompt)
```yaml
name: coding-planner
area: coding              # area of work (coding | travel | research | …)
kind: reasoning           # task kind → default capability/tier hint
chain: plan.frontier      # role-chain = a named gateway fallback chain
chains:                   # optional per-phase overrides (strategos already has this shape)
  plan: plan.frontier     #   e.g. [fable, opus, gpt5, kimi, glm]  — frontier tier
  execute: code.mid       #   e.g. [sonnet, haiku, glm, …]         — coding tier
  reflect: reason.frontier
tools: [fs.read, fs.write, shell]
skills: [clean-code, design-principles, security-compliance]
subagents: [architect, designer]   # focused agents this one may dispatch
# --- markdown body = instructions/persona ---
```

### 6.2 Tiers × chains (D13)
**Tiers and chains are orthogonal axes that compose.** A **tier** is a first-class catalog dimension — a named segment, curated or **attribute-derived** from `auth_type` / cost band / capability / `free_type` / locality (e.g. `premium-reasoning` = OAuth-CLI + reasoning [Claude Code · Codex · Copilot · Cursor]; `cost-optimized` = cheap/high-throughput [DeepSeek · GLM · MiniMax · Qwen]; `fallback-specialty` = local/specialty [Kiro · OpenCode · Antigravity · Vertex]; `free` = any `free_type`). Each tier carries an **intra-tier routing strategy** (`headroom` / `least-used` / `cost` / `priority`).

A **chain** is an ordered list of **tier-refs (or concrete models)**; the registry binds **role/kind → chain** — e.g. `plan.frontier = [premium-reasoning → cost-optimized]`, `research.bulk = [free → cost-optimized → fallback-specialty]`, `code.exec = [cost-optimized(headroom) → premium-reasoning]`.

**Resolution split (keeps the gateway core pure):** static tier *membership* expands into ordered candidates at config-assembly (torii `config_loader`); dynamic intra-tier *ordering* (`headroom`/`least-used`) is chosen at request time by the gateway's selection using SP-0 lockout + SP-DATA live usage; cross-segment fallover reuses the existing trigger logic. Adding a model to a tier **once** updates every chain that references it — and free-tier maximization + automatic lockout fall-over come for free (a large cost win for the fan-out-heavy deep-research skeleton). Cross-**role** context handoff is layered on top by the orchestrator (§8).

### 6.3 Skills = injectable instruction modules
Markdown + frontmatter (`name`, `description`, optional bundled tools/scripts) — the Claude Code skill shape. At prompt-assembly time an agent's listed skills are composed into its system prompt. Skills are **not** executable nodes; executable capabilities are **tools**.

**Referenced by name, activated conditionally.** An agent lists skills **by reference** (a name/id), exactly like it references a chain by name — the registry resolves the body at assembly. Activation need not be all-or-nothing: a skill (and likewise a tool's exposure) may be **conditional / progressively disclosed** — injected only when relevant (by a `description`-driven trigger, a `when`/precondition on the reference, or planner selection) rather than always concatenated. This keeps the prompt within budget (§9) and lets a large skill/tool library attach to an agent without every request paying for all of it. The exact **activation policy** (always-on · trigger/precondition · planner-selected · retrieval-ranked) is an open question (Q4) to settle in the Phase-3 registry spec; the *reference-by-name* contract is fixed now.

### 6.4 ToolDefinition
Named executable capability: JSON-schema args, an **effect class** (§7.1), and — for the opt-in policy layer — declarations for **idempotency key**, **retry class**, **permissions** (path/command/network allowlists, resource caps), and optional **compensation** action. Executed **by the orchestrator** (the gateway only returns `tool_calls`).

## 7. Durable execution core (the spine)

### 7.1 Effects are classed by idempotency — not by "expensive"

The root cause the adversarial reviews found: memoize-by-output is correct for pure reads, catastrophic for world mutations. So every non-deterministic/expensive op is an **effect** with a class:

| Class | Examples | Replay rule |
|---|---|---|
| **Pure** | `ModelCall`, clock, random | memoize forever (the no-token-re-spend win) |
| **Observation** | `fs.read`, web fetch, flight/hotel search | memoize **with TTL + provenance** `{source, fetched_at, content_hash}`; re-read past TTL; staleness is recorded, never silent |
| **Mutation** | `fs.write`, `shell`, booking, `git commit` | **never blindly re-run, never blindly memoize** — two-phase + idempotency key + reconcile (§7.3) |

### 7.2 effect_id — structural, iteration-aware, input-bound, version-fenced
- `effect_id = hash(parent_path ‖ loop_iteration ‖ local_index)`. **Loop iteration is part of the id** — fixes the no-progress infinite loop where a replan loop re-enters the body, the counter resets, and iteration 2 memoizes iteration 1's stale failing output.
- Each `EffectRecorded` binds a **hash of its input**. On replay, recomputed-input ≠ recorded → **halt with a determinism-violation diagnostic**; never memoize a mismatched input.
- The journal is fenced by a **registry/config/code version**; a config change refuses silent resume (explicit migrate/abort).
- Runtime-spliced (`PlanDelta`) nodes get deterministic path-derived ids from the recorded `PlanExpansion` (never insertion/completion order).

### 7.3 Two-phase journaling + the `in_doubt → reconcile` outcome
For **Mutation** effects:
1. Append `EffectIntent{effect_id, idempotency_key, args_hash}` (fsync) **before** the side effect.
2. Perform the side effect (passing the idempotency key to the provider).
3. Append `EffectRecorded{output}` **after**.

On resume, `Intent` **without** `Recorded` = **IN-DOUBT** → the executor must **not** re-run and **not** memoize. It runs a **reconciliation path**: query the provider by idempotency key / probe the world / (last resort) pause to a human. This is the 4th "no-silent-failures" outcome the old model lacked, and it's what prevents double-booking / double-apply.

### 7.4 Journal / payload split, snapshots, compaction
- **Control-flow log** (small): node transitions, effect_ids, statuses, scalar outputs, digests — everything the *fold* needs.
- **ContentStore / CAS** (out-of-band): large effect outputs (fetched pages, big tool output), content-addressed (dedupes identical content). The fold **never** deserializes blobs; they load lazily when a node re-reads them.
- **Snapshots** at node/round boundaries → resume = latest snapshot + replay-the-tail (bounds fold cost for workloads that pause/resume constantly).
- **Compaction**: once a `Map`'s children are terminal and consolidated, collapse per-item records to `{status, digest, cost}`.

### 7.5 Resume/replay
`start(run_id)`: load control-flow log (+ latest snapshot), **fold** events to rebuild state (completed nodes, expanded graph, context refs), memoize completed effects (respecting class + input-hash + intent/recorded), continue from the first non-completed ready node. Hooks fire with `replay: true` during the fold.

### 7.6 Journal trait (illustrative)
```rust
#[async_trait]
pub trait ExecutionJournal: Send + Sync {
    async fn append(&self, run_id: RunId, event: JournalEvent) -> Result<Seq, JournalError>; // STRICT: errors are fatal/pause
    async fn load(&self, run_id: RunId, since: Option<Seq>) -> Result<Vec<JournalEvent>, JournalError>;
    async fn snapshot(&self, run_id: RunId, snap: Snapshot) -> Result<(), JournalError>;
    async fn latest_snapshot(&self, run_id: RunId) -> Result<Option<Snapshot>, JournalError>;
}
// JournalEvent (subset): RunStarted · NodeStarted · EffectIntent · EffectRecorded
//  · PlanExpanded · ContextWrite{seq} · NodeCompleted · NodeFailed{error}
//  · RunPaused{reason, resume_after} · RunResumed · RunCompleted · HookError
```
A **global monotonic `Seq`** stamps every effect completion and every shared-scope `ContextWrite`, so replay folds in **recorded order**, not wall-clock order (determinism under concurrency).

## 8. Shared context / blackboard

- **Scoped** (`run` / `plan`(subgraph) / `node` / `agent`); reads resolve **up** the scope chain; writes are scoped and journaled as `ContextWrite` (with `Seq`).
- **Entries hold refs, not blobs:** `{key, type, digest, size, summary, ptr→CAS}`. Read-miss is an **explicit outcome**, never a silent empty.
- **Concurrent-write conflict semantics** on a shared key are explicit (reject / seq-ordered LWW / merge node); shared-mutable reads are journaled or snapshot-isolated per node so replay can't diverge.
- **Fallback / cross-role handoff:** the agent runtime always assembles the prompt from *current* blackboard state, so whichever model runs (after gateway fallover, or a later refining role) sees the accumulated context. "fable planned → opus refines" = opus reads fable's outputs from the blackboard.
- **Freshness:** entries carry an as-of stamp / optional TTL; secrets/tokens are **never** stored in the durable blackboard.

## 9. Agent runtime

`run_agent(def, inputs, ctx, journal) -> AgentOutcome`:
1. **Resolve chain** from the def (kind/phase → chain id in the gateway catalog).
2. **Assemble prompt (budgeted):** system = instructions(body) + composed skills + **selected** scoped-context (resolved from CAS, summarized to fit) + tool schemas. The token budget is sized to the **minimum context window across the chain** — a prompt that fits `fable` must survive fallover to a smaller model, else you get an opaque `ProviderError` that isn't even a fallback trigger. Over-budget → an explicit **journaled** summarize/select decision, never silent truncation.
3. **ReAct loop:** each turn is a `ModelCall` effect (Pure, memoized) → parse `tool_calls` → each tool is a Tool effect of its declared class (Observation/Mutation) → append results → loop until final answer / max_steps / budget / pause.
4. **Write** outputs + explicit "info-needs" to the blackboard (journaled, `Seq`-ordered).
5. **Subagent dispatch** = emit a `PlanDelta` (subgraph) — a `PlanExpansion` effect.
6. Optional **streaming** via `execute_stream` → `on_agent_stream_chunk` hooks (live tokens + `ProviderSwitch`).

**Tier-downgrade on resume:** if a paused agent resumes on a cheaper tier (or the gateway fell over to a smaller-context model), do **not** splice a frontier transcript onto a cheap model — restart the ReAct loop from a **summarized checkpoint** (treat as replan). A **context-overflow / capability-mismatch** error class maps to replan, not opaque terminal.

### 9.1 Gateway boundary & lifecycle (integration contract)

**The gateway is a long-lived, pure client — not open-per-task.** It is built once (`Gateway::new`/`try_new`/`FacadeBuilder::build`) with a `GatewayConfig` (chains/routers/models/adapters) and held as a long-lived `Arc<Gateway>` (tenancy, if any, is a wrapper running **one entity per tenant** — the gateway *and* orchestrator cores have no tenant concept); there is no per-run create/close. Chains are **instance-level config assembled once** by the data-tier `config_loader` (tier-expansion → named `FallbackChainConfig`s) and hot-swapped via `update_config`/`try_update_config`; keys rotate via `refresh_router_keys`. Per request, callers pass only **which chain by name** (`request.chain`) and **BYOK keys** (`request.credentials`, router→key, applied as a per-call override — `engine.rs:278`). Health-gate state (breaker/cooldown/lockout, §12) therefore lives on the instance and is meaningfully long-lived across requests.

**Agent execution is a wrapper on top; no agent metadata enters the gateway request.** The agent runtime compiles an `AgentInvocation` (skills · subagents · tools · context · inputs) into a plain `InferenceRequest`:
- **skills** → composed into the system prompt (registry supplies bodies) — prompt text, not a gateway field;
- **tools** (built-in / **MCP** / custom) → schemas into `payload.tools` (the model emits `tool_calls`); **execution stays in the orchestrator** tool runtime (MCP via the bridge) — the gateway never runs tools;
- **subagents** → orchestrator dispatch as a nested invocation / `PlanDelta` subgraph — never in the gateway request;
- **context** → read from the blackboard, budgeted into the prompt, written back after — a blackboard concern;
- **chain/tier** → resolved role→chain name into `request.chain`; **keys** → `request.credentials`.

This keeps the gateway reusable by non-agentic callers (torii/sensei) unchanged, and puts every agentic concern one layer up. **Determinism:** the compiled request is a journaled effect memoized by input-hash; skills/agent-def/context snapshot are inputs to that hash, fenced by the catalog config-version (§ config-versioning) — editing a skill or agent def bumps the version so a resume halts loudly rather than mixing new instructions with a memoized old result.

## 10. Execution model (graph)

### 10.1 Nodes & edges
- **Node kinds:** `Agent`, `Tool`, `Loop`, `Subgraph`, `Branch`, `Map` (bounded fan-out), `Consolidate`, `AwaitSignal`/`HumanGate` (§14).
- **Typed edges:** `hard` (dependent cannot run without it → cascade-skip) vs `soft` (dependent tolerates absence). **Cascade-skip fires only across hard edges** — this is what lets research proceed on 180 of 200 sources.
- **Aggregation/completion policy** on `Map`/`Consolidate`: `fail_fast | best_effort | quorum(min_count | min_fraction)`, plus a minimum-viable-input gate on `Consolidate`. `Map` emits `Vec<Result<Item, Error>>` + a **failure manifest**, not a pass/fail signal.

### 10.2 Executor
Drives ready nodes (deps satisfied per edge type) with **provider-aware bounded concurrency** (§12.4), enforces node + run budget/timeout (killing process trees for `shell`), and records completion order as `Seq`.

### 10.3 Runtime expansion & loops
- An `Agent`/planner node may return a `PlanDelta` (subgraph) → executor journals `PlanExpanded{node, subgraph}` then splices it in; replay reconstructs it identically.
- **"Loops of graphs"** = a `Loop` over a `Subgraph` body with a gate agent/tool returning `Continue | Stop` + `max_iters`. **Budget is the primary backstop; `max_iters` secondary.** On budget/timeout, the Loop transitions to a **finalize/best-effort path** (e.g. synthesize from what exists), never a bare fail. A **synthesis budget is reserved up front**. Global caps bound total nodes / expansion depth / PlanDeltas per run (self-DoS guard).

## 11. Resilience & error handling

### 11.1 The invariant: no silent failures
Every error takes exactly one path — nothing dropped:
1. **Execution errors** → journaled **and** mapped to an explicit outcome (`fail | pause | replan | retry | in_doubt→reconcile`) **and** surfaced in the run result + hooks. Structured errors preserved end-to-end (no string flattening).
2. **Journal-write errors** → **strict**: fail or pause the run loudly (the journal is the correctness source of truth — the opposite of the gateway's best-effort metering store).
3. **Hook errors** → isolated from execution but logged + surfaced on a diagnostics channel (optionally a `HookError` event). *Isolated ≠ swallowed.*

### 11.2 Gateway error → executor mapping
| Gateway result | Orchestrator behavior |
|---|---|
| success (`attempts` trail) | record `ModelCall` effect; bubble attempts to hooks |
| `AllAttemptsFailed` (chain exhausted by real errors) | node fails → cascade-skip (hard edges) **or** caught by an enclosing `Loop` to replan |
| `AllGated { resume_after, human_action }` (whole chain gated, §12) | `resume_after = Some(t)` → **durable pause** to that wall-clock time; `None` **with** a `human_action` → the **indefinite HOTL pause** (never auto-woken; `force_wake` after the operator acts) — *M1 reversed 2026-09-04, was "fail-fast, never pause forever"*; `None` with **no** action at all → fail |
| `RateLimit{retry_after_ms}` surfaced at tool level | orchestrator owns backoff: **journaled `Timer`** (jittered) then retry, else pause |

### 11.3 Tools get their own taxonomy
Third-party APIs are not gateway errors. Tool errors classify as `retryable | terminal | in_doubt`, with per-tool retry/backoff mirroring the gateway's `RateLimit → journaled Timer` (429 + `Retry-After`). **Auth/token acquisition is a non-memoized, resolve-at-execution capability** (memoizing it replays a dead token) — explicitly outside the effect-memoization boundary.

### 11.4 Durable scheduler
`Timer` (rate-limit backoff), readiness retries, quota resume, and HITL expiry all need a **durable scheduler that re-arms after a restart** — an in-process sleep is lost on crash.

## 12. Gateway-core prerequisite: model/router health gates (D9)

New/enhanced **selection gates** in the gateway's `validate_chain_entry` pipeline (touches `selection.rs` / `circuit_breaker.rs` / `engine.rs` — coordinate with the approved issue #39 complexity refactor). Each produces a `SkippedCandidate` reason and the chain **falls over** to the next entry; only when **all** entries are gated does `execute` return terminal, carrying `resume_after = min(expiry)`.

| Gate | Granularity | Trigger | Expiry |
|---|---|---|---|
| Circuit breaker (exists) | endpoint `router:model` | N consecutive `ProviderError`/`Timeout` | breaker `timeout` → HalfOpen probe |
| **Connection cooldown** (new) | router/connection | connection-level fault (`Network`, connect timeout) | backoff window (don't hammer a down provider model-by-model) |
| **Model lockout** (new) | model | `QuotaExceeded`, `RateLimit{retry_after}`, `Authentication`, `ModelUnavailable` | `retry_after` / quota-reset / manual clear |

**Key semantic change:** a **provider** quota/rate-limit signal (classified from the adapter response — *not* the caller's subscription quota) **demotes to the next tier instead of terminating**; the run surfaces `GatewayError::AllGated { resume_after }` only when the whole chain is gated, feeding the orchestrator's durable pause. The caller's subscription `GatewayError::QuotaExceeded` (subject/tier) stays a **hard stop** and never demotes. Resolves the "one quota hit kills the run" finding without conflating the two quotas (see `docs/design/selection-policy-pipeline.md`).

**State:** in-memory/per-process today (like the breaker). A future seam can persist gate state for multi-instance sharing (noted, not v1).

### 12.1 Lockout refinements (adopted from OmniRoute `accountFallback`)
- **Cooldown duration by reason, not one value:** `rate_limit` ≈ 60s (fall over; recovers fast); `quota_exhausted` ≈ until the next reset boundary (tomorrow 00:00 / monthly reset), else ~1h; `credits_exhausted` → terminal until a human tops up (pause with a human-action resume hint); `auth`/`expired` → lock until the credential changes.
- **Escalating backoff whose window outlives the cooldown:** a model that fails again right after its lockout expires keeps escalating instead of resetting to base. Clamp to an operator `maxCooldownMs` — **except** honor a real upstream reset hint exactly (never clamp "Resets in 92h" down to the cap).
- **Rich classification** of limit signals: 429→`rate_limit`, 403/quota-body→`quota_exhausted`, credits→terminal; include text-pattern detection for providers that throttle via non-standard 400/403 bodies. This splits today's single `QuotaExceeded` into `rate_limit | quota-until-reset | credits-terminal`, each mapping to a distinct §11.2 outcome.
- **Two scopes:** connection cooldown (all of a router's models) vs model lockout (one `router:model`). The gateway is **tenant-agnostic** — no tenant/credential dimension; a wrapper runs one gateway entity per tenant. Durability is the caller's via an `on_lockout` callback + `apply_lockout` re-seed (SP-0 design §5c); the gateway never persists. Bounded map with an eviction cap so lockout state can't leak.
- **Proactive expiration tracking** (`providerExpiration`): track `oauth_token | subscription | api_credits | free_tier_reset` expiry per credential with pre-emptive `expiring_soon` alerts, and detect expiry from responses (401→token, 402→subscription, 429+reset→free_tier_reset) — skip a credential *before* it fails.
- **Cumulative retry-wait budget** per request (`cooldownAwareRetry`): honor `Retry-After` but cap total wait across all retries; the orchestrator's journaled `Timer` (§11.2) adopts this cap.

### 12.2 Catalog & control-plane (D10; Phase-1 `SP-CAT` catalog + Phase-4 `SP-DATA` tracking/management)
Combine strategos's **DB-driven catalog + CLI/API management layer** (providers/models/routers/chains → runtime `GatewayConfig` via `assembleConfig()`) with OmniRoute's **tracking metadata**:
- **Catalog schema gains free-tier fields** (per model/provider): `free_type` (`recurring-daily | recurring-monthly | recurring-credit | recurring-uncapped | one-time-initial | keyless | discontinued`), `monthly_tokens` / `credit_tokens`, `pool_key` (shared-quota pools counted once), `tos` verdict, `trains_on_prompts`. Paid side: per-key `daily/weekly/monthly` USD limits + `reset_interval` / `reset_time` / `next_reset_at`.
- **Tiers as a catalog dimension** (D13): a `tiers` definition (curated membership *or* an attribute-derived selector over `auth_type`/cost/capability/`free_type`/locality) + a per-tier `routing_policy` (intra-tier strategy: `headroom`/`least-used`/`cost`/`priority`). Chains reference tiers (a `chain_models` entry may be a **tier-ref**); `config_loader` expands tier membership into ordered candidates, and the gateway orders *within* a tier at runtime (SP-0 lockout + SP-DATA usage). Reuses torii's `routing_policies` / `chain_models` / `chain_bindings`.
- **Usage-metering store** extending `GatewayStore`: counters against free-tier + paid limits with reset windows (per key / model / pool), feeding **predicted-exhaustion** pre-emptive lockout (not just reactive 429).
- **Free-tier-aware routing strategies** as chain-selection options: `headroom` (most remaining quota), `fill-first` (exhaust one pool first), `reset-window` (order by reset), and internal `quota-share` (spread across pools).
- **Management surface** (CLI + API, modeled on strategos) to configure the catalog *and* observe tracking state (usage, lockouts, expirations, free-tier budget) — the control plane the Rust gateway lacks today (the consumer currently owns all config loading).

**Orchestrator impact:** free tier becomes a first-class model tier (e.g. `research.bulk = [free → cheap-paid → frontier]`); the deep-research fan-out skeleton rides free tiers first; lockout `resume_after` gives the executor accurate durable-pause wake-up times.

## 13. Security & compliance (opt-in policy)

- **Tool permission model:** per-tool path allowlists, command allow/deny, network egress policy, CPU/mem/time caps — pinned in the `AgentDefinition`, enforced by the executor.
- **Sandbox** `shell` (container/jail) with an ephemeral credential broker.
- **Secret redaction** before journaling effect I/O (durable plaintext creds = compliance landmine).
- **Workspace isolation** for parallel branches (git worktree / copy-on-write) + declared resource locks (git index, build dir) so concurrent writers don't corrupt shared state; explicit merge/`Consolidate` for results.
- **PII vs append-only journal:** **crypto-shredding** — per-run encryption key; delete the key to satisfy erasure without mutating the immutable log.

## 14. HITL / external signals (designed-for; later sub-project)

- `AwaitSignal` / `HumanGate` node suspends **indefinitely** (no timer).
- `ExternalSignal` / `HumanDecision` **effect type** records the delivered payload deterministically.
- **Idempotent, correlated signal-delivery API** backed by a durable **mailbox** (handles the signal-arrives-before-gate race; double-click "approve" must not create two approvals).
- Pause time **excluded from run timeout**; separate **pause-expiry** escalation (human never responds → escalate/cancel + compensation).
- Suspended nodes **release concurrency permits**, durably re-acquire on resume.
- Pause **notification is a durable outbox effect — not a best-effort hook** (else the human is never told and the run hangs).

## 15. Observability — hooks

`OrchestratorHooks` trait in `orchestrator-core`, `async_trait`, every method a no-op default, attached via `.with_hooks(Arc<dyn OrchestratorHooks>)`, composable. Best-effort but **non-silent** (§11.1.3). Scopes:
```
run:     on_run_started · on_run_paused{reason, resume_after} · on_run_resumed · on_run_completed
graph:   on_node_started{kind} · on_plan_expanded{subgraph} · on_node_completed · on_node_failed/skipped
agent:   on_agent_started{name, chain} · on_agent_step{n, summary}
         on_agent_model_attempt{model, outcome}   ← bubbles gateway InferenceResponse.attempts
         on_agent_stream_chunk{delta}              ← live tokens via execute_stream
         on_agent_tool_call / on_agent_tool_result
         on_agent_completed{usage, cost} · on_agent_failed
context: on_context_write{scope, key, seq}
```
- **`on_agent_model_attempt`** surfaces the fallback trail live ("tried fable → rate_limit → opus succeeded").
- **Replay suppression:** during resume-fold, hooks carry `replay: true` (default suppressed) so a UI doesn't double-count.
- Hooks are for observation only; anything execution depends on (e.g. HITL notification) is a durable outbox effect, not a hook.

## 16. Program phasing & decomposition

Four phases, in this order (user-directed). The **gateway core stays pure/stateless** throughout (§4): free-tier *catalog data* is config, but stateful *usage tracking* lives in the Phase-4 data-tier wrapper over the DB-agnostic seam. Free tiers land early so the executor (Phase 3) can ride them; the full management/tracking API lands last.

### Phase 1 — Gateway enrichment (pure core)
| SP | Title | Contents | Depends on |
|---|---|---|---|
| **SP-0** | Health gates | connection cooldown + model lockout via a composable policy pipeline (per-reason cooldowns, reset-window expiry, escalation, 429/quota/credits classification at the adapter boundary, tenant-agnostic `router:model` key + `on_lockout` callback — see `docs/design/selection-policy-pipeline.md`); provider-quota demote-to-tier + `AllGated{resume_after}`. Coordinate with #39. Expiration-tracking (stateful) + cumulative-retry-budget are **out of SP-0** (SP-DATA / orchestrator). | gateway |
| **SP-CAT** | Free-tier catalog + tiers + refresh | catalog schema gains free-tier fields (§12.2 — `free_type`/`pool_key`/`monthly_tokens`/`credit_tokens`/`tos`/`trains_on_prompts`) **and the `tiers` dimension** (curated/attribute-derived + intra-tier strategy — §6.2/§12.2, tier-refs in chains); bundled free-tier data; a **refresh mechanism** (CLI + periodic re-audit of models/providers/routers config, CI-gated totals à la OmniRoute's `computeFreeModelTotals`). Pure config/data — no stateful tracking yet. | gateway |

### Phase 2 — Reference chains (user-managed)
| SP | Title | Contents | Depends on |
|---|---|---|---|
| **SP-CHAINS** | Reference chains | a few curated starter chains **composed from tiers (tier-refs, D13)**, free-tier-first — notably a **"research team"**: planner → `[premium-reasoning]`, coordinator → `[premium-reasoning → cost-optimized]`, bulk sub-agents → `[free → cost-optimized → fallback-specialty]`. Chains remain user-owned config; these ship as reference examples, not auto-generated. | SP-CAT |

### Phase 3 — Agentic executor (agents · skills · tools)
| SP | Title | Contents | Depends on |
|---|---|---|---|
| **SP-1** | **Walking skeleton (deep-research mini)** | crate scaffold; **core types** (effect classes incl. `Mutation`, two-phase Intent/Recorded, `in_doubt`, structural+iteration `effect_id`, input-hash, version fence, journal/CAS split); `ExecutionJournal`+`ContextStore`+`ContentStore` traits; `InMemoryJournal`+CAS; deterministic executor (`Map`/`Consolidate`/`Loop`, typed edges, aggregation policy); agent runtime (budgeted prompt, ReAct, Observation tool); `Gateway::execute` wrap; resume + snapshot; `OrchestratorHooks`; quota→pause. Minimal registry (one agent from md+frontmatter; chains from Phase 1/2 catalog). | SP-CAT (soft), SP-0 (soft) |
| **SP-2** | Registry (agents/skills/tools) | full agent/skill/tool config; role/kind→chain + tiers; hot-reload; tool permission declarations. | SP-1 |
| **SP-3** | Hierarchical executor (full) | planner + coordinator agents; runtime `PlanDelta`; loops-of-graphs; `Branch`/`Map` full; global caps. | SP-1 |
| **SP-4** | Mutation & exactly-once | two-phase enforcement end-to-end; reconciliation providers; idempotency keys; saga/compensation; sandbox + workspace isolation; secret redaction. | SP-1 |
| **SP-7** | Context & tools at scale | retrieval/rerank/summarize prompt assembly; section-wise synthesis; adaptive provider concurrency; panel-as-native-subgraph. | SP-3 |

### Phase 4 — Data-tier + data-layer API (metadata mgmt · refresh · tracking)
| SP | Title | Contents | Depends on |
|---|---|---|---|
| **SP-DATA** | Data-tier & API wrapper (**extract from torii**) | **Extract torii's `catalog` + `config` + `metering` schemas** + staging `import_*` loader (the refresh mechanism) + `config_loader.rs` + `config_versions`/`bump_config_version`, into a **decoupled, user-agnostic subsystem** (tenancy made optional/injectable; torii's `core`/`governance`/`audit`/`content`/`device`/RBAC/budgets excluded). Add SP-CAT free-tier fields + usage-metering feeding **predicted-exhaustion** lockout + free-tier-aware routing (`headroom`/`fill-first`/`reset-window`/`quota-share`) + a management **CLI + API** for model/chain/flow/config. Wraps the pure DB-agnostic seam (shared `catalog.*`/`keyvault.*` schema). Durable `PostgresJournal` + CAS + snapshots/crypto-shred + durable scheduler live here too. | SP-0, SP-1 |
| **SP-6** | HITL & signals | `AwaitSignal`/`HumanGate`; signal mailbox + delivery API; pause-expiry; durable outbox notifications. | SP-DATA |

## 17. Walking skeleton (SP-1) — acceptance criteria

A deep-research **mini** run:
1. A research agent fans out **5 searches** (`Map`, `Observation` effects) over **soft** edges with a `quorum(min=3)` policy → `Consolidate` into a short report.
2. **Partial failure:** 2 of the 5 searches fail; the run **still** produces a consolidated report from the 3 that succeeded (proves soft-edge + quorum), and the failure manifest is recorded (no silent drop).
3. **Resume with no re-spend:** kill the process mid-run; on resume the fold rebuilds state and completes, and a **fake-gateway call counter proves zero duplicate `ModelCall`s** and zero duplicate successful `Observation`s (memoization works; tokens not re-spent).
4. **Determinism fence:** changing the agent config between kill and resume → resume **halts loudly** with a determinism-violation diagnostic (input-hash/version fence).
5. **Quota→pause:** a fake gateway returning `AllGated { resume_after: Some(t) }` → the run **pauses** with the correct wall-clock wake-up time and **resumes** cleanly; an all-terminal `AllGated { resume_after: None, human_action }` → **fail-fast** (no infinite pause).
6. **No-silent-failures:** every error path yields a structured outcome + a hook emission (asserted).
7. `Mutation`/two-phase/`in_doubt` **types and reconcile hook exist and are unit-tested**, even though the demo workload doesn't drive them.

## 18. Testing strategy

Per the repo rules (every change ships with a test; TDD; verify the real outcome, not a masked wrapper):
- **Deterministic fake gateway** (canned responses keyed by input-hash; call counter) + **in-memory journal + CAS**.
- Tests: replay determinism (no duplicate Pure calls); crash-resume (kill after N effects); loop-iteration effect_id (replan loop does not memoize the prior iteration → no-progress guard); input-hash divergence halts; partial-failure (soft/quorum vs hard cascade-skip); `in_doubt`→reconcile invoked (not blind re-run); quota→pause→resume; no-silent-failure assertions on every error branch.
- Verification asserts the **specific effect** (e.g. the call counter, the produced report), not a proxy.

## 19. Risks & open questions

- **R1 — determinism discipline.** Orchestration logic between effects must be pure. Enforced by input-hash + version fence + replay-divergence halt; still requires care in reviews.
- **R2 — SP-0 coupling with issue #39.** The health gates touch the same hot path as the approved engine.rs complexity refactor; do the refactor first (or fold gates into it) to avoid churn.
- **R3 — reconciliation providers.** Exactly-once for real money-moving tools needs provider-specific idempotency/status APIs (SP-4). Providers with neither → mandatory human reconciliation.
- **R4 — cross-store atomicity.** Journal + ContextStore + CAS are separate; a crash between writes needs either a single transactional backend (preferred for `PostgresJournal`) or an outbox/reconciliation-on-resume that repairs dangling refs. Journal is the source of truth; context/CAS derived.
- **R5 — multi-instance.** v1 is single-process + durable resume; gate state and the executor are per-process. Distributed workers are a future seam.
- **Q1 — spec location & naming.** Confirm `sensei-orchestrator` as the crate family name.
- **Q2 — does SP-0 land before SP-1** or can SP-1 proceed against the current gateway (treating `QuotaExceeded` as terminal→pause) and adopt demote-to-tier when SP-0 lands? (SP-1 works either way; SP-0 is a soft dependency.)
- **Q4 — skill/tool activation policy (Phase 3).** Skills and tools are referenced by name; is activation **always-on**, **trigger/precondition-gated** (a `when` on the reference or a `description`-driven match), **planner-selected**, or **retrieval-ranked** to fit the prompt budget? The reference-by-name contract is fixed (D-agent-runtime); the activation policy is deferred to the registry spec (SP-2). Conditional tool *exposure* interacts with the effect/permission model (§7.1/§13).
- **Q3 — data-tier home (mostly answered, D12).** Phase-4 SP-DATA **extracts torii's `catalog`/`config`/`metering` layer** as an independent reusable subsystem (own crate/package) consumed by gateway + orchestrator + torii. Open sub-point: torii's catalog is **multi-tenant (RLS, `tenant_id`, per-tenant chain views like `effective_chain_models`)** — decoupling from user/tenancy means making tenancy **optional/injectable** (a default/platform scope for standalone use). Also: is OmniRoute worth wiring as a dev-only free-tier upstream (mindful of its flagged per-provider ToS-proxy cautions)?

## 20. Appendix

### 20.1 Adversarial review coverage (workloads stress-tested)
- **Coding agent** — surfaced: mutating-effect memoization corruption, exactly-once gap (crash between side-effect and record), loop-iteration effect_id collision, shared-workspace concurrency, tier-downgrade transcript overflow, tool permissioning/secret-in-journal.
- **Trip planner** — surfaced: double-booking (at-least-once + non-idempotent money effect), stale-data-on-long-resume vs determinism, HITL external-signal suspend, third-party auth/rate-limit taxonomy, saga/compensation.
- **Deep research** — surfaced: cascade-skip vs partial-failure tolerance, O(whole-history) fold at scale, context-budgeting/prompt-overflow, fan-out × rate-limit thundering herd, memoized transient failures, panel-as-atomic-effect vs partial resume, unbounded loops/budget.

All are addressed by the effect taxonomy (§7.1), two-phase + `in_doubt` (§7.3), effect_id scheme (§7.2), journal/CAS split + snapshots (§7.4), typed edges + aggregation (§10.1), context budgeting (§9), health gates (§12), HITL (§14), and the core-mechanism-+-opt-in-policy model (D7).

### 20.2 Reusable building blocks
- **From the gateway:** `Gateway::execute`/`execute_stream`, fallback chains, circuit breaker, budget/quota, panels/consensus, `attempts` trail, `GatewayStore`/`VaultStore` trait-seam pattern, `PostgresVaultStore` schema-qualification style.
- **From strategos (design to port):** coordinator ReAct + plan-driven DAG, LLM planner (self-correcting JSON + validation + cycle detection + feasibility), analyst HITL clarification, dispatcher (dependency-ordered parallel, cascade-skip, dependency-output passing), hook-based persistence over swappable backends, HITL/approval protocol, tool registry + provider extension point, and the **DB-driven catalog + CLI (32 cmds) + Hono API** management layer with `assembleConfig()` (the model for SP-DATA's control-plane).
- **From OmniRoute (reference, JS):** per-reason model lockout + escalating cooldown + reset-window awareness (`accountFallback`), proactive credential/quota expiration tracking (`providerExpiration`), cumulative retry-wait budget (`cooldownAwareRetry`), the first-class free-tier catalog with pool-dedup + ToS/privacy flags and CI-gated totals (`freeModelCatalog` / `computeFreeModelTotals`), and free-tier-aware routing strategies (`headroom`/`fill-first`/`reset-window`/`quota-share`). Its OpenAI-compatible proxy can serve as an optional free-tier **dev upstream** (`RouterConfig`) — subject to the per-provider ToS-proxy cautions it flags.
- **From torii (sibling, Rust — reuse/extract directly):** the `catalog` / `config` / `metering` Postgres schemas (providers, models, model_endpoints, model_capabilities, routers, chains, chain_models, chain_bindings, routing_policies, provider_health, usage_daily), the staging `import_*` procedures (refresh/loader), `config_loader.rs` (→ `GatewayConfig`, like `assembleConfig`), `config_versions` + `bump_config_version()` (config versioning ≈ our version-fence), and the Axum service pattern wrapping `sensei-gateway`. Shares the `keyvault.*`/`catalog.*` schema with the gateway vault. Phase-4 SP-DATA is an **extraction** of these, decoupled from torii's tenancy/governance (D12).
