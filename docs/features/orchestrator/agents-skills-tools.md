---
title: Agents · Skills · Tools
doctype: feature
module: orchestrator
status: partial
phase: 3
spec: SP-1, SP-2
source: crates/orchestrator*
---

# Agents · Skills · Tools

> **Status: Partial (Phase 3 · SP-1 slice 2 + SP-2 slice 1 + SP-2 slice 2 + SP-2 slice 3 + SP-2 slice 4 + SP-2 slice 5 (SP-2 complete)).** Design §6/§9;
> config-source design
> [`../../superpowers/specs/2026-08-11-sp2-config-source-design.md`](../../superpowers/specs/2026-08-11-sp2-config-source-design.md).
> **SP-2 slice 1 — pluggable config loading:** the `Registry` now loads from a
> backend-agnostic **`ConfigSource`** seam (`load() -> RegistryConfig` of domain
> objects — no serialization format in the contract). `Registry::from_config`
> assembles + **rejects duplicate names** + `validate`s (the single, shared
> assembly point). Backends: **`FilesystemConfigSource`** (`<root>/agents/*.md` +
> `<root>/skills/*.md` via `from_frontmatter`, `<root>/tools/*.json` via serde —
> ALL md/JSON parsing isolated here; missing subdir ⇒ empty, missing root / bad
> file ⇒ loud `RegistryLoad`) and **`InMemoryConfigSource`**. `ConfigSource` is
> the **extension seam** (`PostgresConfigSource`/`ConvexConfigSource`/`HttpConfigSource`
> impl it later, reusing `from_config` unchanged); `Registry` is the uniform
> assembled result. The in-memory `.with_*` builders + `from_frontmatter` stay.
> Tool **executors** still bind via `ToolRegistry` (a disk `ToolSpec` with no code
> executor loads/validates but is a loud `UnknownTool` at execution — MCP bridge
> deferred). **Deferred (later SP-2 slices):** tool
> permission declarations, activation policy (Q4), hot-reload.
>
> **SP-2 slice 2 — role/kind → chain resolution:** an agent declares `(area, kind)`
> (plus an optional explicit `chain` and an optional per-phase `chains` map) and
> `Registry::resolve_chain(agent, phase)` yields the concrete gateway chain-id
> (order: per-phase → explicit → `(area,kind)` binding → loud `UnknownChainRef`).
> `chain` is now optional; the `(area,kind)` policy table loads from
> `<root>/chains.json`. Phase is an `Agent`-node attribute (not a mid-loop
> transition). **Deferred:** tiers (gateway-catalog), planner-driven phase
> transitions, tenant dimension (multi-tenancy is by composition — per-tenant
> `Executor` = per-tenant `Gateway` + tenant-scoped `ConfigSource`).
>
> **SP-2 slice 3 — tool permission declarations:** a tool declares the capabilities
> it needs (`ToolSpec.permissions`: path/command/network allowlists + resource caps,
> secure-default deny) and an agent declares per-tool grants (`AgentDefinition.grants`,
> loaded from a central auditable `<root>/grants.json`). `Registry::validate` rejects
> any agent whose grant does not **cover** a referenced tool's declared needs
> (`PermissionNotGranted`); `Permissions::covers` is the shared predicate (path-prefix,
> command subset, network `Any`/`Hosts`/`Deny` lattice, caps `need ≤ grant` with
> grant-`None` = unlimited). Declarations are **inert** — not in the prompt/hash, tool
> runtime unchanged this slice. **✅ SP-4 slice 1 turned these declarations into runtime
> enforcement** (authorization gate + the `covers()` hardening — see the SP-4 note below).
> **Still deferred (later SP-4 slices):** runtime *confinement* (a sandbox intercepting
> fs/network so a tool that under-reports its needs can't exceed its grant) + resource-cap
> *killing*, workspace isolation, command deny-lists, secret redaction.
>
> **SP-2 slice 4 — skill/tool activation policy (Q4):** skills/tools carry a
> definition-level `Activation` (`Always` default, or `OnKeywords`) — `SkillDef`
> frontmatter `activate_on: [..]`, tool JSON `"activation"`. `assemble_prompt` composes
> a skill body / tool schema only when `activation.is_active(query)` for the agent's
> rendered input (matched once per run, case-insensitive substring ANY-of) —
> progressive disclosure to fit the prompt budget. `Always` is byte-identical to the
> old behavior; over-budget still halts loud (no silent truncation). Determinism-safe
> (the query is the node input, already in `agent_input_hash`). Activation gates prompt
> **disclosure**, not execution: a tool gated out of a run's prompt simply isn't offered
> to the model that run — the permission grants (slice 3), validated at load, remain the
> security boundary. **Deferred:** per-agent
> override, planner-selected activation (SP-3), retrieval-ranked / semantic match (SP-7),
> per-turn re-activation, prompt compaction (SP-7).
>
> **SP-2 slice 5 — registry hot-reload (closes SP-2):** a `RegistryHandle`
> (`orchestrator-core`) wraps a swappable `Arc<Registry>` + a config generation;
> `reload(source)` is atomic, validated (`Registry::from_config`), and last-good (a
> failed load/validate keeps the old config live). `Executor::with_registry_handle`
> pins the handle's `(registry, generation)` once per run, folding the generation
> into the fence version (`"{base}#cfg{gen}"`). A reload takes effect for NEW runs;
> resuming an in-flight run after a reload is refused loud via the existing
> `VersionFenceMismatch` (one config generation per run). No handle wired ⇒
> byte-identical. **Deferred:** version-pinned resume, an `on_config_reloaded` hook,
> file-watch/auto-reload, and a persistent cross-process config version (SP-DATA
> `config_versions`).
>
> **SP-4 slice 1 — tool permission ENFORCEMENT (runtime authorization):** the SP-2‑s3
> declarations now gate execution. In `execute_tool_effect` (the single chokepoint for
> Pure/Observation/Mutation calls), a tool call is denied unless `tool ∈ agent.tools`
> **and** `agent.grants[tool].covers(tool.required(args))`. **`Tool::required(&self,args)
> -> Permissions`** (default = static `spec().permissions`) reports a call's **concrete**
> needs, so a grant may be *narrower* than the tool's declared surface (a runtime
> **ceiling**) — the load-time full-surface `validate` check is **dropped** (a narrow
> grant is legal, enforced per-call). `covers()` is now **component-aware** (`/work` ⊄
> `/workspace-secret`; empty grant path rejected; `..` rejected) with **host wildcards**
> (`*.example.com`). A denial records a **Pure `EffectRecorded`** (no tool run; **no
> `EffectIntent`** for a Mutation) and is fed back to the agent as a **terse** tool-result
> error (never echoes the grant → confused-deputy defense) ⇒ a resume replays it from the
> memo, tool never re-invoked. **Authorizes, does not confine:** a tool that under-reports
> its `required` bypasses the gate — runtime confinement + cap-killing = the sandbox slice.
> No-permission tools + agents that list them are byte-identical.
>
> **SP-4 credential broker — ephemeral secret injection:** lets a tool authenticate to an
> external system **without the secret ever reaching the model or the durable journal**
> (completes the SP-4 arc: s1 authorizes, s2 redacts outputs, s5 makes writes exactly-once,
> the broker provides the credential out-of-band). A tool **declares** its credential refs
> on `ToolSpec.credentials: Vec<String>` (alongside `permissions`); `record_tool_effect`
> resolves each ref via an injected async **`CredentialBroker`** (`Executor::with_credential_broker`,
> default none; demo `StaticCredentialBroker`, real impl wraps `vault::Vault`) and injects
> the resolved **`Secret`**s (`Zeroizing<String>`, `[REDACTED]` Debug, audited `expose()`)
> into `ToolContext.credentials: Arc<HashMap<String,Secret>>` — resolved **before** the sync
> `call_ctx` (forced by the async-broker / sync-tool boundary). **Ephemeral:** never
> journaled / hashed (`input_hash` is over `args`) / re-injected on a memoized resume (the
> broker is not re-consulted for a replayed tool). **Echo-leak closed** by a per-call
> exact-value scrub of the tool's output (pure over *this* call's output + creds — not a
> run-wide set that would diverge on resume); it runs **before** the s2 pattern redactor so
> a wrapped/composite secret can't be fragmented past the exact-value match. **Fail-loud:**
> a declared ref with no broker / an unresolved (`None`) / an errored broker → journal
> `NodeFailed` + `ToolOutcome::Failed`, tool never runs. **Confused-deputy safe:** a tool
> sees only its *own* declared creds. **No broker + empty `credentials` ⇒ byte-identical.**
> **Deferred:** sandbox egress confinement + resource-cap killing (blocked on the
> tool-execution-model), the real vault-backed broker, per-tenant credential scoping.

Externally-configured **agents** (md+frontmatter: name, area, kind, chain(s),
tools, skills, subagents, system-prompt body), **skills** (injectable
instruction modules), and **tools** (executable capabilities with an effect
class + permissions). The agent runtime assembles a budgeted prompt, resolves
the role→chain, calls the gateway, and runs a ReAct/tool loop.

## Scenarios

```gherkin
Feature: Agent runtime
  Scenario: An agent's chain is resolved from its role/kind
    Given a coding-planner agent with chain "plan.frontier"
    Then its model calls route through the plan.frontier chain

  Scenario: Skills are composed into the system prompt
    Given an agent listing skills [clean-code, security-compliance]
    Then those skill modules appear in the assembled system prompt

  Scenario: The runtime executes tool calls the gateway returned
    Given the model returns a tool call for "fs.read"
    Then the orchestrator executes fs.read (the gateway does not) and feeds the result back

  Scenario: Prompt is budgeted to the smallest model in the chain
    Given a chain whose smallest model has a 32k context window
    Then prompt assembly fits within 32k (summarize/select, never silent truncation)
```

## Slice 2 (implemented)

- An in-memory **registry** (`AgentDefinition` / `SkillDef` / `ToolSpec`) with a
  md+frontmatter-subset parser (`from_frontmatter`) and `Registry::validate`
  (dangling agent/skill/tool refs are a loud load-time error).
- **Prompt assembly** (`assemble_prompt`: system-prompt body + each listed
  skill's body, in order) with **per-turn window budgeting** (`over_budget`) —
  halt-loud when a turn's estimate exceeds the chain's smallest context window;
  no silent truncation.
- A **Pure-only tool runtime** (`Tool` / `ToolRegistry` + the demo `calc` tool);
  Observation/Mutation tools are rejected loud (`ToolEffectDeferred`), never
  silently skipped or run early.
- `NodeKind::Agent`, driving a durable **ReAct loop** (`drive_agent`) where each
  turn's model call is a Pure `ModelCall` effect and each Pure tool call is its
  own Pure effect — so resume-without-re-spend (the durable-executor spine)
  extends into the loop, not just the top-level graph.

**Deferred:** Observation/Mutation tools + TTL/two-phase/reconcile (slice 4);
a filesystem directory loader for the registry; a summarize/select budgeting
strategy (today's over-budget turn halts rather than compacting);
blackboard/shared-context, `Map` fan-out, subagents, per-phase chains, and
streaming (slice 3+).

Source: `crates/orchestrator/src/agent/*` + `executor.rs` (`drive_agent`).

## Notes

- Tool execution + permissions are the orchestrator's job — the gateway only returns `tool_calls` (see [inference/tool-calling](../inference/tool-calling.md)).
