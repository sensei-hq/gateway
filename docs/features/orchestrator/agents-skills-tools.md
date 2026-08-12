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

> **Status: Partial (Phase 3 · SP-1 slice 2 + SP-2 slice 1 + SP-2 slice 2 + SP-2 slice 3).** Design §6/§9;
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
> runtime unchanged. **Deferred to SP-4 (enforcement):** runtime gating on effective =
> grant ∩ need, sandbox/workspace isolation, command deny-lists, secret redaction — and
> hardening the declaration-layer coverage before it gates real access: **path matching
> is raw string-prefix (not path-component-aware, and an empty grant path `""` = allow-all),
> and `Hosts` matching is exact-host (no subdomain/wildcard)** — SP-4 must canonicalize
> paths / reject empty allow-all grants / define host-wildcard semantics.

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
