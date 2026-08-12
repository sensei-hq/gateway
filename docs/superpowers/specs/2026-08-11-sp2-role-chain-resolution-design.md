---
title: SP-2 slice 2 — role/kind → chain resolution
doctype: design
module: orchestrator
spec: SP-2
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§6.1 AgentDefinition, §6.2/§122 tiers×chains, §195 resolve-chain); ./2026-08-11-sp2-config-source-design.md (slice 1)
date: 2026-08-11
---

# SP-2 slice 2 — role/kind → chain resolution

## 1. Goal

Add a **role→chain resolution layer** to the orchestrator `Registry`. An agent
declares *intent* — its `(area, kind)` role, optionally an explicit `chain`
override, optionally a per-phase `chains` map — and the registry resolves that to
a concrete gateway **chain-id** that `drive_agent` routes through. One policy edit
(the `(area,kind)→chain` table) re-points a whole class of agents; the gateway
still owns chain-id → models (D13). This slice is purely the orchestrator-side
resolution seam: **no tiers, no mid-loop phase transitions, no tenant dimension.**

## 2. SP-2 slicing (context)

1. `ConfigSource` adapter seam + `FilesystemConfigSource` (slice 1 — done).
2. **This slice** — role/kind → chain resolution (agents reference a role;
   registry binds `(area,kind)→chain`; optional explicit + per-phase overrides).
3. tool permission declarations (path/command/network allowlists — declarations
   only, enforcement = SP-4).
4. activation policy (Q4: `when`/trigger, progressive disclosure).
5. hot-reload (reload + swap `Arc<Registry>` + version bump).

## 3. Background & impact review

- **Current chain handling.** `AgentDefinition.chain: String` is a **literal
  gateway chain-id** (e.g. `research.bulk`). `drive_agent`
  (`executor/agent.rs:65`) reads `agent.chain.clone()` straight into
  `gateway.min_context_window(&chain)`, `agent_input_hash(&chain, …)`,
  `build_chat_request(&chain, …)`, and the `on_agent_started(…, &chain)` hook.
  There is **no indirection** — every agent hardcodes a concrete gateway chain.
- **The gateway is config-parameterized and tenant-agnostic.**
  `GatewayConfig { routers, models, chains: HashMap<String, FallbackChainConfig> }`
  is *handed to* the gateway (`GatewayBuilder::from_config` / `Gateway::new`); an
  authoring-time `CatalogConfig` (routers, models, tiers, tier-ref chains) is
  expanded by `catalog::assemble()` — done by the **embedder** (torii's
  `config_loader`, D13/D24), not the gateway. The word `tenant` appears nowhere in
  the gateway. → **multi-tenancy is by composition** (D5), not a core API change.
- **Impact: mostly additive, ONE non-additive ripple.** `chain: String →
  Option<String>` forces every literal `AgentDefinition { chain: … }` construction
  and the `required_scalar("chain")` parse to become `Some(…)` / an
  `optional_scalar` — a mechanical, type-level churn. **Behavior for explicit-chain
  agents is preserved** (they hit the override branch of `resolve_chain`), so the
  demo catalog + fan-out e2e route byte-identically.
- **New names, no clash, no exhaustive match.** `ChainBinding`,
  `RegistryConfig.chain_bindings`, `AgentDefinition.chains`, `NodeKind::Agent.phase`,
  and `OrchestratorError::UnknownChainRef` are all new; `OrchestratorError` has no
  exhaustive match, so adding a variant is safe.

## 4. Design

### 4.1 The resolver (pure, `orchestrator-core`)

```rust
impl Registry {
    /// Resolve an agent's concrete gateway chain-id for an optional phase.
    /// Order: per-phase override → explicit `chain` → `(area,kind)` binding →
    /// loud `UnknownChainRef`. A phase key the agent doesn't define is NOT an
    /// error — it falls through to the explicit/table branches.
    pub fn resolve_chain(
        &self,
        agent: &AgentDefinition,
        phase: Option<&str>,
    ) -> Result<&str, OrchestratorError>;
}
```

Resolution order (first match wins):

1. **per-phase override** — `phase == Some(p)` **and** `agent.chains.get(p)` → it.
2. **explicit override** — `agent.chain.as_deref()` → it (today's behavior).
3. **role binding** — `self.chain_bindings.get(&(agent.area, agent.kind))` → it.
4. else → `Err(OrchestratorError::UnknownChainRef { agent: agent.name.clone() })`.

The returned `&str` borrows from `&self`/`&agent`; the executor clones it into
`AgentRun.chain` (as it clones `agent.chain` today).

### 4.2 Types (`orchestrator-core`)

```rust
/// A registry role binding: `(area, kind)` → a gateway chain-id. The policy
/// table that lets one edit re-point every agent of that role (§122).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainBinding { pub area: String, pub kind: String, pub chain: String }

pub struct RegistryConfig {
    pub agents: Vec<AgentDefinition>,
    pub skills: Vec<SkillDef>,
    pub tools:  Vec<ToolSpec>,
    pub chain_bindings: Vec<ChainBinding>,   // NEW — (area,kind) → chain
}

pub struct AgentDefinition {
    pub name: String,
    pub area: String,
    pub kind: String,
    pub chain: Option<String>,               // CHANGED — now an optional override
    pub chains: HashMap<String, String>,     // NEW — phase → chain (empty default)
    pub tools: Vec<String>,
    pub skills: Vec<String>,
    pub system_prompt: String,
}
```

- `Registry` stores `chain_bindings: HashMap<(String, String), String>`.
- **`from_config`** rejects a duplicate `(area,kind)` binding loudly —
  `RegistryLoad("duplicate chain binding: {area}/{kind}")` — mirroring dup-name
  detection (the `Vec` is checked before the `HashMap` collapses it).
- **`validate()`** gains a **routability** check: every agent must resolve for the
  **no-phase** case — `agent.chain.is_some() || chain_bindings.contains_key(&(area,
  kind))` — else `UnknownChainRef { agent }` (mirrors `UnknownSkillRef` /
  `UnknownToolRef`). Per-phase entries are **not** validated (see D3).

### 4.3 Frontmatter encoding (flat controlled subset — no nesting, D6)

The parser's contract is "not general YAML — no nesting". Both new fields keep
that contract:

- **`chain:`** is now optional — a new `optional_scalar` helper (absent → `None`).
- **`chains:`** reuses the existing inline-list syntax with `phase=chain` pairs:
  ```
  chain: plan.frontier                       # optional now
  chains: [plan=plan.frontier, execute=code.mid, reflect=reason.frontier]
  ```
  Each element is `split_once('=')` → `(phase, chain)`; a malformed element (no
  `=`, or empty phase/chain) → `FrontmatterParse`. Absent/empty → empty map.
- The `(area,kind)→chain` **policy table is NOT per-agent** — it lives in a
  separate source (4.5), so nothing about it touches agent frontmatter.

### 4.4 Phase plumbing (executor)

- **`NodeKind::Agent { agent, input, phase: Option<String> }`** — new optional
  field. Existing constructions/tests set `phase: None` (mechanical).
- **`MapBody::Agent` stays phase-less** — fan-out is a different axis from an
  agent's plan/execute/reflect phases (YAGNI); its children resolve with
  `phase = None`.
- **`drive_agent(run, node_id, agent_ref, input, context, fold, phase: Option<&str>)`**
  — `executor/agent.rs:65` becomes
  `let chain = self.registry.resolve_chain(agent, phase)?.to_string();`.
  Everything downstream (`min_win`, `agent_input_hash`, `build_chat_request`, the
  `on_agent_started` hook) uses the resolved chain unchanged. `run_node` passes the
  Agent node's `phase.as_deref()`; `run_map` / `run_consolidate` pass `None`.
- **Determinism / resume-safety (D5-fence untouched).** `resolve_chain` is a pure
  function of `(registry, agent, phase)`. Registry *content* is already fenced by
  `agent_input_hash` (D5, slice 1). `phase` is a **graph attribute** — the caller
  provides the identical graph on resume, the same contract that already governs
  every node. Because the resolved chain feeds `agent_input_hash`, a divergent
  `phase` (or a changed binding table) on resume changes the hash and is caught by
  the existing `DeterminismViolation` guard — never a silent divergence.

### 4.5 Filesystem backend (`orchestrator-store`)

- **`<root>/chains.json`** = a JSON array of `{ "area", "kind", "chain" }` →
  `RegistryConfig.chain_bindings`. A **missing** `chains.json` ⇒ an empty table
  (like a missing `agents`/`skills`/`tools` subdir). A malformed `chains.json` ⇒ a
  loud `RegistryLoad` naming the file (uniform with `tools/*.json`).
- Agents' per-phase `chains` come from their own `.md` (4.3), **not** this file —
  `chains.json` is only the `(area,kind)` policy table.
- `InMemoryConfigSource` carries `chain_bindings` verbatim.

### 4.6 Decisions

- **D1 — resolution order** is phase → explicit `chain` → `(area,kind)` → loud
  `UnknownChainRef`. A phase key the agent doesn't define falls through (never an
  error), matching the layered "override" intent of §6.1.
- **D2 — `chain` becomes `Option<String>`** (the sole non-additive change).
  Explicit-chain agents route byte-identically via branch 2, so the demo + e2e are
  behaviorally unaffected; the churn is purely the mechanical `Some(…)` / field
  updates.
- **D3 — validate checks *routability*, not chain-id existence.** The gateway owns
  chain-ids (unchanged from today, where chain strings were never validated against
  the gateway). Per-phase keys aren't validated because which phases a run requests
  isn't known at load time. A resolved-but-nonexistent chain-id surfaces as a loud
  **gateway** error, never a silent default.
- **D4 — phase is a node attribute**, fixed per `Agent` node — not a mid-loop
  transition. Mid-run phase changes / planner-driven phases are SP-3.
- **D5 — multi-tenancy by composition.** A tenant = a per-tenant `Executor` built
  from a per-tenant `Gateway` (embedder-assembled catalog) + a `Registry` from a
  tenant-scoped `ConfigSource` (slice 1). No `tenant_id` in `ConfigSource::load`,
  `resolve_chain`, or the gateway request path — the gateway and orchestrator core
  stay tenant-agnostic. **Deployment invariant:** the embedder assembles a tenant's
  `Registry` and its `GatewayConfig` from the *same* tenant config, so a chain-id
  the registry emits exists in that gateway's catalog.
- **D6 — flat encoding** for both new structures (inline `phase=chain` pairs; a
  separate `chains.json`), preserving the "no nesting" controlled-subset contract.
- **D7 — one new error variant** (`UnknownChainRef { agent }`); a duplicate
  `(area,kind)` reuses `RegistryLoad`.

## 5. `chains.json` file format

`<root>/chains.json`:
```json
[ { "area": "coding",   "kind": "reasoning", "chain": "plan.frontier" },
  { "area": "research", "kind": "bulk",      "chain": "research.bulk" } ]
```

## 6. Deferred (stated)

- **Tiers** (D13) — gateway-catalog / SP-DATA (`assemble` already lives in the
  gateway; the orchestrator only names chain-ids).
- **Mid-loop phase transitions / planner-driven phases** — SP-3.
- **Per-phase chains on fan-out** (`MapBody::Agent`) bodies.
- **Tenant dimension in the seam** (per-tenant persistence) — SP-DATA.
- **Cross-layer validation** of resolved chain-ids against the gateway catalog.

## 7. Acceptance criteria (TDD)

1. **Resolution order.** An agent with `chains["plan"]` + `phase=Some("plan")` →
   the phase chain; drop the phase key (or `phase=None`) but keep an explicit
   `chain` → the explicit chain; drop the explicit chain but add an `(area,kind)`
   binding → the binding; drop all three → `UnknownChainRef` naming the agent.
2. **Phase fall-through.** `phase=Some("execute")` on an agent that defines only
   `chains["plan"]` falls through to the explicit/`(area,kind)` chain — not an error.
3. **Load-time guards.** `from_config` rejects a duplicate `(area,kind)` binding
   loudly (`RegistryLoad`); `validate()` rejects an agent that resolves to nothing
   (`UnknownChainRef`), and accepts one routable only via the `(area,kind)` table.
4. **Frontmatter.** `chain:` absent → `None`; `chains: [plan=a, execute=b]` → the
   `{plan:a, execute:b}` map; a malformed pair (`chains: [bad]`) → `FrontmatterParse`.
5. **Filesystem.** `chains.json` → `chain_bindings`; a missing `chains.json` → an
   empty table (no panic); a malformed `chains.json` → a loud `RegistryLoad` naming
   the file.
6. **Executor e2e.** An `Agent` node whose agent **omits** `chain` routes via the
   `(area,kind)` table (observable: the gateway/`min_context_window` receives the
   bound chain-id, and a real turn is driven); an `Agent` node with `phase=Some("plan")`
   routes via `chains["plan"]`.
7. **Additive behavior.** Existing explicit-chain agents + all current tests route
   byte-identically (same resolved chain, same journal); the only diff is the
   mechanical `Option`/`phase`/`chains` field updates.
