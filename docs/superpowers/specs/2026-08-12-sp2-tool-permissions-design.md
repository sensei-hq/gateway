---
title: SP-2 slice 3 — tool permission declarations
doctype: design
module: orchestrator
spec: SP-2
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§132 tool declarations, §287 permission model, §340 SP-4 enforcement); ./2026-08-11-sp2-config-source-design.md (slice 1); ./2026-08-11-sp2-role-chain-resolution-design.md (slice 2)
date: 2026-08-12
---

# SP-2 slice 3 — tool permission declarations

## 1. Goal

Add the **permission declaration** layer of the tool model: a tool declares the
capabilities it *needs* (path/command/network allowlists + resource caps, §132),
an agent declares the per-tool scope it *grants* (§287, "pinned in the
`AgentDefinition`"), and `Registry::validate` statically checks that every agent's
grant **covers** its tools' declared needs. This is a **two-sided capability
model**: tool needs → agent grant → (SP-4) executor-enforced effective = grant ∩
need. This slice ships the **declarations + the static coverage check only** —
runtime enforcement, sandboxing, and workspace isolation are **SP-4** (§340),
mirroring how `effect_class` was declared in slice 2 before its enforcement landed
in slice 4.

## 2. SP-2 slicing (context)

1. `ConfigSource` adapter seam + `FilesystemConfigSource` (slice 1 — done).
2. role/kind → chain resolution (slice 2 — done).
3. **This slice** — tool permission declarations + static grant⊇need check.
4. activation policy (Q4: `when`/trigger, progressive disclosure).
5. hot-reload (reload + swap `Arc<Registry>` + version bump).

## 3. Background & impact review

- **Current `ToolSpec`** (`orchestrator-core::registry`): `{ name, description,
  input_schema, effect_class, ttl_secs, source }`, all `Serialize + Deserialize`;
  tools load from `<root>/tools/*.json` (serde). No permission field.
- **Current `AgentDefinition`**: `{ name, area, kind, chain: Option<String>, chains,
  tools, skills, system_prompt }`; loads from md frontmatter via the **flat**
  controlled-subset parser (`key: scalar` / `key: [list]`, explicitly **no
  nesting**).
- **Impact: additive.** New types + two new (defaulted) fields + one `validate`
  branch + one loader step + one error variant. The executor's tool runtime is
  **unchanged** (declarations are inert this slice). `ToolSpec` construction sites
  (code tools' `spec()`, tests) ripple mechanically to add `permissions:
  Permissions::default()`; `#[serde(default)]` keeps existing `tools/*.json` and
  grant-less agents parsing unchanged.
- **No determinism/hash impact.** Permissions/grants are policy metadata — never
  part of the assembled prompt, so they do not enter `agent_input_hash`. Resume is
  unaffected.
- New names — `Permissions`, `NetworkPolicy`, `ResourceCaps`,
  `PermissionNotGranted` — no clash; `OrchestratorError` has no exhaustive match.

## 4. Design

### 4.1 The `Permissions` type (shared, `orchestrator-core`)

```rust
/// A capability declaration — used BOTH as a tool's required needs
/// (`ToolSpec.permissions`) and an agent's per-tool grant
/// (`AgentDefinition.grants[tool]`). Secure default: deny/empty everything.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Permissions {
    #[serde(default)]
    pub paths: Vec<String>,        // allowed path prefixes
    #[serde(default)]
    pub commands: Vec<String>,     // allowed command names
    #[serde(default)]
    pub network: NetworkPolicy,    // default Deny
    #[serde(default)]
    pub caps: ResourceCaps,        // each dimension Option<u64>, default all None
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NetworkPolicy {
    Deny,
    Hosts(Vec<String>),
    Any,
}
impl Default for NetworkPolicy { fn default() -> Self { NetworkPolicy::Deny } }

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceCaps {
    #[serde(default)]
    pub cpu_ms: Option<u64>,
    #[serde(default)]
    pub mem_bytes: Option<u64>,
    #[serde(default)]
    pub wall_ms: Option<u64>,
}
```

### 4.2 Placement

- **`ToolSpec.permissions: Permissions`** — the tool's declared needs (§132).
  `#[serde(default)]` so existing `tools/*.json` without the field parse to an
  empty (needs-nothing) `Permissions`.
- **`AgentDefinition.grants: HashMap<String, Permissions>`** — per-tool scope the
  agent grants (§287). `#[serde(default)]` so grant-less agents / md-only agents
  parse to an empty map.

### 4.3 `covers` — the coverage predicate (the check's reusable core)

```rust
impl Permissions {
    /// Does `self` (a grant) cover `need` (a tool's declared needs)?
    pub fn covers(&self, need: &Permissions) -> bool;
}
```

- **paths:** ∀ needed path `p`, ∃ granted path `g` with `p.starts_with(g)`
  (grant `/workspace` covers need `/workspace/src/main.rs`).
- **commands:** every needed command is in the grant's `commands` (needed ⊆ granted).
- **network:** `Any` covers everything; `Hosts(G)` covers `Hosts(N)` iff N ⊆ G, and
  covers `Deny`; `Deny` covers only `Deny`.
- **caps:** per dimension, covered iff `need ≤ grant`, where **grant `None` =
  unlimited** (agent isn't restricting that dimension → covers any need) and **need
  `None` = no requirement** (trivially covered). (Approved decision (b).)

A tool with default/empty `permissions` needs nothing → `covers` is trivially true
for any grant (even a missing one). A tool that declares any need requires a
covering grant.

### 4.4 Encoding — the filesystem backend

- **Tool needs:** plain JSON in `<root>/tools/<name>.json` (a `"permissions": {…}`
  object; omitted ⇒ empty).
- **Agent grants:** the md frontmatter parser is flat and grants are richly nested,
  so grants live in **one central `<root>/grants.json`** (approved decision (a)) —
  the single, auditable security-policy file:
  ```json
  { "<agent-name>": { "<tool-name>": { "paths": ["/workspace"], "network": "Deny",
                                        "commands": [], "caps": {} } } }
  ```
  `FilesystemConfigSource::load` reads it via the slice-2 `read_optional_file`
  helper, deserializes to `HashMap<String, HashMap<String, Permissions>>`, and
  **merges** each agent's map into that `AgentDefinition.grants` after the md parse.
  A **missing `grants.json`** ⇒ no grants (empty). A **malformed `grants.json`** ⇒
  loud `RegistryLoad` naming the file. A grants entry naming an **unknown agent** ⇒
  loud `RegistryLoad` (never a silently-ignored policy line — a security-relevant
  fail-loud).
- `InMemoryConfigSource` carries grants on the `AgentDefinition`s directly.

### 4.5 Static validation + new error

Extend `Registry::validate` (after the dangling-ref + routability checks): for each
agent, for each tool name it references, if the resolved `ToolSpec.permissions` is
non-empty (declares needs), require `agent.grants.get(tool)` to exist and
`.covers(&tool.permissions)`; otherwise →
`OrchestratorError::PermissionNotGranted { agent, tool }` (mirrors
`UnknownToolRef`). A tool with empty needs requires no grant. This is a
**declaration-time** check, not runtime enforcement.

### 4.6 Decisions

- **D1 — two-sided model, declarations only.** Tool declares needs; agent grants
  scope; `validate` checks grant⊇need statically. Runtime effective-permission
  computation + sandbox = SP-4.
- **D2 — secure defaults.** `Permissions::default` = deny/empty; `NetworkPolicy`
  default = `Deny`. Absence of a grant for a tool that needs something = load
  failure (least privilege, fail-loud).
- **D3 — caps `None` = unlimited on the grant side** (approved (b)); need `None` =
  no requirement. Keeps the common case (agent doesn't cap) frictionless; SP-4 may
  tighten runtime defaults.
- **D4 — central `grants.json`** (approved (a)) for the filesystem backend — one
  auditable policy file, and it sidesteps the flat-frontmatter nesting limit.
  Grants remain a domain field ON `AgentDefinition` (§287); the loader merges.
- **D5 — command allow-lists only this slice.** Deny-lists (subtraction) are an
  enforcement-time refinement → SP-4.
- **D6 — inert; no prompt/hash/executor impact.** Permissions are not sent to the
  model and the tool runtime is unchanged.
- **D7 — `#[serde(default)]` on every new field** (learned from slice 2) so DB/HTTP
  backends and partial JSON deserialize cleanly.

## 5. File formats

`<root>/tools/fs-write.json`:
```json
{ "name": "fs.write", "description": "write files", "input_schema": {"type":"object"},
  "effect_class": "Mutation", "ttl_secs": null, "source": null,
  "permissions": { "paths": ["/workspace"], "commands": [], "network": "Deny", "caps": { "wall_ms": 5000 } } }
```
`<root>/grants.json`:
```json
{ "coder": { "fs.write": { "paths": ["/workspace"], "network": "Deny" } } }
```
(`NetworkPolicy` serializes as the unit string `"Deny"`/`"Any"` or `{"Hosts":[…]}`;
omitted `Permissions` sub-fields default via `#[serde(default)]`.)

## 6. Deferred (stated → SP-4)

Runtime enforcement (gate tool execution on effective = grant ∩ need); sandbox /
workspace isolation; command **deny**-lists; network egress enforcement; resource-cap
enforcement (cgroups/timeouts); secret redaction; per-invocation effective-permission
at execution.

## 7. Acceptance criteria (TDD)

1. **`covers` semantics.** Unit tests per dimension: paths (prefix covers; non-prefix
   fails), commands (subset covers; extra needed fails), network (`Any`⊇all,
   `Hosts`⊇subset & ⊇`Deny`, `Deny` only ⊇`Deny`), caps (need ≤ grant covers; need >
   grant fails; grant `None` covers any; need `None` trivially covered). Empty needs
   → covered by anything.
2. **Serde defaults.** A `ToolSpec` JSON without `permissions` → empty `Permissions`;
   a `Permissions` JSON omitting sub-fields → each defaults (network `Deny`, caps
   `None`); an `AgentDefinition` JSON without `grants` → empty map.
3. **`validate` grant⊇need.** An agent referencing a tool with declared needs and no
   covering grant → `PermissionNotGranted { agent, tool }`; with a covering grant →
   ok; a tool with empty needs → ok with no grant. Well-formedness: `from_config`
   surfaces the error.
4. **Filesystem `grants.json`.** `FilesystemConfigSource` merges `grants.json` into the
   right `AgentDefinition.grants`; missing file ⇒ empty; malformed ⇒ loud
   `RegistryLoad` naming the file; a grant for an unknown agent ⇒ loud `RegistryLoad`.
5. **Additive.** Existing `tools/*.json`, code-tool `spec()`s, and grant-less agents
   load/validate byte-identically in behavior (only the mechanical
   `permissions: Permissions::default()` field additions differ); the executor and
   all current tests are behaviorally unchanged.
6. **End-to-end.** A `from_config` registry with a tool declaring a path/network need
   and an agent granting a covering scope validates and drives a normal agent turn
   through the test gateway (proving declarations are inert — the tool still runs as
   a Pure tool with no enforcement); removing the covering grant makes `from_config`
   fail `PermissionNotGranted` (mutation-verified).
