---
title: SP-4 slice 1 — Tool permission enforcement (authorization gate + per-call arguments)
doctype: design
module: orchestrator
spec: SP-4
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§7.1 effect taxonomy, §13 enforcement & isolation, §287-290 tool permission model/sandbox/redaction/workspace); ./2026-08-12-sp2-tool-permissions-design.md (the SP-2 s3 DECLARATION model — `Permissions`/`covers()`/`grants`, shipped inert "enforcement = SP-4"); ./2026-08-10-sp1-slice4-observation-mutation-design.md (the effect-class dispatch + two-phase Mutation this gate sits in front of)
date: 2026-08-14
---

# SP-4 slice 1 — Tool permission enforcement

## 1. Goal

Turn the SP-2 slice-3 tool-permission **declarations** (`Permissions`/`covers()`/
`AgentDefinition.grants`, shipped deliberately inert) into **runtime enforcement**: before
any tool call executes, require that the tool is one the agent declares (`tool ∈
agent.tools`) **and** that the agent's grant covers the **concrete permissions that specific
call needs** (`grant.covers(tool.required(args))`). A denied call is fed back to the agent as
a journaled tool-result error (the agent can adapt), never silently dropped. Along the way,
make `covers()` matching sound (path-**component**-aware, host wildcards, no empty-path
allow-all) and resolve which permission source is authoritative at runtime.

This closes the live hole where `execute_tool_effect` dispatches **any** `call.name` straight
to the executable `ToolRegistry` with no grant check (master spec §13; SP-2 s3 prereqs
#1/#2/#4/#5). It is the **authorization** layer of SP-4; **runtime confinement** (a sandbox
that stops a tool from exceeding its grant at the syscall level) and **resource-cap killing**
are the sandbox slice (§6).

## 2. SP-4 slicing (context)

SP-4 = "Mutation & exactly-once + isolation" (master spec §16), decomposed:

1. **This slice** — permission **enforcement** (authorization gate + per-call args).
2. Secret **redaction** before journaling effect I/O.
3. **Workspace isolation** (git worktree/CoW + resource locks for parallel branches).
4. **Sandbox** shell (container/jail) + ephemeral credential broker + resource-cap *killing*.
5. Exactly-once **hardening** (real reconcile providers, author-supplied idempotency keys,
   saga/compensation).

Slices 2–5 are provisional (each re-brainstormed when reached). This slice depends only on
SP-1 (done) and the SP-2 registry (done); it adds **no new infrastructure**.

## 3. Background & impact review

- **What exists (SP-2 s3, inert):** `Permissions { paths, commands, network: NetworkPolicy,
  caps: ResourceCaps }` + `Permissions::covers(need)`; `ToolSpec.permissions` (a tool's
  declared needs) + `AgentDefinition.grants: HashMap<tool, Permissions>` (an agent's per-tool
  grant). `Registry::validate` checks `grant.covers(spec.permissions)` **at load time** and
  errors `PermissionNotGranted` — but the executor never consults any of this at runtime.
- **The runtime gap:** `execute_tool_effect` (`executor/agent.rs`) computes
  `tool_input_hash`, reads `self.tools.spec_of(name)` (the **executable** `ToolRegistry`, not
  the core `Registry`) for the `effect_class`, and dispatches by class — Pure/Observation
  replay-or-run, Mutation two-phase — with **no** `tool ∈ agent.tools` or `grant.covers` check.
  An LLM that hallucinates a call to an unlisted/ungranted tool gets it executed.
- **Two sources of truth (prereq #5):** the core `Registry.ToolSpec.permissions` is the
  authoritative *declared* boundary (what `validate` reads); the executable `Tool::spec()`
  carries its own copy (what the executor reads). This slice resolves it (§4.2).
- **`covers()` soundness (prereqs #1/#2):** paths match by raw `starts_with` (so `/work`
  covers `/workspace-secret`, and an empty grant path `""` covers everything); `Hosts` is
  exact-match (no subdomain/wildcard).
- **Impact:** additive by construction — every current tool declares empty `permissions`, so
  its `required(args)` is empty and `grant.covers(empty) == true`; all existing agent/tool/Map/
  Loop/Consolidate behavior stays **byte-identical**. The one non-additive ripple is relaxing
  the load-time `validate` rule (§4.2) and its SP-2 s3 tests.

## 4. Design

### 4.1 The enforcement point

A single gate at the **top of `execute_tool_effect`** (`executor/agent.rs`), before the
`effect_class` match — the chokepoint every Pure/Observation/Mutation call flows through, so a
denied **Mutation never journals an `EffectIntent`**:

```rust
// acting agent resolved from self.registry via the AgentRef already in scope in drive_agent
let need = self.tools.required_of(&call.name, &call.arguments); // concrete needs for THIS call
let allowed = agent.tools.iter().any(|t| t == &call.name)
    && agent.grants.get(&call.name).unwrap_or(&Permissions::default()).covers(&need);
if !allowed {
    // §4.4 — record a denial tool-result, feed it back to the agent
}
```

`drive_agent` already resolves the `AgentDefinition` (for `assemble_prompt` + `resolve_chain`);
its `tools` + `grants` are threaded to `execute_tool_effect` (or re-looked-up from
`self.registry.agent(name)`). The gate is a **pure function of (config grant, call args)**.

### 4.2 `required(args)` + grant-as-ceiling

- **`Tool` trait** gains a per-call needs method with a default:
  ```rust
  fn required(&self, _args: &serde_json::Value) -> Permissions { self.spec().permissions }
  ```
  The default returns the tool's static declaration ⇒ **every existing tool is unchanged**. A
  tool whose arguments carry permission-relevant values overrides it (e.g. a file writer →
  `Permissions { paths: vec![args["path"].as_str()...], ..Default::default() }`). The tool
  **owns its argument semantics** (a URL→host parse, a shell string→argv split live in the
  tool, not the executor). `ToolRegistry::required_of(name, args) -> Permissions` mirrors
  `spec_of` (unknown tool → empty `Permissions`, which combined with the `tool ∈ agent.tools`
  check denies it).
- **Meaning shift (the crux):** `ToolSpec.permissions` is now the tool's **maximum surface**
  (menu disclosure / documentation — the most it could ever need); `agent.grants[tool]` is the
  **runtime ceiling**, which may be *narrower* than the surface. Per-call authorization
  (`grant.covers(required(args))`) is what makes a narrow grant meaningful — grant `fs.write`
  only `/workspace` even though the tool's surface is `/`.
- **Load-time `validate` relaxes:** the SP-2 s3 hard `grant.covers(spec.permissions)` check
  (grant must cover the *full surface*) is **dropped** — a narrower grant is now legal and
  enforced per-call. `validate` keeps the **structural** checks (a `grants` entry names a tool
  the agent actually lists; listed tools/skills resolve). A missing grant for a listed tool ⇒
  `Permissions::default()` (deny-all) ⇒ any non-empty `required` is denied at runtime; a listed
  tool whose `required` is empty (a Pure calc-style tool) passes trivially.
- **Authoritative source (prereq #5):** the **grant** is always the core `Registry`'s
  `agent.grants` (config — the authoritative boundary). The **`required(args)`** comes from the
  executable `Tool` (it alone knows the arguments' meaning). The static `spec.permissions` is
  the disclosure surface; where the core `ToolSpec` and executable `Tool::spec()` could drift,
  the **core `Registry` is authoritative for what is offered/granted**, the executable `Tool`
  for per-call `required`. (Unifying the two spec copies is out of scope; documented.)

### 4.3 `covers()` hardening

- **paths → component-aware.** Compare normalized path **segments**: a grant `g` covers a need
  `n` iff `g`'s segments are a prefix of `n`'s segments. `/workspace` covers `/workspace` and
  `/workspace/sub`, **not** `/workspace-secret`. An **empty grant path `""` is rejected** (no
  longer allow-all). A need containing `..` is rejected lexically (no traversal past a grant
  root). Matching is lexical — true symlink/`realpath` confinement is the sandbox's job (§6).
- **network `Hosts` → wildcard.** A grant host `example.com` matches exactly `example.com`; a
  grant `*.example.com` matches any single-or-multi-label `<sub>.example.com`; comparison is
  case-insensitive. `Any`/`Deny` unchanged.
- **commands** stay `needed ⊆ granted` (exact names). A future `shell` tool's `required`
  extracts argv[0] best-effort; robust shell-string parsing rides with the sandbox slice.
- **caps** `covers()` is unchanged (SP-2 s3 semantics: grant `None` = unlimited); this slice
  *authorizes* against declared caps but does not *kill* on overage (§6).

### 4.4 Denial: feedback + journaling + determinism

- A denied call returns a structured tool-result **error value** —
  `{"error": "permission_denied", "tool": "<name>", "detail": "path /etc/passwd outside
  granted paths [/workspace]"}` — placed into the ReAct transcript as that call's result, so
  the agent's next turn sees it and can retry with an allowed argument. This mirrors how tool
  execution errors already surface to the agent.
- **Journaling / determinism.** The denial is recorded as the call's `EffectRecorded` output
  (a denied **Mutation records no `EffectIntent`** — denial precedes two-phase). Because
  `required(args)` is pure and the grant is config, the decision is a deterministic function of
  the journaled transcript; recording its output keeps memo-vs-live replay **symmetric**, so a
  **resume replays the denial from the memo without re-invoking the tool** and the agent's
  subsequent turns reconstruct identically (the determinism fence on the turn input-hash still
  applies).
- **Observability.** An optional `OrchestratorHooks::on_tool_denied(run, node, tool, detail)`
  may fire from the denial path (best-effort, replay-suppressed like the other hooks); it may
  be folded into this slice or deferred — non-blocking either way.

### 4.5 Trust boundary (authorized ≠ confined)

This slice is the **authorization** layer. It guarantees: an agent can only invoke tools it
**declares**, and an **honest** tool can only be *asked* to act within the agent's grant. It
does **not** confine a tool that **under-reports** its `required(args)` (lies) or that ignores
its declared scope when it runs — a misbehaving/compromised tool bypasses a pure-config gate.
**Runtime confinement** (intercepting fs/network/syscalls so a tool physically *cannot* exceed
the grant) and **resource-cap killing** are the **sandbox slice (SP-4 slice 4)**. The spec
states this plainly so the boundary is not overclaimed: tools are trusted to report `required`
honestly; the gate stops the *agent-driven* over-reach, the sandbox stops the *tool-driven* one.

## 5. Decisions

- **D1 — enforce at `execute_tool_effect`, before the effect-class match** [approved]: one
  chokepoint for all three effect classes; a denied Mutation never reaches two-phase.
- **D2 — `Tool::required(&self, args) -> Permissions`, default = static `spec.permissions`**
  [approved]: the tool owns per-call argument semantics; simple tools need no change; pure ⇒
  replay-stable. Rejected: a declarative `arg→permission` field map on `ToolSpec` (can't
  express derived needs like URL→host or shell→argv).
- **D3 — grant is the runtime ceiling; drop the load-time full-surface `covers` check**
  [approved]: per-call authorization is the point of (A2); a narrow grant is legal and enforced
  at dispatch. `validate` keeps structural checks only.
- **D4 — denied call → journaled tool-result error fed back to the agent** [approved]: a
  recoverable authorization condition the agent can self-correct; not a silent failure (it is
  journaled + auditable); `max_steps` bounds a looping agent. Rejected: hard node failure
  (kills the run on one recoverable misfire) and a per-agent strictness knob (premature policy
  surface).
- **D5 — `covers()` hardened but semantics-preserving** [approved]: component-aware paths +
  empty-path rejection + host wildcards fix prereqs #1/#2; the `covers` predicate itself
  (grant⊇need) is reused, only relocated to runtime and made sound.

## 6. Deferred (stated)

- **Runtime confinement** (the sandbox: fs/network/syscall interception so a lying/compromised
  tool cannot exceed its grant) + **resource-cap killing** (cpu_ms/mem_bytes/wall_ms) →
  **SP-4 slice 4**. This slice authorizes; it does not confine.
- **Robust shell-command extraction** (argv parsing of a shell string, `a; b` chains) → rides
  with the sandbox slice; slice 1 supports `commands` in the model with best-effort extraction.
- **Path canonicalization** (symlink/`realpath` resolution, mount awareness) → sandbox; slice 1
  matches paths lexically by component.
- **Secret redaction** of denied/executed effect I/O → **slice 2**.
- **Unifying the two `ToolSpec` copies** (core `Registry` vs executable `Tool::spec()`) into
  one source → future registry cleanup; slice 1 resolves *authority* (grant = core, `required`
  = executable) without merging the types.
- **`on_tool_denied` hook** may land here or defer (non-blocking).

## 7. Acceptance criteria (TDD)

1. **`covers()` — component-aware paths.** Grant `/workspace` covers `/workspace` and
   `/workspace/sub`; does **not** cover `/workspace-secret` or `/etc`. An empty grant path `""`
   covers **nothing**. A need with `..` is rejected.
2. **`covers()` — host wildcards.** Grant `Hosts(["*.example.com"])` covers
   `Hosts(["api.example.com"])`, not `Hosts(["example.evil.com"])`; `Hosts(["example.com"])`
   covers only exact `example.com`; case-insensitive.
3. **`required(args)` — default + override.** A tool with no override returns its static
   `spec.permissions`; a tool that overrides returns per-call needs derived from `args` (e.g.
   `{paths: [args["path"]]}`).
4. **Gate — granted call executes.** A listed tool whose grant covers the call's `required`
   runs exactly as today (Pure/Observation/Mutation dispatch unchanged).
5. **Gate — ungranted tool denied.** A call to a tool `∉ agent.tools` (or listed with no
   grant + non-empty `required`) is denied → the agent receives the `permission_denied`
   tool-result; the tool did **not** execute (no side effect, and for a Mutation **no
   `EffectIntent`** in the journal).
6. **Gate — in-scope tool, out-of-grant argument denied.** A tool the agent lists and holds a
   *narrow* grant for, called with an argument outside that grant (path/host), is denied and
   fed back; a subsequent call with an in-grant argument succeeds.
7. **Denial is deterministic on resume.** A run that denies a call, then fails/pauses
   downstream, **resumes** replaying the denial from the memo (the tool is **not** re-invoked)
   and the agent's later turns reconstruct identically. (Mutation-verified.)
8. **Additive.** Tools with empty `permissions` (all current demos) + agents listing them run
   **byte-identical** to before; the full existing suite passes. The relaxed `validate` no
   longer raises `PermissionNotGranted` for a narrower-than-surface grant (its SP-2 s3 tests
   updated to the ceiling model; structural grant checks retained).
9. **End-to-end.** An agent with a narrow grant, through the (test) gateway, calls a tool with
   an out-of-scope argument → denial → adapts → an in-scope call succeeds and completes.
