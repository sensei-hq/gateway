---
title: SP-2 slice 4 — skill/tool activation policy (Q4)
doctype: design
module: orchestrator
spec: SP-2
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§6.3 skills, §9 budgeted assembly, §129 conditional activation, Q4); ./2026-08-12-sp2-tool-permissions-design.md (slice 3)
date: 2026-08-12
---

# SP-2 slice 4 — skill/tool activation policy (Q4)

## 1. Goal

Settle Q4: skills and tools are referenced by name (contract fixed), but their
**activation** need not be all-or-nothing. This slice adds a **definition-level
activation policy** so a skill body / tool schema is composed into the prompt only
when relevant to the agent's input — **progressive disclosure** that lets a large
shared skill/tool library attach to an agent without every request paying for all of
it (§9/§129). The default is `Always` (today's behavior); the one gated mechanism is
a deterministic keyword trigger. Planner-selected and retrieval-ranked activation are
deferred (they need SP-3 / SP-7).

## 2. SP-2 slicing (context)

1. `ConfigSource` adapter + `FilesystemConfigSource` (slice 1 — done).
2. role/kind → chain resolution (slice 2 — done).
3. tool permission declarations + static grant⊇need check (slice 3 — done).
4. **This slice** — skill/tool activation policy (Q4).
5. hot-reload (reload + swap `Arc<Registry>` + version bump).

## 3. Background & impact review

- **Current assembly is always-on.** `assemble_prompt(registry, agent, context)`
  (`crates/orchestrator/src/agent/prompt.rs:12`) composes the system prompt as
  body + **every** listed skill body (in order) + a `## Context` section, and
  compiles **every** listed tool's schema. `drive_agent` calls it **once** per
  agent run (`executor/agent.rs:64`) with the node `input`; the resulting
  `system`/`tools` are reused across all ReAct turns and fed to
  `agent_input_hash`. `over_budget` (same file) **halts loud** when the estimate
  exceeds the chain's min window — there is no compaction or selection today.
- **Reference-by-name is fixed** (D-agent-runtime, §129). `AgentDefinition.skills`
  and `.tools` stay `Vec<String>`. Activation therefore lives on the **definition**
  (`SkillDef`/`ToolSpec`), not the reference — the approved design decision.
- **Impact: additive.** New `Activation` type + one `#[serde(default)]` field on
  each of `SkillDef`/`ToolSpec` (default `Always` ⇒ existing config byte-identical
  in behavior) + one filter in `assemble_prompt` + a new `query` argument threaded
  from `drive_agent`. Construction-site ripple (`SkillDef`/`ToolSpec` literals) is
  mechanical. No executor-control-flow change; `validate`/permissions untouched.
- **No determinism hazard** — see §4.3.

## 4. Design

### 4.1 The `Activation` type (`orchestrator-core`)

```rust
/// When a skill/tool is composed into the prompt (definition-level, §129).
/// `Always` (default) = today's unconditional inclusion. `OnKeywords` =
/// progressive disclosure: include only when the agent's input matches.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Activation {
    Always,
    OnKeywords(Vec<String>),
}

impl Default for Activation {
    fn default() -> Self { Activation::Always }
}

impl Activation {
    /// Is this active for `query` (the agent's rendered input text)?
    /// `Always` → true. `OnKeywords` → true iff `query` contains ANY keyword,
    /// case-insensitively (decision (a): ANY-of, case-insensitive substring).
    pub fn is_active(&self, query: &str) -> bool {
        match self {
            Activation::Always => true,
            Activation::OnKeywords(kw) => {
                let q = query.to_lowercase();
                kw.iter().any(|k| q.contains(&k.to_lowercase()))
            }
        }
    }
}
```

An empty `OnKeywords([])` matches nothing (never active) — a well-formed way to
park a skill/tool as unreachable; not special-cased.

### 4.2 Placement + encoding

- **`SkillDef.activation: Activation`** and **`ToolSpec.activation: Activation`**,
  both `#[serde(default)]` (absent ⇒ `Always`).
- **Skill md frontmatter (flat):** `activate_on: [climate, warming]` →
  `Activation::OnKeywords(["climate","warming"])`; **absent ⇒ `Always`**. Reuses the
  existing inline-list syntax (no nesting — the controlled-subset contract holds).
- **Tool JSON:** `"activation": {"OnKeywords":["sql"]}` (serde enum: `"Always"` or
  `{"OnKeywords":[…]}`); omitted ⇒ `Always`.

Both encodings map to the same `Activation` domain value; a DB/HTTP `ConfigSource`
produces `Activation` directly.

### 4.3 Assembly — the one behavior change

`assemble_prompt` gains a `query: &str` parameter (the agent's rendered input):

```rust
pub fn assemble_prompt(
    registry: &Registry,
    agent: &AgentDefinition,
    context: &[(ContextKey, serde_json::Value)],
    query: &str,
) -> Result<(String, Vec<ToolDefinition>), OrchestratorError>;
```

- A listed skill's body is appended **only if** `skill.activation.is_active(query)`.
- A listed tool's schema is compiled **only if** `tool.activation.is_active(query)`.
- Unknown-ref errors, skill ordering, and the `## Context` section are unchanged.
- `drive_agent` passes the rendered node input (`render_input(input)`) as `query`.
  Assembly is once-per-run, so the activated set is **fixed for the run** (decision
  (b): evaluated once against the initial input, not re-evaluated per ReAct turn).

### 4.4 Determinism, budget, and orthogonality

- **Determinism (fence intact).** `query` is the node input — already folded into
  `agent_input_hash` via the first user message. Activation is a pure function of it;
  the activated `system`/`tools` also feed the hash. So the activated set is
  reproducible on resume, and any divergence (different input) is caught by the
  existing `DeterminismViolation` guard — never a silent prompt change.
- **Budget.** Activation is a first-class way to fit the window (progressive
  disclosure). If the *activated* prompt still exceeds the min window, the existing
  `over_budget` **halt-loud** stands — no silent truncation. Summarize/compaction
  remains SP-7.
- **Orthogonal to slice 3.** Permission `validate` checks **all** listed tools at
  load, independent of activation (load-time vs runtime assembly). A tool gated out
  of a given run's prompt is simply not offered to the model that run; its grant is
  still validated.

### 4.5 Decisions

- **D1 — definition-level activation** (approved): the `SkillDef`/`ToolSpec` declares
  its own policy; `AgentDefinition.skills`/`.tools` stay `Vec<String>` (reference-by-
  name fixed). Per-agent/per-reference override is deferred.
- **D2 — mechanisms: `Always` (default) + `OnKeywords`.** Keyword trigger is the
  deterministic, zero-infra form of §129's trigger-gating. Planner-selected (SP-3)
  and retrieval-ranked / semantic description-match (SP-7) are deferred.
- **D3 — keyword match = case-insensitive substring, ANY-of** (approved (a)).
- **D4 — evaluated once per agent run** against the initial rendered input (approved
  (b)); assembly is once-per-run, so this is also the natural seam.
- **D5 — `#[serde(default)] = Always`** so existing config and DB/HTTP backends
  deserialize to today's behavior (the slice-2/3 serde-default lesson).
- **D6 — over-budget still halts loud** after activation; no silent truncation.

## 5. File formats

`<root>/skills/summarize.md`:
```markdown
---
name: summarize
description: Condense long text
activate_on: [summarize, tldr, condense]
---
Produce a terse summary.
```
`<root>/tools/sql.json`:
```json
{ "name": "sql", "description": "run a query", "input_schema": {"type":"object"},
  "effect_class": "Observation", "ttl_secs": 60, "source": null,
  "permissions": {}, "activation": { "OnKeywords": ["query", "database", "sql"] } }
```

## 6. Deferred (stated)

- Per-agent / per-reference activation override (would enrich the reference shape).
- Planner-selected activation (SP-3 planner picks the working set).
- Retrieval-ranked / semantic description-match activation (embedding infra — SP-7).
- Per-turn re-activation against the growing ReAct conversation (assembly is once-
  per-run today).
- Prompt compaction/summarize when even the activated set is over budget (SP-7).

## 7. Acceptance criteria (TDD)

1. **`is_active`.** `Always` → true for any query (incl. empty). `OnKeywords(["a","b"])`
   → true when the query contains `a` or `b` (case-insensitive), false otherwise;
   `OnKeywords([])` → false.
2. **Serde/frontmatter defaults.** A `SkillDef`/`ToolSpec` without an activation field
   → `Always`; skill frontmatter `activate_on: [x, y]` → `OnKeywords(["x","y"])`,
   absent → `Always`; a tool JSON `"activation":{"OnKeywords":["z"]}` round-trips.
3. **Assembly filters.** `assemble_prompt` with a query that matches a gated skill's
   keywords includes that skill's body and its gated tool's schema; a query that
   matches neither omits both; an `Always` skill/tool is always included; skill order
   preserved among the active ones.
4. **Additive.** With all-default (`Always`) skills/tools, `assemble_prompt` output is
   byte-identical to before (existing prompt tests unchanged in behavior); the whole
   workspace is green.
5. **Determinism.** Activation depends only on `query` (the node input) — no new
   input to `agent_input_hash`; a resume with the same input re-activates identically.
   (Exercised by the existing agent-resume tests staying green with an activation-
   bearing registry.)
6. **End-to-end.** An agent whose registry has a keyword-gated skill: an input hitting
   the keyword drives a turn whose assembled system prompt (echoed by the test gateway)
   contains the skill body; an input missing the keyword produces a prompt WITHOUT it —
   both complete a normal turn (activation shapes the prompt, doesn't gate execution).
