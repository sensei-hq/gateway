# SP-2 slice 4 — skill/tool activation policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a definition-level `Activation` policy (`Always` | `OnKeywords`) to skills/tools so `assemble_prompt` composes a skill body / tool schema only when it matches the agent's input — progressive disclosure to fit the prompt budget, with NO planner/retrieval (deferred).

**Architecture:** An `Activation` enum + pure `is_active(query)` predicate in `orchestrator-core`, attached as a `#[serde(default)] = Always` field on `SkillDef`/`ToolSpec`. `assemble_prompt` gains a `query` arg (the agent's rendered input) and filters listed skills/tools by `is_active`. Activation is a pure function of the node input (already in `agent_input_hash`), so it's determinism-safe; over-budget still halts loud.

**Tech Stack:** Rust workspace (`orchestrator-core`, `orchestrator`, `orchestrator-store`); `serde`/`serde_json`; `cargo test`/`clippy`. Spec: `docs/superpowers/specs/2026-08-12-sp2-activation-policy-design.md`.

**House rules (every task):**
- Pre-commit = `make lint` (fmt-check + workspace `clippy -D warnings`), NO tests → always `cargo fmt --all` then `cargo test --workspace` before committing.
- Verify the REAL exit code (never a piped `| tail`); run a single test with a SINGLE positional filter (cargo rejects multiple).
- Commit a fix BEFORE any `git checkout`-based mutation-verify.
- Branch `feat/sp2-activation-policy` (created; spec committed at `9b5a4a8`). Crate `-p` names: `sensei-orchestrator-core`, `sensei-orchestrator`, `sensei-orchestrator-store`.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `crates/orchestrator-core/src/registry.rs` | registry types + parser | `Activation` type + `is_active`; `SkillDef.activation` + `ToolSpec.activation`; `from_frontmatter` `activate_on`. |
| `crates/orchestrator-core/src/lib.rs` | exports | export `Activation`. |
| `crates/orchestrator/src/agent/prompt.rs` | prompt assembly | `assemble_prompt` gains `query` + filters by `is_active`. |
| `crates/orchestrator/src/executor/agent.rs` | ReAct loop | pass the rendered input as `query`. |
| `crates/orchestrator/src/agent/tools.rs` + `config_source.rs` + `executor/tests.rs` | literals | mechanical `activation:` field additions. |
| `docs/features/orchestrator/agents-skills-tools.md` | feature doc | slice-4 status note. |

---

## Task 1: `Activation` type + `is_active` (additive, no ripple)

Purely additive new type in `orchestrator-core` — no struct changes, so the workspace stays green with zero ripple.

**Files:**
- Modify: `crates/orchestrator-core/src/registry.rs` (add type + impl + unit test)
- Modify: `crates/orchestrator-core/src/lib.rs` (export)

- [ ] **Step 1: Write the failing `is_active` test**

Add to the `#[cfg(test)] mod tests` block in `crates/orchestrator-core/src/registry.rs`:

```rust
    #[test]
    fn activation_is_active_matches_keywords_case_insensitively() {
        assert!(Activation::Always.is_active(""));
        assert!(Activation::Always.is_active("anything"));

        let on = Activation::OnKeywords(vec!["Summarize".into(), "TLDR".into()]);
        assert!(on.is_active("please summarize this"), "case-insensitive substring, any-of");
        assert!(on.is_active("give me a tldr"));
        assert!(!on.is_active("translate to french"), "no keyword → inactive");

        // Empty keyword list matches nothing.
        assert!(!Activation::OnKeywords(vec![]).is_active("summarize"));

        // Default is Always.
        assert_eq!(Activation::default(), Activation::Always);
    }
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator-core activation_is_active`
Expected: FAIL to compile — `Activation` not found. (RED.)

- [ ] **Step 3: Add the type + `is_active`**

In `crates/orchestrator-core/src/registry.rs`, add near the other public types (e.g. after `Permissions`/`ResourceCaps`):

```rust
/// When a skill/tool is composed into the prompt (definition-level, §129).
/// `Always` (default) = unconditional inclusion (today's behavior). `OnKeywords`
/// = progressive disclosure: include only when the agent's input matches.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Activation {
    Always,
    OnKeywords(Vec<String>),
}

impl Default for Activation {
    fn default() -> Self {
        Activation::Always
    }
}

impl Activation {
    /// Is this active for `query` (the agent's rendered input text)?
    /// `Always` → true. `OnKeywords` → true iff `query` contains ANY keyword,
    /// case-insensitively (an empty keyword list matches nothing).
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

- [ ] **Step 4: Export `Activation`**

`crates/orchestrator-core/src/lib.rs` — add `Activation` to the `pub use registry::{…}` list (keep alphabetical):

```rust
pub use registry::{
    Activation, AgentDefinition, AgentRef, ChainBinding, ConfigSource, NetworkPolicy, Permissions,
    Registry, RegistryConfig, ResourceCaps, SkillDef, ToolSpec,
};
```

- [ ] **Step 5: Run green + commit**

Run: `cargo test -p sensei-orchestrator-core activation_is_active` (PASS), `cargo test --workspace` (all pass), `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings` (exit 0).

```bash
git add -A
git commit -m "feat(orchestrator): SP-2 slice 4 (1/4) — Activation type + is_active

Activation{Always|OnKeywords(Vec<String>)}, Default=Always, is_active(query) =
case-insensitive substring ANY-of (empty list matches nothing). Additive type only
— no field changes, no ripple."
```

---

## Task 2: Attach `activation` to `SkillDef`/`ToolSpec` + frontmatter + ripple (inert)

Adds the field to both types (default `Always`, so assembly is unchanged this task), parses `activate_on` from skill frontmatter, and fixes every construction-site ripple. Still inert — `assemble_prompt` does not filter yet (Task 3), so the whole workspace stays green with identical behavior.

**Files:**
- Modify: `crates/orchestrator-core/src/registry.rs` (`SkillDef`, `ToolSpec`, `SkillDef::from_frontmatter`)
- Modify (ripple): every `SkillDef {` / `ToolSpec {` literal (see Step 5)
- Test: `crates/orchestrator-core/src/registry.rs`

- [ ] **Step 1: Write the failing frontmatter + serde tests**

Add to `crates/orchestrator-core/src/registry.rs` tests:

```rust
    #[test]
    fn skill_frontmatter_parses_activate_on_into_onkeywords() {
        let md = "---\nname: s\nactivate_on: [summarize, tldr]\n---\nBODY\n";
        let s = SkillDef::from_frontmatter(md).unwrap();
        assert_eq!(s.activation, Activation::OnKeywords(vec!["summarize".into(), "tldr".into()]));

        // Absent activate_on → Always.
        let s2 = SkillDef::from_frontmatter("---\nname: s\n---\nBODY\n").unwrap();
        assert_eq!(s2.activation, Activation::Always);
    }

    #[test]
    fn tool_spec_deserializes_activation_default_and_onkeywords() {
        // Absent → Always.
        let t: ToolSpec = serde_json::from_str(
            r#"{"name":"t","input_schema":{},"effect_class":"Pure"}"#,
        ).unwrap();
        assert_eq!(t.activation, Activation::Always);
        // Explicit OnKeywords round-trips.
        let t2: ToolSpec = serde_json::from_str(
            r#"{"name":"t","input_schema":{},"effect_class":"Pure","activation":{"OnKeywords":["sql"]}}"#,
        ).unwrap();
        assert_eq!(t2.activation, Activation::OnKeywords(vec!["sql".into()]));
    }
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator-core skill_frontmatter_parses_activate_on tool_spec_deserializes_activation` (run each separately — one filter at a time).
Expected: FAIL to compile — `SkillDef`/`ToolSpec` have no field `activation`. (RED.)

- [ ] **Step 3: Add the fields**

In `crates/orchestrator-core/src/registry.rs`:
- To `SkillDef` (after `body`):
```rust
    /// When this skill is composed into the prompt (§129); default `Always`.
    #[serde(default)]
    pub activation: Activation,
```
- To `ToolSpec` (after `permissions`):
```rust
    /// When this tool's schema is exposed to the model (§129); default `Always`.
    #[serde(default)]
    pub activation: Activation,
```

- [ ] **Step 4: Parse `activate_on` in `SkillDef::from_frontmatter`**

Replace the `Ok(SkillDef { … })` constructor in `SkillDef::from_frontmatter` with one that reads `activate_on` (a non-empty list → `OnKeywords`, absent/empty → `Always`), using the existing `optional_list` helper:

```rust
        let kw = optional_list(&f, "activate_on");
        let activation = if kw.is_empty() {
            Activation::Always
        } else {
            Activation::OnKeywords(kw)
        };
        Ok(SkillDef {
            name: required_scalar(&f, "name")?,
            description,
            body: body.to_string(),
            activation,
        })
```

- [ ] **Step 5: Fix every construction-site ripple**

Every `SkillDef { … }` and `ToolSpec { … }` literal needs `activation: Activation::default(),` (except `SkillDef::from_frontmatter`, handled in Step 4, and any literal that already sets `activation`). Enumerate with `grep -rn "SkillDef {" crates/orchestrator*/src` and `grep -rn "ToolSpec {" crates/orchestrator*/src`.

- `SkillDef` literals: `registry.rs` test helpers/literals (~581, 608, 625, 630, 671); `orchestrator-store/src/config_source.rs` (~159); `orchestrator/src/agent/prompt.rs` (~106, ~111); `orchestrator/src/executor/tests.rs` (~1048).
- `ToolSpec` literals: `registry.rs` `tool_spec` (~565) + `tool_needing` (~696); `orchestrator/src/agent/tools.rs` (Calc ~80, Search ~133, RecordNote ~179, Obs test ~301); `orchestrator/src/agent/prompt.rs` (~116); `orchestrator/src/executor/tests.rs` (~4256, ~4316).

In files OUTSIDE `orchestrator-core`, add `Activation` to the `use orchestrator_core::{…}` import (they already import `Permissions`/`ToolSpec`/`SkillDef`): `orchestrator/src/agent/tools.rs`, `orchestrator/src/agent/prompt.rs`, `orchestrator-store/src/config_source.rs`, `orchestrator/src/executor/tests.rs`. In `registry.rs` tests, `Activation` is in scope via `use super::*`.

Worked example (a `SkillDef` literal in `prompt.rs`):
```rust
            .with_skill(SkillDef {
                name: "concise".into(),
                description: None,
                body: "SKILL_CONCISE".into(),
                activation: Activation::default(),
            })
```

- [ ] **Step 6: Run green + commit**

Run: each new test with a single filter (PASS), then `cargo test --workspace` (all pass — behavior unchanged: assembly doesn't filter yet, defaults are `Always`), `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings` (exit 0).

```bash
git add -A
git commit -m "feat(orchestrator): SP-2 slice 4 (2/4) — SkillDef/ToolSpec.activation + activate_on

Both gain activation: Activation (#[serde(default)]=Always); SkillDef::from_frontmatter
parses activate_on:[..] → OnKeywords (absent/empty → Always). Inert this task —
assemble_prompt does not filter yet, so behavior is byte-identical."
```

---

## Task 3: `assemble_prompt` filters by activation

Threads the agent's rendered input as `query` and includes a skill body / tool schema only when its `activation.is_active(query)`.

**Files:**
- Modify: `crates/orchestrator/src/agent/prompt.rs` (`assemble_prompt` signature + filter; test callers)
- Modify: `crates/orchestrator/src/executor/agent.rs` (pass the query)
- Test: `crates/orchestrator/src/agent/prompt.rs`

- [ ] **Step 1: Write the failing filter test**

Add to `crates/orchestrator/src/agent/prompt.rs` tests (the `registry()` helper builds agent `r` with skills `[concise, cite]` + tool `calc`, all default `Always`):

```rust
    #[test]
    fn assemble_filters_skills_and_tools_by_activation() {
        use orchestrator_core::Activation;
        let (mut reg, mut agent) = registry();
        // Add a keyword-gated skill "gated" (body GATED_BODY) referenced by the agent.
        reg = reg.with_skill(SkillDef {
            name: "gated".into(),
            description: None,
            body: "GATED_BODY".into(),
            activation: Activation::OnKeywords(vec!["summarize".into()]),
        });
        agent.skills.push("gated".into());

        // Query hits the keyword → gated skill body present.
        let (system_hit, _t) = assemble_prompt(&reg, &agent, &[], "please summarize this").unwrap();
        assert!(system_hit.contains("GATED_BODY"), "activated skill included: {system_hit}");
        // Always skills still present.
        assert!(system_hit.contains("SKILL_CONCISE") && system_hit.contains("SKILL_CITE"));

        // Query misses → gated skill body absent, Always skills still present.
        let (system_miss, _t) = assemble_prompt(&reg, &agent, &[], "translate to french").unwrap();
        assert!(!system_miss.contains("GATED_BODY"), "inactive skill omitted: {system_miss}");
        assert!(system_miss.contains("SKILL_CONCISE"));
    }

    #[test]
    fn assemble_filters_a_gated_tool_schema() {
        use orchestrator_core::{Activation, EffectClass, Permissions, ToolSpec};
        let (mut reg, mut agent) = registry();
        reg = reg.with_tool(ToolSpec {
            name: "sql".into(),
            description: Some("db".into()),
            input_schema: serde_json::json!({"type":"object"}),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: Permissions::default(),
            activation: Activation::OnKeywords(vec!["query".into()]),
        });
        agent.tools.push("sql".into());

        let (_s, tools_hit) = assemble_prompt(&reg, &agent, &[], "run a query").unwrap();
        assert!(tools_hit.iter().any(|t| t.name == "sql"), "activated tool exposed");
        let (_s, tools_miss) = assemble_prompt(&reg, &agent, &[], "hello").unwrap();
        assert!(!tools_miss.iter().any(|t| t.name == "sql"), "inactive tool hidden");
        assert!(tools_miss.iter().any(|t| t.name == "calc"), "Always tool still exposed");
    }
```

- [ ] **Step 2: Run to confirm failure**

Run: `cargo test -p sensei-orchestrator assemble_filters_skills_and_tools_by_activation` (single filter).
Expected: FAIL to compile — `assemble_prompt` takes 3 args, not 4. (RED.)

- [ ] **Step 3: Add the `query` parameter + filter**

In `crates/orchestrator/src/agent/prompt.rs`, change `assemble_prompt`'s signature and add the two `is_active` guards:

```rust
pub fn assemble_prompt(
    registry: &Registry,
    agent: &AgentDefinition,
    context: &[(ContextKey, serde_json::Value)],
    query: &str,
) -> Result<(String, Vec<ToolDefinition>), OrchestratorError> {
    let mut system = agent.system_prompt.clone();
    for skill_name in &agent.skills {
        let skill =
            registry
                .skill(skill_name)
                .ok_or_else(|| OrchestratorError::UnknownSkillRef {
                    agent: agent.name.clone(),
                    skill: skill_name.clone(),
                })?;
        if !skill.activation.is_active(query) {
            continue;
        }
        system.push_str("\n\n");
        system.push_str(&skill.body);
    }
    // ... the `## Context` block is UNCHANGED ...
    let mut tools = Vec::with_capacity(agent.tools.len());
    for tool_name in &agent.tools {
        let spec = registry
            .tool(tool_name)
            .ok_or_else(|| OrchestratorError::UnknownToolRef {
                agent: agent.name.clone(),
                tool: tool_name.clone(),
            })?;
        if !spec.activation.is_active(query) {
            continue;
        }
        tools.push(ToolDefinition {
            name: spec.name.clone(),
            description: spec.description.clone(),
            input_schema: spec.input_schema.clone(),
        });
    }
    Ok((system, tools))
}
```
(Keep the unknown-ref lookups BEFORE the `is_active` guard, so a dangling ref is still a loud error. Leave the `## Context` section exactly as-is.)

- [ ] **Step 4: Update the existing test callers + the executor caller**

In `crates/orchestrator/src/agent/prompt.rs`, the existing tests call `assemble_prompt(&reg, &agent, &[])` / `(&reg, &agent, &ctx)` (lines ~131, 145, 151, 164). Add a `query` arg to each — use `""` (all their skills/tools are `Always`, so `""` includes everything, keeping assertions byte-identical). E.g. `assemble_prompt(&reg, &agent, &[], "")`.

In `crates/orchestrator/src/executor/agent.rs`, `drive_agent` computes the first user message from `render_input(input)`. Compute the query once and pass it. Change:
```rust
        let (system, tools) = assemble_prompt(&self.registry, agent, context)?;
```
to:
```rust
        let query = render_input(input);
        let (system, tools) = assemble_prompt(&self.registry, agent, context, &query)?;
```
and change the first-message construction (later in the function) from `Message::text(MessageRole::User, render_input(input))` to reuse the value: `Message::text(MessageRole::User, query.clone())`. (`render_input` is already imported in this file.)

- [ ] **Step 5: Run green + commit**

Run: each new test (single filter) PASS; `cargo test --workspace` (all pass — `Always`-only agents byte-identical); `cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings` (exit 0).

```bash
git add -A
git commit -m "feat(orchestrator): SP-2 slice 4 (3/4) — assemble_prompt filters by activation

assemble_prompt gains a query arg (the agent's rendered input) and includes a skill
body / tool schema only when activation.is_active(query); unknown-ref check stays
first. drive_agent computes render_input(input) once and passes it. Always-only
agents byte-identical."
```

---

## Task 4: End-to-end (activation shapes the prompt) + docs

Proves the full stack through the echo gateway: a keyword-gated skill appears in the assembled system prompt when the input matches, and is absent when it doesn't — both drive a normal turn (activation shapes the prompt, doesn't gate execution).

**Files:**
- Test: `crates/orchestrator/src/executor/tests.rs`
- Modify: `docs/features/orchestrator/agents-skills-tools.md`

- [ ] **Step 1: Write the e2e test**

Add to `crates/orchestrator/src/executor/tests.rs` (uses `echo_system_gateway()`, which echoes the assembled system prompt back as the answer, and the `agent_def`/`agent_node` helpers):

```rust
#[tokio::test]
async fn activation_shapes_the_assembled_prompt_end_to_end() {
    use orchestrator_core::{Activation, SkillDef};
    // Agent "a" references a keyword-gated skill "gated" (body "GATED_BODY").
    let mut agent = agent_def("c");
    agent.skills = vec!["gated".into()];
    let registry = Arc::new(
        Registry::default().with_agent(agent).with_skill(SkillDef {
            name: "gated".into(),
            description: None,
            body: "GATED_BODY".into(),
            activation: Activation::OnKeywords(vec!["summarize".into()]),
        }),
    );

    // The echo gateway returns the assembled SYSTEM prompt as the answer.
    let run_with = |input: &'static str| {
        let registry = registry.clone();
        async move {
            let (gateway, _calls) = echo_system_gateway().await;
            let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
                .with_registry(registry);
            let n1 = NodeId("n1".into());
            let outcome = exec
                .run(RunId(uuid::Uuid::new_v4()), &Graph { nodes: vec![agent_node("n1", "a", input)] })
                .await
                .expect("run");
            assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
            outcome.outputs[&n1]["text"].as_str().unwrap().to_string()
        }
    };

    // Input hits the keyword → gated skill body is in the prompt.
    assert!(run_with("please summarize this").await.contains("GATED_BODY"));
    // Input misses → gated skill body absent (but the run still completes).
    assert!(!run_with("hello there").await.contains("GATED_BODY"));
}
```
(If `agent_def`/`agent_node`/`echo_system_gateway`/`outputs[&n1]["text"]` differ from the above, adapt to the ACTUAL helpers — check the file — but keep the intent: a keyword-gated skill present/absent in the echoed system prompt by input, both runs completing.)

- [ ] **Step 2: Run — expect PASS** (Tasks 1-3 implemented the behavior).

Run: `cargo test -p sensei-orchestrator activation_shapes_the_assembled_prompt_end_to_end` → PASS. If it FAILS, STOP and report BLOCKED with the output (do not alter landed code).

- [ ] **Step 3: Mutation-verify the gating is load-bearing**

Hand-edit the test's skill `activation` from `Activation::OnKeywords(vec!["summarize".into()])` to `Activation::Always`. Re-run: the SECOND assertion (`!contains("GATED_BODY")` for the "hello there" input) must now FAIL (an `Always` skill is always present). Then RESTORE the `OnKeywords` line by HAND (do NOT `git checkout` — the test isn't committed yet). Re-run → PASS. Report both observations.

- [ ] **Step 4: Update the feature doc**

In `docs/features/orchestrator/agents-skills-tools.md`, add a slice-4 paragraph to the top `> **Status …**` blockquote and update the header status line to include "+ SP-2 slice 4":

```markdown
> **SP-2 slice 4 — skill/tool activation policy (Q4):** skills/tools carry a
> definition-level `Activation` (`Always` default, or `OnKeywords`) — `SkillDef`
> frontmatter `activate_on: [..]`, tool JSON `"activation"`. `assemble_prompt` composes
> a skill body / tool schema only when `activation.is_active(query)` for the agent's
> rendered input (matched once per run, case-insensitive substring ANY-of) —
> progressive disclosure to fit the prompt budget. `Always` is byte-identical to the
> old behavior; over-budget still halts loud (no silent truncation). Determinism-safe
> (the query is the node input, already in `agent_input_hash`). **Deferred:** per-agent
> override, planner-selected activation (SP-3), retrieval-ranked / semantic match (SP-7),
> per-turn re-activation, prompt compaction (SP-7).
```

- [ ] **Step 5: Run green + commit**

Run: `cargo test --workspace` (all pass — 4-ish new tests over the slice), `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings` (exit 0).

```bash
git add -A
git commit -m "feat(orchestrator): SP-2 slice 4 (4/4) — activation e2e + docs

End-to-end via the echo gateway: a keyword-gated skill body appears in the assembled
system prompt when the input matches and is absent when it doesn't, both runs
completing (activation shapes the prompt, not execution). Mutation-verified: making it
Always breaks the absent-case. Feature doc updated."
```

---

## Self-Review

**1. Spec coverage** (against `2026-08-12-sp2-activation-policy-design.md` §7):
- §7.1 `is_active` → Task 1 `activation_is_active_matches_keywords_case_insensitively` (Always, OnKeywords any-of case-insensitive, empty-list, default).
- §7.2 serde/frontmatter defaults → Task 2 `skill_frontmatter_parses_activate_on…` + `tool_spec_deserializes_activation…`.
- §7.3 assembly filters → Task 3 `assemble_filters_skills_and_tools_by_activation` + `assemble_filters_a_gated_tool_schema`.
- §7.4 additive → Task 2/3 (defaults `Always`; existing prompt tests pass `""` and stay byte-identical; whole workspace green).
- §7.5 determinism → structural (query = node input already in `agent_input_hash`); existing agent-resume tests stay green with the new field (Task 2/3 workspace runs).
- §7.6 end-to-end → Task 4 (echo gateway, present/absent by input, mutation-verified).
All covered.

**2. Placeholder scan:** No TBD/TODO; every code step complete; every ripple site enumerated by line + a `grep` to regenerate.

**3. Type consistency:** `Activation::{Always, OnKeywords(Vec<String>)}`, `Activation::is_active(&self, query: &str) -> bool`, `SkillDef.activation`, `ToolSpec.activation`, `assemble_prompt(registry, agent, context, query)` used identically across Tasks 1-4. `from_frontmatter` maps `activate_on` (empty→Always) → `OnKeywords`. `#[serde(default)]` on both fields keeps existing JSON/frontmatter parsing. The e2e reads `outcome.outputs[&n1]["text"]` (the echoed system prompt).

**4. Green-per-commit:** Task 1 additive type only (no ripple). Task 2 adds fields + parser + ripple, inert (assembly unchanged → byte-identical). Task 3 adds the `query` arg + filter (updates all callers). Task 4 additive test + docs.
