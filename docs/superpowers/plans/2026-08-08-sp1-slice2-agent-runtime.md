# SP-1 (slice 2) — Agent Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** turn an agent definition (md+frontmatter: role→chain, skills, tools, system-prompt body) into a **durable, resumable ReAct loop** through the real gateway, layered on the slice-1 spine — so resume-without-re-spend extends *inside* the loop.

**Architecture:** a new `NodeKind::Agent` the `Executor` drives by running a ReAct loop where **each model turn is a Pure `ModelCall` effect** (iteration-aware `effect_id`) and **each Pure tool call is a Pure tool effect** — reusing slice-1's memoize/fence/resume. Registry (agents/skills/tools) is in-memory typed config with a dependency-free frontmatter-subset parser. Tools execute in the orchestrator, **Pure-only**. Prompt assembly composes body+skills+tool-schemas and budgets **per live turn** to the chain's smallest `context_window`, halting loud.

**Tech Stack:** Rust (edition 2024), gateway workspace. Design: `docs/superpowers/specs/2026-08-08-sp1-slice2-agent-runtime-design.md`. Contract per commit: `cargo build --workspace` + `cargo test --workspace` green; `make check` clean (fmt + clippy `-D warnings`).

**Builds on:** slice 1 (`docs/superpowers/plans/2026-08-08-sp1-orchestrator-spine.md`) — `Executor`/`drive`/`start`, `effect_id`, `JournalEvent`, `input_hash`, and the `test_support.rs` adapter/gateway harness. Read `crates/orchestrator/src/executor.rs` + `crates/orchestrator/src/test_support.rs` before starting.

---

## File Structure

- **Create `crates/orchestrator-core/src/registry.rs`** — `AgentRef`/`AgentDefinition`/`SkillDef`/`ToolSpec`/`Registry` + `from_frontmatter` parser + `validate`. [T1]
- **Modify `crates/orchestrator-core/src/lib.rs`** — `pub mod registry; pub use registry::*;`. [T1]
- **Modify `crates/orchestrator-core/src/error.rs`** — new `OrchestratorError` variants (registry [T1], runtime [T5], loop/tools [T6]).
- **Modify `crates/orchestrator-core/src/graph.rs`** — add `NodeKind::Agent { agent: AgentRef, input: serde_json::Value }`. [T5]
- **Modify `crates/gateway/src/engine/mod.rs`** — add read-only `Gateway::min_context_window`. [T2]
- **Create `crates/orchestrator/src/agent/mod.rs`, `prompt.rs`, `tools.rs`** — prompt assembly + budget [T3]; Pure `Tool`/`ToolRegistry`/`calc` [T4]. [`agent/mod.rs` re-exports]
- **Modify `crates/orchestrator/src/lib.rs`** — `pub mod agent;`. [T3]
- **Modify `crates/orchestrator/src/executor.rs`** — `Executor` gains `registry`/`tools`/`max_steps` + builders; `Fold`; `drive` becomes a `match`; `drive_agent` (the ReAct loop). [T5, T6]
- **Modify `crates/orchestrator/src/test_support.rs`** — add a `ScriptedAdapter` (tool_calls then final) + a demo agent registry/gateway. [T6, T8]
- **Modify docs** `docs/features/orchestrator/agents-skills-tools.md` + `README.md` + `durable-executor.md`. [T8]

---

### Task 1: Registry types + frontmatter parser + validate (core)

**Files:** create `crates/orchestrator-core/src/registry.rs`; modify `crates/orchestrator-core/src/lib.rs`, `crates/orchestrator-core/src/error.rs`.

- [ ] **Step 1: Add error variants** to `crates/orchestrator-core/src/error.rs` `OrchestratorError` (after `Gateway(String)`):

```rust
    #[error("frontmatter parse error: {0}")]
    FrontmatterParse(String),
    #[error("agent {agent:?} references unknown skill {skill:?}")]
    UnknownSkillRef { agent: String, skill: String },
    #[error("agent {agent:?} references unknown tool {tool:?}")]
    UnknownToolRef { agent: String, tool: String },
```

- [ ] **Step 2: Write the failing tests** — create `crates/orchestrator-core/src/registry.rs` with only the tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    const AGENT_MD: &str = "---\nname: researcher\narea: research\nkind: reasoning\nchain: research.bulk\ntools: [calc]\nskills: [concise]\n---\nYou are a careful researcher.\nCite sources.\n";

    #[test]
    fn agent_from_frontmatter_parses_fields_and_body() {
        let a = AgentDefinition::from_frontmatter(AGENT_MD).expect("parses");
        assert_eq!(a.name, "researcher");
        assert_eq!(a.area, "research");
        assert_eq!(a.kind, "reasoning");
        assert_eq!(a.chain, "research.bulk");
        assert_eq!(a.tools, vec!["calc".to_string()]);
        assert_eq!(a.skills, vec!["concise".to_string()]);
        assert_eq!(a.system_prompt, "You are a careful researcher.\nCite sources.\n");
    }

    #[test]
    fn agent_from_frontmatter_defaults_optional_lists_to_empty() {
        let md = "---\nname: n\narea: a\nkind: k\nchain: c\n---\nbody\n";
        let a = AgentDefinition::from_frontmatter(md).expect("parses");
        assert!(a.tools.is_empty());
        assert!(a.skills.is_empty());
    }

    #[test]
    fn agent_from_frontmatter_missing_required_key_errors() {
        let md = "---\nname: n\narea: a\nkind: k\n---\nbody\n"; // no chain
        assert!(matches!(
            AgentDefinition::from_frontmatter(md),
            Err(OrchestratorError::FrontmatterParse(_))
        ));
    }

    #[test]
    fn skill_from_frontmatter_parses_description_and_body() {
        let md = "---\nname: concise\ndescription: Be terse\n---\nUse short sentences.\n";
        let s = SkillDef::from_frontmatter(md).expect("parses");
        assert_eq!(s.name, "concise");
        assert_eq!(s.description.as_deref(), Some("Be terse"));
        assert_eq!(s.body, "Use short sentences.\n");
    }

    fn tool_spec(name: &str) -> ToolSpec {
        ToolSpec { name: name.into(), description: None, input_schema: serde_json::json!({}), effect_class: EffectClass::Pure }
    }

    #[test]
    fn validate_accepts_resolvable_refs_and_rejects_dangling() {
        let agent = AgentDefinition::from_frontmatter(AGENT_MD).unwrap();
        // Missing both the "calc" tool and "concise" skill → two dangling refs.
        let bare = Registry::default().with_agent(agent.clone());
        assert!(matches!(bare.validate(), Err(OrchestratorError::UnknownToolRef { .. }) | Err(OrchestratorError::UnknownSkillRef { .. })));
        // With both registered, validation passes.
        let full = Registry::default()
            .with_agent(agent)
            .with_tool(tool_spec("calc"))
            .with_skill(SkillDef { name: "concise".into(), description: None, body: "b".into() });
        assert!(full.validate().is_ok());
        assert!(full.agent("researcher").is_some());
        assert_eq!(full.tool("calc").map(|t| t.name.as_str()), Some("calc"));
    }
}
```

- [ ] **Step 3: Run to verify FAIL** — `cargo test -p sensei-orchestrator-core registry` → FAIL (types missing).

- [ ] **Step 4: Implement** — prepend the module body above the tests in `registry.rs`:

```rust
//! In-memory agent/skill/tool registry (zero-I/O). Definitions parse from a
//! controlled md+frontmatter subset; a directory loader is deferred (SP-1 later).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::effect::EffectClass;
use crate::error::OrchestratorError;

/// A reference to an agent by name (an `Agent` node carries one).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AgentRef(pub String);

/// An agent: role→chain, its skills/tools (by name), and its system-prompt body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentDefinition {
    pub name: String,
    pub area: String,
    pub kind: String,
    pub chain: String,
    pub tools: Vec<String>,
    pub skills: Vec<String>,
    pub system_prompt: String,
}

/// A skill: an injectable instruction module composed into a prompt by name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkillDef {
    pub name: String,
    pub description: Option<String>,
    pub body: String,
}

/// A tool's schema + effect class (the model-facing metadata + replay class).
/// The executable side lives in the orchestrator's tool runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: serde_json::Value,
    pub effect_class: EffectClass,
}

/// In-memory registry of agents/skills/tool-specs, built by a demo/preset
/// builder or from parsed frontmatter. Pure config: no I/O, no persistence.
#[derive(Debug, Clone, Default)]
pub struct Registry {
    agents: HashMap<String, AgentDefinition>,
    skills: HashMap<String, SkillDef>,
    tools: HashMap<String, ToolSpec>,
}

impl Registry {
    pub fn with_agent(mut self, a: AgentDefinition) -> Self { self.agents.insert(a.name.clone(), a); self }
    pub fn with_skill(mut self, s: SkillDef) -> Self { self.skills.insert(s.name.clone(), s); self }
    pub fn with_tool(mut self, t: ToolSpec) -> Self { self.tools.insert(t.name.clone(), t); self }
    pub fn agent(&self, name: &str) -> Option<&AgentDefinition> { self.agents.get(name) }
    pub fn skill(&self, name: &str) -> Option<&SkillDef> { self.skills.get(name) }
    pub fn tool(&self, name: &str) -> Option<&ToolSpec> { self.tools.get(name) }

    /// Fail loud if any agent references a skill/tool the registry doesn't hold.
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        for agent in self.agents.values() {
            for skill in &agent.skills {
                if !self.skills.contains_key(skill) {
                    return Err(OrchestratorError::UnknownSkillRef { agent: agent.name.clone(), skill: skill.clone() });
                }
            }
            for tool in &agent.tools {
                if !self.tools.contains_key(tool) {
                    return Err(OrchestratorError::UnknownToolRef { agent: agent.name.clone(), tool: tool.clone() });
                }
            }
        }
        Ok(())
    }
}

/// A parsed frontmatter value: a scalar or an inline `[a, b]` list.
enum FmValue { Scalar(String), List(Vec<String>) }

/// Split `---\n<frontmatter>\n---\n<body>` into (frontmatter, body).
fn split_frontmatter(input: &str) -> Result<(&str, &str), OrchestratorError> {
    let rest = input
        .strip_prefix("---\n")
        .ok_or_else(|| OrchestratorError::FrontmatterParse("missing opening '---'".into()))?;
    let end = rest
        .find("\n---")
        .ok_or_else(|| OrchestratorError::FrontmatterParse("missing closing '---'".into()))?;
    let body = rest[end + "\n---".len()..].trim_start_matches('\n');
    Ok((&rest[..end], body))
}

/// Parse `key: scalar` and `key: [a, b]` lines into a map (the controlled subset;
/// not general YAML — no nesting, quotes, or block scalars).
fn parse_fields(fm: &str) -> Result<HashMap<String, FmValue>, OrchestratorError> {
    let mut map = HashMap::new();
    for line in fm.lines() {
        if line.trim().is_empty() { continue; }
        let (key, val) = line
            .split_once(':')
            .ok_or_else(|| OrchestratorError::FrontmatterParse(format!("line missing ':': {line}")))?;
        let val = val.trim();
        let value = match val.strip_prefix('[').and_then(|v| v.strip_suffix(']')) {
            Some(inner) => FmValue::List(
                inner.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
            ),
            None => FmValue::Scalar(val.to_string()),
        };
        map.insert(key.trim().to_string(), value);
    }
    Ok(map)
}

fn required_scalar(map: &HashMap<String, FmValue>, key: &str) -> Result<String, OrchestratorError> {
    match map.get(key) {
        Some(FmValue::Scalar(s)) if !s.is_empty() => Ok(s.clone()),
        _ => Err(OrchestratorError::FrontmatterParse(format!("missing required key: {key}"))),
    }
}

fn optional_list(map: &HashMap<String, FmValue>, key: &str) -> Vec<String> {
    match map.get(key) {
        Some(FmValue::List(v)) => v.clone(),
        _ => Vec::new(),
    }
}

impl AgentDefinition {
    /// Parse an agent from the md+frontmatter subset.
    pub fn from_frontmatter(input: &str) -> Result<Self, OrchestratorError> {
        let (fm, body) = split_frontmatter(input)?;
        let f = parse_fields(fm)?;
        Ok(AgentDefinition {
            name: required_scalar(&f, "name")?,
            area: required_scalar(&f, "area")?,
            kind: required_scalar(&f, "kind")?,
            chain: required_scalar(&f, "chain")?,
            tools: optional_list(&f, "tools"),
            skills: optional_list(&f, "skills"),
            system_prompt: body.to_string(),
        })
    }
}

impl SkillDef {
    /// Parse a skill from the md+frontmatter subset.
    pub fn from_frontmatter(input: &str) -> Result<Self, OrchestratorError> {
        let (fm, body) = split_frontmatter(input)?;
        let f = parse_fields(fm)?;
        let description = match f.get("description") {
            Some(FmValue::Scalar(s)) => Some(s.clone()),
            _ => None,
        };
        Ok(SkillDef { name: required_scalar(&f, "name")?, description, body: body.to_string() })
    }
}
```

Then add to `crates/orchestrator-core/src/lib.rs`: `pub mod registry;` (module list) and `pub use registry::{AgentDefinition, AgentRef, Registry, SkillDef, ToolSpec};` (re-exports).

- [ ] **Step 5: Verify** — `cargo test -p sensei-orchestrator-core` green; `cargo build --workspace` green.

- [ ] **Step 6: Commit:**

```bash
git add crates/orchestrator-core/src/registry.rs crates/orchestrator-core/src/lib.rs crates/orchestrator-core/src/error.rs
git commit -m "feat(orchestrator-core): agent/skill/tool registry + frontmatter-subset parser + validate"
```

---

### Task 2: `Gateway::min_context_window` accessor

**Files:** `crates/gateway/src/engine/mod.rs`.

- [ ] **Step 1: Write the failing test** — add to `crates/gateway/src/engine/mod.rs` (in its `#[cfg(test)] mod tests`, or a new one; mirror `test_support.rs`'s config-building shape). If the file has no test module, add:

```rust
#[cfg(test)]
mod min_window_tests {
    use super::*;
    use crate::adapters::AdapterRegistry;
    use crate::circuit_breaker::{CircuitBreakerConfig, CircuitBreakerManager};
    use kernel::types::capability::Capability;
    use kernel::types::config::{ChainEntry, FallbackChainConfig, GatewayConfig, ModelConfig, RouterConfig};
    use std::collections::HashMap;

    fn model(id: &str, window: u32) -> ModelConfig {
        ModelConfig {
            id: id.into(), api_model_id: None, provider: "r".into(), family: None,
            capabilities: vec![Capability::TextChat], context_window: window,
            max_output_tokens: 1024, pricing: None, catalog: None,
        }
    }

    #[tokio::test]
    async fn min_context_window_is_the_smallest_model_in_the_chain() {
        let mut routers = HashMap::new();
        routers.insert("r".into(), RouterConfig { url: "http://x".into(), api_key_env: None, api_key: None, enabled: true, timeout_ms: None, headers: HashMap::new() });
        let mut models = HashMap::new();
        models.insert("big".into(), model("big", 200_000));
        models.insert("small".into(), model("small", 8_000));
        let mut chains = HashMap::new();
        chains.insert("c".into(), FallbackChainConfig {
            id: "c".into(), capability: Capability::TextChat,
            models: vec![
                ChainEntry { model: "big".into(), router: Some("r".into()), api_model_id: None, priority: 1 },
                ChainEntry { model: "small".into(), router: Some("r".into()), api_model_id: None, priority: 2 },
            ],
            fallback_triggers: Vec::new(),
        });
        let config = GatewayConfig { routers, models, chains, constraints: Default::default(), panels: Default::default(), consensus: Default::default() };
        let gw = Gateway::new(config, AdapterRegistry::new(), CircuitBreakerManager::new(CircuitBreakerConfig::default()));

        assert_eq!(gw.min_context_window("c").await, Some(8_000));
        assert_eq!(gw.min_context_window("nope").await, None);
    }
}
```

- [ ] **Step 2: Run to verify FAIL** — `cargo test -p sensei-gateway min_context_window_is_the_smallest` → FAIL (method missing).

- [ ] **Step 3: Implement** — add to `impl Gateway` (near the other read accessors, after `clear_lockout`):

```rust
    /// The smallest `context_window` among a chain's models (read-only; folds the
    /// chain's `ChainEntry`s against the model table). `None` if the chain is
    /// unknown or has no resolvable models. Used by the agent runtime to budget a
    /// prompt to the model it might fall over to — selection is untouched.
    pub async fn min_context_window(&self, chain: &str) -> Option<u32> {
        let cfg = self.config.read().await;
        let chain = cfg.chains.get(chain)?;
        chain
            .models
            .iter()
            .filter_map(|entry| cfg.models.get(&entry.model))
            .map(|m| m.context_window)
            .min()
    }
```

- [ ] **Step 4: Verify** — `cargo test -p sensei-gateway min_context_window_is_the_smallest` → PASS; `cargo test -p sensei-gateway --lib` green (no regressions).

- [ ] **Step 5: Commit:**

```bash
git add crates/gateway/src/engine/mod.rs
git commit -m "feat(gateway): Gateway::min_context_window read accessor (min over a chain's models)"
```

---

### Task 3: Prompt assembly + per-turn budget (pure functions)

**Files:** create `crates/orchestrator/src/agent/mod.rs`, `crates/orchestrator/src/agent/prompt.rs`; modify `crates/orchestrator/src/lib.rs`.

- [ ] **Step 1: Write the failing tests** — create `crates/orchestrator/src/agent/prompt.rs` with only tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{AgentDefinition, Registry, SkillDef, ToolSpec};
    use orchestrator_core::EffectClass;

    fn registry() -> (Registry, AgentDefinition) {
        let agent = AgentDefinition {
            name: "r".into(), area: "research".into(), kind: "reasoning".into(),
            chain: "research.bulk".into(), tools: vec!["calc".into()], skills: vec!["concise".into(), "cite".into()],
            system_prompt: "BODY".into(),
        };
        let reg = Registry::default()
            .with_agent(agent.clone())
            .with_skill(SkillDef { name: "concise".into(), description: None, body: "SKILL_CONCISE".into() })
            .with_skill(SkillDef { name: "cite".into(), description: None, body: "SKILL_CITE".into() })
            .with_tool(ToolSpec { name: "calc".into(), description: Some("adds".into()), input_schema: serde_json::json!({"type":"object"}), effect_class: EffectClass::Pure });
        (reg, agent)
    }

    #[test]
    fn assemble_composes_body_then_skills_in_order_and_compiles_tool_schemas() {
        let (reg, agent) = registry();
        let (system, tools) = assemble_prompt(&reg, &agent).expect("assembles");
        assert_eq!(system, "BODY\n\nSKILL_CONCISE\n\nSKILL_CITE");
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "calc");
        assert_eq!(tools[0].description.as_deref(), Some("adds"));
    }

    #[test]
    fn est_tokens_is_chars_over_four() {
        assert_eq!(est_tokens("abcdefgh"), 2); // 8 chars / 4
    }

    #[test]
    fn over_budget_true_when_estimate_exceeds_window_and_false_otherwise() {
        // "BODY\n\nSKILL_CONCISE\n\nSKILL_CITE" is 30 chars → est 7; one tiny message.
        let (reg, agent) = registry();
        let (system, tools) = assemble_prompt(&reg, &agent).unwrap();
        let msgs = vec![kernel::types::request::Message::text(kernel::types::request::MessageRole::User, "hi")];
        assert!(over_budget(Some(4), &system, &msgs, &tools));   // tiny window → over
        assert!(!over_budget(Some(100_000), &system, &msgs, &tools)); // huge window → fits
        assert!(!over_budget(None, &system, &msgs, &tools));     // unknown window → never a hard fail
    }
}
```

- [ ] **Step 2: Run to verify FAIL** — `cargo test -p sensei-orchestrator agent::prompt` → FAIL (functions missing / module not wired). If the module isn't found, do Step 3's wiring first, then it fails on the functions.

- [ ] **Step 3: Implement** — prepend to `prompt.rs`:

```rust
//! Prompt assembly + per-turn window budgeting for the agent runtime.

use kernel::types::request::{Message, ToolDefinition};
use orchestrator_core::{AgentDefinition, Registry, OrchestratorError};

/// Assemble an agent's system prompt (body + each listed skill body, in order)
/// and its tool schemas. Unknown skill/tool refs are a loud error (defensive —
/// `Registry::validate` should have caught them at load).
pub fn assemble_prompt(
    registry: &Registry,
    agent: &AgentDefinition,
) -> Result<(String, Vec<ToolDefinition>), OrchestratorError> {
    let mut system = agent.system_prompt.clone();
    for skill_name in &agent.skills {
        let skill = registry.skill(skill_name).ok_or_else(|| OrchestratorError::UnknownSkillRef {
            agent: agent.name.clone(), skill: skill_name.clone(),
        })?;
        system.push_str("\n\n");
        system.push_str(&skill.body);
    }
    let mut tools = Vec::with_capacity(agent.tools.len());
    for tool_name in &agent.tools {
        let spec = registry.tool(tool_name).ok_or_else(|| OrchestratorError::UnknownToolRef {
            agent: agent.name.clone(), tool: tool_name.clone(),
        })?;
        tools.push(ToolDefinition {
            name: spec.name.clone(),
            description: spec.description.clone(),
            input_schema: spec.input_schema.clone(),
        });
    }
    Ok((system, tools))
}

/// Documented heuristic token estimate — `chars / 4`. NOT a real tokenizer; a
/// conservative approximation, replaceable later without changing callers.
pub fn est_tokens(s: &str) -> usize {
    s.chars().count() / 4
}

/// True when the assembled prompt (system + messages + tool schemas) is estimated
/// to exceed the chain's smallest context window. An unknown window (`None`) is
/// never a hard fail — the caller logs and proceeds.
pub fn over_budget(min_window: Option<u32>, system: &str, messages: &[Message], tools: &[ToolDefinition]) -> bool {
    let Some(window) = min_window else { return false };
    let mut est = est_tokens(system);
    for m in messages {
        est += est_tokens(m.content.as_text());
    }
    for t in tools {
        est += est_tokens(&t.name) + t.description.as_deref().map(est_tokens).unwrap_or(0) + est_tokens(&t.input_schema.to_string());
    }
    est as u64 > window as u64
}
```

Create `crates/orchestrator/src/agent/mod.rs`:

```rust
//! The agent runtime: prompt assembly/budget (`prompt`) and the Pure tool
//! runtime (`tools`), driven by the executor's `Agent` node (SP-1 slice 2).

pub mod prompt;
pub mod tools;
```

Add to `crates/orchestrator/src/lib.rs`: `pub mod agent;`.

*(Note: `tools` is referenced by `agent/mod.rs` and created in Task 4. To keep Task 3 compiling, create `crates/orchestrator/src/agent/tools.rs` now as an empty file `//! Pure tool runtime (Task 4).` and fill it in Task 4.)*

- [ ] **Step 4: Verify** — `cargo test -p sensei-orchestrator agent::prompt` → PASS; `cargo build --workspace` green.

- [ ] **Step 5: Commit:**

```bash
git add crates/orchestrator/src/agent/ crates/orchestrator/src/lib.rs
git commit -m "feat(orchestrator): agent prompt assembly + per-turn window budget (pure fns)"
```

---

### Task 4: Pure tool runtime

**Files:** `crates/orchestrator/src/agent/tools.rs`; modify `crates/orchestrator-core/src/error.rs` (tool error variants).

- [ ] **Step 1: Add error variants** to `OrchestratorError` (`error.rs`):

```rust
    #[error("tool {tool:?} has non-Pure effect class {class:?}; Observation/Mutation are deferred to SP-1 slice 4")]
    ToolEffectDeferred { tool: String, class: crate::effect::EffectClass },
    #[error("unknown tool {0:?}")]
    UnknownTool(String),
    #[error("tool {tool:?} failed: {message}")]
    Tool { tool: String, message: String },
```

- [ ] **Step 2: Write the failing tests** — replace `agent/tools.rs`'s placeholder with tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{EffectClass, OrchestratorError, ToolSpec};

    #[test]
    fn calc_adds_two_numbers() {
        let out = Calc.call(serde_json::json!({"op":"add","a":2,"b":3})).expect("calc runs");
        assert_eq!(out, serde_json::json!({"result": 5.0}));
    }

    #[test]
    fn registry_executes_a_pure_tool_by_name() {
        let reg = ToolRegistry::default().with_tool(std::sync::Arc::new(Calc));
        let out = reg.execute("calc", serde_json::json!({"op":"mul","a":4,"b":5})).expect("executes");
        assert_eq!(out, serde_json::json!({"result": 20.0}));
    }

    #[test]
    fn unknown_tool_is_a_loud_error() {
        let reg = ToolRegistry::default();
        assert!(matches!(reg.execute("nope", serde_json::json!({})), Err(OrchestratorError::UnknownTool(_))));
    }

    struct Reader;
    impl Tool for Reader {
        fn spec(&self) -> ToolSpec { ToolSpec { name: "read".into(), description: None, input_schema: serde_json::json!({}), effect_class: EffectClass::Observation } }
        fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> { Ok(serde_json::json!({})) }
    }

    #[test]
    fn non_pure_tool_is_rejected_before_execution() {
        let reg = ToolRegistry::default().with_tool(std::sync::Arc::new(Reader));
        assert!(matches!(reg.execute("read", serde_json::json!({})), Err(OrchestratorError::ToolEffectDeferred { .. })));
    }
}
```

- [ ] **Step 3: Run to verify FAIL** — `cargo test -p sensei-orchestrator agent::tools` → FAIL.

- [ ] **Step 4: Implement** — prepend to `agent/tools.rs`:

```rust
//! Pure tool runtime. Slice 2 executes ONLY Pure (deterministic, memoize-forever)
//! tools in the orchestrator; Observation/Mutation are rejected loud (slice 4).

use std::collections::HashMap;
use std::sync::Arc;

use orchestrator_core::{EffectClass, OrchestratorError, ToolSpec};

/// An executable tool. `spec().effect_class` MUST be `Pure` in slice 2.
pub trait Tool: Send + Sync {
    fn spec(&self) -> ToolSpec;
    fn call(&self, args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError>;
}

/// Name→executor map. Prompt schemas come from the core `Registry`'s `ToolSpec`s;
/// this holds the executable side.
#[derive(Default, Clone)]
pub struct ToolRegistry {
    tools: HashMap<String, Arc<dyn Tool>>,
}

impl ToolRegistry {
    pub fn with_tool(mut self, tool: Arc<dyn Tool>) -> Self {
        self.tools.insert(tool.spec().name, tool);
        self
    }

    /// Execute a Pure tool by name. Unknown → loud; non-Pure → `ToolEffectDeferred`
    /// (an honest slice-4 boundary, never a silent skip); a tool error is surfaced.
    pub fn execute(&self, name: &str, args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        let tool = self.tools.get(name).ok_or_else(|| OrchestratorError::UnknownTool(name.to_string()))?;
        let class = tool.spec().effect_class;
        if class != EffectClass::Pure {
            return Err(OrchestratorError::ToolEffectDeferred { tool: name.to_string(), class });
        }
        tool.call(args)
    }
}

/// Demo Pure tool: deterministic arithmetic `{op: add|mul, a, b} → {result}`.
pub struct Calc;

impl Tool for Calc {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "calc".into(),
            description: Some("Deterministic arithmetic over two numbers".into()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": { "op": {"type":"string"}, "a": {"type":"number"}, "b": {"type":"number"} },
                "required": ["op","a","b"]
            }),
            effect_class: EffectClass::Pure,
        }
    }

    fn call(&self, args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        let err = |m: &str| OrchestratorError::Tool { tool: "calc".into(), message: m.into() };
        let a = args.get("a").and_then(|v| v.as_f64()).ok_or_else(|| err("missing number 'a'"))?;
        let b = args.get("b").and_then(|v| v.as_f64()).ok_or_else(|| err("missing number 'b'"))?;
        let result = match args.get("op").and_then(|v| v.as_str()) {
            Some("add") => a + b,
            Some("mul") => a * b,
            other => return Err(err(&format!("unknown op: {other:?}"))),
        };
        Ok(serde_json::json!({ "result": result }))
    }
}
```

- [ ] **Step 5: Verify** — `cargo test -p sensei-orchestrator agent::tools` → PASS; `cargo test --workspace` green; clippy/fmt clean.

- [ ] **Step 6: Commit:**

```bash
git add crates/orchestrator/src/agent/tools.rs crates/orchestrator-core/src/error.rs
git commit -m "feat(orchestrator): Pure tool runtime (Tool/ToolRegistry + calc; non-Pure rejected)"
```

---

### Task 5: `NodeKind::Agent` + Fold + executor wiring + `drive_agent` (the ReAct loop)

**Files:** `crates/orchestrator-core/src/graph.rs`, `crates/orchestrator-core/src/error.rs`, `crates/orchestrator/src/executor.rs`.

> Writes the full `drive_agent` loop (the cohesive method). T5's tests cover the **single-turn + budget** paths; T6 exercises **multi-turn + tools + max_steps**; T7 exercises **resume + determinism** — the same method, later branches. (Same approach slice 1 used for `drive`.)

- [ ] **Step 1: Add the node variant + runtime error variants.**

`graph.rs` — add to `NodeKind` (and update its doc comment to note two variants):

```rust
    Agent {
        agent: crate::registry::AgentRef,
        input: serde_json::Value,
    },
```

`error.rs` — add:

```rust
    #[error("unknown agent {0:?}")]
    UnknownAgent(String),
    #[error("prompt over budget at node {node:?} turn {turn}: est {est} > window {min_win}")]
    PromptOverBudget { node: NodeId, turn: usize, est: usize, min_win: u32 },
    #[error("agent node {node:?} exceeded max_steps")]
    AgentMaxStepsExceeded { node: NodeId },
```

- [ ] **Step 2: Write the failing tests** — add to `executor.rs`'s `#[cfg(test)] mod tests`:

```rust
    use orchestrator_core::{AgentDefinition, AgentRef, Registry};
    use crate::agent::tools::{Calc, ToolRegistry};
    use std::sync::Arc;

    fn agent_def(chain: &str) -> AgentDefinition {
        AgentDefinition {
            name: "a".into(), area: "research".into(), kind: "reasoning".into(),
            chain: chain.into(), tools: vec![], skills: vec![], system_prompt: "SYS".into(),
        }
    }

    /// A demo registry/executor: one agent "a" on the recording chain "c".
    fn agent_registry(chain: &str) -> Arc<Registry> {
        Arc::new(Registry::default().with_agent(agent_def(chain)))
    }

    fn agent_node(id: &str, agent: &str, input: &str) -> Node {
        Node { id: NodeId(id.into()), kind: NodeKind::Agent { agent: AgentRef(agent.into()), input: serde_json::json!(input) }, deps: vec![] }
    }

    #[tokio::test]
    async fn agent_node_single_turn_runs_through_gateway_and_journals() {
        let (gateway, calls) = recording_gateway().await; // returns empty tool_calls → final on turn 0
        let journal = InMemoryJournal::new();
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
            .with_registry(agent_registry("c"))
            .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(Calc))));

        let n1 = NodeId("n1".into());
        let graph = Graph { nodes: vec![agent_node("n1", "a", "hello")] };
        let run = RunId(uuid::Uuid::new_v4());
        let outcome = exec.run(run, &graph).await.expect("run");

        assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
        assert_eq!(outcome.completed, vec![n1.clone()]);
        assert_eq!(outcome.outputs[&n1]["text"], "canned-response");
        assert_eq!(calls.lock().unwrap().len(), 1, "one model turn, one gateway call");

        let kinds: Vec<String> = journal.load(run).await.unwrap().iter().map(|(_, e)| label(e)).collect();
        assert_eq!(kinds, vec!["RunStarted", "NodeStarted(n1)", "EffectRecorded(n1)", "NodeCompleted(n1)", "RunCompleted"]);
    }

    #[tokio::test]
    async fn agent_node_halts_over_budget_before_any_gateway_call() {
        let (gateway, calls) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        // max_context of chain "c" is 4096; force a tiny window via max_steps? No —
        // budget uses the chain window. Use a registry whose agent has a huge body.
        let big = AgentDefinition { system_prompt: "x".repeat(100_000), ..agent_def("c") };
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
            .with_registry(Arc::new(Registry::default().with_agent(big)))
            .with_tools(Arc::new(ToolRegistry::default()));

        let graph = Graph { nodes: vec![agent_node("n1", "a", "hi")] };
        let run = RunId(uuid::Uuid::new_v4());
        let outcome = exec.run(run, &graph).await.expect("run yields an outcome");
        match &outcome.failed {
            Some((node, msg)) => { assert_eq!(node.0, "n1"); assert!(msg.contains("over budget"), "{msg}"); }
            None => panic!("expected an over-budget failure"),
        }
        assert_eq!(calls.lock().unwrap().len(), 0, "over-budget halts before spending");
    }
```

- [ ] **Step 3: Run to verify FAIL** — `cargo test -p sensei-orchestrator agent_node_single_turn` → FAIL (builders/`drive_agent`/variant missing; `drive`'s irrefutable `let` no longer compiles).

- [ ] **Step 4: Implement.**

**(a)** `Executor` struct — add fields; extend imports. **Merge** into the existing `use` lines (do NOT add a duplicate `use kernel::types::request::{...}`):

```rust
// EXTEND the existing kernel request use to add MessageContent, ToolCall, ToolDefinition:
use kernel::types::request::{InferenceRequest, Message, MessageContent, MessageRole, Payload, ToolCall, ToolDefinition};
// EXTEND the existing orchestrator_core use to add AgentDefinition, AgentRef, Registry:
use orchestrator_core::{AgentDefinition, AgentRef, EffectClass, EffectId, ExecutionJournal, Graph, JournalEvent, NodeId, NodeKind, OrchestratorError, Registry, RunId, Seq, effect_id};
// NEW uses:
use crate::agent::prompt::{assemble_prompt, over_budget};
use crate::agent::tools::ToolRegistry;

pub struct Executor {
    gateway: Arc<Gateway>,
    journal: Arc<dyn ExecutionJournal>,
    version: String,
    registry: Arc<Registry>,
    tools: Arc<ToolRegistry>,
    max_steps: usize,
}
```

`Executor::new` — default the new fields (keeps slice-1 call sites unchanged):

```rust
        Self {
            gateway, journal, version: version.into(),
            registry: Arc::new(Registry::default()),
            tools: Arc::new(ToolRegistry::default()),
            max_steps: 8,
        }
```

Add builders after `new`:

```rust
    pub fn with_registry(mut self, registry: Arc<Registry>) -> Self { self.registry = registry; self }
    pub fn with_tools(mut self, tools: Arc<ToolRegistry>) -> Self { self.tools = tools; self }
    pub fn with_max_steps(mut self, n: usize) -> Self { self.max_steps = n; self }
```

**(b)** Introduce a `Fold` and thread it through `run`/`start`/`drive` (behavior-preserving for `ModelCall`):

```rust
/// The state folded from a journal on resume: the effect memo plus which nodes
/// have already been started/completed (so an Agent node's `NodeStarted`/
/// `NodeCompleted` are appended at most once across resumes).
#[derive(Default)]
struct Fold {
    memo: HashMap<EffectId, (String, serde_json::Value)>,
    started: std::collections::HashSet<NodeId>,
    completed: std::collections::HashSet<NodeId>,
}
```

- `run`: change the final call to `self.drive(run, graph, &Fold::default()).await`.
- `start`: build a `Fold` instead of a bare `memo`, populating all three sets in the fold loop:

```rust
        let mut fold = Fold::default();
        let mut outcome = RunOutcome::default();
        for (_, event) in &events {
            match event {
                JournalEvent::EffectRecorded { node, effect_id, input_hash, output, .. } => {
                    fold.memo.insert(effect_id.clone(), (input_hash.clone(), output.clone()));
                    outcome.outputs.insert(node.clone(), output.clone());
                }
                JournalEvent::NodeStarted { node } => { fold.started.insert(node.clone()); }
                JournalEvent::NodeCompleted { node } => {
                    fold.completed.insert(node.clone());
                    outcome.completed.push(node.clone());
                }
                _ => {}
            }
        }
        if terminal { return Ok(outcome); }
        self.drive(run, graph, &fold).await
```

- `drive`: change the signature to `fold: &Fold`, change the `ModelCall` handling to a `match`, and add the `Agent` arm:

```rust
    async fn drive(&self, run: RunId, graph: &Graph, fold: &Fold) -> Result<RunOutcome, OrchestratorError> {
        let mut outcome = RunOutcome::default();
        for (index, node) in graph.nodes.iter().enumerate() {
            match &node.kind {
                NodeKind::ModelCall { chain, payload } => {
                    let eid = effect_id("", 0, index);
                    let ih = input_hash(chain, payload)?;
                    if let Some((recorded_ih, output)) = fold.memo.get(&eid) {
                        if recorded_ih != &ih {
                            return Err(OrchestratorError::DeterminismViolation { node: node.id.clone(), effect_id: eid });
                        }
                        outcome.outputs.insert(node.id.clone(), output.clone());
                        outcome.completed.push(node.id.clone());
                        continue;
                    }
                    self.append(run, JournalEvent::NodeStarted { node: node.id.clone() }).await?;
                    let request = build_request(chain, payload);
                    match self.gateway.execute(&request).await {
                        Ok(response) => {
                            let output = serde_json::json!({ "model": response.model, "text": response.content.clone().unwrap_or_default() });
                            self.append(run, JournalEvent::EffectRecorded { node: node.id.clone(), effect_id: eid, class: EffectClass::Pure, input_hash: ih, seq: 0, output: output.clone() }).await?;
                            self.append(run, JournalEvent::NodeCompleted { node: node.id.clone() }).await?;
                            outcome.outputs.insert(node.id.clone(), output);
                            outcome.completed.push(node.id.clone());
                        }
                        Err(error) => {
                            let message = error.to_string();
                            self.append(run, JournalEvent::NodeFailed { node: node.id.clone(), error: message.clone() }).await?;
                            outcome.failed = Some((node.id.clone(), message));
                            return Ok(outcome);
                        }
                    }
                }
                NodeKind::Agent { agent, input } => {
                    match self.drive_agent(run, node, agent, input, fold).await? {
                        AgentStep::Completed(output) => {
                            outcome.outputs.insert(node.id.clone(), output);
                            outcome.completed.push(node.id.clone());
                        }
                        AgentStep::Failed(message) => {
                            outcome.failed = Some((node.id.clone(), message));
                            return Ok(outcome);
                        }
                    }
                }
            }
        }
        self.append(run, JournalEvent::RunCompleted).await?;
        Ok(outcome)
    }
```

**(c)** The ReAct loop. Add an `AgentStep` result and `drive_agent`:

```rust
/// The terminal result of one `Agent` node: a completed output, or a node-level
/// failure (budget/max-steps/gateway/tool) already journaled as `NodeFailed`.
enum AgentStep { Completed(serde_json::Value), Failed(String) }

impl Executor {
    /// Run one `Agent` node's ReAct loop. Each turn is a Pure `ModelCall` effect
    /// (iteration-aware id `effect_id(node.id, turn, 0)`); each Pure tool call is a
    /// Pure effect (`effect_id(node.id, turn, k+1)`). Memoized turns/tools replay
    /// from the journal with no gateway call and no re-execution (resume without
    /// re-spend); an input-hash mismatch halts with `DeterminismViolation`.
    async fn drive_agent(
        &self,
        run: RunId,
        node: &orchestrator_core::Node,
        agent_ref: &AgentRef,
        input: &serde_json::Value,
        fold: &Fold,
    ) -> Result<AgentStep, OrchestratorError> {
        let agent: &AgentDefinition = self.registry.agent(&agent_ref.0)
            .ok_or_else(|| OrchestratorError::UnknownAgent(agent_ref.0.clone()))?;
        let (system, tools) = assemble_prompt(&self.registry, agent)?;
        let chain = agent.chain.clone();
        let min_win = self.gateway.min_context_window(&chain).await;

        let mut messages: Vec<Message> = vec![Message::text(MessageRole::User, render_input(input))];
        let mut node_started = fold.started.contains(&node.id);

        for turn in 0..self.max_steps {
            let eid = effect_id(&node.id.0, turn as u64, 0);
            let ih = agent_input_hash(&chain, &system, &messages, &tools)?;

            // Reuse a memoized turn (resume): no gateway call, no re-append.
            let turn_output = if let Some((recorded_ih, output)) = fold.memo.get(&eid) {
                if recorded_ih != &ih {
                    return Err(OrchestratorError::DeterminismViolation { node: node.id.clone(), effect_id: eid });
                }
                output.clone()
            } else {
                // Live turn: budget → NodeStarted (once) → gateway → EffectRecorded.
                if over_budget(min_win, &system, &messages, &tools) {
                    let est = est_prompt_tokens(&system, &messages, &tools);
                    let err = OrchestratorError::PromptOverBudget { node: node.id.clone(), turn, est, min_win: min_win.unwrap_or(0) };
                    let message = err.to_string();
                    self.append(run, JournalEvent::NodeFailed { node: node.id.clone(), error: message.clone() }).await?;
                    return Ok(AgentStep::Failed(message));
                }
                if !node_started {
                    self.append(run, JournalEvent::NodeStarted { node: node.id.clone() }).await?;
                    node_started = true;
                }
                let request = build_chat_request(&chain, &system, messages.clone(), tools.clone());
                match self.gateway.execute(&request).await {
                    Ok(response) => {
                        let output = serde_json::json!({
                            "model": response.model,
                            "text": response.content.clone().unwrap_or_default(),
                            "tool_calls": response.tool_calls,
                        });
                        self.append(run, JournalEvent::EffectRecorded { node: node.id.clone(), effect_id: eid, class: EffectClass::Pure, input_hash: ih, seq: 0, output: output.clone() }).await?;
                        output
                    }
                    Err(error) => {
                        let message = error.to_string();
                        self.append(run, JournalEvent::NodeFailed { node: node.id.clone(), error: message.clone() }).await?;
                        return Ok(AgentStep::Failed(message));
                    }
                }
            };

            let tool_calls: Vec<ToolCall> = serde_json::from_value(turn_output.get("tool_calls").cloned().unwrap_or(serde_json::json!([])))?;
            if tool_calls.is_empty() {
                // Final answer.
                if !fold.completed.contains(&node.id) {
                    self.append(run, JournalEvent::NodeCompleted { node: node.id.clone() }).await?;
                }
                let text = turn_output.get("text").cloned().unwrap_or_default();
                let model = turn_output.get("model").cloned().unwrap_or(serde_json::Value::Null);
                return Ok(AgentStep::Completed(serde_json::json!({ "model": model, "text": text })));
            }

            // Execute (or replay) each tool call, then extend the transcript.
            let assistant_text = turn_output.get("text").and_then(|v| v.as_str()).unwrap_or_default().to_string();
            messages.push(Message { role: MessageRole::Assistant, content: MessageContent::Text { text: assistant_text }, tool_calls: tool_calls.clone(), attachments: Vec::new() });
            for (k, call) in tool_calls.iter().enumerate() {
                let teid = effect_id(&node.id.0, turn as u64, k + 1);
                let args: serde_json::Value = serde_json::from_str(&call.arguments).unwrap_or(serde_json::Value::Null);
                let tih = tool_input_hash(&call.name, &call.arguments);
                let result = if let Some((recorded_ih, output)) = fold.memo.get(&teid) {
                    if recorded_ih != &tih {
                        return Err(OrchestratorError::DeterminismViolation { node: node.id.clone(), effect_id: teid });
                    }
                    output.clone()
                } else {
                    match self.tools.execute(&call.name, args) {
                        Ok(result) => {
                            self.append(run, JournalEvent::EffectRecorded { node: node.id.clone(), effect_id: teid, class: EffectClass::Pure, input_hash: tih, seq: 0, output: result.clone() }).await?;
                            result
                        }
                        Err(err) => {
                            let message = err.to_string();
                            self.append(run, JournalEvent::NodeFailed { node: node.id.clone(), error: message.clone() }).await?;
                            return Ok(AgentStep::Failed(message));
                        }
                    }
                };
                messages.push(Message::tool_result(call.id.clone(), result.to_string()));
            }
        }

        // Ran out of steps without a final answer.
        let err = OrchestratorError::AgentMaxStepsExceeded { node: node.id.clone() };
        let message = err.to_string();
        self.append(run, JournalEvent::NodeFailed { node: node.id.clone(), error: message.clone() }).await?;
        Ok(AgentStep::Failed(message))
    }
}
```

**(d)** Free helper functions (add beside `build_request`):

```rust
/// Render an agent node's JSON `input` into user-message text: a JSON string
/// passes through; any other value is serialized (deterministic — feeds the hash).
fn render_input(input: &serde_json::Value) -> String {
    match input {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Compile one ReAct turn into a chat `InferenceRequest` (system + transcript +
/// tools) over the agent's chain. `budget: None` — cost budgeting is the gateway's
/// dormant axis in slice 2 (see the design); this request carries only window-fit.
fn build_chat_request(chain: &str, system: &str, messages: Vec<Message>, tools: Vec<ToolDefinition>) -> InferenceRequest {
    InferenceRequest {
        capability: Capability::TextChat,
        model: None, router: None, chain: Some(chain.to_string()),
        payload: Payload::Chat { messages, system: Some(system.to_string()), max_tokens: None, temperature: None, tools },
        budget: None, auth: None, panel: None, consensus: None, allow_fallback: true, credentials: Default::default(),
    }
}

/// Determinism key for a ReAct turn: `sha256_hex(chain | system | messages | tools)`.
fn agent_input_hash(chain: &str, system: &str, messages: &[Message], tools: &[ToolDefinition]) -> Result<String, OrchestratorError> {
    let messages = serde_json::to_string(messages)?;
    let tools = serde_json::to_string(tools)?;
    let mut hasher = Sha256::new();
    hasher.update(format!("{chain}|{system}|{messages}|{tools}").as_bytes());
    Ok(format!("{:x}", hasher.finalize()))
}

/// Determinism key for a Pure tool call: `sha256_hex(name | arguments)`.
fn tool_input_hash(name: &str, arguments: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(format!("{name}|{arguments}").as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Estimate a prompt's tokens (for the over-budget diagnostic's `est`).
fn est_prompt_tokens(system: &str, messages: &[Message], tools: &[ToolDefinition]) -> usize {
    use crate::agent::prompt::est_tokens;
    let mut est = est_tokens(system);
    for m in messages { est += est_tokens(m.content.as_text()); }
    for t in tools { est += est_tokens(&t.name) + t.description.as_deref().map(est_tokens).unwrap_or(0) + est_tokens(&t.input_schema.to_string()); }
    est
}
```

*(Import note: add `use orchestrator_core::Node;` if not already re-exported; `Node` is re-exported from `orchestrator_core`.)*

- [ ] **Step 5: Verify** — `cargo test -p sensei-orchestrator` → the two new tests PASS and **all slice-1 executor tests still pass** (the `Fold`/`match` refactor is behavior-preserving); `cargo test --workspace` green; clippy/fmt clean.

- [ ] **Step 6: Commit:**

```bash
git add crates/orchestrator-core/src/graph.rs crates/orchestrator-core/src/error.rs crates/orchestrator/src/executor.rs
git commit -m "feat(orchestrator): NodeKind::Agent + durable ReAct loop (drive_agent); single-turn + budget"
```

---

### Task 6: Multi-turn ReAct with Pure tools (scripted adapter)

**Files:** `crates/orchestrator/src/test_support.rs`, `crates/orchestrator/src/executor.rs` (tests only).

- [ ] **Step 1: Add a `ScriptedAdapter`** to `test_support.rs` — modeled on `RecordingAdapter` (read it first). It returns a pre-scripted queue of `ChatResponse`s (so turn 0 can carry `tool_calls` and turn 1 the final text), recording each call:

```rust
use kernel::types::request::ToolCall;
use std::collections::VecDeque;

/// Chat adapter that replays a scripted queue of responses (one per turn), so a
/// test can drive a multi-turn ReAct loop: e.g. [turn0: a `calc` tool_call, turn1:
/// a final text]. Records each call's prompt like `RecordingAdapter`.
pub struct ScriptedAdapter {
    calls: CallLog,
    script: Mutex<VecDeque<ChatResponse>>,
}

impl Model for ScriptedAdapter { fn id(&self) -> &str { "r" } }

#[async_trait]
impl ChatModel for ScriptedAdapter {
    async fn chat(&self, _cfg: &RouterConfig, req: &ChatRequest) -> Result<ChatResponse, GatewayError> {
        let prompt = req.messages.first().map(|m| m.as_text().to_string()).unwrap_or_default();
        self.calls.lock().unwrap_or_else(|e| e.into_inner()).push((req.model.clone(), prompt));
        let next = self.script.lock().unwrap_or_else(|e| e.into_inner()).pop_front();
        next.ok_or_else(|| GatewayError::ProviderError { adapter: "r".into(), message: "script exhausted".into(), status: Some(500) })
    }
}

/// A gateway on chain "c" whose scripted adapter yields `responses` in order.
pub async fn scripted_gateway(responses: Vec<ChatResponse>) -> (Gateway, CallLog) {
    // Build the same single-chain config as `build_gateway`, but register a
    // ScriptedAdapter. Extract `build_gateway`'s config assembly into a shared
    // helper `fn single_chain_config() -> GatewayConfig` and reuse it here.
    let calls: CallLog = Arc::new(Mutex::new(Vec::new()));
    let adapters = AdapterRegistry::new();
    adapters.register_chat(Arc::new(ScriptedAdapter { calls: calls.clone(), script: Mutex::new(responses.into()) })).await;
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    (Gateway::new(single_chain_config(), adapters, cb), calls)
}

/// Convenience: a `ChatResponse` carrying a single tool call.
pub fn tool_call_response(id: &str, name: &str, arguments: &str) -> ChatResponse {
    ChatResponse { content: Some(String::new()), tool_calls: vec![ToolCall { id: id.into(), name: name.into(), arguments: arguments.into() }], usage: None, model: Some("m".into()), degraded: false }
}

/// Convenience: a final text `ChatResponse` (no tool calls).
pub fn final_response(text: &str) -> ChatResponse {
    ChatResponse { content: Some(text.into()), tool_calls: Vec::new(), usage: None, model: Some("m".into()), degraded: false }
}
```

Refactor `build_gateway` to call a new `fn single_chain_config() -> GatewayConfig` (extract its `routers`/`models`/`chains`/`GatewayConfig` block verbatim) so `scripted_gateway` reuses it. (DRY; no behavior change to the existing helpers.)

- [ ] **Step 2: Write the failing tests** — add to `executor.rs` tests:

```rust
    use crate::test_support::{scripted_gateway, tool_call_response, final_response};

    fn tool_agent_registry() -> Arc<Registry> {
        Arc::new(Registry::default().with_agent(AgentDefinition { tools: vec!["calc".into()], ..agent_def("c") }))
    }
    fn calc_tools() -> Arc<ToolRegistry> { Arc::new(ToolRegistry::default().with_tool(Arc::new(Calc))) }

    #[tokio::test]
    async fn agent_react_loop_executes_a_pure_tool_and_feeds_the_result_back() {
        let (gateway, calls) = scripted_gateway(vec![
            tool_call_response("t1", "calc", "{\"op\":\"add\",\"a\":2,\"b\":3}"),
            final_response("the answer is 5"),
        ]).await;
        let journal = InMemoryJournal::new();
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
            .with_registry(tool_agent_registry()).with_tools(calc_tools());

        let n1 = NodeId("n1".into());
        let graph = Graph { nodes: vec![agent_node("n1", "a", "add 2 and 3")] };
        let run = RunId(uuid::Uuid::new_v4());
        let outcome = exec.run(run, &graph).await.expect("run");

        assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
        assert_eq!(outcome.outputs[&n1]["text"], "the answer is 5");
        assert_eq!(calls.lock().unwrap().len(), 2, "two model turns");

        // Journal: one NodeStarted, a model effect + a tool effect on turn 0, a
        // model effect on turn 1, one NodeCompleted.
        let kinds: Vec<String> = journal.load(run).await.unwrap().iter().map(|(_, e)| label(e)).collect();
        assert_eq!(kinds, vec![
            "RunStarted", "NodeStarted(n1)",
            "EffectRecorded(n1)", "EffectRecorded(n1)", // turn-0 model + calc
            "EffectRecorded(n1)",                        // turn-1 model (final)
            "NodeCompleted(n1)", "RunCompleted",
        ]);
    }

    #[tokio::test]
    async fn agent_rejects_a_non_pure_tool_loudly() {
        // The model asks for a tool the registry exposes as Observation.
        let (gateway, _calls) = scripted_gateway(vec![tool_call_response("t1", "read", "{}")]).await;
        let journal = InMemoryJournal::new();
        struct Reader;
        impl crate::agent::tools::Tool for Reader {
            fn spec(&self) -> orchestrator_core::ToolSpec { orchestrator_core::ToolSpec { name: "read".into(), description: None, input_schema: serde_json::json!({}), effect_class: orchestrator_core::EffectClass::Observation } }
            fn call(&self, _a: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> { Ok(serde_json::json!({})) }
        }
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
            .with_registry(Arc::new(Registry::default().with_agent(AgentDefinition { tools: vec!["read".into()], ..agent_def("c") })))
            .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(Reader))));
        let graph = Graph { nodes: vec![agent_node("n1", "a", "read")] };
        let outcome = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("outcome");
        let (_, msg) = outcome.failed.expect("non-Pure tool fails the node");
        assert!(msg.contains("slice 4"), "deferral message: {msg}");
    }

    #[tokio::test]
    async fn agent_halts_at_max_steps_when_the_model_never_finalizes() {
        // Every turn asks for a tool → never a final answer. max_steps = 2.
        let (gateway, calls) = scripted_gateway(vec![
            tool_call_response("t1", "calc", "{\"op\":\"add\",\"a\":1,\"b\":1}"),
            tool_call_response("t2", "calc", "{\"op\":\"add\",\"a\":1,\"b\":1}"),
        ]).await;
        let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
            .with_registry(tool_agent_registry()).with_tools(calc_tools()).with_max_steps(2);
        let graph = Graph { nodes: vec![agent_node("n1", "a", "loop")] };
        let outcome = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("outcome");
        let (_, msg) = outcome.failed.expect("max_steps halts");
        assert!(msg.contains("max_steps"), "{msg}");
        assert_eq!(calls.lock().unwrap().len(), 2, "exactly max_steps model turns");
    }
```

- [ ] **Step 3: Run to verify FAIL** — `cargo test -p sensei-orchestrator agent_react_loop` (+ the other two) → FAIL until `test_support` compiles.

- [ ] **Step 4: Run to verify PASS** — after Step 1 wiring: `cargo test -p sensei-orchestrator` → all PASS. (The loop/tool/max_steps branches were written in Task 5; these tests exercise them.)

- [ ] **Step 5: Verify** — `cargo test --workspace` green; clippy/fmt clean.

- [ ] **Step 6: Commit:**

```bash
git add crates/orchestrator/src/test_support.rs crates/orchestrator/src/executor.rs
git commit -m "feat(orchestrator): multi-turn ReAct with Pure tools + non-Pure rejection + max_steps (scripted adapter)"
```

---

### Task 7: Resume-without-re-spend inside the loop + determinism fence on an edited skill

**Files:** `crates/orchestrator/src/executor.rs` (tests only).

- [ ] **Step 1: Write the failing tests** — add to `executor.rs` tests:

```rust
    /// Headline: a run that dies at turn 1 resumes and completes WITHOUT re-calling
    /// the gateway for turn 0 or re-executing turn 0's tool — memoized on resume.
    #[tokio::test]
    async fn agent_resume_does_not_respend_completed_turns() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let graph = Graph { nodes: vec![agent_node("n1", "a", "add 2 and 3")] };

        // Run 1: turn 0 (calc tool_call) succeeds, then turn 1 is scripted to ERROR
        // (script exhausted → ProviderError). Turn 0's model + calc effects are
        // journaled; the node fails at turn 1; NO RunCompleted.
        let (gw1, calls1) = scripted_gateway(vec![
            tool_call_response("t1", "calc", "{\"op\":\"add\",\"a\":2,\"b\":3}"),
        ]).await;
        let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
            .with_registry(tool_agent_registry()).with_tools(calc_tools());
        let outcome1 = exec1.run(run, &graph).await.expect("run 1 yields an outcome");
        assert!(outcome1.failed.is_some(), "run 1 fails at turn 1");
        assert_eq!(calls1.lock().unwrap().len(), 2, "run 1 called the gateway for turn 0 and the failing turn 1");

        // Run 2: a FRESH scripted gateway that serves ONLY turn 1's final answer,
        // over the SAME journal. Resume memoizes turn 0 (model + calc) → the run-2
        // gateway is called exactly once (turn 1).
        let (gw2, calls2) = scripted_gateway(vec![final_response("the answer is 5")]).await;
        let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
            .with_registry(tool_agent_registry()).with_tools(calc_tools());
        let outcome2 = exec2.start(run, &graph).await.expect("resume completes");
        assert!(outcome2.failed.is_none(), "{:?}", outcome2.failed);
        assert_eq!(outcome2.outputs[&NodeId("n1".into())]["text"], "the answer is 5");

        // The proof: run-2's gateway saw EXACTLY ONE call (turn 1). Turn 0 was
        // replayed from the journal — not re-spent — and calc was not re-executed.
        assert_eq!(calls2.lock().unwrap().len(), 1, "resume re-spent nothing for turn 0: {:?}", calls2.lock().unwrap());
        let events = journal.load(run).await.unwrap();
        assert_eq!(events.iter().filter(|(_, e)| matches!(e, JournalEvent::RunCompleted)).count(), 1);
    }

    /// Editing a skill body changes the turn's system prompt → its input-hash no
    /// longer matches the memoized turn → resume halts with DeterminismViolation
    /// (never mixes new instructions into a memoized old turn). No gateway call.
    #[tokio::test]
    async fn agent_resume_halts_when_a_skill_changed_under_a_completed_turn() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());

        // A registry with agent "a" (skill "s") whose skill body is parameterized.
        let registry = |body: &str| {
            Arc::new(Registry::default()
                .with_agent(AgentDefinition { skills: vec!["s".into()], ..agent_def("c") })
                .with_skill(orchestrator_core::SkillDef { name: "s".into(), description: None, body: body.into() }))
        };

        // Graph [agent n1, model n2]. Run 1 with skill body "V1": n1's single turn
        // succeeds (gateway call 1), then n2 fails (gateway call 2) → n1 is fully
        // journaled+completed, but there is NO RunCompleted (a partial run to resume).
        let graph = Graph { nodes: vec![
            agent_node("n1", "a", "hi"),
            Node { id: NodeId("n2".into()), kind: model_call("c", "b"), deps: vec![NodeId("n1".into())] },
        ]};
        let (gw1, _c1) = failing_after_gateway(1).await;
        let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1").with_registry(registry("V1"));
        let out1 = exec1.run(run, &graph).await.expect("run 1 yields an outcome");
        assert!(out1.failed.is_some(), "n2 fails, leaving n1's turn journaled without RunCompleted");

        // Run 2: resume with skill body CHANGED to "V2" → n1's turn system prompt
        // (and thus input-hash) differs from the memoized turn → determinism halt.
        let (gw2, calls2) = recording_gateway().await;
        let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1").with_registry(registry("V2"));
        let err = exec2.start(run, &graph).await.expect_err("determinism violation");
        assert!(matches!(err, OrchestratorError::DeterminismViolation { .. }), "got {err:?}");
        assert_eq!(calls2.lock().unwrap().len(), 0, "a determinism violation never touches the gateway");
    }
```

- [ ] **Step 2: Run to verify** — `cargo test -p sensei-orchestrator agent_resume` → both PASS. If the resume test fails because turn-0's tool result isn't memoized, verify `drive_agent`'s tool memo branch keys on `effect_id(node.id, turn, k+1)` and that `single_chain_config`'s model id matches. (These branches are already in Task 5's `drive_agent`.)

- [ ] **Step 3: Verify** — `cargo test --workspace` green; clippy/fmt clean.

- [ ] **Step 4: Commit:**

```bash
git add crates/orchestrator/src/executor.rs
git commit -m "feat(orchestrator): resume without re-spend inside the ReAct loop + determinism fence on edited skills"
```

---

### Task 8: Real end-to-end (Agent node on a reference chain) + docs

**Files:** `crates/orchestrator/src/executor.rs` (test), `docs/features/orchestrator/agents-skills-tools.md`, `docs/features/orchestrator/README.md`, `docs/features/orchestrator/durable-executor.md`.

- [ ] **Step 1: Write the real e2e test** — add to `executor.rs` tests (reuses `demo_reference_gateway` from `test_support.rs`):

```rust
    use crate::test_support::demo_reference_gateway;

    /// Real end-to-end: an `Agent` node whose role resolves to the reference chain
    /// `research.bulk` drives the REAL gateway (assembled from `demo_catalog`). The
    /// chain falls over the credential-gated cloud entries to the local ollama
    /// model; the agent's single (no-tool) turn is served by `llama3.1-local`.
    #[tokio::test]
    async fn agent_node_drives_real_reference_chain_to_local_fallover() {
        let (gateway, calls) = demo_reference_gateway().await;
        let journal = InMemoryJournal::new();
        let registry = Arc::new(Registry::default().with_agent(AgentDefinition {
            name: "researcher".into(), area: "research".into(), kind: "reasoning".into(),
            chain: "research.bulk".into(), tools: vec![], skills: vec![], system_prompt: "Research carefully.".into(),
        }));
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
            .with_registry(registry).with_tools(Arc::new(ToolRegistry::default()));

        let n1 = NodeId("n1".into());
        let graph = Graph { nodes: vec![agent_node("n1", "researcher", "summarize the news")] };
        let outcome = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await.expect("run");

        assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
        assert_eq!(outcome.outputs[&n1]["model"], "llama3.1-local", "fell over to the local model: {:?}", outcome.outputs[&n1]);
        assert_eq!(calls.lock().unwrap().len(), 1, "the served terminal candidate hit the local adapter once");
    }
```

- [ ] **Step 2: Run to verify PASS** — `cargo test -p sensei-orchestrator agent_node_drives_real_reference_chain` → PASS. (If it fails, that's a real integration bug — STOP and report BLOCKED, do not weaken the assertion.)

- [ ] **Step 3: Docs.**

Update `docs/features/orchestrator/agents-skills-tools.md` frontmatter `status: planned` → `status: partial` and the body's status line to `**Status: Partial (Phase 3 · SP-1 slice 2).**`; add a short "Implemented in slice 2 / Deferred" note: implemented = in-memory registry (md+frontmatter subset), prompt assembly + per-turn window budget, Pure-only ReAct loop, `NodeKind::Agent`; deferred = Observation/Mutation tools + safety (slice 4), directory loader, summarize/select strategy, subagents/per-phase chains/streaming (slice 3+).

Update `docs/features/orchestrator/README.md` row for `Agents · skills · tools`: `Planned (Phase 3 · SP-1/2)` → `Partial (Phase 3 · SP-1 slice 2)`, Source `crates/orchestrator*`, Notes: `in-memory registry (frontmatter subset) · prompt assembly + per-turn window budget · Pure-only ReAct loop · resume-without-re-spend inside the loop`.

Update `docs/features/orchestrator/durable-executor.md`: add a line noting `NodeKind::Agent` now rides the same spine (per-turn Pure effects; resume extends into the loop).

- [ ] **Step 4: Verify** — `cargo test --workspace` green; `make check` clean (fmt + clippy `-D warnings`); frontmatter intact.

- [ ] **Step 5: Commit:**

```bash
git add crates/orchestrator/src/executor.rs docs/features/orchestrator/
git commit -m "feat(orchestrator): real reference-chain agent-node e2e + slice-2 docs (agents-skills-tools implemented/partial)"
```

---

## Self-Review

- **Spec coverage** (`2026-08-08-sp1-slice2-agent-runtime-design.md`): §3 registry+parser → T1; §2 `min_context_window` → T2; §5 prompt+budget → T3 (per-turn wiring in T5's `drive_agent`); §6 Pure tools → T4; §4 `NodeKind::Agent`+loop+iteration-aware effect_ids+version-fence-via-input-hash → T5/T6; §7 error taxonomy → variants added across T1/T4/T5; §8 tests → T1(1,2), T3(2), T5(3 budget), T6(4 ReAct, 7 non-Pure), T6(max_steps), T7(5 resume, 6 determinism), T8(8 e2e); §9 deferrals → noted in T8 docs. Every §8 acceptance test maps to a step.
- **Placeholder scan:** no TBD/TODO. The two "read the sibling for the pattern" pointers (`ScriptedAdapter` mirrors `RecordingAdapter`; `single_chain_config` extracts `build_gateway`'s existing block) are concrete refactors of shown code, not placeholders — the same convention the slice-1 plan used and its review sanctioned.
- **Type consistency:** `Registry`/`AgentDefinition`/`SkillDef`/`ToolSpec`/`AgentRef` (T1) are consumed by `assemble_prompt` (T3), `ToolRegistry`/`Tool`/`Calc` (T4), and `drive_agent` (T5). `NodeKind::Agent { agent: AgentRef, input: Value }` (T5) matches `agent_node(..)` in tests. Error variants are defined before first use (registry T1; tool T4; runtime T5). `effect_id(&node.id.0, turn, 0|k+1)`, `agent_input_hash`, `tool_input_hash` are used consistently across T5–T7. `build_chat_request` sets `budget: None` (design §5). `ChatResponse` fields (`content`/`tool_calls`/`usage`/`model`/`degraded`) match `test_support.rs`.
- **Behavior preservation:** `Executor::new` keeps its 3-arg signature (new fields defaulted) and the `Fold` refactor is behavior-identical for `ModelCall` — so all slice-1 executor tests stay green (asserted in T5 Step 5). `Gateway` gains only a read accessor. Core/kernel/catalog otherwise untouched (additive).
- **Sequencing (each green + committed):** 1 registry → 2 gateway accessor → 3 prompt (pure) → 4 tools (pure) → 5 variant+loop (single-turn+budget) → 6 multi-turn+tools → 7 resume+determinism → 8 e2e+docs. No broken intermediate; `drive_agent` is written whole in T5 with the branches T6/T7 exercise (empty memo/tools in T5).
- **No silent failures:** budget/max_steps/non-Pure/tool/gateway errors journal `NodeFailed` and surface in `RunOutcome`; determinism/version/journal errors halt loud; memoized turns/tools never re-spend and never re-append.

## Execution Handoff

Subagent-driven in an isolated worktree off `develop`; per-task spec + code-quality review (T5 `drive_agent` and T7 resume/determinism get the full treatment — the durable-loop correctness is the whole point); final whole-branch review; `finishing-a-development-branch` → merge to `develop`. Then **SP-1 slice 3** — `Map` bounded fan-out + `hard`/`soft` edges + quorum/`Consolidate` + the `ContextStore` blackboard — layers on this runtime. Observation/Mutation tool safety is **slice 4**; persistence (`PostgresJournal`/SP-DATA) stays a separate held-off layer.
