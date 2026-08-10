//! Prompt assembly + per-turn window budgeting for the agent runtime.

use kernel::types::request::{Message, ToolDefinition};
use orchestrator_core::{AgentDefinition, ContextKey, OrchestratorError, Registry};

/// Assemble an agent's system prompt (body + each listed skill body, in order, +
/// a `## Context` section of resolved dependency outputs when `context` is
/// non-empty) and its tool schemas. Unknown skill/tool refs are a loud error
/// (defensive — `Registry::validate` should have caught them at load). An empty
/// `context` adds NOTHING, so a no-dependency agent's prompt is byte-identical to
/// the pre-blackboard prompt.
pub fn assemble_prompt(
    registry: &Registry,
    agent: &AgentDefinition,
    context: &[(ContextKey, serde_json::Value)],
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
        system.push_str("\n\n");
        system.push_str(&skill.body);
    }
    if !context.is_empty() {
        system.push_str("\n\n## Context");
        for (key, value) in context {
            let rendered = match value {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            system.push_str(&format!("\n\n### {}\n{}", key.0, rendered));
        }
    }
    let mut tools = Vec::with_capacity(agent.tools.len());
    for tool_name in &agent.tools {
        let spec = registry
            .tool(tool_name)
            .ok_or_else(|| OrchestratorError::UnknownToolRef {
                agent: agent.name.clone(),
                tool: tool_name.clone(),
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
pub fn over_budget(
    min_window: Option<u32>,
    system: &str,
    messages: &[Message],
    tools: &[ToolDefinition],
) -> bool {
    let Some(window) = min_window else {
        return false;
    };
    let mut est = est_tokens(system);
    for m in messages {
        est += est_tokens(m.content.as_text());
    }
    for t in tools {
        est += est_tokens(&t.name)
            + t.description.as_deref().map(est_tokens).unwrap_or(0)
            + est_tokens(&t.input_schema.to_string());
    }
    est as u64 > window as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::EffectClass;
    use orchestrator_core::{AgentDefinition, Registry, SkillDef, ToolSpec};

    fn registry() -> (Registry, AgentDefinition) {
        let agent = AgentDefinition {
            name: "r".into(),
            area: "research".into(),
            kind: "reasoning".into(),
            chain: "research.bulk".into(),
            tools: vec!["calc".into()],
            skills: vec!["concise".into(), "cite".into()],
            system_prompt: "BODY".into(),
        };
        let reg = Registry::default()
            .with_agent(agent.clone())
            .with_skill(SkillDef {
                name: "concise".into(),
                description: None,
                body: "SKILL_CONCISE".into(),
            })
            .with_skill(SkillDef {
                name: "cite".into(),
                description: None,
                body: "SKILL_CITE".into(),
            })
            .with_tool(ToolSpec {
                name: "calc".into(),
                description: Some("adds".into()),
                input_schema: serde_json::json!({"type":"object"}),
                effect_class: EffectClass::Pure,
                ttl_secs: None,
                source: None,
            });
        (reg, agent)
    }

    #[test]
    fn assemble_composes_body_then_skills_in_order_and_compiles_tool_schemas() {
        let (reg, agent) = registry();
        let (system, tools) = assemble_prompt(&reg, &agent, &[]).expect("assembles");
        assert_eq!(system, "BODY\n\nSKILL_CONCISE\n\nSKILL_CITE");
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "calc");
        assert_eq!(tools[0].description.as_deref(), Some("adds"));
    }

    #[test]
    fn assemble_appends_a_context_section_only_when_present() {
        let (reg, agent) = registry();
        let ctx = vec![(
            orchestrator_core::ContextKey("A".into()),
            serde_json::json!("PRIOR"),
        )];
        let (system, _t) = assemble_prompt(&reg, &agent, &ctx).unwrap();
        assert!(
            system.contains("## Context") && system.contains("### A") && system.contains("PRIOR"),
            "context rendered: {system}"
        );
        // Empty context ⇒ no section (byte-identical to the no-context prompt).
        let (plain, _t) = assemble_prompt(&reg, &agent, &[]).unwrap();
        assert!(!plain.contains("## Context"));
        assert_eq!(plain, "BODY\n\nSKILL_CONCISE\n\nSKILL_CITE");
    }

    #[test]
    fn est_tokens_is_chars_over_four() {
        assert_eq!(est_tokens("abcdefgh"), 2); // 8 chars / 4
    }

    #[test]
    fn over_budget_true_when_estimate_exceeds_window_and_false_otherwise() {
        let (reg, agent) = registry();
        let (system, tools) = assemble_prompt(&reg, &agent, &[]).unwrap();
        let msgs = vec![kernel::types::request::Message::text(
            kernel::types::request::MessageRole::User,
            "hi",
        )];
        assert!(over_budget(Some(4), &system, &msgs, &tools)); // tiny window → over
        assert!(!over_budget(Some(100_000), &system, &msgs, &tools)); // huge window → fits
        assert!(!over_budget(None, &system, &msgs, &tools)); // unknown window → never a hard fail
    }
}
