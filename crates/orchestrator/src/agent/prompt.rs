//! Prompt assembly + per-turn window budgeting for the agent runtime.

use kernel::types::request::{Message, ToolDefinition};
use orchestrator_core::{AgentDefinition, ContextKey, OrchestratorError, Registry};

/// Assemble an agent's system prompt (body + each listed skill body, in order, +
/// a `## Context` section of resolved dependency outputs when `context` is
/// non-empty) and its tool schemas. A listed skill's body / tool's schema is
/// included only when its `activation.is_active(query)` (progressive disclosure;
/// `Always` — the default — always includes, so all-default agents are
/// byte-identical to the pre-activation prompt). Unknown skill/tool refs are a
/// loud error (defensive — `Registry::validate` should have caught them at load).
/// An empty `context` adds NOTHING, so a no-dependency agent's prompt is
/// byte-identical to the pre-blackboard prompt.
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
    use orchestrator_core::{
        Activation, AgentDefinition, Permissions, Registry, SkillDef, ToolSpec,
    };

    fn registry() -> (Registry, AgentDefinition) {
        let agent = AgentDefinition {
            name: "r".into(),
            area: "research".into(),
            kind: "reasoning".into(),
            chain: Some("research.bulk".into()),
            chains: std::collections::HashMap::new(),
            grants: std::collections::HashMap::new(),
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
                activation: Activation::default(),
            })
            .with_skill(SkillDef {
                name: "cite".into(),
                description: None,
                body: "SKILL_CITE".into(),
                activation: Activation::default(),
            })
            .with_tool(ToolSpec {
                name: "calc".into(),
                description: Some("adds".into()),
                input_schema: serde_json::json!({"type":"object"}),
                effect_class: EffectClass::Pure,
                ttl_secs: None,
                source: None,
                permissions: Permissions::default(),
                activation: Activation::default(),
                credentials: vec![],
            });
        (reg, agent)
    }

    #[test]
    fn assemble_composes_body_then_skills_in_order_and_compiles_tool_schemas() {
        let (reg, agent) = registry();
        let (system, tools) = assemble_prompt(&reg, &agent, &[], "").expect("assembles");
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
        let (system, _t) = assemble_prompt(&reg, &agent, &ctx, "").unwrap();
        assert!(
            system.contains("## Context") && system.contains("### A") && system.contains("PRIOR"),
            "context rendered: {system}"
        );
        // Empty context ⇒ no section (byte-identical to the no-context prompt).
        let (plain, _t) = assemble_prompt(&reg, &agent, &[], "").unwrap();
        assert!(!plain.contains("## Context"));
        assert_eq!(plain, "BODY\n\nSKILL_CONCISE\n\nSKILL_CITE");
    }

    #[test]
    fn assemble_filters_skills_and_tools_by_activation() {
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
        assert!(
            system_hit.contains("GATED_BODY"),
            "activated skill included: {system_hit}"
        );
        assert!(system_hit.contains("SKILL_CONCISE") && system_hit.contains("SKILL_CITE"));

        // Query misses → gated skill body absent, Always skills still present.
        let (system_miss, _t) = assemble_prompt(&reg, &agent, &[], "translate to french").unwrap();
        assert!(
            !system_miss.contains("GATED_BODY"),
            "inactive skill omitted: {system_miss}"
        );
        assert!(system_miss.contains("SKILL_CONCISE"));
    }

    #[test]
    fn assemble_filters_a_gated_tool_schema() {
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
            credentials: vec![],
        });
        agent.tools.push("sql".into());

        let (_s, tools_hit) = assemble_prompt(&reg, &agent, &[], "run a query").unwrap();
        assert!(
            tools_hit.iter().any(|t| t.name == "sql"),
            "activated tool exposed"
        );
        let (_s, tools_miss) = assemble_prompt(&reg, &agent, &[], "hello").unwrap();
        assert!(
            !tools_miss.iter().any(|t| t.name == "sql"),
            "inactive tool hidden"
        );
        assert!(
            tools_miss.iter().any(|t| t.name == "calc"),
            "Always tool still exposed"
        );
    }

    #[test]
    fn assemble_preserves_skill_order_with_an_active_gated_skill() {
        let (mut reg, mut agent) = registry();
        reg = reg.with_skill(SkillDef {
            name: "mid".into(),
            description: None,
            body: "MID_BODY".into(),
            activation: Activation::OnKeywords(vec!["go".into()]),
        });
        // Order: concise, mid, cite  (mid is gated but active for this query)
        agent.skills = vec!["concise".into(), "mid".into(), "cite".into()];
        let (system, _t) = assemble_prompt(&reg, &agent, &[], "go now").unwrap();
        let c = system.find("SKILL_CONCISE").unwrap();
        let m = system.find("MID_BODY").unwrap();
        let t = system.find("SKILL_CITE").unwrap();
        assert!(
            c < m && m < t,
            "active skills compose in list order: {system}"
        );
    }

    #[test]
    fn est_tokens_is_chars_over_four() {
        assert_eq!(est_tokens("abcdefgh"), 2); // 8 chars / 4
    }

    #[test]
    fn over_budget_true_when_estimate_exceeds_window_and_false_otherwise() {
        let (reg, agent) = registry();
        let (system, tools) = assemble_prompt(&reg, &agent, &[], "").unwrap();
        let msgs = vec![kernel::types::request::Message::text(
            kernel::types::request::MessageRole::User,
            "hi",
        )];
        assert!(over_budget(Some(4), &system, &msgs, &tools)); // tiny window → over
        assert!(!over_budget(Some(100_000), &system, &msgs, &tools)); // huge window → fits
        assert!(!over_budget(None, &system, &msgs, &tools)); // unknown window → never a hard fail
    }
}
