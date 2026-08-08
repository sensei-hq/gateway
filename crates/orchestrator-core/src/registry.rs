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
    pub fn with_agent(mut self, a: AgentDefinition) -> Self {
        self.agents.insert(a.name.clone(), a);
        self
    }
    pub fn with_skill(mut self, s: SkillDef) -> Self {
        self.skills.insert(s.name.clone(), s);
        self
    }
    pub fn with_tool(mut self, t: ToolSpec) -> Self {
        self.tools.insert(t.name.clone(), t);
        self
    }
    pub fn agent(&self, name: &str) -> Option<&AgentDefinition> {
        self.agents.get(name)
    }
    pub fn skill(&self, name: &str) -> Option<&SkillDef> {
        self.skills.get(name)
    }
    pub fn tool(&self, name: &str) -> Option<&ToolSpec> {
        self.tools.get(name)
    }

    /// Fail loud if any agent references a skill/tool the registry doesn't hold.
    pub fn validate(&self) -> Result<(), OrchestratorError> {
        for agent in self.agents.values() {
            for skill in &agent.skills {
                if !self.skills.contains_key(skill) {
                    return Err(OrchestratorError::UnknownSkillRef {
                        agent: agent.name.clone(),
                        skill: skill.clone(),
                    });
                }
            }
            for tool in &agent.tools {
                if !self.tools.contains_key(tool) {
                    return Err(OrchestratorError::UnknownToolRef {
                        agent: agent.name.clone(),
                        tool: tool.clone(),
                    });
                }
            }
        }
        Ok(())
    }
}

/// A parsed frontmatter value: a scalar or an inline `[a, b]` list.
enum FmValue {
    Scalar(String),
    List(Vec<String>),
}

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
        if line.trim().is_empty() {
            continue;
        }
        let (key, val) = line.split_once(':').ok_or_else(|| {
            OrchestratorError::FrontmatterParse(format!("line missing ':': {line}"))
        })?;
        let val = val.trim();
        let value = match val.strip_prefix('[').and_then(|v| v.strip_suffix(']')) {
            Some(inner) => FmValue::List(
                inner
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
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
        _ => Err(OrchestratorError::FrontmatterParse(format!(
            "missing required key: {key}"
        ))),
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
        Ok(SkillDef {
            name: required_scalar(&f, "name")?,
            description,
            body: body.to_string(),
        })
    }
}

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
        assert_eq!(
            a.system_prompt,
            "You are a careful researcher.\nCite sources.\n"
        );
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
        ToolSpec {
            name: name.into(),
            description: None,
            input_schema: serde_json::json!({}),
            effect_class: EffectClass::Pure,
        }
    }

    #[test]
    fn validate_accepts_resolvable_refs_and_rejects_dangling() {
        let agent = AgentDefinition::from_frontmatter(AGENT_MD).unwrap();
        // Missing both the "calc" tool and "concise" skill → two dangling refs.
        let bare = Registry::default().with_agent(agent.clone());
        assert!(matches!(
            bare.validate(),
            Err(OrchestratorError::UnknownToolRef { .. })
                | Err(OrchestratorError::UnknownSkillRef { .. })
        ));
        // With both registered, validation passes.
        let full = Registry::default()
            .with_agent(agent)
            .with_tool(tool_spec("calc"))
            .with_skill(SkillDef {
                name: "concise".into(),
                description: None,
                body: "b".into(),
            });
        assert!(full.validate().is_ok());
        assert!(full.agent("researcher").is_some());
        assert_eq!(full.tool("calc").map(|t| t.name.as_str()), Some("calc"));
    }
}
