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
    /// TTL (seconds) for an `Observation` tool's memoized read; `None` = never
    /// memoize (always re-read on resume). Ignored for Pure/Mutation.
    pub ttl_secs: Option<u64>,
    /// Provenance `source` label recorded with an Observation. Defaults to the tool name.
    pub source: Option<String>,
}

/// The registry's config as domain objects — the backend-agnostic payload a
/// [`ConfigSource`] yields (no serialization format in the contract, so a DB /
/// HTTP backend maps its own representation → these directly).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegistryConfig {
    pub agents: Vec<AgentDefinition>,
    pub skills: Vec<SkillDef>,
    pub tools: Vec<ToolSpec>,
}

/// A pluggable source of registry config (SP-2). **This is the extension seam**
/// future backends implement — a filesystem source now, `PostgresConfigSource` /
/// `ConvexConfigSource` later — while [`Registry`] itself is the uniform,
/// backend-agnostic *assembled result* (built + validated by
/// [`Registry::from_config`]), NOT an extension point.
#[async_trait::async_trait]
pub trait ConfigSource: Send + Sync {
    /// Load the whole registry config (a one-shot snapshot; hot-reload re-calls it).
    async fn load(&self) -> Result<RegistryConfig, OrchestratorError>;
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

    /// Assemble + validate a `Registry` from already-parsed [`RegistryConfig`].
    /// Rejects a duplicate agent/skill/tool `name` loudly — `with_*` alone would
    /// silently last-wins, and `validate` can't see dupes once the HashMap has
    /// collapsed them — then runs the dangling-ref [`validate`](Self::validate).
    /// The single, format-agnostic assembly point every `ConfigSource` reuses.
    pub fn from_config(cfg: RegistryConfig) -> Result<Registry, OrchestratorError> {
        let mut reg = Registry::default();
        for a in cfg.agents {
            if reg.agent(&a.name).is_some() {
                return Err(OrchestratorError::RegistryLoad(format!(
                    "duplicate agent: {}",
                    a.name
                )));
            }
            reg = reg.with_agent(a);
        }
        for s in cfg.skills {
            if reg.skill(&s.name).is_some() {
                return Err(OrchestratorError::RegistryLoad(format!(
                    "duplicate skill: {}",
                    s.name
                )));
            }
            reg = reg.with_skill(s);
        }
        for t in cfg.tools {
            if reg.tool(&t.name).is_some() {
                return Err(OrchestratorError::RegistryLoad(format!(
                    "duplicate tool: {}",
                    t.name
                )));
            }
            reg = reg.with_tool(t);
        }
        reg.validate()?;
        Ok(reg)
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
///
/// Zero-field frontmatter (`---\n---\n<body>`) is valid: `rest` then starts
/// with the closing delimiter itself, rather than containing `"\n---"`, so
/// that case is checked separately before falling back to `find`.
fn split_frontmatter(input: &str) -> Result<(&str, &str), OrchestratorError> {
    let rest = input
        .strip_prefix("---\n")
        .ok_or_else(|| OrchestratorError::FrontmatterParse("missing opening '---'".into()))?;
    if let Some(body) = rest.strip_prefix("---") {
        return Ok(("", body.trim_start_matches('\n')));
    }
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

    #[test]
    fn skill_from_frontmatter_defaults_description_to_none() {
        let md = "---\nname: concise\n---\nUse short sentences.\n";
        let s = SkillDef::from_frontmatter(md).expect("parses");
        assert_eq!(s.description, None);
    }

    #[test]
    fn from_frontmatter_missing_opening_delimiter_errors() {
        let md = "name: n\n---\nbody";
        assert!(matches!(
            AgentDefinition::from_frontmatter(md),
            Err(OrchestratorError::FrontmatterParse(_))
        ));
    }

    #[test]
    fn from_frontmatter_missing_closing_delimiter_errors() {
        let md = "---\nname: n\nbody";
        assert!(matches!(
            AgentDefinition::from_frontmatter(md),
            Err(OrchestratorError::FrontmatterParse(_))
        ));
    }

    #[test]
    fn from_frontmatter_line_without_colon_errors() {
        let md = "---\nname n\n---\nb";
        assert!(matches!(
            AgentDefinition::from_frontmatter(md),
            Err(OrchestratorError::FrontmatterParse(_))
        ));
    }

    #[test]
    fn from_frontmatter_zero_field_frontmatter_splits_and_fails_on_missing_name() {
        // The delimiter split must succeed (empty frontmatter is valid), and the
        // resulting error must be the downstream "missing required key", not a
        // (wrong) "missing closing '---'".
        match AgentDefinition::from_frontmatter("---\n---\nbody\n") {
            Err(OrchestratorError::FrontmatterParse(msg)) => {
                assert!(
                    msg.contains("required key"),
                    "expected a missing-required-key error, got: {msg}"
                );
            }
            other => panic!("expected FrontmatterParse(missing required key), got {other:?}"),
        }
    }

    fn tool_spec(name: &str) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: None,
            input_schema: serde_json::json!({}),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
        }
    }

    #[test]
    fn from_config_assembles_validates_and_rejects_duplicates() {
        let agent = AgentDefinition::from_frontmatter(AGENT_MD).unwrap(); // researcher, tools:[calc], skills:[concise]
        let cfg = RegistryConfig {
            agents: vec![agent.clone()],
            skills: vec![SkillDef {
                name: "concise".into(),
                description: None,
                body: "b".into(),
            }],
            tools: vec![tool_spec("calc")],
        };
        let reg = Registry::from_config(cfg).expect("assembles + validates");
        assert!(reg.agent("researcher").is_some() && reg.tool("calc").is_some());

        // Dangling ref → validate error.
        let dangling = RegistryConfig {
            agents: vec![agent.clone()],
            skills: vec![],
            tools: vec![],
        };
        assert!(matches!(
            Registry::from_config(dangling),
            Err(OrchestratorError::UnknownToolRef { .. })
                | Err(OrchestratorError::UnknownSkillRef { .. })
        ));

        // Duplicate name → loud RegistryLoad (never a silent last-wins).
        let dup = RegistryConfig {
            agents: vec![agent.clone(), agent],
            skills: vec![SkillDef {
                name: "concise".into(),
                description: None,
                body: "b".into(),
            }],
            tools: vec![tool_spec("calc")],
        };
        assert!(matches!(
            Registry::from_config(dup),
            Err(OrchestratorError::RegistryLoad(m)) if m.contains("duplicate") && m.contains("researcher")
        ));
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
