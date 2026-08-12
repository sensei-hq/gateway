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
    /// An explicit gateway chain-id override. `None` → resolve via the
    /// `(area,kind)` binding table. See [`Registry::resolve_chain`].
    pub chain: Option<String>,
    /// Per-phase chain overrides (phase → chain-id); empty when unused.
    pub chains: HashMap<String, String>,
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

/// A registry role binding: `(area, kind)` → a gateway chain-id. The policy
/// table that lets one edit re-point every agent of that role (§122).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainBinding {
    pub area: String,
    pub kind: String,
    pub chain: String,
}

/// The registry's config as domain objects — the backend-agnostic payload a
/// [`ConfigSource`] yields (no serialization format in the contract, so a DB /
/// HTTP backend maps its own representation → these directly).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegistryConfig {
    pub agents: Vec<AgentDefinition>,
    pub skills: Vec<SkillDef>,
    pub tools: Vec<ToolSpec>,
    pub chain_bindings: Vec<ChainBinding>,
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
    chain_bindings: HashMap<(String, String), String>,
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
    pub fn with_chain_binding(mut self, b: ChainBinding) -> Self {
        self.chain_bindings.insert((b.area, b.kind), b.chain);
        self
    }
    pub fn chain_binding(&self, area: &str, kind: &str) -> Option<&str> {
        self.chain_bindings
            .get(&(area.to_string(), kind.to_string()))
            .map(String::as_str)
    }

    /// Resolve an agent's concrete gateway chain-id for an optional phase.
    /// Order: per-phase override → explicit `chain` → `(area,kind)` binding →
    /// loud `UnknownChainRef`. A phase key the agent does not define is NOT an
    /// error — it falls through.
    pub fn resolve_chain<'a>(
        &'a self,
        agent: &'a AgentDefinition,
        phase: Option<&str>,
    ) -> Result<&'a str, OrchestratorError> {
        if let Some(p) = phase
            && let Some(c) = agent.chains.get(p)
        {
            return Ok(c);
        }
        if let Some(c) = agent.chain.as_deref() {
            return Ok(c);
        }
        if let Some(c) = self.chain_binding(&agent.area, &agent.kind) {
            return Ok(c);
        }
        Err(OrchestratorError::UnknownChainRef {
            agent: agent.name.clone(),
        })
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
        for b in cfg.chain_bindings {
            if reg.chain_binding(&b.area, &b.kind).is_some() {
                return Err(OrchestratorError::RegistryLoad(format!(
                    "duplicate chain binding: {}/{}",
                    b.area, b.kind
                )));
            }
            reg = reg.with_chain_binding(b);
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
            if agent.chain.is_none() && self.chain_binding(&agent.area, &agent.kind).is_none() {
                return Err(OrchestratorError::UnknownChainRef {
                    agent: agent.name.clone(),
                });
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

fn optional_scalar(map: &HashMap<String, FmValue>, key: &str) -> Option<String> {
    match map.get(key) {
        Some(FmValue::Scalar(s)) if !s.is_empty() => Some(s.clone()),
        _ => None,
    }
}

/// Parse an inline `[k=v, k=v]` list into a map (the flat controlled subset —
/// no nesting). A member without '=', or with an empty key/value, is loud.
fn optional_pairs(
    map: &HashMap<String, FmValue>,
    key: &str,
) -> Result<HashMap<String, String>, OrchestratorError> {
    let mut out = HashMap::new();
    if let Some(FmValue::List(items)) = map.get(key) {
        for item in items {
            let (k, v) = item.split_once('=').ok_or_else(|| {
                OrchestratorError::FrontmatterParse(format!("{key} entry missing '=': {item}"))
            })?;
            let (k, v) = (k.trim(), v.trim());
            if k.is_empty() || v.is_empty() {
                return Err(OrchestratorError::FrontmatterParse(format!(
                    "{key} entry has empty key/value: {item}"
                )));
            }
            out.insert(k.to_string(), v.to_string());
        }
    }
    Ok(out)
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
            chain: optional_scalar(&f, "chain"),
            chains: optional_pairs(&f, "chains")?,
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
        assert_eq!(a.chain.as_deref(), Some("research.bulk"));
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
        let md = "---\nname: n\nkind: k\nchain: c\n---\nbody\n"; // no area (still required)
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
            chain_bindings: vec![],
        };
        let reg = Registry::from_config(cfg).expect("assembles + validates");
        assert!(reg.agent("researcher").is_some() && reg.tool("calc").is_some());

        // Dangling ref → validate error.
        let dangling = RegistryConfig {
            agents: vec![agent.clone()],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![],
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
            chain_bindings: vec![],
        };
        assert!(matches!(
            Registry::from_config(dup),
            Err(OrchestratorError::RegistryLoad(m)) if m.contains("duplicate") && m.contains("researcher")
        ));

        // Duplicate SKILL name → loud (each collection is checked independently).
        let dup_skill = RegistryConfig {
            agents: vec![],
            skills: vec![
                SkillDef {
                    name: "s".into(),
                    description: None,
                    body: "b".into(),
                },
                SkillDef {
                    name: "s".into(),
                    description: None,
                    body: "b2".into(),
                },
            ],
            tools: vec![],
            chain_bindings: vec![],
        };
        assert!(matches!(
            Registry::from_config(dup_skill),
            Err(OrchestratorError::RegistryLoad(m)) if m.contains("duplicate skill") && m.contains('s')
        ));

        // Duplicate TOOL name → loud.
        let dup_tool = RegistryConfig {
            agents: vec![],
            skills: vec![],
            tools: vec![tool_spec("calc"), tool_spec("calc")],
            chain_bindings: vec![],
        };
        assert!(matches!(
            Registry::from_config(dup_tool),
            Err(OrchestratorError::RegistryLoad(m)) if m.contains("duplicate tool") && m.contains("calc")
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

    fn role_agent(area: &str, kind: &str, chain: Option<&str>) -> AgentDefinition {
        AgentDefinition {
            name: "role".into(),
            area: area.into(),
            kind: kind.into(),
            chain: chain.map(|c| c.into()),
            chains: HashMap::new(),
            tools: vec![],
            skills: vec![],
            system_prompt: "SYS".into(),
        }
    }

    #[test]
    fn resolve_chain_prefers_phase_then_explicit_then_binding_then_errors() {
        let reg = Registry::default().with_chain_binding(ChainBinding {
            area: "research".into(),
            kind: "reasoning".into(),
            chain: "bound".into(),
        });
        let mut phased = role_agent("research", "reasoning", Some("explicit"));
        phased.chains.insert("plan".into(), "phase-chain".into());
        assert_eq!(
            reg.resolve_chain(&phased, Some("plan")).unwrap(),
            "phase-chain"
        );
        assert_eq!(
            reg.resolve_chain(&phased, Some("nope")).unwrap(),
            "explicit"
        );
        assert_eq!(reg.resolve_chain(&phased, None).unwrap(), "explicit");
        let bound_only = role_agent("research", "reasoning", None);
        assert_eq!(reg.resolve_chain(&bound_only, None).unwrap(), "bound");
        let orphan = role_agent("misc", "misc", None);
        assert!(matches!(
            reg.resolve_chain(&orphan, None),
            Err(OrchestratorError::UnknownChainRef { agent }) if agent == "role"
        ));
    }

    #[test]
    fn from_frontmatter_parses_optional_chain_and_phase_chains() {
        let md = "---\nname: n\narea: a\nkind: k\nchains: [plan=plan.frontier, execute=code.mid]\n---\nbody\n";
        let ag = AgentDefinition::from_frontmatter(md).unwrap();
        assert_eq!(ag.chain, None);
        assert_eq!(
            ag.chains.get("plan").map(String::as_str),
            Some("plan.frontier")
        );
        assert_eq!(
            ag.chains.get("execute").map(String::as_str),
            Some("code.mid")
        );
        let md2 = "---\nname: n\narea: a\nkind: k\nchain: c\n---\nb\n";
        assert_eq!(
            AgentDefinition::from_frontmatter(md2)
                .unwrap()
                .chain
                .as_deref(),
            Some("c")
        );
    }

    #[test]
    fn from_frontmatter_malformed_phase_pair_errors() {
        let md = "---\nname: n\narea: a\nkind: k\nchains: [bad]\n---\nb\n";
        assert!(matches!(
            AgentDefinition::from_frontmatter(md),
            Err(OrchestratorError::FrontmatterParse(_))
        ));
    }

    #[test]
    fn from_frontmatter_empty_phase_key_or_value_errors() {
        // `[plan=]` reaches the empty-VALUE guard, `[=code.mid]` the empty-KEY
        // guard: `parse_fields` only filters comma-split empties, and both items
        // are non-empty so they survive to `optional_pairs`.
        for md in [
            "---\nname: n\narea: a\nkind: k\nchains: [plan=]\n---\nb\n",
            "---\nname: n\narea: a\nkind: k\nchains: [=code.mid]\n---\nb\n",
        ] {
            assert!(
                matches!(
                    AgentDefinition::from_frontmatter(md),
                    Err(OrchestratorError::FrontmatterParse(_))
                ),
                "expected loud parse error for {md:?}"
            );
        }
    }

    #[test]
    fn from_frontmatter_absent_chains_is_empty_map() {
        let md = "---\nname: n\narea: a\nkind: k\nchain: c\n---\nb\n";
        assert!(
            AgentDefinition::from_frontmatter(md)
                .unwrap()
                .chains
                .is_empty()
        );
    }

    #[test]
    fn from_config_rejects_duplicate_area_kind_binding() {
        let cfg = RegistryConfig {
            agents: vec![],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![
                ChainBinding {
                    area: "coding".into(),
                    kind: "reasoning".into(),
                    chain: "a".into(),
                },
                ChainBinding {
                    area: "coding".into(),
                    kind: "reasoning".into(),
                    chain: "b".into(),
                },
            ],
        };
        assert!(matches!(
            Registry::from_config(cfg),
            Err(OrchestratorError::RegistryLoad(m)) if m.contains("duplicate chain binding") && m.contains("coding")
        ));
    }

    #[test]
    fn validate_rejects_an_agent_with_no_resolvable_chain() {
        let reg = Registry::default().with_agent(role_agent("x", "y", None));
        assert!(matches!(
            reg.validate(),
            Err(OrchestratorError::UnknownChainRef { agent }) if agent == "role"
        ));
        let ok = Registry::default()
            .with_agent(role_agent("x", "y", None))
            .with_chain_binding(ChainBinding {
                area: "x".into(),
                kind: "y".into(),
                chain: "c".into(),
            });
        assert!(ok.validate().is_ok());
    }
}
