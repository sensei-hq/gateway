//! In-memory agent/skill/tool registry (zero-I/O). Definitions parse from a
//! controlled md+frontmatter subset; a directory loader is deferred (SP-1 later).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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
    #[serde(default)]
    pub chains: HashMap<String, String>,
    /// Per-tool permission grants (tool name → granted scope, §287) — the
    /// runtime ceiling. NOT checked at load; enforced per-call at runtime against
    /// `Tool::required(args)` (ceiling model, SP-4 s1). Empty when unused.
    #[serde(default)]
    pub grants: HashMap<String, Permissions>,
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
    /// When this skill is composed into the prompt (§129); default `Always`.
    #[serde(default)]
    pub activation: Activation,
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
    /// The tool's declared **maximum surface** (§132) — for disclosure. The
    /// agent's grant is the runtime ceiling, and each call is authorized against
    /// `Tool::required(args)` at runtime (SP-4 s1); `validate` no longer checks
    /// that a grant covers this.
    #[serde(default)]
    pub permissions: Permissions,
    /// When this tool's schema is exposed to the model (§129); default `Always`.
    #[serde(default)]
    pub activation: Activation,
    /// Credential refs this tool needs (SP-4 broker); resolved by the injected
    /// `CredentialBroker` and injected into the call's `ToolContext.credentials`
    /// (ephemeral). Empty ⇒ the tool needs no credentials.
    #[serde(default)]
    pub credentials: Vec<String>,
}

/// A capability declaration — used BOTH as a tool's required needs
/// (`ToolSpec.permissions`) and an agent's per-tool grant
/// (`AgentDefinition.grants[tool]`). Secure default: deny/empty everything.
/// Declarations only this slice — runtime enforcement is SP-4.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Permissions {
    #[serde(default)]
    pub paths: Vec<String>,
    #[serde(default)]
    pub commands: Vec<String>,
    #[serde(default)]
    pub network: NetworkPolicy,
    #[serde(default)]
    pub caps: ResourceCaps,
}

/// Network egress policy. Default `Deny` (secure) — pinned via `#[default]` on
/// the variant so it survives reordering.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub enum NetworkPolicy {
    #[default]
    Deny,
    Hosts(Vec<String>),
    Any,
}

/// Resource ceilings; `None` = unbounded (on a grant) / no requirement (on a need).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ResourceCaps {
    #[serde(default)]
    pub cpu_ms: Option<u64>,
    #[serde(default)]
    pub mem_bytes: Option<u64>,
    #[serde(default)]
    pub wall_ms: Option<u64>,
}

impl Permissions {
    /// Does `self` (an agent's grant) cover `need` (a tool's declared needs)?
    /// paths: each need is prefixed by some grant path. commands: needed ⊆ granted.
    /// network/caps: see [`NetworkPolicy::covers`]/[`ResourceCaps::covers`].
    pub fn covers(&self, need: &Permissions) -> bool {
        need.paths
            .iter()
            .all(|p| self.paths.iter().any(|g| path_covers(g, p)))
            && need.commands.iter().all(|c| self.commands.contains(c))
            && self.network.covers(&need.network)
            && self.caps.covers(&need.caps)
    }
}

impl NetworkPolicy {
    /// `Any` covers all; `Hosts(G)` covers `Hosts(N)` iff N⊆G and covers `Deny`;
    /// `Deny` covers only `Deny`.
    fn covers(&self, need: &NetworkPolicy) -> bool {
        match (self, need) {
            (NetworkPolicy::Any, _) => true,
            (_, NetworkPolicy::Deny) => true,
            (NetworkPolicy::Hosts(g), NetworkPolicy::Hosts(n)) => {
                n.iter().all(|h| g.iter().any(|gh| host_covers(gh, h)))
            }
            _ => false,
        }
    }
}

impl ResourceCaps {
    fn covers(&self, need: &ResourceCaps) -> bool {
        cap_covers(self.cpu_ms, need.cpu_ms)
            && cap_covers(self.mem_bytes, need.mem_bytes)
            && cap_covers(self.wall_ms, need.wall_ms)
    }
}

/// A single cap dimension: no requirement → covered; grant `None` = unlimited →
/// covers any need; else the grant ceiling must be ≥ the need.
fn cap_covers(grant: Option<u64>, need: Option<u64>) -> bool {
    match (grant, need) {
        (_, None) => true,
        (None, Some(_)) => true,
        (Some(g), Some(n)) => g >= n,
    }
}

/// Does the grant path `g` cover the need path `n`? Component-aware: `g`'s path
/// segments must be a prefix of `n`'s. `/workspace` covers `/workspace/sub` but not
/// `/workspace-secret`. An empty grant covers nothing; a need containing `..` is
/// rejected (no traversal). Lexical only — symlink/realpath confinement is the
/// sandbox's job (SP-4 slice 4).
/// Paths are assumed absolute and normalized (canonicalization is the sandbox's
/// job); a grant of `/` covers the whole tree.
fn path_covers(g: &str, n: &str) -> bool {
    if g.is_empty() {
        return false;
    }
    if n.split('/').any(|s| s == "..") {
        return false;
    }
    let gs: Vec<&str> = g
        .split('/')
        .filter(|s| !s.is_empty() && *s != ".")
        .collect();
    let ns: Vec<&str> = n
        .split('/')
        .filter(|s| !s.is_empty() && *s != ".")
        .collect();
    gs.len() <= ns.len() && gs.iter().zip(&ns).all(|(a, b)| a == b)
}

/// Does the grant host `g` cover the need host `n`? Exact match, or a `*.suffix`
/// wildcard grant matching any subdomain of `suffix` (not the bare domain).
/// Case-insensitive.
fn host_covers(g: &str, n: &str) -> bool {
    let (g, n) = (g.to_lowercase(), n.to_lowercase());
    match g.strip_prefix("*.") {
        Some(suffix) => n
            .strip_suffix(&suffix)
            .is_some_and(|prefix| prefix.ends_with('.')),
        None => g == n,
    }
}

/// When a skill/tool is composed into the prompt (definition-level, §129).
/// `Always` (default) = unconditional inclusion (today's behavior). `OnKeywords`
/// = progressive disclosure: include only when the agent's input matches.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub enum Activation {
    #[default]
    Always,
    OnKeywords(Vec<String>),
}

impl Activation {
    /// Is this active for `query` (the agent's rendered input text)?
    /// `Always` → true. `OnKeywords` → true iff `query` contains ANY keyword,
    /// case-insensitively (an empty keyword list matches nothing; an empty
    /// keyword `""` never matches, so it can't accidentally match everything).
    pub fn is_active(&self, query: &str) -> bool {
        match self {
            Activation::Always => true,
            Activation::OnKeywords(kw) => {
                let q = query.to_lowercase();
                kw.iter()
                    .any(|k| !k.is_empty() && q.contains(&k.to_lowercase()))
            }
        }
    }
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
    /// Enumerate all agent definitions (for planner discovery + feasibility).
    pub fn agents(&self) -> impl Iterator<Item = &AgentDefinition> {
        self.agents.values()
    }
    /// Enumerate all skill definitions.
    pub fn skills(&self) -> impl Iterator<Item = &SkillDef> {
        self.skills.values()
    }
    /// Enumerate all tool specs.
    pub fn tools(&self) -> impl Iterator<Item = &ToolSpec> {
        self.tools.values()
    }
    /// Distinct chain ids the registry references (agent `chain`, per-phase
    /// `chains`, and `(area,kind)` bindings), sorted. Best-effort menu — the full
    /// gateway catalog is not registry-visible.
    pub fn chain_names(&self) -> Vec<String> {
        let mut set = std::collections::BTreeSet::new();
        for a in self.agents.values() {
            if let Some(c) = &a.chain {
                set.insert(c.clone());
            }
            for c in a.chains.values() {
                set.insert(c.clone());
            }
        }
        for c in self.chain_bindings.values() {
            set.insert(c.clone());
        }
        set.into_iter().collect()
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
    /// Per-phase `chains` are overrides *layered on* the base route (the explicit
    /// `chain` or the `(area,kind)` binding), so [`validate`](Self::validate)
    /// requires a base route even when per-phase overrides are present.
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

    /// Fail loud on *structural* unresolvability only: any agent that references
    /// a skill/tool the registry doesn't hold, or an unroutable chain. Grants are
    /// NOT checked here — a grant narrower than a tool's declared permission
    /// surface is legal and is enforced per-call at runtime (ceiling model, SP-4 s1).
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

/// A cheaply-clonable handle to a live, swappable [`Registry`] + a monotonic
/// config generation. Clones share one `Arc<RwLock<…>>`, so an operator's clone
/// and the executor's clone observe the same swaps (SP-2 hot-reload).
#[derive(Clone)]
pub struct RegistryHandle {
    inner: Arc<RwLock<(Arc<Registry>, u64)>>,
}

impl RegistryHandle {
    /// A handle over `registry` at generation 0.
    pub fn new(registry: Registry) -> Self {
        Self {
            inner: Arc::new(RwLock::new((Arc::new(registry), 0))),
        }
    }

    /// The current registry (clones the `Arc`).
    pub fn current(&self) -> Arc<Registry> {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .0
            .clone()
    }

    /// The current config generation.
    pub fn generation(&self) -> u64 {
        self.inner.read().unwrap_or_else(|e| e.into_inner()).1
    }

    /// The atomic `(registry, generation)` pair — read together so a run pins a
    /// consistent snapshot.
    pub fn snapshot(&self) -> (Arc<Registry>, u64) {
        let g = self.inner.read().unwrap_or_else(|e| e.into_inner());
        (g.0.clone(), g.1)
    }

    /// Reload from `source`, atomically swapping in the new registry and bumping
    /// the generation (returns the new generation). Validated + last-good: the
    /// load + `Registry::from_config` (which validates) run BEFORE the swap, so a
    /// failed load/validate returns `Err` with the old registry still live. The
    /// `.await` is OUTSIDE the lock.
    pub async fn reload(&self, source: &dyn ConfigSource) -> Result<u64, OrchestratorError> {
        let cfg = source.load().await?;
        let next = Registry::from_config(cfg)?;
        let mut w = self.inner.write().unwrap_or_else(|e| e.into_inner());
        w.0 = Arc::new(next);
        w.1 += 1;
        Ok(w.1)
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
            grants: HashMap::new(),
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
        // A scalar `activate_on` is a loud parse error: forgetting the brackets
        // (`activate_on: summarize`) must NOT silently become `Always` and defeat
        // the gate. Absent → Always; empty list → Always (no gating); non-empty
        // list → OnKeywords.
        let activation = match f.get("activate_on") {
            None => Activation::Always,
            Some(FmValue::List(kw)) if !kw.is_empty() => Activation::OnKeywords(kw.clone()),
            Some(FmValue::List(_)) => Activation::Always,
            Some(FmValue::Scalar(_)) => {
                return Err(OrchestratorError::FrontmatterParse(
                    "activate_on must be a list, e.g. [summarize, tldr]".into(),
                ));
            }
        };
        Ok(SkillDef {
            name: required_scalar(&f, "name")?,
            description,
            body: body.to_string(),
            activation,
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
            permissions: Permissions::default(),
            activation: Activation::default(),
            credentials: vec![],
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
                activation: Activation::default(),
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
                activation: Activation::default(),
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
                    activation: Activation::default(),
                },
                SkillDef {
                    name: "s".into(),
                    description: None,
                    body: "b2".into(),
                    activation: Activation::default(),
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
                activation: Activation::default(),
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
            grants: HashMap::new(),
            tools: vec![],
            skills: vec![],
            system_prompt: "SYS".into(),
        }
    }

    fn tool_needing(name: &str, need: Permissions) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: None,
            input_schema: serde_json::json!({}),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: need,
            activation: Activation::default(),
            credentials: vec![],
        }
    }

    #[test]
    fn validate_accepts_a_grant_narrower_than_the_tool_surface() {
        // SP-4: a grant narrower than a tool's declared surface is now LEGAL
        // (enforced per-call at runtime), so `validate` no longer errors at load.
        // The tool declares a path need; the agent lists it but grants nothing.
        let tool = tool_needing(
            "fs.write",
            Permissions {
                paths: vec!["/workspace".into()],
                ..Default::default()
            },
        );
        let mut agent = role_agent("coding", "reasoning", Some("c"));
        agent.tools = vec!["fs.write".into()]; // lists the tool, grants nothing
        let reg = Registry::default().with_agent(agent).with_tool(tool);
        assert!(
            reg.validate().is_ok(),
            "a narrower-than-surface (here: absent) grant is legal now"
        );
    }

    #[test]
    fn validate_needs_no_grant_for_a_permissionless_tool() {
        // A tool with default (empty) permissions requires no grant.
        let mut agent = role_agent("coding", "reasoning", Some("c"));
        agent.tools = vec!["calc".into()];
        let reg = Registry::default()
            .with_agent(agent)
            .with_tool(tool_needing("calc", Permissions::default()));
        assert!(reg.validate().is_ok());
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
    fn agent_definition_deserializes_without_a_chains_field() {
        // A backend (e.g. a future DB ConfigSource) may serde-deserialize an
        // AgentDefinition with no `chains` key — it must default to empty, not error.
        let json = r#"{"name":"a","area":"r","kind":"k","chain":"c","tools":[],"skills":[],"system_prompt":"s"}"#;
        let ag: AgentDefinition = serde_json::from_str(json).expect("deserializes without chains");
        assert!(ag.chains.is_empty());
        assert_eq!(ag.chain.as_deref(), Some("c"));
    }

    #[test]
    fn permission_fields_deserialize_to_secure_defaults_when_absent() {
        // A backend deserializing config without the new permission fields must
        // default to the empty/deny `Permissions` — never error, never open.
        let tool: ToolSpec =
            serde_json::from_str(r#"{"name":"t","input_schema":{},"effect_class":"Pure"}"#)
                .expect("ToolSpec without permissions");
        assert_eq!(tool.permissions, Permissions::default());
        let ag: AgentDefinition = serde_json::from_str(
            r#"{"name":"a","area":"r","kind":"k","chain":"c","tools":[],"skills":[],"system_prompt":"s"}"#,
        )
        .expect("AgentDefinition without grants");
        assert!(ag.grants.is_empty());
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

    #[test]
    fn permissions_covers_each_dimension() {
        // paths: prefix covers; non-prefix fails.
        let grant = Permissions {
            paths: vec!["/workspace".into()],
            ..Default::default()
        };
        let need = Permissions {
            paths: vec!["/workspace/src/main.rs".into()],
            ..Default::default()
        };
        assert!(grant.covers(&need), "prefix grant covers deeper need");
        let bad = Permissions {
            paths: vec!["/etc/passwd".into()],
            ..Default::default()
        };
        assert!(!grant.covers(&bad), "non-prefix path not covered");

        // commands: subset covers; extra needed fails.
        let g = Permissions {
            commands: vec!["ls".into(), "cat".into()],
            ..Default::default()
        };
        assert!(g.covers(&Permissions {
            commands: vec!["ls".into()],
            ..Default::default()
        }));
        assert!(!g.covers(&Permissions {
            commands: vec!["rm".into()],
            ..Default::default()
        }));

        // network: Any ⊇ all; Hosts ⊇ subset & ⊇ Deny; Deny only ⊇ Deny.
        let any = Permissions {
            network: NetworkPolicy::Any,
            ..Default::default()
        };
        let hosts = Permissions {
            network: NetworkPolicy::Hosts(vec!["a.com".into(), "b.com".into()]),
            ..Default::default()
        };
        let deny = Permissions::default(); // network defaults to Deny
        assert!(any.covers(&hosts) && any.covers(&deny));
        assert!(hosts.covers(&Permissions {
            network: NetworkPolicy::Hosts(vec!["a.com".into()]),
            ..Default::default()
        }));
        assert!(hosts.covers(&deny));
        assert!(!hosts.covers(&any), "Hosts grant does not cover Any need");
        assert!(
            !hosts.covers(&Permissions {
                network: NetworkPolicy::Hosts(vec!["c.com".into()]),
                ..Default::default()
            }),
            "Hosts grant missing the needed host is not covered"
        );
        assert!(!deny.covers(&hosts), "Deny grant does not cover Hosts need");
        assert!(deny.covers(&Permissions::default()), "Deny covers Deny");
        assert_eq!(
            NetworkPolicy::default(),
            NetworkPolicy::Deny,
            "network default is the secure Deny"
        );

        // caps: need ≤ grant covers; need > grant fails; grant None = unlimited; need None trivially covered.
        let capped = Permissions {
            caps: ResourceCaps {
                mem_bytes: Some(1000),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(capped.covers(&Permissions {
            caps: ResourceCaps {
                mem_bytes: Some(500),
                ..Default::default()
            },
            ..Default::default()
        }));
        assert!(!capped.covers(&Permissions {
            caps: ResourceCaps {
                mem_bytes: Some(2000),
                ..Default::default()
            },
            ..Default::default()
        }));
        let uncapped = Permissions::default(); // caps all None
        assert!(
            uncapped.covers(&Permissions {
                caps: ResourceCaps {
                    mem_bytes: Some(9999),
                    ..Default::default()
                },
                ..Default::default()
            }),
            "grant None cap = unlimited, covers any need"
        );
        assert!(
            capped.covers(&Permissions::default()),
            "need None cap trivially covered"
        );

        // empty needs covered by anything (incl. an empty grant).
        assert!(Permissions::default().covers(&Permissions::default()));
    }

    #[test]
    fn covers_paths_are_component_aware() {
        let grant = Permissions {
            paths: vec!["/workspace".into()],
            ..Default::default()
        };
        let inside = Permissions {
            paths: vec!["/workspace/sub/a.txt".into()],
            ..Default::default()
        };
        let sibling = Permissions {
            paths: vec!["/workspace-secret".into()],
            ..Default::default()
        };
        let outside = Permissions {
            paths: vec!["/etc/passwd".into()],
            ..Default::default()
        };
        assert!(grant.covers(&inside));
        assert!(grant.covers(&grant), "a path covers itself");
        assert!(
            !grant.covers(&sibling),
            "/workspace must NOT cover /workspace-secret"
        );
        assert!(!grant.covers(&outside));
        let empty_grant = Permissions {
            paths: vec!["".into()],
            ..Default::default()
        };
        assert!(
            !empty_grant.covers(&inside),
            "an empty grant path covers nothing"
        );
        let traversal = Permissions {
            paths: vec!["/workspace/../etc".into()],
            ..Default::default()
        };
        assert!(
            !grant.covers(&traversal),
            "a `..` traversal need is rejected"
        );
    }

    #[test]
    fn covers_hosts_support_wildcards() {
        let wild = Permissions {
            network: NetworkPolicy::Hosts(vec!["*.example.com".into()]),
            ..Default::default()
        };
        let sub = Permissions {
            network: NetworkPolicy::Hosts(vec!["api.example.com".into()]),
            ..Default::default()
        };
        let evil = Permissions {
            network: NetworkPolicy::Hosts(vec!["example.evil.com".into()]),
            ..Default::default()
        };
        let bare = Permissions {
            network: NetworkPolicy::Hosts(vec!["example.com".into()]),
            ..Default::default()
        };
        assert!(wild.covers(&sub));
        assert!(
            !wild.covers(&evil),
            "*.example.com must not cover example.evil.com"
        );
        assert!(
            !wild.covers(&bare),
            "*.example.com does not match the bare domain"
        );
        let no_dot = Permissions {
            network: NetworkPolicy::Hosts(vec!["notexample.com".into()]),
            ..Default::default()
        };
        let left_label = Permissions {
            network: NetworkPolicy::Hosts(vec!["example.com.attacker.com".into()]),
            ..Default::default()
        };
        assert!(
            !wild.covers(&no_dot),
            "*.example.com must not cover notexample.com (no dot boundary)"
        );
        assert!(
            !wild.covers(&left_label),
            "*.example.com must not cover example.com.attacker.com"
        );
        let exact = Permissions {
            network: NetworkPolicy::Hosts(vec!["example.com".into()]),
            ..Default::default()
        };
        assert!(exact.covers(&bare));
        assert!(
            !exact.covers(&sub),
            "an exact host grant does not cover a subdomain"
        );
    }

    #[test]
    fn activation_is_active_matches_keywords_case_insensitively() {
        assert!(Activation::Always.is_active(""));
        assert!(Activation::Always.is_active("anything"));

        let on = Activation::OnKeywords(vec!["Summarize".into(), "TLDR".into()]);
        assert!(
            on.is_active("please summarize this"),
            "case-insensitive substring, any-of"
        );
        assert!(on.is_active("give me a tldr"));
        // Also case-insensitive on the QUERY side (mixed/upper-case query vs the keywords).
        assert!(on.is_active("Please SUMMARIZE Now"));
        assert!(
            !on.is_active("translate to french"),
            "no keyword → inactive"
        );

        // Empty keyword list matches nothing.
        assert!(!Activation::OnKeywords(vec![]).is_active("summarize"));

        // An empty keyword must not match everything.
        assert!(!Activation::OnKeywords(vec!["".into()]).is_active("anything"));
        assert!(
            Activation::OnKeywords(vec!["".into(), "cat".into()]).is_active("a cat"),
            "other keywords still work"
        );

        // Default is Always.
        assert_eq!(Activation::default(), Activation::Always);
    }

    #[test]
    fn skill_frontmatter_parses_activate_on_into_onkeywords() {
        let md = "---\nname: s\nactivate_on: [summarize, tldr]\n---\nBODY\n";
        let s = SkillDef::from_frontmatter(md).unwrap();
        assert_eq!(
            s.activation,
            Activation::OnKeywords(vec!["summarize".into(), "tldr".into()])
        );

        // Absent activate_on → Always.
        let s2 = SkillDef::from_frontmatter("---\nname: s\n---\nBODY\n").unwrap();
        assert_eq!(s2.activation, Activation::Always);
    }

    #[test]
    fn skill_frontmatter_scalar_activate_on_is_a_loud_error() {
        // Forgot the brackets → must NOT silently become Always; a scalar activate_on is loud.
        let md = "---\nname: s\nactivate_on: summarize\n---\nBODY\n";
        assert!(
            matches!(
                SkillDef::from_frontmatter(md),
                Err(OrchestratorError::FrontmatterParse(_))
            ),
            "scalar activate_on must be a loud FrontmatterParse, not a silent Always"
        );
    }

    #[test]
    fn tool_spec_deserializes_activation_default_and_onkeywords() {
        // Absent → Always.
        let t: ToolSpec =
            serde_json::from_str(r#"{"name":"t","input_schema":{},"effect_class":"Pure"}"#)
                .unwrap();
        assert_eq!(t.activation, Activation::Always);
        // Explicit OnKeywords round-trips.
        let t2: ToolSpec = serde_json::from_str(
            r#"{"name":"t","input_schema":{},"effect_class":"Pure","activation":{"OnKeywords":["sql"]}}"#,
        )
        .unwrap();
        assert_eq!(t2.activation, Activation::OnKeywords(vec!["sql".into()]));
    }

    #[test]
    fn tool_spec_credentials_default_empty() {
        let spec: ToolSpec =
            serde_json::from_str(r#"{"name":"t","input_schema":{},"effect_class":"Pure"}"#)
                .unwrap();
        assert!(spec.credentials.is_empty());
    }

    // A minimal in-core ConfigSource for handle tests (yields a fixed config).
    struct FixedSource(RegistryConfig);
    #[async_trait::async_trait]
    impl ConfigSource for FixedSource {
        async fn load(&self) -> Result<RegistryConfig, OrchestratorError> {
            Ok(self.0.clone())
        }
    }

    fn cfg_with_skill(name: &str) -> RegistryConfig {
        RegistryConfig {
            agents: vec![],
            skills: vec![SkillDef {
                name: name.into(),
                description: None,
                body: "B".into(),
                activation: Activation::default(),
            }],
            tools: vec![],
            chain_bindings: vec![],
        }
    }

    #[tokio::test]
    async fn registry_handle_new_current_and_generation() {
        let reg = Registry::from_config(cfg_with_skill("s0")).unwrap();
        let h = RegistryHandle::new(reg);
        assert_eq!(h.generation(), 0);
        assert!(h.current().skill("s0").is_some());
        let (snap_reg, snap_gen) = h.snapshot();
        assert_eq!(snap_gen, 0);
        assert!(snap_reg.skill("s0").is_some());
    }

    #[tokio::test]
    async fn registry_handle_reload_swaps_and_bumps_generation() {
        let h = RegistryHandle::new(Registry::from_config(cfg_with_skill("s0")).unwrap());
        let new_gen = h.reload(&FixedSource(cfg_with_skill("s1"))).await.unwrap();
        assert_eq!(new_gen, 1);
        assert_eq!(h.generation(), 1);
        assert!(h.current().skill("s1").is_some(), "new config is live");
        assert!(h.current().skill("s0").is_none(), "old config swapped out");
    }

    #[tokio::test]
    async fn registry_handle_reload_is_validated_and_last_good() {
        let h = RegistryHandle::new(Registry::from_config(cfg_with_skill("s0")).unwrap());
        // A config whose agent references a missing tool → from_config validate fails.
        let bad = RegistryConfig {
            agents: vec![AgentDefinition {
                name: "a".into(),
                area: "x".into(),
                kind: "y".into(),
                chain: Some("c".into()),
                chains: std::collections::HashMap::new(),
                grants: std::collections::HashMap::new(),
                tools: vec!["missing".into()],
                skills: vec![],
                system_prompt: "s".into(),
            }],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![],
        };
        let err = h.reload(&FixedSource(bad)).await;
        assert!(err.is_err(), "invalid config → reload errors");
        // Last-good preserved: old registry still live, generation unchanged.
        assert_eq!(h.generation(), 0);
        assert!(h.current().skill("s0").is_some());
    }
}
