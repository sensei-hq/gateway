//! A pure diff between the durable config and an incoming one. This is the guard
//! in front of a replace-all write, so it must never under-report a removal.

use orchestrator_core::RegistryConfig;

// Consumed by Task 7 (cmd/config.rs plan_push), which renders a diff for operator
// confirmation before a replace-all config push.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityKind {
    Agent,
    Skill,
    Tool,
    ChainBinding,
}

impl EntityKind {
    // Consumed by Task 7 (cmd/config.rs plan_push / render.rs).
    #[allow(dead_code)]
    pub fn label(&self) -> &'static str {
        match self {
            EntityKind::Agent => "agent",
            EntityKind::Skill => "skill",
            EntityKind::Tool => "tool",
            EntityKind::ChainBinding => "chain",
        }
    }
}

// Consumed by Task 7 (cmd/config.rs plan_push).
#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffEntry {
    pub kind: EntityKind,
    pub name: String,
}

// Consumed by Task 7 (cmd/config.rs plan_push).
#[allow(dead_code)]
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ConfigDiff {
    pub added: Vec<DiffEntry>,
    pub changed: Vec<DiffEntry>,
    pub removed: Vec<DiffEntry>,
    pub unchanged: usize,
}

impl ConfigDiff {
    /// Any removal requires explicit operator confirmation: a replace-all write
    /// makes a removal unrecoverable.
    // Consumed by Task 7 (cmd/config.rs plan_push), gating the confirmation prompt.
    #[allow(dead_code)]
    pub fn requires_confirmation(&self) -> bool {
        !self.removed.is_empty()
    }

    // Consumed by Task 7 (cmd/config.rs plan_push).
    #[allow(dead_code)]
    pub fn is_noop(&self) -> bool {
        self.added.is_empty() && self.changed.is_empty() && self.removed.is_empty()
    }
}

use std::collections::BTreeMap;

/// Index an entity set by a comparison key -> json, for order-insensitive
/// comparison. `K` must be the entity's real identity (e.g. a `(area, kind)`
/// tuple for a chain binding) — never a joined display string, which can
/// collide across two distinct entities and hide a removal (see `compare`).
// Only called from `diff`, below, which itself isn't consumed outside tests until
// Task 7 (cmd/config.rs plan_push) — see the allow there.
#[allow(dead_code)]
fn index<T, K>(items: &[T], key_of: impl Fn(&T) -> K) -> BTreeMap<K, serde_json::Value>
where
    T: serde::Serialize,
    K: Ord + std::fmt::Debug,
{
    let mut out = BTreeMap::new();
    for item in items {
        let v = serde_json::to_value(item).expect(
            "a config entity must serialize: every RegistryConfig type is a plain \
             derive(Serialize) struct with no non-string map keys and no float fields, \
             so to_value cannot fail. Coercing a failure to Null here would make two \
             unserializable entities compare EQUAL and be reported 'unchanged' — a \
             silent under-report in the guard that prevents config loss.",
        );
        let k = key_of(item);
        // Duplicate keys are impossible on the real call path (`current` comes from
        // PK-backed tables, `incoming` is validated by Registry::from_config first),
        // but silent last-write-wins would UNDER-COUNT a removal, so a caller that
        // bypasses that validation must not fail quietly.
        debug_assert!(!out.contains_key(&k), "duplicate config entity key: {k:?}");
        out.insert(k, v);
    }
    out
}

// Only called from `diff`, below. See the allow there.
#[allow(dead_code)]
fn compare<K>(
    kind: EntityKind,
    current: &BTreeMap<K, serde_json::Value>,
    incoming: &BTreeMap<K, serde_json::Value>,
    display: impl Fn(&K) -> String,
    out: &mut ConfigDiff,
) where
    K: Ord + std::fmt::Debug,
{
    for (key, new_v) in incoming {
        match current.get(key) {
            None => out.added.push(DiffEntry {
                kind,
                name: display(key),
            }),
            Some(old_v) if old_v != new_v => out.changed.push(DiffEntry {
                kind,
                name: display(key),
            }),
            Some(_) => out.unchanged += 1,
        }
    }
    for key in current.keys() {
        if !incoming.contains_key(key) {
            out.removed.push(DiffEntry {
                kind,
                name: display(key),
            });
        }
    }
}

/// Diff two configs entity-by-entity (agents, skills, tools, chain bindings).
///
/// Precondition: names are assumed unique within each `Vec` of a given
/// `RegistryConfig` (and a chain binding's `(area, kind)` pair is assumed
/// unique among chain bindings). `current` gets this from the PK-backed
/// `config_*` tables; `incoming` gets this from `Registry::from_config` running
/// first (it rejects duplicates before this function ever sees the data). If a
/// caller bypasses that and hands in duplicates, `index`'s `debug_assert!`
/// catches it in a debug build rather than silently under-counting via
/// last-write-wins.
// Consumed by Task 7 (cmd/config.rs plan_push), the guard in front of the
// replace-all `ConfigSource::store` write.
#[allow(dead_code)]
pub fn diff(current: &RegistryConfig, incoming: &RegistryConfig) -> ConfigDiff {
    let mut out = ConfigDiff::default();
    let name_key = |n: &String| n.clone();
    compare(
        EntityKind::Agent,
        &index(&current.agents, |a| a.name.clone()),
        &index(&incoming.agents, |a| a.name.clone()),
        name_key,
        &mut out,
    );
    compare(
        EntityKind::Skill,
        &index(&current.skills, |s| s.name.clone()),
        &index(&incoming.skills, |s| s.name.clone()),
        name_key,
        &mut out,
    );
    compare(
        EntityKind::Tool,
        &index(&current.tools, |t| t.name.clone()),
        &index(&incoming.tools, |t| t.name.clone()),
        name_key,
        &mut out,
    );
    // A binding's identity is the real (area, kind) tuple — NOT a joined display
    // string, which is not injective over free-text area/kind values and can
    // collide across two distinct bindings (hiding a removal). `chain` is the
    // value that changes.
    let key = |b: &orchestrator_core::ChainBinding| (b.area.clone(), b.kind.clone());
    let display = |(a, k): &(String, String)| format!("{a}/{k}");
    compare(
        EntityKind::ChainBinding,
        &index(&current.chain_bindings, key),
        &index(&incoming.chain_bindings, key),
        display,
        &mut out,
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{
        Activation, AgentDefinition, ChainBinding, EffectClass, SkillDef, ToolSpec,
    };

    fn skill(name: &str, body: &str) -> SkillDef {
        SkillDef {
            name: name.into(),
            description: None,
            body: body.into(),
            activation: Activation::default(),
        }
    }

    fn agent(name: &str) -> AgentDefinition {
        AgentDefinition {
            name: name.into(),
            area: "research".into(),
            kind: "reasoning".into(),
            chain: None,
            chains: Default::default(),
            grants: Default::default(),
            tools: vec![],
            skills: vec![],
            system_prompt: "you are an agent".into(),
        }
    }

    fn tool(name: &str, description: &str) -> ToolSpec {
        ToolSpec {
            name: name.into(),
            description: Some(description.into()),
            input_schema: serde_json::json!({}),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: Default::default(),
            activation: Activation::default(),
            credentials: vec![],
        }
    }

    fn cfg_with_skills(skills: Vec<SkillDef>) -> RegistryConfig {
        RegistryConfig {
            agents: vec![],
            skills,
            tools: vec![],
            chain_bindings: vec![],
        }
    }

    #[test]
    fn an_added_skill_is_reported_as_added() {
        let d = diff(
            &cfg_with_skills(vec![]),
            &cfg_with_skills(vec![skill("s", "b")]),
        );
        assert_eq!(
            d.added,
            vec![DiffEntry {
                kind: EntityKind::Skill,
                name: "s".into()
            }]
        );
        assert!(d.changed.is_empty());
        assert!(d.removed.is_empty());
        assert!(
            !d.requires_confirmation(),
            "a pure addition needs no confirmation"
        );
    }

    /// The Agent and Tool arms of `diff` are copy-paste of the Skill arm; without
    /// a direct test, a slip (wrong field, swapped current/incoming) would
    /// compile and pass every other test.
    #[test]
    fn an_added_agent_is_reported_under_agent_kind() {
        let current = RegistryConfig {
            agents: vec![],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![],
        };
        let incoming = RegistryConfig {
            agents: vec![agent("a")],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![],
        };
        let d = diff(&current, &incoming);
        assert_eq!(
            d.added,
            vec![DiffEntry {
                kind: EntityKind::Agent,
                name: "a".into()
            }]
        );
        assert!(d.changed.is_empty());
        assert!(d.removed.is_empty());
    }

    #[test]
    fn a_changed_tool_is_reported_as_changed_under_tool_kind() {
        let current = RegistryConfig {
            agents: vec![],
            skills: vec![],
            tools: vec![tool("t", "old description")],
            chain_bindings: vec![],
        };
        let incoming = RegistryConfig {
            agents: vec![],
            skills: vec![],
            tools: vec![tool("t", "new description")],
            chain_bindings: vec![],
        };
        let d = diff(&current, &incoming);
        assert_eq!(
            d.changed,
            vec![DiffEntry {
                kind: EntityKind::Tool,
                name: "t".into()
            }]
        );
        assert!(d.added.is_empty());
        assert!(d.removed.is_empty());
    }

    #[test]
    fn a_changed_body_is_reported_as_changed_not_added() {
        let d = diff(
            &cfg_with_skills(vec![skill("s", "old")]),
            &cfg_with_skills(vec![skill("s", "new")]),
        );
        assert_eq!(
            d.changed,
            vec![DiffEntry {
                kind: EntityKind::Skill,
                name: "s".into()
            }]
        );
        assert!(d.added.is_empty());
        assert!(d.removed.is_empty());
        assert_eq!(d.unchanged, 0);
    }

    #[test]
    fn an_identical_config_is_a_noop() {
        let c = cfg_with_skills(vec![skill("s", "b")]);
        let d = diff(&c, &c);
        assert!(d.is_noop(), "{d:?}");
        assert_eq!(d.unchanged, 1);
        assert!(!d.requires_confirmation());
    }

    /// THE case that matters: pushing an empty directory over a populated database
    /// is a total wipe. Reporting it as "no changes" would be catastrophic.
    #[test]
    fn an_empty_incoming_config_reports_everything_removed() {
        let current = RegistryConfig {
            agents: vec![],
            skills: vec![skill("a", "x"), skill("b", "y")],
            tools: vec![],
            chain_bindings: vec![ChainBinding {
                area: "research".into(),
                kind: "reasoning".into(),
                chain: "c".into(),
            }],
        };
        let d = diff(&current, &cfg_with_skills(vec![]));
        assert_eq!(
            d.removed.len(),
            3,
            "2 skills + 1 binding must all be reported: {d:?}"
        );
        assert!(d.added.is_empty());
        assert!(
            d.requires_confirmation(),
            "a total wipe MUST require confirmation"
        );
    }

    #[test]
    fn a_chain_binding_is_keyed_by_area_and_kind() {
        let b = |chain: &str| ChainBinding {
            area: "research".into(),
            kind: "reasoning".into(),
            chain: chain.into(),
        };
        let current = RegistryConfig {
            agents: vec![],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![b("old")],
        };
        let incoming = RegistryConfig {
            agents: vec![],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![b("new")],
        };
        let d = diff(&current, &incoming);
        assert_eq!(
            d.changed,
            vec![DiffEntry {
                kind: EntityKind::ChainBinding,
                name: "research/reasoning".into()
            }],
            "same (area,kind) with a different chain is a CHANGE, not add+remove: {d:?}"
        );
    }

    /// `area`/`kind` are free text, so joining them with a bare `/` is not injective:
    /// two different pairs could collide, and the collision hid a REMOVAL behind a
    /// spurious "changed" — leaving `removed` empty so `requires_confirmation()` said
    /// false and the replace-all write destroyed a live binding with no prompt.
    #[test]
    fn a_chain_binding_key_collision_does_not_hide_a_removal() {
        let bind = |area: &str, kind: &str, chain: &str| ChainBinding {
            area: area.into(),
            kind: kind.into(),
            chain: chain.into(),
        };
        let current = RegistryConfig {
            agents: vec![],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![bind("research/reasoning", "x", "keep-me")],
        };
        let incoming = RegistryConfig {
            agents: vec![],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![bind("research", "reasoning/x", "unrelated")],
        };
        let d = diff(&current, &incoming);
        assert_eq!(
            d.removed.len(),
            1,
            "the destroyed binding must be reported: {d:?}"
        );
        assert_eq!(
            d.added.len(),
            1,
            "the new binding is an addition, not a change: {d:?}"
        );
        assert!(
            d.changed.is_empty(),
            "these are different bindings, not one changed: {d:?}"
        );
        assert!(
            d.requires_confirmation(),
            "a push that destroys a live binding MUST require confirmation: {d:?}"
        );
    }
}
