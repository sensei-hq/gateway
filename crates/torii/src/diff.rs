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

/// Serialize a named entity set into name -> json for order-insensitive comparison.
// Only called from `diff`, below, which itself isn't consumed outside tests until
// Task 7 (cmd/config.rs plan_push) — see the allow there.
#[allow(dead_code)]
fn index<T: serde::Serialize>(
    items: &[T],
    name_of: impl Fn(&T) -> String,
) -> BTreeMap<String, serde_json::Value> {
    items
        .iter()
        .map(|i| {
            let v = serde_json::to_value(i).expect(
                "a config entity must serialize: every RegistryConfig type is a plain \
                 derive(Serialize) struct with no non-string map keys and no float fields, \
                 so to_value cannot fail. Coercing a failure to Null here would make two \
                 unserializable entities compare EQUAL and be reported 'unchanged' — a \
                 silent under-report in the guard that prevents config loss.",
            );
            (name_of(i), v)
        })
        .collect()
}

// Only called from `diff`, below. See the allow there.
#[allow(dead_code)]
fn compare(
    kind: EntityKind,
    current: &BTreeMap<String, serde_json::Value>,
    incoming: &BTreeMap<String, serde_json::Value>,
    out: &mut ConfigDiff,
) {
    for (name, new_v) in incoming {
        match current.get(name) {
            None => out.added.push(DiffEntry {
                kind,
                name: name.clone(),
            }),
            Some(old_v) if old_v != new_v => out.changed.push(DiffEntry {
                kind,
                name: name.clone(),
            }),
            Some(_) => out.unchanged += 1,
        }
    }
    for name in current.keys() {
        if !incoming.contains_key(name) {
            out.removed.push(DiffEntry {
                kind,
                name: name.clone(),
            });
        }
    }
}

// Consumed by Task 7 (cmd/config.rs plan_push), the guard in front of the
// replace-all `ConfigSource::store` write.
#[allow(dead_code)]
pub fn diff(current: &RegistryConfig, incoming: &RegistryConfig) -> ConfigDiff {
    let mut out = ConfigDiff::default();
    compare(
        EntityKind::Agent,
        &index(&current.agents, |a| a.name.clone()),
        &index(&incoming.agents, |a| a.name.clone()),
        &mut out,
    );
    compare(
        EntityKind::Skill,
        &index(&current.skills, |s| s.name.clone()),
        &index(&incoming.skills, |s| s.name.clone()),
        &mut out,
    );
    compare(
        EntityKind::Tool,
        &index(&current.tools, |t| t.name.clone()),
        &index(&incoming.tools, |t| t.name.clone()),
        &mut out,
    );
    // A binding's identity is (area, kind); its `chain` is the value that changes.
    let key = |b: &orchestrator_core::ChainBinding| format!("{}/{}", b.area, b.kind);
    compare(
        EntityKind::ChainBinding,
        &index(&current.chain_bindings, key),
        &index(&incoming.chain_bindings, key),
        &mut out,
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{Activation, ChainBinding, SkillDef};

    fn skill(name: &str, body: &str) -> SkillDef {
        SkillDef {
            name: name.into(),
            description: None,
            body: body.into(),
            activation: Activation::default(),
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
}
