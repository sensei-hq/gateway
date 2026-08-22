//! The durable config write path. Validate before writing, diff before
//! overwriting, and never write content without advancing the generation.

use crate::cmd::Outcome;
use crate::diff::{ConfigDiff, diff};
use crate::errors::CliError;
use orchestrator_core::{ConfigSource, Registry, RegistryConfig};
use orchestrator_store::FilesystemConfigSource;
use orchestrator_store::postgres::PostgresConfigSource;
use std::path::Path;

/// What `plan_push` decided, before any write happens.
// Consumed by Task 10 (main.rs clap dispatch), `torii config push <dir>`.
#[allow(dead_code)]
#[derive(Debug)]
pub enum PushDecision {
    /// Nothing to do — the incoming config matches the durable one.
    NoOp(ConfigDiff),
    /// Safe to write.
    Apply(ConfigDiff),
    /// Refused: removals need confirmation that was not given.
    NeedsConfirmation(ConfigDiff),
}

/// The pure decision: is this push safe to apply? `confirmed` is true when the
/// operator passed `--yes` or answered the prompt.
// Consumed by Task 10 (main.rs clap dispatch), `torii config push <dir>`.
#[allow(dead_code)]
pub fn plan_push(
    current: &RegistryConfig,
    incoming: &RegistryConfig,
    confirmed: bool,
) -> PushDecision {
    let d = diff(current, incoming);
    if d.is_noop() {
        return PushDecision::NoOp(d);
    }
    if d.requires_confirmation() && !confirmed {
        return PushDecision::NeedsConfirmation(d);
    }
    PushDecision::Apply(d)
}

/// Render a diff for the operator.
// Consumed by Task 10 (main.rs clap dispatch), `torii config push <dir>`.
#[allow(dead_code)]
pub fn describe_diff(d: &ConfigDiff, current_version: u64, source: &str) -> String {
    let mut s = format!("config diff (durable v{current_version} -> {source}):\n");
    for e in &d.added {
        s.push_str(&format!("  + {:<6} {}\n", e.kind.label(), e.name));
    }
    for e in &d.changed {
        s.push_str(&format!("  ~ {:<6} {}\n", e.kind.label(), e.name));
    }
    for e in &d.removed {
        s.push_str(&format!("  - {:<6} {}\n", e.kind.label(), e.name));
    }
    s.push_str(&format!("  = {} unchanged\n", d.unchanged));
    if d.requires_confirmation() {
        s.push_str(&format!(
            "\nThis REMOVES {} entities. A push is replace-all: removed entities cannot be recovered.\n",
            d.removed.len()
        ));
    }
    s
}

// Consumed by Task 10 (main.rs clap dispatch), `torii config version`.
#[allow(dead_code)]
pub async fn version(src: &PostgresConfigSource, json: bool) -> Result<Outcome, CliError> {
    let v = src.version().await?.unwrap_or(0);
    Ok(Outcome::ok(if json {
        serde_json::json!({ "version": v }).to_string()
    } else {
        format!("config version: {v}")
    }))
}

/// `confirm` is called ONLY when the diff removes something and `--yes` was absent.
/// It returns false on a non-interactive stdin, so a scripted push that would
/// delete config refuses instead of proceeding.
// Consumed by Task 10 (main.rs clap dispatch), `torii config push <dir>`.
#[allow(dead_code)]
pub async fn push(
    src: &PostgresConfigSource,
    dir: &Path,
    yes: bool,
    confirm: &mut dyn FnMut(&str) -> bool,
) -> Result<Outcome, CliError> {
    // 1. Load AND VALIDATE the incoming config before touching a single row.
    let incoming = FilesystemConfigSource::new(dir).load().await?;
    Registry::from_config(incoming.clone()).map_err(|e| {
        CliError::error(format!(
            "refusing to push: {} does not assemble into a valid registry: {e}",
            dir.display()
        ))
    })?;

    // 2. One atomic read of the durable (content, generation) pair.
    let (current, current_v) = src.load_versioned().await?;
    let current_v = current_v.unwrap_or(0);

    // 3. Decide.
    let source = dir.display().to_string();
    match plan_push(&current, &incoming, yes) {
        PushDecision::NoOp(_) => Ok(Outcome::ok(format!(
            "no changes: {source} already matches durable config v{current_v}"
        ))),
        PushDecision::NeedsConfirmation(d) => {
            let text = describe_diff(&d, current_v, &source);
            if !confirm(&text) {
                return Ok(Outcome::precondition(format!(
                    "{text}\nrefused: nothing written, config still at v{current_v}"
                )));
            }
            let v = src.store_and_bump(&incoming).await?;
            Ok(Outcome::ok(format!("{text}\npushed: config now at v{v}")))
        }
        PushDecision::Apply(d) => {
            let text = describe_diff(&d, current_v, &source);
            let v = src.store_and_bump(&incoming).await?;
            Ok(Outcome::ok(format!("{text}\npushed: config now at v{v}")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::{Activation, SkillDef};

    fn skill(name: &str, body: &str) -> SkillDef {
        SkillDef {
            name: name.into(),
            description: None,
            body: body.into(),
            activation: Activation::default(),
        }
    }

    fn cfg(skills: Vec<SkillDef>) -> RegistryConfig {
        RegistryConfig {
            agents: vec![],
            skills,
            tools: vec![],
            chain_bindings: vec![],
        }
    }

    #[test]
    fn a_pure_addition_applies_without_confirmation() {
        let d = plan_push(&cfg(vec![]), &cfg(vec![skill("s", "b")]), false);
        assert!(matches!(d, PushDecision::Apply(_)), "{d:?}");
    }

    #[test]
    fn an_identical_config_is_a_noop() {
        let c = cfg(vec![skill("s", "b")]);
        let d = plan_push(&c, &c, false);
        assert!(matches!(d, PushDecision::NoOp(_)), "{d:?}");
    }

    /// AC4: a removal without confirmation must refuse, so nothing is written.
    #[test]
    fn a_removal_without_confirmation_is_refused() {
        let d = plan_push(&cfg(vec![skill("s", "b")]), &cfg(vec![]), false);
        match d {
            PushDecision::NeedsConfirmation(diff) => {
                assert_eq!(diff.removed.len(), 1);
            }
            other => panic!("a removal must refuse without --yes: {other:?}"),
        }
    }

    #[test]
    fn a_removal_with_confirmation_applies() {
        let d = plan_push(&cfg(vec![skill("s", "b")]), &cfg(vec![]), true);
        assert!(matches!(d, PushDecision::Apply(_)), "{d:?}");
    }

    #[test]
    fn describe_diff_names_the_removals_and_the_current_version() {
        let d = diff(
            &cfg(vec![skill("gone", "b")]),
            &cfg(vec![skill("new", "b")]),
        );
        let text = describe_diff(&d, 7, "./config");
        assert!(text.contains("v7"), "{text}");
        assert!(text.contains("./config"), "{text}");
        assert!(text.contains("gone"), "removals must be named: {text}");
        assert!(text.contains("new"), "additions must be named: {text}");
        assert!(
            text.contains("REMOVES"),
            "the destructive fact must be loud: {text}"
        );
    }
}
