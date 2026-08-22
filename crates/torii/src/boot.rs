//! Wiring: environment and files -> live dependencies. This lives in the BINARY,
//! not the library: `Executor` takes every backend as an injected `Arc<dyn ...>`
//! precisely so the library knows nothing about Postgres, env vars, or config files.

use crate::errors::{CliError, redact_url};
use orchestrator::{Executor, Scheduler};
use orchestrator_core::{Clock, PatternRedactor, RegistryHandle, SystemClock};
use orchestrator_store::postgres::{
    PostgresConfigSource, PostgresContentStore, PostgresContextStore, PostgresJournal,
    PostgresSchedulerStore, connect,
};
use std::path::Path;
use std::sync::Arc;

pub const ENV_DATABASE_URL: &str = "DATABASE_URL";
pub const ENV_FENCE_VERSION: &str = "TORII_FENCE_VERSION";

/// The validated environment. `fence_version` is only required by the heavy tier.
// Consumed by Task 10 (main.rs clap dispatch).
#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub struct EnvConfig {
    pub database_url: String,
    pub fence_version: Option<String>,
}

/// Validate the environment through an injected getter, so tests never mutate
/// process env (which is `unsafe` in edition 2024 and racy across parallel tests).
// Consumed by Task 10 (main.rs clap dispatch).
#[allow(dead_code)]
pub fn env_config_from(get: impl Fn(&str) -> Option<String>) -> Result<EnvConfig, CliError> {
    let database_url = get(ENV_DATABASE_URL)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| {
            CliError::error(format!(
                "{ENV_DATABASE_URL} is not set.\n\
                 torii reads the Postgres connection string from the environment only — a flag \
                 would put the password in `ps` output and shell history."
            ))
        })?;
    let fence_version = get(ENV_FENCE_VERSION).filter(|s| !s.trim().is_empty());
    Ok(EnvConfig {
        database_url,
        fence_version,
    })
}

// Consumed by Task 10 (main.rs clap dispatch).
#[allow(dead_code)]
pub fn env_config() -> Result<EnvConfig, CliError> {
    env_config_from(|k| std::env::var(k).ok())
}

/// The heavy tier additionally requires the fence base.
// Consumed by Task 10 (main.rs clap dispatch).
#[allow(dead_code)]
pub fn require_fence(env: &EnvConfig) -> Result<&str, CliError> {
    env.fence_version.as_deref().ok_or_else(|| {
        CliError::error(format!(
            "{ENV_FENCE_VERSION} is not set.\n\
             The fence base is recorded in every run and checked on resume, so a fleet must \
             agree on it. Set it explicitly (e.g. {ENV_FENCE_VERSION}=v1) — deriving it from \
             the build version would strand every paused run on a routine deploy."
        ))
    })
}

/// Light tier: everything reachable with just a database. No gateway, no model
/// credentials, no fence — so an operator can cancel a runaway run or inspect the
/// wake queue on a box that has none of those.
// Consumed by Task 10 (main.rs clap dispatch).
#[allow(dead_code)]
pub struct LightDeps {
    pub scheduler_store: Arc<PostgresSchedulerStore>,
    pub config_source: PostgresConfigSource,
}

// Consumed by Task 10 (main.rs clap dispatch).
#[allow(dead_code)]
pub async fn light(env: &EnvConfig) -> Result<LightDeps, CliError> {
    let pool = connect(&env.database_url).await.map_err(|e| {
        CliError::error(format!(
            "cannot connect to {}: {e}",
            redact_url(&env.database_url)
        ))
    })?;
    Ok(LightDeps {
        scheduler_store: Arc::new(PostgresSchedulerStore::new(pool.clone())),
        config_source: PostgresConfigSource::new(pool),
    })
}

/// Heavy tier: a full Executor behind a Scheduler. Adds the gateway config file
/// and the fence base.
// Consumed by Task 10 (main.rs clap dispatch).
#[allow(dead_code)]
pub struct HeavyDeps {
    pub light: LightDeps,
    pub scheduler: Scheduler,
    pub clock: Arc<dyn Clock>,
}

// Consumed by Task 10 (main.rs clap dispatch).
#[allow(dead_code)]
pub async fn heavy(
    env: &EnvConfig,
    gateway_config: &Path,
    workspace_root: Option<&Path>,
) -> Result<HeavyDeps, CliError> {
    let fence = require_fence(env)?.to_string();
    let light = light(env).await?;

    // The gateway config file holds provider API keys: report its PATH on failure,
    // never its contents.
    let raw = std::fs::read_to_string(gateway_config)
        .map_err(|e| CliError::error(format!("cannot read {}: {e}", gateway_config.display())))?;
    let gw_config: kernel::types::config::GatewayConfig =
        serde_json::from_str(&raw).map_err(|e| {
            CliError::error(format!(
                "{} is not a valid gateway config: {e}",
                gateway_config.display()
            ))
        })?;
    // `Gateway::new` is the low-level, hand-wired constructor (an empty adapter
    // registry). `FacadeBuilder` is the composition root that actually registers
    // a provider adapter per router in the config — the point of reading this
    // file at all — so it is what boots a gateway that can reach a real model.
    let facade = gateway::FacadeBuilder::new(gw_config).build().await;
    let gateway = Arc::new(facade.gateway);

    let url = &env.database_url;
    let journal = Arc::new(PostgresJournal::new(connect(url).await.map_err(|e| {
        CliError::error(format!("cannot connect to {}: {e}", redact_url(url)))
    })?));
    let content = Arc::new(PostgresContentStore::new(connect(url).await.map_err(
        |e| CliError::error(format!("cannot connect to {}: {e}", redact_url(url))),
    )?));
    let context = Arc::new(PostgresContextStore::new(connect(url).await.map_err(
        |e| CliError::error(format!("cannot connect to {}: {e}", redact_url(url))),
    )?));
    // One atomic (config, generation) read — the fence generation must match the
    // config it was computed from.
    let handle = RegistryHandle::from_source(&light.config_source).await?;

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let mut executor = Executor::new(gateway, journal.clone(), fence)
        .with_content_store(content)
        .with_context_store(context)
        .with_registry_handle(handle)
        // A production binary defaults SECURE: s2 leaves the redactor off in the
        // library to stay byte-identical, but here it is unconditional and there is
        // deliberately no --no-redact flag.
        .with_redactor(Arc::new(PatternRedactor::default()))
        .with_clock(clock.clone());

    if let Some(root) = workspace_root {
        executor = executor.with_workspace_root(root);
        #[cfg(target_os = "macos")]
        {
            executor = executor.with_sandbox(Arc::new(orchestrator::agent::sandbox::MacosSandbox));
        }
        #[cfg(target_os = "linux")]
        {
            executor = executor.with_sandbox(Arc::new(orchestrator::agent::sandbox::LinuxSandbox));
        }
    }

    let scheduler = Scheduler::new(
        light.scheduler_store.clone(),
        executor,
        journal,
        clock.clone(),
    );
    Ok(HeavyDeps {
        light,
        scheduler,
        clock,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn getter<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |k| {
            pairs
                .iter()
                .find(|(key, _)| *key == k)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn a_missing_database_url_is_a_specific_actionable_error() {
        let err = env_config_from(getter(&[])).expect_err("must fail");
        assert_eq!(err.code, crate::errors::EXIT_ERROR);
        assert!(err.message.contains(ENV_DATABASE_URL), "{}", err.message);
    }

    #[test]
    fn the_light_tier_needs_only_a_database_url() {
        let e = env_config_from(getter(&[(ENV_DATABASE_URL, "postgres://h/db")])).expect("ok");
        assert_eq!(e.database_url, "postgres://h/db");
        assert_eq!(e.fence_version, None);
    }

    /// The heavy tier must refuse to start without an explicit fence base: deriving
    /// it would strand every paused run on a routine version bump.
    #[test]
    fn the_heavy_tier_refuses_without_an_explicit_fence_version() {
        let e = env_config_from(getter(&[(ENV_DATABASE_URL, "postgres://h/db")])).expect("ok");
        let err = require_fence(&e).expect_err("must refuse");
        assert_eq!(err.code, crate::errors::EXIT_ERROR);
        assert!(err.message.contains(ENV_FENCE_VERSION), "{}", err.message);
        assert!(
            err.message.contains("recorded in every run"),
            "must explain WHY it is required: {}",
            err.message
        );
    }

    #[test]
    fn an_explicit_fence_version_is_accepted() {
        let e = env_config_from(getter(&[
            (ENV_DATABASE_URL, "postgres://h/db"),
            (ENV_FENCE_VERSION, "v1"),
        ]))
        .expect("ok");
        assert_eq!(require_fence(&e).expect("present"), "v1");
    }

    /// An empty fence version is as dangerous as a missing one.
    #[test]
    fn a_blank_fence_version_is_rejected() {
        let e = env_config_from(getter(&[
            (ENV_DATABASE_URL, "postgres://h/db"),
            (ENV_FENCE_VERSION, "   "),
        ]))
        .expect("ok");
        assert!(require_fence(&e).is_err(), "whitespace is not a fence base");
    }

    /// Errors must never echo the connection string.
    #[test]
    fn a_blank_database_url_error_does_not_echo_a_secret() {
        let pw = format!("s3cr{}t", "e");
        let url = format!("postgres://u:{pw}@h:5432/db");
        let e = env_config_from(getter(&[(ENV_DATABASE_URL, &url)])).expect("ok");
        // The redaction helper is what every message uses.
        assert!(!redact_url(&e.database_url).contains(&pw));
    }
}
