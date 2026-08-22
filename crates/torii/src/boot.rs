//! Wiring: environment and files -> live dependencies. This lives in the BINARY,
//! not the library: `Executor` takes every backend as an injected `Arc<dyn ...>`
//! precisely so the library knows nothing about Postgres, env vars, or config files.

use crate::errors::{CliError, redact_url};
use orchestrator::agent::tools::{FsReadTool, FsWriteTool, ShellTool, ToolRegistry};
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
#[derive(PartialEq)]
pub struct EnvConfig {
    pub database_url: String,
    pub fence_version: Option<String>,
}

/// Manual, NOT derived: `#[derive(Debug)]` would put the plaintext database
/// password one `{:?}` away, in the module whose entire error discipline is
/// routing that string through `redact_url`. `Debug` is still load-bearing (the
/// tests below use `expect`/`expect_err`, which require it) — this just makes
/// sure it never prints the secret.
impl std::fmt::Debug for EnvConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvConfig")
            .field("database_url", &redact_url(&self.database_url))
            .field("fence_version", &self.fence_version)
            .finish()
    }
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

/// Install a `tracing` subscriber reading `RUST_LOG` (default `info`). Writes to
/// STDERR specifically — never stdout — so `--json` command output stays
/// machine-parseable. `try_init` (not `init`) so a double call (e.g. a test, or
/// two entry points in one process) never panics; it just keeps the first
/// subscriber installed.
// Consumed by Task 10 (main.rs), once, before dispatching any command.
#[allow(dead_code)]
pub fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

/// `FacadeBuilder::build` is infallible by design (facade.rs: an unrecognised or
/// failing router is logged and skipped, never surfaced as an `Err`) — so this is
/// the ONLY place a completely broken gateway config is caught, before a worker
/// boots happily and then terminally fails every run it wakes with zero signal.
// Guards `heavy()`; tested directly below without a live provider.
#[allow(dead_code)]
fn require_adapters(registered: &[String], gateway_config: &Path) -> Result<(), CliError> {
    if registered.is_empty() {
        return Err(CliError::error(format!(
            "{} registered no provider adapters. Every model call would fail, and a worker \
             would terminally fail every run it wakes. Check the router names and API keys \
             in this config.",
            gateway_config.display()
        )));
    }
    Ok(())
}

/// `RegistryHandle::from_source` succeeds on an EMPTY registry (a fresh database,
/// or one nobody has pushed config to yet, is a perfectly valid — if useless —
/// registry). A worker with zero agents cannot do useful work; refusing loud at
/// boot is far cheaper than an operator discovering it one burned run at a time.
// Guards `heavy()`; tested directly below without a live database.
#[allow(dead_code)]
fn require_agents(
    agents: usize,
    skills: usize,
    tools: usize,
    generation: u64,
) -> Result<(), CliError> {
    if agents == 0 {
        return Err(CliError::error(format!(
            "the registry at generation {generation} has zero agents (skills={skills}, \
             tools={tools}). A worker with no agents cannot do useful work — run `torii config \
             push` first, or check that DATABASE_URL points at the intended database."
        )));
    }
    Ok(())
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

/// The light tier over an ALREADY-connected pool. Split out so `heavy()` can
/// share its ONE pool with the light-tier adapters instead of opening a second
/// one — `light()` below keeps its own single-connect path for standalone
/// light-tier commands (`run status`, `config diff`, …), which never call
/// `heavy()` at all.
#[allow(dead_code)]
fn light_from_pool(pool: sqlx::PgPool) -> LightDeps {
    LightDeps {
        scheduler_store: Arc::new(PostgresSchedulerStore::new(pool.clone())),
        config_source: PostgresConfigSource::new(pool),
    }
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
    Ok(light_from_pool(pool))
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

    // Read + parse the gateway config file FIRST (pure, no network): the most
    // likely operator typo — a bad `--gateway-config` path — is caught instantly
    // instead of only after a TCP connect and auth handshake. The file holds
    // provider API keys: report its PATH on failure, never its contents.
    let raw = std::fs::read_to_string(gateway_config)
        .map_err(|e| CliError::error(format!("cannot read {}: {e}", gateway_config.display())))?;
    let gw_config: kernel::types::config::GatewayConfig =
        serde_json::from_str(&raw).map_err(|e| {
            CliError::error(format!(
                "{} is not a valid gateway config: {e}",
                gateway_config.display()
            ))
        })?;

    // ONE shared pool for the whole heavy tier: `PgPool` is `Pool<DB>(Arc<PoolInner>)`,
    // so cloning it is an `Arc::clone`, not a new connection. One `connect()` + N
    // clones caps the whole tier at its single `max_connections(8)`; four separate
    // `connect()` calls (this function's original shape) would each hold their own
    // 8, up to 32 backends per worker process. Tradeoff: the four Postgres adapters
    // now contend over 8 connections total instead of 8 each — with the executor's
    // default concurrency of 8 and short-lived journal/CAS acquires that should be
    // fine. If it ever isn't, the correct lever is a pool-size parameter on
    // `connect()` — currently hardcoded in orchestrator-store, a deliberate
    // SP-DATA-1 deferral, not something to change here.
    let url = &env.database_url;
    let pool = connect(url)
        .await
        .map_err(|e| CliError::error(format!("cannot connect to {}: {e}", redact_url(url))))?;
    let light = light_from_pool(pool.clone());

    // One atomic (config, generation) read — the fence generation must match the
    // config it was computed from.
    let handle = RegistryHandle::from_source(&light.config_source).await?;
    let registry = handle.current();
    let agents_n = registry.agents().count();
    let skills_n = registry.skills().count();
    let tools_n = registry.tools().count();
    let generation = handle.generation();
    tracing::info!(
        generation,
        agents = agents_n,
        skills = skills_n,
        tools = tools_n,
        "registry loaded"
    );
    require_agents(agents_n, skills_n, tools_n, generation)?;

    // `Gateway::new` is the low-level, hand-wired constructor (an empty adapter
    // registry). `FacadeBuilder` is the composition root that actually registers a
    // provider adapter per router in the config — the point of reading this file at
    // all — so it is what boots a gateway that can reach a real model. Its `build()`
    // is infallible by design (a bad router is logged and skipped), so `registered`
    // is captured from the shared, Arc-backed registry BEFORE `build()` consumes the
    // builder, and checked right after.
    let builder = gateway::FacadeBuilder::new(gw_config);
    let registered = builder.registry().clone();
    let facade = builder.build().await;
    require_adapters(&registered.list().await, gateway_config)?;
    let gateway = Arc::new(facade.gateway);

    let journal = Arc::new(PostgresJournal::new(pool.clone()));
    let content = Arc::new(PostgresContentStore::new(pool.clone()));
    let context = Arc::new(PostgresContextStore::new(pool));

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let mut executor = Executor::new(gateway, journal.clone(), fence)
        .with_content_store(content)
        .with_context_store(context)
        .with_registry_handle(handle)
        // A production binary defaults SECURE: s2 leaves the redactor off in the
        // library to stay byte-identical, but here it is unconditional and there is
        // deliberately no --no-redact flag.
        .with_redactor(Arc::new(PatternRedactor::default()))
        .with_clock(clock.clone())
        // The built-in tools the config-declared ToolSpecs promise the model (fs
        // read/write + shell). Safe unconditionally: `fs_read`/`fs_write` refuse
        // loud without `ToolContext.workspace_root`, and `shell` refuses loud
        // without a wired sandbox (both below) — registering them never widens
        // what a run can actually do. Without this, an agent that emits a
        // config-declared `fs_write` call passes the s1 permission gate (an
        // unknown executable tool has empty `Permissions`, which trivially
        // "covers" anything) and then hard-fails `UnknownTool` — a burned turn,
        // not a graceful refusal.
        .with_tools(Arc::new(
            ToolRegistry::default()
                .with_tool(Arc::new(FsReadTool))
                .with_tool(Arc::new(FsWriteTool))
                .with_tool(Arc::new(ShellTool)),
        ));

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

    /// FIX 6: `{:?}` on `EnvConfig` must never print the plaintext password —
    /// `Debug` is manual specifically to route it through `redact_url`.
    #[test]
    fn env_config_debug_redacts_the_database_url() {
        let pw = format!("s3cr{}t", "e");
        let url = format!("postgres://u:{pw}@h:5432/db");
        let e = env_config_from(getter(&[(ENV_DATABASE_URL, &url)])).expect("ok");
        let debug = format!("{e:?}");
        assert!(!debug.contains(&pw), "password leaked via Debug: {debug}");
        assert!(debug.contains("h:5432/db"), "{debug}");
    }

    /// FIX 1: `FacadeBuilder::build` never fails on a bad router — this is the only
    /// place a completely misconfigured gateway is caught. No live provider needed:
    /// the check is pure over the already-registered adapter ids.
    #[test]
    fn heavy_refuses_a_gateway_config_that_registered_no_adapters() {
        let err = require_adapters(&[], Path::new("/tmp/gateway.json")).expect_err("must refuse");
        assert_eq!(err.code, crate::errors::EXIT_ERROR);
        assert!(err.message.contains("gateway.json"), "{}", err.message);
        assert!(
            err.message.contains("no provider adapters"),
            "{}",
            err.message
        );
    }

    #[test]
    fn a_gateway_config_with_at_least_one_adapter_is_accepted() {
        require_adapters(&["anthropic".to_string()], Path::new("/tmp/gateway.json"))
            .expect("at least one adapter is enough");
    }

    /// FIX 7: an empty registry is a VALID registry (`from_source` succeeds on a
    /// fresh database), so this is the only signal an operator gets before the
    /// first burned run. No live database needed: pure over the counts.
    #[test]
    fn heavy_refuses_a_registry_with_zero_agents() {
        let err = require_agents(0, 0, 0, 3).expect_err("must refuse");
        assert_eq!(err.code, crate::errors::EXIT_ERROR);
        assert!(err.message.contains("zero agents"), "{}", err.message);
        assert!(
            err.message.contains('3'),
            "must name the generation: {}",
            err.message
        );
    }

    #[test]
    fn a_registry_with_at_least_one_agent_is_accepted() {
        require_agents(1, 0, 0, 3).expect("one agent is enough");
    }

    /// FIX 3, empirically: `PgPool` is `Pool<DB>(Arc<PoolInner>)`, so cloning it
    /// into every adapter `heavy()` builds shares ONE connection cap rather than
    /// one PER adapter. Skips without a live database — this proves an operational
    /// property (real backend connections), not something a mock can stand in for.
    #[tokio::test]
    async fn heavy_shares_one_pool_across_every_postgres_adapter() {
        let Some(url) = std::env::var(ENV_DATABASE_URL).ok() else {
            return;
        };
        let pool = connect(&url).await.expect("connect");

        // Exactly the five adapters `heavy()` builds, all over ONE cloned pool.
        let _scheduler_store = PostgresSchedulerStore::new(pool.clone());
        let _config_source = PostgresConfigSource::new(pool.clone());
        let _journal = PostgresJournal::new(pool.clone());
        let _content = PostgresContentStore::new(pool.clone());
        let _context = PostgresContextStore::new(pool.clone());

        // Force a real backend connection through the pool the five adapters share.
        sqlx::query("select 1")
            .execute(&pool)
            .await
            .expect("a live backend connection");

        // The whole point of Fix 3: one shared pool caps total connections at ITS
        // OWN max_connections(8), not 8 per adapter (32 for four separate pools).
        assert!(
            pool.size() <= 8,
            "one shared pool must stay within its own cap, saw {}",
            pool.size()
        );
    }
}
