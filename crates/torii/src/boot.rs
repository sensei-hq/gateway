//! Wiring: environment and files -> live dependencies. This is torii's ONLY
//! Postgres/env/config-file-aware module: `Executor` takes every backend as an injected
//! `Arc<dyn ...>` precisely so the orchestrator library knows nothing about any of them,
//! and every `cmd` here takes its dependencies as arguments so none of them needs this
//! module either. Concentrating the wiring in one place is what keeps the commands
//! unit-testable against in-memory doubles.

use crate::errors::{CliError, redact_url};
use orchestrator::agent::tools::{FsReadTool, FsWriteTool, ShellTool, ToolRegistry};
use orchestrator::{Executor, Scheduler};
use orchestrator_core::{Clock, PatternRedactor, RegistryHandle, SystemClock};
use orchestrator_store::postgres::{
    PostgresConfigSource, PostgresContentStore, PostgresContextStore, PostgresJournal,
    PostgresSchedulerStore, connect_with_max,
};
use std::path::Path;
use std::sync::Arc;

pub const ENV_DATABASE_URL: &str = "DATABASE_URL";
pub const ENV_FENCE_VERSION: &str = "TORII_FENCE_VERSION";
pub const ENV_POOL_SIZE: &str = "TORII_POOL_SIZE";

/// [`connect_with_max`]'s own default, restated here as the fallback when
/// `TORII_POOL_SIZE` is unset — see that function's doc comment for why 8.
const DEFAULT_POOL_SIZE: u32 = 8;

/// A sanity ceiling on `TORII_POOL_SIZE`, not an operational policy. Postgres's own
/// out-of-the-box `max_connections` is 100 (roughly 97 usable once superuser/replication
/// reservations are subtracted — see `boot::heavy`'s pool-sharing comment); no single
/// worker process legitimately needs a pool anywhere near that on its own, let alone
/// past it. This exists purely to catch a fat-fingered value (an extra digit, a copy-paste
/// of the wrong env var) at boot instead of at a confusing connection-limit error deep in
/// a run. It is deliberately generous — high enough that it never second-guesses a real
/// operator's tuning of a fleet against a beefier Postgres — so it rejects that class of
/// certain-mistake without trying to enforce a capacity policy this code cannot know.
const MAX_POOL_SIZE: u32 = 1000;

/// The validated environment. `fence_version` is only required by the heavy tier.
#[derive(PartialEq)]
pub struct EnvConfig {
    pub database_url: String,
    pub fence_version: Option<String>,
    pub pool_size: u32,
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
            .field("pool_size", &self.pool_size)
            .finish()
    }
}

/// Parse `TORII_POOL_SIZE`, mirroring `cmd::worker::parse_interval`'s discipline: reject
/// anything that isn't a plain positive integer, loudly and with the offending value
/// echoed back. A zero-size pool cannot serve any connection, and anything past
/// [`MAX_POOL_SIZE`] is treated as a typo rather than an intentional value — see its doc
/// comment.
fn parse_pool_size(s: &str) -> Result<u32, String> {
    let s = s.trim();
    let v: u32 = s
        .parse()
        .map_err(|_| format!("invalid {ENV_POOL_SIZE} {s:?}: {s:?} is not a whole number"))?;
    if v == 0 {
        return Err(format!(
            "invalid {ENV_POOL_SIZE} {s:?}: a zero-size pool cannot serve any connection"
        ));
    }
    if v > MAX_POOL_SIZE {
        return Err(format!(
            "invalid {ENV_POOL_SIZE} {s:?}: exceeds the sanity ceiling of {MAX_POOL_SIZE} \
             (almost certainly a typo) — Postgres's own default max_connections is 100, so a \
             single worker process asking for a pool anywhere near {MAX_POOL_SIZE} is not a \
             realistic tuning value"
        ));
    }
    Ok(v)
}

/// Validate the environment through an injected getter, so tests never mutate
/// process env (which is `unsafe` in edition 2024 and racy across parallel tests).
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
    let pool_size = match get(ENV_POOL_SIZE).filter(|s| !s.trim().is_empty()) {
        Some(raw) => parse_pool_size(&raw).map_err(CliError::error)?,
        None => DEFAULT_POOL_SIZE,
    };
    Ok(EnvConfig {
        database_url,
        fence_version,
        pool_size,
    })
}

pub fn env_config() -> Result<EnvConfig, CliError> {
    env_config_from(|k| std::env::var(k).ok())
}

/// What replaces credential material scrubbed out of text torii did not compose.
const REDACTED: &str = "<redacted>";

/// The credential material in a connection string, so it can be scrubbed out of an
/// error message torii did NOT compose (see [`connect_failure`]).
///
/// The username is deliberately NOT a needle. It is not a secret, `redact_url` drops it
/// from the part torii composes anyway, and scrubbing it would mangle every legitimate
/// error for the overwhelmingly common `postgres://postgres@host/postgres` — where the
/// scheme, the user and the database name are all the same token.
fn credential_needles(url: &str) -> Vec<&str> {
    let mut out = vec![url];
    // No `://` means a scheme-less URL, which is exactly the shape that leaks: `Url::parse`
    // reads `operator:s3cret@host:5433/db` as scheme `operator` + an opaque path, so sqlx
    // takes the whole password-onward tail as the DATABASE NAME and the server echoes it
    // back in `database "…" does not exist`. Treating the whole string as the post-scheme
    // remainder is what finds the userinfo in that case.
    let after_scheme = url.split_once("://").map_or(url, |(_scheme, rest)| rest);
    if let Some((userinfo, _host_and_path)) = after_scheme.rsplit_once('@')
        && let Some((_user, password)) = userinfo.split_once(':')
    {
        out.push(userinfo);
        out.push(password);
    }
    out
}

/// Replace every occurrence of `url`'s credential material in `text`.
///
/// Deliberately fail-closed: a one-character password is scrubbed too, which can mangle an
/// unrelated word in the message. An over-scrubbed error message is a cosmetic annoyance;
/// an under-scrubbed one is a credential in journald and CI logs. (Known limit: a
/// percent-encoded password that some layer decodes before printing would not match. No
/// shipped sqlx error does that — it echoes the string it was given.)
fn scrub_credentials(text: &str, url: &str) -> String {
    let mut needles = credential_needles(url);
    // Longest first: replacing a nested needle (the password) before the string that
    // contains it (the whole URL) would leave the outer match broken and its remaining
    // fragments in place.
    needles.sort_unstable_by_key(|n| std::cmp::Reverse(n.len()));
    let mut out = text.to_string();
    for n in needles {
        // `str::replace` with an empty pattern splices the replacement between every
        // character — a URL with no password must not shred the message.
        if !n.is_empty() {
            out = out.replace(n, REDACTED);
        }
    }
    out
}

/// Compose a connect failure without echoing the connection string.
///
/// `redact_url` covers the half torii interpolates itself, but it CANNOT cover the sqlx
/// error's own text, which carries the raw input: for a scheme-less `DATABASE_URL` (an
/// ordinary secret-store mistake) `redact_url` correctly returns its placeholder and the
/// adjacent `{e}` then prints `database "s3cret@127.0.0.1:5433/postgres" does not exist`,
/// defeating it entirely. Both connect sites go through here so neither can regress.
fn connect_failure(database_url: &str, err: &str) -> String {
    format!(
        "cannot connect to {}: {}",
        redact_url(database_url),
        scrub_credentials(err, database_url)
    )
}

/// A gateway-config parse failure reports the serde error's LOCATION ONLY — never its
/// `Display`, which echoes the offending VALUE (`invalid type: string "sk-live-…",
/// expected struct RouterConfig`). This file is the one that holds provider API keys, and
/// the single most likely first-run typo is pasting a key where a struct belongs — which
/// would put a live credential in a worker's stderr and thus in journald/CI logs.
/// `RouterConfig` already has a redacting `Debug` for exactly this reason; the serde error
/// is the hole that bypasses it. Line/column/category is enough to find the problem.
fn gateway_config_parse_error(path: &Path, e: &serde_json::Error) -> CliError {
    CliError::error(format!(
        "{} is not a valid gateway config: {:?} error at line {} column {}. \
         The offending value is deliberately not echoed — this file holds provider API keys.",
        path.display(),
        e.classify(),
        e.line(),
        e.column()
    ))
}

/// The heavy tier additionally requires the fence base.
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
///
/// The message names the routers that WERE present but produced no adapter (not
/// just "check your config"): a generic "check the router names and API keys"
/// misdirects an operator whose config is otherwise correct but names an
/// unsupported/skipped router (e.g. `bedrock`, which `register_cloud_from_config`
/// deliberately never registers from config alone) — they would re-verify valid
/// values and stay stuck. Naming the actual routers makes any future skipped-router
/// case self-diagnosing instead of needing its own bespoke message.
// Guards `heavy()`; tested directly below without a live provider.
fn require_adapters(
    registered: &[String],
    configured_routers: &[String],
    gateway_config: &Path,
) -> Result<(), CliError> {
    if registered.is_empty() {
        let detail = if configured_routers.is_empty() {
            "it has no routers configured at all".to_string()
        } else {
            format!(
                "it configured {} but none produced a working adapter — an unsupported or \
                 not-yet-wired router (e.g. `bedrock`, which requires explicit AWS SDK setup \
                 and is never registered from config alone) is silently skipped, not reported \
                 as an error",
                configured_routers.join(", ")
            )
        };
        return Err(CliError::error(format!(
            "{} registered no provider adapters: {detail}. Every model call would fail, and a \
             worker would terminally fail every run it wakes.",
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
///
/// SP-DATA-5 Task 5 adds `journal`: a run's token spend lives in the JOURNAL
/// (`EffectRecorded.usage`), not the `scheduled_runs` row, so both `torii run
/// status` (fold spent/budget for display) and `torii run wake --budget-tokens`
/// (append `BudgetRaised` before waking) need one. Adding it here costs nothing —
/// it is just another adapter over the SAME pool this tier already opens
/// (`light_from_pool` clones it) — and keeping it on the light tier matters: an
/// operator must be able to inspect AND raise a run's budget on a box with no
/// model credentials.
pub struct LightDeps {
    pub scheduler_store: Arc<PostgresSchedulerStore>,
    pub journal: Arc<PostgresJournal>,
    pub config_source: PostgresConfigSource,
}

/// The light tier over an ALREADY-connected pool. Split out so `heavy()` can
/// share its ONE pool with the light-tier adapters instead of opening a second
/// one — `light()` below keeps its own single-connect path for standalone
/// light-tier commands (`run status`, `config diff`, …), which never call
/// `heavy()` at all.
fn light_from_pool(pool: sqlx::PgPool) -> LightDeps {
    LightDeps {
        scheduler_store: Arc::new(PostgresSchedulerStore::new(pool.clone())),
        journal: Arc::new(PostgresJournal::new(pool.clone())),
        config_source: PostgresConfigSource::new(pool),
    }
}

pub async fn light(env: &EnvConfig) -> Result<LightDeps, CliError> {
    let pool = connect_with_max(&env.database_url, env.pool_size)
        .await
        .map_err(|e| CliError::error(connect_failure(&env.database_url, &e.to_string())))?;
    Ok(light_from_pool(pool))
}

/// Heavy tier: a full Executor behind a Scheduler. Adds the gateway config file
/// and the fence base.
pub struct HeavyDeps {
    // Not read by the current dispatch (only `.scheduler` is): kept for a future
    // heavy-tier command that needs the shared pool's config source directly, or a
    // test that wants to fast-forward the injected clock.
    #[allow(dead_code)]
    pub light: LightDeps,
    pub scheduler: Scheduler,
    #[allow(dead_code)]
    pub clock: Arc<dyn Clock>,
}

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
        serde_json::from_str(&raw).map_err(|e| gateway_config_parse_error(gateway_config, &e))?;

    // ONE shared pool for the whole heavy tier: `PgPool` is `Pool<DB>(Arc<PoolInner>)`,
    // so cloning it is an `Arc::clone`, not a new connection. One `connect_with_max()`
    // + N clones caps the whole tier at its single `max_connections(env.pool_size)`;
    // four separate `connect()` calls (this function's original shape) would each
    // hold their own, up to 4x as many backends per worker process. Tradeoff: the
    // four Postgres adapters contend over `env.pool_size` connections total instead
    // of that many each — with the executor's default concurrency of 8 and
    // short-lived journal/CAS acquires, the default of 8 should be fine for most
    // workers. If it ever isn't, the lever is `TORII_POOL_SIZE` (see
    // `env_config_from` / `orchestrator_store::postgres::connect`'s doc comment for
    // why 8 was the original default and what a shared-pool worker should weigh).
    let url = &env.database_url;
    let pool = connect_with_max(url, env.pool_size)
        .await
        .map_err(|e| CliError::error(connect_failure(url, &e.to_string())))?;
    let light = light_from_pool(pool.clone());

    // One atomic (config, generation) read — the fence generation must match the
    // config it was computed from.
    let handle = RegistryHandle::from_source(&light.config_source).await?;
    // `snapshot()`, not `.current()` + `.generation()` as two separate lock
    // acquisitions: those release the lock in between, which is exactly the torn
    // -read shape SP-DATA-2 eliminated. Not reachable today (boot is sequential
    // and nothing calls `reload()`), but it must not quietly plant one for
    // whoever wires the deferred reload trigger.
    let (registry, generation) = handle.snapshot();
    let agents_n = registry.agents().count();
    let skills_n = registry.skills().count();
    let tools_n = registry.tools().count();
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
    let configured_routers: Vec<String> = gw_config.routers.keys().cloned().collect();
    let builder = gateway::FacadeBuilder::new(gw_config);
    let registered = builder.registry().clone();
    let facade = builder.build().await;
    require_adapters(
        &registered.list().await,
        &configured_routers,
        gateway_config,
    )?;
    let gateway = Arc::new(facade.gateway);

    // SP-DATA-5 Task 5: reuse `light.journal` rather than opening a second
    // `PostgresJournal` over another pool clone — `light_from_pool` already built
    // one over this exact pool, and the Scheduler needs the SAME journal the
    // Executor writes to (so `tick`'s pause-deadline read sees what `run` wrote).
    let content = Arc::new(PostgresContentStore::new(pool.clone()));
    let context = Arc::new(PostgresContextStore::new(pool));

    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let mut executor = Executor::new(gateway, light.journal.clone(), fence)
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
        light.journal.clone(),
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
    // Only this probe test connects unconditionally (production code goes through
    // `connect_with_max` so `env.pool_size` is honored) — imported here, not at module
    // scope, so a non-test build of this lib (linked into `main.rs`) doesn't carry an
    // unused import.
    use orchestrator_store::postgres::connect;

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

    // ---- SP-DATA-4.1 Task 5: TORII_POOL_SIZE -----------------------------------------------

    /// Absent `TORII_POOL_SIZE` must fall back to `connect()`'s own default (8), not some
    /// independently-chosen torii constant — the two must never drift apart silently.
    #[test]
    fn an_absent_pool_size_defaults_to_eight() {
        let e = env_config_from(getter(&[(ENV_DATABASE_URL, "postgres://h/db")])).expect("ok");
        assert_eq!(e.pool_size, 8);
    }

    #[test]
    fn a_valid_pool_size_is_accepted() {
        let e = env_config_from(getter(&[
            (ENV_DATABASE_URL, "postgres://h/db"),
            (ENV_POOL_SIZE, "16"),
        ]))
        .expect("ok");
        assert_eq!(e.pool_size, 16);
    }

    /// A zero-size pool cannot serve any connection — reject it loudly rather than let
    /// `PgPoolOptions` fail obscurely (or silently behave as "unlimited", which it does
    /// not, but a reader should not have to know that to trust this value).
    #[test]
    fn a_zero_pool_size_is_rejected() {
        let err = env_config_from(getter(&[
            (ENV_DATABASE_URL, "postgres://h/db"),
            (ENV_POOL_SIZE, "0"),
        ]))
        .expect_err("must refuse");
        assert_eq!(err.code, crate::errors::EXIT_ERROR);
        assert!(err.message.contains(ENV_POOL_SIZE), "{}", err.message);
    }

    #[test]
    fn an_unparseable_pool_size_is_rejected() {
        let err = env_config_from(getter(&[
            (ENV_DATABASE_URL, "postgres://h/db"),
            (ENV_POOL_SIZE, "abc"),
        ]))
        .expect_err("must refuse");
        assert_eq!(err.code, crate::errors::EXIT_ERROR);
        assert!(err.message.contains(ENV_POOL_SIZE), "{}", err.message);
        assert!(err.message.contains("abc"), "{}", err.message);
    }

    /// An absurdly large value is almost certainly a typo (an extra digit), not a real
    /// tuning decision — see `MAX_POOL_SIZE`'s doc comment for the reasoning.
    #[test]
    fn an_absurdly_large_pool_size_is_rejected() {
        let err = env_config_from(getter(&[
            (ENV_DATABASE_URL, "postgres://h/db"),
            (ENV_POOL_SIZE, "5000000"),
        ]))
        .expect_err("must refuse");
        assert_eq!(err.code, crate::errors::EXIT_ERROR);
        assert!(err.message.contains(ENV_POOL_SIZE), "{}", err.message);
    }

    /// A blank value (whitespace only) is treated the same as absent — parity with
    /// `ENV_FENCE_VERSION`'s handling.
    #[test]
    fn a_blank_pool_size_falls_back_to_the_default() {
        let e = env_config_from(getter(&[
            (ENV_DATABASE_URL, "postgres://h/db"),
            (ENV_POOL_SIZE, "   "),
        ]))
        .expect("ok");
        assert_eq!(e.pool_size, 8);
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

    /// WHOLE-SLICE FIX 2: `redact_url` handles the URL torii interpolates itself, and the
    /// AC10 test above proves that half — but only for the out-of-range-port shape, where
    /// sqlx happens not to echo its input. A SCHEME-LESS `DATABASE_URL` (an ordinary
    /// secret-store mistake: the scheme dropped somewhere in the pipeline) is the shape
    /// that actually leaked, reproduced 3/3 against the real binary:
    ///
    /// ```text
    /// torii: cannot connect to <unparseable database url>: error returned from database:
    ///        database "s3cr3t-XyZ@127.0.0.1:5433/postgres" does not exist
    /// ```
    ///
    /// `redact_url` did its job and the adjacent `{e}` defeated it. The error text is
    /// verbatim from that reproduction (a live server round trip, so it cannot be
    /// exercised as a fast unit test) — `connect_failure` is pure precisely so the
    /// composition can be proven without one.
    #[test]
    fn a_connect_failure_scrubs_the_password_out_of_the_sqlx_error_text() {
        let pw = format!("s3cr3t-{}", "XyZ");
        let url = format!("operator:{pw}@127.0.0.1:5433/postgres");
        let sqlx_err = format!(
            "error returned from database: database \"{pw}@127.0.0.1:5433/postgres\" does not exist"
        );
        let msg = connect_failure(&url, &sqlx_err);
        assert!(
            !msg.contains(&pw),
            "password leaked via the error text: {msg}"
        );
        assert!(
            msg.contains("does not exist"),
            "the diagnosis must survive scrubbing: {msg}"
        );
    }

    /// The same guard for the shape where sqlx echoes the WHOLE connection string.
    #[test]
    fn a_connect_failure_scrubs_a_whole_echoed_connection_string() {
        let pw = format!("s3cr{}t", "e");
        let url = format!("postgres://operator:{pw}@db.internal:5432/orch");
        let msg = connect_failure(&url, &format!("invalid connection string: {url}"));
        assert!(!msg.contains(&pw), "password leaked: {msg}");
        assert!(
            msg.contains("db.internal:5432/orch"),
            "the redacted host/db must still be reported: {msg}"
        );
    }

    /// The scrub must not fire when there is nothing to scrub: a passwordless URL whose
    /// user, scheme and database name are the same token (`postgres`) is the common case,
    /// and mangling it would make every legitimate connect error unreadable.
    #[test]
    fn a_connect_failure_leaves_a_passwordless_url_error_intact() {
        let msg = connect_failure(
            "postgres://postgres@localhost:5433/postgres",
            "error returned from database: database \"postgres\" does not exist",
        );
        assert!(
            msg.contains("database \"postgres\" does not exist"),
            "a passwordless URL has no credential to scrub: {msg}"
        );
    }

    /// WHOLE-SLICE FIX 3: serde_json's `Display` echoes the offending VALUE, so a key
    /// pasted as a router value instead of under `api_key` would land a live credential in
    /// a worker's stderr. Drives the REAL serde error (not a fabricated string) so the
    /// assertion is about what serde actually produces.
    #[test]
    fn a_bad_gateway_config_reports_the_location_not_the_offending_value() {
        let key = format!("sk-live-{}", "AbC1234567890");
        let raw = format!("{{\"routers\": {{\"openai\": \"{key}\"}}}}");
        let e = serde_json::from_str::<kernel::types::config::GatewayConfig>(&raw)
            .expect_err("a string where a struct belongs must not parse");
        // The hazard, stated as a fact about serde rather than an assumption.
        assert!(
            e.to_string().contains(&key),
            "precondition: serde's Display is what echoes the key: {e}"
        );
        let err = gateway_config_parse_error(Path::new("/tmp/gw-bad.json"), &e);
        assert!(
            !err.message.contains(&key),
            "the API key must never reach stderr: {}",
            err.message
        );
        assert!(err.message.contains("gw-bad.json"), "{}", err.message);
        assert!(
            err.message.contains("line 1") && err.message.contains("column"),
            "the operator still needs the location: {}",
            err.message
        );
    }

    /// FIX 1: `FacadeBuilder::build` never fails on a bad router — this is the only
    /// place a completely misconfigured gateway is caught. No live provider needed:
    /// the check is pure over the already-registered adapter ids.
    #[test]
    fn heavy_refuses_a_gateway_config_that_registered_no_adapters() {
        let err =
            require_adapters(&[], &[], Path::new("/tmp/gateway.json")).expect_err("must refuse");
        assert_eq!(err.code, crate::errors::EXIT_ERROR);
        assert!(err.message.contains("gateway.json"), "{}", err.message);
        assert!(
            err.message.contains("no provider adapters"),
            "{}",
            err.message
        );
        assert!(
            err.message.contains("no routers configured"),
            "an EMPTY config must say so, not misdirect toward router names/keys: {}",
            err.message
        );
    }

    /// Minor 3 (re-review of `6c71703`): a config that named a router which produced
    /// no adapter (e.g. `bedrock`, deliberately skipped — see `facade.rs`) must be
    /// told WHICH router, not just "check the router names and API keys" — the
    /// operator's names and keys may already be correct for a case torii can't wire.
    #[test]
    fn heavy_names_the_configured_routers_that_produced_no_adapter() {
        let err = require_adapters(
            &[],
            &["bedrock".to_string()],
            Path::new("/tmp/gateway.json"),
        )
        .expect_err("must refuse");
        assert!(
            err.message.contains("bedrock"),
            "must name the skipped router: {}",
            err.message
        );
    }

    #[test]
    fn a_gateway_config_with_at_least_one_adapter_is_accepted() {
        require_adapters(
            &["anthropic".to_string()],
            &["anthropic".to_string()],
            Path::new("/tmp/gateway.json"),
        )
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

    /// Minor 1 (re-review of `f7e6eb8`): the test this replaces
    /// (`heavy_shares_one_pool_across_every_postgres_adapter`) asserted
    /// `pool.size() <= 8` on a pool it built itself and never called `heavy()` —
    /// true by construction for ANY `connect()` result, so it could not fail even
    /// against the pre-fix four-separate-`connect()` shape (mutation-proven by the
    /// reviewer). This version drives the REAL `heavy()` and counts REAL backend
    /// connections in `pg_stat_activity`: the four-pool shape shows ~4 (each
    /// `connect()` eagerly opens a backend), this one ~1. Discrimination was
    /// verified by hand: temporarily reverting `heavy()`'s pool sharing to four
    /// separate `connect()` calls made this exact test fail with a reported 4;
    /// restoring the fix made it pass with 1.
    ///
    /// It counts backends carrying a UNIQUE `application_name` this call put in
    /// `heavy()`'s connection URL, NOT a before/after delta of every backend on the
    /// database. The delta form measured a global: any concurrent DB test in this
    /// binary opening its own pool between the two reads was charged to `heavy()`,
    /// which made it fail 5 runs out of 6 under default threads —
    ///
    /// ```text
    /// saw a delta of 3 (before=5, after=8)
    /// ```
    ///
    /// — with nothing wrong. `config_guard` cannot fix that: the noise is every
    /// OTHER DB test, not a config writer. Naming the connections is strictly more
    /// discriminating than counting them, since it can no longer credit `heavy()`
    /// with a stranger's pool NOR excuse one of its own.
    ///
    /// `before` is asserted to be 0 (the tag is unique to this call) and `after` to
    /// be at least 1 — that lower bound is what proves sqlx actually honoured the
    /// `application_name` parameter, so a silently-ignored tag fails loudly here
    /// rather than making the upper bound vacuously true.
    ///
    /// `config_agents`/`config_versions` are process-wide shared tables and
    /// `store_and_bump` is replace-all (see its own doc comment: concurrent
    /// writers serialize and last-writer-wins, which is clean, not corruption) —
    /// so a concurrent `cmd::config` test's write can legitimately race this
    /// seed away between the write and `heavy()`'s read. `config_guard` now
    /// serializes every durable-config writer in this crate, which closes that
    /// race at the source; the retry below is kept as the backstop for any
    /// future writer that forgets the guard, and any OTHER failure is a real bug
    /// and is not retried.
    #[tokio::test]
    async fn heavy_boots_on_one_pools_worth_of_real_backend_connections() {
        let Some(url) = crate::test_guard::db_url() else {
            return;
        };
        let _guard = crate::test_guard::config_guard().await;

        let probe_pool = connect(&url).await.expect("connect");
        let config_source = PostgresConfigSource::new(probe_pool.clone());
        let agent = orchestrator_core::AgentDefinition {
            name: "torii-boot-probe-agent".to_string(),
            area: "test".to_string(),
            kind: "test".to_string(),
            // An explicit override so this doesn't also need a chain-binding row.
            chain: Some("torii-boot-probe-chain".to_string()),
            chains: Default::default(),
            grants: Default::default(),
            tools: vec![],
            skills: vec![],
            system_prompt: "probe".to_string(),
            backed_by: orchestrator_core::AgentBacking::Model,
        };
        let seed = orchestrator_core::RegistryConfig {
            agents: vec![agent],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![],
        };

        let gw_dir = std::env::temp_dir().join(format!("torii-boot-gw-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&gw_dir).expect("tmp dir");
        let gw_path = gw_dir.join("gateway.json");
        // `ollama` registers WITHOUT credentials (the key resolves lazily per
        // request, confirmed by the review) — exactly what lets this test drive
        // a real `heavy()` boot with no live provider.
        std::fs::write(
            &gw_path,
            r#"{"routers":{"ollama":{"url":"http://127.0.0.1:11434"}}}"#,
        )
        .expect("write gateway config");

        // The tag that makes the count attributable. Unique per call, and carried
        // ONLY by the pool `heavy()` opens from this URL — the probe pool above
        // connects to the bare `url` and so is never counted.
        let tag = format!("torii-boot-probe-{}", uuid::Uuid::new_v4());
        let sep = if url.contains('?') { '&' } else { '?' };
        let env = EnvConfig {
            database_url: format!("{url}{sep}application_name={tag}"),
            fence_version: Some("torii-boot-probe-fence".to_string()),
            pool_size: DEFAULT_POOL_SIZE,
        };

        async fn backend_count(pool: &sqlx::PgPool, tag: &str) -> i64 {
            let (n,): (i64,) = sqlx::query_as(
                "select count(*) from pg_stat_activity
                 where datname = current_database() and application_name = $1",
            )
            .bind(tag)
            .fetch_one(pool)
            .await
            .expect("count backends");
            n
        }

        let mut outcome = None;
        for _ in 0..5 {
            config_source
                .store_and_bump(&seed)
                .await
                .expect("seed the probe agent");
            let before = backend_count(&probe_pool, &tag).await;
            match heavy(&env, &gw_path, None).await {
                Ok(deps) => {
                    let after = backend_count(&probe_pool, &tag).await;
                    outcome = Some((deps, before, after));
                    break;
                }
                Err(e) if e.message.contains("zero agents") => continue,
                Err(e) => panic!("heavy() failed for a reason other than the seed race: {e:?}"),
            }
        }
        let _ = std::fs::remove_dir_all(&gw_dir);
        let (deps, before, after) =
            outcome.expect("heavy() never won the probe-agent seed race after 5 attempts");
        assert_eq!(
            before, 0,
            "the probe tag is unique to this call, so nothing may carry it before \
             heavy() connects (saw {before})",
        );
        assert!(
            after >= 1,
            "no backend carried the probe tag — sqlx did not honour the \
             `application_name` URL parameter, so this test is measuring nothing",
        );
        assert!(
            after <= 2,
            "heavy() must share ONE pool (~1 backend connection), saw {after} \
             carrying the probe tag — a regression to a separate connect() per \
             adapter would show ~4",
        );
        drop(deps);
    }
}
