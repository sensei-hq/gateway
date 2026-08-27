//! `torii` — the operator control plane for the sensei orchestrator.
//!
//! This binary is deliberately thin: the clap surface plus `dispatch`. Every
//! command implementation lives in the `torii` LIBRARY (`src/lib.rs`) so an
//! integration test can call the real thing — see that module's docs.

use clap::{Parser, Subcommand};
use orchestrator_core::{Graph, RunId};
use std::path::PathBuf;
use torii::cmd::Outcome;
use torii::errors::CliError;
use torii::{boot, cmd};

#[derive(Parser)]
#[command(
    name = "torii",
    about = "Operator control plane for the sensei orchestrator",
    long_about = "Observe and intervene on runs, drive due wakes, and manage durable config.\n\n\
                  DATABASE_URL must be set (env only — a flag would leak the password into `ps`).\n\
                  `run submit` and `worker serve` additionally need TORII_FENCE_VERSION and \
                  --gateway-config.\n\n\
                  Exit codes: 0 ok, 1 error (including a submitted run that actually executed \
                  and failed), 2 not-found, precondition-not-met, or a result that is complete \
                  enough to print but not the unqualified success you asked for (`run \
                  list-paused` with a run whose journal could not be folded). Exit 1 puts a \
                  message on stderr and nothing on stdout; exit 2 always still prints its \
                  result. Note exit 2 is also clap's own usage-error code (a missing \
                  subcommand, an unknown flag), so it is not unique to a business-logic \
                  outcome — a script keying off it should also check stderr."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Observe and intervene on runs
    Run {
        #[command(subcommand)]
        action: RunAction,
    },
    /// Drive due wakes
    Worker {
        #[command(subcommand)]
        action: WorkerAction,
    },
    /// Manage the durable registry config
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
}

#[derive(Subcommand)]
enum RunAction {
    /// Submit a graph and drive it (blocks until it pauses or finishes)
    Submit {
        #[arg(long)]
        graph: PathBuf,
        #[arg(long)]
        run_id: Option<String>,
        #[arg(long)]
        gateway_config: PathBuf,
        #[arg(long)]
        workspace_root: Option<PathBuf>,
        /// Stop this run once it has spent this many tokens, and pause it durably so
        /// you can raise the cap with `run wake --budget-tokens N`.
        ///
        /// This is a floor trigger, not a hard ceiling: a call's output tokens are
        /// unknowable until it returns, so the run can exceed the number by at most
        /// one model call. Omit it and the run is unbudgeted, exactly as before.
        /// A budgeted run runs its model calls one at a time, so a wide fan-out is
        /// slower than an unbudgeted one. `0` is not a valid budget.
        #[arg(long, value_parser = cmd::run::parse_budget_tokens)]
        budget_tokens: Option<u64>,
    },
    /// Show one run's schedule record
    Status {
        run_id: String,
        #[arg(long)]
        json: bool,
    },
    /// List every run awaiting a wake, and any node awaiting a signal
    ///
    /// One journal is folded per PAUSED run, to name the nodes awaiting a signal. A run
    /// whose journal cannot be folded — a durable `format_version` bumped by a newer
    /// binary is the realistic case — does not hide the rest of the fleet: its awaiting
    /// column reads `unknown: <error>` (`awaiting_error` under --json) and every other run
    /// is still listed, at exit 2.
    ListPaused {
        #[arg(long)]
        json: bool,
    },
    /// Deliver a decision to a node that is waiting for one (`AwaitSignal`)
    ///
    /// This is human-IN-the-loop: `wake` resumes a run, this ANSWERS it. The payload is
    /// journaled durably, folded as the node's output, and flows into downstream nodes
    /// and model prompts.
    ///
    /// A SIGNAL IS NOT A CREDENTIAL CHANNEL — the credential broker is. Do not paste an
    /// API key, token or password here: it would land in durable storage and in a model
    /// prompt. Secret-shaped text is redacted before it is journaled, but that is a
    /// best-effort scrub by shape, not a safe place to put a secret.
    ///
    /// Reports the effect it achieved, read back after the write and ordered by journal
    /// position: `signalled` when the answer is durable and either the run is queued for
    /// the next tick or a drive already in flight read it and completed the node;
    /// `not delivered` (exit 2) when nothing was written; and `not read` (exit 2) when the
    /// answer IS durable but the node had already terminated — never "not delivered",
    /// which would send you looking for a write that already happened.
    #[command(group(
        clap::ArgGroup::new("payload_src").required(true).multiple(false)
    ))]
    Signal {
        run_id: String,
        /// The awaiting node's id — `torii run list-paused` names it.
        #[arg(long)]
        node: String,
        /// The decision, as JSON, e.g. '{"decision":"approved"}'. Max 4096 bytes as
        /// stored — redaction replaces secret-shaped text with the longer literal
        /// `[REDACTED]` and the journal's `jsonb` column expands numbers, so a payload can
        /// cross the limit on the way to the journal.
        ///
        /// This is argv: it is visible to `ps`, to your shell history and to a CI job's
        /// command echo, none of which redaction can reach. For anything you would rather
        /// not have on a command line, use --payload-file.
        //
        // Taken as a raw `String` and parsed in `dispatch`, NOT through a clap
        // `value_parser` like every other flag in this file. clap wraps a value_parser
        // failure as `error: invalid value '<THE VALUE>' for '--payload …': <message>` —
        // it echoes the offending value itself, which no per-arg setting suppresses. That
        // defeats `parse_payload`'s deliberate non-echo entirely, and this is the one flag
        // an operator might paste a credential into (typing a token bare is not valid
        // JSON, so the parse failure is exactly the path that would print it). Parsing in
        // `dispatch` keeps the whole message torii's to compose. It still happens before
        // any connection — the property that matters — exactly as `parse_run_id` does.
        #[arg(long, group = "payload_src")]
        payload: Option<String>,
        /// Read the decision from a file instead of the command line.
        ///
        /// The same JSON, and the same cap — but the value never becomes an argv entry, so
        /// it cannot be read out of `ps`, `/proc/<pid>/cmdline`, a shell history file or a
        /// CI job's command echo. `DATABASE_URL` is environment-only for this same reason.
        /// A signal is still NOT a credential channel; this only closes the sink that
        /// redaction cannot.
        #[arg(long, group = "payload_src", value_name = "PATH")]
        payload_file: Option<std::path::PathBuf>,
    },
    /// Cancel a non-terminal run so it is never woken
    Cancel { run_id: String },
    /// Queue a paused run for the next worker tick
    Wake {
        run_id: String,
        /// Raise (or lower) the run's token cap before waking it — the way to restart
        /// a run that stopped at its budget. Lowering it below what the run has
        /// already spent is legitimate, and halts the run at its next model call.
        //
        // Recorded as `BudgetRaised` BEFORE the wake is queued; see `cmd::run::wake`'s
        // doc comment for why that order is load-bearing.
        #[arg(long, value_parser = cmd::run::parse_budget_tokens)]
        budget_tokens: Option<u64>,
    },
    /// Delete terminal run records (completed/failed/cancelled) older than a window
    ///
    /// Paused and waking runs are NEVER eligible, at any age.
    Prune {
        /// Retention window, e.g. 30d, 12h, 90m. No default: the policy is the
        /// operator's to state, not this command's to assume.
        #[arg(long, value_parser = cmd::run::parse_retention)]
        older_than: chrono::Duration,
        /// Delete without confirmation
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum WorkerAction {
    /// Poll for due wakes and drive them
    Serve {
        #[arg(long, default_value = "5s", value_parser = cmd::worker::parse_interval)]
        interval: std::time::Duration,
        /// Run exactly one tick and exit (cron-friendly)
        #[arg(long)]
        once: bool,
        #[arg(long)]
        gateway_config: PathBuf,
        #[arg(long)]
        workspace_root: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Show the durable config generation
    Version {
        #[arg(long)]
        json: bool,
    },
    /// Replace the durable config from a directory and advance the generation
    Push {
        dir: PathBuf,
        /// Apply without confirmation even when entities are removed
        #[arg(long)]
        yes: bool,
    },
}

fn parse_run_id(s: &str) -> Result<RunId, CliError> {
    uuid::Uuid::parse_str(s)
        .map(RunId)
        .map_err(|e| CliError::error(format!("invalid run id {s:?}: {e}")))
}

/// Wait for either SIGINT or, on unix, SIGTERM — the graceful-shutdown signal every
/// `docker stop` and rolling deploy sends. Without racing SIGTERM in too, `worker
/// serve`'s documented "let the in-flight tick finish, then exit cleanly" behavior
/// is unreachable in the only deployment shape torii targets: SIGTERM's POSIX
/// default disposition kills the process immediately (no output, no grace period),
/// because nothing has installed a handler for it. This is not a correctness bug —
/// the scheduler's lease reclaim makes an abrupt kill safe by construction — but an
/// abrupt kill still strands whatever run was mid-tick for up to the lease duration.
///
/// Returns a `watch::Receiver<u64>` — a LEVEL, not a one-shot event — whose value
/// this function's background task increments once per received signal. That is
/// what lets `serve` distinguish a first signal (finish the in-flight tick, then
/// exit) from a second (abandon it): a plain one-shot `Future` can, by
/// construction, only ever fire once, so a signal arriving while a tick is in
/// flight would be consumed and discarded with no way to tell "one" from "two or
/// more" — which is exactly the false claim this fixes (see `cmd::worker::serve`'s
/// doc comment for the full reasoning, including why a `watch` value beats
/// `Notify`'s single-permit coalescing here).
///
/// SIGTERM registration happens eagerly here (before `serve`'s loop starts) and its
/// failure is surfaced loudly rather than swallowed: `signal()` can fail for a
/// reachable reason (e.g. the process has already exhausted its signal-handling
/// slots), and this crate's error discipline is to never flatten a loud failure
/// into silence. `ctrl_c()`'s own await-time failure is left exactly as before
/// (discarded) — that path was never surfaced pre-fix and changing it is out of
/// scope here.
#[cfg(unix)]
fn shutdown_signal() -> Result<tokio::sync::watch::Receiver<u64>, CliError> {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|e| CliError::error(format!("cannot install a SIGTERM handler: {e}")))?;
    let (tx, rx) = tokio::sync::watch::channel(0u64);
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
            tx.send_modify(|n| *n += 1);
            // Nobody left to observe further signals (`serve` already returned) —
            // stop looping rather than holding the signal handlers open forever.
            if tx.is_closed() {
                return;
            }
        }
    });
    Ok(rx)
}

/// Non-unix fallback: `tokio::signal::unix` does not exist off unix, and there is
/// no portable SIGTERM equivalent, so SIGINT (`ctrl_c`) is all that's available.
#[cfg(not(unix))]
fn shutdown_signal() -> Result<tokio::sync::watch::Receiver<u64>, CliError> {
    let (tx, rx) = tokio::sync::watch::channel(0u64);
    tokio::spawn(async move {
        loop {
            let _ = tokio::signal::ctrl_c().await;
            tx.send_modify(|n| *n += 1);
            if tx.is_closed() {
                return;
            }
        }
    });
    Ok(rx)
}

/// `ExitCode` rather than `std::process::exit`: `process::exit` skips destructors
/// and any buffered-but-unflushed output. It was safe here only incidentally
/// (`ensure_newline` guarantees a trailing newline and `Stdout` is line-buffered,
/// so nothing was ever actually left unflushed) — `ExitCode` is the robust idiom
/// and costs nothing. Exit codes are always 0/1/2 (see `errors::EXIT_*`), so the
/// `as u8` cast is exact, never a truncation.
#[tokio::main]
async fn main() -> std::process::ExitCode {
    boot::init_tracing();
    let cli = Cli::parse();
    match dispatch(cli).await {
        Ok(out) => {
            print!("{}", ensure_newline(&out.text));
            std::process::ExitCode::from(out.code as u8)
        }
        Err(e) => {
            eprintln!("torii: {}", e.message);
            std::process::ExitCode::from(e.code as u8)
        }
    }
}

fn ensure_newline(s: &str) -> String {
    if s.ends_with('\n') || s.is_empty() {
        s.to_string()
    } else {
        format!("{s}\n")
    }
}

async fn dispatch(cli: Cli) -> Result<Outcome, CliError> {
    let env = boot::env_config()?;
    match cli.command {
        Command::Run { action } => match action {
            RunAction::Status { run_id, json } => {
                // Parse BEFORE connecting (pure, no network): an invalid run id is
                // the most likely operator typo, and sqlx retries a refused
                // connection silently for the whole pool-acquire timeout, so
                // connecting first would make a bad uuid take ~30s to reject.
                let run = parse_run_id(&run_id)?;
                let d = boot::light(&env).await?;
                cmd::run::status(d.scheduler_store.as_ref(), d.journal.as_ref(), run, json).await
            }
            RunAction::ListPaused { json } => {
                let d = boot::light(&env).await?;
                // SP-6 s1: the journal is where `SignalAwaited` lives — the scheduler row
                // is run-level and cannot name the awaiting node.
                cmd::run::list_paused(d.scheduler_store.as_ref(), d.journal.as_ref(), json).await
            }
            RunAction::Signal {
                run_id,
                node,
                payload,
                payload_file,
            } => {
                // Parse BEFORE connecting, same as `status`: an invalid run id is the
                // likeliest operator typo and sqlx would otherwise retry a refused
                // connection for the whole pool-acquire timeout first. The payload is
                // parsed here too — see its `#[arg]` comment for why it is not a clap
                // `value_parser` — so an unparseable or over-limit payload also costs no
                // connection, and its message stays free of the pasted value.
                let run = parse_run_id(&run_id)?;
                // Exactly one source: clap's `payload_src` group makes that a parse error,
                // so the `unwrap_or_default` below is unreachable rather than a silent
                // empty payload.
                let raw = match (payload, payload_file) {
                    (Some(inline), _) => inline,
                    (None, Some(path)) => std::fs::read_to_string(&path).map_err(|e| {
                        // The PATH is echoed (an operator typo is the whole point of this
                        // message) but never anything read from it.
                        CliError::error(format!(
                            "--payload-file {}: {e}. The file holds the decision as JSON, \
                             e.g. {{\"decision\":\"approved\"}}.",
                            path.display()
                        ))
                    })?,
                    (None, None) => String::new(),
                };
                let payload = cmd::run::parse_payload(raw.trim()).map_err(CliError::error)?;
                // LIGHT tier: delivering a decision needs the scheduler store and the
                // journal, nothing else. An operator must be able to answer a waiting run
                // from a box with no gateway config and no model credentials.
                let d = boot::light(&env).await?;
                cmd::run::signal(
                    d.scheduler_store.as_ref(),
                    d.journal.as_ref(),
                    run,
                    orchestrator_core::NodeId(node),
                    payload,
                    chrono::Utc::now(),
                )
                .await
            }
            RunAction::Cancel { run_id } => {
                let run = parse_run_id(&run_id)?;
                let d = boot::light(&env).await?;
                cmd::run::cancel(d.scheduler_store.as_ref(), run).await
            }
            RunAction::Wake {
                run_id,
                budget_tokens,
            } => {
                let run = parse_run_id(&run_id)?;
                let d = boot::light(&env).await?;
                let now = chrono::Utc::now();
                let budget = budget_tokens
                    .map(|total_tokens| orchestrator_core::TokenBudget { total_tokens });
                cmd::run::wake(
                    d.scheduler_store.as_ref(),
                    d.journal.as_ref(),
                    run,
                    now,
                    budget,
                )
                .await
            }
            RunAction::Prune { older_than, yes } => {
                let d = boot::light(&env).await?;
                // Same confirmation discipline as `config push`, and for the same reason:
                // this is an unrecoverable durable delete. `interactive_confirm` refuses on
                // EOF, so a non-interactive invocation (cron, `< /dev/null`) declines
                // rather than proceeding — `--yes` is the only way to script it.
                let mut confirm = |text: &str| {
                    cmd::config::interactive_confirm(
                        text,
                        &mut std::io::stdin().lock(),
                        &mut std::io::stderr(),
                    )
                };
                cmd::run::prune(
                    d.scheduler_store.as_ref(),
                    older_than,
                    chrono::Utc::now(),
                    yes,
                    &mut confirm,
                )
                .await
            }
            RunAction::Submit {
                graph,
                run_id,
                gateway_config,
                workspace_root,
                budget_tokens,
            } => {
                let run = match run_id {
                    Some(s) => parse_run_id(&s)?,
                    None => RunId(uuid::Uuid::new_v4()),
                };
                let raw = std::fs::read_to_string(&graph).map_err(|e| {
                    CliError::error(format!("cannot read {}: {e}", graph.display()))
                })?;
                let g: Graph = serde_json::from_str(&raw).map_err(|e| {
                    CliError::error(format!("{} is not a valid graph: {e}", graph.display()))
                })?;
                let d = boot::heavy(&env, &gateway_config, workspace_root.as_deref()).await?;
                let budget = budget_tokens
                    .map(|total_tokens| orchestrator_core::TokenBudget { total_tokens });
                // Print the id BEFORE driving: an operator who loses the terminal
                // must still be able to find the run. `submit` calls this AFTER its
                // duplicate pre-check, so a rejected submit no longer announces an
                // effect that never happened.
                cmd::run::submit(&d.scheduler, run, g, budget, || {
                    println!("submitted: {}", run.0)
                })
                .await
            }
        },
        Command::Worker { action } => match action {
            WorkerAction::Serve {
                interval,
                once,
                gateway_config,
                workspace_root,
            } => {
                let d = boot::heavy(&env, &gateway_config, workspace_root.as_deref()).await?;
                let shutdown = shutdown_signal()?;
                cmd::worker::serve(
                    &d.scheduler,
                    cmd::worker::ServeOpts { interval, once },
                    shutdown,
                )
                .await
            }
        },
        Command::Config { action } => {
            let d = boot::light(&env).await?;
            match action {
                ConfigAction::Version { json } => {
                    cmd::config::version(&d.config_source, json).await
                }
                ConfigAction::Push { dir, yes } => {
                    let mut confirm = |text: &str| {
                        cmd::config::interactive_confirm(
                            text,
                            &mut std::io::stdin().lock(),
                            &mut std::io::stderr(),
                        )
                    };
                    // The scheduler store rides the same pool `light` already opened —
                    // `push` reads it to disclose how much paused work a generation
                    // bump would strand.
                    cmd::config::push(
                        &d.config_source,
                        d.scheduler_store.as_ref(),
                        &dir,
                        yes,
                        &mut confirm,
                    )
                    .await
                }
            }
        }
    }
}
