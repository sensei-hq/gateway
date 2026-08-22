//! `torii` — the operator control plane for the sensei orchestrator.

mod boot;
mod cmd;
mod diff;
mod errors;
mod render;

use clap::{Parser, Subcommand};
use cmd::Outcome;
use errors::CliError;
use orchestrator_core::{Graph, RunId};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "torii",
    about = "Operator control plane for the sensei orchestrator",
    long_about = "Observe and intervene on runs, drive due wakes, and manage durable config.\n\n\
                  DATABASE_URL must be set (env only — a flag would leak the password into `ps`).\n\
                  `run submit` and `worker serve` additionally need TORII_FENCE_VERSION and \
                  --gateway-config."
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
    },
    /// Show one run's schedule record
    Status {
        run_id: String,
        #[arg(long)]
        json: bool,
    },
    /// List every run awaiting a wake
    ListPaused {
        #[arg(long)]
        json: bool,
    },
    /// Cancel a non-terminal run so it is never woken
    Cancel { run_id: String },
    /// Queue a paused run for the next worker tick
    Wake { run_id: String },
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

#[tokio::main]
async fn main() {
    boot::init_tracing();
    let cli = Cli::parse();
    match dispatch(cli).await {
        Ok(out) => {
            print!("{}", ensure_newline(&out.text));
            std::process::exit(out.code);
        }
        Err(e) => {
            eprintln!("torii: {}", e.message);
            std::process::exit(e.code);
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
                cmd::run::status(d.scheduler_store.as_ref(), run, json).await
            }
            RunAction::ListPaused { json } => {
                let d = boot::light(&env).await?;
                cmd::run::list_paused(d.scheduler_store.as_ref(), json).await
            }
            RunAction::Cancel { run_id } => {
                let run = parse_run_id(&run_id)?;
                let d = boot::light(&env).await?;
                cmd::run::cancel(d.scheduler_store.as_ref(), run).await
            }
            RunAction::Wake { run_id } => {
                let run = parse_run_id(&run_id)?;
                let d = boot::light(&env).await?;
                let now = chrono::Utc::now();
                cmd::run::wake(d.scheduler_store.as_ref(), run, now).await
            }
            RunAction::Submit {
                graph,
                run_id,
                gateway_config,
                workspace_root,
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
                // Print the id BEFORE driving: an operator who loses the terminal
                // must still be able to find the run.
                println!("submitted: {}", run.0);
                cmd::run::submit(&d.scheduler, run, g).await
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
                let shutdown = async {
                    let _ = tokio::signal::ctrl_c().await;
                };
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
                    cmd::config::push(&d.config_source, &dir, yes, &mut confirm).await
                }
            }
        }
    }
}
