use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use decision_bench::fetch::{Hf, default_cache_root};
use decision_bench::scenarios::{FetchOptions, fetch_cases};

#[derive(Parser)]
#[command(
    name = "decision-bench",
    about = "Benchmark System One decision models"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Probe every configured model server.
    Check,
    /// Fetch the public datasets and build the seeded S1a/S1b/S2/S3 case files.
    Fetch {
        /// Cases sampled per scenario.
        #[arg(long, default_value_t = 200)]
        per_scenario: usize,
        /// Sampling seed.
        #[arg(long, default_value_t = 2026)]
        seed: u64,
        /// Output directory for s1a/s1b/s2/s3.jsonl and manifest.json.
        #[arg(long, default_value = "crates/decision-bench/cases")]
        out: PathBuf,
        /// Raw-row cache (default: <user cache dir>/decision-bench).
        #[arg(long)]
        cache_dir: Option<PathBuf>,
    },
    /// Run the benchmark.
    Run,
    /// Render the per-scenario ranking report.
    Report,
}

async fn fetch(
    per_scenario: usize,
    seed: u64,
    out: PathBuf,
    cache_dir: Option<PathBuf>,
) -> anyhow::Result<()> {
    let cache_root = match cache_dir {
        Some(d) => d,
        None => default_cache_root()?,
    };
    std::fs::create_dir_all(&out)?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .user_agent(concat!("decision-bench/", env!("CARGO_PKG_VERSION")))
        .build()?;
    let manifest = fetch_cases(
        &Hf::live(client),
        &FetchOptions {
            cache_root: &cache_root,
            out: &out,
            seed,
            per_scenario,
        },
    )
    .await?;
    for d in &manifest.datasets {
        println!(
            "{} {}/{}  sha {}  licence {}  rows {}",
            d.id, d.config, d.split, d.sha, d.license, d.num_rows
        );
    }
    for f in &manifest.files {
        println!("{}  {} cases", out.join(&f.file).display(), f.cases);
    }
    println!(
        "seed {}  rand {}  cache {}",
        manifest.seed,
        manifest.rand_version,
        cache_root.display()
    );
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let task = match Cli::parse().command {
        Command::Fetch {
            per_scenario,
            seed,
            out,
            cache_dir,
        } => return fetch(per_scenario, seed, out, cache_dir).await,
        Command::Check => 7,
        Command::Run => 4,
        Command::Report => 7,
    };
    eprintln!("not implemented yet (SP-DEC-4 T{task})");
    std::process::exit(2);
}
