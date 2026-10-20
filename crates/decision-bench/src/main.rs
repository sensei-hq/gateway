use std::path::{Path, PathBuf};
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
    #[command(after_long_help = PATH_NOTE)]
    Fetch {
        /// Cases sampled per scenario.
        #[arg(long, default_value_t = 200)]
        per_scenario: usize,
        /// Sampling seed.
        #[arg(long, default_value_t = 2026)]
        seed: u64,
        /// Output directory for s1a/s1b/s2/s3.jsonl and manifest.json, replaced
        /// as a whole (relative: see below).
        #[arg(long, default_value = "cases")]
        out: PathBuf,
        /// Raw-row cache (default: <user cache dir>/decision-bench; relative:
        /// see below).
        #[arg(long)]
        cache_dir: Option<PathBuf>,
    },
    /// Run the benchmark.
    Run,
    /// Render the per-scenario ranking report.
    Report,
}

/// Where a relative path is anchored, so `fetch` writes to the same place
/// whatever directory it is run from.
const PATH_NOTE: &str = concat!(
    "Paths: relative paths resolve against the decision-bench crate directory, ",
    env!("CARGO_MANIFEST_DIR"),
    ", not the current directory; so the default --out is ",
    env!("CARGO_MANIFEST_DIR"),
    "/cases."
);

/// `p` if absolute, else `p` under the crate directory (see [`PATH_NOTE`]).
fn resolve(p: &Path) -> PathBuf {
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        Path::new(env!("CARGO_MANIFEST_DIR")).join(p)
    }
}

async fn fetch(
    per_scenario: usize,
    seed: u64,
    out: PathBuf,
    cache_dir: Option<PathBuf>,
) -> anyhow::Result<()> {
    let cache_root = match cache_dir {
        Some(d) => resolve(&d),
        None => default_cache_root()?,
    };
    let out = resolve(&out);
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    const CRATE: &str = env!("CARGO_MANIFEST_DIR");

    #[test]
    fn relative_paths_resolve_against_the_crate_dir_and_absolute_ones_are_kept() {
        assert_eq!(resolve(Path::new("cases")), Path::new(CRATE).join("cases"));
        assert_eq!(resolve(Path::new("a/b")), Path::new(CRATE).join("a/b"));
        assert_eq!(resolve(Path::new("/tmp/x")), Path::new("/tmp/x"));
    }

    #[test]
    fn the_default_out_is_the_crates_cases_dir_wherever_it_runs_from() {
        let Command::Fetch { out, .. } = Cli::parse_from(["decision-bench", "fetch"]).command
        else {
            panic!("not fetch")
        };
        assert_eq!(resolve(&out), Path::new(CRATE).join("cases"));
    }

    #[test]
    fn fetch_help_says_relative_paths_are_resolved_against_the_crate_dir() {
        let mut cmd = Cli::command();
        let help = cmd
            .find_subcommand_mut("fetch")
            .unwrap()
            .render_long_help()
            .to_string();
        assert!(help.contains("relative paths resolve against"), "{help}");
        assert!(help.contains(CRATE), "{help}");
    }
}
