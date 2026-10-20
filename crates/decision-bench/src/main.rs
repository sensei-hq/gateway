use clap::{Parser, Subcommand};

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
    /// Fetch the benchmark datasets.
    Fetch,
    /// Run the benchmark.
    Run,
    /// Render the per-scenario ranking report.
    Report,
}

fn main() {
    let task = match Cli::parse().command {
        Command::Check => 7,
        Command::Fetch => 3,
        Command::Run => 4,
        Command::Report => 7,
    };
    eprintln!("not implemented yet (SP-DEC-4 T{task})");
    std::process::exit(2);
}
