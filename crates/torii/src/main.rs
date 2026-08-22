//! `torii` — the operator control plane for the sensei orchestrator.
//! Task 10 replaces this with the clap dispatch.

mod boot;
mod cmd;
mod diff;
mod errors;
mod render;

fn main() {
    eprintln!(
        "torii: not yet wired (see docs/superpowers/plans/2026-08-22-sp-data-4-torii-management-cli.md)"
    );
    std::process::exit(errors::EXIT_ERROR);
}
