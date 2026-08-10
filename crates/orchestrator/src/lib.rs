//! `sensei-orchestrator` — the deterministic executor over a durable journal,
//! wired to the real gateway.
//!
//! Slice 1 provides the [`Executor`]: a fresh linear `run` (Task 3) and a
//! resume/fold `start` (Task 4). Each `ModelCall` node compiles into a plain
//! `InferenceRequest`, runs through the gateway, and is journaled with a
//! structural effect id + input hash.

pub mod agent;
pub mod executor;

#[cfg(test)]
mod test_support;

pub use executor::{Executor, RunOutcome};
