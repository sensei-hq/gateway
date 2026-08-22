//! `sensei-orchestrator` — the deterministic executor over a durable journal,
//! wired to the real gateway.
//!
//! Slice 1 provides the [`Executor`]: a fresh linear `run` (Task 3) and a
//! resume/fold `start` (Task 4). Each `ModelCall` node compiles into a plain
//! `InferenceRequest`, runs through the gateway, and is journaled with a
//! structural effect id + input hash.

pub mod agent;
pub mod executor;
pub mod scheduler;

// Dev-only: `test` for this crate's own tests, `feature = "test-support"` for ANOTHER
// crate's dev-dependencies (torii's cross-process e2e needs the same gateway/clock
// doubles). Nothing in a production build enables the feature, so a release binary is
// byte-identical to before.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support;

pub use executor::selector::LlmPlannerSelector;
pub use executor::{Executor, RunOutcome};
pub use scheduler::Scheduler;
