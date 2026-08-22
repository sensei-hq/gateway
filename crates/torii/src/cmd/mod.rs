//! Command implementations. Each returns an `Outcome` rather than printing, so
//! every command is unit-testable without capturing stdout.

pub mod config;
pub mod run;
pub mod worker;

/// What a command produced: the operator-facing text and the process exit code.
// Consumed by Task 10 (main.rs clap dispatch), which prints `.text` and exits `.code`.
#[allow(dead_code)]
#[derive(Debug, PartialEq)]
pub struct Outcome {
    pub text: String,
    pub code: i32,
}

impl Outcome {
    // Consumed by Task 10 (main.rs clap dispatch).
    #[allow(dead_code)]
    pub fn ok(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            code: crate::errors::EXIT_OK,
        }
    }
    /// A command that ran fine but whose precondition was not met (nothing to
    /// cancel, nothing to wake, no such run).
    // Consumed by Task 10 (main.rs clap dispatch).
    #[allow(dead_code)]
    pub fn precondition(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            code: crate::errors::EXIT_PRECONDITION,
        }
    }
}
