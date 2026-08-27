//! Command implementations. Each returns an `Outcome` rather than printing, so
//! every command is unit-testable without capturing stdout.

pub mod config;
pub mod gate;
pub mod run;
pub mod worker;

/// What a command produced: the operator-facing text and the process exit code.
#[derive(Debug, PartialEq)]
pub struct Outcome {
    pub text: String,
    pub code: i32,
}

impl Outcome {
    pub fn ok(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            code: crate::errors::EXIT_OK,
        }
    }
    /// A command that ran fine but whose precondition was not met (nothing to
    /// cancel, nothing to wake, no such run).
    pub fn precondition(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            code: crate::errors::EXIT_PRECONDITION,
        }
    }
}
