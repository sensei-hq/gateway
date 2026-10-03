//! Decision (System One) questions and answers — gh#72.
//!
//! A decision call sends one shared `state` plus up to 64 named, typed
//! questions and gets back **probabilities**, not text. The wire format is
//! TypeSafe's System One API, which Ollama (≥ 0.35), OpenRouter and TypeSafe
//! serve at `POST {base}/v1/systemone`; these types serialize to it verbatim.
//!
//! Order is semantic, so every map here is an [`IndexMap`]: choice ties follow
//! the option order, score levels are ordered lowest-first, and answers come
//! back keyed in request order.

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

/// Free-form content — a non-blank string, or a JSON object/array (possibly
/// empty) the provider serializes as JSON text. Used for `state` and each question's `instructions`.
pub type DecisionContent = serde_json::Value;

/// Named questions about the shared state, in request order.
pub type DecisionQuestions = IndexMap<String, DecisionQuestion>;

/// Answers keyed by question name, in response order.
pub type DecisionAnswers = IndexMap<String, DecisionAnswer>;

/// Most questions one call may carry (Ollama, OpenRouter and TypeSafe agree).
pub const MAX_DECISION_QUESTIONS: usize = 64;

/// Fewest options a `choice` or levels a `score` question may have. Upper
/// bounds differ per provider (Ollama 26; TypeSafe 255 options / 10 levels),
/// so they are left to the provider's own 400, which falls back.
pub const MIN_DECISION_CRITERIA: usize = 2;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DecisionQuestion {
    /// Pick one of the named options. A `None` description means the key
    /// itself describes the option.
    Choice {
        instructions: DecisionContent,
        criteria: IndexMap<String, Option<String>>,
    },
    /// Probability that the condition holds ("noul" — a number, not a bool).
    Noul {
        instructions: DecisionContent,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        criteria: Option<NoulCriteria>,
    },
    /// Probability-weighted level on an ordered scale, lowest first.
    Score {
        instructions: DecisionContent,
        criteria: Vec<String>,
    },
}

/// Optional descriptions for a noul question's two outcomes; an omitted side
/// defaults to "No" / "Yes" on the provider.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NoulCriteria {
    #[serde(rename = "false", default, skip_serializing_if = "Option::is_none")]
    pub r#false: Option<String>,
    #[serde(rename = "true", default, skip_serializing_if = "Option::is_none")]
    pub r#true: Option<String>,
}

/// One question's answer.
///
/// **`confidence` measures how concentrated the probability mass is on one
/// answer, NOT whether that answer is correct.** It is uncalibrated: a
/// confident answer can be wrong, and nothing should present it to a user as
/// accuracy or gate on it as if it were.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DecisionAnswer {
    Choice {
        /// The option key with the highest probability.
        choice: String,
        /// Per-option probabilities, normalized over the supplied options.
        probabilities: IndexMap<String, f64>,
        /// Concentration of `probabilities`, 0–1 — not correctness.
        confidence: f64,
    },
    Noul {
        /// Probability of `true`, 0–1.
        noul: f64,
    },
    Score {
        /// Probability-weighted mean of the zero-based level indices, from 0
        /// to `levels − 1`. Not rounded, not normalized to 0–1.
        score: f64,
        /// Zero-based level index (as a string) → its description.
        legend: IndexMap<String, String>,
        /// Per-level probabilities keyed like `legend`.
        probabilities: IndexMap<String, f64>,
        /// Concentration of `probabilities`, 0–1 — not correctness.
        confidence: f64,
    },
}

/// Check the structural limits every System One provider shares, so a request
/// that cannot succeed anywhere fails once, up front, instead of walking the
/// whole fallback chain. Returns a message naming the offending field.
pub fn validate_decision(
    state: &DecisionContent,
    questions: &DecisionQuestions,
) -> Result<(), String> {
    if is_blank(state) {
        return Err("decision `state` must be a non-blank string, an object or an array".into());
    }
    if questions.is_empty() || questions.len() > MAX_DECISION_QUESTIONS {
        return Err(format!(
            "decision `questions` must contain 1–{MAX_DECISION_QUESTIONS} entries, got {}",
            questions.len()
        ));
    }
    for (name, question) in questions {
        if name.trim().is_empty() {
            return Err("decision question names must not be blank".into());
        }
        let (instructions, criteria) = match question {
            DecisionQuestion::Choice {
                instructions,
                criteria,
            } => {
                if criteria.keys().any(|k| k.trim().is_empty()) {
                    return Err(format!(
                        "decision question {name:?}: option keys must not be blank"
                    ));
                }
                (instructions, Some(criteria.len()))
            }
            DecisionQuestion::Score {
                instructions,
                criteria,
            } => (instructions, Some(criteria.len())),
            DecisionQuestion::Noul { instructions, .. } => (instructions, None),
        };
        if is_blank(instructions) {
            return Err(format!(
                "decision question {name:?}: `instructions` must not be blank"
            ));
        }
        if let Some(n) = criteria
            && n < MIN_DECISION_CRITERIA
        {
            return Err(format!(
                "decision question {name:?}: needs at least {MIN_DECISION_CRITERIA} criteria, got {n}"
            ));
        }
    }
    Ok(())
}

/// Upstream `SystemOneContent` is `string (pattern \S) | object | array` with no
/// `minProperties` / `minItems`: an empty `{}` or `[]` is valid (Ollama answers
/// it), only a whitespace-only string — or a non-string scalar / null — is not.
fn is_blank(content: &DecisionContent) -> bool {
    match content {
        serde_json::Value::String(s) => s.trim().is_empty(),
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => false,
        _ => true,
    }
}
