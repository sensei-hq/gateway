//! Benchmark cases and their JSONL on-disk format.
//!
//! A case file is JSONL: one compact JSON object per line. An optional first
//! line is a [`Header`] (provenance notes, who reviewed the labels).
//!
//! Header rule: the first non-blank line is a header if and only if it is a
//! JSON object that has neither an `id` nor a `scenario` key. Such a line must
//! then deserialize as a [`Header`] (unknown keys are rejected, so a typo does
//! not silently turn a header into nothing). Any other first line is a case.

use std::io::Write;
use std::path::Path;

use anyhow::{Context, bail};
use gateway::types::decision::DecisionQuestion;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Scenario {
    S1aIntentOos,
    S1bBanking77,
    S2Sentiment,
    S3Entailment,
    S4TestOutput,
    S4ReviewFinding,
    S4FixCheck,
}

impl Scenario {
    /// Stable per-variant constant, used to derive seeds. Never renumber.
    pub fn tag(&self) -> u64 {
        match self {
            Scenario::S1aIntentOos => 1,
            Scenario::S1bBanking77 => 2,
            Scenario::S2Sentiment => 3,
            Scenario::S3Entailment => 4,
            Scenario::S4TestOutput => 5,
            Scenario::S4ReviewFinding => 6,
            Scenario::S4FixCheck => 7,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Expected {
    Choice { option: String },
    Noul { value: bool },
    Score { level: usize },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Case {
    pub id: String,
    pub scenario: Scenario,
    pub state: serde_json::Value,
    pub question: DecisionQuestion,
    pub expected: Expected,
    /// e.g. "clinc/clinc_oos@155b9c7104:test#61" or "hand-curated".
    pub source: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Header {
    #[serde(default)]
    pub labels_reviewed_by: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
}

impl Case {
    /// Reject an expectation that cannot be an answer to the case's question.
    pub fn validate(&self) -> anyhow::Result<()> {
        let id = &self.id;
        match (&self.question, &self.expected) {
            (DecisionQuestion::Choice { criteria, .. }, Expected::Choice { option }) => {
                if !criteria.contains_key(option) {
                    bail!(
                        "case {id}: expected option {option:?} is not among the question's criteria"
                    );
                }
            }
            (DecisionQuestion::Noul { .. }, Expected::Noul { .. }) => {}
            (DecisionQuestion::Score { criteria, .. }, Expected::Score { level }) => {
                if *level >= criteria.len() {
                    bail!(
                        "case {id}: expected score level {level} is out of range for {} criteria",
                        criteria.len()
                    );
                }
            }
            (q, e) => bail!(
                "case {id}: expectation {e:?} does not match the question type {}",
                match q {
                    DecisionQuestion::Choice { .. } => "choice",
                    DecisionQuestion::Noul { .. } => "noul",
                    DecisionQuestion::Score { .. } => "score",
                }
            ),
        }
        Ok(())
    }
}

/// Read every non-blank line of a JSONL file as a `T`. Errors name `line N`
/// (1-based, counting blank lines).
pub fn read_jsonl<T: serde::de::DeserializeOwned>(path: &Path) -> anyhow::Result<Vec<T>> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut out = Vec::new();
    for (i, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let v = serde_json::from_str(line)
            .with_context(|| format!("{}: line {}", path.display(), i + 1))?;
        out.push(v);
    }
    Ok(out)
}

/// Write one compact JSON object per line, each `\n`-terminated.
pub fn write_jsonl<T: Serialize>(path: &Path, items: &[T]) -> anyhow::Result<()> {
    let mut f =
        std::fs::File::create(path).with_context(|| format!("creating {}", path.display()))?;
    for item in items {
        serde_json::to_writer(&mut f, item)?;
        f.write_all(b"\n")?;
    }
    f.flush()?;
    Ok(())
}

/// Read a case file: optional header line (see module docs), then cases, each
/// validated.
pub fn read_case_file(path: &Path) -> anyhow::Result<(Header, Vec<Case>)> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut header = Header::default();
    let mut cases = Vec::new();
    let mut seen_first = false;
    for (i, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let at = || format!("{}: line {}", path.display(), i + 1);
        let value: serde_json::Value = serde_json::from_str(line).with_context(at)?;
        let is_header = !seen_first
            && value
                .as_object()
                .is_some_and(|o| !o.contains_key("id") && !o.contains_key("scenario"));
        seen_first = true;
        if is_header {
            header = serde_json::from_value(value).with_context(at)?;
        } else {
            let case: Case = serde_json::from_value(value).with_context(at)?;
            case.validate().with_context(at)?;
            cases.push(case);
        }
    }
    Ok((header, cases))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn q(v: serde_json::Value) -> DecisionQuestion {
        serde_json::from_value(v).unwrap()
    }
    fn choice_q() -> DecisionQuestion {
        q(json!({"type":"choice","instructions":"pick","criteria":{"a":null,"b":"the b"}}))
    }
    fn score_q() -> DecisionQuestion {
        q(json!({"type":"score","instructions":"rate","criteria":["bad","ok","good"]}))
    }
    fn noul_q() -> DecisionQuestion {
        q(json!({"type":"noul","instructions":"is it?"}))
    }
    fn case(id: &str, question: DecisionQuestion, expected: Expected) -> Case {
        Case {
            id: id.into(),
            scenario: Scenario::S2Sentiment,
            state: json!({"text":"hi"}),
            question,
            expected,
            source: "hand-curated".into(),
        }
    }
    fn tmp(content: &str) -> tempfile::NamedTempFile {
        let f = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(f.path(), content).unwrap();
        f
    }

    #[test]
    fn case_round_trips_through_jsonl() {
        let cases = vec![
            case("c1", choice_q(), Expected::Choice { option: "b".into() }),
            case("c2", score_q(), Expected::Score { level: 2 }),
        ];
        let f = tempfile::NamedTempFile::new().unwrap();
        write_jsonl(f.path(), &cases).unwrap();
        let text = std::fs::read_to_string(f.path()).unwrap();
        assert_eq!(text.lines().count(), 2);
        assert!(text.ends_with('\n'));
        let back: Vec<Case> = read_jsonl(f.path()).unwrap();
        assert_eq!(back, cases);
    }

    #[test]
    fn a_header_line_is_returned_separately() {
        let c = case("c1", noul_q(), Expected::Noul { value: true });
        let body = serde_json::to_string(&c).unwrap();
        let f = tmp(&format!(
            "{{\"labels_reviewed_by\": null, \"notes\": \"hello\"}}\n{body}\n"
        ));
        let (h, cs) = read_case_file(f.path()).unwrap();
        assert_eq!(
            h,
            Header {
                labels_reviewed_by: None,
                notes: Some("hello".into())
            }
        );
        assert_eq!(cs, vec![c.clone()]);

        let f = tmp(&format!("{body}\n"));
        let (h, cs) = read_case_file(f.path()).unwrap();
        assert_eq!(h, Header::default());
        assert_eq!(cs, vec![c]);

        // A header-shaped first line with an unknown key is an error, not a silent skip.
        let f = tmp("{\"labels_reviewd_by\": \"x\"}\n");
        assert!(read_case_file(f.path()).is_err());
    }

    #[test]
    fn expected_must_match_the_question_type() {
        let bad = [
            case(
                "choice-bad-opt",
                choice_q(),
                Expected::Choice {
                    option: "zzz".into(),
                },
            ),
            case("score-oob", score_q(), Expected::Score { level: 3 }),
            case("noul-on-choice", choice_q(), Expected::Noul { value: true }),
            case("score-on-choice", choice_q(), Expected::Score { level: 0 }),
            case(
                "choice-on-noul",
                noul_q(),
                Expected::Choice { option: "a".into() },
            ),
            case("score-on-noul", noul_q(), Expected::Score { level: 0 }),
            case(
                "choice-on-score",
                score_q(),
                Expected::Choice { option: "a".into() },
            ),
            case("noul-on-score", score_q(), Expected::Noul { value: false }),
        ];
        for c in &bad {
            let err = c.validate().unwrap_err().to_string();
            assert!(err.contains(&c.id), "{err}");
        }
        for c in [
            case("ok1", choice_q(), Expected::Choice { option: "a".into() }),
            case("ok2", score_q(), Expected::Score { level: 2 }),
            case("ok3", noul_q(), Expected::Noul { value: false }),
        ] {
            c.validate().unwrap();
        }

        let f = tmp(&format!("{}\n", serde_json::to_string(&bad[0]).unwrap()));
        let err = format!("{:#}", read_case_file(f.path()).unwrap_err());
        assert!(err.contains("choice-bad-opt"), "{err}");
    }

    #[test]
    fn blank_lines_are_skipped_and_a_bad_line_names_its_line_number() {
        let c = case("c1", noul_q(), Expected::Noul { value: true });
        let body = serde_json::to_string(&c).unwrap();
        let f = tmp(&format!("\n{body}\n\n   \n{body}\n"));
        let cs: Vec<Case> = read_jsonl(f.path()).unwrap();
        assert_eq!(cs.len(), 2);

        let f = tmp(&format!("{body}\n\n{{not json\n"));
        let err = format!("{:#}", read_jsonl::<Case>(f.path()).unwrap_err());
        assert!(err.contains("line 3"), "{err}");
        let err = format!("{:#}", read_case_file(f.path()).unwrap_err());
        assert!(err.contains("line 3"), "{err}");
    }

    #[test]
    fn scenario_tags_are_distinct() {
        let all = [
            Scenario::S1aIntentOos,
            Scenario::S1bBanking77,
            Scenario::S2Sentiment,
            Scenario::S3Entailment,
            Scenario::S4TestOutput,
            Scenario::S4ReviewFinding,
            Scenario::S4FixCheck,
        ];
        let set: std::collections::HashSet<_> = all.iter().map(Scenario::tag).collect();
        assert_eq!(set.len(), all.len());
    }
}
