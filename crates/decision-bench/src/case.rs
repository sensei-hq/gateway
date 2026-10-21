//! Benchmark cases and their JSONL on-disk format.
//!
//! A case file is JSONL: one compact JSON object per line. An optional first
//! line is a [`Header`] (provenance notes, who reviewed the labels).
//!
//! Header rule: the first non-blank line is a header if and only if it is a
//! JSON object that has neither an `id` nor a `scenario` key. Such a line must
//! then deserialize as a [`Header`] (unknown keys are rejected, so a typo does
//! not silently turn a header into nothing). Any other first line is a case.
//! A header line that fails to parse is reported as "has no id/scenario, so it
//! was read as a header". A leading UTF-8 BOM is ignored.
//!
//! [`read_case_file`] also rejects: an empty or header-only file, duplicate
//! case ids, and a file mixing more than one `scenario`. Every case is checked
//! with the gateway's own `validate_decision`, so a case that could never run
//! fails at load time rather than looking like a model being wrong.
//!
//! Known limitation: duplicate keys inside a choice's `criteria` object
//! collapse silently (serde's map types keep the last value, and the raw
//! `Value` is a map too), so they cannot be detected here. T9's file check is
//! the guard for that.

use std::io::Write;
use std::path::Path;

use anyhow::{Context, bail};
use gateway::types::decision::{DecisionQuestion, DecisionQuestions, validate_decision};
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
#[serde(deny_unknown_fields)]
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
        let mut questions = DecisionQuestions::default();
        questions.insert("q".to_string(), self.question.clone());
        validate_decision(&self.state, &questions)
            .map_err(|msg| anyhow::anyhow!("case {id}: {msg}"))?;
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

fn read_text(path: &Path) -> anyhow::Result<String> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(text
        .strip_prefix('\u{feff}')
        .map(str::to_owned)
        .unwrap_or(text))
}

/// Read every non-blank line of a JSONL file as a `T`. Errors name `line N`
/// (1-based, counting blank lines).
pub fn read_jsonl<T: serde::de::DeserializeOwned>(path: &Path) -> anyhow::Result<Vec<T>> {
    let text = read_text(path)?;
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

/// Write one compact JSON object per line, each `\n`-terminated. Writes to
/// `<path>.tmp`, `sync_all`s it, then renames, so a failed write never leaves a truncated file
/// (or clobbers an existing one).
pub fn write_jsonl<T: Serialize>(path: &Path, items: &[T]) -> anyhow::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = std::path::PathBuf::from(tmp);
    let result = (|| -> anyhow::Result<()> {
        let f =
            std::fs::File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
        let mut w = std::io::BufWriter::new(f);
        for (i, item) in items.iter().enumerate() {
            serde_json::to_writer(&mut w, item)
                .with_context(|| format!("{}: writing item {i}", path.display()))?;
            w.write_all(b"\n")
                .with_context(|| format!("{}: writing item {i}", path.display()))?;
        }
        let f = w
            .into_inner()
            .map_err(std::io::IntoInnerError::into_error)
            .with_context(|| format!("flushing {}", tmp.display()))?;
        // Durable before the rename: callers then write a marker (meta.json,
        // manifest.json) and delete the only other copy.
        f.sync_all()
            .with_context(|| format!("syncing {}", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .with_context(|| format!("renaming {} to {}", tmp.display(), path.display()))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

/// Read a case file: optional header line (see module docs), then cases, each
/// validated.
pub fn read_case_file(path: &Path) -> anyhow::Result<(Header, Vec<Case>)> {
    let text = read_text(path)?;
    let mut header = Header::default();
    let mut cases: Vec<Case> = Vec::new();
    let mut ids: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut scenario: Option<(Scenario, usize)> = None;
    let mut seen_first = false;
    for (i, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let n = i + 1;
        let at = || format!("{}: line {n}", path.display());
        let value: serde_json::Value = serde_json::from_str(line).with_context(at)?;
        let is_header = !seen_first
            && value
                .as_object()
                .is_some_and(|o| !o.contains_key("id") && !o.contains_key("scenario"));
        seen_first = true;
        if is_header {
            header = serde_json::from_value(value)
                .with_context(|| format!("line {n} has no id/scenario, so it was read as a header"))
                .with_context(at)?;
            continue;
        }
        let case: Case = serde_json::from_value(value).with_context(at)?;
        case.validate().with_context(at)?;
        if let Some(first) = ids.get(&case.id) {
            bail!(
                "{}: duplicate case id {:?} (first seen on line {first})",
                at(),
                case.id
            );
        }
        ids.insert(case.id.clone(), n);
        match scenario {
            None => scenario = Some((case.scenario, n)),
            Some((s, first)) if s != case.scenario => bail!(
                "{}: mixed scenarios: case {:?} is {:?} but line {first} is {s:?}",
                at(),
                case.id,
                case.scenario
            ),
            Some(_) => {}
        }
        cases.push(case);
    }
    if cases.is_empty() {
        bail!("{}: file contains no cases", path.display());
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
        let mut c1 = case("c1", choice_q(), Expected::Choice { option: "b".into() });
        c1.state = json!({"text": "hi", "n": 0.5});
        let cases = vec![c1, case("c2", score_q(), Expected::Score { level: 2 })];
        let f = tempfile::NamedTempFile::new().unwrap();
        write_jsonl(f.path(), &cases).unwrap();
        let text = std::fs::read_to_string(f.path()).unwrap();
        let expected = concat!(
            r#"{"id":"c1","scenario":"s2_sentiment","state":{"n":0.5,"text":"hi"},"question":{"type":"choice","instructions":"pick","criteria":{"a":null,"b":"the b"}},"expected":{"kind":"choice","option":"b"},"source":"hand-curated"}"#,
            "\n",
            r#"{"id":"c2","scenario":"s2_sentiment","state":{"text":"hi"},"question":{"type":"score","instructions":"rate","criteria":["bad","ok","good"]},"expected":{"kind":"score","level":2},"source":"hand-curated"}"#,
            "\n",
        );
        assert_eq!(text, expected);
        let back: Vec<Case> = read_jsonl(f.path()).unwrap();
        assert_eq!(back, cases);
    }

    #[test]
    fn option_order_survives_a_round_trip() {
        // IndexMap's PartialEq ignores order, so compare the key order itself.
        // Parse from text: `json!` would sort the keys before we ever wrote them.
        let rev: DecisionQuestion = serde_json::from_str(
            r#"{"type":"choice","instructions":"pick","criteria":{"b":null,"a":null}}"#,
        )
        .unwrap();
        let c = case("c1", rev, Expected::Choice { option: "a".into() });
        let f = tempfile::NamedTempFile::new().unwrap();
        write_jsonl(f.path(), &[c]).unwrap();
        let back: Vec<Case> = read_jsonl(f.path()).unwrap();
        let DecisionQuestion::Choice { criteria, .. } = &back[0].question else {
            panic!("not a choice")
        };
        assert_eq!(criteria.keys().collect::<Vec<_>>(), ["b", "a"]);
    }

    #[test]
    fn write_jsonl_is_atomic_and_names_the_failing_item() {
        struct Boom(bool);
        impl Serialize for Boom {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                if self.0 {
                    Err(serde::ser::Error::custom("boom"))
                } else {
                    s.serialize_u8(1)
                }
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.jsonl");
        std::fs::write(&path, "previous\n").unwrap();
        let err = format!(
            "{:#}",
            write_jsonl(&path, &[Boom(false), Boom(true)]).unwrap_err()
        );
        assert!(err.contains("item 1"), "{err}");
        assert!(err.contains("out.jsonl"), "{err}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "previous\n");
        assert!(!dir.path().join("out.jsonl.tmp").exists());

        let missing = dir.path().join("no-such-dir").join("x.jsonl");
        let err = format!("{:#}", write_jsonl(&missing, &[Boom(false)]).unwrap_err());
        assert!(err.contains("x.jsonl"), "{err}");
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
    fn scenario_tags_and_serde_names_are_pinned() {
        let table = [
            (Scenario::S1aIntentOos, 1, "s1a_intent_oos"),
            (Scenario::S1bBanking77, 2, "s1b_banking77"),
            (Scenario::S2Sentiment, 3, "s2_sentiment"),
            (Scenario::S3Entailment, 4, "s3_entailment"),
            (Scenario::S4TestOutput, 5, "s4_test_output"),
            (Scenario::S4ReviewFinding, 6, "s4_review_finding"),
            (Scenario::S4FixCheck, 7, "s4_fix_check"),
        ];
        for (sc, tag, name) in table {
            assert_eq!(sc.tag(), tag, "{name}");
            assert_eq!(serde_json::to_string(&sc).unwrap(), format!("\"{name}\""));
        }
    }

    #[test]
    fn validate_applies_the_gateways_structural_rules() {
        let one_opt = q(json!({"type":"choice","instructions":"pick","criteria":{"a":null}}));
        let c = case("one-opt", one_opt, Expected::Choice { option: "a".into() });
        assert!(c.validate().unwrap_err().to_string().contains("one-opt"));

        let blank = q(json!({"type":"noul","instructions":"   "}));
        let c = case("blank-instr", blank, Expected::Noul { value: true });
        let err = c.validate().unwrap_err().to_string();
        assert!(
            err.contains("blank-instr") && err.contains("instructions"),
            "{err}"
        );

        let mut c = case("null-state", noul_q(), Expected::Noul { value: true });
        c.state = serde_json::Value::Null;
        let err = c.validate().unwrap_err().to_string();
        assert!(err.contains("null-state") && err.contains("state"), "{err}");
    }

    fn line(c: &Case) -> String {
        serde_json::to_string(c).unwrap()
    }

    #[test]
    fn duplicate_ids_are_rejected_with_both_line_numbers() {
        let a = case("dup", noul_q(), Expected::Noul { value: true });
        let b = case("other", noul_q(), Expected::Noul { value: true });
        let f = tmp(&format!("{}\n{}\n{}\n", line(&a), line(&b), line(&a)));
        let err = format!("{:#}", read_case_file(f.path()).unwrap_err());
        assert!(err.contains("line 3"), "{err}");
        assert!(err.contains("duplicate case id \"dup\""), "{err}");
        assert!(err.contains("first seen on line 1"), "{err}");
    }

    #[test]
    fn a_file_mixing_scenarios_is_rejected() {
        let a = case("a", noul_q(), Expected::Noul { value: true });
        let mut b = case("b", noul_q(), Expected::Noul { value: true });
        b.scenario = Scenario::S3Entailment;
        let f = tmp(&format!("{}\n{}\n", line(&a), line(&b)));
        let err = format!("{:#}", read_case_file(f.path()).unwrap_err());
        assert!(err.contains("scenario") && err.contains("line 2"), "{err}");
    }

    #[test]
    fn a_header_parse_failure_explains_why_it_was_a_header() {
        let f = tmp("{\"labels_reviewd_by\": \"x\"}\n");
        let err = format!("{:#}", read_case_file(f.path()).unwrap_err());
        assert!(
            err.contains("line 1 has no id/scenario, so it was read as a header"),
            "{err}"
        );
    }

    #[test]
    fn a_case_with_an_unknown_field_is_rejected() {
        let c = case("c1", noul_q(), Expected::Noul { value: true });
        let mut v = serde_json::to_value(&c).unwrap();
        v["extra"] = json!(1);
        let f = tmp(&format!("{v}\n"));
        assert!(read_case_file(f.path()).is_err());
    }

    #[test]
    fn empty_and_header_only_files_are_errors_naming_the_path() {
        for content in ["", "\n  \n", "{\"notes\": \"x\"}\n"] {
            let f = tmp(content);
            let err = format!("{:#}", read_case_file(f.path()).unwrap_err());
            assert!(err.contains(&f.path().display().to_string()), "{err}");
            assert!(err.contains("no cases"), "{err}");
        }
    }

    #[test]
    fn a_leading_bom_is_ignored() {
        let c = case("c1", noul_q(), Expected::Noul { value: true });
        let f = tmp(&format!("\u{feff}{}\n", line(&c)));
        let (_, cs) = read_case_file(f.path()).unwrap();
        assert_eq!(cs, vec![c.clone()]);
        let f = tmp(&format!("\u{feff}{}\n", line(&c)));
        let back: Vec<Case> = read_jsonl(f.path()).unwrap();
        assert_eq!(back, vec![c]);
    }
}
