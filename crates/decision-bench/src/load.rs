//! Load the cases a `run` uses: the fetched `cases/*.jsonl` (+ its
//! `manifest.json`) and the hand-curated `scenarios/s4-*.jsonl`, filtered by
//! scenario.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};

use crate::case::{Case, Scenario, read_case_file};
use crate::run::LabelReview;
use crate::scenarios::Manifest;

/// What [`load`] found.
#[derive(Debug, Clone, PartialEq)]
pub struct Loaded {
    /// In scenario order, then file order.
    pub cases: Vec<Case>,
    /// `cases/manifest.json`, if present.
    pub manifest: Option<Manifest>,
    /// Each loaded S4 scenario with its header's `labels_reviewed_by`.
    pub labels_reviewed: Vec<LabelReview>,
    /// Scenarios asked for by name that no file provided.
    pub missing: Vec<Scenario>,
}

/// `--scenarios` names: `s1a`, `s1b`, `s2`, `s3`, `s4` (all three gates),
/// `s4-test-output`, `s4-review-finding`, `s4-fix-check`, or the serde names
/// (`s3_entailment`, …). Comma separated; an unknown name is an error.
pub fn parse_scenarios(list: &str) -> anyhow::Result<Vec<Scenario>> {
    use Scenario::*;
    let mut out = Vec::new();
    for name in list.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        let picked: &[Scenario] = match name {
            "s1a" | "s1a_intent_oos" => &[S1aIntentOos],
            "s1b" | "s1b_banking77" => &[S1bBanking77],
            "s2" | "s2_sentiment" => &[S2Sentiment],
            "s3" | "s3_entailment" => &[S3Entailment],
            "s4" => &[S4TestOutput, S4ReviewFinding, S4FixCheck],
            "s4-test-output" | "s4_test_output" => &[S4TestOutput],
            "s4-review-finding" | "s4_review_finding" => &[S4ReviewFinding],
            "s4-fix-check" | "s4_fix_check" => &[S4FixCheck],
            other => bail!(
                "unknown scenario '{other}' (expected s1a, s1b, s2, s3, s4, s4-test-output, \
                 s4-review-finding or s4-fix-check)"
            ),
        };
        for s in picked {
            if !out.contains(s) {
                out.push(*s);
            }
        }
    }
    if out.is_empty() {
        bail!("--scenarios names no scenario");
    }
    Ok(out)
}

pub fn is_s4(s: Scenario) -> bool {
    matches!(
        s,
        Scenario::S4TestOutput | Scenario::S4ReviewFinding | Scenario::S4FixCheck
    )
}

/// `*.jsonl` files in `dir` whose name starts with `prefix`, sorted by name.
/// A missing directory has none.
fn jsonl_files(dir: &Path, prefix: &str) -> anyhow::Result<Vec<PathBuf>> {
    if !dir.exists() {
        return Ok(vec![]);
    }
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("listing {}", dir.display()))? {
        let path = entry
            .with_context(|| format!("listing {}", dir.display()))?
            .path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if path.is_file() && name.starts_with(prefix) && name.ends_with(".jsonl") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

/// Load every case file from `cases_dir` (`*.jsonl`) and `s4_dir`
/// (`s4-*.jsonl`; a missing dir is fine), keeping the scenarios in `only`
/// (all when `None`). Errors: an unreadable or invalid file, a case id seen
/// twice within one scenario, and a selection that yields no case at all.
pub fn load(cases_dir: &Path, s4_dir: &Path, only: Option<&[Scenario]>) -> anyhow::Result<Loaded> {
    let keep = |s: Scenario| only.is_none_or(|o| o.contains(&s));
    let mut files = jsonl_files(cases_dir, "")?;
    files.extend(jsonl_files(s4_dir, "s4-")?);

    let mut loaded: Vec<(Scenario, Option<String>, Vec<Case>, bool)> = Vec::new();
    for path in &files {
        let (header, cases) = read_case_file(path)?;
        let scenario = cases[0].scenario;
        let hand_curated = path.starts_with(s4_dir);
        if keep(scenario) {
            loaded.push((scenario, header.labels_reviewed_by, cases, hand_curated));
        }
    }
    loaded.sort_by_key(|(s, ..)| s.tag());

    let mut seen: HashSet<(u64, String)> = HashSet::new();
    let mut out = Loaded {
        cases: vec![],
        manifest: None,
        labels_reviewed: vec![],
        missing: vec![],
    };
    for (scenario, reviewed_by, cases, hand_curated) in loaded {
        if hand_curated || is_s4(scenario) {
            out.labels_reviewed.push(LabelReview {
                scenario,
                reviewed_by,
            });
        }
        for c in cases {
            if !seen.insert((scenario.tag(), c.id.clone())) {
                bail!(
                    "case id {:?} appears in more than one {scenario:?} file",
                    c.id
                );
            }
            out.cases.push(c);
        }
    }
    if out.cases.is_empty() {
        bail!(
            "no cases to run: {} has no case files for the selected scenarios (run \
             `decision-bench fetch` first), and {} has no matching s4-*.jsonl",
            cases_dir.display(),
            s4_dir.display()
        );
    }
    if let Some(o) = only {
        out.missing = o
            .iter()
            .copied()
            .filter(|s| !out.cases.iter().any(|c| c.scenario == *s))
            .collect();
    }
    let manifest_path = cases_dir.join("manifest.json");
    if manifest_path.exists() {
        let text = std::fs::read_to_string(&manifest_path)
            .with_context(|| format!("reading {}", manifest_path.display()))?;
        out.manifest = Some(
            serde_json::from_str(&text)
                .with_context(|| format!("parsing {}", manifest_path.display()))?,
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::case::{Expected, Header, write_jsonl};
    use gateway::types::decision::DecisionQuestion;
    use serde_json::json;

    fn noul(id: &str, scenario: Scenario) -> Case {
        Case {
            id: id.into(),
            scenario,
            state: json!("s"),
            question: DecisionQuestion::Noul {
                instructions: json!("holds?"),
                criteria: None,
            },
            expected: Expected::Noul { value: true },
            source: "test".into(),
        }
    }

    fn write_cases(path: &Path, cases: &[Case]) {
        write_jsonl(path, cases).unwrap();
    }

    fn write_s4(path: &Path, reviewed_by: Option<&str>, cases: &[Case]) {
        let header = Header {
            labels_reviewed_by: reviewed_by.map(str::to_owned),
            notes: Some("test".into()),
        };
        let mut text = serde_json::to_string(&header).unwrap() + "\n";
        for c in cases {
            text += &(serde_json::to_string(c).unwrap() + "\n");
        }
        std::fs::write(path, text).unwrap();
    }

    fn manifest_json() -> serde_json::Value {
        json!({
            "seed": 2026, "per_scenario": 2, "rand_version": "0.8.5",
            "datasets": [], "files": []
        })
    }

    /// cases/ with s3 (2 cases) + manifest; scenarios/ with two S4 files and a
    /// non-s4 file that must be ignored.
    fn layout() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let root = tempfile::tempdir().unwrap();
        let cases = root.path().join("cases");
        let s4 = root.path().join("scenarios");
        std::fs::create_dir_all(&cases).unwrap();
        std::fs::create_dir_all(&s4).unwrap();
        write_cases(
            &cases.join("s3.jsonl"),
            &[
                noul("a", Scenario::S3Entailment),
                noul("b", Scenario::S3Entailment),
            ],
        );
        std::fs::write(cases.join("manifest.json"), manifest_json().to_string()).unwrap();
        write_s4(
            &s4.join("s4-fix-check.jsonl"),
            None,
            &[noul("f1", Scenario::S4FixCheck)],
        );
        write_s4(
            &s4.join("s4-test-output.jsonl"),
            Some("jerry"),
            &[noul("t1", Scenario::S4TestOutput)],
        );
        write_cases(
            &s4.join("notes.jsonl"),
            &[noul("x", Scenario::S1bBanking77)],
        );
        (root, cases, s4)
    }

    fn ids(l: &Loaded) -> Vec<&str> {
        l.cases.iter().map(|c| c.id.as_str()).collect()
    }

    #[test]
    fn loads_fetched_and_s4_cases_in_scenario_order_with_label_review_and_manifest() {
        let (_root, cases, s4) = layout();
        let l = load(&cases, &s4, None).unwrap();
        assert_eq!(ids(&l), ["a", "b", "t1", "f1"]);
        assert_eq!(
            l.labels_reviewed,
            vec![
                LabelReview {
                    scenario: Scenario::S4TestOutput,
                    reviewed_by: Some("jerry".to_string())
                },
                LabelReview {
                    scenario: Scenario::S4FixCheck,
                    reviewed_by: None
                }
            ]
        );
        assert_eq!(l.manifest.unwrap().seed, 2026);
        assert!(l.missing.is_empty());
    }

    #[test]
    fn a_missing_s4_dir_is_fine() {
        let (root, cases, _s4) = layout();
        let l = load(&cases, &root.path().join("nope"), None).unwrap();
        assert_eq!(ids(&l), ["a", "b"]);
        assert!(l.labels_reviewed.is_empty());
    }

    #[test]
    fn the_scenario_filter_keeps_only_the_named_scenarios_and_reports_missing_ones() {
        let (_root, cases, s4) = layout();
        let only = parse_scenarios("s4-fix-check,s1a").unwrap();
        let l = load(&cases, &s4, Some(&only)).unwrap();
        assert_eq!(ids(&l), ["f1"]);
        assert_eq!(
            l.labels_reviewed,
            vec![LabelReview {
                scenario: Scenario::S4FixCheck,
                reviewed_by: None
            }]
        );
        assert_eq!(l.missing, vec![Scenario::S1aIntentOos]);
    }

    #[test]
    fn a_selection_with_no_cases_is_an_error_saying_to_fetch() {
        let root = tempfile::tempdir().unwrap();
        let err = load(&root.path().join("cases"), &root.path().join("s"), None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("fetch"), "{err}");
    }

    #[test]
    fn a_case_id_repeated_within_a_scenario_is_an_error() {
        let (_root, cases, s4) = layout();
        write_cases(
            &cases.join("s3-more.jsonl"),
            &[noul("a", Scenario::S3Entailment)],
        );
        let err = load(&cases, &s4, None).unwrap_err().to_string();
        assert!(err.contains("\"a\""), "{err}");
    }

    #[test]
    fn scenario_names_parse_and_s4_expands_to_every_gate() {
        use Scenario::*;
        assert_eq!(
            parse_scenarios("s1a, s3,s4").unwrap(),
            vec![
                S1aIntentOos,
                S3Entailment,
                S4TestOutput,
                S4ReviewFinding,
                S4FixCheck
            ]
        );
        assert_eq!(
            parse_scenarios("s2_sentiment,s2").unwrap(),
            vec![S2Sentiment]
        );
        assert!(
            parse_scenarios("s5")
                .unwrap_err()
                .to_string()
                .contains("s5")
        );
        assert!(parse_scenarios(" , ").is_err());
    }
}
