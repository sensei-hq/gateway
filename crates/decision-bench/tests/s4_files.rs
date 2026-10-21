//! The committed, hand-curated S4 gate files (SP-DEC-4 T9) load, validate,
//! and are balanced by label.
//!
//! This is also the guard for the limitation noted in `case.rs`: duplicate
//! keys inside a choice's `criteria` collapse silently on parse. Every raw
//! line must contain the gate's canonical question verbatim, so a duplicated
//! (or reordered, or reworded) criterion fails here.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;

use decision_bench::case::{Expected, Scenario, read_case_file};

const SOURCE: &str = "hand-curated 2026-10-20";

struct Gate {
    file: &'static str,
    scenario: Scenario,
    id_prefix: &'static str,
    labels: &'static [&'static str],
    question: &'static str,
}

const GATES: &[Gate] = &[
    Gate {
        file: "s4-test-output.jsonl",
        scenario: Scenario::S4TestOutput,
        id_prefix: "s4-test-",
        labels: &["continue", "fix_code", "retry", "fix_environment"],
        question: r#"{"type":"choice","instructions":"This is the output of a test run in a coding pipeline. What should the pipeline do next?","criteria":{"continue":"All tests passed; proceed to the next step","fix_code":"A test assertion failed or the code does not compile; the code must be corrected","retry":"The failure is transient or flaky (timeout, network, port in use); rerun unchanged","fix_environment":"The environment is broken (missing tool, disk full, permissions, missing dependency); the code is fine"}}"#,
    },
    Gate {
        file: "s4-review-finding.jsonl",
        scenario: Scenario::S4ReviewFinding,
        id_prefix: "s4-review-",
        labels: &["block", "follow_up", "dismiss"],
        question: r#"{"type":"choice","instructions":"A code reviewer left this finding on a pull request. How should the pipeline treat it?","criteria":{"block":"A real defect (bug, security hole, data loss, wrong result); must be fixed before merging","follow_up":"Valid but minor (naming, style, missing doc, small refactor); merge now and track it","dismiss":"Not a real problem; the reviewer misread the code or the concern is already handled"}}"#,
    },
    Gate {
        file: "s4-fix-check.jsonl",
        scenario: Scenario::S4FixCheck,
        id_prefix: "s4-fix-",
        labels: &["true", "false"],
        question: r#"{"type":"noul","instructions":"Given the review finding and the follow-up change, does the change actually fix the finding?"}"#,
    },
];

fn label(e: &Expected) -> String {
    match e {
        Expected::Choice { option } => option.clone(),
        Expected::Noul { value } => value.to_string(),
        Expected::Score { level } => format!("score:{level}"),
    }
}

#[test]
fn the_s4_gate_files_load_validate_and_are_balanced() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scenarios");
    for gate in GATES {
        let path = dir.join(gate.file);
        let (header, cases) =
            read_case_file(&path).unwrap_or_else(|e| panic!("{}: {e:#}", path.display()));
        let f = gate.file;

        assert_eq!(
            header.labels_reviewed_by, None,
            "{f}: labels are unreviewed"
        );
        assert_eq!(cases.len(), 60, "{f}: case count");

        let mut ids = HashSet::new();
        let mut counts: BTreeMap<String, usize> = BTreeMap::new();
        for c in &cases {
            assert!(ids.insert(c.id.clone()), "{f}: duplicate id {}", c.id);
            assert!(c.id.starts_with(gate.id_prefix), "{f}: id {}", c.id);
            assert_eq!(c.scenario, gate.scenario, "{f}: {}", c.id);
            assert_eq!(c.source, SOURCE, "{f}: {}", c.id);
            let state = serde_json::to_string(&c.state).unwrap();
            assert!(
                state.chars().count() < 650,
                "{f}: {} state is {} chars",
                c.id,
                state.chars().count()
            );
            *counts.entry(label(&c.expected)).or_default() += 1;
        }

        let uniform = 60.0 / gate.labels.len() as f64;
        let (lo, hi) = (uniform * 0.8, uniform * 1.2);
        let names: Vec<&str> = counts.keys().map(String::as_str).collect();
        let mut want: Vec<&str> = gate.labels.to_vec();
        want.sort_unstable();
        assert_eq!(names, want, "{f}: labels used");
        for (l, n) in &counts {
            let n = *n as f64;
            assert!(
                (lo..=hi).contains(&n),
                "{f}: label {l} has {n} cases, outside {lo}..={hi}"
            );
        }

        // Raw-text guard: each case line carries the canonical question
        // verbatim (catches duplicate criteria keys, which parse silently).
        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
        assert_eq!(lines.len(), 61, "{f}: header + 60 case lines");
        for line in &lines[1..] {
            let needle = format!("\"question\":{}", gate.question);
            assert!(
                line.contains(&needle),
                "{f}: non-canonical question in {line}"
            );
        }
    }
}
