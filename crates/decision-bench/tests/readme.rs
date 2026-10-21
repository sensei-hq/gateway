//! The crate README agrees with the code it documents (SP-DEC-4 review C2–C4).

use std::path::{Component, Path, PathBuf};

use decision_bench::case::Scenario;
use decision_bench::config::resolve_under;
use decision_bench::rank::MIN_SHARED_CASES;
use decision_bench::run::{Outcome, ResultRow};

const README: &str = include_str!("../README.md");
const CRATE: &str = env!("CARGO_MANIFEST_DIR");

/// The README text from `heading` up to the next heading of any level.
fn section(heading: &str) -> &'static str {
    let start = README
        .find(heading)
        .unwrap_or_else(|| panic!("README has no {heading:?}"));
    let body = &README[start + heading.len()..];
    let end = body.find("\n#").unwrap_or(body.len());
    &body[..end]
}

/// `p` with `.` and `..` folded away, without touching the filesystem.
fn normalise(p: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            c => out.push(c),
        }
    }
    out
}

/// C3: the README names the `Insufficient` verdict and its shared-case bound.
#[test]
fn the_readme_documents_the_insufficient_verdict_and_its_bound() {
    assert!(
        README.contains("**Insufficient**"),
        "README lacks Insufficient"
    );
    let bound = format!("fewer than {MIN_SHARED_CASES} shared cases");
    assert!(README.contains(&bound), "README lacks {bound:?}");
}

/// C4: every key a results.json row carries is listed in the README.
#[test]
fn every_results_row_field_is_listed_in_the_readme() {
    let row = ResultRow {
        case_id: "c1".into(),
        scenario: Scenario::S3Entailment,
        model: "m".into(),
        outcome: Outcome::Ok,
        correct: Some(true),
        top_probability: Some(0.9),
        score_value: None,
        expected_level: None,
        answer: Some("yes".into()),
        probabilities: Some([("yes".to_string(), 0.9)].into()),
        latency_ms: 1,
        reached_server: true,
        detail: None,
    };
    let fields = section("### `results.json`");
    let json = serde_json::to_value(&row).unwrap();
    for key in json.as_object().unwrap().keys() {
        assert!(
            fields.contains(&format!("`{key}`")),
            "README's results.json section does not list `{key}`:\n{fields}"
        );
    }
}

/// C2: the README's commit-a-report command writes under `<repo>/docs/benchmarks`
/// once its `--out` is resolved the way `report` resolves it.
#[test]
fn the_readmes_committed_report_out_lands_in_docs_benchmarks() {
    let crate_dir = Path::new(CRATE);
    let benchmarks = normalise(&crate_dir.join("../../docs/benchmarks"));
    let outs: Vec<&str> = README
        .lines()
        .filter_map(|l| l.split_once("report --out ").map(|(_, rest)| rest))
        .map(|rest| rest.split([' ', '`']).next().unwrap())
        .collect();
    assert!(!outs.is_empty(), "README has no `report --out` example");
    for out in outs {
        let resolved = normalise(&resolve_under(Path::new(out), crate_dir));
        assert!(
            resolved.starts_with(&benchmarks),
            "`report --out {out}` writes {}, not under {}",
            resolved.display(),
            benchmarks.display()
        );
    }
}
