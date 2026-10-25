//! The committed `planner.select` cases load, validate, and say what they are.
//!
//! These labels are synthetic: the expected planner is the one whose `area`
//! matches the goal's `signals.area`. The guard that matters is that the file
//! never claims to be reviewed, and that every label is consistent with the
//! planner library it was generated from — a rotation that drifts would
//! otherwise turn the benchmark into noise.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;

use decision_bench::case::{Expected, Scenario, read_case_file};

/// The 12-area planner library the cases were generated from: name -> area.
const PLANNERS: &[(&str, &str)] = &[
    ("analysis-plan", "analysis"),
    ("code-plan", "code"),
    ("data-plan", "data"),
    ("docs-plan", "docs"),
    ("infra-plan", "infra"),
    ("migration-plan", "migration"),
    ("ops-plan", "ops"),
    ("research-plan", "research"),
    ("review-plan", "review"),
    ("security-plan", "security"),
    ("test-plan", "test"),
    ("writing-plan", "writing"),
];

fn area_of(name: &str) -> &str {
    PLANNERS
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, a)| *a)
        .unwrap_or_else(|| panic!("{name} is not in the planner library"))
}

#[test]
fn the_planner_cases_load_validate_and_are_honestly_labelled() {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("scenarios")
        .join("planner-select.jsonl");
    let (header, cases) =
        read_case_file(&path).unwrap_or_else(|e| panic!("{}: {e:#}", path.display()));

    assert_eq!(
        header.labels_reviewed_by, None,
        "synthetic labels must not name a reviewer"
    );
    let notes = header.notes.unwrap_or_default();
    assert!(
        notes.contains("SYNTHETIC") && notes.contains("not human-reviewed"),
        "the header must say the labels are synthetic: {notes}"
    );

    assert!(cases.len() >= 50, "{} cases", cases.len());
    let mut ids = HashSet::new();
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for c in &cases {
        assert!(ids.insert(c.id.clone()), "duplicate id {}", c.id);
        assert!(c.id.starts_with("planner-"), "id {}", c.id);
        assert_eq!(c.scenario, Scenario::PlannerSelect, "{}", c.id);

        let area = c
            .state
            .get("signals")
            .and_then(|s| s.get("area"))
            .and_then(|a| a.as_str())
            .unwrap_or_else(|| panic!("{}: no signals.area", c.id));
        let Expected::Choice { option } = &c.expected else {
            panic!("{}: planner.select is a choice", c.id);
        };
        assert_eq!(
            area_of(option),
            area,
            "{}: expected {option} but the goal's area is {area}",
            c.id
        );
        parameters_are_area_kind(c);
        *counts.entry(area_of(option)).or_default() += 1;
    }

    // Balanced by area: 20 goals x 3 menus, 12 areas (some areas appear twice
    // among the goals), so every area is between 3 and 10 cases.
    assert_eq!(counts.len(), PLANNERS.len(), "every area is exercised");
    for (area, n) in &counts {
        assert!((3..=10).contains(n), "{area}: {n} cases");
    }
}

/// Every option's description is `area/kind`, exactly as `DecisionPlannerSelector`
/// renders it from the registry.
fn parameters_are_area_kind(c: &decision_bench::case::Case) {
    let gateway::types::decision::DecisionQuestion::Choice { criteria, .. } = &c.question else {
        panic!("{}: planner.select is a Choice question", c.id);
    };
    for (agent, description) in criteria {
        let area = area_of(agent);
        assert_eq!(
            description.as_deref(),
            Some(format!("{area}/plan").as_str()),
            "{}: description of {agent}",
            c.id
        );
    }
}
