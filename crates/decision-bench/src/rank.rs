//! Per-scenario ranking (SP-DEC-4 D6): coverage floor, shared-subset
//! head-to-head, and a CI-overlap tie rule.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::case::Scenario;
use crate::metrics::{Metrics, compute_on};
use crate::run::{Outcome, ResultRow};

/// A model below this coverage is reported separately, not ranked.
pub const COVERAGE_FLOOR: f64 = 0.5;
/// Fewer shared cases than this cannot name a winner.
pub const MIN_SHARED_CASES: usize = 20;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum Verdict {
    Winner(String),
    Tie(Vec<String>),
    Insufficient { reason: String },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct ScenarioRanking {
    pub scenario: Scenario,
    /// Cases applicable to every head-to-head model.
    pub shared_cases: usize,
    /// Head-to-head models, best first, metrics over the shared cases.
    pub ranked: Vec<(String, Metrics)>,
    pub verdict: Verdict,
    /// Models under the coverage floor, metrics over their own rows.
    pub low_coverage: Vec<(String, Metrics)>,
    /// Roster models with no rows at all for this scenario.
    pub no_rows: Vec<String>,
}

/// Rank `models` (the configured roster order) on one scenario.
///
/// Rows of other scenarios and of models outside `models` are ignored. A
/// roster model with no rows here goes to `no_rows`; one under
/// [`COVERAGE_FLOOR`] goes to `low_coverage` and never shrinks the shared set.
/// Every head-to-head model is scored with the same `seed`.
pub fn rank(
    scenario: Scenario,
    rows: &[ResultRow],
    models: &[String],
    seed: u64,
) -> ScenarioRanking {
    let mut no_rows = Vec::new();
    let mut low_coverage = Vec::new();
    // (model, its own rows) for every model at or above the floor.
    let mut contenders: Vec<(String, Vec<ResultRow>)> = Vec::new();

    for model in models {
        let own: Vec<ResultRow> = rows
            .iter()
            .filter(|r| r.scenario == scenario && &r.model == model)
            .cloned()
            .collect();
        if own.is_empty() {
            no_rows.push(model.clone());
            continue;
        }
        let m = compute_on(&own, None, seed);
        if m.coverage < COVERAGE_FLOOR {
            low_coverage.push((model.clone(), m));
        } else {
            contenders.push((model.clone(), own));
        }
    }

    let shared = shared_cases(&contenders);
    let mut ranked: Vec<(String, Metrics)> = contenders
        .iter()
        .map(|(m, own)| (m.clone(), compute_on(own, Some(&shared), seed)))
        .collect();
    ranked.sort_by(|(ia, a), (ib, b)| {
        desc_none_last(a.accuracy, b.accuracy)
            .then_with(|| asc_none_last(a.confident_wrong, b.confident_wrong))
            .then_with(|| asc_none_last(a.p50_ms, b.p50_ms))
            .then_with(|| ia.cmp(ib))
    });

    let verdict = verdict(&ranked, shared.len());
    ScenarioRanking {
        scenario,
        shared_cases: shared.len(),
        ranked,
        verdict,
        low_coverage,
        no_rows,
    }
}

/// Rank every scenario present in `rows`, in `Scenario` enum order.
pub fn rank_all(rows: &[ResultRow], models: &[String], seed: u64) -> Vec<ScenarioRanking> {
    let mut scenarios: Vec<Scenario> = Vec::new();
    for r in rows {
        if !scenarios.contains(&r.scenario) {
            scenarios.push(r.scenario);
        }
    }
    scenarios.sort_by_key(Scenario::tag);
    scenarios
        .into_iter()
        .map(|s| rank(s, rows, models, seed))
        .collect()
}

/// Case ids applicable (not `NotApplicable`) for every contender. A case a
/// contender has no row for is not applicable to it.
fn shared_cases(contenders: &[(String, Vec<ResultRow>)]) -> HashSet<String> {
    let mut sets = contenders.iter().map(|(_, rows)| {
        rows.iter()
            .filter(|r| r.outcome != Outcome::NotApplicable)
            .map(|r| r.case_id.clone())
            .collect::<HashSet<String>>()
    });
    let Some(first) = sets.next() else {
        return HashSet::new();
    };
    sets.fold(first, |acc, s| acc.intersection(&s).cloned().collect())
}

fn verdict(ranked: &[(String, Metrics)], shared: usize) -> Verdict {
    let insufficient = |reason: String| Verdict::Insufficient { reason };
    if ranked.len() < 2 {
        return insufficient(format!(
            "{} model(s) at or above the {COVERAGE_FLOOR} coverage floor; need 2",
            ranked.len()
        ));
    }
    if shared < MIN_SHARED_CASES {
        return insufficient(format!(
            "{shared} shared cases; need at least {MIN_SHARED_CASES}"
        ));
    }
    let Some(cis) = ranked
        .iter()
        .map(|(_, m)| m.ci)
        .collect::<Option<Vec<(f64, f64)>>>()
    else {
        return insufficient("a ranked model has no confidence interval".into());
    };
    let top_lo = cis[0].0;
    if top_lo > cis[1].1 {
        return Verdict::Winner(ranked[0].0.clone());
    }
    let tied = ranked
        .iter()
        .zip(&cis)
        .enumerate()
        // Overlaps the top: its upper bound reaches the top's lower bound.
        // (The exact negation of the winner test.)
        .filter(|(i, (_, (_, hi)))| *i == 0 || *hi >= top_lo)
        .map(|(_, ((m, _), _))| m.clone())
        .collect();
    Verdict::Tie(tied)
}

fn desc_none_last<T: PartialOrd>(a: Option<T>, b: Option<T>) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) {
        (Some(a), Some(b)) => b.partial_cmp(&a).unwrap_or(Equal),
        (Some(_), None) => Less,
        (None, Some(_)) => Greater,
        (None, None) => Equal,
    }
}

fn asc_none_last<T: PartialOrd>(a: Option<T>, b: Option<T>) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) {
        (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(Equal),
        (Some(_), None) => Less,
        (None, Some(_)) => Greater,
        (None, None) => Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(
        model: &str,
        case: usize,
        outcome: Outcome,
        correct: Option<bool>,
        p: f64,
        lat: u64,
    ) -> ResultRow {
        ResultRow {
            case_id: format!("c{case}"),
            scenario: Scenario::S3Entailment,
            model: model.into(),
            outcome,
            correct,
            top_probability: correct.map(|_| p),
            score_value: None,
            expected_level: None,
            latency_ms: lat,
            reached_server: true,
            detail: None,
        }
    }

    fn good(model: &str, case: usize) -> ResultRow {
        mk(model, case, Outcome::Ok, Some(true), 0.6, 10)
    }

    fn bad(model: &str, case: usize) -> ResultRow {
        mk(model, case, Outcome::Ok, Some(false), 0.6, 10)
    }

    fn na(model: &str, case: usize) -> ResultRow {
        mk(model, case, Outcome::NotApplicable, None, 0.0, 0)
    }

    /// `n` cases; the first `correct` are right, the rest wrong.
    fn graded(model: &str, n: usize, correct: usize) -> Vec<ResultRow> {
        (1..=n)
            .map(|i| {
                if i <= correct {
                    good(model, i)
                } else {
                    bad(model, i)
                }
            })
            .collect()
    }

    fn ids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn ranked_ids(r: &ScenarioRanking) -> Vec<&str> {
        r.ranked.iter().map(|(m, _)| m.as_str()).collect()
    }

    #[test]
    fn the_head_to_head_uses_only_cases_applicable_to_every_ranked_model() {
        let mut rows: Vec<ResultRow> = (1..=30).map(|i| good("A", i)).collect();
        for i in 1..=30 {
            rows.push(match i {
                3 => na("B", i),
                5 => bad("B", i),
                _ => good("B", i),
            });
        }
        let r = rank(Scenario::S3Entailment, &rows, &ids(&["A", "B"]), 7);
        assert_eq!(r.shared_cases, 29);
        let a = &r.ranked.iter().find(|(m, _)| m == "A").unwrap().1;
        let b = &r.ranked.iter().find(|(m, _)| m == "B").unwrap().1;
        assert_eq!(a.n_total, 29, "A is scored on the shared 29, not its 30");
        assert_eq!(b.n_total, 29);
        assert_eq!(a.accuracy, Some(1.0));
        assert_eq!(b.accuracy, Some(28.0 / 29.0));
        assert_eq!(ranked_ids(&r), vec!["A", "B"]);
    }

    #[test]
    fn a_model_under_half_coverage_is_reported_separately() {
        let mut rows: Vec<ResultRow> = (1..=30).map(|i| good("A", i)).collect();
        rows.extend((1..=30).map(|i| good("B", i)));
        rows.extend((1..=30).map(|i| if i <= 20 { na("C", i) } else { good("C", i) }));
        let r = rank(Scenario::S3Entailment, &rows, &ids(&["A", "B", "C"]), 7);
        assert_eq!(ranked_ids(&r), vec!["A", "B"]);
        assert_eq!(r.shared_cases, 30, "C must not shrink the shared set");
        assert_eq!(r.low_coverage.len(), 1);
        assert_eq!(r.low_coverage[0].0, "C");
        assert!((r.low_coverage[0].1.coverage - 10.0 / 30.0).abs() < 1e-9);
        assert_eq!(r.low_coverage[0].1.n_total, 30, "own rows, not the subset");
    }

    #[test]
    fn a_model_that_is_not_applicable_everywhere_is_low_coverage_not_dropped() {
        let mut rows = graded("A", 30, 30);
        rows.extend((1..=30).map(|i| na("D", i)));
        let r = rank(Scenario::S3Entailment, &rows, &ids(&["A", "D"]), 7);
        assert_eq!(r.low_coverage.len(), 1);
        assert_eq!(r.low_coverage[0].0, "D");
        assert_eq!(r.low_coverage[0].1.coverage, 0.0);
        assert!(r.no_rows.is_empty());
    }

    #[test]
    fn exactly_half_coverage_is_ranked() {
        let mut rows = graded("A", 30, 30);
        rows.extend((1..=30).map(|i| if i <= 15 { na("B", i) } else { good("B", i) }));
        let r = rank(Scenario::S3Entailment, &rows, &ids(&["A", "B"]), 7);
        assert_eq!(ranked_ids(&r), vec!["A", "B"]);
        assert_eq!(r.shared_cases, 15);
    }

    #[test]
    fn separated_cis_name_a_winner() {
        let mut rows = graded("A", 100, 95);
        rows.extend(graded("B", 100, 50));
        let r = rank(Scenario::S3Entailment, &rows, &ids(&["A", "B"]), 7);
        assert_eq!(r.verdict, Verdict::Winner("A".into()));
    }

    #[test]
    fn overlapping_cis_tie() {
        let mut rows = graded("A", 100, 80);
        rows.extend(graded("B", 100, 78));
        let r = rank(Scenario::S3Entailment, &rows, &ids(&["A", "B"]), 7);
        assert_eq!(r.verdict, Verdict::Tie(ids(&["A", "B"])));
    }

    #[test]
    fn two_perfect_models_touching_at_one_are_a_tie_not_a_winner() {
        let mut rows = graded("A", 30, 30);
        rows.extend(graded("B", 30, 30));
        let r = rank(Scenario::S3Entailment, &rows, &ids(&["A", "B"]), 7);
        assert_eq!(r.verdict, Verdict::Tie(ids(&["A", "B"])));
    }

    #[test]
    fn a_tie_lists_only_models_whose_ci_overlaps_the_top() {
        let mut rows = graded("A", 100, 80);
        rows.extend(graded("B", 100, 78));
        rows.extend(graded("C", 100, 30));
        let r = rank(Scenario::S3Entailment, &rows, &ids(&["A", "B", "C"]), 7);
        assert_eq!(r.verdict, Verdict::Tie(ids(&["A", "B"])));
    }

    #[test]
    fn fewer_than_20_shared_cases_is_insufficient() {
        let mut rows = graded("A", 19, 19);
        rows.extend(graded("B", 19, 0));
        let r = rank(Scenario::S3Entailment, &rows, &ids(&["A", "B"]), 7);
        assert_eq!(r.shared_cases, 19);
        assert!(matches!(r.verdict, Verdict::Insufficient { .. }));
    }

    #[test]
    fn a_single_ranked_model_is_insufficient() {
        let rows = graded("A", 40, 40);
        let r = rank(Scenario::S3Entailment, &rows, &ids(&["A"]), 7);
        assert_eq!(ranked_ids(&r), vec!["A"]);
        assert!(matches!(r.verdict, Verdict::Insufficient { .. }));
    }

    #[test]
    fn ordering_is_accuracy_then_confident_wrong_then_p50_then_id() {
        let build = |model: &str, correct: usize, wrong_p: f64, lat: u64| -> Vec<ResultRow> {
            (1..=20)
                .map(|i| {
                    if i <= correct {
                        mk(model, i, Outcome::Ok, Some(true), 0.6, lat)
                    } else {
                        mk(model, i, Outcome::Ok, Some(false), wrong_p, lat)
                    }
                })
                .collect()
        };
        let mut rows = Vec::new();
        rows.extend(build("top", 20, 0.6, 10));
        rows.extend(build("bb", 18, 0.6, 10));
        rows.extend(build("aa", 18, 0.6, 10));
        rows.extend(build("slow", 18, 0.6, 50));
        rows.extend(build("conf", 18, 0.99, 5));
        rows.extend(build("low", 10, 0.6, 1));
        let roster = ids(&["low", "conf", "slow", "bb", "aa", "top"]);
        let r = rank(Scenario::S3Entailment, &rows, &roster, 7);
        assert_eq!(
            ranked_ids(&r),
            vec!["top", "aa", "bb", "slow", "conf", "low"]
        );
    }

    #[test]
    fn a_model_with_no_rows_is_listed_not_dropped() {
        let rows = graded("A", 30, 30);
        let r = rank(Scenario::S3Entailment, &rows, &ids(&["A", "ghost"]), 7);
        assert_eq!(r.no_rows, ids(&["ghost"]));
        assert_eq!(ranked_ids(&r), vec!["A"]);
        assert!(r.low_coverage.is_empty());
    }

    #[test]
    fn rows_of_other_scenarios_are_ignored() {
        let mut rows = graded("A", 30, 30);
        rows.extend((1..=30).map(|i| ResultRow {
            scenario: Scenario::S2Sentiment,
            ..bad("A", i)
        }));
        let r = rank(Scenario::S3Entailment, &rows, &ids(&["A"]), 7);
        assert_eq!(r.ranked[0].1.n_total, 30);
        assert_eq!(r.ranked[0].1.accuracy, Some(1.0));
    }

    #[test]
    fn rank_all_is_in_scenario_enum_order() {
        let mut rows = Vec::new();
        for s in [
            Scenario::S4FixCheck,
            Scenario::S1aIntentOos,
            Scenario::S2Sentiment,
        ] {
            rows.extend(
                graded("A", 3, 3)
                    .into_iter()
                    .map(|r| ResultRow { scenario: s, ..r }),
            );
        }
        let all = rank_all(&rows, &ids(&["A"]), 7);
        let order: Vec<Scenario> = all.iter().map(|r| r.scenario).collect();
        assert_eq!(
            order,
            vec![
                Scenario::S1aIntentOos,
                Scenario::S2Sentiment,
                Scenario::S4FixCheck
            ]
        );
    }
}
