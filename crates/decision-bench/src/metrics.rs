//! Per-(scenario, model) metrics (spec D5). Pure functions over result rows.

use std::collections::HashSet;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use crate::run::{Outcome, ResultRow};

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Metrics {
    pub n_total: usize,
    pub n_applicable: usize,
    pub n_ok: usize,
    pub n_error: usize,
    /// Rows (of any outcome) that never reached the server.
    pub not_reached: usize,
    pub coverage: f64,
    pub accuracy: Option<f64>,
    pub ci: Option<(f64, f64)>,
    pub ece: Option<f64>,
    pub confident_wrong: Option<f64>,
    pub error_rate: Option<f64>,
    pub mae: Option<f64>,
    pub p50_ms: Option<u64>,
    pub p95_ms: Option<u64>,
}

/// Bootstrap resamples for the accuracy CI.
const RESAMPLES: usize = 1000;
/// Equal-width calibration bins over [0, 1].
const ECE_BINS: usize = 10;
/// A wrong answer at or above this top probability is "confidently wrong".
const CONFIDENT: f64 = 0.8;

/// Nearest-rank percentile of an ascending slice, `per_mille` in 0..=1000
/// (integer arithmetic, so 0.025 x 1000 cannot round across a rank):
/// the value at 1-based rank `ceil(per_mille * n / 1000)`, at least 1.
/// `None` for an empty slice.
fn nearest_rank<T: Copy>(sorted: &[T], per_mille: usize) -> Option<T> {
    if sorted.is_empty() {
        return None;
    }
    let rank = (per_mille * sorted.len()).div_ceil(1000).max(1);
    Some(sorted[rank - 1])
}

fn ratio(num: usize, den: usize) -> Option<f64> {
    (den > 0).then(|| num as f64 / den as f64)
}

/// 95% percentile-bootstrap CI of the mean of 0/1 `outcomes`: 1000 resamples
/// with replacement, then the nearest-rank 2.5th and 97.5th percentiles of the
/// sorted resample means. `None` below two outcomes.
fn bootstrap_ci(outcomes: &[u8], seed: u64) -> Option<(f64, f64)> {
    let n = outcomes.len();
    if n < 2 {
        return None;
    }
    let mut rng = StdRng::seed_from_u64(seed);
    let mut means: Vec<f64> = (0..RESAMPLES)
        .map(|_| {
            let ones: usize = (0..n)
                .map(|_| usize::from(outcomes[rng.gen_range(0..n)]))
                .sum();
            ones as f64 / n as f64
        })
        .collect();
    means.sort_by(f64::total_cmp);
    Some((nearest_rank(&means, 25)?, nearest_rank(&means, 975)?))
}

/// Expected calibration error over `(confidence, correct)` pairs.
fn ece(pairs: &[(f64, bool)]) -> Option<f64> {
    if pairs.is_empty() {
        return None;
    }
    // (count, correct, confidence sum) per bin; p = 1.0 lands in the last bin.
    let mut bins = [(0usize, 0usize, 0.0f64); ECE_BINS];
    for &(p, correct) in pairs {
        let i = ((p * ECE_BINS as f64) as usize).min(ECE_BINS - 1);
        bins[i].0 += 1;
        bins[i].1 += usize::from(correct);
        bins[i].2 += p;
    }
    let n = pairs.len() as f64;
    Some(
        bins.iter()
            .filter(|b| b.0 > 0)
            .map(|&(c, k, conf)| {
                let c = c as f64;
                (c / n) * (k as f64 / c - conf / c).abs()
            })
            .sum(),
    )
}

/// Metrics for one (scenario, model) row set. Takes references so a ranking
/// can pass a shared subset without cloning.
///
/// - Applicable = not `NotApplicable`. `Error` rows are applicable and wrong.
/// - Accuracy, error rate, confident-wrong and the CI use applicable rows.
/// - ECE, MAE and the latency percentiles use `Ok` rows only (a refused or
///   gated row takes ~0 ms and has no probability).
pub fn compute(rows: &[&ResultRow], seed: u64) -> Metrics {
    let n_total = rows.len();
    let applicable: Vec<&&ResultRow> = rows
        .iter()
        .filter(|r| r.outcome != Outcome::NotApplicable)
        .collect();
    let n_applicable = applicable.len();
    let oks: Vec<&&ResultRow> = applicable
        .iter()
        .copied()
        .filter(|r| r.outcome == Outcome::Ok)
        .collect();
    let n_ok = oks.len();
    let errors: Vec<&&ResultRow> = applicable
        .iter()
        .copied()
        .filter(|r| r.outcome == Outcome::Error)
        .collect();
    let n_error = errors.len();
    // Every row that never reached the server: gated errors and gated
    // not_applicable rows alike.
    let not_reached = rows.iter().filter(|r| !r.reached_server).count();

    let is_correct = |r: &ResultRow| r.outcome == Outcome::Ok && r.correct == Some(true);
    let outcomes: Vec<u8> = applicable.iter().map(|r| u8::from(is_correct(r))).collect();
    let n_correct = outcomes.iter().filter(|&&o| o == 1).count();

    let calibration: Vec<(f64, bool)> = oks
        .iter()
        .filter_map(|r| Some((r.top_probability?, r.correct?)))
        .collect();
    let confident_wrong = oks
        .iter()
        .filter(|r| r.correct == Some(false) && r.top_probability.is_some_and(|p| p >= CONFIDENT))
        .count();

    let gaps: Vec<f64> = oks
        .iter()
        .filter_map(|r| Some((r.score_value? - r.expected_level? as f64).abs()))
        .collect();

    let mut latencies: Vec<u64> = oks.iter().map(|r| r.latency_ms).collect();
    latencies.sort_unstable();

    Metrics {
        n_total,
        n_applicable,
        n_ok,
        n_error,
        not_reached,
        coverage: ratio(n_applicable, n_total).unwrap_or(0.0),
        accuracy: ratio(n_correct, n_applicable),
        ci: bootstrap_ci(&outcomes, seed),
        ece: ece(&calibration),
        confident_wrong: ratio(confident_wrong, n_applicable),
        error_rate: ratio(n_error, n_applicable),
        mae: (!gaps.is_empty()).then(|| gaps.iter().sum::<f64>() / gaps.len() as f64),
        p50_ms: nearest_rank(&latencies, 500),
        p95_ms: nearest_rank(&latencies, 950),
    }
}

/// [`compute`] over owned rows, optionally restricted to `case_ids` (the
/// head-to-head's shared subset). `None` keeps every row. The kept rows are
/// sorted by `case_id` first, so the bootstrap CI does not depend on the order
/// the rows arrived in.
pub fn compute_on(rows: &[ResultRow], case_ids: Option<&HashSet<String>>, seed: u64) -> Metrics {
    let mut kept: Vec<&ResultRow> = rows
        .iter()
        .filter(|r| case_ids.is_none_or(|ids| ids.contains(&r.case_id)))
        .collect();
    kept.sort_by(|a, b| a.case_id.cmp(&b.case_id));
    compute(&kept, seed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::case::Scenario;

    fn row(outcome: Outcome) -> ResultRow {
        ResultRow {
            case_id: String::new(),
            scenario: Scenario::S2Sentiment,
            model: "m".into(),
            outcome,
            correct: None,
            top_probability: None,
            score_value: None,
            expected_level: None,
            latency_ms: 0,
            reached_server: true,
            detail: None,
        }
    }

    fn ok(correct: bool, p: f64) -> ResultRow {
        ResultRow {
            correct: Some(correct),
            top_probability: Some(p),
            ..row(Outcome::Ok)
        }
    }

    fn na() -> ResultRow {
        row(Outcome::NotApplicable)
    }

    fn err() -> ResultRow {
        row(Outcome::Error)
    }

    fn m(rows: &[ResultRow]) -> Metrics {
        let refs: Vec<&ResultRow> = rows.iter().collect();
        compute(&refs, 7)
    }

    fn close(a: Option<f64>, b: f64) -> bool {
        a.is_some_and(|a| (a - b).abs() < 1e-12)
    }

    #[test]
    fn coverage_counts_not_applicable_out() {
        let mut rows: Vec<ResultRow> = (0..7).map(|_| ok(true, 0.9)).collect();
        rows.extend((0..3).map(|_| na()));
        let x = m(&rows);
        assert_eq!((x.n_total, x.n_applicable), (10, 7));
        assert!((x.coverage - 0.7).abs() < 1e-12);
    }

    #[test]
    fn accuracy_is_over_applicable_and_errors_count_wrong() {
        let mut rows: Vec<ResultRow> = (0..5).map(|_| ok(true, 0.9)).collect();
        rows.push(ok(false, 0.6));
        rows.push(err());
        rows.extend((0..3).map(|_| na()));
        let x = m(&rows);
        assert_eq!((x.n_applicable, x.n_ok, x.n_error), (7, 6, 1));
        assert!(close(x.accuracy, 5.0 / 7.0));
        assert!(close(x.error_rate, 1.0 / 7.0));
    }

    /// T7 review M1: every row that never reached the server, gated
    /// not_applicable ones included (so the report's column and its "never
    /// reached the server" lines agree).
    #[test]
    fn not_reached_counts_every_row_that_never_reached_the_server() {
        let gated = ResultRow {
            reached_server: false,
            ..err()
        };
        let gated_na = ResultRow {
            reached_server: false,
            ..na()
        };
        let rows = vec![gated.clone(), gated, err(), ok(true, 0.9), gated_na, na()];
        let x = m(&rows);
        assert_eq!((x.n_error, x.not_reached), (3, 3));
    }

    #[test]
    fn bootstrap_ci_is_seeded_and_brackets_the_mean() {
        let data = [1, 1, 1, 0, 1, 0, 1, 1, 1, 1];
        let rows: Vec<ResultRow> = data.iter().map(|&d| ok(d == 1, 0.9)).collect();
        let a = m(&rows);
        let b = m(&rows);
        assert_eq!(a.ci, b.ci);
        let (lo, hi) = a.ci.expect("ci");
        assert!(lo <= 0.8 && 0.8 <= hi, "({lo}, {hi})");
        // Pinned once from this implementation (seed 7, 1000 resamples,
        // nearest-rank 2.5th/97.5th): any change to the resampling or the
        // percentile rule moves it.
        assert_eq!((lo, hi), PINNED_CI);
    }

    const PINNED_CI: (f64, f64) = (0.5, 1.0);

    #[test]
    fn the_seed_drives_the_resampling_not_the_point_estimate() {
        // n = 200 gives a fine CI grid (steps of 0.005), so two seeds that
        // resample differently land on different bounds; the n = 10 grid is
        // too coarse to tell seeds apart.
        let rows: Vec<ResultRow> = (0..200).map(|i| ok(i % 5 != 0, 0.9)).collect();
        let refs: Vec<&ResultRow> = rows.iter().collect();
        let base = compute(&refs, 1);
        assert_eq!(base.ci, compute(&refs, 1).ci);
        // Two seeds can still land on the same two grid points by chance, so
        // require that some seed in a small set moves the CI, and that none
        // moves the point estimate.
        let others: Vec<Metrics> = (2..12).map(|s| compute(&refs, s)).collect();
        assert!(others.iter().all(|o| o.accuracy == base.accuracy));
        assert!(others.iter().any(|o| o.ci != base.ci));
    }

    #[test]
    fn ci_needs_two_applicable_rows() {
        assert_eq!(m(&[ok(true, 0.9)]).ci, None);
        assert!(m(&[ok(true, 0.9), ok(false, 0.9)]).ci.is_some());
    }

    #[test]
    fn ece_hand_computed() {
        let rows = vec![
            ok(true, 0.95),
            ok(false, 0.95),
            ok(true, 0.55),
            ok(true, 0.55),
        ];
        // bin 9: acc 0.5 conf 0.95 -> 0.5*0.45; bin 5: acc 1 conf 0.55 -> 0.5*0.45
        assert!(close(m(&rows).ece, 0.45));
    }

    #[test]
    fn ece_puts_p_equal_one_in_the_last_bin() {
        // p=1.0 and p=0.95 share bin 9: acc 0.5, conf 0.975 -> 0.475.
        // (With p=1.0 in its own 11th bin: 0.5*0.05 + 0.5*0.5 = 0.275.)
        let rows = vec![ok(true, 1.0), ok(false, 0.95)];
        assert!(close(m(&rows).ece, 0.475));
    }

    #[test]
    fn ece_uses_ten_bins() {
        // 0.65 and 0.75 are separate tenths (bins 6 and 7): 0.5*0.35 + 0.5*0.75
        // = 0.55. Five bins would merge them (acc 0.5, conf 0.7) to 0.2.
        let rows = vec![ok(true, 0.65), ok(false, 0.75)];
        assert!(close(m(&rows).ece, 0.55));
    }

    #[test]
    fn ece_ignores_error_and_not_applicable_rows() {
        let rows = vec![ok(true, 0.95), err(), na()];
        assert!(close(m(&rows).ece, 0.05));
    }

    #[test]
    fn confident_wrong_rate() {
        // wrong@0.8 and wrong@0.9 count; wrong@0.79 and right@0.99 do not.
        // Denominator is applicable rows: 4 ok + 1 error = 5.
        let rows = vec![
            ok(false, 0.8),
            ok(false, 0.9),
            ok(false, 0.79),
            ok(true, 0.99),
            err(),
            na(),
        ];
        assert!(close(m(&rows).confident_wrong, 2.0 / 5.0));
    }

    #[test]
    fn mae_on_score_rows() {
        let rows: Vec<ResultRow> = [(0usize, 0.5), (1, 1.0), (2, 1.0)]
            .iter()
            .map(|&(e, v)| ResultRow {
                expected_level: Some(e),
                score_value: Some(v),
                ..ok(true, 0.9)
            })
            .collect();
        assert!(close(m(&rows).mae, 0.5));
        assert_eq!(m(&[ok(true, 0.9)]).mae, None);
    }

    #[test]
    fn percentiles_use_ok_rows_only() {
        let mut rows: Vec<ResultRow> = (1..=100)
            .map(|l| ResultRow {
                latency_ms: l,
                ..ok(true, 0.9)
            })
            .collect();
        let x = m(&rows);
        assert_eq!((x.p50_ms, x.p95_ms), (Some(50), Some(95)));
        rows.extend((0..50).map(|_| err()));
        rows.extend((0..50).map(|_| na()));
        let y = m(&rows);
        assert_eq!((y.p50_ms, y.p95_ms), (Some(50), Some(95)));
    }

    #[test]
    fn empty_and_all_not_applicable_rows_give_none_not_nan() {
        for rows in [vec![], vec![na(), na()]] {
            let x = m(&rows);
            assert_eq!(x.n_applicable, 0);
            assert_eq!(x.coverage, 0.0);
            assert!(!x.coverage.is_nan());
            assert_eq!(
                (x.accuracy, x.ci, x.ece, x.confident_wrong, x.error_rate),
                (None, None, None, None, None)
            );
            assert_eq!((x.mae, x.p50_ms, x.p95_ms), (None, None, None));
        }
    }

    #[test]
    fn compute_on_filters_to_the_shared_case_ids() {
        let rows: Vec<ResultRow> = ["a", "b", "c"]
            .iter()
            .map(|id| ResultRow {
                case_id: (*id).into(),
                ..ok(*id != "c", 0.9)
            })
            .collect();
        assert_eq!(compute_on(&rows, None, 1).n_total, 3);
        let keep: HashSet<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
        let x = compute_on(&rows, Some(&keep), 1);
        assert_eq!(x.n_total, 2);
        assert!(close(x.accuracy, 1.0));
    }
}
