//! The Markdown report, rendered from `results.json` alone (spec D4, D6, D7):
//! roster, NOT RUN models, datasets, a scenario × model summary, one ranked
//! section per scenario, the S4 label-review state, rows that never reached a
//! server, and how to reproduce the run.

use std::fmt::Write as _;

use crate::case::Scenario;
use crate::load::is_s4;
use crate::metrics::Metrics;
use crate::rank::{COVERAGE_FLOOR, ScenarioRanking, Verdict, rank_all};
use crate::run::{Outcome, Results};

/// What a missing value renders as.
const DASH: &str = "—";

/// Render `results` as Markdown. `seed` seeds the bootstrap CIs.
pub fn render(results: &Results, seed: u64) -> String {
    let models = run_models(results);
    let rankings = rank_all(&results.rows, &models, seed);
    let mut md = String::new();
    title(&mut md, results);
    roster(&mut md, results);
    not_run(&mut md, results);
    datasets(&mut md, results, &rankings, seed);
    summary(&mut md, &models, &rankings);
    for r in &rankings {
        scenario_section(&mut md, results, r);
    }
    label_review(&mut md, results, &rankings);
    never_reached(&mut md, results, &models);
    reproduce(&mut md, results, &rankings, seed);
    md
}

/// The models that ran: the roster minus NOT RUN, or (for a results file with
/// no roster) every model with rows, in first-seen order.
fn run_models(results: &Results) -> Vec<String> {
    let skipped = |id: &str| results.not_run.iter().any(|n| n.model == id);
    let mut models: Vec<String> = results
        .roster
        .iter()
        .map(|m| m.id.clone())
        .filter(|id| !skipped(id))
        .collect();
    if results.roster.is_empty() {
        for r in &results.rows {
            if !models.contains(&r.model) {
                models.push(r.model.clone());
            }
        }
    }
    models
}

/// Short name, used in headings, the summary and verdicts.
pub fn code(s: Scenario) -> &'static str {
    match s {
        Scenario::S1aIntentOos => "S1a",
        Scenario::S1bBanking77 => "S1b",
        Scenario::S2Sentiment => "S2",
        Scenario::S3Entailment => "S3",
        Scenario::S4TestOutput => "S4 test-output",
        Scenario::S4ReviewFinding => "S4 review-finding",
        Scenario::S4FixCheck => "S4 fix-check",
    }
}

/// The `--scenarios` name (see [`crate::load::parse_scenarios`]).
pub fn cli_name(s: Scenario) -> &'static str {
    match s {
        Scenario::S1aIntentOos => "s1a",
        Scenario::S1bBanking77 => "s1b",
        Scenario::S2Sentiment => "s2",
        Scenario::S3Entailment => "s3",
        Scenario::S4TestOutput => "s4-test-output",
        Scenario::S4ReviewFinding => "s4-review-finding",
        Scenario::S4FixCheck => "s4-fix-check",
    }
}

fn describe(s: Scenario) -> &'static str {
    match s {
        Scenario::S1aIntentOos => "intent routing with an out-of-scope option (choice of 9)",
        Scenario::S1bBanking77 => "tool / intent routing (choice of 8)",
        Scenario::S2Sentiment => "ordered triage score (3 levels)",
        Scenario::S3Entailment => "does B follow from A? (yes / no)",
        Scenario::S4TestOutput => "test output → continue / fix_code / retry / fix_environment",
        Scenario::S4ReviewFinding => "review finding → block / follow_up / dismiss",
        Scenario::S4FixCheck => "does the change fix the finding? (yes / no)",
    }
}

fn cell(s: &str) -> String {
    s.replace('|', "\\|")
}

fn f2(v: Option<f64>) -> String {
    match v {
        Some(x) if x.is_finite() => format!("{x:.2}"),
        _ => DASH.into(),
    }
}

/// Accuracy and CI bounds: three decimals, because CI bounds are multiples of
/// 0.005 and at two a separated winner's lower bound can print equal to the
/// runner-up's upper bound.
fn f3(v: Option<f64>) -> String {
    match v {
        Some(x) if x.is_finite() => format!("{x:.3}"),
        _ => DASH.into(),
    }
}

/// A share as a whole percent, rounded DOWN below 100% so that a model
/// missing even one case never reads as "100%".
fn pct(v: f64) -> String {
    if !v.is_finite() {
        return DASH.into();
    }
    format!("{}%", (v * 100.0 + 1e-9).floor() as i64)
}

fn ci(v: Option<(f64, f64)>) -> String {
    match v {
        Some((lo, hi)) if lo.is_finite() && hi.is_finite() => format!("[{lo:.3}, {hi:.3}]"),
        _ => DASH.into(),
    }
}

fn ms(v: Option<u64>) -> String {
    v.map_or_else(|| DASH.into(), |x| x.to_string())
}

/// Unix seconds as `YYYY-MM-DD HH:MM:SS UTC` (proleptic Gregorian).
fn utc(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    // Howard Hinnant's days-to-civil.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02} UTC",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

fn title(md: &mut String, results: &Results) {
    let _ = writeln!(md, "# System One benchmark\n");
    let _ = writeln!(md, "Generated {}.\n", utc(results.generated_at));
    let unfinished: Vec<&str> = results
        .roster
        .iter()
        .map(|m| m.id.as_str())
        .filter(|id| {
            !results.completed.iter().any(|c| c == id)
                && !results.not_run.iter().any(|n| n.model == *id)
        })
        .collect();
    if !unfinished.is_empty() {
        let _ = writeln!(
            md,
            "> **INCOMPLETE RUN**: {} never finished; the run stopped early, so their rows are \
             partial or missing and this is not a complete ranking.\n",
            unfinished.join(", ")
        );
    }
}

fn roster(md: &mut String, results: &Results) {
    let _ = writeln!(md, "## Roster\n");
    if results.roster.is_empty() {
        let _ = writeln!(
            md,
            "Not recorded (this results file predates the roster field).\n"
        );
        return;
    }
    let _ = writeln!(md, "| Model | Router | API model id | Context window |");
    let _ = writeln!(md, "|---|---|---|---|");
    for m in &results.roster {
        let _ = writeln!(
            md,
            "| {} | {} | {} | {} |",
            cell(&m.id),
            cell(&m.router),
            cell(&m.api_model_id),
            m.context_window
        );
    }
    md.push('\n');
}

fn not_run(md: &mut String, results: &Results) {
    if results.not_run.is_empty() {
        return;
    }
    let _ = writeln!(md, "## NOT RUN\n");
    let _ = writeln!(
        md,
        "`check` could not run these models (unreachable, or no input fits their context \
         window), so they have no results below:\n"
    );
    for n in &results.not_run {
        let _ = writeln!(md, "- **{}**: {}", n.model, n.reason.replace('\n', " "));
    }
    md.push('\n');
}

fn datasets(md: &mut String, results: &Results, rankings: &[ScenarioRanking], seed: u64) {
    let _ = writeln!(md, "## Datasets\n");
    match &results.manifest {
        None => {
            let _ = writeln!(
                md,
                "No `manifest.json` was found with the cases, so the dataset revisions are \
                 unknown.\n"
            );
        }
        Some(m) => {
            if !m.datasets.is_empty() {
                let _ = writeln!(
                    md,
                    "| Dataset | Config / split | Licence | Revision sha | Rows sha256 |"
                );
                let _ = writeln!(md, "|---|---|---|---|---|");
                for d in &m.datasets {
                    let _ = writeln!(
                        md,
                        "| {} | {} / {} | {} | `{}` | `{}` |",
                        cell(&d.id),
                        cell(&d.config),
                        cell(&d.split),
                        cell(&d.license),
                        d.sha,
                        d.rows_sha256
                    );
                }
                md.push('\n');
            }
            let _ = writeln!(
                md,
                "Sampling: seed {}, rand {}, {} cases per scenario.\n",
                m.seed, m.rand_version, m.per_scenario
            );
        }
    }
    if rankings.iter().any(|r| is_s4(r.scenario)) {
        let _ = writeln!(
            md,
            "S4 cases are hand-curated: `crates/decision-bench/scenarios/s4-*.jsonl`.\n"
        );
    }
    let _ = writeln!(md, "Bootstrap CIs: 1000 resamples, seed {seed}.\n");
}

fn summary(md: &mut String, models: &[String], rankings: &[ScenarioRanking]) {
    let _ = writeln!(md, "## Summary\n");
    let _ = writeln!(
        md,
        "Head-to-head accuracy on each scenario's shared cases, with the model's own \
         coverage. `(own cases)`: below the {}% coverage floor, so accuracy is over its own \
         applicable cases and it is not ranked. {DASH}: no rows.\n",
        pct(COVERAGE_FLOOR).trim_end_matches('%')
    );
    let names: Vec<String> = models.iter().map(|m| cell(m)).collect();
    let _ = writeln!(md, "| Scenario | {} |", names.join(" | "));
    let _ = writeln!(md, "|---|{}", "---|".repeat(models.len()));
    for r in rankings {
        let cells: Vec<String> = models
            .iter()
            .map(|m| {
                if let Some(x) = r.ranked.iter().find(|x| &x.id == m) {
                    format!("{} (cov {})", f3(x.shared.accuracy), pct(x.own.coverage))
                } else if let Some((_, x)) = r.low_coverage.iter().find(|(id, _)| id == m) {
                    format!("{} (cov {}, own cases)", f3(x.accuracy), pct(x.coverage))
                } else {
                    DASH.into()
                }
            })
            .collect();
        let _ = writeln!(md, "| {} | {} |", code(r.scenario), cells.join(" | "));
    }
    md.push('\n');
}

const TABLE_HEAD: &str = "| Model | Coverage | Accuracy | 95% CI | ECE | Confident-wrong | \
                          Error rate | Not reached | MAE | p50 ms | p95 ms |";

/// One table line: `own` gives coverage and not-reached (over the model's
/// own rows), `m` everything else.
fn metrics_row(md: &mut String, id: &str, own: &Metrics, m: &Metrics) {
    let _ = writeln!(
        md,
        "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
        cell(id),
        pct(own.coverage),
        f3(m.accuracy),
        ci(m.ci),
        f2(m.ece),
        f2(m.confident_wrong),
        f2(m.error_rate),
        own.not_reached,
        f2(m.mae),
        ms(m.p50_ms),
        ms(m.p95_ms)
    );
}

fn table_head(md: &mut String) {
    let _ = writeln!(md, "{TABLE_HEAD}");
    let _ = writeln!(md, "|---|---|---|---|---|---|---|---|---|---|---|");
}

/// `None` (no entry, e.g. an older results file) counts as unreviewed.
fn reviewed_by(results: &Results, s: Scenario) -> Option<&str> {
    results
        .labels_reviewed
        .iter()
        .find(|l| l.scenario == s)
        .and_then(|l| l.reviewed_by.as_deref())
}

fn scenario_section(md: &mut String, results: &Results, r: &ScenarioRanking) {
    let name = code(r.scenario);
    let _ = writeln!(md, "## {name} — {}\n", describe(r.scenario));
    if is_s4(r.scenario) && reviewed_by(results, r.scenario).is_none() {
        let _ = writeln!(
            md,
            "> **labels unreviewed**: nobody has signed off these hand-curated labels yet, \
             so the results below are provisional.\n"
        );
    }
    if r.ranked.is_empty() {
        let _ = writeln!(
            md,
            "No model reached the {} coverage floor, so there is no head-to-head.\n",
            pct(COVERAGE_FLOOR)
        );
    } else if r.shared_cases == 0 {
        let _ = writeln!(
            md,
            "Shared cases: 0. There are no shared cases: no case is applicable to every \
             ranked model, so there is no head-to-head table.\n"
        );
    } else {
        let _ = writeln!(
            md,
            "Shared cases: {} (applicable to every ranked model). Metrics are over the shared \
             cases; coverage and not-reached are over the model's own cases.\n",
            r.shared_cases
        );
        table_head(md);
        for m in &r.ranked {
            metrics_row(md, &m.id, &m.own, &m.shared);
        }
        md.push('\n');
        let ids: Vec<&str> = r.ranked.iter().map(|m| m.id.as_str()).collect();
        not_applicable_reasons(md, results, r.scenario, &ids);
    }
    let verdict = match &r.verdict {
        Verdict::Winner(m) => format!(
            "**Use {m} for {name}** among the head-to-head models: its 95% CI lies above \
             every other head-to-head model's; models below the coverage floor are not \
             compared."
        ),
        Verdict::Tie(ms) => format!(
            "**Tie: {}**: not statistically separable (their 95% CIs reach the top model's).",
            ms.join(", ")
        ),
        Verdict::Insufficient { reason } => format!("Insufficient: {reason}. No verdict."),
    };
    let _ = writeln!(md, "{verdict}\n");
    if !r.low_coverage.is_empty() {
        let _ = writeln!(
            md,
            "### Below the {} coverage floor (own cases)\n",
            pct(COVERAGE_FLOOR)
        );
        table_head(md);
        for (id, m) in &r.low_coverage {
            metrics_row(md, id, m, m);
        }
        md.push('\n');
        let ids: Vec<&str> = r.low_coverage.iter().map(|(id, _)| id.as_str()).collect();
        not_applicable_reasons(md, results, r.scenario, &ids);
    }
    if !r.no_rows.is_empty() {
        let _ = writeln!(md, "No rows: {}\n", r.no_rows.join(", "));
    }
}

/// Longest not-applicable reason quoted in the report, in chars.
const REASON_MAX_CHARS: usize = 160;

/// For each of `models` with not-applicable rows on `scenario`: its most
/// common not-applicable detail (first seen on a tie) and how many rows have
/// it, e.g. `- m: 29 of 30 not applicable: <detail>`. Details were redacted
/// and clipped when the rows were recorded.
fn not_applicable_reasons(md: &mut String, results: &Results, scenario: Scenario, models: &[&str]) {
    let mut lines = Vec::new();
    for &m in models {
        let mut counts: Vec<(&str, usize)> = Vec::new();
        for r in results.rows.iter().filter(|r| {
            r.model == m && r.scenario == scenario && r.outcome == Outcome::NotApplicable
        }) {
            let d = r.detail.as_deref().unwrap_or("no reason recorded");
            match counts.iter_mut().find(|(x, _)| *x == d) {
                Some((_, n)) => *n += 1,
                None => counts.push((d, 1)),
            }
        }
        let total: usize = counts.iter().map(|(_, n)| n).sum();
        // `max_by_key` keeps the last maximum; reversed, that is the first seen.
        let Some(&(detail, n)) = counts.iter().rev().max_by_key(|(_, n)| *n) else {
            continue;
        };
        let mut reason: String = detail.replace('\n', " ");
        if reason.chars().count() > REASON_MAX_CHARS {
            reason = reason.chars().take(REASON_MAX_CHARS).collect::<String>() + "…";
        }
        let share = if n == total {
            n.to_string()
        } else {
            format!("{n} of {total}")
        };
        lines.push(format!("- {m}: {share} not applicable: {reason}"));
    }
    if lines.is_empty() {
        return;
    }
    let _ = writeln!(md, "Most common not-applicable reason:\n");
    for l in lines {
        let _ = writeln!(md, "{l}");
    }
    md.push('\n');
}

fn label_review(md: &mut String, results: &Results, rankings: &[ScenarioRanking]) {
    let mut s4: Vec<Scenario> = results.labels_reviewed.iter().map(|l| l.scenario).collect();
    for r in rankings {
        if is_s4(r.scenario) && !s4.contains(&r.scenario) {
            s4.push(r.scenario);
        }
    }
    if s4.is_empty() {
        return;
    }
    s4.sort_by_key(Scenario::tag);
    let _ = writeln!(md, "## Label review\n");
    for s in s4 {
        match reviewed_by(results, s) {
            Some(by) => {
                let _ = writeln!(md, "- {}: reviewed by {by}", code(s));
            }
            None => {
                let _ = writeln!(
                    md,
                    "- {}: **labels unreviewed**; results are provisional until \
                     `labels_reviewed_by` is set in the file's header line.",
                    code(s)
                );
            }
        }
    }
    md.push('\n');
}

fn never_reached(md: &mut String, results: &Results, models: &[String]) {
    let mut lines = Vec::new();
    let mut scenarios: Vec<Scenario> = Vec::new();
    for r in &results.rows {
        if !scenarios.contains(&r.scenario) {
            scenarios.push(r.scenario);
        }
    }
    scenarios.sort_by_key(Scenario::tag);
    let mut all_models: Vec<String> = models.to_vec();
    for r in &results.rows {
        if !all_models.contains(&r.model) {
            all_models.push(r.model.clone());
        }
    }
    for m in &all_models {
        for &s in &scenarios {
            let missed: Vec<&Outcome> = results
                .rows
                .iter()
                .filter(|r| &r.model == m && r.scenario == s && !r.reached_server)
                .map(|r| &r.outcome)
                .collect();
            if missed.is_empty() {
                continue;
            }
            let parts: Vec<String> = [
                (Outcome::NotApplicable, "not_applicable"),
                (Outcome::Error, "error"),
                (Outcome::Ok, "ok"),
            ]
            .iter()
            .filter_map(|(o, name)| {
                let n = missed.iter().filter(|x| **x == o).count();
                (n > 0).then(|| format!("{n} {name}"))
            })
            .collect();
            let n = missed.len();
            lines.push(format!(
                "- {m} on {}: {n} {} never reached the server ({})",
                code(s),
                if n == 1 { "row" } else { "rows" },
                parts.join(", ")
            ));
        }
    }
    if lines.is_empty() {
        return;
    }
    let _ = writeln!(md, "## Rows that never reached the server\n");
    let _ = writeln!(
        md,
        "Refused before dispatch by a context-window gate (not_applicable), or by a lockout \
         from a real `Retry-After` or an auth failure (error, counted as wrong); or a \
         transport failure before any reply: a refused or dropped connection, or a DNS \
         failure (error, counted as wrong).\n"
    );
    for l in lines {
        let _ = writeln!(md, "{l}");
    }
    md.push('\n');
}

/// `arg` as one shell word: unchanged when it is plain, else single-quoted with
/// any embedded `'` written as `'\''`.
fn sh_quote(arg: &str) -> String {
    let plain = !arg.is_empty()
        && arg
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "_-./,:=@%+".contains(c));
    if plain {
        arg.to_string()
    } else {
        format!("'{}'", arg.replace('\'', "'\\''"))
    }
}

fn reproduce(md: &mut String, results: &Results, rankings: &[ScenarioRanking], seed: u64) {
    let bin = "cargo run --release -p decision-bench --";
    let paths = results.paths.as_ref();
    let models: Vec<&str> = results.roster.iter().map(|m| m.id.as_str()).collect();
    let models = if models.is_empty() {
        String::new()
    } else {
        format!(" --models {}", models.join(","))
    };
    let config = paths
        .and_then(|p| p.config.as_deref())
        .map(|c| format!(" --config {}", sh_quote(c)))
        .unwrap_or_default();
    let scenarios: Vec<&str> = rankings.iter().map(|r| cli_name(r.scenario)).collect();
    let mut run = format!("{bin} run{config}");
    if let Some(p) = paths {
        let _ = write!(run, " --cases {}", sh_quote(&p.cases));
    }
    if !scenarios.is_empty() {
        let _ = write!(run, " --scenarios {}", scenarios.join(","));
    }
    run += &models;
    if let Some(p) = paths {
        let _ = write!(run, " --out {}", sh_quote(&p.results));
    }
    let report = match paths {
        Some(p) => format!(
            "{bin} report --results {} --out {} --seed {seed}",
            sh_quote(&p.results),
            sh_quote(p.report.as_deref().unwrap_or("report.md"))
        ),
        None => format!("{bin} report --seed {seed}"),
    };
    let _ = writeln!(md, "## Reproduce\n");
    let _ = writeln!(
        md,
        "From the repo root (relative paths resolve under `crates/decision-bench/`):\n"
    );
    let _ = writeln!(md, "```sh");
    let _ = writeln!(md, "{bin} check{config}{models}");
    if let Some(m) = &results.manifest {
        let out = paths
            .map(|p| format!(" --out {}", sh_quote(&p.cases)))
            .unwrap_or_default();
        let _ = writeln!(
            md,
            "{bin} fetch --per-scenario {} --seed {}{out}",
            m.per_scenario, m.seed
        );
    }
    let _ = writeln!(md, "{run}");
    let _ = writeln!(md, "{report}");
    let _ = writeln!(md, "```\n");
    if results.manifest.is_some() {
        let _ = writeln!(
            md,
            "`fetch` rebuilds byte-identical cases only while each dataset is still at the \
             revision sha above; compare the rows sha256 in the cases' `manifest.json`."
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::case::Scenario;
    use crate::metrics::compute_on;
    use crate::run::{LabelReview, NotRun, Outcome, ResultRow, RosterEntry, RunPaths};
    use crate::scenarios::{Manifest, ManifestDataset};

    fn row(model: &str, scenario: Scenario, case: usize, outcome: Outcome) -> ResultRow {
        ResultRow {
            case_id: format!("c{case}"),
            scenario,
            model: model.into(),
            outcome,
            correct: None,
            top_probability: None,
            score_value: None,
            expected_level: None,
            answer: None,
            probabilities: None,
            latency_ms: 10 + case as u64,
            reached_server: true,
            detail: None,
        }
    }

    fn ok(model: &str, scenario: Scenario, case: usize, correct: bool) -> ResultRow {
        ResultRow {
            correct: Some(correct),
            top_probability: Some(0.7),
            ..row(model, scenario, case, Outcome::Ok)
        }
    }

    /// `n` cases of `scenario`, the first `right` correct.
    fn graded(model: &str, scenario: Scenario, n: usize, right: usize) -> Vec<ResultRow> {
        (1..=n)
            .map(|i| ok(model, scenario, i, i <= right))
            .collect()
    }

    fn roster(id: &str, router: &str) -> RosterEntry {
        RosterEntry {
            id: id.into(),
            router: router.into(),
            api_model_id: format!("{id}-api"),
            context_window: 512,
        }
    }

    /// - S1a: `good` and `mid` equal → Tie.
    /// - S2: 5 cases → Insufficient.
    /// - S3: `good` 40/40, `mid` 20/40 (2 gated errors) → Winner good; `ane`
    ///   below the coverage floor.
    /// - S4 test-output reviewed by jerry; S4 fix-check unreviewed.
    /// - `down` NOT RUN.
    fn fixture() -> Results {
        use Scenario::*;
        let mut rows = Vec::new();
        rows.extend(graded("good", S1aIntentOos, 30, 24));
        rows.extend(graded("mid", S1aIntentOos, 30, 23));
        rows.extend(graded("good", S2Sentiment, 5, 5));
        rows.extend(graded("mid", S2Sentiment, 5, 1));
        rows.extend(graded("good", S3Entailment, 40, 40));
        let mut mid = graded("mid", S3Entailment, 40, 20);
        for r in &mut mid[38..] {
            *r = ResultRow {
                reached_server: false,
                detail: Some("gated".into()),
                ..row("mid", S3Entailment, 0, Outcome::Error)
            };
        }
        mid[38].case_id = "c39".into();
        mid[39].case_id = "c40".into();
        // c17..c24 (4 right, 4 wrong) do not fit mid: own coverage 80%, and
        // the shared S3 set is the other 32.
        for (i, r) in mid.iter_mut().enumerate().take(24).skip(16) {
            *r = row("mid", S3Entailment, i + 1, Outcome::NotApplicable);
        }
        rows.extend(mid);
        rows.extend((1..=40).map(|i| {
            if i <= 30 {
                ResultRow {
                    reached_server: false,
                    ..row("ane", S3Entailment, i, Outcome::NotApplicable)
                }
            } else {
                ok("ane", S3Entailment, i, true)
            }
        }));
        rows.extend(graded("good", S4TestOutput, 20, 20));
        rows.extend(graded("mid", S4TestOutput, 20, 0));
        rows.extend(graded("good", S4FixCheck, 20, 20));
        rows.extend(graded("mid", S4FixCheck, 20, 0));
        Results {
            generated_at: 1_760_961_600, // 2025-10-20 12:00:00 UTC
            manifest: Some(Manifest {
                seed: 2026,
                per_scenario: 40,
                rand_version: "0.8.5".into(),
                datasets: vec![ManifestDataset {
                    id: "stanfordnlp/snli".into(),
                    sha: "cdb5c3d5eed6ead6e5a341c8e56e669bb666725b".into(),
                    license: "cc-by-sa-4.0".into(),
                    config: "plain_text".into(),
                    split: "test".into(),
                    num_rows: 10000,
                    rows_sha256: "ab12".repeat(16),
                }],
                files: vec![],
            }),
            not_run: vec![NotRun {
                model: "down".into(),
                reason: "network error: connection refused".into(),
            }],
            roster: vec![
                roster("good", "llamacpp"),
                roster("mid", "ollama"),
                roster("ane", "laya"),
                roster("down", "laya"),
            ],
            labels_reviewed: vec![
                LabelReview {
                    scenario: S4TestOutput,
                    reviewed_by: Some("jerry".into()),
                },
                LabelReview {
                    scenario: S4FixCheck,
                    reviewed_by: None,
                },
            ],
            completed: vec!["good".into(), "mid".into(), "ane".into()],
            paths: None,
            rows,
        }
    }

    fn md() -> String {
        render(&fixture(), 7)
    }

    fn at(md: &str, needle: &str) -> usize {
        md.find(needle)
            .unwrap_or_else(|| panic!("missing {needle:?} in:\n{md}"))
    }

    /// The text of the section starting at `heading`, up to the next `## `.
    fn section<'a>(md: &'a str, heading: &str) -> &'a str {
        let start = at(md, heading);
        let rest = &md[start + heading.len()..];
        let end = rest
            .find("\n## ")
            .map_or(md.len(), |e| start + heading.len() + e);
        &md[start..end]
    }

    #[test]
    fn every_section_appears_in_order() {
        let md = md();
        let order = [
            "# System One benchmark",
            "Generated 2025-10-20 12:00:00 UTC",
            "## Roster",
            "## NOT RUN",
            "## Datasets",
            "## Summary",
            "## S1a",
            "## S2",
            "## S3",
            "## S4 test-output",
            "## S4 fix-check",
            "## Label review",
            "## Rows that never reached the server",
            "## Reproduce",
        ];
        let pos: Vec<usize> = order.iter().map(|h| at(&md, h)).collect();
        for (w, h) in pos.windows(2).zip(order.windows(2)) {
            assert!(w[0] < w[1], "{:?} must come before {:?}:\n{md}", h[0], h[1]);
        }
    }

    /// T7 review I2: a roster model that neither finished nor was NOT RUN
    /// means the run stopped early; the report says so at the top.
    #[test]
    fn a_run_that_stopped_early_is_flagged_incomplete_before_the_summary() {
        let mut f = fixture();
        f.completed = vec!["good".into()];
        let md = render(&f, 7);
        let banner = at(&md, "**INCOMPLETE RUN**");
        assert!(banner < at(&md, "## Summary"));
        let line = md[banner..].lines().next().unwrap();
        assert!(line.contains("mid, ane never finished"), "{line}");
        assert!(!line.contains("down"), "NOT RUN is not incomplete: {line}");
        assert!(
            !render(&fixture(), 7).contains("INCOMPLETE"),
            "a finished run has no banner"
        );
    }

    #[test]
    fn the_roster_names_each_models_router_and_api_model_id() {
        let md = md();
        let roster = section(&md, "## Roster");
        for (id, router) in [
            ("good", "llamacpp"),
            ("mid", "ollama"),
            ("ane", "laya"),
            ("down", "laya"),
        ] {
            assert!(
                roster.contains(&format!("| {id} | {router} | {id}-api |")),
                "{id}:\n{roster}"
            );
        }
    }

    #[test]
    fn a_not_run_model_is_named_with_its_reason_before_any_result() {
        let md = md();
        let not_run = section(&md, "## NOT RUN");
        assert!(
            not_run.contains("**down**") && not_run.contains("connection refused"),
            "{not_run}"
        );
        assert!(at(&md, "## NOT RUN") < at(&md, "## Summary"));
        let mut all_ran = fixture();
        all_ran.not_run.clear();
        assert!(!render(&all_ran, 7).contains("NOT RUN"));
    }

    #[test]
    fn datasets_seed_and_rand_version_come_from_the_manifest() {
        let md = md();
        let ds = section(&md, "## Datasets");
        assert!(ds.contains("stanfordnlp/snli"), "{ds}");
        assert!(ds.contains("cc-by-sa-4.0"), "{ds}");
        assert!(
            ds.contains("cdb5c3d5eed6ead6e5a341c8e56e669bb666725b"),
            "{ds}"
        );
        assert!(ds.contains(&"ab12".repeat(16)), "{ds}");
        assert!(ds.contains("seed 2026"), "{ds}");
        assert!(ds.contains("rand 0.8.5"), "{ds}");
    }

    #[test]
    fn the_summary_has_a_column_per_run_model_with_accuracy_and_coverage() {
        let md = md();
        let summary = section(&md, "## Summary");
        assert!(
            summary.contains("| Scenario | good | mid | ane |"),
            "{summary}"
        );
        let s3 = summary
            .lines()
            .find(|l| l.starts_with("| S3"))
            .unwrap_or_else(|| panic!("{summary}"));
        // good: 32/32 on the 32 shared cases; mid: 16/32 at 80% coverage;
        // ane: 10 own applicable cases, all right.
        assert!(s3.contains("| 1.000 (cov 100%) |"), "{s3}");
        // T7 review I4: the coverage is mid's own, not the shared set's 100%.
        assert!(s3.contains("| 0.500 (cov 80%) |"), "{s3}");
        assert!(s3.contains("| 1.000 (cov 25%, own cases) |"), "{s3}");
        let s2 = summary.lines().find(|l| l.starts_with("| S2")).unwrap();
        assert!(s2.trim_end().ends_with("| — |"), "ane has no S2 rows: {s2}");
    }

    #[test]
    fn a_winner_a_tie_and_an_insufficient_verdict_each_render() {
        let md = md();
        assert!(
            section(&md, "## S3").contains("**Use good for S3**"),
            "{md}"
        );
        let s1a = section(&md, "## S1a");
        assert!(s1a.contains("**Tie: good, mid**"), "{s1a}");
        assert!(s1a.contains("not statistically separable"), "{s1a}");
        let s2 = section(&md, "## S2");
        assert!(s2.contains("Insufficient: 5 shared cases"), "{s2}");
    }

    #[test]
    fn a_scenario_section_has_the_shared_count_ranked_table_low_coverage_and_no_rows() {
        let md = md();
        let s3 = section(&md, "## S3");
        assert!(s3.contains("Shared cases: 32"), "{s3}");
        assert!(
            s3.contains(
                "| Model | Coverage | Accuracy | 95% CI | ECE | Confident-wrong | Error rate | \
                 Not reached | MAE | p50 ms | p95 ms |"
            ),
            "{s3}"
        );
        assert!(s3.contains("Below the 50% coverage floor"), "{s3}");
        assert!(s3.contains("| ane | 25% |"), "{s3}");
        // ane has no S1a rows.
        assert!(
            section(&md, "## S1a").contains("No rows: ane"),
            "{}",
            section(&md, "## S1a")
        );
    }

    #[test]
    fn accuracy_and_cis_use_three_decimals_other_rates_two_and_cis_are_bracketed() {
        let md = md();
        let f = fixture();
        let mid: Vec<ResultRow> = f
            .rows
            .iter()
            .filter(|r| r.model == "mid" && r.scenario == Scenario::S3Entailment)
            .cloned()
            .collect();
        let shared: std::collections::HashSet<String> = mid
            .iter()
            .filter(|r| r.outcome != Outcome::NotApplicable)
            .map(|r| r.case_id.clone())
            .collect();
        assert_eq!(shared.len(), 32);
        let m = compute_on(&mid, Some(&shared), 7);
        let (lo, hi) = m.ci.unwrap();
        let s3 = section(&md, "## S3");
        let line = s3
            .lines()
            .find(|l| l.starts_with("| mid |"))
            .unwrap_or_else(|| panic!("{s3}"));
        assert!(line.contains("| 0.500 |"), "{line}");
        assert!(line.contains(&format!("| [{lo:.3}, {hi:.3}] |")), "{line}");
        let error_rate = format!("| {:.2} |", 2.0 / 32.0);
        assert!(line.contains(&error_rate), "error rate 2/32: {line}");
        // T7 review I4: coverage is over mid's own 40 cases.
        assert!(line.starts_with("| mid | 80% |"), "{line}");
    }

    /// SP-DEC-4 review A3: CI bounds are multiples of 0.005, so at two
    /// decimals a separated winner (lower bound 0.945) and the runner-up
    /// (upper bound 0.94) both print "0.94" and the table contradicts the
    /// verdict. Accuracy and CIs print to three decimals.
    #[test]
    fn ci_bounds_and_accuracy_print_three_decimals_so_a_winner_reads_separated() {
        let metrics = |accuracy: f64, ci: (f64, f64)| Metrics {
            n_total: 200,
            n_applicable: 200,
            n_ok: 200,
            n_error: 0,
            not_reached: 0,
            coverage: 1.0,
            accuracy: Some(accuracy),
            ci: Some(ci),
            ece: Some(0.05),
            confident_wrong: Some(0.01),
            error_rate: Some(0.0),
            mae: None,
            p50_ms: Some(10),
            p95_ms: Some(20),
        };
        let model = |id: &str, m: Metrics| crate::rank::RankedModel {
            id: id.into(),
            shared: m.clone(),
            own: m,
        };
        let ranking = ScenarioRanking {
            scenario: Scenario::S1aIntentOos,
            shared_cases: 200,
            ranked: vec![
                model("nimble", metrics(0.9675, (0.945, 0.99))),
                model("laya-multilingual", metrics(0.9, (0.855, 0.94))),
            ],
            verdict: Verdict::Winner("nimble".into()),
            low_coverage: vec![],
            no_rows: vec![],
        };
        let mut md = String::new();
        scenario_section(&mut md, &fixture(), &ranking);
        summary(
            &mut md,
            &["nimble".into(), "laya-multilingual".into()],
            std::slice::from_ref(&ranking),
        );
        assert!(md.contains("[0.945, 0.990]"), "{md}");
        assert!(md.contains("[0.855, 0.940]"), "{md}");
        assert!(md.contains("| 0.968 |"), "accuracy at 3 dp: {md}");
        assert!(md.contains("| 0.968 (cov 100%) |"), "summary at 3 dp: {md}");
    }

    /// SP-DEC-4 review A4: `check` also marks a reachable server NOT RUN
    /// when no input fits its window, so the preamble must not say every
    /// NOT RUN model was unreachable.
    #[test]
    fn the_not_run_preamble_covers_a_model_no_input_fits() {
        let mut f = fixture();
        f.not_run = vec![NotRun {
            model: "laya-ane".into(),
            reason: "no input fits: the minimal decision needs ≈182 tokens, window 96".into(),
        }];
        let md = render(&f, 7);
        let not_run = section(&md, "## NOT RUN");
        assert!(!not_run.contains("could not reach"), "{not_run}");
        assert!(
            not_run.contains("no input fits their context window"),
            "{not_run}"
        );
        assert!(not_run.contains("**laya-ane**: no input fits"), "{not_run}");
    }

    /// SP-DEC-4 review A5: a model under the coverage floor is shown with
    /// why its cases were not applicable (the most common detail, counted),
    /// so 0% coverage is not left unexplained.
    #[test]
    fn a_low_coverage_model_names_its_most_common_not_applicable_reason() {
        let mut f = fixture();
        let budget = "adapter 'laya' cannot serve this input: the options of question 'q' \
                      need ≈182 tokens";
        for r in f.rows.iter_mut().filter(|r| r.model == "ane") {
            if r.outcome == Outcome::NotApplicable {
                r.detail = Some(budget.into());
            }
        }
        // One odd one out: the most common reason wins, and says it is not all.
        let odd = f
            .rows
            .iter_mut()
            .find(|r| r.model == "ane" && r.outcome == Outcome::NotApplicable)
            .unwrap();
        odd.detail = Some("all candidates gated: over window".into());
        let md = render(&f, 7);
        let s3 = section(&md, "## S3");
        let low = &s3[at(s3, "### Below the 50% coverage floor")..];
        assert!(
            low.contains(&format!("- ane: 29 of 30 not applicable: {budget}")),
            "{low}"
        );
        // A ranked model's not-applicable rows get their reason too.
        let ranked = &s3[..at(s3, "### Below the 50% coverage floor")];
        assert!(
            ranked.contains("- mid: 8 not applicable: no reason recorded"),
            "{ranked}"
        );
    }

    /// The `i`th cell (0 = model) of a table line.
    fn cell_at(line: &str, i: usize) -> &str {
        line.split('|').map(str::trim).nth(i + 1).unwrap()
    }

    /// T7 review M1: the "Not reached" column counts every row that never
    /// reached the server, over the model's own rows, so it agrees with the
    /// "never reached the server" lines.
    #[test]
    fn the_not_reached_column_agrees_with_the_never_reached_lines() {
        let md = md();
        let s3 = section(&md, "## S3");
        let never = section(&md, "## Rows that never reached the server");
        for (model, n) in [("mid", 2), ("ane", 30)] {
            let line = s3
                .lines()
                .find(|l| l.starts_with(&format!("| {model} |")))
                .unwrap();
            assert_eq!(cell_at(line, 7), n.to_string(), "{line}");
            assert!(
                never.contains(&format!("{model} on S3: {n} rows never reached")),
                "{never}"
            );
        }
    }

    #[test]
    fn a_share_just_under_one_never_rounds_up_to_100_percent() {
        assert_eq!(pct(0.995), "99%");
        assert_eq!(pct(1.0), "100%");
        assert_eq!(pct(0.25), "25%");
    }

    /// T7 review M6: the winner is the best of the head-to-head models only.
    #[test]
    fn the_winner_line_says_it_is_among_the_head_to_head_models() {
        let md = md();
        assert!(
            section(&md, "## S3").contains(
                "**Use good for S3** among the head-to-head models: its 95% CI lies above \
                 every other head-to-head model's; models below the coverage floor are not \
                 compared."
            ),
            "{md}"
        );
    }

    /// T7 review M2: the footer replays the run's actual paths.
    #[test]
    fn the_footer_uses_the_recorded_paths() {
        let mut f = fixture();
        f.paths = Some(RunPaths {
            config: Some("my.toml".into()),
            cases: "/data/cases".into(),
            results: "out/r.json".into(),
            report: Some("docs/x.md".into()),
        });
        let md = render(&f, 7);
        let repro = section(&md, "## Reproduce");
        let models = "--models good,mid,ane,down";
        assert!(
            repro.contains(&format!("-- check --config my.toml {models}")),
            "{repro}"
        );
        assert!(
            repro.contains(&format!(
                "-- run --config my.toml --cases /data/cases --scenarios \
                 s1a,s2,s3,s4-test-output,s4-fix-check {models} --out out/r.json"
            )),
            "{repro}"
        );
        assert!(
            repro.contains("-- report --results out/r.json --out docs/x.md --seed 7"),
            "{repro}"
        );
    }

    #[test]
    fn missing_values_render_as_a_dash_never_nan_or_null() {
        let mut f = fixture();
        // A model whose every S3 row is not applicable: no accuracy, CI, ECE…
        f.roster.push(roster("void", "laya"));
        f.rows.extend(
            (1..=40).map(|i| row("void", Scenario::S3Entailment, i, Outcome::NotApplicable)),
        );
        let md = render(&f, 7);
        for bad in ["NaN", "null", "None", "| inf", "inf |"] {
            assert!(!md.contains(bad), "{bad} in:\n{md}");
        }
        let s3 = section(&md, "## S3");
        let void = s3.lines().find(|l| l.starts_with("| void |")).unwrap();
        assert!(void.contains("| 0% | — | — | — |"), "{void}");
        // MAE is not a noul metric.
        let good = s3.lines().find(|l| l.starts_with("| good |")).unwrap();
        assert!(good.contains("| — |"), "{good}");
    }

    #[test]
    fn unreviewed_s4_labels_get_a_provisional_banner_and_reviewed_ones_do_not() {
        let md = md();
        let fix = section(&md, "## S4 fix-check");
        assert!(fix.contains("**labels unreviewed**"), "{fix}");
        assert!(fix.contains("provisional"), "{fix}");
        let tests = section(&md, "## S4 test-output");
        assert!(!tests.contains("unreviewed"), "{tests}");
        let review = section(&md, "## Label review");
        assert!(review.contains("reviewed by jerry"), "{review}");
        assert!(review.contains("**labels unreviewed**"), "{review}");
    }

    #[test]
    fn rows_that_never_reached_the_server_are_counted_per_model_and_scenario() {
        let md = md();
        let never = section(&md, "## Rows that never reached the server");
        assert!(
            never.contains("mid on S3: 2 rows never reached the server (2 error)"),
            "{never}"
        );
        assert!(
            never.contains("ane on S3: 30 rows never reached the server (30 not_applicable)"),
            "{never}"
        );
        let mut all_reached = fixture();
        for r in &mut all_reached.rows {
            r.reached_server = true;
        }
        assert!(!render(&all_reached, 7).contains("never reached the server"));
    }

    #[test]
    fn an_empty_shared_set_says_so_instead_of_a_table() {
        use Scenario::*;
        let mut f = fixture();
        f.rows.retain(|r| r.scenario != S3Entailment);
        // Two models above the floor with disjoint applicable cases.
        for i in 1..=4 {
            let (a, b) = if i <= 2 {
                (Outcome::NotApplicable, Outcome::Ok)
            } else {
                (Outcome::Ok, Outcome::NotApplicable)
            };
            f.rows.push(ResultRow {
                correct: (a == Outcome::Ok).then_some(true),
                ..row("good", S3Entailment, i, a)
            });
            f.rows.push(ResultRow {
                correct: (b == Outcome::Ok).then_some(true),
                ..row("mid", S3Entailment, i, b)
            });
        }
        let md = render(&f, 7);
        let s3 = section(&md, "## S3");
        assert!(s3.contains("no shared cases"), "{s3}");
        assert!(!s3.contains("| Model | Coverage"), "{s3}");
    }

    #[test]
    fn the_footer_gives_the_exact_commands_with_the_seed() {
        let md = md();
        let repro = section(&md, "## Reproduce");
        assert!(
            repro.contains("decision-bench -- fetch --per-scenario 40 --seed 2026"),
            "{repro}"
        );
        assert!(
            repro.contains(
                "decision-bench -- run --scenarios s1a,s2,s3,s4-test-output,s4-fix-check \
                 --models good,mid,ane,down"
            ),
            "{repro}"
        );
        // The bootstrap seed the report was rendered with.
        assert!(
            repro.contains("decision-bench -- report --seed 7"),
            "{repro}"
        );
    }
}
