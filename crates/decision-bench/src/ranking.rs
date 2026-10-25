//! `ranking.json`: the machine-readable ranking a gateway or torii consumes.
//!
//! The Markdown report is for a reader; this is for a program. It is a pure
//! function of `results.json` plus the bootstrap seed, so re-running `rank` on
//! the same inputs is byte-identical (`generated_at` is the *run's* timestamp,
//! never the render's).
//!
//! Two rules the artifact must not blur:
//!
//! - `verdict` decides whether the rows are an **order**. Only `winner` carries
//!   `order`; a `tie` is a set, not a sequence, and `insufficient`/`unranked`
//!   carry none at all.
//! - a category measured through a stand-in says so in `proxy`. `planner.select`
//!   standing on `tool.route` is not a planner measurement.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::category::Category;
use crate::fetch::sha256_hex;
use crate::rank::{Verdict, rank};
use crate::report::cli_name;
use crate::run::{Results, RosterEntry};

/// Bumped when a field changes meaning, never for a new optional field.
pub const SCHEMA: u32 = 1;

/// Which corpus the numbers were measured on.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CorpusRef {
    /// Path as given, relative to the crate directory where it lies under it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lock: Option<String>,
    /// sha256 of the lockfile's bytes, so a consumer can pin the exact revision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    /// Bootstrap seed for the confidence intervals.
    pub seed: u64,
}

impl CorpusRef {
    /// The ref for `lock` (read for its sha256). A missing lock is recorded as
    /// absent rather than invented: `rank` warns, and the artifact says so.
    pub fn from_lock(lock: Option<&Path>, crate_dir: &Path, seed: u64) -> CorpusRef {
        let Some(path) = lock else {
            return CorpusRef {
                lock: None,
                sha256: None,
                seed,
            };
        };
        let shown = path
            .strip_prefix(crate_dir)
            .unwrap_or(path)
            .display()
            .to_string();
        match std::fs::read(path) {
            Ok(bytes) => CorpusRef {
                lock: Some(shown),
                sha256: Some(sha256_hex(&bytes)),
                seed,
            },
            Err(_) => CorpusRef {
                lock: None,
                sha256: None,
                seed,
            },
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RankingVerdict {
    Winner,
    Tie,
    Insufficient,
    Unranked,
}

/// One measured model in a category, best first by the bench's own ordering.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RankedRow {
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accuracy: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ci: Option<(f64, f64)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ece: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confident_wrong: Option<f64>,
    /// Share of the model's own rows it could answer at all (not
    /// `NotApplicable`) — the number a fit check cares about.
    pub coverage: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p50_ms: Option<u64>,
}

/// One category's ranking.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CategoryRanking {
    pub verdict: RankingVerdict,
    /// Set when the evidence is a stand-in (`tool.route` for `planner.select`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy: Option<String>,
    /// Best first. Present **only** for a `winner`: a tie has no order, and a
    /// consumer must not read `ranked`'s order as one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<Vec<String>>,
    /// The measured table, best first. Empty for `unranked`.
    pub ranked: Vec<RankedRow>,
    /// Scenario ids behind the rows (`["s1b"]`), so a reader can audit the claim.
    pub evidence: Vec<String>,
    /// Cases applicable to every ranked model.
    pub shared_cases: usize,
}

/// The whole of `ranking.json`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct RankingArtifact {
    pub schema: u32,
    /// The run's timestamp (Unix seconds), not the render time.
    pub generated_at: u64,
    pub corpus: CorpusRef,
    pub roster: Vec<RosterEntry>,
    /// Keyed by category id, in taxonomy order (`BTreeMap` keeps it stable).
    pub categories: BTreeMap<String, CategoryRanking>,
}

impl RankingArtifact {
    /// The order a consumer may follow for `category`, or `None` when the
    /// verdict is not a winner. This is *the* gate: never read `ranked`.
    pub fn order(&self, category: &str) -> Option<&[String]> {
        self.categories
            .get(category)
            .and_then(|c| c.order.as_deref())
    }

    /// Pretty JSON with a trailing newline — stable enough to diff.
    pub fn to_json(&self) -> String {
        format!(
            "{}\n",
            serde_json::to_string_pretty(self).expect("artifact serializes")
        )
    }
}

/// Build the artifact. Every category in [`Category::ALL`] appears, so a
/// consumer never has to distinguish "absent" from "not measured".
pub fn build(results: &Results, corpus: CorpusRef) -> RankingArtifact {
    let seed = corpus.seed;
    let models: Vec<String> = results.roster.iter().map(|m| m.id.clone()).collect();
    let mut categories = BTreeMap::new();
    for category in Category::ALL {
        categories.insert(
            category.id().to_string(),
            rank_category(category, results, &models, seed),
        );
    }
    RankingArtifact {
        schema: SCHEMA,
        generated_at: results.generated_at,
        corpus,
        roster: results.roster.clone(),
        categories,
    }
}

fn rank_category(
    category: Category,
    results: &Results,
    models: &[String],
    seed: u64,
) -> CategoryRanking {
    let has_rows = |s: crate::case::Scenario| {
        results
            .rows
            .iter()
            .any(|r| r.scenario == s && models.contains(&r.model))
    };
    // Its own cases when they ran; otherwise a labelled stand-in when THAT ran;
    // otherwise unranked — never an order derived from somewhere else.
    let own = category
        .scenario()
        .expect("every category names a scenario");
    let (scenario, proxy) = if has_rows(own) {
        (own, None)
    } else if let Some(p) = category
        .proxy()
        .filter(|p| p.scenario().is_some_and(has_rows))
    {
        (
            p.scenario().expect("proxy scenario"),
            Some(p.id().to_string()),
        )
    } else {
        return CategoryRanking {
            verdict: RankingVerdict::Unranked,
            proxy: None,
            order: None,
            ranked: vec![],
            evidence: vec![],
            shared_cases: 0,
        };
    };

    let r = rank(scenario, &results.rows, models, seed);
    let ranked: Vec<RankedRow> = r
        .ranked
        .iter()
        .map(|m| RankedRow {
            model: m.id.clone(),
            accuracy: m.shared.accuracy,
            ci: m.shared.ci,
            ece: m.shared.ece,
            confident_wrong: m.shared.confident_wrong,
            coverage: m.own.coverage,
            p50_ms: m.shared.p50_ms,
        })
        .collect();
    let (verdict, order) = match &r.verdict {
        Verdict::Winner(_) => (
            RankingVerdict::Winner,
            Some(r.ranked.iter().map(|m| m.id.clone()).collect()),
        ),
        Verdict::Tie(_) => (RankingVerdict::Tie, None),
        Verdict::Insufficient { .. } => (RankingVerdict::Insufficient, None),
    };
    CategoryRanking {
        verdict,
        proxy,
        order,
        ranked,
        evidence: vec![cli_name(scenario).to_string()],
        shared_cases: r.shared_cases,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::case::Scenario;
    use crate::run::{Outcome, ResultRow};

    fn row(model: &str, case: usize, correct: bool) -> ResultRow {
        ResultRow {
            case_id: format!("c{case}"),
            scenario: Scenario::S1bBanking77,
            model: model.into(),
            outcome: Outcome::Ok,
            correct: Some(correct),
            top_probability: Some(0.6),
            score_value: None,
            expected_level: None,
            answer: None,
            probabilities: None,
            latency_ms: 10,
            reached_server: true,
            detail: None,
        }
    }

    /// `right` of `n` correct, on the given scenario.
    fn graded(model: &str, n: usize, right: usize, scenario: Scenario) -> Vec<ResultRow> {
        (1..=n)
            .map(|i| ResultRow {
                scenario,
                ..row(model, i, i <= right)
            })
            .collect()
    }

    fn results(rows: Vec<ResultRow>, models: &[&str]) -> Results {
        Results {
            generated_at: 1_793_409_600,
            manifest: None,
            not_run: vec![],
            roster: models
                .iter()
                .map(|id| RosterEntry {
                    id: (*id).into(),
                    router: "ollama".into(),
                    api_model_id: (*id).into(),
                    context_window: 8194,
                })
                .collect(),
            labels_reviewed: vec![],
            completed: vec![],
            paths: None,
            rows,
        }
    }

    fn corpus() -> CorpusRef {
        CorpusRef {
            lock: Some("corpus.lock.json".into()),
            sha256: Some("abc".into()),
            seed: 2026,
        }
    }

    #[test]
    fn a_separated_run_names_a_winner_and_its_order() {
        let mut rows = graded("a", 60, 58, Scenario::S1bBanking77);
        rows.extend(graded("b", 60, 20, Scenario::S1bBanking77));
        let a = build(&results(rows, &["a", "b"]), corpus());
        let tool = &a.categories["tool.route"];
        assert_eq!(tool.verdict, RankingVerdict::Winner);
        assert_eq!(tool.order.as_deref().unwrap(), ["a", "b"]);
        assert_eq!(a.order("tool.route").unwrap(), ["a", "b"]);
        assert_eq!(tool.proxy, None);
        assert_eq!(tool.evidence, ["s1b"]);
        assert_eq!(tool.shared_cases, 60);
        assert_eq!(tool.ranked[0].model, "a");
        assert_eq!(tool.ranked[0].coverage, 1.0);
    }

    #[test]
    fn a_tie_produces_no_ordering() {
        let mut rows = graded("a", 100, 80, Scenario::S1bBanking77);
        rows.extend(graded("b", 100, 78, Scenario::S1bBanking77));
        let a = build(&results(rows, &["a", "b"]), corpus());
        let tool = &a.categories["tool.route"];
        assert_eq!(tool.verdict, RankingVerdict::Tie);
        assert_eq!(tool.order, None);
        assert_eq!(a.order("tool.route"), None);
        assert_eq!(tool.ranked.len(), 2, "the measurements are still reported");
    }

    #[test]
    fn insufficient_cases_produce_no_ordering() {
        let mut rows = graded("a", 10, 10, Scenario::S1bBanking77);
        rows.extend(graded("b", 10, 0, Scenario::S1bBanking77));
        let a = build(&results(rows, &["a", "b"]), corpus());
        let tool = &a.categories["tool.route"];
        assert_eq!(tool.verdict, RankingVerdict::Insufficient);
        assert_eq!(tool.order, None);
        assert_eq!(tool.shared_cases, 10);
    }

    #[test]
    fn a_category_with_no_cases_is_unranked_never_an_alphabetical_default() {
        let rows = graded("a", 60, 60, Scenario::S1bBanking77);
        let a = build(&results(rows, &["a"]), corpus());
        // No gate scenarios ran at all.
        for id in ["gate.test_output", "entailment.check", "intent.route"] {
            let c = &a.categories[id];
            assert_eq!(c.verdict, RankingVerdict::Unranked, "{id}");
            assert!(c.ranked.is_empty(), "{id}");
            assert_eq!(c.order, None, "{id}");
            assert_eq!(c.proxy, None, "{id}");
            assert!(c.evidence.is_empty(), "{id}");
        }
    }

    #[test]
    fn an_unmeasured_planner_with_no_proxy_rows_is_unranked_not_proxied() {
        // Rows exist for tool.route, so the proxy WOULD be usable; but with no
        // rows at all nothing is in play, and the artifact must not claim it.
        let a = build(&results(vec![], &["a"]), corpus());
        let p = &a.categories["planner.select"];
        assert_eq!(p.verdict, RankingVerdict::Unranked);
        assert_eq!(p.proxy, None);
        assert!(p.ranked.is_empty() && p.order.is_none());
    }

    #[test]
    fn planner_select_is_marked_as_a_tool_route_proxy() {
        let mut rows = graded("a", 60, 58, Scenario::S1bBanking77);
        rows.extend(graded("b", 60, 20, Scenario::S1bBanking77));
        let a = build(&results(rows, &["a", "b"]), corpus());
        let p = &a.categories["planner.select"];
        assert_eq!(p.verdict, RankingVerdict::Winner);
        assert_eq!(p.proxy.as_deref(), Some("tool.route"));
        assert_eq!(p.evidence, ["s1b"]);
    }

    #[test]
    fn real_planner_cases_take_over_from_the_proxy() {
        let mut rows = graded("a", 60, 58, Scenario::PlannerSelect);
        rows.extend(graded("b", 60, 20, Scenario::PlannerSelect));
        let a = build(&results(rows, &["a", "b"]), corpus());
        let p = &a.categories["planner.select"];
        assert_eq!(p.verdict, RankingVerdict::Winner);
        assert_eq!(p.proxy, None, "no stand-in once the category is measured");
        assert_eq!(p.evidence, ["planner"]);
    }

    #[test]
    fn the_artifact_is_byte_identical_on_a_re_run() {
        let mut rows = graded("a", 60, 50, Scenario::S1bBanking77);
        rows.extend(graded("b", 60, 40, Scenario::S1bBanking77));
        let r = results(rows, &["a", "b"]);
        assert_eq!(build(&r, corpus()).to_json(), build(&r, corpus()).to_json());
    }

    #[test]
    fn every_category_is_present() {
        let a = build(&results(vec![], &["a"]), corpus());
        assert_eq!(a.categories.len(), Category::ALL.len());
        assert_eq!(a.schema, SCHEMA);
        assert_eq!(a.generated_at, 1_793_409_600);
        assert_eq!(a.corpus.seed, 2026);
    }
}
