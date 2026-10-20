//! End-to-end (SP-DEC-4 T8, AC6): wiremock servers stand in for System One
//! models; `check` → `run` → `rank` → `report` run in-process through the
//! crate's public API, exactly as the CLI strings them together.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use decision_bench::case::{Case, Expected, Scenario};
use decision_bench::check::{CheckStatus, check};
use decision_bench::config::{BenchConfig, ModelSpec};
use decision_bench::rank::{Verdict, rank_all};
use decision_bench::report::render;
use decision_bench::run::{NotRun, Outcome, ResultRow, Results, RosterEntry, run, write_results};
use gateway::types::decision::DecisionQuestion;
use serde_json::{Value, json};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const SEED: u64 = 2026;

fn noul_case(i: usize, long: bool, expected: bool) -> Case {
    Case {
        id: format!("s3-{i:03}"),
        scenario: Scenario::S3Entailment,
        state: json!({
            "premise": format!("{}premise number {i}", if long { "LONG " } else { "" }),
            "hypothesis": format!("hypothesis {i}"),
        }),
        question: DecisionQuestion::Noul {
            instructions: json!("Does the premise entail the hypothesis?"),
            criteria: None,
        },
        expected: Expected::Noul { value: expected },
        source: "synthetic".into(),
    }
}

/// `n` cases, expectations alternating true/false; the first `longs` cases
/// whose index is a multiple of 6 carry "LONG" in the premise.
fn cases(n: usize, longs: usize) -> Vec<Case> {
    let long_at: HashSet<usize> = (0..longs).map(|k| k * 6).collect();
    (0..n)
        .map(|i| noul_case(i, long_at.contains(&i), i % 2 == 0))
        .collect()
}

fn premise_of(case: &Case) -> String {
    case.state["premise"].as_str().unwrap().to_string()
}

/// A System One server's behaviour, keyed by the request's `state.premise`.
struct Oracle {
    /// premise -> the expected answer.
    expected: HashMap<String, bool>,
    /// Premises answered wrongly.
    wrong: HashSet<String>,
    /// Answer 200 with `usage.truncated: true` when the premise says LONG.
    truncate_long: bool,
    /// Calls so far (shared so a test can read it).
    calls: Arc<AtomicUsize>,
    /// Calls numbered below this (0-based) fail: 500 on even, 429 on odd.
    fail_first: usize,
    /// The call with this 0-based number gets a 429 + `retry-after: 60`.
    rate_limit_at: Option<usize>,
}

impl Oracle {
    fn new(cases: &[Case]) -> Self {
        Oracle {
            expected: cases
                .iter()
                .map(|c| {
                    let Expected::Noul { value } = c.expected else {
                        unreachable!()
                    };
                    (premise_of(c), value)
                })
                .collect(),
            wrong: HashSet::new(),
            truncate_long: false,
            calls: Arc::new(AtomicUsize::new(0)),
            fail_first: 0,
            rate_limit_at: None,
        }
    }
}

impl Respond for Oracle {
    fn respond(&self, request: &Request) -> ResponseTemplate {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if n < self.fail_first {
            // Alternate a 500 with a 429 that has no Retry-After, so a default
            // rate-limit lockout would also show up as a dropped case.
            return if n % 2 == 1 {
                ResponseTemplate::new(429).set_body_string("slow down")
            } else {
                ResponseTemplate::new(500).set_body_string("boom")
            };
        }
        if self.rate_limit_at == Some(n) {
            return ResponseTemplate::new(429)
                .insert_header("retry-after", "60")
                .set_body_string("slow down");
        }
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        let premise = body["state"]["premise"].as_str().unwrap_or_default();
        // `check`'s probe sends `state: "x"`: any answer proves reachability.
        if body["state"].is_string() {
            return ResponseTemplate::new(200).set_body_json(json!({
                "model": "m",
                "answers": {"q": {"type": "noul", "noul": 0.9}},
                "usage": {"input_tokens": 3, "output_tokens": 0},
            }));
        }
        let truth = *self
            .expected
            .get(premise)
            .unwrap_or_else(|| panic!("unknown premise {premise:?} in {body}"));
        let said = truth != self.wrong.contains(premise);
        let mut usage = json!({"input_tokens": 40, "output_tokens": 0});
        if self.truncate_long && premise.contains("LONG") {
            usage["truncated"] = json!(true);
        }
        ResponseTemplate::new(200).set_body_json(json!({
            "model": "m",
            "answers": {"q": {"type": "noul", "noul": if said { 0.95 } else { 0.05 }}},
            "usage": usage,
        }))
    }
}

async fn serve(oracle: Oracle) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/v1/systemone"))
        .respond_with(oracle)
        .mount(&server)
        .await;
    server
}

fn spec(id: &str, url: &str) -> ModelSpec {
    ModelSpec {
        id: id.into(),
        router: "llamacpp".into(),
        url: url.into(),
        api_model_id: format!("{id}-api"),
        context_window: 8192,
        api_key_env: None,
    }
}

fn ids(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

#[tokio::test]
async fn a_full_run_and_report_over_three_models() {
    let cases = cases(30, 5);
    assert_eq!(
        cases
            .iter()
            .filter(|c| premise_of(c).contains("LONG"))
            .count(),
        5
    );
    let good = serve(Oracle::new(&cases)).await;
    let mut picky_oracle = Oracle::new(&cases);
    picky_oracle.truncate_long = true;
    let picky = serve(picky_oracle).await;

    let cfg = BenchConfig {
        models: vec![
            spec("good", &good.uri()),
            spec("picky", &picky.uri()),
            spec("down", "http://127.0.0.1:9"),
        ],
    };
    let all = ids(&["good", "picky", "down"]);

    // check: `down` is NOT RUN, the others are reachable.
    let checked = check(&cfg, &all).await;
    let status = |id: &str| {
        checked
            .iter()
            .find(|c| c.model == id)
            .unwrap_or_else(|| panic!("no check result for {id}: {checked:?}"))
            .status
            .clone()
    };
    assert_eq!(status("good"), CheckStatus::Reachable);
    assert_eq!(status("picky"), CheckStatus::Reachable);
    assert!(
        matches!(status("down"), CheckStatus::NotRun { .. }),
        "{checked:?}"
    );

    let not_run: Vec<NotRun> = checked
        .iter()
        .filter_map(|c| match &c.status {
            CheckStatus::NotRun { reason } => Some(NotRun {
                model: c.model.clone(),
                reason: reason.clone(),
            }),
            CheckStatus::Reachable => None,
        })
        .collect();
    let runnable: Vec<String> = checked
        .iter()
        .filter(|c| c.status == CheckStatus::Reachable)
        .map(|c| c.model.clone())
        .collect();
    assert_eq!(runnable, ids(&["good", "picky"]));

    let rows = run(&cfg, &cases, &runnable).await.unwrap();
    assert_eq!(rows.len(), 60);
    // check probe + warm-up + 30 cases on each server.
    assert_eq!(good.received_requests().await.unwrap().len(), 1 + 1 + 30);
    assert_eq!(picky.received_requests().await.unwrap().len(), 1 + 1 + 30);

    let results = Results {
        generated_at: 1_760_961_600,
        manifest: None,
        not_run,
        roster: cfg.models.iter().map(RosterEntry::from).collect(),
        labels_reviewed: vec![],
        completed: vec![],
        rows,
    };

    // picky: 5 LONG cases are not_applicable (and did reach the server).
    let picky_rows: Vec<&ResultRow> = results.rows.iter().filter(|r| r.model == "picky").collect();
    let na: Vec<&&ResultRow> = picky_rows
        .iter()
        .filter(|r| r.outcome == Outcome::NotApplicable)
        .collect();
    assert_eq!(na.len(), 5, "{picky_rows:?}");
    assert!(
        results
            .rows
            .iter()
            .filter(|r| r.model == "good")
            .all(|r| r.outcome == Outcome::Ok && r.correct == Some(true))
    );
    assert!(
        picky_rows
            .iter()
            .filter(|r| r.outcome == Outcome::Ok)
            .all(|r| r.correct == Some(true))
    );

    let rankings = rank_all(&results.rows, &ids(&["good", "picky"]), SEED);
    assert_eq!(rankings.len(), 1);
    let s3 = &rankings[0];
    assert_eq!(s3.scenario, Scenario::S3Entailment);
    assert_eq!(s3.shared_cases, 25);
    assert!(s3.low_coverage.is_empty() && s3.no_rows.is_empty());
    assert_eq!(s3.ranked.len(), 2);
    let ranked = |id: &str| s3.ranked.iter().find(|m| m.id == id).unwrap();
    // picky's own coverage is 25/30; on the shared set both are at 100%.
    assert_eq!(ranked("picky").own.n_total, 30);
    assert_eq!(ranked("picky").own.n_applicable, 25);
    assert!((ranked("picky").own.coverage - 25.0 / 30.0).abs() < 1e-9);
    assert!((ranked("good").own.coverage - 1.0).abs() < 1e-9);
    for id in ["good", "picky"] {
        assert_eq!(ranked(id).shared.accuracy, Some(1.0), "{id}");
        assert_eq!(ranked(id).shared.ci, Some((1.0, 1.0)), "{id}");
        assert_eq!(ranked(id).shared.n_applicable, 25, "{id}");
    }
    // Both CIs are (1, 1): the winner test needs top_lo > every other hi, and
    // 1 > 1 is false, so the rule yields a Tie of both.
    match &s3.verdict {
        Verdict::Tie(models) => {
            let mut m = models.clone();
            m.sort();
            assert_eq!(m, ids(&["good", "picky"]));
        }
        other => panic!("expected a Tie, got {other:?}"),
    }

    let md = render(&results, SEED);
    let not_run_section = md
        .split("## NOT RUN")
        .nth(1)
        .unwrap_or_else(|| panic!("no NOT RUN section:\n{md}"))
        .split("\n## ")
        .next()
        .unwrap();
    assert!(not_run_section.contains("**down**"), "{not_run_section}");
    assert!(!not_run_section.contains("good"), "{not_run_section}");
    assert!(md.contains("**Tie: "), "{md}");
    assert!(!md.contains("**Use "), "{md}");

    // results.json round-trips with the documented fields.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("results.json");
    write_results(&path, &results).unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(v["generated_at"], json!(1_760_961_600u64));
    assert_eq!(v["not_run"][0]["model"], json!("down"));
    assert_eq!(v["roster"].as_array().unwrap().len(), 3);
    assert_eq!(v["roster"][1]["id"], json!("picky"));
    assert_eq!(v["roster"][1]["router"], json!("llamacpp"));
    assert_eq!(v["roster"][1]["api_model_id"], json!("picky-api"));
    assert_eq!(v["roster"][1]["context_window"], json!(8192));
    assert!(
        v["roster"][1].get("url").is_none(),
        "urls stay out of the file"
    );
    assert_eq!(v["rows"].as_array().unwrap().len(), 60);
    let na_json = v["rows"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["outcome"] == json!("not_applicable"))
        .count();
    assert_eq!(na_json, 5);
    let back: Results = serde_json::from_str(&text).unwrap();
    assert_eq!(back, results);
    // The report rendered from the file alone is the same report.
    assert_eq!(render(&back, SEED), md);
}

#[tokio::test]
async fn a_server_error_never_drops_later_cases() {
    let cases = cases(10, 0);
    let mut oracle = Oracle::new(&cases);
    // The warm-up is call 0, so this fails the warm-up and the first 5 cases.
    oracle.fail_first = 6;
    let server = serve(oracle).await;
    let cfg = BenchConfig {
        models: vec![spec("flaky", &server.uri())],
    };
    let rows = run(&cfg, &cases, &ids(&["flaky"])).await.unwrap();
    assert_eq!(rows.len(), 10);
    // Breaker, cooldown and lockout all off: every call reached the server.
    assert_eq!(server.received_requests().await.unwrap().len(), 11);
    for (i, r) in rows.iter().enumerate() {
        assert!(r.reached_server, "row {i} never reached the server: {r:?}");
        if i < 5 {
            assert_eq!(r.outcome, Outcome::Error, "row {i}: {r:?}");
            assert_eq!(r.correct, None);
        } else {
            assert_eq!(r.outcome, Outcome::Ok, "row {i}: {r:?}");
            assert_eq!(r.correct, Some(true), "row {i}: {r:?}");
        }
    }
}

#[tokio::test]
async fn a_dispatched_429_is_marked_reached_and_gated_cases_are_not() {
    let cases = cases(5, 0);
    let mut oracle = Oracle::new(&cases);
    // Call 0 is the warm-up (answered); call 1 is the first case.
    oracle.rate_limit_at = Some(1);
    let server = serve(oracle).await;
    let cfg = BenchConfig {
        models: vec![spec("limited", &server.uri())],
    };
    let rows = run(&cfg, &cases, &ids(&["limited"])).await.unwrap();
    assert_eq!(rows.len(), 5);
    // Warm-up + the one 429; the real Retry-After then gates the rest.
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
    assert_eq!(rows[0].outcome, Outcome::Error, "{:?}", rows[0]);
    assert!(rows[0].reached_server, "{:?}", rows[0]);
    for r in &rows[1..] {
        assert_eq!(r.outcome, Outcome::Error, "{r:?}");
        assert!(!r.reached_server, "{r:?}");
    }
}

#[tokio::test]
async fn the_report_says_who_won_when_one_model_is_clearly_better() {
    let cases = cases(40, 0);
    let good = serve(Oracle::new(&cases)).await;
    // `bad` answers 24 of 40 wrongly: 40% accuracy.
    let mut bad_oracle = Oracle::new(&cases);
    bad_oracle.wrong = cases.iter().take(24).map(premise_of).collect();
    let bad = serve(bad_oracle).await;
    let cfg = BenchConfig {
        models: vec![spec("good", &good.uri()), spec("bad", &bad.uri())],
    };
    let both = ids(&["good", "bad"]);
    let rows = run(&cfg, &cases, &both).await.unwrap();
    let results = Results {
        generated_at: 1_760_961_600,
        manifest: None,
        not_run: vec![],
        roster: cfg.models.iter().map(RosterEntry::from).collect(),
        labels_reviewed: vec![],
        completed: vec![],
        rows,
    };
    let rankings = rank_all(&results.rows, &both, SEED);
    let s3 = &rankings[0];
    assert_eq!(s3.shared_cases, 40);
    assert_eq!(s3.verdict, Verdict::Winner("good".into()));
    let bad_acc = s3
        .ranked
        .iter()
        .find(|m| m.id == "bad")
        .unwrap()
        .shared
        .accuracy;
    assert_eq!(bad_acc, Some(0.4));
    let md = render(&results, SEED);
    assert!(md.contains("**Use good for"), "{md}");
    assert!(!md.contains("## NOT RUN"), "{md}");
}
