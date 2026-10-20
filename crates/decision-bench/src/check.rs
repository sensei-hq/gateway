//! `check`: probe every selected model with one tiny decision, so a model whose
//! server is down is reported NOT RUN up front instead of filling a run with
//! errors (spec D4, AC1). It never fails and never stops early: every model
//! gets a [`CheckResult`].

use std::time::Duration;

use gateway::types::decision::DecisionQuestion;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::case::{Case, Expected, Scenario};
use crate::config::{BenchConfig, ModelSpec};
use crate::run::{Outcome, gateway_with_timeout, redact, request_for, row_for_model};

/// How long one probe may take. A cold model load (Ollama loads lazily) can
/// take several seconds; this is long enough for that, and far shorter than
/// the 120 s per-case router timeout a run uses.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum CheckStatus {
    /// The server answered: a decision, or a refusal of the input.
    Reachable,
    NotRun {
        reason: String,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CheckResult {
    pub model: String,
    pub status: CheckStatus,
}

/// [`check_with_timeout`] with [`PROBE_TIMEOUT`].
pub async fn check(cfg: &BenchConfig, model_ids: &[String]) -> Vec<CheckResult> {
    check_with_timeout(cfg, model_ids, PROBE_TIMEOUT).await
}

/// Probe each of `model_ids` (config order; ids not in the config last, each
/// NOT RUN) with one noul decision, `state: "x"`, through
/// [`gateway_with_timeout`]: its router timeout is `timeout`, and the whole
/// call is cut off shortly after it in case a server holds the connection.
///
/// - `Reachable`: an answer, or a refusal of the input that came back after
///   the request was dispatched (`InputRejected`, e.g. server-side
///   truncation): the server answered.
/// - A refusal before anything was sent (the gateway's context-window gate;
///   the tiniest probe is estimated at well over 96 tokens) proves nothing
///   about the server. Such a model is `Reachable` iff another probed model
///   with the same router and URL answered, else NOT RUN.
/// - `NotRun { reason }`: an invalid model config, a network failure, a
///   timeout, a 5xx and every other error.
pub async fn check_with_timeout(
    cfg: &BenchConfig,
    model_ids: &[String],
    timeout: Duration,
) -> Vec<CheckResult> {
    let mut probed: Vec<(&ModelSpec, Probe)> = Vec::new();
    for m in cfg.models.iter().filter(|m| model_ids.contains(&m.id)) {
        probed.push((m, probe(cfg, m, timeout).await));
    }
    let server_up = |m: &ModelSpec| {
        probed
            .iter()
            .any(|(o, p)| matches!(p, Probe::Answered) && o.router == m.router && o.url == m.url)
    };
    let mut out: Vec<CheckResult> = probed
        .iter()
        .map(|(m, p)| CheckResult {
            model: m.id.clone(),
            status: match p {
                Probe::Answered => CheckStatus::Reachable,
                Probe::Gated(_) if server_up(m) => CheckStatus::Reachable,
                Probe::Gated(reason) => CheckStatus::NotRun {
                    reason: format!(
                        "the probe never reached the server: {reason}; and no other model \
                         on the same {} server answered, so the server is unverified",
                        m.router
                    ),
                },
                Probe::Failed(reason) => CheckStatus::NotRun {
                    reason: reason.clone(),
                },
            },
        })
        .collect();
    for id in model_ids {
        if !cfg.models.iter().any(|m| &m.id == id) {
            out.push(CheckResult {
                model: id.clone(),
                status: CheckStatus::NotRun {
                    reason: "not in the bench config".into(),
                },
            });
        }
    }
    out
}

/// What one probe showed.
enum Probe {
    /// The server answered (a decision, or a refusal after dispatch).
    Answered,
    /// Refused before dispatch; the reason.
    Gated(String),
    Failed(String),
}

/// The probe case: a yes/no question on a one-character state.
pub fn probe_case() -> Case {
    Case {
        id: "check-probe".into(),
        scenario: Scenario::S3Entailment,
        state: json!("x"),
        question: DecisionQuestion::Noul {
            instructions: json!("Is this a test?"),
            criteria: None,
        },
        expected: Expected::Noul { value: true },
        source: "check".into(),
    }
}

async fn probe(cfg: &BenchConfig, m: &ModelSpec, timeout: Duration) -> Probe {
    let gw = match gateway_with_timeout(cfg, m, timeout).await {
        Ok(gw) => gw,
        Err(e) => return Probe::Failed(redact(&format!("invalid config: {e:#}"), m)),
    };
    let case = probe_case();
    let request = request_for(&case, m);
    let result = match tokio::time::timeout(timeout + CUTOFF_GRACE, gw.execute(&request)).await {
        Err(_) => {
            return Probe::Failed(format!("no answer within {}s", timeout.as_secs_f64()));
        }
        Ok(Ok(_)) => return Probe::Answered,
        Ok(result) => result,
    };
    // `row_for` owns the "did this reach the server" rules for an error.
    let row = row_for_model(&case, m, &result, 0);
    let reason = row.detail.unwrap_or_default();
    match (row.outcome, row.reached_server) {
        (Outcome::NotApplicable, true) => Probe::Answered,
        (Outcome::NotApplicable, false) => Probe::Gated(reason),
        _ => Probe::Failed(reason),
    }
}

/// Slack past the router timeout before the probe is abandoned outright.
const CUTOFF_GRACE: Duration = Duration::from_secs(2);

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn spec(id: &str, url: &str) -> ModelSpec {
        ModelSpec {
            id: id.into(),
            router: "llamacpp".into(),
            url: url.into(),
            api_model_id: "nimble-v3".into(),
            context_window: 8192,
            api_key_env: None,
        }
    }

    fn ids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    fn noul_answer() -> serde_json::Value {
        json!({
            "model": "nimble-v3",
            "answers": {"q": {"type": "noul", "noul": 0.9}},
            "usage": {"input_tokens": 30, "output_tokens": 0}
        })
    }

    async fn serving(template: ResponseTemplate) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(template)
            .mount(&server)
            .await;
        server
    }

    fn not_run_reason(r: &CheckResult) -> &str {
        match &r.status {
            CheckStatus::NotRun { reason } => reason,
            CheckStatus::Reachable => panic!("{} should be NOT RUN", r.model),
        }
    }

    #[tokio::test]
    async fn a_good_router_is_reachable_and_an_unreachable_one_is_not_run() {
        let good = serving(ResponseTemplate::new(200).set_body_json(noul_answer())).await;
        let cfg = BenchConfig {
            models: vec![
                spec("good", &good.uri()),
                spec("down", "http://127.0.0.1:9"),
            ],
        };
        let got = check(&cfg, &ids(&["good", "down"])).await;
        assert_eq!(got.len(), 2, "{got:?}");
        assert_eq!(got[0].model, "good");
        assert_eq!(got[0].status, CheckStatus::Reachable);
        assert_eq!(got[1].model, "down");
        assert!(!not_run_reason(&got[1]).is_empty());
    }

    /// T7 review I1: neither the NOT RUN reason nor the report carries the
    /// model URL (a hosted one can hold a token).
    #[tokio::test]
    async fn a_not_run_reason_and_its_report_never_carry_the_url() {
        let cfg = BenchConfig {
            models: vec![spec("down", "http://127.0.0.1:9/?token=SECRET")],
        };
        let got = check(&cfg, &ids(&["down"])).await;
        let reason = not_run_reason(&got[0]).to_string();
        assert!(!reason.contains("SECRET"), "{reason}");
        let results = crate::run::Results {
            generated_at: 0,
            manifest: None,
            not_run: vec![crate::run::NotRun {
                model: "down".into(),
                reason,
            }],
            roster: vec![(&cfg.models[0]).into()],
            labels_reviewed: vec![],
            completed: vec![],
            rows: vec![],
        };
        let md = crate::report::render(&results, 1);
        assert!(md.contains("**down**"), "{md}");
        assert!(!md.contains("SECRET"), "{md}");
        let json = serde_json::to_string(&results).unwrap();
        assert!(!json.contains("SECRET"), "{json}");
    }

    #[tokio::test]
    async fn the_probe_is_one_tiny_noul_decision_per_model() {
        let good = serving(ResponseTemplate::new(200).set_body_json(noul_answer())).await;
        let cfg = BenchConfig {
            models: vec![spec("good", &good.uri())],
        };
        check(&cfg, &ids(&["good"])).await;
        let reqs = good.received_requests().await.unwrap();
        assert_eq!(reqs.len(), 1, "one probe, no warm-up");
        let body = String::from_utf8_lossy(&reqs[0].body);
        assert!(body.contains("Is this a test?"), "{body}");
    }

    #[tokio::test]
    async fn a_server_error_is_not_run_with_the_reason() {
        let bad = serving(ResponseTemplate::new(503).set_body_string("overloaded")).await;
        let cfg = BenchConfig {
            models: vec![spec("bad", &bad.uri())],
        };
        let got = check(&cfg, &ids(&["bad"])).await;
        assert!(not_run_reason(&got[0]).contains("503"), "{got:?}");
    }

    /// A refusal of the input (`InputRejected` → not_applicable) means the
    /// server answered, so the model is reachable.
    #[tokio::test]
    async fn an_input_rejection_counts_as_reachable() {
        let mut body = noul_answer();
        body["usage"]["truncated"] = json!(true);
        let picky = serving(ResponseTemplate::new(200).set_body_json(body)).await;
        let cfg = BenchConfig {
            models: vec![spec("picky", &picky.uri())],
        };
        let got = check(&cfg, &ids(&["picky"])).await;
        assert_eq!(got[0].status, CheckStatus::Reachable, "{got:?}");
    }

    /// A probe the gateway refuses before sending (context window) says
    /// nothing about the server: alone, and with the server down, the model is
    /// NOT RUN, not reachable.
    #[tokio::test]
    async fn a_probe_gated_before_sending_does_not_prove_the_server_is_up() {
        let mut tiny = spec("tiny", "http://127.0.0.1:9");
        tiny.context_window = 1;
        let cfg = BenchConfig { models: vec![tiny] };
        let got = check(&cfg, &ids(&["tiny"])).await;
        let reason = not_run_reason(&got[0]);
        assert!(reason.contains("never reached the server"), "{reason}");
        assert!(reason.contains("context window"), "{reason}");
    }

    /// ...but a sibling on the same router and URL that answered shows the
    /// server is up, so the gated model is reachable (its cases will be
    /// recorded as not_applicable where they do not fit).
    #[tokio::test]
    async fn a_gated_probe_is_reachable_when_a_sibling_on_the_same_server_answered() {
        let good = serving(ResponseTemplate::new(200).set_body_json(noul_answer())).await;
        let mut tiny = spec("tiny", &good.uri());
        tiny.context_window = 1;
        let mut elsewhere = spec("elsewhere", "http://127.0.0.1:9");
        elsewhere.context_window = 1;
        let cfg = BenchConfig {
            models: vec![tiny, spec("big", &good.uri()), elsewhere],
        };
        let got = check(&cfg, &ids(&["tiny", "big", "elsewhere"])).await;
        assert_eq!(got[0].status, CheckStatus::Reachable, "{got:?}");
        assert_eq!(got[1].status, CheckStatus::Reachable, "{got:?}");
        assert!(not_run_reason(&got[2]).contains("never reached the server"));
        // Only `big`'s probe was sent.
        assert_eq!(good.received_requests().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_slow_server_is_not_run_after_the_probe_timeout_not_the_run_timeout() {
        let slow = serving(
            ResponseTemplate::new(200)
                .set_body_json(noul_answer())
                .set_delay(Duration::from_secs(5)),
        )
        .await;
        let cfg = BenchConfig {
            models: vec![spec("slow", &slow.uri())],
        };
        let started = Instant::now();
        let got = check_with_timeout(&cfg, &ids(&["slow"]), Duration::from_millis(300)).await;
        assert!(
            // Under the 2 s cut-off grace: the router timeout itself fired.
            started.elapsed() < Duration::from_millis(1500),
            "took {:?}",
            started.elapsed()
        );
        assert!(!not_run_reason(&got[0]).is_empty());
    }

    #[tokio::test]
    async fn an_unknown_or_invalid_model_is_not_run_and_the_rest_still_probe() {
        let good = serving(ResponseTemplate::new(200).set_body_json(noul_answer())).await;
        let cfg = BenchConfig {
            models: vec![spec("broken", ""), spec("good", &good.uri())],
        };
        let got = check(&cfg, &ids(&["broken", "good", "ghost"])).await;
        let models: Vec<_> = got.iter().map(|r| r.model.as_str()).collect();
        assert_eq!(models, ["broken", "good", "ghost"]);
        assert!(not_run_reason(&got[0]).contains("empty URL"), "{got:?}");
        assert_eq!(got[1].status, CheckStatus::Reachable);
        assert!(not_run_reason(&got[2]).contains("not in the bench config"));
    }
}
