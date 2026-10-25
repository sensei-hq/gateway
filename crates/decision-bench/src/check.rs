//! `check`: probe every selected model with one tiny decision, so a model whose
//! server is down is reported NOT RUN up front instead of filling a run with
//! errors (spec D4, AC1). It never fails and never stops early: every model
//! gets a [`CheckResult`].

use std::time::Duration;

use gateway::GatewayError;
use gateway::types::decision::DecisionQuestion;
use gateway::types::error::HumanAction;
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

/// [`check_with_timeouts`] with the router timeout `timeout` and a hard
/// cut-off `CUTOFF_GRACE` after it, in case a server holds the connection.
pub async fn check_with_timeout(
    cfg: &BenchConfig,
    model_ids: &[String],
    timeout: Duration,
) -> Vec<CheckResult> {
    check_with_timeouts(cfg, model_ids, timeout, timeout + CUTOFF_GRACE).await
}

/// Probe each of `model_ids` (config order; ids not in the config last, each
/// NOT RUN) with one noul decision, `state: "x"`, through
/// [`gateway_with_timeout`] (router timeout `router_timeout`), abandoning the
/// call outright after `cut_off`.
///
/// - `Reachable`: an answer, or a refusal of the input that came back after
///   the request was dispatched (`InputRejected`, e.g. server-side
///   truncation): the server answered.
/// - NOT RUN, "no input fits: …": the gateway's context-window gate refused
///   the minimal decision before sending, so no case could ever be sent (the
///   example roster's `laya-ane`, 96 tokens).
/// - NOT RUN with its reason: an invalid model entry, any other pre-send
///   gate, a network failure, a timeout, a 5xx and every other error.
pub async fn check_with_timeouts(
    cfg: &BenchConfig,
    model_ids: &[String],
    router_timeout: Duration,
    cut_off: Duration,
) -> Vec<CheckResult> {
    let mut out = Vec::with_capacity(model_ids.len());
    for m in cfg.models.iter().filter(|m| model_ids.contains(&m.id)) {
        out.push(CheckResult {
            model: m.id.clone(),
            status: probe(cfg, m, router_timeout, cut_off).await,
        });
    }
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

/// `(estimate, window)` from the context-window gate's skip text, e.g.
/// `"… estimated 182 input tokens exceeds the model's 96-token context window"`.
fn parse_window_gate(text: &str) -> Option<(u64, u64)> {
    const EST: &str = "estimated ";
    let after = &text[text.find(EST)? + EST.len()..];
    let estimate = after.split(' ').next()?.parse().ok()?;
    let before = &text[..text.find("-token context window")?];
    let window = before.rsplit(' ').next()?.parse().ok()?;
    Some((estimate, window))
}

/// The NOT RUN reason for a probe the context-window gate refused.
fn no_fit_reason(skipped: &[String], m: &ModelSpec) -> String {
    let text = skipped.join("; ");
    match parse_window_gate(&text) {
        Some((estimate, window)) => format!(
            "no input fits: the gateway estimates the minimal decision at {estimate} input \
             tokens > the model's {window}-token context window"
        ),
        None => format!("no input fits: {}", redact(&text, m)),
    }
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

async fn probe(
    cfg: &BenchConfig,
    m: &ModelSpec,
    router_timeout: Duration,
    cut_off: Duration,
) -> CheckStatus {
    let not_run = |reason: String| CheckStatus::NotRun { reason };
    let gw = match gateway_with_timeout(cfg, m, router_timeout).await {
        Ok(gw) => gw,
        Err(e) => return not_run(redact(&format!("invalid config: {e:#}"), m)),
    };
    let case = probe_case();
    let request = request_for(&case, m);
    let result = match tokio::time::timeout(cut_off, gw.execute(&request)).await {
        Err(_) => return not_run(format!("no answer within {}s", cut_off.as_secs_f64())),
        Ok(Ok(_)) => return CheckStatus::Reachable,
        Ok(result) => result,
    };
    if let Err(GatewayError::AllGated {
        human_action: Some(HumanAction::UseLargerContextWindow),
        skipped,
        ..
    }) = &result
    {
        return not_run(no_fit_reason(skipped, m));
    }
    // `row_for_model` owns the "did this reach the server" rules for an error.
    let row = row_for_model(&case, m, &result, 0, router_timeout);
    match (row.outcome, row.reached_server) {
        (Outcome::NotApplicable, true) => CheckStatus::Reachable,
        _ => not_run(row.detail.unwrap_or_else(|| "the probe failed".into())),
    }
}

/// Slack past the router timeout before the probe is abandoned outright.
const CUTOFF_GRACE: Duration = Duration::from_secs(2);

/// How long `/api/show` may take. It is a metadata read, not a generation.
pub const WINDOW_PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// The context window the **server** will honour for `model`, not the model
/// card's (Ollama's `num_ctx` default is far below the card, and it refuses
/// input above it rather than truncating).
///
/// Only Ollama is asked; every other router returns `Ok(None)` — a missing
/// answer, never a guess. An unknown model id, an unreachable server or a
/// non-2xx answer is an error, so a caller can tell "not probed" from
/// "probed and absent".
pub async fn probe_decision_window(cfg: &BenchConfig, model: &str) -> anyhow::Result<Option<u32>> {
    use anyhow::Context;
    let m = cfg
        .models
        .iter()
        .find(|m| m.id == model)
        .ok_or_else(|| anyhow::anyhow!("unknown model '{model}': not in the bench config"))?;
    if m.router != "ollama" {
        return Ok(None);
    }
    let url = format!("{}/api/show", m.url.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .timeout(WINDOW_PROBE_TIMEOUT)
        .build()
        .context("building the window-probe client")?;
    let response = client
        .post(&url)
        .json(&json!({ "model": m.api_model_id }))
        .send()
        .await
        .with_context(|| format!("probing {url}"))?;
    let status = response.status();
    if !status.is_success() {
        anyhow::bail!("{url} answered {status} for model '{}'", m.api_model_id);
    }
    let body: serde_json::Value = response
        .json()
        .await
        .with_context(|| format!("parsing the {url} response"))?;
    Ok(window_from_show(&body))
}

/// `parameters`' `num_ctx` (the runtime value Ollama actually enforces), else
/// the architecture's `*.context_length` from `model_info`.
fn window_from_show(body: &serde_json::Value) -> Option<u32> {
    if let Some(text) = body.get("parameters").and_then(serde_json::Value::as_str) {
        for line in text.lines() {
            let mut words = line.split_whitespace();
            if words.next() == Some("num_ctx")
                && let Some(n) = words.next().and_then(|v| v.parse().ok())
            {
                return Some(n);
            }
        }
    }
    let info = body.get("model_info")?.as_object()?;
    if let Some(n) = info.get("general.context_length").and_then(|v| v.as_u64()) {
        return u32::try_from(n).ok();
    }
    // `qwen2.context_length`, `llama.context_length`, …: take the largest, so a
    // hypothetical per-layer value cannot shadow the model's own.
    info.iter()
        .filter(|(k, _)| k.ends_with(".context_length"))
        .filter_map(|(_, v)| v.as_u64())
        .max()
        .and_then(|n| u32::try_from(n).ok())
}

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
            paths: None,
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

    const NO_FIT: &str = "no input fits: the gateway estimates the minimal decision at ";

    /// T7 review I3 (user decision, option 1): a probe the context-window gate
    /// refuses before sending means no case can ever be sent, so the model is
    /// NOT RUN with that reason, even though its server is up and answers
    /// other models.
    #[tokio::test]
    async fn a_context_window_gated_probe_is_not_run_even_when_the_server_is_up() {
        let good = serving(ResponseTemplate::new(200).set_body_json(noul_answer())).await;
        let mut tiny = spec("tiny", &good.uri());
        tiny.context_window = 1;
        let cfg = BenchConfig {
            models: vec![tiny, spec("big", &good.uri())],
        };
        let got = check(&cfg, &ids(&["tiny", "big"])).await;
        let reason = not_run_reason(&got[0]);
        assert!(reason.starts_with(NO_FIT), "{reason}");
        assert!(
            reason.ends_with(" input tokens > the model's 1-token context window"),
            "{reason}"
        );
        assert_eq!(got[1].status, CheckStatus::Reachable, "{got:?}");
        assert_eq!(
            good.received_requests().await.unwrap().len(),
            1,
            "only big's"
        );
    }

    /// The example roster's `laya-ane` (96-token window) can never be sent
    /// anything; against a live server it is still NOT RUN, and says why.
    #[tokio::test]
    async fn the_example_laya_ane_is_not_run_because_no_input_fits() {
        let cfg = BenchConfig::from_toml(include_str!("../bench.example.toml")).unwrap();
        let mut ane = cfg
            .models
            .iter()
            .find(|m| m.id == "laya-ane")
            .unwrap()
            .clone();
        assert_eq!(ane.context_window, 96);
        let server = serving(ResponseTemplate::new(200).set_body_json(noul_answer())).await;
        ane.url = server.uri();
        let cfg = BenchConfig { models: vec![ane] };
        let got = check(&cfg, &ids(&["laya-ane"])).await;
        let reason = not_run_reason(&got[0]);
        assert!(reason.starts_with(NO_FIT), "{reason}");
        assert!(
            reason.contains("> the model's 96-token context window"),
            "{reason}"
        );
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[test]
    fn the_gate_text_yields_the_estimate_and_window() {
        assert_eq!(
            parse_window_gate(
                "laya:laya-ane — estimated 182 input tokens exceeds the model's 96-token \
                 context window"
            ),
            Some((182, 96))
        );
        assert_eq!(parse_window_gate("something else entirely"), None);
        assert_eq!(parse_window_gate("estimated x input tokens"), None);
    }

    /// T7 review M4: the hard cut-off fires on its own when the router
    /// timeout is longer.
    #[tokio::test]
    async fn the_hard_cut_off_fires_when_the_router_timeout_is_longer() {
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
        let got = check_with_timeouts(
            &cfg,
            &ids(&["slow"]),
            Duration::from_secs(30),
            Duration::from_millis(300),
        )
        .await;
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "{:?}",
            started.elapsed()
        );
        assert!(
            not_run_reason(&got[0]).starts_with("no answer within"),
            "{got:?}"
        );
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

    fn ollama(id: &str, url: &str) -> ModelSpec {
        ModelSpec {
            router: "ollama".into(),
            ..spec(id, url)
        }
    }

    async fn serving_show(body: serde_json::Value) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(wiremock::matchers::path("/api/show"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;
        server
    }

    /// The runtime `num_ctx` wins over the architecture's card-derived
    /// `model_info` value — the whole reason to probe.
    #[tokio::test]
    async fn the_probed_window_is_num_ctx_not_the_model_cards() {
        let server = serving_show(json!({
            "parameters": "num_ctx 8194\ntemperature 0.7",
            "model_info": { "general.architecture": "qwen2", "qwen2.context_length": 262144 }
        }))
        .await;
        let cfg = BenchConfig {
            models: vec![ollama("nimble", &server.uri())],
        };
        let window = probe_decision_window(&cfg, "nimble").await.unwrap();
        assert_eq!(window, Some(8194));
    }

    #[tokio::test]
    async fn model_info_is_the_fallback_when_parameters_has_no_num_ctx() {
        let server = serving_show(json!({
            "parameters": "temperature 0.7",
            "model_info": { "llama.context_length": 131072 }
        }))
        .await;
        let cfg = BenchConfig {
            models: vec![ollama("laya", &server.uri())],
        };
        assert_eq!(
            probe_decision_window(&cfg, "laya").await.unwrap(),
            Some(131072)
        );
    }

    #[tokio::test]
    async fn a_non_ollama_router_is_not_probed_and_an_unknown_model_is_an_error() {
        let cfg = BenchConfig {
            models: vec![spec("llamacpp-model", "http://127.0.0.1:9")],
        };
        assert_eq!(
            probe_decision_window(&cfg, "llamacpp-model").await.unwrap(),
            None,
            "only Ollama publishes a server-side window"
        );
        assert!(probe_decision_window(&cfg, "ghost").await.is_err());
    }

    #[tokio::test]
    async fn an_error_response_is_an_error_not_a_silent_none() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(404).set_body_string("model not found"))
            .mount(&server)
            .await;
        let cfg = BenchConfig {
            models: vec![ollama("missing", &server.uri())],
        };
        let err = format!(
            "{:#}",
            probe_decision_window(&cfg, "missing").await.unwrap_err()
        );
        assert!(err.contains("404"), "{err}");
    }
}
