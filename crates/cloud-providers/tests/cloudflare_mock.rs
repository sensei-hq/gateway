//! SP-DEC-2 T3 — Cloudflare Workers AI decisions (`clef`, `clef-flash`)
//! against a mock server, built from the contract verified 2026-10-03:
//!
//! - only `POST {base}/run/@cf/cloudflare/{model}` exists, where the
//!   operator's `url` is `https://api.cloudflare.com/client/v4/accounts/<id>/ai`
//!   (no `/v1/systemone`);
//! - the body `model` is the SHORT name (`^\s*(clef|clef-flash)\s*$`), the
//!   `@cf/...` id goes in the path; the schema defines no `keep_alive`;
//! - images are data URLs (a bare base64 string matches neither schema branch);
//! - REST wraps the output: `{result:{model,answers,usage}, success, errors,
//!   messages}`; errors use the v4 envelope (live 401: code 10000
//!   "Authentication error").

use std::collections::HashMap;

use kernel::adapters::capability::DecisionModel;
use kernel::types::config::RouterConfig;
use kernel::types::decision::{DecisionAnswer, DecisionQuestion, DecisionQuestions};
use kernel::types::error::GatewayError;
use kernel::types::io::DecisionRequest;

use cloud_providers::cloudflare::CloudflareAdapter;

use serde_json::json;
use wiremock::matchers::{body_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const ACCOUNT_BASE: &str = "/client/v4/accounts/0123456789abcdef0123456789abcdef/ai";
const PNG_B64: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJ";

fn cfg(server: &MockServer) -> RouterConfig {
    RouterConfig {
        url: format!("{}{ACCOUNT_BASE}", server.uri()),
        api_key: Some("cf-token".into()),
        api_key_env: None,
        enabled: true,
        timeout_ms: Some(5000),
        headers: HashMap::new(),
    }
}

fn request(model: Option<&str>, images: Vec<String>) -> DecisionRequest {
    DecisionRequest {
        model: model.map(str::to_string),
        state: json!("A user took this screenshot and wants to know what it shows."),
        questions: DecisionQuestions::from([(
            "has_ollama".to_string(),
            DecisionQuestion::Noul {
                instructions: json!("Does this image contain Ollama?"),
                criteria: None,
            },
        )]),
        images,
        keep_alive: Some(json!("5m")),
    }
}

/// The documented envelope around a System One result.
fn envelope(result: serde_json::Value) -> serde_json::Value {
    json!({"result": result, "success": true, "errors": [], "messages": []})
}

fn clef_result(model: &str) -> serde_json::Value {
    json!({
        "model": model,
        "answers": {"has_ollama": {"type": "noul", "noul": 0.959}},
        "usage": {"input_tokens": 677, "output_tokens": 0}
    })
}

#[tokio::test]
async fn clef_is_called_at_run_with_the_short_model_and_the_envelope_is_unwrapped() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!("{ACCOUNT_BASE}/run/@cf/cloudflare/clef")))
        .and(header("authorization", "Bearer cf-token"))
        // Exactly the schema's fields: short model, no keep_alive, data-URL image.
        .and(body_json(json!({
            "model": "clef",
            "state": "A user took this screenshot and wants to know what it shows.",
            "questions": {"has_ollama": {"type": "noul", "instructions": "Does this image contain Ollama?"}},
            "images": [format!("data:image/png;base64,{PNG_B64}")]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(envelope(clef_result("clef"))))
        .expect(1)
        .mount(&server)
        .await;

    let resp = CloudflareAdapter::new()
        .unwrap()
        .decide(&cfg(&server), &request(Some("clef"), vec![PNG_B64.into()]))
        .await
        .expect("clef answers");
    assert_eq!(
        resp.answers["has_ollama"],
        DecisionAnswer::Noul { noul: 0.959 }
    );
    assert_eq!(
        resp.usage.map(|u| (u.input_tokens, u.output_tokens)),
        Some((677, 0))
    );
    assert_eq!(resp.model.as_deref(), Some("clef"));
}

/// The catalog id `@cf/cloudflare/clef-flash` and the short `clef-flash` are
/// the same model: path takes the id, body takes the short name, either way.
#[tokio::test]
async fn the_full_workers_ai_id_is_accepted_as_the_model() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(format!(
            "{ACCOUNT_BASE}/run/@cf/cloudflare/clef-flash"
        )))
        .and(wiremock::matchers::body_partial_json(
            json!({"model": "clef-flash"}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(envelope(clef_result("clef-flash"))))
        .expect(2)
        .mount(&server)
        .await;
    let adapter = CloudflareAdapter::new().unwrap();
    for model in ["@cf/cloudflare/clef-flash", "clef-flash"] {
        adapter
            .decide(&cfg(&server), &request(Some(model), vec![]))
            .await
            .unwrap_or_else(|e| panic!("{model}: {e}"));
    }
}

#[tokio::test]
async fn a_v4_401_is_an_authentication_error_carrying_cloudflares_message() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({
            "result": null, "success": false,
            "errors": [{"code": 10000, "message": "Authentication error"}], "messages": []
        })))
        .mount(&server)
        .await;
    match CloudflareAdapter::new()
        .unwrap()
        .decide(&cfg(&server), &request(Some("clef"), vec![]))
        .await
        .unwrap_err()
    {
        GatewayError::Authentication { adapter, message } => {
            assert_eq!(adapter, "cloudflare");
            assert_eq!(message, "Authentication error");
        }
        other => panic!("expected Authentication, got {other:?}"),
    }
}

#[tokio::test]
async fn a_v4_400_is_a_provider_error_with_the_status_and_message() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "result": null, "success": false,
            "errors": [{"code": 5007, "message": "No such model"}], "messages": []
        })))
        .mount(&server)
        .await;
    match CloudflareAdapter::new()
        .unwrap()
        .decide(&cfg(&server), &request(Some("clef"), vec![]))
        .await
        .unwrap_err()
    {
        GatewayError::ProviderError {
            status: Some(400),
            message,
            ..
        } => assert_eq!(message, "No such model"),
        other => panic!("expected ProviderError 400, got {other:?}"),
    }
}

/// A 2xx whose envelope says `success: false` (or carries no result) did not
/// answer — it must not read as a success with no answers.
#[tokio::test]
async fn a_2xx_envelope_without_success_is_a_provider_error() {
    for body in [
        json!({"result": null, "success": false, "errors": [{"code": 3040, "message": "Out of capacity"}], "messages": []}),
        json!({"result": null, "success": true, "errors": [], "messages": []}),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body.clone()))
            .mount(&server)
            .await;
        match CloudflareAdapter::new()
            .unwrap()
            .decide(&cfg(&server), &request(Some("clef"), vec![]))
            .await
            .unwrap_err()
        {
            GatewayError::ProviderError {
                status: Some(200), ..
            } => {}
            other => panic!("{body}: expected ProviderError, got {other:?}"),
        }
    }
}

/// The account id lives in `url`, so there is no sensible default host — and
/// no default model: guessing one would bill the caller for a model they
/// never chose.
#[tokio::test]
async fn a_missing_url_or_model_fails_before_any_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&server)
        .await;
    let adapter = CloudflareAdapter::new().unwrap();
    let mut no_url = cfg(&server);
    no_url.url = String::new();
    assert!(matches!(
        adapter.decide(&no_url, &request(Some("clef"), vec![])).await,
        Err(GatewayError::InvalidConfig(m)) if m.contains("accounts/")
    ));
    assert!(matches!(
        adapter.decide(&cfg(&server), &request(None, vec![])).await,
        Err(GatewayError::InvalidRequest { message }) if message.contains("model")
    ));
}

/// The completeness check applies here too: a result missing an asked
/// question is a failed attempt, not success with holes.
#[tokio::test]
async fn a_result_missing_an_asked_question_is_a_provider_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(envelope(json!({
            "model": "clef", "answers": {}, "usage": {"input_tokens": 1, "output_tokens": 0}
        }))))
        .mount(&server)
        .await;
    match CloudflareAdapter::new()
        .unwrap()
        .decide(&cfg(&server), &request(Some("clef"), vec![]))
        .await
        .unwrap_err()
    {
        GatewayError::ProviderError { message, .. } => assert!(message.contains("has_ollama")),
        other => panic!("expected ProviderError, got {other:?}"),
    }
}

/// Live, opt-in: a real Workers AI account. Needs CLOUDFLARE_ACCOUNT_ID and
/// CLOUDFLARE_API_TOKEN (Workers AI Read + Edit).
#[tokio::test]
#[ignore = "requires CLOUDFLARE_ACCOUNT_ID + CLOUDFLARE_API_TOKEN"]
async fn live_clef_flash_answers() {
    let account = std::env::var("CLOUDFLARE_ACCOUNT_ID").expect("CLOUDFLARE_ACCOUNT_ID");
    let cfg = RouterConfig {
        url: format!("https://api.cloudflare.com/client/v4/accounts/{account}/ai"),
        api_key: None,
        api_key_env: Some("CLOUDFLARE_API_TOKEN".into()),
        enabled: true,
        timeout_ms: Some(120_000),
        headers: HashMap::new(),
    };
    let resp = CloudflareAdapter::from_config(&cfg)
        .unwrap()
        .decide(&cfg, &request(Some("clef-flash"), vec![]))
        .await
        .expect("live clef-flash");
    assert!(matches!(
        resp.answers["has_ollama"],
        DecisionAnswer::Noul { .. }
    ));
}
