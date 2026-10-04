//! SP-DEC-1 T4 — System One decision calls against a mock server.
//!
//! Ollama (keyless, `{url}/v1/systemone`) and the generic `SystemOneAdapter`
//! (bearer auth — OpenRouter's base is `https://openrouter.ai/api`, so its
//! path is `/api/v1/systemone`). Error bodies are the ones captured from a
//! live Ollama 0.35.0, including the plain-text 404 a pre-0.35 server returns
//! for an endpoint it does not have.

use std::collections::HashMap;

use kernel::adapters::capability::DecisionModel;
use kernel::types::config::RouterConfig;
use kernel::types::decision::{DecisionAnswer, DecisionQuestion, DecisionQuestions};
use kernel::types::error::GatewayError;
use kernel::types::io::DecisionRequest;

use cloud_providers::ollama::OllamaAdapter;
use cloud_providers::systemone::{DecisionModelStatus, SystemOneAdapter};

use serde_json::json;
use wiremock::matchers::{body_json, body_partial_json, header, header_exists, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

mod common;
use common::{assert_authentication_error, router_config};

fn keyless(url: &str) -> RouterConfig {
    RouterConfig {
        url: url.to_string(),
        api_key: None,
        api_key_env: None,
        enabled: true,
        timeout_ms: Some(5000),
        headers: HashMap::new(),
    }
}

fn questions() -> DecisionQuestions {
    DecisionQuestions::from([(
        "label".to_string(),
        DecisionQuestion::Choice {
            instructions: json!("Which label fits this ticket?"),
            criteria: [
                (
                    "billing".to_string(),
                    Some("Payments and refunds".to_string()),
                ),
                ("bug".to_string(), None),
            ]
            .into_iter()
            .collect(),
        },
    )])
}

fn request(
    model: Option<&str>,
    images: Vec<String>,
    keep_alive: Option<serde_json::Value>,
) -> DecisionRequest {
    DecisionRequest {
        model: model.map(str::to_string),
        state: json!("Our checkout has returned 500 errors since 9am."),
        questions: questions(),
        images,
        keep_alive,
    }
}

fn ok_body(model: &str) -> serde_json::Value {
    json!({
        "model": model,
        "answers": {"label": {
            "type": "choice", "choice": "bug",
            "probabilities": {"billing": 0.0219, "bug": 0.9781},
            "confidence": 0.8906
        }},
        "usage": {"input_tokens": 174, "output_tokens": 1}
    })
}

// ---------------------------------------------------------------------------
// Ollama
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ollama_posts_the_system_one_body_and_parses_typed_answers() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .and(body_json(json!({
            "model": "nimble:9b",
            "state": "Our checkout has returned 500 errors since 9am.",
            "questions": {"label": {
                "type": "choice",
                "instructions": "Which label fits this ticket?",
                "criteria": {"billing": "Payments and refunds", "bug": null}
            }},
            "images": ["aGVsbG8="],
            "keep_alive": "5m"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body("nimble:9b")))
        .expect(1)
        .mount(&server)
        .await;

    let resp = OllamaAdapter::new()
        .unwrap()
        .decide(
            &keyless(&server.uri()),
            &request(
                Some("nimble:9b"),
                vec!["aGVsbG8=".into()],
                Some(json!("5m")),
            ),
        )
        .await
        .expect("decision succeeds");

    let DecisionAnswer::Choice {
        choice,
        probabilities,
        confidence,
    } = &resp.answers["label"]
    else {
        panic!("choice answer expected: {:?}", resp.answers);
    };
    assert_eq!(choice, "bug");
    assert_eq!(probabilities.keys().collect::<Vec<_>>(), ["billing", "bug"]);
    assert!((confidence - 0.8906).abs() < 1e-9);
    let usage = resp.usage.expect("usage mapped");
    assert_eq!(
        (usage.input_tokens, usage.output_tokens, usage.total_tokens),
        (174, 1, 175)
    );
    assert_eq!(resp.model.as_deref(), Some("nimble:9b"));
    assert!(!resp.degraded);
}

#[tokio::test]
async fn ollama_omits_absent_optionals_and_sends_no_auth_and_defaults_to_nimble() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(|req: &Request| {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            // `Value` sorts keys in this crate (no `preserve_order`), so compare
            // the key SET; order is pinned by the kernel's wire tests.
            let mut keys: Vec<_> = body.as_object().unwrap().keys().cloned().collect();
            keys.sort();
            let auth = req.headers.get("authorization").is_some();
            if keys == ["model", "questions", "state"] && body["model"] == "nimble" && !auth {
                ResponseTemplate::new(200).set_body_json(ok_body("nimble"))
            } else {
                ResponseTemplate::new(418)
                    .set_body_string(format!("keys={keys:?} auth={auth} body={body}"))
            }
        })
        .mount(&server)
        .await;

    OllamaAdapter::new()
        .unwrap()
        .decide(&keyless(&server.uri()), &request(None, vec![], None))
        .await
        .expect("bare body accepted");
}

#[tokio::test]
async fn ollama_without_the_endpoint_says_upgrade_not_model_missing() {
    // A pre-0.35 Ollama answers an unknown route with gin's plain-text 404.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(404).set_body_string("404 page not found"))
        .mount(&server)
        .await;

    let err = OllamaAdapter::new()
        .unwrap()
        .decide(
            &keyless(&server.uri()),
            &request(Some("nimble"), vec![], None),
        )
        .await
        .unwrap_err();
    match err {
        GatewayError::ProviderError {
            status: Some(404),
            message,
            ..
        } => {
            assert!(
                message.contains("0.35"),
                "should name the minimum version: {message}"
            );
            assert!(
                !message.contains("pull"),
                "not a missing-model error: {message}"
            );
        }
        other => panic!("expected ProviderError 404, got {other:?}"),
    }
}

#[tokio::test]
async fn ollama_missing_model_says_pull_it() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(
            ResponseTemplate::new(404).set_body_json(
                json!({"error": "model \"nimble\" not found, try pulling it first"}),
            ),
        )
        .mount(&server)
        .await;

    let err = OllamaAdapter::new()
        .unwrap()
        .decide(
            &keyless(&server.uri()),
            &request(Some("nimble"), vec![], None),
        )
        .await
        .unwrap_err();
    match err {
        GatewayError::ProviderError {
            status: Some(404),
            message,
            ..
        } => {
            assert!(message.contains("ollama pull nimble"), "{message}");
            assert!(!message.contains("0.35"), "not a version error: {message}");
        }
        other => panic!("expected ProviderError 404, got {other:?}"),
    }
}

#[tokio::test]
async fn ollama_non_decision_model_is_a_400_carrying_the_server_message() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": "model \"llama3.2\" is not supported by System One; use a local Nimble or Tev GGUF model"
        })))
        .mount(&server)
        .await;

    let err = OllamaAdapter::new()
        .unwrap()
        .decide(
            &keyless(&server.uri()),
            &request(Some("llama3.2"), vec![], None),
        )
        .await
        .unwrap_err();
    match err {
        GatewayError::ProviderError {
            status: Some(400),
            message,
            adapter,
        } => {
            assert_eq!(adapter, "ollama");
            assert!(message.contains("not supported by System One"), "{message}");
        }
        other => panic!("expected ProviderError 400, got {other:?}"),
    }
}

async fn probe_server(version: &str, show: Option<serde_json::Value>) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/version"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"version": version})))
        .mount(&server)
        .await;
    let show_response = match show {
        Some(body) => ResponseTemplate::new(200).set_body_json(body),
        None => {
            ResponseTemplate::new(404).set_body_json(json!({"error": "model 'nimble' not found"}))
        }
    };
    Mock::given(method("POST"))
        .and(path("/api/show"))
        .and(body_json(json!({"model": "nimble"})))
        .respond_with(show_response)
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn probe_distinguishes_too_old_not_pulled_not_decision_and_ready() {
    let adapter = OllamaAdapter::new().unwrap();
    let decision = json!({"capabilities": ["decision", "tools", "thinking", "completion"]});
    let plain = json!({"capabilities": ["completion", "tools"]});

    let cases = [
        (
            "0.34.9",
            Some(decision.clone()),
            DecisionModelStatus::ServerTooOld {
                version: "0.34.9".into(),
            },
        ),
        ("0.35.0", None, DecisionModelStatus::NotPulled),
        (
            "0.35.0",
            Some(plain),
            DecisionModelStatus::NotADecisionModel,
        ),
        ("0.35.1", Some(decision.clone()), DecisionModelStatus::Ready),
        ("1.2.0", Some(decision.clone()), DecisionModelStatus::Ready),
        // A source-built Ollama reports 0.0.0, and an unparseable version is
        // no evidence of age — neither may be called "too old".
        ("0.0.0", Some(decision.clone()), DecisionModelStatus::Ready),
        ("dev", Some(decision), DecisionModelStatus::Ready),
    ];
    for (version, show, expected) in cases {
        let server = probe_server(version, show).await;
        let status = adapter
            .probe_decision_model(&keyless(&server.uri()), "nimble")
            .await
            .unwrap_or_else(|e| panic!("probe at {version}: {e}"));
        assert_eq!(status, expected, "server version {version}");
    }
}

// ---------------------------------------------------------------------------
// SystemOneAdapter (OpenRouter / TypeSafe)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn generic_adapter_sends_bearer_to_base_plus_v1_systemone_and_ignores_extra_fields() {
    let server = MockServer::start().await;
    let mut body = ok_body("typesafe/jev-1.13");
    // OpenRouter's additions must not break parsing.
    body["id"] = json!("gen-123");
    body["provider"] = json!("TypeSafe");
    body["usage"]["cost"] = json!(0.0000073);
    Mock::given(method("POST"))
        .and(path("/api/v1/systemone"))
        .and(header("authorization", "Bearer test-key"))
        .and(header_exists("content-type"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = SystemOneAdapter::with_id("openrouter").unwrap();
    assert_eq!(
        kernel::adapters::capability::Model::id(&adapter),
        "openrouter"
    );
    let resp = adapter
        .decide(
            &router_config(&format!("{}/api", server.uri())),
            &request(Some("typesafe/jev-1.13"), vec![], None),
        )
        .await
        .expect("openrouter-shaped decision succeeds");
    assert!(matches!(
        resp.answers["label"],
        DecisionAnswer::Choice { .. }
    ));
    assert_eq!(resp.usage.map(|u| u.input_tokens), Some(174));
}

#[tokio::test]
async fn generic_adapter_maps_401_and_429_and_requires_a_model() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(401).set_body_string("bad key"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(429).insert_header("retry-after", "7"))
        .mount(&server)
        .await;

    let adapter = SystemOneAdapter::with_id("typesafe").unwrap();
    let cfg = router_config(&server.uri());
    let req = request(Some("jev-latest"), vec![], None);

    assert_authentication_error(&adapter.decide(&cfg, &req).await.unwrap_err());
    match adapter.decide(&cfg, &req).await.unwrap_err() {
        GatewayError::RateLimit { retry_after_ms, .. } => assert_eq!(retry_after_ms, Some(7000)),
        other => panic!("expected RateLimit, got {other:?}"),
    }
    // No default model exists for a generic host — say so instead of guessing.
    match adapter
        .decide(&cfg, &request(None, vec![], None))
        .await
        .unwrap_err()
    {
        GatewayError::InvalidRequest { message } => assert!(message.contains("model"), "{message}"),
        other => panic!("expected InvalidRequest, got {other:?}"),
    }
}

#[tokio::test]
async fn both_adapters_register_into_the_decision_map() {
    use kernel::adapters::{AdapterRegistry, RegisterInto};
    use std::sync::Arc;
    let reg = AdapterRegistry::new();
    Arc::new(OllamaAdapter::new().unwrap())
        .register_into(&reg)
        .await;
    Arc::new(SystemOneAdapter::with_id("openrouter").unwrap())
        .register_into(&reg)
        .await;
    assert!(reg.decision("ollama").await.is_some());
    assert!(reg.chat("ollama").await.is_some(), "ollama keeps chat");
    assert!(reg.decision("openrouter").await.is_some());
    assert!(
        reg.chat("openrouter").await.is_none(),
        "the generic adapter is decision-only"
    );
}

// ---------------------------------------------------------------------------
// Live (opt-in): a real Ollama >= 0.35 with `nimble` pulled.
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore = "requires a local Ollama >= 0.35 at OLLAMA_URL (default http://localhost:11434) with `nimble` pulled"]
async fn live_ollama_nimble_answers_and_probes() {
    let url = std::env::var("OLLAMA_URL").unwrap_or_else(|_| "http://localhost:11434".into());
    let mut cfg = keyless(&url);
    // Generous: a cold nimble load beside other resident models took 104s here.
    cfg.timeout_ms = Some(300_000);
    let adapter = OllamaAdapter::new().unwrap();

    assert_eq!(
        adapter.probe_decision_model(&cfg, "nimble").await.unwrap(),
        DecisionModelStatus::Ready
    );
    assert_eq!(
        adapter
            .probe_decision_model(&cfg, "no-such-decision-model")
            .await
            .unwrap(),
        DecisionModelStatus::NotPulled
    );

    let resp = adapter
        .decide(&cfg, &request(Some("nimble"), vec![], None))
        .await
        .expect("live decision");
    let DecisionAnswer::Choice {
        choice,
        probabilities,
        confidence,
    } = &resp.answers["label"]
    else {
        panic!("choice answer expected: {:?}", resp.answers);
    };
    assert_eq!(
        choice, "bug",
        "a 500-error ticket is a bug: {probabilities:?}"
    );
    let total: f64 = probabilities.values().sum();
    assert!(
        (total - 1.0).abs() < 1e-6,
        "probabilities sum to 1: {total}"
    );
    assert!((0.0..=1.0).contains(confidence));
    assert!(resp.usage.is_some_and(|u| u.input_tokens > 0));
}

// ---------------------------------------------------------------------------
// Review fixes (SP-DEC-1 T7)
// ---------------------------------------------------------------------------

/// A host that accepts the connection and never answers must not hang the
/// caller — even through `with_id`, whose client is built without a config.
#[tokio::test]
async fn a_silent_host_times_out_instead_of_hanging() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for s in listener.incoming().flatten() {
            held.push(s);
        }
    });
    let mut cfg = keyless(&format!("http://{addr}"));
    cfg.timeout_ms = Some(300);

    let adapter = SystemOneAdapter::with_id("llamacpp").unwrap();
    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        adapter.decide(&cfg, &request(Some("kev"), vec![], None)),
    )
    .await
    .expect("the adapter's own timeout must fire well before 10s");
    assert!(outcome.is_err(), "a silent host is an error, not a success");
}

/// The probe honours the router's `timeout_ms` too, however the adapter was
/// built — a wedged Ollama must not hang a readiness check.
#[tokio::test]
async fn the_probe_times_out_against_a_silent_ollama() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for s in listener.incoming().flatten() {
            held.push(s);
        }
    });
    let mut cfg = keyless(&format!("http://{addr}"));
    cfg.timeout_ms = Some(300);

    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        OllamaAdapter::new()
            .unwrap()
            .probe_decision_model(&cfg, "nimble"),
    )
    .await
    .expect("the probe's own timeout must fire well before 10s");
    assert!(outcome.is_err());
}

/// A 2xx that leaves any asked question unanswered is a failed attempt — the
/// engine must see an error (and fall back), not `success: true` with holes.
#[tokio::test]
async fn a_response_missing_any_asked_question_is_a_provider_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "model": "nimble",
            "answers": {"label": {"type": "noul", "noul": 0.5}},
            "usage": {"input_tokens": 10, "output_tokens": 1}
        })))
        .mount(&server)
        .await;

    let mut req = request(Some("nimble"), vec![], None);
    req.questions.insert(
        "urgent".into(),
        DecisionQuestion::Noul {
            instructions: json!("Is it urgent?"),
            criteria: None,
        },
    );
    for err in [
        OllamaAdapter::new()
            .unwrap()
            .decide(&keyless(&server.uri()), &req)
            .await
            .unwrap_err(),
        SystemOneAdapter::with_id("typesafe")
            .unwrap()
            .decide(&keyless(&server.uri()), &req)
            .await
            .unwrap_err(),
    ] {
        match err {
            GatewayError::ProviderError { message, .. } => {
                assert!(
                    message.contains("urgent"),
                    "names the missing question: {message}"
                )
            }
            other => panic!("expected ProviderError, got {other:?}"),
        }
    }
}

/// Router `headers` (attribution, proxy auth) reach decision calls too.
#[tokio::test]
async fn configured_router_headers_are_sent_on_decision_calls() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .and(header("x-title", "gw"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body("jev-latest")))
        .expect(1)
        .mount(&server)
        .await;
    let mut cfg = keyless(&server.uri());
    cfg.headers.insert("x-title".into(), "gw".into());
    SystemOneAdapter::with_id("typesafe")
        .unwrap()
        .decide(&cfg, &request(Some("jev-latest"), vec![], None))
        .await
        .expect("the header-matched mock answers");
}

// ---------------------------------------------------------------------------
// SP-DEC-2 T2 — images are encoded per host
// ---------------------------------------------------------------------------

/// Real magic-byte prefixes, base64'd: a PNG and a JPEG header.
const PNG_B64: &str = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJ";
const JPEG_B64: &str = "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAgGBgcGBQgHBwcJ";
const WEBP_B64: &str = "UklGRiQAAABXRUJQVlA4IBgAAAAwAQCdASoBAAEAAwA0JaQAA3AA";

/// llama.cpp rejects bare base64 (400 "images must be data URLs"), Cloudflare
/// and vLLM's example accept only data URLs, SGLang takes either — so the
/// generic adapter sends data URLs with the sniffed mime type.
#[tokio::test]
async fn the_generic_adapter_sends_images_as_data_urls_with_the_sniffed_mime() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .and(body_partial_json(json!({"images": [
            format!("data:image/png;base64,{PNG_B64}"),
            format!("data:image/jpeg;base64,{JPEG_B64}"),
            format!("data:image/webp;base64,{WEBP_B64}"),
            "data:image/gif;base64,R0lGODlhAQABAAAAACw="
        ]})))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body("nimble-v3")))
        .expect(1)
        .mount(&server)
        .await;

    let images = vec![
        PNG_B64.to_string(),
        JPEG_B64.to_string(),
        WEBP_B64.to_string(),
        // Already a data URL: passed through untouched.
        "data:image/gif;base64,R0lGODlhAQABAAAAACw=".to_string(),
    ];
    SystemOneAdapter::with_id("llamacpp")
        .unwrap()
        .decide(
            &keyless(&server.uri()),
            &request(Some("nimble-v3"), images, None),
        )
        .await
        .expect("data-URL images accepted");
}

/// Ollama is the one host that REQUIRES bare base64 ("URLs and data URLs are
/// not supported") — it must keep receiving exactly what the caller gave.
#[tokio::test]
async fn ollama_keeps_receiving_bare_base64_images() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .and(body_partial_json(json!({"images": [PNG_B64]})))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body("clef")))
        .expect(1)
        .mount(&server)
        .await;
    OllamaAdapter::new()
        .unwrap()
        .decide(
            &keyless(&server.uri()),
            &request(Some("clef"), vec![PNG_B64.to_string()], None),
        )
        .await
        .expect("bare base64 accepted by ollama");
}

/// An image whose bytes are no known format cannot be given a truthful mime
/// type — fail before any request (a ProviderError, so the chain can fall
/// back) rather than send a guessed one.
#[tokio::test]
async fn an_unrecognisable_image_fails_before_any_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body("x")))
        .expect(0)
        .mount(&server)
        .await;
    let err = SystemOneAdapter::with_id("llamacpp")
        .unwrap()
        .decide(
            &keyless(&server.uri()),
            &request(
                Some("x"),
                vec![PNG_B64.to_string(), "aGVsbG8gd29ybGQ=".to_string()],
                None,
            ),
        )
        .await
        .unwrap_err();
    match err {
        GatewayError::ProviderError {
            message,
            status: None,
            ..
        } => {
            assert!(
                message.contains("image 2"),
                "names the offending image: {message}"
            )
        }
        other => panic!("expected ProviderError before sending, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// SP-DEC-2 T4 — self-hosted servers through the generic adapter
// ---------------------------------------------------------------------------

/// Error bodies verbatim from llama.cpp b11381 and SGLang's source: each
/// surfaces as the server's own message, with its status, as a ProviderError
/// (so the chain falls back), never as raw JSON.
#[tokio::test]
async fn self_hosted_error_bodies_surface_the_servers_message() {
    let cases = [
        // llama.cpp: a model without decision metadata.
        (
            501,
            json!({"error":{"code":501,"message":"This model is not a decision model","type":"not_supported_error"}}),
            "This model is not a decision model",
        ),
        // llama.cpp: validation.
        (
            400,
            json!({"error":{"code":400,"message":"questions.severity: \"criteria\" must be an array of 2 to 10 levels","type":"invalid_request_error"}}),
            "questions.severity: \"criteria\" must be an array of 2 to 10 levels",
        ),
        // SGLang 400: flat OpenAI-legacy body (a ':' model name reads as LoRA).
        (
            400,
            json!({"object":"error","message":"model names the LoRA adapter 'qwen3:27b'","type":"BadRequestError","param":null,"code":400}),
            "model names the LoRA adapter 'qwen3:27b'",
        ),
        // SGLang 422: FastAPI validation list.
        (
            422,
            json!({"detail":[{"type":"missing","loc":["body","questions"],"msg":"Field required"}]}),
            "body.questions: Field required",
        ),
    ];
    for (status, body, want) in cases {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .respond_with(ResponseTemplate::new(status).set_body_json(body))
            .mount(&server)
            .await;
        match SystemOneAdapter::with_id("sglang")
            .unwrap()
            .decide(
                &keyless(&server.uri()),
                &request(Some("qwen3:27b"), vec![], None),
            )
            .await
            .unwrap_err()
        {
            GatewayError::ProviderError {
                status: Some(s),
                message,
                adapter,
            } => {
                assert_eq!((s, adapter.as_str()), (status, "sglang"));
                assert_eq!(message, want);
            }
            other => panic!("{status}: expected ProviderError, got {other:?}"),
        }
    }
}

/// Live, opt-in: a real llama.cpp server >= b11364 serving a decision GGUF
/// (verified with b11381 + ggml-org/Bespoke-Nimble-9B-v3-GGUF Q4_K_M):
///   llama-server -m Bespoke-Nimble-9B-v3-Q4_K_M.gguf --alias nimble-v3 --port 8091 -c 8192
///   LLAMACPP_URL=http://localhost:8091 cargo test -p sensei-cloud-providers \
///     --test systemone_mock live_llama_cpp -- --ignored
#[tokio::test]
#[ignore = "requires LLAMACPP_URL: a llama.cpp server >= b11364 serving a decision GGUF"]
async fn live_llama_cpp_answers_and_takes_data_url_images() {
    let url = std::env::var("LLAMACPP_URL").expect("LLAMACPP_URL");
    let mut cfg = keyless(&url);
    cfg.timeout_ms = Some(300_000);
    let adapter = SystemOneAdapter::with_id("llamacpp").unwrap();

    let mut req = request(Some("nimble-v3"), vec![], None);
    req.questions.insert(
        "urgent".into(),
        DecisionQuestion::Noul {
            instructions: json!("Is this urgent?"),
            criteria: None,
        },
    );
    let resp = adapter
        .decide(&cfg, &req)
        .await
        .expect("live llama.cpp decision");
    let DecisionAnswer::Choice {
        choice,
        probabilities,
        ..
    } = &resp.answers["label"]
    else {
        panic!("choice answer expected: {:?}", resp.answers);
    };
    // Assert the contract, not the model's opinion: the choice is one of the
    // asked options and is the argmax, and the distribution sums to 1.
    assert_eq!(
        probabilities.keys().collect::<Vec<_>>(),
        ["billing", "bug"],
        "every option scored, in order"
    );
    let argmax = probabilities
        .iter()
        .max_by(|a, b| a.1.total_cmp(b.1))
        .map(|(k, _)| k.as_str());
    assert_eq!(Some(choice.as_str()), argmax, "{probabilities:?}");
    assert!((probabilities.values().sum::<f64>() - 1.0).abs() < 1e-6);
    assert!(matches!(
        resp.answers["urgent"],
        DecisionAnswer::Noul { .. }
    ));
    assert!(resp.usage.is_some_and(|u| u.input_tokens > 0));

    // The image is ENCODED correctly — llama.cpp rejects a bare base64 image
    // with 400 "images must be data URLs" — and then refused only because a
    // text-only decision model has no vision projector: 501, not 400.
    match adapter
        .decide(
            &cfg,
            &request(Some("nimble-v3"), vec![PNG_B64.to_string()], None),
        )
        .await
        .unwrap_err()
    {
        GatewayError::ProviderError {
            status: Some(501), ..
        } => {}
        other => panic!("expected the 501 that follows a valid data URL, got {other:?}"),
    }
}
