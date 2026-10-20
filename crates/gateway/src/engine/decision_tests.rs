//! SP-DEC-1 T3 — the decision capability through the real engine: selection,
//! the up-front validation hard stop, fallback, the context-window gate, the
//! boundary translation, and the streaming refusal.

use super::*;
use crate::circuit_breaker::CircuitBreakerConfig;
use crate::types::config::{
    ChainEntry, FallbackChainConfig, FallbackTrigger, ModelConfig, RouterConfig,
};
use crate::types::decision::{DecisionAnswer, DecisionQuestion, DecisionQuestions};
use crate::types::io::{DecisionRequest, DecisionResponse};
use crate::types::request::Payload;
use async_trait::async_trait;
use serde_json::json;
use std::sync::Mutex;

/// A decision adapter that records every request it receives and answers
/// with either a fixed error or a one-question noul answer.
struct Decider {
    id: String,
    fail_with_status: Option<u16>,
    /// The first `reject_first` calls return `GatewayError::InputRejected`
    /// (SP-DEC-3 D5); later calls behave as `fail_with_status` says.
    reject_first: usize,
    seen: Mutex<Vec<DecisionRequest>>,
}

impl Decider {
    fn ok(id: &str) -> Arc<Self> {
        Arc::new(Self {
            id: id.into(),
            fail_with_status: None,
            reject_first: 0,
            seen: Mutex::new(Vec::new()),
        })
    }
    fn failing(id: &str, status: u16) -> Arc<Self> {
        Arc::new(Self {
            id: id.into(),
            fail_with_status: Some(status),
            reject_first: 0,
            seen: Mutex::new(Vec::new()),
        })
    }
    /// Refuses the input of its first `n` calls (`InputRejected`), then answers.
    fn rejecting_first(id: &str, n: usize) -> Arc<Self> {
        Arc::new(Self {
            id: id.into(),
            fail_with_status: None,
            reject_first: n,
            seen: Mutex::new(Vec::new()),
        })
    }
    fn calls(&self) -> usize {
        self.seen.lock().unwrap().len()
    }
}

impl crate::adapters::Model for Decider {
    fn id(&self) -> &str {
        &self.id
    }
}

#[async_trait]
impl crate::adapters::DecisionModel for Decider {
    async fn decide(
        &self,
        _cfg: &RouterConfig,
        req: &DecisionRequest,
    ) -> Result<DecisionResponse, GatewayError> {
        let call = {
            let mut seen = self.seen.lock().unwrap();
            seen.push(req.clone());
            seen.len()
        };
        if call <= self.reject_first {
            return Err(GatewayError::InputRejected {
                adapter: self.id.clone(),
                message: "this host ignores images".into(),
            });
        }
        if let Some(status) = self.fail_with_status {
            return Err(GatewayError::ProviderError {
                adapter: self.id.clone(),
                message: "model is not supported by System One".into(),
                status: Some(status),
            });
        }
        let mut answers = crate::types::decision::DecisionAnswers::new();
        answers.insert("refund".to_string(), DecisionAnswer::Noul { noul: 0.75 });
        Ok(DecisionResponse {
            answers,
            usage: Some(TokenUsage {
                input_tokens: 174,
                output_tokens: 1,
                total_tokens: 175,
            }),
            model: req.model.clone(),
            degraded: false,
        })
    }
}

fn router() -> RouterConfig {
    RouterConfig {
        url: "http://localhost".into(),
        api_key_env: None,
        api_key: None,
        enabled: true,
        timeout_ms: None,
        headers: HashMap::new(),
    }
}

fn model(id: &str, router: &str, api_model_id: &str, window: u32) -> ModelConfig {
    ModelConfig {
        id: id.into(),
        api_model_id: Some(api_model_id.into()),
        provider: router.into(),
        family: None,
        capabilities: vec![Capability::Decision],
        context_window: window,
        max_output_tokens: 1,
        pricing: None,
        catalog: None,
    }
}

/// Two decision candidates — `primary` (priority 1) then `backup` — on
/// distinct routers, in one `Decision` chain.
fn gateway_with(primary_window: u32) -> Gateway {
    let routers = HashMap::from([
        ("ollama".to_string(), router()),
        ("openrouter".to_string(), router()),
    ]);
    let models = HashMap::from([
        (
            "nimble".to_string(),
            model("nimble", "ollama", "nimble:9b", primary_window),
        ),
        (
            "jev".to_string(),
            model("jev", "openrouter", "typesafe/jev-1.13", 65_536),
        ),
    ]);
    let chain = FallbackChainConfig {
        id: "decide".into(),
        capability: Capability::Decision,
        models: vec![
            ChainEntry {
                model: "nimble".into(),
                router: Some("ollama".into()),
                api_model_id: None,
                priority: 1,
            },
            ChainEntry {
                model: "jev".into(),
                router: Some("openrouter".into()),
                api_model_id: None,
                priority: 2,
            },
        ],
        fallback_triggers: vec![FallbackTrigger::ProviderError],
    };
    let config = GatewayConfig {
        routers,
        models,
        chains: HashMap::from([("decide".to_string(), chain)]),
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    };
    Gateway::new(
        config,
        AdapterRegistry::new(),
        CircuitBreakerManager::new(CircuitBreakerConfig::default()),
    )
}

fn questions() -> DecisionQuestions {
    DecisionQuestions::from([(
        "refund".to_string(),
        DecisionQuestion::Noul {
            instructions: json!("Is the customer requesting a refund?"),
            criteria: None,
        },
    )])
}

fn decision_request(state: serde_json::Value, questions: DecisionQuestions) -> InferenceRequest {
    InferenceRequest {
        capability: Capability::Decision,
        model: None,
        router: None,
        chain: None,
        payload: Payload::Decision {
            state,
            questions,
            images: vec!["aGVsbG8=".into()],
            keep_alive: Some(json!("5m")),
        },
        budget: None,
        auth: None,
        panel: None,
        consensus: None,
        allow_fallback: true,
        credentials: Default::default(),
        routing: None,
    }
}

#[tokio::test]
async fn a_decision_request_reaches_the_decision_adapter_and_returns_its_answers() {
    let gw = gateway_with(8192);
    let ollama = Decider::ok("ollama");
    gw.adapters.register_decision(ollama.clone()).await;

    let resp = gw
        .execute(&decision_request(
            json!({"ticket": "charged twice"}),
            questions(),
        ))
        .await
        .expect("decision call succeeds");

    assert!(resp.success);
    let answers = resp.decisions.expect("decisions populated");
    assert_eq!(answers["refund"], DecisionAnswer::Noul { noul: 0.75 });
    assert_eq!(resp.usage.map(|u| u.input_tokens), Some(174));
    assert!(resp.content.is_none() && resp.embeddings.is_none());

    // Every payload field crossed the boundary, and the candidate's
    // api_model_id — not the catalog id — is what the adapter was asked for.
    let seen = ollama.seen.lock().unwrap();
    let req = &seen[0];
    assert_eq!(req.model.as_deref(), Some("nimble:9b"));
    assert_eq!(req.state, json!({"ticket": "charged twice"}));
    assert_eq!(req.questions, questions());
    assert_eq!(req.images, vec!["aGVsbG8=".to_string()]);
    assert_eq!(req.keep_alive, Some(json!("5m")));
}

#[tokio::test]
async fn a_malformed_decision_request_is_rejected_before_any_candidate_is_tried() {
    let gw = gateway_with(8192);
    let ollama = Decider::ok("ollama");
    let openrouter = Decider::ok("openrouter");
    gw.adapters.register_decision(ollama.clone()).await;
    gw.adapters.register_decision(openrouter.clone()).await;

    let err = gw
        .execute(&decision_request(
            json!("a ticket"),
            DecisionQuestions::new(),
        ))
        .await
        .expect_err("no questions is invalid");

    assert!(
        matches!(&err, GatewayError::InvalidRequest { message } if message.contains("1–64")),
        "got {err:?}"
    );
    assert_eq!(
        ollama.calls() + openrouter.calls(),
        0,
        "no candidate dispatched"
    );
}

#[tokio::test]
async fn a_provider_rejection_falls_back_to_the_next_decision_router() {
    let gw = gateway_with(8192);
    let ollama = Decider::failing("ollama", 400);
    let openrouter = Decider::ok("openrouter");
    gw.adapters.register_decision(ollama.clone()).await;
    gw.adapters.register_decision(openrouter.clone()).await;

    let resp = gw
        .execute(&decision_request(json!("a ticket"), questions()))
        .await
        .expect("falls back to openrouter");

    assert_eq!((ollama.calls(), openrouter.calls()), (1, 1));
    assert_eq!(
        openrouter.seen.lock().unwrap()[0].model.as_deref(),
        Some("typesafe/jev-1.13")
    );
    assert!(resp.decisions.is_some());
}

#[tokio::test]
async fn a_state_too_large_for_a_short_context_model_skips_it_without_dispatch() {
    // Tev1-class models have ~2K-token windows; the gate must route around
    // them rather than let the provider fail (gh#72 "short-context constraint").
    // No images: an image alone (MAX_TOKENS_PER_ATTACHMENT) would overflow the
    // window and gate the candidate whether or not the state is counted.
    let text_only = |state: serde_json::Value| {
        let mut req = decision_request(state, questions());
        if let Payload::Decision { images, .. } = &mut req.payload {
            images.clear();
        }
        req
    };

    // Control: a small state fits the same 1024-token window and IS dispatched,
    // so the gate is judging the state, not refusing this candidate outright.
    let gw = gateway_with(1024);
    let ollama = Decider::ok("ollama");
    let openrouter = Decider::ok("openrouter");
    gw.adapters.register_decision(ollama.clone()).await;
    gw.adapters.register_decision(openrouter.clone()).await;
    gw.execute(&text_only(json!("a short ticket")))
        .await
        .expect("small state served by the primary");
    assert_eq!((ollama.calls(), openrouter.calls()), (1, 0));

    // A 4096-char state (~1366 tokens) cannot fit: skipped without dispatch.
    gw.execute(&text_only(json!("x".repeat(4096))))
        .await
        .expect("served by the large-window candidate");
    assert_eq!(ollama.calls(), 1, "the 1024-token candidate was gated out");
    assert_eq!(openrouter.calls(), 1);
}

#[tokio::test]
async fn the_noop_adapter_answers_decision_as_degraded_not_success() {
    let gw = gateway_with(8192);
    use crate::adapters::RegisterInto;
    let noop = Arc::new(crate::adapters::noop::NoopAdapter);
    // Register noop under the chain's primary router id via the decision map.
    struct Renamed(Arc<crate::adapters::noop::NoopAdapter>);
    impl crate::adapters::Model for Renamed {
        fn id(&self) -> &str {
            "ollama"
        }
    }
    #[async_trait]
    impl crate::adapters::DecisionModel for Renamed {
        async fn decide(
            &self,
            c: &RouterConfig,
            r: &DecisionRequest,
        ) -> Result<DecisionResponse, GatewayError> {
            self.0.decide(c, r).await
        }
    }
    noop.clone().register_into(&gw.adapters).await;
    gw.adapters.register_decision(Arc::new(Renamed(noop))).await;

    let resp = gw
        .execute(&decision_request(json!("a ticket"), questions()))
        .await
        .expect("noop answers");
    assert!(!resp.success, "a degraded decision reply is not a success");
}

#[tokio::test]
async fn streaming_a_decision_is_refused_up_front() {
    let gw = gateway_with(8192);
    gw.adapters.register_decision(Decider::ok("ollama")).await;
    match gw
        .execute_stream(&decision_request(json!("a ticket"), questions()))
        .await
    {
        Err(GatewayError::Unsupported { what, .. }) => assert!(what.contains("streaming")),
        Err(other) => panic!("expected Unsupported, got {other}"),
        Ok(_) => panic!("a decision call must not stream"),
    }
}

#[test]
fn decision_estimates_count_state_every_question_and_each_image() {
    use super::util::{MAX_TOKENS_PER_ATTACHMENT, estimate_input_tokens};
    let payload = |state: &str, qs: DecisionQuestions, images: usize| Payload::Decision {
        state: json!(state),
        questions: qs,
        images: vec!["aGVsbG8=".to_string(); images],
        keep_alive: None,
    };
    let one = questions();
    let mut two = questions();
    two.insert(
        "urgency".into(),
        DecisionQuestion::Score {
            instructions: json!("How urgently does this need a response?"),
            criteria: vec!["Routine".into(), "Soon".into(), "Immediate".into()],
        },
    );
    let state = "y".repeat(300);

    let base = estimate_input_tokens_pessimistic(&payload(&state, one.clone(), 0));
    assert!(
        base >= 100,
        "a 300-char state is at least 100 tokens: {base}"
    );
    // Every question is in the prompt (Tev1 scores each with the whole set).
    let with_two = estimate_input_tokens_pessimistic(&payload(&state, two, 0));
    assert!(with_two > base, "{with_two} should exceed {base}");
    // Images are priced like chat attachments, after the divide.
    assert_eq!(
        estimate_input_tokens_pessimistic(&payload(&state, one.clone(), 2)),
        base + 2 * MAX_TOKENS_PER_ATTACHMENT
    );
    // Criteria text is in the prompt too, for every question type: the same
    // question with 900 more characters of criteria must estimate higher.
    let long = "z".repeat(900);
    let variants = |crit: &str| {
        [
            DecisionQuestion::Choice {
                instructions: json!("which?"),
                criteria: [
                    ("a".to_string(), Some(crit.to_string())),
                    ("b".into(), None),
                ]
                .into_iter()
                .collect(),
            },
            DecisionQuestion::Noul {
                instructions: json!("y?"),
                criteria: Some(crate::types::decision::NoulCriteria {
                    r#false: Some(crit.to_string()),
                    r#true: None,
                }),
            },
            DecisionQuestion::Score {
                instructions: json!("how?"),
                criteria: vec![crit.to_string(), "high".into()],
            },
        ]
    };
    for (short_q, long_q) in variants("s").into_iter().zip(variants(&long)) {
        let est = |q: DecisionQuestion| {
            estimate_input_tokens_pessimistic(&payload(
                "state",
                DecisionQuestions::from([("q".to_string(), q)]),
                0,
            ))
        };
        let (s, l) = (est(short_q.clone()), est(long_q));
        assert!(
            l >= s + 299,
            "criteria must count for {short_q:?}: {s} vs {l}"
        );
    }
    // The cost estimate is non-zero too (it prices input tokens).
    assert!(estimate_input_tokens(&payload(&state, one, 0)) >= 75);
}

/// Live oracle (Ollama 0.35.0 + nimble, 2026-10-03): state `"x"`, N noul
/// questions `"q1".."qN"` each `"ok?"`. Ollama wraps every question in a
/// prompt holding the WHOLE set — 104 + 38·N tokens per prompt — and bills one
/// prompt per question. Measured `input_tokens`: N=1 → 142, N=8 → 3264,
/// N=64 → 165879.
fn tiny_questions(n: usize) -> Payload {
    Payload::Decision {
        state: json!("x"),
        questions: (1..=n)
            .map(|i| {
                (
                    format!("q{i}"),
                    DecisionQuestion::Noul {
                        instructions: json!("ok?"),
                        criteria: None,
                    },
                )
            })
            .collect(),
        images: vec![],
        keep_alive: None,
    }
}

#[test]
fn the_window_estimate_covers_the_prompt_ollama_actually_builds() {
    // Per-prompt size = billed / N: the window must hold one whole prompt.
    for (n, per_prompt) in [(1usize, 142u32), (8, 3264 / 8), (64, 165_879 / 64)] {
        let est = estimate_input_tokens_pessimistic(&tiny_questions(n));
        assert!(
            est >= per_prompt,
            "N={n}: window estimate {est} < measured per-prompt {per_prompt}"
        );
    }
}

#[test]
fn the_cost_estimate_covers_what_a_decision_call_bills() {
    use super::util::estimate_input_tokens;
    for (n, billed) in [(1usize, 142u32), (8, 3264), (64, 165_879)] {
        let est = estimate_input_tokens(&tiny_questions(n));
        assert!(
            est >= billed,
            "N={n}: cost estimate {est} < billed {billed}"
        );
    }
}

// ── SP-DEC-3 D5 / AC8: `InputRejected` is a skip, not a fault ──────────────

/// A `Decision` chain over `entries` (`(model, router)`, in priority order),
/// each model on its own router, with the given triggers and breaker.
fn gateway_chain(
    entries: &[(&str, &str)],
    fallback_triggers: Vec<FallbackTrigger>,
    breaker: CircuitBreakerManager,
) -> Gateway {
    let routers = entries
        .iter()
        .map(|(_, r)| (r.to_string(), router()))
        .collect();
    let models = entries
        .iter()
        .map(|(m, r)| (m.to_string(), model(m, r, &format!("{m}-api"), 65_536)))
        .collect();
    let chain = FallbackChainConfig {
        id: "decide".into(),
        capability: Capability::Decision,
        models: entries
            .iter()
            .enumerate()
            .map(|(i, (m, r))| ChainEntry {
                model: m.to_string(),
                router: Some(r.to_string()),
                api_model_id: None,
                priority: i as u8 + 1,
            })
            .collect(),
        fallback_triggers,
    };
    let config = GatewayConfig {
        routers,
        models,
        chains: HashMap::from([("decide".to_string(), chain)]),
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    };
    Gateway::new(config, AdapterRegistry::new(), breaker)
}

/// AC8 breaker neutrality: with a breaker that opens on ONE failure, an input
/// rejection must not count as that failure — the next request to the same
/// single-candidate chain is still dispatched, and served.
#[tokio::test]
async fn an_input_rejection_does_not_open_the_breaker() {
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig {
        threshold: 1,
        ..Default::default()
    });
    let gw = gateway_chain(&[("rejecting", "rejecting-router")], vec![], cb.clone());
    let rejecting = Decider::rejecting_first("rejecting-router", 1);
    gw.adapters.register_decision(rejecting.clone()).await;
    let req = decision_request(json!("a ticket"), questions());

    let first = gw.execute(&req).await;
    assert!(
        first.is_err(),
        "the first call is input-rejected: {first:?}"
    );
    // Not the breaker alone: NO recorder may hear it. The performance recorder
    // would otherwise log a failed verdict and drag the endpoint's
    // `success_rate` — and the default strategy's weighting — down.
    assert!(
        gw.performance_stats("rejecting-router:rejecting").is_none(),
        "an input rejection must record no performance sample: {:?}",
        gw.performance_stats("rejecting-router:rejecting")
    );

    let second = gw.execute(&req).await;
    assert_eq!(
        rejecting.calls(),
        2,
        "the second call must be DISPATCHED, not skipped as breaker-open \
         (breaker state: {}, second: {second:?})",
        cb.get_state("rejecting-router:rejecting").name()
    );
    let resp = second.expect("the second call is served");
    assert!(resp.decisions.is_some());
}

/// AC8 fallback: an input rejection falls back even when the chain names no
/// fallback trigger at all — an ineligible candidate is a skip.
#[tokio::test]
async fn an_input_rejection_falls_back_with_an_empty_trigger_set() {
    let gw = gateway_chain(
        &[("rejecting", "rejecting-router"), ("ok", "ok-router")],
        vec![],
        CircuitBreakerManager::new(CircuitBreakerConfig::default()),
    );
    let rejecting = Decider::rejecting_first("rejecting-router", usize::MAX);
    let ok = Decider::ok("ok-router");
    gw.adapters.register_decision(rejecting.clone()).await;
    gw.adapters.register_decision(ok.clone()).await;

    let resp = gw
        .execute(&decision_request(json!("a ticket"), questions()))
        .await
        .expect("served by ok-router");

    assert_eq!((rejecting.calls(), ok.calls()), (1, 1));
    let adapters: Vec<&str> = resp.attempts.iter().map(|a| a.adapter.as_str()).collect();
    assert_eq!(adapters, ["rejecting-router", "ok-router"]);
    assert!(resp.attempts[0].fallback_triggered);
}

/// AC8 exhaustion: a chain exhausted only by input rejections is a terminal,
/// NON-retryable `AllAttemptsFailed` — waiting will not change the input, so it
/// is neither retryable nor `AllGated`.
#[tokio::test]
async fn an_exhaustion_of_input_rejections_is_not_retryable_and_not_all_gated() {
    let gw = gateway_chain(
        &[("rejecting", "rejecting-router")],
        vec![FallbackTrigger::ProviderError],
        CircuitBreakerManager::new(CircuitBreakerConfig::default()),
    );
    gw.adapters
        .register_decision(Decider::rejecting_first("rejecting-router", usize::MAX))
        .await;

    let err = gw
        .execute(&decision_request(json!("a ticket"), questions()))
        .await
        .expect_err("the only candidate rejects the input");

    match err {
        GatewayError::AllAttemptsFailed {
            retryable,
            attempts,
            ..
        } => {
            assert_eq!(attempts, 1);
            assert!(!retryable, "an input rejection must not be retryable");
        }
        other => panic!("expected AllAttemptsFailed {{ retryable: false }}, got {other:?}"),
    }
}

/// AC8, neutral to all-gated: beside a GENUINE gate (a breaker-open candidate),
/// an input rejection does not veto `AllGated` — the exhaustion pauses on that
/// gate. Waiting does fix this chain: after `resume_after` the walk passes the
/// cheap, health-neutral rejection and the recovered candidate serves the
/// request. The same neutrality an over-window skip has (`OverContextWindow`).
#[tokio::test]
async fn an_input_rejection_beside_a_breaker_open_candidate_pauses_on_that_gate() {
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig {
        threshold: 1,
        ..Default::default()
    });
    // Open `gated-router:gated` before the request: selection skips it as
    // breaker-open — a timed gate in `skipped`.
    assert!(cb.can_execute("gated-router:gated"));
    cb.record_failure("gated-router:gated");
    let gw = gateway_chain(
        &[("rejecting", "rejecting-router"), ("gated", "gated-router")],
        vec![],
        cb,
    );
    let rejecting = Decider::rejecting_first("rejecting-router", usize::MAX);
    let gated = Decider::ok("gated-router");
    gw.adapters.register_decision(rejecting.clone()).await;
    gw.adapters.register_decision(gated.clone()).await;

    let err = gw
        .execute(&decision_request(json!("a ticket"), questions()))
        .await
        .expect_err("one candidate rejects the input, the other is breaker-open");

    assert_eq!((rejecting.calls(), gated.calls()), (1, 0));
    match err {
        GatewayError::AllGated {
            resume_after: Some(_),
            ..
        } => {}
        other => panic!("expected AllGated {{ resume_after: Some(_) }}, got {other:?}"),
    }
}

/// AC8, neutral to all-gated, the HOTL class: beside an auth-locked candidate
/// the exhaustion is a no-deadline `AllGated` naming the remedy — rotating the
/// credential fixes the chain, so it must pause for a human, not fail.
#[tokio::test]
async fn an_input_rejection_beside_an_auth_locked_candidate_pauses_for_a_human() {
    use crate::gates::lockout::LockReason;
    use crate::types::error::HumanAction;

    let gw = gateway_chain(
        &[
            ("rejecting", "rejecting-router"),
            ("locked", "locked-router"),
        ],
        vec![],
        CircuitBreakerManager::new(CircuitBreakerConfig::default()),
    );
    gw.apply_lockout("locked-router:locked", LockReason::Auth, None);
    let rejecting = Decider::rejecting_first("rejecting-router", usize::MAX);
    let locked = Decider::ok("locked-router");
    gw.adapters.register_decision(rejecting.clone()).await;
    gw.adapters.register_decision(locked.clone()).await;

    let err = gw
        .execute(&decision_request(json!("a ticket"), questions()))
        .await
        .expect_err("one candidate rejects the input, the other is auth-locked");

    assert_eq!((rejecting.calls(), locked.calls()), (1, 0));
    match err {
        GatewayError::AllGated {
            resume_after: None,
            human_action: Some(HumanAction::RotateCredential),
            ..
        } => {}
        other => panic!(
            "expected AllGated {{ resume_after: None, human_action: RotateCredential }}, \
             got {other:?}"
        ),
    }
}

/// AC8, beside a hard failure: a 500 may clear on its own, so an exhaustion of
/// `[input rejection, 500]` stays retryable — the rejection must not mask it.
#[tokio::test]
async fn an_input_rejection_beside_a_hard_failure_stays_retryable() {
    let gw = gateway_chain(
        &[
            ("rejecting", "rejecting-router"),
            ("failing", "failing-router"),
        ],
        vec![],
        CircuitBreakerManager::new(CircuitBreakerConfig::default()),
    );
    let rejecting = Decider::rejecting_first("rejecting-router", usize::MAX);
    let failing = Decider::failing("failing-router", 500);
    gw.adapters.register_decision(rejecting.clone()).await;
    gw.adapters.register_decision(failing.clone()).await;

    let err = gw
        .execute(&decision_request(json!("a ticket"), questions()))
        .await
        .expect_err("one candidate rejects the input, the other 500s");

    assert_eq!((rejecting.calls(), failing.calls()), (1, 1));
    match err {
        GatewayError::AllAttemptsFailed { retryable, .. } => {
            assert!(retryable, "the 500 may clear on its own")
        }
        other => panic!("expected AllAttemptsFailed {{ retryable: true }}, got {other:?}"),
    }
}
