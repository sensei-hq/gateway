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
    seen: Mutex<Vec<DecisionRequest>>,
}

impl Decider {
    fn ok(id: &str) -> Arc<Self> {
        Arc::new(Self {
            id: id.into(),
            fail_with_status: None,
            seen: Mutex::new(Vec::new()),
        })
    }
    fn failing(id: &str, status: u16) -> Arc<Self> {
        Arc::new(Self {
            id: id.into(),
            fail_with_status: Some(status),
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
        self.seen.lock().unwrap().push(req.clone());
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
    let gw = gateway_with(64);
    let ollama = Decider::ok("ollama");
    let openrouter = Decider::ok("openrouter");
    gw.adapters.register_decision(ollama.clone()).await;
    gw.adapters.register_decision(openrouter.clone()).await;

    let big_state = json!("x".repeat(4096));
    gw.execute(&decision_request(big_state, questions()))
        .await
        .expect("served by the large-window candidate");

    assert_eq!(ollama.calls(), 0, "the 64-token candidate was gated out");
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
    // The cost estimate is non-zero too (it prices input tokens).
    assert!(estimate_input_tokens(&payload(&state, one, 0)) >= 75);
}
