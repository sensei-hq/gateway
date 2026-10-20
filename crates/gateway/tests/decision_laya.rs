//! SP-DEC-3 AC4 — a `laya` answer the server truncated is a failed attempt the
//! engine falls back on: the facade's `laya` router (mocked with Laya's real
//! truncated payload) fails, the next router in the `decide` chain answers.

#![cfg(feature = "cloud")]

use std::collections::HashMap;

use gateway::types::config::{
    ChainEntry, FallbackChainConfig, FallbackTrigger, GatewayConfig, ModelConfig, RouterConfig,
};
use gateway::types::decision::{DecisionAnswer, DecisionQuestion, DecisionQuestions};
use gateway::types::request::Payload;
use gateway::{Capability, FacadeBuilder, InferenceRequest};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn router(url: String) -> RouterConfig {
    RouterConfig {
        url,
        api_key_env: None,
        api_key: None,
        enabled: true,
        timeout_ms: Some(5_000),
        headers: HashMap::new(),
    }
}

fn model(id: &str, provider: &str, window: u32) -> ModelConfig {
    ModelConfig {
        id: id.into(),
        api_model_id: Some(id.into()),
        provider: provider.into(),
        family: None,
        capabilities: vec![Capability::Decision],
        context_window: window,
        max_output_tokens: 1,
        pricing: None,
        catalog: None,
    }
}

fn entry(model: &str, router: &str, priority: u8) -> ChainEntry {
    ChainEntry {
        model: model.into(),
        router: Some(router.into()),
        api_model_id: None,
        priority,
    }
}

#[tokio::test]
async fn a_truncated_laya_answer_falls_back_to_the_next_router() {
    let laya = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"model":"laya-rl-agent","answers":{"q":{"type":"noul","noul":0.6873,"confidence":0.6873,"answer_confidence":0.6873,"action":{"act_probability":1.0}}},"usage":{"input_tokens":512,"output_tokens":0,"state_tokens":1200,"state_tokens_dropped":718,"truncated":true,"truncated_questions":["q"]},"routing":{"model":"english"}})))
        .expect(1)
        .mount(&laya)
        .await;
    let next = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "model": "nimble-v3",
            "answers": {"q": {"type": "noul", "noul": 0.91}},
            "usage": {"input_tokens": 300, "output_tokens": 0}
        })))
        .expect(1)
        .mount(&next)
        .await;

    let config = GatewayConfig {
        routers: HashMap::from([
            ("laya".into(), router(laya.uri())),
            ("llamacpp".into(), router(next.uri())),
        ]),
        models: HashMap::from([
            // Window deliberately overstated so the ContextWindowGate lets the
            // request through and D3 is what catches the truncation.
            ("english".into(), model("english", "laya", 8_192)),
            ("nimble-v3".into(), model("nimble-v3", "llamacpp", 8_192)),
        ]),
        chains: HashMap::from([(
            "decide".into(),
            FallbackChainConfig {
                id: "decide".into(),
                capability: Capability::Decision,
                models: vec![
                    entry("english", "laya", 1),
                    entry("nimble-v3", "llamacpp", 2),
                ],
                fallback_triggers: vec![FallbackTrigger::ProviderError],
            },
        )]),
        ..Default::default()
    };
    let gw = FacadeBuilder::new(config).build().await.gateway;

    let req = InferenceRequest {
        capability: Capability::Decision,
        model: None,
        router: None,
        chain: Some("decide".into()),
        payload: Payload::Decision {
            state: json!("The customer wrote again about the duplicate invoice charge."),
            questions: DecisionQuestions::from([(
                "q".to_string(),
                DecisionQuestion::Noul {
                    instructions: json!("Refund requested?"),
                    criteria: None,
                },
            )]),
            images: vec![],
            keep_alive: None,
        },
        budget: None,
        auth: None,
        panel: None,
        consensus: None,
        allow_fallback: true,
        credentials: Default::default(),
        routing: None,
    };

    let resp = gw.execute(&req).await.expect("falls back to llamacpp");
    assert!(resp.success, "{resp:?}");
    let adapters: Vec<&str> = resp.attempts.iter().map(|a| a.adapter.as_str()).collect();
    assert_eq!(adapters, ["laya", "llamacpp"], "{:?}", resp.attempts);
    // The laya attempt failed for the truncation reason, not some other gate.
    let laya_error = resp.attempts[0]
        .error
        .as_deref()
        .expect("laya attempt error");
    assert!(laya_error.contains("truncated"), "{laya_error}");
    let answers = resp.decisions.expect("decisions");
    assert!(matches!(
        answers["q"],
        DecisionAnswer::Noul { noul } if noul == 0.91
    ));
}
