//! SP-DEC-2 — a System One decision through the real engine to a real
//! self-hosted server: `FacadeBuilder` auto-registers the `llamacpp` router
//! (decision-only `SystemOneAdapter`), the `decide` chain selects it, and a
//! live llama.cpp server answers.
//!
//! Opt-in. Verified with llama.cpp release b11381 serving
//! ggml-org/Bespoke-Nimble-9B-v3-GGUF (Q4_K_M):
//!   llama-server -m Bespoke-Nimble-9B-v3-Q4_K_M.gguf --alias nimble-v3 --port 8091 -c 8192
//!   LLAMACPP_URL=http://localhost:8091 cargo test -p sensei-gateway \
//!     --test live_decision_self_hosted -- --ignored

#![cfg(feature = "cloud")]

use std::collections::HashMap;

use gateway::types::config::{
    ChainEntry, FallbackChainConfig, FallbackTrigger, GatewayConfig, ModelConfig, RouterConfig,
};
use gateway::types::decision::{DecisionAnswer, DecisionQuestion, DecisionQuestions};
use gateway::types::request::Payload;
use gateway::{Capability, FacadeBuilder, InferenceRequest};
use serde_json::json;

fn router(url: &str) -> RouterConfig {
    RouterConfig {
        url: url.into(),
        api_key_env: None,
        api_key: None,
        enabled: true,
        timeout_ms: Some(300_000),
        headers: HashMap::new(),
    }
}

fn model(id: &str, provider: &str, api_model_id: &str, window: u32) -> ModelConfig {
    ModelConfig {
        id: id.into(),
        api_model_id: Some(api_model_id.into()),
        provider: provider.into(),
        family: None,
        capabilities: vec![Capability::Decision],
        context_window: window,
        max_output_tokens: 1,
        pricing: None,
        catalog: None,
    }
}

#[tokio::test]
#[ignore = "requires LLAMACPP_URL: a llama.cpp server >= b11364 serving a decision GGUF"]
async fn a_decide_chain_routes_to_a_live_llama_cpp_server() {
    let url = std::env::var("LLAMACPP_URL").expect("LLAMACPP_URL");
    // llama.cpp first, then a hosted leg that must NOT be needed. OpenRouter's
    // url points nowhere routable, so reaching it would fail loudly.
    let config = GatewayConfig {
        routers: HashMap::from([
            ("llamacpp".into(), router(&url)),
            ("openrouter".into(), router("http://127.0.0.1:9")),
        ]),
        models: HashMap::from([
            (
                "nimble-v3".into(),
                model("nimble-v3", "llamacpp", "nimble-v3", 8192),
            ),
            (
                "jev".into(),
                model("jev", "openrouter", "typesafe/jev-1.13", 32_000),
            ),
        ]),
        chains: HashMap::from([(
            "decide".into(),
            FallbackChainConfig {
                id: "decide".into(),
                capability: Capability::Decision,
                models: vec![
                    ChainEntry {
                        model: "nimble-v3".into(),
                        router: Some("llamacpp".into()),
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
            state: json!({"ticket": "I was charged twice. Please refund the extra payment."}),
            questions: DecisionQuestions::from([
                (
                    "refund".to_string(),
                    DecisionQuestion::Noul {
                        instructions: json!("Is the customer requesting a refund?"),
                        criteria: None,
                    },
                ),
                (
                    "urgency".to_string(),
                    DecisionQuestion::Score {
                        instructions: json!("How urgently does this ticket need a response?"),
                        criteria: vec!["Routine".into(), "Soon".into(), "Immediate".into()],
                    },
                ),
            ]),
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

    let resp = gw.execute(&req).await.expect("the decide chain answers");
    assert!(resp.success, "{resp:?}");
    assert_eq!(
        resp.attempts.len(),
        1,
        "served by llama.cpp, no fallback: {:?}",
        resp.attempts
    );
    assert_eq!(resp.attempts[0].adapter, "llamacpp");
    let answers = resp.decisions.expect("decisions");
    assert_eq!(answers.keys().collect::<Vec<_>>(), ["refund", "urgency"]);
    let Some(DecisionAnswer::Noul { noul }) = answers.get("refund") else {
        panic!("noul answer: {answers:?}");
    };
    assert!(*noul > 0.5, "a refund request reads as a refund: p={noul}");
    let Some(DecisionAnswer::Score { score, legend, .. }) = answers.get("urgency") else {
        panic!("score answer: {answers:?}");
    };
    assert!(
        (0.0..=2.0).contains(score),
        "score is on the 0..levels-1 scale: {score}"
    );
    assert_eq!(legend["2"], "Immediate");
}
