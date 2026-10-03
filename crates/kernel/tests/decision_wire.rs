//! SP-DEC-1 T1 — the decision (System One) wire format, pinned against the
//! published examples (docs.ollama.com/api/systemone) and a response captured
//! live from Ollama 0.35.0 + `nimble`.

use kernel::types::decision::{
    DecisionAnswer, DecisionQuestion, DecisionQuestions, NoulCriteria, validate_decision,
};
use kernel::types::request::{InferenceResponse, Payload};
use serde_json::json;

fn choice(instructions: &str, criteria: &[(&str, Option<&str>)]) -> DecisionQuestion {
    DecisionQuestion::Choice {
        instructions: json!(instructions),
        criteria: criteria
            .iter()
            .map(|(k, v)| (k.to_string(), v.map(str::to_string)))
            .collect(),
    }
}

fn noul(instructions: &str) -> DecisionQuestion {
    DecisionQuestion::Noul {
        instructions: json!(instructions),
        criteria: None,
    }
}

fn score(instructions: &str, levels: &[&str]) -> DecisionQuestion {
    DecisionQuestion::Score {
        instructions: json!(instructions),
        criteria: levels.iter().map(|s| s.to_string()).collect(),
    }
}

#[test]
fn the_documented_multi_question_request_round_trips_byte_for_byte() {
    // docs.ollama.com/capabilities/decision — "Ask multiple questions".
    let wire = json!({
        "type": "decision",
        "state": {"ticket": "I was charged twice. Please refund the extra payment."},
        "questions": {
            "refund": {
                "type": "noul",
                "instructions": "Is the customer requesting a refund?",
                "criteria": {"false": "No refund is requested", "true": "The customer requests a refund"}
            },
            "urgency": {
                "type": "score",
                "instructions": "How urgently does this ticket need a response?",
                "criteria": [
                    "Routine: no time pressure",
                    "Soon: a customer is inconvenienced",
                    "Immediate: a critical service is unavailable"
                ]
            }
        }
    });
    let payload: Payload = serde_json::from_value(wire.clone()).expect("deserialize");
    let Payload::Decision { questions, .. } = &payload else {
        panic!("expected Payload::Decision, got {payload:?}");
    };
    assert_eq!(
        questions.get("refund"),
        Some(&DecisionQuestion::Noul {
            instructions: json!("Is the customer requesting a refund?"),
            criteria: Some(NoulCriteria {
                r#false: Some("No refund is requested".into()),
                r#true: Some("The customer requests a refund".into()),
            }),
        })
    );
    assert_eq!(serde_json::to_value(&payload).unwrap(), wire);
}

#[test]
fn question_and_criteria_order_survive_a_round_trip() {
    // Choice ties follow option order and score levels are ordered, so a
    // HashMap would silently change answers. Keys chosen to be anti-sorted.
    let wire = r#"{"type":"decision","state":"x","questions":{"zeta":{"type":"choice","instructions":"pick","criteria":{"zz":"last alphabetically","aa":null,"mm":"middle"}},"alpha":{"type":"noul","instructions":"y?"}}}"#;
    let payload: Payload = serde_json::from_str(wire).unwrap();
    assert_eq!(serde_json::to_string(&payload).unwrap(), wire);
}

#[test]
fn a_live_ollama_response_deserializes_into_typed_answers_in_order() {
    let raw = include_str!("fixtures/decision/ollama_mixed_response.json");
    let v: serde_json::Value = serde_json::from_str(raw).unwrap();
    let answers: indexmap::IndexMap<String, DecisionAnswer> =
        serde_json::from_value(v["answers"].clone()).unwrap();

    assert_eq!(
        answers.keys().collect::<Vec<_>>(),
        ["label", "refund", "urgency"]
    );
    let DecisionAnswer::Choice {
        choice,
        probabilities,
        confidence,
    } = &answers["label"]
    else {
        panic!("label is a choice answer");
    };
    assert_eq!(choice, "billing");
    assert_eq!(probabilities.keys().collect::<Vec<_>>(), ["billing", "bug"]);
    assert!((probabilities["billing"] - 0.9807893279342103).abs() < 1e-12);
    assert!((*confidence - 0.8630145362206234).abs() < 1e-12);

    assert!(
        matches!(answers["refund"], DecisionAnswer::Noul { noul } if (noul - 0.9931194019118367).abs() < 1e-12)
    );

    let DecisionAnswer::Score {
        score,
        legend,
        probabilities,
        ..
    } = &answers["urgency"]
    else {
        panic!("urgency is a score answer");
    };
    assert!((*score - 0.8286973610466735).abs() < 1e-12);
    assert_eq!(legend["2"], "Immediate");
    assert_eq!(probabilities.len(), 3);
}

#[test]
fn inference_response_carries_decisions_and_omits_them_when_absent() {
    let mut answers = indexmap::IndexMap::new();
    answers.insert("refund".to_string(), DecisionAnswer::Noul { noul: 0.25 });
    let resp: InferenceResponse = serde_json::from_value(json!({
        "success": true,
        "attempts": [],
        "decisions": {"refund": {"type": "noul", "noul": 0.25}}
    }))
    .unwrap();
    assert_eq!(resp.decisions, Some(answers));

    let bare: InferenceResponse =
        serde_json::from_value(json!({"success": true, "attempts": []})).unwrap();
    assert_eq!(bare.decisions, None);
    assert!(
        serde_json::to_value(&bare)
            .unwrap()
            .get("decisions")
            .is_none()
    );
}

fn questions(qs: Vec<(&str, DecisionQuestion)>) -> DecisionQuestions {
    qs.into_iter().map(|(k, q)| (k.to_string(), q)).collect()
}

#[test]
fn validate_accepts_the_documented_shapes() {
    let qs = questions(vec![
        ("label", choice("Which?", &[("a", Some("A")), ("b", None)])),
        ("refund", noul("Refund?")),
        ("urgency", score("How urgent?", &["low", "high"])),
    ]);
    assert_eq!(validate_decision(&json!("a ticket"), &qs), Ok(()));

    // Exactly the documented maximum is legal; 65 is pinned as illegal below.
    let sixty_four: DecisionQuestions = (0..64).map(|i| (format!("q{i}"), noul("y?"))).collect();
    assert_eq!(validate_decision(&json!("x"), &sixty_four), Ok(()));
}

/// Upstream `SystemOneContent` is `string (pattern \S) | object | array` with no
/// `minProperties`/`minItems`, and a live Ollama 0.35 answers `state: {}` /
/// `instructions: []` with a 200 — only a blank STRING is invalid.
#[test]
fn validate_accepts_empty_object_and_array_content() {
    let with_instr = |i: serde_json::Value| {
        questions(vec![(
            "q",
            DecisionQuestion::Noul {
                instructions: i,
                criteria: None,
            },
        )])
    };
    for empty in [json!({}), json!([])] {
        assert_eq!(validate_decision(&empty, &with_instr(json!("y?"))), Ok(()));
        assert_eq!(
            validate_decision(&json!("x"), &with_instr(empty.clone())),
            Ok(())
        );
    }
}

#[test]
fn validate_rejects_each_structural_violation_with_the_offending_name() {
    let ok = || questions(vec![("q", noul("y?"))]);
    let cases: Vec<(serde_json::Value, DecisionQuestions, &str)> = vec![
        (json!("   "), ok(), "state"),
        (json!(null), ok(), "state"),
        (json!("x"), questions(vec![]), "1–64"),
        (
            json!("x"),
            (0..65)
                .map(|i| (format!("q{i}"), noul("y?")))
                .collect::<DecisionQuestions>(),
            "1–64",
        ),
        (json!("x"), questions(vec![(" ", noul("y?"))]), "blank"),
        (json!("x"), questions(vec![("q", noul(" "))]), "\"q\""),
        (
            json!("x"),
            questions(vec![("pick", choice("which?", &[("only", None)]))]),
            "\"pick\"",
        ),
        (
            json!("x"),
            questions(vec![(
                "pick",
                choice("which?", &[("a", None), (" ", None)]),
            )]),
            "\"pick\"",
        ),
        (
            json!("x"),
            questions(vec![("rank", score("how?", &["one"]))]),
            "\"rank\"",
        ),
    ];
    for (state, qs, needle) in cases {
        let err = validate_decision(&state, &qs).expect_err(&format!("{state} / {qs:?}"));
        assert!(err.contains(needle), "{err:?} should mention {needle:?}");
    }
}
