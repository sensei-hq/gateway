use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::engine::Gateway;
use crate::types::capability::Capability;
use crate::types::cost::Cost;
use crate::types::error::GatewayError;
use crate::types::request::{InferenceRequest, Message, MessageRole, Payload};

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// Hint for which model tier to prefer during selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelHint {
    /// Cheapest, fastest — classification, simple extraction
    Fastest,
    /// Mid-tier — summarization, moderate reasoning
    Balanced,
    /// Highest quality — code review, complex analysis
    Best,
    /// Use a specific model by name
    Specific(String),
}

/// Where a step gets its input from.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepInput {
    /// Use the original user input (default for first step)
    #[default]
    FromRequest,
    /// Use the text output of a previous step (0-indexed)
    FromStep(usize),
    /// Format template with step outputs: "{0}" is step 0's output, "{1}" is step 1's, etc.
    Template(String),
}

/// A single step in a purpose workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PurposeStep {
    pub capability: Capability,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub model_hint: Option<ModelHint>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub input: StepInput,
}

/// A named, reusable workflow that composes multiple capability calls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Purpose {
    pub name: String,
    pub description: String,
    pub steps: Vec<PurposeStep>,
}

/// The result of a single step execution.
#[derive(Debug, Clone)]
pub struct StepResult {
    pub step_index: usize,
    pub capability: Capability,
    pub output: String,
    pub duration_ms: u64,
    pub cost: Cost,
    pub model_used: Option<String>,
}

/// The result of executing a purpose.
#[derive(Debug, Clone)]
pub struct PurposeResult {
    pub purpose_name: String,
    /// Final step's output.
    pub output: String,
    pub steps: Vec<StepResult>,
    pub total_cost: Cost,
    pub total_duration_ms: u64,
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

pub struct PurposeBuilder {
    name: String,
    description: String,
    steps: Vec<PurposeStep>,
}

impl PurposeBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            description: String::new(),
            steps: vec![],
        }
    }

    pub fn description(mut self, desc: &str) -> Self {
        self.description = desc.into();
        self
    }

    pub fn step(mut self, step: PurposeStep) -> Self {
        self.steps.push(step);
        self
    }

    pub fn build(self) -> Purpose {
        Purpose {
            name: self.name,
            description: self.description,
            steps: self.steps,
        }
    }
}

pub struct StepBuilder {
    step: PurposeStep,
}

impl StepBuilder {
    pub fn new(capability: Capability) -> Self {
        Self {
            step: PurposeStep {
                capability,
                system_prompt: None,
                model_hint: None,
                max_tokens: None,
                input: StepInput::FromRequest,
            },
        }
    }

    pub fn system(mut self, prompt: &str) -> Self {
        self.step.system_prompt = Some(prompt.into());
        self
    }

    pub fn model_hint(mut self, hint: ModelHint) -> Self {
        self.step.model_hint = Some(hint);
        self
    }

    pub fn max_tokens(mut self, tokens: u32) -> Self {
        self.step.max_tokens = Some(tokens);
        self
    }

    pub fn input(mut self, input: StepInput) -> Self {
        self.step.input = input;
        self
    }

    pub fn build(self) -> PurposeStep {
        self.step
    }
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Execute a purpose workflow against the gateway, running each step in
/// sequence and threading outputs between steps according to their
/// [`StepInput`] configuration.
pub async fn execute_purpose(
    gateway: &Gateway,
    purpose: &Purpose,
    input: &str,
) -> Result<PurposeResult, GatewayError> {
    let mut step_results: Vec<StepResult> = Vec::new();
    let total_start = Instant::now();

    for (i, step) in purpose.steps.iter().enumerate() {
        let step_start = Instant::now();

        // Resolve the input text for this step
        let step_input = resolve_step_input(&step.input, input, &step_results);

        // Build the InferenceRequest for this step
        let request = build_step_request(step, &step_input);

        // Execute via gateway
        let response = gateway.execute(&request).await?;

        // Extract text output from response
        let output = response
            .content
            .or(response.transcription)
            .unwrap_or_default();

        let cost = response.actual_cost.unwrap_or_else(Cost::zero);

        step_results.push(StepResult {
            step_index: i,
            capability: step.capability.clone(),
            output,
            duration_ms: step_start.elapsed().as_millis() as u64,
            cost,
            model_used: response.model,
        });
    }

    let final_output = step_results
        .last()
        .map(|r| r.output.clone())
        .unwrap_or_default();

    let total_cost = step_results.iter().fold(Cost::zero(), |mut acc, r| {
        acc.input_tokens += r.cost.input_tokens;
        acc.output_tokens += r.cost.output_tokens;
        acc.total_tokens += r.cost.total_tokens;
        acc.input_cost += r.cost.input_cost;
        acc.output_cost += r.cost.output_cost;
        acc.total_cost += r.cost.total_cost;
        acc
    });

    Ok(PurposeResult {
        purpose_name: purpose.name.clone(),
        output: final_output,
        steps: step_results,
        total_cost,
        total_duration_ms: total_start.elapsed().as_millis() as u64,
    })
}

fn resolve_step_input(
    input_spec: &StepInput,
    original_input: &str,
    step_results: &[StepResult],
) -> String {
    match input_spec {
        StepInput::FromRequest => original_input.to_string(),
        StepInput::FromStep(idx) => step_results
            .get(*idx)
            .map(|r| r.output.clone())
            .unwrap_or_default(),
        StepInput::Template(template) => {
            let mut result = template.clone();
            for (i, step_result) in step_results.iter().enumerate() {
                result = result.replace(&format!("{{{i}}}"), &step_result.output);
            }
            // Also replace {input} with original input
            result = result.replace("{input}", original_input);
            result
        }
    }
}

fn build_step_request(step: &PurposeStep, input_text: &str) -> InferenceRequest {
    let system = step.system_prompt.clone();

    let messages = vec![Message::text(MessageRole::User, input_text.to_string())];

    let payload = match &step.capability {
        Capability::TextEmbed => Payload::Embed {
            texts: vec![input_text.to_string()],
        },
        // Default: use Chat payload (most flexible for text-based steps)
        _ => Payload::Chat {
            messages,
            system,
            max_tokens: step.max_tokens,
            temperature: None,
            tools: Vec::new(),
        },
    };

    InferenceRequest {
        capability: step.capability.clone(),
        model: match &step.model_hint {
            Some(ModelHint::Specific(model)) => Some(model.clone()),
            _ => None, // Let selection service choose
        },
        router: None,
        chain: None,
        payload,
        budget: None,
    }
}

// ---------------------------------------------------------------------------
// Built-in purposes
// ---------------------------------------------------------------------------

/// Return a set of common built-in purpose workflows.
pub fn builtin_purposes() -> Vec<Purpose> {
    vec![
        PurposeBuilder::new("classify")
            .description("Classify text into categories")
            .step(
                StepBuilder::new(Capability::TextChat)
                    .system(
                        "Classify the following text. \
                         Respond with only the category name, nothing else.",
                    )
                    .model_hint(ModelHint::Fastest)
                    .max_tokens(50)
                    .build(),
            )
            .build(),
        PurposeBuilder::new("summarize")
            .description("Summarize text concisely")
            .step(
                StepBuilder::new(Capability::TextChat)
                    .system("Summarize the following text concisely. Focus on key points.")
                    .model_hint(ModelHint::Balanced)
                    .max_tokens(500)
                    .build(),
            )
            .build(),
        PurposeBuilder::new("review_code")
            .description("Review code for bugs, security, and style issues")
            .step(
                StepBuilder::new(Capability::TextChat)
                    .system(
                        "Review the following code for bugs, security vulnerabilities, \
                         and style issues. List each issue with severity \
                         (critical/important/minor), file location, and suggested fix.",
                    )
                    .model_hint(ModelHint::Best)
                    .build(),
            )
            .build(),
        PurposeBuilder::new("transcribe_and_summarize")
            .description("Transcribe audio then summarize the transcript")
            .step(StepBuilder::new(Capability::AudioTranscribe).build())
            .step(
                StepBuilder::new(Capability::TextChat)
                    .system("Summarize the following transcript in 3-5 bullet points:")
                    .model_hint(ModelHint::Balanced)
                    .max_tokens(300)
                    .input(StepInput::FromStep(0))
                    .build(),
            )
            .build(),
        PurposeBuilder::new("explain_and_simplify")
            .description("Explain complex text, then simplify the explanation")
            .step(
                StepBuilder::new(Capability::TextChat)
                    .system("Explain the following in detail, covering all key concepts:")
                    .model_hint(ModelHint::Best)
                    .build(),
            )
            .step(
                StepBuilder::new(Capability::TextChat)
                    .system(
                        "Simplify the following explanation so a beginner can understand it. \
                         Use simple language and analogies:",
                    )
                    .model_hint(ModelHint::Balanced)
                    .max_tokens(300)
                    .input(StepInput::FromStep(0))
                    .build(),
            )
            .build(),
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::noop::NoopAdapter;
    use crate::adapters::{CapabilityRegistry, RegisterInto};
    use crate::circuit_breaker::{CircuitBreakerConfig, CircuitBreakerManager};
    use crate::types::config::{
        ChainEntry, FallbackChainConfig, FallbackTrigger, GatewayConfig, ModelConfig, RouterConfig,
    };
    use crate::types::cost::Cost;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    // -- helpers -----------------------------------------------------------

    fn noop_gateway_config() -> GatewayConfig {
        let mut routers = HashMap::new();
        routers.insert(
            "noop".to_string(),
            RouterConfig {
                url: "http://localhost".to_string(),
                api_key_env: None,
                api_key: None,
                enabled: true,
                timeout_ms: None,
                headers: HashMap::new(),
            },
        );

        let mut models = HashMap::new();
        models.insert(
            "noop".to_string(),
            ModelConfig {
                id: "noop".to_string(),
                api_model_id: None,
                provider: "noop".to_string(),
                capabilities: vec![
                    Capability::TextChat,
                    Capability::TextComplete,
                    Capability::TextEmbed,
                    Capability::AudioTranscribe,
                ],
                context_window: 4096,
                max_output_tokens: 1024,
                pricing: None,
            },
        );

        let mut chains = HashMap::new();
        chains.insert(
            "chat_chain".to_string(),
            FallbackChainConfig {
                id: "chat_chain".to_string(),
                capability: Capability::TextChat,
                models: vec![ChainEntry {
                    model: "noop".to_string(),
                    router: Some("noop".to_string()),
                    api_model_id: None,
                    priority: 1,
                }],
                fallback_triggers: vec![
                    FallbackTrigger::RateLimit,
                    FallbackTrigger::Timeout,
                    FallbackTrigger::ProviderError,
                ],
            },
        );

        GatewayConfig {
            routers,
            models,
            chains,
        }
    }

    async fn noop_gateway() -> Gateway {
        let config = noop_gateway_config();
        let adapters = CapabilityRegistry::new();
        let cb = CircuitBreakerManager::new(CircuitBreakerConfig {
            threshold: 5,
            timeout: Duration::from_secs(300),
            half_open_max_requests: 3,
        });

        let gw = Gateway::new(config, adapters, cb);
        Arc::new(NoopAdapter).register_into(&gw.adapters).await;
        gw
    }

    // -- serde tests -------------------------------------------------------

    #[test]
    fn purpose_serde_roundtrip() {
        let purpose = PurposeBuilder::new("test")
            .description("A test purpose")
            .step(
                StepBuilder::new(Capability::TextChat)
                    .system("Be helpful")
                    .model_hint(ModelHint::Balanced)
                    .max_tokens(100)
                    .build(),
            )
            .step(
                StepBuilder::new(Capability::TextChat)
                    .input(StepInput::FromStep(0))
                    .build(),
            )
            .build();

        let json = serde_json::to_string(&purpose).unwrap();
        let deserialized: Purpose = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.name, "test");
        assert_eq!(deserialized.description, "A test purpose");
        assert_eq!(deserialized.steps.len(), 2);
        assert_eq!(deserialized.steps[0].capability, Capability::TextChat);
        assert_eq!(
            deserialized.steps[0].system_prompt.as_deref(),
            Some("Be helpful"),
        );
        assert_eq!(deserialized.steps[0].model_hint, Some(ModelHint::Balanced),);
        assert_eq!(deserialized.steps[0].max_tokens, Some(100));
    }

    #[test]
    fn step_input_serde() {
        let inputs = vec![
            (StepInput::FromRequest, r#""from_request""#),
            (StepInput::FromStep(0), r#"{"from_step":0}"#),
            (
                StepInput::Template("hello {0}".to_string()),
                r#"{"template":"hello {0}"}"#,
            ),
        ];

        for (input, expected_json) in inputs {
            let json = serde_json::to_string(&input).unwrap();
            assert_eq!(json, expected_json, "serialization mismatch for {input:?}");

            let deserialized: StepInput = serde_json::from_str(&json).unwrap();
            // Verify round-trip by re-serializing
            let re_json = serde_json::to_string(&deserialized).unwrap();
            assert_eq!(re_json, expected_json);
        }
    }

    #[test]
    fn model_hint_serde() {
        let hints = vec![
            (ModelHint::Fastest, r#""fastest""#),
            (ModelHint::Balanced, r#""balanced""#),
            (ModelHint::Best, r#""best""#),
            (
                ModelHint::Specific("gpt-4".to_string()),
                r#"{"specific":"gpt-4"}"#,
            ),
        ];

        for (hint, expected_json) in hints {
            let json = serde_json::to_string(&hint).unwrap();
            assert_eq!(json, expected_json, "serialization mismatch for {hint:?}");

            let deserialized: ModelHint = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, hint);
        }
    }

    // -- builder tests -----------------------------------------------------

    #[test]
    fn purpose_builder_creates_valid_purpose() {
        let purpose = PurposeBuilder::new("two_step")
            .description("Two-step workflow")
            .step(
                StepBuilder::new(Capability::TextChat)
                    .system("Step one")
                    .build(),
            )
            .step(
                StepBuilder::new(Capability::TextComplete)
                    .input(StepInput::FromStep(0))
                    .build(),
            )
            .build();

        assert_eq!(purpose.name, "two_step");
        assert_eq!(purpose.description, "Two-step workflow");
        assert_eq!(purpose.steps.len(), 2);
        assert_eq!(purpose.steps[0].capability, Capability::TextChat);
        assert_eq!(purpose.steps[1].capability, Capability::TextComplete);
        assert!(matches!(purpose.steps[1].input, StepInput::FromStep(0)));
    }

    #[test]
    fn step_builder_creates_valid_step() {
        let step = StepBuilder::new(Capability::TextChat)
            .system("You are a classifier.")
            .model_hint(ModelHint::Fastest)
            .max_tokens(50)
            .build();

        assert_eq!(step.capability, Capability::TextChat);
        assert_eq!(step.system_prompt.as_deref(), Some("You are a classifier."),);
        assert_eq!(step.model_hint, Some(ModelHint::Fastest));
        assert_eq!(step.max_tokens, Some(50));
        assert!(matches!(step.input, StepInput::FromRequest));
    }

    // -- resolve_step_input tests ------------------------------------------

    #[test]
    fn resolve_input_from_request() {
        let result = resolve_step_input(&StepInput::FromRequest, "hello world", &[]);
        assert_eq!(result, "hello world");
    }

    #[test]
    fn resolve_input_from_step() {
        let step_results = vec![StepResult {
            step_index: 0,
            capability: Capability::TextChat,
            output: "step zero output".to_string(),
            duration_ms: 10,
            cost: Cost::zero(),
            model_used: None,
        }];

        let result = resolve_step_input(&StepInput::FromStep(0), "original", &step_results);
        assert_eq!(result, "step zero output");
    }

    #[test]
    fn resolve_input_from_step_out_of_bounds() {
        let result = resolve_step_input(&StepInput::FromStep(5), "original", &[]);
        assert_eq!(result, "");
    }

    #[test]
    fn resolve_input_template() {
        let step_results = vec![
            StepResult {
                step_index: 0,
                capability: Capability::TextChat,
                output: "alpha".to_string(),
                duration_ms: 10,
                cost: Cost::zero(),
                model_used: None,
            },
            StepResult {
                step_index: 1,
                capability: Capability::TextChat,
                output: "beta".to_string(),
                duration_ms: 10,
                cost: Cost::zero(),
                model_used: None,
            },
        ];

        let template = StepInput::Template("Step0={0}, Step1={1}, Input={input}".to_string());
        let result = resolve_step_input(&template, "original", &step_results);
        assert_eq!(result, "Step0=alpha, Step1=beta, Input=original");
    }

    // -- build_step_request tests ------------------------------------------

    #[test]
    fn build_step_request_text_chat() {
        let step = StepBuilder::new(Capability::TextChat)
            .system("Be concise")
            .max_tokens(200)
            .build();

        let request = build_step_request(&step, "What is Rust?");

        assert_eq!(request.capability, Capability::TextChat);
        assert!(request.model.is_none());
        if let Payload::Chat {
            messages,
            system,
            max_tokens,
            ..
        } = &request.payload
        {
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0].role, MessageRole::User);
            assert_eq!(messages[0].as_text(), "What is Rust?");
            assert_eq!(system.as_deref(), Some("Be concise"));
            assert_eq!(*max_tokens, Some(200));
        } else {
            panic!("Expected Chat payload");
        }
    }

    #[test]
    fn build_step_request_with_specific_model() {
        let step = StepBuilder::new(Capability::TextChat)
            .model_hint(ModelHint::Specific("claude-opus".to_string()))
            .build();

        let request = build_step_request(&step, "hello");

        assert_eq!(request.model, Some("claude-opus".to_string()));
    }

    #[test]
    fn build_step_request_embed_payload() {
        let step = StepBuilder::new(Capability::TextEmbed).build();

        let request = build_step_request(&step, "embed this");

        assert_eq!(request.capability, Capability::TextEmbed);
        if let Payload::Embed { texts } = &request.payload {
            assert_eq!(texts, &["embed this"]);
        } else {
            panic!("Expected Embed payload, got {:?}", request.payload);
        }
    }

    // -- builtin_purposes tests --------------------------------------------

    #[test]
    fn builtin_purposes_are_valid() {
        let purposes = builtin_purposes();
        assert!(purposes.len() >= 5);

        for purpose in &purposes {
            assert!(!purpose.name.is_empty(), "purpose name must not be empty");
            assert!(
                !purpose.description.is_empty(),
                "purpose '{}' must have a description",
                purpose.name,
            );
            assert!(
                !purpose.steps.is_empty(),
                "purpose '{}' must have at least 1 step",
                purpose.name,
            );
        }
    }

    // -- cost aggregation test ---------------------------------------------

    #[test]
    fn purpose_result_aggregates_costs() {
        let steps = [
            StepResult {
                step_index: 0,
                capability: Capability::TextChat,
                output: "a".to_string(),
                duration_ms: 100,
                cost: Cost {
                    input_tokens: 10,
                    output_tokens: 5,
                    total_tokens: 15,
                    input_cost: 0.001,
                    output_cost: 0.002,
                    total_cost: 0.003,
                    currency: "USD".to_string(),
                },
                model_used: Some("m1".to_string()),
            },
            StepResult {
                step_index: 1,
                capability: Capability::TextChat,
                output: "b".to_string(),
                duration_ms: 200,
                cost: Cost {
                    input_tokens: 20,
                    output_tokens: 10,
                    total_tokens: 30,
                    input_cost: 0.002,
                    output_cost: 0.004,
                    total_cost: 0.006,
                    currency: "USD".to_string(),
                },
                model_used: Some("m2".to_string()),
            },
        ];

        let total = steps.iter().fold(Cost::zero(), |mut acc, r| {
            acc.input_tokens += r.cost.input_tokens;
            acc.output_tokens += r.cost.output_tokens;
            acc.total_tokens += r.cost.total_tokens;
            acc.input_cost += r.cost.input_cost;
            acc.output_cost += r.cost.output_cost;
            acc.total_cost += r.cost.total_cost;
            acc
        });

        assert_eq!(total.input_tokens, 30);
        assert_eq!(total.output_tokens, 15);
        assert_eq!(total.total_tokens, 45);
        assert!((total.input_cost - 0.003).abs() < f64::EPSILON);
        assert!((total.output_cost - 0.006).abs() < f64::EPSILON);
        assert!((total.total_cost - 0.009).abs() < f64::EPSILON);
    }

    // -- integration tests (noop gateway) ----------------------------------

    #[tokio::test]
    async fn execute_single_step_purpose() {
        let gw = noop_gateway().await;

        let purpose = PurposeBuilder::new("single")
            .description("One-step test")
            .step(
                StepBuilder::new(Capability::TextChat)
                    .system("Reply concisely")
                    .build(),
            )
            .build();

        let result = execute_purpose(&gw, &purpose, "Hello").await.unwrap();

        assert_eq!(result.purpose_name, "single");
        assert_eq!(result.steps.len(), 1);
        assert_eq!(result.steps[0].step_index, 0);
        assert_eq!(result.steps[0].capability, Capability::TextChat);
        // Noop adapter returns content with "No inference provider"
        assert!(result.output.contains("No inference provider"));
    }

    #[tokio::test]
    async fn execute_multi_step_purpose() {
        let gw = noop_gateway().await;

        let purpose = PurposeBuilder::new("multi")
            .description("Two-step test")
            .step(
                StepBuilder::new(Capability::TextChat)
                    .system("Step one")
                    .build(),
            )
            .step(
                StepBuilder::new(Capability::TextChat)
                    .system("Step two")
                    .input(StepInput::FromStep(0))
                    .build(),
            )
            .build();

        let result = execute_purpose(&gw, &purpose, "Hello").await.unwrap();

        assert_eq!(result.purpose_name, "multi");
        assert_eq!(result.steps.len(), 2);
        assert_eq!(result.steps[0].step_index, 0);
        assert_eq!(result.steps[1].step_index, 1);
        // Both steps should have executed
        assert!(!result.steps[0].output.is_empty());
        assert!(!result.steps[1].output.is_empty());
        // Final output should come from step 1
        assert!(!result.output.is_empty());
    }
}
