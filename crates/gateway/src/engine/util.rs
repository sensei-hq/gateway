use chrono::{DateTime, Utc};

use crate::store::UsageTotals;
use crate::types::config::{MeterUnit, Window};
use crate::types::error::GatewayError;
use crate::types::request::Payload;

/// Map a [`GatewayError`] to a short, stable `code` string for a
/// [`StreamEvent::Error`]. `ProviderError` reports its HTTP status when
/// present (most useful to a consumer), otherwise a variant discriminant.
pub(super) fn stream_error_code(err: &GatewayError) -> String {
    match err {
        GatewayError::Authentication { .. } => "authentication".to_string(),
        GatewayError::RateLimit { .. } => "rate_limit".to_string(),
        GatewayError::BudgetExceeded { .. } => "budget_exceeded".to_string(),
        GatewayError::QuotaExceeded { .. } => "quota_exceeded".to_string(),
        GatewayError::Timeout { .. } => "timeout".to_string(),
        GatewayError::ProviderError { status, .. } => status
            .map(|s| s.to_string())
            .unwrap_or_else(|| "provider_error".to_string()),
        GatewayError::ModelUnavailable { .. } => "model_unavailable".to_string(),
        GatewayError::Unsupported { .. } => "unsupported".to_string(),
        GatewayError::NoCandidates { .. } => "no_candidates".to_string(),
        GatewayError::NotConfigured => "not_configured".to_string(),
        GatewayError::AllAttemptsFailed { .. } => "all_attempts_failed".to_string(),
        GatewayError::AllGated { .. } => "all_gated".to_string(),
        GatewayError::ModelNotReady { .. } => "model_not_ready".to_string(),
        GatewayError::InvalidConfig(_) => "invalid_config".to_string(),
        GatewayError::Network(_) => "network".to_string(),
        GatewayError::Serialization(_) => "serialization".to_string(),
    }
}

/// Rolling-window start for a [`Window`]: `now − period`. Week = 7 days,
/// Month ≈ 30 days (rolling, not calendar-aligned — see the AUTH design D2).
pub(super) fn window_start(now: DateTime<Utc>, w: Window) -> DateTime<Utc> {
    let period = match w {
        Window::Day => chrono::Duration::days(1),
        Window::Week => chrono::Duration::days(7),
        Window::Month => chrono::Duration::days(30),
    };
    now - period
}

/// Read a subject's aggregated usage for a given meter unit.
pub(super) fn usage_value(u: &UsageTotals, unit: MeterUnit) -> u64 {
    match unit {
        MeterUnit::Requests => u.requests,
        MeterUnit::InputTokens => u.input_tokens,
        MeterUnit::OutputTokens => u.output_tokens,
        MeterUnit::TotalTokens => u.total_tokens,
        MeterUnit::CostUsdMillis => u.cost_usd_millis,
    }
}

/// This call's contribution to a meter unit, known pre-flight. Requests count 1;
/// input/total tokens use the request estimate; output tokens and dollars are
/// unknown before the call (0), so those caps are enforced against usage already
/// on record (a deliberate soft guard — see the AUTH design D3).
pub(super) fn call_estimate(unit: MeterUnit, input_tokens: u32) -> u64 {
    match unit {
        MeterUnit::Requests => 1,
        MeterUnit::InputTokens | MeterUnit::TotalTokens => input_tokens as u64,
        MeterUnit::OutputTokens | MeterUnit::CostUsdMillis => 0,
    }
}

/// Estimate input token count from the request payload.
///
/// Rough heuristic: 1 token ~ 4 characters.
pub(super) fn estimate_input_tokens(payload: &Payload) -> u32 {
    match payload {
        Payload::Chat {
            messages, system, ..
        } => {
            let msg_chars: usize = messages.iter().map(|m| m.as_text().len()).sum();
            let sys_chars: usize = system.as_ref().map(|s| s.len()).unwrap_or(0);
            ((msg_chars + sys_chars) / 4) as u32
        }
        Payload::Embed { texts } => {
            let total_chars: usize = texts.iter().map(|t| t.len()).sum();
            (total_chars / 4) as u32
        }
        // STT has no meaningful text input to estimate.
        Payload::Stt { .. } => 0,
        // For TTS, rough heuristic on text length.
        Payload::Tts { text, .. } => (text.len() / 4) as u32,
        // Image generation: estimate based on prompt length.
        Payload::ImageGenerate { prompt, .. } => (prompt.len() / 4) as u32,
        // Video generation: estimate based on prompt length.
        Payload::VideoGenerate { prompt, .. } => (prompt.len() / 4) as u32,
    }
}

/// A deliberately pessimistic input estimate, for the CONTEXT-WINDOW gate only.
///
/// # What it counts that `estimate_input_tokens` does not, on a `Chat` payload
///
/// (Its sibling is named in backticks rather than linked throughout this doc: this
/// function is `pub` and that one is private to `engine`, so an intra-doc link from here
/// to there is a rustdoc warning. The private one is in this same file.)
///
/// The other payload kinds are answered in a different unit or not at all; see "The unit
/// is ONE inference the model's window has to hold" below, which is where the arms that
/// are NOT simply a more pessimistic rounding of the cost figure are argued.
///
/// 1. **Tool schemas.** The cost estimator sums messages + system and stops. An agent's
///    activated schemas routinely outweigh its prompt, so omitting them is not a
///    rounding error — it is most of the payload on exactly the requests this gate
///    exists to catch.
/// 2. **An assistant turn's `tool_calls`.** Those turns carry an EMPTY `content`, so
///    `as_text()` prices them at zero, and they are exactly what the orchestrator's
///    ReAct loop appends every turn and re-sends on every turn after
///    (`executor/agent.rs`). Every adapter puts them on the wire. A serialized plan or
///    an `fs_write` body is the largest thing that loop produces, and counting only the
///    text bodies missed all of it.
/// 3. **`bytes / 3`, not `chars / 4`.** Two changes in one line. The `/4` figure is the
///    rough one for English prose; JSON tokenizes nearer 3 chars/token, and schemas are
///    pure JSON. And the numerator is `str::len()` — UTF-8 BYTES, not characters —
///    which for non-Latin text is a further over-count (9 CJK characters are 27 bytes)
///    and therefore in the safe direction. Rounding is up (`div_ceil`) rather than
///    truncating, so a payload with any content at all estimates at least one token.
///
/// # What it does NOT count, which "pessimistic" must not be read to imply
///
/// **`Message::attachments` are not counted at all**, and that is a decision rather than
/// an oversight. There is no honest token model for media here: the only quantity this
/// crate can measure is the `MediaSource` string, and for a `Base64` source that
/// over-counts by two to three orders of magnitude (a 1 MB image is ~1.4 M base64 bytes,
/// so ~466 k "tokens" at `/3`, against a per-image cost providers publish in the low
/// thousands), while for a `Url` source its length has no relationship to the cost at
/// all. Either would reproduce the failure the
/// `Stt` arm below refuses — an estimate so large that every candidate is skipped and a
/// perfectly serviceable request becomes a terminal `AllGated`.
///
/// So the honest statement is narrower than "pessimistic": on a `Chat` payload this is an
/// upper bound on the TEXT of a request, not on the request. A multimodal call is
/// estimated on its text alone, so the gate can still admit a candidate that its images
/// push over. That was the status quo for every caller before SP-7a — nothing gated on the
/// window at all — but it is not what the word "pessimistic" would lead a reader to assume,
/// and a caller that starts attaching media owes this function a per-attachment token term
/// (providers publish the maxima; the term belongs in tokens, added after the divide, not
/// in bytes before it). No producer in this workspace attaches media today:
/// `executor/agent.rs` and `executor/dispatch.rs` both pass `Vec::new()`, and the adapters
/// only translate what they are handed.
///
/// # Why a third estimator rather than a call to one of the two that exist
///
/// Kept SEPARATE from `estimate_input_tokens` because, where the two measure the same
/// quantity, they want opposite biases: an under-count is optimistic pricing for the cost
/// gate and an admitted-but-doesn't-fit candidate for this one. Changing the shared figure
/// would silently make every `BudgetGate` decision more conservative — a real
/// improvement, and a different slice's call. On two payload kinds they do not measure the
/// same quantity at all, which is a second and independent reason they cannot be one
/// function; see the arm-by-arm section below.
///
/// The workspace once had a second pessimistic estimator,
/// `agent::prompt::est_tokens_pessimistic` (`crates/orchestrator`), which divided by 3 as
/// well but over `chars().count()` rather than bytes and took a `&str` rather than a
/// `Payload`. It is DELETED, and so is the `dispatch::est_input_tokens` that summed it
/// per string: the serving-window review found the two figures were not the same number
/// and that the budget clamp's soundness argument required them to be. This function is
/// the survivor and now serves both questions — see "Why this is `pub`" below for what
/// that cost and why it was the right trade.
///
/// # The unit is ONE inference the model's window has to hold, arm by arm
///
/// This is not the same question `estimate_input_tokens` answers, and on two payload
/// kinds the two functions deliberately return different QUANTITIES rather than
/// different roundings of the same one. The cost estimator prices the whole request;
/// this one measures what a single candidate's `context_window` is asked to hold. Where
/// the window does not bound the payload kind at all, the honest answer is 0, because
/// `est > window` is the only question the gate asks and 0 is how an arm says "not
/// applicable" (`Stt` has said it that way since this function was written).
///
/// - **`Chat`** — the whole conversation, plus system, plus tool schemas. One request,
///   one window. This is the arm the pessimism argument above is about, and the arm
///   where this figure must never undercut the cost estimate.
/// - **`Embed`** — the LARGEST single text, not the batch total. An embeddings request
///   is a BATCH: `dispatch::to_embed_request` forwards `texts` verbatim, and the
///   providers apply the model's max-input-tokens limit to each ARRAY ELEMENT. There is
///   usually a per-REQUEST aggregate limit too, but it is a separate and much larger
///   figure and it is not `context_window`; §8 of the SP-7a spec records it as ungated. The
///   original implementation summed the batch, which terminally refused 100 RAG chunks
///   of 300 bytes against an 8 k model — every one of them ~1.2% of the window — with a
///   remedy ("use a larger-window model") that is not even the fix; splitting the batch
///   is. So the sum is the COST (every text is embedded and every text is billed) and
///   the max is the WINDOW, and the two functions differ here on purpose. Pinned by
///   `an_embed_batch_of_individually_small_texts_is_admitted`.
/// - **`Stt`** — 0. Audio bytes are not characters; counting them would skip every
///   candidate for a 30-second clip.
/// - **`Tts` / `ImageGenerate` / `VideoGenerate`** — 0, because `context_window` is a
///   chat-model concept and these models do not publish one. What a speech or image
///   model publishes is a maximum prompt LENGTH in characters — a different bound, in a
///   different unit, that this config has no field for — so the number on such an entry
///   is filler and `0` is what an operator naturally writes for "not applicable".
///   Judging a TTS request against it would make every such request a terminal
///   `AllGated` telling the operator to route to a model with a bigger window, which is
///   not a property the model has. Gating them properly needs a real per-capability
///   input limit; that is recorded as deferred in the SP-7a spec's §8, and until it
///   exists these are exactly as ungated as they were before SP-7a. Pinned by
///   `tts_image_and_video_requests_admit_even_a_zero_window_candidate`.
///
/// **What this costs, stated so the `0`s are not read as "safe":** a TTS prompt longer
/// than the provider's character limit still earns a 400, and an oversized single embed
/// input is still caught but an oversized BATCH AGGREGATE is not. Both were true before
/// this slice — nothing gated on the window at all — so neither is a regression, and
/// both are cheaper than the over-refusal the alternatives produce.
///
/// An earlier version of this comment argued the opposite for the three media arms: that
/// a `_ => 0` catch-all would under-count them relative to the cost estimate and invert
/// "the one ordering the gate's safety rests on". That argument is circular. The ordering
/// matters only where the window question APPLIES; where it does not, matching the cost
/// estimate buys nothing and costs a permanently refused capability.
///
/// The accepted cost, stated plainly: a pessimistic figure can skip a model the prompt
/// would actually have fitted, sending the request to a larger, likely costlier
/// candidate. That is the cheaper error — the alternative is a provider 400 — and it is
/// visible, because the skip records both numbers.
/// Called once per request by `engine::execute` and once by `engine::execute_stream`,
/// beside `estimate_input_tokens`, and carried to the gate on
/// `SelectionCriteria::input_tokens_pessimistic` — plus once per budgeted `Chat` by the
/// orchestrator's clamp, for the reason argued below.
///
/// This used to add: "if `clippy -D warnings` ever reports this function dead, the
/// plumbing has been removed and every request is silently ungated on the window again."
/// **That tripwire is GONE** and the sentence would now be false: `dead_code` cannot fire
/// on a `pub` item re-exported at the crate root, whatever calls it. What stands in for it
/// is `the_engine_selects_on_the_pessimistic_estimate_not_the_cost_one` (the engine wiring)
/// and `tool_schemas_alone_push_a_request_over_a_small_candidates_window` (the composed
/// payload → estimator → gate path) — tests rather than a lint, which is the weaker
/// guarantee and the honest one to record.
///
/// # Why this is `pub`, and why that matters more than the API surface it costs
///
/// It is re-exported at the crate root — `gateway::estimate_input_tokens_pessimistic`, the
/// name the dependency rename in the orchestrator's `Cargo.toml` gives it — for ONE
/// out-of-crate caller: the orchestrator's SP-DATA-5 budget clamp
/// (`orchestrator/src/executor/dispatch.rs`), which sets a request's `max_tokens` before
/// [`crate::Gateway::execute`] is called and bounds it by
/// [`crate::Gateway::min_serving_context_window`] — the fold over
/// `{ m : m.context_window >= est }`, i.e. exactly the candidate set
/// [`crate::gates::context_window::ContextWindowGate`] admits on the same `est`.
///
/// That bound is sound only if the clamp's `est` and the gate's `est` are the SAME
/// NUMBER, and for one day they were not. The clamp had its own estimator — same parts,
/// same divisor, `ceil` applied PER STRING and summed rather than once over the total,
/// and over `chars` rather than bytes. On ASCII `Σ ceil(Lᵢ/3) >= ceil(Σ Lᵢ/3)`, so the
/// clamp's figure was the LARGER one and its serving set a strict SUBSET of the gate's:
/// the clamp could find nothing able to serve a request the gate then happily admitted
/// (dropping the window term entirely and dispatching `max_tokens` bounded only by the
/// output limit), or bound by a bigger model's window and send that figure to a smaller
/// winner. Both end at a provider 400 — `prompt + max_tokens > context_window` — which
/// on a budgeted run arrives as a terminal `NodeFailed` that no cap raise can clear,
/// on a call an UNBUDGETED run serves (unbudgeted omits `max_tokens` from the wire).
/// On multi-byte text the drift runs the other way (bytes here, chars there), so the two
/// figures were not ordered in either direction and no slack constant could fix it.
///
/// So the duplicate was deleted rather than aligned, and this function widened: one
/// estimator, called by both halves, makes the equality structural instead of an
/// invariant someone has to keep re-proving. Widening a production visibility to suit a
/// TEST would be the wrong trade (this module's own composed tests say so); widening it
/// so two crates cannot disagree about a number they must agree on is a different trade
/// and worth the surface.
///
/// Pinned across the boundary by the orchestrator's
/// `the_clamp_bounds_max_tokens_by_the_estimate_the_gate_will_judge_by` and
/// `the_serving_window_bound_is_safe_for_the_smallest_candidate_the_gate_admits`, which
/// assert the wire rule a provider enforces with a 400 —
/// `est + max_tokens <= context_window(model dispatched to)` — with `est` measured by
/// THIS function on the request that actually went out. Both were red before the
/// unification, in the two different ways a subset relation fails.
pub fn estimate_input_tokens_pessimistic(payload: &Payload) -> u32 {
    let chars: usize = match payload {
        Payload::Chat {
            messages,
            system,
            tools,
            ..
        } => {
            // Each message contributes its text body AND its tool calls. The second
            // term is not defensive: an assistant turn in the ReAct loop has an empty
            // body and carries everything in `tool_calls`, so a sum over `as_text()`
            // alone returns 0 for the largest messages the loop produces. The budget
            // clamp's deleted `executor/dispatch.rs::est_input_tokens` counted the same
            // two parts of a call (`name` + `arguments`) for the same reason; the clamp
            // now reaches this line instead of mirroring it.
            let msg_chars: usize = messages
                .iter()
                .map(|m| {
                    m.as_text().len()
                        + m.tool_calls
                            .iter()
                            .map(|c| c.name.len() + c.arguments.len())
                            .sum::<usize>()
                })
                .sum();
            let sys_chars: usize = system.as_ref().map(|s| s.len()).unwrap_or(0);
            // `to_string()` on the schema is the closest cheap stand-in for what the
            // adapter actually puts on the wire: providers all receive the JSON Schema
            // document verbatim (see `ToolDefinition`), so its serialized length is the
            // thing occupying the window, not the struct's in-memory size.
            let tool_chars: usize = tools
                .iter()
                .map(|t| {
                    t.name.len()
                        + t.description.as_ref().map(|d| d.len()).unwrap_or(0)
                        + t.input_schema.to_string().len()
                })
                .sum();
            msg_chars + sys_chars + tool_chars
        }
        // The LARGEST input, not their sum: the window bounds one array element. See the
        // arm-by-arm section above — `estimate_input_tokens` sums the same texts, and
        // that is right for COST and wrong for the window.
        Payload::Embed { texts } => texts.iter().map(|t| t.len()).max().unwrap_or(0),
        // The four payload kinds a `context_window` does not bound. Written out rather
        // than left to a `_ => 0` catch-all so that adding a seventh `Payload` variant is
        // a compile error here, and whoever adds it has to answer the window question for
        // it rather than inheriting a silent 0.
        Payload::Stt { .. }
        | Payload::Tts { .. }
        | Payload::ImageGenerate { .. }
        | Payload::VideoGenerate { .. } => 0,
    };
    // Saturate rather than wrap. `estimate_input_tokens` uses `as u32`, which is harmless
    // there because an overflowed cost estimate only mis-prices; here a wrap would turn a
    // 4-GiB payload into a tiny number and ADMIT it, which is precisely the failure this
    // estimate exists to prevent. `u32::MAX` skips every candidate instead, loudly.
    u32::try_from(chars.div_ceil(3)).unwrap_or(u32::MAX)
}

/// Extract the user-facing prompt text from a request payload, for addressing a
/// consensus workflow (which takes a plain prompt). Chat → its messages' text
/// joined by newlines; text-bearing media payloads → their prompt/text. Returns
/// `None` when there is no text to run a consensus over (embed / stt).
pub(super) fn request_input_text(payload: &Payload) -> Option<String> {
    let text = match payload {
        Payload::Chat { messages, .. } => messages
            .iter()
            .map(|m| m.as_text())
            .collect::<Vec<_>>()
            .join("\n"),
        Payload::Tts { text, .. } => text.clone(),
        Payload::ImageGenerate { prompt, .. } | Payload::VideoGenerate { prompt, .. } => {
            prompt.clone()
        }
        Payload::Embed { .. } | Payload::Stt { .. } => return None,
    };
    (!text.trim().is_empty()).then_some(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::skip_reason::SkipReason;
    use crate::types::request::{Message, MessageContent, MessageRole, ToolCall, ToolDefinition};

    fn chat(tools: Vec<ToolDefinition>) -> Payload {
        Payload::Chat {
            messages: vec![Message::text(
                MessageRole::User,
                "hello there, this is a prompt",
            )],
            system: Some("you are a helpful assistant".into()),
            max_tokens: None,
            temperature: None,
            tools,
        }
    }

    /// A chat carrying exactly the messages given, and nothing else — no system, no
    /// tools. Used by the tests that pin an ABSOLUTE figure, where a fixture with
    /// incidental content would make the arithmetic unreadable.
    fn chat_of(messages: Vec<Message>) -> Payload {
        Payload::Chat {
            messages,
            system: None,
            max_tokens: None,
            temperature: None,
            tools: Vec::new(),
        }
    }

    /// The shape the ReAct loop appends on every turn: an assistant turn whose text
    /// content is EMPTY and whose whole payload is the tool call.
    fn assistant_tool_call(name: &str, arguments: &str) -> Message {
        Message {
            role: MessageRole::Assistant,
            content: MessageContent::Text {
                text: String::new(),
            },
            tool_calls: vec![ToolCall {
                id: "call_1".to_string(),
                name: name.to_string(),
                arguments: arguments.to_string(),
            }],
            attachments: Vec::new(),
        }
    }

    fn fs_write_tool() -> ToolDefinition {
        ToolDefinition {
            name: "fs_write".into(),
            description: Some("Write a file to the workspace".into()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "path": { "type": "string" },
                    "contents": { "type": "string" }
                },
                "required": ["path", "contents"]
            }),
        }
    }

    // ---------------------------------------------------------------------------
    // Composed tests: payload → estimator → `SelectionCtx` → gate.
    //
    // These live here rather than in `gates/context_window.rs` because that is where the
    // estimator is, and a composed test belongs beside the half of the pair it is
    // asserting the units of. (They were originally here because
    // `estimate_input_tokens_pessimistic` was `pub(super)` to `engine` and widening a
    // production visibility to suit a TEST is the wrong trade. It is `pub` now — widened
    // for a PRODUCTION caller in another crate, which is a different trade and argued at
    // the function — so that reason has expired while the placement stays right.) What
    // they buy over the
    // gate's own unit tests is that the two halves agree about UNITS and DIRECTION over
    // a real `Payload`: the gate's tests hand-set a number, so a payload that estimates
    // in the wrong unit — or an estimator term that silently returns 0 — is invisible to
    // them.
    //
    // The remaining link, `engine::execute` computing the figure and putting it on
    // `SelectionCriteria`, is SP-7a Task 5's and is not exercised here; that is the
    // wiring where reading `input_tokens` instead still compiles, so Task 5 owes it a
    // test of its own.
    // ---------------------------------------------------------------------------

    struct NeverOpen;
    impl crate::gates::EndpointHealthRead for NeverOpen {
        fn open_until(&self, _endpoint: &str) -> Option<std::time::Instant> {
            None
        }
    }

    struct NeverCooling;
    impl crate::gates::RouterHealthRead for NeverCooling {
        fn cooling_until(&self, _router: &str) -> Option<std::time::Instant> {
            None
        }
    }

    struct NeverLocked;
    impl crate::gates::lockout::ModelLockoutRead for NeverLocked {
        fn locked(&self, _endpoint: &str) -> Option<crate::gates::lockout::LockView> {
            None
        }
    }

    fn model_with_window(context_window: u32) -> crate::types::config::ModelConfig {
        crate::types::config::ModelConfig {
            id: "some-model".to_string(),
            api_model_id: None,
            provider: "anthropic".to_string(),
            family: None,
            capabilities: vec![crate::types::capability::Capability::TextChat],
            context_window,
            max_output_tokens: 4096,
            pricing: None,
            catalog: None,
        }
    }

    fn router() -> crate::types::config::RouterConfig {
        crate::types::config::RouterConfig {
            url: "http://localhost".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: std::collections::HashMap::new(),
        }
    }

    /// Run the real gate over the real estimate of a real payload.
    fn gate_verdict_for(payload: &Payload, window: u32) -> crate::gates::GateVerdict {
        use crate::gates::AdmissionGate;
        let mc = model_with_window(window);
        let rc = router();
        let cfg = crate::types::config::GatewayConfig::default();
        let health = NeverOpen;
        crate::gates::context_window::ContextWindowGate.evaluate(
            &crate::gates::CandidateView {
                model: "some-model",
                router: "anthropic",
                endpoint: "anthropic:some-model".to_string(),
                model_config: &mc,
                router_config: &rc,
            },
            &crate::gates::SelectionCtx {
                capability: crate::types::capability::Capability::TextChat,
                budget: None,
                // Deliberately absent: the composed path must reach the gate through the
                // PESSIMISTIC field, and leaving the cost field empty means a gate that
                // read the wrong one would admit everything and redden these tests.
                input_tokens: None,
                input_tokens_pessimistic: Some(estimate_input_tokens_pessimistic(payload)),
                health: &health,
                now: std::time::Instant::now(),
                config: &cfg,
                router_health: &NeverCooling,
                model_lockout: &NeverLocked,
            },
        )
    }

    /// AC6, composed — a request whose TOOL SCHEMAS alone push it over a candidate's
    /// window is skipped for that candidate, and admitted by a larger one.
    ///
    /// The two halves of AC6 are each pinned elsewhere (the estimator counts schemas;
    /// the gate skips when `est > window`), but nothing joined them: no test started
    /// from a `Payload` and observed a skip. That join is where a unit mistake or a
    /// dropped term hides, and it is the acceptance criterion's own prescribed proof.
    ///
    /// The message text is deliberately tiny — under 300 bytes — so the ONLY thing that
    /// can carry this over an 8 k window is the schemas.
    #[test]
    fn tool_schemas_alone_push_a_request_over_a_small_candidates_window() {
        // 80 tools at ~410 bytes of serialized schema each: ~35 KB of JSON in all,
        // 11531 tokens at /3 — over an 8 k window and well under a 128 k one.
        let tools: Vec<ToolDefinition> = (0..80)
            .map(|i| ToolDefinition {
                name: format!("tool_{i}"),
                description: Some("does a thing".into()),
                input_schema: serde_json::json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string", "description": "an absolute path to the file this tool should operate on, which must exist" },
                        "contents": { "type": "string", "description": "the bytes to write, encoded as UTF-8 text, with no length limit imposed here" },
                        "mode": { "type": "string", "enum": ["append", "overwrite", "create"], "description": "how an existing file is treated" }
                    },
                    "required": ["path", "contents"]
                }),
            })
            .collect();
        let payload = Payload::Chat {
            messages: vec![Message::text(MessageRole::User, "write the file")],
            system: Some("you are a helpful assistant".into()),
            max_tokens: None,
            temperature: None,
            tools,
        };
        let prose_bytes = "write the file".len() + "you are a helpful assistant".len();
        assert!(
            prose_bytes < 300,
            "the fixture's prose must be negligible or this test does not isolate the \
             schemas: {prose_bytes} bytes"
        );

        match gate_verdict_for(&payload, 8_192) {
            crate::gates::GateVerdict::Skip(SkipReason::OverContextWindow {
                estimated,
                window,
            }) => {
                assert!(
                    estimated > 8_192,
                    "the recorded estimate must be the one that lost: {estimated}"
                );
                assert_eq!(window, 8_192, "and the window it lost to");
            }
            crate::gates::GateVerdict::Skip(other) => {
                panic!("expected an OverContextWindow skip, got {other}")
            }
            crate::gates::GateVerdict::Admit => panic!(
                "schemas worth {} tokens must not be admitted to an 8192-token window — \
                 the cost estimator, which omits them, would have said {}",
                estimate_input_tokens_pessimistic(&payload),
                estimate_input_tokens(&payload)
            ),
        }

        assert!(
            matches!(
                gate_verdict_for(&payload, 128_000),
                crate::gates::GateVerdict::Admit
            ),
            "and the SAME request must be admitted by a 128k candidate — the whole point \
             is that the question has a per-candidate answer"
        );
    }

    /// AC10, the Stt half — an audio request admits EVERY candidate, including one
    /// configured with a zero window.
    ///
    /// A zero window rather than a realistic one, because `est > window` with any
    /// realistic window is satisfied by a broad range of wrong estimates; only `est == 0`
    /// admits when the window is 0. This is the composed form of the "audio bytes are
    /// not characters" arm: if the estimator ever counts them, transcription becomes a
    /// permanent `AllGated` and this reddens.
    #[test]
    fn an_stt_request_admits_even_a_zero_window_candidate() {
        let payload = Payload::Stt {
            audio: vec![0u8; 960_000], // ~30 s of 16 kHz mono 16-bit
            language: None,
            format: "wav".to_string(),
        };
        assert!(
            matches!(
                gate_verdict_for(&payload, 0),
                crate::gates::GateVerdict::Admit
            ),
            "an Stt payload has no measurable text, so the window gate must never be the \
             reason a transcription candidate is skipped"
        );
    }

    /// AC10, the Embed half — an Embed request IS gated by the window, deliberately, on
    /// the size of its LARGEST input.
    ///
    /// "Unaffected" is the wrong word for Embed and this test is here to stop it being
    /// assumed: embedding models publish real context windows (8 k is typical), the
    /// estimator returns a real number for `Payload::Embed`, and a single text that
    /// exceeds a candidate's max input earns the same provider 400 as an oversized chat.
    /// The honest reading of AC10 is that **Stt** is the unaffected payload kind.
    ///
    /// The fixture's texts are 30 KB EACH, so the skip below holds under the per-item
    /// rule this gate applies. That is not an accident of the numbers — it is the reason
    /// this test cannot stand alone: it would pass just as well under the batch-SUM rule
    /// the estimator originally used, which is why
    /// `an_embed_batch_of_individually_small_texts_is_admitted` exists beside it and
    /// separates the two.
    #[test]
    fn a_single_oversized_embed_input_is_gated_by_the_window_like_a_chat() {
        let payload = Payload::Embed {
            texts: vec!["x".repeat(30_000), "y".repeat(30_000)],
        };
        assert!(
            matches!(
                gate_verdict_for(&payload, 8_192),
                crate::gates::GateVerdict::Skip(SkipReason::OverContextWindow { .. })
            ),
            "one 30 KB text is 10k tokens on its own and does not fit an 8192-token \
             embedding model"
        );
        assert!(
            matches!(
                gate_verdict_for(&payload, 128_000),
                crate::gates::GateVerdict::Admit
            ),
            "and it does fit a large one — the gate is per candidate for Embed too"
        );
    }

    /// A batch of individually SMALL texts is admitted, however many of them there are.
    ///
    /// The quantity a model's `context_window` bounds for an embedding request is ONE
    /// input, not the sum of the array: `to_embed_request` (`dispatch.rs`) forwards
    /// `texts` verbatim and every embeddings API applies its max-input-tokens limit per
    /// array ELEMENT (the aggregate per-request limit is a separate figure, orders of
    /// magnitude larger). So summing the batch and comparing that to the window refuses
    /// a perfectly serviceable request — 100 RAG chunks of 300 bytes here, each ~100
    /// tokens against an 8192-token model, ~1.2% of the window apiece.
    ///
    /// This is the case the sibling test below CANNOT see: its two texts EACH exceed the
    /// window, so it passes under either rule. This one passes only under the per-item
    /// rule, which is why the two are kept apart rather than merged.
    #[test]
    fn an_embed_batch_of_individually_small_texts_is_admitted() {
        let payload = Payload::Embed {
            texts: vec!["x".repeat(300); 100],
        };
        assert_eq!(
            estimate_input_tokens_pessimistic(&payload),
            100,
            "the figure the window bounds is the LARGEST single input (300 bytes ⇒ 100 \
             tokens), not the batch total (30 000 bytes ⇒ 10 000 tokens)"
        );
        assert!(
            matches!(
                gate_verdict_for(&payload, 8_192),
                crate::gates::GateVerdict::Admit
            ),
            "every item fits the window with room to spare, so the batch must be \
             admitted — the remedy an over-refusal would offer (a larger-window model) \
             is not even the right fix; splitting the batch is"
        );
    }

    /// AC10, the media half — a Tts / ImageGenerate / VideoGenerate request admits EVERY
    /// candidate, including one whose window is zero.
    ///
    /// `ModelConfig.context_window` is a chat-model concept: it is the provider-published
    /// PROMPT limit of a language model. A speech, image or video model publishes no such
    /// figure — what it publishes is a maximum prompt LENGTH in characters, a different
    /// field this config does not have — so `context_window` on such an entry is filler,
    /// and `0` is the natural thing an operator writes for "not applicable".
    ///
    /// Gating on it would therefore turn every request of those capabilities into a
    /// terminal `AllGated` whose remedy ("route to a model with a larger context window")
    /// names a lever that does not exist for the call. A zero window rather than a
    /// realistic one for the same reason the Stt test uses zero: with any positive window
    /// a broad range of wrong estimates still admits, and only an estimate of 0 admits
    /// against 0.
    #[test]
    fn tts_image_and_video_requests_admit_even_a_zero_window_candidate() {
        let long_prompt = "a".repeat(50_000);
        let payloads = vec![
            (
                "tts",
                Payload::Tts {
                    text: long_prompt.clone(),
                    voice: None,
                    speed: None,
                    output_format: crate::types::request::AudioFormat::Mp3,
                },
            ),
            (
                "image",
                Payload::ImageGenerate {
                    prompt: long_prompt.clone(),
                    size: None,
                    quality: None,
                    style: None,
                    n: 1,
                },
            ),
            (
                "video",
                Payload::VideoGenerate {
                    prompt: long_prompt.clone(),
                    duration_secs: Some(10),
                    resolution: Some("1080p".to_string()),
                },
            ),
        ];
        for (name, p) in &payloads {
            assert!(
                matches!(gate_verdict_for(p, 0), crate::gates::GateVerdict::Admit),
                "{name}: a model's context window does not bound this payload kind, so \
                 the window gate must never be the reason such a candidate is skipped"
            );
        }
    }

    /// AC6 + AC7 — the window estimate is >= the cost estimate for the same payload, and
    /// strictly greater once tool schemas are present.
    ///
    /// `estimate_input_tokens` counts messages + system at `chars/4`. It omits tool
    /// schemas entirely — and an agent's activated schemas routinely outweigh its prompt.
    /// For COST that is optimistic pricing; for a WINDOW it admits a candidate the
    /// request does not fit, which is the failure the gate exists to prevent. So this one
    /// counts the schemas and uses the JSON-ish `chars/3` rather than the prose `chars/4`.
    #[test]
    fn the_pessimistic_estimate_counts_tools_and_never_undercuts_the_cost_estimate() {
        let no_tools = chat(Vec::new());
        assert!(
            estimate_input_tokens_pessimistic(&no_tools) >= estimate_input_tokens(&no_tools),
            "must never undercut the cost estimate even with no tools: {} < {}",
            estimate_input_tokens_pessimistic(&no_tools),
            estimate_input_tokens(&no_tools)
        );

        let with_tools = chat(vec![fs_write_tool()]);
        assert!(
            estimate_input_tokens_pessimistic(&with_tools)
                > estimate_input_tokens_pessimistic(&no_tools),
            "adding a tool schema must raise the estimate — the schemas are exactly what \
             the cost estimator omits: {} !> {}",
            estimate_input_tokens_pessimistic(&with_tools),
            estimate_input_tokens_pessimistic(&no_tools)
        );

        // And the cost estimator is genuinely blind to them: this is the asymmetry that
        // makes a second function necessary rather than a nicety. If this ever stops
        // holding, `estimate_input_tokens` grew a tools term and §4's argument for two
        // estimates needs re-reading, not this assertion deleting.
        assert_eq!(
            estimate_input_tokens(&with_tools),
            estimate_input_tokens(&no_tools),
            "the COST estimate must still ignore tool schemas — widening it silently \
             makes every BudgetGate decision more conservative, which is a different \
             slice's call"
        );
    }

    /// AC7 + AC10 — every payload kind's answer, pinned as an ABSOLUTE figure, and the
    /// one kind where the "never undercuts the cost estimate" ordering applies.
    ///
    /// **This test used to assert that ordering for every kind, and that was wrong.** The
    /// two functions answer different questions, and on two payload kinds they therefore
    /// measure different QUANTITIES rather than the same quantity at two roundings:
    ///
    /// - `Embed` — the cost estimate sums the batch (every text is embedded and every
    ///   text is billed); the window bounds one array ELEMENT, so this one takes the max.
    ///   On a batch of many small texts the max is far BELOW the sum, and demanding
    ///   `pess >= cost` there is demanding the over-refusal
    ///   `an_embed_batch_of_individually_small_texts_is_admitted` exists to prevent.
    /// - `Tts` / `ImageGenerate` / `VideoGenerate` — `context_window` does not bound them
    ///   at all, so the honest answer is 0 and the ordering is vacuous. The old comment
    ///   argued the opposite ("a `_ => 0` catch-all … inverts the one ordering the gate's
    ///   safety rests on"); that argument assumed the window question applies, which is
    ///   the thing in question.
    ///
    /// So the ordering is asserted where it is meaningful — `Chat`, where both functions
    /// price the same single request — and every other arm is pinned by the exact number
    /// it must return. Absolute figures rather than an ordering for the reason AC7 gives
    /// generally: an ordering cannot see a divisor change, and here it also cannot see a
    /// sum silently returning where a max belongs.
    #[test]
    fn each_payload_kind_is_measured_in_the_unit_its_window_bounds() {
        // Chat: the SAME quantity as the cost estimate, and never below it.
        let with_tools = chat(vec![fs_write_tool()]);
        assert!(
            estimate_input_tokens_pessimistic(&with_tools) >= estimate_input_tokens(&with_tools),
            "chat: the window estimate must never undercut the cost estimate ({} < {}), \
             which would admit a candidate the request does not fit",
            estimate_input_tokens_pessimistic(&with_tools),
            estimate_input_tokens(&with_tools)
        );

        // Embed: the LARGEST text, not the sum — and here that is strictly BELOW the
        // cost estimate, which is the arm the old blanket ordering forbade.
        let embed = Payload::Embed {
            texts: vec!["some text to embed".into(), "and another".into()],
        };
        assert_eq!(
            estimate_input_tokens_pessimistic(&embed),
            6,
            "the largest of the two texts is 18 bytes ⇒ 6 tokens; the SUM would be 29 \
             bytes ⇒ 10, and that is the figure the provider does not apply"
        );
        assert_eq!(
            estimate_input_tokens(&embed),
            7,
            "while the COST estimate does sum them (29/4) — the two functions measure \
             different quantities here, deliberately, and this pins that they do"
        );

        // The four kinds a context window does not bound. A long prompt on each, so a
        // regression to `prompt.len()/3` cannot pass by rounding to zero.
        let long = "a".repeat(50_000);
        let unbounded: Vec<(&str, Payload)> = vec![
            (
                "stt",
                Payload::Stt {
                    audio: vec![0u8; 1000],
                    language: None,
                    format: "wav".to_string(),
                },
            ),
            (
                "tts",
                Payload::Tts {
                    text: long.clone(),
                    voice: None,
                    speed: None,
                    output_format: crate::types::request::AudioFormat::Mp3,
                },
            ),
            (
                "image",
                Payload::ImageGenerate {
                    prompt: long.clone(),
                    size: None,
                    quality: None,
                    style: None,
                    n: 1,
                },
            ),
            (
                "video",
                Payload::VideoGenerate {
                    prompt: long.clone(),
                    duration_secs: Some(10),
                    resolution: Some("1080p".to_string()),
                },
            ),
        ];
        for (name, p) in &unbounded {
            assert_eq!(
                estimate_input_tokens_pessimistic(p),
                0,
                "{name}: a model's context window is not a bound on this payload kind, \
                 so the gate must be given nothing to judge"
            );
        }
    }

    /// A tool's JSON SCHEMA is counted, not merely its name.
    ///
    /// The schema is the bulk of a tool definition — the fixture's is roughly four times
    /// its name and description together — and it is what an agent's activated tools put
    /// in the window. A term that counted `t.name` alone would still satisfy "adding a
    /// tool raises the estimate", pass every other test here, and under-count a ten-tool
    /// agent by thousands of tokens. So this pins the exact figure over a payload whose
    /// only content is the tool, where the arithmetic is readable.
    ///
    /// Mirrors `executor/dispatch.rs::a_tools_json_schema_is_counted`, which asks the
    /// same question of the budget clamp's estimator.
    #[test]
    fn a_tools_json_schema_is_counted_not_just_the_tools_name() {
        let tool = fs_write_tool();
        let schema_bytes = tool.input_schema.to_string().len();
        let name_and_description =
            tool.name.len() + tool.description.as_deref().map_or(0, str::len);
        assert!(
            schema_bytes > 3 * name_and_description,
            "the fixture must be schema-dominated or this test cannot tell a schema \
             term from a name term: schema {schema_bytes} vs name+description \
             {name_and_description}"
        );

        let payload = Payload::Chat {
            messages: Vec::new(),
            system: None,
            max_tokens: None,
            temperature: None,
            tools: vec![tool],
        };
        assert_eq!(
            estimate_input_tokens_pessimistic(&payload),
            (name_and_description + schema_bytes).div_ceil(3) as u32,
            "a tool definition contributes its name, its description AND its serialized \
             schema — the schema is what the provider receives verbatim and what \
             occupies the window"
        );
    }

    /// The SYSTEM prompt is counted, as its own term, to the byte.
    ///
    /// It was guarded only INDIRECTLY before: setting `sys_chars` to `0` reddened exactly
    /// one test in the workspace — `the_pessimistic_estimate_counts_tools_and_never_
    /// undercuts_the_cost_estimate`, and only because the COST estimate counts the system
    /// prompt, so dropping it here inverted an ordering that test asserts for an entirely
    /// different reason. A guard that fires because of what a SIBLING function counts is
    /// one refactor of that sibling away from not firing.
    ///
    /// It is pinned directly now because this function became the budget clamp's estimate
    /// too (`orchestrator/src/executor/dispatch.rs`), and the clamp's own direct guard —
    /// `every_part_of_the_input_is_counted_in_the_estimate`, over the `est_input_tokens`
    /// that unification deleted — went with it. An agent's system prompt is the term most
    /// likely to be dropped by accident, because it is the one every `ModelCall` fixture
    /// leaves `None`.
    ///
    /// An exact equality over a system-only payload, so a term that counted the system
    /// prompt at a fraction of its length would fail as loudly as one that dropped it.
    #[test]
    fn the_system_prompt_is_counted_in_the_window_estimate() {
        let system = "you are a careful assistant; prefer the smallest correct change";
        let payload = Payload::Chat {
            messages: Vec::new(),
            system: Some(system.to_string()),
            max_tokens: None,
            temperature: None,
            tools: Vec::new(),
        };
        assert_eq!(
            estimate_input_tokens_pessimistic(&payload),
            system.len().div_ceil(3) as u32,
            "a payload whose only content is a system prompt estimates at that prompt's \
             own size — the system half of what the provider is sent is not free"
        );
        // And it is ADDITIVE with the message term rather than replacing it: a payload
        // carrying both must cost both, which an implementation that returned
        // `max(system, messages)` would satisfy the assertion above and fail here.
        let user = "hello";
        let both = Payload::Chat {
            messages: vec![Message::text(MessageRole::User, user)],
            system: Some(system.to_string()),
            max_tokens: None,
            temperature: None,
            tools: Vec::new(),
        };
        assert_eq!(
            estimate_input_tokens_pessimistic(&both),
            (system.len() + user.len()).div_ceil(3) as u32,
            "the system prompt and the messages are SUMMED, and the sum is rounded once"
        );
    }

    /// The unit is `ceil(UTF-8 BYTES / 3)`, and both halves of that are load-bearing.
    ///
    /// `/3` rather than the prose `/4` is half the margin this estimate is built on, and
    /// `div_ceil` rather than `/` is what keeps a short payload from estimating zero.
    /// Neither is visible to a test that only compares two estimates to each other —
    /// reverting the divisor to 4 leaves every ordering intact — so one absolute figure
    /// pins both.
    ///
    /// BYTES rather than characters is deliberate too, and it is the unit the budget
    /// clamp inherited when the two estimators became one function. Its predecessor,
    /// `agent::prompt::est_tokens_pessimistic`, counted `chars().count()`; bytes ≥ chars
    /// always, so the difference is more margin, and it is largest exactly where a
    /// chars-per-token heuristic is weakest: CJK text is 3 bytes per character and
    /// tokenizes near 1 token per character, so the byte count lands close to the truth
    /// where the character count was a third of it. The CJK case below is that claim.
    ///
    /// The EMPTY payload is asserted here too, and it is the boundary the clamp cares
    /// about most: `allowance = remaining − est`, so rounding up — right for every
    /// non-empty input — must not charge a token for a system prompt that is not there.
    /// A `div_ceil` over a length that had been nudged (a `+1`, a `max(1)`) passes both
    /// figures above and fails this one. It carries what
    /// `agent::prompt::the_pessimistic_estimate_of_nothing_is_zero` used to.
    #[test]
    fn the_estimate_is_ceil_of_utf8_bytes_over_three() {
        assert_eq!(
            estimate_input_tokens_pessimistic(&chat_of(Vec::new())),
            0,
            "a payload with nothing in it costs nothing"
        );
        assert_eq!(
            estimate_input_tokens_pessimistic(&chat_of(vec![Message::text(MessageRole::User, "")])),
            0,
            "and neither does an empty message body"
        );
        assert_eq!(
            estimate_input_tokens_pessimistic(&chat_of(vec![Message::text(
                MessageRole::User,
                "0123456789"
            )])),
            4,
            "10 bytes is 3.33 tokens at /3, and it must round UP: /4 would give 2 and a \
             truncating /3 would give 3"
        );
        // 9 CJK characters, 27 UTF-8 bytes.
        let cjk = "日本語日本語日本語";
        assert_eq!(cjk.chars().count(), 9);
        assert_eq!(cjk.len(), 27);
        assert_eq!(
            estimate_input_tokens_pessimistic(&chat_of(vec![Message::text(
                MessageRole::User,
                cjk
            )])),
            9,
            "the numerator is bytes: 27/3. Counting characters instead would say 3, \
             which under-counts text that tokenizes at roughly one token per character"
        );
    }

    /// AC10, the Stt half — an audio payload estimates ZERO however long the clip is,
    /// so it can never be skipped by the window gate.
    ///
    /// Asserted as an absolute value, because "≥ the cost estimate" is satisfied by any
    /// non-negative number when the cost estimate is 0 — including `audio.len()`, which
    /// is the disaster this arm exists to avoid: 30 seconds of 16 kHz mono is ~960 KB,
    /// so `audio.len()/3` is ~320 000 "tokens" and every candidate is skipped for every
    /// transcription request, forever.
    #[test]
    fn an_stt_payload_estimates_zero_however_long_the_audio() {
        let payload = Payload::Stt {
            audio: vec![0u8; 1_000_000],
            language: None,
            format: "wav".to_string(),
        };
        assert_eq!(
            estimate_input_tokens_pessimistic(&payload),
            0,
            "audio bytes are not characters and must contribute nothing to a token \
             estimate"
        );
    }

    /// An assistant turn's TOOL CALLS occupy the window, so they are counted.
    ///
    /// This is the shape the ReAct loop produces on every turn and re-sends on every
    /// turn after: `executor/agent.rs` appends an assistant message whose `content` is
    /// empty and whose `tool_calls` carry the whole payload, and every adapter renders
    /// them on the wire (`anthropic/convert.rs` as a `ToolUse` block,
    /// `openai_compat/convert.rs` as a `tool_calls` array). Counting only `as_text()`
    /// priced a serialized plan or an `fs_write` body — the largest thing the loop
    /// produces — at ZERO, which for a WINDOW estimate is the under-count that admits a
    /// candidate the transcript overflows.
    ///
    /// Asserted as an absolute figure rather than "more than without", because a term
    /// that counted only `call.name` would satisfy the weaker form while still dropping
    /// the argument body, which is all of the bulk.
    #[test]
    fn an_assistant_turns_tool_call_arguments_are_counted() {
        let arguments = format!(
            "{{\"path\":\"src/main.rs\",\"contents\":\"{}\"}}",
            "x".repeat(3_000)
        );
        let payload = chat_of(vec![assistant_tool_call("fs_write", &arguments)]);
        let expected = ("fs_write".len() + arguments.len()).div_ceil(3) as u32;
        assert_eq!(
            estimate_input_tokens_pessimistic(&payload),
            expected,
            "an assistant turn's tool call must be counted in full — name AND arguments \
             — because that is what goes on the wire; its text content is empty, so \
             anything less prices the loop's largest payload at nearly nothing"
        );
    }

    #[test]
    fn estimate_input_tokens_stt() {
        let payload = Payload::Stt {
            audio: vec![0u8; 1000],
            language: None,
            format: "wav".to_string(),
        };
        assert_eq!(estimate_input_tokens(&payload), 0);
    }

    #[test]
    fn estimate_input_tokens_tts() {
        let payload = Payload::Tts {
            text: "Hello world, this is a test!".to_string(),
            voice: None,
            speed: None,
            output_format: crate::types::request::AudioFormat::Mp3,
        };
        let expected = ("Hello world, this is a test!".len() / 4) as u32;
        assert_eq!(estimate_input_tokens(&payload), expected);
    }

    #[test]
    fn estimate_input_tokens_image_generate() {
        let payload = Payload::ImageGenerate {
            prompt: "A beautiful sunset over mountains".to_string(),
            size: None,
            quality: None,
            style: None,
            n: 1,
        };
        let expected = ("A beautiful sunset over mountains".len() / 4) as u32;
        assert_eq!(estimate_input_tokens(&payload), expected);
    }

    #[test]
    fn estimate_input_tokens_video_generate() {
        let payload = Payload::VideoGenerate {
            prompt: "A timelapse of a blooming flower".to_string(),
            duration_secs: Some(10),
            resolution: Some("1080p".to_string()),
        };
        let expected = ("A timelapse of a blooming flower".len() / 4) as u32;
        assert_eq!(estimate_input_tokens(&payload), expected);
    }
}
