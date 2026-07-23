//! Consensus workflow: a fan-out debate ([`crate::panel`]) whose outputs are
//! merged by a synthesizer and optionally evaluated by an independent judge
//! (gh#14).
//!
//! Composes the panel primitive with plain [`Gateway::execute`](crate::Gateway::execute)
//! calls: **debate** (fan-out over family-distinct members) → **synthesize**
//! (one chain merges the debaters' answers) → **judge** (optional; a chain
//! family-independent of every debater scores the synthesis). See
//! [`Gateway::execute_consensus`](crate::Gateway::execute_consensus).

use crate::panel::PanelResponse;
use crate::types::capability::Capability;
use crate::types::cost::Cost;
use crate::types::request::{InferenceRequest, InferenceResponse, Message, MessageRole, Payload};

/// The result of a consensus run: every debater's outcome, the merged
/// synthesis, an optional judgment, and the summed cost across all phases.
#[derive(Debug)]
pub struct ConsensusResult {
    /// Per-debater results from the fan-out phase (successes and failures).
    pub debate: Vec<crate::panel::PanelSlotResult>,
    /// The synthesizer's merged answer.
    pub synthesis: InferenceResponse,
    /// The synthesizer's text output (convenience extraction).
    pub synthesis_output: String,
    /// The judge's response, when a judge is configured.
    pub judgment: Option<InferenceResponse>,
    /// The judge's text output, when a judge is configured.
    pub judgment_output: Option<String>,
    /// Debate + synthesis + judgment cost.
    pub total_cost: Cost,
}

/// Text of a chat/transcribe response (content, else transcription, else "").
pub(crate) fn text_of(resp: &InferenceResponse) -> String {
    resp.content
        .clone()
        .or_else(|| resp.transcription.clone())
        .unwrap_or_default()
}

/// Render the **successful** debaters' outputs as a labeled block for the
/// synthesizer's input. Failed slots are omitted — there's nothing to
/// synthesize from an error.
pub(crate) fn render_debate(panel: &PanelResponse) -> String {
    let mut out = String::new();
    for slot in &panel.slots {
        if let Ok(resp) = &slot.result {
            let label = slot.label.as_deref().unwrap_or(&slot.chain);
            out.push_str(&format!("### {label}\n{}\n\n", text_of(resp)));
        }
    }
    out.trim_end().to_string()
}

/// Build a single-turn chat request (used for the synthesizer and judge legs;
/// the caller sets `.chain` to route it to that role's chain).
pub(crate) fn build_chat_request(
    capability: Capability,
    input: &str,
    system: Option<String>,
) -> InferenceRequest {
    InferenceRequest {
        capability,
        model: None,
        router: None,
        chain: None,
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, input.to_string())],
            system,
            max_tokens: None,
            temperature: None,
            tools: Vec::new(),
        },
        budget: None,
        auth: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::panel::PanelSlotResult;
    use crate::types::error::GatewayError;

    fn resp(content: &str) -> InferenceResponse {
        InferenceResponse {
            success: true,
            content: Some(content.to_string()),
            embeddings: None,
            transcription: None,
            audio: None,
            images: None,
            videos: None,
            model: None,
            usage: None,
            tool_calls: Vec::new(),
            estimated_cost: None,
            actual_cost: None,
            attempts: Vec::new(),
        }
    }

    #[test]
    fn render_debate_labels_successes_and_skips_failures() {
        let panel = PanelResponse {
            slots: vec![
                PanelSlotResult {
                    label: Some("proposer".into()),
                    chain: "cp".into(),
                    family: Some("gemma".into()),
                    result: Ok(resp("Answer A")),
                },
                PanelSlotResult {
                    label: None,
                    chain: "cc".into(),
                    family: Some("qwen".into()),
                    result: Ok(resp("Answer B")),
                },
                PanelSlotResult {
                    label: Some("wild".into()),
                    chain: "cw".into(),
                    family: None,
                    result: Err(GatewayError::NotConfigured),
                },
            ],
            total_cost: Cost::zero(),
            collisions: Vec::new(),
        };
        let block = render_debate(&panel);
        assert!(block.contains("### proposer\nAnswer A"));
        // Unlabeled slot falls back to its chain name.
        assert!(block.contains("### cc\nAnswer B"));
        // The failed slot is omitted entirely.
        assert!(!block.contains("wild"));
    }

    #[test]
    fn build_chat_request_sets_system_and_user() {
        let req = build_chat_request(Capability::TextChat, "hello", Some("be terse".into()));
        assert_eq!(req.capability, Capability::TextChat);
        assert!(req.chain.is_none());
        match req.payload {
            Payload::Chat {
                messages, system, ..
            } => {
                assert_eq!(system.as_deref(), Some("be terse"));
                assert_eq!(messages.len(), 1);
                assert_eq!(messages[0].as_text(), "hello");
            }
            other => panic!("expected chat payload, got {other:?}"),
        }
    }
}
