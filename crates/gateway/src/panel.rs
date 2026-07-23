//! Fan-out "panel" execution: run one request across N slots (each an existing
//! fallback chain) concurrently and return every slot's result — the primitive
//! behind MoE / consensus panels (gh#13).
//!
//! A panel is *formed* before any inference: each slot's primary model + family
//! is resolved and the [`DistinctBy`] policy enforced, so a distinctness
//! violation fails fast rather than silently collapsing the panel onto one
//! model. The fan-out itself layers on
//! [`Gateway::execute`](crate::Gateway::execute) — one call per slot scoped to
//! that slot's chain — so each slot keeps its own fallback legs, circuit
//! breaker, and cost accounting. See [`Gateway::execute_panel`](crate::Gateway::execute_panel).

use std::collections::HashMap;

use crate::types::config::{DistinctBy, GatewayConfig, ModelConfig, PanelConfig};
use crate::types::cost::Cost;
use crate::types::error::GatewayError;
use crate::types::request::{InferenceRequest, InferenceResponse, Payload};

/// One slot's outcome in a [`PanelResponse`].
#[derive(Debug)]
pub struct PanelSlotResult {
    /// Role label from the [`crate::types::config::PanelSlot`] (e.g. `"proposer"`).
    pub label: Option<String>,
    /// The chain this slot ran.
    pub chain: String,
    /// The model family that actually answered (post-fallback) when known, else
    /// the formed primary's family. Used to flag runtime family collisions.
    pub family: Option<String>,
    /// The slot's result — `Ok` with the answer, or `Err` if this expert failed.
    /// A failed slot never sinks the panel.
    pub result: Result<InferenceResponse, GatewayError>,
}

/// The aggregate result of [`Gateway::execute_panel`](crate::Gateway::execute_panel):
/// one entry per slot, the summed cost of the successful slots, and any runtime
/// family collisions (two successful slots that ended on the same family despite
/// distinct primaries — a lost-diversity signal for the consumer).
#[derive(Debug)]
pub struct PanelResponse {
    pub slots: Vec<PanelSlotResult>,
    pub total_cost: Cost,
    pub collisions: Vec<String>,
}

/// A slot resolved during formation: its primary model + family, ready to run.
#[derive(Debug)]
pub(crate) struct FormedSlot {
    pub label: Option<String>,
    pub chain: String,
    pub primary_model: String,
    pub family: String,
    /// Per-slot system prompt (gh#18), carried from the [`crate::types::config::PanelSlot`]
    /// and layered onto this slot's request at fan-out time.
    pub system_prompt: Option<String>,
}

#[derive(Debug)]
pub(crate) struct FormedPanel {
    pub slots: Vec<FormedSlot>,
}

/// The family used for distinctness: the declared [`ModelConfig::family`], or
/// the model id when none is declared (so an undeclared model can't silently
/// satisfy a `distinct_by: family` check by comparing equal to everything).
fn model_family(model: &ModelConfig) -> String {
    model.family.clone().unwrap_or_else(|| model.id.clone())
}

/// Resolve the family of a chain's *primary* (lowest-priority) model. Used by
/// the consensus workflow to enforce judge independence. Errors mirror
/// [`form_panel`] (unknown chain / empty chain / unknown model).
pub(crate) fn chain_primary_family(
    config: &GatewayConfig,
    chain_name: &str,
) -> Result<String, GatewayError> {
    let chain = config
        .chains
        .get(chain_name)
        .ok_or_else(|| GatewayError::InvalidConfig(format!("unknown chain '{chain_name}'")))?;
    let primary = chain
        .models
        .iter()
        .min_by_key(|e| e.priority)
        .ok_or_else(|| {
            GatewayError::InvalidConfig(format!("chain '{chain_name}' has no models"))
        })?;
    let model = config.models.get(&primary.model).ok_or_else(|| {
        GatewayError::InvalidConfig(format!(
            "chain '{chain_name}' primary references unknown model '{}'",
            primary.model
        ))
    })?;
    Ok(model_family(model))
}

/// Resolve every slot's primary model + family and enforce `distinct_by`.
///
/// Fails fast — before any inference — with [`GatewayError::InvalidConfig`] on
/// an empty panel, an unknown chain/model, a slot chain whose capability
/// differs from the panel's, or a distinctness violation.
pub(crate) fn form_panel(
    config: &GatewayConfig,
    panel: &PanelConfig,
) -> Result<FormedPanel, GatewayError> {
    if panel.slots.is_empty() {
        return Err(GatewayError::InvalidConfig(format!(
            "panel '{}' has no slots",
            panel.id
        )));
    }

    let mut formed = Vec::with_capacity(panel.slots.len());
    for slot in &panel.slots {
        let chain = config.chains.get(&slot.chain).ok_or_else(|| {
            GatewayError::InvalidConfig(format!(
                "panel '{}' slot references unknown chain '{}'",
                panel.id, slot.chain
            ))
        })?;
        if chain.capability != panel.capability {
            return Err(GatewayError::InvalidConfig(format!(
                "panel '{}' is {:?} but slot chain '{}' is {:?}",
                panel.id, panel.capability, slot.chain, chain.capability
            )));
        }
        // The chain's primary is its lowest-priority entry.
        let primary = chain
            .models
            .iter()
            .min_by_key(|e| e.priority)
            .ok_or_else(|| {
                GatewayError::InvalidConfig(format!(
                    "panel '{}' slot chain '{}' has no models",
                    panel.id, slot.chain
                ))
            })?;
        let model = config.models.get(&primary.model).ok_or_else(|| {
            GatewayError::InvalidConfig(format!(
                "panel '{}' slot chain '{}' primary references unknown model '{}'",
                panel.id, slot.chain, primary.model
            ))
        })?;
        if panel.distinct_by == DistinctBy::Family && model.family.is_none() {
            tracing::warn!(
                panel = %panel.id,
                model = %model.id,
                "panel distinct_by=family but model declares no family; using its id as the family",
            );
        }
        formed.push(FormedSlot {
            label: slot.label.clone(),
            chain: slot.chain.clone(),
            primary_model: model.id.clone(),
            family: model_family(model),
            system_prompt: slot.system_prompt.clone(),
        });
    }

    enforce_distinct(panel, &formed)?;
    Ok(FormedPanel { slots: formed })
}

/// Reject a formed panel whose slots collide on the configured `distinct_by`
/// axis (primary model or family).
fn enforce_distinct(panel: &PanelConfig, formed: &[FormedSlot]) -> Result<(), GatewayError> {
    if panel.distinct_by == DistinctBy::None {
        return Ok(());
    }
    let mut seen: HashMap<String, String> = HashMap::new();
    for slot in formed {
        let key = match panel.distinct_by {
            DistinctBy::Model => slot.primary_model.clone(),
            DistinctBy::Family => slot.family.clone(),
            DistinctBy::None => unreachable!("None returns early above"),
        };
        if let Some(prev) = seen.get(&key) {
            let axis = if panel.distinct_by == DistinctBy::Family {
                "family"
            } else {
                "model"
            };
            return Err(GatewayError::InvalidConfig(format!(
                "panel '{}' requires distinct {axis}s, but chains '{}' and '{}' both resolve to {axis} '{}'",
                panel.id, prev, slot.chain, key
            )));
        }
        seen.insert(key, slot.chain.clone());
    }
    Ok(())
}

/// Layer a slot's system prompt onto a chat request in place (gh#18): appended
/// after any base system prompt (base instructions first, then the slot's
/// persona), so each panel debater can be steered independently. A no-op when
/// the slot declares no prompt, or when the payload isn't a chat (system
/// prompts only apply to chat).
pub(crate) fn apply_slot_system_prompt(req: &mut InferenceRequest, slot_system: Option<&str>) {
    let Some(sp) = slot_system else { return };
    if let Payload::Chat { system, .. } = &mut req.payload {
        *system = Some(match system.take() {
            Some(base) if !base.trim().is_empty() => format!("{base}\n\n{sp}"),
            _ => sp.to_string(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::capability::Capability;
    use crate::types::config::{ChainEntry, FallbackChainConfig, PanelSlot, RouterConfig};

    fn model(id: &str, family: Option<&str>) -> ModelConfig {
        ModelConfig {
            id: id.into(),
            api_model_id: None,
            provider: "r".into(),
            family: family.map(Into::into),
            capabilities: vec![Capability::TextChat],
            context_window: 4096,
            max_output_tokens: 1024,
            pricing: None,
        }
    }

    fn chain(id: &str, model: &str) -> FallbackChainConfig {
        FallbackChainConfig {
            id: id.into(),
            capability: Capability::TextChat,
            models: vec![ChainEntry {
                model: model.into(),
                router: Some("r".into()),
                api_model_id: None,
                priority: 1,
            }],
            fallback_triggers: vec![],
        }
    }

    fn cfg() -> GatewayConfig {
        let mut routers = HashMap::new();
        routers.insert(
            "r".to_string(),
            RouterConfig {
                url: "http://x".into(),
                api_key_env: None,
                api_key: None,
                enabled: true,
                timeout_ms: None,
                headers: HashMap::new(),
            },
        );
        let mut models = HashMap::new();
        models.insert("gemma-a".to_string(), model("gemma-a", Some("gemma")));
        models.insert("qwen-a".to_string(), model("qwen-a", Some("qwen")));
        models.insert("gemma-b".to_string(), model("gemma-b", Some("gemma")));
        let mut chains = HashMap::new();
        chains.insert("cha".to_string(), chain("cha", "gemma-a"));
        chains.insert("chb".to_string(), chain("chb", "qwen-a"));
        chains.insert("chc".to_string(), chain("chc", "gemma-b"));
        GatewayConfig {
            routers,
            models,
            chains,
            constraints: Default::default(),
            panels: Default::default(),
            consensus: Default::default(),
        }
    }

    fn panel(distinct_by: DistinctBy, slots: &[&str]) -> PanelConfig {
        PanelConfig {
            id: "p".into(),
            capability: Capability::TextChat,
            distinct_by,
            slots: slots
                .iter()
                .map(|c| PanelSlot {
                    chain: (*c).into(),
                    label: None,
                    system_prompt: None,
                })
                .collect(),
        }
    }

    #[test]
    fn distinct_families_form_ok() {
        let formed = form_panel(&cfg(), &panel(DistinctBy::Family, &["cha", "chb"])).unwrap();
        assert_eq!(formed.slots.len(), 2);
        assert_eq!(formed.slots[0].family, "gemma");
        assert_eq!(formed.slots[1].family, "qwen");
    }

    #[test]
    fn same_family_rejected() {
        // cha (gemma) + chc (gemma) collide on family.
        let err = form_panel(&cfg(), &panel(DistinctBy::Family, &["cha", "chc"])).unwrap_err();
        assert!(
            matches!(err, GatewayError::InvalidConfig(ref m) if m.contains("family")),
            "got {err:?}"
        );
    }

    #[test]
    fn distinct_by_none_allows_same_family() {
        assert!(form_panel(&cfg(), &panel(DistinctBy::None, &["cha", "chc"])).is_ok());
    }

    #[test]
    fn distinct_by_model_rejects_same_primary() {
        // Two slots on the same chain resolve to the same primary model.
        let err = form_panel(&cfg(), &panel(DistinctBy::Model, &["cha", "cha"])).unwrap_err();
        assert!(matches!(err, GatewayError::InvalidConfig(_)), "got {err:?}");
    }

    #[test]
    fn unknown_chain_errors() {
        let err = form_panel(&cfg(), &panel(DistinctBy::None, &["nope"])).unwrap_err();
        assert!(
            matches!(err, GatewayError::InvalidConfig(ref m) if m.contains("unknown chain")),
            "got {err:?}"
        );
    }

    #[test]
    fn empty_panel_errors() {
        let err = form_panel(&cfg(), &panel(DistinctBy::Family, &[])).unwrap_err();
        assert!(matches!(err, GatewayError::InvalidConfig(_)), "got {err:?}");
    }

    #[test]
    fn slot_system_prompt_layers_onto_chat() {
        use crate::types::request::{Message, MessageRole};

        fn chat_req(system: Option<&str>) -> InferenceRequest {
            InferenceRequest {
                capability: Capability::TextChat,
                model: None,
                router: None,
                chain: None,
                payload: Payload::Chat {
                    messages: vec![Message::text(MessageRole::User, "hi")],
                    system: system.map(Into::into),
                    max_tokens: None,
                    temperature: None,
                    tools: Vec::new(),
                },
                budget: None,
                auth: None,
                panel: None,
                consensus: None,
            }
        }
        fn system_of(req: &InferenceRequest) -> Option<String> {
            match &req.payload {
                Payload::Chat { system, .. } => system.clone(),
                _ => None,
            }
        }

        // No slot prompt → request is unchanged.
        let mut req = chat_req(None);
        apply_slot_system_prompt(&mut req, None);
        assert_eq!(system_of(&req), None);

        // Set on an empty base.
        let mut req = chat_req(None);
        apply_slot_system_prompt(&mut req, Some("Argue in favour"));
        assert_eq!(system_of(&req).as_deref(), Some("Argue in favour"));

        // Layered *after* an existing base prompt.
        let mut req = chat_req(Some("Be concise"));
        apply_slot_system_prompt(&mut req, Some("Red-team it"));
        assert_eq!(
            system_of(&req).as_deref(),
            Some("Be concise\n\nRed-team it")
        );

        // Two different slot prompts → distinct outgoing requests.
        let mut a = chat_req(None);
        let mut b = chat_req(None);
        apply_slot_system_prompt(&mut a, Some("proposer persona"));
        apply_slot_system_prompt(&mut b, Some("challenger persona"));
        assert_ne!(system_of(&a), system_of(&b));
    }
}
