use super::*;

impl super::Gateway {
    /// Run a consensus workflow: fan out a debate
    /// ([`execute_panel`](Self::execute_panel)), merge the debaters' answers with
    /// a synthesizer chain, then optionally have an **independent** judge chain
    /// evaluate the synthesis.
    ///
    /// Validated up front (no inference): the judge's primary family must differ
    /// from every debater's — a judge sharing a debater's family inherits its
    /// bias — on top of the panel's own `distinct_by` enforcement. Returns
    /// [`GatewayError::InvalidConfig`] on an independence/config violation, and a
    /// [`GatewayError::ProviderError`] only if *every* debater fails (nothing to
    /// synthesize). Non-streaming — a consensus is aggregate.
    pub async fn execute_consensus(
        &self,
        spec: &crate::types::config::ConsensusConfig,
        input: &str,
    ) -> Result<crate::consensus::ConsensusResult, GatewayError> {
        let cfg = self.config.read().await.clone();

        // Judge independence: the judge's family must differ from every debater's.
        if let Some(judge) = &spec.judge {
            let judge_family = crate::panel::chain_primary_family(&cfg, &judge.chain)?;
            for slot in &spec.panel.slots {
                let debater_family = crate::panel::chain_primary_family(&cfg, &slot.chain)?;
                if debater_family == judge_family {
                    return Err(GatewayError::InvalidConfig(format!(
                        "consensus '{}': judge chain '{}' shares family '{}' with debater chain '{}'; the judge must be independent",
                        spec.id, judge.chain, judge_family, slot.chain
                    )));
                }
            }
        }

        // Judge quorum (gh#20): mutually exclusive with a single judge, and
        // every quorum member must likewise be independent of every debater.
        if spec.judge.is_some() && spec.judge_quorum.is_some() {
            return Err(GatewayError::InvalidConfig(format!(
                "consensus '{}': set either `judge` or `judge_quorum`, not both",
                spec.id
            )));
        }
        if let Some(quorum) = &spec.judge_quorum {
            for jslot in &quorum.slots {
                let judge_family = crate::panel::chain_primary_family(&cfg, &jslot.chain)?;
                for slot in &spec.panel.slots {
                    let debater_family = crate::panel::chain_primary_family(&cfg, &slot.chain)?;
                    if debater_family == judge_family {
                        return Err(GatewayError::InvalidConfig(format!(
                            "consensus '{}': judge-quorum chain '{}' shares family '{}' with debater chain '{}'; every quorum member must be independent",
                            spec.id, jslot.chain, judge_family, slot.chain
                        )));
                    }
                }
            }
        }

        // 1. Debate — fan out the panel (this also forms + enforces distinctness).
        let debate_req = crate::consensus::build_chat_request(spec.capability.clone(), input, None);
        let panel = self.execute_panel(&debate_req, &spec.panel).await?;
        if panel.slots.iter().all(|s| s.result.is_err()) {
            return Err(GatewayError::ProviderError {
                adapter: spec.id.clone(),
                message: "consensus: every debater failed".to_string(),
                status: None,
            });
        }

        // 2. Synthesize — one chain merges the debaters' answers.
        let debate_block = crate::consensus::render_debate(&panel);
        let mut synth_req = crate::consensus::build_chat_request(
            spec.capability.clone(),
            &debate_block,
            spec.synthesizer.system_prompt.clone(),
        );
        synth_req.chain = Some(spec.synthesizer.chain.clone());
        let synthesis = self.execute(&synth_req).await?;
        let synthesis_output = crate::consensus::text_of(&synthesis);

        // 3. Judge (optional) — a single independent judge, or a quorum panel.
        let judge_input =
            format!("Debate:\n{debate_block}\n\nProposed synthesis:\n{synthesis_output}");
        let (judgment, judgment_output, judge_quorum) = if let Some(judge) = &spec.judge {
            let mut judge_req = crate::consensus::build_chat_request(
                spec.capability.clone(),
                &judge_input,
                judge.system_prompt.clone(),
            );
            judge_req.chain = Some(judge.chain.clone());
            let j = self.execute(&judge_req).await?;
            let jo = crate::consensus::text_of(&j);
            (Some(j), Some(jo), None)
        } else if let Some(quorum) = &spec.judge_quorum {
            // Fan the synthesis out to the judge quorum; each member's persona
            // (system prompt) comes from its own slot (gh#18), and formation
            // enforces the quorum's own family-distinctness.
            let quorum_req =
                crate::consensus::build_chat_request(spec.capability.clone(), &judge_input, None);
            let panel = self.execute_panel(&quorum_req, quorum).await?;
            (None, None, Some(panel))
        } else {
            (None, None, None)
        };

        // 4. Aggregate cost across debate + synthesis + judgment (single or quorum).
        let mut total_cost = panel.total_cost.clone();
        for resp in [Some(&synthesis), judgment.as_ref()].into_iter().flatten() {
            if let Some(c) = &resp.actual_cost {
                total_cost.input_tokens += c.input_tokens;
                total_cost.output_tokens += c.output_tokens;
                total_cost.total_tokens += c.total_tokens;
                total_cost.input_cost += c.input_cost;
                total_cost.output_cost += c.output_cost;
                total_cost.total_cost += c.total_cost;
            }
        }
        if let Some(q) = &judge_quorum {
            total_cost.input_tokens += q.total_cost.input_tokens;
            total_cost.output_tokens += q.total_cost.output_tokens;
            total_cost.total_tokens += q.total_cost.total_tokens;
            total_cost.input_cost += q.total_cost.input_cost;
            total_cost.output_cost += q.total_cost.output_cost;
            total_cost.total_cost += q.total_cost.total_cost;
        }

        Ok(crate::consensus::ConsensusResult {
            debate: panel.slots,
            synthesis,
            synthesis_output,
            judgment,
            judgment_output,
            judge_quorum,
            total_cost,
        })
    }

    /// Resolve a named consensus workflow from
    /// [`GatewayConfig::consensus`](crate::types::config::GatewayConfig::consensus)
    /// by [`request.consensus`](InferenceRequest::consensus) and run it via
    /// [`execute_consensus`](Self::execute_consensus) (gh#19).
    ///
    /// The workflow's prompt is taken from the request payload (the chat
    /// messages' text). `request.consensus` must be set; an unset field, an
    /// unknown id, or a payload with no text input is
    /// [`GatewayError::InvalidConfig`].
    pub async fn execute_consensus_addressed(
        &self,
        request: &InferenceRequest,
    ) -> Result<crate::consensus::ConsensusResult, GatewayError> {
        let id = request.consensus.as_deref().ok_or_else(|| {
            GatewayError::InvalidConfig("request.consensus is not set".to_string())
        })?;
        let spec =
            {
                let config = self.config.read().await;
                config.consensus.get(id).cloned().ok_or_else(|| {
                    GatewayError::InvalidConfig(format!("unknown consensus '{id}'"))
                })?
            };
        let input = request_input_text(&request.payload).ok_or_else(|| {
            GatewayError::InvalidConfig("consensus request payload has no text input".to_string())
        })?;
        self.execute_consensus(&spec, &input).await
    }
}
