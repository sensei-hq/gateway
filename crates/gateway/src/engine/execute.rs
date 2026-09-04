use super::*;
use crate::selection::SelectedModel;
use crate::types::config::FallbackTrigger;

/// Outcome of one per-attempt step in the fallback-chain walk — the seam a
/// later plan extends. Lets the top-level `execute` loop read as: for each
/// candidate, run [`Gateway::attempt_candidate`] and act on the outcome, i.e.
/// return the response, continue to the next candidate, or break to exhaustion
/// handling.
pub(super) enum StepOutcome {
    /// The candidate produced a terminal response; `execute` returns it.
    /// Boxed to keep the enum small (the other variants carry no data).
    Done(Box<InferenceResponse>),
    /// The candidate failed in a way that permits stepping down the chain (no
    /// adapter registered, or an error whose `should_trigger_fallback` is true);
    /// `execute` continues to the next candidate.
    FallBack,
    /// The candidate failed and fallback is not allowed for this error;
    /// `execute` breaks and proceeds to exhaustion handling.
    Stop,
}

impl super::Gateway {
    /// Execute an inference request, walking the fallback chain on failure.
    ///
    /// Returns `GatewayError::NotConfigured` if no config has been set.
    #[tracing::instrument(
        skip(self, request),
        fields(capability = ?request.capability, chain = ?request.chain)
    )]
    pub async fn execute(
        &self,
        request: &InferenceRequest,
    ) -> Result<InferenceResponse, GatewayError> {
        // 1. Clone config from RwLock
        let config = self.config.read().await.clone();

        if config.routers.is_empty() && config.models.is_empty() && config.chains.is_empty() {
            return Err(GatewayError::NotConfigured);
        }

        // 2. Build SelectionCriteria from request. TWO size estimates over one payload,
        // for two gates that want opposite biases: `input_tokens` is the cost figure the
        // `BudgetGate` prices from, and `input_tokens_pessimistic` is the window figure
        // the `ContextWindowGate` judges fit by. They must not be swapped — the cost one
        // omits tool schemas and divides by 4, so feeding it to the window gate admits
        // exactly the requests that gate exists to catch, and it compiles. Pinned by
        // `tests::the_engine_selects_on_the_pessimistic_estimate_not_the_cost_one`.
        let input_tokens = estimate_input_tokens(&request.payload);
        let input_tokens_pessimistic = estimate_input_tokens_pessimistic(&request.payload);
        let criteria = SelectionCriteria {
            capability: request.capability.clone(),
            model: request.model.clone(),
            router: request.router.clone(),
            chain: request.chain.clone(),
            budget: request.budget,
            input_tokens: Some(input_tokens),
            input_tokens_pessimistic: Some(input_tokens_pessimistic),
        };

        // 3. Select all candidates
        let svc = ModelSelectionService::new(
            &config,
            &self.circuit_breaker,
            &self.cooldown,
            &self.model_lockout,
        );
        let result = svc.select_all(&criteria);

        // 4. No candidates? Selection admitted nothing. If every skip was a gate
        // (health-lock / cooling / breaker-open / model-lockout / over-budget /
        // over-context-window), this is an `AllGated` rather than a bare
        // `NoCandidates` — a durable pause when any of those gates is TIMED, and a
        // human-action failure when every one is terminal
        // (`exhaustion::all_gated_error` takes `resume_after` from the timed ones
        // alone; over-window is always terminal, since no deadline makes a window
        // bigger). Only an all-structural (misconfig / wrong-capability /
        // nothing-configured) selection stays `NoCandidates`.
        if result.all_candidates.is_empty() {
            tracing::warn!("no candidates available for request");
            if let Some(gated) = super::exhaustion::all_gated_error(&result.skipped, &[]) {
                return Err(gated);
            }
            return Err(GatewayError::NoCandidates {
                capability: request.capability.clone(),
            });
        }

        tracing::debug!(
            candidates = result.all_candidates.len(),
            "selected candidates for request"
        );

        // Quota pre-flight (AUTH): refuse an over-quota subject before any
        // provider call. No-op without a store / auth / matching constraints.
        self.check_quota(&config, request, input_tokens).await?;

        // 5. Get fallback triggers from chain (or empty)
        let fallback_triggers = result
            .chain
            .as_ref()
            .map(|c| c.fallback_triggers.as_slice())
            .unwrap_or(&[]);

        // 6. Walk candidates in order. When the caller disables fallback
        // (`allow_fallback = false`), attempt only the primary candidate — the
        // failure branches below then have nothing to step down to, so a failed
        // primary returns its error rather than walking the chain.
        let max_attempts = if request.allow_fallback {
            result.all_candidates.len()
        } else {
            1
        };
        let mut attempts: Vec<Attempt> = Vec::new();
        // Per-attempt gate contributions, aggregated at exhaustion into `AllGated`
        // (a recoverable limit that just locked its endpoint contributes a timed
        // resume instant; a terminal one a human-action; a hard fault vetoes
        // all-gated). Kept alongside `attempts` and consumed only at exhaustion.
        let mut contributions: Vec<super::exhaustion::GateContribution> = Vec::new();

        for (sequence, candidate) in (1_u8..).zip(result.all_candidates.iter().take(max_attempts)) {
            match self
                .attempt_candidate(
                    request,
                    candidate,
                    sequence,
                    fallback_triggers,
                    &mut attempts,
                    &mut contributions,
                )
                .await
            {
                StepOutcome::Done(response) => return Ok(*response),
                StepOutcome::FallBack => continue,
                StepOutcome::Stop => break,
            }
        }

        // 7. All candidates exhausted. If a readiness probe is attached, a chain
        // can fail purely because its model(s) are still provisioning: consult the
        // probe for the attempted candidates in priority order and degrade the
        // first still-in-flight one to a terminal `ModelNotReady` (which never
        // triggers fallback) instead of the generic `AllAttemptsFailed`. With no
        // probe attached this block is skipped and behaviour is byte-identical.
        if let Some(probe) = &self.probe {
            for attempt in &attempts {
                let phase = probe.phase(&attempt.model).await;
                if phase.is_in_flight() {
                    return Err(GatewayError::ModelNotReady {
                        model: attempt.model.clone(),
                        phase,
                    });
                }
            }
        }

        // Aggregate selection-skips + per-attempt contributions: if every
        // candidate was gated (health-skip or classified limit) and none
        // hard-failed, this exhaustion is a durable `AllGated` pause; otherwise
        // it stays the generic `AllAttemptsFailed`. Built from `&result.skipped`
        // + `&contributions` (not `attempts`), so it can be computed before
        // `attempts` is moved into `AllAttemptsFailed` below.
        //
        // Guard: only aggregate to `AllGated` when the walk actually attempted
        // every candidate it was willing to try (`attempts.len() == max_attempts`).
        // A terminal limit (401 / credits) `Stop`s the walk early and leaves the
        // later, still-eligible candidates un-attempted (neither skipped nor
        // contributed) — those are NOT gated, so the "every candidate gated"
        // invariant fails and this stays `AllAttemptsFailed` (a `Stop` on the LAST
        // candidate still counts as attempted-all, so `[A(429→over), B(auth→stop)]`
        // is correctly all-gated).
        let attempted_all = attempts.len() == max_attempts;
        let gated = if attempted_all {
            super::exhaustion::all_gated_error(&result.skipped, &contributions)
        } else {
            None
        };

        let errors = attempts
            .iter()
            .filter_map(|a| {
                a.error
                    .as_ref()
                    .map(|e| format!("[{}:{}] {}", a.adapter, a.model, e))
            })
            .collect::<Vec<_>>()
            .join("; ");
        tracing::warn!(
            attempts = attempts.len(),
            errors = %errors,
            "all attempts failed"
        );

        // Best-effort record of the failed terminal call (observability + request
        // counting), attributed to the last attempted candidate.
        if let Some(last) = attempts.last() {
            self.record_call(InferenceCall {
                id: Uuid::new_v4(),
                session_id: None,
                project_id: None,
                capability: request.capability.clone(),
                chain_id: request.chain.clone(),
                adapter: last.adapter.clone(),
                model: last.model.clone(),
                api_model_id: Some(last.api_model_id.clone()),
                input_tokens: last.tokens.as_ref().map(|u| u.input_tokens),
                output_tokens: last.tokens.as_ref().map(|u| u.output_tokens),
                cost_usd: last.cost.unwrap_or(0.0),
                cost_estimated: None, // §D LN-4: embedded-plane estimate population deferred
                duration_ms: last.duration_ms,
                status: CallStatus::Failed,
                error_type: last.error.clone(),
                fallback_sequence: last.sequence.saturating_sub(1),
                recorded_at: Utc::now(),
                subject_id: request.auth.as_ref().map(|a| a.subject_id),
                tier: request.auth.as_ref().and_then(|a| a.tier.clone()),
            })
            .await;
        }

        Err(gated.unwrap_or_else(|| GatewayError::AllAttemptsFailed {
            attempts: attempts.len(),
            errors,
            attempts_detail: attempts,
        }))
    }

    /// Run one per-attempt step of the fallback-chain walk against a single
    /// resolved `candidate`: dispatch the request, then either return the
    /// successful response (filling cost and recording the terminal call) or
    /// record a failed [`Attempt`] and signal whether `execute` should fall back
    /// to the next candidate or stop. Pushes each attempt onto `attempts` in the
    /// same order/content the inline loop did. The per-attempt seam a later plan
    /// extends.
    pub(super) async fn attempt_candidate(
        &self,
        request: &InferenceRequest,
        candidate: &SelectedModel,
        sequence: u8,
        fallback_triggers: &[FallbackTrigger],
        attempts: &mut Vec<Attempt>,
        contributions: &mut Vec<super::exhaustion::GateContribution>,
    ) -> StepOutcome {
        let start = Instant::now();

        tracing::debug!(
            sequence,
            adapter = %candidate.router,
            model = %candidate.model,
            "attempting candidate"
        );

        // Resolve the model to send. Inject the selected candidate's resolved
        // `api_model_id` when the caller didn't pin one, so chain/registry
        // selection actually drives the provider model — otherwise the adapter
        // falls back to its own built-in default. A caller-pinned
        // `request.model` takes precedence.
        let model = if request.model.is_some() {
            request.model.clone()
        } else {
            Some(candidate.api_model_id.clone())
        };
        let endpoint = format!("{}:{}", candidate.router, candidate.model);
        // Per-call credential override: a tenant-aware consumer resolves the
        // caller's key and injects it here, so the engine stays tenant-agnostic.
        // Preferred over the router's configured api_key/env for this dispatch.
        let cfg_override;
        let cfg = match request.credentials.get(&candidate.router) {
            Some(key) => {
                let mut c = candidate.router_config.clone();
                c.api_key = Some(key.clone());
                cfg_override = c;
                &cfg_override
            }
            None => &candidate.router_config,
        };

        // Dispatch by capability to the matching registry map + typed method.
        // `None` means no adapter is registered for this router+capability —
        // handled the same as the legacy no-adapter-registered path below.
        let outcome = self
            .dispatch_capability(request, &candidate.router, model, cfg)
            .await;

        let outcome = match outcome {
            Some(o) => o,
            None => {
                tracing::warn!(
                    sequence,
                    adapter = %candidate.router,
                    model = %candidate.model,
                    will_fall_back = true,
                    "no adapter registered for router; trying next candidate"
                );
                attempts.push(Attempt {
                    sequence,
                    adapter: candidate.router.clone(),
                    model: candidate.model.clone(),
                    api_model_id: candidate.api_model_id.clone(),
                    status: AttemptStatus::Failed,
                    duration_ms: start.elapsed().as_millis() as u64,
                    tokens: None,
                    cost: None,
                    error: Some(format!(
                        "no adapter registered for router '{}'",
                        candidate.router
                    )),
                    fallback_triggered: false,
                });
                // A missing adapter is a hard/structural failure, not a gate — so
                // it vetoes all-gated (`[A(no adapter), B(locked)]` ⇒
                // AllAttemptsFailed, not AllGated).
                contributions.push(super::exhaustion::GateContribution::HardFailure);
                return StepOutcome::FallBack;
            }
        };

        match outcome {
            Ok(mut response) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                let _ = self.record_outcome(&endpoint, &candidate.router, true, None);

                // Fill cost: the pre-call estimate from selection, and the
                // actual dollar cost from the returned token usage × the
                // model's pricing (when both are known).
                response.estimated_cost = candidate.cost_estimate.clone();
                if let (Some(pricing), Some(usage)) = (
                    candidate.model_config.pricing.as_ref(),
                    response.usage.clone(),
                ) {
                    response.actual_cost = Some(Cost::from_usage(
                        &usage,
                        pricing.input_per_1k,
                        pricing.output_per_1k,
                    ));
                }

                let cost = response.actual_cost.as_ref().map(|c| c.total_cost);
                tracing::info!(
                    sequence,
                    adapter = %candidate.router,
                    model = %candidate.model,
                    duration_ms,
                    cost = ?cost,
                    "inference succeeded"
                );

                attempts.push(Attempt {
                    sequence,
                    adapter: candidate.router.clone(),
                    model: candidate.model.clone(),
                    api_model_id: candidate.api_model_id.clone(),
                    status: AttemptStatus::Success,
                    duration_ms,
                    tokens: response.usage.clone(),
                    cost,
                    error: None,
                    fallback_triggered: false,
                });

                response.attempts = std::mem::take(attempts);
                response.model = Some(candidate.model.clone());

                // Best-effort record of the successful terminal call so
                // burn-rate/spend (and, on the AUTH track, quota) have data.
                let usage = response.usage.as_ref();
                self.record_call(InferenceCall {
                    id: Uuid::new_v4(),
                    session_id: None,
                    project_id: None,
                    capability: request.capability.clone(),
                    chain_id: request.chain.clone(),
                    adapter: candidate.router.clone(),
                    model: candidate.model.clone(),
                    api_model_id: Some(candidate.api_model_id.clone()),
                    input_tokens: usage.map(|u| u.input_tokens),
                    output_tokens: usage.map(|u| u.output_tokens),
                    cost_usd: cost.unwrap_or(0.0),
                    cost_estimated: None, // §D LN-4: embedded-plane estimate population deferred
                    duration_ms,
                    status: CallStatus::Success,
                    error_type: None,
                    fallback_sequence: sequence.saturating_sub(1),
                    recorded_at: Utc::now(),
                    subject_id: request.auth.as_ref().map(|a| a.subject_id),
                    tier: request.auth.as_ref().and_then(|a| a.tier.clone()),
                })
                .await;

                StepOutcome::Done(Box::new(response))
            }
            Err(err) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                // Capture the instant the recorder pipeline just wrote (for a
                // recoverable limit that locked this endpoint) so the exhaustion
                // aggregation can attribute a timed resume to this attempt.
                let written_until =
                    self.record_outcome(&endpoint, &candidate.router, false, Some(&err));

                // Classify drives the in-flight fallover so the walk and the next-request
                // lockout agree (design §3.1): a recoverable provider limit (429 / 403-quota)
                // falls over on THIS request; a terminal one (401 / credits) stops; a non-limit
                // error keeps the configured trigger semantics.
                let should_fallback = match crate::gates::lockout::classify(&err) {
                    Some(reason) => reason.is_recoverable(),
                    None => err.should_trigger_fallback(fallback_triggers),
                };

                tracing::warn!(
                    sequence,
                    adapter = %candidate.router,
                    model = %candidate.model,
                    duration_ms,
                    error = %err,
                    will_fall_back = should_fallback,
                    "inference attempt failed"
                );

                attempts.push(Attempt {
                    sequence,
                    adapter: candidate.router.clone(),
                    model: candidate.model.clone(),
                    api_model_id: candidate.api_model_id.clone(),
                    status: AttemptStatus::Failed,
                    duration_ms,
                    tokens: None,
                    cost: None,
                    error: Some(err.to_string()),
                    fallback_triggered: should_fallback,
                });
                contributions.push(super::exhaustion::contribution_for(&err, written_until));

                if should_fallback {
                    StepOutcome::FallBack
                } else {
                    StepOutcome::Stop
                }
            }
        }
    }
}
