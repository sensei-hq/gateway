use super::*;
use crate::selection::{SelectedModel, SelectionResult};
use crate::types::config::FallbackTrigger;
use crate::types::cost::{Cost, CostEstimate};
use crate::types::trace::{
    Attempt, CandidateInfo, ExecutionTrace, RoutingDecision, SkippedInfo, TraceStatus,
};

/// Outcome of one per-attempt step in the fallback-chain walk — the seam a
/// later plan extends. Lets the top-level `execute` loop read as: for each
/// candidate, run [`Gateway::attempt_candidate`] and act on the outcome, i.e.
/// return the response, continue to the next candidate, or break to exhaustion
/// handling.
pub(super) enum StepOutcome {
    /// The candidate produced a terminal response, together with the ledger row it
    /// recorded. `execute` returns the response — filling in the routing decision
    /// and writing the row's trace, neither of which this step can know — and the
    /// row travels WITH the outcome rather than through a side channel, so the two
    /// cannot disagree about which call is being described.
    /// Boxed to keep the enum small (the other variants carry no data).
    Done(Box<(InferenceResponse, InferenceCall)>),
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

        // A structurally malformed decision call fails identically on every
        // candidate — reject it once, here, rather than walk the chain (gh#72 D2).
        if let crate::types::request::Payload::Decision {
            state, questions, ..
        } = &request.payload
        {
            crate::types::decision::validate_decision(state, questions)
                .map_err(|message| GatewayError::InvalidRequest { message })?;
        }

        // One ordering per call. Refused here, before any candidate is gated or
        // tried, so the caller hears about its own contradiction instead of
        // getting an order it cannot account for (see `engine::util::validate_routing`).
        super::util::validate_routing(request.routing.as_ref())?;

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
            // One of the TWO sites (with `stream.rs`) where a caller's routing
            // preferences enter selection. `None` here made the whole of
            // SP-ROUTE-1 inert with a green suite — `only`/`ignore` never
            // reached the `RoutingPolicyGate` and no `sort` selected anything.
            // Pinned by `tests::a_requests_routing_preferences_reach_selection`.
            preferences: request.routing.clone(),
        };

        // 3. Select all candidates
        let svc = self.selection_service(&config);
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
            // Carry the per-candidate reasons rather than discarding them. An
            // all-structural selection is exactly where the caller has no other
            // channel: `AllGated` at least names a deadline or a remedy, while a
            // bare `NoCandidates` used to name only the capability — so a typo'd
            // `only: { routers: [...] }` was undiagnosable.
            return Err(GatewayError::NoCandidates {
                capability: request.capability.clone(),
                skipped: super::exhaustion::render_skipped(&result.skipped),
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
                StepOutcome::Done(boxed) => {
                    let (mut response, call) = *boxed;
                    // AC10 — carry the routing decision out to the caller.
                    // `attempt_candidate` sees one candidate and cannot know why
                    // that candidate came first, so the attachment belongs here,
                    // where `result` is still in scope. Without this line the
                    // whole of Task 11 is a field populated in `SelectionResult`
                    // and read by nobody. Pinned by
                    // `tests::the_routing_decision_reaches_the_inference_response`.
                    response.routing = result.decision.clone();
                    // The trace, written here for the same reason the decision is
                    // attached here: it needs that decision AND the row the walk just
                    // recorded, and this is the only place both exist. Pinned by
                    // `decision_tests::a_recorded_trace_carries_the_ranking_revision_and_the_filtered_models`.
                    self.record_trace(build_trace(
                        &call,
                        &result,
                        TraceOutcome {
                            status: if response.success {
                                TraceStatus::Success
                            } else {
                                TraceStatus::Failed
                            },
                            attempts: &response.attempts,
                            estimated_cost: response.estimated_cost.clone(),
                            actual_cost: response.actual_cost.clone(),
                            routing: response.routing.clone(),
                        },
                    ))
                    .await;
                    return Ok(response);
                }
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
        // counting), attributed to the last attempted candidate — and its trace,
        // because a call that never reached a candidate is the one whose
        // explanation is hardest to reconstruct afterwards: the ledger row names
        // only the last error, `order` is what it WOULD have tried, and the
        // withheld list is the only record of what the gates refused. Both lists
        // are the walk's own outcome, not a response's — an exhausted walk has no
        // response to read them from (see [`TraceOutcome`]).
        if let Some(last) = attempts.last() {
            let call = InferenceCall {
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
                run_id: None,
            };
            self.record_call(&call).await;
            self.record_trace(build_trace(
                &call,
                &result,
                TraceOutcome {
                    status: TraceStatus::Failed,
                    attempts: &attempts,
                    // Nothing was served, so there is no cost to report: the walk's
                    // per-attempt costs stay on `attempts`, where a failed attempt's
                    // own `cost` is `None` too. A zero here would read as "free".
                    estimated_cost: None,
                    actual_cost: None,
                    routing: result.decision.clone(),
                },
            ))
            .await;
        }

        // SP-OPS-1.3: carry the retryability the walk already classified, instead of
        // discarding it. `HardFailure` is the non-limit class (5xx / network /
        // unclassified) — the one that may clear on its own. Without any, exhaustion was
        // reached the OTHER way: a terminal limit `Stop`ped the walk early (so
        // `attempted_all` is false and `gated` is None even though nothing hard-failed),
        // and waiting cannot fix that. Computed from `contributions` rather than sniffed
        // from `errors`, so a provider rewording a message cannot change the verdict.
        // `InputRejected` contributions are not `HardFailure` (SP-DEC-3 D5): an
        // exhaustion made only of them is `retryable: false`.
        let retryable = contributions
            .iter()
            .any(|c| matches!(c, super::exhaustion::GateContribution::HardFailure));

        Err(gated.unwrap_or_else(|| GatewayError::AllAttemptsFailed {
            attempts: attempts.len(),
            errors,
            attempts_detail: attempts,
            retryable,
        }))
    }

    /// Run one per-attempt step of the fallback-chain walk against a single
    /// resolved `candidate`: dispatch the request, then either return the
    /// successful response (filling cost and recording the terminal call) or
    /// record a failed [`Attempt`] and signal whether `execute` should fall back
    /// to the next candidate or stop. Pushes each attempt onto `attempts` in the
    /// same order/content the inline loop did. The per-attempt seam a later plan
    /// extends.
    ///
    /// `StepOutcome::Done` carries the row a successful attempt persisted, because
    /// `execute` — not this function — writes the call's trace: the trace needs the
    /// routing decision, which does not exist until the walk is over (see
    /// [`build_trace`]).
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
        let endpoint = candidate.endpoint_key();
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
                let output_tokens = response.usage.as_ref().map(|u| u.output_tokens);
                let _ = self.record_outcome(&crate::gates::AttemptOutcome {
                    endpoint: &endpoint,
                    router: &candidate.router,
                    success: true,
                    error: None,
                    duration_ms,
                    output_tokens,
                    phase: crate::gates::AttemptPhase::Complete,
                });

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
                let call = InferenceCall {
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
                    run_id: None,
                };
                self.record_call(&call).await;

                // The row rides the outcome so `execute` can link this call's trace
                // to it — see the variant's own note.
                StepOutcome::Done(Box::new((response, call)))
            }
            Err(err) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                // Capture the instant the recorder pipeline just wrote (for a
                // recoverable limit that locked this endpoint) so the exhaustion
                // aggregation can attribute a timed resume to this attempt.
                // An `InputRejected` reaches no recorder and writes nothing
                // (`dispatch_outcome`, SP-DEC-3 D5); `classify` is `None` for it, so
                // `should_trigger_fallback` (always `true` for it) decides the walk.
                let written_until = self.record_outcome(&crate::gates::AttemptOutcome {
                    endpoint: &endpoint,
                    router: &candidate.router,
                    success: false,
                    error: Some(&err),
                    duration_ms,
                    output_tokens: None,
                    phase: crate::gates::AttemptPhase::Complete,
                });

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

/// What a call's own record contributes to its trace, beyond the selection detail
/// [`build_trace`] reads from `result`.
///
/// Two callers, and they cannot share one value: a served call has an
/// [`InferenceResponse`] (its attempts, its costs, the routing decision attached to
/// it), while an exhausted walk has attempts and the order it was handed but **no
/// response at all**. Reusing one builder over a fabricated response would put a
/// zero next to `TraceStatus::Failed` — "this failed call cost $0.00" — and invite a
/// reader to trust the rest of its fields.
struct TraceOutcome<'a> {
    /// Whether a candidate answered. The trace's own statement, and the reader's
    /// first question: a failed trace is the only one whose `attempts` are all
    /// failures.
    status: TraceStatus,
    /// Every step the walk took, in order — the record "what did it try" as
    /// opposed to `routing.order`'s "what would it try".
    attempts: &'a [Attempt],
    /// The pre-call estimate selection priced, when a response carried one.
    estimated_cost: Option<CostEstimate>,
    /// What the provider actually charged. `None` for an exhausted walk: nothing
    /// was served, so there is no cost — not a zero cost.
    actual_cost: Option<Cost>,
    /// The order selection handed the walk. `None` when no strategy ran (a direct
    /// router+model call orders nothing); kept `None` rather than invented, because
    /// a `Some` here claims a strategy made a decision about this call.
    routing: Option<RoutingDecision>,
}

/// Snapshot one recorded call as an [`ExecutionTrace`], linked to the ledger row
/// `call` names.
///
/// **The production writer `ExecutionTrace` did not have.** Every field of that
/// struct was previously unfilled because nothing in the workspace built one
/// outside a test helper; SP-ROUTE-1 shipped `ExecutionTrace::routing` as forward
/// provision and said so. The engine is the only place that can fill it: the
/// `candidates` and `skipped` lists are selection's own detail (a store decorator
/// and an API route both never see them), and `routing` is the decision `execute`
/// attaches to the response immediately before calling this — which is what makes
/// the ranking's CATEGORY and REVISION durable per call rather than only in a boot
/// log.
///
/// Called for BOTH outcomes a walk can reach: a served candidate, and an exhausted
/// chain. A failed call is not a lesser trace — it is the one where the order and
/// the attempts are all a reader has, since there is no winner to ask about.
///
/// `request_id` is the call's own id, matching the API's
/// `routes::chat::build_trace`: this describes deliberately the same call the
/// sibling ledger row records, not a second event. The two are linked explicitly
/// by `inference_call_id` for the readers that walk from a call to its trace.
fn build_trace(
    call: &InferenceCall,
    result: &SelectionResult,
    outcome: TraceOutcome<'_>,
) -> StoredTrace {
    let created_at = call.recorded_at;
    let trace = ExecutionTrace {
        request_id: call.id.to_string(),
        capability: call.capability.clone(),
        status: outcome.status,
        // The call's own figure rather than a second measurement, so the trace and
        // the row it describes cannot disagree about how long the call took.
        duration_ms: call.duration_ms,
        candidates: result
            .all_candidates
            .iter()
            .map(|c| CandidateInfo {
                model: c.model.clone(),
                router: c.router.clone(),
                priority: c.priority,
            })
            .collect(),
        skipped: result
            .skipped
            .iter()
            .map(|s| SkippedInfo {
                model: s.model.clone(),
                router: s.router.clone(),
                reason: s.reason.to_string(),
            })
            .collect(),
        attempts: outcome.attempts.to_vec(),
        estimated_cost: outcome.estimated_cost,
        actual_cost: outcome.actual_cost,
        // PROPAGATE, never default: this is the field's whole reason to exist.
        routing: outcome.routing,
        created_at,
    };
    StoredTrace {
        id: Uuid::new_v4(),
        inference_call_id: Some(call.id),
        trace,
        created_at,
    }
}
