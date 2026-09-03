use super::*;

impl super::Gateway {
    /// Execute an inference request as a token stream.
    ///
    /// The streaming analogue of [`Gateway::execute`]. Reuses the same
    /// selection + candidate-walk + circuit-breaker machinery, but forwards
    /// the chosen adapter's [`ChatModel::chat_stream`](crate::adapters::ChatModel::chat_stream)
    /// output as a sequence of [`StreamEvent`]s rather than assembling a
    /// single [`InferenceResponse`].
    ///
    /// # Setup errors (returned before any stream)
    /// - [`GatewayError::NotConfigured`] — the config is empty.
    /// - [`GatewayError::Unsupported`] — the capability is not a chat
    ///   capability. Only [`Capability::TextChat`] / [`Capability::TextComplete`]
    ///   stream.
    /// - [`GatewayError::NoCandidates`] — selection yielded nothing.
    ///
    /// # Fallback semantics
    /// Fallback is **pre-first-byte only**. Candidates are walked in order; a
    /// candidate whose `chat_stream` fails at setup (or whose router has no
    /// registered adapter) is skipped to the next candidate when the chain's
    /// `fallback_triggers` allow it, and a [`StreamEvent::ProviderSwitch`] is
    /// emitted ahead of the next candidate's output so the consumer observes
    /// the switch. Once a candidate begins streaming, an error mid-stream is
    /// surfaced as a terminal [`StreamEvent::Error`] and the stream stops — no
    /// mid-stream fallback, since bytes have already been sent.
    ///
    /// # Terminal events
    /// A successful candidate ends with a [`StreamEvent::Done`] carrying the
    /// resolved model, the accumulated [`TokenUsage`], and the dollar cost
    /// (`usage × pricing`, or `0.0` when the model has no pricing). If **every**
    /// candidate fails at setup, the stream emits any accrued `ProviderSwitch`
    /// history followed by a terminal [`StreamEvent::Error`] describing the
    /// exhaustion (rather than returning `Err`), so the caller still observes
    /// the fallback trail.
    #[tracing::instrument(
        skip(self, request),
        fields(capability = ?request.capability, chain = ?request.chain)
    )]
    pub async fn execute_stream(
        &self,
        request: &InferenceRequest,
    ) -> Result<std::pin::Pin<Box<dyn futures::Stream<Item = StreamEvent> + Send>>, GatewayError>
    {
        use futures::StreamExt;

        // 1. Clone config from RwLock.
        let config = self.config.read().await.clone();
        if config.routers.is_empty() && config.models.is_empty() && config.chains.is_empty() {
            return Err(GatewayError::NotConfigured);
        }

        // 2. Only chat capabilities stream.
        if !matches!(
            request.capability,
            Capability::TextChat | Capability::TextComplete
        ) {
            return Err(GatewayError::Unsupported {
                adapter: "gateway".to_string(),
                what: "streaming (chat capabilities only)".to_string(),
            });
        }

        // 3. Build SelectionCriteria from request (same as `execute`).
        let input_tokens = estimate_input_tokens(&request.payload);
        let criteria = SelectionCriteria {
            capability: request.capability.clone(),
            model: request.model.clone(),
            router: request.router.clone(),
            chain: request.chain.clone(),
            budget: request.budget,
            input_tokens: Some(input_tokens),
        };

        // 4. Select all candidates.
        let svc = ModelSelectionService::new(
            &config,
            &self.circuit_breaker,
            &self.cooldown,
            &self.model_lockout,
        );
        let result = svc.select_all(&criteria);

        // No candidates? If every skip was a gate (health-lock / cooling /
        // breaker-open / model-lockout / over-budget) this is an `AllGated` rather
        // than a bare `NoCandidates` — a durable pause when any of those gates is
        // TIMED, a human-action failure when all are terminal. Mirrors `execute`'s
        // selection-empty branch. Only an all-structural (misconfig /
        // wrong-capability) selection stays `NoCandidates`.
        if result.all_candidates.is_empty() {
            tracing::warn!("no candidates available for streaming request");
            if let Some(gated) = super::exhaustion::all_gated_error(&result.skipped, &[]) {
                return Err(gated);
            }
            return Err(GatewayError::NoCandidates {
                capability: request.capability.clone(),
            });
        }

        // Quota pre-flight (AUTH) — a setup error returned before any stream.
        self.check_quota(&config, request, input_tokens).await?;

        // Owned state moved into the stream (it must be `'static`).
        // Move the selection-skips out for the `'static` closure — consumed only
        // at stream-exhaustion to decide `AllGated` vs a plain terminal error
        // (distinct field from `all_candidates`, so this partial move is fine).
        let skipped_owned: Vec<crate::selection::SkippedCandidate> = result.skipped;
        // Per-attempt gate contributions, aggregated at exhaustion into `AllGated`
        // (a recoverable limit that just locked its endpoint contributes a timed
        // resume instant; a terminal one a human-action; a hard fault vetoes
        // all-gated). Mirrors `execute`'s `contributions`; lives across loop
        // iterations, so it is declared here and moved into the stream closure.
        let mut contributions: Vec<super::exhaustion::GateContribution> = Vec::new();
        // Fallback disabled ⇒ keep only the primary candidate, so `has_more`
        // is always false downstream and no ProviderSwitch/step-down can fire.
        let mut candidates = result.all_candidates;
        if !request.allow_fallback {
            candidates.truncate(1);
        }
        let fallback_triggers = result
            .chain
            .as_ref()
            .map(|c| c.fallback_triggers.clone())
            .unwrap_or_default();
        let adapters = self.adapters.clone();
        let recorders = self.recorders.clone();
        let store = self.store.clone();
        let request = request.clone();
        let pinned_model = request.model.clone();

        let stream = async_stream::stream! {
            // `ProviderSwitch` events accrued from pre-first-byte fallbacks,
            // flushed ahead of the successful candidate's chunks (or ahead of
            // a terminal `Error` when every candidate fails at setup).
            let mut pending_switches: Vec<StreamEvent> = Vec::new();
            let total = candidates.len();

            for (idx, candidate) in candidates.iter().enumerate() {
                let has_more = idx + 1 < total;
                let endpoint = format!("{}:{}", candidate.router, candidate.model);

                // Resolve the outbound model exactly like `execute`:
                // caller-pinned wins, else the candidate's resolved api_model_id.
                let model = if pinned_model.is_some() {
                    pinned_model.clone()
                } else {
                    Some(candidate.api_model_id.clone())
                };

                // Attempt to obtain a stream for this candidate. Two pre-first-byte
                // failure modes: no adapter registered (always skip to next), or a
                // `chat_stream`/build error (skip only when triggers allow).
                let mut got_stream = None;
                let mut fail_code = String::new();
                let mut fail_message = String::new();
                let mut fail_should_fallback = false;
                // The real setup `GatewayError`, retained so it reaches the
                // recorder sinks (cooldown / lockout) and drives exhaustion
                // aggregation. `None` for the no-adapter arm (not a provider
                // fault) — previously the error's string projections were kept
                // but the error itself was dropped, so a stream-setup failure
                // never cooled/locked its router/endpoint.
                let mut fail_error: Option<GatewayError> = None;

                match adapters.chat(&candidate.router).await {
                    None => {
                        fail_code = "no_adapter".to_string();
                        fail_message =
                            format!("no adapter registered for router '{}'", candidate.router);
                        // A missing adapter is not a provider fault; skip to the
                        // next candidate unconditionally (mirrors `execute`).
                        fail_should_fallback = true;
                    }
                    Some(m) => match crate::dispatch::to_chat_request(&request, model) {
                        Err(e) => {
                            fail_code = stream_error_code(&e);
                            fail_message = e.to_string();
                            // Classify-first fallover, identical to `execute` (§3.1):
                            // a recoverable provider limit demotes on THIS request;
                            // a terminal one stops; a non-limit error keeps the
                            // configured trigger semantics.
                            fail_should_fallback = match crate::gates::lockout::classify(&e) {
                                Some(reason) => reason.is_recoverable(),
                                None => e.should_trigger_fallback(&fallback_triggers),
                            };
                            fail_error = Some(e);
                        }
                        Ok(chat_req) => {
                            // Per-call credential override (see `execute`): tenant-aware
                            // consumer injects the key; engine stays tenant-agnostic.
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
                            match m.chat_stream(cfg, &chat_req).await {
                                Ok(s) => got_stream = Some(s),
                                Err(e) => {
                                    fail_code = stream_error_code(&e);
                                    fail_message = e.to_string();
                                    // Classify-first fallover, identical to
                                    // `execute` (§3.1).
                                    fail_should_fallback =
                                        match crate::gates::lockout::classify(&e) {
                                            Some(reason) => reason.is_recoverable(),
                                            None => e.should_trigger_fallback(&fallback_triggers),
                                        };
                                    fail_error = Some(e);
                                }
                            }
                        }
                    },
                }

                if let Some(mut inner) = got_stream {
                    // A candidate produced a stream: commit to it.
                    let _ =
                        super::dispatch_outcome(&recorders, &endpoint, &candidate.router, true, None);
                    let stream_start = Instant::now();
                    tracing::debug!(adapter = %candidate.router, model = %candidate.model, "streaming candidate");
                    for ev in pending_switches.drain(..) {
                        yield ev;
                    }

                    // Forward chunks. Accumulate the latest usage (the terminal
                    // chunk carries it) for the final `Done` event.
                    let mut usage_acc: Option<TokenUsage> = None;
                    while let Some(item) = inner.next().await {
                        match item {
                            Ok(chunk) => {
                                if chunk.usage.is_some() {
                                    usage_acc = chunk.usage;
                                }
                                if !chunk.content.is_empty() {
                                    yield StreamEvent::Chunk { content: chunk.content };
                                }
                            }
                            Err(e) => {
                                // Mid-stream failure: bytes already sent, so no
                                // fallback — surface and stop.
                                yield StreamEvent::Error {
                                    code: stream_error_code(&e),
                                    message: e.to_string(),
                                    resume_after: None,
                                };
                                return;
                            }
                        }
                    }

                    let tokens = usage_acc.unwrap_or_default();
                    let cost = candidate
                        .model_config
                        .pricing
                        .as_ref()
                        .map(|p| {
                            Cost::from_usage(&tokens, p.input_per_1k, p.output_per_1k).total_cost
                        })
                        .unwrap_or(0.0);

                    // Build the record before `tokens` moves into the event; insert
                    // it after yielding `Done` so the terminal event isn't delayed
                    // by the store write. Best-effort: a store error never surfaces.
                    let call = store.as_ref().map(|_| InferenceCall {
                        id: Uuid::new_v4(),
                        session_id: None,
                        project_id: None,
                        capability: request.capability.clone(),
                        chain_id: request.chain.clone(),
                        adapter: candidate.router.clone(),
                        model: candidate.model.clone(),
                        api_model_id: Some(candidate.api_model_id.clone()),
                        input_tokens: Some(tokens.input_tokens),
                        output_tokens: Some(tokens.output_tokens),
                        cost_usd: cost,
                        cost_estimated: None, // §D LN-4: embedded-plane estimate population deferred
                        duration_ms: stream_start.elapsed().as_millis() as u64,
                        status: CallStatus::Success,
                        error_type: None,
                        fallback_sequence: idx as u8,
                        recorded_at: Utc::now(),
                        subject_id: request.auth.as_ref().map(|a| a.subject_id),
                        tier: request.auth.as_ref().and_then(|a| a.tier.clone()),
                    });
                    yield StreamEvent::Done {
                        model: candidate.model.clone(),
                        tokens,
                        cost,
                    };
                    if let Some(store) = &store
                        && let Some(call) = call
                        && let Err(e) = store.insert_inference_call(&call).await
                    {
                        tracing::warn!(error = %e, "failed to record streaming call (metering is best-effort)");
                    }
                    return;
                }

                // Setup failure for this candidate. Dispatch the RETAINED error
                // to every recorder so the cooldown / lockout sinks classify it —
                // a stream-setup failure now cools/locks exactly like `execute`'s
                // failure path (previously the error was dropped here and never
                // reached the sinks). Capture the instant any recorder just wrote
                // (a recoverable limit that locked this endpoint) so exhaustion
                // can attribute a timed resume to this attempt.
                match &fail_error {
                    Some(err) => {
                        let written_until = super::dispatch_outcome(
                            &recorders,
                            &endpoint,
                            &candidate.router,
                            false,
                            Some(err),
                        );
                        contributions
                            .push(super::exhaustion::contribution_for(err, written_until));
                    }
                    None => {
                        // No adapter registered → a hard/structural failure, not a
                        // gate (vetoes all-gated). Mirrors `execute`: the no-adapter
                        // path dispatches no recorder outcome.
                        contributions.push(super::exhaustion::GateContribution::HardFailure);
                    }
                }
                tracing::warn!(
                    adapter = %candidate.router,
                    model = %candidate.model,
                    error = %fail_message,
                    will_fall_back = has_more && fail_should_fallback,
                    "streaming candidate failed before first byte"
                );

                if has_more && fail_should_fallback {
                    let next = &candidates[idx + 1];
                    pending_switches.push(StreamEvent::ProviderSwitch {
                        from_adapter: candidate.router.clone(),
                        from_model: candidate.model.clone(),
                        to_adapter: next.router.clone(),
                        to_model: next.model.clone(),
                        reason: fail_message.clone(),
                    });
                    continue;
                }

                // No further candidates to try (or a non-fallback stop): flush
                // the switch history, then a terminal Error.
                for ev in pending_switches.drain(..) {
                    yield ev;
                }
                // Guard (mirrors `execute`): only aggregate to `AllGated` when the
                // walk reached the LAST candidate (`!has_more`) — every candidate
                // was attempted-and-gated. A non-fallback stop that left untried,
                // still-eligible candidates (`has_more`) is NOT all-gated, so it
                // stays a plain terminal error.
                if !has_more {
                    match super::exhaustion::all_gated_error(&skipped_owned, &contributions) {
                        Some(GatewayError::AllGated { resume_after, .. }) => {
                            yield StreamEvent::Error {
                                code: "all_gated".to_string(),
                                message: fail_message,
                                resume_after,
                            };
                        }
                        _ => {
                            yield StreamEvent::Error {
                                code: fail_code,
                                message: fail_message,
                                resume_after: None,
                            };
                        }
                    }
                } else {
                    yield StreamEvent::Error {
                        code: fail_code,
                        message: fail_message,
                        resume_after: None,
                    };
                }
                return;
            }
        };

        Ok(Box::pin(stream))
    }
}
