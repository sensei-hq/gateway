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

        // 3. Build SelectionCriteria from request (same as `execute`, including BOTH
        // size estimates — the cost one for the `BudgetGate` and the pessimistic one for
        // the `ContextWindowGate`). Streaming is where an unfit candidate is worst: the
        // provider's 400 arrives after the caller has already committed to a stream,
        // whereas `execute` can still return an error. Parity pinned by
        // `tests::execute_stream_gates_on_the_context_window_like_execute`.
        let input_tokens = estimate_input_tokens(&request.payload);
        let criteria = SelectionCriteria {
            capability: request.capability.clone(),
            model: request.model.clone(),
            router: request.router.clone(),
            chain: request.chain.clone(),
            budget: request.budget,
            input_tokens: Some(input_tokens),
            input_tokens_pessimistic: Some(estimate_input_tokens_pessimistic(&request.payload)),
            // The streaming half of the same wiring as `execute.rs` — a caller's
            // preferences must not depend on which entry point they came in
            // through.
            preferences: request.routing.clone(),
        };

        // 4. Select all candidates.
        let svc = self.selection_service(&config);
        let result = svc.select_all(&criteria);

        // No candidates? If every skip was a gate (health-lock / cooling /
        // breaker-open / model-lockout / over-budget / over-context-window) this is an
        // `AllGated` rather than a bare `NoCandidates` — a durable pause when any of
        // those gates is TIMED, a human-action failure when all are terminal. Mirrors
        // `execute`'s selection-empty branch. Only an all-structural (misconfig /
        // wrong-capability) selection stays `NoCandidates`.
        if result.all_candidates.is_empty() {
            tracing::warn!("no candidates available for streaming request");
            if let Some(gated) = super::exhaustion::all_gated_error(&result.skipped, &[]) {
                return Err(gated);
            }
            // Same diagnostics as `execute`'s selection-empty branch — a
            // caller's routing filter must be as visible on the streaming
            // entry point as on the unary one.
            return Err(GatewayError::NoCandidates {
                capability: request.capability.clone(),
                skipped: super::exhaustion::render_skipped(&result.skipped),
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
        // The explanation for the order the walk below is about to follow,
        // moved out for the `'static` generator like the pieces above it and
        // attached to the terminal `Done`. `execute` puts this very value on
        // `InferenceResponse::routing`; a streaming caller had nowhere to read
        // it from, so a streamed request routed correctly and could not say
        // why.
        //
        // MOVED, never rebuilt at the attachment site. The generator has
        // `candidates` in scope and could reconstruct something that looks
        // right — SP-ROUTE-1 Task 11 shipped exactly that on the unary path and
        // it survived the whole suite — but `reliability` and `weight` are read
        // from a LIVE performance window inside the strategy, and
        // `strategy::order_group` says outright that a second read is not
        // guaranteed to return what the first one did. A rebuilt decision is a
        // plausible story about the routing rather than a record of it. Pinned
        // by `tests::the_streamed_decision_matches_what_execute_reports`.
        let decision = result.decision;
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
                let endpoint = candidate.endpoint_key();
                // Wall time for THIS candidate's setup/acquisition attempt — not
                // the time to stream to completion (Task 5's job). No tokens
                // exist yet at either dispatch point below, pre- or post-first-byte.
                let attempt_start = Instant::now();

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
                    // A candidate produced a stream: commit to it. Phase
                    // `StreamAcquired` — this is a latency-only observation
                    // (time-to-first-response), NOT the verdict: the completion
                    // outcome for this same attempt always follows, and Task 5
                    // is what dispatches it.
                    let _ = super::dispatch_outcome(
                        &recorders,
                        &crate::gates::AttemptOutcome {
                            endpoint: &endpoint,
                            router: &candidate.router,
                            success: true,
                            error: None,
                            duration_ms: attempt_start.elapsed().as_millis() as u64,
                            output_tokens: None,
                            phase: crate::gates::AttemptPhase::StreamAcquired,
                        },
                    );
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
                                // A stream that dies after first byte is a FAILURE, and
                                // the recorders have to hear about it. Before SP-ROUTE-1
                                // this path returned silently while the acquisition
                                // dispatch had already fired, so an endpoint failing
                                // every stream halfway looked perfectly healthy — which
                                // the default strategy's reliability multiplier would
                                // then have weighted traffic toward.
                                //
                                // The returned deadline is discarded: the caller has
                                // already committed to this stream and there is no
                                // fallback left to schedule. Surfacing it on the yielded
                                // `StreamEvent::Error` would change that event's payload
                                // and is deliberately out of scope (spec §12).
                                let _ = super::dispatch_outcome(
                                    &recorders,
                                    &crate::gates::AttemptOutcome {
                                        endpoint: &endpoint,
                                        router: &candidate.router,
                                        success: false,
                                        error: Some(&e),
                                        // GENERATION time, not the total attempt span the
                                        // completion dispatch below reports — and this is
                                        // the one place the two differ. It is inert either
                                        // way: `output_tokens: None` means it reaches no
                                        // throughput mean, and `StreamCompleted` means it
                                        // reaches no latency mean. It is carried for
                                        // tracing and for the recorders that only classify
                                        // `error`. Anything that starts deriving a RATE
                                        // from this dispatch must switch it to
                                        // `attempt_start` first, or it reintroduces
                                        // exactly the unlike-spans pooling the completion
                                        // dispatch was fixed for.
                                        duration_ms: stream_start.elapsed().as_millis() as u64,
                                        // Deliberately `None`, even if an earlier chunk
                                        // carried `usage` (some providers report tokens on
                                        // their last good chunk before dying). The attempt
                                        // FAILED — a rate derived from a broken, partial
                                        // attempt is not a throughput observation, and
                                        // contributing one would let a later ranking
                                        // average a dead attempt's output into a live rate
                                        // (SP-ROUTE-1 Task 5 review, Minor 3).
                                        output_tokens: None,
                                        // The stream ENDED, badly. This is the attempt's
                                        // one and only verdict — the acquisition dispatch
                                        // deliberately cast none — and the phase keeps its
                                        // duration out of the latency mean.
                                        phase: crate::gates::AttemptPhase::StreamCompleted,
                                    },
                                );
                                // METER THE FAILURE. `execute` writes a
                                // `CallStatus::Failed` row for its analogous
                                // exhaustion ("observability + request
                                // counting"); this arm wrote nothing at all, so
                                // a stream that died after generating real
                                // tokens was recorded NOWHERE — unary 1 row,
                                // streamed 0, same adapter. Task 1 fixed only
                                // the success path; this is the same defect on
                                // the failure path (whole-slice review C1).
                                //
                                // Before the `yield`, for the same
                                // stops-polling reason as the success path, and
                                // under the same bounded budget.
                                //
                                // `usage_acc` IS carried here, unlike the
                                // recorder dispatch above which deliberately
                                // sends `output_tokens: None`. The two answer
                                // different questions: a rate derived from a
                                // broken attempt is not a throughput
                                // observation, but the tokens a provider
                                // reported on its last good chunk are tokens it
                                // BILLS. This row is accounting.
                                let failed_call = store.as_ref().map(|_| InferenceCall {
                                    id: Uuid::new_v4(),
                                    session_id: None,
                                    project_id: None,
                                    capability: request.capability.clone(),
                                    chain_id: request.chain.clone(),
                                    adapter: candidate.router.clone(),
                                    model: candidate.model.clone(),
                                    api_model_id: Some(candidate.api_model_id.clone()),
                                    input_tokens: usage_acc.as_ref().map(|u| u.input_tokens),
                                    output_tokens: usage_acc.as_ref().map(|u| u.output_tokens),
                                    // Costed from the usage that WAS reported,
                                    // where `execute` writes 0.0 — not a
                                    // divergence in policy but in information:
                                    // `execute` has no partial-success concept
                                    // and never holds usage for a failed call.
                                    // Discarding it here would knowingly
                                    // under-count spend on tokens we know were
                                    // generated.
                                    cost_usd: usage_acc
                                        .as_ref()
                                        .zip(candidate.model_config.pricing.as_ref())
                                        .map(|(u, p)| {
                                            Cost::from_usage(u, p.input_per_1k, p.output_per_1k)
                                                .total_cost
                                        })
                                        .unwrap_or(0.0),
                                    cost_estimated: None,
                                    // The TOTAL attempt span, as AC3 requires of
                                    // every persisted row on both paths. (The
                                    // dispatch above uses `stream_start` for a
                                    // documented reason that applies to health
                                    // observations, not to this column.)
                                    duration_ms: attempt_start.elapsed().as_millis() as u64,
                                    status: CallStatus::Failed,
                                    error_type: Some(e.to_string()),
                                    fallback_sequence: idx as u8,
                                    recorded_at: Utc::now(),
                                    subject_id: request.auth.as_ref().map(|a| a.subject_id),
                                    tier: request.auth.as_ref().and_then(|a| a.tier.clone()),
                                });
                                record_call_bounded(&store, failed_call).await;
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

                    // Throughput is only knowable here. The acquisition dispatch at the
                    // top of this block recorded LATENCY (time until the stream started
                    // producing); this second dispatch records the throughput rate and
                    // this attempt's one verdict, and `AttemptPhase` is what keeps its
                    // duration out of the latency mean.
                    //
                    // `attempt_start`, NOT `stream_start`: `PerformanceRecorder` derives
                    // `tokens_per_sec` from `duration_ms` regardless of phase, and
                    // `execute` writes the SAME `"{router}:{model}"` key with the whole
                    // call's wall time. Reporting generation time alone here pooled two
                    // unlike quantities into one `mean_tokens_per_sec` — the very thing
                    // spec §6.3 forbids for latency — so an identical real rate read
                    // higher purely because it was served streaming, and an endpoint
                    // that was faster END TO END could lose to a slower one. Output
                    // tokens ÷ total attempt wall time is one well-defined quantity on
                    // both paths, and it includes queueing, which is what a caller
                    // choosing a provider actually pays. (The alternative — making
                    // `Complete` contribute no throughput, mirroring the latency rule —
                    // was rejected: it leaves `sort: throughput` measuring nothing for a
                    // non-streaming fleet, degrading it to priority order.)
                    //
                    // MUST run before `yield StreamEvent::Done` below, not after: in an
                    // `async_stream` generator, code placed after a `yield` runs only on
                    // the NEXT poll, and a real consumer that stops polling once it sees
                    // the terminal `Done` (as any sane one does) would never resume this
                    // generator far enough to run a dispatch placed after it — the
                    // verdict would silently vanish (SP-ROUTE-1 Task 5 review, Minor 2).
                    let _ = super::dispatch_outcome(
                        &recorders,
                        &crate::gates::AttemptOutcome {
                            endpoint: &endpoint,
                            router: &candidate.router,
                            success: true,
                            error: None,
                            duration_ms: attempt_start.elapsed().as_millis() as u64,
                            output_tokens: Some(tokens.output_tokens),
                            phase: crate::gates::AttemptPhase::StreamCompleted,
                        },
                    );

                    let cost = candidate
                        .model_config
                        .pricing
                        .as_ref()
                        .map(|p| {
                            Cost::from_usage(&tokens, p.input_per_1k, p.output_per_1k).total_cost
                        })
                        .unwrap_or(0.0);

                    // Built before `tokens` moves into the event below.
                    // Best-effort: a store error never surfaces.
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
                        // `attempt_start`, NOT `stream_start` — the same parity
                        // the completion dispatch above already restored, on the
                        // PERSISTED column. `execute` writes this very column
                        // with the whole attempt's wall time, so reporting
                        // generation time alone here left `inference_calls`
                        // mixing two quantities under one name, with nothing to
                        // tell an analytics query which one a row carries.
                        duration_ms: attempt_start.elapsed().as_millis() as u64,
                        status: CallStatus::Success,
                        error_type: None,
                        fallback_sequence: idx as u8,
                        recorded_at: Utc::now(),
                        subject_id: request.auth.as_ref().map(|a| a.subject_id),
                        tier: request.auth.as_ref().and_then(|a| a.tier.clone()),
                    });
                    // Before the `yield`, for the reason spelled out above the
                    // completion dispatch: a consumer that stops polling at the
                    // terminal `Done` never resumes this generator, so anything
                    // placed after the yield silently never runs. That cost the
                    // verdict once (SP-ROUTE-1 Task 5 review, Minor 2); here it
                    // cost the BILLING ROW — a streamed request was metered only
                    // when its caller happened to over-poll, while a
                    // non-streaming one always is.
                    //
                    // BOUNDED, and the dispatch above is NOT the precedent for
                    // paying this price unbounded (the whole-slice review's C2
                    // corrected that claim). `dispatch_outcome` is a synchronous
                    // fold over in-memory recorders — it awaits nothing and
                    // charges no I/O. This is an `async` call into a store the
                    // CONSUMER supplies, so putting it ahead of the yield makes
                    // the terminal event wait on somebody else's database. A
                    // stalled pool would then delay `Done` indefinitely, and a
                    // consumer with a per-event timeout would get the full
                    // content and no `Done` at all — losing the tokens, the cost
                    // and the routing decision, and cancelling the in-flight
                    // write so no row lands either. "Delivered but unbilled"
                    // must not become "not delivered and unbilled".
                    record_call_bounded(&store, call).await;
                    yield StreamEvent::Done {
                        model: candidate.model.clone(),
                        tokens,
                        cost,
                        routing: decision,
                    };
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
                        // Phase `Complete`: a setup failure is final — no
                        // completion dispatch follows it, streaming or not.
                        let written_until = super::dispatch_outcome(
                            &recorders,
                            &crate::gates::AttemptOutcome {
                                endpoint: &endpoint,
                                router: &candidate.router,
                                success: false,
                                error: Some(err),
                                duration_ms: attempt_start.elapsed().as_millis() as u64,
                                output_tokens: None,
                                phase: crate::gates::AttemptPhase::Complete,
                            },
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

                // METER THE EXHAUSTION — the second unmetered failure (C1).
                // `execute`'s exhaustion branch writes a `CallStatus::Failed`
                // row attributed to the LAST attempted candidate; this path
                // wrote none, so an all-candidates-failed stream produced no
                // record of the request at all. Built here, before the yields
                // below consume `fail_message`, and recorded ahead of the
                // terminal event for the same stops-polling reason as the
                // success path.
                //
                // Field-for-field parity with `execute`'s failed row, including
                // `tokens: None` / `cost: None`: nothing was generated and the
                // provider reported no usage, so "unknown" is the honest value.
                // `Some(0)` would claim knowledge a setup failure does not give
                // us — the request may have been processed and billed before
                // the stream could be established.
                let exhausted_call = store.as_ref().map(|_| InferenceCall {
                    id: Uuid::new_v4(),
                    session_id: None,
                    project_id: None,
                    capability: request.capability.clone(),
                    chain_id: request.chain.clone(),
                    adapter: candidate.router.clone(),
                    model: candidate.model.clone(),
                    api_model_id: Some(candidate.api_model_id.clone()),
                    input_tokens: None,
                    output_tokens: None,
                    cost_usd: 0.0,
                    cost_estimated: None,
                    duration_ms: attempt_start.elapsed().as_millis() as u64,
                    status: CallStatus::Failed,
                    error_type: Some(fail_message.clone()),
                    fallback_sequence: idx as u8,
                    recorded_at: Utc::now(),
                    subject_id: request.auth.as_ref().map(|a| a.subject_id),
                    tier: request.auth.as_ref().and_then(|a| a.tier.clone()),
                });
                record_call_bounded(&store, exhausted_call).await;

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

/// Wall-clock ceiling on a single streaming metering write.
///
/// Generous for a healthy store and short enough that a sick one cannot hold a
/// stream open. There is no configuration knob on purpose: this is not a tuning
/// parameter but a liveness guarantee, and a deployment that could raise it
/// would reintroduce exactly the head-of-line block it exists to prevent.
const METERING_WRITE_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);

/// Persist one metering row, bounded by [`METERING_WRITE_BUDGET`].
///
/// Metering on the streaming path is best-effort, and after the whole-slice
/// review that is true of **latency** as well as of success. The row is written
/// ahead of the terminal event so a consumer that stops polling at `Done` is
/// still billed (AC1); the budget is what stops that ordering from making the
/// terminal event hostage to a consumer-supplied store. Exceeding it drops the
/// row and logs loudly — losing a billing row is bad, losing the caller's
/// tokens, cost and routing decision is worse, and a stream that never
/// terminates is worst.
///
/// `None` store or `None` call is a no-op, which keeps every store-less
/// configuration byte-identical.
async fn record_call_bounded(store: &Option<Arc<dyn GatewayStore>>, call: Option<InferenceCall>) {
    let (Some(store), Some(call)) = (store, call) else {
        return;
    };
    match tokio::time::timeout(METERING_WRITE_BUDGET, store.insert_inference_call(&call)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "failed to record streaming call (metering is best-effort)")
        }
        Err(_) => tracing::warn!(
            budget = ?METERING_WRITE_BUDGET,
            "metering write exceeded its budget; dropping the row to release the terminal event"
        ),
    }
}
