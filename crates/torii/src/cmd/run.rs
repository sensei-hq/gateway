//! Observe and intervene on runs. Every command reports the EFFECT it achieved,
//! never the fact that the store call returned Ok — `cancel` on a terminal run and
//! `wake` on a non-paused run are both silent no-ops at the store level.

use crate::cmd::Outcome;
use crate::errors::CliError;
use crate::render;
use chrono::{DateTime, Utc};
use orchestrator_core::{
    ExecutionJournal, JournalEvent, NodeId, OrchestratorError, RunId, RunStatus, SchedulerStore,
    Scope, Seq, TokenBudget,
};
use std::collections::HashMap;

pub async fn status(
    store: &dyn SchedulerStore,
    journal: &dyn ExecutionJournal,
    run: RunId,
    json: bool,
) -> Result<Outcome, CliError> {
    match store.status(run).await? {
        // WHOLE-SLICE FIX 5: `--json` promises machine-parseable STDOUT, and the not-found
        // path was emitting prose there — so `torii run status X --json | jq` failed on
        // exactly the case a script is most likely to hit. `null` is the honest JSON answer
        // (the run does not exist); the exit code, not the payload, still carries "2".
        None => Ok(Outcome::precondition(if json {
            "null".to_string()
        } else {
            format!("no such run: {}", run.0)
        })),
        Some(r) => {
            // SP-DATA-5 Task 5: spend lives in the JOURNAL (`EffectRecorded.usage`), not
            // the `scheduled_runs` row, so it has to be folded here. Goes through
            // `orchestrator::spend_of` — the SAME fold the metered-dispatch gate itself
            // uses — rather than summing `EffectRecorded.usage` locally: a second,
            // torii-side sum would inevitably drift from the real one (e.g. missing the
            // effect-id keying that makes a duplicate record from the two-phase
            // Mutation in-doubt-`Confirmed` path count once, not twice) and stay wrong
            // silently, compounding on every resume.
            let events = journal
                .load(run)
                .await
                .map_err(OrchestratorError::Journal)?;
            let (spent, budget) = orchestrator::spend_of(&events);

            if json {
                let base = render::json(&[r]).map_err(|e| CliError::error(e.to_string()))?;
                match budget {
                    // No budget ⇒ return `render::json`'s own string UNTOUCHED — no
                    // parse/re-serialize round trip at all. That round trip is not
                    // idempotent: `serde_json::Value`'s object map does not preserve
                    // insertion order the way `ScheduledRun`'s derived `Serialize`
                    // does, so re-serializing would silently reorder every key. Only
                    // taking that detour when there is something to splice in is what
                    // keeps the unbudgeted case byte-identical.
                    None => Ok(Outcome::ok(base)),
                    Some(cap) => {
                        // Reuse `render::json` for the row shape + redaction, then
                        // splice spent/budget in — rather than hand-building the
                        // object here, which would duplicate `render::json`'s
                        // redaction of `reason`.
                        let mut rows: serde_json::Value = serde_json::from_str(&base)
                            .map_err(|e| CliError::error(e.to_string()))?;
                        rows[0]["spent"] = serde_json::json!(spent);
                        rows[0]["budget"] = serde_json::json!(cap);
                        Ok(Outcome::ok(
                            serde_json::to_string_pretty(&rows)
                                .map_err(|e| CliError::error(e.to_string()))?,
                        ))
                    }
                }
            } else {
                let mut text = render::table(&[r]);
                // Same additivity: nothing appended when unbudgeted, so the table
                // stays byte-identical to the pre-SP-DATA-5 output.
                if let Some(cap) = budget {
                    text.push_str(&format!("spent: {spent} / budget: {cap} tokens\n"));
                }
                Ok(Outcome::ok(text))
            }
        }
    }
}

/// Every run awaiting a wake, plus — SP-6 s1 — which node inside each is awaiting a
/// SIGNAL, and until when.
///
/// The scheduler row alone cannot answer that: `RunPaused` is not node-keyed and a run
/// pauses for many unrelated reasons over its life, which is exactly why `SignalAwaited`
/// exists as its own node-keyed event. So this loads each paused run's journal and folds
/// it. That is one extra round trip per PAUSED run (never per run) on an operator-invoked
/// command — acceptable at control-plane scale, and the alternative (denormalizing the
/// awaiting node onto `scheduled_runs`) would put a second, drift-prone copy of the
/// journal's truth in the schema.
///
/// **Additive:** when no paused run has an awaiting node, the output is BYTE-IDENTICAL to
/// the pre-SP-6 render — the table gets no extra block and the JSON no extra key (and,
/// as in `status`, the JSON path returns `render::json`'s own string untouched rather
/// than taking a non-idempotent parse/re-serialize detour that would reorder keys).
///
/// **A journal fault is scoped to the RUN it belongs to (whole-slice review, Important).**
/// The per-run `journal.load()` above is new in this slice, and propagating its error
/// aborted the whole command: one run whose durable `format_version` had been bumped made
/// `list-paused` exit 1 with an EMPTY stdout, hiding every other paused run — the ones an
/// operator could still `signal`, `wake` or `cancel`, and precisely when they most need
/// the list (a fence bump is what a rolling deploy produces). Before this slice the
/// command read `scheduled_runs` alone and listed every paused run regardless of journal
/// state, so this was a regression in blast radius, not an inherited limitation.
///
/// The error is still never SWALLOWED — reporting an empty awaiting set because the load
/// failed would tell an operator there is nothing to signal, which is the most damaging
/// possible answer for a run blocked on a human. It is reported in the row it belongs to
/// (`unknown: <error>` in the table, `awaiting_error` in `--json`), and the command exits
/// [`EXIT_PRECONDITION`](crate::errors::EXIT_PRECONDITION) so a script still learns the
/// listing is incomplete.
///
/// **Why exit 2 rather than exit 1.** Exit 1 in this CLI is the [`CliError`] path, and
/// `main` prints a `CliError` to STDERR and nothing to stdout — so exiting 1 here would
/// throw away the very listing this fix exists to preserve. Exit 2 is already this
/// surface's code for "the command ran and its output is on stdout, but the outcome is not
/// the unqualified success you asked for": `run status <unknown> --json` exits 2 with a
/// parseable `null` on stdout for exactly that reason. A partially-degraded listing is the
/// same shape, so it gets the same code.
pub async fn list_paused(
    store: &dyn SchedulerStore,
    journal: &dyn ExecutionJournal,
    json: bool,
) -> Result<Outcome, CliError> {
    let rows = store.list_paused().await?;
    let mut awaiting: Vec<(RunId, render::Awaiting)> = Vec::with_capacity(rows.len());
    for r in &rows {
        // `to_string`, not the `CliError` mapping: this is a table CELL, not the process's
        // whole failure, and `JournalError`'s own `Display` already names the run, the
        // stored format and the expected one. It is rendered through the same redact +
        // collapse + cap transform a pause reason gets — see `render::awaiting_section`.
        awaiting.push(match journal.load(r.run).await {
            Ok(events) => (r.run, Ok(awaiting_nodes(&events))),
            Err(e) => (r.run, Err(e.to_string())),
        });
    }
    let degraded = awaiting.iter().any(|(_, a)| a.is_err());
    // The additive path: every journal folded, and none of them had anything awaiting.
    // A failure is never "nothing to add", so this implies `!degraded`.
    let nothing_to_add = awaiting
        .iter()
        .all(|(_, a)| matches!(a, Ok(nodes) if nodes.is_empty()));
    let finish = |text: String| {
        if degraded {
            Outcome::precondition(text)
        } else {
            Outcome::ok(text)
        }
    };

    if json {
        let base = render::json(&rows).map_err(|e| CliError::error(e.to_string()))?;
        if nothing_to_add {
            return Ok(Outcome::ok(base));
        }
        let mut v: serde_json::Value =
            serde_json::from_str(&base).map_err(|e| CliError::error(e.to_string()))?;
        for (i, (_, a)) in awaiting.iter().enumerate() {
            match a {
                Ok(nodes) if !nodes.is_empty() => {
                    v[i]["awaiting"] =
                        serde_json::to_value(nodes).map_err(|e| CliError::error(e.to_string()))?;
                }
                Ok(_) => {}
                // A SEPARATE key, never `"awaiting": []`: a script must be able to tell
                // "this run has nothing awaiting" from "this run's awaiting set is
                // unknown", and an empty array says the first while meaning the second.
                Err(e) => v[i]["awaiting_error"] = serde_json::json!(render::redact_reason(e)),
            }
        }
        return Ok(finish(
            serde_json::to_string_pretty(&v).map_err(|e| CliError::error(e.to_string()))?,
        ));
    }

    let mut text = render::table(&rows);
    // Empty when nothing is awaiting and nothing failed, so this append is a no-op on the
    // additive path.
    text.push_str(&render::awaiting_section(&awaiting));
    Ok(finish(text))
}

pub async fn cancel(store: &dyn SchedulerStore, run: RunId) -> Result<Outcome, CliError> {
    if store.status(run).await?.is_none() {
        return Ok(Outcome::precondition(format!("no such run: {}", run.0)));
    }
    store.cancel(run).await?;
    // Re-read: `cancel` is a conditional no-op on a terminal row, so only the
    // observed state proves what happened.
    // This row is never deleted by any shipped store, so `None` here would mean a
    // hypothetical future retention/purge raced us, not a reachable path today.
    let after = store
        .status(run)
        .await?
        .ok_or_else(|| CliError::error(format!("run {} vanished mid-cancel", run.0)))?;
    if after.status == RunStatus::Cancelled {
        Ok(Outcome::ok(format!("cancelled: {}", run.0)))
    } else {
        Ok(Outcome::precondition(format!(
            "not cancelled: {} is already {}",
            run.0,
            after.status.as_str()
        )))
    }
}

pub async fn wake(
    store: &dyn SchedulerStore,
    journal: &dyn ExecutionJournal,
    run: RunId,
    now: DateTime<Utc>,
    budget: Option<TokenBudget>,
) -> Result<Outcome, CliError> {
    let Some(before) = store.status(run).await? else {
        return Ok(Outcome::precondition(format!("no such run: {}", run.0)));
    };
    if before.status != RunStatus::Paused {
        return Ok(Outcome::precondition(format!(
            "not queued: {} is {}, and only a paused run can be woken",
            run.0,
            before.status.as_str()
        )));
    }
    // SP-DATA-5 Task 5: if the operator is raising the cap, this MUST append to the
    // journal BEFORE calling `force_wake` below — never the other way round. This is
    // a real race, not a style preference: `force_wake` only flips the store's
    // `next_wake` to `now`; it does not itself drive the run. But a worker's
    // `tick()` polling loop can claim the SAME due wake the instant that deadline
    // lands, in another process, before this function returns. If the wake landed
    // first, that worker could win the race, drive the run under the OLD
    // (already-exhausted) cap, and re-pause it immediately — the operator's command
    // would then appear to "succeed" while the run is right back where it started,
    // one moment later, under the very cap they just tried to raise. Appending
    // first closes that window: any worker that can observe the wake can only ever
    // fold a journal that already includes the raise.
    if let Some(b) = budget {
        journal
            .append(
                run,
                JournalEvent::BudgetRaised {
                    new_total_tokens: b.total_tokens,
                },
            )
            .await
            .map_err(OrchestratorError::Journal)?;
    }
    store.force_wake(run, now).await?;
    // This row is never deleted by any shipped store, so `None` here would mean a
    // hypothetical future retention/purge raced us, not a reachable path today.
    let after = store
        .status(run)
        .await?
        .ok_or_else(|| CliError::error(format!("run {} vanished mid-wake", run.0)))?;
    // The primary signal is STATUS, not next_wake's mere presence: `claim_due` flips
    // `paused -> waking` and leaves a stale `next_wake` untouched, and `cancel` clears
    // it to NULL — neither on its own tells us whether OUR force_wake actually applied
    // (both shipped stores make force_wake conditional on the row still being
    // `paused`). A real force_wake success leaves the row `paused` with `next_wake`
    // pinned to (within clock precision of) `now`; a lost race to a concurrent claim
    // or cancel moves the status away from `paused` instead, which the timestamp alone
    // cannot distinguish from a stale pre-existing deadline.
    //
    // The timestamp tolerance is not compensating for multi-process clock skew — `now`
    // here is the exact value this call sent to the store — it only absorbs the
    // sub-microsecond rounding a `timestamptz` column performs on write. Measured
    // empirically against a live Postgres: encoding rounds a nanosecond-precision
    // value to the nearest microsecond (round-half-to-even), a drift of at most
    // ±500ns, in EITHER direction — so a one-sided `t <= now` is not safe. 2µs is a
    // 4x margin over that measured bound, and still five orders of magnitude tighter
    // than any real re-pause deadline (seconds-to-minutes out), so it cannot be
    // satisfied by an unrelated pause that happens to land in the race window.
    let applied = after.status == RunStatus::Paused
        && after.next_wake.is_some_and(|t| {
            let drift = if t >= now { t - now } else { now - t };
            drift <= chrono::Duration::microseconds(2)
        });
    if applied {
        Ok(Outcome::ok(format!(
            "queued for wake: {} (a worker tick will drive it)",
            run.0
        )))
    } else {
        Ok(Outcome::precondition(format!(
            "not queued: {} is {} — force_wake did not apply",
            run.0,
            after.status.as_str()
        )))
    }
}

// ---- SP-6 s1: `torii run signal` ------------------------------------------------------

/// What a run's journal says about one node's `AwaitSignal` state.
///
/// Folded from the journal rather than read off the scheduler row, because the scheduler
/// row is RUN-level: it knows the run is paused, not which node is waiting or whether
/// that node has since read its answer. §6.6 requires `run signal` to report the effect
/// it achieved on the NODE, so the node's state has to come from somewhere node-keyed.
#[derive(Debug, Clone, PartialEq)]
pub enum SignalState {
    /// `SignalAwaited` is journaled and nothing has since terminated the node: a
    /// delivered signal WILL be read on the next drive.
    Awaiting {
        /// The ABSOLUTE deadline the node recorded (first record wins, exactly as the
        /// executor's fold does — a later `SignalAwaited` must never move it).
        /// `None` = the indefinite class, never auto-woken.
        deadline: Option<DateTime<Utc>>,
    },
    /// The node read its answer and completed. A further signal changes nothing it has
    /// already done — and, worse, would sit in the fold as a NEW last-wins answer for a
    /// node that could re-execute on a later resume, silently changing its output. That
    /// is why this is refused rather than delivered.
    Completed,
    /// `NodeFailed` — for an `AwaitSignal` node, its deadline fired.
    Failed,
    /// `NodeSkipped` — a hard dependency failed and cascade-skipped it.
    Skipped,
    /// No `SignalAwaited` was ever journaled for this id: a typo, a node the run has not
    /// reached, or a node that is not an `AwaitSignal` at all.
    NotAwaiting,
}

impl SignalState {
    /// The operator-facing state word, for "not delivered: `<node>` is `<state>`".
    ///
    /// A bare adjective, carrying no "already"/"still" of its own, because the callers
    /// supply their own tense — "is `<state>`" and "was already `<state>` before the write
    /// landed" cannot both be built from one word that already says "already". The
    /// pre-check's Completed refusal has its own dedicated sentence in [`not_delivered`]
    /// and does not go through here.
    pub fn as_str(&self) -> &'static str {
        match self {
            SignalState::Awaiting { .. } => "awaiting a signal",
            SignalState::Completed => "completed",
            SignalState::Failed => "failed",
            SignalState::Skipped => "skipped",
            SignalState::NotAwaiting => "not awaiting a signal",
        }
    }
}

/// One node's [`SignalState`], plus the journal [`Seq`] of the event that established it.
///
/// The seq is what makes the post-write report honest (§6.6). `signal` writes its answer
/// and then re-reads; if the node is terminal by then, "terminal" alone cannot say
/// whether the delivery was READ or ORPHANED — the two outcomes are opposite and the
/// operator needs to know which. The journal ORDER answers it: a terminal marker BEHIND
/// our appended row means the drive that terminated the node folded a journal that
/// already contained the answer; a marker AHEAD of it means the node was already dead
/// when the row landed.
#[derive(Debug, Clone, PartialEq)]
pub struct SignalStateAt {
    pub state: SignalState,
    /// `None` while the node is still awaiting (nothing has terminated it), and for a
    /// node that never awaited at all.
    pub at: Option<Seq>,
}

/// Fold every `AwaitSignal` node's state — and the seq that established it — out of a
/// run's journal in ONE pass.
///
/// A node that never journaled `SignalAwaited` is absent from the map (the caller reads
/// that as [`SignalState::NotAwaiting`]).
///
/// **How a COMPLETED `AwaitSignal` node is recognised.** It journals no `NodeCompleted`
/// (like `Branch`/`Subgraph`, per the executor's `run_await_signal`), so the durable
/// marker is the blackboard publish every completed node makes — `ContextWrite` keyed by
/// the node id (`Executor::publish_context`) — plus, as a backstop, a `RunCompleted`
/// anywhere in the journal, which means every node in the run finished. `torii`'s
/// production boot always wires a `ContextStore`, so the first marker is always present
/// in a real deployment.
///
/// **A LOOP gate is the exception to that paragraph and needs a marker of its own.** It
/// journals no `NodeCompleted` either, and — unlike an `AwaitSignal` — it publishes
/// nothing to the blackboard under its own id, so the `ContextWrite` marker never fires
/// for it. `LoopGateSettled` is its completion event; see that arm for why the DECISION is
/// deliberately not used in its place.
///
/// Deliberately conservative in ONE direction: if no marker is present the node reads as
/// still `Awaiting`. That errs toward delivering a signal that is redundant (harmless —
/// last-wins) rather than toward reporting `already completed` for a node that is
/// genuinely still waiting, which would strand a run on a human who was told their
/// decision had already landed.
fn signal_states(events: &[(Seq, JournalEvent)]) -> HashMap<NodeId, SignalStateAt> {
    let mut awaited: HashMap<NodeId, Option<DateTime<Utc>>> = HashMap::new();
    let mut terminal: HashMap<NodeId, (Seq, SignalState)> = HashMap::new();
    let mut run_completed: Option<Seq> = None;
    for (seq, e) in events {
        match e {
            // FIRST record wins — the same asymmetry the executor's fold enforces, and
            // for the same reason: re-reading a later record would let every resume push
            // the reported deadline forward.
            JournalEvent::SignalAwaited { node, deadline } => {
                awaited.entry(node.clone()).or_insert(*deadline);
            }
            // SP-6 s2: a gate is awaiting for LISTING purposes too, and this arm is what
            // makes it visible at all — without it `awaiting_nodes` finds nothing for a
            // `HumanGate` and the node never appears in `list-paused`, which is the worst
            // available outcome: an operator cannot decide what they cannot see, and the
            // run waits on a human who was never told. FIRST wins, exactly as for
            // `SignalAwaited` above and for the same reason.
            //
            // It does NOT make a gate answerable by `run signal` — that command refuses a
            // node with a journaled menu before it ever reaches this fold (`gate_menu`),
            // and `signal_state` is consulted only after that refusal.
            JournalEvent::GateAwaited { node, deadline, .. } => {
                awaited.entry(node.clone()).or_insert(*deadline);
            }
            // SP-6 s3: the third waiting kind — a human-backed `Agent`. Without this arm
            // `awaiting_nodes` finds nothing for it and the node never appears in
            // `list-paused` at all, which is the worst available outcome for the very
            // feature this slice ships: an operator cannot answer what they cannot see, and
            // the run waits on a human who was never told. It is also what makes
            // `cmd::human::answer`'s node-state pre-check able to say `Awaiting` rather than
            // refusing every legitimate answer as "not awaiting". FIRST wins, exactly as for
            // the two arms above and for the same reason.
            //
            // It does NOT make a human-backed agent answerable by `run signal` or by
            // `run gate decide` — both refuse a node with a journaled question before they
            // reach this fold (`cmd::human::agent_question`), and `signal_state` is consulted
            // only after that refusal.
            JournalEvent::AgentAwaited { node, deadline, .. } => {
                awaited.entry(node.clone()).or_insert(*deadline);
            }
            // SP-6 s4: the FOURTH waiting kind, a `Loop` gate decided by a person. Same
            // arm, same first-wins rule, same stakes as the three above — and here the
            // stakes are at their highest, because the node's path is SYNTHESIZED
            // (`"{loop}/{i}/__gate__"`) and appears in no graph, so this fold is the only
            // thing in the system that can say a human is being waited on there.
            //
            // Without it the window the s4 plan's Task 12 precondition names is open: the
            // executor arm is live, the run pauses, `list-paused` shows nothing, and the
            // gate waits out its SLA and terminally fails the whole `Loop` with no
            // operator recourse at all. It also gives `cmd::gate::decide` the DEADLINE it
            // refuses a late decision against — `SignalState::Awaiting { deadline }` is
            // where that guard reads it from, for this kind as for a `HumanGate`.
            JournalEvent::LoopGateAwaited { node, deadline, .. } => {
                awaited.entry(node.clone()).or_insert(*deadline);
            }
            JournalEvent::NodeCompleted { node } => {
                terminal.insert(node.clone(), (*seq, SignalState::Completed));
            }
            JournalEvent::NodeFailed { node, .. } => {
                terminal.insert(node.clone(), (*seq, SignalState::Failed));
            }
            JournalEvent::NodeSkipped { node } => {
                terminal.insert(node.clone(), (*seq, SignalState::Skipped));
            }
            // A loop gate's completion marker, and it needs its own arm because neither of
            // the two generic ones can see it: the kind journals no `NodeCompleted` (like
            // `Branch`/`Subgraph`), and it publishes nothing to the blackboard under its
            // own id, so the `ContextWrite` backstop below never fires for it either.
            //
            // `LoopGateSettled` is the right marker and `LoopGateDecided` is NOT, which is
            // the whole subtlety. A decision folds LAST-wins precisely so an operator can
            // correct it while the run is still paused; the SETTLEMENT is the line after
            // which "before the run resumes" has passed — the drive read the answer,
            // honoured it and spent an iteration on the strength of it, and
            // `run_human_loop_gate`'s step 1 replays that option forever without
            // re-reading the decision. So a settled gate is one nobody is waiting on: it
            // must leave `list-paused`, and `cmd::gate::decide` must refuse a second
            // decision that nothing will ever read rather than reporting it delivered.
            //
            // Without this arm iteration 0's gate stays `Awaiting` for the life of the
            // run, because `run_loop` re-enters `for i in 0..max_iters` from zero on every
            // drive and the `LoopGateAwaited` above is folded first-wins forever — so a
            // ten-iteration loop would advertise ten decided gates beside the one real ask.
            //
            // `or_insert`, matching the inferred `ContextWrite` marker below rather than
            // the three real terminal events above: a `NodeFailed` at this path (an
            // expired gate, or a cascade skip) is the stronger statement and must win.
            JournalEvent::LoopGateSettled { node, .. } => {
                terminal
                    .entry(node.clone())
                    .or_insert((*seq, SignalState::Completed));
            }
            // The completion marker for the node kinds that journal no `NodeCompleted`.
            // `or_insert`, not `insert`: a real terminal event above is the stronger
            // statement and must not be overwritten by this inferred one.
            JournalEvent::ContextWrite {
                scope: Scope::Run,
                key,
                ..
            } => {
                terminal
                    .entry(NodeId(key.0.clone()))
                    .or_insert((*seq, SignalState::Completed));
            }
            // FIRST wins, like every other marker here: a run completes once, and the
            // instant it did is what a delivery has to be ordered against.
            JournalEvent::RunCompleted => {
                run_completed.get_or_insert(*seq);
            }
            _ => {}
        }
    }
    awaited
        .into_iter()
        .map(|(node, deadline)| {
            let at = match terminal.get(&node) {
                Some((seq, state)) => SignalStateAt {
                    state: state.clone(),
                    at: Some(*seq),
                },
                // The backstop for a deployment with no `ContextStore` wired: an
                // `AwaitSignal` node journals no `NodeCompleted`, so with no blackboard
                // publish to read, `RunCompleted` is the ONLY evidence this node finished.
                None => match run_completed {
                    Some(seq) => SignalStateAt {
                        state: SignalState::Completed,
                        at: Some(seq),
                    },
                    None => SignalStateAt {
                        state: SignalState::Awaiting { deadline },
                        at: None,
                    },
                },
            };
            (node, at)
        })
        .collect()
}

/// One node's [`SignalState`], folded from `events`.
pub fn signal_state(events: &[(Seq, JournalEvent)], node: &NodeId) -> SignalState {
    signal_state_at(events, node).state
}

/// One node's [`SignalState`] **with** the seq that established it — what `signal`'s
/// post-write report needs; see [`SignalStateAt`].
///
/// `pub(crate)` for `cmd::gate::decide`, whose post-append arm has the identical question
/// to answer and must not answer it with a second fold: was our row READ by the drive that
/// terminated the node, or ORPHANED behind it? Only the journal ORDER can tell those apart,
/// and only this function reports it.
pub(crate) fn signal_state_at(events: &[(Seq, JournalEvent)], node: &NodeId) -> SignalStateAt {
    signal_states(events).remove(node).unwrap_or(SignalStateAt {
        state: SignalState::NotAwaiting,
        at: None,
    })
}

/// Every node in this run that is currently awaiting a human, in node-id order so the
/// rendering is deterministic run to run.
///
/// Covers ALL FOUR waiting kinds, because [`signal_states`] folds all four `*Awaited`
/// events: an `AwaitSignal`, a `HumanGate`, a human-backed `Agent` (SP-6 s3) and a `Loop`'s
/// human gate (SP-6 s4).
///
/// **And it tells all four apart**, which is the point of carrying a menu and a question
/// rather than only a node id: a listing that rendered them identically would send an
/// operator to a refusal for a node they had correctly identified. `options` alone is a
/// `HumanGate`, `question` alone a human-backed `Agent`, BOTH a loop gate, and neither an
/// `AwaitSignal` — the discriminator [`render::AwaitingNode::options`] documents for the
/// `--json` path.
///
/// Three kinds, three verbs, four rows: a loop gate takes the SAME `run gate decide` a
/// `HumanGate` does, which is why the pair is a safe discriminator to add — an existing
/// script that keyed on `options` first still builds the right command for it.
///
/// The question is REDACTED here, once, so that every sink is covered by construction. See
/// [`render::AwaitingNode::question`] for why that scrub belongs on this side and why the
/// collapse-and-cap does not. It uses [`render::redact_question`], NOT the pause-reason
/// transform it was written with: that one discards the whole string on either-direction
/// disagreement, which for a question means an operator who cannot see what they are being
/// asked — and it fired on ordinary prose (a line ending in the word "bearer").
fn awaiting_nodes(events: &[(Seq, JournalEvent)]) -> Vec<render::AwaitingNode> {
    // FIRST wins, and this is not merely symmetry with the resolver below: it is the SAME
    // rule `cmd::human::agent_question` folds by, and that function is what
    // `run agent answer` validates against. If the listing showed a later question than the
    // command answers, an operator would answer a question they were never shown.
    let questions: HashMap<NodeId, String> = events
        .iter()
        .filter_map(|(_, e)| match e {
            JournalEvent::AgentAwaited { node, prompt, .. } => Some((node.clone(), prompt)),
            _ => None,
        })
        .fold(HashMap::new(), |mut acc, (n, p)| {
            acc.entry(n).or_insert_with(|| render::redact_question(p));
            acc
        });

    // SP-6 s4's kind is the only one that publishes BOTH — a menu to pick from and the
    // question the pick answers. The MENU comes from the shared resolver below; only the
    // PROMPT is folded here, first-wins on `LoopGateAwaited` exactly as the resolver is, so
    // the two necessarily read the same event.
    //
    // The question takes the SAME `redact_question` scrub an agent's does. The executor
    // already scrubbed it before the `LoopGateAwaited` append, so this is a second pass and
    // not the first — see [`render::AwaitingNode::question`] for why the second is still
    // owed: `Executor::with_redactor` is opt-in and defaults to `None`, so an embedder that
    // wired none writes the question as composed.
    //
    // **The MENU deliberately gets no second pass, and that is a bounded gap rather than a
    // guarantee.** The reason not to re-scrub is right: the menu is not display text, it is
    // the VOCABULARY `gate decide` matches an operator's `--option` against
    // (`published.iter().find(|o| o.name == decision.option)`), so a display-only scrub
    // would offer names the decide path refuses. The scrub is therefore owed ENTIRELY by
    // the executor at the `LoopGateAwaited` append, where `run_human_loop_gate` redacts the
    // option names and refuses a menu redaction would make ambiguous.
    //
    // What this comment used to say — "so the journaled names are the scrubbed ones" — is
    // true only when a redactor was wired, and it asserted the opposite precondition to the
    // one its neighbour on the same struct states. Against a journal written by a library
    // embedder with no redactor, `list-paused` renders the menu as stored while scrubbing
    // the question beside it: the same author string clean in one cell and plaintext in the
    // next, in the table and in `--json`, and recited again by `gate decide`'s bad-option
    // refusal. Not reachable through torii's own boot, which wires `PatternRedactor`
    // unconditionally (`boot.rs`), which is what keeps this a known limit rather than a
    // live leak.
    let loop_prompts: HashMap<NodeId, String> = events
        .iter()
        .filter_map(|(_, e)| match e {
            JournalEvent::LoopGateAwaited { node, prompt, .. } => Some((node.clone(), prompt)),
            _ => None,
        })
        .fold(HashMap::new(), |mut acc, (n, p)| {
            acc.entry(n).or_insert_with(|| render::redact_question(p));
            acc
        });

    let mut out: Vec<render::AwaitingNode> = signal_states(events)
        .into_iter()
        .filter_map(|(node, st)| match st.state {
            SignalState::Awaiting { deadline } => {
                // **ONE resolver for the kind, shared with every refusal path.** Not a
                // parallel rule that agrees by inspection: literally
                // [`crate::cmd::gate::gate_menu`], the function `gate decide`, `run signal`
                // and `run agent answer` each resolve a menu-bearing node with.
                //
                // The three maps were consulted independently until s4, which was harmless
                // while `options` and `question` came from mutually exclusive events. s4
                // made the PAIR the discriminator for a loop gate and replaced that with a
                // menu-first fall-through — which fixed the `GateAwaited` + `AgentAwaited`
                // journal and introduced a new disagreement on the `GateAwaited` +
                // `LoopGateAwaited` one: this side tried the loop-gate map FIRST and
                // unconditionally, while `gate_menu` is a single `find_map` across both
                // variants and so is first-wins in JOURNAL order. A journal that published
                // a `GateAwaited` and then a `LoopGateAwaited` at one id listed as
                // `loop gate: revise` and was then refused `revise` by `gate decide`,
                // which had resolved `ship|hold`. Calling the resolver removes the second
                // order rather than re-deriving it correctly, which is what the previous
                // two attempts each did. Guarded by
                // `the_listing_resolves_a_two_menu_journal_exactly_as_gate_decide_does`.
                //
                // Per awaiting node rather than one pass over the journal, and that cost is
                // accepted deliberately: the alternative is a second implementation of
                // first-wins-across-two-variants, which is the thing that just went wrong
                // twice. `list-paused` folds one journal per PAUSED run and the awaiting
                // set of one run is small, so this is a scan of a journal already in
                // memory, not a load.
                let (options, question) = match crate::cmd::gate::gate_menu(events, &node) {
                    // BOTH ⇒ a loop gate, and now by CONSTRUCTION: the pair can only be
                    // produced here, from a node the shared resolver called a loop gate.
                    Some(crate::cmd::gate::PublishedMenu::Loop(menu)) => (
                        Some(menu.iter().map(|o| o.name.clone()).collect()),
                        // Always `Some` — the resolver answered `Loop` because it read a
                        // `LoopGateAwaited` at this node, and this map folds the same
                        // events by the same first-wins rule. Left as an `Option` rather
                        // than unwrapped anyway: `list-paused` reads journals torii did not
                        // necessarily write, and an unwrap here would take down the listing
                        // for the WHOLE fleet over one row. The degradation is a plain
                        // `gate:` row — a kind whose verb is the same one — rather than a
                        // panic.
                        loop_prompts.get(&node).cloned(),
                    ),
                    Some(crate::cmd::gate::PublishedMenu::Human(opts)) => {
                        (Some(opts.iter().map(|o| o.name.clone()).collect()), None)
                    }
                    None => (None, questions.get(&node).cloned()),
                };
                Some(render::AwaitingNode {
                    node,
                    deadline,
                    options,
                    question,
                })
            }
            _ => None,
        })
        .collect();
    out.sort_by(|a, b| a.node.0.cmp(&b.node.0));
    out
}

/// The largest human answer torii will journal, in bytes of serialized JSON.
///
/// §6.5: an unbounded JSON blob in a journal row is a durable footgun. The executor's own
/// convention for "too big to sit inline in a journal row" is `split_output`'s
/// `cas_threshold`, whose default is exactly this number — but that convention cannot be
/// reused here, because it routes over-threshold values to the `ContentStore` as an
/// `EffectOutput::Ref`, and `SignalReceived.payload` is a bare `serde_json::Value` with
/// no ref-or-inline alternative. Changing that shape would break the journal format and
/// force a `FORMAT_VERSION` bump for a size cap, so the cap is enforced in the CLI, at
/// every writer, by rejecting.
///
/// **Writers, plural, since SP-6 s2.** [`signal`] bounds `SignalReceived.payload`, and
/// `cmd::gate::decide` bounds BOTH operator-supplied fields of `GateDecided` — the `note`
/// and the `actor` — through the same `check_payload_size`, on the same number. That is
/// deliberate: an enforcement each writer re-derives for itself is bounds that drift, and
/// `decide` shipped with no bound at all while this doc said there was only one writer to
/// have. The `actor` is the correction after that: it is the SIBLING field on the same
/// durable row, it was capped by nothing while its neighbour was capped at 4096, and it
/// is not display-only — the executor interpolates it into the rejection `NodeFailed`
/// that `torii run status` renders and every later drive re-emits from the fold.
///
/// SP-6 s4 adds a third row to that list and no third number: `LoopGateDecided.actor`,
/// written by the same `decide` through the same helper. It differs only in WHEN it is
/// measured — after redaction, because unlike `GateDecided.actor` this one is scrubbed
/// before the write, and the bytes bounded must be the bytes written.
///
/// 4 KiB is the same boundary the executor already applies to a model call's inline
/// output, so a journal row torii writes can never be larger than one the
/// executor itself writes inline. **That claim holds only because of the two corrections
/// below, each of which falsified it once — and each was found only by measuring the row
/// rather than the argument.** The cap is checked on the REDACTED value — the one actually
/// written, see [`Measured`] — and on a size measured the way the DURABLE backend stores
/// it rather than the way `serde_json` writes it, see [`jsonb_number_expansion`]. It is
/// also far beyond any real use: both are a human DECISION
/// (`{"decision":"approved","note":"…"}`), not a data channel, and 4 KiB is roughly 600
/// words of prose.
///
/// **Carry-forward:** this bounds the CLI, not the journal. `SignalReceived` and
/// `GateDecided` remain uncapped for any future writer (a webhook/HTTP delivery path,
/// §8's deferred non-CLI delivery). A durable-side cap needs the payload to become
/// ref-or-inline, which is a format break.
// `check_payload_size` above is deliberately NOT bracketed as an intra-doc link. It is
// `pub(crate)` and this item is `pub`, so a link makes `cargo doc` emit a
// `rustdoc::private_intra_doc_links` warning for something that cannot resolve in the
// public docs anyway — SP-6 s2 added exactly one such warning to this crate (8 -> 9) by
// bracketing it. Eight others pre-date the slice and are left alone here.
pub const MAX_PAYLOAD_BYTES: usize = 4096;

/// WHICH size [`check_payload_size`] is measuring — because redaction sits between the
/// two and can make the second LARGER than the first.
///
/// `[REDACTED]` is 10 bytes and the assignment pattern's shortest matched value is 6, so
/// a payload of many short `token:…` pairs inflates by roughly 1.67x: a measured 4064-byte
/// payload journals a 5312-byte row. Checking only [`AsGiven`](Measured::AsGiven) let that
/// through, which falsified the "never larger than an inline executor row" claim above.
#[derive(Clone, Copy)]
pub(crate) enum Measured {
    /// The payload exactly as the operator supplied it. Checked FIRST, and by
    /// [`parse_payload`] before any connection is opened, so the redactor is never handed
    /// an unbounded blob.
    AsGiven,
    /// The payload as it will actually be journaled. This is the check the cap exists
    /// for: the durable row is the thing being bounded.
    AfterRedaction,
}

/// The EXTRA bytes this value costs once the durable backend has normalised it — which is
/// not what `serde_json` wrote.
///
/// `journal_events.event` is a Postgres `jsonb` column, and `jsonb` normalises every JSON
/// number to `numeric`. `serde_json` writes the largest finite `f64` as `1e308` — 5 bytes;
/// `jsonb` stores and returns it as a 309-digit integer. So the wire size does not bound
/// the durable size at all, and the gap is unbounded in the operator's favour:
///
/// ```text
/// {"n":[1e308 x 583]}   serde_json::to_vec  ->      4088 bytes   (under the cap, accepted)
///                       jsonb, as returned  ->   181_320 bytes   (44x the cap)
///                       jsonb, on disk      ->     9_350 bytes   (still over, post-TOAST)
/// ```
///
/// Measured on `postgres:16`. The 181_320 is the number that matters: it is what every
/// `journal.load` decodes, on every drive and every `list-paused`, for the life of the run.
///
/// Charged to numbers ONLY, and only to those the backend really rewrites — an integer's
/// wire form already IS its `numeric` form, so a decision object measures identically
/// before and after. Rust's `Display` for `f64` is positional (never exponential), which
/// makes it exactly the expansion `numeric` performs, in both directions: `1e-308` expands
/// too.
fn jsonb_number_expansion(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Number(n) if !n.is_i64() && !n.is_u64() => {
            let on_the_wire = n.to_string().len();
            let as_stored = n.as_f64().map_or(on_the_wire, |f| format!("{f}").len());
            as_stored.saturating_sub(on_the_wire)
        }
        serde_json::Value::Array(a) => a.iter().map(jsonb_number_expansion).sum(),
        serde_json::Value::Object(o) => o.values().map(jsonb_number_expansion).sum(),
        _ => 0,
    }
}

/// Refuse an over-limit payload, naming BOTH the limit and the actual size — an operator
/// who pasted a file needs to know how much to cut, not just that they were over.
///
/// `what` NAMES the offending input (`--payload`, `--note`), because there is now more
/// than one writer to the durable column this bounds: `cmd::gate::decide` journals a
/// decision note through the same cap. Telling a `gate decide --note` operator to shorten
/// their `--payload` would send them to a flag that command does not have. `pub(crate)`
/// for the same reason — the bound is reused, never re-derived, so the two writers cannot
/// drift on the number OR on how it is measured (wire bytes plus
/// [`jsonb_number_expansion`]).
///
/// The value itself is NEVER echoed, only its size: this is the one input an operator
/// might paste a credential into, and stderr reaches journald and CI logs.
pub(crate) fn check_payload_size(
    payload: &serde_json::Value,
    measured: Measured,
    what: &str,
) -> Result<(), String> {
    // `to_vec` (not `to_string().len()`) so the number is bytes on the wire, PLUS what the
    // backend's own normalisation adds on top — see `jsonb_number_expansion`. Measuring
    // only the wire form bounded a value nobody durably stores.
    let wire = serde_json::to_vec(payload).map_or(usize::MAX, |b| b.len());
    let expansion = jsonb_number_expansion(payload);
    let size = wire.saturating_add(expansion);
    if size > MAX_PAYLOAD_BYTES {
        // Naming which size this is, and WHY it differs from what was sent, matters: an
        // operator who sent 4064 bytes and is told "5312 bytes" with no explanation will
        // assume the tool is broken. Both growth causes are named because either one can
        // be the whole difference — redaction inflates by ~1.67x, the backend's numeric
        // normalisation by up to ~50x.
        let mut growth = String::new();
        if matches!(measured, Measured::AfterRedaction) {
            growth.push_str(
                " once redacted (secret-shaped text is replaced by the longer literal \
                 `[REDACTED]` before the row is written)",
            );
        }
        if expansion > 0 {
            let joiner = if growth.is_empty() { "" } else { " and" };
            growth.push_str(&format!(
                "{joiner} once stored (the journal column is `jsonb`, which normalises \
                 every JSON number to `numeric` — that expands this payload by \
                 {expansion} bytes)"
            ));
        }
        if !growth.is_empty() {
            growth.push_str(", so the durable row is bigger than what you sent");
        }
        return Err(format!(
            "{what} is {size} bytes{growth}, over the {MAX_PAYLOAD_BYTES}-byte limit. This \
             is a human DECISION, not a data channel — it is journaled durably and folded \
             into the node's output on every resume. Put the bulk somewhere the graph can \
             read (a workspace file, the blackboard) and reference it here."
        ));
    }
    Ok(())
}

/// Parse `--payload <json>`: any JSON value, capped at [`MAX_PAYLOAD_BYTES`].
///
/// A clap `value_parser`, so both failures are reported BEFORE `dispatch` reads the
/// environment or opens a connection — the same discipline as `--older-than` and
/// `--budget-tokens`.
///
/// **The offending value is NEVER echoed**, unlike every other parser in this module
/// (which echo a token count or a retention window — values that cannot be secrets).
/// This flag is the one place an operator might paste a credential, and the single most
/// likely way to do it is to type the token bare (`--payload sk-…`), which is not valid
/// JSON — so the invalid-JSON message is exactly the path that would print it to stderr,
/// and thus into journald and CI logs. This is the same discipline `boot`'s
/// `gateway_config_parse_error` applies to the file that holds provider API keys.
///
/// `{e}` is safe to include and is checked, not assumed: deserializing into an UNTYPED
/// `serde_json::Value` can only ever fail syntactically, and serde_json's `Display` for
/// those reports a category and a position (`expected value at line 1 column 1`) with no
/// input bytes. The `invalid type: string "sk-live-…"` shape that leaks in `boot` comes
/// from deserializing into a TYPED struct, which this does not do.
///
/// This checks the payload [`AsGiven`](Measured::AsGiven) only. The check that bounds the
/// durable row — [`AfterRedaction`](Measured::AfterRedaction) — needs the redactor and
/// lives in [`signal`]; it can refuse a payload this one accepted, after a connection has
/// been opened. That is the right split: this one exists to fail fast and to keep an
/// unbounded blob away from the redactor, not to be the authority on the row size.
pub fn parse_payload(s: &str) -> Result<serde_json::Value, String> {
    let v: serde_json::Value = serde_json::from_str(s).map_err(|e| {
        format!(
            "invalid --payload: {e}. The payload is JSON — quote a bare string \
             (\"approved\") or pass an object, e.g. {{\"decision\":\"approved\"}}. The \
             offending value is deliberately not echoed: this flag is not a credential \
             channel and an operator may have pasted one here."
        )
    })?;
    check_payload_size(&v, Measured::AsGiven, "--payload")?;
    Ok(v)
}

/// Deliver an external signal to an `AwaitSignal` node (SP-6 s1) — the HITL primitive's
/// operator surface.
///
/// SP-DATA-4 shipped HOTL, human *on* the loop: `cancel`/`wake` intervene from outside and
/// the run does not know they exist. `force_wake` is a RESUME, not a DECISION. This is the
/// missing half: it carries an ANSWER back into a graph that is designed to wait for one.
///
/// **It queues the wake as well as journaling the answer, and must.** A gate pauses with
/// `resume_after` = its deadline, or `None` for the indefinite class — so the run is
/// either due only at a future instant or never due at all, and in BOTH cases the next
/// worker tick would not claim it. Journaling the answer alone would leave the run
/// sitting there indefinitely while this command claimed "the run will resume on the next
/// worker tick", which is precisely the decorative-feature failure this slice's design
/// warns about.
///
/// **Order: append, THEN `force_wake`** — never the reverse, for the same reason
/// [`wake`]'s `BudgetRaised` append comes first. `force_wake` only flips `next_wake`; a
/// worker in another process can claim that wake the instant it lands. Appending first
/// guarantees any worker that can observe the wake folds a journal that already contains
/// the answer.
///
/// **Check-then-act, ordered by seq.** The node's state is read, and then read AGAIN
/// after the write, and the report is derived from the SECOND read. The pre-check refuses
/// cheaply; the post-check is what makes the report honest when a worker drove the run
/// inside the window (`wake` once reported exactly that lost race as a success). The only
/// state-changing call, `force_wake`, is itself conditional on the row still being
/// `paused` in both shipped stores, so a lost race is a no-op there rather than a
/// double-apply.
///
/// A terminal state on the second read is NOT by itself a refusal, and treating it as one
/// was this command's worst bug: the append has already succeeded, so the answer IS
/// durable, and the live question is whether anything read it. The appended `Seq` against
/// the terminal marker's `Seq` decides — see [`SignalStateAt`].
///
/// **The payload is redacted before it is size-checked and journaled** (§6.4/§6.5) — see
/// [`render::redact_payload`] and [`Measured`]. **A signal is not a credential channel;
/// the credential broker is.**
pub async fn signal(
    store: &dyn SchedulerStore,
    journal: &dyn ExecutionJournal,
    run: RunId,
    node: NodeId,
    payload: serde_json::Value,
    now: DateTime<Utc>,
) -> Result<Outcome, CliError> {
    // Pure, before any I/O: an over-limit payload can never reach the journal, whichever
    // caller got here. `dispatch` rejects it earlier still (via `parse_payload`, before a
    // connection is opened); this is the check EVERY path shares, so the library entry
    // point cannot be used to bypass the cap.
    //
    // A hard error (exit 1), not a precondition (exit 2): exit 2 in this taxonomy means
    // "ran fine, nothing to do" — an over-limit payload is invalid INPUT, which is what
    // `parse_run_id` also treats as exit 1. The two entry points must not disagree about
    // the exit code for one violation.
    //
    // TWO checks, on either side of the redaction, and both are load-bearing. The first
    // bounds what the redactor is handed. The second is the one the cap actually exists
    // for: redaction REPLACES secret-shaped spans with a LONGER literal, so it can grow
    // the payload (a measured 4064-byte payload journals a 5312-byte row) — checking only
    // the as-given size bounded the wrong value entirely. Redacting up here rather than
    // inline at the append is what makes the checked bytes and the written bytes the same
    // bytes; the SAME pure pass the executor applies on the fold-read, so this stays
    // idempotent (`[REDACTED]` matches no credential shape) and live == journaled ==
    // replayed (§6.4).
    check_payload_size(&payload, Measured::AsGiven, "--payload").map_err(CliError::error)?;
    let payload = render::redact_payload(&payload);
    check_payload_size(&payload, Measured::AfterRedaction, "--payload").map_err(CliError::error)?;

    // A node id is operator-supplied free text on this path, and every message below
    // echoes it back to a terminal. `one_line` collapses control characters (Unicode Cc,
    // which includes ESC) for exactly the reason it does in the pause-reason table: a raw
    // newline or an ANSI escape in the echoed id would let the reported outcome forge
    // extra lines or rewrite what is already on screen. Display only — the value written
    // to the journal is the id as given.
    let shown = render::one_line(&node.0);

    let Some(before) = store.status(run).await? else {
        return Ok(Outcome::precondition(format!("no such run: {}", run.0)));
    };
    let events = journal
        .load(run)
        .await
        .map_err(OrchestratorError::Journal)?;

    // SP-6 s2 AC7: a `HumanGate` is answerable ONLY by `GateDecided`. Without this
    // refusal a raw `--payload '{}'` would bypass every validation s2 adds — the menu
    // check, the outcome mapping and the required reason — and would do it silently: the
    // executor's `run_human_gate` reads only `GateDecided`, so the row would be journaled,
    // never read, and reported here as `signalled`.
    //
    // Checked BEFORE `signal_state`, not after, because a gate does not fold as
    // `Awaiting` at all (that fold reads `SignalAwaited`, and a gate journals
    // `GateAwaited`) — reaching the generic arm would refuse it as "not awaiting a
    // signal", which is true and useless: it sends an operator to check the node id when
    // the id was right and the COMMAND was wrong.
    //
    // SP-6 s4 splits this arm in two. `gate_menu` now answers for BOTH menu-bearing kinds
    // and says which published, because the refusal has to name what the node actually is:
    // "a HumanGate" for a loop gate would be a confident lie about a node kind, on the one
    // surface an operator uses to work out what they are looking at. The VERB is the same
    // either way — the operator's vocabulary stays three commands, not four — so both arms
    // point at `run gate decide`.
    match crate::cmd::gate::gate_menu(&events, &node) {
        Some(crate::cmd::gate::PublishedMenu::Human(_)) => {
            return Ok(Outcome::precondition(format!(
                "not delivered: {shown} is a HumanGate, not an AwaitSignal — it accepts a \
                 named option, not arbitrary JSON. Use: torii run gate decide {} --node \
                 {shown} --option <name>",
                run.0
            )));
        }
        // The same bypass, on the kind that cannot even be looked up in a graph. A loop
        // gate's path is synthesized per iteration, so `SignalReceived` here would be
        // journaled at a node whose only reader — `run_human_loop_gate` — reads
        // `LoopGateDecided` and nothing else: never read, and reported as `signalled`.
        Some(crate::cmd::gate::PublishedMenu::Loop(_)) => {
            return Ok(Outcome::precondition(format!(
                "not delivered: {shown} is a Loop's human gate, not an AwaitSignal — it \
                 accepts one of the options it published, not arbitrary JSON. Use: torii \
                 run gate decide {} --node {shown} --option <name>",
                run.0
            )));
        }
        None => {}
    }

    // SP-6 s3 AC7, the third arm of the same matrix. A human-backed `Agent` is answerable
    // ONLY by `AgentAnswered`: `run_human_agent` reads nothing else, so a raw `--payload`
    // would be journaled, never read, and reported here as `signalled`. Unlike the gate
    // refusal above this is not about skipping VALIDATION — there is no menu to validate
    // against — it is about skipping ATTRIBUTION: `SignalReceived` has no `actor` field, and
    // an agent's answer becomes the node's OUTPUT and flows into downstream model prompts,
    // so "who said this" is part of the value rather than an audit trail beside it.
    //
    // Checked BEFORE `signal_state` for the same reason the gate arm is: since s3 the fold
    // DOES report a human-backed agent as `Awaiting` (its `AgentAwaited` arm, added so
    // `list-paused` can show it), so reaching the generic arm would not refuse this at all —
    // it would deliver a payload nothing will ever read.
    if crate::cmd::human::agent_question(&events, &node).is_some() {
        return Ok(Outcome::precondition(format!(
            "not delivered: {shown} is a human-backed Agent, not an AwaitSignal — its \
             answer becomes the node's output and is recorded with WHO answered, which a \
             raw payload cannot carry. Use: torii run agent answer {} --node {shown} \
             --text '<answer>'",
            run.0
        )));
    }

    match signal_state(&events, &node) {
        SignalState::Awaiting { .. } => {}
        // Everything else is a no-op at the node, so say so instead of writing.
        other => {
            return Ok(Outcome::precondition(not_delivered(
                &shown,
                &other,
                CompletedResidue::LastWins,
            )));
        }
    }
    if before.status != RunStatus::Paused {
        // A `waking` row means a worker holds the lease and is folding this journal right
        // now; a terminal row means nothing will ever read the answer. Neither is a state
        // to write into — but they call for OPPOSITE advice, and giving one message for
        // both handed an operator of a cancelled run "retry once it shows paused", which
        // is advice to wait forever: no shipped store ever moves a terminal row back to
        // `paused`. (A terminal run is reachable here with the node still folding as
        // awaiting, because `cancel`/`record_terminal` journal no node event.)
        return Ok(Outcome::precondition(
            if before.status == RunStatus::Waking {
                format!(
                    "not delivered: {} is awaiting a signal, but the run is waking — a worker \
                 holds the lease and is folding this journal right now. Retry once \
                 `torii run status {}` shows it paused.",
                    shown, run.0
                )
            } else {
                format!(
                    "not delivered: {} is awaiting a signal, but the run is {} — a {} run is \
                 never paused again, so nothing will ever read an answer delivered to it. \
                 Start a new run.",
                    shown,
                    before.status.as_str(),
                    before.status.as_str()
                )
            },
        ));
    }

    // The appended seq is KEPT, not discarded: it is the only thing that can order our
    // row against a terminal marker the post-check may find. See `SignalStateAt`.
    let appended = journal
        .append(
            run,
            JournalEvent::SignalReceived {
                node: node.clone(),
                // Already redacted, above — the value checked against the cap and the
                // value written are the same bytes.
                payload,
            },
        )
        .await
        .map_err(OrchestratorError::Journal)?;
    // Past this point the answer is DURABLE, and every remaining call is fallible. A `?`
    // here reported a bare `store error: …` — exit 1, no mention of the row — which is the
    // one thing §6.6 forbids: it reads as "the signal did not go through" for a write that
    // has already succeeded. Worse for the INDEFINITE class (`next_wake` NULL, never
    // auto-woken), where the run then waits forever on an answer nobody knows landed.
    //
    // So a post-append fault is a REPORT, not an error: it names the durable row and the
    // command that unblocks the run, in the same sentence shape the lost-race arm below
    // already uses.
    //
    // The error goes through `render::safe_reason` — redact, collapse control characters,
    // cap — for all three call sites at once, because it is FREE TEXT FROM THE DRIVER and
    // not a message this crate wrote. `PostgresJournal::load` builds it from both
    // `sqlx::Error` (a pool timeout carries the whole connection string, password
    // included) and `serde_json::Error` (over a TYPED `JournalEvent`, which quotes the
    // offending row — the `invalid type: string "sk-live-…"` shape `parse_payload`
    // documents). Interpolated raw it also let a newline forge a line beginning with a
    // pastable uuid and an ANSI escape rewrite what was already on screen. Same
    // transform, same error class, as `render::awaiting_section` applies to a per-run
    // journal fault in `list_paused` — and `cmd::gate::decide` has the identical closure.
    let unread = |what: &str, e: &dyn std::fmt::Display| {
        let e = render::safe_reason(&e.to_string());
        Outcome::precondition(format!(
            "not queued: {shown}'s answer is journaled durably (seq {appended}), but {what} \
             failed: {e}. Nothing has read it yet and the run is not queued to resume — run \
             `torii run wake {}` to drive it.",
            run.0
        ))
    };
    if let Err(e) = store.force_wake(run, now).await {
        return Ok(unread("the wake", &e));
    }

    // ---- The effect actually achieved, read back rather than assumed ------------------
    // A fault on either READ is less damaging than the wake fault above — the wake may
    // well have applied — but it is still a durable row the operator has not been told
    // about, so it takes the same shape rather than a bare `?`.
    //
    // The row is never deleted by any shipped store, so `None` would mean a hypothetical
    // future retention/purge raced us, not a reachable path today.
    let after = match store.status(run).await {
        Ok(Some(s)) => s,
        Ok(None) => return Ok(unread("the status re-read", &"the run vanished mid-signal")),
        Err(e) => return Ok(unread("the status re-read", &e)),
    };
    let after_events = match journal.load(run).await {
        Ok(evs) => evs,
        Err(e) => return Ok(unread("the journal re-read", &e)),
    };
    // The node is terminal by now, or it is not. If it is, "not delivered" is FALSE — the
    // row is already durable — and the honest question is whether anything READ it. The
    // journal order answers that, which is why the append's seq was kept: a terminal
    // marker BEHIND our row means the drive that terminated the node folded a journal
    // that already contained the answer; a marker AHEAD of it means the node was already
    // dead when the row landed and nothing will ever read it.
    //
    // The first cut of this reported every terminal post-check as "not delivered" with
    // exit 2. That inverted §6.6 on the command's most successful path: a worker that
    // claims a due gate the instant the delivery lands folds the answer, completes the
    // node and drives the run to completion — the delivery worked perfectly, and the CLI
    // said it had not happened.
    let SignalStateAt { state, at } = signal_state_at(&after_events, &node);
    if let Some(at) = at {
        return Ok(match (&state, at > appended) {
            // Terminated AFTER our row landed, by completing: the answer was on the
            // journal for the fold that completed it. This is a SUCCESS, and the only
            // difference from the ordinary path is that the run is already moving.
            //
            // The wording claims the ordering (which is proven) and not authorship of the
            // completion (which is not): if a duplicate answer for this node was already
            // on the journal — §7's last-wins case — the drive may have folded that one.
            // Both were delivered; both are durable; which one won is not observable from
            // here, so the sentence does not assert it.
            (SignalState::Completed, true) => Outcome::ok(format!(
                "signalled: {shown} (a drive already in flight completed the node after \
                 the answer landed, so the run is moving without waiting for a tick)"
            )),
            // Terminated AFTER our row, but NOT by completing — a deadline that fired, or
            // a cascade skip. The drive that failed it had loaded the journal before our
            // row landed, so it never saw the answer. Reporting `signalled` here would
            // hide a failed gate behind a success.
            (other, true) => Outcome::precondition(format!(
                "not read: {shown}'s answer is journaled durably, but {shown} is {} — it \
                 terminated while this delivery was in flight, and a drive that had \
                 already loaded the journal would not have seen the answer.",
                other.as_str()
            )),
            // Terminated BEFORE our row landed: a true orphan. Worth saying plainly,
            // because the residue is durable and consequential — an `AwaitSignal` node
            // journals no `NodeCompleted` and `NodeFailed` is not folded as a barrier, so
            // a later re-`start` would re-execute the gate and fold this late answer as
            // its output, silently converting an expired gate into an answered one.
            (other, false) => Outcome::precondition(format!(
                "not read: {shown}'s answer is journaled durably, but {shown} was already \
                 {} before the write landed, so nothing read it. The answer stays on the \
                 journal as a last-wins value that a re-`start` of this run would fold as \
                 the node's output — do not treat this run as answered.",
                other.as_str()
            )),
        });
    }

    // `at: None` ⇒ nothing has terminated the node, so the answer is still there to be
    // read and the only remaining question is the WAKE. (`NotAwaiting` also folds to
    // `None`, but is unreachable here: the pre-check read a `SignalAwaited` and the
    // journal is append-only, so this second, later read is a superset of that one.)

    // The wake half, checked exactly as `wake` checks its own: STATUS plus the pinned
    // timestamp, because `claim_due` leaves a stale `next_wake` untouched and an
    // unrelated re-pause can restore `paused` inside the race window. See `wake`'s
    // comment for why the 2µs tolerance is a `timestamptz` rounding allowance and not a
    // clock-skew fudge.
    let queued = after.status == RunStatus::Paused
        && after.next_wake.is_some_and(|t| {
            let drift = if t >= now { t - now } else { now - t };
            drift <= chrono::Duration::microseconds(2)
        });
    if queued {
        // Says QUEUED, never RESUMED: `force_wake` only sets `next_wake`; a worker tick
        // does the driving. Exactly what `run wake` learned to say.
        Ok(Outcome::ok(format!(
            "signalled: {} (the run will resume on the next worker tick)",
            shown
        )))
    } else {
        Ok(Outcome::precondition(format!(
            "not queued: {}'s answer is journaled durably, but the run is {} and the wake \
             did not apply — the drive that claimed it may have folded the journal before \
             the answer landed. Run `torii run wake {}` once it is paused again.",
            shown,
            after.status.as_str(),
            run.0
        )))
    }
}

/// What a row delivered to an ALREADY-COMPLETED node will do, which is the one thing
/// [`not_delivered`]'s terminal wording cannot state for every kind at once.
///
/// SP-6 s4's whole-slice review: `cmd::gate::decide`'s POST-append `orphan_residue` was
/// deliberately split per kind because the `HumanGate` sentence is FALSE for a loop gate.
/// The PRE-check refusal was not split, and went on asserting the same claim.
#[derive(Clone, Copy)]
pub(crate) enum CompletedResidue {
    /// The node re-executes and re-reads its answer on every drive, so a later row really
    /// would be folded: `AwaitSignal` (`run_await_signal` completes on any folded
    /// payload), `HumanGate` (`run_human_gate` re-reads `GateDecided`) and a human-backed
    /// `Agent` (`run_human_agent` re-reads `AgentAnswered`).
    LastWins,
    /// SP-6 s4: a loop gate does NOT. `run_human_loop_gate` reads a terminal verdict BACK
    /// rather than re-deriving one — step 0 returns a folded `NodeFailed` forever, and
    /// step 1 replays `LoopGateSettled` without ever consulting `LoopGateDecided` again —
    /// so whichever marker made the node terminal wins permanently and a later row is
    /// inert.
    InertLoopGate,
}

impl CompletedResidue {
    fn clause(self) -> &'static str {
        match self {
            CompletedResidue::LastWins => {
                "the node has read its answer and a later signal would only sit in the \
                 journal as a new last-wins value"
            }
            CompletedResidue::InertLoopGate => {
                "the gate's decision has been honoured and replays from its settlement, so \
                 a later decision would sit in the journal as inert residue that nothing \
                 will ever fold"
            }
        }
    }
}

/// The pre-check refusal text for a node that is not awaiting. Split out so the exact
/// wording cannot drift between the state variants.
/// `node` is the DISPLAY form (control characters already collapsed) — see `signal`.
///
/// `pub(crate)` for `cmd::gate::decide`, which refuses a non-awaiting node on the SAME
/// fold and must give the same answer: a `HumanGate` and an `AwaitSignal` are terminal for
/// identical reasons, and two wordings for one condition is two places for it to rot. The
/// text says "signal" where a gate would say "decision"; that is the price of one source
/// of truth, and it is the cheaper half of the trade — the sentence that matters ("a
/// terminal node never re-executes") is exactly the same fact in both commands.
///
/// **The `Completed` arm is the one place that price was too high.** Its clause described
/// what a late row DOES, and "a new last-wins value" is false for a loop gate — the same
/// falsehood `decide`'s post-append `orphan_residue` was split to remove, left standing on
/// the earlier path. So the caller supplies the clause as a [`CompletedResidue`] rather
/// than the function guessing from a state that cannot tell the kinds apart. Everything
/// else stays shared, because everything else really is the same fact.
pub(crate) fn not_delivered(node: &str, state: &SignalState, residue: CompletedResidue) -> String {
    match state {
        SignalState::Completed => format!(
            "not delivered: {node} already completed — {}.",
            residue.clause()
        ),
        SignalState::NotAwaiting => format!(
            "not delivered: {node} is not awaiting a signal — check the node id against \
             `torii run list-paused`, which names every node that is."
        ),
        // Never reached from `signal` (which matches `Awaiting` out first), but stated
        // explicitly rather than swept into the terminal arm below: that arm asserts the
        // node will NEVER read a signal, which for an awaiting node is the exact opposite
        // of the truth, and a future refactor must not be able to produce that sentence.
        SignalState::Awaiting { .. } => format!(
            "not delivered: {node} is awaiting a signal (this should have been delivered \
             — please report it)."
        ),
        other => format!(
            "not delivered: {node} is {} — a terminal node never re-executes, so it will \
             never read a signal.",
            other.as_str()
        ),
    }
}

/// Parse `--budget-tokens N` (on both `run submit` and `run wake`): a plain positive
/// integer, rejecting 0 loudly and with the offending value echoed back —
/// consistent with `--interval 0s` (`cmd::worker::parse_interval`) and
/// `TORII_POOL_SIZE=0` (`boot::parse_pool_size`). A zero-token budget can never
/// dispatch a single model call: the gate checks already-spent-vs-cap BEFORE every
/// dispatch (§6.2 of the design), so `spent (0) >= cap (0)` is true from the very
/// first check and the run would pause immediately, before doing any work at all —
/// almost certainly not what an operator setting a budget intends, so it is
/// refused as a precondition rather than accepted and silently useless.
pub fn parse_budget_tokens(s: &str) -> Result<u64, String> {
    let t = s.trim();
    let v: u64 = t
        .parse()
        .map_err(|_| format!("invalid --budget-tokens {t:?}: {t:?} is not a whole number"))?;
    if v == 0 {
        return Err(format!(
            "invalid --budget-tokens {t:?}: a zero-token budget can never dispatch a single \
             model call — the run would pause immediately, before doing any work"
        ));
    }
    Ok(v)
}

/// Parse a retention window: `30d`, `12h`, `90m`, `45s`.
///
/// A SIBLING of [`crate::cmd::worker::parse_interval`], deliberately, rather than an
/// extension of it. Extending that one to accept `d` would make `worker serve --interval
/// 30d` legal — a poll loop that sleeps for a month — and the units a flag accepts are part
/// of its contract, so widening the shared parser to serve `prune` would quietly loosen
/// `serve`. The two also have opposite natural ranges (milliseconds-to-minutes for a poll,
/// hours-to-days for retention), which is why `ms` is rejected here and `d` is rejected
/// there. `parse_interval` is untouched by this task.
///
/// Returns `chrono::Duration` because the only thing the caller does with it is subtract it
/// from a `DateTime<Utc>`; converting from `std::time::Duration` at the call site would add
/// a second fallible step for nothing.
pub fn parse_retention(s: &str) -> Result<chrono::Duration, String> {
    const UNITS: &str = "expected a number then one of s, m, h, d — e.g. 30d, 12h, 90m";
    let t = s.trim();
    // Split on the first non-digit: the number can only be digits, so the remainder is
    // exactly the unit. This is what turns `500ms` into a named "unknown unit" complaint
    // instead of a confusing "500m is not a number".
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    if num.is_empty() {
        return Err(format!("invalid retention window {t:?}: {UNITS}"));
    }
    // Digits only, so this can fail only by exceeding i64 — which is an overflow, not a
    // typo, and is reported as such.
    let v: i64 = num
        .parse()
        .map_err(|_| format!("invalid retention window {t:?}: {num:?} is out of range"))?;
    let window = match unit {
        "s" => chrono::Duration::try_seconds(v),
        "m" => chrono::Duration::try_minutes(v),
        "h" => chrono::Duration::try_hours(v),
        "d" => chrono::Duration::try_days(v),
        "" => return Err(format!("invalid retention window {t:?}: no unit — {UNITS}")),
        other => {
            return Err(format!(
                "invalid retention window {t:?}: unknown unit {other:?} — {UNITS}"
            ));
        }
    }
    .ok_or_else(|| format!("invalid retention window {t:?}: overflow"))?;
    if window.is_zero() {
        return Err(format!(
            "invalid retention window {t:?}: a zero window would delete a run that went \
             terminal a moment ago — use 1s if that is genuinely what you want"
        ));
    }
    Ok(window)
}

fn fmt_cutoff(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

/// Delete terminal run records older than `older_than`, after showing the operator how many
/// that is and (unless `yes`) asking.
///
/// **Only `completed`/`failed`/`cancelled` rows are ever eligible** — the store enforces
/// that, at any age. A `paused` run is live work awaiting a wake (the in-doubt-mutation
/// class waits INDEFINITELY, with no deadline at all), and a `waking` row may be a lease
/// held by an in-flight drive in another process; neither has an age at which forgetting it
/// is correct.
///
/// This is an explicit operator command rather than something `tick()` does on its own:
/// deleting durable rows as a silent side effect of a poll loop is exactly the kind of
/// surprise that should not exist, and it would make `tick()`'s contract much harder to
/// reason about.
///
/// The count is taken FIRST and shown before the prompt, using the same cutoff the delete
/// then uses — but the number reported at the end is the number actually DELETED, which can
/// differ if a run went terminal, or was pruned by another operator, in between. Reporting
/// the intent instead of the effect is precisely what the rest of this module refuses to do.
pub async fn prune(
    store: &dyn SchedulerStore,
    older_than: chrono::Duration,
    now: DateTime<Utc>,
    yes: bool,
    confirm: &mut dyn FnMut(&str) -> bool,
) -> Result<Outcome, CliError> {
    // `parse_retention` already bounds the window, so this cannot realistically fire — but
    // `now - window` is still fallible arithmetic, and a panic in a delete command is not an
    // acceptable way to find that out.
    let cutoff = now.checked_sub_signed(older_than).ok_or_else(|| {
        CliError::error(format!(
            "retention window {older_than} cannot be subtracted from {}",
            fmt_cutoff(now)
        ))
    })?;

    let counted = store.count_terminal_before(cutoff).await?;
    if counted == 0 {
        // Nothing to consent to, so nothing is asked. Exit 0: the state the operator asked
        // for already holds.
        return Ok(Outcome::ok(format!(
            "nothing to prune: no completed, failed or cancelled run last changed before {}",
            fmt_cutoff(cutoff)
        )));
    }

    if !yes {
        let text = format!(
            "prune will DELETE {counted} terminal run record(s) — completed, failed or \
             cancelled — last changed before {}.\nPaused and waking runs are never \
             eligible and are not counted here.\nThis deletes durable rows and cannot be \
             undone.",
            fmt_cutoff(cutoff)
        );
        if !confirm(&text) {
            // `confirm` already showed `text` to the operator — do not repeat it.
            return Ok(Outcome::precondition(format!(
                "refused: nothing deleted, {counted} terminal run record(s) still stored"
            )));
        }
    }

    let deleted = store.prune_terminal(cutoff).await?;
    let mut text = format!(
        "pruned: {deleted} terminal run record(s) deleted (last changed before {})",
        fmt_cutoff(cutoff)
    );
    if deleted != counted {
        text.push_str(&format!(
            " — the preview counted {counted}; the eligible set changed in between"
        ));
    }
    Ok(Outcome::ok(text))
}

/// Submit a fresh run and drive it inline. Blocks until the run pauses or ends —
/// there is no `--detach` yet, because `enqueue` stamps the row `waking`, so a
/// detached run would only be picked up once the lease expired and the crash-reclaim
/// path grabbed it. Abusing crash recovery as a scheduling primitive was rejected;
/// a real `pending` status is the fix.
///
/// `announce` is called EXACTLY when the submit is known to be going ahead: after the
/// duplicate pre-check, before the (potentially very long) drive. `main` passes the
/// `submitted: <id>` print — WHOLE-SLICE FIX 4. It used to run before this function was
/// even entered, which announced an effect that a rejected `enqueue` never performed:
/// precisely the "report the effect, not the Ok" discipline the rest of this module
/// enforces. It must still precede the drive, so an operator who loses the terminal can
/// find the run.
pub async fn submit(
    scheduler: &orchestrator::Scheduler,
    run: RunId,
    graph: orchestrator_core::Graph,
    budget: Option<TokenBudget>,
    announce: impl FnOnce(),
) -> Result<Outcome, CliError> {
    // A run id that already has a schedule record cannot be submitted again. Left to
    // `enqueue`, this surfaces as `OrchestratorError::Store` — which §7.4 defines as a
    // RETRYABLE TRANSPORT FAULT — at exit 1, with `RunId(..)` `Debug` formatting in the
    // text. It is neither retryable nor a transport problem: the caller passed a
    // `--run-id` that is already taken, which is a precondition failure (exit 2).
    //
    // Read through the scheduler rather than a separately-passed store, deliberately:
    // `Scheduler::status` delegates to the very store its own `enqueue` will hit, so the
    // pre-check cannot be aimed at a different database than the write.
    //
    // A pre-check is a better MESSAGE, not a lock: two concurrent submits of the same id
    // can both pass it. `enqueue` remains the real guard (`on conflict do nothing` +
    // `rows_affected == 0`), so that race degrades to the old loud error rather than
    // double-enqueueing — which is why the check belongs here and not in place of it.
    if let Some(existing) = scheduler.status(run).await? {
        return Ok(Outcome::precondition(format!(
            "already submitted: {} is {} — a run id can only be submitted once. Use \
             `torii run status {}` to inspect it.",
            run.0,
            existing.status.as_str(),
            run.0
        )));
    }
    announce();
    // SP-DATA-5 Task 5: `submit_budgeted` with `None` is exactly `submit` — the
    // operator-specified cap (if any) rides on `RunStarted` from here.
    let outcome = scheduler.submit_budgeted(run, graph, budget).await?;
    if let Some(p) = &outcome.paused {
        return Ok(Outcome::ok(format!(
            "paused: {} at node {} ({})",
            run.0, p.node.0, p.reason
        )));
    }
    if let Some((node, msg)) = &outcome.failed {
        // A run that actually executed and failed is an EXECUTION error (exit 1),
        // not a precondition-not-met no-op (exit 2, this taxonomy's code for "ran
        // fine, nothing to do"): automation needs to tell "the workload failed,
        // page someone" apart from an idempotent no-op. `announce` has already
        // reached stdout by the time this returns — that is fine and intentional
        // (an operator who loses the terminal must still be able to find the run),
        // and by then the enqueue really did happen.
        return Err(CliError::error(format!(
            "failed: {} at node {} ({msg})",
            run.0, node.0
        )));
    }
    Ok(Outcome::ok(format!(
        "completed: {} ({} node(s))",
        run.0,
        outcome.completed.len()
    )))
}

/// `pub(crate)` **only so `cmd::gate`'s tests can reuse four fixtures** — `now`,
/// `paused_store`, `awaiting_journal` and `FailingForceWakeStore`. There is no
/// `tests_support` module in this crate and inventing one would mean moving fixtures
/// away from the tests that own them; re-exporting the four that a second module needs
/// is the smaller change. Copying them into `cmd::gate` was the alternative and is
/// worse: `FailingForceWakeStore` is the append-then-`force_wake` ORDER discriminator
/// for both commands, and two copies of it are two places for that guard to rot
/// independently.
#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    // The `GateAwaited` fixtures live beside `cmd::gate`'s own tests — see that module's
    // doc comment for why the reuse runs in both directions.
    use crate::cmd::gate::tests::{
        LOOP_GATE_QUESTION, gate_journal, loop_gate, loop_gate_journal, loop_gate_journal_asking,
        release,
    };
    // Likewise `cmd::human`'s `AgentAwaited` fixture: the listing tests must fold the SAME
    // shape the command that answers it folds, or the two drift silently.
    use crate::cmd::human::tests::{THE_QUESTION, agent_journal, agent_journal_asking, reviewer};
    use crate::errors::{EXIT_OK, EXIT_PRECONDITION};
    use orchestrator_core::{EffectClass, EffectId, EffectOutput, Graph, NodeId, TokenUsage};
    use orchestrator_store::{InMemoryJournal, InMemorySchedulerStore};
    use std::sync::Arc;

    pub(crate) fn now() -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(3_000_000, 0).unwrap()
    }

    fn empty_graph() -> Graph {
        Graph { nodes: vec![] }
    }

    /// A journal with nothing on it. `status`/`wake` only touch the journal when a
    /// run needs its spend folded (`status`, in the `Some(r)` branch) or a raise is
    /// requested (`wake`, when `budget` is `Some`) — most tests here exercise
    /// neither, so an empty journal is a safe, inert stand-in.
    fn empty_journal() -> InMemoryJournal {
        InMemoryJournal::new()
    }

    /// A run enqueued then recorded paused with a deadline.
    pub(crate) async fn paused_store(
        run: RunId,
        next_wake: Option<DateTime<Utc>>,
    ) -> InMemorySchedulerStore {
        let s = InMemorySchedulerStore::default();
        s.enqueue(run, &empty_graph(), now()).await.unwrap();
        s.record_paused(run, next_wake, "quota: rate limited")
            .await
            .unwrap();
        s
    }

    #[tokio::test]
    async fn status_of_an_unknown_run_is_a_precondition_failure_not_an_error() {
        let s = InMemorySchedulerStore::default();
        let out = status(&s, &empty_journal(), RunId(uuid::Uuid::new_v4()), false)
            .await
            .expect("no hard error");
        assert_eq!(out.code, EXIT_PRECONDITION);
        assert!(out.text.contains("no such run"), "{}", out.text);
    }

    /// WHOLE-SLICE FIX 5: `--json` must never put prose on stdout — `torii run status X
    /// --json | jq` has to survive the not-found case, which is the one a script polling
    /// for a run is most likely to hit first.
    #[tokio::test]
    async fn status_of_an_unknown_run_is_still_valid_json_under_json() {
        let s = InMemorySchedulerStore::default();
        let out = status(&s, &empty_journal(), RunId(uuid::Uuid::new_v4()), true)
            .await
            .expect("no hard error");
        assert_eq!(
            out.code, EXIT_PRECONDITION,
            "the exit code still says not-found: {}",
            out.text
        );
        let v: serde_json::Value = serde_json::from_str(&out.text)
            .unwrap_or_else(|e| panic!("--json emitted non-JSON {:?}: {e}", out.text));
        assert!(
            v.is_null(),
            "a run that does not exist is JSON null, not a fabricated record: {v}"
        );
    }

    #[tokio::test]
    async fn list_paused_renders_the_pending_wake_set() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let out = list_paused(&s, &empty_journal(), false)
            .await
            .expect("lists");
        assert_eq!(out.code, EXIT_OK);
        assert!(out.text.contains(&run.0.to_string()), "{}", out.text);
        assert!(out.text.contains("quota: rate limited"), "{}", out.text);
    }

    #[tokio::test]
    async fn list_paused_json_is_machine_readable() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let out = list_paused(&s, &empty_journal(), true)
            .await
            .expect("lists");
        let rows: Vec<orchestrator_core::ScheduledRun> =
            serde_json::from_str(&out.text).expect("valid json");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].next_wake, None);
    }

    #[tokio::test]
    async fn cancel_reports_the_transition_it_actually_made() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let out = cancel(&s, run).await.expect("cancels");
        assert_eq!(out.code, EXIT_OK);
        assert!(out.text.starts_with("cancelled:"), "{}", out.text);
        assert_eq!(
            s.status(run).await.unwrap().unwrap().status,
            RunStatus::Cancelled
        );
    }

    /// THE honest-reporting case: the store call SUCCEEDS on a terminal run but
    /// changes nothing. Reporting "cancelled" here would be a lie.
    #[tokio::test]
    async fn cancel_on_a_terminal_run_reports_not_cancelled() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = InMemorySchedulerStore::default();
        s.enqueue(run, &empty_graph(), now()).await.unwrap();
        s.record_terminal(run, RunStatus::Completed, None)
            .await
            .unwrap();

        let out = cancel(&s, run).await.expect("no hard error");
        assert_eq!(out.code, EXIT_PRECONDITION);
        assert!(out.text.contains("not cancelled"), "{}", out.text);
        assert!(
            out.text.contains("completed"),
            "must name the actual state: {}",
            out.text
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().status,
            RunStatus::Completed,
            "and the run really is untouched"
        );
    }

    #[tokio::test]
    async fn wake_says_queued_never_resumed() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let out = wake(&s, &empty_journal(), run, now(), None)
            .await
            .expect("wakes");
        assert_eq!(out.code, EXIT_OK);
        assert!(out.text.contains("queued"), "{}", out.text);
        assert!(
            !out.text.contains("resumed") && !out.text.contains("woken"),
            "force_wake only sets next_wake; a worker tick does the driving: {}",
            out.text
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            Some(now()),
            "the NULL deadline is now set to now, so the next tick claims it"
        );
    }

    #[tokio::test]
    async fn wake_on_a_non_paused_run_reports_not_queued() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = InMemorySchedulerStore::default();
        s.enqueue(run, &empty_graph(), now()).await.unwrap(); // status = waking, not paused
        let out = wake(&s, &empty_journal(), run, now(), None)
            .await
            .expect("no hard error");
        assert_eq!(out.code, EXIT_PRECONDITION);
        assert!(out.text.contains("not queued"), "{}", out.text);
        assert!(
            out.text.contains("waking"),
            "must name the actual state: {}",
            out.text
        );
    }

    // ---- SP-DATA-5 Task 5: `--budget-tokens` on submit/wake, spent/budget in status ---

    #[test]
    fn parse_budget_tokens_rejects_zero_with_an_actionable_message() {
        let e = parse_budget_tokens("0").expect_err("a zero budget can never dispatch anything");
        assert!(e.contains("--budget-tokens"), "{e}");
        assert!(e.contains('0'), "{e}");
    }

    #[test]
    fn parse_budget_tokens_accepts_a_positive_value() {
        assert_eq!(parse_budget_tokens("50000"), Ok(50_000));
        assert_eq!(parse_budget_tokens(" 12 "), Ok(12), "whitespace is trimmed");
    }

    #[test]
    fn parse_budget_tokens_rejects_garbage_loudly() {
        let e = parse_budget_tokens("many").expect_err("not a number");
        assert!(e.contains("--budget-tokens"), "{e}");
        assert!(e.contains("many"), "must echo the offending value: {e}");
        assert!(
            parse_budget_tokens("-5").is_err(),
            "negative is not a token count"
        );
        assert!(parse_budget_tokens("").is_err());
    }

    /// A journal seeded with `RunStarted{budget}` plus two `EffectRecorded{usage}` — the
    /// DB-free harness the task calls for, proving `status` displays spend without a live
    /// Postgres.
    async fn journal_with_budget_and_spend(run: RunId, cap: u64) -> InMemoryJournal {
        let journal = InMemoryJournal::new();
        journal
            .append(
                run,
                JournalEvent::RunStarted {
                    version: "v1".into(),
                    budget: Some(TokenBudget { total_tokens: cap }),
                },
            )
            .await
            .unwrap();
        journal
            .append(
                run,
                JournalEvent::EffectRecorded {
                    node: NodeId("n1".into()),
                    effect_id: EffectId("e1".into()),
                    class: EffectClass::Pure,
                    input_hash: "h".into(),
                    seq: 0,
                    output: EffectOutput::Inline(serde_json::Value::Null),
                    observation: None,
                    usage: Some(TokenUsage {
                        input_tokens: 100,
                        output_tokens: 50,
                        total_tokens: 150,
                    }),
                },
            )
            .await
            .unwrap();
        journal
            .append(
                run,
                JournalEvent::EffectRecorded {
                    node: NodeId("n2".into()),
                    effect_id: EffectId("e2".into()),
                    class: EffectClass::Pure,
                    input_hash: "h".into(),
                    seq: 1,
                    output: EffectOutput::Inline(serde_json::Value::Null),
                    observation: None,
                    usage: Some(TokenUsage {
                        input_tokens: 20,
                        output_tokens: 30,
                        total_tokens: 50,
                    }),
                },
            )
            .await
            .unwrap();
        journal
    }

    #[tokio::test]
    async fn status_shows_spent_and_budget_when_a_budget_is_set() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let journal = journal_with_budget_and_spend(run, 50_000).await;

        let out = status(&s, &journal, run, false).await.expect("status");
        assert_eq!(out.code, EXIT_OK);
        assert!(
            out.text.contains("200") && out.text.contains("50000"),
            "spent (150+50 = 200, summed over two distinct effects' total_tokens) and the \
             50000 cap must both be visible: {}",
            out.text
        );
    }

    #[tokio::test]
    async fn status_json_includes_spent_and_budget_when_a_budget_is_set() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let journal = journal_with_budget_and_spend(run, 50_000).await;

        let out = status(&s, &journal, run, true).await.expect("status");
        assert_eq!(out.code, EXIT_OK);
        let v: serde_json::Value = serde_json::from_str(&out.text).expect("valid json");
        assert_eq!(v[0]["spent"], serde_json::json!(200));
        assert_eq!(v[0]["budget"], serde_json::json!(50_000));
    }

    /// Additivity, at the command level: an UNBUDGETED run's `status` table must be
    /// BYTE-IDENTICAL to `render::table` alone — nothing appended, nothing reordered.
    #[tokio::test]
    async fn status_table_is_byte_identical_for_an_unbudgeted_run() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        // No `RunStarted` at all ⇒ folds to `budget: None` — the same as a run whose
        // `RunStarted.budget` is explicitly `None` (Task 1's additivity case).
        let journal = empty_journal();

        let out = status(&s, &journal, run, false).await.expect("status");
        let row = s.status(run).await.unwrap().unwrap();
        assert_eq!(
            out.text,
            render::table(&[row]),
            "an unbudgeted run must render EXACTLY what the pre-SP-DATA-5 table did"
        );
    }

    /// The JSON counterpart of the byte-identical guarantee above.
    #[tokio::test]
    async fn status_json_is_byte_identical_for_an_unbudgeted_run() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let journal = empty_journal();

        let out = status(&s, &journal, run, true).await.expect("status");
        let row = s.status(run).await.unwrap().unwrap();
        assert_eq!(
            out.text,
            render::json(&[row]).unwrap(),
            "an unbudgeted run's --json must be EXACTLY what the pre-SP-DATA-5 render did"
        );
    }

    #[tokio::test]
    async fn wake_with_budget_tokens_appends_budget_raised_to_the_journal() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let journal = empty_journal();

        let out = wake(
            &s,
            &journal,
            run,
            now(),
            Some(TokenBudget {
                total_tokens: 5_000,
            }),
        )
        .await
        .expect("wakes");
        assert_eq!(out.code, EXIT_OK, "{}", out.text);

        let events = journal.load(run).await.unwrap();
        assert!(
            events.iter().any(|(_, e)| matches!(
                e,
                JournalEvent::BudgetRaised {
                    new_total_tokens: 5_000
                }
            )),
            "the raise must be journaled: {events:?}"
        );
    }

    #[tokio::test]
    async fn wake_without_budget_tokens_appends_nothing_to_the_journal() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let journal = empty_journal();

        let out = wake(&s, &journal, run, now(), None).await.expect("wakes");
        assert_eq!(out.code, EXIT_OK, "{}", out.text);
        assert!(
            journal.load(run).await.unwrap().is_empty(),
            "no --budget-tokens ⇒ no journal write at all — unbudgeted wake stays exactly \
             the pre-SP-DATA-5 behavior"
        );
    }

    /// A `SchedulerStore` that delegates everything to a real `InMemorySchedulerStore`
    /// EXCEPT `force_wake`, which always fails. This is the discriminator for the
    /// append-then-wake ORDER, not just its presence: if `wake()` called `force_wake`
    /// BEFORE appending `BudgetRaised` (the swapped, wrong order), the injected failure
    /// here would short-circuit via `?` and the append would never run — the journal
    /// would end up with NO `BudgetRaised` event at all. Because the real order appends
    /// first, the event lands durably even though the subsequent `force_wake` fails.
    pub(crate) struct FailingForceWakeStore(pub(crate) InMemorySchedulerStore);

    #[async_trait::async_trait]
    impl SchedulerStore for FailingForceWakeStore {
        async fn enqueue(
            &self,
            run: RunId,
            graph: &Graph,
            now: DateTime<Utc>,
        ) -> Result<(), OrchestratorError> {
            self.0.enqueue(run, graph, now).await
        }
        async fn record_paused(
            &self,
            run: RunId,
            next_wake: Option<DateTime<Utc>>,
            reason: &str,
        ) -> Result<(), OrchestratorError> {
            self.0.record_paused(run, next_wake, reason).await
        }
        async fn record_terminal(
            &self,
            run: RunId,
            status: RunStatus,
            reason: Option<&str>,
        ) -> Result<(), OrchestratorError> {
            self.0.record_terminal(run, status, reason).await
        }
        async fn claim_due(
            &self,
            now: DateTime<Utc>,
            lease: chrono::Duration,
            limit: usize,
        ) -> Result<Vec<(RunId, Graph)>, OrchestratorError> {
            self.0.claim_due(now, lease, limit).await
        }
        async fn status(
            &self,
            run: RunId,
        ) -> Result<Option<orchestrator_core::ScheduledRun>, OrchestratorError> {
            self.0.status(run).await
        }
        async fn list_paused(
            &self,
        ) -> Result<Vec<orchestrator_core::ScheduledRun>, OrchestratorError> {
            self.0.list_paused().await
        }
        async fn cancel(&self, run: RunId) -> Result<(), OrchestratorError> {
            self.0.cancel(run).await
        }
        async fn count_terminal_before(
            &self,
            before: DateTime<Utc>,
        ) -> Result<u64, OrchestratorError> {
            self.0.count_terminal_before(before).await
        }
        async fn prune_terminal(&self, before: DateTime<Utc>) -> Result<u64, OrchestratorError> {
            self.0.prune_terminal(before).await
        }
        async fn force_wake(
            &self,
            _run: RunId,
            _now: DateTime<Utc>,
        ) -> Result<(), OrchestratorError> {
            Err(OrchestratorError::Store(
                "injected: proves append-then-wake ordering".into(),
            ))
        }
    }

    #[tokio::test]
    async fn wake_appends_budget_raised_before_calling_force_wake() {
        let run = RunId(uuid::Uuid::new_v4());
        let inner = paused_store(run, Some(now())).await;
        let store = FailingForceWakeStore(inner);
        let journal = empty_journal();

        let result = wake(
            &store,
            &journal,
            run,
            now(),
            Some(TokenBudget {
                total_tokens: 5_000,
            }),
        )
        .await;

        assert!(
            result.is_err(),
            "the injected force_wake failure must surface, not be swallowed"
        );
        let events = journal.load(run).await.unwrap();
        assert!(
            events.iter().any(|(_, e)| matches!(
                e,
                JournalEvent::BudgetRaised {
                    new_total_tokens: 5_000
                }
            )),
            "BudgetRaised must already be durable even though force_wake failed — proving \
             the append happens BEFORE force_wake is called, not after: {events:?}"
        );
    }

    // ---- SP-DATA-4.1 #7: `torii run prune` -------------------------------------------

    /// A confirmer that must never be reached. Used on the paths where asking would itself
    /// be the bug (`--yes`, and "nothing to prune").
    fn never_asked() -> impl FnMut(&str) -> bool {
        |text: &str| panic!("the operator must not be prompted here, but saw: {text}")
    }

    /// A store holding `n` terminal runs whose `updated_at` is `at` (this store stamps it at
    /// `enqueue`), plus optionally a paused and a waking run at the same instant.
    async fn prunable_store(
        terminal: usize,
        at: DateTime<Utc>,
        with_live_runs: bool,
    ) -> (InMemorySchedulerStore, Vec<RunId>, Vec<RunId>) {
        let s = InMemorySchedulerStore::default();
        let mut dead = Vec::new();
        for _ in 0..terminal {
            let r = RunId(uuid::Uuid::new_v4());
            s.enqueue(r, &empty_graph(), at).await.unwrap();
            s.record_terminal(r, RunStatus::Completed, None)
                .await
                .unwrap();
            dead.push(r);
        }
        let mut live = Vec::new();
        if with_live_runs {
            let paused = RunId(uuid::Uuid::new_v4());
            s.enqueue(paused, &empty_graph(), at).await.unwrap();
            // The in-doubt class: NULL deadline, waits indefinitely for a human.
            s.record_paused(paused, None, "in-doubt mutation")
                .await
                .unwrap();
            let waking = RunId(uuid::Uuid::new_v4());
            s.enqueue(waking, &empty_graph(), at).await.unwrap();
            live.push(paused);
            live.push(waking);
        }
        (s, dead, live)
    }

    /// A day before `now()`, i.e. comfortably outside any window the tests below use.
    fn long_ago() -> DateTime<Utc> {
        now() - chrono::Duration::days(30)
    }

    #[tokio::test]
    async fn prune_reports_the_number_actually_deleted() {
        let (s, dead, _) = prunable_store(3, long_ago(), false).await;
        let out = prune(
            &s,
            chrono::Duration::days(7),
            now(),
            true,
            &mut never_asked(),
        )
        .await
        .expect("prunes");
        assert_eq!(out.code, EXIT_OK);
        assert!(out.text.contains('3'), "must name the effect: {}", out.text);
        assert!(out.text.starts_with("pruned:"), "{}", out.text);
        for r in dead {
            assert!(s.status(r).await.unwrap().is_none(), "the row is gone");
        }
    }

    /// THE safety property, at the command level: `prune` must not be able to delete live
    /// work even when the operator passes `--yes` and a window that every row is older than.
    #[tokio::test]
    async fn prune_never_deletes_a_paused_or_waking_run() {
        let (s, dead, live) = prunable_store(1, long_ago(), true).await;
        let out = prune(
            &s,
            chrono::Duration::seconds(1),
            now(),
            true,
            &mut never_asked(),
        )
        .await
        .expect("prunes");
        assert_eq!(out.code, EXIT_OK);
        assert!(
            out.text.contains('1'),
            "exactly the one terminal row: {}",
            out.text
        );
        assert!(s.status(dead[0]).await.unwrap().is_none());
        assert_eq!(
            s.status(live[0]).await.unwrap().unwrap().status,
            RunStatus::Paused,
            "a NULL-deadline pause is live work awaiting a human, at any age"
        );
        assert_eq!(
            s.status(live[1]).await.unwrap().unwrap().status,
            RunStatus::Waking,
            "a waking row may be a live lease in another process"
        );
    }

    /// Nothing to delete must not put a destructive prompt in front of an operator, and it
    /// is not a failure — the state they asked for already holds.
    #[tokio::test]
    async fn prune_with_nothing_eligible_exits_zero_without_prompting() {
        let (s, _, live) = prunable_store(0, long_ago(), true).await;
        let out = prune(
            &s,
            chrono::Duration::days(7),
            now(),
            false, // NOT --yes: the prompt is only skipped because the count is zero
            &mut never_asked(),
        )
        .await
        .expect("no hard error");
        assert_eq!(out.code, EXIT_OK);
        assert!(out.text.contains("nothing to prune"), "{}", out.text);
        assert_eq!(
            s.status(live[0]).await.unwrap().unwrap().status,
            RunStatus::Paused
        );
    }

    /// A terminal row INSIDE the retention window is not eligible — the window is the whole
    /// point, so a prune that ignored it would still pass every "deletes terminal rows" test.
    #[tokio::test]
    async fn prune_keeps_a_terminal_row_inside_the_retention_window() {
        let (s, dead, _) = prunable_store(1, now() - chrono::Duration::hours(1), false).await;
        let out = prune(
            &s,
            chrono::Duration::days(7),
            now(),
            true,
            &mut never_asked(),
        )
        .await
        .expect("no hard error");
        assert_eq!(out.code, EXIT_OK);
        assert!(out.text.contains("nothing to prune"), "{}", out.text);
        assert!(
            s.status(dead[0]).await.unwrap().is_some(),
            "an hour-old terminal row is inside a 7-day window"
        );
    }

    /// The operator sees the count BEFORE consenting — a confirmation prompt that does not
    /// disclose the size of the delete is not informed consent.
    #[tokio::test]
    async fn prune_shows_the_count_in_the_prompt_before_asking() {
        let (s, _, _) = prunable_store(4, long_ago(), false).await;
        let mut prompt = String::new();
        {
            let mut confirm = |text: &str| {
                prompt = text.to_string();
                true
            };
            let out = prune(&s, chrono::Duration::days(7), now(), false, &mut confirm)
                .await
                .expect("prunes");
            assert_eq!(out.code, EXIT_OK);
        }
        assert!(prompt.contains('4'), "the count must be shown: {prompt}");
        assert!(
            prompt.to_lowercase().contains("delete"),
            "the prompt must say what it does: {prompt}"
        );
        assert!(
            prompt.contains("cannot be undone"),
            "a durable delete must say so: {prompt}"
        );
    }

    /// Declining must delete nothing. `interactive_confirm` returns false on EOF, so this is
    /// also the shape a non-interactive `torii run prune` (cron, `< /dev/null`) takes.
    #[tokio::test]
    async fn prune_refused_at_the_prompt_deletes_nothing() {
        let (s, dead, _) = prunable_store(2, long_ago(), false).await;
        let mut confirm = |_: &str| false;
        let out = prune(&s, chrono::Duration::days(7), now(), false, &mut confirm)
            .await
            .expect("no hard error");
        assert_eq!(out.code, EXIT_PRECONDITION);
        assert!(out.text.contains("refused"), "{}", out.text);
        assert!(out.text.contains("nothing deleted"), "{}", out.text);
        for r in dead {
            assert!(
                s.status(r).await.unwrap().is_some(),
                "a declined prune must leave every row in place"
            );
        }
    }

    /// The number reported must be the EFFECT, never the preview. `confirm` is the only hook
    /// that lands between the count and the delete — exactly the window a human spends
    /// reading the prompt — so another operator's prune runs there, on its own thread and
    /// runtime (the same technique `cmd::config`'s push-race test uses, `confirm` being
    /// sync). By the time this prune fires there is nothing left, and reporting the
    /// previewed 2 would be a lie. Without this, an implementation that simply echoed
    /// `counted` would pass every other test here.
    #[tokio::test]
    async fn prune_reports_what_it_deleted_not_what_it_previewed() {
        let (s, dead, _) = prunable_store(2, long_ago(), false).await;
        let window = chrono::Duration::days(7);
        let cutoff = now() - window;
        let mut confirm = {
            let s = s.clone(); // `Clone` shares the one Arc-backed map
            move |_: &str| {
                let s = s.clone();
                std::thread::spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    rt.block_on(async move { s.prune_terminal(cutoff).await.unwrap() })
                })
                .join()
                .expect("the concurrent prune must not panic");
                true
            }
        };

        let out = prune(&s, window, now(), false, &mut confirm)
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_OK);
        assert!(
            out.text.contains("pruned: 0"),
            "must report what it actually deleted, not the 2 it previewed: {}",
            out.text
        );
        assert!(
            out.text.contains("preview counted 2"),
            "and must say why the two numbers differ: {}",
            out.text
        );
        for r in dead {
            assert!(
                s.status(r).await.unwrap().is_none(),
                "the other operator's prune really did take them"
            );
        }
    }

    #[test]
    fn parse_retention_accepts_days_hours_minutes_and_seconds() {
        assert_eq!(parse_retention("30d"), Ok(chrono::Duration::days(30)));
        assert_eq!(parse_retention("12h"), Ok(chrono::Duration::hours(12)));
        assert_eq!(parse_retention("90m"), Ok(chrono::Duration::minutes(90)));
        assert_eq!(parse_retention("45s"), Ok(chrono::Duration::seconds(45)));
        assert_eq!(parse_retention(" 30d "), Ok(chrono::Duration::days(30)));
    }

    #[test]
    fn parse_retention_rejects_garbage_loudly() {
        assert!(parse_retention("soon").is_err());
        assert!(parse_retention("30").is_err(), "a bare number has no unit");
        assert!(parse_retention("").is_err());
        assert!(parse_retention("d").is_err(), "a bare unit has no number");
        // Milliseconds are a poll-interval unit, not a retention unit — and the complaint
        // must name the unit rather than claiming "500m is not a number".
        let e = parse_retention("500ms").expect_err("ms is not a retention unit");
        assert!(e.contains("\"ms\""), "{e}");
    }

    #[test]
    fn parse_retention_rejects_a_zero_window() {
        assert!(parse_retention("0d").is_err());
        assert!(parse_retention("0s").is_err());
    }

    #[test]
    fn parse_retention_rejects_an_overflow_instead_of_panicking() {
        // `chrono::Duration::try_days` returns None rather than panicking on these.
        assert!(parse_retention("999999999999999999d").is_err());
        assert!(
            parse_retention("99999999999999999999999d").is_err(),
            "past i64 entirely"
        );
    }

    /// The reason `parse_retention` is a sibling rather than an extension: `--interval 30d`
    /// must stay illegal. If someone later "unifies" the two parsers, this fails.
    #[test]
    fn the_poll_interval_parser_still_refuses_a_retention_unit() {
        assert!(
            crate::cmd::worker::parse_interval("30d").is_err(),
            "a month-long poll interval must not become legal"
        );
    }

    // ---- WHOLE-SLICE FIX 4: a duplicate submit ---------------------------------------

    /// A real `Scheduler` over in-memory backends. No database, no live provider: the
    /// recording gateway is never reached on the duplicate path (the pre-check returns
    /// first), and the empty graph spends nothing on the fresh path.
    async fn scheduler_over(store: Arc<InMemorySchedulerStore>) -> orchestrator::Scheduler {
        let journal = Arc::new(orchestrator_store::InMemoryJournal::new());
        let (gw, _calls) = orchestrator::test_support::recording_gateway().await;
        let clock = orchestrator::test_support::FakeClock::new(now());
        let exec = orchestrator::Executor::new(Arc::new(gw), journal.clone(), "v1");
        orchestrator::Scheduler::new(store, exec, journal, clock)
    }

    /// Re-submitting a taken run id must be a PRECONDITION failure, and must not have
    /// announced `submitted:` first.
    ///
    /// Both halves were wrong before: `enqueue`'s refusal surfaced as
    /// `OrchestratorError::Store` — which the CLI's taxonomy (§7.4) reserves for a
    /// RETRYABLE TRANSPORT FAULT — at exit 1, carrying `RunId(..)` `Debug` formatting; and
    /// `main` had already printed `submitted: <id>` for a run that was never enqueued.
    #[tokio::test]
    async fn a_duplicate_submit_is_a_precondition_failure_and_announces_nothing() {
        let run = RunId(uuid::Uuid::new_v4());
        let store = Arc::new(InMemorySchedulerStore::default());
        store.enqueue(run, &empty_graph(), now()).await.unwrap();
        let sched = scheduler_over(store.clone()).await;

        let mut announced = false;
        let out = submit(&sched, run, empty_graph(), None, || announced = true)
            .await
            .expect("a duplicate submit is not a transport fault");

        assert_eq!(
            out.code, EXIT_PRECONDITION,
            "a taken run id is a precondition failure, not a retryable transport fault: {}",
            out.text
        );
        assert!(out.text.contains("already submitted"), "{}", out.text);
        assert!(
            out.text.contains("waking"),
            "must name the run's actual state: {}",
            out.text
        );
        assert!(
            out.text.contains(&run.0.to_string()) && !out.text.contains("RunId("),
            "the id must be the plain uuid, not `RunId(..)` Debug formatting: {}",
            out.text
        );
        assert!(
            !announced,
            "an effect that never happened must never be announced"
        );
    }

    /// The counterpart, so the fix cannot be satisfied by never announcing at all:
    /// a FIRST submit announces (and structurally does so before the drive — `announce()`
    /// is the statement immediately preceding `scheduler.submit`).
    #[tokio::test]
    async fn a_fresh_submit_announces_and_then_drives() {
        let run = RunId(uuid::Uuid::new_v4());
        let store = Arc::new(InMemorySchedulerStore::default());
        let sched = scheduler_over(store.clone()).await;

        let mut announced = false;
        let out = submit(&sched, run, empty_graph(), None, || announced = true)
            .await
            .expect("a fresh submit runs");

        assert!(announced, "the operator must get the id");
        assert_eq!(out.code, EXIT_OK, "{}", out.text);
        assert!(out.text.starts_with("completed:"), "{}", out.text);
        assert!(
            store.status(run).await.unwrap().is_some(),
            "and the enqueue really happened"
        );
    }

    /// Which concurrent actor lands in the gap between `wake`'s pre-check (`status`)
    /// and its own `force_wake` call.
    #[derive(Clone, Copy)]
    enum ConcurrentActor {
        /// A worker's tick claims the same due pause first (`paused -> waking`).
        ClaimsFirst,
        /// Another operator cancels the run first (`paused -> cancelled`).
        CancelsFirst,
        /// A worker's tick claims it, wake()'s OWN `force_wake` lands while it is
        /// `waking` (a no-op), and THEN the executor's drive finishes and re-pauses
        /// it with a fresh, UNRELATED deadline — landing status back on `paused`
        /// before `wake`'s post-check re-reads it. Proves the status check alone is
        /// not enough: only the timestamp half catches this.
        ReclaimsThenRepausesWithUnrelatedDeadline,
    }

    /// Delegates to a real `InMemorySchedulerStore` for everything, EXCEPT that its
    /// `force_wake` runs `actor` against `run` first. `wake()` calls `store.status`
    /// (the pre-check) and only then `store.force_wake` — so running the concurrent
    /// actor at the top of THIS `force_wake` lands it exactly in that gap, reproducing
    /// a real multi-process race deterministically, single-threaded, no database.
    struct RacingStore {
        inner: InMemorySchedulerStore,
        run: RunId,
        actor: ConcurrentActor,
    }

    #[async_trait::async_trait]
    impl SchedulerStore for RacingStore {
        async fn enqueue(
            &self,
            run: RunId,
            graph: &Graph,
            now: DateTime<Utc>,
        ) -> Result<(), orchestrator_core::OrchestratorError> {
            self.inner.enqueue(run, graph, now).await
        }
        async fn record_paused(
            &self,
            run: RunId,
            next_wake: Option<DateTime<Utc>>,
            reason: &str,
        ) -> Result<(), orchestrator_core::OrchestratorError> {
            self.inner.record_paused(run, next_wake, reason).await
        }
        async fn record_terminal(
            &self,
            run: RunId,
            status: RunStatus,
            reason: Option<&str>,
        ) -> Result<(), orchestrator_core::OrchestratorError> {
            self.inner.record_terminal(run, status, reason).await
        }
        async fn claim_due(
            &self,
            now: DateTime<Utc>,
            lease: chrono::Duration,
            limit: usize,
        ) -> Result<Vec<(RunId, Graph)>, orchestrator_core::OrchestratorError> {
            self.inner.claim_due(now, lease, limit).await
        }
        async fn status(
            &self,
            run: RunId,
        ) -> Result<Option<orchestrator_core::ScheduledRun>, orchestrator_core::OrchestratorError>
        {
            self.inner.status(run).await
        }
        async fn list_paused(
            &self,
        ) -> Result<Vec<orchestrator_core::ScheduledRun>, orchestrator_core::OrchestratorError>
        {
            self.inner.list_paused().await
        }
        async fn cancel(&self, run: RunId) -> Result<(), orchestrator_core::OrchestratorError> {
            self.inner.cancel(run).await
        }
        async fn count_terminal_before(
            &self,
            before: DateTime<Utc>,
        ) -> Result<u64, orchestrator_core::OrchestratorError> {
            self.inner.count_terminal_before(before).await
        }
        async fn prune_terminal(
            &self,
            before: DateTime<Utc>,
        ) -> Result<u64, orchestrator_core::OrchestratorError> {
            self.inner.prune_terminal(before).await
        }
        async fn force_wake(
            &self,
            run: RunId,
            now: DateTime<Utc>,
        ) -> Result<(), orchestrator_core::OrchestratorError> {
            if run != self.run {
                return self.inner.force_wake(run, now).await;
            }
            match self.actor {
                ConcurrentActor::ClaimsFirst => {
                    self.inner
                        .claim_due(now, chrono::Duration::seconds(60), 10)
                        .await?;
                    self.inner.force_wake(run, now).await
                }
                ConcurrentActor::CancelsFirst => {
                    self.inner.cancel(run).await?;
                    self.inner.force_wake(run, now).await
                }
                ConcurrentActor::ReclaimsThenRepausesWithUnrelatedDeadline => {
                    self.inner
                        .claim_due(now, chrono::Duration::seconds(60), 10)
                        .await?;
                    // This IS wake()'s own force_wake call — it lands while the row
                    // is `waking` (conditional on `paused`), so it is a no-op.
                    self.inner.force_wake(run, now).await?;
                    // The executor's drive finishes and re-pauses with a fresh,
                    // UNRELATED deadline (mirrors a journaled `RunPaused.resume_after`
                    // backoff — realistically seconds-to-minutes out, from a
                    // different process than the CLI's own `now`).
                    self.inner
                        .record_paused(
                            run,
                            Some(now + chrono::Duration::minutes(5)),
                            "unrelated re-pause",
                        )
                        .await
                }
            }
        }
    }

    /// FALSE POSITIVE reproduction: a worker's `claim_due` claims the same overdue
    /// pause in the window between `wake`'s pre-check and its `force_wake`. The old
    /// `is_some()` check reported success (`next_wake` survives the claim untouched);
    /// the run was ALREADY being driven and torii's own call changed nothing.
    #[tokio::test]
    async fn wake_reports_not_queued_when_a_concurrent_claim_wins_the_race() {
        let run = RunId(uuid::Uuid::new_v4());
        // Overdue: next_wake <= now, so the injected claim_due actually claims it.
        let inner = paused_store(run, Some(now())).await;
        let racing = RacingStore {
            inner: inner.clone(),
            run,
            actor: ConcurrentActor::ClaimsFirst,
        };

        let out = wake(&racing, &empty_journal(), run, now(), None)
            .await
            .expect("no hard error");

        assert_eq!(
            out.code, EXIT_PRECONDITION,
            "a claimed run must NOT be reported as a successful wake: {}",
            out.text
        );
        assert!(out.text.contains("not queued"), "{}", out.text);
        assert!(
            out.text.contains("waking"),
            "must name the real state, not a proxy: {}",
            out.text
        );
        assert_eq!(
            inner.status(run).await.unwrap().unwrap().status,
            RunStatus::Waking,
            "the claim, not our force_wake, owns this run now"
        );
    }

    /// MISLEADING FAILURE reproduction: another operator's `cancel` wins the race.
    /// The old code read the resulting NULL `next_wake` and reported the generic
    /// "still has no wake deadline" (the retryable HOTL phrasing) — hiding that the
    /// run was actually CANCELLED and retrying will no-op forever.
    #[tokio::test]
    async fn wake_reports_not_queued_when_a_concurrent_cancel_wins_the_race() {
        let run = RunId(uuid::Uuid::new_v4());
        let inner = paused_store(run, Some(now())).await;
        let racing = RacingStore {
            inner: inner.clone(),
            run,
            actor: ConcurrentActor::CancelsFirst,
        };

        let out = wake(&racing, &empty_journal(), run, now(), None)
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION);
        assert!(out.text.contains("not queued"), "{}", out.text);
        assert!(
            out.text.contains("cancelled"),
            "must name the true reason (cancelled), not a generic NULL-deadline phrase: {}",
            out.text
        );
        assert_eq!(
            inner.status(run).await.unwrap().unwrap().status,
            RunStatus::Cancelled
        );
    }

    /// Guards the TIMESTAMP half of the check specifically — `status == Paused` alone
    /// is NOT enough. A `paused -> waking -> paused` round trip (a claim, then the
    /// executor's own re-pause) lands the row back on `Paused` with a fresh,
    /// UNRELATED deadline. Our own `force_wake` landed while the row was `waking` and
    /// never applied, but by the time `wake` re-reads it, status is `Paused` again —
    /// purely because of someone else's unrelated pause, not our call. Without the
    /// timestamp condition this would report a false success.
    #[tokio::test]
    async fn wake_reports_not_queued_when_a_re_pause_restores_paused_with_an_unrelated_deadline() {
        let run = RunId(uuid::Uuid::new_v4());
        let inner = paused_store(run, Some(now())).await;
        let racing = RacingStore {
            inner: inner.clone(),
            run,
            actor: ConcurrentActor::ReclaimsThenRepausesWithUnrelatedDeadline,
        };

        let out = wake(&racing, &empty_journal(), run, now(), None)
            .await
            .expect("no hard error");

        assert_eq!(
            out.code, EXIT_PRECONDITION,
            "our force_wake never applied; an unrelated re-pause landing inside the \
             race window must not be reported as success: {}",
            out.text
        );
        assert!(out.text.contains("not queued"), "{}", out.text);
        assert!(
            out.text.contains("paused"),
            "must name the real state: {}",
            out.text
        );
        assert_eq!(
            inner.status(run).await.unwrap().unwrap().status,
            RunStatus::Paused,
            "the row IS paused again — just not because of our force_wake"
        );
    }

    // ---- SP-6 s1 Task 4: `torii run signal` ------------------------------------------

    fn gate() -> NodeId {
        NodeId("gate".into())
    }

    /// A journal seeded with a node that has begun awaiting a signal — the state the
    /// executor's `run_await_signal` leaves behind on its first execution.
    pub(crate) async fn awaiting_journal(
        run: RunId,
        node: &NodeId,
        deadline: Option<DateTime<Utc>>,
    ) -> InMemoryJournal {
        let j = InMemoryJournal::new();
        j.append(
            run,
            JournalEvent::SignalAwaited {
                node: node.clone(),
                deadline,
            },
        )
        .await
        .unwrap();
        j.append(
            run,
            JournalEvent::RunPaused {
                reason: format!("await_signal: waiting for a signal on node {}", node.0),
                resume_after: deadline,
            },
        )
        .await
        .unwrap();
        j
    }

    /// Every `SignalReceived` payload journaled for `node`, in journal order.
    async fn journaled_signals(
        j: &InMemoryJournal,
        run: RunId,
        node: &NodeId,
    ) -> Vec<serde_json::Value> {
        j.load(run)
            .await
            .unwrap()
            .into_iter()
            .filter_map(|(_, e)| match e {
                JournalEvent::SignalReceived { node: n, payload } if &n == node => Some(payload),
                _ => None,
            })
            .collect()
    }

    /// The blackboard publish an executor writes when a node COMPLETES
    /// (`publish_context`, keyed by node id) — the durable, node-keyed marker torii reads
    /// to tell a completed `AwaitSignal` node from one still awaiting.
    /// The durable marker that a waiting node COMPLETED — the blackboard publish every
    /// completed node makes, since neither an `AwaitSignal` nor a `HumanGate` journals a
    /// `NodeCompleted`. `pub(crate)` so `cmd::gate`'s racing test models a drive exactly as
    /// this module's does; two hand-rolled `ContextWrite` shapes would be two places for
    /// the completion marker `signal_states` reads to drift.
    pub(crate) async fn append_completion(j: &InMemoryJournal, run: RunId, node: &NodeId) {
        j.append(
            run,
            JournalEvent::ContextWrite {
                scope: orchestrator_core::Scope::Run,
                key: orchestrator_core::ContextKey(node.0.clone()),
                content: orchestrator_core::ContentRef {
                    digest: orchestrator_core::Digest("d".into()),
                    size: 3,
                    summary: None,
                },
                summary: None,
                seq: 0,
            },
        )
        .await
        .unwrap();
    }

    fn approved() -> serde_json::Value {
        serde_json::json!({"decision": "approved"})
    }

    /// THE happy path, asserted by the OBSERVED state rather than by the call's `Ok`:
    /// the payload is durable AND the run is queued for the next tick.
    #[tokio::test]
    async fn signal_appends_signal_received_and_reports_the_node() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = awaiting_journal(run, &gate(), None).await;

        let out = signal(&s, &j, run, gate(), approved(), now())
            .await
            .expect("delivers");

        assert_eq!(out.code, EXIT_OK, "{}", out.text);
        assert!(out.text.contains("signalled"), "{}", out.text);
        assert!(
            out.text.contains("gate"),
            "must name the node: {}",
            out.text
        );
        assert!(
            !out.text.contains("resumed"),
            "a signal does not resume the run; a worker tick drives it: {}",
            out.text
        );
        // Observed state, not the Ok: the payload really is durable...
        let signals = journaled_signals(&j, run, &gate()).await;
        assert_eq!(signals.len(), 1, "exactly one delivery: {signals:?}");
        assert_eq!(signals[0]["decision"], "approved");
        // ...and the never-auto-woken pause really is queued for the next tick.
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            Some(now()),
            "a NULL-deadline gate is never claimed unless the signal queues it"
        );
    }

    /// §6.6: once the node has completed it never re-reads the fold for a NEW answer,
    /// so claiming the signal landed would be a lie.
    #[tokio::test]
    async fn signal_on_a_completed_node_reports_not_delivered() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = awaiting_journal(run, &gate(), None).await;
        j.append(
            run,
            JournalEvent::SignalReceived {
                node: gate(),
                payload: approved(),
            },
        )
        .await
        .unwrap();
        append_completion(&j, run, &gate()).await;

        let out = signal(
            &s,
            &j,
            run,
            gate(),
            serde_json::json!({"decision": "rejected"}),
            now(),
        )
        .await
        .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(out.text.contains("not delivered"), "{}", out.text);
        assert!(out.text.contains("already completed"), "{}", out.text);
        // Observed state: nothing was written, and the run was not queued.
        let signals = journaled_signals(&j, run, &gate()).await;
        assert_eq!(
            signals.len(),
            1,
            "the original answer must be the only one: {signals:?}"
        );
        assert_eq!(signals[0]["decision"], "approved");
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            None,
            "a refused delivery must not queue a wake"
        );
    }

    /// §6.6: the node is not awaiting, so name the state it IS in.
    #[tokio::test]
    async fn signal_on_a_run_that_is_not_paused_reports_not_delivered() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = InMemorySchedulerStore::default();
        s.enqueue(run, &empty_graph(), now()).await.unwrap(); // waking, not paused
        let j = awaiting_journal(run, &gate(), None).await;

        let out = signal(&s, &j, run, gate(), approved(), now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(out.text.contains("not delivered"), "{}", out.text);
        assert!(
            out.text.contains("waking"),
            "must name the actual state: {}",
            out.text
        );
        // `waking` is TRANSIENT — the lease resolves into `paused` again or into a
        // terminal state — so waiting is real advice here, and this is the arm that must
        // give it. (The terminal arm must not; see the test below.)
        assert!(
            out.text.contains("shows it paused"),
            "a waking run is worth retrying, and the operator must be told so: {}",
            out.text
        );
        assert!(
            journaled_signals(&j, run, &gate()).await.is_empty(),
            "a worker holds the lease and is folding this journal — nothing may be written"
        );
    }

    /// A terminal run is NEVER paused again by any shipped store, so "retry once `status`
    /// shows it paused" is advice to wait forever. `cancel`/`record_terminal` journal no
    /// node event, so the gate still folds as awaiting on a run that is over — which is
    /// exactly how an operator reaches this path.
    #[tokio::test]
    async fn signal_on_a_terminal_run_does_not_advise_waiting_for_a_pause_that_never_comes() {
        for terminal in [
            RunStatus::Cancelled,
            RunStatus::Completed,
            RunStatus::Failed,
        ] {
            let run = RunId(uuid::Uuid::new_v4());
            let s = InMemorySchedulerStore::default();
            s.enqueue(run, &empty_graph(), now()).await.unwrap();
            s.record_terminal(run, terminal, None).await.unwrap();
            let j = awaiting_journal(run, &gate(), None).await;

            let out = signal(&s, &j, run, gate(), approved(), now())
                .await
                .expect("no hard error");

            assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
            assert!(
                out.text.contains(terminal.as_str()),
                "must name the actual state: {}",
                out.text
            );
            assert!(
                !out.text.contains("shows it paused") && !out.text.contains("Retry"),
                "a {} run never pauses again — this is advice to wait forever: {}",
                terminal.as_str(),
                out.text
            );
            assert!(
                journaled_signals(&j, run, &gate()).await.is_empty(),
                "nothing may be written into a run that is over"
            );
        }
    }

    /// The `RunCompleted` BACKSTOP, alone. An `AwaitSignal` node journals no
    /// `NodeCompleted`, so the node-keyed completion marker is the blackboard
    /// `ContextWrite` — which only exists when a `ContextStore` is wired. With none
    /// wired, `RunCompleted` is the ONLY evidence the node finished, and without it a
    /// signal would be written into a run that has already ended and reported as
    /// delivered.
    #[tokio::test]
    async fn a_completed_run_marks_its_await_signal_node_completed_without_a_context_write() {
        let run = RunId(uuid::Uuid::new_v4());
        // The row stays `paused`, so ONLY the journal fold can refuse this: if the
        // backstop is removed, the node reads as awaiting and the delivery goes through.
        let s = paused_store(run, None).await;
        let j = awaiting_journal(run, &gate(), None).await;
        j.append(run, JournalEvent::RunCompleted).await.unwrap();
        assert!(
            seq_of(&j, run, |e| matches!(e, JournalEvent::ContextWrite { .. }))
                .await
                .is_none(),
            "precondition: no ContextStore was wired, so there is no node-keyed marker"
        );

        // The fold itself, directly...
        assert_eq!(
            signal_state(&j.load(run).await.unwrap(), &gate()),
            SignalState::Completed,
            "a finished run finished every node in it"
        );

        // ...and the refusal it produces.
        let out = signal(&s, &j, run, gate(), approved(), now())
            .await
            .expect("no hard error");
        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(out.text.contains("already completed"), "{}", out.text);
        assert!(
            journaled_signals(&j, run, &gate()).await.is_empty(),
            "the run is over — a delivery here can only ever be a last-wins answer for a \
             node that a re-`start` would re-execute"
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            None,
            "and a refused delivery must not queue a wake"
        );
    }

    #[tokio::test]
    async fn signal_on_an_unknown_run_exits_two() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = InMemorySchedulerStore::default();
        let j = empty_journal();

        let out = signal(&s, &j, run, gate(), approved(), now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(out.text.contains("no such run"), "{}", out.text);
        assert!(
            journaled_signals(&j, run, &gate()).await.is_empty(),
            "nothing may be journaled for a run that does not exist"
        );
    }

    /// A node id that never began awaiting — a typo, or a node the run has not reached.
    #[tokio::test]
    async fn signal_on_a_node_that_never_awaited_reports_not_delivered() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = awaiting_journal(run, &gate(), None).await;
        let typo = NodeId("gat".into());

        let out = signal(&s, &j, run, typo.clone(), approved(), now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(out.text.contains("not delivered"), "{}", out.text);
        assert!(out.text.contains("gat"), "must name the node: {}", out.text);
        assert!(journaled_signals(&j, run, &typo).await.is_empty());
    }

    /// The deadline fired: the node is terminally failed and a signal changes nothing.
    #[tokio::test]
    async fn signal_on_a_failed_node_reports_not_delivered() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = awaiting_journal(run, &gate(), Some(now())).await;
        j.append(
            run,
            JournalEvent::NodeFailed {
                node: gate(),
                error: "await_signal: no signal for node gate by ...".into(),
            },
        )
        .await
        .unwrap();

        let out = signal(&s, &j, run, gate(), approved(), now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(out.text.contains("not delivered"), "{}", out.text);
        assert!(
            out.text.contains("failed"),
            "must name the actual state: {}",
            out.text
        );
        assert!(journaled_signals(&j, run, &gate()).await.is_empty());
    }

    /// A hard dependency failed and cascade-skipped the gate: the node will never
    /// re-execute, so a signal is not an answer to anything.
    ///
    /// The third of the three terminal states §6.6 requires this command to report, and
    /// the one that shipped unguarded — the `NodeSkipped` arm of [`signal_states`] could
    /// be deleted with the whole suite green. Without it the gate folds back as
    /// `Awaiting`, so the pre-check passes, a durable `SignalReceived` is appended, the run
    /// is force-woken and the operator is told `signalled` with exit 0: a false success,
    /// plus exactly the last-wins orphan row a re-`start` would fold as the node's output.
    #[tokio::test]
    async fn signal_on_a_cascade_skipped_node_reports_not_delivered() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = awaiting_journal(run, &gate(), None).await;
        j.append(run, JournalEvent::NodeSkipped { node: gate() })
            .await
            .unwrap();

        let out = signal(&s, &j, run, gate(), approved(), now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(out.text.contains("not delivered"), "{}", out.text);
        assert!(
            out.text.contains("skipped"),
            "must name the actual state: {}",
            out.text
        );
        assert!(
            journaled_signals(&j, run, &gate()).await.is_empty(),
            "a refused delivery must leave no durable orphan behind"
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            None,
            "and must not queue a wake for a node that will never re-execute"
        );
    }

    /// AC6, on the DURABLE side. Task 3 redacts on the fold-READ path, which protects the
    /// node's return and its CAS output — it does NOT protect the journal row, and this
    /// command is that row's only writer. A human who pastes a token must not have put it
    /// into durable storage permanently.
    ///
    /// The credential is assembled at runtime: the repo's Semgrep CWE-798 hook blocks a
    /// literal one in a fixture.
    #[tokio::test]
    async fn a_signal_payload_is_redacted_before_it_is_journaled() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = awaiting_journal(run, &gate(), None).await;
        let secret = format!("sk-{}", "A".repeat(24));

        let out = signal(
            &s,
            &j,
            run,
            gate(),
            serde_json::json!({"decision": "approved", "note": format!("use {secret}")}),
            now(),
        )
        .await
        .expect("delivers");
        assert_eq!(out.code, EXIT_OK, "{}", out.text);

        let signals = journaled_signals(&j, run, &gate()).await;
        assert_eq!(signals.len(), 1);
        let durable = serde_json::to_string(&signals[0]).expect("serializes");
        assert!(
            !durable.contains(&secret),
            "the credential is now in durable storage forever: {durable}"
        );
        assert!(
            durable.contains("[REDACTED]"),
            "the payload must be scrubbed, not dropped: {durable}"
        );
        assert_eq!(
            signals[0]["decision"], "approved",
            "the DECISION must survive redaction: {durable}"
        );
    }

    /// AC7, half two: a `HumanGate` is answerable ONLY by `GateDecided`, so `run signal`
    /// must refuse one — and point at the command that works.
    ///
    /// This is not a politeness. Without it a raw `--payload '{}'` writes a
    /// `SignalReceived` for a gate node and bypasses every validation s2 adds: the menu
    /// check, the outcome mapping and the required reason. (The executor would not honour
    /// that row — `run_human_gate` reads only `GateDecided` — so the operator would be
    /// told "signalled" for a delivery that can never be read, which is the reporting
    /// failure §6.6 forbids as much as the bypass itself.)
    #[tokio::test]
    async fn signal_on_a_human_gate_is_refused_and_points_at_run_gate() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = InMemoryJournal::new();
        j.append(
            run,
            JournalEvent::GateAwaited {
                node: gate(),
                deadline: None,
                options: vec![orchestrator_core::GateOption {
                    name: "ship".into(),
                    outcome: orchestrator_core::GateOutcome::Complete,
                }],
            },
        )
        .await
        .unwrap();

        let out = signal(&s, &j, run, gate(), approved(), now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("HumanGate"),
            "must name what the node actually is: {}",
            out.text
        );
        assert!(
            out.text.contains("run gate"),
            "must name the command that would work: {}",
            out.text
        );
        assert!(
            journaled_signals(&j, run, &gate()).await.is_empty(),
            "a raw payload must never become a durable answer to a gate"
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            None,
            "and must not queue a wake"
        );
    }

    /// §6.5: an unbounded JSON blob in a journal row is a durable footgun, and
    /// `SignalReceived.payload` is a bare `Value` with no ref-or-inline alternative — so
    /// the cap is enforced here, before anything is written.
    #[tokio::test]
    async fn an_oversized_signal_payload_is_rejected_before_anything_is_journaled() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = awaiting_journal(run, &gate(), None).await;
        let huge = serde_json::json!({ "note": "x".repeat(MAX_PAYLOAD_BYTES) });
        let actual = serde_json::to_vec(&huge).unwrap().len();
        assert!(actual > MAX_PAYLOAD_BYTES, "precondition: {actual}");

        let e = signal(&s, &j, run, gate(), huge, now())
            .await
            .expect_err("an over-limit payload is refused");

        assert_eq!(e.code, crate::errors::EXIT_ERROR, "{}", e.message);
        assert!(
            e.message.contains(&MAX_PAYLOAD_BYTES.to_string()),
            "must name the limit: {}",
            e.message
        );
        assert!(
            e.message.contains(&actual.to_string()),
            "must name the ACTUAL size so the operator knows how much to cut: {}",
            e.message
        );
        assert!(
            journaled_signals(&j, run, &gate()).await.is_empty(),
            "an over-limit payload must never reach the journal"
        );
        assert_eq!(s.status(run).await.unwrap().unwrap().next_wake, None);
    }

    /// §6.5, and the half the first cut missed: the cap governs the JOURNAL ROW, and the
    /// row holds the REDACTED payload — which can be LARGER than what the operator sent.
    /// `[REDACTED]` is 10 bytes and the assignment pattern's shortest matched value is 6,
    /// so a payload of many short `token:…` pairs inflates by roughly 1.67x. Checking
    /// only the as-given size therefore let a ~4 KiB payload journal a ~5.3 KiB row.
    ///
    /// The pair is assembled at runtime: the repo's Semgrep CWE-798 hook blocks a
    /// credential-shaped literal in a fixture.
    #[tokio::test]
    async fn a_payload_that_only_exceeds_the_cap_after_redaction_is_rejected() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = awaiting_journal(run, &gate(), None).await;

        // `token:abcdef ` — a 6-byte value (the pattern's minimum) that redacts to the
        // 10-byte placeholder, repeated. The trailing space is load-bearing: without a
        // separator the value class runs to the end of the string and the whole run
        // collapses into ONE placeholder, which shrinks rather than grows.
        let unit = format!("{}:{} ", "token", "abcdef");
        let raw = serde_json::json!({ "n": unit.repeat(312) });
        let as_given = serde_json::to_vec(&raw).unwrap().len();
        let journaled = serde_json::to_vec(&render::redact_payload(&raw))
            .unwrap()
            .len();
        assert!(
            as_given <= MAX_PAYLOAD_BYTES,
            "precondition: this passes the as-given check ({as_given} bytes)"
        );
        assert!(
            journaled > MAX_PAYLOAD_BYTES,
            "precondition: redaction GROWS it past the cap ({as_given} -> {journaled} bytes)"
        );

        let e = signal(&s, &j, run, gate(), raw, now())
            .await
            .expect_err("a row that would exceed the cap once redacted is refused");

        assert_eq!(e.code, crate::errors::EXIT_ERROR, "{}", e.message);
        assert!(
            e.message.contains(&journaled.to_string()),
            "must name the size that would actually be JOURNALED ({journaled}), not the \
             one the operator sent: {}",
            e.message
        );
        assert!(
            e.message.contains(&MAX_PAYLOAD_BYTES.to_string()),
            "must name the limit: {}",
            e.message
        );
        assert!(
            journaled_signals(&j, run, &gate()).await.is_empty(),
            "an over-limit row must never reach the journal"
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            None,
            "and a refused delivery must not queue a wake"
        );
    }

    /// §6.5, and the SECOND thing that falsified [`MAX_PAYLOAD_BYTES`]'s claim: the cap
    /// governs the DURABLE ROW, and the durable column is `jsonb`, which normalises every
    /// JSON number to `numeric`. The wire form `1e308` is 5 bytes; `jsonb` stores and reads
    /// it back as a 309-digit integer. Measured on `postgres:16`, a payload `serde_json`
    /// reports as 4088 bytes — under the cap, accepted by both checks — occupies 181_320
    /// bytes of `jsonb`: 44x the bound this cap exists to enforce, reachable from the
    /// operator CLI with no privilege, and paid again on every `journal.load` for the life
    /// of the run.
    #[tokio::test]
    async fn a_payload_that_only_exceeds_the_cap_once_the_backend_stores_it_is_rejected() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = awaiting_journal(run, &gate(), None).await;

        // 583 copies of the largest finite f64. Each is 5 bytes on the wire and 309 as
        // `numeric`, so this clears the as-given cap and blows through the durable one.
        let raw = serde_json::json!({ "n": vec![1e308_f64; 583] });
        let as_given = serde_json::to_vec(&raw).unwrap().len();
        let durable = as_given + jsonb_number_expansion(&raw);
        assert!(
            as_given <= MAX_PAYLOAD_BYTES,
            "precondition: this passes the as-given check ({as_given} bytes)"
        );
        assert!(
            durable > MAX_PAYLOAD_BYTES,
            "precondition: the backend EXPANDS it past the cap ({as_given} -> {durable} bytes)"
        );

        let e = signal(&s, &j, run, gate(), raw, now())
            .await
            .expect_err("a row that only exceeds the cap once stored is refused");

        assert_eq!(e.code, crate::errors::EXIT_ERROR, "{}", e.message);
        assert!(
            e.message.contains(&durable.to_string()),
            "must name the size the BACKEND will store ({durable}), not the one \
             `serde_json` wrote: {}",
            e.message
        );
        assert!(
            e.message.contains(&MAX_PAYLOAD_BYTES.to_string()),
            "must name the limit: {}",
            e.message
        );
        assert!(
            journaled_signals(&j, run, &gate()).await.is_empty(),
            "an over-limit row must never reach the journal"
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            None,
            "and a refused delivery must not queue a wake"
        );
    }

    /// The expansion is charged to NUMBERS only, and only to those the backend really
    /// rewrites. A decision object — the shape this flag exists for — must measure
    /// identically before and after, or every ordinary signal pays for this guard.
    #[test]
    fn jsonb_expansion_is_zero_for_a_decision_and_for_plain_integers() {
        assert_eq!(jsonb_number_expansion(&approved()), 0);
        assert_eq!(
            jsonb_number_expansion(&serde_json::json!({ "attempts": 3, "score": -12 })),
            0,
            "an integer's wire form IS its numeric form"
        );
        assert_eq!(
            jsonb_number_expansion(&serde_json::json!({ "ratio": 0.5 })),
            0,
            "a float that needs no expansion is charged nothing"
        );
        assert_eq!(
            jsonb_number_expansion(&serde_json::json!(1e308_f64)),
            309 - 6,
            "`serde_json` writes `1e+308` (6 bytes); `numeric` stores 309 digits"
        );
        assert_eq!(
            jsonb_number_expansion(&serde_json::json!(1e-308_f64)),
            310 - 6,
            "and it expands in the other direction too: `1e-308` -> `0.000…1`"
        );
        assert!(
            jsonb_number_expansion(&serde_json::json!({ "deep": [[{ "n": 1e308_f64 }]] })) > 0,
            "the walk must reach a number nested under arrays and objects"
        );
    }

    /// §6.6, on the one path where getting it wrong is irreversible: once the append has
    /// succeeded the answer IS durable, and no later fault may be reported in a way that
    /// reads as "the signal did not go through".
    ///
    /// `force_wake` faulting on a transient backend error (pool-acquire timeout, failover)
    /// used to `?`-propagate as a bare `store error: …`. For the INDEFINITE class — the
    /// gate with no deadline, whose `next_wake` is NULL so no tick will ever claim it —
    /// that is the worst possible report: the decision is on the journal, the run will
    /// never be driven again, and the operator has been told only that something errored.
    /// This command already refuses to say `not delivered` for a durable row on every
    /// MODELLED outcome; a fault must not be the one exception.
    #[tokio::test]
    async fn a_wake_fault_after_the_append_still_reports_the_durable_answer() {
        let run = RunId(uuid::Uuid::new_v4());
        let store = FailingForceWakeStore(paused_store(run, None).await);
        let j = awaiting_journal(run, &gate(), None).await;

        let out = signal(&store, &j, run, gate(), approved(), now())
            .await
            .expect("a post-append fault is a REPORT, not a bare error");

        assert_eq!(
            out.code,
            crate::errors::EXIT_PRECONDITION,
            "ran, wrote, but not the unqualified success asked for: {}",
            out.text
        );
        assert!(
            !out.text.contains("not delivered"),
            "the row IS durable — `not delivered` sends the operator looking for a write \
             that already happened: {}",
            out.text
        );
        assert!(
            out.text.contains("journaled durably"),
            "must say the answer survived: {}",
            out.text
        );
        assert!(
            out.text.contains(&format!("torii run wake {}", run.0)),
            "must name the command that unblocks it: {}",
            out.text
        );
        assert_eq!(
            journaled_signals(&j, run, &gate()).await,
            vec![approved()],
            "and the answer really is on the journal"
        );
    }

    #[test]
    fn parse_payload_rejects_something_that_is_not_json() {
        let e = parse_payload("approved").expect_err("bare prose is not JSON");
        assert!(e.contains("--payload"), "{e}");
        assert!(
            e.contains("line 1 column 1"),
            "must locate the problem: {e}"
        );
        assert!(
            e.contains(r#"{"decision":"approved"}"#),
            "must show the shape that would work: {e}"
        );
    }

    /// The likeliest way an operator pastes a credential into this flag is to type the
    /// token BARE — which is not valid JSON, so the invalid-JSON message is exactly the
    /// path that would echo it to stderr and thus into journald and CI logs. Two leaks in
    /// this codebase came from adjacent error text doing precisely that.
    #[test]
    fn an_invalid_payload_error_never_echoes_the_offending_value() {
        let secret = format!("sk-{}", "A".repeat(24));
        let e = parse_payload(&secret).expect_err("a bare token is not JSON");
        assert!(
            !e.contains(&secret),
            "a pasted credential reached stderr: {e}"
        );
        // ...and not a long fragment of it either.
        assert!(!e.contains(&"A".repeat(8)), "a fragment leaked: {e}");
    }

    #[test]
    fn parse_payload_accepts_an_object_and_a_bare_scalar() {
        assert_eq!(
            parse_payload(r#"{"decision":"approved"}"#).expect("an object"),
            approved()
        );
        assert_eq!(
            parse_payload("\"approved\"").expect("a bare string"),
            serde_json::json!("approved")
        );
    }

    /// The cap is enforced by the value parser too, so an operator with a runaway payload
    /// is refused BEFORE any database connection is opened.
    #[test]
    fn parse_payload_rejects_an_oversized_payload_before_any_connection() {
        let huge = format!("{{\"n\":\"{}\"}}", "x".repeat(MAX_PAYLOAD_BYTES));
        let e = parse_payload(&huge).expect_err("over the cap");
        assert!(e.contains(&MAX_PAYLOAD_BYTES.to_string()), "{e}");
        assert!(e.to_lowercase().contains("payload"), "{e}");
    }

    /// What the concurrent worker's drive does to the awaiting node — see
    /// [`SignalRacingStore`].
    #[derive(Clone, Copy, PartialEq)]
    enum RacingDrive {
        /// It merely claimed the run (`paused -> waking`) and is still driving it.
        ClaimsOnly,
        /// It folded the journal — which by then contains our delivery — and completed
        /// the node with that answer.
        CompletesTheNode,
        /// It had loaded the journal BEFORE our delivery landed, found no signal and an
        /// expired deadline, and failed the node. Our answer is durable but was never
        /// read.
        FailsTheDeadline,
    }

    /// A `SchedulerStore` that runs a concurrent worker against `run` at the top of
    /// `force_wake` — i.e. exactly in the window between `signal`'s pre-check and the
    /// point where its effect becomes observable. `signal` appends BEFORE it calls
    /// `force_wake`, so this models a worker that drove the run AFTER the delivery
    /// landed (the terminal marker is journaled behind our `SignalReceived`). Same
    /// technique as `RacingStore` above: single-threaded, deterministic, no database.
    ///
    /// The other half of the window — a drive that terminated the node BEFORE our append
    /// — cannot be modelled here, because `signal` makes no store call between its
    /// journal read and its journal write; see [`RacingJournal`].
    struct SignalRacingStore {
        inner: InMemorySchedulerStore,
        journal: Arc<InMemoryJournal>,
        run: RunId,
        drive: RacingDrive,
    }

    #[async_trait::async_trait]
    impl SchedulerStore for SignalRacingStore {
        async fn enqueue(
            &self,
            run: RunId,
            graph: &Graph,
            now: DateTime<Utc>,
        ) -> Result<(), OrchestratorError> {
            self.inner.enqueue(run, graph, now).await
        }
        async fn record_paused(
            &self,
            run: RunId,
            next_wake: Option<DateTime<Utc>>,
            reason: &str,
        ) -> Result<(), OrchestratorError> {
            self.inner.record_paused(run, next_wake, reason).await
        }
        async fn record_terminal(
            &self,
            run: RunId,
            status: RunStatus,
            reason: Option<&str>,
        ) -> Result<(), OrchestratorError> {
            self.inner.record_terminal(run, status, reason).await
        }
        async fn claim_due(
            &self,
            now: DateTime<Utc>,
            lease: chrono::Duration,
            limit: usize,
        ) -> Result<Vec<(RunId, Graph)>, OrchestratorError> {
            self.inner.claim_due(now, lease, limit).await
        }
        async fn status(
            &self,
            run: RunId,
        ) -> Result<Option<orchestrator_core::ScheduledRun>, OrchestratorError> {
            self.inner.status(run).await
        }
        async fn list_paused(
            &self,
        ) -> Result<Vec<orchestrator_core::ScheduledRun>, OrchestratorError> {
            self.inner.list_paused().await
        }
        async fn cancel(&self, run: RunId) -> Result<(), OrchestratorError> {
            self.inner.cancel(run).await
        }
        async fn count_terminal_before(
            &self,
            before: DateTime<Utc>,
        ) -> Result<u64, OrchestratorError> {
            self.inner.count_terminal_before(before).await
        }
        async fn prune_terminal(&self, before: DateTime<Utc>) -> Result<u64, OrchestratorError> {
            self.inner.prune_terminal(before).await
        }
        async fn force_wake(
            &self,
            run: RunId,
            now: DateTime<Utc>,
        ) -> Result<(), OrchestratorError> {
            if run != self.run {
                return self.inner.force_wake(run, now).await;
            }
            // A worker's tick claims the run: `paused -> waking`.
            self.inner
                .claim_due(now, chrono::Duration::seconds(60), 10)
                .await?;
            match self.drive {
                RacingDrive::ClaimsOnly => {}
                RacingDrive::CompletesTheNode => {
                    // ...and its drive folds the journal (which now includes our
                    // delivery), completes the gate, and finishes the run.
                    append_completion(&self.journal, run, &gate()).await;
                    self.journal.append(run, JournalEvent::RunCompleted).await?;
                    self.inner
                        .record_terminal(run, RunStatus::Completed, None)
                        .await?;
                }
                RacingDrive::FailsTheDeadline => {
                    // ...but this drive had already loaded the journal before our
                    // delivery landed (`run_await_signal` reads the fold it was handed),
                    // so it saw no signal, found the deadline expired, and failed the
                    // node — behind our row, without ever reading it.
                    self.journal
                        .append(
                            run,
                            JournalEvent::NodeFailed {
                                node: gate(),
                                error: "await_signal: no signal for node gate by ...".into(),
                            },
                        )
                        .await?;
                    self.inner
                        .record_terminal(run, RunStatus::Failed, Some("await_signal"))
                        .await?;
                }
            }
            // Our own force_wake: a conditional no-op, because the row is no longer
            // `paused`.
            self.inner.force_wake(run, now).await
        }
    }

    /// The `Seq` of the first event matching `want`. The ORDER of two journal rows is the
    /// only evidence available for whether a delivery was read or orphaned, so these
    /// tests assert on it rather than on the exit code alone.
    async fn seq_of(
        j: &InMemoryJournal,
        run: RunId,
        want: impl Fn(&JournalEvent) -> bool,
    ) -> Option<Seq> {
        j.load(run)
            .await
            .unwrap()
            .into_iter()
            .find(|(_, e)| want(e))
            .map(|(s, _)| s)
    }

    async fn delivery_seq(j: &InMemoryJournal, run: RunId) -> Option<Seq> {
        seq_of(
            j,
            run,
            |e| matches!(e, JournalEvent::SignalReceived { node, .. } if node == &gate()),
        )
        .await
    }

    /// A whole journal rendered `seq=<n> <event kind>`, for a failure message that shows
    /// WHY a report is wrong (which row landed first) rather than only that it is.
    async fn journal_shape(j: &InMemoryJournal, run: RunId) -> String {
        j.load(run)
            .await
            .unwrap()
            .into_iter()
            .map(|(s, e)| format!("seq={s} {}", event_kind(&e)))
            .collect::<Vec<_>>()
            .join("\n  ")
    }

    fn event_kind(e: &JournalEvent) -> String {
        match e {
            JournalEvent::SignalAwaited { node, deadline } => {
                format!("SignalAwaited node={} deadline={deadline:?}", node.0)
            }
            JournalEvent::SignalReceived { node, .. } => {
                format!("SignalReceived node={}", node.0)
            }
            JournalEvent::ContextWrite { key, .. } => format!("ContextWrite key={}", key.0),
            JournalEvent::NodeFailed { node, .. } => format!("NodeFailed node={}", node.0),
            JournalEvent::RunCompleted => "RunCompleted".into(),
            JournalEvent::RunPaused { .. } => "RunPaused".into(),
            other => format!("{other:?}"),
        }
    }

    /// THE check-then-act case, and the one the first cut of this command got INVERTED.
    /// The node was awaiting when `signal` checked; a concurrent worker then folded the
    /// journal — which by that point contained our answer — and completed the node with
    /// it. The delivery LANDED and was READ, so reporting `not delivered` (exit 2) would
    /// be a false negative on the most successful outcome this command has.
    ///
    /// The discriminator is the journal ORDER: the completion marker sits BEHIND our
    /// `SignalReceived`, so our row is what the drive folded. That is what this asserts —
    /// not merely the exit code, which the pre-fix test canonized while never checking
    /// whether the row was consumed.
    #[tokio::test]
    async fn signal_reports_signalled_when_a_racing_drive_reads_the_answer() {
        let run = RunId(uuid::Uuid::new_v4());
        // A TIMED gate whose deadline has just come due: `next_wake <= now`, so a
        // worker's `claim_due` really can grab it in the delivery window. (The indefinite
        // class cannot be claimed before `force_wake` runs, so it has no such race.)
        let inner = paused_store(run, Some(now())).await;
        let journal = Arc::new(awaiting_journal(run, &gate(), Some(now())).await);
        let racing = SignalRacingStore {
            inner: inner.clone(),
            journal: journal.clone(),
            run,
            drive: RacingDrive::CompletesTheNode,
        };

        let out = signal(&racing, journal.as_ref(), run, gate(), approved(), now())
            .await
            .expect("no hard error");
        // The evidence FIRST: the row is durable and the node completed behind it.
        let delivered = delivery_seq(&journal, run)
            .await
            .expect("the delivery is durable");
        let completed = seq_of(
            &journal,
            run,
            |e| matches!(e, JournalEvent::ContextWrite { key, .. } if key.0 == gate().0),
        )
        .await
        .expect("the racing drive completed the gate");
        assert!(
            delivered < completed,
            "precondition: the gate completed by folding OUR answer\n  {}",
            journal_shape(&journal, run).await
        );
        assert_eq!(
            journaled_signals(&journal, run, &gate()).await.len(),
            1,
            "and there is no OTHER answer it could have read instead"
        );

        assert_eq!(
            out.code,
            EXIT_OK,
            "the answer was delivered, folded and consumed — this is the success case, \
             not a refusal:\n  {}\n  report was: {}",
            journal_shape(&journal, run).await,
            out.text
        );
        assert!(
            out.text.starts_with("signalled:"),
            "must report the effect actually achieved: {}",
            out.text
        );
        assert!(
            out.text.contains("gate"),
            "must name the node: {}",
            out.text
        );
        assert_eq!(
            inner.status(run).await.unwrap().unwrap().status,
            RunStatus::Completed,
            "the racing drive really did finish the run"
        );
    }

    /// The same window, but the racing drive had loaded the journal BEFORE our answer
    /// landed: it saw no signal, found the deadline expired, and failed the node behind
    /// our row. The answer is durable and was NEVER read, so `signalled` would be just as
    /// wrong as `not delivered` was in the test above — and the report must say which.
    #[tokio::test]
    async fn signal_does_not_claim_delivery_when_a_racing_drive_fails_the_node_after_the_write() {
        let run = RunId(uuid::Uuid::new_v4());
        let inner = paused_store(run, Some(now())).await;
        let journal = Arc::new(awaiting_journal(run, &gate(), Some(now())).await);
        let racing = SignalRacingStore {
            inner: inner.clone(),
            journal: journal.clone(),
            run,
            drive: RacingDrive::FailsTheDeadline,
        };

        let out = signal(&racing, journal.as_ref(), run, gate(), approved(), now())
            .await
            .expect("no hard error");
        let delivered = delivery_seq(&journal, run)
            .await
            .expect("the delivery is durable");
        let failed = seq_of(
            &journal,
            run,
            |e| matches!(e, JournalEvent::NodeFailed { node, .. } if node == &gate()),
        )
        .await
        .expect("the racing drive failed the gate");
        assert!(
            delivered < failed,
            "precondition: the node terminated AFTER the write landed\n  {}",
            journal_shape(&journal, run).await
        );
        assert!(
            seq_of(&journal, run, |e| {
                matches!(e, JournalEvent::ContextWrite { key, .. } if key.0 == gate().0)
            })
            .await
            .is_none(),
            "precondition: a failed gate published nothing — the answer was not read"
        );

        assert_eq!(
            out.code, EXIT_PRECONDITION,
            "a deadline-failed node never read the answer: {}",
            out.text
        );
        assert!(
            !out.text.starts_with("signalled"),
            "the node failed; claiming the answer was delivered would hide it: {}",
            out.text
        );
        assert!(
            out.text.contains("durabl"),
            "the row IS durable — an operator must not be told it can simply retry: {}",
            out.text
        );
        assert!(
            out.text.contains("is failed"),
            "must name the state the node is in: {}",
            out.text
        );
        assert!(
            out.text.contains("while this delivery was in flight"),
            "must say the node died AFTER the row landed — the opposite order is a \
             different report with different advice, and the two must not read alike: {}",
            out.text
        );
    }

    /// A journal whose FIRST `load` answers from a snapshot taken *before* a concurrent
    /// worker's drive, and then lets that drive land. This is the ONLY way to model the
    /// other half of the delivery window — a node that terminated between `signal`'s
    /// pre-check read and its append — because `signal` makes no store call in between,
    /// so a `SchedulerStore` double cannot reach it.
    ///
    /// The result: the pre-check legitimately sees the node awaiting, and everything
    /// written afterwards is journaled BEHIND the terminal marker.
    struct RacingJournal {
        inner: Arc<InMemoryJournal>,
        /// The event the racing drive lands, taken on the first `load` so it fires
        /// exactly once.
        pending: std::sync::Mutex<Option<JournalEvent>>,
    }

    #[async_trait::async_trait]
    impl ExecutionJournal for RacingJournal {
        async fn append(
            &self,
            run: RunId,
            event: JournalEvent,
        ) -> Result<Seq, orchestrator_core::JournalError> {
            self.inner.append(run, event).await
        }
        async fn load(
            &self,
            run: RunId,
        ) -> Result<Vec<(Seq, JournalEvent)>, orchestrator_core::JournalError> {
            let snapshot = self.inner.load(run).await?;
            // Taken (and the guard dropped) before the await: the racing drive lands
            // AFTER this read has been answered.
            let pending = self.pending.lock().expect("not poisoned").take();
            if let Some(e) = pending {
                self.inner.append(run, e).await?;
            }
            Ok(snapshot)
        }
    }

    /// The TRUE orphan, on BOTH terminal shapes. The node terminated before the append,
    /// so the row is durable, unread, and permanent — and, because an `AwaitSignal` node
    /// journals no `NodeCompleted` and no terminal event is folded as a barrier, a later
    /// re-`start` would re-execute the gate and fold this late answer as its output,
    /// silently converting a deadline-expired gate into an approved one — or replacing the
    /// answer a completed gate actually acted on.
    ///
    /// So the report may claim NEITHER "signalled" (nothing read it) NOR "not delivered"
    /// (the row is durable). It has to say both halves — and it has to say them for the
    /// COMPLETED marker too, because that is the one shape whose *terminated-after* twin
    /// is a success, so it is the shape where getting the ORDER wrong reports an orphaned
    /// answer as delivered.
    #[tokio::test]
    async fn signal_reports_the_answer_unread_when_the_node_terminated_before_the_write() {
        for (marker, state) in [
            (
                JournalEvent::NodeFailed {
                    node: gate(),
                    error: "await_signal: no signal for node gate by ...".into(),
                },
                "failed",
            ),
            (
                JournalEvent::ContextWrite {
                    scope: orchestrator_core::Scope::Run,
                    key: orchestrator_core::ContextKey(gate().0.clone()),
                    content: orchestrator_core::ContentRef {
                        digest: orchestrator_core::Digest("d".into()),
                        size: 3,
                        summary: None,
                    },
                    summary: None,
                    seq: 0,
                },
                "completed",
            ),
        ] {
            let run = RunId(uuid::Uuid::new_v4());
            // The store row is still `paused` throughout — the drive that terminated the
            // node has not recorded the run terminal yet, which is exactly why the
            // pre-checks pass and the write goes through. The wake even applies; the node
            // is simply dead.
            let s = paused_store(run, Some(now())).await;
            let inner = Arc::new(awaiting_journal(run, &gate(), Some(now())).await);
            let j = RacingJournal {
                inner: inner.clone(),
                pending: std::sync::Mutex::new(Some(marker)),
            };

            let out = signal(&s, &j, run, gate(), approved(), now())
                .await
                .expect("no hard error");

            let delivered = delivery_seq(&inner, run)
                .await
                .expect("the row is durable — it was appended before anything was re-read");
            let terminated = seq_of(&inner, run, |e| {
                matches!(e, JournalEvent::NodeFailed { node, .. } if node == &gate())
                    || matches!(e, JournalEvent::ContextWrite { key, .. } if key.0 == gate().0)
            })
            .await
            .expect("the racing drive terminated the gate");
            assert!(
                terminated < delivered,
                "precondition: the node terminated BEFORE the write landed\n  {}",
                journal_shape(&inner, run).await
            );

            assert_eq!(
                out.code, EXIT_PRECONDITION,
                "nothing read the answer: {}",
                out.text
            );
            assert!(
                !out.text.starts_with("signalled"),
                "the node was already dead when the row landed: {}",
                out.text
            );
            assert!(
                out.text.contains("durabl"),
                "the row IS durable and permanent — 'not delivered' would send an operator \
                 looking for a write that already happened: {}",
                out.text
            );
            assert!(
                out.text
                    .contains(&format!("already {state} before the write landed")),
                "must say the node was terminal BEFORE the row landed — the opposite order \
                 is the case where the answer WAS read, and the two must not read alike: {}",
                out.text
            );
            assert!(
                out.text.contains("re-`start`"),
                "must name the durable residue: this answer is still on the journal for a \
                 later re-execution of the gate to fold: {}",
                out.text
            );
        }
    }

    /// The other half of the race: the run was claimed but not finished, so our
    /// `force_wake` never applied. The delivery IS durable, but "the run will resume on
    /// the next worker tick" would be false — the claiming drive may have folded the
    /// journal before our append landed.
    #[tokio::test]
    async fn signal_reports_not_queued_when_a_concurrent_claim_wins_the_race() {
        let run = RunId(uuid::Uuid::new_v4());
        // A TIMED gate whose deadline has just come due: `next_wake <= now`, so a
        // worker's `claim_due` really can grab it in the delivery window. (The indefinite
        // class cannot be claimed before `force_wake` runs, so it has no such race.)
        let inner = paused_store(run, Some(now())).await;
        let journal = Arc::new(awaiting_journal(run, &gate(), Some(now())).await);
        let racing = SignalRacingStore {
            inner: inner.clone(),
            journal: journal.clone(),
            run,
            drive: RacingDrive::ClaimsOnly,
        };

        let out = signal(&racing, journal.as_ref(), run, gate(), approved(), now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("waking"),
            "must name the real state: {}",
            out.text
        );
        assert!(
            out.text.contains("torii run wake"),
            "must give the operator a next step: {}",
            out.text
        );
        assert_eq!(
            journaled_signals(&journal, run, &gate()).await.len(),
            1,
            "the delivery is still durable — the report is about the WAKE, not the write"
        );
    }

    /// A store fault must SURFACE, never be flattened into a green no-op — and the append
    /// must already be durable when it does, proving `signal` writes the answer BEFORE it
    /// queues the wake (the same ordering, and the same reason, as `wake`'s
    /// `BudgetRaised`: any worker that can observe the wake folds a journal that already
    /// contains the signal).
    ///
    /// This is the ORDER discriminator: swap the two calls and the injected failure
    /// short-circuits before the append, leaving zero `SignalReceived` rows. What the
    /// fault is REPORTED as is a separate property, pinned by
    /// [`a_wake_fault_after_the_append_still_reports_the_durable_answer`].
    #[tokio::test]
    async fn signal_appends_the_answer_before_calling_force_wake_and_surfaces_its_failure() {
        let run = RunId(uuid::Uuid::new_v4());
        let inner = paused_store(run, None).await;
        let store = FailingForceWakeStore(inner);
        let j = awaiting_journal(run, &gate(), None).await;

        let out = signal(&store, &j, run, gate(), approved(), now())
            .await
            .expect("a post-append fault is reported, not returned as a bare error");

        assert_ne!(
            out.code, EXIT_OK,
            "the injected force_wake failure must surface, not be swallowed into a green \
             success: {}",
            out.text
        );
        assert_eq!(
            journaled_signals(&j, run, &gate()).await.len(),
            1,
            "the answer must already be durable even though force_wake failed"
        );
    }

    /// `signal`'s post-append `unread` closure renders a backend fault to stdout, and a
    /// backend message is free text from the driver — the same class `list-paused` puts
    /// through `safe_reason`, and it was echoed here with no redaction, no
    /// control-character collapse and no cap.
    ///
    /// `PostgresJournal::load` builds that message from both `sqlx::Error` and
    /// `serde_json::Error`, so it can carry a connection string with a password, a
    /// newline that forges a pastable run row, and a raw ESC. `cmd::gate::decide` has the
    /// identical closure and the identical test — the guard has to hold at every sink,
    /// which is exactly why it is one shared helper rather than a habit.
    #[tokio::test]
    async fn signal_does_not_echo_a_raw_journal_fault_after_the_append() {
        /// Folds cleanly for the pre-check, faults on the post-append re-read.
        struct FaultingReloadJournal {
            inner: Arc<InMemoryJournal>,
            loads: std::sync::atomic::AtomicUsize,
        }
        #[async_trait::async_trait]
        impl ExecutionJournal for FaultingReloadJournal {
            async fn append(
                &self,
                run: RunId,
                event: JournalEvent,
            ) -> Result<Seq, orchestrator_core::JournalError> {
                self.inner.append(run, event).await
            }
            async fn load(
                &self,
                run: RunId,
            ) -> Result<Vec<(Seq, JournalEvent)>, orchestrator_core::JournalError> {
                if self.loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    return self.inner.load(run).await;
                }
                Err(hostile_backend_error(run))
            }
        }

        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let inner = Arc::new(awaiting_journal(run, &gate(), None).await);
        let j = FaultingReloadJournal {
            inner: inner.clone(),
            loads: std::sync::atomic::AtomicUsize::new(0),
        };

        let out = signal(&s, &j, run, gate(), approved(), now())
            .await
            .expect("a post-append fault is reported, not returned as a bare error");

        assert_eq!(
            journaled_signals(&inner, run, &gate()).await.len(),
            1,
            "precondition: the answer is durable, so this is the post-append report"
        );
        assert!(
            out.text.contains("journaled durably"),
            "precondition: the fault must be reported on the unread path: {}",
            out.text
        );

        assert!(
            !out.text.contains(&hostile_password()),
            "a journal fault leaked the database password: {}",
            out.text
        );
        assert!(
            out.text.contains("[REDACTED]"),
            "the credential span must be visibly redacted, not dropped: {}",
            out.text
        );
        assert!(
            !out.text.contains('\u{1b}'),
            "a raw escape byte survived into the report: {:?}",
            out.text
        );
        assert_eq!(
            out.text
                .lines()
                .filter(|l| l.trim_start().starts_with(FORGED_RUN))
                .count(),
            0,
            "a newline in the fault forged a line that reads as its own run row:\n{}",
            out.text
        );
    }

    // ---- SP-6 s1 Task 4: `list-paused` names the awaiting node ------------------------

    /// An operator must be able to discover WHAT to signal without reading the graph.
    #[tokio::test]
    async fn list_paused_names_the_awaiting_node_and_its_deadline() {
        let run = RunId(uuid::Uuid::new_v4());
        let deadline = now() + chrono::Duration::hours(1);
        let s = paused_store(run, Some(deadline)).await;
        let j = awaiting_journal(run, &gate(), Some(deadline)).await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        assert_eq!(out.code, EXIT_OK);
        assert!(
            out.text.contains("gate"),
            "the awaiting node must be named: {}",
            out.text
        );
        assert!(
            out.text.contains("1970-02-04T18:20:00Z"),
            "the deadline must be shown: {}",
            out.text
        );
    }

    /// The indefinite class — `SignalAwaited { deadline: None }`, a NULL `next_wake` — is
    /// the one an operator is most likely to lose track of, so it must be named too.
    #[tokio::test]
    async fn list_paused_names_an_indefinitely_awaiting_node() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = awaiting_journal(run, &gate(), None).await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        assert_eq!(out.code, EXIT_OK);
        assert!(
            out.text.contains("gate"),
            "an indefinite gate must still be named: {}",
            out.text
        );
    }

    /// A node that has already been signalled and completed is no longer awaiting, so
    /// listing it would send an operator to deliver a signal that changes nothing.
    #[tokio::test]
    async fn list_paused_does_not_name_a_node_that_already_completed() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = awaiting_journal(run, &gate(), None).await;
        j.append(
            run,
            JournalEvent::SignalReceived {
                node: gate(),
                payload: approved(),
            },
        )
        .await
        .unwrap();
        append_completion(&j, run, &gate()).await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        assert!(
            !out.text.contains("gate"),
            "a completed gate must not be advertised as awaiting: {}",
            out.text
        );
    }

    // ---- SP-6 s2 Task 8: `list-paused` shows a gate's MENU ----------------------------

    /// An operator must be able to see WHAT to decide without reading the graph. The menu
    /// comes from the journaled `GateAwaited`, so no graph load is needed.
    #[tokio::test]
    async fn list_paused_names_a_gates_menu() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship", "hold", "escalate"]).await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        assert_eq!(out.code, EXIT_OK);
        assert!(out.text.contains("release"), "{}", out.text);
        for o in ["ship", "hold", "escalate"] {
            assert!(
                out.text.contains(o),
                "the menu must be shown so an operator knows the choices: {}",
                out.text
            );
        }
    }

    /// The two waiting kinds must be TOLD APART in the listing, not merely both listed: a
    /// gate takes `torii run gate decide --option <name>` and refuses raw JSON, while an
    /// `AwaitSignal` takes `torii run signal --payload` and has no menu at all. An
    /// operator who cannot tell which is which will reach for the wrong command, and the
    /// cross-refusals that catch that mistake are a rescue, not a substitute for saying so
    /// here.
    ///
    /// **Both waiting nodes sit in ONE run**, and that is what makes the negative
    /// assertion mean anything. The fixture originally split them across two runs, and
    /// [`list_paused`] folds one journal PER RUN — so a menu could not have leaked between
    /// them however `awaiting_nodes` was written. Mutating its `menus.get(&node)` to
    /// `menus.values().next()` — the literal "borrow another node's menu" this test names
    /// — left the whole suite green. With both nodes in one fold, the negative assertion
    /// depends on the per-node keying it claims to check.
    #[tokio::test]
    async fn list_paused_distinguishes_a_gate_from_an_await_signal() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        // ONE run, two waiting nodes: a `HumanGate` with a menu and an `AwaitSignal`
        // without one — the shape a graph with both kinds actually produces.
        let j = gate_journal(run, &release(), None, &["ship", "hold"]).await;
        j.append(
            run,
            JournalEvent::SignalAwaited {
                node: gate(),
                deadline: None,
            },
        )
        .await
        .unwrap();

        let out = list_paused(&s, &j, false).await.expect("lists");
        // Keyed on the NODE COLUMN, not `contains`: a gate's menu cell is rendered
        // `gate: ship|hold`, so `contains("gate")` matches the `release` row too and the
        // two rows could not be told apart at all.
        let row_for = |node: &str| {
            out.text
                .lines()
                .filter(|l| l.starts_with(&run.0.to_string()))
                .find(|l| l.split_whitespace().nth(1) == Some(node))
                .unwrap_or_else(|| panic!("no awaiting row for {node}:\n{}", out.text))
                .to_string()
        };
        let gate_row = row_for("release");
        assert!(
            gate_row.contains("ship") && gate_row.contains("hold"),
            "the gate's own row must carry its menu: {gate_row}"
        );
        let signal_row = row_for("gate");
        assert!(
            !signal_row.contains("ship") && !signal_row.contains("hold"),
            "an AwaitSignal has no menu and must not borrow the gate's — they are in the \
             SAME fold, so only the per-node keying stops it: {signal_row}"
        );
        assert!(
            signal_row.contains("signal"),
            "…and must still say what it IS waiting for: {signal_row}"
        );
    }

    // ---- SP-6 s3: a human-backed `Agent` in the listing -------------------------------

    /// The third waiting kind must be VISIBLE. `signal_states`' `AgentAwaited` arm is the
    /// only thing that puts a human-backed agent in the awaited set, and until this test
    /// nothing in `list-paused` exercised it — dropping that arm reddened nine `cmd::human`
    /// tests and zero listing tests, so the arm's stated purpose ("without it the node never
    /// appears in `list-paused` at all") was guarded by nothing that renders a listing.
    ///
    /// An operator cannot answer what they cannot see, and this kind is the one where that
    /// bites hardest: a human-backed agent can pause INDEFINITELY (`AgentBacking::Human`
    /// with no timeout ⇒ `resume_after: None` ⇒ a NULL `next_wake` the durable scheduler
    /// never auto-wakes), so nothing but this listing will ever mention it again.
    #[tokio::test]
    async fn list_paused_names_a_human_backed_agent() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &reviewer(), None).await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        assert_eq!(out.code, EXIT_OK);
        let row = out
            .text
            .lines()
            .filter(|l| l.starts_with(&run.0.to_string()))
            .find(|l| l.split_whitespace().nth(1) == Some("reviewer"))
            .unwrap_or_else(|| panic!("no awaiting row for the agent:\n{}", out.text))
            .to_string();
        assert!(
            row.contains("waits until signalled"),
            "an agent with no timeout is never auto-woken, so the row must say it waits for \
             a human rather than leaving the deadline cell blank: {row}"
        );
    }

    /// One awaiting row, taken from the `AWAITING` BLOCK only.
    ///
    /// Splitting on the header first is not decoration. `render::table`'s run row starts
    /// with the same uuid and its last column is the pause REASON — free text that can
    /// contain the very words the awaiting cell is being checked for. s2's menu test
    /// matched the whole output and passed while the awaiting cell was empty; the same
    /// mistake here would let every assertion below be satisfied by the reason column.
    /// The second whitespace-separated token then discriminates the rows WITHIN the block,
    /// which is the node id (the table's second token is a status word).
    fn awaiting_row(text: &str, run: RunId, node: &str) -> String {
        let (_, block) = text
            .split_once("AWAITING")
            .unwrap_or_else(|| panic!("no AWAITING block at all:\n{text}"));
        block
            .lines()
            .filter(|l| l.starts_with(&run.0.to_string()))
            .find(|l| l.split_whitespace().nth(1) == Some(node))
            .unwrap_or_else(|| panic!("no awaiting row for {node}:\n{text}"))
            .to_string()
    }

    /// The in-process half of AC13's second leg ("`list-paused` shows the question"); the
    /// cross-process half is the Postgres e2e. The question is the WORK of a human-backed
    /// agent, so an operator must see it without loading the graph — which `list-paused`
    /// could not do anyway, since it folds one journal per paused run and holds no graph.
    ///
    /// Anchored on the agent's own row inside the awaiting block; see [`awaiting_row`].
    #[tokio::test]
    async fn list_paused_shows_a_human_agents_question() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &reviewer(), None).await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        assert_eq!(out.code, EXIT_OK);
        let row = awaiting_row(&out.text, run, "reviewer");
        assert!(
            row.contains(THE_QUESTION),
            "the question must be in the agent's OWN awaiting row: {row}"
        );
        assert!(
            row.contains("agent:"),
            "…and the row must name the KIND, or an operator reaches for `run signal` — \
             the one verb this node refuses: {row}"
        );
        assert!(
            out.text.contains("run agent answer"),
            "the block must name the verb that works, exactly as it does for a gate:\n{}",
            out.text
        );
    }

    /// FIRST wins on the QUESTION, and this is the guard the comment at `awaiting_nodes`'
    /// fold has always claimed: "if the listing showed a later question than the command
    /// answers, an operator would answer a question they were never shown."
    ///
    /// Re-review flipped `acc.entry(n).or_insert_with(…)` to `acc.insert(n, …)` and the
    /// whole workspace stayed green — the rule was asserted at three sites and guarded at
    /// one, the executor's `Fold::agent_prompts`. Both torii copies survived. This closes
    /// the listing's half; `cmd::human::the_question_a_late_ask_cannot_restate` closes the
    /// answering command's, so the two sides are pinned to the SAME first-wins rule they
    /// have to agree on.
    #[tokio::test]
    async fn list_paused_shows_the_first_question_a_node_published() {
        const RESTATED: &str = "Actually — approve the OTHER contract?";
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &reviewer(), None).await;
        // A second ask for the SAME node. The executor cannot write one (its fold is
        // first-wins too), but a corrupt or hand-written journal can, and the listing must
        // not disagree with `run agent answer` about which question is live.
        j.append(
            run,
            JournalEvent::AgentAwaited {
                node: reviewer(),
                deadline: None,
                prompt: RESTATED.to_string(),
            },
        )
        .await
        .unwrap();

        let out = list_paused(&s, &j, false).await.expect("lists");
        let row = awaiting_row(&out.text, run, "reviewer");
        assert!(
            row.contains(THE_QUESTION),
            "the FIRST question is the one a human was shown: {row}"
        );
        assert!(
            !row.contains(RESTATED),
            "a later ask must not retroactively change what the operator is answering — \
             `cmd::human::agent_question` validates against the first one: {row}"
        );
    }

    /// **This test was the inverse until Task 6.** It asserted that nothing told a
    /// human-backed agent from an `AwaitSignal` — a known gap, written as an assertion
    /// rather than a comment. It is inverted rather than deleted so the closure is proven
    /// on the same fixture that pinned the gap.
    ///
    /// **All THREE waiting kinds sit in ONE run**, and that is what makes the negative
    /// assertions mean anything: [`list_paused`] folds one journal PER RUN, so a question
    /// could not leak between runs however `awaiting_nodes` was written. In one fold, only
    /// the per-node keying stops it — the same lesson
    /// `list_paused_distinguishes_a_gate_from_an_await_signal` records, which found that
    /// mutating `menus.get(&node)` to `menus.values().next()` left its two-run fixture
    /// green.
    #[tokio::test]
    async fn list_paused_tells_a_human_backed_agent_from_an_await_signal() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &reviewer(), None).await;
        j.append(
            run,
            JournalEvent::SignalAwaited {
                node: gate(),
                deadline: None,
            },
        )
        .await
        .unwrap();
        j.append(
            run,
            JournalEvent::GateAwaited {
                node: release(),
                deadline: None,
                options: vec![orchestrator_core::GateOption {
                    name: "ship".into(),
                    outcome: orchestrator_core::GateOutcome::Complete,
                }],
            },
        )
        .await
        .unwrap();

        let out = list_paused(&s, &j, false).await.expect("lists");
        let agent = awaiting_row(&out.text, run, "reviewer");
        assert!(
            agent.contains("agent:") && agent.contains(THE_QUESTION),
            "{agent}"
        );
        assert!(
            // Keyed on the `gate:` PREFIX, not on the option name: the fixture question is
            // "Does this release look safe to ship?", which contains both `release` and
            // `ship` as ordinary words. A `contains("ship")` here fails on the correct
            // output — which it did, on first run.
            !agent.contains("gate:"),
            "a human-backed agent publishes no menu and must not borrow the gate's: {agent}"
        );
        let gate_row = awaiting_row(&out.text, run, "release");
        assert!(
            gate_row.contains("gate: ship") && !gate_row.contains(THE_QUESTION),
            "a gate keeps its menu and takes no question: {gate_row}"
        );
        let signal = awaiting_row(&out.text, run, "gate");
        assert!(
            !signal.contains(THE_QUESTION) && !signal.contains("agent:"),
            "an AwaitSignal has no question and must not borrow the agent's — they are in \
             the SAME fold, so only the per-node keying stops it: {signal}"
        );
        assert!(
            signal.split_whitespace().nth(2) == Some("signal"),
            "…and must still say what it IS waiting for: {signal}"
        );

        // The `--json` half: key PRESENCE is the discriminator a script reads, and it must
        // now separate all THREE kinds — `options` a gate, `question` a human-backed agent,
        // neither an `AwaitSignal`.
        let json = list_paused(&s, &j, true).await.expect("lists");
        let parsed: serde_json::Value = serde_json::from_str(&json.text).expect("valid JSON");
        let node_json = |name: &str| -> serde_json::Value {
            parsed[0]["awaiting"]
                .as_array()
                .expect("an awaiting array")
                .iter()
                .find(|n| n["node"] == name)
                .unwrap_or_else(|| panic!("no awaiting entry for {name}: {parsed}"))
                .clone()
        };
        let agent_json = node_json("reviewer");
        assert_eq!(
            agent_json["question"], THE_QUESTION,
            "a script must be able to read the question without the graph: {agent_json}"
        );
        assert!(
            agent_json.get("options").is_none(),
            "a human-backed agent publishes no menu: {agent_json}"
        );
        let signal_json = node_json("gate");
        assert!(
            signal_json.get("question").is_none() && signal_json.get("options").is_none(),
            "an AwaitSignal has neither, and `null` would change s1's JSON shape — a script \
             written against s1 must see byte-identical output: {signal_json}"
        );
    }

    /// A question is AUTHOR free text — an agent's `system_prompt`, its skill bodies, the
    /// rendered upstream context and the node's input — reaching a line-oriented table, so
    /// it carries exactly the hazards an option name does: a raw newline forges a line that
    /// reads as its own awaiting row,
    /// carrying a uuid an operator could paste into `run cancel`, and an ANSI escape can
    /// rewrite what is already on screen. This is the guard on `render::one_line` in the
    /// question cell; without it the forged line is real.
    #[tokio::test]
    async fn a_hostile_question_cannot_forge_an_awaiting_row_or_move_the_cursor() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let hostile = format!("ship it?\n{FORGED_RUN}  review  answered\u{1b}[2K");
        let j = agent_journal_asking(run, &reviewer(), None, &hostile).await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        assert!(
            !out.text.contains('\u{1b}'),
            "a raw escape byte survived into the table: {:?}",
            out.text
        );
        let forged = out
            .text
            .lines()
            .filter(|l| l.trim_start().starts_with(FORGED_RUN))
            .count();
        assert_eq!(
            forged, 0,
            "a newline in a question forged a line that reads as its own row:\n{}",
            out.text
        );
    }

    /// The `## Task` text of the compose-shaped fixture below — distinctive, so an assertion
    /// can prove the ask SURVIVED the cap rather than merely that something did.
    const THE_ASK: &str = "Approve the ACME MSA renewal?";

    /// The cell is CAPPED as well as collapsed — **and the cap must never eat the ASK**.
    ///
    /// The executor composes a question in a fixed order: the agent's system prompt, every
    /// activated skill body, the rendered `## Context` section of upstream outputs, and
    /// LAST a `## Task` section holding the node's input — the thing the human is actually
    /// being asked. The authored half alone may run to `MAX_HUMAN_TEXT_BYTES` (4096) and
    /// the context half to `MAX_HUMAN_CONTEXT_BYTES` (32768), so a multi-KB question is the
    /// NORMAL case here, not a hostile one, and the cap is load-bearing.
    ///
    /// A front-cut therefore shows ~290 characters of standing instructions and never one
    /// byte of the ask. That is the same defect `HumanQuestion::redact_and_clamp` was fixed
    /// for on the DURABLE row ("the clamp must never eat the ASK"), left in place on the one
    /// surface that displays it — `redact_question`'s own doc calls `list-paused` "the ONLY
    /// torii surface that ever displays one".
    ///
    /// The fixture is COMPOSE-SHAPED for that reason. The version this replaces used
    /// `"q".repeat(4_000)` — homogeneous, so a front-cut and a tail-preserving cut render
    /// identically and the assertion could not tell them apart.
    #[tokio::test]
    async fn an_overlong_question_is_capped_but_still_shows_the_ask() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        // `HumanQuestion::compose`'s exact order and delimiter.
        let long = format!(
            "You are a contract reviewer. {}\n\n## Context\n\n### brief\n{}\n\n## Task\n{}",
            "Follow the reviewing checklist carefully and cite clauses. ".repeat(40),
            "L".repeat(2_000),
            THE_ASK
        );
        let j = agent_journal_asking(run, &reviewer(), None, &long).await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        let row = awaiting_row(&out.text, run, "reviewer");
        assert!(
            row.chars().count() < 500,
            "an unbounded question wrecks the block: {} chars",
            row.chars().count()
        );
        assert!(row.contains('…'), "truncation must be visible: {row}");
        assert!(
            row.contains(THE_ASK),
            "the cap ate the ASK — the operator is shown the role's standing instructions \
             and no statement of what to decide, which is the one thing they need to \
             answer: {row}"
        );
    }

    /// Design §6's rule that EVERY operator-facing string goes through the redactor — not
    /// AC9, which is about the ANSWER (`--text` scrubbed in the journal and in the output).
    ///
    /// The question needs its own guard because torii applies a SECOND, display-oriented
    /// scrub (`render::redact_question`) over a value the executor has already redacted —
    /// `executor/human.rs` runs its redactor over the whole composed question before the
    /// `AgentAwaited` append. (An earlier version of this comment claimed the opposite and
    /// said it was "verified in `executor/human.rs`, which appends
    /// `prompt: prompt.to_string()`" — an expression that exists on no code path. The word
    /// "verified" is how this feature's false claims propagated: it invites the next reader
    /// to skip re-checking.)
    ///
    /// The second pass earns its place two ways. `Executor::with_redactor` is opt-in and
    /// defaults to `None`, so an embedder that wired none writes the question as composed —
    /// and its ingredients are an agent's `system_prompt` and skill bodies, operator-authored
    /// config pushed with `torii config push`, which redacts nothing (there is no `redact`
    /// anywhere in `orchestrator-store` or `cmd::config`). And the `--json` sink serializes
    /// `AwaitingNode` wholesale, so a credential would reach a script verbatim.
    #[tokio::test]
    async fn a_secret_shaped_question_is_redacted_in_the_listing() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        // Assembled at runtime: the repo's Semgrep CWE-798 hook blocks a credential-shaped
        // literal in a fixture.
        let secret = format!("sk-{}", "A".repeat(24));
        let j = agent_journal_asking(
            run,
            &reviewer(),
            None,
            &format!("call the API with {secret} and report the status"),
        )
        .await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        assert!(
            !out.text.contains(&secret),
            "the table leaked: {}",
            out.text
        );
        // The EXACT placeholder, not `contains("[REDACTED")`. Task 6's review caught that
        // the loose prefix is satisfied by `render::WITHHELD_REASON`
        // (`[REDACTED: reason withheld]`) as well as by the real placeholder, so a
        // transform that threw the whole question away — the one this test's sibling
        // `an_ordinary_prose_question_that_wraps_after_bearer_survives` exists to forbid —
        // would have passed here while destroying the feature.
        assert!(
            out.text.contains("[REDACTED]"),
            "the secret must be replaced by the placeholder: {}",
            out.text
        );
        // …and the rest of the question must SURVIVE the scrub. Redaction that takes the
        // prose with the secret leaves an operator unable to answer, which is the whole
        // point of showing the question here.
        let row = awaiting_row(&out.text, run, "reviewer");
        assert!(
            row.contains("call the API with") && row.contains("report the status"),
            "the words either side of the secret must still be there: {row}"
        );

        let json = list_paused(&s, &j, true).await.expect("lists");
        assert!(
            !json.text.contains(&secret),
            "a script must not receive a credential either — `render::json` redacts the \
             pause reason for exactly this reason: {}",
            json.text
        );
    }

    /// **The finding this test is the fix for.** The question was scrubbed with
    /// `render::redact_reason`, whose withhold-on-evasion transform discards the ENTIRE
    /// string when redacting-then-normalizing disagrees with normalizing-then-redacting.
    /// That is free for a pause reason — the value it was written for — and ruinous here:
    /// `list-paused` is the ONLY torii surface that ever displays a question
    /// (`cmd::human::agent_question` reads the prompt to validate the node, never echoes
    /// it), so a withheld question leaves an operator with no way whatsoever to learn what
    /// they are being asked.
    ///
    /// And the disagreement fires on ORDINARY PROSE. `PatternRedactor`'s
    /// `(?i)bearer\s+[A-Za-z0-9._-]{8,}` matches across the newline in
    /// `"…carries a bearer\nAuthorization header…"` — `\s` matches `\n` — but NOT in the
    /// control-stripped copy, where the two words glue into `bearerAuthorization` and the
    /// mandatory whitespace is gone. Equality-of-the-two-orders therefore reported an
    /// "evasion" for a question whose redaction was working exactly as intended. A question
    /// is the shape most exposed to it: multi-KB and newline-dense (system prompt + every
    /// activated skill body + `## Context` + `## Task`), and a security-reviewer's prompt is
    /// precisely the one that says the word "bearer".
    ///
    /// `render::redact_question` therefore withholds only in the DIRECTION the guard was
    /// written for — when reassembly hides MORE than the plain pass does. Here the plain
    /// pass hides more, so the question renders.
    #[tokio::test]
    async fn an_ordinary_prose_question_that_wraps_after_bearer_survives() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let question = "You are a contract reviewer. Confirm the request carries a bearer\n\
                        Authorization header before approving. Does this contract permit \
                        sub-processing?";
        let j = agent_journal_asking(run, &reviewer(), None, question).await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        let row = awaiting_row(&out.text, run, "reviewer");
        assert!(
            !row.contains("withheld"),
            "an ordinary prose question was thrown away wholesale — an operator cannot \
             answer what they cannot see: {row}"
        );
        assert!(
            row.contains("contract reviewer") && row.contains("sub-processing"),
            "the words of the question must reach the operator: {row}"
        );

        let json = list_paused(&s, &j, true).await.expect("lists");
        let parsed: serde_json::Value = serde_json::from_str(&json.text).expect("valid JSON");
        let q = parsed[0]["awaiting"][0]["question"]
            .as_str()
            .expect("a question")
            .to_string();
        assert!(
            !q.contains("withheld") && q.contains("sub-processing"),
            "the `--json` sink takes the same value and must not be withheld either: {q}"
        );
    }

    /// The other half of the pair, and the reason `redact_question` narrows the withhold
    /// rather than deleting it: a credential BISECTED by a control byte is reassembled by
    /// the strip-then-redact pass and by nothing else. `PatternRedactor`'s whole-match
    /// patterns are contiguous classes that exclude control characters, so the plain pass
    /// sees two 12-character runs, neither long enough to fire, and would print both halves
    /// of a live key to a terminal. When reassembly hides MORE than the plain pass, the
    /// question is withheld — the ambiguity is real and there is no honest partial
    /// rendering of a question whose secret we can locate only after gluing it back
    /// together.
    ///
    /// This is a guard test, green on arrival. Mutation-verified in a scratch copy: with
    /// `redact_question`'s reassembly comparison deleted (`return redacted;` before the
    /// check), the bisected key's halves both appear in the row and this test fails while
    /// `an_ordinary_prose_question_that_wraps_after_bearer_survives` still passes — so the
    /// two tests pin the two directions independently.
    #[tokio::test]
    async fn a_control_bisected_secret_in_a_question_is_still_withheld() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        // Assembled at runtime for the Semgrep CWE-798 hook, and split by a control byte
        // exactly as the evasion this guard exists for does.
        let half = "A".repeat(12);
        let bisected = format!("sk-{half}\u{1}{half}");
        let j = agent_journal_asking(
            run,
            &reviewer(),
            None,
            &format!("approve the deploy that uses {bisected}?"),
        )
        .await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        let row = awaiting_row(&out.text, run, "reviewer");
        assert!(
            !row.contains(&format!("sk-{half}")),
            "a bisected key's first half reached the terminal: {row}"
        );
        assert!(
            row.contains("withheld"),
            "the question must be withheld outright when reassembly finds what the plain \
             pass missed: {row}"
        );
    }

    /// An option name is AUTHOR free text that reaches a line-oriented table, so it
    /// carries exactly the hazards a pause reason does: a raw newline would forge a line
    /// that reads as its own awaiting row — carrying a uuid an operator could paste into
    /// `run cancel` — and an ANSI escape could rewrite what is already on screen. This is
    /// the guard on `render::one_line` in the menu cell; without it the forged line is
    /// real.
    #[tokio::test]
    async fn a_hostile_option_name_cannot_forge_an_awaiting_row_or_move_the_cursor() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let hostile = format!("ship\n{FORGED_RUN}  release  approved\u{1b}[2K");
        let j = gate_journal(run, &release(), None, &[&hostile]).await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        assert!(
            !out.text.contains('\u{1b}'),
            "a raw escape byte survived into the table: {:?}",
            out.text
        );
        let forged = out
            .text
            .lines()
            .filter(|l| l.trim_start().starts_with(FORGED_RUN))
            .count();
        assert_eq!(
            forged, 0,
            "a newline in an option name forged a line that reads as its own row:\n{}",
            out.text
        );
    }

    /// The cell is CAPPED as well as collapsed, for the same reason a node id is: an
    /// option name is unbounded by `validate_dag` (it checks non-empty, unique and
    /// outcome-reachable, never length), so one overlong name — or a fifty-option menu —
    /// would wreck the alignment of every other row in the block.
    #[tokio::test]
    async fn an_overlong_menu_is_capped_so_it_cannot_wreck_the_block() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let long = "s".repeat(5_000);
        let j = gate_journal(run, &release(), None, &[&long]).await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        let row = out
            .text
            .lines()
            .find(|l| l.contains("release"))
            .unwrap_or_else(|| panic!("no awaiting row:\n{}", out.text));
        assert!(
            row.chars().count() < 400,
            "an unbounded menu wrecks the block: {} chars",
            row.chars().count()
        );
        assert!(row.contains('…'), "truncation must be visible: {row}");
    }

    /// `--json` is the scripting surface, so the menu must reach it too — and, for a run
    /// with no gate, the s1 shape must be UNCHANGED: the key is absent rather than `null`,
    /// so a script written against s1 sees byte-identical output.
    #[tokio::test]
    async fn list_paused_json_carries_the_menu_and_omits_it_for_a_signal() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship", "hold"]).await;

        let out = list_paused(&s, &j, true).await.expect("lists");
        let v: serde_json::Value = serde_json::from_str(&out.text)
            .unwrap_or_else(|e| panic!("--json emitted non-JSON {:?}: {e}", out.text));
        assert_eq!(v[0]["awaiting"][0]["node"], "release", "{v}");
        assert_eq!(
            v[0]["awaiting"][0]["options"],
            serde_json::json!(["ship", "hold"]),
            "a script must be able to read the menu without the graph: {v}"
        );

        let signal_run = RunId(uuid::Uuid::new_v4());
        let s2 = paused_store(signal_run, None).await;
        let j2 = awaiting_journal(signal_run, &gate(), None).await;
        let out2 = list_paused(&s2, &j2, true).await.expect("lists");
        let v2: serde_json::Value = serde_json::from_str(&out2.text).expect("valid json");
        assert!(
            v2[0]["awaiting"][0].get("options").is_none(),
            "an AwaitSignal has no menu, and `null` would change s1's JSON shape: {v2}"
        );
    }

    // ---- Whole-slice review, Important: one bad journal must not hide the fleet -------

    /// A journal that FAILS to fold ONE run and delegates every other — the shape a
    /// durable `format_version` fence takes in a fleet mid-rolling-deploy, where one run
    /// was journaled by a newer binary.
    ///
    /// `fail` is a plain `fn` rather than a stored error so one double covers both the
    /// fence (the realistic trigger) and a backend fault whose free text is hostile (the
    /// leak test) — `JournalError` is not `Clone`, so it cannot simply be held.
    struct FailingLoadJournal {
        inner: Arc<InMemoryJournal>,
        fenced: RunId,
        fail: fn(RunId) -> orchestrator_core::JournalError,
    }

    #[async_trait::async_trait]
    impl ExecutionJournal for FailingLoadJournal {
        async fn append(
            &self,
            run: RunId,
            event: JournalEvent,
        ) -> Result<Seq, orchestrator_core::JournalError> {
            self.inner.append(run, event).await
        }
        async fn load(
            &self,
            run: RunId,
        ) -> Result<Vec<(Seq, JournalEvent)>, orchestrator_core::JournalError> {
            if run == self.fenced {
                return Err((self.fail)(run));
            }
            self.inner.load(run).await
        }
    }

    fn fence_error(run: RunId) -> orchestrator_core::JournalError {
        orchestrator_core::JournalError::IncompatibleFormat {
            run,
            stored: 2,
            expected: 1,
        }
    }

    /// A backend fault whose message carries everything a table cell must survive: a
    /// connection string with a password, a newline that would forge a second row, and an
    /// ANSI escape that would rewrite what is already on screen. Assembled at runtime —
    /// the repo's Semgrep CWE-798 hook blocks a credential-shaped literal in a fixture.
    ///
    /// `pub(crate)` for `cmd::gate`'s tests: `decide`'s post-append `unread` closure
    /// renders the SAME error class to stdout and needed the same guard. One hostile
    /// fixture, so the two commands cannot disagree about what a backend fault may carry.
    pub(crate) fn hostile_backend_error(_run: RunId) -> orchestrator_core::JournalError {
        orchestrator_core::JournalError::Backend(hostile_backend_message())
    }

    fn hostile_backend_message() -> String {
        format!(
            "pool timed out connecting to postgres://operator:{}@db.internal:5432/orch\
             \n{} is also stuck\u{1b}[2K",
            hostile_password(),
            FORGED_RUN
        )
    }

    pub(crate) fn hostile_password() -> String {
        format!("s3cr{}t", "e")
    }

    /// A uuid an operator could paste into `run cancel` if a forged line read as a row.
    pub(crate) const FORGED_RUN: &str = "deadbeef-dead-beef-dead-beefdeadbeef";

    /// Two paused runs, one of which cannot be folded, plus a shared journal in which the
    /// HEALTHY one has a node awaiting a signal.
    async fn two_paused_runs(
        healthy: RunId,
        fenced: RunId,
    ) -> (InMemorySchedulerStore, Arc<InMemoryJournal>) {
        let s = InMemorySchedulerStore::default();
        for (run, reason) in [
            (healthy, "await_signal: waiting for a signal on node gate"),
            (fenced, "quota: rate limited"),
        ] {
            s.enqueue(run, &empty_graph(), now()).await.unwrap();
            s.record_paused(run, None, reason).await.unwrap();
        }
        let j = Arc::new(InMemoryJournal::new());
        j.append(
            healthy,
            JournalEvent::SignalAwaited {
                node: gate(),
                deadline: None,
            },
        )
        .await
        .unwrap();
        (s, j)
    }

    /// THE regression this fix exists for. One run whose durable format was bumped used to
    /// abort the whole command — exit 1, EMPTY stdout — hiding the healthy run an operator
    /// could still signal, wake or cancel. A fence bump is exactly what a rolling deploy
    /// produces, i.e. the moment `list-paused` matters most.
    #[tokio::test]
    async fn list_paused_still_lists_every_other_run_when_one_journal_cannot_be_folded() {
        let healthy = RunId(uuid::Uuid::new_v4());
        let fenced = RunId(uuid::Uuid::new_v4());
        let (s, inner) = two_paused_runs(healthy, fenced).await;
        let j = FailingLoadJournal {
            inner,
            fenced,
            fail: fence_error,
        };

        let out = list_paused(&s, &j, false)
            .await
            .expect("a per-run journal fault must not abort the whole listing");

        assert!(
            out.text.contains(&healthy.0.to_string()),
            "the HEALTHY run must still be listed — it is the one an operator can act \
             on: {}",
            out.text
        );
        assert!(
            out.text.contains("gate"),
            "and its awaiting node must still be named: {}",
            out.text
        );
        assert!(
            out.text.contains(&fenced.0.to_string()),
            "the unfoldable run must still be listed: {}",
            out.text
        );
        assert!(
            out.text.contains("unknown:"),
            "its awaiting set is UNKNOWN — reporting it as empty would say there is \
             nothing to signal: {}",
            out.text
        );
        assert!(
            out.text.contains("stored 2, expected 1"),
            "and the fault itself must not be swallowed: {}",
            out.text
        );
        assert_eq!(
            out.code, EXIT_PRECONDITION,
            "a script must still learn the listing is incomplete: {}",
            out.text
        );
    }

    /// The same guarantee on the machine-readable path — and it must NOT be expressible as
    /// `"awaiting": []`, which says "nothing to signal" while meaning "unknown".
    #[tokio::test]
    async fn list_paused_json_carries_the_per_run_journal_failure() {
        let healthy = RunId(uuid::Uuid::new_v4());
        let fenced = RunId(uuid::Uuid::new_v4());
        let (s, inner) = two_paused_runs(healthy, fenced).await;
        let j = FailingLoadJournal {
            inner,
            fenced,
            fail: fence_error,
        };

        let out = list_paused(&s, &j, true).await.expect("lists");
        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        let v: serde_json::Value = serde_json::from_str(&out.text)
            .unwrap_or_else(|e| panic!("--json emitted non-JSON {:?}: {e}", out.text));
        let rows = v.as_array().expect("an array of runs");
        let find = |run: RunId| {
            rows.iter()
                .find(|r| r["run"] == serde_json::json!(run.0.to_string()))
                .unwrap_or_else(|| panic!("run {} is missing from {out:?}", run.0))
        };

        let ok = find(healthy);
        assert_eq!(ok["awaiting"][0]["node"], "gate", "{ok}");
        assert!(ok.get("awaiting_error").is_none(), "{ok}");

        let bad = find(fenced);
        assert!(
            bad["awaiting_error"]
                .as_str()
                .is_some_and(|e| e.contains("stored 2, expected 1")),
            "the per-run failure must be machine-readable: {bad}"
        );
        assert!(
            bad.get("awaiting").is_none(),
            "an empty awaiting array would tell a script there is nothing to signal: {bad}"
        );
    }

    /// The new rendering formats a driver's free text into a table CELL, so it inherits
    /// every hazard `safe_reason` exists for: a connection string with a password, a
    /// newline that would forge a row carrying a pastable uuid, and an ANSI escape.
    #[tokio::test]
    async fn list_paused_never_leaks_a_connection_string_or_forges_a_row_from_a_journal_fault() {
        let healthy = RunId(uuid::Uuid::new_v4());
        let fenced = RunId(uuid::Uuid::new_v4());
        let (s, inner) = two_paused_runs(healthy, fenced).await;
        let j = FailingLoadJournal {
            inner,
            fenced,
            fail: hostile_backend_error,
        };

        for json in [false, true] {
            let out = list_paused(&s, &j, json).await.expect("lists");
            assert!(
                !out.text.contains(&hostile_password()),
                "a journal fault leaked the database password (json={json}): {}",
                out.text
            );
            assert!(
                out.text.contains("[REDACTED]"),
                "the credential span must be visibly redacted (json={json}): {}",
                out.text
            );
            if !json {
                assert!(
                    !out.text.contains('\u{1b}'),
                    "a raw escape byte survived into the table: {:?}",
                    out.text
                );
                let forged = out
                    .text
                    .lines()
                    .filter(|l| l.trim_start().starts_with(FORGED_RUN))
                    .count();
                assert_eq!(
                    forged, 0,
                    "a newline in the fault forged a line that reads as its own run row:\n{}",
                    out.text
                );
            }
        }
    }

    /// Additivity: a run with no `AwaitSignal` node must render EXACTLY the pre-SP-6
    /// table and JSON — nothing appended, nothing reordered.
    #[tokio::test]
    async fn list_paused_is_byte_identical_for_a_run_with_no_awaiting_node() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let j = empty_journal();
        let rows = s.list_paused().await.unwrap();

        let text = list_paused(&s, &j, false).await.expect("lists");
        assert_eq!(text.text, render::table(&rows));
        let json = list_paused(&s, &j, true).await.expect("lists");
        assert_eq!(json.text, render::json(&rows).unwrap());
        // ...and the exit code stays 0. Only a run whose journal could not be folded
        // degrades it to 2; nothing else about the listing does.
        assert_eq!((text.code, json.code), (EXIT_OK, EXIT_OK));
    }

    /// The same AC6 claim, against the REAL durable backends rather than in-memory
    /// doubles — because "the secret is not in durable storage" is a claim about
    /// Postgres, and the in-memory journal cannot falsify it. Reads the row back through
    /// a SECOND `PostgresJournal` over its own connection, so nothing in-process is
    /// shared with the writer.
    ///
    /// Touches only its own freshly-generated run id (`status`/`record_paused`/
    /// `force_wake` are all run-scoped and no assertion here reads the global paused
    /// list), so it needs no `scheduled_runs` guard and cannot race another suite.
    #[cfg_attr(
        not(have_database_url),
        ignore = "needs a Postgres at $DATABASE_URL; see README, Postgres-backed tests"
    )]
    #[tokio::test]
    async fn a_signal_payload_is_redacted_before_it_reaches_postgres() {
        let Some(url) = crate::test_guard::db_url() else {
            return;
        };
        use orchestrator_store::postgres::{PostgresJournal, PostgresSchedulerStore, connect};

        let run = RunId(uuid::Uuid::new_v4());
        let store = PostgresSchedulerStore::new(connect(&url).await.expect("connect"));
        let journal = PostgresJournal::new(connect(&url).await.expect("connect"));

        store.enqueue(run, &empty_graph(), now()).await.unwrap();
        journal
            .append(
                run,
                JournalEvent::RunStarted {
                    version: "v1".into(),
                    budget: None,
                },
            )
            .await
            .unwrap();
        journal
            .append(
                run,
                JournalEvent::SignalAwaited {
                    node: gate(),
                    deadline: None,
                },
            )
            .await
            .unwrap();
        store
            .record_paused(run, None, "await_signal: waiting for a signal on node gate")
            .await
            .unwrap();

        let secret = format!("sk-{}", "A".repeat(24));
        let sent = chrono::Utc::now();
        let out = signal(
            &store,
            &journal,
            run,
            gate(),
            serde_json::json!({"decision": "approved", "note": format!("use {secret}")}),
            sent,
        )
        .await
        .expect("delivers");
        assert_eq!(out.code, EXIT_OK, "{}", out.text);

        // A FRESH reader over its own connection — the durable bytes, not this process's.
        let reader = PostgresJournal::new(connect(&url).await.expect("connect"));
        let events = reader.load(run).await.expect("load");
        let payload = events
            .iter()
            .find_map(|(_, e)| match e {
                JournalEvent::SignalReceived { node, payload } if node == &gate() => Some(payload),
                _ => None,
            })
            .expect("the delivery is durable");
        let durable = serde_json::to_string(payload).expect("serializes");
        assert!(
            !durable.contains(&secret),
            "the credential is in Postgres forever: {durable}"
        );
        assert!(durable.contains("[REDACTED]"), "{durable}");
        assert_eq!(payload["decision"], "approved");
        // And the wake really was queued, in the durable row.
        let row = store.status(run).await.unwrap().expect("a schedule record");
        assert_eq!(row.status, RunStatus::Paused);
        assert!(
            row.next_wake.is_some(),
            "a NULL-deadline gate must be made claimable by the delivery"
        );

        // Leave nothing behind for another suite to trip over. `cancel`, not
        // `record_terminal`: the latter is conditional on the row being `waking` (so a
        // concurrent cancel always wins) and would silently no-op on this paused row.
        store.cancel(run).await.expect("clean up the paused row");
        assert_eq!(
            store.status(run).await.unwrap().unwrap().status,
            RunStatus::Cancelled,
            "the cleanup must actually apply, or this test leaks a paused row"
        );
    }

    /// `--json` stays machine-parseable, with the awaiting set spliced in per row — the
    /// same technique `status` uses for `spent`/`budget`.
    #[tokio::test]
    async fn list_paused_json_carries_the_awaiting_node() {
        let run = RunId(uuid::Uuid::new_v4());
        let deadline = now() + chrono::Duration::hours(1);
        let s = paused_store(run, Some(deadline)).await;
        let j = awaiting_journal(run, &gate(), Some(deadline)).await;

        let out = list_paused(&s, &j, true).await.expect("lists");
        let v: serde_json::Value = serde_json::from_str(&out.text)
            .unwrap_or_else(|e| panic!("--json emitted non-JSON {:?}: {e}", out.text));
        assert_eq!(v[0]["awaiting"][0]["node"], "gate");
        assert_eq!(v[0]["awaiting"][0]["deadline"], "1970-02-04T18:20:00Z");
    }

    // ---- SP-6 s4: the FOURTH waiting kind, a human-decided LOOP gate ------------------

    /// AC17, the `run signal` corner of what is now a FOUR-way cross-refusal. A loop gate
    /// is answerable only by `LoopGateDecided`, so a raw payload here would be journaled,
    /// never read (`run_human_loop_gate` reads nothing else), and reported as delivered.
    ///
    /// Checked before `signal_state`, like both siblings: since this slice folds
    /// `LoopGateAwaited`, a loop gate DOES read as `Awaiting`, so without its own arm the
    /// generic path would not refuse this at all.
    #[tokio::test]
    async fn signal_on_a_loop_gate_is_refused_and_points_at_run_gate_decide() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = loop_gate_journal(run, &loop_gate(), None, &["revise", "ship"]).await;

        let out = signal(&s, &j, run, loop_gate(), approved(), now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("run gate decide"),
            "must name the verb that WOULD work: {}",
            out.text
        );
        // The KIND as well as the verb, and it needs its own assertion because the verb
        // cannot distinguish the two menu-bearing arms: BOTH point at `run gate decide`, so
        // with this arm rewritten to the `HumanGate` wording the whole torii suite stayed
        // green (measured) and the per-kind split `gate_menu`'s `PublishedMenu` return
        // exists for was inert here. Naming the kind is that split's whole purpose —
        // `lp/0/__gate__` is a synthesized path in no graph, and an operator told it is a
        // `HumanGate` goes looking for an authored node that does not exist.
        assert!(
            out.text.contains("Loop's human gate"),
            "must name what the node actually IS, not its menu-bearing sibling: {}",
            out.text
        );
        assert!(
            !out.text.contains("is a HumanGate"),
            "…and must not call it the authored kind: {}",
            out.text
        );
        assert!(
            journaled_signals(&j, run, &loop_gate()).await.is_empty(),
            "a raw payload must never become a durable answer to a loop gate"
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            None,
            "and must not queue a wake"
        );
    }

    /// AC18 — the listing renders a loop gate's QUESTION and its MENU, so an operator sees
    /// both what is being asked and what they may answer without reading a graph. There is
    /// no graph to read: the node's path is synthesized per iteration, so the journal is
    /// the only place either value exists.
    ///
    /// `signal_states`' `LoopGateAwaited` arm is the only thing that puts this kind in the
    /// awaited set at all — without it the node never appears here, an operator cannot
    /// decide what they cannot see, and the gate waits out its SLA and destroys the run.
    #[tokio::test]
    async fn list_paused_renders_a_loop_gates_question_and_menu() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = loop_gate_journal(run, &loop_gate(), None, &["revise", "ship"]).await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        assert_eq!(out.code, EXIT_OK);
        let row = awaiting_row(&out.text, run, "lp/0/__gate__");
        assert!(
            row.contains("revise") && row.contains("ship"),
            "the menu must be shown so an operator knows the choices: {row}"
        );
        assert!(
            row.contains(LOOP_GATE_QUESTION),
            "…and the question, which is the whole of what is being decided: {row}"
        );
        assert!(
            row.contains("loop gate:"),
            "the row must name the KIND: a loop gate takes `gate decide`, and a row \
             labelled `agent:` would send an operator to the one verb it refuses: {row}"
        );
        assert!(
            out.text.contains("run gate decide"),
            "the block must name the verb that works:\n{}",
            out.text
        );
        // …and the header must name the LABEL this fleet's rows actually carry, which
        // `contains("run gate decide")` above cannot see: the un-widened s2 wording
        // contains that substring verbatim, so with `any_loop_gate` forced to `false` the
        // whole suite stayed green (measured). An operator scanning the header for the
        // `loop gate:` label in front of them would find only advice about some other
        // kind of row and conclude it is not for them.
        assert!(
            out.text
                .contains("a `gate:` or `loop gate:` row takes a named option instead"),
            "the header must name the row label this fleet actually shows:\n{}",
            out.text
        );
        assert!(
            !out.text.contains("run agent answer"),
            "…and must NOT advertise the verb it refuses. A loop gate publishes a question, \
             so keying the agent header on `question.is_some()` alone prints `an agent: row \
             takes free text` for a fleet whose only waiting node takes no free text at \
             all:\n{}",
            out.text
        );

        // **The `--json` half, which is the scripting contract and the one sink where a
        // wrong shape is silent.** s3's discriminator block says key PRESENCE "must now
        // separate all THREE kinds"; s4 makes it four, and `AwaitingNode::options`' own doc
        // makes the load-bearing promise that a loop gate serializes BOTH keys so a
        // consumer that never heard of one still reads `options` first and builds
        // `gate decide`. A table assertion cannot see a serde attribute change.
        let json = list_paused(&s, &j, true).await.expect("lists");
        let parsed: serde_json::Value = serde_json::from_str(&json.text).expect("valid JSON");
        let entry = parsed[0]["awaiting"]
            .as_array()
            .expect("an awaiting array")
            .iter()
            .find(|n| n["node"] == "lp/0/__gate__")
            .unwrap_or_else(|| panic!("no awaiting entry for the loop gate: {parsed}"))
            .clone();
        assert_eq!(
            entry["options"],
            serde_json::json!(["revise", "ship"]),
            "a script must be able to build `gate decide --option` without the graph — and \
             for this kind there is no graph to fall back on: {entry}"
        );
        assert_eq!(
            entry["question"], LOOP_GATE_QUESTION,
            "…and must be able to show a human what the pick decides: {entry}"
        );
    }

    /// The negative half of the header pair, and the reason the widening is conditional at
    /// all: a fleet with no loop gate must see the s2 wording BYTE-IDENTICALLY.
    ///
    /// Asserted because the positive alone cannot see it — forcing `any_loop_gate` to
    /// `true` also left the whole suite green (measured), so both directions of that branch
    /// were unguarded. An s2 fleet told its rows may be labelled `loop gate:` goes looking
    /// for a label no row in front of them carries.
    #[tokio::test]
    async fn a_gate_only_fleets_header_keeps_the_s2_wording() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship", "hold"]).await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        assert!(
            out.text
                .contains("a `gate:` row takes a named option instead"),
            "the s2 header must be unchanged for a fleet with no loop gate:\n{}",
            out.text
        );
        assert!(
            !out.text.contains("loop gate:"),
            "…and must not advertise a label none of its rows carry:\n{}",
            out.text
        );
    }

    /// **The loop-gate twin of `a_secret_shaped_question_is_redacted_in_the_listing`, and
    /// the higher-risk of the two.**
    ///
    /// `awaiting_nodes` scrubs the loop gate's prompt at a SECOND call site of
    /// `render::redact_question`, distinct from the `AgentAwaited` one beside it, and that
    /// site was reached by no test: replacing it with `prompt.clone()` left the whole torii
    /// package green (measured — 251 lib + 24 cli, 0 failed). This slice has already
    /// shipped a leak on a new arm of an existing scrub twice, so an unguarded third is the
    /// same defect waiting.
    ///
    /// The second pass is owed for the reason [`render::AwaitingNode::question`] states:
    /// `Executor::with_redactor` is opt-in and defaults to `None`, and the config that
    /// feeds a role's `system_prompt` and its skill bodies arrives through
    /// `torii config push`, which redacts nothing. For a deployment that wired no redactor
    /// this line is the ONLY thing between a credential and both sinks.
    ///
    /// And a loop gate is the kind most exposed to it, not the least: `HumanQuestion::
    /// compose` puts the ITERATION OUTPUT — model text about the run — in the `## Context`
    /// half of the prompt that `LoopGateAwaited` journals.
    ///
    /// Both sinks are asserted. `--json` serializes `AwaitingNode` wholesale, and its own
    /// doc names it as the sink a scrub placed sink-side would be forgotten in.
    #[tokio::test]
    async fn a_secret_shaped_loop_gate_question_is_redacted_in_the_listing() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        // Assembled at runtime: the repo's Semgrep CWE-798 hook blocks a credential-shaped
        // literal in a fixture.
        let secret = format!("sk-{}", "A".repeat(24));
        let j = loop_gate_journal_asking(
            run,
            &loop_gate(),
            None,
            &["revise", "ship"],
            &format!("the draft calls the API with {secret} — is it good enough to ship?"),
        )
        .await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        assert!(
            !out.text.contains(&secret),
            "the table leaked: {}",
            out.text
        );
        // The EXACT placeholder, not `contains("[REDACTED")`: s3's own comment records that
        // the loose prefix is satisfied by `render::WITHHELD_REASON` too, so a transform
        // that threw the whole question away would pass while destroying the feature.
        assert!(
            out.text.contains("[REDACTED]"),
            "the secret must be replaced by the placeholder: {}",
            out.text
        );
        let row = awaiting_row(&out.text, run, "lp/0/__gate__");
        assert!(
            row.contains("the draft calls the API with") && row.contains("good enough to ship"),
            "…and the prose either side of it must survive, or an operator cannot decide: \
             {row}"
        );

        let json = list_paused(&s, &j, true).await.expect("lists");
        assert!(
            !json.text.contains(&secret),
            "a script must not receive a credential either — `--json` serializes \
             `AwaitingNode` wholesale: {}",
            json.text
        );
        let parsed: serde_json::Value = serde_json::from_str(&json.text).expect("valid JSON");
        let q = parsed[0]["awaiting"][0]["question"]
            .as_str()
            .expect("a question")
            .to_string();
        assert!(
            q.contains("[REDACTED]"),
            "the placeholder must reach the scripting sink too: {q}"
        );
    }

    /// An option name is AUTHOR free text that reaches a line-oriented table, and the
    /// `loop gate:` cell inherited the `gate:` cell's `one_line` collapse without
    /// inheriting its guard: deleting the collapse left the whole torii package green
    /// (measured).
    ///
    /// Nothing upstream bounds the content. `check_menu_option_names` rejects only empty
    /// and duplicate names — never length, never control characters — and design §7 records
    /// that an untrusted planner can author a `GateSpec::Human`, so this is untrusted text
    /// on a live `Expand` path. A raw newline forges a line that reads as its own awaiting
    /// row, carrying a uuid an operator could paste into `run cancel`, and an ESC rewrites
    /// what is already on screen.
    #[tokio::test]
    async fn a_hostile_loop_gate_option_name_cannot_forge_an_awaiting_row_or_move_the_cursor() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let hostile = format!("revise\n{FORGED_RUN}  release  approved\u{1b}[2K");
        let j = loop_gate_journal(run, &loop_gate(), None, &[&hostile]).await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        assert!(
            !out.text.contains('\u{1b}'),
            "a raw escape byte survived into the table: {:?}",
            out.text
        );
        let forged = out
            .text
            .lines()
            .filter(|l| l.trim_start().starts_with(FORGED_RUN))
            .count();
        assert_eq!(
            forged, 0,
            "a newline in a loop gate's option name forged a line that reads as its own \
             row:\n{}",
            out.text
        );
    }

    /// The other half of the inherited-but-unguarded pair: the `loop gate:` menu is CAPPED
    /// as well as collapsed. Deleting `cap_chars(.., MENU_MAX)` from the new cell also left
    /// the package green.
    ///
    /// The two halves of this cell are bounded SEPARATELY by design, so this asserts the
    /// MENU's bound specifically — the question is the fixture's short one, and a row that
    /// blew past the bound could only have done so through the menu.
    #[tokio::test]
    async fn an_overlong_loop_gate_menu_is_capped_so_it_cannot_wreck_the_block() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let long = "s".repeat(5_000);
        let j = loop_gate_journal(run, &loop_gate(), None, &[&long]).await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        let row = awaiting_row(&out.text, run, "lp/0/__gate__");
        assert!(
            row.chars().count() < 400,
            "an unbounded menu wrecks the block: {} chars",
            row.chars().count()
        );
        assert!(row.contains('…'), "truncation must be visible: {row}");
        assert!(
            row.contains(LOOP_GATE_QUESTION),
            "…and capping the menu must not cost the question, which is bounded on its own \
             budget precisely so a verbose menu cannot delete it: {row}"
        );
    }

    /// **The `## Task` tail reserve, on the label this commit invented it for.**
    ///
    /// `question_cell`'s reserve arithmetic was generalised from a hardcoded
    /// `"agent: \"\"".chars().count()` to `label.chars().count() + 2`, and the loop gate —
    /// which passes the EMPTY label, because its row leads with the menu instead — is the
    /// only caller that exercises the `0 + 2` case. It did so in production on every real
    /// loop gate and in no test: a `assert!(!label.is_empty())` probe inserted at the top of
    /// the marker arm left 251 tests passing, i.e. every test that reached the arm passed
    /// `"agent: "`.
    ///
    /// The fixture is COMPOSE-SHAPED because that is what the executor writes:
    /// `HumanQuestion::compose` appends `TASK_MARKER` and the ask unconditionally, and
    /// `run_human_loop_gate` composes through that same seam — so a real loop gate ALWAYS
    /// takes this arm. If the reserve mis-computes for the empty label the operator is
    /// shown the role's standing instructions and never the ask, on the only surface that
    /// displays a question.
    #[tokio::test]
    async fn an_overlong_loop_gate_question_is_capped_but_still_shows_the_ask() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        // `HumanQuestion::compose`'s exact order and delimiter — the `## Context` half is a
        // loop gate's ITERATION OUTPUT, which is where the multi-KB comes from.
        let long = format!(
            "You are a release gate. {}\n\n## Context\n\n### draft\n{}\n\n## Task\n{}",
            "Weigh the draft against the acceptance criteria. ".repeat(40),
            "L".repeat(2_000),
            THE_ASK
        );
        let j = loop_gate_journal_asking(run, &loop_gate(), None, &["revise", "ship"], &long).await;

        let out = list_paused(&s, &j, false).await.expect("lists");
        let row = awaiting_row(&out.text, run, "lp/0/__gate__");
        assert!(
            row.chars().count() < 500,
            "an unbounded question wrecks the block: {} chars",
            row.chars().count()
        );
        assert!(row.contains('…'), "truncation must be visible: {row}");
        assert!(
            row.contains(THE_ASK),
            "the cap ate the ASK — the operator is shown the gate role's standing \
             instructions and no statement of what to decide: {row}"
        );
        assert!(
            row.contains("loop gate: revise|ship"),
            "…and the menu, on its own budget, is untouched by a verbose question: {row}"
        );
    }

    /// One node, two asks — a `GateAwaited` and an `AgentAwaited` at the same id. The
    /// executor cannot write that (they are different node kinds, each arm writes one
    /// event), but a hand-written journal can, and SP-6 s4 gives the listing a reason to
    /// care: `(options, question)` both `Some` is now what MEANS a loop gate, so a node
    /// that set both by accident would render as a third kind it is not.
    ///
    /// It reads as the `HumanGate` every refusal path already treats it as, because
    /// `awaiting_nodes` resolves the kind by calling `cmd::gate::gate_menu` — the same
    /// function `cmd::run::signal` and `cmd::human::answer` refuse against — and attaches a
    /// question beside a menu only for its `Loop` variant. A listing that disagreed with
    /// the refusals about a node's kind sends an operator to a command that rejects them.
    ///
    /// The claim that the two agreed was originally an argument about ORDER ("both check
    /// the menu first"), and it was false in both directions when it was written: `answer`
    /// checked the question first, and the listing's own menu-first rule diverged from
    /// `gate_menu` on the other two-ask journal (see
    /// `the_listing_resolves_a_two_menu_journal_exactly_as_gate_decide_does`). Sharing the
    /// resolver is what makes it true rather than argued.
    #[tokio::test]
    async fn list_paused_reads_a_node_with_two_asks_as_the_gate_the_refusals_do() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship", "hold"]).await;
        j.append(
            run,
            JournalEvent::AgentAwaited {
                node: release(),
                deadline: None,
                prompt: THE_QUESTION.to_string(),
            },
        )
        .await
        .unwrap();

        let out = list_paused(&s, &j, false).await.expect("lists");
        let row = awaiting_row(&out.text, run, "release");
        assert!(
            row.contains("gate: ship|hold") && !row.contains("loop gate:"),
            "menu-first: the same kind `cmd::run::signal` refuses it as: {row}"
        );
        assert!(
            !row.contains(THE_QUESTION),
            "…and it must not also carry the question, which is what would make the PAIR \
             stop meaning `a loop gate`: {row}"
        );
    }

    /// **The OTHER two-ask journal — and the one the sibling above did not cover.**
    ///
    /// A node can carry a `GateAwaited` and a `LoopGateAwaited`, and there the two
    /// resolvers really did disagree. `cmd::gate::gate_menu` is a single `find_map` across
    /// BOTH variants, so it is first-wins in JOURNAL order; `awaiting_nodes` used to try
    /// its loop-gate map FIRST, unconditionally. For a journal that published a
    /// `GateAwaited` and then a `LoopGateAwaited` at one id the listing therefore rendered
    /// `loop gate: revise` while `gate decide` validated against `ship|hold` and refused
    /// `revise` outright — a listing that recites a vocabulary the command rejects, which
    /// is the exact misdirection the block's own comment forbids.
    ///
    /// Closed by making the listing resolve the KIND through `gate_menu` itself rather than
    /// through a parallel rule: ONE resolver, so there is no second order to drift. The
    /// assertion is written against `gate_menu`'s answer rather than against a literal, so
    /// it holds whichever way that first-wins rule is ever changed — what it forbids is the
    /// two disagreeing.
    ///
    /// Both journal orders, because only one of them was ever wrong and a fix that simply
    /// swapped the precedence would move the bug rather than remove it.
    #[tokio::test]
    async fn the_listing_resolves_a_two_menu_journal_exactly_as_gate_decide_does() {
        for gate_first in [true, false] {
            let run = RunId(uuid::Uuid::new_v4());
            let s = paused_store(run, None).await;
            let j = InMemoryJournal::new();
            let human = JournalEvent::GateAwaited {
                node: release(),
                deadline: None,
                options: vec![orchestrator_core::GateOption {
                    name: "ship".into(),
                    outcome: orchestrator_core::GateOutcome::Complete,
                }],
            };
            let lp = JournalEvent::LoopGateAwaited {
                node: release(),
                deadline: None,
                prompt: LOOP_GATE_QUESTION.to_string(),
                menu: vec![orchestrator_core::LoopGateOption {
                    name: "revise".into(),
                    stops: false,
                }],
            };
            let (first, second) = if gate_first { (human, lp) } else { (lp, human) };
            j.append(run, first).await.unwrap();
            j.append(run, second).await.unwrap();

            // What every REFUSAL path and `gate decide` itself resolve this node to.
            let events = j.load(run).await.unwrap();
            let resolved = crate::cmd::gate::gate_menu(&events, &release()).expect("a menu");
            let (label, option) = match &resolved {
                crate::cmd::gate::PublishedMenu::Human(_) => ("gate: ship", "ship"),
                crate::cmd::gate::PublishedMenu::Loop(_) => ("loop gate: revise", "revise"),
            };

            let out = list_paused(&s, &j, false).await.expect("lists");
            let row = awaiting_row(&out.text, run, "release");
            assert!(
                row.contains(label),
                "gate_first={gate_first}: the listing must show the kind and the menu \
                 `gate_menu` resolved ({label}), or an operator reads a vocabulary the \
                 command rejects: {row}"
            );

            // …and the option the row recites is one `decide` actually accepts. The row is
            // where an operator READS the name they will type, so agreement about the kind
            // is only half of it.
            let out =
                crate::cmd::gate::decide(&s, &j, run, release(), option, "alice", None, now())
                    .await
                    .expect("no hard error");
            assert_eq!(
                out.code, EXIT_OK,
                "gate_first={gate_first}: the option the listing published must be one \
                 `gate decide` accepts: {}",
                out.text
            );
        }
    }

    /// A SETTLED loop gate is one nobody is waiting on, and it must leave the listing.
    ///
    /// A `Loop` re-derives every iteration's gate on every drive and a loop gate journals
    /// no `NodeCompleted` and writes no blackboard entry — so without `LoopGateSettled`
    /// folding as this node's terminal marker, iteration 0's gate stays `Awaiting`
    /// forever, and a ten-iteration loop ends up advertising ten decided gates beside the
    /// one real ask. `LoopGateDecided` is deliberately NOT the marker: it folds last-wins
    /// so an operator can correct a decision before the run resumes, and the settlement is
    /// exactly the line after which "before" has passed.
    #[tokio::test]
    async fn list_paused_does_not_name_a_settled_loop_gate() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = loop_gate_journal(run, &loop_gate(), None, &["revise", "ship"]).await;
        j.append(
            run,
            JournalEvent::LoopGateSettled {
                node: loop_gate(),
                option: "revise".into(),
            },
        )
        .await
        .unwrap();
        // The NEXT iteration's gate — a different synthetic path — is the one genuinely
        // waiting, and it must still be listed. Without it this test would also pass on a
        // listing that had simply stopped rendering loop gates altogether.
        let live = NodeId("lp/1/__gate__".into());
        j.append(
            run,
            JournalEvent::LoopGateAwaited {
                node: live.clone(),
                deadline: None,
                prompt: LOOP_GATE_QUESTION.to_string(),
                menu: vec![orchestrator_core::LoopGateOption {
                    name: "ship".into(),
                    stops: true,
                }],
            },
        )
        .await
        .unwrap();

        let out = list_paused(&s, &j, false).await.expect("lists");
        let (_, block) = out.text.split_once("AWAITING").expect("an awaiting block");
        assert!(
            !block.contains("lp/0/__gate__"),
            "a gate whose decision has been honoured is not awaiting anything:\n{block}"
        );
        assert!(
            block.contains("lp/1/__gate__"),
            "…while the iteration that IS asking must still be listed:\n{block}"
        );
    }
}
