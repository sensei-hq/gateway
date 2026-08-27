//! `torii run gate` — the typed operator surface for a `HumanGate` (SP-6 s2).
//!
//! [`crate::cmd::run::signal`] delivers arbitrary JSON to an `AwaitSignal`. This delivers
//! a NAMED CHOICE to a gate that published a menu, so the answer carries an outcome
//! (`Complete` or `Fail`) instead of being a value the graph author must remember to test
//! for. The two commands refuse each other's node kinds, in both directions — see
//! [`decide`]'s `AwaitSignal` arm and `signal`'s `HumanGate` arm.
//!
//! Everything that was hard-won in `signal` is reproduced here rather than re-derived:
//! append THEN `force_wake`, the effect read back rather than assumed, and a post-append
//! fault reported as a durable-but-unqueued answer instead of a bare store error. Read
//! that function before changing this one.

use crate::cmd::Outcome;
use crate::errors::CliError;
use crate::render;
use chrono::{DateTime, Utc};
use orchestrator_core::{
    ExecutionJournal, GateOption, GateOutcome, JournalEvent, NodeId, OrchestratorError, RunId,
    RunStatus, SchedulerStore, Seq,
};

/// Deliver a typed decision to a `HumanGate` (SP-6 s2).
///
/// **The menu comes from the JOURNAL, not the graph.** `GateAwaited` records what the
/// human was actually shown; validating against a graph that may since have been edited
/// would defeat the durability §4 requires — and nothing binds the graph in hand to the
/// one the human saw (there is no graph fence, and `Executor::start` takes the graph as a
/// caller parameter). This is the same rule the executor's `run_human_gate` follows on
/// its side, deliberately: two enforcements of one rule, which must not drift.
///
/// **This check is advisory and the executor re-checks.** It is non-atomic — it reads the
/// menu, then appends — and the library entry point bypasses it entirely. It exists to
/// refuse cheaply and to keep a bad row out of the journal, not to be the authority.
/// That matters more here than the word "advisory" suggests: `run_human_gate` fails the
/// node LOUDLY on an undeclared option, and a `NodeFailed` on a waiting node is
/// irreversible by design (the fail-closed rule that stops a late decision resurrecting
/// an expired gate). So at the durable layer a mistyped option is a terminal typo, and
/// this pre-check is the only thing between an operator and it.
///
/// **What it deliberately does NOT check: whether this gate has already been answered or
/// has already terminated.** A run whose gate failed is itself terminal, so the status
/// check below catches that case; a gate that already COMPLETED inside a run still paused
/// elsewhere is not detected, and a second decision would sit on the journal as a
/// last-wins value that a re-`start` of the run would fold. That residue is identical to
/// `signal`'s — see its `not read` arm — and telling the two apart needs a gate-state
/// fold that does not exist yet.
///
/// **Order: append, THEN `force_wake`** — never the reverse, for the same reason
/// `signal` and `wake` do it: `force_wake` only flips `next_wake`, and a worker in another
/// process can claim that wake the instant it lands. Appending first guarantees any worker
/// that can observe the wake folds a journal that already contains the decision.
// Eight parameters, over clippy's seven: the alternative is a struct that exists only to
// satisfy the lint, and `signal` — the function this one is deliberately shaped like —
// takes its arguments the same way. The three that would be grouped (option/actor/note)
// are not a domain concept on their own; they are the decision's three fields, and naming
// them here is what makes the call sites readable.
#[allow(clippy::too_many_arguments)]
pub async fn decide(
    store: &dyn SchedulerStore,
    journal: &dyn ExecutionJournal,
    run: RunId,
    node: NodeId,
    option: &str,
    actor: &str,
    note: Option<&str>,
    now: DateTime<Utc>,
) -> Result<Outcome, CliError> {
    // A node id is operator-supplied free text and every message below echoes it back to
    // a terminal, so control characters are collapsed for DISPLAY — a raw newline or an
    // ANSI escape in the echoed id would let the reported outcome forge extra lines or
    // rewrite what is already on screen. Same reasoning, same helper, as `signal`.
    let shown = render::one_line(&node.0);

    let Some(before) = store.status(run).await? else {
        return Ok(Outcome::precondition(format!("no such run: {}", run.0)));
    };
    let events = journal
        .load(run)
        .await
        .map_err(OrchestratorError::Journal)?;

    // The menu comes from the JOURNAL. Absent ⇒ this node has not asked yet (or is not a
    // gate at all), and there is nothing to validate an option against.
    let Some(menu) = gate_menu(&events, &node) else {
        return Ok(Outcome::precondition(if awaiting_signal(&events, &node) {
            format!(
                "not delivered: {shown} is an AwaitSignal, not a HumanGate — it takes \
                 arbitrary JSON, not a named option. Use: torii run signal {} --node \
                 {shown} --payload '<json>'",
                run.0
            )
        } else {
            format!(
                "not delivered: {shown} is not awaiting a decision. \
                 `torii run list-paused` names the nodes that are."
            )
        }));
    };

    let Some(chosen) = menu.iter().find(|o| o.name == option) else {
        return Ok(Outcome::precondition(format!(
            "not delivered: gate {shown} has no option {option:?}. Its options are: {}. \
             Use: torii run gate decide {} --node {shown} --option <name>",
            menu.iter()
                .map(|o| o.name.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            run.0
        )));
    };

    // A Fail option must record WHY. CLI-layer only, deliberately: `GateDecided.note`
    // stays `Option` because a `Complete` decision legitimately has none, and an absent
    // reason is a DOCUMENTATION failure, not a safety one — nothing downstream is unsafe
    // without it, so the executor must not refuse a decision over it. Enforcing it here
    // is what makes "why did this run stop" answerable from the journal alone.
    //
    // Trimmed, not merely present: clap's `required` cannot see that `--reason ''` is the
    // same omission with quotes around it, so this is the check that actually holds.
    if chosen.outcome == GateOutcome::Fail && note.map(str::trim).unwrap_or("").is_empty() {
        return Ok(Outcome::precondition(format!(
            "not delivered: {option:?} stops the run, so it needs a reason. \
             Use: torii run gate reject {} --node {shown} --reason '<why>'",
            run.0
        )));
    }

    if before.status != RunStatus::Paused {
        // A `waking` row means a worker holds the lease and is folding this journal right
        // now; a terminal row means nothing will ever read the decision. Neither is a
        // state to write into — but they call for OPPOSITE advice, and `signal` shipped
        // one message for both, which handed an operator of a cancelled run "retry once
        // it shows paused": advice to wait forever, since no shipped store moves a
        // terminal row back to `paused`.
        return Ok(Outcome::precondition(
            if before.status == RunStatus::Waking {
                format!(
                    "not delivered: {shown} is awaiting a decision, but the run is waking — a \
                 worker holds the lease and is folding this journal right now. Retry once \
                 `torii run status {}` shows it paused.",
                    run.0
                )
            } else {
                format!(
                    "not delivered: {shown} is awaiting a decision, but the run is {} — a {} \
                 run is never paused again, so nothing will ever read a decision \
                 delivered to it. Start a new run.",
                    before.status.as_str(),
                    before.status.as_str()
                )
            },
        ));
    }

    // Redact BEFORE the write, and with the same pure pass the executor applies on the
    // fold-read (`Executor::redact` over the `{decision,actor,note}` object), so live ==
    // journaled == replayed. Redacting on only one side is what produces a false
    // `DeterminismViolation`, and this codebase has shipped that defect twice. The note is
    // not merely displayed: on a `Complete` option it becomes this node's OUTPUT and flows
    // into downstream nodes and model prompts. Double-scrubbing is a non-issue —
    // `[REDACTED]` matches no credential shape, so the pass is idempotent.
    //
    // "The same pass" is literal, not approximate, and it is worth stating WHY rather than
    // assuming it: `Executor`'s redactor is opt-in and defaults to `None`, so the two
    // agree only because `boot::heavy` wires `PatternRedactor::default()` — the very
    // redactor `render::redact_payload` holds. The determinism argument does not actually
    // DEPEND on that (this scrub happens before the write, so every reader folds the same
    // journaled bytes either way); what depends on it is the claim that a note torii wrote
    // is byte-identical to one the executor would have produced from the raw text.
    //
    // The non-string arm is fail-CLOSED, unlike the executor's `redact_text` (which keeps
    // the original message). `Redactor::redact(&Value) -> Value` promises nothing about
    // preserving the variant, and only `PatternRedactor` happens to map a string to a
    // string; a third-party impl that did not would leak here. The executor keeps the
    // original because discarding it loses the only record of why a run failed. This path
    // has no such stake: a decision is re-issuable while the run is still paused, so
    // losing the note costs a retype and leaking it is permanent.
    let note = note.map(|n| {
        render::redact_payload(&serde_json::json!(n))
            .as_str()
            .unwrap_or("[REDACTED]")
            .to_string()
    });

    // The appended seq is KEPT, not discarded: it is what names the durable row in the
    // post-append fault report below, so an operator can find the write that succeeded.
    let appended = journal
        .append(
            run,
            JournalEvent::GateDecided {
                node: node.clone(),
                option: option.to_string(),
                // Collapsed on the way IN, not just on the way out — unlike the node id,
                // which is journaled as given. `actor` is interpolated by the executor
                // into a `NodeFailed` message that `torii run status` renders and that a
                // later drive re-emits from the fold, so an escape sequence smuggled
                // through `--as` would be replayed at every operator who reads the run.
                actor: render::one_line(actor),
                note,
            },
        )
        .await
        .map_err(OrchestratorError::Journal)?;

    // Past here the decision is DURABLE. Every remaining call reports rather than `?`s —
    // a bare store error reads as "it did not go through" for a write that succeeded, and
    // for an indefinite gate (`next_wake` NULL, never auto-woken) the run would then wait
    // forever on a decision nobody knows landed. Identical in shape and in reason to
    // `cmd::run::signal`'s `unread` closure.
    let unread = |what: &str, e: &dyn std::fmt::Display| {
        Outcome::precondition(format!(
            "not queued: {shown}'s decision is journaled durably (seq {appended}), but \
             {what} failed: {e}. Nothing has read it yet and the run is not queued to \
             resume — run `torii run wake {}` to drive it.",
            run.0
        ))
    };
    if let Err(e) = store.force_wake(run, now).await {
        return Ok(unread("the wake", &e));
    }
    let after = match store.status(run).await {
        Ok(Some(s)) => s,
        Ok(None) => {
            return Ok(unread(
                "the status re-read",
                &"the run vanished mid-decision",
            ));
        }
        Err(e) => return Ok(unread("the status re-read", &e)),
    };

    // The effect actually achieved, read back rather than assumed. STATUS plus the pinned
    // timestamp, exactly as `wake` and `signal` check their own: `claim_due` leaves a
    // stale `next_wake` untouched and an unrelated re-pause can restore `paused` inside
    // the race window, so neither field alone proves OUR wake applied. The 2µs tolerance
    // is a `timestamptz` rounding allowance, not a clock-skew fudge.
    let queued = after.status == RunStatus::Paused
        && after.next_wake.is_some_and(|t| {
            let drift = if t >= now { t - now } else { now - t };
            drift <= chrono::Duration::microseconds(2)
        });
    Ok(if queued {
        // Says QUEUED, never RESUMED: `force_wake` only sets `next_wake`; a worker tick
        // does the driving.
        Outcome::ok(format!(
            "decided: {shown} = {option} (the run will resume on the next worker tick)"
        ))
    } else {
        Outcome::precondition(format!(
            "not queued: {shown}'s decision is journaled durably, but the run is {} and \
             the wake did not apply. Run `torii run wake {}` once it is paused again.",
            after.status.as_str(),
            run.0
        ))
    })
}

/// The menu a `HumanGate` published, folded from `GateAwaited`. FIRST wins, matching the
/// executor's fold — two copies of one rule, so they must not drift.
///
/// `None` = this node never asked, which is what distinguishes a gate from an
/// `AwaitSignal` without loading the graph. `cmd::run::signal` reads it for exactly that:
/// a `Some` here is a `HumanGate`, and a raw payload must be refused.
pub(crate) fn gate_menu(events: &[(Seq, JournalEvent)], node: &NodeId) -> Option<Vec<GateOption>> {
    events.iter().find_map(|(_, e)| match e {
        JournalEvent::GateAwaited {
            node: n, options, ..
        } if n == node => Some(options.clone()),
        _ => None,
    })
}

/// Whether this node is awaiting a RAW signal — used only to give the right cross-refusal.
fn awaiting_signal(events: &[(Seq, JournalEvent)], node: &NodeId) -> bool {
    events
        .iter()
        .any(|(_, e)| matches!(e, JournalEvent::SignalAwaited { node: n, .. } if n == node))
}

/// Resolve `--as` to the actor string journaled on `GateDecided`.
///
/// **ATTRIBUTION, NOT AUTHENTICATION.** It is whatever string the caller supplied, so it
/// answers "who claimed to decide", never "who decided" — anyone who can reach the
/// database can write any actor. The help says so in those words, guarded by a
/// binary-level test, because an operator reading a `--as` flag will otherwise reasonably
/// assume it is authenticated.
///
/// Lives in the library, not in `main.rs`, so the fallback chain is testable: the binary
/// is deliberately thin (clap plus `dispatch`) and has no test module.
pub fn actor_or_user(supplied: &str) -> String {
    actor_or(supplied, std::env::var("USER").ok().as_deref())
}

/// [`actor_or_user`]'s rule, pure over the environment lookup so all three cases can be
/// tested without mutating this process's environment — which is `unsafe` in edition 2024
/// and global to every test running in parallel.
///
/// Never yields an empty actor: `GateDecided.actor` is what an audit reads, and a blank
/// one is indistinguishable from a bug, so an unresolvable actor is named `unknown`.
fn actor_or(supplied: &str, from_env: Option<&str>) -> String {
    if !supplied.trim().is_empty() {
        return supplied.trim().to_string();
    }
    from_env
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .unwrap_or("unknown")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::cmd::run::tests::{FailingForceWakeStore, awaiting_journal, now, paused_store};
    use crate::errors::{EXIT_OK, EXIT_PRECONDITION};
    use orchestrator_core::{
        GateOption, GateOutcome, Graph, JournalEvent, NodeId, RunId, RunStatus, SchedulerStore,
    };
    use orchestrator_store::{InMemoryJournal, InMemorySchedulerStore};

    fn release() -> NodeId {
        NodeId("release".into())
    }

    fn gopt(name: &str, outcome: GateOutcome) -> GateOption {
        GateOption {
            name: name.to_string(),
            outcome,
        }
    }

    /// A journal in which `node` has already ASKED, with the given menu. Every option is
    /// `Complete` except one literally named "reject", which is `Fail` — enough to
    /// exercise the required-reason rule without a second helper.
    async fn gate_journal(
        run: RunId,
        node: &NodeId,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
        options: &[&str],
    ) -> InMemoryJournal {
        let j = InMemoryJournal::new();
        j.append(
            run,
            JournalEvent::GateAwaited {
                node: node.clone(),
                deadline,
                options: options
                    .iter()
                    .map(|o| {
                        gopt(
                            o,
                            if *o == "reject" {
                                GateOutcome::Fail
                            } else {
                                GateOutcome::Complete
                            },
                        )
                    })
                    .collect(),
            },
        )
        .await
        .unwrap();
        j
    }

    /// Every `GateDecided` journaled for `node`, as `(option, actor, note)`.
    async fn journaled_decisions(
        j: &InMemoryJournal,
        run: RunId,
        node: &NodeId,
    ) -> Vec<(String, String, Option<String>)> {
        j.load(run)
            .await
            .unwrap()
            .into_iter()
            .filter_map(|(_, e)| match e {
                JournalEvent::GateDecided {
                    node: n,
                    option,
                    actor,
                    note,
                } if &n == node => Some((option, actor, note)),
                _ => None,
            })
            .collect()
    }

    /// `gate reject` with no reason, i.e. what the library sees when clap is bypassed.
    async fn reject_without_reason(
        s: &dyn SchedulerStore,
        j: &InMemoryJournal,
        run: RunId,
        node: NodeId,
        actor: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Outcome, CliError> {
        decide(s, j, run, node, "reject", actor, None, now).await
    }

    /// AC8: an undeclared option is refused BEFORE anything is journaled. The CLI reads
    /// the menu from the journaled `GateAwaited`, not the graph, so it validates against
    /// what the human was actually shown.
    #[tokio::test]
    async fn an_undeclared_option_is_refused_before_anything_is_journaled() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship", "hold"]).await;

        let out = decide(&s, &j, run, release(), "shipp", "alice", None, now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("ship") && out.text.contains("hold"),
            "must name the real menu so the operator can retry: {}",
            out.text
        );
        assert!(
            journaled_decisions(&j, run, &release()).await.is_empty(),
            "a refused decision must leave NOTHING durable"
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            None,
            "and must not queue a wake"
        );
    }

    /// AC10: a Fail option demands a reason. Failing a run without recording why is the
    /// ops equivalent of a bare `catch {}`.
    #[tokio::test]
    async fn a_fail_option_without_a_reason_is_refused() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship", "reject"]).await;

        let out = reject_without_reason(&s, &j, run, release(), "alice", now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(out.text.contains("reason"), "{}", out.text);
        assert!(journaled_decisions(&j, run, &release()).await.is_empty());
    }

    /// A blank `--reason` is the same omission with a quote around it, and clap's
    /// `required` cannot see it — so the trim is what actually enforces AC10 against the
    /// operator who wants the refusal to go away.
    #[tokio::test]
    async fn a_fail_option_with_a_blank_reason_is_refused() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship", "reject"]).await;

        let out = decide(
            &s,
            &j,
            run,
            release(),
            "reject",
            "alice",
            Some("  \t "),
            now(),
        )
        .await
        .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(out.text.contains("reason"), "{}", out.text);
        assert!(journaled_decisions(&j, run, &release()).await.is_empty());
    }

    /// A legitimate decision IS journaled and DOES queue the wake — the guard that the
    /// two refusal tests above are not vacuously passing because nothing ever works.
    #[tokio::test]
    async fn a_declared_option_is_journaled_and_queues_the_wake() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship", "hold"]).await;

        let out = decide(&s, &j, run, release(), "ship", "alice", Some("ok"), now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_OK, "{}", out.text);
        let decisions = journaled_decisions(&j, run, &release()).await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].0, "ship", "the option");
        assert_eq!(decisions[0].1, "alice", "the actor");
        assert_eq!(decisions[0].2.as_deref(), Some("ok"), "the note");
        assert!(
            s.status(run).await.unwrap().unwrap().next_wake.is_some(),
            "the run must be queued to resume"
        );
    }

    /// A `Fail` option WITH a reason is delivered — the rule is "record why", not
    /// "rejections are harder to deliver than approvals". Without this, dropping the
    /// whole `Fail` branch would still leave the suite green.
    #[tokio::test]
    async fn a_fail_option_with_a_reason_is_journaled() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship", "reject"]).await;

        let out = decide(
            &s,
            &j,
            run,
            release(),
            "reject",
            "alice",
            Some("the canary suite is red"),
            now(),
        )
        .await
        .expect("no hard error");

        assert_eq!(out.code, EXIT_OK, "{}", out.text);
        let decisions = journaled_decisions(&j, run, &release()).await;
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].0, "reject");
        assert_eq!(decisions[0].2.as_deref(), Some("the canary suite is red"));
    }

    /// AC9, the torii half: a secret-shaped note is redacted BEFORE the durable write.
    /// The credential is assembled at runtime — the repo's Semgrep CWE-798 hook blocks a
    /// literal one in a fixture.
    #[tokio::test]
    async fn a_secret_shaped_note_is_redacted_before_it_is_journaled() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship"]).await;
        let secret = format!("sk-{}", "A".repeat(24));

        decide(
            &s,
            &j,
            run,
            release(),
            "ship",
            "alice",
            Some(&format!("use {secret} to deploy")),
            now(),
        )
        .await
        .expect("delivers");

        let durable = format!("{:?}", j.load(run).await.unwrap());
        assert!(
            !durable.contains(&secret),
            "the note reached durable storage in plaintext: {durable}"
        );
        assert!(durable.contains("[REDACTED]"), "{durable}");
    }

    /// AC7, half one: a `HumanGate` decision aimed at an `AwaitSignal` node is refused,
    /// and the refusal names the command that WOULD work. `run signal`'s symmetric
    /// refusal lives in `cmd::run`'s tests.
    #[tokio::test]
    async fn a_decision_aimed_at_an_await_signal_node_points_at_run_signal() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = awaiting_journal(run, &release(), None).await;

        let out = decide(&s, &j, run, release(), "ship", "alice", None, now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("AwaitSignal"),
            "must name what the node actually is: {}",
            out.text
        );
        assert!(
            out.text.contains("run signal"),
            "must name the command that would work: {}",
            out.text
        );
        assert!(journaled_decisions(&j, run, &release()).await.is_empty());
    }

    /// A node that never asked has no menu, so there is nothing to validate against and
    /// nothing durable to write. The refusal points at the command that lists what IS
    /// waiting rather than leaving the operator to guess at node ids.
    #[tokio::test]
    async fn a_decision_for_a_node_that_never_asked_is_refused() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = InMemoryJournal::new();

        let out = decide(&s, &j, run, release(), "ship", "alice", None, now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(out.text.contains("list-paused"), "{}", out.text);
        assert!(journaled_decisions(&j, run, &release()).await.is_empty());
    }

    #[tokio::test]
    async fn a_decision_for_an_unknown_run_is_refused() {
        let s = InMemorySchedulerStore::default();
        let run = RunId(uuid::Uuid::new_v4());
        let j = InMemoryJournal::new();

        let out = decide(&s, &j, run, release(), "ship", "alice", None, now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(out.text.contains("no such run"), "{}", out.text);
    }

    /// An enqueued-but-not-yet-paused run — `InMemorySchedulerStore::enqueue` leaves the
    /// row `waking`, which is also the only status `record_terminal` will transition
    /// from, so this is the base for both the waking and the terminal fixtures.
    async fn waking_store(run: RunId) -> InMemorySchedulerStore {
        let s = InMemorySchedulerStore::default();
        s.enqueue(run, &Graph { nodes: vec![] }, now())
            .await
            .unwrap();
        s
    }

    /// A terminal run is never paused again by any shipped store, so nothing would ever
    /// fold the decision — and the advice must not be "retry once it shows paused", which
    /// is advice to wait forever. `record_terminal` journals no node event, so the gate
    /// still folds as having asked on a run that is over: that is how an operator reaches
    /// this path. Same lesson `run signal` learned.
    #[tokio::test]
    async fn a_decision_on_a_terminal_run_does_not_advise_waiting_for_a_pause() {
        for terminal in [
            RunStatus::Cancelled,
            RunStatus::Completed,
            RunStatus::Failed,
        ] {
            let run = RunId(uuid::Uuid::new_v4());
            let s = waking_store(run).await;
            s.record_terminal(run, terminal, None).await.unwrap();
            let j = gate_journal(run, &release(), None, &["ship"]).await;

            let out = decide(&s, &j, run, release(), "ship", "alice", None, now())
                .await
                .expect("no hard error");

            assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
            assert!(
                out.text.contains(terminal.as_str()),
                "must name the actual state: {}",
                out.text
            );
            assert!(
                !out.text.contains("Retry") && !out.text.contains("shows it paused"),
                "a {} run never pauses again — this is advice to wait forever: {}",
                terminal.as_str(),
                out.text
            );
            assert!(
                journaled_decisions(&j, run, &release()).await.is_empty(),
                "nothing may be written into a run that is over"
            );
        }
    }

    /// `waking` is TRANSIENT — a worker holds the lease and is folding this journal right
    /// now — so retrying IS real advice here, and this is the arm that must give it. The
    /// opposite of the terminal arm above, which is why one message for both was wrong.
    #[tokio::test]
    async fn a_decision_on_a_waking_run_is_worth_retrying_and_says_so() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = waking_store(run).await;
        let j = gate_journal(run, &release(), None, &["ship"]).await;

        let out = decide(&s, &j, run, release(), "ship", "alice", None, now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("shows it paused"),
            "a waking run is worth retrying, and the operator must be told so: {}",
            out.text
        );
        assert!(
            journaled_decisions(&j, run, &release()).await.is_empty(),
            "a worker holds the lease and is folding this journal — nothing may be written"
        );
    }

    /// The ORDER discriminator, exactly as `run signal` has one: swap the append and the
    /// `force_wake` and the injected store failure short-circuits before the append,
    /// leaving zero `GateDecided` rows. Appending first is what guarantees any worker
    /// that can observe the wake folds a journal that already holds the decision.
    #[tokio::test]
    async fn a_decision_is_appended_before_force_wake_and_its_failure_surfaces() {
        let run = RunId(uuid::Uuid::new_v4());
        let store = FailingForceWakeStore(paused_store(run, None).await);
        let j = gate_journal(run, &release(), None, &["ship"]).await;

        let out = decide(&store, &j, run, release(), "ship", "alice", None, now())
            .await
            .expect("a post-append fault is reported, not returned as a bare error");

        assert_ne!(
            out.code, EXIT_OK,
            "the injected force_wake failure must surface, not be swallowed into a green \
             success: {}",
            out.text
        );
        assert_eq!(
            journaled_decisions(&j, run, &release()).await.len(),
            1,
            "the decision must already be durable even though force_wake failed"
        );
        assert!(
            out.text.contains("journaled durably"),
            "a post-append fault must not read as 'it did not go through': {}",
            out.text
        );
        assert!(
            out.text.contains("run wake"),
            "must name what unblocks the run: {}",
            out.text
        );
    }

    /// `--as` is resolved once, at the CLI edge. Pure over the environment lookup so the
    /// three cases are testable without mutating this process's environment (which is
    /// `unsafe` in edition 2024 and global to every parallel test).
    #[test]
    fn a_supplied_actor_wins_over_the_environment() {
        assert_eq!(actor_or("alice", Some("bob")), "alice");
        assert_eq!(actor_or("  alice  ", Some("bob")), "alice", "trimmed");
    }

    #[test]
    fn an_omitted_actor_falls_back_to_the_unix_user() {
        assert_eq!(actor_or("", Some("bob")), "bob");
        assert_eq!(actor_or("   ", Some("bob")), "bob");
    }

    /// Never an empty actor: `GateDecided.actor` is what an audit reads, and a blank one
    /// is indistinguishable from a bug. A named non-answer is the honest record.
    #[test]
    fn an_actor_with_nothing_to_fall_back_on_is_named_unknown() {
        assert_eq!(actor_or("", None), "unknown");
        assert_eq!(actor_or("", Some("  ")), "unknown");
    }
}
