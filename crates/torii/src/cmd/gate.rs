//! `torii run gate` — the typed operator surface for the two MENU-bearing waiting kinds: a
//! `HumanGate` (SP-6 s2) and a `Loop`'s human gate (SP-6 s4).
//!
//! [`crate::cmd::run::signal`] delivers arbitrary JSON to an `AwaitSignal`. This delivers
//! a NAMED CHOICE to a gate that published a menu, so the answer is validated against what
//! the human was actually shown instead of being a value the graph author must remember to
//! test for. Every command refuses the other kinds and names the verb that would work —
//! see [`decide`]'s `AwaitSignal` and `Agent` arms, `signal`'s two gate arms and
//! `cmd::human::answer`'s.
//!
//! **One verb, two kinds, two journal events.** The operator's vocabulary stays three
//! commands — both gates are `run gate decide` — but the events they write are NOT
//! interchangeable, because each executor arm reads only its own and the two menus are
//! different vocabularies: a `HumanGate`'s option carries a `GateOutcome`, a loop gate's
//! carries `stops`. [`PublishedMenu`] is what keeps those two facts from being one fact,
//! and everything up to the append itself is factored over the option NAMES, which both
//! kinds have. Appending the wrong event would record a decision the executor cannot
//! interpret: durable, unread, and reported here as delivered.
//!
//! The loop gate is also the reason the "menu comes from the JOURNAL" rule is not merely
//! prudent but structural. Its node id is SYNTHESIZED per iteration
//! (`"{loop}/{i}/__gate__"`) and exists in no graph, so there is no graph copy of the menu
//! to prefer even if one wanted it.
//!
//! Everything that was hard-won in `signal` is reproduced here rather than re-derived:
//! append THEN `force_wake`; a post-append fault reported as a durable-but-unqueued answer
//! instead of a bare store error; the effect read back rather than assumed — from the
//! JOURNAL as well as the scheduler row, because only the journal is node-keyed and only
//! its ORDER says whether a racing drive READ the answer or orphaned it; and no refusal
//! that advises waiting for a pause a terminal run will never reach. Each of those four is
//! a defect `signal` shipped and fixed, and the first cut of this command re-derived three
//! of them wrongly anyway. Read that function before changing this one.

use crate::cmd::Outcome;
use crate::cmd::run::{SignalState, SignalStateAt, not_delivered, signal_state, signal_state_at};
use crate::errors::CliError;
use crate::render;
use chrono::{DateTime, Utc};
use orchestrator_core::{
    ExecutionJournal, GateOption, GateOutcome, JournalEvent, LoopGateOption, NodeId,
    OrchestratorError, RunId, RunStatus, SchedulerStore, Seq,
};

/// The three verbs of `run gate`. `approve` and `reject` are sugar for `decide --option
/// approve|reject`, and they work only when the gate actually declares options by those
/// names — when it does not, the refusal names the REAL menu, read from the journaled
/// `GateAwaited`, so the operator is told what the human was offered rather than what
/// this CLI assumed.
///
/// **Lives in the library, not in `main.rs`, so [`decision_of`] is testable.** The binary
/// has no test module, and while this enum sat there the verb→option mapping was asserted
/// by nothing at any layer — see `each_verb_maps_to_the_option_that_names_it` for what
/// that let through. Same reason [`actor_or_user`] lives here.
#[derive(clap::Subcommand)]
pub enum GateAction {
    /// Pick the `approve` option
    Approve {
        run_id: String,
        /// The waiting node's id — `torii run list-paused` names it.
        #[arg(long)]
        node: String,
        // `hide_default_value`: the clap default is the empty string, but the
        // EFFECTIVE default is $USER (`cmd::gate::actor_or_user` resolves it), so
        // rendering `[default: ""]` next to a sentence that says "defaulting to
        // $USER" contradicts it on the one surface the trust boundary is stated.
        #[arg(long, default_value = "", hide_default_value = true, help = ACTOR_HELP)]
        r#as: String,
        /// Free text recorded alongside the decision. It becomes part of this node's
        /// output, so it flows into downstream nodes and model prompts. Max 4096 bytes
        /// as stored — redaction replaces secret-shaped text with the longer literal
        /// `[REDACTED]`, so a note can cross the limit on the way to the journal.
        #[arg(long)]
        note: Option<String>,
    },
    /// Pick the `reject` option — `--reason` is required
    Reject {
        run_id: String,
        /// The waiting node's id — `torii run list-paused` names it.
        #[arg(long)]
        node: String,
        // `hide_default_value`: the clap default is the empty string, but the
        // EFFECTIVE default is $USER (`cmd::gate::actor_or_user` resolves it), so
        // rendering `[default: ""]` next to a sentence that says "defaulting to
        // $USER" contradicts it on the one surface the trust boundary is stated.
        #[arg(long, default_value = "", hide_default_value = true, help = ACTOR_HELP)]
        r#as: String,
        /// Why. Required: failing a run without recording why is a bare `catch {}`.
        /// Recorded as the decision's note, and bounded the same way: max 4096 bytes as
        /// stored.
        #[arg(long)]
        reason: String,
    },
    /// Pick a named option — the general form
    Decide {
        run_id: String,
        /// The waiting node's id — `torii run list-paused` names it.
        #[arg(long)]
        node: String,
        /// One of the options the gate published. An undeclared name is refused before
        /// anything is written.
        #[arg(long)]
        option: String,
        // `hide_default_value`: the clap default is the empty string, but the
        // EFFECTIVE default is $USER (`cmd::gate::actor_or_user` resolves it), so
        // rendering `[default: ""]` next to a sentence that says "defaulting to
        // $USER" contradicts it on the one surface the trust boundary is stated.
        #[arg(long, default_value = "", hide_default_value = true, help = ACTOR_HELP)]
        r#as: String,
        /// Free text recorded alongside the decision. It becomes part of this node's
        /// output, so it flows into downstream nodes and model prompts. Max 4096 bytes
        /// as stored — redaction replaces secret-shaped text with the longer literal
        /// `[REDACTED]`, so a note can cross the limit on the way to the journal.
        /// A LOOP gate records no note and REFUSES this flag rather than dropping it.
        #[arg(long)]
        note: Option<String>,
    },
}

/// One text for all three `--as` flags. A doc comment cannot be shared between fields, and
/// the trust boundary has to be on the surface an operator reads when they type the flag —
/// three hand-copied paragraphs would be three chances for one of them to lose the second
/// sentence, which is the sentence that matters.
const ACTOR_HELP: &str = "Who decided. ATTRIBUTION, NOT AUTHENTICATION: it is whatever \
                          string you supply (defaulting to $USER), so it records who \
                          CLAIMED to decide. Anyone who can reach the database can write \
                          any actor.";

/// The one shape all three [`GateAction`] verbs reduce to — [`decide`]'s arguments, still
/// unparsed and unresolved.
///
/// NAMED fields, not a tuple: every one is a `String`, so a shuffled destructuring would
/// compile silently. That is the second thing normalising three verbs into one call site
/// exists to prevent, and the first (`approve` delivering `reject`) is the defect that put
/// this type here.
pub struct Decision {
    pub run_id: String,
    pub node: String,
    /// The option name that will be matched against the journaled `GateAwaited` menu.
    /// `approve`/`reject` are the two verbs' literals; `decide` passes the operator's own
    /// through untouched.
    pub option: String,
    /// Still RAW — [`actor_or_user`] resolves the `$USER` fallback at the call site.
    pub actor: String,
    pub note: Option<String>,
}

/// Normalise a verb into the one shape [`decide`] takes.
///
/// The three verbs differ only in how the option and the note are SOURCED, so they are
/// reduced here and `dispatch` has exactly ONE call to [`decide`] — a second dispatch arm
/// per verb would be three places for the argument order to be got wrong.
///
/// Pure, and in the library, precisely so the mapping can be asserted: while this lived in
/// the binary's `dispatch`, swapping `"approve"` and `"reject"` left every test in this
/// crate green.
pub fn decision_of(action: GateAction) -> Decision {
    match action {
        GateAction::Approve {
            run_id,
            node,
            r#as,
            note,
        } => Decision {
            run_id,
            node,
            option: "approve".to_string(),
            actor: r#as,
            note,
        },
        // `--reason` is clap-required, so the note is always `Some` — but it may still be
        // blank, and `decide` re-checks the trimmed value. The required flag costs no
        // connection; the trim is what actually holds.
        GateAction::Reject {
            run_id,
            node,
            r#as,
            reason,
        } => Decision {
            run_id,
            node,
            option: "reject".to_string(),
            actor: r#as,
            note: Some(reason),
        },
        GateAction::Decide {
            run_id,
            node,
            option,
            r#as,
            note,
        } => Decision {
            run_id,
            node,
            option,
            actor: r#as,
            note,
        },
    }
}

/// Deliver a typed decision to a gate of either kind — a `HumanGate` (SP-6 s2) or a
/// `Loop`'s human gate (SP-6 s4).
///
/// **The menu comes from the JOURNAL, not the graph.** `GateAwaited`/`LoopGateAwaited`
/// record what the human was actually shown; validating against a graph that may since
/// have been edited would defeat the durability §4 requires — and nothing binds the graph
/// in hand to the one the human saw (there is no graph fence, and `Executor::start` takes
/// the graph as a caller parameter). This is the same rule the executor's
/// `run_human_gate`/`run_human_loop_gate` follow on their side, deliberately: two
/// enforcements of one rule, which must not drift.
///
/// For a loop gate that rule is not a preference but the only thing available: its node id
/// is synthesized per iteration and exists in NO graph, so the journal is the only record
/// that anything is waiting there or of what it may be answered with. Reading the menu
/// from the journal is why this command works on that kind at all — it needed a
/// [`PublishedMenu`] variant, not a new command.
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
/// **The NODE's state is checked, not just the RUN's** — see the `signal_state` arm below.
/// This was once left to the run-status check, on the reasoning that "a run whose gate
/// failed is itself terminal". That reasoning is FALSE, and the review that caught it
/// reproduced the consequence against the real `Executor`: `Scheduler::record` matches
/// `Ok(o) if o.paused.is_some()` BEFORE `Ok(o) if o.failed.is_some()`, so a drive that
/// fails one node while another is still waiting records the run **paused**. A gate whose
/// deadline had already fired therefore passed every guard here — menu present, option
/// declared, run paused — and this command journaled the decision, force-woke the run and
/// reported `decided: …` with exit 0, while the next drive's `gate_precheck` returned the
/// folded `NodeFailed` before the decision was ever read. The operator was told their
/// approval landed; it provably had not.
///
/// **And the gate's own DEADLINE is checked against `now`.** `run_human_gate` takes
/// `WaitState::Expired` before it reads any decision, so a decision appended after the
/// recorded instant can never approve the gate — it only makes the next tick terminally
/// fail the run, having reported success here. This is not an unavoidable race: the
/// deadline is durable on `GateAwaited` and `now` is a parameter, so the answer is
/// deterministic at this layer. It is still not ATOMIC (the executor re-checks on its own
/// clock at fold time), so it NARROWS the window rather than closing it — exactly what
/// `run_human_gate`'s `Expired` arm says this command does.
///
/// **What it deliberately does NOT check: whether a SECOND decision is redundant.** A gate
/// that already COMPLETED is caught by the `signal_state` arm; but two decisions delivered
/// while the node is still awaiting both land, and the later one is a last-wins value the
/// fold reads. That residue is identical to `signal`'s — see its `not read` arm.
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
        } else if crate::cmd::human::agent_question(&events, &node).is_some() {
            // SP-6 s3 AC7, the third kind. A human-backed `Agent` publishes a QUESTION and
            // no menu, so an option name means nothing to it: `run_human_agent` reads only
            // `AgentAnswered`. Without this arm the refusal below would be technically true
            // ("not awaiting a decision") and useless — it sends an operator to check a node
            // id that was right when the COMMAND was wrong.
            format!(
                "not delivered: {shown} is a human-backed Agent, not a HumanGate — it \
                 answers with free text, not one of a published set of options. Use: torii \
                 run agent answer {} --node {shown} --text '<answer>'",
                run.0
            )
        } else {
            format!(
                "not delivered: {shown} is not awaiting a decision. \
                 `torii run list-paused` names the nodes that are."
            )
        }));
    };

    // The NODE's own state, folded from the journal, and checked BEFORE the menu is
    // validated: a dead gate's options are not a useful thing to recite back. The fold is
    // `cmd::run`'s, not a second one — `signal_state` already classifies a `HumanGate`
    // correctly (its `GateAwaited` arm, added by this slice, is what puts a gate in the
    // awaited set at all), and the refusal text is `signal`'s so the two commands cannot
    // drift on a condition they share exactly.
    //
    // `NotAwaiting` is unreachable from here — a journaled `GateAwaited` is what produced
    // the menu above, and that is precisely what puts the node in the fold — but it is
    // matched by the same catch-all rather than special-cased, because it must never be
    // able to report success.
    match signal_state(&events, &node) {
        // The SLA has run out. `>=`, and `now` against the JOURNALED instant, because that
        // is exactly `wait_or_expire`'s test (`Some(d) if self.clock.now() >= d` ⇒
        // `WaitState::Expired`) — a boundary invented here instead of copied would accept
        // decisions the executor is about to reject, which is the failure this arm exists
        // to stop. Refusing is the whole gain: the alternative is a journaled decision, an
        // exit 0 reading `decided:`, and a next tick that terminally fails the run.
        //
        // Deterministic, not a race: the deadline is durable and `now` is a parameter.
        // Still not ATOMIC — the executor re-checks on its own clock at fold time, and a
        // decision that leaves here validly can arrive after that clock has passed the
        // instant — so this NARROWS the window and the executor stays the authority.
        SignalState::Awaiting { deadline: Some(d) } if now >= d => {
            return Ok(Outcome::precondition(format!(
                "not delivered: {shown}'s deadline passed at {d} — an expired gate is \
                 failed before any decision is read, so this would terminally fail the \
                 run rather than approve it. Start a new run."
            )));
        }
        SignalState::Awaiting { .. } => {}
        other => return Ok(Outcome::precondition(not_delivered(&shown, &other))),
    }

    // Resolved over the option NAMES, which is the one thing the two menu kinds share —
    // and the only thing an operator types. The per-kind half is what comes BACK: a
    // `HumanGate`'s option carries a `GateOutcome`, a loop gate's does not, and that
    // `Option<GateOutcome>` is what the required-reason rule below branches on. Doing the
    // `find` once, inside the kind match, is what keeps a second lookup (and a second
    // chance to disagree with this one) from existing.
    let chosen: Option<(&str, Option<GateOutcome>)> = match &menu {
        PublishedMenu::Human(opts) => opts
            .iter()
            .find(|o| o.name == option)
            .map(|o| (o.name.as_str(), Some(o.outcome))),
        PublishedMenu::Loop(opts) => opts
            .iter()
            .find(|o| o.name == option)
            .map(|o| (o.name.as_str(), None)),
    };
    let Some((chosen_name, outcome)) = chosen else {
        // The recited menu gets the SAME collapse and cap `render::awaiting_section`
        // gives these very values, and for the same reasons: an option name is AUTHOR
        // free text (a `run submit` file, a `scheduled_runs.graph` row, or a runtime
        // `Expand` subgraph from a planner model), and `validate_dag` checks a gate's
        // options only for non-emptiness, uniqueness and a reachable outcome — never for
        // content or length. So a raw newline here would forge a line that reads as its
        // own awaiting row, an ESC could rewrite what is already on screen, and one
        // 5,000-character name would flood the refusal an operator has to read.
        //
        // For a LOOP gate the recital matters more than for a `HumanGate`, not less: the
        // node exists in no graph, so this refusal is the ONLY place an operator can
        // discover the vocabulary other than `run list-paused`. And the executor's own
        // arm fails a loop gate LOUDLY on an unmatched option — terminally, for the whole
        // `Loop` — so this pre-check is what stands between a typo and a dead run.
        //
        // `{option:?}` is left as Debug on purpose: it is the operator's OWN input and
        // Debug already escapes it. The menu is Display and was not guarded at all.
        let menu_shown = render::cap_chars(
            &menu
                .option_names()
                .iter()
                .map(|n| render::one_line(n))
                .collect::<Vec<_>>()
                .join(", "),
            render::MENU_MAX,
        );
        return Ok(Outcome::precondition(format!(
            "not delivered: gate {shown} has no option {option:?}. Its options are: \
             {menu_shown}. Use: torii run gate decide {} --node {shown} --option <name>",
            run.0
        )));
    };

    // The option is echoed back on every SUCCESS line below, and by this point it is a
    // journaled option name (it was matched against the menu), i.e. the same author free
    // text the refusal above collapses — so it gets the same collapse AND the same cap.
    // Display-only: the value journaled on the decision's `option` is the one the operator
    // supplied, because that is what the executor re-matches against the menu.
    //
    // `MENU_MAX`, never a second number: it is `pub(crate)` precisely so this crate has
    // one bound for one class of value rather than two that can drift. Collapsing without
    // capping left the refusal path at 324 chars with a visible ellipsis and the SUCCESS
    // path — the one an operator always reaches — at 5065 with none, for the same
    // journaled name.
    let chosen_shown = render::cap_chars(&render::one_line(chosen_name), render::MENU_MAX);

    // A Fail option must record WHY. CLI-layer only, deliberately: `GateDecided.note`
    // stays `Option` because a `Complete` decision legitimately has none, and an absent
    // reason is a DOCUMENTATION failure, not a safety one — nothing downstream is unsafe
    // without it, so the executor must not refuse a decision over it. Enforcing it here
    // is what makes "why did this run stop" answerable from the journal alone.
    //
    // Trimmed, not merely present: clap's `required` cannot see that `--reason ''` is the
    // same omission with quotes around it, so this is the check that actually holds.
    if outcome == Some(GateOutcome::Fail) && note.map(str::trim).unwrap_or("").is_empty() {
        return Ok(Outcome::precondition(format!(
            "not delivered: {option:?} stops the run, so it needs a reason. \
             Use: torii run gate reject {} --node {shown} --reason '<why>'",
            run.0
        )));
    }

    // The mirror image on the other kind: a loop gate's row has NO note field
    // (`LoopGateDecided` is `{node, option, actor}`), so a `--note` here can only be
    // DROPPED — and dropping it silently is worse than refusing. An operator who typed an
    // explanation has every reason to believe it was recorded, and this is the one place
    // that belief can still be corrected; afterwards the only evidence is a journal row
    // they would have to read to discover the absence.
    //
    // Not fixed by widening the event either: `run_human_loop_gate` reads only `option`,
    // so a note would be a durable field nothing renders, and adding it is a journal-shape
    // change this task has no mandate for. Refusing costs the operator one retype.
    if matches!(menu, PublishedMenu::Loop(_)) && note.is_some() {
        return Ok(Outcome::precondition(format!(
            "not delivered: {shown} is a loop gate, and a loop gate's decision records no \
             note — there is nowhere durable to put --note, so it would be silently \
             dropped. Re-run without it: torii run gate decide {} --node {shown} --option \
             {option:?}",
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

    // §6.5, the same cap `cmd::run::signal` enforces on the same durable column, through
    // the same helper — never a second bound that could drift from it. `GateDecided.note`
    // was appended with no length check at all, and `ARG_MAX` permits a ~120 KB `--note`:
    // 30x this limit, journaled durably, reloaded on every drive, folded into the gate's
    // output on a `Complete` option and carried in every downstream Agent's prompt for the
    // life of the run.
    //
    // Measured AFTER the redaction and checked BEFORE the append, so the bytes bounded are
    // exactly the bytes written. Checking the raw note would bound a value nobody stores:
    // `[REDACTED]` is longer than the shortest span it replaces, so redaction can GROW the
    // note past the cap — the defect `signal` shipped once and fixed, recorded on
    // `Measured`. There is no as-given pre-check here (`signal` has one) because a note is
    // a `String`, not arbitrary JSON: the redactor is ReDoS-safe by construction and the
    // only thing left to bound is the row.
    //
    // A hard error (exit 1), matching `signal` exactly: exit 2 in this taxonomy means "ran
    // fine, nothing to do", and an over-limit note is invalid INPUT. Two writers to one
    // column must not disagree about the exit code for one violation.
    if let Some(n) = &note {
        crate::cmd::run::check_payload_size(
            &serde_json::json!(n),
            crate::cmd::run::Measured::AfterRedaction,
            "the decision note",
        )
        .map_err(CliError::error)?;
    }

    // Resolved HERE as well as at `dispatch`, and that is deliberate rather than
    // redundant. Both events' `actor` is a required `String`, so "nobody said who" has no
    // legible encoding: a caller that skipped the resolver journals clap's empty default
    // as a silent `""`, indistinguishable at a glance from a real name in the row an
    // audit reads forever. Applying it at the one place BOTH append sites pass through
    // makes that unreachable from the library entry point too — `decide` is `pub`, and
    // `main.rs` is not the only caller (the e2e drives it directly).
    //
    // Idempotent, so the dispatch call is not defeated and an ordinary `--as alice` is
    // untouched: [`actor_or`] returns a non-blank supplied value verbatim, and only an
    // empty one falls through to `$USER` and then to `unknown`.
    let actor = actor_or_user(actor);

    // Collapsed on the way IN, not just on the way out — unlike the node id, which is
    // journaled as given. `actor` is interpolated by the executor into a `NodeFailed`
    // message that `torii run status` renders and that a later drive re-emits from the
    // fold, so an escape sequence smuggled through `--as` would be replayed at every
    // operator who reads the run.
    let actor = render::one_line(&actor);

    // **On a LOOP gate the actor is REDACTED, and on a `HumanGate` it is not.** The
    // asymmetry looks like an inconsistency and is the opposite of one — it is the reason
    // this branch could not simply inherit the path beside it, and inheriting it silently
    // is how the s3 `--as` leak happened on `AgentAnswered.actor`.
    //
    // `GateDecided.actor` has a second line of defence: `run_human_gate` interpolates it
    // into the rejection `NodeFailed`, so it passes the executor's own redacting chokepoint
    // on the way back out. `LoopGateDecided.actor` has NONE — `run_human_loop_gate` reads
    // only `option`, puts the actor in no message and no node output — so whatever is
    // appended here is what an audit reads forever, and the journal row is the one place a
    // credential-shaped `--as "$CI_TOKEN_OWNER"` would land in the clear. The variant's own
    // doc states the obligation as the appending writer's alone; this is the writer.
    //
    // **Redact, THEN size-check** — the order `cmd::human::answer` had to be fixed to, for
    // the reason `Measured` records: `[REDACTED]` is longer than the shortest span it
    // replaces, so a value that fitted as typed can exceed the cap once scrubbed, and a
    // check placed first bounds a value nobody stores. `Measured::AfterRedaction` travels
    // with it so the growth explanation names a transform this value really went through —
    // which is exactly why the `HumanGate` arm keeps `AsGiven`.
    //
    // The non-string arm is fail-CLOSED, like the note's above and for the same reason.
    let (actor, measured) = match menu {
        PublishedMenu::Human(_) => (actor, crate::cmd::run::Measured::AsGiven),
        PublishedMenu::Loop(_) => (
            render::redact_payload(&serde_json::json!(actor))
                .as_str()
                .unwrap_or("[REDACTED]")
                .to_string(),
            crate::cmd::run::Measured::AfterRedaction,
        ),
    };

    // The SIBLING field on the same durable row, held to the same bound. `--as` was
    // capped by nothing while `--note` was capped at 4096, so `ARG_MAX` permitted a
    // ~131 KB actor — 32x the limit — accepted with exit 0 and journaled into the very
    // row the note is bounded in. `GateDecided.actor` is not merely displayed either: the
    // executor interpolates it into the rejection `NodeFailed` above, so an unbounded one
    // is reloaded on every drive and re-rendered by every `torii run status`.
    //
    // Measured on the value about to be WRITTEN — collapsed, and scrubbed on the kind that
    // scrubs — for the same reason the note is measured after redaction: these are the
    // bytes actually written.
    crate::cmd::run::check_payload_size(
        &serde_json::json!(actor),
        measured,
        "the decision actor (--as)",
    )
    .map_err(CliError::error)?;

    // **The one place the two kinds stop sharing.** Everything above is factored over the
    // option NAMES precisely so this stays a single `match` on a single line of divergence:
    // the two events are not interchangeable, because each executor arm reads only its own
    // and each menu is a different vocabulary, so appending the wrong one records a
    // decision that is durable, unreadable and reported as delivered.
    //
    // `note` is `None` on the loop arm by construction — the refusal above rejects a
    // `--note` on this kind rather than dropping it — so the field's absence here loses
    // nothing an operator supplied.
    let event = match menu {
        PublishedMenu::Human(_) => JournalEvent::GateDecided {
            node: node.clone(),
            option: option.to_string(),
            actor,
            note,
        },
        PublishedMenu::Loop(_) => JournalEvent::LoopGateDecided {
            node: node.clone(),
            option: option.to_string(),
            actor,
        },
    };

    // The appended seq is KEPT, not discarded: it is what names the durable row in the
    // post-append fault report below, so an operator can find the write that succeeded.
    let appended = journal
        .append(run, event)
        .await
        .map_err(OrchestratorError::Journal)?;

    // Past here the decision is DURABLE. Every remaining call reports rather than `?`s —
    // a bare store error reads as "it did not go through" for a write that succeeded, and
    // for an indefinite gate (`next_wake` NULL, never auto-woken) the run would then wait
    // forever on a decision nobody knows landed. Identical in shape and in reason to
    // `cmd::run::signal`'s `unread` closure.
    //
    // The error goes through `render::safe_reason` — redact, collapse control characters,
    // cap — because a backend fault is FREE TEXT FROM THE DRIVER, not a message this
    // crate wrote. `PostgresJournal::load` builds it from both `sqlx::Error` (a pool
    // timeout carries the whole connection string, password included) and
    // `serde_json::Error` (over a TYPED `JournalEvent`, which quotes the offending row —
    // the `invalid type: string "sk-live-…"` shape `parse_payload` documents). Interpolated
    // raw it also let a newline forge a line beginning with a pastable uuid and an ANSI
    // escape rewrite what was already on screen. This is the same transform, on the same
    // error class, that `render::awaiting_section` already applies to a per-run journal
    // fault in `list-paused` — one guard, every sink.
    let unread = |what: &str, e: &dyn std::fmt::Display| {
        let e = render::safe_reason(&e.to_string());
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

    // The JOURNAL is re-read too, not just the scheduler row — and that is the whole
    // point. The scheduler row is RUN-level: it says the run is no longer paused, never
    // whether THIS node read the decision. Reporting off the row alone inverted this
    // command on its most successful path, exactly as it once did `signal`'s: a worker
    // that claims the run the instant the decision lands folds our `GateDecided`,
    // completes the gate and drives the run to completion — the decision worked
    // perfectly — and the report said `not queued`, exit 2, advising `torii run wake`,
    // which refuses every non-paused run and which no shipped store can ever satisfy.
    let after_events = match journal.load(run).await {
        Ok(evs) => evs,
        Err(e) => return Ok(unread("the journal re-read", &e)),
    };
    // Terminal by now, or not. If it is, "not delivered" is FALSE — the row is already
    // durable — and the honest question is whether anything READ it. The journal ORDER
    // answers it, which is why the append's seq was kept: a terminal marker BEHIND our row
    // means the drive that terminated the node folded a journal that already contained the
    // decision; a marker AHEAD of it means the node was already dead when the row landed.
    let SignalStateAt { state, at } = signal_state_at(&after_events, &node);

    // **What the durable orphan below will DO — and the two kinds do opposite things.**
    // Split because the `HumanGate` sentence was inherited whole by the loop branch and is
    // false there, which is the defect class this slice keeps shipping: a message that
    // asserts something untrue is worse than one that says less.
    //
    // A `HumanGate` re-executes. It journals no `NodeCompleted`, `NodeFailed` is not folded
    // as a barrier, and `run_human_gate` re-reads `GateDecided` on every drive — so a
    // re-`start` really does fold this late row as the node's answer, and an operator has a
    // reason to care about it.
    //
    // A loop gate does NOT. `run_human_loop_gate` reads a terminal verdict BACK rather than
    // re-deriving one: step 0 returns a folded `NodeFailed` forever, and step 1 replays
    // `LoopGateSettled` without ever consulting `LoopGateDecided` again (that ordering is
    // deliberate and separately tested there). Whichever marker beat this write therefore
    // wins permanently and the row is inert. Telling an operator it will be folded sends
    // them to clean up a decision nothing will ever read.
    let orphan_residue = match menu {
        PublishedMenu::Human(_) => {
            "The decision stays on the journal as a last-wins value that a re-`start` of \
             this run would fold as the node's answer"
        }
        PublishedMenu::Loop(_) => {
            "The decision stays on the journal as inert residue: `run_human_loop_gate` \
             replays whichever terminal verdict beat it — a folded `NodeFailed`, or the \
             `LoopGateSettled` of the option that was honoured — instead of re-reading a \
             decision, so nothing will ever fold this row"
        }
    };

    if let Some(at) = at {
        return Ok(match (&state, at > appended) {
            // Terminated AFTER our row, by completing: the decision was on the journal for
            // the fold that completed it. A SUCCESS — the only difference from the
            // ordinary path is that the run is already moving, so there is no tick to wait
            // for and nothing for the operator to do.
            //
            // The wording claims the ORDERING (proven) and not authorship of the
            // completion (not proven): a duplicate decision already on the journal is
            // last-wins, so which one the drive folded is not observable from here.
            (SignalState::Completed, true) => Outcome::ok(format!(
                "decided: {shown} = {chosen_shown} (a drive already in flight completed the \
                 node after the decision landed, so the run is moving without waiting for a \
                 tick)"
            )),
            // Terminated AFTER our row by a REJECTION — a decision that WAS read, and
            // honoured. A `GateOutcome::Fail` option's whole purpose is to fail the node,
            // so this `NodeFailed` is the decision working, not the decision being missed.
            //
            // This arm exists because the generic one below was imported from
            // `cmd::run::signal`, where its premise HOLDS: `run_await_signal` completes on
            // any folded payload and never fails a node because of one, so for an
            // `AwaitSignal` a terminal-Failed ahead of the delivery really does mean
            // nothing read it. `HumanGate` breaks that premise, and without this arm
            // `torii run gate reject` reported exit 2 — "it terminated while this decision
            // was in flight, and a drive that had already loaded the journal would not
            // have seen it" — on a rejection that had done precisely what it was asked to.
            //
            // `chosen.outcome == Fail` is NOT the discriminator, and that is the whole
            // subtlety: `wait_or_expire` takes `Expired` BEFORE any decision is read, so a
            // deadline firing in the same window journals its own `NodeFailed` at the same
            // place, for the same `Fail` option, and must keep the "not read" text. What
            // separates them is the journaled MESSAGE — `fail_gate`'s rejection form
            // against its expiry form — so that is what is matched, at the exact seq the
            // fold said established the state.
            //
            // The wording claims the ORDERING (proven) and not authorship (not proven),
            // exactly as the `Completed` arm above: a duplicate decision already on the
            // journal is last-wins, so which one the drive folded is not observable here.
            (SignalState::Failed, true) if rejected_at(&after_events, at, &node) => {
                Outcome::ok(format!(
                    "decided: {shown} = {chosen_shown} (a drive already in flight read a \
                     decision after this one landed and rejected the node — stopping the \
                     run is what a Fail option does, so there is nothing further to do)"
                ))
            }
            // Terminated AFTER our row, but NOT by completing and NOT by an honoured
            // rejection — an expired deadline, or a cascade skip. That drive had loaded
            // the journal before our row landed, so it never saw the decision. Reporting
            // `decided` here would hide a failed gate behind a success.
            (other, true) => Outcome::precondition(format!(
                "not read: {shown}'s decision is journaled durably, but {shown} is {} — it \
                 terminated while this decision was in flight, and a drive that had \
                 already loaded the journal would not have seen it.",
                other.as_str()
            )),
            // Terminated BEFORE our row landed: a true orphan. The pre-check refuses this
            // shape, so reaching it means the node died inside the write window — worth
            // saying plainly, because the residue is durable and what it will DO differs by
            // kind. See `orphan_residue`: this sentence was inherited whole from the s2
            // path and is FALSE for a loop gate.
            (other, false) => Outcome::precondition(format!(
                "not read: {shown}'s decision is journaled durably, but {shown} was already \
                 {} before the write landed, so nothing read it. {orphan_residue} — do not \
                 treat this gate as decided.",
                other.as_str()
            )),
        });
    }

    // `at: None` ⇒ nothing has terminated the node, so the decision is still there to be
    // read and the only remaining question is the WAKE. (`NotAwaiting` also folds to
    // `None`, but is unreachable: the pre-check read a `GateAwaited` and the journal is
    // append-only, so this later read is a superset of that one.)

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
            "decided: {shown} = {chosen_shown} (the run will resume on the next worker tick)"
        ))
    } else if after.status.is_terminal() {
        // The run is over while the node itself never terminated — `cancel`/
        // `record_terminal` journal no node event, so this is reachable. "Retry once it is
        // paused again" would be advice to wait forever, the same dead end the pre-check
        // arm above was already fixed for; the post-append arm must not reintroduce it.
        Outcome::precondition(format!(
            "not read: {shown}'s decision is journaled durably, but the run is {} — a {} \
             run is never paused again, so nothing will ever read it. Start a new run.",
            after.status.as_str(),
            after.status.as_str()
        ))
    } else {
        Outcome::precondition(format!(
            "not queued: {shown}'s decision is journaled durably, but the run is {} and \
             the wake did not apply — the drive that claimed it may have folded the \
             journal before the decision landed. Run `torii run wake {}` once it is paused \
             again.",
            after.status.as_str(),
            run.0
        ))
    })
}

/// Did the `NodeFailed` that terminated this node at seq `at` come from a REJECTION the
/// executor read and honoured, rather than from the deadline firing?
///
/// The two are indistinguishable by state — both are `SignalState::Failed` on the same
/// node, and a `Fail` option is involved either way — and they are opposite outcomes for
/// the operator: one says the rejection landed, the other says nothing ever looked at it.
/// Only the journaled MESSAGE separates them, so only the message is consulted.
///
/// Matched against `Executor::fail_gate`'s rejection form,
/// `"human_gate: node {id} rejected by {actor} ({option}): {reason}"`
/// (`crates/orchestrator/src/executor/gate.rs`), anchored on the node id this command was
/// given rather than on the bare word "rejected" — the id is the one part of that prefix
/// the caller already knows, so a `NodeFailed` a DIFFERENT node's rejection wrote cannot
/// match, and neither can operator prose that happens to contain the word.
///
/// **Conservative in the safe direction.** Every failure text goes through the executor's
/// redactor, so a node id or actor of secret SHAPE would be rewritten and this returns
/// `false` — which falls through to the generic "not read" report. That is the fail-safe
/// answer: it under-claims (an operator is told to check a decision that in fact landed)
/// rather than over-claiming a rejection that never happened. Matching by seq as well as
/// by node is what keeps this reading the event the FOLD chose, not some other failure.
fn rejected_at(events: &[(Seq, JournalEvent)], at: Seq, node: &NodeId) -> bool {
    let prefix = format!("human_gate: node {} rejected by ", node.0);
    events.iter().any(|(seq, e)| {
        matches!(
            e,
            JournalEvent::NodeFailed { node: n, error }
                if *seq == at && n == node && error.starts_with(&prefix)
        )
    })
}

/// Which waiting kind published a menu at this node, and what it published.
///
/// **Two kinds carry menus and they are NOT interchangeable.** A `HumanGate`'s
/// [`GateOption`] carries a [`GateOutcome`] of `{Complete, Fail}`; a loop gate's
/// [`LoopGateOption`] carries `stops`, and "continue" has no representation in the former
/// at all. Each is answered by its OWN journal event — `GateDecided` and
/// `LoopGateDecided` — and each executor arm reads only its own: appending the wrong one
/// records a decision the executor structurally cannot interpret, which is journaled,
/// never read, and reported here as delivered.
///
/// So the KIND is returned rather than just the options. An `Option<Vec<GateOption>>`
/// could not carry it, and the alternative — a second `loop_gate_menu` lookup beside this
/// one — would leave every caller free to consult only the first and silently mis-handle
/// the other kind, which is exactly the state `cmd::run::signal` and `cmd::human::answer`
/// were in before this slice: both refused a loop gate as "not awaiting", true and
/// useless.
///
/// What the two DO share is the option NAMES, which is the whole of what an operator
/// types and the whole of what a refusal recites — hence [`PublishedMenu::option_names`],
/// over which [`decide`] factors everything up to the append itself.
pub(crate) enum PublishedMenu {
    Human(Vec<GateOption>),
    Loop(Vec<LoopGateOption>),
}

impl PublishedMenu {
    /// The option names, in published order — the shared vocabulary. Borrowed rather than
    /// cloned: every consumer either compares or renders.
    pub(crate) fn option_names(&self) -> Vec<&str> {
        match self {
            PublishedMenu::Human(o) => o.iter().map(|o| o.name.as_str()).collect(),
            PublishedMenu::Loop(o) => o.iter().map(|o| o.name.as_str()).collect(),
        }
    }
}

/// The menu a gate of EITHER kind published, folded from `GateAwaited` or
/// `LoopGateAwaited`. FIRST wins, matching the executor's fold — two copies of one rule,
/// so they must not drift.
///
/// `None` = this node never asked, which is what distinguishes a gate from the two
/// menu-less waiting kinds without loading the graph. `cmd::run::signal` and
/// `cmd::human::answer` read it for exactly that: a `Some` here means a raw payload or
/// free text must be refused, and the variant says which verb to name.
///
/// **Reading the journal is not a shortcut here, it is the only design available for a
/// LOOP gate.** A `HumanGate` is at least an authored node one could look up in a graph;
/// a loop gate's path is SYNTHESIZED per iteration (`"{loop}/{i}/__gate__"`) and exists in
/// no graph at all, so the journaled `LoopGateAwaited` is the only record of both the menu
/// and the fact that anything is waiting there. That is what makes `torii run gate decide`
/// work on it at all.
///
/// First-wins across BOTH variants, not within each: one node cannot legitimately publish
/// two kinds of ask (they are different node kinds, and each executor arm writes exactly
/// one event), so a journal that contains both is corrupt and the earlier row is the one
/// that describes what a human was actually shown.
pub(crate) fn gate_menu(events: &[(Seq, JournalEvent)], node: &NodeId) -> Option<PublishedMenu> {
    events.iter().find_map(|(_, e)| match e {
        JournalEvent::GateAwaited {
            node: n, options, ..
        } if n == node => Some(PublishedMenu::Human(options.clone())),
        JournalEvent::LoopGateAwaited { node: n, menu, .. } if n == node => {
            Some(PublishedMenu::Loop(menu.clone()))
        }
        _ => None,
    })
}

/// Whether this node is awaiting a RAW signal — used only to give the right cross-refusal.
///
/// `pub(crate)` since SP-6 s3 made the refusal matrix three-way — four-way as of s4, whose
/// loop gate is a fourth waiting KIND answered by an existing verb: `cmd::human::answer`
/// has to distinguish an `AwaitSignal` from the gates exactly as this module does, and a
/// second `SignalAwaited` scan would be a second place for that rule to drift.
pub(crate) fn awaiting_signal(events: &[(Seq, JournalEvent)], node: &NodeId) -> bool {
    events
        .iter()
        .any(|(_, e)| matches!(e, JournalEvent::SignalAwaited { node: n, .. } if n == node))
}

/// Resolve `--as` to the actor string journaled on `GateDecided`, `LoopGateDecided` or
/// `AgentAnswered`.
///
/// **ATTRIBUTION, NOT AUTHENTICATION.** It is whatever string the caller supplied, so it
/// answers "who claimed to decide", never "who decided" — anyone who can reach the
/// database can write any actor. The help says so in those words, guarded by a
/// binary-level test, because an operator reading a `--as` flag will otherwise reasonably
/// assume it is authenticated.
///
/// Lives in the library, not in `main.rs`, so the fallback chain is testable: the binary
/// is deliberately thin (clap plus `dispatch`) and has no test module.
///
/// **Called at BOTH edges since s4: `dispatch` resolves it, and [`decide`] resolves it
/// again** at the one point both of its append sites pass through. That is defence, not
/// duplication: every one of these `actor` fields is a required `String`, so a caller that
/// skipped this resolver would journal clap's empty default as a silent `""` — a blank
/// audit row, indistinguishable at a glance from a real name, on a row nothing downstream
/// rewrites. `decide` is `pub` and `main.rs` is not its only caller. Idempotent, so the
/// second application changes nothing an operator supplied: a non-blank value comes back
/// trimmed and otherwise verbatim.
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

/// `pub(crate)` **only so `cmd::run`'s tests can reuse two fixtures** — `release` and
/// `gate_journal`. The reuse runs in BOTH directions (this module already imports four
/// fixtures from `cmd::run::tests`), which is deliberate rather than accidental: a
/// fixture belongs beside the tests that own it, and `gate_journal` builds the
/// `GateAwaited` menu shape that `list-paused` and `gate decide` must agree about. Two
/// copies of it would be two places for that agreement to rot independently — the same
/// argument `cmd::run::tests` records for its own four.
#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use crate::cmd::run::tests::{
        FORGED_RUN, FailingForceWakeStore, awaiting_journal, now, paused_store,
    };
    use crate::errors::{EXIT_OK, EXIT_PRECONDITION};
    use orchestrator_core::{
        GateOption, GateOutcome, Graph, JournalEvent, NodeId, RunId, RunStatus, SchedulerStore,
    };
    use orchestrator_store::{InMemoryJournal, InMemorySchedulerStore};

    pub(crate) fn release() -> NodeId {
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
    pub(crate) async fn gate_journal(
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

    /// The RUN's status is not the NODE's, and conflating them let this command report a
    /// green `decided:` for a gate that can never read the decision. A `HumanGate` whose
    /// deadline fired is TERMINAL — `gate_precheck` reads its `NodeFailed` back on every
    /// later drive and returns `Failed` before any decision is looked at — while the run
    /// itself stays `paused` whenever some OTHER node is still waiting, because
    /// `Scheduler::record` matches `paused` BEFORE `failed`. So "the run is paused"
    /// proves nothing about this node, and the refusal has to come from the node's own
    /// fold.
    #[tokio::test]
    async fn a_decision_on_a_terminally_failed_gate_is_refused() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship", "reject"]).await;
        j.append(
            run,
            JournalEvent::NodeFailed {
                node: release(),
                error: "human_gate: node release passed its deadline 2026-08-26T00:00:00Z".into(),
            },
        )
        .await
        .unwrap();

        let out = decide(&s, &j, run, release(), "ship", "alice", None, now())
            .await
            .expect("no hard error");

        assert_eq!(
            out.code, EXIT_PRECONDITION,
            "the gate is dead — reporting exit 0 tells an operator their approval landed \
             when the next drive will never read it: {}",
            out.text
        );
        assert!(
            out.text.contains("never re-executes"),
            "must say WHY it can never be read, in `run signal`'s words: {}",
            out.text
        );
        assert!(
            journaled_decisions(&j, run, &release()).await.is_empty(),
            "nothing may be written for a node that is already terminal"
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            None,
            "and the run must not be woken for a decision nothing will read"
        );
    }

    /// A gate whose SLA has run out cannot be approved, and this command can say so
    /// DETERMINISTICALLY rather than leaving it to the next tick: the deadline is on the
    /// journal (`GateAwaited.deadline`) and `now` is a parameter, so nothing here is a
    /// race. Without the check the decision is journaled and reported exit 0, and the next
    /// drive takes `WaitState::Expired` and terminally fails the run — the same lie about a
    /// landed approval as the terminal-node case above, one tick earlier.
    #[tokio::test]
    async fn a_decision_after_the_gates_deadline_is_refused() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(
            run,
            &release(),
            Some(now() - chrono::Duration::hours(1)),
            &["ship"],
        )
        .await;

        let out = decide(&s, &j, run, release(), "ship", "alice", None, now())
            .await
            .expect("no hard error");

        assert_eq!(
            out.code, EXIT_PRECONDITION,
            "the deadline is an hour behind `now` — the executor will fail this gate \
             before it reads any decision: {}",
            out.text
        );
        assert!(
            out.text.contains("deadline"),
            "must name what ran out, so the operator does not simply retry: {}",
            out.text
        );
        assert!(
            journaled_decisions(&j, run, &release()).await.is_empty(),
            "an expired gate must leave NOTHING durable — a late decision on the journal \
             is what a re-`start` would fold as an approval"
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            None,
            "and must not queue a wake"
        );
    }

    /// The boundary itself, at the ONE instant that distinguishes `>=` from `>`. The two
    /// tests either side of this one pin it an hour out in each direction, which `now > d`
    /// satisfies just as well — so without this the boundary the refusal claims to copy
    /// from `wait_or_expire` (`Some(d) if self.clock.now() >= d` ⇒ `WaitState::Expired`)
    /// is asserted by nothing, and a CLI that accepted a decision exactly at the deadline
    /// would journal a row the executor is about to reject.
    #[tokio::test]
    async fn a_decision_exactly_at_the_gates_deadline_is_refused() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), Some(now()), &["ship"]).await;

        let out = decide(&s, &j, run, release(), "ship", "alice", None, now())
            .await
            .expect("no hard error");

        assert_eq!(
            out.code, EXIT_PRECONDITION,
            "`now == deadline` is EXPIRED for the executor, so it must be expired here: \
             {}",
            out.text
        );
        assert!(out.text.contains("deadline"), "{}", out.text);
        assert!(
            journaled_decisions(&j, run, &release()).await.is_empty(),
            "an expired gate must leave NOTHING durable"
        );
    }

    /// The boundary the refusal above sits on is the EXECUTOR's (`now >= deadline` ⇒
    /// `WaitState::Expired`), not one invented here, and a deadline still in the future is
    /// the other side of it. Without this, refusing every timed gate outright would leave
    /// the suite green while making the whole timed class undecidable.
    #[tokio::test]
    async fn a_decision_before_the_gates_deadline_is_delivered() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(
            run,
            &release(),
            Some(now() + chrono::Duration::seconds(1)),
            &["ship"],
        )
        .await;

        let out = decide(&s, &j, run, release(), "ship", "alice", None, now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_OK, "{}", out.text);
        assert_eq!(journaled_decisions(&j, run, &release()).await.len(), 1);
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

    /// A `SchedulerStore` that runs a concurrent worker against `run` at the top of
    /// `force_wake` — i.e. exactly in the window between `decide`'s append and the point
    /// where its effect becomes observable. `decide` appends BEFORE it calls `force_wake`,
    /// so this models a worker that drove the run AFTER the decision landed: the drive
    /// folds a journal that already contains our `GateDecided`, completes the gate
    /// (`ContextWrite` under the node id — a `HumanGate` journals no `NodeCompleted`) and
    /// finishes the run. The `force_wake` itself SUCCEEDS; it is simply a no-op, because
    /// the row is no longer `paused`. Same technique as `cmd::run`'s `SignalRacingStore`:
    /// single-threaded, deterministic, no database.
    struct GateRacingStore {
        inner: InMemorySchedulerStore,
        journal: std::sync::Arc<InMemoryJournal>,
        run: RunId,
        drive: GateRacingDrive,
    }

    /// What the concurrent worker does to the run inside the delivery window — see
    /// [`GateRacingStore`].
    #[derive(Clone, Copy, PartialEq)]
    enum GateRacingDrive {
        /// It folded the journal — which by then contains our decision — and completed the
        /// gate with it, finishing the run.
        CompletesTheNode,
        /// It folded the journal and FAILED the gate, journaling the carried `NodeFailed`
        /// message verbatim and filing the run `Failed`. The message is the discriminator
        /// the report depends on, so it is supplied by the test rather than invented here:
        /// `fail_gate`'s REJECTION form means the decision was read and honoured, its
        /// EXPIRY form means `wait_or_expire` took `Expired` before any decision was read.
        /// Both land on `(SignalState::Failed, at > appended)` and they are opposite
        /// outcomes.
        FailsTheNode(&'static str),
        /// An operator cancelled the run. `cancel` journals no NODE event, so the gate
        /// still folds as awaiting while the run is over: the decision is durable, the
        /// node never terminated, and nothing will ever read it.
        CancelsTheRun,
    }

    /// The EXACT `NodeFailed` text `Executor::run_human_gate` journals for a decision it
    /// read and honoured on a `Fail` option — `fail_gate`'s rejection form,
    /// `"human_gate: node {id} rejected by {actor} ({option}): {reason}"`
    /// (`crates/orchestrator/src/executor/gate.rs`). Copied verbatim rather than
    /// paraphrased: it is what `decide` matches on to tell a honoured rejection from an
    /// expiry, so a paraphrase would test a string this repo never writes.
    const EXECUTOR_REJECTION: &str =
        "human_gate: node release rejected by alice (reject): the canary suite is red";

    /// The EXACT `NodeFailed` text the same function journals when `wait_or_expire`
    /// returns `Expired` — which happens BEFORE any decision is read. Same node, same
    /// seq ordering, opposite meaning.
    const EXECUTOR_EXPIRY: &str = "human_gate: node release passed its deadline \
         1970-02-04T17:20:01Z; the gate fails on the deadline BEFORE any decision is \
         read, so a decision that had already landed does not approve it";

    #[async_trait::async_trait]
    impl SchedulerStore for GateRacingStore {
        async fn enqueue(
            &self,
            run: RunId,
            graph: &Graph,
            now: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), orchestrator_core::OrchestratorError> {
            self.inner.enqueue(run, graph, now).await
        }
        async fn record_paused(
            &self,
            run: RunId,
            next_wake: Option<chrono::DateTime<chrono::Utc>>,
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
            now: chrono::DateTime<chrono::Utc>,
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
            before: chrono::DateTime<chrono::Utc>,
        ) -> Result<u64, orchestrator_core::OrchestratorError> {
            self.inner.count_terminal_before(before).await
        }
        async fn prune_terminal(
            &self,
            before: chrono::DateTime<chrono::Utc>,
        ) -> Result<u64, orchestrator_core::OrchestratorError> {
            self.inner.prune_terminal(before).await
        }
        async fn force_wake(
            &self,
            run: RunId,
            now: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), orchestrator_core::OrchestratorError> {
            if run != self.run {
                return self.inner.force_wake(run, now).await;
            }
            match self.drive {
                GateRacingDrive::CompletesTheNode => {
                    // A worker's tick claims the run (`paused -> waking`), and its drive
                    // folds the journal — which by now contains our decision — completes
                    // the gate and finishes the run.
                    self.inner
                        .claim_due(now, chrono::Duration::seconds(60), 10)
                        .await?;
                    crate::cmd::run::tests::append_completion(&self.journal, run, &release()).await;
                    self.journal.append(run, JournalEvent::RunCompleted).await?;
                    self.inner
                        .record_terminal(run, RunStatus::Completed, None)
                        .await?;
                }
                // Same claim, same ordering — the drive folds a journal that already
                // holds our decision — but it FAILS the gate rather than completing it.
                // A `HumanGate` journals a real `NodeFailed` on every failure arm
                // (`fail_gate`), so unlike the completion above there is no inferred
                // marker involved.
                GateRacingDrive::FailsTheNode(error) => {
                    self.inner
                        .claim_due(now, chrono::Duration::seconds(60), 10)
                        .await?;
                    self.journal
                        .append(
                            run,
                            JournalEvent::NodeFailed {
                                node: release(),
                                error: error.to_string(),
                            },
                        )
                        .await?;
                    self.inner
                        .record_terminal(run, RunStatus::Failed, None)
                        .await?;
                }
                // No claim and no journal write at all: `cancel` is unconditional and
                // node-blind, which is exactly what leaves the gate folding as awaiting on
                // a run that is over.
                GateRacingDrive::CancelsTheRun => self.inner.cancel(run).await?,
            }
            // Our own force_wake: succeeds, but is a conditional no-op now that the row is
            // no longer `paused`.
            self.inner.force_wake(run, now).await
        }
    }

    /// THE check-then-act case on the post-append arm, and the one this command had
    /// INVERTED on its most successful path — the same defect `run signal` fixed, whose
    /// own comment calls it "the delivery worked perfectly, and the CLI said it had not
    /// happened".
    ///
    /// `decide`'s post-append arm re-read only the SCHEDULER row, never the journal. A
    /// worker that claims the run inside the delivery window folds our `GateDecided`,
    /// completes the gate and files the run `Completed` — so the re-read sees a non-paused
    /// row and the command reported `not queued … Run torii run wake <id> once it is
    /// paused again`: exit 2 on a decision that was delivered AND read, plus advice `wake`
    /// refuses for every non-paused run and that no shipped store can ever satisfy. This
    /// module already enforces the no-dead-end-advice rule on its PRE-check arm
    /// (`a_decision_on_a_terminal_run_does_not_advise_waiting_for_a_pause`) and violated it
    /// on the post-append arm of the same function.
    #[tokio::test]
    async fn a_decision_a_racing_worker_already_folded_is_not_reported_as_a_failure() {
        let run = RunId(uuid::Uuid::new_v4());
        // A TIMED gate already due: `next_wake <= now`, so a worker's `claim_due` really
        // can grab it in the delivery window. (Still in the future, so the deadline
        // pre-check does not refuse it first.)
        let inner = paused_store(run, Some(now())).await;
        let journal = std::sync::Arc::new(
            gate_journal(
                run,
                &release(),
                Some(now() + chrono::Duration::seconds(1)),
                &["ship"],
            )
            .await,
        );
        let racing = GateRacingStore {
            inner: inner.clone(),
            journal: journal.clone(),
            run,
            drive: GateRacingDrive::CompletesTheNode,
        };

        let out = decide(
            &racing,
            journal.as_ref(),
            run,
            release(),
            "ship",
            "alice",
            None,
            now(),
        )
        .await
        .expect("no hard error");

        // The evidence FIRST: our row is durable and the completion sits BEHIND it, so the
        // drive that completed the gate folded OUR decision.
        let events = journal.load(run).await.unwrap();
        let decided = events
            .iter()
            .find(
                |(_, e)| matches!(e, JournalEvent::GateDecided { node, .. } if node == &release()),
            )
            .map(|(s, _)| *s)
            .expect("the decision is durable");
        let completed = events
            .iter()
            .find(|(_, e)| {
                matches!(e, JournalEvent::ContextWrite { key, .. } if key.0 == release().0)
            })
            .map(|(s, _)| *s)
            .expect("the racing drive completed the gate");
        assert!(
            decided < completed,
            "precondition: the gate completed by folding OUR decision (decided={decided} \
             completed={completed})"
        );
        assert_eq!(
            journaled_decisions(&journal, run, &release()).await.len(),
            1,
            "and there is no OTHER decision it could have read instead"
        );

        assert_eq!(
            out.code, EXIT_OK,
            "the decision was delivered, folded and consumed — this is the success case, \
             not a refusal: {}",
            out.text
        );
        assert!(
            !out.text.contains("run wake"),
            "`wake` refuses every non-paused run, so this advice is a dead end no shipped \
             store can satisfy: {}",
            out.text
        );
        assert_eq!(
            inner.status(run).await.unwrap().unwrap().status,
            RunStatus::Completed,
            "the racing drive really did finish the run"
        );
    }

    /// Deliver `reject --reason "the canary suite is red"` against a worker that claims
    /// the run inside the delivery window and FAILS the gate with `error`.
    ///
    /// One fixture for both post-append failure tests, and the decision is IDENTICAL in
    /// both: only the journaled `NodeFailed` text differs. That is the point — the
    /// option's outcome is `Fail` either way, so `chosen.outcome` alone cannot tell a
    /// honoured rejection from a deadline that fired in the same window, and a report that
    /// tried to would be wrong exactly half the time.
    ///
    /// Returns the outcome, the seq of our `GateDecided` and the seq of the drive's
    /// `NodeFailed`, so each test can pin the ORDERING its claim depends on.
    async fn decide_against_a_failing_drive(error: &'static str) -> (Outcome, Seq, Seq) {
        let run = RunId(uuid::Uuid::new_v4());
        // A TIMED gate already due, so a worker's `claim_due` really can grab it in the
        // window; the deadline itself is still ahead of `now`, so the pre-check does not
        // refuse first.
        let inner = paused_store(run, Some(now())).await;
        let journal = std::sync::Arc::new(
            gate_journal(
                run,
                &release(),
                Some(now() + chrono::Duration::seconds(1)),
                &["ship", "reject"],
            )
            .await,
        );
        let racing = GateRacingStore {
            inner: inner.clone(),
            journal: journal.clone(),
            run,
            drive: GateRacingDrive::FailsTheNode(error),
        };

        let out = decide(
            &racing,
            journal.as_ref(),
            run,
            release(),
            "reject",
            "alice",
            Some("the canary suite is red"),
            now(),
        )
        .await
        .expect("no hard error");

        let events = journal.load(run).await.unwrap();
        let seq_of = |p: fn(&JournalEvent) -> bool| {
            events
                .iter()
                .find(|(_, e)| p(e))
                .map(|(s, _)| *s)
                .expect("the event is on the journal")
        };
        let decided = seq_of(|e| matches!(e, JournalEvent::GateDecided { .. }));
        let failed = seq_of(|e| matches!(e, JournalEvent::NodeFailed { .. }));
        assert_eq!(
            journaled_decisions(&journal, run, &release()).await.len(),
            1,
            "precondition: exactly one decision is durable"
        );
        assert_eq!(
            inner.status(run).await.unwrap().unwrap().status,
            RunStatus::Failed,
            "precondition: the racing drive really did fail the run"
        );
        (out, decided, failed)
    }

    /// A rejection that a racing worker READ AND HONOURED is a SUCCESS, not a lost
    /// delivery.
    ///
    /// The generic `(other, true)` arm was imported wholesale from `cmd::run::signal`,
    /// where it is sound: `run_await_signal` completes on ANY folded payload and never
    /// fails a node BECAUSE of one, so for an `AwaitSignal` a `NodeFailed` ahead of the
    /// delivery really does mean nothing read it. `HumanGate` breaks that premise —
    /// `GateOutcome::Fail` makes a `NodeFailed` the CORRECT, requested outcome of a
    /// decision that was read — and the arm's own comment enumerated only "an expired
    /// deadline, or a cascade skip".
    ///
    /// So `torii run gate reject --reason "the canary suite is red"`, raced by a worker
    /// claiming the run inside the window `force_wake` itself opens, exited 2 saying the
    /// decision "terminated while this decision was in flight, and a drive that had
    /// already loaded the journal would not have seen it" — every clause of which is
    /// false here: the drive loaded the journal AFTER our row, read the decision, and
    /// failed the node BECAUSE of it. The operator is told their rejection was lost, and
    /// the retry they are pushed toward is refused by the node-state pre-check.
    #[tokio::test]
    async fn a_rejection_a_racing_worker_already_folded_is_not_reported_as_not_read() {
        let (out, decided, failed) = decide_against_a_failing_drive(EXECUTOR_REJECTION).await;

        // The evidence FIRST: the failure sits BEHIND our row, so the drive that failed
        // the node folded a journal that already contained the decision.
        assert!(
            decided < failed,
            "precondition: the gate failed by folding OUR decision (decided={decided} \
             failed={failed})"
        );

        assert_eq!(
            out.code, EXIT_OK,
            "the rejection was delivered, folded and honoured — the run stopped because \
             of it, which is what a Fail option is for: {}",
            out.text
        );
        assert!(
            !out.text.contains("would not have seen it"),
            "the drive DID see it — this sentence is false on every clause: {}",
            out.text
        );
    }

    /// The other side of that discriminator, and the reason it cannot be
    /// `chosen.outcome == Fail`: `wait_or_expire` takes `Expired` BEFORE any decision is
    /// read, so a deadline firing inside the same window journals a `NodeFailed` at the
    /// same place with the same `(Failed, at > appended)` shape — for a decision nothing
    /// looked at.
    ///
    /// Reported as decided, this would tell an operator their rejection stopped a run
    /// that in fact died of its SLA, and would hide the far more useful fact that the
    /// gate expired. This test and its sibling above are also what stops the whole
    /// post-append classification collapsing to `if at.is_some() { ok("decided") }`,
    /// which left 192 tests green.
    #[tokio::test]
    async fn a_decision_a_racing_expiry_failed_past_is_not_reported_as_decided() {
        let (out, decided, failed) = decide_against_a_failing_drive(EXECUTOR_EXPIRY).await;

        assert!(
            decided < failed,
            "precondition: identical ORDERING to the honoured rejection — only the \
             journaled reason differs (decided={decided} failed={failed})"
        );

        assert_eq!(
            out.code, EXIT_PRECONDITION,
            "the deadline fired before any decision was read — reporting this as decided \
             claims a rejection landed that nothing ever looked at: {}",
            out.text
        );
        assert!(
            out.text.contains("not read"),
            "must say plainly that nothing read it: {}",
            out.text
        );
    }

    /// The `(other, false)` arm — a node that died INSIDE the write window, so our row
    /// landed behind a marker that was already there. Reached with a journal whose
    /// `append` slips the `NodeFailed` in first: the store hook fires on `force_wake`,
    /// which is post-append by construction, so no `SchedulerStore` fixture can produce
    /// this ordering.
    ///
    /// It must never report success: the residue is durable and consequential — a
    /// `HumanGate` journals no `NodeCompleted` and `NodeFailed` is not folded as a
    /// barrier, so a re-`start` would re-execute the gate and fold this late decision as
    /// its answer.
    #[tokio::test]
    async fn a_decision_the_node_died_before_is_reported_as_an_orphan_not_as_decided() {
        struct DiesInsideTheWindow {
            inner: std::sync::Arc<InMemoryJournal>,
        }
        #[async_trait::async_trait]
        impl ExecutionJournal for DiesInsideTheWindow {
            async fn append(
                &self,
                run: RunId,
                event: JournalEvent,
            ) -> Result<Seq, orchestrator_core::JournalError> {
                if matches!(event, JournalEvent::GateDecided { .. }) {
                    self.inner
                        .append(
                            run,
                            JournalEvent::NodeFailed {
                                node: release(),
                                error: EXECUTOR_EXPIRY.to_string(),
                            },
                        )
                        .await?;
                }
                self.inner.append(run, event).await
            }
            async fn load(
                &self,
                run: RunId,
            ) -> Result<Vec<(Seq, JournalEvent)>, orchestrator_core::JournalError> {
                self.inner.load(run).await
            }
        }

        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let inner = std::sync::Arc::new(gate_journal(run, &release(), None, &["ship"]).await);
        let j = DiesInsideTheWindow {
            inner: inner.clone(),
        };

        let out = decide(&s, &j, run, release(), "ship", "alice", None, now())
            .await
            .expect("no hard error");

        let events = inner.load(run).await.unwrap();
        let seq_of = |p: fn(&JournalEvent) -> bool| {
            events
                .iter()
                .find(|(_, e)| p(e))
                .map(|(s, _)| *s)
                .expect("the event is on the journal")
        };
        assert!(
            seq_of(|e| matches!(e, JournalEvent::NodeFailed { .. }))
                < seq_of(|e| matches!(e, JournalEvent::GateDecided { .. })),
            "precondition: the node was already dead when the row landed"
        );

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("was already") && out.text.contains("nothing read it"),
            "must say the row is a durable orphan, not a delivery: {}",
            out.text
        );
        // The RESIDUE claim, which is kind-specific and true only here: a `HumanGate`
        // journals no `NodeCompleted` and `NodeFailed` is not folded as a barrier, so a
        // re-`start` really would re-execute the gate and fold this late row. Asserted so
        // that the loop-gate twin's opposite assertion is a pair rather than a lone claim.
        assert!(
            out.text.contains("re-`start`"),
            "…and must say what the durable residue will DO, which for this kind is be \
             folded as the node's answer on a re-`start`: {}",
            out.text
        );
    }

    /// **The same arm, on the kind whose residue behaves the OPPOSITE way.**
    ///
    /// No test reached any post-append reporting arm with a loop gate, and the inherited
    /// `(other, false)` sentence — "a last-wins value that a re-`start` of this run would
    /// fold as the node's answer" — is FALSE for one. `run_human_loop_gate` reads a
    /// terminal verdict BACK rather than re-deriving it: step 0 replays a folded
    /// `NodeFailed` forever, and step 1 replays `LoopGateSettled` without ever consulting
    /// `LoopGateDecided` again. So the decision is inert, and an operator told the durable
    /// residue will be folded may act to remove a row that will never be read.
    ///
    /// `LoopGateSettled` is the marker, because it is the one this kind actually writes on
    /// the honoured path (a loop gate journals no `NodeCompleted`) and because the pre-check
    /// refuses a gate that was already settled when the command STARTED — reaching this arm
    /// requires the settlement to land INSIDE the write window, which is what the local
    /// `SettlesInsideTheWindow` journal produces. It is the loop-kind twin of
    /// `a_decision_the_node_died_before_is_reported_as_an_orphan_not_as_decided`'s
    /// `DiesInsideTheWindow`, and for the same reason that one exists: the store hook fires
    /// on `force_wake`, which is post-append by construction, so no `SchedulerStore` fixture
    /// can produce this ordering.
    #[tokio::test]
    async fn a_loop_gate_decision_the_settlement_beat_is_reported_as_inert_not_as_foldable() {
        struct SettlesInsideTheWindow {
            inner: std::sync::Arc<InMemoryJournal>,
        }
        #[async_trait::async_trait]
        impl ExecutionJournal for SettlesInsideTheWindow {
            async fn append(
                &self,
                run: RunId,
                event: JournalEvent,
            ) -> Result<Seq, orchestrator_core::JournalError> {
                if matches!(event, JournalEvent::LoopGateDecided { .. }) {
                    self.inner
                        .append(
                            run,
                            JournalEvent::LoopGateSettled {
                                node: loop_gate(),
                                option: "revise".into(),
                            },
                        )
                        .await?;
                }
                self.inner.append(run, event).await
            }
            async fn load(
                &self,
                run: RunId,
            ) -> Result<Vec<(Seq, JournalEvent)>, orchestrator_core::JournalError> {
                self.inner.load(run).await
            }
        }

        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let inner = std::sync::Arc::new(
            loop_gate_journal(run, &loop_gate(), None, &["revise", "ship"]).await,
        );
        let j = SettlesInsideTheWindow {
            inner: inner.clone(),
        };

        let out = decide(&s, &j, run, loop_gate(), "ship", "alice", None, now())
            .await
            .expect("no hard error");

        let events = inner.load(run).await.unwrap();
        let seq_of = |p: fn(&JournalEvent) -> bool| {
            events
                .iter()
                .find(|(_, e)| p(e))
                .map(|(s, _)| *s)
                .expect("the event is on the journal")
        };
        assert!(
            seq_of(|e| matches!(e, JournalEvent::LoopGateSettled { .. }))
                < seq_of(|e| matches!(e, JournalEvent::LoopGateDecided { .. })),
            "precondition: the gate had settled before the row landed"
        );

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("was already") && out.text.contains("nothing read it"),
            "must say the row is a durable orphan, not a delivery: {}",
            out.text
        );
        assert!(
            !out.text.contains("re-`start`"),
            "a loop gate's decision is NOT folded on a re-`start`: step 1 of \
             `run_human_loop_gate` replays the SETTLEMENT and never re-reads a decision, \
             so this sentence sends an operator to clean up a row nothing will read: {}",
            out.text
        );
        assert!(
            out.text.contains("nothing will ever fold"),
            "…and must say what is actually true — the row is inert: {}",
            out.text
        );
    }

    /// The other half of the post-append window: the run went TERMINAL while the node
    /// itself never did. `cancel` is node-blind and journals no node event, so the gate
    /// still folds as awaiting — `at: None` — and the run will never be paused again.
    ///
    /// "Run `torii run wake <id>` once it is paused again" is a dead end here for exactly
    /// the reason the PRE-check arm was already fixed
    /// (`a_decision_on_a_terminal_run_does_not_advise_waiting_for_a_pause`): `wake` refuses
    /// every non-paused run and no shipped store moves a terminal row back to `paused`.
    /// The rule has to hold on BOTH arms of the same function.
    #[tokio::test]
    async fn a_decision_orphaned_by_a_cancel_does_not_advise_waiting_for_a_pause() {
        let run = RunId(uuid::Uuid::new_v4());
        let inner = paused_store(run, Some(now())).await;
        let journal = std::sync::Arc::new(
            gate_journal(
                run,
                &release(),
                Some(now() + chrono::Duration::seconds(1)),
                &["ship"],
            )
            .await,
        );
        let racing = GateRacingStore {
            inner: inner.clone(),
            journal: journal.clone(),
            run,
            drive: GateRacingDrive::CancelsTheRun,
        };

        let out = decide(
            &racing,
            journal.as_ref(),
            run,
            release(),
            "ship",
            "alice",
            None,
            now(),
        )
        .await
        .expect("no hard error");

        assert_eq!(
            inner.status(run).await.unwrap().unwrap().status,
            RunStatus::Cancelled,
            "precondition: the run really was cancelled inside the window"
        );
        assert_eq!(
            journaled_decisions(&journal, run, &release()).await.len(),
            1,
            "precondition: the decision is durable — this is a post-append report, not a \
             refusal"
        );

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("cancelled"),
            "must name the actual state: {}",
            out.text
        );
        assert!(
            !out.text.contains("once it is paused again"),
            "a cancelled run never pauses again — this is advice to wait forever: {}",
            out.text
        );
    }

    /// A journal fault raised AFTER the append reaches stdout as an operator report
    /// rather than as an error — and a backend message is free text from the driver, so
    /// it gets the SAME transform `list-paused` already gives the identical error class:
    /// redact, collapse control characters, cap.
    ///
    /// `PostgresJournal::load` builds this message from both `sqlx::Error` and
    /// `serde_json::Error`, so it can carry a connection string with a password (the
    /// realistic pool-timeout case) and — over a TYPED `JournalEvent` — quoted row
    /// content, which is the `invalid type: string "sk-live-…"` shape this crate
    /// documents at `parse_payload`. Unguarded it also carries a newline that forges a
    /// pastable run row and a raw ESC that rewrites what is already on screen.
    ///
    /// `run list-paused` proves the same property on the same fixture
    /// (`list_paused_never_leaks_a_connection_string_or_forges_a_row_from_a_journal_fault`),
    /// which is why the fixture is shared rather than copied.
    #[tokio::test]
    async fn a_journal_fault_after_the_append_is_not_echoed_raw() {
        /// Folds cleanly for the PRE-check and faults on the post-append re-read — the
        /// one window in which a backend message reaches this command's stdout.
        struct FaultingReloadJournal {
            inner: std::sync::Arc<InMemoryJournal>,
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
                Err(crate::cmd::run::tests::hostile_backend_error(run))
            }
        }

        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let inner = std::sync::Arc::new(gate_journal(run, &release(), None, &["ship"]).await);
        let j = FaultingReloadJournal {
            inner: inner.clone(),
            loads: std::sync::atomic::AtomicUsize::new(0),
        };

        let out = decide(&s, &j, run, release(), "ship", "alice", None, now())
            .await
            .expect("a post-append fault is reported, not returned as a bare error");

        assert_eq!(
            journaled_decisions(&inner, run, &release()).await.len(),
            1,
            "precondition: the decision is durable, so this really is the post-append \
             report and not a refusal"
        );
        assert!(
            out.text.contains("journaled durably"),
            "precondition: the fault must be reported on the unread path: {}",
            out.text
        );

        assert!(
            !out.text
                .contains(&crate::cmd::run::tests::hostile_password()),
            "a journal fault leaked the database password: {}",
            out.text
        );
        assert!(
            out.text.contains("[REDACTED]"),
            "the credential span must be visibly redacted — dropping the message \
             entirely would pass the leak assertion while telling the operator nothing: \
             {}",
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

    /// Neither hazard an option name carries may survive into `decide`'s stdout: no raw
    /// ESC (which could rewrite what is already on screen) and no line that reads as an
    /// awaiting row an operator might paste into `run cancel`.
    fn assert_clean(what: &str, text: &str) {
        assert!(
            !text.contains('\u{1b}'),
            "{what}: a raw escape byte survived into the output: {text:?}"
        );
        assert_eq!(
            text.lines()
                .filter(|l| l.trim_start().starts_with(FORGED_RUN))
                .count(),
            0,
            "{what}: a newline in an option name forged a line that reads as its own \
             awaiting row:\n{text}"
        );
    }

    /// An option name is AUTHOR free text and it reaches this command's stdout on TWO
    /// paths — so both get the collapse `render::awaiting_section` already applies to the
    /// very same values (`one_line` + `cap_chars`), which `decide` did not.
    ///
    /// "Author free text" is not hypothetical here: a menu arrives from a `run submit`
    /// JSON file, a `scheduled_runs.graph` row, or a runtime `Expand` subgraph produced by
    /// a planner model — and `validate_dag` checks a `HumanGate`'s options only for
    /// non-emptiness, uniqueness and a reachable outcome, never for content. So
    /// `"ship\n<uuid>  release  approved\u{1b}[2K"` is accepted, journaled verbatim, and
    /// recited back here.
    ///
    /// The `{option:?}` interpolations elsewhere in this function are Debug-escaped and
    /// already safe; these two are Display.
    #[tokio::test]
    async fn a_hostile_option_name_cannot_forge_a_line_or_move_the_cursor() {
        let hostile = format!("ship\n{FORGED_RUN}  release  approved\u{1b}[2K");

        // (a) THE REFUSAL, which recites the whole journaled menu back so the operator
        // can retype it.
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &[&hostile]).await;
        let refused = decide(&s, &j, run, release(), "nope", "alice", None, now())
            .await
            .expect("no hard error");
        assert_eq!(refused.code, EXIT_PRECONDITION, "{}", refused.text);
        assert_clean("the recited menu", &refused.text);

        // (b) THE SUCCESS LINE, which echoes the option that was picked — and it is
        // picked BY MATCHING the journaled menu, so it is the same author free text, not
        // something the operator invented.
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &[&hostile]).await;
        let ok = decide(&s, &j, run, release(), &hostile, "alice", None, now())
            .await
            .expect("no hard error");
        assert_eq!(ok.code, EXIT_OK, "{}", ok.text);
        assert_clean("the success line", &ok.text);
    }

    /// The recited menu is CAPPED as well as collapsed, for the reason `MENU_MAX` exists:
    /// `validate_dag` bounds neither an option name's length nor how many there are, so
    /// one 5,000-character name turns a refusal an operator has to READ into a screenful
    /// of scrollback. `list-paused` guards the identical values with
    /// `an_overlong_menu_is_capped_so_it_cannot_wreck_the_block`.
    #[tokio::test]
    async fn an_overlong_menu_is_capped_in_the_refusal() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let long = "s".repeat(5_000);
        let j = gate_journal(run, &release(), None, &[&long]).await;

        let out = decide(&s, &j, run, release(), "nope", "alice", None, now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.chars().count() < 700,
            "an unbounded menu floods the refusal: {} chars",
            out.text.chars().count()
        );
        assert!(
            out.text.contains('…'),
            "truncation must be visible: {}",
            out.text
        );
    }

    /// §6.5 for the OTHER writer to the same durable column. `cmd::run::signal` enforces
    /// `MAX_PAYLOAD_BYTES` because "an unbounded JSON blob in a journal row is a durable
    /// footgun … the cap is enforced in the CLI, at every writer, by rejecting" — wording
    /// that read "at the ONLY writer" until this command became the second one, which is
    /// the drift this test exists to stop — and `GateDecided.note` was appended with no
    /// length check at all. `ARG_MAX` permits a
    /// ~120 KB `--note`, which is 30x the sibling's limit, and the row is durable: it is
    /// reloaded on every drive, folded into the gate's output on a `Complete` option, and
    /// carried in every downstream Agent's prompt for the life of the run.
    ///
    /// Exit 1, matching `signal` exactly: exit 2 in this taxonomy means "ran fine, nothing
    /// to do", and an over-limit note is invalid INPUT. The two writers must not disagree
    /// about the exit code for one violation.
    #[tokio::test]
    async fn an_oversized_note_is_rejected_before_anything_is_journaled() {
        use crate::cmd::run::MAX_PAYLOAD_BYTES;
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship"]).await;
        let huge = "x".repeat(MAX_PAYLOAD_BYTES + 1000);

        let e = decide(&s, &j, run, release(), "ship", "alice", Some(&huge), now())
            .await
            .expect_err("an over-limit note is refused");

        assert_eq!(e.code, crate::errors::EXIT_ERROR, "{}", e.message);
        assert!(
            e.message.contains(&MAX_PAYLOAD_BYTES.to_string()),
            "must name the limit: {}",
            e.message
        );
        assert!(
            !e.message.contains(&huge),
            "and must not echo the note back: {}",
            e.message
        );
        // The `what` parameter exists for exactly this: `check_payload_size` is shared
        // with `run signal`, and its message named `--payload` unconditionally until this
        // command started calling it. Telling a `gate decide --note` operator to shorten
        // their `--payload` sends them to a flag this command does not have.
        assert!(
            e.message.contains("the decision note"),
            "must name the input the operator actually supplied: {}",
            e.message
        );
        assert!(
            !e.message.contains("--payload"),
            "`gate decide` has no `--payload` flag — naming it is advice to edit \
             something that does not exist: {}",
            e.message
        );
        assert!(
            journaled_decisions(&j, run, &release()).await.is_empty(),
            "an over-limit note must never reach the journal"
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            None,
            "and a refused decision must not queue a wake"
        );
    }

    /// **The SIBLING field on the same durable row.** `--as` was bounded by nothing while
    /// `--note` was held to 4096 bytes, so `ARG_MAX` permitted a ~131 KB actor — 32x the
    /// cap — accepted with exit 0 and journaled, in the same `GateDecided` the note is
    /// bounded in. `GateDecided.actor` is not merely displayed: the executor interpolates
    /// it into the `NodeFailed` message that `torii run status` renders and that every
    /// later drive re-emits from the fold.
    ///
    /// Exit 1 and the same helper as the note, so the two fields of one row cannot
    /// disagree about the number, the measurement or the exit code.
    #[tokio::test]
    async fn an_oversized_actor_is_rejected_before_anything_is_journaled() {
        use crate::cmd::run::MAX_PAYLOAD_BYTES;
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship"]).await;
        let huge = "a".repeat(MAX_PAYLOAD_BYTES + 1000);

        let e = decide(&s, &j, run, release(), "ship", &huge, Some("ok"), now())
            .await
            .expect_err("an over-limit actor is refused");

        assert_eq!(e.code, crate::errors::EXIT_ERROR, "{}", e.message);
        assert!(
            e.message.contains(&MAX_PAYLOAD_BYTES.to_string()),
            "must name the limit: {}",
            e.message
        );
        assert!(
            !e.message.contains(&huge),
            "and must not echo the actor back: {}",
            e.message
        );
        assert!(
            e.message.contains("--as"),
            "must name the flag the operator actually typed: {}",
            e.message
        );
        // **The other half of the asymmetry `decide` argues for.** A `HumanGate`'s actor is
        // journaled AS GIVEN — the executor scrubs it on the way back out — so this message
        // must NOT offer redaction as the explanation for a size it never grew by. The two
        // arms are one `match` on one line, so without this the discriminant could be set
        // on both and nothing would notice: the loop-gate test asserts only that the
        // explanation is PRESENT.
        assert!(
            !e.message.contains("once redacted"),
            "a `HumanGate`'s actor is written as typed, so the growth explanation would \
             name a transform this value never went through: {}",
            e.message
        );
        assert!(
            journaled_decisions(&j, run, &release()).await.is_empty(),
            "an over-limit actor must never reach the journal"
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            None,
            "and a refused decision must not queue a wake"
        );
    }

    /// The success line echoes the chosen option, and it is the path an operator ALWAYS
    /// reaches — yet it was the only one of the two echoes left uncapped. Measured before
    /// the fix: the refusal path rendered 324 chars with an ellipsis and the success path
    /// 5065 with none, for the same journaled value.
    ///
    /// Same `MENU_MAX`, not a second bound: it was made `pub(crate)` in the very commit
    /// that capped the refusal, with the rationale "one bound for one class of value, not
    /// two that can drift". An option name is author free text from a `run submit` file, a
    /// `scheduled_runs.graph` row or a planner's `Expand` subgraph, and `validate_dag`
    /// bounds neither its length nor how many there are.
    #[tokio::test]
    async fn an_overlong_option_name_is_capped_in_the_success_line() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let long = "s".repeat(5_000);
        let j = gate_journal(run, &release(), None, &[&long]).await;

        let out = decide(&s, &j, run, release(), &long, "alice", None, now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_OK, "{}", out.text);
        assert!(
            out.text.chars().count() < 400,
            "an unbounded option name floods the line an operator always reads: {} chars",
            out.text.chars().count()
        );
        assert!(
            out.text.contains('…'),
            "truncation must be visible: {}",
            out.text
        );
    }

    /// The cap is measured on the REDACTED note — the bytes actually written — not on
    /// what the operator typed. `[REDACTED]` is 10 bytes and the assignment pattern's
    /// shortest matched value is 6, so redaction GROWS a note of short `token:…` pairs by
    /// roughly 1.67x; a check placed before the scrub bounds a value nobody stores. That
    /// is a defect `signal` shipped once and fixed, and its `Measured` doc records the
    /// measurement.
    ///
    /// The pair is assembled at runtime: the repo's Semgrep CWE-798 hook blocks a
    /// credential-shaped literal in a fixture.
    #[tokio::test]
    async fn a_note_that_only_exceeds_the_cap_after_redaction_is_rejected() {
        use crate::cmd::run::MAX_PAYLOAD_BYTES;
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship"]).await;

        // The trailing space is load-bearing: without a separator the value class runs to
        // the end of the string and the whole run collapses into ONE placeholder, which
        // shrinks rather than grows.
        let unit = format!("{}:{} ", "token", "abcdef");
        let raw = unit.repeat(310);
        let as_given = serde_json::to_vec(&serde_json::json!(raw)).unwrap().len();
        let journaled = serde_json::to_vec(&render::redact_payload(&serde_json::json!(raw)))
            .unwrap()
            .len();
        assert!(
            as_given <= MAX_PAYLOAD_BYTES,
            "precondition: this note is under the cap as typed ({as_given} bytes)"
        );
        assert!(
            journaled > MAX_PAYLOAD_BYTES,
            "precondition: redaction GROWS it past the cap ({as_given} -> {journaled} bytes)"
        );

        let e = decide(&s, &j, run, release(), "ship", "alice", Some(&raw), now())
            .await
            .expect_err("a note that would exceed the cap once redacted is refused");

        assert_eq!(e.code, crate::errors::EXIT_ERROR, "{}", e.message);
        assert!(
            e.message.contains(&journaled.to_string()),
            "must name the size that would actually be JOURNALED ({journaled}), not the \
             one the operator typed: {}",
            e.message
        );
        assert!(
            journaled_decisions(&j, run, &release()).await.is_empty(),
            "an over-limit row must never reach the journal"
        );
    }

    /// **The mapping every other test in this crate took on trust.** Swapping the
    /// `"approve"` and `"reject"` literals used to leave the ENTIRE torii suite green —
    /// lib, `cli.rs` and the e2e alike — because `cli.rs` only proves the three verbs are
    /// LISTED and that clap requires `--reason`, and every library test calls [`decide`]
    /// with an option string it passed in itself. Nothing anywhere read what a VERB
    /// produces.
    ///
    /// The consequence of that gap is the exact inversion this slice's fail-closed design
    /// exists to prevent: `torii run gate reject --reason "security hole"` would deliver
    /// the `approve` option, a gate whose `approve` is `Complete` would SHIP, and the
    /// report would read `decided: release = approve` — so nothing looks wrong at any
    /// layer. A human's rejection becomes an approval, silently.
    ///
    /// The passthrough fields are asserted with three DISTINCT values on purpose: they are
    /// all `String`, so a shuffled destructuring is invisible to the compiler, and that is
    /// the second thing collapsing three verbs into one call site was supposed to prevent.
    #[test]
    fn each_verb_maps_to_the_option_that_names_it() {
        let approve = decision_of(GateAction::Approve {
            run_id: "the-run".into(),
            node: "release".into(),
            r#as: "alice".into(),
            note: Some("canary is green".into()),
        });
        assert_eq!(approve.option, "approve", "`gate approve` picks `approve`");
        assert_eq!(approve.run_id, "the-run");
        assert_eq!(approve.node, "release");
        assert_eq!(approve.actor, "alice");
        assert_eq!(approve.note.as_deref(), Some("canary is green"));

        // `--reason` is where a rejection's note comes from, and it is not optional.
        let reject = decision_of(GateAction::Reject {
            run_id: "the-run".into(),
            node: "release".into(),
            r#as: "bob".into(),
            reason: "security hole".into(),
        });
        assert_eq!(
            reject.option, "reject",
            "`gate reject` must NEVER deliver `approve` — that is a rejection shipping the \
             release"
        );
        assert_eq!(reject.run_id, "the-run");
        assert_eq!(reject.node, "release");
        assert_eq!(reject.actor, "bob");
        assert_eq!(
            reject.note.as_deref(),
            Some("security hole"),
            "`--reason` IS the note — `decide` refuses a Fail option without one"
        );

        // The general form passes the operator's own option through untouched, including
        // one that happens to be spelled like a verb.
        for option in ["hold", "approve", "reject"] {
            let decide = decision_of(GateAction::Decide {
                run_id: "the-run".into(),
                node: "release".into(),
                option: option.into(),
                r#as: "carol".into(),
                note: None,
            });
            assert_eq!(decide.option, option);
            assert_eq!(decide.actor, "carol");
            assert_eq!(decide.note, None, "an omitted --note stays absent");
        }
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

    // ---- SP-6 s4: the SECOND menu-bearing kind, a human-decided LOOP gate -------------

    /// The synthetic path a `Loop` gate is asked at. It exists in NO graph —
    /// `run_loop` composes it per iteration from the loop's own id — which is exactly
    /// why every check in this command reads the JOURNAL: there is nothing else to read.
    pub(crate) fn loop_gate() -> NodeId {
        NodeId("lp/0/__gate__".into())
    }

    /// A journal in which a LOOP gate has already asked, with the given menu.
    ///
    /// `stops` is `true` for an option literally named `ship` and `false` otherwise —
    /// the same one-name convention [`gate_journal`] uses for `reject`, and enough to
    /// build a converging menu without a second helper. Nothing in `torii` reads `stops`
    /// (the executor resolves it), so the flag is fixture realism rather than a subject.
    pub(crate) async fn loop_gate_journal(
        run: RunId,
        node: &NodeId,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
        options: &[&str],
    ) -> InMemoryJournal {
        loop_gate_journal_asking(run, node, deadline, options, LOOP_GATE_QUESTION).await
    }

    /// [`loop_gate_journal`] with the question spelled out, for the tests whose subject IS
    /// the question rather than the menu.
    ///
    /// The loop gate is the kind most exposed there, which is why it needs its own: its
    /// prompt is composed by `HumanQuestion::compose` from the gate role's system prompt,
    /// its activated skill bodies AND the ITERATION OUTPUT — model text about the run —
    /// so it is the longest, the most `## Task`-shaped and the most likely of the four to
    /// carry a credential. `cmd::run`'s listing tests need an overlong compose-shaped one
    /// and a secret-shaped one, both through the SAME `LoopGateAwaited` shape the executor
    /// writes rather than a hand-rolled event, for the reason [`loop_gate`] records.
    pub(crate) async fn loop_gate_journal_asking(
        run: RunId,
        node: &NodeId,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
        options: &[&str],
        prompt: &str,
    ) -> InMemoryJournal {
        let j = InMemoryJournal::new();
        j.append(
            run,
            JournalEvent::LoopGateAwaited {
                node: node.clone(),
                deadline,
                prompt: prompt.to_string(),
                menu: options
                    .iter()
                    .map(|o| orchestrator_core::LoopGateOption {
                        name: o.to_string(),
                        stops: *o == "ship",
                    })
                    .collect(),
            },
        )
        .await
        .unwrap();
        j
    }

    /// The question [`loop_gate_journal`] publishes. Named rather than inlined because
    /// `cmd::run`'s listing tests assert on this exact text, for the reason
    /// `cmd::human::tests::THE_QUESTION` records.
    pub(crate) const LOOP_GATE_QUESTION: &str = "Is this draft good enough to ship?";

    /// Every `LoopGateDecided` journaled for `node`, as `(option, actor)`.
    ///
    /// A SECOND reader beside [`journaled_decisions`] rather than one generic over both,
    /// deliberately: the whole point of this task is that the two events are not
    /// interchangeable, and a helper that flattened them would let a test asserting "a
    /// loop gate was decided" pass on a `GateDecided` — the exact confusion the split
    /// exists to prevent.
    async fn journaled_loop_decisions(
        j: &InMemoryJournal,
        run: RunId,
        node: &NodeId,
    ) -> Vec<(String, String)> {
        j.load(run)
            .await
            .unwrap()
            .into_iter()
            .filter_map(|(_, e)| match e {
                JournalEvent::LoopGateDecided {
                    node: n,
                    option,
                    actor,
                } if &n == node => Some((option, actor)),
                _ => None,
            })
            .collect()
    }

    /// AC17 — `run gate decide` decides a LOOP gate at its synthetic path, and the row it
    /// writes is a `LoopGateDecided`.
    ///
    /// It works at all only because [`gate_menu`] reads the menu from the JOURNAL: the
    /// node exists in no graph, so there is no other place a menu could come from.
    ///
    /// The negative assertion is the load-bearing half. `GateDecided` also carries an
    /// `option: String`, so appending it here would compile and would look right in the
    /// journal — and the executor's `run_human_loop_gate` reads only `LoopGateDecided`,
    /// so the decision would be journaled, never read, and reported as delivered.
    #[tokio::test]
    async fn a_loop_gate_is_decided_at_its_synthetic_path() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = loop_gate_journal(run, &loop_gate(), None, &["revise", "ship"]).await;

        let out = decide(&s, &j, run, loop_gate(), "ship", "alice", None, now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_OK, "{}", out.text);
        assert!(
            out.text.contains("ship"),
            "confirms the choice: {}",
            out.text
        );
        assert_eq!(
            journaled_loop_decisions(&j, run, &loop_gate()).await,
            vec![("ship".to_string(), "alice".to_string())],
            "a LoopGateDecided is appended — never a GateDecided, which is resolved \
             against a menu whose options carry a GateOutcome this kind cannot interpret"
        );
        assert!(
            journaled_decisions(&j, run, &loop_gate()).await.is_empty(),
            "and no GateDecided beside it"
        );
        assert!(
            s.status(run).await.unwrap().unwrap().next_wake.is_some(),
            "a delivered decision queues the wake, exactly as a HumanGate's does"
        );
    }

    /// AC17 — a bad option recites the JOURNALED menu, so the operator can retry without
    /// reading a graph that does not contain this node.
    #[tokio::test]
    async fn a_bad_option_recites_a_loop_gates_journaled_menu() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = loop_gate_journal(run, &loop_gate(), None, &["revise", "ship"]).await;

        let out = decide(&s, &j, run, loop_gate(), "sideways", "alice", None, now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("revise") && out.text.contains("ship"),
            "recites the menu: {}",
            out.text
        );
        assert!(
            journaled_loop_decisions(&j, run, &loop_gate())
                .await
                .is_empty(),
            "and nothing is journaled — the executor fails a loop gate LOUDLY on an \
             option its menu does not carry, which is terminal for the whole Loop"
        );
    }

    /// A loop gate's decision carries no note, so a `--note` must be REFUSED rather than
    /// dropped. `LoopGateDecided` is `{node, option, actor}` — there is no field to put it
    /// in — and an operator who typed an explanation has every reason to believe it was
    /// recorded. Silently discarding operator input is the defect class this slice keeps
    /// finding; this is the same judgement `gate reject --reason ''` makes in reverse.
    #[tokio::test]
    async fn a_note_on_a_loop_gate_is_refused_rather_than_dropped() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = loop_gate_journal(run, &loop_gate(), None, &["revise", "ship"]).await;

        let out = decide(
            &s,
            &j,
            run,
            loop_gate(),
            "ship",
            "alice",
            Some("the canary suite is green"),
            now(),
        )
        .await
        .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("--note"),
            "must name the flag the operator typed: {}",
            out.text
        );
        assert!(
            journaled_loop_decisions(&j, run, &loop_gate())
                .await
                .is_empty(),
            "nothing is journaled: a decision recorded without the note beside it is the \
             silent drop this refusal exists to prevent"
        );
    }

    /// The deadline guard, inherited by construction from the `SignalState::Awaiting`
    /// arm — but only once `signal_states` folds `LoopGateAwaited`, so it is asserted on
    /// this kind too rather than assumed.
    ///
    /// `now == deadline` is EXPIRED for the executor (`wait_or_expire`'s `now >= d`), and
    /// `run_human_loop_gate` reads expiry BEFORE any decision, so a CLI that accepted this
    /// would report success and then watch the next wake terminally fail the whole `Loop`.
    /// The sibling boundary test is `a_decision_exactly_at_the_gates_deadline_is_refused`.
    #[tokio::test]
    async fn a_loop_gate_decision_exactly_at_the_deadline_is_refused() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, Some(now())).await;
        let j = loop_gate_journal(run, &loop_gate(), Some(now()), &["revise", "ship"]).await;

        let out = decide(&s, &j, run, loop_gate(), "ship", "alice", None, now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("deadline"),
            "must name the deadline that closed the gate: {}",
            out.text
        );
        assert!(
            journaled_loop_decisions(&j, run, &loop_gate())
                .await
                .is_empty(),
            "an expired gate must not be given a decision to read"
        );
    }

    /// The other side of that boundary: one microsecond before the deadline is DELIVERED.
    /// Without it the refusal above is satisfied by a guard that refuses everything.
    #[tokio::test]
    async fn a_loop_gate_decision_before_the_deadline_is_delivered() {
        let run = RunId(uuid::Uuid::new_v4());
        let deadline = now() + chrono::Duration::microseconds(1);
        let s = paused_store(run, Some(deadline)).await;
        let j = loop_gate_journal(run, &loop_gate(), Some(deadline), &["revise", "ship"]).await;

        let out = decide(&s, &j, run, loop_gate(), "ship", "alice", None, now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_OK, "{}", out.text);
        assert_eq!(
            journaled_loop_decisions(&j, run, &loop_gate()).await.len(),
            1,
            "a decision inside the SLA is delivered"
        );
    }

    /// AC16, second half: the loop branch REDACTS `--as` before the durable write.
    ///
    /// This cannot be inherited and that is the trap. The `GateDecided` path deliberately
    /// does NOT redact its actor (it measures `Measured::AsGiven`) because the executor
    /// interpolates that field into a `NodeFailed` and scrubs it on the way out.
    /// `run_human_loop_gate` reads only `option` off `LoopGateDecided`: the actor is put in
    /// no message and no output, so nothing downstream is a second line of defence and
    /// whatever is appended here is what an audit reads forever. s3's whole-slice review
    /// found exactly this leak on `AgentAnswered.actor`.
    ///
    /// The credential is assembled at runtime — the repo's Semgrep CWE-798 hook blocks a
    /// literal one in a fixture.
    #[tokio::test]
    async fn a_secret_shaped_actor_is_redacted_before_a_loop_gate_decision_is_journaled() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = loop_gate_journal(run, &loop_gate(), None, &["revise", "ship"]).await;
        let secret = format!("sk-{}", "A".repeat(24));

        decide(&s, &j, run, loop_gate(), "ship", &secret, None, now())
            .await
            .expect("delivers");

        let durable = format!("{:?}", j.load(run).await.unwrap());
        assert!(
            !durable.contains(&secret),
            "the actor reached durable storage in plaintext: {durable}"
        );
        assert!(durable.contains("[REDACTED]"), "{durable}");
    }

    /// …and the redaction runs BEFORE the size check, so the bytes bounded are the bytes
    /// written. `[REDACTED]` is longer than the shortest span it replaces, so an actor
    /// that fitted as typed can exceed the cap once scrubbed — the ordering `signal`
    /// shipped wrong once, `cmd::human::answer` had to fix, and `Measured` records.
    ///
    /// The pair is assembled at runtime for the Semgrep reason above.
    #[tokio::test]
    async fn a_loop_gate_actor_that_only_exceeds_the_cap_after_redaction_is_rejected() {
        use crate::cmd::run::MAX_PAYLOAD_BYTES;
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = loop_gate_journal(run, &loop_gate(), None, &["ship"]).await;

        // The trailing space is load-bearing: without a separator the value class runs to
        // the end of the string and the whole run collapses into ONE placeholder, which
        // shrinks rather than grows.
        let unit = format!("{}:{} ", "token", "abcdef");
        let raw = unit.repeat(310);
        let as_given = serde_json::to_vec(&serde_json::json!(raw)).unwrap().len();
        // `.trim()` because `actor_or` trims a supplied actor before anything else touches
        // it, and this expectation has to be the size of the row that is actually written
        // — the very property the test is asserting the CHECK gets right.
        let journaled = serde_json::to_vec(&render::redact_payload(&serde_json::json!(raw.trim())))
            .unwrap()
            .len();
        assert!(
            as_given <= MAX_PAYLOAD_BYTES,
            "precondition: this actor is under the cap as typed ({as_given} bytes)"
        );
        assert!(
            journaled > MAX_PAYLOAD_BYTES,
            "precondition: redaction GROWS it past the cap ({as_given} -> {journaled} bytes)"
        );

        let e = decide(&s, &j, run, loop_gate(), "ship", &raw, None, now())
            .await
            .expect_err("an actor that would exceed the cap once redacted is refused");

        assert_eq!(e.code, crate::errors::EXIT_ERROR, "{}", e.message);
        assert!(
            e.message.contains(&journaled.to_string()),
            "must name the size that would actually be JOURNALED ({journaled}), not the \
             one the operator typed: {}",
            e.message
        );
        // **The WORDING, not only the number** — and it is a separate assertion because
        // the byte count cannot see the discriminant. `Measured::AfterRedaction`'s only job
        // is this explanation, and the size assertion above passes for BOTH values: with
        // this arm switched to `AsGiven` the whole torii suite stayed green (measured),
        // leaving an operator who typed 4,030 bytes told "5,580 bytes" with no reason —
        // exactly the confusion `check_payload_size`'s own comment says the discriminant
        // exists to prevent.
        assert!(
            e.message.contains("once redacted"),
            "must explain WHY the row is bigger than what was typed, naming a transform \
             this value really went through: {}",
            e.message
        );
        assert!(
            journaled_loop_decisions(&j, run, &loop_gate())
                .await
                .is_empty(),
            "an over-limit row must never reach the journal"
        );
    }

    /// **The blank-audit-row guard.** `LoopGateDecided.actor` is a required `String`, so
    /// "nobody said who" has no legible encoding: a path that skipped the resolver would
    /// journal clap's empty default as a silent `""`, indistinguishable at a glance from a
    /// real name. [`actor_or_user`] is what stands between an operator and that row, and
    /// this pins that BOTH append sites in [`decide`] route through it — the failure mode
    /// the plan names is a second append site added beside the first, and a second site is
    /// exactly what this task adds.
    ///
    /// Asserted on both kinds because the resolution is one shared line: a fix applied to
    /// only the new branch would leave `GateDecided` — whose `actor_or` doc already
    /// promises "never an empty actor" — able to journal one through the library entry
    /// point.
    #[tokio::test]
    async fn no_decision_can_journal_a_blank_actor() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = loop_gate_journal(run, &loop_gate(), None, &["ship"]).await;
        decide(&s, &j, run, loop_gate(), "ship", "", None, now())
            .await
            .expect("delivers");
        let (_, actor) = journaled_loop_decisions(&j, run, &loop_gate())
            .await
            .pop()
            .expect("a decision was journaled");
        assert!(
            !actor.is_empty(),
            "a blank actor is indistinguishable from a bug in an audit; \
             `actor_or_user` names an unresolvable one `unknown`"
        );

        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &release(), None, &["ship"]).await;
        decide(&s, &j, run, release(), "ship", "", None, now())
            .await
            .expect("delivers");
        let (_, actor, _) = journaled_decisions(&j, run, &release())
            .await
            .pop()
            .expect("a decision was journaled");
        assert!(!actor.is_empty(), "the same rule on the s2 row");
    }

    /// A SETTLED loop gate is one nobody is waiting on: the drive that read the decision
    /// journaled `LoopGateSettled`, spent the iteration on the strength of it, and
    /// `run_human_loop_gate`'s step 1 replays that answer forever WITHOUT re-reading
    /// `LoopGateDecided`. A second decision would therefore be journaled, never read, and
    /// reported as delivered — so it is refused instead.
    ///
    /// This is the operator-facing half of the fold arm `list_paused` needs: without
    /// `LoopGateSettled` folding as the node's terminal marker, iteration 0's gate stays
    /// `Awaiting` for the life of the run, because a loop gate journals no
    /// `NodeCompleted` and writes no blackboard entry of its own.
    #[tokio::test]
    async fn a_decision_on_a_settled_loop_gate_is_refused() {
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

        let out = decide(&s, &j, run, loop_gate(), "ship", "alice", None, now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        // Keyed on the SETTLED wording, not merely on the exit code. Before the fold arm
        // existed this command refused a loop gate for a different reason entirely — "not
        // awaiting a decision", because `gate_menu` read only `GateAwaited` — so an
        // exit-code-only assertion passed while nothing about settlement was implemented.
        assert!(
            out.text.contains("already completed"),
            "the refusal must say the gate is DONE, not that the node is unknown — an \
             operator told 'not awaiting a decision' checks their node id, which was \
             right: {}",
            out.text
        );
        assert!(
            journaled_loop_decisions(&j, run, &loop_gate())
                .await
                .is_empty(),
            "a decision the executor will never re-read must not be written: {}",
            out.text
        );
    }
}
