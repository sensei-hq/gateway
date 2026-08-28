//! `torii run agent answer` — the operator surface for a human-backed `Agent` (SP-6 s3).
//!
//! The third and last of the waiting verbs. [`crate::cmd::run::signal`] delivers arbitrary
//! JSON to an `AwaitSignal`; [`crate::cmd::gate::decide`] delivers a named option to a
//! `HumanGate`; this delivers FREE TEXT, with an actor, to a role the registry says is
//! answered by a person. All three refuse each other's node kinds — the matrix is proven
//! in one place, this module's `mod tests`.
//!
//! **A new file rather than more of `gate.rs`**, matching how the executor put
//! `run_human_agent` in its own `human.rs`: `gate.rs` is the menu path and stays that.
//!
//! Everything hard-won in `signal` and `decide` is reproduced here rather than
//! re-derived — append THEN `force_wake`; the seq-ordered post-write report; a post-append
//! fault reported as a durable-but-unqueued answer instead of a bare store error; the
//! NODE's own state checked, not just the RUN's; and no refusal that advises waiting for a
//! pause a terminal run will never reach. Each of those is a defect one of the two
//! siblings shipped and fixed. Read both before changing this one.
//!
//! **The ONE place this must not copy `decide` is the DEADLINE.** See [`answer`].

use crate::cmd::Outcome;
use crate::cmd::gate::{awaiting_signal, gate_menu};
use crate::cmd::run::{
    Measured, SignalState, SignalStateAt, not_delivered, signal_state, signal_state_at,
};
use crate::errors::CliError;
use crate::render;
use chrono::{DateTime, Utc};
use orchestrator_core::{
    ExecutionJournal, JournalEvent, MAX_HUMAN_TEXT_BYTES, NodeId, OrchestratorError, RunId,
    RunStatus, SchedulerStore, Seq,
};

/// The verbs of `run agent`. One today; the subcommand layer exists so a later verb (an
/// `ask` that re-poses a question, say) is an addition rather than a restructuring, and so
/// the help text has a place to state the trust boundary once for the group.
///
/// **Lives in the library, not in `main.rs`**, for the reason [`crate::cmd::gate::GateAction`]
/// records: the binary has no test module, so anything that sits there is asserted by
/// nothing at any layer. [`answer_args`] is the testable half.
#[derive(clap::Subcommand)]
pub enum AgentAction {
    /// Answer a human-backed `Agent` node on behalf of a person
    #[command(group(
        clap::ArgGroup::new("answer_src").required(true).multiple(false)
    ))]
    Answer {
        run_id: String,
        /// The waiting node's id — `torii run list-paused` names every node that is waiting
        /// and shows the question each one asked, in its `agent:` cell.
        //
        // This help once promised the question while the listing did not render it. It does
        // now (`render::AwaitingNode::question`, folded by `cmd::run::awaiting_nodes` from
        // `AgentAwaited`), so the promise is accurate. See `crate::main`'s `Agent` variant
        // for the whole history and for the guard — `cli.rs`'s
        // `agent_help_names_the_question_list_paused_now_shows`, which asserts on BOTH help
        // surfaces because an operator reads whichever one they reached.
        #[arg(long)]
        node: String,
        /// The answer, as free text. It becomes this node's OUTPUT under the `text` key —
        /// exactly where a model-backed agent's answer would go — so it flows into every
        /// downstream node and model prompt for the life of the run. Max 4096 bytes as
        /// stored; redaction replaces secret-shaped text with the longer literal
        /// `[REDACTED]`, so an answer can cross the limit on the way to the journal.
        ///
        /// This is argv: it is visible to `ps`, to your shell history and to a CI job's
        /// command echo, none of which redaction can reach. For anything you would rather
        /// not have on a command line, use --text-file.
        #[arg(long, group = "answer_src")]
        text: Option<String>,
        /// Read the answer from a file instead of the command line.
        ///
        /// The same text and the same cap — but the value never becomes an argv entry, so
        /// it cannot be read out of `ps`, `/proc/<pid>/cmdline`, a shell history file or a
        /// CI job's command echo. `DATABASE_URL` is environment-only for this same reason.
        #[arg(long, group = "answer_src", value_name = "PATH")]
        text_file: Option<std::path::PathBuf>,
        // `hide_default_value`: the clap default is the empty string, but the EFFECTIVE
        // default is $USER (`cmd::gate::actor_or_user` resolves it), so rendering
        // `[default: ""]` next to a sentence that says "defaulting to $USER" contradicts
        // it on the one surface the trust boundary is stated.
        #[arg(long, default_value = "", hide_default_value = true, help = ACTOR_HELP)]
        r#as: String,
    },
}

/// The `--as` help.
///
/// Deliberately NOT shared with `cmd::gate`'s `ACTOR_HELP`, even though the load-bearing
/// second sentence is identical: the consequence differs, and the stronger statement
/// belongs on the stronger case. A gate's actor is an AUDIT TRAIL; this one is folded into
/// the node's OUTPUT (`{"text","actor"}`, `JournalEvent::AgentAnswered`) and carried into
/// every downstream model prompt, so an operator who reads it as authenticated would be
/// branching real work on an unverified string.
///
/// The duplication is guarded rather than trusted: `cli.rs`'s
/// `agent_answer_help_says_attribution_is_not_authentication` and
/// `gate_help_says_attribution_is_not_authentication` each assert the sentence on their own
/// command's two help surfaces, so losing it from either is a red test rather than a silent
/// downgrade.
const ACTOR_HELP: &str = "Who answered. ATTRIBUTION, NOT AUTHENTICATION: it is whatever \
                          string you supply (defaulting to $USER), so it records who \
                          CLAIMED to answer. Anyone who can reach the database can write \
                          any actor — and unlike a gate's, this one becomes part of the \
                          node's output.";

/// [`AgentAction::Answer`]'s arguments, sourced and flattened — still unparsed and
/// unresolved.
///
/// NAMED fields, not a tuple: every one is a `String`, so a shuffled destructuring would
/// compile silently. Same reasoning as [`crate::cmd::gate::Decision`].
pub struct AnswerArgs {
    pub run_id: String,
    pub node: String,
    /// From `--text` or from the file `--text-file` named — the two are one value by the
    /// time anything downstream sees them, so no code below this point can behave
    /// differently depending on which the operator used.
    pub text: String,
    /// Still RAW — [`crate::cmd::gate::actor_or_user`] resolves the `$USER` fallback at the
    /// call site, so there is exactly ONE definition of "who answered" across both
    /// human-facing verbs.
    pub actor: String,
}

/// Resolve `--text` / `--text-file` into the one shape [`answer`] takes.
///
/// In the LIBRARY rather than in `dispatch` so the file path can be tested: the binary has
/// no test module, and the file read is the half with a failure mode (a typo'd path) and a
/// disclosure rule (the PATH is echoed, because an operator typo is the whole point of the
/// message; nothing READ from the file ever is — that is the value which might be a
/// credential).
///
/// The `(None, None)` arm is unreachable through the binary: clap's required `answer_src`
/// group makes it a parse error. It is still an explicit refusal rather than an empty
/// string, because a silent empty answer would be journaled as this node's OUTPUT.
pub fn answer_args(action: AgentAction) -> Result<AnswerArgs, CliError> {
    let AgentAction::Answer {
        run_id,
        node,
        text,
        text_file,
        r#as,
    } = action;
    let text = match (text, text_file) {
        (Some(inline), _) => inline,
        (None, Some(path)) => std::fs::read_to_string(&path).map_err(|e| {
            CliError::error(format!(
                "--text-file {}: {e}. The file holds the answer as plain text.",
                path.display()
            ))
        })?,
        (None, None) => {
            return Err(CliError::error(
                "an answer needs either --text or --text-file".to_string(),
            ));
        }
    };
    Ok(AnswerArgs {
        run_id,
        node,
        text,
        actor: r#as,
    })
}

/// The QUESTION a human-backed `Agent` published, folded from `AgentAwaited`. FIRST wins,
/// matching the executor's fold — two copies of one rule, so they must not drift.
///
/// `None` = this node never asked, which is what distinguishes a human-backed `Agent` from
/// an `AwaitSignal` or a `HumanGate` **without loading the graph**. That is not a
/// simplification, it is the only design available: `SchedulerStore::status()` returns a DTO
/// whose own doc says "the observe DTO (NOT the graph)", no trait method exposes a
/// submitted run's graph for read-back (only `enqueue`/`claim_due` touch `Graph` at all),
/// "is this agent human-backed" is a REGISTRY question `cmd::run`/`cmd::gate`/this module
/// have no `Registry` or `ConfigSource` to answer, and a path-qualified id (`{map}/3`,
/// `{loop}/2/__gate__`) has no `NodeKind` in the graph in the first place. Folding the
/// journal needs none of that and handles path-qualified ids for free.
///
/// `pub(crate)` because both siblings read it for their half of the three-way cross-refusal:
/// a `Some` here means a raw payload or a named option must be refused.
pub(crate) fn agent_question(events: &[(Seq, JournalEvent)], node: &NodeId) -> Option<String> {
    events.iter().find_map(|(_, e)| match e {
        JournalEvent::AgentAwaited {
            node: n, prompt, ..
        } if n == node => Some(prompt.clone()),
        _ => None,
    })
}

/// Scrub one human-supplied string with the SP-4 s2 redactor, before it is journaled.
///
/// The non-string arm is fail-CLOSED, exactly as `cmd::gate::decide`'s is and unlike the
/// executor's `redact_text` (which keeps the original message).
/// `Redactor::redact(&Value) -> Value` promises nothing about preserving the variant, and
/// only `PatternRedactor` happens to map a string to a string; a third-party impl that did
/// not would leak here. The executor keeps the original because discarding it loses the
/// only record of why a run failed. This path has no such stake: an answer is re-typeable
/// while the run is still paused, so losing it costs a retype and leaking it is permanent.
fn redact_answer(text: &str) -> String {
    render::redact_payload(&serde_json::json!(text))
        .as_str()
        .unwrap_or("[REDACTED]")
        .to_string()
}

/// Bound one durable human-text field, naming BOTH the limit and the actual size — an
/// operator who pasted a file needs to know how much to cut, not just that they were over.
///
/// **[`MAX_HUMAN_TEXT_BYTES`], not `cmd::run::MAX_PAYLOAD_BYTES`, and not through
/// `check_payload_size`.** The two constants are both 4096 today, so nothing behaves
/// differently — what differs is which one a future change moves, and this field must move
/// with the executor's. `Executor::run_human_agent` bounds the journaled QUESTION by
/// `MAX_HUMAN_TEXT_BYTES` on `prompt.len()`; the question and the answer are the two halves
/// of one exchange and must not be bounded by two numbers that can drift apart.
/// `check_payload_size` also measures a `serde_json::Value` and charges
/// `jsonb_number_expansion`, which is meaningless for a bare `String`: `jsonb` normalises
/// NUMBERS to `numeric`, and a string has none, so byte length as given is exactly the
/// durable size.
///
/// The value itself is NEVER echoed, only its size: an answer is the one input an operator
/// might paste a credential into, and stderr reaches journald and CI logs.
fn check_human_text_size(text: &str, measured: Measured, what: &str) -> Result<(), String> {
    let size = text.len();
    if size <= MAX_HUMAN_TEXT_BYTES {
        return Ok(());
    }
    // Naming WHY the size differs from what was sent matters: an operator who typed 4030
    // bytes and is told "5580 bytes" with no explanation will assume the tool is broken.
    let growth = match measured {
        Measured::AfterRedaction => {
            " once redacted (secret-shaped text is replaced by the longer literal \
             `[REDACTED]` before the row is written), so the durable row is bigger than \
             what you sent"
        }
        Measured::AsGiven => "",
    };
    Err(format!(
        "{what} is {size} bytes{growth}, over the {MAX_HUMAN_TEXT_BYTES}-byte limit. This \
         is a human ANSWER, not a data channel — it is journaled durably, folded as the \
         node's output on every resume, and carried in every downstream prompt for the life \
         of the run. Put the bulk somewhere the graph can read (a workspace file, the \
         blackboard) and reference it here."
    ))
}

/// Deliver a human's answer to a human-backed `Agent` node (SP-6 s3).
///
/// **Validation is JOURNAL-ONLY.** A journaled `AgentAwaited` for this node is the whole
/// evidence that a human was asked anything; absent, this refuses. The graph is not read
/// and the registry is not consulted — see [`agent_question`] for why neither is available
/// and why folding is not merely the cheaper option but the only one that works. This is
/// exactly what `cmd::gate::gate_menu` and `cmd::run::signal_state` already do; s2's spec
/// claimed `gate decide` "reads the graph from `scheduled_runs`", which was never true of
/// the implementation.
///
/// **This check is advisory and the executor re-checks.** It is non-atomic — it reads the
/// journal, then appends — and the library entry point bypasses nothing but the CLI's own
/// argument sourcing. It exists to refuse cheaply and to keep a bad row out of the journal,
/// not to be the authority.
///
/// **The NODE's state is checked, not just the RUN's.** `Scheduler::record` matches
/// `Ok(o) if o.paused.is_some()` BEFORE `Ok(o) if o.failed.is_some()`, so a drive that
/// fails one node while another is still waiting records the run **paused** — "the run is
/// paused" therefore proves nothing about this node. This is the guard s2's review found
/// missing on `gate decide`, applied here from the start rather than after the same
/// incident.
///
/// **The deadline is deliberately NOT checked, and that is the one place this must not copy
/// [`crate::cmd::gate::decide`].** `decide` refuses a decision past the gate's recorded
/// deadline because `run_human_gate` takes `WaitState::Expired` BEFORE it reads any
/// decision, so a late decision could only make the next tick terminally fail the run.
/// `run_human_agent` inverts that order on purpose (its doc comment argues it at length): an
/// agent's answer is WORK PRODUCT, not an approval, so there is nothing to self-approve, and
/// discarding a human's answer because a worker was down punishes them for infrastructure
/// they had no part in. A deadline arm hand-copied from `decide` would therefore refuse
/// answers the executor would have honoured — the refusal that matters is the node being
/// TERMINAL, which the `signal_state` arm above already gives.
/// `an_answer_after_the_deadline_is_still_delivered` is the guard.
///
/// **It queues the wake as well as journaling the answer, and must.** A human-backed agent
/// pauses with `resume_after` = its deadline, or `None` for the indefinite class — so the
/// run is either due only at a future instant or never due at all, and in both cases the
/// next worker tick would not claim it.
///
/// **Order: append, THEN `force_wake`** — never the reverse, for the reason both siblings
/// record: `force_wake` only flips `next_wake`, and a worker in another process can claim
/// that wake the instant it lands. Appending first guarantees any worker that can observe
/// the wake folds a journal that already contains the answer.
///
/// **The answer is redacted before it is size-checked and journaled.** An answer is not a
/// credential channel; the credential broker is.
pub async fn answer(
    store: &dyn SchedulerStore,
    journal: &dyn ExecutionJournal,
    run: RunId,
    node: NodeId,
    text: &str,
    actor: &str,
    now: DateTime<Utc>,
) -> Result<Outcome, CliError> {
    // ---- Pure, before any I/O ---------------------------------------------------------
    // An over-limit or empty answer can never reach the journal, whichever caller got here
    // and whichever of `--text`/`--text-file` sourced it, and neither costs a round trip.
    //
    // TRIMMED here, in the library, rather than in `dispatch`: a file written by `$EDITOR`
    // always ends with a newline, so trimming in only one of the two sourcing paths would
    // make the same answer journal different bytes depending on how it was supplied. An
    // answer is prose; surrounding whitespace carries no meaning in it.
    let text = text.trim();
    // clap's `required` cannot see that `--text ''` is the same omission with quotes around
    // it, so this is the check that actually holds — the same gap `gate reject --reason ''`
    // has. It matters more here: a gate's blank reason is a missing explanation, whereas a
    // blank answer becomes this node's OUTPUT and flows into every downstream model prompt
    // as a silent non-answer.
    if text.is_empty() {
        return Ok(Outcome::precondition(
            "not delivered: the answer is empty. It becomes this node's output and flows \
             into every downstream prompt, so there is nothing useful to journal."
                .to_string(),
        ));
    }
    // Redact BEFORE the size check, and with the same pure pass the executor applies on the
    // fold-read, so live == journaled == replayed. `[REDACTED]` is LONGER than the shortest
    // span it replaces, so redaction can GROW the answer past the cap: checking the raw text
    // would bound a value nobody stores. That exact ordering shipped wrong twice in this
    // feature — s1 capped pre-redaction while writing post-redaction, and s2 repeated the
    // shape — which is why it is a test (`an_answer_that_only_exceeds_the_cap_after_
    // redaction_is_rejected`) and not a comment.
    //
    // Double-scrubbing is a non-issue: `[REDACTED]` matches no credential shape, so the pass
    // is idempotent.
    //
    // A hard error (exit 1), matching both siblings: exit 2 in this taxonomy means "ran
    // fine, nothing to do", and an over-limit answer is invalid INPUT.
    let text = redact_answer(text);
    check_human_text_size(&text, Measured::AfterRedaction, "the answer (--text)")
        .map_err(CliError::error)?;

    // Collapsed on the way IN, not just on the way out — unlike the node id, which is
    // journaled as given. `actor` is folded into the node's OUTPUT, so an escape sequence
    // smuggled through `--as` would be re-rendered by every reader of this run's output and
    // carried into every downstream model prompt. Guarded by
    // `a_hostile_actor_cannot_forge_a_line_or_move_the_cursor`, on the JOURNALED row rather
    // than on stdout: the actor is never echoed in the outcome text, so a stdout assertion
    // would pass while the durable row still carried the newline and the escape. It is a
    // test because review mutated this call to `actor.to_string()` and the whole crate —
    // lib, cli and e2e — stayed green.
    //
    // The SIBLING field on the same durable row, held to the same bound AND the same
    // redaction as the answer beside it. `--as` was bounded by nothing on `gate decide`
    // while `--note` was capped, and `ARG_MAX` permits a ~131 KB actor.
    //
    // **Redacted, and redacted BEFORE the size check.** The whole-slice review found this
    // field reaching `journal_events` in plaintext while `text` on the same row was
    // scrubbed — against design §6, which lists the actor by name among the strings that go
    // "through the redactor before the durable write". `--as` is exactly the field an
    // operator scripts (`--as "$CI_TOKEN_OWNER"`), and the executor's fold-read redactor is
    // no backstop: `Executor::with_redactor` is opt-in and defaults to `None`, so an
    // embedder without it carries a plaintext actor into the node's output, the blackboard
    // and every downstream prompt — and the durable jsonb keeps it either way.
    //
    // The order is the answer's, for the answer's reason: `[REDACTED]` is longer than the
    // shortest span it replaces, so redaction can GROW the value past the cap, and checking
    // the raw text would bound a value nobody stores. `Measured::AfterRedaction` moves with
    // it, so an operator told "5580 bytes" for a 4030-byte actor gets the explanation.
    let actor = redact_answer(&render::one_line(actor));
    check_human_text_size(
        &actor,
        Measured::AfterRedaction,
        "the answer's actor (--as)",
    )
    .map_err(CliError::error)?;

    // A node id is operator-supplied free text and every message below echoes it back to a
    // terminal, so control characters are collapsed for DISPLAY — a raw newline or an ANSI
    // escape in the echoed id would let the reported outcome forge extra lines or rewrite
    // what is already on screen. Same reasoning, same helper, as both siblings.
    let shown = render::one_line(&node.0);

    let Some(before) = store.status(run).await? else {
        return Ok(Outcome::precondition(format!("no such run: {}", run.0)));
    };
    let events = journal
        .load(run)
        .await
        .map_err(OrchestratorError::Journal)?;

    // The question comes from the JOURNAL. Absent ⇒ this node has not asked yet, or is one
    // of the other two waiting kinds — and each of those gets its own refusal naming the
    // verb that WOULD work, because a refusal that only says "wrong kind" sends an operator
    // to check a node id that was right when the COMMAND was wrong.
    if agent_question(&events, &node).is_none() {
        return Ok(Outcome::precondition(
            if gate_menu(&events, &node).is_some() {
                format!(
                    "not delivered: {shown} is a HumanGate, not a human-backed Agent — it takes \
                 one of the options it published, not free text. Use: torii run gate decide \
                 {} --node {shown} --option <name>",
                    run.0
                )
            } else if awaiting_signal(&events, &node) {
                format!(
                    "not delivered: {shown} is an AwaitSignal, not a human-backed Agent — it \
                 takes arbitrary JSON, and it records no actor. Use: torii run signal {} \
                 --node {shown} --payload '<json>'",
                    run.0
                )
            } else {
                format!(
                    "not delivered: {shown} is not awaiting a human answer. \
                 `torii run list-paused` names the nodes that are."
                )
            },
        ));
    }

    // The NODE's own state, folded from the journal. The fold is `cmd::run`'s, not a second
    // one — its `AgentAwaited` arm is what puts a human-backed agent in the awaited set at
    // all — and the refusal text is `signal`'s, so the three commands cannot drift on a
    // condition they share exactly. That text says "signal" where an answer would say
    // "answer"; that is the price of one source of truth, and the sentence that matters ("a
    // terminal node never re-executes") is exactly the same fact in all three.
    //
    // `NotAwaiting` is unreachable from here — a journaled `AgentAwaited` is what produced
    // the question above, and that is precisely what puts the node in the fold — but it is
    // matched by the same catch-all rather than special-cased, because it must never be able
    // to report success.
    match signal_state(&events, &node) {
        SignalState::Awaiting { .. } => {}
        other => return Ok(Outcome::precondition(not_delivered(&shown, &other))),
    }

    if before.status != RunStatus::Paused {
        // A `waking` row means a worker holds the lease and is folding this journal right
        // now; a terminal row means nothing will ever read the answer. Neither is a state to
        // write into — but they call for OPPOSITE advice, and `signal` shipped one message
        // for both, which handed an operator of a cancelled run "retry once it shows
        // paused": advice to wait forever, since no shipped store moves a terminal row back
        // to `paused`.
        return Ok(Outcome::precondition(
            if before.status == RunStatus::Waking {
                format!(
                    "not delivered: {shown} is awaiting an answer, but the run is waking — a \
                     worker holds the lease and is folding this journal right now. Retry \
                     once `torii run status {}` shows it paused.",
                    run.0
                )
            } else {
                format!(
                    "not delivered: {shown} is awaiting an answer, but the run is {} — a {} \
                     run is never paused again, so nothing will ever read an answer \
                     delivered to it. Start a new run.",
                    before.status.as_str(),
                    before.status.as_str()
                )
            },
        ));
    }

    // The appended seq is KEPT, not discarded: it is the only thing that can order our row
    // against a terminal marker the post-check may find, and it names the durable row in the
    // post-append fault report below.
    let appended = journal
        .append(
            run,
            JournalEvent::AgentAnswered {
                node: node.clone(),
                // Already redacted and bounded, above — the value checked and the value
                // written are the same bytes.
                text,
                actor,
            },
        )
        .await
        .map_err(OrchestratorError::Journal)?;

    // Past here the answer is DURABLE. Every remaining call reports rather than `?`s — a
    // bare store error reads as "it did not go through" for a write that succeeded, and for
    // an indefinite role (`next_wake` NULL, never auto-woken) the run would then wait forever
    // on an answer nobody knows landed. Identical in shape and in reason to both siblings'
    // `unread` closures.
    //
    // The error goes through `render::safe_reason` — redact, collapse control characters,
    // cap — because a backend fault is FREE TEXT FROM THE DRIVER, not a message this crate
    // wrote. `PostgresJournal::load` builds it from both `sqlx::Error` (a pool timeout
    // carries the whole connection string, password included) and `serde_json::Error` (over
    // a TYPED `JournalEvent`, which quotes the offending row). Interpolated raw it also let a
    // newline forge a line beginning with a pastable uuid and an ANSI escape rewrite what was
    // already on screen.
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
    let after = match store.status(run).await {
        Ok(Some(s)) => s,
        Ok(None) => return Ok(unread("the status re-read", &"the run vanished mid-answer")),
        Err(e) => return Ok(unread("the status re-read", &e)),
    };

    // The JOURNAL is re-read too, not just the scheduler row — and that is the whole point.
    // The scheduler row is RUN-level: it says the run is no longer paused, never whether
    // THIS node read the answer. Reporting off the row alone inverted both siblings on their
    // most successful path: a worker that claims the run the instant the answer lands folds
    // it, completes the node and drives the run to completion — the delivery worked
    // perfectly — and the report said `not queued`, exit 2, advising `torii run wake`, which
    // refuses every non-paused run.
    let after_events = match journal.load(run).await {
        Ok(evs) => evs,
        Err(e) => return Ok(unread("the journal re-read", &e)),
    };
    let SignalStateAt { state, at } = signal_state_at(&after_events, &node);
    if let Some(at) = at {
        return Ok(match (&state, at > appended) {
            // Terminated AFTER our row, by completing: the answer was on the journal for the
            // fold that completed it. A SUCCESS — the only difference from the ordinary path
            // is that the run is already moving, so there is no tick to wait for.
            //
            // The wording claims the ORDERING (proven) and not authorship of the completion
            // (not proven): a duplicate answer already on the journal is last-wins, so which
            // one the drive folded is not observable from here.
            (SignalState::Completed, true) => Outcome::ok(format!(
                "answered: {shown} (a drive already in flight completed the node after the \
                 answer landed, so the run is moving without waiting for a tick)"
            )),
            // Terminated AFTER our row, but NOT by completing — an expired deadline, or a
            // cascade skip. That drive had loaded the journal before our row landed, so it
            // never saw the answer.
            //
            // **`gate decide` needs a fourth arm here and this does not**, which is worth
            // stating because the shapes are otherwise identical. A `GateOutcome::Fail`
            // option makes a `NodeFailed` the CORRECT, requested outcome of a decision that
            // WAS read, so `decide` must tell an honoured rejection from an expiry by
            // matching the journaled message. An answer can never fail a node: every
            // `NodeFailed` arm in `run_human_agent` (`fail_human_agent`) is an overflowing
            // timeout, an oversized prompt, a non-top-level refusal, or an expiry with no
            // answer — none of them caused by an answer being read. And step 3 of that
            // function returns `Completed` before the deadline is acted on at all, so a
            // drive that folded our row could not have expired the node. `run_await_signal`
            // has the same premise, which is why this arm is `signal`'s unchanged.
            (other, true) => Outcome::precondition(format!(
                "not read: {shown}'s answer is journaled durably, but {shown} is {} — it \
                 terminated while this delivery was in flight, and a drive that had already \
                 loaded the journal would not have seen it.",
                other.as_str()
            )),
            // Terminated BEFORE our row landed: a true orphan. The pre-check refuses this
            // shape, so reaching it means the node died inside the write window — worth
            // saying plainly, because the residue is durable: a human-backed agent journals
            // no `NodeCompleted` and `NodeFailed` is not folded as a barrier, so a re-`start`
            // would re-execute the node and fold this late answer as its output.
            (other, false) => Outcome::precondition(format!(
                "not read: {shown}'s answer is journaled durably, but {shown} was already {} \
                 before the write landed, so nothing read it. The answer stays on the journal \
                 as a last-wins value that a re-`start` of this run would fold as the node's \
                 output — do not treat this node as answered.",
                other.as_str()
            )),
        });
    }

    // `at: None` ⇒ nothing has terminated the node, so the answer is still there to be read
    // and the only remaining question is the WAKE. (`NotAwaiting` also folds to `None`, but
    // is unreachable: the pre-check read an `AgentAwaited` and the journal is append-only, so
    // this later read is a superset of that one.)
    //
    // The effect actually achieved, read back rather than assumed. STATUS plus the pinned
    // timestamp, exactly as `wake`, `signal` and `decide` check their own: `claim_due` leaves
    // a stale `next_wake` untouched and an unrelated re-pause can restore `paused` inside the
    // race window, so neither field alone proves OUR wake applied. The 2µs tolerance is a
    // `timestamptz` rounding allowance, not a clock-skew fudge.
    let queued = after.status == RunStatus::Paused
        && after.next_wake.is_some_and(|t| {
            let drift = if t >= now { t - now } else { now - t };
            drift <= chrono::Duration::microseconds(2)
        });
    Ok(if queued {
        // Says QUEUED, never RESUMED: `force_wake` only sets `next_wake`; a worker tick does
        // the driving.
        Outcome::ok(format!(
            "answered: {shown} (the run will resume on the next worker tick)"
        ))
    } else if after.status.is_terminal() {
        // The run is over while the node itself never terminated — `cancel`/`record_terminal`
        // journal no node event, so this is reachable. "Retry once it is paused again" would
        // be advice to wait forever, the same dead end the pre-check arm above avoids; the
        // post-append arm must not reintroduce it.
        Outcome::precondition(format!(
            "not read: {shown}'s answer is journaled durably, but the run is {} — a {} run is \
             never paused again, so nothing will ever read it. Start a new run.",
            after.status.as_str(),
            after.status.as_str()
        ))
    } else {
        Outcome::precondition(format!(
            "not queued: {shown}'s answer is journaled durably, but the run is {} and the \
             wake did not apply — the drive that claimed it may have folded the journal \
             before the answer landed. Run `torii run wake {}` once it is paused again.",
            after.status.as_str(),
            run.0
        ))
    })
}

/// `pub(crate)` for the same reason `cmd::gate::tests` is: the three-way cross-refusal
/// matrix is proven here, in one place, rather than split across three modules that could
/// each drift.
#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use crate::cmd::gate::tests::gate_journal;
    use crate::cmd::run::tests::{FailingForceWakeStore, awaiting_journal, now, paused_store};
    use crate::errors::{EXIT_OK, EXIT_PRECONDITION};
    use orchestrator_core::{
        ExecutionJournal, Graph, JournalEvent, MAX_HUMAN_TEXT_BYTES, NodeId, RunId, RunStatus,
        SchedulerStore,
    };
    use orchestrator_store::{InMemoryJournal, InMemorySchedulerStore};

    /// The node id every test here uses: a `review`-area role answered by a person.
    ///
    /// `pub(crate)` alongside [`agent_journal`], for the reason `cmd::gate::tests::release`
    /// is: `cmd::run`'s `list-paused` tests need the SAME fixture this module answers
    /// against, and a second hand-rolled `AgentAwaited` would be a second place for the
    /// shape `signal_states` folds to drift from the one the executor writes.
    pub(crate) fn reviewer() -> NodeId {
        NodeId("reviewer".into())
    }

    /// The question [`agent_journal`] asks. Named rather than inlined because
    /// `cmd::run`'s listing tests assert on this exact text — the row must show what the
    /// journal recorded, and a second literal is a second thing to forget to change.
    pub(crate) const THE_QUESTION: &str = "Does this release look safe to ship?";

    /// A journal in which `node` has already ASKED — the state
    /// `Executor::run_human_agent` leaves behind on its first execution (`AgentAwaited`
    /// with the assembled prompt, then the durable pause).
    pub(crate) async fn agent_journal(
        run: RunId,
        node: &NodeId,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
    ) -> InMemoryJournal {
        agent_journal_asking(run, node, deadline, THE_QUESTION).await
    }

    /// [`agent_journal`] with the question spelled out, for the tests whose subject IS the
    /// question: `cmd::run`'s listing renders it into a line-oriented table, so it needs a
    /// hostile one (control characters), an overlong one and a secret-shaped one. Those go
    /// through the SAME `AgentAwaited` shape the executor writes rather than a hand-rolled
    /// event, for the reason [`reviewer`] records.
    pub(crate) async fn agent_journal_asking(
        run: RunId,
        node: &NodeId,
        deadline: Option<chrono::DateTime<chrono::Utc>>,
        prompt: &str,
    ) -> InMemoryJournal {
        let j = InMemoryJournal::new();
        j.append(
            run,
            JournalEvent::AgentAwaited {
                node: node.clone(),
                deadline,
                prompt: prompt.to_string(),
            },
        )
        .await
        .unwrap();
        j.append(
            run,
            JournalEvent::RunPaused {
                reason: format!("human_agent: waiting for a human answer on node {}", node.0),
                resume_after: deadline,
            },
        )
        .await
        .unwrap();
        j
    }

    /// Every `AgentAnswered` journaled for `node`, as `(text, actor)`.
    async fn journaled_answers(
        j: &InMemoryJournal,
        run: RunId,
        node: &NodeId,
    ) -> Vec<(String, String)> {
        j.load(run)
            .await
            .unwrap()
            .into_iter()
            .filter_map(|(_, e)| match e {
                JournalEvent::AgentAnswered {
                    node: n,
                    text,
                    actor,
                } if &n == node => Some((text, actor)),
                _ => None,
            })
            .collect()
    }

    /// The positive guard. Without it every refusal test below passes vacuously — a
    /// command that refused unconditionally would satisfy all of them.
    #[tokio::test]
    async fn a_legitimate_answer_is_journaled_and_queues_the_wake() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &reviewer(), None).await;

        let out = answer(&s, &j, run, reviewer(), "ship it", "alice", now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_OK, "{}", out.text);
        let answers = journaled_answers(&j, run, &reviewer()).await;
        assert_eq!(answers.len(), 1);
        assert_eq!(answers[0].0, "ship it", "the text");
        assert_eq!(answers[0].1, "alice", "the actor");
        assert!(
            s.status(run).await.unwrap().unwrap().next_wake.is_some(),
            "a human-backed agent pauses with resume_after = its deadline, or NULL for the \
             indefinite class — journaling the answer without queuing the wake would leave \
             the run sitting there while this command claimed it would resume"
        );
    }

    /// Validation is JOURNAL-ONLY: an `AgentAwaited` for this node is the entire evidence
    /// that a human was asked anything. No `AgentAwaited` ⇒ nothing to answer, and nothing
    /// durable may be written — a stray `AgentAnswered` is a last-wins value that a
    /// re-`start` of the run would fold as the node's output.
    #[tokio::test]
    async fn an_answer_to_a_node_that_never_asked_is_refused() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = InMemoryJournal::new();

        let out = answer(&s, &j, run, reviewer(), "ship it", "alice", now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("list-paused"),
            "must point at the command that names what IS waiting, rather than leaving the \
             operator to guess node ids: {}",
            out.text
        );
        assert!(
            journaled_answers(&j, run, &reviewer()).await.is_empty(),
            "a refused answer must leave NOTHING durable"
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            None,
            "and must not queue a wake"
        );
    }

    /// The guard s2's review found missing on `gate decide`, applied here from the start.
    ///
    /// The RUN's status is not the NODE's: `Scheduler::record` matches
    /// `Ok(o) if o.paused.is_some()` BEFORE `Ok(o) if o.failed.is_some()`, so a drive that
    /// fails one node while another is still waiting records the run **paused**. A
    /// human-backed agent whose deadline already fired therefore passes a run-level check,
    /// while `run_human_agent`'s `gate_precheck_by_id` reads the folded `NodeFailed` back
    /// before any answer is looked at. Reporting exit 0 here tells an operator their answer
    /// landed when nothing will ever read it.
    #[tokio::test]
    async fn an_answer_to_a_terminally_failed_node_is_refused() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &reviewer(), None).await;
        j.append(
            run,
            JournalEvent::NodeFailed {
                node: reviewer(),
                error: "human_agent: node reviewer passed its deadline \
                        2026-08-27T00:00:00Z with no answer"
                    .into(),
            },
        )
        .await
        .unwrap();

        let out = answer(&s, &j, run, reviewer(), "ship it", "alice", now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("failed"),
            "must name the state the node is actually in: {}",
            out.text
        );
        assert!(
            out.text.contains("never re-executes"),
            "must say WHY it can never be read, in `run signal`'s words: {}",
            out.text
        );
        assert!(
            journaled_answers(&j, run, &reviewer()).await.is_empty(),
            "nothing may be written for a node that is already terminal"
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            None,
            "and the run must not be woken for an answer nothing will read"
        );
    }

    /// AC3's CLI half, and the one place this command must NOT copy `gate decide`.
    ///
    /// `decide` refuses a decision after the gate's deadline because `run_human_gate` takes
    /// `WaitState::Expired` BEFORE it reads any decision — a late decision cannot approve an
    /// expired gate, so journaling it would only make the next tick terminally fail the run.
    /// `run_human_agent` deliberately inverts that order: it reads the answer FIRST, because
    /// an agent's answer is WORK PRODUCT, not an approval, and discarding a human's answer
    /// because a worker was down punishes them for infrastructure they had no part in.
    ///
    /// So the deadline being behind `now` does NOT make the answer unreadable here, and a
    /// deadline arm hand-copied from `decide` would silently throw away answers the executor
    /// would have honoured. The node being TERMINAL is the real refusal, and the test above
    /// covers it.
    #[tokio::test]
    async fn an_answer_after_the_deadline_is_still_delivered() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &reviewer(), Some(now() - chrono::Duration::hours(1))).await;

        let out = answer(&s, &j, run, reviewer(), "ship it", "alice", now())
            .await
            .expect("no hard error");

        assert_eq!(
            out.code, EXIT_OK,
            "run_human_agent reads the answer BEFORE it acts on the deadline, so a late \
             answer on a node that has not yet failed is still read: {}",
            out.text
        );
        assert_eq!(journaled_answers(&j, run, &reviewer()).await.len(), 1);
    }

    /// AC10, the `--text` half. §6: the bound is `MAX_HUMAN_TEXT_BYTES`, the SAME constant
    /// the executor applies to the journaled QUESTION — the two halves of one exchange must
    /// not be bounded by two numbers that can drift.
    ///
    /// Exit 1, matching both siblings: exit 2 in this taxonomy means "ran fine, nothing to
    /// do", and an over-limit answer is invalid INPUT.
    #[tokio::test]
    async fn an_oversized_answer_is_rejected_before_anything_is_journaled() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &reviewer(), None).await;
        let huge = "x".repeat(MAX_HUMAN_TEXT_BYTES + 1000);

        let e = answer(&s, &j, run, reviewer(), &huge, "alice", now())
            .await
            .expect_err("an over-limit answer is refused");

        assert_eq!(e.code, crate::errors::EXIT_ERROR, "{}", e.message);
        assert!(
            e.message.contains(&MAX_HUMAN_TEXT_BYTES.to_string()),
            "must name the limit: {}",
            e.message
        );
        assert!(
            !e.message.contains(&huge),
            "and must never echo the answer back — stderr reaches journald and CI logs: {}",
            e.message
        );
        assert!(
            e.message.contains("--text"),
            "must name the input the operator actually supplied: {}",
            e.message
        );
        assert!(
            journaled_answers(&j, run, &reviewer()).await.is_empty(),
            "an over-limit answer must never reach the journal"
        );
        assert_eq!(
            s.status(run).await.unwrap().unwrap().next_wake,
            None,
            "and a refused answer must not queue a wake"
        );
    }

    /// The cap is measured on the REDACTED text — the bytes actually written — not on what
    /// the operator typed. `[REDACTED]` is 10 bytes and the assignment rule's shortest
    /// matched value is 6, so redaction GROWS a `token:…` run by roughly 1.38x; a check
    /// placed before the scrub bounds a value nobody stores.
    ///
    /// Stated as a test rather than a comment because that exact ordering shipped wrong
    /// twice in this feature: s1 capped pre-redaction while writing post-redaction, and s2
    /// repeated the shape. The pair is assembled at runtime — the repo's Semgrep CWE-798
    /// hook blocks a credential-shaped literal in a fixture.
    #[tokio::test]
    async fn an_answer_that_only_exceeds_the_cap_after_redaction_is_rejected() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &reviewer(), None).await;

        // The trailing space is load-bearing: without a separator the value class runs to
        // the end of the string and the whole run collapses into ONE placeholder, which
        // shrinks rather than grows.
        let unit = format!("{}:{} ", "token", "abcdef");
        let raw = unit.repeat(310);
        // `answer` trims before it redacts (a file from `$EDITOR` ends with a newline), so
        // the expectation is computed on the trimmed value — otherwise this asserts a size
        // one byte off the one actually reported, which is how it first failed.
        let journaled = redact_answer(raw.trim());
        assert!(
            raw.len() <= MAX_HUMAN_TEXT_BYTES,
            "precondition: this answer is under the cap as typed ({} bytes)",
            raw.len()
        );
        assert!(
            journaled.len() > MAX_HUMAN_TEXT_BYTES,
            "precondition: redaction GROWS it past the cap ({} -> {} bytes)",
            raw.len(),
            journaled.len()
        );

        let e = answer(&s, &j, run, reviewer(), &raw, "alice", now())
            .await
            .expect_err("an answer that would exceed the cap once redacted is refused");

        assert_eq!(e.code, crate::errors::EXIT_ERROR, "{}", e.message);
        assert!(
            e.message.contains(&journaled.len().to_string()),
            "must name the size that would actually be JOURNALED ({}), not the one the \
             operator typed: {}",
            journaled.len(),
            e.message
        );
        assert!(
            journaled_answers(&j, run, &reviewer()).await.is_empty(),
            "an over-limit row must never reach the journal"
        );
    }

    /// The SIBLING field on the same durable row, held to the same bound. `--as` is not
    /// merely displayed here and it is not merely an audit trail either: the executor folds
    /// it into the node's OUTPUT (`{"text","actor"}`), so it flows into every downstream
    /// node and model prompt for the life of the run. `ARG_MAX` permits a ~131 KB actor.
    #[tokio::test]
    async fn an_oversized_actor_is_rejected_before_anything_is_journaled() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &reviewer(), None).await;
        let huge = "a".repeat(MAX_HUMAN_TEXT_BYTES + 1000);

        let e = answer(&s, &j, run, reviewer(), "ship it", &huge, now())
            .await
            .expect_err("an over-limit actor is refused");

        assert_eq!(e.code, crate::errors::EXIT_ERROR, "{}", e.message);
        assert!(
            e.message.contains(&MAX_HUMAN_TEXT_BYTES.to_string()),
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
        assert!(
            journaled_answers(&j, run, &reviewer()).await.is_empty(),
            "an over-limit actor must never reach the journal"
        );
    }

    /// The ORDERING half of the actor's bound — the sibling of
    /// `an_answer_that_only_exceeds_the_cap_after_redaction_is_rejected`, which pins the
    /// same property one screen above for `--text`.
    ///
    /// `--as` is redacted BEFORE it is size-checked, so the bytes checked are the bytes
    /// written. Nothing pinned that: the re-review reversed the two statements — checking
    /// the raw one-lined value (`Measured::AsGiven`) and redacting afterwards — and the
    /// whole `sensei-torii` crate stayed green while an actor that grows past the cap under
    /// redaction was accepted and journaled over-limit. `[REDACTED]` is LONGER than the
    /// shortest span it replaces, so the raw check bounds a value nobody stores.
    ///
    /// Asserts the POST-redaction size in the message for the same operator reason the
    /// answer's guard does: an operator told the number they typed cannot tell how much to
    /// cut.
    #[tokio::test]
    async fn an_actor_that_only_exceeds_the_cap_after_redaction_is_rejected() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &reviewer(), None).await;

        // The answer's fixture, for the answer's reason: the trailing space keeps each
        // value class bounded, so redaction yields 310 placeholders rather than collapsing
        // the whole string into one (which would SHRINK it).
        let unit = format!("{}:{} ", "token", "abcdef");
        let raw = unit.repeat(310);
        // Unlike `--text`, the actor is NOT trimmed — it is passed through
        // `render::one_line` (an identity here: no control characters) and then redacted.
        let journaled = redact_answer(&render::one_line(&raw));
        assert!(
            raw.len() <= MAX_HUMAN_TEXT_BYTES,
            "precondition: this actor is under the cap as typed ({} bytes)",
            raw.len()
        );
        assert!(
            journaled.len() > MAX_HUMAN_TEXT_BYTES,
            "precondition: redaction GROWS it past the cap ({} -> {} bytes)",
            raw.len(),
            journaled.len()
        );

        let e = answer(&s, &j, run, reviewer(), "ship it", &raw, now())
            .await
            .expect_err("an actor that would exceed the cap once redacted is refused");

        assert_eq!(e.code, crate::errors::EXIT_ERROR, "{}", e.message);
        assert!(
            e.message.contains(&journaled.len().to_string()),
            "must name the size that would actually be JOURNALED ({}), not the one the \
             operator typed ({}): {}",
            journaled.len(),
            raw.len(),
            e.message
        );
        assert!(
            e.message.contains("--as"),
            "must name the flag the operator actually typed: {}",
            e.message
        );
        assert!(
            journaled_answers(&j, run, &reviewer()).await.is_empty(),
            "an over-limit actor must never reach the journal"
        );
    }

    /// AC9, the torii half: a secret-shaped answer is redacted BEFORE the durable write.
    ///
    /// Not merely a display concern — the answer BECOMES the node's output and flows into
    /// downstream model prompts. Redacting before the write is also what keeps live ==
    /// journaled == replayed: the executor applies the same pure pass on the fold-read, and
    /// `[REDACTED]` matches no credential shape, so the double scrub is idempotent.
    ///
    /// The credential is assembled at runtime — the repo's Semgrep CWE-798 hook blocks a
    /// literal one in a fixture.
    #[tokio::test]
    async fn a_secret_shaped_answer_is_redacted_before_it_is_journaled() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &reviewer(), None).await;
        let secret = format!("sk-{}", "A".repeat(24));

        answer(
            &s,
            &j,
            run,
            reviewer(),
            &format!("ship it, the key is {secret}"),
            "alice",
            now(),
        )
        .await
        .expect("delivers");

        let durable = format!("{:?}", j.load(run).await.unwrap());
        assert!(
            !durable.contains(&secret),
            "the answer reached durable storage in plaintext: {durable}"
        );
        assert!(durable.contains("[REDACTED]"), "{durable}");
    }

    /// The SIBLING field on the same durable row, held to the same rule.
    ///
    /// Design §6: "Every operator-facing string — the answer, the actor, the prompt, the
    /// question in `list-paused` — … goes through the redactor before the durable write."
    /// The whole-slice review found `actor` reaching `journal_events` in plaintext while
    /// `text` on the SAME `AgentAnswered` row was scrubbed: it got `render::one_line` and a
    /// size check and no redaction pass at all.
    ///
    /// `--as` is exactly the field an operator scripts (`--as "$CI_TOKEN_OWNER"`,
    /// `--as "$(vault read ...)"`), and `render::redact_payload`'s own doc states the reason
    /// the scrub exists at all: "a human who pastes a token has put it into durable storage
    /// permanently". The exposure is not bounded by the executor's fold-read redactor
    /// either: that is opt-in (`Executor::with_redactor`, default `None`), so an embedder
    /// without it carries the plaintext actor into the node's OUTPUT, the blackboard, the
    /// CAS blob and every downstream model prompt — and the durable jsonb keeps it either
    /// way.
    ///
    /// The redaction runs BEFORE the size check, so the bytes checked are the bytes
    /// written, and the check's label moves to `Measured::AfterRedaction` with it because
    /// `[REDACTED]` can now GROW this value past the cap — the exact ordering defect s1 and
    /// s2 each shipped once for the answer.
    #[tokio::test]
    async fn a_secret_shaped_actor_is_redacted_before_it_is_journaled() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &reviewer(), None).await;
        let secret = format!("sk-{}", "A".repeat(24));

        answer(&s, &j, run, reviewer(), "ship it", &secret, now())
            .await
            .expect("delivers");

        let durable = format!("{:?}", j.load(run).await.unwrap());
        assert!(
            !durable.contains(&secret),
            "the actor reached durable storage in plaintext: {durable}"
        );
        assert!(durable.contains("[REDACTED]"), "{durable}");
    }

    /// `render::one_line(actor)` is load-bearing and was guarded by nothing: review mutated
    /// it to `actor.to_string()` and the whole `sensei-torii` suite (lib + cli + e2e) stayed
    /// green.
    ///
    /// The hazard is strictly worse than the node id's, which IS pinned
    /// (`a_hostile_node_id_cannot_forge_a_line_or_move_the_cursor`): a node id is only
    /// echoed to a terminal, whereas `actor` is folded into the node's OUTPUT and travels
    /// into every downstream model prompt and every later render of this run.
    ///
    /// Asserted on the JOURNALED row rather than on stdout, because the actor is not echoed
    /// in the outcome text — a stdout-only assertion would pass while the durable row still
    /// carried the newline and the escape.
    #[tokio::test]
    async fn a_hostile_actor_cannot_forge_a_line_or_move_the_cursor() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &reviewer(), None).await;
        let hostile = format!(
            "mallory\n{}  x  y\u{1b}[2K",
            crate::cmd::run::tests::FORGED_RUN
        );

        answer(&s, &j, run, reviewer(), "ship it", &hostile, now())
            .await
            .expect("delivers");

        let answers = journaled_answers(&j, run, &reviewer()).await;
        assert_eq!(answers.len(), 1, "{answers:?}");
        let actor = &answers[0].1;
        assert!(
            !actor.contains('\n') && !actor.contains('\u{1b}'),
            "a control character survived into the durable actor: {actor:?}"
        );
    }

    // ---- AC7: the three-way cross-refusal, proven in one place ------------------------
    //
    // `run signal` answers an `AwaitSignal`, `run gate decide` a `HumanGate`, and this
    // command a human-backed `Agent`. Each must refuse the other two AND name the verb that
    // would work — a refusal that only says "wrong kind" sends an operator to check a node
    // id that was right when the COMMAND was wrong.

    /// AC7, arm one: `run agent answer` aimed at an `AwaitSignal`.
    #[tokio::test]
    async fn an_answer_aimed_at_an_await_signal_node_points_at_run_signal() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = awaiting_journal(run, &reviewer(), None).await;

        let out = answer(&s, &j, run, reviewer(), "ship it", "alice", now())
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
        assert!(journaled_answers(&j, run, &reviewer()).await.is_empty());
    }

    /// AC7, arm two: `run agent answer` aimed at a `HumanGate`.
    #[tokio::test]
    async fn an_answer_aimed_at_a_human_gate_points_at_run_gate_decide() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = gate_journal(run, &reviewer(), None, &["ship", "hold"]).await;

        let out = answer(&s, &j, run, reviewer(), "ship it", "alice", now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("HumanGate"),
            "must name what the node actually is: {}",
            out.text
        );
        assert!(
            out.text.contains("run gate decide"),
            "must name the command that would work: {}",
            out.text
        );
        assert!(journaled_answers(&j, run, &reviewer()).await.is_empty());
    }

    /// AC7, arm three: `run signal` aimed at a human-backed `Agent`.
    ///
    /// Without this refusal a raw `--payload` would be journaled as a `SignalReceived` that
    /// `run_human_agent` never reads — reported here as `signalled`, read by nothing. It
    /// would also skip ATTRIBUTION entirely: `SignalReceived` has no `actor` field, and an
    /// agent's answer lands in the node's OUTPUT.
    #[tokio::test]
    async fn signal_on_a_human_backed_agent_is_refused_and_points_at_run_agent_answer() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &reviewer(), None).await;

        let out =
            crate::cmd::run::signal(&s, &j, run, reviewer(), serde_json::json!("ship it"), now())
                .await
                .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("Agent"),
            "must name what the node actually is: {}",
            out.text
        );
        assert!(
            out.text.contains("run agent answer"),
            "must name the command that would work: {}",
            out.text
        );
        assert!(
            j.load(run)
                .await
                .unwrap()
                .iter()
                .all(|(_, e)| !matches!(e, JournalEvent::SignalReceived { .. })),
            "a raw payload must never be journaled for a node that will never read one"
        );
    }

    /// AC7, arm four: `run gate decide` aimed at a human-backed `Agent`.
    #[tokio::test]
    async fn a_decision_on_a_human_backed_agent_is_refused_and_points_at_run_agent_answer() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &reviewer(), None).await;

        let out =
            crate::cmd::gate::decide(&s, &j, run, reviewer(), "approve", "alice", None, now())
                .await
                .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("Agent"),
            "must name what the node actually is: {}",
            out.text
        );
        assert!(
            out.text.contains("run agent answer"),
            "must name the command that would work: {}",
            out.text
        );
        assert!(
            j.load(run)
                .await
                .unwrap()
                .iter()
                .all(|(_, e)| !matches!(e, JournalEvent::GateDecided { .. })),
            "a named option must never be journaled for a node that has no menu"
        );
    }

    // ---- The write window --------------------------------------------------------------

    /// The ORDER discriminator: swap the append and the `force_wake` and the injected store
    /// failure short-circuits before the append, leaving zero `AgentAnswered` rows.
    /// Appending first is what guarantees any worker that can observe the wake folds a
    /// journal that already holds the answer.
    #[tokio::test]
    async fn an_answer_is_appended_before_force_wake_and_its_failure_surfaces() {
        let run = RunId(uuid::Uuid::new_v4());
        let store = FailingForceWakeStore(paused_store(run, None).await);
        let j = agent_journal(run, &reviewer(), None).await;

        let out = answer(&store, &j, run, reviewer(), "ship it", "alice", now())
            .await
            .expect("a post-append fault is reported, not returned as a bare error");

        assert_ne!(
            out.code, EXIT_OK,
            "the injected force_wake failure must surface, not be swallowed into a green \
             success: {}",
            out.text
        );
        assert_eq!(
            journaled_answers(&j, run, &reviewer()).await.len(),
            1,
            "the answer must already be durable even though force_wake failed"
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

    /// A journal fault raised AFTER the append reaches stdout as an operator report rather
    /// than as an error — and a backend message is FREE TEXT FROM THE DRIVER, so it gets the
    /// same transform `list-paused` and `gate decide` already give the identical error
    /// class: redact, collapse control characters, cap.
    ///
    /// `PostgresJournal::load` builds this message from both `sqlx::Error` (a pool timeout
    /// carries the whole connection string, password included) and `serde_json::Error` (over
    /// a TYPED `JournalEvent`, which quotes the offending row). Unguarded it also carries a
    /// newline that forges a pastable run row and a raw ESC that rewrites the screen.
    #[tokio::test]
    async fn a_journal_fault_after_the_append_is_not_echoed_raw() {
        /// Folds cleanly for the PRE-check and faults on the post-append re-read — the one
        /// window in which a backend message reaches this command's stdout.
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
            ) -> Result<orchestrator_core::Seq, orchestrator_core::JournalError> {
                self.inner.append(run, event).await
            }
            async fn load(
                &self,
                run: RunId,
            ) -> Result<Vec<(orchestrator_core::Seq, JournalEvent)>, orchestrator_core::JournalError>
            {
                if self.loads.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    return self.inner.load(run).await;
                }
                Err(crate::cmd::run::tests::hostile_backend_error(run))
            }
        }

        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let inner = std::sync::Arc::new(agent_journal(run, &reviewer(), None).await);
        let j = FaultingReloadJournal {
            inner: inner.clone(),
            loads: std::sync::atomic::AtomicUsize::new(0),
        };

        let out = answer(&s, &j, run, reviewer(), "ship it", "alice", now())
            .await
            .expect("a post-append fault is reported, not returned as a bare error");

        assert_eq!(
            journaled_answers(&inner, run, &reviewer()).await.len(),
            1,
            "precondition: the answer is durable, so this really is the post-append report"
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
            "the credential span must be visibly redacted — dropping the message entirely \
             would pass the leak assertion while telling the operator nothing: {}",
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
                .filter(|l| l
                    .trim_start()
                    .starts_with(crate::cmd::run::tests::FORGED_RUN))
                .count(),
            0,
            "a newline in the fault forged a line that reads as its own run row:\n{}",
            out.text
        );
    }

    // ---- The post-append report ---------------------------------------------------------
    //
    // Everything below `journal.append` is REPORTING: the answer is already durable, and the
    // only remaining question is what — if anything — read it. Four outcomes, and they are
    // not interchangeable: one of them is the exit-0 "answered", and reporting an ORPHANED
    // row as answered tells an operator a human is done when the run will never see their
    // answer.
    //
    // This block shipped with ZERO coverage while both siblings tested their identical
    // copies (`cmd::gate`'s `a_decision_the_node_died_before_is_reported_as_an_orphan_not_
    // as_decided` and `a_decision_orphaned_by_a_cancel_does_not_advise_waiting_for_a_pause`,
    // and `cmd::run`'s equivalents): replacing the `signal_state_at` fold with a hard-coded
    // `(NotAwaiting, None)` left every test in `sensei-torii` green, and so did turning the
    // terminal-run arm's condition into `false &&`. Both are covered now.

    /// What the concurrent worker's drive does to `run` inside the delivery window — see
    /// [`HumanRacingStore`].
    #[derive(Clone, Copy, PartialEq)]
    enum HumanRacingDrive {
        /// It folded the journal — which by then contains our answer — completed the node
        /// with it and finished the run. The delivery worked PERFECTLY; the only thing that
        /// differs from the ordinary path is that there is no tick left to wait for.
        CompletesTheNode,
        /// It had loaded the journal BEFORE our answer landed, so it saw no answer, found
        /// the deadline expired and failed the node behind our row without ever reading it.
        ///
        /// This is the only way a human-backed agent reaches `(other, at > appended)`: step 3
        /// of `run_human_agent` returns `Completed` before the deadline is acted on at all,
        /// so a drive that DID fold our row could not have expired the node. That is why this
        /// command needs no fourth arm where `gate decide` does — see [`answer`].
        ExpiresTheNode,
        /// An operator cancelled the run. `cancel` is node-blind and journals no NODE event,
        /// so the agent still folds as awaiting (`at: None`) while the run is over — the one
        /// route to the `after.status.is_terminal()` arm below the `at` match.
        CancelsTheRun,
    }

    /// A `SchedulerStore` that runs a concurrent worker against `run` at the top of
    /// `force_wake` — i.e. exactly in the window between [`answer`]'s append and the point
    /// where its effect becomes observable. `answer` appends BEFORE it calls `force_wake`, so
    /// this models a worker that drove the run AFTER the answer landed. The `force_wake`
    /// itself SUCCEEDS; it is simply a no-op, because the row is no longer `paused`.
    ///
    /// A third hand-rolled copy of `cmd::run`'s `SignalRacingStore` and `cmd::gate`'s
    /// `GateRacingStore` only because each is pinned to its own module's node id and neither
    /// is exported; the one piece that must NOT be re-derived — the durable completion marker
    /// `signal_states` reads — is the shared `cmd::run::tests::append_completion`, since a
    /// human-backed agent journals no `NodeCompleted` either and two hand-rolled
    /// `ContextWrite` shapes would be two places for that marker to drift.
    ///
    /// Single-threaded, deterministic, no database.
    struct HumanRacingStore {
        inner: InMemorySchedulerStore,
        journal: std::sync::Arc<InMemoryJournal>,
        run: RunId,
        drive: HumanRacingDrive,
    }

    /// The EXACT `NodeFailed` text `Executor::run_human_agent` journals when
    /// `wait_or_expire_by_id` returns `Expired` — copied verbatim from
    /// `crates/orchestrator/src/executor/human.rs` rather than paraphrased, so this fixture
    /// models a row this repo actually writes.
    const EXECUTOR_EXPIRY: &str =
        "human_agent: node reviewer passed its deadline 1970-02-04T17:20:01Z with no answer";

    #[async_trait::async_trait]
    impl SchedulerStore for HumanRacingStore {
        async fn enqueue(
            &self,
            run: RunId,
            graph: &Graph,
            now: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), OrchestratorError> {
            self.inner.enqueue(run, graph, now).await
        }
        async fn record_paused(
            &self,
            run: RunId,
            next_wake: Option<chrono::DateTime<chrono::Utc>>,
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
            now: chrono::DateTime<chrono::Utc>,
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
            before: chrono::DateTime<chrono::Utc>,
        ) -> Result<u64, OrchestratorError> {
            self.inner.count_terminal_before(before).await
        }
        async fn prune_terminal(
            &self,
            before: chrono::DateTime<chrono::Utc>,
        ) -> Result<u64, OrchestratorError> {
            self.inner.prune_terminal(before).await
        }
        async fn force_wake(
            &self,
            run: RunId,
            now: chrono::DateTime<chrono::Utc>,
        ) -> Result<(), OrchestratorError> {
            if run != self.run {
                return self.inner.force_wake(run, now).await;
            }
            match self.drive {
                HumanRacingDrive::CompletesTheNode => {
                    // A worker's tick claims the run (`paused -> waking`), and its drive
                    // folds the journal — which by now contains our answer — completes the
                    // node with it and finishes the run.
                    self.inner
                        .claim_due(now, chrono::Duration::seconds(60), 10)
                        .await?;
                    crate::cmd::run::tests::append_completion(&self.journal, run, &reviewer())
                        .await;
                    self.journal.append(run, JournalEvent::RunCompleted).await?;
                    self.inner
                        .record_terminal(run, RunStatus::Completed, None)
                        .await?;
                }
                HumanRacingDrive::ExpiresTheNode => {
                    self.inner
                        .claim_due(now, chrono::Duration::seconds(60), 10)
                        .await?;
                    self.journal
                        .append(
                            run,
                            JournalEvent::NodeFailed {
                                node: reviewer(),
                                error: EXECUTOR_EXPIRY.to_string(),
                            },
                        )
                        .await?;
                    self.inner
                        .record_terminal(run, RunStatus::Failed, Some("human_agent"))
                        .await?;
                }
                // No claim and no journal write at all: `cancel` is unconditional and
                // node-blind, which is exactly what leaves the agent folding as awaiting on
                // a run that is over.
                HumanRacingDrive::CancelsTheRun => self.inner.cancel(run).await?,
            }
            // Our own force_wake: succeeds, but is a conditional no-op now that the row is
            // no longer `paused`.
            self.inner.force_wake(run, now).await
        }
    }

    /// Drive `answer` against a worker that races it inside the delivery window, returning
    /// the report together with the seqs of our row and of whatever the drive journaled.
    /// The ORDERING is the whole discriminator on this arm, so every caller asserts it as a
    /// precondition rather than trusting the fixture.
    async fn answer_against_a_racing_drive(
        drive: HumanRacingDrive,
    ) -> (Outcome, std::sync::Arc<InMemoryJournal>, RunId) {
        let run = RunId(uuid::Uuid::new_v4());
        // `Some(now())` and NOT `None`: `claim_due` claims a paused row only when its
        // `next_wake` is due, so with a NULL deadline the modelled worker claims nothing,
        // `record_terminal` (which applies only to a `waking` row) silently no-ops, and the
        // run stays `paused` — every drive below would then collapse into the ordinary
        // queued-wake path and assert nothing. The journaled deadline matches the one
        // [`EXECUTOR_EXPIRY`] names, so the fixture is coherent with the row it writes.
        let inner = paused_store(run, Some(now())).await;
        let journal = std::sync::Arc::new(
            agent_journal(run, &reviewer(), Some(now() + chrono::Duration::seconds(1))).await,
        );
        let racing = HumanRacingStore {
            inner,
            journal: journal.clone(),
            run,
            drive,
        };

        let out = answer(
            &racing,
            journal.as_ref(),
            run,
            reviewer(),
            "ship it",
            "alice",
            now(),
        )
        .await
        .expect("a post-append race is reported, not returned as a bare error");
        (out, journal, run)
    }

    /// The seq of the first event matching `p`, which must be on the journal.
    async fn seq_of(
        j: &InMemoryJournal,
        run: RunId,
        p: fn(&JournalEvent) -> bool,
    ) -> orchestrator_core::Seq {
        j.load(run)
            .await
            .unwrap()
            .into_iter()
            .find(|(_, e)| p(e))
            .map(|(s, _)| s)
            .expect("the event is on the journal")
    }

    /// THE check-then-act case on the post-append arm, and the one BOTH siblings shipped
    /// INVERTED: a worker that claims the run the instant the answer lands folds it,
    /// completes the node and drives the run to completion — the delivery worked perfectly —
    /// and a report that read only the SCHEDULER row said `not queued`, exit 2, advising
    /// `torii run wake`, which refuses every non-paused run.
    ///
    /// This is the only exit-0 path through the post-append tail, so it is also the arm that
    /// must never be reachable for an ORPHAN: the two tests below are its other half.
    #[tokio::test]
    async fn an_answer_a_racing_worker_already_folded_is_reported_as_answered() {
        let (out, j, run) = answer_against_a_racing_drive(HumanRacingDrive::CompletesTheNode).await;

        let answered = seq_of(&j, run, |e| matches!(e, JournalEvent::AgentAnswered { .. })).await;
        let completed = seq_of(&j, run, |e| matches!(e, JournalEvent::ContextWrite { .. })).await;
        assert!(
            answered < completed,
            "precondition: the drive completed the node by folding OUR answer \
             (answered={answered} completed={completed})"
        );

        assert_eq!(
            out.code, EXIT_OK,
            "the answer was delivered AND read — reporting a failure here would send an \
             operator to `torii run wake`, which refuses every non-paused run: {}",
            out.text
        );
        assert!(
            out.text.starts_with("answered:"),
            "must report the delivery it actually achieved: {}",
            out.text
        );
        assert!(
            !out.text.contains("not queued") && !out.text.contains("run wake"),
            "the run is already moving; there is no wake to chase: {}",
            out.text
        );
    }

    /// The other side of that discriminator: same `at > appended` ORDERING, opposite meaning.
    /// A drive that had already loaded the journal before our row landed sees no answer,
    /// finds the deadline expired and fails the node behind us — so the answer is durable and
    /// nothing read it.
    ///
    /// Without this test the whole `at.is_some()` classification collapses to
    /// `Outcome::ok("answered")`, which would tell an operator their answer stopped a node
    /// that in fact died of its SLA, and hide the far more useful fact that it expired.
    #[tokio::test]
    async fn an_answer_a_racing_expiry_never_read_is_not_reported_as_answered() {
        let (out, j, run) = answer_against_a_racing_drive(HumanRacingDrive::ExpiresTheNode).await;

        let answered = seq_of(&j, run, |e| matches!(e, JournalEvent::AgentAnswered { .. })).await;
        let failed = seq_of(&j, run, |e| matches!(e, JournalEvent::NodeFailed { .. })).await;
        assert!(
            answered < failed,
            "precondition: identical ORDERING to the honoured answer above — only what the \
             drive journaled differs (answered={answered} failed={failed})"
        );

        assert_eq!(
            out.code, EXIT_PRECONDITION,
            "the deadline fired with no answer read; reporting this as answered claims a \
             delivery nothing ever looked at: {}",
            out.text
        );
        assert!(
            out.text.contains("not read") && out.text.contains("would not have seen it"),
            "must say plainly that the drive never saw it: {}",
            out.text
        );
    }

    /// The `(other, false)` arm — a node that died INSIDE the write window, so our row landed
    /// BEHIND a marker that was already there. Reached with a journal whose `append` slips
    /// the `NodeFailed` in first: the store hook fires on `force_wake`, which is post-append
    /// by construction, so no `SchedulerStore` fixture can produce this ordering.
    ///
    /// It must never report success, and the residue is durable and consequential: a
    /// human-backed agent journals no `NodeCompleted` and `NodeFailed` is not folded as a
    /// barrier, so a re-`start` of this run would re-execute the node and fold this late
    /// answer as its OUTPUT — which then flows into every downstream model prompt.
    #[tokio::test]
    async fn an_answer_the_node_died_before_is_reported_as_an_orphan_not_as_answered() {
        struct DiesInsideTheWindow {
            inner: std::sync::Arc<InMemoryJournal>,
        }
        #[async_trait::async_trait]
        impl ExecutionJournal for DiesInsideTheWindow {
            async fn append(
                &self,
                run: RunId,
                event: JournalEvent,
            ) -> Result<orchestrator_core::Seq, orchestrator_core::JournalError> {
                if matches!(event, JournalEvent::AgentAnswered { .. }) {
                    self.inner
                        .append(
                            run,
                            JournalEvent::NodeFailed {
                                node: reviewer(),
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
            ) -> Result<Vec<(orchestrator_core::Seq, JournalEvent)>, orchestrator_core::JournalError>
            {
                self.inner.load(run).await
            }
        }

        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let inner = std::sync::Arc::new(agent_journal(run, &reviewer(), None).await);
        let j = DiesInsideTheWindow {
            inner: inner.clone(),
        };

        let out = answer(&s, &j, run, reviewer(), "ship it", "alice", now())
            .await
            .expect("no hard error");

        let answered = seq_of(&inner, run, |e| {
            matches!(e, JournalEvent::AgentAnswered { .. })
        })
        .await;
        let failed = seq_of(&inner, run, |e| {
            matches!(e, JournalEvent::NodeFailed { .. })
        })
        .await;
        assert!(
            failed < answered,
            "precondition: the node was already dead when the row landed (failed={failed} \
             answered={answered})"
        );

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("was already") && out.text.contains("nothing read it"),
            "must say the row is a durable orphan, not a delivery: {}",
            out.text
        );
        assert!(
            out.text.contains("re-`start`"),
            "must name the durable consequence — the answer is still a last-wins value: {}",
            out.text
        );
    }

    /// The other half of the post-append window: the RUN went terminal while the NODE itself
    /// never did. `cancel` is node-blind and journals no node event, so the agent still folds
    /// as awaiting (`at: None`) and the report falls through to the status arm.
    ///
    /// "Run `torii run wake <id>` once it is paused again" is a dead end here for exactly the
    /// reason the PRE-check arm already avoids it
    /// (`an_answer_on_a_terminal_run_does_not_advise_waiting_for_a_pause`): `wake` refuses
    /// every non-paused run and no shipped store moves a terminal row back to `paused`. The
    /// rule has to hold on BOTH arms of the same function — `gate decide` shipped it on one.
    #[tokio::test]
    async fn an_answer_orphaned_by_a_cancel_does_not_advise_waiting_for_a_pause() {
        let (out, j, run) = answer_against_a_racing_drive(HumanRacingDrive::CancelsTheRun).await;

        assert_eq!(
            journaled_answers(&j, run, &reviewer()).await.len(),
            1,
            "precondition: the answer is durable — this is a post-append report, not a refusal"
        );
        assert!(
            j.load(run)
                .await
                .unwrap()
                .iter()
                .all(|(_, e)| !matches!(e, JournalEvent::NodeFailed { .. })),
            "precondition: a cancel journals no NODE event, so the node still folds as \
             awaiting and this really is the `at: None` arm"
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

    // ---- Run-level state ---------------------------------------------------------------

    #[tokio::test]
    async fn an_answer_for_an_unknown_run_is_refused() {
        let s = InMemorySchedulerStore::default();
        let run = RunId(uuid::Uuid::new_v4());
        let j = InMemoryJournal::new();

        let out = answer(&s, &j, run, reviewer(), "ship it", "alice", now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(out.text.contains("no such run"), "{}", out.text);
    }

    /// A terminal run is never paused again by any shipped store, so nothing would ever
    /// fold the answer — and the advice must not be "retry once it shows paused", which is
    /// advice to wait forever. `record_terminal` journals no node event, so the agent still
    /// folds as having asked on a run that is over: that is how an operator reaches this
    /// path. Same lesson `run signal` and `gate decide` each learned.
    #[tokio::test]
    async fn an_answer_on_a_terminal_run_does_not_advise_waiting_for_a_pause() {
        for terminal in [
            RunStatus::Cancelled,
            RunStatus::Completed,
            RunStatus::Failed,
        ] {
            let run = RunId(uuid::Uuid::new_v4());
            let s = InMemorySchedulerStore::default();
            s.enqueue(run, &Graph { nodes: vec![] }, now())
                .await
                .unwrap();
            s.record_terminal(run, terminal, None).await.unwrap();
            let j = agent_journal(run, &reviewer(), None).await;

            let out = answer(&s, &j, run, reviewer(), "ship it", "alice", now())
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
                journaled_answers(&j, run, &reviewer()).await.is_empty(),
                "nothing may be written into a run that is over"
            );
        }
    }

    /// `waking` is TRANSIENT — a worker holds the lease and is folding this journal right
    /// now — so retrying IS real advice here, and this is the arm that must give it. The
    /// opposite of the terminal arm above, which is why one message for both was wrong.
    #[tokio::test]
    async fn an_answer_on_a_waking_run_is_worth_retrying_and_says_so() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = InMemorySchedulerStore::default();
        s.enqueue(run, &Graph { nodes: vec![] }, now())
            .await
            .unwrap();
        let j = agent_journal(run, &reviewer(), None).await;

        let out = answer(&s, &j, run, reviewer(), "ship it", "alice", now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(
            out.text.contains("shows it paused"),
            "a waking run is worth retrying, and the operator must be told so: {}",
            out.text
        );
        assert!(
            journaled_answers(&j, run, &reviewer()).await.is_empty(),
            "a worker holds the lease and is folding this journal — nothing may be written"
        );
    }

    /// An answer that is only whitespace is the same omission with quotes around it, and
    /// clap's required `answer_src` group cannot see it — so the trim is what actually
    /// holds. It matters more than `gate reject --reason ''`: a blank reason is a missing
    /// explanation, whereas a blank answer becomes this node's OUTPUT and flows into every
    /// downstream model prompt as a silent non-answer.
    #[tokio::test]
    async fn a_blank_answer_is_refused() {
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &reviewer(), None).await;

        let out = answer(&s, &j, run, reviewer(), "  \t \n ", "alice", now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_PRECONDITION, "{}", out.text);
        assert!(out.text.contains("empty"), "{}", out.text);
        assert!(journaled_answers(&j, run, &reviewer()).await.is_empty());
    }

    /// The two sourcing paths must produce the SAME journaled bytes. A file written by
    /// `$EDITOR` always ends with a newline, so trimming in only one of them would make the
    /// same answer journal differently depending on how it was supplied — and a resumed run
    /// folds those bytes as the node's output.
    #[tokio::test]
    async fn the_same_answer_journals_identically_from_a_file_and_from_argv() {
        let dir = std::env::temp_dir().join(format!("torii-answer-lib-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let path = dir.join("answer.txt");
        std::fs::write(&path, "ship it\n").expect("write answer");

        let from_file = answer_args(AgentAction::Answer {
            run_id: "the-run".into(),
            node: "reviewer".into(),
            text: None,
            text_file: Some(path.clone()),
            r#as: "alice".into(),
        })
        .expect("reads the file");
        let from_argv = answer_args(AgentAction::Answer {
            run_id: "the-run".into(),
            node: "reviewer".into(),
            text: Some("ship it".into()),
            text_file: None,
            r#as: "alice".into(),
        })
        .expect("takes the inline text");
        assert_eq!(from_file.run_id, "the-run");
        assert_eq!(from_file.node, "reviewer");
        assert_eq!(from_file.actor, "alice", "the actor is passed through RAW");

        async fn journal_from(text: &str) -> Vec<(String, String)> {
            let run = RunId(uuid::Uuid::new_v4());
            let s = paused_store(run, None).await;
            let j = agent_journal(run, &reviewer(), None).await;
            answer(&s, &j, run, reviewer(), text, "alice", now())
                .await
                .expect("delivers");
            journaled_answers(&j, run, &reviewer()).await
        }
        assert_eq!(
            journal_from(&from_file.text).await,
            journal_from(&from_argv.text).await,
            "a trailing newline from a file must not change the durable answer"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An unreadable file is an operator typo: the PATH is echoed because that is the whole
    /// point of the message, and nothing READ from it ever is — that is the value which
    /// might be a credential.
    ///
    /// `expect_err` is deliberately not used: it needs `AnswerArgs: Debug`, and this struct
    /// holds the operator's raw answer — the one field most likely to carry a pasted
    /// credential. A `Debug` impl on it would make that value one `{:?}` away from any log
    /// line, so `cmd::gate::Decision` does not derive it either.
    #[test]
    fn an_unreadable_answer_file_names_the_path_and_the_flag() {
        let Err(e) = answer_args(AgentAction::Answer {
            run_id: "the-run".into(),
            node: "reviewer".into(),
            text: None,
            text_file: Some("/nonexistent/answer.txt".into()),
            r#as: String::new(),
        }) else {
            panic!("an unreadable file must be refused");
        };

        assert_eq!(e.code, crate::errors::EXIT_ERROR, "{}", e.message);
        assert!(e.message.contains("--text-file"), "{}", e.message);
        assert!(
            e.message.contains("/nonexistent/answer.txt"),
            "{}",
            e.message
        );
    }

    /// A node id is operator-supplied free text and every message echoes it back to a
    /// terminal, so a raw newline must not be able to forge a line that reads as its own
    /// awaiting row, and an ESC must not be able to rewrite what is already on screen.
    #[tokio::test]
    async fn a_hostile_node_id_cannot_forge_a_line_or_move_the_cursor() {
        let hostile = NodeId(format!(
            "reviewer\n{}  reviewer  answered\u{1b}[2K",
            crate::cmd::run::tests::FORGED_RUN
        ));
        let run = RunId(uuid::Uuid::new_v4());
        let s = paused_store(run, None).await;
        let j = agent_journal(run, &hostile, None).await;

        let out = answer(&s, &j, run, hostile.clone(), "ship it", "alice", now())
            .await
            .expect("no hard error");

        assert_eq!(out.code, EXIT_OK, "{}", out.text);
        assert!(
            !out.text.contains('\u{1b}'),
            "a raw escape byte survived into the output: {:?}",
            out.text
        );
        assert_eq!(
            out.text
                .lines()
                .filter(|l| l
                    .trim_start()
                    .starts_with(crate::cmd::run::tests::FORGED_RUN))
                .count(),
            0,
            "a newline in the node id forged a line that reads as its own row:\n{}",
            out.text
        );
    }
}
