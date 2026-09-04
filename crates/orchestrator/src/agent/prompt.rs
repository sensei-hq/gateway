//! Prompt assembly + per-turn window budgeting for the agent runtime.

use kernel::types::request::ToolDefinition;
use orchestrator_core::{AgentDefinition, ContextKey, OrchestratorError, Registry};

/// An assembled prompt with its two halves still SEPARATE.
///
/// The join is [`PromptParts::join`], which is what the model path uses. The split exists for
/// SP-6 s3's human-backed roles, and it exists because the two halves have different OWNERS
/// and so must be bounded by different rules: `authored` is written by the config author,
/// who can trim it; `context` is whatever the upstream nodes happened to produce, which
/// nobody can bound at config time. Charging both against one cap whose breach is a terminal
/// `NodeFailed` is the defect the s3 whole-slice review found — see
/// [`orchestrator_core::MAX_HUMAN_CONTEXT_BYTES`].
pub struct PromptParts {
    /// The agent's `system_prompt` followed by each ACTIVATED skill body. Author-controlled.
    pub authored: String,
    /// One `(key, rendered value)` per resolved dependency, in `context` order — the raw
    /// material of the `## Context` section, NOT yet rendered, so a caller that must bound
    /// it can do so per dependency. Run data.
    pub context: Vec<(String, String)>,
    /// The activated tool schemas.
    pub tools: Vec<ToolDefinition>,
}

impl PromptParts {
    /// Re-join the two halves into the system prompt the MODEL receives, using the
    /// unbounded [`render_context_section`] — the model's own context window is the bound
    /// that applies on that path, and it is applied by the GATEWAY's `ContextWindowGate`
    /// (SP-7a) rather than truncated here, so a model is never silently asked about half
    /// a document.
    ///
    /// **The model path calls THIS**, and so does [`assemble_prompt`]. That is the whole
    /// reason it exists as a method rather than three lines inlined at each site. Before
    /// this, `drive_agent` concatenated the halves itself and `assemble_prompt` had ZERO
    /// production callers — its doc claimed to be "what the model path uses" while every
    /// one of its callers was a test in this file. The drift guard
    /// `the_model_context_section_is_unbounded_and_joins_exactly_as_before` pinned a
    /// function nothing shipped ran, so changing the executor's inline join alone left it,
    /// and the four `assemble_*` tests, green.
    pub fn join(self) -> (String, Vec<ToolDefinition>) {
        let mut system = self.authored;
        system.push_str(&render_context_section(&self.context));
        (system, self.tools)
    }

    /// [`Self::join`]'s budgeted sibling: the model path's answer to a prompt no candidate can
    /// hold.
    ///
    /// [`Self::join`]'s doc argues the model path must never truncate, "so a model is never
    /// silently asked about half a document". The operative word is SILENTLY, and SP-7b answers it
    /// on four channels rather than by keeping the refusal: the per-entry marker and the
    /// `(N of M dependencies shown)` tail [`render_context_section_measured`] already emits, the
    /// `ContextBudgeted` journal record, an additive `context_budgeted` key on the node's output,
    /// and an operator warn. See the spec's §5.5. Only the first of those is this function's own
    /// work; it returns the [`ContextCut`] so its caller can drive the other three, and so the
    /// caller can check the cut against the context floor ([`retained_meets_floor`]) — a plan is
    /// not proof of fit, which [`plan_budget`]'s doc spells out.
    ///
    /// `authored` is never cut (spec §5.2) — those are the config author's own bytes and they can
    /// trim them, which is the same asymmetry that made [`PromptParts`] two halves in the first
    /// place. Tool schemas are dropped WHOLE, per `plan.dropped_tools`, because a schema
    /// truncated mid-JSON is an invalid tool definition a provider rejects with a 400: a
    /// degradation turned into a hard failure.
    ///
    /// Filtering by NAME rather than by index is deliberate. `dropped_tools` carries the names
    /// [`plan_budget`] read off this same activation order, so a name it holds that no longer
    /// matches drops nothing and the prompt stays over-window — which the per-candidate
    /// `ContextWindowGate` then refuses, loudly, rather than putting an over-window request on the
    /// wire. An index into a list that had shifted would drop the WRONG schema and dispatch
    /// happily.
    pub fn join_bounded(self, plan: &BudgetPlan) -> (String, Vec<ToolDefinition>, ContextCut) {
        let (section, cut) =
            render_context_section_measured(&self.context, plan.context_budget_bytes);
        let mut system = self.authored;
        system.push_str(&section);
        let tools = self
            .tools
            .into_iter()
            .filter(|t| !plan.dropped_tools.contains(&t.name))
            .collect();
        (system, tools, cut)
    }
}

/// [`assemble_prompt`]'s work, stopping one step short of joining the halves — see
/// [`PromptParts`]. Every activation/unknown-ref rule described on `assemble_prompt` is
/// implemented HERE and nowhere else, so the model path and the human path cannot drift
/// apart in what they consider "the agent's prompt".
pub fn assemble_prompt_parts(
    registry: &Registry,
    agent: &AgentDefinition,
    context: &[(ContextKey, serde_json::Value)],
    query: &str,
) -> Result<PromptParts, OrchestratorError> {
    let mut authored = agent.system_prompt.clone();
    for skill_name in &agent.skills {
        let skill =
            registry
                .skill(skill_name)
                .ok_or_else(|| OrchestratorError::UnknownSkillRef {
                    agent: agent.name.clone(),
                    skill: skill_name.clone(),
                })?;
        if !skill.activation.is_active(query) {
            continue;
        }
        authored.push_str("\n\n");
        authored.push_str(&skill.body);
    }
    let rendered = context
        .iter()
        .map(|(key, value)| {
            let body = match value {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            (key.0.clone(), body)
        })
        .collect();
    let mut tools = Vec::with_capacity(agent.tools.len());
    for tool_name in &agent.tools {
        let spec = registry
            .tool(tool_name)
            .ok_or_else(|| OrchestratorError::UnknownToolRef {
                agent: agent.name.clone(),
                tool: tool_name.clone(),
            })?;
        if !spec.activation.is_active(query) {
            continue;
        }
        tools.push(ToolDefinition {
            name: spec.name.clone(),
            description: spec.description.clone(),
            input_schema: spec.input_schema.clone(),
        });
    }
    Ok(PromptParts {
        authored,
        context: rendered,
        tools,
    })
}

/// The heading both renderers open the section with, and the first thing
/// [`render_context_section_bounded`] spends its budget on.
///
/// A const rather than a literal because SP-7b's [`context_section_overhead`] has to charge for
/// it too — see that function for why a section's structural bytes have to be priced outside the
/// renderer at all. Both renderers take it from here, so they cannot open a section differently.
/// The per-entry heading is NOT shared with the unbounded renderer below: it builds that inline
/// to avoid an allocation per dependency on the model path, which is the hot one.
const CONTEXT_HEAD: &str = "\n\n## Context";

/// Render the `## Context` section exactly as the model receives it: no bound, no
/// truncation. An empty `entries` renders the EMPTY STRING, which is what keeps a
/// no-dependency agent's prompt byte-identical to the pre-blackboard prompt.
pub fn render_context_section(entries: &[(String, String)]) -> String {
    if entries.is_empty() {
        return String::new();
    }
    let mut out = String::from(CONTEXT_HEAD);
    for (key, body) in entries {
        out.push_str("\n\n### ");
        out.push_str(key);
        out.push('\n');
        out.push_str(body);
    }
    out
}

/// Assemble an agent's system prompt (body + each listed skill body, in order, +
/// a `## Context` section of resolved dependency outputs when `context` is
/// non-empty) and its tool schemas. A listed skill's body / tool's schema is
/// included only when its `activation.is_active(query)` (progressive disclosure;
/// `Always` — the default — always includes, so all-default agents are
/// byte-identical to the pre-activation prompt). Unknown skill/tool refs are a
/// loud error (defensive — `Registry::validate` should have caught them at load).
/// An empty `context` adds NOTHING, so a no-dependency agent's prompt is
/// byte-identical to the pre-blackboard prompt.
///
/// A thin composition of [`assemble_prompt_parts`] + [`PromptParts::join`] — the SAME two
/// calls, in the same order, that `drive_agent`'s model path makes. It is the whole prompt
/// as one expression, for callers (and tests) that want the joined string; the executor
/// keeps the halves apart only because it must choose between the model and human renderers
/// in between.
pub fn assemble_prompt(
    registry: &Registry,
    agent: &AgentDefinition,
    context: &[(ContextKey, serde_json::Value)],
    query: &str,
) -> Result<(String, Vec<ToolDefinition>), OrchestratorError> {
    Ok(assemble_prompt_parts(registry, agent, context, query)?.join())
}

/// The joined `system` string WITHOUT consuming the parts — for PRICING a prompt before
/// deciding whether to budget it.
///
/// [`PromptParts::join`] takes `self`, and the probe must not destroy the parts the real join
/// still needs. Rendering the same two halves the same way is the whole point: the figure
/// SP-7b compares against the window has to be the size of the prompt that WOULD have been
/// dispatched, so this calls [`render_context_section`] — the model path's own renderer —
/// rather than measuring the halves apart and adding up their lengths, which would omit the
/// section's structural bytes and could price an over-window prompt as fitting.
///
/// The cost is one extra render of the unbounded section on a turn that is about to be
/// budgeted or dispatched anyway; [`PromptParts::join`] is unchanged and the in-window path
/// pays it only when the chain's window is known.
pub fn render_unbounded_system(parts: &PromptParts) -> String {
    let mut system = parts.authored.clone();
    system.push_str(&render_context_section(&parts.context));
    system
}

/// The largest byte offset at or below `n` that is a char boundary in `s`.
///
/// `str::floor_char_boundary` is still unstable, and slicing on a raw byte index PANICS
/// mid-character. This codebase has already been burned by exactly that: `parse_fm_duration`
/// shipped a `split_at(len - 1)` that blew up on a multi-byte timeout unit. Every truncation
/// below goes through here, so a dependency output containing any non-ASCII text — which is
/// most real prose — cannot kill a run.
fn floor_char_boundary(s: &str, n: usize) -> usize {
    if n >= s.len() {
        return s.len();
    }
    let mut i = n;
    while i > 0 && !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

/// The marker [`truncate_with_marker`] appends, as its own function because SP-7b's
/// [`context_section_overhead`] has to know how wide it can get in order to reserve room for it.
///
/// One function called by both rather than a format string written twice: the reservation is only
/// as good as its agreement with what actually gets rendered, and this module's own tombstone
/// (below [`render_context_section_bounded`]) records what a duplicated size calculation cost the
/// last time — two figures that drifted and a provider 400 at the end of it.
fn truncation_marker(shown: usize, total: usize) -> String {
    format!("\n… (truncated: {shown} of {total} bytes shown)")
}

/// Truncate `s` to at most `max` bytes, appending a marker that says so, and report how many
/// bytes OF `s` the result carries.
///
/// The marker is not decoration. A human-backed reviewer shown a clipped contract with no
/// indication it was clipped will answer about the part they were given as though it were
/// the whole — which is a worse outcome than the failed node this truncation replaces. So
/// the marker carries both numbers, and it is charged AGAINST `max` rather than added on
/// top, so the caller's bound still holds.
///
/// When `max` is smaller than the marker itself the marker wins and the result overruns;
/// the caller's final clamp catches that. It needs hundreds of simultaneous dependencies to
/// reach, and "the section says it was cut but the section is itself cut" is still honest.
///
/// The second return value is the SOURCE bytes emitted — the length of the `&s[..shown]` prefix,
/// with the marker excluded. It is returned rather than left to the caller because a caller that
/// wanted it would have to re-derive `shown` from `max` and the marker's width, which is the
/// duplicated size calculation [`truncation_marker`]'s own doc exists to prevent; and it cannot be
/// recovered from the returned string, whose two halves are not separable once concatenated (a
/// body is free to contain the marker's text). [`ContextCut::retained_bytes`] is the sum of these.
fn truncate_with_marker(s: &str, max: usize) -> (String, usize) {
    if s.len() <= max {
        return (s.to_string(), s.len());
    }
    let marker = |shown: usize| truncation_marker(shown, s.len());
    // The marker's own length depends on `shown`, so budget with the widest it can be
    // (`shown` can never exceed `max`) and then re-render with the real number: fewer digits
    // only ever makes it shorter, so the total cannot grow past `max`.
    let widest = marker(max).len();
    let shown = floor_char_boundary(s, max.saturating_sub(widest));
    (format!("{}{}", &s[..shown], marker(shown)), shown)
}

/// Clamp an already-composed human question to the absolute size its durable journal row
/// may be, appending the same honest marker every other truncation here uses.
///
/// A second clamp AFTER [`render_context_section_bounded`] is not redundant: the executor
/// runs its redactor over the composed question before journaling it, and `[REDACTED]` is
/// LONGER than the shortest span it replaces, so a question that fitted before the scrub can
/// overrun after it. Truncating rather than failing there is deliberate — by that point the
/// author-error diagnosis has already happened, and the only remaining goal is that the row
/// is bounded.
pub fn truncate_prompt_to_bound(text: String, max: usize) -> String {
    if text.len() <= max {
        return text;
    }
    // The final clamp [`truncate_with_marker`]'s doc defers to. This function is the LAST
    // step of the durable write — `redact_and_clamp` returns straight into
    // `AgentAwaited.prompt` — so there is no later caller to catch the marker overrun, and
    // without this line a `max` below the ~36-byte marker width returned MORE than `max`.
    // Guarded by `a_prompt_clamp_never_overruns_its_bound`.
    //
    // `.0` discards the emitted-source-byte count: this clamp bounds a durable journal row, and
    // nothing downstream of it compares what survived against what was asked for.
    let (mut out, _emitted) = truncate_with_marker(&text, max);
    out.truncate(floor_char_boundary(&out, max));
    out
}

/// The `### {key}` heading [`render_context_section_bounded`] writes before each entry's body.
///
/// Shared with [`context_section_overhead`] for the same reason as [`truncation_marker`]: the
/// overhead is a reservation, and a reservation computed from a second copy of the layout is one
/// edit away from being wrong.
fn context_entry_heading(key: &str) -> String {
    format!("\n\n### {key}\n")
}

/// Render the `## Context` section for a HUMAN-backed node's question, bounded to `budget`
/// bytes in total.
///
/// The model path must NOT use this — [`render_context_section`] is its renderer, and a
/// model's own context window is the bound that applies there. That bound is enforced by
/// the gateway's `ContextWindowGate` (SP-7a), which SKIPS a candidate the prompt does not
/// fit rather than truncating, so a model is never silently asked about half a document.
///
/// This is the human path's answer to the same problem, and it differs because the failure
/// modes differ: an over-window model call falls through to a LARGER candidate in the same
/// chain — true since SP-7a, and the reason that slice exists; before it, the orchestrator
/// refused such a call outright against the chain's smallest window — whereas a
/// human-backed node that fails takes the whole run terminal AFTER the upstream tokens
/// have been spent.
///
/// This paragraph used to carry an "on an UNBUDGETED run" qualifier, because SP-DATA-5's
/// clamp (`executor/dispatch.rs`) bounded by `min_context_window(chain)` BEFORE selection
/// and refused a budgeted run with a durable pause, so the fall-through never happened
/// there. The SP-7a follow-on bounds by the smallest window that can SERVE the request
/// instead, which is the set the gate admits, so the qualifier is gone: budgeted and
/// unbudgeted runs both fall through. See
/// `a_budgeted_run_serves_a_prompt_only_the_larger_model_can_hold`.
///
/// The even split, the per-entry marker and the reserved tail are documented on
/// [`render_context_section_measured`], where the code is. This is that function with the
/// counts dropped — a wrapper rather than a second copy, so the human path and SP-7b's
/// budgeted model path cannot come to disagree about what "bounded to `budget`" renders.
/// Its own name is kept because `executor/human.rs` calls it and SP-6 s3's tests pin it.
pub fn render_context_section_bounded(entries: &[(String, String)], budget: usize) -> String {
    render_context_section_measured(entries, budget).0
}

/// [`render_context_section_bounded`]'s work, with the counts it computed on the way out.
///
/// Two callers, and they want different halves of the same render:
/// [`render_context_section_bounded`] (the human path, which needs only the string) and
/// [`PromptParts::join_bounded`] (SP-7b's budgeted model path, which must then check the cut
/// against the context floor). The bound itself is applied identically for both.
///
/// The counts cannot be recovered from the returned string: dependency bodies are arbitrary run
/// data and are free to contain the very `### ` headings and truncation markers a parser would key
/// on, so measuring afterwards would mean re-parsing text a dependency controls. So they are
/// accumulated as the section is written — `retained` from the same per-entry `room` the loop
/// already computes, via what [`truncate_with_marker`] reports it emitted.
///
/// The budget is split EVENLY across dependencies rather than first-come-first-served, so
/// one verbose upstream cannot crowd the others out of the question entirely — the human is
/// shown something from every node they were meant to consider, and when even that is
/// impossible they are TOLD how many nodes were dropped.
///
/// The promise is not unconditional, and the escape hatch is where the honesty lives. An
/// entry only exceeds its share when `room` falls under the marker's own ~36 bytes, at which
/// point `truncate_with_marker` overruns and the accumulated overflow pushes the section
/// past `budget`. The shipped clamp then cut trailing dependencies with NOTHING in the
/// output saying so — a silent breach of §5.4's "never show the human LESS than the model
/// would have had", and the one place an unmarked clip could still happen after every
/// per-entry cut had been marked. So the tail is reserved for a count, the cut is taken at
/// the end of the last COMPLETE entry (never mid-entry, so the number is exact rather than
/// estimated), and the section says `(N of M dependencies shown)`. Guarded by
/// `a_context_section_that_drops_dependencies_says_how_many`.
pub fn render_context_section_measured(
    entries: &[(String, String)],
    budget: usize,
) -> (String, ContextCut) {
    if entries.is_empty() {
        // No entries, no section, nothing asked for and nothing retained — and `deps_total: 0` is
        // what makes `retained_meets_floor(0, 0)`'s "never refused for retaining none of it"
        // branch the one this reaches.
        return (
            String::new(),
            ContextCut {
                requested_bytes: 0,
                retained_bytes: 0,
                deps_shown: 0,
                deps_total: 0,
            },
        );
    }
    let requested_bytes = entries.iter().map(|(_, body)| body.len()).sum();
    let share = budget.saturating_sub(CONTEXT_HEAD.len()) / entries.len();
    let mut out = String::from(CONTEXT_HEAD);
    // Where each entry ENDS, recorded as it is written. This is what lets the degradation
    // below report an EXACT count and cut on an entry boundary; recomputing it afterwards
    // would mean re-parsing the very headings a dependency's own body is free to forge.
    let mut ends = Vec::with_capacity(entries.len());
    // And what each entry contributed in BODY bytes, in the same order and for the same reason:
    // the degradation below drops WHOLE trailing entries, so the retained total has to lose
    // exactly the bytes those entries put in, which needs them attributed per entry.
    let mut retained = Vec::with_capacity(entries.len());
    for (key, body) in entries {
        let head = context_entry_heading(key);
        // The heading is what tells the human WHICH dependency this is, so it is never the
        // thing truncated; only the body competes for what is left of this entry's share.
        let room = share.saturating_sub(head.len());
        out.push_str(&head);
        let (rendered, body_bytes) = truncate_with_marker(body, room);
        out.push_str(&rendered);
        ends.push(out.len());
        retained.push(body_bytes);
    }
    let cut = |deps_shown: usize| ContextCut {
        requested_bytes,
        retained_bytes: retained[..deps_shown].iter().sum(),
        deps_shown,
        deps_total: entries.len(),
    };
    if out.len() <= budget {
        return (out, cut(entries.len()));
    }
    // Budget with the WIDEST count the marker can carry (`shown` can never exceed the total)
    // and re-render with the real one: fewer digits only ever makes it shorter, so the total
    // cannot grow past `budget` — the same arithmetic `truncate_with_marker` uses.
    let omitted = |shown: usize| format!("\n\n… ({shown} of {} dependencies shown)", entries.len());
    let ceiling = budget.saturating_sub(omitted(entries.len()).len());
    let shown = ends.iter().take_while(|end| **end <= ceiling).count();
    // `CONTEXT_HEAD.len()` rather than 0 when not even the first entry fits: the section heading
    // is what makes the remaining line legible as a statement about context at all.
    out.truncate(if shown == 0 {
        CONTEXT_HEAD.len()
    } else {
        ends[shown - 1]
    });
    out.push_str(&omitted(shown));
    // The unconditional clamp, kept. Everything above is best-effort shaping; this is the
    // line that makes "the journaled question is bounded" true no matter how many
    // dependencies, how long their keys, or how the marker arithmetic lands.
    //
    // It cannot make `retained_bytes` overstate the string, and that is derived rather than
    // assumed: with `shown >= 1` the clamp is a NO-OP here, because `ends[shown - 1] <= ceiling`
    // by the `take_while` and `omitted(shown).len() <= omitted(entries.len()).len()` (a count that
    // cannot exceed the total cannot need more digits than it), so the pushed tail lands at or
    // under `budget`. With `shown == 0` the clamp does bite, and `cut(0)` counts no body bytes at
    // all — what it eats is `CONTEXT_HEAD` and the tail. Either way the count matches the bodies
    // in `out`.
    out.truncate(floor_char_boundary(&out, budget));
    (out, cut(shown))
}

// This module holds NO token estimator any more, and the two it held are gone for the
// same reason one commit apart.
//
// `est_tokens(s) = s.chars().count() / 4` — the prose heuristic the deleted window
// pre-check ran on — went in SP-7a's review, once both of its callers (`over_budget` and
// `executor::support::est_prompt_tokens`) went with the halt they served.
//
// `est_tokens_pessimistic(s) = s.chars().count().div_ceil(3)` went in the serving-window
// review, when its last caller — `executor::dispatch::est_input_tokens` — was deleted in
// favour of `gateway::estimate_input_tokens_pessimistic`. It is deleted rather than kept
// as a utility for exactly the reason that commit gave for deleting the other two: `pub`
// in a `pub mod` makes a callerless function invisible to `dead_code`, so
// `clippy -D warnings` cannot report what it has become, and a function kept because it
// might be wanted again is the kind of thing that gets called again by mistake.
//
// Here that is not a general worry, it is the specific defect the review found. This
// function applied `ceil` PER STRING, and summing per-string ceilings is what made the
// clamp's estimate LARGER than the gateway's `ceil(Σ bytes / 3)` on ASCII text — which
// made the clamp's serving set a strict subset of the set selection drew from and put an
// over-window `max_tokens` on the wire. A second caller reaching for a convenient
// `est_tokens_pessimistic(&str)` would rebuild that divergence one string at a time. The
// orchestrator now has ONE way to size a payload: ask the gateway, over the whole
// `Payload`, with the function the `ContextWindowGate` uses.
//
// Nothing lost coverage. `the_pessimistic_estimate_is_chars_over_three_rounded_up` and
// `the_pessimistic_estimate_of_nothing_is_zero` tested this function's divisor and its
// empty-string boundary; `gateway::engine::util::the_estimate_is_ceil_of_utf8_bytes_over_
// three` pins both for the estimator that replaced it, from both sides and in the right
// unit (it asserts the CJK case this one got wrong by a factor of three).

// `over_budget(min_window, system, messages, tools)` lived here until SP-7a. It answered
// "does this prompt fit the chain's SMALLEST context window", and `executor/agent.rs`
// called it before every live ReAct turn, failing the node when it said yes.
//
// The question was asked in the wrong place and against the wrong number. Selection is
// the gateway's job, and a chain minimum is not a fact about any candidate: on
// `[gpt-4o 128k, fallback 8k]` this refused every prompt over 8k, including the ones the
// primary would have served. The gateway's `ContextWindowGate` now asks it per candidate.
//
// Deleted rather than left for a future caller. Its last caller was the halt this slice
// removed, and a function kept because it might be wanted again is exactly the kind of
// thing that gets called again by mistake — a second, chain-minimum window check
// re-introduced beside the per-candidate one would silently restore the bug.

/// What a budgeted `## Context` section actually cost, measured as it was rendered.
///
/// Returned alongside the rendered string rather than parsed back out of it: the bodies are
/// arbitrary run data and are free to contain the very `### ` headings a parser would key on, so
/// recomputing these figures after the fact would be re-parsing text a dependency controls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextCut {
    /// The sum of the raw entry BODIES before rendering — headings and separators excluded, so
    /// both sides of the floor ratio measure the same thing.
    pub requested_bytes: usize,
    /// The body bytes actually emitted. Excludes headings, truncation markers and the
    /// `(N of M dependencies shown)` tail — a marker is not retained content, and counting it
    /// would let a section consisting entirely of markers pass the floor.
    pub retained_bytes: usize,
    pub deps_shown: usize,
    pub deps_total: usize,
}

/// The plan for one budgeted turn: how many bytes the `## Context` section may use, and which
/// tool schemas were dropped whole to make room.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetPlan {
    pub context_budget_bytes: usize,
    /// Tool names, in the order they were dropped (reverse activation order).
    pub dropped_tools: Vec<String>,
}

/// Why [`plan_budget`] could not produce a plan — and, at the call site, WHOSE refusal that is.
///
/// The two causes look identical as an `Option::None` and they belong to different components.
/// Getting that wrong shipped a real defect: every `None` was read as a floor failure, so an
/// agent whose own 100 000-byte system prompt overran a 4096-token window was told its 100-byte
/// dependency context had failed a 25% floor, with remedies (shorten the upstream output, split
/// the node) that could not work — while the SAME agent with no dependencies at all fell through
/// to the gateway's accurate per-candidate diagnosis.
///
/// The rule the two variants encode: **SP-7b refuses only when a cut that FITS exists and
/// retains too little.** When no cut can fit, the un-cut prompt goes to selection and the
/// per-candidate `ContextWindowGate` refuses it — which is safe rather than a dispatch of an
/// over-window request, because the caller reaches this decision only under a guard that the
/// un-cut prompt already exceeds the chain's LARGEST window, and that is the same predicate the
/// gate skips on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetRefusal {
    /// `authored` alone overruns the budget. The authored half is never cut (spec §5.2), so no
    /// cut this planner can make brings the prompt inside the window: **the GATE's refusal.**
    AuthoredOverBudget,
    /// `authored` fits — so a dispatchable prompt exists, at every context size from zero up —
    /// but none of them retains [`orchestrator_core::CONTEXT_FLOOR_FRACTION`] of the dependency
    /// bodies, even with every tool schema dropped: **SP-7b's own refusal**, the floor, and the
    /// reason the floor exists at all.
    FloorUnreachable,
}

/// The bytes available to the whole `system` half, from the window and the transcript.
///
/// `window - MIN_OUTPUT_TOKENS - transcript_tokens`, converted to bytes by `× 3`. The `× 3` is
/// EXACT, not a fudge factor: `estimate_input_tokens_pessimistic` is `ceil(bytes / 3)`
/// (`gateway/src/engine/util.rs`), so a token budget of `T` is precisely a byte budget of `3T`
/// over the parts that estimator counts. `MIN_OUTPUT_TOKENS` is reserved so a degraded turn still
/// has room for a usable reply rather than being cut off mid-sentence.
///
/// **The reserve is the ONLY headroom the growing transcript gets, and that is a real limit
/// rather than an oversight.** A budgeted turn 0 is dispatched at exactly
/// `window - MIN_OUTPUT_TOKENS` tokens, so a ReAct agent that then calls a tool re-sends
/// `system` plus an assistant turn plus the tool result and has 256 tokens — 768 bytes — of room
/// for all of it. Past that every candidate is gated and the run takes the `AllGated` HOTL pause,
/// AFTER paying for turn 0. Spec §2 excludes the TRANSCRIPT from being budgeted; it does not
/// promise the transcript any room, and this is where those two facts meet. Pinned by
/// `a_budgeted_agent_that_calls_a_tool_busts_the_window_on_the_next_turn` so a change here is a
/// visible one; a growth allowance is the fix and it needs a number nobody has evidence for yet.
///
/// `None` when the transcript plus the reserve already fills the window — no cut can fit, so per
/// [`BudgetRefusal`] the caller hands the un-cut prompt to the gate rather than refusing here.
pub fn available_context_bytes(window: u32, transcript_tokens: u32) -> Option<usize> {
    let reserve = u32::try_from(orchestrator_core::MIN_OUTPUT_TOKENS).unwrap_or(u32::MAX);
    let spare = window
        .checked_sub(reserve)?
        .checked_sub(transcript_tokens)?;
    if spare == 0 {
        return None;
    }
    Some(spare as usize * 3)
}

/// The estimator-counted byte weight of one tool schema.
///
/// Mirrors `estimate_input_tokens_pessimistic`'s tools term exactly — `name + description +
/// input_schema.to_string()`. It is a separate function from the estimator rather than a call
/// into it because the estimator answers in TOKENS over a whole payload, and dropping schemas
/// needs a per-schema BYTE figure.
///
/// Being a mirror, it can drift, so it is pinned to the estimator ACROSS THE CRATE BOUNDARY by
/// `the_byte_budget_is_the_gateway_estimators_own_arithmetic`. That test builds a payload whose
/// counted content is one whole budget, sized as `system` bytes PLUS one real schema priced by
/// this function, and asserts the estimator returns exactly the token spare the budget came from.
/// Sizing the system half as the remainder is what puts this term on trial: price the schema
/// differently from the gateway and the total stops being one budget, whatever the schema's share
/// of it. Mutation-checked three ways — the gateway's tools term, the gateway's divisor, and this
/// crate's `× 3` — each reddens it.
///
/// Two earlier claims here were wrong and are recorded because the correction is the point. The
/// first said `the_planner_converts_a_token_window_into_a_byte_budget` was the guard: it is not,
/// it asserts a hand-derived literal over THIS crate's `× 3` and never calls the estimator. The
/// second, added while removing the first, said that test pinned the estimator's DIVISOR — the
/// same error from the other side. Nothing pinned either half until the test named above existed.
///
/// The residual, stated so the guard is not read as total: it holds the two together at ONE point
/// and only to within `div_ceil(3)`'s rounding, so a mirror one or two bytes light stays
/// invisible. If the terms do drift, the budget does not decide admission — the per-candidate
/// `ContextWindowGate` still measures the real payload — so the cost is a skipped candidate, and
/// when every candidate is skipped selection refuses before any provider is called rather than
/// putting an over-window prompt on the wire.
///
/// This is not the per-string estimator the tombstone above warns about, and the difference is
/// the rounding: that one divided and rounded UP per string, so summing its answers exceeded the
/// gateway's `ceil(Σ bytes / 3)`. Nothing here divides. The single conversion to tokens happens
/// once, in [`available_context_bytes`], over the whole budget.
fn tool_bytes(t: &ToolDefinition) -> usize {
    // `description` is `Option<String>` (`kernel/src/types/request.rs:200-205`), and the estimator
    // prices an absent one at ZERO rather than skipping the tool. Mirrored exactly, including that.
    t.name.len()
        + t.description.as_ref().map(|d| d.len()).unwrap_or(0)
        + t.input_schema.to_string().len()
}

/// The bytes a rendered `## Context` section spends on STRUCTURE rather than on dependency
/// bodies: [`CONTEXT_HEAD`], one [`context_entry_heading`] per entry, and the widest
/// [`truncation_marker`] each entry could be charged.
///
/// It exists because the two numbers SP-7b compares are in different units. The floor is measured
/// over BODY bytes ([`ContextCut::retained_bytes`], spec §5.3), but the figure
/// [`render_context_section_bounded`] takes bounds the WHOLE section — every heading and every
/// marker is paid for out of it. Subtracting this puts [`plan_budget`]'s fit check back in the
/// floor's unit; without it, a budget approved AT the floor renders below it, because the
/// structure was silently charged to the body's share.
///
/// Deliberately an OVER-estimate in two places, since being light is what re-opens that gap:
/// - a marker is reserved for EVERY entry, though only the truncated ones pay one, and at its
///   widest: a marker appears only when the body did NOT fit, so both the width the renderer
///   reserves and the one it finally emits are rendered from figures below `total`, and pricing
///   it at `total` bounds them both;
/// - `entries.len() - 1` absorbs the remainder the renderer's `budget / entries.len()` integer
///   division throws away.
///
/// Zero for no entries, matching the renderer's own early return: no entries, no section, nothing
/// charged. That branch is load-bearing rather than decorative — `entries.len() - 1` underflows on
/// an empty slice, which is a dependency-free agent, i.e. the most ordinary node there is.
fn context_section_overhead(entries: &[(String, String)]) -> usize {
    if entries.is_empty() {
        return 0;
    }
    let per_entry: usize = entries
        .iter()
        .map(|(key, body)| {
            context_entry_heading(key).len() + truncation_marker(body.len(), body.len()).len()
        })
        .sum();
    CONTEXT_HEAD.len() + (entries.len() - 1) + per_entry
}

/// Decide the context budget, dropping whole tool schemas from the END of the activation order
/// until the context floor fits.
///
/// Pure over its four arguments — no clock, no config, no window read. That purity is what makes
/// the journaled-budget determinism argument work (spec §4.2): the caller journals
/// `available_bytes`, and every later drive reproduces this plan from it.
///
/// Takes the context ENTRIES rather than a byte total because both of the figures it needs come
/// from them and must agree: the floor is a fraction of the entry bodies, and the room those
/// bodies will actually get is the budget minus the section structure the keys and bodies imply
/// (see [`context_section_overhead`]). A caller passing the two separately could pass a pair that
/// does not describe the same section.
///
/// **The fit check is an APPROXIMATION, in both directions, and [`retained_meets_floor`] over
/// the MEASURED cut is what actually decides (spec §5.2/§5.3).** What the overhead subtraction
/// buys is that the ordinary shapes cannot be approved here and refused there — one dependency,
/// or several of the SAME size, render at or above the floor whenever this approves, which
/// `a_budget_the_planner_approves_renders_at_or_above_the_floor` sweeps across the whole boundary
/// region for both — and that the drop loop no longer ends one schema early because heading bytes
/// were charged to the floor.
///
/// It does not make the two agree in general:
/// - it can still APPROVE a budget the render falls short of, because the renderer splits the
///   budget EVENLY and never redistributes an unused share (spec §5.2 records that as an
///   inherited limitation) — a 10-byte dependency beside a 10-KiB one leaves half the budget
///   unspent, and the retained total lands under a floor this arithmetic had reserved for;
/// - and it reserves conservatively, so it can demand more bytes than the render needs. **How
///   much more grows with the ENTRY COUNT, and nothing bounds what that costs.** The
///   over-reservation is about one marker width per entry plus `entries.len() - 1` for the
///   remainder the even split discards, so it scales with `n`; the schemas it pays for are sized
///   independently of `n`. A wide-but-shallow context — many small dependencies — can therefore
///   over-reserve by enough to drop several schemas that would have fitted.
///
///   An earlier revision of this sentence said the over-reservation "costs at most one further
///   dropped schema". That was a quantified claim with nothing behind it and it is false for any
///   `n` where `n × marker_width` approaches a schema's size. Recorded rather than quietly
///   deleted because it was introduced by the commit that FIXED the unit mismatch above — the
///   third time in this slice's history that a fix shipped a fresh false claim.
///
/// So a caller must handle a post-render floor failure rather than treating a plan as proof of
/// fit.
///
/// **This is the FIRST drive's planner only.** A later drive must not re-run it: its answer folds
/// in [`orchestrator_core::CONTEXT_FLOOR_FRACTION`], [`context_section_overhead`]'s reservation
/// and [`tool_bytes`], so an edit to any of them would change `dropped_tools` — and therefore
/// `system` and `tools`, and therefore `agent_input_hash` — under a run whose turn is already
/// memoized against the old answer. [`replayed_plan`] reproduces the shipped plan from the
/// journal instead, which keeps a constant the spec intends to re-tune out of the replay path.
///
/// `Err` is the refusal, and WHICH refusal is [`BudgetRefusal`]'s subject.
pub fn plan_budget(
    available_bytes: usize,
    authored_bytes: usize,
    tools: &[ToolDefinition],
    entries: &[(String, String)],
) -> Result<BudgetPlan, BudgetRefusal> {
    // The authored half is never cut (spec §5.2), so it comes off the top.
    let room = available_bytes
        .checked_sub(authored_bytes)
        .ok_or(BudgetRefusal::AuthoredOverBudget)?;
    // The least BODY bytes worth dispatching, and beside it the bytes that never reach a body.
    // Both are derived here rather than taken as arguments so they cannot describe different
    // sections — the point of computing the floor inside the planner is that the drop loop stops
    // at a budget the renderer has a chance of meeting, not at one it demonstrably cannot.
    let floor = floor_bytes(entries.iter().map(|(_, body)| body.len()).sum());
    let overhead = context_section_overhead(entries);
    let mut kept = tools.len();
    let mut dropped_tools = Vec::new();
    loop {
        let tool_total: usize = tools[..kept].iter().map(tool_bytes).sum();
        // `checked_sub`, NOT `saturating_sub`. The schemas can outweigh the whole window — the
        // repo ships a fixture where tools are ~100% of the payload (spec §3) — and a saturating
        // subtraction reports that as `0` bytes spare, which clears a floor of `0` (i.e. any
        // dependency-free agent) and returns a "plan" for a prompt that still does not fit. It
        // also underflowed `room - tool_total` on the way out. Keep dropping instead.
        //
        // The overhead subtraction IS saturating, which is the opposite choice for the opposite
        // reason: it only changes the ANSWER when the floor is `0` — with any larger floor,
        // `0 >= floor` is false and the loop keeps dropping either way — and a floor of `0` is a
        // section with no body bytes to retain. Refusing a turn because its HEADINGS do not fit
        // would be refusing over nothing; whether those survive is the renderer's own final
        // clamp to answer.
        if let Some(spare) = room.checked_sub(tool_total)
            && spare.saturating_sub(overhead) >= floor
        {
            return Ok(BudgetPlan {
                context_budget_bytes: spare,
                dropped_tools,
            });
        }
        if kept == 0 {
            // Every schema is gone and the floor still does not fit.
            return Err(BudgetRefusal::FloorUnreachable);
        }
        kept -= 1;
        dropped_tools.push(tools[kept].name.clone());
    }
}

/// Reproduce a SHIPPED plan from the journal: the schemas the writing drive dropped, verbatim,
/// and the section budget those schemas imply.
///
/// This is what makes the cut a function of journaled state in the strong sense the spec's §4.1
/// asks for. [`plan_budget`] answers "which schemas SHOULD go", and that answer depends on
/// [`orchestrator_core::CONTEXT_FLOOR_FRACTION`] — a constant whose own doc says it exists to be
/// replaced by a measurement once AC10's warn supplies one — plus [`context_section_overhead`]'s
/// reservation and [`tool_bytes`]. Re-running it on a resume therefore made every in-flight
/// budgeted run's prompt a function of the binary's arithmetic: re-tune the fraction and
/// `dropped_tools` moves, `system` and `tools` move with it, `agent_input_hash` stops matching
/// the memo, and the resume dies `DeterminismViolation` — terminal, and unrevivable because
/// `force_wake` matches only `status = 'paused'`. The executor's own version fence cannot catch
/// it: that compares a hand-set string.
///
/// So the replay asks a different question — "which schemas DID go" — and the journal already
/// answers it. What is left in the replay path is the renderer, which is irreducible: both drives
/// must render the same bytes from the same budget, and that is what
/// [`render_context_section_measured`] being pure buys.
///
/// The section budget is re-derived rather than journaled, and it is derived WITHOUT the floor or
/// the overhead: `available - authored - Σ tool_bytes(kept)` is the same expression
/// [`plan_budget`] returns as `spare` on the iteration it accepted, so the two agree by
/// construction while sharing none of the deciding arithmetic. Pinned by
/// `a_replayed_plan_reproduces_the_planners_own_answer`.
///
/// `None` — which the caller must treat as the same drift `agent_turn_output` would raise one step
/// later off the memo hash — when the journaled record cannot describe THIS activation list:
/// - more names dropped than there are schemas, or
/// - the kept/dropped split does not match the tail of the list (the registry's activation drifted
///   under the run: a renamed, reordered or removed tool), or
/// - the budget no longer covers `authored` plus the kept schemas.
///
/// The names are checked against the tail rather than merely filtered out by name, and that is the
/// difference between reproducing a cut and approximating one: [`plan_budget`] drops from the END
/// of the activation order, so `dropped_tools[i]` must be `tools[n-1-i]`. A list that merely
/// CONTAINS those names in another order describes a different prompt.
pub fn replayed_plan(
    available_bytes: usize,
    authored_bytes: usize,
    tools: &[ToolDefinition],
    dropped_tools: &[String],
) -> Option<BudgetPlan> {
    let kept = tools.len().checked_sub(dropped_tools.len())?;
    // The dropped names must BE the tail, in drop order (last activated first).
    if !dropped_tools
        .iter()
        .zip(tools[kept..].iter().rev())
        .all(|(name, tool)| name == &tool.name)
    {
        return None;
    }
    let tool_total: usize = tools[..kept].iter().map(tool_bytes).sum();
    let context_budget_bytes = available_bytes
        .checked_sub(authored_bytes)?
        .checked_sub(tool_total)?;
    Some(BudgetPlan {
        context_budget_bytes,
        dropped_tools: dropped_tools.to_vec(),
    })
}

/// The transcript's own token weight, priced by the gateway's ONE estimator over a payload
/// carrying the messages and NOTHING else.
///
/// [`available_context_bytes`] takes this figure and the window and returns the bytes the whole
/// `system` half may use, so the decomposition has to be exact: the sum this subtracts from the
/// window must be the same sum the estimator will charge for those messages inside the real
/// payload. It is a function here, rather than a probe built at the call site, so the test that
/// checks the decomposition across the crate boundary
/// (`the_byte_budget_is_the_gateway_estimators_own_arithmetic`) measures the PRODUCTION
/// probe rather than a copy of it.
///
/// Pure: `estimate_input_tokens_pessimistic` is a `match` over the payload with no clock, env or
/// state, which is what lets §4.2's purity argument survive this import.
pub fn transcript_estimate(messages: &[kernel::types::request::Message]) -> u32 {
    gateway::estimate_input_tokens_pessimistic(&kernel::types::request::Payload::Chat {
        messages: messages.to_vec(),
        // Both empty, because this figure is the COMPLEMENT of the half being budgeted: the
        // `system` string and the tool schemas are what `available_context_bytes` is sizing.
        system: None,
        max_tokens: None,
        temperature: None,
        tools: Vec::new(),
    })
}

/// The minimum retained body bytes for a turn to be worth dispatching.
fn floor_bytes(requested_context_bytes: usize) -> usize {
    (requested_context_bytes as f64 * orchestrator_core::CONTEXT_FLOOR_FRACTION).ceil() as usize
}

/// Whether an achieved cut clears the floor.
///
/// `requested == 0` is TRUE by an explicit branch, which the spec's §5.3 states as a rule of its
/// own ("evaluated only when `requested_context_bytes > 0`"): an agent with no dependencies has
/// nothing to retain and must not be refused for retaining none of it. That case is reachable on
/// any dependency-free agent node whose transcript alone crowds the window.
///
/// The branch is not defending against a division — there is no division here, only a multiply —
/// and today's arithmetic would agree with it anyway, since `0 >= floor_bytes(0)`. It is the
/// intent written down: the moment the floor grows an absolute term (a `max(fraction × requested,
/// N)`, which is the shape a measurement would most plausibly suggest), the fallthrough starts
/// refusing dependency-free turns and this branch is what keeps that from happening silently.
pub fn retained_meets_floor(requested_bytes: usize, retained_bytes: usize) -> bool {
    if requested_bytes == 0 {
        return true;
    }
    retained_bytes >= floor_bytes(requested_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use orchestrator_core::EffectClass;
    use orchestrator_core::{
        Activation, AgentBacking, AgentDefinition, Permissions, Registry, SkillDef, ToolSpec,
    };

    fn registry() -> (Registry, AgentDefinition) {
        let agent = AgentDefinition {
            name: "r".into(),
            area: "research".into(),
            kind: "reasoning".into(),
            chain: Some("research.bulk".into()),
            chains: std::collections::HashMap::new(),
            grants: std::collections::HashMap::new(),
            tools: vec!["calc".into()],
            skills: vec!["concise".into(), "cite".into()],
            system_prompt: "BODY".into(),
            backed_by: AgentBacking::Model,
        };
        let reg = Registry::default()
            .with_agent(agent.clone())
            .with_skill(SkillDef {
                name: "concise".into(),
                description: None,
                body: "SKILL_CONCISE".into(),
                activation: Activation::default(),
            })
            .with_skill(SkillDef {
                name: "cite".into(),
                description: None,
                body: "SKILL_CITE".into(),
                activation: Activation::default(),
            })
            .with_tool(ToolSpec {
                name: "calc".into(),
                description: Some("adds".into()),
                input_schema: serde_json::json!({"type":"object"}),
                effect_class: EffectClass::Pure,
                ttl_secs: None,
                source: None,
                permissions: Permissions::default(),
                activation: Activation::default(),
                credentials: vec![],
            });
        (reg, agent)
    }

    #[test]
    fn assemble_composes_body_then_skills_in_order_and_compiles_tool_schemas() {
        let (reg, agent) = registry();
        let (system, tools) = assemble_prompt(&reg, &agent, &[], "").expect("assembles");
        assert_eq!(system, "BODY\n\nSKILL_CONCISE\n\nSKILL_CITE");
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "calc");
        assert_eq!(tools[0].description.as_deref(), Some("adds"));
    }

    #[test]
    fn assemble_appends_a_context_section_only_when_present() {
        let (reg, agent) = registry();
        let ctx = vec![(
            orchestrator_core::ContextKey("A".into()),
            serde_json::json!("PRIOR"),
        )];
        let (system, _t) = assemble_prompt(&reg, &agent, &ctx, "").unwrap();
        assert!(
            system.contains("## Context") && system.contains("### A") && system.contains("PRIOR"),
            "context rendered: {system}"
        );
        // Empty context ⇒ no section (byte-identical to the no-context prompt).
        let (plain, _t) = assemble_prompt(&reg, &agent, &[], "").unwrap();
        assert!(!plain.contains("## Context"));
        assert_eq!(plain, "BODY\n\nSKILL_CONCISE\n\nSKILL_CITE");
    }

    #[test]
    fn assemble_filters_skills_and_tools_by_activation() {
        let (mut reg, mut agent) = registry();
        // Add a keyword-gated skill "gated" (body GATED_BODY) referenced by the agent.
        reg = reg.with_skill(SkillDef {
            name: "gated".into(),
            description: None,
            body: "GATED_BODY".into(),
            activation: Activation::OnKeywords(vec!["summarize".into()]),
        });
        agent.skills.push("gated".into());

        // Query hits the keyword → gated skill body present.
        let (system_hit, _t) = assemble_prompt(&reg, &agent, &[], "please summarize this").unwrap();
        assert!(
            system_hit.contains("GATED_BODY"),
            "activated skill included: {system_hit}"
        );
        assert!(system_hit.contains("SKILL_CONCISE") && system_hit.contains("SKILL_CITE"));

        // Query misses → gated skill body absent, Always skills still present.
        let (system_miss, _t) = assemble_prompt(&reg, &agent, &[], "translate to french").unwrap();
        assert!(
            !system_miss.contains("GATED_BODY"),
            "inactive skill omitted: {system_miss}"
        );
        assert!(system_miss.contains("SKILL_CONCISE"));
    }

    #[test]
    fn assemble_filters_a_gated_tool_schema() {
        let (mut reg, mut agent) = registry();
        reg = reg.with_tool(ToolSpec {
            name: "sql".into(),
            description: Some("db".into()),
            input_schema: serde_json::json!({"type":"object"}),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: Permissions::default(),
            activation: Activation::OnKeywords(vec!["query".into()]),
            credentials: vec![],
        });
        agent.tools.push("sql".into());

        let (_s, tools_hit) = assemble_prompt(&reg, &agent, &[], "run a query").unwrap();
        assert!(
            tools_hit.iter().any(|t| t.name == "sql"),
            "activated tool exposed"
        );
        let (_s, tools_miss) = assemble_prompt(&reg, &agent, &[], "hello").unwrap();
        assert!(
            !tools_miss.iter().any(|t| t.name == "sql"),
            "inactive tool hidden"
        );
        assert!(
            tools_miss.iter().any(|t| t.name == "calc"),
            "Always tool still exposed"
        );
    }

    #[test]
    fn assemble_preserves_skill_order_with_an_active_gated_skill() {
        let (mut reg, mut agent) = registry();
        reg = reg.with_skill(SkillDef {
            name: "mid".into(),
            description: None,
            body: "MID_BODY".into(),
            activation: Activation::OnKeywords(vec!["go".into()]),
        });
        // Order: concise, mid, cite  (mid is gated but active for this query)
        agent.skills = vec!["concise".into(), "mid".into(), "cite".into()];
        let (system, _t) = assemble_prompt(&reg, &agent, &[], "go now").unwrap();
        let c = system.find("SKILL_CONCISE").unwrap();
        let m = system.find("MID_BODY").unwrap();
        let t = system.find("SKILL_CITE").unwrap();
        assert!(
            c < m && m < t,
            "active skills compose in list order: {system}"
        );
    }

    /// SP-6 s3 whole-slice review: the human path's context bound is a TRUNCATION, and a
    /// truncation that panics or lies is worse than the terminal failure it replaced.
    ///
    /// Three properties in one table, because they only hold together:
    /// - the result never exceeds the budget, whatever the input;
    /// - the human is TOLD it was cut, so a clipped contract is never mistaken for a whole
    ///   one — the silent-clip failure is what makes an unmarked truncation dangerous;
    /// - a multi-byte body cannot panic. `parse_fm_duration` shipped a byte-index
    ///   `split_at` in this same slice and blew up on `48ℏ`; every cut here goes through
    ///   `floor_char_boundary` for that reason, and the `é`/`日本語` rows are what prove it.
    #[test]
    fn a_bounded_context_section_fits_its_budget_and_says_where_it_cut() {
        /// `(what the case is, the dependency outputs, the byte budget)`.
        type Case = (&'static str, Vec<(String, String)>, usize);
        let cases: Vec<Case> = vec![
            ("empty", vec![], 4096),
            (
                "one huge ascii dep",
                vec![("brief".into(), "L".repeat(50_000))],
                4096,
            ),
            (
                // The EVEN-SPLIT property is proven by its own test below, not here: these
                // three assertions (total size, heading, the word "truncated" somewhere) are
                // all satisfied by the first dependency alone, so this case can only say
                // that two deps stay within budget.
                "two deps stay within budget",
                vec![
                    ("a".into(), "A".repeat(50_000)),
                    ("b".into(), "B".repeat(50_000)),
                ],
                4096,
            ),
            (
                // Every cut lands mid-character unless the boundary is respected.
                "multibyte body",
                vec![("brief".into(), "é".repeat(20_000))],
                4096,
            ),
            (
                "multibyte body, odd budget",
                vec![("brief".into(), "日本語".repeat(20_000))],
                1_001,
            ),
            (
                // Enough dependencies that each share is smaller than the marker itself —
                // the case the final clamp exists for.
                "share smaller than the marker",
                (0..200)
                    .map(|i| (format!("dep{i}"), "X".repeat(500)))
                    .collect(),
                1_024,
            ),
        ];

        for (name, entries, budget) in cases {
            let out = render_context_section_bounded(&entries, budget);
            assert!(
                out.len() <= budget,
                "{name}: {} bytes over a {budget}-byte budget",
                out.len()
            );
            if entries.is_empty() {
                assert!(out.is_empty(), "{name}: no deps must add NOTHING");
                continue;
            }
            assert!(
                out.starts_with("\n\n## Context"),
                "{name}: the section heading survives: {out:?}"
            );
            assert!(
                out.contains("truncated"),
                "{name}: an unmarked clip lets a human answer about half a document as \
                 though it were the whole: {out:?}"
            );
        }
    }

    /// The budget is split EVENLY, and every dependency is REPRESENTED — the property this
    /// renderer exists for, and the one its doc calls load-bearing: "one verbose upstream
    /// cannot crowd the others out of the question entirely — the human is shown something
    /// from every node they were meant to consider."
    ///
    /// Re-review mutated the render loop to `entries.iter().take(1)` and the entire
    /// workspace stayed green. The case above named for this rule asserts only the total
    /// size, the section heading and that the word "truncated" appears somewhere — all three
    /// of which the FIRST dependency alone satisfies. So a human-backed reviewer with N Hard
    /// deps could have been shown only some of them, with nothing in the question saying the
    /// rest were omitted: a direct breach of §5.4's "never show the human LESS than the
    /// model would have had", and silent.
    ///
    /// Bodies are `X`/`Y`/`Z` because those are the only letters absent from every piece of
    /// boilerplate this function emits (`## Context` contributes the one uppercase `C`), so
    /// counting occurrences measures the SHARES and nothing else.
    #[test]
    fn a_bounded_context_section_splits_its_budget_evenly_across_dependencies() {
        let entries = vec![
            ("a".to_string(), "X".repeat(50_000)),
            ("b".to_string(), "Y".repeat(50_000)),
            ("c".to_string(), "Z".repeat(50_000)),
        ];
        let out = render_context_section_bounded(&entries, 4096);

        let mut shares = Vec::new();
        for (key, filler) in [("a", 'X'), ("b", 'Y'), ("c", 'Z')] {
            assert!(
                out.contains(&format!("### {key}")),
                "dependency {key} was crowded out of the question entirely: {out:?}"
            );
            let shown = out.matches(filler).count();
            assert!(
                shown >= 100,
                "dependency {key} got a heading and essentially no body ({shown} bytes), \
                 which shows the human a name and none of the thing named"
            );
            shares.push(shown);
        }

        let (lo, hi) = (
            *shares.iter().min().expect("three deps"),
            *shares.iter().max().expect("three deps"),
        );
        assert!(
            hi - lo <= 1,
            "the budget is split EVENLY, not first-come-first-served: shares were {shares:?}"
        );
        assert!(
            out.len() <= 4096,
            "and the whole section still fits: {} bytes",
            out.len()
        );
    }

    /// `truncate_prompt_to_bound` is the LAST step of the human question's durable write, so
    /// nothing downstream re-clamps what it returns.
    ///
    /// `truncate_with_marker`, which it delegates to, documents that "when `max` is smaller
    /// than the marker itself the marker wins and the result overruns; the caller's final
    /// clamp catches that". [`render_context_section_bounded`] is such a caller — its
    /// unconditional `out.truncate(..)` is that clamp. This function had NO such line, so it
    /// was the one overrun nobody caught: `truncate_prompt_to_bound("x"*1000, 10)` returned
    /// 39 bytes for a 10-byte bound. The marker is `34 + digits(shown) + digits(len)` bytes
    /// wide, so every bound below ~36 overran.
    ///
    /// Not reachable from either shipped call site today (both pass
    /// `MAX_HUMAN_TEXT_BYTES + MAX_HUMAN_CONTEXT_BYTES`, and `redact_and_clamp`'s `room`
    /// branch can only fall under ~36 if the redacted `## Task` tail reaches ~36.8 KB from a
    /// half already capped at 4096). Pinned anyway: this is a `pub` helper whose whole
    /// contract is one inequality, and the next caller inherits it.
    #[test]
    fn a_prompt_clamp_never_overruns_its_bound() {
        for max in [0usize, 1, 10, 35, 37, 39, 64] {
            let out = truncate_prompt_to_bound("x".repeat(1000), max);
            assert!(
                out.len() <= max,
                "a {max}-byte bound returned {} bytes — the marker overran the bound this \
                 function exists to enforce, and no caller re-clamps it: {out:?}",
                out.len()
            );
        }
    }

    /// The bounded renderer's doc promises "the human is shown something from every node
    /// they were meant to consider". Its final `out.truncate(budget)` can break that
    /// promise — and, as shipped, broke it SILENTLY.
    ///
    /// When a per-entry share falls below the ~36-byte marker width, `truncate_with_marker`
    /// overruns that share (the same arithmetic the test above pins), each entry overshoots
    /// a little, and the accumulated overflow makes the total clamp drop TRAILING
    /// dependencies outright. Nothing in the output says so, which is exactly the §5.4
    /// breach "never show the human LESS than the model would have had" — and the honest
    /// degradation the marker exists for, absent at the one point it matters most.
    ///
    /// The sibling table case named `"share smaller than the marker"` walks into this and
    /// cannot see it: it asserts only the total size, the heading, and that `"truncated"`
    /// appears SOMEWHERE — all three satisfied by dependency 0 alone.
    ///
    /// Either outcome is acceptable, which is why the assertion is a disjunction: show every
    /// heading, or say how many were dropped. Silence is the only failure.
    #[test]
    fn a_context_section_that_drops_dependencies_says_how_many() {
        let entries: Vec<(String, String)> = (0..200)
            .map(|i| (format!("dep{i}"), "X".repeat(500)))
            .collect();
        let out = render_context_section_bounded(&entries, 1_024);

        assert!(out.len() <= 1_024, "still bounded: {} bytes", out.len());

        let shown = (0..200)
            .filter(|i| out.contains(&format!("### dep{i}\n")))
            .count();
        if shown == 200 {
            return; // Every dependency represented — the promise held literally.
        }
        assert!(
            out.contains(&format!("of {} dependencies shown", entries.len())),
            "{} of {} dependencies were dropped by the final clamp with NOTHING in the \
             question saying so; a human answers about the nodes they can see as though \
             they were all of them: {out:?}",
            entries.len() - shown,
            entries.len()
        );
    }

    /// The un-truncated renderer is the MODEL's, and it must stay byte-identical to what
    /// `assemble_prompt` used to build inline — the s3 review split that function in two, and
    /// a drift here silently changes every model-backed agent's system prompt.
    #[test]
    fn the_model_context_section_is_unbounded_and_joins_exactly_as_before() {
        let entries = vec![
            ("brief".into(), "L".repeat(50_000)),
            ("notes".into(), "n".into()),
        ];
        let rendered = render_context_section(&entries);
        assert_eq!(
            rendered,
            format!(
                "\n\n## Context\n\n### brief\n{}\n\n### notes\nn",
                "L".repeat(50_000)
            ),
            "the model path renders every dependency in full, in order"
        );
        assert!(
            render_context_section(&[]).is_empty(),
            "and an empty context adds NOTHING, which is what keeps a no-dependency \
             agent's prompt byte-identical to the pre-blackboard one"
        );
    }

    /// A `ToolDefinition` whose ESTIMATOR-COUNTED bytes are exactly `bytes`.
    ///
    /// The estimator counts `name + description.unwrap_or("") + input_schema.to_string()`
    /// (`gateway/src/engine/util.rs:301-309`), so padding the description is what moves the
    /// figure. `input_schema: json!({})` stringifies to `{}` — two bytes — and `description` is
    /// `Option<String>`, so the arithmetic is `name.len() + pad + 2`.
    fn tool_def(name: &str, bytes: usize) -> ToolDefinition {
        let pad = bytes.saturating_sub(name.len() + 2);
        ToolDefinition {
            name: name.to_string(),
            description: Some("d".repeat(pad)),
            input_schema: serde_json::json!({}),
        }
    }

    /// The overhead subtraction saturates, and that choice only shows at a floor of ZERO.
    ///
    /// `plan_budget`'s comment argues the saturation "only changes the ANSWER when the floor is
    /// `0`" — with any larger floor, `0 >= floor` is false and the loop keeps dropping either way.
    /// That reasoning is correct and was UNPINNED: the fix commit claimed every new arithmetic
    /// term was mutation-pinned and this term was not, so the claim was true of three terms out of
    /// four. This is the fourth.
    ///
    /// The case: a context section whose headings alone exceed the available room, with no body
    /// bytes to retain (`requested == 0`, so `floor == 0`). Saturating yields `0 >= 0` and a plan;
    /// a `checked_sub` there would yield `None` and REFUSE the turn because its headings did not
    /// fit — refusing over nothing, when whether the headings survive is the renderer's own final
    /// clamp to answer.
    #[test]
    fn a_zero_floor_is_not_refused_when_only_the_overhead_does_not_fit() {
        // One entry with an EMPTY body: `requested == 0` ⇒ `floor == 0`, while the section still
        // costs `"\n\n## Context"` plus a `### k` heading. Room is deliberately under that.
        let entries = vec![("k".to_string(), String::new())];
        let plan = plan_budget(4, 0, &[], &entries)
            .expect("a zero floor must not be refused because the HEADINGS do not fit");
        assert_eq!(
            plan.context_budget_bytes, 4,
            "and the budget is the room that was actually available — the renderer's own final \
             clamp decides what survives of the headings"
        );
    }

    /// The window arithmetic: the reserve comes off the top, the transcript next, and what is
    /// left becomes bytes.
    ///
    /// What this pins is THIS crate's `× 3`, the two `None` boundaries, and — as a side effect of
    /// deriving `11_232` from `(4096 - 256 - 96) × 3` — `MIN_OUTPUT_TOKENS == 256` as a hard
    /// literal. That last one is worth naming rather than leaving as a surprise: changing the
    /// output reserve reddens this test, which is correct but will read as unrelated. An earlier
    /// revision of this paragraph said "nothing more", which was false precision of the same kind
    /// it was written to correct. It does
    /// NOT hold that multiplier to `estimate_input_tokens_pessimistic`'s divisor: the estimator is
    /// never called here, so `11_232` is a hand-derived literal and a gateway that started
    /// dividing by something else would leave this green. That claim was made here twice and was
    /// false both times; the guard actually doing that job is
    /// `the_byte_budget_is_the_gateway_estimators_own_arithmetic`, which was written for it and
    /// mutation-checked from both sides of the boundary.
    #[test]
    fn the_planner_converts_a_token_window_into_a_byte_budget() {
        // window 4096, reserve 256 for output, transcript 96 tokens ⇒ 3744 tokens ⇒ 11232 bytes.
        assert_eq!(available_context_bytes(4096, 96), Some(11_232));
        // The reserve and the transcript together exceed the window ⇒ nothing to budget.
        assert_eq!(available_context_bytes(4096, 4096), None);
        assert_eq!(
            available_context_bytes(100, 0),
            None,
            "a window under the output reserve"
        );
    }

    /// Tool schemas are dropped WHOLE and from the END of the activation order.
    ///
    /// Whole because a schema truncated mid-JSON is an invalid tool definition the provider
    /// rejects with a 400 — a degradation turned into a hard failure. From the end because that
    /// is the reverse of the order `assemble_prompt_parts` produced them in, which is the
    /// activation policy's own ranking; size- or name-ordered would be stable too but would
    /// discard that ranking.
    #[test]
    fn tool_schemas_are_dropped_whole_from_the_end_until_the_context_floor_fits() {
        let tools = vec![
            tool_def("alpha", 300),
            tool_def("beta", 300),
            tool_def("gamma", 300),
        ];
        // Room for authored(0) + one tool(300) + a 500-byte context floor and the section bytes
        // around it, and no more. The entry's BODY is derived from the fraction rather than
        // written as a literal 2000: the constant's own doc says it exists to be replaced by a
        // measurement, and a test that pinned today's 0.25 would stop exercising two drops the
        // moment it was re-tuned.
        let body = (500.0 / orchestrator_core::CONTEXT_FLOOR_FRACTION) as usize;
        let entries = vec![("A".to_string(), "z".repeat(body))];
        let plan = plan_budget(900, 0, &tools, &entries).expect("above the floor");
        assert_eq!(
            plan.dropped_tools,
            vec!["gamma".to_string(), "beta".to_string()],
            "the LAST-activated schemas go first, whole, in that order"
        );
        assert!(
            plan.context_budget_bytes >= 500,
            "and enough room is freed for the context floor: {}",
            plan.context_budget_bytes
        );
    }

    /// Schemas that outweigh the WHOLE window must keep being dropped, not budgeted around.
    ///
    /// This is the case a `saturating_sub` in the fit check gets wrong twice over, and it is
    /// reachable on the most ordinary node there is. A dependency-free agent has
    /// `requested == 0`, so its floor is `0`; a saturating subtraction reports the tool-heavy
    /// case as `0` bytes spare, `0 >= 0` clears that floor, and the arm then computes
    /// `room - tool_total` — an underflow panic in debug, a colossal budget in release, and in
    /// either case a "plan" for a prompt whose schemas alone do not fit. The repo ships a
    /// gateway fixture where tool schemas are ~100% of the payload (spec §3), so this is the
    /// shape SP-7b exists to degrade rather than a contrived one.
    #[test]
    fn schemas_heavier_than_the_window_are_dropped_rather_than_budgeted_around() {
        let tools = vec![tool_def("heavy", 500)];
        let plan = plan_budget(100, 0, &tools, &[]).expect("a dependency-free turn can still run");
        assert_eq!(
            plan.dropped_tools,
            vec!["heavy".to_string()],
            "the schema does not fit, so it goes"
        );
        assert_eq!(
            plan.context_budget_bytes, 100,
            "and the budget is the room actually left, never a wrapped subtraction"
        );
    }

    /// The refusal is reachable two ways, and **which way decides WHOSE refusal it is.**
    ///
    /// The two causes were an `Option::None` apiece and the call site read every one of them as
    /// a floor failure. That shipped a real defect: an agent whose own 100 000-byte system
    /// prompt overran a 4096-token window was told its 100-byte dependency context had failed a
    /// 25% floor, with remedies that could not work — and the SAME agent with no dependencies
    /// fell through to the gateway's accurate per-candidate diagnosis instead, so the diagnosis
    /// was non-monotonic in the dependency size. `BudgetRefusal` is what the call site now keys
    /// on; this test is what holds the two apart.
    ///
    /// `FloorUnreachable` ⇒ `requested > 0` is asserted here rather than argued in prose,
    /// because the floor pause's message depends on it: `floor_bytes(0) == 0` and the fit check
    /// clears a zero floor at `kept == 0`, so a dependency-free turn can only ever refuse as
    /// `AuthoredOverBudget`. That is what makes "only 0 survive" honest in the pause.
    #[test]
    fn the_planner_refuses_when_the_floor_cannot_be_met_at_all() {
        let tools = vec![tool_def("heavy", 500)];
        let entries = vec![("A".to_string(), "z".repeat(4_000))];
        assert_eq!(
            plan_budget(100, 0, &tools, &entries),
            Err(BudgetRefusal::FloorUnreachable),
            "dropping every schema still leaves less than the floor — SP-7b's own refusal, \
             because a FITTING cut exists at every context size and none retains enough"
        );
        assert_eq!(
            plan_budget(100, 900, &[], &[]),
            Err(BudgetRefusal::AuthoredOverBudget),
            "the authored half alone overruns the window and is never cut, so no cut fits and \
             the refusal belongs to the per-candidate gate"
        );
        assert_eq!(
            plan_budget(100, 900, &tools, &entries),
            Err(BudgetRefusal::AuthoredOverBudget),
            "authored is subtracted FIRST, so it outranks the floor: a dependency context \
             beside an over-budget authored half must not be reported as a floor failure"
        );
        assert!(
            plan_budget(100, 0, &tools, &[]).is_ok(),
            "and a dependency-free turn is never a FLOOR refusal — `floor_bytes(0) == 0`, so \
             `FloorUnreachable` implies `requested > 0`, which is what lets the floor pause \
             name a non-zero requested figure"
        );
    }

    /// A replayed plan reproduces the PLANNER's answer without re-running the planner.
    ///
    /// The equivalence is the whole contract: for the inputs the writing drive had,
    /// `replayed_plan(available, authored, tools, plan.dropped_tools)` must equal
    /// `plan_budget(available, authored, tools, entries)` — while sharing NONE of the deciding
    /// arithmetic. `replayed_plan` never touches `CONTEXT_FLOOR_FRACTION`,
    /// `context_section_overhead` or the `entries` at all, which is why a re-tune of the
    /// fraction can no longer change the prompt of a run whose turn is already memoized.
    ///
    /// The second assertion is the half that makes the first non-trivial: fed a record that
    /// dropped NOTHING at the same `available` the planner refused to keep everything at, the
    /// replay keeps everything. A `replayed_plan` that quietly re-consulted the floor could not
    /// do that.
    ///
    /// The drift cases return `None` — the caller raises the same `DeterminismViolation` the
    /// memo hash would raise one step later — and the ORDER case is the one that says why the
    /// names are checked against the tail rather than filtered out: `plan_budget` drops from the
    /// END of the activation order, so `["beta", "gamma"]` describes a prompt this list cannot
    /// produce. Mutation: replace the tail check with `dropped.contains(&t.name)` filtering and
    /// the order case starts returning a plan.
    #[test]
    fn a_replayed_plan_reproduces_the_planners_own_answer() {
        let tools = vec![
            tool_def("alpha", 300),
            tool_def("beta", 300),
            tool_def("gamma", 300),
        ];
        let body = (500.0 / orchestrator_core::CONTEXT_FLOOR_FRACTION) as usize;
        let entries = vec![("A".to_string(), "z".repeat(body))];
        let plan = plan_budget(900, 0, &tools, &entries).expect("above the floor");
        assert_eq!(
            plan.dropped_tools,
            vec!["gamma".to_string(), "beta".to_string()],
            "the premise: the planner really did drop two schemas here"
        );
        assert_eq!(
            replayed_plan(900, 0, &tools, &plan.dropped_tools),
            Some(plan.clone()),
            "and the replay reproduces that plan EXACTLY, from the journal's `dropped_tools` \
             plus `available` — no floor, no overhead, no entries"
        );
        assert_eq!(
            replayed_plan(900, 0, &tools, &[]).map(|p| p.context_budget_bytes),
            Some(0),
            "fed a record that dropped nothing, it keeps all three schemas — 900 available \
             less 3 × 300 — rather than re-deciding that the floor needs the room"
        );
        assert_eq!(
            replayed_plan(900, 0, &tools, &["beta".into(), "gamma".into()]),
            None,
            "the drop order is load-bearing: schemas go from the END, so this record does not \
             describe any prompt this activation list can produce"
        );
        assert_eq!(
            replayed_plan(900, 0, &tools, &["alpha".into()]),
            None,
            "nor does one naming a schema that is not the last"
        );
        assert_eq!(
            replayed_plan(
                900,
                0,
                &tools,
                &["d".into(), "c".into(), "b".into(), "a".into()]
            ),
            None,
            "nor one dropping more schemas than the agent activates"
        );
        assert_eq!(
            replayed_plan(100, 900, &[], &[]),
            None,
            "and a budget that no longer covers the authored half is drift too — the authored \
             text changed under the run"
        );
    }

    /// A node with NO dependencies is never refused for retaining none of them.
    #[test]
    fn a_node_with_no_context_is_never_below_the_floor() {
        assert!(
            retained_meets_floor(0, 0),
            "0 requested ⇒ the ratio is undefined, not failing"
        );
    }

    /// The floor is a fraction of the REQUESTED body bytes, and it admits EXACTLY the fraction.
    ///
    /// The figures are derived from the constant for the reason its doc gives — it is a judgment
    /// call awaiting fleet data, and pinning `250` here would make that re-tuning look like a
    /// regression. What is pinned is the boundary DIRECTION, which is not a copy of the
    /// implementation: `>` instead of `>=` reddens the first assertion, and any floor computed
    /// from a different multiple of `requested` reddens one of the two.
    #[test]
    fn the_floor_rejects_a_cut_that_keeps_less_than_the_fraction() {
        let requested = 1_000usize;
        let exactly_the_floor =
            (requested as f64 * orchestrator_core::CONTEXT_FLOOR_FRACTION).ceil() as usize;
        assert!(
            retained_meets_floor(requested, exactly_the_floor),
            "exactly the fraction is admitted"
        );
        assert!(
            !retained_meets_floor(requested, exactly_the_floor - 1),
            "a byte under it is refused"
        );
    }

    /// A budget the planner APPROVES must render at or above the floor.
    ///
    /// The two figures are in different units unless the planner is careful: the floor is
    /// measured over dependency BODY bytes, while the number the renderer takes bounds the whole
    /// SECTION — heading, per-entry `### key` headings and the truncation markers all come out of
    /// it. A planner that compares `spare` straight against the floor therefore approves a budget
    /// that renders SHORT of it, and the turn is refused after the whole plan was built.
    ///
    /// Asserted as a property over the boundary region rather than at one hand-computed budget,
    /// so it cannot be satisfied by re-deriving the implementation's own arithmetic: whatever the
    /// smallest `available` the planner accepts turns out to be, the section rendered at the
    /// budget it returns has to clear the floor. The body is `z` because the scaffolding the
    /// renderer adds around it — `## Context`, `### A`, `(truncated: N of M bytes shown)`,
    /// `(N of M dependencies shown)` — contains no `z`, so counting them counts body bytes and
    /// nothing else.
    ///
    /// The second shape is not a duplicate of the first. `budget / entries.len()` throws its
    /// remainder away, so with more than one entry an approval has to reserve for that loss as
    /// well; two 301-byte bodies put the floor on an odd number and give the division something
    /// to lose. Dropping the reservation's `entries.len() - 1` term reddens this shape ALONE —
    /// one byte per entry is exactly the size of defect a single hand-picked example misses.
    #[test]
    fn a_budget_the_planner_approves_renders_at_or_above_the_floor() {
        for entries in [
            vec![("A".to_string(), "z".repeat(1_000))],
            vec![
                ("A".to_string(), "z".repeat(301)),
                ("B".to_string(), "z".repeat(301)),
            ],
        ] {
            let requested: usize = entries.iter().map(|(_, b)| b.len()).sum();
            let floor =
                (requested as f64 * orchestrator_core::CONTEXT_FLOOR_FRACTION).ceil() as usize;
            let mut approvals = 0usize;
            for available in floor..floor + 400 {
                let Ok(plan) = plan_budget(available, 0, &[], &entries) else {
                    continue;
                };
                approvals += 1;
                let retained = render_context_section_bounded(&entries, plan.context_budget_bytes)
                    .matches('z')
                    .count();
                assert!(
                    retained >= floor,
                    "{} entries: approved {available} available ⇒ a budget of {} ⇒ only \
                     {retained} body bytes rendered, under the floor of {floor}",
                    entries.len(),
                    plan.context_budget_bytes
                );
            }
            assert!(
                approvals > 0,
                "the sweep over {} entries has to reach an approving budget or it asserts nothing",
                entries.len()
            );
        }
    }

    /// A schema is dropped when the floor needs the room the headings and markers take.
    ///
    /// The other half of the unit mismatch, and the behavioural one: dropping schemas is how the
    /// planner buys room for context, and stopping one schema early because the heading bytes
    /// were charged to the floor converts a turn that could have been DEGRADED into a refusal —
    /// exactly the outcome SP-7b exists to prevent. The schema here is sized to leave just over
    /// the floor in section bytes and just under it in body bytes, derived from the fraction
    /// rather than written as a literal so re-tuning the constant does not silently change what
    /// the case exercises.
    #[test]
    fn a_schema_is_dropped_when_the_headings_and_markers_need_the_room() {
        let entries = vec![
            ("alpha_output".to_string(), "z".repeat(200)),
            ("beta_output".to_string(), "z".repeat(200)),
        ];
        let requested: usize = entries.iter().map(|(_, b)| b.len()).sum();
        let floor = (requested as f64 * orchestrator_core::CONTEXT_FLOOR_FRACTION).ceil() as usize;
        let available = 1_000usize;
        let tools = vec![tool_def("wide", available - floor - 10)];
        let plan = plan_budget(available, 0, &tools, &entries).expect("the floor is reachable");
        assert_eq!(
            plan.dropped_tools,
            vec!["wide".to_string()],
            "the schema goes, because keeping it leaves the floor unreachable once the section's \
             own bytes are paid for"
        );
        let retained = render_context_section_bounded(&entries, plan.context_budget_bytes)
            .matches('z')
            .count();
        assert!(
            retained >= floor,
            "and the room it freed really does render the floor: {retained} of {floor}"
        );
    }

    /// The byte budget is the GATEWAY estimator's own arithmetic, asserted across the crate
    /// boundary.
    ///
    /// [`available_context_bytes`] and [`tool_bytes`] are both mirrors of
    /// `estimate_input_tokens_pessimistic` — one of its divisor, one of its tools term — and
    /// until this test nothing in the workspace held either of them to it. This closes both at
    /// once, and in the only unit that matters: a `Chat` payload whose ENTIRE counted content is
    /// one budget's worth of bytes must price at exactly the token spare that budget was computed
    /// from. Part of that content is a real tool schema and the `system` half is sized as the
    /// REMAINDER after `tool_bytes` prices it, which is what puts the tools term on trial beside
    /// the divisor: price the schema differently from the gateway and the total stops being one
    /// budget. The schema is a small share of the bytes, and that does not matter — only the
    /// difference does.
    ///
    /// `assert_eq!` rather than `<=` deliberately. `<=` would pass a divisor CHANGE in the
    /// direction that merely wastes window (a bigger divisor ⇒ the same bytes price cheaper ⇒
    /// the budget under-fills), and the equality is what pins the `× 3` from both sides without
    /// naming `3` anywhere in the test.
    ///
    /// It does NOT pin a drift smaller than the estimator's own rounding: `div_ceil(3)` maps
    /// three byte counts to one token, so a mirror one or two bytes light is invisible here. A
    /// tools term that gained or lost a whole field is not.
    ///
    /// # The second case pins the DECOMPOSITION, and only a specific class of drift can break it
    ///
    /// The plan's own self-review flagged that the transcript term — `transcript_estimate`, a
    /// payload carrying the messages and nothing else — was "a reasonable decomposition of the
    /// estimator's sum but NOT verified against `estimate_input_tokens_pessimistic`'s exact
    /// arithmetic". The second case verifies it, through the PRODUCTION function rather than a
    /// copy of the probe: measure the transcript, take it off the window, size the whole
    /// `system` half to what is left, and the full payload must price at exactly
    /// `window - MIN_OUTPUT_TOKENS`.
    ///
    /// What it catches is worth stating exactly, because the reviewer's stated hazard — "add a
    /// per-message overhead term or count `Message.attachments` and `transcript` becomes an
    /// under-estimate" — turns out NOT to be one, and asserting it here would have been a fifth
    /// false claim. `est(3T + rest) == T + est(rest)` for any `rest`, so a term the estimator
    /// charges over the MESSAGES is charged in the probe AND in the payload and CANCELS. That
    /// holds whether the term is in chars or added in tokens after the division: verified by
    /// mutation — `+ messages.len()` on the estimator's return leaves BOTH cases green.
    ///
    /// What it does catch is a DISAGREEMENT between [`transcript_estimate`] and what the
    /// estimator charges those same messages inside the real payload, in either direction. The
    /// equality is two-sided and the `system` half is sized FROM the transcript figure, so a
    /// probe `δ` tokens light leaves the payload pricing `δ` over the window and one `δ` heavy
    /// leaves it `δ` under. Mutation-verified: a `transcript_estimate` one token light reddens
    /// this case with `3841` against `3840` and leaves the first one green, because that one has
    /// no transcript to measure. That disagreement is exactly what the plan's self-review asked
    /// to be verified, and it is the failure that would silently overflow a real window.
    #[test]
    fn the_byte_budget_is_the_gateway_estimators_own_arithmetic() {
        let window = 4_096u32;
        let transcript = 96u32;
        let budget = available_context_bytes(window, transcript).expect("room to budget");
        // A schema with all three counted parts non-empty, so a tools term that stopped counting
        // any one of them shows up as a difference rather than as a zero either way.
        let tool = ToolDefinition {
            name: "fs_write".to_string(),
            description: Some("Write a file to the run's workspace".to_string()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": { "path": { "type": "string" } },
                "required": ["path"]
            }),
        };
        let schema_bytes = tool_bytes(&tool);
        let payload = kernel::types::request::Payload::Chat {
            messages: Vec::new(),
            system: Some("s".repeat(budget - schema_bytes)),
            max_tokens: None,
            temperature: None,
            tools: vec![tool],
        };
        let spare_tokens = window
            - u32::try_from(orchestrator_core::MIN_OUTPUT_TOKENS).expect("the reserve is small")
            - transcript;
        assert_eq!(
            gateway::estimate_input_tokens_pessimistic(&payload),
            spare_tokens,
            "one budget's worth of bytes must price at exactly the tokens it was budgeted from"
        );

        // The decomposition, with a real transcript: exactly what `drive_agent` does.
        let messages = vec![
            kernel::types::request::Message::text(
                kernel::types::request::MessageRole::User,
                "summarize the attached brief in two paragraphs",
            ),
            kernel::types::request::Message::text(
                kernel::types::request::MessageRole::Assistant,
                "which section should I start from?",
            ),
        ];
        let measured_transcript = transcript_estimate(&messages);
        assert!(
            measured_transcript > 0,
            "the case is vacuous with an empty transcript — that is the FIRST case"
        );
        let budget = available_context_bytes(window, measured_transcript).expect("room to budget");
        let tool = ToolDefinition {
            name: "fs_read".to_string(),
            description: Some("Read a file from the run's workspace".to_string()),
            input_schema: serde_json::json!({ "type": "object" }),
        };
        let payload = kernel::types::request::Payload::Chat {
            messages,
            system: Some("s".repeat(budget - tool_bytes(&tool))),
            max_tokens: None,
            temperature: None,
            tools: vec![tool],
        };
        assert_eq!(
            gateway::estimate_input_tokens_pessimistic(&payload),
            window - u32::try_from(orchestrator_core::MIN_OUTPUT_TOKENS).expect("small"),
            "the transcript measured by `transcript_estimate`, taken off the window by \
             `available_context_bytes`, and the `system` half filled to the result must \
             price at exactly the window less the output reserve — the identity the \
             executor's budget rests on"
        );
    }

    /// The measured renderer returns the SAME string as the existing bounded one, plus the counts.
    ///
    /// This is the no-regression half: `render_context_section_bounded` has one production caller
    /// today (the human path) and six reviewed behaviours, so the measured variant must be the
    /// same function with an extra return value, not a reimplementation.
    ///
    /// What it can catch, stated exactly, because as written it looks stronger than it is: while
    /// `render_context_section_bounded` is a one-line delegation this assertion CANNOT fail, so it
    /// proves nothing about the extraction today. It is a drift guard for the day someone gives
    /// the wrapper a body of its own — which is the shape the human path shipped in before this
    /// commit and the shape a merge conflict most easily restores.
    ///
    /// The extraction's byte-identity is established by the tests that predate it and still pass
    /// unchanged — `a_bounded_context_section_fits_its_budget_and_says_where_it_cut`,
    /// `a_bounded_context_section_splits_its_budget_evenly_across_dependencies`,
    /// `a_context_section_that_drops_dependencies_says_how_many`,
    /// `a_budget_the_planner_approves_renders_at_or_above_the_floor`,
    /// `a_schema_is_dropped_when_the_headings_and_markers_need_the_room` and the 81 `human`
    /// tests — plus `the_counts_describe_the_string_at_every_budget`, which re-checks the bound
    /// itself across 1,200 budgets on eight shapes.
    #[test]
    fn the_measured_renderer_matches_the_bounded_one_byte_for_byte() {
        let entries = vec![
            ("A".to_string(), "a".repeat(500)),
            ("B".to_string(), "b".repeat(500)),
        ];
        for budget in [50usize, 200, 1000, 5000] {
            let (measured, _cut) = render_context_section_measured(&entries, budget);
            assert_eq!(
                measured,
                render_context_section_bounded(&entries, budget),
                "the two renderers must not drift at budget {budget}"
            );
        }
    }

    /// `retained_bytes` counts BODY bytes only — not headings, not markers, not the tail.
    ///
    /// The body is `z`, not the `a` this test was drafted with, and the difference is the test
    /// itself: `truncation_marker` renders the word "trunc**a**ted", so counting `a`s counts
    /// every body byte PLUS one marker byte and the assertion failed 139 against 140 — reporting
    /// the probe's flaw as a mismatch in the figure under test. `z` is the letter this file's
    /// other body-counting tests use for exactly that reason: of the four literals the renderer
    /// emits (`\n\n## Context`, `\n\n### {key}\n`, the truncation marker and the
    /// `(N of M dependencies shown)` tail) none contains a `z`, so `matches('z')` measures bodies
    /// and nothing else.
    ///
    /// The second assertion is an independent cross-check rather than a restatement: the marker
    /// is the renderer's HUMAN-facing claim about how much it kept, and `retained_bytes` is the
    /// machine-facing one SP-7b decides its floor on. They are computed from the same `shown`, so
    /// a refactor that reported either separately shows up here. It is written for the
    /// single-entry case only, where the one marker's count IS the section total; with several
    /// entries the markers each carry their own share and no such equality holds.
    #[test]
    fn retained_bytes_excludes_headings_and_markers() {
        let entries = vec![("A".to_string(), "z".repeat(1000))];
        let (out, cut) = render_context_section_measured(&entries, 200);
        assert_eq!(cut.requested_bytes, 1000);
        assert!(
            cut.retained_bytes < 200,
            "bounded below the budget: {}",
            cut.retained_bytes
        );
        assert_eq!(
            cut.retained_bytes,
            out.matches('z').count(),
            "retained counts exactly the body bytes emitted — every 'z' and nothing else, so \
             the heading, the marker and the tail are all excluded"
        );
        assert!(
            out.contains(&format!(
                "truncated: {} of 1000 bytes shown",
                cut.retained_bytes
            )),
            "and the section TELLS its reader the same number the floor is decided on: {out:?}"
        );
    }

    /// `join_bounded` cuts the context, drops the planned schemas, and never touches `authored`.
    ///
    /// `authored` is deliberately LONGER than `context_budget_bytes`, and that sizing is what makes
    /// the last clause a claim about the code rather than about the fixture. The version of this
    /// test that shipped first used an 8-byte `authored` against the same 200-byte budget, so the
    /// bound was never in play and the assertion could not tell "never cut" from "shorter than the
    /// budget anyway": I ran both `system.truncate(plan.context_budget_bytes)` after the push and
    /// `truncate_prompt_to_bound(self.authored, plan.context_budget_bytes)` before it, and each left
    /// the whole `sensei-orchestrator` lib suite green (442 tests, as it stood). That suite is also
    /// the only one that could have caught them: `join_bounded` has no production caller until the
    /// executor is wired, so until then this test is the only thing behind the "`authored` is never
    /// cut" claim on [`PromptParts::join_bounded`].
    ///
    /// It has to be pinned HERE rather than left to a downstream check, because cutting `authored`
    /// makes the prompt SMALLER. The floor check ([`retained_meets_floor`]) is decided on
    /// [`ContextCut`], which counts context BODIES and knows nothing of `authored`, and the
    /// gateway's window gate only ever refuses a prompt for being too BIG. Neither can see bytes
    /// that went missing under the bound.
    ///
    /// The length assertion is not a restatement of the `starts_with`: it names the distinction the
    /// first mutation gets wrong — the budget bounds the `## Context` SECTION, not the joined
    /// prompt — and it says so with one legible number rather than a 700-byte string diff. Under
    /// that mutation its own claim is false (the join comes back at exactly 200 bytes, which I
    /// confirmed by evaluating it first), though in the order below the `starts_with` fires before
    /// it. Under the second mutation it PASSES: only `authored` is clamped there, so the joined
    /// prompt still runs past the budget and the `starts_with` is the assertion that fires.
    ///
    /// The remainder equality holds the rest of the composition — what follows `authored` is
    /// EXACTLY the measured section — so a dropped `push_str`, a swapped order or an added
    /// separator cannot pass either. It slices at `authored.len()`, which is safe because the
    /// `starts_with` above has already established that prefix.
    #[test]
    fn join_bounded_cuts_context_and_drops_schemas_but_never_authored() {
        let authored = format!("AUTHORED{}", "A".repeat(500));
        let context = vec![("A".to_string(), "a".repeat(1000))];
        let parts = PromptParts {
            authored: authored.clone(),
            context: context.clone(),
            tools: vec![tool_def("alpha", 100), tool_def("beta", 100)],
        };
        let plan = BudgetPlan {
            context_budget_bytes: 200,
            dropped_tools: vec!["beta".to_string()],
        };
        let (system, tools, cut) = parts.join_bounded(&plan);
        assert!(
            system.starts_with(&authored),
            "all {} authored bytes survive a {}-byte context budget: {system:?}",
            authored.len(),
            plan.context_budget_bytes
        );
        assert!(
            system.len() > plan.context_budget_bytes,
            "the budget bounds the SECTION, not the joined prompt, which is {} bytes",
            system.len()
        );
        assert_eq!(
            &system[authored.len()..],
            render_context_section_measured(&context, plan.context_budget_bytes).0,
            "and what follows the authored bytes is exactly the measured section"
        );
        assert_eq!(
            tools.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
            vec!["alpha"],
            "the planned schema is dropped WHOLE and the survivor is intact"
        );
        assert_eq!(cut.deps_total, 1);
        assert!(
            cut.retained_bytes < cut.requested_bytes,
            "and the context really was cut"
        );
    }

    // Four tests of two token estimators were here: `est_tokens_is_chars_over_four` and
    // `the_pessimistic_estimate_is_never_below_the_window_fit_one` (SP-7a, with
    // `est_tokens`), then `the_pessimistic_estimate_is_chars_over_three_rounded_up` and
    // `the_pessimistic_estimate_of_nothing_is_zero` (the serving-window review, with
    // `est_tokens_pessimistic`). This module estimates nothing now — the tombstone
    // between `render_context_section` and `over_budget`'s own says why — and
    // `gateway::engine::util::the_estimate_is_ceil_of_utf8_bytes_over_three` holds the
    // divisor, the rounding AND the empty-input boundary for the one estimator left.

    // `over_budget_true_when_estimate_exceeds_window_and_false_otherwise` was here. It
    // tested the deleted `over_budget` against a chain MINIMUM, which is the behaviour
    // SP-7a replaced rather than a property that moved. Its three cases went three ways:
    //
    // - "tiny window → over" and "large window → not over" are now
    //   `gates::context_window::over_window_skips_and_under_window_admits`, asked per
    //   CANDIDATE, so the same request gets two different answers.
    // - "unknown window (`min_context_window` → `None`) → never a hard fail" is
    //   UNREACHABLE in selection rather than relocated. The gate reads
    //   `ModelConfig.context_window`, a plain required `u32` on a candidate that has
    //   already resolved, so there is no absent-window branch for it to have. An earlier
    //   version of this comment said the case "is `no_estimate_admits`", and that was
    //   wrong: `no_estimate_admits` covers an absent ESTIMATE
    //   (`input_tokens_pessimistic: None`), which is a different question. The only
    //   surviving consumer of an OPTIONAL window is the SP-DATA-5 clamp
    //   (`executor/dispatch.rs`, `(a, b) => a.or(b)` over the chain fold), untouched by
    //   SP-7a, which is where "unknown window ⇒ no bound from here" still lives.
    //
    // Removed with the function; nothing it asserted lost its home, and the one case with
    // no home in the gateway ceased to exist rather than moving.

    /// The COUNTS describe the string, at every budget and on the degradation path.
    ///
    /// [`ContextCut`] is the whole reason the measured renderer exists, and SP-7b's floor check
    /// ([`retained_meets_floor`]) is decided on it — so a count that overstates what the section
    /// actually carries admits a turn whose context was mostly dropped, silently. The plan's
    /// single-budget test cannot see that: with the trailing-entry accounting mutated to
    /// `cut(entries.len())` (i.e. counting the dependencies the final clamp DROPPED as shown, and
    /// their bodies as retained), `retained_bytes_excludes_headings_and_markers`,
    /// `the_measured_renderer_matches_the_bounded_one_byte_for_byte` and
    /// `join_bounded_cuts_context_and_drops_schemas_but_never_authored` all stayed green. This
    /// test reddens at budget 0.
    ///
    /// Asserted against the RENDERED STRING rather than against re-derived arithmetic, which is
    /// what makes it independent of the implementation: the bodies are a single filler character
    /// absent from all four literals the renderer emits (`\n\n## Context`, `\n\n### {key}\n`,
    /// the truncation marker — note it spells "truncated", so `a` is NOT such a character and cost
    /// this test a false failure while it was being written — and the
    /// `(N of M dependencies shown)` tail), so counting occurrences counts retained bodies, and
    /// `deps_shown` is checked against the headings actually present. The renderer measuring by
    /// parsing its own output is the thing the type's doc rules out; a TEST may do it, because a
    /// test chooses its own inputs and these bodies forge nothing.
    ///
    /// The multibyte shapes are here because `retained_bytes` is in BYTES while `matches` counts
    /// CHARS: `é` is two bytes, so the product form is what holds, and a cut landing mid-character
    /// would break the equality rather than pass unnoticed.
    #[test]
    fn the_counts_describe_the_string_at_every_budget() {
        /// `(what the case is, the entries, a filler absent from the scaffolding, its byte width)`
        type Shape = (&'static str, Vec<(String, String)>, char, usize);
        let shapes: Vec<Shape> = vec![
            ("no deps", vec![], 'z', 1),
            ("one dep", vec![("A".into(), "z".repeat(1000))], 'z', 1),
            (
                "three even deps",
                (0..3).map(|i| (format!("d{i}"), "z".repeat(700))).collect(),
                'z',
                1,
            ),
            (
                "keys longer than the share",
                (0..5)
                    .map(|i| (format!("very_long_key_name_{i}"), "z".repeat(300)))
                    .collect(),
                'z',
                1,
            ),
            (
                // The degradation path: each share falls under the marker width, the section
                // overruns, and trailing dependencies are dropped WHOLE.
                "200 deps, share under the marker",
                (0..200)
                    .map(|i| (format!("d{i}"), "z".repeat(500)))
                    .collect(),
                'z',
                1,
            ),
            ("an empty body", vec![("A".into(), String::new())], 'z', 1),
            (
                "multibyte body",
                vec![("A".into(), "é".repeat(600))],
                'é',
                2,
            ),
            (
                "multibyte, two deps",
                vec![("A".into(), "é".repeat(400)), ("B".into(), "é".repeat(400))],
                'é',
                2,
            ),
        ];
        for (name, entries, filler, width) in shapes {
            let requested: usize = entries.iter().map(|(_, b)| b.len()).sum();
            for budget in 0usize..1200 {
                let (out, cut) = render_context_section_measured(&entries, budget);
                assert!(
                    out.len() <= budget || entries.is_empty(),
                    "{name} at budget {budget}: {} bytes, and the bound is the point",
                    out.len()
                );
                assert_eq!(
                    cut.requested_bytes, requested,
                    "{name} at budget {budget}: requested is the raw bodies, whatever was cut"
                );
                assert_eq!(
                    cut.retained_bytes,
                    out.matches(filler).count() * width,
                    "{name} at budget {budget}: retained disagrees with the bodies in the                      string, which is the number the context floor is decided on"
                );
                assert_eq!(
                    cut.deps_shown,
                    entries
                        .iter()
                        .filter(|(k, _)| out.contains(&format!("\n\n### {k}\n")))
                        .count(),
                    "{name} at budget {budget}: deps_shown disagrees with the headings the                      reader can actually see: {out:?}"
                );
                assert_eq!(
                    cut.deps_total,
                    entries.len(),
                    "{name}: deps_total is every dep"
                );
                assert!(
                    cut.retained_bytes <= cut.requested_bytes,
                    "{name} at budget {budget}: retained more than was asked for"
                );
            }
        }
    }
}
