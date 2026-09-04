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

/// Render the `## Context` section exactly as the model receives it: no bound, no
/// truncation. An empty `entries` renders the EMPTY STRING, which is what keeps a
/// no-dependency agent's prompt byte-identical to the pre-blackboard prompt.
pub fn render_context_section(entries: &[(String, String)]) -> String {
    if entries.is_empty() {
        return String::new();
    }
    let mut out = String::from("\n\n## Context");
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

/// Truncate `s` to at most `max` bytes, appending a marker that says so.
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
fn truncate_with_marker(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let marker = |shown: usize| format!("\n… (truncated: {shown} of {} bytes shown)", s.len());
    // The marker's own length depends on `shown`, so budget with the widest it can be
    // (`shown` can never exceed `max`) and then re-render with the real number: fewer digits
    // only ever makes it shorter, so the total cannot grow past `max`.
    let widest = marker(max).len();
    let shown = floor_char_boundary(s, max.saturating_sub(widest));
    format!("{}{}", &s[..shown], marker(shown))
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
    let mut out = truncate_with_marker(&text, max);
    out.truncate(floor_char_boundary(&out, max));
    out
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
/// chain — true since SP-7a on an UNBUDGETED run, and the reason that slice exists; before
/// it, the orchestrator refused such a call outright against the chain's smallest window —
/// whereas a human-backed node that fails takes the whole run terminal AFTER the upstream
/// tokens have been spent.
///
/// The unbudgeted qualifier is load-bearing rather than pedantic: on a run with a token
/// cap the SP-DATA-5 clamp (`executor/dispatch.rs`) still bounds by
/// `min_context_window(chain)` BEFORE selection happens and refuses with a durable pause,
/// so the fall-through never occurs there. See the SP-7a spec's §8 and
/// `a_budgeted_run_is_refused_by_the_clamp_before_the_window_gate_is_asked`.
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
pub fn render_context_section_bounded(entries: &[(String, String)], budget: usize) -> String {
    if entries.is_empty() {
        return String::new();
    }
    const HEAD: &str = "\n\n## Context";
    let share = budget.saturating_sub(HEAD.len()) / entries.len();
    let mut out = String::from(HEAD);
    // Where each entry ENDS, recorded as it is written. This is what lets the degradation
    // below report an EXACT count and cut on an entry boundary; recomputing it afterwards
    // would mean re-parsing the very headings a dependency's own body is free to forge.
    let mut ends = Vec::with_capacity(entries.len());
    for (key, body) in entries {
        let head = format!("\n\n### {key}\n");
        // The heading is what tells the human WHICH dependency this is, so it is never the
        // thing truncated; only the body competes for what is left of this entry's share.
        let room = share.saturating_sub(head.len());
        out.push_str(&head);
        out.push_str(&truncate_with_marker(body, room));
        ends.push(out.len());
    }
    if out.len() <= budget {
        return out;
    }
    // Budget with the WIDEST count the marker can carry (`shown` can never exceed the total)
    // and re-render with the real one: fewer digits only ever makes it shorter, so the total
    // cannot grow past `budget` — the same arithmetic `truncate_with_marker` uses.
    let omitted = |shown: usize| format!("\n\n… ({shown} of {} dependencies shown)", entries.len());
    let ceiling = budget.saturating_sub(omitted(entries.len()).len());
    let shown = ends.iter().take_while(|end| **end <= ceiling).count();
    // `HEAD.len()` rather than 0 when not even the first entry fits: the section heading is
    // what makes the remaining line legible as a statement about context at all.
    out.truncate(if shown == 0 {
        HEAD.len()
    } else {
        ends[shown - 1]
    });
    out.push_str(&omitted(shown));
    // The unconditional clamp, kept. Everything above is best-effort shaping; this is the
    // line that makes "the journaled question is bounded" true no matter how many
    // dependencies, how long their keys, or how the marker arithmetic lands.
    out.truncate(floor_char_boundary(&out, budget));
    out
}

// `est_tokens(s) = s.chars().count() / 4` lived here until SP-7a's review. It was the
// documented prose heuristic the window pre-check ran on, and SP-7a deleted both of its
// callers (`over_budget` and `executor::support::est_prompt_tokens`) along with the halt
// they served — after which it survived only because `pub` in a `pub mod` makes it
// invisible to `dead_code`, so `clippy -D warnings` could not report what it had become.
//
// Kept for one commit as "the prose baseline `est_tokens_pessimistic`'s bias is defined
// against", and deleted now for the reason that commit itself gave for deleting
// `over_budget`: a function kept because it might be wanted again is exactly the kind of
// thing that gets called again by mistake — and this one is the `chars/4` UNDER-count the
// window check's whole failure history came from. The baseline argument did not survive
// contact either: `est_tokens_pessimistic` is `chars/3` outright rather than a multiplier
// on it, deliberately, so the two were already independent.
//
// Nothing lost coverage. `the_pessimistic_estimate_is_never_below_the_window_fit_one`
// compared the two functions; `the_pessimistic_estimate_is_chars_over_three_rounded_up`
// bounds `est_tokens_pessimistic` from BOTH sides against the raw character count, which
// implies the comparison for every input, and it now states the prose figure as an
// absolute so the claim survives without the function.

/// A deliberately pessimistic token estimate, for the BUDGET path only.
///
/// `chars / 4` is the standard rough figure for English prose. The orchestrator's prompts
/// are not mostly English prose: they carry JSON tool schemas and a `## Context` section
/// rendered from upstream outputs, and JSON tokenizes nearer 3 chars/token. So `chars / 4`
/// UNDER-counts precisely where these prompts are heaviest.
///
/// A window-fit estimate and a budget estimate want OPPOSITE biases, which is why the
/// prose figure was never simply widened. A window check asks "will this prompt fit",
/// where an over-count refuses a candidate that would in fact have held the request —
/// under SP-7a a SKIP, and only a failure when it skips the last candidate in the chain;
/// under the chain-minimum halt this function used to serve, a failed node outright. The
/// budget asks "what is the worst this call can cost", and an under-count there is the
/// expensive direction: the clamp computes `max_tokens = remaining − est`, so too low an
/// estimate leaves too high an allowance and the cap is overshot by the difference.
///
/// The window half of that pair is no longer here at all. SP-7a moved the question to the
/// gateway, which owns its own estimator — `engine::util::estimate_input_tokens_pessimistic`,
/// over BYTES rather than chars and over a whole `Payload` rather than a `&str`, and a
/// separate function because the gateway crate cannot depend on this one (the dependency
/// runs the other way). So this function is the orchestrator's only token estimate now,
/// and the only caller is `executor::dispatch::est_input_tokens`.
///
/// So this one inverts the bias — it is wrong toward refusing early rather than toward
/// overspending. It does not make the overshoot zero (the clamp design's §4 writes out the
/// arithmetic); it bounds it by the remaining estimate error.
///
/// Not a real tokenizer — a real one needs a per-model vocabulary and a per-chain mapping,
/// and would still need a heuristic like this as its fallback for an unknown model, so it
/// is additional work on top of this rather than instead of it.
///
/// # The pessimism assumes a LATIN script
///
/// Three characters per token is an over-count only where a token is worth three or more
/// characters, which is Latin-script text — the case both this and `est_tokens` were
/// calibrated on. CJK, Cyrillic and emoji run nearer 1–3 tokens PER character, so on such
/// a prompt this UNDER-counts by a multiple and the bias flips: the clamp's residual
/// (`actual_input − est_input`, the clamp design's §4) stops being a small error and
/// becomes a fraction of the prompt.
///
/// That is stated rather than fixed because the fix is the deferred real tokenizer, and a
/// heuristic cannot be made script-agnostic by choosing a different divisor — a divisor
/// pessimistic enough for CJK would refuse most Latin prompts outright. The AC11
/// `budget clamp under-estimated the input` warning is the mitigation, and a non-Latin
/// prompt is precisely the case it is expected to fire on.
///
/// Rounds UP, so any non-empty text costs at least one token, and `""` costs none.
pub fn est_tokens_pessimistic(s: &str) -> usize {
    s.chars().count().div_ceil(3)
}

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

    // `est_tokens_is_chars_over_four` and
    // `the_pessimistic_estimate_is_never_below_the_window_fit_one` were here. Both tested
    // `est_tokens`, deleted with its last caller; the second compared the two estimates
    // to each other, and the test below subsumes it — bounding `est_tokens_pessimistic`
    // from both sides against the raw character count implies "at least the prose figure"
    // for every input, and does so without needing the prose figure to exist as code. The
    // subsumption is made explicit in that test's last assertion rather than left to be
    // re-derived.

    /// The DIVISOR, pinned from both sides — an ordering against any other estimate is
    /// not enough.
    ///
    /// `div_ceil(3)` → `div_ceil(4)` is a one-character edit that removes essentially
    /// all of the margin this function exists for, and it still satisfies "strictly
    /// greater than the `chars / 4` prose figure" on any length not divisible by 4,
    /// because rounding up beats flooring by a token. So the sign of a difference proves
    /// nothing about its SIZE, and the size is the whole point: JSON tokenizes nearer 3
    /// chars/token, and clamping on a `chars / 4` under-count overshoots the cap by the
    /// error.
    ///
    /// Both bounds are needed. Without the first, the divisor can grow and the margin
    /// silently evaporates. Without the second, it can shrink — `chars / 2` would pass
    /// every other assertion here while charging a budgeted run double for its input and
    /// refusing calls that would comfortably have fitted.
    #[test]
    fn the_pessimistic_estimate_is_chars_over_three_rounded_up() {
        let json = r#"{"name":"fs_write","parameters":{"type":"object","properties":{"path":{"type":"string"},"contents":{"type":"string"}},"required":["path","contents"]}}"#;
        for s in [
            json,
            "The quick brown fox jumps over the lazy dog.",
            "",
            "a",
            "ab",
        ] {
            let n = s.chars().count();
            let est = est_tokens_pessimistic(s);
            assert!(
                est * 3 >= n,
                "the estimate must be at least chars/3 for {s:?}: {est} * 3 < {n}"
            );
            assert!(
                est * 3 <= n + 2,
                "and no more than chars/3 rounded up — an over-count is a tax on every \
                 budgeted call, not free caution — for {s:?}: {est} * 3 > {n} + 2"
            );
            // The property the deleted `the_pessimistic_estimate_is_never_below_the_
            // window_fit_one` asserted, restated without needing the deleted `est_tokens`
            // to exist: this estimate is never below the `chars / 4` prose figure the
            // orchestrator's window check used to run on. Implied by the lower bound
            // above, and written out because "implied" is not something a future reader
            // should have to re-derive before touching the divisor.
            assert!(
                est >= n / 4,
                "and never below the chars/4 prose figure it replaced for {s:?}: {est} \
                 < {}",
                n / 4
            );
        }
    }

    /// The empty string costs nothing under either estimate.
    ///
    /// The boundary is worth its own test because the clamp subtracts this value:
    /// rounding UP is right for every non-empty input and wrong for the empty one, so
    /// a `div_ceil` on a length that had been nudged (a `+1`, a `max(1)`) would charge
    /// a token for a system prompt that is not there.
    #[test]
    fn the_pessimistic_estimate_of_nothing_is_zero() {
        assert_eq!(est_tokens_pessimistic(""), 0);
    }

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
}
