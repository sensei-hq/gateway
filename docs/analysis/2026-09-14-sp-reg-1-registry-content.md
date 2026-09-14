---
title: SP-REG-1 — the orchestrator ships no registry content
doctype: analysis
module: orchestrator
slice: SP-REG-1
status: grounded
date: 2026-09-14
supersedes-scope-of: SP-7c (semantic / retrieval-ranked activation) — deferred again, see §5
---

# SP-REG-1 — the orchestrator ships no registry content

## 1. How this analysis started

The queued next slice was **SP-7c — semantic / retrieval-ranked activation**, deferred across six
surfaces since SP-7a. `/sensei:design` was invoked and stopped at Step 0: SP-7c has no analysis,
and its entire written record is one-line deferrals. Grounding the ask (Step 1) found that the
premise underneath SP-7c is not what the deferrals describe.

## 2. What grounding found — the three surprises

**2.1 SP-2's activation policy has no production user, and there is no library to activate.**

`Activation` (`orchestrator-core/src/registry.rs:228`) is `Always | OnKeywords(Vec<String>)`.
`OnKeywords` is constructed in exactly three places, all inside `#[cfg(test)]`
(`agent/prompt.rs:1063`, `:1095`, `:1123`). The only non-test construction is the frontmatter
parser at `registry.rs:957` — the mechanism, with nothing feeding it.

And there is nothing to feed it: no `import/` seed tree under `database/` (so `config_skills`
ships empty), and no on-disk registry root anywhere in the repo (`.claude/skills` is Claude
Code's own; `docs/skills` is two site pages that the site build copies).

SP-2 slice 4 justified activation as progressive disclosure so *"a large shared skill/tool
library [can] attach to an agent without every request paying for all of it"*. **That library
does not exist.**

**2.2 A shipped feature is inoperable for the same reason — and this is the real finding.**

`Executor::planner_candidates` (`executor/mod.rs:1343`) selects planner agents by
`area == PLANNER_AREA` (`"planning"`, `orchestrator-core/src/planner.rs:21`). With no registry
content there are none, so `PlannerRef::Select` takes the failure path at
`executor/expand.rs:169` — `"expand {}: no planner agents (area==planning)"`.

SP-3 slice 4B built the planner selector, `RulePlannerSelector` and `LlmPlannerSelector`, the
capability menu and the anti-hallucination `∉candidates` check. **None of it can run**, because
the set it selects from is empty by construction. This is not a latent hole like SP-7a.1's; it is
a feature with no inputs.

**2.3 "SP-7c needs embedding infrastructure" is mostly false; what is missing is storage.**

`Capability::TextEmbed` exists (`kernel/src/types/capability.rs:9`), adapters implement `embed`,
and the orchestrator already dispatches `Payload::Embed` (`orchestrator/src/test_support.rs:259`,
the payload the SP-DATA-5 clamp deliberately skips). Embedding is reachable today.

What is genuinely absent is **vector storage** — no pgvector, no vector column anywhere in
`database/`.

## 3. The determinism constraint, recorded for whenever SP-7c runs

`agent_input_hash(chain, system, messages, tools)` (`executor/support.rs:601`) hashes the
**post-activation** `system` and `tools`. Activation output is therefore already inside the replay
key, and a ranking produced by a live model call would change the hash between drives →
`DeterminismViolation` on resume.

**Decision taken (user, 2026-09-14): journal the selected SET, not the ranking.** The direct
analogue of SP-7b's "journal the BUDGET, not the cut" — the first drive ranks and records which
skills won; every replay reads the set; assembly stays pure. The alternative considered was
embed-at-config-load with pure cosine at request time, which needs vector storage and a
re-embed-on-config-change story. Recorded here so SP-7c's design does not re-litigate it.

## 4. What this slice is

**Ship real registry content**, so the mechanisms that consume it have inputs.

The on-disk contract already exists and is the target
(`orchestrator-store/src/config_source.rs:24`):

- `<root>/agents/*.md` — frontmatter (`name`, `area`, `kind`, `tools`, `skills`, `chain`,
  `grants`) + body = `system_prompt`
- `<root>/skills/*.md` — frontmatter (`name`, `description`, `activate_on: [kw, …]`) + body
- `<root>/tools/*.json` — `ToolSpec`

`activate_on` as a list → `OnKeywords`; absent → `Always`; a scalar is a loud parse error
(`registry.rs:955`).

### Done gate (observable)

1. At least one agent with `area: planning` exists, so `planner_candidates()` is non-empty and
   `PlannerRef::Select` reaches a planner instead of `expand_failed`.
2. At least one skill declares `activate_on`, and a test drives `assemble_prompt` proving the body
   is composed in for a matching input and absent for a non-matching one — i.e. `OnKeywords` is
   exercised by something that is not a unit-test literal.
3. `FilesystemConfigSource::load` over the shipped root parses with zero errors.
4. `torii config push` of that root, then `config pull`/`diff`, round-trips unchanged.
5. The full suite stays green and byte-identical for runs that do not use the registry.

### Wrong gate — how this passes and is still wrong

- **The content is fixture-shaped.** Skills authored only to satisfy the done gate teach nothing
  about whether progressive disclosure helps. The keyword sets must be ones a real prompt would
  hit.
- **Agents exist but nothing routes to them.** `area`/`kind` must match a `ChainBinding`, or
  `resolve_chain` fails and the agent is decorative.
- **It ships a second source of truth.** If the fs root and the Postgres `config_*` tables can
  disagree, SP-DATA-2's config-generation fence is being worked around rather than used.

## 5. SP-7c after this

Deferred again, deliberately, and now for a stated reason rather than by inertia: retrieval
ranking is an optimisation over a library, and measuring whether it beats `OnKeywords` requires a
library and a baseline. SP-REG-1 produces both. §3 records the determinism decision so SP-7c
starts from it.

## 6. Open — must be answered before design

**What the library should contain** is a product decision this analysis cannot make from the code.
The roles the code already expects are `area: "planning"` (planner selector), and
`area: "research" / kind: "reasoning"` (used in `plan.rs:270` and `torii/diff.rs:183` fixtures).

## 7. Not yet run

The Step 4 depth check (`sensei-analyst`, `sensei-plan-depth-reviewer`,
`sensei-persona-reviewer`) has **not** been run against this analysis. It is the next step, and
§6 should be closed first — a depth review of an analysis with an open content question would
report that question and little else.

## 8. Method note

The sensei daemon was not running, so the MCP code-graph tools (`search`, `get_callers`) were
unavailable and every claim above was derived with ripgrep over `crates/`, `database/` and
`docs/`. "Nothing calls this" is exactly the claim those tools answer better; the absolutes in
§2.1 and §2.2 should be re-checked with the graph when the daemon is up.
