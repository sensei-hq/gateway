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
3. `FilesystemConfigSource::load` over the shipped root parses with zero errors **and
   `Registry::from_config` then ASSEMBLES it** — parsing alone proves less than it sounds like,
   because `load()` does frontmatter/JSON only; chain resolvability, dangling skill/tool refs and
   duplicate detection all live in `from_config` (`registry.rs:428-467`).
4. `torii config push` of that root succeeds, and an immediate second push of the same root reports
   `PushDecision::NoOp` — i.e. the durable write round-trips. **Stated in terms of `push` alone
   because `torii config` has exactly two subcommands, `Version` and `Push` (`main.rs:300-314`):
   there is no `config pull` and no standalone `config diff`. An earlier draft of this item named
   both, which a builder could not have executed.**
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

## 6. Ownership — and a premise that turned out to be already satisfied

Raised 2026-09-14: *"this is a generic gateway utility, so the bundled agents/skills should be for
that purpose; the registry should be handled by torii rather than gateway; gateway should not
assume anything but receive the agents/skills/tools for execution."*

**Two of those three are already how it is built**, verified rather than assumed:

| Crate | Knows about agents/skills/tools? |
|---|---|
| `crates/gateway` | **No.** `rg 'AgentDefinition\|SkillDef\|Activation::' crates/gateway/src/` → 0; its `Cargo.toml` has no orchestrator dependency at all. It models routers, chains, selection, adapters. |
| `orchestrator-core` | The TYPES + the `ConfigSource` **trait** (`registry.rs:277`) — a seam, not a backend. |
| `orchestrator-store` | The backends: `FilesystemConfigSource`, `PostgresConfigSource`. |
| `orchestrator` | `Executor::with_registry_handle` (`executor/mod.rs:837`) — injected, `Option`, default `None`. |
| `torii` | The durable write path: validate → diff → push (`PushDecision::{NoOp, Apply, NeedsConfirmation}`). |

So "gateway must receive rather than assume" already holds — gateway never sees a registry, and the
executor takes a handle rather than constructing one. "torii should handle the registry" already
holds for the write path. The types must stay in `orchestrator-core` because torii depends on core
and not the reverse.

**What was genuinely missing is content and a layering story**, which is this slice.

### Decisions (user, 2026-09-14)

**D1 — the shipped content serves the TOOLKIT's own purpose**, not a domain. At minimum an
`area: planning` agent, because without one `PlannerRef::Select` is inoperable (§2.2).

> **⚠️ Corrected by the depth check (2026-09-14), and this one is embarrassing.** An earlier draft
> added: *"Other roles the code already expects: `area: "research" / kind: "reasoning"`
> (`plan.rs:270`, `torii/diff.rs:183`)."* **All three reviewers independently refuted it.** Every
> occurrence of those strings is inside a `#[cfg(test)]` fixture constructor (`fn agent_reg()`,
> `fn agent(name)`); no production path filters or dispatches on them. Contrast `PLANNER_AREA`,
> which `executor/mod.rs:1347` genuinely reads. **`"planning"` is the only load-bearing role
> string in the codebase.** The claim was the same evidentiary trap §2.1 of this very document
> disqualifies — test-only construction cited as production expectation — and it was being used to
> justify shipping content beyond the one role that is actually required.

**What content beyond the planner agent ships is deliberately NOT decided here** — see §6.1.

**D2 — built-ins live at `crates/torii/registry/`.** torii owns the management surface and the
write path, so the baseline content sits beside it. The orchestrator stays content-free.

**D3 — layered sources, override by name.** A defaults source composed with the implementer's
source; on a name collision the later layer wins, so an implementer gets working defaults free,
can override any single one, and never forks the whole set.

**D4 — the merge happens BEFORE the durable write.** `torii config push` composes the built-in
defaults root with the implementer's root, merges, validates, and writes the MERGED result. The
depth check found D3 had no route to production otherwise: `ConfigAction::Push { dir: PathBuf }`
(`main.rs:308`) takes ONE directory and is **replace-all**, and the executor boots from a single
`PostgresConfigSource` (`torii/boot.rs:371`). So an implementer pushing only their override
directory would **wipe the defaults** — precisely the "fork the whole set" outcome D3 exists to
prevent. The rejected alternative was composing at executor boot: that leaves the fs defaults and
the Postgres tables free to disagree, which is the "second source of truth" failure §4's Wrong
Gate already names, and it would hollow out SP-DATA-2's config-generation fence. Merging before
the write keeps exactly one durable source of truth.

**D5 — a push that would leave zero planners warns and requires confirmation.** The depth check
found override-by-name is otherwise a **trap that re-opens the very bug this slice closes**: an
override keeping the built-in planner's NAME but changing `area` passes silently, because
`Registry::validate` has no planner invariant (`rg 'PLANNER_AREA|"planning"' registry.rs` → 0),
`diff::compare` keys agents by name so an `area` edit is classified `changed` not `removed`, and
`ConfigDiff::requires_confirmation` is `!self.removed.is_empty()` (`diff.rs:42-44`) — so it
applies **unprompted**. The failure then surfaces much later as `expand_failed("no planner agents
(area==planning)")`, disconnected from the push that caused it.

So `describe_diff` gains a check: if the push would leave zero `area: planning` agents, force
confirmation and name the consequence. A hard invariant in `Registry::validate` was rejected — a
registry with no planner is legitimately legal (you simply cannot use `PlannerRef::Select`), and
making it illegal would redden the many existing test registries that have none. Reserving
built-in names as non-overridable was rejected as removing the flexibility D3 was chosen for.

**Also unguarded, and NOT closed by D5 — record it for the design:** an implementer who *adds* a
planner rather than overriding one gets a silent competitor. `RulePlannerSelector::select` falls
back to `candidates.first()` sorted by name, so the built-in can win alphabetically; and
`LlmPlannerSelector` puts every `area == planning` candidate in one menu, so the built-in competes
for every plan. Neither is covered by the done gate.

### The constraint D3 has to respect, found while checking it

`Registry::from_config` (`registry.rs:428`) **rejects duplicates loudly** —
`OrchestratorError::RegistryLoad("duplicate agent: {name}")`, pinned by
`from_config_assembles_validates_and_rejects_duplicates` (`registry.rs:1289`).

So D3 **cannot** be implemented by concatenating two `RegistryConfig`s and calling `from_config`:
that errors on every intentional override. The merge must happen at `RegistryConfig` level
**before** `from_config`, giving the rule:

- **within one source** — a duplicate name stays a loud error (it is an accident: two files
  defining the same agent);
- **across layers** — the later layer wins (it is intent).

That preserves the existing guard rather than weakening it. No composing `ConfigSource` exists
today (`rg 'Chained|Layered|Composite|Overlay'` over the config sources → nothing), so it is new
code.

**The merge key, per collection — stated, because an earlier draft only flagged it.**
`RegistryConfig` is four `Vec`s:

| collection | merge key | override rule |
|---|---|---|
| `agents`, `skills`, `tools` | `name` | later layer replaces the earlier entry with the same `name` |
| `chain_bindings` | **`(area, kind)`** | later layer replaces the earlier entry with the same `(area, kind)` tuple |

`chain_bindings` has no `name` field, so it is the collection a name-keyed merge would silently
*concatenate* — and `Registry::from_config` rejects a duplicate `(area, kind)` just as loudly as a
duplicate name (`"duplicate chain binding: {area}/{kind}"`, `registry.rs:457-465`, pinned by
`duplicate_area_kind_in_chains_json_is_rejected_by_from_config`). So the natural oversight —
merge by name, concatenate the rest — would pass every other gate item while reintroducing
"every intentional override is a loud error" for bindings specifically.

**Where intra-layer dedup lives.** Today duplicate detection exists *only* in
`Registry::from_config`; `FilesystemConfigSource::load` does none. Since D4 merges before
`from_config` runs, the "within a layer this is a loud error" half needs its own per-layer pass in
the merge code — it cannot be inherited from `from_config`, which by then sees one merged config
in which the collisions have already been resolved.

### Added to the done gate by D1–D5

6. A defaults layer and an implementer layer, both present, where the implementer's definition of a
   colliding **name** is the one the `Registry` resolves — and a duplicate WITHIN either layer is
   still a loud `RegistryLoad` error, raised by the merge's own per-layer pass.
7. The same, keyed on **`(area, kind)`**, for `chain_bindings` — asserted separately, because this
   is the collection a name-keyed merge silently concatenates.
8. Overriding a built-in requires no fork: the implementer supplies one file, pushes, and the
   merged durable config contains their version plus every un-overridden default.
9. A push whose merged result would contain zero `area: planning` agents requires confirmation and
   says why (D5). Asserted through `plan_push`/`describe_diff`, not by inspecting the diff struct.

## 6.1 Still open — the minimum content list

D1 fixes the *principle* (toolkit purpose, not domain) and the *one required agent*
(`area: planning`). It does **not** name the rest: how many skills, their actual `activate_on`
keyword sets, and whether any `tools/*.json` ship at all — even though `<root>/tools/*.json` is
part of the on-disk contract in §4.

This is deliberately deferred to `/sensei:design` rather than guessed here, but it is a real
blocker for gate item 2, whose own Wrong Gate demands keyword sets "a real prompt would hit"
rather than fixture-shaped ones. **Design must produce a committed list.**

One constraint design inherits: the registry's `tools/*.json` declares the model-facing `ToolSpec`
**schema**, while the executable tool lives in the orchestrator (`agent/tools.rs` ships `fs_read`,
`fs_write`, `shell`). A shipped schema whose name has no executable counterpart is a tool the model
can call and the runtime cannot serve.

## 6.2 A persona gap, upstream of this slice

`README.md`'s **Crates table** (lines 7-13) lists exactly five: `kernel`, `cloud-providers`,
`gateway`, `local-providers`, `local-engine`. None of `orchestrator-core`, `orchestrator`,
`orchestrator-store` or `torii` appears in it, despite all four being workspace members with a
large feature surface. (`torii` is named three times elsewhere in the README, but only inside test
invocations such as `cargo test -p sensei-torii --test e2e_pg` — it is documented as something to
run in CI, not as something to consume.) SP-REG-1 writes a public extension contract
(layered sources, override semantics, a done gate framed around "an implementer") for a
consumption path the README does not yet acknowledge. That does not make the mechanism wrong, but
the audience is currently self-declared by this analysis rather than documented by the repo. Worth
naming in the design, or fixing in the README.

## 7. Depth check — RUN 2026-09-14, all three reviewers

`sensei-analyst`, `sensei-plan-depth-reviewer` and `sensei-persona-reviewer`, launched in parallel
and blind to each other. Verdict: **not-ready**. Ten findings, every one landing on §6 — the
section added last and never checked until now. All were verified by hand before acting.

| # | Finding | Found by | Closed |
|---|---|---|---|
| B | D3 had no route through the durable write path; `push` is replace-all | analyst + depth | **D4** |
| E | Override-by-name silently disables planner selection, unprompted | persona | **D5** |
| D | `research/reasoning` cited as production expectation — test-only | **all three** | D1 struck |
| A | Gate item 4 named `config pull`/`diff`; neither subcommand exists | analyst + depth | §4 item 4 |
| C | `chain_bindings` merge key noted, not answered | analyst + depth | §6 table, gate 7 |
| J | Intra-layer dedup had no owner | depth | §6 "Where intra-layer dedup lives" |
| I | No committed minimum content list | depth | §6.1 (scoped to design) |
| F | "Add, don't override" leaves a silent competing planner | persona | recorded in D5 |
| G | README documents no orchestrator/torii crate — persona undocumented | persona | §6.2 |
| H | Gate item 3 read stronger than it was (`load` ≠ assemble) | depth | §4 item 3 |

**Re-run required.** Per the design stage's own rule, a rewrite that asserts something new about
existing code goes back through verification — and D4, D5 and the §6 merge-key table all do. The
depth check should be re-run against this revised document before `/sensei:design`.

## 8. Method note

The sensei daemon was not running, so the MCP code-graph tools (`search`, `get_callers`) were
unavailable and every claim above was derived with ripgrep over `crates/`, `database/` and
`docs/`. "Nothing calls this" is exactly the claim those tools answer better; the absolutes in
§2.1 and §2.2 should be re-checked with the graph when the daemon is up.
