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

> **⚠️ D3 and D4 are SUPERSEDED by D6, after the second depth check.** They said: layered sources
> with the later layer winning on a name collision (D3), merged before the durable write by
> `torii config push` (D4). The reviewers showed that merging defaults in on **every push** —
> which D4 required — produces three separate defects with one root cause:
>
> 1. **No deletion.** "Later layer wins" has no tombstone, so an implementer can never remove a
>    built-in they do not want — only override its name with inert content that still validates.
>    D3 was chosen to avoid forking the set, and this forced exactly that.
> 2. **A torii upgrade silently mutates the implementer's durable config.** If a release changed
>    `crates/torii/registry/`, the next routine CI push of an *unedited* directory becomes a
>    non-noop, bumps the generation, and applies without confirmation (`plan_push` gates only on
>    `!removed.is_empty() || paused_runs > 0`). `torii config version` reports a bare `u64` with no
>    record that the cause was a binary upgrade rather than an edit — and **any successful push
>    terminally kills every already-journaled paused run** (`cmd/config.rs:30-39`).
> 3. **No opt-out.** The merge was unconditional, so a deployer whose requirement is "the durable
>    registry contains only what I reviewed" could not meet it. Before this slice there was no
>    shipped content at all, so D4 would have left that persona **worse off than the status quo** —
>    in a repo whose whole SP-4 model is an auditable ceiling of trust.

**D6 — seed-once, not merge-always.** A new `torii config init <dir>` writes the built-in defaults
into the implementer's OWN directory, once. From then on those files are theirs: edit them, delete
them, review them in their own VCS, push them with the `torii config push` that already exists.

This is a large simplification, not merely a different choice. It deletes the need for:
a composing `ConfigSource` (none exists — would have been new code), the per-collection merge-key
table, the per-layer-vs-cross-layer dedup ordering, a tombstone syntax, a `--no-defaults` flag,
and a generation-bump provenance record. `torii config push` needs **no change at all**: it stays
one directory, replace-all, and `Registry::from_config`'s loud duplicate rejection stays exactly
the guard it has always been rather than something the merge has to work around.

The cost, stated plainly: an implementer does not get upstream improvements to the defaults for
free. They re-run `init` into a scratch directory and diff. That is the trade for owning their own
registry outright, and it is the right way round — the alternative made their durable config a
function of which binary happened to run the push.

**D7 — the defaults are compiled into the `torii` binary** (`include_dir!` / `rust-embed`). D2
puts them at `crates/torii/registry/`, which is a SOURCE-tree path: a `cargo install`ed or
containerised `torii` has no such directory, so `config init` could not find them. Embedding works
identically from a checkout, an install, or a container, with nothing to ship alongside and
nothing to misconfigure. Neither dependency is present today (`crates/torii/Cargo.toml` has no
`rust-embed`/`include_dir`, and `torii/build.rs` is about `DATABASE_URL` cfg, unrelated), so this
is a new dependency. Refreshing the defaults requires a rebuild, which is correct: they are part
of the binary's contract.

**D5 (revised) — the zero-planner gate lives in `plan_push`, NOT `describe_diff`.** Still needed
under D6, because an implementer can still edit the seeded planner agent's `area` and silently
disable planner selection: `Registry::validate` has no planner invariant, `diff::compare` keys
agents by name so an `area` edit classifies as `changed` not `removed`, and
`ConfigDiff::requires_confirmation` is `!self.removed.is_empty()` (`diff.rs:42-44`) — so it
applies unprompted and fails much later as `expand_failed("no planner agents (area==planning)")`.

> **⚠️ An earlier draft put this check in `describe_diff`, and that was wrong.** `describe_diff` is
> a pure renderer with no return path into the decision, and on the `Apply` arm
> (`cmd/config.rs:243-248`) its text is passed to `write_and_report`, which calls
> `store_and_bump_if` and returns the text as a **prefix on the success message of an
> already-committed write** — `confirm` is never called on that path. A check there would have
> printed a warning about a push that had already happened: the very silent-application bug D5
> exists to close, wearing a fix.

So: the **decision** half goes in `plan_push`, which already receives `incoming: &RegistryConfig`
(`cmd/config.rs:40-45`) and so can count `area == PLANNER_AREA` agents — a third OR-condition
beside `d.requires_confirmation() || paused_runs > 0`, forcing `NeedsConfirmation`. The
**disclosure** half needs a channel into `describe_diff`, which today receives only
`(&ConfigDiff, u64, &str, usize)` and whose `ConfigDiff`/`DiffEntry` carry no `area`. The
established pattern in that same file is `paused_runs: usize`, added for exactly this reason —
warning about something the diff cannot express — and tested at `cmd/config.rs:399` and `:419`.
D5 follows it with an analogous parameter.

A hard invariant in `Registry::validate` was rejected and the reason is now measured, not
asserted: only **5 of 57** `AgentDefinition` literal constructions in `crates/` declare
`area: "planning"`, and a named currently-green test would flip red —
`from_config_assembles_validates_and_rejects_duplicates` (`registry.rs:1289`) assembles a registry
whose only agent is `area: research` and asserts it validates.

**Two things D5 does NOT close, recorded for the design:**

- **Edge vs level.** As worded the rule is level-triggered: once a registry is legitimately
  planner-less it re-warns on *every* future push, including unrelated edits. Only the transition
  case was argued. Design should decide.
- **"Add, don't override."** An implementer who *adds* a planner rather than editing the seeded one
  gets a silent competitor: `RulePlannerSelector::select` falls back to `candidates.first()` sorted
  by name, and `LlmPlannerSelector` puts every `area == planning` candidate in one menu.

### What D6 deletes — a merge constraint that no longer applies

An earlier draft spent a section on the merge semantics D3/D4 required: that
`Registry::from_config` (`registry.rs:428`) rejects duplicates loudly, so a merge could not be
concatenate-then-build; that it therefore had to happen at `RegistryConfig` level with a
per-collection key table (`name` for agents/skills/tools, `(area, kind)` for `chain_bindings`);
and that intra-layer dedup needed its own new pass because `FilesystemConfigSource::load` does
none.

**All of that is moot under D6.** There is one layer at push time — the implementer's directory —
so `from_config`'s duplicate rejection is simply the guard it always was, `chain_bindings` needs
no special key, and no per-layer dedup pass exists to write. The finding that produced the
constraint was correct and is retained here only as the reason D6 is cheaper than it looks: the
merge design was accumulating mechanisms (composing source, key table, dedup pass, tombstone,
opt-out flag, provenance record) that seed-once does not need at all.

### Added to the done gate by D1, D5–D7

6. `torii config init <dir>` writes the built-in defaults into an empty directory, and
   `FilesystemConfigSource::load` + `Registry::from_config` over that directory then assemble
   without error — i.e. what it seeds is immediately valid, not a template needing repair.
7. The seeded content is **owned**: deleting a file from `<dir>` and pushing removes that entity
   from the durable config, and a later `config init` into a *different* directory is the only way
   defaults reappear. No push re-introduces them.
8. `config init` works from a binary with no source tree beside it (D7) — asserted against the
   installed/compiled artifact, not against `crates/torii/registry/` on disk.
9. A push whose incoming config contains zero `area: planning` agents returns
   `PushDecision::NeedsConfirmation`, and the rendered text names the consequence. Asserted
   through `plan_push` for the decision and `describe_diff` for the text — **not** by adding a
   check to `describe_diff` alone, which renders after the write on the `Apply` path.

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

### Round 2 — re-run 2026-09-14 against the revised document

All three again, blind, and deliberately NOT told what round 1 found. **Round 2 invalidated two of
the three decisions round 1 produced**, which is the case for re-running rather than assuming a
rewrite closes what it claims to.

**Every one of round 1's five re-checked assertions was independently CONFIRMED** — D4's premises,
D5's premises, the merge-key table, §6.1's tool-executable hazard, §6.2's README count. Including
the one flagged as the author's own unverified weak point (that a hard `validate` invariant would
break existing tests), which the analyst proved with a named green test rather than a ratio.

What round 2 found anyway:

| # | Finding | Found by | Outcome |
|---|---|---|---|
| 1 | **D5 was specified in the wrong function.** `describe_diff` is a pure renderer; on the `Apply` arm its text prefixes an already-committed write and `confirm` is never called. The check would have warned about a push that already happened. | persona + depth | **D5 revised** — decision in `plan_push`, disclosure via a `paused_runs`-style parameter |
| 2 | **Defaults-root discovery undecided** — `crates/torii/registry/` is a source path an installed binary cannot see; no embedding mechanism exists. | analyst + depth | **D7** |
| 3 | No deletion: "later layer wins" has no tombstone. | persona | **D3/D4 superseded by D6** |
| 4 | A torii upgrade silently bumps the durable generation on an unedited push — and any push kills paused runs. | persona | **D3/D4 superseded by D6** |
| 5 | No opt-out; the minimal-trust deployer ends up worse off than the pre-slice status quo. | persona | **D3/D4 superseded by D6** |
| 6 | Gate item 2 remains unbuildable — no committed skill/keyword content. | depth | open, §6.1 |
| 7 | D5 is level-triggered, re-warning forever once legitimately planner-less. | depth | recorded for design |
| 8 | `ConfigAction::Push`'s doc comment would have gone stale under D4. | persona | moot under D6 — push is unchanged |
| 9 | `describe_diff` renders no field-level values, so an `area` reassignment looks like any `~ agent`. | persona | recorded for design |
| 10 | Per-layer vs cross-layer dedup ordering underspecified. | depth | moot under D6 |

Findings 3, 4 and 5 shared one root cause — merging defaults on *every* push — and killing that
property with D6 closed all three at once, while also making 8 and 10 moot and removing five
mechanisms the design would otherwise have owed.

**A third round is required**, by the same rule that forced this one: D6 and D7 assert new things
about existing code (that `torii config push` needs no change under seed-once; that no embedding
dependency exists today), and the done gate has been rewritten around them.

## 8. Method note

The sensei daemon was not running, so the MCP code-graph tools (`search`, `get_callers`) were
unavailable and every claim above was derived with ripgrep over `crates/`, `database/` and
`docs/`. "Nothing calls this" is exactly the claim those tools answer better; the absolutes in
§2.1 and §2.2 should be re-checked with the graph when the daemon is up.
