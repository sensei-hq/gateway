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
`area: planning` agent, because without one `PlannerRef::Select` is inoperable (§2.2). Other roles
the code already expects: `area: "research" / kind: "reasoning"` (`plan.rs:270`,
`torii/diff.rs:183`).

**D2 — built-ins live at `crates/torii/registry/`.** torii owns the management surface and the
write path, so the baseline content sits beside it. The orchestrator stays content-free.

**D3 — layered sources, override by name.** A defaults source composed with the implementer's
source; on a name collision the later layer wins, so an implementer gets working defaults free,
can override any single one, and never forks the whole set.

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
code. `RegistryConfig` is four `Vec`s (`agents`, `skills`, `tools`, `chain_bindings`), so the merge
is per-collection by name — and `chain_bindings` is keyed by `(area, kind)`, not a name, which the
design must handle separately.

### Added to the done gate by D1–D3

6. A defaults layer and an implementer layer, both present, where the implementer's definition of a
   colliding name is the one the `Registry` resolves — and a duplicate WITHIN either layer is still
   a loud `RegistryLoad` error.
7. Overriding a built-in requires no fork: the implementer supplies one file, not a copy of the set.

## 7. Not yet run

The Step 4 depth check (`sensei-analyst`, `sensei-plan-depth-reviewer`,
`sensei-persona-reviewer`) has **not** been run against this analysis. §6 is now closed, so it is
the immediate next step before `/sensei:design`.

## 8. Method note

The sensei daemon was not running, so the MCP code-graph tools (`search`, `get_callers`) were
unavailable and every claim above was derived with ripgrep over `crates/`, `database/` and
`docs/`. "Nothing calls this" is exactly the claim those tools answer better; the absolutes in
§2.1 and §2.2 should be re-checked with the graph when the daemon is up.
