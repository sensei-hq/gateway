---
title: SP-REG — sequencing what is left of the registry programme
doctype: design-spec
module: orchestrator + torii
slice: SP-REG-2..5
status: draft
date: 2026-09-15
analysis: ../../analysis/2026-09-14-sp-reg-1-registry-content.md
---

# SP-REG — sequencing what is left

## 1. Why this exists

SP-REG-1's analysis went through four adversarial review rounds producing **10 → 12 → 12**
findings — not converging — and established that "ship registry content" had accreted into roughly
eight independent workstreams. Two shipped alone (**SP-REG-0**, the planner selector, PR #63;
**the torii entry-point docs**, PR #64). This spec sequences the rest and **resolves the technical
forks**, so what remains is one product decision rather than seven open questions.

Everything here is decided, not offered as a menu. Where a fork was genuinely open, the reasoning
is recorded so it can be re-opened deliberately rather than by accident.

## 2. The slices, in dependency order

| slice | what | blocked on |
|---|---|---|
| **SP-REG-2** | Discovery tools, composed per-run from the pinned registry | code: nothing. **Done gate: the tool SPECS** — see §3 |
| **SP-REG-3** | Designating which planner wins | nothing |
| **SP-REG-4** | `config init` + embedded defaults + the shipped content | **the content list (§6)** |
| **SP-REG-5** | Cross-check agent chain ids against the gateway config at push | nothing |

Distribution (a published artifact, build provenance, `--version`) is **out of scope**. It is a
release-engineering concern with no dependency on any of the above, and folding it in is what made
SP-REG-1 a programme in the first place.

## 3. SP-REG-2 — discovery tools, per-run

`ListAgents`, `ListSkills`, `ListTools`, `ListChains`, `ValidatePlan` are `pub` production types so
a planner can introspect the registry and self-validate a draft plan. **Nothing in production wires
them** — zero references anywhere in `crates/torii`.

They are not unproven, though, and that de-risks the slice: a live test at
`executor/tests.rs:9358-9465` composes a real `ToolRegistry` with `ValidatePlan` + `ListAgents` over
a real registry and drives a planner agent that calls `validate_plan` end to end. The shape works;
what is missing is the production composition site.

**The done gate has a dependency the code change does not.** That same test also registers the
matching `ToolSpec`s into the CORE registry — it must, because `assemble_prompt_parts` resolves
`agent.tools` names against the core registry and errors `UnknownToolRef` on a miss, and
`Registry::validate` rejects the whole config at load for an agent listing an unregistered tool.
So an operator's pushed registry needs a `tools/list_agents.json` before a planner can declare it.
That is authorable by hand today, but SHIPPING those specs is §6's blocked content list. The code
change is unblocked; done gate 1 is not.

### Resolved design

Compose them in `Executor::pinned` (`executor/mod.rs:935`), from the registry that call is already
pinning:

```rust
fn pinned(mut self, registry: Arc<Registry>, generation: u64) -> Self {
    self.version = format!("{}#cfg{}", self.version, generation);
    self.tools = Arc::new((*self.tools).clone()
        .with_tool(Arc::new(ListAgents(registry.clone())))
        // … the other four, ValidatePlan taking max_nodes from the executor's cap
    );
    self.registry = registry;
    self.handle = None;
    self
}
```

`ToolRegistry` already derives `Clone` and holds `HashMap<String, Arc<dyn Tool>>`, so this is cheap.

### Why NOT at boot

Boot-wiring is the natural reach and is wrong for a reason that survives review: `with_tools` is
set **once** for the executor's whole life, while `pinned` re-pins the registry per run. A boot
snapshot would let a planner introspect a registry its own run is not pinned to — and in-process,
after a `RegistryHandle::reload`, every subsequent run would still see the boot-time set.

> **⚠️ Corrected.** An earlier draft justified this with "two processes booted either side of a
> `config push` would hand the same run different agent lists." On the shipped path that cannot
> happen: a push bumps the durable generation, so the later process pins `v1#cfg{g+1}` against a
> recorded `v1#cfg{g}` and `start_inner` refuses with `VersionFenceMismatch` **before driving
> anything**. The real reason is the in-process reload case above. Note `RegistryHandle::reload`
> has no production caller today, so the two are currently observationally identical in `torii` —
> which makes this a correctness-by-construction argument, not a live-bug argument.

### The determinism argument, scoped honestly

1. **The hash is unaffected.** `agent_input_hash(chain, system, messages, tools)` takes
   `AgentRun.tools`, built by `assemble_prompt_parts` from the schemas the AGENT declared, resolved
   against the core `Registry`. The executor's `ToolRegistry` never reaches it.
2. **It widens nothing.** The SP-4 s1 gate requires a called tool to be LISTED on the agent
   (`ar.agent_tools.iter().any(...)`), so registering an executable an agent has not declared is
   inert. The one read that happens BEFORE that gate is `spec_of` → `class`, which defaults to
   `Pure` for an unregistered name — and all five discovery tools declare `EffectClass::Pure`, so
   registering them changes no class and no memo behaviour.

   > **⚠️ Corrected.** An earlier draft cited `boot.rs` as recording "the same argument". It does
   > not: `boot.rs`'s argument for `fs_read`/`fs_write`/`shell` is that those tools **fail closed**
   > without a workspace root or a sandbox, and it mentions the s1 gate only as something that is
   > *passed*. The discovery tools have no such fail-closed property — `ListAgents::call` returns
   > the full list unconditionally. The s1 listing check is the whole defence, so it is stated
   > directly rather than leaning on a false analogy.

3. **Replay-safety holds only on the fenced path, and that scopes the slice.** The tools read the
   pinned registry, and the run's fence is `{version}#cfg{generation}`.

   > **⚠️ An earlier draft claimed "there is no third outcome". That is FALSE — there are at least
   > three.** `format!("{}#cfg{}", …)` is the only writer of the suffix and lives inside `pinned`.
   > So: **(a)** an executor built with `with_registry` (no handle) never gets a `#cfg` term at all,
   > so two processes with *different* fixed registries carry the *same* fence and compare equal;
   > **(b)** a handle over an unversioned source pins generation `0` — `ConfigSource::version()`
   > defaults to `None` and `from_source` does `unwrap_or(0)` — so a `FilesystemConfigSource` yields
   > `v1#cfg0` whatever its content, which the repo states itself in `e2e_pg.rs` ("a worker that
   > never pins a generation can never observe a fence drift"); **(c)** `run_inner` never loads the
   > journal and never checks the fence at all.

**This inverts the earlier draft's "wrinkle".** That draft said composing on the no-handle path was
a completeness requirement and the likeliest way to ship half-working. The opposite is true:
**compose ONLY on the pinned path.** A fixed-registry executor has no generation, therefore no
fence, therefore nothing making discovery output replay-safe — registering the tools there would
manufacture exactly the divergence this design avoids. That is a deliberate boundary with a reason,
and the doc comment must say so. Production is unaffected: `boot::heavy` uses
`with_registry_handle`, and `with_registry` appears only in tests.

### Done gate

1. A planner agent declaring `list_agents` gets the pinned registry's agents, not a boot snapshot.
2. The no-handle construction path registers them too.
3. `agent_input_hash` is byte-identical for a run whose agent declares none of them.
4. An agent that does NOT declare a discovery tool still cannot call it (s1 gate unchanged).

## 4. SP-REG-3 — designating the planner

With two `area: planning` agents the winner is `candidates.first()` over a name sort — alphabetical
accident. `RulePlannerSelector` already has the mechanism (`default: Option<AgentRef>`, preferred
when it is among the candidates); **nothing can supply it**.

### Resolved design: a registry field, ordered at selection time — NOT read at boot

Add an optional marker to `AgentDefinition` (frontmatter `default_planner: true`), and make
`Executor::planner_candidates` — which already reads `self.registry` and sorts by name — order the
marked agent **first**, then the rest by name.

That is the whole change. `RulePlannerSelector::new(None)` already takes `candidates.first()`, so
it picks the marked one with no new plumbing; and `LlmPlannerSelector` sees it first in the
capability menu. No trait change, no new constructor argument, and the value is read from the
**pinned** registry at selection time rather than from a process-scoped snapshot.

> **⚠️ Both halves of this section were wrong in an earlier draft, and the review caught both.**
>
> **The rationale was false.** It argued a flag or env var risks "two workers disagreeing about
> which planner a run uses". They cannot: `PlannerRef::Select` journals its pick as
> `PlannerSelected`, and on every later drive `expand.rs` reuses `fold.selections` — the code's own
> comment reads *"RESUME: reuse the recorded pick; the selector is NOT re-invoked."* Replay
> stability here comes from the journal, not from where the default lives.
>
> **The implementation tripped this spec's own Wrong Gate.** It put the read in `boot::heavy` →
> `RulePlannerSelector::new(marked)` → `with_planner_selector`, which is set-once; `pinned` never
> touches `self.selector`. That is a boot snapshot outside the fence — exactly what §3 declares
> wrong for the discovery tools, and exactly what §8 item 2 warns against. Reading it from
> `planner_candidates` instead is what makes the marker actually live in the fenced path.

The honest argument for the registry over a flag is therefore **not** replay-safety. It is
**fleet consistency**: one pushed config gives every worker the same answer for a first selection,
whereas a flag must be set identically on every process or two runs started on different workers
get different planners. That is an operability property, not a correctness one, and it is the real
reason to prefer it.

Two or more agents marked is a **loud `RegistryLoad` error** at `from_config`, consistent with how
duplicate names and duplicate `(area, kind)` bindings already fail there. Zero marked keeps today's
name-order behaviour, so this is additive.

### Done gate

1. Two planners, one marked ⇒ the marked one is selected regardless of name order.
2. Two marked ⇒ loud refusal at config load, naming both.
3. Zero marked ⇒ byte-identical to today.

## 5. SP-REG-5 — cross-check chain ids at push

An agent's `chain` is a string. `Registry::validate` checks only that it is *present*; the id is
resolved later in the gateway against `GatewayConfig.chains`, a file `torii config push` never
reads. Disagreement yields empty candidates → `NoCandidates` → terminal `NodeFailed`, with a
message the code itself says names "neither cause nor remedy".

### Resolved design

`torii config push` takes an **optional** `--gateway-config`. When supplied, every chain id the
incoming registry references — explicit `agent.chain`, per-phase `chains`, and `ChainBinding.chain`
— must exist in that file's `chains` map, or the push is refused loudly, naming the agent and the
missing id.

Optional rather than required so the change is additive and no existing invocation breaks. The
value is that an operator who supplies it converts a terminal mid-run failure into a push-time
refusal naming both sides.

### Done gate

1. A registry referencing an unknown chain id, pushed with `--gateway-config`, is refused naming
   the agent and the id.
2. The same push without the flag behaves exactly as today.
3. A registry whose ids all resolve pushes unchanged.

## 6. SP-REG-4 — BLOCKED on one product decision

`config init` (contract in analysis D9), defaults embedded via `include_dir!` (chosen over
`rust-embed`: a `Dir` const with no derive and no runtime trait is the simpler fit for a fixed
directory baked at compile time), and the content itself.

**The blocker is the content list** — how many skills, their real `activate_on` keyword sets, and
whether any `tools/*.json` ship. The analysis's own Wrong Gate demands keyword sets "a real prompt
would hit" rather than fixture-shaped ones, and nothing in the code can answer that.

Also unresolved inside this slice and inherited from the analysis: `config init` must not require
`DATABASE_URL`, and the fix is **larger than D9 states** — `dispatch()` calls `boot::env_config()?`
at `main.rs:418`, before the match on the command, so restructuring the `Config` arm alone is
insufficient.

## 7. Claims

Every check is a **command**, not a line reference. An earlier draft used `read <file>:<lines>`,
which is not re-runnable and drifts the moment a line is inserted above it — one of its own
citations had already gone stale by the time it was reviewed. Re-run this table at build start.

| claim | check | expect | verdict |
|---|---|---|---|
| The five discovery tools are wired nowhere in production | `rg --no-ignore 'ListAgents\|ListSkills\|ListTools\|ListChains\|ValidatePlan' crates/torii/` | 0 | CONFIRMED |
| …but a live test proves the composition shape | `rg -n 'ListAgents\|ValidatePlan' crates/orchestrator/src/executor/tests.rs` | non-empty | CONFIRMED |
| All five hold an `Arc<Registry>` snapshot, and all are `EffectClass::Pure` | `rg -n 'pub struct (List\|ValidatePlan)' -A 12 crates/orchestrator/src/agent/tools.rs` | snapshot + Pure | CONFIRMED |
| `pinned` re-pins the registry, clears the handle, and does NOT touch `selector` or `tools` | `rg -n 'fn pinned' -A 7 crates/orchestrator/src/executor/mod.rs` | 3 assignments only | CONFIRMED |
| `ToolRegistry` derives `Clone` | `rg -n 'derive.*Clone' -A 3 crates/orchestrator/src/agent/tools.rs \| head` | derives | CONFIRMED |
| `agent_input_hash` hashes the AGENT's declared tools, not the executor registry | `rg -n 'fn agent_input_hash' -A 12 crates/orchestrator/src/executor/support.rs` | `chain\|system\|messages\|tools` from `AgentRun` | CONFIRMED |
| **The executor's `ToolRegistry` has FIVE read sites, and one feeds the journal** | `rg -U -n 'self\s*\n?\s*\.tools' crates/orchestrator/src/executor/agent.rs` | **6 matches, 5 real reads** — the 6th is a comment | **CONFIRMED — an earlier draft listed four.** A single-line `rg 'self\.tools'` misses the chain split across lines at the `idempotency_key_of` call, whose result is journaled into `EffectIntent`. Headline survives: that value never enters `agent_input_hash`, and all five discovery tools are `Pure` so they emit no `EffectIntent` at all |
| The s1 gate requires a called tool to be LISTED on the agent | `rg -n 'agent_tools.iter' crates/orchestrator/src/executor/agent.rs` | present | CONFIRMED |
| `pinned` is reached only when a handle is set | `rg -n -B 4 '\.pinned\(' crates/orchestrator/src/executor/mod.rs` | both under `if let Some(h)` | CONFIRMED |
| **`#cfg` is written in exactly one place, inside `pinned`** | `rg --no-ignore -n 'format!\("\{\}#cfg' crates/` | **exactly 1**, in `executor/mod.rs` | CONFIRMED — this is why the fence is absent on the no-handle path. (A bare `rg '#cfg\{'` returns 13 hits over 6 files; 12 are doc comments and tests. The pattern must target the `format!` writer or it reports a number that means nothing.) |
| An unversioned `ConfigSource` pins generation 0 | `rg -n 'fn version' -A 4 crates/orchestrator-core/src/registry.rs; rg -n 'unwrap_or\(0\)' crates/orchestrator-core/src/registry.rs` | default `None` → 0 | CONFIRMED |
| `run_inner` never checks the fence | `rg -n 'fn run_inner' -A 30 crates/orchestrator/src/executor/mod.rs \| rg -c 'fence\|VersionFence'` | 0 | CONFIRMED |
| `PlannerRef::Select` reuses a journaled pick on resume | `rg -n 'PlannerRef::Select' -A 6 crates/orchestrator/src/executor/expand.rs` | "selector is NOT re-invoked" | CONFIRMED |
| `planner_candidates` reads `self.registry` and sorts by name | `rg -n 'fn planner_candidates' -A 12 crates/orchestrator/src/executor/mod.rs` | reads + sorts | CONFIRMED |
| `RulePlannerSelector`'s `default` slot has no supplier | `rg --no-ignore -n 'RulePlannerSelector::new' crates/` | only `new(None)` in prod | CONFIRMED |
| `Registry::validate` rejects an agent listing an unregistered tool | `rg -n 'UnknownToolRef' crates/orchestrator-core/src/registry.rs crates/orchestrator/src/agent/prompt.rs` | both sites | CONFIRMED |
| `Registry::validate` checks chain PRESENCE, not resolvability | `rg -n 'UnknownChainRef' -B 6 crates/orchestrator-core/src/registry.rs` | no catalog consulted | CONFIRMED |
| An unknown chain id yields empty candidates → terminal `NodeFailed` | `rg -n 'fn resolve_candidates' -A 16 crates/gateway/src/selection.rs` | `all_candidates: vec![]` | CONFIRMED |
| `torii config push` never reads the gateway config | `rg -n 'enum ConfigAction' -A 14 crates/torii/src/main.rs` | no gateway-config arg | CONFIRMED |
| `dispatch()` requires `DATABASE_URL` before matching the command | `rg -n 'fn dispatch' -A 4 crates/torii/src/main.rs` | `env_config()?` first | CONFIRMED |

## 7.1 Verification round 1 — `sensei-claims-verifier`, 2026-09-15

Run against the first draft. **Seven findings; three FALSE, one of which invalidated §4's entire
fork resolution.** The pattern that has run through this whole document family held again: a true
premise with a false THEREFORE.

| finding | outcome |
|---|---|
| §3.2 "there is no third outcome" — **FALSE**, three of them: the no-handle path has no `#cfg` term at all; a handle over an unversioned source pins `cfg0` regardless of content; `run_inner` never checks the fence | §3 rewritten; the "wrinkle" INVERTED — compose only on the pinned path |
| §4's rationale — **FALSE**. `PlannerRef::Select` journals its pick and resume reuses `fold.selections`; the selector is not re-invoked, so a flag could not cause two workers to disagree | §4 rewritten: the real argument is fleet consistency, not replay-safety |
| §4's implementation — **FALSE by this spec's own standard.** Reading the marker in `boot::heavy` makes it a set-once snapshot; `pinned` never touches `selector`. It tripped §8 item 2 | §4 now orders candidates in `planner_candidates`, inside the fenced path |
| §3.3's `boot.rs` analogy — **FALSE**. `boot.rs` argues fail-closed tools, not the s1 listing check, and cites the gate as *passed* | analogy dropped; the s1 argument stated directly |
| Ledger row 6 — **FALSE**. A fifth `self.tools` read (`idempotency_key_of`) feeds `EffectIntent`; a single-line `rg` misses the chain split across lines | row corrected; headline survives |
| "None is wired anywhere" — MISLEADING. A live test proves the composition shape | §3 now cites it as de-risking |
| "SP-REG-2 blocked on nothing" — MISLEADING. The done gate needs a `ToolSpec` in the pushed registry | §2 and §3 corrected |

It also made a structural point worth keeping: the first draft's ledger used `read <file>:<lines>`
as its checks, which are not re-runnable and drift — one had already gone stale. Every row is now a
command, and two of those commands were themselves imprecise on first write (`rg '#cfg\{'` counts
doc comments; `rg 'self\.tools'` counts a comment line) and were tightened until they return what
the table claims.

**A second verification round is owed** before build: §3 and §4 were substantially rewritten, and
both now assert new things about existing code.

## 8. Wrong gate

- **SP-REG-2 ships half-working** because only the handle path composes the tools. §3's wrinkle.
- **SP-REG-3's marker is read at boot but not fenced**, if it is sourced from anywhere but the
  pinned registry — which would recreate the per-process divergence it exists to avoid.
- **SP-REG-5's check passes vacuously** if it validates only explicit `agent.chain` and skips
  per-phase `chains` and `ChainBinding.chain`.
- **The done gates certify tooling rather than outcome** — the failure mode round 3 found in
  SP-REG-1's own gate item 1, where a precondition was asserted as if it were the result.
