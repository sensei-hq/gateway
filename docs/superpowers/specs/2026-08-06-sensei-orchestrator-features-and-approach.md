---
title: sensei-orchestrator — Features & Approach
doctype: reference
status: master-reference
date: 2026-08-06
companion: ./2026-08-06-sensei-orchestrator-design.md (decisions D1–D13, SP-0…SP-DATA)
purpose: enumerate the complete set of features and the approach before any per-slice design/implementation
---

# sensei-orchestrator — Features & Approach

---

## 1. What we're building (one paragraph)

A **resilient agentic execution framework** in Rust that wraps the `sensei-gateway` crate. It runs configurable **agents** (with **skills**, **tools**, and role→**chain** bindings over model **tiers**) across a **hierarchical, runtime-expandable graph**, on a **durable step-journal** that resumes after crashes / token-quota exhaustion / rate limits **without re-spending tokens** and **without silent failure**. Underneath, the gateway gains **model/router health gates** and a **free-tier catalog**; alongside, a **decoupled data-tier** (extracted from torii) manages catalog metadata, refresh, and usage tracking. It is domain-agnostic (coding, workflow automation, deep research, …).

## 2. Approach (the *how*)

**A1 — Convergence, not greenfield.** Reuse the `sensei-hq` family: the **gateway crate** (pure inference core), **torii**'s `catalog`/`config`/`metering` data layer (extracted + decoupled), **strategos**'s agent-layer design (ported to Rust), and **OmniRoute** as the reference for free-tier + lockout mechanics. One shared `catalog.*`/`keyvault.*` schema across the family.

**A2 — Pure core + data-tier wrapper.** The gateway core stays *stateless* (receives config, never persists). Free-tier *catalog data* is config; all stateful *tracking* lives in a data-tier that wraps the DB-agnostic seam (`GatewayStore`/`VaultStore`-style). Storage backends stay swappable.

**A3 — Durable-execution engine.** Step-journal + deterministic replay; every model/tool op is a memoized **effect** classed by idempotency (pure / observation / mutation); resume never re-spends tokens; two-phase + `in_doubt→reconcile` for side effects.

**A4 — No silent failures.** Every error is propagated, or journaled+mapped to an explicit outcome (fail / pause / replan / retry / reconcile), or surfaced on a diagnostics channel. Journal-write errors are strict. Structured errors preserved end-to-end.

**A5 — Core mechanism + opt-in policy.** The core always provides mechanisms (effect classes, two-phase, typed edges, health gates); workloads turn on expensive policies (sandbox, saga/compensation, exactly-once, TTLs) via config — so a coding agent stays light and a booking workflow turns the knobs on.

**A6 — Tiers × chains, free-tier-first.** Tiers are a catalog dimension (curated/attribute-derived) with intra-tier strategies; chains compose tiers; free tier is a first-class option chains can lead with.

**A7 — Four-phase delivery** (D11): **(1)** gateway enrichment (health gates + free-tier catalog + tiers + refresh) → **(2)** reference chains (free-tier "research team") → **(3)** agentic executor (agents/skills/tools + durable core) → **(4)** data-tier + management API (metadata/refresh/tracking). Gateway + executor ship before the heavy management/tracking layer.

**A8 — Engineering discipline.** TDD (every change ships with a test), small single-purpose changes, config-driven, adversarially validated (design was stress-tested against coding / trip-planner / deep-research workloads). First implementation slice: **SP-0 gateway health gates**, coordinated with issue #39.

**Source legend for §3:** `[GW]` exists in gateway · `[GW+]` gateway enhancement · `[orch]` new orchestrator · `[torii]` reuse/extract torii · `[omni]` OmniRoute-inspired · `[strat]` strategos design.

## 3. Feature catalog (the *what*)

### A. Resilient inference (gateway core)
| Feature | Description | Source | Phase |
|---|---|---|---|
| Fallback chains | Ordered `ChainEntry`s by priority; per-chain triggers; intra-call fallover with `attempts` trail | `[GW]` | — |
| Circuit breaker | Per-endpoint (`router:model`) Closed/Open/HalfOpen | `[GW]` | — |
| Budget + subscription quota | Per-request budget; rolling-window quota tiers | `[GW]` | — |
| Panels / consensus | Multi-model fan-out + debate→synthesize→judge | `[GW]` | — |
| Streaming | `execute_stream` with `ProviderSwitch`/`Done`/`Error` events (pre-first-byte fallover) | `[GW]` | — |
| BYOK vault | Per-tenant envelope-encrypted credentials; `oauth:` tokens | `[GW]` | — |

### B. Model/router health & availability (SP-0)
| Feature | Description | Source | Phase |
|---|---|---|---|
| Connection cooldown | Router/connection-level cooldown on connection faults (skip all of a provider's models) | `[GW+][omni]` | 1 |
| Model lockout | Per-`model:credential` lockout with **per-reason** cooldowns (`rate_limit`≈60s, `quota_exhausted`≈until reset, `credits`→terminal) | `[GW+][omni]` | 1 |
| Escalating backoff | Escalation window outlives the cooldown; clamp to `maxCooldownMs` but honor real upstream reset hints exactly | `[GW+][omni]` | 1 |
| Limit classification | 429→rate_limit, 403/quota-body→quota_exhausted, credits→terminal; text-pattern detection for non-standard bodies | `[GW+][omni]` | 1 |
| Proactive expiration tracking | Track `oauth_token`/`subscription`/`api_credits`/`free_tier_reset` expiry with pre-emptive alerts; skip a credential before it fails | `[GW+][omni]` | 1 |
| Quota demote-to-tier | `QuotaExceeded` falls over to next tier instead of terminating; pause only if whole chain gated, with `resume_after` | `[GW+]` | 1 |
| Cumulative retry-wait budget | Honor `Retry-After` but cap total wait across retries | `[GW+][omni]` | 1 |

### C. Free-tier & cost management
| Feature | Description | Source | Phase |
|---|---|---|---|
| Free-tier catalog | Per-model `free_type`/`monthly_tokens`/`credit_tokens`/`pool_key`/`tos`/`trains_on_prompts`; pool-dedup totals | `[omni]` | 1 |
| Catalog refresh mechanism | CLI + periodic re-audit of models/providers/routers; CI-gated totals | `[omni][torii]` | 1 |
| Usage metering | Counters vs free + paid limits, reset windows (per key/model/pool) | `[torii][omni]` | 4 |
| Predicted-exhaustion lockout | Pre-emptive lockout from usage trend, not just reactive 429 | `[omni]` | 4 |
| Free-tier-aware routing | Intra-tier strategies: `headroom`/`fill-first`/`reset-window`/`quota-share` | `[omni]` | 4 |
| Paid budgets | Per-key `daily/weekly/monthly` USD + reset interval/time | `[torii][omni]` | 4 |
| Free-tier dev upstream (optional) | Point a `RouterConfig` at a local OmniRoute for free-tier models in dev (ToS-caution aware) | `[omni]` | dev |

### D. Catalog & config control-plane (data-tier)
| Feature | Description | Source | Phase |
|---|---|---|---|
| Catalog schema | providers/models/model_endpoints/model_capabilities/routers/chains/chain_models/chain_bindings/routing_policies/provider_health | `[torii]` | 1/4 |
| Config loader | Assemble catalog → runtime `GatewayConfig` (like `assembleConfig`) | `[torii]` | 1 |
| Config versioning | `config_versions` + `bump_config_version` (≈ the durable-replay version-fence) | `[torii]` | 4 |
| Import/staging loader | `import_models/providers/routers/fallback_chains/...` staging procedures | `[torii]` | 1/4 |
| Management CLI + API | Configure catalog + observe usage/lockouts/expirations/free-tier budget | `[torii][strat]` | 4 |
| Decoupled, user-agnostic subsystem | Extract `catalog`+`config`+`metering`; tenancy optional/injectable; user/governance excluded | `[torii]` (D12) | 4 |

### E. Tiers & chains
| Feature | Description | Source | Phase |
|---|---|---|---|
| Tiers as catalog dimension | Named segments, curated or attribute-derived (auth_type/cost/capability/free_type/locality) | `[omni]` (D13) | 1 |
| Intra-tier routing strategy | Per-tier `headroom`/`least-used`/`cost`/`priority` | `[omni][torii]` | 1/4 |
| Chains compose tiers | Chain = ordered list of tier-refs (or concrete models); tier-expansion at assembly | `[orch][torii]` (D13) | 1 |
| Role→chain binding | Agent role/kind → named chain (e.g. `plan.frontier`, `research.bulk`) | `[orch][strat]` | 2/3 |
| Reference chains | Curated starters incl. free-tier "research team" (chains stay user-managed) | `[orch]` | 2 |

### F. Agent / skill / tool registry & runtime
| Feature | Description | Source | Phase |
|---|---|---|---|
| Agent definitions | md+frontmatter: name, area, kind, chain(s), tools, skills, subagents, system-prompt body | `[orch]` | 3 |
| Skills | Injectable instruction modules (md+frontmatter), composed into prompts | `[orch]` | 3 |
| Tools | Executable capabilities (JSON-schema args), executed by orchestrator; effect-class + permissions per tool | `[orch][strat]` | 3 |
| Agent runtime | Budgeted prompt assembly + ReAct/tool loop + chain resolution + gateway call | `[orch][strat]` | 3 |
| Planner / coordinator / sub-agents | LLM planner (self-correcting JSON + validation + cycle detection + feasibility); coordinator; focused sub-agents | `[strat]` | 3 |
| Hot-reloadable registry | External config for agents/skills/tools/chains | `[orch]` | 3 |

### G. Orchestration & execution graph
| Feature | Description | Source | Phase |
|---|---|---|---|
| Hierarchical graph | Node kinds: Agent/Tool/Loop/Subgraph/Branch/Map/Consolidate/HumanGate | `[orch]` | 3 |
| Runtime expansion | Planner emits `PlanDelta` subgraphs spliced in at runtime (journaled) | `[orch]` | 3 |
| Loops of graphs | Loop over a Subgraph with a gate agent (Continue/Stop) + max_iters + budget backstop | `[orch]` | 3 |
| Typed edges + aggregation | hard/soft edges; `fail_fast`/`best_effort`/`quorum` completion policies; failure manifests | `[orch]` | 3 |
| Bounded + adaptive concurrency | Bounded fan-out; provider-aware adaptive limiter + jittered backoff | `[orch]` | 3/4 |

### H. Durable execution & recovery
| Feature | Description | Source | Phase |
|---|---|---|---|
| Step-journal + replay | Fold events to rebuild state; resume from first incomplete node | `[orch]` | 3 |
| Effect classes | pure (memoize) / observation (memoize+TTL+provenance) / mutation (two-phase) | `[orch]` | 3 |
| Effect memoization | No token re-spend on resume | `[orch]` | 3 |
| Two-phase + in-doubt/reconcile | Intent→Recorded; in-doubt→reconcile via idempotency key (exactly-once for side effects) | `[orch]` | 3/4 |
| effect_id scheme | Structural path + loop-iteration + input-hash + version-fence; replay-divergence halts loudly | `[orch]` | 3 |
| Journal/CAS split + snapshots | Small control-flow log vs content-addressed payloads; snapshots + compaction | `[orch]` | 3/4 |
| Quota/limit → pause → resume | Durable pause with `resume_after`; durable scheduler re-arms timers | `[orch]` | 3/4 |

### I. Shared context / blackboard
| Feature | Description | Source | Phase |
|---|---|---|---|
| Scoped durable context | run/plan/node/agent scopes; reads resolve up-chain; writes journaled (global seq) | `[orch][strat]` | 3 |
| Refs not blobs | Entries hold digests→CAS; read-miss explicit | `[orch]` | 3 |
| Prompt budgeting | Sized to min-context across the chain; explicit summarize/select, never silent truncation | `[orch]` | 3/4 |
| Cross-role/fallback handoff | Later roles/fallback models see accumulated context | `[orch]` | 3 |

### J. Observability & progress
| Feature | Description | Source | Phase |
|---|---|---|---|
| Progress hooks | `OrchestratorHooks` (run/graph/agent/context); per-agent lifecycle | `[orch][strat]` | 3 |
| Attempts bubbling | Surface the gateway fallover trail live | `[orch]` | 3 |
| Replay suppression | `replay:true` flag so UIs don't double-count on resume | `[orch]` | 3 |
| Durable outbox notifications | Pause/HITL notifications as durable effects (not best-effort hooks) | `[orch]` | 4 |
| Analytics / metering views | Usage/spend/model-mix rollups | `[torii]` | 4 |

### K. Security & compliance (opt-in policy)
| Feature | Description | Source | Phase |
|---|---|---|---|
| Tool permission model | Path/command/network allowlists + resource caps per tool | `[orch]` | 3/4 |
| Sandboxed shell | Container/jail + ephemeral credential broker | `[orch]` | 4 |
| Secret redaction | Redact secrets before journaling effect I/O | `[orch]` | 3 |
| Workspace isolation | git worktree / CoW per parallel branch + declared resource locks | `[orch]` | 4 |
| Crypto-shredding | Per-run key; delete key to satisfy PII erasure vs append-only journal | `[orch]` | 4 |

### L. HITL & extensibility
| Feature | Description | Source | Phase |
|---|---|---|---|
| HITL / external signals | `AwaitSignal`/`HumanGate` + idempotent correlated signal mailbox; pause-expiry | `[orch][strat]` | 4 |
| Approval gates | Human approval before sensitive tool exec | `[strat]` | 3/4 |
| Provider extensibility | New providers via capability traits + `RegisterInto` | `[GW]` | — |
| MCP tools bridge | External tools via MCP | `[strat][torii]` | 3/4 |
| Saga / compensation | Compensating actions for committed side effects | `[orch]` | 4 |

## 4. Feature → phase summary

- **Phase 1 — Gateway enrichment (pure core):** B (health gates), C (free-tier catalog + refresh), E (tiers + tier-composed chains), D (schema + loader). *First slice = SP-0 (health gates).*
- **Phase 2 — Reference chains (user-managed):** E (reference chains, incl. free-tier research team).
- **Phase 3 — Agentic executor:** F (registry + runtime), G (graph), H (durable core), I (context), J (hooks). *Walking skeleton = deep-research mini.*
- **Phase 4 — Data-tier + API:** D (management API + versioning), C (usage metering + predicted lockout + free-tier routing), H (durable Postgres adapter + scheduler), K (sandbox/isolation/crypto-shred), L (HITL/signals, saga).

## 5. Non-goals (v1) & future

- Not a distributed multi-worker scheduler (single-process executor + durable resume; multi-instance is a future seam, incl. shared health-gate state).
- Not a UI (hooks feed one).
- Semantic long-term memory across runs (RAG substrate exists in the family; out of scope v1).
- Torii's tenancy/governance/RBAC/budgets/RAG stay in torii; the extracted data-tier is user-agnostic.

## 6. References

- Architecture & decisions: [`2026-08-06-sensei-orchestrator-design.md`](./2026-08-06-sensei-orchestrator-design.md)
- Siblings: `sensei-gateway` (this repo) · `~/Developer/torii` (data layer) · `~/Developer/strategos/strategos` (agent prior-art, TS) · `~/Developer/Labs/OmniRoute` (free-tier/lockout reference, JS)
