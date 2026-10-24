# System One in the product — corpus → ranking → torii → gateway → Seiki

**Opened:** 2026-10-24 · **Revised:** 2026-10-24 (roster re-measured on Ollama; the corpus/ranking loop
promoted to Phase 1; **ranking authority moved to torii**, with the gateway taking a caller-supplied,
tenant/user-filtered ranking on each decision call).
**Base:** gateway `develop` (decision seam uncommitted in the working tree), torii `origin/develop` @
`dddc38e` (the local `~/developer/torii` checkout is 350 commits behind and has no `crates/cli`).
**Status:** plan. No torii code has been written.

## 1. Goal

Make System One decisions usable **by the product**, with the model choice owned by the product and
measured rather than hardcoded:

1. the gateway **measures** — a corpus and a per-category ranking artifact, with no ranking data
   compiled into it;
2. torii **owns** the ranking — seeds it from the artifact, edits it when needed, and filters it to
   the models actually available to this tenant and user;
3. the filtered ranking is **passed to the gateway per decision call**, which orders only the
   candidates that survived its gates;
4. the executor can select a planner with a typed choice (built here, unreleased);
5. `torii` picks the selector at boot without hand-wiring, and refuses a chain that cannot serve;
6. Seiki shows the decision models, their effective windows, their ranking provenance and calls.

The long-term payoff is the engine follow-on (decision-backed gates), not the selector itself.

## 2. Where each side stands

**gateway (this repo)**

| Piece | State |
|---|---|
| `Capability::Decision`, typed questions/answers, `DecisionModel` | shipped (SP-DEC-1, v0.7.0) |
| Decision hosts: `ollama`, `openrouter`, `typesafe`, `llamacpp`, `sglang`, `laya`, `cloudflare` | shipped (SP-DEC-2/3, v0.11/0.12) |
| Benchmark harness (`crates/decision-bench`) + first local report | on `main`, unreleased (SP-DEC-4) |
| Ranking data in the gateway | **none, and that is the design** — no table, file or default ranking |
| Per-request ordering | `RoutingPreferences { sort, only, ignore, order }` on `InferenceRequest.routing` — **any capability, decisions included**: `to_decision_request` runs after selection, so `sort`/`only`/`ignore`/`order` already reach a decision call |
| A *measured* order | **does not exist** — `order` is hand-authored and carries no category, revision or provenance |
| `DecisionRequest` | `model`, `state`, `questions`, `images`, `keep_alive` — the **adapter boundary**, built by `to_decision_request` *after* selection; it needs no routing field |
| Corpus | cases **fetched at run time and gitignored**; only the S4 gates are committed |
| `DecisionModelStatus` | `Ready` / `ServerTooOld` / `NotPulled` / `NotADecisionModel` — **no effective window**, though the probe already reads `/api/show` |
| `ModelDispatch::choose` + `DecisionPlannerSelector` | **uncommitted working tree**, unreleased |

**torii (`origin/develop`)**

| Piece | State |
|---|---|
| Pin | every `sensei-*` crate @ `v0.12.0`; local dev via `[patch] ../gateway`, **CI strips the patch** |
| Catalog capability map | `map_capability` maps `chat/agent/embedding/image/vision/audio/video`; **no `decision`** — decision models and chains are silently dropped |
| Catalog data | no `decision` capability row, no decision model rows, no decision chain, no ranking table |
| Executor boot | wires `RulePlannerSelector::new(None)` only |
| Model availability | per tenant (key-configured chains) and per user (permissions/allowlists); nothing expresses it as a ranking filter |
| HTTP API | `/v1/chat` hardcodes `Capability::TextChat`; `/v1/models/available` filters `c.name = 'chat'`; `/v1/routing` is capability-agnostic |
| Ledger | `metering.inference_calls.capability` stores any capability in snake_case — no DDL needed |
| Engine-call ledger | the executor's gateway has no `GatewayStore`; run spend is journaled by the executor and charged at run level (AG-13) |

**The three blockers, in order:** there is no *measured* ordering to send (the field, the store and the
artifact are all missing); torii has no ranking store and no `decision` capability mapping; and the
gateway seam is untagged.

The correction that matters: ordering a decision call is **not** the blocker. `RoutingPreferences`
travels on `InferenceRequest`, and a decision request is an `InferenceRequest` with a `Decision`
payload that reaches selection like any other — `DecisionRequest` is the adapter's wire shape,
filled in afterwards. So the gateway change is one field on `RoutingPreferences`, not a field on two
types.

### 2.1 The Ollama roster, measured (2026-10-24)

Ollama publishes ten decision models (`/search?c=decision`): `laya`, `kev`, `strands-decider`,
`tev1`, `d3`, `nimble`, `nimble-v3`, `clef-flash`, `clef`, `pplx-decider`. On this machine
(Ollama 0.40.0, client 0.40.4) four are pulled and reachable through the `ollama` router:

| Model | Params | Server `num_ctx` | Model card | s1b accuracy (95% CI) | p50 / p95 |
|---|---|---|---|---|---|
| `nimble` | 9B Q8_0 | **8194** | 262144 | 0.950 [0.920, 0.980] | 558 / 625 ms |
| `strands-decider` | 1.9B mxfp8 | 4096 | 4096 | 0.945 [0.910, 0.975] | **48 / 57 ms** |
| `tev1:0.8b` | 752M BF16 | 2048 | 262144 | 0.930 [0.890, 0.965] | 41 / 60 ms |
| `laya` | 421M | 512 | 512 | 0.865 [0.815, 0.915] | 15 / 19 ms |

Scenario s1b (tool/intent routing, 8 candidates), 200 cases, seed 2026 — the closest published
analogue to "pick one of N candidates". **Tie: nimble, strands-decider, tev1** — their CIs reach the
top model's. The 9B model buys no measurable accuracy over a 1.9B one and costs ~11× the latency.
`kev` cannot be pulled on this server (needs Ollama ≥ 0.40.4).

Reproduce (config is `crates/decision-bench/bench.toml`, gitignored):

```sh
cargo run --release -p decision-bench -- run --scenarios s1b --out results-ollama.json
cargo run --release -p decision-bench -- report --results results-ollama.json --out /tmp/ollama.md
```

### 2.2 Token sizing is a server property, not a model property

Measured against `nimble` at `http://127.0.0.1:11434/v1/systemone`:

| Input | Result |
|---|---|
| ~575 tokens | 200, answered, 0.86 s cold |
| ~9 184 tokens | **400** `prompt 0 has 9184 tokens; expected 1–8194 (input is never truncated)` |
| ~25 k tokens | **413** `text and schema must not exceed 64 KiB` |

Two consequences:

- **`context_window` must be the server's `num_ctx`.** `nimble`'s card advertises 262 144, Ollama
  serves 8 194. A catalog seeded from the card passes the gateway's own window gate and then takes a
  400 from the server on every call — the `laya-ane` bug one layer down.
- **No Ollama-hosted decision model can take a decision above ~8 k tokens**, and 64 KiB of body is a
  hard ceiling regardless of tokens. A planner decision carrying a whole graph state either stays
  small or leaves the Ollama router.

## 3. Decisions (settled 2026-10-24)

| # | Decision | Resolution |
|---|---|---|
| D1 | Scope | Internal first (planner, then gates); a tenant `/v1/decide` route is Phase 6, only if still wanted. |
| D2 | Model roster | Ollama-native decision models. Default `strands-decider` (1.9B, 4096, 48 ms); `nimble` where an 8 194-token window fits and ~0.6 s/decision is acceptable; `tev1:0.8b` and `laya` as cheap tiers. No llama.cpp/Laya servers on the default path. |
| D3 | Selector configuration | Process env (`TORII_PLANNER_SELECTOR`, `TORII_DECISION_CHAIN`, `TORII_DECISION_CATEGORY`) now; per-tenant config later. |
| **D4** | **Who owns the ranking** | **torii.** It seeds from the gateway's artifact, may edit it, filters it per tenant/user, and passes it on each decision call. The gateway holds no ranking data and has no default. |
| D5 | What the gateway does with it | Orders only the candidates that survived its gates — never filters, never resurrects, never bypasses budget/health/window. The gateway is not told *why* the order is what it is beyond a category and a source id, and it does not second-guess it. |
| D6 | Follow-on gate | A decision-backed gate is the real product win for coding pipelines; after the selector is live. |
| **D7** | **Quality floor** | **A margin, not an absolute accuracy, configurable per category and overridable per tenant.** Absolute cannot work: the best measured model is 0.950 on `tool.route` and 0.630 on `triage.score`. The effective margin is `max(configured, top model's CI half-width)` — never a tolerance finer than the measurement. See §8.1a. |

### 3.1 Why a new input, and why not just `order`

`RoutingPreferences.order` already carries a per-request try-order that reaches a decision call, so
torii *could* send an order today — but only a hand-authored one, with nothing saying where it came
from or what it was measured for. Three things make a distinct `ranking` field worth its weight
rather than a second ordering mechanism:

- **Provenance is part of the data.** An authored `order` is an operator preference with no date; a
  ranking is a measurement with a category, an artifact revision and a generation time. The trace has
  to distinguish them, or "why did this run use the 9B model?" is unanswerable after the fact.
- **They are different lifetimes.** `order` is hand-written in a call site; a ranking is regenerated
  on a schedule and edited by an operator, with a revision that changes.
- **Undetectable misuse becomes detectable.** Sending both an `order` and a `ranking` is a caller
  bug (two orderings for one call), so the gateway can refuse it instead of silently letting one win.

Internally both go through **one** stable re-rank function, with `Ranking::preferred` following the
same first-match-wins grammar as `order`; there is no second ordering implementation.

## 4. Phase 0 — release the seam (this repo)

**Why first:** torii's CI resolves the pinned tag, not `../gateway`.

- Commit the decision seam + `DecisionPlannerSelector` (the working tree) **and** the Phase 2 ranking
  input below, in one release: `ModelDispatch::choose` is a required-trait-method break, and
  `RoutingPreferences` / `RoutingDecision` gaining a field breaks exhaustive struct literals, so one
  `0.13.0` bump covers both.
- Tag `v0.13.0`; update the README's consumer examples.
- Gates: `cargo test --workspace`, stable clippy `-D warnings`, fmt,
  `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked`.

**AC:** the tag exposes `DecisionPlannerSelector`, `ModelDispatch::choose`, `RoutingPreferences::ranking`
(passed on an `InferenceRequest`, no new field on `DecisionRequest`), and `RoutingDecision::ranking`.

## 5. Phase 1 — the corpus and the ranking artifact (this repo)

The gateway measures; it does not decide. Today the measurement stops at a Markdown report and a
gitignored `results.json`.

### 5.1 Category taxonomy

The bench's seven scenario ids are experiment names. Add one bidirectional mapping:

| Category | Scenarios that evidence it |
|---|---|
| `intent.route` | s1a |
| `tool.route` | s1b |
| `triage.score` | s2 |
| `entailment.check` | s3 |
| `gate.test_output` | s4-test-output |
| `gate.review_finding` | s4-review-finding |
| `gate.fix_check` | s4-fix-check |
| `planner.select` | **new** — see 5.2 |

A category with no cases is `unranked`, never "the first model alphabetically".

### 5.2 `planner.select` cases

The planner decision is not a public dataset: it is "given this goal and these candidate agents,
which planner?", generated from the engine's own `PlannerRef::Select` shapes. Build 50–100 committed
cases under `crates/decision-bench/scenarios/planner-*.jsonl`, with the same `labels_reviewed_by`
header discipline as S4. Until they exist, torii must stand on `tool.route` as a **labelled proxy**
in the ranking artifact; it must not be passed off as a planner measurement.

### 5.3 A pinned corpus

`fetch` records each dataset's revision sha and rows sha256 but writes only gitignored directories, so
a ranking cannot be rebuilt from the repo. Add `--corpus-lock <file>`: write the manifest to a
committed lockfile and verify it on `fetch`/`run`. A ranking names the lockfile it was built from.

### 5.4 `ranking.json`

`decision-bench rank --results results.json --out ranking.json`. Machine-readable, versioned, stable
field names:

```jsonc
{
  "schema": 1,
  "generated_at": 1793409600,
  "corpus": { "lock": "crates/decision-bench/corpus.lock.json", "sha256": "…", "seed": 2026 },
  "roster": [{ "id": "strands-decider", "router": "ollama", "api_model_id": "strands-decider", "context_window": 4096 }],
  "categories": {
    "planner.select": {
      "verdict": "tie",                       // winner | tie | insufficient | unranked
      "proxy": null,                           // set to "tool.route" when the evidence is a stand-in
      "ranked": [
        { "model": "strands-decider", "accuracy": 0.945, "ci": [0.910, 0.975], "ece": 0.03,
          "confident_wrong": 0.01, "coverage": 1.0, "p50_ms": 48 },
        …
      ],
      "evidence": ["s1b"],
      "shared_cases": 200
    }
  }
}
```

Rules it must keep (all already implemented in `rank.rs`, now machine-readable):

- `winner` only when the leader's CI lower bound exceeds **every** other ranked model's upper bound;
- `tie` lists the top model and every model whose CI overlaps it — **a tie is not an order**;
- `insufficient` has no ranking at all;
- a model under the 50% coverage floor is not ranked, only reported over its own cases;
- the roster records the **probed** windows, so a consumer can check fit.

**AC:** `rank` is deterministic given `results.json` + `--seed` (byte-identical on re-run); each
verdict branch has a unit test; a `tie` and an `insufficient` category produce no ordering.

### 5.5 Surface the effective window on the probe

`probe_decision_model` already fetches `/api/show`; discarding `parameters.num_ctx` is what lets a
card-sized window into a server that will refuse it. Add a **non-breaking**
`probe_decision_window(config, model) -> Result<Option<u32>>` (Ollama: `parameters`' `num_ctx`,
falling back to `model_info`), reused by the bench's `check` and torii's seeding. Do not change
`DecisionModelStatus`'s variants.

**AC:** probing `nimble` reports 8 194, `tev1:0.8b` 2 048, `laya` 512.

## 6. Phase 2 — the ranking input on a decision call (this repo)

**Landed** with the decision seam: `RoutingPreferences::ranking`, applied by the same re-rank as
`order`, recorded as `RoutingDecision::ranking` (`RankingApplied`). Five tests in
`engine/decision_tests.rs` pin the semantics below.

The gateway gains the ability to be *told* an order, and the memory of where it came from. It gains no
ranking data and makes no ranking decisions of its own.

```rust
// kernel/src/types/request.rs
pub struct Ranking {
    /// The category this order was measured for (e.g. "planner.select"). Free-form.
    pub category: String,
    /// Best first. Same ref grammar and first-match-wins semantics as `RoutingPreferences::order`.
    pub preferred: Vec<CandidateRef>,
    /// Where it came from. Recorded, never consulted for ordering.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<RankingSource>,
}

pub struct RankingSource {
    pub artifact: String,          // e.g. "torii:catalog.model_rankings"
    pub revision: Option<u64>,
    pub generated_at: Option<i64>,
}

// `RoutingPreferences` gains:
pub ranking: Option<Ranking>,

// (No `DecisionRequest` change — it is the adapter boundary, built after selection.)
```

Semantics, each pinned by a test:

- **Never filters.** Gates, `only` and `ignore` decide who is admitted; a ranking only permutes
  survivors. A ranking therefore cannot bypass a context-window skip, a cooldown, a lockout, the
  breaker or the budget gate.
- **Never resurrects.** A skipped candidate stays skipped even if it ranks first.
- **Applied after `sort`**, exactly like `order`, so a metric sort still computes and the ranking then
  reorders its output.
- **Unmatched refs are inert.** A ref naming a model the tenant does not have is recorded in the
  trace, never an error: the artifact's roster drifts from a tenant's availability by design.
- **`ranking` together with `order` is `InvalidRequest`** — two orderings for one call.
- **Absent means byte-identical.** No ranking ⇒ the strategy's order, unchanged, and no new trace
  field populated.
- **No compiled-in ranking.** A test asserts a freshly built `Gateway` orders exactly as it did before
  this feature and that `RoutingDecision.ranking` is `None`; there is no default table to fall back on.

The trace gains:

```rust
// selection.rs
pub struct RankingApplied {
    pub category: String,
    pub source: Option<RankingSource>,
    pub matched: usize,
    pub unmatched: Vec<String>,   // refs no admitted candidate answered
}
// RoutingDecision { …, ranking: Option<RankingApplied> }
```

**AC:** a decision request carrying a ranking tries candidates in that order; the same request without
it is byte-identical to `v0.12.0`; a ranking cannot move a gated candidate; a ranking plus an `order`
is rejected with a message naming both.

## 7. Phase 3 — torii's catalog and ranking store

**Repo:** torii. **Depends on:** Phase 0 (tag) and Phase 1 (artifact).

- `crates/core/src/config.rs`: add `"decision" => Capability::Decision` to `map_capability`; update the
  doc comment.
- `database/import/staging/capabilities.jsonl`: add a `decision` capability row, then `dbd
  reconcile` / `dbd deploy` per `database/README.md` — never a hand-written migration.
- Catalog: decision model rows (`catalog.models` + `model_capabilities` + `model_endpoints`) and the
  `decide` chain, with `context_window` from the probed server value.
- **New: the ranking store**, seeded from `ranking.json` and editable:

  ```sql
  -- catalog.model_rankings (sketch)
  category        text      -- 'planner.select'
  model_id        uuid      -- references catalog.models(id)
  rank            int       -- 1 = best
  accuracy        numeric
  ci_lo, ci_hi    numeric
  ece             numeric
  confident_wrong numeric
  coverage        numeric
  p50_ms          int
  verdict         text      -- winner | tie | insufficient | unranked
  evidence        text[]    -- scenario ids behind the row
  proxy           text      -- e.g. 'tool.route' when the evidence is a stand-in
  corpus_sha      text
  artifact_sha    text
  generated_at    timestamptz
  revision        bigint    -- monotonic per category; bumped on any edit
  edited_by       text
  ```

- Seeding is idempotent and never deletes a hand-edit: a re-seed inserts new revisions for rows whose
  artifact changed, and leaves edited rows alone with a warning. `dbd` owns the DDL; the seed command
  owns the data.
- Tests: `crates/core/tests/catalog.rs` — a decision model and a `Decision` chain survive
  `build_gateway_config`; a decision capability no longer disappears silently; an unranked category
  still yields its chain rows with `within_margin` NULL and no order; a card-sized window never
  reaches the config.

**AC:** the `decide` chain and its models are in `GatewayConfig` with server-true windows; a category's
ranking is readable with its revision and provenance; and `v_ranked_chain(tenant, capability, category)`
returns an ordered, tenant-scoped candidate list — chain rows always present, ordered where a winner
exists, with `within_margin` NULL and a status row saying why where it does not.

## 8. Phase 4 — composing and passing the filtered ranking

**Repo:** torii. **Depends on:** Phase 0 + 2 + 3.

### 8.1 Filtering (the part the user named)

For a given (tenant, capability, run) — and, once a per-user allowlist exists, user — the available
decision set is:

1. the tenant's `decide` chain members with `Capability::Decision`;
2. models the tenant has an enabled endpoint/key for;
3. models the user may use (permission, per-user allowlist, remaining quota);
4. models that **fit** this call: probed window ≥ the gateway's pessimistic estimate for the decision,
   and image support when the payload carries images.

Then: `ranking_rows(category) ⋈ available`, ordered by `rank`. Drop the ranking entirely when

- the category's verdict is `tie` or `insufficient` (nothing to claim), or
- fewer than 2 models survive (nothing to order), or
- the best survivor falls outside **the quality margin** (§8.1a), or the artifact is older than the
  freshness policy.

**Substitution is not free.** If the top-ranked model is unavailable and the next one is a coin flip,
ordering by rank silently converts a quality guarantee into a token bill. Dropping the ranking leaves
the chain's authored order (the deliberate fallback), which is the honest outcome and is visible in
Seiki.

### 8.1a The quality margin (margin, not absolute)

An absolute floor cannot work across these categories: the best measured model is **0.950** on
`tool.route` and **0.630** on `triage.score`, so a single number is either trivially met on the easy
category or unreachable on the hard one. The defensible question is relative — *how much measured
accuracy may this category give up when the top model is unavailable?*

- `margin` is in **accuracy points**, per category, default `0.03`.
- `effective_margin = max(margin, half-width of the top model's CI)`. A tolerance finer than the
  measurement's own uncertainty is not a claim anyone can keep: on the 60-case S4 gates the CI
  half-width reaches ±0.10, so their effective margin is 0.10 whatever the configuration says.
- A model joins the ranking when `accuracy ≥ accuracy_top − effective_margin`, where `accuracy_top`
  is the **category's** top model, available or not. Two tenants with different availability therefore
  share one definition of "good"; only their survivors differ.
- The margin decides **membership**, never position: order stays the artifact's rank order.
- No admissible model ⇒ send no ranking; the chain's authored order takes over and the trace says
  `no ranking: nothing within margin`.
- The margin governs the **ranking, not the chain**. A model outside the margin remains a chain
  candidate — an authored fallback can still beat failing the run — it just never leads on measurement.

Storage: `catalog.ranking_policies(category, tenant_id, margin, updated_at)`, `tenant_id IS NULL` for
the default row. The effective margin and the policy revision are recorded with each call, so a run's
ordering can be re-derived after the fact.

### 8.1b The resolution view (one query, one revision set)

Yes — the resolved unit is **`(tenant, capability, category)` → an ordered chain plus the ranking
that ordered it**, and a plain view is the right place for the static half of that join. Plain, not
materialised: it is read once per run per category from already-indexed tables, so the freshness cost
of a matview buys nothing.

The grain is two axes, and conflating them is the easy mistake: **`capability` is what the chain is
for** (`catalog.chains.capability_id`, already on the chain) and **`category` is what was measured**
(`tool.route`, `gate.review_finding`, …). One is a uuid FK, the other a string in the artifact. The
resolver filters the chain by capability and orders it by category.

There is **no per-user enablement table today** — `chains_for_tenant` is tenant-scoped, and nothing in
`catalog` carries a user id. So `user_id` stays a reserved resolver argument, not part of the view
key, until a per-user allowlist exists: a key dimension that cannot change the answer only multiplies
cache entries.

The category is per **action**, not per run: a run's planning step reads `planner.select`, a review
gate reads `gate.review_finding`. So the resolver is called once per category and a run may hold
several resolved snapshots. The view is still not the whole decision: it resolves *availability and
preference*, while **fit, health and budget stay in the gateway's gates** — per-call facts the
database does not have.

```sql
-- v_ranked_chain(tenant, capability, category): one statement, so the chain and the
-- ranking can never come from different revisions.
with ranked as (
  select r.model_id, r.rank, r.accuracy, r.ci_lo, r.ci_hi
       , (r.ci_hi - r.ci_lo) / 2 as half_width
       , r.revision, r.artifact_sha
  from catalog.model_rankings r
  where r.category = $2 and r.verdict = 'winner'   -- tie / insufficient ⇒ no order at all
), top as (
  select accuracy, half_width from ranked where rank = 1
), policy as (
  select greatest(coalesce(p.margin, 0.03), t.half_width) as effective_margin
  from top t
  left join catalog.ranking_policies p
         on p.category = $2 and p.tenant_id = $1
)
select ct.model_id, ct.router_name, ep.router_model_id, m.context_window
     , r.rank, r.accuracy, r.half_width
     , r.revision as ranking_revision, r.artifact_sha
     , (r.model_id is not null)                                as measured
     , (r.accuracy >= top.accuracy - policy.effective_margin)    as within_margin
     , policy.effective_margin
from catalog.chains_for_tenant ct          -- enabled: override resolution + viability, already done
join catalog.models m on m.id = ct.model_id
left join lateral (                        -- the step's API model id; ONE row, never multiplies grain
  select me.router_model_id
  from catalog.model_endpoints me
  where me.model_id      = ct.model_id
    and me.router_id     = ct.router_id
    and me.capability_id = ct.capability_id
    and me.is_active
  order by me.is_default desc nulls last, me.priority asc
  limit 1
) ep on true
left join ranked r on r.model_id = ct.model_id
left join top on true                      -- chain rows survive "no winner": they are the fallback
left join policy on true
where ct.tenant_id = $1
  and ct.capability_id = $3                -- chain axis; $2 is the measurement axis
order by r.rank nulls last, ct.sequence_order;
```

Four things this shape gets from the existing schema rather than reinventing:

- **It builds on `chains_for_tenant` (torii: `database/ddl/view/catalog/chains_for_tenant.ddl`),
  which is already the source of truth for model enablement** — tenant override → platform
  inheritance, credential viability, price. Re-joining `catalog.chains`/`chain_models` would
  reimplement that union and drift from it the first time the inheritance rule changes.
- **No `model_capabilities` join.** Membership already establishes the model is a decision model;
  the chain's `capability_id` is the capability filter.
- **`sequence_order`**, not `position`; `router_model_id` lives on `model_endpoints` (one row via the
  lateral, the same scalar-subquery trick `chains_for_tenant` uses for price), and `context_window`
  is on `catalog.models`. The first draft of this sketch used three column names that do not exist.
- **Tier-ref steps are still expanded nowhere.** `chain_models` permits `tier_id` steps with NULL
  model/router, and `chains_for_tenant` drops them too — matching the config loader, which also drops
  them today. Expanding tier-refs is a shared prerequisite for both, not something this view can
  solve alone.

What the view returns and what it must not do:

- **Ordered candidates** = chain members the tenant can actually run (and the user, once a per-user
  allowlist exists), with `rank`,
  `within_margin` and the effective margin attached. The caller builds `Ranking.preferred` from the
  rows where `within_margin`, in rank order; the rest remain chain fallbacks (margin governs the
  ranking, not the chain — §8.1a).
- **`verdict = 'tie' | 'insufficient'` yields `within_margin = NULL` on every row, not silence.**
  The filter lives in the `ranked` CTE, so no caller can assemble an order by accident, while the
  chain rows still come back as fallbacks. A companion `v_ranking_status(tenant, category)` returns
  the verdict and the reason — "no measurement" and "nothing within margin" must not look alike, and
  the reason is tenant-specific because the margin is.
- **No health, budget or circuit filtering.** Those are gateway gates that run after admission; a
  view that pre-filtered them would be a second, stale copy of live state.
- **Only a static window check.** The minimum decision size must come from the gateway's own
  estimator (torii already depends on the crate), never a copied `182`; a hardcoded constant in torii
  drifts the first time the gateway's prompt overhead changes.
- **Tenant isolation follows the house pattern, which is not RLS on the view.**
  `chains_for_tenant` is deliberately **not granted to `authenticated`** and has no
  `security_invoker`: it returns all-tenant rows and is read through the `service_role` pool, where
  the tenant predicate is the resolver's obligation. This view is the same class of object. So: no
  grant, a resolver function that takes the tenant as a non-optional argument, and a test asserting a
  resolver call for tenant A cannot see tenant B's rows. "Add RLS to the view" is the wrong fix here:
  the view is read as its owner, so base-table RLS does not apply to it, and a policy on the view
  would look like protection without being it.
- **New indexes are on the new tables only**: `model_rankings(category, verdict, rank)` for the
  `ranked` CTE and `model_rankings(category, model_id)` for the join;
  `ranking_policies(category, tenant_id)` with `tenant_id null` as the default row. The chain side is
  already indexed (`chains`, `chain_models`, `model_endpoints`).

**Caching.** Resolve once per run and reuse the snapshot for every decision of that category. If it is
cached beyond a run, the key must cover every input that can change the answer:

```
(tenant_id, capability_id, category, hash(effective endpoints), chain.revision,
 ranking.revision, policy.revision)
```

`hash(effective endpoints)` rather than model ids: the API model id is per endpoint, so two tenants
with the same model row but different routers are not interchangeable. `permissions.version` joins
this key only when a per-user enablement table exists (see the grain note).

A TTL alone is not invalidation: an edited ranking or a revoked user model must take effect on the
next call, not on the next expiry. Write-through invalidation on any of those tables is the simplest
correct rule; the per-user id does not belong in the key (two users with the same effective model set
and permissions get the same answer), which also keeps cardinality bounded.

**Consistency trap.** Reading the chain and the ranking in two queries can compose an order from two
different edits. One statement (or one repeatable-read transaction), with both revisions returned in
the rows, is the requirement — and the revisions travel into `RankingSource`, so the run's trace names
the exact pair that was used.

Record, per call: category, revision, effective margin, which refs were sent, which were filtered out
and why.

### 8.2 Wiring

- `crates/cli/src/boot.rs`: after `Executor::new(...)`:
  - absent env → `RulePlannerSelector::new(None)` (unchanged);
  - `TORII_PLANNER_SELECTOR=decision` → `DecisionPlannerSelector::new(registry, chain)`, with
    `TORII_DECISION_CHAIN` (default `decide`) and `TORII_DECISION_CATEGORY` (default
    `planner.select`, falling back to a labelled proxy).
- The selector carries the composed per-run ranking and sets `InferenceRequest.routing` (which holds
  `RoutingPreferences::ranking`) on each decision call.
- Resolve the snapshot **once per run per category** (§8.1b), not per decision, and reuse it across the
  run's steps. A resolver failure is not a run failure: log it, send no ranking, and let the chain's
  authored order stand — the gateway must remain fully usable with torii's resolver unavailable.
- **Fail loud at boot** when the chain is missing, is not a `Decision` chain, **carries a different
  capability than the decision one** (the chain's capability, not the benchmark category — one is the
  chain axis, the other the measurement axis), or its members' windows cannot hold the minimum decision — the
  heavy tier's zero-adapter stance. Otherwise every `Select` run dies mid-flight after spend, takes a
  400 from a window the catalog invented, or silently orders by a ranking measured for a different
  task.
- Log once at boot: selector, chain, category, ranking revision (or "no ranking: <reason>").
- Tests: a fake decision provider → `Select` picks a candidate, `PlannerSelected` is journaled, one
  decision call is journaled under `e/__select__` with usage; a **filtered** ranking (one model
  withheld) walks to the next ranked model; a `tie` category sends no ranking and the chain order is
  observed; an undersized window refuses at boot naming both numbers; the default path is byte-identical.

**AC:** `torii run submit` on a `PlannerRef::Select` graph completes; the run's trace shows the
category, the ranking revision and the filtered-out models; run spend includes exactly one decision
call; no silent fallback to text selection.

## 9. Phase 5 — Seiki visibility

**Repo:** torii (API) + `apps/admin` (Seiki).

- Models page: decision models, their **effective window**, and which chain type enables them (the page
  currently derives enablement from key-configured **chat** chains).
- A rankings view per category: verdict, rows, CI, provenance (artifact, corpus sha, generated_at,
  revision, edited_by), and an edit action that bumps the revision — this is the "when needed it can be
  modified" surface.
- `/v1/routing`: add each chain's capability and its category; label the `decide` chain.
- `/v1/models/available`: keep chat-only, or add `?capability=decision`; do not silently change its shape.
- Requests/ledger: `capability = decision` must render and not be filtered out; show the ranking
  revision that was in force for the call.
- In-run decision spend is journaled by the executor, not written as `inference_calls` rows — a
  pre-existing gap; decide it separately.

**AC:** an owner sees the `decide` chain, its models' windows, the ranking behind it and can edit it;
a run's decision call is distinguishable from a chat call.

## 10. Phase 6 — tenant-facing decisions (only if still wanted)

`POST /v1/decide`, mirroring `/v1/chat`'s governance: auth + `require_*`; model enablement from
decision chains; budget reserve/commit on an input-only estimate; redaction, trace, ledger with
`capability = decision`, SIEM; validation (1–64 questions, ≥2 criteria). A full slice, not wiring.

## 11. Ordering and conflicts

- **torii checkout:** 350 behind `origin/develop`, no `crates/cli`; start from an updated `develop` or
  a worktree.
- **Queue:** torii #61 (bun pin), then wave C #47 (gate authorization). Phase 3 touches the catalog/DDL
  and can run in parallel; Phase 4 touches `boot.rs` and should follow #47.
- **Pin discipline:** one gateway ref across every crate (TM-8a); every `sensei-*` pin and both
  `[patch]` blocks move in one commit.
- **Kernel break:** `RoutingPreferences` and `RoutingDecision` each gain a field, which is
  source-breaking for exhaustive struct literals; both ride the same `0.13.0` as `choose`.

## 12. Risks and traps

| Risk | Why it bites | Mitigation |
|---|---|---|
| Card-sized `context_window` | `nimble`'s card says 262 144, the server serves 8 194; the gateway's gate passes and the server 400s every call | seed from the probed `num_ctx` (Phase 1.5 / 3); boot check |
| 64 KiB body cap | a decision over 64 KiB is a 413 regardless of tokens | keep planner states small; larger states leave the Ollama router |
| Silent downgrade by filtering | the top-ranked model is unavailable, the next is a coin flip, the call still "succeeds" | the margin rule (§8.1a), effective margin never below the CI half-width; no ranking when nothing is within it; shown in Seiki |
| A tie read as a ranking | the corpus ties constantly (s1a: five models; s1b: three) | verdict gates the ranking; no ranking sent for `tie`/`insufficient` |
| Two orderings in one call | `order` and `ranking` would silently fight | `InvalidRequest`, test-pinned |
| Ranking mistaken for a filter | a caller expects it to drop models | docs + tests: gates/`only`/`ignore` filter; a ranking only permutes |
| Gateway grows a default ranking | "just fall back to the artifact" is the tempting shortcut | no data, no default, a test that absence is byte-identical |
| Stale artifact | a ranking survives a model upgrade it never measured | generated_at + corpus sha in the store; freshness policy |
| Stale resolved chain | a cached order outlives an edit to the ranking, chain, policy or a user's models | cache key covers every revision; write-through invalidation; snapshot per run |
| Mixed-revision snapshot | the chain and the ranking are read in two queries and straddle an edit | one view statement / repeatable read, both revisions carried into `RankingSource` |
| Torii copies the token estimator | a hardcoded minimum decision size (182) drifts from the gateway's prompt overhead | call the gateway's own estimator via the crate |
| The `planner.select` proxy | no committed cases yet, so `tool.route` would stand in | label the proxy in the artifact and in Seiki |
| Merging torii code before the tag | CI strips `[patch]`, compiles against v0.12.0 | Phase 0 first |
| Decision spend inside runs | the executor journals its own usage | decide the ledger stance explicitly (Phase 5) |
| Silent capability drops | `map_capability` returns `None` with a `warn!` | assert the mapping, not the absence of errors |
| The resolver becomes a hard dependency | a view outage or a bad join would stop decisions entirely, even though the chain alone is sufficient | ranking is advisory: resolver failure ⇒ no ranking, chain order, logged; a test that a decision works with the resolver down |

## 13. Not in scope

- Calibration fitting, multi-question cases, a bench CI job (needs model servers), and a `noul`/`score`
  shape on the `ModelDispatch` port.
- Hosted decision models (`clef`, `clef-flash`, `pplx-decider`, `typesafe`, OpenRouter): config-ready,
  unmeasured — they need keys and a spend cap before they can enter a ranking.
- A gateway-side `SortKey` for quality. Ordering by measured rank is the caller's data (Phase 2), not a
  gateway sort; a `SortKey::Measured` would put ranking knowledge back inside the gateway, which is the
  thing D4 forbids.
- Ordering *within* the margin by price or latency instead of accuracy rank. The evidence genuinely
  cannot separate those models, so it is a plausible optimisation — but a policy nobody has asked for,
  and it would make a run's ordering depend on live prices rather than on the ranking.

## 14. The one-line version

The gateway publishes a pinned corpus and a per-category ranking artifact and learns to be *told* an
order on a decision call (never to hold one); torii stores that ranking, edits it, filters it to what
each tenant and user can actually run, passes it per call, and only when the evidence supports an
order — and Seiki makes the whole chain, window and provenance visible.
