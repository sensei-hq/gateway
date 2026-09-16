# Capability survey — what an embedder can do today vs. what production agentic execution needs

**Date:** 2026-09-16 · **Base:** `main` = `e6658d6`, `develop` = `75d4991`, 1831 tests green
**Method:** six independent read-only lenses, each blind to the others: scale/backpressure,
observability, multi-tenancy, embedder API, failure/recovery, streaming/interactivity.

Every finding in §2 was **re-verified by hand** against the source before being recorded here;
the citation is the line I read, not the line the lens reported. Findings I did not personally
re-derive are in §3 and are labelled as such. Two lenses independently reached the §2.3
double-drive conclusion without seeing each other's output.

The embedder lens is the most credible of the six: it compiled and ran a real consumer crate
outside the repo against path-deps, with no feature flags, and quotes compiler errors and runtime
output verbatim. Its findings are empirical rather than grep-derived.

---

## 1. Verdict

**The hard part is built.** Durable journal-and-fold replay, effect classes with two-phase
mutation and in-doubt reconcile, hierarchical graphs (Subgraph/Branch/Expand/Loop), a planner with
three selection strategies, permission enforcement with runtime grant ceilings, secret redaction,
a credential broker, workspace jails, OS-level subprocess confinement on **both** macOS and Linux,
Postgres persistence with a config-version fence, a durable scheduler, HITL gates, and context
budgeting. That is a genuinely strong durable-execution core and it is the part that is expensive
to get right.

**The composition root is empty.** The dominant defect shape across all six lenses is not a
missing feature — it is a feature that is *built, tested, and connected to nothing*. This is
systemic, not incidental (§2.1).

**Three defects are live bugs, not gaps** (§2.2–2.4). One of them breaks the most ordinary thing a
user can do: run the same graph twice.

---

## 2. Verified findings

### 2.1 The systemic pattern: built, tested, wired to nothing

Twelve seams, each reachable in the library and unreachable in the shipped binary. Verified by
enumerating and **reading** call sites (not counting them — see
`regex-counts-in-gateway-are-unreliable`):

| Seam | Definition | Production callers |
|---|---|---|
| `with_planner` | `executor/mod.rs:861` | **0** (20, all `tests.rs`) |
| `with_reconcilers` | `executor/mod.rs:906` | **0** (11, all `tests.rs`) |
| `with_hooks` | `executor/mod.rs:927` | **0** (15, all `tests.rs`) |
| `with_concurrency` | `executor/mod.rs:821` | **0** — never called, *not even in tests* |
| `with_lease` | `scheduler.rs:56` | **0** |
| `GatewayStore` / `with_store` | `gateway/src/store.rs` | **0**; only impl is `InMemoryStore`, "for testing" |
| `Gateway::execute_stream` | `engine/stream.rs:41` | **0** |
| `latest_snapshot` | written every round, **never read** | **0** |
| `ExecutionTrace` | constructed only inside `#[cfg(test)]` | **0** |
| `Attempt` fallback trail | populated at 3 sites, then discarded | **0** readers |
| five discovery tools | built SP-REG | **0** |
| `crates/vault` | compiles | **linked by no crate** |

`PlannerRef::Select` was a thirteenth until SP-REG-0 (PR #63) fixed it. **§2.4 is its live
sibling.** The architecture is right; `crates/torii/src/boot.rs` is the single highest-leverage
file in the repository.

Distinguish two costs — they are not the same work:

- **Wiring gaps** (one line each): `with_hooks`, the discovery tools, `with_lease`.
- **Missing implementations** (real work): no durable `GatewayStore` impl exists to wire; no
  reconciler can be shipped generically because reconciliation is application-specific; streaming
  needs a consumer seam that does not exist.

### 2.2 A transient provider 500 permanently kills a run — highest reachability

The executor **deliberately** supports retry-on-resume. `executor/mod.rs:303-310`:

> *"A `NodeFailed` does not make a node terminal in general: a `ModelCall` or `Agent` node whose
> provider died journals one and **RE-ATTEMPTS on the next drive**, which is the documented resume
> contract."*

The scheduler forecloses it:

- `classify_gateway_error` (`executor/support.rs:667-706`) pauses only on two `AllGated` arms;
  `other => GatewayDisposition::Fail(...)`. A 500, a network fault and a timeout are all `Fail`.
- `Scheduler::record` (`scheduler.rs:165-170`): `Ok(o) if o.failed.is_some() =>
  record_terminal(RunStatus::Failed)`.
- `claim_due` (`orchestrator-store/src/postgres.rs:945-954`) selects only
  `status='paused' AND next_wake <= now` **or** a stale `'waking'`. A `failed` row is never claimed
  again.

Two layers each locally correct, composing into a defect. **Needs nothing but time to hit**, has no
recovery command, and the cheapest correct fix is in `Scheduler::record` — not in the classifier.

### 2.3 The durable blackboard is not run-scoped — a live single-tenant bug

`PostgresContextStore::scope_cols` (`postgres.rs:321-326`):

```rust
Scope::Run => ("run", String::new()),
```

The DDL is self-incriminating — `database/ddl/table/orchestrator/context_refs.sql`:

```sql
scope_id text not null,   -- run id or node path
primary key (scope_kind, scope_id, ctx_key)
```

The schema says a run-scoped row carries **the run id**; the code writes the empty string. So the
durable key is `("run", "", <node_id>)` — global to the deployment, forever. A different `RunId`
provides no separation whatsoever.

Chain, each link read directly:

1. `publish_context` (`executor/mod.rs:1667-1681`) writes every completed node's output under
   `Scope::Run` keyed by the **bare node id**.
2. `put` (`postgres.rs:381-396`) is a plain insert, deliberately mapping a unique violation to
   `ContextKeyCollision` — *"LOUD collision … not a silent overwrite"*.
3. `apply_node_result` (`executor/mod.rs:1256`) propagates it with `?`, aborting **the whole
   drive** — after the node completed and the tokens were paid for.
4. `boot::heavy:413-418` wires `PostgresContextStore` in **production**.

**Consequence: submitting the same graph twice fails the second time.** Node ids are author-chosen
and stable, so a re-run collides on its first completed node. It is also a permanent poison pill —
the failed `put` journals no `ContextWrite`, so the fold guard never engages and every resume
re-collides.

This is known and worked around in the repo's own suite, `crates/torii/tests/e2e_pg.rs:141-146`:

> *"a `Scope::Run` write carries an EMPTY scope id, so two runs publishing the same node id collide
> LOUDLY (`ContextKeyCollision`). Sharing the bare `n1` of the tests above would make this suite
> fail on its second run against a persistent database."*

The tests inject a run-unique marker to dodge it. Production graphs have no such discipline.

### 2.4 `Expand` via JSON is dead on the default path — the SP-REG-0 sibling

- `PlannerRef` derives `Default` with `#[default] Injected` (`orchestrator-core/src/graph.rs:186-194`).
- `Expand.planner` is `#[serde(default)]` (`graph.rs:80-81`).
- `with_planner` has **zero** production callers (§2.1).
- `PlannerRef`'s own doc: *"`Injected` = the slice-3 `Planner` trait (**deterministic/test**)"*.

So any JSON graph with an `Expand` node that omits `planner` deserializes to the **test-only**
variant and fails with `"expand e1: no planner wired"`. The embedder lens reproduced this through
the exact `boot::heavy` builder chain.

**The fix is the default, not the wiring.** `Injected` is *meant* to be embedder-supplied; the
defect is that a test variant is what the production JSON path selects. Either make `Select` the
`#[default]`, or drop `#[serde(default)]` so the field is required and the failure is at parse time.

### 2.5 Concurrent double-drive — corroborated by two independent lenses

- `DEFAULT_LEASE_SECS: i64 = 60`, hardcoded; `with_lease` has no production caller.
- `claimed_at` is written only at enqueue and claim, cleared on pause/terminal — **never renewed**.
  No renewal primitive exists (the single `renew`-shaped grep hit is a test fixture string,
  `"Approve the ACME MSA renewal?"`).
- `tick()` (`scheduler.rs:96-127`) claims `CLAIM_BATCH = 64` in one `claim_due`, all stamped with
  the same `claimed_at`, then drives them **serially** in a `for … .await` loop.
- `claim_due` reclaims `status='waking' AND claimed_at < now - lease`.

**No single run needs to be slow.** A 64-run batch averaging 2s puts run #31 past its lease before
it starts; a second worker reclaims and drives it concurrently with the first.

The scheduler header asserts the opposite:

> *"A double-drive is harmless (idempotent resume: fold + memo, zero re-spend)."*

True for a **sequential** re-drive, where the first drive's effects are already journaled and the
memo fences them. False for two **concurrent** drives: both `journal.load` before either writes, so
the memo has nothing to fence with and a plain `ModelCall` is double-spent. Same shape as §2.2 — a
true premise with a scope error. Notably this is *not* on SP-DATA-3's deferred list; it was reasoned
about and concluded safe.

### 2.6 No reconciler ships, while two Mutation tools do

`boot.rs:434-439` registers `FsWriteTool` and `ShellTool`, both `EffectClass::Mutation`.
`with_reconcilers` has zero production callers and `crates/torii/src` contains no reconciler
reference at all. Absent a provider, `agent.rs:1156-1159` yields `Indeterminate` →
`RunPaused{resume_after: None}` → NULL `next_wake` → never auto-woken. `force_wake` re-reconciles
and returns `Indeterminate` again. **A crash mid-mutation parks the run forever**, and the pause
reason names only a sha256 hash — no tool name, no remedy.

---

## 3. Reported but not personally re-derived

Recorded for follow-up; treat as plausible, not established.

- **Observability is absent outright.** No Prometheus/OTel/statsd dependency; zero spans; 27
  `tracing` call sites workspace-wide; no run id on any log line; unstructured stderr text.
- **Per-tenant/per-agent cost data is never written.** `InferenceCall` has the right shape, but no
  `GatewayStore` is attached and `session_id`/`project_id` are hardcoded `None` at all three
  production sites. There is also no table to write to.
- **Spend is surfaced only for budgeted runs** — `cmd/run.rs:64,81` gate the field on
  `if let Some(cap) = budget`, though it is computable for any run.
- **`shell` is not path-confined.** `ShellTool::required` returns no paths, so the executor's jail
  pre-check short-circuits on `need.paths.is_empty()`; OS sandboxes allow whole-filesystem *reads*
  on both platforms. With no workspace GC, every workspace ever created may be readable.
- **The sandboxed child inherits the daemon environment**, including `DATABASE_URL` with its
  password. No `env_clear()` anywhere.
- **`NetworkPolicy::Hosts([...])` is allow-all on both platforms** — the declaration layer
  implements wildcard matching the enforcement layer ignores.
- **`RunPaused` has no fold guard**, so a re-waking run grows its journal without bound — in a
  codebase that added exactly this guard to `NodeFailed`/`NodeSkipped` for the same reason.
- **`torii run cancel` is a row update**, not cancellation; the in-flight drive runs to completion
  and its outcome is silently discarded.
- **`Ok(RunOutcome)` is returned on total failure** at every entry point; the embedder must check
  `.failed`/`.paused` and nothing says so.
- **Omitting `with_context_store` silently blinds every Agent to its `Hard` deps** —
  `resolve_context` (`executor/mod.rs:1728-1730`) returns an empty `Vec` with no store. *(Code path
  verified; the empirical prompt-capture proof is the lens's.)*
- **No network API exists** — no HTTP server, websocket or bus is even a dependency.
- **No examples, no doctests, no embedder docs**; the orchestrator is absent from `docs/llms/` and
  from the README's "consuming it" list.

---

## 4. What it takes

Two different bars, and conflating them is how this gets mis-scoped.

### Bar A — embed as a Rust library, single tenant, one worker

Small. This is days, not slices:

1. Fix §2.3 (run-scope the context key: `Scope`, `scope_cols`, the PK, a migration).
2. Fix §2.2 (retry in `Scheduler::record` rather than terminalizing).
3. Fix §2.4 (change the default).
4. Wire `with_hooks` + the discovery tools; add a `#[must_use]`-shaped affordance for §3's
   `Ok`-on-failure.
5. Ship one example and one sample graph JSON.

### Bar B — serve it as a product

Substantially larger, and mostly *not* in the durable core:

- **A network API.** None exists; no server framework is a dependency. Everything web-based is
  gated on this.
- **Streaming to the caller.** The gateway's is complete and unconsumed; the orchestrator boundary
  needs a seam. Not blocked by determinism — journal the assembled output, stream the deltas.
- **Metrics and correlation.** Nothing to alert on today. `OrchestratorHooks` already carries
  `RunId` on all 13 callbacks, so this is cheaper than it looks.
- **A tenant dimension.** The big one: a `tenant_id` across 8+ tables, scoping on
  `claim_due`/`list_paused`/registry/config-generation, an authn/authz layer, and per-tenant
  `Gateway` instances so lockout state stops bleeding. **Gated on §2.3** — nothing else matters
  while any two runs can collide.
- **Lease renewal or per-run mutual exclusion** (§2.5) before running more than one worker.
- **A durable metering store** plus populating `AuthContext`, or per-tenant cost stays unanswerable.

---

## 5. Recommendation

One consolidation slice before any new feature. §2.2, §2.3 and §2.4 are small, independent, and
each has an obvious red test; §2.5 and §2.6 need a design decision (renew vs. shrink the batch vs.
per-run locking; ship a reconciler or refuse to register Mutation tools without one).

The reason to do it *first* is the pattern in §2.1: with an empty composition root, every feature
added now becomes the thirteenth dead seam. The marginal value of new capability is currently lower
than the marginal value of connecting what exists.

---

## Provenance

Lens outputs are in this session's subagent transcripts under
`~/.claude/projects/-Users-Jerry-Developer-gateway/<session>/subagents/`. Related memories:
`verify-review-findings-before-acting` (7 of 10 blocking findings were refuted last time — hence
§2 vs §3), `regex-counts-in-gateway-are-unreliable` (every count above was read, not counted).
