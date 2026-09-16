# SP-OPS-1 — consolidation: connect the core, fix the three live bugs

**Opened:** 2026-09-16 · **Base:** `develop` = `75d4991` (= `main` `e6658d6` + merge)
**Grounding:** `docs/analysis/2026-09-16-agentic-execution-capability-survey.md`

The survey found the durable core strong and the composition root empty: twelve seams built,
tested and wired to nothing, plus three live bugs. This slice fixes the bugs and wires what can be
wired. It runs **before** any new feature, because with an empty composition root each new feature
becomes the thirteenth dead seam.

Increment ids map 1:1 onto the analysis sections — no parallel numbering scheme.

| Increment | Analysis | What | Size |
|---|---|---|---|
| **SP-OPS-1.1** | §2.3 | Run-scope the durable blackboard | Medium |
| **SP-OPS-1.2** | §2.4 | `Expand`'s planner default | Small |
| **SP-OPS-1.3** | §2.2 | Transient-failure retry, bounded | Large |
| **SP-OPS-1.4** | §2.5 | Lease renewal vs. batch bound | Design call |
| **SP-OPS-1.5** | §2.6 | Reconciler policy for shipped Mutation tools | Design call |

**Execution order: 1.2 → 1.1 → 1.3** (smallest first, to prove the loop on the cheapest change).
1.4 and 1.5 need a decision from the user and are not started blind.

---

## Two corrections to the survey's own fix advice

Both were stated in the 2026-09-16 report and are wrong; recorded here so the plan is not built on
them.

1. **"Fix §2.2 in `Scheduler::record`."** False. `RunOutcome.failed` is
   `Option<(NodeId, String)>` (`executor/mod.rs:112`) — a bare message. `record` has no signal to
   classify transient vs. permanent. The fix belongs in `classify_gateway_error`
   (`executor/support.rs:667`), which *does* see the typed `GatewayError`.
2. **"§2.3 is small."** False. `Scope::Run` is a **unit variant** (`context.rs:19-22`) and
   `ContextStore::{put,get,insert_ref}` take no `RunId`. Run-scoping is a trait signature change,
   not a mapping fix.

---

## SP-OPS-1.2 — `Expand`'s planner default (§2.4)

**Grounded decision.** The obvious fix — make `Select` the `#[default]` — is **wrong**: no
planning-area registry content ships. The only `area: "planning"` literal outside tests is a
`#[cfg(test)]` fixture at `torii/src/cmd/config.rs:527`, so `Select` would swap `"no planner
wired"` for `"no planning candidates"`. Neither variant works out of the box.

**Fix:** drop `#[serde(default)]` from `Expand.planner` (`graph.rs:80`) so the field is
**required**. The failure moves from mid-run (after tokens are spent) to parse time, with serde
naming the missing field.

**Safe for durable graphs:** `scheduled_runs.graph` is serde-serialized and `planner` carries no
`skip_serializing_if`, so every stored graph already has the field. Verify before landing.

- **Red:** a graph JSON with an `Expand` node omitting `planner` currently deserializes to
  `Injected`; assert it is a parse error naming the field.
- **Done:** parse fails; a graph that specifies `planner` still round-trips; stored-graph shape
  unchanged.

## SP-OPS-1.1 — run-scope the durable blackboard (§2.3)

**Shape chosen to avoid a journal-format bump.** Do **not** change `Scope::Run` to
`Scope::Run(RunId)` — `Scope` is `Serialize` and embedded in `JournalEvent::ContextWrite`, so that
would change the durable journal encoding and force a `FORMAT_VERSION` bump.

Instead: add a `run: RunId` **parameter** to `ContextStore::{put,get,insert_ref}` and a `run_id`
**column** to `context_refs`, with PK `(run_id, scope_kind, scope_id, ctx_key)`. The `Scope` enum
and its serde shape are untouched ⇒ existing journals still load.

This also fixes `Scope::Node`, which collides across runs for the same reason.

- **Red:** two distinct `RunId`s publishing the same node id — currently `ContextKeyCollision`,
  must both succeed and read back independently. Write it against `InMemoryContextStore` first
  (identical defect, `stores.rs:55`), so the proof needs no database; mirror it for Postgres.
- **Also assert:** the same run re-publishing one key still collides loudly (do not weaken the
  guard into last-write-wins).
- **Schema:** dbd project is **pre-release** (`database/{design.yaml,ddl}`, no migrations dir) ⇒
  edit DDL + `dbd reconcile`. Do **not** hand-write a migration.
- **Done:** re-running the same graph twice against a persistent DB completes both times; the
  `e2e_pg.rs:141` run-unique-marker workaround becomes unnecessary (leave the markers, drop the
  comment's claim).

## SP-OPS-1.3 — transient-failure retry, bounded (§2.2)

The executor already re-attempts a failed `ModelCall`/`Agent` on the next drive
(`executor/mod.rs:303-310`); nothing reaches it because the scheduler terminalizes. Reuse the
existing pause machinery rather than inventing a retry path.

### Third correction: the transient signal does not survive the gateway boundary

Grounding for this increment found the planned shape ("classify transient `GatewayError`s in
`classify_gateway_error`") **unbuildable as written**, for a reason worth recording.

The gateway *does* classify. `exhaustion.rs` computes a `GateContribution` per candidate —
`Timed(Instant)` / `Terminal(HumanAction)` / **`HardFailure`**, the last documented as
"non-limit fault (500 / network / unclassified)", i.e. exactly the retryable class. But
`all_gated_error` returns `None` as soon as any `HardFailure` is present
(`exhaustion.rs:52-57`), and the caller then raises `AllAttemptsFailed { attempts, errors,
attempts_detail }` — where `errors` is a flattened **string** and `attempts_detail`'s
`AttemptStatus` is only `Success | Failed`, with the cause in a free-text `error: Option<String>`.

**So the distinction is computed and then thrown away**, and by the time the orchestrator sees the
error the only way to recover it is to string-match provider prose. That is how a permanent auth
failure gets retried forever, or a transient one gets failed — a wrong retry decision that spends
money either way. This is the same "built, then not connected" shape as §2.1; the classification
is not missing, it is discarded one layer above the consumer.

**Replacement approach:** preserve it. Give `AllAttemptsFailed` a typed retryability signal
(`retryable: bool`, or a `FailureKind`) populated from the `GateContribution`s the gateway has
already computed, then `classify_gateway_error` reads a type instead of a message. Cost: a public
field on a `kernel` error variant (breaking for embedders — pre-1.0, but it is an API change),
plus the gateway construction sites, plus the orchestrator arm. Scope is three crates, not one.

**This is a decision, not a detail** — an alternative is to leave the gateway alone and retry on
*any* non-gated failure with a small bound, accepting that permanent failures burn N attempts
before dying. Cheaper and orchestrator-local; wrong-but-bounded rather than right.

### Part 1 — DONE (`741a93e`)

`AllAttemptsFailed.retryable`, computed from the contributions, wired into the **existing**
`is_retryable()` rather than added beside it — that method excluded the aggregate because
pre-aggregation it could not know, and it has zero production callers, so this connects a dead
seam instead of duplicating it. Both tests mutation-verified.

### Part 2 — the retry loop. One trap, recorded before it is rediscovered

**A `Pause` disposition appends no `NodeFailed`.** In both dispatch arms, `Fail` appends
`NodeFailed` and `Pause` appends only `RunPaused`. So turning a transient failure into a pause —
the obvious implementation — means the attempt counter (folded from `NodeFailed` occurrences)
**never increments, and the run retries forever.** That is the poison-run shape this increment
exists to avoid, reintroduced by its own fix.

Therefore the retry arm must append **both**: `NodeFailed` (the honest record of this attempt, and
the thing that counts) *and* `RunPaused` (the deadline the scheduler wakes on). Appending
`NodeFailed` for a `ModelCall` is already the normal path and is safe — `Fold::failed` is read as
a verdict only by waiting kinds via `gate_precheck`, and `ready_nodes` works off the per-drive
`DriveState`, which never consults it.

Counting: `attempts_so_far` = prior `NodeFailed` rows for the node (this attempt's row is appended
after the decision). Retry while `attempts_so_far < MAX_ATTEMPTS - 1`, so attempts 1 and 2 pause
and attempt 3 fails terminally.

### Once the signal exists

- Transient ⇒ `Pause { resume_after: Some(now + backoff) }` instead of `Fail`.
- **A bound is mandatory** — unbounded retry is the poison-run shape the survey also found, and
  `RunPaused` has no fold guard, so each wake grows the journal.
- Attempt count: fold `NodeFailed` occurrences per node. Verified available — the `ModelCall`
  `Fail` arm appends `NodeFailed` **unconditionally** every drive (`mod.rs:1541`), and the
  `failure_messages` dedup set is read only by `fail_loop`, so repeated identical failures each
  leave a row. No schema or journal-format change needed.
- Two dispatch sites, not one: `executor/mod.rs:1518` (ModelCall) and `executor/agent.rs:1519`
  (Agent).
- **Red:** a provider failing transiently once ⇒ the run completes on the next drive; failing
  forever ⇒ terminal after N attempts, not an infinite wake loop.
- **Defaults (my call unless overridden):** N = 3 total attempts; exponential backoff from 2s,
  ×2, capped at 60s; bound is **per-node** (the fold already keys per node, and one flaky node
  should not consume a sibling's budget).

## SP-OPS-1.4 / 1.5 — the two design calls

**1.4 lease (§2.5):** renew mid-drive · shrink `CLAIM_BATCH` to what fits one lease · per-run
advisory lock on the journal. Not started until chosen.

**1.5 reconciler (§2.6):** ship a reconciler for `fs_write` (feasible — the file is evidence) ·
refuse to register a Mutation tool with no reconciler · document and leave. Not started until
chosen.

---

## Gate

Per increment: red test observed failing → smallest fix → green → **full** `cargo test --workspace`
→ `cargo fmt --all` → clippy `-D warnings` via the rustup toolchain (Homebrew `rustc` 1.97 shadows
rustup's 1.98). Commit one increment per commit; checkpoint after each.

Mutation-verify every "guarded by X" claim — an exit code of 101 may be a compile error, so grep
the log for `panicked at`.
