---
title: Persistence Store
doctype: feature
module: observability
status: implemented
source: crates/gateway/src/store.rs
---

# Persistence Store

The `gateway` crate has **no database of its own**. It never opens a connection,
runs a migration, or picks a storage engine. Instead, persistence is delegated
to the consumer through the `GatewayStore` trait: the consumer (e.g. the daemon)
implements the trait against whatever backing store it uses and owns the schema,
connection pool, and lifecycle.

Source: `crates/gateway/src/store.rs`.

## Purpose

`GatewayStore` is the contract for recording and querying the two durable
artefacts the gateway produces:

- **Inference calls** — one accounting record per model invocation (adapter,
  model, tokens, cost, status, timing). Used for spend tracking and per-session
  history.
- **Execution traces** — the structured `ExecutionTrace` for a request (the
  candidates considered, what was skipped, each attempt, cost estimates). Used
  for debugging and observability.

By keeping this behind a trait, the crate stays storage-agnostic: the same
gateway logic can be backed by Postgres, SQLite, an in-memory map, or anything
else the consumer provides.

## Domain types

These are plain data structs the consumer stores and returns; all derive
`Serialize`/`Deserialize`.

### `CallStatus`

```rust
pub enum CallStatus { Success, Failed }
```

`#[serde(rename_all = "snake_case")]` — serialises to `"success"` / `"failed"`.

### `InferenceCall`

One accounting record per model invocation.

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `Uuid` | Primary key for the call. |
| `session_id` | `Option<Uuid>` | Owning session, if any (drives `get_inference_calls_by_session`). |
| `project_id` | `Option<Uuid>` | Owning project, if any. |
| `capability` | `Capability` | The capability requested (text chat, embed, etc.). |
| `chain_id` | `Option<String>` | Fallback chain that produced the call, if selection went via a chain. |
| `adapter` | `String` | Adapter / router id that served the call. |
| `model` | `String` | Internal model id. |
| `api_model_id` | `Option<String>` | Provider-facing model id actually sent, if different. |
| `input_tokens` | `Option<u32>` | Input tokens (may be unknown). |
| `output_tokens` | `Option<u32>` | Output tokens (may be unknown). |
| `cost_usd` | `f64` | Cost of the call in USD. |
| `duration_ms` | `u64` | Wall-clock duration of the **whole attempt** — from just before adapter dispatch to the point the result is complete. Identical in meaning on both paths: for a streamed call it covers acquisition *and* generation, not generation alone (see below). |
| `status` | `CallStatus` | Success or failure. |
| `error_type` | `Option<String>` | Error classification when failed. |
| `fallback_sequence` | `u8` | Position in the fallback walk (0 = first candidate). |
| `recorded_at` | `DateTime<Utc>` | When the call was recorded (the field spend queries filter on). |

#### Three things to know about streamed rows (SP-ROUTE-1.2)

**A streamed call is metered whether it succeeds or fails.** Two separate gaps
closed here, and they had different causes.

*Successes* were lost to **event ordering**: the `insert_inference_call` write
sat *after* the terminal `StreamEvent::Done` was yielded, and in an
`async_stream` generator code after a `yield` runs only on the **next poll** —
so a consumer that stopped at the terminal event (the normal SSE shape) produced
**no row at all**. The write now precedes the `yield`.

*Failures* were lost to **absence**: neither streaming failure path wrote a row
at all, for any consumer, however thoroughly it polled. A stream that died
mid-generation — after the provider had generated and reported real tokens — and
a stream whose every candidate failed at setup both recorded nothing, while
`execute` wrote a `CallStatus::Failed` row for the analogous exhaustion. Unary
one row, streamed zero, same adapter. Both streaming paths now write one, before
their terminal `StreamEvent::Error`, mirroring `execute` field-for-field:

| Outcome | Row | `output_tokens` | `cost_usd` |
|---|---|---|---|
| Stream completes | `Success` | reported usage | costed from usage |
| Stream dies mid-generation | `Failed` | usage reported before the death | costed from that usage |
| Every candidate fails at setup | `Failed` | `None` — unknown, as `execute` writes | `0.0` |

The middle row is the one that differs from `execute`, and only because
`execute` has no partial-success concept: those tokens were generated and the
provider bills for them, so discarding them would knowingly under-count spend.
The last row uses `None` rather than `Some(0)` deliberately — a setup failure
does not prove the provider generated nothing, only that no usage was reported.

Expect more rows than before the upgrade, across successes *and* failures, and a
higher streamed-traffic total. Nothing is back-filled.

**`duration_ms` changed meaning for streamed rows, once.** It used to be
measured from the moment the stream was obtained — generation time only — while
`execute` wrote the same column with the whole attempt's wall time, so this
table mixed two quantities under one name with nothing in the row to say which.
Both paths now record the total attempt span. Rows written before that change
still carry the old quantity and cannot be repaired (the acquisition span of a
past call was never recorded anywhere), so **analytics spanning the upgrade sees
a one-time upward step**. Annotate the date; see `docs/llms/upgrading.md`.

**Metering is best-effort in latency as well as in success — the streaming write
is time-bounded.** On both paths a store *error* is logged at `warn` and never
surfaces to the caller. On the streaming path the write also runs under a
**2-second ceiling**, because it now sits *ahead* of the terminal event: without
a bound, a saturated connection pool or an unreachable database would hold the
terminal event open, and a consumer with a per-event timeout would receive the
full content and then **no terminal event at all** — losing the tokens, the cost
and the routing decision, and cancelling the in-flight write so no row landed
either. Exceeding the ceiling drops the row and logs loudly.

There is no knob to raise it: it is a liveness guarantee, not a tuning
parameter, and raising it would reintroduce the block it prevents.

**So do not treat an absent row as proof a call did not happen.** If you
reconcile against a provider invoice, a persistent shortfall on streamed traffic
means your store is too slow to meet the ceiling — check your logs for the
budget warning rather than assuming the calls were not made.

### `StoredTrace`

An `ExecutionTrace` wrapped with storage metadata.

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `Uuid` | Primary key for the trace. |
| `inference_call_id` | `Option<Uuid>` | Links the trace back to an `InferenceCall`, if one exists. |
| `trace` | `ExecutionTrace` | The full structured trace payload. |
| `created_at` | `DateTime<Utc>` | When the trace was stored. |

## The `GatewayStore` trait

```rust
#[async_trait]
pub trait GatewayStore: Send + Sync {
    async fn insert_inference_call(&self, call: &InferenceCall) -> Result<Uuid, GatewayError>;
    async fn get_inference_calls_by_session(&self, session_id: Uuid) -> Result<Vec<InferenceCall>, GatewayError>;
    async fn get_spend_since(&self, since: DateTime<Utc>) -> Result<f64, GatewayError>;
    async fn get_spend_by_model_since(&self, since: DateTime<Utc>) -> Result<Vec<(String, f64)>, GatewayError>;

    async fn insert_execution_trace(&self, trace: &StoredTrace) -> Result<Uuid, GatewayError>;
    async fn get_execution_trace(&self, id: Uuid) -> Result<Option<StoredTrace>, GatewayError>;
    async fn get_traces_by_call(&self, inference_call_id: Uuid) -> Result<Vec<StoredTrace>, GatewayError>;
}
```

It is `async` (via `#[async_trait]`) and `Send + Sync` so a single
implementation can be shared across tasks. All methods return
`Result<_, GatewayError>`, so a consumer maps its storage errors into the
crate's error type.

### Inference-call methods

- **`insert_inference_call(&self, call) -> Uuid`** — Persist one `InferenceCall`
  and return its id. Implementations typically echo back `call.id`.
- **`get_inference_calls_by_session(&self, session_id) -> Vec<InferenceCall>`** —
  All calls whose `session_id == Some(session_id)`. Returns an empty vec for an
  unknown session (not an error).
- **`get_spend_since(&self, since) -> f64`** — Total `cost_usd` summed over all
  calls with `recorded_at >= since`. Used for time-window budget checks.
- **`get_spend_by_model_since(&self, since) -> Vec<(String, f64)>`** — Spend
  grouped by `model` for calls with `recorded_at >= since`, as `(model, cost)`
  pairs. The reference impl sorts alphabetically by model name; the trait itself
  does not mandate an order.

### Execution-trace methods

- **`insert_execution_trace(&self, trace) -> Uuid`** — Persist one `StoredTrace`
  and return its id.
- **`get_execution_trace(&self, id) -> Option<StoredTrace>`** — Fetch a trace by
  its own id; `None` if not found (a missing row is not an error).
- **`get_traces_by_call(&self, inference_call_id) -> Vec<StoredTrace>`** — All
  traces whose `inference_call_id == Some(inference_call_id)`.

## What the crate ships

The consumer implements `GatewayStore`. There is **no** production, DB-backed
implementation in the crate.

There is, however, one concrete implementation shipped: `InMemoryStore`, a
`pub struct` backed by two `Mutex<Vec<…>>` fields. Its source comment labels it
"In-memory implementation (for testing)". Note two things:

- It is **not** gated behind `#[cfg(test)]` — only the unit tests are — so it is
  part of the crate's public API and a consumer *can* depend on it, e.g. for
  tests or ephemeral runs. It provides no durability.
- The task premise that the crate "ships no concrete impl" is therefore slightly
  inaccurate: it ships `InMemoryStore`. What it does not ship is any *persistent*
  store; that remains the consumer's responsibility.

## Wiring note (surprise)

The store module is **decoupled from the engine**. `Gateway`
(`crates/gateway/src/engine.rs`) holds only `config`, `adapters`, and
`circuit_breaker` — it has no `GatewayStore` field and never calls the trait. A
grep for store usage across `crates/gateway/src` finds references only in
`store.rs` and its `pub mod store;` export in `lib.rs`.

In other words, the gateway does not record calls or traces itself. It produces
the `ExecutionTrace` (returned to the caller) and the accounting data; the
consumer is responsible for constructing `InferenceCall` / `StoredTrace` values
and calling `insert_*` on its own store. `GatewayStore` defines the shape of that
persistence layer but is not invoked from inside the crate.

## Scenarios

```gherkin
Feature: Persistence store
  Scenario: A GatewayStore records an inference call
    Given a gateway configured with a GatewayStore
    When a request completes
    Then insert_inference_call is invoked with the call record
  Scenario: A store error is best-effort
    Given the store's insert fails
    Then the request still succeeds and the error is logged, not propagated
```
