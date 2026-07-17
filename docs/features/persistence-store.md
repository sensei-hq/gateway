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
| `duration_ms` | `u64` | Wall-clock duration. |
| `status` | `CallStatus` | Success or failure. |
| `error_type` | `Option<String>` | Error classification when failed. |
| `fallback_sequence` | `u8` | Position in the fallback walk (0 = first candidate). |
| `recorded_at` | `DateTime<Utc>` | When the call was recorded (the field spend queries filter on). |

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
