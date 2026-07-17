# Tracing and Attempts

Every call the engine makes against a provider endpoint is recorded as a
structured `Attempt`. On success the full ordered list of attempts is attached
to the response so callers can see the entire fallback walk that led to the
answer. This doc describes the `Attempt` record, how the engine builds one per
candidate, what survives to the caller on success versus total failure, and the
(currently unwired) `StreamEvent` streaming trace surface.

Source: `crates/gateway/src/types/trace.rs`,
`crates/gateway/src/engine.rs`,
`crates/gateway/src/types/request.rs`,
`crates/gateway/src/types/cost.rs`.

## `AttemptStatus`

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus { Success, Failed }
```

A two-state marker for a single attempt. Serializes as `"success"` / `"failed"`.
It derives `PartialEq`/`Eq` (used by the engine tests to assert per-attempt
status); the sibling `TraceStatus` enum in the same file is the equivalent for a
whole `ExecutionTrace`.

## `Attempt`

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attempt { … }
```

One `Attempt` records a single try against one selected candidate endpoint.

| Field | Type | What it records |
| --- | --- | --- |
| `sequence` | `u8` | 1-based position in the candidate walk. The engine iterates with `(1_u8..).zip(result.all_candidates.iter())`, so the first candidate is `1`, the second `2`, and so on. |
| `adapter` | `String` | The router id the attempt was routed to (`candidate.router`) — the registered adapter that handled (or would have handled) the call. |
| `model` | `String` | The gateway's registry model id (`candidate.model`), e.g. the internal key, not the provider's wire name. |
| `api_model_id` | `String` | The resolved provider-facing model id (`candidate.api_model_id`) actually sent to the provider. May differ from `model` (see `fallback-chains` / `routing-and-selection`). |
| `status` | `AttemptStatus` | `Success` or `Failed`. |
| `duration_ms` | `u64` | Wall-clock elapsed for this attempt, measured from an `Instant::now()` taken just before adapter dispatch to the point the result is observed (`start.elapsed().as_millis() as u64`). |
| `tokens` | `Option<TokenUsage>` | Token counts (`input_tokens` / `output_tokens` / `total_tokens`) from `response.usage` — set only on success; `None` on any failure. Omitted from JSON when `None`. |
| `cost` | `Option<f64>` | Actual total cost for the attempt, taken from `response.actual_cost.total_cost` — set only on success; `None` on failure or when the adapter reported no cost. Omitted from JSON when `None`. |
| `error` | `Option<String>` | Failure detail: `err.to_string()` for a provider/transport error, or a `"no adapter registered for router '<id>'"` message when no adapter was found. `None` on success. Omitted from JSON when `None`. |
| `fallback_triggered` | `bool` | `true` when this attempt's failure was classified as a fallback trigger (see below), i.e. the engine moved on to the next candidate because of it. Always `false` for a successful attempt and for the "no adapter registered" case. |

`tokens`, `cost`, and `error` carry `#[serde(skip_serializing_if = "Option::is_none")]`;
the remaining fields are always serialized. `Attempt` derives `Clone` but not
`PartialEq`.

## How the engine builds the attempt list

All construction happens in `Gateway::execute` (`engine.rs`). Selection first
produces `result.all_candidates` (ordered) plus, if a chain was chosen, its
`fallback_triggers`:

```rust
let fallback_triggers = result.chain.as_ref()
    .map(|c| c.fallback_triggers.as_slice())
    .unwrap_or(&[]);          // empty when no chain (direct model/router pin)

let mut attempts: Vec<Attempt> = Vec::new();
for (sequence, candidate) in (1_u8..).zip(result.all_candidates.iter()) {
    let start = Instant::now();
    …
}
```

Inside the loop there are exactly three places an `Attempt` is pushed:

1. **No adapter registered.** `self.adapters.get(&candidate.router)` returns
   `None`. A `Failed` attempt is pushed with `tokens: None`, `cost: None`,
   `error: Some("no adapter registered for router '<router>'")`,
   `fallback_triggered: false`, then the loop **`continue`s** to the next
   candidate unconditionally (a missing adapter never stops the walk).

2. **Adapter succeeded.** The adapter's `Ok(response)` branch records the
   circuit-breaker success for `"<router>:<model>"`, then pushes a `Success`
   attempt carrying `duration_ms`, `tokens: response.usage.clone()`, and
   `cost: response.actual_cost.map(|c| c.total_cost)`, with `error: None` and
   `fallback_triggered: false`. It then **attaches the whole list and returns**:

   ```rust
   response.attempts = attempts;              // all prior failures + this success
   response.model = Some(candidate.model.clone());
   return Ok(response);
   ```

3. **Adapter failed.** The `Err(err)` branch records a circuit-breaker failure,
   computes `should_fallback = err.should_trigger_fallback(fallback_triggers)`,
   and pushes a `Failed` attempt with `error: Some(err.to_string())` and
   `fallback_triggered: should_fallback`. Then:
   - if `should_fallback` → `continue` (try the next candidate);
   - else → `break` (stop the walk — e.g. an auth error, or any failure when the
     candidate came from a direct pin with no chain, since `fallback_triggers`
     is then empty and nothing qualifies).

Because the loop threads a single `attempts` vec, a successful response's
`InferenceResponse.attempts` contains the **full history**: every skipped/failed
candidate that came before, followed by the winning `Success` attempt as the
last element. (`response.attempts` is a required, non-`Option` `Vec<Attempt>`
field on `InferenceResponse`.)

## What the caller sees: success vs. total failure

**On success** the caller gets an `InferenceResponse` whose `attempts` field is
the complete structured trace — sequence numbers, per-attempt adapter/model,
durations, tokens, cost, errors of failed predecessors, and fallback flags — all
preserved as typed `Attempt` values. `response.model` is set to the winning
candidate's registry `model`.

**On total failure** (the loop ends without any success) the structured trace is
**discarded**. The engine flattens the collected attempts into a single string
and returns only that plus a count:

```rust
let errors = attempts.iter()
    .filter_map(|a| a.error.as_ref()
        .map(|e| format!("[{}:{}] {}", a.adapter, a.model, e)))
    .collect::<Vec<_>>()
    .join("; ");
Err(GatewayError::AllAttemptsFailed { attempts: attempts.len(), errors })
```

So `AllAttemptsFailed` exposes only:
- `attempts: usize` — the number of attempts made, and
- `errors: String` — a `"; "`-joined list of `"[<adapter>:<model>] <error>"`
  fragments (built only from attempts that carried an `error`).

Everything else — per-attempt `duration_ms`, `sequence`, `status`,
`fallback_triggered`, `api_model_id`, and the `Attempt` structure itself — is
**lost** on the failure path. **Surprise / asymmetry:** callers that want a
structured post-mortem can only get one when the request ultimately *succeeds*;
a fully-failed request yields a lossy flattened string. Note this is distinct
from the richer `ExecutionTrace` type in `trace.rs` (which also holds a
`Vec<Attempt>` plus candidates/skipped/costs) — `Gateway::execute` does not
build an `ExecutionTrace`; it only fills `InferenceResponse.attempts`.

## `StreamEvent` — the streaming trace surface

`types/request.rs` defines the streaming-side event enum:

```rust
#[derive(Debug, Clone)]
pub enum StreamEvent {
    Chunk        { content: String },
    ProviderSwitch { from_adapter: String, from_model: String,
                     to_adapter: String, to_model: String, reason: String },
    Done         { model: String, tokens: TokenUsage, cost: f64 },
    Error        { code: String, message: String },
}
```

| Variant | Fields | Intended meaning |
| --- | --- | --- |
| `Chunk` | `content` | An incremental slice of generated text. |
| `ProviderSwitch` | `from_adapter`, `from_model`, `to_adapter`, `to_model`, `reason` | A mid-stream fallback: the stream moved from one endpoint to another; the streaming analogue of a `fallback_triggered` `Attempt`. |
| `Done` | `model`, `tokens`, `cost` | Terminal success event carrying the final model, `TokenUsage`, and total `cost` — the streaming counterpart to a `Success` attempt's `tokens` + `cost`. |
| `Error` | `code`, `message` | Terminal error event. |

Notes and caveats:

- Unlike `Attempt`, `StreamEvent` derives only `Debug, Clone` — **no
  `Serialize`/`Deserialize`**. It is not a wire type as written.
- **It is currently unwired.** Across the crate `StreamEvent` is referenced only
  by its own unit test (`stream_event_variants`); it has no producer and no
  consumer, and it is not re-exported from `lib.rs`. `Gateway` has no streaming
  `execute`; the only streaming entry point is the adapter trait's `stream()`,
  which yields `StreamChunk` (not `StreamEvent`).
- `StreamChunk` (the type adapters actually stream) is the per-token unit:
  `content: String`, `finish_reason: Option<String>`, `usage: Option<TokenUsage>`,
  and `tool_calls: Vec<ToolCall>` (assembled tool calls arrive on the terminal
  chunk). It, too, is a plain `Debug, Clone` struct with no serde. Treat
  `StreamEvent` as a forward-looking design for a gateway-level streaming trace
  that the engine does not yet emit.
