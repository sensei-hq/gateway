# Recipes

Short, task-oriented snippets. All assume a configured `gateway` from
[quickstart](quickstart.md).

## Choose how a request routes

Three modes on `InferenceRequest`, in precedence order:

```rust
// 1. Pin an exact model (+ optionally its router).
InferenceRequest { model: Some("gpt-4o".into()), router: Some("openai".into()), chain: None, .. }

// 2. Use a named fallback chain (tries candidates in priority order).
InferenceRequest { model: None, router: None, chain: Some("chat".into()), .. }

// 3. Constrain to a router and let selection pick a model on it.
InferenceRequest { model: None, router: Some("openai".into()), chain: None, .. }
```

A caller-pinned `model` always wins. With a `chain`, failures walk to the next
candidate when the error is in the chain's `fallback_triggers`. The `attempts`
field on the response is the full trail (what was tried, what failed, why).

## Shape routing per request

`InferenceRequest.routing` overrides the operator's authored order for one call.
Absent ⇒ unchanged behaviour (the field is skipped on the wire entirely).

```rust
use gateway::types::request::{RoutingPreferences, SortKey, CandidateSet, CandidateRef};

InferenceRequest {
    chain: Some("chat".into()),
    routing: Some(RoutingPreferences {
        sort:   Some(SortKey::Price),                       // price | latency | throughput
        ignore: Some(CandidateSet { routers: vec!["ollama".into()], models: vec![] }),
        only:   None,
        order:  Some(vec![CandidateRef { model: Some("claude-haiku".into()), router: None }]),
    }),
    ..
}
```

Filtering runs first, then ordering: `only`/`ignore` → `sort` (or the default) →
`order`.

- **`only` is AND across non-empty axes; `ignore` is OR.** An empty list is
  "don't care", not "match nothing" — `only: {routers: ["anthropic"]}` admits
  every anthropic candidate. The two have no precedence over each other
  (admission is `only_ok && !ignore_match`), but satisfying `only` does **not**
  exempt a candidate from `ignore`. Excluding *everything* is a terminal
  `NoCandidates`, not a retryable pause.
- **`sort: price`** orders by estimated cost across the whole chain and
  deliberately overrides `priority`.
- **`sort: latency|throughput`** reorders only the candidates it has actually
  measured, in the slots they already occupy — unmeasured candidates never move,
  so a cold process is exactly priority order.
- **`order` is first-matching-ref-wins.** `[{model: "charlie"}, {router: "north"}]`
  means "charlie, then the rest of north". Candidates matching no ref follow as
  fallbacks; `order` never drops anything. Two inert forms to know: `order: []`
  and `order: [CandidateRef::default()]` are both no-ops, and an all-wildcard ref
  placed first shadows every ref after it. A **router-only** ref lifts every
  model on that router above every other priority tier — correct, but it is the
  cross-tier reordering the default strategy will not do on its own.
- **They filter *within* the chain that was already resolved.** A capability
  request (no `chain`) picks one chain by **lowest chain id** before any
  filtering runs, so `only` cannot make it go looking in a different chain — it
  narrows the chosen one, and over-narrowing yields `NoCandidates`. Pin the
  chain by name to combine "this chain" with "these providers".
- **A direct `router` + `model` request is filtered too.** The gate vector runs
  on every resolution path, so an `ignore` naming that pair excludes the one
  candidate and the call fails with `NoCandidates`.

`response.routing` explains what happened: the strategy that ran, the resulting
order, and (for the weighted default) each candidate's cost, reliability and draw
weight. Two readings to get right:

- It is `None` for a direct router+model request, which orders nothing. That
  is **not** "preferences were ignored" — they still filtered it (above).
- `reliability` and `weight` are `null` for every candidate under `sort: price`,
  `sort: latency` and `sort: throughput`, because only the weighted default
  consults them. Read `strategy` before concluding anything about fleet health.

> `execute_stream` applies the same preferences but returns `StreamEvent`s rather
> than an `InferenceResponse`, so a streamed call has **no** routing explanation.

Full reference: `docs/features/routing/provider-preferences.md`.

## Stream a chat response

```rust
use futures::StreamExt;
use gateway::types::request::StreamEvent;

let mut stream = gateway.execute_stream(&req).await?;   // chat capabilities only
while let Some(ev) = stream.next().await {
    match ev {
        StreamEvent::Chunk { content }        => print!("{content}"),
        StreamEvent::ProviderSwitch { reason, to_model, .. } =>
            eprintln!("[fell back to {to_model}: {reason}]"),
        StreamEvent::Done { model, tokens, cost } =>
            eprintln!("\n[{model}: {} tok, ${cost:.4}]", tokens.total_tokens),
        StreamEvent::Error { code, message }  => eprintln!("[error {code}: {message}]"),
    }
}
```

Fallback is **pre-first-byte only**: once bytes flow, a mid-stream error is terminal
(`Error`), not a switch.

## Read cost + token usage

```rust
let resp = gateway.execute(&req).await?;
if let Some(cost) = resp.actual_cost {          // needs ModelPricing on the model
    println!("${:.6} ({} in / {} out)", cost.total_cost, cost.input_tokens, cost.output_tokens);
}
```

`estimated_cost` is the pre-flight figure (from input tokens + max output);
`actual_cost` is computed from the provider's real `usage`.

## Cap per-request spend (budget)

`budget` is a per-call USD ceiling used during **model selection** — candidates whose
estimated cost exceeds it are filtered out (a cheaper chain entry can still run).

```rust
InferenceRequest { budget: Some(0.05), .. }     // skip any candidate estimated over 5¢
```

This is distinct from windowed subscription quotas (below).

## Persist calls + query burn-rate

Attach a store; the engine then records every terminal call (best-effort — a store
error never fails inference).

```rust
use std::sync::Arc;
use chrono::{Utc, Duration};
use gateway::store::{GatewayStore, InMemoryStore};   // swap InMemoryStore for your DB impl

let store = Arc::new(InMemoryStore::default());
let gateway = Gateway::new(config, adapters, cb).with_store(store.clone());

// … after some calls …
let today = store.get_spend_since(Utc::now() - Duration::hours(24)).await?;      // f64 USD
let by_model = store.get_spend_by_model_since(Utc::now() - Duration::hours(24)).await?;
```

`InMemoryStore` is for tests/dev; implement `GatewayStore` over Postgres for
production (one method, `get_usage_since`, powers quotas — see the upgrade guide's SQL).

## Enforce subscription quotas

Two parts: **config** (the limits) + **request auth** (who to meter). See
`docs/features/subscription-quota.md` for the full model.

```rust
use gateway::types::config::{ConstraintsConfig, TierConstraints, QuotaLimit, MeterUnit, Window};
use gateway::types::request::AuthContext;
use std::collections::HashMap;

// Config: a "free" tier = 100 requests/day.
let constraints = ConstraintsConfig {
    tiers: HashMap::from([("free".into(), TierConstraints {
        quota: vec![QuotaLimit { unit: MeterUnit::Requests, window: Window::Day, limit: 100 }],
        per_capability: HashMap::new(),
    })]),
    default: None,
};
let config = GatewayBuilder::new().add_router(..).add_model(..).constraints(constraints).build()?;
let gateway = Gateway::new(config, adapters, cb).with_store(store); // a store is REQUIRED to enforce

// Request: tag it with the subject + tier (resolved by your auth layer / Kavach).
let req = InferenceRequest {
    auth: Some(AuthContext { subject_id: team_uuid, tier: Some("free".into()) }),
    .. // capability, payload, etc.
};
// Over quota ⇒ Err(GatewayError::QuotaExceeded { .. }) BEFORE any provider call.
```

No store, no `auth`, or no matching tier ⇒ no enforcement (unchanged behaviour).
Units: `Requests | InputTokens | OutputTokens | TotalTokens | CostUsdMillis`;
windows are rolling `Day | Week | Month`.

## Tool calling

Offer tools on a chat request; read `tool_calls` off the response; feed results back
as a follow-up turn.

```rust
use gateway::types::request::{ToolDefinition, Message};

// Offer a tool:
let payload = Payload::Chat {
    messages,
    system: None, max_tokens: Some(512), temperature: None,
    tools: vec![ToolDefinition {
        name: "get_weather".into(),
        description: Some("Current weather for a city".into()),
        input_schema: serde_json::json!({
            "type": "object",
            "properties": { "city": { "type": "string" } },
            "required": ["city"]
        }),
    }],
};

// Handle the model's calls:
let resp = gateway.execute(&req).await?;
for call in &resp.tool_calls {                 // ToolCall { id, name, arguments (JSON string) }
    let result = run_tool(&call.name, &call.arguments);
    // Append a tool-result message and call execute again with the extended history:
    // Message::tool_result(call.id.clone(), result)
}
```

Tool wire differences (OpenAI vs Anthropic vs Gemini) are handled inside the
adapters — you always read/write the same `ToolDefinition` / `ToolCall` shapes.
