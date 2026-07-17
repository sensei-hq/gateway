# Feature: Tool / Function Calling

- **Status:** Reference (reflects code as of 2026-07-17)
- **Crate:** `gateway`
- **Primary sources:** `src/types/request.rs`, `src/adapters/openai.rs`, `src/adapters/anthropic.rs`

---

## 1. Overview

Tool calling lets a caller advertise a set of callable functions to a model,
receive back the model's decision to invoke one or more of them, run those tools
itself, and feed the results back for a follow-up turn. The gateway models this
with **provider-neutral types** in `src/types/request.rs` and lets each adapter
translate them into the provider's native wire shape.

Two gateway types carry the payload in each direction:

- **`ToolDefinition`** — what the caller advertises (a function the model *may*
  call).
- **`ToolCall`** — what the model emits (a function the model *decided* to call).

The gateway never runs tools itself. The multi-turn loop is entirely
caller-driven: the caller dispatches each `ToolCall`, then sends a follow-up
request whose `messages` include the tool results.

---

## 2. `ToolDefinition`

```rust
pub struct ToolDefinition {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub input_schema: serde_json::Value,
}
```

- `input_schema` is a **JSON Schema document** describing the call's argument
  object (typically `{type: "object", properties: {...}, required: [...]}`). It
  is a raw `serde_json::Value` and is **passed through verbatim** to every
  provider — the source comment notes OpenAI / Anthropic / Gemini / Bedrock all
  accept JSON Schema, with only minor wrapping differences handled per-adapter.
- `description` is optional and omitted from the wire entirely when `None`
  (`skip_serializing_if`).
- Round-trip is verified by `tool_definition_round_trips_with_json_schema_pass_through`.

## 3. `ToolCall`

```rust
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}
```

- `arguments` is the argument object encoded as a **JSON string**, not a
  structured object. The source comment states this matches OpenAI's wire shape
  directly; adapters whose providers emit a native JSON object (Anthropic,
  Gemini, Bedrock) serialize it into a string on the way out.
- `id` correlates a call with its later result (`tool_call_id`).

`ToolCall` appears in two places:

- On an assistant `Message` — `Message.tool_calls: Vec<ToolCall>` (the calls the
  assistant emitted, echoed back when continuing the conversation). Empty for
  any non-assistant role; `skip_serializing_if = "Vec::is_empty"`.
- On the response — `InferenceResponse.tool_calls: Vec<ToolCall>` (§5).

---

## 4. Request flow — advertising tools

Tools ride on the `Payload::Chat` variant:

```rust
Payload::Chat {
    messages: Vec<Message>,
    system: Option<String>,
    max_tokens: Option<u32>,
    temperature: Option<f32>,
    tools: Vec<ToolDefinition>,   // #[serde(default, skip_serializing_if = "Vec::is_empty")]
}
```

Per the field comment: an **empty `tools` vec disables tool calling** for that
turn even if the provider would otherwise advertise tools, and the field is
omitted from the serialized request when empty (`chat_payload_tools_field_omitted_from_json_when_empty`).

Both adapters destructure `tools` out of `Payload::Chat` in `execute` and hand
it to their own `build_tools` helper (§7, §8).

---

## 5. Response flow — receiving tool calls

The model's decision comes back on:

```rust
pub struct InferenceResponse {
    // ...
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    // ...
}
```

Empty when the model returned plain text. Per the field comment, the **caller is
expected to dispatch each call and feed the results back** via a follow-up
request containing one `Message::tool_result` per call.

---

## 6. The multi-turn loop

A tool-calling round trip, all driven by the caller:

1. Caller sends `Payload::Chat` with a non-empty `tools`.
2. Response comes back with `InferenceResponse.tool_calls` populated (and often
   `content == None`).
3. Caller appends to the conversation:
   - An **assistant** `Message` carrying those `tool_calls` (so the provider
     sees its own prior call when the history is replayed), and
   - One **tool-result** `Message` per call.
4. Caller re-sends the whole `messages` history. Repeat until `tool_calls` is
   empty.

Tool results are built with the `Message::tool_result` constructor:

```rust
Message::tool_result(tool_call_id, content)
// role   = MessageRole::Tool
// content = MessageContent::ToolResult { tool_call_id, content }
```

`MessageContent` is a tagged enum (`#[serde(tag = "type", rename_all = "snake_case")]`)
with two variants — `Text { text }` and `ToolResult { tool_call_id, content }`.
The linking `tool_call_id` must match the `id` of the originating `ToolCall`.
`MessageContent::as_text()` returns the `content` body for a `ToolResult`, so
adapters that don't model tools natively can still read it as plain text.

Note the asymmetry the source calls out: the assistant's *emitted* calls live on
the separate `Message.tool_calls` field, while the *result* of a call is carried
inside `MessageContent::ToolResult`. A single `Message` struct thus models both
"assistant said X" and "assistant called tool T".

---

## 7. OpenAI adapter (`src/adapters/openai.rs`)

### Advertising tools — the `{type:"function", function:{…}}` envelope

OpenAI wraps every tool in a function envelope. Two wire structs model this:

```rust
struct ChatTool {
    #[serde(rename = "type")]
    tool_type: &'static str,   // always "function" for now
    function: ChatToolFunction,
}
struct ChatToolFunction {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    parameters: serde_json::Value,
}
```

`build_tools` maps each `ToolDefinition` into this shape:

```rust
ChatTool {
    tool_type: "function",
    function: ChatToolFunction {
        name: t.name.clone(),
        description: t.description.clone(),
        parameters: t.input_schema.clone(),   // input_schema → parameters
    },
}
```

**Key rename:** the gateway's `input_schema` becomes OpenAI's `parameters`. The
schema value itself is copied unchanged. Verified by
`build_tools_wraps_each_definition_in_function_envelope`.

`ChatCompletionRequest.tools: Vec<ChatTool>` uses `skip_serializing_if =
"Vec::is_empty"`, so no `tools` key is emitted when none are configured.

### Emitting the assistant's prior calls back

`OpenAiToolCall` / `OpenAiToolCallFunction` mirror the gateway `ToolCall`:

```rust
struct OpenAiToolCall { id: String, #[serde(rename="type")] tool_type: String, function: OpenAiToolCallFunction }
struct OpenAiToolCallFunction { name: String, arguments: String }   // arguments stays a JSON string
```

`to_openai_tool_call` re-wraps a gateway `ToolCall` into that envelope
(`tool_type = "function"`, `arguments` passed through verbatim). In
`build_chat_messages`, an assistant message with non-empty `tool_calls`
serializes them onto `ChatMessage.tool_calls`; when the text body is empty the
`content` field is emitted as `null`/omitted (OpenAI accepts a null content on a
tool-call turn). Verified by `build_chat_messages_echoes_assistant_tool_calls_back_to_the_wire`.

### Tool results

`MessageContent::ToolResult` maps to a `ChatMessage` with `role = "tool"`,
`content = <result body>`, and `tool_call_id = Some(<id>)`. OpenAI uses
`tool_call_id` (matching the gateway field name). Verified by
`build_chat_messages_maps_tool_result_to_role_tool_with_tool_call_id`.

### Parsing calls back out

`ChatResponseMessage.tool_calls: Option<Vec<OpenAiToolCall>>` is parsed and each
entry runs through `from_openai_tool_call`, which strips the function envelope
back to a gateway `ToolCall` (`id`, `function.name` → `name`,
`function.arguments` → `arguments`, still a string).

---

## 8. Anthropic adapter (`src/adapters/anthropic.rs`)

### Advertising tools — top-level `input_schema`, no envelope

Anthropic uses the **same JSON Schema shape at the top level** — no
`{type:"function"}` wrapper and the field keeps the name `input_schema`:

```rust
struct AnthropicTool {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    input_schema: serde_json::Value,   // same field name as the gateway type
}
```

`build_tools` is therefore a near-identity map (`name`, `description`,
`input_schema.clone()`). Verified by
`build_tools_passes_json_schema_through_unchanged`, which asserts there is **no**
`function` wrapper key. `AnthropicRequest.tools` also uses `skip_serializing_if =
"Vec::is_empty"`.

### Content blocks — `tool_use` / `tool_result`

Anthropic always emits the array-of-blocks form for message content. Outbound
blocks are a tagged enum (`#[serde(tag = "type", rename_all = "snake_case")]`):

```rust
enum OutContentBlock {
    Text       { text: String },
    ToolUse    { id: String, name: String, input: serde_json::Value },
    ToolResult { tool_use_id: String, content: String },
    Image      { source: AnthropicImageSource },
}
```

`build_content_blocks` translates a gateway `Message`:

- Assistant `tool_calls` → one `ToolUse` block each. Because Anthropic wants
  `input` as a **JSON object** (not a string), `parse_tool_input` parses the
  gateway's `ToolCall.arguments` string back into a `serde_json::Value` — empty
  string → `{}`, malformed → a JSON string node (provider rejects, but no panic).
  Verified by `build_messages_emits_tool_use_block_for_assistant_tool_calls`.
- `MessageContent::ToolResult` → a single `ToolResult` block. **Naming
  difference:** the gateway's `tool_call_id` maps onto Anthropic's `tool_use_id`
  (singular), unlike OpenAI's `tool_call_id`. Verified by
  `build_messages_emits_tool_result_block_for_tool_role`.

Role mapping (`build_messages`): `System` messages are filtered out (hoisted to
the top-level `system` field by `extract_system`); `Assistant` → `"assistant"`;
both `User` and `Tool` → `"user"`, because tool-result blocks live in a user turn.

### Parsing calls back out

Inbound blocks are parsed loosely into a single `ContentBlock` struct (optional
fields) so unknown block types don't fail the whole response. `extract_tool_calls`
filters `block_type == "tool_use"` and builds a gateway `ToolCall`, re-serializing
the `input` object into the **JSON-string `arguments`** form for round-trip parity
with OpenAI (`serde_json::to_string(input)`). Blocks missing `id` or `name` are
dropped. `extract_text` separately joins all `text` blocks into
`InferenceResponse.content`.

---

## 9. Streaming tool-call assembly

### The shared accumulator — `StreamingToolCall`

Streaming providers split a tool call across many events: `id` + `name` arrive
once on an opening event, then argument JSON arrives as a sequence of string
fragments. `StreamingToolCall` (in `src/types/request.rs`) captures that
intermediate state:

```rust
pub struct StreamingToolCall {
    pub id: Option<String>,
    pub name: Option<String>,
    pub arguments_buffer: String,
}
```

- `new(id, name)` seeds a fresh accumulator.
- `push_arguments(fragment)` appends a partial JSON fragment.
- `finalize() -> Option<ToolCall>` materializes a `ToolCall` only once **both**
  `id` and `name` are present (else `None` — no half-formed calls). An empty
  `arguments_buffer` finalizes to the string `"{}"` so the result is always valid
  JSON.

Both adapters key accumulators by index in a `BTreeMap<u32, StreamingToolCall>`
(so parallel calls stay separate and emit in index order) and surface finalized
calls on the **terminal `StreamChunk`** — the one that also carries
`finish_reason`. `StreamChunk.tool_calls` is empty on every earlier chunk.

### OpenAI streaming — sends tools, assembles by index

OpenAI's `stream()` **does** advertise tools: the request body sets
`tools: build_tools(tools)` and `stream: true`. Assembly (`process_sse_line`):

- `StreamDelta.tool_calls: Vec<StreamToolCallDelta>` — each keyed by `index`.
- The first delta for an index carries `id` + `function.name`; later deltas carry
  only `function.arguments` fragments, appended via `push_arguments`.
- When `finish_reason` arrives, all accumulators are drained through
  `finalize` onto that chunk (OpenAI doesn't repeat `tool_calls` on close).

Verified by `process_sse_line_accumulates_tool_call_fragments_across_deltas` and
`process_sse_line_handles_multiple_parallel_tool_calls_by_index`.

### Anthropic streaming — parser is complete, but tools are DEFERRED

Anthropic's SSE parser has full tool-call plumbing:

- `content_block_start` with a `tool_use` block → `StreamingToolCall::new(id, name)`
  inserted at that `index`.
- `content_block_delta` with `delta.type == "input_json_delta"` →
  `partial_json` fragment appended via `push_arguments` (this is Anthropic's
  argument-accumulation mechanism, distinct from OpenAI's `function.arguments`).
- `content_block_delta` with `text_delta` → user-facing text chunk.
- `message_delta` → carries the terminal `stop_reason` (default `"end_turn"`) and
  drains the accumulators into the chunk's `tool_calls`. `message_stop` is a
  defensive final drain.

This is exercised by
`process_sse_line_accumulates_tool_use_blocks_across_input_json_deltas`.

**Surprise / discrepancy to flag:** despite that complete parser, Anthropic's
`stream()` **hard-codes `tools: Vec::new()`** on the outbound request:

```rust
// Streaming + tool calling is deferred — Anthropic emits
// `input_json_delta` events for tool arguments that need
// accumulation in the stream layer. v1 ships tools through
// execute() only.
tools: Vec::new(),
```

So in practice a **streamed Anthropic request never advertises tools**, and the
model will never emit `tool_use` in that path — the accumulation code is present
and tested but dormant until tools are wired into `stream()`. OpenAI, by
contrast, streams tools live. If you need Anthropic tool calls today, use the
non-streaming `execute()` path.

---

## 10. Per-provider translation at a glance

| Concern | Gateway type | OpenAI wire | Anthropic wire |
|---|---|---|---|
| Tool schema field | `input_schema` | `function.parameters` | `input_schema` (top level) |
| Tool envelope | — | `{type:"function", function:{…}}` | none (flat object) |
| Emitted call | `ToolCall{id,name,arguments:String}` | `tool_calls[].function{name,arguments:String}` | `tool_use` block `{id,name,input:object}` |
| Call arguments encoding | JSON **string** | JSON **string** (verbatim) | JSON **object** (parsed / re-serialized) |
| Result linking id | `tool_call_id` | `tool_call_id` | `tool_use_id` |
| Result carrier | `role:Tool` + `MessageContent::ToolResult` | `role:"tool"` message | `tool_result` block in a `user` turn |
| Streaming arg deltas | `StreamingToolCall.arguments_buffer` | `function.arguments` fragments (by `index`) | `input_json_delta.partial_json` (by `index`) |
| Tools sent when streaming? | n/a | **Yes** | **No — deferred (`Vec::new()`)** |
