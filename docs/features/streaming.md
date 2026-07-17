# Streaming

How the `gateway` crate turns a provider's chunked HTTP response into a
uniform stream of `StreamChunk`s, and how fragmented tool-call arguments
are reassembled along the way.

Source of truth:

- `crates/gateway/src/types/request.rs` — `StreamChunk`, `StreamingToolCall`, `StreamEvent`
- `crates/gateway/src/adapters/mod.rs` — the `InferenceAdapter::stream` trait method
- `crates/gateway/src/adapters/anthropic.rs` — `AnthropicStreamState`, `process_stream_bytes`, `process_sse_line`
- `crates/gateway/src/adapters/openai.rs` — `OpenAiStreamState`, its `process_sse_line`
- `crates/gateway/src/adapters/ollama.rs` — the simpler `bytes_stream` variant

---

## The `stream()` adapter method

`stream` is a **required** method on the `InferenceAdapter` trait
(`adapters/mod.rs`) — there is no default implementation, so every adapter
must provide one:

```rust
async fn stream(
    &self,
    config: &RouterConfig,
    request: &InferenceRequest,
) -> Result<Pin<Box<dyn Stream<Item = Result<StreamChunk, GatewayError>> + Send>>, GatewayError>;
```

There are two layers of error handling:

- The **outer** `Result` covers setup failures that happen before any
  bytes flow: a non-`Chat` payload (`"streaming is only supported for chat
  payloads"`), a missing API key (`GatewayError::Authentication`), or a
  non-2xx HTTP status (mapped to `Authentication` for 401/403, `RateLimit`
  for 429, otherwise `ProviderError` carrying the status).
- The **inner** per-item `Result` covers mid-stream failures — e.g. a
  `reqwest` byte-stream error is pushed into the stream as
  `Err(GatewayError::ProviderError { … })` and then the stream ends.

The stream item is `Result<StreamChunk, GatewayError>` and the box is
`+ Send` so it can cross task boundaries.

---

## `StreamChunk`

Defined in `request.rs`. It derives only `Debug, Clone` — it is **not** a
serde type; it is an internal pipeline value, not a wire shape.

| Field | Type | Meaning |
|-------|------|---------|
| `content` | `String` | Incremental text for this chunk. Empty on pure framing / terminal chunks. |
| `finish_reason` | `Option<String>` | Set only on the terminal chunk (`"stop"`, `"tool_use"`, `"end_turn"`, `"tool_calls"`, …). |
| `usage` | `Option<TokenUsage>` | Token counts, when the provider reports them (usually on the terminal event). |
| `tool_calls` | `Vec<ToolCall>` | Finalised tool calls. **Empty on every chunk** until the stream resolves them; the assembled calls appear on the terminal chunk that carries `finish_reason`. |

The `tool_calls` doc comment on the struct spells out the contract:
adapters accumulate fragmented argument JSON internally (via
`StreamingToolCall`) and emit the assembled calls in the terminal chunk.

> `StreamEvent` (also in `request.rs`) is a **separate** enum
> (`Chunk` / `ProviderSwitch` / `Done` / `Error`) used at a higher engine
> level. The adapter `stream()` methods documented here produce
> `StreamChunk`, not `StreamEvent`.

---

## `StreamingToolCall` — the per-call accumulator

Also in `request.rs`. Derives `Debug, Default, Clone`.

```rust
pub struct StreamingToolCall {
    pub id: Option<String>,
    pub name: Option<String>,
    pub arguments_buffer: String,
}
```

Every provider that streams tool calls shares the same problem: the call's
`id` + `name` arrive once (on the opening event), and the argument JSON
arrives as a sequence of string fragments that must be concatenated. This
accumulator captures that intermediate state.

**API:**

- `new(id, name)` — build a fresh accumulator with `id` + `name` already
  `Some` and an empty buffer. Used when both fields arrive together on the
  opening event (Anthropic's `content_block_start`).
- `push_arguments(&mut self, fragment: &str)` — `push_str` the fragment
  onto `arguments_buffer`.
- `finalize(self) -> Option<ToolCall>` — materialise a complete `ToolCall`:
  - Returns `None` if **either** `id` or `name` is still missing (via the
    `?` operator on `self.id?` / `self.name?`) — safer than emitting a
    half-formed call.
  - If `arguments_buffer` is empty, `arguments` becomes `"{}"` (an empty
    buffer would be invalid JSON; `{}` is always a usable JSON object).
  - Otherwise `arguments` is the accumulated buffer verbatim.

Two accumulation styles, both keyed by the call's `u32` index in a
`BTreeMap<u32, StreamingToolCall>` (the `BTreeMap` keeps parallel calls in
index order when finalised):

- **OpenAI** uses `.entry(index).or_default()` — relying on `Default` — and
  then fills `id` / `name` / arguments as deltas arrive, because OpenAI
  splits the opening info and fragments across deltas keyed by `index`.
- **Anthropic** uses `StreamingToolCall::new(id, name)` at
  `content_block_start`, then `push_arguments` on each `input_json_delta`.

Both drain the map with `.into_values().filter_map(StreamingToolCall::finalize)`
on the terminal event.

---

## SSE line parsing

### Robust variant (OpenAI, Anthropic)

Both adapters carry a persistent state struct across HTTP byte chunks —
`OpenAiStreamState` / `AnthropicStreamState`. The relevant fields:

- `byte_stream` — the pinned `response.bytes_stream()`.
- `line_buf: String` — holds a **partial trailing line** so an SSE line
  split across two HTTP byte chunks reassembles correctly.
- `tool_calls: BTreeMap<u32, StreamingToolCall>` — in-flight accumulators.
- `pending: VecDeque<Result<StreamChunk, GatewayError>>` — a queue, since a
  single byte chunk can yield zero or many gateway chunks.
- `eof: bool`.

The stream is driven by `futures::stream::unfold`: on each poll it first
drains `pending`, then (if not `eof`) pulls the next byte chunk and runs it
through `process_stream_bytes`.

`process_stream_bytes`:

```rust
state.line_buf.push_str(&String::from_utf8_lossy(bytes));
while let Some(newline_pos) = state.line_buf.find('\n') {
    let mut line = state.line_buf.drain(..=newline_pos).collect::<String>();
    line.truncate(line.trim_end().len());
    process_sse_line(state, line.trim());
}
```

It appends the (lossily-decoded) bytes, then repeatedly splits off complete
lines at each `\n`, leaving any incomplete tail in `line_buf` for the next
byte chunk.

`process_sse_line`:

- **OpenAI** drops empty lines and the exact `"data: [DONE]"` sentinel,
  strips the `"data: "` prefix (`strip_prefix("data: ").unwrap_or(line)`),
  and parses the remainder as `StreamChatResponse`. Parse errors are
  silently skipped (`Err(_) => return`). Tool-call deltas are absorbed into
  the per-index accumulator; when `finish_reason` is present it drains the
  accumulators onto that terminal chunk (OpenAI does **not** repeat
  tool_calls on close, so this is the one shot to surface them). A chunk
  with empty content + no finish + no usage + no tool_calls is pure framing
  and is not emitted.
- **Anthropic** ignores empty lines and `event:` lines (the event-type
  discriminator is duplicated inside the `data:` JSON), strips `"data: "`,
  and parses a `StreamEvent` (the adapter-local struct). It dispatches on
  `event_type`: `content_block_start` opens a `tool_use` accumulator;
  `content_block_delta` emits a text chunk for `text_delta` or appends to
  the accumulator for `input_json_delta`; `content_block_stop` is framing;
  `message_delta` emits the terminal chunk carrying `stop_reason` (default
  `"end_turn"`), usage, and drained tool calls; `message_stop` defensively
  drains any accumulators still pending.

### Simpler variant (Ollama — and grok, together, gemini)

`ollama.rs` does **not** use a persistent `line_buf`. It maps directly over
`response.bytes_stream()`, and for each byte chunk decodes the bytes and
iterates `text.lines()`, applying the same `data:`/`[DONE]` handling per
line, collecting into a `Vec<StreamChunk>` that is then flattened into the
output stream:

```rust
for line in text.lines() {
    let line = line.trim();
    if line.is_empty() || line == "data: [DONE]" { continue; }
    let json_str = line.strip_prefix("data: ").unwrap_or(line);
    if let Ok(parsed) = serde_json::from_str::<StreamChatResponse>(json_str)
        && let Some(choice) = parsed.choices.first() { … }
}
```

Because there is no cross-chunk buffer, an SSE line split across a byte-chunk
boundary can be mis-parsed and dropped. OpenAI and Anthropic explicitly test
the split-line case (`process_stream_bytes_reassembles_lines_split_across_…`);
the simpler variant has no such guarantee. `ollama.rs` also always sets
`tool_calls: Vec::new()` on every chunk.

---

## Which adapters really stream vs. return an error

**Real streaming** (drive `bytes_stream()`, parse into `StreamChunk`s):

- `openai.rs` — full SSE state machine with line reassembly **and**
  tool-call streaming.
- `anthropic.rs` — full SSE state machine with line reassembly and a
  tool-call accumulation path (see caveat below).
- `ollama.rs`, `grok.rs`, `together.rs`, `gemini.rs` — real streaming but
  the simpler per-chunk `.lines()` parse; text only, no tool-call streaming.
- `bedrock.rs` — real streaming via the AWS SDK `converse_stream()` (not
  SSE), mapped through `into_stream_chunks`.
- `noop.rs` — not an error: emits a single canned chunk with
  `finish_reason: Some("no_provider")`.

**Return an error** — all with `"streaming is not supported for …"`:

- Image generation: `flux.rs`, `stability.rs`, `recraft.rs`.
- Video generation: `kling.rs`, `luma.rs`, `runway.rs`.
- Image/video: `fal.rs`, `replicate.rs`.

(`async_job.rs` is not an adapter — it is the shared `poll_until_complete`
polling helper used by the async media adapters.)

All streaming adapters additionally reject non-`Chat` payloads up front with
`ProviderError { message: "streaming is only supported for chat payloads" }`.

---

## Discrepancies and surprises

- **Anthropic streaming silently drops tools.** `AnthropicAdapter::stream`
  hard-codes `tools: Vec::new()` in the request body, with a comment that
  streaming + tool calling is deferred and "v1 ships tools through
  execute() only". So although `AnthropicStreamState` + `process_sse_line`
  fully implement `input_json_delta` tool-call accumulation — and unit tests
  exercise it — **in production no tools are ever sent while streaming**, so
  no `tool_use` blocks come back and the accumulation path is effectively
  test-only for now. `execute()` (non-streaming) does send tools.

- **OpenAI is the only adapter that genuinely streams tool calls.**
  `OpenAIAdapter::stream` passes `tools: build_tools(tools)` and assembles
  the deltas onto the `finish_reason` chunk.

- **Ollama never models tools at all.** Its wire `ChatCompletionRequest`
  has no `tools` field; both `execute()` and `stream()` destructure
  `tools: _` and ignore it, and `StreamChunk.tool_calls` is always empty.

- **Two divergent SSE parsers.** Only `openai.rs` and `anthropic.rs` carry a
  `line_buf` for cross-byte-chunk line reassembly. `ollama.rs`, `grok.rs`,
  `together.rs`, and `gemini.rs` iterate `.lines()` per byte chunk and can
  lose a line split across a chunk boundary.

- **Terminal-chunk shape differs by provider.** OpenAI can carry `content`
  and `finish_reason` (plus finalised `tool_calls`) on the same chunk.
  Anthropic emits text deltas as separate chunks and a final
  empty-`content` `message_delta` chunk carrying `finish_reason` + usage +
  tool calls; its `message_stop` handler only emits an extra chunk if tool
  calls somehow remain undrained.

- **`StreamChunk` / `StreamingToolCall` are not serde types** (only
  `Debug` / `Clone` / `Default`). They live entirely inside the streaming
  pipeline and are never serialised to the wire.
