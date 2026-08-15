---
title: SP-4 slice 2 — Secret redaction before journaling effect I/O
doctype: design
module: orchestrator
spec: SP-4
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§7.4 journal/CAS split, §13 enforcement & isolation, §289 secret redaction); ./2026-08-14-sp4-permission-enforcement-design.md (SP-4 slice 1 — the authorization gate; the effect-output/`EffectRecorded` path this scrubs); ./2026-08-10-sp1-slice4-observation-mutation-design.md (the effect-class dispatch + `record_tool_effect`/`agent_turn_output` output sites)
date: 2026-08-14
---

# SP-4 slice 2 — Secret redaction

## 1. Goal

Scrub secrets from effect **outputs** (tool results + model-turn text) **before they are
journaled or fed back to the agent**, so durable plaintext credentials never land in the
journal/CAS (compliance landmine, master spec §289) and the model never sees a secret in a
tool result (anti-exfiltration). Redaction is a **pure, injected `Redactor`** (default
`PatternRedactor` — curated secret-shape patterns → `[REDACTED]`), applied at the two leaf
output sites so live == journaled == replayed (determinism-safe). Opt-in
(`Executor::with_redactor`); default off ⇒ byte-identical.

This is the **redaction** layer of SP-4. It is best-effort by *shape*; precise
known-value/vault-backed redaction and reversible tokenization are future `Redactor` impls /
SP-DATA. Runtime *confinement* remains the sandbox (slice 4).

## 2. SP-4 slicing (context)

SP-4 = "Mutation & exactly-once + isolation" (master spec §16). 1. permission enforcement ✅
(slice 1). 2. **This slice** — secret redaction. 3. workspace isolation. 4. sandbox + cred
broker + resource-cap killing. 5. exactly-once hardening. This slice depends only on SP-1
(the effect-output journaling) + adds no infrastructure (a pure redactor).

## 3. Background & impact review

- **Where plaintext could land durably:** every effect **output** is journaled as
  `EffectRecorded.output` (an `EffectOutput`, CAS-split by `split_output` when large) and, for
  agent effects, fed back into the ReAct transcript. The two **leaf** producers of externally-
  sourced output are: **tool results** (`record_tool_effect`, `executor/agent.rs`) and
  **model-turn text** (`agent_turn_output` → the `{model, text, tool_calls}` Pure effect). All
  other journaled outputs are compositions of these — Map/Consolidate sink maps, `ContextWrite`
  blackboard publishes, CAS blobs — so redacting the two leaves covers the durable surface
  transitively.
- **What is NOT plaintext-durable:** effect **inputs** are hashed, not stored — `EffectIntent.
  args_hash` and the per-turn `input_hash` are one-way hashes; the system prompt/context is only
  hashed into `agent_input_hash`. So the exposure is in outputs, not inputs.
- **The determinism constraint (the crux):** `split_output` produces only the *journaled*
  representation; the value fed to the agent is the un-split result. If redaction scrubbed only
  the journaled copy, a resume would replay `[REDACTED]` from the memo while the original live
  run fed the agent the secret → the next turn's `input_hash` would differ → a spurious
  `DeterminismViolation`. Therefore redaction MUST scrub the output **at production, before both
  journaling and the agent-return**, and the redactor MUST be **pure** — so live == journaled ==
  replayed. (This also means the model never sees the secret: the correct secure posture; the
  credential-*use* path is the sandbox/broker, slice 4.)
- **No existing redaction** in the orchestrator. The `vault` crate (zeroize/KEK/crypto) is a
  separate encryption-at-rest subsystem the orchestrator does not consume.
- **Impact:** additive — a `Redactor` injected via `with_redactor` (default `None` ⇒ identity ⇒
  byte-identical, like `ContentStore`/reconcilers). The only behavior change is at the two leaf
  sites *when a redactor is wired*.

## 4. Design

### 4.1 The `Redactor` seam + `PatternRedactor`

`orchestrator-core` gains `redact.rs`:
```rust
pub trait Redactor: Send + Sync {
    /// Redact secrets from an effect output. MUST be pure (replay-stable) — no I/O,
    /// clock, or RNG — since the redacted value is BOTH journaled and fed to the
    /// agent, so a resume must reproduce it identically.
    fn redact(&self, value: &serde_json::Value) -> serde_json::Value;
}
```
`PatternRedactor` (default impl) walks a `serde_json::Value` recursively — objects, arrays,
and **string leaves** (object keys are NOT redacted; non-string scalars pass through) — and
replaces each substring matching its curated pattern set with the fixed placeholder
`[REDACTED]`. The default set (extensible; `PatternRedactor::new(patterns)` + a
`PatternRedactor::default()` with the built-ins):
- **Provider key prefixes:** `sk-ant-[A-Za-z0-9_-]{20,}` (Anthropic), `sk-[A-Za-z0-9]{20,}`
  (OpenAI), `AKIA[0-9A-Z]{16}` (AWS), `ghp_[A-Za-z0-9]{36}` (GitHub PAT), `xox[baprs]-[A-Za-z0-9-]{10,}`
  (Slack), `AIza[0-9A-Za-z_-]{35}` (Google).
- **Bearer tokens:** `(?i)bearer\s+[A-Za-z0-9._-]{8,}`.
- **PEM private keys:** `-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----`.
- **Assignment forms:** `(?i)(api[_-]?key|secret|token|password|passwd)("?\s*[=:]\s*"?)([^\s"',]{6,})`
  — redacts **only the value** (capture group 3 → `[REDACTED]`), preserving the key label
  (`api_key=[REDACTED]`).

**No generic entropy/length heuristic** in slice 2 (false-positive risk — would redact hashes,
digests, base64 payloads); deferred. **ReDoS-safe by construction:** Rust's `regex` crate uses
finite automata (no backtracking), so linear-time matching on adversarial tool output is
guaranteed — no catastrophic-backtracking risk from scanning untrusted content.

### 4.2 Placement — determinism-safe leaf sites (`executor/`)

The `Executor` gains `redactor: Option<Arc<dyn Redactor>>` + `with_redactor(Arc<dyn Redactor>)`
and a helper:
```rust
fn redact(&self, v: &serde_json::Value) -> serde_json::Value {
    match &self.redactor { Some(r) => r.redact(v), None => v.clone() }
}
```
Applied at the two leaf sites, **before** the value is used for either journaling or the
agent-return:
- **Tool result** (`record_tool_effect`): `let result = self.redact(&result);` — then
  `split_output(&result)` is journaled AND `Ok(ToolOutcome::Ok(result))` is returned. (So the
  CAS blob, produced by `split_output` *after* redaction, stores redacted bytes too.)
- **Model turn** (`agent_turn_output`): redact the **`text`** field of the `{model, text,
  tool_calls}` output before it is journaled and threaded into the next turn. **`tool_calls`
  is left intact** — a model can only emit a secret in a tool-call argument if it *saw* one,
  and we redact the sources (tool results + text), so the tool-invocation path stays whole and
  the journaled==replayed tool_calls keep resume determinism.

Derived outputs (Map/Consolidate sink maps, `ContextWrite`, CAS blobs) inherit redaction from
the leaves. A **memo hit replays the already-redacted journaled value** (no re-redaction). The
redactor is pure, so live == journaled == replayed within a run's lifetime; a redactor **swapped
across a resume** changes new outputs and trips the `input_hash` determinism fence **loud**
(never silent). Folding a redactor version into the fence is a stated hardening (§6).

### 4.3 Representation & scope

Irreversible `[REDACTED]` placeholder — pure, no key management (reversible tokenization /
crypto-shred needs the vault/KEK → SP-DATA). A uniform placeholder (not typed per pattern) so
the redaction itself discloses nothing about the secret. **Redacted:** tool results + model
text + everything derived. **Not redacted:** `input_hash`/`args_hash` (one-way hashes), tool-
call *arguments* (source-redacted), permission-denial values (no secrets), object *keys*.

### 4.4 Trust boundary (best-effort by shape)

Pattern redaction catches secrets whose *shape* matches a known pattern; it will miss a novel
credential format, a secret split across fields, or one the model paraphrases. It is a
defense-in-depth scrub of the durable/transcript surface, not a guarantee. Precise
known-value/vault-backed redaction is a future `Redactor` impl the seam already supports;
runtime confinement is the sandbox (slice 4). Stated so the boundary is not overclaimed.

## 5. Decisions

- **D1 — pattern-based `Redactor` seam, `PatternRedactor` default** [approved]: core-mechanism
  + opt-in-policy; catches unknown/tool-leaked secrets by shape; the trait admits a future
  known-value/vault-backed impl. Rejected: known-secret-set only (misses tool-leaked/unknown
  secrets); both-now (a bigger first increment).
- **D2 — opt-in injected (`with_redactor`), default off** [approved]: additive (byte-identical
  when unwired), matches every other Executor seam; production opts in with one line
  (documented). Rejected: secure-default-ON (non-additive; false-positive-redacts existing
  test/demo outputs; diverges from the seam convention).
- **D3 — redact at the two leaf sites, before journal AND agent-return; redactor pure**
  [approved]: the only determinism-safe placement (live == journaled == replayed). Redacting
  the journaled copy only would spuriously trip the fence on resume.
- **D4 — irreversible `[REDACTED]` placeholder** [approved]: pure, no key management; reversible
  tokenization/crypto-shred is SP-DATA.
- **D5 — curated prefix/keyword patterns, no entropy heuristic; ReDoS-safe `regex`** [approved]:
  false-positive control; the `regex` crate's automata guarantee linear-time on adversarial
  output.

## 6. Deferred (stated)

- **Known-value / vault-backed `Redactor`** (redact exact registered credential values from the
  vault/config — zero false-positive precision) — a future impl of the same seam; pairs with the
  slice-4 credential broker.
- **Reversible tokenization / crypto-shred** (recover a value from an authorized context) — needs
  the vault/KEK → SP-DATA (master spec §7.4 crypto-shred).
- **Entropy/length heuristic** for unknown-shape secrets (with a false-positive budget).
- **Redactor version in the determinism fence** (so a redactor swap across resume is a clean
  refusal rather than an `input_hash` mismatch) — a hardening; today it trips the fence loud.
- **Input-side redaction** (skills/context that embed secrets) — inputs are hashed-not-stored, so
  lower risk; a prompt-assembly redactor is future.
- **Runtime confinement** (a tool exfiltrating a secret out-of-band) — the sandbox, slice 4.

## 7. Acceptance criteria (TDD)

1. **`PatternRedactor` — each pattern class.** An OpenAI/Anthropic/AWS/GitHub/Slack/Google key,
   a `Bearer <token>`, a PEM private-key block, and an `api_key=<value>` assignment each become
   `[REDACTED]` (value portion); a clean string (`"hello world"`, a plain UUID, a short word) is
   **untouched**.
2. **Recursive walk.** A nested `{a:{b:["sk-…", "clean"]}, k:"AKIA…"}` redacts the secret string
   leaves at any depth and in arrays; object **keys** and non-string scalars are unchanged.
3. **Purity.** `redact(x) == redact(x)` for the same input (deterministic; no state).
4. **Tool-result redaction (determinism-safe).** With a redactor wired, a tool returning a
   secret → the journaled `EffectRecorded.output` for that call is redacted **and** the value
   fed back to the agent (the `ToolOutcome::Ok`) is redacted — the plaintext secret appears in
   neither.
5. **Model-`text` redaction; `tool_calls` intact.** A model turn whose `text` contains a secret
   → the journaled + threaded text is redacted, while a `tool_calls` argument in the same output
   is unchanged (the tool still receives its real arguments).
6. **Resume replays the redacted output.** A run with a redactor that redacted a tool output,
   resumed, replays the redacted value from the memo (no `DeterminismViolation`, tool not
   re-invoked). (Mutation-verified.)
7. **CAS blob redacted.** With a `ContentStore` wired + an over-`cas_threshold` output
   containing a secret, the bytes `put` into the CAS are redacted (redaction precedes
   `split_output`).
8. **Additive.** No redactor wired ⇒ every effect output is byte-identical; the full existing
   suite passes unchanged.
9. **End-to-end.** An agent calls a tool that returns a secret, through the executor with a
   `PatternRedactor` wired; the final journal (all `EffectRecorded` outputs) and the agent
   transcript contain `[REDACTED]`, never the plaintext secret.
