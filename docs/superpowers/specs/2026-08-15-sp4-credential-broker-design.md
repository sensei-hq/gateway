---
title: SP-4 slice — Ephemeral credential broker
doctype: design
module: orchestrator
spec: SP-4
status: approved
companion: ./2026-08-06-sensei-orchestrator-design.md (§13 enforcement & isolation, §288 sandbox + ephemeral credential broker); ./2026-08-14-sp4-permission-enforcement-design.md (s1 — the authorize boundary); ./2026-08-14-sp4-secret-redaction-design.md (s2 — the `Redactor` this composes with); ./2026-08-15-sp4-exactly-once-idempotency-design.md (s5 — the `ToolContext`/`call_ctx` seam this extends); crates/vault (the encryption-at-rest `Vault` a real broker wraps)
date: 2026-08-15
---

# SP-4 slice — Ephemeral credential broker

## 1. Goal

Let a tool authenticate to an external system **without the secret ever touching the model or
the durable journal**. A `CredentialBroker` (injected, wraps the `vault`) resolves a tool's
**declared** credential refs and the executor injects the resolved `Secret`s into the tool's
**execution `ToolContext`** — ephemeral, zeroized on drop, never journaled, never hashed, never
in the prompt. This answers the "how does a tool safely GET a secret" question and completes the
SP-4 security arc: s1 **authorizes** the tool, s2 **redacts** leaks, s5 makes writes
**exactly-once**, and the broker **provides the credential out-of-band**. A per-call
known-value scrub (composing with s2) closes the echo-leak: a tool that returns its own
credential has it scrubbed by exact value.

**Scope (user-chosen): the credential broker only.** Sandbox *confinement* and resource-cap
*killing* are **deferred** (§6) — both need a killable/confineable execution unit (a subprocess),
i.e. a tool-execution-model decision this slice does not take. This slice is self-contained (no
new infrastructure beyond the `vault` seam) and byte-identical when no broker is wired.

## 2. Background & impact review

- **s5 shipped `ToolContext{idempotency_key, effect_id}`** (`crates/orchestrator/src/agent/
  tools.rs`), passed to `Tool::call_ctx(args, &ToolContext)` — the seam this extends with
  `credentials`.
- **No credential-injection concept exists** in the orchestrator: a tool has no way to obtain a
  secret. The `vault` crate (`Vault<K,S>`: `resolve_router_key`/`resolve_oauth`, tenant-scoped,
  zeroize/KEK-encrypted) holds real credentials but the orchestrator does not consume it. (The
  `credentials: Default::default()` fields at `support.rs`/`selector.rs` are the *gateway
  request*'s slot, unrelated.)
- **Why the tool DECLARES its cred needs statically:** the broker's `resolve` is async (wraps
  the async vault), but `Tool::call_ctx` is **sync**, so the executor must resolve creds *before*
  the call. A per-call dynamic request can't work across the sync/async boundary — so a tool
  declares its refs on its `ToolSpec` and the executor resolves them upfront.
- **Impact:** additive — an injected `Option<Arc<dyn CredentialBroker>>` (default none ⇒
  byte-identical), a `credentials: Vec<String>` field on `ToolSpec` (`#[serde(default)]`, empty),
  a `credentials` map on `ToolContext`, and a resolve+inject+scrub step in `record_tool_effect`.
  A tool declaring no creds, or no broker wired, is unchanged.

## 4. Design

### 4.1 The `CredentialBroker` seam + `Secret`

`orchestrator-core` (`credential.rs`, `zeroize` dep):
```rust
/// A secret value. `Debug` prints `[REDACTED]`; the bytes are zeroized on drop.
pub struct Secret(zeroize::Zeroizing<String>);
impl Secret {
    pub fn new(s: impl Into<String>) -> Self { Self(zeroize::Zeroizing::new(s.into())) }
    /// Expose the raw secret — call sites must NOT journal/log the returned &str.
    pub fn expose(&self) -> &str { &self.0 }
}
impl std::fmt::Debug for Secret { /* writes "[REDACTED]" */ }

#[async_trait::async_trait]
pub trait CredentialBroker: Send + Sync {
    /// Resolve a credential ref (e.g. "stripe_key") to its secret. Unknown ref → `Ok(None)`.
    async fn resolve(&self, cred_ref: &str) -> Result<Option<Secret>, OrchestratorError>;
}
```
A demo `StaticCredentialBroker(HashMap<String, String>)` proves the pattern in tests; a real impl
wraps `vault::Vault` (`resolve_router_key`/`resolve_oauth`). Injected on the `Executor` as
`credential_broker: Option<Arc<dyn CredentialBroker>>` + `with_credential_broker` (default none).

### 4.2 Declare + resolve + inject

- **`ToolSpec.credentials: Vec<String>`** (`#[serde(default)]`) — the cred refs the tool needs,
  declarative alongside s1's `permissions`. Empty ⇒ the tool needs no creds.
- **`ToolContext.credentials: HashMap<String, Secret>`** (extends s5's `ToolContext`).
- In `record_tool_effect`, **before** `execute_ctx`: for each ref in the tool's
  `spec().credentials`, `broker.resolve(ref).await?` → insert `(ref, secret)` into the context's
  `credentials`. A tool reads `ctx.credentials.get("stripe_key").map(Secret::expose)` and sends
  it to its API. No broker wired ⇒ the map stays empty (a tool that needs a cred and finds none
  fails its own call — §4.4 resolve-failure).

### 4.3 Ephemeral & determinism

The injected secret lives ONLY in the `ToolContext` for the call's duration:
- **Never journaled** — not in `EffectRecorded`, `EffectIntent`, or `ContextWrite`.
- **Not in any `input_hash`** — the per-effect hash is over `args`; the cred **ref** is static
  config, the resolved **value** is never hashed. So creds add ZERO determinism surface.
- **Not re-injected on a memoized resume** — a completed tool replays its journaled (secret-free)
  output from the memo; it is never re-run, so the broker is not re-consulted for it.
- **Zeroized** when the `ToolContext` drops (via `Secret`'s `Zeroizing`).

### 4.4 The echo-leak boundary — a per-call known-value scrub (determinism-safe)

The broker hands the tool a secret to **use** (send to its API), not **return**. To harden
against a tool that echoes a credential into its output (a bug s2's pattern redaction may miss
for a novel-shape secret), the executor performs a **local, per-call** exact-value scrub: after
the tool returns, it replaces every occurrence of each **injected** credential value (the ones in
THIS call's `ToolContext.credentials`) in the output with `[REDACTED]`, composing with s2's
pattern redaction — in `record_tool_effect`, before journaling and the return:
```
let result = tool.call_ctx(args, &ctx)?;
let result = self.redact(&result);                       // s2 pattern (unchanged)
let result = scrub_secret_values(&result, ctx.secret_values()); // s4 known-value, THIS call's creds
// split_output(&result) journaled; Ok(result) returned — both the scrubbed value
```
**Why per-call, not a run-wide known-value set:** a run-wide accumulating set would be mutable
state whose contents differ between a live run and a resume (a memoized cred-injecting tool is
not re-run, so its value would be absent from the set on resume) — redacting a *later* tool's
output differently across the seam and tripping the determinism fence. The per-call scrub avoids
this entirely: it is a **pure function of (this tool's output, this call's injected creds)** —
identical live and on a resume-live re-run (same static refs → same creds), and a memoized tool
replays its already-scrubbed journaled output. A tool only ever holds its **own** declared creds,
so a per-call scrub covers every realistic echo (a tool cannot hold another tool's cred).

### 4.5 Additive & trust boundary

- **Additive:** no broker wired, or a tool with empty `credentials`, ⇒ the `ToolContext.credentials`
  map is empty, `scrub_secret_values` over an empty cred set is the identity, and the whole path
  is **byte-identical**.
- **Trust boundary:** the broker keeps a secret out of the model + the journal and scrubs an
  accidental echo — but a tool is trusted to USE the secret responsibly (it could still send it
  somewhere it shouldn't). True egress confinement is the sandbox (deferred). The `expose()` API
  is the single audited point where plaintext is available; call sites must not log/journal it.

## 5. Decisions

- **D1 — injected `CredentialBroker` seam, default none** [approved]: additive; matches the
  Redactor/Reconcile seams; a real impl wraps the `vault`. Demo `StaticCredentialBroker` proves it.
- **D2 — tools declare cred refs statically (`ToolSpec.credentials`); executor resolves+injects
  into `ToolContext.credentials` before the sync `call_ctx`** [approved]: forced by the async
  broker / sync `call_ctx` boundary; declarative, alongside `permissions`.
- **D3 — `Secret` = `Zeroizing<String>` with a `[REDACTED]` Debug + an audited `expose()`**
  [approved]: no memory-lingering plaintext, no Debug/log leak.
- **D4 — ephemeral: never journaled/hashed/re-injected** [approved]: zero determinism/durable
  surface; resume replays the secret-free journaled output.
- **D5 — echo-leak closed by a per-call exact-value scrub composing with s2, NOT a run-wide set**
  [approved]: determinism-safe (pure over this call's output+creds); a run-wide set would diverge
  across resume.

## 6. Deferred (stated)

- **Sandbox confinement + resource-cap killing** — need a killable/confineable execution unit
  (subprocess/external tool); blocked on the tool-execution-model decision. The heavy remainder
  of §288.
- **Real vault-backed broker** — this slice ships the seam + a `StaticCredentialBroker` demo; a
  `VaultCredentialBroker` wrapping `vault::Vault` (tenant-scoped, `resolve_oauth`/`resolve_router_key`)
  is the production impl.
- **Per-tenant / per-agent credential scoping + grant** — which agent may request which cred
  (an authorization layer over the broker, à la s1 grants); this slice resolves any declared ref.
- **Cross-tool / arg-passed credential leaks** — out of scope (a tool holds only its own creds).
- **Credential rotation/expiry mid-run**, streaming-secret injection.

## 7. Acceptance criteria (TDD)

1. **`Secret` hygiene.** `Secret::new("sk-x").expose() == "sk-x"`; `format!("{:?}", secret)` is
   `[REDACTED]` (never the value). (Zeroize-on-drop is structural via `Zeroizing`; a `Debug`
   assertion + `expose` round-trip cover the observable contract.)
2. **Seam + demo.** `CredentialBroker` trait + `StaticCredentialBroker`; `Executor::with_credential_broker`;
   `broker.resolve("known") == Some`, `resolve("unknown") == None`.
3. **Declared creds injected.** A tool declaring `ToolSpec.credentials = ["api_token"]`, run with
   a broker holding `api_token`, receives `ctx.credentials["api_token"]` (== the secret) in its
   `call_ctx`. A tool with empty `credentials`, or no broker wired, gets an empty map.
4. **Ephemeral.** The injected secret does NOT appear in any journaled `EffectRecorded`/
   `EffectIntent`, is NOT in the effect `input_hash` (a run with vs without the cred hashes the
   same over identical `args`), and a memoized resume does NOT re-invoke the broker for that tool.
5. **Echo-leak scrubbed by exact value.** A tool that returns its injected credential in its
   output → the journaled output + the value fed back are `[REDACTED]` (exact-value scrub), even
   for a secret whose shape s2's patterns would NOT catch (e.g. a plain word like `hunter2`).
6. **Resolve failure is loud.** A tool declaring a cred ref the broker returns `None` for (or a
   broker error) → the tool call fails loud (surfaced as a node failure), never a silent
   empty/missing credential.
7. **Additive.** No broker + tools with empty `credentials` ⇒ byte-identical; the full existing
   suite (incl. s5 idempotency + s2 redaction + s1 gate) passes unchanged.
8. **End-to-end.** An agent's tool authenticates using a broker-injected secret (uses `expose()`),
   completes; scanning the whole journal + the agent transcript finds no plaintext secret.
