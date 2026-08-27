# SP-4 Credential Broker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a tool authenticate to an external system with a broker-injected secret that never touches the model or the durable journal.

**Architecture:** A `CredentialBroker` seam (injected, default none) + a `Secret` type (zeroized, Debug-redacting). A tool declares its cred refs on `ToolSpec.credentials`; the executor resolves them via the broker and injects `Secret`s into the s5 `ToolContext.credentials` (ephemeral — never journaled/hashed/re-injected). After a tool returns, the executor scrubs the output of the exact injected secret values (per-call, composing with s2 redaction) to close the echo-leak. Byte-identical when no broker is wired / a tool declares no creds.

**Tech Stack:** Rust workspace crates `sensei-orchestrator-core` (`Secret`/`CredentialBroker`, `zeroize`; `ToolSpec.credentials`) and `sensei-orchestrator` (the `Executor` + `ToolContext` + `record_tool_effect`). Design: `docs/superpowers/specs/2026-08-15-sp4-credential-broker-design.md`.

---

## File Structure

- `crates/orchestrator-core/Cargo.toml` **(modify)** — add `zeroize`.
- `crates/orchestrator-core/src/credential.rs` **(create)** — `Secret` + `CredentialBroker` trait.
- `crates/orchestrator-core/src/lib.rs` **(modify)** — export `credential::{CredentialBroker, Secret}`.
- `crates/orchestrator-core/src/registry.rs` **(modify)** — `ToolSpec.credentials: Vec<String>`.
- `crates/orchestrator/src/agent/tools.rs` **(modify)** — `ToolContext.credentials` + `secret_values()`; a `pub StaticCredentialBroker` demo.
- `crates/orchestrator/src/executor/mod.rs` **(modify)** — `credential_broker` field + `with_credential_broker`.
- `crates/orchestrator/src/executor/content.rs` **(modify)** — `scrub_secret_values` helper.
- `crates/orchestrator/src/executor/agent.rs` **(modify)** — resolve+inject in `record_tool_effect` + the per-call scrub + resolve-failure-loud.
- `crates/orchestrator/src/executor/tests.rs` **(modify)** — integration/e2e tests.

House rules: `cargo fmt --all` before every commit (pre-commit hook = fmt-check + workspace `clippy -D warnings`, runs NO tests). Verify REAL exit codes — read cargo's `test result:` line, never pipe to decide pass/fail. ⚠️ The repo's **semgrep CWE-798 hook blocks hard-coded credential-shaped literals** — assemble any secret-shaped test fixture at runtime (`format!`/`.join`). Do NOT push.

---

## Task 1: Core — `Secret` + `CredentialBroker` + `ToolSpec.credentials`

**Files:**
- Modify: `crates/orchestrator-core/Cargo.toml`
- Create: `crates/orchestrator-core/src/credential.rs`
- Modify: `crates/orchestrator-core/src/lib.rs`, `crates/orchestrator-core/src/registry.rs`

- [ ] **Step 1: Add `zeroize`**

In `crates/orchestrator-core/Cargo.toml` `[dependencies]`, add `zeroize = "1"` (grep other `crates/*/Cargo.toml` for a pinned `zeroize` version — the `vault` crate uses it — and match that string; it's a pure dep, fine for the zero-I/O core).

- [ ] **Step 2: Write the failing tests**

Create `crates/orchestrator-core/src/credential.rs` with the tests first:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_exposes_but_debug_redacts() {
        let raw = format!("sk-{}", "abcdefghij"); // runtime-assembled (semgrep hook)
        let s = Secret::new(raw.clone());
        assert_eq!(s.expose(), raw, "expose returns the raw value");
        assert_eq!(format!("{s:?}"), "[REDACTED]", "Debug never leaks the value");
        assert!(!format!("{s:?}").contains(&raw));
    }
}
```

- [ ] **Step 3: Run to verify FAIL**

Run: `cargo test -p sensei-orchestrator-core --lib secret_exposes_but_debug_redacts`
Expected: FAIL to compile (`Secret` undefined). Read the real error, no piping.

- [ ] **Step 4: Implement `Secret` + `CredentialBroker`**

Prepend to `credential.rs`:
```rust
//! Ephemeral credential broker (SP-4). A `CredentialBroker` resolves a tool's declared
//! credential refs to `Secret`s that the executor injects into the tool's `ToolContext`
//! — never journaled, never in the prompt (design §4).

use zeroize::Zeroizing;

use crate::error::OrchestratorError;

/// A secret value. `Debug` prints `[REDACTED]`; the bytes are zeroized on drop.
#[derive(Clone)]
pub struct Secret(Zeroizing<String>);

impl Secret {
    pub fn new(s: impl Into<String>) -> Self {
        Self(Zeroizing::new(s.into()))
    }
    /// Expose the raw secret. Call sites MUST NOT journal/log the returned `&str`.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED]")
    }
}

/// Resolves a tool's declared credential refs to secrets. Injected on the `Executor`
/// (default none). A real impl wraps `vault::Vault`; `StaticCredentialBroker` is a demo.
#[async_trait::async_trait]
pub trait CredentialBroker: Send + Sync {
    /// Resolve a credential ref (e.g. `"stripe_key"`) to its secret. Unknown → `Ok(None)`.
    async fn resolve(&self, cred_ref: &str) -> Result<Option<Secret>, OrchestratorError>;
}
```
(Confirm `async_trait` is already a core dep — `ReconcileProvider` uses `#[async_trait::async_trait]`, so it is.)

- [ ] **Step 5: Export + add `ToolSpec.credentials`**

In `lib.rs`: `pub mod credential;` (alphabetical — after `pub mod content;`/before `pub mod context;`, wherever it sorts) and `pub use credential::{CredentialBroker, Secret};`.
In `registry.rs` `ToolSpec` (after the `activation` field), add:
```rust
    /// Credential refs this tool needs (SP-4 broker); resolved by the injected
    /// `CredentialBroker` and injected into the call's `ToolContext.credentials`
    /// (ephemeral). Empty ⇒ the tool needs no credentials.
    #[serde(default)]
    pub credentials: Vec<String>,
```
Add a serde-default test in `registry.rs` `mod tests`:
```rust
    #[test]
    fn tool_spec_credentials_default_empty() {
        let spec: ToolSpec =
            serde_json::from_str(r#"{"name":"t","input_schema":{},"effect_class":"Pure"}"#).unwrap();
        assert!(spec.credentials.is_empty());
    }
```
Every `ToolSpec { .. }` struct-literal in core/orchestrator will now fail to compile (missing field) — the compiler lists them; add `credentials: Vec::new()` (or `credentials: vec![]`) to each. (Grep `ToolSpec {` across `crates/` — there are demo tools in `agent/tools.rs` + tests; add the field. This is mechanical.)

- [ ] **Step 6: Run to verify PASS + lint**

Run: `cargo test -p sensei-orchestrator-core --lib` → the 2 new tests pass + whole core green. Then `cargo build -p sensei-orchestrator` (the `ToolSpec {` sites in the orchestrator crate compile with the new field). Read real `test result:`/exit codes. `cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings` exit 0.

- [ ] **Step 7: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-core/Cargo.toml crates/orchestrator-core/src/credential.rs crates/orchestrator-core/src/lib.rs crates/orchestrator-core/src/registry.rs crates/orchestrator/src/agent/tools.rs crates/orchestrator/src/executor/tests.rs
# stage any other file whose `ToolSpec {` literal you had to extend
git commit -m "feat(orchestrator): SP-4 broker (1/4) — Secret + CredentialBroker + ToolSpec.credentials"
```

---

## Task 2: `ToolContext.credentials` + `StaticCredentialBroker` demo + `Executor::with_credential_broker`

**Files:**
- Modify: `crates/orchestrator/src/agent/tools.rs` (`ToolContext` ~17; add `StaticCredentialBroker`)
- Modify: `crates/orchestrator/src/executor/mod.rs` (Executor field + `with_*` + default)
- Modify: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Write the failing tests**

In `agent/tools.rs` `mod tests`:
```rust
    #[tokio::test]
    async fn static_broker_resolves_known_refs() {
        let mut m = std::collections::HashMap::new();
        m.insert("api_token".to_string(), format!("tok-{}", "xyz"));
        let broker = StaticCredentialBroker::new(m);
        let got = broker.resolve("api_token").await.unwrap();
        assert_eq!(got.as_ref().map(|s| s.expose()), Some(format!("tok-{}", "xyz").as_str()));
        assert!(broker.resolve("nope").await.unwrap().is_none());
    }

    #[test]
    fn tool_context_secret_values_lists_injected() {
        let mut creds = std::collections::HashMap::new();
        creds.insert("k".to_string(), orchestrator_core::Secret::new("s3cret"));
        let ctx = ToolContext {
            idempotency_key: "i".into(),
            effect_id: orchestrator_core::effect::effect_id("n", 0, 0),
            credentials: creds,
        };
        assert_eq!(ctx.secret_values(), vec!["s3cret"]);
    }
```

- [ ] **Step 2: Run to verify FAIL**

Run: `cargo test -p sensei-orchestrator --lib static_broker_resolves_known_refs tool_context_secret_values_lists_injected` (run each name separately). Expected: FAIL — `StaticCredentialBroker`, `ToolContext.credentials`, `secret_values` undefined.

- [ ] **Step 3: Extend `ToolContext` + add `secret_values`**

In `agent/tools.rs`, add the field to `ToolContext` (it derives `Debug, Clone` — `Secret` is `Debug`+`Clone`, so it stays deriving):
```rust
    /// Broker-resolved credentials for THIS call (SP-4). Ephemeral — never journaled/
    /// hashed; zeroized on drop. A tool reads `ctx.credentials.get(ref).map(Secret::expose)`
    /// and sends it to its external API.
    pub credentials: std::collections::HashMap<String, orchestrator_core::Secret>,
```
Add an impl:
```rust
impl ToolContext {
    /// The raw injected secret values, for the executor's per-call exact-value scrub.
    pub fn secret_values(&self) -> Vec<&str> {
        self.credentials.values().map(orchestrator_core::Secret::expose).collect()
    }
}
```
Every `ToolContext { idempotency_key, effect_id }` literal (in `record_tool_effect` + tests: `KeyProbe`, `IdempotentStore` tests, etc.) now needs `credentials: Default::default()` — the compiler lists them; add it. (Grep `ToolContext {`.)

- [ ] **Step 4: Add the demo broker**

In `agent/tools.rs` (near the demo tools):
```rust
/// Demo credential broker: an in-memory `ref → secret` map (SP-4). A real broker wraps
/// `vault::Vault`.
pub struct StaticCredentialBroker(std::collections::HashMap<String, String>);

impl StaticCredentialBroker {
    pub fn new(map: std::collections::HashMap<String, String>) -> Self {
        Self(map)
    }
}

#[async_trait::async_trait]
impl orchestrator_core::CredentialBroker for StaticCredentialBroker {
    async fn resolve(
        &self,
        cred_ref: &str,
    ) -> Result<Option<orchestrator_core::Secret>, OrchestratorError> {
        Ok(self.0.get(cred_ref).map(orchestrator_core::Secret::new))
    }
}
```

- [ ] **Step 5: Executor plumbing (`mod.rs`)**

Add the field to `struct Executor` (near `redactor`): `credential_broker: Option<Arc<dyn orchestrator_core::CredentialBroker>>,`. In `Executor::new` default it `None`. Add the builder (near `with_redactor`):
```rust
    /// Wire a [`CredentialBroker`](orchestrator_core::CredentialBroker) (SP-4). Default none
    /// ⇒ tools declaring no credentials are unchanged; a tool that declares a credential ref
    /// with no broker (or an unresolvable ref) fails loud (never a silent missing credential).
    pub fn with_credential_broker(mut self, broker: Arc<dyn orchestrator_core::CredentialBroker>) -> Self {
        self.credential_broker = Some(broker);
        self
    }
```
(`Executor` derives `Clone`, so the per-run pin carries the broker — no manual clone site. If a manual `Executor { .. }` literal exists, add the field; the compiler will flag it.)

- [ ] **Step 6: Run to verify PASS + lint**

Run: `cargo test -p sensei-orchestrator --lib` → the 2 new tests pass + whole lib green (the new field is inert until Task 3 wires it). `cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings` exit 0.

- [ ] **Step 7: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/agent/tools.rs crates/orchestrator/src/executor/mod.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-4 broker (2/4) — ToolContext.credentials + StaticCredentialBroker + with_credential_broker"
```

---

## Task 3: Resolve + inject in `record_tool_effect` + the per-call scrub

**Files:**
- Modify: `crates/orchestrator/src/executor/content.rs` (`scrub_secret_values`)
- Modify: `crates/orchestrator/src/executor/agent.rs` (`record_tool_effect`)
- Modify: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Add the `scrub_secret_values` helper (`content.rs`)**

Add a free function (near the `redact` helper):
```rust
/// Scrub a tool output of the EXACT secret values injected into this call (SP-4 broker) —
/// replace each occurrence in every string leaf with `[REDACTED]`, composing with the s2
/// pattern redactor. Per-call + pure ⇒ determinism-safe (a tool holds only its own creds).
pub(super) fn scrub_secret_values(v: &serde_json::Value, secrets: &[&str]) -> serde_json::Value {
    if secrets.is_empty() {
        return v.clone();
    }
    match v {
        serde_json::Value::String(s) => {
            let mut out = s.clone();
            for secret in secrets {
                if !secret.is_empty() {
                    out = out.replace(secret, "[REDACTED]");
                }
            }
            serde_json::Value::String(out)
        }
        serde_json::Value::Array(a) => {
            serde_json::Value::Array(a.iter().map(|x| scrub_secret_values(x, secrets)).collect())
        }
        serde_json::Value::Object(o) => serde_json::Value::Object(
            o.iter().map(|(k, x)| (k.clone(), scrub_secret_values(x, secrets))).collect(),
        ),
        other => other.clone(),
    }
}
```
(If `content.rs` is `impl Executor` only, add this as a free `pub(super) fn` at module scope, or as an `&self`-free `pub(super) fn` — it needs no executor state. Make sure `agent.rs` can call it, e.g. `super::content::scrub_secret_values(...)` or a re-export.)

- [ ] **Step 2: Write the failing tests (`tests.rs`)**

STUDY the existing tool tests (grep `RecordNote`/`ScopedWriter`/`scripted_gateway`, how a Mutation tool's `EffectIntent`/`EffectRecorded` is read, and the s2 redaction tests). Add a local tool that DECLARES a credential and (a) receives it in `call_ctx`, (b) can optionally ECHO it. Then:
- `declared_credential_is_injected_into_call_ctx` (AC3): a local tool with `spec().credentials = ["api_token"]` that records the `ctx.credentials.get("api_token").map(expose)` it received into a shared cell; run it with `with_credential_broker(StaticCredentialBroker{api_token: <runtime-assembled secret>})`; assert the tool received the secret. A second run with NO broker → the tool received `None`/empty (and fails loud per AC6 — see below) OR a tool with empty `credentials` → empty map, runs fine.
- `echoed_credential_is_scrubbed_by_exact_value` (AC5): a local tool that RETURNS its injected credential in its output (e.g. `{"leaked": ctx.credentials["api_token"].expose()}`), with a broker holding a NON-secret-shaped value like `format!("hun{}", "ter2")` (so s2's PATTERNS do NOT catch it — proving the exact-value scrub, not the pattern redactor); assert the journaled `EffectRecorded.output` + the fed-back value show `[REDACTED]`, never the plaintext.
- `credential_is_ephemeral_not_in_journal_or_hash` (AC4): the injected secret does not appear anywhere in the serialized journal; and a run of the SAME tool+args WITH vs WITHOUT the cred produces the SAME effect `input_hash` (the cred is not hashed). (Read the `EffectIntent.args_hash`/the effect `input_hash` via the existing helpers.)
- `unresolvable_declared_credential_fails_loud` (AC6): a tool declaring `["missing"]` + a broker that returns `None` (or no broker wired) → the tool call fails (node `Failed`), never a silent missing cred.

- [ ] **Step 3: Wire resolve + inject + scrub in `record_tool_effect`**

In `record_tool_effect` (agent.rs), BEFORE building the `ToolContext`, resolve the tool's declared creds; then include them in the ctx; then scrub the output after the s2 redact:
```rust
        // SP-4 broker: resolve the tool's DECLARED credential refs + inject into the ctx
        // (ephemeral — never journaled/hashed). A declared ref that cannot be resolved
        // (no broker, or the broker returns None) fails loud — never a silent missing cred.
        let mut credentials = std::collections::HashMap::new();
        if let Some(spec) = self.tools.spec_of(&call.name) {
            for cred_ref in &spec.credentials {
                let resolved = match &self.credential_broker {
                    Some(broker) => broker.resolve(cred_ref).await?,
                    None => None,
                };
                match resolved {
                    Some(secret) => {
                        credentials.insert(cred_ref.clone(), secret);
                    }
                    None => {
                        let msg = format!(
                            "tool '{}' requires credential '{}' but no broker resolved it",
                            call.name, cred_ref
                        );
                        self.append(
                            ar.run,
                            JournalEvent::NodeFailed { node: ar.node_id.clone(), error: msg.clone() },
                        )
                        .await?;
                        return Ok(ToolOutcome::Failed(msg));
                    }
                }
            }
        }
        let ctx = crate::agent::tools::ToolContext {
            idempotency_key: idempotency_key.to_string(),
            effect_id: teid.clone(),
            credentials,
        };
        match self.tools.execute_ctx(&call.name, args, &ctx) {
            Ok(result) => {
                let result = self.redact(&result); // s2 pattern (unchanged)
                let result = super::content::scrub_secret_values(&result, &ctx.secret_values()); // s4 exact-value
                let recorded = self.split_output(&result).await?;
                // ... UNCHANGED: append EffectRecorded { output: recorded, .. }; Ok(ToolOutcome::Ok(result))
            }
            // Err arm UNCHANGED.
        }
```
(Adapt the `super::content::scrub_secret_values` path to how `agent.rs` reaches `content.rs` — if `content`'s items are already in scope via `use super::content::...` or `impl Executor`, call accordingly. The gate is: redact THEN scrub, both before `split_output` and the returned `result`.)

- [ ] **Step 4: Run + regressions**

Run: `cargo test -p sensei-orchestrator` → the 4 new tests pass + the FULL suite green. CRITICAL: existing tool/reconcile/redaction tests are byte-identical (tools with no `credentials` → empty map → `scrub_secret_values` over `[]` is identity → unchanged). Read the real `test result:` line, exit 0. `cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings` exit 0.

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/content.rs crates/orchestrator/src/executor/agent.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-4 broker (3/4) — resolve+inject declared creds into ToolContext + per-call exact-value scrub; unresolvable→fail loud"
```

---

## Task 4: End-to-end + determinism + full-suite gate

**Files:**
- Modify: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Determinism-on-resume (AC4 resume clause)**

Add `broker_not_reinvoked_for_a_memoized_tool_on_resume`: run a tool that uses a broker cred to completion, journal it, then resume over the same journal with a broker that COUNTS `resolve` calls (an `Arc<AtomicUsize>`-backed broker). Assert the memoized tool replays from the memo and the resume broker's `resolve` count is 0 for that effect (the tool is not re-run ⇒ the broker is not re-consulted), and the run completes with no `DeterminismViolation`. (Mirror the existing resume-truncation idiom.)

- [ ] **Step 2: End-to-end (AC8)**

Add `agent_tool_authenticates_with_injected_secret_no_plaintext_e2e`: an agent whose tool DECLARES a credential, driven through the (scripted/test) gateway with a `StatusQueryReconciler`-style `StaticCredentialBroker` wired; the tool `expose()`s the secret to do its work (e.g. returns a masked confirmation that does NOT echo the raw value) and completes. Assert: the run completes; scanning the WHOLE journal (all `EffectRecorded` outputs, materialized) + the final agent output finds NO plaintext secret. (This is the headline "the secret never lands" e2e; reuse the Task-3 tool/broker harness.)

- [ ] **Step 3: Additive full-suite + lint gate (AC7)**

Run: `cargo test --workspace` — read the REAL exit code + aggregate DIRECTLY (file + `echo $?`; NO pipe-to-tail-to-decide — mandatory rule). Confirm 0 failed; report the total (baseline before this slice ~1070 + the broker additions). Then `cargo fmt --all --check` (exit 0) + `cargo clippy --workspace --all-targets -- -D warnings` (exit 0). Confirm the existing s1/s2/s5 suites are byte-identical green (additivity).

- [ ] **Step 4: Commit (do NOT push — the coordinator pushes after the whole-slice review)**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): SP-4 broker (4/4) — resume-not-reinvoked + no-plaintext e2e; full-suite green"
```

---

## Acceptance Criteria → Task map (self-review)

| Spec AC | Task | Test |
|---|---|---|
| 1 Secret hygiene | 1 | `secret_exposes_but_debug_redacts` |
| 2 seam + demo | 1, 2 | (`CredentialBroker`/`ToolSpec.credentials` compile) + `static_broker_resolves_known_refs` |
| 3 declared creds injected | 3 | `declared_credential_is_injected_into_call_ctx` (+ `tool_context_secret_values_lists_injected` in 2) |
| 4 ephemeral (not journaled/hashed/re-injected) | 3, 4 | `credential_is_ephemeral_not_in_journal_or_hash`, `broker_not_reinvoked_for_a_memoized_tool_on_resume` |
| 5 echo scrubbed by exact value | 3 | `echoed_credential_is_scrubbed_by_exact_value` |
| 6 resolve failure loud | 3 | `unresolvable_declared_credential_fails_loud` |
| 7 additive | 3, 4 | existing suites byte-identical + `cargo test --workspace` |
| 8 end-to-end | 4 | `agent_tool_authenticates_with_injected_secret_no_plaintext_e2e` |

**Deferred (spec §6, NOT in this plan):** sandbox confinement + resource-cap killing; real vault-backed broker; per-tenant/per-agent credential authorization; cross-tool/arg-passed leaks; rotation/expiry.

**Self-review notes:** (1) every spec §7 AC maps to a task. (2) No placeholders — all code shown; the executor tests (Task 3/4) give structure + assertions + a pointer to the existing tool/resume/redaction harness to mirror (idiom-heavy, same approach as prior slices). (3) Type consistency: `Secret::{new, expose}`, `CredentialBroker::resolve(&str) -> Result<Option<Secret>>`, `ToolSpec.credentials: Vec<String>`, `ToolContext.credentials: HashMap<String, Secret>` + `secret_values() -> Vec<&str>`, `Executor::with_credential_broker(Arc<dyn CredentialBroker>)`, `scrub_secret_values(&Value, &[&str]) -> Value`, and the `record_tool_effect(..., idempotency_key, record)` signature + the `spec_of`/`execute_ctx`/`redact` sites all match the real code (verified from `registry.rs`/`tools.rs`/`agent.rs`).
