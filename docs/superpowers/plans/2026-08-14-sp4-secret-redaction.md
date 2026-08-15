# SP-4 slice 2 — Secret Redaction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Scrub secrets from effect outputs (tool results + model-turn text) before they are journaled or fed back to the agent, via a pure, injected `Redactor` (default `PatternRedactor`).

**Architecture:** A `Redactor` trait in `orchestrator-core` (pure, replay-stable) with a `PatternRedactor` default that walks a JSON value and replaces curated secret-shape matches with `[REDACTED]`. The `Executor` gains an opt-in `redactor: Option<Arc<dyn Redactor>>` (`with_redactor`; default `None` ⇒ byte-identical). Redaction is applied at the two leaf output sites — the tool result and the model-turn `text` — **before both journaling and the agent-return**, so live == journaled == replayed (determinism-safe).

**Tech Stack:** Rust workspace crates `sensei-orchestrator-core` (pure types + the `Redactor`/`PatternRedactor`, `regex` dep) and `sensei-orchestrator` (the `Executor` leaf sites). Design: `docs/superpowers/specs/2026-08-14-sp4-secret-redaction-design.md`.

---

## File Structure

- `crates/orchestrator-core/Cargo.toml` **(modify)** — add `regex = "1"`.
- `crates/orchestrator-core/src/redact.rs` **(create)** — `Redactor` trait + `PatternRedactor` + patterns + the JSON walk.
- `crates/orchestrator-core/src/lib.rs` **(modify)** — `pub mod redact;` + `pub use redact::{PatternRedactor, Redactor};`.
- `crates/orchestrator/src/executor/mod.rs` **(modify)** — `redactor` field + `with_redactor` + the `redact` helper + the pinned-clone.
- `crates/orchestrator/src/executor/agent.rs` **(modify)** — apply redaction at the two leaf sites (`record_tool_effect`, `dispatch_model_turn`).
- `crates/orchestrator/src/executor/tests.rs` **(modify)** — integration/determinism/CAS/e2e tests.

House rules: `cargo fmt --all` before every commit (pre-commit hook = fmt-check + workspace `clippy -D warnings`, runs NO tests). Verify REAL exit codes — read cargo's `test result:` line, never pipe to `tail`/`grep` to decide pass/fail. Do NOT push (the coordinator pushes after the whole-slice review).

---

## Task 1: The `Redactor` trait + `PatternRedactor` (core)

**Files:**
- Modify: `crates/orchestrator-core/Cargo.toml`
- Create: `crates/orchestrator-core/src/redact.rs`
- Modify: `crates/orchestrator-core/src/lib.rs`

- [ ] **Step 1: Add the `regex` dependency**

In `crates/orchestrator-core/Cargo.toml` under `[dependencies]`, add:
```toml
regex = "1"
```
(Match the exact version other workspace crates pin if they differ — grep `regex = ` across the other `crates/*/Cargo.toml` and use that string; `regex` is a pure dep, no tokio/I-O, fine for the zero-I/O core.)

- [ ] **Step 2: Write the failing tests (in `redact.rs`, `#[cfg(test)] mod tests`)**

Create `crates/orchestrator-core/src/redact.rs` with ONLY the tests first (so they fail to compile → then add the impl):
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn redacts(s: &str) -> bool {
        PatternRedactor::default()
            .redact(&json!(s))
            .as_str()
            .unwrap()
            .contains("[REDACTED]")
    }

    #[test]
    fn scrubs_each_secret_class() {
        assert!(redacts("sk-abcdefghijklmnopqrstuvwx"), "OpenAI");
        assert!(redacts("sk-ant-abcdefghijklmnopqrstuvwx"), "Anthropic");
        assert!(redacts("AKIAIOSFODNN7EXAMPLE"), "AWS");
        assert!(redacts("ghp_1234567890abcdefghijklmnopqrstuvwx"), "GitHub PAT");
        assert!(redacts("xoxb-1234567890-abcdefghij"), "Slack");
        assert!(redacts("AIzaSyA1234567890abcdefghijklmnopqrs"), "Google");
        assert!(redacts("Bearer abcdef1234567890"), "bearer");
        assert!(
            redacts("-----BEGIN RSA PRIVATE KEY-----\nMIIabc\n-----END RSA PRIVATE KEY-----"),
            "PEM"
        );
    }

    #[test]
    fn assignment_form_redacts_only_the_value() {
        let out = PatternRedactor::default().redact(&json!("api_key=supersecretvalue"));
        let s = out.as_str().unwrap();
        assert!(s.contains("api_key="), "keeps the key label: {s}");
        assert!(s.contains("[REDACTED]"), "redacts the value: {s}");
        assert!(!s.contains("supersecretvalue"), "value gone: {s}");
    }

    #[test]
    fn clean_strings_are_untouched() {
        for clean in ["hello world", "a-short-id", "the quick brown fox", "1234"] {
            let out = PatternRedactor::default().redact(&json!(clean));
            assert_eq!(out.as_str().unwrap(), clean, "clean string changed: {clean}");
        }
    }

    #[test]
    fn walks_nested_json_leaves_only() {
        let v = json!({
            "a": { "b": ["sk-abcdefghijklmnopqrstuvwx", "clean"] },
            "AKIAIOSFODNN7EXAMPLE": "AKIAIOSFODNN7EXAMPLE",
            "n": 42,
            "ok": true
        });
        let out = PatternRedactor::default().redact(&v);
        // string leaf redacted at depth
        assert_eq!(out["a"]["b"][0], json!("[REDACTED]"));
        assert_eq!(out["a"]["b"][1], json!("clean"));
        // object KEY untouched (the secret-shaped key stays), its string VALUE redacted
        assert!(out.get("AKIAIOSFODNN7EXAMPLE").is_some(), "key not rewritten");
        assert_eq!(out["AKIAIOSFODNN7EXAMPLE"], json!("[REDACTED]"));
        // non-string scalars pass through
        assert_eq!(out["n"], json!(42));
        assert_eq!(out["ok"], json!(true));
    }

    #[test]
    fn is_pure_deterministic() {
        let r = PatternRedactor::default();
        let v = json!({ "t": "Bearer abcdef1234567890", "x": "clean" });
        assert_eq!(r.redact(&v), r.redact(&v));
    }
}
```

- [ ] **Step 3: Run to verify FAIL**

Run: `cargo test -p sensei-orchestrator-core --lib redact`
Expected: FAIL to compile (`Redactor`/`PatternRedactor` undefined). Read the real error, no piping.

- [ ] **Step 4: Implement `Redactor` + `PatternRedactor`**

Prepend to `crates/orchestrator-core/src/redact.rs` (above the `mod tests`):
```rust
//! Secret redaction (SP-4 slice 2). A pure, replay-stable scrub of effect outputs
//! applied before they are journaled or fed back to the agent (§4).

use regex::Regex;
use serde_json::Value;

/// Redact secrets from an effect output. MUST be pure (replay-stable) — no I/O,
/// clock, or RNG — since the redacted value is BOTH journaled and fed to the agent,
/// so a resume must reproduce it identically.
pub trait Redactor: Send + Sync {
    fn redact(&self, value: &Value) -> Value;
}

/// The fixed, type-agnostic placeholder (discloses nothing about the secret).
const PLACEHOLDER: &str = "[REDACTED]";

/// Pattern-based redactor: replaces substrings matching curated secret-SHAPE
/// patterns with `[REDACTED]`. Best-effort by shape (design §4.4 — misses novel
/// formats). ReDoS-safe: the `regex` crate uses finite automata (no backtracking),
/// so scanning adversarial tool output is linear-time.
pub struct PatternRedactor {
    /// Whole-match patterns → the entire match becomes the placeholder.
    whole: Vec<Regex>,
    /// `key = value` form → only the value (capture group 3) is redacted.
    assignment: Regex,
}

impl Default for PatternRedactor {
    fn default() -> Self {
        let whole = [
            r"sk-ant-[A-Za-z0-9_-]{20,}",
            r"sk-[A-Za-z0-9]{20,}",
            r"AKIA[0-9A-Z]{16}",
            r"ghp_[A-Za-z0-9]{36}",
            r"xox[baprs]-[A-Za-z0-9-]{10,}",
            r"AIza[0-9A-Za-z_-]{35}",
            r"(?i)bearer\s+[A-Za-z0-9._-]{8,}",
            r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----",
        ]
        .iter()
        .map(|p| Regex::new(p).expect("static redaction pattern compiles"))
        .collect();
        let assignment = Regex::new(
            r#"(?i)(api[_-]?key|secret|token|password|passwd)("?\s*[=:]\s*"?)([^\s"',]{6,})"#,
        )
        .expect("static redaction pattern compiles");
        Self { whole, assignment }
    }
}

impl PatternRedactor {
    /// Redact one string: the assignment form first (keep key label, redact the
    /// value), then the whole-match patterns.
    fn redact_str(&self, s: &str) -> String {
        let step = self
            .assignment
            .replace_all(s, |c: &regex::Captures| format!("{}{}{PLACEHOLDER}", &c[1], &c[2]));
        let mut out = step.into_owned();
        for re in &self.whole {
            out = re.replace_all(&out, PLACEHOLDER).into_owned();
        }
        out
    }
}

impl Redactor for PatternRedactor {
    fn redact(&self, value: &Value) -> Value {
        match value {
            Value::String(s) => Value::String(self.redact_str(s)),
            Value::Array(a) => Value::Array(a.iter().map(|v| self.redact(v)).collect()),
            // Redact string VALUES; leave object KEYS as-is (a secret-shaped key is
            // structural, not a leaked credential value).
            Value::Object(o) => {
                Value::Object(o.iter().map(|(k, v)| (k.clone(), self.redact(v))).collect())
            }
            other => other.clone(),
        }
    }
}
```

- [ ] **Step 5: Export from the crate root**

In `crates/orchestrator-core/src/lib.rs`, add `pub mod redact;` in the `pub mod` block (alphabetical: after `pub mod reconcile;` / before `pub mod registry;`) and `pub use redact::{PatternRedactor, Redactor};` in the `pub use` block.

- [ ] **Step 6: Run to verify PASS + lint**

Run: `cargo test -p sensei-orchestrator-core --lib redact` → all 5 tests PASS (read the real `test result: ok. N passed; 0 failed` line, exit 0). Then `cargo test -p sensei-orchestrator-core --lib` → whole core green. `cargo fmt --all`; confirm `cargo clippy --workspace --all-targets -- -D warnings` exits 0.

- [ ] **Step 7: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator-core/Cargo.toml crates/orchestrator-core/Cargo.lock crates/orchestrator-core/src/redact.rs crates/orchestrator-core/src/lib.rs
# (Cargo.lock is at the workspace root — `git add Cargo.lock` if the regex add changed it)
git commit -m "feat(orchestrator): SP-4 s2 (1/3) — Redactor trait + PatternRedactor (curated secret-shape scrub, ReDoS-safe)"
```

---

## Task 2: Executor injection + apply at the two leaf sites

**Files:**
- Modify: `crates/orchestrator/src/executor/mod.rs` (Executor field ~48, `new` defaults ~178, a `with_redactor` builder near `with_content_store` ~197, the pinned-clone ~902)
- Modify: `crates/orchestrator/src/executor/agent.rs` (`record_tool_effect` ~543, `dispatch_model_turn` ~588)
- Modify: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Add the `redactor` field + `with_redactor` + the `redact` helper + pinned-clone**

In `executor/mod.rs`:
- Add the field to `struct Executor` (near `content: Option<Arc<dyn ContentStore>>`, ~48):
  ```rust
      redactor: Option<Arc<dyn orchestrator_core::Redactor>>,
  ```
- In `Executor::new`, set the default (near `content: None`, ~178):
  ```rust
              redactor: None,
  ```
- Add the builder (near `with_content_store`, ~197):
  ```rust
      /// Wire a secret [`Redactor`](orchestrator_core::Redactor) (SP-4 s2). Default
      /// none ⇒ effect outputs are journaled/fed-back verbatim (byte-identical).
      /// Recommended for production: `with_redactor(Arc::new(PatternRedactor::default()))`.
      pub fn with_redactor(mut self, redactor: Arc<dyn orchestrator_core::Redactor>) -> Self {
          self.redactor = Some(redactor);
          self
      }
  ```
- Add the helper (an `impl Executor` method, e.g. next to `split_output` in `content.rs`, or in `mod.rs`):
  ```rust
      /// Scrub secrets from an effect output before it is journaled/returned (SP-4
      /// s2). Identity when no redactor is wired. Pure ⇒ live == journaled == replayed.
      pub(super) fn redact(&self, v: &serde_json::Value) -> serde_json::Value {
          match &self.redactor {
              Some(r) => r.redact(v),
              None => v.clone(),
          }
      }
  ```
- In the pinned-clone (`pinned`/`self.clone()` construction ~902 that copies `content: r.content.clone()`), add:
  ```rust
              redactor: r.redactor.clone(),
  ```
  (Grep the struct-literal construction around line 902 — it lists every field; add `redactor` alongside `content`. If `Executor` derives `Clone` and the pin uses `..`, no change is needed — verify.)

- [ ] **Step 2: Apply redaction at the two leaf sites (`agent.rs`)**

- **Tool result** — in `record_tool_effect`'s `Ok(result) =>` arm (~543), redact BEFORE `split_output` and the return:
  ```rust
          match self.tools.execute(&call.name, args) {
              Ok(result) => {
                  let result = self.redact(&result); // SP-4 s2: scrub before journal+return
                  let recorded = self.split_output(&result).await?;
                  self.append(/* ... EffectRecorded { output: recorded, ... } */).await?;
                  Ok(ToolOutcome::Ok(result))
              }
  ```
  (Keep the rest of the arm identical; only the added `let result = self.redact(&result);` line + the fact that `split_output` and the returned `result` now use the redacted value.)

- **Model turn text** — in `dispatch_model_turn`'s `Ok(response) =>` arm (~588), redact ONLY the `text` field (leave `model` + `tool_calls`):
  ```rust
              Ok(response) => {
                  let output = serde_json::json!({
                      "model": response.model,
                      "text": self.redact(&serde_json::Value::String(
                          response.content.clone().unwrap_or_default(),
                      )),
                      "tool_calls": response.tool_calls,
                  });
                  let recorded = self.split_output(&output).await?;
                  // ... unchanged: append EffectRecorded { output: recorded, ... }; Ok(ToolOutcome::Ok(output))
              }
  ```

- [ ] **Step 3: Write the integration tests (`tests.rs`)**

Study the existing tool tests (grep `RecordNote`, `ScopedWriter`, `scripted_gateway`, `final_response`, how a tool's output reaches the journal + how the agent transcript is inspected). Add a demo tool that returns a secret (a tiny local `Tool` in the test module, or reuse a `RecordNote`-style tool that echoes a secret string), an agent that calls it, and an `Executor::with_redactor(Arc::new(PatternRedactor::default()))`. Add:

- `tool_result_secret_is_redacted_in_journal_and_transcript` (AC4): a tool returns `{"key": "sk-abcdefghijklmnopqrstuvwx"}`; with a redactor wired, assert the journaled `EffectRecorded.output` for that call contains `[REDACTED]` and NOT the plaintext, AND the value fed back to the agent (the `ToolOutcome`/next-turn input) is redacted. Use the existing journal-inspection helpers (`recorded_output`/effect-id lookups) + the sink/transcript.
- `model_text_secret_is_redacted_tool_calls_intact` (AC5): a scripted gateway returns a model turn whose `content` (text) contains a secret AND a tool_call with a real argument; assert the journaled turn output's `text` is `[REDACTED]` while the `tool_calls` argument is unchanged (the tool receives its real args — e.g. the tool runs and its result reflects the real arg).
- `no_redactor_is_byte_identical` (AC8): the SAME tool-returns-a-secret run WITHOUT `with_redactor` → the plaintext secret IS present in the journal (proving redaction is opt-in and the tests above are load-bearing).

- [ ] **Step 4: Run + regressions**

Run: `cargo test -p sensei-orchestrator` (or the specific test names) → the new tests pass; the full orchestrator suite still green (no redactor wired anywhere else ⇒ byte-identical). Read the real `test result:` line, exit 0, no piping. `cargo fmt --all`; `cargo clippy --workspace --all-targets -- -D warnings` exit 0.

- [ ] **Step 5: Commit**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/mod.rs crates/orchestrator/src/executor/content.rs crates/orchestrator/src/executor/agent.rs crates/orchestrator/src/executor/tests.rs
git commit -m "feat(orchestrator): SP-4 s2 (2/3) — with_redactor + scrub at the tool-result & model-text leaf sites"
```

---

## Task 3: Determinism-resume + CAS + e2e + full-suite gate

**Files:**
- Modify: `crates/orchestrator/src/executor/tests.rs`

- [ ] **Step 1: Determinism-on-resume test (AC6)**

Add `redacted_output_replays_on_resume`. Property: a run with a redactor that redacted a tool output, then partially fails, resumes → the memo replays the **redacted** value (no `DeterminismViolation`, tool not re-invoked). STUDY the resume-truncation idiom (grep `_resume_`, `.run()`-seed / `.start()`-resume, `effect_recorded_count`). Shape: run 1 (redactor wired) drives a secret-returning tool → the redacted `EffectRecorded` is journaled → the run exhausts/fails partially; run 2 resumes over the SAME journal (redactor wired) with a fresh gateway → assert the run completes, no determinism violation, the tool's `EffectRecorded` for that call appears exactly once (`effect_recorded_count == 1`) and is redacted, and (if the tool has a side-effect sink) the sink was NOT touched on resume. Note in a comment that the redacted value is what both live and replay see (the determinism-safe placement).

- [ ] **Step 2: CAS-blob redaction test (AC7)**

Add `over_threshold_secret_is_redacted_in_cas`. With a `ContentStore` wired (`with_content_store`) + a low `cas_threshold` (`with_cas_threshold(small)`) + a redactor, a tool returns an output LARGER than the threshold that contains a secret. Assert the bytes stored in the CAS (fetch via the `ContentStore` by the journaled `ContentRef` digest, or assert the materialized value) contain `[REDACTED]` and NOT the plaintext — proving redaction precedes `split_output`. (Mirror the existing CAS tests for how a `ContentStore` is wired + read back.)

- [ ] **Step 3: End-to-end (AC9)**

Add `agent_tool_secret_never_lands_plaintext_e2e`: an agent, through the (scripted/test) gateway with a `PatternRedactor` wired, calls a tool that returns a secret; drive to completion; assert that scanning the ENTIRE journal (all `EffectRecorded` outputs, materialized) and the final agent output contains no plaintext secret (only `[REDACTED]`). If Task 2's AC4 test already covers the single-tool journal case, make this the broader "scan the whole journal" assertion so it adds value; if it would duplicate, say so and keep the stronger one.

- [ ] **Step 4: Full-workspace + lint gate (AC8 additive)**

Run: `cargo test --workspace` — read the REAL exit code + aggregate DIRECTLY (write to a file + `echo $?`; do NOT pipe to tail/grep to decide pass — masked exit codes violate a mandatory repo rule). Confirm 0 failed; report the total (baseline before this slice ~1043 + the s2 additions). Then `cargo fmt --all --check` (exit 0) + `cargo clippy --workspace --all-targets -- -D warnings` (exit 0).

- [ ] **Step 5: Commit (do NOT push — the coordinator pushes after the whole-slice review)**

```bash
cd /Users/Jerry/Developer/gateway
cargo fmt --all
git add crates/orchestrator/src/executor/tests.rs
git commit -m "test(orchestrator): SP-4 s2 (3/3) — redaction resume-determinism + CAS-blob + e2e; full-suite green"
```

---

## Acceptance Criteria → Task map (self-review)

| Spec AC | Task | Test |
|---|---|---|
| 1 each pattern class | 1 | `scrubs_each_secret_class`, `assignment_form_redacts_only_the_value` |
| 2 recursive walk (leaves; keys/scalars untouched) | 1 | `walks_nested_json_leaves_only` |
| 3 purity | 1 | `is_pure_deterministic` |
| 4 tool-result redaction (journal + fed-back) | 2 | `tool_result_secret_is_redacted_in_journal_and_transcript` |
| 5 model-`text` redaction; `tool_calls` intact | 2 | `model_text_secret_is_redacted_tool_calls_intact` |
| 6 resume replays redacted output | 3 | `redacted_output_replays_on_resume` |
| 7 CAS blob redacted | 3 | `over_threshold_secret_is_redacted_in_cas` |
| 8 additive (no redactor byte-identical) | 2, 3 | `no_redactor_is_byte_identical` + `cargo test --workspace` |
| 9 end-to-end | 3 | `agent_tool_secret_never_lands_plaintext_e2e` |

**Deferred (spec §6, NOT in this plan):** known-value/vault-backed `Redactor`; reversible tokenization/crypto-shred; entropy heuristic; redactor-version in the determinism fence; input-side (prompt/context) redaction; runtime confinement (sandbox slice 4).

**Self-review notes:** (1) every spec §7 AC maps to a task. (2) No placeholders — all code shown; the executor tests (Task 2/3) give structure + assertions + a pointer to the existing tool/CAS/resume harness to mirror (idiom-heavy, same approach as slice 1). (3) Type consistency: `Redactor::redact(&Value)->Value`, `PatternRedactor::default()`, `Executor::with_redactor(Arc<dyn Redactor>)`, `self.redact(&v)`, and the two leaf-site edits all match the real signatures read from the code (`record_tool_effect` Ok arm at agent.rs:543, `dispatch_model_turn` Ok arm at agent.rs:588, the `Option<Arc<dyn …>>`+`with_*`+pinned-clone injection pattern).
