# SP-DEC-3 — Laya local System One Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `laya` router id that sends System One decisions to a local `laya-coreml-serve`, refuses images it would silently drop, and treats a truncated answer as a failed attempt.

**Architecture:** No new adapter and no new dialect. `laya` is one more id the facade maps to the generic `SystemOneAdapter` (beside `llamacpp`/`sglang`). Two behaviours are added to the shared core in `crates/cloud-providers/src/systemone.rs`: a third `ImageEncoding` (`Unsupported`, chosen by adapter id) and an optional `usage.truncated` read on every response.

**Tech Stack:** Rust, `reqwest`, `serde`, `wiremock` (tests), `laya-coreml` 0.5.1 `[serve]` for the live tests.

**Spec:** `docs/superpowers/specs/2026-10-20-sp-dec-3-laya-local-system-one-design.md` (D1–D4, AC1–AC7).

---

## Ground rules

- Branch: `git switch -c sp-dec-3-laya` off `develop`. One worktree = one `CARGO_TARGET_DIR`.
- Red first, every task. A red run must show `panicked at` (or the named compile error the step expects) in the log — exit 101 alone is not proof (a compile error also exits 101).
- Real exit codes: never pipe `cargo test` into `tail` when judging pass/fail. Use `cargo test … > /tmp/sp-dec-3.log 2>&1; echo "exit $?"` and then `rg -n "panicked at|test result" /tmp/sp-dec-3.log`.
- `cargo fmt --all` before every commit. The pre-commit hook runs fmt-check and workspace clippy, but **no tests**, so run the tests yourself.
- Package names: `sensei-cloud-providers`, `sensei-gateway`.
- `git diff --cached --stat` before every commit: one concern per commit.

## Captured Laya bodies (live, laya-coreml 0.5.1, `english` checkpoint, 2026-10-20)

These are the fixtures. They are copied verbatim into the tests below.

```text
200 ok        {"model":"laya-rl-agent","answers":{"label":{"type":"choice","choice":"billing","probabilities":{"billing":0.868,"bug":0.132},"confidence":0.437,"answer_confidence":0.868,"action":{"act_probability":1.0},"x_jev_confidence":0.736},"refund":{"type":"noul","noul":0.9038,"confidence":0.9038,"answer_confidence":0.9038,"action":{"act_probability":1.0}},"urgency":{"type":"score","score":1.9366,"legend":{"0":"Routine","1":"Soon","2":"Immediate"},"probabilities":{"0":0.0092,"1":0.045,"2":0.9458},"confidence":0.7857,"answer_confidence":0.9458,"action":{"act_probability":1.0},"x_jev_confidence":0.9049}},"usage":{"input_tokens":98,"output_tokens":0,"state_tokens":8,"state_tokens_dropped":0,"truncated":false,"truncated_questions":[]},"routing":{"model":"english","repo":"convaiinnovations/laya","reason":"explicit model='english'","detection":null,"workflow":null}}
200 truncated {"model":"laya-rl-agent","answers":{"q":{"type":"noul","noul":0.6873,"confidence":0.6873,"answer_confidence":0.6873,"action":{"act_probability":1.0}}},"usage":{"input_tokens":512,"output_tokens":0,"state_tokens":1200,"state_tokens_dropped":718,"truncated":true,"truncated_questions":["q"]},"routing":{"model":"english","repo":"convaiinnovations/laya","reason":"explicit model='english'","detection":null,"workflow":null}}
401           {"detail":"invalid or missing bearer token"}
400           {"detail":"'state' is required"}
413           {"detail":"too many questions (70 > 64)"}
```

A body with `"images":[…]` and `"keep_alive":"5m"` is answered 200, with both keys ignored.

## File map

| File | Change |
|---|---|
| `crates/cloud-providers/src/systemone.rs` | `ImageEncoding::Unsupported`; `image_encoding(id)`; `WireUsage.{truncated, state_tokens_dropped, truncated_questions}`; the truncation check in `decide`; unit tests |
| `crates/cloud-providers/tests/systemone_mock.rs` | Laya section: AC2, AC3, AC4 (adapter level), AC5, and the ignored live test |
| `crates/gateway/src/facade.rs` | `"laya"` joins the `llamacpp \| sglang` arm and the doc comment; a facade test (AC1) |
| `crates/gateway/tests/decision_laya.rs` (new) | AC4 through the engine: truncated `laya` → falls back to the next router |
| `crates/gateway/tests/live_decision_self_hosted.rs` | ignored live `laya` chain test (AC6) |
| `crates/gateway/tests/docs_sync_capabilities.rs` | `"laya"` in `row_names("systemone")` |
| `docs/features/README.md`, `docs/features/inference/capabilities-and-adapters.md`, `docs/features/inference/providers.md`, `docs/llms/local.md`, `docs/llms/recipes.md`, `docs/llms/upgrading.md`, `docs/skills/using-gateway/SKILL.md` | the `laya` route (AC7) |
| `docs/superpowers/specs/2026-10-20-sp-dec-3-laya-local-system-one-design.md` | AC6 wording (Task 6) |

The site copies (`site/src/lib/content/docs/`, `site/static/skills/`) are **gitignored and generated** by `site/scripts/copy-docs.mjs`. Regenerate them with `bun run check` in `site/` (Task 7); never edit them by hand.

---

### Task 1: Refuse images for `laya` before any request (D2, AC2)

**Files:**
- Modify: `crates/cloud-providers/src/systemone.rs` (the `ImageEncoding` enum, `encode_images`, `SystemOneAdapter::decide`, `mod tests`)
- Test: `crates/cloud-providers/tests/systemone_mock.rs`

- [ ] **Step 1: Write the failing unit tests** (append inside `mod tests` in `systemone.rs`)

```rust
    /// SP-DEC-3 D2: a host that ignores `images` must never receive them —
    /// the answers would silently describe a text-only request.
    #[test]
    fn an_unsupported_host_refuses_images_and_passes_text_only_calls() {
        assert_eq!(encode_images(&[], ImageEncoding::Unsupported), Ok(vec![]));
        let err = encode_images(
            &["iVBORw0KGgo=".to_string(), "iVBORw0KGgo=".to_string()],
            ImageEncoding::Unsupported,
        )
        .unwrap_err();
        assert!(err.contains("does not read images"), "{err}");
        assert!(err.contains('2'), "says how many were refused: {err}");
    }

    #[test]
    fn laya_is_the_only_generic_id_that_refuses_images() {
        assert_eq!(image_encoding("laya"), ImageEncoding::Unsupported);
        for id in ["openrouter", "typesafe", "llamacpp", "sglang", "custom"] {
            assert_eq!(image_encoding(id), ImageEncoding::DataUrl, "{id}");
        }
    }
```

- [ ] **Step 2: Write the failing mock test** (append to `systemone_mock.rs`, after the SP-DEC-2 section)

```rust
// ---------------------------------------------------------------------------
// SP-DEC-3 — Laya (`laya-coreml-serve`) through the generic adapter
// ---------------------------------------------------------------------------

/// Laya ignores `images` (verified live: 200, text-only answers), so a `laya`
/// call with images fails BEFORE sending — a ProviderError the chain falls
/// back on — while `llamacpp` keeps sending data URLs (AC2).
#[tokio::test]
async fn laya_refuses_images_before_any_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body("laya-rl-agent")))
        .expect(0)
        .mount(&server)
        .await;
    let err = SystemOneAdapter::with_id("laya")
        .unwrap()
        .decide(
            &keyless(&server.uri()),
            &request(Some("english"), vec![PNG_B64.to_string()], None),
        )
        .await
        .unwrap_err();
    match err {
        GatewayError::ProviderError {
            adapter,
            message,
            status: None,
        } => {
            assert_eq!(adapter, "laya");
            assert!(message.contains("does not read images"), "{message}");
        }
        other => panic!("expected ProviderError before sending, got {other:?}"),
    }
}
```

`the_generic_adapter_sends_images_as_data_urls_with_the_sniffed_mime` (existing, on `llamacpp`) is the AC2 control. Leave it unchanged.

- [ ] **Step 3: Run the tests and confirm they fail**

```bash
cargo test -p sensei-cloud-providers --lib systemone::tests > /tmp/sp-dec-3.log 2>&1; echo "exit $?"
rg -n "error\[|panicked at|test result" /tmp/sp-dec-3.log
```

Expected: a compile error, `no variant or associated item named 'Unsupported'` and `cannot find function 'image_encoding'`.

- [ ] **Step 4: Implement**

In `systemone.rs`, extend the enum:

```rust
/// How a host wants `DecisionRequest.images` (bare base64 from the caller).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImageEncoding {
    /// Sent verbatim. Ollama REQUIRES this ("URLs and data URLs are not
    /// supported").
    Bare,
    /// `data:<sniffed mime>;base64,<b64>` — what llama.cpp (bare → 400),
    /// Cloudflare, vLLM's example and SGLang all accept.
    DataUrl,
    /// The host does not read images and IGNORES the field (Laya: 200 with
    /// text-only answers), so any image is refused before sending (SP-DEC-3 D2).
    Unsupported,
}
```

At the top of `encode_images`, before the `Bare` check:

```rust
    if encoding == ImageEncoding::Unsupported {
        return match images.len() {
            0 => Ok(vec![]),
            n => Err(format!(
                "this host does not read images ({n} sent) — it would answer as if they were \
                 absent; route image decisions to a vision-capable System One host"
            )),
        };
    }
```

Below `encode_images`, add:

```rust
/// The image encoding the generic adapter uses for a router id: `laya`
/// ignores images (SP-DEC-3 D2); every other `/v1/systemone` host that takes
/// images accepts data URLs (SP-DEC-2 D1).
fn image_encoding(id: &str) -> ImageEncoding {
    match id {
        "laya" => ImageEncoding::Unsupported,
        _ => ImageEncoding::DataUrl,
    }
}
```

In `SystemOneAdapter::decide`, replace `images: ImageEncoding::DataUrl,` with:

```rust
            images: image_encoding(&self.id),
```

- [ ] **Step 5: Run the tests and confirm they pass**

```bash
cargo test -p sensei-cloud-providers --lib systemone::tests > /tmp/sp-dec-3.log 2>&1; echo "exit $?"
cargo test -p sensei-cloud-providers --test systemone_mock > /tmp/sp-dec-3-mock.log 2>&1; echo "exit $?"
rg -n "panicked at|test result" /tmp/sp-dec-3.log /tmp/sp-dec-3-mock.log
```

Expected: both runs exit 0. `laya_refuses_images_before_any_request` and the existing data-URL test pass.

- [ ] **Step 6: Mutation-check the guard.** Temporarily change the `"laya"` arm to `"layaX"`, rerun the mock test, and confirm `laya_refuses_images_before_any_request` shows `panicked at`. Restore the arm and rerun to green. **Commit only after the restore is green**: a `git checkout` of the file would also revert the uncommitted fix.

- [ ] **Step 7: Commit**

```bash
cargo fmt --all
git add crates/cloud-providers/src/systemone.rs crates/cloud-providers/tests/systemone_mock.rs
git diff --cached --stat
git commit -m "feat(cloud-providers): laya refuses decision images before sending (SP-DEC-3 T1, D2)"
```

### Task 2: A truncated answer is a failed attempt (D3, AC3, AC4 adapter level)

**Files:**
- Modify: `crates/cloud-providers/src/systemone.rs` (`WireUsage`, `decide`)
- Test: `crates/cloud-providers/tests/systemone_mock.rs`

- [ ] **Step 1: Write the failing mock tests** (append to the SP-DEC-3 section)

```rust
/// Laya's real default (non-strict) payload, captured live.
fn laya_ok_body() -> serde_json::Value {
    json!({"model":"laya-rl-agent","answers":{"label":{"type":"choice","choice":"billing","probabilities":{"billing":0.868,"bug":0.132},"confidence":0.437,"answer_confidence":0.868,"action":{"act_probability":1.0},"x_jev_confidence":0.736},"refund":{"type":"noul","noul":0.9038,"confidence":0.9038,"answer_confidence":0.9038,"action":{"act_probability":1.0}},"urgency":{"type":"score","score":1.9366,"legend":{"0":"Routine","1":"Soon","2":"Immediate"},"probabilities":{"0":0.0092,"1":0.045,"2":0.9458},"confidence":0.7857,"answer_confidence":0.9458,"action":{"act_probability":1.0},"x_jev_confidence":0.9049}},"usage":{"input_tokens":98,"output_tokens":0,"state_tokens":8,"state_tokens_dropped":0,"truncated":false,"truncated_questions":[]},"routing":{"model":"english","repo":"convaiinnovations/laya","reason":"explicit model='english'","detection":null,"workflow":null}})
}

fn laya_questions() -> DecisionQuestions {
    let mut q = questions();
    q.insert(
        "refund".into(),
        DecisionQuestion::Noul {
            instructions: json!("Refund requested?"),
            criteria: None,
        },
    );
    q.insert(
        "urgency".into(),
        DecisionQuestion::Score {
            instructions: json!("How urgent?"),
            criteria: vec!["Routine".into(), "Soon".into(), "Immediate".into()],
        },
    );
    q
}

/// AC3: every Laya extension (`routing`, `x_jev_confidence`, `answer_confidence`,
/// `action`, noul `confidence`, extended `usage`) decodes into the plain answers.
#[tokio::test]
async fn laya_default_payload_decodes_into_typed_answers() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .and(body_partial_json(json!({"model": "english"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(laya_ok_body()))
        .expect(1)
        .mount(&server)
        .await;
    let mut req = request(Some("english"), vec![], None);
    req.questions = laya_questions();
    let resp = SystemOneAdapter::with_id("laya")
        .unwrap()
        .decide(&keyless(&server.uri()), &req)
        .await
        .expect("laya's default payload is a success");

    match &resp.answers["label"] {
        DecisionAnswer::Choice {
            choice,
            probabilities,
            confidence,
        } => {
            assert_eq!(choice, "billing");
            assert_eq!(probabilities["bug"], 0.132);
            assert_eq!(*confidence, 0.437, "laya's own confidence, not x_jev_confidence");
        }
        other => panic!("choice expected, got {other:?}"),
    }
    assert_eq!(resp.answers["refund"], DecisionAnswer::Noul { noul: 0.9038 });
    match &resp.answers["urgency"] {
        DecisionAnswer::Score { score, legend, .. } => {
            assert_eq!(*score, 1.9366);
            assert_eq!(legend["2"], "Immediate");
        }
        other => panic!("score expected, got {other:?}"),
    }
    assert_eq!(resp.model.as_deref(), Some("laya-rl-agent"));
    let usage = resp.usage.expect("usage");
    assert_eq!((usage.input_tokens, usage.output_tokens), (98, 0));
}

/// AC4 (adapter level): Laya answers an over-window state with 200 and
/// `usage.truncated: true` — answers about a state it never fully read. That is
/// a failed attempt (ProviderError, so the chain falls back), never a success.
#[tokio::test]
async fn a_truncated_laya_answer_is_a_provider_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"model":"laya-rl-agent","answers":{"label":{"type":"choice","choice":"bug","probabilities":{"billing":0.4,"bug":0.6},"confidence":0.03}},"usage":{"input_tokens":512,"output_tokens":0,"state_tokens":1200,"state_tokens_dropped":718,"truncated":true,"truncated_questions":["label"]},"routing":{"model":"english"}})))
        .mount(&server)
        .await;
    let err = SystemOneAdapter::with_id("laya")
        .unwrap()
        .decide(
            &keyless(&server.uri()),
            &request(Some("english"), vec![], None),
        )
        .await
        .unwrap_err();
    match err {
        GatewayError::ProviderError {
            adapter,
            message,
            status: Some(200),
        } => {
            assert_eq!(adapter, "laya");
            assert!(message.contains("truncated"), "{message}");
            assert!(message.contains("718"), "names the dropped tokens: {message}");
            assert!(message.contains("label"), "names the cut question: {message}");
        }
        other => panic!("expected ProviderError, got {other:?}"),
    }
}

/// D3 is additive: `truncated: false`, and a usage block with no such key
/// (every other host), stay successes.
#[tokio::test]
async fn untruncated_and_truncation_unaware_answers_stay_successes() {
    let mut not_truncated = ok_body("laya-rl-agent");
    not_truncated["usage"]["truncated"] = json!(false);
    for body in [not_truncated, ok_body("nimble-v3")] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;
        SystemOneAdapter::with_id("laya")
            .unwrap()
            .decide(
                &keyless(&server.uri()),
                &request(Some("english"), vec![], None),
            )
            .await
            .expect("not truncated → success");
    }
}
```

- [ ] **Step 2: Run them and confirm the truncation test fails**

```bash
cargo test -p sensei-cloud-providers --test systemone_mock laya_ > /tmp/sp-dec-3.log 2>&1; echo "exit $?"
cargo test -p sensei-cloud-providers --test systemone_mock truncat > /tmp/sp-dec-3b.log 2>&1; echo "exit $?"
rg -n "panicked at|test result" /tmp/sp-dec-3.log /tmp/sp-dec-3b.log
```

Expected: `a_truncated_laya_answer_is_a_provider_error` shows `panicked at` with `called Result::unwrap_err() on an Ok value`. `laya_default_payload_decodes_into_typed_answers` and `untruncated_and_truncation_unaware_answers_stay_successes` already pass: they pin behaviour the existing decoder has, and they guard D3 against over-reach.

- [ ] **Step 3: Implement.** In `systemone.rs`, replace `WireUsage` with:

```rust
#[derive(Deserialize)]
struct WireUsage {
    #[serde(default)]
    input_tokens: u32,
    #[serde(default)]
    output_tokens: u32,
    /// Laya's report that it cut the input to fit its window (SP-DEC-3 D3).
    /// Absent on every other host, so it defaults to "not truncated".
    #[serde(default)]
    truncated: bool,
    #[serde(default)]
    state_tokens_dropped: u32,
    #[serde(default)]
    truncated_questions: Vec<String>,
}
```

In `decide`, directly after the `missing` check's `if` block and before `Ok(Ok(DecisionResponse {`, add:

```rust
    // SP-DEC-3 D3: a host that truncated the input still answers 200 — about a
    // state it never fully read. Report a failed attempt so the chain falls
    // back, as for the missing-answer case above.
    if let Some(u) = wire.usage.as_ref().filter(|u| u.truncated) {
        let mut cut = Vec::new();
        if u.state_tokens_dropped > 0 {
            cut.push(format!("{} state tokens dropped", u.state_tokens_dropped));
        }
        if !u.truncated_questions.is_empty() {
            cut.push(format!("questions cut: {:?}", u.truncated_questions));
        }
        return Err(GatewayError::ProviderError {
            adapter: adapter.into(),
            message: format!(
                "the host truncated the input to fit its window ({}); its answers do not cover \
                 the whole request — set the model's context_window to the checkpoint's window",
                if cut.is_empty() { "no detail".to_string() } else { cut.join("; ") }
            ),
            status: Some(status.as_u16()),
        });
    }
```

- [ ] **Step 4: Run the tests and confirm they pass**

```bash
cargo test -p sensei-cloud-providers > /tmp/sp-dec-3.log 2>&1; echo "exit $?"
rg -n "panicked at|test result" /tmp/sp-dec-3.log
```

Expected: exit 0, and every `test result` line shows `0 failed`.

- [ ] **Step 5: Mutation-check D3.** Change `.filter(|u| u.truncated)` to `.filter(|u| !u.truncated)`, rerun `--test systemone_mock`, and confirm that both `a_truncated_laya_answer_is_a_provider_error` and `untruncated_and_truncation_unaware_answers_stay_successes` show `panicked at`. Restore and rerun to green.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add crates/cloud-providers/src/systemone.rs crates/cloud-providers/tests/systemone_mock.rs
git diff --cached --stat
git commit -m "feat(cloud-providers): a truncated System One answer is a failed attempt (SP-DEC-3 T2, D3)"
```

### Task 3: Pin Laya's error mapping (AC5)

**Files:**
- Test: `crates/cloud-providers/tests/systemone_mock.rs`

This is a pinning test: `extract_error_message` already reads a FastAPI `{"detail": "<string>"}`, so the test is expected to pass on its first run. Its proof is the mutation in Step 3, not a red run.

- [ ] **Step 1: Write the test** (append to the SP-DEC-3 section)

```rust
/// AC5: Laya's FastAPI errors, verbatim from the live server — 401 is an
/// Authentication error; every other status is a ProviderError carrying the
/// server's `detail` string and the status.
#[tokio::test]
async fn laya_errors_surface_the_servers_detail() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(
            ResponseTemplate::new(401)
                .set_body_json(json!({"detail":"invalid or missing bearer token"})),
        )
        .mount(&server)
        .await;
    let adapter = SystemOneAdapter::with_id("laya").unwrap();
    let req = request(Some("english"), vec![], None);
    assert_authentication_error(
        &adapter
            .decide(&keyless(&server.uri()), &req)
            .await
            .unwrap_err(),
    );

    for (status, detail) in [
        (400, "'state' is required"),
        (413, "too many questions (70 > 64)"),
        (422, "max_len must be a positive integer"),
        (500, "inference failed"),
        (503, "server busy"),
    ] {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .respond_with(ResponseTemplate::new(status).set_body_json(json!({"detail": detail})))
            .mount(&server)
            .await;
        match adapter
            .decide(&keyless(&server.uri()), &req)
            .await
            .unwrap_err()
        {
            GatewayError::ProviderError {
                status: Some(s),
                message,
                adapter,
            } => assert_eq!((s, message.as_str(), adapter.as_str()), (status, detail, "laya")),
            other => panic!("{status}: expected ProviderError, got {other:?}"),
        }
    }
}
```

The 422, 500 and 503 `detail` strings are representative of Laya's handlers (`serve.py` raises `HTTPException(status_code=…, detail=…)` for each). Only the 401/400/413 bodies were captured live.

- [ ] **Step 2: Run it**

```bash
cargo test -p sensei-cloud-providers --test systemone_mock laya_errors > /tmp/sp-dec-3.log 2>&1; echo "exit $?"
rg -n "panicked at|test result" /tmp/sp-dec-3.log
```

Expected: PASS (1 passed).

- [ ] **Step 3: Mutation-check it.** In `crates/cloud-providers/src/base.rs::extract_error_message`, comment out the `// FastAPI: { "detail": "..." }` `.or_else` block. Rerun and confirm `panicked at` (the message becomes the raw JSON body). Restore it and rerun to green.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/cloud-providers/tests/systemone_mock.rs
git diff --cached --stat
git commit -m "test(cloud-providers): laya's FastAPI error bodies map to Authentication / ProviderError (SP-DEC-3 T3)"
```

### Task 4: The facade auto-registers `laya`, decision-only (D1, AC1)

**Files:**
- Modify: `crates/gateway/src/facade.rs` (doc comment ~line 162, the `llamacpp | sglang` arm ~line 269, and `mod tests`)

- [ ] **Step 1: Write the failing facade test** (in `facade.rs` `mod cloud_tests` — gated on the default `cloud` feature; `mod tests` is `local`-only — after `build_registers_the_sp_dec_2_decision_routers_decision_only`)

```rust
    /// SP-DEC-3 D1: `laya` comes up decision-only as a `SystemOneAdapter` —
    /// proven by calling it (plain `/v1/systemone`) and by its D2 behaviour,
    /// which only the `laya` id has: an image is refused before sending.
    #[tokio::test]
    async fn build_registers_laya_decision_only() {
        use crate::types::decision::{DecisionQuestion, DecisionQuestions};
        use crate::types::io::DecisionRequest;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/systemone"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "model": "laya-rl-agent",
                "answers": {"q": {"type": "noul", "noul": 0.5}},
                "usage": {"input_tokens": 1, "output_tokens": 0}
            })))
            .expect(1)
            .mount(&server)
            .await;
        let laya = RouterConfig {
            url: server.uri(),
            ..router()
        };
        let builder = FacadeBuilder::new(GatewayConfig {
            routers: HashMap::from([("laya".to_string(), laya.clone())]),
            ..Default::default()
        });
        let registry = builder.registry().clone();
        builder.build().await;

        let req = |images: Vec<String>| DecisionRequest {
            model: Some("english".into()),
            state: serde_json::json!("x"),
            questions: DecisionQuestions::from([(
                "q".to_string(),
                DecisionQuestion::Noul {
                    instructions: serde_json::json!("y?"),
                    criteria: None,
                },
            )]),
            images,
            keep_alive: None,
        };
        let adapter = registry.decision("laya").await.expect("laya decides");
        adapter
            .decide(&laya, &req(vec![]))
            .await
            .expect("laya speaks plain /v1/systemone");
        assert!(matches!(
            adapter
                .decide(&laya, &req(vec!["iVBORw0KGgo=".into()]))
                .await,
            Err(GatewayError::ProviderError { status: None, .. })
        ));
        assert!(registry.chat("laya").await.is_none(), "decision-only");
    }
```

`GatewayError` is imported at the top of `facade.rs` (`use crate::types::error::GatewayError;`); the tests module reaches it through `use super::*`.

- [ ] **Step 2: Run it and confirm it fails**

```bash
cargo test -p sensei-gateway --lib facade::cloud_tests::build_registers_laya > /tmp/sp-dec-3.log 2>&1; echo "exit $?"
rg -n "panicked at|test result" /tmp/sp-dec-3.log
```

Expected: `panicked at` … `laya decides`.

- [ ] **Step 3: Implement.** In `facade.rs`, change the arm:

```rust
            id @ ("llamacpp" | "sglang" | "laya") => Some(
```

Update the comment directly above the `"cloudflare"` arm:

```rust
            // SP-DEC-2: Cloudflare Workers AI (clef / clef-flash) and the
            // self-hosted System One servers — decision-only; a chat adapter the
            // caller registers under the same id lives in its own map, untouched.
            // SP-DEC-3: `laya` (laya-coreml-serve) is one more self-hosted id.
```

In the `register_cloud_from_config` doc comment, replace `` `typesafe`, `cloudflare`, `llamacpp` and `sglang` are `` with `` `typesafe`, `cloudflare`, `llamacpp`, `sglang` and `laya` are ``.

- [ ] **Step 4: Run it and confirm it passes**

```bash
cargo test -p sensei-gateway --lib facade::cloud_tests > /tmp/sp-dec-3.log 2>&1; echo "exit $?"
rg -n "panicked at|test result" /tmp/sp-dec-3.log
```

Expected: exit 0, `0 failed`.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add crates/gateway/src/facade.rs
git diff --cached --stat
git commit -m "feat(gateway): facade auto-registers laya decision-only (SP-DEC-3 T4, D1)"
```

### Task 5: A truncated `laya` answer falls back through the engine (AC4 end to end)

**Files:**
- Create: `crates/gateway/tests/decision_laya.rs`

This composes Task 2 with Task 4 and the engine's existing fallback, so it is expected to pass once Tasks 2 and 4 are in. The proof is Step 3's mutation.

- [ ] **Step 1: Write the test**

```rust
//! SP-DEC-3 AC4 — a `laya` answer the server truncated is a failed attempt the
//! engine falls back on: the facade's `laya` router (mocked with Laya's real
//! truncated payload) fails, the next router in the `decide` chain answers.

#![cfg(feature = "cloud")]

use std::collections::HashMap;

use gateway::types::config::{
    ChainEntry, FallbackChainConfig, FallbackTrigger, GatewayConfig, ModelConfig, RouterConfig,
};
use gateway::types::decision::{DecisionQuestion, DecisionQuestions};
use gateway::types::request::Payload;
use gateway::{Capability, FacadeBuilder, InferenceRequest};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn router(url: String) -> RouterConfig {
    RouterConfig {
        url,
        api_key_env: None,
        api_key: None,
        enabled: true,
        timeout_ms: Some(5_000),
        headers: HashMap::new(),
    }
}

fn model(id: &str, provider: &str, window: u32) -> ModelConfig {
    ModelConfig {
        id: id.into(),
        api_model_id: Some(id.into()),
        provider: provider.into(),
        family: None,
        capabilities: vec![Capability::Decision],
        context_window: window,
        max_output_tokens: 1,
        pricing: None,
        catalog: None,
    }
}

fn entry(model: &str, router: &str, priority: u8) -> ChainEntry {
    ChainEntry {
        model: model.into(),
        router: Some(router.into()),
        api_model_id: None,
        priority,
    }
}

#[tokio::test]
async fn a_truncated_laya_answer_falls_back_to_the_next_router() {
    let laya = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"model":"laya-rl-agent","answers":{"q":{"type":"noul","noul":0.6873,"confidence":0.6873,"answer_confidence":0.6873,"action":{"act_probability":1.0}}},"usage":{"input_tokens":512,"output_tokens":0,"state_tokens":1200,"state_tokens_dropped":718,"truncated":true,"truncated_questions":["q"]},"routing":{"model":"english"}})))
        .expect(1)
        .mount(&laya)
        .await;
    let next = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/systemone"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "model": "nimble-v3",
            "answers": {"q": {"type": "noul", "noul": 0.91}},
            "usage": {"input_tokens": 300, "output_tokens": 0}
        })))
        .expect(1)
        .mount(&next)
        .await;

    let config = GatewayConfig {
        routers: HashMap::from([
            ("laya".into(), router(laya.uri())),
            ("llamacpp".into(), router(next.uri())),
        ]),
        models: HashMap::from([
            // Window deliberately overstated so the ContextWindowGate lets the
            // request through and D3 is what catches the truncation.
            ("english".into(), model("english", "laya", 8_192)),
            ("nimble-v3".into(), model("nimble-v3", "llamacpp", 8_192)),
        ]),
        chains: HashMap::from([(
            "decide".into(),
            FallbackChainConfig {
                id: "decide".into(),
                capability: Capability::Decision,
                models: vec![entry("english", "laya", 1), entry("nimble-v3", "llamacpp", 2)],
                fallback_triggers: vec![FallbackTrigger::ProviderError],
            },
        )]),
        ..Default::default()
    };
    let gw = FacadeBuilder::new(config).build().await.gateway;

    let req = InferenceRequest {
        capability: Capability::Decision,
        model: None,
        router: None,
        chain: Some("decide".into()),
        payload: Payload::Decision {
            state: json!("The customer wrote again about the duplicate invoice charge."),
            questions: DecisionQuestions::from([(
                "q".to_string(),
                DecisionQuestion::Noul {
                    instructions: json!("Refund requested?"),
                    criteria: None,
                },
            )]),
            images: vec![],
            keep_alive: None,
        },
        budget: None,
        auth: None,
        panel: None,
        consensus: None,
        allow_fallback: true,
        credentials: Default::default(),
        routing: None,
    };

    let resp = gw.execute(&req).await.expect("falls back to llamacpp");
    assert!(resp.success, "{resp:?}");
    let adapters: Vec<&str> = resp.attempts.iter().map(|a| a.adapter.as_str()).collect();
    assert_eq!(adapters, ["laya", "llamacpp"], "{:?}", resp.attempts);
    let answers = resp.decisions.expect("decisions");
    assert!(matches!(
        answers["q"],
        gateway::types::decision::DecisionAnswer::Noul { noul } if noul == 0.91
    ));
}
```

If `resp.attempts[i].adapter` or `resp.decisions` differ in name, take the field names from `crates/gateway/tests/live_decision_self_hosted.rs`, which reads both.

- [ ] **Step 2: Run it**

```bash
cargo test -p sensei-gateway --test decision_laya > /tmp/sp-dec-3.log 2>&1; echo "exit $?"
rg -n "panicked at|test result" /tmp/sp-dec-3.log
```

Expected: PASS.

- [ ] **Step 3: Mutation-check it.** Apply the Task 2 mutation (`.filter(|u| !u.truncated)`), rerun, and confirm `panicked at`: the attempts become `["laya"]` and the answer is 0.6873. Restore and rerun to green.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add crates/gateway/tests/decision_laya.rs
git diff --cached --stat
git commit -m "test(gateway): a truncated laya answer falls back through the engine (SP-DEC-3 T5, AC4)"
```

### Task 6: Live tests against a real `laya-coreml-serve` (AC6)

**Files:**
- Modify: `crates/cloud-providers/tests/systemone_mock.rs` (ignored live test)
- Modify: `crates/gateway/tests/live_decision_self_hosted.rs` (ignored live chain test)
- Modify: the spec's AC6 wording

- [ ] **Step 1: Start the server** (scratch venv from the brainstorm; recreate it if `/tmp/laya-bench` is gone)

```bash
test -x /tmp/laya-bench/env/.venv/bin/laya-coreml-serve || { mkdir -p /tmp/laya-bench/env /tmp/laya-bench/scripts && uv venv -q -p 3.12 /tmp/laya-bench/env/.venv && VIRTUAL_ENV=/tmp/laya-bench/env/.venv uv pip install -q "laya-coreml[serve]==0.5.1"; }
cd /tmp/laya-bench/scripts && LAYA_HOST=127.0.0.1 LAYA_PORT=8765 LAYA_MODELS=english,multilingual nohup ../env/.venv/bin/laya-coreml-serve > /tmp/laya-bench/server.log 2>&1 &
for i in $(seq 1 40); do curl -s 127.0.0.1:8765/health | rg -q '"status":"ok"' && break; sleep 3; done; curl -s 127.0.0.1:8765/health | head -c 200
```

- [ ] **Step 2: Add the adapter-level live test** (append to the SP-DEC-3 section of `systemone_mock.rs`)

```rust
/// Live, opt-in: a real `laya-coreml-serve` (laya-coreml 0.5.1, verified on an
/// M4 Max, CoreML CPU+GPU):
///   pip install "laya-coreml[serve]"
///   LAYA_HOST=127.0.0.1 LAYA_PORT=8765 LAYA_MODELS=english laya-coreml-serve
///   LAYA_URL=http://127.0.0.1:8765 cargo test -p sensei-cloud-providers \
///     --test systemone_mock live_laya -- --ignored
/// `LAYA_MODEL` picks the checkpoint (default `english`, a 512-token window).
#[tokio::test]
#[ignore = "requires LAYA_URL: a running laya-coreml-serve"]
async fn live_laya_answers_every_type_and_reports_truncation() {
    let url = std::env::var("LAYA_URL").expect("LAYA_URL");
    let model = std::env::var("LAYA_MODEL").unwrap_or_else(|_| "english".into());
    let mut cfg = keyless(&url);
    cfg.timeout_ms = Some(120_000);
    let adapter = SystemOneAdapter::with_id("laya").unwrap();

    let mut req = request(Some(&model), vec![], None);
    req.questions = laya_questions();
    let resp = adapter.decide(&cfg, &req).await.expect("live laya decision");
    let DecisionAnswer::Choice {
        choice,
        probabilities,
        ..
    } = &resp.answers["label"]
    else {
        panic!("choice answer expected: {:?}", resp.answers);
    };
    // The contract, not the model's opinion.
    assert_eq!(probabilities.keys().collect::<Vec<_>>(), ["billing", "bug"]);
    let argmax = probabilities
        .iter()
        .max_by(|a, b| a.1.total_cmp(b.1))
        .map(|(k, _)| k.as_str());
    assert_eq!(Some(choice.as_str()), argmax, "{probabilities:?}");
    assert!((probabilities.values().sum::<f64>() - 1.0).abs() < 1e-3);
    assert!(matches!(resp.answers["refund"], DecisionAnswer::Noul { noul } if (0.0..=1.0).contains(&noul)));
    assert!(matches!(resp.answers["urgency"], DecisionAnswer::Score { score, .. } if (0.0..=2.0).contains(&score)));
    assert!(resp.usage.is_some_and(|u| u.input_tokens > 0 && u.output_tokens == 0));

    // D3 against the real server: ~1200 tokens into a 512-token window.
    let mut long = request(Some(&model), vec![], None);
    long.state = json!(
        ["The customer wrote again about the duplicate invoice charge."; 120].join(" ")
    );
    match adapter.decide(&cfg, &long).await.unwrap_err() {
        GatewayError::ProviderError { message, .. } => {
            assert!(message.contains("truncated"), "{message}")
        }
        other => panic!("expected the D3 truncation error, got {other:?}"),
    }
}
```

- [ ] **Step 3: Add the engine-level live test** (append to `live_decision_self_hosted.rs`, reusing its `router` and `model` helpers)

```rust
/// SP-DEC-3 — the facade's `laya` router, a `decide` chain, and a real
/// `laya-coreml-serve` (see `live_laya_answers_every_type_and_reports_truncation`
/// in cloud-providers for the server command):
///   LAYA_URL=http://127.0.0.1:8765 cargo test -p sensei-gateway \
///     --test live_decision_self_hosted laya -- --ignored
#[tokio::test]
#[ignore = "requires LAYA_URL: a running laya-coreml-serve"]
async fn a_decide_chain_routes_to_a_live_laya_server() {
    let url = std::env::var("LAYA_URL").expect("LAYA_URL");
    let config = GatewayConfig {
        routers: HashMap::from([
            ("laya".into(), router(&url)),
            ("openrouter".into(), router("http://127.0.0.1:9")),
        ]),
        models: HashMap::from([
            ("english".into(), model("english", "laya", "english", 512)),
            ("jev".into(), model("jev", "openrouter", "typesafe/jev-1.13", 32_000)),
        ]),
        chains: HashMap::from([(
            "decide".into(),
            FallbackChainConfig {
                id: "decide".into(),
                capability: Capability::Decision,
                models: vec![
                    ChainEntry {
                        model: "english".into(),
                        router: Some("laya".into()),
                        api_model_id: None,
                        priority: 1,
                    },
                    ChainEntry {
                        model: "jev".into(),
                        router: Some("openrouter".into()),
                        api_model_id: None,
                        priority: 2,
                    },
                ],
                fallback_triggers: vec![FallbackTrigger::ProviderError],
            },
        )]),
        ..Default::default()
    };
    let gw = FacadeBuilder::new(config).build().await.gateway;
    let req = InferenceRequest {
        capability: Capability::Decision,
        model: None,
        router: None,
        chain: Some("decide".into()),
        payload: Payload::Decision {
            state: json!({"ticket": "I was charged twice. Please refund the extra payment."}),
            questions: DecisionQuestions::from([
                (
                    "refund".to_string(),
                    DecisionQuestion::Noul {
                        instructions: json!("Is the customer requesting a refund?"),
                        criteria: None,
                    },
                ),
                (
                    "urgency".to_string(),
                    DecisionQuestion::Score {
                        instructions: json!("How urgently does this ticket need a response?"),
                        criteria: vec!["Routine".into(), "Soon".into(), "Immediate".into()],
                    },
                ),
            ]),
            images: vec![],
            keep_alive: None,
        },
        budget: None,
        auth: None,
        panel: None,
        consensus: None,
        allow_fallback: true,
        credentials: Default::default(),
        routing: None,
    };
    let resp = gw.execute(&req).await.expect("the decide chain answers");
    assert!(resp.success, "{resp:?}");
    assert_eq!(resp.attempts.len(), 1, "served by laya: {:?}", resp.attempts);
    assert_eq!(resp.attempts[0].adapter, "laya");
    let answers = resp.decisions.expect("decisions");
    assert!(matches!(answers["refund"], DecisionAnswer::Noul { .. }));
    assert!(matches!(answers["urgency"], DecisionAnswer::Score { .. }));
}
```

- [ ] **Step 4: Run both live tests against the server**

```bash
LAYA_URL=http://127.0.0.1:8765 cargo test -p sensei-cloud-providers --test systemone_mock live_laya -- --ignored > /tmp/sp-dec-3-live.log 2>&1; echo "exit $?"
LAYA_URL=http://127.0.0.1:8765 cargo test -p sensei-gateway --test live_decision_self_hosted laya -- --ignored > /tmp/sp-dec-3-live2.log 2>&1; echo "exit $?"
rg -n "panicked at|test result" /tmp/sp-dec-3-live.log /tmp/sp-dec-3-live2.log
```

Expected: both exit 0 with `1 passed`. Also run with `LAYA_MODEL=multilingual` for the first test, but expect its truncation half to need a longer state, since multilingual has a 1024-token window. If it doesn't truncate, set `LAYA_MODEL=english` and record the multilingual result in the commit message.

- [ ] **Step 5: Update the spec's AC6** to match what is tested:

```markdown
- **AC6** Ignored live tests against a real `laya-coreml-serve` (`LAYA_URL`; `LAYA_MODEL`, default `english`):
  the adapter answers a choice, a noul and a score with a well-formed distribution, and a ~1200-token state
  into the 512-token window comes back as the D3 `ProviderError`; a `decide` chain through the facade and
  engine is served by `laya` in one attempt.
```

- [ ] **Step 6: Stop the server and commit**

```bash
pkill -f laya-coreml-serve
cargo fmt --all
git add crates/cloud-providers/tests/systemone_mock.rs crates/gateway/tests/live_decision_self_hosted.rs docs/superpowers/specs/2026-10-20-sp-dec-3-laya-local-system-one-design.md
git diff --cached --stat
git commit -m "test: live laya-coreml-serve decisions, adapter + engine (SP-DEC-3 T6, AC6)"
```

### Task 7: Docs on every surface (AC7)

**Files:**
- Modify: `crates/gateway/tests/docs_sync_capabilities.rs:67`
- Modify: `docs/features/README.md`, `docs/features/inference/capabilities-and-adapters.md`, `docs/features/inference/providers.md`, `docs/llms/local.md`, `docs/llms/recipes.md`, `docs/llms/upgrading.md`, `docs/skills/using-gateway/SKILL.md`

- [ ] **Step 1: Make the docs-sync gate fail first.** In `docs_sync_capabilities.rs`:

```rust
        "systemone" => vec!["openrouter", "typesafe", "llamacpp", "sglang", "laya"],
```

```bash
cargo test -p sensei-gateway --test docs_sync_capabilities > /tmp/sp-dec-3.log 2>&1; echo "exit $?"
rg -n "panicked at|missing" /tmp/sp-dec-3.log
```

Expected: `panicked at` … `has no row for ["laya"]`, for both matrix docs.

- [ ] **Step 2: Matrix rows.** In `docs/features/README.md`, after the `| `sglang` |` row (line 72), add:

```markdown
| `laya` | | | | | | | ✓ |
```

In `docs/features/inference/capabilities-and-adapters.md`, after the `| `sglang`     |` row (line 277), add:

```markdown
| `laya`       |   |   |   |   |   |   | ✓ |
```

Rerun the Step 1 command. Expected: exit 0.

- [ ] **Step 3: Prose, surface by surface.** Every place below currently lists `llamacpp`/`sglang`; add `laya` with the facts from the spec's Contract section.
  - `docs/features/README.md:99`: add `laya` to "`cloudflare`, `llamacpp` and `sglang` are likewise auto-registered…", with: "`laya` = `SystemOneAdapter` for a local `laya-coreml-serve` (CoreML on Apple Silicon); refuses images; a truncated answer is a failed attempt."
  - `capabilities-and-adapters.md:303-307` and `:414`: add `laya` to the decision-only list and the self-hosted bullet, with: "`laya` refuses `images` before sending (the server ignores them) and turns `usage.truncated: true` into a `ProviderError`."
  - `providers.md`: add a `| `laya` |` row after the `sglang` row (line 79), in the same columns: url `RouterConfig.url` (no default, e.g. `http://127.0.0.1:8000`) + `/v1/systemone`; bearer only if a key is present (the server's `LAYA_API_KEY`); FastAPI `{"detail": "…"}` errors. Add `laya` to the lines 92–93 and 104 lists. After the SGLang paragraph (line 136), add a **Laya (`laya`)** paragraph: `pip install "laya-coreml[serve]"`, `LAYA_PORT`/`LAYA_MODELS`/`LAYA_API_KEY`; models `english` (512 tokens), `multilingual` (1024), `typed-decisions` (1024), or `LAYA_EXTRA_MODELS` (e.g. the ANE checkpoint, 96 tokens); the response `model` is always `laya-rl-agent`; `usage.output_tokens` is 0; images are refused; a truncated answer is a `ProviderError` (needs the default payload, so don't set `LAYA_JEV_STRICT`); set `context_window` to the checkpoint's window; measured HTTP overhead is about 0.35 ms per decision.
  - `docs/llms/local.md`: rename the heading `## Self-hosted decision servers (llama.cpp, SGLang)` to `## Self-hosted decision servers (llama.cpp, SGLang, Laya)`, add `laya` to "auto-registers the router ids…", and add this bullet after the SGLang bullet:

```markdown
- **Laya** (`laya`, open-weight, CoreML on Apple Silicon — no cloud key) — `pip install
  "laya-coreml[serve]"`, then `LAYA_HOST=127.0.0.1 LAYA_PORT=8000 LAYA_MODELS=english
  laya-coreml-serve`; router `url = "http://127.0.0.1:8000"`, model `api_model_id =
  "english"` (or `multilingual` / `typed-decisions`). Set the model's `context_window` to
  the checkpoint's window — `english` 512, `multilingual` / `typed-decisions` 1024 — because
  Laya **truncates silently** (200); the gateway also reads Laya's `usage.truncated` and
  turns a truncated answer into a `ProviderError` so the chain falls back (keep the default
  payload: `LAYA_JEV_STRICT` removes that field). Laya ignores images, so a `laya` call with
  images is refused before sending. The response `model` is `laya-rl-agent`;
  `usage.output_tokens` is 0. HTTP adds ≈ 0.35 ms per decision (measured on an M4 Max).
```

  - `docs/llms/recipes.md:248-251, 320`: add `laya` to both id lists. After the SGLang/vLLM paragraph (line 391–395), add:

```markdown
A local **Laya** server (`laya-coreml-serve`) is configured the same way under the
`laya` id — `url: "http://127.0.0.1:8000"`, `api_model_id: Some("english".into())`,
`context_window: 512` (1024 for `multilingual` / `typed-decisions`). It refuses images
before sending, and a truncated answer (`usage.truncated`) fails the attempt so the chain
falls back.
```

  - `docs/llms/upgrading.md`: insert above the `## 0.10.x → 0.11.0` section:

```markdown
## Unreleased (after 0.11.0) — SP-DEC-3, Laya System One

Additive; no type changes.

| Area | Change | Action |
|---|---|---|
| Facade (`FacadeBuilder::build`) | auto-registers `laya` (`SystemOneAdapter`, decision-only) for a local `laya-coreml-serve` | if you registered your own decision adapter under `laya`, it is now replaced; rename yours |
| `SystemOneAdapter` | id `laya`: a decision with `images` fails before sending (`ProviderError`, `status: None`) | route image decisions to a vision-capable host |
| `SystemOneAdapter` (every id) | a 200 whose `usage.truncated` is `true` is a `ProviderError` (status 200), not a success. Only Laya sends the field today | none; add `FallbackTrigger::ProviderError` to chains that should fall back |
```

  - `docs/skills/using-gateway/SKILL.md:118-121`: change "and self-hosted llamacpp / sglang servers" to "self-hosted llamacpp / sglang servers, and a local laya (laya-coreml-serve, CoreML; refuses images, truncated answers fail the attempt)".

- [ ] **Step 4: Regenerate the site copies and check them**

```bash
cd site && bun run check > /tmp/sp-dec-3-site.log 2>&1; echo "exit $?"; cd ..
rg -n "laya" site/src/lib/content/docs/local.md site/static/skills/using-gateway/SKILL.md | head -5
rg -n "error|Error" /tmp/sp-dec-3-site.log | head
```

Expected: exit 0, and `laya` appears in both copies. (`site/src/lib/content` and `site/static/skills` are gitignored: nothing to stage.)

- [ ] **Step 5: Run the docs tests and commit**

```bash
cargo test -p sensei-gateway --test docs_sync_capabilities > /tmp/sp-dec-3.log 2>&1; echo "exit $?"
cargo fmt --all
git add crates/gateway/tests/docs_sync_capabilities.rs docs/features docs/llms docs/skills
git diff --cached --stat
git commit -m "docs: laya (laya-coreml-serve) System One route on every surface (SP-DEC-3 T7, AC7)"
```

### Task 8: Verify the slice, review it, and land it

- [ ] **Step 1: Full workspace, real exit codes**

```bash
df -h . | tail -1
cargo test --workspace > /tmp/sp-dec-3-all.log 2>&1; echo "exit $?"
rg -n "panicked at|test result: FAILED" /tmp/sp-dec-3-all.log; rg -c "test result: ok" /tmp/sp-dec-3-all.log
cargo clippy --workspace --all-targets -- -D warnings > /tmp/sp-dec-3-clippy.log 2>&1; echo "exit $?"
cargo fmt --all --check; echo "exit $?"
```

Expected: all three exit 0, and no `panicked at`. Also run clippy with the rustup stable toolchain (`rustup run stable cargo clippy --workspace --all-targets -- -D warnings`), because Homebrew rustc can be older than CI's.

- [ ] **Step 2: Whole-slice review.** Run `/sensei:review` (or the `review-slice` skill) over `develop..sp-dec-3-laya`. Verify every finding adversarially before acting on it. Fix any confirmed critical, high or medium finding red-first. Then review the fixes, because fixes introduce defects.

- [ ] **Step 3: Rerun the live tests** (Task 6 Step 4) after any review fix that touched `systemone.rs`.

- [ ] **Step 4: Land on develop**

```bash
git switch develop && git merge --no-ff sp-dec-3-laya -m "Merge SP-DEC-3: laya local System One route"
cargo test --workspace > /tmp/sp-dec-3-merged.log 2>&1; echo "exit $?"
git push origin develop
```

- [ ] **Step 5: Checkpoint and clean up.** Run `/sensei:checkpoint`, rewrite `docs/CHECKPOINT.md` (SP-DEC-3 done, carry-forwards: in-process CoreML Laya, CLM on Metal, a System One gate slice per the 2026-10-20 gate experiment, `/v1/systemone/batch`), and commit it. Then remove the scratch files: `rm -rf /tmp/laya-bench /tmp/laya-src /tmp/clm-src /tmp/sp-dec-3*.log` (nothing in them is tracked). `develop → main` goes as a batched PR for human review, so don't open it from this slice.

---

## Addendum — whole-slice review fixes (2026-10-20, user chose option 2)

The Task 8 whole-slice review found that Laya silently cuts long question heads (HIGH), that input-caused refusals trip the circuit breaker and are retried (MEDIUM), and that a misspelled checkpoint is silently re-routed (MEDIUM). Spec D5–D7 and AC8–AC10 record the design. These tasks run before Task 8 resumes (its Steps 1–5 are re-run on the result). Same ground rules; red first; mutation-check every guard in a scratch copy or with a `/tmp` backup.

### Task R1: `GatewayError::InputRejected` in the kernel (D5)

**Files:** `crates/kernel/src/types/error.rs`, `crates/gateway/src/engine/util.rs` (`stream_error_code`), and any other exhaustive `match` the compiler names.

- Variant (after `ProviderError`), with a doc comment stating D5's meaning:
  ```rust
      /// The candidate cannot serve this request's INPUT — the endpoint is healthy (SP-DEC-3 D5):
      /// an image to a host that ignores images, an over-window state or question, … Falls back
      /// whatever the trigger set, is never retryable, and is never recorded as an endpoint failure.
      #[error("adapter '{adapter}' cannot serve this input: {message}")]
      InputRejected { adapter: String, message: String },
  ```
- `should_trigger_fallback`: return `true` for `InputRejected` BEFORE the `triggers.is_empty()` early return.
- `is_retryable`: `false` (it is not in the `matches!` list — assert it in a test).
- `stream_error_code`: `GatewayError::InputRejected { .. } => "input_rejected".to_string()`.
- Red-first unit tests in `error.rs`'s test module: falls back with `&[]` and with `&[FallbackTrigger::RateLimit]`; not retryable; Display names the adapter and message.
- `crates/gateway/tests/docs_sync_capabilities.rs::the_fallback_table_answers_for_every_gateway_error_variant` will now fail until the fallback table in the docs gains an `InputRejected` row — add it (find the table the test reads) in this task.

### Task R2: the engine treats `InputRejected` as a skip, not a fault (D5, AC8)

**Files:** `crates/gateway/src/engine/execute.rs`, `crates/gateway/src/engine/exhaustion.rs`, tests in `crates/gateway/src/engine/decision_tests.rs` (and `tests.rs` if helpers live there).

- In the `Err(err)` arm: if `err` is `InputRejected`, do NOT call `record_outcome`; still push the `Attempt` (`fallback_triggered` from `should_trigger_fallback`, which is now `true`); push `GateContribution::InputRejected`.
- `GateContribution::InputRejected` (new): not `HardFailure` (so not retryable); `all_gated_error` returns `None` if any contribution is `InputRejected`.
- `contribution_for` must not be the path for it (or must map it to the new variant) — keep one source of truth.
- Red-first tests:
  1. Breaker neutrality: a gateway whose breaker threshold is 1, a `Decider` double that returns `InputRejected` on the first call and succeeds after; two calls to the same router → the second is DISPATCHED (decider call count 2), not skipped as breaker-open.
  2. Fallback with empty triggers: chain `[rejecting, ok]` with `fallback_triggers: vec![]` → served by `ok`, attempts `[rejecting, ok]`.
  3. Exhaustion: chain `[rejecting]` only → `AllAttemptsFailed { retryable: false, .. }`, not `AllGated`.
- Mutation-check each: re-add `record_outcome` for it (1 goes red); drop the early `true` in fallback (2 red); count it as `HardFailure` (3 red).

### Task R3: producers use it; D6 budget; D7 re-route; test gaps (D2, D3, D5–D7, AC2/4/9/10)

**Files:** `crates/cloud-providers/src/systemone.rs`, `crates/cloud-providers/tests/systemone_mock.rs`, `crates/cloud-providers/tests/cloudflare_mock.rs`, `crates/gateway/src/facade.rs` (test), `crates/gateway/tests/decision_laya.rs`.

- Red first: change the existing expectations to `InputRejected` — `laya_refuses_images_before_any_request`, `an_unrecognisable_image_fails_before_any_request`, the four truncation tests (laya, no-detail, missing-answers, ollama, cloudflare), the facade test's image assertion, `decision_laya.rs`'s attempt-error check (its fallback now needs no `ProviderError` trigger — keep the trigger set as is; also add a variant with `fallback_triggers: vec![]` that still falls back).
- Then switch the producers: `encode_images` errors and the truncation branch return `GatewayError::InputRejected { adapter, message }` (no status).
- D6: a private `fn laya_head_budget(model: &str) -> usize` (`"english"` → 192; `"multilingual" | "typed-decisions"` → 256; `_` → 192) and `fn estimated_head_tokens(q: &DecisionQuestion) -> usize` per the spec formula (instructions rendered as the JSON-to-text the wire uses: a string as-is, an object/array as its JSON text). In `SystemOneAdapter::decide`, for `self.id == "laya"`, refuse the first question over budget BEFORE `decide(...)` with `InputRejected` naming the question, its estimate and the budget. Red tests: an 808-char instruction on `english` → `InputRejected`, wiremock `expect(0)`; the same on `multilingual` (budget 256, estimate ≈ 275) → refused; a 288-char one → sent; a choice with many long options counts the options; id `llamacpp` with the 808-char instruction → sent (no budget).
- D7: `WireResponse` gains `#[serde(default)] routing: Option<WireRouting>` with `struct WireRouting { #[serde(default)] model: Option<String> }`; after the truncation check, if `routing.model` is `Some(m)` and `m != body_model` → `ProviderError { status: Some(status) }` "the host served checkpoint '{m}' instead of the requested '{body_model}' — check the model's api_model_id against the server's loaded checkpoints". Red tests: mismatch → error; equal → success; absent → success. Check `body_model` vs the Workers AI short name so Cloudflare is unaffected.
- Test gaps (review MEDIUMs): a success case with `truncated:false, state_tokens_dropped:5, truncated_questions:["q"]`; a `truncated: null` case → `ProviderError` whose message contains "failed to parse System One response".
- Live (ignored) additions in `live_laya_answers_every_type_and_reports_truncation`: the 808-char instruction is refused (`InputRejected`) and `model: "englsh"` is refused (`ProviderError` naming `english`). Run against `LAYA_URL=http://127.0.0.1:8766`.

### Task R4: docs, spec, upgrading (D4–D7)

- Every surface that says truncation / image refusal is a `ProviderError` → `InputRejected`; describe D5 (falls back regardless of triggers, not retried, not a breaker signal) — and REMOVE the now-false breaker/retry caveat for truncation and images (spec carry-forward too).
- D6: Laya's per-question budget (english 192, multilingual / typed-decisions 256 tokens, estimate `chars/3`, conservative) on `local.md`, `providers.md`, `capabilities-and-adapters.md`, `recipes.md`, SKILL.
- D7: a misspelled `api_model_id` is refused (re-routed checkpoint), check names against `/health`'s `loaded`.
- The LOW security note while here: Laya's default `LAYA_HOST` is `0.0.0.0` with no auth unless `LAYA_API_KEY` — keep `127.0.0.1` or set a key and the router's `api_key_env`.
- `upgrading.md`: retitle the Unreleased section as **breaking** (target 0.12.0): new `GatewayError::InputRejected` variant (add an arm to exhaustive matches); the D2/D3/unrecognisable-image refusals are now `InputRejected` (callers matching `ProviderError` for them must change); the D6 and D7 refusals.
- `cargo test -p sensei-gateway --test docs_sync_capabilities`, `cargo doc`, `cd site && bun run check`.

### Task R5: re-verify (Task 8 Steps 1–3 again)

Full workspace tests, stable clippy, fmt, both live tests against the running server, then a re-review of the R1–R4 diff (fixes introduce defects) before the commit gate.
