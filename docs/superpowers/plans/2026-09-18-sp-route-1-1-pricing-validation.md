# SP-ROUTE-1.1 — Pricing Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reject model pricing that cannot be used to compare candidates — non-finite or negative — at the boundary, so an operator gets a loud failure at load time instead of a silently mis-routed model.

**Architecture:** One rule (`ModelPricing::validate`) called from three sites: a `try_from`-based `Deserialize` (catches a config file on every path), `Facade::build` (drops the model and warns — the production path is deliberately unchecked), and `collect_validation_errors` (keeps the checked paths consistent).

**Tech Stack:** Rust 2024, `serde` (`#[serde(try_from = ...)]`), `tracing`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-09-18-sp-route-1-1-pricing-validation-design.md`

---

## Orientation — read before Task 1

**Baseline: 1909 passed, 0 failed, 60 ignored, real exit 0.** Repo root is `/Users/Jerry/Developer/gateway`.

**The two things this slice must not break:**

1. **Zero is a VALID price.** `Some(0.0)` is an explicit zero, distinct from `pricing: None`, and SP-ROUTE-1 Task 8 pinned that the two tie under `sort: price` (`price_sort_puts_an_unpriced_candidate_first_in_both_input_orders` at `strategy.rs:1178`, `unpriced_ties_with_an_explicit_zero_price` at `strategy.rs:1781`). Both must stay green untouched.
2. **A large finite price is deliberately ACCEPTED.** `1e300` is legal. Any magnitude threshold would be invented, and a deliberately prohibitive price is a legitimate way to park a model at the back of a chain. AC7 pins the non-rejection so it cannot drift into a silent threshold later.

**The trap this slice exists to avoid repeating:** do **not** "repair" a bad price to `None`. `None` means *free*, and free sorts **first**, so cleaning it would hand a broken price the cheapest slot. That is the same reasoning SP-ROUTE-1 used when it mapped a non-finite `PriceStrategy` key to `+inf` rather than to zero.

**Repo conventions:**
- **TDD, strictly red-first.** Write the failing test, RUN it, read the actual failure, then implement.
- **`cargo fmt --all` before committing.** Pre-commit runs fmt-check + `cargo clippy --workspace --all-targets -- -D warnings`, and **no tests** — run them yourself.
- **Verify real exit codes.** `cargo test` exits 101 on a compile error too. Redirect to a file and read `$?` — this is zsh, `${PIPESTATUS[0]}` does NOT work. Grep the log for `panicked at`.
- Run clippy under **both** the Homebrew default and rustup stable; Homebrew's rustc shadows rustup here and has let clippy pass locally while CI failed.
- Mutation-check every "guarded by X" claim: apply the one-line mutation, confirm a real panic, restore, quote the panic line.

---

## File Structure

**Modified:**
- `crates/kernel/src/types/config.rs` — `ModelPricing::validate`, the `try_from` shadow struct
- `crates/gateway/src/config.rs` — the `collect_validation_errors` rule
- `crates/gateway/src/facade.rs` — drop-and-warn in `build`
- `docs/features/routing/provider-preferences.md`, `docs/llms/configuration.md`, `docs/llms/upgrading.md` — the new rejection

No new files. No new dependencies.

---

## Task 1: The one rule

**Files:**
- Modify: `crates/kernel/src/types/config.rs`
- Test: same file, inline `mod tests`

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn pricing_validate_rejects_negative_and_non_finite_but_accepts_zero() {
    let ok = |p: ModelPricing| assert!(p.validate().is_ok(), "{p:?} must be valid");
    let bad = |p: ModelPricing, needle: &str| {
        let e = p.validate().expect_err("must be rejected");
        assert!(
            e.contains(needle),
            "the error must name the offending field and value; got {e:?}"
        );
    };

    // Zero is an EXPLICIT price, distinct from `pricing: None`, and SP-ROUTE-1
    // Task 8 pinned that the two tie under `sort: price`. Rejecting it would
    // break shipped, tested behaviour.
    ok(pricing(0.0, 0.0, None));
    ok(pricing(0.0008, 0.004, Some(0.0)));
    // Deliberately accepted — see the spec. Any magnitude threshold is invented,
    // and a prohibitive price is a legitimate way to park a model last.
    ok(pricing(1e300, 1e300, Some(1e300)));

    bad(pricing(-0.001, 0.004, None), "input_per_1k");
    bad(pricing(0.001, -0.004, None), "output_per_1k");
    bad(pricing(0.001, 0.004, Some(-1.0)), "per_request");
    bad(pricing(f64::NAN, 0.004, None), "input_per_1k");
    bad(pricing(0.001, f64::INFINITY, None), "output_per_1k");
    bad(pricing(0.001, 0.004, Some(f64::NEG_INFINITY)), "per_request");
}

fn pricing(input: f64, output: f64, per_request: Option<f64>) -> ModelPricing {
    ModelPricing { input_per_1k: input, output_per_1k: output, per_request }
}
```

- [ ] **Step 2: Run them, confirm they fail**

Run: `cargo test -p sensei-kernel pricing_validate_rejects -- --nocapture`
Expected: FAIL — `no method named 'validate'`.

- [ ] **Step 3: Implement**

```rust
impl ModelPricing {
    /// `Err(reason)` when this pricing cannot be used to COMPARE candidates.
    ///
    /// Rejects non-finite and negative values. A `NaN` makes the routing
    /// comparator intransitive — `sort_by` panics on that — and a negative
    /// price sorts FIRST under `sort: price`, winning the cheapest slot with a
    /// number that is not a price.
    ///
    /// **Zero is valid**: `Some(0.0)` is an explicit zero, distinct from
    /// `pricing: None`, and the two deliberately tie.
    ///
    /// **A large finite value is valid**: any magnitude threshold would be
    /// invented, the routing layer already fences the overflow it can cause,
    /// and a prohibitive price is a legitimate way to park a model last.
    pub fn validate(&self) -> Result<(), String> {
        let check = |name: &str, v: f64| -> Result<(), String> {
            if !v.is_finite() {
                return Err(format!("{name} must be a finite number, got {v}"));
            }
            if v < 0.0 {
                return Err(format!("{name} must not be negative, got {v}"));
            }
            Ok(())
        };
        check("input_per_1k", self.input_per_1k)?;
        check("output_per_1k", self.output_per_1k)?;
        if let Some(per_request) = self.per_request {
            check("per_request", per_request)?;
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Confirm green, then mutation-check**

Run the test. Then apply each and confirm a real `panicked at`, restoring after each:
- `if v < 0.0` → `if v < -1.0` (a `-0.001` price passes)
- `!v.is_finite()` → `v.is_nan()` (an infinity passes)
- drop the `per_request` branch

Quote each panic line.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(kernel): ModelPricing::validate — the one rule (SP-ROUTE-1.1 Task 1)"
```

---

## Task 2: Reject at the deserialization boundary

**Files:**
- Modify: `crates/kernel/src/types/config.rs`
- Test: same file

This is the site that matters most — it catches a config **file** on every path, checked or unchecked, with no signature change anywhere.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn a_negative_price_fails_to_deserialize_and_the_error_names_the_field() {
    let err = serde_json::from_str::<ModelPricing>(
        r#"{"input_per_1k": -0.001, "output_per_1k": 0.004}"#,
    )
    .expect_err("a negative price must not load");
    let msg = err.to_string();
    assert!(msg.contains("input_per_1k"), "must name the field: {msg}");
    assert!(msg.contains("-0.001"), "must name the value: {msg}");
}

/// An explicit zero and a large finite price both still load. These are the two
/// non-rejections the spec makes deliberately; pinning them stops either
/// drifting into a silent threshold.
#[test]
fn an_explicit_zero_and_a_large_finite_price_still_deserialize() {
    let zero: ModelPricing =
        serde_json::from_str(r#"{"input_per_1k": 0.0, "output_per_1k": 0.0}"#).unwrap();
    assert_eq!(zero.input_per_1k, 0.0);

    let big: ModelPricing =
        serde_json::from_str(r#"{"input_per_1k": 1e300, "output_per_1k": 1e300}"#).unwrap();
    assert_eq!(big.input_per_1k, 1e300);
}

/// `serde_json` already refuses a literal `NaN` and an out-of-range `1e400`.
/// Pinned so a future format change (or a hand-rolled visitor) cannot quietly
/// start accepting them.
#[test]
fn a_non_finite_literal_is_refused_by_the_format_itself() {
    assert!(serde_json::from_str::<ModelPricing>(
        r#"{"input_per_1k": NaN, "output_per_1k": 0.004}"#).is_err());
    assert!(serde_json::from_str::<ModelPricing>(
        r#"{"input_per_1k": 1e400, "output_per_1k": 0.004}"#).is_err());
}

/// A format that CAN express a non-finite value must still be rejected by our
/// rule rather than relying on the format. Round-trips through a value type
/// that carries `f64::INFINITY` faithfully.
#[test]
fn a_non_finite_value_is_rejected_by_our_own_rule() {
    let raw = serde_json::json!({"input_per_1k": 0.001, "output_per_1k": 0.004});
    let mut v = raw.clone();
    // serde_json cannot hold an infinity, so assert the rule directly at the
    // seam instead — the `TryFrom` is what `Deserialize` delegates to.
    let _ = v.as_object_mut();
    let bad = ModelPricing { input_per_1k: f64::INFINITY, output_per_1k: 0.004, per_request: None };
    assert!(bad.validate().is_err(), "the rule, not the format, must reject an infinity");
}

/// Serialization is UNCHANGED — a valid pricing round-trips byte-identically.
#[test]
fn a_valid_pricing_round_trips_unchanged() {
    let p = ModelPricing { input_per_1k: 0.0008, output_per_1k: 0.004, per_request: Some(0.01) };
    let json = serde_json::to_string(&p).unwrap();
    let back: ModelPricing = serde_json::from_str(&json).unwrap();
    assert_eq!(back.input_per_1k, p.input_per_1k);
    assert_eq!(back.output_per_1k, p.output_per_1k);
    assert_eq!(back.per_request, p.per_request);
}
```

- [ ] **Step 2: Run them, confirm the negative one fails**

The negative test should fail (it deserializes fine today). Quote the actual failure.

- [ ] **Step 3: Implement via `try_from`**

There is **no custom-`Deserialize` precedent in this workspace**, so use the idiomatic shadow-struct form rather than a hand-rolled visitor — it keeps the derive for `Serialize` and produces a clean error with no `Visitor` boilerplate:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "ModelPricingRaw")]
pub struct ModelPricing {
    pub input_per_1k: f64,
    pub output_per_1k: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub per_request: Option<f64>,
}

/// Deserialization shadow for [`ModelPricing`], so [`ModelPricing::validate`]
/// runs at the boundary. A price that cannot be compared must not load at all:
/// a `NaN` makes the routing comparator intransitive and a negative price wins
/// the cheapest slot under `sort: price`.
#[derive(Deserialize)]
struct ModelPricingRaw {
    input_per_1k: f64,
    output_per_1k: f64,
    #[serde(default)]
    per_request: Option<f64>,
}

impl TryFrom<ModelPricingRaw> for ModelPricing {
    type Error = String;
    fn try_from(raw: ModelPricingRaw) -> Result<Self, Self::Error> {
        let p = ModelPricing {
            input_per_1k: raw.input_per_1k,
            output_per_1k: raw.output_per_1k,
            per_request: raw.per_request,
        };
        p.validate()?;
        Ok(p)
    }
}
```

**Watch for:** `#[serde(try_from)]` requires the target to be `Clone`. It is. Confirm `Serialize` still derives on `ModelPricing` itself and that the `skip_serializing_if` behaviour is unchanged — the round-trip test covers it.

- [ ] **Step 4: Confirm green, then mutation-check**

Apply and confirm a real panic, restoring after each:
- delete `p.validate()?;` from `try_from`
- change `try_from = "ModelPricingRaw"` so validation is bypassed (remove the attribute entirely and restore the plain derive)

- [ ] **Step 5: Confirm nothing else broke**

Run: `cargo test --workspace`
**Any existing config fixture carrying a negative or non-finite price will now fail to load.** That is the intended change — but report every such test individually with its file and what it was asserting, and do NOT adjust one you cannot explain.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat(kernel): reject unusable pricing at the deserialization boundary (SP-ROUTE-1.1 Task 2, AC1-AC3, AC7)"
```

---

## Task 3: The checked config paths

**Files:**
- Modify: `crates/gateway/src/config.rs`
- Test: same file

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn validate_config_rejects_a_model_whose_pricing_cannot_be_compared() {
    let mut config = /* a valid two-model config */;
    config.models.get_mut("claude-haiku").unwrap().pricing = Some(ModelPricing {
        input_per_1k: -0.001,
        output_per_1k: 0.004,
        per_request: None,
    });

    match validate_config(&config) {
        Err(GatewayError::InvalidConfig(msg)) => {
            assert!(msg.contains("claude-haiku"), "must name the model: {msg}");
            assert!(msg.contains("input_per_1k"), "must name the field: {msg}");
        }
        other => panic!("expected InvalidConfig, got {other:?}"),
    }
}
```

Note this must be constructed **programmatically** — a negative price can no longer be deserialized after Task 2, which is the point.

- [ ] **Step 2: Run it, confirm it fails**

- [ ] **Step 3: Add the rule**

In `collect_validation_errors`, after the existing model rules:

```rust
    // Rule: a model's pricing must be comparable. Same rule as the
    // deserialization boundary (`ModelPricing::validate`), called rather than
    // restated — SP-ROUTE-1 shipped a defect where a second derivation of one
    // value drifted from the first.
    for (id, model) in models {
        if let Some(pricing) = &model.pricing
            && let Err(reason) = pricing.validate()
        {
            errors.push(format!("model '{id}' has unusable pricing: {reason}"));
        }
    }
```

- [ ] **Step 4: Confirm green, mutation-check (delete the rule), commit**

```bash
git add -A
git commit -m "feat(gateway): collect_validation_errors rejects unusable pricing (SP-ROUTE-1.1 Task 3, AC6)"
```

---

## Task 4: The facade drops the model, and must not make it free

**Files:**
- Modify: `crates/gateway/src/facade.rs`
- Test: same file (note the `#[cfg(all(test, feature = "local"))]` module — put these in a default-feature test module instead, or they will not run)

This is the production path: `Facade::build` calls the deliberately unchecked `Gateway::new`, and its doc states *"construction never fails on a single bad router"*. It gains a `warn` and may build with fewer models; **its signature does not change.**

- [ ] **Step 1: Write the failing tests**

```rust
/// AC4 — a programmatically-constructed bad price does not reach routing.
/// The facade drops the model and still builds, matching how it already logs
/// and skips a cloud router that fails to construct.
#[tokio::test]
async fn the_facade_drops_a_model_whose_pricing_cannot_be_compared() {
    let config = /* two models; "bad" has input_per_1k: -1.0 */;
    let facade = Facade::builder(config).build().await;
    let models = facade.gateway().list_models().await.unwrap();
    let ids: Vec<String> = models.iter().map(|m| m["id"].as_str().unwrap().to_string()).collect();
    assert!(!ids.contains(&"bad".to_string()), "the unusable model must be dropped: {ids:?}");
    assert!(ids.contains(&"good".to_string()), "and the rest must survive: {ids:?}");
}

/// AC5 — the SHARP one. A dropped model must not come back as FREE.
///
/// `None` means free and free sorts FIRST, so "repairing" a bad price by
/// nulling it would hand it the cheapest slot — the exact hazard SP-ROUTE-1
/// avoided by mapping a non-finite `PriceStrategy` key to `+inf`. This asserts
/// the dropped model never appears AHEAD of a priced candidate under
/// `sort: price`, which a `pricing: None` repair would fail.
#[tokio::test]
async fn a_dropped_model_never_outranks_a_priced_one() {
    // chain: [bad (negative price), good (0.5)]  — with `sort: price`
    // A `None`-repair would put `bad` FIRST (free); a drop removes it entirely.
    let order = /* select_all through the facade's gateway with sort: price */;
    assert_eq!(order, vec!["good"], "the dropped model must be absent, not free-and-first");
}
```

- [ ] **Step 2: Run them, confirm they fail**

- [ ] **Step 3: Implement**

In `Facade::build`, before `Gateway::new`:

```rust
        // A model whose price cannot be compared is not safely routable, so drop
        // it — and drop it rather than nulling its pricing. `None` means FREE,
        // and free sorts FIRST, so "repairing" a broken price would hand it the
        // cheapest slot. Same stance the facade already takes for a cloud router
        // that fails to build: logged, skipped, construction still succeeds.
        let mut config = self.config;
        config.models.retain(|id, model| match model.pricing.as_ref().map(|p| p.validate()) {
            Some(Err(reason)) => {
                tracing::warn!(model = %id, error = %reason,
                    "model dropped: its pricing cannot be compared");
                false
            }
            _ => true,
        });

        let gateway = Gateway::new(config, self.registry.clone(), breaker);
```

- [ ] **Step 4: Confirm green, then mutation-check BOTH**

Restore after each, quote the panic:
- delete the `retain` (the drop test fails)
- replace the drop with `model.pricing = None` — i.e. the tempting repair. **`a_dropped_model_never_outranks_a_priced_one` must fail**, and that failure is the whole reason the test exists. If it passes, the test is not pinning what it claims and that is a BLOCKER.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat(gateway): facade drops a model with uncomparable pricing (SP-ROUTE-1.1 Task 4, AC4-AC5)"
```

---

## Task 5: Docs and final verification

**Files:**
- Modify: `docs/features/routing/provider-preferences.md`, `docs/llms/configuration.md`, `docs/llms/upgrading.md`
- Modify: the SP-ROUTE-1 plan's carry-forward entry, `docs/CHECKPOINT.md`

- [ ] **Step 1: Find every surface**

```
rg --no-ignore -g '!target' -g '!site/node_modules' -l 'ModelPricing|input_per_1k|pricing' docs/ README.md
```

Report the full list and which you changed.

- [ ] **Step 2: Document**

- The rejection, with **both** deliberate non-rejections stated (zero is valid; a large finite price is valid) so neither reads as an oversight.
- That a config **file** with a bad price now fails to load — the one breaking change, belongs in `upgrading.md`.
- That `Facade::build` drops such a model and warns, and that its signature is unchanged.
- Update SP-ROUTE-1's carry-forward 2 from "contained" to **closed**, naming this slice.

- [ ] **Step 3: Verify**

Each with its REAL unpiped exit code (zsh — `${PIPESTATUS[0]}` does NOT work):

```
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings      # BOTH toolchains
cargo fmt --all --check
cargo test -p sensei-gateway --features local --locked
cargo doc --workspace --no-deps
```

Grep the test log for `panicked at`.

- [ ] **Step 4: Confirm each AC has a named passing test**

AC1–AC8 from the spec. Name the test for each and run it individually. Any AC without a green named test is unfinished — report it.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "docs: SP-ROUTE-1.1 pricing validation (Task 5, AC8)"
```

---

## Self-review notes

**Spec coverage:** §2 (what is invalid, and the two deliberate non-rejections) → Task 1 + AC7. §3 (three sites, one rule) → Tasks 2, 3, 4. §3.1 (drop, do not neutralise) → Task 4 AC5. §4 (blast radius) → Task 2 Step 5 + Task 5. §5 AC8 → Task 5.

**The one test that must not be waved through** is AC5. Every other test here fails if the feature is absent; AC5 fails if the feature is implemented the *tempting* way. It is the only guard against re-introducing the hazard the spec was written to avoid, so its mutation check (replace the drop with `pricing = None`) is mandatory, not optional.
