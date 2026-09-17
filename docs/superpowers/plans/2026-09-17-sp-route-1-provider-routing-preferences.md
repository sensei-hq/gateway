# SP-ROUTE-1 — Per-Request Provider Routing Preferences Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a caller shape provider routing per request — `sort` (price/latency/throughput), `only`/`ignore` filters, an explicit `order` — and make price-weighted, uptime-aware selection the default within equal-priority groups.

**Architecture:** Additive. A new `RoutingPreferences` on `InferenceRequest` threads through `SelectionCriteria` into `ModelSelectionService`. Filtering lands as an `AdmissionGate` registered first; ordering lands on the existing `RoutingStrategy` seam, widened with a `StrategyCtx` carrying a performance read-port and a random source. A new `PerformanceStore`/`PerformanceRecorder` pair mirrors the existing cooldown/lockout store+sink pattern to supply live latency, throughput and reliability.

**Tech Stack:** Rust 2024, `serde`, `uuid` (already a dependency — used to seed SplitMix64 so no RNG crate is added), `tokio`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-09-17-sp-route-1-provider-routing-preferences-design.md`

---

## Progress

| Task | Commits | State |
|---|---|---|
| 1 | `83a5371` · `ba5e93f` · `cd1fcc7` | ✅ done, two review rounds |
| 2 | `cf19179` | ✅ done, reviewed jointly with Task 3 |
| 3 | `da0dfb5` · `6e5cfa5` | ✅ done, 2 Critical + 3 Important fixed |
| 4 | `edab9c4` · `1b75dae` | ✅ done; review forced an `AttemptPhase` design fix — see below |
| 5–12 | — | pending |

**Task 4 changed the design, and Tasks 5 / 7 / 9 inherit it.** The review found that
`mean_latency_ms` pooled two unrelated time spans (full request wall time from `execute`, stream
acquisition from `stream`) because `Sample` had no discriminator — and that Task 5 would have
added a third. It also found the consequence that actually bites: an endpoint failing every
stream mid-way converged on `success_rate == 0.5` forever, because its acquisition success was
counted beside its completion failure, so Task 7's reliability multiplier could never de-weight it.

The fix is `AttemptPhase { Complete, StreamAcquired, StreamCompleted }` on `AttemptOutcome`, read
**only** by `PerformanceRecorder` — the breaker, cooldown and lockout still see `success`
unchanged. `StreamAcquired` contributes a latency and no verdict; `StreamCompleted` contributes a
verdict and a throughput but no latency. One attempt, one vote.

`dispatch_outcome` now takes `&AttemptOutcome` rather than seven positional arguments. Task 5's
snippets below are already updated for both changes.

Off-slice, landed alongside: `5208952` — revived `facade.rs`'s test module (uncompilable since
2026-07-23) and added a `cargo check -p sensei-gateway --features local --all-targets` CI step,
because no CI job had ever enabled a non-default feature.

## The review lesson — apply it to every remaining task

Every Critical and Important finding in Tasks 1–3 was the same shape: **a test that cannot fail on
the thing its name claims.** Three of the four were the negative half asserted without the positive
half.

- Task 1: deleting a field from the hand-written `Debug` left the whole workspace green.
- Task 1: the serde test pinned 2 of 8 `skip_serializing_if` attributes.
- Task 3: both service tests asserted the named candidate was *excluded*, never that an unnamed one
  *survived* — so a gate that excluded everything passed 332/332.
- Task 3: `only`'s empty-`routers` axis was unpinned while the mirror `models` case was covered.

So, for every task below:

1. **Assert the positive half.** A filtering or ordering test that only checks what was removed or
   demoted will pass against an implementation that removes or demotes everything. Name the
   candidate that must SURVIVE, and where it must land.
2. **Name the mutation up front.** Any claim of the form "guarded by X" gets the one-line source
   change that should break it, and that mutation is actually run — a real `panicked at`, not a
   compile error (`cargo test` exits 101 for both).
3. **Check the mirror case.** If a rule has two symmetric axes or branches, test both. The Task 3
   asymmetry — `models` covered, `routers` not — is exactly how half a rule ships unpinned.

This bites hardest in Tasks 7–10: a weighted strategy that returns the right *set* in the wrong
*order* will pass any test that only checks membership.

## Orientation — read before Task 1

**The two-dimensional candidate space.** A candidate is a `(router, model)` pair. `router` is the provider backend (`anthropic`, `ollama`); `model` is the model id (`claude-haiku`, `gemma3:27b`). The endpoint key is `format!("{router}:{model}")` and **cannot be parsed back** — model ids contain colons. Always carry the two parts separately.

**Where things live:**
- `crates/kernel/src/types/request.rs` — `InferenceRequest` and the new preference types
- `crates/gateway/src/selection.rs` — `ModelSelectionService`, `SelectionCriteria`, `SelectedModel`
- `crates/gateway/src/strategy.rs` — the `RoutingStrategy` ordering seam
- `crates/gateway/src/gates/` — one file per admission gate; each holds gate + (optional) store + sink
- `crates/gateway/src/skip_reason.rs` — `SkipReason` and its `gate_status()` classification
- `crates/gateway/src/engine/{execute,stream,mod}.rs` — request execution, streaming, recorder fan-out

**Verify real exit codes.** `cargo test` exits 101 on a *compile error* too, so a red step must be confirmed by reading the failure text, not the exit code. When a step says "expect FAIL with X", check that X actually appears.

**Run from the repo root**, `/Users/Jerry/Developer/gateway`.

---

## File Structure

**Created:**
- `crates/gateway/src/gates/routing_policy.rs` — `RoutingPolicyGate` + the `only`/`ignore` matching rules
- `crates/gateway/src/gates/performance.rs` — `EndpointPerformanceRead`, `EndpointStats`, `PerformanceStore`, `PerformanceRecorder`
- `crates/gateway/src/random.rs` — `RandomSource` + `SplitMix64`

**Modified:**
- `crates/kernel/src/types/request.rs` — preference types + `InferenceRequest.routing`
- `crates/gateway/src/skip_reason.rs` — `ExcludedByPolicy` variant
- `crates/gateway/src/gates/mod.rs` — `AttemptOutcome` fields, module registration
- `crates/gateway/src/selection.rs` — criteria field, gate registration, strategy resolution, `order` re-ranking
- `crates/gateway/src/strategy.rs` — `StrategyCtx`, the four strategies
- `crates/gateway/src/resilience.rs` — `min_samples` tunable
- `crates/gateway/src/engine/mod.rs` — `Gateway` fields, `build_recorders`, `dispatch_outcome`
- `crates/gateway/src/engine/execute.rs` / `stream.rs` — criteria construction, outcome dispatch, mid-stream fix
- `crates/kernel/src/types/trace.rs` — routing decision record

---

## Task 1: The request surface

**Files:**
- Modify: `crates/kernel/src/types/request.rs`
- Test: `crates/kernel/src/types/request.rs` (inline `mod tests`)

- [x] **Step 1: Write the failing test**

Add to the `mod tests` block at the bottom of `crates/kernel/src/types/request.rs`:

```rust
#[test]
fn routing_preferences_round_trip_and_stay_absent_by_default() {
    let prefs = RoutingPreferences {
        sort: Some(SortKey::Price),
        only: Some(CandidateSet {
            routers: vec!["anthropic".into()],
            models: vec![],
        }),
        ignore: None,
        order: Some(vec![CandidateRef {
            router: Some("ollama".into()),
            model: None,
        }]),
    };
    let json = serde_json::to_string(&prefs).unwrap();
    assert_eq!(
        serde_json::from_str::<RoutingPreferences>(&json).unwrap(),
        prefs
    );
    // Empty axes and absent knobs must not be emitted — a caller sending
    // `only: {routers: [...]}` should not get `models: []` back.
    assert!(!json.contains("models"), "empty axis must be skipped: {json}");
    assert!(!json.contains("ignore"), "absent knob must be skipped: {json}");
}

/// A request with no routing preferences must serialize byte-identically to
/// one from before this slice — the additive guarantee.
#[test]
fn a_request_without_preferences_emits_no_routing_key() {
    let req = InferenceRequest {
        capability: Capability::TextChat,
        model: None,
        router: None,
        chain: None,
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, "hi")],
            system: None,
            max_tokens: None,
            temperature: None,
            tools: Vec::new(),
        },
        budget: None,
        auth: None,
        panel: None,
        consensus: None,
        allow_fallback: true,
        credentials: Default::default(),
        routing: None,
    };
    let json = serde_json::to_string(&req).unwrap();
    assert!(!json.contains("routing"), "absent ⇒ no key: {json}");
}
```

`Payload::Chat` takes `messages, system, max_tokens, temperature, tools` — `tools` is a `Vec`, not an `Option`. Import `Message`, `MessageRole` and `Payload` from this module if the test block does not already have them.

- [x] **Step 2: Run the test to verify it fails**

Run: `cargo test -p sensei-kernel routing_preferences_round_trip -- --nocapture`
Expected: FAIL — `cannot find type 'RoutingPreferences' in this scope`.

- [x] **Step 3: Add the preference types**

In `crates/kernel/src/types/request.rs`, above `pub struct InferenceRequest`:

```rust
/// Per-request provider-routing preferences (SP-ROUTE-1).
///
/// Every knob is optional; an entirely absent `RoutingPreferences` means "use
/// the default" — price-weighted, uptime-aware selection within equal-priority
/// groups, which is a no-op on any chain whose priorities are distinct.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct RoutingPreferences {
    /// Replace the default ordering with a deterministic sort.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort: Option<SortKey>,
    /// Allowlist. AND across non-empty axes (see `CandidateSet`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub only: Option<CandidateSet>,
    /// Denylist. OR across non-empty axes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ignore: Option<CandidateSet>,
    /// Explicit try-order. Candidates matching no entry follow as fallbacks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub order: Option<Vec<CandidateRef>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortKey {
    Price,
    Latency,
    Throughput,
}

/// A set of candidates named on either axis. The two axes are SEPARATE because
/// the endpoint key `"{router}:{model}"` cannot be parsed back — model ids
/// contain colons (`"ollama:gemma3:27b"`).
///
/// An EMPTY list is "don't care" on that axis.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct CandidateSet {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub routers: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub models: Vec<String>,
}

/// One position in an explicit sequence. An ABSENT field is a wildcard, so
/// `{router: "anthropic"}` means "every anthropic candidate, here".
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct CandidateRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub router: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}
```

- [x] **Step 4: Add the field to `InferenceRequest`**

After the `credentials` field:

```rust
    /// Per-request provider-routing preferences (SP-ROUTE-1). `None` ⇒ the
    /// default strategy; the wire format is byte-identical to before this slice.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<RoutingPreferences>,
```

And in the hand-written `impl std::fmt::Debug for InferenceRequest`, after the `consensus` line:

```rust
            .field("routing", &self.routing)
```

- [x] **Step 5: Sweep the 55 struct literals**

Adding a field breaks every `InferenceRequest { .. }` literal (there is no `Default` impl). Find them:

Run: `cargo build --workspace 2>&1 | grep -E "^error\[E0063\]" -A 3 | head -40`

Add `routing: None,` to each. To enumerate the files first:

Run: `rg --no-ignore -g '!target' -c 'InferenceRequest \{' --type rust`
Expected: 55 occurrences across ~12 files, the largest being `crates/gateway/src/engine/tests.rs` (18).

- [x] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p sensei-kernel routing_preferences_round_trip a_request_without_preferences -- --nocapture`
Expected: PASS, 2 tests.

Run: `cargo build --workspace`
Expected: success, no E0063.

- [x] **Step 7: Commit**

```bash
git add crates/kernel/src/types/request.rs crates/gateway crates/orchestrator
git commit -m "feat(kernel): RoutingPreferences on InferenceRequest (SP-ROUTE-1 Task 1)"
```

---

## Task 2: `SkipReason::ExcludedByPolicy`

**Files:**
- Modify: `crates/gateway/src/skip_reason.rs`
- Test: `crates/gateway/src/skip_reason.rs` (inline `mod tests`)

- [x] **Step 1: Write the failing test**

```rust
/// A policy exclusion is STRUCTURAL, and that is a behaviour rather than a label.
///
/// `all_gated_error` counts only `Timed`/`Terminal` toward `any_gate`
/// (`exhaustion.rs:73,90`), so a selection excluded entirely by the caller's own
/// filters returns `None` from it and surfaces as a terminal `NoCandidates`
/// rather than a pause. That is correct: no deadline and no human remedy makes
/// a candidate the caller excluded eligible again.
#[test]
fn a_policy_exclusion_is_structural_and_never_pauses_a_run() {
    assert!(matches!(
        SkipReason::ExcludedByPolicy.gate_status(),
        GateStatus::Structural
    ));
    assert_eq!(
        SkipReason::ExcludedByPolicy.to_string(),
        "excluded by request routing preferences"
    );
}
```

- [x] **Step 2: Run the test to verify it fails**

Run: `cargo test -p sensei-gateway a_policy_exclusion_is_structural -- --nocapture`
Expected: FAIL — `no variant named 'ExcludedByPolicy'`.

- [x] **Step 3: Add the variant**

In `crates/gateway/src/skip_reason.rs`, add to `enum SkipReason` after `UnsupportedCapability`:

```rust
    /// The request's own `only`/`ignore` preferences excluded this candidate.
    ///
    /// Deliberately carries NO detail. The caller already knows what it asked
    /// for, and echoing the matched rule back would only widen the diagnostic
    /// surface for no gain.
    ExcludedByPolicy,
```

In `impl Display`, add:

```rust
            SkipReason::ExcludedByPolicy => write!(f, "excluded by request routing preferences"),
```

In `gate_status`, add `ExcludedByPolicy` to the existing `Structural` arm:

```rust
            SkipReason::ModelNotFound
            | SkipReason::RouterNotFound
            | SkipReason::RouterDisabled
            | SkipReason::ExcludedByPolicy
            | SkipReason::UnsupportedCapability(_) => GateStatus::Structural,
```

- [x] **Step 4: Run the test to verify it passes**

Run: `cargo test -p sensei-gateway a_policy_exclusion_is_structural -- --nocapture`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add crates/gateway/src/skip_reason.rs
git commit -m "feat(gateway): SkipReason::ExcludedByPolicy, structural (SP-ROUTE-1 Task 2)"
```

---

## Task 3: `RoutingPolicyGate` — `only` / `ignore`

**Files:**
- Create: `crates/gateway/src/gates/routing_policy.rs`
- Modify: `crates/gateway/src/gates/mod.rs`, `crates/gateway/src/selection.rs`
- Test: `crates/gateway/src/gates/routing_policy.rs`, `crates/gateway/src/selection.rs`

- [x] **Step 1: Write the failing matching tests**

Create `crates/gateway/src/gates/routing_policy.rs` with only the test module for now:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::request::{CandidateSet, RoutingPreferences};

    fn set(routers: &[&str], models: &[&str]) -> CandidateSet {
        CandidateSet {
            routers: routers.iter().map(|s| s.to_string()).collect(),
            models: models.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// `only` is AND across NON-EMPTY axes. An empty axis constrains nothing —
    /// the convention `TierPredicate` already uses ("every PRESENT field must
    /// match; an absent field is don't-care").
    #[test]
    fn only_is_and_across_non_empty_axes() {
        let p = RoutingPreferences {
            only: Some(set(&["anthropic"], &["claude-haiku"])),
            ..Default::default()
        };
        assert!(admits(&p, "anthropic", "claude-haiku"));
        assert!(!admits(&p, "anthropic", "claude-opus"), "model axis must bind");
        assert!(!admits(&p, "bedrock", "claude-haiku"), "router axis must bind");

        let routers_only = RoutingPreferences {
            only: Some(set(&["anthropic"], &[])),
            ..Default::default()
        };
        assert!(
            admits(&routers_only, "anthropic", "anything"),
            "an EMPTY axis is don't-care, not 'match nothing'"
        );
    }

    /// `ignore` is OR across non-empty axes. AND would exclude only the single
    /// named pair, which is not what "ignore this router and that model" means.
    #[test]
    fn ignore_is_or_across_non_empty_axes() {
        let p = RoutingPreferences {
            ignore: Some(set(&["ollama"], &["claude-opus"])),
            ..Default::default()
        };
        assert!(!admits(&p, "ollama", "gemma3:27b"), "router match excludes");
        assert!(!admits(&p, "anthropic", "claude-opus"), "model match excludes");
        assert!(admits(&p, "anthropic", "claude-haiku"), "neither ⇒ admitted");
    }

    /// `ignore` is applied AFTER `only` (§4.2), so it can subtract from an
    /// allowlist. Reversing the order would let `only` re-admit an ignored
    /// candidate.
    #[test]
    fn ignore_subtracts_from_only() {
        let p = RoutingPreferences {
            only: Some(set(&["anthropic"], &[])),
            ignore: Some(set(&[], &["claude-opus"])),
            ..Default::default()
        };
        assert!(admits(&p, "anthropic", "claude-haiku"));
        assert!(!admits(&p, "anthropic", "claude-opus"));
    }

    /// No preferences at all ⇒ everything admitted. The additive guarantee.
    #[test]
    fn absent_preferences_admit_everything() {
        let p = RoutingPreferences::default();
        assert!(admits(&p, "any", "thing"));
    }

    fn admits(p: &RoutingPreferences, router: &str, model: &str) -> bool {
        super::admitted_by_policy(Some(p), router, model)
    }
}
```

- [x] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sensei-gateway routing_policy -- --nocapture`
Expected: FAIL — `file not found for module 'routing_policy'` until Step 3 registers it, then `cannot find function 'admitted_by_policy'`.

- [x] **Step 3: Write the gate and the matching rules**

At the top of `crates/gateway/src/gates/routing_policy.rs`, above the test module:

```rust
use super::{AdmissionGate, CandidateView, GateVerdict, SelectionCtx};
use crate::skip_reason::SkipReason;
use crate::types::request::{CandidateSet, RoutingPreferences};

/// Gate: the request's own `only`/`ignore` preferences must not exclude this
/// candidate.
///
/// **Registered FIRST** in `ModelSelectionService::new`, and that position is
/// load-bearing twice over. `admit` returns the FIRST skip, so position decides
/// which reason a multiply-gated candidate reports: the caller's own instruction
/// is the most specific explanation available, and reporting "circuit breaker
/// open" for a candidate the caller explicitly excluded is misleading. More
/// importantly `ExcludedByPolicy` is `Structural`, which contributes nothing to
/// `all_gated_error`'s `any_gate` — so an excluded candidate must NOT donate its
/// breaker deadline to `resume_after`. Waiting never makes an excluded candidate
/// eligible, so a run that pauses on one waits for nothing.
pub struct RoutingPolicyGate;

impl AdmissionGate for RoutingPolicyGate {
    fn name(&self) -> &'static str {
        "routing_policy"
    }

    fn evaluate(&self, c: &CandidateView<'_>, x: &SelectionCtx<'_>) -> GateVerdict {
        if admitted_by_policy(x.preferences, c.router, c.model) {
            GateVerdict::Admit
        } else {
            GateVerdict::Skip(SkipReason::ExcludedByPolicy)
        }
    }
}

/// `only` then `ignore`, in that order (§4.2).
pub(crate) fn admitted_by_policy(
    prefs: Option<&RoutingPreferences>,
    router: &str,
    model: &str,
) -> bool {
    let Some(p) = prefs else { return true };
    if let Some(only) = &p.only
        && !matches_all_axes(only, router, model)
    {
        return false;
    }
    if let Some(ignore) = &p.ignore
        && matches_any_axis(ignore, router, model)
    {
        return false;
    }
    true
}

/// AND across non-empty axes — an empty list is don't-care.
fn matches_all_axes(s: &CandidateSet, router: &str, model: &str) -> bool {
    let router_ok = s.routers.is_empty() || s.routers.iter().any(|r| r == router);
    let model_ok = s.models.is_empty() || s.models.iter().any(|m| m == model);
    router_ok && model_ok
}

/// OR across non-empty axes.
fn matches_any_axis(s: &CandidateSet, router: &str, model: &str) -> bool {
    s.routers.iter().any(|r| r == router) || s.models.iter().any(|m| m == model)
}
```

- [x] **Step 4: Register the module and thread preferences through**

In `crates/gateway/src/gates/mod.rs`, add to the module list:

```rust
pub mod routing_policy;
```

and add to `SelectionCtx`, after `model_lockout`:

```rust
    /// The request's routing preferences, read by [`routing_policy::RoutingPolicyGate`].
    /// `None` ⇒ no filtering.
    pub preferences: Option<&'a RoutingPreferences>,
```

with `use crate::types::request::RoutingPreferences;` at the top.

In `crates/gateway/src/selection.rs`, add to `SelectionCriteria`:

```rust
    /// Per-request routing preferences (SP-ROUTE-1). `None` ⇒ default routing.
    pub preferences: Option<RoutingPreferences>,
```

register the gate FIRST in `ModelSelectionService::new`'s vector:

```rust
            gates: vec![
                // FIRST, deliberately — see `RoutingPolicyGate`'s doc comment.
                Box::new(crate::gates::routing_policy::RoutingPolicyGate),
                Box::new(CapabilityGate),
```

and populate the ctx in `admit`:

```rust
            preferences: criteria.preferences.as_ref(),
```

Then fix every `SelectionCriteria { .. }` literal by adding `preferences: None,`:

Run: `cargo build --workspace 2>&1 | grep -c "E0063"`

- [x] **Step 5: Write the whole-service ordering test**

In `crates/gateway/src/selection.rs`'s `mod tests`:

```rust
/// AC5 — a candidate that is BOTH excluded by policy and circuit-open reports
/// the POLICY, and the selection does not become pausable.
///
/// This is the test the gate's position exists for. Moving `RoutingPolicyGate`
/// from first to last in `ModelSelectionService::new` flips the reported reason
/// to `CircuitOpen`, whose `gate_status()` is `Timed` — which would make
/// `all_gated_error` return `AllGated { resume_after: Some(..) }` and park a run
/// waiting on a breaker for a candidate the caller had already excluded.
#[test]
fn a_policy_exclusion_is_reported_ahead_of_an_open_breaker() {
    let config = test_config();
    let cb = test_cb();
    cb.can_execute("ollama:gemma3:27b");
    for _ in 0..5 {
        cb.record_failure("ollama:gemma3:27b");
    }
    assert!(!cb.can_execute("ollama:gemma3:27b"), "fixture needs it open");

    let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
    let lockout = crate::gates::lockout::ModelLockoutStore::new();
    let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

    let result = svc.select_all(&SelectionCriteria {
        capability: Capability::TextChat,
        model: None,
        router: None,
        chain: Some("chat_chain".to_string()),
        budget: None,
        input_tokens: None,
        input_tokens_pessimistic: None,
        preferences: Some(crate::types::request::RoutingPreferences {
            ignore: Some(crate::types::request::CandidateSet {
                routers: vec!["ollama".to_string()],
                models: vec![],
            }),
            ..Default::default()
        }),
    });

    let skipped = result
        .skipped
        .iter()
        .find(|s| s.model == "gemma3:27b")
        .expect("the excluded candidate must be recorded");
    assert!(
        matches!(skipped.reason, SkipReason::ExcludedByPolicy),
        "policy must win over the open breaker: {:?}",
        skipped.reason
    );
    assert!(
        matches!(skipped.reason.gate_status(), crate::skip_reason::GateStatus::Structural),
        "and it must contribute nothing to resume_after"
    );
}

/// AC5, the other half — excluding EVERY candidate is terminal, not a pause.
#[test]
fn excluding_every_candidate_is_terminal_not_pausable() {
    let config = test_config();
    let cb = test_cb();
    let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
    let lockout = crate::gates::lockout::ModelLockoutStore::new();
    let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

    let result = svc.select_all(&SelectionCriteria {
        capability: Capability::TextChat,
        model: None,
        router: None,
        chain: Some("chat_chain".to_string()),
        budget: None,
        input_tokens: None,
        input_tokens_pessimistic: None,
        preferences: Some(crate::types::request::RoutingPreferences {
            only: Some(crate::types::request::CandidateSet {
                routers: vec!["nonexistent".to_string()],
                models: vec![],
            }),
            ..Default::default()
        }),
    });

    assert!(result.all_candidates.is_empty());
    assert!(
        crate::engine::exhaustion::all_gated_error(&result.skipped, &[]).is_none(),
        "an all-structural exhaustion must NOT become AllGated — no deadline \
         and no human remedy makes an excluded candidate eligible"
    );
}
```

If `engine::exhaustion` is not visible from `selection.rs`'s tests, widen it to `pub(crate)` for the test only — it is already `pub(super)` inside `engine`.

- [x] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p sensei-gateway routing_policy a_policy_exclusion_is_reported excluding_every_candidate -- --nocapture`
Expected: PASS, 6 tests.

- [x] **Step 7: Mutation-check the gate position**

Temporarily move `Box::new(RoutingPolicyGate)` to the END of the gates vector in `selection.rs`.

Run: `cargo test -p sensei-gateway a_policy_exclusion_is_reported 2>&1 | grep "panicked at"`
Expected: a panic line mentioning "policy must win over the open breaker".

If NO panic appears, the test is not pinning the position and must be fixed before proceeding. Restore the original order and re-run to green.

- [x] **Step 8: Commit**

```bash
git add crates/gateway/src crates/kernel/src
git commit -m "feat(gateway): RoutingPolicyGate for only/ignore, registered first (SP-ROUTE-1 Task 3)"
```

---

## Task 4: The performance store and read port

**Files:**
- Create: `crates/gateway/src/gates/performance.rs`
- Modify: `crates/gateway/src/gates/mod.rs`
- Test: `crates/gateway/src/gates/performance.rs`

- [x] **Step 1: Write the failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn sample(latency_ms: u64, tps: Option<f64>, success: bool) -> Sample {
        Sample {
            at: Instant::now(),
            latency_ms,
            tokens_per_sec: tps,
            success,
        }
    }

    #[test]
    fn stats_average_latency_and_success_over_the_window() {
        let store = PerformanceStore::new(8, Duration::from_secs(60));
        store.record("r:m", sample(100, Some(10.0), true));
        store.record("r:m", sample(300, Some(30.0), true));
        store.record("r:m", sample(200, None, false));

        let s = store.stats("r:m").expect("three samples recorded");
        assert_eq!(s.samples, 3);
        assert!((s.mean_latency_ms - 200.0).abs() < f64::EPSILON);
        assert!((s.success_rate - 2.0 / 3.0).abs() < 1e-9);
    }

    /// Throughput is counted SEPARATELY from latency. An endpoint with plenty of
    /// latency samples but no token counts must not look measured to a
    /// throughput sort — otherwise it sorts on a mean over zero observations.
    #[test]
    fn throughput_samples_are_counted_separately_from_latency_samples() {
        let store = PerformanceStore::new(8, Duration::from_secs(60));
        store.record("r:m", sample(100, None, true));
        store.record("r:m", sample(100, None, true));
        store.record("r:m", sample(100, Some(50.0), true));

        let s = store.stats("r:m").unwrap();
        assert_eq!(s.samples, 3, "all three carry latency");
        assert_eq!(s.throughput_samples, 1, "only one carries token counts");
        assert!((s.mean_tokens_per_sec - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn the_ring_is_bounded_by_length() {
        let store = PerformanceStore::new(2, Duration::from_secs(60));
        for _ in 0..10 {
            store.record("r:m", sample(100, None, true));
        }
        assert_eq!(store.stats("r:m").unwrap().samples, 2, "capped at 2");
    }

    #[test]
    fn an_unknown_endpoint_has_no_stats() {
        let store = PerformanceStore::new(8, Duration::from_secs(60));
        assert!(store.stats("never:seen").is_none());
    }

    /// The no-op port used as the default in `ModelSelectionService::new`, so a
    /// caller that never wires performance gets today's behaviour exactly.
    #[test]
    fn the_null_port_never_reports_stats() {
        assert!(NoPerformance.stats("r:m").is_none());
    }
}
```

- [x] **Step 2: Run the test to verify it fails**

Run: `cargo test -p sensei-gateway performance -- --nocapture`
Expected: FAIL — `file not found for module 'performance'`, then unresolved names.

- [x] **Step 3: Write the store**

At the top of `crates/gateway/src/gates/performance.rs`:

```rust
use super::{AttemptOutcome, HealthRecorder};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Observed performance for one endpoint over the live window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EndpointStats {
    /// Live samples in the window. Every sample carries a latency.
    pub samples: u32,
    /// Of those, how many carried token counts. Counted separately because a
    /// throughput sort must not treat a latency-only history as measured.
    pub throughput_samples: u32,
    pub mean_latency_ms: f64,
    /// Mean over `throughput_samples`; `0.0` when there are none.
    pub mean_tokens_per_sec: f64,
    /// Over `samples`. Feeds the default strategy's reliability multiplier.
    pub success_rate: f64,
}

/// Synchronous read port. Selection is not async (`ModelSelectionService::select`),
/// so routing cannot query the async `GatewayStore` inline — this mirrors the
/// existing `EndpointHealthRead` / `RouterHealthRead` / `ModelLockoutRead` trio.
pub trait EndpointPerformanceRead: Send + Sync {
    fn stats(&self, endpoint: &str) -> Option<EndpointStats>;
}

/// The null port: the default wherever performance was never wired, so absent
/// wiring is byte-identical to before this slice.
pub struct NoPerformance;
impl EndpointPerformanceRead for NoPerformance {
    fn stats(&self, _endpoint: &str) -> Option<EndpointStats> {
        None
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct Sample {
    pub at: Instant,
    pub latency_ms: u64,
    /// `None` for an attempt that produced no token counts (a setup failure, or
    /// the stream-acquisition dispatch that fires before any tokens exist).
    pub tokens_per_sec: Option<f64>,
    pub success: bool,
}

/// In-memory rolling window per endpoint. Arc-backed + `Clone` so the read
/// reference held by selection and the owned copy inside the recorder share one
/// map — the same pattern as `ConnectionCooldownStore`.
#[derive(Clone)]
pub struct PerformanceStore {
    inner: Arc<Mutex<HashMap<String, VecDeque<Sample>>>>,
    max_samples: usize,
    max_age: Duration,
}

impl PerformanceStore {
    pub fn new(max_samples: usize, max_age: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            max_samples,
            max_age,
        }
    }

    pub(crate) fn record(&self, endpoint: &str, s: Sample) {
        let mut m = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let ring = m.entry(endpoint.to_string()).or_default();
        ring.push_back(s);
        while ring.len() > self.max_samples {
            ring.pop_front();
        }
    }

    /// Drop endpoints whose every sample has aged out, once over `cap`. Active
    /// endpoints are never dropped, so the cap is soft — the same bounded-memory
    /// discipline as `ConnectionCooldownStore::evict_expired_over_cap`.
    pub fn evict_stale_over_cap(&self, cap: usize) {
        let mut m = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if m.len() <= cap {
            return;
        }
        let now = Instant::now();
        let max_age = self.max_age;
        m.retain(|_, ring| {
            ring.iter()
                .any(|s| now.saturating_duration_since(s.at) <= max_age)
        });
    }
}

impl EndpointPerformanceRead for PerformanceStore {
    fn stats(&self, endpoint: &str) -> Option<EndpointStats> {
        let m = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let ring = m.get(endpoint)?;
        let now = Instant::now();
        let live: Vec<&Sample> = ring
            .iter()
            .filter(|s| now.saturating_duration_since(s.at) <= self.max_age)
            .collect();
        if live.is_empty() {
            return None;
        }

        let n = live.len() as f64;
        let mean_latency_ms = live.iter().map(|s| s.latency_ms as f64).sum::<f64>() / n;
        let success_rate = live.iter().filter(|s| s.success).count() as f64 / n;

        let tps: Vec<f64> = live.iter().filter_map(|s| s.tokens_per_sec).collect();
        let mean_tokens_per_sec = if tps.is_empty() {
            0.0
        } else {
            tps.iter().sum::<f64>() / tps.len() as f64
        };

        Some(EndpointStats {
            samples: live.len() as u32,
            throughput_samples: tps.len() as u32,
            mean_latency_ms,
            mean_tokens_per_sec,
            success_rate,
        })
    }
}

/// Write side. Never gates, so it always returns `None` — it contributes no
/// deadline to `AllGated.resume_after`.
pub struct PerformanceRecorder {
    store: PerformanceStore,
    eviction_cap: usize,
}

impl PerformanceRecorder {
    pub fn new(store: PerformanceStore, eviction_cap: usize) -> Self {
        Self {
            store,
            eviction_cap,
        }
    }
}

impl HealthRecorder for PerformanceRecorder {
    fn on_outcome(&self, o: &AttemptOutcome<'_>) -> Option<Instant> {
        let tokens_per_sec = match (o.output_tokens, o.duration_ms) {
            (Some(t), ms) if ms > 0 && t > 0 => Some(t as f64 * 1000.0 / ms as f64),
            _ => None,
        };
        self.store.record(
            o.endpoint,
            Sample {
                at: Instant::now(),
                latency_ms: o.duration_ms,
                tokens_per_sec,
                success: o.success,
            },
        );
        self.store.evict_stale_over_cap(self.eviction_cap);
        None
    }
}
```

- [x] **Step 4: Extend `AttemptOutcome` and `dispatch_outcome`**

In `crates/gateway/src/gates/mod.rs`, add `pub mod performance;` and two fields to `AttemptOutcome`:

```rust
    /// Wall time for this attempt, in ms. For a STREAM this is the time until
    /// the stream was obtained, not the time to complete it — see
    /// `engine::stream` for why the two are never pooled.
    pub duration_ms: u64,
    /// Output tokens, when the attempt produced a countable response. `None`
    /// for a setup failure or a stream-acquisition dispatch.
    pub output_tokens: Option<u32>,
```

In `crates/gateway/src/engine/mod.rs`, extend `dispatch_outcome` and `record_outcome`:

```rust
pub(super) fn dispatch_outcome(
    recorders: &[std::sync::Arc<dyn crate::gates::HealthRecorder>],
    endpoint: &str,
    router: &str,
    success: bool,
    error: Option<&crate::types::error::GatewayError>,
    duration_ms: u64,
    output_tokens: Option<u32>,
) -> Option<std::time::Instant> {
    let o = crate::gates::AttemptOutcome {
        endpoint,
        router,
        success,
        error,
        duration_ms,
        output_tokens,
    };
    recorders.iter().filter_map(|r| r.on_outcome(&o)).min()
}
```

Update `Gateway::record_outcome` to take and forward the same two arguments, then fix every call site the compiler names.

Run: `cargo build --workspace 2>&1 | grep -E "^error" | head -20`

For each call site, pass the duration already in scope (`start.elapsed().as_millis() as u64` at `engine/execute.rs:332`) and `response.usage.map(|u| u.output_tokens)` where a response exists, `None` otherwise. On the stream-acquisition dispatch at `engine/stream.rs:230`, pass the elapsed time since the attempt began and `None` for tokens.

- [x] **Step 5: Wire the store into `Gateway`**

In `crates/gateway/src/engine/mod.rs`, add a field beside `cooldown` / `model_lockout`:

```rust
    /// Rolling per-endpoint performance window: read side wired into selection
    /// (`sort: latency|throughput` and the default's reliability multiplier),
    /// write side is the `PerformanceRecorder` in `recorders` — both share this
    /// one store, exactly as `cooldown` and `model_lockout` do.
    performance: crate::gates::performance::PerformanceStore,
```

Construct it in `Gateway::new` **before** `recorders` (so the sink gets a clone of the same store), using the same window constants:

```rust
        let performance = crate::gates::performance::PerformanceStore::new(
            crate::resilience::DEFAULT_PERF_SAMPLES,
            crate::resilience::DEFAULT_PERF_WINDOW,
        );
```

Add it as a parameter to `build_recorders` and append the recorder to the returned vector:

```rust
        Arc::new(crate::gates::performance::PerformanceRecorder::new(
            performance.clone(),
            resilience.eviction_cap,
        )),
```

Update `Gateway::with_resilience` to pass `&self.performance` through.

In `crates/gateway/src/resilience.rs`:

```rust
/// Samples retained per endpoint in the rolling performance window.
pub const DEFAULT_PERF_SAMPLES: usize = 64;
/// How long a performance sample stays live.
pub const DEFAULT_PERF_WINDOW: Duration = Duration::from_secs(300);
```

- [x] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p sensei-gateway performance -- --nocapture`
Expected: PASS, 5 tests.

Run: `cargo test --workspace 2>&1 | tail -20`
Expected: all green — the new fields are ignored by every existing recorder.

- [x] **Step 7: Commit**

```bash
git add crates/gateway/src
git commit -m "feat(gateway): rolling per-endpoint performance store + recorder (SP-ROUTE-1 Task 4)"
```

---

## Task 5: The mid-stream failure correction

**Files:**
- Modify: `crates/gateway/src/engine/stream.rs`
- Test: `crates/gateway/src/engine/tests.rs`

**Context:** `dispatch_outcome(success = true)` fires at `stream.rs:230`, the instant a stream is *obtained*. The mid-stream error path (`stream.rs:250-259`) yields `StreamEvent::Error` and returns **without dispatching anything**, so a stream that dies halfway is recorded to every health recorder as a success. AC9 fixes that, and it is the one change in this slice that alters existing behaviour: mid-stream failures start counting toward the breaker, cooldown and lockout.

- [ ] **Step 1: Write the failing test**

In `crates/gateway/src/engine/tests.rs`:

First the adapter, placed beside `FakeStreamFailer` (which fails at *setup*; this one fails *after* the caller has committed):

```rust
/// Chat adapter whose `chat_stream` yields one good chunk then an error — a
/// stream that dies AFTER the caller has committed to it. Distinct from
/// `FakeStreamFailer`, which fails at setup and is already covered.
struct FakeStreamMidFailer {
    id: String,
}

impl crate::adapters::capability::Model for FakeStreamMidFailer {
    fn id(&self) -> &str {
        &self.id
    }
}

#[async_trait::async_trait]
impl crate::adapters::capability::ChatModel for FakeStreamMidFailer {
    async fn chat(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<crate::types::io::ChatResponse, GatewayError> {
        Ok(crate::types::io::ChatResponse::default())
    }

    async fn chat_stream(
        &self,
        _cfg: &RouterConfig,
        _req: &crate::types::io::ChatRequest,
    ) -> Result<
        std::pin::Pin<
            Box<
                dyn futures::Stream<Item = Result<crate::types::request::StreamChunk, GatewayError>>
                    + Send,
            >,
        >,
        GatewayError,
    > {
        use crate::types::request::StreamChunk;
        let chunks: Vec<Result<StreamChunk, GatewayError>> = vec![
            Ok(StreamChunk {
                content: "partial".to_string(),
                finish_reason: None,
                usage: None,
                tool_calls: Vec::new(),
            }),
            Err(GatewayError::ProviderError {
                adapter: self.id.clone(),
                message: "connection reset mid-stream".to_string(),
                status: Some(500),
            }),
        ];
        Ok(Box::pin(futures::stream::iter(chunks)))
    }
}
```

Then the test:

```rust
/// AC9 — a stream that fails after its first chunk must reach the health
/// recorders as a FAILURE.
///
/// Before this fix, `dispatch_outcome(success = true)` fired the instant the
/// stream was obtained (`stream.rs:230`) and the mid-stream error path returned
/// without dispatching at all, so an endpoint failing every stream halfway
/// looked perfectly healthy — and the default strategy's reliability multiplier
/// would then have weighted traffic toward it.
///
/// Two samples are recorded, but only ONE carries a verdict. Task 4's
/// `AttemptPhase` split means the acquisition dispatch (`StreamAcquired`)
/// contributes a latency and no verdict, because a completion outcome for the
/// same attempt always follows. So `success_rate` is **0.0**, not 0.5 — one
/// attempt, one vote, and it failed.
///
/// (An earlier draft of this plan said 0.5 here. That was written before the
/// phase split and is exactly the floor the split exists to remove: an endpoint
/// failing every stream mid-way must be able to reach 0.0, or Task 7's
/// reliability multiplier can never de-weight it.)
#[tokio::test]
async fn a_mid_stream_failure_is_recorded_as_a_failure() {
    let mut routers = HashMap::new();
    routers.insert(
        "mid".to_string(),
        RouterConfig {
            url: "http://localhost".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        },
    );
    let mut models = HashMap::new();
    models.insert(
        "mid".to_string(),
        ModelConfig {
            id: "mid".to_string(),
            api_model_id: None,
            provider: "mid".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window: 4096,
            max_output_tokens: 1024,
            pricing: None,
            catalog: None,
        },
    );
    let config = GatewayConfig {
        routers,
        models,
        chains: HashMap::new(),
        constraints: Default::default(),
        panels: Default::default(),
        consensus: Default::default(),
    };
    let cb = CircuitBreakerManager::new(CircuitBreakerConfig::default());
    let gw = Gateway::new(config, AdapterRegistry::new(), cb);
    gw.adapters
        .register_chat(Arc::new(FakeStreamMidFailer {
            id: "mid".to_string(),
        }))
        .await;

    let request = InferenceRequest {
        capability: Capability::TextChat,
        model: Some("mid".to_string()),
        router: Some("mid".to_string()),
        chain: None,
        payload: Payload::Chat {
            messages: vec![Message::text(MessageRole::User, "hi")],
            system: None,
            max_tokens: None,
            temperature: None,
            tools: Vec::new(),
        },
        budget: None,
        auth: None,
        panel: None,
        consensus: None,
        allow_fallback: true,
        credentials: Default::default(),
        routing: None,
    };

    let events = collect_stream(&gw, &request).await;
    assert!(
        matches!(events.last(), Some(StreamEvent::Error { .. })),
        "the fixture must actually fail mid-stream: {events:?}"
    );

    // Endpoint key is `format!("{router}:{model}")` — here both are "mid".
    let stats = gw
        .performance_stats("mid:mid")
        .expect("the attempt must have been recorded at all");
    assert!(
        stats.success_rate < 1.0,
        "a mid-stream failure must not be recorded as a success: {stats:?}"
    );
}
```

Add the read-back accessor to `crates/gateway/src/engine/mod.rs`:

```rust
    /// Observed performance for an endpoint (`"{router}:{model}"`), for
    /// operators and tests. `None` until the endpoint has a live sample.
    pub fn performance_stats(
        &self,
        endpoint: &str,
    ) -> Option<crate::gates::performance::EndpointStats> {
        use crate::gates::performance::EndpointPerformanceRead;
        self.performance.stats(endpoint)
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p sensei-gateway a_mid_stream_failure_is_recorded -- --nocapture 2>&1 | grep -E "panicked at|test result"`
Expected: a panic mentioning "must not be recorded as a success" — `success_rate` is `1.0`.

- [ ] **Step 3: Dispatch the failure**

In `crates/gateway/src/engine/stream.rs`, in the mid-stream error arm (currently lines 250-259), before the `return`:

```rust
                            Err(e) => {
                                // A stream that dies after first byte is a FAILURE, and
                                // the recorders have to hear about it. Before SP-ROUTE-1
                                // this path returned silently while the success dispatch
                                // at stream acquisition had already fired, so an endpoint
                                // failing every stream halfway looked perfectly healthy —
                                // which the default strategy's reliability multiplier
                                // would then have weighted traffic toward.
                                //
                                // The returned deadline is discarded: the caller has
                                // already committed to this stream and there is no
                                // fallback left to schedule. Surfacing it on the yielded
                                // `StreamEvent::Error` would change that event's payload
                                // and is deliberately out of scope (spec §12).
                                let _ = super::dispatch_outcome(
                                    &recorders,
                                    &crate::gates::AttemptOutcome {
                                        endpoint: &endpoint,
                                        router: &candidate.router,
                                        success: false,
                                        error: Some(&e),
                                        duration_ms: stream_start.elapsed().as_millis() as u64,
                                        output_tokens: usage_acc.map(|u| u.output_tokens),
                                        // The stream ENDED, badly. This is the attempt's one
                                        // and only verdict — the acquisition dispatch above
                                        // deliberately cast none — and its duration is
                                        // generation time, so it contributes no latency.
                                        phase: crate::gates::AttemptPhase::StreamCompleted,
                                    },
                                );
                                // Mid-stream failure: bytes already sent, so no
                                // fallback — surface and stop.
                                yield StreamEvent::Error {
                                    code: stream_error_code(&e),
                                    message: e.to_string(),
                                    resume_after: None,
                                };
                                return;
                            }
```

- [ ] **Step 4: Add the end-of-stream throughput dispatch**

Throughput only exists at completion. After `let tokens = usage_acc.unwrap_or_default();` (currently line 263), before the `InferenceCall` is built:

```rust
                    // Throughput is only knowable here. The acquisition dispatch at the
                    // top of this block recorded LATENCY (time until the stream started
                    // producing); this second dispatch records the generation rate. The
                    // two durations measure different spans, and `AttemptPhase` is what
                    // keeps them out of one mean — see `EndpointStats`.
                    let _ = super::dispatch_outcome(
                        &recorders,
                        &crate::gates::AttemptOutcome {
                            endpoint: &endpoint,
                            router: &candidate.router,
                            success: true,
                            error: None,
                            duration_ms: stream_start.elapsed().as_millis() as u64,
                            output_tokens: Some(tokens.output_tokens),
                            phase: crate::gates::AttemptPhase::StreamCompleted,
                        },
                    );
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p sensei-gateway a_mid_stream_failure_is_recorded -- --nocapture`
Expected: PASS.

Run: `cargo test -p sensei-gateway --lib 2>&1 | tail -5`
Expected: all green. If a breaker/cooldown test now fails because a mid-stream failure trips it, that is the intended behaviour change — update the test's expectation and note it in the commit message.

- [ ] **Step 6: Commit**

```bash
git add crates/gateway/src/engine
git commit -m "fix(gateway): record mid-stream failures as failures (SP-ROUTE-1 Task 5, AC9)

A stream that died after first byte dispatched no outcome, while the success
dispatch at stream acquisition had already fired — so an endpoint failing every
stream halfway looked perfectly healthy to the breaker, cooldown and lockout.
Mid-stream failures now count toward all three, which they never have.

Also adds the end-of-stream dispatch carrying output tokens, which is the only
point at which throughput is knowable."
```

---

## Task 6: `RandomSource` and the widened strategy seam

**Files:**
- Create: `crates/gateway/src/random.rs`
- Modify: `crates/gateway/src/lib.rs`, `crates/gateway/src/strategy.rs`, `crates/gateway/src/selection.rs`
- Test: `crates/gateway/src/random.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/gateway/src/random.rs` with the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Seeded ⇒ reproducible. This is what makes every weighted-strategy test
    /// in Task 7 deterministic rather than flaky.
    #[test]
    fn the_same_seed_yields_the_same_sequence() {
        let a = SplitMix64::seeded(42);
        let b = SplitMix64::seeded(42);
        let xs: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
        let ys: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
        assert_eq!(xs, ys);
    }

    #[test]
    fn different_seeds_diverge() {
        let a = SplitMix64::seeded(1);
        let b = SplitMix64::seeded(2);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn successive_draws_differ() {
        let r = SplitMix64::seeded(7);
        assert_ne!(r.next_u64(), r.next_u64(), "state must advance on &self");
    }

    /// Two processes must not make the same first routing decision, or
    /// "load balancing" would synchronise every worker.
    #[test]
    fn from_entropy_differs_across_instances() {
        let a = SplitMix64::from_entropy();
        let b = SplitMix64::from_entropy();
        assert_ne!(a.next_u64(), b.next_u64());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p sensei-gateway splitmix -- --nocapture`
Expected: FAIL — module not registered, then `cannot find type 'SplitMix64'`.

- [ ] **Step 3: Write the random source**

At the top of `crates/gateway/src/random.rs`:

```rust
use std::sync::atomic::{AtomicU64, Ordering};

/// Randomness for weighted routing, injected as a port.
///
/// `&self` with interior mutability, matching `CircuitBreakerManager`'s `Mutex`
/// style — it keeps `RoutingStrategy::order` taking `&self` and makes a seeded
/// source trivial to substitute in tests (the `FakeClock` precedent, SP-DATA-3).
pub trait RandomSource: Send + Sync {
    fn next_u64(&self) -> u64;
}

/// SplitMix64. Chosen over adding a crate: the workspace has no `rand`
/// dependency, and `uuid` (already a dependency, CSPRNG-backed for v4) supplies
/// the seed. Quality is ample for weighting a handful of candidates.
pub struct SplitMix64 {
    state: AtomicU64,
}

const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;

impl SplitMix64 {
    /// `const` so it can back a `static` default.
    pub const fn seeded(seed: u64) -> Self {
        Self {
            state: AtomicU64::new(seed),
        }
    }

    /// Seeded from a v4 UUID's bytes, so two processes diverge — otherwise
    /// every worker would make the same first choice and "load balancing" would
    /// synchronise rather than spread.
    pub fn from_entropy() -> Self {
        let b = uuid::Uuid::new_v4().into_bytes();
        let seed = u64::from_le_bytes(b[0..8].try_into().expect("16 bytes contains 8"));
        Self::seeded(seed)
    }
}

impl RandomSource for SplitMix64 {
    fn next_u64(&self) -> u64 {
        // `fetch_add` returns the PREVIOUS value and wraps on overflow, so add
        // GOLDEN back to get the advanced state SplitMix64 specifies.
        let z = self.state.fetch_add(GOLDEN, Ordering::Relaxed).wrapping_add(GOLDEN);
        let z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}
```

Register it in `crates/gateway/src/lib.rs`: `pub mod random;`

- [ ] **Step 4: Widen the strategy trait, keeping `PriorityStrategy` green**

Replace the top of `crates/gateway/src/strategy.rs`:

```rust
use crate::gates::performance::EndpointPerformanceRead;
use crate::random::RandomSource;
use crate::selection::SelectedModel;

/// What an ordering strategy may consult beyond the candidates themselves.
pub struct StrategyCtx<'a> {
    pub perf: &'a dyn EndpointPerformanceRead,
    pub rng: &'a dyn RandomSource,
    /// Minimum live samples before a metric sort considers a candidate measured.
    pub min_samples: u32,
}

/// Orders admitted candidates. The single ordering seam.
pub trait RoutingStrategy: Send + Sync {
    fn order(&self, admitted: &mut Vec<SelectedModel>, ctx: &StrategyCtx<'_>);
}

/// Strict ascending priority, stable. Retained as the explicit baseline every
/// other strategy is compared against in tests.
pub struct PriorityStrategy;
impl RoutingStrategy for PriorityStrategy {
    fn order(&self, admitted: &mut Vec<SelectedModel>, _ctx: &StrategyCtx<'_>) {
        admitted.sort_by_key(|m| m.priority);
    }
}
```

Update the two existing tests in that file to pass a ctx. Add a shared test helper at the bottom of the `mod tests` block:

```rust
    fn test_ctx<'a>(
        perf: &'a dyn EndpointPerformanceRead,
        rng: &'a dyn RandomSource,
    ) -> StrategyCtx<'a> {
        StrategyCtx {
            perf,
            rng,
            min_samples: 3,
        }
    }
```

and call e.g. `PriorityStrategy.order(&mut v, &test_ctx(&NoPerformance, &SplitMix64::seeded(1)));`

- [ ] **Step 5: Give `ModelSelectionService` the two ports, with defaults**

In `crates/gateway/src/selection.rs`, add fields and builders:

```rust
    /// Performance read port. Defaults to the null port so a caller that never
    /// wires performance behaves exactly as before this slice.
    perf: &'a dyn crate::gates::performance::EndpointPerformanceRead,
    /// Randomness for weighted ordering.
    rng: &'a dyn crate::random::RandomSource,
    min_samples: u32,
```

```rust
static NO_PERF: crate::gates::performance::NoPerformance =
    crate::gates::performance::NoPerformance;
/// A FIXED seed, deliberately. This default is reached only by callers that
/// construct the service directly — unit tests — where reproducibility is what
/// you want. Both production paths (`engine::execute`, `engine::stream`) pass
/// the gateway's entropy-seeded source via `with_random`.
static DEFAULT_RNG: crate::random::SplitMix64 = crate::random::SplitMix64::seeded(0x5EED_5EED);
```

In `new`, initialise `perf: &NO_PERF, rng: &DEFAULT_RNG, min_samples: 3`, and add:

```rust
    pub fn with_performance(
        mut self,
        perf: &'a dyn crate::gates::performance::EndpointPerformanceRead,
        min_samples: u32,
    ) -> Self {
        self.perf = perf;
        self.min_samples = min_samples;
        self
    }

    pub fn with_random(mut self, rng: &'a dyn crate::random::RandomSource) -> Self {
        self.rng = rng;
        self
    }
```

In `resolve_chain`, replace `self.strategy.order(&mut all_candidates);` with:

```rust
        let ctx = crate::strategy::StrategyCtx {
            perf: self.perf,
            rng: self.rng,
            min_samples: self.min_samples,
        };
        self.strategy.order(&mut all_candidates, &ctx);
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p sensei-gateway splitmix priority_strategy -- --nocapture`
Expected: PASS, 6 tests.

Run: `cargo test --workspace 2>&1 | tail -5`
Expected: all green — `PriorityStrategy` is still the registered strategy, so nothing reorders.

- [ ] **Step 7: Commit**

```bash
git add crates/gateway/src
git commit -m "feat(gateway): RandomSource + StrategyCtx, widening the ordering seam (SP-ROUTE-1 Task 6)"
```

---

## Task 7: `GroupedWeightedStrategy` — the new default

**Files:**
- Modify: `crates/gateway/src/strategy.rs`, `crates/gateway/src/selection.rs`
- Test: `crates/gateway/src/strategy.rs`

- [ ] **Step 1: Write the failing tests**

In `crates/gateway/src/strategy.rs`'s `mod tests`. Extend the `sm` helper to take a cost:

```rust
    fn sm_cost(model: &str, priority: u8, cost: Option<f64>) -> SelectedModel {
        let mut m = sm(model, priority);
        m.cost_estimate = cost.map(|estimated| crate::types::cost::CostEstimate {
            estimated,
            minimum: estimated,
            maximum: estimated,
            currency: "USD".to_string(),
            model: model.to_string(),
        });
        m
    }

    fn names(v: &[SelectedModel]) -> Vec<String> {
        v.iter().map(|m| m.model.clone()).collect()
    }
```

```rust
    /// AC1 — THE safety test. With distinct priorities every group is a
    /// singleton, so the weighted default must be indistinguishable from
    /// `PriorityStrategy` on every chain that exists today (`assemble()`
    /// reassigns ascending 1-based priorities by position, so it never ties).
    ///
    /// Across MANY seeds: a single seed would pass against a strategy that
    /// happened to shuffle the same way once.
    #[test]
    fn distinct_priorities_select_identically_to_priority_order_on_every_seed() {
        for seed in 0..256u64 {
            let mut weighted = vec![
                sm_cost("c", 3, Some(0.5)),
                sm_cost("a", 1, Some(9.0)),
                sm_cost("b", 2, None),
            ];
            let mut baseline = weighted.clone();

            let rng = SplitMix64::seeded(seed);
            GroupedWeightedStrategy.order(&mut weighted, &test_ctx(&NoPerformance, &rng));
            PriorityStrategy.order(&mut baseline, &test_ctx(&NoPerformance, &rng));

            assert_eq!(
                names(&weighted),
                names(&baseline),
                "seed {seed}: a distinct-priority chain must not be perturbed"
            );
        }
    }

    /// AC2 — the WEIGHTING, not merely "it varies". A test asserting only that
    /// order changes would pass against uniform shuffling.
    ///
    /// At 1 vs 3, weights are 1/1 and 1/9, so the cheap model leads 9 times in 10.
    #[test]
    fn a_tied_group_is_weighted_by_inverse_square_price() {
        let rng = SplitMix64::seeded(0xC0FFEE);
        let mut cheap_first = 0;
        const N: usize = 4000;
        for _ in 0..N {
            let mut v = vec![sm_cost("dear", 1, Some(3.0)), sm_cost("cheap", 1, Some(1.0))];
            GroupedWeightedStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));
            if v[0].model == "cheap" {
                cheap_first += 1;
            }
        }
        let share = cheap_first as f64 / N as f64;
        assert!(
            (share - 0.9).abs() < 0.03,
            "expected the 1-unit model to lead ~9/10, got {share}"
        );
    }

    /// AC3 — free beats every price, on every seed. `1/0²` is undefined; the
    /// limit is "always first", and that is what this pins.
    #[test]
    fn a_free_candidate_leads_its_group_on_every_seed() {
        for seed in 0..256u64 {
            let mut v = vec![
                sm_cost("priced", 1, Some(0.000_001)),
                sm_cost("free", 1, None),
            ];
            let rng = SplitMix64::seeded(seed);
            GroupedWeightedStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));
            assert_eq!(v[0].model, "free", "seed {seed}");
        }
    }

    /// AC3, other half — a candidate whose every observed attempt failed has
    /// weight zero and can never be DRAWN, so it goes last rather than being
    /// dropped. The breaker, not the router, is what removes a candidate.
    #[test]
    fn a_zero_reliability_candidate_goes_last_but_is_never_dropped() {
        struct DeadEndpoint;
        impl EndpointPerformanceRead for DeadEndpoint {
            fn stats(&self, endpoint: &str) -> Option<EndpointStats> {
                endpoint.ends_with(":dead").then_some(EndpointStats {
                    samples: 10,
                    throughput_samples: 0,
                    mean_latency_ms: 100.0,
                    mean_tokens_per_sec: 0.0,
                    success_rate: 0.0,
                })
            }
        }
        for seed in 0..64u64 {
            let mut v = vec![sm_cost("dead", 1, Some(0.1)), sm_cost("live", 1, Some(9.0))];
            let rng = SplitMix64::seeded(seed);
            GroupedWeightedStrategy.order(&mut v, &test_ctx(&DeadEndpoint, &rng));
            assert_eq!(names(&v), vec!["live", "dead"], "seed {seed}");
        }
    }

    /// Groups never interleave: a priority-2 candidate cannot precede a
    /// priority-1 one however cheap it is.
    #[test]
    fn a_cheaper_lower_priority_candidate_never_jumps_its_group() {
        for seed in 0..64u64 {
            let mut v = vec![sm_cost("dear_first", 1, Some(100.0)), sm_cost("cheap_second", 2, Some(0.01))];
            let rng = SplitMix64::seeded(seed);
            GroupedWeightedStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));
            assert_eq!(v[0].model, "dear_first", "seed {seed}");
        }
    }
```

Add `use crate::gates::performance::{EndpointPerformanceRead, EndpointStats, NoPerformance}; use crate::random::SplitMix64;` to the test module.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sensei-gateway grouped weighted free_candidate zero_reliability cheaper_lower -- --nocapture`
Expected: FAIL — `cannot find value 'GroupedWeightedStrategy'`.

- [ ] **Step 3: Write the strategy**

In `crates/gateway/src/strategy.rs`:

```rust
/// The default (SP-ROUTE-1). Groups by `priority`, orders groups ascending, and
/// within each group draws a full permutation weighted by `(1/cost²) × reliability`.
///
/// Grouping is what makes price weighting coherent here. OpenRouter weights
/// across PROVIDERS of one model, which are interchangeable; a gateway chain
/// holds different MODELS, which are not. Equal priority is the one signal an
/// operator has for "these are interchangeable", so that is the only scope
/// weighting is applied at. With distinct priorities every group is a singleton
/// and this is exactly `PriorityStrategy` — which is why it is safe as a default.
pub struct GroupedWeightedStrategy;

enum Weight {
    /// Costs nothing (or so little that `1/cost²` overflows, which is
    /// indistinguishable at that price). Always ahead of anything priced.
    Free,
    Draw(f64),
    /// Weight zero — unreachable by a draw, so it goes last.
    Zero,
}

fn classify(cost: f64, reliability: f64) -> Weight {
    if cost <= 0.0 {
        return Weight::Free;
    }
    let base = 1.0 / (cost * cost);
    if !base.is_finite() {
        return Weight::Free;
    }
    let w = base * reliability;
    if w > 0.0 { Weight::Draw(w) } else { Weight::Zero }
}

impl RoutingStrategy for GroupedWeightedStrategy {
    fn order(&self, admitted: &mut Vec<SelectedModel>, ctx: &StrategyCtx<'_>) {
        // Stable, so equal priorities end up adjacent IN CHAIN ORDER — which is
        // the input order the free/zero buckets below preserve.
        admitted.sort_by_key(|m| m.priority);

        let mut rest = std::mem::take(admitted);
        let mut out = Vec::with_capacity(rest.len());
        while !rest.is_empty() {
            let p = rest[0].priority;
            let split = rest
                .iter()
                .position(|m| m.priority != p)
                .unwrap_or(rest.len());
            let group: Vec<SelectedModel> = rest.drain(..split).collect();
            out.extend(order_group(group, ctx));
        }
        *admitted = out;
    }
}

fn order_group(group: Vec<SelectedModel>, ctx: &StrategyCtx<'_>) -> Vec<SelectedModel> {
    let mut free = Vec::new();
    let mut zero = Vec::new();
    let mut pool: Vec<(f64, SelectedModel)> = Vec::new();

    for m in group {
        let cost = m.cost_estimate.as_ref().map(|c| c.estimated).unwrap_or(0.0);
        let endpoint = format!("{}:{}", m.router, m.model);
        let reliability = ctx
            .perf
            .stats(&endpoint)
            .map(|s| s.success_rate)
            .unwrap_or(1.0);
        match classify(cost, reliability) {
            Weight::Free => free.push(m),
            Weight::Zero => zero.push(m),
            Weight::Draw(w) => pool.push((w, m)),
        }
    }

    let mut out = free;
    while !pool.is_empty() {
        let total: f64 = pool.iter().map(|(w, _)| *w).sum();
        let mut u = (ctx.rng.next_u64() as f64 / u64::MAX as f64) * total;
        let mut idx = pool.len() - 1;
        for (i, (w, _)) in pool.iter().enumerate() {
            u -= *w;
            if u <= 0.0 {
                idx = i;
                break;
            }
        }
        out.push(pool.remove(idx).1);
    }
    out.extend(zero);
    out
}
```

- [ ] **Step 4: Make it the registered default**

In `crates/gateway/src/selection.rs`, `ModelSelectionService::new`:

```rust
            strategy: Box::new(crate::strategy::GroupedWeightedStrategy),
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p sensei-gateway --lib 2>&1 | tail -5`
Expected: all green. AC1 is why the existing chain-order tests still pass.

- [ ] **Step 6: Mutation-check the two claims that matter**

(a) Delete the `Weight::Free => free.push(m)` arm's priority by changing `if cost <= 0.0 { return Weight::Free; }` to `if cost < 0.0 { ... }`.

Run: `cargo test -p sensei-gateway a_free_candidate_leads 2>&1 | grep "panicked at"`
Expected: a panic. Restore.

(b) Change `admitted.sort_by_key(|m| m.priority)` in `GroupedWeightedStrategy::order` to a no-op.

Run: `cargo test -p sensei-gateway distinct_priorities_select_identically 2>&1 | grep "panicked at"`
Expected: a panic. Restore, and re-run the suite to green.

- [ ] **Step 7: Commit**

```bash
git add crates/gateway/src
git commit -m "feat(gateway): GroupedWeightedStrategy as the default (SP-ROUTE-1 Task 7, AC1-AC3)

Weighting is scoped to equal-priority groups because chain entries are different
models, not interchangeable providers of one model. With distinct priorities
every group is a singleton, so this is byte-identical to PriorityStrategy on
every chain assemble() produces."
```

---

## Task 8: `sort: price`

**Files:**
- Modify: `crates/gateway/src/strategy.rs`
- Test: `crates/gateway/src/strategy.rs`

- [ ] **Step 1: Write the failing test**

```rust
    /// AC8 — `sort: price` orders across the WHOLE chain and deliberately
    /// overrides `priority`. That is what "load balancing switches off and the
    /// router tries providers strictly in that order" means.
    #[test]
    fn price_sort_overrides_priority_across_the_whole_chain() {
        let rng = SplitMix64::seeded(1);
        let mut v = vec![
            sm_cost("dear_but_first", 1, Some(10.0)),
            sm_cost("cheap_but_last", 9, Some(0.1)),
            sm_cost("free_but_middle", 5, None),
        ];
        PriceStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));
        assert_eq!(
            names(&v),
            vec!["free_but_middle", "cheap_but_last", "dear_but_first"]
        );
    }

    /// Equal prices fall back to authored priority, so the sort is total.
    #[test]
    fn price_sort_breaks_ties_on_priority() {
        let rng = SplitMix64::seeded(1);
        let mut v = vec![sm_cost("second", 2, Some(1.0)), sm_cost("first", 1, Some(1.0))];
        PriceStrategy.order(&mut v, &test_ctx(&NoPerformance, &rng));
        assert_eq!(names(&v), vec!["first", "second"]);
    }
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p sensei-gateway price_sort -- --nocapture`
Expected: FAIL — `cannot find value 'PriceStrategy'`.

- [ ] **Step 3: Write the strategy**

```rust
/// `sort: price` — ascending estimated cost across every candidate, free first,
/// ties broken by authored priority so the order is total.
pub struct PriceStrategy;

impl RoutingStrategy for PriceStrategy {
    fn order(&self, admitted: &mut Vec<SelectedModel>, _ctx: &StrategyCtx<'_>) {
        admitted.sort_by(|a, b| {
            let ca = a.cost_estimate.as_ref().map(|c| c.estimated).unwrap_or(0.0);
            let cb = b.cost_estimate.as_ref().map(|c| c.estimated).unwrap_or(0.0);
            ca.partial_cmp(&cb)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.priority.cmp(&b.priority))
        });
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test -p sensei-gateway price_sort -- --nocapture`
Expected: PASS, 2 tests.

- [ ] **Step 5: Commit**

```bash
git add crates/gateway/src/strategy.rs
git commit -m "feat(gateway): PriceStrategy for sort=price (SP-ROUTE-1 Task 8, AC8)"
```

---

## Task 9: `sort: latency` / `sort: throughput`

**Files:**
- Modify: `crates/gateway/src/strategy.rs`, `crates/gateway/src/resilience.rs`
- Test: `crates/gateway/src/strategy.rs`

- [ ] **Step 1: Write the failing tests**

```rust
    struct FixedStats(&'static [(&'static str, u32, f64, f64)]);
    impl EndpointPerformanceRead for FixedStats {
        fn stats(&self, endpoint: &str) -> Option<EndpointStats> {
            self.0
                .iter()
                .find(|(e, ..)| endpoint.ends_with(e))
                .map(|(_, samples, latency, tps)| EndpointStats {
                    samples: *samples,
                    throughput_samples: *samples,
                    mean_latency_ms: *latency,
                    mean_tokens_per_sec: *tps,
                    success_rate: 1.0,
                })
        }
    }

    /// AC7 — with NO observations, a metric sort is exactly priority order.
    /// Matches `IntraTierStrategy::is_dynamic`'s existing convention of
    /// degrading to `Priority` rather than inventing numbers.
    #[test]
    fn a_metric_sort_with_no_observations_is_priority_order() {
        let rng = SplitMix64::seeded(1);
        let mut v = vec![sm_cost("b", 2, None), sm_cost("a", 1, None)];
        MetricStrategy::latency(3).order(&mut v, &test_ctx(&NoPerformance, &rng));
        assert_eq!(names(&v), vec!["a", "b"]);
    }

    /// AC7 — with FULL observations it is a complete metric sort.
    #[test]
    fn latency_sort_orders_measured_candidates_ascending() {
        let perf = FixedStats(&[(":slow", 5, 900.0, 10.0), (":fast", 5, 100.0, 90.0)]);
        let rng = SplitMix64::seeded(1);
        let mut v = vec![sm_cost("slow", 1, None), sm_cost("fast", 2, None)];
        MetricStrategy::latency(3).order(&mut v, &test_ctx(&perf, &rng));
        assert_eq!(names(&v), vec!["fast", "slow"], "fast must overtake despite priority 2");
    }

    #[test]
    fn throughput_sort_orders_measured_candidates_descending() {
        let perf = FixedStats(&[(":slow", 5, 900.0, 10.0), (":fast", 5, 100.0, 90.0)]);
        let rng = SplitMix64::seeded(1);
        let mut v = vec![sm_cost("slow", 1, None), sm_cost("fast", 2, None)];
        MetricStrategy::throughput(3).order(&mut v, &test_ctx(&perf, &rng));
        assert_eq!(names(&v), vec!["fast", "slow"]);
    }

    /// AC7, the interesting half — an UNMEASURED candidate holds its index.
    /// It is neither promoted nor demoted, because "observed 400ms" and "never
    /// measured" are not comparable quantities.
    #[test]
    fn an_unmeasured_candidate_holds_its_index() {
        // Only positions 0 and 2 are measured; position 1 is not.
        let perf = FixedStats(&[(":slow", 5, 900.0, 1.0), (":fast", 5, 100.0, 1.0)]);
        let rng = SplitMix64::seeded(1);
        let mut v = vec![
            sm_cost("slow", 1, None),
            sm_cost("unmeasured", 2, None),
            sm_cost("fast", 3, None),
        ];
        MetricStrategy::latency(3).order(&mut v, &test_ctx(&perf, &rng));
        assert_eq!(
            names(&v),
            vec!["fast", "unmeasured", "slow"],
            "the measured pair swaps within slots 0 and 2; the unmeasured one does not move"
        );
    }

    /// Below `min_samples` a candidate is NOT measured, so a single lucky
    /// observation cannot reorder a chain.
    #[test]
    fn a_candidate_below_min_samples_is_not_measured() {
        let perf = FixedStats(&[(":fast", 1, 10.0, 99.0)]);
        let rng = SplitMix64::seeded(1);
        let mut v = vec![sm_cost("slow", 1, None), sm_cost("fast", 2, None)];
        MetricStrategy::latency(3).order(&mut v, &test_ctx(&perf, &rng));
        assert_eq!(names(&v), vec!["slow", "fast"], "1 sample < min 3 ⇒ no reorder");
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sensei-gateway metric_sort latency_sort throughput_sort unmeasured min_samples -- --nocapture`
Expected: FAIL — `cannot find type 'MetricStrategy'`.

- [ ] **Step 3: Write the strategy**

```rust
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Metric {
    Latency,
    Throughput,
}

/// `sort: latency | throughput`.
///
/// **Reorders only what it knows about.** A fresh process has no observations,
/// and "observed 400ms" against "never measured" are not comparable quantities
/// — so rather than impute a value, this sorts the MEASURED subset among the
/// indices that subset already occupies. Unmeasured candidates never move.
///
/// Three properties, no magic constants: zero observations ⇒ pure priority
/// order; full observations ⇒ a complete metric sort; partial ⇒ a monotone
/// interpolation between them.
pub struct MetricStrategy {
    metric: Metric,
    min_samples: u32,
}

impl MetricStrategy {
    pub fn latency(min_samples: u32) -> Self {
        Self { metric: Metric::Latency, min_samples }
    }
    pub fn throughput(min_samples: u32) -> Self {
        Self { metric: Metric::Throughput, min_samples }
    }

    /// `None` ⇒ not measured for THIS metric. Throughput reads
    /// `throughput_samples`, not `samples`: an endpoint with plenty of latency
    /// observations but no token counts would otherwise sort on a mean over
    /// nothing.
    fn value(&self, m: &SelectedModel, ctx: &StrategyCtx<'_>) -> Option<f64> {
        let s = ctx.perf.stats(&format!("{}:{}", m.router, m.model))?;
        match self.metric {
            Metric::Latency => (s.samples >= self.min_samples).then_some(s.mean_latency_ms),
            // Negated so ascending sort == descending throughput.
            Metric::Throughput => {
                (s.throughput_samples >= self.min_samples).then_some(-s.mean_tokens_per_sec)
            }
        }
    }
}

impl RoutingStrategy for MetricStrategy {
    fn order(&self, admitted: &mut Vec<SelectedModel>, ctx: &StrategyCtx<'_>) {
        admitted.sort_by_key(|m| m.priority); // the baseline every unmeasured candidate keeps

        let slots: Vec<usize> = admitted
            .iter()
            .enumerate()
            .filter(|(_, m)| self.value(m, ctx).is_some())
            .map(|(i, _)| i)
            .collect();

        let mut subset: Vec<SelectedModel> = slots.iter().map(|&i| admitted[i].clone()).collect();
        subset.sort_by(|a, b| {
            let (va, vb) = (self.value(a, ctx), self.value(b, ctx));
            va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
        });
        for (&slot, m) in slots.iter().zip(subset) {
            admitted[slot] = m;
        }
    }
}
```

- [ ] **Step 4: Add the tunable**

In `crates/gateway/src/resilience.rs`, add to `ResilienceConfig`:

```rust
    /// Minimum live samples before `sort: latency|throughput` treats a candidate
    /// as measured. Three is the smallest count at which a mean is not simply
    /// the last observation; below it the sort degrades to priority order, which
    /// is the documented fallback anyway.
    pub min_samples: u32,
```

and to `Default`: `min_samples: 3,`. Extend `default_matches_todays_hardcoded_behavior` with `assert_eq!(r.min_samples, 3);`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p sensei-gateway metric_sort latency_sort throughput_sort unmeasured min_samples -- --nocapture`
Expected: PASS, 5 tests.

- [ ] **Step 6: Commit**

```bash
git add crates/gateway/src
git commit -m "feat(gateway): MetricStrategy for sort=latency|throughput (SP-ROUTE-1 Task 9, AC7)"
```

---

## Task 10: `order`, precedence, and engine wiring

**Files:**
- Modify: `crates/gateway/src/selection.rs`, `crates/gateway/src/engine/execute.rs`, `crates/gateway/src/engine/stream.rs`
- Test: `crates/gateway/src/selection.rs`

- [ ] **Step 1: Write the failing tests**

```rust
    /// AC6 — `order` sequences the candidates it names; unmatched candidates
    /// follow as FALLBACKS rather than being dropped. `only` is the knob that
    /// actually restricts.
    #[test]
    fn order_sequences_named_candidates_and_keeps_the_rest_as_fallbacks() {
        use crate::types::request::{CandidateRef, RoutingPreferences};
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        // chat_chain is [gemma3:27b (p1), claude-haiku (p2)]. Name the SECOND
        // one first — if `order` did nothing, priority would keep gemma first.
        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: Some(RoutingPreferences {
                order: Some(vec![CandidateRef {
                    router: None,
                    model: Some("claude-haiku".to_string()),
                }]),
                ..Default::default()
            }),
        });

        let got: Vec<String> = result.all_candidates.iter().map(|c| c.model.clone()).collect();
        assert_eq!(
            got,
            vec!["claude-haiku".to_string(), "gemma3:27b".to_string()],
            "the named candidate leads; the unnamed one follows as a fallback"
        );
    }

    /// A wildcard ref names every candidate on a router.
    #[test]
    fn an_order_ref_with_only_a_router_is_a_wildcard_over_its_models() {
        use crate::types::request::{CandidateRef, RoutingPreferences};
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: None,
            input_tokens: None,
            input_tokens_pessimistic: None,
            preferences: Some(RoutingPreferences {
                order: Some(vec![CandidateRef {
                    router: Some("anthropic".to_string()),
                    model: None,
                }]),
                ..Default::default()
            }),
        });
        assert_eq!(result.all_candidates[0].router, "anthropic");
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sensei-gateway order_sequences an_order_ref_with_only -- --nocapture`
Expected: FAIL — the assertion, with `gemma3:27b` still leading.

- [ ] **Step 3: Resolve the strategy per request and apply `order`**

In `crates/gateway/src/selection.rs`, remove the `strategy` field from the struct and its initialisation, and add:

```rust
    /// Strategy for THIS request (§4.2): an explicit `sort` replaces the
    /// default, and `order` is layered on top of whichever ran.
    fn strategy_for(&self, criteria: &SelectionCriteria) -> Box<dyn RoutingStrategy> {
        use crate::types::request::SortKey;
        match criteria.preferences.as_ref().and_then(|p| p.sort) {
            Some(SortKey::Price) => Box::new(crate::strategy::PriceStrategy),
            Some(SortKey::Latency) => {
                Box::new(crate::strategy::MetricStrategy::latency(self.min_samples))
            }
            Some(SortKey::Throughput) => {
                Box::new(crate::strategy::MetricStrategy::throughput(self.min_samples))
            }
            None => Box::new(crate::strategy::GroupedWeightedStrategy),
        }
    }
```

In `resolve_chain`, replace the ordering block with:

```rust
        let ctx = crate::strategy::StrategyCtx {
            perf: self.perf,
            rng: self.rng,
            min_samples: self.min_samples,
        };
        self.strategy_for(criteria).order(&mut all_candidates, &ctx);

        // `order` is layered ON TOP of whichever strategy ran (§4.2). A STABLE
        // sort by matched-ref index gives exactly the specified semantics:
        // named candidates lead in ref order, candidates sharing a ref keep the
        // strategy's relative order, and unmatched candidates (rank usize::MAX)
        // follow as fallbacks in the strategy's order.
        if let Some(refs) = criteria.preferences.as_ref().and_then(|p| p.order.as_ref()) {
            all_candidates.sort_by_key(|m| {
                refs.iter()
                    .position(|r| {
                        r.router.as_deref().is_none_or(|x| x == m.router)
                            && r.model.as_deref().is_none_or(|x| x == m.model)
                    })
                    .unwrap_or(usize::MAX)
            });
        }
```

- [ ] **Step 4: Wire the engine's ports through**

In `crates/gateway/src/engine/execute.rs`, add `preferences: request.routing.clone(),` to the `SelectionCriteria` literal, and extend the service construction:

```rust
        let svc = ModelSelectionService::new(
            &config,
            &self.circuit_breaker,
            &self.cooldown,
            &self.model_lockout,
        )
        .with_performance(&self.performance, self.resilience_min_samples)
        .with_random(self.rng.as_ref());
```

Do the same in `crates/gateway/src/engine/stream.rs`.

Add the two `Gateway` fields this needs:

```rust
    /// Entropy-seeded so two workers do not make the same first choice.
    rng: Arc<crate::random::SplitMix64>,
    /// Mirrors `ResilienceConfig::min_samples` for the selection path.
    resilience_min_samples: u32,
```

initialised in `Gateway::new` as `rng: Arc::new(crate::random::SplitMix64::from_entropy())` and `resilience_min_samples: crate::resilience::ResilienceConfig::default().min_samples`, and updated in `with_resilience` from the passed config.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p sensei-gateway order_sequences an_order_ref_with_only -- --nocapture`
Expected: PASS, 2 tests.

Run: `cargo test --workspace 2>&1 | tail -5`
Expected: all green.

- [ ] **Step 6: Commit**

```bash
git add crates/gateway/src
git commit -m "feat(gateway): explicit order + per-request strategy resolution (SP-ROUTE-1 Task 10, AC6)"
```

---

## Task 11: Observability

**Files:**
- Modify: `crates/gateway/src/selection.rs`, `crates/kernel/src/types/trace.rs`, `crates/gateway/src/engine/execute.rs`
- Test: `crates/gateway/src/selection.rs`

- [ ] **Step 1: Write the failing test**

```rust
    /// AC10 — a weighted selection must be explainable. Without the applied
    /// strategy and the per-candidate weights, "why did it pick the expensive
    /// one" has no answer at all in a bug report.
    #[test]
    fn a_selection_records_the_strategy_and_the_weights_behind_it() {
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: None,
            input_tokens: Some(1000),
            input_tokens_pessimistic: None,
            preferences: None,
        });

        let d = result.decision.as_ref().expect("every selection records one");
        assert_eq!(d.strategy, "grouped_weighted");
        assert_eq!(
            d.order.len(),
            result.all_candidates.len(),
            "the recorded order must cover every admitted candidate"
        );
        assert_eq!(d.order[0].endpoint, {
            let c = &result.all_candidates[0];
            format!("{}:{}", c.router, c.model)
        });
    }

    /// A `sort` names itself, so a trace distinguishes "the caller asked for
    /// price" from "the default happened to pick the cheap one".
    #[test]
    fn an_explicit_sort_is_named_in_the_decision() {
        use crate::types::request::{RoutingPreferences, SortKey};
        let config = test_config();
        let cb = test_cb();
        let cooldown = crate::gates::cooldown::ConnectionCooldownStore::new();
        let lockout = crate::gates::lockout::ModelLockoutStore::new();
        let svc = ModelSelectionService::new(&config, &cb, &cooldown, &lockout);

        let result = svc.select_all(&SelectionCriteria {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: Some("chat_chain".to_string()),
            budget: None,
            input_tokens: Some(1000),
            input_tokens_pessimistic: None,
            preferences: Some(RoutingPreferences {
                sort: Some(SortKey::Price),
                ..Default::default()
            }),
        });
        assert_eq!(result.decision.unwrap().strategy, "price");
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p sensei-gateway a_selection_records_the_strategy an_explicit_sort_is_named -- --nocapture`
Expected: FAIL — `no field 'decision' on type 'SelectionResult'`.

- [ ] **Step 3: Record the decision**

In `crates/kernel/src/types/trace.rs`:

```rust
/// Why the candidates came out in the order they did (SP-ROUTE-1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingDecision {
    /// `grouped_weighted` | `price` | `latency` | `throughput`.
    pub strategy: String,
    /// Whether a metric sort degraded for want of samples.
    pub degraded: bool,
    pub order: Vec<RoutedCandidate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutedCandidate {
    /// `"{router}:{model}"`.
    pub endpoint: String,
    pub priority: u8,
    /// Estimated cost; `None` for an unpriced (free) candidate.
    pub cost: Option<f64>,
    /// Windowed success rate; `None` when unmeasured.
    pub reliability: Option<f64>,
    /// The draw weight, when the weighted default ran.
    pub weight: Option<f64>,
}
```

In `crates/gateway/src/selection.rs`, add `pub decision: Option<RoutingDecision>` to `SelectionResult`, populate it in `resolve_chain` after ordering, and set `decision: None` in the other `SelectionResult` literals (direct/not-found paths). Name the strategy from the same match `strategy_for` uses so the two cannot drift:

```rust
        fn strategy_name(prefs: Option<&RoutingPreferences>) -> &'static str {
            use crate::types::request::SortKey;
            match prefs.and_then(|p| p.sort) {
                Some(SortKey::Price) => "price",
                Some(SortKey::Latency) => "latency",
                Some(SortKey::Throughput) => "throughput",
                None => "grouped_weighted",
            }
        }
```

In `crates/gateway/src/engine/execute.rs`, attach `result.decision` to the `ExecutionTrace` built for the call.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p sensei-gateway a_selection_records_the_strategy an_explicit_sort_is_named -- --nocapture`
Expected: PASS, 2 tests.

- [ ] **Step 5: Commit**

```bash
git add crates/gateway/src crates/kernel/src
git commit -m "feat(gateway): record the routing decision on the trace (SP-ROUTE-1 Task 11, AC10)"
```

---

## Task 12: Docs and final verification

**Files:**
- Modify: `docs/features/` (routing docs), `README.md` if it documents request fields
- Modify: `docs/CHECKPOINT.md`

- [ ] **Step 1: Find every doc surface that describes routing or request fields**

Run: `rg --no-ignore -g '!target' -g '!site/node_modules' -l 'allow_fallback|fallback chain|priority' docs/ README.md`

Update each hit that enumerates request fields or describes selection order.

- [ ] **Step 2: Document the tied-chain consequence loudly**

In the routing feature doc, add:

```markdown
### Determinism

With distinct chain priorities — which is every chain `assemble()` produces —
routing is deterministic and identical to prior releases.

**If you author a chain that gives two entries the SAME priority**, those
entries become a load-balanced pool: two fresh runs may pick different models.
Orchestrator *resume* is unaffected (a completed model call replays from its
journal memo and never re-enters selection), but two independent runs of the
same graph may now diverge. That is the feature; author ties deliberately.
```

- [ ] **Step 3: Run the full verification**

```bash
cargo fmt --all
cargo test --workspace 2>&1 | tail -20
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

Expected: tests all green with a real exit code of 0 (`echo $?` immediately after, not through a pipe); clippy and fmt silent.

Confirm no test was silently skipped:

Run: `cargo test --workspace 2>&1 | grep -E "test result" | awk -F'[;.]' '{print}'`
Expected: `failed` is 0 on every line.

- [ ] **Step 4: Verify each acceptance criterion has a passing test**

| AC | Test |
|---|---|
| AC1 | `distinct_priorities_select_identically_to_priority_order_on_every_seed` |
| AC2 | `a_tied_group_is_weighted_by_inverse_square_price` |
| AC3 | `a_free_candidate_leads_its_group_on_every_seed`, `a_zero_reliability_candidate_goes_last_but_is_never_dropped` |
| AC4 | `only_is_and_across_non_empty_axes`, `ignore_is_or_across_non_empty_axes`, `ignore_subtracts_from_only` |
| AC5 | `a_policy_exclusion_is_reported_ahead_of_an_open_breaker`, `excluding_every_candidate_is_terminal_not_pausable` |
| AC6 | `order_sequences_named_candidates_and_keeps_the_rest_as_fallbacks` |
| AC7 | `a_metric_sort_with_no_observations_is_priority_order`, `an_unmeasured_candidate_holds_its_index` |
| AC8 | `price_sort_overrides_priority_across_the_whole_chain` |
| AC9 | `a_mid_stream_failure_is_recorded_as_a_failure` |
| AC10 | `a_selection_records_the_strategy_and_the_weights_behind_it` |
| AC11 | Step 3 |

Run each named test individually and confirm it passes. Any AC without a green test is unfinished work, not a rounding error.

- [ ] **Step 5: Update the checkpoint**

Overwrite `docs/CHECKPOINT.md` (one current entry, under 40 lines) with the slice state, then run `/sensei:checkpoint`.

- [ ] **Step 6: Commit**

```bash
git add docs
git commit -m "docs: SP-ROUTE-1 routing preferences + tied-chain determinism note (Task 12)"
```

- [ ] **Step 7: Whole-slice review**

Run `/sensei:review` over the full diff before opening the develop→main PR. Per the SP-6 lesson, review must not be the last thing a long session does — if the session is running low, stop and review in a fresh one rather than landing unreviewed commits.

---

## Self-review notes

**Spec coverage:** §4 → Task 1; §4.1 → Task 3; §4.2 → Task 10; §5.0 → Tasks 2–3; §5.1 → Task 7; §5.2 → Task 8; §5.3 → Task 9; §5.4 → Task 6; §6 → Task 4; §7 → Task 5; §8 → no task required (the boundary is enforced by *not* touching `build_request`; the docs note in Task 12 records it); §9 → Task 11; §10 AC11 → Task 12.

**Known sequencing constraint:** Task 7's reliability multiplier reads the port built in Task 4, and Task 5's test reads it back — so 4 must precede 5 and 7. Task 10 depends on 7, 8 and 9 all existing, since `strategy_for` matches every `SortKey` arm.
