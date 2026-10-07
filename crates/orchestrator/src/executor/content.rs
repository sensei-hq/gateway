//! The CAS content boundary on the executor: `split_output` (inline-vs-ref by
//! `cas_threshold`, §7.4) and `materialize` (lazy ref fetch). Split out of
//! `super` for readability; both are `impl Executor` methods sharing its state.

use orchestrator_core::{ContentRef, EffectOutput, OrchestratorError};

use super::Executor;

/// SP-DATA-5 + AG-12: the single conversion from what the gateway reported on a
/// response to the journal's usage record (`orchestrator_core::TokenUsage`, defined
/// without a `kernel` dependency). Lives beside `model_output`, the sibling OUTPUT-side
/// chokepoint, because both exist so a new model-call producer picks up the conversion
/// by construction rather than by remembering to copy it. Producers reach it only through
/// `Fold::recorded_usage`, which supplies `priced`.
///
/// A free function, not a `From` impl: both types are foreign to this crate, so the
/// orphan rule forbids one. A field added to the JOURNALED side fails to compile HERE —
/// one fix — instead of being silently dropped at some of the producers.
///
/// `priced` is "this run has a money cap in force". Only then is the cost ledgered, so a
/// run without one journals exactly what it did before AG-12. Under a cap a response
/// without a usable cost still converts (with `cost_micro_usd: None`) — the chokepoint
/// has already refused such a call before any producer journals it.
pub(super) fn recorded_usage(
    response: &kernel::types::request::InferenceResponse,
    priced: bool,
) -> Option<orchestrator_core::TokenUsage> {
    let u = response.usage.as_ref()?;
    Some(orchestrator_core::TokenUsage {
        input_tokens: u.input_tokens,
        output_tokens: u.output_tokens,
        total_tokens: u.total_tokens,
        cost_micro_usd: if priced {
            response.actual_cost.as_ref().and_then(cost_micro_usd)
        } else {
            None
        },
    })
}

/// AG-12: a gateway-reported [`Cost`](kernel::types::cost::Cost) in integer micro-dollars,
/// rounded UP — or `None` when it cannot be trusted as a USD figure.
///
/// The ONE place a float price becomes a ledger integer, used by both the live meter and
/// the journaled record so the two can never disagree about one call. Rounding up biases
/// the ledger high: a cap can only be reached early, never overshot by accumulated
/// truncation. `None` — a non-USD currency, or a non-finite or negative total — is
/// treated by the chokepoint exactly like a missing cost: refused under a money cap.
pub(super) fn cost_micro_usd(cost: &kernel::types::cost::Cost) -> Option<u64> {
    if !cost.currency.eq_ignore_ascii_case("USD")
        || !cost.total_cost.is_finite()
        || cost.total_cost < 0.0
    {
        return None;
    }
    Some(ceil_micro(
        cost.total_cost * orchestrator_core::MICRO_USD_PER_USD as f64,
    ))
}

/// AG-12: round a non-negative micro-dollar figure UP to an integer, ignoring float
/// representation noise below a thousandth of a micro-dollar.
///
/// A plain `ceil` is wrong here in a way that matters: the gateway's `f64` total for a
/// call priced at exactly 21 000 micro-dollars comes back as `21000.000000000004`, and
/// `ceil` charges 21 001 — one phantom micro-dollar on EVERY call, which compounds across
/// a run and makes the ledger disagree with arithmetic an operator can check. Rounding to
/// the nearest nano-dollar first removes representation noise (relative error ~1e-16)
/// while still charging any genuine sub-micro fraction of a nano-dollar or more as a
/// whole micro-dollar — still biased high, just not by noise.
///
/// `as` saturates on an out-of-range float, so an absurd figure pins the result at
/// `u64::MAX` (which pauses the run) rather than wrapping it toward zero.
pub(super) fn ceil_micro(micro: f64) -> u64 {
    ((micro * 1000.0).round() / 1000.0).ceil() as u64
}

impl Executor {
    /// Split an effect output for the journal (§7.4): if a `ContentStore` is
    /// wired and the serialized output exceeds `cas_threshold`, `put` the bytes
    /// into the CAS and return a [`ContentRef`] (identical content dedupes to one
    /// digest); otherwise carry the value inline. Keeps the durable journal a
    /// lean control-flow log while large payloads live once in the CAS.
    pub(super) async fn split_output(
        &self,
        output: &serde_json::Value,
    ) -> Result<EffectOutput, OrchestratorError> {
        // No CAS wired ⇒ everything stays inline (the slice-1/2 behavior).
        let Some(content) = &self.content else {
            return Ok(EffectOutput::Inline(output.clone()));
        };
        let bytes = serde_json::to_vec(output)?;
        if bytes.len() <= self.cas_threshold {
            return Ok(EffectOutput::Inline(output.clone()));
        }
        // Over threshold: store the bytes in the CAS (identical content dedupes
        // to one digest) and carry a lightweight ref in the journal.
        let digest = content.put(&bytes).await?;
        Ok(EffectOutput::Ref(ContentRef {
            digest,
            size: bytes.len(),
            summary: None,
        }))
    }

    /// Scrub secrets from an effect output before it is journaled/returned (SP-4 s2).
    /// Identity when no redactor is wired. Pure ⇒ live == journaled == replayed.
    pub(super) fn redact(&self, v: &serde_json::Value) -> serde_json::Value {
        match &self.redactor {
            Some(r) => r.redact(v),
            None => v.clone(),
        }
    }

    /// Build a model node's journaled+returned output `{model, text}` with `text`
    /// redacted (SP-4 s2 — the SINGLE redaction point every model-output producer
    /// routes through, so a new producer is scrubbed by construction). The five live
    /// producers are the direct `ModelCall` node, the `Map`-item call, the
    /// `Consolidate` synthesis, the planner selector's lent dispatch
    /// (`SelectorDispatch::complete`), and the ReAct turn (`dispatch_model_turn`,
    /// which APPENDS `tool_calls` to this shape after — it is the only path that
    /// carries tool calls; the other four are single-shot and text-only).
    ///
    /// This count is the same census the INPUT side keeps at `dispatch_metered`, and
    /// it moves with it: the budget-completeness pass made the selector the fifth of
    /// both. Each producer has its own redaction test — that regime is what SP-4 s2's
    /// review left behind after finding the redactor wired into 1 of the then-4, so a
    /// sixth producer means a sixth test, not just a bigger number here.
    pub(super) fn model_output(
        &self,
        resp: &kernel::types::request::InferenceResponse,
    ) -> serde_json::Value {
        serde_json::json!({
            "model": resp.model,
            "text": self.redact(&serde_json::Value::String(
                resp.content.clone().unwrap_or_default(),
            )),
        })
    }

    /// Materialize a recorded [`EffectOutput`] into its value: an inline value is
    /// cloned; a [`ContentRef`] is fetched lazily from the `ContentStore` and
    /// deserialized. A ref with no store wired, or a digest miss, is loud
    /// ([`ContentDigestMiss`](OrchestratorError::ContentDigestMiss)) — never a
    /// silent empty value.
    pub(super) async fn materialize(
        &self,
        out: &EffectOutput,
    ) -> Result<serde_json::Value, OrchestratorError> {
        match out {
            EffectOutput::Inline(value) => Ok(value.clone()),
            EffectOutput::Ref(r) => {
                let store = self
                    .content
                    .as_ref()
                    .ok_or_else(|| OrchestratorError::ContentDigestMiss(r.digest.0.clone()))?;
                let bytes = store.get(&r.digest).await?;
                Ok(serde_json::from_slice(&bytes)?)
            }
        }
    }
}

/// Scrub a tool output of the EXACT secret values injected into this call (SP-4 broker) —
/// replace each occurrence in every string leaf with `[REDACTED]`, composing with the s2
/// pattern redactor. Per-call + pure ⇒ determinism-safe (a tool holds only its own creds).
pub(super) fn scrub_secret_values(v: &serde_json::Value, secrets: &[&str]) -> serde_json::Value {
    if secrets.is_empty() {
        return v.clone();
    }
    // Replace LONGER secrets first: an overlapping shorter secret must not fragment (and
    // partially leak) a longer one, and this makes the output stable regardless of the
    // HashMap iteration order the callers pass. Sort ONCE here, not per recursive call.
    let mut ordered: Vec<&str> = secrets.iter().copied().filter(|s| !s.is_empty()).collect();
    ordered.sort_by_key(|s| std::cmp::Reverse(s.len()));
    scrub_walk(v, &ordered)
}

/// The recursive string-leaf walker for [`scrub_secret_values`]. `secrets` is already
/// filtered non-empty and length-sorted (longest first) by the public entry point.
fn scrub_walk(v: &serde_json::Value, secrets: &[&str]) -> serde_json::Value {
    match v {
        serde_json::Value::String(s) => {
            let mut out = s.clone();
            for secret in secrets {
                out = out.replace(secret, "[REDACTED]"); // already non-empty + length-sorted
            }
            serde_json::Value::String(out)
        }
        serde_json::Value::Array(a) => {
            serde_json::Value::Array(a.iter().map(|x| scrub_walk(x, secrets)).collect())
        }
        serde_json::Value::Object(o) => serde_json::Value::Object(
            o.iter()
                .map(|(k, x)| (k.clone(), scrub_walk(x, secrets)))
                .collect(),
        ),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    /// Overlapping secrets (one value a substring of another) must never partially leak,
    /// regardless of the order the caller passes them in — the entry point sorts by length
    /// descending so the longer secret is replaced whole before the shorter one can
    /// fragment it. Pins the rank-1 security fix (HashMap iteration order is randomized).
    #[test]
    fn scrub_handles_overlapping_secrets_no_partial_leak() {
        let v = serde_json::json!({ "out": "xtok-secret-123y" });
        for order in [["tok", "tok-secret-123"], ["tok-secret-123", "tok"]] {
            let got = super::scrub_secret_values(&v, &order);
            assert_eq!(
                got["out"],
                serde_json::json!("x[REDACTED]y"),
                "no partial leak regardless of secret order: {order:?}"
            );
        }
    }
}

#[cfg(test)]
mod cost_tests {
    use super::{ceil_micro, cost_micro_usd};
    use kernel::types::cost::Cost;

    fn cost(total: f64, currency: &str) -> Cost {
        Cost {
            input_tokens: 0,
            output_tokens: 0,
            total_tokens: 0,
            input_cost: 0.0,
            output_cost: total,
            total_cost: total,
            currency: currency.into(),
        }
    }

    /// AG-12: the ledger integer is the gateway's float total rounded UP — but NOT by
    /// float representation noise. `0.001 + 0.02` is `0.021000000000000001` in `f64`, and
    /// a plain `ceil` charged 21 001 for a call worth exactly 21 000; a genuine fraction
    /// of a micro-dollar is still charged as a whole one.
    #[test]
    fn a_cost_is_rounded_up_to_whole_micro_dollars_but_not_by_float_noise() {
        // Built the way the gateway builds it (`Cost::from_usage`), which is where the
        // noise comes from: `(100 / 1000) × 0.2` is `0.020000000000000004`.
        let usage = kernel::types::cost::TokenUsage {
            input_tokens: 10,
            output_tokens: 100,
            total_tokens: 110,
        };
        let priced = Cost::from_usage(&usage, 0.1, 0.2);
        assert!(
            priced.total_cost * 1e6 > 21_000.0,
            "the fixture must carry the noise"
        );
        assert_eq!(cost_micro_usd(&priced), Some(21_000));
        assert_eq!(cost_micro_usd(&cost(0.000_000_5, "USD")), Some(1));
        assert_eq!(cost_micro_usd(&cost(0.0, "usd")), Some(0));
        assert_eq!(ceil_micro(2.4), 3);
    }

    /// AG-12: a figure the money ledger cannot trust is refused, not guessed: a non-USD
    /// currency (the cap is in dollars), a non-finite or a negative total.
    #[test]
    fn an_untrustworthy_cost_is_none() {
        assert_eq!(cost_micro_usd(&cost(1.0, "EUR")), None);
        assert_eq!(cost_micro_usd(&cost(f64::NAN, "USD")), None);
        assert_eq!(cost_micro_usd(&cost(f64::INFINITY, "USD")), None);
        assert_eq!(cost_micro_usd(&cost(-0.5, "USD")), None);
    }
}
