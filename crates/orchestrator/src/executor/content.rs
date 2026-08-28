//! The CAS content boundary on the executor: `split_output` (inline-vs-ref by
//! `cas_threshold`, §7.4) and `materialize` (lazy ref fetch). Split out of
//! `super` for readability; both are `impl Executor` methods sharing its state.

use orchestrator_core::{ContentRef, EffectOutput, OrchestratorError};

use super::Executor;

/// SP-DATA-5: the single conversion from the gateway's reported usage
/// (`kernel::types::cost::TokenUsage`) to the journal's local mirror
/// (`orchestrator_core::TokenUsage`, defined without a `kernel` dependency —
/// see Task 1). Lives beside `model_output`, the sibling OUTPUT-side
/// chokepoint, because both exist so a new model-call producer picks up the
/// conversion by construction rather than by remembering to copy it.
///
/// A free function, not a `From` impl: both types are foreign to this crate
/// (`kernel::types::cost::TokenUsage` and `orchestrator_core::TokenUsage`), so
/// `impl From<A> for B` here would violate the orphan rule — neither type is
/// local to `orchestrator`. All four producers call
/// `response.usage.map(convert_usage)`; a field added to the JOURNALED
/// (`orchestrator_core`) side fails to compile HERE — one fix — instead of
/// being silently dropped at three of the four call sites the way an inlined
/// field-by-field copy would leave it.
pub(super) fn convert_usage(u: kernel::types::cost::TokenUsage) -> orchestrator_core::TokenUsage {
    orchestrator_core::TokenUsage {
        input_tokens: u.input_tokens,
        output_tokens: u.output_tokens,
        total_tokens: u.total_tokens,
    }
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
