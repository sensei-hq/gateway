//! The CAS content boundary on the executor: `split_output` (inline-vs-ref by
//! `cas_threshold`, §7.4) and `materialize` (lazy ref fetch). Split out of
//! `super` for readability; both are `impl Executor` methods sharing its state.

use orchestrator_core::{ContentRef, EffectOutput, OrchestratorError};

use super::Executor;

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
    /// routes through, so a new producer is scrubbed by construction). The four live
    /// producers are the direct `ModelCall` node, the `Map`-item call, the
    /// `Consolidate` synthesis, and the ReAct turn (`dispatch_model_turn`, which
    /// APPENDS `tool_calls` to this shape after — it is the only path that carries
    /// tool calls; the other three are single-shot and text-only).
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
