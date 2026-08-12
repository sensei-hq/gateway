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
