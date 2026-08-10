use super::*;

impl super::Gateway {
    /// Dispatch one attempt to the adapter registered for `router` + the
    /// request's capability, translating at the [`crate::dispatch`] boundary.
    ///
    /// `None` means no adapter is registered for this router+capability (or the
    /// capability has no dispatch route) — the caller treats it as a no-adapter
    /// miss and falls through to the next candidate. Reserved capabilities
    /// return `Some(Err(Unsupported))`. Exhaustive (no `_`) so a new
    /// `Capability` variant forces a routing decision here at compile time.
    pub(super) async fn dispatch_capability(
        &self,
        request: &InferenceRequest,
        router: &str,
        model: Option<String>,
        cfg: &kernel::types::config::RouterConfig,
    ) -> Option<Result<InferenceResponse, GatewayError>> {
        // gh#39: this exhaustive 6-way match intentionally has no `_` arm —
        // it's a compile-time routing guarantee that every `Capability`
        // variant is handled. The qlty complexity smell here is accepted,
        // not contorted into sub-functions.
        match request.capability {
            Capability::TextChat | Capability::TextComplete => {
                match self.adapters.chat(router).await {
                    Some(m) => Some(match to_chat_request(request, model) {
                        Ok(r) => m.chat(cfg, &r).await.map(from_chat_response),
                        Err(e) => Err(e),
                    }),
                    None => None,
                }
            }
            Capability::TextEmbed => match self.adapters.embed(router).await {
                Some(m) => Some(match to_embed_request(request, model) {
                    Ok(r) => m.embed(cfg, &r).await.map(from_embed_response),
                    Err(e) => Err(e),
                }),
                None => None,
            },
            Capability::AudioTranscribe => match self.adapters.stt(router).await {
                Some(m) => Some(match to_stt_request(request, model) {
                    Ok(r) => m.transcribe(cfg, &r).await.map(from_stt_response),
                    Err(e) => Err(e),
                }),
                None => None,
            },
            Capability::AudioGenerate => match self.adapters.tts(router).await {
                Some(m) => Some(match to_tts_request(request, model) {
                    Ok(r) => m.speak(cfg, &r).await.map(from_tts_response),
                    Err(e) => Err(e),
                }),
                None => None,
            },
            Capability::ImageGenerate => match self.adapters.image(router).await {
                Some(m) => Some(match to_image_request(request, model) {
                    Ok(r) => m.generate_image(cfg, &r).await.map(from_image_response),
                    Err(e) => Err(e),
                }),
                None => None,
            },
            Capability::VideoGenerate => match self.adapters.video(router).await {
                Some(m) => Some(match to_video_request(request, model) {
                    Ok(r) => m.generate_video(cfg, &r).await.map(from_video_response),
                    Err(e) => Err(e),
                }),
                None => None,
            },
            // Reserved capabilities have no payload / trait / dispatch route
            // yet — surface an honest "not yet supported" rather than the
            // misleading "no adapter registered".
            Capability::TextRerank
            | Capability::TextModerate
            | Capability::ImageEdit
            | Capability::ImageAnalyze => Some(Err(GatewayError::Unsupported {
                adapter: router.to_string(),
                what: "capability not yet supported (reserved)".to_string(),
            })),
        }
    }
}
