//! Boundary translation between the public unified request/response types
//! and the capability-typed structs the segregated traits use. Keeps the
//! `Gateway::execute(InferenceRequest) -> InferenceResponse` facade stable
//! while adapters speak focused types.

use crate::types::error::GatewayError;
use crate::types::io::{
    ChatRequest, ChatResponse, EmbedRequest, EmbedResponse, ImageRequest, ImageResponse,
    SttRequest, SttResponse, TtsRequest, TtsResponse, VideoRequest, VideoResponse,
};
use crate::types::request::{InferenceRequest, InferenceResponse, Payload};

fn wrong_payload(expected: &str) -> GatewayError {
    GatewayError::ProviderError {
        adapter: "dispatch".into(),
        message: format!("expected {expected} payload for this capability"),
        status: None,
    }
}

/// Base response with all optional result fields cleared — the engine fills
/// `attempts`/cost afterwards.
fn empty_response() -> InferenceResponse {
    InferenceResponse {
        success: true,
        content: None,
        embeddings: None,
        transcription: None,
        audio: None,
        images: None,
        videos: None,
        model: None,
        usage: None,
        tool_calls: Vec::new(),
        estimated_cost: None,
        actual_cost: None,
        attempts: Vec::new(),
    }
}

pub fn to_chat_request(
    req: &InferenceRequest,
    model: Option<String>,
) -> Result<ChatRequest, GatewayError> {
    let Payload::Chat {
        messages,
        system,
        max_tokens,
        temperature,
        tools,
    } = &req.payload
    else {
        return Err(wrong_payload("chat"));
    };
    Ok(ChatRequest {
        model: model.or_else(|| req.model.clone()),
        messages: messages.clone(),
        system: system.clone(),
        max_tokens: *max_tokens,
        temperature: *temperature,
        tools: tools.clone(),
    })
}

pub fn from_chat_response(r: ChatResponse) -> InferenceResponse {
    InferenceResponse {
        success: !r.degraded,
        content: r.content,
        tool_calls: r.tool_calls,
        usage: r.usage,
        model: r.model,
        ..empty_response()
    }
}

pub fn to_embed_request(
    req: &InferenceRequest,
    model: Option<String>,
) -> Result<EmbedRequest, GatewayError> {
    let Payload::Embed { texts } = &req.payload else {
        return Err(wrong_payload("embed"));
    };
    Ok(EmbedRequest {
        model: model.or_else(|| req.model.clone()),
        texts: texts.clone(),
    })
}

pub fn from_embed_response(r: EmbedResponse) -> InferenceResponse {
    InferenceResponse {
        success: !r.degraded,
        embeddings: Some(r.embeddings),
        usage: r.usage,
        ..empty_response()
    }
}

pub fn to_stt_request(
    req: &InferenceRequest,
    model: Option<String>,
) -> Result<SttRequest, GatewayError> {
    let Payload::Stt {
        audio,
        language,
        format,
    } = &req.payload
    else {
        return Err(wrong_payload("stt"));
    };
    Ok(SttRequest {
        model: model.or_else(|| req.model.clone()),
        audio: audio.clone(),
        language: language.clone(),
        format: format.clone(),
    })
}

pub fn from_stt_response(r: SttResponse) -> InferenceResponse {
    InferenceResponse {
        success: !r.degraded,
        transcription: Some(r.transcription),
        usage: r.usage,
        ..empty_response()
    }
}

pub fn to_tts_request(
    req: &InferenceRequest,
    model: Option<String>,
) -> Result<TtsRequest, GatewayError> {
    let Payload::Tts {
        text,
        voice,
        speed,
        output_format,
    } = &req.payload
    else {
        return Err(wrong_payload("tts"));
    };
    Ok(TtsRequest {
        model: model.or_else(|| req.model.clone()),
        text: text.clone(),
        voice: voice.clone(),
        speed: *speed,
        output_format: output_format.clone(),
    })
}

pub fn from_tts_response(r: TtsResponse) -> InferenceResponse {
    InferenceResponse {
        success: !r.degraded,
        audio: Some(r.audio),
        ..empty_response()
    }
}

pub fn to_image_request(
    req: &InferenceRequest,
    model: Option<String>,
) -> Result<ImageRequest, GatewayError> {
    let Payload::ImageGenerate {
        prompt,
        size,
        quality,
        style,
        n,
    } = &req.payload
    else {
        return Err(wrong_payload("image_generate"));
    };
    Ok(ImageRequest {
        model: model.or_else(|| req.model.clone()),
        prompt: prompt.clone(),
        size: size.clone(),
        quality: quality.clone(),
        style: style.clone(),
        n: *n,
    })
}

pub fn from_image_response(r: ImageResponse) -> InferenceResponse {
    InferenceResponse {
        success: !r.degraded,
        images: Some(r.images),
        ..empty_response()
    }
}

pub fn to_video_request(
    req: &InferenceRequest,
    model: Option<String>,
) -> Result<VideoRequest, GatewayError> {
    let Payload::VideoGenerate {
        prompt,
        duration_secs,
        resolution,
    } = &req.payload
    else {
        return Err(wrong_payload("video_generate"));
    };
    Ok(VideoRequest {
        model: model.or_else(|| req.model.clone()),
        prompt: prompt.clone(),
        duration_secs: *duration_secs,
        resolution: resolution.clone(),
    })
}

pub fn from_video_response(r: VideoResponse) -> InferenceResponse {
    InferenceResponse {
        success: !r.degraded,
        videos: Some(r.videos),
        ..empty_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::capability::Capability;
    use crate::types::io::{ChatResponse, ImageResponse, SttResponse, TtsResponse, VideoResponse};
    use crate::types::request::{AudioFormat, Message, MessageRole, Payload};

    fn chat_req(model: Option<&str>) -> InferenceRequest {
        InferenceRequest {
            capability: Capability::TextChat,
            model: model.map(Into::into),
            router: None,
            chain: None,
            payload: Payload::Chat {
                messages: vec![Message::text(MessageRole::User, "hi")],
                system: Some("sys".into()),
                max_tokens: Some(10),
                temperature: Some(0.5),
                tools: Vec::new(),
            },
            budget: None,
            auth: None,
            panel: None,
            consensus: None,
        }
    }

    /// A request carrying an arbitrary payload (capability is not inspected by
    /// the `to_*` converters — they match on the payload).
    fn req_with(payload: Payload) -> InferenceRequest {
        InferenceRequest {
            capability: Capability::TextChat,
            model: None,
            router: None,
            chain: None,
            payload,
            budget: None,
            auth: None,
            panel: None,
            consensus: None,
        }
    }

    #[test]
    fn to_chat_request_extracts_payload_and_model() {
        let ir = chat_req(None);
        let cr = to_chat_request(&ir, Some("m1".into())).unwrap();
        assert_eq!(cr.model.as_deref(), Some("m1"));
        assert_eq!(cr.messages.len(), 1);
        assert_eq!(cr.system.as_deref(), Some("sys"));
        assert_eq!(cr.max_tokens, Some(10));
    }

    #[test]
    fn to_chat_request_prefers_injected_model_then_request_model() {
        let ir = chat_req(Some("pinned"));
        // injected model wins when present
        assert_eq!(
            to_chat_request(&ir, Some("injected".into()))
                .unwrap()
                .model
                .as_deref(),
            Some("injected")
        );
        // falls back to the request's pinned model when none injected
        assert_eq!(
            to_chat_request(&ir, None).unwrap().model.as_deref(),
            Some("pinned")
        );
    }

    #[test]
    fn to_chat_request_rejects_non_chat_payload() {
        let ir = InferenceRequest {
            capability: Capability::TextEmbed,
            model: None,
            router: None,
            chain: None,
            payload: Payload::Embed {
                texts: vec!["x".into()],
            },
            budget: None,
            auth: None,
            panel: None,
            consensus: None,
        };
        assert!(to_chat_request(&ir, None).is_err());
    }

    #[test]
    fn from_chat_response_fills_only_chat_fields() {
        let resp = ChatResponse {
            content: Some("hello".into()),
            model: Some("m1".into()),
            ..Default::default()
        };
        let ir = from_chat_response(resp);
        assert_eq!(ir.content.as_deref(), Some("hello"));
        assert!(ir.embeddings.is_none());
        assert!(ir.success);
    }

    #[test]
    fn from_embed_response_sets_embeddings() {
        let ir = from_embed_response(EmbedResponse {
            embeddings: vec![vec![0.1, 0.2]],
            usage: None,
            ..Default::default()
        });
        assert_eq!(ir.embeddings.as_ref().unwrap().len(), 1);
        assert!(ir.content.is_none());
    }

    #[test]
    fn from_chat_response_degraded_flag_maps_to_success() {
        // A degraded typed response yields `success == false`.
        let degraded = from_chat_response(ChatResponse {
            content: Some("placeholder".into()),
            degraded: true,
            ..Default::default()
        });
        assert!(!degraded.success);

        // A normal typed response yields `success == true`.
        let normal = from_chat_response(ChatResponse {
            content: Some("real answer".into()),
            degraded: false,
            ..Default::default()
        });
        assert!(normal.success);
    }

    // --- Embed / STT / TTS / Image / Video converters ---

    #[test]
    fn to_embed_request_maps_texts_and_prefers_injected_model() {
        let r = req_with(Payload::Embed {
            texts: vec!["a".into(), "b".into()],
        });
        let er = to_embed_request(&r, Some("m".into())).unwrap();
        assert_eq!(er.model.as_deref(), Some("m"));
        assert_eq!(er.texts, vec!["a".to_string(), "b".to_string()]);
        assert!(to_embed_request(&chat_req(None), None).is_err());
    }

    #[test]
    fn to_stt_request_maps_audio_language_and_format() {
        let r = req_with(Payload::Stt {
            audio: vec![1, 2, 3],
            language: Some("en".into()),
            format: "wav".into(),
        });
        let sr = to_stt_request(&r, None).unwrap();
        assert_eq!(sr.audio, vec![1u8, 2, 3]);
        assert_eq!(sr.language.as_deref(), Some("en"));
        assert_eq!(sr.format, "wav");
        assert!(to_stt_request(&chat_req(None), None).is_err());
    }

    #[test]
    fn from_stt_response_sets_transcription_and_success() {
        let ir = from_stt_response(SttResponse {
            transcription: "hello".into(),
            usage: None,
            degraded: false,
        });
        assert_eq!(ir.transcription.as_deref(), Some("hello"));
        assert!(ir.success);
    }

    #[test]
    fn to_tts_request_maps_text_voice_and_speed() {
        let r = req_with(Payload::Tts {
            text: "hi".into(),
            voice: Some("v".into()),
            speed: Some(1.5),
            output_format: AudioFormat::Wav,
        });
        let tr = to_tts_request(&r, Some("m".into())).unwrap();
        assert_eq!(tr.model.as_deref(), Some("m"));
        assert_eq!(tr.text, "hi");
        assert_eq!(tr.voice.as_deref(), Some("v"));
        assert_eq!(tr.speed, Some(1.5));
        assert!(to_tts_request(&chat_req(None), None).is_err());
    }

    #[test]
    fn from_tts_response_sets_audio_and_degraded_maps_to_success() {
        let ir = from_tts_response(TtsResponse {
            audio: vec![9u8],
            degraded: true,
        });
        assert_eq!(ir.audio, Some(vec![9u8]));
        assert!(!ir.success);
    }

    #[test]
    fn to_image_request_maps_prompt_size_and_count() {
        let r = req_with(Payload::ImageGenerate {
            prompt: "a cat".into(),
            size: Some("1024x1024".into()),
            quality: None,
            style: None,
            n: 2,
        });
        let img = to_image_request(&r, None).unwrap();
        assert_eq!(img.prompt, "a cat");
        assert_eq!(img.size.as_deref(), Some("1024x1024"));
        assert_eq!(img.n, 2);
        assert!(to_image_request(&chat_req(None), None).is_err());
    }

    #[test]
    fn from_image_response_sets_images() {
        let ir = from_image_response(ImageResponse {
            images: Vec::new(),
            degraded: false,
        });
        assert!(ir.images.is_some());
        assert!(ir.success);
    }

    #[test]
    fn to_video_request_maps_prompt_and_duration() {
        let r = req_with(Payload::VideoGenerate {
            prompt: "sunset".into(),
            duration_secs: Some(10),
            resolution: Some("1080p".into()),
        });
        let vr = to_video_request(&r, None).unwrap();
        assert_eq!(vr.prompt, "sunset");
        assert_eq!(vr.duration_secs, Some(10));
        assert_eq!(vr.resolution.as_deref(), Some("1080p"));
        assert!(to_video_request(&chat_req(None), None).is_err());
    }

    #[test]
    fn from_video_response_sets_videos() {
        let ir = from_video_response(VideoResponse {
            videos: Vec::new(),
            degraded: false,
        });
        assert!(ir.videos.is_some());
        assert!(ir.success);
    }
}
