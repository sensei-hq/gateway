use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    // Text modality
    TextChat,     // multi-turn messages, tools, system prompts -> text
    TextComplete, // single prompt -> text (legacy/Ollama)
    TextEmbed,    // text -> dense vectors
    TextRerank,   // candidates + query -> ranked list
    TextModerate, // text -> safety labels + scores

    // Image modality
    ImageGenerate, // text -> image(s)
    ImageEdit,     // image + instructions -> image
    ImageAnalyze,  // image -> text (vision, OCR)

    // Audio modality
    AudioTranscribe, // audio -> text (STT)
    AudioGenerate,   // text -> audio (TTS)

    // Video modality
    VideoGenerate, // text/image -> video
}
