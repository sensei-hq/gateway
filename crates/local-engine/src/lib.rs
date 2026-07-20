//! `sensei-local-engine` — the local model engine: resolvers that map a stable
//! model id to on-disk bytes (managed / Ollama / external, composed via
//! `ChainedResolver`) plus Hugging Face pull (`hf-download`). Model vocabulary
//! (`ModelEntry`/`ModelResolver`/…) lives in `kernel::registry`.
pub mod registry;
