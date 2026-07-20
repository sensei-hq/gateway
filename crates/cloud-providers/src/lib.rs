//! `sensei-cloud-providers` — cloud LLM provider adapters for the sensei
//! gateway. Each adapter implements the `kernel` capability traits; construction
//! is caller-driven (register into a `kernel::adapters::AdapterRegistry`).

pub mod anthropic;
pub mod async_job;
pub mod base;
pub mod bedrock;
pub mod fal;
pub mod flux;
pub mod gemini;
pub mod grok;
pub mod huggingface;
pub mod kling;
pub mod luma;
pub mod ollama;
pub mod openai;
pub mod openai_compat;
pub mod recraft;
pub mod replicate;
pub mod runway;
pub mod stability;
pub mod together;
