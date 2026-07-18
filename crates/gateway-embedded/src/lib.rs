//! In-process inference adapters and model registry for the sensei gateway.
//!
//! Each engine (llama-cpp-2, fastembed, ort) lives behind a cargo feature so
//! callers only compile in what they need. Adapters implement the gateway's
//! capability traits (`Model` + `ChatModel`/`EmbedModel`) — no new abstraction
//! in the gateway core.
//!
//! See `docs/backlog.md` (Future scope — gateway-embedded) for design rationale.

pub mod adapters;
pub mod math;
pub mod registry;
