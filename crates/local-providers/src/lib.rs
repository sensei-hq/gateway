//! `sensei-local-providers` — in-process inference adapters (llama.cpp,
//! fastembed, ONNX Runtime). Each implements the `kernel` capability traits and
//! loads a `kernel::registry::ModelEntry`; construction is caller-driven.
pub mod adapters;
pub mod math;
