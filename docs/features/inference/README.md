---
title: Inference — Module Reference
doctype: module
module: inference
status: implemented
---

# Inference

The request→provider surface: the provider adapters, the capability-trait model
they implement, streaming, and tool-calling.

## Status

| Feature | Status | Source | Notes |
|---|---|---|---|
| [Providers](providers.md) | Implemented | `crates/cloud-providers/src` | ~18 cloud adapters + async media |
| [Capabilities & adapters](capabilities-and-adapters.md) | Implemented | `crates/kernel/src/adapters/capability.rs` | per-capability traits + registry |
| [Streaming](streaming.md) | Implemented | `crates/gateway/src/engine.rs` | `execute_stream`; pre-first-byte fallover only |
| [Tool calling](tool-calling.md) | Implemented | `crates/kernel/src/types/request.rs` | gateway returns `tool_calls`; does not execute |

## Notes

- Local adapters (`crates/local-providers`) implement the same capability traits, so local + cloud compose in one config — see [local](../local/README.md).
- Tool *execution* is out of scope for the gateway; the [orchestrator](../orchestrator/README.md) owns it.
