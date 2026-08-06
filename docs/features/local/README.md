---
title: Local — Module Reference
doctype: module
module: local
status: implemented
---

# Local

In-process (local) inference behind the same adapter abstraction as cloud, so
local and cloud models compose in one routing config.

## Status

| Feature | Status | Source | Notes |
|---|---|---|---|
| [Embedded inference](embedded-inference.md) | Implemented | `crates/local-providers/src` | llama.cpp / ONNX (ort) / FastEmbed; cargo features |

## Notes

- Model resolution + Hugging Face pull live in `crates/local-engine` — see [catalog/model-registry](../catalog/model-registry.md).
- Local models are the `$0` floor of a fallback chain (e.g. the `fallback-specialty` tier).
