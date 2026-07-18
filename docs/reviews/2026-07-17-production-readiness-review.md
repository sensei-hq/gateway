# Gateway Production-Readiness Review — 2026-07-17

Under-the-hood review of the `gateway` + `gateway-embedded` crates for a reusable
LLM-routing library: cost visibility, observability, operational config, and
release readiness. Ran after the capability-trait refactor landed. Findings were
verified in context; this doc tracks each to a fix.

## The three headline questions (as-found)

- **Per-call dollar cost? Was NO → now YES.** Token `usage` flowed through, but no
  dollar cost was ever computed (`actual_cost` always `None`; `purpose.rs` totalled
  `$0.00`). Fixed: engine computes `estimated_cost` + `actual_cost` (`521c185`).
- **Burn rate / aggregate spend? NO.** `GatewayStore` (with `get_spend_since`) is
  defined but wired to nothing; the engine never persists a call. **Deferred to the
  AUTH track** (it's the prerequisite for quota/subscription auth).
- **Observable trace? PARTIAL.** Good `Vec<Attempt>` on success; but no `tracing`
  in the hot path, `ExecutionTrace`/`skipped` unused, streaming unreachable via the
  public API, and structured attempts flattened to a string on total failure.

## Decisions (2026-07-17)

- **Degraded signal:** add a typed `degraded: bool` to the capability responses
  (any adapter can signal it) — preferred over engine-detects-noop.
- **Burn-rate store wiring:** fold into the **AUTH track**, not this pass.
- **Streaming:** it is a **v1 requirement** — add `Gateway::execute_stream` + `StreamEvent`.
- **Reserved capabilities:** `TextRerank`, `TextModerate`, `ImageEdit`, `ImageAnalyze`
  are genuinely distinct features (edit = image+instructions→image; analyze =
  image→text) — **reserve** them with an honest "not supported" error, don't drop.
- **Security pass:** yes — `cargo audit` + semgrep + redacting `Debug` for `RouterConfig`.

## Fix tracking

| # | Finding | Decision / plan | Status |
|---|---------|-----------------|--------|
| 1 | `actual_cost`/`estimated_cost` never computed; `purpose.rs` totals `$0.00` | Compute in engine from usage × `ModelPricing` | ✅ `521c185` |
| 2 | Reserved capabilities → misleading "no adapter registered" | Honest `Unsupported` + exhaustive dispatch | ✅ `7434e65` |
| 3 | README version drift; release gate lacks fmt/clippy | Fix pin to 0.2.24; `check` = fmt+clippy+build+test | ✅ `7ba8384` |
| 4 | anthropic ignores `config.headers`; `anthropic-version` un-overridable | Apply headers; version from config (const = fallback) | ✅ `3623517` |
| 5 | bedrock ignores all `RouterConfig` + false module doc | Fix doc; wire headers+timeout via SDK `customize()` | ✅ `3623517` |
| 6 | bedrock silently drops invalid-base64 image | Return `GatewayError` on decode failure | ✅ `3623517` |
| 7 | `JobConfig` 5-min ceiling hardcoded (async media) | `JobConfig::from_config`; all 8 call sites | ✅ `3623517` |
| 8 | noop `success:false` degraded signal dropped by refactor | Typed `degraded: bool` → `InferenceResponse.success` | ✅ `093012c` |
| 9 | `AllAttemptsFailed` flattens `Vec<Attempt>` to a string | Added `attempts_detail: Vec<Attempt>` on the terminal error | ✅ `1a22cb4` |
| 10 | `Gateway::new`/`update_config` bypass `GatewayBuilder::validate` | Added validating `try_new`/`try_update_config` + `InvalidConfig` | ✅ `1a22cb4` |
| 11 | ~no `tracing` in the hot path | `#[instrument]` on execute + select/attempt/fallback events | ✅ `1a22cb4` |
| 12 | Streaming unreachable via public API; `StreamEvent` dead | `Gateway::execute_stream` emitting `StreamEvent` (v1) | ⬜ pending (last item) |
| 13 | Burn rate / `GatewayStore` unwired | Optional store on `Gateway`; persist calls | ⬜ **AUTH track** |
| 14 | Security: deps, static analysis, secret-leak surface | 2 RUSTSEC vulns patched (consumers update locks); semgrep clean; redacting `Debug(RouterConfig)` | ✅ `f8f0d79` |
| 15 | `ExecutionTrace` / `skipped` diagnostics unused | Build+surface `ExecutionTrace` incl. `skipped` | ⬜ pending (with 9/12) |

## Security pass (`f8f0d79`)

- `cargo audit`: 2 vulnerabilities found, both transitive and now fixed by bumping the
  lock — **quinn-proto → 0.11.16** (RUSTSEC-2026-0185, HIGH remote memory exhaustion)
  and **crossbeam-epoch → 0.9.20** (RUSTSEC-2026-0204). `Cargo.lock` is gitignored
  (library convention), so **consumers (sensei/strategos) must run
  `cargo update -p quinn-proto -p crossbeam-epoch`** in their own repos to ship the fix.
- Accepted (informational, transitive): `number_prefix`/`paste` unmaintained,
  `anyhow` unsound `downcast_mut` (RUSTSEC-2025-0119 / 2024-0436 / 2026-0190).
- `semgrep --config auto`: clean (0 findings).
- Secret-leak surface: `RouterConfig`'s `api_key` now redacted in `Debug`.

## Notes

- Positive: token `usage` is populated per adapter and no provider wire-types leak
  (all private); `GatewayError` variants are structured/actionable.
- The `success` field on `InferenceResponse` is the public facade's degraded signal;
  #8 restores it via the typed `degraded` flag so it generalizes beyond noop.
