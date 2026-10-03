# Checkpoint

**SP-DEC-1 — Decision (System One) capability, gh#72: DONE and RELEASED as v0.7.0**
(tag `b444b6b`). gh#73 released as v0.6.1. **PR #74 (develop→main, both releases,
Closes #72 #73) is fully green — 18/18 checks — and awaits human review** (main is
protected). develop head: `ce10413`.

## Done

- v0.6.1: llama-cpp-2 → 0.1.158 vocab API, floor raised (#73).
- v0.7.0: SP-DEC-1 T1–T7 (plan `docs/superpowers/plans/2026-10-03-sp-dec-1-decision-capability.md`);
  whole-slice review 12 must-fix fixed red-first. Shipped tag verified: a fresh
  consumer on `tag = "v0.7.0"` → facade → Ollama nimble, `success=true`.
- CI had been red since Rust 1.99 hit stable (2026-10-01): async-trait 0.1.92
  (`c77cde3`) + `fetch_update` → `try_update` (`ce10413`). Tags v0.6.1/v0.7.0 predate
  the fix — affects clippy only, not consumers' builds.
- sensei PR **sensei-hq/sensei#228** (`feat/decision-seed`, closes sensei#202): pins
  → v0.7.0, `decision` enum + seed (decision models, openrouter/typesafe routers,
  `decide` chain, Jun–Oct 2026 models), `map_capability`, router mirror guard;
  senseid 3157/0, workspace 358/0, DB seed test, end-to-end decision via nimble.

## Next

1. Human review/merge of gateway #74 (18/18 green) and sensei #228 (9/9 green).

## Open questions

None.

## Known-broken (pre-existing, not this work)

- sensei `dbd reconcile` fails on `sensei.libraries` (column used by a view), even on
  pristine develop. Shared local `sensei_test` is schema-stale (309 failures until
  re-deployed).
- gateway `OllamaAdapter::from_config` / `OpenAIAdapter::from_config`: no default
  timeout when `timeout_ms` is unset (chat path).
- LOW carry: estimator test doesn't pin question names/option keys; llms README's
  upgrading row still says "0.3→0.4" first.
