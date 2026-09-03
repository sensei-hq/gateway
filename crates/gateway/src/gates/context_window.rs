use super::{AdmissionGate, CandidateView, GateVerdict, SelectionCtx};
use crate::skip_reason::SkipReason;

/// Gate: the candidate's `context_window` must be able to hold this request's estimated
/// input.
///
/// The sixth gate — registered by SP-7a Task 5, beside the five in
/// `ModelSelectionService::new`; until that lands this type is built and tested but
/// changes no selection outcome. It is the one that makes the orchestrator's
/// pre-dispatch check unnecessary. That check tested the prompt against
/// `min_context_window(chain)` — the SMALLEST window in the chain — and failed the node
/// terminally, so a chain of `[gpt-4o 128k, fallback 8k]` refused a 20k prompt the
/// primary would have served. Here the question is asked per CANDIDATE, which is the
/// only place it has a correct answer.
///
/// The estimate read is `input_tokens_pessimistic`, NOT the cost gate's `input_tokens`:
/// the cost figure omits tool schemas and divides by 4, so judging on it would admit
/// exactly the requests this gate exists to catch. See
/// `engine::util::estimate_input_tokens_pessimistic` — including its statement of what
/// it does NOT count, since the gate is exactly as complete as its input.
///
/// `None` admits, matching `BudgetGate`'s treatment of a model with no pricing: an absent
/// estimate is not evidence of a problem, and a gate that skipped on missing data would
/// refuse every request that did not carry one.
///
/// # The boundary is `>`, so a request that exactly fills the window is admitted
///
/// This gate answers ONE question — can this candidate hold the INPUT — and a request
/// of exactly `window` tokens can be held. Whether there is room for OUTPUT beside it is
/// a different question, and answering it here would need a floor for "enough output"
/// that the gateway does not have: `max_tokens` may be set by the caller, defaulted by
/// the adapter (`anthropic` sends 1024 when the request carries `None`), or omitted from
/// the wire entirely (`openai_compat` skips the field). Any floor picked here would
/// refuse candidates that would have served a short reply.
///
/// What this comment used to say, and what is NOT true: that bounding output is the
/// SP-DATA-5 clamp's job, so the boundary is safe. The clamp bounds `max_tokens` by a
/// window only on a BUDGETED run with a `Chat` payload (`executor/dispatch.rs`;
/// `budget: None` — every unbudgeted run, which is the default — never clamps), and when
/// it does run it bounds by `min_context_window(chain)`, the chain MINIMUM this gate
/// exists to stop trusting. So on the default path nothing downstream bounds output by
/// the window at all. The `>` stands on the narrow-question argument above, not on a
/// downstream guard.
///
/// Changing this to `>=` is therefore a design change needing that floor figure and an
/// argument for it — not a bug fix.
pub struct ContextWindowGate;

impl AdmissionGate for ContextWindowGate {
    fn name(&self) -> &'static str {
        "context_window"
    }

    fn evaluate(&self, c: &CandidateView<'_>, x: &SelectionCtx<'_>) -> GateVerdict {
        match x.input_tokens_pessimistic {
            Some(est) if est > c.model_config.context_window => {
                GateVerdict::Skip(SkipReason::OverContextWindow {
                    estimated: est,
                    window: c.model_config.context_window,
                })
            }
            _ => GateVerdict::Admit,
        }
    }
}

#[cfg(test)]
mod tests {
    // `super::*` already carries `AdmissionGate` / `CandidateView` / `GateVerdict` /
    // `SelectionCtx` / `SkipReason` through this module's own imports.
    use super::*;
    use crate::gates::EndpointHealthRead;
    use crate::types::capability::Capability;
    use crate::types::config::{GatewayConfig, ModelConfig, RouterConfig};
    use std::collections::HashMap;
    use std::time::Instant;

    struct NeverOpen;
    impl EndpointHealthRead for NeverOpen {
        fn open_until(&self, _endpoint: &str) -> Option<Instant> {
            None
        }
    }

    struct NeverCooling;
    impl crate::gates::RouterHealthRead for NeverCooling {
        fn cooling_until(&self, _router: &str) -> Option<Instant> {
            None
        }
    }

    struct NeverLocked;
    impl crate::gates::lockout::ModelLockoutRead for NeverLocked {
        fn locked(&self, _endpoint: &str) -> Option<crate::gates::lockout::LockView> {
            None
        }
    }

    fn model_with_window(context_window: u32) -> ModelConfig {
        ModelConfig {
            id: "some-model".to_string(),
            api_model_id: None,
            provider: "anthropic".to_string(),
            family: None,
            capabilities: vec![Capability::TextChat],
            context_window,
            max_output_tokens: 4096,
            pricing: None,
            catalog: None,
        }
    }

    fn test_router_config() -> RouterConfig {
        RouterConfig {
            url: "http://localhost".to_string(),
            api_key_env: None,
            api_key: None,
            enabled: true,
            timeout_ms: None,
            headers: HashMap::new(),
        }
    }

    fn cand<'a>(mc: &'a ModelConfig, rc: &'a RouterConfig) -> CandidateView<'a> {
        CandidateView {
            model: "some-model",
            router: "anthropic",
            endpoint: "anthropic:some-model".to_string(),
            model_config: mc,
            router_config: rc,
        }
    }

    /// `input_tokens` (the COST estimate) and `input_tokens_pessimistic` (this gate's)
    /// are taken separately so a test can set them to different values and pin which one
    /// the gate reads.
    fn ctx<'a>(
        cfg: &'a GatewayConfig,
        health: &'a dyn EndpointHealthRead,
        input_tokens: Option<u32>,
        input_tokens_pessimistic: Option<u32>,
    ) -> SelectionCtx<'a> {
        SelectionCtx {
            capability: Capability::TextChat,
            budget: None,
            input_tokens,
            input_tokens_pessimistic,
            health,
            now: Instant::now(),
            config: cfg,
            router_health: &NeverCooling,
            model_lockout: &NeverLocked,
        }
    }

    /// AC5 — a missing estimate ADMITS. The gate is not a filter on absent data.
    ///
    /// Mirrors `BudgetGate`, which admits a model with no pricing: an absent estimate is
    /// not evidence of a problem, and skipping on it would refuse every request that did
    /// not carry one — which is every caller that reaches selection by a path other than
    /// `engine::execute`.
    ///
    /// Asserted against a ZERO window, deliberately. With the 8192 fixture this test
    /// could not tell "admitted because the estimate is absent" from "admitted because a
    /// substituted default happened to fit": `.or(Some(8_000))` in the gate passes an
    /// 8192-window test while skipping every candidate under 8000 tokens. Nothing fits a
    /// zero window, so admitting here is possible ONLY because the estimate is absent,
    /// and every `unwrap_or(k)` mutant reddens rather than just the extreme ones.
    #[test]
    fn no_estimate_admits() {
        let mc = model_with_window(0);
        let rc = test_router_config();
        let cfg = GatewayConfig::default();
        let health = NeverOpen;
        assert!(
            matches!(
                ContextWindowGate.evaluate(&cand(&mc, &rc), &ctx(&cfg, &health, None, None)),
                GateVerdict::Admit
            ),
            "a request carrying no pessimistic estimate must admit EVERY candidate, \
             including one whose window could hold nothing"
        );
    }

    /// A request inside the window admits; one over it is skipped with BOTH numbers; and
    /// the SAME request gets DIFFERENT answers from two candidates.
    ///
    /// The third clause is the slice's whole reason to exist and was the one thing no
    /// test pinned: replacing `c.model_config.context_window` with the literal 8_192 —
    /// deleting every read of the candidate — left the entire gateway suite green. So
    /// one estimate is now evaluated against a 128k candidate and an 8k one, which is
    /// AC1's chain at unit scale and dies on any regression to a single chain-wide
    /// number.
    ///
    /// `8_192` admitting and `8_193` skipping pins the boundary as `est > window`, not
    /// `est >= window` — see the type's doc for why that is the narrow question and what
    /// does NOT bound the output half.
    #[test]
    fn over_window_skips_and_under_window_admits() {
        let mc = model_with_window(8_192);
        let rc = test_router_config();
        let cfg = GatewayConfig::default();
        let health = NeverOpen;
        assert!(
            matches!(
                ContextWindowGate.evaluate(&cand(&mc, &rc), &ctx(&cfg, &health, None, Some(8_192))),
                GateVerdict::Admit
            ),
            "a request that exactly fills the window must be admitted"
        );
        match ContextWindowGate.evaluate(&cand(&mc, &rc), &ctx(&cfg, &health, None, Some(8_193))) {
            GateVerdict::Skip(SkipReason::OverContextWindow { estimated, window }) => {
                assert_eq!(estimated, 8_193);
                assert_eq!(window, 8_192);
            }
            GateVerdict::Skip(other) => panic!("expected an OverContextWindow skip, got {other}"),
            GateVerdict::Admit => panic!("8193 tokens must not be admitted to an 8192 window"),
        }

        // AC1 at unit scale: ONE request, two candidates, two answers.
        let big = model_with_window(128_000);
        let small = model_with_window(8_192);
        let twenty_k = ctx(&cfg, &health, None, Some(20_000));
        assert!(
            matches!(
                ContextWindowGate.evaluate(&cand(&big, &rc), &twenty_k),
                GateVerdict::Admit
            ),
            "the 128k candidate holds 20k and must be admitted"
        );
        match ContextWindowGate.evaluate(&cand(&small, &rc), &twenty_k) {
            GateVerdict::Skip(SkipReason::OverContextWindow { estimated, window }) => {
                assert_eq!(estimated, 20_000);
                assert_eq!(
                    window, 8_192,
                    "the skip must record THIS candidate's window, not a chain-wide \
                     figure — recording the wrong one sends the operator after the \
                     wrong model"
                );
            }
            GateVerdict::Skip(other) => panic!("expected an OverContextWindow skip, got {other}"),
            GateVerdict::Admit => panic!(
                "the 8192 candidate cannot hold 20k — a gate that admits it is reading \
                 something other than this candidate's window"
            ),
        }
    }

    /// The gate reads the PESSIMISTIC estimate, never the cost one.
    ///
    /// The two fields exist precisely because they differ — the cost estimate omits tool
    /// schemas and divides by 4 — so a gate reading `input_tokens` would compile, pass
    /// the other two tests, and still admit exactly the over-window requests this slice
    /// was written to catch. Both directions are asserted, because reading the wrong
    /// field fails in both.
    #[test]
    fn the_gate_reads_the_pessimistic_estimate_not_the_cost_one() {
        let mc = model_with_window(8_192);
        let rc = test_router_config();
        let cfg = GatewayConfig::default();
        let health = NeverOpen;

        // Cost estimate says "tiny"; the pessimistic one says "over". Must skip.
        match ContextWindowGate
            .evaluate(&cand(&mc, &rc), &ctx(&cfg, &health, Some(1), Some(20_000)))
        {
            GateVerdict::Skip(SkipReason::OverContextWindow { estimated, .. }) => {
                assert_eq!(
                    estimated, 20_000,
                    "the skip must record the pessimistic figure it judged on"
                );
            }
            _ => panic!("a pessimistic estimate of 20000 must skip an 8192 window"),
        }

        // Cost estimate says "over"; the pessimistic one is absent. Must admit — AC5
        // applies to the field the gate owns, and the cost estimate is not its business.
        assert!(matches!(
            ContextWindowGate.evaluate(&cand(&mc, &rc), &ctx(&cfg, &health, Some(20_000), None)),
            GateVerdict::Admit
        ));
    }
}
