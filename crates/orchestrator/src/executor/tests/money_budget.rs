//! AG-12 (#89): the money-denominated run budget, end to end on the in-memory stores.
//!
//! Every test here prices the single-chain fixture with [`price_single_chain`] at
//! `$0.1 / 1k` input and `$0.2 / 1k` output — 100 and 200 micro-dollars per token — so
//! each number below is arithmetic a reader can check by hand. A clamp-observing call that
//! reports 10 input and 100 output tokens therefore costs `10·100 + 100·200 = 21 000`
//! micro-dollars.

use super::*;
use crate::test_support::{
    price_single_chain, price_single_chain_with_fee, price_single_chain_with_output_limit,
};
use orchestrator_core::{MoneyBudget, RunBudget};

/// `$0.1 / 1k` input — 100 micro-dollars a token.
const IN_PER_1K: f64 = 0.1;
/// `$0.2 / 1k` output — 200 micro-dollars a token.
const OUT_PER_1K: f64 = 0.2;
const IN_MICRO: u64 = 100;
const OUT_MICRO: u64 = 200;

fn money(total_micro_usd: u64) -> RunBudget {
    RunBudget {
        tokens: None,
        money: Some(MoneyBudget { total_micro_usd }),
    }
}

fn chain_of(n: usize) -> Graph {
    Graph {
        nodes: (1..=n)
            .map(|i| Node {
                id: NodeId(format!("n{i}")),
                kind: model_call("c", &format!("p{i}")),
                deps: if i == 1 {
                    vec![]
                } else {
                    vec![Dep::hard(format!("n{}", i - 1).as_str())]
                },
            })
            .collect(),
    }
}

/// The journaled `cost_micro_usd` of every `EffectRecorded` whose node passes `pred`.
fn journaled_costs(
    events: &[(Seq, JournalEvent)],
    pred: impl Fn(&str) -> bool,
) -> Vec<Option<u64>> {
    events
        .iter()
        .filter_map(|(_, e)| match e {
            JournalEvent::EffectRecorded { node, usage, .. } if pred(&node.0) => {
                Some(usage.and_then(|u| u.cost_micro_usd))
            }
            _ => None,
        })
        .collect()
}

/// THE done-when: a run capped at $X pauses before exceeding it, resumes when the cap is
/// raised, and re-spends nothing it already paid for.
///
/// Cap `$0.10` = 100 000 micro-dollars, each call 21 000. Calls 1–3 go out (spend 63 000);
/// before call 4 the remaining 37 000, less the input estimate, affords fewer than
/// `MIN_OUTPUT_TOKENS` output tokens at 200 each, so the run PAUSES with `spent < cap` —
/// the money clamp's floor, exactly as the token clamp's floor does. Raising to `$1` and
/// waking runs n4 and n5 only: n1–n3 replay from the memo.
#[tokio::test]
async fn a_money_capped_run_pauses_before_exceeding_its_cap_and_resumes_with_zero_respend() {
    let (gateway, seen, ests) = window_watching_clamp_gateway(10, 100).await;
    price_single_chain(&gateway, IN_PER_1K, OUT_PER_1K).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = chain_of(5);
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");

    let cap = 100_000;
    let first = exec
        .run_with_budget(run, &graph, money(cap))
        .await
        .expect("drives");

    assert_eq!(
        seen.lock().unwrap().len(),
        3,
        "three calls fit under $0.10; the fourth must never reach the provider"
    );
    let pause = first.paused.as_ref().expect("the money cap pauses the run");
    assert_eq!(pause.node.0, "n4");
    assert!(pause.reason.starts_with("budget: "), "{}", pause.reason);
    let events = journal.load(run).await.unwrap();
    assert_eq!(crate::money_spend_of(&events), (63_000, Some(cap)));
    assert!(
        crate::money_spend_of(&events).0 <= cap,
        "the cap is never exceeded"
    );
    assert_eq!(
        journaled_costs(&events, |n| n.starts_with('n')),
        vec![Some(21_000); 3],
        "each call's cost is journaled on its own effect record — the durable ledger"
    );
    // The clamp: call 1's `max_tokens` is what the remaining money affords after the
    // input estimate, at the output price — under the model's own 1024, so money binds.
    let est = u64::from(ests.lock().unwrap()[0]);
    let affordable = (cap - est * IN_MICRO) / OUT_MICRO;
    assert!(affordable < u64::from(FIXTURE_MAX_OUTPUT_TOKENS));
    assert_eq!(
        seen.lock().unwrap()[0],
        Some(affordable as u32),
        "max_tokens is the remaining dollars converted at the chain's output price"
    );
    let snap = journal
        .latest_snapshot(run)
        .await
        .unwrap()
        .expect("a round-boundary snapshot was written");
    assert_eq!(
        (snap.spent_micro_usd, snap.money_budget_micro_usd),
        (63_000, Some(cap)),
        "the snapshot carries the money half of the ledger"
    );

    // Raise the cap and wake: n4 and n5 run, n1–n3 replay from the memo.
    journal
        .append(
            run,
            JournalEvent::MoneyBudgetRaised {
                new_total_micro_usd: 1_000_000,
            },
        )
        .await
        .unwrap();
    let second = exec.start(run, &graph).await.expect("resumes");
    assert!(
        second.paused.is_none() && second.failed.is_none(),
        "{second:?}"
    );
    assert_eq!(
        seen.lock().unwrap().len(),
        5,
        "zero re-spend: only n4 and n5 reach the provider after the raise"
    );
    assert_eq!(
        crate::money_spend_of(&journal.load(run).await.unwrap()),
        (105_000, Some(1_000_000))
    );
}

/// One call cannot spend past what is left: with a model that would happily emit 5000
/// tokens, `max_tokens` holds the reply to what the remaining dollars afford, and the
/// recorded cost lands at or under the cap. The adapter reports ONE input token, under
/// any estimate, so the documented residual (`actual_input − est_input`) is not in play
/// and the bound is exact.
#[tokio::test]
async fn the_money_clamp_bounds_a_single_call_to_the_remaining_dollars() {
    let (gateway, seen) = clamp_observing_gateway(1, 5_000).await;
    price_single_chain(&gateway, IN_PER_1K, OUT_PER_1K).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");

    let cap = 100_000;
    let out = exec
        .run_with_budget(run, &chain_of(1), money(cap))
        .await
        .expect("drives");
    assert!(out.paused.is_none() && out.failed.is_none(), "{out:?}");
    let sent = seen.lock().unwrap()[0].expect("a money-capped Chat is clamped");
    assert!(
        u64::from(sent) * OUT_MICRO <= cap,
        "max_tokens {sent} must be affordable"
    );
    let (spent, _) = crate::money_spend_of(&journal.load(run).await.unwrap());
    assert!(spent <= cap, "spent {spent} must stay within cap {cap}");
    assert!(
        spent > cap - 2 * OUT_MICRO - IN_MICRO * 10,
        "and the clamp spends what it can rather than refusing early: {spent}"
    );
}

/// A money cap cannot measure a model without a price, so a chain with an unpriced entry
/// is refused BEFORE dispatch, as a node failure — the same class as an unmetered call
/// under a token cap. The provider is never called.
#[tokio::test]
async fn an_unpriced_chain_fails_closed_under_a_money_cap_without_calling_the_provider() {
    let (gateway, seen) = clamp_observing_gateway(10, 100).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let out = exec
        .run_with_budget(run, &chain_of(1), money(1_000_000))
        .await
        .expect("drives");
    assert_eq!(
        seen.lock().unwrap().len(),
        0,
        "nothing is dispatched unmeasured"
    );
    let (node, error) = out.failed.as_ref().expect("the node fails closed");
    assert_eq!(node.0, "n1");
    assert!(error.contains("unpriced"), "{error}");
    assert!(out.paused.is_none());
}

/// A provider that reports no usage reports no cost either, and under a money-ONLY cap
/// that call is refused exactly as it is under a token cap: a node failure.
#[tokio::test]
async fn an_unmetered_call_fails_the_node_under_a_money_only_cap() {
    let (gateway, _calls) = metered_gateway(None).await;
    price_single_chain(&gateway, IN_PER_1K, OUT_PER_1K).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let out = exec
        .run_with_budget(run, &chain_of(1), money(1_000_000))
        .await
        .expect("drives");
    let (_, error) = out
        .failed
        .as_ref()
        .expect("an unmeasured call fails closed");
    assert!(error.contains("unmetered"), "{error}");
}

/// Additivity: a TOKEN-only run on a PRICED chain journals no cost and no money cap — the
/// cost is ledgered only while a money cap is in force, so nothing about the run's
/// journal changes because its models happen to carry prices.
#[tokio::test]
async fn a_token_only_run_on_a_priced_chain_journals_no_money() {
    let (gateway, _seen) = clamp_observing_gateway(10, 100).await;
    price_single_chain(&gateway, IN_PER_1K, OUT_PER_1K).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let out = exec
        .run_budgeted(
            run,
            &chain_of(2),
            Some(orchestrator_core::TokenBudget {
                total_tokens: 1_000_000,
            }),
        )
        .await
        .expect("drives");
    assert!(out.paused.is_none() && out.failed.is_none(), "{out:?}");
    let events = journal.load(run).await.unwrap();
    assert_eq!(journaled_costs(&events, |_| true), vec![None, None]);
    assert_eq!(crate::money_spend_of(&events), (0, None));
    for (_, e) in &events {
        let json = serde_json::to_string(e).unwrap();
        assert!(
            !json.contains("micro_usd") && !json.contains("money_budget"),
            "no money field may appear on a token-only run's journal: {json}"
        );
    }
}

/// The money cap takes the same 1-permit gate the token cap does, so a concurrent `Map`
/// cannot read an unchanged ledger six times and walk past the cap together. One child
/// spends 21 000 of 60 000; the rest are left under the floor and refused.
#[tokio::test]
async fn a_money_capped_map_fanout_dispatches_one_child_before_the_gate_fires() {
    let (gateway, calls) = metered_latency_gateway(
        Some(kernel::types::cost::TokenUsage {
            input_tokens: 10,
            output_tokens: 100,
            total_tokens: 110,
        }),
        std::time::Duration::from_millis(20),
    )
    .await;
    price_single_chain(&gateway, IN_PER_1K, OUT_PER_1K).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = map_graph(
        "m",
        map_items(["i0", "i1", "i2", "i3", "i4", "i5"]),
        Aggregation::BestEffort,
    );
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    exec.run_with_budget(run, &graph, money(60_000))
        .await
        .expect("drives");
    assert_eq!(
        calls.lock().unwrap().len(),
        1,
        "a money-capped run serialises its model calls"
    );
    let (spent, cap) = crate::money_spend_of(&journal.load(run).await.unwrap());
    assert_eq!((spent, cap), (21_000, Some(60_000)));
}

/// Producer coverage: the ReAct turn and the Map item journal their cost too (the
/// `ModelCall` node is covered above; `Consolidate` and the selector by their own tests
/// below). Every producer converts through `Fold::recorded_usage` (or the meter's mirror),
/// but nothing in the type system stops a site calling `content::recorded_usage` with
/// `priced: false` — so each of the five producers is pinned by a test, not by the shape.
#[tokio::test]
async fn the_react_turn_and_map_item_producers_journal_their_cost() {
    let usage = kernel::types::cost::TokenUsage {
        input_tokens: 10,
        output_tokens: 100,
        total_tokens: 110,
    };
    let with_usage = |mut r: kernel::types::io::ChatResponse| {
        r.usage = Some(usage.clone());
        r
    };
    let (gateway, _calls) = scripted_gateway(vec![
        with_usage(tool_call_response(
            "t0",
            "calc",
            "{\"op\":\"add\",\"a\":1,\"b\":1}",
        )),
        with_usage(final_response("done")),
    ])
    .await;
    price_single_chain(&gateway, IN_PER_1K, OUT_PER_1K).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("n1".into()),
            kind: NodeKind::Agent {
                agent: AgentRef("a".into()),
                input: serde_json::json!("hi"),
                phase: None,
            },
            deps: vec![],
        }],
    };
    let out = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools())
        .run_with_budget(run, &graph, money(10_000_000))
        .await
        .expect("drives");
    assert!(out.failed.is_none() && out.paused.is_none(), "{out:?}");
    let events = journal.load(run).await.unwrap();
    assert_eq!(
        journaled_costs(&events, |n| n == "n1")
            .into_iter()
            .filter(|c| *c == Some(21_000))
            .count(),
        2,
        "both ReAct turns journal their cost"
    );

    let (gateway, _calls) = metered_gateway(Some(usage.clone())).await;
    price_single_chain(&gateway, IN_PER_1K, OUT_PER_1K).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let out = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .run_with_budget(
            run,
            &map_graph("m", map_items(["i0", "i1"]), Aggregation::BestEffort),
            money(10_000_000),
        )
        .await
        .expect("drives");
    assert!(out.failed.is_none() && out.paused.is_none(), "{out:?}");
    assert_eq!(
        journaled_costs(&journal.load(run).await.unwrap(), |n| n.starts_with("m/")),
        vec![Some(21_000), Some(21_000)],
        "each Map child journals its own cost"
    );
}

/// The scheduler's submit path journals the money cap and records the pause, so torii's
/// `run submit` can hand the engine a dollar limit.
#[tokio::test]
async fn submit_with_budget_journals_the_money_cap_and_records_the_pause() {
    use crate::Scheduler;
    use orchestrator_core::{RunStatus, SchedulerStore};
    use orchestrator_store::InMemorySchedulerStore;

    let (gateway, seen) = clamp_observing_gateway(10, 100).await;
    price_single_chain(&gateway, IN_PER_1K, OUT_PER_1K).await;
    let journal = InMemoryJournal::new();
    let store = Arc::new(InMemorySchedulerStore::new());
    let run = RunId(uuid::Uuid::new_v4());
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let sched = Scheduler::new(
        store.clone(),
        exec,
        Arc::new(journal.clone()),
        Arc::new(orchestrator_core::SystemClock),
    );
    let out = sched
        .submit_with_budget(run, chain_of(5), money(100_000))
        .await
        .expect("submits");
    assert_eq!(out.paused.as_ref().map(|p| p.node.0.as_str()), Some("n4"));
    assert_eq!(seen.lock().unwrap().len(), 3);
    assert_eq!(
        store.status(run).await.unwrap().unwrap().status,
        RunStatus::Paused
    );
    let started = journal.load(run).await.unwrap();
    assert!(matches!(
        &started[0].1,
        JournalEvent::RunStarted {
            money_budget: Some(MoneyBudget {
                total_micro_usd: 100_000
            }),
            ..
        }
    ));
}

/// The money GATE's boundary: spend EXACTLY at the cap stops the run on the gate (`>=`),
/// not on the floor one line later. Seeded from a journal so the gate is the first thing
/// the next call meets; the reason wording tells the gate's message from the floor's,
/// which is what makes a `>=` → `>` change visible.
#[tokio::test]
async fn spending_exactly_the_money_cap_stops_the_run_on_the_gate() {
    let (gateway, seen) = clamp_observing_gateway(10, 100).await;
    price_single_chain(&gateway, IN_PER_1K, OUT_PER_1K).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let cap = 21_000;
    journal
        .append(
            run,
            JournalEvent::RunStarted {
                version: "v1".into(),
                budget: None,
                money_budget: Some(MoneyBudget {
                    total_micro_usd: cap,
                }),
            },
        )
        .await
        .unwrap();
    journal
        .append(
            run,
            JournalEvent::EffectRecorded {
                node: NodeId("n1".into()),
                effect_id: effect_id("n1", 0, 0),
                class: EffectClass::Pure,
                input_hash: input_hash("c", &serde_json::json!({ "prompt": "p1" })).unwrap(),
                seq: 0,
                output: EffectOutput::Inline(serde_json::json!({ "model": "m", "text": "x" })),
                observation: None,
                usage: Some(orchestrator_core::TokenUsage {
                    input_tokens: 10,
                    output_tokens: 100,
                    total_tokens: 110,
                    cost_micro_usd: Some(cap),
                }),
            },
        )
        .await
        .unwrap();
    journal
        .append(
            run,
            JournalEvent::NodeCompleted {
                node: NodeId("n1".into()),
            },
        )
        .await
        .unwrap();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let out = exec.start(run, &chain_of(2)).await.expect("drives");
    assert_eq!(seen.lock().unwrap().len(), 0, "n1 replays; n2 is gated");
    let pause = out.paused.as_ref().expect("spent == cap pauses");
    assert_eq!(pause.node.0, "n2");
    assert!(
        pause
            .reason
            .starts_with("budget: $0.021000 of $0.021000 spent against the run's money cap"),
        "the GATE's message, in dollars: {}",
        pause.reason
    );
}

/// On a MONEY-only run a model bound (here the chain's 200-token output limit, under the
/// 256 floor) refuses against the MONEY cap — in dollars — rather than naming a token cap
/// the run does not have.
#[tokio::test]
async fn a_money_only_run_refused_by_a_model_bound_reports_against_the_money_cap() {
    let (gateway, seen) = sub_floor_output_clamp_observing_gateway(10, 100).await;
    price_single_chain_with_output_limit(
        &gateway,
        IN_PER_1K,
        OUT_PER_1K,
        SUB_FLOOR_MAX_OUTPUT_TOKENS,
    )
    .await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let out = exec
        .run_with_budget(run, &chain_of(1), money(10_000_000))
        .await
        .expect("drives");
    assert_eq!(seen.lock().unwrap().len(), 0);
    let pause = out.paused.as_ref().expect("a sub-floor ceiling pauses");
    assert!(
        pause.reason.contains("run's money cap")
            && pause.reason.contains("$0.000000 of $10.000000"),
        "a money-only run's refusal names the money cap, in dollars: {}",
        pause.reason
    );
}

/// With BOTH caps set, the tighter one clamps — in either direction. Money binds the first
/// case (about 500 affordable tokens against a token allowance near a million); the token
/// cap binds the second (exactly 300 tokens left after the estimate, against ~50 000
/// affordable).
#[tokio::test]
async fn with_both_caps_the_tighter_one_clamps() {
    for money_binds in [true, false] {
        let (gateway, seen, ests) = window_watching_clamp_gateway(1, 5_000).await;
        price_single_chain(&gateway, IN_PER_1K, OUT_PER_1K).await;
        let journal = InMemoryJournal::new();
        let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
        // An unbudgeted probe of the same one-node graph records the gateway's estimate of
        // its prompt, so the token cap can sit exactly 300 above it.
        exec.run(RunId(uuid::Uuid::new_v4()), &chain_of(1))
            .await
            .expect("probe");
        let est = u64::from(ests.lock().unwrap()[0]);
        let (token_cap, micro) = if money_binds {
            (1_000_000, 100_000)
        } else {
            (est + 300, 10_000_000)
        };
        let out = exec
            .run_with_budget(
                RunId(uuid::Uuid::new_v4()),
                &chain_of(1),
                RunBudget {
                    tokens: Some(orchestrator_core::TokenBudget {
                        total_tokens: token_cap,
                    }),
                    money: Some(MoneyBudget {
                        total_micro_usd: micro,
                    }),
                },
            )
            .await
            .expect("drives");
        assert!(out.paused.is_none() && out.failed.is_none(), "{out:?}");
        let sent = seen.lock().unwrap()[1].expect("a capped Chat is clamped");
        let expected = if money_binds {
            (micro - est * IN_MICRO) / OUT_MICRO
        } else {
            300
        };
        assert_eq!(
            u64::from(sent),
            expected,
            "money_binds={money_binds}: the tighter cap's allowance is what is sent"
        );
    }
}

/// A chat adapter that, on its FIRST call, appends a `MoneyBudgetRaised` to the run's
/// journal — modelling an operator (or torii, cross-process) moving the money cap while a
/// drive is in flight. Reports `usage` on every response so the gateway prices each call.
struct RaisingMidDriveAdapter {
    journal: InMemoryJournal,
    run: RunId,
    calls: Arc<std::sync::Mutex<usize>>,
}

impl gateway::adapters::capability::Model for RaisingMidDriveAdapter {
    fn id(&self) -> &str {
        "r"
    }
}

#[async_trait::async_trait]
impl gateway::adapters::capability::ChatModel for RaisingMidDriveAdapter {
    async fn chat(
        &self,
        _cfg: &kernel::types::config::RouterConfig,
        req: &kernel::types::io::ChatRequest,
    ) -> Result<kernel::types::io::ChatResponse, kernel::types::error::GatewayError> {
        let first = {
            let mut n = self.calls.lock().unwrap();
            *n += 1;
            *n == 1
        };
        if first {
            self.journal
                .append(
                    self.run,
                    JournalEvent::MoneyBudgetRaised {
                        new_total_micro_usd: 30_000,
                    },
                )
                .await
                .unwrap();
        }
        Ok(kernel::types::io::ChatResponse {
            content: Some("canned-response".into()),
            tool_calls: Vec::new(),
            usage: Some(kernel::types::cost::TokenUsage {
                input_tokens: 10,
                output_tokens: 100,
                total_tokens: 110,
            }),
            model: req.model.clone(),
            degraded: false,
        })
    }
}

/// HIGH (AG-12 review): a money cap moved INTO a run that started without one, while a
/// drive is in flight, must never be reported over spend the ledger did not count.
///
/// The drive folded "no money cap" when it started, so every call it makes journals
/// `cost_micro_usd: None` — permanently: no later fold can recover a cost that was never
/// written. Reporting `(0, Some(30_000))` after five priced calls (105 000 micro-dollars)
/// would tell an operator a cap was in force and nothing was spent under it. The ledger
/// may only claim a money cap it has counted every call against.
#[tokio::test]
async fn a_money_cap_raised_into_an_uncapped_run_mid_drive_is_never_reported_over_uncounted_spend()
{
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let calls = Arc::new(std::sync::Mutex::new(0usize));
    let adapters = gateway::adapters::AdapterRegistry::new();
    adapters
        .register_chat(Arc::new(RaisingMidDriveAdapter {
            journal: journal.clone(),
            run,
            calls: calls.clone(),
        }))
        .await;
    let gateway = gateway::Gateway::new(
        crate::test_support::two_window_chain_config(),
        adapters,
        gateway::circuit_breaker::CircuitBreakerManager::new(
            gateway::circuit_breaker::CircuitBreakerConfig::default(),
        ),
    );
    price_single_chain(&gateway, IN_PER_1K, OUT_PER_1K).await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    exec.run_with_budget(run, &chain_of(5), RunBudget::default())
        .await
        .expect("drives");
    assert_eq!(
        *calls.lock().unwrap(),
        5,
        "an uncapped drive runs every node"
    );
    let events = journal.load(run).await.unwrap();
    let (spent, cap) = crate::money_spend_of(&events);
    assert!(
        cap.is_none() || spent > 0,
        "the ledger reports a money cap of {cap:?} over {spent} counted micro-dollars, after \
         five priced calls the cap's own drive never ledgered"
    );
}

/// MEDIUM (AG-12 review): the post-call `Uncosted` refusal is the only thing between a
/// money-capped run and spend its ledger cannot see. Reached in memory through the
/// unchecked `update_config`: a NEGATIVE input price prices the chain (so the pre-call
/// `Unpriced` check passes) but makes the gateway's `actual_cost` total negative, which
/// `content::cost_micro_usd` refuses to trust. The node fails closed and nothing reaches
/// the money ledger.
#[tokio::test]
async fn an_uncosted_response_fails_the_node_under_a_money_cap() {
    let (gateway, calls) = metered_gateway(Some(kernel::types::cost::TokenUsage {
        input_tokens: 10,
        output_tokens: 100,
        total_tokens: 110,
    }))
    .await;
    price_single_chain(&gateway, -10.0, OUT_PER_1K).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let out = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .run_with_budget(run, &chain_of(2), money(1_000_000))
        .await
        .expect("drives");
    assert_eq!(
        calls.lock().unwrap().len(),
        1,
        "the provider was called once"
    );
    let (node, error) = out.failed.as_ref().expect("an uncosted call fails closed");
    assert_eq!(node.0, "n1");
    assert!(error.starts_with("uncosted model call: "), "{error}");
    let events = journal.load(run).await.unwrap();
    assert!(
        events.iter().any(|(_, e)| matches!(
            e,
            JournalEvent::NodeFailed { node, error } if node.0 == "n1" && error.starts_with("uncosted")
        )),
        "the refusal is journaled as the node's failure"
    );
    assert!(
        journaled_costs(&events, |_| true).is_empty(),
        "nothing uncosted is journaled as an effect"
    );
    assert_eq!(crate::money_spend_of(&events), (0, Some(1_000_000)));
}

/// MEDIUM (AG-12 review): the money clamp reserves the chain's flat `per_request` fee
/// before converting what is left into output tokens. With a `$0.01` fee (10 000
/// micro-dollars) against a `$0.10` cap, `max_tokens` is exactly
/// `(cap − fee − est·IN) / OUT`; a clamp that forgot the fee would size the reply as if
/// the fee were free and overshoot the remaining dollars by the whole fee.
#[tokio::test]
async fn the_money_clamp_reserves_the_per_request_fee() {
    let (gateway, seen, ests) = window_watching_clamp_gateway(1, 100).await;
    price_single_chain_with_fee(&gateway, IN_PER_1K, OUT_PER_1K, 0.01).await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let cap = 100_000;
    let fee = 10_000;
    let out = exec
        .run_with_budget(RunId(uuid::Uuid::new_v4()), &chain_of(1), money(cap))
        .await
        .expect("drives");
    assert!(out.paused.is_none() && out.failed.is_none(), "{out:?}");
    let est = u64::from(ests.lock().unwrap()[0]);
    assert_eq!(
        seen.lock().unwrap()[0],
        Some(((cap - fee - est * IN_MICRO) / OUT_MICRO) as u32),
        "max_tokens is what is left after the fee and the input estimate"
    );
}

/// The fee's sibling: when the fee alone leaves fewer than `MIN_OUTPUT_TOKENS` affordable,
/// the run pauses on the floor BEFORE dispatch. `$0.09` of a `$0.10` cap leaves under
/// 10 000 micro-dollars — under 50 output tokens at 200 each — where ignoring the fee
/// would afford ~495 and send the call.
#[tokio::test]
async fn a_per_request_fee_that_leaves_less_than_the_floor_pauses_before_dispatch() {
    let (gateway, seen) = clamp_observing_gateway(1, 100).await;
    price_single_chain_with_fee(&gateway, IN_PER_1K, OUT_PER_1K, 0.09).await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let out = exec
        .run_with_budget(RunId(uuid::Uuid::new_v4()), &chain_of(1), money(100_000))
        .await
        .expect("drives");
    assert!(seen.lock().unwrap().is_empty(), "nothing is dispatched");
    let pause = out
        .paused
        .as_ref()
        .expect("the fee leaves too little: pause");
    assert!(pause.reason.starts_with("budget: "), "{}", pause.reason);
}

fn priced_usage() -> kernel::types::cost::TokenUsage {
    kernel::types::cost::TokenUsage {
        input_tokens: 10,
        output_tokens: 100,
        total_tokens: 110,
    }
}

/// MEDIUM (AG-12 review): the `Consolidate` synthesis journals its cost. Nothing but this
/// test stops that producer from converting its usage with the cost dropped — the money
/// cap would see the call live and lose it on every resume.
#[tokio::test]
async fn the_consolidate_producer_journals_its_cost() {
    let (gateway, _calls) = metered_gateway(Some(priced_usage())).await;
    price_single_chain(&gateway, IN_PER_1K, OUT_PER_1K).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![
            Node {
                id: NodeId("m".into()),
                kind: NodeKind::Map {
                    body: MapBody::ModelCall { chain: "c".into() },
                    over: map_items(["i0"]),
                    concurrency: 1,
                    aggregation: Aggregation::BestEffort,
                },
                deps: vec![],
            },
            Node {
                id: NodeId("cons".into()),
                kind: NodeKind::Consolidate {
                    over: NodeId("m".into()),
                    min_viable: 1,
                    body: MapBody::ModelCall { chain: "c".into() },
                },
                deps: vec![Dep::soft("m")],
            },
        ],
    };
    let out = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .run_with_budget(run, &graph, money(10_000_000))
        .await
        .expect("drives");
    assert!(out.failed.is_none() && out.paused.is_none(), "{out:?}");
    let events = journal.load(run).await.unwrap();
    assert_eq!(
        journaled_costs(&events, |n| n == "cons"),
        vec![Some(21_000)],
        "the Consolidate synthesis journals its cost"
    );
    assert_eq!(crate::money_spend_of(&events).0, 42_000);
}

/// MEDIUM (AG-12 review): the planner selector's lent dispatch journals its cost on the
/// `e/__select__` record. `PlannerSelected` memoizes the choice, so a resumed run never
/// re-invokes the selector: a cost it did not journal is gone from the money ledger for
/// good. The run fails (the canned reply is not a candidate) — deliberately, as in
/// `the_planner_selector_journals_its_spend_to_the_ledger`: the money was spent either way.
#[tokio::test]
async fn the_planner_selector_producer_journals_its_cost() {
    let (gateway, calls) =
        metered_latency_gateway(Some(priced_usage()), std::time::Duration::ZERO).await;
    price_single_chain(&gateway, IN_PER_1K, OUT_PER_1K).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let registry = two_planner_registry();
    let graph = Graph {
        nodes: vec![expand_select_node("e", vec![])],
    };
    Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(registry.clone())
        .with_planner_selector(Arc::new(crate::LlmPlannerSelector::new(registry, "c")))
        .run_with_budget(run, &graph, money(10_000_000))
        .await
        .expect("drives");
    assert_eq!(
        calls.lock().unwrap().len(),
        1,
        "the selector dispatched once"
    );
    let select = format!("e/{}", orchestrator_core::RESERVED_SELECT_ID);
    let events = journal.load(run).await.unwrap();
    assert_eq!(
        journaled_costs(&events, |n| n == select),
        vec![Some(21_000)],
        "the selector's call journals its cost"
    );
    assert_eq!(crate::money_spend_of(&events).0, 21_000);
}

/// MEDIUM (AG-12 review): a chain with a ZERO output price and a positive input price
/// must still refuse a call whose input cost alone exceeds the remaining dollars. Zero
/// output price makes every output token affordable — but only once the call's
/// reservation (fee + input estimate) fits at all. Here one input token costs
/// 100 000 micro-dollars against a 100 micro-dollar cap.
#[tokio::test]
async fn a_free_output_chain_still_refuses_a_call_whose_input_alone_exceeds_the_cap() {
    let (gateway, seen) = clamp_observing_gateway(1, 10).await;
    price_single_chain(&gateway, 100.0, 0.0).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let out = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .run_with_budget(run, &chain_of(1), money(100))
        .await
        .expect("drives");
    let (spent, _) = crate::money_spend_of(&journal.load(run).await.unwrap());
    assert!(
        seen.lock().unwrap().is_empty(),
        "nothing may be dispatched: spent {spent} against a cap of 100"
    );
    let pause = out.paused.as_ref().expect("the floor pauses the run");
    assert!(pause.reason.starts_with("budget: "), "{}", pause.reason);
}
