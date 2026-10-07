//! AG-12 (#89): the money-denominated run budget, end to end on the in-memory stores.
//!
//! Every test here prices the single-chain fixture with [`price_single_chain`] at
//! `$0.1 / 1k` input and `$0.2 / 1k` output — 100 and 200 micro-dollars per token — so
//! each number below is arithmetic a reader can check by hand. A clamp-observing call that
//! reports 10 input and 100 output tokens therefore costs `10·100 + 100·200 = 21 000`
//! micro-dollars.

use super::*;
use crate::test_support::price_single_chain;
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
/// `ModelCall` node is covered above). Every producer converts its usage through the ONE
/// `Fold::recorded_usage`, so a producer cannot journal tokens and silently drop cost.
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
