//! AG-3 (#87) × AG-12 (#89): an automatic scheduler retry never re-dispatches a PAID
//! effect whose spend went unrecorded.
//!
//! A producer charges a successful model call to the drive's in-memory meter and then
//! journals its `EffectRecorded { usage }` — the durable ledger. If THAT append fails, the
//! provider has been paid but the ledger never heard of it. AG-3 allowlists a journal
//! backend fault as retryable, so before this fix the scheduler re-drove the run: the
//! retry's meter restarted from the journal (which lacks the call), the effect had no memo,
//! and the call was dispatched — and paid for — again, walking straight past the cap.
//!
//! Every test here prices the single-chain fixture at `$0.1 / 1k` input and `$0.2 / 1k`
//! output (100 and 200 micro-dollars per token) and has each call report 10 input and 295
//! output tokens: `10·100 + 295·200 = 60 000` micro-dollars a call. The cap the wake runs
//! under is `100 000`: ONE call fits, a second paid copy of it (`120 000`) would not.

use super::*;
use crate::Scheduler;
use crate::test_support::{FakeClock, price_single_chain};
use chrono::{DateTime, Duration, Utc};
use orchestrator_core::{MoneyBudget, RunBudget, RunStatus, SchedulerStore};
use orchestrator_store::InMemorySchedulerStore;
use std::sync::atomic::{AtomicBool, Ordering};

const COST_PER_CALL: u64 = 60_000;
const CAP: u64 = 100_000;

fn money(total_micro_usd: u64) -> RunBudget {
    RunBudget {
        tokens: None,
        money: Some(MoneyBudget { total_micro_usd }),
    }
}

fn t0() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(1_000_000, 0).unwrap()
}

/// An in-memory journal that, once ARMED, fails exactly one `EffectRecorded` append for a
/// node whose id starts with `node` with a retryable backend fault, then heals.
struct FailSpendOnce {
    inner: InMemoryJournal,
    node: &'static str,
    armed: AtomicBool,
}

impl FailSpendOnce {
    fn new(node: &'static str) -> Arc<Self> {
        Arc::new(Self {
            inner: InMemoryJournal::new(),
            node,
            armed: AtomicBool::new(false),
        })
    }
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl ExecutionJournal for FailSpendOnce {
    async fn append(
        &self,
        run: RunId,
        event: JournalEvent,
    ) -> Result<Seq, orchestrator_core::JournalError> {
        if let JournalEvent::EffectRecorded { node, .. } = &event
            && node.0.starts_with(self.node)
            && self.armed.swap(false, Ordering::SeqCst)
        {
            return Err(orchestrator_core::JournalError::Backend(
                "the database blinked".into(),
            ));
        }
        self.inner.append(run, event).await
    }
    async fn load(
        &self,
        run: RunId,
    ) -> Result<Vec<(Seq, JournalEvent)>, orchestrator_core::JournalError> {
        self.inner.load(run).await
    }
    async fn load_since(
        &self,
        run: RunId,
        since: Seq,
    ) -> Result<Vec<(Seq, JournalEvent)>, orchestrator_core::JournalError> {
        self.inner.load_since(run, since).await
    }
}

fn retry(max_attempts: u32) -> crate::WakeRetryPolicy {
    crate::WakeRetryPolicy {
        max_attempts,
        base_backoff: Duration::seconds(10),
        max_backoff: Duration::hours(1),
        jitter: 0.0,
        jitter_seed: 0,
    }
}

/// Submit `graph` under a cap too small for ANY call (so it pauses before spending), raise
/// the cap to [`CAP`], arm the fault, force a wake, and then keep ticking well past every
/// backoff the policy could schedule. Returns the scheduler's final row for the run.
async fn wake_into_one_failed_spend_record(
    exec: Executor,
    journal: Arc<FailSpendOnce>,
    graph: Graph,
) -> (RunId, orchestrator_core::ScheduledRun) {
    let store = Arc::new(InMemorySchedulerStore::new());
    let clock = FakeClock::new(t0());
    let run = RunId(uuid::Uuid::new_v4());
    let sched = Scheduler::new(
        store.clone(),
        exec.with_clock(clock.clone()),
        journal.clone(),
        clock.clone(),
    )
    .with_wake_retry(retry(3));

    let first = sched
        .submit_with_budget(run, graph.clone(), money(1))
        .await
        .expect("submits");
    assert!(
        first.paused.is_some(),
        "a 1-micro-dollar cap pauses before the first call: {first:?}"
    );
    journal
        .append(
            run,
            JournalEvent::MoneyBudgetRaised {
                new_total_micro_usd: CAP,
            },
        )
        .await
        .unwrap();
    journal.arm();
    sched.force_wake(run).await.unwrap();

    // Attempt 1 dispatches the call and fails to journal its spend. Then walk the clock
    // past every backoff `retry(3)` could schedule (10s, 20s) and tick at each step.
    assert_eq!(sched.tick().await.unwrap(), 1, "the forced wake is driven");
    for secs in [10, 30, 70, 150, 3_700] {
        clock.set(t0() + Duration::seconds(secs));
        sched.tick().await.unwrap();
    }
    let row = store.status(run).await.unwrap().expect("the run has a row");
    (run, row)
}

/// THE property, `ModelCall` producer: the paid call whose `EffectRecorded` append failed is
/// never dispatched again by an automatic retry — the provider is called exactly once, the
/// run never spends past its cap, and the run is filed terminal with a reason that names
/// the unrecorded spend rather than being backed off into a re-spend.
#[tokio::test]
async fn a_paid_model_call_whose_spend_was_not_journaled_is_never_redispatched_by_a_retry() {
    let (gateway, seen) = clamp_observing_gateway(10, 295).await;
    price_single_chain(&gateway, 0.1, 0.2).await;
    let journal = FailSpendOnce::new("n1");
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("n1".into()),
            kind: model_call("c", "go"),
            deps: vec![],
        }],
    };
    let exec = Executor::new(Arc::new(gateway), journal.clone(), "v1");

    let (_run, row) = wake_into_one_failed_spend_record(exec, journal, graph).await;

    let calls = seen.lock().unwrap().len() as u64;
    assert_eq!(
        calls, 1,
        "the paid call is dispatched exactly once — a retry would pay for it again"
    );
    assert!(
        calls * COST_PER_CALL <= CAP,
        "the provider was paid {} against a cap of {CAP}",
        calls * COST_PER_CALL
    );
    assert_eq!(row.status, RunStatus::Failed, "{row:?}");
    let reason = row.reason.unwrap_or_default();
    assert!(
        reason.contains("spend") && reason.contains("not recorded"),
        "the terminal reason names the unrecorded spend: {reason}"
    );
}

/// The same property for the ReAct-turn producer (`dispatch_model_turn`): an agent turn's
/// spend that failed to journal is not re-bought by a retry.
#[tokio::test]
async fn a_paid_react_turn_whose_spend_was_not_journaled_is_never_redispatched_by_a_retry() {
    let usage = kernel::types::cost::TokenUsage {
        input_tokens: 10,
        output_tokens: 295,
        total_tokens: 305,
    };
    let with_usage = |mut r: kernel::types::io::ChatResponse| {
        r.usage = Some(usage.clone());
        r
    };
    // Two scripted finals: the second is what an (unfixed) retry would buy.
    let (gateway, calls) = scripted_gateway(vec![
        with_usage(final_response("done")),
        with_usage(final_response("done again")),
    ])
    .await;
    price_single_chain(&gateway, 0.1, 0.2).await;
    let journal = FailSpendOnce::new("n1");
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
    let exec = Executor::new(Arc::new(gateway), journal.clone(), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools());

    let (_run, row) = wake_into_one_failed_spend_record(exec, journal, graph).await;

    let calls = calls.lock().unwrap().len() as u64;
    assert_eq!(
        calls, 1,
        "the paid ReAct turn is dispatched exactly once — a retry would pay for it again"
    );
    assert!(
        calls * COST_PER_CALL <= CAP,
        "the provider was paid {} against a cap of {CAP}",
        calls * COST_PER_CALL
    );
    assert_eq!(row.status, RunStatus::Failed, "{row:?}");
    let reason = row.reason.unwrap_or_default();
    assert!(
        reason.contains("spend") && reason.contains("not recorded"),
        "the terminal reason names the unrecorded spend: {reason}"
    );
}

/// The same property for the Map-item producer: a fan-out child's paid call whose spend
/// failed to journal is not re-bought by a retry of the whole Map.
#[tokio::test]
async fn a_paid_map_item_whose_spend_was_not_journaled_is_never_redispatched_by_a_retry() {
    let (gateway, seen) = clamp_observing_gateway(10, 295).await;
    price_single_chain(&gateway, 0.1, 0.2).await;
    let journal = FailSpendOnce::new("m/");
    let graph = map_graph("m", map_items(["i0"]), Aggregation::BestEffort);
    let exec = Executor::new(Arc::new(gateway), journal.clone(), "v1");

    let (_run, row) = wake_into_one_failed_spend_record(exec, journal, graph).await;

    let calls = seen.lock().unwrap().len() as u64;
    assert_eq!(
        calls, 1,
        "the paid Map item is dispatched exactly once — a retry would pay for it again"
    );
    assert!(calls * COST_PER_CALL <= CAP);
    assert_eq!(row.status, RunStatus::Failed, "{row:?}");
    let reason = row.reason.unwrap_or_default();
    assert!(
        reason.contains("spend") && reason.contains("not recorded"),
        "the terminal reason names the unrecorded spend: {reason}"
    );
}

/// The other half of the classification: a journal fault BEFORE any paid dispatch is still
/// retryable — nothing was bought, so a retry costs nothing it has not already accounted
/// for. The `NodeStarted` append fails once; the retry then pays for the call exactly once
/// and the run completes.
#[tokio::test]
async fn a_journal_fault_before_any_paid_dispatch_is_still_retried() {
    struct FailStartOnce {
        inner: InMemoryJournal,
        armed: AtomicBool,
    }
    #[async_trait::async_trait]
    impl ExecutionJournal for FailStartOnce {
        async fn append(
            &self,
            run: RunId,
            event: JournalEvent,
        ) -> Result<Seq, orchestrator_core::JournalError> {
            if matches!(&event, JournalEvent::NodeStarted { node } if node.0 == "n1")
                && self.armed.swap(false, Ordering::SeqCst)
            {
                return Err(orchestrator_core::JournalError::Backend(
                    "the database blinked".into(),
                ));
            }
            self.inner.append(run, event).await
        }
        async fn load(
            &self,
            run: RunId,
        ) -> Result<Vec<(Seq, JournalEvent)>, orchestrator_core::JournalError> {
            self.inner.load(run).await
        }
        async fn load_since(
            &self,
            run: RunId,
            since: Seq,
        ) -> Result<Vec<(Seq, JournalEvent)>, orchestrator_core::JournalError> {
            self.inner.load_since(run, since).await
        }
    }

    let (gateway, seen) = clamp_observing_gateway(10, 295).await;
    price_single_chain(&gateway, 0.1, 0.2).await;
    let journal = Arc::new(FailStartOnce {
        inner: InMemoryJournal::new(),
        armed: AtomicBool::new(false),
    });
    let store = Arc::new(InMemorySchedulerStore::new());
    let clock = FakeClock::new(t0());
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("n1".into()),
            kind: model_call("c", "go"),
            deps: vec![],
        }],
    };
    let sched = Scheduler::new(
        store.clone(),
        Executor::new(Arc::new(gateway), journal.clone(), "v1").with_clock(clock.clone()),
        journal.clone(),
        clock.clone(),
    )
    .with_wake_retry(retry(3));
    sched
        .submit_with_budget(run, graph, money(1))
        .await
        .expect("submits");
    journal
        .append(
            run,
            JournalEvent::MoneyBudgetRaised {
                new_total_micro_usd: CAP,
            },
        )
        .await
        .unwrap();
    journal.armed.store(true, Ordering::SeqCst);
    sched.force_wake(run).await.unwrap();

    assert_eq!(sched.tick().await.unwrap(), 1);
    let st = store.status(run).await.unwrap().unwrap();
    assert_eq!(
        (st.status, st.next_wake),
        (RunStatus::Paused, Some(t0() + Duration::seconds(10))),
        "a fault before any spend is backed off, not filed terminal: {st:?}"
    );
    assert_eq!(seen.lock().unwrap().len(), 0, "nothing was bought yet");

    clock.set(t0() + Duration::seconds(10));
    assert_eq!(sched.tick().await.unwrap(), 1);
    assert_eq!(
        store.status(run).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    assert_eq!(seen.lock().unwrap().len(), 1, "the call is paid for once");
}
