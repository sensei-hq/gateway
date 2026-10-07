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
    let arm = journal.clone();
    wake_into_armed_faults(exec, journal, move || arm.arm(), graph, CAP, |_| {}).await
}

/// The general shape behind [`wake_into_one_failed_spend_record`]: submit `graph` under a
/// 1-micro-dollar cap (so it pauses before spending), raise the cap to `cap`, `arm` the
/// journal's faults, force a wake, call `after_wake_1` with the row as wake 1 left it, then
/// tick past every backoff the policy could schedule. Returns the final row.
async fn wake_into_armed_faults(
    exec: Executor,
    journal: Arc<dyn ExecutionJournal>,
    arm: impl FnOnce(),
    graph: Graph,
    cap: u64,
    after_wake_1: impl FnOnce(&orchestrator_core::ScheduledRun),
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
                new_total_micro_usd: cap,
            },
        )
        .await
        .unwrap();
    arm();
    sched.force_wake(run).await.unwrap();

    // Attempt 1 dispatches the call and fails to journal its spend. Then walk the clock
    // past every backoff `retry(3)` could schedule (10s, 20s) and tick at each step.
    assert_eq!(sched.tick().await.unwrap(), 1, "the forced wake is driven");
    after_wake_1(&store.status(run).await.unwrap().expect("the run has a row"));
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

// ---- Precedence: an unrecorded spend in ONE concurrent child outranks a retryable
// ---- fault in ANOTHER.

/// A cap roomy enough for every call the masking scenario could make.
const ROOMY_CAP: u64 = 10_000_000;

/// The journal behind the Map-masking scenario. Once ARMED it fails, once each:
///
/// - `m/0`'s first UNPAID `EffectRecorded` (its `calc` tool result, `usage: None`) — a plain
///   retryable `Journal(Backend)` fault that cost nothing; and
/// - `m/1`'s first PAID `EffectRecorded` (its ReAct turn, `usage: Some`) — the spend record
///   of a call the provider has already been paid for.
struct FailMaskedMapChildren {
    inner: InMemoryJournal,
    armed: AtomicBool,
    unpaid_m0: AtomicBool,
    paid_m1: AtomicBool,
}

impl FailMaskedMapChildren {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: InMemoryJournal::new(),
            armed: AtomicBool::new(false),
            unpaid_m0: AtomicBool::new(true),
            paid_m1: AtomicBool::new(true),
        })
    }
    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl ExecutionJournal for FailMaskedMapChildren {
    async fn append(
        &self,
        run: RunId,
        event: JournalEvent,
    ) -> Result<Seq, orchestrator_core::JournalError> {
        if self.armed.load(Ordering::SeqCst)
            && let JournalEvent::EffectRecorded { node, usage, .. } = &event
        {
            let blink = match (node.0.as_str(), usage.is_some()) {
                ("m/0", false) => self.unpaid_m0.swap(false, Ordering::SeqCst),
                ("m/1", true) => self.paid_m1.swap(false, Ordering::SeqCst),
                _ => false,
            };
            if blink {
                return Err(orchestrator_core::JournalError::Backend(
                    "the database blinked".into(),
                ));
            }
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

/// A Map over `[i0, i1]` whose body is the `calc`-holding agent `a`, at concurrency 4 — so
/// both children are driven under one `join_all`. Every scripted response reports usage,
/// so the money cap admits it; the script is long enough for whatever an (unfixed) retry
/// would buy.
async fn masking_map_fixture() -> (Executor, CallLog, Arc<FailMaskedMapChildren>, Graph) {
    let usage = kernel::types::cost::TokenUsage {
        input_tokens: 10,
        output_tokens: 295,
        total_tokens: 305,
    };
    let with_usage = |mut r: kernel::types::io::ChatResponse| {
        r.usage = Some(usage.clone());
        r
    };
    let calc = || tool_call_response("t1", "calc", "{\"op\":\"add\",\"a\":2,\"b\":3}");
    let (gateway, calls) = scripted_gateway(vec![
        with_usage(calc()),
        with_usage(calc()),
        with_usage(final_response("done")),
        with_usage(final_response("done")),
        with_usage(final_response("done")),
        with_usage(final_response("done")),
    ])
    .await;
    price_single_chain(&gateway, 0.1, 0.2).await;
    let journal = FailMaskedMapChildren::new();
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("m".into()),
            kind: NodeKind::Map {
                body: MapBody::Agent(AgentRef("a".into())),
                over: vec![serde_json::json!("i0"), serde_json::json!("i1")],
                concurrency: 4,
                aggregation: Aggregation::BestEffort,
            },
            deps: vec![],
        }],
    };
    let exec = Executor::new(Arc::new(gateway), journal.clone(), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools());
    (exec, calls, journal, graph)
}

/// THE precedence property, through the scheduler. `m/0` (the LOWER index) fails with a
/// retryable journal fault that bought nothing; `m/1` fails to journal a call it already
/// paid for. If the Map surfaced the first fatal by index, the retryable one would win, the
/// scheduler would back the run off, and the retry would buy `m/1`'s call again — a run
/// that ends `Completed` with a ledger missing a payment. Whatever ELSE went wrong in the
/// round, an unrecorded spend is what must reach the scheduler: wake 1 files the run
/// `Failed`, and no wake ever dispatches again.
#[tokio::test]
async fn an_unrecorded_spend_in_one_map_child_is_not_masked_by_a_retryable_fault_in_another() {
    let (exec, calls, journal, graph) = masking_map_fixture().await;
    let arm = journal.clone();

    let (_run, row) = wake_into_armed_faults(
        exec,
        journal,
        move || arm.arm(),
        graph,
        ROOMY_CAP,
        |row| {
            assert_eq!(
                row.status,
                RunStatus::Failed,
                "wake 1 files the run terminal — a retry would re-buy m/1's call: {row:?}"
            );
        },
    )
    .await;

    assert_eq!(
        calls.lock().unwrap().len(),
        2,
        "one paid turn per child, and never a second copy of m/1's"
    );
    assert_eq!(row.status, RunStatus::Failed, "{row:?}");
    let reason = row.reason.unwrap_or_default();
    assert!(
        reason.contains("spend") && reason.contains("not recorded"),
        "the terminal reason names the unrecorded spend, not m/0's blink: {reason}"
    );
}

/// The same scenario on a bare executor, to pin the TYPE rather than a message: the drive's
/// error is `SpendUnrecorded` naming `m/1`, not `m/0`'s `Journal(Backend)`.
#[tokio::test]
async fn a_map_surfaces_the_unrecorded_spend_as_its_typed_error_whatever_its_index() {
    let (exec, calls, journal, graph) = masking_map_fixture().await;
    journal.arm();

    let err = exec
        .run_with_budget(RunId(uuid::Uuid::new_v4()), &graph, money(ROOMY_CAP))
        .await
        .expect_err("a child's spend went unrecorded");

    assert!(
        matches!(&err, OrchestratorError::SpendUnrecorded { node, .. } if node.0 == "m/1"),
        "expected SpendUnrecorded at m/1, got {err:?}"
    );
    assert_eq!(calls.lock().unwrap().len(), 2, "one paid turn per child");
}

// ---- The planner-selector producer (`SelectorDispatch::complete`).

/// `"e/__select__"` — the reserved path the selector of `expand_select_node("e")`
/// journals its spend under.
fn select_path() -> String {
    format!("e/{}", orchestrator_core::RESERVED_SELECT_ID)
}

fn select_graph() -> Graph {
    Graph {
        nodes: vec![expand_select_node("e", vec![])],
    }
}

/// The selector producer: a `Select` planner's paid call whose `EffectRecorded` append
/// fails is never re-bought by a retry. The selector reaches the provider only through
/// the lent `SelectorDispatch`, whose `complete` is the fifth paid producer; a plain
/// journal fault there would be retryable, and the retry — with no memo for the call —
/// would dispatch it again.
#[tokio::test]
async fn a_paid_selector_call_whose_spend_was_not_journaled_is_never_redispatched_by_a_retry() {
    let (gateway, seen) = clamp_observing_gateway(10, 295).await;
    price_single_chain(&gateway, 0.1, 0.2).await;
    let journal = FailSpendOnce::new("e/__select__");
    let registry = two_planner_registry();
    let exec = Executor::new(Arc::new(gateway), journal.clone(), "v1")
        .with_registry(registry.clone())
        .with_planner_selector(Arc::new(crate::LlmPlannerSelector::new(registry, "c")));

    let (_run, row) = wake_into_one_failed_spend_record(exec, journal, select_graph()).await;

    assert_eq!(
        seen.lock().unwrap().len(),
        1,
        "the selector's paid call is dispatched exactly once — a retry would pay for it again"
    );
    assert_eq!(row.status, RunStatus::Failed, "{row:?}");
    let reason = row.reason.unwrap_or_default();
    assert!(
        reason.contains("spend") && reason.contains("not recorded"),
        "the terminal reason names the unrecorded spend: {reason}"
    );
}

/// The selector case's TYPE, on a bare executor: the drive aborts with `SpendUnrecorded`
/// at the reserved select path — not a `Journal(Backend)` the scheduler would retry, and
/// not a soft `NodeFailed` (`LlmPlannerSelector` propagates the dispatch's `Err` with `?`,
/// which the `Select` arm would otherwise turn into `expand_failed`).
#[tokio::test]
async fn a_selectors_unrecorded_spend_aborts_the_drive_as_spend_unrecorded() {
    let (gateway, seen) = clamp_observing_gateway(10, 295).await;
    price_single_chain(&gateway, 0.1, 0.2).await;
    let journal = FailSpendOnce::new("e/__select__");
    journal.arm();
    let registry = two_planner_registry();

    let err = Executor::new(Arc::new(gateway), journal.clone(), "v1")
        .with_registry(registry.clone())
        .with_planner_selector(Arc::new(crate::LlmPlannerSelector::new(registry, "c")))
        .run_with_budget(RunId(uuid::Uuid::new_v4()), &select_graph(), money(CAP))
        .await
        .expect_err("the selector's spend went unrecorded");

    assert!(
        matches!(&err, OrchestratorError::SpendUnrecorded { node, .. } if node.0 == select_path()),
        "expected SpendUnrecorded at {}, got {err:?}",
        select_path()
    );
    assert_eq!(seen.lock().unwrap().len(), 1, "one paid selector call");
}

/// A selector that SWALLOWS the dispatch's `Err` and falls back to the first candidate
/// still cannot turn the unrecorded spend into a normal selection: the error is the
/// executor's, stashed on the dispatch and read before `select()`'s own result, so the
/// drive aborts with the typed `SpendUnrecorded` — no `PlannerSelected`, no soft
/// `NodeFailed`, and no further spend on the strength of the fallback pick.
#[tokio::test]
async fn a_selector_cannot_swallow_an_unrecorded_spend() {
    let (gateway, seen) = clamp_observing_gateway(10, 295).await;
    price_single_chain(&gateway, 0.1, 0.2).await;
    let journal = FailSpendOnce::new("e/__select__");
    journal.arm();
    let run = RunId(uuid::Uuid::new_v4());

    let err = Executor::new(Arc::new(gateway), journal.clone(), "v1")
        .with_registry(two_planner_registry())
        .with_planner_selector(Arc::new(SwallowingSelector))
        .run_with_budget(run, &select_graph(), money(CAP))
        .await
        .expect_err("the unrecorded spend outranks the selector's fallback");

    assert!(
        matches!(&err, OrchestratorError::SpendUnrecorded { node, .. } if node.0 == select_path()),
        "expected SpendUnrecorded at {}, got {err:?}",
        select_path()
    );
    assert_eq!(
        seen.lock().unwrap().len(),
        1,
        "nothing further is dispatched on the strength of the fallback pick"
    );
    let events = journal.load(run).await.unwrap();
    assert!(
        !events.iter().any(|(_, ev)| matches!(
            ev,
            JournalEvent::PlannerSelected { .. } | JournalEvent::NodeFailed { .. }
        )),
        "neither a journaled selection nor a soft failure: {events:?}"
    );
}

// ---- The Consolidate producer (`run_consolidate`'s `ModelCall` body).

/// The Consolidate producer: a synthesis call whose `EffectRecorded` append fails is never
/// re-bought by a retry. The Map child `m/0` journals its spend normally (its record is the
/// memo every later drive replays), so every gateway call past the first two would be a
/// second paid synthesis.
#[tokio::test]
async fn a_paid_consolidate_call_whose_spend_was_not_journaled_is_never_redispatched_by_a_retry() {
    let (gateway, seen) = clamp_observing_gateway(10, 295).await;
    price_single_chain(&gateway, 0.1, 0.2).await;
    let journal = FailSpendOnce::new("cons");
    let graph = Graph {
        nodes: vec![
            Node {
                id: NodeId("m".into()),
                kind: NodeKind::Map {
                    body: MapBody::ModelCall { chain: "c".into() },
                    over: map_items(["i0"]),
                    concurrency: 4,
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
                deps: vec![Dep::hard("m")],
            },
        ],
    };
    let exec = Executor::new(Arc::new(gateway), journal.clone(), "v1");
    let arm = journal.clone();

    let (run, row) = wake_into_armed_faults(
        exec,
        journal.clone(),
        move || arm.arm(),
        graph,
        ROOMY_CAP,
        |_| {},
    )
    .await;

    assert_eq!(
        seen.lock().unwrap().len(),
        2,
        "one Map-item call and exactly ONE synthesis call — a retry would pay for it again"
    );
    assert!(
        journal
            .load(run)
            .await
            .unwrap()
            .iter()
            .any(|(_, ev)| matches!(
                ev,
                JournalEvent::EffectRecorded { node, .. } if node.0 == "m/0"
            )),
        "the Map item's spend was journaled, so it cannot be the re-bought call"
    );
    assert_eq!(row.status, RunStatus::Failed, "{row:?}");
    let reason = row.reason.unwrap_or_default();
    assert!(
        reason.contains("spend") && reason.contains("not recorded"),
        "the terminal reason names the unrecorded spend: {reason}"
    );
}

/// A selector that swallows EVERY dispatch error and calls `complete` twice — the
/// shortlist-then-choose shape — before falling back to the first candidate.
struct SwallowTwiceSelector;
#[async_trait::async_trait]
impl orchestrator_core::PlannerSelector for SwallowTwiceSelector {
    async fn select(
        &self,
        _goal: &serde_json::Value,
        candidates: &[AgentRef],
        dispatch: &dyn orchestrator_core::ModelDispatch,
    ) -> Result<AgentRef, OrchestratorError> {
        let _ = dispatch.complete("sys", "shortlist", Some("c")).await;
        let _ = dispatch.complete("sys", "choose", Some("c")).await;
        Ok(candidates[0].clone())
    }
}

/// The selector's stash is a fold of several executor fatals from ONE `select()`, so it
/// obeys the same precedence as a Map: a later fault — here call 1's determinism
/// violation against a memo an earlier drive left — must not overwrite call 0's
/// unrecorded spend. Last-wins would file the run under the later error (and a later
/// retryable `Store` fault would hand the scheduler a retry that re-buys call 0).
#[tokio::test]
async fn a_selectors_later_fault_does_not_overwrite_its_unrecorded_spend() {
    let (gateway, seen) = clamp_observing_gateway(10, 295).await;
    price_single_chain(&gateway, 0.1, 0.2).await;
    let journal = FailSpendOnce::new("e/__select__");
    let run = RunId(uuid::Uuid::new_v4());
    journal
        .append(
            run,
            JournalEvent::RunStarted {
                version: "v1".into(),
                budget: None,
                money_budget: Some(MoneyBudget {
                    total_micro_usd: CAP,
                }),
            },
        )
        .await
        .unwrap();
    journal
        .append(
            run,
            spent_effect(
                &select_path(),
                orchestrator_core::effect_id(&select_path(), 0, 1),
                "bogus".into(),
                0,
            ),
        )
        .await
        .unwrap();
    journal.arm();

    let err = Executor::new(Arc::new(gateway), journal.clone(), "v1")
        .with_registry(two_planner_registry())
        .with_planner_selector(Arc::new(SwallowTwiceSelector))
        .start(run, &select_graph())
        .await
        .expect_err("call 0's spend went unrecorded");

    assert!(
        matches!(&err, OrchestratorError::SpendUnrecorded { node, .. } if node.0 == select_path()),
        "expected SpendUnrecorded at {}, got {err:?}",
        select_path()
    );
    assert_eq!(seen.lock().unwrap().len(), 1, "only call 0 was paid for");
}
