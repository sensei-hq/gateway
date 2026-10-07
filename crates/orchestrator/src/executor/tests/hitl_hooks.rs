//! AG-2 (sensei-hq/gateway#86): the human-in-the-loop `OrchestratorHooks`.
//!
//! Every test here drives a run the way production does — one drive per process, a
//! FRESH `Executor` each time over the same durable journal, with the decision appended
//! BETWEEN drives exactly as torii's CLI appends it — and asserts on what each drive
//! fired. The property under test is the issue's "done when": each event fires exactly
//! once per real occurrence, and never on a resumed replay.
//!
//! The fixtures that keep a run resumable after its human node has completed carry a
//! second, never-answered `AwaitSignal` (`hold`). Without it the honouring drive
//! finalizes the run, and `start` on a terminal run returns the folded outcome without
//! re-driving — which is precisely the case that CANNOT see a replay re-fire.

use super::human_agent::human_registry;
use super::human_gate::at;
use super::*;
use crate::test_support::FakeClock;
use orchestrator_core::{GateOption, GateOutcome, LoopGateOption};

/// The hook names this slice adds — the filter that separates them from the run/node
/// lifecycle the same spy records.
const HITL: [&str; 9] = [
    "signal_awaited(",
    "signal_received(",
    "gate_awaited(",
    "gate_decided(",
    "agent_awaited(",
    "agent_answered(",
    "loop_gate_awaited(",
    "loop_gate_decided(",
    "loop_gate_settled(",
];

fn is_hitl(entry: &str) -> bool {
    HITL.iter().any(|p| entry.starts_with(p))
}

/// The never-answered sibling that keeps the run live (see the module doc).
fn hold() -> Node {
    Node {
        id: NodeId("hold".into()),
        kind: NodeKind::AwaitSignal { timeout: None },
        deps: vec![],
    }
}

/// A shared journal and a shared spy, with one fresh executor per drive.
struct Harness {
    journal: InMemoryJournal,
    hooks: RecordingHooks,
    run: RunId,
    registry: Option<Arc<Registry>>,
    clock: Arc<FakeClock>,
    seen: usize,
    /// Wire `PatternRedactor::default()` on every drive.
    redacted: bool,
}

impl Harness {
    fn new(registry: Option<Arc<Registry>>) -> Self {
        Harness {
            journal: InMemoryJournal::new(),
            hooks: RecordingHooks::default(),
            run: RunId(uuid::Uuid::new_v4()),
            registry,
            clock: FakeClock::new(at(1_000_000)),
            seen: 0,
            redacted: false,
        }
    }

    /// One drive by a brand-new executor (a process restart), returning ONLY the hook
    /// entries this drive fired — every one, lifecycle included, so ordering against
    /// `node_failed` is visible.
    async fn drive(&mut self, graph: &Graph) -> Vec<String> {
        let (gw, _calls) = recording_gateway().await;
        let mut ex = Executor::new(Arc::new(gw), Arc::new(self.journal.clone()), "v1")
            .with_hooks(Arc::new(self.hooks.clone()))
            .with_clock(self.clock.clone());
        if let Some(r) = &self.registry {
            ex = ex.with_registry(r.clone());
        }
        if self.redacted {
            ex = ex.with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()));
        }
        ex.start(self.run, graph)
            .await
            .expect("the drive yields an outcome");
        let log = self.hooks.log();
        let fresh = log[self.seen..].to_vec();
        self.seen = log.len();
        fresh
    }

    /// The HITL hooks this drive fired, in order.
    async fn drive_hitl(&mut self, graph: &Graph) -> Vec<String> {
        self.drive(graph)
            .await
            .into_iter()
            .filter(|e| is_hitl(e))
            .collect()
    }

    /// The labels of every journaled row, in order.
    async fn rows(&self) -> Vec<String> {
        self.journal
            .load(self.run)
            .await
            .unwrap()
            .iter()
            .map(|(_, e)| label(e))
            .collect()
    }

    /// What torii's CLI does: append a row straight to the journal, between drives.
    async fn append(&self, event: JournalEvent) {
        self.journal
            .append(self.run, event)
            .await
            .expect("an out-of-process append");
    }
}

// ----------------------------------------------------------------- AwaitSignal

fn signal_graph() -> Graph {
    Graph {
        nodes: vec![
            Node {
                id: NodeId("gate".into()),
                kind: NodeKind::AwaitSignal { timeout: None },
                deps: vec![],
            },
            hold(),
        ],
    }
}

/// `on_signal_awaited` fires once per ask (never on a re-pause), and
/// `on_signal_received` fires on the drive that completes the node on its signal —
/// and on no later drive, though every later drive of this still-live run re-completes
/// that node from the fold.
#[tokio::test]
async fn signal_hooks_fire_once_per_occurrence_and_never_on_a_resumed_replay() {
    let mut h = Harness::new(None);
    let graph = signal_graph();

    assert_eq!(
        h.drive_hitl(&graph).await,
        vec!["signal_awaited(gate,None)", "signal_awaited(hold,None)"],
        "the first drive asks both nodes, once each"
    );
    assert_eq!(
        h.drive_hitl(&graph).await,
        Vec::<String>::new(),
        "a resume that re-pauses both nodes asks nobody anything new"
    );

    h.append(JournalEvent::SignalReceived {
        node: NodeId("gate".into()),
        payload: serde_json::json!({ "decision": "approved" }),
    })
    .await;
    assert_eq!(
        h.drive_hitl(&graph).await,
        vec![r#"signal_received(gate,{"decision":"approved"})"#],
        "the drive that honours the signal reports it, once"
    );
    assert_eq!(
        h.drive_hitl(&graph).await,
        Vec::<String>::new(),
        "a resumed drive that REPLAYS the completed node must not report the signal again"
    );
    assert_eq!(h.drive_hitl(&graph).await, Vec::<String>::new());
}

/// The early-signal race: a signal folded before its node ever ran completes the node
/// without an ask, so `on_signal_received` fires with no `on_signal_awaited` — and,
/// again, never on a replay.
#[tokio::test]
async fn an_early_signal_reports_received_without_an_ask_and_never_again() {
    let mut h = Harness::new(None);
    let graph = signal_graph();
    h.append(JournalEvent::RunStarted {
        version: "v1".into(),
        budget: None,
        money_budget: None,
    })
    .await;
    h.append(JournalEvent::SignalReceived {
        node: NodeId("gate".into()),
        payload: serde_json::json!("early"),
    })
    .await;

    assert_eq!(
        h.drive_hitl(&graph).await,
        vec![
            r#"signal_received(gate,"early")"#,
            "signal_awaited(hold,None)"
        ],
    );
    assert_eq!(h.drive_hitl(&graph).await, Vec::<String>::new());
}

// ------------------------------------------------------------------- HumanGate

fn gate_graph() -> Graph {
    Graph {
        nodes: vec![
            Node {
                id: NodeId("release".into()),
                kind: NodeKind::HumanGate {
                    options: vec![
                        GateOption {
                            name: "ship".into(),
                            outcome: GateOutcome::Complete,
                        },
                        GateOption {
                            name: "reject".into(),
                            outcome: GateOutcome::Fail,
                        },
                    ],
                    timeout: None,
                },
                deps: vec![],
            },
            hold(),
        ],
    }
}

fn gate_decided(option: &str, note: Option<&str>) -> JournalEvent {
    JournalEvent::GateDecided {
        node: NodeId("release".into()),
        option: option.into(),
        actor: "alice".into(),
        note: note.map(str::to_string),
    }
}

/// A `Complete` decision: `on_gate_awaited` once, `on_gate_decided` on the honouring
/// drive only.
#[tokio::test]
async fn gate_hooks_fire_once_per_occurrence_and_never_on_a_resumed_replay() {
    let mut h = Harness::new(None);
    let graph = gate_graph();

    assert_eq!(
        h.drive_hitl(&graph).await,
        vec![
            "gate_awaited(release,None,ship|reject)",
            "signal_awaited(hold,None)"
        ],
    );
    assert_eq!(h.drive_hitl(&graph).await, Vec::<String>::new());

    h.append(gate_decided("ship", None)).await;
    assert_eq!(
        h.drive_hitl(&graph).await,
        vec!["gate_decided(release,ship,alice,None)"],
    );
    assert_eq!(
        h.drive_hitl(&graph).await,
        Vec::<String>::new(),
        "a resumed drive that replays the completed gate must not report the decision again"
    );
    assert_eq!(
        h.rows()
            .await
            .iter()
            .filter(|r| *r == "DecisionHookFired(release)")
            .count(),
        1,
        "the bookkeeping row is written once, by the honouring drive — a replay that \
         re-wrote it would grow the journal on every wake"
    );
}

/// A decision corrected before any drive read it (`GateDecided` folds LAST-wins) is
/// reported as the decision actually honoured, and the superseded one never is.
#[tokio::test]
async fn only_the_decision_actually_honoured_is_reported() {
    let mut h = Harness::new(None);
    let graph = gate_graph();
    h.drive(&graph).await;

    h.append(gate_decided("reject", Some("oops"))).await;
    h.append(gate_decided("ship", None)).await;
    assert_eq!(
        h.drive_hitl(&graph).await,
        vec!["gate_decided(release,ship,alice,None)"],
    );
}

/// A decision that lands after the gate's deadline is never honoured — the gate fails on
/// the deadline BEFORE any decision is read — so nothing reports it, on that drive or any
/// later one.
#[tokio::test]
async fn a_decision_the_deadline_beat_is_never_reported() {
    let mut h = Harness::new(None);
    let mut graph = gate_graph();
    if let NodeKind::HumanGate { timeout, .. } = &mut graph.nodes[0].kind {
        *timeout = Some(chrono::Duration::hours(1));
    }
    h.drive(&graph).await;

    h.append(gate_decided("ship", None)).await;
    h.clock.set(at(1_000_000 + 2 * 3600));
    let fired = h.drive(&graph).await;
    assert!(
        fired.contains(&"node_failed(release)".to_string())
            && !fired.iter().any(|e| e.starts_with("gate_decided(")),
        "the expiry is reported, the late decision is not: {fired:?}"
    );
    assert_eq!(h.drive_hitl(&graph).await, Vec::<String>::new());
}

/// A `Fail` decision is honoured too — it is what the human chose — and is reported
/// BEFORE the node failure it causes; the failure is then read back on every later
/// drive, which reports nothing.
#[tokio::test]
async fn a_rejecting_gate_decision_is_reported_once_before_the_node_fails() {
    let mut h = Harness::new(None);
    let graph = gate_graph();
    h.drive(&graph).await;

    h.append(gate_decided("reject", Some("not yet"))).await;
    let fired = h.drive(&graph).await;
    let decided = fired
        .iter()
        .position(|e| e == r#"gate_decided(release,reject,alice,Some("not yet"))"#);
    let failed = fired.iter().position(|e| e == "node_failed(release)");
    assert!(
        decided.is_some() && failed.is_some() && decided < failed,
        "the decision is reported, then the failure it caused: {fired:?}"
    );
    assert_eq!(h.drive_hitl(&graph).await, Vec::<String>::new());
}

// ------------------------------------------------------- human-backed Agent

fn agent_graph() -> Graph {
    Graph {
        nodes: vec![agent_node("review", "reviewer", "the Acme MSA"), hold()],
    }
}

#[tokio::test]
async fn agent_hooks_fire_once_per_occurrence_and_never_on_a_resumed_replay() {
    let mut h = Harness::new(Some(human_registry(None)));
    let graph = agent_graph();

    assert_eq!(
        h.drive_hitl(&graph).await,
        vec!["agent_awaited(review,None)", "signal_awaited(hold,None)"],
    );
    assert_eq!(h.drive_hitl(&graph).await, Vec::<String>::new());

    h.append(JournalEvent::AgentAnswered {
        node: NodeId("review".into()),
        text: "it does not".into(),
        actor: "bob".into(),
    })
    .await;
    assert_eq!(
        h.drive_hitl(&graph).await,
        vec!["agent_answered(review,it does not,bob)"],
    );
    assert_eq!(
        h.drive_hitl(&graph).await,
        Vec::<String>::new(),
        "a resumed drive that replays the answered node must not report the answer again"
    );
}

// ------------------------------------------------------------ human loop gate

fn loop_gate_graph() -> Graph {
    Graph {
        nodes: vec![
            Node {
                id: NodeId("lp".into()),
                kind: NodeKind::Loop {
                    body: LoopBody::ModelCall { chain: "c".into() },
                    input: serde_json::json!({ "prompt": "draft it" }),
                    gate: GateSpec::Human {
                        agent: AgentRef("reviewer".into()),
                        menu: vec![
                            LoopGateOption {
                                name: "revise".into(),
                                stops: false,
                            },
                            LoopGateOption {
                                name: "ship".into(),
                                stops: true,
                            },
                        ],
                    },
                    max_iters: 3,
                },
                deps: vec![],
            },
            hold(),
        ],
    }
}

fn loop_decided(i: usize, option: &str) -> JournalEvent {
    JournalEvent::LoopGateDecided {
        node: NodeId(format!("lp/{i}/__gate__")),
        option: option.into(),
        actor: "carol".into(),
    }
}

/// One ask per ITERATION; decided-then-settled on the drive that settles each gate; and
/// nothing on the drives that replay the settled gates (`run_loop` re-derives every
/// iteration's gate on every drive, so iteration 0's is replayed on each one).
#[tokio::test]
async fn loop_gate_hooks_fire_once_per_occurrence_and_never_on_a_resumed_replay() {
    let mut h = Harness::new(Some(human_registry(None)));
    let graph = loop_gate_graph();

    assert_eq!(
        h.drive_hitl(&graph).await,
        vec![
            "loop_gate_awaited(lp/0/__gate__,None,revise|ship)",
            "signal_awaited(hold,None)"
        ],
    );
    assert_eq!(h.drive_hitl(&graph).await, Vec::<String>::new());

    h.append(loop_decided(0, "revise")).await;
    assert_eq!(
        h.drive_hitl(&graph).await,
        vec![
            "loop_gate_decided(lp/0/__gate__,revise,carol)",
            "loop_gate_settled(lp/0/__gate__,revise)",
            "loop_gate_awaited(lp/1/__gate__,None,revise|ship)",
        ],
        "iteration 0's gate settles on `revise`, so iteration 1 runs and asks"
    );
    assert_eq!(
        h.drive_hitl(&graph).await,
        Vec::<String>::new(),
        "replaying iteration 0's settled gate reports nothing"
    );

    h.append(loop_decided(1, "ship")).await;
    assert_eq!(
        h.drive_hitl(&graph).await,
        vec![
            "loop_gate_decided(lp/1/__gate__,ship,carol)",
            "loop_gate_settled(lp/1/__gate__,ship)",
        ],
    );
    assert_eq!(
        h.drive_hitl(&graph).await,
        Vec::<String>::new(),
        "replaying both settled gates reports nothing"
    );
}

// ------------------------------------------------------ hooks change nothing

/// The whole journal of a gate run driven WITHOUT hooks, across the ask, a resume, the
/// decision and a replay. Pinned literally: the bookkeeping row the hooked path writes
/// to make `on_gate_decided` exactly-once must never appear when no hooks are wired —
/// "an executor with no hooks journals exactly what it did before".
#[tokio::test]
async fn an_unhooked_run_journals_exactly_what_it_did_before() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = gate_graph();
    let drive = || async {
        let (gw, _calls) = recording_gateway().await;
        Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
            .start(run, &graph)
            .await
            .expect("drives");
    };
    drive().await;
    journal
        .append(run, gate_decided("ship", None))
        .await
        .unwrap();
    drive().await;
    drive().await;

    let labels: Vec<String> = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .map(|(_, e)| label(e))
        .collect();
    assert_eq!(
        labels,
        vec![
            "RunStarted",
            "GateAwaited(release)",
            "RunPaused",
            "SignalAwaited(hold)",
            "RunPaused",
            "GateDecided(release)",
            "RunPaused",
            "RunPaused",
        ]
    );
}

// ------------------------------------------------ corrections after honouring

/// AG-2 review: a `HumanGate`, `AwaitSignal` or human `Agent` journals no durable
/// completion, so a decision row appended AFTER a hooked drive already honoured the
/// first one is honoured by the next drive of a still-live run (the fold is LAST-wins).
/// That drive changes what the node did — here a `Fail` option fails a gate that had
/// completed — so it must report the decision it honoured. The node-keyed marker used
/// to suppress it, and observers saw `node_failed(release)` with no decision before it.
#[tokio::test]
async fn a_correction_honoured_after_the_first_decision_is_reported_too() {
    let mut h = Harness::new(None);
    let graph = gate_graph();
    h.drive(&graph).await;

    h.append(gate_decided("ship", None)).await;
    assert_eq!(
        h.drive_hitl(&graph).await,
        vec!["gate_decided(release,ship,alice,None)"],
    );

    h.append(gate_decided("reject", Some("changed mind"))).await;
    let fired = h.drive(&graph).await;
    let decided = fired
        .iter()
        .position(|e| e == r#"gate_decided(release,reject,alice,Some("changed mind"))"#);
    let failed = fired.iter().position(|e| e == "node_failed(release)");
    assert!(
        failed.is_some() && decided.is_some() && decided < failed,
        "the honoured correction is reported before the failure it causes: {fired:?}"
    );
    assert_eq!(h.drive_hitl(&graph).await, Vec::<String>::new());
}

/// The same for a signal: a corrected payload honoured by a later drive changes the
/// node's output, so it is reported — once.
#[tokio::test]
async fn a_corrected_signal_honoured_after_the_first_is_reported_once() {
    let mut h = Harness::new(None);
    let graph = signal_graph();
    h.drive(&graph).await;

    h.append(JournalEvent::SignalReceived {
        node: NodeId("gate".into()),
        payload: serde_json::json!("first"),
    })
    .await;
    assert_eq!(
        h.drive_hitl(&graph).await,
        vec![r#"signal_received(gate,"first")"#]
    );
    h.append(JournalEvent::SignalReceived {
        node: NodeId("gate".into()),
        payload: serde_json::json!("second"),
    })
    .await;
    assert_eq!(
        h.drive_hitl(&graph).await,
        vec![r#"signal_received(gate,"second")"#]
    );
    assert_eq!(h.drive_hitl(&graph).await, Vec::<String>::new());
}

// ---------------------------------------------- identical redeliveries

fn signal(payload: &str) -> JournalEvent {
    JournalEvent::SignalReceived {
        node: NodeId("gate".into()),
        payload: serde_json::json!(payload),
    }
}

fn agent_answered(text: &str) -> JournalEvent {
    JournalEvent::AgentAnswered {
        node: NodeId("review".into()),
        text: text.into(),
        actor: "bob".into(),
    }
}

/// AG-2 re-review: a retrying webhook redelivers the SAME signal after a hooked drive
/// honoured it. The node's output does not change, so nothing new was decided and the
/// decided hook must not fire again. A marker compared by the decision row's `Seq` alone
/// re-fired it, because every redelivered row is a new `Seq`.
#[tokio::test]
async fn an_identical_signal_redelivery_is_not_reported_again() {
    let mut h = Harness::new(None);
    let graph = signal_graph();
    h.drive(&graph).await;

    h.append(signal("first")).await;
    assert_eq!(
        h.drive_hitl(&graph).await,
        vec![r#"signal_received(gate,"first")"#]
    );
    h.append(signal("first")).await;
    assert_eq!(
        h.drive_hitl(&graph).await,
        Vec::<String>::new(),
        "a redelivery of the decision already reported changes nothing and reports nothing"
    );
}

/// The same for a double-submitted gate option.
#[tokio::test]
async fn an_identical_gate_decision_redelivery_is_not_reported_again() {
    let mut h = Harness::new(None);
    let graph = gate_graph();
    h.drive(&graph).await;

    h.append(gate_decided("ship", None)).await;
    assert_eq!(
        h.drive_hitl(&graph).await,
        vec!["gate_decided(release,ship,alice,None)"],
    );
    h.append(gate_decided("ship", None)).await;
    assert_eq!(h.drive_hitl(&graph).await, Vec::<String>::new());
}

/// The same for a resubmitted human-agent answer.
#[tokio::test]
async fn an_identical_agent_answer_redelivery_is_not_reported_again() {
    let mut h = Harness::new(Some(human_registry(None)));
    let graph = agent_graph();
    h.drive(&graph).await;

    h.append(agent_answered("it does not")).await;
    assert_eq!(
        h.drive_hitl(&graph).await,
        vec!["agent_answered(review,it does not,bob)"],
    );
    h.append(agent_answered("it does not")).await;
    assert_eq!(h.drive_hitl(&graph).await, Vec::<String>::new());
}

/// A correction that is reverted before any drive honours it leaves the node honouring
/// exactly the decision already reported, so nothing is reported: what counts is the
/// content honoured, not how many rows were appended to reach it.
#[tokio::test]
async fn a_correction_reverted_before_any_drive_honoured_it_is_not_reported() {
    let mut h = Harness::new(None);
    let graph = signal_graph();
    h.drive(&graph).await;

    h.append(signal("first")).await;
    h.drive(&graph).await;
    h.append(signal("second")).await;
    h.append(signal("first")).await;
    assert_eq!(h.drive_hitl(&graph).await, Vec::<String>::new());
}

/// But a correction that WAS honoured, then reverted, is two real changes to what the
/// node did, and each is reported: the comparison is against the decision LAST reported.
#[tokio::test]
async fn a_correction_honoured_then_reverted_reports_each_change() {
    let mut h = Harness::new(None);
    let graph = signal_graph();
    h.drive(&graph).await;

    for payload in ["first", "second", "first"] {
        h.append(signal(payload)).await;
        assert_eq!(
            h.drive_hitl(&graph).await,
            vec![format!(r#"signal_received(gate,"{payload}")"#)]
        );
    }
    assert_eq!(h.drive_hitl(&graph).await, Vec::<String>::new());
}

// ------------------------------------------------- a failed bookkeeping write

/// An `InMemoryJournal` that rejects every `DecisionHookFired` append and passes every
/// other call through — a transient store error on exactly the hooks' bookkeeping write.
#[derive(Clone)]
struct FailMarker(InMemoryJournal);

#[async_trait::async_trait]
impl ExecutionJournal for FailMarker {
    async fn append(&self, run: RunId, event: JournalEvent) -> Result<Seq, JournalError> {
        if matches!(event, JournalEvent::DecisionHookFired { .. }) {
            return Err(JournalError::Backend("injected marker failure".into()));
        }
        self.0.append(run, event).await
    }
    async fn load(&self, run: RunId) -> Result<Vec<(Seq, JournalEvent)>, JournalError> {
        self.0.load(run).await
    }
}

/// AG-2 review: the marker write is best-effort, but a failed one must not LOSE the
/// hook. When the honouring drive also finishes the run there is no later drive to
/// retry the claim, so a skip on `Err` lost `on_gate_decided` for good. The drive fires
/// the hook anyway (at worst a later drive of a still-live run reports it again), and
/// the failed write never fails the node or the run.
#[tokio::test]
async fn a_failed_marker_write_still_fires_the_decided_hook_and_never_fails_the_run() {
    let journal = InMemoryJournal::new();
    let hooks = RecordingHooks::default();
    let run = RunId(uuid::Uuid::new_v4());
    let mut graph = gate_graph();
    graph.nodes.truncate(1); // `release` alone: the honouring drive finishes the run.

    let drive = |j: Arc<dyn ExecutionJournal>| {
        let hooks = hooks.clone();
        let graph = graph.clone();
        async move {
            let (gw, _calls) = recording_gateway().await;
            Executor::new(Arc::new(gw), j, "v1")
                .with_hooks(Arc::new(hooks))
                .with_clock(FakeClock::new(at(1_000_000)))
                .start(run, &graph)
                .await
        }
    };
    drive(Arc::new(journal.clone())).await.expect("asks");
    journal
        .append(run, gate_decided("ship", None))
        .await
        .unwrap();

    let before = hooks.log().len();
    let outcome = drive(Arc::new(FailMarker(journal.clone())))
        .await
        .expect("a failed bookkeeping write never fails the drive");
    let fired = hooks.log()[before..].to_vec();
    assert!(
        outcome.failed.is_none() && outcome.completed.contains(&NodeId("release".into())),
        "the gate completes on its decision: {outcome:?}"
    );
    assert!(
        !fired.iter().any(|e| e.starts_with("node_failed(")),
        "the failed write fails nothing: {fired:?}"
    );
    assert!(
        fired.contains(&"gate_decided(release,ship,alice,None)".to_string()),
        "the honouring drive reports the decision even though its marker was not \
         written — no later drive ever reaches the claim: {fired:?}"
    );
}

// ------------------------------------------------------------------- redaction

/// A string `PatternRedactor::default()` scrubs.
const SECRET: &str = "sk-abcdefghijklmnopqrstuvwx";

/// AG-2 review: every string a HITL hook hands an observer is redacted — the awaited
/// gate's MENU included. `HumanGate` journals its graph-authored options as-is (a
/// planner-emitted graph carries model-derived text), and `on_gate_awaited` used to hand
/// them straight to the SSE consumer, while `on_gate_decided` delivered the redacted name
/// for the same option.
#[tokio::test]
async fn the_awaited_gate_menu_is_redacted_like_the_decided_option() {
    let mut h = Harness::new(None);
    h.redacted = true;
    let mut graph = gate_graph();
    if let NodeKind::HumanGate { options, .. } = &mut graph.nodes[0].kind {
        options[0].name = format!("ship {SECRET}");
    }
    let fired = h.drive_hitl(&graph).await;
    assert!(
        fired.iter().any(|e| e.starts_with("gate_awaited(release,")),
        "the ask fires: {fired:?}"
    );
    assert!(
        !fired.iter().any(|e| e.contains(SECRET)),
        "no hook entry carries the secret: {fired:?}"
    );
}

/// Asserts `fired` holds an entry starting with `hook` and that no entry carries
/// [`SECRET`].
fn assert_fired_scrubbed(fired: &[String], hook: &str) {
    assert!(
        fired.iter().any(|e| e.starts_with(hook)),
        "{hook} fires: {fired:?}"
    );
    assert!(
        !fired.iter().any(|e| e.contains(SECRET)),
        "no hook entry carries the secret: {fired:?}"
    );
}

/// AG-2 review: the decided hooks' strings go through the redactor — `on_gate_decided`'s
/// actor and note (operator free text), never the raw journaled row.
#[tokio::test]
async fn the_gate_decided_hook_receives_redacted_strings() {
    let mut h = Harness::new(None);
    h.redacted = true;
    let graph = gate_graph();
    h.drive(&graph).await;
    h.append(JournalEvent::GateDecided {
        node: NodeId("release".into()),
        option: "ship".into(),
        actor: format!("alice {SECRET}"),
        note: Some(format!("key {SECRET}")),
    })
    .await;
    assert_fired_scrubbed(&h.drive_hitl(&graph).await, "gate_decided(release,ship,");
}

/// `on_signal_received` gets the redacted node output, not the raw payload.
#[tokio::test]
async fn the_signal_received_hook_receives_the_redacted_payload() {
    let mut h = Harness::new(None);
    h.redacted = true;
    let graph = signal_graph();
    h.drive(&graph).await;
    h.append(JournalEvent::SignalReceived {
        node: NodeId("gate".into()),
        payload: serde_json::json!({ "k": format!("key {SECRET}") }),
    })
    .await;
    assert_fired_scrubbed(&h.drive_hitl(&graph).await, "signal_received(gate,");
}

/// `on_agent_answered` gets the redacted text and actor.
#[tokio::test]
async fn the_agent_answered_hook_receives_redacted_strings() {
    let mut h = Harness::new(Some(human_registry(None)));
    h.redacted = true;
    let graph = agent_graph();
    h.drive(&graph).await;
    h.append(JournalEvent::AgentAnswered {
        node: NodeId("review".into()),
        text: format!("the key is {SECRET}"),
        actor: format!("bob {SECRET}"),
    })
    .await;
    assert_fired_scrubbed(&h.drive_hitl(&graph).await, "agent_answered(review,");
}

/// `on_loop_gate_decided` gets the redacted actor (attribution nothing upstream scrubs).
#[tokio::test]
async fn the_loop_gate_decided_hook_receives_a_redacted_actor() {
    let mut h = Harness::new(Some(human_registry(None)));
    h.redacted = true;
    let graph = loop_gate_graph();
    h.drive(&graph).await;
    h.append(JournalEvent::LoopGateDecided {
        node: NodeId("lp/0/__gate__".into()),
        option: "ship".into(),
        actor: format!("carol {SECRET}"),
    })
    .await;
    assert_fired_scrubbed(
        &h.drive_hitl(&graph).await,
        "loop_gate_decided(lp/0/__gate__,ship,",
    );
}

/// AG-2 review: a decision naming an option the published menu does not contain is
/// never honoured — the gate fails — so no decided hook reports it, on that drive or any
/// later one. Moving the claim above the menu check would show observers a decision the
/// executor rejected.
#[tokio::test]
async fn an_off_menu_decision_is_never_reported() {
    let mut h = Harness::new(None);
    let graph = gate_graph();
    h.drive(&graph).await;

    h.append(gate_decided("bogus", None)).await;
    let fired = h.drive(&graph).await;
    assert!(
        fired.contains(&"node_failed(release)".to_string())
            && !fired.iter().any(|e| e.starts_with("gate_decided(")),
        "the rejection is reported, the off-menu decision is not: {fired:?}"
    );
    assert_eq!(h.drive_hitl(&graph).await, Vec::<String>::new());
}

// ------------------------------------------------------- awaited-hook arguments

/// AG-2 review: the awaited hooks carry the ABSOLUTE deadline the ask recorded and, for
/// the agent and loop-gate asks, the JOURNALED question — the two things an SSE stream or
/// an expiry alert needs. Every other fixture uses `timeout: None`, so a hook passing
/// `None` or an empty prompt stayed green.
#[tokio::test]
async fn awaited_hooks_carry_the_recorded_deadline_and_the_journaled_prompt() {
    let hour = chrono::Duration::hours(1);
    let due = format!("{:?}", Some(at(1_000_000) + hour));

    // Signal + gate, both with a one-hour SLA.
    let mut h = Harness::new(None);
    let mut graph = gate_graph();
    if let NodeKind::HumanGate { timeout, .. } = &mut graph.nodes[0].kind {
        *timeout = Some(hour);
    }
    graph.nodes[1].kind = NodeKind::AwaitSignal {
        timeout: Some(hour),
    };
    assert_eq!(
        h.drive_hitl(&graph).await,
        vec![
            format!("gate_awaited(release,{due},ship|reject)"),
            format!("signal_awaited(hold,{due})"),
        ],
    );

    // A human-backed agent and a human loop gate, the role carrying the SLA.
    for graph in [agent_graph(), loop_gate_graph()] {
        let mut h = Harness::new(Some(human_registry(Some(hour))));
        let fired = h.drive_hitl(&graph).await;
        let asked = fired
            .iter()
            .find(|e| e.starts_with("agent_awaited(") || e.starts_with("loop_gate_awaited("))
            .unwrap_or_else(|| panic!("the human ask fires: {fired:?}"));
        assert!(asked.contains(&due), "{asked} carries the deadline {due}");

        let journaled: Vec<(String, String)> = h
            .journal
            .load(h.run)
            .await
            .unwrap()
            .into_iter()
            .filter_map(|(_, e)| match e {
                JournalEvent::AgentAwaited { node, prompt, .. }
                | JournalEvent::LoopGateAwaited { node, prompt, .. } => Some((node.0, prompt)),
                _ => None,
            })
            .collect();
        assert!(
            !journaled.is_empty() && journaled.iter().all(|(_, p)| !p.is_empty()),
            "the ask journaled a question: {journaled:?}"
        );
        assert_eq!(
            h.hooks.prompts(),
            journaled,
            "the hook hands over exactly the journaled question"
        );
    }
}
