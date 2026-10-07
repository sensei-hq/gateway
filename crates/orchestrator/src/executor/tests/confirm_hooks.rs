//! AG-2 × AG-15 (sensei-hq/gateway#86, #90): the human-in-the-loop hooks for AG-15's
//! confirm-before-run tool calls (`ToolConfirmAwaited`/`ToolConfirmDecided`) and escalated
//! questions (`AgentEscalated`), under AG-2's contract — once per real occurrence, never
//! on a resumed replay, redacted, and nothing extra journaled when no hooks are wired.
//!
//! Every test drives a run the way production does: one FRESH `Executor` per drive over
//! one durable journal, the decision appended BETWEEN drives as torii's CLI appends it. A
//! never-answered `AwaitSignal` sibling (`hold`) keeps the run live after the node under
//! test completes, so a later drive CAN replay it — the only case a re-fire shows up in.

use super::agent_tool_policy::{confirm_agent, registry_of};
use super::human_agent::reviewer;
use super::human_gate::at;
use super::*;
use crate::test_support::FakeClock;
use chrono::Duration;
use orchestrator_core::{Activation, ToolSpec};
use std::sync::atomic::AtomicBool;

/// The hooks under test — the filter that separates them from everything else the shared
/// spy records.
const HOOKS: [&str; 3] = [
    "tool_confirm_awaited(",
    "tool_confirm_decided(",
    "agent_escalated(",
];

/// A string `PatternRedactor::default()` scrubs.
const SECRET: &str = "sk-abcdefghijklmnopqrstuvwx";

fn n1() -> NodeId {
    NodeId("n1".into())
}

/// The never-answered sibling that keeps the run live (see the module doc).
fn hold() -> Node {
    Node {
        id: NodeId("hold".into()),
        kind: NodeKind::AwaitSignal { timeout: None },
        deps: vec![],
    }
}

fn agent_graph(node: &str, agent: &str) -> Graph {
    Graph {
        nodes: vec![agent_node(node, agent, "do it"), hold()],
    }
}

fn write_args(path: &str, content: &str) -> String {
    serde_json::json!({ "path": path, "content": content }).to_string()
}

/// One model turn asking for several tool calls at once.
fn calls_response(calls: &[(&str, &str, &str)]) -> kernel::types::io::ChatResponse {
    kernel::types::io::ChatResponse {
        content: Some(String::new()),
        tool_calls: calls
            .iter()
            .map(|(id, name, arguments)| kernel::types::request::ToolCall {
                id: (*id).into(),
                name: (*name).into(),
                arguments: (*arguments).into(),
            })
            .collect(),
        usage: None,
        model: Some("m".into()),
        degraded: false,
    }
}

fn decided(eid: &EffectId, approved: bool, actor: &str, note: Option<&str>) -> JournalEvent {
    JournalEvent::ToolConfirmDecided {
        node: n1(),
        effect_id: eid.clone(),
        approved,
        actor: actor.into(),
        note: note.map(str::to_string),
    }
}

/// A PURE confirm tool that fails while `failing` is set — the one shape in which a call
/// whose decision was honoured is judged AGAIN: its failure journals `NodeFailed` and no
/// `EffectRecorded`, so the node re-attempts on resume with the call un-memoized. (A
/// Mutation would have journaled its `EffectIntent` and go to the in-doubt reconcile
/// instead, never back to the confirmation.)
struct Probe {
    failing: Arc<AtomicBool>,
    runs: Arc<AtomicUsize>,
}

impl Tool for Probe {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "probe".into(),
            description: Some("Probe something".into()),
            input_schema: serde_json::json!({ "type": "object" }),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: Permissions::default(),
            activation: Activation::default(),
            credentials: vec![],
        }
    }
    fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(OrchestratorError::Tool {
                tool: "probe".into(),
                message: "transient".into(),
            });
        }
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(serde_json::json!({ "probed": true }))
    }
}

/// Agent "p" lists `probe` (empty permissions ⇒ an empty grant covers it) and must
/// confirm it.
fn probe_agent() -> AgentDefinition {
    AgentDefinition {
        name: "p".into(),
        tools: vec!["probe".into()],
        confirm_tools: vec!["probe".into()],
        ..agent_def("c")
    }
}

/// A human-backed role named `name` with an SLA of `hours`, escalating to `to`.
fn role(name: &str, hours: i64, to: Option<&str>) -> AgentDefinition {
    AgentDefinition {
        name: name.into(),
        escalate_to: to.map(str::to_string),
        ..reviewer(Some(Duration::hours(hours)), vec![])
    }
}

/// A shared journal, spy and clock, with one fresh executor per drive.
struct Harness {
    journal: InMemoryJournal,
    hooks: RecordingHooks,
    run: RunId,
    registry: Arc<Registry>,
    tools: ToolRegistry,
    clock: Arc<FakeClock>,
    seen: usize,
    /// Wire hooks on every drive (`false` = the unhooked baseline).
    hooked: bool,
    /// Wire `PatternRedactor::default()` on every drive.
    redacted: bool,
}

impl Harness {
    fn new(registry: Arc<Registry>, tools: ToolRegistry) -> Self {
        Harness {
            journal: InMemoryJournal::new(),
            hooks: RecordingHooks::default(),
            run: RunId(uuid::Uuid::new_v4()),
            registry,
            tools,
            clock: FakeClock::new(at(1_000_000)),
            seen: 0,
            hooked: true,
            redacted: false,
        }
    }

    /// The `fs.write` confirm agent "a" (`confirm_timeout` = `timeout`) over a
    /// `ScopedWriter`.
    fn writer(timeout: Option<Duration>) -> Self {
        let sink = Arc::new(std::sync::Mutex::new(Vec::new()));
        Self::new(
            registry_of(confirm_agent(timeout)),
            ToolRegistry::default().with_tool(Arc::new(ScopedWriter::new(sink))),
        )
    }

    /// The `probe` confirm agent "p", with the tool's failure switch and run counter.
    fn probe() -> (Self, Arc<AtomicBool>, Arc<AtomicUsize>) {
        let failing = Arc::new(AtomicBool::new(false));
        let runs = Arc::new(AtomicUsize::new(0));
        let tool = Probe {
            failing: failing.clone(),
            runs: runs.clone(),
        };
        let registry = Arc::new(
            Registry::default()
                .with_agent(probe_agent())
                .with_tool(tool.spec()),
        );
        let h = Self::new(registry, ToolRegistry::default().with_tool(Arc::new(tool)));
        (h, failing, runs)
    }

    /// One drive by a brand-new executor (a process restart) whose model answers with
    /// `script`, returning ONLY the hooks under test that this drive fired.
    async fn drive(
        &mut self,
        graph: &Graph,
        script: Vec<kernel::types::io::ChatResponse>,
    ) -> Vec<String> {
        let (gw, _calls) = scripted_gateway(script).await;
        let mut ex = Executor::new(Arc::new(gw), Arc::new(self.journal.clone()), "v1")
            .with_registry(self.registry.clone())
            .with_tools(Arc::new(self.tools.clone()))
            .with_clock(self.clock.clone());
        if self.hooked {
            ex = ex.with_hooks(Arc::new(self.hooks.clone()));
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
            .into_iter()
            .filter(|e| HOOKS.iter().any(|p| e.starts_with(p)))
            .collect()
    }

    /// What torii's CLI does: append a row straight to the journal, between drives.
    async fn append(&self, event: JournalEvent) {
        self.journal
            .append(self.run, event)
            .await
            .expect("an out-of-process append");
    }

    async fn events(&self) -> Vec<JournalEvent> {
        self.journal
            .load(self.run)
            .await
            .unwrap()
            .into_iter()
            .map(|(_, e)| e)
            .collect()
    }
}

// ------------------------------------------------------------- confirm-before-run

/// The headline: the ask fires once (never on a re-pause), the decision fires on the drive
/// that runs the approved call and on no later drive — though every later drive of this
/// still-live run replays the node.
#[tokio::test]
async fn tool_confirm_hooks_fire_once_per_occurrence_and_never_on_a_resumed_replay() {
    let mut h = Harness::writer(Some(Duration::hours(1)));
    let graph = agent_graph("n1", "a");
    let teid = effect_id("n1", 0, 1);
    let args = write_args("/workspace/a.txt", "x");
    let due = Some(at(1_000_000) + Duration::hours(1));

    assert_eq!(
        h.drive(&graph, vec![tool_call_response("t1", "fs.write", &args)])
            .await,
        vec![format!(
            "tool_confirm_awaited(n1,{},fs.write,{args},{due:?})",
            teid.0
        )],
        "the first drive asks about the call, once, with its arguments and deadline"
    );
    assert_eq!(
        h.drive(&graph, vec![]).await,
        Vec::<String>::new(),
        "a resume that re-pauses on the call asks nothing new"
    );

    h.append(decided(&teid, true, "alice", None)).await;
    assert_eq!(
        h.drive(&graph, vec![final_response("done")]).await,
        vec![format!(
            "tool_confirm_decided(n1,{},true,alice,None)",
            teid.0
        )],
        "the drive that runs the approved call reports the decision, once"
    );
    for _ in 0..2 {
        assert_eq!(
            h.drive(&graph, vec![]).await,
            Vec::<String>::new(),
            "a resumed drive that REPLAYS the node must not report the decision again"
        );
    }
}

/// A rejection is reported by the drive that refuses the call to the model — and a
/// decision the deadline beat is never honoured, so it reports nothing at all.
#[tokio::test]
async fn a_rejection_is_reported_and_a_decision_the_deadline_beat_is_not() {
    let graph = agent_graph("n1", "a");
    let teid = effect_id("n1", 0, 1);
    let args = write_args("/workspace/a.txt", "x");

    let mut h = Harness::writer(None);
    h.drive(&graph, vec![tool_call_response("t1", "fs.write", &args)])
        .await;
    h.append(decided(&teid, false, "bob", Some("use staging")))
        .await;
    assert_eq!(
        h.drive(&graph, vec![final_response("ok")]).await,
        vec![format!(
            "tool_confirm_decided(n1,{},false,bob,Some(\"use staging\"))",
            teid.0
        )]
    );
    assert_eq!(h.drive(&graph, vec![]).await, Vec::<String>::new());

    let mut h = Harness::writer(Some(Duration::hours(1)));
    h.drive(&graph, vec![tool_call_response("t1", "fs.write", &args)])
        .await;
    h.clock.set(at(1_000_000 + 7_200));
    h.append(decided(&teid, true, "alice", None)).await;
    assert_eq!(
        h.drive(&graph, vec![final_response("ok")]).await,
        Vec::<String>::new(),
        "an approval after the deadline runs nothing, so it reports nothing"
    );
}

/// One agent node can ask about several calls: each call's ask and decision fire, keyed by
/// the CALL's effect id. The two approvals here are identical in content (same verdict,
/// same actor, no note) — a dedupe keyed by node would take the second for a redelivery of
/// the first and swallow it.
#[tokio::test]
async fn several_confirms_on_one_node_each_fire() {
    let mut h = Harness::writer(None);
    let graph = agent_graph("n1", "a");
    let (e1, e2) = (effect_id("n1", 0, 1), effect_id("n1", 0, 2));
    let (a1, a2) = (
        write_args("/workspace/a.txt", "x"),
        write_args("/workspace/b.txt", "y"),
    );

    assert_eq!(
        h.drive(
            &graph,
            vec![calls_response(&[
                ("t1", "fs.write", &a1),
                ("t2", "fs.write", &a2),
            ])]
        )
        .await,
        vec![format!(
            "tool_confirm_awaited(n1,{},fs.write,{a1},None)",
            e1.0
        )],
        "the first call asks; the second is not reached until the first is answered"
    );

    h.append(decided(&e1, true, "alice", None)).await;
    assert_eq!(
        h.drive(&graph, vec![]).await,
        vec![
            format!("tool_confirm_decided(n1,{},true,alice,None)", e1.0),
            format!("tool_confirm_awaited(n1,{},fs.write,{a2},None)", e2.0),
        ],
    );

    h.append(decided(&e2, true, "alice", None)).await;
    assert_eq!(
        h.drive(&graph, vec![final_response("done")]).await,
        vec![format!("tool_confirm_decided(n1,{},true,alice,None)", e2.0)],
        "the second call's decision is its own occurrence, though it says what the first said"
    );
    assert_eq!(h.drive(&graph, vec![]).await, Vec::<String>::new());
}

/// The one way a call whose decision was honoured is judged again: the approved tool
/// FAILED, so nothing memoized the call and the node re-attempts it on resume, reading the
/// same decision. That re-attempt reports nothing — nor does an identical REDELIVERY of
/// the decision (a retrying webhook), which decides nothing new.
#[tokio::test]
async fn a_re_attempted_call_and_an_identical_redelivery_report_nothing() {
    let (mut h, failing, runs) = Harness::probe();
    let graph = agent_graph("n1", "p");
    let teid = effect_id("n1", 0, 1);

    h.drive(&graph, vec![tool_call_response("t1", "probe", "{}")])
        .await;
    h.append(decided(&teid, true, "alice", None)).await;

    failing.store(true, Ordering::SeqCst);
    assert_eq!(
        h.drive(&graph, vec![]).await,
        vec![format!(
            "tool_confirm_decided(n1,{},true,alice,None)",
            teid.0
        )],
        "the honouring drive reports the decision, then the tool fails"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 0);

    h.append(decided(&teid, true, "alice", None)).await;
    failing.store(false, Ordering::SeqCst);
    assert_eq!(
        h.drive(&graph, vec![final_response("done")]).await,
        Vec::<String>::new(),
        "the re-attempt of the same call, on an identical redelivery, reports nothing"
    );
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the re-attempt DID run the call — it was re-judged, and the report suppressed"
    );
    assert_eq!(h.drive(&graph, vec![]).await, Vec::<String>::new());
}

/// But a CORRECTION appended after the honoured decision, and honoured by the re-attempt,
/// is a different decision behind what the call did — it is reported, as AG-2 reports a
/// gate correction.
#[tokio::test]
async fn a_correction_honoured_by_a_re_attempt_is_reported() {
    let (mut h, failing, runs) = Harness::probe();
    let graph = agent_graph("n1", "p");
    let teid = effect_id("n1", 0, 1);

    h.drive(&graph, vec![tool_call_response("t1", "probe", "{}")])
        .await;
    h.append(decided(&teid, true, "alice", None)).await;
    failing.store(true, Ordering::SeqCst);
    h.drive(&graph, vec![]).await;

    h.append(decided(&teid, false, "bob", None)).await;
    failing.store(false, Ordering::SeqCst);
    assert_eq!(
        h.drive(&graph, vec![final_response("done")]).await,
        vec![format!(
            "tool_confirm_decided(n1,{},false,bob,None)",
            teid.0
        )],
    );
    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "the corrected call never ran"
    );
}

/// Every string a confirm hook hands an observer is redacted: the arguments the human
/// approves (redacted before the ask is journaled), and the decision's actor and note
/// (operator free text, redacted at dispatch).
#[tokio::test]
async fn tool_confirm_hooks_receive_redacted_strings() {
    let mut h = Harness::writer(None);
    h.redacted = true;
    let graph = agent_graph("n1", "a");
    let teid = effect_id("n1", 0, 1);

    let asked = h
        .drive(
            &graph,
            vec![tool_call_response(
                "t1",
                "fs.write",
                &write_args("/workspace/a.txt", &format!("key {SECRET}")),
            )],
        )
        .await;
    assert!(
        asked.len() == 1
            && asked[0].starts_with("tool_confirm_awaited(n1,")
            && !asked[0].contains(SECRET),
        "{asked:?}"
    );

    h.append(decided(
        &teid,
        true,
        &format!("alice {SECRET}"),
        Some(&format!("note {SECRET}")),
    ))
    .await;
    let fired = h.drive(&graph, vec![final_response("done")]).await;
    assert!(
        fired.len() == 1
            && fired[0].starts_with("tool_confirm_decided(n1,")
            && !fired[0].contains(SECRET),
        "{fired:?}"
    );
}

// -------------------------------------------------------------------- escalation

/// An escalation fires once, when it is journaled; a resume inside the target's SLA
/// neither escalates again nor re-fires, and the answer is reported by AG-2's own
/// `on_agent_answered`, not by another escalation.
#[tokio::test]
async fn an_escalation_fires_once_and_never_on_a_resumed_replay() {
    let registry = Arc::new(
        Registry::default()
            .with_agent(role("reviewer", 1, Some("lead")))
            .with_agent(role("lead", 2, None)),
    );
    let mut h = Harness::new(registry, ToolRegistry::default());
    let graph = agent_graph("review", "reviewer");

    assert_eq!(h.drive(&graph, vec![]).await, Vec::<String>::new());
    h.clock.set(at(1_000_000 + 3_600));
    let lead_due = Some(at(1_000_000 + 3_600 + 7_200));
    assert_eq!(
        h.drive(&graph, vec![]).await,
        vec![format!(
            "agent_escalated(review,reviewer,lead,{lead_due:?})"
        )],
    );
    h.clock.set(at(1_000_000 + 3_600 + 60));
    assert_eq!(h.drive(&graph, vec![]).await, Vec::<String>::new());

    h.append(JournalEvent::AgentAnswered {
        node: NodeId("review".into()),
        text: "yes".into(),
        actor: "carol".into(),
    })
    .await;
    assert_eq!(h.drive(&graph, vec![]).await, Vec::<String>::new());
    assert!(
        h.hooks
            .log()
            .contains(&"agent_answered(review,yes,carol)".to_string()),
        "the escalated question's answer is reported as an answer: {:?}",
        h.hooks.log()
    );
}

// --------------------------------------------------------------- the unhooked path

/// The hooked path's bookkeeping never reaches an unhooked journal: the same confirm run —
/// ask, approve, a failed tool, an identical redelivery, the re-attempt — journals exactly
/// the same rows with and without hooks, but for the hooked run's `DecisionHookFired`.
#[tokio::test]
async fn an_unhooked_confirm_run_journals_exactly_what_it_did_before() {
    let mut journals = Vec::new();
    for hooked in [false, true] {
        let (mut h, failing, _runs) = Harness::probe();
        h.hooked = hooked;
        let graph = agent_graph("n1", "p");
        let teid = effect_id("n1", 0, 1);
        h.drive(&graph, vec![tool_call_response("t1", "probe", "{}")])
            .await;
        h.append(decided(&teid, true, "alice", None)).await;
        failing.store(true, Ordering::SeqCst);
        h.drive(&graph, vec![]).await;
        h.append(decided(&teid, true, "alice", None)).await;
        failing.store(false, Ordering::SeqCst);
        h.drive(&graph, vec![final_response("done")]).await;
        journals.push(h.events().await);
    }
    let unhooked = &journals[0];
    assert!(
        !unhooked
            .iter()
            .any(|e| matches!(e, JournalEvent::DecisionHookFired { .. })),
        "an unhooked run journals no hooks bookkeeping: {unhooked:?}"
    );
    let enc = |events: &[JournalEvent]| -> Vec<String> {
        events
            .iter()
            .filter(|e| !matches!(e, JournalEvent::DecisionHookFired { .. }))
            .map(|e| serde_json::to_string(e).unwrap())
            .collect()
    };
    assert_eq!(enc(unhooked), enc(&journals[1]));
    assert_eq!(
        journals[1]
            .iter()
            .filter(|e| matches!(e, JournalEvent::DecisionHookFired { .. }))
            .count(),
        1,
        "the hooked run marks the one decision it reported: {:?}",
        journals[1]
    );
}
