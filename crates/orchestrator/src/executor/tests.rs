use super::*;
use crate::test_support::{
    content_gated_gateway, demo_reference_gateway, failing_after_gateway, final_response,
    recording_gateway, scripted_gateway, tool_call_response,
};
use orchestrator_core::{
    Aggregation, ChildStatus, Dep, Graph, JournalError, MapBody, Node, NodeId, NodeKind,
};
use orchestrator_store::InMemoryJournal;

use crate::agent::tools::{Calc, Tool, ToolRegistry};
use orchestrator_core::{
    AgentDefinition, AgentRef, Clock, EffectClass, OrchestratorError, Registry, ToolSpec,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A test `Clock` pinned to a fixed instant (`at`), shared across executors via a
/// clonable handle so a resume can be driven at a chosen point relative to an
/// Observation's `fetched_at`.
#[derive(Clone)]
struct AdvanceableClock(Arc<std::sync::Mutex<chrono::DateTime<chrono::Utc>>>);
impl AdvanceableClock {
    fn at(unix_secs: i64) -> Self {
        Self(Arc::new(std::sync::Mutex::new(
            chrono::DateTime::from_timestamp(unix_secs, 0).expect("valid timestamp"),
        )))
    }
}
impl Clock for AdvanceableClock {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        *self.0.lock().unwrap()
    }
}

/// An Observation tool that counts its live executions, so a test can tell a
/// memo replay (count unchanged) from a live re-read (count +1). `ttl_secs = 60`.
struct Probe(Arc<AtomicUsize>);
impl Tool for Probe {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "probe".into(),
            description: None,
            input_schema: serde_json::json!({ "type": "object" }),
            effect_class: EffectClass::Observation,
            ttl_secs: Some(60),
            source: Some("probe".into()),
        }
    }
    fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(serde_json::json!({ "probed": true }))
    }
}

fn agent_def(chain: &str) -> AgentDefinition {
    AgentDefinition {
        name: "a".into(),
        area: "research".into(),
        kind: "reasoning".into(),
        chain: chain.into(),
        tools: vec![],
        skills: vec![],
        system_prompt: "SYS".into(),
    }
}

/// A demo registry/executor: one agent "a" on the recording chain "c".
fn agent_registry(chain: &str) -> Arc<Registry> {
    Arc::new(Registry::default().with_agent(agent_def(chain)))
}

fn agent_node(id: &str, agent: &str, input: &str) -> Node {
    Node {
        id: NodeId(id.into()),
        kind: NodeKind::Agent {
            agent: AgentRef(agent.into()),
            input: serde_json::json!(input),
        },
        deps: vec![],
    }
}

fn tool_agent_registry() -> Arc<Registry> {
    // The core `Registry` needs the tool's *schema* (`ToolSpec`, via
    // `Tool::spec()`) to compile it into the prompt (`assemble_prompt`);
    // the *executable* side is the separate `ToolRegistry` (`calc_tools`).
    Arc::new(
        Registry::default()
            .with_agent(AgentDefinition {
                tools: vec!["calc".into()],
                ..agent_def("c")
            })
            .with_tool(Calc.spec()),
    )
}
fn calc_tools() -> Arc<ToolRegistry> {
    Arc::new(ToolRegistry::default().with_tool(Arc::new(Calc)))
}

#[tokio::test]
async fn agent_react_loop_executes_a_pure_tool_and_feeds_the_result_back() {
    let (gateway, calls) = scripted_gateway(vec![
        tool_call_response("t1", "calc", "{\"op\":\"add\",\"a\":2,\"b\":3}"),
        final_response("the answer is 5"),
    ])
    .await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools());

    let n1 = NodeId("n1".into());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "add 2 and 3")],
    };
    let run = RunId(uuid::Uuid::new_v4());
    let outcome = exec.run(run, &graph).await.expect("run");

    assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
    assert_eq!(outcome.outputs[&n1]["text"], "the answer is 5");
    assert_eq!(calls.lock().unwrap().len(), 2, "two model turns");

    let kinds: Vec<String> = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .map(|(_, e)| label(e))
        .collect();
    assert_eq!(
        kinds,
        vec![
            "RunStarted",
            "NodeStarted(n1)",
            "EffectRecorded(n1)",
            "EffectRecorded(n1)", // turn-0 model + calc
            "EffectRecorded(n1)", // turn-1 model (final)
            "NodeCompleted(n1)",
            "RunCompleted",
        ]
    );
}

#[tokio::test]
async fn agent_halts_at_max_steps_when_the_model_never_finalizes() {
    let (gateway, calls) = scripted_gateway(vec![
        tool_call_response("t1", "calc", "{\"op\":\"add\",\"a\":1,\"b\":1}"),
        tool_call_response("t2", "calc", "{\"op\":\"add\",\"a\":1,\"b\":1}"),
    ])
    .await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools())
        .with_max_steps(2);
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "loop")],
    };
    let outcome = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("outcome");
    let (_, msg) = outcome.failed.expect("max_steps halts");
    assert!(msg.contains("max_steps"), "{msg}");
    assert_eq!(
        calls.lock().unwrap().len(),
        2,
        "exactly max_steps model turns"
    );
}

#[tokio::test]
async fn agent_node_single_turn_runs_through_gateway_and_journals() {
    let (gateway, calls) = recording_gateway().await; // returns empty tool_calls → final on turn 0
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(Calc))));

    let n1 = NodeId("n1".into());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "hello")],
    };
    let run = RunId(uuid::Uuid::new_v4());
    let outcome = exec.run(run, &graph).await.expect("run");

    assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
    assert_eq!(outcome.completed, vec![n1.clone()]);
    assert_eq!(outcome.outputs[&n1]["text"], "canned-response");
    assert_eq!(
        calls.lock().unwrap().len(),
        1,
        "one model turn, one gateway call"
    );

    let kinds: Vec<String> = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .map(|(_, e)| label(e))
        .collect();
    assert_eq!(
        kinds,
        vec![
            "RunStarted",
            "NodeStarted(n1)",
            "EffectRecorded(n1)",
            "NodeCompleted(n1)",
            "RunCompleted"
        ]
    );
}

#[tokio::test]
async fn agent_node_halts_over_budget_before_any_gateway_call() {
    let (gateway, calls) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    // max_context of chain "c" is 4096; force a tiny window via max_steps? No —
    // budget uses the chain window. Use a registry whose agent has a huge body.
    let big = AgentDefinition {
        system_prompt: "x".repeat(100_000),
        ..agent_def("c")
    };
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(Arc::new(Registry::default().with_agent(big)))
        .with_tools(Arc::new(ToolRegistry::default()));

    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "hi")],
    };
    let run = RunId(uuid::Uuid::new_v4());
    let outcome = exec.run(run, &graph).await.expect("run yields an outcome");
    match &outcome.failed {
        Some((node, msg)) => {
            assert_eq!(node.0, "n1");
            assert!(msg.contains("over budget"), "{msg}");
        }
        None => panic!("expected an over-budget failure"),
    }
    assert_eq!(
        calls.lock().unwrap().len(),
        0,
        "over-budget halts before spending"
    );
}

/// A terminal resume of a completed Agent node returns the SAME canonical
/// `{model, text}` output as the original `run` — not the raw 3-key final
/// model-turn effect (`{model, text, tool_calls}`). Proves the durable output
/// shape is identical across every completion path, while preserving the
/// no-op-reappend contract (the terminal resume appends nothing).
#[tokio::test]
async fn agent_node_terminal_resume_yields_canonical_output_shape() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let n1 = NodeId("n1".into());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "hello")],
    };

    // Run 1: drive the single-turn agent node to full completion.
    let (gw1, _calls1) = recording_gateway().await;
    let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(Calc))));
    let outcome1 = exec1.run(run, &graph).await.expect("first run completes");
    assert!(outcome1.failed.is_none());
    assert_eq!(outcome1.completed, vec![n1.clone()]);

    let before = journal.load(run).await.unwrap();

    // Terminal resume on a FRESH gateway: returns the folded outcome without
    // re-driving, projected to the canonical agent-node output shape.
    let (gw2, calls2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(Calc))));
    let outcome2 = exec2
        .start(run, &graph)
        .await
        .expect("resume of a completed agent run");

    // Same canonical shape on terminal resume as on the original run.
    assert_eq!(
        outcome2.outputs[&n1], outcome1.outputs[&n1],
        "terminal resume yields the same output shape as the original run"
    );
    // Canonical `{model, text}` — NOT the 3-key raw model-turn effect.
    assert!(
        outcome2.outputs[&n1].get("tool_calls").is_none(),
        "terminal-resume agent output is canonical (no raw tool_calls key): {:?}",
        outcome2.outputs[&n1]
    );
    assert_eq!(
        calls2.lock().unwrap().len(),
        0,
        "a completed run is not re-driven — no gateway call"
    );

    // No-op reappend preserved: the terminal resume appended nothing.
    let after = journal.load(run).await.unwrap();
    assert_eq!(
        after.len(),
        before.len(),
        "terminal resume of a completed agent run appends nothing"
    );
}

/// Headline: a run that dies at turn 1 resumes and completes WITHOUT re-calling
/// the gateway for turn 0 or re-executing turn 0's tool — memoized on resume.
#[tokio::test]
async fn agent_resume_does_not_respend_completed_turns() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "add 2 and 3")],
    };

    // Run 1: turn 0 (calc tool_call) succeeds, then turn 1 is scripted to ERROR
    // (script exhausted → ProviderError). Turn 0's model + calc effects are
    // journaled; the node fails at turn 1; NO RunCompleted.
    let (gw1, calls1) = scripted_gateway(vec![tool_call_response(
        "t1",
        "calc",
        "{\"op\":\"add\",\"a\":2,\"b\":3}",
    )])
    .await;
    let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools());
    let outcome1 = exec1
        .run(run, &graph)
        .await
        .expect("run 1 yields an outcome");
    assert!(outcome1.failed.is_some(), "run 1 fails at turn 1");
    assert_eq!(
        calls1.lock().unwrap().len(),
        2,
        "run 1 called the gateway for turn 0 and the failing turn 1"
    );

    // Run 2: a FRESH scripted gateway that serves ONLY turn 1's final answer,
    // over the SAME journal. Resume memoizes turn 0 (model + calc) → the run-2
    // gateway is called exactly once (turn 1).
    let (gw2, calls2) = scripted_gateway(vec![final_response("the answer is 5")]).await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools());
    let outcome2 = exec2.start(run, &graph).await.expect("resume completes");
    assert!(outcome2.failed.is_none(), "{:?}", outcome2.failed);
    assert_eq!(
        outcome2.outputs[&NodeId("n1".into())]["text"],
        "the answer is 5"
    );

    // The proof: run-2's gateway saw EXACTLY ONE call (turn 1). Turn 0 was
    // replayed from the journal — not re-spent — and calc was not re-executed.
    assert_eq!(
        calls2.lock().unwrap().len(),
        1,
        "resume re-spent nothing for turn 0: {:?}",
        calls2.lock().unwrap()
    );
    let events = journal.load(run).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|(_, e)| matches!(e, JournalEvent::RunCompleted))
            .count(),
        1
    );

    // The non-vacuous proof: turn 0's model effect and its calc tool effect
    // each appear in EXACTLY ONE `EffectRecorded` across BOTH runs — recorded
    // once in run 1, and NOT re-appended in run 2. If the memo lookup were
    // broken (forcing turn 0 to re-run live on resume), these effects would
    // be re-recorded and this count would be 2, even though `calls2 == 1` and
    // the final-text assertion above would still spuriously pass (a lone
    // scripted final response finalizes a wrongly-re-run turn 0 in one call).
    let recorded_count = |eid: &EffectId| {
        events
                .iter()
                .filter(|(_, e)| {
                    matches!(e, JournalEvent::EffectRecorded { effect_id: rec, .. } if rec == eid)
                })
                .count()
    };
    assert_eq!(
        recorded_count(&effect_id("n1", 0, 0)),
        1,
        "turn 0's model call was replayed from the journal on resume (memoized), not re-recorded/re-spent"
    );
    assert_eq!(
        recorded_count(&effect_id("n1", 0, 1)),
        1,
        "turn 0's calc tool was memoized on resume, not re-executed/re-recorded"
    );
}

/// Headline (Observation §7.1): a memoized Observation is REPLAYED while fresh
/// (no re-execution) but RE-READ once its TTL lapses. Two independent partial
/// runs record the same `probe` Observation at `T0` (ttl=60); one resumes within
/// the TTL (replay — probe not re-run), the other past it (re-read — probe runs
/// once more and a second `EffectRecorded` supersedes).
#[tokio::test]
async fn observation_replays_within_ttl_and_rereads_when_stale() {
    const T0: i64 = 1_000_000_000;
    let probe_eid = effect_id("n1", 0, 1);

    // Record `probe` (Observation, fetched_at=T0) in a partial run that dies at
    // turn 1 (script exhausted). Returns the journal, run id, and live-call
    // counter (== 1 after this).
    async fn seed(counter: Arc<AtomicUsize>) -> (InMemoryJournal, RunId) {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let graph = Graph {
            nodes: vec![agent_node("n1", "a", "probe it")],
        };
        let (gw, _c) = scripted_gateway(vec![tool_call_response("t1", "probe", "{}")]).await;
        let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
            .with_registry(agent_registry("c"))
            .with_tools(Arc::new(
                ToolRegistry::default().with_tool(Arc::new(Probe(counter))),
            ))
            .with_clock(Arc::new(AdvanceableClock::at(T0)));
        let o = exec
            .run(run, &graph)
            .await
            .expect("seed run yields an outcome");
        assert!(
            o.failed.is_some(),
            "seed run fails at turn 1 (script exhausted)"
        );
        (journal, run)
    }

    let recorded_count = |events: &[(Seq, JournalEvent)], eid: &EffectId| {
        events
            .iter()
            .filter(
                |(_, e)| matches!(e, JournalEvent::EffectRecorded { effect_id: r, .. } if r == eid),
            )
            .count()
    };
    let resume = |journal: InMemoryJournal, run: RunId, counter: Arc<AtomicUsize>, at: i64| async move {
        let graph = Graph {
            nodes: vec![agent_node("n1", "a", "probe it")],
        };
        let (gw, _c) = scripted_gateway(vec![final_response("done")]).await;
        Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
            .with_registry(agent_registry("c"))
            .with_tools(Arc::new(
                ToolRegistry::default().with_tool(Arc::new(Probe(counter))),
            ))
            .with_clock(Arc::new(AdvanceableClock::at(at)))
            .start(run, &graph)
            .await
            .expect("resume completes");
        journal.load(run).await.unwrap()
    };

    // Fresh (T0 + 30s < TTL): replay — probe is NOT re-run, no second record.
    let fresh = Arc::new(AtomicUsize::new(0));
    let (j, run) = seed(fresh.clone()).await;
    assert_eq!(fresh.load(Ordering::SeqCst), 1, "seed ran probe once");
    let events = resume(j, run, fresh.clone(), T0 + 30).await;
    assert_eq!(
        fresh.load(Ordering::SeqCst),
        1,
        "within TTL the Observation replays from the memo — probe NOT re-executed"
    );
    assert_eq!(
        recorded_count(&events, &probe_eid),
        1,
        "a fresh Observation is not re-recorded on resume"
    );

    // Stale (T0 + 90s > TTL): re-read — probe runs once more, a second record supersedes.
    let stale = Arc::new(AtomicUsize::new(0));
    let (j2, run2) = seed(stale.clone()).await;
    let events2 = resume(j2, run2, stale.clone(), T0 + 90).await;
    assert_eq!(
        stale.load(Ordering::SeqCst),
        2,
        "past TTL the Observation is re-read — probe executed once more"
    );
    assert_eq!(
        recorded_count(&events2, &probe_eid),
        2,
        "a stale re-read appends a second EffectRecorded (supersedes)"
    );
}

/// Editing a skill body changes the turn's system prompt → its input-hash no
/// longer matches the memoized turn → resume halts with DeterminismViolation
/// (never mixes new instructions into a memoized old turn). No gateway call.
#[tokio::test]
async fn agent_resume_halts_when_a_skill_changed_under_a_completed_turn() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());

    // A registry with agent "a" (skill "s") whose skill body is parameterized.
    let registry = |body: &str| {
        Arc::new(
            Registry::default()
                .with_agent(AgentDefinition {
                    skills: vec!["s".into()],
                    ..agent_def("c")
                })
                .with_skill(orchestrator_core::SkillDef {
                    name: "s".into(),
                    description: None,
                    body: body.into(),
                }),
        )
    };

    // Graph [agent n1, model n2]. Run 1 with skill body "V1": n1's single turn
    // succeeds (gateway call 1), then n2 fails (gateway call 2) → n1 is fully
    // journaled+completed, but there is NO RunCompleted (a partial run to resume).
    let graph = Graph {
        nodes: vec![
            agent_node("n1", "a", "hi"),
            Node {
                id: NodeId("n2".into()),
                kind: model_call("c", "b"),
                deps: vec![Dep::hard("n1")],
            },
        ],
    };
    let (gw1, _c1) = failing_after_gateway(1).await;
    let exec1 =
        Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1").with_registry(registry("V1"));
    let out1 = exec1
        .run(run, &graph)
        .await
        .expect("run 1 yields an outcome");
    assert!(
        out1.failed.is_some(),
        "n2 fails, leaving n1's turn journaled without RunCompleted"
    );

    // Run 2: resume with skill body CHANGED to "V2" → n1's turn system prompt
    // (and thus input-hash) differs from the memoized turn → determinism halt.
    let (gw2, calls2) = recording_gateway().await;
    let exec2 =
        Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1").with_registry(registry("V2"));
    let err = exec2
        .start(run, &graph)
        .await
        .expect_err("determinism violation");
    assert!(
        matches!(err, OrchestratorError::DeterminismViolation { .. }),
        "got {err:?}"
    );
    assert_eq!(
        calls2.lock().unwrap().len(),
        0,
        "a determinism violation never touches the gateway"
    );
}

fn model_call(chain: &str, prompt: &str) -> NodeKind {
    NodeKind::ModelCall {
        chain: chain.to_string(),
        payload: serde_json::json!({ "prompt": prompt }),
    }
}

/// A canonical linear 2-node graph `[n1{prompt:p1} → n2{prompt:p2}]` on the
/// recording chain `"c"`, returned with its node ids for assertions.
fn two_node_graph(p1: &str, p2: &str) -> (Graph, NodeId, NodeId) {
    let n1 = NodeId("n1".into());
    let n2 = NodeId("n2".into());
    let graph = Graph {
        nodes: vec![
            Node {
                id: n1.clone(),
                kind: model_call("c", p1),
                deps: vec![],
            },
            Node {
                id: n2.clone(),
                kind: model_call("c", p2),
                deps: vec![Dep::hard(n1.clone())],
            },
        ],
    };
    (graph, n1, n2)
}

/// A journal whose every `append` fails — proves a backend write error is
/// surfaced as `OrchestratorError::Journal`, never swallowed.
struct FailingJournal;

#[async_trait::async_trait]
impl ExecutionJournal for FailingJournal {
    async fn append(&self, _run: RunId, _event: JournalEvent) -> Result<Seq, JournalError> {
        Err(JournalError::Backend("injected backend failure".into()))
    }
    async fn load(&self, _run: RunId) -> Result<Vec<(Seq, JournalEvent)>, JournalError> {
        Ok(Vec::new())
    }
}

/// Compact, order-preserving label for a journal event, so the test asserts
/// the exact event sequence (kind + node) without matching payloads.
fn label(event: &JournalEvent) -> String {
    match event {
        JournalEvent::RunStarted { .. } => "RunStarted".to_string(),
        JournalEvent::NodeStarted { node } => format!("NodeStarted({})", node.0),
        JournalEvent::EffectRecorded { node, .. } => format!("EffectRecorded({})", node.0),
        JournalEvent::EffectIntent { node, .. } => format!("EffectIntent({})", node.0),
        JournalEvent::NodeCompleted { node } => format!("NodeCompleted({})", node.0),
        JournalEvent::NodeFailed { node, .. } => format!("NodeFailed({})", node.0),
        JournalEvent::NodeSkipped { node } => format!("NodeSkipped({})", node.0),
        JournalEvent::MapExpanded { node, child_count } => {
            format!("MapExpanded({}x{})", node.0, child_count)
        }
        JournalEvent::MapCompacted { node, children } => {
            format!("MapCompacted({}x{})", node.0, children.len())
        }
        JournalEvent::RunCompleted => "RunCompleted".to_string(),
        JournalEvent::RunPaused { .. } => "RunPaused".to_string(),
    }
}

/// The DAG scheduler runs a diamond (`a → {b, c} → d`) declared OUT of
/// topological order, scheduling each node only once its dependencies have
/// completed. The old linear drive rejected this graph outright
/// (`validate_linear`); the scheduler runs it and completes in a valid
/// topological order (`a` first, `d` last, `b`/`c` before `d`).
#[tokio::test]
async fn scheduler_runs_a_diamond_dag_in_topological_order() {
    let (gateway, _calls) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");

    // Declared out of order: [d, b, c, a]. d hard-deps b & c; b, c hard-dep a.
    let graph = Graph {
        nodes: vec![
            Node {
                id: NodeId("d".into()),
                kind: model_call("c", "pd"),
                deps: vec![Dep::hard("b"), Dep::hard("c")],
            },
            Node {
                id: NodeId("b".into()),
                kind: model_call("c", "pb"),
                deps: vec![Dep::hard("a")],
            },
            Node {
                id: NodeId("c".into()),
                kind: model_call("c", "pc"),
                deps: vec![Dep::hard("a")],
            },
            Node {
                id: NodeId("a".into()),
                kind: model_call("c", "pa"),
                deps: vec![],
            },
        ],
    };

    let run = RunId(uuid::Uuid::new_v4());
    let outcome = exec.run(run, &graph).await.expect("diamond DAG runs");

    assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
    assert_eq!(outcome.completed.len(), 4, "all four nodes completed");
    let pos = |id: &str| {
        outcome
            .completed
            .iter()
            .position(|n| n.0 == id)
            .unwrap_or_else(|| panic!("{id} completed"))
    };
    assert_eq!(outcome.completed.first().unwrap().0, "a", "root first");
    assert_eq!(outcome.completed.last().unwrap().0, "d", "sink last");
    assert!(pos("a") < pos("b") && pos("a") < pos("c"), "a before b,c");
    assert!(pos("b") < pos("d") && pos("c") < pos("d"), "b,c before d");
}

/// Map items as `{prompt}` payloads for `MapBody::ModelCall`; a prompt
/// containing `"FAIL"` fails under the content-gated gateway.
fn map_items<const N: usize>(prompts: [&str; N]) -> Vec<serde_json::Value> {
    prompts
        .iter()
        .map(|p| serde_json::json!({ "prompt": p }))
        .collect()
}

/// A single-node graph holding one `Map` over `over` with the given aggregation.
fn map_graph(id: &str, over: Vec<serde_json::Value>, aggregation: Aggregation) -> Graph {
    Graph {
        nodes: vec![Node {
            id: NodeId(id.into()),
            kind: NodeKind::Map {
                body: MapBody::ModelCall { chain: "c".into() },
                over,
                concurrency: 4,
                aggregation,
            },
            deps: vec![],
        }],
    }
}

/// Acceptance 2 — a `BestEffort` Map with two failing children completes,
/// carrying a `{ok:3, failed:2}` manifest and results indexed by item order.
#[tokio::test]
async fn map_best_effort_completes_with_a_failure_manifest() {
    let (gateway, _calls) = content_gated_gateway().await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");

    let over = map_items(["a-0", "b-1", "c-2", "FAIL-3", "FAIL-4"]);
    let graph = map_graph("m", over, Aggregation::BestEffort);
    let m = NodeId("m".into());

    let outcome = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("map runs");

    assert!(
        outcome.failed.is_none(),
        "BestEffort never fails the run: {:?}",
        outcome.failed
    );
    assert_eq!(outcome.completed, vec![m.clone()], "the Map node completed");
    let out = &outcome.outputs[&m];
    assert_eq!(out["manifest"]["ok"], 3, "manifest: {out}");
    assert_eq!(out["manifest"]["failed"], 2, "manifest: {out}");

    let results = out["results"].as_array().expect("results array");
    assert_eq!(results.len(), 5, "one result per item");
    // Deterministic index order regardless of concurrent completion order.
    for (i, r) in results.iter().enumerate() {
        assert_eq!(r["index"], i as i64, "result {i} carries its index");
    }
    assert!(results[0].get("ok").is_some(), "child 0 succeeded");
    assert!(results[2].get("ok").is_some(), "child 2 succeeded");
    assert!(results[3].get("error").is_some(), "child 3 failed");
    assert!(results[4].get("error").is_some(), "child 4 failed");
}

/// Acceptance 3 — `Quorum{min_fraction:0.6}` over 5: with 2 failures (3/5 =
/// 0.6) the Map completes; with 3 failures (2/5 = 0.4) it fails loudly, with
/// the manifest still attached to the outcome.
#[tokio::test]
async fn map_quorum_completes_at_threshold_and_fails_below_it() {
    let quorum = || Aggregation::Quorum {
        min_count: None,
        min_fraction: Some(0.6),
    };
    let m = NodeId("m".into());

    // 3 ok / 5 == 0.6 → meets quorum → Completed.
    let (g1, _c1) = content_gated_gateway().await;
    let exec1 = Executor::new(Arc::new(g1), Arc::new(InMemoryJournal::new()), "v1");
    let graph1 = map_graph("m", map_items(["a", "b", "c", "FAIL", "FAIL"]), quorum());
    let out1 = exec1
        .run(RunId(uuid::Uuid::new_v4()), &graph1)
        .await
        .expect("runs");
    assert!(
        out1.failed.is_none(),
        "3/5 == 0.6 meets quorum: {:?}",
        out1.failed
    );
    assert_eq!(out1.outputs[&m]["manifest"]["ok"], 3);

    // 2 ok / 5 == 0.4 → below quorum → Failed, manifest attached.
    let (g2, _c2) = content_gated_gateway().await;
    let exec2 = Executor::new(Arc::new(g2), Arc::new(InMemoryJournal::new()), "v1");
    let graph2 = map_graph("m", map_items(["a", "b", "FAIL", "FAIL", "FAIL"]), quorum());
    let out2 = exec2
        .run(RunId(uuid::Uuid::new_v4()), &graph2)
        .await
        .expect("runs");
    let (fnode, _msg) = out2.failed.as_ref().expect("2/5 < 0.6 fails quorum");
    assert_eq!(fnode.0, "m", "the Map node is the failure");
    assert_eq!(
        out2.outputs[&m]["manifest"]["failed"], 3,
        "the failure manifest is carried into the outcome, never dropped"
    );
}

/// Acceptance 4 — a failed node cascade-skips its hard-dependents
/// (transitively across hard edges), journaling `NodeSkipped` and surfacing
/// them in `RunOutcome.skipped`; a node that only *soft*-depends on the
/// failure still runs.
#[tokio::test]
async fn cascade_skip_hard_dependents_but_run_soft_dependents() {
    let (gateway, _calls) = content_gated_gateway().await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");

    let mc = |p: &str| NodeKind::ModelCall {
        chain: "c".into(),
        payload: serde_json::json!({ "prompt": p }),
    };
    // f fails; h hard-deps f → skip; h2 hard-deps h → cascade-skip;
    // s soft-deps f → still runs.
    let graph = Graph {
        nodes: vec![
            Node {
                id: NodeId("f".into()),
                kind: mc("FAIL"),
                deps: vec![],
            },
            Node {
                id: NodeId("h".into()),
                kind: mc("h-ok"),
                deps: vec![Dep::hard("f")],
            },
            Node {
                id: NodeId("h2".into()),
                kind: mc("h2-ok"),
                deps: vec![Dep::hard("h")],
            },
            Node {
                id: NodeId("s".into()),
                kind: mc("s-ok"),
                deps: vec![Dep::soft("f")],
            },
        ],
    };

    let run = RunId(uuid::Uuid::new_v4());
    let outcome = exec.run(run, &graph).await.expect("run yields an outcome");

    let (fnode, _) = outcome.failed.as_ref().expect("f failed");
    assert_eq!(fnode.0, "f");

    let skipped: Vec<&str> = outcome.skipped.iter().map(|n| n.0.as_str()).collect();
    assert!(
        skipped.contains(&"h"),
        "h hard-depends on failed f → skipped: {skipped:?}"
    );
    assert!(
        skipped.contains(&"h2"),
        "h2 hard-depends on skipped h → cascade-skipped: {skipped:?}"
    );
    assert!(
        outcome.completed.iter().any(|n| n.0 == "s"),
        "s soft-depends on f → still runs"
    );
    assert!(!skipped.contains(&"s"), "s is not skipped");

    // NodeSkipped is journaled for both h and h2 (no silent skip).
    let skips: Vec<String> = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .filter_map(|(_, e)| match e {
            JournalEvent::NodeSkipped { node } => Some(node.0.clone()),
            _ => None,
        })
        .collect();
    assert!(skips.contains(&"h".to_string()) && skips.contains(&"h2".to_string()));

    // A failed run never writes RunCompleted (stays resumable).
    assert!(
        !journal
            .load(run)
            .await
            .unwrap()
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "a run with a failure is not marked complete"
    );
}

/// A `Map`("m", BestEffort over 5 with 2 failing) → `Consolidate`("cons")
/// soft-depending on it, with the given `min_viable`.
fn consolidate_graph(min_viable: usize) -> Graph {
    Graph {
        nodes: vec![
            Node {
                id: NodeId("m".into()),
                kind: NodeKind::Map {
                    body: MapBody::ModelCall { chain: "c".into() },
                    over: map_items(["a", "b", "c", "FAIL", "FAIL"]),
                    concurrency: 4,
                    aggregation: Aggregation::BestEffort,
                },
                deps: vec![],
            },
            Node {
                id: NodeId("cons".into()),
                kind: NodeKind::Consolidate {
                    over: NodeId("m".into()),
                    min_viable,
                    body: MapBody::ModelCall { chain: "c".into() },
                },
                deps: vec![Dep::soft("m")],
            },
        ],
    }
}

/// A 3-node graph — `Map m` (3 items, BestEffort) → `Consolidate cons`
/// (min_viable 1) → `ModelCall n3` ("tail") — the shared fixture for the
/// resume/compaction tests where the Map + Consolidate complete but a
/// hard-dependent tail node fails.
fn map_consolidate_tail_graph() -> Graph {
    Graph {
        nodes: vec![
            Node {
                id: NodeId("m".into()),
                kind: NodeKind::Map {
                    body: MapBody::ModelCall { chain: "c".into() },
                    over: map_items(["i0", "i1", "i2"]),
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
                deps: vec![Dep::soft("m")],
            },
            Node {
                id: NodeId("n3".into()),
                kind: model_call("c", "tail"),
                deps: vec![Dep::hard("cons")],
            },
        ],
    }
}

/// Acceptance 5 — `Consolidate` synthesizes over the Map's survivors when
/// they meet `min_viable`, and halts loudly (`ConsolidateStarved`) when they
/// don't — never a silent empty synthesis.
#[tokio::test]
async fn consolidate_synthesizes_survivors_and_starves_below_min_viable() {
    let cons = NodeId("cons".into());

    // 3 survivors ≥ min_viable 3 → Consolidate runs and produces output.
    let (g1, _c1) = content_gated_gateway().await;
    let exec1 = Executor::new(Arc::new(g1), Arc::new(InMemoryJournal::new()), "v1");
    let out1 = exec1
        .run(RunId(uuid::Uuid::new_v4()), &consolidate_graph(3))
        .await
        .expect("runs");
    assert!(
        out1.failed.is_none(),
        "3 survivors ≥ min_viable 3: {:?}",
        out1.failed
    );
    assert!(
        out1.completed.iter().any(|n| n.0 == "cons"),
        "consolidate completed"
    );
    assert!(
        out1.outputs.contains_key(&cons),
        "consolidate produced a synthesis output"
    );

    // Only 3 survivors < min_viable 4 → ConsolidateStarved (loud halt).
    let (g2, _c2) = content_gated_gateway().await;
    let exec2 = Executor::new(Arc::new(g2), Arc::new(InMemoryJournal::new()), "v1");
    let out2 = exec2
        .run(RunId(uuid::Uuid::new_v4()), &consolidate_graph(4))
        .await
        .expect("runs");
    let (fnode, msg) = out2.failed.as_ref().expect("starved below min_viable");
    assert_eq!(fnode.0, "cons");
    assert!(
        msg.contains("starved") || msg.contains("viable"),
        "loud starvation message: {msg}"
    );
    assert!(
        !out2.completed.iter().any(|n| n.0 == "cons"),
        "a starved consolidate does not complete"
    );
    // The Map's manifest is still carried through, never dropped.
    assert_eq!(out2.outputs[&NodeId("m".into())]["manifest"]["ok"], 3);
}

#[tokio::test]
async fn run_drives_linear_graph_through_gateway_and_journals_in_order() {
    let (gateway, calls) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let executor = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");

    let n1 = NodeId("n1".into());
    let n2 = NodeId("n2".into());
    let graph = Graph {
        nodes: vec![
            Node {
                id: n1.clone(),
                kind: model_call("c", "a"),
                deps: vec![],
            },
            Node {
                id: n2.clone(),
                kind: model_call("c", "b"),
                deps: vec![Dep::hard(n1.clone())],
            },
        ],
    };

    let run = RunId(uuid::Uuid::new_v4());
    let outcome = executor.run(run, &graph).await.expect("run succeeds");

    // Both nodes completed, in order, with no failure.
    assert!(
        outcome.failed.is_none(),
        "no node should fail: {:?}",
        outcome.failed
    );
    assert_eq!(outcome.completed, vec![n1.clone(), n2.clone()]);
    assert!(outcome.outputs.contains_key(&n1));
    assert!(outcome.outputs.contains_key(&n2));

    // Exactly two gateway calls reached the recording adapter, carrying the
    // two nodes' distinct prompts in order.
    let recorded = calls.lock().unwrap().clone();
    assert_eq!(recorded.len(), 2, "one gateway call per node: {recorded:?}");
    assert_eq!(recorded[0].1, "a");
    assert_eq!(recorded[1].1, "b");

    // The journal holds the exact event sequence, in order.
    let events = journal.load(run).await.expect("load");
    let kinds: Vec<String> = events.iter().map(|(_, e)| label(e)).collect();
    assert_eq!(
        kinds,
        vec![
            "RunStarted",
            "NodeStarted(n1)",
            "EffectRecorded(n1)",
            "NodeCompleted(n1)",
            "NodeStarted(n2)",
            "EffectRecorded(n2)",
            "NodeCompleted(n2)",
            "RunCompleted",
        ],
    );
}

/// Headline / load-bearing: a run that dies after n1 resumes to completion
/// WITHOUT re-spending tokens on n1 — the second gateway is called for n2
/// only, because n1 is replayed from the journal.
#[tokio::test]
async fn start_resumes_without_respending_memoized_model_calls() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (graph, n1, n2) = two_node_graph("a", "b");

    // Run 1: adapter succeeds on its 1st call (n1) and errors on its 2nd
    // (n2) — a provider dying mid-run. n1 is journaled+completed; n2 fails;
    // NO RunCompleted is written.
    let (gw1, calls1) = failing_after_gateway(1).await;
    let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1");
    let outcome1 = exec1
        .run(run, &graph)
        .await
        .expect("run 1 yields an outcome");
    assert_eq!(
        outcome1.completed,
        vec![n1.clone()],
        "only n1 completed in run 1"
    );
    match &outcome1.failed {
        Some((node, _)) => assert_eq!(node, &n2, "n2 is the failed node in run 1"),
        None => panic!("run 1 must fail at n2, got {:?}", outcome1.failed),
    }
    assert_eq!(
        calls1.lock().unwrap().len(),
        2,
        "run 1 hit the gateway for n1 and the failing n2"
    );

    // Run 2: a FRESH gateway + adapter that always succeeds, over the SAME
    // journal. `start` folds the journal, memoizes n1, and drives only the
    // tail.
    let (gw2, calls2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1");
    let outcome2 = exec2
        .start(run, &graph)
        .await
        .expect("start resumes the run");
    assert!(
        outcome2.failed.is_none(),
        "resume completes with no failure: {:?}",
        outcome2.failed
    );
    assert_eq!(
        outcome2.completed,
        vec![n1.clone(), n2.clone()],
        "both nodes completed after resume"
    );
    assert!(outcome2.outputs.contains_key(&n1));
    assert!(outcome2.outputs.contains_key(&n2));

    // The proof: run-2's gateway saw EXACTLY ONE call, carrying n2's prompt
    // "b". n1 was replayed from the journal — not re-spent.
    let recorded2 = calls2.lock().unwrap().clone();
    assert_eq!(
        recorded2.len(),
        1,
        "resume re-called the gateway only for the tail node n2: {recorded2:?}"
    );
    assert_eq!(
        recorded2[0].1, "b",
        "the single resume call carried n2's prompt"
    );

    // Exactly one RunCompleted across both runs (run 1 wrote none; the
    // resume wrote one), and the journal ends on it.
    let events = journal.load(run).await.expect("load");
    let completes = events
        .iter()
        .filter(|(_, e)| matches!(e, JournalEvent::RunCompleted))
        .count();
    assert_eq!(completes, 1, "exactly one RunCompleted across both runs");
    assert!(
        matches!(
            events.last().map(|(_, e)| e),
            Some(JournalEvent::RunCompleted)
        ),
        "the journal ends with RunCompleted"
    );
}

/// A resume whose graph changed under a completed node halts with a
/// determinism violation — it never silently re-runs or re-memoizes, and
/// never calls the gateway for the changed node.
#[tokio::test]
async fn start_halts_on_determinism_violation_without_calling_gateway() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let n1 = NodeId("n1".into());

    // Pre-seed a partial journal: n1 recorded for payload {prompt:"a"}, no
    // RunCompleted. Direct appends — independent of the gateway.
    journal
        .append(
            run,
            JournalEvent::RunStarted {
                version: "v1".into(),
            },
        )
        .await
        .unwrap();
    let ih_a = input_hash("c", &serde_json::json!({ "prompt": "a" })).expect("hash");
    journal
        .append(
            run,
            JournalEvent::EffectRecorded {
                node: n1.clone(),
                effect_id: effect_id("", 0, 0),
                class: EffectClass::Pure,
                input_hash: ih_a,
                seq: 0,
                output: EffectOutput::Inline(
                    serde_json::json!({ "model": "m", "text": "canned-response" }),
                ),
                observation: None,
            },
        )
        .await
        .unwrap();
    journal
        .append(run, JournalEvent::NodeCompleted { node: n1.clone() })
        .await
        .unwrap();

    // Resume with n1's payload CHANGED — its input hash no longer matches.
    let (gw, calls) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1");
    let (graph, _, _) = two_node_graph("CHANGED", "b");
    let err = exec
        .start(run, &graph)
        .await
        .expect_err("determinism violation halts the resume");
    match err {
        OrchestratorError::DeterminismViolation { node, .. } => assert_eq!(node, n1),
        other => panic!("expected DeterminismViolation, got {other:?}"),
    }
    assert_eq!(
        calls.lock().unwrap().len(),
        0,
        "a determinism violation never touches the gateway"
    );
}

/// A resume against a journal written by a different version is refused by
/// the version fence — no gateway call, no silent re-run.
#[tokio::test]
async fn start_refuses_resume_on_version_fence_mismatch() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    journal
        .append(
            run,
            JournalEvent::RunStarted {
                version: "v1".into(),
            },
        )
        .await
        .unwrap();

    let (gw, calls) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v2");
    let (graph, _, _) = two_node_graph("a", "b");
    let err = exec
        .start(run, &graph)
        .await
        .expect_err("version fence refuses the resume");
    match err {
        OrchestratorError::VersionFenceMismatch { recorded, current } => {
            assert_eq!(recorded, "v1");
            assert_eq!(current, "v2");
        }
        other => panic!("expected VersionFenceMismatch, got {other:?}"),
    }
    assert_eq!(
        calls.lock().unwrap().len(),
        0,
        "a fenced run never touches the gateway"
    );
}

/// A journal-backend write error is surfaced as `OrchestratorError::Journal`
/// and aborts the run — it is never swallowed and the run does not silently
/// continue to the gateway.
#[tokio::test]
async fn run_surfaces_a_journal_backend_error_instead_of_swallowing_it() {
    let (gw, calls) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gw), Arc::new(FailingJournal), "v1");
    let (graph, _, _) = two_node_graph("a", "b");
    let run = RunId(uuid::Uuid::new_v4());

    let err = exec
        .run(run, &graph)
        .await
        .expect_err("a journal backend error surfaces");
    assert!(
        matches!(err, OrchestratorError::Journal(JournalError::Backend(_))),
        "expected OrchestratorError::Journal(Backend), got {err:?}"
    );
    assert_eq!(
        calls.lock().unwrap().len(),
        0,
        "the run aborts on the first failed append, before any gateway call"
    );
}

/// Resuming an already-completed run is a no-op re-append: it returns the
/// folded outcome, does not re-drive (no gateway call), and appends no
/// second RunCompleted.
#[tokio::test]
async fn start_on_a_completed_run_is_a_noop_reappend() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (graph, n1, n2) = two_node_graph("a", "b");

    // Drive the run to full completion first.
    let (gw1, _calls1) = recording_gateway().await;
    let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1");
    let outcome1 = exec1.run(run, &graph).await.expect("first run completes");
    assert!(outcome1.failed.is_none());
    assert_eq!(outcome1.completed, vec![n1.clone(), n2.clone()]);

    let before = journal.load(run).await.unwrap();
    let completes_before = before
        .iter()
        .filter(|(_, e)| matches!(e, JournalEvent::RunCompleted))
        .count();
    assert_eq!(completes_before, 1, "one RunCompleted after the first run");

    // Resume the already-terminal run on a FRESH gateway: returns the folded
    // outcome without re-driving.
    let (gw2, calls2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1");
    let outcome2 = exec2
        .start(run, &graph)
        .await
        .expect("resume of a completed run");
    assert!(outcome2.failed.is_none());
    assert_eq!(
        outcome2.completed,
        vec![n1.clone(), n2.clone()],
        "folded outcome lists both completed nodes"
    );
    assert!(outcome2.outputs.contains_key(&n1));
    assert!(outcome2.outputs.contains_key(&n2));
    assert_eq!(
        calls2.lock().unwrap().len(),
        0,
        "a completed run is not re-driven — no gateway call"
    );

    let after = journal.load(run).await.unwrap();
    let completes_after = after
        .iter()
        .filter(|(_, e)| matches!(e, JournalEvent::RunCompleted))
        .count();
    assert_eq!(
        completes_after, 1,
        "resume of a completed run appends no second RunCompleted"
    );
    assert_eq!(
        after.len(),
        before.len(),
        "resume of a completed run appends nothing at all"
    );
}

/// Real end-to-end: the durable executor drives the REAL gateway assembled
/// from the illustrative demo catalog (`gateway::catalog::assemble(
/// demo_catalog())`) over a REFERENCE chain (`research.bulk`). The selector
/// walks `groq-llama-free` (no adapter → fall over) → `deepseek-chat` (no
/// adapter → fall over) → `llama3.1-local` (served by the local ollama
/// adapter). The run completes and the orchestrator records the model the
/// chain fell over to — proving the spine drives the real gateway + a real
/// reference chain, not a bespoke test-only single-model chain.
#[tokio::test]
async fn run_drives_real_reference_chain_end_to_end_to_local_fallover() {
    let (gateway, calls) = demo_reference_gateway().await;
    let journal = InMemoryJournal::new();
    let executor = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");

    // A single `ModelCall` node on the reference chain `research.bulk`.
    let n1 = NodeId("n1".into());
    let graph = Graph {
        nodes: vec![Node {
            id: n1.clone(),
            kind: NodeKind::ModelCall {
                chain: "research.bulk".into(),
                payload: serde_json::json!({ "prompt": "hello" }),
            },
            deps: vec![],
        }],
    };

    let run = RunId(uuid::Uuid::new_v4());
    let outcome = executor.run(run, &graph).await.expect("run succeeds");

    // The reference chain ran to completion via genuine fallover.
    assert!(
        outcome.failed.is_none(),
        "the reference chain runs to completion via fallover: {:?}",
        outcome.failed
    );
    assert_eq!(outcome.completed, vec![n1.clone()], "n1 completed");
    // The load-bearing assertion: the orchestrator recorded that the chain
    // fell over the credential-gated cloud entries to the LOCAL model.
    assert_eq!(
        outcome.outputs[&n1]["model"], "llama3.1-local",
        "the reference chain fell over cloud entries to the local model, recorded by the orchestrator: {:?}",
        outcome.outputs[&n1],
    );

    // The chain genuinely reached the local adapter (the terminal candidate
    // was served, not short-circuited earlier).
    assert_eq!(
        calls.lock().unwrap().len(),
        1,
        "the served terminal candidate hit the local ollama adapter exactly once",
    );

    // And the journal is a clean single-node run ending on RunCompleted.
    let events = journal.load(run).await.expect("load");
    let kinds: Vec<String> = events.iter().map(|(_, e)| label(e)).collect();
    assert_eq!(
        kinds,
        vec![
            "RunStarted",
            "NodeStarted(n1)",
            "EffectRecorded(n1)",
            "NodeCompleted(n1)",
            "RunCompleted",
        ],
    );
}

/// Real end-to-end: an `Agent` node whose role resolves to the reference chain
/// `research.bulk` drives the REAL gateway (assembled from `demo_catalog`). The
/// chain falls over the credential-gated cloud entries to the local ollama
/// model; the agent's single (no-tool) turn is served by `llama3.1-local`.
#[tokio::test]
async fn agent_node_drives_real_reference_chain_to_local_fallover() {
    let (gateway, calls) = demo_reference_gateway().await;
    let journal = InMemoryJournal::new();
    let registry = Arc::new(Registry::default().with_agent(AgentDefinition {
        name: "researcher".into(),
        area: "research".into(),
        kind: "reasoning".into(),
        chain: "research.bulk".into(),
        tools: vec![],
        skills: vec![],
        system_prompt: "Research carefully.".into(),
    }));
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(registry)
        .with_tools(Arc::new(ToolRegistry::default()));

    let n1 = NodeId("n1".into());
    let graph = Graph {
        nodes: vec![agent_node("n1", "researcher", "summarize the news")],
    };
    let outcome = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");

    assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
    assert_eq!(
        outcome.outputs[&n1]["model"], "llama3.1-local",
        "fell over to the local model: {:?}",
        outcome.outputs[&n1]
    );
    assert_eq!(
        calls.lock().unwrap().len(),
        1,
        "the served terminal candidate hit the local adapter once"
    );
}

/// Acceptance 7 (split + dedupe) — with a `ContentStore` wired, an effect
/// output whose serialized size exceeds `cas_threshold` is stored in the CAS
/// and the journal carries a `ContentRef` (never the inline value); two
/// identical outputs share one digest (dedupe); a below-threshold output
/// stays inline (the gate cuts both ways). The blob round-trips via the CAS.
#[tokio::test]
async fn cas_threshold_splits_large_outputs_to_deduped_refs_and_keeps_small_ones_inline() {
    use orchestrator_store::InMemoryContentStore;

    // Two ModelCall nodes; the recording gateway returns the SAME canned
    // output for both (~38 bytes), so with a low threshold both split to ONE
    // shared digest, and with the default high threshold both stay inline.
    let (graph, n1, n2) = two_node_graph("a", "b");

    // Low threshold (8 < ~38 bytes) → both outputs split to refs.
    let (gw_lo, _c_lo) = recording_gateway().await;
    let journal_lo = InMemoryJournal::new();
    let content = Arc::new(InMemoryContentStore::new());
    let exec_lo = Executor::new(Arc::new(gw_lo), Arc::new(journal_lo.clone()), "v1")
        .with_content_store(content.clone())
        .with_cas_threshold(8);
    let run_lo = RunId(uuid::Uuid::new_v4());
    let out_lo = exec_lo
        .run(run_lo, &graph)
        .await
        .expect("low-threshold run");
    assert!(out_lo.failed.is_none(), "{:?}", out_lo.failed);

    // Every EffectRecorded carries a Ref; collect their digests.
    let digests: Vec<String> = journal_lo
        .load(run_lo)
        .await
        .unwrap()
        .iter()
        .filter_map(|(_, e)| match e {
            JournalEvent::EffectRecorded {
                output: EffectOutput::Ref(r),
                ..
            } => Some(r.digest.0.clone()),
            JournalEvent::EffectRecorded {
                output: EffectOutput::Inline(v),
                ..
            } => panic!("over-threshold output must split to a Ref, got inline {v}"),
            _ => None,
        })
        .collect();
    assert_eq!(digests.len(), 2, "both nodes recorded a ref");
    assert_eq!(
        digests[0], digests[1],
        "identical outputs dedupe to one digest"
    );

    // The blob is addressable in the CAS and round-trips to the recorded value.
    let bytes = content
        .get(&orchestrator_core::Digest(digests[0].clone()))
        .await
        .expect("blob present in the CAS");
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["text"], "canned-response");
    // The outcome still exposes the full materialized value.
    assert_eq!(out_lo.outputs[&n1]["text"], "canned-response");
    assert_eq!(out_lo.outputs[&n2]["text"], "canned-response");

    // Default (4 KiB) threshold with a store wired → the same small output
    // stays INLINE (behavior-preserving).
    let (gw_hi, _c_hi) = recording_gateway().await;
    let journal_hi = InMemoryJournal::new();
    let exec_hi = Executor::new(Arc::new(gw_hi), Arc::new(journal_hi.clone()), "v1")
        .with_content_store(Arc::new(InMemoryContentStore::new()));
    let run_hi = RunId(uuid::Uuid::new_v4());
    exec_hi
        .run(run_hi, &graph)
        .await
        .expect("high-threshold run");
    for (_, e) in journal_hi.load(run_hi).await.unwrap() {
        if let JournalEvent::EffectRecorded { output, .. } = e {
            assert!(
                matches!(output, EffectOutput::Inline(_)),
                "below-threshold output stays inline: {output:?}"
            );
        }
    }
}

/// Acceptance 7 (lazy fold + resume) — a large memoized output is recorded as
/// a ref; on resume the fold reads that ref WITHOUT loading its blob, and the
/// node re-materializes it from the SHARED CAS exactly once (the memoized
/// replay) — re-spending no tokens. If the fold eagerly loaded blobs, the CAS
/// `get` count on resume would be 2 (fold + replay) instead of 1.
#[tokio::test]
async fn resume_folds_a_ref_lazily_and_rematerializes_it_from_the_cas_without_respending() {
    use orchestrator_core::{ContentStore, Digest};
    use orchestrator_store::InMemoryContentStore;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // A get-counting CAS wrapper — proves the fold does not load blobs.
    struct CountingCas {
        inner: InMemoryContentStore,
        gets: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl ContentStore for CountingCas {
        async fn put(&self, bytes: &[u8]) -> Result<Digest, OrchestratorError> {
            self.inner.put(bytes).await
        }
        async fn get(&self, d: &Digest) -> Result<Vec<u8>, OrchestratorError> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.inner.get(d).await
        }
    }

    let gets = Arc::new(AtomicUsize::new(0));
    let content: Arc<dyn ContentStore> = Arc::new(CountingCas {
        inner: InMemoryContentStore::new(),
        gets: gets.clone(),
    });

    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (graph, n1, n2) = two_node_graph("a", "b");

    // Run 1: n1 succeeds (recorded as a ref via the low threshold), n2 fails
    // → no RunCompleted. The live path never reads back from the CAS.
    let (gw1, _c1) = failing_after_gateway(1).await;
    let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_content_store(content.clone())
        .with_cas_threshold(8);
    let out1 = exec1
        .run(run, &graph)
        .await
        .expect("run 1 yields an outcome");
    assert!(
        out1.failed.is_some(),
        "n2 fails, leaving n1 journaled without RunCompleted"
    );
    assert_eq!(
        gets.load(Ordering::SeqCst),
        0,
        "the live run never reads back from the CAS"
    );

    // n1's effect was recorded as a REF (not inline).
    let n1_is_ref = journal.load(run).await.unwrap().iter().any(|(_, e)| {
        matches!(
            e,
            JournalEvent::EffectRecorded { node, output: EffectOutput::Ref(_), .. }
                if node == &n1
        )
    });
    assert!(n1_is_ref, "n1's over-threshold output was split to a ref");

    // Run 2: resume on a FRESH gateway over the SAME journal + SAME CAS. The
    // fold reads n1's ref without loading it; the replay materializes it once.
    let gets_before_run2 = gets.load(Ordering::SeqCst);
    let (gw2, calls2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_content_store(content.clone())
        .with_cas_threshold(8);
    let out2 = exec2.start(run, &graph).await.expect("resume completes");
    assert!(out2.failed.is_none(), "{:?}", out2.failed);
    assert_eq!(out2.completed, vec![n1.clone(), n2.clone()]);
    assert_eq!(
        out2.outputs[&n1]["text"], "canned-response",
        "n1 re-materialized from the CAS"
    );

    // The proof of lazy fold: resume loaded n1's blob EXACTLY ONCE (the
    // memoized replay), not twice (which an eager fold would cause).
    assert_eq!(
        gets.load(Ordering::SeqCst) - gets_before_run2,
        1,
        "resume loaded n1's blob once (lazy replay), never during the fold"
    );
    // And n1 was not re-spent: the run-2 gateway was called only for n2.
    assert_eq!(
        calls2.lock().unwrap().len(),
        1,
        "resume re-called the gateway only for the tail n2"
    );
}

/// Increment 8a — the executor writes a round-boundary snapshot after each
/// scheduling round (out-of-band, so the journal event order is unchanged):
/// the latest snapshot captures every completed node and its output.
#[tokio::test]
async fn drive_writes_a_round_boundary_snapshot_capturing_completed_nodes() {
    let (gateway, _calls) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let (graph, n1, n2) = two_node_graph("a", "b"); // linear → two rounds
    let run = RunId(uuid::Uuid::new_v4());
    let outcome = exec.run(run, &graph).await.expect("run");
    assert!(outcome.failed.is_none(), "{:?}", outcome.failed);

    // The latest snapshot reflects BOTH completed nodes and carries each
    // node's output.
    let snap = journal
        .latest_snapshot(run)
        .await
        .unwrap()
        .expect("a snapshot was written");
    assert!(
        snap.completed.contains(&n1) && snap.completed.contains(&n2),
        "snapshot lists completed nodes: {:?}",
        snap.completed
    );
    let keyed: Vec<&NodeId> = snap.outputs.iter().map(|(k, _)| k).collect();
    assert!(
        keyed.contains(&&n1) && keyed.contains(&&n2),
        "snapshot carries per-node outputs: {keyed:?}"
    );
    assert!(snap.seq > 0, "snapshot records a journal boundary seq");

    // The journal event order is byte-identical (snapshots are out-of-band).
    let kinds: Vec<String> = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .map(|(_, e)| label(e))
        .collect();
    assert_eq!(
        kinds,
        vec![
            "RunStarted",
            "NodeStarted(n1)",
            "EffectRecorded(n1)",
            "NodeCompleted(n1)",
            "NodeStarted(n2)",
            "EffectRecorded(n2)",
            "NodeCompleted(n2)",
            "RunCompleted",
        ],
    );
}

/// Acceptance 8 (headline) — a run that dies after a `Map` completed but
/// before its dependent finished resumes and **re-spends nothing** for the
/// Map's children: the completed Map is replayed from the journal memo (no
/// gateway calls, its aggregated output reconstructed) and is NOT re-journaled
/// (no duplicate `NodeStarted`/`MapExpanded`/`NodeCompleted`), so each child's
/// effect stays exactly-once. Only the unfinished tail node runs live.
#[tokio::test]
async fn resume_replays_a_completed_map_with_no_respend_and_no_reappend() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let m = NodeId("m".into());
    let n2 = NodeId("n2".into());
    let graph = Graph {
        nodes: vec![
            Node {
                id: m.clone(),
                kind: NodeKind::Map {
                    body: MapBody::ModelCall { chain: "c".into() },
                    over: map_items(["i0", "i1", "i2"]),
                    concurrency: 4,
                    aggregation: Aggregation::BestEffort,
                },
                deps: vec![],
            },
            Node {
                id: n2.clone(),
                kind: model_call("c", "tail"),
                deps: vec![Dep::hard("m")],
            },
        ],
    };

    // Run 1: the 3 Map children succeed (gateway calls 1–3), then n2 fails
    // (call 4) → no RunCompleted. The Map is fully journaled + completed.
    let (gw1, calls1) = failing_after_gateway(3).await;
    let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1");
    let out1 = exec1
        .run(run, &graph)
        .await
        .expect("run 1 yields an outcome");
    assert!(out1.failed.is_some(), "n2 fails in run 1");
    assert_eq!(
        calls1.lock().unwrap().len(),
        4,
        "run 1: 3 Map children + the failing n2"
    );
    let before = journal.load(run).await.unwrap().len();

    // Run 2: resume on a FRESH gateway. n2 succeeds; the Map replays.
    let (gw2, calls2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1");
    let out2 = exec2.start(run, &graph).await.expect("resume completes");
    assert!(out2.failed.is_none(), "{:?}", out2.failed);
    assert!(
        out2.completed.contains(&m) && out2.completed.contains(&n2),
        "both nodes completed after resume: {:?}",
        out2.completed
    );
    assert_eq!(
        out2.outputs[&m]["manifest"]["ok"], 3,
        "the Map's aggregated output is reconstructed on resume"
    );

    // Re-spend nothing for the children: run-2 gateway called ONLY for n2.
    let recorded2 = calls2.lock().unwrap().clone();
    assert_eq!(
        recorded2.len(),
        1,
        "resume re-called the gateway only for the tail n2: {recorded2:?}"
    );
    assert_eq!(recorded2[0].1, "tail");

    // The completed Map is NOT re-journaled on resume.
    let all = journal.load(run).await.unwrap();
    let run2_labels: Vec<String> = all[before..].iter().map(|(_, e)| label(e)).collect();
    assert!(
        !run2_labels.iter().any(|l| l == "NodeStarted(m)"
            || l == "NodeCompleted(m)"
            || l.starts_with("MapExpanded(m")),
        "the completed Map is not re-journaled on resume: {run2_labels:?}"
    );
    // Each child's effect appears in exactly ONE EffectRecorded across BOTH runs.
    for i in 0..3 {
        let eid = effect_id(&format!("m/{i}"), 0, 0);
        let count = all
                .iter()
                .filter(|(_, e)| {
                    matches!(e, JournalEvent::EffectRecorded { effect_id, .. } if effect_id == &eid)
                })
                .count();
        assert_eq!(count, 1, "child {i}'s effect recorded exactly once");
    }
}

/// Acceptance 8 (Consolidate) — a completed `Consolidate` replays on resume
/// WITHOUT re-spending its synthesis body: its body effect is memoized (no
/// gateway call) and it is not re-journaled. Only the unfinished tail runs.
#[tokio::test]
async fn resume_replays_a_completed_consolidate_without_respending_its_body() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let cons = NodeId("cons".into());
    let n3 = NodeId("n3".into());
    // Map m (3 ok) → Consolidate cons (soft m) → ModelCall n3 (hard cons).
    let graph = map_consolidate_tail_graph();

    // Run 1: 3 children (calls 1–3) + cons body (call 4) succeed, n3 fails
    // (call 5) → no RunCompleted.
    let (gw1, calls1) = failing_after_gateway(4).await;
    let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1");
    let out1 = exec1
        .run(run, &graph)
        .await
        .expect("run 1 yields an outcome");
    assert!(out1.failed.is_some(), "n3 fails in run 1");
    assert_eq!(
        calls1.lock().unwrap().len(),
        5,
        "run 1: 3 children + cons body + failing n3"
    );
    let before = journal.load(run).await.unwrap().len();

    // Run 2: resume on a fresh gateway → only n3 runs live.
    let (gw2, calls2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1");
    let out2 = exec2.start(run, &graph).await.expect("resume completes");
    assert!(out2.failed.is_none(), "{:?}", out2.failed);
    assert!(out2.completed.contains(&cons) && out2.completed.contains(&n3));

    // Re-spend nothing: the run-2 gateway is called only for the tail n3 —
    // NOT for the Map's children and NOT for the Consolidate's body.
    let recorded2 = calls2.lock().unwrap().clone();
    assert_eq!(
        recorded2.len(),
        1,
        "resume re-called the gateway only for n3: {recorded2:?}"
    );

    // The Consolidate is not re-journaled, and its body effect stays exactly-once.
    let all = journal.load(run).await.unwrap();
    let run2_labels: Vec<String> = all[before..].iter().map(|(_, e)| label(e)).collect();
    assert!(
        !run2_labels
            .iter()
            .any(|l| l == "NodeStarted(cons)" || l == "NodeCompleted(cons)"),
        "the completed Consolidate is not re-journaled on resume: {run2_labels:?}"
    );
    let cons_eid = effect_id("cons", 0, 0);
    let body_count = all
            .iter()
            .filter(
                |(_, e)| matches!(e, JournalEvent::EffectRecorded { effect_id, .. } if effect_id == &cons_eid),
            )
            .count();
    assert_eq!(
        body_count, 1,
        "the Consolidate body effect recorded exactly once"
    );
}

/// Acceptance 9 (compaction) — once a `Map`'s `Consolidate` completes, the
/// Map's per-child `EffectRecorded` records collapse to a `MapCompacted`
/// manifest of `{index, status, digest}`; the child content stays fetchable
/// from the CAS by digest (never dropped).
#[tokio::test]
async fn compaction_collapses_a_consolidated_maps_child_records_to_digests() {
    use orchestrator_store::InMemoryContentStore;
    let (gateway, _calls) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let content = Arc::new(InMemoryContentStore::new());
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_content_store(content.clone())
        .with_cas_threshold(8); // child outputs (~38 bytes) split into the CAS
    let m = NodeId("m".into());
    let graph = Graph {
        nodes: vec![
            Node {
                id: m.clone(),
                kind: NodeKind::Map {
                    body: MapBody::ModelCall { chain: "c".into() },
                    over: map_items(["i0", "i1", "i2"]),
                    concurrency: 4,
                    aggregation: Aggregation::BestEffort,
                },
                deps: vec![],
            },
            Node {
                id: NodeId("cons".into()),
                kind: NodeKind::Consolidate {
                    over: m.clone(),
                    min_viable: 1,
                    body: MapBody::ModelCall { chain: "c".into() },
                },
                deps: vec![Dep::soft("m")],
            },
        ],
    };
    let run = RunId(uuid::Uuid::new_v4());
    let out = exec.run(run, &graph).await.expect("run");
    assert!(out.failed.is_none(), "{:?}", out.failed);

    let events = journal.load(run).await.unwrap();
    // The Map's per-child EffectRecorded are gone (collapsed).
    let child_records = events
            .iter()
            .filter(|(_, e)| {
                matches!(e, JournalEvent::EffectRecorded { node, .. } if node.0.starts_with("m/"))
            })
            .count();
    assert_eq!(
        child_records, 0,
        "the Map's child records are compacted away"
    );

    // A MapCompacted manifest carries {index,status,digest} for all 3 children.
    let manifest = events
        .iter()
        .find_map(|(_, e)| match e {
            JournalEvent::MapCompacted { node, children } if node == &m => Some(children.clone()),
            _ => None,
        })
        .expect("MapCompacted manifest present");
    assert_eq!(manifest.len(), 3, "one record per child");
    for c in &manifest {
        assert_eq!(c.status, ChildStatus::Ok);
        let digest = c.digest.clone().expect("an ok child carries a digest");
        // The child content is still fetchable from the CAS by digest.
        let bytes = content
            .get(&digest)
            .await
            .expect("child content fetchable from the CAS");
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["text"], "canned-response");
    }
}

/// Acceptance 9 (resume after compaction) — a Map whose child records were
/// compacted still replays on resume with ZERO re-spend: the fold rebuilds the
/// children's memo (as content refs) from the `MapCompacted` manifest, so a
/// replay materializes them from the shared CAS instead of re-dispatching.
#[tokio::test]
async fn resume_after_compaction_replays_the_map_from_the_cas_without_respending() {
    use orchestrator_store::InMemoryContentStore;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    // The CAS is SHARED across runs — the compacted refs point into it.
    let content = Arc::new(InMemoryContentStore::new());
    let cons = NodeId("cons".into());
    let n3 = NodeId("n3".into());
    let graph = map_consolidate_tail_graph();

    // Run 1: 3 children (1–3) + cons body (4) succeed; cons completes and
    // compacts the Map; n3 fails (5) → no RunCompleted.
    let (gw1, calls1) = failing_after_gateway(4).await;
    let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_content_store(content.clone())
        .with_cas_threshold(8);
    let out1 = exec1
        .run(run, &graph)
        .await
        .expect("run 1 yields an outcome");
    assert!(out1.failed.is_some(), "n3 fails in run 1");
    assert_eq!(calls1.lock().unwrap().len(), 5);
    // The Map was compacted in run 1.
    assert!(
        journal
            .load(run)
            .await
            .unwrap()
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::MapCompacted { .. })),
        "the Map was compacted after the Consolidate completed"
    );

    // Run 2: resume on a FRESH gateway + the SHARED CAS.
    let (gw2, calls2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_content_store(content.clone())
        .with_cas_threshold(8);
    let out2 = exec2.start(run, &graph).await.expect("resume completes");
    assert!(out2.failed.is_none(), "{:?}", out2.failed);
    assert!(out2.completed.contains(&cons) && out2.completed.contains(&n3));
    assert_eq!(
        out2.outputs[&NodeId("m".into())]["manifest"]["ok"],
        3,
        "the compacted Map's output is reconstructed from the CAS"
    );

    // Re-spend nothing: the Map's children (compacted) and the Consolidate
    // body replay from the CAS/memo; only the tail n3 hits the gateway.
    let recorded2 = calls2.lock().unwrap().clone();
    assert_eq!(
        recorded2.len(),
        1,
        "resume re-called the gateway only for the tail n3: {recorded2:?}"
    );
}

/// Acceptance 10 (real e2e) — a `Map { body: Agent("researcher"), over: [3] }`
/// → `Consolidate { body: Agent("synthesizer") }` drives the REAL gateway
/// assembled from the demo catalog over the reference chain `research.bulk`.
/// Each child agent AND the synthesis agent fall over the credential-gated
/// cloud entries to `llama3.1-local`. Proves fan-out of real agents through a
/// real reference chain, consolidated, end-to-end.
#[tokio::test]
async fn map_of_agents_then_consolidate_drives_the_real_reference_chain_to_local_fallover() {
    let (gateway, calls) = demo_reference_gateway().await;
    let journal = InMemoryJournal::new();
    let mk_agent = |name: &str| AgentDefinition {
        name: name.into(),
        area: "research".into(),
        kind: "reasoning".into(),
        chain: "research.bulk".into(),
        tools: vec![],
        skills: vec![],
        system_prompt: "Work carefully.".into(),
    };
    let registry = Arc::new(
        Registry::default()
            .with_agent(mk_agent("researcher"))
            .with_agent(mk_agent("synthesizer")),
    );
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(registry)
        .with_tools(Arc::new(ToolRegistry::default()));

    let m = NodeId("m".into());
    let cons = NodeId("cons".into());
    let graph = Graph {
        nodes: vec![
            Node {
                id: m.clone(),
                kind: NodeKind::Map {
                    body: MapBody::Agent(AgentRef("researcher".into())),
                    over: vec![
                        serde_json::json!("topic-0"),
                        serde_json::json!("topic-1"),
                        serde_json::json!("topic-2"),
                    ],
                    concurrency: 4,
                    aggregation: Aggregation::BestEffort,
                },
                deps: vec![],
            },
            Node {
                id: cons.clone(),
                kind: NodeKind::Consolidate {
                    over: m.clone(),
                    min_viable: 1,
                    body: MapBody::Agent(AgentRef("synthesizer".into())),
                },
                deps: vec![Dep::soft("m")],
            },
        ],
    };

    let run = RunId(uuid::Uuid::new_v4());
    let outcome = exec.run(run, &graph).await.expect("e2e run");

    assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
    assert!(
        outcome.completed.contains(&m) && outcome.completed.contains(&cons),
        "the Map and the Consolidate both completed: {:?}",
        outcome.completed
    );

    // All 3 child agents succeeded, each served by the local fallover model.
    assert_eq!(outcome.outputs[&m]["manifest"]["ok"], 3);
    let results = outcome.outputs[&m]["results"].as_array().expect("results");
    assert_eq!(results.len(), 3);
    for (i, r) in results.iter().enumerate() {
        assert_eq!(
            r["ok"]["model"], "llama3.1-local",
            "child {i} fell over to the local model: {r}"
        );
    }
    // The synthesis agent also ran on the reference chain and fell over local.
    assert_eq!(
        outcome.outputs[&cons]["model"], "llama3.1-local",
        "the Consolidate's agent synthesized via the local fallover: {:?}",
        outcome.outputs[&cons]
    );

    // The chain genuinely reached the local adapter once per agent turn:
    // 3 child agents (one no-tool turn each) + 1 synthesis agent = 4 calls.
    assert_eq!(
        calls.lock().unwrap().len(),
        4,
        "3 fanned-out agents + 1 synthesis agent each hit the local adapter once"
    );

    // The children ran as AGENT sub-runs (a ReAct node lifecycle at the child
    // path), not bare ModelCalls — each journals its own NodeStarted/Completed.
    let labels: Vec<String> = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .map(|(_, e)| label(e))
        .collect();
    for i in 0..3 {
        assert!(
            labels.contains(&format!("NodeStarted(m/{i})"))
                && labels.contains(&format!("NodeCompleted(m/{i})")),
            "child {i} ran as an agent sub-run (has its own node lifecycle): {labels:?}"
        );
    }
}
