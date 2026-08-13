use super::*;
use crate::test_support::{
    content_gated_gateway, demo_reference_gateway, demo_reference_tool_gateway,
    echo_system_gateway, failing_after_gateway, final_response, recording_gateway,
    scripted_gateway, tool_call_response,
};
use orchestrator_core::{
    Aggregation, ChildStatus, Dep, EdgeKind, Graph, JournalError, LoopGate, MapBody, Node, NodeId,
    NodeKind,
};
use orchestrator_store::InMemoryJournal;

use crate::agent::tools::{
    AlwaysIndeterminate, Calc, NoteReconciler, ReconcileRegistry, RecordNote, Search, Tool,
    ToolRegistry,
};
use orchestrator_core::{
    AgentDefinition, AgentRef, Clock, EffectClass, OrchestratorError, OrchestratorHooks, Registry,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A test `Clock` backed by a shared, mutable instant so a resume can be driven
/// at a chosen point relative to an Observation's `fetched_at` (§7.1). Cloning
/// shares the same instant; `advance` moves time forward for every clone's reads.
#[derive(Clone)]
struct AdvanceableClock(Arc<std::sync::Mutex<chrono::DateTime<chrono::Utc>>>);
impl AdvanceableClock {
    fn at(unix_secs: i64) -> Self {
        Self(Arc::new(std::sync::Mutex::new(
            chrono::DateTime::from_timestamp(unix_secs, 0).expect("valid timestamp"),
        )))
    }
    /// Move the shared clock forward by `secs` — the next `now()` (on any clone)
    /// reflects it. Used to cross an Observation's TTL between a run and its resume.
    fn advance(&self, secs: i64) {
        *self.0.lock().unwrap() += chrono::Duration::seconds(secs);
    }
}
impl Clock for AdvanceableClock {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        *self.0.lock().unwrap()
    }
}

/// Count the `EffectRecorded` events for one effect id — distinguishes a memo
/// replay (unchanged) from a live re-read/re-record (incremented).
fn effect_recorded_count(events: &[(Seq, JournalEvent)], eid: &EffectId) -> usize {
    events
        .iter()
        .filter(|(_, e)| matches!(e, JournalEvent::EffectRecorded { effect_id: r, .. } if r == eid))
        .count()
}

fn agent_def(chain: &str) -> AgentDefinition {
    AgentDefinition {
        name: "a".into(),
        area: "research".into(),
        kind: "reasoning".into(),
        chain: Some(chain.into()),
        chains: std::collections::HashMap::new(),
        grants: std::collections::HashMap::new(),
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
            phase: None,
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
async fn agent_node_phase_selects_the_per_phase_chain() {
    // Agent has NO explicit chain and NO (area,kind) binding — only chains["plan"] = "c".
    // So it is routable ONLY when the node requests phase "plan".
    let (gateway, _calls) = recording_gateway().await; // knows chain "c"
    let mut agent = agent_def("c");
    agent.chain = None;
    agent.chains.insert("plan".into(), "c".into());
    let registry = Arc::new(Registry::default().with_agent(agent));
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(registry);

    let node = Node {
        id: NodeId("n1".into()),
        kind: NodeKind::Agent {
            agent: AgentRef("a".into()),
            input: serde_json::json!("hi"),
            phase: Some("plan".into()),
        },
        deps: vec![],
    };
    let outcome = exec
        .run(RunId(uuid::Uuid::new_v4()), &Graph { nodes: vec![node] })
        .await
        .expect("run");
    assert!(
        outcome.failed.is_none(),
        "phase route completes: {:?}",
        outcome.failed
    );
    // The node produced an output under its id — the per-phase chain actually drove
    // a turn (not merely "didn't fail").
    assert!(outcome.outputs.contains_key(&NodeId("n1".into())));
}

#[tokio::test]
async fn agent_routes_via_area_kind_binding_end_to_end() {
    use orchestrator_core::{ChainBinding, RegistryConfig};
    // Agent omits chain; the (research,reasoning) binding maps it to "c".
    let agent = AgentDefinition {
        name: "a".into(),
        area: "research".into(),
        kind: "reasoning".into(),
        chain: None,
        chains: std::collections::HashMap::new(),
        grants: std::collections::HashMap::new(),
        tools: vec![],
        skills: vec![],
        system_prompt: "SYS".into(),
    };
    let cfg = RegistryConfig {
        agents: vec![agent],
        skills: vec![],
        tools: vec![],
        chain_bindings: vec![ChainBinding {
            area: "research".into(),
            kind: "reasoning".into(),
            chain: "c".into(),
        }],
    };
    let registry = Arc::new(Registry::from_config(cfg).expect("assembles + validates"));

    let (gateway, _calls) = recording_gateway().await; // only knows chain "c"
    let n1 = NodeId("n1".into());
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(registry);
    let outcome = exec
        .run(
            RunId(uuid::Uuid::new_v4()),
            &Graph {
                nodes: vec![agent_node("n1", "a", "hi")],
            },
        )
        .await
        .expect("run");

    assert!(
        outcome.failed.is_none(),
        "table-routed agent completes: {:?}",
        outcome.failed
    );
    assert!(outcome.outputs.contains_key(&n1));
}

#[tokio::test]
async fn phase_override_wins_over_base_route_through_from_config() {
    use orchestrator_core::RegistryConfig;
    // Base route is an explicit chain the gateway does NOT know ("bogus-base"), so it
    // satisfies validate() but would fail at execution; the phase override "plan"→"c"
    // is the only chain the harness knows. Completion proves the override won.
    let mut chains = std::collections::HashMap::new();
    chains.insert("plan".to_string(), "c".to_string());
    let agent = AgentDefinition {
        name: "a".into(),
        area: "research".into(),
        kind: "reasoning".into(),
        chain: Some("bogus-base".into()),
        chains,
        grants: std::collections::HashMap::new(),
        tools: vec![],
        skills: vec![],
        system_prompt: "SYS".into(),
    };
    let cfg = RegistryConfig {
        agents: vec![agent],
        skills: vec![],
        tools: vec![],
        chain_bindings: vec![],
    };
    let registry = Arc::new(Registry::from_config(cfg).expect("assembles + validates"));
    let (gateway, _calls) = recording_gateway().await; // only knows chain "c"
    let n1 = NodeId("n1".into());
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(registry);
    let node = Node {
        id: n1.clone(),
        kind: NodeKind::Agent {
            agent: AgentRef("a".into()),
            input: serde_json::json!("hi"),
            phase: Some("plan".into()),
        },
        deps: vec![],
    };
    let outcome = exec
        .run(RunId(uuid::Uuid::new_v4()), &Graph { nodes: vec![node] })
        .await
        .expect("run");
    assert!(
        outcome.failed.is_none(),
        "phase override wins over base: {:?}",
        outcome.failed
    );
    assert!(outcome.outputs.contains_key(&n1));
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

const OBS_T0: i64 = 1_000_000_000;

/// Seed a partial agent run that reads the `search` Observation (`ttl=60`,
/// `fetched_at` = the clock's instant) then dies at turn 1 (script exhausted) —
/// no `RunCompleted`. The counter reflects the one live read (`== 1`).
async fn seed_observation(
    counter: Arc<AtomicUsize>,
    clock: AdvanceableClock,
) -> (InMemoryJournal, RunId) {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "search it")],
    };
    let (gw, _c) = scripted_gateway(vec![tool_call_response(
        "t1",
        "search",
        "{\"query\":\"rust\"}",
    )])
    .await;
    let o = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(
            ToolRegistry::default().with_tool(Arc::new(Search::new(counter))),
        ))
        .with_clock(Arc::new(clock))
        .run(run, &graph)
        .await
        .expect("seed run yields an outcome");
    assert!(o.failed.is_some(), "seed dies at turn 1 (script exhausted)");
    (journal, run)
}

/// Resume the seeded Observation run at the clock's current instant; return the
/// journal events after it completes.
async fn resume_observation(
    journal: InMemoryJournal,
    run: RunId,
    counter: Arc<AtomicUsize>,
    clock: AdvanceableClock,
) -> Vec<(Seq, JournalEvent)> {
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "search it")],
    };
    let (gw, _c) = scripted_gateway(vec![final_response("done")]).await;
    Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(
            ToolRegistry::default().with_tool(Arc::new(Search::new(counter))),
        ))
        .with_clock(Arc::new(clock))
        .start(run, &graph)
        .await
        .expect("resume completes");
    journal.load(run).await.unwrap()
}

/// Acceptance §8.1 — within TTL, a resume REPLAYS the memoized Observation: the
/// live `search` is not re-executed and no second `EffectRecorded` is appended.
#[tokio::test]
async fn observation_within_ttl_replays_without_reexecuting() {
    let search_eid = effect_id("n1", 0, 1);
    let counter = Arc::new(AtomicUsize::new(0));
    let clock = AdvanceableClock::at(OBS_T0);
    let (j, run) = seed_observation(counter.clone(), clock.clone()).await;
    assert_eq!(counter.load(Ordering::SeqCst), 1, "seed read search once");

    clock.advance(30); // < ttl (60)
    let events = resume_observation(j, run, counter.clone(), clock).await;

    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "within TTL the Observation replays from the memo — search NOT re-executed"
    );
    assert_eq!(
        effect_recorded_count(&events, &search_eid),
        1,
        "a fresh Observation is not re-recorded on resume"
    );
}

/// Acceptance §8.2 — past TTL, a resume RE-READS the Observation: the live
/// `search` runs once more, and a second `EffectRecorded` (fresh provenance)
/// supersedes the stale one.
#[tokio::test]
async fn observation_past_ttl_rereads_and_supersedes() {
    let search_eid = effect_id("n1", 0, 1);
    let counter = Arc::new(AtomicUsize::new(0));
    let clock = AdvanceableClock::at(OBS_T0);
    let (j, run) = seed_observation(counter.clone(), clock.clone()).await;

    clock.advance(90); // > ttl (60)
    let events = resume_observation(j, run, counter.clone(), clock).await;

    assert_eq!(
        counter.load(Ordering::SeqCst),
        2,
        "past TTL the Observation is re-read — search executed once more"
    );
    assert_eq!(
        effect_recorded_count(&events, &search_eid),
        2,
        "a stale re-read appends a second, superseding EffectRecorded"
    );
    // Every record for the effect carries fresh Observation provenance (§7.1).
    let sources: Vec<String> = events
        .iter()
        .filter_map(|(_, e)| match e {
            JournalEvent::EffectRecorded {
                effect_id: r,
                observation: Some(m),
                ..
            } if r == &search_eid => Some(m.source.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        sources,
        vec!["search".to_string(), "search".to_string()],
        "both the original read and the re-read journal Observation provenance"
    );
}

/// Acceptance §8.3 (happy path, live): a live Mutation is two-phase — it journals
/// an `EffectIntent` (before the side effect) then an `EffectRecorded` (after), in
/// that order, and applies the side effect exactly once.
#[tokio::test]
async fn mutation_two_phase_journals_intent_then_recorded() {
    let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "note it")],
    };
    let (gw, _c) = scripted_gateway(vec![
        tool_call_response("t1", "record_note", "{\"note\":\"hello\"}"),
        final_response("done"),
    ])
    .await;
    let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(
            ToolRegistry::default().with_tool(Arc::new(RecordNote::new(sink.clone()))),
        ));
    let o = exec.run(run, &graph).await.expect("run completes");
    assert!(o.failed.is_none(), "{:?}", o.failed);

    // The side effect ran exactly once.
    assert_eq!(
        &*sink.lock().unwrap(),
        &["hello".to_string()],
        "the note sink saw the mutation exactly once"
    );

    // The tool effect's Intent immediately precedes its Recorded.
    let labels: Vec<String> = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .map(|(_, e)| label(e))
        .collect();
    assert_eq!(
        labels.iter().filter(|l| *l == "EffectIntent(n1)").count(),
        1,
        "exactly one EffectIntent for the single Mutation: {labels:?}"
    );
    let intent_idx = labels
        .iter()
        .position(|l| l == "EffectIntent(n1)")
        .expect("an EffectIntent was journaled before the side effect");
    assert_eq!(
        labels[intent_idx + 1],
        "EffectRecorded(n1)",
        "the Mutation's EffectRecorded immediately follows its Intent: {labels:?}"
    );
}

/// Acceptance §8.3 (happy path, resume): a completed Mutation (Intent+Recorded
/// journaled) is MEMOIZED on resume — replayed from the journal, never re-applied.
/// The shared sink proves the side effect lands exactly once across both runs.
#[tokio::test]
async fn mutation_resume_memoizes_completed_effect_without_reapplying() {
    let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "note it")],
    };

    // Seed: turn 0 records the Mutation (Intent+Recorded); turn 1 script-exhausted
    // → fails → no RunCompleted. The one live application lands in the sink.
    let (gw1, _c1) = scripted_gateway(vec![tool_call_response(
        "t1",
        "record_note",
        "{\"note\":\"hello\"}",
    )])
    .await;
    let o1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(
            ToolRegistry::default().with_tool(Arc::new(RecordNote::new(sink.clone()))),
        ))
        .run(run, &graph)
        .await
        .expect("seed yields an outcome");
    assert!(o1.failed.is_some(), "seed dies at turn 1");
    assert_eq!(
        &*sink.lock().unwrap(),
        &["hello".to_string()],
        "seed applied the mutation once"
    );

    // Resume on the SAME sink: the Mutation memo-hits (Recorded present) → replayed,
    // NOT re-applied → the sink is unchanged and the run completes.
    let (gw2, _c2) = scripted_gateway(vec![final_response("done")]).await;
    let o2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(
            ToolRegistry::default().with_tool(Arc::new(RecordNote::new(sink.clone()))),
        ))
        .start(run, &graph)
        .await
        .expect("resume completes");
    assert!(
        o2.failed.is_none() && o2.paused.is_none(),
        "{:?}",
        o2.failed
    );
    assert_eq!(
        &*sink.lock().unwrap(),
        &["hello".to_string()],
        "resume memoized the completed Mutation — side effect applied exactly once total"
    );
}

/// Build a journal in the **in-doubt** state (§7.3): a real run through the
/// two-phase Mutation, truncated to the prefix up to and including the note's
/// `EffectIntent` — so the resume sees an Intent with no matching `EffectRecorded`.
async fn seed_in_doubt_note() -> (InMemoryJournal, RunId) {
    let full = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "note it")],
    };
    let (gw, _c) = scripted_gateway(vec![
        tool_call_response("t1", "record_note", "{\"note\":\"hello\"}"),
        final_response("done"),
    ])
    .await;
    Executor::new(Arc::new(gw), Arc::new(full.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(
            RecordNote::new(Arc::new(std::sync::Mutex::new(Vec::new()))),
        ))))
        .run(run, &graph)
        .await
        .expect("seed run completes");

    let events = full.load(run).await.unwrap();
    let cut = events
        .iter()
        .position(|(_, e)| matches!(e, JournalEvent::EffectIntent { .. }))
        .expect("seed run journaled an EffectIntent");
    let seeded = InMemoryJournal::new();
    for (_, e) in &events[..=cut] {
        seeded.append(run, e.clone()).await.unwrap();
    }
    (seeded, run)
}

/// Resume an in-doubt run with a caller-owned note `sink` (its contents model
/// whether the side effect applied before the crash) + reconcilers; return the
/// outcome and journal events.
async fn resume_in_doubt(
    journal: InMemoryJournal,
    run: RunId,
    sink: Arc<std::sync::Mutex<Vec<String>>>,
    reconcilers: ReconcileRegistry,
) -> (RunOutcome, Vec<(Seq, JournalEvent)>) {
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "note it")],
    };
    let (gw, _c) = scripted_gateway(vec![final_response("done")]).await;
    let out = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(
            ToolRegistry::default().with_tool(Arc::new(RecordNote::new(sink))),
        ))
        .with_reconcilers(Arc::new(reconcilers))
        .start(run, &graph)
        .await
        .expect("resume yields an outcome");
    let events = journal.load(run).await.unwrap();
    (out, events)
}

/// Acceptance §8.4 — in-doubt Mutation, reconcile `Confirmed`: the world already
/// holds the note (the side effect applied before the crash), so the real
/// `NoteReconciler` confirms it — the executor records without re-running, and the
/// sink keeps exactly one copy. The run completes.
#[tokio::test]
async fn in_doubt_confirmed_records_without_rerunning_the_side_effect() {
    let note_eid = effect_id("n1", 0, 1);
    let (journal, run) = seed_in_doubt_note().await;
    let sink = Arc::new(std::sync::Mutex::new(vec!["hello".to_string()]));
    let reconcilers = ReconcileRegistry::default()
        .with_provider("record_note", Arc::new(NoteReconciler::new(sink.clone())));
    let (out, events) = resume_in_doubt(journal, run, sink.clone(), reconcilers).await;

    assert!(
        out.failed.is_none() && out.paused.is_none(),
        "Confirmed completes"
    );
    assert_eq!(
        &*sink.lock().unwrap(),
        &["hello".to_string()],
        "Confirmed: the side effect is NOT repeated — the sink still holds exactly one note"
    );
    assert_eq!(
        effect_recorded_count(&events, &note_eid),
        1,
        "Confirmed appends the Mutation's EffectRecorded once"
    );
    assert!(
        events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "the run completes"
    );
}

/// Acceptance §8.5 — in-doubt Mutation, reconcile `NotApplied`: the world does NOT
/// hold the note (the crash was before the side effect), so the real
/// `NoteReconciler` says NotApplied and the effect runs now — exactly once, under
/// the standing Intent (no second Intent). The run completes.
#[tokio::test]
async fn in_doubt_not_applied_runs_the_effect_once_under_the_standing_intent() {
    let (journal, run) = seed_in_doubt_note().await;
    let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let reconcilers = ReconcileRegistry::default()
        .with_provider("record_note", Arc::new(NoteReconciler::new(sink.clone())));
    let (out, events) = resume_in_doubt(journal, run, sink.clone(), reconcilers).await;

    assert!(
        out.failed.is_none() && out.paused.is_none(),
        "NotApplied completes"
    );
    assert_eq!(
        &*sink.lock().unwrap(),
        &["hello".to_string()],
        "NotApplied: the side effect runs exactly once on resume"
    );
    assert_eq!(
        events
            .iter()
            .filter(|(_, e)| matches!(e, JournalEvent::EffectIntent { .. }))
            .count(),
        1,
        "NotApplied re-uses the standing Intent — no second EffectIntent"
    );
    assert!(
        events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted))
    );
}

/// Acceptance §8.6 — in-doubt Mutation, `Indeterminate` (an `AlwaysIndeterminate`
/// provider that cannot decide): the executor pauses loud — journals `RunPaused`,
/// sets `outcome.paused`, applies NOTHING, and does not complete.
#[tokio::test]
async fn in_doubt_indeterminate_pauses_without_applying() {
    let (journal, run) = seed_in_doubt_note().await;
    let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let reconcilers =
        ReconcileRegistry::default().with_provider("record_note", Arc::new(AlwaysIndeterminate));
    let (out, events) = resume_in_doubt(journal, run, sink.clone(), reconcilers).await;

    let pause = out.paused.expect("Indeterminate pauses the run");
    assert_eq!(pause.node, NodeId("n1".into()));
    assert!(
        sink.lock().unwrap().is_empty(),
        "a paused in-doubt Mutation applies no side effect"
    );
    assert!(
        events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunPaused { .. })),
        "RunPaused is journaled"
    );
    assert!(
        !events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "a paused run does not complete"
    );
}

/// Acceptance §8.7 (no silent failure) — if a tool effect's recorded input hash
/// no longer matches the (replayed) tool call on resume, the executor halts loud
/// with a `DeterminismViolation` on that effect — it never silently re-runs or
/// re-memoizes, never re-executes the tool, and never touches the gateway.
#[tokio::test]
async fn changed_tool_input_on_resume_halts_with_determinism_violation() {
    let tool_eid = effect_id("n1", 0, 1);
    let counter = Arc::new(AtomicUsize::new(0));
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "search it")],
    };

    // Seed a consistent partial run: turn 0 records the model + the search
    // Observation (n1,0,1); turn 1 script-exhausted → fails → no RunCompleted.
    let (gw1, _c1) = scripted_gateway(vec![tool_call_response(
        "t1",
        "search",
        "{\"query\":\"rust\"}",
    )])
    .await;
    Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(
            ToolRegistry::default().with_tool(Arc::new(Search::new(counter.clone()))),
        ))
        .with_clock(Arc::new(AdvanceableClock::at(OBS_T0)))
        .run(run, &graph)
        .await
        .expect("seed yields an outcome");
    assert_eq!(counter.load(Ordering::SeqCst), 1, "seed read search once");

    // Tamper: copy the journal into a fresh one, rewriting ONLY the search effect's
    // recorded input_hash. The model turn is left untouched, so it replays cleanly
    // and the TOOL fence is what trips.
    let tampered = InMemoryJournal::new();
    for (_, e) in journal.load(run).await.unwrap() {
        let e = match e {
            JournalEvent::EffectRecorded {
                effect_id,
                node,
                class,
                seq,
                output,
                observation,
                ..
            } if effect_id == tool_eid => JournalEvent::EffectRecorded {
                node,
                effect_id,
                class,
                seq,
                output,
                observation,
                input_hash: "TAMPERED".into(),
            },
            other => other,
        };
        tampered.append(run, e).await.unwrap();
    }

    // Resume: turn 0 model memo-hits and replays the search tool call; the tool
    // effect's memoized input_hash ("TAMPERED") ≠ the replayed call's hash → halt.
    let (gw2, calls2) = recording_gateway().await;
    let err = Executor::new(Arc::new(gw2), Arc::new(tampered.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(
            ToolRegistry::default().with_tool(Arc::new(Search::new(counter.clone()))),
        ))
        .with_clock(Arc::new(AdvanceableClock::at(OBS_T0)))
        .start(run, &graph)
        .await
        .expect_err("a changed tool input halts the resume");
    match err {
        OrchestratorError::DeterminismViolation { node, effect_id } => {
            assert_eq!(node, NodeId("n1".into()));
            assert_eq!(
                effect_id, tool_eid,
                "the violation is on the tampered tool effect"
            );
        }
        other => panic!("expected DeterminismViolation, got {other:?}"),
    }
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "the tool was never re-executed"
    );
    assert_eq!(
        calls2.lock().unwrap().len(),
        0,
        "a determinism violation never touches the gateway"
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
                    activation: orchestrator_core::Activation::default(),
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

/// A gated-OUT `OnKeywords` skill (its keyword absent from the node input) is
/// omitted from the assembled prompt on BOTH the original run and the resume:
/// the input is unchanged, so `is_active` reproduces the same (false) decision,
/// the memoized turn's system prompt/input-hash is byte-identical, and the turn
/// replays with zero re-spend (no `DeterminismViolation`). Guards that
/// activation-gating is reproduced deterministically across the crash/resume seam.
#[tokio::test]
async fn agent_resume_with_a_gated_out_skill_replays_without_respend() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());

    // Agent "a" references skill "s", gated on the keyword "summarize".
    let registry = Arc::new(
        Registry::default()
            .with_agent(AgentDefinition {
                skills: vec!["s".into()],
                ..agent_def("c")
            })
            .with_skill(orchestrator_core::SkillDef {
                name: "s".into(),
                description: None,
                body: "SKILL_BODY".into(),
                activation: orchestrator_core::Activation::OnKeywords(vec!["summarize".into()]),
            }),
    );

    // Graph [agent n1, model n2]. n1's input MISSES "summarize" → the skill is
    // gated OUT. Run 1: n1's single turn succeeds (gateway call 1), then n2 fails
    // (gateway call 2) → n1 is fully journaled+completed, but there is NO
    // RunCompleted (a partial run to resume).
    let graph = Graph {
        nodes: vec![
            agent_node("n1", "a", "hello world"),
            Node {
                id: NodeId("n2".into()),
                kind: model_call("c", "b"),
                deps: vec![Dep::hard("n1")],
            },
        ],
    };
    let (gw1, _c1) = failing_after_gateway(1).await;
    let out1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_registry(registry.clone())
        .run(run, &graph)
        .await
        .expect("run 1 yields an outcome");
    assert!(
        out1.failed.is_some(),
        "n2 fails, leaving n1's gated-out turn journaled without RunCompleted"
    );

    // Resume with the SAME registry/input: n1's turn replays from memo (the skill
    // stays gated out → identical prompt → memo hit, NO gateway call), then n2
    // retries and the run completes. No DeterminismViolation, and only n2 touches
    // the gateway — n1's turn is reproduced from the memo with zero re-spend.
    let (gw2, calls2) = recording_gateway().await;
    let out2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_registry(registry)
        .start(run, &graph)
        .await
        .expect("resume completes without a determinism violation");
    assert!(
        out2.failed.is_none(),
        "resume completes cleanly (gated-out activation reproduced): {:?}",
        out2.failed
    );
    assert_eq!(
        calls2.lock().unwrap().len(),
        1,
        "only n2 is (re)driven on resume — n1's gated-out turn replays from memo with zero re-spend"
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
        JournalEvent::PlanExpanded { node, .. } => format!("PlanExpanded({})", node.0),
        JournalEvent::ContextWrite { key, .. } => format!("ContextWrite({})", key.0),
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
                effect_id: effect_id("n1", 0, 0),
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
        chain: Some("research.bulk".into()),
        chains: std::collections::HashMap::new(),
        grants: std::collections::HashMap::new(),
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
        chain: Some("research.bulk".into()),
        chains: std::collections::HashMap::new(),
        grants: std::collections::HashMap::new(),
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

/// Acceptance §8.8 (real-gateway e2e) — a `Map { body: Agent("researcher") }`
/// whose agent's ReAct loop calls the `Search` Observation, `Quorum`-aggregated →
/// `Consolidate { Agent("synthesizer") }`, PLUS an independent `Agent("recorder")`
/// node whose loop calls the `RecordNote` Mutation — all driven THROUGH the real
/// gateway (demo catalog, `research.bulk` fallover to the local model) with an
/// injected clock + reconcilers. Proves Observation fan-out (recorded with
/// provenance), Mutation two-phase, and a clean completion in one run. (Resume is
/// proven exhaustively by the §8.1–8.7 acceptance tests above.)
#[tokio::test]
async fn e2e_map_observation_agents_plus_mutation_agent_through_the_real_gateway() {
    let (gateway, _calls) = demo_reference_tool_gateway().await;
    let journal = InMemoryJournal::new();
    let search_calls = Arc::new(AtomicUsize::new(0));
    let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

    let mk_agent = |name: &str, tools: Vec<String>| AgentDefinition {
        name: name.into(),
        area: "research".into(),
        kind: "reasoning".into(),
        chain: Some("research.bulk".into()),
        chains: std::collections::HashMap::new(),
        grants: std::collections::HashMap::new(),
        tools,
        skills: vec![],
        system_prompt: "Work carefully.".into(),
    };
    let search = Arc::new(Search::new(search_calls.clone()));
    let recorder_tool = Arc::new(RecordNote::new(sink.clone()));
    let registry = Arc::new(
        Registry::default()
            .with_agent(mk_agent("researcher", vec!["search".into()]))
            .with_agent(mk_agent("synthesizer", vec![]))
            .with_agent(mk_agent("recorder", vec!["record_note".into()]))
            .with_tool(search.spec())
            .with_tool(recorder_tool.spec()),
    );
    let tools = Arc::new(
        ToolRegistry::default()
            .with_tool(search.clone())
            .with_tool(recorder_tool.clone()),
    );
    let reconcilers = Arc::new(
        ReconcileRegistry::default()
            .with_provider("record_note", Arc::new(NoteReconciler::new(sink.clone()))),
    );
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(registry)
        .with_tools(tools)
        .with_reconcilers(reconcilers)
        .with_clock(Arc::new(AdvanceableClock::at(OBS_T0)));

    let m = NodeId("m".into());
    let cons = NodeId("cons".into());
    let rec = NodeId("rec".into());
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
                    aggregation: Aggregation::Quorum {
                        min_count: None,
                        min_fraction: Some(0.6),
                    },
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
            agent_node("rec", "recorder", "log the run"),
        ],
    };

    let run = RunId(uuid::Uuid::new_v4());
    let outcome = exec.run(run, &graph).await.expect("e2e run");

    // Everything completes cleanly.
    assert!(
        outcome.failed.is_none() && outcome.paused.is_none(),
        "{:?}",
        outcome.failed
    );
    for n in [&m, &cons, &rec] {
        assert!(
            outcome.completed.contains(n),
            "{} completed: {:?}",
            n.0,
            outcome.completed
        );
    }
    assert_eq!(
        outcome.outputs[&m]["manifest"]["ok"], 3,
        "quorum met — all 3 children ok"
    );

    // Observation fan-out: each of the 3 children read Search live once, and each
    // records with provenance {source: "search"}.
    assert_eq!(
        search_calls.load(Ordering::SeqCst),
        3,
        "each fanned-out child agent read the Search Observation once"
    );
    let events = journal.load(run).await.unwrap();
    let obs_provenance = events
        .iter()
        .filter(|(_, e)| {
            matches!(e, JournalEvent::EffectRecorded { class: EffectClass::Observation, observation: Some(meta), .. } if meta.source == "search")
        })
        .count();
    assert_eq!(
        obs_provenance, 3,
        "3 Observation records carry search provenance across the fan-out"
    );

    // Mutation two-phase: the recorder's record_note journals EffectIntent then
    // EffectRecorded, and the side effect landed exactly once.
    assert_eq!(
        &*sink.lock().unwrap(),
        &["log the run".to_string()],
        "the Mutation applied exactly once through the real gateway"
    );
    let rec_labels: Vec<String> = events
        .iter()
        .filter(|(_, e)| {
            matches!(e,
                JournalEvent::EffectIntent { node, .. } | JournalEvent::EffectRecorded { node, .. }
                    if node.0 == "rec")
        })
        .map(|(_, e)| label(e))
        .collect();
    let intent_at = rec_labels
        .iter()
        .position(|l| l == "EffectIntent(rec)")
        .expect("recorder journaled an EffectIntent");
    assert_eq!(
        rec_labels[intent_at + 1],
        "EffectRecorded(rec)",
        "record_note is two-phase — Intent → Recorded: {rec_labels:?}"
    );

    // The synthesis agent ran on the reference chain and fell over to the local model.
    assert_eq!(
        outcome.outputs[&cons]["model"], "llama3.1-local",
        "the Consolidate's agent synthesized via the local fallover: {:?}",
        outcome.outputs[&cons]
    );
}

/// Regression (no-silent-failure §7.3): an in-doubt Mutation inside a fanned-out
/// Map CHILD pauses the WHOLE run loud — it must NEVER journal `RunCompleted` over
/// the unresolved Intent (which would silently abandon the side effect). Seed a
/// `Map{Agent("recorder")}` child through its `record_note` Mutation, truncate to
/// the child's `EffectIntent`, then resume with an Indeterminate reconciler → the
/// Map (and run) pause; `RunPaused` is journaled, `RunCompleted` is NOT, and no
/// side effect is applied.
#[tokio::test]
async fn in_doubt_mutation_in_a_map_child_pauses_the_whole_run() {
    let run = RunId(uuid::Uuid::new_v4());
    let mk_recorder = |sink: Arc<std::sync::Mutex<Vec<String>>>| {
        let recorder = AgentDefinition {
            name: "recorder".into(),
            area: "research".into(),
            kind: "reasoning".into(),
            chain: Some("research.bulk".into()),
            chains: std::collections::HashMap::new(),
            grants: std::collections::HashMap::new(),
            tools: vec!["record_note".into()],
            skills: vec![],
            system_prompt: "Record.".into(),
        };
        (
            Arc::new(
                Registry::default()
                    .with_agent(recorder)
                    .with_tool(RecordNote::new(sink.clone()).spec()),
            ),
            Arc::new(ToolRegistry::default().with_tool(Arc::new(RecordNote::new(sink)))),
        )
    };
    let map_graph = Graph {
        nodes: vec![Node {
            id: NodeId("m".into()),
            kind: NodeKind::Map {
                body: MapBody::Agent(AgentRef("recorder".into())),
                over: vec![serde_json::json!("item-0")],
                concurrency: 1,
                aggregation: Aggregation::BestEffort,
            },
            deps: vec![],
        }],
    };

    // Seed: run the Map to completion, then truncate to the child's EffectIntent
    // (drops its record_note EffectRecorded) → the child is in-doubt on resume.
    let full = InMemoryJournal::new();
    let (seed_reg, seed_tools) = mk_recorder(Arc::new(std::sync::Mutex::new(Vec::new())));
    let (gw_s, _c) = demo_reference_tool_gateway().await;
    Executor::new(Arc::new(gw_s), Arc::new(full.clone()), "v1")
        .with_registry(seed_reg)
        .with_tools(seed_tools)
        .run(run, &map_graph)
        .await
        .expect("seed Map run completes");
    let events = full.load(run).await.unwrap();
    let cut = events
        .iter()
        .position(|(_, e)| matches!(e, JournalEvent::EffectIntent { .. }))
        .expect("the child journaled a record_note EffectIntent");
    let seeded = InMemoryJournal::new();
    for (_, e) in &events[..=cut] {
        seeded.append(run, e.clone()).await.unwrap();
    }

    // Resume with an Indeterminate reconciler + a FRESH empty sink → the child's
    // Mutation is in-doubt → it pauses → the whole Map pauses.
    let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let (reg, tools) = mk_recorder(sink.clone());
    let reconcilers =
        ReconcileRegistry::default().with_provider("record_note", Arc::new(AlwaysIndeterminate));
    let (gw_r, _c2) = demo_reference_tool_gateway().await;
    let outcome = Executor::new(Arc::new(gw_r), Arc::new(seeded.clone()), "v1")
        .with_registry(reg)
        .with_tools(tools)
        .with_reconcilers(Arc::new(reconcilers))
        .start(run, &map_graph)
        .await
        .expect("resume yields an outcome");

    let pause = outcome
        .paused
        .expect("the in-doubt Map child pauses the whole run");
    assert_eq!(
        pause.node,
        NodeId("m".into()),
        "the Map node is the pause point"
    );
    let resumed = seeded.load(run).await.unwrap();
    assert!(
        resumed
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunPaused { .. })),
        "RunPaused is journaled"
    );
    assert!(
        !resumed
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "the run must NOT complete over an unresolved in-doubt Intent (no silent failure)"
    );
    assert!(
        sink.lock().unwrap().is_empty(),
        "a paused in-doubt Mutation applies no side effect"
    );
}

/// The pause is re-entrant: a run paused on an in-doubt Mutation (Indeterminate)
/// resumes to COMPLETION once its reconciler becomes decisive, and the standing
/// Intent still bounds the side effect to exactly one application.
#[tokio::test]
async fn a_paused_in_doubt_run_resumes_to_completion_when_the_reconciler_decides() {
    let (journal, run) = seed_in_doubt_note().await;
    let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));

    // First resume: Indeterminate → paused, nothing applied.
    let indeterminate =
        ReconcileRegistry::default().with_provider("record_note", Arc::new(AlwaysIndeterminate));
    let (out1, _e1) = resume_in_doubt(journal.clone(), run, sink.clone(), indeterminate).await;
    assert!(out1.paused.is_some(), "first resume pauses (Indeterminate)");
    assert!(
        sink.lock().unwrap().is_empty(),
        "nothing applied while paused"
    );

    // Second resume of the SAME journal (now carrying RunPaused, which the fold
    // ignores so the Mutation stays in-doubt): the reconciler now says NotApplied
    // → the effect runs exactly once and the run completes.
    let decisive = ReconcileRegistry::default()
        .with_provider("record_note", Arc::new(NoteReconciler::new(sink.clone())));
    let (out2, events2) = resume_in_doubt(journal, run, sink.clone(), decisive).await;
    assert!(
        out2.failed.is_none() && out2.paused.is_none(),
        "second resume completes: {:?}",
        out2.paused
    );
    assert_eq!(
        &*sink.lock().unwrap(),
        &["hello".to_string()],
        "the mutation applied exactly once across both resumes"
    );
    assert!(
        events2
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "the run completes"
    );
}

// ============================ SP-1 blackboard wiring ============================

/// The `with_context_store` seam exists and composes; behavior lands in later
/// tasks. Pins the builder is wired.
#[tokio::test]
async fn with_context_store_builder_is_wired() {
    use orchestrator_store::{InMemoryContentStore, InMemoryContextStore};
    let ctx = Arc::new(InMemoryContextStore::new(Arc::new(
        InMemoryContentStore::new(),
    )));
    let (gw, _c) = recording_gateway().await;
    let _exec =
        Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1").with_context_store(ctx);
}

/// Acceptance §8.2 — a completed node publishes to Run/node.id; the journal
/// carries a ContextWrite whose content is a CAS ref (not inline), and the blob
/// round-trips from the blackboard.
#[tokio::test]
async fn completed_node_publishes_a_context_ref_to_the_blackboard() {
    use orchestrator_store::{InMemoryContentStore, InMemoryContextStore};
    let content = Arc::new(InMemoryContentStore::new());
    let ctx = Arc::new(InMemoryContextStore::new(content.clone()));
    let (gw, _c) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (graph, n1, _n2) = two_node_graph("a", "b");
    let out = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_content_store(content)
        .with_context_store(ctx.clone())
        .run(run, &graph)
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{:?}", out.failed);
    let events = journal.load(run).await.unwrap();
    assert!(
        events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::ContextWrite { key, .. } if key.0 == n1.0)),
        "n1's completion journaled a ContextWrite: {:?}",
        events.iter().map(|(_, e)| label(e)).collect::<Vec<_>>()
    );
    let got = ctx
        .get(
            orchestrator_core::Scope::Run,
            orchestrator_core::ContextKey(n1.0.clone()),
        )
        .await
        .unwrap()
        .expect("n1 present on the blackboard");
    assert_eq!(ctx.load(&got).await.unwrap()["text"], "canned-response");
}

/// Acceptance §8.1 — no context store wired ⇒ NO ContextWrite events ⇒
/// byte-identical to slice 4.
#[tokio::test]
async fn no_context_store_journals_no_context_writes() {
    let (gw, _c) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (graph, _n1, _n2) = two_node_graph("a", "b");
    Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .run(run, &graph)
        .await
        .expect("run");
    assert!(
        journal
            .load(run)
            .await
            .unwrap()
            .iter()
            .all(|(_, e)| !matches!(e, JournalEvent::ContextWrite { .. })),
        "no store ⇒ no ContextWrite"
    );
}

/// Acceptance §8.4 — a duplicate (Run, key) publish surfaces ContextKeyCollision
/// loudly (never a silent overwrite). Pre-seed Run/"n1" WITHOUT a ContextWrite so
/// the fold-guard does not skip, then run — n1's publish collides.
#[tokio::test]
async fn duplicate_context_key_publish_is_a_loud_collision() {
    use orchestrator_store::{InMemoryContentStore, InMemoryContextStore};
    let content = Arc::new(InMemoryContentStore::new());
    let ctx = Arc::new(InMemoryContextStore::new(content.clone()));
    ctx.put(
        orchestrator_core::Scope::Run,
        orchestrator_core::ContextKey("n1".into()),
        serde_json::json!({ "pre": "seeded" }),
    )
    .await
    .unwrap();
    let (gw, _c) = recording_gateway().await;
    let (graph, _n1, _n2) = two_node_graph("a", "b");
    let err = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_content_store(content)
        .with_context_store(ctx)
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect_err("duplicate publish collides");
    assert!(
        matches!(err, OrchestratorError::ContextKeyCollision { .. }),
        "got {err:?}"
    );
}

/// An Agent node with declared dependencies (for blackboard read tests).
fn agent_node_with_deps(id: &str, agent: &str, input: &str, deps: Vec<Dep>) -> Node {
    Node {
        id: NodeId(id.into()),
        kind: NodeKind::Agent {
            agent: AgentRef(agent.into()),
            input: serde_json::json!(input),
            phase: None,
        },
        deps,
    }
}

/// Acceptance §8.3 — cross-role handoff: in A(agent) → B(agent, hard-dep A), B's
/// assembled system prompt (echoed back by the gateway) contains A's blackboard
/// output, proving B read A's context. The gateway echoes each agent's SYSTEM, so
/// A's output text is its distinct system marker ("PLANNER_SYS"); its appearance
/// in B's prompt can ONLY come from B reading A off the blackboard.
#[tokio::test]
async fn agent_prompt_includes_its_dependency_output_from_the_blackboard() {
    use orchestrator_store::{InMemoryContentStore, InMemoryContextStore};
    let content = Arc::new(InMemoryContentStore::new());
    let ctx = Arc::new(InMemoryContextStore::new(content.clone()));
    let (gw, _c) = echo_system_gateway().await;
    let mk = |name: &str, sys: &str| AgentDefinition {
        name: name.into(),
        area: "research".into(),
        kind: "reasoning".into(),
        chain: Some("c".into()),
        chains: std::collections::HashMap::new(),
        grants: std::collections::HashMap::new(),
        tools: vec![],
        skills: vec![],
        system_prompt: sys.into(),
    };
    let registry = Arc::new(
        Registry::default()
            .with_agent(mk("planner", "PLANNER_SYS"))
            .with_agent(mk("refiner", "REFINER_SYS")),
    );
    let graph = Graph {
        nodes: vec![
            agent_node("A", "planner", "plan it"),
            agent_node_with_deps("B", "refiner", "refine", vec![Dep::hard("A")]),
        ],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(registry)
        .with_tools(Arc::new(ToolRegistry::default()))
        .with_content_store(content)
        .with_context_store(ctx)
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    let b_text = out.outputs[&NodeId("B".into())]["text"]
        .as_str()
        .expect("B has text");
    assert!(
        b_text.starts_with("REFINER_SYS"),
        "B's prompt starts with its own system: {b_text}"
    );
    assert!(
        b_text.contains("## Context") && b_text.contains("### A"),
        "B's prompt has a Context section keyed by its dependency A: {b_text}"
    );
    assert!(
        b_text.contains("PLANNER_SYS"),
        "B read A's blackboard output (A's system marker) into its prompt: {b_text}"
    );
}

/// Acceptance §8.5 — resume rehydrates the blackboard and re-spends nothing.
/// A(model)→B(agent, dep A): B dies at turn 1; resuming with a FRESH context
/// store (empty entries) sharing the SAME CAS forces rehydration to repopulate
/// A's entry, so B's prompt is identical and its completed turns replay — the
/// resume gateway sees only B's tail turn.
#[tokio::test]
async fn resume_rehydrates_the_blackboard_and_respends_nothing() {
    use orchestrator_store::{InMemoryContentStore, InMemoryContextStore};
    let content = Arc::new(InMemoryContentStore::new());
    let ctx = Arc::new(InMemoryContextStore::new(content.clone()));
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![
            Node {
                id: NodeId("A".into()),
                kind: model_call("c", "plan"),
                deps: vec![],
            },
            agent_node_with_deps("B", "a", "refine", vec![Dep::hard("A")]),
        ],
    };
    let (gw1, _c1) = scripted_gateway(vec![
        final_response("A-done"),
        tool_call_response("t1", "calc", "{\"op\":\"add\",\"a\":1,\"b\":1}"),
    ])
    .await;
    let o1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools())
        .with_content_store(content.clone())
        .with_context_store(ctx)
        .run(run, &graph)
        .await
        .expect("seed");
    assert!(o1.failed.is_some(), "B fails at turn 1 (script exhausted)");

    let ctx2 = Arc::new(InMemoryContextStore::new(content.clone()));
    let (gw2, calls2) = scripted_gateway(vec![final_response("the answer is 2")]).await;
    let o2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools())
        .with_content_store(content)
        .with_context_store(ctx2)
        .start(run, &graph)
        .await
        .expect("resume");
    assert!(
        o2.failed.is_none() && o2.paused.is_none(),
        "{:?}",
        o2.failed
    );
    assert_eq!(
        calls2.lock().unwrap().len(),
        1,
        "resume re-spent only B's tail turn (A + B turn 0 memoized, blackboard rehydrated)"
    );
}

/// Acceptance §8.7 — a tampered upstream context on resume halts loud. Rewrite
/// A's `ContextWrite` to point at a different blob; B's prompt then differs from
/// the memoized turn → `DeterminismViolation` on B, never a silent mix.
#[tokio::test]
async fn tampered_upstream_context_on_resume_halts_with_determinism_violation() {
    use orchestrator_store::{InMemoryContentStore, InMemoryContextStore};
    let content = Arc::new(InMemoryContentStore::new());
    let ctx = Arc::new(InMemoryContextStore::new(content.clone()));
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![
            Node {
                id: NodeId("A".into()),
                kind: model_call("c", "plan"),
                deps: vec![],
            },
            agent_node_with_deps("B", "a", "refine", vec![Dep::hard("A")]),
        ],
    };
    let (gw1, _c1) = scripted_gateway(vec![
        final_response("A-done"),
        tool_call_response("t1", "calc", "{\"op\":\"add\",\"a\":1,\"b\":1}"),
    ])
    .await;
    Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools())
        .with_content_store(content.clone())
        .with_context_store(ctx)
        .run(run, &graph)
        .await
        .expect("seed");

    // Tamper: a different blob, and rewrite A's ContextWrite to reference it.
    let bytes = serde_json::to_vec(&serde_json::json!({"model":"m","text":"TAMPERED"})).unwrap();
    let digest = content.put(&bytes).await.unwrap();
    let tampered = InMemoryJournal::new();
    for (_, e) in journal.load(run).await.unwrap() {
        let e = match e {
            JournalEvent::ContextWrite {
                scope,
                key,
                summary,
                seq,
                ..
            } if key.0 == "A" => JournalEvent::ContextWrite {
                scope,
                key,
                summary,
                seq,
                content: orchestrator_core::ContentRef {
                    digest: digest.clone(),
                    size: bytes.len(),
                    summary: None,
                },
            },
            other => other,
        };
        tampered.append(run, e).await.unwrap();
    }

    let ctx2 = Arc::new(InMemoryContextStore::new(content.clone()));
    let (gw2, _c2) = scripted_gateway(vec![final_response("done")]).await;
    let err = Executor::new(Arc::new(gw2), Arc::new(tampered.clone()), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools())
        .with_content_store(content)
        .with_context_store(ctx2)
        .start(run, &graph)
        .await
        .expect_err("tampered upstream context halts the resume");
    assert!(
        matches!(&err, OrchestratorError::DeterminismViolation { node, .. } if node.0 == "B"),
        "got {err:?}"
    );
}

/// Acceptance §8.7 (over-budget) — an oversized dependency context busts the
/// per-turn window and halts loud (`over budget`), never silently truncated.
#[tokio::test]
async fn oversized_dependency_context_halts_over_budget_never_truncates() {
    use orchestrator_store::{InMemoryContentStore, InMemoryContextStore};
    let content = Arc::new(InMemoryContentStore::new());
    let ctx = Arc::new(InMemoryContextStore::new(content.clone()));
    let graph = Graph {
        nodes: vec![
            Node {
                id: NodeId("A".into()),
                kind: model_call("c", "plan"),
                deps: vec![],
            },
            agent_node_with_deps("B", "a", "refine", vec![Dep::hard("A")]),
        ],
    };
    // A's output is huge → B's prompt (with A's context) exceeds the 4096 window.
    let (gw, _c) = scripted_gateway(vec![final_response(&"x".repeat(100_000))]).await;
    let out = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools())
        .with_content_store(content)
        .with_context_store(ctx)
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run yields an outcome");
    match &out.failed {
        Some((node, msg)) => {
            assert_eq!(node.0, "B");
            assert!(msg.contains("over budget"), "{msg}");
        }
        None => panic!("expected B to halt over budget"),
    }
}

/// Regression (determinism, review Finding 1): a SOFT dependency is NOT read into
/// an agent's context, so a soft dep that flips terminal-state across a crash
/// (Failed on run 1 → Succeeds on resume) does NOT change the dependent's prompt
/// and the resume completes cleanly — reads are Hard-dep-only so the resolved
/// context stays a pure function of the journal.
#[tokio::test]
async fn a_soft_dependency_is_not_read_so_a_flip_across_resume_is_safe() {
    use orchestrator_store::{InMemoryContentStore, InMemoryContextStore};
    let content = Arc::new(InMemoryContentStore::new());
    let ctx = Arc::new(InMemoryContextStore::new(content.clone()));
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    // S is a ModelCall that FAILS on run 1 (prompt contains FAIL); B is an agent
    // that SOFT-deps S. B still runs (soft dep terminal) with empty context.
    let graph = Graph {
        nodes: vec![
            Node {
                id: NodeId("S".into()),
                kind: model_call("c", "FAIL"),
                deps: vec![],
            },
            agent_node_with_deps("B", "a", "go", vec![Dep::soft("S")]),
        ],
    };
    // Run 1: content-gated gateway fails S; B (soft-dep, terminal) runs turn 0
    // (calc) then turn 1 exhausted... use scripted so B partially completes.
    // Simpler: content_gated fails S and serves B. Drive B to completion.
    let (gw1, _c1) = content_gated_gateway().await;
    let o1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(ToolRegistry::default()))
        .with_content_store(content.clone())
        .with_context_store(ctx)
        .run(run, &graph)
        .await
        .expect("run 1");
    // S failed (no ContextWrite for S); B completed with empty context; the
    // failure suppressed RunCompleted → resumable.
    assert!(o1.failed.is_some(), "S failed");
    assert!(
        o1.completed.iter().any(|n| n.0 == "B"),
        "B ran despite the soft-dep failure"
    );
    let events1 = journal.load(run).await.unwrap();
    assert!(
        !events1
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::ContextWrite { key, .. } if key.0 == "S")),
        "a failed S published no ContextWrite"
    );

    // Resume: S now SUCCEEDS (recording gw) and publishes ContextWrite(S). B is
    // memoized. If B had read the soft dep, its prompt would now differ and the
    // resume would trip DeterminismViolation — it must NOT (soft deps not read).
    let ctx2 = Arc::new(InMemoryContextStore::new(content.clone()));
    let (gw2, _c2) = recording_gateway().await;
    let o2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(ToolRegistry::default()))
        .with_content_store(content)
        .with_context_store(ctx2)
        .start(run, &graph)
        .await
        .expect("resume must not trip a determinism violation on a soft-dep flip");
    assert!(
        o2.failed.is_none() && o2.paused.is_none(),
        "resume completes: {:?}",
        o2.failed
    );
}

// ================================ SP-1 Loop node ================================

/// Acceptance §9.1 — stop on gate: a Loop whose body emits the marker at
/// iteration 1 completes with iterations=2, converged=true, and ran the body twice.
#[tokio::test]
async fn loop_stops_when_the_gate_fires() {
    let (gw, calls) = scripted_gateway(vec![
        final_response("keep going"),
        final_response("we are DONE"),
    ])
    .await;
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("L".into()),
            kind: NodeKind::Loop {
                body: MapBody::ModelCall { chain: "c".into() },
                input: serde_json::json!({ "prompt": "start" }),
                gate: LoopGate::TextContains("DONE".into()),
                max_iters: 5,
            },
            deps: vec![],
        }],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{:?}", out.failed);
    let l = &out.outputs[&NodeId("L".into())];
    assert_eq!(l["iterations"], 2, "stopped at the 2nd iteration");
    assert_eq!(l["converged"], true);
    assert_eq!(l["output"]["text"], "we are DONE");
    assert_eq!(calls.lock().unwrap().len(), 2, "body ran exactly twice");
}

/// Acceptance §9.2 — cap without stop: the gate never fires, so the Loop runs
/// exactly max_iters and completes best-effort with converged=false (NOT failed).
#[tokio::test]
async fn loop_caps_at_max_iters_and_completes_unconverged() {
    let (gw, calls) = recording_gateway().await; // always "canned-response", never "STOP"
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("L".into()),
            kind: NodeKind::Loop {
                body: MapBody::ModelCall { chain: "c".into() },
                input: serde_json::json!({ "prompt": "go" }),
                gate: LoopGate::TextContains("STOP".into()),
                max_iters: 3,
            },
            deps: vec![],
        }],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(
        out.failed.is_none(),
        "cap is best-effort, not a failure: {:?}",
        out.failed
    );
    let l = &out.outputs[&NodeId("L".into())];
    assert_eq!(l["iterations"], 3);
    assert_eq!(l["converged"], false, "hit the cap without converging");
    assert_eq!(
        calls.lock().unwrap().len(),
        3,
        "ran exactly max_iters times"
    );
}

/// Acceptance §9.4 — a body failure fails the whole Loop (no silent finalize).
#[tokio::test]
async fn loop_body_failure_fails_the_loop() {
    let (gw, _c) = content_gated_gateway().await; // fails any prompt containing FAIL
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("L".into()),
            kind: NodeKind::Loop {
                body: MapBody::ModelCall { chain: "c".into() },
                input: serde_json::json!({ "prompt": "FAIL" }),
                gate: LoopGate::TextContains("never".into()),
                max_iters: 3,
            },
            deps: vec![],
        }],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run yields an outcome");
    let (node, msg) = out
        .failed
        .as_ref()
        .expect("the loop fails on a body failure");
    assert_eq!(node.0, "L");
    assert!(
        msg.contains("iteration 0"),
        "names the failing iteration: {msg}"
    );
}

/// Acceptance §9.3 — refine thread: iteration i>0 receives i-1's output as its
/// input. With an Agent body on the content-gated chain (echoes `ok:{input}`),
/// iteration 1's output text embeds iteration 0's output ("ok:start").
#[tokio::test]
async fn loop_threads_each_iterations_output_into_the_next() {
    let (gw, _c) = content_gated_gateway().await; // returns "ok:{first user message}"
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("L".into()),
            kind: NodeKind::Loop {
                body: MapBody::Agent(AgentRef("a".into())),
                input: serde_json::json!("start"),
                gate: LoopGate::TextContains("NEVER".into()),
                max_iters: 2,
            },
            deps: vec![],
        }],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(ToolRegistry::default()))
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{:?}", out.failed);
    let l = &out.outputs[&NodeId("L".into())];
    assert_eq!(l["iterations"], 2);
    let final_text = l["output"]["text"].as_str().expect("final text");
    // Non-vacuous: iteration 1's input is iteration 0's answer TEXT ("ok:start"),
    // so the echoing gateway returns "ok:ok:start". Without threading, iteration 1
    // would see the original "start" and return "ok:start" — so the extra "ok:"
    // prefix distinguishes the refine thread from no-threading.
    assert_eq!(
        final_text, "ok:ok:start",
        "iteration 1 received iteration 0's answer text as input (refine thread): {final_text}"
    );
}

/// Acceptance §9.3 (ModelCall body, review Finding 1) — the refine thread also
/// works for a `ModelCall` body: iteration 1's PROMPT is iteration 0's answer
/// text, so the echoing chain returns "ok:ok:start" (not the empty-prompt "ok:").
#[tokio::test]
async fn loop_threads_the_prior_answer_into_a_modelcall_bodys_prompt() {
    let (gw, _c) = content_gated_gateway().await; // returns "ok:{prompt}"
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("L".into()),
            kind: NodeKind::Loop {
                body: MapBody::ModelCall { chain: "c".into() },
                input: serde_json::json!({ "prompt": "start" }),
                gate: LoopGate::TextContains("NEVER".into()),
                max_iters: 2,
            },
            deps: vec![],
        }],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{:?}", out.failed);
    let final_text = out.outputs[&NodeId("L".into())]["output"]["text"]
        .as_str()
        .expect("final text");
    assert_eq!(
        final_text, "ok:ok:start",
        "iteration 1's prompt was iteration 0's answer text, not an empty prompt: {final_text}"
    );
}

/// A `Loop L → ModelCall n2` graph where the loop never converges (caps at 2).
fn loop_then_modelcall_graph() -> Graph {
    Graph {
        nodes: vec![
            Node {
                id: NodeId("L".into()),
                kind: NodeKind::Loop {
                    body: MapBody::ModelCall { chain: "c".into() },
                    input: serde_json::json!({ "prompt": "go" }),
                    gate: LoopGate::TextContains("STOP".into()), // never fires → cap at 2
                    max_iters: 2,
                },
                deps: vec![],
            },
            Node {
                id: NodeId("n2".into()),
                kind: model_call("c", "after"),
                deps: vec![Dep::hard("L")],
            },
        ],
    }
}

/// Acceptance §9.5 — resume replays completed iterations without re-spending.
/// Seed: L's 2 iterations succeed, n2 fails (no RunCompleted). Resume: L's
/// iterations memo-hit (0 gateway calls), n2 runs live → exactly 1 call.
#[tokio::test]
async fn loop_resume_replays_completed_iterations_without_respending() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = loop_then_modelcall_graph();
    let (gw1, _c1) = failing_after_gateway(2).await; // L iters 1,2 ok; n2 (call 3) fails
    let o1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .run(run, &graph)
        .await
        .expect("seed");
    assert!(
        o1.failed.is_some(),
        "n2 fails, L completed → no RunCompleted"
    );
    let (gw2, calls2) = recording_gateway().await;
    let o2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .start(run, &graph)
        .await
        .expect("resume");
    assert!(o2.failed.is_none(), "{:?}", o2.failed);
    assert_eq!(
        calls2.lock().unwrap().len(),
        1,
        "resume re-spent only n2 (L's iterations memoized)"
    );
    // The Loop's own control events are fold-guarded: exactly one across both runs,
    // never re-journaled on the resumed replay of the completed Loop.
    let labels: Vec<String> = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .map(|(_, e)| label(e))
        .collect();
    assert_eq!(
        labels.iter().filter(|l| *l == "NodeStarted(L)").count(),
        1,
        "one NodeStarted(L) across both runs: {labels:?}"
    );
    assert_eq!(
        labels.iter().filter(|l| *l == "NodeCompleted(L)").count(),
        1,
        "one NodeCompleted(L) across both runs (fold-guarded replay): {labels:?}"
    );
}

/// Acceptance §9.6 — a tampered completed iteration halts loud on resume. Rewrite
/// iteration 0's body effect input_hash; resume → L replays iteration 0 → memo
/// mismatch → DeterminismViolation, gateway untouched.
#[tokio::test]
async fn loop_resume_halts_on_a_tampered_iteration() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = loop_then_modelcall_graph();
    let (gw1, _c1) = failing_after_gateway(2).await;
    Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .run(run, &graph)
        .await
        .expect("seed");

    let target = effect_id("L/0", 0, 0);
    let tampered = InMemoryJournal::new();
    for (_, e) in journal.load(run).await.unwrap() {
        let e = match e {
            JournalEvent::EffectRecorded {
                effect_id,
                node,
                class,
                seq,
                output,
                observation,
                ..
            } if effect_id == target => JournalEvent::EffectRecorded {
                effect_id,
                node,
                class,
                seq,
                output,
                observation,
                input_hash: "TAMPERED".into(),
            },
            other => other,
        };
        tampered.append(run, e).await.unwrap();
    }

    let (gw2, calls2) = recording_gateway().await;
    let err = Executor::new(Arc::new(gw2), Arc::new(tampered.clone()), "v1")
        .start(run, &graph)
        .await
        .expect_err("tampered iteration halts the resume");
    assert!(
        matches!(&err, OrchestratorError::DeterminismViolation { node, .. } if node.0 == "L/0"),
        "got {err:?}"
    );
    assert_eq!(
        calls2.lock().unwrap().len(),
        0,
        "a determinism violation never touches the gateway"
    );
}

/// Acceptance §9 (real-gateway e2e) — a `Loop { body: ModelCall(research.bulk) }`
/// drives the REAL demo-catalog gateway each iteration, falling over the
/// credential-gated cloud entries to `llama3.1-local`; the gate never fires, so it
/// caps at 2 and completes `converged: false` after 2 local calls.
#[tokio::test]
async fn loop_drives_the_real_reference_chain_each_iteration() {
    let (gw, calls) = demo_reference_gateway().await;
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("L".into()),
            kind: NodeKind::Loop {
                body: MapBody::ModelCall {
                    chain: "research.bulk".into(),
                },
                input: serde_json::json!({ "prompt": "iterate" }),
                gate: LoopGate::TextContains("NEVER".into()),
                max_iters: 2,
            },
            deps: vec![],
        }],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("e2e run");
    assert!(out.failed.is_none(), "{:?}", out.failed);
    let l = &out.outputs[&NodeId("L".into())];
    assert_eq!(l["iterations"], 2);
    assert_eq!(l["converged"], false);
    assert_eq!(
        l["output"]["model"], "llama3.1-local",
        "each iteration fell over to the local model: {l}"
    );
    assert_eq!(
        calls.lock().unwrap().len(),
        2,
        "2 iterations each hit the local adapter once"
    );
}

// ============================= SP-1 OrchestratorHooks ==========================

/// A hooks spy: each fired hook appends a "label(args)" string.
#[derive(Clone, Default)]
struct RecordingHooks(Arc<std::sync::Mutex<Vec<String>>>);
impl RecordingHooks {
    fn log(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
    fn push(&self, s: String) {
        self.0.lock().unwrap().push(s);
    }
}
#[async_trait::async_trait]
impl OrchestratorHooks for RecordingHooks {
    async fn on_run_started(&self, _r: RunId) {
        self.push("run_started".into());
    }
    async fn on_run_completed(&self, _r: RunId) {
        self.push("run_completed".into());
    }
    async fn on_run_paused(&self, _r: RunId, reason: &str) {
        self.push(format!("run_paused({reason})"));
    }
    async fn on_node_started(&self, _r: RunId, n: &NodeId) {
        self.push(format!("node_started({})", n.0));
    }
    async fn on_node_completed(&self, _r: RunId, n: &NodeId) {
        self.push(format!("node_completed({})", n.0));
    }
    async fn on_node_failed(&self, _r: RunId, n: &NodeId, _e: &str) {
        self.push(format!("node_failed({})", n.0));
    }
    async fn on_node_skipped(&self, _r: RunId, n: &NodeId) {
        self.push(format!("node_skipped({})", n.0));
    }
    async fn on_agent_started(&self, _r: RunId, n: &NodeId, agent: &str, chain: &str) {
        self.push(format!("agent_started({},{agent},{chain})", n.0));
    }
    async fn on_agent_turn(&self, _r: RunId, n: &NodeId, turn: usize) {
        self.push(format!("agent_turn({},{turn})", n.0));
    }
    async fn on_agent_tool_call(&self, _r: RunId, n: &NodeId, tool: &str) {
        self.push(format!("agent_tool_call({},{tool})", n.0));
    }
    async fn on_context_write(
        &self,
        _r: RunId,
        _s: &orchestrator_core::Scope,
        k: &orchestrator_core::ContextKey,
    ) {
        self.push(format!("context_write({})", k.0));
    }
}

/// Acceptance §9.1 — run + node lifecycle fires in order.
#[tokio::test]
async fn hooks_fire_run_and_node_lifecycle_in_order() {
    let hooks = RecordingHooks::default();
    let (gw, _c) = recording_gateway().await;
    let (graph, _n1, _n2) = two_node_graph("a", "b");
    Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_hooks(Arc::new(hooks.clone()))
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert_eq!(
        hooks.log(),
        vec![
            "run_started",
            "node_started(n1)",
            "node_completed(n1)",
            "node_started(n2)",
            "node_completed(n2)",
            "run_completed",
        ]
    );
}

/// Acceptance §9.3 — a failed node fires on_node_failed; a hard-dependent fires
/// on_node_skipped; a failed run does not fire run_completed.
#[tokio::test]
async fn hooks_fire_failure_and_cascade_skip() {
    let hooks = RecordingHooks::default();
    let (gw, _c) = content_gated_gateway().await;
    let mc = |p: &str| NodeKind::ModelCall {
        chain: "c".into(),
        payload: serde_json::json!({ "prompt": p }),
    };
    let graph = Graph {
        nodes: vec![
            Node {
                id: NodeId("f".into()),
                kind: mc("FAIL"),
                deps: vec![],
            },
            Node {
                id: NodeId("h".into()),
                kind: mc("ok"),
                deps: vec![Dep::hard("f")],
            },
        ],
    };
    Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_hooks(Arc::new(hooks.clone()))
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run yields an outcome");
    let log = hooks.log();
    assert!(log.contains(&"node_failed(f)".to_string()), "{log:?}");
    assert!(log.contains(&"node_skipped(h)".to_string()), "{log:?}");
    assert!(
        !log.contains(&"run_completed".to_string()),
        "a failed run does not complete: {log:?}"
    );
}

/// Acceptance §9.7 — no hooks wired ⇒ identical journal (hooks change nothing).
#[tokio::test]
async fn hooks_unwired_is_byte_identical() {
    let (graph, _n1, _n2) = two_node_graph("a", "b");
    let run = RunId(uuid::Uuid::new_v4());
    let (gw1, _c1) = recording_gateway().await;
    let j1 = InMemoryJournal::new();
    Executor::new(Arc::new(gw1), Arc::new(j1.clone()), "v1")
        .run(run, &graph)
        .await
        .unwrap();
    let (gw2, _c2) = recording_gateway().await;
    let j2 = InMemoryJournal::new();
    Executor::new(Arc::new(gw2), Arc::new(j2.clone()), "v1")
        .with_hooks(Arc::new(RecordingHooks::default()))
        .run(run, &graph)
        .await
        .unwrap();
    let l1: Vec<String> = j1
        .load(run)
        .await
        .unwrap()
        .iter()
        .map(|(_, e)| label(e))
        .collect();
    let l2: Vec<String> = j2
        .load(run)
        .await
        .unwrap()
        .iter()
        .map(|(_, e)| label(e))
        .collect();
    assert_eq!(l1, l2, "hooks change no journaled event");
}

/// Acceptance §9.2 — agent lifecycle: an Agent node that makes one tool call then
/// finishes fires agent_started, agent_turn(0), agent_tool_call, agent_turn(1),
/// plus the generic node_started/node_completed.
#[tokio::test]
async fn hooks_fire_agent_lifecycle() {
    let hooks = RecordingHooks::default();
    let (gw, _c) = scripted_gateway(vec![
        tool_call_response("t1", "calc", "{\"op\":\"add\",\"a\":2,\"b\":3}"),
        final_response("the answer is 5"),
    ])
    .await;
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "add 2 and 3")],
    };
    Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools())
        .with_hooks(Arc::new(hooks.clone()))
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    let log = hooks.log();
    for expected in [
        "node_started(n1)",
        "agent_started(n1,a,c)",
        "agent_turn(n1,0)",
        "agent_tool_call(n1,calc)",
        "agent_turn(n1,1)",
        "node_completed(n1)",
    ] {
        assert!(
            log.contains(&expected.to_string()),
            "missing {expected}: {log:?}"
        );
    }
}

/// Acceptance §9.6 (headline) — hooks do NOT re-fire for the replayed prefix on
/// resume. Seed n1 ok / n2 failed (no RunCompleted); a spy attached to the RESUME
/// sees only n2's tail — n1 (replayed from the memo) fires no hook.
#[tokio::test]
async fn hooks_do_not_refire_for_the_replayed_prefix_on_resume() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (graph, _n1, _n2) = two_node_graph("a", "b");
    let (gw1, _c1) = failing_after_gateway(1).await; // n1 ok, n2 fails
    Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .run(run, &graph)
        .await
        .expect("seed");
    let hooks = RecordingHooks::default();
    let (gw2, _c2) = recording_gateway().await;
    Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_hooks(Arc::new(hooks.clone()))
        .start(run, &graph)
        .await
        .expect("resume");
    let log = hooks.log();
    assert!(
        !log.iter().any(|l| l.contains("(n1)")),
        "n1 (replayed) fires no hook on resume: {log:?}"
    );
    assert!(
        log.contains(&"node_started(n2)".to_string())
            && log.contains(&"node_completed(n2)".to_string()),
        "n2's tail fires: {log:?}"
    );
    assert!(log.contains(&"run_completed".to_string()));
}

/// Acceptance §9.4 — an in-doubt Mutation resume that pauses fires on_run_paused
/// and NOT on_run_completed.
#[tokio::test]
async fn hooks_fire_run_paused_on_an_in_doubt_pause() {
    let (journal, run) = seed_in_doubt_note().await;
    let hooks = RecordingHooks::default();
    let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let reconcilers =
        ReconcileRegistry::default().with_provider("record_note", Arc::new(AlwaysIndeterminate));
    let (gw, _c) = scripted_gateway(vec![final_response("done")]).await;
    let out = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(
            ToolRegistry::default().with_tool(Arc::new(RecordNote::new(sink))),
        ))
        .with_reconcilers(Arc::new(reconcilers))
        .with_hooks(Arc::new(hooks.clone()))
        .start(run, &graph_note_it())
        .await
        .expect("resume yields an outcome");
    assert!(out.paused.is_some(), "the run paused");
    let log = hooks.log();
    assert!(
        log.iter().any(|l| l.starts_with("run_paused(")),
        "on_run_paused fired: {log:?}"
    );
    assert!(
        !log.contains(&"run_completed".to_string()),
        "a paused run does not complete: {log:?}"
    );
}

/// The single-`note` agent graph the in-doubt seed/resume share.
fn graph_note_it() -> Graph {
    Graph {
        nodes: vec![agent_node("n1", "a", "note it")],
    }
}

/// Acceptance §9.5 — a completed node's blackboard publish fires on_context_write.
#[tokio::test]
async fn hooks_fire_on_context_write() {
    use orchestrator_store::{InMemoryContentStore, InMemoryContextStore};
    let content = Arc::new(InMemoryContentStore::new());
    let ctx = Arc::new(InMemoryContextStore::new(content.clone()));
    let hooks = RecordingHooks::default();
    let (gw, _c) = recording_gateway().await;
    let (graph, _n1, _n2) = two_node_graph("a", "b");
    Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_content_store(content)
        .with_context_store(ctx)
        .with_hooks(Arc::new(hooks.clone()))
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    let log = hooks.log();
    assert!(log.contains(&"context_write(n1)".to_string()), "{log:?}");
    assert!(log.contains(&"context_write(n2)".to_string()), "{log:?}");
}

/// Acceptance §9.6 (agent path, review Finding 1) — agent hooks do NOT re-fire for
/// a replayed completed prefix on resume. Seed an agent whose turn 0 (a calc tool
/// call) completes but turn 1 fails; resume with a spy: turn 0's memoized
/// model+tool replay fires NO agent_started/agent_turn(0)/agent_tool_call/
/// node_started, only the live tail (agent_turn(1) + node_completed).
#[tokio::test]
async fn agent_hooks_do_not_refire_for_a_replayed_prefix_on_resume() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "add 2 and 3")],
    };
    // Seed: turn 0 (calc) records; turn 1 script-exhausted → fails → no RunCompleted.
    let (gw1, _c1) = scripted_gateway(vec![tool_call_response(
        "t1",
        "calc",
        "{\"op\":\"add\",\"a\":2,\"b\":3}",
    )])
    .await;
    let o1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools())
        .run(run, &graph)
        .await
        .expect("seed");
    assert!(o1.failed.is_some(), "seed fails at turn 1");

    // Resume with a spy: only the live tail fires.
    let hooks = RecordingHooks::default();
    let (gw2, _c2) = scripted_gateway(vec![final_response("the answer is 5")]).await;
    Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools())
        .with_hooks(Arc::new(hooks.clone()))
        .start(run, &graph)
        .await
        .expect("resume");
    let log = hooks.log();
    for suppressed in [
        "agent_started(n1,a,c)",
        "agent_turn(n1,0)",
        "agent_tool_call(n1,calc)",
        "node_started(n1)",
    ] {
        assert!(
            !log.contains(&suppressed.to_string()),
            "replayed prefix must not re-fire {suppressed}: {log:?}"
        );
    }
    // The live tail (turn 1) + completion DO fire.
    assert!(
        log.contains(&"agent_turn(n1,1)".to_string()),
        "live tail turn fires: {log:?}"
    );
    assert!(log.contains(&"node_completed(n1)".to_string()), "{log:?}");
}

// ============================= SP-1 quota→pause ===============================

/// Acceptance §6.1 — the warm-up fixture yields a genuine AllGated: a first
/// (warm-up) execute times out and cools the sole router; the second execute is
/// all-gated with a timed resume_after.
#[tokio::test]
async fn warmup_gateway_yields_allgated_with_resume_after() {
    use crate::test_support::timeout_gateway;
    let gw = timeout_gateway().await;
    let req = support::build_request("c", &serde_json::json!({ "prompt": "x" }));
    let _warm = gw.execute(&req).await; // times out → cools router "r"
    let second = gw.execute(&req).await;
    assert!(
        matches!(
            second,
            Err(kernel::types::error::GatewayError::AllGated {
                resume_after: Some(_),
                ..
            })
        ),
        "second execute is AllGated with a timed resume_after: {second:?}"
    );
}

/// Acceptance §6.3 — a top-level ModelCall node whose chain is all-gated (timed)
/// PAUSES: RunOutcome.paused set, RunPaused{resume_after:Some} journaled, no
/// RunCompleted, and on_run_paused fires.
#[tokio::test]
async fn modelcall_node_pauses_on_a_timed_gate() {
    use crate::test_support::timeout_gateway;
    let hooks = RecordingHooks::default();
    let gw = timeout_gateway().await;
    let req = support::build_request("c", &serde_json::json!({ "prompt": "warm" }));
    let _ = gw.execute(&req).await; // warm-up cools router "r"
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("n1".into()),
            kind: model_call("c", "go"),
            deps: vec![],
        }],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_hooks(Arc::new(hooks.clone()))
        .run(run, &graph)
        .await
        .expect("run yields an outcome");
    let pause = out.paused.expect("the all-gated node pauses");
    assert_eq!(pause.node, NodeId("n1".into()));
    assert!(
        out.failed.is_none(),
        "a timed gate pauses, does not fail: {:?}",
        out.failed
    );
    let events = journal.load(run).await.unwrap();
    assert!(
        events.iter().any(|(_, e)| matches!(
            e,
            JournalEvent::RunPaused {
                resume_after: Some(_),
                ..
            }
        )),
        "RunPaused with a timed resume_after is journaled"
    );
    assert!(
        !events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "a paused run does not complete"
    );
    assert!(
        !events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::NodeFailed { .. })),
        "a paused node is NOT also failed (RunPaused and NodeFailed are mutually exclusive)"
    );
    assert!(
        hooks.log().iter().any(|l| l.starts_with("run_paused(")),
        "on_run_paused fired: {:?}",
        hooks.log()
    );
}

/// Acceptance §6.4 — an Agent node whose turn is all-gated (timed) pauses (not fails).
#[tokio::test]
async fn agent_node_pauses_on_a_timed_gate() {
    use crate::test_support::timeout_gateway;
    let gw = timeout_gateway().await;
    let req = support::build_request("c", &serde_json::json!({ "prompt": "warm" }));
    let _ = gw.execute(&req).await; // warm-up cools router "r"
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "go")],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(ToolRegistry::default()))
        .run(run, &graph)
        .await
        .expect("run yields an outcome");
    assert!(
        out.paused.is_some(),
        "the agent's gated turn pauses: {:?}",
        out.failed
    );
    assert!(out.failed.is_none());
    let events = journal.load(run).await.unwrap();
    assert!(
        events.iter().any(|(_, e)| matches!(
            e,
            JournalEvent::RunPaused {
                resume_after: Some(_),
                ..
            }
        )),
        "the agent journals RunPaused with a timed resume_after"
    );
    assert!(
        !events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::NodeFailed { .. })),
        "a paused agent turn is NOT also failed"
    );
}

/// Acceptance §6.6 — a run paused on a timed gate RE-ATTEMPTS on resume: resuming
/// with a fresh, un-gated gateway (same journal) re-runs the node, which now
/// succeeds, and the run completes — no DeterminismViolation (the gated call
/// journaled no EffectRecorded, so there is nothing to replay/fence).
#[tokio::test]
async fn a_paused_gated_run_reattempts_and_completes_on_resume() {
    use crate::test_support::timeout_gateway;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("n1".into()),
            kind: model_call("c", "go"),
            deps: vec![],
        }],
    };
    // Pause: warm-up cools the sole router → the node is all-gated → paused.
    let gw = timeout_gateway().await;
    let req = support::build_request("c", &serde_json::json!({ "prompt": "warm" }));
    let _ = gw.execute(&req).await;
    let o1 = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .run(run, &graph)
        .await
        .expect("first run");
    assert!(o1.paused.is_some(), "first run pauses");
    assert!(
        !journal
            .load(run)
            .await
            .unwrap()
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "paused run is not complete"
    );
    // Resume on a fresh, un-gated gateway → n1 re-attempts, succeeds, completes.
    let (gw2, _c2) = recording_gateway().await;
    let o2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .start(run, &graph)
        .await
        .expect("resume");
    assert!(
        o2.failed.is_none() && o2.paused.is_none(),
        "resume completes: {:?} / {:?}",
        o2.failed,
        o2.paused
    );
    assert!(
        journal
            .load(run)
            .await
            .unwrap()
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "the resumed run completes"
    );
    assert_eq!(o2.outputs[&NodeId("n1".into())]["text"], "canned-response");
}

// ============================= SP-2 config-source =============================

/// SP-2 e2e — a registry loaded from a filesystem ConfigSource drives an agent
/// node end-to-end (disk config → Registry::from_config → with_registry → run).
#[tokio::test]
async fn agent_runs_from_a_filesystem_loaded_registry() {
    use orchestrator_core::{ConfigSource, Registry};
    use orchestrator_store::FilesystemConfigSource;
    // A temp config dir with one no-tool agent "a" on chain "c".
    let root = std::env::temp_dir().join(format!("sp2-e2e-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(root.join("agents")).unwrap();
    std::fs::write(
        root.join("agents").join("a.md"),
        "---\nname: a\narea: research\nkind: reasoning\nchain: c\n---\nBe helpful.\n",
    )
    .unwrap();
    let registry = Registry::from_config(
        FilesystemConfigSource::new(&root)
            .load()
            .await
            .expect("load"),
    )
    .expect("validate");

    let (gw, _c) = recording_gateway().await;
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "hi")],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(Arc::new(registry))
        .with_tools(Arc::new(ToolRegistry::default()))
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{:?}", out.failed);
    assert_eq!(out.outputs[&NodeId("n1".into())]["text"], "canned-response");
    std::fs::remove_dir_all(&root).ok();
}

#[tokio::test]
async fn granted_tool_permissions_are_inert_end_to_end() {
    use orchestrator_core::{NetworkPolicy, Permissions, RegistryConfig, ToolSpec};
    // Tool "calc" DECLARES a path+network need; agent GRANTS a covering scope.
    let tool = ToolSpec {
        name: "calc".into(),
        description: None,
        input_schema: serde_json::json!({}),
        effect_class: orchestrator_core::EffectClass::Pure,
        ttl_secs: None,
        source: None,
        permissions: Permissions {
            paths: vec!["/workspace".into()],
            network: NetworkPolicy::Any,
            ..Default::default()
        },
        activation: orchestrator_core::Activation::default(),
    };
    let mut agent = agent_def("c");
    agent.tools = vec!["calc".into()];
    agent.grants.insert(
        "calc".into(),
        Permissions {
            paths: vec!["/workspace".into()],
            network: NetworkPolicy::Any,
            ..Default::default()
        },
    );
    let cfg = RegistryConfig {
        agents: vec![agent],
        skills: vec![],
        tools: vec![tool],
        chain_bindings: vec![],
    };
    let registry =
        Arc::new(Registry::from_config(cfg).expect("assembles + validates (grant covers need)"));

    // The agent runs a normal turn; declarations don't gate anything (SP-4 does).
    let (gateway, _calls) = recording_gateway().await; // final response, no tool_calls
    let n1 = NodeId("n1".into());
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(registry);
    let outcome = exec
        .run(
            RunId(uuid::Uuid::new_v4()),
            &Graph {
                nodes: vec![agent_node("n1", "a", "hi")],
            },
        )
        .await
        .expect("run");
    assert!(
        outcome.failed.is_none(),
        "granted tool runs (declarations inert): {:?}",
        outcome.failed
    );
    assert!(outcome.outputs.contains_key(&n1));
}

#[tokio::test]
async fn granted_tool_executes_normally_declarations_dont_gate() {
    use orchestrator_core::{NetworkPolicy, Permissions, RegistryConfig, ToolSpec};
    // Core registry: a "calc" tool DECLARING a path+network need, and agent "a"
    // that lists it and GRANTS a covering scope. (The executable Calc in the
    // ToolRegistry ignores permissions — declarations are inert at execution.)
    let calc_spec = ToolSpec {
        name: "calc".into(),
        description: Some("arith".into()),
        input_schema: serde_json::json!({"type":"object"}),
        effect_class: orchestrator_core::EffectClass::Pure,
        ttl_secs: None,
        source: None,
        permissions: Permissions {
            paths: vec!["/workspace".into()],
            network: NetworkPolicy::Any,
            ..Default::default()
        },
        activation: orchestrator_core::Activation::default(),
    };
    let mut agent = agent_def("c");
    agent.tools = vec!["calc".into()];
    agent.grants.insert(
        "calc".into(),
        Permissions {
            paths: vec!["/workspace".into()],
            network: NetworkPolicy::Any,
            ..Default::default()
        },
    );
    let cfg = RegistryConfig {
        agents: vec![agent],
        skills: vec![],
        tools: vec![calc_spec],
        chain_bindings: vec![],
    };
    let registry = Arc::new(Registry::from_config(cfg).expect("grant covers need"));

    // The model calls calc, then finalizes — proving the granted tool RUNS.
    let (gateway, calls) = scripted_gateway(vec![
        tool_call_response("t1", "calc", "{\"op\":\"add\",\"a\":2,\"b\":3}"),
        final_response("the answer is 5"),
    ])
    .await;
    let n1 = NodeId("n1".into());
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(registry)
        .with_tools(calc_tools());
    let outcome = exec
        .run(
            RunId(uuid::Uuid::new_v4()),
            &Graph {
                nodes: vec![agent_node("n1", "a", "add 2 and 3")],
            },
        )
        .await
        .expect("run");
    assert!(
        outcome.failed.is_none(),
        "granted tool executes (declarations inert): {:?}",
        outcome.failed
    );
    assert_eq!(outcome.outputs[&n1]["text"], "the answer is 5");
    assert_eq!(
        calls.lock().unwrap().len(),
        2,
        "two model turns: the tool call + the final"
    );
}

/// SP-2 slice 4 e2e — activation shapes the ASSEMBLED PROMPT, not execution. The
/// echo gateway returns each agent's system prompt as the answer, so the presence
/// of a keyword-gated skill body in the output is a direct read of the composed
/// prompt: it appears when the input matches the keyword and is absent otherwise,
/// with BOTH runs completing (gating is progressive disclosure, never a failure).
#[tokio::test]
async fn activation_shapes_the_assembled_prompt_end_to_end() {
    use orchestrator_core::{Activation, SkillDef};
    // Agent "a" references a keyword-gated skill "gated" (body "GATED_BODY").
    let mut agent = agent_def("c");
    agent.skills = vec!["gated".into()];
    let registry = Arc::new(Registry::default().with_agent(agent).with_skill(SkillDef {
        name: "gated".into(),
        description: None,
        body: "GATED_BODY".into(),
        activation: Activation::OnKeywords(vec!["summarize".into()]),
    }));

    // The echo gateway returns the assembled SYSTEM prompt as the answer.
    let run_with = |input: &'static str| {
        let registry = registry.clone();
        async move {
            let (gateway, _calls) = echo_system_gateway().await;
            let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
                .with_registry(registry);
            let n1 = NodeId("n1".into());
            let outcome = exec
                .run(
                    RunId(uuid::Uuid::new_v4()),
                    &Graph {
                        nodes: vec![agent_node("n1", "a", input)],
                    },
                )
                .await
                .expect("run");
            assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
            outcome.outputs[&n1]["text"].as_str().unwrap().to_string()
        }
    };

    // Input hits the keyword → gated skill body is in the prompt.
    assert!(
        run_with("please summarize this")
            .await
            .contains("GATED_BODY")
    );
    // Input misses → gated skill body absent (but the run still completes).
    assert!(!run_with("hello there").await.contains("GATED_BODY"));
}

#[tokio::test]
async fn reload_bumps_the_run_version_and_fences_in_flight_resume() {
    use orchestrator_core::{RegistryConfig, RegistryHandle};
    use orchestrator_store::InMemoryConfigSource;
    let handle = RegistryHandle::new(Registry::default().with_agent(agent_def("c")));
    let journal = InMemoryJournal::new();
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry_handle(handle.clone());

    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "hi")],
    };
    exec.run(run, &graph).await.expect("run at gen 0");

    // The run recorded the pinned version "v1#cfg0".
    let recorded = journal
        .load(run)
        .await
        .unwrap()
        .into_iter()
        .find_map(|(_, e)| match e {
            JournalEvent::RunStarted { version } => Some(version),
            _ => None,
        })
        .unwrap();
    assert_eq!(recorded, "v1#cfg0");

    // Reload → gen 1. Resuming the gen-0 run on the (now gen-1) executor is fenced.
    handle
        .reload(&InMemoryConfigSource(RegistryConfig {
            agents: vec![agent_def("c")],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![],
        }))
        .await
        .unwrap();
    let err = exec
        .start(run, &graph)
        .await
        .expect_err("reload fences the in-flight resume");
    assert!(
        matches!(
            &err,
            OrchestratorError::VersionFenceMismatch { recorded, current }
                if recorded == "v1#cfg0" && current == "v1#cfg1"
        ),
        "got {err:?}"
    );
}

#[tokio::test]
async fn each_run_pins_the_generation_live_at_its_start() {
    use orchestrator_core::{RegistryConfig, RegistryHandle};
    use orchestrator_store::InMemoryConfigSource;
    let handle = RegistryHandle::new(Registry::default().with_agent(agent_def("c")));
    let journal = InMemoryJournal::new();
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry_handle(handle.clone());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "hi")],
    };

    let run_a = RunId(uuid::Uuid::new_v4());
    exec.run(run_a, &graph).await.expect("run A @ gen0");
    handle
        .reload(&InMemoryConfigSource(RegistryConfig {
            agents: vec![agent_def("c")],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![],
        }))
        .await
        .unwrap();
    let run_b = RunId(uuid::Uuid::new_v4());
    exec.run(run_b, &graph).await.expect("run B @ gen1");

    let version_of = |r: RunId| {
        let j = journal.clone();
        async move {
            j.load(r)
                .await
                .unwrap()
                .into_iter()
                .find_map(|(_, e)| match e {
                    JournalEvent::RunStarted { version } => Some(version),
                    _ => None,
                })
                .unwrap()
        }
    };
    assert_eq!(version_of(run_a).await, "v1#cfg0", "run A pinned gen 0");
    assert_eq!(
        version_of(run_b).await,
        "v1#cfg1",
        "run B pinned gen 1 (live at its start)"
    );
}

/// The positive twin of `reload_bumps_...`: WITHOUT a reload, a handle-wired
/// executor RESUMES a partial run at the same generation and finishes it — the
/// per-run pin records and re-compares the SAME `"v1#cfg0"`, so no false
/// `VersionFenceMismatch` fires. This is the one resume-through-handle path (a
/// successful resume, not a fenced one) the fence tests don't cover.
#[tokio::test]
async fn handle_wired_executor_resumes_a_partial_run_at_the_same_generation() {
    use orchestrator_core::RegistryHandle;
    let handle = RegistryHandle::new(Registry::default().with_agent(agent_def("c")));
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (graph, n1, n2) = two_node_graph("a", "b");

    // Run 1: n1 succeeds, n2 fails → a partial journal (no RunCompleted), pinned
    // at gen 0 (recorded version "v1#cfg0").
    let (gw1, _c1) = failing_after_gateway(1).await;
    let out1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_registry_handle(handle.clone())
        .run(run, &graph)
        .await
        .expect("run 1 yields an outcome");
    assert!(
        out1.failed.is_some(),
        "partial run: n2 fails, leaving n1 journaled without RunCompleted"
    );
    let recorded = journal
        .load(run)
        .await
        .unwrap()
        .into_iter()
        .find_map(|(_, e)| match e {
            JournalEvent::RunStarted { version } => Some(version),
            _ => None,
        })
        .unwrap();
    assert_eq!(recorded, "v1#cfg0", "run 1 pinned gen 0");

    // NO reload → generation stays 0. Run 2 resumes on a fresh recording gateway:
    // the pin re-compares "v1#cfg0" == "v1#cfg0" ⇒ no fence ⇒ the run completes.
    let (gw2, _c2) = recording_gateway().await;
    let out2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_registry_handle(handle.clone())
        .start(run, &graph)
        .await
        .expect("same-generation resume through the handle completes (no false fence)");
    assert!(
        out2.failed.is_none(),
        "same-gen resume through the handle completes: {:?}",
        out2.failed
    );
    assert_eq!(
        out2.completed,
        vec![n1, n2],
        "both nodes finish on the resume"
    );
}

#[tokio::test]
async fn a_reloaded_agent_becomes_runnable_end_to_end() {
    use orchestrator_core::{RegistryConfig, RegistryHandle};
    use orchestrator_store::InMemoryConfigSource;
    // Handle starts EMPTY — agent "a" does not exist yet.
    let handle = RegistryHandle::new(Registry::default());
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry_handle(handle.clone());
    let n1 = NodeId("n1".into());

    // Before reload: the run references unknown agent "a" → fails. An `Agent` node
    // against an empty registry surfaces `UnknownAgent` as a top-level run error
    // (it never resolves far enough to become an `outcome.failed`).
    let before = exec
        .run(
            RunId(uuid::Uuid::new_v4()),
            &Graph {
                nodes: vec![agent_node("n1", "a", "hi")],
            },
        )
        .await;
    assert!(
        matches!(&before, Err(OrchestratorError::UnknownAgent(a)) if a == "a"),
        "unknown agent fails before reload: {before:?}"
    );

    // Reload a config that defines agent "a" on chain "c".
    handle
        .reload(&InMemoryConfigSource(RegistryConfig {
            agents: vec![agent_def("c")],
            skills: vec![],
            tools: vec![],
            chain_bindings: vec![],
        }))
        .await
        .expect("reload");

    // After reload: a NEW run resolves and drives agent "a".
    let after = exec
        .run(
            RunId(uuid::Uuid::new_v4()),
            &Graph {
                nodes: vec![agent_node("n1", "a", "hi")],
            },
        )
        .await
        .expect("run");
    assert!(after.failed.is_none(), "reloaded agent runs: {after:?}");
    assert!(after.outputs.contains_key(&n1));
}

/// `start()` on a handle-wired executor with an EMPTY journal is a fresh run via
/// the pin (start → pin → start_inner → empty-journal branch → run_inner): it
/// completes AND stamps the pinned generation — the last handle-path corner.
#[tokio::test]
async fn start_on_a_handle_wired_executor_freshly_runs_and_pins_the_generation() {
    use orchestrator_core::RegistryHandle;
    let handle = RegistryHandle::new(Registry::default().with_agent(agent_def("c")));
    let journal = InMemoryJournal::new();
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry_handle(handle.clone());
    let n1 = NodeId("n1".into());
    let run = RunId(uuid::Uuid::new_v4());

    // No journal yet ⇒ start() delegates to the fresh-run path on the pinned clone.
    let out = exec
        .start(
            run,
            &Graph {
                nodes: vec![agent_node("n1", "a", "hi")],
            },
        )
        .await
        .expect("fresh start via handle");
    assert!(out.failed.is_none(), "{out:?}");
    assert!(out.outputs.contains_key(&n1));

    let recorded = journal
        .load(run)
        .await
        .unwrap()
        .into_iter()
        .find_map(|(_, e)| match e {
            JournalEvent::RunStarted { version } => Some(version),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        recorded, "v1#cfg0",
        "start's empty-journal path pins the generation"
    );
}

// ---------------------------------------------------------------------------
// SP-3 slice 1 — `NodeKind::Subgraph`: a node whose work is a whole nested DAG,
// driven under the node's path in the SAME run.
// ---------------------------------------------------------------------------

fn mc(id: &str, dep: Option<&str>) -> Node {
    Node {
        id: NodeId(id.into()),
        kind: NodeKind::ModelCall {
            chain: "c".into(),
            payload: serde_json::json!({ "prompt": id }),
        },
        deps: dep
            .map(|d| {
                vec![Dep {
                    on: NodeId(d.into()),
                    kind: EdgeKind::Hard,
                }]
            })
            .unwrap_or_default(),
    }
}
fn mc_dep(id: &str, dep: Dep) -> Node {
    Node {
        id: NodeId(id.into()),
        kind: NodeKind::ModelCall {
            chain: "c".into(),
            payload: serde_json::json!({ "prompt": id }),
        },
        deps: vec![dep],
    }
}
fn subgraph_node(id: &str, inner: Vec<Node>) -> Node {
    Node {
        id: NodeId(id.into()),
        kind: NodeKind::Subgraph {
            graph: Box::new(Graph { nodes: inner }),
        },
        deps: vec![],
    }
}

#[tokio::test]
async fn subgraph_executes_a_nested_line_and_returns_the_sink_map() {
    let (gateway, _c) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let s = NodeId("s".into());
    let graph = Graph {
        nodes: vec![subgraph_node(
            "s",
            vec![mc("n1", None), mc("n2", Some("n1"))],
        )],
    };
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    let sub_out = &out.outputs[&s];
    assert!(sub_out.get("n2").is_some(), "sink map has n2: {sub_out}");
    assert!(sub_out.get("n1").is_none(), "n1 is not a sink");
}

#[tokio::test]
async fn subgraph_diamond_returns_all_sink_outputs() {
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let s = NodeId("s".into());
    let inner = vec![mc("a", None), mc("b", Some("a")), mc("c", Some("a"))];
    let graph = Graph {
        nodes: vec![subgraph_node("s", inner)],
    };
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    let sub = &out.outputs[&s];
    assert!(
        sub.get("b").is_some() && sub.get("c").is_some(),
        "both sinks present: {sub}"
    );
    assert!(sub.get("a").is_none(), "a is not a sink");
}

/// Proves a subgraph's inner nodes replay from the memo across the subgraph
/// boundary on resume (no re-spend): run 1 dies partway *inside* the subgraph
/// (inner `n1` completes, inner `n2` fails), so on resume the already-completed
/// inner node replays from the memo — the resume gateway is called only for the
/// node that actually failed. The complementary shape — a failing outer tail that
/// hard-deps a *completed* subgraph — is covered by
/// `a_run_with_a_completed_subgraph_and_a_failing_tail_resumes_correctly`.
#[tokio::test]
async fn subgraph_inner_nodes_replay_from_memo_on_resume() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let s = NodeId("s".into());
    let graph = Graph {
        nodes: vec![subgraph_node(
            "s",
            vec![mc("n1", None), mc("n2", Some("n1"))],
        )],
    };

    // Run 1: adapter succeeds on its 1st call (inner n1) and errors on its 2nd
    // (inner n2). n1 is journaled+completed; n2 fails; the subgraph fails; NO
    // RunCompleted is written (the nested drive did not complete).
    let (gw1, calls1) = failing_after_gateway(1).await;
    let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1");
    let outcome1 = exec1
        .run(run, &graph)
        .await
        .expect("run 1 yields an outcome");
    assert!(
        outcome1.failed.is_some(),
        "run 1 fails inside the subgraph: {outcome1:?}"
    );
    assert_eq!(
        calls1.lock().unwrap().len(),
        2,
        "run 1 hit the gateway for inner n1 and the failing inner n2"
    );

    // Run 2: a FRESH always-succeeding gateway over the SAME journal. Resume folds
    // the journal, memoizes inner n1, and re-drives only the failed inner n2.
    let (gw2, calls2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1");
    let outcome2 = exec2.start(run, &graph).await.expect("resume completes");
    assert!(
        outcome2.failed.is_none(),
        "resume completes with no failure: {:?}",
        outcome2.failed
    );
    assert!(
        outcome2.outputs[&s].get("n2").is_some(),
        "resumed subgraph sink map has n2: {}",
        outcome2.outputs[&s]
    );

    // The proof: run-2's gateway saw EXACTLY ONE call, carrying inner n2's prompt.
    // Inner n1 was replayed from the memo — not re-spent.
    let recorded2 = calls2.lock().unwrap().clone();
    assert_eq!(
        recorded2.len(),
        1,
        "resume re-called the gateway only for the failed inner node n2: {recorded2:?}"
    );
    assert_eq!(
        recorded2[0].1, "n2",
        "the single resume call carried n2's prompt"
    );
}

/// Fix A regression: a completed `Subgraph` drives its nested DAG through the SAME
/// run's `drive`, which must NOT append `RunCompleted` — that is a run-level event.
/// Here run 1 completes the subgraph then fails at the tail node `d`; the journal
/// must carry NO premature `RunCompleted`, and a resume (where `d` succeeds) must
/// finish the run. Before Fix A, the nested drive emitted a `RunCompleted` for the
/// whole run, so `start` treated it as terminal and never resumed `d`.
#[tokio::test]
async fn a_run_with_a_completed_subgraph_and_a_failing_tail_resumes_correctly() {
    // Outer: subgraph "s" (nested n1) → node "d" (Hard-dep s). Run 1 fails at "d";
    // the subgraph's completion must NOT prematurely mark the whole run complete.
    let graph = Graph {
        nodes: vec![
            subgraph_node("s", vec![mc("n1", None)]),
            Node {
                id: NodeId("d".into()),
                kind: NodeKind::ModelCall {
                    chain: "c".into(),
                    payload: serde_json::json!("d"),
                },
                deps: vec![Dep {
                    on: NodeId("s".into()),
                    kind: EdgeKind::Hard,
                }],
            },
        ],
    };
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    // Run 1: gateway succeeds for the subgraph's inner node (call 1) then fails at
    // "d" (call 2).
    {
        let (gw, _c) = failing_after_gateway(1).await;
        let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1");
        let o1 = exec.run(run, &graph).await.expect("run1");
        assert!(o1.failed.is_some(), "tail failed: {o1:?}");
    }
    // The journal must have NO RunCompleted yet (the subgraph's completion must not
    // have emitted one for the whole run).
    let rc = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .filter(|(_, e)| matches!(e, JournalEvent::RunCompleted))
        .count();
    assert_eq!(rc, 0, "no premature RunCompleted from the subgraph");
    // Resume on a gateway where "d" succeeds → the run completes (tail re-driven).
    {
        let (gw, _c) = recording_gateway().await;
        let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1");
        let o2 = exec.start(run, &graph).await.expect("resume");
        assert!(o2.failed.is_none(), "resume completes the tail: {o2:?}");
    }
}

/// Fix B regression: a sibling top-level `ModelCall` (`m`, ready index 0) and a
/// subgraph's inner `ModelCall` (`s/n1`, the nested drive's ready index 0) must
/// record under DISTINCT effect ids. With the old empty-prefix, index-based scheme
/// both keyed `effect_id("", 0, 0)` — a collision that poisons the resume memo.
/// Node-id-scoped ids (`effect_id(&node.id.0, 0, 0)`) keep them apart. Asserting
/// the recorded effect ids directly makes this load-bearing: reverting Fix B makes
/// the two ids equal and trips the `assert_ne!`.
#[tokio::test]
async fn a_sibling_modelcall_and_a_subgraph_inner_modelcall_do_not_share_an_effect_id() {
    let (gateway, _c) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let m = NodeId("m".into());
    let s = NodeId("s".into());
    let graph = Graph {
        nodes: vec![
            Node {
                id: m.clone(),
                kind: NodeKind::ModelCall {
                    chain: "c".into(),
                    payload: serde_json::json!("M"),
                },
                deps: vec![],
            },
            subgraph_node("s", vec![mc("n1", None)]),
        ],
    };
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let run = RunId(uuid::Uuid::new_v4());
    let out = exec.run(run, &graph).await.expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    assert!(
        out.outputs.contains_key(&m),
        "outer ModelCall produced output"
    );
    assert!(
        out.outputs[&s].get("n1").is_some(),
        "subgraph inner ModelCall produced its own output"
    );

    // The two recorded ModelCall effects must have DISTINCT ids (no collision).
    let eids: Vec<EffectId> = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .filter_map(|(_, e)| match e {
            JournalEvent::EffectRecorded { effect_id, .. } => Some(effect_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(eids.len(), 2, "two ModelCall effects recorded: {eids:?}");
    assert_ne!(
        eids[0], eids[1],
        "a sibling and a nested ModelCall must not share an effect id"
    );
}

#[tokio::test]
async fn subgraph_nesting_beyond_max_depth_halts_loud() {
    let (gateway, _c) = recording_gateway().await;
    // A subgraph containing a subgraph (2 levels of nesting).
    let inner = subgraph_node("inner", vec![mc("x", None)]);
    let graph = Graph {
        nodes: vec![subgraph_node("outer", vec![inner])],
    };
    // max_depth = 1 allows one subgraph level; the second is refused loud.
    let exec =
        Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1").with_max_depth(1);
    let res = exec.run(RunId(uuid::Uuid::new_v4()), &graph).await;
    // The inner subgraph's GlobalCapExceeded surfaces either as the outer subgraph's
    // failure OR as a top-level Err — assert the "max_depth" message appears either way.
    let msg = match &res {
        Ok(o) => o
            .failed
            .as_ref()
            .map(|(_, m)| m.clone())
            .unwrap_or_default(),
        Err(e) => format!("{e:?}"),
    };
    assert!(msg.contains("max_depth"), "cap halts loud: {res:?}");

    // With the default (8), the same 2-level graph runs fine.
    let (gateway2, _c2) = recording_gateway().await;
    let ok = Executor::new(Arc::new(gateway2), Arc::new(InMemoryJournal::new()), "v1")
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("runs within default depth");
    assert!(ok.failed.is_none(), "{ok:?}");
}

/// Failure propagates OUT of a subgraph: a nested node that fails makes the whole
/// `Subgraph` node `Failed`, which then cascade-skips the node's HARD dependents in
/// the OUTER graph while leaving a SOFT dependent runnable (soft edges never
/// cascade). Proves `run_subgraph`'s Failed mapping is wired to the outer scheduler.
#[tokio::test]
async fn a_failing_nested_node_fails_the_subgraph_and_cascades_hard_dependents() {
    let (gateway, _c) = failing_after_gateway(0).await; // 0 successes ⇒ fails immediately
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let graph = Graph {
        nodes: vec![
            subgraph_node("s", vec![mc("n1", None)]),
            Node {
                id: NodeId("d".into()),
                kind: NodeKind::ModelCall {
                    chain: "c".into(),
                    payload: serde_json::json!(0),
                },
                deps: vec![Dep {
                    on: NodeId("s".into()),
                    kind: EdgeKind::Hard,
                }],
            },
            Node {
                id: NodeId("e".into()),
                kind: NodeKind::ModelCall {
                    chain: "c".into(),
                    payload: serde_json::json!(0),
                },
                deps: vec![Dep {
                    on: NodeId("s".into()),
                    kind: EdgeKind::Soft,
                }],
            },
        ],
    };
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("outcome");
    assert!(out.failed.is_some(), "the subgraph failed: {out:?}");
    assert!(
        out.skipped.contains(&NodeId("d".into())),
        "hard dependent cascade-skipped: {out:?}"
    );
    // The soft dependent is NEVER cascade-skipped — it stays runnable even though
    // "s" failed (here it then fails too against the always-failing gateway, but it
    // is emphatically not in `skipped`).
    assert!(
        !out.skipped.contains(&NodeId("e".into())),
        "soft dependent is not cascade-skipped: {out:?}"
    );
}

/// Pause propagates OUT of a subgraph: an in-doubt Mutation inside a NESTED agent
/// pauses that agent (`RunPaused` journaled), `run_subgraph` maps the nested
/// `RunOutcome.paused` → `NodeExec::Paused`, and the outer scheduler pauses the whole
/// run — it must NEVER journal `RunCompleted` over the unresolved Intent. This is the
/// `in_doubt_mutation_in_a_map_child_pauses_the_whole_run` shape with the mutation-
/// bearing agent wrapped in a `Subgraph` instead of a `Map`.
#[tokio::test]
async fn an_in_doubt_mutation_in_a_subgraph_pauses_the_run() {
    let run = RunId(uuid::Uuid::new_v4());
    let mk_recorder = |sink: Arc<std::sync::Mutex<Vec<String>>>| {
        let recorder = AgentDefinition {
            name: "recorder".into(),
            area: "research".into(),
            kind: "reasoning".into(),
            chain: Some("research.bulk".into()),
            chains: std::collections::HashMap::new(),
            grants: std::collections::HashMap::new(),
            tools: vec!["record_note".into()],
            skills: vec![],
            system_prompt: "Record.".into(),
        };
        (
            Arc::new(
                Registry::default()
                    .with_agent(recorder)
                    .with_tool(RecordNote::new(sink.clone()).spec()),
            ),
            Arc::new(ToolRegistry::default().with_tool(Arc::new(RecordNote::new(sink)))),
        )
    };
    // Same harness as the Map-child test, but the mutation-bearing agent lives inside
    // a Subgraph "s" (inner node "s/n1") rather than a Map.
    let subgraph = Graph {
        nodes: vec![subgraph_node(
            "s",
            vec![agent_node("n1", "recorder", "item-0")],
        )],
    };

    // Seed: run the subgraph to completion, then truncate to the nested agent's
    // record_note EffectIntent (drops its EffectRecorded) → in-doubt on resume.
    let full = InMemoryJournal::new();
    let (seed_reg, seed_tools) = mk_recorder(Arc::new(std::sync::Mutex::new(Vec::new())));
    let (gw_s, _c) = demo_reference_tool_gateway().await;
    Executor::new(Arc::new(gw_s), Arc::new(full.clone()), "v1")
        .with_registry(seed_reg)
        .with_tools(seed_tools)
        .run(run, &subgraph)
        .await
        .expect("seed Subgraph run completes");
    let events = full.load(run).await.unwrap();
    let cut = events
        .iter()
        .position(|(_, e)| matches!(e, JournalEvent::EffectIntent { .. }))
        .expect("the nested agent journaled a record_note EffectIntent");
    let seeded = InMemoryJournal::new();
    for (_, e) in &events[..=cut] {
        seeded.append(run, e.clone()).await.unwrap();
    }

    // Resume with an Indeterminate reconciler + a FRESH empty sink → the nested
    // Mutation is in-doubt → the nested agent pauses → the subgraph pauses → the run
    // pauses.
    let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let (reg, tools) = mk_recorder(sink.clone());
    let reconcilers =
        ReconcileRegistry::default().with_provider("record_note", Arc::new(AlwaysIndeterminate));
    let (gw_r, _c2) = demo_reference_tool_gateway().await;
    let outcome = Executor::new(Arc::new(gw_r), Arc::new(seeded.clone()), "v1")
        .with_registry(reg)
        .with_tools(tools)
        .with_reconcilers(Arc::new(reconcilers))
        .start(run, &subgraph)
        .await
        .expect("resume yields an outcome");

    let pause = outcome
        .paused
        .expect("the in-doubt nested Mutation pauses the whole run");
    assert_eq!(
        pause.node,
        NodeId("s".into()),
        "the Subgraph node is the pause point"
    );
    let resumed = seeded.load(run).await.unwrap();
    assert!(
        resumed
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunPaused { .. })),
        "RunPaused is journaled"
    );
    assert!(
        !resumed
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "the run must NOT complete over an unresolved in-doubt Intent (no silent failure)"
    );
    assert!(
        sink.lock().unwrap().is_empty(),
        "a paused in-doubt Mutation applies no side effect"
    );
}

/// Terminal-resume output shape (a code-review follow-up): re-`start`ing an
/// ALREADY-COMPLETED subgraph run returns the folded outcome WITHOUT re-driving.
/// This documents a known fresh-vs-terminal ASYMMETRY (shared with Map/Loop
/// synthesized outputs): a fresh `run` returns the subgraph's synthesized sink map
/// under "s", but the terminal fold reconstructs `outputs` from the journal's
/// per-node `EffectRecorded` — which for a subgraph are the NAMESPACED inner nodes
/// ("s/n1"), never the synthesized "s" sink map. Captured here, not fixed in slice 1.
#[tokio::test]
async fn re_starting_a_completed_subgraph_run_returns_the_folded_outcome() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![subgraph_node("s", vec![mc("n1", None)])],
    };
    // Fresh run completes: "s" carries the synthesized sink map {n1: <output>}.
    {
        let (gw, _c) = recording_gateway().await;
        let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1");
        let o1 = exec.run(run, &graph).await.expect("run1");
        assert!(o1.failed.is_none());
        assert!(
            o1.outputs[&NodeId("s".into())].get("n1").is_some(),
            "fresh run: sink map under s"
        );
    }
    // Re-start the already-terminal run: returns the folded outcome without re-driving.
    {
        let (gw, _c) = recording_gateway().await;
        let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1");
        let o2 = exec.start(run, &graph).await.expect("terminal replay");
        assert!(o2.failed.is_none(), "terminal replay succeeds: {o2:?}");
        // The REAL terminal-replay shape: the synthesized "s" sink map is ABSENT
        // (it is never journaled), and the namespaced inner output "s/n1" is present
        // instead. KNOWN LIMITATION — the fresh-vs-terminal asymmetry (Map/Loop share
        // it); documented, not fixed, in this slice.
        assert!(
            !o2.outputs.contains_key(&NodeId("s".into())),
            "terminal replay: the synthesized sink map under s is absent (known asymmetry): {:?}",
            o2.outputs
        );
        assert!(
            o2.outputs.contains_key(&NodeId("s/n1".into())),
            "terminal replay: the namespaced inner node output is present instead: {:?}",
            o2.outputs
        );
    }
}

/// End-to-end: a `Subgraph` drives a nested `Agent` node through the real gateway,
/// and the agent's output is the subgraph's sink (`{n1: <agent output>}`).
#[tokio::test]
async fn subgraph_drives_a_nested_agent_end_to_end() {
    let (gateway, _c) = recording_gateway().await;
    let registry = agent_registry("c");
    let s = NodeId("s".into());
    let graph = Graph {
        nodes: vec![subgraph_node("s", vec![agent_node("n1", "a", "hi")])],
    };
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(registry);
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    assert!(
        out.outputs[&s].get("n1").is_some(),
        "nested agent output is the subgraph sink: {}",
        out.outputs[&s]
    );
}

/// Edge case (whole-slice review): a `Subgraph` wrapping an EMPTY DAG validates and
/// completes trivially with an empty sink map `{}` (no nodes ⇒ no sinks). Documents
/// the behavior so a future "reject empty subgraph" decision is a conscious change.
#[tokio::test]
async fn an_empty_subgraph_completes_with_an_empty_sink_map() {
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let s = NodeId("s".into());
    let graph = Graph {
        nodes: vec![subgraph_node("s", vec![])],
    };
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    assert_eq!(
        out.outputs[&s],
        serde_json::json!({}),
        "empty subgraph → empty sink map"
    );
}

// ---------------------------------------------------------------------------
// SP-3 slice 2 — `NodeKind::Branch`: a deterministic conditional that tests a
// predecessor's output and drives the first matching arm (else `default`) as a
// nested graph under `"{branch}/{label}/…"`.
// ---------------------------------------------------------------------------

fn arm(inner_id: &str) -> Graph {
    Graph {
        nodes: vec![mc(inner_id, None)],
    }
}
fn branch_graph(arms: Vec<(orchestrator_core::BranchCond, Graph)>, default: Graph) -> Graph {
    Graph {
        nodes: vec![
            mc("on", None),
            Node {
                id: NodeId("br".into()),
                kind: NodeKind::Branch {
                    on: NodeId("on".into()),
                    arms,
                    default,
                },
                deps: vec![Dep {
                    on: NodeId("on".into()),
                    kind: EdgeKind::Hard,
                }],
            },
        ],
    }
}

#[tokio::test]
async fn branch_selects_first_matching_arm() {
    use orchestrator_core::BranchCond;
    let (gateway, _c) = recording_gateway().await; // `on` output = {"text":"canned-response"}
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let br = NodeId("br".into());
    let graph = branch_graph(
        vec![
            (BranchCond::TextContains("zzz-nope".into()), arm("armA_out")),
            (BranchCond::TextContains("canned".into()), arm("armB_out")),
        ],
        arm("armDefault_out"),
    );
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    let b = &out.outputs[&br];
    assert!(b.get("armB_out").is_some(), "arm 1 (first match) ran: {b}");
    assert!(
        b.get("armA_out").is_none() && b.get("armDefault_out").is_none(),
        "others didn't: {b}"
    );
}

#[tokio::test]
async fn branch_earlier_matching_arm_wins_over_later() {
    use orchestrator_core::BranchCond;
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let br = NodeId("br".into());
    let graph = branch_graph(
        vec![
            (BranchCond::TextContains("canned".into()), arm("armA_out")),
            (BranchCond::TextContains("response".into()), arm("armB_out")),
        ],
        arm("armDefault_out"),
    );
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    let b = &out.outputs[&br];
    assert!(
        b.get("armA_out").is_some() && b.get("armB_out").is_none(),
        "earlier arm wins: {b}"
    );
}

#[tokio::test]
async fn branch_runs_default_when_no_arm_matches() {
    use orchestrator_core::BranchCond;
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let br = NodeId("br".into());
    let graph = branch_graph(
        vec![(BranchCond::TextContains("zzz".into()), arm("armA_out"))],
        arm("armDefault_out"),
    );
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    let b = &out.outputs[&br];
    assert!(
        b.get("armDefault_out").is_some() && b.get("armA_out").is_none(),
        "default ran: {b}"
    );
}

#[tokio::test]
async fn branch_journals_only_the_selected_arm() {
    use orchestrator_core::BranchCond;
    let (gateway, _c) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let run = RunId(uuid::Uuid::new_v4());
    let graph = branch_graph(
        vec![
            (BranchCond::TextContains("zzz".into()), arm("armA_out")),
            (BranchCond::TextContains("canned".into()), arm("armB_out")),
        ],
        arm("armDefault_out"),
    );
    exec.run(run, &graph).await.expect("run");
    let labels: Vec<String> = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .filter_map(|(_, e)| match e {
            JournalEvent::NodeStarted { node } => Some(node.0.clone()),
            _ => None,
        })
        .collect();
    assert!(
        labels.iter().any(|l| l == "br/1/armB_out"),
        "selected arm journaled: {labels:?}"
    );
    assert!(
        !labels
            .iter()
            .any(|l| l.contains("armA_out") || l.contains("armDefault_out")),
        "unselected arms not journaled: {labels:?}"
    );
}

/// Determinism/resume: a run whose Branch selected an arm, then a downstream OUTER
/// node (`d`) fails, resumes by recomputing the SAME arm — the decision is pure over
/// `on`'s memoized output — and replays the arm's inner node from the memo (no
/// re-spend). NO branch-decision event is journaled (the Branch node itself never
/// appends a `NodeStarted`/`NodeCompleted`; only the selected arm's namespaced inner
/// nodes are). Modeled on `subgraph_inner_nodes_replay_from_memo_on_resume` /
/// `a_run_with_a_completed_subgraph_and_a_failing_tail_resumes_correctly`.
#[tokio::test]
async fn branch_replays_the_same_arm_on_resume_without_respend() {
    use orchestrator_core::BranchCond;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let br = NodeId("br".into());
    // Outer: on → br(Branch selecting arm 0 = "armB_out") → d (Hard-dep br).
    let graph = Graph {
        nodes: vec![
            mc("on", None),
            Node {
                id: br.clone(),
                kind: NodeKind::Branch {
                    on: NodeId("on".into()),
                    arms: vec![(BranchCond::TextContains("canned".into()), arm("armB_out"))],
                    default: arm("armDefault_out"),
                },
                deps: vec![Dep {
                    on: NodeId("on".into()),
                    kind: EdgeKind::Hard,
                }],
            },
            mc("d", Some("br")),
        ],
    };

    // Run 1: succeed through `on` (gateway call 1) and the selected arm's inner
    // ModelCall "br/0/armB_out" (call 2), then FAIL at the outer tail "d" (call 3).
    let (gw1, _c1) = failing_after_gateway(2).await;
    let out1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .run(run, &graph)
        .await
        .expect("run 1 yields an outcome");
    assert!(out1.failed.is_some(), "run 1 fails at the tail d: {out1:?}");
    let rc = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .filter(|(_, e)| matches!(e, JournalEvent::RunCompleted))
        .count();
    assert_eq!(rc, 0, "no RunCompleted after a failed tail (partial run)");

    // Resume on a FRESH always-succeeding gateway over the SAME journal. The Branch
    // recomputes the SAME arm from `on`'s memoized output and replays the arm's inner
    // node from the memo; only the failed tail `d` is re-driven.
    let (gw2, calls2) = recording_gateway().await;
    let out2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .start(run, &graph)
        .await
        .expect("resume completes");
    assert!(out2.failed.is_none(), "resume completes: {:?}", out2.failed);

    // The proof: resume's gateway saw EXACTLY ONE call, carrying `d`'s prompt — the
    // arm's inner ModelCall "br/0/armB_out" was replayed from the memo, not re-spent.
    let recorded2 = calls2.lock().unwrap().clone();
    assert_eq!(
        recorded2.len(),
        1,
        "resume re-called the gateway only for the failed tail d: {recorded2:?}"
    );
    assert_eq!(
        recorded2[0].1, "d",
        "the single resume call carried d's prompt"
    );
    assert!(
        !recorded2.iter().any(|(_, p)| p == "armB_out"),
        "the branch arm's inner node was NOT re-driven on resume: {recorded2:?}"
    );

    // No branch-decision event is journaled: the Branch node "br" itself never
    // appends a NodeStarted (only its namespaced arm nodes like "br/0/armB_out" do).
    let events = journal.load(run).await.unwrap();
    assert!(
        !events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::NodeStarted { node } if node.0 == "br")),
        "the Branch node journals no decision event of its own"
    );
}

/// A failed `on` cascade-skips the Branch: the Branch's HARD dep on `on` means a
/// failed `on` skips "br" before it ever decides (no arm runs).
#[tokio::test]
async fn a_failed_on_cascade_skips_the_branch() {
    use orchestrator_core::BranchCond;
    let (gateway, _c) = failing_after_gateway(0).await; // `on` fails immediately
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let graph = branch_graph(
        vec![(BranchCond::TextContains("x".into()), arm("armA_out"))],
        arm("armDefault_out"),
    );
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("outcome");
    assert!(out.failed.is_some(), "on failed: {out:?}");
    assert!(
        out.skipped.contains(&NodeId("br".into())),
        "branch cascade-skipped (never decided): {out:?}"
    );
}

/// Arm failure propagates OUT of a Branch: a failing node inside the SELECTED arm
/// makes the Branch node `Failed`, which cascade-skips the Branch's outer
/// Hard-dependent ("dret"). Here `on` succeeds (recording_gateway → "canned-response"
/// selects arm 0), but that arm's inner ModelCall targets an UNKNOWN chain, so it
/// fails cleanly at the gateway (`NoCandidates`).
#[tokio::test]
async fn a_failing_node_in_the_selected_arm_fails_the_branch() {
    use orchestrator_core::BranchCond;
    let (gateway, _c) = recording_gateway().await; // `on` → {"text":"canned-response"}
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let arm_fail = Graph {
        nodes: vec![Node {
            id: NodeId("armfail".into()),
            kind: NodeKind::ModelCall {
                chain: "nonexistent".into(),
                payload: serde_json::json!(0),
            },
            deps: vec![],
        }],
    };
    let graph = Graph {
        nodes: vec![
            mc("on", None),
            Node {
                id: NodeId("br".into()),
                kind: NodeKind::Branch {
                    on: NodeId("on".into()),
                    arms: vec![(BranchCond::TextContains("canned".into()), arm_fail)],
                    default: arm("armDefault_out"),
                },
                deps: vec![Dep {
                    on: NodeId("on".into()),
                    kind: EdgeKind::Hard,
                }],
            },
            Node {
                id: NodeId("dret".into()),
                kind: NodeKind::ModelCall {
                    chain: "c".into(),
                    payload: serde_json::json!(0),
                },
                deps: vec![Dep {
                    on: NodeId("br".into()),
                    kind: EdgeKind::Hard,
                }],
            },
        ],
    };
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("outcome");
    assert!(
        out.failed.is_some(),
        "the selected arm's failure fails the Branch: {out:?}"
    );
    assert!(
        out.skipped.contains(&NodeId("dret".into())),
        "the outer Hard-dependent of a failed Branch cascade-skips: {out:?}"
    );
}

/// Pause propagates OUT of a Branch: an in-doubt Mutation inside the SELECTED arm's
/// nested agent pauses that agent, `run_branch` maps the nested `RunOutcome.paused` →
/// `NodeExec::Paused`, and the outer scheduler pauses the whole run — never journaling
/// `RunCompleted` over the unresolved Intent. This is the
/// `an_in_doubt_mutation_in_a_subgraph_pauses_the_run` shape with the mutation-bearing
/// agent wrapped in a Branch's selected arm instead of a Subgraph. NOTE: the Branch's
/// `on` is a plain `ModelCall` on the demo `research.bulk` chain (the chain the
/// `ToolEmittingOllamaAdapter` serves) — the `mc()` helper's chain "c" does not exist
/// in the demo catalog — so `on` runs on the SAME gateway as the agent; its
/// "synthesized locally" answer selects the agent arm via `TextContains`.
#[tokio::test]
async fn an_in_doubt_mutation_in_a_branch_arm_pauses_the_run() {
    use orchestrator_core::BranchCond;
    let run = RunId(uuid::Uuid::new_v4());
    let mk_recorder = |sink: Arc<std::sync::Mutex<Vec<String>>>| {
        let recorder = AgentDefinition {
            name: "recorder".into(),
            area: "research".into(),
            kind: "reasoning".into(),
            chain: Some("research.bulk".into()),
            chains: std::collections::HashMap::new(),
            grants: std::collections::HashMap::new(),
            tools: vec!["record_note".into()],
            skills: vec![],
            system_prompt: "Record.".into(),
        };
        (
            Arc::new(
                Registry::default()
                    .with_agent(recorder)
                    .with_tool(RecordNote::new(sink.clone()).spec()),
            ),
            Arc::new(ToolRegistry::default().with_tool(Arc::new(RecordNote::new(sink)))),
        )
    };
    // The mutation-bearing agent lives inside the Branch's SELECTED arm ("br/0/rec").
    let branch = Graph {
        nodes: vec![
            Node {
                id: NodeId("on".into()),
                kind: NodeKind::ModelCall {
                    chain: "research.bulk".into(),
                    payload: serde_json::json!("decide"),
                },
                deps: vec![],
            },
            Node {
                id: NodeId("br".into()),
                kind: NodeKind::Branch {
                    on: NodeId("on".into()),
                    arms: vec![(
                        BranchCond::TextContains("synthesized".into()),
                        Graph {
                            nodes: vec![agent_node("rec", "recorder", "item-0")],
                        },
                    )],
                    default: arm("armDefault_out"),
                },
                deps: vec![Dep {
                    on: NodeId("on".into()),
                    kind: EdgeKind::Hard,
                }],
            },
        ],
    };

    // Seed: run the Branch to completion, then truncate to the nested agent's
    // record_note EffectIntent (drops its EffectRecorded) → in-doubt on resume.
    let full = InMemoryJournal::new();
    let (seed_reg, seed_tools) = mk_recorder(Arc::new(std::sync::Mutex::new(Vec::new())));
    let (gw_s, _c) = demo_reference_tool_gateway().await;
    Executor::new(Arc::new(gw_s), Arc::new(full.clone()), "v1")
        .with_registry(seed_reg)
        .with_tools(seed_tools)
        .run(run, &branch)
        .await
        .expect("seed Branch run completes");
    let events = full.load(run).await.unwrap();
    let cut = events
        .iter()
        .position(|(_, e)| matches!(e, JournalEvent::EffectIntent { .. }))
        .expect("the nested agent journaled a record_note EffectIntent");
    let seeded = InMemoryJournal::new();
    for (_, e) in &events[..=cut] {
        seeded.append(run, e.clone()).await.unwrap();
    }

    // Resume with an Indeterminate reconciler + a FRESH empty sink → the nested
    // Mutation is in-doubt → the nested agent pauses → the Branch pauses → the run pauses.
    let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let (reg, tools) = mk_recorder(sink.clone());
    let reconcilers =
        ReconcileRegistry::default().with_provider("record_note", Arc::new(AlwaysIndeterminate));
    let (gw_r, _c2) = demo_reference_tool_gateway().await;
    let outcome = Executor::new(Arc::new(gw_r), Arc::new(seeded.clone()), "v1")
        .with_registry(reg)
        .with_tools(tools)
        .with_reconcilers(Arc::new(reconcilers))
        .start(run, &branch)
        .await
        .expect("resume yields an outcome");

    let pause = outcome
        .paused
        .expect("the in-doubt nested Mutation pauses the whole run");
    assert_eq!(
        pause.node,
        NodeId("br".into()),
        "the Branch node is the pause point"
    );
    let resumed = seeded.load(run).await.unwrap();
    assert!(
        resumed
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunPaused { .. })),
        "RunPaused is journaled"
    );
    assert!(
        !resumed
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "the run must NOT complete over an unresolved in-doubt Intent (no silent failure)"
    );
    assert!(
        sink.lock().unwrap().is_empty(),
        "a paused in-doubt Mutation applies no side effect"
    );
}

/// End-to-end: a `Branch` drives a nested `Agent` arm through the real gateway; the
/// agent's output is the selected arm's sink (`{agent_out: <agent output>}`).
#[tokio::test]
async fn branch_drives_a_nested_agent_arm_end_to_end() {
    use orchestrator_core::BranchCond;
    let (gateway, _c) = recording_gateway().await;
    let registry = agent_registry("c");
    let br = NodeId("br".into());
    let agent_arm = Graph {
        nodes: vec![agent_node("agent_out", "a", "hi")],
    };
    let graph = Graph {
        nodes: vec![
            mc("on", None),
            Node {
                id: br.clone(),
                kind: NodeKind::Branch {
                    on: NodeId("on".into()),
                    arms: vec![(BranchCond::TextContains("canned".into()), agent_arm)],
                    default: Graph {
                        nodes: vec![mc("armDefault_out", None)],
                    },
                },
                deps: vec![Dep {
                    on: NodeId("on".into()),
                    kind: EdgeKind::Hard,
                }],
            },
        ],
    };
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(registry);
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    assert!(
        out.outputs[&br].get("agent_out").is_some(),
        "nested agent arm ran: {}",
        out.outputs[&br]
    );
}

/// Regression (whole-slice review): `namespace_graph` must rewrite a nested
/// `Branch.on` (a sibling reference), else a Branch inside a Subgraph (or a Branch
/// arm) hits `BranchInputMissing` at runtime on a *validated* graph — the branch's
/// predecessor is namespaced to `"s/on"` while the branch's `on` field stays `"on"`,
/// so `run_branch`'s `prior_outputs.get(on)` misses. Top-level Branch is unaffected
/// (its predecessor is not namespaced). `.expect` panics before the fix.
#[tokio::test]
async fn a_branch_nested_in_a_subgraph_namespaces_its_on_and_runs() {
    use orchestrator_core::BranchCond;
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let s = NodeId("s".into());
    // Subgraph whose inner graph is [ on(ModelCall "c"), br(Branch on `on`, Hard-dep) ].
    let inner = vec![
        mc("on", None),
        Node {
            id: NodeId("br".into()),
            kind: NodeKind::Branch {
                on: NodeId("on".into()),
                arms: vec![(BranchCond::TextContains("canned".into()), arm("armX"))],
                default: arm("armD"),
            },
            deps: vec![Dep {
                on: NodeId("on".into()),
                kind: EdgeKind::Hard,
            }],
        },
    ];
    let graph = Graph {
        nodes: vec![subgraph_node("s", inner)],
    };
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    // Before the fix: run_branch(prior_outputs["on"]) misses (predecessor is "s/on")
    // → BranchInputMissing → Err → .expect panics. After: the branch resolves + runs armX.
    assert!(out.failed.is_none(), "nested branch runs: {out:?}");
    // The subgraph's sink is the nested branch "br"; its value is the selected arm's
    // sink map ({armX: <output>}).
    let sub = &out.outputs[&s];
    assert!(
        sub.get("br").is_some(),
        "subgraph sink includes the nested branch: {sub}"
    );
    assert!(
        sub["br"].get("armX").is_some(),
        "the nested branch's output is the selected arm's sink map: {sub}"
    );
}

#[test]
fn validate_dag_rejects_bad_branch() {
    use orchestrator_core::BranchCond;
    let no_dep = Graph {
        nodes: vec![
            mc("on", None),
            Node {
                id: NodeId("br".into()),
                kind: NodeKind::Branch {
                    on: NodeId("on".into()),
                    arms: vec![(
                        BranchCond::FieldTrue("x".into()),
                        Graph {
                            nodes: vec![mc("a", None)],
                        },
                    )],
                    default: Graph {
                        nodes: vec![mc("d", None)],
                    },
                },
                deps: vec![], // MISSING Hard dep on `on`
            },
        ],
    };
    assert!(matches!(
        no_dep.validate_dag(),
        Err(OrchestratorError::InvalidGraph(_))
    ));
    // A SOFT dep on `on` is not enough — the Branch requires a HARD dep on its
    // predecessor (a failed `on` must cascade-skip the branch, never let it decide
    // over an absent input).
    let soft_dep = Graph {
        nodes: vec![
            mc("on", None),
            Node {
                id: NodeId("br".into()),
                kind: NodeKind::Branch {
                    on: NodeId("on".into()),
                    arms: vec![(
                        BranchCond::FieldTrue("x".into()),
                        Graph {
                            nodes: vec![mc("a", None)],
                        },
                    )],
                    default: Graph {
                        nodes: vec![mc("d", None)],
                    },
                },
                deps: vec![Dep {
                    on: NodeId("on".into()),
                    kind: EdgeKind::Soft, // Soft, not Hard
                }],
            },
        ],
    };
    assert!(matches!(
        soft_dep.validate_dag(),
        Err(OrchestratorError::InvalidGraph(_))
    ));
    let undeclared = Graph {
        nodes: vec![Node {
            id: NodeId("br".into()),
            kind: NodeKind::Branch {
                on: NodeId("ghost".into()),
                arms: vec![],
                default: Graph {
                    nodes: vec![mc("d", None)],
                },
            },
            deps: vec![Dep {
                on: NodeId("ghost".into()),
                kind: EdgeKind::Hard,
            }],
        }],
    };
    assert!(matches!(
        undeclared.validate_dag(),
        Err(OrchestratorError::InvalidGraph(_))
    ));
    let cyc = Graph {
        nodes: vec![
            mc("on", None),
            Node {
                id: NodeId("br".into()),
                kind: NodeKind::Branch {
                    on: NodeId("on".into()),
                    arms: vec![(
                        BranchCond::FieldTrue("x".into()),
                        Graph {
                            nodes: vec![
                                Node {
                                    id: NodeId("a".into()),
                                    kind: NodeKind::ModelCall {
                                        chain: "c".into(),
                                        payload: serde_json::json!(0),
                                    },
                                    deps: vec![Dep {
                                        on: NodeId("b".into()),
                                        kind: EdgeKind::Hard,
                                    }],
                                },
                                Node {
                                    id: NodeId("b".into()),
                                    kind: NodeKind::ModelCall {
                                        chain: "c".into(),
                                        payload: serde_json::json!(0),
                                    },
                                    deps: vec![Dep {
                                        on: NodeId("a".into()),
                                        kind: EdgeKind::Hard,
                                    }],
                                },
                            ],
                        },
                    )],
                    default: Graph {
                        nodes: vec![mc("d", None)],
                    },
                },
                deps: vec![Dep {
                    on: NodeId("on".into()),
                    kind: EdgeKind::Hard,
                }],
            },
        ],
    };
    assert!(matches!(
        cyc.validate_dag(),
        Err(OrchestratorError::InvalidGraph(_))
    ));
    // nested cycle in the DEFAULT arm → InvalidGraph (recursion into default).
    let default_cyc = Graph {
        nodes: vec![
            mc("on", None),
            Node {
                id: NodeId("br".into()),
                kind: NodeKind::Branch {
                    on: NodeId("on".into()),
                    arms: vec![],
                    default: Graph {
                        nodes: vec![
                            Node {
                                id: NodeId("a".into()),
                                kind: NodeKind::ModelCall {
                                    chain: "c".into(),
                                    payload: serde_json::json!(0),
                                },
                                deps: vec![Dep {
                                    on: NodeId("b".into()),
                                    kind: EdgeKind::Hard,
                                }],
                            },
                            Node {
                                id: NodeId("b".into()),
                                kind: NodeKind::ModelCall {
                                    chain: "c".into(),
                                    payload: serde_json::json!(0),
                                },
                                deps: vec![Dep {
                                    on: NodeId("a".into()),
                                    kind: EdgeKind::Hard,
                                }],
                            },
                        ],
                    },
                },
                deps: vec![Dep {
                    on: NodeId("on".into()),
                    kind: EdgeKind::Hard,
                }],
            },
        ],
    };
    assert!(matches!(
        default_cyc.validate_dag(),
        Err(OrchestratorError::InvalidGraph(_))
    ));
}

// ---------------------------------------------------------------------------
// SP-3 slice 3 — `NodeKind::Expand`: a node whose nested DAG is produced at
// runtime by an injected `Planner`, journaled as `PlanExpanded`, and driven
// under the node's path.
// ---------------------------------------------------------------------------

/// A `Planner` that always returns a fixed graph (the produced plan under test).
struct FixedPlanner(Graph);
#[async_trait::async_trait]
impl orchestrator_core::Planner for FixedPlanner {
    async fn plan(&self, _input: &serde_json::Value) -> Result<Graph, OrchestratorError> {
        Ok(self.0.clone())
    }
}
/// A `Planner` that always errors — exercises the planner-failure path.
struct ErrPlanner;
#[async_trait::async_trait]
impl orchestrator_core::Planner for ErrPlanner {
    async fn plan(&self, _input: &serde_json::Value) -> Result<Graph, OrchestratorError> {
        Err(OrchestratorError::InvalidGraph("planner boom".into()))
    }
}

fn expand_node(id: &str, deps: Vec<Dep>) -> Node {
    Node {
        id: NodeId(id.into()),
        kind: NodeKind::Expand {
            input: serde_json::json!({}),
            planner: orchestrator_core::PlannerRef::Injected,
        },
        deps,
    }
}

#[tokio::test]
async fn expand_drives_a_produced_plan_and_returns_the_sink_map() {
    let (gateway, _c) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let planner = Arc::new(FixedPlanner(Graph {
        nodes: vec![mc("n1", None), mc("n2", Some("n1"))],
    }));
    let exec =
        Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1").with_planner(planner);
    let e = NodeId("e".into());
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![expand_node("e", vec![])],
    };
    let out = exec.run(run, &graph).await.expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    assert!(
        out.outputs[&e].get("n2").is_some(),
        "sink map has n2: {}",
        out.outputs[&e]
    );
    assert!(out.outputs[&e].get("n1").is_none(), "n1 is not a sink");

    // AC2: PlanExpanded precedes the nested effects.
    let events = journal.load(run).await.unwrap();
    let pe = events
        .iter()
        .position(|(_, ev)| matches!(ev, JournalEvent::PlanExpanded { .. }))
        .expect("PlanExpanded journaled");
    let first_rec = events
        .iter()
        .position(|(_, ev)| matches!(ev, JournalEvent::EffectRecorded { .. }))
        .expect("nested effects journaled");
    assert!(pe < first_rec, "PlanExpanded precedes the nested effects");
}

#[tokio::test]
async fn expand_planner_error_fails_the_node_and_cascade_skips_hard_dependents() {
    let (gateway, _c) = recording_gateway().await;
    // e (Expand, ErrPlanner) → d (Hard-dep e) ; s (Soft-dep e).
    let graph = Graph {
        nodes: vec![
            expand_node("e", vec![]),
            mc_dep("d", Dep::hard("e")),
            mc_dep("s", Dep::soft("e")),
        ],
    };
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_planner(Arc::new(ErrPlanner));
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(
        matches!(&out.failed, Some((n, _)) if n == &NodeId("e".into())),
        "expand failed: {out:?}"
    );
    assert!(
        out.skipped.contains(&NodeId("d".into())),
        "hard-dependent skipped"
    );
    assert!(
        out.completed.contains(&NodeId("s".into())),
        "soft-dependent still ran"
    );
}

#[tokio::test]
async fn expand_invalid_plan_fails_the_node_without_journaling_an_expansion() {
    let (gateway, _c) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    // A cyclic produced graph (a → b → a): validate_dag rejects it.
    let cyclic = Graph {
        nodes: vec![mc_dep("a", Dep::hard("b")), mc_dep("b", Dep::hard("a"))],
    };
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_planner(Arc::new(FixedPlanner(cyclic)));
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![expand_node("e", vec![])],
    };
    let out = exec.run(run, &graph).await.expect("run yields an outcome");
    assert!(
        out.failed.is_some(),
        "invalid plan fails the expand: {out:?}"
    );
    let events = journal.load(run).await.unwrap();
    assert!(
        !events
            .iter()
            .any(|(_, ev)| matches!(ev, JournalEvent::PlanExpanded { .. })),
        "no PlanExpanded journaled for an invalid plan (validated before append)"
    );
}

#[tokio::test]
async fn expand_with_no_planner_fails_loud() {
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1");
    let graph = Graph {
        nodes: vec![expand_node("e", vec![])],
    };
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(
        matches!(&out.failed, Some((n, m)) if n == &NodeId("e".into()) && m.contains("no planner")),
        "expand with no planner fails loud: {out:?}"
    );
}

/// AC3: after a crash mid-plan, a resume reconstructs the JOURNALED plan and never
/// re-invokes the planner — even one rigged to return a different graph.
#[tokio::test]
async fn expand_resume_uses_the_journaled_plan_not_a_re_plan() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let e = NodeId("e".into());
    let graph = Graph {
        nodes: vec![expand_node("e", vec![])],
    };

    // Run 1: planner returns plan A (n1 → n2). Gateway succeeds on inner n1 (call 1),
    // fails on inner n2 (call 2). PlanExpanded{A} + e/n1 are journaled; the run fails.
    let plan_a = Graph {
        nodes: vec![mc("n1", None), mc("n2", Some("n1"))],
    };
    let (gw1, calls1) = failing_after_gateway(1).await;
    let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_planner(Arc::new(FixedPlanner(plan_a)));
    let o1 = exec1
        .run(run, &graph)
        .await
        .expect("run 1 yields an outcome");
    assert!(o1.failed.is_some(), "run 1 fails inside the plan: {o1:?}");
    assert_eq!(
        calls1.lock().unwrap().len(),
        2,
        "run 1 hit the gateway for n1 and the failing n2"
    );

    // Run 2: a DIFFERENT planner (would return `zzz`) + an always-succeeding gateway
    // over the SAME journal. Resume must reuse plan A from the journal.
    let plan_b = Graph {
        nodes: vec![mc("zzz", None)],
    };
    let (gw2, calls2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_planner(Arc::new(FixedPlanner(plan_b)));
    let o2 = exec2.start(run, &graph).await.expect("resume completes");
    assert!(o2.failed.is_none(), "resume completes: {:?}", o2.failed);
    assert!(
        o2.outputs[&e].get("n2").is_some(),
        "journaled plan A used (n2 present): {}",
        o2.outputs[&e]
    );
    assert!(
        o2.outputs[&e].get("zzz").is_none(),
        "the re-plan graph (zzz) was NOT used"
    );

    // The proof: run-2's gateway saw EXACTLY ONE call, for the failed inner n2 — n1
    // replayed from the memo, the planner was never re-invoked.
    let recorded2 = calls2.lock().unwrap().clone();
    assert_eq!(
        recorded2.len(),
        1,
        "resume re-called the gateway only for n2: {recorded2:?}"
    );
    assert_eq!(
        recorded2[0].1, "n2",
        "the single resume call carried n2's prompt"
    );
}

/// AC4: a completed `Expand` whose OUTER tail fails resumes without re-planning or
/// re-spending on the plan's inner nodes.
#[tokio::test]
async fn expand_completed_then_failing_tail_resumes_without_replan() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    // e (Expand, plan = single node "n1") → d (Hard-dep e).
    let graph = Graph {
        nodes: vec![expand_node("e", vec![]), mc_dep("d", Dep::hard("e"))],
    };

    // Run 1: e's plan completes (inner n1 = call 1), then d fails (call 2).
    let (gw1, _c1) = failing_after_gateway(1).await;
    let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1").with_planner(
        Arc::new(FixedPlanner(Graph {
            nodes: vec![mc("n1", None)],
        })),
    );
    let o1 = exec1.run(run, &graph).await.expect("run 1");
    assert!(o1.failed.is_some(), "tail d failed: {o1:?}");

    // Run 2: a planner that would produce a DIFFERENT plan (`other`) + a succeeding
    // gateway. Resume replays e from the journal (no re-plan) and re-drives only d.
    let (gw2, calls2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1").with_planner(
        Arc::new(FixedPlanner(Graph {
            nodes: vec![mc("other", None)],
        })),
    );
    let o2 = exec2.start(run, &graph).await.expect("resume completes");
    assert!(o2.failed.is_none(), "resume completes: {o2:?}");
    assert!(
        o2.outputs[&NodeId("e".into())].get("n1").is_some(),
        "e replayed the journaled plan (n1), not the re-plan: {}",
        o2.outputs[&NodeId("e".into())]
    );
    let recorded2 = calls2.lock().unwrap().clone();
    assert_eq!(
        recorded2.len(),
        1,
        "resume re-called the gateway only for d: {recorded2:?}"
    );
    assert_eq!(
        recorded2[0].1, "d",
        "the single resume call carried d's prompt"
    );
}

/// AC8: more expansions than `max_expansions` → a hard `GlobalCapExceeded` halt.
#[tokio::test]
async fn expand_max_expansions_cap_halts_loud() {
    // e1 → e2 (sequential): two expansions. Each plan is a single node.
    let graph = Graph {
        nodes: vec![
            expand_node("e1", vec![]),
            expand_node("e2", vec![Dep::hard("e1")]),
        ],
    };
    let planner = Arc::new(FixedPlanner(Graph {
        nodes: vec![mc("x", None)],
    }));

    // Limit 1: the 2nd expansion breaches the cap → Err.
    let (gw, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
        .with_planner(planner.clone())
        .with_max_expansions(1);
    let err = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect_err("cap halts");
    assert!(
        matches!(&err, OrchestratorError::GlobalCapExceeded { cap, .. } if cap == "max_expansions"),
        "max_expansions breach: {err:?}"
    );

    // Limit 2: both expansions fit → ok.
    let (gw2, _c2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(InMemoryJournal::new()), "v1")
        .with_planner(planner)
        .with_max_expansions(2);
    let out = exec2
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("within cap");
    assert!(out.failed.is_none(), "{out:?}");
}

/// AC9: `max_nodes` is cumulative AND spans resume — the counter is seeded from the
/// journal, so a resumed expansion is charged against nodes counted before the crash.
#[tokio::test]
async fn expand_max_nodes_cap_spans_resume() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    // e1 (plan = 2 nodes) → d (tail) → e2 (plan = 2 nodes). max_nodes = 3.
    let graph = Graph {
        nodes: vec![
            expand_node("e1", vec![]),
            mc_dep("d", Dep::hard("e1")),
            expand_node("e2", vec![Dep::hard("d")]),
        ],
    };
    let plan2 = Graph {
        nodes: vec![mc("p", None), mc("q", Some("p"))],
    };

    // Run 1: e1 expands (2 nodes: calls 1,2), then d fails (call 3). e2 never runs.
    // Journal carries PlanExpanded{e1} (2 nodes).
    let (gw1, _c1) = failing_after_gateway(2).await;
    let exec1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_planner(Arc::new(FixedPlanner(plan2.clone())))
        .with_max_nodes(3);
    let o1 = exec1
        .run(run, &graph)
        .await
        .expect("run 1 yields an outcome");
    assert!(o1.failed.is_some(), "run 1 fails at d: {o1:?}");

    // Run 2: resume seeds the node counter from the journal (=2). e1 replays (no
    // re-count); d succeeds; e2 expands → 2 + 2 = 4 > 3 → cap. Without seeding, e2
    // alone (2 nodes) would fit, so this assertion is what proves the cap SPANS resume.
    let (gw2, _c2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_planner(Arc::new(FixedPlanner(plan2)))
        .with_max_nodes(3);
    let err = exec2
        .start(run, &graph)
        .await
        .expect_err("resume breaches max_nodes");
    assert!(
        matches!(&err, OrchestratorError::GlobalCapExceeded { cap, .. } if cap == "max_nodes"),
        "max_nodes breach spans resume: {err:?}"
    );
}

/// AC10 (failure): a failing node inside the produced plan fails the Expand node and
/// cascade-skips its outer hard-dependent.
#[tokio::test]
async fn a_failing_node_in_the_expand_plan_fails_the_expand() {
    // The plan's single inner node fails on the gateway's first call.
    let (gateway, _c) = failing_after_gateway(0).await;
    let graph = Graph {
        nodes: vec![expand_node("e", vec![]), mc_dep("d", Dep::hard("e"))],
    };
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_planner(Arc::new(FixedPlanner(Graph {
            nodes: vec![mc("boom", None)],
        })));
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(
        matches!(&out.failed, Some((n, _)) if n == &NodeId("e".into())),
        "nested failure fails the expand: {out:?}"
    );
    assert!(
        out.skipped.contains(&NodeId("d".into())),
        "outer hard-dependent skipped"
    );
}

/// AC10 (pause): an in-doubt Mutation inside the Expand's plan pauses the whole run —
/// mirrors `an_in_doubt_mutation_in_a_subgraph_pauses_the_run`, wrapping the mutating
/// agent in an `Expand` plan instead of a `Subgraph`.
#[tokio::test]
async fn an_in_doubt_mutation_in_an_expand_plan_pauses_the_run() {
    let run = RunId(uuid::Uuid::new_v4());
    let mk_recorder = |sink: Arc<std::sync::Mutex<Vec<String>>>| {
        let recorder = AgentDefinition {
            name: "recorder".into(),
            area: "research".into(),
            kind: "reasoning".into(),
            chain: Some("research.bulk".into()),
            chains: std::collections::HashMap::new(),
            grants: std::collections::HashMap::new(),
            tools: vec!["record_note".into()],
            skills: vec![],
            system_prompt: "Record.".into(),
        };
        (
            Arc::new(
                Registry::default()
                    .with_agent(recorder)
                    .with_tool(RecordNote::new(sink.clone()).spec()),
            ),
            Arc::new(ToolRegistry::default().with_tool(Arc::new(RecordNote::new(sink)))),
        )
    };
    // The mutation-bearing agent lives inside an Expand plan (inner node "n1").
    let plan = Graph {
        nodes: vec![agent_node("n1", "recorder", "item-0")],
    };
    let graph = Graph {
        nodes: vec![expand_node("e", vec![])],
    };

    // Seed: run to completion, then truncate to the nested agent's record_note
    // EffectIntent (drops its EffectRecorded) → in-doubt on resume.
    let full = InMemoryJournal::new();
    let (seed_reg, seed_tools) = mk_recorder(Arc::new(std::sync::Mutex::new(Vec::new())));
    let (gw_s, _c) = demo_reference_tool_gateway().await;
    Executor::new(Arc::new(gw_s), Arc::new(full.clone()), "v1")
        .with_registry(seed_reg)
        .with_tools(seed_tools)
        .with_planner(Arc::new(FixedPlanner(plan.clone())))
        .run(run, &graph)
        .await
        .expect("seed Expand run completes");
    let events = full.load(run).await.unwrap();
    let cut = events
        .iter()
        .position(|(_, e)| matches!(e, JournalEvent::EffectIntent { .. }))
        .expect("the nested agent journaled a record_note EffectIntent");
    let seeded = InMemoryJournal::new();
    for (_, e) in &events[..=cut] {
        seeded.append(run, e.clone()).await.unwrap();
    }

    // Resume with an Indeterminate reconciler + a FRESH empty sink → the nested
    // Mutation is in-doubt → the nested agent pauses → the Expand pauses → the run pauses.
    let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let (reg, tools) = mk_recorder(sink.clone());
    let reconcilers =
        ReconcileRegistry::default().with_provider("record_note", Arc::new(AlwaysIndeterminate));
    let (gw_r, _c2) = demo_reference_tool_gateway().await;
    let outcome = Executor::new(Arc::new(gw_r), Arc::new(seeded.clone()), "v1")
        .with_registry(reg)
        .with_tools(tools)
        .with_reconcilers(Arc::new(reconcilers))
        .with_planner(Arc::new(FixedPlanner(plan)))
        .start(run, &graph)
        .await
        .expect("resume yields an outcome");

    let pause = outcome
        .paused
        .expect("the in-doubt nested Mutation pauses the whole run");
    assert_eq!(
        pause.node,
        NodeId("e".into()),
        "the Expand node is the pause point"
    );
    let resumed = seeded.load(run).await.unwrap();
    assert!(
        resumed
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunPaused { .. })),
        "RunPaused is journaled"
    );
    assert!(
        !resumed
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "the run must NOT complete over an unresolved in-doubt Intent"
    );
    assert!(
        sink.lock().unwrap().is_empty(),
        "a paused in-doubt Mutation applies no side effect"
    );
}

/// AC12 (end-to-end): an Expand whose produced plan is a nested `Agent` node drives it
/// through the gateway; the agent's output is the Expand's sink.
#[tokio::test]
async fn expand_drives_a_produced_agent_plan_end_to_end() {
    let (gateway, _c) = recording_gateway().await;
    let registry = agent_registry("c");
    let e = NodeId("e".into());
    let graph = Graph {
        nodes: vec![expand_node("e", vec![])],
    };
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(registry)
        .with_planner(Arc::new(FixedPlanner(Graph {
            nodes: vec![agent_node("n1", "a", "hi")],
        })));
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    assert!(
        out.outputs[&e].get("n1").is_some(),
        "nested agent output is the expand sink: {}",
        out.outputs[&e]
    );
}

/// on_plan_expanded fires once with the graph + labels when a plan is journaled.
#[tokio::test]
async fn on_plan_expanded_fires_with_the_plan() {
    use std::sync::{Arc, Mutex};
    struct Spy(Arc<Mutex<Vec<(String, usize, usize)>>>);
    #[async_trait::async_trait]
    impl OrchestratorHooks for Spy {
        async fn on_plan_expanded(
            &self,
            _run: RunId,
            node: &NodeId,
            graph: &Graph,
            node_plans: &std::collections::HashMap<NodeId, orchestrator_core::NodePlan>,
        ) {
            self.0
                .lock()
                .unwrap()
                .push((node.0.clone(), graph.nodes.len(), node_plans.len()));
        }
    }
    let log = Arc::new(Mutex::new(Vec::new()));
    let (gateway, _c) = recording_gateway().await;
    let planner = Arc::new(FixedPlanner(Graph {
        nodes: vec![mc("n1", None)],
    }));
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_planner(planner)
        .with_hooks(Arc::new(Spy(log.clone())));
    let graph = Graph {
        nodes: vec![expand_node("e", vec![])],
    };
    exec.run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    let seen = log.lock().unwrap().clone();
    assert_eq!(seen.len(), 1, "on_plan_expanded fired once: {seen:?}");
    assert_eq!(seen[0].0, "e");
    assert_eq!(seen[0].1, 1, "graph carried to the hook");
}

// ---------------------------------------------------------------------------
// SP-3 slice 4A — the journaled planner AGENT: an `Expand` node whose plan is
// produced by a real ReAct sub-run (`PlannerRef::Agent`) under `"{expand}/__plan__"`,
// parsed as a `PlannedGraph`, run through `feasible`, journaled, and spliced.
// ---------------------------------------------------------------------------

/// A registry with a `planner` agent on a plain chain (no tools for the minimal
/// path — the agent's single turn emits the plan JSON directly). The plan itself
/// comes from the scripted gateway, so this helper takes no plan argument.
fn planner_registry() -> Arc<Registry> {
    Arc::new(Registry::default().with_agent(AgentDefinition {
        name: "planner".into(),
        area: "planning".into(),
        kind: "reasoning".into(),
        chain: Some("c".into()),
        chains: std::collections::HashMap::new(),
        grants: std::collections::HashMap::new(),
        tools: vec![],
        skills: vec![],
        system_prompt: "Emit a plan as JSON.".into(),
    }))
}

fn expand_agent_node(id: &str, deps: Vec<Dep>) -> Node {
    Node {
        id: NodeId(id.into()),
        kind: NodeKind::Expand {
            input: serde_json::json!({ "goal": "do the thing" }),
            planner: orchestrator_core::PlannerRef::Agent(AgentRef("planner".into())),
        },
        deps,
    }
}

#[tokio::test]
async fn journaled_planner_agent_produces_and_splices_a_plan() {
    // The planner agent emits a 2-node plan (n1 -> n2); the executor splices + runs it.
    let plan_json = r#"{"graph":{"nodes":[
        {"id":"n1","kind":{"ModelCall":{"chain":"c","payload":{"prompt":"n1"}}},"deps":[]},
        {"id":"n2","kind":{"ModelCall":{"chain":"c","payload":{"prompt":"n2"}}},"deps":[{"on":"n1","kind":"Hard"}]}
    ]},"node_plans":{"n1":{"label":"first"},"n2":{"label":"second"}}}"#;
    let reg = planner_registry();
    // response[0] → the planner turn (runs first, sequentially); [1]/[2] → the spliced
    // plan nodes n1 then n2 (each a ModelCall on chain "c", one gateway call apiece).
    let (gateway, _c) = scripted_gateway(vec![
        final_response(plan_json),
        final_response("n1 out"),
        final_response("n2 out"),
    ])
    .await;
    let journal = InMemoryJournal::new();

    // A spy over `on_plan_expanded` proving the `node_plans` side-map reaches the hook
    // end-to-end (len == 2, not an empty map).
    use std::sync::Mutex;
    struct PlanSpy(Arc<Mutex<Vec<(String, usize)>>>);
    #[async_trait::async_trait]
    impl OrchestratorHooks for PlanSpy {
        async fn on_plan_expanded(
            &self,
            _run: RunId,
            node: &NodeId,
            _graph: &Graph,
            node_plans: &std::collections::HashMap<NodeId, orchestrator_core::NodePlan>,
        ) {
            self.0
                .lock()
                .unwrap()
                .push((node.0.clone(), node_plans.len()));
        }
    }
    let plan_log = Arc::new(Mutex::new(Vec::new()));

    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(reg)
        .with_hooks(Arc::new(PlanSpy(plan_log.clone())));
    let run = RunId(uuid::Uuid::new_v4());
    let e = NodeId("e".into());
    let graph = Graph {
        nodes: vec![expand_agent_node("e", vec![])],
    };
    let out = exec.run(run, &graph).await.expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    assert!(
        out.outputs[&e].get("n2").is_some(),
        "sink map has n2: {}",
        out.outputs[&e]
    );

    // The `node_plans` side-map reached the hook with both entries (n1, n2).
    let seen_plans = plan_log.lock().unwrap().clone();
    assert_eq!(
        seen_plans.len(),
        1,
        "on_plan_expanded fired once: {seen_plans:?}"
    );
    assert_eq!(seen_plans[0].0, "e");
    assert_eq!(
        seen_plans[0].1, 2,
        "node_plans side-map carried both entries to the hook: {seen_plans:?}"
    );

    // The planner turns are journaled under "e/__plan__"; the plan nodes under "e/…".
    let labels: Vec<String> = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .filter_map(|(_, ev)| match ev {
            JournalEvent::NodeStarted { node } => Some(node.0.clone()),
            _ => None,
        })
        .collect();
    assert!(
        labels.iter().any(|l| l.starts_with("e/__plan__")),
        "planner turn journaled: {labels:?}"
    );
    assert!(
        labels.iter().any(|l| l == "e/n1"),
        "plan node journaled: {labels:?}"
    );
    assert!(
        labels.iter().any(|l| l == "e/n2"),
        "both plan nodes spliced+journaled: {labels:?}"
    );
    // The reserved id names the planner sub-run ("e/__plan__") — it is never itself a
    // spliced plan node ("__plan__" bare, or nested under a plan node).
    assert!(
        !labels
            .iter()
            .any(|l| l == "__plan__" || l == "e/n1/__plan__"),
        "reserved id is only the planner sub-run path: {labels:?}"
    );
}

#[tokio::test]
async fn planner_agent_invalid_plan_fails_the_node() {
    let reg = planner_registry();
    let (gateway, _c) = scripted_gateway(vec![final_response("this is not json")]).await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1").with_registry(reg);
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![expand_agent_node("e", vec![]), mc_dep("d", Dep::hard("e"))],
    };
    let out = exec.run(run, &graph).await.expect("run");
    assert!(
        matches!(&out.failed, Some((n, _)) if n == &NodeId("e".into())),
        "{out:?}"
    );
    assert!(
        out.skipped.contains(&NodeId("d".into())),
        "hard-dependent skipped"
    );
    assert!(
        !journal
            .load(run)
            .await
            .unwrap()
            .iter()
            .any(|(_, ev)| matches!(ev, JournalEvent::PlanExpanded { .. })),
        "no PlanExpanded for an unparseable plan"
    );
}

#[tokio::test]
async fn unresolvable_planner_agent_fails_the_node() {
    // PlannerRef::Agent names an agent NOT in the registry.
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(Arc::new(Registry::default()));
    let graph = Graph {
        nodes: vec![expand_agent_node("e", vec![])],
    };
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(
        matches!(&out.failed, Some((n, _)) if n == &NodeId("e".into())),
        "{out:?}"
    );
}

/// Resume post-PlanExpanded reuses the journaled plan; the planner agent is NOT
/// re-invoked and the plan node is NOT re-spent (mirrors the slice-3
/// `expand_completed_then_failing_tail_resumes_without_replan`, but the planner is
/// an agent). The load-bearing proof: run-2's gateway sees EXACTLY ONE call — `d`.
#[tokio::test]
async fn planner_agent_resume_reuses_journaled_plan() {
    // Single-node plan (ModelCall n1 with prompt "n1"). e (planner agent) -> d (tail).
    let plan_json = r#"{"graph":{"nodes":[{"id":"n1","kind":{"ModelCall":{"chain":"c","payload":{"prompt":"n1"}}},"deps":[]}]}}"#;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![expand_agent_node("e", vec![]), mc_dep("d", Dep::hard("e"))],
    };
    // Run 1: a scripted gateway supplies the planner's plan (call 1) and plan node n1
    // (call 2); it has NO 3rd response, so d's call errors → d fails, leaving PlanExpanded
    // + n1 journaled and NO RunCompleted.
    {
        let (gw_s, _c) = scripted_gateway(vec![
            final_response(plan_json), // planner turn → the plan
            final_response("n1 out"),  // plan node n1
        ])
        .await;
        let exec = Executor::new(Arc::new(gw_s), Arc::new(journal.clone()), "v1")
            .with_registry(planner_registry());
        let o1 = exec.run(run, &graph).await.expect("run1");
        assert!(o1.failed.is_some(), "tail d failed: {o1:?}");
    }
    // Run 2: a FRESH recording gateway (succeeds for everything) over the SAME journal.
    // Resume reuses the journaled plan (planner skipped, n1 replayed from memo) and
    // re-drives ONLY d.
    let (gw2, calls2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_registry(planner_registry());
    let o2 = exec2.start(run, &graph).await.expect("resume");
    assert!(o2.failed.is_none(), "resume completes: {o2:?}");
    assert!(
        o2.outputs[&NodeId("e".into())].get("n1").is_some(),
        "journaled plan (n1) reused: {}",
        o2.outputs[&NodeId("e".into())]
    );
    let recorded2 = calls2.lock().unwrap().clone();
    assert_eq!(
        recorded2.len(),
        1,
        "resume re-called the gateway only for d (no re-plan, no n1 re-spend): {recorded2:?}"
    );
    assert_eq!(
        recorded2[0].1, "d",
        "the single resume call carried d's prompt"
    );
}

/// AC8: the planner agent's turn hits a timed gate → `AgentStep::Paused` →
/// `run_expand` returns `NodeExec::Paused` → the run pauses (RunOutcome.paused set,
/// RunPaused journaled, no RunCompleted, no plan produced). Reuses the SP-1
/// `timeout_gateway()` warm-up→all-gated fixture.
#[tokio::test]
async fn planner_agent_pause_pauses_the_run() {
    use crate::test_support::timeout_gateway;
    let gw = timeout_gateway().await;
    let req = support::build_request("c", &serde_json::json!({ "prompt": "warm" }));
    let _ = gw.execute(&req).await; // warm-up cools the sole router "r"
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![expand_agent_node("e", vec![])],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_registry(planner_registry())
        .with_tools(Arc::new(ToolRegistry::default()))
        .run(run, &graph)
        .await
        .expect("run yields an outcome");
    assert!(
        out.paused.is_some(),
        "the planner's gated turn pauses the run: {:?}",
        out.failed
    );
    assert!(out.failed.is_none());
    let events = journal.load(run).await.unwrap();
    assert!(
        events.iter().any(|(_, e)| matches!(
            e,
            JournalEvent::RunPaused {
                resume_after: Some(_),
                ..
            }
        )),
        "RunPaused with a timed resume_after is journaled"
    );
    assert!(
        !events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "a paused run does not complete"
    );
    assert!(
        !events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::PlanExpanded { .. })),
        "a planner paused before producing a plan journals no PlanExpanded"
    );
}

/// AC11 (full palette): the planner emits a tier-2 `Map` -> `Consolidate` plan
/// (ModelCall bodies over the test chain "c"); the executor splices + runs it to
/// completion, folding the Consolidate as the Expand's sink.
#[tokio::test]
async fn planner_agent_emits_a_map_consolidate_plan() {
    // Map "m" (2 items, BestEffort) -> Consolidate "cons" (soft-dep m, min_viable 1).
    let plan_json = r#"{"graph":{"nodes":[
        {"id":"m","kind":{"Map":{"body":{"ModelCall":{"chain":"c"}},"over":[{"prompt":"i0"},{"prompt":"i1"}],"concurrency":2,"aggregation":"BestEffort"}},"deps":[]},
        {"id":"cons","kind":{"Consolidate":{"over":"m","min_viable":1,"body":{"ModelCall":{"chain":"c"}}}},"deps":[{"on":"m","kind":"Soft"}]}
    ]},"node_plans":{"m":{"label":"fan out"},"cons":{"label":"consolidate"}}}"#;
    // response[0] → planner turn (sequential, first); the rest → the spliced Map
    // children + Consolidate body (all succeed; content is irrelevant, so identical
    // responses sidestep the Map's nondeterministic concurrent child order). A small
    // surplus is harmless (unused responses are never popped).
    let mut responses = vec![final_response(plan_json)];
    responses.extend((0..5).map(|_| final_response("ok")));
    let (gateway, _c) = scripted_gateway(responses).await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(planner_registry());
    let e = NodeId("e".into());
    let graph = Graph {
        nodes: vec![expand_agent_node("e", vec![])],
    };
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    assert!(
        out.outputs[&e].get("cons").is_some(),
        "Consolidate is the Expand sink: {}",
        out.outputs[&e]
    );
}

/// A mid-plan crash (the planner turn journaled, but NO `PlanExpanded`) that resumes
/// after the planner agent's `system_prompt` changed → the memoized planner turn's
/// input-hash diverges → resume HALTS with a fatal `DeterminismViolation` (a hard
/// `Err`), never silently downgraded to a node `Failed`. Proves the planner branch
/// `?`-propagates fatal `drive_agent` errors like every other caller.
#[tokio::test]
async fn planner_agent_determinism_violation_in_the_plan_sub_run_halts() {
    // The planner agent, parameterized by its `system_prompt` (the input-hash input).
    let planner_reg = |sys: &str| {
        Arc::new(Registry::default().with_agent(AgentDefinition {
            name: "planner".into(),
            area: "planning".into(),
            kind: "reasoning".into(),
            chain: Some("c".into()),
            chains: std::collections::HashMap::new(),
            grants: std::collections::HashMap::new(),
            tools: vec![],
            skills: vec![],
            system_prompt: sys.into(),
        }))
    };
    // Single-node plan (n1); e (planner agent) drives it.
    let plan_json = r#"{"graph":{"nodes":[{"id":"n1","kind":{"ModelCall":{"chain":"c","payload":{"prompt":"n1"}}},"deps":[]}]}}"#;
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![expand_agent_node("e", vec![])],
    };

    // Run 1 (system_prompt "SYS_A"): run to completion so the planner turn is fully
    // journaled (NodeStarted/EffectRecorded/NodeCompleted at "e/__plan__") ahead of
    // the PlanExpanded event.
    let full = InMemoryJournal::new();
    let (gw1, _c1) =
        scripted_gateway(vec![final_response(plan_json), final_response("n1 out")]).await;
    Executor::new(Arc::new(gw1), Arc::new(full.clone()), "v1")
        .with_registry(planner_reg("SYS_A"))
        .run(run, &graph)
        .await
        .expect("seed run completes");

    // Truncate to the prefix BEFORE PlanExpanded → the planner turn is memoized, but
    // "e" has no journaled expansion, so resume re-enters the Agent branch and replays
    // the memoized planner turn (hash-checked).
    let events = full.load(run).await.unwrap();
    let cut = events
        .iter()
        .position(|(_, e)| matches!(e, JournalEvent::PlanExpanded { .. }))
        .expect("run 1 journaled a PlanExpanded");
    let seeded = InMemoryJournal::new();
    for (_, e) in &events[..cut] {
        seeded.append(run, e.clone()).await.unwrap();
    }

    // Run 2 (system_prompt "SYS_B" → divergent planner input-hash): resume HALTS with a
    // fatal DeterminismViolation at "e/__plan__" — never a soft node Failed — and the
    // gateway is never touched.
    let (gw2, calls2) = recording_gateway().await;
    let err = Executor::new(Arc::new(gw2), Arc::new(seeded.clone()), "v1")
        .with_registry(planner_reg("SYS_B"))
        .start(run, &graph)
        .await
        .expect_err("a changed planner system_prompt halts the mid-plan resume");
    assert!(
        matches!(&err, OrchestratorError::DeterminismViolation { node, .. } if node.0 == "e/__plan__"),
        "got {err:?}"
    );
    assert_eq!(
        calls2.lock().unwrap().len(),
        0,
        "a determinism violation never touches the gateway"
    );
}
