use super::*;
use crate::test_support::{
    CallLog, content_gated_gateway, demo_reference_gateway, demo_reference_tool_gateway,
    echo_system_gateway, failing_after_gateway, final_response, metered_gateway,
    metered_latency_gateway, recording_gateway, scripted_gateway, tool_call_response,
};
use orchestrator_core::{
    Aggregation, ChildStatus, Dep, EdgeKind, GateSpec, Graph, JournalError, LoopBody, LoopGate,
    MapBody, Node, NodeId, NodeKind,
};
use orchestrator_store::InMemoryJournal;

use crate::agent::tools::{
    AlwaysIndeterminate, Calc, NoteReconciler, ReconcileRegistry, RecordNote, ScopedWriter, Search,
    Tool, ToolContext, ToolRegistry,
};
use orchestrator_core::{
    AgentDefinition, AgentRef, Clock, EffectClass, OrchestratorError, OrchestratorHooks,
    Permissions, Registry,
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

/// The inline value recorded by the `EffectRecorded` for one effect id (these
/// tests wire no `ContentStore`, so every output stays inline). `None` if no such
/// record — used by the gate tests to read a call's denial/allow output.
fn recorded_output(events: &[(Seq, JournalEvent)], eid: &EffectId) -> Option<serde_json::Value> {
    events.iter().find_map(|(_, e)| match e {
        JournalEvent::EffectRecorded {
            effect_id,
            output: EffectOutput::Inline(v),
            ..
        } if effect_id == eid => Some(v.clone()),
        _ => None,
    })
}

/// Whether an `EffectIntent` was journaled for one effect id — a DENIED Mutation
/// never journals one (it skips two-phase), so this is `false` for a denial.
fn has_effect_intent(events: &[(Seq, JournalEvent)], eid: &EffectId) -> bool {
    events
        .iter()
        .any(|(_, e)| matches!(e, JournalEvent::EffectIntent { effect_id: r, .. } if r == eid))
}

/// The `idempotency_key` journaled in the `EffectIntent` for one effect id (`None`
/// if the effect journaled no Intent — e.g. a Pure/Observation call or a denial).
fn intent_key(events: &[(Seq, JournalEvent)], eid: &EffectId) -> Option<String> {
    events.iter().find_map(|(_, e)| match e {
        JournalEvent::EffectIntent {
            effect_id,
            idempotency_key,
            ..
        } if effect_id == eid => Some(idempotency_key.clone()),
        _ => None,
    })
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

/// A demo registry/executor: one agent "a" on the recording chain "c". The agent
/// LISTS the demo `record_note` (Mutation) and `search` (Observation) tools it may
/// call, and the registry carries their specs so `assemble_prompt` compiles them.
/// Both tools carry EMPTY permissions, so the agent's empty grant covers them and
/// the SP-4 s1 authorization gate is transparent for these pre-gate tool tests.
fn agent_registry(chain: &str) -> Arc<Registry> {
    Arc::new(
        Registry::default()
            .with_agent(AgentDefinition {
                tools: vec!["record_note".into(), "search".into()],
                ..agent_def(chain)
            })
            .with_tool(RecordNote::new(Arc::new(std::sync::Mutex::new(Vec::new()))).spec())
            .with_tool(Search::new(Arc::new(AtomicUsize::new(0))).spec()),
    )
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

/// A path-only grant `{paths}` (the shape the SP-4 gate tests hand an agent for
/// the `fs.write` ScopedWriter).
fn path_grant(paths: &[&str]) -> Permissions {
    Permissions {
        paths: paths.iter().map(|p| p.to_string()).collect(),
        ..Default::default()
    }
}

/// A single-agent registry for the SP-4 authorization-gate tests: agent "a" on
/// chain "c" that LISTS `tools` and holds `grants`, with the `fs.write`
/// ScopedWriter *spec* compiled into the prompt (its executable side — the sink —
/// is wired separately via `ToolRegistry`). `with_agent` does NOT validate, so a
/// grant that under-covers a tool is allowed here and enforced at call time.
fn writer_registry(
    tools: Vec<String>,
    grants: std::collections::HashMap<String, Permissions>,
) -> Arc<Registry> {
    Arc::new(
        Registry::default()
            .with_agent(AgentDefinition {
                grants,
                tools,
                ..agent_def("c")
            })
            .with_tool(ScopedWriter::new(Arc::new(std::sync::Mutex::new(Vec::new()))).spec()),
    )
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

/// SP-4 s1 AC4 (authorized): an agent that LISTS `fs.write` AND holds a grant
/// covering the call's concrete path executes the tool normally — the gate is
/// transparent. The Mutation still runs two-phase (Intent→Recorded) and the side
/// effect lands exactly once.
#[tokio::test]
async fn granted_tool_call_executes() {
    let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let writer = Arc::new(ScopedWriter::new(sink.clone()));
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "write it")],
    };
    let grants =
        std::collections::HashMap::from([("fs.write".to_string(), path_grant(&["/workspace"]))]);
    let (gw, calls) = scripted_gateway(vec![
        tool_call_response(
            "t1",
            "fs.write",
            "{\"path\":\"/workspace/a.txt\",\"content\":\"x\"}",
        ),
        final_response("done"),
    ])
    .await;
    let o = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_registry(writer_registry(vec!["fs.write".into()], grants))
        .with_tools(Arc::new(ToolRegistry::default().with_tool(writer)))
        .run(run, &graph)
        .await
        .expect("run completes");

    assert!(o.failed.is_none() && o.paused.is_none(), "{:?}", o.failed);
    assert_eq!(
        calls.lock().unwrap().len(),
        2,
        "two model turns (tool + final)"
    );
    // The tool RAN: the concrete path is in the sink exactly once.
    assert_eq!(
        &*sink.lock().unwrap(),
        &["/workspace/a.txt".to_string()],
        "the granted write reached the tool"
    );

    // The Mutation is two-phase: its Intent immediately precedes its Recorded, at
    // the turn-0 tool effect id.
    let events = journal.load(run).await.unwrap();
    let tool_eid = effect_id("n1", 0, 1);
    assert!(
        has_effect_intent(&events, &tool_eid),
        "a granted Mutation journals an EffectIntent"
    );
    assert_eq!(
        effect_recorded_count(&events, &tool_eid),
        1,
        "and exactly one EffectRecorded"
    );
    // The recorded output is the tool's real result, NOT a denial.
    let out = recorded_output(&events, &tool_eid).expect("tool effect recorded");
    assert_eq!(out["written"], "/workspace/a.txt");
    assert_ne!(out["error"], "permission_denied");
}

/// SP-4 s1 AC5 (unauthorized → fed back): an agent that LISTS `fs.write` but holds
/// NO grant is denied — the tool never runs, the denial is recorded as a Pure
/// effect (NO `EffectIntent`, since the Mutation is skipped) and fed back to the
/// agent, which then finishes. A second variant proves a tool that is NOT listed
/// at all is denied identically.
#[tokio::test]
async fn ungranted_tool_is_denied_and_fed_back() {
    let tool_eid = effect_id("n1", 0, 1);
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "write it")],
    };

    // Variant A: LISTED but no grant → denied (grant does not cover the need).
    {
        let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let (gw, _c) = scripted_gateway(vec![
            tool_call_response(
                "t1",
                "fs.write",
                "{\"path\":\"/workspace/a.txt\",\"content\":\"x\"}",
            ),
            final_response("done"),
        ])
        .await;
        let o = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
            .with_registry(writer_registry(
                vec!["fs.write".into()],
                std::collections::HashMap::new(),
            ))
            .with_tools(Arc::new(
                ToolRegistry::default().with_tool(Arc::new(ScopedWriter::new(sink.clone()))),
            ))
            .run(run, &graph)
            .await
            .expect("run completes despite the denial");

        assert!(o.failed.is_none() && o.paused.is_none(), "{:?}", o.failed);
        // The tool NEVER ran.
        assert!(
            sink.lock().unwrap().is_empty(),
            "an ungranted write must not reach the tool"
        );
        let events = journal.load(run).await.unwrap();
        // The denial IS the call's recorded output, fed back to the agent.
        let out = recorded_output(&events, &tool_eid).expect("denied call still records an effect");
        assert_eq!(out["error"], "permission_denied");
        assert_eq!(out["tool"], "fs.write");
        // A denied Mutation skips two-phase: EffectRecorded, but NO EffectIntent.
        assert_eq!(
            effect_recorded_count(&events, &tool_eid),
            1,
            "the denial is recorded once"
        );
        assert!(
            !has_effect_intent(&events, &tool_eid),
            "a denied Mutation journals NO EffectIntent"
        );
    }

    // Variant B: NOT listed at all → denied for the same reason (fed back, no run).
    {
        let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let grants = std::collections::HashMap::from([(
            "fs.write".to_string(),
            path_grant(&["/workspace"]),
        )]);
        let (gw, _c) = scripted_gateway(vec![
            tool_call_response(
                "t1",
                "fs.write",
                "{\"path\":\"/workspace/a.txt\",\"content\":\"x\"}",
            ),
            final_response("done"),
        ])
        .await;
        // The agent holds a covering grant but does NOT list the tool.
        let o = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
            .with_registry(writer_registry(vec![], grants))
            .with_tools(Arc::new(
                ToolRegistry::default().with_tool(Arc::new(ScopedWriter::new(sink.clone()))),
            ))
            .run(run, &graph)
            .await
            .expect("run completes despite the denial");

        assert!(o.failed.is_none() && o.paused.is_none(), "{:?}", o.failed);
        assert!(
            sink.lock().unwrap().is_empty(),
            "an unlisted tool must not run even with a covering grant"
        );
        let events = journal.load(run).await.unwrap();
        let out = recorded_output(&events, &tool_eid).expect("denied call still records an effect");
        assert_eq!(out["error"], "permission_denied");
        assert!(
            !has_effect_intent(&events, &tool_eid),
            "an unlisted-tool denial journals NO EffectIntent"
        );
    }
}

/// Drives the narrow-grant "denied → adapt → succeed" journey once and hands back
/// the pieces the AC6 (structural) and AC9 (fed-back payload) tests each assert
/// over. Grant = `{paths:["/workspace"]}`; script: `fs.write /etc/passwd` (denied)
/// → `fs.write /workspace/ok.txt` (allowed) → final `"done"`. Returns the run
/// outcome, the loaded journal events, the `ScopedWriter` sink handle, and the
/// scripted-gateway call log — a PURE setup extraction (no assertions), so both
/// callers observe byte-identical behavior.
async fn drive_adapt_and_succeed() -> (
    RunOutcome,
    Vec<(Seq, JournalEvent)>,
    Arc<std::sync::Mutex<Vec<String>>>,
    CallLog,
) {
    let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "write it")],
    };
    let grants =
        std::collections::HashMap::from([("fs.write".to_string(), path_grant(&["/workspace"]))]);
    let (gw, calls) = scripted_gateway(vec![
        // turn 0: out-of-grant path → denied + fed back.
        tool_call_response(
            "t1",
            "fs.write",
            "{\"path\":\"/etc/passwd\",\"content\":\"x\"}",
        ),
        // turn 1: in-grant path → allowed, runs the tool.
        tool_call_response(
            "t2",
            "fs.write",
            "{\"path\":\"/workspace/ok.txt\",\"content\":\"x\"}",
        ),
        // turn 2: final answer → the run completes.
        final_response("done"),
    ])
    .await;
    let o = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_registry(writer_registry(vec!["fs.write".into()], grants))
        .with_tools(Arc::new(
            ToolRegistry::default().with_tool(Arc::new(ScopedWriter::new(sink.clone()))),
        ))
        .run(run, &graph)
        .await
        .expect("run completes");
    let events = journal.load(run).await.unwrap();
    (o, events, sink, calls)
}

/// SP-4 s1 AC6 (per-argument): with a `/workspace` grant, an out-of-grant call
/// (`/etc/passwd`) is denied and fed back, then an in-grant call
/// (`/workspace/ok.txt`) on a later turn succeeds — proving the gate authorizes
/// each call against its CONCRETE args, not the tool's coarse static surface. The
/// denial and the allow land at DISTINCT tool effect ids. (Completion / turn-count
/// facets of the same journey are asserted by `agent_hits_a_denial_adapts_and_succeeds`.)
#[tokio::test]
async fn out_of_grant_argument_denied_then_in_grant_succeeds() {
    let (_o, events, sink, _calls) = drive_adapt_and_succeed().await;

    // Only the in-grant write reached the tool.
    assert_eq!(
        &*sink.lock().unwrap(),
        &["/workspace/ok.txt".to_string()],
        "the out-of-grant write was denied; only the in-grant one ran"
    );

    let denied_eid = effect_id("n1", 0, 1); // turn-0 tool call
    let allowed_eid = effect_id("n1", 1, 1); // turn-1 tool call
    assert_ne!(denied_eid, allowed_eid, "distinct tool effect ids");

    // The denial: a Pure EffectRecorded, NO EffectIntent.
    let denied = recorded_output(&events, &denied_eid).expect("denied call recorded");
    assert_eq!(denied["error"], "permission_denied");
    assert!(
        !has_effect_intent(&events, &denied_eid),
        "the denied out-of-grant Mutation journals NO EffectIntent"
    );

    // The allow: a two-phase Mutation — EffectIntent + EffectRecorded with the real
    // tool output.
    assert!(
        has_effect_intent(&events, &allowed_eid),
        "the in-grant Mutation journals an EffectIntent"
    );
    let allowed = recorded_output(&events, &allowed_eid).expect("allowed call recorded");
    assert_eq!(allowed["written"], "/workspace/ok.txt");
}

/// SP-4 s1 AC7 (resume determinism): a DENIED call is journaled ONCE as a Pure
/// `EffectRecorded` and, on resume, REPLAYS from that memo — the tool is never
/// invoked (a fresh sink stays empty) and no second denial is recorded. Because
/// the gate is a pure fn of (grant, args), re-running it on resume would re-deny
/// too — so "tool never invoked" alone can't distinguish a memo replay from a
/// live re-deny (both leave the sink empty). The load-bearing proof is therefore
/// the COUNT: the denial `EffectRecorded` for the tool effect id appears EXACTLY
/// ONCE across the whole final journal (recorded live in run 1, replayed — not
/// re-recorded — in run 2). Disabling the memo replay makes the resume re-record
/// the denial (count → 2), failing the load-bearing assertion.
#[tokio::test]
async fn a_denied_call_replays_from_the_memo_on_resume() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "write it")],
    };
    let denied_eid = effect_id("n1", 0, 1); // turn-0 tool call

    // Run 1 (seed a PARTIAL run): the agent LISTS `fs.write` with an EMPTY grant, so
    // turn 0's `fs.write` is DENIED (Pure EffectRecorded, no tool run); the script
    // then runs out on turn 1 → the node fails → NO RunCompleted. The denial is
    // journaled exactly once, live.
    let sink1 = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let (gw1, _c1) = scripted_gateway(vec![tool_call_response(
        "t1",
        "fs.write",
        "{\"path\":\"/workspace/a.txt\",\"content\":\"x\"}",
    )])
    .await;
    let o1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_registry(writer_registry(
            vec!["fs.write".into()],
            std::collections::HashMap::new(),
        ))
        .with_tools(Arc::new(
            ToolRegistry::default().with_tool(Arc::new(ScopedWriter::new(sink1.clone()))),
        ))
        .run(run, &graph)
        .await
        .expect("seed yields an outcome");
    assert!(
        o1.failed.is_some(),
        "seed dies at turn 1 (script exhausted)"
    );
    assert!(
        sink1.lock().unwrap().is_empty(),
        "the denied write never reached the tool, even live"
    );
    let seeded = journal.load(run).await.unwrap();
    assert_eq!(
        effect_recorded_count(&seeded, &denied_eid),
        1,
        "the denial is recorded once, live, in run 1"
    );

    // Run 2 (resume over the SAME journal, FRESH gateway + FRESH sink): turn 0's model
    // turn and its `fs.write` both MEMO-HIT (the denial replays from the journal — the
    // gate/tool are NOT re-reached), so the fresh sink stays empty; turn 1 is driven
    // live to a final answer and the run completes.
    let sink2 = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let (gw2, _c2) = scripted_gateway(vec![final_response("done")]).await;
    let o2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_registry(writer_registry(
            vec!["fs.write".into()],
            std::collections::HashMap::new(),
        ))
        .with_tools(Arc::new(
            ToolRegistry::default().with_tool(Arc::new(ScopedWriter::new(sink2.clone()))),
        ))
        .start(run, &graph)
        .await
        .expect("resume yields an outcome");

    assert!(
        o2.failed.is_none() && o2.paused.is_none(),
        "resume reaches the same completed state: {:?}",
        o2.failed
    );
    assert!(
        sink2.lock().unwrap().is_empty(),
        "the denial replayed from the memo — the tool was NOT re-invoked on resume"
    );

    // Load-bearing (mutation-verifiable): the denial `EffectRecorded` appears EXACTLY
    // ONCE across BOTH runs — live in run 1, replayed (not re-recorded) in run 2 — and
    // a denied Mutation journals NO `EffectIntent`.
    let events = journal.load(run).await.unwrap();
    assert_eq!(
        effect_recorded_count(&events, &denied_eid),
        1,
        "the denial is journaled once total: recorded live, then replayed from the memo"
    );
    assert!(
        !has_effect_intent(&events, &denied_eid),
        "a denied Mutation never journals an EffectIntent, on either run"
    );
}

/// SP-4 s1 AC9 (end-to-end adapt-and-succeed): the AC6 journey — narrow
/// `/workspace` grant → out-of-scope call denied+fed back → in-scope call succeeds
/// → run completes — is driven end-to-end. The structural half of this journey
/// (distinct eids, denied-has-no-Intent, allowed-has-Intent, sink) is already
/// proven by `out_of_grant_argument_denied_then_in_grant_succeeds`; this test adds
/// the COMPLEMENTARY, uncovered half: the exact denial value the agent's transcript
/// receives (agent.rs feeds `record_denied_effect`'s value back as the tool result)
/// is self-describing yet TERSE — it names the tool and a reason but NEVER
/// enumerates the grant/allowlist, so a denied model can't be redirected onto
/// another granted resource (confused-deputy / injection defense).
#[tokio::test]
async fn agent_hits_a_denial_adapts_and_succeeds() {
    let (o, events, sink, calls) = drive_adapt_and_succeed().await;

    // The journey succeeded end-to-end: the model was re-invoked after the denial
    // (3 turns) and only the adapted, in-grant write reached the tool.
    assert!(o.failed.is_none() && o.paused.is_none(), "{:?}", o.failed);
    assert_eq!(
        calls.lock().unwrap().len(),
        3,
        "the denial was fed back — the loop continued to the adapted call and a final turn"
    );
    assert_eq!(
        &*sink.lock().unwrap(),
        &["/workspace/ok.txt".to_string()],
        "the agent adapted: only the in-grant write ran"
    );

    // The exact value fed back to the agent on the denied call is self-describing…
    let denied_eid = effect_id("n1", 0, 1); // turn-0 tool call
    let denied = recorded_output(&events, &denied_eid).expect("denied call recorded");
    assert_eq!(denied["error"], "permission_denied");
    assert_eq!(denied["tool"], "fs.write");
    assert!(
        denied["detail"].as_str().is_some_and(|d| !d.is_empty()),
        "the fed-back denial carries a non-empty, model-facing reason"
    );
    // …yet TERSE: the fed-back value must not enumerate the grant/allowlist — leaking
    // `/workspace` would invite a redirect onto another granted resource.
    // "/workspace" is the SOLE granted path in this fixture — a terse denial must not
    // echo it (confused-deputy). If a second grant path is added above, assert against each.
    assert!(
        !denied.to_string().contains("/workspace"),
        "the fed-back denial must not leak the grant (confused-deputy defense)"
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

/// A local Mutation probe tool for the SP-4 s5 idempotency-key threading tests. Its
/// `call_ctx` CAPTURES the `ctx.idempotency_key` the executor threads in (so a test can
/// assert the tool received the SAME key journaled in the effect's `EffectIntent`), and
/// it can optionally OVERRIDE `Tool::idempotency_key` to return an author key read from
/// `args["ref"]` (so a test can assert an author key overrides the structural default).
/// Empty permissions ⇒ the agent's empty grant covers it and the SP-4 s1 gate is
/// transparent.
struct KeyProbe {
    /// The last `ctx.idempotency_key` threaded into `call_ctx` (`None` until called).
    seen: Arc<std::sync::Mutex<Option<String>>>,
    /// The last `ctx.effect_id` threaded into `call_ctx` (`None` until called) — lets a test
    /// pin AC2's clause that the ToolContext's effect id IS the call's teid.
    seen_eid: Arc<std::sync::Mutex<Option<EffectId>>>,
    /// When set, `idempotency_key(args)` returns `args["ref"]` — an author override.
    author_key: bool,
}
impl KeyProbe {
    fn new(
        seen: Arc<std::sync::Mutex<Option<String>>>,
        seen_eid: Arc<std::sync::Mutex<Option<EffectId>>>,
        author_key: bool,
    ) -> Self {
        Self {
            seen,
            seen_eid,
            author_key,
        }
    }
}
impl Tool for KeyProbe {
    fn spec(&self) -> orchestrator_core::ToolSpec {
        orchestrator_core::ToolSpec {
            name: "mut_probe".into(),
            description: Some("a Mutation probe that captures its threaded idempotency key".into()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": { "ref": {"type": "string"} }
            }),
            effect_class: EffectClass::Mutation,
            ttl_secs: None,
            source: None,
            permissions: Permissions::default(),
            activation: orchestrator_core::Activation::default(),
            credentials: vec![],
        }
    }
    fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        Ok(serde_json::json!({ "ok": true }))
    }
    fn call_ctx(
        &self,
        args: serde_json::Value,
        ctx: &ToolContext,
    ) -> Result<serde_json::Value, OrchestratorError> {
        *self.seen.lock().unwrap() = Some(ctx.idempotency_key.clone());
        *self.seen_eid.lock().unwrap() = Some(ctx.effect_id.clone());
        self.call(args)
    }
    fn idempotency_key(&self, args: &serde_json::Value) -> Option<String> {
        if self.author_key {
            args.get("ref").and_then(|v| v.as_str()).map(str::to_string)
        } else {
            None
        }
    }
}

/// Registry whose agent "a" (chain "c") LISTS the `mut_probe` Mutation tool, with its
/// spec compiled into the prompt. Empty grant ⇒ the SP-4 s1 gate is transparent.
fn probe_registry() -> Arc<Registry> {
    Arc::new(
        Registry::default()
            .with_agent(AgentDefinition {
                tools: vec!["mut_probe".into()],
                ..agent_def("c")
            })
            .with_tool(
                KeyProbe::new(
                    Arc::new(std::sync::Mutex::new(None)),
                    Arc::new(std::sync::Mutex::new(None)),
                    false,
                )
                .spec(),
            ),
    )
}

/// SP-4 s5 (AC2/AC3): the executor threads the JOURNALED idempotency key into the tool
/// via `call_ctx` — the key the tool receives is byte-identical to the one journaled in
/// the effect's `EffectIntent`. (A default tool, so the key is the structural one; the
/// point here is that the threaded key and the journaled key are the SAME string.)
#[tokio::test]
async fn tool_receives_the_journaled_idempotency_key() {
    let seen = Arc::new(std::sync::Mutex::new(None::<String>));
    let seen_eid = Arc::new(std::sync::Mutex::new(None::<EffectId>));
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "probe it")],
    };
    let (gw, _c) = scripted_gateway(vec![
        tool_call_response("t1", "mut_probe", "{}"),
        final_response("done"),
    ])
    .await;
    let o = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_registry(probe_registry())
        .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(
            KeyProbe::new(seen.clone(), seen_eid.clone(), false),
        ))))
        .run(run, &graph)
        .await
        .expect("run completes");
    assert!(o.failed.is_none() && o.paused.is_none(), "{:?}", o.failed);

    let tool_eid = effect_id("n1", 0, 1);
    let events = journal.load(run).await.unwrap();
    let journaled = intent_key(&events, &tool_eid).expect("the Mutation journaled an EffectIntent");
    let received = seen
        .lock()
        .unwrap()
        .clone()
        .expect("the tool's call_ctx ran");
    assert_eq!(
        received, journaled,
        "the tool receives EXACTLY the idempotency key journaled in the EffectIntent"
    );
    // AC2: the ToolContext's `effect_id` is the call's actual teid — the same id the
    // EffectIntent/EffectRecorded are keyed on (so a tool can correlate its external call).
    let received_eid = seen_eid
        .lock()
        .unwrap()
        .clone()
        .expect("the tool's call_ctx ran");
    assert_eq!(
        received_eid, tool_eid,
        "the tool receives EXACTLY the call's effect id in its ToolContext"
    );
}

/// SP-4 s5 (AC1/AC2): a tool that OVERRIDES `Tool::idempotency_key` (an author/domain
/// key derived from args) makes that key the effective one — it is journaled in the
/// `EffectIntent` AND threaded to the tool via `call_ctx`, overriding the structural
/// default.
#[tokio::test]
async fn author_supplied_key_is_journaled_and_threaded() {
    let seen = Arc::new(std::sync::Mutex::new(None::<String>));
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "probe it")],
    };
    let (gw, _c) = scripted_gateway(vec![
        tool_call_response("t1", "mut_probe", "{\"ref\":\"bk-42\"}"),
        final_response("done"),
    ])
    .await;
    let o = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_registry(probe_registry())
        .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(
            KeyProbe::new(seen.clone(), Arc::new(std::sync::Mutex::new(None)), true),
        ))))
        .run(run, &graph)
        .await
        .expect("run completes");
    assert!(o.failed.is_none() && o.paused.is_none(), "{:?}", o.failed);

    let tool_eid = effect_id("n1", 0, 1);
    let events = journal.load(run).await.unwrap();
    let journaled = intent_key(&events, &tool_eid).expect("the Mutation journaled an EffectIntent");
    assert_eq!(
        journaled, "bk-42",
        "the author key is journaled as the effective idempotency key"
    );
    let received = seen
        .lock()
        .unwrap()
        .clone()
        .expect("the tool's call_ctx ran");
    assert_eq!(
        received, "bk-42",
        "and the SAME author key is threaded to the tool via call_ctx"
    );
}

/// SP-4 s5 (AC1, additivity): a Mutation tool with NO `idempotency_key` override
/// journals the STRUCTURAL key `sha256(effect_id | args_hash)` — byte-identical to the
/// pre-s5 behavior — and threads that same key to the tool. This is the crux that keeps
/// the existing in-doubt/reconcile tests green: a default tool journals exactly what a
/// recompute would have produced.
#[tokio::test]
async fn default_tool_journals_the_structural_key() {
    let seen = Arc::new(std::sync::Mutex::new(None::<String>));
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "probe it")],
    };
    let args = "{}";
    let (gw, _c) = scripted_gateway(vec![
        tool_call_response("t1", "mut_probe", args),
        final_response("done"),
    ])
    .await;
    let o = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_registry(probe_registry())
        .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(
            KeyProbe::new(seen.clone(), Arc::new(std::sync::Mutex::new(None)), false),
        ))))
        .run(run, &graph)
        .await
        .expect("run completes");
    assert!(o.failed.is_none() && o.paused.is_none(), "{:?}", o.failed);

    let tool_eid = effect_id("n1", 0, 1);
    let tih = super::support::tool_input_hash("mut_probe", args);
    let structural = orchestrator_core::idempotency_key(&tool_eid, &tih);
    let events = journal.load(run).await.unwrap();
    let journaled = intent_key(&events, &tool_eid).expect("the Mutation journaled an EffectIntent");
    assert_eq!(
        journaled, structural,
        "a default Mutation journals the structural sha256(effect_id | args_hash), byte-identical to pre-s5"
    );
    let received = seen
        .lock()
        .unwrap()
        .clone()
        .expect("the tool's call_ctx ran");
    assert_eq!(
        received, structural,
        "and threads that SAME structural key to the tool"
    );
}

/// A shared, keyed "external system" the demo Mutation writes to (SP-4 s5, AC5). Keys
/// are the effective idempotency key; values are the recorded side-effect output.
type Store = std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, serde_json::Value>>>;

/// Demo Mutation tool with provider-side idempotency: writes to a keyed "external
/// system" under `ctx.idempotency_key`; re-applying the same key is a no-op returning
/// the recorded output. `calls` counts REAL applications (dedup MISSES; dedup hits do NOT
/// count); `invocations` counts EVERY `call_ctx` ENTRY (before the dedup check) — so a test
/// can prove the tool was NOT re-invoked on resume even though the store would have absorbed
/// a wrong re-invocation (leaving `calls` reading 1 either way). Empty permissions ⇒ the
/// agent's empty grant covers it and the SP-4 s1 gate is transparent.
struct IdempotentStore {
    store: Store,
    calls: Arc<AtomicUsize>,
    invocations: Arc<AtomicUsize>,
}
impl Tool for IdempotentStore {
    fn spec(&self) -> orchestrator_core::ToolSpec {
        orchestrator_core::ToolSpec {
            name: "store".into(),
            description: Some("Write to a keyed external system with provider-side dedup".into()),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": { "item": {"type": "string"} }
            }),
            effect_class: EffectClass::Mutation,
            ttl_secs: None,
            source: None,
            permissions: Permissions::default(),
            activation: orchestrator_core::Activation::default(),
            credentials: vec![],
        }
    }
    fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        // The executor always drives `call_ctx`; a bare `call` would lose the key, so
        // fail closed rather than apply the mutation without provider-side dedup.
        Err(OrchestratorError::Tool {
            tool: "store".into(),
            message: "needs ctx".into(),
        })
    }
    fn call_ctx(
        &self,
        args: serde_json::Value,
        ctx: &ToolContext,
    ) -> Result<serde_json::Value, OrchestratorError> {
        // Count EVERY entry (before dedup) so a Confirmed resume that WRONGLY re-invokes the
        // tool is caught — the store's dedup would otherwise mask it (`calls` stays 1).
        self.invocations.fetch_add(1, Ordering::SeqCst);
        let mut s = self.store.lock().unwrap();
        if let Some(existing) = s.get(&ctx.idempotency_key) {
            return Ok(existing.clone()); // provider-side dedup: no second effect
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        let out = serde_json::json!({ "stored": args });
        s.insert(ctx.idempotency_key.clone(), out.clone());
        Ok(out)
    }
    fn idempotency_key(&self, args: &serde_json::Value) -> Option<String> {
        // Author/domain key when the caller supplies `ref` (a booking ref); absent ⇒ None ⇒
        // the executor uses the STRUCTURAL key. So a `{"item": …}` drive stays structural
        // (the structural-key exactly-once tests are unaffected); only a `{"ref": …}` drive
        // overrides to the author key.
        args.get("ref").and_then(|v| v.as_str()).map(str::to_string)
    }
}

/// Reconcile provider paired with `IdempotentStore`: on an in-doubt resume, query the
/// keyed "external system" under the JOURNALED key — a status query, never a re-run. Key
/// present ⇒ the side effect already landed (`Confirmed` with its recorded output); key
/// absent ⇒ it did not (`NotApplied`). Never guesses ⇒ never `Indeterminate`.
struct StatusQueryReconciler {
    store: Store,
}
#[async_trait::async_trait]
impl orchestrator_core::ReconcileProvider for StatusQueryReconciler {
    async fn reconcile(
        &self,
        idempotency_key: &str,
        _args: &serde_json::Value,
    ) -> Result<orchestrator_core::ReconcileOutcome, OrchestratorError> {
        match self.store.lock().unwrap().get(idempotency_key) {
            Some(out) => Ok(orchestrator_core::ReconcileOutcome::Confirmed(out.clone())),
            None => Ok(orchestrator_core::ReconcileOutcome::NotApplied),
        }
    }
}

/// Registry whose agent "a" (chain "c") LISTS the `store` Mutation tool, with its spec
/// compiled into the prompt. Empty grant ⇒ the SP-4 s1 gate is transparent. The spec-only
/// instance's store/calls are throwaway (only `.spec()` is read).
fn store_registry() -> Arc<Registry> {
    Arc::new(
        Registry::default()
            .with_agent(AgentDefinition {
                tools: vec!["store".into()],
                ..agent_def("c")
            })
            .with_tool(
                IdempotentStore {
                    store: Store::default(),
                    calls: Arc::new(AtomicUsize::new(0)),
                    invocations: Arc::new(AtomicUsize::new(0)),
                }
                .spec(),
            ),
    )
}

/// Build a journal in the in-doubt state for the `store` Mutation (mirrors
/// `seed_in_doubt_note`): a real run through the two-phase Mutation over the caller-owned
/// `store`/`calls`, truncated to the prefix up to and including the effect's `EffectIntent`
/// — so the resume sees an Intent with no matching `EffectRecorded`. The live seed applies
/// the effect (`store[key]` written, `calls == 1`); the caller decides — by resuming over
/// the SAME store or a fresh empty one — whether the side effect "survived the crash".
/// `args` is the tool-call payload (JSON string): `{"item": …}` ⇒ a structural key,
/// `{"ref": …}` ⇒ an author key (see `IdempotentStore::idempotency_key`).
async fn seed_in_doubt_store(
    store: Store,
    calls: Arc<AtomicUsize>,
    invocations: Arc<AtomicUsize>,
    args: &str,
) -> (InMemoryJournal, RunId) {
    let full = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "store it")],
    };
    let (gw, _c) = scripted_gateway(vec![
        tool_call_response("t1", "store", args),
        final_response("done"),
    ])
    .await;
    Executor::new(Arc::new(gw), Arc::new(full.clone()), "v1")
        .with_registry(store_registry())
        .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(
            IdempotentStore {
                store,
                calls,
                invocations,
            },
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

/// Resume an in-doubt `store` run with a caller-owned `store`/`calls` (whose contents
/// model whether the side effect survived the crash) + `reconcilers`; return the outcome
/// and journal events. Mirrors `resume_in_doubt` for the keyed-store demo.
async fn resume_in_doubt_store(
    journal: InMemoryJournal,
    run: RunId,
    store: Store,
    calls: Arc<AtomicUsize>,
    invocations: Arc<AtomicUsize>,
    reconcilers: ReconcileRegistry,
) -> (RunOutcome, Vec<(Seq, JournalEvent)>) {
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "store it")],
    };
    let (gw, _c) = scripted_gateway(vec![final_response("done")]).await;
    let out = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_registry(store_registry())
        .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(
            IdempotentStore {
                store,
                calls,
                invocations,
            },
        ))))
        .with_reconcilers(Arc::new(reconcilers))
        .start(run, &graph)
        .await
        .expect("resume yields an outcome");
    let events = journal.load(run).await.unwrap();
    (out, events)
}

/// SP-4 s5 (AC5) — exactly-once, Confirmed-by-key: the side effect DID apply before the
/// crash (the live seed wrote `store[key]`, `calls == 1`), then the journal was truncated
/// before the `EffectRecorded` (in-doubt). A FRESH executor resumes sharing the SAME
/// `store` + `calls` + a `StatusQueryReconciler` over that store. The reconciler finds the
/// key → `Confirmed` → the executor records WITHOUT re-running: `calls` stays `1`, the
/// store holds exactly one entry, no `DeterminismViolation`. The side effect happened
/// exactly once across crash+resume.
#[tokio::test]
async fn exactly_once_confirmed_by_key_does_not_double_apply() {
    let store_eid = effect_id("n1", 0, 1);
    // Shared across crash+resume: the live seed applies the effect into THIS store.
    let store: Store = Store::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let invocations = Arc::new(AtomicUsize::new(0));
    let (journal, run) = seed_in_doubt_store(
        store.clone(),
        calls.clone(),
        invocations.clone(),
        "{\"item\":\"widget\"}",
    )
    .await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the live seed applied the side effect once before the crash"
    );
    assert_eq!(
        store.lock().unwrap().len(),
        1,
        "the seed wrote exactly one keyed entry"
    );

    // Resume over the SAME store (the effect survived the crash) with a status-query
    // reconciler that reads it.
    let reconcilers = ReconcileRegistry::default().with_provider(
        "store",
        Arc::new(StatusQueryReconciler {
            store: store.clone(),
        }),
    );
    let (out, events) = resume_in_doubt_store(
        journal,
        run,
        store.clone(),
        calls.clone(),
        invocations.clone(),
        reconcilers,
    )
    .await;

    assert!(
        out.failed.is_none() && out.paused.is_none(),
        "Confirmed completes with no DeterminismViolation: {:?}",
        out.failed
    );
    assert_eq!(
        invocations.load(Ordering::SeqCst),
        1,
        "the tool's call_ctx was NOT re-invoked on resume (Confirmed records from the reconciler, not the tool)"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "exactly-once: Confirmed-by-key records without re-applying — calls stays 1"
    );
    assert_eq!(
        store.lock().unwrap().len(),
        1,
        "the external system still holds exactly one entry for the key (no double-apply)"
    );
    assert_eq!(
        effect_recorded_count(&events, &store_eid),
        1,
        "Confirmed appends the Mutation's EffectRecorded exactly once"
    );
    assert!(
        events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "the run completes"
    );
}

/// SP-4 s5 (AC5 + D3, author key) — exactly-once with an AUTHOR key: driving the tool with
/// `{"ref":"bk-77"}` makes `bk-77` the EFFECTIVE key journaled in the `EffectIntent`. The
/// live seed applies the effect under `store["bk-77"]`, then the journal is truncated before
/// `EffectRecorded` (in-doubt). A FRESH executor resumes sharing the SAME store + a
/// `StatusQueryReconciler`. Because reconcile READS the journaled key (no structural
/// recompute — the D3 "no drift for author keys" rationale), it queries by `bk-77`, finds it
/// → `Confirmed` → records WITHOUT re-running: `invocations == 1`, `calls == 1`, one store
/// entry, `RunCompleted`. Proves the author key survived the crash AND drove reconcile.
#[tokio::test]
async fn exactly_once_author_key_confirmed_does_not_double_apply() {
    let store_eid = effect_id("n1", 0, 1);
    // Shared across crash+resume; `{"ref":"bk-77"}` ⇒ the effective key is the AUTHOR key.
    let store: Store = Store::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let invocations = Arc::new(AtomicUsize::new(0));
    let (journal, run) = seed_in_doubt_store(
        store.clone(),
        calls.clone(),
        invocations.clone(),
        "{\"ref\":\"bk-77\"}",
    )
    .await;

    // The AUTHOR key — not a structural hash — is what got journaled in the Intent, so it is
    // what survives the crash into the resume.
    let seeded = journal.load(run).await.unwrap();
    assert_eq!(
        intent_key(&seeded, &store_eid).as_deref(),
        Some("bk-77"),
        "the author key is journaled in the EffectIntent (no structural recompute)"
    );
    assert!(
        store.lock().unwrap().contains_key("bk-77"),
        "the seed applied the effect under the author key"
    );

    // Resume over the SAME store with a status-query reconciler that reads it.
    let reconcilers = ReconcileRegistry::default().with_provider(
        "store",
        Arc::new(StatusQueryReconciler {
            store: store.clone(),
        }),
    );
    let (out, events) = resume_in_doubt_store(
        journal,
        run,
        store.clone(),
        calls.clone(),
        invocations.clone(),
        reconcilers,
    )
    .await;

    assert!(
        out.failed.is_none() && out.paused.is_none(),
        "Confirmed completes with no DeterminismViolation: {:?}",
        out.failed
    );
    assert_eq!(
        invocations.load(Ordering::SeqCst),
        1,
        "the tool's call_ctx was NOT re-invoked on resume (reconcile confirmed by the author key)"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "exactly-once: author-keyed Confirmed records without re-applying — calls stays 1"
    );
    assert_eq!(
        store.lock().unwrap().len(),
        1,
        "the external system still holds exactly one entry for bk-77 (no double-apply)"
    );
    assert_eq!(
        effect_recorded_count(&events, &store_eid),
        1,
        "Confirmed appends the Mutation's EffectRecorded exactly once"
    );
    assert!(
        events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "the run completes"
    );
}

/// SP-4 s5 (AC5) — exactly-once, NotApplied-runs-once: the side effect did NOT apply
/// before the crash. We resume over a fresh EMPTY store (a different keyed system than the
/// throwaway seed store), so the status-query reconciler finds the key absent → `NotApplied`
/// → the tool runs now, exactly once (`calls == 1`, the store gains the key), under the
/// standing Intent (no second `EffectIntent`). Still exactly once.
#[tokio::test]
async fn exactly_once_not_applied_runs_the_effect_once() {
    // Seed the in-doubt Intent over a THROWAWAY store; its write is discarded by resuming
    // over a fresh empty store (models "the crash was before the side effect landed").
    let (journal, run) = seed_in_doubt_store(
        Store::default(),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        "{\"item\":\"widget\"}",
    )
    .await;

    let store: Store = Store::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let invocations = Arc::new(AtomicUsize::new(0));
    let reconcilers = ReconcileRegistry::default().with_provider(
        "store",
        Arc::new(StatusQueryReconciler {
            store: store.clone(),
        }),
    );
    let (out, events) = resume_in_doubt_store(
        journal,
        run,
        store.clone(),
        calls.clone(),
        invocations.clone(),
        reconcilers,
    )
    .await;

    assert!(
        out.failed.is_none() && out.paused.is_none(),
        "NotApplied completes: {:?}",
        out.failed
    );
    assert_eq!(
        invocations.load(Ordering::SeqCst),
        1,
        "NotApplied invoked the tool exactly once on resume (fresh counter ⇒ only this run's application)"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "exactly-once: NotApplied runs the effect exactly once on resume"
    );
    assert_eq!(
        store.lock().unwrap().len(),
        1,
        "the effect now holds exactly one entry for the key"
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
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "the run completes"
    );
}

/// SP-4 s5 (AC7, preserved) — absent provider still pauses: an in-doubt `store` Mutation
/// with NO `ReconcileProvider` registered resolves to `Indeterminate` → the run pauses
/// loud (`RunPaused` journaled, `outcome.paused` set), applies NOTHING (the fresh resume
/// store stays empty, `calls == 0`), and does not complete. The R3 mandatory-human path is
/// unchanged by s5's exactly-once machinery.
#[tokio::test]
async fn absent_provider_for_in_doubt_mutation_pauses() {
    let (journal, run) = seed_in_doubt_store(
        Store::default(),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        "{\"item\":\"widget\"}",
    )
    .await;

    // Fresh empty resume store + NO provider registered for "store".
    let store: Store = Store::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let invocations = Arc::new(AtomicUsize::new(0));
    let reconcilers = ReconcileRegistry::default();
    let (out, events) = resume_in_doubt_store(
        journal,
        run,
        store.clone(),
        calls.clone(),
        invocations.clone(),
        reconcilers,
    )
    .await;

    let pause = out.paused.expect("an absent provider pauses the run");
    assert_eq!(pause.node, NodeId("n1".into()));
    assert_eq!(
        invocations.load(Ordering::SeqCst),
        0,
        "the tool's call_ctx is never entered while paused"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "a paused in-doubt Mutation applies no side effect"
    );
    assert!(
        store.lock().unwrap().is_empty(),
        "the external system is untouched while paused"
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
                // SP-DATA-5 mechanical fix: the `..` above doesn't capture `usage`, and this
                // test doesn't exercise it — always None here.
                usage: None,
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
        JournalEvent::PlannerSelected { node, agent } => {
            format!("PlannerSelected({}->{})", node.0, agent.0)
        }
        JournalEvent::ContextWrite { key, .. } => format!("ContextWrite({})", key.0),
        JournalEvent::RunCompleted => "RunCompleted".to_string(),
        JournalEvent::RunPaused { .. } => "RunPaused".to_string(),
        // SP-DATA-5 Task 2 gives this a real label once BudgetRaised is exercised.
        JournalEvent::BudgetRaised { .. } => "BudgetRaised".to_string(),
        JournalEvent::SignalAwaited { node, .. } => format!("SignalAwaited({})", node.0),
        JournalEvent::SignalReceived { node, .. } => format!("SignalReceived({})", node.0),
        // SP-6 s2 (Task 1): no test exercises these yet (Task 5 adds the executor
        // node that journals them), but this labeler is exhaustive by design — an
        // explicit arm now means a future test gets a real label instead of a
        // compile error silently avoided by a wildcard.
        JournalEvent::GateAwaited { node, .. } => format!("GateAwaited({})", node.0),
        JournalEvent::GateDecided { node, .. } => format!("GateDecided({})", node.0),
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
                // SP-DATA-5: this test doesn't exercise a budget.
                budget: None,
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
                // SP-DATA-5 Task 4 threads real usage through here; this test doesn't
                // exercise it.
                usage: None,
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
                // SP-DATA-5: this test doesn't exercise a budget.
                budget: None,
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
                body: LoopBody::ModelCall { chain: "c".into() },
                input: serde_json::json!({ "prompt": "start" }),
                gate: GateSpec::Pure(LoopGate::TextContains("DONE".into())),
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
                body: LoopBody::ModelCall { chain: "c".into() },
                input: serde_json::json!({ "prompt": "go" }),
                gate: GateSpec::Pure(LoopGate::TextContains("STOP".into())),
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
                body: LoopBody::ModelCall { chain: "c".into() },
                input: serde_json::json!({ "prompt": "FAIL" }),
                gate: GateSpec::Pure(LoopGate::TextContains("never".into())),
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
                body: LoopBody::Agent(AgentRef("a".into())),
                input: serde_json::json!("start"),
                gate: GateSpec::Pure(LoopGate::TextContains("NEVER".into())),
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
                body: LoopBody::ModelCall { chain: "c".into() },
                input: serde_json::json!({ "prompt": "start" }),
                gate: GateSpec::Pure(LoopGate::TextContains("NEVER".into())),
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
                    body: LoopBody::ModelCall { chain: "c".into() },
                    input: serde_json::json!({ "prompt": "go" }),
                    gate: GateSpec::Pure(LoopGate::TextContains("STOP".into())), // never fires → cap at 2
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
                // SP-DATA-5 mechanical fix: the `..` above doesn't capture `usage`, and this
                // test doesn't exercise it — always None here.
                usage: None,
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
                body: LoopBody::ModelCall {
                    chain: "research.bulk".into(),
                },
                input: serde_json::json!({ "prompt": "iterate" }),
                gate: GateSpec::Pure(LoopGate::TextContains("NEVER".into())),
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

// ===================== SP-3 s5 Loop over a Subgraph body =======================

/// AC2 / AC9 — a Loop over a Subgraph body drives the authored graph fresh each
/// iteration; a pure gate that never matches the sink map runs to `max_iters` and
/// completes best-effort → `{iterations:2, converged:false, output:<sink map>}`.
/// Each iteration's inner node is journaled under `"lp/{i}/…"`.
#[tokio::test]
async fn loop_over_a_subgraph_body_iterates_and_stops() {
    let (gateway, _c) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    // Subgraph: a single ModelCall node "s1" whose output is `{model, text}`.
    let inner = Graph {
        nodes: vec![mc("s1", None)],
    };
    let loop_node = Node {
        id: NodeId("lp".into()),
        kind: NodeKind::Loop {
            body: LoopBody::Subgraph(Box::new(inner)),
            input: serde_json::json!({}),
            // The gate inspects the SINK MAP (`{"s1": {model,text}}`), which has no
            // top-level "text" string → never fires → best-effort at the cap.
            gate: GateSpec::Pure(LoopGate::TextContains("zzz-never".into())),
            max_iters: 2,
        },
        deps: vec![],
    };
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let out = exec
        .run(
            run,
            &Graph {
                nodes: vec![loop_node],
            },
        )
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    let o = &out.outputs[&NodeId("lp".into())];
    assert_eq!(o["converged"], false, "gate never matched → best-effort");
    assert_eq!(o["iterations"], 2, "ran max_iters");
    // The iteration output is the subgraph's SINK MAP: `{"s1": {model, text}}`.
    assert_eq!(
        o["output"]["s1"]["text"], "canned-response",
        "loop output is the fresh subgraph's sink map: {o}"
    );
    // Inner nodes journaled fresh under "lp/0/s1" and "lp/1/s1".
    let labels: Vec<String> = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .map(|(_, e)| label(e))
        .collect();
    assert!(
        labels.iter().any(|l| l == "NodeCompleted(lp/0/s1)"),
        "iteration 0's inner node journaled under lp/0/s1: {labels:?}"
    );
    assert!(
        labels.iter().any(|l| l == "NodeCompleted(lp/1/s1)"),
        "iteration 1's inner node journaled under lp/1/s1: {labels:?}"
    );
}

// Note: a Loop-over-Subgraph-body CONVERGENCE test with a *pure* gate is
// intentionally absent. A graph body's iteration output is a wrapped sink map
// (`{sink_id: <node output>}`) whose values are always objects (`{model,text}`),
// which no `LoopGate` (`TextContains` reads a top-level `"text"` string; `FieldTrue`
// a top-level `== true`) can match — semantic convergence over a nested result is by
// design the gate-agent's job (design §4.3), landing naturally in slice-5 Tasks 4/5.
// The body-agnostic `converged=true`/`break` path is already covered by the leaf-body
// convergence tests (`loop_stops_when_the_gate_fires`), and the Subgraph sink-map
// output shape by `loop_over_a_subgraph_body_iterates_and_stops` above.

/// AC7 (pause) — an in-doubt Mutation inside a Loop's Subgraph body pauses the WHOLE run
/// loud: the nested agent pauses (`RunPaused` journaled), the Subgraph body maps that to
/// `NodeExec::Paused`, `run_loop` returns `Paused`, and the outer scheduler pauses the run
/// — it must NEVER journal `RunCompleted` over the unresolved Intent (no silent failure).
/// This is the `an_in_doubt_mutation_in_a_subgraph_pauses_the_run` shape with the Subgraph
/// wrapped as a Loop BODY (inner node under `"lp/0/n1"`).
#[tokio::test]
async fn loop_subgraph_body_pause_pauses_the_loop() {
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
    // The mutation-bearing agent lives inside a Loop's Subgraph BODY (inner node "lp/0/n1")
    // rather than a top-level Subgraph. A pure gate that never fires keeps the loop looping,
    // but the pause happens DURING iteration 0's body — before the gate is ever reached.
    let loop_graph = Graph {
        nodes: vec![Node {
            id: NodeId("lp".into()),
            kind: NodeKind::Loop {
                body: LoopBody::Subgraph(Box::new(Graph {
                    nodes: vec![agent_node("n1", "recorder", "item-0")],
                })),
                input: serde_json::json!({}),
                gate: GateSpec::Pure(LoopGate::TextContains("zzz-never".into())),
                max_iters: 2,
            },
            deps: vec![],
        }],
    };

    // Seed: run the loop to completion, then truncate to iteration 0's nested agent's
    // record_note EffectIntent (drops its EffectRecorded) → in-doubt on resume.
    let full = InMemoryJournal::new();
    let (seed_reg, seed_tools) = mk_recorder(Arc::new(std::sync::Mutex::new(Vec::new())));
    let (gw_s, _c) = demo_reference_tool_gateway().await;
    Executor::new(Arc::new(gw_s), Arc::new(full.clone()), "v1")
        .with_registry(seed_reg)
        .with_tools(seed_tools)
        .run(run, &loop_graph)
        .await
        .expect("seed Loop run completes");
    let events = full.load(run).await.unwrap();
    let cut = events
        .iter()
        .position(|(_, e)| matches!(e, JournalEvent::EffectIntent { .. }))
        .expect("iteration 0's nested agent journaled a record_note EffectIntent");
    let seeded = InMemoryJournal::new();
    for (_, e) in &events[..=cut] {
        seeded.append(run, e.clone()).await.unwrap();
    }

    // Resume with an Indeterminate reconciler + a FRESH empty sink → iteration 0's nested
    // Mutation is in-doubt → the nested agent pauses → the Subgraph body pauses → the Loop
    // pauses → the run pauses.
    let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let (reg, tools) = mk_recorder(sink.clone());
    let reconcilers =
        ReconcileRegistry::default().with_provider("record_note", Arc::new(AlwaysIndeterminate));
    let (gw_r, _c2) = demo_reference_tool_gateway().await;
    let outcome = Executor::new(Arc::new(gw_r), Arc::new(seeded.clone()), "v1")
        .with_registry(reg)
        .with_tools(tools)
        .with_reconcilers(Arc::new(reconcilers))
        .start(run, &loop_graph)
        .await
        .expect("resume yields an outcome");

    let pause = outcome
        .paused
        .expect("the in-doubt nested Mutation pauses the whole run");
    assert_eq!(
        pause.node,
        NodeId("lp".into()),
        "the Loop node is the pause point"
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

/// AC3 — a Loop over an Expand body: each iteration plans+executes; the refine-thread
/// feeds iteration i's output into iteration i+1's planner input. A FixedPlanner emits a
/// single-ModelCall plan; assert the loop runs max_iters (the refine is exercised
/// structurally — the behavioral refine proof is the coordinator e2e in Task 5).
#[tokio::test]
async fn loop_over_an_expand_body_refines_across_iterations() {
    let plan = Graph {
        nodes: vec![mc("n1", None)],
    };
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_planner(Arc::new(FixedPlanner(plan)));
    let loop_node = Node {
        id: NodeId("lp".into()),
        kind: NodeKind::Loop {
            body: LoopBody::Expand {
                planner: orchestrator_core::PlannerRef::Injected,
            },
            input: serde_json::json!({ "goal": "g" }),
            gate: GateSpec::Pure(LoopGate::TextContains("zzz-never".into())),
            max_iters: 2,
        },
        deps: vec![],
    };
    let out = exec
        .run(
            RunId(uuid::Uuid::new_v4()),
            &Graph {
                nodes: vec![loop_node],
            },
        )
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    let o = &out.outputs[&NodeId("lp".into())];
    assert_eq!(o["iterations"], 2);
    assert_eq!(
        o["converged"], false,
        "pure gate never matches an Expand sink map → best-effort"
    );
}

/// AC3 (behavioral) — the Expand-body refine threads: iteration 1's planner input IS
/// iteration 0's output (sink map), not the original input. A recording planner captures
/// each plan(input) call; asserting seen[1] carries iter-0's sink proves the refine.
#[tokio::test]
async fn loop_expand_body_refine_threads_prior_output_into_next_planner_input() {
    use std::sync::Mutex;
    struct RecordingPlanner {
        plan: Graph,
        seen: Arc<Mutex<Vec<serde_json::Value>>>,
    }
    #[async_trait::async_trait]
    impl orchestrator_core::Planner for RecordingPlanner {
        async fn plan(&self, input: &serde_json::Value) -> Result<Graph, OrchestratorError> {
            self.seen.lock().unwrap().push(input.clone());
            Ok(self.plan.clone())
        }
    }
    let seen = Arc::new(Mutex::new(Vec::new()));
    let planner = RecordingPlanner {
        plan: Graph {
            nodes: vec![mc("n1", None)],
        },
        seen: seen.clone(),
    };
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_planner(Arc::new(planner));
    let loop_node = Node {
        id: NodeId("lp".into()),
        kind: NodeKind::Loop {
            body: orchestrator_core::LoopBody::Expand {
                planner: orchestrator_core::PlannerRef::Injected,
            },
            input: serde_json::json!({ "goal": "start" }),
            gate: orchestrator_core::GateSpec::Pure(LoopGate::TextContains("zzz-never".into())),
            max_iters: 2,
        },
        deps: vec![],
    };
    let out = exec
        .run(
            RunId(uuid::Uuid::new_v4()),
            &Graph {
                nodes: vec![loop_node],
            },
        )
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2, "planner called once per iteration: {seen:?}");
    assert_eq!(
        seen[0],
        serde_json::json!({ "goal": "start" }),
        "iter 0 sees the initial input"
    );
    // iter 1's planner input is iter 0's OUTPUT (the sink map {n1: {model,text}}), NOT the initial input.
    assert!(
        seen[1].get("n1").is_some(),
        "iter 1 planner input must carry iter 0's sink output (refine); got {:?}",
        seen[1]
    );
    assert_ne!(
        seen[1],
        serde_json::json!({ "goal": "start" }),
        "iter 1 must NOT re-see the initial input"
    );
}

/// AC7 — a body failure inside any Loop iteration fails the whole Loop. The planned
/// single ModelCall fails on the gateway (`failing_after_gateway(0)`), so iteration 0's
/// expansion fails → the Loop node surfaces as failed (no silent finalize).
#[tokio::test]
async fn loop_expand_body_iteration_failure_fails_the_loop() {
    let (gateway, _c) = failing_after_gateway(0).await; // the planned node's ModelCall fails
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_planner(Arc::new(FixedPlanner(Graph {
            nodes: vec![mc("n1", None)],
        })));
    let loop_node = Node {
        id: NodeId("lp".into()),
        kind: NodeKind::Loop {
            body: LoopBody::Expand {
                planner: orchestrator_core::PlannerRef::Injected,
            },
            input: serde_json::json!({ "goal": "g" }),
            gate: GateSpec::Pure(LoopGate::TextContains("zzz-never".into())),
            max_iters: 2,
        },
        deps: vec![],
    };
    let out = exec
        .run(
            RunId(uuid::Uuid::new_v4()),
            &Graph {
                nodes: vec![loop_node],
            },
        )
        .await
        .expect("run yields an outcome");
    assert!(
        matches!(&out.failed, Some((n, _)) if n == &NodeId("lp".into())),
        "an Expand-body iteration failure fails the loop: {out:?}"
    );
}

/// AC8 — a Loop over Expand composes with the global expansion cap: each iteration is
/// one expansion, so the 2nd iteration breaches `max_expansions(1)` with a hard
/// `GlobalCapExceeded` (a loud halt, not a soft best-effort finalize).
#[tokio::test]
async fn loop_of_expands_respects_max_expansions_cap() {
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_planner(Arc::new(FixedPlanner(Graph {
            nodes: vec![mc("n1", None)],
        })))
        .with_max_expansions(1);
    let loop_node = Node {
        id: NodeId("lp".into()),
        kind: NodeKind::Loop {
            body: LoopBody::Expand {
                planner: orchestrator_core::PlannerRef::Injected,
            },
            input: serde_json::json!({ "goal": "g" }),
            gate: GateSpec::Pure(LoopGate::TextContains("zzz-never".into())),
            max_iters: 3,
        },
        deps: vec![],
    };
    let res = exec
        .run(
            RunId(uuid::Uuid::new_v4()),
            &Graph {
                nodes: vec![loop_node],
            },
        )
        .await;
    assert!(
        matches!(&res, Err(OrchestratorError::GlobalCapExceeded { cap, .. }) if cap == "max_expansions"),
        "2nd iteration's expansion breaches the cap: {res:?}"
    );
}

// ======================= SP-3 s5 Loop gate-agent (Task 4) ======================

/// The `[lp: Loop{ModelCall body, gate: Agent{a, stop_when: TextContains("STOP")}}]`
/// graph, `max_iters=3` (so convergence is by the gate, not the cap), agent "a" on
/// chain "c". Shared by AC5 and the AC6 resume.
fn loop_gate_agent_graph() -> Graph {
    Graph {
        nodes: vec![Node {
            id: NodeId("lp".into()),
            kind: NodeKind::Loop {
                body: LoopBody::ModelCall { chain: "c".into() },
                input: serde_json::json!({ "prompt": "start" }),
                gate: GateSpec::Agent {
                    agent: AgentRef("a".into()),
                    stop_when: LoopGate::TextContains("STOP".into()),
                },
                max_iters: 3,
            },
            deps: vec![],
        }],
    }
}

/// AC5 — a `GateSpec::Agent` gate drives a gate-agent over each iteration's output and
/// applies the pure `stop_when` to the AGENT's answer (not the body output). The
/// gate-agent answers "keep going" at iter 0 (continue) then "…STOP" at iter 1 (stop),
/// so the Loop converges at iterations=2 even though max_iters=3. The gate turns are
/// journaled under the reserved "lp/0/__gate__" / "lp/1/__gate__" paths.
#[tokio::test]
async fn loop_gate_agent_decides_stop() {
    // `scripted_gateway` is a single call-order queue. A ModelCall body consumes ONE
    // response per iteration; a no-tool gate-agent answer consumes ONE more. Call order:
    // iter0 body → iter0 gate-agent → iter1 body → iter1 gate-agent. The BODY texts never
    // carry the STOP marker, so only the gate-agent's answer can drive convergence — a
    // bug applying `stop_when` to the body output would miss STOP and run to the cap
    // (iterations=3, converged=false), which the asserts below would catch.
    let (gw, calls) = scripted_gateway(vec![
        final_response("body draft v0"),
        final_response("not yet, keep going"),
        final_response("body draft v1"),
        final_response("looks good, STOP"),
    ])
    .await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let out = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(ToolRegistry::default()))
        .run(run, &loop_gate_agent_graph())
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{:?}", out.failed);
    let o = &out.outputs[&NodeId("lp".into())];
    assert_eq!(o["converged"], true, "the gate-agent said STOP at iter 1");
    assert_eq!(
        o["iterations"], 2,
        "converged by the gate-agent at iter 1, not the max_iters=3 cap"
    );
    assert_eq!(
        calls.lock().unwrap().len(),
        4,
        "2 iterations × (1 body call + 1 gate-agent call)"
    );
    // The gate-agent turns were journaled under the reserved "{loop}/{i}/__gate__" path.
    let labels: Vec<String> = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .map(|(_, e)| label(e))
        .collect();
    assert!(
        labels.iter().any(|l| l == "NodeStarted(lp/0/__gate__)"),
        "iter 0's gate-agent journaled under lp/0/__gate__: {labels:?}"
    );
    assert!(
        labels.iter().any(|l| l == "EffectRecorded(lp/0/__gate__)"),
        "iter 0's gate-agent turn recorded a Pure effect: {labels:?}"
    );
    assert!(
        labels.iter().any(|l| l == "NodeStarted(lp/1/__gate__)"),
        "iter 1's gate-agent journaled under lp/1/__gate__: {labels:?}"
    );
}

/// AC6 — the gate-agent decision REPLAYS from the memo on resume: the gateway is never
/// re-called for a completed iteration's gate turn. Seed run 1 to a PARTIAL state (iter
/// 0's body + gate-agent journaled, then the script is exhausted so iter 1's body errors
/// → the Loop fails, NO RunCompleted). Resume on a FRESH gateway that serves ONLY iter
/// 1's body + gate-agent: iter 0 (body AND gate-agent) replays from the journal, so the
/// resume gateway sees EXACTLY 2 calls, and the Loop stops at the SAME iteration
/// (iterations=2, converged=true) — the resume-determinism the coordinator depends on.
#[tokio::test]
async fn loop_gate_agent_decision_replays_on_resume() {
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());

    // Run 1: only iter 0's body + gate-agent are scripted ("keep going" → continue);
    // iter 1's body then hits the exhausted script and errors → the Loop fails, so there
    // is NO RunCompleted and iter 0's effects (incl. the gate turn) are durably journaled.
    let (gw1, calls1) = scripted_gateway(vec![
        final_response("body draft v0"),
        final_response("not yet, keep going"),
    ])
    .await;
    let o1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(ToolRegistry::default()))
        .run(run, &loop_gate_agent_graph())
        .await
        .expect("run 1 yields an outcome");
    assert!(
        o1.failed.is_some(),
        "iter 1's body fails (script exhausted) → the Loop fails, no RunCompleted"
    );
    assert_eq!(
        calls1.lock().unwrap().len(),
        3,
        "run 1: iter0 body + iter0 gate-agent + the failing iter1 body"
    );

    // Run 2: a FRESH gateway serving ONLY iter 1's body + gate-agent ("…STOP" → stop),
    // over the SAME journal. Iter 0's body AND gate-agent replay from the memo, so this
    // gateway is called EXACTLY twice; were the gate decision re-driven, iter 0's gate
    // replay would consume iter 1's response (wrong order → wrong decision / count).
    let (gw2, calls2) = scripted_gateway(vec![
        final_response("body draft v1"),
        final_response("looks good, STOP"),
    ])
    .await;
    let o2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(ToolRegistry::default()))
        .start(run, &loop_gate_agent_graph())
        .await
        .expect("resume completes");
    assert!(o2.failed.is_none(), "{:?}", o2.failed);
    let o = &o2.outputs[&NodeId("lp".into())];
    assert_eq!(o["converged"], true, "the gate-agent said STOP at iter 1");
    assert_eq!(o["iterations"], 2, "stops at the SAME iteration on resume");
    assert_eq!(
        calls2.lock().unwrap().len(),
        2,
        "resume re-spent only iter 1 (body + gate-agent); iter 0's body AND gate-agent replayed from the memo"
    );
    // Non-vacuous: iter 0's gate-agent turn appears in EXACTLY ONE `EffectRecorded`
    // across BOTH runs — recorded live in run 1, replayed (not re-recorded) on resume.
    // A broken memo would re-run it live on resume and this count would be 2.
    let events = journal.load(run).await.unwrap();
    assert_eq!(
        effect_recorded_count(&events, &effect_id("lp/0/__gate__", 0, 0)),
        1,
        "iter 0's gate-agent turn was replayed from the journal on resume, not re-recorded/re-spent"
    );
}

// ==================== SP-3 s5 coordinator e2e (Task 5, AC10) ===================

/// AC10 (coordinator e2e) — the full slice-5 coordinator: a `Loop` whose body is an
/// `Expand{planner: Agent}` (plan+execute per iteration, threading the prior sink map
/// into the next planner input) and whose gate is a gate-`Agent` deciding Continue|Stop.
/// Two iterations run plan→execute→gate through a real (test) gateway; the gate-agent
/// answers "not yet" at iter 0 (Continue) then "…DONE" at iter 1 (Stop), so the Loop
/// converges at iterations=2 with max_iters=3 — convergence is the GATE, not the cap.
/// `on_plan_expanded` fires once per iteration (spy records lp/0 then lp/1).
///
/// Non-vacuous: the planner turn, the plan-node output, and the iter-0 "continue" answer
/// NEVER carry "DONE" — only the gate-agent's iter-1 answer can converge. A bug applying
/// `stop_when` to the wrong value (e.g. the Expand sink map, which has no top-level
/// "text"/"DONE") would miss the marker and run to the cap (iterations=3, converged=false),
/// which the asserts below would catch.
#[tokio::test]
async fn coordinator_loop_expand_body_with_gate_agent_converges() {
    // A registry with BOTH the `planner` agent (area "planning", drives the Expand body)
    // and the `gate` agent (decides Continue|Stop over each iteration's output).
    let reg = Arc::new(
        Registry::default()
            .with_agent(AgentDefinition {
                name: "planner".into(),
                area: "planning".into(),
                kind: "reasoning".into(),
                chain: Some("c".into()),
                chains: std::collections::HashMap::new(),
                grants: std::collections::HashMap::new(),
                tools: vec![],
                skills: vec![],
                system_prompt: "Emit a plan as JSON.".into(),
            })
            .with_agent(AgentDefinition {
                name: "gate".into(),
                area: "gating".into(),
                kind: "reasoning".into(),
                chain: Some("c".into()),
                chains: std::collections::HashMap::new(),
                grants: std::collections::HashMap::new(),
                tools: vec![],
                skills: vec![],
                system_prompt: "Answer DONE once the goal is met, else keep going.".into(),
            }),
    );

    // The planner emits a minimal single-`ModelCall` plan (no "DONE" anywhere).
    let plan_json = r#"{"graph":{"nodes":[
        {"id":"n1","kind":{"ModelCall":{"chain":"c","payload":{"prompt":"work"}}},"deps":[]}
    ]},"node_plans":{"n1":{"label":"do work"}}}"#;

    // `scripted_gateway` is a single FIFO call-order queue. Per iteration the coordinator
    // consumes it in THIS order:
    //   (1) the planner agent's ReAct turn → emits the plan JSON     (1 call, no tools),
    //   (2) the planned single-`ModelCall` node's execution          (1 call),
    //   (3) the gate agent's answer turn   → Continue|Stop text      (1 call, no tools).
    // → 3 calls/iteration × 2 iterations = 6. Iter 0's gate answers "not yet" (no DONE →
    //   Continue); iter 1's gate answers "…DONE" (→ Stop). None of the planner/plan-node/
    //   continue texts carry "DONE", so ONLY the gate-agent's iter-1 answer can converge.
    let (gw, calls) = scripted_gateway(vec![
        final_response(plan_json),       // iter0 (1) planner turn
        final_response("draft v0"),      // iter0 (2) plan node n1
        final_response("not yet"),       // iter0 (3) gate → Continue
        final_response(plan_json),       // iter1 (1) planner turn
        final_response("draft v1"),      // iter1 (2) plan node n1
        final_response("all set, DONE"), // iter1 (3) gate → Stop
    ])
    .await;

    // Spy over `on_plan_expanded`: records the path of each PlanExpanded (one per iter).
    use std::sync::Mutex;
    struct PlanSpy(Arc<Mutex<Vec<String>>>);
    #[async_trait::async_trait]
    impl OrchestratorHooks for PlanSpy {
        async fn on_plan_expanded(
            &self,
            _run: RunId,
            node: &NodeId,
            _graph: &Graph,
            _node_plans: &std::collections::HashMap<NodeId, orchestrator_core::NodePlan>,
        ) {
            self.0.lock().unwrap().push(node.0.clone());
        }
    }
    let plan_log = Arc::new(Mutex::new(Vec::new()));

    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let loop_node = Node {
        id: NodeId("lp".into()),
        kind: NodeKind::Loop {
            body: LoopBody::Expand {
                planner: orchestrator_core::PlannerRef::Agent(AgentRef("planner".into())),
            },
            input: serde_json::json!({ "goal": "converge on the answer" }),
            gate: GateSpec::Agent {
                agent: AgentRef("gate".into()),
                stop_when: LoopGate::TextContains("DONE".into()),
            },
            max_iters: 3, // > 2 so convergence is the gate, not the cap
        },
        deps: vec![],
    };
    let out = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_registry(reg)
        .with_tools(Arc::new(ToolRegistry::default()))
        .with_hooks(Arc::new(PlanSpy(plan_log.clone())))
        .run(
            run,
            &Graph {
                nodes: vec![loop_node],
            },
        )
        .await
        .expect("run");

    assert!(out.failed.is_none(), "{out:?}");
    let o = &out.outputs[&NodeId("lp".into())];
    assert_eq!(
        o["converged"], true,
        "the gate-agent said DONE at iter 1 → converged (not the max_iters cap): {o}"
    );
    assert_eq!(
        o["iterations"], 2,
        "converged by the gate-agent at iter 1, not run to the max_iters=3 cap: {o}"
    );
    // The final output carries the CONVERGED (iter-1) result — the Expand sink map keyed
    // by the bare plan-node id "n1", whose text is iter 1's plan output ("draft v1"), NOT
    // iter 0's ("draft v0"). Proves the loop threaded through to the converging iteration.
    assert_eq!(
        o["output"]["n1"]["text"], "draft v1",
        "final output is the converged iteration's Expand sink: {}",
        o["output"]
    );

    // Exactly 6 gateway calls: 2 iterations × (planner turn + plan node + gate turn).
    assert_eq!(
        calls.lock().unwrap().len(),
        6,
        "2 iterations × (1 planner turn + 1 plan node + 1 gate turn)"
    );

    // `on_plan_expanded` fired once per iteration (one plan splice each), at the reserved
    // Loop-iteration paths lp/0 then lp/1 — the per-iteration expansion the coordinator
    // performs, in order.
    let planned = plan_log.lock().unwrap().clone();
    assert_eq!(
        planned,
        vec!["lp/0".to_string(), "lp/1".to_string()],
        "one PlanExpanded per iteration, hook-fired under lp/0 then lp/1: {planned:?}"
    );

    // The two plan splices are journaled as `PlanExpanded` events under lp/0 and lp/1, and
    // each iteration's planner sub-run + gate-agent turn journal under their reserved paths.
    let labels: Vec<String> = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .map(|(_, e)| label(e))
        .collect();
    assert!(
        labels.iter().any(|l| l == "PlanExpanded(lp/0)"),
        "iter 0's plan spliced+journaled under lp/0: {labels:?}"
    );
    assert!(
        labels.iter().any(|l| l == "PlanExpanded(lp/1)"),
        "iter 1's plan spliced+journaled under lp/1: {labels:?}"
    );
    assert!(
        labels.iter().any(|l| l == "NodeStarted(lp/0/__plan__)"),
        "iter 0's planner sub-run journaled under lp/0/__plan__: {labels:?}"
    );
    assert!(
        labels.iter().any(|l| l == "NodeStarted(lp/0/__gate__)"),
        "iter 0's gate-agent journaled under lp/0/__gate__: {labels:?}"
    );
    assert!(
        labels.iter().any(|l| l == "NodeStarted(lp/1/__gate__)"),
        "iter 1's gate-agent journaled under lp/1/__gate__: {labels:?}"
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
        credentials: vec![],
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
        credentials: vec![],
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
            JournalEvent::RunStarted { version, budget: _ } => Some(version),
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
                    JournalEvent::RunStarted { version, budget: _ } => Some(version),
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
            JournalEvent::RunStarted { version, budget: _ } => Some(version),
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
            JournalEvent::RunStarted { version, budget: _ } => Some(version),
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

/// An infeasible plan whose `Map` body names an UNKNOWN agent must be caught by
/// `feasible` (the authoritative gate) BEFORE splicing: the Expand node ends
/// `Failed` (cascade-skipping its hard-dependent), NOT a fatal `?`-propagated hard
/// halt, and nothing is journaled as `PlanExpanded`. Regression guard for the gap
/// where `Map`/`Consolidate`/`Loop` `MapBody::Agent` refs skipped feasibility and
/// only blew up at `drive_agent` splice time (a non-resumable run abort).
#[tokio::test]
async fn planner_agent_map_body_unknown_agent_fails_the_node() {
    // Parseable + structurally valid (one Map node, no deps), but its body agent
    // "ghost" is absent from the registry (planner_registry holds only "planner").
    let plan_json = r#"{"graph":{"nodes":[{"id":"m","kind":{"Map":{"body":{"Agent":"ghost"},"over":[{}],"concurrency":1,"aggregation":"BestEffort"}},"deps":[]}]}}"#;
    let reg = planner_registry();
    let (gateway, _c) = scripted_gateway(vec![final_response(plan_json)]).await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1").with_registry(reg);
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![expand_agent_node("e", vec![]), mc_dep("d", Dep::hard("e"))],
    };
    let out = exec
        .run(run, &graph)
        .await
        .expect("run returns Ok (node Failed), not a fatal Err/panic");
    assert!(
        matches!(&out.failed, Some((n, _)) if n == &NodeId("e".into())),
        "infeasible Map-body agent → Expand node Failed: {out:?}"
    );
    assert!(
        out.skipped.contains(&NodeId("d".into())),
        "hard-dependent cascade-skipped (resumable), not aborted"
    );
    assert!(
        !journal
            .load(run)
            .await
            .unwrap()
            .iter()
            .any(|(_, ev)| matches!(ev, JournalEvent::PlanExpanded { .. })),
        "no PlanExpanded journaled for an infeasible plan (feasible rejects pre-splice)"
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

/// Full grounding e2e: the planner agent calls validate_plan (a real tool) on a draft,
/// then emits the final single-Agent plan (right-sizing: tier 1). Executed to completion.
#[tokio::test]
async fn planner_agent_uses_validate_plan_then_emits_a_single_agent_plan() {
    // Registry: a `planner` agent granted validate_plan + list_agents, and a `worker` agent.
    let worker = AgentDefinition {
        name: "worker".into(),
        area: "research".into(),
        kind: "reasoning".into(),
        chain: Some("c".into()),
        chains: std::collections::HashMap::new(),
        grants: std::collections::HashMap::new(),
        tools: vec![],
        skills: vec![],
        system_prompt: "work".into(),
    };
    let planner = AgentDefinition {
        name: "planner".into(),
        area: "planning".into(),
        kind: "reasoning".into(),
        chain: Some("c".into()),
        chains: std::collections::HashMap::new(),
        grants: std::collections::HashMap::new(),
        tools: vec!["validate_plan".into(), "list_agents".into()],
        skills: vec![],
        system_prompt: "Plan. Prefer the simplest structure.".into(),
    };
    let reg = Arc::new(
        Registry::default()
            .with_agent(planner)
            .with_agent(worker)
            .with_tool(
                crate::agent::tools::ValidatePlan {
                    registry: Arc::new(Registry::default()),
                    max_nodes: 512,
                }
                .spec(),
            )
            .with_tool(crate::agent::tools::ListAgents(Arc::new(Registry::default())).spec()),
    );

    // The single-Agent plan the planner ends up emitting (tier 1 — right-sizing).
    // The `Graph` serde shape is {"nodes":[{id, kind, deps}]}; an Agent node's kind is
    // {"Agent":{"agent":<name>, "input":<value>, "phase":null}}.
    let plan_json = r#"{"graph":{"nodes":[{"id":"n1","kind":{"Agent":{"agent":"worker","input":"go","phase":null}},"deps":[]}]},"node_plans":{"n1":{"label":"do it all"}}}"#;

    // Executable tools the planner actually calls: validate_plan (over the real reg) + list_agents.
    let tools = Arc::new(
        ToolRegistry::default()
            .with_tool(Arc::new(crate::agent::tools::ValidatePlan {
                registry: reg.clone(),
                max_nodes: 512,
            }))
            .with_tool(Arc::new(crate::agent::tools::ListAgents(reg.clone()))),
    );

    // Scripted gateway: planner turn 1 → call validate_plan(draft); turn 2 → final plan JSON;
    // then the spliced worker agent's single turn → final answer.
    let validate_args = serde_json::json!({ "plan": plan_json }).to_string();
    let (gateway, _c) = scripted_gateway(vec![
        tool_call_response("t1", "validate_plan", &validate_args),
        final_response(plan_json),
        final_response("worker done"),
    ])
    .await;

    let journal = InMemoryJournal::new();
    // One spy captures three signals: how many times on_plan_expanded fired, the
    // produced graph's node count (for the right-sizing assertion), and every tool
    // the planner agent invoked (to prove validate_plan was actually called).
    use std::sync::Mutex;
    struct Counter {
        expansions: Arc<Mutex<Vec<usize>>>, // node count per on_plan_expanded fire
        tool_calls: Arc<Mutex<Vec<String>>>,
    }
    #[async_trait::async_trait]
    impl OrchestratorHooks for Counter {
        async fn on_plan_expanded(
            &self,
            _r: RunId,
            _n: &NodeId,
            g: &Graph,
            _p: &std::collections::HashMap<NodeId, orchestrator_core::NodePlan>,
        ) {
            self.expansions.lock().unwrap().push(g.nodes.len());
        }
        async fn on_agent_tool_call(&self, _r: RunId, _n: &NodeId, tool: &str) {
            self.tool_calls.lock().unwrap().push(tool.to_string());
        }
    }
    let expansions = Arc::new(Mutex::new(Vec::<usize>::new()));
    let tool_calls = Arc::new(Mutex::new(Vec::<String>::new()));
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(reg)
        .with_tools(tools)
        .with_hooks(Arc::new(Counter {
            expansions: expansions.clone(),
            tool_calls: tool_calls.clone(),
        }));

    let e = NodeId("e".into());
    let graph = Graph {
        nodes: vec![expand_agent_node("e", vec![])],
    };
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    // NOTE: a scripted gateway cannot *act on* validate_plan's verdict (the model's
    // turns are fixed), so this e2e proves the planner *invoked* validate_plan and
    // right-sized the plan — not that it reasoned over the tool's ok/errors result.
    assert!(
        out.outputs[&e].get("n1").is_some(),
        "single-agent plan executed: {}",
        out.outputs[&e]
    );
    // AC10 right-sizing: exactly one expansion, and its produced graph is a single node.
    let expanded = expansions.lock().unwrap().clone();
    assert_eq!(
        expanded.len(),
        1,
        "on_plan_expanded fired once for the produced plan: {expanded:?}"
    );
    assert_eq!(
        expanded[0], 1,
        "tier-1 right-sizing: the plan is a single Agent node"
    );
    // The planner actually invoked validate_plan (an executed tool), not just emitted a plan.
    let invoked = tool_calls.lock().unwrap().clone();
    assert!(
        invoked.contains(&"validate_plan".to_string()),
        "planner invoked validate_plan: {invoked:?}"
    );
}

// ---- SP-3 slice 4B: PlannerRef::Select + the selection arm ----

/// A stub selector that always returns a fixed agent (tests the Select flow).
struct FixedSelector(AgentRef);
#[async_trait::async_trait]
impl orchestrator_core::PlannerSelector for FixedSelector {
    async fn select(
        &self,
        _goal: &serde_json::Value,
        _cands: &[AgentRef],
    ) -> Result<AgentRef, OrchestratorError> {
        Ok(self.0.clone())
    }
}

/// A stub selector whose `select` always errors — exercises the selector-`Err`
/// failure arm (the Expand node ends `Failed`, resumable, with no `PlanExpanded`).
struct ErrSelector;
#[async_trait::async_trait]
impl orchestrator_core::PlannerSelector for ErrSelector {
    async fn select(
        &self,
        _goal: &serde_json::Value,
        _cands: &[AgentRef],
    ) -> Result<AgentRef, OrchestratorError> {
        Err(OrchestratorError::RegistryLoad("boom".into()))
    }
}

/// A registry with two `planning`-area planner agents (both emit a plan via the gateway).
fn two_planner_registry() -> Arc<Registry> {
    let mk = |name: &str| AgentDefinition {
        name: name.into(),
        area: "planning".into(),
        kind: "reasoning".into(),
        chain: Some("c".into()),
        chains: std::collections::HashMap::new(),
        grants: std::collections::HashMap::new(),
        tools: vec![],
        skills: vec![],
        system_prompt: format!("planner {name}"),
    };
    Arc::new(
        Registry::default()
            .with_agent(mk("alpha"))
            .with_agent(mk("beta")),
    )
}

fn expand_select_node(id: &str, deps: Vec<Dep>) -> Node {
    Node {
        id: NodeId(id.into()),
        kind: NodeKind::Expand {
            input: serde_json::json!({ "goal": "g" }),
            planner: orchestrator_core::PlannerRef::Select,
        },
        deps,
    }
}

#[tokio::test]
async fn select_drives_the_chosen_planner_and_journals_the_selection() {
    let plan_json = r#"{"graph":{"nodes":[{"id":"n1","kind":{"ModelCall":{"chain":"c","payload":{"prompt":"n1"}}},"deps":[]}]}}"#;
    // response[0] → beta's planner turn (the plan); [1] → the spliced plan node n1
    // (a ModelCall on chain "c", one gateway call). The plan under-scripted this to a
    // single response; the spliced n1 needs its own call (cf. the resume sibling test).
    let (gateway, _c) =
        scripted_gateway(vec![final_response(plan_json), final_response("n1 out")]).await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(two_planner_registry())
        .with_planner_selector(Arc::new(FixedSelector(AgentRef("beta".into()))));
    let run = RunId(uuid::Uuid::new_v4());
    let e = NodeId("e".into());
    let graph = Graph {
        nodes: vec![expand_select_node("e", vec![])],
    };
    let out = exec.run(run, &graph).await.expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    assert!(
        out.outputs[&e].get("n1").is_some(),
        "chosen planner produced+spliced a plan"
    );
    // PlannerSelected{e -> beta} journaled, and the planner ran under "e/__plan__".
    let evs = journal.load(run).await.unwrap();
    assert!(
        evs.iter().any(|(_, ev)| matches!(ev, JournalEvent::PlannerSelected { node, agent } if node.0=="e" && agent.0=="beta")),
        "PlannerSelected journaled for beta"
    );
}

#[tokio::test]
async fn select_with_no_candidates_fails_the_node() {
    let (gateway, _c) = recording_gateway().await;
    // registry has an agent but NOT area=="planning".
    let reg = Arc::new(Registry::default().with_agent(AgentDefinition {
        name: "coder".into(),
        area: "coding".into(),
        kind: "exec".into(),
        chain: Some("c".into()),
        chains: std::collections::HashMap::new(),
        grants: std::collections::HashMap::new(),
        tools: vec![],
        skills: vec![],
        system_prompt: "c".into(),
    }));
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(reg)
        .with_planner_selector(Arc::new(FixedSelector(AgentRef("x".into()))));
    let graph = Graph {
        nodes: vec![expand_select_node("e", vec![])],
    };
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(
        matches!(&out.failed, Some((n, _)) if n == &NodeId("e".into())),
        "no planning agents → Failed: {out:?}"
    );
}

#[tokio::test]
async fn select_with_no_selector_wired_fails_the_node() {
    let (gateway, _c) = recording_gateway().await;
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(two_planner_registry()); // no selector
    let graph = Graph {
        nodes: vec![expand_select_node("e", vec![])],
    };
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(
        matches!(&out.failed, Some((n, _)) if n == &NodeId("e".into())),
        "no selector → Failed: {out:?}"
    );
}

#[tokio::test]
async fn select_picking_a_non_candidate_fails_the_node() {
    let (gateway, _c) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(two_planner_registry())
        .with_planner_selector(Arc::new(FixedSelector(AgentRef("ghost".into())))); // not a candidate
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![expand_select_node("e", vec![])],
    };
    let out = exec.run(run, &graph).await.expect("run");
    assert!(
        matches!(&out.failed, Some((n, _)) if n == &NodeId("e".into())),
        "non-candidate pick → Failed: {out:?}"
    );
    // Anti-hallucination ordering: the pick is validated against `candidates` BEFORE the
    // `PlannerSelected` append, so a non-candidate pick is never journaled.
    let evs = journal.load(run).await.unwrap();
    assert!(
        !evs.iter()
            .any(|(_, ev)| matches!(ev, JournalEvent::PlannerSelected { .. })),
        "a non-candidate pick is never journaled"
    );
}

/// Extra (coverage gap the plan flagged): a selector that returns `Err` fails the node
/// (resumable, no `PlanExpanded`) — the selector-error arm of the failure taxonomy.
#[tokio::test]
async fn select_selector_error_fails_the_node() {
    let (gateway, _c) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(two_planner_registry())
        .with_planner_selector(Arc::new(ErrSelector));
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![expand_select_node("e", vec![])],
    };
    let out = exec.run(run, &graph).await.expect("run");
    assert!(
        matches!(&out.failed, Some((n, _)) if n == &NodeId("e".into())),
        "selector Err → Failed: {out:?}"
    );
    assert!(
        !journal
            .load(run)
            .await
            .unwrap()
            .iter()
            .any(|(_, ev)| matches!(ev, JournalEvent::PlanExpanded { .. })),
        "no PlanExpanded journaled on a selector error"
    );
}

/// Resume reuses the journaled pick; the selector is NOT re-invoked even if it would
/// now pick differently. Mutation-verified: a selector that flips its choice on resume
/// is ignored because PlannerSelected pinned the original.
#[tokio::test]
async fn select_resume_reuses_the_recorded_pick() {
    let plan_json = r#"{"graph":{"nodes":[{"id":"n1","kind":{"ModelCall":{"chain":"c","payload":{"prompt":"n1"}}},"deps":[]}]}}"#;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![expand_select_node("e", vec![]), mc_dep("d", Dep::hard("e"))],
    };
    // Run 1: selector picks beta; beta's plan (n1) runs; then d fails (no 2nd scripted response).
    {
        let (gw, _c) =
            scripted_gateway(vec![final_response(plan_json), final_response("n1 out")]).await;
        let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
            .with_registry(two_planner_registry())
            .with_planner_selector(Arc::new(FixedSelector(AgentRef("beta".into()))));
        let o1 = exec.run(run, &graph).await.expect("run1");
        assert!(o1.failed.is_some(), "tail d failed: {o1:?}");
    }
    // Run 2: a selector that would pick ALPHA + a fresh recording gateway. Resume must
    // reuse beta (journaled) and re-drive only d.
    let (gw2, calls2) = recording_gateway().await;
    let exec2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_registry(two_planner_registry())
        .with_planner_selector(Arc::new(FixedSelector(AgentRef("alpha".into()))));
    let o2 = exec2.start(run, &graph).await.expect("resume");
    assert!(o2.failed.is_none(), "resume completes: {o2:?}");
    let recorded2 = calls2.lock().unwrap().clone();
    assert_eq!(
        recorded2.len(),
        1,
        "resume re-called the gateway only for d (planner not re-run): {recorded2:?}"
    );
    assert_eq!(recorded2[0].1, "d");
    // Exactly one PlannerSelected (beta), from run 1 — resume did not re-select.
    let sel: Vec<String> = journal
        .load(run)
        .await
        .unwrap()
        .iter()
        .filter_map(|(_, ev)| match ev {
            JournalEvent::PlannerSelected { agent, .. } => Some(agent.0.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        sel,
        vec!["beta".to_string()],
        "one selection, beta, never re-selected to alpha: {sel:?}"
    );
}

/// Resume in the crash-AFTER-`PlannerSelected`-BEFORE-`PlanExpanded` window: the OUTER
/// `fold.expansions` short-circuit is absent (the expansion was truncated away), so
/// resume re-enters the `Select` arm and MUST hit the INNER `fold.selections` guard —
/// reusing the journaled pick (beta) and skipping the selector. This is the memo the
/// slice-4B `Select` arm added; the other resume test only exercises the outer
/// `PlanExpanded` short-circuit, so without this the inner guard is unreached.
///
/// Discriminating property: were the inner `Some(a) => a.clone()` reuse removed, run 2's
/// flipped selector would re-select `alpha` and drive alpha's planner over beta's
/// memoized `e/__plan__` turn — different `system_prompt` ⇒ different input-hash ⇒ a
/// `DeterminismViolation` (the `.expect("resume …")` would panic). So this test goes red
/// without the guard.
#[tokio::test]
async fn select_resume_before_plan_expanded_reuses_the_recorded_pick() {
    let plan_json = r#"{"graph":{"nodes":[{"id":"n1","kind":{"ModelCall":{"chain":"c","payload":{"prompt":"n1"}}},"deps":[]}]}}"#;
    let run = RunId(uuid::Uuid::new_v4());
    let e = NodeId("e".into());
    let graph = Graph {
        nodes: vec![expand_select_node("e", vec![])],
    };

    // Run 1: FixedSelector(beta) → beta's single planner turn emits the plan (n1), run to
    // completion so PlannerSelected(beta) + the "e/__plan__" turn effects + PlanExpanded
    // are all journaled.
    let full = InMemoryJournal::new();
    let (gw1, _c1) =
        scripted_gateway(vec![final_response(plan_json), final_response("n1 out")]).await;
    Executor::new(Arc::new(gw1), Arc::new(full.clone()), "v1")
        .with_registry(two_planner_registry())
        .with_planner_selector(Arc::new(FixedSelector(AgentRef("beta".into()))))
        .run(run, &graph)
        .await
        .expect("seed run completes");

    // Truncate to the prefix BEFORE PlanExpanded → keep PlannerSelected(beta) + beta's
    // memoized planner turn, drop the expansion. So resume folds `selections[e]=beta` but
    // NO `expansions[e]`, forcing it back through the `Select` arm's inner guard.
    let events = full.load(run).await.unwrap();
    let cut = events
        .iter()
        .position(|(_, ev)| matches!(ev, JournalEvent::PlanExpanded { .. }))
        .expect("run 1 journaled a PlanExpanded");
    let seeded = InMemoryJournal::new();
    for (_, ev) in &events[..cut] {
        seeded.append(run, ev.clone()).await.unwrap();
    }

    // Run 2: a FLIPPED selector (would pick alpha) + a fresh succeeding gateway. Resume
    // reuses beta (inner guard), replays beta's memoized turn (no gateway call), journals
    // PlanExpanded, then runs n1 — never re-selecting to alpha.
    let (gw2, calls2) = recording_gateway().await;
    let out = Executor::new(Arc::new(gw2), Arc::new(seeded.clone()), "v1")
        .with_registry(two_planner_registry())
        .with_planner_selector(Arc::new(FixedSelector(AgentRef("alpha".into()))))
        .start(run, &graph)
        .await
        .expect("resume completes (beta's turn replays under beta, no DeterminismViolation)");
    assert!(out.failed.is_none(), "resume completes cleanly: {out:?}");
    assert!(
        out.outputs[&e].get("n1").is_some(),
        "the reused plan executed: {}",
        out.outputs[&e]
    );
    // Only n1 hit the gateway — beta's planner turn replayed from the memo, not re-spent.
    let recorded = calls2.lock().unwrap().clone();
    assert_eq!(
        recorded.len(),
        1,
        "only n1 hit the gateway (beta's turn replayed from memo): {recorded:?}"
    );
    assert_eq!(recorded[0].1, "n1");
    // Exactly one PlannerSelected, and it is beta — resume did NOT re-select to alpha.
    let sel: Vec<String> = seeded
        .load(run)
        .await
        .unwrap()
        .iter()
        .filter_map(|(_, ev)| match ev {
            JournalEvent::PlannerSelected { agent, .. } => Some(agent.0.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        sel,
        vec!["beta".to_string()],
        "one selection, beta, never re-selected to alpha on resume: {sel:?}"
    );
}

#[tokio::test]
async fn llm_planner_selector_picks_the_named_agent_from_the_menu() {
    use crate::executor::selector::LlmPlannerSelector;
    // Scripted gateway returns the chosen agent name (with surrounding whitespace, so the
    // selector's `.trim()` is exercised) as the response content.
    let (gateway, _c) = scripted_gateway(vec![final_response("  beta \n")]).await;
    // The scripted-gateway fixture only wires chain "c" (single_chain_config); the plan's
    // literal "select.chain" isn't configured, so the gateway would return "no candidates".
    // The registry (alpha+beta, area=="planning") lets the selector render a capability menu.
    let sel = LlmPlannerSelector::new(Arc::new(gateway), two_planner_registry(), "c");
    let cands = vec![AgentRef("alpha".into()), AgentRef("beta".into())];
    let got = sel
        .select(&serde_json::json!({ "goal": "do X" }), &cands)
        .await
        .expect("select");
    assert_eq!(got, AgentRef("beta".into()));
}

#[tokio::test]
async fn llm_planner_selector_errors_on_empty_content() {
    use crate::executor::selector::LlmPlannerSelector;
    // Empty response content → a clear diagnostic Err (not AgentRef("") — the executor's
    // Select arm would otherwise report it as a non-candidate pick).
    let (gateway, _c) = scripted_gateway(vec![final_response("")]).await;
    let sel = LlmPlannerSelector::new(Arc::new(gateway), two_planner_registry(), "c");
    let cands = vec![AgentRef("alpha".into()), AgentRef("beta".into())];
    let err = sel
        .select(&serde_json::json!({ "goal": "do X" }), &cands)
        .await;
    assert!(err.is_err(), "empty content → Err, got {err:?}");
}

/// End-to-end: goal → Select (LlmPlannerSelector picks a planner from the menu) → that
/// planner agent emits a plan → executed; PlannerSelected + on_planner_selected +
/// on_plan_expanded all observed.
#[tokio::test]
async fn select_end_to_end_with_llm_selector_and_hook() {
    use crate::executor::selector::LlmPlannerSelector;
    use std::sync::{Arc as StdArc, Mutex};
    let plan_json = r#"{"graph":{"nodes":[{"id":"n1","kind":{"ModelCall":{"chain":"c","payload":{"prompt":"n1"}}},"deps":[]}]}}"#;
    // Scripted gateway: call 1 = selector picks "beta"; call 2 = beta's planner turn → plan;
    // call 3 = the spliced plan node n1.
    let (gateway, _c) = scripted_gateway(vec![
        final_response("beta"),
        final_response(plan_json),
        final_response("n1 out"),
    ])
    .await;
    let selected = StdArc::new(Mutex::new(Vec::<String>::new()));
    struct Spy(StdArc<Mutex<Vec<String>>>);
    #[async_trait::async_trait]
    impl OrchestratorHooks for Spy {
        async fn on_planner_selected(&self, _run: RunId, node: &NodeId, agent: &AgentRef) {
            self.0
                .lock()
                .unwrap()
                .push(format!("{}->{}", node.0, agent.0));
        }
    }
    let gw = Arc::new(gateway);
    let reg = two_planner_registry();
    let exec = Executor::new(gw.clone(), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(reg.clone())
        // LlmPlannerSelector::new(gateway, registry, chain) — the registry renders the
        // capability menu (name/area/kind); reuse the same reg the executor selects from.
        .with_planner_selector(Arc::new(LlmPlannerSelector::new(gw, reg.clone(), "c")))
        .with_hooks(Arc::new(Spy(selected.clone())));
    let e = NodeId("e".into());
    let graph = Graph {
        nodes: vec![expand_select_node("e", vec![])],
    };
    let out = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(out.failed.is_none(), "{out:?}");
    assert!(
        out.outputs[&e].get("n1").is_some(),
        "selected planner produced+spliced a plan"
    );
    assert_eq!(
        *selected.lock().unwrap(),
        vec!["e->beta".to_string()],
        "on_planner_selected fired for beta"
    );
}

// ---------------------------------------------------------------------------
// SP-4 slice 2 — secret redaction wired into the executor (opt-in `with_redactor`,
// applied at the two LEAF output sites BEFORE journal + feed-back).
// ---------------------------------------------------------------------------

/// A Pure demo tool that RETURNS a canned payload — used to plant a secret in a tool
/// RESULT (not in a tool ARGUMENT), so the redaction target is the tool output. The
/// secret is assembled at runtime by the tests, never a source literal, so the repo's
/// semgrep CWE-798 (hard-coded-credential) hook stays quiet. Empty permissions ⇒ the
/// agent's empty grant covers it and the SP-4 s1 authorization gate is transparent.
///
/// `calls` is an OPTIONAL invocation sink: each live `call` pushes its args. A resume
/// that correctly memo-replays the tool's (redacted) output never re-executes the tool,
/// so a FRESH sink handle stays EMPTY on resume — the direct, independent signal (beside
/// the `EffectRecorded` count) that the tool was not re-invoked.
struct LeakTool {
    payload: serde_json::Value,
    calls: Option<Arc<std::sync::Mutex<Vec<serde_json::Value>>>>,
}
impl Tool for LeakTool {
    fn spec(&self) -> orchestrator_core::ToolSpec {
        orchestrator_core::ToolSpec {
            name: "leak".into(),
            description: Some("returns a canned payload".into()),
            input_schema: serde_json::json!({ "type": "object", "properties": {} }),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: Permissions::default(),
            activation: orchestrator_core::Activation::default(),
            credentials: vec![],
        }
    }
    fn call(&self, args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        if let Some(sink) = &self.calls {
            sink.lock().unwrap().push(args);
        }
        Ok(self.payload.clone())
    }
}

/// Registry whose agent "a" (chain "c") LISTS the `leak` demo tool, with its spec
/// compiled into the prompt. Empty grant ⇒ the SP-4 s1 gate is transparent.
fn leak_registry() -> Arc<Registry> {
    Arc::new(
        Registry::default()
            .with_agent(AgentDefinition {
                tools: vec!["leak".into()],
                ..agent_def("c")
            })
            .with_tool(
                LeakTool {
                    payload: serde_json::Value::Null,
                    calls: None,
                }
                .spec(),
            ),
    )
}

/// AC4: a tool RESULT carrying a secret is scrubbed before it is journaled AND before
/// it is fed back to the agent. With a `PatternRedactor` wired, the tool effect's
/// journaled `EffectRecorded.output` holds `[REDACTED]` (not the plaintext), and — since
/// the executor redacts once and uses that SAME value for both the journal split and the
/// `ToolOutcome::Ok` returned to the agent — the value fed back is redacted too.
#[tokio::test]
async fn tool_result_secret_is_redacted_in_journal_and_transcript() {
    // Assemble at RUNTIME so no credential-shaped literal sits in source (the semgrep
    // CWE-798 hook blocks those); the redactor still matches the built string.
    let secret = format!("sk-{}", "abcdefghijklmnopqrstuvwx");
    let (gateway, calls) = scripted_gateway(vec![
        tool_call_response("t1", "leak", "{}"),
        final_response("done"),
    ])
    .await;
    let journal = InMemoryJournal::new();
    let tools = Arc::new(ToolRegistry::default().with_tool(Arc::new(LeakTool {
        payload: serde_json::json!({ "leaked": secret.clone() }),
        calls: None,
    })));
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(leak_registry())
        .with_tools(tools)
        .with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()));

    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "leak a secret")],
    };
    let run = RunId(uuid::Uuid::new_v4());
    let outcome = exec.run(run, &graph).await.expect("run");
    assert!(outcome.failed.is_none(), "{:?}", outcome.failed);

    // The tool effect is turn-0, call index 0 → effect_id(node, 0, 1).
    let tool_eid = effect_id("n1", 0, 1);
    let events = journal.load(run).await.unwrap();
    let out = recorded_output(&events, &tool_eid).expect("tool effect recorded");
    assert_eq!(
        out["leaked"],
        serde_json::json!("[REDACTED]"),
        "journaled tool output is scrubbed: {out}"
    );

    // The value fed back to the agent is the SAME redacted value (single source), so no
    // plaintext survives ANYWHERE in the journal — a resume replays the scrubbed value.
    let dump = serde_json::to_string(&events).unwrap();
    assert!(
        !dump.contains(&secret),
        "plaintext secret must not appear anywhere in the journal"
    );
    assert_eq!(calls.lock().unwrap().len(), 2, "two model turns");
}

/// AC5: a model turn's free TEXT is scrubbed while its `tool_calls` (the structured call
/// args the next turn dispatches on) are left intact. The scripted turn emits both a
/// secret in `content` and a real `calc` tool call; the journaled turn output's `text`
/// is `[REDACTED]`, the tool-call `arguments` are byte-identical, and `calc` acts on the
/// real numbers (result 5) — proving redaction touches text only, never call args.
#[tokio::test]
async fn model_text_secret_is_redacted_tool_calls_intact() {
    let secret = format!("sk-{}", "abcdefghijklmnopqrstuvwx");
    let calc_args = "{\"op\":\"add\",\"a\":2,\"b\":3}";
    // Turn 0: the model emits FREE TEXT carrying the secret AND a real calc tool call.
    let turn0 = kernel::types::io::ChatResponse {
        content: Some(secret.clone()),
        tool_calls: vec![kernel::types::request::ToolCall {
            id: "t1".into(),
            name: "calc".into(),
            arguments: calc_args.into(),
        }],
        usage: None,
        model: Some("m".into()),
        degraded: false,
    };
    let (gateway, _calls) = scripted_gateway(vec![turn0, final_response("done")]).await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools())
        .with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()));

    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "compute and leak")],
    };
    let run = RunId(uuid::Uuid::new_v4());
    let outcome = exec.run(run, &graph).await.expect("run");
    assert!(outcome.failed.is_none(), "{:?}", outcome.failed);

    let events = journal.load(run).await.unwrap();
    // Model turn-0 output: text scrubbed, tool_call args intact.
    let model_out = recorded_output(&events, &effect_id("n1", 0, 0)).expect("model turn recorded");
    assert_eq!(
        model_out["text"],
        serde_json::json!("[REDACTED]"),
        "model free text redacted: {model_out}"
    );
    assert_eq!(
        model_out["tool_calls"][0]["arguments"],
        serde_json::json!(calc_args),
        "tool_call arguments left intact: {model_out}"
    );
    assert!(
        !serde_json::to_string(&events).unwrap().contains(&secret),
        "plaintext secret not journaled"
    );

    // The tool-call arg reached calc unredacted → it computed on the REAL numbers.
    let calc_out = recorded_output(&events, &effect_id("n1", 0, 1)).expect("calc effect recorded");
    assert_eq!(
        calc_out["result"].as_f64(),
        Some(5.0),
        "calc acted on the intact arg: {calc_out}"
    );
}

/// AC8: WITHOUT `with_redactor`, the exact same tool-returns-a-secret run journals the
/// plaintext verbatim — proving redaction is opt-in (byte-identical default) and that
/// the two tests above are load-bearing, not asserting an always-on scrub.
#[tokio::test]
async fn no_redactor_is_byte_identical() {
    let secret = format!("sk-{}", "abcdefghijklmnopqrstuvwx");
    let (gateway, _calls) = scripted_gateway(vec![
        tool_call_response("t1", "leak", "{}"),
        final_response("done"),
    ])
    .await;
    let journal = InMemoryJournal::new();
    let tools = Arc::new(ToolRegistry::default().with_tool(Arc::new(LeakTool {
        payload: serde_json::json!({ "leaked": secret.clone() }),
        calls: None,
    })));
    // Deliberately NO .with_redactor — redaction is opt-in.
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(leak_registry())
        .with_tools(tools);

    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "leak a secret")],
    };
    let run = RunId(uuid::Uuid::new_v4());
    exec.run(run, &graph).await.expect("run");

    let events = journal.load(run).await.unwrap();
    let out = recorded_output(&events, &effect_id("n1", 0, 1)).expect("tool effect recorded");
    assert_eq!(
        out["leaked"],
        serde_json::json!(secret),
        "without a redactor the plaintext secret is journaled verbatim"
    );
}

/// AC (gap fix): a direct `ModelCall` node whose model echoes a secret is scrubbed too
/// — proving the redaction chokepoint (`model_output`) covers the `run_node` ModelCall
/// leaf, not only the ReAct `dispatch_model_turn`.
#[tokio::test]
async fn model_call_node_text_is_redacted() {
    // Runtime-assembled token (no source literal ⇒ semgrep CWE-798 hook stays quiet).
    let token = ["sk", "abcdefghijklmnopqrstuvwx"].join("-");
    let (gateway, _calls) =
        scripted_gateway(vec![final_response(&format!("here it is {token}"))]).await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()));

    let n1 = NodeId("n1".into());
    let graph = Graph {
        nodes: vec![Node {
            id: n1.clone(),
            kind: model_call("c", "hello"),
            deps: vec![],
        }],
    };
    let run = RunId(uuid::Uuid::new_v4());
    let outcome = exec.run(run, &graph).await.expect("run");
    assert!(outcome.failed.is_none(), "{:?}", outcome.failed);

    // The ModelCall node's structural effect id is effect_id(node, 0, 0).
    let events = journal.load(run).await.unwrap();
    let out = recorded_output(&events, &effect_id("n1", 0, 0)).expect("model call recorded");
    let text = out["text"].as_str().expect("text field");
    assert!(
        text.contains("[REDACTED]"),
        "model-call text scrubbed: {text}"
    );
    assert!(
        !text.contains(&token),
        "plaintext token gone from text: {text}"
    );
    assert!(
        !serde_json::to_string(&events).unwrap().contains(&token),
        "plaintext token not journaled anywhere"
    );
}

/// AC (gap fix): a `Map`-item `ModelCall` whose model echoes a secret is scrubbed —
/// covering the Map-item leaf (`run_map_item`). The content-gated gateway echoes
/// `ok:{prompt}`, so a secret-bearing item lands the token in the item's model output.
#[tokio::test]
async fn map_item_model_text_is_redacted() {
    let token = ["sk", "abcdefghijklmnopqrstuvwx"].join("-");
    let (gateway, _calls) = content_gated_gateway().await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()));

    // One item whose prompt carries the secret ⇒ the echoed model text is `ok:{token}`.
    let over = map_items([token.as_str()]);
    let graph = map_graph("m", over, Aggregation::BestEffort);
    let m = NodeId("m".into());
    let run = RunId(uuid::Uuid::new_v4());
    let outcome = exec.run(run, &graph).await.expect("map runs");
    assert!(outcome.failed.is_none(), "{:?}", outcome.failed);

    // The child's aggregated model text is redacted in the Map node output …
    let child_text = outcome.outputs[&m]["results"][0]["ok"]["text"]
        .as_str()
        .expect("child text");
    assert!(
        child_text.contains("[REDACTED]"),
        "map-item text scrubbed: {child_text}"
    );
    assert!(
        !child_text.contains(&token),
        "plaintext token gone from map-item text: {child_text}"
    );
    // … and nowhere in the journal (the child's own `EffectRecorded` is scrubbed too).
    let events = journal.load(run).await.unwrap();
    assert!(
        !serde_json::to_string(&events).unwrap().contains(&token),
        "plaintext token not journaled anywhere"
    );
}

/// AC (gap fix): a `Consolidate` synthesis `ModelCall` whose model echoes a secret is
/// scrubbed — covering the Consolidate leaf (`run_consolidate`). A single-item Map (so
/// the scripted gateway is consumed in a deterministic order: item first, then the
/// synthesis) feeds a Consolidate whose synthesis response carries the secret.
#[tokio::test]
async fn consolidate_synthesis_text_is_redacted() {
    let token = ["sk", "abcdefghijklmnopqrstuvwx"].join("-");
    // Response order: [0] the single Map item (clean), [1] the Consolidate synthesis
    // (secret). One Map item ⇒ no concurrent race for the shared scripted queue.
    let (gateway, _calls) =
        scripted_gateway(vec![final_response("clean item"), final_response(&token)]).await;
    let journal = InMemoryJournal::new();
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()));

    let cons = NodeId("cons".into());
    let graph = Graph {
        nodes: vec![
            Node {
                id: NodeId("m".into()),
                kind: NodeKind::Map {
                    body: MapBody::ModelCall { chain: "c".into() },
                    over: map_items(["only"]),
                    concurrency: 1,
                    aggregation: Aggregation::BestEffort,
                },
                deps: vec![],
            },
            Node {
                id: cons.clone(),
                kind: NodeKind::Consolidate {
                    over: NodeId("m".into()),
                    min_viable: 1,
                    body: MapBody::ModelCall { chain: "c".into() },
                },
                deps: vec![Dep::soft("m")],
            },
        ],
    };
    let run = RunId(uuid::Uuid::new_v4());
    let outcome = exec.run(run, &graph).await.expect("run");
    assert!(outcome.failed.is_none(), "{:?}", outcome.failed);

    // The synthesis text (the Consolidate node output) is exactly the placeholder.
    assert_eq!(
        outcome.outputs[&cons]["text"],
        serde_json::json!("[REDACTED]"),
        "consolidate synthesis text scrubbed: {}",
        outcome.outputs[&cons]
    );
    let events = journal.load(run).await.unwrap();
    assert!(
        !serde_json::to_string(&events).unwrap().contains(&token),
        "plaintext token not journaled anywhere"
    );
}

/// AC6 (resume determinism): a redacted tool output is journaled ONCE (redacted) and,
/// on resume, REPLAYS from that memo — the tool is NOT re-invoked and no second
/// `EffectRecorded` is appended. Unlike the pure-gate denial case (where a live re-run
/// would re-produce the same value), a re-invoked `LeakTool` here would re-execute and
/// re-record, so the COUNT is a sharp, mutation-verifiable proof: the tool effect's
/// `EffectRecorded` appears EXACTLY ONCE across BOTH runs (recorded live in run 1,
/// replayed — not re-recorded — in run 2). A broken tool memo would re-run the tool on
/// resume (count → 2, fresh sink non-empty). The replayed value is the JOURNALED
/// (redacted) one, so no plaintext ever re-enters the transcript on resume.
#[tokio::test]
async fn redacted_output_replays_on_resume() {
    // Runtime-assembled (no source literal ⇒ the semgrep CWE-798 hook stays quiet); the
    // redactor still matches the joined string.
    let secret = ["sk", "abcdefghijklmnopqrstuvwx"].join("-");
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "leak a secret")],
    };
    // The tool effect is turn-0, call index 0 → effect_id(node, 0, 1).
    let tool_eid = effect_id("n1", 0, 1);

    // Run 1 (seed a PARTIAL run): turn 0's `leak` call executes LIVE (sink1 gets ONE
    // entry) → its redacted output is journaled once; the script then runs out on turn 1
    // → the node fails → NO RunCompleted.
    let sink1 = Arc::new(std::sync::Mutex::new(Vec::<serde_json::Value>::new()));
    let (gw1, _c1) = scripted_gateway(vec![tool_call_response("t1", "leak", "{}")]).await;
    let tools1 = Arc::new(ToolRegistry::default().with_tool(Arc::new(LeakTool {
        payload: serde_json::json!({ "leaked": secret.clone() }),
        calls: Some(sink1.clone()),
    })));
    let o1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_registry(leak_registry())
        .with_tools(tools1)
        .with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()))
        .run(run, &graph)
        .await
        .expect("seed yields an outcome");
    assert!(
        o1.failed.is_some(),
        "seed dies at turn 1 (script exhausted)"
    );
    assert_eq!(
        sink1.lock().unwrap().len(),
        1,
        "the tool ran live exactly once in run 1"
    );
    let seeded = journal.load(run).await.unwrap();
    assert_eq!(
        effect_recorded_count(&seeded, &tool_eid),
        1,
        "the redacted tool output is recorded once, live, in run 1"
    );
    assert_eq!(
        recorded_output(&seeded, &tool_eid).expect("tool effect recorded")["leaked"],
        serde_json::json!("[REDACTED]"),
        "the journaled output is redacted even before resume"
    );

    // Run 2 (resume over the SAME journal, FRESH gateway + FRESH `LeakTool` sink): turn
    // 0's model turn and its `leak` call both MEMO-HIT — the redacted value replays from
    // the journal (the tool is NOT re-executed), so the fresh sink stays empty; turn 1 is
    // driven live to a final answer and the run completes.
    let sink2 = Arc::new(std::sync::Mutex::new(Vec::<serde_json::Value>::new()));
    let (gw2, _c2) = scripted_gateway(vec![final_response("done")]).await;
    let tools2 = Arc::new(ToolRegistry::default().with_tool(Arc::new(LeakTool {
        payload: serde_json::json!({ "leaked": secret.clone() }),
        calls: Some(sink2.clone()),
    })));
    let o2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_registry(leak_registry())
        .with_tools(tools2)
        .with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()))
        .start(run, &graph)
        .await
        .expect("resume yields an outcome");
    assert!(
        o2.failed.is_none() && o2.paused.is_none(),
        "resume reaches the completed state: {:?}",
        o2.failed
    );
    assert!(
        sink2.lock().unwrap().is_empty(),
        "the redacted output replayed from the memo — the tool was NOT re-invoked on resume"
    );

    // Load-bearing (mutation-verifiable): the tool effect's `EffectRecorded` appears
    // EXACTLY ONCE across BOTH runs — the memo replays the redacted value rather than
    // re-executing the tool. Disabling the tool memo makes resume re-run + re-record it
    // (count → 2, sink2 non-empty), failing this assertion.
    let events = journal.load(run).await.unwrap();
    assert_eq!(
        effect_recorded_count(&events, &tool_eid),
        1,
        "the redacted output is journaled once total: recorded live, then replayed from the memo"
    );
    assert_eq!(
        recorded_output(&events, &tool_eid).expect("tool effect recorded")["leaked"],
        serde_json::json!("[REDACTED]"),
        "the single record is the redacted value"
    );
    assert!(
        !serde_json::to_string(&events).unwrap().contains(&secret),
        "no plaintext secret survives anywhere in the journal across the crash/resume seam"
    );
}

/// AC7 (CAS-blob redaction): with a `ContentStore` and a low `cas_threshold`, an
/// over-threshold tool output is stored in the CAS as a `ContentRef`. Redaction runs in
/// `record_tool_effect` BEFORE `split_output`, so the BYTES in the CAS are the redacted
/// ones — the blob contains `[REDACTED]` and never the plaintext. Both the plaintext AND
/// the redacted output exceed the threshold, so the split happens regardless; the proof
/// is that the stored blob is scrubbed, i.e. redaction precedes the CAS write.
#[tokio::test]
async fn over_threshold_secret_is_redacted_in_cas() {
    use orchestrator_store::InMemoryContentStore;

    let secret = ["sk", "abcdefghijklmnopqrstuvwx"].join("-");
    let (gateway, _calls) = scripted_gateway(vec![
        tool_call_response("t1", "leak", "{}"),
        final_response("done"),
    ])
    .await;
    let journal = InMemoryJournal::new();
    let content = Arc::new(InMemoryContentStore::new());
    // Payload `{"leaked": <secret>}` serializes to ~40 bytes and its redacted form
    // `{"leaked":"[REDACTED]"}` to ~22 — both exceed an 8-byte threshold, so the output
    // splits to a Ref either way. What we assert is that the stored blob is SCRUBBED.
    let tools = Arc::new(ToolRegistry::default().with_tool(Arc::new(LeakTool {
        payload: serde_json::json!({ "leaked": secret.clone() }),
        calls: None,
    })));
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(leak_registry())
        .with_tools(tools)
        .with_content_store(content.clone())
        .with_cas_threshold(8)
        .with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()));

    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "leak a secret")],
    };
    let run = RunId(uuid::Uuid::new_v4());
    let outcome = exec.run(run, &graph).await.expect("run");
    assert!(outcome.failed.is_none(), "{:?}", outcome.failed);

    // The tool effect (turn 0, call index 0) split to a CAS Ref, not an inline value.
    let tool_eid = effect_id("n1", 0, 1);
    let events = journal.load(run).await.unwrap();
    let digest = events
        .iter()
        .find_map(|(_, e)| match e {
            JournalEvent::EffectRecorded {
                effect_id,
                output: EffectOutput::Ref(r),
                ..
            } if effect_id == &tool_eid => Some(r.digest.clone()),
            _ => None,
        })
        .expect("the over-threshold tool output split to a CAS ref");

    // The bytes stored in the CAS are the REDACTED ones (redaction precedes split_output).
    let bytes = content.get(&digest).await.expect("blob present in the CAS");
    let raw = String::from_utf8(bytes.clone()).expect("utf8 blob");
    assert!(
        raw.contains("[REDACTED]"),
        "the CAS blob is redacted: {raw}"
    );
    assert!(
        !raw.contains(&secret),
        "the plaintext secret is NOT in the CAS blob: {raw}"
    );
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        value["leaked"],
        serde_json::json!("[REDACTED]"),
        "the materialized CAS value is scrubbed: {value}"
    );
    // Defense in depth: the plaintext is in NEITHER the journal NOR any CAS blob.
    assert!(
        !serde_json::to_string(&events).unwrap().contains(&secret),
        "plaintext secret not in the journal control log"
    );
}

/// AC9 (whole-journal, multi-surface e2e): one agent run leaks a secret on BOTH scrub
/// surfaces at once — a tool RESULT (`leak`) and the model's free TEXT — and is driven to
/// completion. Task 2's AC4 already whole-journal-scans a single tool-result secret; this
/// widens the guarantee to the combined tool + model-text surfaces in ONE run, then
/// asserts NEITHER plaintext appears ANYWHERE in the journal, that BOTH surfaces are
/// present as `[REDACTED]`, and that the final agent output carries no plaintext. No CAS
/// is wired, so every output is inline in the journal — the scan is over the real bytes.
#[tokio::test]
async fn agent_tool_secret_never_lands_plaintext_e2e() {
    // Two DISTINCT runtime-assembled secrets — one per surface — so each surface is
    // proven scrubbed independently (not one scrub masking the other).
    let tool_secret = ["sk", "abcdefghijklmnopqrstuvwx"].join("-");
    let model_secret = ["sk", "zyxwvutsrqponmlkjihgfedcba"].join("-");
    // Turn 0: a `leak` tool call (result carries `tool_secret`). Turn 1: the model's final
    // free text echoes `model_secret`.
    let (gateway, calls) = scripted_gateway(vec![
        tool_call_response("t1", "leak", "{}"),
        final_response(&format!("here it is: {model_secret}")),
    ])
    .await;
    let journal = InMemoryJournal::new();
    let tools = Arc::new(ToolRegistry::default().with_tool(Arc::new(LeakTool {
        payload: serde_json::json!({ "leaked": tool_secret.clone() }),
        calls: None,
    })));
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(leak_registry())
        .with_tools(tools)
        .with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()));

    let n1 = NodeId("n1".into());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "leak on both surfaces")],
    };
    let run = RunId(uuid::Uuid::new_v4());
    let outcome = exec.run(run, &graph).await.expect("run");
    assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
    assert_eq!(
        calls.lock().unwrap().len(),
        2,
        "two model turns drove the run"
    );

    let events = journal.load(run).await.unwrap();

    // Positive proof BOTH surfaces were scrubbed (not merely absent because the run
    // short-circuited): the tool result is `[REDACTED]` …
    let tool_out = recorded_output(&events, &effect_id("n1", 0, 1)).expect("tool effect recorded");
    assert_eq!(
        tool_out["leaked"],
        serde_json::json!("[REDACTED]"),
        "tool-result surface scrubbed: {tool_out}"
    );
    // … and the model's final free text contains `[REDACTED]` (not the plaintext).
    let final_text = outcome.outputs[&n1]["text"].as_str().expect("final text");
    assert!(
        final_text.contains("[REDACTED]") && !final_text.contains(&model_secret),
        "model-text surface scrubbed in the final output: {final_text}"
    );

    // The whole-journal guarantee: NEITHER plaintext secret appears ANYWHERE across the
    // serialized events (all inline — no CAS wired).
    let dump = serde_json::to_string(&events).unwrap();
    assert!(
        !dump.contains(&tool_secret),
        "the tool-result plaintext never lands in the journal"
    );
    assert!(
        !dump.contains(&model_secret),
        "the model-text plaintext never lands in the journal"
    );
}

/// A `ReconcileProvider` that unconditionally CONFIRMS an in-doubt Mutation with a
/// caller-supplied output — models a real reconciler whose recorded side-effect output
/// (the same secret-bearing class as a live tool result) carries a secret. Ignores the
/// idempotency key/args so the confirmed `output` is fully controlled by the test.
struct ConfirmWith {
    output: serde_json::Value,
}
#[async_trait::async_trait]
impl orchestrator_core::ReconcileProvider for ConfirmWith {
    async fn reconcile(
        &self,
        _idempotency_key: &str,
        _args: &serde_json::Value,
    ) -> Result<orchestrator_core::ReconcileOutcome, OrchestratorError> {
        Ok(orchestrator_core::ReconcileOutcome::Confirmed(
            self.output.clone(),
        ))
    }
}

/// SP-4 s2 (leak fix): the reconcile-in-doubt `Confirmed` path is the fourth
/// side-effect-output producer and MUST redact like `record_tool_effect`. On resume an
/// in-doubt Mutation asks its reconciler; a `Confirmed(output)` records that provider's
/// output AND feeds it back to the agent. Because run 1 recorded NO `EffectRecorded`
/// (in-doubt), there is no memo to fence — a missed scrub is a SILENT durable plaintext
/// write. With a `PatternRedactor` wired, the executor redacts ONCE and uses that SAME
/// value for both the journaled `split_output` and the `ToolOutcome::Ok` returned to the
/// agent, so the journaled record is `[REDACTED]`, the fed-back value is the identical
/// redacted one, and NO plaintext survives anywhere in the journal. Mutation-verify:
/// dropping the `let output = self.redact(&output);` line journals the plaintext and
/// fails both the journaled-value and whole-journal assertions.
#[tokio::test]
async fn reconcile_confirmed_output_is_redacted() {
    // The note effect id is turn-0, call index 0 → effect_id(node, 0, 1) (matches the
    // sibling in-doubt reconcile tests).
    let note_eid = effect_id("n1", 0, 1);
    // Assemble the secret at RUNTIME so no credential-shaped literal sits in source (the
    // repo's semgrep CWE-798 hook blocks those); the redactor still matches the built
    // string. This is the reconciler's confirmed side-effect output — `{"recorded": …}`,
    // mirroring `NoteReconciler`'s shape — carrying the secret.
    let secret = ["sk", "abcdefghijklmnopqrstuvwx"].join("-");
    let (journal, run) = seed_in_doubt_note().await;

    // A reconciler that CONFIRMS with a secret-bearing output. A FRESH empty sink proves
    // the Confirmed path records WITHOUT re-running the tool (the sink stays empty).
    let sink = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let reconcilers = ReconcileRegistry::default().with_provider(
        "record_note",
        Arc::new(ConfirmWith {
            output: serde_json::json!({ "recorded": secret.clone() }),
        }),
    );

    // Resume with a `PatternRedactor` wired (mirrors `resume_in_doubt`, plus the redactor).
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "note it")],
    };
    let (gw, _c) = scripted_gateway(vec![final_response("done")]).await;
    let out = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"))
        .with_tools(Arc::new(
            ToolRegistry::default().with_tool(Arc::new(RecordNote::new(sink.clone()))),
        ))
        .with_reconcilers(Arc::new(reconcilers))
        .with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()))
        .start(run, &graph)
        .await
        .expect("resume yields an outcome");
    let events = journal.load(run).await.unwrap();

    assert!(
        out.failed.is_none() && out.paused.is_none(),
        "Confirmed completes the run: {:?}",
        out.failed
    );
    assert!(
        sink.lock().unwrap().is_empty(),
        "Confirmed records WITHOUT re-running the side effect — the tool was not re-invoked"
    );
    assert_eq!(
        effect_recorded_count(&events, &note_eid),
        1,
        "Confirmed appends the Mutation's EffectRecorded exactly once"
    );

    // The journaled reconciler output is scrubbed — `[REDACTED]`, NOT the plaintext.
    let recorded = recorded_output(&events, &note_eid).expect("the reconciled effect is recorded");
    assert_eq!(
        recorded["recorded"],
        serde_json::json!("[REDACTED]"),
        "the Confirmed path's journaled output is redacted: {recorded}"
    );

    // The value fed back to the agent is the SAME redacted value (the executor redacts
    // once and reuses it for both the journal split and the returned `ToolOutcome::Ok`),
    // so no plaintext secret survives ANYWHERE in the journal — a resume replays the
    // scrubbed value, and the model never sees the secret.
    assert!(
        !serde_json::to_string(&events).unwrap().contains(&secret),
        "plaintext secret must not appear anywhere in the journal"
    );
    assert!(
        events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "the run completes"
    );
}

// ---------------------------------------------------------------------------
// SP-4 credential broker (Task 3) — resolve the tool's DECLARED cred refs via the
// broker, inject them into the call's `ToolContext.credentials` (ephemeral: never
// journaled/hashed), scrub the tool output of the injected VALUES by exact match
// (composing with the s2 pattern redactor), and fail LOUD on a declared-but-
// unresolvable ref.
// ---------------------------------------------------------------------------

/// SP-4 broker probe: a Pure tool that DECLARES credential refs (`declares`) and, per
/// call, records the injected `api_token` secret it received (via `call_ctx`) into a
/// shared cell. With `echo`, it RETURNS that injected value in its output
/// (`{"leaked": <cred>}`) so a test can assert the exact-value scrub. The tool's
/// `spec().credentials` is `declares` — that is the surface `record_tool_effect` reads
/// (via the executable `ToolRegistry`) to know which refs to resolve+inject.
struct CredTool {
    declares: Vec<String>,
    /// Records `ctx.credentials.get("api_token").map(expose)` seen on each live call.
    seen: Arc<std::sync::Mutex<Vec<Option<String>>>>,
    /// When true, the tool ECHOES its injected credential in its output.
    echo: bool,
}
impl Tool for CredTool {
    fn spec(&self) -> orchestrator_core::ToolSpec {
        orchestrator_core::ToolSpec {
            name: "cred_probe".into(),
            description: Some("records/echoes its injected credential".into()),
            input_schema: serde_json::json!({ "type": "object", "properties": {} }),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: Permissions::default(),
            activation: orchestrator_core::Activation::default(),
            credentials: self.declares.clone(),
        }
    }
    // The executor always dispatches via `call_ctx`; `call` exists only to satisfy the
    // trait (never reached on the executor path — it cannot see the injected ctx).
    fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        Ok(serde_json::json!({ "ok": true }))
    }
    fn call_ctx(
        &self,
        _args: serde_json::Value,
        ctx: &ToolContext,
    ) -> Result<serde_json::Value, OrchestratorError> {
        let got = ctx
            .credentials
            .get("api_token")
            .map(|s| s.expose().to_string());
        self.seen.lock().unwrap().push(got.clone());
        if self.echo {
            Ok(serde_json::json!({ "leaked": got }))
        } else {
            Ok(serde_json::json!({ "ok": true }))
        }
    }
}

/// Registry whose agent "a" (chain "c") LISTS the `cred_probe` tool, with a spec compiled
/// into the prompt. Empty permissions ⇒ the agent's empty grant covers it and the SP-4 s1
/// authorization gate is transparent (the credential wiring is what's under test).
fn cred_registry() -> Arc<Registry> {
    Arc::new(
        Registry::default()
            .with_agent(AgentDefinition {
                tools: vec!["cred_probe".into()],
                ..agent_def("c")
            })
            .with_tool(
                CredTool {
                    declares: vec!["api_token".into()],
                    seen: Arc::new(std::sync::Mutex::new(Vec::new())),
                    echo: false,
                }
                .spec(),
            ),
    )
}

/// A `ref → secret` broker map, assembled at RUNTIME (no credential-shaped literal sits in
/// source — the repo's semgrep CWE-798 hook blocks those).
fn broker_map(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// The `input_hash` recorded by the `EffectRecorded` for one effect id (`None` if no such
/// record). Used to prove the injected credential is NOT folded into the effect hash.
fn recorded_input_hash(events: &[(Seq, JournalEvent)], eid: &EffectId) -> Option<String> {
    events.iter().find_map(|(_, e)| match e {
        JournalEvent::EffectRecorded {
            effect_id,
            input_hash,
            ..
        } if effect_id == eid => Some(input_hash.clone()),
        _ => None,
    })
}

/// AC3: a tool that DECLARES `api_token` has that credential RESOLVED by the broker and
/// INJECTED into the call's `ToolContext.credentials` — the tool reads the real secret.
/// A tool that declares NO credential (even with a broker wired) gets an EMPTY map and
/// runs fine (the injection is scoped to declared refs only).
#[tokio::test]
async fn declared_credential_is_injected_into_call_ctx() {
    // Runtime-assembled secret (semgrep CWE-798 hook stays quiet).
    let secret = format!("tok-{}", "abcdef123456");
    let seen = Arc::new(std::sync::Mutex::new(Vec::<Option<String>>::new()));

    let (gateway, _calls) = scripted_gateway(vec![
        tool_call_response("t1", "cred_probe", "{}"),
        final_response("done"),
    ])
    .await;
    let tools = Arc::new(ToolRegistry::default().with_tool(Arc::new(CredTool {
        declares: vec!["api_token".into()],
        seen: seen.clone(),
        echo: false,
    })));
    let exec = Executor::new(Arc::new(gateway), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(cred_registry())
        .with_tools(tools)
        .with_credential_broker(Arc::new(crate::agent::tools::StaticCredentialBroker::new(
            broker_map(&[("api_token", &secret)]),
        )));

    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "use the token")],
    };
    let outcome = exec
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(outcome.failed.is_none(), "{:?}", outcome.failed);
    assert_eq!(
        *seen.lock().unwrap(),
        vec![Some(secret.clone())],
        "the declared credential was resolved + injected into the call ctx"
    );

    // Companion: a tool that declares NO credential gets an EMPTY map (even with a broker
    // wired) and runs fine — the injection is scoped to declared refs only.
    let seen_none = Arc::new(std::sync::Mutex::new(Vec::<Option<String>>::new()));
    let (gw2, _c2) = scripted_gateway(vec![
        tool_call_response("t1", "cred_probe", "{}"),
        final_response("done"),
    ])
    .await;
    let tools2 = Arc::new(ToolRegistry::default().with_tool(Arc::new(CredTool {
        declares: vec![], // declares NOTHING
        seen: seen_none.clone(),
        echo: false,
    })));
    let out2 = Executor::new(Arc::new(gw2), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(cred_registry())
        .with_tools(tools2)
        .with_credential_broker(Arc::new(crate::agent::tools::StaticCredentialBroker::new(
            broker_map(&[("api_token", &secret)]),
        )))
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run");
    assert!(out2.failed.is_none(), "{:?}", out2.failed);
    assert_eq!(
        *seen_none.lock().unwrap(),
        vec![None],
        "a tool that declares no credential gets an empty ctx map"
    );
}

/// AC5: a tool that ECHOES its injected credential in its output has that EXACT value
/// scrubbed to `[REDACTED]`. The broker holds a NON-pattern-shaped value (`hunter2`), so
/// the s2 `PatternRedactor` (wired here to prove COMPOSITION) does NOT catch it — the
/// `[REDACTED]` can only come from the per-call exact-value scrub. The journaled output
/// AND the value fed back to the agent are scrubbed; no plaintext survives anywhere.
#[tokio::test]
async fn echoed_credential_is_scrubbed_by_exact_value() {
    // NON-secret-shaped, runtime-assembled: the pattern redactor cannot match it, so only
    // the exact-value scrub can produce `[REDACTED]`.
    use orchestrator_core::Redactor;
    let secret = format!("hun{}", "ter2");
    assert_eq!(secret, "hunter2");
    // Prove the premise: the pattern redactor leaves this value untouched.
    assert_eq!(
        orchestrator_core::PatternRedactor::default().redact(&serde_json::json!(secret.clone())),
        serde_json::json!(secret.clone()),
        "the value is NOT pattern-shaped, so the s2 redactor is transparent to it"
    );

    let (gateway, _calls) = scripted_gateway(vec![
        tool_call_response("t1", "cred_probe", "{}"),
        final_response("done"),
    ])
    .await;
    let journal = InMemoryJournal::new();
    let tools = Arc::new(ToolRegistry::default().with_tool(Arc::new(CredTool {
        declares: vec!["api_token".into()],
        seen: Arc::new(std::sync::Mutex::new(Vec::new())),
        echo: true, // RETURNS the injected credential
    })));
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(cred_registry())
        .with_tools(tools)
        .with_credential_broker(Arc::new(crate::agent::tools::StaticCredentialBroker::new(
            broker_map(&[("api_token", &secret)]),
        )))
        .with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()));

    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "echo the token")],
    };
    let run = RunId(uuid::Uuid::new_v4());
    let outcome = exec.run(run, &graph).await.expect("run");
    assert!(outcome.failed.is_none(), "{:?}", outcome.failed);

    // The tool effect is turn-0, call index 0 → effect_id(node, 0, 1).
    let tool_eid = effect_id("n1", 0, 1);
    let events = journal.load(run).await.unwrap();
    let out = recorded_output(&events, &tool_eid).expect("tool effect recorded");
    assert_eq!(
        out["leaked"],
        serde_json::json!("[REDACTED]"),
        "the echoed credential is scrubbed by exact value: {out}"
    );
    // The value fed back to the agent is the SAME scrubbed value (single source) — no
    // plaintext credential survives ANYWHERE in the journal.
    assert!(
        !serde_json::to_string(&events).unwrap().contains(&secret),
        "plaintext credential must not appear anywhere in the journal"
    );
}

/// AC4: the injected credential is EPHEMERAL — it is not journaled and it is not folded
/// into the effect hash. Proof: (1) with the credential injected but NOT echoed, the
/// serialized journal never contains the secret; (2) the recorded `input_hash` for the
/// SAME tool+args is IDENTICAL whether the credential is injected or not (the cred is not
/// part of the hash), so a resume replays deterministically regardless of the broker.
#[tokio::test]
async fn credential_is_ephemeral_not_in_journal_or_hash() {
    let secret = format!("tok-{}", "abcdef123456");
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "use the token")],
    };
    let tool_eid = effect_id("n1", 0, 1);

    // Run WITH the credential injected (broker present), tool does NOT echo it.
    let jr_with = InMemoryJournal::new();
    let (gw1, _c1) = scripted_gateway(vec![
        tool_call_response("t1", "cred_probe", "{}"),
        final_response("done"),
    ])
    .await;
    let tools1 = Arc::new(ToolRegistry::default().with_tool(Arc::new(CredTool {
        declares: vec!["api_token".into()],
        seen: Arc::new(std::sync::Mutex::new(Vec::new())),
        echo: false,
    })));
    let run_with = RunId(uuid::Uuid::new_v4());
    Executor::new(Arc::new(gw1), Arc::new(jr_with.clone()), "v1")
        .with_registry(cred_registry())
        .with_tools(tools1)
        .with_credential_broker(Arc::new(crate::agent::tools::StaticCredentialBroker::new(
            broker_map(&[("api_token", &secret)]),
        )))
        .run(run_with, &graph)
        .await
        .expect("run");
    let events_with = jr_with.load(run_with).await.unwrap();
    // Ephemeral: the injected secret is nowhere in the serialized journal.
    assert!(
        !serde_json::to_string(&events_with)
            .unwrap()
            .contains(&secret),
        "the injected credential is never journaled"
    );
    let hash_with = recorded_input_hash(&events_with, &tool_eid).expect("tool effect recorded");

    // Run WITHOUT any credential (tool declares nothing, NO broker), SAME tool name + args.
    let jr_without = InMemoryJournal::new();
    let (gw2, _c2) = scripted_gateway(vec![
        tool_call_response("t1", "cred_probe", "{}"),
        final_response("done"),
    ])
    .await;
    let tools2 = Arc::new(ToolRegistry::default().with_tool(Arc::new(CredTool {
        declares: vec![],
        seen: Arc::new(std::sync::Mutex::new(Vec::new())),
        echo: false,
    })));
    let run_without = RunId(uuid::Uuid::new_v4());
    Executor::new(Arc::new(gw2), Arc::new(jr_without.clone()), "v1")
        .with_registry(cred_registry())
        .with_tools(tools2)
        .run(run_without, &graph)
        .await
        .expect("run");
    let hash_without = recorded_input_hash(&jr_without.load(run_without).await.unwrap(), &tool_eid)
        .expect("tool effect recorded");

    assert_eq!(
        hash_with, hash_without,
        "the effect input_hash is identical with vs without the credential — the cred is not hashed"
    );
}

/// AC6: a tool that DECLARES a credential the broker cannot resolve fails LOUD — the node
/// is `Failed` (never a silent missing credential, and the tool is never executed under a
/// half-populated ctx). Covers BOTH tiers: a broker that returns `None`, and no broker
/// wired at all.
#[tokio::test]
async fn unresolvable_declared_credential_fails_loud() {
    let n1 = NodeId("n1".into());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "needs a missing cred")],
    };
    let seen = Arc::new(std::sync::Mutex::new(Vec::<Option<String>>::new()));

    // Tier 1: a broker is wired but returns `None` for the declared ref.
    let (gw1, _c1) = scripted_gateway(vec![tool_call_response("t1", "cred_probe", "{}")]).await;
    let tools1 = Arc::new(ToolRegistry::default().with_tool(Arc::new(CredTool {
        declares: vec!["missing".into()],
        seen: seen.clone(),
        echo: false,
    })));
    let out1 = Executor::new(Arc::new(gw1), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(cred_registry())
        .with_tools(tools1)
        .with_credential_broker(Arc::new(crate::agent::tools::StaticCredentialBroker::new(
            broker_map(&[]), // resolves NOTHING
        )))
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run yields an outcome");
    let (node, msg) = out1
        .failed
        .expect("an unresolvable declared credential fails the node");
    assert_eq!(node, n1, "the failing node is named");
    assert!(
        msg.contains("credential 'missing'"),
        "the failure names the missing credential (not silent): {msg}"
    );

    // Tier 2: NO broker wired at all — a declared ref is still a loud failure.
    let (gw2, _c2) = scripted_gateway(vec![tool_call_response("t1", "cred_probe", "{}")]).await;
    let tools2 = Arc::new(ToolRegistry::default().with_tool(Arc::new(CredTool {
        declares: vec!["missing".into()],
        seen: seen.clone(),
        echo: false,
    })));
    let out2 = Executor::new(Arc::new(gw2), Arc::new(InMemoryJournal::new()), "v1")
        .with_registry(cred_registry())
        .with_tools(tools2)
        .run(RunId(uuid::Uuid::new_v4()), &graph)
        .await
        .expect("run yields an outcome");
    let (node2, msg2) = out2
        .failed
        .expect("no broker + a declared credential fails the node");
    assert_eq!(node2, n1);
    assert!(msg2.contains("credential 'missing'"), "{msg2}");

    // The tool never executed under either tier (fail closes BEFORE `call_ctx`).
    assert!(
        seen.lock().unwrap().is_empty(),
        "the tool is never invoked when its declared credential cannot be resolved"
    );
}

// ---------------------------------------------------------------------------
// SP-4 credential broker (Task 4) — determinism-on-resume + whole-journal
// no-plaintext e2e. A completed cred-using effect replays from the memo WITHOUT
// re-consulting the broker (its scrubbed output was journaled, secret-free); a
// full agent ReAct run that authenticates with an injected secret never lands the
// plaintext anywhere in the journal OR the final agent output.
// ---------------------------------------------------------------------------

/// A `CredentialBroker` that COUNTS its `resolve` calls (shared `Arc<AtomicUsize>`) and
/// delegates to a static `ref → secret` map. Proves a memoized cred-using tool effect is
/// NOT re-resolved on resume: a FRESH counting broker sees ZERO resolves because the effect
/// replays from the journal, never re-running the tool.
struct CountingBroker {
    map: std::collections::HashMap<String, String>,
    resolves: Arc<AtomicUsize>,
}
#[async_trait::async_trait]
impl orchestrator_core::CredentialBroker for CountingBroker {
    async fn resolve(
        &self,
        cred_ref: &str,
    ) -> Result<Option<orchestrator_core::Secret>, OrchestratorError> {
        self.resolves.fetch_add(1, Ordering::SeqCst);
        Ok(self.map.get(cred_ref).map(orchestrator_core::Secret::new))
    }
}

/// SP-4 e2e probe: an agent tool that DECLARES `api_token`, EXPOSES it to "authenticate"
/// (flips `authed` ONLY when the injected secret matches `expect` — an in-memory, never-
/// journaled comparison), and returns a MASKED confirmation `{"authed": <bool>}` that never
/// echoes the raw value. Distinct from `CredTool`: it does REAL work with the secret yet its
/// output is a boolean, so the e2e proves the secret is USED but never LANDS.
struct AuthTool {
    /// The secret the tool expects on `expose()` — authenticating means it matches.
    expect: String,
}
impl Tool for AuthTool {
    fn spec(&self) -> orchestrator_core::ToolSpec {
        orchestrator_core::ToolSpec {
            name: "cred_probe".into(),
            description: Some("authenticates with an injected credential".into()),
            input_schema: serde_json::json!({ "type": "object", "properties": {} }),
            effect_class: EffectClass::Pure,
            ttl_secs: None,
            source: None,
            permissions: Permissions::default(),
            activation: orchestrator_core::Activation::default(),
            credentials: vec!["api_token".into()],
        }
    }
    // Never reached on the executor path (it always dispatches via `call_ctx`, the only
    // surface that can see the injected credential): without a ctx there is no secret.
    fn call(&self, _args: serde_json::Value) -> Result<serde_json::Value, OrchestratorError> {
        Ok(serde_json::json!({ "authed": false }))
    }
    fn call_ctx(
        &self,
        _args: serde_json::Value,
        ctx: &ToolContext,
    ) -> Result<serde_json::Value, OrchestratorError> {
        // Do real work with the secret (authenticate) — but return only a masked boolean:
        // `authed == true` can ONLY arise from `expose() == self.expect`, never a raw echo.
        let authed = ctx
            .credentials
            .get("api_token")
            .map(|s| s.expose() == self.expect)
            .unwrap_or(false);
        Ok(serde_json::json!({ "authed": authed }))
    }
}

/// AC4 (resume clause): a tool that used a broker credential, once completed + journaled, is
/// NOT re-run on resume — so the broker is NOT re-consulted (its scrubbed output was
/// journaled, secret-free). Seed: run the single-turn cred agent to completion through a
/// COUNTING broker (resolve fires exactly once), then TRUNCATE the journal to the prefix
/// ending at the tool's `EffectRecorded` (dropping the turn-1 final model call + completion)
/// — an in-memo tail the resume must drive. Resume over that journal with a FRESH counting
/// broker: the memoized tool effect replays from the journal, the resume broker's resolve
/// count stays 0, the tool's `call_ctx` is never re-invoked, the effect is recorded exactly
/// once, and the run completes with no `DeterminismViolation`.
#[tokio::test]
async fn broker_not_reinvoked_for_a_memoized_tool_on_resume() {
    let secret = format!("tok-{}", "abcdef123456"); // runtime-assembled (semgrep CWE-798)
    let tool_eid = effect_id("n1", 0, 1);
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "use the token")],
    };

    // --- Seed run: complete the cred tool through a COUNTING broker. ---
    let full = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let seed_seen = Arc::new(std::sync::Mutex::new(Vec::<Option<String>>::new()));
    let seed_resolves = Arc::new(AtomicUsize::new(0));
    let (gw1, _c1) = scripted_gateway(vec![
        tool_call_response("t1", "cred_probe", "{}"),
        final_response("done"),
    ])
    .await;
    let tools1 = Arc::new(ToolRegistry::default().with_tool(Arc::new(CredTool {
        declares: vec!["api_token".into()],
        seen: seed_seen.clone(),
        echo: false,
    })));
    Executor::new(Arc::new(gw1), Arc::new(full.clone()), "v1")
        .with_registry(cred_registry())
        .with_tools(tools1)
        .with_credential_broker(Arc::new(CountingBroker {
            map: broker_map(&[("api_token", &secret)]),
            resolves: seed_resolves.clone(),
        }))
        .run(run, &graph)
        .await
        .expect("seed run completes");
    assert_eq!(
        seed_resolves.load(Ordering::SeqCst),
        1,
        "the seed run resolved the declared credential exactly once"
    );
    assert_eq!(
        *seed_seen.lock().unwrap(),
        vec![Some(secret.clone())],
        "the seed run's tool saw the real injected secret"
    );

    // Truncate to the prefix ending at the tool's `EffectRecorded` — the effect is memoized,
    // but the turn-1 final model call + NodeCompleted + RunCompleted are dropped, so the
    // resume MUST drive the tail (and would re-resolve if it re-ran the tool).
    let events = full.load(run).await.unwrap();
    let cut = events
        .iter()
        .position(|(_, e)| {
            matches!(e, JournalEvent::EffectRecorded { effect_id, .. } if effect_id == &tool_eid)
        })
        .expect("seed run journaled the tool's EffectRecorded");
    assert!(
        !events[..=cut]
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "the truncated seed is a partial (no RunCompleted) — the resume must drive the tail"
    );
    let seeded = InMemoryJournal::new();
    for (_, e) in &events[..=cut] {
        seeded.append(run, e.clone()).await.unwrap();
    }

    // --- Resume: FRESH counting broker; the memoized tool must NOT be re-resolved. ---
    let resume_seen = Arc::new(std::sync::Mutex::new(Vec::<Option<String>>::new()));
    let resume_resolves = Arc::new(AtomicUsize::new(0));
    let (gw2, _c2) = scripted_gateway(vec![final_response("done")]).await;
    let tools2 = Arc::new(ToolRegistry::default().with_tool(Arc::new(CredTool {
        declares: vec!["api_token".into()],
        seen: resume_seen.clone(),
        echo: false,
    })));
    let outcome = Executor::new(Arc::new(gw2), Arc::new(seeded.clone()), "v1")
        .with_registry(cred_registry())
        .with_tools(tools2)
        .with_credential_broker(Arc::new(CountingBroker {
            map: broker_map(&[("api_token", &secret)]),
            resolves: resume_resolves.clone(),
        }))
        .start(run, &graph)
        .await
        .expect("resume yields an outcome");

    // Load-bearing: the memoized cred effect replayed WITHOUT re-consulting the broker.
    assert_eq!(
        resume_resolves.load(Ordering::SeqCst),
        0,
        "the resume broker was NOT consulted — the memoized tool effect replayed from the journal"
    );
    assert!(
        resume_seen.lock().unwrap().is_empty(),
        "the memoized tool's call_ctx was NOT re-invoked on resume"
    );
    assert!(
        outcome.failed.is_none() && outcome.paused.is_none(),
        "resume completes with no DeterminismViolation: failed={:?} paused={:?}",
        outcome.failed,
        outcome.paused
    );
    let resumed = seeded.load(run).await.unwrap();
    assert_eq!(
        effect_recorded_count(&resumed, &tool_eid),
        1,
        "the tool effect is recorded exactly once across seed + resume (replayed, not re-run)"
    );
    assert!(
        resumed
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "the resumed run completes"
    );
}

/// AC8 (end-to-end, no plaintext): a full agent ReAct run — reason → call `cred_probe`
/// (which DECLARES `api_token`) → observe → final answer — with a `StaticCredentialBroker`
/// holding a runtime-assembled secret. The tool EXPOSES the secret to authenticate but
/// returns a MASKED `{"authed": true}` (never the raw value). Assert: the run completes;
/// the tool genuinely authenticated with the real secret (`authed == true` ⇒ `expose()`
/// matched); and NO plaintext secret appears ANYWHERE — not across the whole serialized
/// journal, and not in the final agent output. Distinct from Task 3's bare tool-effect
/// tests: this drives a real multi-turn agent node AND scans the final agent output too.
#[tokio::test]
async fn agent_tool_authenticates_with_injected_secret_no_plaintext_e2e() {
    let secret = format!("tok-{}", "abcdef123456"); // runtime-assembled (semgrep CWE-798)
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (gateway, _calls) = scripted_gateway(vec![
        tool_call_response("t1", "cred_probe", "{}"),
        final_response("authenticated; proceeding"),
    ])
    .await;
    let tools = Arc::new(ToolRegistry::default().with_tool(Arc::new(AuthTool {
        expect: secret.clone(),
    })));
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(cred_registry())
        .with_tools(tools)
        .with_credential_broker(Arc::new(crate::agent::tools::StaticCredentialBroker::new(
            broker_map(&[("api_token", &secret)]),
        )));
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "authenticate, then report status")],
    };
    let n1 = NodeId("n1".into());
    let outcome = exec.run(run, &graph).await.expect("run");

    // The full ReAct run completed.
    assert!(
        outcome.failed.is_none() && outcome.paused.is_none(),
        "run completes: failed={:?} paused={:?}",
        outcome.failed,
        outcome.paused
    );
    assert_eq!(outcome.completed, vec![n1.clone()]);

    // The tool genuinely authenticated with the REAL injected secret (`authed == true` can
    // only arise from `expose() == secret`), yet its output is a MASKED boolean.
    let tool_eid = effect_id("n1", 0, 1);
    let events = journal.load(run).await.unwrap();
    let out = recorded_output(&events, &tool_eid).expect("tool effect recorded");
    assert_eq!(
        out,
        serde_json::json!({ "authed": true }),
        "the tool authenticated with the real secret and returned a masked confirmation: {out}"
    );

    // Whole-journal scan: the plaintext secret appears in NO event, anywhere.
    assert!(
        !serde_json::to_string(&events).unwrap().contains(&secret),
        "plaintext secret must not appear anywhere in the journal"
    );
    // Final agent output scan: the node's canonical output carries no plaintext secret.
    assert!(
        !serde_json::to_string(&outcome.outputs)
            .unwrap()
            .contains(&secret),
        "plaintext secret must not appear in the final agent output"
    );
    // The run genuinely reached completion.
    assert!(
        events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "the run completes"
    );
}

// ---------------------------------------------------------------------------
// SP-4 credential broker — whole-slice review fixes.
// (1 · Important) scrub the EXACT injected values BEFORE the s2 pattern redact, so a
//     wrapped/composite secret is redacted WHOLE (no surviving high-entropy-adjacent
//     fragment). (2 · Minor) a broker RESOLVE ERROR fails LOUD as a node failure,
//     mirroring the unresolvable-`None` path (journal-everything parity).
// ---------------------------------------------------------------------------

/// SP-4 broker whole-slice review (Important, security): a WRAPPED/COMPOSITE injected
/// credential — a high-entropy `sk-…` span wrapped in non-pattern `wrap-` bytes — is
/// scrubbed WHOLE, with NO surviving fragment. If the s2 pattern redactor ran FIRST it
/// would collapse only the inner `sk-…` span to `[REDACTED]` (`wrap-[REDACTED]`), leaving
/// the `wrap-` prefix — real credential material — to survive into BOTH the journal AND the
/// value fed back to the agent. Scrubbing the EXACT injected value FIRST redacts the whole
/// secret; the residual pattern pass then finds nothing. The broker holds a runtime-
/// assembled composite value (the repo's semgrep CWE-798 hook blocks credential literals).
#[tokio::test]
async fn echoed_composite_credential_no_fragment_leak() {
    use orchestrator_core::Redactor;
    // Composite: `wrap-` (non-pattern) + `sk-` + 20 alnum (matches the s2
    // `sk-[A-Za-z0-9_-]{20,}` pattern). The pattern alone fragments this; only the
    // exact-value scrub can redact the WHOLE thing.
    let secret = format!("wrap-sk-{}", "abcdef0123456789abcd");
    // Premise: the s2 redactor catches ONLY the inner `sk-…` span, leaving `wrap-`.
    assert_eq!(
        orchestrator_core::PatternRedactor::default().redact(&serde_json::json!(secret.clone())),
        serde_json::json!("wrap-[REDACTED]"),
        "premise: the pattern redactor alone fragments the composite, leaving `wrap-`"
    );

    let (gateway, _calls) = scripted_gateway(vec![
        tool_call_response("t1", "cred_probe", "{}"),
        final_response("done"),
    ])
    .await;
    let journal = InMemoryJournal::new();
    let tools = Arc::new(ToolRegistry::default().with_tool(Arc::new(CredTool {
        declares: vec!["api_token".into()],
        seen: Arc::new(std::sync::Mutex::new(Vec::new())),
        echo: true, // RETURNS the injected credential verbatim
    })));
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(cred_registry())
        .with_tools(tools)
        .with_credential_broker(Arc::new(crate::agent::tools::StaticCredentialBroker::new(
            broker_map(&[("api_token", &secret)]),
        )))
        .with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()));

    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "echo the token")],
    };
    let run = RunId(uuid::Uuid::new_v4());
    let outcome = exec.run(run, &graph).await.expect("run");
    assert!(outcome.failed.is_none(), "{:?}", outcome.failed);

    // The journaled tool-effect output: the WHOLE composite is `[REDACTED]` — neither the
    // full secret nor the `wrap-` fragment survives.
    let tool_eid = effect_id("n1", 0, 1);
    let events = journal.load(run).await.unwrap();
    let out = recorded_output(&events, &tool_eid).expect("tool effect recorded");
    assert_eq!(
        out["leaked"],
        serde_json::json!("[REDACTED]"),
        "the composite credential is scrubbed WHOLE (no fragment): {out}"
    );
    // Whole-journal scan (journaled == fed-back, a single-source `result`): neither the full
    // secret nor the `wrap-` prefix fragment appears ANYWHERE.
    let serialized = serde_json::to_string(&events).unwrap();
    assert!(
        !serialized.contains(&secret),
        "no plaintext composite secret survives in the journal"
    );
    assert!(
        !serialized.contains("wrap-"),
        "no `wrap-` credential fragment survives in the journal: {serialized}"
    );
    // The value fed back to the agent carries no fragment either (same single-source
    // `result` that was journaled).
    assert!(
        !serde_json::to_string(&outcome.outputs)
            .unwrap()
            .contains("wrap-"),
        "no `wrap-` fragment in the final agent output fed back"
    );
}

/// SP-4 broker whole-slice review (Minor, parity/observability): a broker RESOLVE ERROR
/// fails LOUD as a NODE failure — a `NodeFailed` is journaled AND the outcome surfaces the
/// node as `Failed`, mirroring the unresolvable-`None` path (never a raw run-level `Err`
/// with no journal, and never a silent success). The tool is never executed under a
/// half-populated ctx.
#[tokio::test]
async fn broker_error_fails_loud_as_node_failure() {
    // A broker whose `resolve` always ERRORS (an infrastructure failure, not a miss).
    struct ErroringBroker;
    #[async_trait::async_trait]
    impl orchestrator_core::CredentialBroker for ErroringBroker {
        async fn resolve(
            &self,
            _cred_ref: &str,
        ) -> Result<Option<orchestrator_core::Secret>, OrchestratorError> {
            Err(OrchestratorError::Gateway(
                "broker backend unavailable".into(),
            ))
        }
    }

    let n1 = NodeId("n1".into());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "needs a cred the broker can't fetch")],
    };
    let seen = Arc::new(std::sync::Mutex::new(Vec::<Option<String>>::new()));
    let (gateway, _calls) =
        scripted_gateway(vec![tool_call_response("t1", "cred_probe", "{}")]).await;
    let journal = InMemoryJournal::new();
    let tools = Arc::new(ToolRegistry::default().with_tool(Arc::new(CredTool {
        declares: vec!["api_token".into()],
        seen: seen.clone(),
        echo: false,
    })));
    let run = RunId(uuid::Uuid::new_v4());
    let outcome = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(cred_registry())
        .with_tools(tools)
        .with_credential_broker(Arc::new(ErroringBroker))
        .run(run, &graph)
        .await
        .expect("a broker error surfaces as an outcome, not a raw run-level Err");

    // The node failed loud, naming the credential AND the broker error (mirror of the
    // unresolvable-`None` path — not silent, not a raw run-level Err).
    let (node, msg) = outcome
        .failed
        .expect("a broker resolve error fails the node");
    assert_eq!(node, n1, "the failing node is named");
    assert!(
        msg.contains("credential 'api_token'") && msg.contains("broker errored"),
        "the failure names the credential AND the broker error: {msg}"
    );
    // A `NodeFailed` for n1 is journaled (journal-everything parity with the None path).
    let events = journal.load(run).await.unwrap();
    assert!(
        events.iter().any(|(_, e)| matches!(
            e,
            JournalEvent::NodeFailed { node, .. } if node == &n1
        )),
        "a NodeFailed is journaled for the broker error"
    );
    // The tool never executed under a half-populated ctx (fail closes BEFORE `call_ctx`).
    assert!(
        seen.lock().unwrap().is_empty(),
        "the tool is never invoked when its broker errors"
    );
}

// ---------------------------------------------------------------------------
// SP-4 s3 workspace-jail e2e (Task 3)
// ---------------------------------------------------------------------------

/// A single-agent registry for the SP-4 workspace e2e tests: agent "a" on chain
/// "c" that LISTS `tools` and holds `grants`, with the real `fs_write`/`fs_read`
/// specs compiled into the prompt (their executable side is wired via a
/// `ToolRegistry`). Mirrors `writer_registry`, but for the confined fs tools.
fn fs_registry(
    tools: Vec<String>,
    grants: std::collections::HashMap<String, Permissions>,
) -> Arc<Registry> {
    Arc::new(
        Registry::default()
            .with_agent(AgentDefinition {
                grants,
                tools,
                ..agent_def("c")
            })
            .with_tool(crate::agent::tools::FsWriteTool.spec())
            .with_tool(crate::agent::tools::FsReadTool.spec())
            // SP-4 s4: also compile the `shell` spec so an agent that LISTS `shell` prompts
            // correctly (`assemble_prompt` only compiles an agent's listed tools, so the fs-only
            // s3 tests that never list `shell` are unaffected).
            .with_tool(crate::agent::tools::ShellTool.spec()),
    )
}

/// SP-4 s3 e2e (AC2/AC3): a full agent run writes then reads a file inside its
/// per-run workspace jail — the file really lands on disk and both effects journal
/// their real `{bytes,path}` / `{content}` outputs.
#[tokio::test]
async fn fs_tools_round_trip_within_the_workspace_jail() {
    let base = tempfile::tempdir().unwrap();
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (gateway, _calls) = scripted_gateway(vec![
        tool_call_response(
            "t1",
            "fs_write",
            r#"{"path":"work/notes.md","content":"hello"}"#,
        ),
        tool_call_response("t2", "fs_read", r#"{"path":"work/notes.md"}"#),
        final_response("done"),
    ])
    .await;
    let grants = std::collections::HashMap::from([
        ("fs_write".to_string(), path_grant(&["work"])),
        ("fs_read".to_string(), path_grant(&["work"])),
    ]);
    let tools = Arc::new(
        ToolRegistry::default()
            .with_tool(Arc::new(crate::agent::tools::FsWriteTool))
            .with_tool(Arc::new(crate::agent::tools::FsReadTool)),
    );
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(fs_registry(
            vec!["fs_write".into(), "fs_read".into()],
            grants,
        ))
        .with_tools(tools)
        .with_workspace_root(base.path());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "write then read")],
    };
    let outcome = exec.run(run, &graph).await.expect("run");

    assert!(
        outcome.failed.is_none() && outcome.paused.is_none(),
        "failed={:?} paused={:?}",
        outcome.failed,
        outcome.paused
    );
    // The file really exists on disk in the per-run jail.
    let path_on_disk = base
        .path()
        .join(run.0.to_string())
        .join("work")
        .join("notes.md");
    assert_eq!(std::fs::read_to_string(&path_on_disk).unwrap(), "hello");
    // fs_write (turn 0, tool idx 1) journaled {bytes,path}; fs_read (turn 1, idx 1) content.
    let events = journal.load(run).await.unwrap();
    assert_eq!(
        recorded_output(&events, &effect_id("n1", 0, 1)).unwrap(),
        serde_json::json!({"bytes": 5, "path": "work/notes.md"})
    );
    assert_eq!(
        recorded_output(&events, &effect_id("n1", 1, 1)).unwrap(),
        serde_json::json!({"content": "hello"})
    );
}

/// SP-4 s3 e2e (AC4): the jail denies a symlink-out path that s1 GRANTS (grant
/// covers `work/…`, no `..`) — proving the jail's unique contribution over s1's
/// lexical `covers`. Only the executor's jail pre-check produces this shape (a
/// clean Pure `permission_denied`, no `EffectIntent`, run still clean); without it,
/// s1 would allow and the two-phase Mutation would journal an Intent.
#[tokio::test]
async fn jail_denies_symlink_escape_that_s1_would_allow() {
    let base = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(outside.path().join("sub")).unwrap();
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    // Pre-create the per-run jail + a symlink `work/evil -> <outside>` inside it.
    let run_dir = base.path().join(run.0.to_string());
    std::fs::create_dir_all(run_dir.join("work")).unwrap();
    std::os::unix::fs::symlink(outside.path(), run_dir.join("work").join("evil")).unwrap();

    let (gateway, _calls) = scripted_gateway(vec![
        tool_call_response(
            "t1",
            "fs_write",
            r#"{"path":"work/evil/sub/x","content":"pwned"}"#,
        ),
        final_response("done"),
    ])
    .await;
    // s1 COVERS work/evil/sub/x (a `work` prefix grant, `..`-free) — only the jail denies.
    let grants = std::collections::HashMap::from([("fs_write".to_string(), path_grant(&["work"]))]);
    let tools =
        Arc::new(ToolRegistry::default().with_tool(Arc::new(crate::agent::tools::FsWriteTool)));
    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(fs_registry(vec!["fs_write".into()], grants))
        .with_tools(tools)
        .with_workspace_root(base.path());
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "write")],
    };
    let outcome = exec.run(run, &graph).await.expect("run");

    // The denial is fed back to the agent, which then finalizes — a CLEAN run.
    assert!(
        outcome.failed.is_none() && outcome.paused.is_none(),
        "failed={:?} paused={:?}",
        outcome.failed,
        outcome.paused
    );
    // No file escaped into the outside dir.
    assert!(
        !outside.path().join("sub").join("x").exists(),
        "the write ESCAPED the jail"
    );
    // The effect was recorded as a terse DENIAL (Pure), not a Mutation write.
    let events = journal.load(run).await.unwrap();
    let denied_eid = effect_id("n1", 0, 1);
    let out =
        recorded_output(&events, &denied_eid).expect("the denied write recorded a Pure effect");
    assert_eq!(out["error"], serde_json::json!("permission_denied"));
    // No EffectIntent for the (denied) write — it never entered the two-phase path.
    assert!(
        !has_effect_intent(&events, &denied_eid),
        "a denied write must not journal an EffectIntent"
    );
}

/// SP-4 s3 e2e (AC5): two distinct runs writing the SAME relative path land in
/// isolated per-run dirs with distinct contents. Sequential by design — this proves
/// distinct run_ids get distinct directories, not thread-safety.
#[tokio::test]
async fn distinct_runs_get_isolated_workspaces() {
    let base = tempfile::tempdir().unwrap();
    let run_once = |content: &'static str| {
        let base = base.path().to_path_buf();
        async move {
            let journal = InMemoryJournal::new();
            let run = RunId(uuid::Uuid::new_v4());
            let (gateway, _c) = scripted_gateway(vec![
                tool_call_response(
                    "t1",
                    "fs_write",
                    &format!(r#"{{"path":"work/f","content":"{content}"}}"#),
                ),
                final_response("done"),
            ])
            .await;
            let grants =
                std::collections::HashMap::from([("fs_write".to_string(), path_grant(&["work"]))]);
            let tools = Arc::new(
                ToolRegistry::default().with_tool(Arc::new(crate::agent::tools::FsWriteTool)),
            );
            let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
                .with_registry(fs_registry(vec!["fs_write".into()], grants))
                .with_tools(tools)
                .with_workspace_root(base.clone());
            let graph = Graph {
                nodes: vec![agent_node("n1", "a", "write")],
            };
            let o = exec.run(run, &graph).await.unwrap();
            assert!(o.failed.is_none() && o.paused.is_none(), "{:?}", o.failed);
            base.join(run.0.to_string()).join("work").join("f")
        }
    };
    let p1 = run_once("one").await;
    let p2 = run_once("two").await;
    assert_ne!(p1, p2, "runs must not share a path");
    assert_eq!(std::fs::read_to_string(&p1).unwrap(), "one");
    assert_eq!(std::fs::read_to_string(&p2).unwrap(), "two");
}

// ---------------------------------------------------------------------------
// SP-4 s3 workspace-jail — resume exactly-once (AC6) + s2 redaction compose (AC7).
// ---------------------------------------------------------------------------

/// SP-4 s3 (AC6): a COMPLETED `fs_write` (a Mutation) replays `{bytes,path}` from the memo
/// on resume — the tool is NOT re-run, so the file on disk is NOT re-written (exactly-once
/// for a REAL side effect). Decisive proof: after the seed writes `"orig"`, we truncate the
/// journal to the prefix ending at the `fs_write` `EffectRecorded` (asserting the prefix has
/// NO `RunCompleted`, so the resume must drive a real tail) and EXTERNALLY clobber the file
/// on disk with `"SENTINEL"`. A re-run of the write would clobber it BACK to `"orig"`; the
/// surviving `"SENTINEL"` is the load-bearing assertion that the write replayed, not re-ran.
#[tokio::test]
async fn fs_write_replays_from_memo_without_rewriting_on_resume() {
    let base = tempfile::tempdir().unwrap();
    let run = RunId(uuid::Uuid::new_v4());
    let grants = std::collections::HashMap::from([("fs_write".to_string(), path_grant(&["work"]))]);
    let tool_eid = effect_id("n1", 0, 1); // turn 0, tool idx 1 (idx 0 is the model turn)
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "write")],
    };

    // --- seed run: write "orig" to completion, sharing `base` + `run` with the resume. ---
    let seed = InMemoryJournal::new();
    let (gw1, _c1) = scripted_gateway(vec![
        tool_call_response("t1", "fs_write", r#"{"path":"work/f","content":"orig"}"#),
        final_response("done"),
    ])
    .await;
    let tools1 =
        Arc::new(ToolRegistry::default().with_tool(Arc::new(crate::agent::tools::FsWriteTool)));
    Executor::new(Arc::new(gw1), Arc::new(seed.clone()), "v1")
        .with_registry(fs_registry(vec!["fs_write".into()], grants.clone()))
        .with_tools(tools1)
        .with_workspace_root(base.path())
        .run(run, &graph)
        .await
        .expect("seed run completes");
    let file = base.path().join(run.0.to_string()).join("work").join("f");
    assert_eq!(std::fs::read_to_string(&file).unwrap(), "orig");

    // Truncate to the prefix ending at the fs_write's `EffectRecorded` — the effect is
    // memoized, but the turn-1 final model call + NodeCompleted + RunCompleted are dropped,
    // so the resume MUST drive the tail (and would re-write the file if it re-ran the tool).
    let events = seed.load(run).await.unwrap();
    let cut = events
        .iter()
        .position(|(_, e)| {
            matches!(e, JournalEvent::EffectRecorded { effect_id, .. } if effect_id == &tool_eid)
        })
        .expect("seed run journaled the fs_write EffectRecorded");
    assert!(
        !events[..=cut]
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "the truncated seed is a partial (no RunCompleted) — the resume must drive the tail"
    );
    let seeded = InMemoryJournal::new();
    for (_, e) in &events[..=cut] {
        seeded.append(run, e.clone()).await.unwrap();
    }
    // Externally clobber the file: a resume that RE-RAN the write would restore "orig".
    std::fs::write(&file, "SENTINEL").unwrap();

    // --- resume: fs_write memo-replays; the file must NOT be rewritten. ---
    let (gw2, _c2) = scripted_gateway(vec![final_response("done")]).await;
    let tools2 =
        Arc::new(ToolRegistry::default().with_tool(Arc::new(crate::agent::tools::FsWriteTool)));
    let outcome = Executor::new(Arc::new(gw2), Arc::new(seeded.clone()), "v1")
        .with_registry(fs_registry(vec!["fs_write".into()], grants))
        .with_tools(tools2)
        .with_workspace_root(base.path())
        .start(run, &graph)
        .await
        .expect("resume yields an outcome");

    assert!(
        outcome.failed.is_none() && outcome.paused.is_none(),
        "resume completes with no DeterminismViolation: failed={:?} paused={:?}",
        outcome.failed,
        outcome.paused
    );
    // Load-bearing: the write replayed from the memo — the on-disk sentinel SURVIVES.
    assert_eq!(
        std::fs::read_to_string(&file).unwrap(),
        "SENTINEL",
        "resume RE-WROTE the file — not exactly-once"
    );
    // And the effect is recorded exactly once across seed + resume (replayed, not re-run).
    assert_eq!(
        effect_recorded_count(&seeded.load(run).await.unwrap(), &tool_eid),
        1,
        "the fs_write effect is recorded exactly once (memo replay, no re-record)"
    );
}

/// SP-4 s3 × s2 (AC7): the s2 `PatternRedactor` composes over REAL file content — a secret
/// stored as a file's bytes, read back via `fs_read`, is `[REDACTED]` in the journaled effect
/// output AND appears NOWHERE in the journal. The secret is pre-seeded as file content (not
/// routed through a model `fs_write` tool-call argument): tool-call `arguments` are journaled
/// UNREDACTED by design (see `model_text_secret_is_redacted_tool_calls_intact`), so writing
/// the secret via the agent would leak the plaintext into the turn's `tool_calls` and defeat
/// the whole-journal scan. Reading real on-disk content is the faithful test of AC7.
/// The secret is assembled at RUNTIME (the repo's semgrep CWE-798 hook blocks literals).
#[tokio::test]
async fn fs_read_output_is_redacted() {
    let base = tempfile::tempdir().unwrap();
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    // Matches the s2 `sk-[A-Za-z0-9_-]{20,}` pattern (20 alnum after `sk-`).
    let secret = format!("sk-{}", "abcdefghij0123456789");

    // Pre-seed the secret as REAL file content inside the per-run jail. Pre-creating
    // `base/<run>/work/` is safe + idempotent (the executor's lazy `create_dir_all` +
    // `canonicalize` resolves to the same dir on the first tool call). The agent then
    // `fs_read`s it back.
    let run_dir = base.path().join(run.0.to_string());
    std::fs::create_dir_all(run_dir.join("work")).unwrap();
    std::fs::write(run_dir.join("work").join("s"), &secret).unwrap();

    let grants = std::collections::HashMap::from([("fs_read".to_string(), path_grant(&["work"]))]);
    let (gateway, _c) = scripted_gateway(vec![
        tool_call_response("t1", "fs_read", r#"{"path":"work/s"}"#),
        final_response("done"),
    ])
    .await;
    let tools =
        Arc::new(ToolRegistry::default().with_tool(Arc::new(crate::agent::tools::FsReadTool)));
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "read the file")],
    };
    let outcome = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(fs_registry(vec!["fs_read".into()], grants))
        .with_tools(tools)
        .with_workspace_root(base.path())
        .with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()))
        .run(run, &graph)
        .await
        .expect("run");

    assert!(
        outcome.failed.is_none() && outcome.paused.is_none(),
        "failed={:?} paused={:?}",
        outcome.failed,
        outcome.paused
    );
    // fs_read is turn-0, tool idx 1 → its journaled output is redacted (real content in → out).
    let events = journal.load(run).await.unwrap();
    let read_out =
        recorded_output(&events, &effect_id("n1", 0, 1)).expect("fs_read effect recorded");
    assert_eq!(
        read_out,
        serde_json::json!({ "content": "[REDACTED]" }),
        "fs_read output must be redacted (real file content in → [REDACTED] out): {read_out}"
    );
    assert!(
        !serde_json::to_string(&read_out).unwrap().contains(&secret),
        "secret leaked in the fs_read output"
    );
    // Whole-journal scan: the plaintext appears NOWHERE (it lives only on disk).
    assert!(
        !serde_json::to_string(&events).unwrap().contains(&secret),
        "secret leaked in the journal"
    );
}

// ---------------------------------------------------------------------------
// SP-4 s4 subprocess-sandbox e2e (Task 4) — portable (fake Sandbox, CI-testable).
// ---------------------------------------------------------------------------

/// A portable fake `Sandbox`: counts spawns, CAPTURES the policy it was handed (the caps + the
/// per-run workspace), and returns a canned outcome — it runs NO real subprocess, so these
/// e2e tests are deterministic on Linux CI. The load-bearing property under test is AC8: the
/// executor hands the sandbox the GRANT's caps + the per-run workspace (NOT anything the tool
/// supplied), proving the argv-only tool can't widen the policy.
struct FakeSandbox {
    spawns: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    seen: std::sync::Mutex<
        Option<(
            orchestrator_core::ResourceCaps,
            std::path::PathBuf,
            orchestrator_core::NetworkPolicy,
        )>,
    >,
    stdout: String,
}
impl crate::agent::sandbox::Sandbox for FakeSandbox {
    fn run(
        &self,
        spec: &crate::agent::sandbox::SandboxSpec,
    ) -> Result<crate::agent::sandbox::CapOutcome, OrchestratorError> {
        self.spawns
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        *self.seen.lock().unwrap() = Some((
            spec.caps.clone(),
            spec.workspace.to_path_buf(),
            spec.network.clone(),
        ));
        Ok(crate::agent::sandbox::CapOutcome {
            exit_code: Some(0),
            stdout: self.stdout.clone(),
            stderr: String::new(),
            killed: None,
        })
    }
}

/// SP-4 s4 e2e (AC8): an agent calls `shell`; the executor builds a `BoundSandbox` from the
/// GRANT (caps/network) + the per-run workspace and runs the argv through it; the outcome is
/// journaled. Asserts the run completes, the sandbox is spawned exactly once, the journaled
/// shell output carries the canned stdout, AND the sandbox SAW the grant's `wall_ms` cap + the
/// canonical per-run workspace `base/<run_id>` (never a tool-supplied policy).
#[tokio::test]
async fn shell_runs_through_the_sandbox_and_journals() {
    let base = tempfile::tempdir().unwrap();
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (gateway, _c) = scripted_gateway(vec![
        tool_call_response("t1", "shell", r#"{"argv":["echo","hello"]}"#),
        final_response("done"),
    ])
    .await;
    // `mem_bytes: None` — a Some(_) mem cap fails closed on macOS via RLIMIT_AS EINVAL (T1); the
    // fake ignores it, but the grant models what the real sandbox would receive.
    let grants = std::collections::HashMap::from([(
        "shell".to_string(),
        Permissions {
            commands: vec!["echo".into()],
            caps: orchestrator_core::ResourceCaps {
                cpu_ms: None,
                mem_bytes: None,
                wall_ms: Some(2000),
            },
            // A NON-default network (default is `Deny`) so the assertion below proves the
            // executor forwards the grant's network — a mis-wire hardcoding `Deny`/`Any` fails.
            network: orchestrator_core::NetworkPolicy::Hosts(vec!["api.example.com".to_string()]),
            ..Default::default()
        },
    )]);
    let spawns = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let fake = std::sync::Arc::new(FakeSandbox {
        spawns: spawns.clone(),
        seen: std::sync::Mutex::new(None),
        stdout: "hello\n".into(),
    });
    let tools =
        Arc::new(ToolRegistry::default().with_tool(Arc::new(crate::agent::tools::ShellTool)));
    let outcome = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(fs_registry(vec!["shell".into()], grants))
        .with_tools(tools)
        .with_workspace_root(base.path())
        .with_sandbox(fake.clone())
        .run(
            run,
            &Graph {
                nodes: vec![agent_node("n1", "a", "run a command")],
            },
        )
        .await
        .unwrap();

    assert!(
        outcome.failed.is_none() && outcome.paused.is_none(),
        "failed={:?} paused={:?}",
        outcome.failed,
        outcome.paused
    );
    assert_eq!(
        spawns.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the sandbox must be spawned exactly once"
    );
    // shell is turn-0, tool idx 1 → its journaled output carries the canned stdout.
    let events = journal.load(run).await.unwrap();
    let out = recorded_output(&events, &effect_id("n1", 0, 1)).expect("shell effect recorded");
    assert_eq!(out["stdout"], serde_json::json!("hello\n"));
    // AC8: the sandbox saw the GRANT's caps + network + the per-run workspace — all three
    // policy dimensions are grant-derived (not tool- or default-supplied), completing the
    // grant → BoundSandbox → SandboxSpec provenance the argv-only tool cannot widen.
    let (caps, ws, net) = fake.seen.lock().unwrap().clone().unwrap();
    assert_eq!(
        caps.wall_ms,
        Some(2000),
        "the sandbox saw the grant's wall cap"
    );
    // The executor canonicalizes the per-run workspace (`workspace_root_for`), so compare
    // against the canonicalized `base/<run_id>` (on macOS `/var` → `/private/var`).
    let expected_ws = base.path().join(run.0.to_string()).canonicalize().unwrap();
    assert_eq!(ws, expected_ws, "the sandbox saw the per-run workspace");
    assert_eq!(
        net,
        orchestrator_core::NetworkPolicy::Hosts(vec!["api.example.com".to_string()]),
        "the sandbox saw the grant's network policy"
    );
}

/// SP-4 s4 e2e (AC5): NO sandbox wired ⇒ `shell` refuses LOUD (fail-closed) — the tested
/// behavior on Linux/CI until an OS-confinement backend lands there. The refusal surfaces via
/// `record_tool_effect`'s `Err` arm as a `NodeFailed` carrying the `call_ctx` error message, so
/// a whole-journal scan finds the "sandbox required" refusal.
#[tokio::test]
async fn shell_refuses_loud_without_a_sandbox() {
    let base = tempfile::tempdir().unwrap();
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (gateway, _c) = scripted_gateway(vec![
        tool_call_response("t1", "shell", r#"{"argv":["echo","hi"]}"#),
        final_response("done"),
    ])
    .await;
    let grants = std::collections::HashMap::from([(
        "shell".to_string(),
        Permissions {
            commands: vec!["echo".into()],
            ..Default::default()
        },
    )]);
    let tools =
        Arc::new(ToolRegistry::default().with_tool(Arc::new(crate::agent::tools::ShellTool)));
    let outcome = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(fs_registry(vec!["shell".into()], grants))
        .with_tools(tools)
        .with_workspace_root(base.path())
        // NO .with_sandbox(...) — `bound_sandbox_for` returns None ⇒ ctx.sandbox is None.
        .run(
            run,
            &Graph {
                nodes: vec![agent_node("n1", "a", "run a command")],
            },
        )
        .await
        .expect("the refusal surfaces as an outcome, not a raw run-level Err");

    // The node failed loud (the shell tool refused), naming the missing sandbox.
    let (node, msg) = outcome.failed.expect("the shell refusal fails the node");
    assert_eq!(node, NodeId("n1".into()));
    assert!(
        msg.contains("sandbox required"),
        "the failure names the missing sandbox: {msg}"
    );
    // And the refusal is journaled loud (a NodeFailed carrying the message).
    let events = journal.load(run).await.unwrap();
    assert!(
        serde_json::to_string(&events)
            .unwrap()
            .contains("sandbox required"),
        "expected a loud 'sandbox required' refusal in the journal"
    );
}

// ---------------------------------------------------------------------------
// SP-4 s4 subprocess-sandbox (Task 6) — resume exactly-once (AC9) + s2 redaction
// compose over stdout (AC10). Portable (fake Sandbox, CI-testable).
// ---------------------------------------------------------------------------

/// SP-4 s4 (AC9): a COMPLETED `shell` (a Mutation) replays `{exit_code,stdout,stderr,killed}`
/// from the memo on resume — the sandbox is NOT re-invoked, so no subprocess is re-spawned
/// (exactly-once for a real side effect). Mirrors `fs_write_replays_from_memo_...`: the seed runs
/// the shell to completion (spawn counter == 1), we truncate the journal to the prefix ending at
/// the shell `EffectRecorded` (asserting NO `RunCompleted`, so the resume must drive a real tail),
/// copy it into a fresh journal via the same per-event `seeded.append(run, e.clone())` helper, and
/// resume with a FRESH `FakeSandbox` (fresh counter). The load-bearing assertion is that the
/// resume's spawn counter stays 0 — the memoized shell replayed, the sandbox was never re-called.
#[tokio::test]
async fn shell_replays_from_memo_without_respawning_on_resume() {
    let base = tempfile::tempdir().unwrap();
    let run = RunId(uuid::Uuid::new_v4());
    let tool_eid = effect_id("n1", 0, 1); // turn 0, tool idx 1 (idx 0 is the model turn)
    let graph = Graph {
        nodes: vec![agent_node("n1", "a", "run a command")],
    };
    // `mem_bytes: None` — a Some(_) mem cap fails closed on macOS via RLIMIT_AS (T1); the fake
    // ignores it, but the grant models what the real sandbox would receive.
    let grants = std::collections::HashMap::from([(
        "shell".to_string(),
        Permissions {
            commands: vec!["echo".into()],
            caps: orchestrator_core::ResourceCaps {
                cpu_ms: None,
                mem_bytes: None,
                wall_ms: Some(2000),
            },
            ..Default::default()
        },
    )]);

    // --- seed run: run `shell` to completion through FakeSandbox #1 (spawn counter == 1),
    // sharing `base` + `run` with the resume. ---
    let seed = InMemoryJournal::new();
    let (gw1, _c1) = scripted_gateway(vec![
        tool_call_response("t1", "shell", r#"{"argv":["echo","hello"]}"#),
        final_response("done"),
    ])
    .await;
    let seed_spawns = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let fake1 = std::sync::Arc::new(FakeSandbox {
        spawns: seed_spawns.clone(),
        seen: std::sync::Mutex::new(None),
        stdout: "hello\n".into(),
    });
    let tools1 =
        Arc::new(ToolRegistry::default().with_tool(Arc::new(crate::agent::tools::ShellTool)));
    Executor::new(Arc::new(gw1), Arc::new(seed.clone()), "v1")
        .with_registry(fs_registry(vec!["shell".into()], grants.clone()))
        .with_tools(tools1)
        .with_workspace_root(base.path())
        .with_sandbox(fake1.clone())
        .run(run, &graph)
        .await
        .expect("seed run completes");
    assert_eq!(
        seed_spawns.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "the seed run spawned the sandbox exactly once"
    );

    // Truncate to the prefix ending at the shell's `EffectRecorded` — the effect is memoized, but
    // the turn-1 final model call + NodeCompleted + RunCompleted are dropped, so the resume MUST
    // drive the tail (and would re-spawn the sandbox if it re-ran the tool).
    let events = seed.load(run).await.unwrap();
    let cut = events
        .iter()
        .position(|(_, e)| {
            matches!(e, JournalEvent::EffectRecorded { effect_id, .. } if effect_id == &tool_eid)
        })
        .expect("seed run journaled the shell EffectRecorded");
    assert!(
        !events[..=cut]
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "the truncated seed is a partial (no RunCompleted) — the resume must drive the tail"
    );
    let seeded = InMemoryJournal::new();
    for (_, e) in &events[..=cut] {
        seeded.append(run, e.clone()).await.unwrap();
    }

    // --- resume: FRESH FakeSandbox #2 (fresh counter); the memoized shell must NOT re-spawn. ---
    let resume_spawns = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let fake2 = std::sync::Arc::new(FakeSandbox {
        spawns: resume_spawns.clone(),
        seen: std::sync::Mutex::new(None),
        stdout: "hello\n".into(),
    });
    let (gw2, _c2) = scripted_gateway(vec![final_response("done")]).await;
    let tools2 =
        Arc::new(ToolRegistry::default().with_tool(Arc::new(crate::agent::tools::ShellTool)));
    let outcome = Executor::new(Arc::new(gw2), Arc::new(seeded.clone()), "v1")
        .with_registry(fs_registry(vec!["shell".into()], grants))
        .with_tools(tools2)
        .with_workspace_root(base.path())
        .with_sandbox(fake2.clone())
        .start(run, &graph)
        .await
        .expect("resume yields an outcome");

    // Load-bearing: the completed shell replayed from the memo — the sandbox was NOT re-invoked.
    assert_eq!(
        resume_spawns.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the resume did NOT re-spawn — the memoized shell effect replayed from the journal"
    );
    assert!(
        outcome.failed.is_none() && outcome.paused.is_none(),
        "resume completes with no DeterminismViolation: failed={:?} paused={:?}",
        outcome.failed,
        outcome.paused
    );
    // The shell effect is recorded exactly once across seed + resume (replayed, not re-run).
    assert_eq!(
        effect_recorded_count(&seeded.load(run).await.unwrap(), &tool_eid),
        1,
        "the shell effect is recorded exactly once (memo replay, no re-record)"
    );
}

/// SP-4 s4 × s2 (AC10): the s2 `PatternRedactor` composes over the `shell` tool's stdout — a
/// secret emitted on the subprocess's stdout is `[REDACTED]` in the journaled effect output AND
/// appears NOWHERE in the journal. The `FakeSandbox` returns the secret on stdout; the secret is
/// assembled at RUNTIME (the repo's semgrep CWE-798 hook blocks literals).
#[tokio::test]
async fn shell_stdout_is_redacted() {
    let base = tempfile::tempdir().unwrap();
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    // Matches the s2 `sk-[A-Za-z0-9_-]{20,}` pattern (20 alnum after `sk-`).
    let secret = format!("sk-{}", "abcdefghij0123456789");
    let grants = std::collections::HashMap::from([(
        "shell".to_string(),
        Permissions {
            commands: vec!["echo".into()],
            caps: orchestrator_core::ResourceCaps {
                cpu_ms: None,
                mem_bytes: None,
                wall_ms: Some(2000),
            },
            ..Default::default()
        },
    )]);
    let (gateway, _c) = scripted_gateway(vec![
        tool_call_response("t1", "shell", r#"{"argv":["echo","secret"]}"#),
        final_response("done"),
    ])
    .await;
    // The subprocess "emits" the secret on stdout.
    let fake = std::sync::Arc::new(FakeSandbox {
        spawns: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        seen: std::sync::Mutex::new(None),
        stdout: secret.clone(),
    });
    let tools =
        Arc::new(ToolRegistry::default().with_tool(Arc::new(crate::agent::tools::ShellTool)));
    let outcome = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(fs_registry(vec!["shell".into()], grants))
        .with_tools(tools)
        .with_workspace_root(base.path())
        .with_sandbox(fake.clone())
        .with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()))
        .run(
            run,
            &Graph {
                nodes: vec![agent_node("n1", "a", "run a command")],
            },
        )
        .await
        .expect("run");

    assert!(
        outcome.failed.is_none() && outcome.paused.is_none(),
        "failed={:?} paused={:?}",
        outcome.failed,
        outcome.paused
    );
    // shell is turn-0, tool idx 1 → its journaled output's stdout is redacted (secret in → out).
    let events = journal.load(run).await.unwrap();
    let out = recorded_output(&events, &effect_id("n1", 0, 1)).expect("shell effect recorded");
    assert_eq!(
        out["stdout"],
        serde_json::json!("[REDACTED]"),
        "the shell stdout must be redacted (secret in → [REDACTED] out): {out}"
    );
    assert!(
        !serde_json::to_string(&out).unwrap().contains(&secret),
        "secret leaked in the shell output: {out}"
    );
    // Whole-journal scan: the plaintext appears NOWHERE.
    assert!(
        !serde_json::to_string(&events).unwrap().contains(&secret),
        "secret leaked in the journal"
    );
}

// ======================= SP-DATA-5 budget gate (chokepoint) ===================
//
// One test per model-output PRODUCER. There are four `gateway.execute()` sites and
// SP-4 s2's review found the secret redactor wired into only ONE of them; the same
// miss here would let a producer spend real tokens past the operator's cap. These
// four are the coverage proof for the single `dispatch_metered` chokepoint: remove
// the `spent >= cap` check and ALL FOUR must fail.
//
// Each test seeds a resumable journal whose folded spend already meets a small
// budget, using an in-graph MEMOIZED effect to carry the spend — so the seeded work
// replays for free and the producer under test is the only live dispatcher left.

/// A budgeted `RunStarted` for a seeded journal. `"v1"` matches the executor's
/// fence version, so `start` resumes rather than refusing.
fn run_started_with_budget(cap: u64) -> JournalEvent {
    JournalEvent::RunStarted {
        version: "v1".into(),
        budget: Some(orchestrator_core::TokenBudget { total_tokens: cap }),
    }
}

/// A memoized `EffectRecorded` carrying `total_tokens` of spend: the seed that makes
/// a resumed run's folded spend meet its cap with NO live call. `ih` must be the real
/// `input_hash(chain, payload)` of the node it stands in for, or the replay is a
/// `DeterminismViolation` instead of a memo hit.
fn spent_effect(node: &str, eid: EffectId, ih: String, total_tokens: u32) -> JournalEvent {
    JournalEvent::EffectRecorded {
        node: NodeId(node.into()),
        effect_id: eid,
        class: EffectClass::Pure,
        input_hash: ih,
        seq: 0,
        output: EffectOutput::Inline(serde_json::json!({ "model": "m", "text": "seeded" })),
        observation: None,
        usage: Some(orchestrator_core::TokenUsage {
            input_tokens: 0,
            output_tokens: total_tokens,
            total_tokens,
        }),
    }
}

/// The shared assertion for every producer: the run PAUSED at `node` with a budget
/// reason, the pause is the HOTL class (`resume_after: None` — no amount of waiting
/// refills a budget, only an operator raising the cap does), and the gateway was
/// never called.
fn assert_budget_paused(
    events: &[(Seq, JournalEvent)],
    outcome: &RunOutcome,
    calls: &CallLog,
    node: &str,
) {
    assert_eq!(
        calls.lock().unwrap().len(),
        0,
        "an exhausted budget must dispatch NOTHING — this is the whole point of the gate"
    );
    let pause = outcome
        .paused
        .as_ref()
        .expect("an exhausted budget pauses the run");
    assert_eq!(pause.node.0, node, "paused at the producer under test");
    assert!(
        pause.reason.starts_with("budget: "),
        "budget pause reason: {}",
        pause.reason
    );
    let resume_after = events
        .iter()
        .find_map(|(_, e)| match e {
            JournalEvent::RunPaused {
                reason,
                resume_after,
            } if reason.starts_with("budget: ") => Some(*resume_after),
            _ => None,
        })
        .expect("the budget pause is journaled as RunPaused");
    assert!(
        resume_after.is_none(),
        "a budget pause is the HOTL class: no deadline to auto-wake on"
    );
    assert!(
        !events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "a paused run stays resumable — never marked complete"
    );
}

/// Producer 1/4 — the ReAct turn (`dispatch_model_turn`, `agent.rs`). `n0` replays
/// from the memo carrying the spend; the agent node's turn 0 is the live dispatcher.
#[tokio::test]
async fn budget_gate_stops_the_react_turn_producer() {
    let (gateway, calls) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![
            Node {
                id: NodeId("n0".into()),
                kind: model_call("c", "p0"),
                deps: vec![],
            },
            Node {
                id: NodeId("n1".into()),
                kind: NodeKind::Agent {
                    agent: AgentRef("a".into()),
                    input: serde_json::json!("hi"),
                    phase: None,
                },
                deps: vec![Dep::hard("n0")],
            },
        ],
    };

    let ih = input_hash("c", &serde_json::json!({ "prompt": "p0" })).unwrap();
    journal
        .append(run, run_started_with_budget(100))
        .await
        .unwrap();
    journal
        .append(run, spent_effect("n0", effect_id("n0", 0, 0), ih, 120))
        .await
        .unwrap();
    journal
        .append(
            run,
            JournalEvent::NodeCompleted {
                node: NodeId("n0".into()),
            },
        )
        .await
        .unwrap();

    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(agent_registry("c"));
    let out = exec.start(run, &graph).await.expect("drives");
    assert_budget_paused(&journal.load(run).await.unwrap(), &out, &calls, "n1");
}

/// Producer 2/4 — the `ModelCall` node (`run_node`, `mod.rs`). `n1` replays from the
/// memo carrying the spend; `n2` is the live dispatcher.
#[tokio::test]
async fn budget_gate_stops_the_model_call_node_producer() {
    let (gateway, calls) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let (graph, ..) = two_node_graph("p1", "p2");

    let ih = input_hash("c", &serde_json::json!({ "prompt": "p1" })).unwrap();
    journal
        .append(run, run_started_with_budget(100))
        .await
        .unwrap();
    journal
        .append(run, spent_effect("n1", effect_id("n1", 0, 0), ih, 120))
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
    let out = exec.start(run, &graph).await.expect("drives");
    assert_budget_paused(&journal.load(run).await.unwrap(), &out, &calls, "n2");
}

/// Producer 3/4 — a Map item (`run_map_child_modelcall`, `fanout.rs`). Child `m/0`
/// replays from the memo carrying the spend; `m/1` is the live dispatcher, and its
/// refusal pauses the WHOLE Map (the established `MapChildPaused` idiom).
#[tokio::test]
async fn budget_gate_stops_the_map_item_producer() {
    let (gateway, calls) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = map_graph("m", map_items(["i0", "i1"]), Aggregation::BestEffort);

    let ih = input_hash("c", &serde_json::json!({ "prompt": "i0" })).unwrap();
    journal
        .append(run, run_started_with_budget(100))
        .await
        .unwrap();
    journal
        .append(
            run,
            JournalEvent::NodeStarted {
                node: NodeId("m".into()),
            },
        )
        .await
        .unwrap();
    journal
        .append(
            run,
            JournalEvent::MapExpanded {
                node: NodeId("m".into()),
                child_count: 2,
            },
        )
        .await
        .unwrap();
    journal
        .append(run, spent_effect("m/0", effect_id("m/0", 0, 0), ih, 120))
        .await
        .unwrap();

    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let out = exec.start(run, &graph).await.expect("drives");
    assert_budget_paused(&journal.load(run).await.unwrap(), &out, &calls, "m");
}

/// Producer 4/4 — the `Consolidate` synthesis (`run_consolidate`, `fanout.rs`). BOTH
/// Map children replay from the memo (so the Map completes free and the spend is
/// already folded); the synthesis is the only live dispatcher left.
#[tokio::test]
async fn budget_gate_stops_the_consolidate_producer() {
    let (gateway, calls) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![
            Node {
                id: NodeId("m".into()),
                kind: NodeKind::Map {
                    body: MapBody::ModelCall { chain: "c".into() },
                    over: map_items(["i0", "i1"]),
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
        ],
    };

    journal
        .append(run, run_started_with_budget(100))
        .await
        .unwrap();
    journal
        .append(
            run,
            JournalEvent::NodeStarted {
                node: NodeId("m".into()),
            },
        )
        .await
        .unwrap();
    journal
        .append(
            run,
            JournalEvent::MapExpanded {
                node: NodeId("m".into()),
                child_count: 2,
            },
        )
        .await
        .unwrap();
    for (i, item) in ["i0", "i1"].iter().enumerate() {
        let path = format!("m/{i}");
        let ih = input_hash("c", &serde_json::json!({ "prompt": item })).unwrap();
        journal
            .append(run, spent_effect(&path, effect_id(&path, 0, 0), ih, 60))
            .await
            .unwrap();
    }
    journal
        .append(
            run,
            JournalEvent::NodeCompleted {
                node: NodeId("m".into()),
            },
        )
        .await
        .unwrap();

    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let out = exec.start(run, &graph).await.expect("drives");
    assert_budget_paused(&journal.load(run).await.unwrap(), &out, &calls, "cons");
}

// ============================= SP-DATA-5 Task 4: usage capture =================
//
// Task 3 built the `Refusal::Unmetered` chokepoint arm but left it untestable —
// nothing set a budget until now. Task 4 owns proving both halves of the fail-
// closed contract (budgeted ⇒ refuse; unbudgeted ⇒ untouched) AND that a real
// `response.usage` actually lands on the journaled `EffectRecorded`.

/// Fail closed: with a budget set, a provider that reports no usage is refused
/// rather than spent blind (mirrors the sandbox/`shell`/fence precedent of never
/// trusting an unenforceable boundary). `recording_gateway` always returns
/// `usage: None`, so the very first call trips `Refusal::Unmetered` even though
/// the budget itself is nowhere near exhausted (spent 0 < cap 100) — proving the
/// refusal is about METERABILITY, not the cap.
#[tokio::test]
async fn an_unmetered_call_fails_the_node_when_a_budget_is_set() {
    let (gateway, calls) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("n1".into()),
            kind: model_call("c", "p1"),
            deps: vec![],
        }],
    };
    journal
        .append(run, run_started_with_budget(100))
        .await
        .unwrap();

    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let out = exec.start(run, &graph).await.expect("drives");

    assert_eq!(
        calls.lock().unwrap().len(),
        1,
        "the call DOES dispatch — the refusal fires only after the response comes back unmetered"
    );
    let (node, msg) = out
        .failed
        .expect("an unmetered call under a budget fails the node, it does not pause");
    assert_eq!(node.0, "n1");
    assert!(
        msg.contains("unmetered model call") && msg.contains("'m'"),
        "the failure names the model that reported no usage: {msg}"
    );
    let events = journal.load(run).await.unwrap();
    assert!(
        events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::NodeFailed { node, .. } if node.0 == "n1")),
        "the refusal is journaled as a NodeFailed"
    );
    assert!(
        !events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunPaused { .. })),
        "an unmetered call is a hard failure, not the budget-exhausted pause"
    );
}

/// The additivity guarantee: an unmetered response is invisible when no budget is
/// set — every one of the 1312 baseline tests runs a gateway that always reports
/// `usage: None`, and none of them may start failing because Task 4 exists.
#[tokio::test]
async fn an_unmetered_call_is_ignored_when_no_budget_is_set() {
    let (gateway, calls) = recording_gateway().await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("n1".into()),
            kind: model_call("c", "p1"),
            deps: vec![],
        }],
    };

    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let out = exec.run(run, &graph).await.expect("drives");

    assert_eq!(calls.lock().unwrap().len(), 1);
    assert!(out.failed.is_none(), "no budget ⇒ no gate ⇒ no refusal");
    assert!(out.paused.is_none());
    assert_eq!(out.completed, vec![NodeId("n1".into())]);
    let events = journal.load(run).await.unwrap();
    let usage = events.iter().find_map(|(_, e)| match e {
        JournalEvent::EffectRecorded { node, usage, .. } if node.0 == "n1" => Some(*usage),
        _ => None,
    });
    assert_eq!(
        usage,
        Some(None),
        "unbudgeted + unmetered: the record exists and its usage stays None, byte-identical to pre-SP-DATA-5"
    );
}

/// Usage reported by the provider reaches the journal, proven by reading the
/// `EffectRecorded` event back — not by inspecting a call counter, which would
/// pass even if the conversion silently dropped every field.
#[tokio::test]
async fn reported_usage_is_journaled_on_the_effect_record() {
    let reported = kernel::types::cost::TokenUsage {
        input_tokens: 30,
        output_tokens: 70,
        total_tokens: 100,
    };
    let (gateway, _calls) = metered_gateway(Some(reported.clone())).await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("n1".into()),
            kind: model_call("c", "p1"),
            deps: vec![],
        }],
    };

    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let out = exec.run(run, &graph).await.expect("drives");
    assert!(out.failed.is_none() && out.paused.is_none());

    let events = journal.load(run).await.unwrap();
    let usage = events
        .iter()
        .find_map(|(_, e)| match e {
            JournalEvent::EffectRecorded { node, usage, .. } if node.0 == "n1" => Some(*usage),
            _ => None,
        })
        .expect("n1's EffectRecorded is on the journal");
    assert_eq!(
        usage,
        Some(orchestrator_core::TokenUsage {
            input_tokens: 30,
            output_tokens: 70,
            total_tokens: 100,
        }),
        "the reported usage reached the journal through the boundary conversion, field for field"
    );
}

// ---- Whole-slice review, Important: usage CAPTURE, at all four producers ----------
//
// The test above covers the `ModelCall` node (producer 2/4). The other three —
// `dispatch_model_turn` (the ReAct turn), the Map item and the Consolidate synthesis
// — each had `usage: response.usage.map(convert_usage)` on their `EffectRecorded`
// with NO test: mutating any of them to `usage: None` left the entire workspace
// green, PG e2e included.
//
// Neither existing family could catch it. The four Task 3 gate tests resume from a
// SEEDED journal and never dispatch, so nothing captures usage in them at all; the
// Task 6 tests watch the LIVE meter, which charges the ledger at the chokepoint
// independently of what any producer journals. The loss is therefore invisible until
// the NEXT drive, where the durable base is short — the exact "counter restarts at
// zero" failure this slice exists to prevent, and the same shape as Critical 2.
//
// So these assert on the journal, re-read after a real dispatch. Nothing else does.

/// The journaled `EffectRecorded.usage` totals for every node the predicate accepts,
/// in journal order. Reads the DURABLE record — the only thing a later drive sees.
fn journaled_usage_totals(
    events: &[(Seq, JournalEvent)],
    node_pred: impl Fn(&str) -> bool,
) -> Vec<Option<u32>> {
    events
        .iter()
        .filter_map(|(_, e)| match e {
            JournalEvent::EffectRecorded { node, usage, .. } if node_pred(&node.0) => {
                Some(usage.map(|u| u.total_tokens))
            }
            _ => None,
        })
        .collect()
}

/// Producer 1/4 — the ReAct turn (`dispatch_model_turn`, `agent.rs`). BOTH turns'
/// records must carry their usage; a run that ledgers only its last turn is just as
/// broken as one that ledgers none.
///
/// *Mutation:* set `usage: None` on `dispatch_model_turn`'s `EffectRecorded` — this
/// is the only test in the workspace that fails.
#[tokio::test]
async fn the_react_turn_producer_journals_its_reported_usage() {
    let usage = kernel::types::cost::TokenUsage {
        input_tokens: 40,
        output_tokens: 60,
        total_tokens: 100,
    };
    let with_usage = |mut r: kernel::types::io::ChatResponse| {
        r.usage = Some(usage.clone());
        r
    };
    let (gateway, calls) = scripted_gateway(vec![
        with_usage(tool_call_response(
            "t0",
            "calc",
            "{\"op\":\"add\",\"a\":1,\"b\":1}",
        )),
        with_usage(final_response("done")),
    ])
    .await;
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
        .run(run, &graph)
        .await
        .expect("drives");
    assert!(out.failed.is_none() && out.paused.is_none(), "{out:?}");
    assert_eq!(calls.lock().unwrap().len(), 2, "two live model turns");

    let events = journal.load(run).await.unwrap();
    // The agent's tool call also journals an effect under `n1`, and it is NOT a model
    // call — it must stay unmetered. So filter to the model turns by their non-None
    // expectation being exactly the two turns: assert the two model-turn records carry
    // usage and that the ledger totals them.
    let totals = journaled_usage_totals(&events, |n| n == "n1");
    assert_eq!(
        totals.iter().filter(|u| **u == Some(100)).count(),
        2,
        "both ReAct turns journal their 100 tokens: {totals:?}"
    );
    assert_eq!(
        crate::spend_of(&events).0,
        200,
        "the durable ledger — what the NEXT drive folds — carries both turns"
    );
}

/// Producer 3/4 — a Map item (`run_map_child_modelcall`, `fanout.rs`). Every child's
/// own record must carry its own spend.
///
/// *Mutation:* set `usage: None` on the Map child's `EffectRecorded` — only this test
/// (and the compaction tests, which read the same records) fails.
#[tokio::test]
async fn the_map_item_producer_journals_its_reported_usage() {
    let (gateway, _calls) = metered_gateway(Some(kernel::types::cost::TokenUsage {
        input_tokens: 40,
        output_tokens: 60,
        total_tokens: 100,
    }))
    .await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = map_graph("m", map_items(["i0", "i1"]), Aggregation::BestEffort);

    let out = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .run(run, &graph)
        .await
        .expect("drives");
    assert!(out.failed.is_none() && out.paused.is_none(), "{out:?}");

    let events = journal.load(run).await.unwrap();
    assert_eq!(
        journaled_usage_totals(&events, |n| n.starts_with("m/")),
        vec![Some(100), Some(100)],
        "each Map child journals its own spend"
    );
    assert_eq!(crate::spend_of(&events).0, 200);
}

/// Producer 4/4 — the `Consolidate` synthesis (`run_consolidate`, `fanout.rs`). No
/// CAS is wired, so compaction is skipped and the synthesis record is read directly.
///
/// *Mutation:* set `usage: None` on the Consolidate's `EffectRecorded` — only this
/// test fails.
#[tokio::test]
async fn the_consolidate_producer_journals_its_reported_usage() {
    let (gateway, _calls) = metered_gateway(Some(kernel::types::cost::TokenUsage {
        input_tokens: 40,
        output_tokens: 60,
        total_tokens: 100,
    }))
    .await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
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
                deps: vec![Dep::soft("m")],
            },
        ],
    };

    let out = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .run(run, &graph)
        .await
        .expect("drives");
    assert!(out.failed.is_none() && out.paused.is_none(), "{out:?}");

    let events = journal.load(run).await.unwrap();
    assert_eq!(
        journaled_usage_totals(&events, |n| n == "cons"),
        vec![Some(100)],
        "the synthesis journals its own spend"
    );
    assert_eq!(crate::spend_of(&events).0, 200, "child + synthesis");
}

// ---- Whole-slice review, Minor: the `spent == cap` boundary ----------------------

/// The gate is `spent >= cap`, but every other budget test overshoots strictly, so
/// mutating it to `spent > cap` left the workspace green. This lands EXACTLY on the
/// cap: two calls at 75 tokens against a cap of 150 means node 3 sees `spent == cap`
/// and must be stopped.
///
/// *Mutation:* `spent >= cap` → `spent > cap` and this fails with three calls and a
/// completed run.
#[tokio::test]
async fn spending_exactly_the_cap_stops_the_run() {
    let (gateway, calls) = metered_gateway(Some(kernel::types::cost::TokenUsage {
        input_tokens: 25,
        output_tokens: 50,
        total_tokens: 75,
    }))
    .await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![
            Node {
                id: NodeId("n1".into()),
                kind: model_call("c", "p1"),
                deps: vec![],
            },
            Node {
                id: NodeId("n2".into()),
                kind: model_call("c", "p2"),
                deps: vec![Dep::hard("n1")],
            },
            Node {
                id: NodeId("n3".into()),
                kind: model_call("c", "p3"),
                deps: vec![Dep::hard("n2")],
            },
        ],
    };

    let out = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .run_budgeted(
            run,
            &graph,
            Some(orchestrator_core::TokenBudget { total_tokens: 150 }),
        )
        .await
        .expect("drives");

    assert_eq!(
        calls.lock().unwrap().len(),
        2,
        "spending exactly the cap is spending it — the third call must not go out"
    );
    let pause = out
        .paused
        .as_ref()
        .expect("landing on the cap pauses the run");
    assert_eq!(pause.node.0, "n3");
    assert!(
        pause.reason.starts_with("budget: 150 of 150"),
        "the reason reports the boundary honestly: {}",
        pause.reason
    );
    assert_eq!(crate::spend_of(&journal.load(run).await.unwrap()).0, 150);
}

// ---- Task 6: the gate fires WITHIN a drive, not only at a drive boundary ----------
//
// Two defects found while writing the AC6 e2e, both of which made a freshly submitted
// budgeted run un-gateable — it spent its whole reachable graph and completed, however
// small the cap:
//
//   1. `run_inner` drove a FRESH run with `Fold::default()`, whose `budget` is `None`.
//      The cap was journaled on `RunStarted` and then never consulted on the very drive
//      it was set for.
//   2. `Fold` is built ONCE per drive and shared as `&Fold`, so `fold.spent()` was
//      frozen at the drive's starting value. Even with the cap present, node 2 gated
//      against node 1's PRE-call ledger — and on a fresh run that value is 0 forever.
//
// Together they made spec §6.5's "overshoot is bounded by at most one call" false: the
// real bound was "everything one drive can reach". These tests pin the fix.

/// A 3-node linear graph under a cap that one call's worth of usage already exceeds.
/// The gate must stop the run at node 2 — INSIDE the first drive, with nothing resumed
/// and nothing re-folded — leaving the overshoot at exactly one call.
///
/// Mutation-verified two ways: seed the fresh fold with `Fold::default()` (dropping the
/// budget) or hand the chokepoint `fold.spent()` instead of the live meter, and this
/// fails with all three nodes completed and 450 tokens spent against a cap of 100.
#[tokio::test]
async fn a_fresh_budgeted_run_pauses_mid_drive_after_one_call() {
    let (gateway, calls) = metered_gateway(Some(kernel::types::cost::TokenUsage {
        input_tokens: 100,
        output_tokens: 50,
        total_tokens: 150,
    }))
    .await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![
            Node {
                id: NodeId("n1".into()),
                kind: model_call("c", "p1"),
                deps: vec![],
            },
            Node {
                id: NodeId("n2".into()),
                kind: model_call("c", "p2"),
                deps: vec![Dep::hard("n1")],
            },
            Node {
                id: NodeId("n3".into()),
                kind: model_call("c", "p3"),
                deps: vec![Dep::hard("n2")],
            },
        ],
    };

    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let out = exec
        .run_budgeted(
            run,
            &graph,
            Some(orchestrator_core::TokenBudget { total_tokens: 100 }),
        )
        .await
        .expect("drives");

    assert_eq!(
        calls.lock().unwrap().len(),
        1,
        "the cap is exceeded by the FIRST call, so exactly one call may escape — the \
         floor-trigger property. More than one means the ledger froze for the drive."
    );
    let pause = out
        .paused
        .as_ref()
        .expect("the second node must be gated inside this very drive");
    assert_eq!(pause.node.0, "n2");
    assert!(pause.reason.starts_with("budget: "), "{}", pause.reason);
    assert_eq!(out.completed, vec![NodeId("n1".into())]);

    let events = journal.load(run).await.unwrap();
    let (spent, budget) = crate::spend_of(&events);
    assert_eq!((spent, budget), (150, Some(100)));
    assert!(
        !events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "a budget-paused run stays resumable"
    );
}

/// The same property one level down: a ReAct agent dispatches once PER TURN inside a
/// single drive, so a frozen ledger would let it burn every one of `max_steps` turns
/// against the spend as it stood before turn 0. Turn 0 spends past the cap; turn 1 must
/// never reach the gateway.
#[tokio::test]
async fn a_budgeted_agent_stops_between_react_turns() {
    let usage = kernel::types::cost::TokenUsage {
        input_tokens: 100,
        output_tokens: 50,
        total_tokens: 150,
    };
    // Turn 0 asks for a tool (so the loop would continue); turn 1 would be the final
    // answer. Both carry usage, so the metering path is exercised, not the unmetered
    // refusal.
    let with_usage = |mut r: kernel::types::io::ChatResponse| {
        r.usage = Some(usage.clone());
        r
    };
    let (gateway, calls) = scripted_gateway(vec![
        with_usage(tool_call_response(
            "t0",
            "calc",
            "{\"op\":\"add\",\"a\":1,\"b\":1}",
        )),
        with_usage(final_response("done")),
    ])
    .await;
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

    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_registry(tool_agent_registry())
        .with_tools(calc_tools());
    let out = exec
        .run_budgeted(
            run,
            &graph,
            Some(orchestrator_core::TokenBudget { total_tokens: 100 }),
        )
        .await
        .expect("drives");

    assert_eq!(
        calls.lock().unwrap().len(),
        1,
        "turn 0 spends past the cap; turn 1 must be gated before it dispatches"
    );
    let pause = out.paused.as_ref().expect("the agent pauses mid-loop");
    assert_eq!(pause.node.0, "n1");
    assert!(pause.reason.starts_with("budget: "), "{}", pause.reason);
}

/// Additivity, restated at the level the two fixes above touched: with NO budget the
/// live ledger is inert and a multi-node run behaves exactly as it always did.
#[tokio::test]
async fn an_unbudgeted_run_is_never_gated_however_much_it_spends() {
    let (gateway, calls) = metered_gateway(Some(kernel::types::cost::TokenUsage {
        input_tokens: 1_000_000,
        output_tokens: 1_000_000,
        total_tokens: 2_000_000,
    }))
    .await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![
            Node {
                id: NodeId("n1".into()),
                kind: model_call("c", "p1"),
                deps: vec![],
            },
            Node {
                id: NodeId("n2".into()),
                kind: model_call("c", "p2"),
                deps: vec![Dep::hard("n1")],
            },
        ],
    };

    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let out = exec.run(run, &graph).await.expect("drives");

    assert_eq!(calls.lock().unwrap().len(), 2);
    assert!(out.paused.is_none(), "no cap ⇒ no gate, at any spend");
    assert_eq!(
        out.completed,
        vec![NodeId("n1".into()), NodeId("n2".into())]
    );
    let (spent, budget) = crate::spend_of(&journal.load(run).await.unwrap());
    assert_eq!(
        (spent, budget),
        (4_000_000, None),
        "spend is still ledgered for an unbudgeted run — it is only never ENFORCED"
    );
}

// ---- Whole-slice review, Critical 1: a CONCURRENT fan-out must not pass the gate en masse ----
//
// `dispatch_metered` reads the ledger, awaits the provider, then charges. `run_map`
// polls all its children under one `join_all`, so before the fix every child that
// held a semaphore permit read the ledger BEFORE any sibling's response returned —
// a deterministic check-then-act, not a memory-ordering race. A Map no wider than
// `min(map.concurrency, executor.concurrency)` (default 8) was never gated at all.
//
// These two tests use `metered_latency_gateway`, the ONLY double in `test_support`
// with a real suspension point. That is the point: against a zero-latency double
// `join_all` runs the children strictly sequentially and the first test passes even
// with the defect present. See `LatencyMeteredAdapter`'s doc comment.

/// One call's worth of latency, long enough that 6 sequential calls (6×) are
/// unmistakably distinguishable from 6 concurrent ones (1×) without being slow.
const FANOUT_DELAY: std::time::Duration = std::time::Duration::from_millis(60);

/// A `Map` over 6 items, each of which would spend 150 tokens, under a 100-token cap
/// and a fan-out wide enough to dispatch every child at once.
///
/// Exactly ONE call may escape — the floor-trigger bound of §6.5. Before the fix this
/// produced **6 calls, 900 tokens, `Completed`, zero pauses**: every child read the
/// ledger while the others were still awaiting the provider.
///
/// *Mutation:* drop the `budget.is_some()` serial gate from `dispatch_metered` and
/// this fails with 6 calls and a completed run.
#[tokio::test]
async fn a_budgeted_map_fanout_dispatches_exactly_one_child_before_the_gate_fires() {
    let (gateway, calls) = metered_latency_gateway(
        Some(kernel::types::cost::TokenUsage {
            input_tokens: 100,
            output_tokens: 50,
            total_tokens: 150,
        }),
        FANOUT_DELAY,
    )
    .await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("m".into()),
            kind: NodeKind::Map {
                body: MapBody::ModelCall { chain: "c".into() },
                over: map_items(["i0", "i1", "i2", "i3", "i4", "i5"]),
                concurrency: 6,
                aggregation: Aggregation::BestEffort,
            },
            deps: vec![],
        }],
    };

    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let out = exec
        .run_budgeted(
            run,
            &graph,
            Some(orchestrator_core::TokenBudget { total_tokens: 100 }),
        )
        .await
        .expect("drives");

    assert_eq!(
        calls.lock().unwrap().len(),
        1,
        "a budgeted run serialises check→dispatch→charge, so the first child's 150 \
         tokens are on the ledger before any sibling checks it"
    );
    let pause = out
        .paused
        .as_ref()
        .expect("the remaining children are gated, which pauses the whole Map");
    assert_eq!(pause.node.0, "m");
    assert!(pause.reason.starts_with("budget: "), "{}", pause.reason);
    let events = journal.load(run).await.unwrap();
    let (spent, budget) = crate::spend_of(&events);
    assert_eq!(
        (spent, budget),
        (150, Some(100)),
        "overshoot is bounded by ONE call even under fan-out"
    );
    assert!(
        !events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "a budget-paused Map stays resumable"
    );
}

/// The other half of the trade: an UNBUDGETED Map keeps its full concurrency. The
/// serial gate is taken only when `budget.is_some()`, so the 1329 pre-existing tests
/// (and every unbudgeted production run) are untouched.
///
/// Asserts both halves — all 6 children dispatch, and the wall clock is nearer one
/// delay than six. The timing assertion has a 4× margin (6 sequential calls take
/// ≥ 6×60 ms = 360 ms; the bound is 240 ms), which is wide enough not to flake on a
/// loaded machine while still failing outright if the gate is taken unconditionally.
#[tokio::test]
async fn an_unbudgeted_map_fanout_keeps_its_concurrency() {
    let (gateway, calls) = metered_latency_gateway(
        Some(kernel::types::cost::TokenUsage {
            input_tokens: 100,
            output_tokens: 50,
            total_tokens: 150,
        }),
        FANOUT_DELAY,
    )
    .await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("m".into()),
            kind: NodeKind::Map {
                body: MapBody::ModelCall { chain: "c".into() },
                over: map_items(["i0", "i1", "i2", "i3", "i4", "i5"]),
                concurrency: 6,
                aggregation: Aggregation::BestEffort,
            },
            deps: vec![],
        }],
    };

    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let started = std::time::Instant::now();
    let out = exec.run(run, &graph).await.expect("drives");
    let elapsed = started.elapsed();

    assert_eq!(
        calls.lock().unwrap().len(),
        6,
        "no cap ⇒ no gate ⇒ no serialisation"
    );
    assert!(out.paused.is_none(), "{:?}", out.paused);
    assert_eq!(out.completed, vec![NodeId("m".into())]);
    assert!(
        elapsed < FANOUT_DELAY * 4,
        "an unbudgeted fan-out must still overlap its calls: {elapsed:?} is closer to \
         6 sequential {FANOUT_DELAY:?} calls than to one"
    );
}

/// The gate is held across an `.await` on the provider, so the obvious hazard is a
/// path that re-enters `dispatch_metered` from INSIDE another call's critical section
/// — a nested `Subgraph`/`Loop` child dispatching while an outer holder waits on it
/// would deadlock permanently.
///
/// By construction it cannot: the critical section's only await is
/// `gateway.execute()`, which never drives executor nodes, and `drive_nested`/
/// `run_loop` acquire nothing themselves — they hand the SAME `&Fold` (hence the same
/// gate) down and each dispatcher takes it from its own task. This test is the
/// empirical half of that argument: the deepest realistic nesting a budgeted run can
/// take — `Loop` → `Subgraph` → concurrent `Map` → `Consolidate` — driven under a
/// budget generous enough to complete. A deadlock shows up as the timeout, not as a
/// hung suite.
#[tokio::test]
async fn a_budgeted_nested_loop_over_a_subgraph_map_does_not_deadlock_on_the_gate() {
    let (gateway, calls) = metered_latency_gateway(
        Some(kernel::types::cost::TokenUsage {
            input_tokens: 2,
            output_tokens: 3,
            total_tokens: 5,
        }),
        std::time::Duration::from_millis(5),
    )
    .await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    // Loop body = a Subgraph = [Map(3, concurrent) → Consolidate]. Two iterations
    // (the pure gate never sees "STOP"), so the gate is taken, released and re-taken
    // across nesting levels many times over.
    let inner = Graph {
        nodes: vec![
            Node {
                id: NodeId("m".into()),
                kind: NodeKind::Map {
                    body: MapBody::ModelCall { chain: "c".into() },
                    over: map_items(["i0", "i1", "i2"]),
                    concurrency: 3,
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
    let graph = Graph {
        nodes: vec![Node {
            id: NodeId("L".into()),
            kind: NodeKind::Loop {
                body: LoopBody::Subgraph(Box::new(inner)),
                input: serde_json::json!({ "prompt": "go" }),
                gate: GateSpec::Pure(LoopGate::TextContains("STOP".into())),
                max_iters: 2,
            },
            deps: vec![],
        }],
    };

    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1");
    let out = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        exec.run_budgeted(
            run,
            &graph,
            // 8 calls × 5 tokens = 40; the cap is far above it so the run completes
            // and the ONLY way this test fails is a hang or an error.
            Some(orchestrator_core::TokenBudget {
                total_tokens: 10_000,
            }),
        ),
    )
    .await
    .expect("the serial gate must not deadlock across nested Loop/Subgraph/Map dispatch")
    .expect("drives");

    assert!(out.failed.is_none(), "{:?}", out.failed);
    assert!(out.paused.is_none(), "{:?}", out.paused);
    // 2 iterations × (3 Map children + 1 Consolidate) = 8 serialised calls.
    assert_eq!(calls.lock().unwrap().len(), 8);
    let (spent, budget) = crate::spend_of(&journal.load(run).await.unwrap());
    assert_eq!((spent, budget), (40, Some(10_000)));
}

// ---- Whole-slice review, Critical 2: compaction must not erase the children's spend ----
//
// `compact_map` collects a completed Map's child `EffectRecorded` seqs into
// `remove_seqs` and REALLY deletes them (both journal impls do), replacing them with
// a `MapCompacted` manifest. Before the fix `CompactChild` carried no `usage`, so a
// `Consolidate` over a `ModelCall` Map deleted that Map's spend from the durable
// ledger permanently — and the next drive folded a base short by exactly the
// children's tokens, which is the "in-memory counter restarts at zero" failure this
// slice exists to prevent, wearing a different hat.

/// A Map(3) + Consolidate with a CAS wired, so the Consolidate triggers compaction.
/// Every one of the 4 calls spends 100 tokens; the durable ledger must still read 400
/// AFTER the children's records are gone.
///
/// *Mutation:* stop populating `CompactChild.usage` (or stop summing it in
/// `fold_journal`'s `MapCompacted` arm) and this fails with a ledger of 100 — the
/// Consolidate's own spend and nothing else.
#[tokio::test]
async fn compaction_preserves_the_map_children_spend_in_the_ledger() {
    use orchestrator_store::InMemoryContentStore;
    let (gateway, calls) = metered_gateway(Some(kernel::types::cost::TokenUsage {
        input_tokens: 60,
        output_tokens: 40,
        total_tokens: 100,
    }))
    .await;
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
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
        ],
    };

    let exec = Executor::new(Arc::new(gateway), Arc::new(journal.clone()), "v1")
        .with_content_store(Arc::new(InMemoryContentStore::new()))
        .with_cas_threshold(8);
    let out = exec.run(run, &graph).await.expect("drives");
    assert!(out.failed.is_none(), "{:?}", out.failed);
    assert_eq!(calls.lock().unwrap().len(), 4, "3 children + 1 synthesis");

    let events = journal.load(run).await.unwrap();
    // The premise: compaction really happened and really removed the child records.
    assert!(
        events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::MapCompacted { .. })),
        "the Consolidate must have compacted the Map, or this test proves nothing"
    );
    assert!(
        !events.iter().any(|(_, e)| matches!(
            e,
            JournalEvent::EffectRecorded { node, .. } if node.0.starts_with("m/")
        )),
        "the children's EffectRecorded really are gone — their spend has nowhere \
         else to live but the manifest"
    );

    let (spent, _) = crate::spend_of(&events);
    assert_eq!(
        spent, 400,
        "compaction must carry the children's 300 tokens onto the manifest, not drop them"
    );
}

/// The consequence the reviewer measured, end to end: with the children's spend
/// erased, a budgeted run's SECOND drive folds a short base and blows through the cap
/// with no operator action and nothing loud in the journal.
///
/// Map(3) + Consolidate + 2 tail nodes at 150/call under a 700-token cap. Serialised
/// (Critical 1's gate), the first drive spends 150·5 = 750 and pauses at the node
/// after the cap is met. Compaction then removes 450 of it. Before the fix the resumed
/// drive read a base of 300, dispatched the rest, and the run COMPLETED at 1050 real
/// tokens against a 700 cap. After the fix the resume re-pauses at the same ledger.
#[tokio::test]
async fn a_compacted_map_cannot_let_a_budgeted_run_overshoot_across_drives() {
    use orchestrator_store::InMemoryContentStore;
    let usage = kernel::types::cost::TokenUsage {
        input_tokens: 100,
        output_tokens: 50,
        total_tokens: 150,
    };
    let content = Arc::new(InMemoryContentStore::new());
    let journal = InMemoryJournal::new();
    let run = RunId(uuid::Uuid::new_v4());
    let graph = Graph {
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
                id: NodeId("t1".into()),
                kind: model_call("c", "t1"),
                deps: vec![Dep::hard("cons")],
            },
            Node {
                id: NodeId("t2".into()),
                kind: model_call("c", "t2"),
                deps: vec![Dep::hard("t1")],
            },
        ],
    };

    // Drive 1: a fresh budgeted run.
    let (gw1, calls1) = metered_gateway(Some(usage.clone())).await;
    let out1 = Executor::new(Arc::new(gw1), Arc::new(journal.clone()), "v1")
        .with_content_store(content.clone())
        .with_cas_threshold(8)
        .run_budgeted(
            run,
            &graph,
            Some(orchestrator_core::TokenBudget { total_tokens: 700 }),
        )
        .await
        .expect("drives");
    assert!(out1.paused.is_some(), "the cap must stop drive 1");
    let spent_live = calls1.lock().unwrap().len() as u64 * 150;
    assert_eq!(
        spent_live, 750,
        "5 serialised calls escape before the cap is met"
    );
    assert_eq!(
        crate::spend_of(&journal.load(run).await.unwrap()).0,
        spent_live,
        "the DURABLE ledger must equal what was really spent, compaction or not"
    );

    // Drive 2: a plain worker tick, exactly as the scheduler would issue it.
    let (gw2, calls2) = metered_gateway(Some(usage.clone())).await;
    let out2 = Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1")
        .with_content_store(content.clone())
        .with_cas_threshold(8)
        .start(run, &graph)
        .await
        .expect("drives");
    assert_eq!(
        calls2.lock().unwrap().len(),
        0,
        "the resumed drive is already over cap and must spend NOTHING"
    );
    assert!(
        out2.paused.is_some() && out2.completed.iter().all(|n| n.0 != "t2"),
        "the run must stay paused, not complete past its cap: {out2:?}"
    );
    let events = journal.load(run).await.unwrap();
    assert!(
        !events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "a run that has blown its cap must never report RunCompleted"
    );
    assert_eq!(crate::spend_of(&events).0, 750);
}

// ============================= SP-DATA-3 scheduler driver =====================

/// The `Scheduler` driver (in-memory store + a fake clock): a run pauses on a timed gate, is recorded
/// `paused` with the journaled deadline, is NOT woken before it, and a `tick` past the deadline wakes it
/// to completion (a second tick no-ops). Mirrors `a_paused_gated_run_reattempts_and_completes_on_resume`
/// (tests.rs) but with the scheduler automating the wake half — the fake clock drives `claim_due`, so the
/// gated-submit / un-gated-wake split needs no fight with the gateway's real-time cooldown.
mod scheduler_driver {
    use super::*;
    use crate::Scheduler;
    use crate::test_support::{FakeClock, gated_gateway};
    use chrono::{DateTime, Duration, Utc};
    use orchestrator_core::{RunId, RunStatus, SchedulerStore};
    use orchestrator_store::InMemorySchedulerStore;
    use std::sync::Arc;

    fn one_node_graph() -> Graph {
        Graph {
            nodes: vec![Node {
                id: NodeId("n1".into()),
                kind: model_call("c", "go"),
                deps: vec![],
            }],
        }
    }

    #[tokio::test]
    async fn a_paused_run_is_recorded_then_woken_by_a_tick_after_its_deadline() {
        let journal = InMemoryJournal::new();
        let store = Arc::new(InMemorySchedulerStore::new());
        let run = RunId(uuid::Uuid::new_v4());
        let graph = one_node_graph();
        let clock = FakeClock::new(DateTime::<Utc>::from_timestamp(1_000_000, 0).unwrap());

        // Submit with a GATED executor → the run pauses on the timed gate (resume_after journaled).
        let gw = gated_gateway().await;
        let gated_exec =
            Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1").with_clock(clock.clone());
        let sched_submit = Scheduler::new(
            store.clone(),
            gated_exec,
            Arc::new(journal.clone()),
            clock.clone(),
        );
        let o1 = sched_submit
            .submit(run, graph.clone())
            .await
            .expect("submit");
        assert!(o1.paused.is_some(), "the run pauses on the timed gate");
        let st = store.status(run).await.unwrap().unwrap();
        assert_eq!(st.status, RunStatus::Paused);
        let deadline = st.next_wake.expect("a timed pause has a next_wake");

        // A fresh, UN-GATED scheduler (a real quota reset) drives the wake half.
        let (gw2, calls2) = recording_gateway().await;
        let un_gated =
            Executor::new(Arc::new(gw2), Arc::new(journal.clone()), "v1").with_clock(clock.clone());
        let sched = Scheduler::new(
            store.clone(),
            un_gated,
            Arc::new(journal.clone()),
            clock.clone(),
        );

        // Before the deadline: nothing is due.
        clock.set(deadline - Duration::seconds(1));
        assert_eq!(sched.tick().await.unwrap(), 0, "not due yet");

        // Past the deadline: the tick wakes it → completed; a second tick no-ops.
        clock.set(deadline + Duration::seconds(1));
        assert_eq!(sched.tick().await.unwrap(), 1, "woken");
        assert_eq!(
            store.status(run).await.unwrap().unwrap().status,
            RunStatus::Completed
        );
        assert_eq!(
            calls2.lock().unwrap().len(),
            1,
            "only the gated node re-attempted on the wake"
        );
        assert_eq!(
            sched.tick().await.unwrap(),
            0,
            "a terminal run is not re-woken"
        );
    }

    #[tokio::test]
    async fn cancel_prevents_a_wake() {
        let journal = InMemoryJournal::new();
        let store = Arc::new(InMemorySchedulerStore::new());
        let run = RunId(uuid::Uuid::new_v4());
        let clock = FakeClock::new(DateTime::<Utc>::from_timestamp(1_000_000, 0).unwrap());
        // Seed a paused run directly (a store-level behavior — no gateway needed).
        store
            .enqueue(run, &one_node_graph(), clock.now())
            .await
            .unwrap();
        store
            .record_paused(run, Some(clock.now() + Duration::seconds(10)), "gated")
            .await
            .unwrap();
        let (gw, _c) = recording_gateway().await;
        let sched = Scheduler::new(
            store.clone(),
            Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1").with_clock(clock.clone()),
            Arc::new(journal.clone()),
            clock.clone(),
        );
        sched.cancel(run).await.unwrap();
        clock.set(clock.now() + Duration::seconds(100));
        assert_eq!(
            sched.tick().await.unwrap(),
            0,
            "a cancelled run is not woken"
        );
        assert_eq!(
            store.status(run).await.unwrap().unwrap().status,
            RunStatus::Cancelled
        );
    }

    /// A journal that is unloadable for ONE run and healthy for every other — the
    /// `format_version` fence a rolling deploy trips, which is exactly the shape
    /// `torii run list-paused` was fixed for in this same slice.
    struct OneFencedJournal {
        inner: Arc<InMemoryJournal>,
        fenced: RunId,
    }

    #[async_trait::async_trait]
    impl ExecutionJournal for OneFencedJournal {
        async fn append(
            &self,
            run: RunId,
            event: JournalEvent,
        ) -> Result<Seq, orchestrator_core::JournalError> {
            self.inner.append(run, event).await
        }
        async fn load(
            &self,
            run: RunId,
        ) -> Result<Vec<(Seq, JournalEvent)>, orchestrator_core::JournalError> {
            if run == self.fenced {
                return Err(orchestrator_core::JournalError::IncompatibleFormat {
                    run,
                    stored: 2,
                    expected: 1,
                });
            }
            self.inner.load(run).await
        }
        async fn load_since(
            &self,
            run: RunId,
            since: Seq,
        ) -> Result<Vec<(Seq, JournalEvent)>, orchestrator_core::JournalError> {
            if run == self.fenced {
                return Err(orchestrator_core::JournalError::IncompatibleFormat {
                    run,
                    stored: 2,
                    expected: 1,
                });
            }
            self.inner.load_since(run, since).await
        }
    }

    /// One unreadable journal must not take the whole claimed batch down with it.
    ///
    /// `tick`'s contract (and its doc): "A STORE failure aborts loudly; a drive's own
    /// failure is recorded (terminal), not propagated." A journal that will not load is a
    /// DRIVE failure — before this slice `Executor::start` hit the same fenced `load`,
    /// returned `Err`, and `record` filed the run terminal-`Failed`, so it left the due set
    /// and the batch carried on. The pre-drive watermark this slice added propagated
    /// instead, which turns one poisoned run into a fleet-wide stall: the run is never
    /// recorded terminal, `claim_due` leaves its `next_wake` in the past, its stale
    /// `waking` lease is reclaimed on every later tick, and `worker serve` exits after
    /// `MAX_CONSECUTIVE_FAILURES`.
    #[tokio::test]
    async fn one_unloadable_journal_does_not_abort_the_claimed_batch() {
        let inner = Arc::new(InMemoryJournal::new());
        let store = Arc::new(InMemorySchedulerStore::new());
        let healthy = RunId(uuid::Uuid::new_v4());
        let fenced = RunId(uuid::Uuid::new_v4());
        let clock = FakeClock::new(DateTime::<Utc>::from_timestamp(1_000_000, 0).unwrap());

        // Two paused runs, both due. Seeded at the store level: the fault under test is on
        // the journal read, so neither needs to have really been driven.
        for run in [fenced, healthy] {
            store
                .enqueue(run, &one_node_graph(), clock.now())
                .await
                .unwrap();
            store
                .record_paused(run, Some(clock.now() + Duration::seconds(10)), "gated")
                .await
                .unwrap();
        }

        let journal = Arc::new(OneFencedJournal {
            inner: inner.clone(),
            fenced,
        });
        let (gw, _c) = recording_gateway().await;
        let sched = Scheduler::new(
            store.clone(),
            Executor::new(Arc::new(gw), journal.clone(), "v1").with_clock(clock.clone()),
            journal.clone(),
            clock.clone(),
        );

        clock.set(clock.now() + Duration::seconds(100));
        let n = sched
            .tick()
            .await
            .expect("one unloadable journal must not abort the tick");
        assert_eq!(n, 2, "both due runs were claimed");

        // The poisoned run is classified, not left waking — so it leaves the due set.
        assert_eq!(
            store.status(fenced).await.unwrap().unwrap().status,
            RunStatus::Failed,
            "a run whose journal will not load is recorded terminal, exactly as a drive \
             failure is"
        );
        // The healthy run behind it in the batch was still driven.
        assert_eq!(
            store.status(healthy).await.unwrap().unwrap().status,
            RunStatus::Completed,
            "the run claimed after the poisoned one must still be driven"
        );
        // And the poison does not come back on the next tick.
        assert_eq!(
            sched.tick().await.unwrap(),
            0,
            "neither run is re-claimed once both are terminal"
        );
    }
}

// ============================== SP-6 s1 AwaitSignal ============================

/// The HITL primitive: a node that pauses until an external signal arrives, with an
/// optional deadline that FAILS it. Every test drives a graph of one `AwaitSignal` node
/// over a `FakeClock`, so the deadline arithmetic is exact and no test sleeps.
///
/// The four rows of the design's §6.2 three-way fold read, one test each:
///   signal folded                        → `completes_immediately_when_the_signal_is_already_folded`
///   no signal, no deadline recorded      → `pauses_and_records_its_deadline_when_no_signal_is_present`
///                                          (+ `without_a_timeout_pauses_with_no_deadline`)
///   deadline recorded, now >= deadline   → `fails_when_the_deadline_has_passed_with_no_signal`
///   deadline recorded, now <  deadline   → `repauses_with_the_same_deadline_when_woken_early`
mod await_signal {
    use super::*;
    use crate::test_support::FakeClock;
    use chrono::{DateTime, Duration, Utc};

    const HOUR: i64 = 3600;

    /// A fixed instant, so every deadline in these tests is an exact literal.
    fn at(unix_secs: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(unix_secs, 0).expect("valid timestamp")
    }

    fn gate() -> NodeId {
        NodeId("gate".into())
    }

    /// A one-node graph whose sole node is the `AwaitSignal` under test.
    fn await_graph(timeout: Option<Duration>) -> Graph {
        Graph {
            nodes: vec![Node {
                id: gate(),
                kind: NodeKind::AwaitSignal { timeout },
                deps: vec![],
            }],
        }
    }

    /// Every deadline this run has journaled, in order. THE assertion surface for the
    /// slice's trap: a correct implementation records exactly one, forever.
    fn awaited_deadlines(events: &[(Seq, JournalEvent)]) -> Vec<Option<DateTime<Utc>>> {
        events
            .iter()
            .filter_map(|(_, e)| match e {
                JournalEvent::SignalAwaited { node, deadline } if node == &gate() => {
                    Some(*deadline)
                }
                _ => None,
            })
            .collect()
    }

    /// Every `RunPaused.resume_after` this run has journaled, in order — what the
    /// durable scheduler re-arms on after each wake.
    fn paused_resume_afters(events: &[(Seq, JournalEvent)]) -> Vec<Option<DateTime<Utc>>> {
        events
            .iter()
            .filter_map(|(_, e)| match e {
                JournalEvent::RunPaused { resume_after, .. } => Some(*resume_after),
                _ => None,
            })
            .collect()
    }

    fn has<F: Fn(&JournalEvent) -> bool>(events: &[(Seq, JournalEvent)], f: F) -> bool {
        events.iter().any(|(_, e)| f(e))
    }

    /// Seed a journal as if a prior process had already written these events, so a
    /// `start` folds them exactly as a real resume would. `RunStarted.version` matches
    /// the executor's, or the fence would refuse the resume.
    async fn seed(journal: &InMemoryJournal, run: RunId, events: Vec<JournalEvent>) {
        journal
            .append(
                run,
                JournalEvent::RunStarted {
                    version: "v1".into(),
                    budget: None,
                },
            )
            .await
            .expect("seed RunStarted");
        for e in events {
            journal.append(run, e).await.expect("seed event");
        }
    }

    /// AC3 / §6.3 — the early-signal race resolves itself. A signal journaled BEFORE
    /// the node ever ran is simply already in the fold when it runs, so the node
    /// completes on the spot: no buffering, no ordering constraint, and — the point —
    /// it never waits, so no `SignalAwaited` is journaled at all.
    #[tokio::test]
    async fn completes_immediately_when_the_signal_is_already_folded() {
        let (gw, calls) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        seed(
            &journal,
            run,
            vec![JournalEvent::SignalReceived {
                node: gate(),
                payload: serde_json::json!({ "decision": "approved" }),
            }],
        )
        .await;

        let out = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
            .with_clock(FakeClock::new(at(1_000_000)))
            .start(run, &await_graph(Some(Duration::seconds(HOUR))))
            .await
            .expect("start");

        assert!(
            out.paused.is_none() && out.failed.is_none(),
            "an already-answered gate completes: paused={:?} failed={:?}",
            out.paused,
            out.failed
        );
        assert_eq!(
            out.outputs[&gate()]["decision"],
            "approved",
            "the signal payload IS the node's output"
        );
        let events = journal.load(run).await.unwrap();
        assert!(
            awaited_deadlines(&events).is_empty(),
            "a node whose answer is already folded never begins waiting: {:?}",
            events.iter().map(|(_, e)| label(e)).collect::<Vec<_>>()
        );
        assert!(has(&events, |e| matches!(e, JournalEvent::RunCompleted)));
        assert!(
            calls.lock().unwrap().is_empty(),
            "an AwaitSignal node spends no tokens"
        );
    }

    /// §6.2 row 2 — the first execution fixes the deadline: `SignalAwaited` carries the
    /// ABSOLUTE instant, and the pause re-arms the durable scheduler on that same
    /// instant via `RunPaused.resume_after` (without which the timeout would never be
    /// auto-woken and the whole deadline branch would be decorative).
    #[tokio::test]
    async fn pauses_and_records_its_deadline_when_no_signal_is_present() {
        let (gw, _c) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let t0 = at(1_000_000);
        let deadline = t0 + Duration::seconds(HOUR);

        let out = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
            .with_clock(FakeClock::new(t0))
            .run(run, &await_graph(Some(Duration::seconds(HOUR))))
            .await
            .expect("run");

        let pause = out.paused.as_ref().expect("an unsignalled gate pauses");
        assert_eq!(pause.node, gate());
        assert!(out.failed.is_none(), "a wait is not a failure: {out:?}");

        let events = journal.load(run).await.unwrap();
        assert_eq!(
            awaited_deadlines(&events),
            vec![Some(deadline)],
            "the ABSOLUTE deadline `now + timeout` is journaled once"
        );
        assert_eq!(
            paused_resume_afters(&events),
            vec![Some(deadline)],
            "the pause carries the deadline so the scheduler wakes it at that instant"
        );
        assert!(
            !has(&events, |e| matches!(e, JournalEvent::RunCompleted)),
            "a waiting run does not complete"
        );
        assert!(
            !has(&events, |e| matches!(e, JournalEvent::NodeFailed { .. })),
            "waiting is not failing"
        );
    }

    /// §6.2 row 2, the `None` half — the indefinite HITL gate (the common shape: wait
    /// for a human, however long). `resume_after: None` is SP-DATA-3's never-auto-woken
    /// class, so only a `torii run force-wake` (or a signal) moves it — and it must
    /// NEVER expire, however many times it is re-driven. `SignalAwaited` is still
    /// journaled with `deadline: None`, because it is the node-keyed record that tells
    /// an operator WHICH node is awaiting.
    #[tokio::test]
    async fn without_a_timeout_pauses_with_no_deadline_and_never_expires() {
        let (gw, _c) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let clock = FakeClock::new(at(1_000_000));
        let graph = await_graph(None);
        let exec =
            Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1").with_clock(clock.clone());

        let out = exec.run(run, &graph).await.expect("run");
        assert_eq!(out.paused.expect("pauses").node, gate());
        let events = journal.load(run).await.unwrap();
        assert_eq!(
            awaited_deadlines(&events),
            vec![None],
            "the awaiting node is recorded, with no deadline"
        );
        assert_eq!(
            paused_resume_afters(&events),
            vec![None],
            "no deadline ⇒ the never-auto-woken (HOTL) pause class"
        );

        // A century later it is still waiting, not expired: `None` means no deadline,
        // and no deadline can ever have passed.
        clock.set(at(1_000_000 + 100 * 365 * 24 * HOUR));
        let out2 = exec.start(run, &graph).await.expect("resume");
        assert!(
            out2.paused.is_some() && out2.failed.is_none(),
            "a deadline-less gate never times out: {out2:?}"
        );
        let events = journal.load(run).await.unwrap();
        assert!(
            !has(&events, |e| matches!(e, JournalEvent::NodeFailed { .. })),
            "no deadline ⇒ no timeout failure, ever"
        );
    }

    /// **I1 (whole-slice review).** A deadline-less gate records `SignalAwaited`
    /// EXACTLY ONCE, however many times the run is re-driven — and the re-drives are
    /// NOT human-bounded.
    ///
    /// The first implementation re-recorded the event on every drive, justified by "with
    /// no deadline the run is in the never-auto-woken class, so a re-drive only ever
    /// follows a human `force_wake`". This test is the disproof of that premise, with no
    /// human anywhere in it: `drive` runs EVERY ready node in a round even after one
    /// pauses, so the gate's `RunPaused { resume_after: None }` is followed in the SAME
    /// drive by a dep-free sibling's `RunPaused { resume_after: Some(t) }` — and
    /// `Scheduler::record` takes `next_wake` from the LAST `RunPaused`. The run therefore
    /// keeps a non-NULL `next_wake` and is auto-woken at the provider's re-eligibility
    /// cadence for the whole human-approval window.
    #[tokio::test]
    async fn a_deadline_less_gate_records_itself_once_across_automatic_wakes() {
        use crate::Scheduler;
        use crate::test_support::gated_gateway;
        use orchestrator_core::SchedulerStore;
        use orchestrator_store::InMemorySchedulerStore;

        let journal = InMemoryJournal::new();
        let store = Arc::new(InMemorySchedulerStore::new());
        let run = RunId(uuid::Uuid::new_v4());
        let clock = FakeClock::new(at(1_000_000));
        // The gate carries NO deadline; the sibling `ModelCall` hits a gated provider
        // and pauses WITH one. Both are dep-free, so both execute in the same round.
        let graph = Graph {
            nodes: vec![
                Node {
                    id: gate(),
                    kind: NodeKind::AwaitSignal { timeout: None },
                    deps: vec![],
                },
                Node {
                    id: NodeId("n1".into()),
                    kind: model_call("c", "go"),
                    deps: vec![],
                },
            ],
        };
        let exec = Executor::new(
            Arc::new(gated_gateway().await),
            Arc::new(journal.clone()),
            "v1",
        )
        .with_clock(clock.clone());
        let sched = Scheduler::new(
            store.clone(),
            exec,
            Arc::new(journal.clone()),
            clock.clone(),
        );

        sched.submit(run, graph.clone()).await.expect("submit");
        assert_eq!(
            awaited_deadlines(&journal.load(run).await.unwrap()),
            vec![None],
            "the gate records itself once on the first drive"
        );

        for wake in 1..=5 {
            let next = store
                .status(run)
                .await
                .unwrap()
                .expect("the run is scheduled")
                .next_wake
                .expect(
                    "the sibling's timed pause keeps the run AUTO-wakeable — \
                     no human is required to re-drive a deadline-less gate",
                );
            clock.set(next + Duration::seconds(1));
            assert_eq!(
                sched.tick().await.unwrap(),
                1,
                "wake {wake} fires automatically, with no operator involved"
            );
            assert_eq!(
                awaited_deadlines(&journal.load(run).await.unwrap()),
                vec![None],
                "wake {wake}: the awaiting node is recorded ONCE, not once per drive"
            );
        }
    }

    /// A two-gate graph in the caller's declaration order: one gate with `timeout`, one
    /// with none, both dep-free so `drive` runs BOTH in the same round and journals two
    /// `RunPaused` events for one drive.
    fn two_gate_graph(timed_first: bool, timeout: Duration) -> Graph {
        let timed = Node {
            id: NodeId("timed".into()),
            kind: NodeKind::AwaitSignal {
                timeout: Some(timeout),
            },
            deps: vec![],
        };
        let indef = Node {
            id: NodeId("indef".into()),
            kind: NodeKind::AwaitSignal { timeout: None },
            deps: vec![],
        };
        Graph {
            nodes: if timed_first {
                vec![timed, indef]
            } else {
                vec![indef, timed]
            },
        }
    }

    /// **Whole-slice review, Important — reproduced independently by two reviewers.**
    /// A deadline-LESS gate must not erase a timed gate's wake.
    ///
    /// "Legal sign-off, 48h SLA" beside "customer confirms, whenever" is a first-class
    /// HITL shape, and it is exactly the case where the timeout exists to bound an
    /// otherwise unbounded wait. Both gates are dep-free, so ONE drive journals
    /// `RunPaused{Some(deadline)}` and `RunPaused{None}` — and `Scheduler::record` used
    /// to take `next_wake` from the LAST of them and `flatten()` it away, leaving a NULL
    /// `next_wake` that no `tick()` will ever claim. The 48h deadline then fires only if
    /// a human answers the OTHER gate, i.e. only when it was never needed.
    ///
    /// Both declaration orders are asserted because the defect is *silently* ordered:
    /// swap the two lines and the timeout works again. A test of the benign order alone
    /// (which is what shipped) proves nothing about the harmful one.
    #[tokio::test]
    async fn a_deadline_less_gate_cannot_erase_a_timed_gates_wake() {
        use crate::Scheduler;
        use orchestrator_core::{RunStatus, SchedulerStore};
        use orchestrator_store::InMemorySchedulerStore;

        for timed_first in [true, false] {
            let order = if timed_first {
                "timed declared first"
            } else {
                "deadline-less declared first"
            };
            let (gw, _c) = recording_gateway().await;
            let journal = InMemoryJournal::new();
            let store = Arc::new(InMemorySchedulerStore::new());
            let run = RunId(uuid::Uuid::new_v4());
            let t0 = at(1_000_000);
            let deadline = t0 + Duration::seconds(HOUR);
            let clock = FakeClock::new(t0);
            let graph = two_gate_graph(timed_first, Duration::seconds(HOUR));
            let sched = Scheduler::new(
                store.clone(),
                Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
                    .with_clock(clock.clone()),
                Arc::new(journal.clone()),
                clock.clone(),
            );

            let out = sched.submit(run, graph.clone()).await.expect("submit");
            assert!(out.paused.is_some(), "{order}: both gates wait");
            let row = store
                .status(run)
                .await
                .unwrap()
                .expect("the run is scheduled");
            assert_eq!(
                row.status,
                RunStatus::Paused,
                "{order}: a waiting run is paused"
            );
            assert_eq!(
                row.next_wake,
                Some(deadline),
                "{order}: the run must wake at the EARLIEST deadline this drive recorded, \
                 not at whatever the last pause happened to carry"
            );

            // The payoff: a tick past the deadline actually claims the run, and the
            // timed gate expires. A NULL `next_wake` claims nothing, forever.
            clock.set(deadline + Duration::seconds(27 * HOUR));
            assert_eq!(
                sched.tick().await.unwrap(),
                1,
                "{order}: the deadline is honoured by an automatic wake"
            );
            let events = journal.load(run).await.unwrap();
            assert!(
                has(&events, |e| matches!(
                    e,
                    JournalEvent::NodeFailed { node, .. } if node == &NodeId("timed".into())
                )),
                "{order}: the woken drive expires the timed gate loudly: {:?}",
                events.iter().map(|(_, e)| label(e)).collect::<Vec<_>>()
            );
        }
    }

    /// The other half of the same rule: the earliest deadline is taken from **this
    /// drive's** pauses, never from the whole journal.
    ///
    /// Drive 1 records `Some(t0+1h)` (the timed gate) and `None` (the indefinite one).
    /// The wake at `t0+1h` expires the timed gate, so drive 2's ONLY pause is the
    /// deadline-less one — the run is now genuinely in SP-DATA-3's never-auto-woken
    /// (HOTL) class and `next_wake` must go back to NULL. Scanning the whole journal
    /// would re-adopt drive 1's now-PAST deadline, and a `next_wake` in the past is
    /// claimed by every tick: a hot loop that re-drives the run forever.
    #[tokio::test]
    async fn a_previous_drives_deadline_is_never_resurrected_as_a_next_wake() {
        use crate::Scheduler;
        use orchestrator_core::{RunStatus, SchedulerStore};
        use orchestrator_store::InMemorySchedulerStore;

        let (gw, _c) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let store = Arc::new(InMemorySchedulerStore::new());
        let run = RunId(uuid::Uuid::new_v4());
        let t0 = at(1_000_000);
        let deadline = t0 + Duration::seconds(HOUR);
        let clock = FakeClock::new(t0);
        let graph = two_gate_graph(true, Duration::seconds(HOUR));
        let sched = Scheduler::new(
            store.clone(),
            Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1").with_clock(clock.clone()),
            Arc::new(journal.clone()),
            clock.clone(),
        );

        sched.submit(run, graph.clone()).await.expect("submit");
        assert_eq!(
            store.status(run).await.unwrap().unwrap().next_wake,
            Some(deadline)
        );

        clock.set(deadline + Duration::seconds(1));
        assert_eq!(sched.tick().await.unwrap(), 1, "the deadline wakes the run");

        let row = store.status(run).await.unwrap().unwrap();
        assert_eq!(
            row.status,
            RunStatus::Paused,
            "the surviving deadline-less gate keeps the run resumable"
        );
        assert_eq!(
            row.next_wake, None,
            "with the timed gate expired, this drive paused only WITHOUT a deadline — \
             a stale past deadline here is claimed by every tick (a hot loop)"
        );
        for tick in 1..=3 {
            assert_eq!(
                sched.tick().await.unwrap(),
                0,
                "tick {tick}: a deadline-less pause is never auto-woken"
            );
        }
    }

    /// AC4 / §6.2 row 3 — the deadline fires LOUDLY. Reaching it with no signal fails
    /// the node (naming it and the deadline) and the run does NOT complete. Never a
    /// silent self-approval: there is deliberately no default-payload-on-timeout (§4).
    #[tokio::test]
    async fn fails_when_the_deadline_has_passed_with_no_signal() {
        let (gw, _c) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let deadline = at(1_000_000 + HOUR);
        seed(
            &journal,
            run,
            vec![JournalEvent::SignalAwaited {
                node: gate(),
                deadline: Some(deadline),
            }],
        )
        .await;

        let out = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
            // Exactly ON the deadline: the check is `now >= deadline`.
            .with_clock(FakeClock::new(deadline))
            .start(run, &await_graph(Some(Duration::seconds(HOUR))))
            .await
            .expect("start");

        let (node, message) = out.failed.expect("the deadline fires");
        assert_eq!(node, gate());
        assert!(
            message.contains("gate") && message.contains(&deadline.to_string()),
            "the failure names the node and the deadline: {message}"
        );
        assert!(
            out.paused.is_none(),
            "an expired gate fails, it does not keep waiting"
        );

        let events = journal.load(run).await.unwrap();
        assert!(
            has(
                &events,
                |e| matches!(e, JournalEvent::NodeFailed { node, .. } if node == &gate())
            ),
            "the timeout is journaled loudly as NodeFailed"
        );
        assert!(
            !has(&events, |e| matches!(e, JournalEvent::RunCompleted)),
            "a timed-out run does not complete"
        );
        assert_eq!(
            awaited_deadlines(&events),
            vec![Some(deadline)],
            "an expiring node does not re-record a deadline"
        );
    }

    /// How many `NodeFailed` events this run has journaled for the gate.
    fn gate_failures(events: &[(Seq, JournalEvent)]) -> usize {
        events
            .iter()
            .filter(|(_, e)| matches!(e, JournalEvent::NodeFailed { node, .. } if node == &gate()))
            .count()
    }

    /// **Whole-slice review, Important — the serious half.** A signal that arrives AFTER
    /// the deadline must never resurrect an expired gate as *approved*.
    ///
    /// `fold_journal` had no `NodeFailed` arm, so an expired gate was not terminal on
    /// resume: it re-ran, and by then `fold.signals` held the late answer, so it took the
    /// first arm of §6.2 and completed. The run that had terminally failed on its
    /// deadline then reached `RunCompleted` carrying `{"decision":"approved"}` — the
    /// silent self-approval §4 explicitly rejects ("a gate that silently self-approves is
    /// exactly the footgun this codebase's fail-closed stance argues against").
    ///
    /// It is reachable: `torii run signal` pre-checks the gate's state and then appends,
    /// and nothing makes those two steps atomic. The CLI now reports the outcome honestly
    /// (`not read`), but it cannot stop the row existing — so the executor must be the
    /// guard, not the reporter.
    ///
    /// The failure wins whatever the relative order of the two events. A signal appended
    /// just BEFORE the expiry (an operator answering while the drive was mid-flight, off a
    /// snapshot that predates it) is still an answer that arrived after the deadline
    /// passed, and the deadline is the contract. Fail-closed is the only reading that does
    /// not turn a missed SLA into an approval.
    #[tokio::test]
    async fn a_late_signal_never_resurrects_an_expired_gate() {
        let (gw, _c) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let deadline = at(1_000_000 + HOUR);
        let graph = await_graph(Some(Duration::seconds(HOUR)));
        seed(
            &journal,
            run,
            vec![JournalEvent::SignalAwaited {
                node: gate(),
                deadline: Some(deadline),
            }],
        )
        .await;
        let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
            .with_clock(FakeClock::new(deadline));

        assert_eq!(
            exec.start(run, &graph).await.expect("start").failed,
            Some((
                gate(),
                format!("await_signal: no signal for node gate by {deadline}")
            )),
            "the gate expires at its deadline"
        );

        // The late answer — exactly the row `torii run signal` can append into the race.
        journal
            .append(
                run,
                JournalEvent::SignalReceived {
                    node: gate(),
                    payload: serde_json::json!({ "decision": "approved" }),
                },
            )
            .await
            .unwrap();

        let out = exec.start(run, &graph).await.expect("resume");
        let (node, _) = out
            .failed
            .expect("an expired gate stays expired — a late signal is not an approval");
        assert_eq!(node, gate());
        assert!(
            !out.outputs.contains_key(&gate()),
            "the late payload must not become the gate's output: {:?}",
            out.outputs
        );
        let events = journal.load(run).await.unwrap();
        assert!(
            !has(&events, |e| matches!(e, JournalEvent::RunCompleted)),
            "a run that failed on its deadline must never reach RunCompleted: {:?}",
            events.iter().map(|(_, e)| label(e)).collect::<Vec<_>>()
        );
        assert_eq!(
            gate_failures(&events),
            1,
            "the expiry is journaled once — the resume READS it, it does not re-fail"
        );
    }

    /// The other half: an expired gate does not re-fail on every drive.
    ///
    /// A run whose gate has expired stays resumable while any OTHER node is still paused
    /// (the run is not terminal — `record` files it `paused`, correctly, so the surviving
    /// gate can still be answered), so it is re-driven on every wake. Without a folded
    /// `NodeFailed` each of those wakes appended another `NodeFailed` for the same
    /// already-dead node — a terminal event recorded over and over.
    ///
    /// (The cascade-skip of `after` still journals its `NodeSkipped` per drive, as it does
    /// for any repeatedly-failing node of any kind. That is untouched here deliberately:
    /// it is general cascade behaviour, not an `AwaitSignal` defect, and it changes no
    /// growth class — a re-driven paused run appends its `RunPaused` every wake anyway.)
    #[tokio::test]
    async fn an_expired_gate_journals_its_failure_once_however_often_it_is_re_driven() {
        let (gw, _c) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let deadline = at(1_000_000 + HOUR);
        let graph = Graph {
            nodes: vec![
                Node {
                    id: gate(),
                    kind: NodeKind::AwaitSignal {
                        timeout: Some(Duration::seconds(HOUR)),
                    },
                    deps: vec![],
                },
                Node {
                    id: NodeId("after".into()),
                    kind: model_call("c", "after"),
                    deps: vec![Dep::hard("gate")],
                },
            ],
        };
        seed(
            &journal,
            run,
            vec![JournalEvent::SignalAwaited {
                node: gate(),
                deadline: Some(deadline),
            }],
        )
        .await;
        let clock = FakeClock::new(deadline);
        let exec =
            Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1").with_clock(clock.clone());

        for wake in 1..=5 {
            clock.set(deadline + Duration::seconds(wake * HOUR));
            let out = exec.start(run, &graph).await.expect("re-drive");
            assert_eq!(
                out.failed.map(|(n, _)| n),
                Some(gate()),
                "wake {wake}: the gate is still the run's failure"
            );
            assert_eq!(
                gate_failures(&journal.load(run).await.unwrap()),
                1,
                "wake {wake}: the expiry is recorded ONCE, not once per drive"
            );
        }
    }

    /// **Whole-slice review, Minor — the reviewer's exact collision graph, refused.**
    ///
    /// `Subgraph("sg"){gate}` namespaces its inner node to `"sg/gate"`; declare a
    /// TOP-LEVEL node literally named `sg/gate` beside it and the two share one fold key.
    /// The graph passed `validate_dag`, and one `SignalReceived{node:"sg/gate"}` completed
    /// BOTH gates — a human decision intended for one approval silently answering another.
    ///
    /// The fix is in the validator (`/` is the executor's path separator and is not the
    /// author's to use), which is why the assertion here is that the run never starts. It
    /// is asserted at THIS level as well as in `orchestrator-core` because the collision is
    /// only observable through the executor's namespacing, and a validator rule with no
    /// executor-level witness is the kind that gets "simplified" away later.
    #[tokio::test]
    async fn a_top_level_id_can_never_alias_a_subgraphs_namespaced_gate() {
        let (gw, _c) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let colliding = NodeId("sg/gate".into());
        let graph = Graph {
            nodes: vec![
                Node {
                    id: NodeId("sg".into()),
                    kind: NodeKind::Subgraph {
                        graph: Box::new(await_graph(None)),
                    },
                    deps: vec![],
                },
                Node {
                    id: colliding.clone(),
                    kind: NodeKind::AwaitSignal { timeout: None },
                    deps: vec![],
                },
            ],
        };
        // The one signal that used to answer both gates at once.
        seed(
            &journal,
            run,
            vec![JournalEvent::SignalReceived {
                node: colliding.clone(),
                payload: serde_json::json!({ "who": "one" }),
            }],
        )
        .await;

        let refused = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
            .with_clock(FakeClock::new(at(1_000_000)))
            .start(run, &graph)
            .await;
        match refused {
            Err(OrchestratorError::InvalidGraph(m)) => assert!(
                m.contains("sg/gate"),
                "the refusal names the aliasing id: {m}"
            ),
            other => panic!(
                "a top-level id that aliases a namespaced one must be refused before it \
                 runs; instead it ran and one signal answered these nodes: {:?}",
                other.map(|o| o.outputs)
            ),
        }
    }

    /// **AC1 — the slice's most important test.** The deadline is journaled ONCE and
    /// READ thereafter, never recomputed.
    ///
    /// An operator force-wakes the awaiting run three times across the hour (the
    /// SP-DATA-4 HOTL path). Each wake must re-pause on the SAME absolute instant. The
    /// obvious `now + timeout` implementation pushes the deadline forward on every one
    /// of them, so a run woken every ten minutes with a one-hour timeout would NEVER
    /// expire — which is why this test ends by advancing to the ORIGINAL deadline and
    /// demanding the node expire there. Under the recompute bug the deadline would by
    /// then sit at t0+70min and the final drive would still be pausing.
    #[tokio::test]
    async fn repauses_with_the_same_deadline_when_woken_early() {
        let (gw, _c) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let t0 = at(1_000_000);
        let deadline = t0 + Duration::seconds(HOUR);
        let clock = FakeClock::new(t0);
        let graph = await_graph(Some(Duration::seconds(HOUR)));
        let exec =
            Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1").with_clock(clock.clone());

        assert!(
            exec.run(run, &graph).await.expect("run").paused.is_some(),
            "the gate starts waiting"
        );

        for wake in 1..=3 {
            clock.set(t0 + Duration::minutes(10 * wake));
            let out = exec.start(run, &graph).await.expect("force-wake");
            assert!(
                out.paused.is_some() && out.failed.is_none(),
                "wake {wake} re-pauses rather than completing or failing: {out:?}"
            );
            let events = journal.load(run).await.unwrap();
            assert_eq!(
                awaited_deadlines(&events),
                vec![Some(deadline)],
                "wake {wake}: exactly ONE deadline was ever recorded, and it has not moved"
            );
            assert!(
                paused_resume_afters(&events)
                    .iter()
                    .all(|r| *r == Some(deadline)),
                "wake {wake}: every re-pause re-arms the scheduler on the ORIGINAL instant: {:?}",
                paused_resume_afters(&events)
            );
        }

        // The payoff. Three wakes later, the ORIGINAL deadline still governs.
        clock.set(deadline);
        let expired = exec
            .start(run, &graph)
            .await
            .expect("resume at the deadline");
        let (node, message) = expired
            .failed
            .expect("the ORIGINAL deadline fires despite three intervening wakes");
        assert_eq!(node, gate());
        assert!(
            message.contains(&deadline.to_string()),
            "it expires at the instant first recorded, not a rolled-forward one: {message}"
        );
    }

    /// AC2 — the answer is folded, so the node never re-asks. A gate that has waited,
    /// then been signalled, completes on the next drive; the `SignalAwaited` count does
    /// not grow, and driving the (now terminal) run again neither re-drives it nor
    /// appends a second `RunCompleted`.
    #[tokio::test]
    async fn a_signalled_gate_completes_on_resume_and_never_re_asks() {
        let (gw, _c) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let t0 = at(1_000_000);
        let clock = FakeClock::new(t0);
        let graph = await_graph(Some(Duration::seconds(HOUR)));
        let exec =
            Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1").with_clock(clock.clone());

        exec.run(run, &graph)
            .await
            .expect("run")
            .paused
            .expect("waits");

        // The operator answers (what `torii run signal` will append in Task 4), then a
        // worker tick re-drives the run.
        journal
            .append(
                run,
                JournalEvent::SignalReceived {
                    node: gate(),
                    payload: serde_json::json!({ "decision": "approved" }),
                },
            )
            .await
            .unwrap();
        clock.set(t0 + Duration::minutes(5));
        let out = exec.start(run, &graph).await.expect("resume");
        assert!(
            out.failed.is_none() && out.paused.is_none(),
            "the signalled gate completes: {out:?}"
        );
        assert_eq!(out.outputs[&gate()]["decision"], "approved");

        let events = journal.load(run).await.unwrap();
        assert_eq!(
            awaited_deadlines(&events).len(),
            1,
            "the answered gate does not begin waiting a second time"
        );
        let completions = events
            .iter()
            .filter(|(_, e)| matches!(e, JournalEvent::RunCompleted))
            .count();
        assert_eq!(completions, 1);

        // A terminal run re-driven is a fold, not an execution — nothing is appended.
        let before = events.len();
        exec.start(run, &graph).await.expect("terminal resume");
        assert_eq!(
            journal.load(run).await.unwrap().len(),
            before,
            "re-driving a completed run journals nothing further"
        );
    }

    /// AC6 / §6.4 — a signal payload is not a credential channel, and unlike a pause
    /// reason it does not merely get *displayed*: it becomes the node's output and flows
    /// into downstream nodes and model prompts. So the s2 `Redactor` is applied ONCE and
    /// that single value is BOTH what the node returns AND what is written durably (the
    /// blackboard blob the journaled `ContextWrite` addresses).
    ///
    /// The seeded `SignalReceived` here holds the plaintext — i.e. a producer that did
    /// NOT redact — precisely so this proves the executor is an independent guard rather
    /// than inheriting a scrub someone else performed.
    #[tokio::test]
    async fn payload_is_redacted_before_both_the_return_and_the_durable_write() {
        use orchestrator_store::{InMemoryContentStore, InMemoryContextStore};
        // Assembled at RUNTIME: the repo's semgrep CWE-798 hook blocks credential-shaped
        // literals in source. The redactor still matches the built string.
        let secret = format!("sk-{}", "abcdefghijklmnopqrstuvwx");
        let content = Arc::new(InMemoryContentStore::new());
        let ctx = Arc::new(InMemoryContextStore::new(content.clone()));
        let (gw, _c) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        seed(
            &journal,
            run,
            vec![JournalEvent::SignalReceived {
                node: gate(),
                payload: serde_json::json!({ "decision": "approved", "token": secret }),
            }],
        )
        .await;

        let out = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
            .with_clock(FakeClock::new(at(1_000_000)))
            .with_content_store(content.clone())
            .with_context_store(ctx.clone())
            .with_redactor(Arc::new(orchestrator_core::PatternRedactor::default()))
            .start(run, &await_graph(None))
            .await
            .expect("start");

        // (a) THE RETURN — what downstream nodes and model prompts will see.
        let output = &out.outputs[&gate()];
        assert_eq!(
            output["token"],
            serde_json::json!("[REDACTED]"),
            "the node's OUTPUT is scrubbed: {output}"
        );
        assert_eq!(
            output["decision"], "approved",
            "a legitimate decision is untouched — the redactor matches credential SHAPES"
        );
        assert!(
            !serde_json::to_string(output).unwrap().contains(&secret),
            "no plaintext survives into the node output"
        );

        // (b) THE DURABLE WRITE — the same single redacted value, not a second scrub.
        let events = journal.load(run).await.unwrap();
        assert!(
            has(
                &events,
                |e| matches!(e, JournalEvent::ContextWrite { key, .. } if key.0 == "gate")
            ),
            "the completed gate published to the blackboard"
        );
        let r = ctx
            .get(orchestrator_core::Scope::Run, ContextKey("gate".into()))
            .await
            .unwrap()
            .expect("the gate's output is on the blackboard");
        assert_eq!(
            ctx.load(&r).await.unwrap()["token"],
            serde_json::json!("[REDACTED]"),
            "the durably-stored value is scrubbed too"
        );
        let blob = content.get(&r.content.digest).await.unwrap();
        assert!(
            !String::from_utf8_lossy(&blob).contains(&secret),
            "no plaintext reaches the content store"
        );
    }

    /// **Whole-slice review, Critical — layer 2 of the overflow guard.** A timeout so
    /// large that `now + timeout` leaves the `DateTime<Utc>` range must FAIL the node,
    /// never panic.
    ///
    /// `validate_dag` now refuses such a graph (layer 1), and this test deliberately
    /// walks around it: `run_await_signal` is called directly, because
    /// [`Executor::start`] takes the graph as a caller-supplied parameter and NOTHING
    /// guarantees anyone validated it — the executor is a public API, `start` is the
    /// scheduler's resume entry point, and the graph a wake re-drives comes back out of
    /// the store. A node kind that can panic is unacceptable at any distance from a
    /// validator, because the panic is not local: it unwinds through `Scheduler::tick`
    /// (which has already claimed the batch) and out of `worker::serve`'s in-task
    /// `ticker.tick()`, taking the worker process with it.
    ///
    /// The failure must also arrive BEFORE anything is journaled: a `SignalAwaited`
    /// written with a nonsense deadline would be folded first-wins forever.
    #[tokio::test]
    async fn an_unaddable_timeout_fails_the_node_instead_of_panicking() {
        let (gw, _c) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        seed(&journal, run, vec![]).await;
        let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1")
            .with_clock(FakeClock::new(at(1_000_000)));

        let node = Node {
            id: gate(),
            kind: NodeKind::AwaitSignal {
                timeout: Some(Duration::MAX),
            },
            deps: vec![],
        };
        let exec_result = exec
            .run_await_signal(run, &node, Some(Duration::MAX), &Fold::default())
            .await
            .expect("an unaddable timeout is a node failure, not an executor error");

        let message = match exec_result {
            NodeExec::Failed { message, .. } => message,
            NodeExec::Completed(v) => panic!("expected a loud NodeFailed, got Completed({v})"),
            NodeExec::Paused { reason } => {
                panic!("expected a loud NodeFailed, got Paused({reason})")
            }
        };
        assert!(
            message.contains("gate"),
            "the failure names the offending node: {message}"
        );

        let events = journal.load(run).await.unwrap();
        assert!(
            has(
                &events,
                |e| matches!(e, JournalEvent::NodeFailed { node, .. } if node == &gate())
            ),
            "the failure is journaled loudly: {:?}",
            events.iter().map(|(_, e)| label(e)).collect::<Vec<_>>()
        );
        assert!(
            awaited_deadlines(&events).is_empty(),
            "no nonsense deadline is recorded — the fold is first-wins, so a bad one is forever"
        );
        assert!(
            paused_resume_afters(&events).is_empty(),
            "a node that cannot compute a deadline does not pause"
        );
    }

    /// The layer-1 payoff, on the exact path the reviewer drove: `Scheduler::submit`
    /// enqueues the store row BEFORE the drive, so a panicking drive used to leave a
    /// durable `(Waking, next_wake: None)` row that every later `tick()` reclaimed and
    /// re-panicked on. Now the drive returns `InvalidGraph`, `record` files the run
    /// terminal-`Failed`, and no tick ever picks it up again.
    #[tokio::test]
    async fn submitting_an_unaddable_timeout_leaves_no_poison_row_in_the_scheduler() {
        use crate::Scheduler;
        use orchestrator_core::{RunStatus, SchedulerStore};
        use orchestrator_store::InMemorySchedulerStore;

        let (gw, _c) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let store = Arc::new(InMemorySchedulerStore::new());
        let run = RunId(uuid::Uuid::new_v4());
        let clock = FakeClock::new(at(1_000_000));
        // The reviewer's exact input, as `torii run submit <graph.json>` would parse it.
        let graph: Graph = serde_json::from_str(
            r#"{"nodes":[{"id":"gate","kind":{"AwaitSignal":{"timeout":[9223372036854775,807000000]}},"deps":[]}]}"#,
        )
        .expect("the reviewer's input parses");
        let sched = Scheduler::new(
            store.clone(),
            Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1").with_clock(clock.clone()),
            Arc::new(journal.clone()),
            clock.clone(),
        );

        let err = sched
            .submit(run, graph)
            .await
            .expect_err("a graph the validator refuses cannot drive");
        assert!(
            matches!(err, OrchestratorError::InvalidGraph(_)),
            "refused by the validator, not by a panic: {err:?}"
        );

        let row = store
            .status(run)
            .await
            .unwrap()
            .expect("submit enqueued a row before driving");
        assert_eq!(
            row.status,
            RunStatus::Failed,
            "the refused run is filed TERMINAL, not left mid-`Waking` for the next tick to reclaim"
        );

        // The poison pill's signature: an ancient `waking` lease that every tick reclaims.
        clock.set(at(1_000_000 + 365 * 24 * 3600));
        for tick in 1..=3 {
            sched.tick().await.expect("a tick must never panic");
            assert_eq!(
                store.status(run).await.unwrap().map(|r| r.status),
                Some(RunStatus::Failed),
                "tick {tick} does not resurrect a terminally-refused run"
            );
        }
    }
}

// ======================= SP-6 s2 shared waiting machinery (Task 3) ======================

/// Direct unit tests on [`Executor::wait_or_expire`], the shared helper Task 3 extracted
/// from `run_await_signal` for `HumanGate` to reuse.
///
/// **Why these exist, and why they are NOT in the `await_signal` module.** Task 3's review
/// mutation-tested the extraction and found the 15 s1 tests guard `gate_precheck` but NOT
/// the expiry decision. The reason is structural, not an oversight in s1: `run_await_signal`
/// decides expiry in TWO places — `WaitState::Expired`, and the retained post-match check
/// that catches a freshly computed deadline which has already passed — and the two emit
/// byte-identical events and returns. They therefore MASK each other, and no black-box test
/// driven through `run_await_signal` can distinguish them:
///
/// | mutation | s1 suite |
/// |---|---|
/// | `wait_or_expire` never returns `Expired` | GREEN, 15 passed |
/// | the post-match check deleted, `Expired` kept | GREEN, 15 passed |
/// | both disabled | RED, 6 failures |
///
/// Only the aggregate was guarded. That is tolerable while one function owns both sites,
/// and NOT tolerable now: Task 5's `run_human_gate` consumes `WaitState::Expired` and has
/// no duplicate post-match check to mask a bug in it, so a broken `Expired` arm would ship
/// with every s1 test green. These tests call the helper directly — the only way to observe
/// one site independently of the other — and the module is named so that it does not join
/// the `await_signal` filter, whose count is a deliberate gate.
mod waiting_node_helpers {
    use super::signal::WaitState;
    use super::*;
    use crate::test_support::FakeClock;
    use chrono::{DateTime, Duration, Utc};

    fn at(unix_secs: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(unix_secs, 0).expect("valid timestamp")
    }

    fn gate() -> NodeId {
        NodeId("gate".into())
    }

    fn gate_node() -> Node {
        Node {
            id: gate(),
            kind: NodeKind::AwaitSignal { timeout: None },
            deps: vec![],
        }
    }

    /// An executor whose only relevant wiring is the clock — `wait_or_expire` reads nothing
    /// else off `self`.
    async fn exec_at(now: DateTime<Utc>) -> Executor {
        let (gw, _c) = recording_gateway().await;
        Executor::new(Arc::new(gw), Arc::new(InMemoryJournal::new()), "v1")
            .with_clock(FakeClock::new(now))
    }

    /// A fold in which the gate has already begun asking, with the given deadline —
    /// built through the real `fold_journal` rather than by poking private fields, so
    /// these tests break if the folding contract changes under them.
    fn folded_await(deadline: Option<DateTime<Utc>>) -> Fold {
        let (fold, _, _) = fold_journal(&[(
            0,
            JournalEvent::SignalAwaited {
                node: gate(),
                deadline,
            },
        )]);
        fold
    }

    /// The arm with no independent guard before this test, and the one Task 5 depends on.
    #[tokio::test]
    async fn a_recorded_deadline_that_has_passed_reports_expired_with_that_exact_instant() {
        let deadline = at(2_000_000);
        let exec = exec_at(deadline).await;

        match exec.wait_or_expire(&gate_node(), None, &folded_await(Some(deadline))) {
            Ok(WaitState::Expired(d)) => assert_eq!(
                d, deadline,
                "the failure must name the instant the run actually recorded"
            ),
            other => panic!("expected Expired, got {}", describe(&other)),
        }

        // `now >= deadline` is inclusive: expiring exactly ON the deadline is the
        // fail-closed reading, and the boundary is where an off-by-one would live.
        let exec = exec_at(deadline + Duration::seconds(1)).await;
        assert!(
            matches!(
                exec.wait_or_expire(&gate_node(), None, &folded_await(Some(deadline))),
                Ok(WaitState::Expired(_))
            ),
            "a deadline in the past is expired"
        );
    }

    /// THE never-expires guard, at the helper level: the recorded deadline is READ BACK,
    /// and the `timeout` argument is ignored once anything is recorded. Recomputing
    /// `now + timeout` here is what made a run force-woken every ten minutes with a
    /// one-hour timeout never expire.
    #[tokio::test]
    async fn a_future_deadline_is_read_back_and_the_timeout_argument_is_ignored() {
        let recorded = at(2_000_000);
        let exec = exec_at(at(1_000_000)).await;

        // A timeout that, if recomputed, would produce a visibly different instant.
        let got = exec.wait_or_expire(
            &gate_node(),
            Some(Duration::seconds(9_999)),
            &folded_await(Some(recorded)),
        );
        match got {
            Ok(WaitState::Waiting(Some(d))) => assert_eq!(
                d,
                recorded,
                "the ORIGINAL deadline survives; recomputing it from `now + timeout` \
                 would return {}",
                at(1_000_000) + Duration::seconds(9_999)
            ),
            other => panic!("expected Waiting(Some(recorded)), got {}", describe(&other)),
        }
    }

    /// The indefinite human gate that has already begun asking: `Some(None)` is a REAL
    /// recorded value, so it must report as waiting forever — never as "not yet asking",
    /// which would re-journal its awaited event on every drive.
    #[tokio::test]
    async fn an_indefinite_gate_that_has_begun_asking_waits_with_no_deadline() {
        let exec = exec_at(at(1_000_000)).await;
        match exec.wait_or_expire(&gate_node(), Some(Duration::hours(1)), &folded_await(None)) {
            Ok(WaitState::Waiting(None)) => {}
            other => panic!("expected Waiting(None), got {}", describe(&other)),
        }
    }

    /// Nothing recorded ⇒ the ONE execution that computes a deadline, from this `now`.
    #[tokio::test]
    async fn nothing_recorded_computes_the_deadline_once_from_now_plus_the_timeout() {
        let now = at(1_000_000);
        let exec = exec_at(now).await;
        match exec.wait_or_expire(&gate_node(), Some(Duration::hours(1)), &Fold::default()) {
            Ok(WaitState::NotYetAsking(Some(d))) => {
                assert_eq!(d, now + Duration::hours(1));
            }
            other => panic!("expected NotYetAsking(Some(..)), got {}", describe(&other)),
        }
    }

    /// A timeout-less node records `None` — which the caller journals as a real value, so
    /// the node reads back as "already waiting" forever after.
    #[tokio::test]
    async fn nothing_recorded_and_no_timeout_computes_no_deadline_at_all() {
        let exec = exec_at(at(1_000_000)).await;
        match exec.wait_or_expire(&gate_node(), None, &Fold::default()) {
            Ok(WaitState::NotYetAsking(None)) => {}
            other => panic!("expected NotYetAsking(None), got {}", describe(&other)),
        }
    }

    /// Layer 2 of the overflow guard, at the helper. `chrono::Duration` reaches ~292
    /// million years and `DateTime<Utc>` stops at year 262143, so the plain `+` panics —
    /// and a panic in a node kind is not local, it takes the worker down through
    /// `Scheduler::tick`. The helper must report, not panic, and must journal nothing
    /// (it cannot: it has no journal handle).
    #[tokio::test]
    async fn an_unaddable_timeout_reports_an_error_rather_than_panicking() {
        let exec = exec_at(at(1_000_000)).await;
        match exec.wait_or_expire(&gate_node(), Some(Duration::MAX), &Fold::default()) {
            Err(message) => assert!(
                message.contains("gate") && message.contains("overflows"),
                "the error names the offending node and the reason: {message}"
            ),
            other => panic!("expected Err, got {}", describe(&other)),
        }
    }

    fn describe(state: &Result<WaitState, String>) -> String {
        match state {
            Err(m) => format!("Err({m})"),
            Ok(WaitState::NotYetAsking(d)) => format!("NotYetAsking({d:?})"),
            Ok(WaitState::Expired(d)) => format!("Expired({d})"),
            Ok(WaitState::Waiting(d)) => format!("Waiting({d:?})"),
        }
    }

    /// A clock that reads one instant ONCE and a later one thereafter — the minimum needed
    /// to pin the second expiry site, and deliberately not a change to the `Clock` trait.
    ///
    /// `run_await_signal` reads the clock exactly twice on the fresh-deadline path
    /// (`wait_or_expire` fixes the deadline, then the post-match check re-reads it after an
    /// `await`ed journal append — `Executor::append` itself reads no clock), and under
    /// `FakeClock` both reads return the same instant, which is why the deterministic suite
    /// could not reach that path at all.
    struct SteppingClock {
        first: DateTime<Utc>,
        then: DateTime<Utc>,
        reads: AtomicUsize,
    }
    impl Clock for SteppingClock {
        fn now(&self) -> DateTime<Utc> {
            if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
                self.first
            } else {
                self.then
            }
        }
    }

    /// Pins the SECOND expiry site — the post-match check in `run_await_signal` — which no
    /// s1 test reaches, because it fires only when the clock moves between the two reads.
    ///
    /// The comment above that check documents this as "measured": `timeout: Some(1ns)`
    /// against a REAL clock journals `SignalAwaited` and then `NodeFailed` in a single
    /// execution. A measurement nothing re-runs is a claim, not a guard, and against a real
    /// clock it would be a race. A stepping clock makes it deterministic: the deadline is
    /// fixed from `first`, and the check re-reads `then`, which is past it.
    ///
    /// Correct and loud — a gate given a nanosecond to answer has genuinely expired. The
    /// order matters as much as the outcome: the awaited event is journaled BEFORE the
    /// failure, because the node did begin asking, and the run must not pause.
    #[tokio::test]
    async fn a_deadline_that_passes_between_the_two_clock_reads_fails_the_node_and_never_pauses() {
        let (gw, _c) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        journal
            .append(
                run,
                JournalEvent::RunStarted {
                    version: "v1".into(),
                    budget: None,
                },
            )
            .await
            .expect("seed RunStarted");

        let first = at(1_000_000);
        let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1").with_clock(
            Arc::new(SteppingClock {
                first,
                then: first + Duration::seconds(1),
                reads: AtomicUsize::new(0),
            }),
        );

        let node = Node {
            id: gate(),
            kind: NodeKind::AwaitSignal {
                timeout: Some(Duration::milliseconds(1)),
            },
            deps: vec![],
        };
        let result = exec
            .run_await_signal(
                run,
                &node,
                Some(Duration::milliseconds(1)),
                &Fold::default(),
            )
            .await
            .expect("an expiry is a node failure, not an executor error");

        match result {
            NodeExec::Failed { message, .. } => assert!(
                message.contains("no signal for node gate"),
                "the failure names the node: {message}"
            ),
            NodeExec::Paused { reason } => panic!(
                "a deadline that has passed must NOT pause — the scheduler would re-arm on \
                 an instant already behind it: Paused({reason})"
            ),
            NodeExec::Completed(v) => panic!("expected a loud failure, got Completed({v})"),
        }

        let events = journal.load(run).await.unwrap();
        let kinds: Vec<&str> = events
            .iter()
            .map(|(_, e)| match e {
                JournalEvent::RunStarted { .. } => "RunStarted",
                JournalEvent::SignalAwaited { .. } => "SignalAwaited",
                JournalEvent::NodeFailed { .. } => "NodeFailed",
                JournalEvent::RunPaused { .. } => "RunPaused",
                _ => "other",
            })
            .collect();
        assert_eq!(
            kinds,
            vec!["RunStarted", "SignalAwaited", "NodeFailed"],
            "the node records that it began asking, then fails — and pauses on nothing"
        );
    }
}

// ================================ SP-6 s2 HumanGate ============================

/// SP-6 s2: the TYPED gate — a human picks one of an enumerated menu, and each option
/// carries its own outcome, so a rejection has real semantics instead of being a value
/// the author must remember to test for. Every test drives a graph over a `FakeClock`,
/// so the deadline arithmetic is exact and no test sleeps.
///
/// The rows of design §6.2's fold read, one test each:
///   failure recorded              → `a_late_decision_never_resurrects_an_expired_gate`
///                                   + `a_corrected_decision_does_not_resurrect_a_failed_gate`
///   decided, in the menu, Complete → `a_complete_option_becomes_the_nodes_output`
///   decided, in the menu, Fail     → `a_fail_option_fails_the_node_naming_who_and_why`
///   decided, NOT in the menu       → `an_undeclared_option_fails_the_node_loudly`
///   no decision, deadline passed   → `an_expired_gate_never_self_approves`
///   no decision, still in time     → `a_well_formed_human_gate_never_panics_it_pauses_for_a_human`
mod human_gate {
    use super::*;
    use crate::test_support::FakeClock;
    use chrono::{DateTime, Duration, Utc};
    use orchestrator_core::{GateOption, GateOutcome};

    /// A fixed instant, so every deadline in these tests is an exact literal.
    fn at(unix_secs: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(unix_secs, 0).expect("valid timestamp")
    }

    fn release() -> NodeId {
        NodeId("release".into())
    }

    fn opt(name: &str, outcome: GateOutcome) -> GateOption {
        GateOption {
            name: name.to_string(),
            outcome,
        }
    }

    /// ship = Complete, reject = Fail — the shape every test below uses.
    fn menu() -> Vec<GateOption> {
        vec![
            opt("ship", GateOutcome::Complete),
            opt("reject", GateOutcome::Fail),
        ]
    }

    fn gate_graph(timeout: Option<Duration>) -> Graph {
        Graph {
            nodes: vec![Node {
                id: release(),
                kind: NodeKind::HumanGate {
                    options: menu(),
                    timeout,
                },
                deps: vec![],
            }],
        }
    }

    /// An executor over a caller-owned journal, so a test can append a decision BETWEEN
    /// two drives exactly as `torii run gate decide` does from another process. The
    /// clock is handed back so a test can move time past a deadline.
    async fn exec_at(journal: &InMemoryJournal, now: DateTime<Utc>) -> (Executor, Arc<FakeClock>) {
        let clock = FakeClock::new(now);
        let (gw, _calls) = recording_gateway().await;
        let ex =
            Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1").with_clock(clock.clone());
        (ex, clock)
    }

    fn decided(node: &NodeId, option: &str, actor: &str, note: Option<&str>) -> JournalEvent {
        JournalEvent::GateDecided {
            node: node.clone(),
            option: option.to_string(),
            actor: actor.to_string(),
            note: note.map(str::to_string),
        }
    }

    /// Every `NodeFailed` this run journaled for the gate, in order.
    async fn failures(journal: &InMemoryJournal, run: RunId) -> Vec<String> {
        journal
            .load(run)
            .await
            .unwrap()
            .iter()
            .filter_map(|(_, e)| match e {
                JournalEvent::NodeFailed { node, error } if node == &release() => {
                    Some(error.clone())
                }
                _ => None,
            })
            .collect()
    }

    /// AC2, the Complete half: the decision becomes the node's output, in the exact shape
    /// `BranchCond::FieldEquals("decision", …)` matches — so `Branch` is reused unchanged.
    #[tokio::test]
    async fn a_complete_option_becomes_the_nodes_output() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let (ex, _clock) = exec_at(&journal, at(1_000)).await;

        // First drive: the gate asks and pauses.
        let o1 = ex.start(run, &gate_graph(None)).await.expect("drives");
        assert!(o1.paused.is_some(), "the gate pauses on the first drive");

        journal
            .append(run, decided(&release(), "ship", "alice", Some("cleared")))
            .await
            .unwrap();

        let o2 = ex.start(run, &gate_graph(None)).await.expect("resumes");
        assert!(o2.paused.is_none(), "answered: {o2:?}");
        let out = o2
            .outputs
            .get(&release())
            .expect("the gate produced output");
        assert_eq!(out["decision"], serde_json::json!("ship"));
        assert_eq!(out["actor"], serde_json::json!("alice"));
        assert_eq!(out["note"], serde_json::json!("cleared"));
    }

    /// AC2, the Fail half: a Fail option terminates the node, and the reason NAMES the
    /// actor and their reason — a rejection whose cause is unrecorded is useless in ops.
    #[tokio::test]
    async fn a_fail_option_fails_the_node_naming_who_and_why() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let (ex, _clock) = exec_at(&journal, at(1_000)).await;

        ex.start(run, &gate_graph(None)).await.expect("asks");
        journal
            .append(
                run,
                decided(&release(), "reject", "bob", Some("missing DPA")),
            )
            .await
            .unwrap();

        let o = ex.start(run, &gate_graph(None)).await.expect("resumes");
        let (node, message) = o.failed.clone().expect("a Fail option fails the node");
        assert_eq!(node, release());
        assert!(message.contains("bob"), "must name the actor: {message}");
        assert!(
            message.contains("missing DPA"),
            "must carry the reason: {message}"
        );
        assert!(
            !o.outputs.contains_key(&release()),
            "a rejected gate produces no output for dependents to read: {o:?}"
        );
    }

    /// AC3: an undeclared option FAILS the node loudly. Never ignored — ignoring would
    /// leave the gate waiting while the operator was told their decision landed, which is
    /// the silently-ineffective shape s1's review kept finding.
    #[tokio::test]
    async fn an_undeclared_option_fails_the_node_loudly() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let (ex, _clock) = exec_at(&journal, at(1_000)).await;

        ex.start(run, &gate_graph(None)).await.expect("asks");
        journal
            .append(run, decided(&release(), "shipp", "alice", None))
            .await
            .unwrap();

        let o = ex.start(run, &gate_graph(None)).await.expect("resumes");
        let (_node, message) = o.failed.expect("an undeclared option must fail the node");
        assert!(message.contains("shipp"), "must name the option: {message}");
        assert!(
            message.contains("ship") && message.contains("reject"),
            "must name the journaled menu so the operator can see the real choices: {message}"
        );
    }

    /// AC4: s1's exact regression, re-guarded one layer up. A decision arriving after the
    /// deadline must NEVER resurrect the gate — `torii` pre-checks then appends, and those
    /// two steps are not atomic, so the row can exist.
    ///
    /// The journaled-failure count is part of the assertion, not decoration: it is what
    /// pins `gate_precheck`'s "read the verdict back, never re-derive it" half here. The
    /// OUTCOME alone cannot — `wait_or_expire` reports `Expired` before the decision is
    /// ever read, so it re-derives the same failure and masks a missing precheck exactly
    /// as s1's two expiry sites masked each other. A second `NodeFailed` for a node that
    /// is already dead is that mask made visible.
    #[tokio::test]
    async fn a_late_decision_never_resurrects_an_expired_gate() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let (ex, clock) = exec_at(&journal, at(1_000)).await;

        ex.start(run, &gate_graph(Some(Duration::hours(1))))
            .await
            .expect("asks");

        // The deadline passes with no answer.
        clock.set(at(1_000) + Duration::hours(2));
        let expired = ex
            .start(run, &gate_graph(Some(Duration::hours(1))))
            .await
            .expect("drives");
        assert!(expired.failed.is_some(), "the deadline fired");

        // A late approval lands anyway.
        journal
            .append(run, decided(&release(), "ship", "alice", None))
            .await
            .unwrap();

        let after = ex
            .start(run, &gate_graph(Some(Duration::hours(1))))
            .await
            .expect("drives");
        let (_n, message) = after.failed.expect("the gate STAYS failed");
        assert!(
            message.contains("no decision"),
            "the expiry is read back, not replaced by the late answer: {message}"
        );
        assert!(
            !after.outputs.contains_key(&release()),
            "a late decision must not produce output"
        );
        assert_eq!(
            failures(&journal, run).await.len(),
            1,
            "the expiry is READ BACK from the fold: re-deriving it appends a second \
             NodeFailed for an already-dead node on every drive"
        );
    }

    /// AC4's sibling, and the guard that pins `gate_precheck` for this node kind by an
    /// OUTCOME flip rather than by a journal count: a gate that has already failed stays
    /// failed even when a perfectly valid decision lands afterwards.
    ///
    /// Reachable with no deadline at all, which is the point — the expiry path cannot
    /// isolate the precheck (see the note above), and here nothing else can stand in for
    /// it. Delete the `gate_precheck` call at the top of `run_human_gate` and this run
    /// COMPLETES carrying "ship": s1's self-approval-by-the-back-door, in the node kind
    /// whose entire purpose is a human decision.
    ///
    /// The operator-facing cost is real and deliberate: a mistyped option is TERMINAL,
    /// because a `NodeFailed` on a waiting node is irreversible by construction. That is
    /// why `torii run gate decide` validates the option against the journaled menu before
    /// it appends anything — the CLI is what keeps this path rare; the executor is what
    /// keeps it safe when the CLI is bypassed.
    #[tokio::test]
    async fn a_corrected_decision_does_not_resurrect_a_failed_gate() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let (ex, _clock) = exec_at(&journal, at(1_000)).await;

        ex.start(run, &gate_graph(None)).await.expect("asks");
        journal
            .append(run, decided(&release(), "shipp", "alice", None))
            .await
            .unwrap();
        let failed = ex.start(run, &gate_graph(None)).await.expect("drives");
        assert!(
            failed.failed.is_some(),
            "the typo fails the gate: {failed:?}"
        );

        // The operator corrects themselves. Too late: the node is terminal.
        journal
            .append(run, decided(&release(), "ship", "alice", None))
            .await
            .unwrap();
        let after = ex.start(run, &gate_graph(None)).await.expect("drives");
        let (_n, message) = after
            .failed
            .expect("a gate that has already failed STAYS failed");
        assert!(
            message.contains("shipp"),
            "the ORIGINAL failure is read back verbatim: {message}"
        );
        assert!(
            !after.outputs.contains_key(&release()),
            "a corrected decision must not produce output for a dead gate"
        );
    }

    /// AC5: expiry produces a failure and NEVER an output. A gate that self-approves on
    /// timeout is the footgun this codebase's fail-closed stance exists against, and s1 §8
    /// mandates that it stay impossible to configure here.
    #[tokio::test]
    async fn an_expired_gate_never_self_approves() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let (ex, clock) = exec_at(&journal, at(1_000)).await;

        ex.start(run, &gate_graph(Some(Duration::hours(1))))
            .await
            .expect("asks");
        clock.set(at(1_000) + Duration::hours(2));

        let o = ex
            .start(run, &gate_graph(Some(Duration::hours(1))))
            .await
            .expect("drives");
        assert!(o.failed.is_some(), "expiry fails");
        assert!(
            !o.outputs.contains_key(&release()),
            "expiry must produce NO output, defaulted or otherwise"
        );
    }

    /// AC14: the ask precedes the answer, unconditionally.
    ///
    /// A durable menu BREAKS s1's "the early-signal race resolves itself for free"
    /// property: a decision folded with no menu has nothing to validate against. Resolved
    /// by journaling the ask FIRST, then reading the pending decision against the menu
    /// just published — so the early decision is honoured in the SAME execution and there
    /// is never a decision without a menu.
    #[tokio::test]
    async fn a_decision_delivered_before_the_gate_first_runs_still_resolves() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let (ex, _clock) = exec_at(&journal, at(1_000)).await;

        // The answer lands BEFORE the node has ever executed.
        journal
            .append(run, decided(&release(), "ship", "alice", None))
            .await
            .unwrap();

        let o = ex.start(run, &gate_graph(None)).await.expect("drives");
        assert!(o.paused.is_none(), "the early decision resolves it: {o:?}");
        assert_eq!(
            o.outputs.get(&release()).expect("output")["decision"],
            serde_json::json!("ship")
        );

        // ...and the menu was still published, so the audit trail records what was offered.
        let kinds: Vec<&str> = journal
            .load(run)
            .await
            .unwrap()
            .iter()
            .map(|(_, e)| match e {
                JournalEvent::GateAwaited { .. } => "GateAwaited",
                JournalEvent::GateDecided { .. } => "GateDecided",
                _ => "other",
            })
            .collect();
        assert!(
            kinds.contains(&"GateAwaited"),
            "the ask must be journaled even when the answer arrived first: {kinds:?}"
        );
    }

    /// AC1: THE MENU IS DURABLE. The decision is validated against the menu journaled in
    /// `GateAwaited`, never against the graph handed to this drive.
    ///
    /// A human was shown a menu; validating their answer against a DIFFERENT menu later
    /// is simply wrong. This is the same argument s1 made for the deadline ("the deadline
    /// belongs to the RUN, not to the graph"), and it is reachable for the same reason:
    /// `Executor::start` takes the graph as a caller parameter and never journals it.
    #[tokio::test]
    async fn a_decision_is_validated_against_the_journaled_menu_not_the_graph() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let (ex, _clock) = exec_at(&journal, at(1_000)).await;

        // Ask with the real menu: ship | reject.
        ex.start(run, &gate_graph(None)).await.expect("asks");
        journal
            .append(run, decided(&release(), "ship", "alice", None))
            .await
            .unwrap();

        // The author now edits the graph, dropping `ship` entirely. The human's recorded
        // answer must STILL resolve — it was valid when they gave it.
        let edited = Graph {
            nodes: vec![Node {
                id: release(),
                kind: NodeKind::HumanGate {
                    options: vec![
                        opt("escalate", GateOutcome::Complete),
                        opt("reject", GateOutcome::Fail),
                    ],
                    timeout: None,
                },
                deps: vec![],
            }],
        };

        let o = ex.start(run, &edited).await.expect("resumes");
        assert!(
            o.failed.is_none(),
            "an edited graph must not retroactively invalidate a recorded decision: {o:?}"
        );
        assert_eq!(
            o.outputs.get(&release()).expect("output")["decision"],
            serde_json::json!("ship"),
            "the answer resolves against the menu the human was SHOWN"
        );
    }

    /// AC11: a decided gate replays from the fold — no gateway call, so zero token
    /// re-spend by construction.
    #[tokio::test]
    async fn a_decided_gate_costs_nothing_on_resume() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let clock = FakeClock::new(at(1_000));
        let (gw, calls) = recording_gateway().await;
        let ex =
            Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1").with_clock(clock.clone());

        ex.start(run, &gate_graph(None)).await.expect("asks");
        journal
            .append(run, decided(&release(), "ship", "alice", None))
            .await
            .unwrap();
        ex.start(run, &gate_graph(None)).await.expect("resumes");
        ex.start(run, &gate_graph(None))
            .await
            .expect("resumes again");

        assert_eq!(
            calls.lock().unwrap().len(),
            0,
            "a gate must never call the gateway"
        );
    }

    /// A gate whose node id already recorded a wait but published NO menu — the one shape
    /// that can reach the answer read with nothing durable to validate against. It is
    /// reachable only by editing a live run's graph to swap a waiting node's KIND
    /// (`AwaitSignal` → `HumanGate`), because both kinds fold their "has this node begun
    /// asking?" record into the same map while only `GateAwaited` carries a menu.
    ///
    /// It fails loudly rather than falling back to the graph's `options`: that fallback
    /// would be a menu no human was ever shown, which is precisely the non-durable menu
    /// §4 rejects — and it would be silent.
    #[tokio::test]
    async fn a_gate_that_recorded_a_wait_without_a_menu_fails_loudly() {
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let (ex, _clock) = exec_at(&journal, at(1_000)).await;

        // As if this node had begun waiting as an `AwaitSignal` before the graph was
        // edited: a recorded wait, no menu.
        journal
            .append(
                run,
                JournalEvent::RunStarted {
                    version: "v1".into(),
                    budget: None,
                },
            )
            .await
            .unwrap();
        journal
            .append(
                run,
                JournalEvent::SignalAwaited {
                    node: release(),
                    deadline: None,
                },
            )
            .await
            .unwrap();
        journal
            .append(run, decided(&release(), "ship", "alice", None))
            .await
            .unwrap();

        let o = ex.start(run, &gate_graph(None)).await.expect("drives");
        let (_n, message) = o
            .failed
            .expect("a decision with no journaled menu must fail, not resolve");
        assert!(
            message.contains("release") && message.contains("menu"),
            "the failure names the node and what is missing: {message}"
        );
        assert!(
            !o.outputs.contains_key(&release()),
            "no output may be derived from a menu that was never published"
        );
    }

    /// The never-panics property, carried across the implementation boundary.
    ///
    /// Task 2's code-quality review, Important 1: `NodeKind::HumanGate { .. } =>
    /// unimplemented!()` was a REACHABLE worker panic on a graph `validate_dag` had just
    /// certified well-formed (`run_inner`/`start_inner` both call it before ever reaching
    /// the `run_node` match) — the exact poison-pill shape
    /// `submitting_an_unaddable_timeout_leaves_no_poison_row_in_the_scheduler` already
    /// fixed once for `AwaitSignal`: `Scheduler::submit` enqueues the durable store row
    /// BEFORE the drive, so a panicking drive leaves a `(Waking, next_wake: None)` row
    /// that every later `tick()` reclaims and panics on again.
    ///
    /// Task 2 satisfied that by failing the node loudly; Task 5 replaces the expectation
    /// with the real behaviour — an unanswered gate PAUSES for its human — and the
    /// property itself is unchanged and still guarded: this node kind never panics, and
    /// the executor call itself never errors.
    #[tokio::test]
    async fn a_well_formed_human_gate_never_panics_it_pauses_for_a_human() {
        let (gw, _c) = recording_gateway().await;
        let journal = InMemoryJournal::new();
        let run = RunId(uuid::Uuid::new_v4());
        let exec = Executor::new(Arc::new(gw), Arc::new(journal.clone()), "v1");

        let graph = gate_graph(None);
        graph.validate_dag().expect(
            "a well-formed HumanGate is a valid graph — the panic this test guards \
             against only happens on a graph validate_dag has certified",
        );

        let outcome = exec.run(run, &graph).await.expect(
            "an unanswered gate pauses the RUN; it must not error (let alone panic) the \
             executor call itself",
        );

        let pause = outcome
            .paused
            .expect("an unanswered gate waits for its human rather than failing or completing");
        assert_eq!(pause.node, release());
        assert!(
            pause.reason.contains("release"),
            "the pause names the node an operator must answer: {}",
            pause.reason
        );
        assert!(
            outcome.failed.is_none(),
            "waiting for a human is not a failure: {:?}",
            outcome.failed
        );

        let events = journal.load(run).await.unwrap();
        assert!(
            events.iter().any(
                |(_, e)| matches!(e, JournalEvent::GateAwaited { node, .. } if node == &release())
            ),
            "the ask is journaled with the menu the human is being shown"
        );
    }
}

/// SP-DATA-1 (5/5) — the HEADLINE: cross-process durable resume + durable in-doubt reconcile,
/// proven on a live Docker Postgres. Feature-gated (`postgres-tests`) AND `DATABASE_URL`-guarded,
/// so the default suite is byte-identical and DB-free (each test `return`s early with no DB).
///
/// The Executor is UNCHANGED: the durable `PostgresJournal`/`PostgresContentStore` inject through
/// the same seams the InMemory resume tests use (`Executor::new(.., journal, ..)` +
/// `with_content_store`). These tests reach Postgres ONLY through
/// `orchestrator_store::postgres::{connect, PostgresJournal, PostgresContentStore}` — no direct
/// sqlx handle. Every test uses a fresh `RunId` so the shared `orchestrator.*` tables never
/// collide across tests (belt-and-suspenders with `--test-threads=1`). SP-DATA-3 adds the scheduler
/// cross-process wake e2e here too.
#[cfg(feature = "postgres-tests")]
mod postgres_e2e {
    use super::*;
    use orchestrator_core::{RegistryConfig, RegistryHandle};
    use orchestrator_store::postgres::{
        PostgresConfigSource, PostgresContentStore, PostgresJournal, PostgresSchedulerStore,
        connect,
    };

    fn db_url() -> Option<String> {
        std::env::var("DATABASE_URL").ok()
    }

    /// The headline (spec §4.3, AC4/AC5): a run journaled by one Executor instance resumes in a
    /// FRESH Executor (process B) on the SAME Postgres DB — completes, the memoized effect replays
    /// with ZERO gateway re-spend, and a CAS `Ref` materializes from the durable Postgres CAS.
    ///
    /// Mirrors the in-memory `resume_folds_a_ref_lazily_and_rematerializes_it_from_the_cas_without_respending`,
    /// but the two executors share NOTHING in-process: they hold independent pools/instances over
    /// the same `DATABASE_URL`, so a pass proves the state crossed the process boundary via Postgres.
    #[tokio::test]
    async fn postgres_cross_process_resume_replays_from_the_durable_journal() {
        let Some(url) = db_url() else { return };

        let run = RunId(uuid::Uuid::new_v4());
        let (graph, n1, n2) = two_node_graph("a", "b"); // linear: n1 -> n2

        // ---- Process A: seed a PARTIAL run into Postgres --------------------------------------
        // n1 succeeds; its ~28-byte model output exceeds the low CAS threshold, so the durable
        // journal records an `EffectOutput::Ref` and the blob lands in the PG CAS. n2 then fails
        // (the crash) → no `RunCompleted`; the run is left durably resumable.
        let journal_a = PostgresJournal::new(connect(&url).await.unwrap());
        let content_a = PostgresContentStore::new(connect(&url).await.unwrap());
        let (gw_a, _calls_a) = failing_after_gateway(1).await;
        let out_a = Executor::new(Arc::new(gw_a), Arc::new(journal_a.clone()), "v1")
            .with_content_store(Arc::new(content_a))
            .with_cas_threshold(8)
            .run(run, &graph)
            .await
            .expect("seed run yields an outcome");
        assert!(
            out_a.failed.is_some(),
            "n2 crashes, leaving n1 durably journaled without RunCompleted"
        );

        // n1's effect was recorded as a durable Ref (not inline). Capture its digest so we can
        // prove the blob materializes from the PG CAS via a fresh pool below.
        let events_a = journal_a.load(run).await.unwrap();
        let n1_digest = events_a
            .iter()
            .find_map(|(_, e)| match e {
                JournalEvent::EffectRecorded {
                    node,
                    output: EffectOutput::Ref(r),
                    ..
                } if node == &n1 => Some(r.digest.clone()),
                _ => None,
            })
            .expect("n1's over-threshold output split to a durable Ref in Postgres");

        // ---- Process B: a FRESH Executor + FRESH PG stores on the SAME DATABASE_URL -----------
        // Brand-new `PostgresJournal`/`PostgresContentStore` instances (independent pools) over
        // the same durable schema + the same run id — nothing shared in-process. A call-counting
        // gateway proves n1 is NOT re-spent.
        let journal_b = PostgresJournal::new(connect(&url).await.unwrap());
        let content_b = PostgresContentStore::new(connect(&url).await.unwrap());
        let (gw_b, calls_b) = recording_gateway().await;
        let out_b = Executor::new(Arc::new(gw_b), Arc::new(journal_b), "v1")
            .with_content_store(Arc::new(content_b))
            .with_cas_threshold(8)
            .start(run, &graph)
            .await
            .expect("the fresh executor resumes from the durable journal");

        // The run COMPLETES across the process boundary.
        assert!(out_b.failed.is_none(), "{:?}", out_b.failed);
        assert_eq!(out_b.completed, vec![n1.clone(), n2.clone()]);

        // ZERO re-spend: the fresh gateway was called ONLY for the tail n2 — n1 replayed from the
        // durable memo, never re-dispatched.
        assert_eq!(
            calls_b.lock().unwrap().len(),
            1,
            "the fresh process re-called the gateway only for n2 (n1 replayed → 0 re-spend)"
        );

        // n1's output MATERIALIZED from the durable Postgres CAS Ref (proves the ref crossed the
        // boundary and the blob is durable, not in-process).
        assert_eq!(
            out_b.outputs[&n1]["text"], "canned-response",
            "n1 re-materialized from the Postgres CAS on resume"
        );

        // And the Ref is genuinely addressable in the durable CAS via a THIRD fresh pool.
        let cas = PostgresContentStore::new(connect(&url).await.unwrap());
        let bytes = cas
            .get(&n1_digest)
            .await
            .expect("the Ref blob is durable in the Postgres CAS");
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value["text"], "canned-response",
            "the durable CAS blob round-trips"
        );
    }

    /// Durable in-doubt reconcile (spec §4.3, AC5): a standing `EffectIntent` with no matching
    /// `EffectRecorded` persisted in Postgres → a FRESH Executor resumes, folds the durable
    /// journal, sees the in-doubt Mutation, and CONSULTS the reconciler (a status query) rather
    /// than blindly re-running. The reconciler finds the key → `Confirmed` → the executor records
    /// WITHOUT re-invoking the tool: no double-apply across the process boundary.
    ///
    /// Mirrors the in-memory `exactly_once_confirmed_by_key_does_not_double_apply`, but the
    /// standing intent lives in Postgres (durable) and a brand-new `PostgresJournal` reads it back.
    #[tokio::test]
    async fn postgres_in_doubt_reconcile_is_durable() {
        let Some(url) = db_url() else { return };

        // The mutation's effect id: n1, turn 0, tool index 1 (turn-0 model is index 0).
        let store_eid = effect_id("n1", 0, 1);
        // The shared "external system" (a keyed HashMap) — durable state lives in PG; the world is
        // this mock, shared across the seed and the resume via `Arc` clone.
        let store: Store = Store::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let invocations = Arc::new(AtomicUsize::new(0));
        let graph = Graph {
            nodes: vec![agent_node("n1", "a", "store it")],
        };

        // ---- Seed: run the full flow into Postgres, then persist ONLY the prefix up to and
        // including the mutation's `EffectIntent` under a FRESH run id — a durable standing intent
        // with no `EffectRecorded`. The live seed applies the effect once (store[key], calls == 1).
        let seed_journal = PostgresJournal::new(connect(&url).await.unwrap());
        let seed_run = RunId(uuid::Uuid::new_v4());
        let (gw_seed, _c) = scripted_gateway(vec![
            tool_call_response("t1", "store", "{\"item\":\"widget\"}"),
            final_response("done"),
        ])
        .await;
        Executor::new(Arc::new(gw_seed), Arc::new(seed_journal.clone()), "v1")
            .with_registry(store_registry())
            .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(
                IdempotentStore {
                    store: store.clone(),
                    calls: calls.clone(),
                    invocations: invocations.clone(),
                },
            ))))
            .run(seed_run, &graph)
            .await
            .expect("seed run completes");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the live seed applied the side effect once before the crash"
        );

        let events = seed_journal.load(seed_run).await.unwrap();
        let cut = events
            .iter()
            .position(|(_, e)| matches!(e, JournalEvent::EffectIntent { .. }))
            .expect("seed run journaled an EffectIntent");
        // Persist the prefix under a fresh run id via a fresh PostgresJournal — the durable
        // standing intent (no `EffectRecorded` for the mutation).
        let run = RunId(uuid::Uuid::new_v4());
        let indoubt_journal = PostgresJournal::new(connect(&url).await.unwrap());
        for (_, e) in &events[..=cut] {
            indoubt_journal.append(run, e.clone()).await.unwrap();
        }

        // ---- Resume: a FRESH Executor + FRESH PostgresJournal on the SAME DATABASE_URL, sharing
        // the SAME store + a `StatusQueryReconciler` over it. The reconciler finds the key →
        // Confirmed → records without re-running the tool.
        let reconcilers = ReconcileRegistry::default().with_provider(
            "store",
            Arc::new(StatusQueryReconciler {
                store: store.clone(),
            }),
        );
        let (gw_resume, _c2) = scripted_gateway(vec![final_response("done")]).await;
        let resume_journal = PostgresJournal::new(connect(&url).await.unwrap());
        let out = Executor::new(Arc::new(gw_resume), Arc::new(resume_journal.clone()), "v1")
            .with_registry(store_registry())
            .with_tools(Arc::new(ToolRegistry::default().with_tool(Arc::new(
                IdempotentStore {
                    store: store.clone(),
                    calls: calls.clone(),
                    invocations: invocations.clone(),
                },
            ))))
            .with_reconcilers(Arc::new(reconcilers))
            .start(run, &graph)
            .await
            .expect("resume yields an outcome");
        let resume_events = resume_journal.load(run).await.unwrap();

        // The reconcile path ran (not a blind re-run): Confirmed completes with no pause/failure.
        assert!(
            out.failed.is_none() && out.paused.is_none(),
            "durable in-doubt Confirmed completes cleanly: failed={:?} paused={:?}",
            out.failed,
            out.paused
        );
        assert_eq!(
            invocations.load(Ordering::SeqCst),
            1,
            "the tool was NOT re-invoked on resume — Confirmed records from the reconciler, not the tool"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "exactly-once across the process boundary: Confirmed records without re-applying"
        );
        assert_eq!(
            store.lock().unwrap().len(),
            1,
            "the external system still holds exactly one entry for the key (no double-apply)"
        );
        assert_eq!(
            effect_recorded_count(&resume_events, &store_eid),
            1,
            "the standing Intent resolves to exactly one Mutation EffectRecorded"
        );
        assert!(
            resume_events
                .iter()
                .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
            "the resumed run completes"
        );
    }

    /// AC5 — unchanged config, cross-process resume PASSES with zero re-spend. Process A runs a
    /// partial with a handle booted from the durable config version → `RunStarted.version = "v1#cfg{v}"`.
    /// A FRESH process B boots a fresh handle from the SAME source (config unchanged) → same gen → the
    /// fence matches → the completed prefix replays from the durable memo (the fresh gateway is called
    /// only for the tail). The cross-process form of the in-process fence test
    /// `reload_bumps_the_run_version_and_fences_in_flight_resume`.
    #[tokio::test]
    async fn postgres_unchanged_config_generation_permits_cross_process_resume() {
        let Some(url) = db_url() else { return };
        let run = RunId(uuid::Uuid::new_v4());
        let (graph, n1, n2) = two_node_graph("a", "b");

        // Seed durable config + move to a known generation.
        let cfg_src = PostgresConfigSource::new(connect(&url).await.unwrap());
        cfg_src.store(&RegistryConfig::default()).await.unwrap();
        let v = cfg_src.bump_config_version().await.unwrap(); // v >= 1

        // Process A: partial run (n1 ok, n2 crashes), handle pinned at the durable version.
        let handle_a = RegistryHandle::from_source(&cfg_src).await.unwrap();
        assert_eq!(
            handle_a.generation(),
            v,
            "handle boots at the durable version"
        );
        let (gw_a, _ca) = failing_after_gateway(1).await;
        let out_a = Executor::new(
            Arc::new(gw_a),
            Arc::new(PostgresJournal::new(connect(&url).await.unwrap())),
            "v1",
        )
        .with_content_store(Arc::new(PostgresContentStore::new(
            connect(&url).await.unwrap(),
        )))
        .with_cas_threshold(8)
        .with_registry_handle(handle_a)
        .run(run, &graph)
        .await
        .expect("seed run yields an outcome");
        assert!(
            out_a.failed.is_some(),
            "n2 crashes, leaving n1 durably journaled"
        );

        // Process B: FRESH source/handle over the SAME DB, config unchanged (still v).
        let cfg_src_b = PostgresConfigSource::new(connect(&url).await.unwrap());
        let handle_b = RegistryHandle::from_source(&cfg_src_b).await.unwrap();
        assert_eq!(
            handle_b.generation(),
            v,
            "process B agrees on the durable generation"
        );
        let (gw_b, calls_b) = recording_gateway().await;
        let out_b = Executor::new(
            Arc::new(gw_b),
            Arc::new(PostgresJournal::new(connect(&url).await.unwrap())),
            "v1",
        )
        .with_content_store(Arc::new(PostgresContentStore::new(
            connect(&url).await.unwrap(),
        )))
        .with_cas_threshold(8)
        .with_registry_handle(handle_b)
        .start(run, &graph)
        .await
        .expect("fence matches across processes → resume proceeds");
        assert!(out_b.failed.is_none(), "{:?}", out_b.failed);
        assert_eq!(out_b.completed, vec![n1.clone(), n2.clone()]);
        assert_eq!(
            calls_b.lock().unwrap().len(),
            1,
            "n1 replayed from the durable memo → the fresh gateway ran only the tail (0 re-spend)"
        );
    }

    /// AC6 — a config change (a bump) between the original run and the resume is caught LOUDLY across
    /// the process boundary. AC7 (mutation-check): were `version()` to return None, both handles would
    /// boot at 0 and this mismatch would NOT fire — proving the durable version carries the fence.
    #[tokio::test]
    async fn postgres_bumped_config_generation_fences_a_cross_process_resume() {
        let Some(url) = db_url() else { return };
        let run = RunId(uuid::Uuid::new_v4());
        let (graph, _n1, _n2) = two_node_graph("a", "b");

        let cfg_src = PostgresConfigSource::new(connect(&url).await.unwrap());
        cfg_src.store(&RegistryConfig::default()).await.unwrap();
        let v = cfg_src.bump_config_version().await.unwrap();

        // Process A runs (fully) at generation v.
        let handle_a = RegistryHandle::from_source(&cfg_src).await.unwrap();
        let (gw_a, _ca) = recording_gateway().await;
        Executor::new(
            Arc::new(gw_a),
            Arc::new(PostgresJournal::new(connect(&url).await.unwrap())),
            "v1",
        )
        .with_content_store(Arc::new(PostgresContentStore::new(
            connect(&url).await.unwrap(),
        )))
        .with_registry_handle(handle_a)
        .run(run, &graph)
        .await
        .expect("A completes at gen v");

        // Config is re-committed and the generation bumped → v2 (the fence is generation-based,
        // so advancing the counter is what matters, not the content).
        cfg_src.store(&RegistryConfig::default()).await.unwrap();
        let v2 = cfg_src.bump_config_version().await.unwrap();
        assert!(v2 > v, "the bump advanced the durable generation");

        // Process B boots at v2 → resuming the v-authored run is fenced LOUDLY.
        let handle_b =
            RegistryHandle::from_source(&PostgresConfigSource::new(connect(&url).await.unwrap()))
                .await
                .unwrap();
        assert_eq!(handle_b.generation(), v2);
        let (gw_b, _cb) = recording_gateway().await;
        let err = Executor::new(
            Arc::new(gw_b),
            Arc::new(PostgresJournal::new(connect(&url).await.unwrap())),
            "v1",
        )
        .with_content_store(Arc::new(PostgresContentStore::new(
            connect(&url).await.unwrap(),
        )))
        .with_registry_handle(handle_b)
        .start(run, &graph)
        .await
        .expect_err("a changed config generation must fence the cross-process resume");
        assert!(
            matches!(
                &err,
                OrchestratorError::VersionFenceMismatch { recorded, current }
                    if recorded == &format!("v1#cfg{v}") && current == &format!("v1#cfg{v2}")
            ),
            "expected a loud config-generation fence, got {err:?}"
        );
    }

    /// SP-DATA-3 AC6: a run paused by process A wakes durably in a FRESH process B at its deadline.
    /// The scheduler owns the graph (`scheduled_runs`), so process B needs only the run id + the SAME
    /// DB — it shares NOTHING in-process with A. B reads A's durable pause (status + deadline) from PG,
    /// then a `tick` past the deadline drives the woken run to completion (the gated node re-attempts
    /// once under the un-gated gateway — a real quota reset).
    #[tokio::test]
    async fn scheduler_wakes_a_paused_run_cross_process() {
        let Some(url) = db_url() else { return };
        use crate::Scheduler;
        use crate::test_support::{FakeClock, gated_gateway};
        use chrono::{DateTime, Duration, Utc};
        use orchestrator_core::{RunStatus, SchedulerStore};

        let run = RunId(uuid::Uuid::new_v4());
        let graph = Graph {
            nodes: vec![Node {
                id: NodeId("n1".into()),
                kind: model_call("c", "go"),
                deps: vec![],
            }],
        };
        let clock = FakeClock::new(DateTime::<Utc>::from_timestamp(3_000_000, 0).unwrap());

        // --- Process A: submit with a gated executor → pauses + persists to PG (scheduled_runs + journal).
        let store_a = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
        let journal_a = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
        let gw = gated_gateway().await;
        let exec_a = Executor::new(Arc::new(gw), journal_a.clone(), "v1").with_clock(clock.clone());
        let sched_a = Scheduler::new(store_a.clone(), exec_a, journal_a.clone(), clock.clone());
        let o1 = sched_a.submit(run, graph.clone()).await.unwrap();
        assert!(o1.paused.is_some(), "the run pauses on the timed gate");
        let deadline = store_a
            .status(run)
            .await
            .unwrap()
            .unwrap()
            .next_wake
            .expect("a timed pause has a next_wake");

        // --- Process B: FRESH store/journal/executor on the SAME DB — nothing shared in-process.
        let store_b = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
        let journal_b = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
        // B reads A's durable pause straight from Postgres.
        let st_b = store_b.status(run).await.unwrap().unwrap();
        assert_eq!(st_b.status, RunStatus::Paused, "B sees A's durable pause");
        assert_eq!(
            st_b.next_wake,
            Some(deadline),
            "B sees the durable deadline"
        );

        let (gw_b, calls_b) = recording_gateway().await;
        let clock_b = FakeClock::new(deadline + Duration::seconds(1));
        let exec_b = Executor::new(Arc::new(gw_b), journal_b.clone(), "v1")
            .with_content_store(Arc::new(PostgresContentStore::new(
                connect(&url).await.unwrap(),
            )))
            .with_clock(clock_b.clone());
        let sched_b = Scheduler::new(store_b.clone(), exec_b, journal_b.clone(), clock_b.clone());
        // A tick past the deadline wakes the due set — OUR run among any others sharing the
        // `scheduled_runs` table (unique run ids + a GLOBAL claim, per the harness's shared-table
        // convention). Assert OUR run's outcome, not the global count.
        let woken = sched_b.tick().await.unwrap();
        assert!(woken >= 1, "process B wakes at least the due run");
        assert_eq!(
            store_b.status(run).await.unwrap().unwrap().status,
            RunStatus::Completed,
            "the woken run completes across the process boundary"
        );
        assert_eq!(
            calls_b.lock().unwrap().len(),
            1,
            "the woken run drove the one node once (no re-spend beyond it)"
        );
    }
}
