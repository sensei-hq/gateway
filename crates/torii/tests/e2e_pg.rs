//! AC8 — the operator loop, end to end, across a process boundary.
//!
//! `DATABASE_URL`-guarded rather than feature-gated: torii depends on
//! `orchestrator-store/postgres` unconditionally (it has no non-Postgres mode), so there
//! is no feature to hang this off. Absent a database each test returns early, which keeps
//! the default `cargo test` DB-free.
//!
//! Process A submits a graph against a gated gateway and it takes a DURABLE pause. The
//! torii operator commands then observe that pause and act on it. Finally a FRESH set of
//! stores + executor — process B, sharing NOTHING in-process with A — drives it through
//! torii's own `worker serve --once` loop, with zero re-spend of the tokens A already paid
//! for.
//!
//! Every test here drives its process B through the REAL [`torii::cmd::worker::serve`]
//! rather than calling `Scheduler::tick` directly: asserting a re-implementation of the
//! single-tick contract would prove nothing about the command an operator actually runs.
//! That is the reason this crate is a lib+bin pair.
//!
//! SP-6 s1 adds AC7, the HITL counterpart: the pause is not a provider's, it is a
//! GRAPH's — an `AwaitSignal` node designed to wait for a human — and the operator
//! command in the middle carries an ANSWER back in rather than merely resuming.

use chrono::{DateTime, Duration, Utc};
use kernel::types::cost::TokenUsage;
use orchestrator::test_support::{
    CallLog, FakeClock, gated_gateway, metered_gateway, recording_gateway,
};
use orchestrator::{Executor, Scheduler};
use orchestrator_core::{
    ConfigSource, ContextKey, ContextStore, ExecutionJournal, GateOption, GateOutcome, Graph,
    JournalEvent, Node, NodeId, NodeKind, RegistryHandle, RunId, RunStatus, SchedulerStore, Scope,
    TokenBudget,
};
use orchestrator_store::postgres::{
    PostgresConfigSource, PostgresContentStore, PostgresContextStore, PostgresJournal,
    PostgresSchedulerStore, connect,
};
use std::sync::Arc;

/// **The SECOND layer, since the conditional-ignore gate.** The first is
/// `#[cfg_attr(not(have_database_url), ignore = "...")]` on every test below, driven by
/// this package's `build.rs` — that is what makes a database-less run report these as
/// `ignored` rather than as PASSED, which is what the runtime early-return made them.
/// This helper still runs, and still announces, for the case the cfg cannot see: the
/// variable present at BUILD time and gone at run time.
///
/// WHOLE-SLICE FIX 6: `None` also emits a VISIBLE skip notice naming the test, so a green
/// run that touched no database is distinguishable from one that did. Written to the real
/// stderr rather than through `eprintln!`, which libtest captures for a passing test. (Same
/// helper as torii's internal `test_guard::db_url`; an integration test is a separate crate
/// and cannot see a `#[cfg(test)]` item.)
fn db_url() -> Option<String> {
    let url = std::env::var("DATABASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty());
    if url.is_none() {
        use std::io::Write;
        let name = std::thread::current()
            .name()
            .unwrap_or("<unnamed test>")
            .to_string();
        // Formatted first, then ONE `write_all`: `Stderr` is unbuffered, so a
        // `writeln!` emits a separate syscall per format fragment and a parallel
        // test's output interleaves mid-line.
        let line = format!("SKIP {name}: DATABASE_URL not set\n");
        let _ = std::io::stderr().write_all(line.as_bytes());
    }
    url
}

/// `scheduled_runs` is a GLOBAL table and `tick()` claims the whole due set, not just the
/// caller's run — so two of these tests running concurrently would each drive the other's
/// run through their own gateway, and the re-spend counts below would be measuring the
/// wrong executor. The singleton `config_versions` row has the same problem for the fence
/// test, which asserts an exact generation.
///
/// Both now come from `orchestrator_store::test_guard`, the ONE shared implementation. This
/// file used to hold a THIRD private copy — and a weaker one: a bare process-wide
/// `tokio::sync::Mutex`, justified by "cross-PROCESS isolation is not needed: cargo runs
/// test binaries one at a time". That is a property of `cargo test`, not of the code:
/// `cargo nextest` runs test binaries in parallel and would defeat it silently, and it was
/// never true of two concurrent `cargo` invocations either. The shared guard adds a Postgres
/// session advisory lock, which holds across processes however the suite is invoked.
use orchestrator_store::test_guard::{config_guard, scheduler_guard};

/// One `ModelCall` node whose prompt is `marker` — the smallest graph that spends a token.
///
/// The marker is the run's own id, which makes every gateway call attributable to the run
/// that caused it. That matters because a `tick` legitimately drives EVERY due run in the
/// shared table (leftover rows from other suites included), so a bare `calls.len()` would
/// count other runs' spend as this one's.
fn one_node_graph(marker: &str) -> Graph {
    Graph {
        nodes: vec![Node {
            id: NodeId("n1".into()),
            kind: NodeKind::ModelCall {
                chain: "c".into(),
                payload: serde_json::json!({ "prompt": marker }),
            },
            deps: vec![],
        }],
    }
}

/// SP-DATA-5 AC6: a LINEAR TWO-node graph, `n1 → n2`, each node's prompt carrying the
/// run's own marker plus its node id so a call is attributable to both the run AND the
/// node — which is what lets the budget e2e below say "n1 was replayed, n2 was driven"
/// rather than merely "one call happened".
///
/// Two nodes because a budget pause needs a node LEFT to gate: the run must spend past
/// its cap on `n1` and then be refused at `n2`, in that same drive.
fn two_node_graph(marker: &str) -> Graph {
    let node = |id: &str| Node {
        id: NodeId(id.into()),
        kind: NodeKind::ModelCall {
            chain: "c".into(),
            payload: serde_json::json!({ "prompt": format!("{marker}#{id}") }),
        },
        deps: if id == "n1" {
            vec![]
        } else {
            vec![orchestrator_core::Dep::hard("n1")]
        },
    };
    Graph {
        nodes: vec![node("n1"), node("n2")],
    }
}

/// SP-6 s1 AC7: `n1 → gate → n2`, where `gate` is an [`NodeKind::AwaitSignal`] — the
/// smallest graph with all three parts the acceptance criterion names: a PREFIX that
/// costs real tokens (`n1`, paid for by process A), the human gate itself, and a
/// SUCCESSOR that can only run once the gate has read its answer (`n2`).
///
/// Each `ModelCall`'s prompt IS its node id, and every node id is qualified by the run's
/// own marker, so a recorded gateway call is attributable to both the run AND the node —
/// the same technique [`two_node_graph`] uses, and the only way the zero-re-spend
/// assertion can say "`n1` was replayed, `n2` was driven" rather than merely "one call
/// happened".
///
/// The ids must be run-unique for a second, durable reason: this test wires a
/// `PostgresContextStore` (as `boot::heavy` does in production), and `context_refs`' primary
/// key is `(scope_kind, scope_id, ctx_key)` — a `Scope::Run` write carries an EMPTY scope
/// id, so two runs publishing the same node id collide LOUDLY
/// (`ContextKeyCollision`). Sharing the bare `n1` of the tests above would make this suite
/// fail on its second run against a persistent database.
fn signal_graph(marker: &str, timeout: Option<Duration>) -> Graph {
    let (n1, gate, n2) = signal_graph_ids(marker);
    let model = |id: &NodeId, deps: Vec<orchestrator_core::Dep>| Node {
        id: id.clone(),
        kind: NodeKind::ModelCall {
            chain: "c".into(),
            payload: serde_json::json!({ "prompt": id.0 }),
        },
        deps,
    };
    Graph {
        nodes: vec![
            model(&n1, vec![]),
            Node {
                id: gate.clone(),
                kind: NodeKind::AwaitSignal { timeout },
                deps: vec![orchestrator_core::Dep::hard(n1.0.clone())],
            },
            model(&n2, vec![orchestrator_core::Dep::hard(gate.0.clone())]),
        ],
    }
}

/// The three node ids of [`signal_graph`], in graph order. A named helper because the test
/// asserts on all three by name and a drifted literal would silently assert nothing.
fn signal_graph_ids(marker: &str) -> (NodeId, NodeId, NodeId) {
    (
        NodeId(format!("n1-{marker}")),
        NodeId(format!("gate-{marker}")),
        NodeId(format!("n2-{marker}")),
    )
}

/// SP-6 s2 AC12: [`signal_graph`]'s shape with the middle node a [`NodeKind::HumanGate`]
/// instead — `n1 → release → n2`, where `release` offers an enumerated menu rather than
/// accepting arbitrary JSON.
///
/// The menu carries BOTH outcomes (`ship` completes, `hold` fails) even though the test
/// only ever picks `ship`: a one-outcome menu would let a `HumanGate` that ignored
/// `GateOutcome` entirely — completing on whatever it was handed — pass the whole e2e.
///
/// Everything else is [`signal_graph`]'s reasoning verbatim, including the run-unique node
/// ids: each `ModelCall`'s prompt IS its node id (so a recorded call is attributable to
/// the run AND the node), and `context_refs`' `(scope_kind, scope_id, ctx_key)` primary
/// key makes a shared bare `n1` collide LOUDLY on this suite's second run against a
/// persistent database.
fn gate_graph(marker: &str, timeout: Option<Duration>) -> Graph {
    let (n1, release, n2) = gate_graph_ids(marker);
    let model = |id: &NodeId, deps: Vec<orchestrator_core::Dep>| Node {
        id: id.clone(),
        kind: NodeKind::ModelCall {
            chain: "c".into(),
            payload: serde_json::json!({ "prompt": id.0 }),
        },
        deps,
    };
    Graph {
        nodes: vec![
            model(&n1, vec![]),
            Node {
                id: release.clone(),
                kind: NodeKind::HumanGate {
                    options: vec![
                        GateOption {
                            name: "ship".into(),
                            outcome: GateOutcome::Complete,
                        },
                        GateOption {
                            name: "hold".into(),
                            outcome: GateOutcome::Fail,
                        },
                    ],
                    timeout,
                },
                deps: vec![orchestrator_core::Dep::hard(n1.0.clone())],
            },
            model(&n2, vec![orchestrator_core::Dep::hard(release.0.clone())]),
        ],
    }
}

/// The three node ids of [`gate_graph`], in graph order — a named helper for the same
/// reason [`signal_graph_ids`] is one.
fn gate_graph_ids(marker: &str) -> (NodeId, NodeId, NodeId) {
    (
        NodeId(format!("n1-{marker}")),
        NodeId(format!("release-{marker}")),
        NodeId(format!("n2-{marker}")),
    )
}

/// SP-6 s3 AC13: the human-backed role this suite asks. Its `system_prompt` is the
/// question, and `backed_by: Human` is the ONLY thing that makes the middle node of
/// [`human_agent_graph`] wait for a person rather than call a model.
///
/// `chain: None`, no `chains`, and — crucially — no `(area, kind)` binding anywhere: a
/// human-backed role has no gateway chain by construction, and `drive_agent` branches
/// BEFORE `resolve_chain` precisely so that shape is legal. If the branch ever moved below
/// the chain resolution, this definition would fail the node rather than pause it, which is
/// why the definition is left un-resolvable on purpose instead of being given a harmless
/// `chain: Some("c")`.
const REVIEWER_PROMPT: &str = "Read the MSA and say whether it permits sub-processing.";

/// What the node hands the role — the model path's first user message, which SP-6 s3's
/// `drive_agent` appends to `assemble_prompt`'s output under a `## Task` heading so the
/// human sees what the model would have seen. Asserted on BOTH sides of the process
/// boundary below.
const REVIEWER_TASK: &str = "the Acme MSA, clause 7";

fn reviewer(timeout: Option<Duration>) -> orchestrator_core::AgentDefinition {
    orchestrator_core::AgentDefinition {
        name: "reviewer".into(),
        area: "review".into(),
        kind: "human".into(),
        chain: None,
        chains: Default::default(),
        grants: Default::default(),
        tools: vec![],
        skills: vec![],
        system_prompt: REVIEWER_PROMPT.into(),
        backed_by: orchestrator_core::AgentBacking::Human { timeout },
    }
}

/// A registry holding exactly [`reviewer`], for ONE process.
///
/// Called separately by each side of the boundary rather than cloning one `Arc` across it,
/// so the two "processes" share no registry state — the same discipline every other helper
/// in this file applies to pools, journals and gateways.
///
/// **Stated precisely, because it is the one seam this test does NOT carry over the
/// database:** the config here is built in-process on each side rather than read back from
/// `PostgresConfigSource`, so what is proven below is a human-backed role surviving a
/// process boundary in the JOURNAL and the SCHEDULER — not its `AgentDefinition` surviving
/// a `config_agents` jsonb round-trip. Adjacent coverage, named rather than gestured at:
/// `orchestrator-core`'s `agent_backing_is_serde_defaulted_and_round_trips` covers the
/// serde of a `config_agents` row, and `orchestrator-store`'s
/// `filesystem_source_carries_a_human_backing_through_to_the_registry` covers the authored
/// md → `load` → `Registry::from_config` path. What NOTHING covers today, and this test
/// does not either, is a human-backed definition written to and read back from a live
/// `PostgresConfigSource`.
///
/// Wiring `PostgresConfigSource` here instead would also pin a config generation into the
/// fence (`RegistryHandle::from_source`, the way `fresh_worker_pinned` does it), which is
/// the subject of `a_stale_config_generation_fails_a_wake_at_the_fence_before_spending_
/// anything` and not of this one.
fn human_registry(timeout: Option<Duration>) -> std::sync::Arc<orchestrator_core::Registry> {
    Arc::new(orchestrator_core::Registry::default().with_agent(reviewer(timeout)))
}

/// SP-6 s3 AC13: [`gate_graph`]'s shape with the middle node a top-level
/// [`NodeKind::Agent`] pointing at the human-backed [`reviewer`] — `n1 → review → n2`,
/// where `review` is a ROLE answered by a person and takes free text with an actor.
///
/// Top-level deliberately: `drive_agent` refuses a human-backed role in a `Map`/`Loop`
/// body, a loop gate or a planner (design §5.5), so this is the only position in which the
/// node pauses at all.
///
/// **No `timeout` parameter, unlike [`gate_graph`] and [`signal_graph`]**, and that is the
/// design speaking rather than an omission: s3 put the SLA on the ROLE
/// (`AgentBacking::Human { timeout }`) so a role and its deadline travel together and the
/// graph never changes. The deadline is therefore threaded through [`human_registry`].
///
/// Everything else is [`signal_graph`]'s reasoning verbatim, including the run-unique node
/// ids: each `ModelCall`'s prompt IS its node id (so a recorded call is attributable to the
/// run AND the node), and `context_refs`' `(scope_kind, scope_id, ctx_key)` primary key
/// makes a shared bare `n1` collide LOUDLY on this suite's second run against a persistent
/// database.
fn human_agent_graph(marker: &str) -> Graph {
    let (n1, review, n2) = human_agent_graph_ids(marker);
    let model = |id: &NodeId, deps: Vec<orchestrator_core::Dep>| Node {
        id: id.clone(),
        kind: NodeKind::ModelCall {
            chain: "c".into(),
            payload: serde_json::json!({ "prompt": id.0 }),
        },
        deps,
    };
    Graph {
        nodes: vec![
            model(&n1, vec![]),
            Node {
                id: review.clone(),
                kind: NodeKind::Agent {
                    agent: orchestrator_core::AgentRef("reviewer".into()),
                    input: serde_json::json!(REVIEWER_TASK),
                    phase: None,
                },
                deps: vec![orchestrator_core::Dep::hard(n1.0.clone())],
            },
            model(&n2, vec![orchestrator_core::Dep::hard(review.0.clone())]),
        ],
    }
}

/// The three node ids of [`human_agent_graph`], in graph order — a named helper for the
/// same reason [`signal_graph_ids`] is one.
fn human_agent_graph_ids(marker: &str) -> (NodeId, NodeId, NodeId) {
    (
        NodeId(format!("n1-{marker}")),
        NodeId(format!("review-{marker}")),
        NodeId(format!("n2-{marker}")),
    )
}

/// [`fresh_context_worker`], but ALSO carrying the human-backed registry — which a real
/// worker gets from its config source and without which `drive_agent` cannot resolve the
/// `AgentRef` at all (`OrchestratorError::UnknownAgent`).
///
/// Its own constructor rather than a parameter on `fresh_context_worker`, so every
/// existing test's wiring stays byte-identical.
async fn fresh_human_worker(
    url: &str,
    at: DateTime<Utc>,
    timeout: Option<Duration>,
) -> (Scheduler, CallLog) {
    let (exec, journal, clock, calls) = fresh_context_executor(url, at).await;
    let exec = exec.with_registry(human_registry(timeout));
    let store = Arc::new(PostgresSchedulerStore::new(connect(url).await.unwrap()));
    (Scheduler::new(store, exec, journal, clock), calls)
}

/// How many recorded gateway calls carried `marker` as their prompt.
fn calls_for(log: &CallLog, marker: &str) -> usize {
    log.lock()
        .unwrap()
        .iter()
        .filter(|(_, prompt)| prompt == marker)
        .count()
}

/// A fresh executor over its own pool, journal and content store, clocked at `at`, plus
/// the call log of its own private recording gateway. A separate "process" in every sense
/// that matters: it shares no in-process state with the submitting side — only the
/// database.
async fn fresh_executor(
    url: &str,
    at: DateTime<Utc>,
) -> (Executor, Arc<PostgresJournal>, Arc<FakeClock>, CallLog) {
    let journal = Arc::new(PostgresJournal::new(connect(url).await.unwrap()));
    let (gw, calls) = recording_gateway().await;
    let clock = FakeClock::new(at);
    let exec = Executor::new(Arc::new(gw), journal.clone(), "v1")
        .with_content_store(Arc::new(PostgresContentStore::new(
            connect(url).await.unwrap(),
        )))
        .with_clock(clock.clone());
    (exec, journal, clock, calls)
}

/// [`fresh_executor`] behind a scheduler — a worker process.
async fn fresh_worker(url: &str, at: DateTime<Utc>) -> (Scheduler, CallLog) {
    let (exec, journal, clock, calls) = fresh_executor(url, at).await;
    let store = Arc::new(PostgresSchedulerStore::new(connect(url).await.unwrap()));
    (Scheduler::new(store, exec, journal, clock), calls)
}

/// SP-DATA-5: [`fresh_worker`], but over a gateway that REPORTS token usage.
///
/// A separate constructor rather than a parameter on `fresh_worker` because the two are
/// not interchangeable under a budget: `recording_gateway` hardcodes `usage: None`, which
/// a budgeted run refuses outright as an unmetered call (spec §4, fail closed). A worker
/// that resumes a budgeted run therefore MUST be metered, and a test that used the
/// recording double here would fail the node instead of completing the run — arriving at
/// a red test by the right rule, which is confusing rather than informative.
async fn fresh_metered_worker(
    url: &str,
    at: DateTime<Utc>,
    per_call_tokens: u32,
) -> (Scheduler, CallLog) {
    let journal = Arc::new(PostgresJournal::new(connect(url).await.unwrap()));
    let (gw, calls) = metered_gateway(Some(usage(per_call_tokens))).await;
    let clock = FakeClock::new(at);
    let exec = Executor::new(Arc::new(gw), journal.clone(), "v1")
        .with_content_store(Arc::new(PostgresContentStore::new(
            connect(url).await.unwrap(),
        )))
        .with_clock(clock.clone());
    let store = Arc::new(PostgresSchedulerStore::new(connect(url).await.unwrap()));
    (Scheduler::new(store, exec, journal, clock), calls)
}

/// A provider-reported usage of `total` tokens on one call. The input/output split is
/// arbitrary — `Fold::spent` only ever reads `total_tokens`.
fn usage(total: u32) -> TokenUsage {
    TokenUsage {
        input_tokens: total / 2,
        output_tokens: total - total / 2,
        total_tokens: total,
    }
}

/// Like [`fresh_worker`], but ALSO wired with a registry handle freshly loaded from the
/// shared durable config source — exactly how `boot::heavy` wires a real worker
/// (`RegistryHandle::from_source` then `Executor::with_registry_handle`), so
/// `Executor::start` pins `self.version` to `"{fence}#cfg{generation}"` using WHATEVER
/// generation is durably current at the moment this "process" boots, not the raw
/// unversioned fence base the other e2e tests use. Needed only by the fence-composition
/// test below: a worker that never pins a generation can never observe a fence drift.
async fn fresh_worker_pinned(url: &str, at: DateTime<Utc>) -> (Scheduler, CallLog) {
    let journal = Arc::new(PostgresJournal::new(connect(url).await.unwrap()));
    let (gw, calls) = recording_gateway().await;
    let clock = FakeClock::new(at);
    let config_source = PostgresConfigSource::new(connect(url).await.unwrap());
    let handle = RegistryHandle::from_source(&config_source)
        .await
        .expect("a registry handle over the shared config source");
    let exec = Executor::new(Arc::new(gw), journal.clone(), "v1")
        .with_content_store(Arc::new(PostgresContentStore::new(
            connect(url).await.unwrap(),
        )))
        .with_registry_handle(handle)
        .with_clock(clock.clone());
    let store = Arc::new(PostgresSchedulerStore::new(connect(url).await.unwrap()));
    (Scheduler::new(store, exec, journal, clock), calls)
}

/// SP-6 s1: [`fresh_executor`], but ALSO wired with a durable `PostgresContextStore` —
/// which is how `boot::heavy` wires every real torii process, and what makes the
/// blackboard half of the signal path observable.
///
/// It matters twice over. A completed node publishes its output to the blackboard
/// (`Executor::publish_context` → a `ContextWrite` keyed by the node id), so with a store
/// wired the signalled gate's PAYLOAD becomes a durable row a third process can read back
/// — the only way to assert the human's answer actually became the node's output without
/// going through a terminal re-`start`, whose `RunOutcome` is empty for a node kind that
/// journals no `NodeCompleted` (see the test below). And `torii`'s own `signal_state` fold
/// treats that same `ContextWrite` as the completion marker for exactly those node kinds,
/// so a store-less run would exercise only its `RunCompleted` backstop.
async fn fresh_context_executor(
    url: &str,
    at: DateTime<Utc>,
) -> (Executor, Arc<PostgresJournal>, Arc<FakeClock>, CallLog) {
    let journal = Arc::new(PostgresJournal::new(connect(url).await.unwrap()));
    let (gw, calls) = recording_gateway().await;
    let clock = FakeClock::new(at);
    let exec = Executor::new(Arc::new(gw), journal.clone(), "v1")
        .with_content_store(Arc::new(PostgresContentStore::new(
            connect(url).await.unwrap(),
        )))
        .with_context_store(Arc::new(PostgresContextStore::new(
            connect(url).await.unwrap(),
        )))
        .with_clock(clock.clone());
    (exec, journal, clock, calls)
}

/// [`fresh_context_executor`] behind a scheduler — the SP-6 s1 worker process.
async fn fresh_context_worker(url: &str, at: DateTime<Utc>) -> (Scheduler, CallLog) {
    let (exec, journal, clock, calls) = fresh_context_executor(url, at).await;
    let store = Arc::new(PostgresSchedulerStore::new(connect(url).await.unwrap()));
    (Scheduler::new(store, exec, journal, clock), calls)
}

/// Exactly one tick through torii's real worker loop. A `watch` receiver that never
/// reaches level 1 for shutdown — `_tx` is kept alive (not dropped) for the duration
/// of the call so `changed()` genuinely never resolves: `--once` must not need a
/// signal to stop.
async fn serve_once(sched: &Scheduler) -> torii::cmd::Outcome {
    let (_tx, rx) = tokio::sync::watch::channel(0u64);
    torii::cmd::worker::serve(
        sched,
        torii::cmd::worker::ServeOpts {
            interval: std::time::Duration::from_millis(10),
            once: true,
        },
        rx,
    )
    .await
    .expect("one tick against a live database")
}

/// Drive `run` through the REAL `worker serve --once` until it LEAVES `Paused`, and return
/// the outcome of the serve that moved it.
///
/// Why a loop rather than the single `serve_once` this replaces: a tick claims at most
/// `CLAIM_BATCH` (64) due rows, `order by next_wake`, out of a `scheduled_runs` table that
/// every suite which has ever paused a run shares. An OLDER backlog — the rows a crashed or
/// red run leaves behind — therefore crowds this test's run out of the first batch, and the
/// serve returns a perfectly healthy `tick complete: 64 run(s) woken` having never touched
/// it. Measured, not theorised: seeding 300 ancient due rows turned three of these tests red
/// with `left: Paused / right: Completed` and one with `left: Paused / right: Failed`, none
/// of which had anything wrong with them.
///
/// This is not a weaker assertion — every caller still asserts the exact terminal status and
/// the exact per-node spend afterwards. It is the same thing a real `torii worker serve`
/// (without `--once`) does: keep ticking. The bound keeps a GENUINE never-woken regression
/// loud and fast rather than trading one flake for a hang.
///
/// Only for callers that expect the run to MOVE. The tests asserting a run stays `Paused`
/// (a wake with no decision) or stays `Cancelled` keep using `serve_once`: for them a
/// crowd-out leaves the row untouched, which is exactly what they assert.
async fn serve_until_settled(
    sched: &Scheduler,
    store: &PostgresSchedulerStore,
    run: RunId,
) -> torii::cmd::Outcome {
    for _ in 0..16 {
        let served = serve_once(sched).await;
        let status = store.status(run).await.unwrap().map(|s| s.status);
        if status != Some(RunStatus::Paused) {
            return served;
        }
    }
    panic!("`worker serve --once` never moved {run:?} off `paused` in 16 ticks");
}

/// AC8. The full operator loop over a real Postgres:
///
/// 1. **Process A** submits against a warmed-gated gateway → the run pauses, durably.
/// 2. **The operator, light tier** (`run list-paused`, `run status`) sees that pause,
///    reading only what A wrote to the database.
/// 3. **The operator intervenes** (`run wake`) to queue it for the next tick — and the run
///    is still merely `paused` afterwards, because `wake` queues, it does not drive.
/// 4. **Process B** — a fresh store/journal/content-store/gateway — drives it through
///    torii's real `worker serve --once`.
/// 5. The run reaches `Completed`, and process B's gateway saw exactly ONE call for this
///    run: the single un-run node. A's journaled prefix was replayed from the durable
///    journal + CAS, never re-spent.
#[cfg_attr(
    not(have_database_url),
    ignore = "needs a Postgres at $DATABASE_URL; see README, Postgres-backed tests"
)]
#[tokio::test]
async fn the_operator_loop_drives_a_paused_run_to_completion_across_processes() {
    let Some(url) = db_url() else { return };
    let _guard = scheduler_guard().await;

    let run = RunId(uuid::Uuid::new_v4());
    let marker = run.0.to_string();
    let graph = one_node_graph(&marker);
    let clock = FakeClock::new(DateTime::<Utc>::from_timestamp(3_000_000, 0).unwrap());

    // ---- Process A: submit against a gated gateway → a durable pause ---------------
    let store_a = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
    let journal_a = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
    let exec_a = Executor::new(Arc::new(gated_gateway().await), journal_a.clone(), "v1")
        .with_content_store(Arc::new(PostgresContentStore::new(
            connect(&url).await.unwrap(),
        )))
        .with_clock(clock.clone());
    let sched_a = Scheduler::new(store_a.clone(), exec_a, journal_a.clone(), clock.clone());
    // `|| {}` for the announce hook: `main` passes the `submitted: <id>` print, which a
    // test has no use for.
    let submitted = torii::cmd::run::submit(&sched_a, run, graph.clone(), None, || {})
        .await
        .expect("a paused run is not an error");
    assert_eq!(submitted.code, torii::errors::EXIT_OK, "{}", submitted.text);
    assert!(
        submitted.text.starts_with("paused:"),
        "the gated run must PAUSE (resumable), not complete or fail: {}",
        submitted.text
    );

    // ---- The operator, light tier: observe A's pause, sharing nothing with A -------
    // A separate pool, and torii's own commands — the operator's real surface.
    let store_b = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
    // SP-DATA-5 Task 5: `status`/`wake` need a journal too (spend lives there, not in
    // the scheduler row) — a fresh handle over its OWN connection, same discipline as
    // `store_b`: process B shares nothing in-process with A.
    let journal_b = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
    let listed = torii::cmd::run::list_paused(store_b.as_ref(), journal_b.as_ref(), false)
        .await
        .expect("list-paused");
    assert_eq!(listed.code, torii::errors::EXIT_OK);
    assert!(
        listed.text.contains(&marker),
        "list-paused must surface the durable pause: {}",
        listed.text
    );

    let shown = torii::cmd::run::status(store_b.as_ref(), journal_b.as_ref(), run, true)
        .await
        .expect("status");
    assert_eq!(shown.code, torii::errors::EXIT_OK, "{}", shown.text);
    assert!(
        shown.text.contains(&marker) && shown.text.contains("\"paused\""),
        "status must report THIS run's durable pause: {}",
        shown.text
    );
    let deadline = store_b
        .status(run)
        .await
        .unwrap()
        .expect("a schedule record")
        .next_wake
        .expect("a timed pause has a next_wake");

    // ---- The operator intervenes: queue it for the next tick -----------------------
    // `queued_at` is well past A's own deadline, so the assertion below cannot be
    // satisfied by the pre-existing timer still sitting in the column.
    let queued_at = deadline + Duration::seconds(600);
    let woken = torii::cmd::run::wake(store_b.as_ref(), journal_b.as_ref(), run, queued_at, None)
        .await
        .expect("wake");
    assert_eq!(woken.code, torii::errors::EXIT_OK, "{}", woken.text);
    assert!(woken.text.contains("queued for wake"), "{}", woken.text);
    let after_wake = store_b.status(run).await.unwrap().unwrap();
    assert_eq!(
        after_wake.status,
        RunStatus::Paused,
        "wake queues; it does not drive — the run must still be paused"
    );
    assert_eq!(
        after_wake.next_wake,
        Some(queued_at),
        "wake moved the deadline to the operator's `now`"
    );

    // ---- Process B: a FRESH worker drives it --------------------------------------
    // The ONLY thing carried over from A is the run id; everything else B needs — the
    // graph included — it reads out of Postgres.
    let (sched_b, calls_b) = fresh_worker(&url, queued_at + Duration::seconds(1)).await;
    let served = serve_until_settled(&sched_b, store_b.as_ref(), run).await;
    assert_eq!(served.code, torii::errors::EXIT_OK, "{}", served.text);

    // ---- The run completed, and A's journaled prefix was not re-spent --------------
    assert_eq!(
        store_b.status(run).await.unwrap().unwrap().status,
        RunStatus::Completed,
        "the woken run completes in the fresh process: {}",
        served.text
    );
    assert_eq!(
        calls_for(&calls_b, &marker),
        1,
        "exactly the one un-run node, driven once"
    );

    // ---- And the completed work is now MEMOIZED, durably ---------------------------
    // Process C: a third executor, again sharing nothing in-process, re-drives the very
    // same run. Everything it needs to know that `n1` is done — the journal events AND
    // the node's output, materialized from the durable CAS — comes out of Postgres, so
    // it must spend NOTHING. This is the assertion that actually pins "zero re-spend":
    // the `== 1` above would also hold if the journal were amnesiac, because B's node had
    // never run before. Here the node HAS run, in another process, and must not run again.
    let (exec_c, _journal_c, _clock_c, calls_c) =
        fresh_executor(&url, queued_at + Duration::seconds(2)).await;
    let resumed = exec_c
        .start(run, &graph)
        .await
        .expect("re-driving a completed run is a no-op, not an error");
    assert_eq!(
        resumed.completed,
        vec![NodeId("n1".into())],
        "the completed node is folded back out of the durable journal"
    );
    assert_eq!(
        calls_for(&calls_c, &marker),
        0,
        "a completed prefix is replayed from the journal + CAS, never re-spent"
    );
}

/// The other half of the operator's intervene surface, cross-process: `cancel` makes a
/// durable pause permanently un-wakeable, so a LATER worker tick — a fresh scheduler,
/// clocked well past the original deadline — must neither drive the run nor spend a token
/// on it.
///
/// That trailing tick is the whole point. Without it the test would only prove a status
/// column changed; with it, the claim path itself is shown to honour the cancellation,
/// which is the claim `run cancel`'s output actually makes to the operator.
#[cfg_attr(
    not(have_database_url),
    ignore = "needs a Postgres at $DATABASE_URL; see README, Postgres-backed tests"
)]
#[tokio::test]
async fn a_cancelled_run_is_never_driven_by_a_later_worker_tick() {
    let Some(url) = db_url() else { return };
    let _guard = scheduler_guard().await;

    let run = RunId(uuid::Uuid::new_v4());
    let marker = run.0.to_string();
    let clock = FakeClock::new(DateTime::<Utc>::from_timestamp(2_000_000, 0).unwrap());

    let store_a = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
    let journal_a = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
    let exec_a = Executor::new(Arc::new(gated_gateway().await), journal_a.clone(), "v1")
        .with_content_store(Arc::new(PostgresContentStore::new(
            connect(&url).await.unwrap(),
        )))
        .with_clock(clock.clone());
    let sched_a = Scheduler::new(store_a.clone(), exec_a, journal_a.clone(), clock.clone());
    let submitted = torii::cmd::run::submit(&sched_a, run, one_node_graph(&marker), None, || {})
        .await
        .expect("a paused run is not an error");
    assert!(submitted.text.starts_with("paused:"), "{}", submitted.text);
    let deadline = store_a
        .status(run)
        .await
        .unwrap()
        .unwrap()
        .next_wake
        .expect("a timed pause has a next_wake");

    // The operator cancels through torii, on a fresh pool.
    let store_b = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
    let cancelled = torii::cmd::run::cancel(store_b.as_ref(), run)
        .await
        .expect("cancel");
    assert_eq!(cancelled.code, torii::errors::EXIT_OK, "{}", cancelled.text);
    assert!(cancelled.text.contains("cancelled"), "{}", cancelled.text);

    // A worker tick an hour past the ORIGINAL deadline must not touch it.
    let (sched_b, calls_b) = fresh_worker(&url, deadline + Duration::seconds(3600)).await;
    let served = serve_once(&sched_b).await;
    assert_eq!(served.code, torii::errors::EXIT_OK, "{}", served.text);

    assert_eq!(
        store_b.status(run).await.unwrap().unwrap().status,
        RunStatus::Cancelled,
        "a cancelled run stays cancelled across a tick that was past its deadline"
    );
    assert_eq!(
        calls_for(&calls_b, &marker),
        0,
        "a cancelled run must cost nothing"
    );
}

/// SP-DATA-3's AC8 fence-composition arm, SP-DATA-4.1 Task 6: deferred at s3 ("explicitly
/// deferred testing it"), and made reachable in PRODUCTION by SP-DATA-4's `torii config
/// push` — a push that lands while a run is paused now strands it, and the whole-slice
/// review demonstrated the arm by hand rather than as a test. This is that test.
///
/// 1. **Process A** — pinned to the durably current config generation *g* via a real
///    `RegistryHandle::from_source` (exactly how `boot::heavy` wires a worker) — submits
///    against a gated gateway and the run pauses, durably, with `RunStarted.version =
///    "v1#cfg{g}"` journaled.
/// 2. **The generation is bumped** through `store_and_bump_if` — the same atomic
///    compare-and-swap `torii config push` uses — to `g+1`, with the config CONTENT left
///    untouched (only the generation moves, so this cannot be mistaken for a content
///    change tripping some other check).
/// 3. **Process B** — a FRESH worker that boots its OWN `RegistryHandle::from_source`
///    against the NOW-drifted generation — ticks. `Executor::start` computes
///    `self.version = "v1#cfg{g+1}"`, finds the journal's recorded `"v1#cfg{g}"` does not
///    match, and refuses with `VersionFenceMismatch` BEFORE touching the graph at all.
/// 4. The scheduler must record that as terminal-`Failed` (never `Paused`, never silently
///    dropped), naming both generations, and the run must have cost the gateway NOTHING —
///    the whole point of a fence is refusing before spending.
/// 5. Afterwards `torii run wake` on the now-`failed` run must say so plainly, not queue
///    it as if the fence had never fired.
#[cfg_attr(
    not(have_database_url),
    ignore = "needs a Postgres at $DATABASE_URL; see README, Postgres-backed tests"
)]
#[tokio::test]
async fn a_stale_config_generation_fails_a_wake_at_the_fence_before_spending_anything() {
    let Some(url) = db_url() else { return };
    // BOTH classes, in the documented global order (config-tables before scheduled-runs).
    // This is the one test here that also writes the SINGLETON `config_versions` row, and it
    // asserts an exact generation twice — `current == Some(gen_before)` ("nothing else may
    // move the shared generation between pin and bump") and `gen_after == gen_before + 1`.
    // Any concurrent config writer breaks both, and the scheduler guard it used to take
    // alone is a DIFFERENT lock class that excludes none of them.
    let _config = config_guard().await;
    let _guard = scheduler_guard().await;

    let run = RunId(uuid::Uuid::new_v4());
    let marker = run.0.to_string();
    let graph = one_node_graph(&marker);
    let clock = FakeClock::new(DateTime::<Utc>::from_timestamp(4_000_000, 0).unwrap());

    // ---- Process A: submit, PINNED to the config generation at submit time ---------
    let config_source = PostgresConfigSource::new(connect(&url).await.unwrap());
    let handle_a = RegistryHandle::from_source(&config_source)
        .await
        .expect("a registry handle over the shared config source");
    let gen_before = handle_a.snapshot().1;

    let store_a = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
    let journal_a = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
    let exec_a = Executor::new(Arc::new(gated_gateway().await), journal_a.clone(), "v1")
        .with_content_store(Arc::new(PostgresContentStore::new(
            connect(&url).await.unwrap(),
        )))
        .with_registry_handle(handle_a)
        .with_clock(clock.clone());
    let sched_a = Scheduler::new(store_a.clone(), exec_a, journal_a.clone(), clock.clone());
    let submitted = torii::cmd::run::submit(&sched_a, run, graph.clone(), None, || {})
        .await
        .expect("a paused run is not an error");
    assert!(
        submitted.text.starts_with("paused:"),
        "the gated run must PAUSE (resumable): {}",
        submitted.text
    );
    let deadline = store_a
        .status(run)
        .await
        .unwrap()
        .expect("a schedule record")
        .next_wake
        .expect("a timed pause has a next_wake");

    // ---- Bump the generation — the same CAS `torii config push` uses, content held
    // fixed so ONLY the generation moves ---------------------------------------------
    let (cfg, current) = config_source.load_versioned().await.unwrap();
    assert_eq!(
        current,
        Some(gen_before),
        "nothing else may move the shared generation between pin and bump"
    );
    let gen_after = config_source
        .store_and_bump_if(&cfg, gen_before)
        .await
        .unwrap()
        .expect("the CAS bump applies against the generation just read");
    assert_eq!(gen_after, gen_before + 1);

    // ---- Process B: a FRESH worker, pinned to the NOW-DRIFTED generation, ticks ----
    let (sched_b, calls_b) = fresh_worker_pinned(&url, deadline + Duration::seconds(1)).await;
    let served = serve_until_settled(&sched_b, store_a.as_ref(), run).await;
    assert_eq!(served.code, torii::errors::EXIT_OK, "{}", served.text);

    // ---- The run FAILS at the fence — never Completed, never silently re-Paused ----
    let after = store_a
        .status(run)
        .await
        .unwrap()
        .expect("the schedule record still exists");
    assert_eq!(
        after.status,
        RunStatus::Failed,
        "a drifted-generation wake must be recorded terminal-Failed, not completed or \
         re-paused: {after:?}"
    );
    let reason = after.reason.clone().unwrap_or_default();
    assert!(
        reason.contains("stale"),
        "the reason must name the fence miss as stale: {reason}"
    );
    assert!(
        reason.contains(&format!("v1#cfg{gen_before}"))
            && reason.contains(&format!("v1#cfg{gen_after}")),
        "the reason must name BOTH the recorded and the current generation: {reason}"
    );

    // ---- Zero gateway spend: it must fail AT THE FENCE, before spending anything ---
    assert_eq!(
        calls_for(&calls_b, &marker),
        0,
        "a fence miss must refuse before any model call — this run must cost nothing"
    );

    // ---- `torii run wake` afterwards gives an honest answer, not a silent no-op ----
    let woken = torii::cmd::run::wake(
        store_a.as_ref(),
        journal_a.as_ref(),
        run,
        deadline + Duration::seconds(2),
        None,
    )
    .await
    .expect("wake");
    assert_eq!(
        woken.code,
        torii::errors::EXIT_PRECONDITION,
        "{}",
        woken.text
    );
    assert!(
        woken.text.contains("not queued") && woken.text.contains("failed"),
        "an operator waking a failed run must be told plainly, not given a silent no-op: {}",
        woken.text
    );
}

/// SP-DATA-5 AC6 — the per-run token budget, end to end, across a process boundary.
///
/// The one property that cannot be shown in-process: a budget is only worth anything if
/// the ledger is DURABLE. An in-memory counter would restart at zero on every resume, so
/// each wake would let the run spend its whole cap again. Here process A's spend is read
/// back out of Postgres by a process that shares nothing with it.
///
/// 1. **Process A** submits a two-node graph with `--budget-tokens 100` against a gateway
///    that reports 150 tokens per call. `n1` dispatches (nothing spent yet), `n2` is
///    refused — inside that same drive, which is the floor-trigger property: the cap is
///    overshot by exactly ONE call, never by "everything the drive can reach".
/// 2. The pause is the **HOTL class** — `next_wake` NULL. No amount of waiting refills a
///    budget, so the scheduler must never auto-wake this run; only an operator can.
/// 3. **The operator, light tier** (`run status`) reads spend and cap out of the durable
///    journal — on its own pool, with no model credentials and no gateway at all.
/// 4. **`run wake --budget-tokens 1000`** appends `BudgetRaised` and queues the run.
/// 5. **Process B** — a fresh store/journal/content-store/gateway — drives it through
///    torii's real `worker serve --once` to `Completed`, folding the RAISED cap.
/// 6. **Zero re-spend**, asserted per node: B's gateway saw `n2` exactly once and `n1`
///    NEVER — A's already-paid-for prefix was replayed from the durable journal + CAS.
///    And the final ledger reads 300, not 150: spend ACCUMULATED across the boundary
///    rather than restarting, which is the whole point of journaling it.
#[cfg_attr(
    not(have_database_url),
    ignore = "needs a Postgres at $DATABASE_URL; see README, Postgres-backed tests"
)]
#[tokio::test]
async fn a_budget_exhausted_run_is_raised_by_an_operator_and_completes_in_a_fresh_process() {
    let Some(url) = db_url() else { return };
    let _guard = scheduler_guard().await;

    const PER_CALL: u32 = 150;
    const CAP: u64 = 100;
    const RAISED: u64 = 1_000;

    let run = RunId(uuid::Uuid::new_v4());
    let marker = run.0.to_string();
    let (first, second) = (format!("{marker}#n1"), format!("{marker}#n2"));
    let graph = two_node_graph(&marker);
    let clock = FakeClock::new(DateTime::<Utc>::from_timestamp(5_000_000, 0).unwrap());

    // ---- Process A: submit under a cap one call already exceeds --------------------
    let store_a = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
    let journal_a = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
    let (gw_a, calls_a) = metered_gateway(Some(usage(PER_CALL))).await;
    let exec_a = Executor::new(Arc::new(gw_a), journal_a.clone(), "v1")
        .with_content_store(Arc::new(PostgresContentStore::new(
            connect(&url).await.unwrap(),
        )))
        .with_clock(clock.clone());
    let sched_a = Scheduler::new(store_a.clone(), exec_a, journal_a.clone(), clock.clone());
    let submitted = torii::cmd::run::submit(
        &sched_a,
        run,
        graph.clone(),
        Some(TokenBudget { total_tokens: CAP }),
        || {},
    )
    .await
    .expect("a budget-paused run is not an error");
    assert_eq!(submitted.code, torii::errors::EXIT_OK, "{}", submitted.text);
    assert!(
        submitted.text.starts_with("paused:") && submitted.text.contains("budget:"),
        "an exhausted budget must PAUSE the run (resumable), naming the budget as the \
         cause: {}",
        submitted.text
    );
    assert!(
        submitted.text.contains("n2"),
        "the pause lands on the node that was REFUSED, not the one that spent: {}",
        submitted.text
    );

    // The floor-trigger property, measured: exactly one call escaped the 100-token cap.
    assert_eq!(
        (calls_for(&calls_a, &first), calls_for(&calls_a, &second)),
        (1, 0),
        "n1 spends past the cap and n2 is refused — the overshoot is bounded by ONE \
         call. Two calls here would mean the ledger froze for the drive."
    );

    // ---- The pause is the HOTL class: NULL deadline, never auto-woken ---------------
    let paused = store_a
        .status(run)
        .await
        .unwrap()
        .expect("a schedule record");
    assert_eq!(paused.status, RunStatus::Paused);
    assert_eq!(
        paused.next_wake, None,
        "a budget pause has no deadline to auto-wake on — waiting never refills a \
         budget, so only an operator can move this run: {paused:?}"
    );
    assert!(
        paused
            .reason
            .as_deref()
            .unwrap_or_default()
            .contains("budget:"),
        "the durable reason must name the budget: {paused:?}"
    );

    // ---- The operator, light tier: read spend out of the DURABLE journal ------------
    // A separate pool and a separate journal handle — this process has no gateway, no
    // model credentials, and shares nothing in-process with A.
    let store_b = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
    let journal_b = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
    let shown = torii::cmd::run::status(store_b.as_ref(), journal_b.as_ref(), run, false)
        .await
        .expect("status");
    assert_eq!(shown.code, torii::errors::EXIT_OK, "{}", shown.text);
    assert!(
        shown
            .text
            .contains(&format!("spent: {PER_CALL} / budget: {CAP}")),
        "status must show the spend it folded from the journal against the cap: {}",
        shown.text
    );

    // ---- The operator raises the cap and queues the run ----------------------------
    let queued_at = paused.updated_at + Duration::seconds(600);
    let woken = torii::cmd::run::wake(
        store_b.as_ref(),
        journal_b.as_ref(),
        run,
        queued_at,
        Some(TokenBudget {
            total_tokens: RAISED,
        }),
    )
    .await
    .expect("wake");
    assert_eq!(woken.code, torii::errors::EXIT_OK, "{}", woken.text);
    assert!(woken.text.contains("queued for wake"), "{}", woken.text);

    // The raise is DURABLE, and the run is now gateable at the new cap. Without
    // `BudgetRaised` the woken run would fold the ORIGINAL 100, refuse `n2` again and
    // re-pause — permanently stuck, and the feature useless.
    let (spent_before, budget_before) = orchestrator::spend_of(&journal_b.load(run).await.unwrap());
    assert_eq!(
        (spent_before, budget_before),
        (u64::from(PER_CALL), Some(RAISED)),
        "the raise is folded as the effective cap (latest wins), and the spend it is \
         weighed against survived the pause"
    );

    // ---- Process B: a FRESH metered worker drives it to completion ------------------
    let (sched_b, calls_b) =
        fresh_metered_worker(&url, queued_at + Duration::seconds(1), PER_CALL).await;
    let served = serve_until_settled(&sched_b, store_b.as_ref(), run).await;
    assert_eq!(served.code, torii::errors::EXIT_OK, "{}", served.text);
    assert_eq!(
        store_b.status(run).await.unwrap().unwrap().status,
        RunStatus::Completed,
        "under the raised cap the fresh process finishes the run: {}",
        served.text
    );

    // ---- Zero re-spend of the completed prefix, per node ----------------------------
    assert_eq!(
        calls_for(&calls_b, &first),
        0,
        "n1 was paid for by process A — B must replay it from the durable journal + \
         CAS, never call the gateway for it again"
    );
    assert_eq!(
        calls_for(&calls_b, &second),
        1,
        "exactly the one un-run node, driven once"
    );

    // ---- The ledger ACCUMULATED across the process boundary -------------------------
    // The assertion an in-memory counter could never satisfy: 300, not 150. A counter
    // that restarted at zero on resume would let every wake re-spend the whole cap.
    let (spent_after, budget_after) = orchestrator::spend_of(&journal_b.load(run).await.unwrap());
    assert_eq!(
        (spent_after, budget_after),
        (u64::from(PER_CALL) * 2, Some(RAISED)),
        "both processes' spend is in ONE durable ledger, folded by effect id"
    );
}

/// SP-6 s1 AC7 — the HITL loop, end to end, across a process boundary.
///
/// SP-DATA-4's e2e above proves the HOTL loop: an operator *outside* the run resumes it,
/// and `force_wake` is a RESUME, not a DECISION. This is the other half — a graph that is
/// DESIGNED to wait for a human, and an operator who carries an ANSWER back into it.
///
/// 1. **Process A** submits `n1 → gate → n2`. `n1` costs a real token; `gate` (an
///    `AwaitSignal` with a one-hour timeout) journals its ABSOLUTE deadline and pauses the
///    run durably, with `n2` never reached.
/// 2. **The operator, light tier** (`run list-paused`) names the awaiting NODE and its
///    deadline — read from the durable journal on its own pool, sharing nothing with A.
///    The scheduler row alone could not answer this: it is run-level.
/// 3. **`run signal`** journals the answer and queues the wake. The run is still merely
///    `paused` afterwards, because signalling queues; it does not drive.
/// 4. **Process B** — a fresh store/journal/content-store/context-store/gateway — drives
///    it through torii's real `worker serve --once`.
/// 5. The run reaches `Completed`, and the human's payload is the gate's durable output.
/// 6. **Zero re-spend, per node:** B's gateway saw `n2` exactly once and `n1` NEVER.
///
/// **Completion is asserted through the scheduler's `RunStatus` and the journal's
/// `RunCompleted`, never through a terminal re-`start`'s `RunOutcome`.** That is not
/// squeamishness: `run_await_signal` journals no `NodeCompleted` (matching `Branch` and
/// `Subgraph`), and a re-`start` of an already-TERMINAL run rebuilds `outputs`/`completed`
/// from exactly those events — so it would report an empty outcome here for a reason that
/// has nothing to do with signalling. That is the documented fresh-vs-terminal limitation
/// `run_subgraph` already carries, and "fixing" it for one node kind would make
/// `AwaitSignal` divergent from its siblings. The durable blackboard is correct throughout,
/// which is what the `context_refs` read below actually demonstrates.
#[cfg_attr(
    not(have_database_url),
    ignore = "needs a Postgres at $DATABASE_URL; see README, Postgres-backed tests"
)]
#[tokio::test]
async fn a_signalled_gate_is_answered_by_an_operator_and_completes_in_a_fresh_process() {
    let Some(url) = db_url() else { return };
    let _guard = scheduler_guard().await;

    let run = RunId(uuid::Uuid::new_v4());
    let marker = run.0.to_string();
    let (n1, gate, n2) = signal_graph_ids(&marker);
    // An hour out, so the deadline is far beyond every instant this test reaches: the run
    // is due at T+1h and nothing else can make it claimable. That is what turns the
    // completion assertion into a statement about the SIGNAL — without the delivery below,
    // process B's tick simply would not claim this run at all.
    let graph = signal_graph(&marker, Some(Duration::hours(1)));
    let t0 = DateTime::<Utc>::from_timestamp(6_000_000, 0).unwrap();
    let clock = FakeClock::new(t0);

    // ---- Process A: the prefix is paid for, then the gate pauses the run -------------
    let store_a = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
    let journal_a = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
    let (gw_a, calls_a) = recording_gateway().await;
    let exec_a = Executor::new(Arc::new(gw_a), journal_a.clone(), "v1")
        .with_content_store(Arc::new(PostgresContentStore::new(
            connect(&url).await.unwrap(),
        )))
        .with_context_store(Arc::new(PostgresContextStore::new(
            connect(&url).await.unwrap(),
        )))
        .with_clock(clock.clone());
    let sched_a = Scheduler::new(store_a.clone(), exec_a, journal_a.clone(), clock.clone());
    let submitted = torii::cmd::run::submit(&sched_a, run, graph.clone(), None, || {})
        .await
        .expect("a gate-paused run is not an error");
    assert_eq!(submitted.code, torii::errors::EXIT_OK, "{}", submitted.text);
    assert!(
        submitted.text.starts_with("paused:") && submitted.text.contains("await_signal"),
        "the gate must PAUSE the run (resumable), naming itself as the cause: {}",
        submitted.text
    );

    // The prefix is REAL — this is the spend the zero-re-spend assertion later protects.
    assert_eq!(
        (calls_for(&calls_a, &n1.0), calls_for(&calls_a, &n2.0)),
        (1, 0),
        "n1 spends; n2 is behind the gate and must not have run"
    );

    let deadline = t0 + Duration::hours(1);
    let paused = store_a
        .status(run)
        .await
        .unwrap()
        .expect("a schedule record");
    assert_eq!(paused.status, RunStatus::Paused);
    assert_eq!(
        paused.next_wake,
        Some(deadline),
        "the pause carries the gate's ABSOLUTE deadline, so the scheduler re-arms on the \
         same instant however often the run is woken early: {paused:?}"
    );

    // ---- The operator, light tier: WHICH node is waiting, and until when? ------------
    // A separate pool and a separate journal handle — no gateway, no model credentials,
    // nothing in-process shared with A.
    let store_b = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
    let journal_b = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
    let listed = torii::cmd::run::list_paused(store_b.as_ref(), journal_b.as_ref(), false)
        .await
        .expect("list-paused");
    assert_eq!(listed.code, torii::errors::EXIT_OK, "{}", listed.text);
    assert!(
        listed.text.contains("AWAITING A SIGNAL") && listed.text.contains(&gate.0),
        "list-paused must name the awaiting NODE, folded out of A's durable journal — an \
         operator cannot signal what they cannot discover: {}",
        listed.text
    );
    assert!(
        listed.text.contains(&format!(
            "deadline {}",
            deadline.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        )),
        "…and the deadline it will fail at: {}",
        listed.text
    );

    // ---- The operator answers ---------------------------------------------------------
    // A minute in — far short of the hour — so the gate is genuinely still waiting and the
    // completion below cannot be the timeout firing.
    let signalled_at = t0 + Duration::seconds(60);
    let payload = serde_json::json!({ "decision": "approved", "note": "reviewed by ops" });
    let answered = torii::cmd::run::signal(
        store_b.as_ref(),
        journal_b.as_ref(),
        run,
        gate.clone(),
        payload.clone(),
        signalled_at,
    )
    .await
    .expect("signal");
    assert_eq!(answered.code, torii::errors::EXIT_OK, "{}", answered.text);
    assert!(
        answered.text.starts_with("signalled:") && answered.text.contains(&gate.0),
        "{}",
        answered.text
    );

    // Signalling QUEUES; it does not drive — exactly what the message claims.
    let after_signal = store_b.status(run).await.unwrap().unwrap();
    assert_eq!(
        (after_signal.status, after_signal.next_wake),
        (RunStatus::Paused, Some(signalled_at)),
        "the answer is journaled and the wake is queued, but a worker tick does the \
         driving: {after_signal:?}"
    );

    // ---- Process B: a FRESH worker drives it ------------------------------------------
    // The only thing carried over from A is the run id; the graph included, everything
    // else B needs it reads out of Postgres.
    let (sched_b, calls_b) = fresh_context_worker(&url, signalled_at + Duration::seconds(1)).await;
    let served = serve_until_settled(&sched_b, store_b.as_ref(), run).await;
    assert_eq!(served.code, torii::errors::EXIT_OK, "{}", served.text);

    // ---- The run completed — asserted at the scheduler AND in the journal --------------
    assert_eq!(
        store_b.status(run).await.unwrap().unwrap().status,
        RunStatus::Completed,
        "the signalled run runs to completion in the fresh process: {}",
        served.text
    );
    let events = journal_b.load(run).await.unwrap();
    assert!(
        events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "…and says so durably, in the journal the scheduler read it from"
    );

    // ---- ZERO RE-SPEND of the completed prefix, per node --------------------------------
    // `calls_for` counts the recording gateway's calls whose PROMPT is the node id — so
    // this is attributable to this run and this node, not a global "did anything happen".
    // A bare total would be meaningless: `tick()` legitimately drives every due run in the
    // shared `scheduled_runs` table, leftovers from other suites included.
    assert_eq!(
        calls_for(&calls_b, &n1.0),
        0,
        "n1 was paid for by process A — B must replay it from the durable journal + CAS, \
         never call the gateway for it again"
    );
    assert_eq!(
        calls_for(&calls_b, &n2.0),
        1,
        "exactly the one node the answer unblocked, driven once"
    );

    // ---- The gate's deadline was recorded ONCE and never moved --------------------------
    // Two drives (A's pause, B's completion) and exactly one `SignalAwaited`. A deadline
    // recomputed as `now + timeout` per execution would journal a second one, an hour
    // later — the never-expires bug, which no assertion above would notice.
    let awaited: Vec<_> = events
        .iter()
        .filter_map(|(_, e)| match e {
            JournalEvent::SignalAwaited { node, deadline } if node == &gate => Some(*deadline),
            _ => None,
        })
        .collect();
    assert_eq!(
        awaited,
        vec![Some(deadline)],
        "the gate records its absolute deadline once, at first execution, and folds it \
         thereafter"
    );

    // ---- The human's answer IS the gate's durable output ---------------------------------
    // Read back through a THIRD context store on its own pool, so this is the durable row,
    // not B's in-process blackboard. Not asserted via a terminal re-`start`: see this
    // test's doc comment for why that would be empty here.
    let ctx = PostgresContextStore::new(connect(&url).await.unwrap());
    let published = ctx
        .get(Scope::Run, ContextKey(gate.0.clone()))
        .await
        .unwrap()
        .expect("the completed gate published its output to the durable blackboard");
    assert_eq!(
        ctx.load(&published).await.unwrap(),
        payload,
        "the operator's payload — unredacted, because a decision matches no credential \
         shape — is what the gate returned into the graph"
    );
}

/// SP-6 s2 AC12 — a `HumanGate` decided in one process is honoured by a FRESH worker in
/// another, over real Postgres, with zero token re-spend.
///
/// **DEV/CI-GATED: this test requires `DATABASE_URL` and is skipped without it, so it does
/// NOT run in the default `cargo test --workspace`.** That is why the in-process tests in
/// `cmd::gate` and `cmd::run` cover the same arms — the menu validation, the required
/// reason, the cross-refusals, the fold. This test exists for the one property none of
/// them can express: that the menu, the decision and the memoized prefix are all DURABLE,
/// read back by a process that shares nothing in-memory with the one that wrote them.
///
/// 1. **Process A** submits `n1 → release → n2`. `n1` costs a real token; `release` (a
///    `HumanGate` with a one-hour timeout and a `ship | hold` menu) journals its menu and
///    its ABSOLUTE deadline, and pauses the run durably with `n2` never reached.
/// 2. **The operator, light tier** (`run list-paused`) names the awaiting node AND ITS
///    MENU on its OWN pool — folded out of A's durable `GateAwaited`, with no graph in
///    hand. An operator cannot decide what they cannot see.
/// 3. **Discrimination: a bare `run wake` is a RESUME, not a DECISION.** A worker tick
///    after it re-pauses the run untouched — which is what makes step 5's completion a
///    statement about the DECISION rather than about the wake that accompanied it.
/// 4. **`run gate decide --option ship`** delivers on a THIRD pool, journaling
///    `GateDecided` and queuing the wake.
/// 5. **Process B** — a fresh store/journal/content-store/context-store/gateway — drives
///    it through torii's real `worker serve --once` to `Completed`.
/// 6. **Zero re-spend is ATTRIBUTABLE, not an empty log:** B's gateway calls are filtered
///    by PROMPT, which IS the node id, and the same filter returns 1 for `n2` and 0 for
///    `n1`. A bare total would prove nothing — `tick()` legitimately drives every due run
///    in the shared `scheduled_runs` table.
///
/// Completion is asserted through the scheduler's `RunStatus`, the journal's
/// `RunCompleted` and the durable blackboard — never through a terminal re-`start`'s
/// `RunOutcome`, which is empty for this node-kind family for reasons that have nothing to
/// do with gating. See the `AwaitSignal` e2e above for that argument in full.
#[cfg_attr(
    not(have_database_url),
    ignore = "needs a Postgres at $DATABASE_URL; see README, Postgres-backed tests"
)]
#[tokio::test]
async fn a_human_gate_decided_in_another_process_completes_the_run() {
    let Some(url) = db_url() else { return };
    let _guard = scheduler_guard().await;

    let run = RunId(uuid::Uuid::new_v4());
    let marker = run.0.to_string();
    let (n1, release, n2) = gate_graph_ids(&marker);
    // An hour out, so the deadline is far beyond every instant this test reaches: nothing
    // but an operator can make this run claimable, which is what turns the completion
    // below into a statement about the DECISION.
    let graph = gate_graph(&marker, Some(Duration::hours(1)));
    let t0 = DateTime::<Utc>::from_timestamp(7_000_000, 0).unwrap();
    let clock = FakeClock::new(t0);

    // ---- Process A: the prefix is paid for, then the gate asks and pauses -------------
    let store_a = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
    let journal_a = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
    let (gw_a, calls_a) = recording_gateway().await;
    let exec_a = Executor::new(Arc::new(gw_a), journal_a.clone(), "v1")
        .with_content_store(Arc::new(PostgresContentStore::new(
            connect(&url).await.unwrap(),
        )))
        .with_context_store(Arc::new(PostgresContextStore::new(
            connect(&url).await.unwrap(),
        )))
        .with_clock(clock.clone());
    let sched_a = Scheduler::new(store_a.clone(), exec_a, journal_a.clone(), clock.clone());
    let submitted = torii::cmd::run::submit(&sched_a, run, graph.clone(), None, || {})
        .await
        .expect("a gate-paused run is not an error");
    assert_eq!(submitted.code, torii::errors::EXIT_OK, "{}", submitted.text);
    assert!(
        submitted.text.starts_with("paused:") && submitted.text.contains("human_gate"),
        "the gate must PAUSE the run (resumable), naming itself as the cause: {}",
        submitted.text
    );

    // The prefix is REAL — this is the spend the zero-re-spend assertion later protects.
    assert_eq!(
        (calls_for(&calls_a, &n1.0), calls_for(&calls_a, &n2.0)),
        (1, 0),
        "n1 spends; n2 is behind the gate and must not have run"
    );

    let deadline = t0 + Duration::hours(1);
    let paused = store_a
        .status(run)
        .await
        .unwrap()
        .expect("a schedule record");
    assert_eq!(paused.status, RunStatus::Paused);
    assert_eq!(
        paused.next_wake,
        Some(deadline),
        "the pause carries the gate's ABSOLUTE deadline, so the scheduler re-arms on the \
         same instant however often the run is woken early: {paused:?}"
    );

    // ---- The operator, light tier: WHICH node is waiting, and on WHAT choices? --------
    // A separate pool and a separate journal handle — no gateway, no model credentials,
    // nothing in-process shared with A. The menu comes out of the durable `GateAwaited`;
    // `list-paused` has no graph in hand and must not need one.
    let store_b = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
    let journal_b = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
    let listed = torii::cmd::run::list_paused(store_b.as_ref(), journal_b.as_ref(), false)
        .await
        .expect("list-paused");
    assert_eq!(listed.code, torii::errors::EXIT_OK, "{}", listed.text);
    // The MENU is asserted on the gate's OWN row INSIDE the awaiting block, never merely
    // somewhere in the output — and both halves of that are load-bearing. `scheduled_runs`
    // is global, so a bare `contains("ship")` could be satisfied by a leftover row from
    // another suite; and this run's own TABLE row already carries the menu incidentally,
    // because the executor's pause reason lists it (`human_gate: waiting for a decision on
    // node release-… (ship | hold)`). Searching the whole output found that line first and
    // passed with the awaiting block missing the menu entirely — the assertion has to be
    // anchored past the header to mean anything.
    let block = listed
        .text
        .split_once("AWAITING A SIGNAL")
        .unwrap_or_else(|| {
            panic!(
                "list-paused must render the awaiting block for a gate:\n{}",
                listed.text
            )
        })
        .1;
    let row = block
        .lines()
        .find(|l| l.starts_with(&marker) && l.contains(&release.0))
        .unwrap_or_else(|| {
            panic!(
                "the awaiting block must name the awaiting NODE — an operator cannot \
                 decide on what they cannot discover:\n{}",
                listed.text
            )
        });
    assert!(
        row.contains("gate:"),
        "…and must mark it a GATE, which takes a named option rather than the raw JSON \
         `run signal` delivers: {row}"
    );
    assert!(
        row.contains("ship") && row.contains("hold"),
        "…and its MENU, so the choices are visible without reading the graph: {row}"
    );
    assert!(
        row.contains(&format!(
            "deadline {}",
            deadline.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        )),
        "…and the deadline it will FAIL at: {row}"
    );

    // ---- Discrimination: a bare `wake` is a RESUME, not a DECISION --------------------
    // Without this the completion below would be ambiguous: `gate decide` also queues a
    // wake, so a `HumanGate` that completed on ANY resume would pass every assertion after
    // it. Here the very same run is woken with no decision on the journal, driven by a
    // real worker tick, and must come back untouched.
    let woken_at = t0 + Duration::seconds(60);
    let woken = torii::cmd::run::wake(store_b.as_ref(), journal_b.as_ref(), run, woken_at, None)
        .await
        .expect("wake");
    assert_eq!(woken.code, torii::errors::EXIT_OK, "{}", woken.text);
    let (sched_w, calls_w) = fresh_context_worker(&url, woken_at + Duration::seconds(1)).await;
    let served_w = serve_once(&sched_w).await;
    assert_eq!(served_w.code, torii::errors::EXIT_OK, "{}", served_w.text);
    let after_wake = store_b.status(run).await.unwrap().unwrap();
    assert_eq!(
        (after_wake.status, after_wake.next_wake),
        (RunStatus::Paused, Some(deadline)),
        "a wake with no decision must leave the gate waiting, re-armed on the deadline it \
         RECORDED — never completed, never failed: {after_wake:?}"
    );
    assert_eq!(
        (calls_for(&calls_w, &n1.0), calls_for(&calls_w, &n2.0)),
        (0, 0),
        "and it must cost nothing: n1 replays from the durable journal + CAS, and n2 is \
         still behind an unanswered gate"
    );

    // ---- The operator decides, on a THIRD pool ---------------------------------------
    // Two minutes in — far short of the hour — so the gate is genuinely still waiting and
    // the completion below cannot be the timeout firing.
    let decided_at = t0 + Duration::seconds(120);
    let store_c = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
    let journal_c = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
    let decided = torii::cmd::gate::decide(
        store_c.as_ref(),
        journal_c.as_ref(),
        run,
        release.clone(),
        "ship",
        "ops",
        Some("reviewed by ops"),
        decided_at,
    )
    .await
    .expect("decide");
    assert_eq!(decided.code, torii::errors::EXIT_OK, "{}", decided.text);
    assert!(
        decided.text.starts_with("decided:") && decided.text.contains(&release.0),
        "{}",
        decided.text
    );

    // Deciding QUEUES; it does not drive — exactly what the message claims.
    let after_decide = store_c.status(run).await.unwrap().unwrap();
    assert_eq!(
        (after_decide.status, after_decide.next_wake),
        (RunStatus::Paused, Some(decided_at)),
        "the decision is journaled and the wake is queued, but a worker tick does the \
         driving: {after_decide:?}"
    );

    // ---- Process B: a FRESH worker drives it -----------------------------------------
    // The only thing carried over from A is the run id; everything else B needs — the
    // graph included — it reads out of Postgres.
    let (sched_b, calls_b) = fresh_context_worker(&url, decided_at + Duration::seconds(1)).await;
    let served = serve_until_settled(&sched_b, store_b.as_ref(), run).await;
    assert_eq!(served.code, torii::errors::EXIT_OK, "{}", served.text);

    // ---- The run completed — asserted at the scheduler AND in the journal --------------
    assert_eq!(
        store_b.status(run).await.unwrap().unwrap().status,
        RunStatus::Completed,
        "the decided run runs to completion in the fresh process: {}",
        served.text
    );
    let events = journal_b.load(run).await.unwrap();
    assert!(
        events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "…and says so durably, in the journal the scheduler read it from"
    );

    // ---- ZERO RE-SPEND of the completed prefix, per node --------------------------------
    // Attributable, not an empty log: `calls_for` counts the recording gateway's calls
    // whose PROMPT is the node id, so the SAME filter that returns 1 for `n2` returns 0 for
    // `n1`. A bare total would be meaningless — `tick()` legitimately drives every due run
    // in the shared `scheduled_runs` table, leftovers from other suites included.
    assert_eq!(
        calls_for(&calls_b, &n1.0),
        0,
        "n1 was paid for by process A — B must replay it from the durable journal + CAS, \
         never call the gateway for it again"
    );
    assert_eq!(
        calls_for(&calls_b, &n2.0),
        1,
        "exactly the one node the decision unblocked, driven once"
    );

    // ---- The gate asked ONCE: one menu, one deadline, across three drives ---------------
    // A's pause, the bare wake's re-pause and B's completion — and exactly one
    // `GateAwaited`. A menu re-published per execution would let an author edit a live
    // run's graph and retroactively change what a human's recorded answer meant; a
    // deadline recomputed as `now + timeout` would be the never-expires bug. Neither is
    // visible to any assertion above.
    let awaited: Vec<_> = events
        .iter()
        .filter_map(|(_, e)| match e {
            JournalEvent::GateAwaited {
                node,
                deadline,
                options,
            } if node == &release => Some((
                *deadline,
                options.iter().map(|o| o.name.clone()).collect::<Vec<_>>(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        awaited,
        vec![(Some(deadline), vec!["ship".to_string(), "hold".to_string()])],
        "the gate publishes its menu and its absolute deadline once, at first execution, \
         and folds them thereafter"
    );

    // ---- The human's decision IS the gate's durable output ------------------------------
    // Read back through a THIRD context store on its own pool, so this is the durable row,
    // not B's in-process blackboard. `{decision, actor, note}` is the shape a downstream
    // `Branch` matches on, which is why the whole object is asserted rather than the name.
    let ctx = PostgresContextStore::new(connect(&url).await.unwrap());
    let published = ctx
        .get(Scope::Run, ContextKey(release.0.clone()))
        .await
        .unwrap()
        .expect("the completed gate published its decision to the durable blackboard");
    assert_eq!(
        ctx.load(&published).await.unwrap(),
        serde_json::json!({
            "decision": "ship",
            "actor": "ops",
            "note": "reviewed by ops",
        }),
        "the operator's decision — who, what and why — is what the gate returned into the \
         graph"
    );
}

/// SP-6 s3 AC13 — a human-backed ROLE, end to end, across a process boundary.
///
/// The third waiting kind (SP-6 s4's human loop gate is the fourth). s1's `AwaitSignal`
/// waits for arbitrary JSON and s2's `HumanGate` for a named option; here the waiting
/// thing is an ordinary top-level
/// [`NodeKind::Agent`] whose `AgentRef` happens to resolve to an `AgentBacking::Human`
/// definition — so the GRAPH is the model-backed graph, unchanged, and the substitution
/// lives entirely in the registry. That is the property this test exists to show over a
/// process boundary:
///
/// 1. **Process A** submits `n1 → review → n2`. `n1` costs a real gateway call; `review`
///    journals its QUESTION and takes a durable, timed pause.
/// 2. **The operator, light tier** (`run list-paused`) reads that question out of the
///    database on its OWN pool — no gateway, no model credentials, no graph in hand.
/// 3. **Discrimination**: a bare `run wake` is a RESUME, not an ANSWER. The run comes back
///    still paused, re-armed on the same instant, having spent nothing.
/// 4. **The operator answers** on a THIRD pool (`run agent answer --text … --as …`).
/// 5. **Process B** — a fresh store/journal/content-store/context-store/gateway/registry,
///    sharing nothing in-process with A — drives it through torii's real
///    `worker serve --once` to `Completed`.
/// 6. **Zero re-spend**, asserted per node, and the human's free text is the node's durable
///    OUTPUT under `"text"` — the same key a model-backed agent writes, which is what lets
///    a downstream node consume it without knowing a person produced it.
///
/// **`DATABASE_URL`-gated, and the consequence is worth stating plainly: with no database
/// this test RETURNS EARLY and is counted as PASSED while exercising nothing.** The raw
/// stderr `SKIP` line [`db_url`] writes is the only signal that happened. It is not
/// `#[ignore]`d, and it cannot be: `#[ignore]` is a compile-time attribute and cannot be
/// conditioned on an environment variable. (s2's spec claimed this suite was `#[ignore]`d.
/// It never was, and the claim is repeated here only to kill it.)
#[cfg_attr(
    not(have_database_url),
    ignore = "needs a Postgres at $DATABASE_URL; see README, Postgres-backed tests"
)]
#[tokio::test]
async fn a_human_backed_agent_answered_in_another_process_completes_the_run() {
    let Some(url) = db_url() else { return };
    let _guard = scheduler_guard().await;

    let run = RunId(uuid::Uuid::new_v4());
    let marker = run.0.to_string();
    let (n1, review, n2) = human_agent_graph_ids(&marker);
    let graph = human_agent_graph(&marker);
    // An hour out, so the deadline is far beyond every instant this test reaches: nothing
    // but an operator can make this run claimable, which is what turns the completion
    // below into a statement about the ANSWER.
    let sla = Some(Duration::hours(1));
    let t0 = DateTime::<Utc>::from_timestamp(8_000_000, 0).unwrap();
    let clock = FakeClock::new(t0);

    // ---- Process A: the prefix is paid for, then the role asks and pauses -------------
    let store_a = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
    let journal_a = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
    let (gw_a, calls_a) = recording_gateway().await;
    let exec_a = Executor::new(Arc::new(gw_a), journal_a.clone(), "v1")
        .with_content_store(Arc::new(PostgresContentStore::new(
            connect(&url).await.unwrap(),
        )))
        .with_context_store(Arc::new(PostgresContextStore::new(
            connect(&url).await.unwrap(),
        )))
        .with_registry(human_registry(sla))
        .with_clock(clock.clone());
    let sched_a = Scheduler::new(store_a.clone(), exec_a, journal_a.clone(), clock.clone());
    let submitted = torii::cmd::run::submit(&sched_a, run, graph.clone(), None, || {})
        .await
        .expect("a role-paused run is not an error");
    assert_eq!(submitted.code, torii::errors::EXIT_OK, "{}", submitted.text);
    assert!(
        submitted.text.starts_with("paused:") && submitted.text.contains("human_agent"),
        "the human-backed role must PAUSE the run (resumable), naming itself as the cause \
         — not fail it, and not fall through to the model path: {}",
        submitted.text
    );

    // The prefix is REAL — this is the spend the zero-re-spend assertion later protects.
    // `n2` is behind the role and `review` itself resolves no chain at all, so a `2` here
    // would mean the human branch had been bypassed.
    assert_eq!(
        (calls_for(&calls_a, &n1.0), calls_for(&calls_a, &n2.0)),
        (1, 0),
        "n1 spends; n2 is behind the unanswered role and must not have run"
    );

    let deadline = t0 + Duration::hours(1);
    let paused = store_a
        .status(run)
        .await
        .unwrap()
        .expect("a schedule record");
    assert_eq!(paused.status, RunStatus::Paused);
    assert_eq!(
        paused.next_wake,
        Some(deadline),
        "the pause carries the role's ABSOLUTE deadline, so the scheduler re-arms on the \
         same instant however often the run is woken early: {paused:?}"
    );

    // ---- The operator, light tier: WHICH node is waiting, and on WHAT question? -------
    // A separate pool and a separate journal handle — no gateway, no model credentials,
    // no registry, nothing in-process shared with A. The question comes out of the durable
    // `AgentAwaited`; `list-paused` has neither the graph nor the config in hand and must
    // not need either.
    let store_b = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
    let journal_b = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
    let listed = torii::cmd::run::list_paused(store_b.as_ref(), journal_b.as_ref(), false)
        .await
        .expect("list-paused");
    assert_eq!(listed.code, torii::errors::EXIT_OK, "{}", listed.text);
    // The question is asserted on this run's OWN row INSIDE the awaiting block, and both
    // anchors are load-bearing — but NOT for the gate e2e's reason, which an earlier
    // version of this comment copied down here and which is FALSE for a role. A gate's
    // pause reason really does list its menu (`human_gate: waiting for a decision on node
    // release-… (ship | hold)`), so for a gate a whole-output `contains("ship")` passes off
    // the TABLE row with the awaiting block empty. A human-backed role's pause reason
    // carries the node id and the deadline and NOTHING else — `executor/human.rs` builds it
    // from exactly those two — so the question cannot leak into the table that way. The
    // assertion below pins that, because the whole argument for anchoring rests on it.
    //
    // What makes each anchor load-bearing here, both observed against a live database
    // rather than reasoned about:
    //
    //   * `starts_with(marker)` + the node id, because `scheduled_runs` is GLOBAL and every
    //     human-backed run in this suite asks the IDENTICAL question — one `REVIEWER_PROMPT`
    //     const, one registry. A run left paused by an earlier failed invocation is still
    //     listed, so this block legitimately renders a STRANGER's `agent:` row carrying the
    //     same prompt, and an unanchored `listed.text.contains(REVIEWER_PROMPT)` is
    //     satisfied by it. Two consecutive runs against one database rendered exactly that:
    //     two `agent:` rows differing only in their run and node ids.
    //   * `split_once` past the header, because the TABLE row above ALSO starts with the
    //     marker and names the node (its reason interpolates the node id), so an unanchored
    //     `find` lands there first — and that line satisfies `contains("agent:")` through
    //     the `human_agent:` prefix of its reason. Unanchored, the kind assertion below
    //     would pass on a line that is not in the awaiting block at all.
    let first_match = listed
        .text
        .lines()
        .find(|l| l.starts_with(&marker) && l.contains(&review.0))
        .expect("at minimum, this run's own table row");
    assert!(
        !first_match.contains(REVIEWER_PROMPT),
        "a role's pause reason must NOT carry the question — it is the node id and the \
         deadline only. If it ever does, the anchoring argument above changes and the \
         question assertions stop distinguishing the awaiting block from the table: \
         {first_match}"
    );
    let block = listed
        .text
        .split_once("AWAITING A SIGNAL")
        .unwrap_or_else(|| {
            panic!(
                "list-paused must render the awaiting block for a human-backed role:\n{}",
                listed.text
            )
        })
        .1;
    let row = block
        .lines()
        .find(|l| l.starts_with(&marker) && l.contains(&review.0))
        .unwrap_or_else(|| {
            panic!(
                "the awaiting block must name the awaiting NODE — an operator cannot \
                 answer what they cannot discover:\n{}",
                listed.text
            )
        });
    assert!(
        row.contains("agent:"),
        "…and must mark it an AGENT, which takes free text rather than the named option a \
         gate takes or the raw JSON `run signal` delivers — the three verbs refuse each \
         other, so a mislabelled row sends an operator to a refusal: {row}"
    );
    assert!(
        row.contains(REVIEWER_PROMPT) && row.contains(REVIEWER_TASK),
        "…and the QUESTION, both halves of it: the role's standing instructions AND the \
         node's input under `## Task`. The input half is the one Task 4's review found \
         missing (finding 4, fixed in 367daf6) — a reviewer asked to judge a contract with \
         no contract named — so it is asserted here too, at the surface an operator \
         actually reads: {row}"
    );
    assert!(
        row.contains(&format!(
            "deadline {}",
            deadline.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
        )),
        "…and the deadline it will FAIL at: {row}"
    );

    // ---- Discrimination: a bare `wake` is a RESUME, not an ANSWER --------------------
    // Without this the completion below would be ambiguous: `agent answer` also queues a
    // wake, so a role that completed on ANY resume — on an empty answer, on a default —
    // would pass every assertion after it. Here the very same run is woken with no answer
    // on the journal, driven by a real worker tick, and must come back untouched.
    let woken_at = t0 + Duration::seconds(60);
    let woken = torii::cmd::run::wake(store_b.as_ref(), journal_b.as_ref(), run, woken_at, None)
        .await
        .expect("wake");
    assert_eq!(woken.code, torii::errors::EXIT_OK, "{}", woken.text);
    let (sched_w, calls_w) = fresh_human_worker(&url, woken_at + Duration::seconds(1), sla).await;
    let served_w = serve_once(&sched_w).await;
    assert_eq!(served_w.code, torii::errors::EXIT_OK, "{}", served_w.text);
    let after_wake = store_b.status(run).await.unwrap().unwrap();
    assert_eq!(
        (after_wake.status, after_wake.next_wake),
        (RunStatus::Paused, Some(deadline)),
        "a wake with no answer must leave the role waiting, re-armed on the deadline it \
         RECORDED — never completed, never failed: {after_wake:?}"
    );
    assert_eq!(
        (calls_for(&calls_w, &n1.0), calls_for(&calls_w, &n2.0)),
        (0, 0),
        "and it must cost nothing: n1 replays from the durable journal + CAS, and n2 is \
         still behind an unanswered role"
    );

    // ---- The operator answers, on a THIRD pool ---------------------------------------
    // Two minutes in — far short of the hour — so the role is genuinely still waiting and
    // the completion below cannot be the timeout firing.
    const ANSWER: &str = "Clause 7 permits sub-processing with 30 days' written notice.";
    let answered_at = t0 + Duration::seconds(120);
    let store_c = Arc::new(PostgresSchedulerStore::new(connect(&url).await.unwrap()));
    let journal_c = Arc::new(PostgresJournal::new(connect(&url).await.unwrap()));
    let delivered = torii::cmd::human::answer(
        store_c.as_ref(),
        journal_c.as_ref(),
        run,
        review.clone(),
        ANSWER,
        "counsel",
        answered_at,
    )
    .await
    .expect("answer");
    assert_eq!(delivered.code, torii::errors::EXIT_OK, "{}", delivered.text);
    assert!(
        delivered.text.starts_with("answered:") && delivered.text.contains(&review.0),
        "{}",
        delivered.text
    );

    // Answering QUEUES; it does not drive — exactly what the message claims.
    let after_answer = store_c.status(run).await.unwrap().unwrap();
    assert_eq!(
        (after_answer.status, after_answer.next_wake),
        (RunStatus::Paused, Some(answered_at)),
        "the answer is journaled and the wake is queued, but a worker tick does the \
         driving: {after_answer:?}"
    );

    // ---- Process B: a FRESH worker drives it -----------------------------------------
    // The only things carried over from A are the run id and the config — everything else
    // B needs, the graph included, it reads out of Postgres. (Why the config is rebuilt
    // rather than read back: see `human_registry`.)
    let (sched_b, calls_b) =
        fresh_human_worker(&url, answered_at + Duration::seconds(1), sla).await;
    let served = serve_until_settled(&sched_b, store_b.as_ref(), run).await;
    assert_eq!(served.code, torii::errors::EXIT_OK, "{}", served.text);

    // ---- The run completed — asserted at the scheduler AND in the journal -------------
    assert_eq!(
        store_b.status(run).await.unwrap().unwrap().status,
        RunStatus::Completed,
        "the answered run runs to completion in the fresh process: {}",
        served.text
    );
    let events = journal_b.load(run).await.unwrap();
    assert!(
        events
            .iter()
            .any(|(_, e)| matches!(e, JournalEvent::RunCompleted)),
        "…and says so durably, in the journal the scheduler read it from"
    );

    // ---- ZERO RE-SPEND of the completed prefix, per node ------------------------------
    // Attributable, not an empty log: `calls_for` counts the recording gateway's calls
    // whose PROMPT is the node id, so the SAME filter that returns 1 for `n2` returns 0 for
    // `n1`. A bare total would be meaningless — `tick()` legitimately drives every due run
    // in the shared `scheduled_runs` table, leftovers from other suites included.
    assert_eq!(
        calls_for(&calls_b, &n1.0),
        0,
        "n1 was paid for by process A — B must replay it from the durable journal + CAS, \
         never call the gateway for it again"
    );
    assert_eq!(
        calls_for(&calls_b, &n2.0),
        1,
        "exactly the one node the answer unblocked, driven once"
    );

    // ---- The role asked ONCE: one question, one deadline, across three drives ----------
    // A's pause, the bare wake's re-pause and B's completion — and exactly one
    // `AgentAwaited`. A question re-published per execution would let an author edit a live
    // run's config and retroactively change what a human's recorded answer meant; a
    // deadline recomputed as `now + timeout` would be the never-expires bug. Neither is
    // visible to any assertion above.
    //
    // The question is asserted WHOLE here rather than by substring as the listing row was:
    // the row is capped for display, the journal is not, and this is the durable value.
    let awaited: Vec<_> = events
        .iter()
        .filter_map(|(_, e)| match e {
            JournalEvent::AgentAwaited {
                node,
                deadline,
                prompt,
            } if node == &review => Some((*deadline, prompt.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(awaited.len(), 1, "exactly one ask, ever: {awaited:?}");
    let (asked_deadline, question) = &awaited[0];
    assert_eq!(
        *asked_deadline,
        Some(deadline),
        "the role publishes its ABSOLUTE deadline once, at first execution, and folds it \
         thereafter"
    );
    // Start and end rather than full equality, because the middle is the `## Context`
    // section `assemble_prompt` renders from `n1`'s durable blackboard output — real, and
    // deliberately not pinned here, since asserting a `ModelCall`'s output shape would make
    // this test fail for a reason that has nothing to do with human-backed roles.
    assert!(
        question.starts_with(REVIEWER_PROMPT),
        "the question opens with the role's standing instructions: {question:?}"
    );
    assert!(
        question.ends_with(&format!("\n\n## Task\n{REVIEWER_TASK}")),
        "…and closes with the node's input under `## Task`, which is what the model would \
         have received as its first user message: {question:?}"
    );

    // ---- The human's answer IS the role's durable output --------------------------------
    // Read back through a THIRD context store on its own pool, so this is the durable row,
    // not B's in-process blackboard.
    //
    // `"text"` is deliberately the SAME key a model-backed agent writes, which is what lets
    // a downstream `Branch` or prompt consume a human's answer without knowing it was
    // human; `"actor"` is the part a model output has no equivalent of, and it is what makes
    // the run auditable after the fact.
    let ctx = PostgresContextStore::new(connect(&url).await.unwrap());
    let published = ctx
        .get(Scope::Run, ContextKey(review.0.clone()))
        .await
        .unwrap()
        .expect("the completed role published its answer to the durable blackboard");
    assert_eq!(
        ctx.load(&published).await.unwrap(),
        serde_json::json!({ "text": ANSWER, "actor": "counsel" }),
        "the human's free text — and who gave it — is what the role returned into the graph"
    );
}
