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
//! Both tests drive their process B through the REAL [`torii::cmd::worker::serve`] rather
//! than calling `Scheduler::tick` directly: asserting a re-implementation of the
//! single-tick contract would prove nothing about the command an operator actually runs.
//! That is the reason this crate is a lib+bin pair.

use chrono::{DateTime, Duration, Utc};
use orchestrator::test_support::{CallLog, FakeClock, gated_gateway, recording_gateway};
use orchestrator::{Executor, Scheduler};
use orchestrator_core::{
    ConfigSource, Graph, Node, NodeId, NodeKind, RegistryHandle, RunId, RunStatus, SchedulerStore,
};
use orchestrator_store::postgres::{
    PostgresConfigSource, PostgresContentStore, PostgresJournal, PostgresSchedulerStore, connect,
};
use std::sync::Arc;

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
/// wrong executor. Serializing them process-wide is the same guard the store crate's own
/// scheduler tests use, for the same reason. (Cross-PROCESS isolation is not needed:
/// cargo runs test binaries one at a time.)
static SCHEDULED_RUNS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
#[tokio::test]
async fn the_operator_loop_drives_a_paused_run_to_completion_across_processes() {
    let Some(url) = db_url() else { return };
    let _guard = SCHEDULED_RUNS.lock().await;

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
    let listed = torii::cmd::run::list_paused(store_b.as_ref(), false)
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
    let served = serve_once(&sched_b).await;
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
#[tokio::test]
async fn a_cancelled_run_is_never_driven_by_a_later_worker_tick() {
    let Some(url) = db_url() else { return };
    let _guard = SCHEDULED_RUNS.lock().await;

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
#[tokio::test]
async fn a_stale_config_generation_fails_a_wake_at_the_fence_before_spending_anything() {
    let Some(url) = db_url() else { return };
    let _guard = SCHEDULED_RUNS.lock().await;

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
    let served = serve_once(&sched_b).await;
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
